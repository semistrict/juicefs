/*
 * JuiceFS, Copyright 2024 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/juicedata/juicefs/pkg/meta/pb"
	"google.golang.org/protobuf/proto"
)

func init() {
	Register("wskv", newKVMeta)
	drivers["wskv"] = newWskvClient
}

// wskvClient implements tkvClient by proxying KV operations over WebSocket
// to a Cloudflare Durable Object. Messages are protobuf-encoded binary frames
// using the WskvMessage envelope (see pb/wskv.proto).
type wskvClient struct {
	ws      *websocket.Conn
	wsMu    sync.Mutex // serializes WebSocket writes
	reqID   atomic.Uint64
	pending sync.Map // id → chan *pb.WskvMessage
	ready   chan struct{}
}

var globalWskvClient *wskvClient

func newWskvClient(addr string) (tkvClient, error) {
	if globalWskvClient == nil {
		globalWskvClient = &wskvClient{
			ready: make(chan struct{}),
		}
	}
	<-globalWskvClient.ready
	return globalWskvClient, nil
}

// SetWskvConnection stores the WebSocket connection and starts the read loop.
// Must be called before any meta operations.
func SetWskvConnection(ws *websocket.Conn) {
	if globalWskvClient == nil {
		globalWskvClient = &wskvClient{
			ready: make(chan struct{}),
		}
	}
	globalWskvClient.ws = ws
	close(globalWskvClient.ready)
	go globalWskvClient.readLoop()
}

func (c *wskvClient) readLoop() {
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			c.pending.Range(func(key, value interface{}) bool {
				ch := value.(chan *pb.WskvMessage)
				ch <- nil // signal error
				return true
			})
			return
		}
		var msg pb.WskvMessage
		if err := proto.Unmarshal(data, &msg); err != nil {
			continue
		}
		// Extract the ID from whichever response type is set.
		var id uint64
		switch m := msg.Msg.(type) {
		case *pb.WskvMessage_GetResp:
			id = m.GetResp.Id
		case *pb.WskvMessage_ListResp:
			id = m.ListResp.Id
		case *pb.WskvMessage_CommitResp:
			id = m.CommitResp.Id
		case *pb.WskvMessage_ResetResp:
			id = m.ResetResp.Id
		default:
			continue
		}
		if ch, ok := c.pending.LoadAndDelete(id); ok {
			ch.(chan *pb.WskvMessage) <- &msg
		}
	}
}

func (c *wskvClient) send(msg *pb.WskvMessage) (*pb.WskvMessage, error) {
	// Extract ID from the request.
	var id uint64
	switch m := msg.Msg.(type) {
	case *pb.WskvMessage_GetReq:
		id = m.GetReq.Id
	case *pb.WskvMessage_ListReq:
		id = m.ListReq.Id
	case *pb.WskvMessage_CommitReq:
		id = m.CommitReq.Id
	case *pb.WskvMessage_ResetReq:
		id = m.ResetReq.Id
	}

	ch := make(chan *pb.WskvMessage, 1)
	c.pending.Store(id, ch)

	data, err := proto.Marshal(msg)
	if err != nil {
		c.pending.Delete(id)
		return nil, err
	}

	c.wsMu.Lock()
	err = c.ws.WriteMessage(websocket.BinaryMessage, data)
	c.wsMu.Unlock()
	if err != nil {
		c.pending.Delete(id)
		return nil, err
	}

	resp := <-ch
	if resp == nil {
		return nil, fmt.Errorf("websocket closed")
	}
	return resp, nil
}

func (c *wskvClient) nextID() uint64 {
	return c.reqID.Add(1)
}

func (c *wskvClient) name() string {
	return "wskv"
}

func (c *wskvClient) shouldRetry(err error) bool {
	return strings.Contains(err.Error(), "write conflict")
}

func (c *wskvClient) config(key string) interface{} {
	return nil
}

func (c *wskvClient) gc() {}

func (c *wskvClient) close() error {
	if c.ws != nil {
		return c.ws.Close()
	}
	return nil
}

// wskvTxn implements kvtxn with OCC: reads go to the remote store and record
// versions, writes are buffered locally, and commit sends everything in one shot.
type wskvTxn struct {
	client   *wskvClient
	observed map[string]uint32 // raw key (as string) → version
	buffer   map[string][]byte // raw key (as string) → value (nil = delete)
}

func (tx *wskvTxn) get(key []byte) []byte {
	k := string(key)
	if v, ok := tx.buffer[k]; ok {
		return v
	}
	resp, err := tx.client.send(&pb.WskvMessage{
		Msg: &pb.WskvMessage_GetReq{GetReq: &pb.GetRequest{
			Id:  tx.client.nextID(),
			Key: key,
		}},
	})
	if err != nil {
		tx.observed[k] = 0
		return nil
	}
	r := resp.GetGetResp()
	if r == nil {
		tx.observed[k] = 0
		return nil
	}
	tx.observed[k] = r.Ver
	if !r.Found {
		return nil
	}
	return r.Value
}

func (tx *wskvTxn) gets(keys ...[]byte) [][]byte {
	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = tx.get(key)
	}
	return values
}

func (tx *wskvTxn) scan(begin, end []byte, keysOnly bool, handler func(k, v []byte) bool) {
	resp, err := tx.client.send(&pb.WskvMessage{
		Msg: &pb.WskvMessage_ListReq{ListReq: &pb.ListRequest{
			Id:       tx.client.nextID(),
			Start:    begin,
			End:      end,
			KeysOnly: keysOnly,
		}},
	})
	if err != nil {
		return
	}
	r := resp.GetListResp()
	if r == nil {
		return
	}
	for _, entry := range r.Entries {
		tx.observed[string(entry.Key)] = entry.Ver
		if !handler(entry.Key, entry.Value) {
			break
		}
	}
}

func (tx *wskvTxn) exist(prefix []byte) bool {
	end := nextKey(prefix)
	resp, err := tx.client.send(&pb.WskvMessage{
		Msg: &pb.WskvMessage_ListReq{ListReq: &pb.ListRequest{
			Id:       tx.client.nextID(),
			Start:    prefix,
			End:      end,
			KeysOnly: true,
			Limit:    1,
		}},
	})
	if err != nil {
		return false
	}
	r := resp.GetListResp()
	if r == nil {
		return false
	}
	for _, entry := range r.Entries {
		tx.observed[string(entry.Key)] = entry.Ver
	}
	return len(r.Entries) > 0
}

func (tx *wskvTxn) set(key, value []byte) {
	tx.buffer[string(key)] = value
}

func (tx *wskvTxn) append(key []byte, value []byte) {
	new := append(tx.get(key), value...)
	tx.set(key, new)
}

func (tx *wskvTxn) incrBy(key []byte, value int64) int64 {
	buf := tx.get(key)
	new := parseCounter(buf)
	if value != 0 {
		new += value
		tx.set(key, packCounter(new))
	}
	return new
}

func (tx *wskvTxn) delete(key []byte) {
	tx.buffer[string(key)] = nil
}

func (c *wskvClient) txn(ctx context.Context, f func(*kvTxn) error, retry int) error {
	tx := &wskvTxn{
		client:   c,
		observed: make(map[string]uint32),
		buffer:   make(map[string][]byte),
	}
	if err := f(&kvTxn{tx, retry}); err != nil {
		return err
	}
	if len(tx.buffer) == 0 {
		return nil // read-only transaction
	}

	// Build commit message
	var observed []*pb.Observed
	for k, ver := range tx.observed {
		observed = append(observed, &pb.Observed{Key: []byte(k), Ver: ver})
	}
	var puts []*pb.Put
	var dels [][]byte
	for k, v := range tx.buffer {
		if v == nil {
			dels = append(dels, []byte(k))
		} else {
			puts = append(puts, &pb.Put{Key: []byte(k), Value: v})
		}
	}

	resp, err := c.send(&pb.WskvMessage{
		Msg: &pb.WskvMessage_CommitReq{CommitReq: &pb.CommitRequest{
			Id:       c.nextID(),
			Observed: observed,
			Puts:     puts,
			Dels:     dels,
		}},
	})
	if err != nil {
		return err
	}
	r := resp.GetCommitResp()
	if r == nil {
		return fmt.Errorf("unexpected response")
	}
	if !r.Ok {
		return fmt.Errorf("write conflict")
	}
	return nil
}

func (c *wskvClient) simpleTxn(ctx context.Context, f func(*kvTxn) error, retry int) error {
	return c.txn(ctx, f, retry)
}

func (c *wskvClient) scan(prefix []byte, handler func(key, value []byte) bool) error {
	resp, err := c.send(&pb.WskvMessage{
		Msg: &pb.WskvMessage_ListReq{ListReq: &pb.ListRequest{
			Id:    c.nextID(),
			Start: prefix,
			End:   nextKey(prefix),
		}},
	})
	if err != nil {
		return err
	}
	r := resp.GetListResp()
	if r == nil {
		return fmt.Errorf("unexpected response")
	}
	for _, entry := range r.Entries {
		if !handler(entry.Key, entry.Value) {
			break
		}
	}
	return nil
}

func (c *wskvClient) reset(prefix []byte) error {
	if len(prefix) == 0 {
		resp, err := c.send(&pb.WskvMessage{
			Msg: &pb.WskvMessage_ResetReq{ResetReq: &pb.ResetRequest{
				Id: c.nextID(),
			}},
		})
		if err != nil {
			return err
		}
		r := resp.GetResetResp()
		if r != nil && r.Error != "" {
			return fmt.Errorf("%s", r.Error)
		}
		return nil
	}
	return c.txn(Background(), func(kt *kvTxn) error {
		return c.scan(prefix, func(key, value []byte) bool {
			kt.delete(key)
			return true
		})
	}, 0)
}
