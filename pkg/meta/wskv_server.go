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
	"bytes"
	"sort"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/juicedata/juicefs/pkg/meta/pb"
	"google.golang.org/protobuf/proto"
)

// WskvServer implements the DO-side of the wskv WebSocket protocol.
// It stores key-value pairs with OCC versioning and handles get/list/commit/reset
// messages over a WebSocket connection using protobuf encoding.
//
// This can be backed by any storage; the default implementation uses an in-memory
// store suitable for testing. For production, the Cloudflare DO uses SQLite.
type WskvServer struct {
	mu    sync.Mutex
	store map[string]*wskvServerEntry
}

type wskvServerEntry struct {
	value []byte
	ver   uint32
}

// NewWskvServer creates a new in-memory wskv server.
func NewWskvServer() *WskvServer {
	return &WskvServer{store: make(map[string]*wskvServerEntry)}
}

// Serve reads messages from the WebSocket and handles them until the connection closes.
// This blocks and should be called in a goroutine.
func (s *WskvServer) Serve(ws *websocket.Conn) {
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		var msg pb.WskvMessage
		if err := proto.Unmarshal(data, &msg); err != nil {
			continue
		}
		resp := s.handle(&msg)
		if resp == nil {
			continue
		}
		out, err := proto.Marshal(resp)
		if err != nil {
			continue
		}
		ws.WriteMessage(websocket.BinaryMessage, out)
	}
}

func (s *WskvServer) handle(msg *pb.WskvMessage) *pb.WskvMessage {
	switch m := msg.Msg.(type) {
	case *pb.WskvMessage_GetReq:
		return s.handleGet(m.GetReq)
	case *pb.WskvMessage_ListReq:
		return s.handleList(m.ListReq)
	case *pb.WskvMessage_CommitReq:
		return s.handleCommit(m.CommitReq)
	case *pb.WskvMessage_ResetReq:
		return s.handleReset(m.ResetReq)
	}
	return nil
}

func (s *WskvServer) handleGet(req *pb.GetRequest) *pb.WskvMessage {
	s.mu.Lock()
	entry := s.store[string(req.Key)]
	s.mu.Unlock()

	resp := &pb.GetResponse{Id: req.Id}
	if entry != nil {
		resp.Value = entry.value
		resp.Ver = entry.ver
		resp.Found = true
	}
	return &pb.WskvMessage{Msg: &pb.WskvMessage_GetResp{GetResp: resp}}
}

func (s *WskvServer) handleList(req *pb.ListRequest) *pb.WskvMessage {
	s.mu.Lock()
	var keys []string
	start := string(req.Start)
	end := string(req.End)
	for k := range s.store {
		if k >= start && k < end {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	limit := int(req.Limit)
	entries := make([]*pb.Entry, 0, len(keys))
	for _, k := range keys {
		if limit > 0 && len(entries) >= limit {
			break
		}
		entry := s.store[k]
		e := &pb.Entry{
			Key: []byte(k),
			Ver: entry.ver,
		}
		if !req.KeysOnly {
			e.Value = entry.value
		}
		entries = append(entries, e)
	}
	s.mu.Unlock()

	return &pb.WskvMessage{Msg: &pb.WskvMessage_ListResp{ListResp: &pb.ListResponse{
		Id:      req.Id,
		Entries: entries,
	}}}
}

func (s *WskvServer) handleCommit(req *pb.CommitRequest) *pb.WskvMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check observed versions (OCC)
	for _, obs := range req.Observed {
		entry := s.store[string(obs.Key)]
		var curVer uint32
		if entry != nil {
			curVer = entry.ver
		}
		if curVer != obs.Ver {
			return &pb.WskvMessage{Msg: &pb.WskvMessage_CommitResp{CommitResp: &pb.CommitResponse{
				Id:    req.Id,
				Ok:    false,
				Error: "write conflict",
			}}}
		}
	}

	// Apply puts
	for _, put := range req.Puts {
		k := string(put.Key)
		if entry := s.store[k]; entry != nil {
			entry.ver++
			entry.value = put.Value
		} else {
			s.store[k] = &wskvServerEntry{value: put.Value, ver: 1}
		}
	}

	// Apply deletes
	for _, del := range req.Dels {
		delete(s.store, string(del))
	}

	return &pb.WskvMessage{Msg: &pb.WskvMessage_CommitResp{CommitResp: &pb.CommitResponse{
		Id: req.Id,
		Ok: true,
	}}}
}

func (s *WskvServer) handleReset(req *pb.ResetRequest) *pb.WskvMessage {
	s.mu.Lock()
	s.store = make(map[string]*wskvServerEntry)
	s.mu.Unlock()
	return &pb.WskvMessage{Msg: &pb.WskvMessage_ResetResp{ResetResp: &pb.ResetResponse{
		Id: req.Id,
		Ok: true,
	}}}
}

// sortedKeys returns all keys in the store matching [start, end), sorted.
// This is used for debugging/testing. The caller must hold s.mu.
func sortedKeys(store map[string]*wskvServerEntry, start, end string) []string {
	var keys []string
	for k := range store {
		if k >= start && (end == "" || k < end) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// Len returns the number of keys in the store. Useful for testing.
func (s *WskvServer) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.store)
}

// Keys returns all keys sorted. Useful for debugging.
func (s *WskvServer) Keys() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for k := range s.store {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	result := make([][]byte, len(keys))
	for i, k := range keys {
		result[i] = []byte(k)
	}
	return result
}

// Get returns a value and its version. Useful for testing.
func (s *WskvServer) Get(key []byte) ([]byte, uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.store[string(key)]
	if entry == nil {
		return nil, 0
	}
	return entry.value, entry.ver
}

// Scan iterates over keys with the given prefix. Useful for testing.
func (s *WskvServer) Scan(prefix []byte, handler func(key, value []byte) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	end := string(nextKey(prefix))
	keys := sortedKeys(s.store, string(prefix), end)
	for _, k := range keys {
		entry := s.store[k]
		if !handler([]byte(k), entry.value) {
			break
		}
	}
}

// Export returns a snapshot of all key-value pairs sorted by key. Useful for cloning.
func (s *WskvServer) Export() []KeyValue {
	s.mu.Lock()
	defer s.mu.Unlock()
	var kvs []KeyValue
	for k, entry := range s.store {
		kvs = append(kvs, KeyValue{Key: []byte(k), Value: entry.value})
	}
	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
	})
	return kvs
}

// Import loads key-value pairs into the store (used for cloning).
func (s *WskvServer) Import(kvs []KeyValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, kv := range kvs {
		s.store[string(kv.Key)] = &wskvServerEntry{value: kv.Value, ver: 1}
	}
}

// KeyValue is a key-value pair for export/import.
type KeyValue struct {
	Key   []byte
	Value []byte
}
