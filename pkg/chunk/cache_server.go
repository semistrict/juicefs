//go:build linux

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

package chunk

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"github.com/cloudwego/shmipc-go"
	"github.com/prometheus/client_golang/prometheus"
)

type sentFDInfo struct {
	fileLength int
}

type clientState struct {
	mu       sync.Mutex
	sentFDs  map[string]sentFDInfo
	ctrlConn *net.UnixConn
	id       string
}

func newClientState(id string, ctrlConn *net.UnixConn) *clientState {
	return &clientState{
		sentFDs:  make(map[string]sentFDInfo),
		ctrlConn: ctrlConn,
		id:       id,
	}
}

// CacheServer wraps an existing CacheManager and serves it over
// a UDS control channel (for FD passing) and shmipc data channel.
type CacheServer struct {
	cache    CacheManager
	ctrlSock string
	dataSock string
	ctrlLn   *net.UnixListener
	dataLn   *net.UnixListener
	shmConf  *shmipc.Config
	mu       sync.Mutex
	clients  map[string]*clientState
	closed   chan struct{}
	checksum string
}

func NewCacheServer(cache CacheManager, dataSock, ctrlSock, checksum string) *CacheServer {
	return &CacheServer{
		cache:    cache,
		ctrlSock: ctrlSock,
		dataSock: dataSock,
		clients:  make(map[string]*clientState),
		closed:   make(chan struct{}),
		checksum: checksum,
	}
}

func NewCacheServerWithConfig(config *Config, reg prometheus.Registerer, dataSock, ctrlSock string) *CacheServer {
	cm := newCacheManager(config, reg, nil)
	return NewCacheServer(cm, dataSock, ctrlSock, config.CacheChecksum)
}

func (s *CacheServer) ListenAndServe() error {
	os.Remove(s.ctrlSock)
	os.Remove(s.dataSock)

	ctrlAddr, err := net.ResolveUnixAddr("unix", s.ctrlSock)
	if err != nil {
		return fmt.Errorf("resolve ctrl addr: %w", err)
	}
	s.ctrlLn, err = net.ListenUnix("unix", ctrlAddr)
	if err != nil {
		return fmt.Errorf("listen ctrl: %w", err)
	}

	dataAddr, err := net.ResolveUnixAddr("unix", s.dataSock)
	if err != nil {
		s.ctrlLn.Close()
		return fmt.Errorf("resolve data addr: %w", err)
	}
	s.dataLn, err = net.ListenUnix("unix", dataAddr)
	if err != nil {
		s.ctrlLn.Close()
		return fmt.Errorf("listen data: %w", err)
	}

	if s.shmConf == nil {
		s.shmConf = shmipc.DefaultConfig()
		s.shmConf.ShareMemoryBufferCap = 32 << 20
	}

	go s.acceptCtrl()
	go s.acceptData()
	return nil
}

func (s *CacheServer) Close() {
	select {
	case <-s.closed:
		return
	default:
	}
	close(s.closed)
	if s.ctrlLn != nil {
		s.ctrlLn.Close()
	}
	if s.dataLn != nil {
		s.dataLn.Close()
	}
	s.mu.Lock()
	for _, cs := range s.clients {
		cs.ctrlConn.Close()
	}
	s.mu.Unlock()
	if cm, ok := s.cache.(*cacheManager); ok {
		cm.close()
	}
	os.Remove(s.ctrlSock)
	os.Remove(s.dataSock)
}

func (s *CacheServer) acceptCtrl() {
	for {
		conn, err := s.ctrlLn.AcceptUnix()
		if err != nil {
			select {
			case <-s.closed:
				return
			default:
				logger.Warnf("cache server: accept ctrl: %s", err)
				continue
			}
		}
		go s.handleCtrlConn(conn)
	}
}

func (s *CacheServer) handleCtrlConn(conn *net.UnixConn) {
	clientID, err := readClientID(conn)
	if err != nil {
		logger.Warnf("cache server: read client id (ctrl): %s", err)
		conn.Close()
		return
	}

	s.mu.Lock()
	cs := newClientState(clientID, conn)
	s.clients[clientID] = cs
	s.mu.Unlock()

	logger.Infof("cache server: client %s connected (ctrl)", clientID)

	tmp := make([]byte, 1)
	for {
		_, err := conn.Read(tmp)
		if err != nil {
			break
		}
	}

	logger.Infof("cache server: client %s disconnected (ctrl)", clientID)
	s.mu.Lock()
	delete(s.clients, clientID)
	s.mu.Unlock()
	conn.Close()
}

func (s *CacheServer) acceptData() {
	for {
		conn, err := s.dataLn.AcceptUnix()
		if err != nil {
			select {
			case <-s.closed:
				return
			default:
				logger.Warnf("cache server: accept data: %s", err)
				continue
			}
		}
		go s.handleDataConn(conn)
	}
}

func (s *CacheServer) handleDataConn(conn *net.UnixConn) {
	session, err := shmipc.Server(conn, s.shmConf)
	if err != nil {
		logger.Warnf("cache server: shmipc server session: %s", err)
		conn.Close()
		return
	}
	defer session.Close()

	// Accept streams and handle each with a persistent read loop
	for {
		select {
		case <-s.closed:
			return
		default:
		}

		stream, err := session.AcceptStream()
		if err != nil {
			logger.Debugf("cache server: accept stream: %s", err)
			return
		}
		go s.handleDataStream(stream)
	}
}

// handleDataStream runs a sync read loop on a persistent stream.
// Each iteration reads one request, processes it, writes the response.
func (s *CacheServer) handleDataStream(stream *shmipc.Stream) {
	defer stream.Close()
	var req protoRequest
	var clientID string // read once from first request, reused for all
	for {
		select {
		case <-s.closed:
			return
		default:
		}

		reader := stream.BufferReader()
		req.Op = 0
		req.Flags = 0
		req.ClientID = ""
		req.Key = ""
		req.Payload = nil
		if err := decodeRequestFromBufInto(reader, &req); err != nil {
			return
		}
		reader.ReleasePreviousRead()

		// First request carries the client ID; subsequent ones may omit it
		if req.ClientID != "" {
			clientID = req.ClientID
		}
		req.ClientID = clientID

		w := stream.BufferWriter()
		if err := s.handleAndWriteResponse(&req, w); err != nil {
			logger.Warnf("cache server: write response: %s", err)
			return
		}
		if err := stream.Flush(false); err != nil {
			logger.Warnf("cache server: flush response: %s", err)
			return
		}
	}
}

func (s *CacheServer) getClient(clientID string) *clientState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clients[clientID]
}

// handleAndWriteResponse processes a request and writes the response directly
// into shared memory, avoiding intermediate protoResponse allocations.
func (s *CacheServer) handleAndWriteResponse(req *protoRequest, w shmipc.BufferWriter) error {
	switch req.Op {
	case opLoad:
		return s.handleLoadDirect(req.ClientID, req, w)
	default:
		resp := s.handleRequest(req.ClientID, req)
		return encodeResponseToShm(resp, w)
	}
}

func (s *CacheServer) handleRequest(clientID string, req *protoRequest) *protoResponse {
	switch req.Op {
	case opCache:
		return s.handleCache(req)
	case opExist:
		return s.handleExist(req)
	case opRemove:
		return s.handleRemove(req)
	case opStage:
		return s.handleStage(req)
	case opUploaded:
		return s.handleUploaded(req)
	case opStats:
		return s.handleStats()
	case opRemoveStag:
		return s.handleRemoveStage(req)
	default:
		return &protoResponse{Status: statusError, Payload: []byte("unknown op")}
	}
}

// handleLoadDirect writes the LOAD response directly into shared memory,
// avoiding protoResponse and encodeUint64 heap allocations on the hot path.
func (s *CacheServer) handleLoadDirect(clientID string, req *protoRequest, w shmipc.BufferWriter) error {
	cs := s.getClient(clientID)

	// Fast path: FD already sent — zero alloc response
	if cs != nil {
		cs.mu.Lock()
		info, alreadySent := cs.sentFDs[req.Key]
		cs.mu.Unlock()
		if alreadySent {
			return writeResponseToShm(w, statusOK, flagFDCached, uint64(info.fileLength))
		}
	}

	// Slow path: load from cache and send FD
	rc, err := s.cache.load(req.Key)
	if err != nil {
		return writeResponseToShm(w, statusNotFound, 0)
	}

	cf, ok := rc.(*cacheFile)
	if !ok {
		rc.Close()
		return encodeResponseToShm(&protoResponse{Status: statusError, Payload: []byte("not a file-backed cache entry")}, w)
	}

	size := cf.length

	if cs == nil {
		cf.Close()
		return encodeResponseToShm(&protoResponse{Status: statusError, Payload: []byte("no control connection for client")}, w)
	}

	cs.mu.Lock()
	fd := int(cf.File.Fd())
	msg := make([]byte, 2+len(req.Key)+8)
	binary.LittleEndian.PutUint16(msg[0:2], uint16(len(req.Key)))
	copy(msg[2:2+len(req.Key)], req.Key)
	binary.LittleEndian.PutUint64(msg[2+len(req.Key):], uint64(size))
	err = sendFD(cs.ctrlConn, msg, fd)
	if err != nil {
		cs.mu.Unlock()
		cf.Close()
		logger.Warnf("cache server: send FD to %s: %s", clientID, err)
		return encodeResponseToShm(&protoResponse{Status: statusError, Payload: []byte(err.Error())}, w)
	}
	cs.sentFDs[req.Key] = sentFDInfo{fileLength: size}
	cs.mu.Unlock()
	cf.Close()
	return writeResponseToShm(w, statusOK, flagFDSent, uint64(size))
}

func (s *CacheServer) handleLoad(clientID string, req *protoRequest) *protoResponse {
	cs := s.getClient(clientID)

	if cs != nil {
		cs.mu.Lock()
		info, alreadySent := cs.sentFDs[req.Key]
		cs.mu.Unlock()
		if alreadySent {
			return &protoResponse{
				Status:  statusOK,
				Flags:   flagFDCached,
				Payload: encodeUint64(uint64(info.fileLength)),
			}
		}
	}

	rc, err := s.cache.load(req.Key)
	if err != nil {
		return &protoResponse{Status: statusNotFound}
	}

	cf, ok := rc.(*cacheFile)
	if !ok {
		rc.Close()
		return &protoResponse{Status: statusError, Payload: []byte("not a file-backed cache entry")}
	}

	size := cf.length

	if cs == nil {
		cf.Close()
		return &protoResponse{Status: statusError, Payload: []byte("no control connection for client")}
	}

	cs.mu.Lock()
	fd := int(cf.File.Fd())
	msg := make([]byte, 2+len(req.Key)+8)
	binary.LittleEndian.PutUint16(msg[0:2], uint16(len(req.Key)))
	copy(msg[2:2+len(req.Key)], req.Key)
	binary.LittleEndian.PutUint64(msg[2+len(req.Key):], uint64(size))
	err = sendFD(cs.ctrlConn, msg, fd)
	if err != nil {
		cs.mu.Unlock()
		cf.Close()
		logger.Warnf("cache server: send FD to %s: %s", clientID, err)
		return &protoResponse{Status: statusError, Payload: []byte(err.Error())}
	}
	cs.sentFDs[req.Key] = sentFDInfo{fileLength: size}
	cs.mu.Unlock()
	cf.Close()

	return &protoResponse{
		Status:  statusOK,
		Flags:   flagFDSent,
		Payload: encodeUint64(uint64(size)),
	}
}

func (s *CacheServer) handleCache(req *protoRequest) *protoResponse {
	if len(req.Payload) < 2 {
		return &protoResponse{Status: statusError, Payload: []byte("payload too short")}
	}
	force := req.Payload[0] != 0
	dropCache := req.Payload[1] != 0
	data := req.Payload[2:]

	page := NewPage(data)
	s.cache.cache(req.Key, page, force, dropCache)
	return &protoResponse{Status: statusOK}
}

func (s *CacheServer) handleExist(req *protoRequest) *protoResponse {
	loc, ok := s.cache.exist(req.Key)
	if !ok {
		return &protoResponse{Status: statusNotFound}
	}
	return &protoResponse{Status: statusOK, Payload: []byte(loc)}
}

func (s *CacheServer) handleRemove(req *protoRequest) *protoResponse {
	staging := false
	if len(req.Payload) > 0 {
		staging = req.Payload[0] != 0
	}
	s.cache.remove(req.Key, staging)

	s.mu.Lock()
	for _, cs := range s.clients {
		cs.mu.Lock()
		delete(cs.sentFDs, req.Key)
		cs.mu.Unlock()
	}
	s.mu.Unlock()

	return &protoResponse{Status: statusOK}
}

func (s *CacheServer) handleStage(req *protoRequest) *protoResponse {
	if len(req.Payload) < 1 {
		return &protoResponse{Status: statusError, Payload: []byte("payload too short")}
	}
	tierID := req.Payload[0]
	data := req.Payload[1:]

	path, err := s.cache.stage(req.Key, data, tierID)
	if err != nil {
		return &protoResponse{Status: statusError, Payload: []byte(err.Error())}
	}
	return &protoResponse{Status: statusOK, Payload: []byte(path)}
}

func (s *CacheServer) handleUploaded(req *protoRequest) *protoResponse {
	if len(req.Payload) < 4 {
		return &protoResponse{Status: statusError, Payload: []byte("payload too short")}
	}
	size := int(decodeUint32(req.Payload[:4]))
	s.cache.uploaded(req.Key, size)
	return &protoResponse{Status: statusOK}
}

func (s *CacheServer) handleStats() *protoResponse {
	cnt, used := s.cache.stats()
	payload := make([]byte, 16)
	binary.LittleEndian.PutUint64(payload[0:8], uint64(cnt))
	binary.LittleEndian.PutUint64(payload[8:16], uint64(used))
	return &protoResponse{Status: statusOK, Payload: payload}
}

func (s *CacheServer) handleRemoveStage(req *protoRequest) *protoResponse {
	err := s.cache.removeStage(req.Key)
	if err != nil {
		return &protoResponse{Status: statusError, Payload: []byte(err.Error())}
	}
	return &protoResponse{Status: statusOK}
}

type fdReadCloser struct {
	f      *os.File
	length int
}

func newFDReadCloser(fd int, length int) *fdReadCloser {
	f := os.NewFile(uintptr(fd), "cache-fd")
	return &fdReadCloser{f: f, length: length}
}

func (r *fdReadCloser) ReadAt(b []byte, off int64) (int, error) {
	return syscall.Pread(int(r.f.Fd()), b, off)
}

func (r *fdReadCloser) Close() error {
	return r.f.Close()
}
