//go:build !windows

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

	"github.com/prometheus/client_golang/prometheus"
)

// clientState tracks per-client state including which FDs have been sent.
type clientState struct {
	mu       sync.Mutex
	sentFDs  map[string]struct{} // cache keys for which FD was already sent
	ctrlConn *net.UnixConn       // UDS control connection for FD passing
	id       string
}

func newClientState(id string, ctrlConn *net.UnixConn) *clientState {
	return &clientState{
		sentFDs:  make(map[string]struct{}),
		ctrlConn: ctrlConn,
		id:       id,
	}
}

// CacheServer wraps an existing CacheManager and serves it over
// a UDS control channel (for FD passing) and a data channel.
type CacheServer struct {
	cache     CacheManager
	ctrlSock  string // path for UDS control socket (FD passing)
	dataSock  string // path for data socket (protocol messages + bulk data)
	ctrlLn    *net.UnixListener
	dataLn    *net.UnixListener
	mu        sync.Mutex
	clients   map[string]*clientState
	closed    chan struct{}
	checksum  string
}

// NewCacheServer creates a new cache server wrapping the given CacheManager.
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

// NewCacheServerWithConfig creates a CacheServer by first creating a local cacheManager.
func NewCacheServerWithConfig(config *Config, reg prometheus.Registerer, dataSock, ctrlSock string) *CacheServer {
	cm := newCacheManager(config, reg, nil)
	return NewCacheServer(cm, dataSock, ctrlSock, config.CacheChecksum)
}

// ListenAndServe starts both the data and control listeners.
func (s *CacheServer) ListenAndServe() error {
	// Clean up old socket files
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

	go s.acceptCtrl()
	go s.acceptData()
	return nil
}

// Close shuts down the server and the underlying cache manager.
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
	// Stop the cache manager's background goroutines
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
	// Read client ID (first message on control channel)
	buf := make([]byte, 128)
	n, err := conn.Read(buf)
	if err != nil {
		logger.Warnf("cache server: read client id: %s", err)
		conn.Close()
		return
	}
	clientID := string(buf[:n])

	s.mu.Lock()
	cs := newClientState(clientID, conn)
	s.clients[clientID] = cs
	s.mu.Unlock()

	logger.Infof("cache server: client %s connected (ctrl)", clientID)

	// Keep the control connection alive until the client disconnects.
	// The control channel is only used for server→client FD sends,
	// triggered by the data channel handler. We just wait for EOF/error.
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
	defer conn.Close()

	// First message: client ID to correlate with ctrl connection
	buf := make([]byte, 128)
	n, err := conn.Read(buf)
	if err != nil {
		logger.Warnf("cache server: read client id on data: %s", err)
		return
	}
	clientID := string(buf[:n])
	logger.Infof("cache server: client %s connected (data)", clientID)

	for {
		select {
		case <-s.closed:
			return
		default:
		}

		req, err := decodeRequest(conn)
		if err != nil {
			if err.Error() != "EOF" {
				logger.Debugf("cache server: client %s read request: %s", clientID, err)
			}
			return
		}

		resp := s.handleRequest(clientID, req)
		if _, err := conn.Write(encodeResponse(resp)); err != nil {
			logger.Warnf("cache server: client %s write response: %s", clientID, err)
			return
		}
	}
}

func (s *CacheServer) getClient(clientID string) *clientState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clients[clientID]
}

func (s *CacheServer) handleRequest(clientID string, req *protoRequest) *protoResponse {
	switch req.Op {
	case opLoad:
		return s.handleLoad(clientID, req)
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

func (s *CacheServer) handleLoad(clientID string, req *protoRequest) *protoResponse {
	cs := s.getClient(clientID)

	// Try to load from cache
	rc, err := s.cache.load(req.Key)
	if err != nil {
		return &protoResponse{Status: statusNotFound}
	}

	// Check if we already sent this FD to this client
	if cs != nil {
		cs.mu.Lock()
		_, alreadySent := cs.sentFDs[req.Key]
		cs.mu.Unlock()

		if alreadySent {
			// Client already has the FD, just tell them the size
			if cf, ok := rc.(*cacheFile); ok {
				size := cf.length
				cf.Close()
				return &protoResponse{
					Status:  statusOK,
					Flags:   flagFDCached,
					Payload: encodeUint64(uint64(size)),
				}
			}
			rc.Close()
			return &protoResponse{Status: statusError, Payload: []byte("not a file-backed cache entry")}
		}
	}

	// Need to send the FD
	cf, ok := rc.(*cacheFile)
	if !ok {
		// Memory-backed entry, can't pass FD. Send data inline.
		rc.Close()
		return &protoResponse{Status: statusError, Payload: []byte("not a file-backed cache entry")}
	}

	size := cf.length

	if cs != nil {
		cs.mu.Lock()
		// Send FD over the control channel
		fd := int(cf.File.Fd())
		// Send the key length + key as the msg alongside the FD so client can correlate
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
		cs.sentFDs[req.Key] = struct{}{}
		cs.mu.Unlock()
	}

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
	// Don't release page here — cache() takes ownership via pending channel
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

	// Invalidate FD tracking for all clients for this key
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

// fdReadCloser wraps an os.File created from a received file descriptor.
// It implements the ReadCloser interface used by the cache system.
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
