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
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/cloudwego/shmipc-go"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

var errServerUnavailable = errors.New("cache server unavailable")

type fdEntry struct {
	fd   int
	size int
}

const defaultStreamPoolSize = 4

// remoteCacheManager implements CacheManager by forwarding requests to a
// cache server over shmipc. A pool of persistent streams provides concurrency.
// FDs for cache reads are received over a separate UDS control channel.
type remoteCacheManager struct {
	dataSock string
	ctrlSock string
	clientID string

	shmMgr     *shmipc.SessionManager // session management
	streamPool chan *shmipc.Stream     // pool of persistent streams
	idSent     sync.Map               // stream ptr → bool: whether clientID was sent on this stream
	ctrlConn   *net.UnixConn          // control channel for receiving FDs
	spinMicros int                     // polling spin duration in μs (0=disabled)

	fdMu    sync.RWMutex
	fdCache map[string]fdEntry // key -> cached FD + size

	metrics *cacheManagerMetrics

	ctrlDone chan struct{}
}

func newRemoteCacheManager(dataSock, ctrlSock string, reg prometheus.Registerer) (CacheManager, error) {
	return newRemoteCacheManagerWithSpin(dataSock, ctrlSock, reg, 0)
}

func newRemoteCacheManagerWithSpin(dataSock, ctrlSock string, reg prometheus.Registerer, spinMicros int) (CacheManager, error) {
	clientID := uuid.New().String()
	metrics := newCacheManagerMetrics(reg)

	m := &remoteCacheManager{
		dataSock:   dataSock,
		ctrlSock:   ctrlSock,
		clientID:   clientID,
		fdCache:    make(map[string]fdEntry),
		metrics:    metrics,
		ctrlDone:   make(chan struct{}),
		spinMicros: spinMicros,
	}

	if err := m.connect(); err != nil {
		return nil, err
	}

	go m.recvFDs()
	return m, nil
}

func (m *remoteCacheManager) connect() error {
	// Connect control channel
	ctrlAddr, err := net.ResolveUnixAddr("unix", m.ctrlSock)
	if err != nil {
		return fmt.Errorf("resolve ctrl: %w", err)
	}
	ctrlConn, err := net.DialUnix("unix", nil, ctrlAddr)
	if err != nil {
		return fmt.Errorf("dial ctrl: %w", err)
	}
	if err := writeClientID(ctrlConn, m.clientID); err != nil {
		ctrlConn.Close()
		return fmt.Errorf("send client id (ctrl): %w", err)
	}
	m.ctrlConn = ctrlConn

	// Connect shmipc data channel
	cfg := shmipc.DefaultSessionManagerConfig()
	cfg.Network = "unix"
	cfg.Address = m.dataSock
	cfg.SessionNum = 1
	cfg.MaxStreamNum = 64
	cfg.ShareMemoryPathPrefix = fmt.Sprintf("/dev/shm/jfs_cache_%s", m.clientID)
	cfg.QueuePath = fmt.Sprintf("/dev/shm/jfs_cache_%s_queue", m.clientID)
	cfg.ShareMemoryBufferCap = 32 << 20
	if m.spinMicros > 0 {
		cfg.PollingSpinDuration = time.Duration(m.spinMicros) * time.Microsecond
	}
	shmMgr, err := shmipc.NewSessionManager(cfg)
	if err != nil {
		ctrlConn.Close()
		return fmt.Errorf("shmipc session manager: %w", err)
	}
	m.shmMgr = shmMgr

	// Pre-open persistent streams — one per CPU for parallelism
	poolSize := runtime.NumCPU()
	if poolSize < 1 {
		poolSize = 1
	}
	if poolSize > defaultStreamPoolSize {
		poolSize = defaultStreamPoolSize
	}
	m.streamPool = make(chan *shmipc.Stream, poolSize)
	for i := 0; i < poolSize; i++ {
		stream, err := shmMgr.GetStream()
		if err != nil {
			ctrlConn.Close()
			shmMgr.Close()
			return fmt.Errorf("open stream %d: %w", i, err)
		}
		m.streamPool <- stream
	}

	return nil
}

func (m *remoteCacheManager) recvFDs() {
	defer close(m.ctrlDone)
	for {
		// Set a deadline so Recvmsg unblocks periodically, allowing
		// close() to terminate this goroutine.
		m.ctrlConn.SetReadDeadline(time.Now().Add(time.Second))
		msg, fds, err := recvFD(m.ctrlConn, 1)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // deadline expired, loop and try again
			}
			logger.Debugf("remote cache: recv FD: %s", err)
			return
		}
		if len(fds) == 0 || len(msg) < 2 {
			continue
		}
		keyLen := binary.LittleEndian.Uint16(msg[0:2])
		if int(keyLen)+2 > len(msg) {
			for _, fd := range fds {
				closefd(fd)
			}
			continue
		}
		key := string(msg[2 : 2+keyLen])

		fileSize := 0
		if int(keyLen)+2+8 <= len(msg) {
			fileSize = int(binary.LittleEndian.Uint64(msg[2+keyLen : 2+keyLen+8]))
		}

		m.fdMu.Lock()
		if old, exists := m.fdCache[key]; exists {
			closefd(old.fd)
		}
		m.fdCache[key] = fdEntry{fd: fds[0], size: fileSize}
		m.fdMu.Unlock()
		for i := 1; i < len(fds); i++ {
			closefd(fds[i])
		}
	}
}

func closefd(fd int) {
	os.NewFile(uintptr(fd), "").Close()
}

// sendRequest gets a persistent stream from the pool, writes the request
// directly into shared memory, reads the response, then returns the stream.
func (m *remoteCacheManager) sendRequest(req *protoRequest) (*protoResponse, error) {
	if m.streamPool == nil {
		return nil, errServerUnavailable
	}

	stream := <-m.streamPool

	// If the stream's session is dead (server crashed), don't write to
	// unmapped shared memory — return the stream and report error.
	if !stream.Session().IsHealthy() {
		m.streamPool <- stream // return to pool (caller can retry or close)
		return nil, errServerUnavailable
	}

	// Send client ID only on the first request per stream
	if _, sent := m.idSent.LoadOrStore(stream, true); !sent {
		req.ClientID = m.clientID
	}

	if err := encodeRequestToShm(req, stream.BufferWriter()); err != nil {
		stream.Close()
		return nil, fmt.Errorf("send request: %w", err)
	}
	if err := stream.Flush(false); err != nil {
		stream.Close()
		return nil, fmt.Errorf("flush request: %w", err)
	}

	reader := stream.BufferReader()
	resp, err := decodeResponseFromBuf(reader)
	if err != nil {
		stream.Close()
		return nil, fmt.Errorf("read response: %w", err)
	}
	reader.ReleasePreviousRead()

	m.streamPool <- stream
	return resp, nil
}

func (m *remoteCacheManager) cache(key string, p *Page, force, dropCache bool) {
	payload := make([]byte, 2+len(p.Data))
	if force {
		payload[0] = 1
	}
	if dropCache {
		payload[1] = 1
	}
	copy(payload[2:], p.Data)

	_, err := m.sendRequest(&protoRequest{
		Op:      opCache,
		Key:     key,
		Payload: payload,
	})
	if err != nil {
		logger.Warnf("remote cache: cache %s: %s", key, err)
	}
}

func (m *remoteCacheManager) remove(key string, staging bool) {
	payload := []byte{0}
	if staging {
		payload[0] = 1
	}
	_, err := m.sendRequest(&protoRequest{
		Op:      opRemove,
		Key:     key,
		Payload: payload,
	})
	if err != nil {
		logger.Warnf("remote cache: remove %s: %s", key, err)
	}

	m.fdMu.Lock()
	if entry, ok := m.fdCache[key]; ok {
		closefd(entry.fd)
		delete(m.fdCache, key)
	}
	m.fdMu.Unlock()
}

func (m *remoteCacheManager) load(key string) (ReadCloser, error) {
	// Fast path: FD already cached locally — skip the server round-trip entirely.
	m.fdMu.RLock()
	if entry, ok := m.fdCache[key]; ok {
		m.fdMu.RUnlock()
		return newFDReadCloserNonOwning(entry.fd, entry.size), nil
	}
	m.fdMu.RUnlock()

	resp, err := m.sendRequest(&protoRequest{
		Op:  opLoad,
		Key: key,
	})
	if err != nil {
		return nil, err
	}
	if resp.Status == statusNotFound {
		return nil, errNotCached
	}
	if resp.Status != statusOK {
		return nil, fmt.Errorf("remote cache: load %s: %s", key, string(resp.Payload))
	}

	if len(resp.Payload) < 8 {
		return nil, fmt.Errorf("remote cache: load %s: short payload", key)
	}

	if resp.Flags == flagFDSent {
		for i := 0; i < 200; i++ {
			m.fdMu.RLock()
			entry, ok := m.fdCache[key]
			m.fdMu.RUnlock()
			if ok {
				return newFDReadCloserNonOwning(entry.fd, entry.size), nil
			}
			runtime.Gosched()
			if i > 10 {
				time.Sleep(time.Millisecond)
			}
		}
		return nil, fmt.Errorf("remote cache: load %s: FD not received", key)
	}

	m.fdMu.RLock()
	entry, ok := m.fdCache[key]
	m.fdMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("remote cache: load %s: FD_CACHED but no local FD", key)
	}
	return newFDReadCloserNonOwning(entry.fd, entry.size), nil
}

type fdReadCloserNonOwning struct {
	fd     int
	length int
}

func newFDReadCloserNonOwning(fd int, length int) *fdReadCloserNonOwning {
	return &fdReadCloserNonOwning{fd: fd, length: length}
}

func (r *fdReadCloserNonOwning) ReadAt(b []byte, off int64) (int, error) {
	return readAtFD(r.fd, b, off)
}

func (r *fdReadCloserNonOwning) Close() error {
	return nil
}

func readAtFD(fd int, b []byte, off int64) (int, error) {
	var total int
	for total < len(b) {
		n, err := pread(fd, b[total:], off+int64(total))
		if n > 0 {
			total += n
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, fmt.Errorf("pread returned 0")
		}
	}
	return total, nil
}

func (m *remoteCacheManager) exist(key string) (string, bool) {
	resp, err := m.sendRequest(&protoRequest{Op: opExist, Key: key})
	if err != nil {
		return "", false
	}
	if resp.Status != statusOK {
		return "", false
	}
	return string(resp.Payload), true
}

func (m *remoteCacheManager) uploaded(key string, size int) {
	_, err := m.sendRequest(&protoRequest{
		Op:      opUploaded,
		Key:     key,
		Payload: encodeUint32(uint32(size)),
	})
	if err != nil {
		logger.Warnf("remote cache: uploaded %s: %s", key, err)
	}
}

func (m *remoteCacheManager) stage(key string, data []byte, tierID uint8) (string, error) {
	payload := make([]byte, 1+len(data))
	payload[0] = tierID
	copy(payload[1:], data)

	resp, err := m.sendRequest(&protoRequest{Op: opStage, Key: key, Payload: payload})
	if err != nil {
		return "", err
	}
	if resp.Status != statusOK {
		return "", fmt.Errorf("remote cache: stage %s: %s", key, string(resp.Payload))
	}
	return string(resp.Payload), nil
}

func (m *remoteCacheManager) removeStage(key string) error {
	resp, err := m.sendRequest(&protoRequest{Op: opRemoveStag, Key: key})
	if err != nil {
		return err
	}
	if resp.Status != statusOK {
		return fmt.Errorf("remote cache: removeStage %s: %s", key, string(resp.Payload))
	}
	return nil
}

func (m *remoteCacheManager) stats() (int64, int64) {
	resp, err := m.sendRequest(&protoRequest{Op: opStats})
	if err != nil {
		return 0, 0
	}
	if resp.Status != statusOK || len(resp.Payload) < 16 {
		return 0, 0
	}
	return int64(decodeUint64(resp.Payload[0:8])), int64(decodeUint64(resp.Payload[8:16]))
}

func (m *remoteCacheManager) usedMemory() int64 { return 0 }
func (m *remoteCacheManager) isEmpty() bool      { return false }

func (m *remoteCacheManager) close() {
	if m.ctrlConn != nil {
		m.ctrlConn.Close()
	}
	<-m.ctrlDone
	if m.streamPool != nil {
		close(m.streamPool)
		for s := range m.streamPool {
			s.Close()
		}
	}
	if m.shmMgr != nil {
		m.shmMgr.Close()
	}
	m.fdMu.Lock()
	for key, entry := range m.fdCache {
		closefd(entry.fd)
		delete(m.fdCache, key)
	}
	m.fdMu.Unlock()
}

func (m *remoteCacheManager) getMetrics() *cacheManagerMetrics {
	return m.metrics
}
