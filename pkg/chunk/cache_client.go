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
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

var errServerUnavailable = errors.New("cache server unavailable")

// remoteCacheManager implements CacheManager by forwarding requests
// to a cache server over UDS. FDs for cache reads are received over
// a separate control channel and cached locally for reuse.
type remoteCacheManager struct {
	dataSock string
	ctrlSock string
	clientID string

	dataConn *net.UnixConn // data channel for protocol messages
	ctrlConn *net.UnixConn // control channel for receiving FDs

	dataMu sync.Mutex // serializes data channel requests

	fdMu    sync.RWMutex
	fdCache map[string]int // key -> file descriptor (kept open)

	metrics *cacheManagerMetrics

	ctrlDone chan struct{}
}

// newRemoteCacheManager connects to a cache server.
func newRemoteCacheManager(dataSock, ctrlSock string, reg prometheus.Registerer) (CacheManager, error) {
	clientID := uuid.New().String()
	metrics := newCacheManagerMetrics(reg)

	m := &remoteCacheManager{
		dataSock: dataSock,
		ctrlSock: ctrlSock,
		clientID: clientID,
		fdCache:  make(map[string]int),
		metrics:  metrics,
		ctrlDone: make(chan struct{}),
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
	// Send client ID
	if _, err := ctrlConn.Write([]byte(m.clientID)); err != nil {
		ctrlConn.Close()
		return fmt.Errorf("send client id (ctrl): %w", err)
	}
	m.ctrlConn = ctrlConn

	// Connect data channel
	dataAddr, err := net.ResolveUnixAddr("unix", m.dataSock)
	if err != nil {
		ctrlConn.Close()
		return fmt.Errorf("resolve data: %w", err)
	}
	dataConn, err := net.DialUnix("unix", nil, dataAddr)
	if err != nil {
		ctrlConn.Close()
		return fmt.Errorf("dial data: %w", err)
	}
	if _, err := dataConn.Write([]byte(m.clientID)); err != nil {
		ctrlConn.Close()
		dataConn.Close()
		return fmt.Errorf("send client id (data): %w", err)
	}
	m.dataConn = dataConn

	return nil
}

// recvFDs runs in a goroutine, receiving FDs from the control channel
// and storing them in the local fdCache.
func (m *remoteCacheManager) recvFDs() {
	defer close(m.ctrlDone)
	for {
		msg, fds, err := recvFD(m.ctrlConn, 1)
		if err != nil {
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

		m.fdMu.Lock()
		if oldFD, exists := m.fdCache[key]; exists {
			closefd(oldFD)
		}
		m.fdCache[key] = fds[0]
		m.fdMu.Unlock()
		// Close extra FDs if any
		for i := 1; i < len(fds); i++ {
			closefd(fds[i])
		}
	}
}

func closefd(fd int) {
	os.NewFile(uintptr(fd), "").Close()
}

// sendRequest sends a request and reads the response, holding the data lock.
func (m *remoteCacheManager) sendRequest(req *protoRequest) (*protoResponse, error) {
	m.dataMu.Lock()
	defer m.dataMu.Unlock()

	if m.dataConn == nil {
		return nil, errServerUnavailable
	}

	if _, err := m.dataConn.Write(encodeRequest(req)); err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	return decodeResponse(m.dataConn)
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

	// Also remove from local FD cache
	m.fdMu.Lock()
	if fd, ok := m.fdCache[key]; ok {
		closefd(fd)
		delete(m.fdCache, key)
	}
	m.fdMu.Unlock()
}

func (m *remoteCacheManager) load(key string) (ReadCloser, error) {
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
	size := int(decodeUint64(resp.Payload[:8]))

	if resp.Flags == flagFDSent {
		// FD is being sent over control channel — wait for it.
		// The recvFDs goroutine will populate fdCache asynchronously.
		// The FD was sent before the data response, so it should arrive quickly.
		for i := 0; i < 200; i++ {
			m.fdMu.RLock()
			fd, ok := m.fdCache[key]
			m.fdMu.RUnlock()
			if ok {
				return newFDReadCloserNonOwning(fd, size), nil
			}
			runtime.Gosched()
			if i > 10 {
				time.Sleep(time.Millisecond)
			}
		}
		return nil, fmt.Errorf("remote cache: load %s: FD not received", key)
	}

	// FD_CACHED: we should already have it
	m.fdMu.RLock()
	fd, ok := m.fdCache[key]
	m.fdMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("remote cache: load %s: FD_CACHED but no local FD", key)
	}
	return newFDReadCloserNonOwning(fd, size), nil
}

// fdReadCloserNonOwning wraps a file descriptor for ReadAt without owning it.
// Close() is a no-op since the FD is cached for reuse.
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
	// Don't close — FD is cached for reuse
	return nil
}

// readAtFD does a pread on a raw file descriptor.
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
	resp, err := m.sendRequest(&protoRequest{
		Op:  opExist,
		Key: key,
	})
	if err != nil {
		return "", false
	}
	if resp.Status != statusOK {
		return "", false
	}
	return string(resp.Payload), true
}

func (m *remoteCacheManager) uploaded(key string, size int) {
	payload := encodeUint32(uint32(size))
	_, err := m.sendRequest(&protoRequest{
		Op:      opUploaded,
		Key:     key,
		Payload: payload,
	})
	if err != nil {
		logger.Warnf("remote cache: uploaded %s: %s", key, err)
	}
}

func (m *remoteCacheManager) stage(key string, data []byte, tierID uint8) (string, error) {
	payload := make([]byte, 1+len(data))
	payload[0] = tierID
	copy(payload[1:], data)

	resp, err := m.sendRequest(&protoRequest{
		Op:      opStage,
		Key:     key,
		Payload: payload,
	})
	if err != nil {
		return "", err
	}
	if resp.Status != statusOK {
		return "", fmt.Errorf("remote cache: stage %s: %s", key, string(resp.Payload))
	}
	return string(resp.Payload), nil
}

func (m *remoteCacheManager) removeStage(key string) error {
	resp, err := m.sendRequest(&protoRequest{
		Op:  opRemoveStag,
		Key: key,
	})
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
	cnt := int64(decodeUint64(resp.Payload[0:8]))
	used := int64(decodeUint64(resp.Payload[8:16]))
	return cnt, used
}

func (m *remoteCacheManager) usedMemory() int64 {
	return 0
}

func (m *remoteCacheManager) isEmpty() bool {
	// Remote cache is never "empty" in the sense that triggers fallback to memStore.
	// The server manages its own cache directory lifecycle.
	return false
}

func (m *remoteCacheManager) getMetrics() *cacheManagerMetrics {
	return m.metrics
}
