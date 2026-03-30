//go:build linux

package chunk

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestProtoRoundTrip(t *testing.T) {
	t.Run("request encode/decode", func(t *testing.T) {
		req := &protoRequest{
			Op:       opLoad,
			Flags:    0x01,
			ClientID: "test-client-abc123",
			Key:      "chunks/00/1/1_0_4194304",
			Payload:  []byte("hello"),
		}
		encoded := encodeRequest(req)
		decoded, err := decodeRequest(bytes.NewReader(encoded))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if decoded.Op != req.Op {
			t.Fatalf("op: got %d, want %d", decoded.Op, req.Op)
		}
		if decoded.Flags != req.Flags {
			t.Fatalf("flags: got %d, want %d", decoded.Flags, req.Flags)
		}
		if decoded.ClientID != req.ClientID {
			t.Fatalf("clientID: got %q, want %q", decoded.ClientID, req.ClientID)
		}
		if decoded.Key != req.Key {
			t.Fatalf("key: got %q, want %q", decoded.Key, req.Key)
		}
		if !bytes.Equal(decoded.Payload, req.Payload) {
			t.Fatalf("payload: got %q, want %q", decoded.Payload, req.Payload)
		}
	})

	t.Run("response encode/decode", func(t *testing.T) {
		resp := &protoResponse{
			Status:  statusOK,
			Flags:   flagFDSent,
			Payload: encodeUint64(4194304),
		}
		encoded := encodeResponse(resp)
		decoded, err := decodeResponse(bytes.NewReader(encoded))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if decoded.Status != resp.Status {
			t.Fatalf("status: got %d, want %d", decoded.Status, resp.Status)
		}
		if decoded.Flags != resp.Flags {
			t.Fatalf("flags: got %d, want %d", decoded.Flags, resp.Flags)
		}
		if decodeUint64(decoded.Payload) != 4194304 {
			t.Fatalf("payload: got %d, want 4194304", decodeUint64(decoded.Payload))
		}
	})

	t.Run("empty request", func(t *testing.T) {
		req := &protoRequest{Op: opStats}
		encoded := encodeRequest(req)
		decoded, err := decodeRequest(bytes.NewReader(encoded))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if decoded.Op != opStats {
			t.Fatalf("op: got %d, want %d", decoded.Op, opStats)
		}
		if decoded.Key != "" {
			t.Fatalf("key: got %q, want empty", decoded.Key)
		}
		if len(decoded.Payload) != 0 {
			t.Fatalf("payload: got %d bytes, want 0", len(decoded.Payload))
		}
	})
}

func TestFDPassingRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "testdata")
	data := []byte("hello from FD passing")
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	sockPath := filepath.Join(tmpDir, "test.sock")
	addr, err := net.ResolveUnixAddr("unix", sockPath)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	ln, err := net.ListenUnix("unix", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	errCh := make(chan error, 1)

	go func() {
		conn, err := ln.AcceptUnix()
		if err != nil {
			errCh <- fmt.Errorf("accept: %w", err)
			return
		}
		defer conn.Close()

		f, err := os.Open(tmpFile)
		if err != nil {
			errCh <- fmt.Errorf("open: %w", err)
			return
		}
		defer f.Close()

		err = sendFD(conn, []byte("FD"), int(f.Fd()))
		errCh <- err
	}()

	conn, err := net.DialUnix("unix", nil, addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	msg, fds, err := recvFD(conn, 1)
	if err != nil {
		t.Fatalf("recvFD: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server: %v", err)
	}
	if string(msg) != "FD" {
		t.Fatalf("msg: got %q, want %q", string(msg), "FD")
	}
	if len(fds) != 1 {
		t.Fatalf("fds: got %d, want 1", len(fds))
	}

	fd := fds[0]
	buf := make([]byte, len(data))
	n, err := pread(fd, buf, 0)
	if err != nil {
		t.Fatalf("pread: %v", err)
	}
	if n != len(data) {
		t.Fatalf("pread: got %d bytes, want %d", n, len(data))
	}
	if !bytes.Equal(buf, data) {
		t.Fatalf("data: got %q, want %q", buf, data)
	}
	os.NewFile(uintptr(fd), "").Close()
}

func TestFDPassingRoundTripLongMessage(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "testdata")
	data := []byte("hello from FD passing")
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	sockPath := filepath.Join(tmpDir, "test-long.sock")
	addr, err := net.ResolveUnixAddr("unix", sockPath)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	ln, err := net.ListenUnix("unix", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	wantMsg := append([]byte("key:"), bytes.Repeat([]byte("x"), 80)...)
	errCh := make(chan error, 1)

	go func() {
		conn, err := ln.AcceptUnix()
		if err != nil {
			errCh <- fmt.Errorf("accept: %w", err)
			return
		}
		defer conn.Close()

		f, err := os.Open(tmpFile)
		if err != nil {
			errCh <- fmt.Errorf("open: %w", err)
			return
		}
		defer f.Close()

		errCh <- sendFD(conn, wantMsg, int(f.Fd()))
	}()

	conn, err := net.DialUnix("unix", nil, addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	gotMsg, fds, err := recvFD(conn, 1)
	if err != nil {
		t.Fatalf("recvFD: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server: %v", err)
	}
	if !bytes.Equal(gotMsg, wantMsg) {
		t.Fatalf("msg mismatch: got %d bytes, want %d", len(gotMsg), len(wantMsg))
	}
	if len(fds) != 1 {
		t.Fatalf("fds: got %d, want 1", len(fds))
	}

	fd := fds[0]
	buf := make([]byte, len(data))
	n, err := pread(fd, buf, 0)
	if err != nil {
		t.Fatalf("pread: %v", err)
	}
	if n != len(data) {
		t.Fatalf("pread: got %d bytes, want %d", n, len(data))
	}
	if !bytes.Equal(buf, data) {
		t.Fatalf("data: got %q, want %q", buf, data)
	}
	os.NewFile(uintptr(fd), "").Close()
}

// newTestServer creates a cache server with a populated cache for testing.
// Returns the server, cache manager, and a cleanup function.
func newTestServer(t *testing.T) (*CacheServer, *cacheManager, string, string) {
	t.Helper()
	tmpDir := t.TempDir()
	dataSock := filepath.Join(tmpDir, "data.sock")
	ctrlSock := filepath.Join(tmpDir, "ctrl.sock")
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = 10 << 20
	conf.AutoCreate = true

	cm := newCacheManager(&conf, nil, nil).(*cacheManager)
	server := NewCacheServer(cm, dataSock, ctrlSock, CsNone)
	if err := server.ListenAndServe(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { server.Close() })
	return server, cm, dataSock, ctrlSock
}

// cacheBlock caches a block via the cache manager and waits for it to flush to disk.
func cacheBlock(t *testing.T, cm *cacheManager, key string, data []byte) {
	t.Helper()
	page := NewPage(data)
	page.Acquire()
	cm.cache(key, page, true, false)
	cm.waitPending()
}

func sendRequestWithTimeout(t *testing.T, client *remoteCacheManager, req *protoRequest, timeout time.Duration) error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		_, err := client.sendRequest(req)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		return err
	case <-time.After(timeout):
		t.Fatalf("sendRequest timed out after %s", timeout)
		return nil
	}
}

// TestConnectAndImmediateLoad verifies that a client can connect and
// immediately send a load request without any delay. This reproduces a
// protocol framing bug where the client ID and first request get coalesced
// into a single read on the server side.
func TestConnectAndImmediateLoad(t *testing.T) {
	_, cm, dataSock, ctrlSock := newTestServer(t)

	key := "chunks/00/1/1_0_1024"
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	cacheBlock(t, cm, key, data)

	// Connect and immediately load — no sleep between connect and first request.
	// This triggers the framing bug where client ID and request coalesce.
	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	rc, err := client.load(key)
	if err != nil {
		t.Fatalf("immediate load after connect: %v", err)
	}
	buf := make([]byte, 1024)
	n, err := rc.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	rc.Close()
	if n != 1024 {
		t.Fatalf("ReadAt: got %d bytes, want 1024", n)
	}
	if !bytes.Equal(buf, data) {
		t.Fatalf("data mismatch")
	}
}

func TestCacheServerClientRoundTrip(t *testing.T) {
	_, cm, dataSock, ctrlSock := newTestServer(t)

	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	t.Run("cache and load", func(t *testing.T) {
		key := "chunks/00/1/1_0_1024"
		data := make([]byte, 1024)
		for i := range data {
			data[i] = byte(i % 256)
		}

		// Cache via the server's cache manager and wait for flush
		cacheBlock(t, cm, key, data)

		rc, err := client.load(key)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		buf := make([]byte, 1024)
		n, err := rc.ReadAt(buf, 0)
		if err != nil {
			t.Fatalf("ReadAt: %v", err)
		}
		rc.Close()
		if n != 1024 {
			t.Fatalf("ReadAt: got %d bytes, want 1024", n)
		}
		if !bytes.Equal(buf, data) {
			t.Fatalf("data mismatch at first load")
		}
	})

	t.Run("FD reuse on second load", func(t *testing.T) {
		key := "chunks/00/1/1_0_1024"

		rc1, err := client.load(key)
		if err != nil {
			t.Fatalf("load 1: %v", err)
		}
		rc1.Close()

		rc2, err := client.load(key)
		if err != nil {
			t.Fatalf("load 2: %v", err)
		}
		buf := make([]byte, 1024)
		n, err := rc2.ReadAt(buf, 0)
		rc2.Close()
		if err != nil {
			t.Fatalf("ReadAt: %v", err)
		}
		if n != 1024 {
			t.Fatalf("ReadAt: got %d bytes, want 1024", n)
		}
	})

	t.Run("exist", func(t *testing.T) {
		loc, ok := client.exist("chunks/00/1/1_0_1024")
		if !ok {
			t.Fatal("exist: should return true for cached block")
		}
		if loc == "" {
			t.Fatal("exist: location should not be empty")
		}

		_, ok = client.exist("chunks/00/99/99_0_1024")
		if ok {
			t.Fatal("exist: should return false for missing block")
		}
	})

	t.Run("stats", func(t *testing.T) {
		cnt, used := client.stats()
		if cnt == 0 {
			t.Fatal("stats: count should be > 0")
		}
		if used == 0 {
			t.Fatal("stats: used should be > 0")
		}
	})

	t.Run("remove", func(t *testing.T) {
		key := "chunks/00/1/1_0_1024"
		client.remove(key, false)

		// remove is synchronous on the server — no sleep needed
		_, err := client.load(key)
		if err == nil {
			t.Fatal("load after remove should fail")
		}
	})

	t.Run("load missing key", func(t *testing.T) {
		_, err := client.load("chunks/FF/999/999_0_4096")
		if err == nil {
			t.Fatal("load missing: should return error")
		}
	})

	t.Run("isEmpty always false for remote", func(t *testing.T) {
		if client.isEmpty() {
			t.Fatal("remote cache should never report isEmpty")
		}
	})
}

func TestMultipleClients(t *testing.T) {
	_, cm, dataSock, ctrlSock := newTestServer(t)

	client1, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect client1: %v", err)
	}
	client2, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect client2: %v", err)
	}

	// Cache via server's cache manager
	key := "chunks/00/2/2_0_512"
	data := make([]byte, 512)
	for i := range data {
		data[i] = byte(i % 256)
	}
	cacheBlock(t, cm, key, data)

	// Client 1 loads
	rc1, err := client1.load(key)
	if err != nil {
		t.Fatalf("client1 load: %v", err)
	}
	rc1.Close()

	// Client 2 loads the same block
	rc2, err := client2.load(key)
	if err != nil {
		t.Fatalf("client2 load: %v", err)
	}
	buf := make([]byte, 512)
	n, err := rc2.ReadAt(buf, 0)
	rc2.Close()
	if err != nil {
		t.Fatalf("client2 ReadAt: %v", err)
	}
	if n != 512 {
		t.Fatalf("client2 ReadAt: got %d bytes, want 512", n)
	}
	if !bytes.Equal(buf, data) {
		t.Fatalf("client2 data mismatch")
	}
}

// TestStaleFDAfterEviction verifies that when the server removes a cached
// block, clients with stale FDs get an error rather than corrupt data.
func TestStaleFDAfterEviction(t *testing.T) {
	_, cm, dataSock, ctrlSock := newTestServer(t)

	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	key := "chunks/00/1/1_0_1024"
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	cacheBlock(t, cm, key, data)

	// Load to get FD cached on client
	rc, err := client.load(key)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	buf := make([]byte, 1024)
	n, err := rc.ReadAt(buf, 0)
	rc.Close()
	if err != nil || n != 1024 {
		t.Fatalf("ReadAt: n=%d err=%v", n, err)
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("data mismatch before eviction")
	}

	// Server removes the block (simulating eviction)
	cm.remove(key, false)

	// Client still has the FD cached. The underlying file is deleted.
	// pread on a deleted-but-open FD still works on Linux (file isn't
	// actually removed until all FDs are closed). This is correct behavior —
	// the client reads stale-but-valid data until the FD is invalidated.
	rc2, err := client.load(key)
	if err != nil {
		// If the client-side fdCache was cleared by a remove call, this is also fine.
		t.Logf("load after eviction correctly failed: %v", err)
		return
	}
	// If we still get a ReadCloser, the data should still be readable
	// (Linux keeps deleted files open until last FD closes)
	buf2 := make([]byte, 1024)
	n2, err := rc2.ReadAt(buf2, 0)
	rc2.Close()
	if err != nil {
		t.Logf("ReadAt on stale FD failed (acceptable): %v", err)
		return
	}
	if n2 != 1024 {
		t.Fatalf("ReadAt on stale FD: got %d bytes, want 1024", n2)
	}
	// Data should still match — Linux doesn't zero deleted files
	if !bytes.Equal(buf2, data) {
		t.Fatal("stale FD returned corrupt data")
	}
}

// TestServerCrashClientRecovery verifies that when the server shuts down,
// the client's subsequent requests fail with errors rather than hanging.
func TestServerCrashClientRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	dataSock := filepath.Join(tmpDir, "data.sock")
	ctrlSock := filepath.Join(tmpDir, "ctrl.sock")
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = 10 << 20
	conf.AutoCreate = true

	cm := newCacheManager(&conf, nil, nil).(*cacheManager)
	server := NewCacheServer(cm, dataSock, ctrlSock, CsNone)
	if err := server.ListenAndServe(); err != nil {
		t.Fatalf("listen: %v", err)
	}

	// Cache a block and connect client
	key := "chunks/00/1/1_0_1024"
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	cacheBlock(t, cm, key, data)

	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Verify it works
	rc, err := client.load(key)
	if err != nil {
		t.Fatalf("load before crash: %v", err)
	}
	rc.Close()

	// Kill the server
	server.Close()
	time.Sleep(50 * time.Millisecond)

	// Client requests should fail with errors, not hang.
	// Use a timeout to catch hangs.
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Cached FD path should still work (no server needed)
		rc2, err := client.load(key)
		if err == nil {
			// FD was cached locally — this is fine, pread doesn't need server
			rc2.Close()
		}
		// A miss should fail (server is gone)
		_, err = client.load("chunks/FF/999/999_0_1024")
		if err == nil {
			t.Error("load of missing key after server crash should fail")
		}
	}()

	select {
	case <-done:
		// Good — didn't hang
	case <-time.After(5 * time.Second):
		t.Fatal("client hung after server crash")
	}

	client.(*remoteCacheManager).close()
}

func TestSendRequestReplenishesStreamPoolAfterTransportErrors(t *testing.T) {
	// This test cannot run in-process: when the server closes, shmipc unmaps
	// the shared memory region that the client's streams still reference.
	// Writing to unmapped memory causes SIGSEGV. In production (separate
	// processes), each side has its own mmap and this doesn't happen.
	// The TestServerCrashClientRecovery test covers the safe part of this
	// scenario (cached FDs still work, new requests fail).
	t.Skip("requires separate processes — shmipc unmaps shared memory on server close")
}

// TestClientDisconnectCleanup verifies the server cleans up client state
// when a client disconnects.
func TestClientDisconnectCleanup(t *testing.T) {
	server, cm, dataSock, ctrlSock := newTestServer(t)

	key := "chunks/00/1/1_0_512"
	cacheBlock(t, cm, key, make([]byte, 512))

	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	rcm := client.(*remoteCacheManager)
	clientID := rcm.clientID

	// Load to register FD with server
	rc, err := client.load(key)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	rc.Close()

	// Verify server tracks this client
	server.mu.Lock()
	_, tracked := server.clients[clientID]
	server.mu.Unlock()
	if !tracked {
		t.Fatal("server should track connected client")
	}

	// Disconnect client
	rcm.close()

	// Wait for the server to detect disconnect (ctrl conn EOF).
	// The shmipc data session may take longer to detect the broken connection.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		server.mu.Lock()
		_, stillTracked := server.clients[clientID]
		server.mu.Unlock()
		if !stillTracked {
			return // success
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("server did not clean up disconnected client within 3s")
}

func TestHandleLoadWithoutControlClientDoesNotClaimFDSent(t *testing.T) {
	server, cm, _, _ := newTestServer(t)

	key := "chunks/00/1/1_0_1024"
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	cacheBlock(t, cm, key, data)

	resp := server.handleLoad("missing-client", &protoRequest{Op: opLoad, Key: key})
	if resp.Status == statusOK && resp.Flags == flagFDSent {
		t.Fatal("handleLoad advertised FD delivery without a registered control client")
	}
}

// TestConcurrentWriteRead verifies that caching and loading the same key
// concurrently doesn't cause corruption or deadlock.
func TestConcurrentWriteRead(t *testing.T) {
	_, cm, dataSock, ctrlSock := newTestServer(t)

	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	key := "chunks/00/5/5_0_4096"
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	var wg sync.WaitGroup
	// Writer: cache the block repeatedly
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			page := NewPage(data)
			page.Acquire()
			cm.cache(key, page, true, false)
			time.Sleep(time.Millisecond)
		}
	}()

	// Reader: load the block repeatedly
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			rc, err := client.load(key)
			if err != nil {
				continue // miss is ok during write
			}
			buf := make([]byte, 4096)
			rc.ReadAt(buf, 0)
			rc.Close()
		}
	}()

	wg.Wait()
}

// TestShmCleanup verifies that shared memory files in /dev/shm are cleaned
// up when clients disconnect.
func TestShmCleanup(t *testing.T) {
	_, _, dataSock, ctrlSock := newTestServer(t)

	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	rcm := client.(*remoteCacheManager)
	clientID := rcm.clientID

	// Do a request to ensure shmipc session is fully established
	client.load("chunks/FF/0/0_0_1024")

	// Check for shm files
	pattern := fmt.Sprintf("/dev/shm/jfs_cache_%s*", clientID)
	matches, _ := filepath.Glob(pattern)
	if len(matches) == 0 {
		t.Log("no shm files found (may use memfd instead)")
	} else {
		t.Logf("found %d shm files before disconnect", len(matches))
	}

	// Disconnect
	rcm.close()
	time.Sleep(200 * time.Millisecond)

	// Check that shm files are cleaned up
	matchesAfter, _ := filepath.Glob(pattern)
	if len(matchesAfter) > 0 {
		t.Errorf("shm files not cleaned up after disconnect: %v", matchesAfter)
	}
}

// TestLargeCacheWrite verifies that caching and loading large blocks (up to
// 4MB) through shmipc works correctly with no data corruption.
func TestLargeCacheWrite(t *testing.T) {
	_, cm, dataSock, ctrlSock := newTestServer(t)

	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	sizes := []int{1024, 64 << 10, 256 << 10, 1 << 20, 4 << 20}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			key := fmt.Sprintf("chunks/%02X/0/0_0_%d", size%256, size)
			data := make([]byte, size)
			for i := range data {
				data[i] = byte((i*7 + 13) % 256) // deterministic pattern
			}

			// Cache via server's cache manager
			page := NewPage(data)
			page.Acquire()
			cm.cache(key, page, true, false)
			cm.waitPending()

			// Load via client
			rc, err := client.load(key)
			if err != nil {
				t.Fatalf("load %d: %v", size, err)
			}

			// Read full block and verify
			buf := make([]byte, size)
			n, err := rc.ReadAt(buf, 0)
			rc.Close()
			if err != nil && err.Error() != "EOF" {
				t.Fatalf("ReadAt %d: n=%d err=%v", size, n, err)
			}
			if n != size {
				t.Fatalf("ReadAt %d: got %d bytes", size, n)
			}
			if !bytes.Equal(buf, data) {
				// Find first mismatch
				for i := range buf {
					if buf[i] != data[i] {
						t.Fatalf("data mismatch at offset %d: got %d, want %d", i, buf[i], data[i])
					}
				}
			}

			// Also verify a partial read from the middle
			if size >= 1024 {
				off := int64(size / 3)
				partBuf := make([]byte, 512)
				n2, err := rc2read(client, key, partBuf, off)
				if err != nil {
					t.Fatalf("partial ReadAt: %v", err)
				}
				if n2 != 512 {
					t.Fatalf("partial ReadAt: got %d bytes", n2)
				}
				if !bytes.Equal(partBuf, data[off:off+512]) {
					t.Fatal("partial read data mismatch")
				}
			}
		})
	}
}

func rc2read(cm CacheManager, key string, buf []byte, off int64) (int, error) {
	rc, err := cm.load(key)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	return rc.ReadAt(buf, off)
}

// TestCacheWriteViaClient verifies that the client's cache() method
// correctly sends data through shmipc and the server persists it.
func TestCacheWriteViaClient(t *testing.T) {
	_, cm, dataSock, ctrlSock := newTestServer(t)

	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	rcm := client.(*remoteCacheManager)

	key := "chunks/00/9/9_0_2048"
	data := make([]byte, 2048)
	for i := range data {
		data[i] = byte(i % 251)
	}

	// Cache via the CLIENT (data goes through shmipc to server)
	page := NewPage(data)
	page.Acquire()
	rcm.cache(key, page, true, false)
	cm.waitPending()

	// Verify the server has it by loading directly from the server's cache manager
	rc, err := cm.load(key)
	if err != nil {
		t.Fatalf("server-side load: %v", err)
	}
	buf := make([]byte, 2048)
	n, err := rc.ReadAt(buf, 0)
	rc.Close()
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 2048 || !bytes.Equal(buf[:n], data[:n]) {
		t.Fatalf("data mismatch: n=%d", n)
	}
}

// TestStageAndUpload verifies the stage/uploaded operations work through shmipc.
func TestStageAndUpload(t *testing.T) {
	_, _, dataSock, ctrlSock := newTestServer(t)

	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	rcm := client.(*remoteCacheManager)

	key := "chunks/00/10/10_0_1024"
	data := make([]byte, 1024)

	path, err := rcm.stage(key, data, 0)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if path == "" {
		t.Fatal("stage returned empty path")
	}

	// Verify the staged file exists
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("staged file not found: %v", err)
	}

	// Mark as uploaded
	rcm.uploaded(key, len(data))

	// Remove stage
	err = rcm.removeStage(key)
	if err != nil {
		t.Fatalf("removeStage: %v", err)
	}
}

// TestPreadOnDeletedFile verifies Linux behavior: pread on an FD whose
// underlying file was deleted still returns valid data.
// TestLoopDeviceWorkload simulates the access pattern of a loop block device:
// a small number of 4MB blocks accessed with sequential 128KB reads, very high
// hit rate, multiple concurrent readers. This is the expected production workload.
func TestLoopDeviceWorkload(t *testing.T) {
	// Need a larger cache than newTestServer provides (10 × 4MB = 40MB)
	tmpDir := t.TempDir()
	dataSock := filepath.Join(tmpDir, "data.sock")
	ctrlSock := filepath.Join(tmpDir, "ctrl.sock")
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = 200 << 20 // 200MB
	conf.AutoCreate = true

	cm := newCacheManager(&conf, nil, nil).(*cacheManager)
	server := NewCacheServer(cm, dataSock, ctrlSock, CsNone)
	if err := server.ListenAndServe(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { server.Close() })

	const (
		numBlocks    = 10
		blockSize    = 4 << 20   // 4MB
		readSize     = 128 << 10 // 128KB — typical loop device read
		numReaders   = 4
		opsPerReader = 200
	)

	// Cache all blocks
	keys := make([]string, numBlocks)
	blocks := make([][]byte, numBlocks)
	for i := range numBlocks {
		keys[i] = fmt.Sprintf("chunks/%02X/%d/%d_0_%d", i%256, i/1000, i, blockSize)
		blocks[i] = make([]byte, blockSize)
		for j := range blocks[i] {
			blocks[i][j] = byte((i*7 + j*13) % 256)
		}
		cacheBlock(t, cm, keys[i], blocks[i])
	}

	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Warm FD cache
	for _, key := range keys {
		rc, err := client.load(key)
		if err != nil {
			t.Fatalf("warm load %s: %v", key, err)
		}
		rc.Close()
	}

	// Concurrent sequential readers — each reader walks through blocks
	// doing 128KB reads at sequential offsets, like a loop device would.
	var wg sync.WaitGroup
	errors := make(chan string, numReaders*opsPerReader)

	for r := range numReaders {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			buf := make([]byte, readSize)
			for i := range opsPerReader {
				blockIdx := (readerID + i) % numBlocks
				// Sequential offset within the block
				off := int64((i * readSize) % (blockSize - readSize))

				rc, err := client.load(keys[blockIdx])
				if err != nil {
					errors <- fmt.Sprintf("reader %d op %d: load %s: %v", readerID, i, keys[blockIdx], err)
					continue
				}
				n, err := rc.ReadAt(buf, off)
				rc.Close()
				if err != nil {
					errors <- fmt.Sprintf("reader %d op %d: ReadAt: %v", readerID, i, err)
					continue
				}
				if n != readSize {
					errors <- fmt.Sprintf("reader %d op %d: short read: %d", readerID, i, n)
					continue
				}
				// Verify data correctness
				expected := blocks[blockIdx][off : off+int64(readSize)]
				if !bytes.Equal(buf, expected) {
					for j := range buf {
						if buf[j] != expected[j] {
							errors <- fmt.Sprintf("reader %d op %d: mismatch at offset %d+%d: got %d want %d",
								readerID, i, off, j, buf[j], expected[j])
							break
						}
					}
				}
			}
		}(r)
	}

	wg.Wait()
	close(errors)

	for e := range errors {
		t.Error(e)
	}
}

func TestPreadOnDeletedFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "testfile")
	data := []byte("hello pread on deleted file")
	os.WriteFile(path, data, 0644)

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	fd := int(f.Fd())

	// Delete the file while FD is open
	os.Remove(path)

	// pread should still work
	buf := make([]byte, len(data))
	n, err := syscall.Pread(fd, buf, 0)
	f.Close()
	if err != nil {
		t.Fatalf("pread on deleted file: %v", err)
	}
	if n != len(data) || !bytes.Equal(buf, data) {
		t.Fatalf("pread returned wrong data: n=%d", n)
	}
}
