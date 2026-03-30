//go:build !windows

package chunk

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProtoRoundTrip(t *testing.T) {
	t.Run("request encode/decode", func(t *testing.T) {
		req := &protoRequest{
			Op:      opLoad,
			Flags:   0x01,
			Key:     "chunks/00/1/1_0_4194304",
			Payload: []byte("hello"),
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
	// Create a temp file to pass
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "testdata")
	data := []byte("hello from FD passing")
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	// Create a UDS pair
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
	fdCh := make(chan int, 1)

	// Server side: accept and send FD
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

		msg := []byte("FD")
		err = sendFD(conn, msg, int(f.Fd()))
		errCh <- err
	}()

	// Client side: connect and receive FD
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
	fdCh <- fds[0]

	// Read through the received FD
	fd := <-fdCh
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

func TestCacheServerClientRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	dataSock := filepath.Join(tmpDir, "data.sock")
	ctrlSock := filepath.Join(tmpDir, "ctrl.sock")
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = 10 << 20
	conf.AutoCreate = true

	cm := newCacheManager(&conf, nil, nil)
	server := NewCacheServer(cm, dataSock, ctrlSock, CsNone)
	if err := server.ListenAndServe(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer server.Close()

	// Give server a moment to start accepting
	time.Sleep(50 * time.Millisecond)

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

		// Cache a block
		page := NewPage(data)
		page.Acquire() // extra ref since cache() takes ownership
		client.cache(key, page, true, false)

		// Wait for async flush
		time.Sleep(200 * time.Millisecond)

		// Load it back
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

		// First load should send FD
		rc1, err := client.load(key)
		if err != nil {
			t.Fatalf("load 1: %v", err)
		}
		rc1.Close()

		// Second load should reuse cached FD (FD_CACHED path)
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

		// Wait for removal
		time.Sleep(100 * time.Millisecond)

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
	tmpDir := t.TempDir()
	dataSock := filepath.Join(tmpDir, "data.sock")
	ctrlSock := filepath.Join(tmpDir, "ctrl.sock")
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = 10 << 20
	conf.AutoCreate = true

	cm := newCacheManager(&conf, nil, nil)
	server := NewCacheServer(cm, dataSock, ctrlSock, CsNone)
	if err := server.ListenAndServe(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer server.Close()
	time.Sleep(50 * time.Millisecond)

	// Connect two clients
	client1, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect client1: %v", err)
	}
	client2, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		t.Fatalf("connect client2: %v", err)
	}

	// Client 1 writes
	key := "chunks/00/2/2_0_512"
	data := make([]byte, 512)
	for i := range data {
		data[i] = byte(i % 256)
	}
	page := NewPage(data)
	page.Acquire()
	client1.cache(key, page, true, false)
	time.Sleep(200 * time.Millisecond)

	// Client 2 reads
	rc, err := client2.load(key)
	if err != nil {
		t.Fatalf("client2 load: %v", err)
	}
	buf := make([]byte, 512)
	n, err := rc.ReadAt(buf, 0)
	rc.Close()
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
