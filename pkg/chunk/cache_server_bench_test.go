//go:build linux

package chunk

import (
	"crypto/rand"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkSharedCacheRead benchmarks 3 concurrent readers hitting a shared
// cache server. 80 blocks are cached (the "hot" set) and 20 are not (the
// "cold" set). A weighted random picks hot blocks 80% of the time.
// This gives a realistic ~80% hit ratio on cached blocks via FD passing.
func BenchmarkSharedCacheRead(b *testing.B) {
	const (
		numReaders = 3
		blockSize  = 4 << 20 // 4MB blocks, matching JuiceFS default
		hotBlocks  = 80      // cached blocks
		coldBlocks = 20      // uncached blocks (will miss)
		cacheSize  = 500 << 20 // 500MB — comfortably fits all 80 hot blocks (320MB)
	)

	tmpDir := b.TempDir()
	dataSock := filepath.Join(tmpDir, "data.sock")
	ctrlSock := filepath.Join(tmpDir, "ctrl.sock")
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = cacheSize
	conf.BlockSize = blockSize
	conf.AutoCreate = true
	conf.CacheEviction = Eviction2Random

	cm := newCacheManager(&conf, nil, nil)
	server := NewCacheServer(cm, dataSock, ctrlSock, CsNone)
	if err := server.ListenAndServe(); err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer server.Close()
	// no sleep — DialUnix blocks until the listener accepts

	// Build keys for hot and cold blocks
	hotKeys := make([]string, hotBlocks)
	coldKeys := make([]string, coldBlocks)
	for i := 0; i < hotBlocks; i++ {
		hotKeys[i] = fmt.Sprintf("chunks/%02X/%d/%d_0_%d", i%256, i/1000, i, blockSize)
	}
	for i := 0; i < coldBlocks; i++ {
		coldKeys[i] = fmt.Sprintf("chunks/%02X/%d/%d_0_%d", (hotBlocks+i)%256, (hotBlocks+i)/1000, hotBlocks+i, blockSize)
	}

	// Cache hot blocks synchronously by writing directly to disk
	for _, key := range hotKeys {
		data := make([]byte, blockSize)
		rand.Read(data) //nolint:errcheck
		page := NewPage(data)
		page.Acquire()
		cm.cache(key, page, true, false)
	}
	// Wait for all pending flushes
	cm.(*cacheManager).waitPending()

	cnt, used := cm.stats()
	b.Logf("cache populated: %d blocks, %d MB used, %d MB capacity",
		cnt, used>>20, cacheSize>>20)
	if cnt < int64(hotBlocks/2) {
		b.Fatalf("too few blocks cached: %d, expected ~%d", cnt, hotBlocks)
	}

	// Connect readers
	clients := make([]CacheManager, numReaders)
	for i := 0; i < numReaders; i++ {
		c, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
		if err != nil {
			b.Fatalf("connect reader %d: %v", i, err)
		}
		clients[i] = c
	}

	// Warm FD cache — load each hot block once per client
	for _, c := range clients {
		for _, key := range hotKeys {
			if rc, err := c.load(key); err == nil {
				rc.Close()
			}
		}
	}

	var hits, misses atomic.Int64
	readBuf := make([]byte, blockSize)

	b.SetBytes(blockSize)
	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerReader := b.N / numReaders

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(client CacheManager, seed uint64) {
			defer wg.Done()
			rng := mathrand.New(mathrand.NewPCG(seed, seed+1))
			for i := 0; i < opsPerReader; i++ {
				var key string
				if rng.Float64() < 0.8 {
					key = hotKeys[rng.IntN(hotBlocks)]
				} else {
					key = coldKeys[rng.IntN(coldBlocks)]
				}
				rc, err := client.load(key)
				if err != nil {
					misses.Add(1)
					continue
				}
				rc.ReadAt(readBuf, 0)
				rc.Close()
				hits.Add(1)
			}
		}(clients[r], uint64(r*1000))
	}
	wg.Wait()
	b.StopTimer()

	totalOps := hits.Load() + misses.Load()
	hitRate := float64(hits.Load()) / float64(totalOps) * 100
	b.Logf("ops=%d hits=%d misses=%d hit_rate=%.1f%%",
		totalOps, hits.Load(), misses.Load(), hitRate)
}

// BenchmarkSharedCacheReadBaseline benchmarks reading directly from the local
// cacheManager (no server/client UDS overhead) for comparison.
func BenchmarkSharedCacheReadBaseline(b *testing.B) {
	const (
		numReaders = 3
		blockSize  = 4 << 20
		hotBlocks  = 80
		coldBlocks = 20
		cacheSize  = 500 << 20
	)

	tmpDir := b.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = cacheSize
	conf.BlockSize = blockSize
	conf.AutoCreate = true
	conf.CacheEviction = Eviction2Random

	cm := newCacheManager(&conf, nil, nil)
	defer func() {
		if m, ok := cm.(*cacheManager); ok {
			m.close()
		}
	}()

	hotKeys := make([]string, hotBlocks)
	coldKeys := make([]string, coldBlocks)
	for i := 0; i < hotBlocks; i++ {
		hotKeys[i] = fmt.Sprintf("chunks/%02X/%d/%d_0_%d", i%256, i/1000, i, blockSize)
		data := make([]byte, blockSize)
		rand.Read(data) //nolint:errcheck
		page := NewPage(data)
		page.Acquire()
		cm.cache(hotKeys[i], page, true, false)
	}
	for i := 0; i < coldBlocks; i++ {
		coldKeys[i] = fmt.Sprintf("chunks/%02X/%d/%d_0_%d", (hotBlocks+i)%256, (hotBlocks+i)/1000, hotBlocks+i, blockSize)
	}
	cm.(*cacheManager).waitPending()

	cnt, used := cm.stats()
	b.Logf("cache populated: %d blocks, %d MB used, %d MB capacity",
		cnt, used>>20, cacheSize>>20)

	var hits, misses atomic.Int64
	buf := make([]byte, blockSize)

	b.SetBytes(blockSize)
	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerReader := b.N / numReaders

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := mathrand.New(mathrand.NewPCG(seed, seed+1))
			for i := 0; i < opsPerReader; i++ {
				var key string
				if rng.Float64() < 0.8 {
					key = hotKeys[rng.IntN(hotBlocks)]
				} else {
					key = coldKeys[rng.IntN(coldBlocks)]
				}
				rc, err := cm.load(key)
				if err != nil {
					misses.Add(1)
					continue
				}
				rc.ReadAt(buf, 0)
				rc.Close()
				hits.Add(1)
			}
		}(uint64(r * 1000))
	}
	wg.Wait()
	b.StopTimer()

	totalOps := hits.Load() + misses.Load()
	hitRate := float64(hits.Load()) / float64(totalOps) * 100
	b.Logf("ops=%d hits=%d misses=%d hit_rate=%.1f%%",
		totalOps, hits.Load(), misses.Load(), hitRate)
}

// BenchmarkSharedCacheSmallRead benchmarks 3 concurrent readers doing 4KB reads
// from 4MB cached blocks. This isolates the protocol + FD overhead since the
// actual pread is tiny.
func BenchmarkSharedCacheSmallRead(b *testing.B) {
	const (
		numReaders = 3
		blockSize  = 4 << 20 // 4MB blocks
		readSize   = 4 << 10 // 4KB reads
		numBlocks  = 20      // small working set, all cached
		cacheSize  = 200 << 20
	)

	tmpDir := b.TempDir()
	dataSock := filepath.Join(tmpDir, "data.sock")
	ctrlSock := filepath.Join(tmpDir, "ctrl.sock")
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = cacheSize
	conf.BlockSize = blockSize
	conf.AutoCreate = true
	conf.CacheEviction = Eviction2Random

	cm := newCacheManager(&conf, nil, nil)
	server := NewCacheServer(cm, dataSock, ctrlSock, CsNone)
	if err := server.ListenAndServe(); err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer server.Close()

	keys := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		keys[i] = fmt.Sprintf("chunks/%02X/%d/%d_0_%d", i%256, i/1000, i, blockSize)
		data := make([]byte, blockSize)
		rand.Read(data) //nolint:errcheck
		page := NewPage(data)
		page.Acquire()
		cm.cache(keys[i], page, true, false)
	}
	cm.(*cacheManager).waitPending()

	clients := make([]CacheManager, numReaders)
	for i := 0; i < numReaders; i++ {
		c, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
		if err != nil {
			b.Fatalf("connect reader %d: %v", i, err)
		}
		clients[i] = c
	}

	// Warm FD cache
	for _, c := range clients {
		for _, key := range keys {
			if rc, err := c.load(key); err == nil {
				rc.Close()
			}
		}
	}

	var ops atomic.Int64
	buf := make([]byte, readSize)

	b.SetBytes(readSize)
	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerReader := b.N / numReaders

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(client CacheManager, seed uint64) {
			defer wg.Done()
			rng := mathrand.New(mathrand.NewPCG(seed, seed+1))
			for i := 0; i < opsPerReader; i++ {
				key := keys[rng.IntN(numBlocks)]
				rc, err := client.load(key)
				if err != nil {
					continue
				}
				// Read 4KB from a random offset within the 4MB block
				off := int64(rng.IntN(blockSize - readSize))
				rc.ReadAt(buf, off)
				rc.Close()
				ops.Add(1)
			}
		}(clients[r], uint64(r*1000))
	}
	wg.Wait()
	b.StopTimer()

	b.Logf("ops=%d", ops.Load())
}

// BenchmarkSharedCacheSmallMiss benchmarks 3 concurrent readers where every load
// is a cache miss — the requested blocks don't exist in the cache. This measures
// the pure protocol round-trip cost of a NOT_FOUND response.
func BenchmarkSharedCacheSmallMiss(b *testing.B) {
	const (
		numReaders = 3
		blockSize  = 4 << 20
		cacheSize  = 100 << 20
	)

	tmpDir := b.TempDir()
	dataSock := filepath.Join(tmpDir, "data.sock")
	ctrlSock := filepath.Join(tmpDir, "ctrl.sock")
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = cacheSize
	conf.BlockSize = blockSize
	conf.AutoCreate = true

	cm := newCacheManager(&conf, nil, nil)
	server := NewCacheServer(cm, dataSock, ctrlSock, CsNone)
	if err := server.ListenAndServe(); err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer server.Close()

	// Don't cache anything — all loads will miss

	clients := make([]CacheManager, numReaders)
	for i := 0; i < numReaders; i++ {
		c, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
		if err != nil {
			b.Fatalf("connect reader %d: %v", i, err)
		}
		clients[i] = c
	}

	var misses atomic.Int64

	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerReader := b.N / numReaders

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(client CacheManager, seed uint64) {
			defer wg.Done()
			for i := 0; i < opsPerReader; i++ {
				// Request a block that doesn't exist
				key := fmt.Sprintf("chunks/FF/%d/%d_0_%d", seed, uint64(i)+seed*1000000, blockSize)
				_, err := client.load(key)
				if err != nil {
					misses.Add(1)
				}
			}
		}(clients[r], uint64(r))
	}
	wg.Wait()
	b.StopTimer()
	b.Logf("misses=%d", misses.Load())
}

// BenchmarkSharedCacheSmallMissBaseline is the local-cache baseline for 100% miss.
func BenchmarkSharedCacheSmallMissBaseline(b *testing.B) {
	const (
		numReaders = 3
		blockSize  = 4 << 20
		cacheSize  = 100 << 20
	)

	tmpDir := b.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = cacheSize
	conf.BlockSize = blockSize
	conf.AutoCreate = true

	cm := newCacheManager(&conf, nil, nil)
	defer func() { cm.(*cacheManager).close() }()

	var misses atomic.Int64

	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerReader := b.N / numReaders

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			for i := 0; i < opsPerReader; i++ {
				key := fmt.Sprintf("chunks/FF/%d/%d_0_%d", seed, uint64(i)+seed*1000000, blockSize)
				_, err := cm.load(key)
				if err != nil {
					misses.Add(1)
				}
			}
		}(uint64(r))
	}
	wg.Wait()
	b.StopTimer()
	b.Logf("misses=%d", misses.Load())
}

// BenchmarkSharedCacheSmallReadBaseline is the local-cache baseline for small reads.
func BenchmarkSharedCacheSmallReadBaseline(b *testing.B) {
	const (
		numReaders = 3
		blockSize  = 4 << 20
		readSize   = 4 << 10
		numBlocks  = 20
		cacheSize  = 200 << 20
	)

	tmpDir := b.TempDir()
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = cacheSize
	conf.BlockSize = blockSize
	conf.AutoCreate = true
	conf.CacheEviction = Eviction2Random

	cm := newCacheManager(&conf, nil, nil)
	defer func() { cm.(*cacheManager).close() }()

	keys := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		keys[i] = fmt.Sprintf("chunks/%02X/%d/%d_0_%d", i%256, i/1000, i, blockSize)
		data := make([]byte, blockSize)
		rand.Read(data) //nolint:errcheck
		page := NewPage(data)
		page.Acquire()
		cm.cache(keys[i], page, true, false)
	}
	cm.(*cacheManager).waitPending()

	var ops atomic.Int64
	buf := make([]byte, readSize)

	b.SetBytes(readSize)
	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerReader := b.N / numReaders

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := mathrand.New(mathrand.NewPCG(seed, seed+1))
			for i := 0; i < opsPerReader; i++ {
				key := keys[rng.IntN(numBlocks)]
				rc, err := cm.load(key)
				if err != nil {
					continue
				}
				off := int64(rng.IntN(blockSize - readSize))
				rc.ReadAt(buf, off)
				rc.Close()
				ops.Add(1)
			}
		}(uint64(r * 1000))
	}
	wg.Wait()
	b.StopTimer()

	b.Logf("ops=%d", ops.Load())
}

// BenchmarkFDPassingOverhead measures the raw overhead of a single LOAD
// request through the cache server (FD already cached on client side).
func BenchmarkFDPassingOverhead(b *testing.B) {
	const blockSize = 4 << 20

	tmpDir := b.TempDir()
	dataSock := filepath.Join(tmpDir, "data.sock")
	ctrlSock := filepath.Join(tmpDir, "ctrl.sock")
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = 100 << 20
	conf.BlockSize = blockSize
	conf.AutoCreate = true

	cm := newCacheManager(&conf, nil, nil)
	server := NewCacheServer(cm, dataSock, ctrlSock, CsNone)
	if err := server.ListenAndServe(); err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer server.Close()
	// no sleep — DialUnix blocks until the listener accepts

	// Cache one block
	key := "chunks/00/0/0_0_4194304"
	data := make([]byte, blockSize)
	rand.Read(data) //nolint:errcheck
	page := NewPage(data)
	page.Acquire()
	cm.cache(key, page, true, false)
	cm.(*cacheManager).waitPending()

	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		b.Fatalf("connect: %v", err)
	}

	// Prime the FD cache
	rc, err := client.load(key)
	if err != nil {
		b.Fatalf("prime load: %v", err)
	}
	rc.Close()
	// no sleep — DialUnix blocks until the listener accepts

	buf := make([]byte, blockSize)
	b.SetBytes(blockSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rc, err := client.load(key)
		if err != nil {
			b.Fatalf("load: %v", err)
		}
		rc.ReadAt(buf, 0)
		rc.Close()
	}

	b.StopTimer()
	_ = math.E // ensure math import used
}

// BenchmarkShmipcRawPingPong measures the raw shmipc round-trip latency
// using the same server/client pattern as our cache server (SessionManager
// client + shmipc.Server() server). This is the achievable baseline.
func BenchmarkShmipcRawPingPong(b *testing.B) {
	tmpDir := b.TempDir()
	dataSock := filepath.Join(tmpDir, "data.sock")
	ctrlSock := filepath.Join(tmpDir, "ctrl.sock")
	cacheDir := filepath.Join(tmpDir, "cache")
	os.MkdirAll(cacheDir, 0755)

	// Use our actual cache server for the baseline — this measures the
	// full round-trip including protocol encode/decode overhead.
	conf := defaultConf
	conf.CacheDir = cacheDir
	conf.CacheSize = 10 << 20
	conf.AutoCreate = true

	cm := newCacheManager(&conf, nil, nil)
	server := NewCacheServer(cm, dataSock, ctrlSock, CsNone)
	if err := server.ListenAndServe(); err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer server.Close()

	client, err := newRemoteCacheManager(dataSock, ctrlSock, nil)
	if err != nil {
		b.Fatalf("connect: %v", err)
	}

	// All loads will miss — measures pure protocol round-trip
	key := "chunks/FF/999/999_0_4096"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.load(key) //nolint:errcheck
	}
	b.StopTimer()
}
