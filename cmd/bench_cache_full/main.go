//go:build linux

package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	mathrand "math/rand/v2"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
)

const blockSize = 4 << 20

func main() {
	mode := flag.String("mode", "inprocess", "inprocess | server | client | full")
	duration := flag.Duration("duration", 5*time.Second, "benchmark duration")
	workers := flag.Int("workers", 4, "concurrent workers")
	hotBlocks := flag.Int("hot", 40, "number of hot (cached) blocks")
	coldBlocks := flag.Int("cold", 10, "number of cold (uncached) blocks")
	hitRatio := flag.Float64("hit-ratio", 0.8, "probability of picking a hot block")
	spinMicros := flag.Int("spin", 50, "polling spin duration in μs")
	dataSock := flag.String("data", "/tmp/jfs-full-data.sock", "data socket")
	ctrlSock := flag.String("ctrl", "/tmp/jfs-full-ctrl.sock", "ctrl socket")
	cacheDir := flag.String("cache-dir", "/tmp/jfs-full-cache", "cache directory")
	flag.Parse()

	switch *mode {
	case "inprocess":
		runInProcess(*duration, *workers, *hotBlocks, *coldBlocks, *hitRatio)
	case "server":
		runServer(*dataSock, *ctrlSock, *cacheDir, *spinMicros, *hotBlocks)
	case "client":
		runClient(*dataSock, *ctrlSock, *duration, *workers, *hotBlocks, *coldBlocks, *hitRatio, *spinMicros)
	case "full":
		runFull(*dataSock, *ctrlSock, *cacheDir, *duration, *workers, *hotBlocks, *coldBlocks, *hitRatio, *spinMicros)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(1)
	}
}

type stats struct {
	hits4K, hits64K, hits256K, hits1M, misses atomic.Int64
	readBytes                                  atomic.Int64
	latencies                                  []int64 // nanoseconds, collected per-worker then merged
	latMu                                      sync.Mutex
}

func (s *stats) addLatencies(lats []int64) {
	s.latMu.Lock()
	s.latencies = append(s.latencies, lats...)
	s.latMu.Unlock()
}

func (s *stats) report(label string, duration time.Duration) {
	h4, h64, h256, h1m, m := s.hits4K.Load(), s.hits64K.Load(), s.hits256K.Load(), s.hits1M.Load(), s.misses.Load()
	total := h4 + h64 + h256 + h1m + m
	rb := s.readBytes.Load()
	if total == 0 {
		fmt.Println("no ops")
		return
	}
	perOp := float64(duration) / float64(total)
	throughput := float64(rb) / duration.Seconds() / (1024 * 1024 * 1024)
	fmt.Printf("[%s] total=%d ops/s=%.0f per_op=%s throughput=%.2f GiB/s\n",
		label, total, float64(total)/duration.Seconds(), time.Duration(perOp), throughput)
	fmt.Printf("  hits: 4K=%d 64K=%d 256K=%d 1M=%d misses=%d hit%%=%.1f\n",
		h4, h64, h256, h1m, m, float64(h4+h64+h256+h1m)/float64(total)*100)

	if len(s.latencies) > 0 {
		sort.Slice(s.latencies, func(i, j int) bool { return s.latencies[i] < s.latencies[j] })
		n := len(s.latencies)
		p := func(pct float64) time.Duration { return time.Duration(s.latencies[int(float64(n)*pct)]) }
		fmt.Printf("  latency: p50=%s p90=%s p99=%s p999=%s max=%s\n",
			p(0.50), p(0.90), p(0.99), min64(p(0.999), time.Duration(s.latencies[n-1])),
			time.Duration(s.latencies[n-1]))
	}
}

func min64(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

var readSizes = []struct {
	size   int
	cumW   float64
}{
	{4 << 10, 0.50},
	{64 << 10, 0.75},
	{256 << 10, 0.90},
	{1 << 20, 1.00},
}

func pickReadSize(rng *mathrand.Rand) int {
	r := rng.Float64()
	for _, rs := range readSizes {
		if r < rs.cumW {
			return rs.size
		}
	}
	return readSizes[len(readSizes)-1].size
}

func worker(bm *chunk.BenchCacheManager, hotKeys, coldKeys []string, hitRatio float64, st *stats, done chan struct{}, wg *sync.WaitGroup, seed uint64) {
	defer wg.Done()
	rng := mathrand.New(mathrand.NewPCG(seed, seed+1))
	buf := make([]byte, 1<<20)
	localLats := make([]int64, 0, 100000)

	for {
		select {
		case <-done:
			st.addLatencies(localLats)
			return
		default:
		}

		var key string
		if rng.Float64() < hitRatio && len(hotKeys) > 0 {
			key = hotKeys[rng.IntN(len(hotKeys))]
		} else {
			key = coldKeys[rng.IntN(len(coldKeys))]
		}

		readSize := pickReadSize(rng)
		maxOff := blockSize - readSize
		if maxOff < 0 {
			maxOff = 0
		}
		off := int64(rng.IntN(maxOff + 1))

		t0 := time.Now()
		n, err := bm.ReadAt(key, buf[:readSize], off)
		lat := time.Since(t0).Nanoseconds()
		localLats = append(localLats, lat)

		if err != nil {
			st.misses.Add(1)
			continue
		}
		st.readBytes.Add(int64(n))
		switch readSize {
		case 4 << 10:
			st.hits4K.Add(1)
		case 64 << 10:
			st.hits64K.Add(1)
		case 256 << 10:
			st.hits256K.Add(1)
		default:
			st.hits1M.Add(1)
		}
	}
}

func generateKeys(n int) []string {
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("chunks/%02X/%d/%d_0_%d", i%256, i/1000, i, blockSize)
	}
	return keys
}

func cacheBlocks(bm *chunk.BenchCacheManager, keys []string) {
	for _, key := range keys {
		data := make([]byte, blockSize)
		rand.Read(data)
		page := chunk.NewPage(data)
		page.Acquire()
		bm.Cache(key, page, true, false)
	}
	bm.WaitPending()
}

func runBench(label string, bm *chunk.BenchCacheManager, duration time.Duration, workers int, hotKeys, coldKeys []string, hitRatio float64) {
	runtime.GC()
	st := &stats{}
	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go worker(bm, hotKeys, coldKeys, hitRatio, st, done, &wg, uint64(w*1000))
	}
	time.Sleep(duration)
	close(done)
	wg.Wait()
	st.report(label, duration)
}

// --- In-process ---

func runInProcess(duration time.Duration, workers, hotCount, coldCount int, hitRatio float64) {
	dir, _ := os.MkdirTemp("", "jfs-bench-*")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "cache"), 0755)

	cm := chunk.NewCacheManagerForBench(&chunk.Config{
		CacheDir:      filepath.Join(dir, "cache"),
		CacheSize:     500 << 20,
		BlockSize:     blockSize,
		AutoCreate:    true,
		CacheEviction: chunk.Eviction2Random,
		CacheChecksum: chunk.CsNone,
	})
	bm := &chunk.BenchCacheManager{M: cm}
	defer bm.Close()

	hotKeys := generateKeys(hotCount)
	coldKeys := make([]string, coldCount)
	for i := range coldCount {
		coldKeys[i] = fmt.Sprintf("chunks/FF/%d/%d_0_%d", 900+i, 900000+i, blockSize)
	}
	cacheBlocks(bm, hotKeys)
	fmt.Printf("=== IN-PROCESS: workers=%d hot=%d cold=%d hit%%=%.0f\n", workers, hotCount, coldCount, hitRatio*100)
	runBench("inprocess", bm, duration, workers, hotKeys, coldKeys, hitRatio)
}

// --- Server ---

func runServer(dataSock, ctrlSock, cacheDir string, spinMicros, hotCount int) {
	os.MkdirAll(cacheDir, 0755)
	srv, err := chunk.NewCacheServerForBench(dataSock, ctrlSock, cacheDir, spinMicros)
	if err != nil {
		fmt.Fprintf(os.Stderr, "server: %v\n", err)
		os.Exit(1)
	}
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(1)
	}

	bm := &chunk.BenchCacheManager{M: srv.CacheManager()}
	hotKeys := generateKeys(hotCount)
	cacheBlocks(bm, hotKeys)
	fmt.Fprintf(os.Stderr, "SERVER READY spin=%dμs cached=%d\n", spinMicros, hotCount)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Close()
}

// --- Client ---

func runClient(dataSock, ctrlSock string, duration time.Duration, workers, hotCount, coldCount int, hitRatio float64, spinMicros int) {
	client, err := chunk.NewRemoteCacheManagerForBench(dataSock, ctrlSock, spinMicros)
	if err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	bm := &chunk.BenchCacheManager{M: client.AsManager()}
	hotKeys := generateKeys(hotCount)
	coldKeys := make([]string, coldCount)
	for i := range coldCount {
		coldKeys[i] = fmt.Sprintf("chunks/FF/%d/%d_0_%d", 900+i, 900000+i, blockSize)
	}

	// Warm FD cache
	for _, key := range hotKeys {
		rc, err := client.Load(key)
		if err == nil {
			rc.Close()
		}
	}

	fmt.Printf("=== OUT-OF-PROCESS: workers=%d hot=%d cold=%d hit%%=%.0f spin=%dμs\n",
		workers, hotCount, coldCount, hitRatio*100, spinMicros)
	runBench("out-of-process", bm, duration, workers, hotKeys, coldKeys, hitRatio)
}

// --- Full: spawn server subprocess ---

func runFull(dataSock, ctrlSock, cacheDir string, duration time.Duration, workers, hotCount, coldCount int, hitRatio float64, spinMicros int) {
	os.Remove(dataSock)
	os.Remove(ctrlSock)
	os.RemoveAll(cacheDir)

	self, _ := os.Executable()
	cmd := exec.Command(self, "-mode", "server",
		"-data", dataSock, "-ctrl", ctrlSock, "-cache-dir", cacheDir,
		"-spin", fmt.Sprintf("%d", spinMicros), "-hot", fmt.Sprintf("%d", hotCount))
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start server: %v\n", err)
		os.Exit(1)
	}
	defer func() { cmd.Process.Signal(syscall.SIGTERM); cmd.Wait() }()

	for i := 0; i < 50; i++ {
		if _, err := os.Stat(dataSock); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // wait for cache flush

	runClient(dataSock, ctrlSock, duration, workers, hotCount, coldCount, hitRatio, spinMicros)
}
