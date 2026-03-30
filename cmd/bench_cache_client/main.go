//go:build linux

package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
)

func main() {
	dataSock := flag.String("data", "/tmp/jfs-bench-data.sock", "data socket path")
	ctrlSock := flag.String("ctrl", "/tmp/jfs-bench-ctrl.sock", "ctrl socket path")
	duration := flag.Duration("duration", 5*time.Second, "benchmark duration")
	workers := flag.Int("workers", 1, "concurrent workers")
	spinMicros := flag.Int("spin", 0, "polling spin duration in microseconds")
	mode := flag.String("mode", "miss", "benchmark mode: miss, hit")
	blockSize := flag.Int("block-size", 4<<20, "block size for hit mode")
	numBlocks := flag.Int("blocks", 20, "number of blocks for hit mode")
	readSize := flag.Int("read-size", 4<<10, "read size for hit mode")
	flag.Parse()

	fmt.Printf("mode=%s data=%s ctrl=%s workers=%d spin=%dμs\n",
		*mode, *dataSock, *ctrlSock, *workers, *spinMicros)

	client, err := chunk.NewRemoteCacheManagerForBench(*dataSock, *ctrlSock, *spinMicros)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	switch *mode {
	case "miss":
		benchMiss(client, *workers, *duration)
	case "hit":
		benchHit(client, *workers, *duration, *blockSize, *numBlocks, *readSize)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(1)
	}
}

func benchMiss(client *chunk.BenchCacheClient, workers int, duration time.Duration) {
	// Warm up
	for i := 0; i < 100; i++ {
		client.Load(fmt.Sprintf("chunks/FF/%d/%d_0_4096", i, i))
	}
	runtime.GC()

	var ops atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				client.Load(fmt.Sprintf("chunks/FF/%d/%d_0_4096", id, i))
				ops.Add(1)
			}
		}(w)
	}

	time.Sleep(duration)
	close(done)
	wg.Wait()
	report("miss", ops.Load(), duration)
}

func benchHit(client *chunk.BenchCacheClient, workers int, duration time.Duration, blockSize, numBlocks, readSize int) {
	// Cache blocks via the server
	keys := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		keys[i] = fmt.Sprintf("chunks/%02X/%d/%d_0_%d", i%256, i/1000, i, blockSize)
		data := make([]byte, blockSize)
		rand.Read(data)
		client.CacheViaManager(keys[i], data)
	}
	// Wait for cache flush
	time.Sleep(500 * time.Millisecond)

	// Warm FD cache — first load gets the FD, subsequent loads reuse it
	for _, key := range keys {
		rc, err := client.Load(key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warm load %s: %v\n", key, err)
			os.Exit(1)
		}
		rc.Close()
	}
	fmt.Printf("Cached %d blocks of %d bytes, read size %d\n", numBlocks, blockSize, readSize)

	runtime.GC()

	var ops atomic.Int64
	var bytes atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			buf := make([]byte, readSize)
			i := 0
			for {
				select {
				case <-done:
					return
				default:
				}
				key := keys[i%numBlocks]
				rc, err := client.Load(key)
				if err != nil {
					continue
				}
				off := int64((i * 4096) % (blockSize - readSize))
				n, _ := rc.ReadAt(buf, off)
				rc.Close()
				ops.Add(1)
				bytes.Add(int64(n))
				i++
			}
		}(w)
	}

	time.Sleep(duration)
	close(done)
	wg.Wait()

	total := ops.Load()
	totalBytes := bytes.Load()
	perOp := float64(duration) / float64(total)
	throughput := float64(totalBytes) / duration.Seconds() / (1024 * 1024 * 1024)
	fmt.Printf("ops=%d duration=%s ops/s=%.0f per_op=%s bytes=%d throughput=%.1f GiB/s\n",
		total, duration, float64(total)/duration.Seconds(),
		time.Duration(perOp), totalBytes, throughput)
}

func report(mode string, total int64, duration time.Duration) {
	perOp := float64(duration) / float64(total)
	fmt.Printf("ops=%d duration=%s ops/s=%.0f per_op=%s\n",
		total, duration, float64(total)/duration.Seconds(),
		time.Duration(perOp))
}
