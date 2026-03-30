// Shared cache consistency checker.
//
// Simulates N nodes, each with its own JuiceFS mount point, all sharing a cache
// server. Writes files from random nodes with random sizes, records SHA-256 hashes,
// then reads back from all nodes and verifies every byte matches.
//
// Inspired by Jepsen linearizability checking — we maintain a consistency log of
// all writes and verify that reads from any mount point return the exact data
// that was written.
//
// Usage:
//
//	go run ./hack/sharedcache/checker \
//	  --mounts=/mnt/jfs0,/mnt/jfs1,/mnt/jfs2 \
//	  --files=1000 \
//	  --writers=8 \
//	  --readers=8 \
//	  --read-rounds=3
package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type fileRecord struct {
	name   string
	size   int
	sha256 string
}

func main() {
	var (
		mountsFlag = flag.String("mounts", "", "comma-separated list of JuiceFS mount points")
		numFiles   = flag.Int("files", 1000, "number of files to write")
		writers    = flag.Int("writers", 8, "concurrent writer goroutines")
		readers    = flag.Int("readers", 8, "concurrent reader goroutines")
		readRounds = flag.Int("read-rounds", 3, "how many times to read all files from each mount")
		minSize    = flag.Int("min-size", 512*1024, "minimum file size in bytes")
		maxSize    = flag.Int("max-size", 10*1024*1024, "maximum file size in bytes")
	)
	flag.Parse()

	if *mountsFlag == "" {
		fmt.Fprintln(os.Stderr, "error: --mounts is required")
		os.Exit(1)
	}
	mounts := strings.Split(*mountsFlag, ",")
	if len(mounts) < 2 {
		fmt.Fprintln(os.Stderr, "error: need at least 2 mount points")
		os.Exit(1)
	}

	// Verify all mount points exist
	for _, m := range mounts {
		if fi, err := os.Stat(m); err != nil || !fi.IsDir() {
			fmt.Fprintf(os.Stderr, "error: mount point %q is not a directory\n", m)
			os.Exit(1)
		}
	}

	// Create workload directory (via first mount)
	workdir := "checker-" + fmt.Sprintf("%d", time.Now().UnixNano())
	if err := os.MkdirAll(filepath.Join(mounts[0], workdir), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "error: mkdir %s: %v\n", workdir, err)
		os.Exit(1)
	}
	fmt.Printf("workdir: %s\n", workdir)

	// Phase 1: Write files from random mount points
	fmt.Printf("\n=== PHASE 1: Writing %d files (%d-%d bytes) from %d goroutines ===\n",
		*numFiles, *minSize, *maxSize, *writers)

	records := make([]fileRecord, *numFiles)
	var writeErrors atomic.Int64
	var bytesWritten atomic.Int64

	writeCh := make(chan int, *numFiles)
	for i := 0; i < *numFiles; i++ {
		writeCh <- i
	}
	close(writeCh)

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range writeCh {
				// Pick a random mount point
				mount := mounts[mathrand.IntN(len(mounts))]
				// Random size between min and max
				size := *minSize + mathrand.IntN(*maxSize-*minSize+1)
				name := fmt.Sprintf("file-%06d.bin", idx)

				data := make([]byte, size)
				rand.Read(data) //nolint:errcheck

				hash := sha256.Sum256(data)
				hashStr := hex.EncodeToString(hash[:])

				path := filepath.Join(mount, workdir, name)
				if err := os.WriteFile(path, data, 0644); err != nil {
					fmt.Fprintf(os.Stderr, "WRITE ERROR [%s via %s]: %v\n", name, mount, err)
					writeErrors.Add(1)
					continue
				}

				records[idx] = fileRecord{name: name, size: size, sha256: hashStr}
				bytesWritten.Add(int64(size))
			}
		}()
	}
	wg.Wait()
	writeDur := time.Since(start)

	if writeErrors.Load() > 0 {
		fmt.Fprintf(os.Stderr, "FATAL: %d write errors\n", writeErrors.Load())
		os.Exit(1)
	}
	fmt.Printf("wrote %d files, %s in %v (%.1f MB/s)\n",
		*numFiles,
		humanBytes(bytesWritten.Load()),
		writeDur.Round(time.Millisecond),
		float64(bytesWritten.Load())/1e6/writeDur.Seconds())

	// Phase 2: Read all files from every mount point, multiple rounds
	fmt.Printf("\n=== PHASE 2: Reading from %d mounts x %d rounds x %d goroutines ===\n",
		len(mounts), *readRounds, *readers)

	var readErrors atomic.Int64
	var hashMismatches atomic.Int64
	var bytesRead atomic.Int64
	var readsCompleted atomic.Int64

	start = time.Now()
	for round := 0; round < *readRounds; round++ {
		for _, mount := range mounts {
			readCh := make(chan int, *numFiles)
			for i := 0; i < *numFiles; i++ {
				readCh <- i
			}
			close(readCh)

			var rwg sync.WaitGroup
			for r := 0; r < *readers; r++ {
				rwg.Add(1)
				go func(m string) {
					defer rwg.Done()
					for idx := range readCh {
						rec := records[idx]
						if rec.name == "" {
							continue // skipped due to write error
						}
						path := filepath.Join(m, workdir, rec.name)

						data, err := os.ReadFile(path)
						if err != nil {
							fmt.Fprintf(os.Stderr, "READ ERROR [%s via %s round %d]: %v\n",
								rec.name, m, round, err)
							readErrors.Add(1)
							continue
						}

						if len(data) != rec.size {
							fmt.Fprintf(os.Stderr, "SIZE MISMATCH [%s via %s round %d]: got %d, want %d\n",
								rec.name, m, round, len(data), rec.size)
							hashMismatches.Add(1)
							continue
						}

						hash := sha256.Sum256(data)
						hashStr := hex.EncodeToString(hash[:])
						if hashStr != rec.sha256 {
							fmt.Fprintf(os.Stderr, "HASH MISMATCH [%s via %s round %d]: got %s, want %s\n",
								rec.name, m, round, hashStr, rec.sha256)
							hashMismatches.Add(1)
							continue
						}

						bytesRead.Add(int64(len(data)))
						readsCompleted.Add(1)
					}
				}(mount)
			}
			rwg.Wait()
		}
		fmt.Printf("  round %d/%d complete (%d reads, %s)\n",
			round+1, *readRounds, readsCompleted.Load(), humanBytes(bytesRead.Load()))
	}
	readDur := time.Since(start)

	// Phase 3: Summary
	totalExpectedReads := int64(*numFiles) * int64(len(mounts)) * int64(*readRounds)
	fmt.Printf("\n=== RESULTS ===\n")
	fmt.Printf("files written:     %d\n", *numFiles)
	fmt.Printf("bytes written:     %s\n", humanBytes(bytesWritten.Load()))
	fmt.Printf("write throughput:  %.1f MB/s\n", float64(bytesWritten.Load())/1e6/writeDur.Seconds())
	fmt.Printf("reads completed:   %d / %d expected\n", readsCompleted.Load(), totalExpectedReads)
	fmt.Printf("bytes read:        %s\n", humanBytes(bytesRead.Load()))
	fmt.Printf("read throughput:   %.1f MB/s\n", float64(bytesRead.Load())/1e6/readDur.Seconds())
	fmt.Printf("read errors:       %d\n", readErrors.Load())
	fmt.Printf("hash mismatches:   %d\n", hashMismatches.Load())

	if readErrors.Load() > 0 || hashMismatches.Load() > 0 {
		fmt.Println("\nFAILED: data corruption detected")
		os.Exit(1)
	}
	if readsCompleted.Load() != totalExpectedReads {
		fmt.Printf("\nFAILED: expected %d reads, got %d\n", totalExpectedReads, readsCompleted.Load())
		os.Exit(1)
	}
	fmt.Println("\nPASSED: all reads consistent across all mounts")
}

func humanBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
