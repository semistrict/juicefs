//go:build linux

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/juicedata/juicefs/pkg/chunk"
)

func main() {
	dataSock := flag.String("data", "/tmp/jfs-bench-data.sock", "data socket path")
	ctrlSock := flag.String("ctrl", "/tmp/jfs-bench-ctrl.sock", "ctrl socket path")
	cacheDir := flag.String("cache-dir", "/tmp/jfs-bench-cache", "cache directory")
	spinMicros := flag.Int("spin", 0, "polling spin duration in microseconds (0=disabled)")
	flag.Parse()

	os.MkdirAll(*cacheDir, 0755)

	srv, err := chunk.NewCacheServerForBench(*dataSock, *ctrlSock, *cacheDir, *spinMicros)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create server: %v\n", err)
		os.Exit(1)
	}

	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Cache server ready: data=%s ctrl=%s spin=%dμs\n", *dataSock, *ctrlSock, *spinMicros)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("Shutting down")
	srv.Close()
}
