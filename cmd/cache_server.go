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

package cmd

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
)

func cmdCacheServer() *cli.Command {
	return &cli.Command{
		Name:      "cache-server",
		Action:    cacheServer,
		Category:  "SERVICE",
		Usage:     "Start a shared cache server for multiple JuiceFS mount points",
		ArgsUsage: "DATA-SOCK CTRL-SOCK",
		Description: `
Start a shared cache server that allows multiple JuiceFS mount processes on the
same machine to share a single disk cache. Clients connect via Unix domain sockets
and receive file descriptors for cached blocks (zero-copy reads).

Examples:
# Start cache server with default settings
$ juicefs cache-server /var/run/juicefs-cache-data.sock /var/run/juicefs-cache-ctrl.sock

# Start with custom cache directory and size
$ juicefs cache-server --cache-dir /mnt/ssd/jfscache --cache-size 200G \
    /var/run/juicefs-cache-data.sock /var/run/juicefs-cache-ctrl.sock`,
		Flags: expandFlags(dataCacheFlags(), []cli.Flag{
			&cli.IntFlag{
				Name:  "block-size",
				Value: 4096,
				Usage: "block size in KiB",
			},
			&cli.StringFlag{
				Name:  "metrics",
				Value: "127.0.0.1:9568",
				Usage: "address to expose Prometheus metrics",
			},
		}),
	}
}

func cacheServer(ctx *cli.Context) error {
	setup0(ctx, 2, 2)
	dataSock := ctx.Args().Get(0)
	ctrlSock := ctx.Args().Get(1)

	config, err := buildCacheConfig(ctx)
	if err != nil {
		return err
	}

	registry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_", registry)

	server := chunk.NewCacheServerWithConfig(config, registerer, dataSock, ctrlSock)
	if err := server.ListenAndServe(); err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	// Expose Prometheus metrics
	metricsAddr := ctx.String("metrics")
	exposeCacheServerMetrics(registry, registerer, metricsAddr)

	logger.Infof("Cache server started: data=%s ctrl=%s cache-dir=%s cache-size=%s metrics=%s",
		dataSock, ctrlSock, config.CacheDir, formatBytes(int64(config.CacheSize)), metricsAddr)

	// Wait for signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Infof("Shutting down cache server")
	server.Close()
	return nil
}

func exposeCacheServerMetrics(registry *prometheus.Registry, registerer prometheus.Registerer, addr string) {
	registerer.MustRegister(collectors.NewBuildInfoCollector())

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Warnf("Failed to listen on %s for metrics: %v", addr, err)
		return
	}

	go func() {
		if err := http.Serve(ln, mux); err != nil {
			logger.Errorf("Serve metrics: %s", err)
		}
	}()
	logger.Infof("Prometheus metrics listening on %s", ln.Addr().String())
}

func buildCacheConfig(ctx *cli.Context) (*chunk.Config, error) {
	cm, err := parseOctal(ctx.String("cache-mode"))
	if err != nil {
		return nil, fmt.Errorf("invalid cache-mode: %w", err)
	}
	config := &chunk.Config{
		CacheDir:          ctx.String("cache-dir"),
		CacheSize:         utils.ParseBytes(ctx, "cache-size", 'M'),
		CacheItems:        ctx.Int64("cache-items"),
		FreeSpace:         float32(ctx.Float64("free-space-ratio")),
		CacheChecksum:     ctx.String("verify-cache-checksum"),
		CacheEviction:     ctx.String("cache-eviction"),
		CacheScanInterval: utils.Duration(ctx.String("cache-scan-interval")),
		CacheExpire:       utils.Duration(ctx.String("cache-expire")),
		CacheMode:         os.FileMode(cm),
		BlockSize:         ctx.Int("block-size") << 10,
		BufferSize:        utils.ParseBytes(ctx, "buffer-size", 'M'),
		AutoCreate:        true,
	}
	return config, nil
}

func parseOctal(s string) (uint64, error) {
	var val uint64
	for _, c := range s {
		if c < '0' || c > '7' {
			return 0, fmt.Errorf("invalid octal digit %c", c)
		}
		val = val*8 + uint64(c-'0')
	}
	return val, nil
}

func formatBytes(mb int64) string {
	if mb >= 1024 {
		return fmt.Sprintf("%dG", mb/1024)
	}
	return fmt.Sprintf("%dM", mb)
}
