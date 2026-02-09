/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
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

// juicefs-cf-mount is a standalone binary for Cloudflare Workers containers.
// It starts a WebSocket server, waits for a Durable Object to connect and
// provide storage config, then mounts JuiceFS via FUSE at the given mountpoint.
package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"google.golang.org/protobuf/proto"

	"github.com/juicedata/juicefs/pkg/chunk"
	jfuse "github.com/juicedata/juicefs/pkg/fuse"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/meta/pb"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/juicedata/juicefs/pkg/vfs"
)

var logger = logrus.WithField("comp", "cfmount")


func main() {
	app := &cli.App{
		Name:      "juicefs-cf-mount",
		Usage:     "Mount JuiceFS via FUSE inside a Cloudflare Workers container",
		Version:   version.Version(),
		ArgsUsage: "MOUNTPOINT",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "address",
				Value: "0.0.0.0:14234",
				Usage: "WebSocket server listen address",
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() < 1 {
				fmt.Fprintf(os.Stderr, "ERROR: MOUNTPOINT is required\n")
				fmt.Fprintf(os.Stderr, "USAGE:\n   juicefs-cf-mount [options] MOUNTPOINT\n")
				os.Exit(1)
			}
			mountpoint := c.Args().Get(0)
			addr := c.String("address")

			// Prepare the mountpoint directory.
			if err := os.MkdirAll(mountpoint, 0755); err != nil {
				logger.Fatalf("mkdir %s: %s", mountpoint, err)
			}

			// Channel to receive the init message and WebSocket connection.
			type connResult struct {
				ws   *websocket.Conn
				init *pb.InitNotification
			}
			connCh := make(chan connResult)

			upgrader := websocket.Upgrader{
				CheckOrigin: func(r *http.Request) bool { return true },
			}

			http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
				ws, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					logger.Errorf("websocket upgrade: %s", err)
					return
				}

				// Read init message (protobuf WskvMessage with init_notify).
				_, data, err := ws.ReadMessage()
				if err != nil {
					logger.Errorf("read init message: %s", err)
					ws.Close()
					return
				}

				var envelope pb.WskvMessage
				if err := proto.Unmarshal(data, &envelope); err != nil {
					logger.Errorf("parse init message: %s", err)
					ws.Close()
					return
				}
				initNotify := envelope.GetInitNotify()
				if initNotify == nil {
					logger.Errorf("expected init_notify message")
					ws.Close()
					return
				}

				logger.Infof("Received init: volume=%s storage=%s bucket=%s", initNotify.VolumeName, initNotify.Storage, initNotify.Bucket)

				connCh <- connResult{ws: ws, init: initNotify}
			})

			// Start HTTP server in background.
			logger.Infof("Starting WebSocket server on %s", addr)
			go func() {
				if err := http.ListenAndServe(addr, nil); err != nil {
					logger.Fatalf("http server: %s", err)
				}
			}()

			// Wait for DO to connect.
			logger.Infof("Waiting for Durable Object connection...")
			result := <-connCh
			ws := result.ws
			initMsg := result.init

			// Set up the WebSocket KV client for the TKV meta engine.
			meta.SetWskvConnection(ws)

			// Create the meta client using our wskv driver.
			metaConf := meta.DefaultConf()
			metaConf.MountPoint = mountpoint
			metaCli := meta.NewClient("wskv://local", metaConf)

			// Try loading existing format. If it fails, this is a fresh volume.
			format, err := metaCli.Load(false)
			if err != nil {
				if !strings.Contains(err.Error(), "database is not formatted") {
					logger.Fatalf("load metadata: %s", err)
				}
				logger.Infof("Volume not formatted, initializing...")
				format = &meta.Format{
					Name:        initMsg.VolumeName,
					UUID:        uuid.New().String(),
					Storage:     initMsg.Storage,
					Bucket:      initMsg.Bucket,
					AccessKey:   initMsg.AccessKey,
					SecretKey:   initMsg.SecretKey,
					BlockSize:   4096, // 4 MiB in KiB
					Compression: "none",
					TrashDays:   0,
					MetaVersion: 1,
					DirStats:    true,
				}
				if err := metaCli.Init(format, false); err != nil {
					logger.Fatalf("format volume: %s", err)
				}
				logger.Infof("Volume %s formatted successfully", format.Name)

				// Reload after init.
				format, err = metaCli.Load(false)
				if err != nil {
					logger.Fatalf("reload after format: %s", err)
				}
			}
			logger.Infof("Volume loaded: %s (storage=%s)", format.Name, format.Storage)

			// Create object storage.
			object.UserAgent = "JuiceFS-" + version.Version()
			blob, err := object.CreateStorage(format.Storage, format.Bucket, format.AccessKey, format.SecretKey, format.SessionToken)
			if err != nil {
				logger.Fatalf("object storage: %s", err)
			}
			logger.Infof("Data storage: %s", blob)

			// Set up chunk store.
			chunkConf := chunk.Config{
				BlockSize:  format.BlockSize * 1024,
				Compress:   format.Compression,
				HashPrefix: format.HashPrefix,
				MaxUpload:  20,
				MaxRetries: 10,
				BufferSize: 300 << 20, // 300 MiB
				CacheSize:  1024,      // 1 GiB in MiB
				AutoCreate: true,
				GetTimeout: 60e9,
				PutTimeout: 60e9,
			}
			store := chunk.NewCachedStore(blob, chunkConf, nil)

			// Register meta message handlers.
			metaCli.OnMsg(meta.DeleteSlice, func(args ...interface{}) error {
				return store.Remove(args[0].(uint64), int(args[1].(uint32)))
			})
			metaCli.OnMsg(meta.CompactChunk, func(args ...interface{}) error {
				return vfs.Compact(chunkConf, store, args[0].([]meta.Slice), args[1].(uint64))
			})

			// Create a new session.
			if err := metaCli.NewSession(true); err != nil {
				logger.Fatalf("new session: %s", err)
			}

			// Helper to send a ready notification on a WebSocket.
			sendReady := func(conn *websocket.Conn) error {
				readyData, err := proto.Marshal(&pb.WskvMessage{
					Msg: &pb.WskvMessage_ReadyNotify{ReadyNotify: &pb.ReadyNotification{}},
				})
				if err != nil {
					return fmt.Errorf("marshal ready: %w", err)
				}
				return conn.WriteMessage(websocket.BinaryMessage, readyData)
			}

			// Notify DO that mount is ready.
			if err := sendReady(ws); err != nil {
				logger.Fatalf("send ready: %s", err)
			}

			// Accept reconnections in background.
			go func() {
				for cr := range connCh {
					logger.Infof("WebSocket reconnected (volume=%s)", cr.init.VolumeName)
					meta.SetWskvConnection(cr.ws)
					if err := sendReady(cr.ws); err != nil {
						logger.Errorf("send ready on reconnect: %s", err)
					}
				}
			}()

			// Set up VFS config.
			vfsConf := &vfs.Config{
				Meta:     metaConf,
				Format:   *format,
				Version:  version.Version(),
				Chunk:    &chunkConf,
				Pid:      os.Getpid(),
				PPid:     os.Getppid(),
				FuseOpts: &vfs.FuseOptions{},
			}
			v := vfs.NewVFS(vfsConf, metaCli, store, nil, nil)

			// Mount FUSE.
			logger.Infof("Mounting JuiceFS at %s ...", mountpoint)
			if err := jfuse.Serve(v, "allow_other", true, false); err != nil {
				logger.Fatalf("fuse: %s", err)
			}

			// Cleanup on exit.
			if err := v.FlushAll(""); err != nil {
				logger.Errorf("flush: %s", err)
			}
			metaCli.CloseSession()
			object.Shutdown(blob)
			logger.Infof("JuiceFS unmounted from %s", mountpoint)
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		logger.Fatalf("%s", err)
	}
}
