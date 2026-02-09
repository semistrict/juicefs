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

//nolint:errcheck
package meta

import (
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/gorilla/websocket"
)

// startWskvServer starts a WskvServer behind a WebSocket endpoint.
// Returns the server URL and a cleanup function.
func startWskvServer(t *testing.T) (*WskvServer, string, func()) {
	t.Helper()
	server := NewWskvServer()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer ws.Close()
		server.Serve(ws)
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: mux}
	go httpServer.Serve(listener)

	addr := fmt.Sprintf("ws://127.0.0.1:%d/ws", listener.Addr().(*net.TCPAddr).Port)
	cleanup := func() {
		httpServer.Close()
		listener.Close()
	}
	return server, addr, cleanup
}

// connectWskv creates a wskvClient connected to the given WebSocket URL.
func connectWskv(t *testing.T, addr string) *wskvClient {
	t.Helper()
	ws, _, err := websocket.DefaultDialer.Dial(addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &wskvClient{
		ready: make(chan struct{}),
	}
	client.ws = ws
	close(client.ready)
	go client.readLoop()
	return client
}

func TestWskvTKV(t *testing.T) {
	_, addr, cleanup := startWskvServer(t)
	defer cleanup()

	client := connectWskv(t, addr)
	defer client.close()

	c := withPrefix(client, []byte("jfs"))
	testTKV(t, c)
}

func TestWskvMeta(t *testing.T) {
	_, addr, cleanup := startWskvServer(t)
	defer cleanup()

	// Set the global client so newWskvClient returns it.
	oldClient := globalWskvClient
	defer func() { globalWskvClient = oldClient }()

	client := connectWskv(t, addr)
	globalWskvClient = client

	m, err := newKVMeta("wskv", "jfs-unit-test", testConfig())
	if err != nil || m.Name() != "wskv" {
		t.Fatalf("create meta: %s", err)
	}
	testMeta(t, m)
}
