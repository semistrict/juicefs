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
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
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

// startCFWskvServer starts a local WebSocket listener, then kicks the
// Cloudflare TS worker at cfURL to connect back to it. The TS Durable Object
// connects to our listener and wires up its WskvServer (backed by real DO
// SQLite). Returns a wskvClient using the accepted connection.
func startCFWskvServer(t *testing.T, cfURL string) (*wskvClient, func()) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	wsCh := make(chan *websocket.Conn, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		wsCh <- ws
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: mux}
	go httpServer.Serve(listener) //nolint:errcheck

	port := listener.Addr().(*net.TCPAddr).Port
	target := fmt.Sprintf("ws://127.0.0.1:%d/ws", port)

	// Kick the TS worker to connect to our listener. Pass test name so each
	// test gets a fresh Durable Object instance with clean SQLite state.
	kickURL := fmt.Sprintf("%s/connect?target=%s&name=%s", cfURL, url.QueryEscape(target), url.QueryEscape(t.Name()))
	resp, err := http.Get(kickURL) //nolint:gosec
	if err != nil {
		listener.Close()
		t.Fatalf("kick TS server: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		listener.Close()
		t.Fatalf("kick TS server: status %d: %s", resp.StatusCode, body)
	}

	ws := <-wsCh

	client := &wskvClient{
		ready: make(chan struct{}),
	}
	client.ws = ws
	close(client.ready)
	go client.readLoop()

	cleanup := func() {
		client.close()
		httpServer.Close()
		listener.Close()
	}
	return client, cleanup
}

// wskvClient connects to the Go in-memory WskvServer, or to the Cloudflare TS
// WskvServer if WSKV_CF_URL is set (e.g. WSKV_CF_URL=http://localhost:8787).
func newTestWskvClient(t *testing.T) (*wskvClient, func()) {
	t.Helper()
	cfURL := os.Getenv("WSKV_CF_URL")
	if cfURL != "" {
		t.Logf("using Cloudflare TS server at %s", cfURL)
		return startCFWskvServer(t, cfURL)
	}
	_, addr, serverCleanup := startWskvServer(t)
	client := connectWskv(t, addr)
	cleanup := func() {
		client.close()
		serverCleanup()
	}
	return client, cleanup
}

func TestWskvTKV(t *testing.T) {
	client, cleanup := newTestWskvClient(t)
	defer cleanup()

	c := withPrefix(client, []byte("jfs"))
	testTKV(t, c)
}

func TestWskvMeta(t *testing.T) {
	client, cleanup := newTestWskvClient(t)
	defer cleanup()

	// Set the global client so newWskvClient returns it.
	oldClient := globalWskvClient
	defer func() { globalWskvClient = oldClient }()
	globalWskvClient = client

	m, err := newKVMeta("wskv", "jfs-unit-test", testConfig())
	if err != nil || m.Name() != "wskv" {
		t.Fatalf("create meta: %s", err)
	}
	testMeta(t, m)
}
