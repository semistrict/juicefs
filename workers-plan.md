# JuiceFS Cloudflare Workers Integration Plan

## Problem

Cloudflare sandbox containers mount R2 via s3fs, which has poor POSIX compliance and performance. We want to use JuiceFS instead, but JuiceFS needs a metadata engine and the container can't connect outbound to the Durable Object — only the DO can connect inward.

## Why Not Proxy JuiceFS's SQL Engine?

Our first approach was proxying JuiceFS's SQL meta engine (XORM + database/sql) over WebSocket to DO SQLite. This fails because:

- DO's `transactionSync()` is **synchronous-only** — the callback cannot `await` anything
- But a WebSocket SQL proxy is inherently async (send query → wait for response)
- You can't do `transactionSync(() => { await ws.send(...) })` — it's not allowed
- Without transactions, JuiceFS metadata operations are not safe (concurrent writes corrupt state)

## Solution: TKV Meta Engine + DO SQLite KV Table

JuiceFS has a **key-value meta engine** (`tkvClient` interface in `pkg/meta/tkv.go`) used by Redis, TiKV, etcd, Badger, etc. It uses **optimistic concurrency control (OCC)**:

1. Transaction reads go to the store, recording each key's version
2. Writes are buffered locally in Go
3. At commit: send one message with `{observed: {key: ver}, writes: {key: val}, deletes: [key]}` to DO
4. DO runs `transactionSync()` — runs synchronous SQL to check versions and apply writes atomically
5. If any version changed → return "write conflict" → JuiceFS retries (built-in)

The DO stores all metadata in a single SQLite table:

```sql
CREATE TABLE IF NOT EXISTS jfs_kv (
  k TEXT PRIMARY KEY,
  v BLOB NOT NULL,
  ver INTEGER NOT NULL DEFAULT 1
);
```

This works because:
- All SQL inside `transactionSync` is **synchronous** (`ctx.storage.sql.exec()`) — no async needed
- Reads happen over normal async WebSocket round-trips (outside the transaction)
- The commit is a single WebSocket message → DO handles it entirely synchronously
- SQLite gives us proper indexing for range scans (`WHERE k >= ? AND k < ?`)
- JuiceFS already has retry logic for OCC conflicts

## Architecture

```
┌─────────────────────────────────┐      ┌──────────────────────────────────┐
│  Cloudflare Durable Object      │      │  Sandbox Container               │
│                                 │      │                                  │
│  ┌───────────────────────┐      │      │  ┌──────────────────────────┐    │
│  │ DO SQLite              │      │ WS   │  │ juicefs-cf-mount         │    │
│  │                        │◄─────┼──────┼──┤                          │    │
│  │ TABLE jfs_kv (         │      │      │  │  WebSocket server :9876  │    │
│  │   k TEXT PK,           │      │      │  │  ↕                       │    │
│  │   v BLOB,              │      │      │  │  wskvClient (tkvClient)  │    │
│  │   ver INTEGER          │      │      │  │  ↕                       │    │
│  │ )                      │      │      │  │  kvMeta (TKV engine)      │    │
│  └───────────────────────┘      │      │  │  ↕                       │    │
│                                 │      │  │  FUSE → /workspace        │    │
│  DO connects OUT to container   │      │  └──────────────────────────┘    │
│  on port 9876 via sandbox proxy │      │                                  │
│                                 │      │                                  │
│  R2 bucket ◄────────────────────┼──────┼── JuiceFS data chunks            │
└─────────────────────────────────┘      └──────────────────────────────────┘
```

**Key insight:** We implement a `tkvClient` that proxies KV operations over WebSocket. The DO stores metadata in a single `jfs_kv` SQLite table and handles OCC conflict detection + atomic writes inside `transactionSync()` using synchronous SQL. Zero changes to JuiceFS core — just a new TKV driver.

## WebSocket Protocol

Container (Go) is the **server** (listens on port N).
DO is the **client** (connects via sandbox proxy).

### Phase 1: Init (DO → Container)

After WebSocket connects, DO sends storage config:

```json
{"type": "init", "storage": "s3", "bucket": "https://ACCOUNT.r2.cloudflarestorage.com/deepagents-workspace", "accessKey": "...", "secretKey": "...", "volumeName": "ws-THREAD_ID"}
```

Container ACKs when JuiceFS mount is ready:

```json
{"type": "ready"}
```

### Phase 2: KV operations (Container → DO → Container)

#### Get (single key)

Request:
```json
{"id": 1, "op": "get", "key": "0141000000000001"}
```

DO runs: `SELECT v, ver FROM jfs_kv WHERE k = ?`

Response:
```json
{"id": 1, "data": "base64-encoded-value", "ver": 5}
```

If key doesn't exist:
```json
{"id": 1, "data": null, "ver": 0}
```

#### List (range scan)

Request:
```json
{"id": 2, "op": "list", "start": "0141000000000000", "end": "0141ffffffffffff", "keysOnly": false}
```

DO runs: `SELECT k, v, ver FROM jfs_kv WHERE k >= ? AND k < ? ORDER BY k`

Response:
```json
{"id": 2, "entries": [
  {"k": "0141000000000001", "d": "base64-value", "v": 3},
  {"k": "0141000000000002", "d": "base64-value", "v": 1}
]}
```

#### Commit (atomic write)

Request:
```json
{
  "id": 3,
  "op": "commit",
  "observed": {"0141000000000001": 5, "0141000000000002": 3},
  "puts": {"0141000000000001": "base64-new-value"},
  "dels": ["0141000000000002"]
}
```

Success:
```json
{"id": 3, "ok": true}
```

Conflict:
```json
{"id": 3, "ok": false, "error": "write conflict"}
```

The DO handles commit inside `transactionSync`:

```typescript
ctx.storage.transactionSync(() => {
  const sql = ctx.storage.sql;

  // Check all observed versions still match
  for (const [key, expectedVer] of Object.entries(msg.observed)) {
    const row = sql.exec("SELECT ver FROM jfs_kv WHERE k = ?", key).one();
    const currentVer = row ? row.ver : 0;
    if (currentVer !== expectedVer) {
      throw new Error("write conflict");
    }
  }

  // Apply puts (INSERT OR REPLACE, incrementing version)
  for (const [key, b64value] of Object.entries(msg.puts)) {
    const existing = sql.exec("SELECT ver FROM jfs_kv WHERE k = ?", key).one();
    const newVer = existing ? existing.ver + 1 : 1;
    sql.exec(
      "INSERT OR REPLACE INTO jfs_kv (k, v, ver) VALUES (?, ?, ?)",
      key, base64ToBytes(b64value), newVer
    );
  }

  // Apply deletes
  for (const key of msg.dels) {
    sql.exec("DELETE FROM jfs_kv WHERE k = ?", key);
  }
});
```

All SQL inside `transactionSync` is synchronous — no `await` needed.

### Key encoding

JuiceFS TKV keys are binary (`[]byte`). DO SQLite keys are TEXT. We use **hex encoding** — simple, debuggable, bijective, and preserves sort order (critical for range scans).

### Value encoding

Values are binary. On the wire: base64-encoded in JSON. In SQLite: stored as BLOB.

## Files to Create/Modify

### In ~/src/juicefs (Go)

#### 1. `pkg/meta/tkv_wskv.go` — tkvClient implementation

Implements the `tkvClient` interface by proxying over WebSocket:

```go
package meta

func init() {
    Register("wskv", newKVMeta)
    drivers["wskv"] = newWskvClient
}
```

Transaction implementation follows the same pattern as `memKV` in `tkv_mem.go`:

- `wskvTxn.get(key)` → sends `{"op": "get", "key": hex(key)}` over WebSocket, records observed version
- `wskvTxn.scan(begin, end)` → sends `{"op": "list", "start": hex, "end": hex}`, records observed versions
- `wskvTxn.set(key, value)` → buffers locally
- `wskvTxn.delete(key)` → buffers locally (nil value)
- `wskvClient.txn()` → runs callback, then sends `{"op": "commit", observed, puts, dels}`

Also implements `append` and `incrBy` in terms of `get` + `set` (same as memKV).

#### 2. `cmd/cfmount/main.go` — The binary (updated)

```
Usage: juicefs-cf-mount /workspace --port 9876

1. mkdir -p /workspace && chmod 755 /workspace
2. Write /workspace/README.txt placeholder
3. Start WebSocket server on 0.0.0.0:9876
4. Wait for DO to connect (blocking)
5. Receive init message with storage config
6. Create wskvClient, call SetConnection(ws)
7. Check if volume is formatted:
   - meta.NewClient("wskv://local", conf)
   - Try m.Load() — if fails, run Init() with storage config
8. Remove README.txt
9. Mount JuiceFS at /workspace via FUSE (blocking)
```

#### Files to DELETE (from old SQL approach)

- `pkg/meta/wssqlite/driver.go` — no longer needed
- `pkg/meta/sql_wssqlite.go` — no longer needed

### In ~/src/deepagentsjs (TypeScript)

#### 3. DO-side message handler

On WebSocket connect, DO creates the table if needed:

```typescript
ctx.storage.sql.exec(`
  CREATE TABLE IF NOT EXISTS jfs_kv (
    k TEXT PRIMARY KEY,
    v BLOB NOT NULL,
    ver INTEGER NOT NULL DEFAULT 1
  )
`);
```

Then handles messages:

```typescript
#handleKvRequest(msg: any, ws: WebSocket) {
  const sql = this.ctx.storage.sql;
  try {
    if (msg.op === "get") {
      const row = sql.exec("SELECT v, ver FROM jfs_kv WHERE k = ?", msg.key).one();
      ws.send(JSON.stringify({
        id: msg.id,
        data: row ? bytesToBase64(row.v) : null,
        ver: row ? row.ver : 0,
      }));

    } else if (msg.op === "list") {
      const cursor = sql.exec(
        "SELECT k, v, ver FROM jfs_kv WHERE k >= ? AND k < ? ORDER BY k",
        msg.start, msg.end
      );
      const entries = [];
      for (const row of cursor) {
        entries.push({
          k: row.k,
          d: msg.keysOnly ? undefined : bytesToBase64(row.v),
          v: row.ver,
        });
      }
      ws.send(JSON.stringify({ id: msg.id, entries }));

    } else if (msg.op === "commit") {
      this.ctx.storage.transactionSync(() => {
        // Verify observed versions
        for (const [key, expectedVer] of Object.entries(msg.observed)) {
          const row = sql.exec("SELECT ver FROM jfs_kv WHERE k = ?", key).one();
          const curVer = row ? row.ver : 0;
          if (curVer !== expectedVer) throw new Error("write conflict");
        }
        // Apply puts
        for (const [key, b64val] of Object.entries(msg.puts || {})) {
          const row = sql.exec("SELECT ver FROM jfs_kv WHERE k = ?", key).one();
          const newVer = row ? row.ver + 1 : 1;
          sql.exec(
            "INSERT OR REPLACE INTO jfs_kv (k, v, ver) VALUES (?, ?, ?)",
            key, base64ToBytes(b64val), newVer
          );
        }
        // Apply deletes
        for (const key of (msg.dels || [])) {
          sql.exec("DELETE FROM jfs_kv WHERE k = ?", key);
        }
      });
      ws.send(JSON.stringify({ id: msg.id, ok: true }));
    }
  } catch (err) {
    if (msg.op === "commit") {
      ws.send(JSON.stringify({ id: msg.id, ok: false, error: err.message }));
    } else {
      ws.send(JSON.stringify({ id: msg.id, data: null, ver: 0, error: err.message }));
    }
  }
}
```

#### 4. Dockerfile (unchanged)

Same binary, same container setup. Only the binary internals change.

## Advantages Over SQL Proxy Approach

| Concern | SQL Proxy (V1) | TKV + DO SQLite Table |
|---------|---------------|----------------------|
| Transactions | **Broken** — can't await WebSocket inside `transactionSync` | **Works** — reads async, commit is sync SQL |
| Complexity | database/sql driver + XORM dialect + SQL registration | Single `tkvClient` implementation |
| Wire protocol | Full SQL queries with column types | Simple get/list/commit |
| JuiceFS changes | None | None |
| CGO dependency | Pulled in by XORM/SQLite dialect | None — TKV engine is pure Go |
| DO storage | 20+ tables (jfs_node, jfs_edge, jfs_chunk...) | 1 table (`jfs_kv`) |
| Open questions | lastInsertId, SQL transaction semantics | None |

## Build & Test

```bash
# Build the Go binary (from ~/src/juicefs)
cd ~/src/juicefs
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o juicefs-cf-mount ./cmd/cfmount

# Build & deploy (from deepagentsjs)
cd ~/src/deepagentsjs/examples/cloudflare-worker
pnpm run deploy
```

## Edge Cases

| Scenario | Handling |
|----------|----------|
| Container dies mid-operation | DO detects WebSocket close. SQLite data in DO persists. Next container reconnects. |
| DO eviction | DO re-hibernates, SQLite persists on disk. On wake, reconnects to container. |
| First mount (no keys) | `juicefs-cf-mount` detects Load() failure → runs Init() to bootstrap metadata |
| WebSocket disconnect during transaction | Go-side sendRPC returns error → transaction fails → JuiceFS retries |
| Write conflict | DO's transactionSync detects version mismatch → returns conflict → JuiceFS retries (built-in) |
| Large scans | SQLite handles range queries efficiently with B-tree index on PK |
| Binary keys | Hex-encoded on wire + in SQLite TEXT column. Preserves sort order. |

## Cloudflare Sandbox Integration

This section describes how to integrate `juicefs-cf-mount` with the existing
`deepagentsjs/examples/cloudflare-worker` project, which currently uses
`@cloudflare/sandbox`'s `mountBucket()` (s3fs) for `/workspace`.

### Current architecture (s3fs)

```
DO (agent-do.ts)
  → CloudflareSandbox.from(env.SANDBOX, threadId, env.WORKSPACE, mount)
  → sandbox.execute("...") triggers lazy container start
  → #startContainer() calls sandbox.mountBucket() → s3fs mounts R2 at /workspace
```

### New architecture (JuiceFS)

```
DO (agent-do.ts)
  → CloudflareSandbox.from(env.SANDBOX, threadId, mount, ctx.storage)
  → sandbox.execute("...") triggers lazy container start
  → #startContainer():
      1. Container already has juicefs-cf-mount running (Dockerfile CMD)
         - Listening on port 9876
         - /workspace shows README.txt placeholder
      2. DO opens WebSocket to container port 9876 via sandbox proxy
      3. DO sends init message with R2 credentials + volume name
      4. DO creates jfs_kv table if needed
      5. juicefs-cf-mount formats volume (first time) or loads existing metadata
      6. Container sends "ready" message
      7. /workspace is now a JuiceFS FUSE mount
      8. DO enters message loop: forward KV ops from container to DO SQLite
```

### Dockerfile changes

```dockerfile
FROM docker.io/cloudflare/sandbox:0.7.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 fuse3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the pre-built juicefs-cf-mount binary
COPY juicefs-cf-mount /usr/local/bin/juicefs-cf-mount
RUN chmod +x /usr/local/bin/juicefs-cf-mount

EXPOSE 8080

# Start the WebSocket server + placeholder mount on container boot.
# JuiceFS won't actually mount until the DO connects and sends init.
CMD ["juicefs-cf-mount", "/workspace", "--port", "9876"]
```

The binary is built separately (`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build
-o juicefs-cf-mount ./cmd/cfmount`) and copied into the Docker context before
`docker build`.

### sandbox.ts changes

Replace `#startContainer()` — remove `mountBucket()`, add WebSocket connection
to `juicefs-cf-mount` and a KV message handler loop.

#### New fields

```typescript
class CloudflareSandbox extends BaseSandbox {
  // ... existing fields ...
  #kvWebSocket: WebSocket | null = null;
  #kvPendingRequests: Map<number, {
    resolve: (msg: any) => void;
    reject: (err: Error) => void;
  }> = new Map();
  #kvNextId = 1;
  #storage: DurableObjectStorage;  // reference to DO's ctx.storage
}
```

The `CloudflareSandbox.from()` factory is simplified — the R2 bucket binding is
no longer needed (JuiceFS accesses R2 via S3 API from the container). The DO
passes `this.ctx.storage` so the sandbox can handle KV operations against DO
SQLite.

```typescript
static from(
  binding: DurableObjectNamespace<SandboxDO>,
  sandboxId: string,
  mount: R2MountConfig,
  storage: DurableObjectStorage,
  options?: CloudflareSandboxOptions,
): CloudflareSandbox
```

#### New `#startContainer()` flow

```typescript
async #startContainer(): Promise<void> {
  this.#containerStatus = "requested";

  // 1. Ensure jfs_kv table exists in DO SQLite
  this.#storage.sql.exec(`
    CREATE TABLE IF NOT EXISTS jfs_kv (
      k TEXT PRIMARY KEY,
      v BLOB NOT NULL,
      ver INTEGER NOT NULL DEFAULT 1
    )
  `);

  // 2. Open WebSocket to container's juicefs-cf-mount on port 9876
  //    via the sandbox proxy (fetch with Upgrade: websocket header)
  const ws = await this.#openWebSocket(9876);
  this.#kvWebSocket = ws;

  // 3. Send init message with R2 storage config
  ws.send(JSON.stringify({
    type: "init",
    storage: "s3",
    bucket: `https://${this.#mount.endpoint.replace('https://', '')}/${this.#mount.bucketName}`,
    accessKey: this.#mount.credentials.accessKeyId,
    secretKey: this.#mount.credentials.secretAccessKey,
    volumeName: `ws-${this.#threadId}`,
  }));

  // 4. Start message loop — handle KV requests from container
  ws.addEventListener("message", (event) => {
    const msg = JSON.parse(event.data as string);

    if (msg.type === "ready") {
      // JuiceFS mount complete — resolve the container ready promise
      this.#containerStatus = "started";
      this.#containerStartedAt = Date.now();
      return;
    }

    // KV operation from container
    if (msg.op) {
      this.#handleKvRequest(msg, ws);
      return;
    }

    // Response to a DO→container request (PrepareClone, etc.)
    const pending = this.#kvPendingRequests.get(msg.id);
    if (pending) {
      this.#kvPendingRequests.delete(msg.id);
      pending.resolve(msg);
    }
  });

  // 5. Wait for "ready" message (JuiceFS mounted)
  await this.#waitForReady();
}
```

#### KV request handler

```typescript
#handleKvRequest(msg: any, ws: WebSocket) {
  const sql = this.#storage.sql;
  try {
    if (msg.op === "get") {
      const row = sql.exec("SELECT v, ver FROM jfs_kv WHERE k = ?", msg.key).one();
      ws.send(JSON.stringify({
        id: msg.id,
        data: row ? bytesToBase64(row.v as ArrayBuffer) : null,
        ver: row ? (row.ver as number) : 0,
      }));

    } else if (msg.op === "list") {
      const cursor = sql.exec(
        "SELECT k, v, ver FROM jfs_kv WHERE k >= ? AND k < ? ORDER BY k",
        msg.start, msg.end
      );
      const entries = [];
      for (const row of cursor) {
        entries.push({
          k: row.k,
          d: msg.keysOnly ? undefined : bytesToBase64(row.v as ArrayBuffer),
          v: row.ver,
        });
      }
      ws.send(JSON.stringify({ id: msg.id, entries }));

    } else if (msg.op === "commit") {
      this.#storage.transactionSync(() => {
        for (const [key, expectedVer] of Object.entries(msg.observed)) {
          const row = sql.exec("SELECT ver FROM jfs_kv WHERE k = ?", key).one();
          const curVer = row ? (row.ver as number) : 0;
          if (curVer !== expectedVer) throw new Error("write conflict");
        }
        for (const [key, b64val] of Object.entries(msg.puts || {})) {
          const row = sql.exec("SELECT ver FROM jfs_kv WHERE k = ?", key).one();
          const newVer = row ? (row.ver as number) + 1 : 1;
          sql.exec(
            "INSERT OR REPLACE INTO jfs_kv (k, v, ver) VALUES (?, ?, ?)",
            key, base64ToBytes(b64val as string), newVer
          );
        }
        for (const key of (msg.dels || [])) {
          sql.exec("DELETE FROM jfs_kv WHERE k = ?", key);
        }
      });
      ws.send(JSON.stringify({ id: msg.id, ok: true }));
    }
  } catch (err: any) {
    if (msg.op === "commit") {
      ws.send(JSON.stringify({ id: msg.id, ok: false, error: err.message }));
    } else {
      ws.send(JSON.stringify({ id: msg.id, data: null, ver: 0, error: err.message }));
    }
  }
}
```

#### WebSocket via sandbox proxy

The `@cloudflare/sandbox` SDK provides a `fetch()` method on the sandbox handle.
To open a WebSocket to a container port:

```typescript
async #openWebSocket(port: number): Promise<WebSocket> {
  const sandboxFetch = (this.#sandbox as any).fetch.bind(this.#sandbox);
  const resp = await sandboxFetch(
    new Request(`http://localhost/proxy/${port}/ws`, {
      headers: { Upgrade: "websocket" },
    })
  );
  const ws = (resp as any).webSocket;
  if (!ws) throw new Error("WebSocket upgrade failed");
  ws.accept();
  return ws;
}
```

If the sandbox proxy doesn't support WebSocket upgrade, the fallback is to use
HTTP request/response pairs over `sandbox.fetch()` with the container running an
HTTP server instead of a WebSocket server. Each KV operation becomes a POST to
`/proxy/9876/kv` with the message as the body. This is less efficient but avoids
the WebSocket requirement.

### agent-do.ts changes

Two changes:

**1. Pass `ctx.storage` to sandbox factory:**

```typescript
// In ensureAgent():
if (this.sandboxEnabled) {
  this.sandbox = CloudflareSandbox.from(
    this.env.SANDBOX,
    this.threadId,
    {
      bucketName: "deepagents-workspace",
      endpoint: `https://${this.env.CF_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      credentials: {
        accessKeyId: this.env.R2_ACCESS_KEY_ID,
        secretAccessKey: this.env.R2_SECRET_ACCESS_KEY,
      },
    },
    this.ctx.storage,   // ← pass DO storage for SQLite + transactionSync
  );
  this.#backend = this.sandbox;
}
```

**2. Handle WebSocket lifecycle on container destroy:**

```typescript
// In recreateContainer():
async recreateContainer() {
  if (this.sandbox) {
    await this.sandbox.closeKvWebSocket(); // gracefully close WS before destroy
    await this.sandbox.destroy();
  }
  // ... rest unchanged ...
}
```

### wrangler.jsonc

No changes needed. The existing bindings cover everything:
- `SANDBOX` DO binding → container with `juicefs-cf-mount`
- `WORKSPACE` R2 binding → JuiceFS data chunks (accessed via S3 API from container)
- `DEEP_AGENT_DO` binding → the DO with `jfs_kv` in its SQLite

The `WORKSPACE` R2 bucket binding is no longer used directly by the DO for file
ops — all file operations go through the container's JuiceFS mount. The binding
can be kept for admin/debugging purposes or removed.

The Dockerfile change (adding `juicefs-cf-mount`) is picked up automatically by
the `containers[].image` reference.

### End-to-end sequence

```
1. User sends chat message
2. DO.chat() → ensureAgent() → CloudflareSandbox.from(...)
3. Agent decides to run a command → sandbox.execute("ls /workspace")
4. execute() → #ensureContainer()
5. #startContainer():
   a. CREATE TABLE IF NOT EXISTS jfs_kv (...)
   b. Open WebSocket to container:9876/ws via sandbox proxy
   c. Send init message: {type: "init", storage: "s3", bucket: ..., volumeName: "ws-THREAD_ID"}
   d. juicefs-cf-mount receives init:
      - Creates wskvClient pointed at the WebSocket
      - meta.NewClient("wskv://local", conf)
      - First time: Init() formats volume → writes metadata via KV ops → DO SQLite
      - Subsequent: Load() reads existing metadata from DO SQLite
      - Mounts JuiceFS via FUSE at /workspace
      - Sends {type: "ready"}
   e. DO receives "ready" → resolves #ensureContainer()
6. sandbox.exec("ls /workspace") runs against the JuiceFS mount
7. All file ops (read, write, ls, glob) go through the container
8. JuiceFS metadata ops flow: FUSE → kvMeta → wskvClient → WebSocket → DO SQLite
9. JuiceFS data chunks flow: FUSE → chunk store → R2 (S3 API) directly from container
10. Container restart: DO detects WS close, next execute() reopens WS + re-sends init
    (metadata persists in DO SQLite, data persists in R2 — JuiceFS re-mounts cleanly)
```

## Open Questions

1. **WebSocket via sandbox proxy** — Does `@cloudflare/sandbox`'s fetch support WebSocket upgrade? If not, alternative: HTTP long-poll or direct TCP.

2. **DO SQLite row limits** — DO SQLite supports up to 1 GB storage. For typical workspaces this is plenty (metadata is small), but worth monitoring.

3. **FUSE in container** — The `cloudflare/sandbox` base image supports FUSE (it uses s3fs). Need `fuse3` for go-fuse.
