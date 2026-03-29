# V2: DO Cloning with Shared R2 Data

## Problem

We want to clone a DO (fork a workspace) without duplicating R2 data. Both the original and clone should see the same files, but writes after the clone point are independent. JuiceFS's per-volume GC would delete shared chunks — we need a safe ownership model.

## Design: UUID-per-clone + Union Object Storage

Each clone gets a fresh volume UUID and its own R2 prefix for writes. Reads fall through a parent chain (like Docker layers or git objects).

```
DO-A (uuid: aaa)
  writes to:  aaa/chunks/...
  reads from: aaa/chunks/...
     ↓ clone
DO-B (uuid: bbb, parents: [aaa])
  writes to:  bbb/chunks/...
  reads from: bbb/ → aaa/
     ↓ clone
DO-C (uuid: ccc, parents: [bbb, aaa])
  writes to:  ccc/chunks/...
  reads from: ccc/ → bbb/ → aaa/
```

### No chunk ID collision

Each DO writes to its own R2 prefix. Even if two DOs allocate the same sequential chunk ID (5001), they write to different keys (`bbb/chunks/5001` vs `aaa/chunks/5001`). No collision possible.

### Read optimization

Record `clonePointChunkId` during clone (the parent's `nextChunk` counter at clone time). For reads:
- chunk ID > clonePointChunkId → definitely in own prefix (written after clone)
- chunk ID ≤ clonePointChunkId → try parent prefix first (written before clone)

This eliminates most 404 fallback probes.

## Union Object Storage

A wrapper around JuiceFS's `ObjectStorage` interface, injected in `juicefs-cf-mount`:

```go
type UnionStorage struct {
    primary   object.ObjectStorage   // own prefix — reads + writes
    fallbacks []object.ObjectStorage // parent prefixes — reads only
}

func (u *UnionStorage) Get(key string, off, limit int64) (io.ReadCloser, error) {
    r, err := u.primary.Get(key, off, limit)
    if err == nil {
        return r, nil
    }
    for _, fb := range u.fallbacks {
        r, err = fb.Get(key, off, limit)
        if err == nil {
            return r, nil
        }
    }
    return nil, err
}

func (u *UnionStorage) Put(key string, in io.Reader) error {
    return u.primary.Put(key, in)
}

func (u *UnionStorage) Delete(key string) error {
    return u.primary.Delete(key) // only delete from own prefix
}
```

Passed to `chunk.NewCachedStore(unionBlob, ...)` instead of the raw R2 storage. No JuiceFS core changes needed — the union sits outside JuiceFS.

## Keeping KV fully opaque at the DO layer

### The principle

The DO/TypeScript side never parses, inspects, or modifies any jfs_kv keys or
values. All KV data is opaque binary. Any operation that requires understanding
the KV format (reading the UUID, reading counters, producing modified setting
bytes for a clone) is performed by the Go side and exposed as wskv protocol
messages.

This keeps all JuiceFS format knowledge in Go and avoids a second
implementation in TypeScript that would need to track Go-side changes.

### Protocol messages for clone support

Two new message pairs are added to the `WskvMessage` envelope in `wskv.proto`.
These are **container → DO direction** (the DO sends requests, the container
responds), same as all other wskv messages.

#### PrepareClone

The DO sends `PrepareCloneRequest` with the new clone's UUID. The Go side
reads its own metadata, produces a modified "setting" value with the new UUID
baked in, and returns everything the DO needs — all as opaque bytes.

```protobuf
message PrepareCloneRequest {
  uint64 id = 1;
  string new_uuid = 2;       // UUID the DO generated for the clone
}

message PrepareCloneResponse {
  uint64 id = 1;
  string parent_uuid = 2;    // this volume's current UUID (becomes parent)
  int64  next_chunk = 3;     // current nextChunk counter (for clonePointChunkId)
  bytes  setting_key = 4;    // the raw key bytes for the setting row
  bytes  setting_value = 5;  // the modified setting value with new_uuid applied
}
```

The DO uses `setting_key` and `setting_value` to overwrite one row in the
cloned jfs_kv table. It never interprets the bytes — just a blind
`INSERT OR REPLACE INTO jfs_kv (k, v, ver) VALUES (?, ?, 1)`.

On the Go side:

```go
func handlePrepareClone(meta Meta, req *pb.PrepareCloneRequest) *pb.PrepareCloneResponse {
    format, _ := meta.Load(false)
    settingKey := []byte("setting")

    // Produce modified setting with new UUID
    cloneFormat := format
    cloneFormat.UUID = req.NewUuid
    cloneSettingJSON, _ := json.Marshal(cloneFormat)

    return &pb.PrepareCloneResponse{
        Id:           req.Id,
        ParentUuid:   format.UUID,
        NextChunk:    meta.NextChunk(),
        SettingKey:   settingKey,
        SettingValue: cloneSettingJSON,
    }
}
```

#### GetVolumeMeta

A general-purpose message for the DO to query volume-level metadata without
understanding the KV encoding. Useful for health checks, status pages, and
any future operation that needs volume info.

```protobuf
message GetVolumeMetaRequest {
  uint64 id = 1;
}

message GetVolumeMetaResponse {
  uint64 id = 1;
  string uuid = 2;
  string volume_name = 3;
  int64  next_chunk = 4;
  int64  used_space = 5;     // bytes
  int64  total_inodes = 6;
}
```

This replaces any need for the DO to peek at KV data for operational purposes.

### Container must be running

Both PrepareClone and GetVolumeMeta require the container's WebSocket to be
connected. This is the normal case — workspaces have a running container. If
the container is not running, the DO spins it up briefly (same as any other
operation that needs the filesystem).

### Caching in the ready message (optimization)

The container can include `uuid` and `next_chunk` in its initial "ready"
WebSocket message (sent after mounting). The DO caches these values. For clone
operations, the DO can use cached values as a fallback if the container is
unavailable — a stale `nextChunk` just means slightly more 404 fallback probes
during reads (always safe, never incorrect). However, `setting_value` still
requires a live PrepareClone call since it must embed the clone's specific UUID.

## Clone Operation

```
DO-A.clone(newThreadId):

  1. Generate new UUID for clone
  2. Send PrepareCloneRequest{new_uuid} to container
     → get {parent_uuid, next_chunk, setting_key, setting_value}
  3. Bulk-copy all jfs_kv rows from A's SQLite to new DO's SQLite
     (rows are opaque binary — copied as-is, no interpretation needed)
  4. In the clone's jfs_kv, overwrite the setting row:
     INSERT OR REPLACE INTO jfs_kv (k, v, ver) VALUES (setting_key, setting_value, 1)
     (DO treats both key and value as opaque blobs — no parsing)
  5. Set clone's metadata in its own SQLite:
     - parentChain = [parent_uuid, ...A.parentChain]
     - clonePointChunkId = next_chunk from step 2
     - rootThreadId = A.rootThreadId (or A's own threadId if A is a root)
  6. Walk up to root DO → root.registerDescendant({threadId, uuid, parentChain})
```

The clone's `cfmount` receives the parent chain in the init message:

```json
{
  "type": "init",
  "storage": "s3",
  "bucket": "https://ACCOUNT.r2.cloudflarestorage.com/deepagents-workspace",
  "accessKey": "...",
  "secretKey": "...",
  "volumeName": "ws-CLONE_THREAD_ID",
  "uuid": "bbb",
  "parentUuids": ["aaa"],
  "clonePointChunkId": 5000
}
```

The binary creates a `UnionStorage` with:
- primary: R2 prefix `bbb/`
- fallbacks: [R2 prefix `aaa/`]

## GC: Root-coordinated Sweep

### Per-DO: no deletion

Set `MaxDeletes = 0` in JuiceFS meta config. Refcounts still decrement in metadata, but chunks are never deleted from R2 by JuiceFS itself.

### Root DO as family coordinator

Every DO knows its `rootThreadId` (stored in its own SQLite). The root is the original, un-cloned workspace. It has two roles:

1. **Workspace** — normal agent/JuiceFS functionality (can be "deleted")
2. **Coordinator** — maintains the family tree of all descendant volumes

The root's SQLite has a `jfs_volume_family` table:

```sql
CREATE TABLE jfs_volume_family (
  thread_id  TEXT PRIMARY KEY,
  uuid       TEXT NOT NULL,
  parent_chain TEXT NOT NULL,  -- JSON array of UUIDs
  status     TEXT NOT NULL DEFAULT 'active'  -- 'active' or 'deleted'
);
```

The root also has its own entry (with itself as a volume).

### Finding the root

Every DO stores `rootThreadId` in its own SQLite. To reach the root:
- Get the root DO stub via `env.DEEP_AGENT_DO.idFromName(rootThreadId)`
- Call RPC methods on it

No chain walking needed at runtime — each DO has a direct reference to the root. The `rootThreadId` is set at clone time and never changes.

(The parentChain walk is only needed if the rootThreadId were ever lost, as a recovery mechanism.)

### Delete operation

When a user deletes workspace B:

```
deleteWorkspace(B):

  1. B reads its rootThreadId from its own SQLite
  2. B calls root.markDeleted(B.threadId)
  3. Root updates jfs_volume_family: SET status='deleted' WHERE thread_id=B
  4. Root runs sweep (inline, synchronous — see below)
  5. B clears its own workspace data (jfs_kv rows, agent tables, checkpoints)
     but keeps rootThreadId and its own volume metadata
  6. If root's sweep cleaned B's R2 prefix:
     root calls B.completeDeletion()
     → B clears remaining metadata, DO becomes truly empty
```

### When the root itself is deleted

The root is "deleted" as a workspace but persists as a hollow coordinator:

```
deleteWorkspace(root):

  1. Root marks itself as deleted in jfs_volume_family
  2. Root runs sweep — its own R2 prefix may or may not be orphaned
     (if descendants still reference root's UUID in their parentChain, it stays)
  3. Root clears workspace data (jfs_kv rows, agent tables, checkpoints)
  4. Root keeps jfs_volume_family table — coordinator role persists
  5. When the last descendant is deleted and sweep cleans everything:
     root clears jfs_volume_family, DO becomes truly empty
```

### Sweep (runs on root DO)

Triggered inline on every `markDeleted()` call — no cron needed:

```
root.sweep():

  1. Read all rows from jfs_volume_family
  2. referencedUuids = union of (uuid + parentChain) for all rows WHERE status='active'
     (deleted entries contribute nothing — they no longer need any data)
  3. orphanedUuids = all UUIDs that appear in ANY row but NOT in referencedUuids
  4. For each orphanedUuid:
     - Skip if any R2 object under that prefix was uploaded < 24h ago
     - Delete all R2 objects under {orphanedUuid}/ prefix
  5. For each row WHERE status='deleted' AND uuid IN orphanedUuids:
     - Call that DO's completeDeletion() via RPC
     - Delete the row from jfs_volume_family
  6. If all rows are gone (including root's own):
     - Root clears jfs_volume_family table
     - Root DO becomes truly empty
```

### Example lifecycle

```
A created (root)
  A.jfs_volume_family:
    {A, uuid:aaa, parents:[], active}
  R2: {aaa/}

B cloned from A
  A.jfs_volume_family:
    {A, uuid:aaa, parents:[], active}
    {B, uuid:bbb, parents:[aaa], active}
  R2: {aaa/, bbb/}

C cloned from B
  A.jfs_volume_family:
    {A, uuid:aaa, parents:[], active}
    {B, uuid:bbb, parents:[aaa], active}
    {C, uuid:ccc, parents:[bbb,aaa], active}
  R2: {aaa/, bbb/, ccc/}

B deleted → root.markDeleted(B) → root.sweep()
  A.jfs_volume_family:
    {A, uuid:aaa, parents:[], active}
    {B, uuid:bbb, parents:[aaa], deleted}    ← marked deleted
    {C, uuid:ccc, parents:[bbb,aaa], active}
  referencedUuids (from active only) = {aaa, ccc, bbb, aaa} = {aaa, bbb, ccc}
  orphaned = {} (bbb still in C's parentChain)
  → nothing to clean yet
  B clears its workspace data, keeps rootThreadId

C deleted → root.markDeleted(C) → root.sweep()
  A.jfs_volume_family:
    {A, uuid:aaa, parents:[], active}
    {B, uuid:bbb, parents:[aaa], deleted}
    {C, uuid:ccc, parents:[bbb,aaa], deleted}  ← marked deleted
  referencedUuids (from active only) = {aaa}
  orphaned = {bbb, ccc}
  → delete bbb/ and ccc/ from R2
  → call B.completeDeletion(), C.completeDeletion()
  → remove B and C rows from table
  A.jfs_volume_family:
    {A, uuid:aaa, parents:[], active}
  R2: {aaa/}

A deleted → root.markDeleted(A) → root.sweep()
  A.jfs_volume_family:
    {A, uuid:aaa, parents:[], deleted}
  referencedUuids (from active only) = {}
  orphaned = {aaa}
  → delete aaa/ from R2
  → remove A's own row
  → table empty → root clears everything, DO becomes empty
  R2: {}
```

### Race condition: mid-write chunks

JuiceFS writes R2 data before committing metadata. The sweep skips any R2 prefix where the most recent object was uploaded less than 24 hours ago. Writes complete in seconds; 24h grace period makes premature deletion impossible.

### Properties

- **No external registry** — the root DO's SQLite is the source of truth (strongly consistent)
- **No cron needed** — sweep runs inline on each deletion
- **No inter-DO coordination for reads/writes** — only clone and delete touch the root
- **Root persists as hollow coordinator** — workspace can be deleted, coordinator role remains until family is gone
- **Each DO knows its root** — direct reference via `rootThreadId`, no chain walking

## Data Model

### Each DO's SQLite (own metadata)

JuiceFS metadata is stored in a single KV table (see V1 plan):

```sql
CREATE TABLE IF NOT EXISTS jfs_kv (
  k TEXT PRIMARY KEY,
  v BLOB NOT NULL,
  ver INTEGER NOT NULL DEFAULT 1
);
```

Volume identity is stored in a separate table:

```sql
-- Volume identity (in jfs_cf_meta or a dedicated table)
-- Set at creation/clone time, never changes
rootThreadId  TEXT  -- thread ID of the root DO
uuid          TEXT  -- this volume's UUID
parentChain   TEXT  -- JSON array of parent UUIDs
clonePointChunkId  INTEGER  -- parent's nextChunk at clone time
```

### Root DO's SQLite (family coordinator)

```sql
CREATE TABLE jfs_volume_family (
  thread_id    TEXT PRIMARY KEY,
  uuid         TEXT NOT NULL,
  parent_chain TEXT NOT NULL,     -- JSON array of UUIDs
  status       TEXT NOT NULL DEFAULT 'active',
  created_at   TEXT NOT NULL
);
```

### Init message (extended from V1)

```json
{
  "type": "init",
  "storage": "s3",
  "bucket": "...",
  "accessKey": "...",
  "secretKey": "...",
  "volumeName": "ws-THREAD_ID",
  "uuid": "bbb",
  "parentUuids": ["aaa"],
  "clonePointChunkId": 5000
}
```

## Summary

| Concern | Solution |
|---------|----------|
| Chunk ID collision | Each clone writes to its own R2 prefix (uuid-namespaced) |
| Shared reads | UnionStorage falls through parent chain |
| Read perf | clonePointChunkId skips 404 probes for post-clone chunks |
| GC safety | MaxDeletes=0, no per-DO deletion |
| Cleanup | Root DO runs sweep inline on each markDeleted() |
| Root deletion | Root becomes hollow coordinator, persists until family is gone |
| Consistency | Root DO SQLite — strongly consistent, no KV |
| Opaque KV | DO never parses KV data; PrepareClone + GetVolumeMeta RPCs expose what DO needs |
| JuiceFS changes | None — UnionStorage wraps the ObjectStorage interface externally |
