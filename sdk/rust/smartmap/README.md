# JuiceFS Smartmap Rust Client

This crate is an embeddable Linux client for the JuiceFS Smartmap socket API.
Smartmap gives a runtime one contiguous private mapping of a JuiceFS file while
the mounted JuiceFS process deduplicates clean 2 MiB ranges in shared hugetlb
memory and services page faults with `userfaultfd`.

Opening a mapping creates one persistent Unix socket session for one file. The
client sends a `map` frame, receives a read-only shared-memory fd plus extents,
maps those extents privately as one virtual range, creates and registers its own
`userfaultfd`, sends `attach`, and then uses the same socket for synchronous
server controls until the mapping is closed.

```rust,no_run
use juicefs_smartmap::{Client, ControlHandler};

struct Handler;

impl ControlHandler for Handler {
    fn release(&mut self, _ranges: &[juicefs_smartmap::ControlRange]) -> std::io::Result<()> {
        Ok(())
    }

    fn probe(&mut self, _ranges: &[juicefs_smartmap::ControlRange]) -> std::io::Result<()> {
        Ok(())
    }

    fn write_fault(&mut self, _ranges: &[juicefs_smartmap::ControlRange]) -> std::io::Result<()> {
        Ok(())
    }
}

fn main() -> juicefs_smartmap::Result<()> {
    let client = Client::new("/tmp/smartmap.sock");
    let mut mapping = client.open_mapping("/vm-memory", 1024 * 1024 * 1024)?;
    let ptr = mapping.as_ptr();
    let len = mapping.len();
    let _ = (ptr, len);

    std::thread::spawn(move || {
        let mut handler = Handler;
        let _ = mapping.run_control_loop(&mut handler);
    });

    Ok(())
}
```

Dirty writeback policy belongs to the embedder. A VM runtime should write dirty
private pages back through the mounted JuiceFS path on its own timer or sync
policy. `SmartMapping::sync_dirty` pauses the mutator only long enough to find
and write-protect dirty pages, then resumes it while those pages are written
through the mounted path. If the mutator writes a protected page before the
background copy reaches it, the server sends a synchronous `write_fault`
control; the client finishes that page's writeback, acks the control, and the
server unprotects and wakes the blocked write.

## Read Path

Opening a Smartmap file does not read file bytes from object storage. The server
resolves the JuiceFS file and records its clean 2 MiB page sources. Object data
is read only when a client touches a page that is not already resident in the
shared backing memory.

On a read fault, the server maps the fault address to a file offset, finds the
deduplicated shared page, loads that 2 MiB range from JuiceFS if needed, and
continues or copies the fault from the shared backing. Clean pages can be
released later for memory pressure; private dirty pages stay client-owned until
the embedder writes them back through FUSE.

## C ABI

The C ABI exposes one opaque client and one opaque mapping object. It hides the
memory fd, `userfaultfd`, and session details from normal callers.

```c
#include "juicefs_smartmap.h"

static int release(void *userdata, const jfs_smartmap_range *ranges, size_t len) {
  return 0;
}

static int pause_mutator(void *userdata) {
  return 0;
}

static int resume_mutator(void *userdata) {
  return 0;
}

int open_vm_memory(void) {
  char *err = NULL;
  jfs_smartmap_client *client = NULL;
  jfs_smartmap_mapping *mapping = NULL;

  if (jfs_smartmap_client_new("/tmp/smartmap.sock", &client, &err) != 0) goto fail;
  if (jfs_smartmap_mapping_open(client, "/vm-memory", 1024ULL * 1024 * 1024,
                                &mapping, &err) != 0) goto fail;

  void *base = jfs_smartmap_mapping_ptr(mapping);
  size_t len = jfs_smartmap_mapping_len(mapping);
  (void)base;
  (void)len;

  jfs_smartmap_sync_callbacks sync_callbacks = {
      .userdata = NULL,
      .pause = pause_mutator,
      .resume = resume_mutator,
      .page_synced = NULL};
  if (jfs_smartmap_mapping_sync(mapping, "/jfs/vm-memory",
                                &sync_callbacks, &err) != 0) goto fail;

  jfs_smartmap_callbacks callbacks = {
      .userdata = NULL,
      .release = release,
      .probe = NULL,
      .write_fault = NULL};
  if (jfs_smartmap_run_control_loop(mapping, &callbacks, &err) != 0) goto fail;

  jfs_smartmap_mapping_free(mapping);
  jfs_smartmap_client_free(client);
  return 0;

fail:
  jfs_smartmap_string_free(err);
  jfs_smartmap_mapping_free(mapping);
  jfs_smartmap_client_free(client);
  return -1;
}
```
