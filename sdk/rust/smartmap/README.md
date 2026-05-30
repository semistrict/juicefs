# JuiceFS Smartmap Rust Client

This crate is an embeddable Linux client for the JuiceFS Smartmap socket API.
It opens a JuiceFS memory file, receives the server memfd with `SCM_RIGHTS`,
maps the returned extents as one contiguous private mapping, creates and
registers a `userfaultfd`, and starts the `serve_memory_faults` session.

```rust,no_run
use juicefs_smartmap::{Client, ControlHandler};

struct Handler;

impl ControlHandler for Handler {
    fn evict(&mut self, _ranges: &[juicefs_smartmap::ControlRange]) -> std::io::Result<()> {
        // Flush dirty private pages through the mounted JuiceFS file, then drop/remap
        // the affected ranges if the embedder keeps additional mapping state.
        Ok(())
    }
}

fn main() -> juicefs_smartmap::Result<()> {
    let client = Client::new("/tmp/smartmap.sock");
    let memory = client.open_memory_file("/vm-memory", 1024 * 1024 * 1024)?;
    let mapping = memory.map_private_contiguous()?;
    let uffd = memory.create_userfaultfd(&mapping)?;
    let mut session = memory.serve_faults(&uffd, &mapping)?;

    std::thread::spawn(move || {
        let mut handler = Handler;
        let _ = session.run_control_loop(&mut handler);
    });

    Ok(())
}
```

Dirty writeback policy intentionally belongs to the embedder. A VM runtime
should flush dirty private pages through the mounted JuiceFS path on its own
timer or policy; Smartmap eviction only drops shared clean backing on the
server. `Mapping::sync_dirty` pauses the mutator only long enough to find and
write-protect dirty pages, then resumes the mutator while the dirty pages are
written through the mounted JuiceFS path. If the mutator writes a protected page
before the background copy reaches it, the server forwards a synchronous
`write_fault` control message; the client flushes that page, then the server
unprotects and wakes the blocked write. In Firecracker, the pause/resume hooks
are where the VM should stop and start vCPUs around the snapshot step.

## Read path

Opening and mapping a Smartmap file does not read the file bytes from object
storage. The server resolves the JuiceFS file, records the page layout, returns
shared backing extents, and the client registers its private virtual range with
its own `userfaultfd`. Object data is only needed after the client actually
touches a page that is not already resident in the shared backing memory.

When the client reads a byte at file offset `X`, the CPU first tries to satisfy
the load from the client's private contiguous mapping. If that 2 MiB page is
already present in the client, the load is just a normal memory read. If it is
not present, the kernel reports a UFFD fault for that client mapping. The
Smartmap server maps the fault address back to `X`, finds the corresponding
2 MiB shared page, and either continues the fault to an already-populated shared
page or reads that 2 MiB range from JuiceFS into the shared backing memory
before continuing the fault. That JuiceFS read is the point where object storage
may be reached if the data is not already cached locally.

## C ABI

The crate also exports a C ABI and installs declarations in
`include/juicefs_smartmap.h`.

```c
#include "juicefs_smartmap.h"

static int evict(void *userdata, const jfs_smartmap_range *ranges, size_t len) {
  /* Record/acknowledge server shared-page eviction. Dirty writeback is separate. */
  return 0;
}

static int pause_mutator(void *userdata) {
  /* Pause the VM/vCPUs before dirty-page sync. */
  return 0;
}

static int resume_mutator(void *userdata) {
  /* Resume the VM/vCPUs after dirty-page sync. */
  return 0;
}

int open_vm_memory(void) {
  char *err = NULL;
  jfs_smartmap_client *client = NULL;
  jfs_smartmap_memory *memory = NULL;
  jfs_smartmap_mapping *mapping = NULL;
  jfs_smartmap_uffd *uffd = NULL;
  jfs_smartmap_session *session = NULL;

  if (jfs_smartmap_client_new("/tmp/smartmap.sock", &client, &err) != 0) goto fail;
  if (jfs_smartmap_open_memory(client, "/vm-memory", 1024ULL * 1024 * 1024, &memory, &err) != 0) goto fail;
  if (jfs_smartmap_map_private(memory, &mapping, &err) != 0) goto fail;
  if (jfs_smartmap_create_uffd(memory, mapping, &uffd, &err) != 0) goto fail;
  /* Or attach an embedder-owned registered UFFD:
   * if (jfs_smartmap_uffd_from_fd(existing_uffd_fd, &uffd, &err) != 0) goto fail;
   */
  if (jfs_smartmap_serve_faults(memory, uffd, mapping, &session, &err) != 0) goto fail;

  void *base = jfs_smartmap_mapping_ptr(mapping);
  size_t len = jfs_smartmap_mapping_len(mapping);
  (void)base;
  (void)len;

  jfs_smartmap_sync_callbacks sync_callbacks = {
      .userdata = NULL,
      .pause = pause_mutator,
      .resume = resume_mutator,
      .page_synced = NULL};
  if (jfs_smartmap_mapping_sync(mapping, uffd, "/jfs/vm-memory", &sync_callbacks, &err) != 0) goto fail;

  jfs_smartmap_callbacks callbacks = {
      .userdata = NULL, .evict = evict, .probe = NULL, .write_fault = NULL};
  return jfs_smartmap_run_control_loop(session, &callbacks, &err);

  /* Event-loop embedders can instead call one control step at a time:
   * int handled = 0;
   * if (jfs_smartmap_handle_next_control(session, &callbacks, &handled, &err) != 0) goto fail;
   */

fail:
  jfs_smartmap_string_free(err);
  return -1;
}
```
