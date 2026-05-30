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

Dirty writeback policy intentionally belongs to the embedder. The control
handler is where a VM runtime should flush dirty private pages through the
mounted JuiceFS path before acknowledging an eviction.

## C ABI

The crate also exports a C ABI and installs declarations in
`include/juicefs_smartmap.h`.

```c
#include "juicefs_smartmap.h"

static int evict(void *userdata, const jfs_smartmap_range *ranges, size_t len) {
  /* Flush dirty private pages through the mounted JuiceFS file. */
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

  jfs_smartmap_callbacks callbacks = {.userdata = NULL, .evict = evict, .probe = NULL};
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
