#ifndef JUICEFS_SMARTMAP_H
#define JUICEFS_SMARTMAP_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct jfs_smartmap_client jfs_smartmap_client;
typedef struct jfs_smartmap_mapping jfs_smartmap_mapping;

typedef struct jfs_smartmap_range {
  uint64_t file_offset;
  uint64_t length;
  uint64_t shm_offset;
} jfs_smartmap_range;

typedef struct jfs_smartmap_extent {
  uint64_t file_offset;
  uint64_t length;
  uint64_t shm_offset;
} jfs_smartmap_extent;

typedef int (*jfs_smartmap_control_cb)(void *userdata,
                                       const jfs_smartmap_range *ranges,
                                       size_t len);
typedef int (*jfs_smartmap_mutator_cb)(void *userdata);
typedef int (*jfs_smartmap_page_synced_cb)(void *userdata, size_t offset);

typedef struct jfs_smartmap_callbacks {
  void *userdata;
  jfs_smartmap_control_cb release;
  jfs_smartmap_control_cb probe;
  jfs_smartmap_control_cb write_fault;
} jfs_smartmap_callbacks;

typedef struct jfs_smartmap_sync_callbacks {
  void *userdata;
  jfs_smartmap_mutator_cb pause;
  jfs_smartmap_mutator_cb resume;
  jfs_smartmap_page_synced_cb page_synced;
} jfs_smartmap_sync_callbacks;

int jfs_smartmap_client_new(const char *socket,
                            jfs_smartmap_client **out,
                            char **err);
void jfs_smartmap_client_free(jfs_smartmap_client *client);

int jfs_smartmap_mapping_open(const jfs_smartmap_client *client,
                              const char *path,
                              uint64_t size,
                              jfs_smartmap_mapping **out,
                              char **err);
void *jfs_smartmap_mapping_ptr(const jfs_smartmap_mapping *mapping);
size_t jfs_smartmap_mapping_len(const jfs_smartmap_mapping *mapping);
int jfs_smartmap_mapping_raw_fd(const jfs_smartmap_mapping *mapping);
size_t jfs_smartmap_mapping_extent_count(const jfs_smartmap_mapping *mapping);
int jfs_smartmap_mapping_extent_at(const jfs_smartmap_mapping *mapping,
                                   size_t index,
                                   jfs_smartmap_extent *out);
int jfs_smartmap_mapping_sync(const jfs_smartmap_mapping *mapping,
                              const char *writeback_path,
                              const jfs_smartmap_sync_callbacks *callbacks,
                              char **err);
int jfs_smartmap_run_control_loop(jfs_smartmap_mapping *mapping,
                                  const jfs_smartmap_callbacks *callbacks,
                                  char **err);
int jfs_smartmap_handle_next_control(jfs_smartmap_mapping *mapping,
                                     const jfs_smartmap_callbacks *callbacks,
                                     int *handled,
                                     char **err);
int jfs_smartmap_mapping_shutdown(jfs_smartmap_mapping *mapping, char **err);
void jfs_smartmap_mapping_free(jfs_smartmap_mapping *mapping);

void jfs_smartmap_string_free(char *s);

#ifdef __cplusplus
}
#endif

#endif
