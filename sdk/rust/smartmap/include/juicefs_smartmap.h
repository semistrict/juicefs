#ifndef JUICEFS_SMARTMAP_H
#define JUICEFS_SMARTMAP_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct jfs_smartmap_client jfs_smartmap_client;
typedef struct jfs_smartmap_memory jfs_smartmap_memory;
typedef struct jfs_smartmap_mapping jfs_smartmap_mapping;
typedef struct jfs_smartmap_uffd jfs_smartmap_uffd;
typedef struct jfs_smartmap_session jfs_smartmap_session;

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

typedef struct jfs_smartmap_callbacks {
  void *userdata;
  jfs_smartmap_control_cb evict;
  jfs_smartmap_control_cb probe;
} jfs_smartmap_callbacks;

int jfs_smartmap_client_new(const char *socket,
                            jfs_smartmap_client **out,
                            char **err);
void jfs_smartmap_client_free(jfs_smartmap_client *client);

int jfs_smartmap_open_memory(const jfs_smartmap_client *client,
                             const char *path,
                             uint64_t size,
                             jfs_smartmap_memory **out,
                             char **err);
const char *jfs_smartmap_memory_id(const jfs_smartmap_memory *memory);
int jfs_smartmap_memory_raw_fd(const jfs_smartmap_memory *memory);
size_t jfs_smartmap_memory_extent_count(const jfs_smartmap_memory *memory);
int jfs_smartmap_memory_extent_at(const jfs_smartmap_memory *memory,
                                  size_t index,
                                  jfs_smartmap_extent *out);
int jfs_smartmap_memory_close(jfs_smartmap_memory *memory, char **err);
void jfs_smartmap_memory_free(jfs_smartmap_memory *memory);

int jfs_smartmap_map_private(const jfs_smartmap_memory *memory,
                             jfs_smartmap_mapping **out,
                             char **err);
void *jfs_smartmap_mapping_ptr(const jfs_smartmap_mapping *mapping);
size_t jfs_smartmap_mapping_len(const jfs_smartmap_mapping *mapping);
void jfs_smartmap_mapping_free(jfs_smartmap_mapping *mapping);

int jfs_smartmap_create_uffd(const jfs_smartmap_memory *memory,
                             const jfs_smartmap_mapping *mapping,
                             jfs_smartmap_uffd **out,
                             char **err);
int jfs_smartmap_uffd_from_fd(int fd, jfs_smartmap_uffd **out, char **err);
int jfs_smartmap_uffd_raw_fd(const jfs_smartmap_uffd *uffd);
void jfs_smartmap_uffd_free(jfs_smartmap_uffd *uffd);

int jfs_smartmap_serve_faults(const jfs_smartmap_memory *memory,
                              const jfs_smartmap_uffd *uffd,
                              const jfs_smartmap_mapping *mapping,
                              jfs_smartmap_session **out,
                              char **err);
int jfs_smartmap_serve_faults_len(const jfs_smartmap_memory *memory,
                                  const jfs_smartmap_uffd *uffd,
                                  const jfs_smartmap_mapping *mapping,
                                  size_t len,
                                  jfs_smartmap_session **out,
                                  char **err);
int jfs_smartmap_run_control_loop(jfs_smartmap_session *session,
                                  const jfs_smartmap_callbacks *callbacks,
                                  char **err);
int jfs_smartmap_handle_next_control(jfs_smartmap_session *session,
                                     const jfs_smartmap_callbacks *callbacks,
                                     int *handled,
                                     char **err);
int jfs_smartmap_session_shutdown(jfs_smartmap_session *session, char **err);
void jfs_smartmap_session_free(jfs_smartmap_session *session);

void jfs_smartmap_string_free(char *s);

#ifdef __cplusplus
}
#endif

#endif
