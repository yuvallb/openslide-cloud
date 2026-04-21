/*
 *  OpenSlide, a library for reading whole slide image files
 *
 *  Copyright (c) 2026 Carnegie Mellon University
 *  All rights reserved.
 *
 *  OpenSlide is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Lesser General Public License as
 *  published by the Free Software Foundation, version 2.1.
 *
 *  OpenSlide is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with OpenSlide. If not, see
 *  <http://www.gnu.org/licenses/>.
 *
 */

/*
 * Internal header shared by all openslide-storage-*.c translation units.
 * Provides full struct definitions for the opaque storage types and
 * declarations for cross-TU helpers.
 *
 * Each .c file must #include <config.h> before including this header so
 * that the HAVE_*_PROVIDER macros are visible.
 */

#pragma once

#include "openslide-private.h"

#include <stdint.h>
#include <glib.h>

#if defined(HAVE_S3_PROVIDER) || defined(HAVE_GCS_PROVIDER) || defined(HAVE_AZURE_PROVIDER)
#include <curl/curl.h>
#endif

/* Full definitions of opaque storage types */

struct _openslide_storage_provider_ops {
  bool (*from_local_path)(struct _openslide_storage_provider *provider,
                          const char *path,
                          struct _openslide_object_ref **out,
                          GError **err);

  bool (*exists)(struct _openslide_storage_provider *provider,
                 const struct _openslide_object_ref *ref,
                 GError **err);

  bool (*stat)(struct _openslide_storage_provider *provider,
               const struct _openslide_object_ref *ref,
               struct _openslide_object_stat *out,
               GError **err);

  struct _openslide_readable *(*open_readable)(struct _openslide_storage_provider *provider,
                                               const struct _openslide_object_ref *ref,
                                               GError **err);

  bool (*resolve_child)(struct _openslide_storage_provider *provider,
                        const struct _openslide_object_ref *base,
                        const char *child_name,
                        struct _openslide_object_ref **out,
                        GError **err);

  bool (*list_children)(struct _openslide_storage_provider *provider,
                        const struct _openslide_object_ref *base,
                        GPtrArray **out,
                        GError **err);

  void (*destroy)(struct _openslide_storage_provider *provider);
};

struct _openslide_storage_provider {
  const struct _openslide_storage_provider_ops *ops;
  gpointer provider_data;
};

struct _openslide_object_ref {
  int refcount;
  struct _openslide_storage_provider *provider;
  char *debug_name;
  gpointer provider_data;
  GDestroyNotify provider_data_destroy;
};

struct _openslide_readable_ops {
  bool (*get_size)(struct _openslide_readable *obj,
                   int64_t *size_out,
                   GError **err);

  bool (*read_at)(struct _openslide_readable *obj,
                  int64_t offset,
                  void *buf,
                  size_t len,
                  size_t *bytes_read,
                  GError **err);

  void (*close)(struct _openslide_readable *obj);
};

struct _openslide_readable {
  const struct _openslide_readable_ops *ops;
  struct _openslide_object_ref *ref;
  gpointer provider_handle_data;
};

struct local_object_ref_data {
  char *path;
};

struct local_readable_data {
  FILE *fp;
  int64_t size;
};

#ifdef HAVE_S3_PROVIDER
struct s3_metadata_cache_entry {
  bool valid;
  struct _openslide_object_stat stat;
};

struct s3_range_cache_entry {
  uint8_t *data;
  size_t len;
};

struct s3_inflight_entry {
  GCond done_cond;
  bool done;
  bool success;
  int waiters;
  uint8_t *data;
  size_t len;
  char *error_message;
};

struct s3_settings {
  int refcount;
  uint32_t connection_timeout_ms;
  uint32_t read_timeout_ms;
  uint32_t max_retries;
  uint32_t max_parallel_requests;
  uint64_t storage_cache_bytes;
  char *region;
  char *endpoint;
  char *access_key_id;
  char *secret_access_key;
  char *session_token;
  GMutex metadata_cache_mutex;
  GHashTable *metadata_cache;
  GMutex range_cache_mutex;
  GHashTable *range_cache;
  GQueue *range_lru;
  uint64_t range_cache_current_bytes;
  GHashTable *inflight_ranges;
};

struct s3_object_ref_data {
  struct s3_settings *settings;
  char *bucket;
  char *key;
};

struct s3_readable_data {
  struct s3_object_ref_data *obj;
  int64_t size;
};

struct s3_http_result {
  long http_status;
  uint8_t *body;
  size_t body_len;
  int64_t content_length;
};
#endif

#ifdef HAVE_GCS_PROVIDER
struct gcs_settings {
  int refcount;
  uint32_t connection_timeout_ms;
  uint32_t read_timeout_ms;
  uint32_t max_retries;
  char *endpoint;
  char *bearer_token;
};

struct gcs_object_ref_data {
  struct gcs_settings *settings;
  char *bucket;
  char *key;
};

struct gcs_readable_data {
  struct gcs_object_ref_data *obj;
  int64_t size;
};

struct gcs_http_result {
  long http_status;
  uint8_t *body;
  size_t body_len;
  int64_t content_length;
};
#endif

#ifdef HAVE_AZURE_PROVIDER
struct azure_settings {
  int refcount;
  uint32_t connection_timeout_ms;
  uint32_t read_timeout_ms;
  uint32_t max_retries;
  char *endpoint_suffix;
  char *default_account;
  char *bearer_token;
  char *sas_token;
};

struct azure_object_ref_data {
  struct azure_settings *settings;
  char *account;
  char *container;
  char *blob;
};

struct azure_readable_data {
  struct azure_object_ref_data *obj;
  int64_t size;
};

struct azure_http_result {
  long http_status;
  uint8_t *body;
  size_t body_len;
  int64_t content_length;
};
#endif

/* Shared constructor (implemented in openslide-storage.c) */

struct _openslide_object_ref *new_object_ref(
    struct _openslide_storage_provider *provider,
    const char *debug_name,
    gpointer provider_data,
    GDestroyNotify provider_data_destroy);

/* Shared cloud utilities (implemented in openslide-storage-cloud.c) */

#if defined(HAVE_S3_PROVIDER) || defined(HAVE_GCS_PROVIDER) || defined(HAVE_AZURE_PROVIDER)

struct cloud_grow_ctx {
  uint8_t *buf;
  size_t len;
};

size_t cloud_grow_write_cb(char *ptr, size_t size, size_t nmemb, void *userdata);
bool cloud_is_retryable_curl(CURLcode code);
bool cloud_is_retryable_http(long status);
void cloud_init_curl_once(void);

#endif

/* Provider URI parsers (implemented in provider .c files) */

#ifdef HAVE_S3_PROVIDER
bool s3_parse_uri(const char *uri,
                  const struct _openslide_open_options *opts,
                  struct _openslide_object_ref **out,
                  GError **err);
#endif

#ifdef HAVE_GCS_PROVIDER
bool gcs_parse_uri(const char *uri,
                   const struct _openslide_open_options *opts,
                   struct _openslide_object_ref **out,
                   GError **err);
#endif

#ifdef HAVE_AZURE_PROVIDER
bool azure_parse_uri(const char *uri,
                     const struct _openslide_open_options *opts,
                     struct _openslide_object_ref **out,
                     GError **err);
#endif
