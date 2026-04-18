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

#include <config.h>

#include "openslide-private.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <glib.h>
#include <glib/gstdio.h>

#if defined(HAVE_S3_PROVIDER) || defined(HAVE_GCS_PROVIDER) || defined(HAVE_AZURE_PROVIDER)
#include <curl/curl.h>
#endif

#ifndef _WIN32
#include <unistd.h>
#endif

#if !defined(HAVE_FSEEKO) && defined(_WIN32)
#define fseeko _fseeki64
#endif
#if !defined(HAVE_FTELLO) && defined(_WIN32)
#define ftello _ftelli64
#endif

struct _openslide_storage_provider;

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

struct local_readable_data {
  FILE *fp;
  int64_t size;
};

static const struct _openslide_storage_provider_ops local_provider_ops;
static const struct _openslide_readable_ops local_readable_ops;

#ifdef HAVE_S3_PROVIDER
static const struct _openslide_storage_provider_ops s3_provider_ops;
static const struct _openslide_readable_ops s3_readable_ops;
#endif

#ifdef HAVE_GCS_PROVIDER
static const struct _openslide_storage_provider_ops gcs_provider_ops;
static const struct _openslide_readable_ops gcs_readable_ops;
#endif

#ifdef HAVE_AZURE_PROVIDER
static const struct _openslide_storage_provider_ops azure_provider_ops;
static const struct _openslide_readable_ops azure_readable_ops;
#endif

static struct _openslide_storage_provider local_provider = {
  .ops = &local_provider_ops,
};

#ifdef HAVE_S3_PROVIDER
static struct _openslide_storage_provider s3_provider = {
  .ops = &s3_provider_ops,
};
#endif

#ifdef HAVE_GCS_PROVIDER
static struct _openslide_storage_provider gcs_provider = {
  .ops = &gcs_provider_ops,
};
#endif

#ifdef HAVE_AZURE_PROVIDER
static struct _openslide_storage_provider azure_provider = {
  .ops = &azure_provider_ops,
};
#endif

static void io_error(GError **err, const char *fmt, ...) G_GNUC_PRINTF(2, 3);
static void io_error(GError **err, const char *fmt, ...) {
  int my_errno = errno;
  va_list ap;

  va_start(ap, fmt);
  g_autofree char *msg = g_strdup_vprintf(fmt, ap);
  g_set_error(err, G_FILE_ERROR, g_file_error_from_errno(my_errno),
              "%s: %s", msg, g_strerror(my_errno));
  va_end(ap);
}

static const char *local_path(const struct _openslide_object_ref *ref) {
  const struct local_object_ref_data *data = ref->provider_data;
  return data->path;
}

static void local_object_ref_data_destroy(gpointer arg) {
  struct local_object_ref_data *data = arg;
  g_free(data->path);
  g_free(data);
}

static struct _openslide_object_ref *new_object_ref(struct _openslide_storage_provider *provider,
                                             const char *debug_name,
                                             gpointer provider_data,
                                             GDestroyNotify provider_data_destroy) {
  struct _openslide_object_ref *ref = g_new0(struct _openslide_object_ref, 1);
  ref->refcount = 1;
  ref->provider = provider;
  ref->debug_name = g_strdup(debug_name);
  ref->provider_data = provider_data;
  ref->provider_data_destroy = provider_data_destroy;
  return ref;
}

static bool local_from_local_path(struct _openslide_storage_provider *provider,
                                  const char *path,
                                  struct _openslide_object_ref **out,
                                  GError **err G_GNUC_UNUSED) {
  if (!path || !path[0]) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Empty path");
    return false;
  }

  struct local_object_ref_data *data = g_new0(struct local_object_ref_data, 1);
  data->path = g_strdup(path);

  *out = new_object_ref(provider, path, data, local_object_ref_data_destroy);
  return true;
}

static bool local_exists(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                         const struct _openslide_object_ref *ref,
                         GError **err G_GNUC_UNUSED) {
  return g_file_test(local_path(ref), G_FILE_TEST_EXISTS);  // ci-allow
}

static bool local_stat(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                       const struct _openslide_object_ref *ref,
                       struct _openslide_object_stat *out,
                       GError **err) {
  struct stat sb;
  if (g_stat(local_path(ref), &sb) == -1) {
    if (errno == ENOENT || errno == ENOTDIR) {
      out->size = 0;
      out->exists = false;
      out->is_container = false;
      return true;
    }
    io_error(err, "Couldn't stat %s", local_path(ref));
    return false;
  }

  out->size = sb.st_size;
  out->exists = true;
  out->is_container = S_ISDIR(sb.st_mode);
  return true;
}

static bool open_local_file(const char *path, FILE **out, GError **err) {
#ifdef _WIN32
  g_autofree wchar_t *path16 =
    (wchar_t *) g_utf8_to_utf16(path, -1, NULL, NULL, err);
  if (path16 == NULL) {
    g_prefix_error(err, "Couldn't open %s: ", path);
    return false;
  }
  FILE *f = _wfopen(path16, L"rb" FOPEN_CLOEXEC_FLAG);
  if (f == NULL) {
    io_error(err, "Couldn't open %s", path);
    return false;
  }
#else
  FILE *f = fopen(path, "rb" FOPEN_CLOEXEC_FLAG);  // ci-allow
  if (f == NULL) {
    io_error(err, "Couldn't open %s", path);
    return false;
  }
  // Unnecessary if FOPEN_CLOEXEC_FLAG is non-empty, but compile-checked.
  if (!FOPEN_CLOEXEC_FLAG[0]) {
    int fd = fileno(f);
    if (fd == -1) {
      io_error(err, "Couldn't fileno() %s", path);
      fclose(f);  // ci-allow
      return false;
    }
    long flags = fcntl(fd, F_GETFD);
    if (flags == -1) {
      io_error(err, "Couldn't F_GETFD %s", path);
      fclose(f);  // ci-allow
      return false;
    }
    if (fcntl(fd, F_SETFD, flags | FD_CLOEXEC)) {
      io_error(err, "Couldn't F_SETFD %s", path);
      fclose(f);  // ci-allow
      return false;
    }
  }
#endif

  *out = f;
  return true;
}

static struct _openslide_readable *local_open_readable(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                                                        const struct _openslide_object_ref *ref,
                                                        GError **err) {
  g_autofree char *path = g_strdup(local_path(ref));

  FILE *fp;
  if (!open_local_file(path, &fp, err)) {
    return NULL;
  }

  if (fseeko(fp, 0, SEEK_END)) {  // ci-allow
    io_error(err, "Couldn't seek file %s", path);
    fclose(fp);  // ci-allow
    return NULL;
  }
  int64_t size = ftello(fp);  // ci-allow
  if (size == -1) {
    io_error(err, "Couldn't get size of %s", path);
    fclose(fp);  // ci-allow
    return NULL;
  }
  if (fseeko(fp, 0, SEEK_SET)) {  // ci-allow
    io_error(err, "Couldn't seek file %s", path);
    fclose(fp);  // ci-allow
    return NULL;
  }

  struct local_readable_data *data = g_new0(struct local_readable_data, 1);
  data->fp = fp;
  data->size = size;

  struct _openslide_readable *obj = g_new0(struct _openslide_readable, 1);
  obj->ops = &local_readable_ops;
  obj->ref = _openslide_object_ref_ref((struct _openslide_object_ref *) ref);
  obj->provider_handle_data = data;
  return obj;
}

static bool local_resolve_child(struct _openslide_storage_provider *provider,
                                const struct _openslide_object_ref *base,
                                const char *child_name,
                                struct _openslide_object_ref **out,
                                GError **err) {
  if (!child_name || !child_name[0]) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Empty child name");
    return false;
  }

  const char *base_path = local_path(base);
  g_autofree char *parent = NULL;
  if (g_file_test(base_path, G_FILE_TEST_IS_DIR)) {  // ci-allow
    parent = g_strdup(base_path);
  } else {
    parent = g_path_get_dirname(base_path);
  }

  g_autofree char *child_path = g_build_filename(parent, child_name, NULL);
  return local_from_local_path(provider, child_path, out, err);
}

static bool local_list_children(struct _openslide_storage_provider *provider,
                                const struct _openslide_object_ref *base,
                                GPtrArray **out,
                                GError **err) {
  const char *base_path = local_path(base);

  g_autofree char *dir_path = NULL;
  if (g_file_test(base_path, G_FILE_TEST_IS_DIR)) {  // ci-allow
    dir_path = g_strdup(base_path);
  } else {
    dir_path = g_path_get_dirname(base_path);
  }

  g_autoptr(GDir) dir = g_dir_open(dir_path, 0, err);
  if (!dir) {
    return false;
  }

  g_autoptr(GPtrArray) entries = g_ptr_array_new_with_free_func((GDestroyNotify) _openslide_list_entry_free);
  while (true) {
    errno = 0;
    const char *name = g_dir_read_name(dir);
    if (!name) {
      if (errno) {
        io_error(err, "Reading directory %s", dir_path);
        return false;
      }
      break;
    }

    struct _openslide_list_entry *entry = g_new0(struct _openslide_list_entry, 1);
    entry->name = g_strdup(name);

    g_autofree char *child_path = g_build_filename(dir_path, name, NULL);
    if (!local_from_local_path(provider, child_path, &entry->ref, err)) {
      g_free(entry->name);
      g_free(entry);
      return false;
    }

    g_ptr_array_add(entries, entry);
  }

  *out = g_steal_pointer(&entries);
  return true;
}

static void local_provider_destroy(struct _openslide_storage_provider *provider G_GNUC_UNUSED) {
}

static bool local_readable_get_size(struct _openslide_readable *obj,
                                    int64_t *size_out,
                                    GError **err G_GNUC_UNUSED) {
  struct local_readable_data *data = obj->provider_handle_data;
  *size_out = data->size;
  return true;
}

static bool local_readable_read_at(struct _openslide_readable *obj,
                                   int64_t offset,
                                   void *buf,
                                   size_t len,
                                   size_t *bytes_read,
                                   GError **err) {
  if (offset < 0) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Invalid read offset: %" PRId64, offset);
    return false;
  }

  struct local_readable_data *data = obj->provider_handle_data;
  if (fseeko(data->fp, offset, SEEK_SET)) {  // ci-allow
    io_error(err, "Couldn't seek file %s", _openslide_object_ref_get_debug_name(obj->ref));
    return false;
  }

  size_t total = 0;
  char *bufp = buf;
  while (total < len) {
    size_t count = fread(bufp + total, 1, len - total, data->fp);  // ci-allow
    if (count == 0) {
      break;
    }
    total += count;
  }

  if (total == 0 && ferror(data->fp)) {
    g_set_error(err, G_FILE_ERROR, G_FILE_ERROR_IO,
                "I/O error reading file %s",
                _openslide_object_ref_get_debug_name(obj->ref));
    return false;
  }

  *bytes_read = total;
  return true;
}

static void local_readable_close(struct _openslide_readable *obj) {
  struct local_readable_data *data = obj->provider_handle_data;
  fclose(data->fp);  // ci-allow
  g_free(data);

  _openslide_object_ref_unref(obj->ref);
  g_free(obj);
}

static const struct _openslide_storage_provider_ops local_provider_ops = {
  .from_local_path = local_from_local_path,
  .exists = local_exists,
  .stat = local_stat,
  .open_readable = local_open_readable,
  .resolve_child = local_resolve_child,
  .list_children = local_list_children,
  .destroy = local_provider_destroy,
};

static const struct _openslide_readable_ops local_readable_ops = {
  .get_size = local_readable_get_size,
  .read_at = local_readable_read_at,
  .close = local_readable_close,
};

#if defined(HAVE_S3_PROVIDER) || defined(HAVE_GCS_PROVIDER) || defined(HAVE_AZURE_PROVIDER)
struct cloud_grow_ctx {
  uint8_t *buf;
  size_t len;
};

static size_t cloud_grow_write_cb(char *ptr, size_t size, size_t nmemb, void *userdata) {
  struct cloud_grow_ctx *ctx = userdata;
  size_t bytes = size * nmemb;
  uint8_t *nbuf = g_realloc(ctx->buf, ctx->len + bytes + 1);
  ctx->buf = nbuf;
  memcpy(ctx->buf + ctx->len, ptr, bytes);
  ctx->len += bytes;
  ctx->buf[ctx->len] = '\0';
  return bytes;
}

static bool cloud_is_retryable_curl(CURLcode code) {
  return code == CURLE_OPERATION_TIMEDOUT ||
         code == CURLE_COULDNT_CONNECT ||
         code == CURLE_COULDNT_RESOLVE_HOST ||
         code == CURLE_RECV_ERROR ||
         code == CURLE_SEND_ERROR;
}

static bool cloud_is_retryable_http(long status) {
  return status == 408 || status == 429 || (status >= 500 && status <= 599);
}

static void cloud_init_curl_once(void) {
  static gsize curl_initialized = 0;
  if (g_once_init_enter(&curl_initialized)) {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    g_once_init_leave(&curl_initialized, 1);
  }
}
#endif

#ifdef HAVE_S3_PROVIDER
static char *s3_cache_key(const char *bucket, const char *key) {
  return g_strdup_printf("%s/%s", bucket, key);
}

static void s3_range_cache_entry_free(gpointer arg) {
  struct s3_range_cache_entry *entry = arg;
  if (!entry) {
    return;
  }
  g_free(entry->data);
  g_free(entry);
}

static void s3_inflight_entry_free(gpointer arg) {
  struct s3_inflight_entry *entry = arg;
  if (!entry) {
    return;
  }
  g_cond_clear(&entry->done_cond);
  g_free(entry->data);
  g_free(entry->error_message);
  g_free(entry);
}

static struct s3_settings *s3_settings_ref(struct s3_settings *settings) {
  g_atomic_int_inc(&settings->refcount);
  return settings;
}

static void s3_settings_unref(struct s3_settings *settings) {
  if (!settings) {
    return;
  }
  if (!g_atomic_int_dec_and_test(&settings->refcount)) {
    return;
  }

  g_hash_table_unref(settings->metadata_cache);
  g_hash_table_unref(settings->range_cache);
  g_hash_table_unref(settings->inflight_ranges);
  g_queue_free_full(settings->range_lru, g_free);
  g_mutex_clear(&settings->metadata_cache_mutex);
  g_mutex_clear(&settings->range_cache_mutex);
  g_free(settings->region);
  g_free(settings->endpoint);
  g_free(settings->access_key_id);
  g_free(settings->secret_access_key);
  g_free(settings->session_token);
  g_free(settings);
}

static struct s3_settings *s3_settings_new(const struct _openslide_open_options *opts) {
  struct s3_settings *settings = g_new0(struct s3_settings, 1);
  settings->refcount = 1;
  settings->connection_timeout_ms = opts ? opts->connection_timeout_ms : 10000;
  settings->read_timeout_ms = opts ? opts->read_timeout_ms : 30000;
  settings->max_retries = opts ? opts->max_retries : 3;
  settings->max_parallel_requests = opts ? opts->max_parallel_requests : 4;
  settings->storage_cache_bytes = opts ? opts->storage_cache_bytes : (64 * 1024 * 1024);
  settings->region = g_strdup(g_getenv("OPENSLIDE_S3_REGION"));
  if (!settings->region) {
    settings->region = g_strdup(g_getenv("AWS_REGION"));
  }
  if (!settings->region) {
    settings->region = g_strdup(g_getenv("AWS_DEFAULT_REGION"));
  }
  if (!settings->region) {
    settings->region = g_strdup("us-east-1");
  }
  settings->endpoint = g_strdup(g_getenv("OPENSLIDE_S3_ENDPOINT"));
  settings->access_key_id = g_strdup(g_getenv("OPENSLIDE_S3_ACCESS_KEY_ID"));
  if (!settings->access_key_id) {
    settings->access_key_id = g_strdup(g_getenv("AWS_ACCESS_KEY_ID"));
  }
  settings->secret_access_key = g_strdup(g_getenv("OPENSLIDE_S3_SECRET_ACCESS_KEY"));
  if (!settings->secret_access_key) {
    settings->secret_access_key = g_strdup(g_getenv("AWS_SECRET_ACCESS_KEY"));
  }
  settings->session_token = g_strdup(g_getenv("OPENSLIDE_S3_SESSION_TOKEN"));
  if (!settings->session_token) {
    settings->session_token = g_strdup(g_getenv("AWS_SESSION_TOKEN"));
  }

  g_mutex_init(&settings->metadata_cache_mutex);
  settings->metadata_cache = g_hash_table_new_full(g_str_hash,
                                                   g_str_equal,
                                                   g_free,
                                                   g_free);
  g_mutex_init(&settings->range_cache_mutex);
  settings->range_cache = g_hash_table_new_full(g_str_hash,
                                                g_str_equal,
                                                g_free,
                                                s3_range_cache_entry_free);
  settings->range_lru = g_queue_new();
  settings->inflight_ranges = g_hash_table_new_full(g_str_hash,
                                                    g_str_equal,
                                                    g_free,
                                                    s3_inflight_entry_free);
  return settings;
}

static void s3_object_ref_data_destroy(gpointer arg) {
  struct s3_object_ref_data *data = arg;
  if (!data) {
    return;
  }
  s3_settings_unref(data->settings);
  g_free(data->bucket);
  g_free(data->key);
  g_free(data);
}

static bool s3_new_ref(struct s3_settings *settings,
                       const char *bucket,
                       const char *key,
                       struct _openslide_object_ref **out) {
  g_autofree char *uri = g_strdup_printf("s3://%s/%s", bucket, key);
  struct s3_object_ref_data *data = g_new0(struct s3_object_ref_data, 1);
  data->settings = s3_settings_ref(settings);
  data->bucket = g_strdup(bucket);
  data->key = g_strdup(key);
  *out = new_object_ref(&s3_provider, uri, data, s3_object_ref_data_destroy);
  return true;
}

static char *s3_uri_encode_path(const char *key) {
  if (!key || !key[0]) {
    return g_strdup("/");
  }
  g_autoptr(GString) result = g_string_new("/");
  for (const char *p = key; *p; p++) {
    if (g_ascii_isalnum(*p) || *p == '-' || *p == '_' || *p == '.' || *p == '~' || *p == '/') {
      g_string_append_c(result, *p);
    } else {
      g_string_append_printf(result, "%%%02X", (unsigned char) *p);
    }
  }
  return g_string_free(g_steal_pointer(&result), false);
}

static char *s3_hex(const uint8_t *buf, gsize len) {
  g_autoptr(GString) s = g_string_sized_new(len * 2);
  for (gsize i = 0; i < len; i++) {
    g_string_append_printf(s, "%02x", buf[i]);
  }
  return g_string_free(g_steal_pointer(&s), false);
}

static void s3_hmac_sha256(const uint8_t *key,
                           gsize key_len,
                           const char *msg,
                           uint8_t *out32) {
  g_autoptr(GHmac) hmac = g_hmac_new(G_CHECKSUM_SHA256, key, key_len);
  g_hmac_update(hmac, (const uint8_t *) msg, strlen(msg));
  gsize out_len = 32;
  g_hmac_get_digest(hmac, out32, &out_len);
}

static char *s3_sha256_hex(const char *s) {
  g_autoptr(GChecksum) cks = g_checksum_new(G_CHECKSUM_SHA256);
  g_checksum_update(cks, (const guchar *) s, strlen(s));
  return g_strdup(g_checksum_get_string(cks));
}

static void s3_timestamp(char amz_date[17], char date[9]) {
  GDateTime *now = g_date_time_new_now_utc();
  g_autofree char *full = g_date_time_format(now, "%Y%m%dT%H%M%SZ");
  g_autofree char *shortd = g_date_time_format(now, "%Y%m%d");
  memcpy(amz_date, full, 16);
  amz_date[16] = '\0';
  memcpy(date, shortd, 8);
  date[8] = '\0';
  g_date_time_unref(now);
}

static bool s3_should_sign(const struct s3_settings *settings) {
  return settings->access_key_id && settings->secret_access_key;
}

static bool s3_sign_headers(const struct s3_settings *settings,
                            const char *method,
                            const char *host,
                            const char *canonical_uri,
                            const char *canonical_query,
                            struct curl_slist **headers,
                            GError **err) {
  const char *empty_payload_hash =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  if (!s3_should_sign(settings)) {
    *headers = curl_slist_append(*headers, "x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    return true;
  }

  char amz_date[17];
  char date[9];
  s3_timestamp(amz_date, date);

  g_autofree char *hdr_host = g_strdup_printf("host:%s\n", host);
  g_autofree char *hdr_date = g_strdup_printf("x-amz-date:%s\n", amz_date);
  g_autofree char *hdr_payload =
    g_strdup_printf("x-amz-content-sha256:%s\n", empty_payload_hash);

  g_autoptr(GString) canonical_headers = g_string_new("");
  g_string_append(canonical_headers, hdr_host);
  g_string_append(canonical_headers, hdr_date);
  g_string_append(canonical_headers, hdr_payload);

  const char *signed_headers = "host;x-amz-date;x-amz-content-sha256";
  if (settings->session_token && settings->session_token[0]) {
    g_autofree char *hdr_token =
      g_strdup_printf("x-amz-security-token:%s\n", settings->session_token);
    g_string_append(canonical_headers, hdr_token);
    signed_headers = "host;x-amz-date;x-amz-content-sha256;x-amz-security-token";
  }

  g_autofree char *canonical_request = g_strdup_printf(
    "%s\n%s\n%s\n%s\n%s\n%s",
    method,
    canonical_uri,
    canonical_query ? canonical_query : "",
    canonical_headers->str,
    signed_headers,
    empty_payload_hash);
  g_autofree char *canonical_request_hash = s3_sha256_hex(canonical_request);

  g_autofree char *scope =
    g_strdup_printf("%s/%s/s3/aws4_request", date, settings->region);
  g_autofree char *string_to_sign = g_strdup_printf(
    "AWS4-HMAC-SHA256\n%s\n%s\n%s",
    amz_date,
    scope,
    canonical_request_hash);

  g_autofree char *ksecret_str = g_strdup_printf("AWS4%s", settings->secret_access_key);
  uint8_t kdate[32], kregion[32], kservice[32], ksigning[32], sig[32];
  s3_hmac_sha256((const uint8_t *) ksecret_str, strlen(ksecret_str), date, kdate);
  s3_hmac_sha256(kdate, sizeof(kdate), settings->region, kregion);
  s3_hmac_sha256(kregion, sizeof(kregion), "s3", kservice);
  s3_hmac_sha256(kservice, sizeof(kservice), "aws4_request", ksigning);
  s3_hmac_sha256(ksigning, sizeof(ksigning), string_to_sign, sig);
  g_autofree char *sig_hex = s3_hex(sig, sizeof(sig));

  g_autofree char *authorization = g_strdup_printf(
    "Authorization: AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
    settings->access_key_id,
    scope,
    signed_headers,
    sig_hex);
  g_autofree char *date_header = g_strdup_printf("x-amz-date: %s", amz_date);
  g_autofree char *payload_header = g_strdup_printf("x-amz-content-sha256: %s", empty_payload_hash);

  *headers = curl_slist_append(*headers, date_header);
  *headers = curl_slist_append(*headers, payload_header);
  *headers = curl_slist_append(*headers, authorization);
  if (settings->session_token && settings->session_token[0]) {
    g_autofree char *token_header =
      g_strdup_printf("x-amz-security-token: %s", settings->session_token);
    *headers = curl_slist_append(*headers, token_header);
  }
  if (!*headers) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Unable to allocate curl headers for S3 request");
    return false;
  }
  return true;
}

static char *s3_range_key(const struct s3_object_ref_data *obj,
                          int64_t offset,
                          size_t len) {
  return g_strdup_printf("%s/%s@%" PRId64 ":%zu",
                         obj->bucket,
                         obj->key,
                         offset,
                         len);
}

static uint8_t *s3_dup_bytes(const uint8_t *src, size_t len) {
  uint8_t *dst = g_try_malloc(len);
  if (!dst) {
    return NULL;
  }
  memcpy(dst, src, len);
  return dst;
}

static void s3_lru_drop_key(GQueue *lru, const char *key) {
  for (GList *cur = lru->head; cur; cur = cur->next) {
    if (g_str_equal(cur->data, key)) {
      g_free(cur->data);
      g_queue_delete_link(lru, cur);
      return;
    }
  }
}

static void s3_lru_touch_key(GQueue *lru, const char *key) {
  s3_lru_drop_key(lru, key);
  g_queue_push_tail(lru, g_strdup(key));
}

static void s3_range_cache_evict_until_fit(struct s3_settings *settings,
                                           size_t incoming_len) {
  while (settings->range_cache_current_bytes + incoming_len > settings->storage_cache_bytes) {
    char *oldest = g_queue_pop_head(settings->range_lru);
    if (!oldest) {
      break;
    }
    struct s3_range_cache_entry *entry = g_hash_table_lookup(settings->range_cache, oldest);
    if (entry) {
      settings->range_cache_current_bytes -= entry->len;
      g_hash_table_remove(settings->range_cache, oldest);
    }
    g_free(oldest);
  }
}

static bool s3_range_cache_read(const struct s3_object_ref_data *obj,
                                int64_t offset,
                                size_t len,
                                void *buf,
                                size_t *bytes_read) {
  struct s3_settings *settings = obj->settings;
  if (settings->storage_cache_bytes == 0) {
    return false;
  }

  g_autofree char *key = s3_range_key(obj, offset, len);
  g_mutex_lock(&settings->range_cache_mutex);
  struct s3_range_cache_entry *entry = g_hash_table_lookup(settings->range_cache, key);
  if (entry) {
    memcpy(buf, entry->data, entry->len);
    *bytes_read = entry->len;
    s3_lru_touch_key(settings->range_lru, key);
    g_mutex_unlock(&settings->range_cache_mutex);
    return true;
  }
  g_mutex_unlock(&settings->range_cache_mutex);
  return false;
}

static void s3_range_cache_write(const struct s3_object_ref_data *obj,
                                 int64_t offset,
                                 const uint8_t *data,
                                 size_t len) {
  struct s3_settings *settings = obj->settings;
  if (settings->storage_cache_bytes == 0 || len == 0 || len > settings->storage_cache_bytes) {
    return;
  }

  g_autofree char *key = s3_range_key(obj, offset, len);
  struct s3_range_cache_entry *new_entry = g_new0(struct s3_range_cache_entry, 1);
  new_entry->data = s3_dup_bytes(data, len);
  if (!new_entry->data) {
    g_free(new_entry);
    return;
  }
  new_entry->len = len;

  g_mutex_lock(&settings->range_cache_mutex);
  struct s3_range_cache_entry *existing = g_hash_table_lookup(settings->range_cache, key);
  if (existing) {
    settings->range_cache_current_bytes -= existing->len;
  }

  s3_lru_drop_key(settings->range_lru, key);
  s3_range_cache_evict_until_fit(settings, len);

  g_hash_table_replace(settings->range_cache, g_strdup(key), new_entry);
  s3_lru_touch_key(settings->range_lru, key);
  settings->range_cache_current_bytes += len;
  g_mutex_unlock(&settings->range_cache_mutex);
}

static struct s3_inflight_entry *s3_inflight_entry_new(void) {
  struct s3_inflight_entry *entry = g_new0(struct s3_inflight_entry, 1);
  g_cond_init(&entry->done_cond);
  entry->done = false;
  entry->success = false;
  entry->waiters = 0;
  return entry;
}

static bool s3_do_request(const struct s3_settings *settings,
                          const char *method,
                          const char *bucket,
                          const char *key,
                          const char *query,
                          const char *range_header,
                          struct s3_http_result *result,
                          GError **err) {
  g_autofree char *host = NULL;
  g_autofree char *url = NULL;
  if (settings->endpoint && settings->endpoint[0]) {
    host = g_strdup(settings->endpoint);
    g_autofree char *escaped_key = s3_uri_encode_path(key);
    url = g_strdup_printf("https://%s/%s%s%s%s",
                          settings->endpoint,
                          bucket,
                          escaped_key,
                          query ? "?" : "",
                          query ? query : "");
  } else {
    host = g_strdup_printf("%s.s3.amazonaws.com", bucket);
    g_autofree char *escaped_key = s3_uri_encode_path(key);
    url = g_strdup_printf("https://%s%s%s%s",
                          host,
                          escaped_key,
                          query ? "?" : "",
                          query ? query : "");
  }

  g_autofree char *canonical_uri = s3_uri_encode_path(key);
  uint32_t retries = settings->max_retries;
  for (uint32_t attempt = 0; attempt <= retries; attempt++) {
    CURL *curl = curl_easy_init();
    if (!curl) {
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "Failed to initialize curl for S3 request");
      return false;
    }

    struct curl_slist *headers = NULL;
    if (!s3_sign_headers(settings,
                         method,
                         host,
                         canonical_uri,
                         query,
                         &headers,
                         err)) {
      curl_easy_cleanup(curl);
      return false;
    }
    if (range_header) {
      headers = curl_slist_append(headers, range_header);
    }

    struct cloud_grow_ctx grow = {0};
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, (long) settings->connection_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long) settings->read_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cloud_grow_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &grow);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "openslide/4.1 s3-provider");

    CURLcode cc = curl_easy_perform(curl);
    long status = 0;
    double content_length = -1;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &content_length);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (cc == CURLE_OK && (status >= 200 && status < 300)) {
      result->http_status = status;
      result->body = grow.buf;
      result->body_len = grow.len;
      result->content_length = content_length >= 0 ? (int64_t) content_length : -1;
      return true;
    }

    bool retry = cloud_is_retryable_curl(cc) || cloud_is_retryable_http(status);
    g_free(grow.buf);
    if (!retry || attempt == retries) {
      if (cc != CURLE_OK) {
        g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                    "S3 %s failed for s3://%s/%s: %s",
                    method,
                    bucket,
                    key,
                    curl_easy_strerror(cc));
      } else {
        g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                    "S3 %s failed for s3://%s/%s: HTTP %ld",
                    method,
                    bucket,
                    key,
                    status);
      }
      return false;
    }

    uint64_t backoff_ms = 100ULL << attempt;
    g_usleep(backoff_ms * 1000);
  }

  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "S3 request retry loop terminated unexpectedly");
  return false;
}

static bool s3_stat_cached(const struct s3_object_ref_data *data,
                           struct _openslide_object_stat *out) {
  struct s3_settings *settings = data->settings;
  g_autofree char *key = s3_cache_key(data->bucket, data->key);

  g_mutex_lock(&settings->metadata_cache_mutex);
  struct s3_metadata_cache_entry *entry = g_hash_table_lookup(settings->metadata_cache, key);
  if (entry && entry->valid) {
    *out = entry->stat;
    g_mutex_unlock(&settings->metadata_cache_mutex);
    return true;
  }
  g_mutex_unlock(&settings->metadata_cache_mutex);
  return false;
}

static void s3_stat_store_cache(const struct s3_object_ref_data *data,
                                const struct _openslide_object_stat *stat) {
  struct s3_settings *settings = data->settings;
  g_autofree char *key = s3_cache_key(data->bucket, data->key);
  struct s3_metadata_cache_entry *entry = g_new0(struct s3_metadata_cache_entry, 1);
  entry->valid = true;
  entry->stat = *stat;
  g_mutex_lock(&settings->metadata_cache_mutex);
  g_hash_table_replace(settings->metadata_cache, g_strdup(key), entry);
  g_mutex_unlock(&settings->metadata_cache_mutex);
}

static bool s3_from_local_path(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                               const char *path G_GNUC_UNUSED,
                               struct _openslide_object_ref **out G_GNUC_UNUSED,
                               GError **err) {
  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "S3 provider cannot resolve local paths");
  return false;
}

static bool s3_exists(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                      const struct _openslide_object_ref *ref,
                      GError **err) {
  struct _openslide_object_stat st;
  if (!s3_provider_ops.stat(provider, ref, &st, err)) {
    return false;
  }
  return st.exists;
}

static bool s3_stat(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                    const struct _openslide_object_ref *ref,
                    struct _openslide_object_stat *out,
                    GError **err) {
  const struct s3_object_ref_data *data = ref->provider_data;
  if (s3_stat_cached(data, out)) {
    return true;
  }

  struct s3_http_result result = {0};
  if (!s3_do_request(data->settings,
                     "HEAD",
                     data->bucket,
                     data->key,
                     NULL,
                     NULL,
                     &result,
                     err)) {
    if (err && *err && strstr((*err)->message, "HTTP 404")) {
      g_clear_error(err);
      out->exists = false;
      out->is_container = false;
      out->size = 0;
      s3_stat_store_cache(data, out);
      return true;
    }
    return false;
  }

  out->exists = true;
  out->is_container = false;
  out->size = result.content_length;
  s3_stat_store_cache(data, out);
  g_free(result.body);
  return true;
}

static struct _openslide_readable *s3_open_readable(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                                                     const struct _openslide_object_ref *ref,
                                                     GError **err) {
  struct _openslide_object_stat st = {0};
  if (!s3_stat(provider, ref, &st, err)) {
    return NULL;
  }
  if (!st.exists) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "S3 object not found: %s", _openslide_object_ref_get_debug_name(ref));
    return NULL;
  }

  struct s3_readable_data *rdata = g_new0(struct s3_readable_data, 1);
  rdata->obj = ref->provider_data;
  rdata->size = st.size;
  struct _openslide_readable *obj = g_new0(struct _openslide_readable, 1);
  obj->ops = &s3_readable_ops;
  obj->ref = _openslide_object_ref_ref((struct _openslide_object_ref *) ref);
  obj->provider_handle_data = rdata;
  return obj;
}

static char *s3_dirname_key(const char *key) {
  if (!key || !key[0]) {
    return g_strdup("");
  }
  const char *slash = strrchr(key, '/');
  if (!slash) {
    return g_strdup("");
  }
  return g_strndup(key, slash - key);
}

static bool s3_resolve_child(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                             const struct _openslide_object_ref *base,
                             const char *child_name,
                             struct _openslide_object_ref **out,
                             GError **err) {
  if (!child_name || !child_name[0]) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Empty child name");
    return false;
  }
  const struct s3_object_ref_data *data = base->provider_data;
  g_autofree char *parent = NULL;
  if (data->key[0] == '\0' || data->key[strlen(data->key) - 1] == '/') {
    parent = g_strdup(data->key);
  } else {
    parent = s3_dirname_key(data->key);
  }

  g_autofree char *child_key = NULL;
  if (!parent[0]) {
    child_key = g_strdup(child_name);
  } else if (parent[strlen(parent) - 1] == '/') {
    child_key = g_strdup_printf("%s%s", parent, child_name);
  } else {
    child_key = g_strdup_printf("%s/%s", parent, child_name);
  }
  return s3_new_ref(data->settings, data->bucket, child_key, out);
}

static bool s3_list_children(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                             const struct _openslide_object_ref *base,
                             GPtrArray **out,
                             GError **err) {
  const struct s3_object_ref_data *data = base->provider_data;
  g_autofree char *prefix = NULL;
  if (data->key[0] == '\0' || data->key[strlen(data->key) - 1] == '/') {
    prefix = g_strdup(data->key);
  } else {
    g_autofree char *dir = s3_dirname_key(data->key);
    prefix = dir[0] ? g_strdup_printf("%s/", dir) : g_strdup("");
  }

  g_autofree char *encoded_prefix = g_uri_escape_string(prefix, "/", true);
  g_autofree char *query = g_strdup_printf("list-type=2&delimiter=/&prefix=%s", encoded_prefix);
  struct s3_http_result result = {0};
  if (!s3_do_request(data->settings,
                     "GET",
                     data->bucket,
                     "",
                     query,
                     NULL,
                     &result,
                     err)) {
    return false;
  }

  g_autoptr(GPtrArray) entries = g_ptr_array_new_with_free_func((GDestroyNotify) _openslide_list_entry_free);
  const char *needle = "<Key>";
  const char *end_needle = "</Key>";
  char *cursor = (char *) result.body;
  while (cursor && (cursor = strstr(cursor, needle))) {
    cursor += strlen(needle);
    char *end = strstr(cursor, end_needle);
    if (!end) {
      break;
    }
    g_autofree char *full_key = g_strndup(cursor, end - cursor);
    if (g_str_has_prefix(full_key, prefix)) {
      const char *name = full_key + strlen(prefix);
      if (name[0] && !strchr(name, '/')) {
        struct _openslide_list_entry *entry = g_new0(struct _openslide_list_entry, 1);
        entry->name = g_strdup(name);
        if (!s3_new_ref(data->settings, data->bucket, full_key, &entry->ref)) {
          g_free(entry->name);
          g_free(entry);
          g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                      "Failed creating S3 child reference");
          g_free(result.body);
          return false;
        }
        g_ptr_array_add(entries, entry);
      }
    }
    cursor = end + strlen(end_needle);
  }

  g_free(result.body);
  *out = g_steal_pointer(&entries);
  return true;
}

static void s3_provider_destroy(struct _openslide_storage_provider *provider G_GNUC_UNUSED) {
}

static bool s3_readable_get_size(struct _openslide_readable *obj,
                                 int64_t *size_out,
                                 GError **err) {
  struct s3_readable_data *data = obj->provider_handle_data;
  if (data->size >= 0) {
    *size_out = data->size;
    return true;
  }
  struct _openslide_object_stat st = {0};
  if (!s3_stat(obj->ref->provider, obj->ref, &st, err)) {
    return false;
  }
  data->size = st.size;
  *size_out = data->size;
  return true;
}

static bool s3_readable_read_at(struct _openslide_readable *obj,
                                int64_t offset,
                                void *buf,
                                size_t len,
                                size_t *bytes_read,
                                GError **err) {
  if (offset < 0) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Invalid read offset: %" PRId64, offset);
    return false;
  }
  if (len == 0) {
    *bytes_read = 0;
    return true;
  }

  const struct s3_object_ref_data *data = obj->ref->provider_data;
  if (s3_range_cache_read(data, offset, len, buf, bytes_read)) {
    return true;
  }

  struct s3_settings *settings = data->settings;
  g_autofree char *range_key = s3_range_key(data, offset, len);
  bool leader = false;
  struct s3_inflight_entry *inflight = NULL;

  g_mutex_lock(&settings->range_cache_mutex);
  inflight = g_hash_table_lookup(settings->inflight_ranges, range_key);
  if (!inflight) {
    inflight = s3_inflight_entry_new();
    inflight->waiters = 1;
    g_hash_table_insert(settings->inflight_ranges, g_strdup(range_key), inflight);
    leader = true;
  } else {
    inflight->waiters++;
  }
  g_mutex_unlock(&settings->range_cache_mutex);

  if (leader) {
    int64_t end = offset + (int64_t) len - 1;
    g_autofree char *range =
      g_strdup_printf("Range: bytes=%" PRId64 "-%" PRId64, offset, end);

    struct s3_http_result result = {0};
    bool ok = s3_do_request(data->settings,
                            "GET",
                            data->bucket,
                            data->key,
                            NULL,
                            range,
                            &result,
                            err);

    g_mutex_lock(&settings->range_cache_mutex);
    if (ok) {
      inflight->success = true;
      inflight->data = result.body;
      inflight->len = result.body_len;
    } else {
      inflight->success = false;
      inflight->error_message = g_strdup((err && *err) ? (*err)->message : "S3 read failed");
      if (err && *err) {
        g_clear_error(err);
      }
    }
    inflight->done = true;
    g_cond_broadcast(&inflight->done_cond);
  } else {
    g_mutex_lock(&settings->range_cache_mutex);
    while (!inflight->done) {
      g_cond_wait(&inflight->done_cond, &settings->range_cache_mutex);
    }
  }

  bool success = inflight->success;
  uint8_t *cache_copy = NULL;
  size_t cache_copy_len = 0;
  if (success) {
    size_t copy_len = MIN(len, inflight->len);
    memcpy(buf, inflight->data, copy_len);
    *bytes_read = copy_len;
    if (copy_len > 0) {
      cache_copy = s3_dup_bytes(inflight->data, copy_len);
      cache_copy_len = copy_len;
    }
  } else {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "%s",
                inflight->error_message ? inflight->error_message : "S3 read failed");
  }

  inflight->waiters--;
  if (inflight->waiters == 0) {
    g_hash_table_remove(settings->inflight_ranges, range_key);
  }
  g_mutex_unlock(&settings->range_cache_mutex);

  if (success && cache_copy) {
    s3_range_cache_write(data, offset, cache_copy, cache_copy_len);
    g_free(cache_copy);
  }

  return success;
}

static void s3_readable_close(struct _openslide_readable *obj) {
  g_free(obj->provider_handle_data);
  _openslide_object_ref_unref(obj->ref);
  g_free(obj);
}

static const struct _openslide_storage_provider_ops s3_provider_ops = {
  .from_local_path = s3_from_local_path,
  .exists = s3_exists,
  .stat = s3_stat,
  .open_readable = s3_open_readable,
  .resolve_child = s3_resolve_child,
  .list_children = s3_list_children,
  .destroy = s3_provider_destroy,
};

static const struct _openslide_readable_ops s3_readable_ops = {
  .get_size = s3_readable_get_size,
  .read_at = s3_readable_read_at,
  .close = s3_readable_close,
};

static bool s3_parse_uri(const char *uri,
                         const struct _openslide_open_options *opts,
                         struct _openslide_object_ref **out,
                         GError **err) {
  const char *rest = uri + strlen("s3://");
  const char *slash = strchr(rest, '/');
  if (!slash) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Invalid S3 URI (missing key): %s", uri);
    return false;
  }
  g_autofree char *bucket = g_strndup(rest, slash - rest);
  const char *key = slash + 1;
  if (!bucket[0]) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Invalid S3 URI (missing bucket): %s", uri);
    return false;
  }

  struct s3_settings *settings = s3_settings_new(opts);
  bool ok = s3_new_ref(settings, bucket, key, out);
  s3_settings_unref(settings);
  return ok;
}
#endif

#ifdef HAVE_GCS_PROVIDER
static struct gcs_settings *gcs_settings_ref(struct gcs_settings *settings) {
  g_atomic_int_inc(&settings->refcount);
  return settings;
}

static void gcs_settings_unref(struct gcs_settings *settings) {
  if (!settings) {
    return;
  }
  if (!g_atomic_int_dec_and_test(&settings->refcount)) {
    return;
  }
  g_free(settings->endpoint);
  g_free(settings->bearer_token);
  g_free(settings);
}

static struct gcs_settings *gcs_settings_new(const struct _openslide_open_options *opts) {
  struct gcs_settings *settings = g_new0(struct gcs_settings, 1);
  settings->refcount = 1;
  settings->connection_timeout_ms = opts ? opts->connection_timeout_ms : 10000;
  settings->read_timeout_ms = opts ? opts->read_timeout_ms : 30000;
  settings->max_retries = opts ? opts->max_retries : 3;
  settings->endpoint = g_strdup(g_getenv("OPENSLIDE_GCS_ENDPOINT"));
  if (!settings->endpoint) {
    settings->endpoint = g_strdup("storage.googleapis.com");
  }
  settings->bearer_token = g_strdup(g_getenv("OPENSLIDE_GCS_BEARER_TOKEN"));
  if (!settings->bearer_token) {
    settings->bearer_token = g_strdup(g_getenv("GOOGLE_OAUTH_ACCESS_TOKEN"));
  }
  return settings;
}

static void gcs_object_ref_data_destroy(gpointer arg) {
  struct gcs_object_ref_data *data = arg;
  if (!data) {
    return;
  }
  gcs_settings_unref(data->settings);
  g_free(data->bucket);
  g_free(data->key);
  g_free(data);
}

static bool gcs_new_ref(struct gcs_settings *settings,
                        const char *bucket,
                        const char *key,
                        struct _openslide_object_ref **out) {
  g_autofree char *uri = g_strdup_printf("gs://%s/%s", bucket, key);
  struct gcs_object_ref_data *data = g_new0(struct gcs_object_ref_data, 1);
  data->settings = gcs_settings_ref(settings);
  data->bucket = g_strdup(bucket);
  data->key = g_strdup(key);
  *out = new_object_ref(&gcs_provider, uri, data, gcs_object_ref_data_destroy);
  return true;
}

static char *gcs_uri_encode_path(const char *key) {
  if (!key || !key[0]) {
    return g_strdup("/");
  }
  g_autoptr(GString) result = g_string_new("/");
  for (const char *p = key; *p; p++) {
    if (g_ascii_isalnum(*p) || *p == '-' || *p == '_' || *p == '.' || *p == '~' || *p == '/') {
      g_string_append_c(result, *p);
    } else {
      g_string_append_printf(result, "%%%02X", (unsigned char) *p);
    }
  }
  return g_string_free(g_steal_pointer(&result), false);
}

static char *gcs_dirname_key(const char *key) {
  if (!key || !key[0]) {
    return g_strdup("");
  }
  const char *slash = strrchr(key, '/');
  if (!slash) {
    return g_strdup("");
  }
  return g_strndup(key, slash - key);
}

static bool gcs_do_request(const struct gcs_settings *settings,
                           const char *method,
                           const char *bucket,
                           const char *key,
                           const char *query,
                           const char *range_header,
                           struct gcs_http_result *result,
                           GError **err) {
  g_autofree char *escaped_key = gcs_uri_encode_path(key);
  g_autofree char *url = g_strdup_printf("https://%s/%s%s%s%s",
                                         settings->endpoint,
                                         bucket,
                                         escaped_key,
                                         query ? "?" : "",
                                         query ? query : "");

  uint32_t retries = settings->max_retries;
  for (uint32_t attempt = 0; attempt <= retries; attempt++) {
    CURL *curl = curl_easy_init();
    if (!curl) {
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "Failed to initialize curl for GCS request");
      return false;
    }

    struct curl_slist *headers = NULL;
    if (settings->bearer_token && settings->bearer_token[0]) {
      g_autofree char *auth = g_strdup_printf("Authorization: Bearer %s", settings->bearer_token);
      headers = curl_slist_append(headers, auth);
    }
    if (range_header) {
      headers = curl_slist_append(headers, range_header);
    }

    struct cloud_grow_ctx grow = {0};
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
    if (headers) {
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    }
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, (long) settings->connection_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long) settings->read_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cloud_grow_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &grow);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "openslide/4.1 gcs-provider");

    CURLcode cc = curl_easy_perform(curl);
    long status = 0;
    double content_length = -1;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &content_length);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (cc == CURLE_OK && (status >= 200 && status < 300)) {
      result->http_status = status;
      result->body = grow.buf;
      result->body_len = grow.len;
      result->content_length = content_length >= 0 ? (int64_t) content_length : -1;
      return true;
    }

    bool retry = cloud_is_retryable_curl(cc) || cloud_is_retryable_http(status);
    g_free(grow.buf);
    if (!retry || attempt == retries) {
      if (cc != CURLE_OK) {
        g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                    "GCS %s failed for gs://%s/%s: %s",
                    method,
                    bucket,
                    key,
                    curl_easy_strerror(cc));
      } else {
        g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                    "GCS %s failed for gs://%s/%s: HTTP %ld",
                    method,
                    bucket,
                    key,
                    status);
      }
      return false;
    }

    uint64_t backoff_ms = 100ULL << attempt;
    g_usleep(backoff_ms * 1000);
  }

  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "GCS request retry loop terminated unexpectedly");
  return false;
}

static bool gcs_from_local_path(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                                const char *path G_GNUC_UNUSED,
                                struct _openslide_object_ref **out G_GNUC_UNUSED,
                                GError **err) {
  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "GCS provider cannot resolve local paths");
  return false;
}

static bool gcs_exists(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                       const struct _openslide_object_ref *ref,
                       GError **err) {
  struct _openslide_object_stat st;
  if (!gcs_provider_ops.stat(provider, ref, &st, err)) {
    return false;
  }
  return st.exists;
}

static bool gcs_stat(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                     const struct _openslide_object_ref *ref,
                     struct _openslide_object_stat *out,
                     GError **err) {
  const struct gcs_object_ref_data *data = ref->provider_data;
  struct gcs_http_result result = {0};
  if (!gcs_do_request(data->settings,
                      "HEAD",
                      data->bucket,
                      data->key,
                      NULL,
                      NULL,
                      &result,
                      err)) {
    if (err && *err && strstr((*err)->message, "HTTP 404")) {
      g_clear_error(err);
      out->exists = false;
      out->is_container = false;
      out->size = 0;
      return true;
    }
    return false;
  }

  out->exists = true;
  out->is_container = false;
  out->size = result.content_length;
  g_free(result.body);
  return true;
}

static struct _openslide_readable *gcs_open_readable(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                                                      const struct _openslide_object_ref *ref,
                                                      GError **err) {
  struct _openslide_object_stat st = {0};
  if (!gcs_stat(provider, ref, &st, err)) {
    return NULL;
  }
  if (!st.exists) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "GCS object not found: %s", _openslide_object_ref_get_debug_name(ref));
    return NULL;
  }

  struct gcs_readable_data *rdata = g_new0(struct gcs_readable_data, 1);
  rdata->obj = ref->provider_data;
  rdata->size = st.size;
  struct _openslide_readable *obj = g_new0(struct _openslide_readable, 1);
  obj->ops = &gcs_readable_ops;
  obj->ref = _openslide_object_ref_ref((struct _openslide_object_ref *) ref);
  obj->provider_handle_data = rdata;
  return obj;
}

static bool gcs_resolve_child(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                              const struct _openslide_object_ref *base,
                              const char *child_name,
                              struct _openslide_object_ref **out,
                              GError **err) {
  if (!child_name || !child_name[0]) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Empty child name");
    return false;
  }
  const struct gcs_object_ref_data *data = base->provider_data;
  g_autofree char *parent = NULL;
  if (data->key[0] == '\0' || data->key[strlen(data->key) - 1] == '/') {
    parent = g_strdup(data->key);
  } else {
    parent = gcs_dirname_key(data->key);
  }

  g_autofree char *child_key = NULL;
  if (!parent[0]) {
    child_key = g_strdup(child_name);
  } else if (parent[strlen(parent) - 1] == '/') {
    child_key = g_strdup_printf("%s%s", parent, child_name);
  } else {
    child_key = g_strdup_printf("%s/%s", parent, child_name);
  }
  return gcs_new_ref(data->settings, data->bucket, child_key, out);
}

static bool gcs_list_children(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                              const struct _openslide_object_ref *base,
                              GPtrArray **out,
                              GError **err) {
  const struct gcs_object_ref_data *data = base->provider_data;
  g_autofree char *prefix = NULL;
  if (data->key[0] == '\0' || data->key[strlen(data->key) - 1] == '/') {
    prefix = g_strdup(data->key);
  } else {
    g_autofree char *dir = gcs_dirname_key(data->key);
    prefix = dir[0] ? g_strdup_printf("%s/", dir) : g_strdup("");
  }

  g_autofree char *encoded_prefix = g_uri_escape_string(prefix, "/", true);
  g_autofree char *query = g_strdup_printf("delimiter=/&prefix=%s", encoded_prefix);
  struct gcs_http_result result = {0};
  if (!gcs_do_request(data->settings,
                      "GET",
                      data->bucket,
                      "",
                      query,
                      NULL,
                      &result,
                      err)) {
    return false;
  }

  g_autoptr(GPtrArray) entries = g_ptr_array_new_with_free_func((GDestroyNotify) _openslide_list_entry_free);
  const char *needle = "<Key>";
  const char *end_needle = "</Key>";
  char *cursor = (char *) result.body;
  while (cursor && (cursor = strstr(cursor, needle))) {
    cursor += strlen(needle);
    char *end = strstr(cursor, end_needle);
    if (!end) {
      break;
    }
    g_autofree char *full_key = g_strndup(cursor, end - cursor);
    if (g_str_has_prefix(full_key, prefix)) {
      const char *name = full_key + strlen(prefix);
      if (name[0] && !strchr(name, '/')) {
        struct _openslide_list_entry *entry = g_new0(struct _openslide_list_entry, 1);
        entry->name = g_strdup(name);
        if (!gcs_new_ref(data->settings, data->bucket, full_key, &entry->ref)) {
          g_free(entry->name);
          g_free(entry);
          g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                      "Failed creating GCS child reference");
          g_free(result.body);
          return false;
        }
        g_ptr_array_add(entries, entry);
      }
    }
    cursor = end + strlen(end_needle);
  }

  g_free(result.body);
  *out = g_steal_pointer(&entries);
  return true;
}

static void gcs_provider_destroy(struct _openslide_storage_provider *provider G_GNUC_UNUSED) {
}

static bool gcs_readable_get_size(struct _openslide_readable *obj,
                                  int64_t *size_out,
                                  GError **err) {
  struct gcs_readable_data *data = obj->provider_handle_data;
  if (data->size >= 0) {
    *size_out = data->size;
    return true;
  }
  struct _openslide_object_stat st = {0};
  if (!gcs_stat(obj->ref->provider, obj->ref, &st, err)) {
    return false;
  }
  data->size = st.size;
  *size_out = data->size;
  return true;
}

static bool gcs_readable_read_at(struct _openslide_readable *obj,
                                 int64_t offset,
                                 void *buf,
                                 size_t len,
                                 size_t *bytes_read,
                                 GError **err) {
  if (offset < 0) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Invalid read offset: %" PRId64, offset);
    return false;
  }
  if (len == 0) {
    *bytes_read = 0;
    return true;
  }

  const struct gcs_object_ref_data *data = obj->ref->provider_data;
  int64_t end = offset + (int64_t) len - 1;
  g_autofree char *range = g_strdup_printf("Range: bytes=%" PRId64 "-%" PRId64, offset, end);
  struct gcs_http_result result = {0};
  if (!gcs_do_request(data->settings,
                      "GET",
                      data->bucket,
                      data->key,
                      NULL,
                      range,
                      &result,
                      err)) {
    return false;
  }

  *bytes_read = MIN(len, result.body_len);
  memcpy(buf, result.body, *bytes_read);
  g_free(result.body);
  return true;
}

static void gcs_readable_close(struct _openslide_readable *obj) {
  g_free(obj->provider_handle_data);
  _openslide_object_ref_unref(obj->ref);
  g_free(obj);
}

static const struct _openslide_storage_provider_ops gcs_provider_ops = {
  .from_local_path = gcs_from_local_path,
  .exists = gcs_exists,
  .stat = gcs_stat,
  .open_readable = gcs_open_readable,
  .resolve_child = gcs_resolve_child,
  .list_children = gcs_list_children,
  .destroy = gcs_provider_destroy,
};

static const struct _openslide_readable_ops gcs_readable_ops = {
  .get_size = gcs_readable_get_size,
  .read_at = gcs_readable_read_at,
  .close = gcs_readable_close,
};

static bool gcs_parse_uri(const char *uri,
                          const struct _openslide_open_options *opts,
                          struct _openslide_object_ref **out,
                          GError **err) {
  const char *rest = uri + strlen("gs://");
  const char *slash = strchr(rest, '/');
  if (!slash) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Invalid GCS URI (missing object key): %s", uri);
    return false;
  }
  g_autofree char *bucket = g_strndup(rest, slash - rest);
  const char *key = slash + 1;
  if (!bucket[0]) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Invalid GCS URI (missing bucket): %s", uri);
    return false;
  }

  struct gcs_settings *settings = gcs_settings_new(opts);
  bool ok = gcs_new_ref(settings, bucket, key, out);
  gcs_settings_unref(settings);
  return ok;
}
#endif

#ifdef HAVE_AZURE_PROVIDER
static struct azure_settings *azure_settings_ref(struct azure_settings *settings) {
  g_atomic_int_inc(&settings->refcount);
  return settings;
}

static void azure_settings_unref(struct azure_settings *settings) {
  if (!settings) {
    return;
  }
  if (!g_atomic_int_dec_and_test(&settings->refcount)) {
    return;
  }
  g_free(settings->endpoint_suffix);
  g_free(settings->default_account);
  g_free(settings->bearer_token);
  g_free(settings->sas_token);
  g_free(settings);
}

static struct azure_settings *azure_settings_new(const struct _openslide_open_options *opts) {
  struct azure_settings *settings = g_new0(struct azure_settings, 1);
  settings->refcount = 1;
  settings->connection_timeout_ms = opts ? opts->connection_timeout_ms : 10000;
  settings->read_timeout_ms = opts ? opts->read_timeout_ms : 30000;
  settings->max_retries = opts ? opts->max_retries : 3;
  settings->endpoint_suffix = g_strdup(g_getenv("OPENSLIDE_AZURE_ENDPOINT_SUFFIX"));
  if (!settings->endpoint_suffix) {
    settings->endpoint_suffix = g_strdup("blob.core.windows.net");
  }
  settings->default_account = g_strdup(g_getenv("OPENSLIDE_AZURE_ACCOUNT"));
  if (!settings->default_account) {
    settings->default_account = g_strdup(g_getenv("AZURE_STORAGE_ACCOUNT"));
  }
  settings->bearer_token = g_strdup(g_getenv("OPENSLIDE_AZURE_BEARER_TOKEN"));
  settings->sas_token = g_strdup(g_getenv("OPENSLIDE_AZURE_SAS_TOKEN"));
  if (!settings->sas_token) {
    settings->sas_token = g_strdup(g_getenv("AZURE_STORAGE_SAS_TOKEN"));
  }
  return settings;
}

static void azure_object_ref_data_destroy(gpointer arg) {
  struct azure_object_ref_data *data = arg;
  if (!data) {
    return;
  }
  azure_settings_unref(data->settings);
  g_free(data->account);
  g_free(data->container);
  g_free(data->blob);
  g_free(data);
}

static bool azure_new_ref(struct azure_settings *settings,
                          const char *account,
                          const char *container,
                          const char *blob,
                          struct _openslide_object_ref **out) {
  g_autofree char *uri = g_strdup_printf("az://%s/%s/%s", account, container, blob);
  struct azure_object_ref_data *data = g_new0(struct azure_object_ref_data, 1);
  data->settings = azure_settings_ref(settings);
  data->account = g_strdup(account);
  data->container = g_strdup(container);
  data->blob = g_strdup(blob);
  *out = new_object_ref(&azure_provider, uri, data, azure_object_ref_data_destroy);
  return true;
}

static char *azure_uri_encode_path(const char *blob) {
  if (!blob || !blob[0]) {
    return g_strdup("");
  }
  g_autoptr(GString) result = g_string_new("");
  for (const char *p = blob; *p; p++) {
    if (g_ascii_isalnum(*p) || *p == '-' || *p == '_' || *p == '.' || *p == '~' || *p == '/') {
      g_string_append_c(result, *p);
    } else {
      g_string_append_printf(result, "%%%02X", (unsigned char) *p);
    }
  }
  return g_string_free(g_steal_pointer(&result), false);
}

static char *azure_dirname_blob(const char *blob) {
  if (!blob || !blob[0]) {
    return g_strdup("");
  }
  const char *slash = strrchr(blob, '/');
  if (!slash) {
    return g_strdup("");
  }
  return g_strndup(blob, slash - blob);
}

static char *azure_join_query(const char *q1, const char *q2) {
  if (q1 && q1[0] && q2 && q2[0]) {
    return g_strdup_printf("%s&%s", q1, q2);
  }
  if (q1 && q1[0]) {
    return g_strdup(q1);
  }
  if (q2 && q2[0]) {
    return g_strdup(q2);
  }
  return g_strdup("");
}

static bool azure_do_request(const struct azure_settings *settings,
                             const char *method,
                             const char *account,
                             const char *container,
                             const char *blob,
                             const char *query,
                             const char *range_header,
                             struct azure_http_result *result,
                             GError **err) {
  g_autofree char *host = g_strdup_printf("%s.%s", account, settings->endpoint_suffix);
  g_autofree char *escaped_blob = azure_uri_encode_path(blob);
  const char *sas = settings->sas_token ? settings->sas_token : "";
  const char *sas_trimmed = sas[0] == '?' ? sas + 1 : sas;
  g_autofree char *merged_query = azure_join_query(query, sas_trimmed);
  g_autofree char *url = g_strdup_printf("https://%s/%s%s%s%s",
                                         host,
                                         container,
                                         escaped_blob[0] ? "/" : "",
                                         escaped_blob,
                                         merged_query[0] ? "" : "");
  if (merged_query[0]) {
    g_autofree char *tmp = g_strdup_printf("%s?%s", url, merged_query);
    g_free(url);
    url = g_steal_pointer(&tmp);
  }

  uint32_t retries = settings->max_retries;
  for (uint32_t attempt = 0; attempt <= retries; attempt++) {
    CURL *curl = curl_easy_init();
    if (!curl) {
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "Failed to initialize curl for Azure request");
      return false;
    }

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "x-ms-version: 2023-11-03");
    if (settings->bearer_token && settings->bearer_token[0]) {
      g_autofree char *auth = g_strdup_printf("Authorization: Bearer %s", settings->bearer_token);
      headers = curl_slist_append(headers, auth);
    }
    if (range_header) {
      headers = curl_slist_append(headers, range_header);
    }

    struct cloud_grow_ctx grow = {0};
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, (long) settings->connection_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long) settings->read_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cloud_grow_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &grow);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "openslide/4.1 azure-provider");

    CURLcode cc = curl_easy_perform(curl);
    long status = 0;
    double content_length = -1;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &content_length);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (cc == CURLE_OK && (status >= 200 && status < 300)) {
      result->http_status = status;
      result->body = grow.buf;
      result->body_len = grow.len;
      result->content_length = content_length >= 0 ? (int64_t) content_length : -1;
      return true;
    }

    bool retry = cloud_is_retryable_curl(cc) || cloud_is_retryable_http(status);
    g_free(grow.buf);
    if (!retry || attempt == retries) {
      if (cc != CURLE_OK) {
        g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                    "Azure %s failed for az://%s/%s/%s: %s",
                    method,
                    account,
                    container,
                    blob,
                    curl_easy_strerror(cc));
      } else {
        g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                    "Azure %s failed for az://%s/%s/%s: HTTP %ld",
                    method,
                    account,
                    container,
                    blob,
                    status);
      }
      return false;
    }

    uint64_t backoff_ms = 100ULL << attempt;
    g_usleep(backoff_ms * 1000);
  }

  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "Azure request retry loop terminated unexpectedly");
  return false;
}

static bool azure_from_local_path(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                                  const char *path G_GNUC_UNUSED,
                                  struct _openslide_object_ref **out G_GNUC_UNUSED,
                                  GError **err) {
  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "Azure provider cannot resolve local paths");
  return false;
}

static bool azure_exists(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                         const struct _openslide_object_ref *ref,
                         GError **err) {
  struct _openslide_object_stat st;
  if (!azure_provider_ops.stat(provider, ref, &st, err)) {
    return false;
  }
  return st.exists;
}

static bool azure_stat(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                       const struct _openslide_object_ref *ref,
                       struct _openslide_object_stat *out,
                       GError **err) {
  const struct azure_object_ref_data *data = ref->provider_data;
  struct azure_http_result result = {0};
  if (!azure_do_request(data->settings,
                        "HEAD",
                        data->account,
                        data->container,
                        data->blob,
                        NULL,
                        NULL,
                        &result,
                        err)) {
    if (err && *err && strstr((*err)->message, "HTTP 404")) {
      g_clear_error(err);
      out->exists = false;
      out->is_container = false;
      out->size = 0;
      return true;
    }
    return false;
  }

  out->exists = true;
  out->is_container = false;
  out->size = result.content_length;
  g_free(result.body);
  return true;
}

static struct _openslide_readable *azure_open_readable(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                                                        const struct _openslide_object_ref *ref,
                                                        GError **err) {
  struct _openslide_object_stat st = {0};
  if (!azure_stat(provider, ref, &st, err)) {
    return NULL;
  }
  if (!st.exists) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Azure blob not found: %s", _openslide_object_ref_get_debug_name(ref));
    return NULL;
  }

  struct azure_readable_data *rdata = g_new0(struct azure_readable_data, 1);
  rdata->obj = ref->provider_data;
  rdata->size = st.size;
  struct _openslide_readable *obj = g_new0(struct _openslide_readable, 1);
  obj->ops = &azure_readable_ops;
  obj->ref = _openslide_object_ref_ref((struct _openslide_object_ref *) ref);
  obj->provider_handle_data = rdata;
  return obj;
}

static bool azure_resolve_child(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                                const struct _openslide_object_ref *base,
                                const char *child_name,
                                struct _openslide_object_ref **out,
                                GError **err) {
  if (!child_name || !child_name[0]) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Empty child name");
    return false;
  }
  const struct azure_object_ref_data *data = base->provider_data;
  g_autofree char *parent = NULL;
  if (data->blob[0] == '\0' || data->blob[strlen(data->blob) - 1] == '/') {
    parent = g_strdup(data->blob);
  } else {
    parent = azure_dirname_blob(data->blob);
  }

  g_autofree char *child_blob = NULL;
  if (!parent[0]) {
    child_blob = g_strdup(child_name);
  } else if (parent[strlen(parent) - 1] == '/') {
    child_blob = g_strdup_printf("%s%s", parent, child_name);
  } else {
    child_blob = g_strdup_printf("%s/%s", parent, child_name);
  }
  return azure_new_ref(data->settings, data->account, data->container, child_blob, out);
}

static bool azure_list_children(struct _openslide_storage_provider *provider G_GNUC_UNUSED,
                                const struct _openslide_object_ref *base,
                                GPtrArray **out,
                                GError **err) {
  const struct azure_object_ref_data *data = base->provider_data;
  g_autofree char *prefix = NULL;
  if (data->blob[0] == '\0' || data->blob[strlen(data->blob) - 1] == '/') {
    prefix = g_strdup(data->blob);
  } else {
    g_autofree char *dir = azure_dirname_blob(data->blob);
    prefix = dir[0] ? g_strdup_printf("%s/", dir) : g_strdup("");
  }

  g_autofree char *encoded_prefix = g_uri_escape_string(prefix, "/", true);
  g_autofree char *query = g_strdup_printf("restype=container&comp=list&delimiter=/&prefix=%s", encoded_prefix);
  struct azure_http_result result = {0};
  if (!azure_do_request(data->settings,
                        "GET",
                        data->account,
                        data->container,
                        "",
                        query,
                        NULL,
                        &result,
                        err)) {
    return false;
  }

  g_autoptr(GPtrArray) entries = g_ptr_array_new_with_free_func((GDestroyNotify) _openslide_list_entry_free);
  const char *needle = "<Name>";
  const char *end_needle = "</Name>";
  char *cursor = (char *) result.body;
  while (cursor && (cursor = strstr(cursor, needle))) {
    cursor += strlen(needle);
    char *end = strstr(cursor, end_needle);
    if (!end) {
      break;
    }
    g_autofree char *full_blob = g_strndup(cursor, end - cursor);
    if (g_str_has_prefix(full_blob, prefix)) {
      const char *name = full_blob + strlen(prefix);
      if (name[0] && !strchr(name, '/')) {
        struct _openslide_list_entry *entry = g_new0(struct _openslide_list_entry, 1);
        entry->name = g_strdup(name);
        if (!azure_new_ref(data->settings, data->account, data->container, full_blob, &entry->ref)) {
          g_free(entry->name);
          g_free(entry);
          g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                      "Failed creating Azure child reference");
          g_free(result.body);
          return false;
        }
        g_ptr_array_add(entries, entry);
      }
    }
    cursor = end + strlen(end_needle);
  }

  g_free(result.body);
  *out = g_steal_pointer(&entries);
  return true;
}

static void azure_provider_destroy(struct _openslide_storage_provider *provider G_GNUC_UNUSED) {
}

static bool azure_readable_get_size(struct _openslide_readable *obj,
                                    int64_t *size_out,
                                    GError **err) {
  struct azure_readable_data *data = obj->provider_handle_data;
  if (data->size >= 0) {
    *size_out = data->size;
    return true;
  }
  struct _openslide_object_stat st = {0};
  if (!azure_stat(obj->ref->provider, obj->ref, &st, err)) {
    return false;
  }
  data->size = st.size;
  *size_out = data->size;
  return true;
}

static bool azure_readable_read_at(struct _openslide_readable *obj,
                                   int64_t offset,
                                   void *buf,
                                   size_t len,
                                   size_t *bytes_read,
                                   GError **err) {
  if (offset < 0) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Invalid read offset: %" PRId64, offset);
    return false;
  }
  if (len == 0) {
    *bytes_read = 0;
    return true;
  }

  const struct azure_object_ref_data *data = obj->ref->provider_data;
  int64_t end = offset + (int64_t) len - 1;
  g_autofree char *range = g_strdup_printf("x-ms-range: bytes=%" PRId64 "-%" PRId64, offset, end);
  struct azure_http_result result = {0};
  if (!azure_do_request(data->settings,
                        "GET",
                        data->account,
                        data->container,
                        data->blob,
                        NULL,
                        range,
                        &result,
                        err)) {
    return false;
  }

  *bytes_read = MIN(len, result.body_len);
  memcpy(buf, result.body, *bytes_read);
  g_free(result.body);
  return true;
}

static void azure_readable_close(struct _openslide_readable *obj) {
  g_free(obj->provider_handle_data);
  _openslide_object_ref_unref(obj->ref);
  g_free(obj);
}

static const struct _openslide_storage_provider_ops azure_provider_ops = {
  .from_local_path = azure_from_local_path,
  .exists = azure_exists,
  .stat = azure_stat,
  .open_readable = azure_open_readable,
  .resolve_child = azure_resolve_child,
  .list_children = azure_list_children,
  .destroy = azure_provider_destroy,
};

static const struct _openslide_readable_ops azure_readable_ops = {
  .get_size = azure_readable_get_size,
  .read_at = azure_readable_read_at,
  .close = azure_readable_close,
};

static bool azure_parse_uri(const char *uri,
                            const struct _openslide_open_options *opts,
                            struct _openslide_object_ref **out,
                            GError **err) {
  const char *rest = uri + strlen("az://");
  g_auto(GStrv) parts = g_strsplit(rest, "/", 0);
  if (!parts || !parts[0] || !parts[1]) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Invalid Azure URI (expected az://account/container/blob): %s", uri);
    return false;
  }

  const char *account = parts[0];
  const char *container = parts[1];

  g_autofree char *blob = NULL;
  if (parts[2]) {
    blob = g_strdup(parts[2]);
    for (int i = 3; parts[i]; i++) {
      g_autofree char *tmp = g_strdup_printf("%s/%s", blob, parts[i]);
      g_free(blob);
      blob = g_steal_pointer(&tmp);
    }
  } else {
    blob = g_strdup("");
  }

  if (!account[0] || !container[0]) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Invalid Azure URI (missing account or container): %s", uri);
    return false;
  }

  struct azure_settings *settings = azure_settings_new(opts);
  const char *effective_account = account;
  if (g_str_equal(account, "_") && settings->default_account && settings->default_account[0]) {
    effective_account = settings->default_account;
  }
  bool ok = azure_new_ref(settings, effective_account, container, blob, out);
  azure_settings_unref(settings);
  return ok;
}
#endif

struct _openslide_storage_provider *_openslide_get_local_provider(void) {
  return &local_provider;
}

bool _openslide_object_ref_from_uri(const char *uri,
                                    const struct _openslide_open_options *opts,
                                    struct _openslide_object_ref **out,
                                    GError **err) {
  if (!uri || !uri[0]) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Empty URI");
    return false;
  }

  if (g_str_has_prefix(uri, "file://")) {
    const char *path = uri + 7;
    if (path[0] == '/' && path[1] == '/') {
      path++;
    }
    return _openslide_object_ref_from_local_path(path, out, err);
  }

#ifdef HAVE_S3_PROVIDER
  if (g_str_has_prefix(uri, "s3://")) {
    cloud_init_curl_once();
    return s3_parse_uri(uri, opts, out, err);
  }
#else
  (void) opts;
  if (g_str_has_prefix(uri, "s3://")) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "S3 URI support unavailable: OpenSlide built without S3 provider");
    return false;
  }
#endif

#ifdef HAVE_GCS_PROVIDER
  if (g_str_has_prefix(uri, "gs://")) {
    cloud_init_curl_once();
    return gcs_parse_uri(uri, opts, out, err);
  }
#else
  if (g_str_has_prefix(uri, "gs://")) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "GCS URI support unavailable: OpenSlide built without GCS provider");
    return false;
  }
#endif

#ifdef HAVE_AZURE_PROVIDER
  if (g_str_has_prefix(uri, "az://")) {
    cloud_init_curl_once();
    return azure_parse_uri(uri, opts, out, err);
  }
#else
  if (g_str_has_prefix(uri, "az://")) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Azure URI support unavailable: OpenSlide built without Azure provider");
    return false;
  }
#endif

  g_autofree char *scheme = g_strdup(uri);
  char *colon = strchr(scheme, ':');
  if (colon) {
    *colon = '\0';
  }
  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "Unsupported URI scheme: %s", scheme);
  return false;
}

bool _openslide_object_ref_from_local_path(const char *path,
                                           struct _openslide_object_ref **out,
                                           GError **err) {
  if (g_str_has_prefix(path, "file://") ||
      g_str_has_prefix(path, "s3://") ||
      g_str_has_prefix(path, "gs://") ||
      g_str_has_prefix(path, "az://")) {
    return _openslide_object_ref_from_uri(path, NULL, out, err);
  }

  return _openslide_get_local_provider()->ops->from_local_path(_openslide_get_local_provider(),
                                                                path,
                                                                out,
                                                                err);
}

struct _openslide_object_ref *_openslide_object_ref_ref(struct _openslide_object_ref *ref) {
  g_atomic_int_inc(&ref->refcount);
  return ref;
}

void _openslide_object_ref_unref(struct _openslide_object_ref *ref) {
  if (!ref) {
    return;
  }

  if (!g_atomic_int_dec_and_test(&ref->refcount)) {
    return;
  }

  if (ref->provider_data_destroy) {
    ref->provider_data_destroy(ref->provider_data);
  }
  g_free(ref->debug_name);
  g_free(ref);
}

const char *_openslide_object_ref_get_debug_name(const struct _openslide_object_ref *ref) {
  return ref->debug_name;
}

bool _openslide_object_ref_exists(const struct _openslide_object_ref *ref,
                                  GError **err) {
  return ref->provider->ops->exists(ref->provider, ref, err);
}

bool _openslide_object_ref_stat(const struct _openslide_object_ref *ref,
                                struct _openslide_object_stat *out,
                                GError **err) {
  return ref->provider->ops->stat(ref->provider, ref, out, err);
}

bool _openslide_object_ref_resolve_child(const struct _openslide_object_ref *base,
                                         const char *child_name,
                                         struct _openslide_object_ref **out,
                                         GError **err) {
  return base->provider->ops->resolve_child(base->provider, base, child_name, out, err);
}

bool _openslide_object_ref_list_children(const struct _openslide_object_ref *base,
                                         GPtrArray **out,
                                         GError **err) {
  return base->provider->ops->list_children(base->provider, base, out, err);
}

void _openslide_list_entry_free(struct _openslide_list_entry *entry) {
  if (!entry) {
    return;
  }
  _openslide_object_ref_unref(entry->ref);
  g_free(entry->name);
  g_free(entry);
}

struct _openslide_readable *_openslide_readable_open(const struct _openslide_object_ref *ref,
                                                      GError **err) {
  return ref->provider->ops->open_readable(ref->provider, ref, err);
}

bool _openslide_readable_get_size(struct _openslide_readable *obj,
                                  int64_t *size_out,
                                  GError **err) {
  return obj->ops->get_size(obj, size_out, err);
}

bool _openslide_readable_read_at(struct _openslide_readable *obj,
                                 int64_t offset,
                                 void *buf,
                                 size_t len,
                                 size_t *bytes_read,
                                 GError **err) {
  return obj->ops->read_at(obj, offset, buf, len, bytes_read, err);
}

void _openslide_readable_close(struct _openslide_readable *obj) {
  if (obj) {
    obj->ops->close(obj);
  }
}

// Helper: read exactly len bytes from offset, or fail
bool _openslide_readable_read_exact(struct _openslide_readable *obj,
                                    int64_t offset,
                                    void *buf,
                                    size_t len,
                                    GError **err) {
  size_t bytes_read = 0;
  if (!_openslide_readable_read_at(obj, offset, buf, len, &bytes_read, err)) {
    return false;
  }

  if (bytes_read != len) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Short read from %s at offset %" PRId64 ": got %zu, wanted %zu",
                _openslide_object_ref_get_debug_name(obj->ref),
                offset, bytes_read, len);
    return false;
    }

  return true;
}

// Helper for reading and buffering data from readable objects.
// Useful for image decoders that expect contiguous buffers.
uint8_t *_openslide_readable_read_into_buffer(struct _openslide_readable *obj,
                                              int64_t offset,
                                              size_t len,
                                              GError **err) {
  uint8_t *buf = g_try_malloc(len);
  if (!buf) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Couldn't allocate %zu bytes for reading", len);
    return NULL;
  }

  if (!_openslide_readable_read_exact(obj, offset, buf, len, err)) {
    g_free(buf);
    return NULL;
  }

  return buf;
}
