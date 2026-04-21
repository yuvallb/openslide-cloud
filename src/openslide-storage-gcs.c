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
#include "openslide-storage-internal.h"

#ifdef HAVE_GCS_PROVIDER
static const struct _openslide_storage_provider_ops gcs_provider_ops;
static const struct _openslide_readable_ops gcs_readable_ops;

static struct _openslide_storage_provider gcs_provider = {
  .ops = &gcs_provider_ops,
};

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
      if (!cloud_headers_append(&headers, auth, "GCS request", err)) {
        curl_easy_cleanup(curl);
        return false;
      }
    }
    if (range_header) {
      if (!cloud_headers_append(&headers, range_header, "GCS request", err)) {
        curl_easy_cleanup(curl);
        return false;
      }
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

bool gcs_parse_uri(const char *uri,
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
