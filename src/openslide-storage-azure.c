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

#ifdef HAVE_AZURE_PROVIDER
static const struct _openslide_storage_provider_ops azure_provider_ops;
static const struct _openslide_readable_ops azure_readable_ops;

static struct _openslide_storage_provider azure_provider = {
  .ops = &azure_provider_ops,
};

static void secure_zero_free(char *secret) {
  if (!secret) {
    return;
  }
  volatile char *p = secret;
  while (*p != '\0') {
    *p++ = '\0';
  }
  g_free(secret);
}

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
  secure_zero_free(settings->bearer_token);
  secure_zero_free(settings->sas_token);
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
    if (!cloud_headers_append(&headers,
                              "x-ms-version: 2023-11-03",
                              "Azure request",
                              err)) {
      curl_easy_cleanup(curl);
      return false;
    }
    if (settings->bearer_token && settings->bearer_token[0]) {
      g_autofree char *auth = g_strdup_printf("Authorization: Bearer %s", settings->bearer_token);
      if (!cloud_headers_append(&headers, auth, "Azure request", err)) {
        curl_easy_cleanup(curl);
        return false;
      }
    }
    if (range_header) {
      if (!cloud_headers_append(&headers, range_header, "Azure request", err)) {
        curl_easy_cleanup(curl);
        return false;
      }
    }

    struct cloud_grow_ctx grow = {0};
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    bool has_auth = (settings->bearer_token && settings->bearer_token[0]) ||
            (settings->sas_token && settings->sas_token[0]);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION,
             cloud_should_follow_redirect(has_auth) ? 1L : 0L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, (long) settings->connection_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long) settings->read_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cloud_grow_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &grow);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "openslide/4.1 azure-provider");
    if (cloud_should_skip_tls_verify()) {
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }

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

bool azure_parse_uri(const char *uri,
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
