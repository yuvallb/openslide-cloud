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

#ifdef HAVE_S3_PROVIDER
static const struct _openslide_storage_provider_ops s3_provider_ops;
static const struct _openslide_readable_ops s3_readable_ops;

static struct _openslide_storage_provider s3_provider = {
  .ops = &s3_provider_ops,
};

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
  g_mutex_clear(&settings->request_limit_mutex);
  g_cond_clear(&settings->request_limit_cond);
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
  g_mutex_init(&settings->request_limit_mutex);
  g_cond_init(&settings->request_limit_cond);
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

char *_openslide_s3_build_request_path(const char *bucket,
                                       const char *key,
                                       bool path_style) {
  g_autofree char *escaped_key = s3_uri_encode_path(key);
  if (path_style) {
    return g_strdup_printf("/%s%s", bucket, escaped_key);
  }
  return g_steal_pointer(&escaped_key);
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
    return cloud_headers_append(headers,
                                "x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                                "S3 request",
                                err);
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

  if (!cloud_headers_append(headers, date_header, "S3 request", err) ||
      !cloud_headers_append(headers, payload_header, "S3 request", err) ||
      !cloud_headers_append(headers, authorization, "S3 request", err)) {
    return false;
  }
  if (settings->session_token && settings->session_token[0]) {
    g_autofree char *token_header =
      g_strdup_printf("x-amz-security-token: %s", settings->session_token);
    if (!cloud_headers_append(headers, token_header, "S3 request", err)) {
      return false;
    }
  }
  return true;
}

static void s3_request_slot_acquire(struct s3_settings *settings) {
  if (settings->max_parallel_requests == 0) {
    return;
  }

  g_mutex_lock(&settings->request_limit_mutex);
  while (settings->active_requests >= settings->max_parallel_requests) {
    g_cond_wait(&settings->request_limit_cond, &settings->request_limit_mutex);
  }
  settings->active_requests++;
  g_mutex_unlock(&settings->request_limit_mutex);
}

static void s3_request_slot_release(struct s3_settings *settings) {
  if (settings->max_parallel_requests == 0) {
    return;
  }

  g_mutex_lock(&settings->request_limit_mutex);
  g_assert(settings->active_requests > 0);
  settings->active_requests--;
  g_cond_signal(&settings->request_limit_cond);
  g_mutex_unlock(&settings->request_limit_mutex);
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
  g_autofree char *request_path = NULL;
  if (settings->endpoint && settings->endpoint[0]) {
    host = g_strdup(settings->endpoint);
    request_path = _openslide_s3_build_request_path(bucket, key, true);
    url = g_strdup_printf("https://%s%s%s%s",
                          settings->endpoint,
                          request_path,
                          query ? "?" : "",
                          query ? query : "");
  } else {
    host = g_strdup_printf("%s.s3.amazonaws.com", bucket);
    request_path = _openslide_s3_build_request_path(bucket, key, false);
    url = g_strdup_printf("https://%s%s%s%s",
                          host,
                          request_path,
                          query ? "?" : "",
                          query ? query : "");
  }

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
                         request_path,
                         query,
                         &headers,
                         err)) {
      curl_easy_cleanup(curl);
      return false;
    }
    if (range_header) {
      if (!cloud_headers_append(&headers, range_header, "S3 request", err)) {
        curl_easy_cleanup(curl);
        return false;
      }
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

    s3_request_slot_acquire((struct s3_settings *) settings);
    CURLcode cc = curl_easy_perform(curl);
    long status = 0;
    double content_length = -1;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &content_length);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    s3_request_slot_release((struct s3_settings *) settings);

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

bool s3_parse_uri(const char *uri,
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
