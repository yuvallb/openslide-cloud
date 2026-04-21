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

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <glib.h>
#include <glib/gstdio.h>

#include "openslide-private.h"
#include "openslide-storage-internal.h"

struct fixture {
  char *dir;
  char *base_file;
  char *child_a;
  char *child_b;
};

static void assert_true(bool cond, const char *msg) {
  if (!cond) {
    fprintf(stderr, "Assertion failed: %s\n", msg);
    exit(2);
  }
}

static void G_GNUC_UNUSED assert_error_contains(const GError *err,
                                                const char *needle,
                                                const char *msg) {
  assert_true(err != NULL, msg);
  assert_true(strstr(err->message, needle) != NULL, msg);
}

static void write_bytes(const char *path, const void *buf, size_t len) {
  if (!g_file_set_contents(path, buf, len, NULL)) {
    fprintf(stderr, "Couldn't write %s\n", path);
    exit(2);
  }
}

static struct fixture setup_fixture(void) {
  struct fixture fx = {0};

  g_autofree char *tmpl = g_build_filename(g_get_tmp_dir(),
                                           "openslide-storage-test-XXXXXX",
                                           NULL);
  fx.dir = g_mkdtemp(g_steal_pointer(&tmpl));
  if (!fx.dir) {
    fprintf(stderr, "Couldn't create test temp directory\n");
    exit(2);
  }

  fx.base_file = g_build_filename(fx.dir, "base.bin", NULL);
  fx.child_a = g_build_filename(fx.dir, "alpha.dat", NULL);
  fx.child_b = g_build_filename(fx.dir, "beta.dat", NULL);

  const uint8_t base_bytes[] = {1, 2, 3, 4};
  const uint8_t alpha_bytes[] = {10, 11, 12};
  const uint8_t beta_bytes[] = {20, 21, 22, 23, 24};

  write_bytes(fx.base_file, base_bytes, sizeof(base_bytes));
  write_bytes(fx.child_a, alpha_bytes, sizeof(alpha_bytes));
  write_bytes(fx.child_b, beta_bytes, sizeof(beta_bytes));

  return fx;
}

static void teardown_fixture(struct fixture *fx) {
  if (!fx || !fx->dir) {
    return;
  }

  g_remove(fx->base_file);
  g_remove(fx->child_a);
  g_remove(fx->child_b);
  g_rmdir(fx->dir);

  g_free(fx->base_file);
  g_free(fx->child_a);
  g_free(fx->child_b);
  g_free(fx->dir);
}

static void test_short_read_and_eof(const struct fixture *fx) {
  g_autoptr(_openslide_object_ref) ref = NULL;
  assert_true(_openslide_object_ref_from_local_path(fx->base_file, NULL, &ref, NULL),
              "object ref from file path should succeed");

  g_autoptr(_openslide_readable) readable = _openslide_readable_open(ref, NULL);
  assert_true(readable != NULL, "open readable should succeed");

  uint8_t buf[16] = {0};
  size_t count = 0;
  bool ok = false;

  ok = _openslide_readable_read_at(readable, 0, buf, 3, &count, NULL);
  assert_true(ok, "first read_at should succeed");
  assert_true(count == 3, "first read should read 3 bytes");
  assert_true(buf[0] == 1 && buf[1] == 2 && buf[2] == 3,
              "first read contents should match");

  ok = _openslide_readable_read_at(readable, 3, buf + 3, 4, &count, NULL);
  assert_true(ok, "second read_at should succeed");
  assert_true(count == 1, "second read should stop at EOF with 1 byte");
  assert_true(buf[3] == 4, "second read byte should match");

  ok = _openslide_readable_read_at(readable, 4, buf + 4, 2, &count, NULL);
  assert_true(ok, "third read_at should succeed");
  assert_true(count == 0, "third read at EOF should return zero");

  int64_t size = -1;
  ok = _openslide_readable_get_size(readable, &size, NULL);
  assert_true(ok, "get_size should succeed");
  assert_true(size == 4, "get_size should report expected file size");
}

static void test_resolve_child_semantics(const struct fixture *fx) {
  g_autoptr(_openslide_object_ref) base_ref = NULL;
  assert_true(_openslide_object_ref_from_local_path(fx->base_file, NULL, &base_ref,
                                                     NULL),
              "object ref from file path should succeed");

  g_autoptr(_openslide_object_ref) child_from_file = NULL;
  struct _openslide_object_stat stat = {0};

  assert_true(_openslide_object_ref_resolve_child(base_ref,
                                                   "alpha.dat",
                                                   &child_from_file,
                                                   NULL),
              "resolve_child from file base should succeed");
  assert_true(_openslide_object_ref_stat(child_from_file, &stat, NULL),
              "stat on resolved child from file base should succeed");
  assert_true(stat.exists,
              "resolve_child from file base should target sibling");

  g_autoptr(_openslide_object_ref) dir_ref = NULL;
  assert_true(_openslide_object_ref_from_local_path(fx->dir, NULL, &dir_ref, NULL),
              "object ref from directory path should succeed");

  g_autoptr(_openslide_object_ref) child_from_dir = NULL;
  assert_true(_openslide_object_ref_resolve_child(dir_ref,
                                                   "beta.dat",
                                                   &child_from_dir,
                                                   NULL),
              "resolve_child from dir base should succeed");
  assert_true(_openslide_object_ref_stat(child_from_dir, &stat, NULL),
              "stat on resolved child from dir base should succeed");
  assert_true(stat.exists,
              "resolve_child from dir base should target child");
}

static bool nameset_contains(GHashTable *set, const char *name) {
  return g_hash_table_contains(set, name);
}

static void collect_names_into_set(GPtrArray *entries, GHashTable *set) {
  for (guint i = 0; i < entries->len; i++) {
    struct _openslide_list_entry *entry = g_ptr_array_index(entries, i);
    g_hash_table_add(set, g_strdup(entry->name));
  }
}

static void test_list_children_semantics(const struct fixture *fx) {
  g_autoptr(_openslide_object_ref) dir_ref = NULL;
  assert_true(_openslide_object_ref_from_local_path(fx->dir, NULL, &dir_ref, NULL),
              "object ref from directory path should succeed");

  g_autoptr(GPtrArray) dir_entries = NULL;
  assert_true(_openslide_object_ref_list_children(dir_ref, &dir_entries, NULL),
              "list_children on dir should succeed");

  g_autoptr(GHashTable) dir_names = g_hash_table_new_full(g_str_hash,
                                                          g_str_equal,
                                                          g_free,
                                                          NULL);
  collect_names_into_set(dir_entries, dir_names);
  assert_true(nameset_contains(dir_names, "base.bin"),
              "dir listing should include base.bin");
  assert_true(nameset_contains(dir_names, "alpha.dat"),
              "dir listing should include alpha.dat");
  assert_true(nameset_contains(dir_names, "beta.dat"),
              "dir listing should include beta.dat");

  g_autoptr(_openslide_object_ref) file_ref = NULL;
  assert_true(_openslide_object_ref_from_local_path(fx->base_file, NULL, &file_ref, NULL),
              "object ref from file path should succeed");

  g_autoptr(GPtrArray) file_entries = NULL;
  assert_true(_openslide_object_ref_list_children(file_ref, &file_entries, NULL),
              "list_children on file should succeed");

  g_autoptr(GHashTable) file_names = g_hash_table_new_full(g_str_hash,
                                                           g_str_equal,
                                                           g_free,
                                                           NULL);
  collect_names_into_set(file_entries, file_names);
  assert_true(nameset_contains(file_names, "base.bin"),
              "file listing should include file itself as sibling entry");
  assert_true(nameset_contains(file_names, "alpha.dat"),
              "file listing should include sibling alpha.dat");
  assert_true(nameset_contains(file_names, "beta.dat"),
              "file listing should include sibling beta.dat");
}

static void test_gcs_uri_parsing_and_child_resolution(void) {
  const char *uri = "gs://bucket/path/to/slide.czi";
  g_autoptr(GError) err = NULL;
  g_autoptr(_openslide_object_ref) ref = NULL;

  bool ok = _openslide_object_ref_from_uri(uri, NULL, &ref, &err);

#ifdef HAVE_GCS_PROVIDER
  assert_true(ok, "gs:// URI parsing should succeed when GCS provider is enabled");
  assert_true(ref != NULL, "parsed gs:// URI should return object ref");
  assert_true(g_str_equal(_openslide_object_ref_get_debug_name(ref), uri),
              "GCS debug name should preserve URI identity");

  g_autoptr(_openslide_object_ref) child = NULL;
  ok = _openslide_object_ref_resolve_child(ref, "macro.jpg", &child, &err);
  assert_true(ok, "GCS resolve_child should succeed from file-like base");
  assert_true(g_str_equal(_openslide_object_ref_get_debug_name(child),
                          "gs://bucket/path/to/macro.jpg"),
              "GCS resolve_child from file base should use dirname semantics");

  g_autoptr(_openslide_object_ref) prefix_ref = NULL;
  ok = _openslide_object_ref_from_uri("gs://bucket/path/to/", NULL, &prefix_ref, &err);
  assert_true(ok, "gs:// prefix URI parsing should succeed");

  g_autoptr(_openslide_object_ref) prefix_child = NULL;
  ok = _openslide_object_ref_resolve_child(prefix_ref, "macro.jpg", &prefix_child, &err);
  assert_true(ok, "GCS resolve_child should succeed from prefix base");
  assert_true(g_str_equal(_openslide_object_ref_get_debug_name(prefix_child),
                          "gs://bucket/path/to/macro.jpg"),
              "GCS resolve_child from prefix base should append child under prefix");
#else
  assert_true(!ok, "gs:// URI parsing should fail when GCS provider is disabled");
  assert_error_contains(err,
                        "built without GCS provider",
                        "disabled GCS provider should return actionable error");
#endif
}

static void test_azure_uri_parsing_and_child_resolution(void) {
  const char *uri = "az://acct/container/path/to/slide.czi";
  g_autoptr(GError) err = NULL;
  g_autoptr(_openslide_object_ref) ref = NULL;

  bool ok = _openslide_object_ref_from_uri(uri, NULL, &ref, &err);

#ifdef HAVE_AZURE_PROVIDER
  assert_true(ok, "az:// URI parsing should succeed when Azure provider is enabled");
  assert_true(ref != NULL, "parsed az:// URI should return object ref");
  assert_true(g_str_equal(_openslide_object_ref_get_debug_name(ref), uri),
              "Azure debug name should preserve URI identity");

  g_autoptr(_openslide_object_ref) child = NULL;
  ok = _openslide_object_ref_resolve_child(ref, "macro.jpg", &child, &err);
  assert_true(ok, "Azure resolve_child should succeed from file-like base");
  assert_true(g_str_equal(_openslide_object_ref_get_debug_name(child),
                          "az://acct/container/path/to/macro.jpg"),
              "Azure resolve_child from file base should use dirname semantics");

  g_autoptr(_openslide_object_ref) prefix_ref = NULL;
  ok = _openslide_object_ref_from_uri("az://acct/container/path/to/", NULL, &prefix_ref, &err);
  assert_true(ok, "az:// prefix URI parsing should succeed");

  g_autoptr(_openslide_object_ref) prefix_child = NULL;
  ok = _openslide_object_ref_resolve_child(prefix_ref, "macro.jpg", &prefix_child, &err);
  assert_true(ok, "Azure resolve_child should succeed from prefix base");
  assert_true(g_str_equal(_openslide_object_ref_get_debug_name(prefix_child),
                          "az://acct/container/path/to/macro.jpg"),
              "Azure resolve_child from prefix base should append child under prefix");

  assert_true(g_setenv("OPENSLIDE_AZURE_ACCOUNT", "fallbackacct", true),
              "setting OPENSLIDE_AZURE_ACCOUNT should succeed");
  g_autoptr(_openslide_object_ref) fallback_ref = NULL;
  ok = _openslide_object_ref_from_uri("az://_/container/slide.czi", NULL, &fallback_ref, &err);
  assert_true(ok, "Azure account placeholder URI parsing should succeed");
  assert_true(g_str_equal(_openslide_object_ref_get_debug_name(fallback_ref),
                          "az://fallbackacct/container/slide.czi"),
              "Azure placeholder account should resolve from environment");
  g_unsetenv("OPENSLIDE_AZURE_ACCOUNT");
#else
  assert_true(!ok, "az:// URI parsing should fail when Azure provider is disabled");
  assert_error_contains(err,
                        "built without Azure provider",
                        "disabled Azure provider should return actionable error");
#endif
}

static void test_public_source_api(const struct fixture *fx) {
  g_autofree char *file_uri = g_strdup_printf("file://%s", fx->base_file);

  openslide_source_t *source = openslide_source_create_uri(file_uri);
  assert_true(source != NULL, "file:// source creation should succeed");

  openslide_open_options_t *opts = openslide_open_options_create();
  assert_true(opts != NULL, "open options creation should succeed");

  openslide_t *osr = openslide_open_uri(file_uri);
  assert_true(osr == NULL, "open_uri should return NULL for non-slide input");

  osr = openslide_open_with_options(source, opts);
  assert_true(osr == NULL,
              "open_with_options should return NULL for non-slide input");

  openslide_open_options_destroy(opts);
  openslide_source_destroy(source);

  source = openslide_source_create_uri("bogus://example/path");
  assert_true(source == NULL,
              "unsupported URI schemes should be rejected at source creation");
}

#if defined(HAVE_S3_PROVIDER) || defined(HAVE_GCS_PROVIDER) || defined(HAVE_AZURE_PROVIDER)
static void test_cloud_retry_and_redirect_policies(void) {
  assert_true(cloud_is_retryable_http(408),
              "HTTP 408 should be retryable");
  assert_true(cloud_is_retryable_http(429),
              "HTTP 429 should be retryable");
  assert_true(cloud_is_retryable_http(500),
              "HTTP 500 should be retryable");
  assert_true(cloud_is_retryable_http(503),
              "HTTP 503 should be retryable");
  assert_true(!cloud_is_retryable_http(404),
              "HTTP 404 should not be retryable");

  assert_true(cloud_is_retryable_curl(CURLE_OPERATION_TIMEDOUT),
              "timeout should be retryable");
  assert_true(cloud_is_retryable_curl(CURLE_COULDNT_CONNECT),
              "connect failures should be retryable");
  assert_true(cloud_is_retryable_curl(CURLE_RECV_ERROR),
              "recv errors should be retryable");
  assert_true(!cloud_is_retryable_curl(CURLE_OK),
              "successful transfer should not be retryable");
  assert_true(!cloud_is_retryable_curl(CURLE_HTTP_RETURNED_ERROR),
              "generic HTTP returned error should defer to status handling");

  assert_true(cloud_should_follow_redirect(false),
              "requests without auth headers should follow redirects");
  assert_true(!cloud_should_follow_redirect(true),
              "requests with auth headers should not follow redirects");
}
#endif

#ifdef HAVE_S3_PROVIDER
static void test_s3_uri_like_path_applies_options(void) {
  openslide_open_options_t *opts = openslide_open_options_create();
  assert_true(opts != NULL, "open options creation should succeed");
  openslide_open_options_set_connection_timeout(opts, 1111);
  openslide_open_options_set_read_timeout(opts, 2222);
  openslide_open_options_set_max_retries(opts, 7);
  openslide_open_options_set_max_parallel_requests(opts, 3);
  openslide_open_options_set_storage_cache_size(opts, 123456);

  g_autoptr(_openslide_object_ref) ref = NULL;
  assert_true(_openslide_object_ref_from_local_path("s3://bucket/path/to/slide.czi",
                                                    opts,
                                                    &ref,
                                                    NULL),
              "URI-like S3 PATH should resolve via S3 provider");

  const struct s3_object_ref_data *data = ref->provider_data;
  assert_true(data->settings->connection_timeout_ms == 1111,
              "S3 PATH source should preserve connection timeout");
  assert_true(data->settings->read_timeout_ms == 2222,
              "S3 PATH source should preserve read timeout");
  assert_true(data->settings->max_retries == 7,
              "S3 PATH source should preserve retry count");
  assert_true(data->settings->max_parallel_requests == 3,
              "S3 PATH source should preserve max_parallel_requests");
  assert_true(data->settings->storage_cache_bytes == 123456,
              "S3 PATH source should preserve storage cache size");

  openslide_open_options_destroy(opts);
}

static void test_s3_scope_reuses_root_ref_settings(void) {
  openslide_open_options_t *opts = openslide_open_options_create();
  assert_true(opts != NULL, "open options creation should succeed");
  openslide_open_options_set_connection_timeout(opts, 1111);
  openslide_open_options_set_read_timeout(opts, 2222);
  openslide_open_options_set_max_retries(opts, 7);
  openslide_open_options_set_max_parallel_requests(opts, 3);
  openslide_open_options_set_storage_cache_size(opts, 123456);

  g_autoptr(_openslide_object_ref) ref = NULL;
  assert_true(_openslide_object_ref_from_uri("s3://bucket/path/to/slide.czi",
                                             opts,
                                             &ref,
                                             NULL),
              "S3 URI parsing should succeed");

  _openslide_open_scope_enter(ref);

  g_autoptr(_openslide_object_ref) same_ref = NULL;
  assert_true(_openslide_object_ref_from_local_path("s3://bucket/path/to/slide.czi", NULL, &same_ref,
                                                    NULL),
              "path conversion should reuse the in-scope S3 root ref");

  const struct s3_object_ref_data *root_data = ref->provider_data;
  const struct s3_object_ref_data *same_data = same_ref->provider_data;
  assert_true(root_data->settings == same_data->settings,
              "scoped S3 ref reuse should preserve provider settings");
  assert_true(root_data->settings->connection_timeout_ms == 1111,
              "S3 connection timeout should be preserved");
  assert_true(root_data->settings->read_timeout_ms == 2222,
              "S3 read timeout should be preserved");
  assert_true(root_data->settings->max_retries == 7,
              "S3 retry count should be preserved");
  assert_true(root_data->settings->max_parallel_requests == 3,
              "S3 max_parallel_requests should be preserved");
  assert_true(root_data->settings->storage_cache_bytes == 123456,
              "S3 storage cache size should be preserved");

  g_autoptr(_openslide_object_ref) child = NULL;
  assert_true(_openslide_object_ref_resolve_child(same_ref,
                                                  "macro.jpg",
                                                  &child,
                                                  NULL),
              "resolve_child should succeed from reused S3 ref");
  const struct s3_object_ref_data *child_data = child->provider_data;
  assert_true(child_data->settings == root_data->settings,
              "S3 child refs should inherit original provider settings");

  _openslide_open_scope_leave();
  openslide_open_options_destroy(opts);
}

static void test_s3_request_path_generation(void) {
  g_autofree char *host_style =
    _openslide_s3_build_request_path("bucket", "dir/slide one.svs", false);
  assert_true(g_str_equal(host_style, "/dir/slide%20one.svs"),
              "host-style S3 requests should sign only the object path");

  g_autofree char *path_style =
    _openslide_s3_build_request_path("bucket", "dir/slide one.svs", true);
  assert_true(g_str_equal(path_style, "/bucket/dir/slide%20one.svs"),
              "path-style S3 requests should sign the bucket-qualified path");

  g_autofree char *empty_key =
    _openslide_s3_build_request_path("bucket", "", true);
  assert_true(g_str_equal(empty_key, "/bucket/"),
              "path-style S3 requests should preserve the trailing slash for empty keys");
}
#endif

#ifdef HAVE_GCS_PROVIDER
static void test_gcs_uri_like_path_applies_options(void) {
  openslide_open_options_t *opts = openslide_open_options_create();
  assert_true(opts != NULL, "open options creation should succeed");
  openslide_open_options_set_connection_timeout(opts, 3333);
  openslide_open_options_set_read_timeout(opts, 4444);
  openslide_open_options_set_max_retries(opts, 9);

  g_autoptr(_openslide_object_ref) ref = NULL;
  assert_true(_openslide_object_ref_from_local_path("gs://bucket/path/to/slide.czi",
                                                    opts,
                                                    &ref,
                                                    NULL),
              "URI-like GCS PATH should resolve via GCS provider");

  const struct gcs_object_ref_data *data = ref->provider_data;
  assert_true(data->settings->connection_timeout_ms == 3333,
              "GCS PATH source should preserve connection timeout");
  assert_true(data->settings->read_timeout_ms == 4444,
              "GCS PATH source should preserve read timeout");
  assert_true(data->settings->max_retries == 9,
              "GCS PATH source should preserve retry count");

  openslide_open_options_destroy(opts);
}

static void test_gcs_scope_reuses_root_ref_settings(void) {
  openslide_open_options_t *opts = openslide_open_options_create();
  assert_true(opts != NULL, "open options creation should succeed");
  openslide_open_options_set_connection_timeout(opts, 3333);
  openslide_open_options_set_read_timeout(opts, 4444);
  openslide_open_options_set_max_retries(opts, 9);

  g_autoptr(_openslide_object_ref) ref = NULL;
  assert_true(_openslide_object_ref_from_uri("gs://bucket/path/to/slide.czi",
                                             opts,
                                             &ref,
                                             NULL),
              "GCS URI parsing should succeed");

  _openslide_open_scope_enter(ref);

  g_autoptr(_openslide_object_ref) same_ref = NULL;
  assert_true(_openslide_object_ref_from_local_path("gs://bucket/path/to/slide.czi", NULL, &same_ref,
                                                    NULL),
              "path conversion should reuse the in-scope GCS root ref");

  const struct gcs_object_ref_data *root_data = ref->provider_data;
  const struct gcs_object_ref_data *same_data = same_ref->provider_data;
  assert_true(root_data->settings == same_data->settings,
              "scoped GCS ref reuse should preserve provider settings");
  assert_true(root_data->settings->connection_timeout_ms == 3333,
              "GCS connection timeout should be preserved");
  assert_true(root_data->settings->read_timeout_ms == 4444,
              "GCS read timeout should be preserved");
  assert_true(root_data->settings->max_retries == 9,
              "GCS retry count should be preserved");

  g_autoptr(_openslide_object_ref) child = NULL;
  assert_true(_openslide_object_ref_resolve_child(same_ref,
                                                  "macro.jpg",
                                                  &child,
                                                  NULL),
              "resolve_child should succeed from reused GCS ref");
  const struct gcs_object_ref_data *child_data = child->provider_data;
  assert_true(child_data->settings == root_data->settings,
              "GCS child refs should inherit original provider settings");

  _openslide_open_scope_leave();
  openslide_open_options_destroy(opts);
}
#endif

#ifdef HAVE_AZURE_PROVIDER
static void test_azure_uri_like_path_applies_options(void) {
  openslide_open_options_t *opts = openslide_open_options_create();
  assert_true(opts != NULL, "open options creation should succeed");
  openslide_open_options_set_connection_timeout(opts, 5555);
  openslide_open_options_set_read_timeout(opts, 6666);
  openslide_open_options_set_max_retries(opts, 4);

  g_autoptr(_openslide_object_ref) ref = NULL;
  assert_true(_openslide_object_ref_from_local_path("az://acct/container/path/to/slide.czi",
                                                    opts,
                                                    &ref,
                                                    NULL),
              "URI-like Azure PATH should resolve via Azure provider");

  const struct azure_object_ref_data *data = ref->provider_data;
  assert_true(data->settings->connection_timeout_ms == 5555,
              "Azure PATH source should preserve connection timeout");
  assert_true(data->settings->read_timeout_ms == 6666,
              "Azure PATH source should preserve read timeout");
  assert_true(data->settings->max_retries == 4,
              "Azure PATH source should preserve retry count");

  openslide_open_options_destroy(opts);
}

static void test_azure_scope_reuses_root_ref_settings(void) {
  openslide_open_options_t *opts = openslide_open_options_create();
  assert_true(opts != NULL, "open options creation should succeed");
  openslide_open_options_set_connection_timeout(opts, 5555);
  openslide_open_options_set_read_timeout(opts, 6666);
  openslide_open_options_set_max_retries(opts, 4);

  g_autoptr(_openslide_object_ref) ref = NULL;
  assert_true(_openslide_object_ref_from_uri("az://acct/container/path/to/slide.czi",
                                             opts,
                                             &ref,
                                             NULL),
              "Azure URI parsing should succeed");

  _openslide_open_scope_enter(ref);

  g_autoptr(_openslide_object_ref) same_ref = NULL;
  assert_true(_openslide_object_ref_from_local_path("az://acct/container/path/to/slide.czi", NULL, &same_ref,
                                                    NULL),
              "path conversion should reuse the in-scope Azure root ref");

  const struct azure_object_ref_data *root_data = ref->provider_data;
  const struct azure_object_ref_data *same_data = same_ref->provider_data;
  assert_true(root_data->settings == same_data->settings,
              "scoped Azure ref reuse should preserve provider settings");
  assert_true(root_data->settings->connection_timeout_ms == 5555,
              "Azure connection timeout should be preserved");
  assert_true(root_data->settings->read_timeout_ms == 6666,
              "Azure read timeout should be preserved");
  assert_true(root_data->settings->max_retries == 4,
              "Azure retry count should be preserved");

  g_autoptr(_openslide_object_ref) child = NULL;
  assert_true(_openslide_object_ref_resolve_child(same_ref,
                                                  "macro.jpg",
                                                  &child,
                                                  NULL),
              "resolve_child should succeed from reused Azure ref");
  const struct azure_object_ref_data *child_data = child->provider_data;
  assert_true(child_data->settings == root_data->settings,
              "Azure child refs should inherit original provider settings");

  _openslide_open_scope_leave();
  openslide_open_options_destroy(opts);
}
#endif

int main(int argc, char **argv) {
  (void) argc;
  (void) argv;

  struct fixture fx = setup_fixture();
  test_short_read_and_eof(&fx);
  test_resolve_child_semantics(&fx);
  test_list_children_semantics(&fx);
  test_gcs_uri_parsing_and_child_resolution();
  test_azure_uri_parsing_and_child_resolution();
  test_public_source_api(&fx);
#if defined(HAVE_S3_PROVIDER) || defined(HAVE_GCS_PROVIDER) || defined(HAVE_AZURE_PROVIDER)
  test_cloud_retry_and_redirect_policies();
#endif
#ifdef HAVE_S3_PROVIDER
  test_s3_uri_like_path_applies_options();
  test_s3_scope_reuses_root_ref_settings();
  test_s3_request_path_generation();
#endif
#ifdef HAVE_GCS_PROVIDER
  test_gcs_uri_like_path_applies_options();
  test_gcs_scope_reuses_root_ref_settings();
#endif
#ifdef HAVE_AZURE_PROVIDER
  test_azure_uri_like_path_applies_options();
  test_azure_scope_reuses_root_ref_settings();
#endif
  teardown_fixture(&fx);
  return 0;
}
