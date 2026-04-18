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

static void assert_error_contains(const GError *err,
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
  assert_true(_openslide_object_ref_from_local_path(fx->base_file, &ref, NULL),
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
  assert_true(_openslide_object_ref_from_local_path(fx->base_file,
                                                     &base_ref,
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
  assert_true(_openslide_object_ref_from_local_path(fx->dir, &dir_ref, NULL),
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
  assert_true(_openslide_object_ref_from_local_path(fx->dir, &dir_ref, NULL),
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
  assert_true(_openslide_object_ref_from_local_path(fx->base_file, &file_ref, NULL),
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

int main(int argc, char **argv) {
  (void) argc;
  (void) argv;

  struct fixture fx = setup_fixture();
  test_short_read_and_eof(&fx);
  test_resolve_child_semantics(&fx);
  test_list_children_semantics(&fx);
  test_gcs_uri_parsing_and_child_resolution();
  test_azure_uri_parsing_and_child_resolution();
  teardown_fixture(&fx);
  return 0;
}
