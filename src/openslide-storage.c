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

#include <string.h>

struct _openslide_object_ref *new_object_ref(struct _openslide_storage_provider *provider,
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
