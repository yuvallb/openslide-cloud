/*
 *  OpenSlide, a library for reading whole slide image files
 *
 *  Copyright (c) 2007-2015 Carnegie Mellon University
 *  Copyright (c) 2015 Benjamin Gilbert
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

#include <stdio.h>
#include <glib.h>

struct _openslide_file {
  struct _openslide_readable *readable;
  struct _openslide_object_ref *ref;
  int64_t cursor;
  char *path;
};

struct _openslide_dir {
  GPtrArray *entries;
  guint index;
  char *path;
};

struct _openslide_file *_openslide_fopen_ref(const struct _openslide_object_ref *ref,
                                             GError **err) {
  g_autoptr(_openslide_readable) readable = _openslide_readable_open(ref, err);
  if (readable == NULL) {
    return NULL;
  }

  struct _openslide_file *file = g_new0(struct _openslide_file, 1);
  file->readable = g_steal_pointer(&readable);
  file->ref = _openslide_object_ref_ref((struct _openslide_object_ref *) ref);
  file->cursor = 0;
  file->path = g_strdup(_openslide_object_ref_get_debug_name(ref));
  return file;
}

struct _openslide_file *_openslide_fopen(const char *path, GError **err) {
  g_autoptr(_openslide_object_ref) ref = NULL;
  if (!_openslide_object_ref_from_local_path(path, NULL, &ref, err)) {
    g_prefix_error(err, "Couldn't open %s: ", path);
    return NULL;
  }

  return _openslide_fopen_ref(ref, err);
}

// returns 0/NULL on EOF and 0/non-NULL on I/O error
size_t _openslide_fread(struct _openslide_file *file, void *buf, size_t size,
                        GError **err) {
  size_t total = 0;
  if (!_openslide_readable_read_at(file->readable,
                                   file->cursor,
                                   buf,
                                   size,
                                   &total,
                                   err)) {
    if (err && *err == NULL) {
      g_set_error(err, G_FILE_ERROR, G_FILE_ERROR_IO,
                  "I/O error reading file %s", file->path);
    }
    return 0;
  }
  file->cursor += total;
  return total;
}

bool _openslide_fread_exact(struct _openslide_file *file,
                            void *buf, size_t size, GError **err) {
  GError *tmp_err = NULL;
  size_t count = _openslide_fread(file, buf, size, &tmp_err);
  if (tmp_err) {
    g_propagate_error(err, tmp_err);
    return false;
  } else if (count < size) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Short read of file %s: %"PRIu64" < %"PRIu64,
                file->path, (uint64_t) count, (uint64_t) size);
    return false;
  }
  return true;
}

bool _openslide_fseek(struct _openslide_file *file, int64_t offset, int whence,
                      GError **err) {
  int64_t size;
  if (!_openslide_readable_get_size(file->readable, &size, err)) {
    g_prefix_error(err, "Couldn't seek file %s: ", file->path);
    return false;
  }

  int64_t new_offset = _openslide_compute_seek(file->cursor, size, offset, whence);
  if (new_offset < 0) {
    g_set_error(err, G_FILE_ERROR, G_FILE_ERROR_INVAL,
                "Invalid seek in file %s", file->path);
    return false;
  }

  file->cursor = new_offset;
  return true;
}

int64_t _openslide_ftell(struct _openslide_file *file, GError **err) {
  if (err && *err) {
    return -1;
  }
  return file->cursor;
}

int64_t _openslide_fsize(struct _openslide_file *file, GError **err) {
  int64_t ret;
  if (!_openslide_readable_get_size(file->readable, &ret, err)) {
    g_prefix_error(err, "Couldn't get size of %s: ", file->path);
    return -1;
  }
  return ret;
}

void _openslide_fclose(struct _openslide_file *file) {
  _openslide_readable_close(file->readable);
  _openslide_object_ref_unref(file->ref);
  g_free(file->path);
  g_free(file);
}

bool _openslide_fexists(const char *path, GError **err) {
  g_autoptr(_openslide_object_ref) ref = NULL;
  if (!_openslide_object_ref_from_local_path(path, NULL, &ref, err)) {
    return false;
  }
  return _openslide_fexists_ref(ref, err);
}

bool _openslide_fexists_ref(const struct _openslide_object_ref *ref,
                            GError **err) {
  return _openslide_object_ref_exists(ref, err);
}

struct _openslide_dir *_openslide_dir_open(const char *dirname, GError **err) {
  g_autoptr(_openslide_object_ref) ref = NULL;
  if (!_openslide_object_ref_from_local_path(dirname, NULL, &ref, err)) {
    return NULL;
  }

  g_autoptr(GPtrArray) entries = NULL;
  if (!_openslide_object_ref_list_children(ref, &entries, err)) {
    return NULL;
  }

  g_autoptr(_openslide_dir) d = g_new0(struct _openslide_dir, 1);
  d->entries = g_steal_pointer(&entries);
  d->index = 0;
  d->path = g_strdup(dirname);
  return g_steal_pointer(&d);
}

const char *_openslide_dir_next(struct _openslide_dir *d, GError **err G_GNUC_UNUSED) {
  if (d->index >= d->entries->len) {
    return NULL;
  }
  struct _openslide_list_entry *entry = g_ptr_array_index(d->entries, d->index++);
  return entry->name;
}

void _openslide_dir_close(struct _openslide_dir *d) {
  if (d->entries) {
    g_ptr_array_unref(d->entries);
  }
  g_free(d->path);
  g_free(d);
}
