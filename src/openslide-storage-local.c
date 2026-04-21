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

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include <glib.h>
#include <glib/gstdio.h>

#ifndef _WIN32
#include <unistd.h>
#endif

#if !defined(HAVE_FSEEKO) && defined(_WIN32)
#define fseeko _fseeki64
#endif
#if !defined(HAVE_FTELLO) && defined(_WIN32)
#define ftello _ftelli64
#endif

static const struct _openslide_storage_provider_ops local_provider_ops;
static const struct _openslide_readable_ops local_readable_ops;

static struct _openslide_storage_provider local_provider = {
  .ops = &local_provider_ops,
};

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

static bool local_from_local_path(struct _openslide_storage_provider *provider,
                                  const char *path,
                                  struct _openslide_object_ref **out,
                                  GError **err) {
  if (!path) {
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

struct _openslide_storage_provider *_openslide_get_local_provider(void) {
  return &local_provider;
}
