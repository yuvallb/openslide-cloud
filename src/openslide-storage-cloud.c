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

#if defined(HAVE_S3_PROVIDER) || defined(HAVE_GCS_PROVIDER) || defined(HAVE_AZURE_PROVIDER)

#include "openslide-private.h"
#include "openslide-storage-internal.h"

#include <string.h>
#include <glib.h>
#include <curl/curl.h>

size_t cloud_grow_write_cb(char *ptr, size_t size, size_t nmemb, void *userdata) {
  struct cloud_grow_ctx *ctx = userdata;
  size_t bytes = size * nmemb;
  uint8_t *nbuf = g_realloc(ctx->buf, ctx->len + bytes + 1);
  ctx->buf = nbuf;
  memcpy(ctx->buf + ctx->len, ptr, bytes);
  ctx->len += bytes;
  ctx->buf[ctx->len] = '\0';
  return bytes;
}

bool cloud_is_retryable_curl(CURLcode code) {
  return code == CURLE_OPERATION_TIMEDOUT ||
         code == CURLE_COULDNT_CONNECT ||
         code == CURLE_COULDNT_RESOLVE_HOST ||
         code == CURLE_RECV_ERROR ||
         code == CURLE_SEND_ERROR;
}

bool cloud_is_retryable_http(long status) {
  return status == 408 || status == 429 || (status >= 500 && status <= 599);
}

void cloud_init_curl_once(void) {
  static gsize curl_initialized = 0;
  if (g_once_init_enter(&curl_initialized)) {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    g_once_init_leave(&curl_initialized, 1);
  }
}

#endif
