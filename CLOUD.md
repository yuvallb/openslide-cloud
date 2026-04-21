# Cloud Read Feature

This document explains how to read whole-slide images from cloud object storage using OpenSlide 4.1+.

## 1. Build with cloud providers enabled

Cloud URI support is compiled in per provider.

Meson options:

- `-Ds3=enabled` for Amazon S3
- `-Dgcs=enabled` for Google Cloud Storage
- `-Dazure=enabled` for Azure Blob Storage

Example build:

```bash
meson setup builddir -Ds3=enabled -Dgcs=enabled -Dazure=enabled
meson compile -C builddir
```

If a provider is not built, opening its URI fails with an error like:

- `S3 URI support unavailable: OpenSlide built without S3 provider`

## 2. Supported URI schemes

OpenSlide supports these schemes:

- `file://` local files
- `s3://bucket/key`
- `gs://bucket/key`
- `az://account/container/blob`

Examples:

- `s3://my-bucket/slides/case-001.svs`
- `gs://my-bucket/slides/case-001.svs`
- `az://myaccount/slides/case-001.svs`

Azure shortcut:

- `az://_/container/blob` uses the account from `OPENSLIDE_AZURE_ACCOUNT` (or `AZURE_STORAGE_ACCOUNT`).

## 3. Authentication and endpoint environment variables

Set provider credentials in environment variables before opening a URI.

### S3

- `OPENSLIDE_S3_REGION` (fallbacks: `AWS_REGION`, `AWS_DEFAULT_REGION`; default `us-east-1`)
- `OPENSLIDE_S3_ENDPOINT` (optional custom endpoint)
- `OPENSLIDE_S3_ACCESS_KEY_ID` (fallback: `AWS_ACCESS_KEY_ID`)
- `OPENSLIDE_S3_SECRET_ACCESS_KEY` (fallback: `AWS_SECRET_ACCESS_KEY`)
- `OPENSLIDE_S3_SESSION_TOKEN` (fallback: `AWS_SESSION_TOKEN`)

Notes:

- If access key and secret key are unset, OpenSlide sends unsigned requests.
- `OPENSLIDE_S3_ENDPOINT` is useful for S3-compatible storage.

### GCS

- `OPENSLIDE_GCS_ENDPOINT` (default `storage.googleapis.com`)
- `OPENSLIDE_GCS_BEARER_TOKEN` (fallback: `GOOGLE_OAUTH_ACCESS_TOKEN`)

### Azure Blob

- `OPENSLIDE_AZURE_ENDPOINT_SUFFIX` (default `blob.core.windows.net`)
- `OPENSLIDE_AZURE_ACCOUNT` (fallback: `AZURE_STORAGE_ACCOUNT`)
- `OPENSLIDE_AZURE_BEARER_TOKEN`
- `OPENSLIDE_AZURE_SAS_TOKEN` (fallback: `AZURE_STORAGE_SAS_TOKEN`)

Notes:

- SAS token may include leading `?`; both forms are accepted.

## 4. Open a cloud URI in code

Use `openslide_open_uri()` for default network/cache behavior:

```c
#include <openslide.h>

openslide_t *osr = openslide_open_uri("s3://my-bucket/slides/case-001.svs");
if (!osr) {
  // Not recognized or unsupported scheme
}
```

Use `openslide_open_with_options()` to control network and cache settings:

```c
#include <openslide.h>

openslide_source_t *src =
  openslide_source_create_uri("gs://my-bucket/slides/case-001.svs");
openslide_open_options_t *opts = openslide_open_options_create();

openslide_open_options_set_connection_timeout(opts, 15000);      // ms
openslide_open_options_set_read_timeout(opts, 60000);            // ms
openslide_open_options_set_max_retries(opts, 5);
openslide_open_options_set_max_parallel_requests(opts, 8);
openslide_open_options_set_storage_cache_size(opts, 128 * 1024 * 1024);

openslide_t *osr = openslide_open_with_options(src, opts);

openslide_open_options_destroy(opts);
openslide_source_destroy(src);
```

Default option values (when not overridden):

- connection timeout: 10000 ms
- read timeout: 30000 ms
- max retries: 3
- max parallel requests: 4
- storage cache size: 64 MiB

## 5. Quick troubleshooting

- Error `Unsupported URI scheme`: use one of `file://`, `s3://`, `gs://`, `az://`.
- Provider unavailable error: rebuild with the corresponding Meson option enabled.
- Auth failures (`HTTP 401/403`): verify token/keys and endpoint variables.
- URI parse errors: ensure required URI parts are present:
  - S3 and GCS need `bucket` and `key`
  - Azure needs `account`, `container`, and `blob`
