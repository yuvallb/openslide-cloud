/*
 *  OpenSlide, a library for reading whole slide image files
 *
 *  Copyright (c) 2007-2014 Carnegie Mellon University
 *  Copyright (c) 2021      Benjamin Gilbert
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

/**
 * @file openslide.h
 * The API for the OpenSlide library.
 *
 * All functions except openslide_close() are thread-safe.
 * See the openslide_close() documentation for its restrictions.
 */

#pragma once

#include "openslide-features.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * The main OpenSlide type.
 *
 * An @ref openslide_t object can be used concurrently from multiple threads
 * without locking.  (But you must lock or otherwise use memory barriers
 * when passing the object between threads.)
 */
typedef struct _openslide openslide_t;

/**
 * An OpenSlide tile cache.
 *
 * An @ref openslide_cache_t object can be used concurrently from multiple
 * threads without locking.  (But you must lock or otherwise use memory
 * barriers when passing the object between threads.)
 */
typedef struct _openslide_cache openslide_cache_t;

/**
 * A source identifier for opening a whole slide image.
 *
 * An @ref openslide_source_t identifies a slide source, which can be a local
 * file path, a URI, or a provider-backed reference. Use
 * openslide_source_create_path() to create from a local path, or
 * openslide_source_create_uri() to create from a URI.
 *
 * @since 4.1.0
 */
typedef struct _openslide_source openslide_source_t;

/**
 * Configuration options for opening a whole slide image.
 *
 * An @ref openslide_open_options_t contains optional configuration for
 * network behavior, caching, and provider-specific settings. Use
 * openslide_open_options_create() to create a new options object.
 *
 * @since 4.1.0
 */
typedef struct _openslide_open_options openslide_open_options_t;

/**
 * @name Basic Usage
 * Opening, reading, and closing whole slide images.
 */
//@{

/**
 * Quickly determine whether a whole slide image is recognized.
 *
 * If OpenSlide recognizes the file referenced by @p filename, return a
 * string identifying the slide format vendor.  This is equivalent to the
 * value of the #OPENSLIDE_PROPERTY_NAME_VENDOR property.  Calling
 * openslide_open() on this file will return a valid OpenSlide object or
 * an OpenSlide object in error state.
 *
 * Otherwise, return NULL.  Calling openslide_open() on this file will also
 * return NULL.
 *
 * @param filename The filename to check.  On Windows, this must be in UTF-8.
 * @return An identification of the format vendor for this file, or NULL.
 * @since 3.4.0
 */
OPENSLIDE_PUBLIC()
const char *openslide_detect_vendor(const char *filename);


/**
 * Open a whole slide image.
 *
 * This function can be expensive; avoid calling it unnecessarily.  For
 * example, a tile server should not call openslide_open() on every tile
 * request.  Instead, it should maintain a cache of OpenSlide objects and
 * reuse them when possible.
 *
 * @param filename The filename to open.  On Windows, this must be in UTF-8.
 * @return
 *         On success, a new OpenSlide object.
 *         If the file is not recognized by OpenSlide, NULL.
 *         If the file is recognized but an error occurred, an OpenSlide
 *         object in error state.
 */
OPENSLIDE_PUBLIC()
openslide_t *openslide_open(const char *filename);

/**
 * Open a whole slide image from a URI.
 *
 * This function is similar to openslide_open() but accepts a URI instead
 * of a local filesystem path. Supported URI schemes include:
 *
 * - `file://` - Local filesystem path (e.g., `file:///path/to/slide.svs`)
 * - `s3://` - Amazon S3 object (e.g., `s3://bucket/slides/sample.czi`)
 * - `gs://` - Google Cloud Storage object (e.g., `gs://bucket/slides/sample.czi`)
 * - `az://` - Azure Blob object (e.g., `az://account/container/slide.czi`)
 *
 * This function uses default configuration for network behavior, timeouts,
 * and caching. For advanced control, use openslide_open_with_options().
 *
 * On Windows, URIs should use forward slashes and paths should be in UTF-8.
 *
 * @param uri The URI to open.  On Windows, this must be in UTF-8.
 * @return
 *         On success, a new OpenSlide object.
 *         If the URI scheme is not recognized, NULL.
 *         If the resource is not a recognized slide format, NULL.
 *         If the resource is recognized but an error occurred, an OpenSlide
 *         object in error state.
 * @since 4.1.0
 */
OPENSLIDE_PUBLIC()
openslide_t *openslide_open_uri(const char *uri);

/**
 * Create an openslide_source from a local filesystem path.
 *
 * The returned source can be passed to openslide_open_with_options() along
 * with options to control network and cache behavior.
 *
 * The source must be released with openslide_source_destroy() when done.
 *
 * @param path The local filesystem path.
 * @return A new source object, or NULL if path is invalid.
 * @since 4.1.0
 */
OPENSLIDE_PUBLIC()
openslide_source_t *openslide_source_create_path(const char *path);

/**
 * Create an openslide_source from a URI.
 *
 * The returned source can be passed to openslide_open_with_options() along
 * with options to control network and cache behavior.
 *
 * The source must be released with openslide_source_destroy() when done.
 *
 * @param uri The URI (e.g., `s3://bucket/slide.czi`,
 *            `gs://bucket/slide.czi`, `az://account/container/slide.czi`).
 * @return A new source object, or NULL if the URI scheme is not recognized.
 * @since 4.1.0
 */
OPENSLIDE_PUBLIC()
openslide_source_t *openslide_source_create_uri(const char *uri);

/**
 * Release an openslide_source.
 *
 * @param source The source to release, or NULL.
 * @since 4.1.0
 */
OPENSLIDE_PUBLIC()
void openslide_source_destroy(openslide_source_t *source);

/**
 * Create default open options.
 *
 * The returned options object contains default values for all settings.
 * Individual settings can be modified with functions like
 * openslide_open_options_set_connection_timeout().
 *
 * The options must be released with openslide_open_options_destroy() when done.
 *
 * @return A new options object.
 * @since 4.1.0
 */
OPENSLIDE_PUBLIC()
openslide_open_options_t *openslide_open_options_create(void);

/**
 * Set the connection timeout for remote operations.
 *
 * This controls the maximum time to establish a connection to a remote
 * storage provider. The default is 10000 milliseconds (10 seconds).
 *
 * @param opts The options object.
 * @param timeout_ms The timeout in milliseconds, or 0 for no timeout.
 * @since 4.1.0
 */
OPENSLIDE_PUBLIC()
void openslide_open_options_set_connection_timeout(openslide_open_options_t *opts,
                                                   uint32_t timeout_ms);

/**
 * Set the read timeout for remote operations.
 *
 * This controls the maximum time to wait for a remote read operation to
 * complete. The default is 30000 milliseconds (30 seconds).
 *
 * @param opts The options object.
 * @param timeout_ms The timeout in milliseconds, or 0 for no timeout.
 * @since 4.1.0
 */
OPENSLIDE_PUBLIC()
void openslide_open_options_set_read_timeout(openslide_open_options_t *opts,
                                             uint32_t timeout_ms);

/**
 * Set the maximum number of retries for transient failures.
 *
 * When a remote read operation fails with a transient error, it will be
 * retried up to this many times. The default is 3 retries.
 *
 * @param opts The options object.
 * @param max_retries The maximum number of retries.
 * @since 4.1.0
 */
OPENSLIDE_PUBLIC()
void openslide_open_options_set_max_retries(openslide_open_options_t *opts,
                                            uint32_t max_retries);

/**
 * Set the maximum number of parallel requests.
 *
 * This controls the degree of parallelism when performing remote I/O
 * operations. The default is 4 concurrent requests.
 *
 * @param opts The options object.
 * @param max_parallel The maximum number of parallel requests.
 * @since 4.1.0
 */
OPENSLIDE_PUBLIC()
void openslide_open_options_set_max_parallel_requests(openslide_open_options_t *opts,
                                                      uint32_t max_parallel);

/**
 * Set the storage cache size.
 *
 * This configures a cache for frequently accessed metadata and byte ranges
 * from remote storage. The default is 64 MiB (67108864 bytes).
 *
 * Set to 0 to disable the storage cache.
 *
 * @param opts The options object.
 * @param cache_bytes The cache size in bytes.
 * @since 4.1.0
 */
OPENSLIDE_PUBLIC()
void openslide_open_options_set_storage_cache_size(openslide_open_options_t *opts,
                                                   uint64_t cache_bytes);

/**
 * Release open options.
 *
 * @param opts The options object, or NULL.
 * @since 4.1.0
 */
OPENSLIDE_PUBLIC()
void openslide_open_options_destroy(openslide_open_options_t *opts);

/**
 * Open a whole slide image with explicit source and options.
 *
 * This function combines a source (local path or URI) with explicit
 * configuration options to open a slide. It allows callers to control
 * network timeouts, cache sizes, retry behavior, and other tuning parameters.
 *
 * This function can be expensive; avoid calling it unnecessarily.  For
 * example, a tile server should not call openslide_open_with_options() on
 * every tile request.  Instead, it should maintain a cache of OpenSlide
 * objects and reuse them when possible.
 *
 * @param source The source to open.
 * @param opts The configuration options, or NULL for defaults.
 * @return
 *         On success, a new OpenSlide object.
 *         If the source is not a recognized slide format, NULL.
 *         If the source is recognized but an error occurred, an OpenSlide
 *         object in error state.
 * @since 4.1.0
 */
OPENSLIDE_PUBLIC()
openslide_t *openslide_open_with_options(const openslide_source_t *source,
                                         const openslide_open_options_t *opts);

/**
 * Get the number of levels in the whole slide image.
 *
 * @param osr The OpenSlide object.
 * @return The number of levels, or -1 if an error occurred.
 * @since 3.3.0
 */
OPENSLIDE_PUBLIC()
int32_t openslide_get_level_count(openslide_t *osr);


/**
 * Get the dimensions of level 0 (the largest level). Exactly
 * equivalent to calling openslide_get_level_dimensions(osr, 0, w, h).
 *
 * @param osr The OpenSlide object.
 * @param[out] w The width of the image, or -1 if an error occurred.
 * @param[out] h The height of the image, or -1 if an error occurred.
 * @since 3.3.0
 */
OPENSLIDE_PUBLIC()
void openslide_get_level0_dimensions(openslide_t *osr, int64_t *w, int64_t *h);


/**
 * Get the dimensions of a level.
 *
 * @param osr The OpenSlide object.
 * @param level The desired level.
 * @param[out] w The width of the image, or -1 if an error occurred
 *               or the level was out of range.
 * @param[out] h The height of the image, or -1 if an error occurred
 *               or the level was out of range.
 * @since 3.3.0
 */
OPENSLIDE_PUBLIC()
void openslide_get_level_dimensions(openslide_t *osr, int32_t level,
				    int64_t *w, int64_t *h);


/**
 * Get the downsampling factor of a given level.
 *
 * @param osr The OpenSlide object.
 * @param level The desired level.
 * @return The downsampling factor for this level, or -1.0 if an error occurred
 *         or the level was out of range.
 * @since 3.3.0
 */
OPENSLIDE_PUBLIC()
double openslide_get_level_downsample(openslide_t *osr, int32_t level);


/**
 * Get the best level to use for displaying the given downsample.
 *
 * @param osr The OpenSlide object.
 * @param downsample The downsample factor.
 * @return The level identifier, or -1 if an error occurred.
 * @since 3.3.0
 */
OPENSLIDE_PUBLIC()
int32_t openslide_get_best_level_for_downsample(openslide_t *osr,
						double downsample);

/**
 * Copy pre-multiplied ARGB data from a whole slide image.
 *
 * This function reads and decompresses a region of a whole slide
 * image into the specified memory location. @p dest must be a valid
 * pointer to enough memory to hold the region, at least (@p w * @p h * 4)
 * bytes in length. If an error occurs or has occurred, then the memory
 * pointed to by @p dest will be cleared.
 *
 * The returned pixel data is in device color space.  The slide's ICC color
 * profile, if available, can be read with openslide_read_icc_profile() and
 * used to transform the pixels for display.
 *
 * For more information about processing pre-multiplied pixel data, see
 * the [OpenSlide website](https://openslide.org/docs/premultiplied-argb/).
 *
 * @param osr The OpenSlide object.
 * @param dest The destination buffer for the ARGB data.
 * @param x The top left x-coordinate, in the level 0 reference frame.
 * @param y The top left y-coordinate, in the level 0 reference frame.
 * @param level The desired level.
 * @param w The width of the region. Must be non-negative.
 * @param h The height of the region. Must be non-negative.
 */
OPENSLIDE_PUBLIC()
void openslide_read_region(openslide_t *osr,
			   uint32_t *dest,
			   int64_t x, int64_t y,
			   int32_t level,
			   int64_t w, int64_t h);


/**
 * Get the size in bytes of the ICC color profile for the whole slide image.
 *
 * @param osr The OpenSlide object.
 * @return -1 on error, 0 if no profile is available, otherwise the profile
 * size in bytes.
 */
OPENSLIDE_PUBLIC()
int64_t openslide_get_icc_profile_size(openslide_t *osr);


/**
 * Copy the ICC color profile from a whole slide image.
 *
 * This function reads the ICC color profile from the slide into the specified
 * memory location.  @p dest must be a valid pointer to enough memory
 * to hold the profile.  Get the profile size with
 * openslide_get_icc_profile_size().
 *
 * If an error occurs or has occurred, then the memory pointed to by @p dest
 * will be cleared.
 *
 * @param osr The OpenSlide object.
 * @param dest The destination buffer for the ICC color profile.
 */
OPENSLIDE_PUBLIC()
void openslide_read_icc_profile(openslide_t *osr, void *dest);


/**
 * Close an OpenSlide object.
 * No other threads may be using the object.
 * After this function returns, the object cannot be used anymore.
 *
 * @param osr The OpenSlide object.
 */
OPENSLIDE_PUBLIC()
void openslide_close(openslide_t *osr);
//@}

/**
 * @name Error Handling
 * A simple mechanism for detecting errors.
 *
 * Sometimes an unrecoverable error can occur that will invalidate the
 * OpenSlide object. (This is typically something like an I/O error or
 * data corruption.)  When such an error happens in an OpenSlide
 * object, the object will move terminally into an error state.
 *
 * While an object is in an error state, no OpenSlide functions will
 * have any effect on it except for openslide_close(). Functions
 * that are expected to return values will instead return an error
 * value, typically something like NULL or -1. openslide_read_region()
 * will clear its destination buffer instead of painting into
 * it. openslide_get_error() will return a non-NULL string containing
 * an error message. See the documentation for each function for
 * details on what is returned in case of error.
 *
 * This style of error handling allows programs written in C to check
 * for errors only when convenient, because the error state is
 * terminal and the OpenSlide functions return harmlessly when there
 * has been an error.
 *
 * If writing wrappers for OpenSlide in languages that support
 * exceptions, it is recommended that the error state be checked after
 * each call and converted into an exception for that language.
 */
//@{
/**
 * Get the current error string.
 *
 * For a given OpenSlide object, once this function returns a non-NULL
 * value, the only useful operation on the object is to call
 * openslide_close() to free its resources.
 *
 * @param osr The OpenSlide object.
 * @return A string describing the original error that caused
 * the problem, or NULL if no error has occurred.
 * @since 3.2.0
 *
 */
OPENSLIDE_PUBLIC()
const char *openslide_get_error(openslide_t *osr);
//@}

/**
 * @name Predefined Properties
 * Some predefined properties.
 */
//@{

/**
 * The name of the property containing a slide's background color, if any.
 * It is represented as an RGB hex triplet.
 *
 * @since 3.2.3
 */
#define OPENSLIDE_PROPERTY_NAME_BACKGROUND_COLOR "openslide.background-color"

/**
 * The name of the property containing the height of the rectangle bounding
 * the non-empty region of the slide, if available.
 *
 * @since 3.4.0
 */
#define OPENSLIDE_PROPERTY_NAME_BOUNDS_HEIGHT "openslide.bounds-height"

/**
 * The name of the property containing the width of the rectangle bounding
 * the non-empty region of the slide, if available.
 *
 * @since 3.4.0
 */
#define OPENSLIDE_PROPERTY_NAME_BOUNDS_WIDTH "openslide.bounds-width"

/**
 * The name of the property containing the X coordinate of the rectangle
 * bounding the non-empty region of the slide, if available.
 *
 * @since 3.4.0
 */
#define OPENSLIDE_PROPERTY_NAME_BOUNDS_X "openslide.bounds-x"

/**
 * The name of the property containing the Y coordinate of the rectangle
 * bounding the non-empty region of the slide, if available.
 *
 * @since 3.4.0
 */
#define OPENSLIDE_PROPERTY_NAME_BOUNDS_Y "openslide.bounds-y"

/**
 * The name of the property containing a slide's comment, if any.
 *
 * @since 3.0.0
 */
#define OPENSLIDE_PROPERTY_NAME_COMMENT "openslide.comment"

/**
 * The name of the property containing the size of a slide's ICC color profile,
 * if any.
 *
 * @since 4.0.0
 */
#define OPENSLIDE_PROPERTY_NAME_ICC_SIZE "openslide.icc-size"

/**
 * The name of the property containing the number of microns per pixel in
 * the X dimension of level 0, if known.
 *
 * @since 3.3.0
 */
#define OPENSLIDE_PROPERTY_NAME_MPP_X "openslide.mpp-x"

/**
 * The name of the property containing the number of microns per pixel in
 * the Y dimension of level 0, if known.
 *
 * @since 3.3.0
 */
#define OPENSLIDE_PROPERTY_NAME_MPP_Y "openslide.mpp-y"

/**
 * The name of the property containing a slide's objective power, if known.
 *
 * @since 3.3.0
 */
#define OPENSLIDE_PROPERTY_NAME_OBJECTIVE_POWER "openslide.objective-power"

/**
 * The name of the property containing the "quickhash-1" sum.
 *
 * @since 3.0.0
 */
#define OPENSLIDE_PROPERTY_NAME_QUICKHASH1 "openslide.quickhash-1"

/**
 * The name of the property containing an identification of the vendor.
 *
 * @since 3.0.0
 */
#define OPENSLIDE_PROPERTY_NAME_VENDOR "openslide.vendor"

//@}

/**
 * @name Properties
 * Querying properties.
 *
 * Properties are string key-value pairs containing metadata about a whole
 * slide image.  These functions allow listing the available properties and
 * obtaining their values.
 *
 * [Some properties](https://openslide.org/properties/) are officially
 * documented and are expected to be stable; others are undocumented but may
 * still be useful.  Many properties are uninterpreted data gathered
 * directly from the slide files.  New properties may be added in future
 * releases of OpenSlide.
 */
//@{

/**
 * Get the NULL-terminated array of property names.
 *
 * This function returns an array of strings naming properties available
 * in the whole slide image.
 *
 * @param osr The OpenSlide object.
 * @return A NULL-terminated string array of property names, or
 *         an empty array if an error occurred.
 */
OPENSLIDE_PUBLIC()
const char * const *openslide_get_property_names(openslide_t *osr);

/**
 * Get the value of a single property.
 *
 * This function returns the value of the property given by @p name.
 *
 * @param osr The OpenSlide object.
 * @param name The name of the desired property. Must be
               a valid name as given by openslide_get_property_names().
 * @return The value of the named property, or NULL if the property
 *         doesn't exist or an error occurred.
 */
OPENSLIDE_PUBLIC()
const char *openslide_get_property_value(openslide_t *osr, const char *name);

//@}

/**
 * @name Associated Images
 * Reading associated images.
 *
 * Certain vendor-specific associated images may exist within a whole slide
 * image, such as label and thumbnail images.  Each associated image has a
 * name, dimensions, and pixel data.  These functions allow listing the
 * available associated images and reading their contents.
 */
//@{

/**
 * Get the NULL-terminated array of associated image names.
 *
 * This function returns an array of strings naming associated images
 * available in the whole slide image.
 *
 * @param osr The OpenSlide object.
 * @return A NULL-terminated string array of associated image names, or
           an empty array if an error occurred.
 */
OPENSLIDE_PUBLIC()
const char * const *openslide_get_associated_image_names(openslide_t *osr);

/**
 * Get the dimensions of an associated image.
 *
 * This function returns the width and height of an associated image
 * associated with a whole slide image. Once the dimensions are known,
 * use openslide_read_associated_image() to read the image.
 *
 * @param osr The OpenSlide object.
 * @param name The name of the desired associated image. Must be
 *            a valid name as given by openslide_get_associated_image_names().
 * @param[out] w The width of the associated image, or -1 if an error occurred.
 * @param[out] h The height of the associated image, or -1 if an error occurred.
 */
OPENSLIDE_PUBLIC()
void openslide_get_associated_image_dimensions(openslide_t *osr,
					       const char *name,
					       int64_t *w, int64_t *h);


/**
 * Copy pre-multiplied ARGB data from an associated image.
 *
 * This function reads and decompresses an associated image associated
 * with a whole slide image. @p dest must be a valid pointer to enough
 * memory to hold the image, at least (width * height * 4) bytes in
 * length.  Get the width and height with
 * openslide_get_associated_image_dimensions().
 *
 * If an error occurs or has occurred, then the memory pointed to by @p dest
 * will be cleared. In versions prior to 4.0.0, this function did nothing
 * if an error occurred.
 *
 * The returned pixel data is in device color space.  The associated image's
 * ICC color profile, if available, can be read with
 * openslide_read_associated_image_icc_profile() and used to transform the
 * pixels for display.
 *
 * For more information about processing pre-multiplied pixel data, see
 * the [OpenSlide website](https://openslide.org/docs/premultiplied-argb/).
 *
 * @param osr The OpenSlide object.
 * @param name The name of the desired associated image. Must be
 *             a valid name as given by openslide_get_associated_image_names().
 * @param dest The destination buffer for the ARGB data.
 */
OPENSLIDE_PUBLIC()
void openslide_read_associated_image(openslide_t *osr,
				     const char *name,
				     uint32_t *dest);


/**
 * Get the size in bytes of the ICC color profile for an associated image.
 *
 * @param osr The OpenSlide object.
 * @param name The name of the desired associated image. Must be
 *             a valid name as given by openslide_get_associated_image_names().
 * @return -1 on error, 0 if no profile is available, otherwise the profile
 * size in bytes.
 */
OPENSLIDE_PUBLIC()
int64_t openslide_get_associated_image_icc_profile_size(openslide_t *osr,
                                                        const char *name);


/**
 * Copy the ICC color profile from an associated image.
 *
 * This function reads the ICC color profile from an associated image into
 * the specified memory location.  @p dest must be a valid pointer to enough
 * memory to hold the profile.  Get the profile size with
 * openslide_get_associated_image_icc_profile_size().
 *
 * If an error occurs or has occurred, then the memory pointed to by @p dest
 * will be cleared.
 *
 * @param osr The OpenSlide object.
 * @param name The name of the desired associated image. Must be
 *             a valid name as given by openslide_get_associated_image_names().
 * @param dest The destination buffer for the ICC color profile.
 */
OPENSLIDE_PUBLIC()
void openslide_read_associated_image_icc_profile(openslide_t *osr,
                                                 const char *name,
                                                 void *dest);

//@}

/**
 * @name Caching
 * Managing the in-memory tile cache.
 *
 * By default, each OpenSlide object has its own internal cache.  These
 * functions can be used to configure a cache with a custom size, which may
 * be shared between multiple OpenSlide objects.
 */
//@{

/**
 * Create a new tile cache, unconnected to any OpenSlide object.  The cache
 * can be attached to one or more OpenSlide objects with openslide_set_cache().
 * The cache must be released with openslide_cache_release() when done.
 *
 * @param capacity The capacity of the cache, in bytes.
 * @return A new cache.
 * @since 4.0.0
 */
OPENSLIDE_PUBLIC()
openslide_cache_t *openslide_cache_create(size_t capacity);

/**
 * Attach a cache to the specified OpenSlide object, replacing the
 * current cache.
 *
 * @param osr The OpenSlide object.
 * @param cache The cache to attach.
 * @since 4.0.0
 */
OPENSLIDE_PUBLIC()
void openslide_set_cache(openslide_t *osr, openslide_cache_t *cache);

/**
 * Release the cache.  The cache may be released while it is still attached
 * to OpenSlide objects.  It will be freed once the last attached OpenSlide
 * object is closed.
 *
 * @param cache The cache to release.
 * @since 4.0.0
 */
OPENSLIDE_PUBLIC()
void openslide_cache_release(openslide_cache_t *cache);

//@}

/**
 * @name Miscellaneous
 * Utility functions.
 */
//@{

/**
 * Get the version of the OpenSlide library.
 *
 * @return A string describing the OpenSlide version.
 * @since 3.3.0
 */
OPENSLIDE_PUBLIC()
const char *openslide_get_version(void);

//@}

/**
 * @mainpage OpenSlide
 *
 * OpenSlide is a C library that provides a simple interface to read
 * whole-slide images (also known as virtual slides). See the
 * [OpenSlide website](https://openslide.org/) for more details.
 */

#ifdef __cplusplus
}
#endif
