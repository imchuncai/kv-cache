#ifndef KV_CACHE_H
#define KV_CACHE_H

#include <stddef.h>
#include <stdbool.h>

/*
 * Thread safety: this cache is NOT thread-safe by design (single-threaded
 * model, same as Redis). The caller must ensure only one thread accesses
 * the cache at a time. For multi-threaded programs, funnel all cache
 * operations through a single dedicated thread.
 */
typedef struct kv_cache kv_cache_t;

/* Create a new LRU cache with the given capacity (max number of entries). */
kv_cache_t *kv_cache_create(size_t capacity);

/* Destroy the cache and free all memory. */
void kv_cache_destroy(kv_cache_t *cache);

/*
 * Insert or update a key-value pair.
 * Keys and values are copied internally.
 * Returns true on success, false on allocation failure.
 */
bool kv_cache_put(kv_cache_t *cache, const char *key, const char *value);

/*
 * Retrieve the value for a key.
 * Returns a pointer to the internal value string (valid until the entry is
 * evicted or updated), or NULL if the key is not found.
 * Accessing a key promotes it to most-recently-used.
 */
const char *kv_cache_get(kv_cache_t *cache, const char *key);

/*
 * Retrieve the value for a key WITHOUT promoting it in the LRU list.
 * Pure read — safe to call concurrently with other peeks (no mutation).
 */
const char *kv_cache_peek(kv_cache_t *cache, const char *key);

/* Remove a key. Returns true if the key existed. */
bool kv_cache_delete(kv_cache_t *cache, const char *key);

/* Return the current number of entries in the cache. */
size_t kv_cache_size(kv_cache_t *cache);

#endif /* KV_CACHE_H */
