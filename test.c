#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "kv_cache.h"

int main(void)
{
    kv_cache_t *c = kv_cache_create(3);
    assert(c);

    /* Basic put/get */
    assert(kv_cache_put(c, "a", "1"));
    assert(kv_cache_put(c, "b", "2"));
    assert(kv_cache_put(c, "c", "3"));
    assert(strcmp(kv_cache_get(c, "a"), "1") == 0);
    assert(strcmp(kv_cache_get(c, "b"), "2") == 0);
    assert(strcmp(kv_cache_get(c, "c"), "3") == 0);
    assert(kv_cache_size(c) == 3);

    /* Eviction: inserting "d" should evict LRU.
     * Access order after gets above: c(MRU), b, a(LRU).
     * So "a" gets evicted. */
    assert(kv_cache_put(c, "d", "4"));
    assert(kv_cache_get(c, "a") == NULL);    /* evicted */
    assert(strcmp(kv_cache_get(c, "b"), "2") == 0);
    assert(kv_cache_size(c) == 3);

    /* Update existing key */
    assert(kv_cache_put(c, "b", "20"));
    assert(strcmp(kv_cache_get(c, "b"), "20") == 0);

    /* Delete */
    assert(kv_cache_delete(c, "b"));
    assert(kv_cache_get(c, "b") == NULL);
    assert(kv_cache_size(c) == 2);
    assert(!kv_cache_delete(c, "nonexistent"));

    kv_cache_destroy(c);
    printf("All tests passed.\n");
    return 0;
}
