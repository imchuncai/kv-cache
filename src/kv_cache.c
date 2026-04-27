#include "kv_cache.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/* ── Doubly-linked list node (also the hash-map entry) ─────────────────── */

typedef struct node {
    char *key;
    char *value;
    struct node *prev;      /* LRU list */
    struct node *next;      /* LRU list */
    struct node *hash_next; /* hash chain */
} node_t;

/* ── Cache structure ───────────────────────────────────────────────────── */

struct bucket {
    pthread_mutex_t mu;
    node_t *nodes;
};

struct kv_cache {
    pthread_mutex_t mu;
    size_t   capacity;
    size_t   size;
    size_t   bucket_count;
    struct bucket *buckets; /* hash table (separate chaining) */
    node_t   head;          /* sentinel: head.next = MRU */
    node_t   tail;          /* sentinel: tail.prev = LRU */
};

/* ── Hash function (FNV-1a) ────────────────────────────────────────────── */

static size_t hash_key(const char *key, size_t bucket_count)
{
    size_t h = 14695981039346656037ULL;
    for (const char *p = key; *p; p++) {
        h ^= (unsigned char)*p;
        h *= 1099511628211ULL;
    }
    return h % bucket_count;
}

/* ── Linked-list helpers ───────────────────────────────────────────────── */

static void list_remove(node_t *n)
{
    n->prev->next = n->next;
    n->next->prev = n->prev;
}

static void list_push_front(kv_cache_t *cache, node_t *n)
{
    n->next = cache->head.next;
    n->prev = &cache->head;
    cache->head.next->prev = n;
    cache->head.next = n;
}

/* ── Internal helpers ──────────────────────────────────────────────────── */

static node_t *find_node(kv_cache_t *cache, const char *key, size_t bucket)
{
    for (node_t *n = cache->buckets[bucket].nodes; n; n = n->hash_next)
        if (strcmp(n->key, key) == 0)
            return n;
    return NULL;
}

static void remove_from_bucket(kv_cache_t *cache, node_t *n, size_t bucket)
{
    node_t **pp = &cache->buckets[bucket].nodes;
    while (*pp != n)
        pp = &(*pp)->hash_next;
    *pp = n->hash_next;
}

static void free_node(node_t *n)
{
    free(n->key);
    free(n->value);
    free(n);
}

static void evict_lru(kv_cache_t *cache)
{
    node_t *lru = cache->tail.prev;
    list_remove(lru);
    size_t bucket = hash_key(lru->key, cache->bucket_count);
    remove_from_bucket(cache, lru, bucket);
    free_node(lru);
    cache->size--;
}

/* ── Public API ────────────────────────────────────────────────────────── */

kv_cache_t *kv_cache_create(size_t capacity)
{
    if (capacity == 0)
        return NULL;

    kv_cache_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;

    pthread_mutex_init(&c->mu, NULL);
    c->capacity     = capacity;
    c->bucket_count = capacity * 2;   /* load factor ~0.5 */
    c->buckets      = calloc(c->bucket_count, sizeof(*(c->buckets)));
    if (!c->buckets) { free(c); return NULL; }

    for (size_t i = 0; i < c->bucket_count; i++) {
        pthread_mutex_init(&c->buckets[i].mu, NULL);
    }

    /* Wire sentinels */
    c->head.next = &c->tail;
    c->tail.prev = &c->head;

    return c;
}

void kv_cache_destroy(kv_cache_t *cache)
{
    if (!cache) return;
    node_t *n = cache->head.next;
    while (n != &cache->tail) {
        node_t *next = n->next;
        free_node(n);
        n = next;
    }
    free(cache->buckets);
    free(cache);
}

bool kv_cache_put(kv_cache_t *cache, const char *key, const char *value)
{
    size_t bucket = hash_key(key, cache->bucket_count);
    node_t *n = find_node(cache, key, bucket);

    if (n) {
        /* Update existing entry */
        char *new_val = strdup(value);
        if (!new_val) return false;
        free(n->value);
        n->value = new_val;
        list_remove(n);
        list_push_front(cache, n);
        return true;
    }

    /* Evict if at capacity */
    if (cache->size >= cache->capacity)
        evict_lru(cache);

    /* Create new node */
    n = malloc(sizeof(*n));
    if (!n) return false;
    n->key  = strdup(key);
    n->value = strdup(value);
    if (!n->key || !n->value) {
        free(n->key);
        free(n->value);
        free(n);
        return false;
    }

    /* Insert into hash table */
    n->hash_next = cache->buckets[bucket].nodes;
    cache->buckets[bucket].nodes = n;

    /* Insert at front of LRU list */
    list_push_front(cache, n);
    cache->size++;

    return true;
}

bool kv_cache_mu_put(kv_cache_t *cache, const char *key, const char *value)
{
    size_t bucket = hash_key(key, cache->bucket_count);
    struct bucket *b = &cache->buckets[bucket];
    struct node *evict = NULL;

    pthread_mutex_lock(&b->mu);
    node_t *n = b->nodes;
    while (n && strcmp(n->key, key)) {
        n = n->hash_next;
    }
    if (n) {
        char *new_val = strdup(value);
        if (new_val == NULL)
            goto unlock_exit_fail;

        free(n->value);
        n->value = new_val;

        pthread_mutex_lock(&cache->mu);
        if (n->next) {
            list_remove(n);
            list_push_front(cache, n);
        }
        pthread_mutex_unlock(&cache->mu);
    } else {
        n = malloc(sizeof(*n));
        if (n == NULL)
            goto unlock_exit_fail;

        n->key  = strdup(key);
        n->value = strdup(value);
        if (!n->key || !n->value) {
            free(n->key);
            free(n->value);
            free(n);
            goto unlock_exit_fail;
        }
        /* Insert into hash table */
        n->hash_next = b->nodes;
        b->nodes = n;

        pthread_mutex_lock(&cache->mu);
        if (cache->size >= cache->capacity) {
            evict = cache->tail.prev;
            list_remove(evict);
            evict->next = NULL;
            cache->size--;
        }
        /* Insert at front of LRU list */
        list_push_front(cache, n);
        cache->size++;
        pthread_mutex_unlock(&cache->mu);
    }
    pthread_mutex_unlock(&b->mu);
    if (evict) {
        size_t bucket = hash_key(evict->key, cache->bucket_count);
        struct bucket *b = &cache->buckets[bucket];

        pthread_mutex_lock(&b->mu);
        remove_from_bucket(cache, evict, bucket);
        pthread_mutex_unlock(&b->mu);

        free_node(evict);
    }
    return true;

unlock_exit_fail:
    pthread_mutex_unlock(&b->mu);
    return false;
}

const char *kv_cache_get(kv_cache_t *cache, const char *key)
{
    size_t bucket = hash_key(key, cache->bucket_count);
    node_t *n = find_node(cache, key, bucket);
    if (!n) return NULL;

    /* Promote to MRU */
    list_remove(n);
    list_push_front(cache, n);
    return n->value;
}

bool kv_cache_mu_get(kv_cache_t *cache, const char *key)
{
    size_t bucket = hash_key(key, cache->bucket_count);
    struct bucket *b = &cache->buckets[bucket];
    pthread_mutex_lock(&b->mu);
    node_t *n = b->nodes;
    while (n && strcmp(n->key, key)) {
        n = n->hash_next;
    }
    if (n) {
        pthread_mutex_lock(&cache->mu);
        if (n->next) {
            list_remove(n);
            list_push_front(cache, n);
        }
        pthread_mutex_unlock(&cache->mu);
    }
    pthread_mutex_unlock(&b->mu);
    return n;
}

const char *kv_cache_peek(kv_cache_t *cache, const char *key)
{
    size_t bucket = hash_key(key, cache->bucket_count);
    node_t *n = find_node(cache, key, bucket);
    return n ? n->value : NULL;
}

bool kv_cache_delete(kv_cache_t *cache, const char *key)
{
    size_t bucket = hash_key(key, cache->bucket_count);
    node_t *n = find_node(cache, key, bucket);
    if (!n) return false;

    list_remove(n);
    remove_from_bucket(cache, n, bucket);
    free_node(n);
    cache->size--;
    return true;
}

size_t kv_cache_size(kv_cache_t *cache)
{
    return cache->size;
}
