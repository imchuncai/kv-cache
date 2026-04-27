#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include "kv_cache.h"

#define NUM_OPS      1000000
#define CACHE_CAP    10000
#define KEY_SPACE    50000
#define VAL_LEN      64
#define NUM_THREADS  4

static pthread_mutex_t cache_lock = PTHREAD_MUTEX_INITIALIZER;

static char *rand_value(char *buf, size_t len, unsigned int *seed)
{
    for (size_t i = 0; i < len - 1; i++)
        buf[i] = 'A' + (rand_r(seed) % 26);
    buf[len - 1] = '\0';
    return buf;
}

static double elapsed_ms(struct timespec start, struct timespec end)
{
    return (end.tv_sec - start.tv_sec) * 1e3
         + (end.tv_nsec - start.tv_nsec) / 1e6;
}

/* ── Single-threaded benchmark ─────────────────────────────────────────── */

static void bench_single_threaded(void)
{
    unsigned int seed = 42;
    kv_cache_t *cache = kv_cache_create(CACHE_CAP);
    char key[32], val[VAL_LEN];
    size_t hits = 0, misses = 0;

    /* Pre-fill */
    for (int i = 0; i < CACHE_CAP; i++) {
        snprintf(key, sizeof(key), "key_%d", rand_r(&seed) % KEY_SPACE);
        rand_value(val, VAL_LEN, &seed);
        kv_cache_put(cache, key, val);
    }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < NUM_OPS; i++) {
        snprintf(key, sizeof(key), "key_%d", rand_r(&seed) % KEY_SPACE);
        if (rand_r(&seed) % 5 == 0) {
            rand_value(val, VAL_LEN, &seed);
            kv_cache_put(cache, key, val);
        } else {
            if (kv_cache_get(cache, key))
                hits++;
            else
                misses++;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);

    printf("  time       : %.2f ms\n", ms);
    printf("  throughput : %.0f ops/sec\n", NUM_OPS / (ms / 1e3));
    printf("  avg latency: %.0f ns/op\n", ms * 1e6 / NUM_OPS);
    printf("  hit rate   : %.1f%% (%zu hits, %zu misses)\n",
           100.0 * hits / (hits + misses), hits, misses);

    kv_cache_destroy(cache);
}

/* ── Multi-threaded benchmark (mutex-protected) ────────────────────────── */

typedef struct {
    kv_cache_t *cache;
    int         ops_per_thread;
    int         thread_id;
    size_t      hits;
    size_t      misses;
} thread_arg_t;

static void *worker(void *arg)
{
    thread_arg_t *a = (thread_arg_t *)arg;
    unsigned int seed = 42 + a->thread_id;
    char key[32], val[VAL_LEN];

    for (int i = 0; i < a->ops_per_thread; i++) {
        snprintf(key, sizeof(key), "key_%d", rand_r(&seed) % KEY_SPACE);

        if (rand_r(&seed) % 5 == 0) {
            rand_value(val, VAL_LEN, &seed);
            kv_cache_mu_put(a->cache, key, val);
        } else {
            if (kv_cache_mu_get(a->cache, key))
                a->hits++;
            else
                a->misses++;
        }
    }
    return NULL;
}

static void bench_multi_threaded(void)
{
    unsigned int seed = 100;
    // Note: thread always allocate node before evict, so reserve some memory
    kv_cache_t *cache = kv_cache_create(CACHE_CAP - NUM_THREADS);
    char key[32], val[VAL_LEN];

    /* Pre-fill */
    for (int i = 0; i < CACHE_CAP; i++) {
        snprintf(key, sizeof(key), "key_%d", rand_r(&seed) % KEY_SPACE);
        rand_value(val, VAL_LEN, &seed);
        kv_cache_put(cache, key, val);
    }

    int ops_per_thread = NUM_OPS / NUM_THREADS;
    int total_ops = ops_per_thread * NUM_THREADS;
    pthread_t threads[NUM_THREADS];
    thread_arg_t args[NUM_THREADS];

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < NUM_THREADS; i++) {
        args[i] = (thread_arg_t){
            .cache = cache,
            .ops_per_thread = ops_per_thread,
            .thread_id = i,
            .hits = 0,
            .misses = 0,
        };
        pthread_create(&threads[i], NULL, worker, &args[i]);
    }

    size_t total_hits = 0, total_misses = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
        total_hits += args[i].hits;
        total_misses += args[i].misses;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);

    printf("  threads    : %d\n", NUM_THREADS);
    printf("  total ops  : %d (%d per thread)\n", total_ops, ops_per_thread);
    printf("  time       : %.2f ms\n", ms);
    printf("  throughput : %.0f ops/sec\n", total_ops / (ms / 1e3));
    printf("  avg latency: %.0f ns/op\n", ms * 1e6 / total_ops);
    printf("  hit rate   : %.1f%% (%zu hits, %zu misses)\n",
           100.0 * total_hits / (total_hits + total_misses),
           total_hits, total_misses);

    kv_cache_destroy(cache);
}

/* ── Pre-generate keys (removes snprintf/rand from hot path) ──────────── */

static char **pregenerate_keys(int count, unsigned int *seed)
{
    char **keys = malloc(count * sizeof(char *));
    for (int i = 0; i < count; i++) {
        keys[i] = malloc(32);
        snprintf(keys[i], 32, "key_%d", rand_r(seed) % KEY_SPACE);
    }
    return keys;
}

static void free_keys(char **keys, int count)
{
    for (int i = 0; i < count; i++)
        free(keys[i]);
    free(keys);
}

/* ── Write-only: single-threaded (pre-generated keys+values) ───────────── */

static void bench_write_single(void)
{
    unsigned int seed = 42;
    kv_cache_t *cache = kv_cache_create(CACHE_CAP);

    /* Pre-generate keys and values */
    char **keys = pregenerate_keys(NUM_OPS, &seed);
    char **vals = malloc(NUM_OPS * sizeof(char *));
    for (int i = 0; i < NUM_OPS; i++) {
        vals[i] = malloc(VAL_LEN);
        rand_value(vals[i], VAL_LEN, &seed);
    }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < NUM_OPS; i++)
        kv_cache_put(cache, keys[i], vals[i]);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);

    printf("  time       : %.2f ms\n", ms);
    printf("  throughput : %.0f ops/sec\n", NUM_OPS / (ms / 1e3));
    printf("  avg latency: %.0f ns/op\n", ms * 1e6 / NUM_OPS);

    free_keys(keys, NUM_OPS);
    free_keys(vals, NUM_OPS);
    kv_cache_destroy(cache);
}

/* ── Write-only: multi-threaded (pre-generated keys+values, mutex) ─────── */

typedef struct {
    kv_cache_t *cache;
    char      **keys;
    char      **vals;
    int         ops_per_thread;
} write_thread_arg_t;

static void *write_worker(void *arg)
{
    write_thread_arg_t *a = (write_thread_arg_t *)arg;

    for (int i = 0; i < a->ops_per_thread; i++) {
        pthread_mutex_lock(&cache_lock);
        kv_cache_put(a->cache, a->keys[i], a->vals[i]);
        pthread_mutex_unlock(&cache_lock);
    }
    return NULL;
}

static void bench_write_multi(void)
{
    kv_cache_t *cache = kv_cache_create(CACHE_CAP);

    int ops_per_thread = NUM_OPS / NUM_THREADS;
    int total_ops = ops_per_thread * NUM_THREADS;

    /* Pre-generate keys and values per thread */
    char **all_keys[NUM_THREADS];
    char **all_vals[NUM_THREADS];
    for (int t = 0; t < NUM_THREADS; t++) {
        unsigned int tseed = 42 + t;
        all_keys[t] = pregenerate_keys(ops_per_thread, &tseed);
        all_vals[t] = malloc(ops_per_thread * sizeof(char *));
        for (int i = 0; i < ops_per_thread; i++) {
            all_vals[t][i] = malloc(VAL_LEN);
            rand_value(all_vals[t][i], VAL_LEN, &tseed);
        }
    }

    pthread_t threads[NUM_THREADS];
    write_thread_arg_t args[NUM_THREADS];

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < NUM_THREADS; i++) {
        args[i] = (write_thread_arg_t){
            .cache = cache,
            .keys = all_keys[i],
            .vals = all_vals[i],
            .ops_per_thread = ops_per_thread,
        };
        pthread_create(&threads[i], NULL, write_worker, &args[i]);
    }

    for (int i = 0; i < NUM_THREADS; i++)
        pthread_join(threads[i], NULL);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);

    printf("  threads    : %d\n", NUM_THREADS);
    printf("  total ops  : %d (%d per thread)\n", total_ops, ops_per_thread);
    printf("  time       : %.2f ms\n", ms);
    printf("  throughput : %.0f ops/sec\n", total_ops / (ms / 1e3));
    printf("  avg latency: %.0f ns/op\n", ms * 1e6 / total_ops);

    for (int t = 0; t < NUM_THREADS; t++) {
        free_keys(all_keys[t], ops_per_thread);
        free_keys(all_vals[t], ops_per_thread);
    }
    kv_cache_destroy(cache);
}

/* ── Read-only (peek, no LRU): single-threaded ────────────────────────── */

static void bench_read_single(void)
{
    unsigned int seed = 42;
    kv_cache_t *cache = kv_cache_create(CACHE_CAP);
    char key[32], val[VAL_LEN];
    size_t hits = 0, misses = 0;

    /* Pre-fill */
    for (int i = 0; i < CACHE_CAP; i++) {
        snprintf(key, sizeof(key), "key_%d", rand_r(&seed) % KEY_SPACE);
        rand_value(val, VAL_LEN, &seed);
        kv_cache_put(cache, key, val);
    }

    /* Pre-generate lookup keys */
    char **keys = pregenerate_keys(NUM_OPS, &seed);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < NUM_OPS; i++) {
        if (kv_cache_peek(cache, keys[i]))
            hits++;
        else
            misses++;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);

    printf("  time       : %.2f ms\n", ms);
    printf("  throughput : %.0f ops/sec\n", NUM_OPS / (ms / 1e3));
    printf("  avg latency: %.0f ns/op\n", ms * 1e6 / NUM_OPS);
    printf("  hit rate   : %.1f%% (%zu hits, %zu misses)\n",
           100.0 * hits / (hits + misses), hits, misses);

    free_keys(keys, NUM_OPS);
    kv_cache_destroy(cache);
}

/* ── Read-only (peek, no LRU): multi-threaded, NO locks ───────────────── */

typedef struct {
    kv_cache_t *cache;
    char      **keys;
    int         ops_per_thread;
    int         thread_id;
    size_t      hits;
    size_t      misses;
} read_thread_arg_t;

static void *read_worker(void *arg)
{
    read_thread_arg_t *a = (read_thread_arg_t *)arg;

    for (int i = 0; i < a->ops_per_thread; i++) {
        if (kv_cache_peek(a->cache, a->keys[i]))
            a->hits++;
        else
            a->misses++;
    }
    return NULL;
}

static void bench_read_multi(void)
{
    unsigned int seed = 100;
    kv_cache_t *cache = kv_cache_create(CACHE_CAP);
    char key[32], val[VAL_LEN];

    /* Pre-fill */
    for (int i = 0; i < CACHE_CAP; i++) {
        snprintf(key, sizeof(key), "key_%d", rand_r(&seed) % KEY_SPACE);
        rand_value(val, VAL_LEN, &seed);
        kv_cache_put(cache, key, val);
    }

    int ops_per_thread = NUM_OPS / NUM_THREADS;
    int total_ops = ops_per_thread * NUM_THREADS;

    /* Pre-generate keys per thread */
    char **all_keys[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
        unsigned int tseed = 42 + i;
        all_keys[i] = pregenerate_keys(ops_per_thread, &tseed);
    }

    pthread_t threads[NUM_THREADS];
    read_thread_arg_t args[NUM_THREADS];

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < NUM_THREADS; i++) {
        args[i] = (read_thread_arg_t){
            .cache = cache,
            .keys = all_keys[i],
            .ops_per_thread = ops_per_thread,
            .thread_id = i,
            .hits = 0,
            .misses = 0,
        };
        pthread_create(&threads[i], NULL, read_worker, &args[i]);
    }

    size_t total_hits = 0, total_misses = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
        total_hits += args[i].hits;
        total_misses += args[i].misses;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);

    printf("  threads    : %d\n", NUM_THREADS);
    printf("  total ops  : %d (%d per thread)\n", total_ops, ops_per_thread);
    printf("  time       : %.2f ms\n", ms);
    printf("  throughput : %.0f ops/sec\n", total_ops / (ms / 1e3));
    printf("  avg latency: %.0f ns/op\n", ms * 1e6 / total_ops);
    printf("  hit rate   : %.1f%% (%zu hits, %zu misses)\n",
           100.0 * total_hits / (total_hits + total_misses),
           total_hits, total_misses);

    for (int i = 0; i < NUM_THREADS; i++)
        free_keys(all_keys[i], ops_per_thread);
    kv_cache_destroy(cache);
}

/* ── Main ──────────────────────────────────────────────────────────────── */

int main(void)
{
    printf("=== LRU KV Cache Benchmark ===\n");
    printf("  capacity : %d\n", CACHE_CAP);
    printf("  key space: %d\n", KEY_SPACE);
    printf("  ops      : %d\n\n", NUM_OPS);

    printf("[1] Mixed 80/20 — Single-threaded (no locks)\n");
    bench_single_threaded();

    printf("\n[2] Mixed 80/20 — Multi-threaded (mutex, %d threads)\n", NUM_THREADS);
    bench_multi_threaded();

    printf("\n[3] Write-only — Single-threaded (no locks)\n");
    bench_write_single();

    printf("\n[4] Write-only — Multi-threaded (mutex, %d threads)\n", NUM_THREADS);
    bench_write_multi();

    printf("\n[5] Read-only (peek) — Single-threaded (no locks)\n");
    bench_read_single();

    printf("\n[6] Read-only (peek) — Multi-threaded (NO locks, %d threads)\n", NUM_THREADS);
    bench_read_multi();

    return 0;
}
