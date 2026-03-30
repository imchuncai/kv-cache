#include "shard.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Virtual node on the hash ring ─────────────────────────────────────── */

typedef struct {
    uint32_t hash;
    int      shard_id;
} vnode_t;

/* ── Shard ring structure ──────────────────────────────────────────────── */

struct shard_ring {
    int          num_shards;
    size_t       capacity_per_shard;
    int          vnodes_per_shard;
    int          replica_count;
    kv_cache_t  *shards[MAX_SHARDS];
    bool         alive[MAX_SHARDS];
    int          num_vnodes;
    vnode_t     *vnodes;            /* sorted by hash */
};

/* ── Hash function (FNV-1a, 32-bit for ring position) ──────────────────── */

static uint32_t ring_hash(const char *key)
{
    uint32_t h = 2166136261U;
    for (const char *p = key; *p; p++) {
        h ^= (unsigned char)*p;
        h *= 16777619U;
    }
    return h;
}

/* ── Comparator for sorting vnodes ─────────────────────────────────────── */

static int vnode_cmp(const void *a, const void *b)
{
    uint32_t ha = ((const vnode_t *)a)->hash;
    uint32_t hb = ((const vnode_t *)b)->hash;
    return (ha > hb) - (ha < hb);
}

/*
 * Find N distinct alive shards for a key by walking the ring clockwise.
 * Returns the number of shards found (may be < n if shards are down).
 */
static int find_n_shards(shard_ring_t *ring, const char *key, int *out, int n)
{
    uint32_t h = ring_hash(key);

    /* Binary search for first vnode with hash >= h */
    int lo = 0, hi = ring->num_vnodes;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (ring->vnodes[mid].hash < h)
            lo = mid + 1;
        else
            hi = mid;
    }

    int found = 0;
    for (int i = 0; i < ring->num_vnodes && found < n; i++) {
        int idx = (lo + i) % ring->num_vnodes;
        int sid = ring->vnodes[idx].shard_id;
        if (!ring->alive[sid])
            continue;
        /* Skip duplicates (same physical shard from different vnodes) */
        bool dup = false;
        for (int j = 0; j < found; j++)
            if (out[j] == sid) { dup = true; break; }
        if (!dup)
            out[found++] = sid;
    }

    return found;
}

/* ── Public API ────────────────────────────────────────────────────────── */

shard_ring_t *shard_ring_create(int num_shards, size_t capacity_per_shard,
                                int vnodes_per_shard, int replica_count)
{
    if (num_shards <= 0 || num_shards > MAX_SHARDS)
        return NULL;
    if (replica_count < 1) replica_count = 1;
    if (replica_count > MAX_REPLICAS) replica_count = MAX_REPLICAS;
    if (replica_count > num_shards) replica_count = num_shards;

    shard_ring_t *ring = calloc(1, sizeof(*ring));
    if (!ring) return NULL;

    ring->num_shards = num_shards;
    ring->capacity_per_shard = capacity_per_shard;
    ring->vnodes_per_shard = vnodes_per_shard;
    ring->replica_count = replica_count;
    ring->num_vnodes = num_shards * vnodes_per_shard;
    ring->vnodes = malloc(ring->num_vnodes * sizeof(vnode_t));

    /* Create shards and virtual nodes */
    int vi = 0;
    for (int s = 0; s < num_shards; s++) {
        ring->shards[s] = kv_cache_create(capacity_per_shard);
        ring->alive[s] = true;

        for (int v = 0; v < vnodes_per_shard; v++) {
            char vname[64];
            snprintf(vname, sizeof(vname), "shard-%d-vnode-%d", s, v);
            ring->vnodes[vi].hash = ring_hash(vname);
            ring->vnodes[vi].shard_id = s;
            vi++;
        }
    }

    /* Sort ring by hash */
    qsort(ring->vnodes, ring->num_vnodes, sizeof(vnode_t), vnode_cmp);

    return ring;
}

void shard_ring_destroy(shard_ring_t *ring)
{
    if (!ring) return;
    for (int i = 0; i < ring->num_shards; i++)
        if (ring->shards[i])
            kv_cache_destroy(ring->shards[i]);
    free(ring->vnodes);
    free(ring);
}

bool shard_ring_put(shard_ring_t *ring, const char *key, const char *value)
{
    int targets[MAX_REPLICAS];
    int n = find_n_shards(ring, key, targets, ring->replica_count);
    if (n == 0) return false;

    bool ok = true;
    for (int i = 0; i < n; i++)
        ok = kv_cache_put(ring->shards[targets[i]], key, value) && ok;
    return ok;
}

const char *shard_ring_get(shard_ring_t *ring, const char *key)
{
    int targets[MAX_REPLICAS];
    int n = find_n_shards(ring, key, targets, ring->replica_count);

    /* Try each replica in order until we get a hit */
    for (int i = 0; i < n; i++) {
        const char *val = kv_cache_peek(ring->shards[targets[i]], key);
        if (val) return val;
    }
    return NULL;
}

int shard_ring_down(shard_ring_t *ring, int shard_id)
{
    if (shard_id < 0 || shard_id >= ring->num_shards) return -1;
    ring->alive[shard_id] = false;
    kv_cache_destroy(ring->shards[shard_id]);
    ring->shards[shard_id] = NULL;
    return shard_id;
}

int shard_ring_up(shard_ring_t *ring, int shard_id)
{
    if (shard_id < 0 || shard_id >= ring->num_shards) return -1;
    ring->shards[shard_id] = kv_cache_create(ring->capacity_per_shard);
    ring->alive[shard_id] = true;
    return 0;
}

bool shard_ring_is_alive(shard_ring_t *ring, int shard_id)
{
    if (shard_id < 0 || shard_id >= ring->num_shards) return false;
    return ring->alive[shard_id];
}

int shard_ring_locate(shard_ring_t *ring, const char *key)
{
    int target;
    int n = find_n_shards(ring, key, &target, 1);
    return n > 0 ? target : -1;
}

int shard_ring_locate_replicas(shard_ring_t *ring, const char *key,
                               int *out, int max_out)
{
    int n = ring->replica_count;
    if (n > max_out) n = max_out;
    return find_n_shards(ring, key, out, n);
}

int shard_ring_num_shards(shard_ring_t *ring)
{
    return ring->num_shards;
}

int shard_ring_replica_count(shard_ring_t *ring)
{
    return ring->replica_count;
}

int shard_ring_vnodes_per_shard(shard_ring_t *ring)
{
    return ring->vnodes_per_shard;
}

size_t shard_ring_shard_size(shard_ring_t *ring, int shard_id)
{
    if (shard_id < 0 || shard_id >= ring->num_shards) return 0;
    if (!ring->shards[shard_id]) return 0;
    return kv_cache_size(ring->shards[shard_id]);
}
