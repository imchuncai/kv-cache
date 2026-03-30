#ifndef SHARD_H
#define SHARD_H

#include <stddef.h>
#include <stdbool.h>
#include "kv_cache.h"

#define MAX_SHARDS       16
#define MAX_REPLICAS     5    /* max replication factor */

typedef struct shard_ring shard_ring_t;

/*
 * Create a shard ring.
 *   num_shards         - number of physical shards
 *   capacity_per_shard - LRU cache capacity per shard
 *   vnodes_per_shard   - virtual nodes per shard (higher = better balance)
 *   replica_count      - how many shards each key is written to (1 = no replication)
 */
shard_ring_t *shard_ring_create(int num_shards, size_t capacity_per_shard,
                                int vnodes_per_shard, int replica_count);

void shard_ring_destroy(shard_ring_t *ring);

/* Put a key-value pair (writes to `replica_count` shards). */
bool shard_ring_put(shard_ring_t *ring, const char *key, const char *value);

/* Get a value by key (tries primary, then replicas). */
const char *shard_ring_get(shard_ring_t *ring, const char *key);

/* Mark a shard as down (data lost). */
int shard_ring_down(shard_ring_t *ring, int shard_id);

/* Bring a shard back up (empty). */
int shard_ring_up(shard_ring_t *ring, int shard_id);

bool shard_ring_is_alive(shard_ring_t *ring, int shard_id);

/* Get the primary shard for a key. */
int shard_ring_locate(shard_ring_t *ring, const char *key);

/*
 * Get all replica shards for a key (fills `out`, returns count).
 * Includes the primary as out[0].
 */
int shard_ring_locate_replicas(shard_ring_t *ring, const char *key,
                               int *out, int max_out);

int shard_ring_num_shards(shard_ring_t *ring);
int shard_ring_replica_count(shard_ring_t *ring);
int shard_ring_vnodes_per_shard(shard_ring_t *ring);
size_t shard_ring_shard_size(shard_ring_t *ring, int shard_id);

#endif /* SHARD_H */
