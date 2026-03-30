#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "shard.h"

#define NUM_KEYS    100000
#define NUM_SHARDS  5
#define SHARD_CAP   50000

/* ── Helpers ───────────────────────────────────────────────────────────── */

static void print_distribution(shard_ring_t *ring, const char *label)
{
    int n = shard_ring_num_shards(ring);
    printf("  %-32s", label);
    size_t total = 0;
    for (int i = 0; i < n; i++) {
        size_t sz = shard_ring_shard_size(ring, i);
        total += sz;
        if (shard_ring_is_alive(ring, i))
            printf("  S%d: %5zu", i, sz);
        else
            printf("  S%d:  DOWN", i);
    }
    printf("  | total: %zu\n", total);
}

static double std_dev(shard_ring_t *ring, int num_keys)
{
    int n = shard_ring_num_shards(ring);
    double mean = (double)num_keys / n;
    double sum_sq = 0;
    for (int i = 0; i < n; i++) {
        double diff = (double)shard_ring_shard_size(ring, i) - mean;
        sum_sq += diff * diff;
    }
    return sqrt(sum_sq / n);
}

static char (*generate_keys(int count))[32]
{
    char (*keys)[32] = malloc(count * sizeof(*keys));
    for (int i = 0; i < count; i++)
        snprintf(keys[i], 32, "user:%d:session", i);
    return keys;
}

/* ── Part 1: Vnode tuning for balance ──────────────────────────────────── */

static void test_vnode_balance(char keys[][32])
{
    printf("╔══════════════════════════════════════════════════════════╗\n");
    printf("║  Part 1: Vnode Count vs Distribution Balance            ║\n");
    printf("╚══════════════════════════════════════════════════════════╝\n\n");

    int vnode_counts[] = {10, 50, 150, 500, 1000};
    int num_tests = sizeof(vnode_counts) / sizeof(vnode_counts[0]);
    double ideal = (double)NUM_KEYS / NUM_SHARDS;

    printf("  %d keys, %d shards, ideal = %.0f keys/shard\n\n", NUM_KEYS, NUM_SHARDS, ideal);
    printf("  %-12s  %-40s  %-10s  %-10s\n", "vnodes/shard", "distribution", "std dev", "max skew");
    printf("  %-12s  %-40s  %-10s  %-10s\n", "────────────", "────────────────────────────────────────", "──────────", "──────────");

    for (int t = 0; t < num_tests; t++) {
        int vn = vnode_counts[t];
        shard_ring_t *ring = shard_ring_create(NUM_SHARDS, SHARD_CAP, vn, 1);

        for (int i = 0; i < NUM_KEYS; i++) {
            char val[16];
            snprintf(val, sizeof(val), "%d", i);
            shard_ring_put(ring, keys[i], val);
        }

        /* Collect sizes */
        char dist[128] = "";
        size_t max_size = 0, min_size = NUM_KEYS;
        for (int s = 0; s < NUM_SHARDS; s++) {
            size_t sz = shard_ring_shard_size(ring, s);
            if (sz > max_size) max_size = sz;
            if (sz < min_size) min_size = sz;
            char buf[24];
            snprintf(buf, sizeof(buf), "%zu ", sz);
            strcat(dist, buf);
        }

        double sd = std_dev(ring, NUM_KEYS);
        double max_skew = 100.0 * ((double)max_size - ideal) / ideal;

        printf("  %-12d  %-40s  %-10.0f  %+.1f%%\n", vn, dist, sd, max_skew);

        shard_ring_destroy(ring);
    }
}

/* ── Part 2: Replication vs no-replication on shard failure ────────────── */

static void test_replication(char keys[][32])
{
    printf("\n╔══════════════════════════════════════════════════════════╗\n");
    printf("║  Part 2: Replication Factor vs Data Survival            ║\n");
    printf("╚══════════════════════════════════════════════════════════╝\n\n");

    int replica_counts[] = {1, 2, 3};
    int num_tests = sizeof(replica_counts) / sizeof(replica_counts[0]);
    int vnodes = 500; /* good balance from Part 1 */

    for (int t = 0; t < num_tests; t++) {
        int rf = replica_counts[t];
        printf("  ── Replica factor = %d (vnodes = %d) ──────────────────────\n", rf, vnodes);

        shard_ring_t *ring = shard_ring_create(NUM_SHARDS, SHARD_CAP, vnodes, rf);

        /* Insert all keys */
        for (int i = 0; i < NUM_KEYS; i++) {
            char val[16];
            snprintf(val, sizeof(val), "%d", i);
            shard_ring_put(ring, keys[i], val);
        }

        print_distribution(ring, "All shards up:");

        /* Verify all keys readable */
        int readable_before = 0;
        for (int i = 0; i < NUM_KEYS; i++)
            if (shard_ring_get(ring, keys[i]))
                readable_before++;

        printf("  Keys readable: %d / %d\n", readable_before, NUM_KEYS);

        /* Kill shard 2 */
        shard_ring_down(ring, 2);
        printf("\n  Shard 2 goes DOWN.\n");

        int readable_after = 0;
        for (int i = 0; i < NUM_KEYS; i++)
            if (shard_ring_get(ring, keys[i]))
                readable_after++;

        int lost = readable_before - readable_after;
        printf("  Keys readable: %d / %d  (lost: %d, %.1f%%)\n",
               readable_after, NUM_KEYS, lost, 100.0 * lost / NUM_KEYS);

        /* Kill shard 4 too */
        shard_ring_down(ring, 4);
        printf("\n  Shard 4 also goes DOWN (2 shards down).\n");

        int readable_2down = 0;
        for (int i = 0; i < NUM_KEYS; i++)
            if (shard_ring_get(ring, keys[i]))
                readable_2down++;

        lost = readable_before - readable_2down;
        printf("  Keys readable: %d / %d  (lost: %d, %.1f%%)\n",
               readable_2down, NUM_KEYS, lost, 100.0 * lost / NUM_KEYS);

        /* Bring shard 2 back */
        shard_ring_up(ring, 2);
        printf("\n  Shard 2 comes back UP (empty).\n");

        int readable_recover = 0;
        for (int i = 0; i < NUM_KEYS; i++)
            if (shard_ring_get(ring, keys[i]))
                readable_recover++;

        lost = readable_before - readable_recover;
        printf("  Keys readable: %d / %d  (lost: %d, %.1f%%)\n",
               readable_recover, NUM_KEYS, lost, 100.0 * lost / NUM_KEYS);

        printf("\n");
        shard_ring_destroy(ring);
    }
}

/* ── Part 3: Key movement on topology change ───────────────────────────── */

static void test_key_movement(char keys[][32])
{
    printf("╔══════════════════════════════════════════════════════════╗\n");
    printf("║  Part 3: Key Redistribution on Topology Changes         ║\n");
    printf("╚══════════════════════════════════════════════════════════╝\n\n");

    int vnodes = 500;
    shard_ring_t *ring = shard_ring_create(NUM_SHARDS, SHARD_CAP, vnodes, 1);

    /* Insert keys */
    for (int i = 0; i < NUM_KEYS; i++) {
        char val[16];
        snprintf(val, sizeof(val), "%d", i);
        shard_ring_put(ring, keys[i], val);
    }

    /* Record initial mapping */
    int *map_before = malloc(NUM_KEYS * sizeof(int));
    int *map_after  = malloc(NUM_KEYS * sizeof(int));
    for (int i = 0; i < NUM_KEYS; i++)
        map_before[i] = shard_ring_locate(ring, keys[i]);

    print_distribution(ring, "Initial (5 shards):");

    /* Take shard 2 down */
    shard_ring_down(ring, 2);
    printf("\n  Shard 2 DOWN:\n");
    for (int i = 0; i < NUM_KEYS; i++)
        map_after[i] = shard_ring_locate(ring, keys[i]);

    int moved = 0;
    int remap_dest[MAX_SHARDS] = {0};
    for (int i = 0; i < NUM_KEYS; i++) {
        if (map_before[i] != map_after[i]) {
            moved++;
            remap_dest[map_after[i]]++;
        }
    }
    double ideal_moved = 100.0 / NUM_SHARDS; /* with consistent hashing, ~1/N should move */
    printf("  Keys remapped : %d / %d (%.1f%%)  [ideal: ~%.0f%%]\n",
           moved, NUM_KEYS, 100.0 * moved / NUM_KEYS, ideal_moved);
    printf("  Remap targets :");
    for (int i = 0; i < NUM_SHARDS; i++)
        if (remap_dest[i] > 0)
            printf("  S%d: +%d", i, remap_dest[i]);
    printf("\n");

    /* Bring shard 2 back */
    memcpy(map_before, map_after, NUM_KEYS * sizeof(int));
    shard_ring_up(ring, 2);
    printf("\n  Shard 2 back UP:\n");
    for (int i = 0; i < NUM_KEYS; i++)
        map_after[i] = shard_ring_locate(ring, keys[i]);

    moved = 0;
    memset(remap_dest, 0, sizeof(remap_dest));
    int left_from[MAX_SHARDS] = {0};
    for (int i = 0; i < NUM_KEYS; i++) {
        if (map_before[i] != map_after[i]) {
            moved++;
            remap_dest[map_after[i]]++;
            left_from[map_before[i]]++;
        }
    }
    printf("  Keys remapped : %d / %d (%.1f%%)\n",
           moved, NUM_KEYS, 100.0 * moved / NUM_KEYS);
    printf("  Moving TO S2  : %d\n", remap_dest[2]);
    printf("  Leaving from  :");
    for (int i = 0; i < NUM_SHARDS; i++)
        if (left_from[i] > 0)
            printf("  S%d: -%d", i, left_from[i]);
    printf("\n");

    free(map_before);
    free(map_after);
    shard_ring_destroy(ring);
}

/* ── Main ──────────────────────────────────────────────────────────────── */

int main(void)
{
    printf("=== Consistent Hashing + Replication Simulation ===\n");
    printf("  shards: %d, keys: %d\n\n", NUM_SHARDS, NUM_KEYS);

    char (*keys)[32] = generate_keys(NUM_KEYS);

    test_vnode_balance(keys);
    printf("\n");
    test_replication(keys);
    test_key_movement(keys);

    printf("\n=== CAP Theorem Summary ===\n");
    printf("  RF=1 (no replication): AP — available, partition tolerant, but data lost on crash\n");
    printf("  RF=2: tolerates 1 shard failure with zero data loss\n");
    printf("  RF=3: tolerates 2 simultaneous failures with zero data loss\n");
    printf("  Tradeoff: higher RF = more write amplification (each put hits RF shards)\n");

    free(keys);
    return 0;
}
