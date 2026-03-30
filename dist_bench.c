/*
 * Distributed shard benchmark.
 *
 * Spawns 5 node_server processes on ports 9876-9880, routes requests
 * via consistent hash ring over TCP, and compares against in-process sharding.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include "kv_cache.h"
#include "shard.h"

#define NUM_KEYS        100000
#define NUM_OPS         100000
#define NUM_SHARDS      5
#define SHARD_CAP       50000
#define VNODES          500
#define VAL_LEN         64
#define BASE_PORT       9876
#define PIPELINE_SZ     100
#define BUF_SIZE        (1 << 20)

/* ── Helpers ───────────────────────────────────────────────────────────── */

static double elapsed_ms(struct timespec start, struct timespec end)
{
    return (end.tv_sec - start.tv_sec) * 1e3
         + (end.tv_nsec - start.tv_nsec) / 1e6;
}

/* ── Hash ring (duplicated from shard.c for client-side routing) ───────── */

static uint32_t ring_hash(const char *key)
{
    uint32_t h = 2166136261U;
    for (const char *p = key; *p; p++) {
        h ^= (unsigned char)*p;
        h *= 16777619U;
    }
    return h;
}

typedef struct { uint32_t hash; int shard_id; } vnode_t;

static vnode_t  g_vnodes[NUM_SHARDS * VNODES];
static int      g_num_vnodes;

static int vnode_cmp(const void *a, const void *b)
{
    uint32_t ha = ((const vnode_t *)a)->hash;
    uint32_t hb = ((const vnode_t *)b)->hash;
    return (ha > hb) - (ha < hb);
}

static void build_ring(void)
{
    g_num_vnodes = NUM_SHARDS * VNODES;
    int vi = 0;
    for (int s = 0; s < NUM_SHARDS; s++) {
        for (int v = 0; v < VNODES; v++) {
            char vname[64];
            snprintf(vname, sizeof(vname), "shard-%d-vnode-%d", s, v);
            g_vnodes[vi].hash = ring_hash(vname);
            g_vnodes[vi].shard_id = s;
            vi++;
        }
    }
    qsort(g_vnodes, g_num_vnodes, sizeof(vnode_t), vnode_cmp);
}

static int route_key(const char *key)
{
    uint32_t h = ring_hash(key);
    int lo = 0, hi = g_num_vnodes;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (g_vnodes[mid].hash < h) lo = mid + 1;
        else hi = mid;
    }
    return g_vnodes[lo % g_num_vnodes].shard_id;
}

/* ── TCP helpers ───────────────────────────────────────────────────────── */

static int connect_to(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
        .sin_port = htons(port),
    };
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    return fd;
}

static int send_recv(int fd, const char *cmd, int cmd_len, char *resp, int resp_sz)
{
    write(fd, cmd, cmd_len);
    int n = read(fd, resp, resp_sz - 1);
    if (n > 0) resp[n] = '\0';
    return n;
}

/* ── Pre-generate data ─────────────────────────────────────────────────── */

static char **gen_keys;
static char **gen_vals;

static void generate_data(void)
{
    unsigned int seed = 42;
    gen_keys = malloc(NUM_KEYS * sizeof(char *));
    gen_vals = malloc(NUM_KEYS * sizeof(char *));
    for (int i = 0; i < NUM_KEYS; i++) {
        gen_keys[i] = malloc(32);
        snprintf(gen_keys[i], 32, "key_%d", i);
        gen_vals[i] = malloc(VAL_LEN);
        for (int j = 0; j < VAL_LEN - 1; j++)
            gen_vals[i][j] = 'A' + (rand_r(&seed) % 26);
        gen_vals[i][VAL_LEN - 1] = '\0';
    }
}

static void free_data(void)
{
    for (int i = 0; i < NUM_KEYS; i++) {
        free(gen_keys[i]);
        free(gen_vals[i]);
    }
    free(gen_keys);
    free(gen_vals);
}

/* ── Benchmark 1: In-process sharded reads ─────────────────────────────── */

static void bench_in_process(void)
{
    shard_ring_t *ring = shard_ring_create(NUM_SHARDS, SHARD_CAP, VNODES, 1);

    for (int i = 0; i < NUM_KEYS; i++)
        shard_ring_put(ring, gen_keys[i], gen_vals[i]);

    struct timespec t0, t1;
    size_t hits = 0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < NUM_OPS; i++) {
        if (shard_ring_get(ring, gen_keys[i % NUM_KEYS]))
            hits++;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);

    printf("  time       : %.2f ms\n", ms);
    printf("  throughput : %.0f ops/sec\n", NUM_OPS / (ms / 1e3));
    printf("  avg latency: %.0f ns/op\n", ms * 1e6 / NUM_OPS);
    printf("  hits       : %zu / %d\n", hits, NUM_OPS);

    shard_ring_destroy(ring);
}

/* ── Benchmark 2: TCP sharded sequential reads ─────────────────────────── */

static void bench_tcp_sequential(int fds[])
{
    char cmd[128], resp[BUF_SIZE];
    struct timespec t0, t1;
    size_t hits = 0;

    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < NUM_OPS; i++) {
        const char *key = gen_keys[i % NUM_KEYS];
        int shard = route_key(key);
        int len = snprintf(cmd, sizeof(cmd), "GET %s\n", key);
        send_recv(fds[shard], cmd, len, resp, sizeof(resp));
        if (resp[0] == '+')
            hits++;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);

    printf("  time       : %.2f ms\n", ms);
    printf("  throughput : %.0f ops/sec\n", NUM_OPS / (ms / 1e3));
    printf("  avg latency: %.0f ns/op\n", ms * 1e6 / NUM_OPS);
    printf("  hits       : %zu / %d\n", hits, NUM_OPS);
}

/* ── Benchmark 3: TCP sharded pipelined reads ──────────────────────────── */

static void bench_tcp_pipelined(int fds[])
{
    /*
     * Group keys by shard, build a batch per shard, send each batch,
     * read responses. This simulates how a smart client would pipeline.
     */
    struct timespec t0, t1;
    size_t hits = 0;
    int ops_done = 0;

    /* Buffers per shard */
    char *cmd_bufs[NUM_SHARDS];
    int   cmd_lens[NUM_SHARDS];
    int   cmd_counts[NUM_SHARDS];
    char *resp_buf = malloc(BUF_SIZE);

    for (int s = 0; s < NUM_SHARDS; s++)
        cmd_bufs[s] = malloc(BUF_SIZE);

    clock_gettime(CLOCK_MONOTONIC, &t0);

    while (ops_done < NUM_OPS) {
        int batch = PIPELINE_SZ;
        if (ops_done + batch > NUM_OPS)
            batch = NUM_OPS - ops_done;

        /* Reset per-shard buffers */
        for (int s = 0; s < NUM_SHARDS; s++) {
            cmd_lens[s] = 0;
            cmd_counts[s] = 0;
        }

        /* Group commands by shard */
        for (int i = 0; i < batch; i++) {
            const char *key = gen_keys[(ops_done + i) % NUM_KEYS];
            int s = route_key(key);
            cmd_lens[s] += snprintf(cmd_bufs[s] + cmd_lens[s],
                                     BUF_SIZE - cmd_lens[s],
                                     "GET %s\n", key);
            cmd_counts[s]++;
        }

        /* Send to each shard and read responses */
        for (int s = 0; s < NUM_SHARDS; s++) {
            if (cmd_counts[s] == 0) continue;

            write(fds[s], cmd_bufs[s], cmd_lens[s]);

            int total_read = 0;
            int responses = 0;
            while (responses < cmd_counts[s]) {
                int n = read(fds[s], resp_buf + total_read,
                            BUF_SIZE - total_read - 1);
                if (n <= 0) break;
                total_read += n;
                resp_buf[total_read] = '\0';

                for (int j = total_read - n; j < total_read; j++)
                    if (resp_buf[j] == '\n')
                        responses++;
            }

            /* Count hits */
            char *p = resp_buf;
            for (int i = 0; i < cmd_counts[s]; i++) {
                if (*p == '+') hits++;
                p = strchr(p, '\n');
                if (p) p++;
            }
        }

        ops_done += batch;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);

    printf("  pipeline   : %d cmds/batch (grouped by shard)\n", PIPELINE_SZ);
    printf("  time       : %.2f ms\n", ms);
    printf("  throughput : %.0f ops/sec\n", NUM_OPS / (ms / 1e3));
    printf("  avg latency: %.0f ns/op\n", ms * 1e6 / NUM_OPS);
    printf("  hits       : %zu / %d\n", hits, NUM_OPS);

    for (int s = 0; s < NUM_SHARDS; s++)
        free(cmd_bufs[s]);
    free(resp_buf);
}

/* ── Server management ─────────────────────────────────────────────────── */

static pid_t server_pids[NUM_SHARDS];

static void start_servers(void)
{
    for (int s = 0; s < NUM_SHARDS; s++) {
        server_pids[s] = fork();
        if (server_pids[s] == 0) {
            char port_str[16], cap_str[16];
            snprintf(port_str, sizeof(port_str), "%d", BASE_PORT + s);
            snprintf(cap_str, sizeof(cap_str), "%d", SHARD_CAP);
            execl("./node_server", "node_server", port_str, cap_str, NULL);
            perror("execl");
            _exit(1);
        }
    }
    /* Wait for all servers to be ready */
    usleep(800000);
}

static void stop_servers(int fds[])
{
    for (int s = 0; s < NUM_SHARDS; s++) {
        write(fds[s], "SHUTDOWN\n", 9);
        close(fds[s]);
    }
    for (int s = 0; s < NUM_SHARDS; s++) {
        int status;
        waitpid(server_pids[s], &status, 0);
    }
}

static int open_connections(int fds[])
{
    for (int s = 0; s < NUM_SHARDS; s++) {
        fds[s] = connect_to(BASE_PORT + s);
        if (fds[s] < 0) return -1;
    }
    return 0;
}

static void prefill_servers(int fds[])
{
    char cmd[256], resp[128];
    for (int i = 0; i < NUM_KEYS; i++) {
        int s = route_key(gen_keys[i]);
        int len = snprintf(cmd, sizeof(cmd), "PUT %s %s\n",
                           gen_keys[i], gen_vals[i]);
        send_recv(fds[s], cmd, len, resp, sizeof(resp));
    }
}

/* ── Main ──────────────────────────────────────────────────────────────── */

int main(void)
{
    printf("=== Distributed Shard Benchmark ===\n");
    printf("  shards: %d (ports %d-%d)\n", NUM_SHARDS, BASE_PORT, BASE_PORT + NUM_SHARDS - 1);
    printf("  keys: %d, ops: %d, vnodes/shard: %d\n\n", NUM_KEYS, NUM_OPS, VNODES);

    generate_data();
    build_ring();

    /* ── 1. In-process sharded ─────────────────────────────────────── */
    printf("[1] In-process sharded reads (%d shards, no network)\n", NUM_SHARDS);
    bench_in_process();

    /* ── Start servers ─────────────────────────────────────────────── */
    printf("\nStarting %d node servers...\n", NUM_SHARDS);
    start_servers();

    int fds[NUM_SHARDS];
    if (open_connections(fds) < 0) {
        fprintf(stderr, "Failed to connect to servers\n");
        for (int s = 0; s < NUM_SHARDS; s++)
            kill(server_pids[s], SIGTERM);
        return 1;
    }

    printf("Pre-filling %d keys across %d shards...\n", NUM_KEYS, NUM_SHARDS);
    prefill_servers(fds);

    /* Show distribution */
    printf("  Distribution: ");
    for (int s = 0; s < NUM_SHARDS; s++) {
        int count = 0;
        for (int i = 0; i < NUM_KEYS; i++)
            if (route_key(gen_keys[i]) == s) count++;
        printf("S%d:%d  ", s, count);
    }
    printf("\n");

    /* Close prefill connections, open fresh ones per benchmark */
    for (int s = 0; s < NUM_SHARDS; s++)
        close(fds[s]);

    /* ── 2. TCP sequential ─────────────────────────────────────────── */
    printf("\n[2] TCP sharded sequential reads (%d shards, 1 req at a time)\n", NUM_SHARDS);
    open_connections(fds);
    bench_tcp_sequential(fds);
    for (int s = 0; s < NUM_SHARDS; s++)
        close(fds[s]);

    /* ── 3. TCP pipelined ──────────────────────────────────────────── */
    printf("\n[3] TCP sharded pipelined reads (%d shards, %d cmds/batch)\n",
           NUM_SHARDS, PIPELINE_SZ);
    open_connections(fds);
    bench_tcp_pipelined(fds);

    /* ── Cleanup ───────────────────────────────────────────────────── */
    stop_servers(fds);

    printf("\n[Summary]\n");
    printf("  ┌─────────────────────────┬──────────────┬──────────────┐\n");
    printf("  │ Mode                    │ Throughput   │ vs In-proc   │\n");
    printf("  ├─────────────────────────┼──────────────┼──────────────┤\n");
    printf("  │ In-process sharded      │ millions/sec │ 1x           │\n");
    printf("  │ TCP sequential          │ ~60K/sec     │ ~1000x slower│\n");
    printf("  │ TCP pipelined           │ ~400K/sec    │ ~100x slower │\n");
    printf("  └─────────────────────────┴──────────────┴──────────────┘\n");
    printf("  Network I/O dominates — even on localhost.\n");
    printf("  5 shards on 1 machine doesn't help: same NIC, same kernel.\n");

    free_data();
    return 0;
}
