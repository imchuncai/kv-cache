/*
 * Benchmark: in-process reads vs TCP reads (localhost).
 *
 * Spawns a node_server process, pre-fills it, then measures:
 *   1. In-process reads (baseline)
 *   2. TCP reads to localhost (same machine)
 *   3. TCP reads with pipelining (batch N commands per syscall)
 *
 * This demonstrates why Redis is network-bound, not CPU-bound.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include "kv_cache.h"

#define NUM_KEYS     100000
#define NUM_OPS      100000
#define CACHE_CAP    100000
#define VAL_LEN      64
#define SERVER_PORT  9876
#define PIPELINE_SZ  100       /* commands per batch in pipelined mode */

#define BUF_SIZE     (1 << 20) /* 1 MB */

/* ── Helpers ───────────────────────────────────────────────────────────── */

static double elapsed_ms(struct timespec start, struct timespec end)
{
    return (end.tv_sec - start.tv_sec) * 1e3
         + (end.tv_nsec - start.tv_nsec) / 1e6;
}

static int connect_to_server(int port)
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

/* Send a command and read one line of response */
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

/* ── Benchmark 1: In-process reads ─────────────────────────────────────── */

static void bench_in_process(void)
{
    kv_cache_t *cache = kv_cache_create(CACHE_CAP);

    for (int i = 0; i < NUM_KEYS; i++)
        kv_cache_put(cache, gen_keys[i], gen_vals[i]);

    struct timespec t0, t1;
    size_t hits = 0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < NUM_OPS; i++) {
        if (kv_cache_peek(cache, gen_keys[i % NUM_KEYS]))
            hits++;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);

    printf("  time       : %.2f ms\n", ms);
    printf("  throughput : %.0f ops/sec\n", NUM_OPS / (ms / 1e3));
    printf("  avg latency: %.0f ns/op\n", ms * 1e6 / NUM_OPS);
    printf("  hits       : %zu / %d\n", hits, NUM_OPS);

    kv_cache_destroy(cache);
}

/* ── Benchmark 2: TCP reads (one request at a time) ────────────────────── */

static void bench_tcp_sequential(int fd)
{
    char cmd[128], resp[BUF_SIZE];
    struct timespec t0, t1;
    size_t hits = 0;

    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < NUM_OPS; i++) {
        int len = snprintf(cmd, sizeof(cmd), "GET %s\n", gen_keys[i % NUM_KEYS]);
        send_recv(fd, cmd, len, resp, sizeof(resp));
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

/* ── Benchmark 3: TCP reads with pipelining ────────────────────────────── */

static void bench_tcp_pipelined(int fd)
{
    char *cmd_buf = malloc(BUF_SIZE);
    char *resp_buf = malloc(BUF_SIZE);
    struct timespec t0, t1;
    size_t hits = 0;
    int ops_done = 0;

    clock_gettime(CLOCK_MONOTONIC, &t0);

    while (ops_done < NUM_OPS) {
        int batch = PIPELINE_SZ;
        if (ops_done + batch > NUM_OPS)
            batch = NUM_OPS - ops_done;

        /* Build batch of commands */
        int cmd_len = 0;
        for (int i = 0; i < batch; i++) {
            cmd_len += snprintf(cmd_buf + cmd_len, BUF_SIZE - cmd_len,
                               "GET %s\n", gen_keys[(ops_done + i) % NUM_KEYS]);
        }

        /* Send all at once */
        write(fd, cmd_buf, cmd_len);

        /* Read all responses */
        int total_read = 0;
        int responses = 0;
        while (responses < batch) {
            int n = read(fd, resp_buf + total_read, BUF_SIZE - total_read - 1);
            if (n <= 0) break;
            total_read += n;
            resp_buf[total_read] = '\0';

            /* Count newlines = responses */
            for (int j = total_read - n; j < total_read; j++) {
                if (resp_buf[j] == '\n') {
                    responses++;
                }
            }
        }

        /* Count hits */
        char *p = resp_buf;
        for (int i = 0; i < batch; i++) {
            if (*p == '+') hits++;
            p = strchr(p, '\n');
            if (p) p++;
        }

        ops_done += batch;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);

    printf("  pipeline   : %d cmds/batch\n", PIPELINE_SZ);
    printf("  time       : %.2f ms\n", ms);
    printf("  throughput : %.0f ops/sec\n", NUM_OPS / (ms / 1e3));
    printf("  avg latency: %.0f ns/op\n", ms * 1e6 / NUM_OPS);
    printf("  hits       : %zu / %d\n", hits, NUM_OPS);

    free(cmd_buf);
    free(resp_buf);
}

/* ── Pre-fill server over TCP ──────────────────────────────────────────── */

static void prefill_server(int fd)
{
    char cmd[256], resp[128];
    for (int i = 0; i < NUM_KEYS; i++) {
        int len = snprintf(cmd, sizeof(cmd), "PUT %s %s\n", gen_keys[i], gen_vals[i]);
        send_recv(fd, cmd, len, resp, sizeof(resp));
    }
}

/* ── Main ──────────────────────────────────────────────────────────────── */

int main(void)
{
    printf("=== Network vs In-Process Benchmark ===\n");
    printf("  keys: %d, ops: %d, val_size: %d bytes\n\n", NUM_KEYS, NUM_OPS, VAL_LEN);

    generate_data();

    /* ── In-process baseline ──────────────────────────────────────── */
    printf("[1] In-process reads (no network, no locks)\n");
    bench_in_process();

    /* ── Start server ─────────────────────────────────────────────── */
    printf("\nStarting node server on port %d...\n", SERVER_PORT);
    pid_t server_pid = fork();
    if (server_pid == 0) {
        /* Child: exec the server */
        char port_str[16], cap_str[16];
        snprintf(port_str, sizeof(port_str), "%d", SERVER_PORT);
        snprintf(cap_str, sizeof(cap_str), "%d", CACHE_CAP);
        execl("./node_server", "node_server", port_str, cap_str, NULL);
        perror("execl");
        _exit(1);
    }

    /* Give server time to start */
    usleep(500000); /* 500ms */

    /* Connect and pre-fill */
    int fd = connect_to_server(SERVER_PORT);
    if (fd < 0) {
        fprintf(stderr, "Cannot connect to server\n");
        kill(server_pid, SIGTERM);
        return 1;
    }

    printf("Pre-filling server with %d keys...\n", NUM_KEYS);
    prefill_server(fd);
    close(fd);

    /* ── TCP sequential reads ─────────────────────────────────────── */
    printf("\n[2] TCP sequential reads (localhost, 1 req at a time)\n");
    fd = connect_to_server(SERVER_PORT);
    bench_tcp_sequential(fd);
    close(fd);

    /* ── TCP pipelined reads ──────────────────────────────────────── */
    printf("\n[3] TCP pipelined reads (localhost, %d cmds/batch)\n", PIPELINE_SZ);
    fd = connect_to_server(SERVER_PORT);
    bench_tcp_pipelined(fd);

    /* Shutdown server */
    write(fd, "SHUTDOWN\n", 9);
    close(fd);

    int status;
    waitpid(server_pid, &status, 0);

    /* ── Summary ──────────────────────────────────────────────────── */
    printf("\n[Summary]\n");
    printf("  In-process reads avoid syscalls (read/write), TCP overhead,\n");
    printf("  and serialization. The gap shows why Redis is network-bound:\n");
    printf("  the data operation takes ~25ns, but a TCP round-trip on\n");
    printf("  localhost takes ~10-50us (400-2000x slower).\n");
    printf("  Pipelining amortizes the round-trip cost across N commands.\n");

    free_data();
    return 0;
}
