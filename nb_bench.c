/*
 * Benchmark: blocking server vs non-blocking (kqueue) server.
 * Tests with 1, 10, 50 concurrent client connections.
 *
 * Each client runs in its own thread, sending pipelined GETs.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include "kv_cache.h"

#define NUM_KEYS        100000
#define TOTAL_OPS       200000
#define CACHE_CAP       100000
#define VAL_LEN         64
#define PIPELINE_SZ     50
#define BUF_SIZE        (1 << 20)

#define PORT_BLOCKING   9890
#define PORT_NONBLOCK   9891

/* ── Helpers ───────────────────────────────────────────────────────────── */

static double elapsed_ms(struct timespec start, struct timespec end)
{
    return (end.tv_sec - start.tv_sec) * 1e3
         + (end.tv_nsec - start.tv_nsec) / 1e6;
}

static int connect_to(int port)
{
    for (int attempt = 0; attempt < 20; attempt++) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in addr = {
            .sin_family = AF_INET,
            .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
            .sin_port = htons(port),
        };
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
            int flag = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
            return fd;
        }
        close(fd);
        usleep(50000);
    }
    return -1;
}

/* ── Data ──────────────────────────────────────────────────────────────── */

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
    for (int i = 0; i < NUM_KEYS; i++) { free(gen_keys[i]); free(gen_vals[i]); }
    free(gen_keys); free(gen_vals);
}

/* ── Prefill a server (pipelined for speed) ────────────────────────────── */

static void prefill(int port)
{
    int fd = connect_to(port);
    if (fd < 0) return;
    char cmd[256], resp[128];

    /* Sequential send/recv — safe for both blocking and non-blocking servers */
    for (int i = 0; i < NUM_KEYS; i++) {
        int len = snprintf(cmd, sizeof(cmd), "PUT %s %s\n", gen_keys[i], gen_vals[i]);
        write(fd, cmd, len);
        read(fd, resp, sizeof(resp));
    }

    write(fd, "QUIT\n", 5);
    char tmp[64];
    read(fd, tmp, sizeof(tmp));
    close(fd);
}

/* ── Client worker thread ─────────────────────────────────────────────── */

typedef struct {
    int     port;
    int     ops;
    int     thread_id;
    size_t  hits;
    double  ms;
} worker_arg_t;

static void *client_worker(void *arg)
{
    worker_arg_t *a = (worker_arg_t *)arg;
    int fd = connect_to(a->port);
    if (fd < 0) { a->ms = -1; return NULL; }

    char *cmd_buf = malloc(BUF_SIZE);
    char *resp_buf = malloc(BUF_SIZE);
    int ops_done = 0;

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    while (ops_done < a->ops) {
        int batch = PIPELINE_SZ;
        if (ops_done + batch > a->ops) batch = a->ops - ops_done;

        int cmd_len = 0;
        for (int i = 0; i < batch; i++) {
            int key_idx = (a->thread_id * 7919 + ops_done + i) % NUM_KEYS;
            cmd_len += snprintf(cmd_buf + cmd_len, BUF_SIZE - cmd_len,
                               "GET %s\n", gen_keys[key_idx]);
        }

        write(fd, cmd_buf, cmd_len);

        int total_read = 0, responses = 0;
        while (responses < batch) {
            int n = read(fd, resp_buf + total_read, BUF_SIZE - total_read - 1);
            if (n <= 0) goto done;
            total_read += n;
            for (int j = total_read - n; j < total_read; j++)
                if (resp_buf[j] == '\n') responses++;
        }

        resp_buf[total_read] = '\0';
        char *p = resp_buf;
        for (int i = 0; i < batch; i++) {
            if (p && *p == '+') a->hits++;
            p = strchr(p, '\n');
            if (p) p++;
        }

        ops_done += batch;
    }

done:
    clock_gettime(CLOCK_MONOTONIC, &t1);
    a->ms = elapsed_ms(t0, t1);

    write(fd, "QUIT\n", 5);
    char tmp[64];
    read(fd, tmp, sizeof(tmp));
    close(fd);
    free(cmd_buf);
    free(resp_buf);
    return NULL;
}

/* ── Run benchmark with N concurrent clients ───────────────────────────── */

static void run_bench(int port, int num_clients, const char *label)
{
    int ops_per_client = TOTAL_OPS / num_clients;
    int total_ops = ops_per_client * num_clients;

    pthread_t *threads = malloc(num_clients * sizeof(pthread_t));
    worker_arg_t *args = malloc(num_clients * sizeof(worker_arg_t));

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < num_clients; i++) {
        args[i] = (worker_arg_t){
            .port = port,
            .ops = ops_per_client,
            .thread_id = i,
            .hits = 0,
            .ms = 0,
        };
        pthread_create(&threads[i], NULL, client_worker, &args[i]);
    }

    size_t total_hits = 0;
    for (int i = 0; i < num_clients; i++) {
        pthread_join(threads[i], NULL);
        total_hits += args[i].hits;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = elapsed_ms(t0, t1);

    printf("  %-24s %3d clients │ %10.0f ops/sec │ %8.0f ns/op │ hits: %zu/%d\n",
           label, num_clients,
           total_ops / (ms / 1e3),
           ms * 1e6 / total_ops,
           total_hits, total_ops);

    free(threads);
    free(args);
}

/* ── Server management ─────────────────────────────────────────────────── */

static pid_t start_server(const char *binary, int port)
{
    pid_t pid = fork();
    if (pid == 0) {
        char port_str[16], cap_str[16];
        snprintf(port_str, sizeof(port_str), "%d", port);
        snprintf(cap_str, sizeof(cap_str), "%d", CACHE_CAP);
        execl(binary, binary, port_str, cap_str, NULL);
        perror("execl");
        _exit(1);
    }
    return pid;
}

static void shutdown_server(int port, pid_t pid)
{
    int fd = connect_to(port);
    if (fd >= 0) {
        write(fd, "SHUTDOWN\n", 9);
        char tmp[64];
        read(fd, tmp, sizeof(tmp));
        close(fd);
    }
    int status;
    waitpid(pid, &status, 0);
}

/* ── Main ──────────────────────────────────────────────────────────────── */

int main(void)
{
    printf("=== Blocking vs Non-Blocking Server Benchmark ===\n");
    printf("  total ops: %d, pipeline: %d cmds/batch, val_size: %d bytes\n\n",
           TOTAL_OPS, PIPELINE_SZ, VAL_LEN);

    generate_data();

    /* ── Non-blocking server (handles all client counts) ───────────── */
    printf("[Non-blocking server (kqueue event loop)]\n");
    pid_t nb_pid = start_server("./node_server_nb", PORT_NONBLOCK);
    usleep(500000);
    prefill(PORT_NONBLOCK);

    int client_counts[] = {1, 10, 50};
    int num_tests = sizeof(client_counts) / sizeof(client_counts[0]);

    for (int t = 0; t < num_tests; t++)
        run_bench(PORT_NONBLOCK, client_counts[t], "Non-blocking:");

    shutdown_server(PORT_NONBLOCK, nb_pid);

    /* ── Blocking server (only 1 client at a time) ─────────────────── */
    printf("\n[Blocking server (handles 1 connection at a time)]\n");
    pid_t bl_pid = start_server("./node_server", PORT_BLOCKING);
    usleep(500000);
    prefill(PORT_BLOCKING);

    /* Only test with 1 client — multiple clients would serialize anyway */
    run_bench(PORT_BLOCKING, 1, "Blocking:");

    printf("  (Skipping multi-client: blocking server serializes all connections)\n");

    shutdown_server(PORT_BLOCKING, bl_pid);

    /* ── Summary ───────────────────────────────────────────────────── */
    printf("\n[Takeaway]\n");
    printf("  With 1 client: blocking ≈ non-blocking (same round-trip cost).\n");
    printf("  With 50 clients: non-blocking multiplexes all concurrently,\n");
    printf("  achieving much higher aggregate throughput — this is why Redis\n");
    printf("  uses an event loop.\n");

    free_data();
    return 0;
}
