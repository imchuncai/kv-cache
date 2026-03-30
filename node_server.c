/*
 * Single-threaded KV cache node server (Redis-style event loop).
 * Protocol (line-based):
 *   PUT key value\n  →  +OK\n | -ERR\n
 *   GET key\n        →  +value\n | $NULL\n
 *   QUIT\n           →  +BYE\n (closes connection)
 *   SHUTDOWN\n       →  server exits
 *
 * Usage: ./node_server <port> <capacity>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include "kv_cache.h"

#define BACKLOG     16
#define BUF_SIZE    4096

static volatile int running = 1;

static void handle_connection(int client_fd, kv_cache_t *cache)
{
    char buf[BUF_SIZE];
    ssize_t n;

    while ((n = read(client_fd, buf, sizeof(buf) - 1)) > 0) {
        buf[n] = '\0';

        /* Process each line in the buffer */
        char *line = buf;
        char *newline;
        while ((newline = strchr(line, '\n')) != NULL) {
            *newline = '\0';

            if (strncmp(line, "PUT ", 4) == 0) {
                /* PUT key value */
                char *key_start = line + 4;
                char *space = strchr(key_start, ' ');
                if (space) {
                    *space = '\0';
                    const char *key = key_start;
                    const char *val = space + 1;
                    if (kv_cache_put(cache, key, val))
                        write(client_fd, "+OK\n", 4);
                    else
                        write(client_fd, "-ERR\n", 5);
                } else {
                    write(client_fd, "-ERR\n", 5);
                }
            } else if (strncmp(line, "GET ", 4) == 0) {
                const char *key = line + 4;
                const char *val = kv_cache_peek(cache, key);
                if (val) {
                    char resp[BUF_SIZE];
                    int len = snprintf(resp, sizeof(resp), "+%s\n", val);
                    write(client_fd, resp, len);
                } else {
                    write(client_fd, "$NULL\n", 6);
                }
            } else if (strncmp(line, "QUIT", 4) == 0) {
                write(client_fd, "+BYE\n", 5);
                return;
            } else if (strncmp(line, "SHUTDOWN", 8) == 0) {
                write(client_fd, "+BYE\n", 5);
                running = 0;
                return;
            }

            line = newline + 1;
        }
    }
}

int main(int argc, char *argv[])
{
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <port> <capacity>\n", argv[0]);
        return 1;
    }

    int port = atoi(argv[1]);
    int capacity = atoi(argv[2]);

    signal(SIGPIPE, SIG_IGN);

    kv_cache_t *cache = kv_cache_create(capacity);
    if (!cache) {
        fprintf(stderr, "Failed to create cache\n");
        return 1;
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
        .sin_port = htons(port),
    };

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }

    listen(server_fd, BACKLOG);
    fprintf(stderr, "Node server listening on 127.0.0.1:%d (capacity=%d)\n", port, capacity);

    while (running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) continue;

        /* Disable Nagle for low latency */
        int flag = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        handle_connection(client_fd, cache);
        close(client_fd);
    }

    close(server_fd);
    kv_cache_destroy(cache);
    fprintf(stderr, "Node server shut down.\n");
    return 0;
}
