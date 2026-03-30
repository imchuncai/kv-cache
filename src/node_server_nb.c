/*
 * Non-blocking KV cache server using kqueue (macOS).
 * Single-threaded event loop, handles many clients concurrently.
 *
 * Usage: ./node_server_nb <port> <capacity>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/event.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include "kv_cache.h"

#define BACKLOG      128
#define MAX_EVENTS   128
#define READ_BUF     65536
#define WRITE_BUF    (1 << 20)

/* ── Per-client state ──────────────────────────────────────────────────── */

typedef struct client {
    int     fd;
    char   *rbuf;          /* read buffer */
    int     rlen;
    int     rcap;
    char   *wbuf;          /* write buffer */
    int     wlen;
    int     wpos;          /* how much already written */
    int     wcap;
} client_t;

static client_t *clients[65536]; /* indexed by fd */

static client_t *client_new(int fd)
{
    client_t *c = calloc(1, sizeof(*c));
    c->fd   = fd;
    c->rcap = READ_BUF;
    c->rbuf = malloc(c->rcap);
    c->wcap = WRITE_BUF;
    c->wbuf = malloc(c->wcap);
    return c;
}

static void client_free(client_t *c)
{
    close(c->fd);
    free(c->rbuf);
    free(c->wbuf);
    free(c);
}

static void client_append_response(client_t *c, const char *resp, int len)
{
    /* Grow write buffer if needed */
    while (c->wlen + len > c->wcap) {
        c->wcap *= 2;
        c->wbuf = realloc(c->wbuf, c->wcap);
    }
    memcpy(c->wbuf + c->wlen, resp, len);
    c->wlen += len;
}

/* ── Set non-blocking ──────────────────────────────────────────────────── */

static void set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* ── Process commands in read buffer ───────────────────────────────────── */

static volatile int running = 1;

static void process_commands(client_t *c, kv_cache_t *cache)
{
    char *buf = c->rbuf;
    int   pos = 0;

    while (pos < c->rlen) {
        char *newline = memchr(buf + pos, '\n', c->rlen - pos);
        if (!newline) break; /* incomplete line */

        *newline = '\0';
        char *line = buf + pos;
        int line_len = (int)(newline - line);
        pos += line_len + 1;

        if (line_len >= 4 && strncmp(line, "PUT ", 4) == 0) {
            char *key_start = line + 4;
            char *space = strchr(key_start, ' ');
            if (space) {
                *space = '\0';
                if (kv_cache_put(cache, key_start, space + 1))
                    client_append_response(c, "+OK\n", 4);
                else
                    client_append_response(c, "-ERR\n", 5);
            } else {
                client_append_response(c, "-ERR\n", 5);
            }
        } else if (line_len >= 4 && strncmp(line, "GET ", 4) == 0) {
            const char *key = line + 4;
            const char *val = kv_cache_peek(cache, key);
            if (val) {
                char resp[READ_BUF];
                int rlen = snprintf(resp, sizeof(resp), "+%s\n", val);
                client_append_response(c, resp, rlen);
            } else {
                client_append_response(c, "$NULL\n", 6);
            }
        } else if (line_len >= 4 && strncmp(line, "QUIT", 4) == 0) {
            client_append_response(c, "+BYE\n", 5);
        } else if (line_len >= 8 && strncmp(line, "SHUTDOWN", 8) == 0) {
            client_append_response(c, "+BYE\n", 5);
            running = 0;
        }
    }

    /* Compact read buffer */
    if (pos > 0) {
        c->rlen -= pos;
        if (c->rlen > 0)
            memmove(c->rbuf, c->rbuf + pos, c->rlen);
    }
}

/* ── Main event loop ───────────────────────────────────────────────────── */

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
    if (!cache) { fprintf(stderr, "Failed to create cache\n"); return 1; }

    /* Create listening socket */
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    set_nonblocking(server_fd);

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
        .sin_port = htons(port),
    };

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    listen(server_fd, BACKLOG);

    /* Create kqueue */
    int kq = kqueue();
    struct kevent ev;
    EV_SET(&ev, server_fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
    kevent(kq, &ev, 1, NULL, 0, NULL);

    fprintf(stderr, "Non-blocking server on 127.0.0.1:%d (capacity=%d)\n", port, capacity);

    struct kevent events[MAX_EVENTS];

    while (running) {
        struct timespec timeout = {0, 100000000}; /* 100ms */
        int nev = kevent(kq, NULL, 0, events, MAX_EVENTS, &timeout);

        for (int i = 0; i < nev; i++) {
            int fd = (int)events[i].ident;

            if (fd == server_fd) {
                /* Accept new connections */
                while (1) {
                    struct sockaddr_in client_addr;
                    socklen_t client_len = sizeof(client_addr);
                    int cfd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
                    if (cfd < 0) break;

                    set_nonblocking(cfd);
                    int flag = 1;
                    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

                    clients[cfd] = client_new(cfd);

                    EV_SET(&ev, cfd, EVFILT_READ, EV_ADD, 0, 0, NULL);
                    kevent(kq, &ev, 1, NULL, 0, NULL);
                }
            } else if (events[i].filter == EVFILT_READ) {
                client_t *c = clients[fd];
                if (!c) continue;

                /* Read available data */
                while (1) {
                    if (c->rlen >= c->rcap) {
                        c->rcap *= 2;
                        c->rbuf = realloc(c->rbuf, c->rcap);
                    }
                    ssize_t n = read(fd, c->rbuf + c->rlen, c->rcap - c->rlen);
                    if (n > 0) {
                        c->rlen += n;
                    } else if (n == 0) {
                        /* Client disconnected */
                        EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
                        kevent(kq, &ev, 1, NULL, 0, NULL);
                        clients[fd] = NULL;
                        client_free(c);
                        goto next_event;
                    } else {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break;
                        /* Error */
                        clients[fd] = NULL;
                        client_free(c);
                        goto next_event;
                    }
                }

                /* Process complete commands */
                process_commands(c, cache);

                /* If there's data to write, try writing */
                if (c->wlen > c->wpos) {
                    while (c->wpos < c->wlen) {
                        ssize_t n = write(fd, c->wbuf + c->wpos, c->wlen - c->wpos);
                        if (n > 0) {
                            c->wpos += n;
                        } else {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                /* Register for write events */
                                EV_SET(&ev, fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, NULL);
                                kevent(kq, &ev, 1, NULL, 0, NULL);
                            }
                            break;
                        }
                    }
                    /* Reset write buffer if fully flushed */
                    if (c->wpos >= c->wlen) {
                        c->wpos = 0;
                        c->wlen = 0;
                    }
                }
            } else if (events[i].filter == EVFILT_WRITE) {
                client_t *c = clients[fd];
                if (!c) continue;

                while (c->wpos < c->wlen) {
                    ssize_t n = write(fd, c->wbuf + c->wpos, c->wlen - c->wpos);
                    if (n > 0) {
                        c->wpos += n;
                    } else {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            EV_SET(&ev, fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, NULL);
                            kevent(kq, &ev, 1, NULL, 0, NULL);
                        }
                        break;
                    }
                }
                if (c->wpos >= c->wlen) {
                    c->wpos = 0;
                    c->wlen = 0;
                }
            }
            next_event:;
        }
    }

    /* Cleanup remaining clients */
    for (int i = 0; i < 65536; i++) {
        if (clients[i]) {
            client_free(clients[i]);
            clients[i] = NULL;
        }
    }

    close(server_fd);
    close(kq);
    kv_cache_destroy(cache);
    fprintf(stderr, "Non-blocking server shut down.\n");
    return 0;
}
