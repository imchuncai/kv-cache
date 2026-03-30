CC      = cc
CFLAGS  = -Wall -Wextra -O2 -std=c11

all: test bench shard_sim node_server node_server_nb net_bench dist_bench nb_bench

test: test.c kv_cache.c kv_cache.h
	$(CC) $(CFLAGS) -o $@ test.c kv_cache.c

bench: bench.c kv_cache.c kv_cache.h
	$(CC) $(CFLAGS) -o $@ bench.c kv_cache.c

shard_sim: shard_sim.c shard.c kv_cache.c shard.h kv_cache.h
	$(CC) $(CFLAGS) -o $@ shard_sim.c shard.c kv_cache.c

node_server: node_server.c kv_cache.c kv_cache.h
	$(CC) $(CFLAGS) -o $@ node_server.c kv_cache.c

net_bench: net_bench.c kv_cache.c kv_cache.h
	$(CC) $(CFLAGS) -o $@ net_bench.c kv_cache.c

node_server_nb: node_server_nb.c kv_cache.c kv_cache.h
	$(CC) $(CFLAGS) -o $@ node_server_nb.c kv_cache.c

dist_bench: dist_bench.c shard.c kv_cache.c shard.h kv_cache.h
	$(CC) $(CFLAGS) -o $@ dist_bench.c shard.c kv_cache.c -lm

nb_bench: nb_bench.c kv_cache.c kv_cache.h
	$(CC) $(CFLAGS) -o $@ nb_bench.c kv_cache.c

clean:
	rm -f test bench shard_sim node_server net_bench dist_bench

.PHONY: all clean
