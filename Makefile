CC      = cc
CFLAGS  = -Wall -Wextra -O2 -std=c11 -Iinclude
LDFLAGS =

CORE    = src/kv_cache.c
SHARD   = src/shard.c

.PHONY: all test clean

all: test bench shard_sim node_server node_server_nb net_bench dist_bench nb_bench

# --- Tests ---

test: build/test
	./build/test

build/test: tests/test.c $(CORE) include/kv_cache.h
	@mkdir -p build
	$(CC) $(CFLAGS) -o $@ tests/test.c $(CORE)

# --- Servers ---

build/node_server: src/node_server.c $(CORE) include/kv_cache.h
	@mkdir -p build
	$(CC) $(CFLAGS) -o $@ src/node_server.c $(CORE)

build/node_server_nb: src/node_server_nb.c $(CORE) include/kv_cache.h
	@mkdir -p build
	$(CC) $(CFLAGS) -o $@ src/node_server_nb.c $(CORE)

node_server: build/node_server
node_server_nb: build/node_server_nb

# --- Benchmarks ---

build/bench: benchmarks/bench.c $(CORE) include/kv_cache.h
	@mkdir -p build
	$(CC) $(CFLAGS) -o $@ benchmarks/bench.c $(CORE)

build/shard_sim: benchmarks/shard_sim.c $(SHARD) $(CORE) include/shard.h include/kv_cache.h
	@mkdir -p build
	$(CC) $(CFLAGS) -o $@ benchmarks/shard_sim.c $(SHARD) $(CORE)

build/net_bench: benchmarks/net_bench.c $(CORE) include/kv_cache.h
	@mkdir -p build
	$(CC) $(CFLAGS) -o $@ benchmarks/net_bench.c $(CORE)

build/nb_bench: benchmarks/nb_bench.c $(CORE) include/kv_cache.h
	@mkdir -p build
	$(CC) $(CFLAGS) -o $@ benchmarks/nb_bench.c $(CORE)

build/dist_bench: benchmarks/dist_bench.c $(SHARD) $(CORE) include/shard.h include/kv_cache.h
	@mkdir -p build
	$(CC) $(CFLAGS) -o $@ benchmarks/dist_bench.c $(SHARD) $(CORE) -lm

bench: build/bench
shard_sim: build/shard_sim
net_bench: build/net_bench
nb_bench: build/nb_bench
dist_bench: build/dist_bench

clean:
	rm -rf build
