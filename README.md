# KV Cache in C

A from-scratch key-value cache in C that explores the design decisions behind Redis — LRU eviction, single-threaded vs multi-threaded performance, consistent hashing, replication, and non-blocking I/O.

Built as a learning project to answer: **why is Redis designed the way it is?**

## Project Structure

```
include/
├── kv_cache.h          # Core LRU cache — hash map (FNV-1a) + doubly-linked list
└── shard.h             # Consistent hash ring with configurable vnodes and replication
src/
├── kv_cache.c          # O(1) get/put/delete with LRU eviction
├── shard.c             # Consistent hashing implementation
├── node_server.c       # Blocking TCP server (one client at a time)
└── node_server_nb.c    # Non-blocking TCP server using kqueue (macOS)
tests/
└── test.c              # Correctness tests for the cache
benchmarks/
├── bench.c             # Single-threaded vs multi-threaded benchmarks
├── shard_sim.c         # Shard simulation — vnode tuning, replication, CAP tradeoffs
├── net_bench.c         # In-process vs TCP reads (proves network is the bottleneck)
├── nb_bench.c          # Blocking vs non-blocking server with concurrent clients
└── dist_bench.c        # Distributed benchmark — 5 TCP servers with hash ring routing
```

## Build & Run

```bash
make              # builds everything
make test         # run correctness tests
./build/bench     # single vs multi-threaded benchmarks
./build/shard_sim # consistent hashing + replication simulation
./build/net_bench # in-process vs network benchmark (starts/stops server automatically)
./build/nb_bench  # blocking vs non-blocking server benchmark
./build/dist_bench # 5-node distributed shard benchmark
```

Requires macOS (uses `kqueue` for the non-blocking server). Tested on Apple Silicon.

## Benchmark Results (Apple Silicon)

### Single-threaded vs Multi-threaded (pre-generated keys)

| Benchmark | Single-threaded | Multi-threaded (4T) | Delta |
|---|---|---|---|
| Read (peek, no LRU) | 41M ops/sec | 88.5M ops/sec | +2.2x |
| Write (mutex) | 6.2M ops/sec | 1.5M ops/sec | -4x |

**Takeaway:** Reads without LRU mutation scale with cores. Writes with a global mutex get *slower* — cache-line bouncing between cores costs more than the operation itself. This is why Redis is single-threaded.

### In-process vs Network

| Mode | Throughput | Latency |
|---|---|---|
| In-process | 61.8M ops/sec | 16 ns/op |
| TCP sequential (localhost) | 62K ops/sec | 16,114 ns/op |
| TCP pipelined (100/batch) | 431K ops/sec | 2,318 ns/op |

**Takeaway:** The hash lookup takes 16 ns. A localhost TCP round-trip adds 16,000 ns. Network is 99.9% of the cost. This is why Redis is network-bound, not CPU-bound.

### Blocking vs Non-blocking Server

| Server | Clients | Throughput |
|---|---|---|
| Non-blocking (kqueue) | 1 | 1.9M ops/sec |
| Non-blocking (kqueue) | 10 | 4.4M ops/sec |
| Non-blocking (kqueue) | 50 | 4.4M ops/sec |
| Blocking | 1 | 404K ops/sec |

**Takeaway:** The kqueue event loop multiplexes clients without blocking. 10 clients → 2.3x throughput because the server processes other clients while one waits for its response. Saturates at ~4.4M ops/sec (single CPU core ceiling).

### Consistent Hashing — Shard Simulation

**Vnode tuning (5 shards, 100K keys):**

| vnodes/shard | max skew from ideal |
|---|---|
| 10 | +150% |
| 50 | +28% |
| 500 | +9.1% |
| 1000 | +5.8% |

**Replication vs data survival (1 shard crashes):**

| Replica Factor | Keys Lost |
|---|---|
| RF=1 | 21.8% |
| RF=2 | 0% |
| RF=3 | 0% |

**Key redistribution:** When a shard goes down, only ~20% of keys remap (1/N, ideal for consistent hashing). The other 80% are untouched.

## Key Lessons

1. **LRU makes every read a write** — `get()` must update the linked list, so reads can't be lock-free
2. **Mutexes + multiple threads can be slower than single-threaded** — cache-line bouncing between cores costs more than the operation
3. **Network I/O is 1000x slower than in-process** — adding CPU threads doesn't help when you're waiting 16us for TCP on every request
4. **Non-blocking event loops maximize throughput** — the server never idles waiting for one client
5. **Connection pooling doesn't eliminate per-request overhead** — it saves the TCP handshake, not the round-trip
6. **More ports on one machine != more parallelism** — same NIC, same kernel, same CPU
7. **Consistent hashing minimizes redistribution** — only 1/N keys move when a shard goes down
8. **Replication trades write amplification for fault tolerance** — RF=2 survives single shard failure with zero data loss
