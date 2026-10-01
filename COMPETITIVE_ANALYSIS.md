# VeltrixDB vs Redis vs ScyllaDB vs Aerospike
**Competitive Performance Analysis — June 2026, updated October 2026**

> The June VeltrixDB numbers are from a YCSB run on AWS EC2 (495 GB RAM, 4× 873 GB NVMe); the October update is a macOS laptop.  
> Redis, ScyllaDB, and Aerospike numbers are from their official benchmarks and widely-cited third-party tests on **other** hardware. None of the three was run on the VeltrixDB machine, so every VeltrixDB-vs-X ratio below is indicative only.
>
> **The VeltrixDB YCSB numbers are historical and from the pure-Go build.** They predate the binary WAL, the one-`write(2)`-per-batch WAL flusher, the ordered-index fix, the off-heap native index and the opt-in C++ network front-end. Later measurements (different machines, stated in [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md#later-measurements)) are not YCSB and are not substituted into these tables.

---

## Quick Summary

| Database | Read Throughput | Write Throughput (durable) | Read P99 | Dataset limit |
|----------|----------------|---------------------------|----------|---------------|
| **VeltrixDB** | **427,697 ops/sec** | **18,064 ops/sec** | **2.77 ms** | **NVMe (TBs)** |
| Redis (no AOF) | 100K–500K ops/sec | 100K–200K ops/sec | 1–3 ms | RAM only |
| Redis (AOF always) | 100K–500K ops/sec | 10K–30K ops/sec | 1–3 ms | RAM only |
| ScyllaDB | 500K–2M ops/sec | 100K–500K ops/sec | <1–5 ms | SSD (TBs) |
| Aerospike | 500K–2M ops/sec | 100K–500K ops/sec | <1 ms | SSD (TBs) |

The VeltrixDB row is the June 2026 YCSB run; the others are published figures
from other hardware. Nothing in this table was run side by side.

---

## October 2026 update

What changed since the June tables, and what it measured. These are
VeltrixDB-only numbers on a macOS laptop (conditions in
[BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md#october-2026-measurements)); they
are **not** a same-hardware comparison and are not substituted into the
tables below.

| Area | Change | Measured |
|--|--|--|
| Durable single-key writes | Adaptive group commit (default): the flush window became an upper bound | emulated 300 µs sync: 1 writer 63 → 2,186 writes/s (P50 16 → 0.45 ms); 64 writers 4.0K → 37.5K writes/s (P99 16.8 → 2.6 ms); 256 writers 15.3K → 74K |
| KV through the server | go-ycsb workloads via `bench/compare` | C 55K ops/s (P99 0.39 ms), B 52K (0.65 ms), A 35K (1.3 ms), 16 threads, same-host client |
| Vector search | HNSW rework, int8 / PQ quantization, disk-resident layer-0 graph, filters, cluster fan-out | GloVe-100 100K: recall@10 0.953 at ef = 256, p50 0.77 ms (float32); network, 20K, int8, 8 clients: 24.6K QPS at recall 0.894 |
| Full-text / hybrid | BM25 inverted index, weighted RRF fusion | functional + ranking gates; no throughput benchmark yet |
| Cluster reads | new Raft leader waits for its no-op before local reads; searches fail closed on a missing peer | failover read test 20/20 missing writes without the barrier, 0/20 with it |

The durable-write gap to the published Aerospike and ScyllaDB figures was
largely the fixed group-commit window (5 ms in the June run, 15 ms by default),
which every durable write waited out; adaptive commit removes that wait. Whether it closes the gap on real
NVMe is **unmeasured** — that needs `bench/compare/compare.sh` on one Linux
machine with the durability settings its README lists.

### Search features

| | VeltrixDB | Redis | Aerospike | ScyllaDB |
|--|--|--|--|--|
| Vector search | ✅ HNSW in the database (float32 / int8 / PQ, graph in RAM or mapped file) | Redis Stack / RediSearch | Aerospike Vector Search (separate service) | Vector search (ScyllaDB Cloud) |
| Full-text | ✅ BM25, Unicode tokenizer | RediSearch | — | — |
| Hybrid vector + text | ✅ reciprocal-rank fusion | RediSearch | — | — |
| Filter on the record | ✅ QUERY predicates, index-accelerated `=` | ✅ | check vendor docs | check vendor docs |
| Measured side by side | ❌ not yet | ❌ | ❌ | ❌ |

Competitor columns list products their vendors publish; they were not tested
here, and "—" means not verified rather than absent. VectorDBBench has no
Aerospike or ScyllaDB client, so a like-for-like vector comparison with those
two needs a new driver in `bench/compare/cmd/vecbench`.

---

## 1. VeltrixDB vs Redis

### Architecture difference
Redis stores everything in RAM. VeltrixDB stores data on NVMe SSD with a large LIRS cache in RAM. When data fits in cache (as in this benchmark), both serve reads from memory.

### Write comparison

| | VeltrixDB | Redis (no persistence) | Redis (AOF always) |
|--|-----------|----------------------|-------------------|
| Throughput | **18,064 ops/sec** | 100K–200K ops/sec | 10K–30K ops/sec |
| Avg latency | 11 ms | 0.3–1 ms | 3–10 ms |
| Durability | ✅ Every write | ❌ Data loss on crash | ✅ Every write |

**Honest take:**  
- Redis without persistence publishes 5–10× these write figures (not run side by side) — but loses data on crash.  
- Redis with `appendfsync always` drops to 10K–30K ops/sec — **VeltrixDB is comparable** at 18K ops/sec and can go higher with a lower WAL flush window.

### Read comparison

| | VeltrixDB | Redis |
|--|-----------|-------|
| Throughput (200 clients) | **427,697 ops/sec** | 100K–200K ops/sec |
| Avg latency | 461 µs | 100–300 µs |
| P99 latency | 2.77 ms | 1–3 ms |
| Dataset > RAM | ✅ Reads from NVMe | ❌ OOM / eviction |

**VeltrixDB's reads were 2–4× the published Redis no-pipeline range** (not run side by side; the dataset fit in cache).  
Redis is ~2× faster on avg latency (in-memory vs cache lookup overhead).

### When to choose which
| Use case | Winner |
|----------|--------|
| Sub-millisecond latency, small dataset, no durability needed | Redis |
| Large dataset (> RAM), full durability required | **VeltrixDB** |
| Pure caching layer | Redis |
| Persistent key-value at scale | **VeltrixDB** |

---

## 2. VeltrixDB vs ScyllaDB

### Architecture difference
ScyllaDB is a distributed wide-column store (Cassandra-compatible) built in C++ using the Seastar framework (shard-per-core). It is designed for **multi-node clusters**. VeltrixDB is a single-node key-value store with built-in clustering support.

### Write comparison

| | VeltrixDB | ScyllaDB |
|--|-----------|---------|
| Throughput (single node) | 18,064 ops/sec | 100K–500K ops/sec |
| Avg latency | 11 ms | 1–5 ms |
| Durability | ✅ fsync every write | ⚠️ Commit log, periodic sync |
| Data model | Key-value (plus hashes, lists, sets, namespaces, vectors / text) | Wide-column (complex) |

**ScyllaDB's published write figures are 5–25× the June VeltrixDB run** — but ScyllaDB does NOT fsync on every individual write by default. It uses a commit log with batched/periodic sync (similar to Redis `appendfsync everysec`). With `commitlog_sync: batch` for equivalent per-write durability the gap should shrink; by how much has not been measured.

### Read comparison

| | VeltrixDB | ScyllaDB |
|--|-----------|---------|
| Throughput (single node) | **427,697 ops/sec** | 300K–800K ops/sec |
| Avg latency | 461 µs | 0.5–3 ms |
| P99 latency | 2.77 ms | 1–5 ms |

**On these different-hardware figures, VeltrixDB's single-node reads fall inside ScyllaDB's published range** (the VeltrixDB dataset fit in cache). ScyllaDB's full performance requires a multi-node cluster to distribute load.

### When to choose which
| Use case | Winner |
|----------|--------|
| Simple key-value access | **VeltrixDB** |
| Wide rows, complex queries, time-series | ScyllaDB |
| Multi-region distributed writes | ScyllaDB |
| Single-node, high read throughput | **VeltrixDB** |

---

## 3. VeltrixDB vs Aerospike

### Architecture difference
Aerospike is the closest architectural match — both use a hybrid approach with index in RAM and values on NVMe SSD. Aerospike is written in C with years of NVMe optimization. VeltrixDB is Go with WiscKey KV separation; on cgo builds (the default) the index lives off the Go heap in per-shard C++ tables, and the VLog read path is Go `pread`. Connections are served goroutine-per-connection by default; `--net=cpp` is an opt-in, experimental C++ event-loop front-end (one loop per core, binary PUT/GET/DEL/PING/MPUT/MGET only).

### Write comparison

| | VeltrixDB | Aerospike |
|--|-----------|----------|
| Throughput (single node) | 18,064 ops/sec | 100K–500K ops/sec |
| Avg latency | 11 ms | 1–5 ms |
| Durability | ✅ fsync every write | ⚠️ ACKs from the write buffer (`flush-max-ms`) by default; per-write `commit-to-device` is documented as Enterprise |
| io_uring | Opt-in VLog write bridge, off by default | ✅ |

**Aerospike's published write figures are 5–25× the June VeltrixDB run** — partly because Aerospike's default ACKs before the device write (row above). This is the most significant gap. On the VeltrixDB side the June run used a fixed 5 ms group-commit window that every durable write waited out. Group commit is now adaptive by default (`--group-commit=adaptive`: the window, default 15 ms, is only an upper bound; a lone writer is synced at once), and `--wal-flush-window-ms` (whole milliseconds; 0 flushes without waiting) is still available; whether this closes the gap on NVMe is unmeasured.

### Read comparison

| | VeltrixDB | Aerospike |
|--|-----------|----------|
| Throughput (single node) | 427,697 ops/sec | 500K–2M ops/sec |
| Avg latency | 461 µs | 100–500 µs |
| P99 latency | 2.77 ms | <1 ms |
| NVMe read path | Go `pread` (4 KB-aligned) | Custom I/O layer |

**Aerospike's published read figures are 1.2–4× the June VeltrixDB run** (different hardware). Aerospike's C-based stack and years of NVMe tuning give it an edge. VeltrixDB's read path is Go `pread`; a C++ VLog reader and io_uring read scheduler exist under `cpp/` but have no Go call site and do not run.

### When to choose which
| Use case | Winner |
|----------|--------|
| Absolute lowest latency, production-grade | Aerospike |
| Open-source, full control over storage | **VeltrixDB** |
| Sub-millisecond P99 writes | Aerospike |
| Large dataset, commodity NVMe hardware | **VeltrixDB** |
| Enterprise support required | Aerospike |

---

## Overall Positioning

```
Write throughput (durable):
Aerospike  ████████████████████████  100K–500K/sec
ScyllaDB   ████████████████░░░░░░░░  100K–500K/sec
VeltrixDB  ████░░░░░░░░░░░░░░░░░░░░   18K/sec  ← room to grow
Redis AOF  ████░░░░░░░░░░░░░░░░░░░░   10K–30K/sec

Read throughput:
Aerospike  ████████████████████████  500K–2M/sec
ScyllaDB   ████████████████░░░░░░░░  300K–800K/sec
VeltrixDB  ████████████░░░░░░░░░░░░  427K/sec    ← competitive
Redis      ████████░░░░░░░░░░░░░░░░  100K–200K/sec

Dataset scalability:
VeltrixDB  ████████████████████████  NVMe TBs, index in RAM (~142–232 B/key)
Aerospike  ████████████████████████  NVMe TBs, index in RAM
ScyllaDB   ████████████████████████  SSD TBs, distributed
Redis      ████░░░░░░░░░░░░░░░░░░░░  RAM only
```

---

## Where VeltrixDB Stands Out

### 1. Read throughput vs published Redis figures (2–4×)
427K reads/sec vs Redis's published 100–200K without pipelining (not run side by side; the dataset fit in the 400 GB cache) — while keeping data durably on disk.

### 2. Durable writes competitive with Redis AOF
18K durable writes/sec is on par with published Redis `appendfsync always` figures (not run side by side). Redis is only faster when it sacrifices durability.

### 3. Dataset size bounded by NVMe plus the index, not by values in RAM
100M keys used 25 GB per disk. 1 billion keys would use ~250 GB per disk — still under 30% of available NVMe — but the index stays in RAM: ~142 B/key (native index) plus ~90 B/key for the ordered index unless `--disable-ordered-index`, i.e. ~142–232 GB at 1B keys. Redis would need 300–450 GB of RAM for the same ~100-byte values, so at this value size the RAM saving is modest (well under 3×); with 1 KB values it is roughly 4–7×.

### 4. Full open-source, no licensing
Aerospike Enterprise and ScyllaDB Enterprise have licensing costs. VeltrixDB is fully open-source.

---

## Where VeltrixDB Needs Work

### 1. Write throughput gap vs Aerospike / ScyllaDB
18K vs 100K–500K ops/sec for per-key durable writes (YCSB, historical). The fixed WAL group-commit window was the primary bottleneck; adaptive group commit (October 2026, above; `storage/group_commit.go`) removes the wait, re-measurement on NVMe pending. Batched writes are a different picture: 8 clients × 1024-key MPUT later measured 1.77M keys/s (4-CPU CI runner, tmpfs, same-host client) and 3.54M keys/s (macOS, 1M-key space). The io_uring write bridge is not the lever: on that CI runner the C++ storage layer all on (bridge with SQPOLL, batch engine, native index) measured 1.12M vs 1.77M keys/s with all three off, so the bridge is opt-in (per-part attribution pending).

### 2. Write P99 latency
47 ms P99 writes vs Aerospike's 1–5 ms (historical YCSB). The fixed flush window added its full length to every durable write. Since 2026-10 group commit is adaptive (`--group-commit=adaptive`, default): with an emulated 300 µs device sync, a lone writer's P50 fell from 16 ms to 0.45 ms and 64 writers went from 4.0K to 37.5K durable writes/s with P99 16.8 → 2.6 ms (`VELTRIX_GC_TABLE=1 go test ./storage -run TestGroupCommit_LatencyTable -v`; emulated sync, not a device measurement). YCSB on NVMe has not been re-run.

### 3. Maturity
Aerospike has 15+ years of NVMe optimization. ScyllaDB has been production-hardened for 10+ years. VeltrixDB is newer. Since October 2026 a nightly workflow runs a 20-minute oracle-checked search soak, 10 SIGKILL/restart cycles and recall gates on real embeddings; there is still no long-running production deployment record.

### 4. Vector search at scale
Tested to 100K real vectors and 10K at 768-dim. The HNSW graph is rebuilt at every start (searches are refused until it finishes), and graph nodes stay on the Go heap (~280–400 B per vector even with PQ and the disk graph). 10M+ vectors are untested.

---

## Measuring it on the same hardware

The tables above compare VeltrixDB runs against numbers others published on
other machines. `bench/compare/` replaces that with one harness for all
three: go-ycsb workloads A–F through a VeltrixDB driver and go-ycsb's own
Aerospike and Cassandra (ScyllaDB) drivers, one database at a time on the
same host, with the durability settings stated (see its README — ScyllaDB's
default commit log sync and Aerospike CE's write buffer both ACK before the
disk). No same-hardware run has been published yet; the Aerospike and
ScyllaDB paths are compile-checked only.

Vector search has no like-for-like harness against these two: VectorDBBench
has no client for either, and `bench/compare/cmd/vecbench` ships only a
VeltrixDB driver. VeltrixDB's own numbers on a real dataset are in
[BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md#later-measurements) (GloVe-100).

## Tuning Roadmap to Close the Gap

| Improvement | Expected / measured write impact | Status |
|-------------|----------------------|--------|
| Adaptive group commit (window = upper bound) | Emulated 300 µs sync: 64 writers 4.0K → 37.5K writes/s, 256 writers 15.3K → 74K | **Default since 2026-10**; YCSB on NVMe pending |
| WAL window 1 ms + 500 threads | ~50K–80K ops/sec (estimate, unmeasured) | Configurable now (`--wal-flush-window-ms 1 --vlog-flush-window-ms 1`) |
| io_uring write path (C++) | Not a gain so far: C++ storage layer all on (bridge, batch engine, native index) 1.12M vs 1.77M keys/s all off (CI batch writes); per-part attribution pending | Implemented, opt-in (`VELTRIXDB_URING_BRIDGE=on\|sqpoll`) |
| WAL window 0 ms + io_uring | ~200K–400K ops/sec (estimate, unmeasured) | Requires testing |
| Parallel WAL per disk (4 disks) | — | Already the design: one WAL, VLog and flusher per disk |

---

*June tables: VeltrixDB v1.0 · YCSB 0.17.0 · 100M keys · 4× NVMe · 495 GB RAM. October update: macOS laptop, see BENCHMARK_RESULTS.md.*
