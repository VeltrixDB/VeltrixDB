# VeltrixDB Benchmark Results
**Date:** June 9, 2026  
**Dataset:** 100 Million Keys · 100 Byte Values · 4× NVMe Disks · 400 GB Cache

> **Historical, pure-Go build.** This run predates the binary WAL, the
> one-`write(2)`-per-batch WAL flusher, the ordered-index fix, the off-heap
> native index (cgo builds, now the default and the Docker image) and the
> opt-in C++ network front-end. The numbers are kept as recorded; later
> measurements are under [Later measurements](#later-measurements).
> The YCSB 0.17.0 binding used for this run is not in this repository, so
> this section cannot be re-run from it; `bench/compare` (go-ycsb, below) is
> the reproducible YCSB path.

---

## Test Environment

| Component | Specification |
|-----------|--------------|
| Instance | AWS EC2 (high-memory) |
| RAM | 495 GB total · 488 GB available |
| Storage | 4× NVMe SSD · 873 GB each = **3.5 TB total** |
| VLog mode | Raw block device (`/dev/nvmeXn1p2`) |
| VeltrixDB cache | 400 GB (LIRS) |
| Client threads | 200 |
| Key count | 100,000,000 |
| Value size | ~100 bytes |
| Benchmark tool | YCSB 0.17.0 |

---

## Write Benchmark (100M Key Load)

### Configuration
| Parameter | Value |
|-----------|-------|
| WAL flush window | 5 ms |
| VLog flush window | 5 ms |
| Threads | 200 |
| Disks | 4× NVMe (striped) |

### Results

| Metric | Value |
|--------|-------|
| **Total keys written** | **100,000,000** |
| **Throughput** | **18,064 ops/sec** |
| Total runtime | 92.3 minutes |
| Avg write latency | 11.06 ms |
| Min latency | 413 µs |
| P95 latency | 30.8 ms |
| P99 latency | 47.3 ms |
| **Failures** | **0 (100% success)** |
| Durability | WAL + fdatasync on every write |

### Latency Distribution

```
Min     ████░░░░░░░░░░░░░░░░░░░░░░░░░░    413 µs
Avg     ████████████░░░░░░░░░░░░░░░░░░   11.06 ms
P95     ████████████████████████░░░░░░   30.8 ms
P99     ████████████████████████████░░   47.3 ms
```

---

## Read Benchmark (10M Reads on 100M Key Dataset)

### Configuration
| Parameter | Value |
|-----------|-------|
| Cache | 400 GB (entire dataset fits in RAM) |
| Threads | 200 |
| Operations | 10,000,000 |
| Workload | 100% reads (YCSB workload_c) |

### Results

| Metric | Value |
|--------|-------|
| **Throughput** | **427,697 ops/sec** |
| Total runtime | 23.4 seconds for 10M reads |
| Avg read latency | 461 µs |
| Min latency | **10 µs** |
| P95 latency | 2,069 µs |
| P99 latency | 2,771 µs |
| Max latency | 53.6 ms |
| **Hit rate** | **100% (0 NOT_FOUND)** |

### Latency Distribution

```
Min     █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░      10 µs
Avg     ████░░░░░░░░░░░░░░░░░░░░░░░░░░     461 µs
P95     ██████████████░░░░░░░░░░░░░░░░    2,069 µs
P99     ████████████████░░░░░░░░░░░░░░    2,771 µs
```

---

## VeltrixDB vs Redis — Head to Head

> Redis was **not** run on this machine. The Redis columns are typical
> published ranges from other hardware, so the ratios below are indicative,
> not a side-by-side measurement.

### Write Throughput

| Mode | VeltrixDB | Redis |
|------|-----------|-------|
| **Durable writes** (fsync every write) | **18,064 ops/sec** | 10,000–30,000 ops/sec |
| Non-durable writes (no fsync) | N/A — always durable | 100,000–200,000 ops/sec |

> VeltrixDB **always** writes durably. Redis requires `appendfsync always` for equivalent guarantees which reduces Redis to 10K–30K ops/sec — **comparable to or slower than VeltrixDB**.

---

### Read Throughput

| Clients | VeltrixDB | Redis (no pipeline) | Redis (pipelined) |
|---------|-----------|--------------------|--------------------|
| 200 concurrent | **427,697 ops/sec** | 100,000–200,000 ops/sec | 500,000–1,000,000 ops/sec |

> Against those published ranges, VeltrixDB's reads were **2–4× the Redis no-pipeline figures**. The whole dataset fit in the 400 GB cache, so these reads were served from RAM, not NVMe.

---

### Read Latency

| Percentile | VeltrixDB | Redis |
|-----------|-----------|-------|
| Average | 461 µs | 100–300 µs |
| P95 | 2,069 µs | 500–1,500 µs |
| P99 | **2,771 µs** | 1,000–3,000 µs |

> Latency is in the same range. Redis is ~2× faster at avg due to pure in-memory access, but VeltrixDB's P99 is competitive.

---

### Resource Usage

| Resource | VeltrixDB (100M keys) | Redis (100M keys) |
|----------|-----------------------|-------------------|
| RAM required | **15 GB** as recorded (index; 400 GB used as cache) — today's per-key figures give ~14–23 GB (142 B/key native index + ~90 B/key ordered index) | **30–45 GB** (data lives in RAM; estimate) |
| Disk required | ~25 GB per NVMe | Swap/RDB snapshots only |
| Max dataset size | **3.5 TB (NVMe)** | Limited by RAM |
| Dataset > RAM | ✅ Works (reads from NVMe) | ❌ Evicts or crashes |

---

### Feature Comparison

| Feature | VeltrixDB | Redis |
|---------|-----------|-------|
| Crash-safe writes | ✅ WAL + fdatasync | ❌ Not by default |
| Dataset size limit | NVMe capacity (TBs) | Available RAM |
| 100M keys possible | ✅ Used 25 GB disk | ⚠️ Needs 30–45 GB RAM |
| 1B keys possible | ✅ ~250 GB disk, plus ~142–232 GB RAM for the index | ⚠️ Needs 300–450 GB RAM |
| Read throughput | **427K ops/sec** | 100–200K ops/sec (published, not run here) |
| Write throughput (durable) | **18K ops/sec** | 10–30K ops/sec (published, not run here) |
| Multi-disk striping | ✅ 4× NVMe (one WAL + VLog per `--data-dirs` entry) | ❌ Single instance |
| Key-value separation | ✅ WiscKey (VLog) | ❌ All in memory |

---

## Progression Across Runs

| Run | Write Speed | Read Speed | Read Hit Rate |
|-----|------------|-----------|--------------|
| 1M keys · 50 threads · 10ms WAL | 2,036 ops/sec | 4,000 ops/sec | 48.5% |
| 10M keys · 50 threads · 10ms WAL | 3,164 ops/sec | 301,386 ops/sec | 79% |
| **100M keys · 200 threads · 5ms WAL** | **18,064 ops/sec** | **427,697 ops/sec** | **100%** |

---

## Key Takeaways

1. **Writes**: 18,064 durable writes/sec across 4 NVMe disks. Zero failures across 100M operations. Comparable to Redis with `appendfsync always`.

2. **Reads**: 427,697 reads/sec at 461 µs average latency — 2–4× the published Redis single-instance no-pipeline range (not run side by side). Data was persisted on NVMe, but the whole dataset fit in the 400 GB cache, so reads were served from RAM.

3. **Scalability**: 100M keys used only ~25 GB per disk out of 873 GB available. The same machine can hold **1 billion+ keys** on disk, but the index still lives in RAM: ~142 B/key (native index) plus ~90 B/key for the ordered index, i.e. ~142–232 GB at 1B keys, leaving less for the cache. Redis would need 300–450 GB RAM for the same ~100-byte values, so at this value size the RAM saving is well under 3×; it grows with value size (≈4–7× at 1 KB).

4. **Durability**: Every single write is crash-safe (WAL + fdatasync). Redis requires special configuration for the same guarantee, and pays a similar performance cost when enabled.

5. **100% read hit rate**: With proper dataset sizing (`recordcount` matching `operationcount`), zero NOT_FOUND errors across 10 million reads.

---

## Later measurements

Not YCSB and not this machine; each row states its conditions.

| Measurement | Result | Conditions |
|-------------|--------|------------|
| Server batch writes, 8 clients × 1024-key MPUT, 1M-key space | **3.54M keys/s, P99 4.2 ms** (1.45M, 9.6 ms before the ordered-index fix) | macOS |
| Batch writes, Go front-end, C++ storage off | **1.77M keys/s, P99 12.2–12.7 ms** | 4-CPU CI runner, tmpfs, client on same host |
| Reads, same configuration | **177–182K ops/s, P99 1.8–2.0 ms** | same |
| Mixed-workload write P99, 1 ms flush window | **2.57 ms** | same |
| Batch writes, C++ storage layer all on (io_uring bridge + SQPOLL, batch engine, native index) | 1.12M keys/s | same; per-part attribution pending |
| C++ network front-end (`--net=cpp`, opt-in) vs Go | read P50 154 vs 286 µs, poll read throughput 194K vs 177K ops/s; read P99 ~3.0 vs 1.8 ms, batch writes ~12% lower | same, measured before disk reads moved off the loop; not faster overall |
| Vector search on GloVe-100 (100K real word vectors, 100-dim, 500 queries), recall@10 at ef = 64 / 128 / 256 / 512 | float32 0.842 / 0.906 / 0.953 / 0.983 (p50 0.25 / 0.42 / 0.77 / 1.43 ms); int8 0.839 / 0.905 / 0.952 / 0.983; pq (m = 25 = dim/4) 0.797 / 0.870 / 0.927 / 0.969; pq + disk graph 0.788 / 0.875 / 0.931 / 0.966 | macOS, in-process, single query at a time (`TestRealEmbeddings`, needs `VELTRIX_ANN_DIR`; see below) |
| Durable single-key Put, adaptive vs fixed group commit, 15 ms window | 1 writer P50 16.0 ms → 0.45 ms (63 → 2.2K writes/s); 64 writers 4.0K → 37.5K writes/s, P99 16.8 → 2.6 ms; 256 writers 15.3K → 74K writes/s; same writes per fdatasync | macOS, **emulated 300 µs fdatasync** (`VELTRIX_GC_TABLE=1 go test ./storage -run TestGroupCommit_LatencyTable -v`); 2 ms sync: 64 writers 3.5K → 11.7K |
| Native index vs Go map, 5M keys | full GC 21 → 0.27 ms, settled RSS 168 → 142 B/key, ~19 ns more per cache-miss lookup | engine benchmark; `go test ./storage -run '^$' -bench 'Index_Footprint\|Index_Get' -benchtime 1x` compares the two (footprint uses 2M keys by default) |

Reproduce the CI rows with `scripts/net-bench.sh` or the `Net front-end`
workflow (see BENCHMARKING.md): `ENGINES=nocgo NETS=go WINDOWS="5 1"` for
the "C++ storage off" rows, `ENGINES=sqpoll` for the "all on" row (the bridge
is now opt-in, so `native` alone no longer matches it), `NETS="go uring poll"`
for the front-end row. The macOS 3.54M row is the same batch phase
(`loadtest --mode write --concurrency 8 --batch-size 1024 --num-keys 1000000`)
against a local server.

---

## October 2026 measurements

Taken while building adaptive group commit, vector / text / hybrid search,
PQ and the disk graph. **All on one macOS laptop (Apple silicon) unless
stated**, so absolute numbers do not predict a Linux NVMe server and none of
them is a comparison with another database (`bench/compare` exists for that
and has not been run on shared hardware yet). Each table names the test or
tool that reproduces it.

### Durable single-key writes — adaptive vs fixed group commit

`VELTRIX_GC_TABLE=1 go test ./storage -run TestGroupCommit_LatencyTable -v`.
The device's fdatasync cost is **emulated** (a sleep before the real sync),
because macOS fsync returns at the drive cache. 15 ms window, 128 B values,
each writer doing sequential durable `Put`s for 1.5 s.

Emulated sync = 300 µs (NVMe-like):

| Writers | Mode | P50 | P99 | Writes/s | Writes per fdatasync |
|--|--|--|--|--|--|
| 1 | fixed | 16.01 ms | 17.76 ms | 63 | 1.0 |
| 1 | adaptive | 0.45 ms | 0.52 ms | 2,186 | 1.0 |
| 8 | fixed | 16.02 ms | 16.39 ms | 501 | 8.0 |
| 8 | adaptive | 0.96 ms | 1.22 ms | 8,283 | 8.0 |
| 64 | fixed | 15.94 ms | 16.81 ms | 4,053 | 64.0 |
| 64 | adaptive | 1.67 ms | 2.64 ms | 37,504 | 63.9 |
| 256 | fixed | 16.40 ms | 33.15 ms | 15,291 | 249.3 |
| 256 | adaptive | 3.18 ms | 11.54 ms | 73,999 | 255.2 |

Emulated sync = 2 ms (slow / network disk):

| Writers | Mode | P50 | P99 | Writes/s |
|--|--|--|--|--|
| 1 | fixed | 18.10 ms | 23.68 ms | 55 |
| 1 | adaptive | 2.41 ms | 2.52 ms | 417 |
| 64 | fixed | 18.18 ms | 19.12 ms | 3,541 |
| 64 | adaptive | 5.40 ms | 7.07 ms | 11,691 |
| 256 | fixed | 18.32 ms | 35.17 ms | 13,705 |
| 256 | adaptive | 6.96 ms | 18.56 ms | 32,277 |

Side effect: the storage test suite went from ~50 s to ~27 s, and the
search quality gate from 17 s to 3.7 s (its 2,000 single-key deletes no
longer wait 15 ms each).

### YCSB through the server — VeltrixDB only

`bench/compare/cmd/ycsb` (go-ycsb core workloads) against a local server,
20,000 records × 10 fields × 100 B, 20,000 operations, 16 threads, zipfian,
whole-record writes (`cd bench/compare && START=0 DBS=veltrixdb RECORDS=20000
OPS=20000 THREADS=16 WORKLOADS="a b c d e f" ./compare.sh`). Client and server on the same laptop; macOS fsync does
not reach the platter, so write latency here is a lower bound.

| Workload | Op | Ops/s | Avg | P99 | P99.9 |
|--|--|--|--|--|--|
| load | INSERT | 37,495 | 419 µs | 1.29 ms | 2.04 ms |
| A (50/50 read/update) | total | 34,640 | 453 µs | 1.30 ms | 2.98 ms |
| | READ | 17,230 | 262 µs | 0.77 ms | 1.55 ms |
| | UPDATE | 17,434 | 642 µs | 1.63 ms | 3.14 ms |
| B (95/5) | total | 52,255 | 303 µs | 0.65 ms | 0.74 ms |
| C (read only) | READ | 55,287 | 287 µs | 0.39 ms | 0.53 ms |
| D (read latest) | total | 55,163 | 288 µs | 0.39 ms | 0.59 ms |
| E (short scans) | SCAN | 23,651 | 638 µs | 1.43 ms | 1.97 ms |
| F (read-modify-write) | total | 40,067 | 392 µs | 0.81 ms | 0.98 ms |
| | READ_MODIFY_WRITE | 13,397 | 915 µs | 1.22 ms | 1.49 ms |

### Vector search

Full tables and methodology: [docs/vector-search.md](docs/vector-search.md#measured-performance).

| Measurement | Result | Conditions |
|--|--|--|
| GloVe-100, 100K words, recall@10 at ef = 64 / 128 / 256 / 512 | float32 0.842 / 0.906 / 0.953 / 0.983; int8 0.839 / 0.905 / 0.952 / 0.983; pq (m = 25) 0.797 / 0.870 / 0.927 / 0.969; pq + disk 0.788 / 0.875 / 0.931 / 0.966 | in-process, one query at a time: `scripts/ann-dataset.py glove-100-angular /tmp/ann --train 100000 --test 500` then `VELTRIX_ANN_DIR=/tmp/ann go test ./storage -run TestRealEmbeddings -v -timeout 30m` |
| GloVe-100 float32 latency, ef = 128 | p50 0.42 ms, p99 0.63 ms, 2,351 QPS (1 thread) | same |
| GloVe-100 20K words, int8, over the network, 8 clients | ef = 64: recall 0.894, 24,637 QPS, p99 0.68 ms; ef = 256: recall 0.985, 15,629 QPS, p99 0.91 ms | `bench/compare/cmd/vecbench -data <dir> -train 20000 -quant int8 -threads 8`, same-host client |
| Heap per vector, 768-dim, 10K vectors | float32 3,383 B; int8 1,078 B; pq (m = 96) 493 B; pq + disk graph 378 B + 216 B mapped | `VELTRIX_VECTOR_MEMORY=1 go test ./storage -run TestVectorMemoryTable -v` |
| Quantized re-rank via parallel MultiGet | GloVe int8 ef = 64: p50 1.95 → 0.61 ms, p99 13.8 → 0.81 ms | before / after |
| HNSW rework, 20K × 128 random | search 240 → 99 µs (853 → 6 allocs); with 50 % deleted 6.56 ms → 190 µs; insert 349 → 209 µs | `go test ./storage -run '^$' -bench 'HNSW_(Search\|Search_Deleted50pct\|Insert)$'`, index only |
| CI quality gate, 5K × 64 clustered | recall@10 f32 0.927, ef256 0.998, int8 0.923, pq 0.882, pq + disk 0.881, filtered 0.994, after 40 % deletes ~0.96 | every PR (`node-7-search`, `go test ./storage -run TestSearchQualityGate -v`) |

### Stability

| Test | Result | Conditions |
|--|--|--|
| Search soak, 60 s, 4 writers + 4 searchers, 5K-id space | 201,573 writes, 292,079 searches, 0 results outside the oracle, 288,025 self-queries with 0 misses, RSS flat at ~1.0 GB, exact oracle match before and after restart | `VELTRIX_SOAK_DURATION=60s go test ./tests/integration -run TestSearchSoak -v`; nightly runs 20 min on Linux |
| Crash chaos, SIGKILL mid-write | 3 cycles, 8,524 / 8,380 / 11,851 acknowledged writes, all visible after restart + rebuild, no deleted id returned | `VELTRIX_CHAOS_CYCLES=3 go test ./tests/integration -run TestSearchCrashRecovery -v`; nightly runs 10 cycles |
| Raft failover read barrier | without it a new leader missed the acknowledged write in 20 / 20 test rounds; with it 0 / 20 | `go test ./consensus -run TestWaitLeaderApplied_FailoverVisibility -v` |

### CI (GitHub-hosted runner, 2026-09-30, run 36698240341)

All jobs green. Durations: node-7 search regression 3 m 40 s (quality gates
19 s, race tests 2 m 39 s, 3-node end-to-end 10 s); node-5 race (storage)
3 m 57 s; node-3 integration 59 s; bench (density + GC gates, ±15 %
regression check) 5 m 24 s. The recall values measured on the runner are in
that run's job summary.

---

*June 2026 section: YCSB 0.17.0 · VeltrixDB v1.0. October 2026 section: go-ycsb v1.0.3 (bench/compare), VeltrixDB feat/perf-scale-hardening.*
