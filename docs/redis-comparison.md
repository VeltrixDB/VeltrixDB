# VeltrixDB vs Redis: When to Use Each

This is an honest comparison. Redis is excellent software with a massive ecosystem.
VeltrixDB solves a different set of problems. Read this to decide which fits your workload.

---

## The Fundamental Difference

**Redis** stores everything in RAM. Fast by default, expensive at scale, and subject to
GC pause spikes under write pressure.

**VeltrixDB** stores values on NVMe SSDs with a DRAM index and LIRS cache. Predictable
P99 latency under sustained load, 25× storage density, and GC that cannot be permanently
paused. On cgo builds (the default, including the Docker image) the index lives off the
Go heap in C++ tables, so Go's garbage collector does not scan it: with 5M keys a full
GC takes 0.27 ms instead of 21 ms.

---

## Latency at a Glance

| Percentile | Redis (cache hit) | VeltrixDB (cache hit) | VeltrixDB (NVMe miss) |
|---|---|---|---|
| P50 read | ~100 µs (network) | ~92 ns (in-process) | ~400 µs |
| P99 read | ~1–50 ms (GC spikes) | ~510 µs | ~1 ms |
| P50 write | ~200 µs | ~17 µs per key | — |
| P99 write | spikes during compaction | ~42 µs (stable) | — |

> Redis P99 numbers are from production reports under mixed read/write load with AOF enabled.
> VeltrixDB cache-hit P50 is the in-process lookup (~92 ns since the cache was sharded). The other
> VeltrixDB numbers are from an internal 30-minute sustained 80R/20W run on GKE n2-highmem-64
> (8x NVMe) that has not been reproduced with a published harness; see Benchmark Reference.

The key difference is the **shape** of P99 over time. Redis P99 is low until it isn't.
VeltrixDB P99 does not drift — the three-tier, admission-controlled VLog GC enforces a ceiling.

---

## Storage Cost at Scale

| Keys | Value size | Redis RAM | VeltrixDB RAM (index) | VeltrixDB NVMe (values) |
|---|---|---|---|---|
| 100M | 128 B | ~25 GB | ~14–23 GB | ~15 GB |
| 1B | 128 B | ~250 GB | ~142–232 GB | ~160 GB |
| 100M | 1 KB | > 100 GB | ~14–23 GB | ~137 GB |
| 1B | 1 KB | > 1 TB | ~142–232 GB | ~1.4 TB |

VeltrixDB's RAM column is the index: ~142 B/key with the native index
(measured at 5M keys), plus ~90 B/key for the ordered index unless
`--disable-ordered-index`; add whatever cache you configure. The Redis column
is an estimate of values + per-key overhead; we have not measured it.

**The RAM saving depends on value size.** With 128-byte values the index is
about as large as the values, so VeltrixDB saves little RAM — its advantage
there is NVMe durability without fork-based snapshots. With 1 KB values it
needs roughly 4–7× less RAM. These rows are per-key arithmetic, not a
billion-key benchmark.

---

## Write Amplification

| Engine | Write amplification | Why |
|---|---|---|
| Redis (AOF rewrite) | 2–5× | AOF rewrite rewrites full dataset |
| RocksDB / LevelDB | 10–30× | LSM compaction sorts and rewrites |
| **VeltrixDB** | **~1.0×** | Values append-once to Value Log, never rewritten |

Write amplification directly maps to SSD wear and write latency. At 1.0× every byte
lands on NVMe exactly once.

---

## When Redis Wins

Use Redis when:

- **Your full dataset fits in RAM comfortably** — if you have 5M keys at 256 bytes,
  Redis's DRAM-only path is faster at P50 than any disk-backed system
- **You need pub/sub, streams, sorted sets, or Lua scripting** — Redis has a rich
  data structure model; VeltrixDB is a key-value store
- **Your team already runs Redis** — operational familiarity has real value
- **You need a managed hosted service today** — Redis Cloud, ElastiCache, Upstash
  all exist; VeltrixDB's managed offering is not yet available

---

## When VeltrixDB Wins

Use VeltrixDB when:

- **Your dataset doesn't fit in RAM** — or fitting it in RAM costs more than you want
  to pay. VeltrixDB's LIRS cache handles hot keys in DRAM; cold keys spill to NVMe
  automatically
- **You need predictable P99 under sustained write pressure** — if your production
  dashboards show Redis P99 spikes during AOF rewrite or keyspace expiry, VeltrixDB's
  three-tier GC is designed for this exact problem
- **You're doing bulk writes at high concurrency** — MultiPut with 1024-entry batches
  measured 1.77M keys/s (8 clients, 4-CPU CI runner, tmpfs, same-host client) and 3.54M
  keys/s (8 clients, macOS, 1M-key space) vs Redis pipelining which is limited by single-
  threaded command processing
- **You're running on Kubernetes with local NVMe** — first-class Kubernetes Operator,
  Helm chart, and GKE local SSD auto-detection
- **Write amplification matters** — if you're running on cloud SSDs and watching your
  disk wear, 1.0× vs 10–30× is the difference between replacing SSDs yearly vs every
  few months

---

## Migrating from Redis

VeltrixDB uses a simple text protocol compatible with `nc` (served by the default
`--net=go` front-end; the opt-in C++ front-end, `--net=cpp`, speaks only the binary protocol):

```bash
# Redis
redis-cli SET mykey myvalue
redis-cli GET mykey

# VeltrixDB (same semantics)
echo -e "PUT mykey myvalue\nGET mykey" | nc localhost 9000
```

For SDK migration, swap the client:

```python
# Before (redis-py)
import redis
r = redis.Redis(host='localhost', port=6379)
r.set('key', 'value')
val = r.get('key')

# After (veltrixdb-python)
import veltrixdb
r = veltrixdb.Client(host='localhost', port=9000)
r.put('key', 'value')
val = r.get('key')
```

The API surface is intentionally close. PUT/GET/DEL map directly to SET/GET/DEL.
MultiPut/MultiGet map to Redis pipelines. SETNX, INCR, DECR, CAS are supported.

> **Redis protocol compatibility (RESP) is on the roadmap.** Once available, zero
> code changes will be required — only a connection string update.

---

## Feature Matrix

| Feature | Redis | VeltrixDB |
|---|---|---|
| In-memory speed (P50) | ✅ ~100 µs | ✅ ~92 ns (in-process cache hit) |
| Predictable P99 under writes | ⚠️ spikes | ✅ bounded |
| NVMe storage tier | ❌ | ✅ |
| Write amplification | ⚠️ 2–30× | ✅ ~1.0× |
| Pub/Sub | ✅ | ❌ |
| Sorted sets | ✅ | ❌ |
| Lua scripting | ✅ | ❌ |
| Kubernetes Operator | ⚠️ third-party | ✅ first-class |
| Prometheus metrics | ✅ | ✅ |
| Encryption at rest | ✅ | ✅ (AES-256-GCM) |
| Multi-node replication | ✅ | ✅ (async/quorum/strong) |
| Change data capture | ❌ built-in | ✅ |
| Atomic CAS / INCR | ✅ | ✅ |
| SDKs (Go/Python/Node/Rust/C++) | ✅ | ✅ |
| RESP protocol | ✅ | 🗓 roadmap |
| Managed cloud offering | ✅ | 🗓 roadmap |

---

## Benchmark Reference

All VeltrixDB numbers below are from an internal 3-node GKE cluster run (n2-highmem-64,
8×375 GB NVMe per node, raw block device VLog, Linux 6.6, `--read-heavy` preset). It is
historical, predates the current storage and network changes, and has not been reproduced
with a published harness. Reproducible single-node YCSB numbers are in
[BENCHMARK_RESULTS.md](../BENCHMARK_RESULTS.md).

```
3-node cluster · 80R/20W · 30 minutes sustained · 1 billion keys

Reads/s:         7,200,000
Writes/s:        1,800,000
P50 (blended):   ~220 ns
P99 (blended):   ~510 µs
GC emergency:    0 events in 30 min
Errors:          0
```

Full benchmark methodology: [BENCHMARKING.md](../BENCHMARKING.md)

---

## Questions?

Open an issue on GitHub or start a discussion. If you're evaluating VeltrixDB for
a production workload and want to talk through your specific requirements, we're happy
to help you benchmark against your actual data shape.
