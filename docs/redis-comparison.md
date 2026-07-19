# VeltrixDB vs Redis: When to Use Each

This is an honest comparison. Redis is excellent software with a massive ecosystem.
VeltrixDB solves a different set of problems. Read this to decide which fits your workload.

---

## The Fundamental Difference

**Redis** stores everything in RAM. Fast by default, expensive at scale, and subject to
GC pause spikes under write pressure.

**VeltrixDB** stores values on NVMe SSDs with a DRAM index and LIRS cache. Predictable
P99 latency under sustained load, 25× storage density, and GC that cannot be permanently
paused.

---

## Latency at a Glance

| Percentile | Redis (cache hit) | VeltrixDB (cache hit) | VeltrixDB (NVMe miss) |
|---|---|---|---|
| P50 read | ~100 µs (network) | ~220 ns | ~400 µs |
| P99 read | ~1–50 ms (GC spikes) | ~510 µs | ~1 ms |
| P50 write | ~200 µs | ~17 µs per key | — |
| P99 write | spikes during compaction | ~42 µs (stable) | — |

> Redis P99 numbers are from production reports under mixed read/write load with AOF enabled.
> VeltrixDB numbers are from a 30-minute sustained 80R/20W benchmark on GKE n2-highmem-64 (8x NVMe).

The key difference is the **shape** of P99 over time. Redis P99 is low until it isn't.
VeltrixDB P99 does not drift — the three-tier GC and RT-priority NVMe queue enforce a ceiling.

---

## Storage Cost at Scale

| Keys | Value size | Redis RAM | VeltrixDB NVMe |
|---|---|---|---|
| 10M | 128 B | ~2.5 GB RAM | ~1.5 GB NVMe |
| 100M | 128 B | ~25 GB RAM | ~15 GB NVMe |
| 1B | 128 B | ~250 GB RAM | ~160 GB NVMe |

RAM costs ~15–20× more per GB than NVMe SSD in cloud pricing.
At 1 billion keys with 128-byte values:

- Redis: ~$3,000–5,000/month for the memory (r6g.16xlarge territory)
- VeltrixDB: ~$300–500/month for NVMe (n2-highmem-64 local SSD, 3 nodes)

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
  achieves ~446K batch ops/s per node vs Redis pipelining which is limited by single-
  threaded command processing
- **You're running on Kubernetes with local NVMe** — first-class Kubernetes Operator,
  Helm chart, and GKE local SSD auto-detection
- **Write amplification matters** — if you're running on cloud SSDs and watching your
  disk wear, 1.0× vs 10–30× is the difference between replacing SSDs yearly vs every
  few months

---

## Migrating from Redis

VeltrixDB uses a simple text protocol compatible with `nc`:

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
| In-memory speed (P50) | ✅ ~100 µs | ✅ ~220 ns |
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

All VeltrixDB numbers below are from a 3-node GKE cluster (n2-highmem-64, 8×375 GB
NVMe per node, raw block device VLog, Linux 6.6, `--read-heavy` preset).

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
