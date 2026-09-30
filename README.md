# VeltrixDB

[![CI](https://github.com/VeltrixDB/veltrixdb/actions/workflows/ci.yml/badge.svg)](https://github.com/VeltrixDB/veltrixdb/actions/workflows/ci.yml)
[![Go 1.19+](https://img.shields.io/badge/Go-1.19+-00ADD8?logo=go)](https://golang.org)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![GitHub Stars](https://img.shields.io/github/stars/VeltrixDB/veltrixdb?style=social)](https://github.com/VeltrixDB/veltrixdb/stargazers)

**NVMe-native distributed key-value database with built-in vector, full-text and hybrid search. 427K reads/s measured (YCSB). ~1× write amplification by design. Kubernetes-first.**

An in-memory store keeps every value in RAM. VeltrixDB keeps values on NVMe and only the index in RAM:
~142 B/key (plus ~90 B/key for the ordered index unless disabled). At 1 KB values that is roughly 4–7× less RAM;
at 128 B values the index is about as large as the values, so the saving comes from larger values.  
No LSM compaction, so no compaction-driven P99 spikes.

```bash
docker run -p 9000:9000 ghcr.io/veltrixdb/veltrixdb:latest
echo -e "PUT hello world\nGET hello\nPING" | nc localhost 9000
```

---

## Benchmark numbers

**Measured — YCSB 0.17.0 · single node · AWS EC2 (4×NVMe) · 100M keys · 200 threads**
(full setup and raw output: [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md))

| Metric | Value |
|--------|-------|
| Reads/s | **427,697** |
| Durable writes/s (fsync every write) | **18,064** |
| Read latency (avg) | **461 µs** |
| Errors across 100M ops | **0** |
| Storage density | **~160 GB** for 1B × 128 B values |

> **Note on cluster-scale numbers.** Earlier drafts cited 7.2M reads/s / 1.8M
> writes/s from an internal 3-node GKE run. That configuration (io_uring
> write path fully activated, 8×NVMe per node) has not yet been reproduced
> with a published harness, so we no longer lead with it. The YCSB numbers
> above are the ones you can reproduce today with `scripts/bench.sh`.

**Single node (Linux NVMe), historical — pure-Go path, fixed group-commit window:**

| Operation | P50 | P99 |
|-----------|-----|-----|
| GET — cache hit | 0.05 ms | 0.28 ms (~1.4M reads/s) |
| PUT — 15 ms fixed flush window | 5 ms | 15.2 ms |
| PUT — 5 ms window, 512 workers | 2.6 ms | 5.2 ms (~102K writes/s) |
| MultiPut 1024 entries | — | ~9.5 ms (~426K entries/s) |

**October 2026 (macOS laptop; see [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md#october-2026-measurements) for conditions):**

| Measurement | Result |
|--|--|
| Durable PUT, **adaptive group commit** (default now), emulated 300 µs fdatasync | 1 writer P50 16 ms → **0.45 ms**; 64 writers 4.0K → **37.5K writes/s** (P99 2.6 ms); 256 writers → **74K writes/s** |
| YCSB C / B / A through the server (go-ycsb, 16 threads, same-host client) | 55K / 52K / 35K ops/s, P99 0.39 / 0.65 / 1.3 ms |
| Vector search, GloVe-100 (100K real word vectors), recall@10 at ef = 128 / 256 | 0.906 / 0.953, p50 0.42 / 0.77 ms (float32, one query at a time) |
| Vector search over the network, 20K GloVe, int8, 8 clients | 24.6K QPS at recall 0.894; 15.6K QPS at recall 0.985 |
| RAM per 768-dim vector | float32 3.4 KB, int8 1.1 KB, pq 0.49 KB, pq + disk graph 0.38 KB |

The first table's rows are historical (pure-Go path). Later server batch writes, 8 clients × 1024-key MPUT: 3.54M keys/s, P99 4.2 ms (macOS, 1M-key space) and 1.77M keys/s, P99 12.2–12.7 ms (4-CPU CI runner, tmpfs, same-host client). See [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md#later-measurements).

Full methodology: [BENCHMARKING.md](BENCHMARKING.md). To compare against
Aerospike and ScyllaDB on your own hardware — same machine, same YCSB
workloads, one database at a time — use [bench/compare](bench/compare/README.md).

---

## Why VeltrixDB

### The problem with Redis at scale

Redis is in-memory: every value, hot or cold, is in RAM, plus Redis's own per-key overhead. Cloud RAM costs far more per GB than NVMe SSD, so the bill grows with the dataset.

VeltrixDB stores values on NVMe and keeps the index (and whatever cache you configure) in DRAM. The index costs ~142 B/key with the native index (measured at 5M keys) plus ~90 B/key for the ordered index unless `--disable-ordered-index`; a 1 KB value packs into ~1.37 KB of NVMe. Worked through for 1 billion keys × 1 KB values: an in-memory store needs over 1 TB of RAM; VeltrixDB needs 142–232 GB of RAM for the index plus ~1.4 TB of NVMe. With 128-byte values the index is about as large as the values, so small-value workloads save little RAM. These are per-key arithmetic, not a billion-key benchmark.

### The problem with LSM trees (RocksDB, LevelDB)

LSM compaction rewrites full key-value records to merge sorted runs. Typical write amplification is **10–30×** — every byte you write eventually lands on disk 10–30 times. That burns SSD write endurance and adds latency spikes during heavy compaction.

VeltrixDB uses [WiscKey](https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu) KV-separation: **values are written once to an append-only Value Log on NVMe**, and there is no LSM compaction. GC only copies still-live values out of mostly-dead regions of the VLog, so write amplification is ~**1×** by design (not a measured figure).

### Predictable P99

Redis P99 spikes when AOF rewrite runs. RocksDB P99 spikes during compaction. VeltrixDB's three-tier admission-controlled GC enforces a ceiling: GC is rate-limited proportional to read latency, and emergency GC bypasses pauses before garbage can accumulate.

In the YCSB run above (100M operations): **zero errors and zero GC emergency events**.

---

## How it works

**8192 shards, FNV-1a routing.** Each key hashes (FNV-1a & 0x1FFF) into 1 of 8192 shards. With 8 NVMe disks, shard `N` routes to disk `N % 8`. All 8 disks write in parallel — no single hot lock.

**Values on NVMe, index in DRAM.** The in-memory index holds a 64-byte record per key (disk offset, shard, size, TTL, version) — ~142 B/key of RAM in practice with the native index, measured at 5M keys, plus ~90 B/key for the ordered index unless it is disabled. Value bytes go directly to the per-disk append-only VLog. A cache hit is a DRAM lookup (**~92 ns** since the cache was sharded; it was 711 ns when a single mutex fronted it). A cache miss is one NVMe random read (~400 µs).

**Adaptive group-commit WAL.** A background flusher amortizes `fdatasync` across writers. The window (default 15 ms) is only an upper bound: a lone writer is synced immediately, and concurrent writers are synced together after an idle gap of about one fdatasync (`--group-commit=fixed` restores the full-window wait). Records are binary with a CRC32C over the whole record (replay still reads legacy text WALs). One `write(2)` and one `fdatasync` per batch instead of per write, and every write is acknowledged only after the sync that covers it.

**Vector, full-text and hybrid search.** HNSW vector indexes (float32, int8 or product-quantized, graph on the heap or in a mapped file), a BM25 inverted index and reciprocal-rank hybrid search, filtered by the KV record with the same id, fanned out across cluster nodes. Vectors and documents persist as ordinary keys; the indexes are rebuilt from them at startup. See [docs/vector-search.md](docs/vector-search.md).

**LIRS cache.** Scan-resistant eviction: large sequential reads don't evict your hot keys. Small values (≤256 B) get higher eviction priority, keeping the working set in RAM even under mixed workloads.

**C++ acceleration (cgo builds).** With `CGO_ENABLED=1` the Index Vault moves off the Go heap into C++ shard tables (full GC with 5M keys: 21 ms → 0.27 ms; RSS 168 → 142 B/key), and an opt-in `io_uring` bridge (`VELTRIXDB_URING_BRIDGE=on|sqpoll`, Linux, default off) can submit VLog batch writes. The VLog read path and the LIRS cache stay in Go. `--net=cpp|uring|poll` swaps the Go listener for a C++ event-loop front-end (binary PUT GET DEL PING MPUT MGET, standalone mode); reads that need disk are handed to goroutines so they never stall a loop. Compare configurations with `scripts/net-bench.sh` ([BENCHMARKING.md](BENCHMARKING.md#network-front-ends-and-storage-configurations-net-benchsh)).

> The Docker image is built `CGO_ENABLED=1`. The benchmarks above predate that and are the pure-Go path. An ART index and a priority io_uring scheduler exist under `cpp/` but have no Go call site and do not run; see [cpp/README.md](cpp/README.md).

---

## Quick start

```bash
# Docker — no build required
docker run -p 9000:9000 ghcr.io/veltrixdb/veltrixdb:latest

# Test with netcat
echo -e "PUT hello world\nGET hello\nPING" | nc localhost 9000
```

```bash
# Build from source (macOS and Linux; CGO_ENABLED=0 for a pure-Go build)
go build ./...
go run ./cmd/server -addr :9000 -data ./dev-data -cache 256
```

```bash
# 8-disk production setup
./veltrixdb -addr :9000 \
  -data-dirs /mnt/nvme0,/mnt/nvme1,/mnt/nvme2,/mnt/nvme3,\
/mnt/nvme4,/mnt/nvme5,/mnt/nvme6,/mnt/nvme7 \
  -cache 65536
```

---

## Client SDKs

Six languages, same binary protocol, connection pooling built in.

| SDK | Install |
|-----|---------|
| **Go** | `go get github.com/VeltrixDB/veltrixdb-client-go` |
| **Python** | `pip install veltrixdb` |
| **Node.js** | `npm install veltrixdb-client` |
| **Java** | `com.veltrixdb:veltrixdb-client:1.0.0` |
| **Rust** | `cargo add veltrixdb-client` |
| **C++** | header-only — copy `cpp/include/veltrixdb.hpp` |

`MPUT` / `MGET` batches are 50× faster than individual calls. Use them.

```python
import veltrixdb
db = veltrixdb.Client(host="localhost", port=9000)
db.put("user:1001", "alice")
print(db.get("user:1001"))  # alice
```

---

## Features

| | |
|--|--|
| **Storage** | WiscKey KV-separation, 8192-shard index, LIRS cache, append-only VLog |
| **Durability** | Group-commit WAL (binary, CRC32C-checksummed records), fdatasync amortization, crash recovery via WAL replay |
| **Atomic ops** | CAS, INCR, DECR, SETNX — shard-locked RMW, safe under concurrency |
| **Data types** | Keys with TTL, hash fields with per-field TTL, namespaces |
| **Vector search** | HNSW (cosine), multiple namespaces, optional int8 quantization (~4× less vector RAM, exact re-rank from disk), metadata filters, background tombstone compaction |
| **Full-text / hybrid** | BM25 inverted index (Unicode tokenizer), hybrid vector + text search fused by Reciprocal Rank Fusion; searches fan out across cluster nodes |
| **Replication** | Raft consensus, async / quorum / strong modes, anti-entropy |
| **Transactions** | Optimistic MVCC with vector clocks |
| **Security** | AES-256-GCM at-rest encryption, RBAC, mTLS, append-only audit log |
| **Quotas** | Per-namespace rate limiting (token bucket) + key-count caps |
| **CDC** | In-process change data capture, `repl-ship` for cross-process forwarding |
| **Backup** | Full + incremental chains, S3 + GCS upload/restore |
| **Observability** | 60+ Prometheus metrics, web dashboard, liveness/readiness probes |
| **Compression** | Per-record zstd, 256 B threshold, transparent on read |
| **Scrubber** | Background CRC32C integrity validation at configurable MB/s |

---

## Kubernetes

First-class Kubernetes support: Helm chart, CRD operator, and a cloud-agnostic NVMe provisioner.

```bash
# Helm
helm repo add veltrixdb https://charts.veltrixdb.io
helm install veltrixdb veltrixdb/veltrixdb \
  --namespace veltrixdb --create-namespace

# Operator
kubectl apply -f VeltrixDB-Kubernetes-Operator/config/crd/bases/
kubectl apply -f VeltrixDB-Kubernetes-Operator/config/manager/manager.yaml
```

**What's included:**
- `StatefulSet` with stable hostnames per node
- `nvme-prep` DaemonSet — formats XFS, handles kubelet mount-namespace isolation on GKE/EKS/AKS
- `StorageClass` for local NVMe PersistentVolumes
- `ServiceMonitor` for Prometheus scraping
- `PodDisruptionBudget` (`minAvailable: 2`) for safe rolling upgrades
- A `PrometheusRule` — the repo's copy, `monitoring/prometheus-rules.yaml`, has 10 alerts (write throttle, GC stalled, VLog read errors, scrub corruption, low cache hit rate, read P99, node failed, cluster shrank, disk full, restart). The chart is published separately; check its own rules for anything beyond these.

> **GKE requirement**: create node pools with `--local-ssd-interface=NVME`. Without it, GKE merges all SSDs into one RAID-0 device and you lose the parallel I/O benefit.

The Operator handles rolling upgrades, auto-reshard on replica count changes, and self-healing pod replacement.

---

## Operator CLI

```bash
go build -o veltrix ./cmd/veltrix

veltrix status          # cluster health, ops/s, GC state
veltrix compaction      # per-disk GC ratio (color-coded)
veltrix nodes           # topology — role, Raft term, replication lag
veltrix top --watch 2   # live refreshing dashboard
veltrix put mykey val   # write a key
veltrix get mykey       # read a key
veltrix backup /dest    # trigger full backup
veltrix --help
```

---

## Server flags

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `:9000` | TCP listen address |
| `-data` | — | Single data directory |
| `-data-dirs` | — | Comma-separated NVMe disk paths |
| `-cache` | `256` | LIRS cache size in MB |
| `-group-commit` | `adaptive` | `adaptive`: flush windows are upper bounds (lone writer synced at once); `fixed`: every batch waits the full window |
| `-wal-flush-window-ms` | `15` | WAL group-commit window (upper bound in adaptive mode) |
| `-vlog-flush-window-ms` | `15` | VLog flush window (keep equal to WAL) |
| `-wal-format` | `binary` | WAL record encoding; `text` only to roll back to a pre-binary build (run once, stop cleanly) |
| `-net` | `go` | Network front-end: `go` \| `cpp` \| `uring` \| `poll` (C++ front-ends are opt-in, experimental, binary subset only, standalone without `-auth-config`) |
| `-net-threads` | NumCPU | Event loops for the C++ front-end |
| `-pprof-addr` | — (off) | CPU/heap/trace profiles on a separate listener |
| `-encrypt-at-rest` | `false` | AES-256-GCM (key via `VELTRIXDB_ENCRYPTION_KEY`) |
| `-tls-cert` / `-tls-key` | — | TLS certificate and key |
| `-auth-config` | — | Path to auth config JSON |
| `-audit-log` | — | Append-only JSONL audit log path |
| `-read-heavy` | `false` | 400 GB cache + extended GC interval preset |
| `-raw-vlogs` | — | Raw NVMe block devices (Linux + CAP_SYS_RAWIO) |
| `-scrub-mb-per-sec` | `50` | Background CRC scrubber bandwidth |
| `-mode` | `standalone` | Deployment mode: `standalone` \| `raft` \| `replicated` |
| `-node-id` | `node-1` | Node ID in the cluster |
| `-peers` | — | Cluster peers: `id@host:port,...` (host:port = peer's `-addr`) |
| `-consistency` | `eventual` | Replicated-mode write consistency: `eventual` \| `quorum` \| `strong` |
| `-raft-addr` / `-repl-addr` / `-gossip-addr` | derived | Override inter-node listeners (default: client port +2 / +1 / +3) |
| `-cluster-tls-cert` / `-cluster-tls-key` / `-cluster-tls-ca` | — | Inter-node (Raft/replication) TLS |
| `-cluster-mtls` | `false` | Require + verify peer client certs (mutual TLS) |
| `-cluster-secret-file` | — | Shared secret signing (HMAC-SHA256) all transfer-listener traffic: key migration and distributed search. Env `VELTRIXDB_CLUSTER_SECRET` if unset |
| `-search-fanout` | `true` | Distributed modes: run searches, `QUERY` and `IDXQUERY` on every node and merge |
| `-search-timeout-ms` | `2000` | Per-peer timeout for one distributed search phase |
| `-search-allow-partial` | `false` | Answer a search without peers that failed, and during the startup index rebuild (default: fail and say why) |
| `-linearizable-reads` | `false` | Raft mode: reads go through the ReadIndex fence (followers redirect) |
| `-auto-rebalance` | `true` | Distributed modes: migrate keys to their new owners on membership changes |

---

## Deployment modes

VeltrixDB serves every request from local state by default. The distributed
layer is opt-in via `-mode` and is wired into the actual serving path (every
PUT/DELETE/MultiPut/atomic/TXN goes through a write coordinator):

| Mode | Writes | Consistency guarantee |
|------|--------|-----------------------|
| `standalone` (default) | Local engine only. Identical to the historical single-node behaviour. | Single-node linearizable. |
| `raft` | Quorum-committed through a Raft log, applied on all nodes via a storage-backed state machine. Non-leaders return a `MOVED <leader>` redirect. | **Linearizable writes.** Reads are local → possibly stale (no linearizable read path claimed). |
| `replicated` | Local write + primary-copy replication; `-consistency` sets the ACK point. | Durability across N copies; **not** linearizable under concurrent writers. Reads local. |

`-consistency` (replicated mode): `eventual` ACKs after the local write;
`quorum` ACKs after a majority of replicas apply it; `strong` ACKs after all
replicas apply it. `quorum`/`strong` return a clear error (timeout / quorum-not-
reached) when the required copies are unreachable.

```bash
# 3-node raft cluster (run on each host, same --peers value)
veltrixdb -mode raft -node-id n1 -addr 127.0.0.1:9000 \
  -peers n1@127.0.0.1:9000,n2@127.0.0.1:9100,n3@127.0.0.1:9200
```

The bundled cluster-aware client (`client.NewClient`) discovers topology
(`TOPOLOGY` command / `/admin/cluster`), routes each key with the same
consistent hash the server uses, and follows `MOVED` redirects to the leader.

Namespace, hash-field, list/set, vector and text writes route through the
coordinator in every mode (see [ARCHITECTURE.md](ARCHITECTURE.md)).
Vector, text and hybrid searches, `QUERY` and `IDXQUERY` fan out to every
node, and `IDXCREATE` / `IDXDROP` replicate in both raft and replicated mode.
Node-to-node traffic (key migration and distributed search) can be signed
with a shared secret: `--cluster-secret-file` (or `VELTRIXDB_CLUSTER_SECRET`)
on every node; without it, or mTLS, that listener is unauthenticated and the
server logs a warning.

---

## Wire protocol

### Text (nc / telnet)
```
PUT key value   → OK
GET key         → value  (or ERR)
DEL key         → OK
PING            → PONG
INFO            → keys=N writes=N reads=N ...
AUTH user pass  → OK
QUIT            → BYE

VCREATE ns dim [QUANT none|int8|pq] [PQM m] [PQTRAIN n]
        [GRAPH memory|disk]                                  → OK
VSET id [NS ns] f1 f2 ...                                    → OK
VSEARCH k [NS ns] [EF n] [FILTER field op value] f1 f2 ...   → "id score" lines, END
VDEL id [NS ns]                                              → OK

TSET id [NS ns] TEXT free text ...                           → OK
TDEL id [NS ns]                                              → OK
TSEARCH k [NS ns] [FILTER field op value] QUERY free text    → "id score" lines, END
HSEARCH k [NS ns] [EF n] [ALPHA a] [CAND n] [FILTER field op value]
        [VEC f1 ... fn] QUERY free text                      → "id rrf-score" lines, END
```

**Vectors.** `NS` defaults to `default`; a namespace's dimension is fixed by
its first `VSET` or by `VCREATE`. The full float32 vectors are always
persisted in the VLog; the RAM copy can be smaller:

| `VCREATE` option | RAM per vector (768-dim, measured) | Search |
|--|--|--|
| (default) float32 | ~3.4 KB | exact scores |
| `QUANT int8` | ~1.1 KB | top 4 × k graph candidates re-ranked from disk |
| `QUANT pq [PQM m]` | ~0.5 KB (m bytes of codes, default m = dim/8) | the whole beam re-ranked from disk |
| `QUANT pq GRAPH disk` | ~0.4 KB heap + ~0.2 KB in a mapped file | as pq; layer-0 edges live in a file the kernel can page to NVMe |

A PQ namespace stores float32 until it holds `PQTRAIN` vectors (default
10,000), then trains its codebook in the background and re-encodes. Running
`VCREATE` on an existing namespace with other settings re-encodes it. The
graph is rebuilt from the persisted vectors at startup (it is not saved).

**Text.** `TSET` indexes a document for BM25 (lowercased letter/digit runs, no
stemming or stop words). One id names one record across all of these: `PUT
doc-42 {"lang":"en"}`, `VSET doc-42 ...` and `TSET doc-42 ...` describe the
same thing, and `HSEARCH` fuses the vector and text rankings of a namespace by
reciprocal rank (`ALPHA` = vector weight, default 0.5; `CAND` = candidates per
list, default max(50, 4k)). Either half may be omitted.

**Filters.** `FILTER` is a `QUERY` predicate (`= != > < >= <= contains`) on the
ordinary KV record whose key equals the vector id — e.g. `PUT doc-42
{"lang":"en"}` next to `VSET doc-42 ...`. An `=` filter on a field with an
`IDXCREATE` index is served from the index (exact scan when ≤ 2048 ids
match); other filters are checked during the graph walk. The same filter
works on `TSEARCH` and `HSEARCH`.

**Clusters.** Search writes go through the normal replicated / Raft write
path. The ring places a record's vector, text and index entries on the
record's own node, so after a rebalance each node filters its own records;
searches run on every non-failed node and are merged (BM25 is scored with
cluster-wide statistics). `--search-fanout=false` keeps searches local;
`--search-timeout-ms` bounds each peer, and a peer that misses it is left out
of the result.

### Binary (used by all SDKs, auto-detected)

With `-net=cpp|uring|poll` only the binary PUT GET DEL PING MPUT MGET commands are served; everything else needs the default `-net=go`.

```
Request:  [1B cmd][2B keyLen][4B valLen][key][value]
Response: [1B status][4B payloadLen][payload]

Single:  0x01=PUT  0x02=GET  0x03=DEL  0x04=PING  0x05=INFO
Batch:   0x06=MPUT 0x07=MGET
Atomic:  0x18=CAS  0x19=INCR 0x1A=DECR 0x1B=SETNX
Vector:  0x26=VSET 0x27=VSEARCH (namespace "default")
         0x08=VSETNS 0x1E=VSEARCHX (namespace, ef, filter) 0x1F=VDEL
Search:  0x1C=SEARCH, sub-op in the 2-byte field: 1=VCREATE 2=TSET 3=TDEL
         4=TSEARCH 5=HSEARCH (layouts in cmd/server/ext_ops.go)
Status:  0x00=OK   0x01=ERR  0x02=NOT_FOUND
```

---

## Backup

```bash
go build -o veltrixdb-backup ./cmd/backup

# Full local backup
veltrixdb-backup full --data-dirs=/data --dest=/backup/2026-05-26

# Upload to S3
veltrixdb-backup upload --src=/backup/2026-05-26 \
  --provider=s3 --bucket=my-bucket --region=us-east-1

# Restore (engine must be stopped)
veltrixdb-backup restore --chain=/backup/full,/backup/inc1 --data-dirs=/data-new
```

---

## Observability

```bash
curl http://localhost:2112/metrics   # Prometheus scrape endpoint
curl http://localhost:2112/healthz   # liveness probe
curl http://localhost:2112/readyz    # readiness probe
open http://localhost:2112/admin/ui  # web dashboard
```

Key metrics to watch:

| Metric | Healthy value |
|--------|---------------|
| `veltrixdb_storage_writes_total` / `reads_total` | growing |
| `veltrixdb_cache_hits_total` / `misses_total` | hit rate > 90% |
| `veltrixdb_storage_write_admission_throttles_total` | 0 |
| `veltrixdb_vlog_garbage_ratio` | < 0.30 |
| `veltrixdb_vlog_gc_emergency_runs_total` | 0 |

---

## Admin API

```bash
curl    http://localhost:2112/admin/stats       # engine snapshot (JSON)
curl    http://localhost:2112/admin/version     # schema version
curl -X POST http://localhost:2112/admin/checkpoint  # force WAL checkpoint
curl    http://localhost:2112/admin/cdc?prefix= # live CDC events (NDJSON)
curl    http://localhost:2112/admin/cluster     # topology: role, raft term/leader, peers, epoch, replica lag
```

---

## Honest limitations

These are real gaps. We'd rather you know them upfront:

- **No Redis protocol (RESP).** You can't point a Redis client at VeltrixDB yet. RESP compatibility is on the roadmap — once it ships, migration requires only a connection-string change.
- **No managed cloud offering.** Self-hosted only today. Managed service is planned.
- **Range scans cost memory on every write.** `RANGE` and `SCANCUR` are served by an ordered skiplist of all live keys (~90 B/key resident). `--disable-ordered-index` gives that memory back, and then both commands return an error.
- **CDC is in-process only.** Events lost if `repl-ship` is down. Durable WAL-tail mode is future work.
- **Raft reads are local by default (possibly stale on followers).** `raft` mode gives linearizable *writes* (quorum commit); reads are served from local applied state. A newly elected leader first applies everything its predecessor committed, so reads on the leader include every acknowledged write. For linearizable reads use `--linearizable-reads` (ReadIndex fence; followers redirect to the leader).
- **Replicated mode is not linearizable.** `replicated` mode is primary-copy replication for durability across copies; it has no single-writer ordering, so concurrent writers to the same key are not linearizable. Use `raft` mode when you need write linearizability.
- **Distributed searches ask every node.** Each vector / text / hybrid search, `QUERY` and `IDXQUERY` runs on all non-failed nodes, so its cost grows with the cluster. If a peer does not answer within `--search-timeout-ms`, the request fails and names it — until the failure detector marks it failed — unless `--search-allow-partial` is set.
- **Search indexes live in RAM and are rebuilt at every start.** Vector codes (float32 / int8 / PQ), graph nodes and the BM25 inverted index are in memory (layer-0 graph edges can go to a mapped file with `GRAPH disk`); the full-precision vectors and document text stay on NVMe. Searches are refused until the rebuild finishes. Tested to 100K real vectors; 10M+ is untested.
- **No same-hardware comparison yet** with Aerospike, ScyllaDB or other vector databases. `bench/compare` is the harness; its Aerospike / ScyllaDB paths are compile-checked only.

See [docs/redis-comparison.md](docs/redis-comparison.md) for a full feature-by-feature comparison.

---

## Documentation

| | |
|--|--|
| [ARCHITECTURE.md](ARCHITECTURE.md) | System design: sharding, write path, read path, admission control |
| [PERFORMANCE.md](PERFORMANCE.md) | Tuning guide for your specific workload |
| [BENCHMARKING.md](BENCHMARKING.md) | Bench harness, reference numbers, pass/fail gates |
| [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) | Every measured number, with the conditions it was measured under |
| [docs/vector-search.md](docs/vector-search.md) | Vector, full-text and hybrid search: commands, memory layouts, clusters, measured recall / latency / RAM |
| [bench/compare](bench/compare/README.md) | Same-hardware harness for VeltrixDB vs Aerospike vs ScyllaDB (YCSB) and vector benchmarks |
| [docs/redis-comparison.md](docs/redis-comparison.md) | When to use Redis vs VeltrixDB |
| [docs/TESTING_GUIDE.md](docs/TESTING_GUIDE.md) | Run every feature test |
| [docs/DR_RUNBOOK.md](docs/DR_RUNBOOK.md) | Disaster recovery procedures |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to contribute |

---

## Contributing

Bug reports, feature requests, and pull requests are welcome.

```bash
git clone https://github.com/VeltrixDB/veltrixdb
cd veltrixdb
go build ./...
go test ./...           # unit tests
./tests/e2e/run_all.sh  # e2e tests (requires a running server)
./scripts/bench.sh      # benchmark with pass/fail gates
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, invariants you must not break, and the PR process.

---

## License

Apache 2.0. See [LICENSE](LICENSE).

Built by [Shubham Sharma](https://github.com/shubhamsharma).
