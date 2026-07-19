# VeltrixDB

[![CI](https://github.com/VeltrixDB/veltrixdb/actions/workflows/ci.yml/badge.svg)](https://github.com/VeltrixDB/veltrixdb/actions/workflows/ci.yml)
[![Go 1.19+](https://img.shields.io/badge/Go-1.19+-00ADD8?logo=go)](https://golang.org)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![GitHub Stars](https://img.shields.io/github/stars/VeltrixDB/veltrixdb?style=social)](https://github.com/VeltrixDB/veltrixdb/stargazers)

**NVMe-native distributed key-value database. 427K reads/s measured (YCSB). 1.0× write amplification. Kubernetes-first.**

Storing 1 billion keys in Redis costs ~$3,000–5,000/month in RAM.  
The same dataset in VeltrixDB fits in ~160 GB of NVMe — roughly $300–500/month.  
P99 latency that doesn't spike during compaction.

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

**Single node (Linux NVMe):**

| Operation | P50 | P99 |
|-----------|-----|-----|
| GET — cache hit | 0.05 ms | 0.28 ms (~1.4M reads/s) |
| PUT — 10 ms flush window | 5 ms | 10.2 ms |
| PUT — 5 ms window, 512 workers | 2.6 ms | 5.2 ms (~102K writes/s) |
| MultiPut 1024 entries | — | ~9.5 ms (~426K entries/s) |

Full methodology: [BENCHMARKING.md](BENCHMARKING.md)

---

## Why VeltrixDB

### The problem with Redis at scale

Redis is in-memory. At 1 billion keys with 128-byte values you need ~250 GB of RAM. In GCP that's an `r6g.16xlarge`-class node: ~$3,000–5,000/month. Cloud RAM costs 15–20× more per GB than NVMe SSD.

VeltrixDB stores values on NVMe with the index and hot data in DRAM. The same 1 billion keys fit in ~160 GB of NVMe. Three nodes cost ~$300–500/month.

### The problem with LSM trees (RocksDB, LevelDB)

LSM compaction rewrites full key-value records to merge sorted runs. Typical write amplification is **10–30×** — every byte you write eventually lands on disk 10–30 times. That burns SSD write endurance and adds latency spikes during heavy compaction.

VeltrixDB uses [WiscKey](https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu) KV-separation: **values are written once to an append-only Value Log on NVMe and never rewritten**. Compaction only GCs dead space in the VLog. Write amplification is ~**1.0×**.

### Predictable P99

Redis P99 spikes when AOF rewrite runs. RocksDB P99 spikes during compaction. VeltrixDB's three-tier admission-controlled GC enforces a ceiling: GC is rate-limited proportional to read latency, and emergency GC bypasses pauses before garbage can accumulate.

In the YCSB run above (100M operations): **zero errors and zero GC emergency events**.

---

## How it works

**8192 shards, FNV-1a routing.** Each key hashes (FNV-1a & 0x1FFF) into 1 of 8192 shards. With 8 NVMe disks, shard `N` routes to disk `N % 8`. All 8 disks write in parallel — no single hot lock.

**Values on NVMe, index in DRAM.** The in-memory index stores only a 64-byte pointer per key (disk offset, shard, size, TTL, version). Value bytes go directly to the per-disk append-only VLog. A cache hit is a DRAM lookup (~220 ns). A cache miss is one NVMe random read (~400 µs).

**Group-commit WAL.** A background flusher amortizes `fdatasync` across all writers within a configurable window (default 10 ms). One `fdatasync` per batch instead of per write — 10–100× write throughput improvement at the cost of at most one window of durability latency.

**LIRS cache.** Scan-resistant eviction: large sequential reads don't evict your hot keys. Small values (≤256 B) get higher eviction priority, keeping the working set in RAM even under mixed workloads.

**C++ io_uring on Linux.** The optional C++ layer uses `io_uring` SQPOLL + `O_DIRECT` for VLog reads, an Adaptive Radix Tree (ART) index on 2 MB hugepages, and NUMA-aware thread pinning. The Go layer is cross-platform; the C++ layer adds ~25 µs off the NVMe read P99 on production hardware.

---

## Quick start

```bash
# Docker — no build required
docker run -p 9000:9000 ghcr.io/veltrixdb/veltrixdb:latest

# Test with netcat
echo -e "PUT hello world\nGET hello\nPING" | nc localhost 9000
```

```bash
# Build from source (Go only — works on macOS and Linux)
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
| **Durability** | Group-commit WAL, fdatasync amortization, crash recovery via WAL replay |
| **Atomic ops** | CAS, INCR, DECR, SETNX — shard-locked RMW, safe under concurrency |
| **Data types** | Keys with TTL, hash fields with per-field TTL, namespaces |
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
- 22 `PrometheusRule` alerts (slow disk, GC emergency, corruption detected, replication lag)

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
| `-cache` | `1024` | LIRS cache size in MB |
| `-wal-flush-window-ms` | `10` | WAL group-commit window |
| `-vlog-flush-window-ms` | `10` | VLog flush window (keep equal to WAL) |
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

**Not yet routed through the distributed layer:** namespace, hash-field,
vector, secondary-index, and query operations apply locally only in raft/
replicated modes. Plain KV + atomic + TXN are fully wired.

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
```

### Binary (used by all SDKs, auto-detected)
```
Request:  [1B cmd][2B keyLen][4B valLen][key][value]
Response: [1B status][4B payloadLen][payload]

Single:  0x01=PUT  0x02=GET  0x03=DEL  0x04=PING  0x05=INFO
Batch:   0x06=MPUT 0x07=MGET
Atomic:  0x18=CAS  0x19=INCR 0x1A=DECR 0x1B=SETNX
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
| `veltrixdb_writes_total` / `reads_total` | growing |
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
- **No native range/prefix scans.** Workaround: use namespaces + NSSCAN, or secondary indexes.
- **CDC is in-process only.** Events lost if `repl-ship` is down. Durable WAL-tail mode is future work.
- **Raft reads are local (possibly stale).** `raft` mode gives linearizable *writes* (quorum commit) but reads are served from local applied state — there is no read-index / lease-read path yet.
- **Replicated mode is not linearizable.** `replicated` mode is primary-copy replication for durability across copies; it has no single-writer ordering, so concurrent writers to the same key are not linearizable. Use `raft` mode when you need write linearizability.
- **Non-KV ops are node-local in cluster modes.** Namespace, hash-field, vector, secondary-index, and query operations are not routed through Raft/replication yet — only plain KV + atomic + TXN are.

See [docs/redis-comparison.md](docs/redis-comparison.md) for a full feature-by-feature comparison.

---

## Documentation

| | |
|--|--|
| [ARCHITECTURE.md](ARCHITECTURE.md) | System design: sharding, write path, read path, admission control |
| [PERFORMANCE.md](PERFORMANCE.md) | Tuning guide for your specific workload |
| [BENCHMARKING.md](BENCHMARKING.md) | Bench harness, reference numbers, pass/fail gates |
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
