# VeltrixDB Documentation

Technical documentation for VeltrixDB internals.

## Index

| Document | What It Covers |
|----------|---------------|
| [../ARCHITECTURE.md](../ARCHITECTURE.md) | **Start here** — system diagram, write/read path sequences, shard routing, durability, admission control, GC tiers, the real C++ boundary |
| [storage.md](storage.md) | How data is stored: sharding, WAL, VLog, Index Vault, LIRS cache, defragmentation, crash recovery |
| [replication.md](replication.md) | Raft consensus replication + async Replication Engine; consistency levels; anti-entropy; version vectors |
| [partitioning.md](partitioning.md) | Consistent hash ring; virtual nodes; partition assignment; rebalancing; data migration via TransferAgent |
| [node-lifecycle.md](node-lifecycle.md) | Node failover (leader election timeline); node addition; graceful removal; crash detection and recovery |
| [backup-restore.md](backup-restore.md) | Full and incremental backup; cloud backup (S3/GCS/Azure); restore procedure; backup safety guarantees |
| [vector-search.md](vector-search.md) | Vector, full-text and hybrid search: commands, float32 / int8 / PQ / disk-graph layouts, filters, clusters, restarts, measured recall / latency / RAM |

## Existing Docs

| Document | What It Covers |
|----------|---------------|
| [DR_RUNBOOK.md](DR_RUNBOOK.md) | Disaster recovery runbook; encryption key rotation; data corruption recovery |
| [SOC2_CONTROLS.md](SOC2_CONTROLS.md) | SOC 2 compliance controls |
| [TESTING_GUIDE.md](TESTING_GUIDE.md) | How to run unit, cgo/native-index, `netfront/`, integration and e2e tests; what each CI job runs |
| [../BENCHMARKING.md](../BENCHMARKING.md) | Bench harness gates; `scripts/net-bench.sh`; group-commit table; vector benchmarks |
| [../BENCHMARK_RESULTS.md](../BENCHMARK_RESULTS.md) | Every measured number with its conditions (June 2026 YCSB; October 2026 group commit, YCSB, vectors, stability) |
| [../bench/compare/README.md](../bench/compare/README.md) | Same-hardware harness: VeltrixDB vs Aerospike vs ScyllaDB (go-ycsb), `vecbench` |
| [../cpp/README.md](../cpp/README.md) | What in `cpp/` is actually reachable from Go (most of it is not) |

## Quick Reference

**Where is the code?**

| Topic | Primary File |
|-------|-------------|
| Storage engine | `storage/engine.go` |
| WAL | `storage/wal.go`, `storage/wal_replay.go` |
| WAL record format (binary default, legacy text) | `storage/wal_format.go` |
| Value Log | `storage/vlog.go` |
| In-memory index | `storage/shard.go`, `storage/index_table.go` |
| Off-heap native index (cgo) | `storage/native_index.cpp`, `storage/index_table_native.go` |
| Raft consensus | `consensus/raft.go` |
| Cluster partition map | `cluster/partition_map.go` |
| Failure detection + gossip | `cluster/failure_detection.go` |
| Data migration | `cluster/partition_transfer.go` |
| Replication engine | `replication/engine.go` |
| Backup / restore | `storage/backup.go`, `cmd/backup/main.go` |
| TCP server | `cmd/server/main.go` |
| C++ network front-end (`--net=cpp\|uring\|poll`, opt-in) | `netfront/` |
| Profiling listener (`--pprof-addr`) | `cmd/server/pprof.go` |
| cgo toggles (`VELTRIXDB_DISABLE_CGO_ENGINE`, `VELTRIXDB_URING_BRIDGE`) | `storage/cgo_toggle.go` |
| Prometheus metrics | `metrics/prometheus.go` |
| Sharded LIRS cache | `storage/cache_sharded.go` |
| Value transform (compress/encrypt) | `storage/engine.go` — `transformForWrite` |
| Transform-metadata repair | `storage/repair.go`, `cmd/veltrix-repair/main.go` |
| Platform fdatasync split | `storage/fdatasync_linux.go`, `storage/fdatasync_other.go` |
| Group commit pacing (adaptive / fixed) | `storage/group_commit.go` |
| Vector index (HNSW, int8, PQ, disk graph) | `storage/hnsw.go`, `storage/hnsw_pq.go`, `storage/pq.go`, `storage/vector_index.go`, `storage/vector_adj.go` |
| Full-text and hybrid search | `storage/text_index.go`, `storage/hybrid.go` |
| Search index sync + startup rebuild | `storage/search_hooks.go` |
| Distributed search, cluster signing | `cmd/server/search_fanout.go`, `cluster/partition_transfer.go` |
| Derived-key routing | `cluster/partition_map.go` — `RoutingKey` |
