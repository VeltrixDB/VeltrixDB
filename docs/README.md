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

## Existing Docs

| Document | What It Covers |
|----------|---------------|
| [DR_RUNBOOK.md](DR_RUNBOOK.md) | Disaster recovery runbook; encryption key rotation; data corruption recovery |
| [SOC2_CONTROLS.md](SOC2_CONTROLS.md) | SOC 2 compliance controls |
| [TESTING_GUIDE.md](TESTING_GUIDE.md) | How to run integration and e2e tests |
| [../cpp/README.md](../cpp/README.md) | What in `cpp/` is actually reachable from Go (most of it is not) |

## Quick Reference

**Where is the code?**

| Topic | Primary File |
|-------|-------------|
| Storage engine | `storage/engine.go` |
| WAL | `storage/wal.go`, `storage/wal_replay.go` |
| Value Log | `storage/vlog.go` |
| In-memory index | `storage/shard.go` |
| Raft consensus | `consensus/raft.go` |
| Cluster partition map | `cluster/partition_map.go` |
| Failure detection + gossip | `cluster/failure_detection.go` |
| Data migration | `cluster/partition_transfer.go` |
| Replication engine | `replication/engine.go` |
| Backup / restore | `storage/backup.go`, `cmd/backup/main.go` |
| TCP server | `cmd/server/main.go` |
| Prometheus metrics | `metrics/prometheus.go` |
| Sharded LIRS cache | `storage/cache_sharded.go` |
| Value transform (compress/encrypt) | `storage/engine.go` — `transformForWrite` |
| Transform-metadata repair | `storage/repair.go`, `cmd/veltrix-repair/main.go` |
| Platform fdatasync split | `storage/fdatasync_linux.go`, `storage/fdatasync_other.go` |
