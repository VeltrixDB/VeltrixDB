# VeltrixDB Testing Guide

---

## Run the full test suite

```bash
# Unit tests (all packages)
go test ./... -timeout 120s -count=1

# With verbose output
go test -v ./storage/... -timeout 120s

# Specific package
go test ./consensus/...    # Raft election, log replication
go test ./cluster/...      # failure detection, partition map
go test ./replication/...  # async, quorum, strong modes
```

---

## Integration tests (3-node cluster)

These spin up real server processes and test crash recovery, failover, and backup/restore.

```bash
# Build the server binary first
go build -o /tmp/veltrixdb ./cmd/server
go build -o /tmp/veltrixdb-backup ./cmd/backup

# Run integration tests
VELTRIX_SERVER_BIN=/tmp/veltrixdb \
VELTRIX_BACKUP_BIN=/tmp/veltrixdb-backup \
  go test -v ./tests/integration/... -timeout 300s
```

Tests covered: single-node CRUD, crash recovery (SIGKILL), graceful restart (SIGTERM), health/metrics endpoints, node failover, leader election, node addition, backup and restore.

---

## E2E shell scripts

Each script starts its own server and cleans up after itself.

```bash
# Run all e2e tests
./tests/e2e/run_all.sh

# Or run individual scenarios
./tests/e2e/test_basic_ops.sh        # PUT / GET / DEL / PING
./tests/e2e/test_binary_protocol.sh  # raw binary framing
./tests/e2e/test_persistence.sh      # data survives restart
./tests/e2e/test_ttl.sh              # key expiry
./tests/e2e/test_batch_ops.sh        # MPUT / MGET throughput
./tests/e2e/test_metrics.sh          # Prometheus endpoint
./tests/e2e/test_auth.sh             # AUTH command
./tests/e2e/test_namespaces.sh       # NSPUT / NSGET / NSSCAN
./tests/e2e/test_node_failover.sh    # SIGKILL one node, verify reads
./tests/e2e/test_node_addition.sh    # add a node mid-run
./tests/e2e/test_backup_restore.sh   # full backup + restore cycle
./tests/e2e/test_stress.sh           # sustained load, no errors
```

---

## Bench harness (pass/fail gates)

```bash
# Quick CI run (200K keys)
NUM_KEYS=200000 CACHE_MB=512 ./scripts/bench.sh

# Production scale (10M keys, 8 NVMe disks)
DATA_DIRS=/mnt/nvme0,...,/mnt/nvme7 CACHE_MB=409600 NUM_KEYS=10000000 \
  ./scripts/bench.sh
```

The bench exits 0 only if both gates pass:
- **Density gate**: `bytes/record ≤ 1.2 × (24 + value_size)` — packing is working
- **GC emergency gate**: `gc_emergency_runs Δ == 0` — no GC death spirals

See [BENCHMARKING.md](../BENCHMARKING.md) for full details.

---

## What each test package covers

| Package | Tests |
|---------|-------|
| `storage/` | engine CRUD, WAL group-commit, LIRS cache, bloom filters, compression, atomic ops (CAS/INCR/DECR/SETNX), transactions, secondary indexes, quotas, CDC broker, backup, tiered storage |
| `consensus/` | Raft election, log replication, quorum commit, persistence, heartbeat, stale vote rejection |
| `cluster/` | failure detection, partition map assignment, rebalance, consistent hashing |
| `replication/` | async/quorum/strong write, vector clocks, anti-entropy, tombstone GC, replica lag |
| `tests/integration/` | 3-node cluster, crash recovery, failover, backup/restore |
| `tests/e2e/` | full stack, binary protocol, namespaces, auth, stress |
