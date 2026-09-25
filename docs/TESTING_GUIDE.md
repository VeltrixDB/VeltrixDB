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

# Race detector — CI runs this as a blocking gate across four groups
go test -race -timeout 600s -count=1 ./storage/...
```

### cgo vs pure Go

`go test` builds with cgo by default, so `storage/` runs on the off-heap
native index (`storage/native_index.cpp`, cgo + Go >= 1.21, macOS included)
and `netfront/` is compiled. On Linux a cgo build also links liburing. Most CI
jobs run `CGO_ENABLED=0`, which uses the Go map index and skips the
cgo-tagged tests (`storage/index_table_native_test.go`,
`netfront/netfront_test.go`).

| Knob | Effect |
|------|--------|
| `CGO_ENABLED=0` | Pure Go: map index, no C++ |
| `VELTRIXDB_INDEX=map` / `=native` | Force the index implementation on a cgo build |
| `VELTRIXDB_DISABLE_CGO_ENGINE=1` | No C++ batch engine or io_uring bridge. Also selects the map index unless `VELTRIXDB_INDEX=native` |
| `VELTRIXDB_URING_BRIDGE=on` | Exercise the opt-in io_uring VLog write bridge (Linux cgo). Use `on`, not `sqpoll`, in tests: many short-lived SQPOLL engines starve a runner |

The native index's correctness oracle is `TestEntryTable_NativeMatchesMap`,
which compares it with the map under forced hash collisions and edge-case keys.

**After editing C++ that a `storage/cgo_*_linux.cpp` shim `#include`s from
`cpp/src/`, run `go test -a`.** The build cache does not track included
files, so a plain `go test` can silently re-run the stale object (CLAUDE.md
invariant 44). `storage/native_index.cpp` and `netfront/netfront.cpp` live in
their package directories and are tracked normally.

### Network front-end tests (`netfront/`)

```bash
# poll() backend — runs anywhere cgo does, including macOS
go test -v -count=1 ./netfront/...

# io_uring backend — Linux with liburing; fails if io_uring is unavailable
VXNF_TEST_BACKEND=uring go test -v -count=1 -timeout 600s ./netfront/...
```

These cover per-connection ordering (`TestNetfront_PipelineOrder`), deferred
disk reads (`TestNetfront_DeferredReadBlocksLaterBatches`,
`TestNetfront_DeferredReadNoHeadOfLine`) and shutdown with writes in flight.
The `Net front-end` workflow (`.github/workflows/net-bench.yml`) runs them on
Linux with `uring`, with `poll`, and with `uring` under `-race`.

### What CI runs

| Job | Build | Scope |
|-----|-------|-------|
| node-1 … node-4 | `CGO_ENABLED=0` | build, vet, unit, cluster, integration |
| node-5-race | cgo, `VELTRIXDB_DISABLE_CGO_ENGINE=1`, `VELTRIXDB_INDEX=native` | `-race` in four groups: storage, server, cluster, rest |
| node-6-cpp | CMake + cgo shims + `scripts/build.sh` | `./storage/...` with cgo and `VELTRIXDB_URING_BRIDGE=on` |
| Net front-end | cgo, liburing | `./netfront/...` tests plus `scripts/net-bench.sh` |

### Writing a new test that needs an engine

Build the config with **`testStorageConfig()`** (`storage/testconfig_test.go`),
never `DefaultStorageConfig()` directly. The production defaults are sized for
a 64-core box with NVMe, and a test inheriting them pays:

- **256 MB+ of Bloom filter, eagerly, per engine** — it is
  `BloomFilterShardBits / 8 × 8192`. Three helpers once got this wrong and
  took the suite to 8 GB peak RSS, which is what killed CI.
- **A 15 ms group-commit window** per write, with nothing to batch, because
  tests write sequentially.
- **A scrubber goroutine per VLog** doing real I/O for the second the engine
  is alive.

If you need a knob the helper doesn't set, set it on the returned config
rather than starting from `DefaultStorageConfig()` again.

### Timings are platform-dependent in a way that matters

The suite runs in ~13 s on macOS and needs tmpfs to run acceptably on Linux
CI. Darwin's `fsync(2)` returns at the drive cache in ~0.02 ms; Linux
`fdatasync` against a real disk is a genuine round trip, and every `Put` does
two. Do not read a local pass as evidence that a test is fast in CI.

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

See [BENCHMARKING.md](../BENCHMARKING.md) for full details, including
`scripts/net-bench.sh`, which compares `--net=go|uring|poll` and storage
configurations on Linux.

---

## What each test package covers

| Package | Tests |
|---------|-------|
| `storage/` | engine CRUD, WAL group-commit, binary/text WAL decoding and crash replay, native vs map index, `GetNoIO`, LIRS cache, bloom filters, compression, atomic ops (CAS/INCR/DECR/SETNX), transactions, secondary indexes, quotas, CDC broker, backup, tiered storage |
| `consensus/` | Raft election, log replication, quorum commit, persistence, heartbeat, stale vote rejection |
| `cluster/` | failure detection, partition map assignment, rebalance, consistent hashing |
| `replication/` | async/quorum/strong write, vector clocks, anti-entropy, tombstone GC, replica lag |
| `netfront/` | C++ front-end: binary ops, batches, pipeline order, deferred reads, many connections (cgo only) |
| `tests/integration/` | 3-node cluster, crash recovery, failover, backup/restore |
| `tests/e2e/` | full stack, binary protocol, namespaces, auth, stress |
