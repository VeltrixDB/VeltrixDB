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
`TestNetfront_DeferredReadNoHeadOfLine`) and shutdown with writes in flight
(`TestNetfront_CloseWithWritesInFlight`). The `Net front-end` workflow
(`.github/workflows/net-bench.yml`) runs them on Linux with `uring`, with
`poll`, and with `uring` under `-race`. It is not part of PR CI: it runs on
manual dispatch or on pushes to the `perf/cpp-native-engine` branch.

### What CI runs

`.github/workflows/ci.yml`, on every push to and PR against `main`:

| Job | Build | Scope |
|-----|-------|-------|
| node-1-storage | `CGO_ENABLED=0` | gofmt, build, vet; `./storage/... ./adminapi/...` (on tmpfs), `./client/... ./security/... ./tracing/...` |
| node-2-cluster | `CGO_ENABLED=0` | `./consensus/...`, `./cluster/...`, `./replication/...` |
| node-3-integration | `CGO_ENABLED=0` | builds `cmd/server` and `cmd/backup`, runs `./tests/integration/...` |
| node-4-paper-changes | `CGO_ENABLED=0` | GC sort, defrag thread pinning, tiering unit tests (`TestGCColdFirstSort`, `TestLockDefragThread`, `TestTierManager`, …) |
| node-5-race | cgo, `VELTRIXDB_DISABLE_CGO_ENGINE=1`, `VELTRIXDB_INDEX=native` | `-race` in four groups: storage, server, cluster, rest |
| node-6-cpp | CMake + cgo shims + `scripts/build.sh` | `./storage/...` with cgo and `VELTRIXDB_URING_BRIDGE=on` |
| node-7-search | `CGO_ENABLED=0` + cgo for `-race` | search regression: recall / ranking gates (`TestSearchQualityGate*`, numbers in the job summary), the search / hook / routing / signing tests under `-race`, and `TestSearchCluster_EndToEnd` on a real 3-process cluster |
| bench-compare | module `bench/compare` | `go vet`, `go test`, `bash -n compare.sh` |
| bench | `CGO_ENABLED=0` | `scripts/bench.sh` (200K keys) gates, plus GC-sort / tiering benchmarks with a ±15 % benchstat regression check against the cached `main` baseline |
| security | `CGO_ENABLED=0` | `govulncheck ./...` (non-blocking: `continue-on-error`) |
| bench-image | Docker | bench image build (pushed on `main` only) |
| Net front-end (`net-bench.yml`, manual / `perf/cpp-native-engine` only) | cgo, liburing | `./netfront/...` tests plus `scripts/net-bench.sh` |

### Nightly hardening (`.github/workflows/nightly.yml`)

| Job | What it checks | Run locally |
|--|--|--|
| real-embeddings | recall@10 gates on GloVe-100 (first 100K words, 500 queries) for float32 / int8 / pq / pq + disk graph | `scripts/ann-dataset.py glove-100-angular /tmp/ann --train 100000 --test 500` then `VELTRIX_ANN_DIR=/tmp/ann go test ./storage -run TestRealEmbeddings -v` |
| search-soak | 4 writers + 4 searchers for 20 min against an oracle: no result that was not live, self-queries and text markers of stable ids found, RSS levels off, exact match after a clean restart | `VELTRIX_SOAK_DURATION=5m go test ./tests/integration -run TestSearchSoak -v` |
| crash-chaos | SIGKILL mid-write, restart, every acknowledged vector / text / record write present and no acknowledged delete back — 10 cycles. Between cycles `settle()` rewrites every id whose write had an unknown outcome at the kill to a fresh, fully acknowledged version, so later cycles check those ids too | `VELTRIX_CHAOS_CYCLES=10 go test ./tests/integration -run TestSearchCrashRecovery -v` |
| perf-tables | adaptive vs fixed group commit (emulated fdatasync), RAM per vector at 768-dim | `VELTRIX_GC_TABLE=1 VELTRIX_VECTOR_MEMORY=1 go test ./storage -run 'TestGroupCommit_LatencyTable|TestVectorMemoryTable' -v` |
| kv-soak | `scripts/soak.sh` (manual dispatch with `kv_soak_hours`) | `SOAK_HOURS=1 ./scripts/soak.sh` |

The workflow runs daily at 01:30 UTC; `kv-soak` runs only on manual dispatch
with `kv_soak_hours` > 0. The soak and chaos tests skip unless their variable
is set, and build the server themselves unless `VELTRIX_SERVER_BIN` points at
a binary (the workflow pre-builds `/tmp/veltrixdb`).

`scripts/ann-dataset.py` needs `h5py` and `numpy`. It sends its own
`User-Agent` (ann-benchmarks.com answers 403 to urllib's default
`Python-urllib/x.y`), makes up to 4 download attempts (waiting 5 s, 10 s,
15 s between them), and downloads to `<name>.hdf5.part`, renamed into place
only when complete, so a failed download never leaves a truncated file for
the CI cache to keep.

The soak test caught its own false positives before it was trusted: a miss
only counts when the id's version is unchanged across the search.

### Search quality gates

`storage/search_quality_test.go` pins recall@10 on a fixed, seeded, clustered
dataset (float32 at default ef and ef=256, int8 after re-rank, an indexed
filter forced onto the graph path, and after deleting 40 %), plus a labelled
BM25 / hybrid corpus. It runs without `-race` (skipped under `-race` and `-short`) and logs the
measured values:

```bash
go test ./storage -run TestSearchQualityGate -v
```

A change that lowers a value below its gate fails CI even if every other test
passes — it is how the int8 path searching with a narrower beam than float32
was found. If a change is meant to move the numbers, update the measured
values in the file's comment together with the gates.

### Writing a new test that needs an engine

Build the config with **`testStorageConfig()`** (`storage/testconfig_test.go`),
never `DefaultStorageConfig()` directly. The production defaults are sized for
a 64-core box with NVMe, and a test inheriting them pays:

- **512 MB of Bloom filter, eagerly, per engine** — it is
  `BloomFilterShardBits / 8 × 8192`, and the default is `1 << 19` bits
  (`testStorageConfig()` uses `1 << 12`: 4 MB). Three helpers once got this wrong and
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
# Optional: pre-build the server (otherwise the tests build it once themselves)
go build -o /tmp/veltrixdb ./cmd/server

# Run integration tests
VELTRIX_SERVER_BIN=/tmp/veltrixdb \
  go test -v ./tests/integration/... -timeout 300s
```

CI also builds `cmd/backup` and sets `VELTRIX_BACKUP_BIN`, but no test reads
it: the backup tests drive `storage.BackupEngine` / `storage.Restore` in
process.

Tests covered: single-node CRUD, ping/info, concurrency, large values, crash recovery (SIGKILL), graceful restart (SIGTERM), health/metrics endpoints (`single_node_test.go`); node failover, leader election, node addition, network partition (`cluster_test.go`); Raft failover, replicated async / strong modes, cluster-client routing (`distributed_test.go`); full, incremental, under-write backup/restore and manifest validation (`backup_restore_test.go`); 3-node distributed search (`search_test.go`); and the env-gated soak / crash-chaos tests (`soak_test.go`).

---

## E2E shell scripts

Each script starts its own server and cleans up after itself. `run_all.sh`
honours `SKIP_STRESS=1` and `SKIP_TTL=1`.

```bash
# Run all e2e tests
./tests/e2e/run_all.sh

# Or run individual scenarios
./tests/e2e/test_basic_ops.sh        # PUT / GET / DEL / PING / INFO
./tests/e2e/test_binary_protocol.sh  # binary protocol via the loadtest binary
./tests/e2e/test_persistence.sh      # data survives restart
./tests/e2e/test_ttl.sh              # key expiry
./tests/e2e/test_batch_ops.sh        # MPUT / MGET throughput
./tests/e2e/test_metrics.sh          # Prometheus endpoint
./tests/e2e/test_auth.sh             # AUTH command
./tests/e2e/test_namespaces.sh       # NSPUT / NSGET / NSDEL / NSDROP / NSSCAN / NSLIST
./tests/e2e/test_node_failover.sh    # single node: SIGKILL / restart cycles, keys survive
./tests/e2e/test_node_addition.sh    # single node: second server on the same data dir
./tests/e2e/test_backup_restore.sh   # POST /admin/checkpoint + data-dir copy + restart
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
| `tests/integration/` | 3-node cluster, Raft / replicated modes, crash recovery, failover, network partition, backup/restore, distributed search, soak / crash chaos |
| `tests/e2e/` | full stack (single node), binary protocol, namespaces, auth, TTL, checkpoint + copy backup, stress |
