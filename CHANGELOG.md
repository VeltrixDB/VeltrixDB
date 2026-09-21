# Changelog

All notable changes to VeltrixDB are documented here.

Format: `[version] — YYYY-MM-DD`  
Types: `Added`, `Changed`, `Fixed`, `Performance`, `Breaking`

---

## [Unreleased]

### Fixed

- **At-rest encryption and compression were skipped on every batched write
  path.** `MaybeCompress` / `Encrypt` were called only from
  `StorageEngine.Put`; `multiPutKVSep` — which backs `MPUT`, the
  `WriteBatcher`, and the server's PUT coalescing — wrote raw plaintext to the
  VLog. With `--encrypt-at-rest` on, the paths the docs recommend for
  throughput were the unencrypted ones, and the failure was silent because
  `FlagEncrypted` is per-record so reads never attempted to decrypt. Both
  transforms now go through a single chokepoint, `transformForWrite`, used by
  `Put`, `multiPutKVSep`, and legacy-WAL re-append alike.

- **Compressed and encrypted values were unreadable after a restart.** The WAL
  recorded only the plaintext length and no transform flags, so replay rebuilt
  `IndexEntry.ValueSize` from the plaintext length while the VLog held a blob
  of a different size — `ReadValue` then pulled the wrong byte count and
  failed CRC. Where the lengths happened to match, the missing flags meant
  `Get` returned raw ciphertext. Compression is enabled by default
  (`Compression: "zstd"`), so this affected default deployments for every
  value over the 256-byte threshold; it went unnoticed because every existing
  crash-recovery test used values far below it. The WAL record gained two
  fields (`diskLen|xflags`, now 10 total) and `writeWALCheckpoint` — which had
  the same defect on the clean-shutdown path, plus dropped `FlagPacked` —
  emits them too. Records in the older 6/7/8-field formats still parse.

- **Raw block-device VLog could overwrite live data on restart.** WAL replay is
  asynchronous and the server accepts writes as soon as `NewStorageEngine`
  returns, but `vl.end` was seeded from the rebuilt index only *after* replay
  finished. In raw mode `Stat().Size()` is 0, so until then the cursor sat at
  offset 4096 and any write arriving during warmup reserved offsets on top of
  live records. The cursor is now pre-seeded synchronously from the parsed WAL
  entries (`maxVLogEndFromWAL`) before the engine is handed out; the
  post-replay index-derived seed remains as a safety net.

- **Shutdown race in `VLog.close()` and `WriteAheadLog.close()`.** Both closed
  the file descriptor while the flusher goroutine could still be running its
  final `fdatasync` / `Write` — a data race on `*os.File` and, for the WAL, a
  path where the last batch could fail with "file already closed". Both now
  wait for the flusher to exit before closing. Surfaced by the race detector,
  which no CI job was running.

- **Data race in the `client` package test harness.** `newTextServer` started
  its accept loop before the test assigned `authRequired` / `authUser` /
  `authPass`, so handler goroutines read those fields unsynchronised. Replaced
  by `newTextServerAuth`, which installs credentials before the goroutine
  starts.

- **VLog GC mislabelled oversized relocations as packed.** `compactVLog`
  hardcoded `newPacked: true`, but `VLogBatcher.Stage` falls back to an
  unpacked span for records larger than a 4 KB block. `MarkDead` then
  subtracted header+value instead of the full aligned span, inflating
  `liveBytes` permanently and making `GCRatio` under-report garbage so GC
  fired late. `Stage` now returns whether it packed, and both call sites
  propagate that value.

### Changed

- **WAL record format is now 10 fields**:
  `timestamp|tombstone|key|valueLen|crc32hex|version|vlogOffset|packed|diskLen|xflags`.
  `valueLen` remains the plaintext length; `diskLen` is the on-disk blob length
  after compression and encryption; `xflags` (hex) carries
  `FlagCompressed|FlagEncrypted`. Forward-compatible: 6-, 7- and 8-field
  records still replay, defaulting `diskLen` to `valueLen` and `xflags` to 0.
- `VLogBatcher.Stage` returns `(offset int64, packed bool, err error)`.
  Callers must propagate `packed` to `FlagPacked` verbatim rather than
  recomputing or assuming it.

- **`StorageConfig.NumShards` is now marked deprecated and warns when set.**
  It had no effect — the index shard count is the compile-time `numShards`
  (8192) — but `hardware/config.go` computed it from CPU count and callers set
  it to 1024, all of which read as working tuning. The dead computation and the
  no-op assignments are gone, `NewStorageEngine` logs a warning if the field is
  set to anything other than 8192, and the auto-config log line no longer
  reports a shard count it did not choose.

- **`cpp/src/vlog.cpp` and `cpp/src/ebpf_gc_throttle.cpp` are now in the CMake
  build.** They were absent from `cpp/CMakeLists.txt` and from every cgo shim,
  so they were compiled by nothing while the docs advertised `vlog.cpp` as a
  performance hot path. Both wrap their body in `#ifdef __linux__`. Note this
  gives them compile coverage only — neither has a Go call site.

### Added

- **`cmd/veltrix-repair`** — offline repair for value-transform metadata lost
  by pre-fix builds. `--scan` is read-only and reports what is affected;
  `--repair` corrects the index and writes a corrected checkpoint, after
  backing up each `wal.log`. The repair is exact rather than heuristic:
  `IndexEntry.CRC32C` is the CRC of the *plaintext* and survived the bug, so
  the tool enumerates the four possible interpretations of each on-disk blob
  and accepts only the one that reproduces that CRC. Anything else is reported
  and left untouched. Idempotent; safe to re-run.

### CI

- **Added `node-5-race`**: the full test suite under `-race`. No job ran the
  race detector before, which is why the two races above went unnoticed.
- **Added `node-6-cpp`**: builds the CMake static library, the cgo shims
  (`CGO_ENABLED=1`), the storage tests with cgo, and `scripts/build.sh` end to
  end on a runner with `liburing-dev`. Nothing compiled `cpp/` before, so any
  change under it was completely unverified.
- **Added a `gofmt` gate** to `node-1-storage`; the tree is now gofmt-clean
  (39 files were not).

### Documentation

- **Removed three flags from the docs that do not exist.** The "operator
  checklist" for the 2M reads/s target told operators to pass
  `--cgo-batch-engine=true --numa-aware=true --sqpoll-reader=true`. No such
  flags are defined, and `flag.Parse()` is `ExitOnError`, so following the
  checklist made the server exit with "flag provided but not defined". All
  three behaviours are unconditional in a Linux CGO build and cannot be
  toggled at runtime.

- Corrected admission-control and flush-window constants in `CLAUDE.md` and
  `ARCHITECTURE.md`, which had drifted from the code: admission throttle is
  20 ms (not 4 ms), resume 10 ms (not 2 ms), GC bandwidth threshold 15 ms (not
  3 ms), and the default WAL/VLog flush windows are 15 ms (not 10 ms).
- Documented which C++ sources are actually compiled and reachable from Go.
  `cpp/src/vlog.cpp` and `cpp/src/ebpf_gc_throttle.cpp` are built by nothing;
  `art.cpp` and `scheduler.cpp` compile into the CMake static lib but have no
  Go call site. The published Docker image and all CI jobs are
  `CGO_ENABLED=0`, so they contain no C++ at all.
- Noted that `StorageConfig.NumShards` is vestigial — the index always uses the
  compile-time `numShards = 8192`. Fixed the startup log line that derived
  shards-per-disk from 1024.

---

## [1.1.0] — 2026-07-18

Distributed-completeness release: everything that previously applied
locally-only in cluster modes is now wired end-to-end.

### Added
- **Rack/zone-aware replica placement** (`--rack-id`): partition placement
  and `GetReplicasForKey` never put two copies of a partition in the same
  failure domain unless RF exceeds the distinct racks. Racks propagate
  between members via gossip digests; `/admin/cluster` reports each node's
  rack. Rackless clusters keep the exact pre-1.1 round-robin placement.
- **Admin API access control** (`--admin-token` / `VELTRIX_ADMIN_TOKEN`):
  `/admin/*` (stats, checkpoint, backup, CDC, changes, cluster) now rejects
  non-loopback requests unless the bearer token is presented; with a token
  configured, every request must carry it. `/metrics`, `/healthz` and
  `/readyz` stay unauthenticated for Prometheus and Kubernetes probes.
  `repl-ship` gained `--src-token` to authenticate against a guarded source.
- **Auto-rebalance**: node join/leave/failure now triggers ring rebalance AND
  physical key migration via the (previously dormant) TransferAgent
  (`cmd/server/rebalancer.go`; transfer HTTP on clientPort+5; opt out with
  `--auto-rebalance=false`).
- **Linearizable reads in raft mode** (`--linearizable-reads`): GET runs the
  Raft §6.4 ReadIndex fence — quorum-confirmed, never stale
  (`consensus/read_index.go`).
- **Distributed routing for all data types**: namespace, hash-field, vector,
  secondary-index, and the new list/set ops go through the coordinator in
  raft (dedicated FSM ops) and replicated (composite-key KV effects) modes.
- **List and set data types**: LPUSH/RPUSH/LPOP/RPOP/LLEN/LRANGE and
  SADD/SREM/SISMEMBER/SMEMBERS/SCARD (`storage/list_set_ops.go`) — stored as
  plain KV under reserved separators, no new on-disk format.
- **Text-protocol parity**: PUTEX (TTL), INCR/DECR/CAS/SETNX, MGET.
- **HNSW vector index** (`storage/hnsw.go`): replaces the brute-force scan;
  ~O(log N) queries at 0.99 recall@10 (M=16, efSearch=64), deterministic
  per-id levels, tombstoned deletes.
- **Disk-failure auto-degrade** (`storage/disk_health.go`): 5 consecutive
  I/O errors trip a per-disk breaker — writes to that disk fail fast,
  GC skips it, `/readyz` reports 503 degraded, INFO shows FAILED_DISKS.
- **Durable cross-region catch-up**: `/admin/changes` (index-backed change
  feed incl. tombstones) + `repl-ship --checkpoint` — a restarted shipper
  replays exactly the delta it missed before rejoining the live CDC stream.
- **PBKDF2-HMAC-SHA256 password hashing** (210k iterations, random salt) with
  legacy-hash fallback and a deprecation warning at load;
  `veltrix-admin hash-password` now emits the new format.
- **SDK atomics everywhere**: CAS/INCR/DECR/SETNX added to the Go, Python,
  Node.js, and Java clients (previously Rust/C++ only).

### Fixed
- Consistent-hash ring: FNV-1a output now passes through the fmix64
  finalizer — sequential key patterns ("user:0001"…) previously collapsed
  onto ONE node's arc. **Re-shards ring placement** (pre-GA breaking change).
- `PartitionMap.Rebalance`: a set top bit in the partition hash caused a
  negative array index panic; assignment is now round-robin over an
  ID-sorted node list — deterministic across members (map-iteration order
  previously made every node compute a DIFFERENT partition table) and
  balanced (max skew 1).
- Raft shutdown no longer leaks background persist goroutines that could
  write `raft_state_*.tmp` files after `Stop()` returned.
- repl-ship: low-traffic events no longer wait indefinitely for a full
  batch — pending events flush every 200 ms.
- `GetReplicasForKey` no longer returns the primary node twice.

### Changed
- README/CHANGELOG lead with the reproducible YCSB numbers; the internal
  GKE cluster figures are marked unverified until a published harness
  reproduces them.

---

## [1.0.0] — 2026-05-26

First public release.

### Storage engine
- WiscKey KV-separation: values written once to append-only NVMe VLog, never rewritten during compaction
- 1024-shard FNV-1a index with per-shard `sync.RWMutex` for parallel reads
- LIRS cache with value-aware eviction (small values ≤256 B get priority 2)
- Group-commit WAL: one `fdatasync` per batch window (default 10 ms) instead of per write
- Block packing: up to 26 records per 4 KB VLog block for 128 B values (26× density vs legacy 1-record-per-block)
- Three-tier admission-controlled GC: rate cap at 3 ms read EWMA, pause at 4 ms, emergency bypass at 65% garbage ratio
- CRC32C scrubber: background integrity validation at configurable MB/s per disk
- Tiered storage: `ColdTier` interface with `LocalFSColdTier` implementation

### Durability
- WAL replay crash recovery (6-field and 7-field formats, backward compatible)
- Raw NVMe block-device VLog mode with superblock and `BLKDISCARD` reclaim (Linux + `CAP_SYS_RAWIO`)
- AES-256-GCM at-rest encryption per record with per-record 12-byte nonce

### Data types and operations
- Keys with TTL (per-key expiry, background scanner)
- Hash fields with independent per-field TTL (8 wire opcodes)
- Namespaces (`ns\x00key` internal encoding)
- Atomic ops: `CAS`, `INCR`, `DECR`, `SETNX` (shard-locked RMW)
- Optimistic MVCC transactions with vector clock conflict detection
- Secondary indexes with user-supplied extraction functions

### Distributed
- Raft consensus: leader election, log replication, persisted state (`raft_state.gob`)
- Three replication modes: Async, Quorum, Strong
- Anti-entropy background sync for slow replicas
- Gossip failure detection: Heartbeat → Suspect → Failed → Recovering states
- 256-partition consistent hash ring for cluster topology

### Networking
- Auto-detected text and binary wire protocols on the same TCP port
- Pipeline coalescing: buffered PUT/GET frames automatically batched into MPUT/MGET (cap 256)
- Binary protocol: 27 opcodes (single, batch, namespace, hash-field, atomic)

### Security and operations
- RBAC auth with per-namespace prefix restriction
- mTLS support
- Async append-only JSONL audit log (never logs value bytes, never blocks data plane)
- Per-namespace token-bucket rate limiting + key-count quotas
- In-process CDC broker with `repl-ship` for cross-process forwarding

### Observability
- 60+ Prometheus metrics including P50/P90/P99 read latency histogram
- `/healthz`, `/readyz`, `/metrics` HTTP endpoints
- Admin API: `/admin/stats`, `/admin/checkpoint`, `/admin/quotas`, `/admin/cdc`, `/admin/ui`
- Web dashboard at `/admin/ui` (polling `/admin/stats`)
- OTel-shape request tracing with ring buffer at `/traces`

### Kubernetes
- Helm chart: StatefulSet, `nvme-prep` DaemonSet, StorageClass, ServiceMonitor, PDB, 22 PrometheusRule alerts
- CRD Operator: `VeltrixCluster` v1alpha1 with rolling upgrades, auto-reshard, self-healing
- `kubectl-veltrix` plugin: auto port-forwards to cluster pod, exposes all admin commands
- Cloud-agnostic NVMe provisioner: GKE, AWS i3/i4i, Azure Lsv2/Lsv3 auto-detection

### Client SDKs
- Go: thread-safe, connection pooled, full atomic ops
- Java: Maven-ready
- Python: PyPI-ready
- Node.js: npm-ready
- Rust: stdlib-only, no unsafe in hot path, full atomic ops
- C++: header-only POSIX client, full atomic ops

### Performance — measured (YCSB 0.17.0, single node, AWS EC2 4×NVMe, 100M keys)
- 427,697 reads/s (200 threads, 461 µs avg latency)
- 18,064 durable writes/s (fsync every write)
- Zero errors, zero GC emergency events
- (An internal 3-node GKE run reached 7.2M reads/s / 1.8M writes/s but has not
  been reproduced with a published harness; treat as unverified until then.)
- ~160 GB storage for 1B × 128 B values (~1.0× write amplification)

### C++ io_uring layer (Linux only)
- `io_uring` SQPOLL + `O_DIRECT` VLog reader with 256-deep submission ring
- ART index on 2 MB hugepages with SSE2 node search
- Lock-free open-addressing index with 32-byte buckets on hugepage backing
- NUMA-aware thread pinning via NVMe IRQ affinity
- 3-tier priority I/O scheduler (RT reads, normal GC writes, idle scrubber)
- CGO bridge: `runtime.Pinner` zero-copy path on Go ≥1.21

---

## What's coming

- **RESP protocol (Redis compatibility)**: point Redis clients at VeltrixDB without code changes
- **Raft log snapshots**: prevent slow restarts on large clusters with 100+ GB WALs
- **Durable CDC**: WAL-tail mode for repl-ship, preventing event loss on restarts
- **Native range/prefix scans**: remove the NSSCAN workaround for sorted-data use cases
- **Managed cloud offering**
- **HNSW vector index**: replace brute-force cosine for million-scale embedding workloads
- **Multi-region replication**: automatic cross-datacenter failover

Follow [GitHub Releases](https://github.com/VeltrixDB/veltrixdb/releases) for updates.
