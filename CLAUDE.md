# CLAUDE.md — VeltrixDB Project Context

This file is read automatically by Claude Code at the start of every session.
It captures non-obvious decisions, invariants, and working patterns that are
not derivable from reading the code alone.

---

## Project Identity

- **Module**: `github.com/VeltrixDB/veltrixdb`
- **Language split**: Go 1.19+ (cluster, replication, storage engine, TCP server) + C++20 (ART index, io_uring scheduler, compaction)
- **Build**: `go build ./...` for Go; CMake + liburing for C++ (Linux only)
- **Target platform**: GKE Linux nodes with local NVMe SSDs. macOS is dev-only; C++ io_uring code does not compile on macOS (expected).

---

## Key Commands

```bash
# Full build: C++ io_uring engine + Go binary + tarball (Linux, Go 1.19+)
VERSION=1.0.0 ./scripts/build.sh --output ./dist

# Go-only build (macOS dev or CI without liburing)
./scripts/build.sh --go-only --output ./dist

# Build everything (Go only, no C++)
go build ./...

# Run server (single disk, dev)
go run ./cmd/server -addr :9000 -data ./dev-data -cache 256

# Run server (8 NVMe disks, prod)
./veltrixdb \
  -addr :9000 \
  -data-dirs /mnt/nvme0,/mnt/nvme1,/mnt/nvme2,/mnt/nvme3,\
/mnt/nvme4,/mnt/nvme5,/mnt/nvme6,/mnt/nvme7 \
  -cache 65536

# Load test (30s mixed 70R/30W, 64 workers)
go run ./cmd/loadtest \
  --addr=127.0.0.1:9000 \
  --mode=mixed --concurrency=64 --duration=30 \
  --num-keys=1000000 --value-size=128 --read-ratio=0.7

# Load test — batched (engages packing). --batch-size > 1 switches the worker
# from single Put/Get to MPut/MGet. Per-batch P99 + per-key amortized are
# both reported. Use this path for any density or bulk-load benchmark.
go run ./cmd/loadtest \
  --addr=127.0.0.1:9000 \
  --mode=write --batch-size=1024 \
  --concurrency=8 --duration=30 \
  --num-keys=1000000 --value-size=128

# One-command bench harness: build → bulk-load → reads → mixed → write-stress.
# Hard pass/fail gates on packing density and GC emergency. Exit 0 = PASS.
# Re-run after every storage-engine change. See BENCHMARKING.md.
./scripts/bench.sh
DATA_DIRS=/mnt/nvme0,...,/mnt/nvme7 RAW_VLOGS=/dev/nvme0n1,...,/dev/nvme7n1 \
  CACHE_MB=409600 NUM_KEYS=10000000 ./scripts/bench.sh

# Prometheus metrics
curl http://localhost:2112/metrics
curl http://localhost:2112/healthz
curl http://localhost:2112/readyz
```

---

## Architecture in One Paragraph

**8192 shards** (FNV-1a hash & 0x1FFF), each with its own `sync.RWMutex`. N NVMe disks each own: one `SegmentWriter` (O_DIRECT on Linux), one `VLog` (append-only Value Log, WiscKey KV separation), one group-commit `WriteAheadLog`, one compaction goroutine, one io_uring `PriorityScheduler`. Shard routing: `shard % numDisks`. With `KeyValueSeparation=true` (default), only key metadata lives in the Index Vault; value bytes go directly to the per-disk VLog — compaction only GCs dead VLog space instead of merging full KV records. The Go layer handles crash durability (WAL + fdatasync); the C++ layer handles the ART index, high-speed io_uring VLog reads (SQPOLL, O_DIRECT), and vectorized batch puts (1024-entry CGO batches, `runtime.Pinner` on Go 1.21+). LIRS is the sole cache with value-aware eviction (small ≤256 B values have priority 2, resisting eviction). Client connections are 1-per-goroutine; the binary protocol uses `binPayloadPool` to eliminate per-request heap allocation.

---

## File Map — Critical Files

| File | Purpose |
|------|---------|
| `storage/engine.go` | `StorageEngine`: Put/Get/Delete, per-disk WAL routing, compaction dispatch; pipelined value transform = compress → encrypt → VLog (Get reverses: decrypt → decompress → migrate-on-read) |
| `storage/bloom_shard.go` | Lock-free per-shard Bloom: `Add`/`MayContain` via atomic-uint64 CAS; `installBlooms` allocates 8192 filters at startup; `vacuumBloomFilters` rebuilds from live index every defrag pass |
| `storage/scrubber.go` | One goroutine per VLog walks records at `ScrubMBPerSec`; honors `GCPaused`; CRC mismatch increments `ScrubCorruption` and emits structured log line with disk + offset |
| `storage/atomic_ops.go` | `CompareAndSwap` / `Increment` / `Decrement` / `SetIfNotExists`; holds shard write lock for the whole RMW; updates `shard.entries` directly to avoid recursive lock acquisition; saturating arithmetic on INCR overflow |
| `storage/compress.go` | `MaybeCompress` (256-byte threshold + savings floor); algorithm-prefix byte enables zstd swap-in without on-disk format change |
| `storage/encrypt.go` | AES-256-GCM with 12-byte nonce per record; key from `VELTRIXDB_ENCRYPTION_KEY` env or file; `setEncryptor`/`getEncryptor` global; `EncryptionEnabled()` check on Put/Get hot paths |
| `storage/schema.go` | `RegisterMigration(toVersion, fn)`; `MigrateOnRead` chains v→v+1 fns up to `CurrentSchemaVersion`; `MigrateAll()` admin path rewrites every key at current version |
| `storage/audit.go` | Async JSONL writer with bounded channel + drops counter; periodic fsync; never logs value bytes |
| `storage/cdc.go` | In-process broker; per-subscriber buffered channel; 3-strikes auto-evict on slow consumer; `Subscribe(buf, prefix)` returns `<-chan CDCEvent` + cancel |
| `storage/quotas.go` | Token-bucket writes/sec + `MaxKeys` cap per namespace; `CheckWrite(ns, isNewKey)` returns `ErrRateLimited` / `ErrQuotaExceeded`; engine wires from `PutNS` / `DeleteNS` |
| `storage/transaction.go` | `BeginTxn` / `Set` / `SetIf` / `Delete` / `Commit`; optimistic-CAS guard on `WriteTimestampUs`; commit path goes through `MultiPut` for per-disk atomicity |
| `storage/secondary_index.go` | `RegisterIndexRule(name, extractFn)`; key-prefix `@idx/<rule>/<value>/<primary>`; `LookupBySecondary` shard scan; `applySecondaryIndexes` diff-applies on Put |
| `storage/vector_index.go` | Brute-force cosine; in-RAM `[]float32` per namespace; persists via reserved `@vec/<ns>/<id>` key prefix |
| `storage/predicates.go` | Go-function registry; `PredicateScan(name, prefix, limit)` walks shards under RLock + Get under no lock; `PredicateFilter` operates on caller-supplied keys |
| `storage/tombstone_replicated.go` | Per-replica watermark coordinator; `CanReapTombstone` returns false if any replica's acked timestamp is below the tombstone; single-node = no-op |
| `storage/tiered.go` | `ColdTier` interface; `LocalFSColdTier` ships; `SetColdTier` global; CRC32C-derived handle; `PutColdTier` / `GetColdTier` |
| `adminapi/admin.go` + `adminapi/ui.go` | `/admin/{stats,checkpoint,quotas,migrate,cdc,version,ui}` HTTP handlers; `/admin/ui` is an 8 KB single-page dashboard polling `/admin/stats` |
| `tracing/tracing.go` | OTel-shape `Start(ctx, name) → ctx, Span`; rate sampler + always-record on slow (≥50 ms) and error spans; ring buffer queryable at `/traces` (NDJSON) |
| `cmd/repl-ship/main.go` | Long-poll `/admin/cdc` → write to dst-tcp via binary protocol; deadletter file on retry exhaustion |
| `cmd/veltrix-migrate/main.go` | export/import/migrate over the binary protocol; uses `/admin/stats` for progress |
| `cmd/veltrix-chaos/main.go` | kill / pause (SIGSTOP+CONT) / network (tc/iptables) / clock-skew / soak; supports `pid:`, `process:`, `k8s:NS/POD` target descriptors |
| `cmd/kubectl-veltrix/main.go` | kubectl plugin; auto port-forwards to a matching pod and hits `/admin/*` |
| `Veltrixdb-client/rust/src/lib.rs` | Blocking, stdlib-only Rust client; full atomic-op surface — lives in the **Veltrixdb-client** repo, not this repo |
| `Veltrixdb-client/cpp/include/veltrixdb.hpp` | Header-only POSIX-sockets C++17 client — lives in the **Veltrixdb-client** repo, not this repo |
| `storage/wal.go` | Group-commit `WriteAheadLog`: channel → flusher goroutine → single fdatasync; 7-field WAL format with `vlogOffset`; `truncate()` for clean shutdown |
| `storage/wal_replay.go` | `replayWAL()` parses on-disk WAL (6-field or 7-field); `applyWALReplay()` rebuilds `shardedIndex` and restores `se.version` on startup; `walPathForDir()` mirrors `newWriteAheadLog` path |
| `storage/segment.go` | `SegmentWriter`: O_DIRECT binary record format, `ioPool` usage |
| `storage/segment_linux.go` | `openSegmentFile` (O_DIRECT), `newAlignedBuf`, `ioPool` (Linux) |
| `storage/segment_other.go` | `openSegmentFile` (buffered), `ioPool` (macOS/Windows dev) |
| `storage/shard.go` | `indexShard`: per-shard RWMutex + in-memory index map |
| `storage/cache.go` | LIRS cache; value-aware eviction: `priority=2` for ≤256 B values; 16-entry scan window picks largest cold victim |
| `storage/defrag.go` | Defragmenter: physical-order compaction; also drives VLog GC via `VLog.GCRatio()` |
| `storage/vlog.go` | `VLog`: append-only per-disk Value Log (WiscKey KV separation); magic=0x564C5402; 24-byte header + sector-aligned payload; `VLogBatcher` for group-append; `MarkDead` for GC accounting |
| `storage/batcher.go` | `WriteBatcher`: 2 MB / **4096-entry** / 5 ms flush windows; batchBufPool pre-sized 4096; channel depth 65536; falls back to sync Put when saturated |
| `storage/cgo_bridge.go` | CGO bridge (Go <1.21): zero-copy uintptr pointer passing to C++ VeltrixBatchEngine |
| `storage/cgo_bridge_pinner.go` | CGO bridge (Go ≥1.21): `runtime.Pinner` path — pins Go backing arrays, stores pointers directly in C heap |
| `storage/index_hugepage_linux.go` | `HugepageAlloc` / `HugepageFree`: 2 MB hugepage mmap for Go Index Vault (Linux); falls back to regular mmap |
| `storage/mlockall_linux.go` | `LockProcessMemory` / `SetMemoryRLimitLock`: mlockall(MCL_CURRENT\|MCL_FUTURE\|MCL_ONFAULT) to prevent swap eviction at 1B+ key scale |
| `cmd/server/main.go` | TCP server: binary + text protocol, `binPayloadPool`, flag parsing |
| `cmd/server/readv_linux.go` | `readvInto` + `setSocketRecvBuf`: scatter-gather readv(2) + SO_RCVBUF 256 KB hint on accept |
| `client/tcp.go` | `TCPConn`: persistent TCP connection, Put/Get/Delete/Ping/Info/Redial |
| `cmd/loadtest/main.go` | Concurrent load tester: per-goroutine conn + RNG, latency percentiles |
| `cpp/include/scheduler.hpp` | `PriorityScheduler`: 3-tier io_uring queue + write batching + fixed bufs |
| `cpp/src/scheduler.cpp` | Scheduler implementation: ioprio, write group commit, three separate tier deques |
| `cpp/include/allocator.hpp` | `SegmentedPool` (hugepage-capable slab allocator), `IoBuffer` — no longer contains `ArtSlabAllocator` |
| `cpp/include/art.hpp` | ART index: Node4/16/48/256, SSE2 search, Janitor compaction, `ArtSlabAllocator` (256 KB cache-line-aligned slabs) |
| `cpp/include/defragmenter.hpp` | `Defragmenter::Config` — uses constructor initializer list (not NSDMIs) to avoid CWG 1497 |
| `cpp/include/vlog.hpp` | `VLogReader` (io_uring SQPOLL, O_DIRECT, 256-deep ring), `VLogWriter` (pwrite+fdatasync), `VLogContext` (per-disk bundle); SSE4.2 CRC32C |
| `cpp/src/vlog.cpp` | VLog C++ implementation; IORING_SETUP_SQPOLL; `posix_memalign` sector-aligned buffers; `BatchRead` submits all SQEs in one `io_uring_submit` call |
| `cpp/include/batch_engine.hpp` | `VeltrixBatchEngine` C API: shard-parallel vectorized batch put/get; NUMA-aware constructor (`veltrix_batch_engine_create_ex`) |
| `cpp/include/lockfree_index.hpp` | `LockFreeIndex`: lock-free hugepage-backed open-addressing hash map; 32-byte buckets; 50% load factor; CAS upsert, acquire/release ordering |
| `cpp/include/numa_topology.hpp` | `NumaTopology` / `nvme_preferred_node` / `pin_thread_to_node`: NUMA node discovery + NVMe IRQ affinity thread pinning |
| `cpp/include/uring_reader.hpp` | `UringReader`: SQPOLL io_uring SSTable reader; zero-syscall submission; dedicated Tier 0 completion thread; pre-registered fixed buffers |
| `cpp/src/batch_engine.cpp` | VeltrixBatchEngine implementation |
| `cpp/src/numa_topology.cpp` | NUMA topology implementation |
| `cpp/src/uring_reader.cpp` | UringReader implementation |
| `metrics/prometheus.go` | `VeltrixCollector`: all Prometheus metrics |
| `cluster/` | Partition map, gossip failure detector, consistent hash ring |
| `replication/` | Async/quorum/strong replication, vector clocks, anti-entropy |
| `scripts/build.sh` | Full build: C++ + Go + tarball; uses `cd` subshell (Go 1.19+ compatible) |
| `scripts/hugepages.sh` | One-time host prep: hugepages, NVMe scheduler, ulimits |
| `scripts/sysctl.conf` | Drop-in `/etc/sysctl.d/99-veltrixdb.conf` for production VMs |

---

## Invariants — Never Break These

1. **Shard routing is `FNV-1a(key) & 0x1FFF` for index (8192 shards), `shard % numDisks` for disk/WAL.** Both must stay consistent — an IndexEntry's `DiskOffset` is only valid on the segment/VLog file for `shard % numDisks`. The C++ `kNumShards = 8192` and Go `numShards = 8192` must always match.

2. **Every segment record is 512-byte-sector-aligned.** O_DIRECT requires it. `sw.end` is only ever incremented by `alignedLen` (rounded up to 512). Never write partial sectors.

3. **`WriteAt` not `Seek`+`Write`.** `pread64`/`pwrite64` are atomic w.r.t. file position. The old seek+write pattern was unsafe with concurrent `ReadAt`.

4. **`ioPool.put(buf)` stores `b[:cap(b)]` not `b[:len(b)]`.** On Linux the backing array has extra bytes for alignment padding. Storing the full cap preserves alignment for the next `get()` caller.

5. **`binPayloadPool` is only safe to return after `engine.Put` returns.** `Put` is synchronous — it blocks on `<-resp` in the WAL group-commit until fdatasync completes. Both WAL serialize and segment WriteRecord copy the value bytes before Put returns. Never pool the buffer if you pass value to an async callback.

6. **WAL does not use O_DIRECT.** O_DIRECT + O_APPEND has kernel bugs on some versions. WAL uses buffered I/O; group commit compensates for fdatasync latency.

7. **`--local-ssd-interface=NVME` is required on GKE.** Without it COS creates a single RAID-0 device across all local SSDs. VeltrixDB needs 8 independent NVMe queues. The nvme-prep DaemonSet will not work correctly without this flag.

8. **`/dev/nvme0n1` is the GKE boot disk.** The nvme-prep DaemonSet detects it by checking if it's mounted AND by size (375 GiB local SSDs vs 100 GiB boot). Never hardcode device paths.

9. **C++ `io_uring` code is Linux-only.** Do not attempt to compile `cpp/` on macOS. The Go layer is the cross-platform storage engine; C++ is an optional high-performance layer for Linux production.

10. **`write_pending_count_` is incremented before the inbox spinlock in `enqueue()`.** The increment uses `memory_order_release`; the decrement uses `memory_order_relaxed`. Readers use `memory_order_relaxed`. This is intentional — the count is a monitoring hint, not a synchronisation primitive.

11. **`fd.SetLocalNode(nodeID)` must be called before `fd.Start()`.** The local node never heartbeats itself via gossip, so its `nodeHeartbeats` entry is zero. Without `SetLocalNode`, `checkNodeHealth()` computes `timeSinceHeartbeat = now - 0 = epoch_ns`, which always exceeds the 10s failure threshold → constant FAILED/Recovering cycles (~2–3 false events/s in single-node mode).

12. **`Defragmenter::Config` must not use non-static data member initializers (NSDMIs).** GCC rejects `Config cfg = {}` as a default argument when the nested `Config` struct has NSDMIs (CWG 1497 — NSDMIs require the enclosing class to be complete, but default-argument evaluation happens before the class is complete). Use a constructor initializer list instead: `Config() : field(value), … {}`. This applies to any nested struct used as a default argument inside a class body.

13. **`scripts/build.sh` uses `(cd REPO_ROOT && go build …)` subshell, not `go build -C`.** The `-C` flag was added in Go 1.21; we target Go 1.19+. Never revert to `-C` without a minimum-version gate.

14. **C++ code uses namespace `veltrix`, not `axon`.** All C++ headers and sources were renamed from `namespace axon` to `namespace veltrix` in recent commits. Never revert or mix the two namespaces.

15. **`IndexEntry.DiskOffset / SegmentID / ValueSize` double as a `ValuePointer` into the VLog when `KeyValueSeparation=true`.** `DiskOffset` = byte offset of the VLog record header; `SegmentID` = disk index (VLog file index); `ValueSize` = unpadded value length. The VLog record header is 24 bytes; the total record is sector-aligned. `ReadValue(DiskOffset, ValueSize)` reads from `vlogs[SegmentID]`. Never interpret these fields as segment-file pointers when KV separation is on.

16. **VLog records use magic `0x564C5402` ("VLT\x02"), not `0x564C5401` ("VLT\x01") which is the segment format.** Both files share the `ioPool` and `openSegmentFile` helper for consistent O_DIRECT alignment. The 24-byte VLog header is NOT the same as the 64-byte segment header — do not parse them interchangeably.

17. **`VLog.MarkDead(valueLen)` must be called every time a key's old VLog pointer is superseded** (overwrite in `Put`, tombstone in `Delete`). Failure to call `MarkDead` causes `GCRatio()` to under-report garbage and the VLog to grow without bound.

18. **VLog write offset reservation is lock-free via `vl.end.Add(alignedLen)`**. Both `VLog.beginAppend()` and `VLogBatcher.Stage()` use `vl.end.Add(int64(alignedLen)) - int64(alignedLen)` to atomically reserve `[offset, offset+alignedLen)`. Because `alignedLen` is always a multiple of `vlogBlockSize` (4096) and `startOffset` is 4K-aligned at open, every caller gets a 4K-aligned offset with no mutex. POSIX guarantees concurrent `pwrite64` to non-overlapping ranges is race-free. Never reintroduce a mutex around `WriteAt` in the VLog hot path — it serialises all writers to `1/WriteAt_latency ≈ 10K writes/s/disk`. **`vlogBlockSize` must stay 4096**: XFS (default block size = 4096) and 4Kn NVMe drives both require 4096-byte O_DIRECT alignment — 512-byte writes return EINVAL.

19. **VLog is appended BEFORE WAL in the KV-separation `Put()` path.** `engine.go Put()` calls `vl.beginAppend(value)` first (non-blocking atomic offset reservation), stores the returned `vlogOffset` in the `WALEntry`, then calls `wal.beginAppend(walEntry)`. Both flusher goroutines then race to fdatasync; the caller unblocks when `max(WAL, VLog)` completes. Do not serialize these calls — sequential submission doubles the P99 latency at matched window sizes. This ordering means: if the process crashes after VLog fdatasync but before WAL fdatasync, the VLog offset is never written to WAL and the value is simply unreferenced garbage — the index is never updated so the key is not visible. Safe.

20. **`WALFlushWindowMs` and `VLogFlushWindowMs` must always be equal** (both default 10 ms). The concurrent `Put()` path waits for `max(WAL_wait, VLog_wait)`. If windows differ, the shorter one finishes first but the caller still blocks on the longer one — the shorter window's throughput benefit is lost. Keep them equal so both fdatasyncs race to completion within the same window.

21. **WAL uses a 7-field pipe-delimited format; `vlogOffset=0` means value bytes follow inline.** On-disk record: `timestamp|tombstone|key|valueLen|crc32hex|version|vlogOffset\n[value bytes\n]`. In KV-sep mode, `vlogOffset > 0` → value already durable in VLog, no value bytes in WAL (WAL header only, ~100 B/write instead of header+value). On restart, `replayWAL()` parses each disk's WAL and `applyWALReplay()` rebuilds the in-memory index. Clean shutdown calls `wal.truncate()` (zeros the file) so next startup skips replay entirely. Dirty shutdown (crash) leaves WAL intact for full replay. Legacy 6-field entries (no vlogOffset field) are parsed for backward compatibility: if value bytes are present they are re-appended to VLog and old VLog data becomes GC garbage.

22. **Admission control thresholds: 4 ms EWMA activates write throttle + GC pause; 2 ms EWMA clears both; stale EWMA auto-expires after 4 min.** When `ReadLatencyEWMANs > admissionThrottleNs (4ms)`, `AdmissionControl.WriteThrottleActive` is set true and `GCPaused` is set true. Each `Put()` call checks `WriteThrottleActive` before `beginAppend()` and sleeps 2 ms if set. `compactVLog()` checks `GCPaused` at entry. Staleness guard: if `AdmissionControl.LastReadNs` is 0 (no reads ever) or the last `Get()` was more than `gcEWMAStaleDuration` (4 min = 2 × DefragInterval) ago, `compactVLog()` treats the EWMA as stale, clears `GCPaused`, and resets `ReadLatencyEWMANs` to 0 so the next real read starts fresh — this prevents a single slow read early in a write-only benchmark from permanently blocking VLog GC. `LastReadNs` is stamped inside the `Get()` defer on every call. The `veltrixdb_storage_write_admission_throttles_total` counter tracks throttled Put operations. The GC rate limiter threshold (`gcLatencyThresholdNs=3ms`) fires before admission control (4ms) to progressively cap GC bandwidth before full pause.

23. **Tiered emergency GC overrides admission-control pause at high garbage ratios — escapes the death spiral.** Without this, a high read EWMA permanently pauses GC, garbage keeps growing, more cache misses → reads stay slow → permanent pause (the "71% choke" symptom). Three tiers in `defrag.go`:
    - `gcRatio < 0.50` — normal: bandwidth cap = `gcThrottledBPS` (60 MB/s) when reads slow; respects `GCPaused`.
    - `0.50 ≤ gcRatio < 0.65` (CRITICAL) — bandwidth cap raised to `gcCriticalBPS` (200 MB/s); defrag interval halved automatically; still respects `GCPaused`.
    - `gcRatio ≥ 0.65` (EMERGENCY) — `GCPaused` is **bypassed**, bandwidth cap is unlimited, defrag interval is quartered. Each emergency pass increments `VLogGCEmergencyRuns` and emits a `[gc] disk=N EMERGENCY ...` log. A persistent non-zero `veltrixdb_vlog_gc_emergency_runs_total` rate is the canonical signal that sustained write rate exceeds GC throughput — investigate disk IOPS or reduce workload.

24. **Adaptive defrag interval shortens automatically when any one disk's garbage ratio rises.** `Defragmenter.loop()` uses a `time.Timer` (not a `Ticker`) so the period can be adjusted dynamically. After each pass, `nextInterval()` walks all VLogs, takes the worst garbage ratio, and returns the full interval (< 50%), half (50–65%), or quarter (≥ 65%). High-garbage disks get more frequent attention without operator retuning.

25. **`gcMinimumBPS = 16 MB/s` (anti-starvation floor), raised from 4 MB/s.** At 4 MB/s reclaiming 100 GB takes ~7 hours, long enough for write rate to outpace GC. 16 MB/s keeps GC ahead of moderate workloads while still leaving 95%+ NVMe headroom for reads. Token bucket burst (`gcBurstBytes`) raised to 16 MB to match.

26. **Raw NVMe block-device VLog backing (`RawVLogDevices` / `--raw-vlogs`).** When set, the VLog opens `/dev/nvmeXnY` directly with `O_DIRECT|O_EXCL` instead of `vlog_active.dat` on XFS. WAL and segment files still live on `--data-dirs`. Linux-only (BLKDISCARD ioctl). Layout per device: bytes [0, 4096) = `RawSuperblock` (magic `0x564C4252` "VLBR", version, `vlogStart=4096`, device size, init timestamp, sector size, CRC32C of head); bytes [4096, deviceSize) = sector-aligned VLog records, identical encoding to the file-based path. GC reclaim uses `BLKDISCARD` ioctl (NVMe TRIM) instead of `fallocate(PUNCH_HOLE)` — the kernel forwards the discard to the NVMe controller, freeing flash erase blocks. **Never discard offset [0, 4096)** — that's the superblock; `punchDeadHead` clamps the start offset to `RawSuperblockSize` before issuing the ioctl. Magic mismatch on read aborts startup rather than overwriting foreign data. Server flag count must equal `--data-dirs` count or startup fails. Capability requirement: `CAP_SYS_RAWIO` (operator and Helm chart wire this in automatically when raw mode is on).

27. **Raw VLog crash recovery: `vl.end` is seeded after WAL replay via `SetEndAtLeast(index.maxVLogEndOffset(...))`.** Block devices return `Stat().Size()=0`, so `newVLog` cannot infer where prior writes ended — it temporarily sets `vl.end = vlogStart (4096)`. The engine's startup path then walks the rebuilt index per disk, computes `max(DiskOffset + alignedSize)` over all entries (alignedSize = full 4 KB block for unpacked entries, raw size rounded up for packed), and calls `vl.SetEndAtLeast(maxEnd)`. This advances `vl.end` past every live record so the next `beginAppend` can't overwrite them. Without this seed, the first `Put` after a raw-mode restart would CAS `vl.end.Add(alignedLen)` starting at offset 4096 and stomp existing live VLog records that the index correctly references. `SetEndAtLeast` is monotonic (never retreats), so calling it on a file-mode VLog whose `vl.end` already reflects the file size is a safe no-op. Crash mid-write semantics are preserved: a value durably written to VLog but not WAL-fdatasync'd is correctly considered dead after replay (its WAL entry never landed → not in the index → its bytes are reclaimable garbage). The client never received OK, so no durability contract is broken.

28. **VLog block packing — multiple records per 4 KB block via `VLogBatcher`.** For 128 B-class values the legacy "one record per 4 KB block" wasted 96 % of disk to padding. Block packing puts up to ~26 records (header 24 B + value 152 B) into a single 4 KB block, dropping per-record disk usage from 4096 B to ~152 B (25× density gain). Three things make this safe:
    - **Read path is unchanged.** `ReadValue` already rounds the offset DOWN to the next 4 KB and extracts the record at `intraBlock = offset - alignedOffset`. Packed records simply get non-4K-aligned `DiskOffset` values like `blockOff + 152`, `blockOff + 304`, …
    - **`FlagPacked = 0x40`** on `IndexEntry` distinguishes packed (subtract raw header+value on MarkDead) from unpacked (subtract full 4 KB block). `entry.IsPacked()` is the only correct way to tell — never infer from offset alignment because the first packed record in a block sits at offset `blockOff + 0` which IS 4 KB-aligned.
    - **WAL carries packed flag in 8th pipe-delimited field**: `timestamp|tomb|key|valLen|crc|version|vlogOffset|packed`. Old 6/7-field records parse with `packed=false` (legacy unpacked layout). Replay path sets `entry.Flags |= FlagPacked` from this field so MarkDead semantics survive a restart.
    Single `Put()` (engine.go) still uses the lock-free unpacked `beginAppend` path — packing only kicks in via the batched path (`MultiPut`, GC compactor relocations, `WriteBatcher`). Hot single-Put path stays as fast as before; bulk-load and steady-state-throughput paths get the density win. Oversized records (header+value > 4 KB) automatically fall back to the unpacked path inside the batcher. Mismatched FlagPacked → permanent over- or under-count of `liveBytes` → distorted `GCRatio`. Always source the flag from `entry.IsPacked()` on the **superseded** entry when calling `MarkDead`.

    **Crash-recovery preserves packing.** `MultiPut` (since 2026-05-09) writes VLog **first**, then WAL with `VLogOffset + Packed` already set — same pattern as engine.Put. Replay sees `vlogOffset > 0` and `packed=1` for those records and reuses the original packed VLog bytes directly, restoring `FlagPacked` on the rebuilt index entry. No re-write through the unpacked legacy path. Failure modes: VLog Flush fails ⇒ all entries on that disk fail (their staged bytes orphaned, reclaimed by next GC); per-entry WAL fails ⇒ that one entry's index update is skipped (its VLog bytes orphaned). Concurrent WAL+VLog fdatasync ⇒ effective P99 = max(both), not sum.

29. **Value transform pipeline order: compress → encrypt → VLog write; decrypt → decompress → migrate-on-read.** Compression must be done BEFORE encryption — encrypted ciphertext is incompressible (high entropy), so compressing after encrypt gains nothing and wastes CPU. Encryption must be done AFTER compression because the encrypted blob is what lands on disk. On the read path the order reverses exactly: decrypt the on-disk bytes, then decompress, then run any pending schema migration. `IndexEntry.UncompressedSize` is the original plaintext byte length; `IndexEntry.ValueSize` is the post-encryption blob length (= post-compression length when encryption is off). Per-record flags `FlagCompressed` (`0x02`) and `FlagEncrypted` (`0x04`) tell the read path which transforms to apply — entries written before encryption was enabled remain readable.

30. **Atomic-op shard write lock is held for the entire RMW including WAL+VLog fdatasync (~10 ms).** `CompareAndSwap` / `Increment` / `Decrement` / `SetIfNotExists` take `shard.mu.Lock()` for the duration of: read-current-value → compare → durably persist new value → install new IndexEntry. During the persist phase the shard is unreadable. With 8192 shards this stalls 1/8192 of the keyspace for ~10 ms = ~1.2 µs random-distributed P99 read tail — acceptable for atomic-op rare path. NEVER call `engine.Put`/`engine.Delete` from inside an atomic op (they would re-acquire the same shard lock and deadlock). Internal helper `persistAtomicKVSep` mutates `shard.entries` directly without re-locking.

31. **Bloom filter probes use double-hashing `pos = h1 + i × h2`; `Add` MUST `break` (not `return`) on already-set bits.** The CAS loop inside Add advances to the next probe via `break` once the bit is observed set or successfully CAS'd; an early `return` would leave only the first probe set and break the "no false negatives" invariant on subsequent MayContain (caught by `TestProperty_BloomNoFalseNegative`). The bloom is rebuilt from the live index every defrag pass via `vacuumBloomFilters` — without this, deletes leave bits set forever and the FP rate climbs.

32. **At-rest encryption key MUST be 32 raw bytes (AES-256).** Key sources, in priority order: `VELTRIXDB_ENCRYPTION_KEY` env (base64-encoded), then `--encryption-key-path` (raw or base64). Startup fails if `--encrypt-at-rest` is set but no key resolves. Online key rotation is NOT supported — see DR_RUNBOOK.md §5 for the offline rotation sequence.

33. **CDC broker is in-process and bounded.** Subscribers receive events through a per-subscription buffered channel; a slow consumer that fills its buffer 3 consecutive times is auto-evicted (channel closed). The broker `Broadcast` call NEVER blocks the producer — drops on full channels are counted in `cdc_dropped_total`. For cross-process / cross-region CDC use `cmd/repl-ship`, which long-polls `/admin/cdc` and forwards over the binary protocol; events that arrive while repl-ship is down are LOST (production gap; durable WAL-tail mode is separate work).

34. **Audit log writes never block the data plane.** `AuditLog.Log` enqueues to a bounded channel (default 8192). On full channel the record is dropped and `audit_dropped_total` increments — Veltrix's contract is that auditing must NEVER stall mutating operations. Operators that require zero-loss audit must size the channel based on burst rate (`AuditChannelDepth` config) and ship to an immutable backend (Loki / Splunk / S3 Object Lock) before disk fills.

35. **Per-namespace quota check happens BEFORE the actual write but the key-count increment happens AFTER.** `PutNS` calls `quotas.CheckWrite(ns, isNewKey)` first; if the limit is over, returns `ErrRateLimited` / `ErrQuotaExceeded` and does not append to WAL. After a successful Put, `IncKeyCount(ns, +1)` runs; on Delete, `IncKeyCount(ns, -1)`. The `isNewKey` boolean comes from a pre-Put `index.get(key)` lookup — overwrites do NOT count against `MaxKeys`. The token-bucket and the key-count are decoupled: a namespace can hit the rate limit without exceeding the key cap, and vice versa.

---

## Performance Hotspots

| Hotspot | File:Line | What matters |
|---------|-----------|-------------|
| Binary frame alloc | `cmd/server/main.go:375` | `binPayloadPool.Get()` — must not alloc on hot path |
| Storage I/O alloc | `storage/segment.go` | `ioPool.get()` — must be 512-byte aligned on Linux |
| WAL fdatasync | `storage/wal.go:flusher` | One fdatasync per group, not per write |
| SQE submission | `cpp/src/scheduler.cpp:submit_batch` | Write batch window: 32 ops or 1ms |
| ART lookup TLB | `cpp/include/art.hpp` | `ArtSlabAllocator` uses 2MB hugepages |
| NVMe read priority | `cpp/src/scheduler.cpp:fill_sqe` | `sqe->ioprio = RT class` for reads |
| Pipeline coalescing | `cmd/server/main.go:tryCoalescePuts` | Opportunistic batching of buffered PUT/GET frames into MultiPut/MultiGet; cap=256; one Flush() per batch |
| WriteBatcher flush | `storage/batcher.go:flush` | 2 MB **or 4096-entry** threshold + 5 ms timer; batchBufPool pre-sized 4096; channel depth 65536 |
| CGO batch dispatch | `storage/cgo_bridge_pinner.go:batchPutViaCGO` | `runtime.Pinner` zero-copy; 1024-entry CGO call groups 1024 entries across 8192 shards on thread pool |
| VLog append | `storage/vlog.go:Append` | Sector-aligned ioPool buf; one fdatasync per write; `VLogBatcher` groups multiple appends under one fdatasync |
| VLog read (C++) | `cpp/src/vlog.cpp:BatchRead` | io_uring SQPOLL; single `io_uring_submit` for all SQEs; SSE4.2 CRC32C; O_DIRECT avoids page cache |
| LockFreeIndex lookup | `cpp/include/lockfree_index.hpp` | Single acquire-load per hit; hugepage-backed 32-byte buckets; 50% load factor |

---

## Wire Protocol Quick Reference

### Text (nc-compatible)
```
PUT key value\n  →  OK\n
GET key\n        →  value\n  |  ERR\n
DEL key\n        →  OK\n
PING\n           →  PONG\n
INFO\n           →  keys=N writes=N ...\n
QUIT\n           →  BYE\n
```

### Binary (auto-detected, first byte 0x01–0x05)
```
Request:  [1B cmd][2B keyLen LE][4B valLen LE][key][value]
Response: [1B status][4B payloadLen LE][payload]

cmd:    0x01=PUT  0x02=GET  0x03=DEL  0x04=PING  0x05=INFO
status: 0x00=OK   0x01=ERR  0x02=NOT_FOUND
```

---

## GKE Deployment Checklist

1. `--local-ssd-interface=NVME` in node pool creation — **mandatory**
2. `helm install veltrixdb veltrixdb/veltrixdb --namespace veltrixdb --create-namespace` — deploys StorageClass + nvme-prep DaemonSet + StatefulSet + ServiceMonitor + PDB in one step. Or use the Operator: `kubectl apply -f VeltrixDB-Kubernetes-Operator/config/crd/bases/ && kubectl apply -f VeltrixDB-Kubernetes-Operator/config/manager/manager.yaml`
3. Wait for 24 PVs (3 nodes × 8 SSDs) to be `Available`
4. Verify pods are Running: `kubectl get pods -n veltrixdb -l app.kubernetes.io/name=veltrixdb`
5. Set `vm.nr_hugepages = 512` on each node (startup script or DaemonSet)
6. Verify `io_uring_register_buffers` succeeds — needs RLIMIT_MEMLOCK or CAP_IPC_LOCK

---

## Tuning Reference

| Goal | Config | Value |
|------|--------|-------|
| Max read throughput | `cfg.CacheMaxSizeMB` | 262144 (256 GB) |
| Max write throughput | `cfg.write_batch_limit` | 32 (C++ scheduler) |
| Write latency cap | `cfg.write_batch_window_us` | 1000 (1 ms) |
| **WAL flush window (Go)** | `cfg.WALFlushWindowMs` | **10 ms** — P99 ≈ 10.2 ms on NVMe; batch_size ≈ writes/s/disk × 0.010; 0 = immediate flush (legacy) |
| **VLog flush window (Go)** | `cfg.VLogFlushWindowMs` | **10 ms** — must equal WALFlushWindowMs; VLog group-commit runs same pattern; WAL+VLog submitted concurrently in Put() |
| WAL max batch cap | `cfg.WALMaxBatchEntries` | 4096 — forces early flush if batch reaches this before window expires |
| Range scan speed | `cfg.DefragInterval` | 30s |
| Hugepages (C++) | `/proc/sys/vm/nr_hugepages` | ≥ 512 |
| io_uring fixed bufs | `scheduler.register_fixed_buffers(N, size)` | N=8, size=512KB |
| KV separation (WiscKey) | `cfg.KeyValueSeparation` | `true` (default) |
| VLog GC trigger | `cfg.DefragThreshold` | 0.30 — GC when 30% of VLog is dead space |
| GC bandwidth cap (normal) | `gcThrottledBPS` (defrag.go) | 60 MB/s (~15% of 400 MB/s NVMe) when read EWMA > 15 ms AND garbage < 50%; unlimited when reads fast |
| GC bandwidth cap (critical) | `gcCriticalBPS` (defrag.go) | 200 MB/s — applied when 50% ≤ garbage < 65% AND reads slow |
| GC critical ratio | `gcCriticalRatio` (defrag.go) | 0.50 — raise BW cap to 200 MB/s; halve defrag interval |
| GC emergency ratio | `gcEmergencyRatio` (defrag.go) | 0.65 — bypass `GCPaused`; uncap BW; quarter defrag interval |
| GC min bandwidth | `gcMinimumBPS` (defrag.go) | 16 MB/s — anti-starvation floor; raised from 4 MB/s to keep GC ahead of moderate write workloads |
| GC rate threshold | `gcLatencyThresholdNs` (defrag.go) | 15 ms EWMA → cap to 60 MB/s; precedes 20 ms admission-control full-pause |
| Write admission throttle | `admissionThrottleNs` (types.go) | 4 ms EWMA → 2 ms sleep per Put + GC fully paused |
| Write admission resume | `admissionResumeNs` (types.go) | 2 ms EWMA → throttle cleared, GC resumed |
| 100K writes/s target | `--wal-flush-window-ms` / `--vlog-flush-window-ms` | 5 ms (both equal) → ~200 writes/goroutine/s; 512 goroutines = ~102K writes/s |
| 2M reads/s P99<5ms target | `--read-heavy` flag | Activates `ReadHeavyConfig`: 400 GB cache, LIRRatio=0.95, 300 s defrag, 60 s TTL scan |
| Read EWMA sample rate | `readEWMASampleEvery` (types.go) | 64 — only every 64th Get() does the CAS-loop EWMA update; eliminates cache-line ping-pong at multi-M ops/sec |
| CGO batch size | `batchFlushCount` (batcher.go) | 4096 entries per CGO transition |
| Shard count | `numShards` (shard.go) + `kNumShards` (batch_engine.cpp) | 8192 |

---

## Benchmark Interpretation

When reading load test output or Prometheus metrics, keep these in mind:

**Read-heavy P99 < 5 ms at 2 M ops/sec on n2-highmem-64.** Achievable with the `--read-heavy` preset + correct hardware setup. Key facts:

- **Cache hit (>95% of reads at the right size)**: ~200 ns LIRS lookup. At 200 ns/op the per-core ceiling is ~5 M ops/sec; 64 cores → ~320 M ops/sec theoretical → 2 M is well within budget.
- **Cache miss (≤5% with 400 GB cache for ~1.5 B small keys)**: 1 NVMe random read via the C++ `UringReader` with SQPOLL — ~80 µs P99 on n2-highmem-64 local SSD. 5% × 80 µs + 95% × 200 ns ≈ 4.2 µs blended P99 — order of magnitude under the 5 ms target.
- **The hot-path bottleneck at 2 M ops/sec was the EWMA CAS-loop in `Get()`** — every read CAS'd the same `ReadLatencyEWMANs` atomic, so 64 cores ping-ponged a single cache line. Fixed: the CAS now runs only every 64th read (`readEWMASampleEvery=64`). Admission-control lag is ≤ 32 µs at 2 M/s — negligible vs the 10–20 ms admission thresholds.
- **What still hurts P99**: GC bandwidth competing with reads (mitigated by 3-tier I/O priority + the 60→200 MB/s tiered cap), a single hot key crossing CPU NUMA nodes (mitigated by `--numa-aware`), or running without hugepages (TLB misses on the index hash map).
- **Operator checklist for the target**: `--read-heavy --cgo-batch-engine=true --numa-aware=true --sqpoll-reader=true`, `vm.nr_hugepages ≥ 2048`, mlockall enabled, `--local-ssd-interface=NVME` GKE node pool, clients use MGET batches of 256 keys with one persistent TCP connection per CPU core.

**WAL + VLog flush windows are the write throughput lever.** Both default to **10 ms**. `WALFlushWindowMs=10` amortises the fdatasync cost across all writers arriving within a 10 ms window. In the KV-separation path, `Put()` submits to WAL and VLog concurrently via `beginAppend()` and waits for both — effective P99 = `max(WAL_wait, VLog_wait) + fdatasync`, not their sum. Before the window: macOS P99 was ~122 ms (one `F_FULLFSYNC` per write). With the 10 ms window: macOS P99 ~19 ms, Linux NVMe P99 ~10.2 ms. Write throughput is 10× higher at low concurrency because N writes share 1 fdatasync instead of paying N times. **To hit 100K+ writes/s**: set both windows to 5 ms (`--wal-flush-window-ms 5 --vlog-flush-window-ms 5`); formula: `writes/goroutine/s ≈ 1000 / window_ms` → at 5 ms, 512 goroutines yield ~102K writes/s theoretical ceiling.

**VLog lock-free write path eliminated the mutex bottleneck.** Previously `VLog.beginAppend()` and `VLogBatcher.Stage()` held `vl.mu` during `WriteAt` (~100 µs O_DIRECT). With N=1024 goroutines/disk this caused `1024 × 100 µs = 102 ms` of serialised mutex queue per cycle. Fix: atomic `vl.end.Add(alignedLen)` reserves a non-overlapping offset range before `WriteAt`. Concurrent `pwrite64` to non-overlapping ranges is POSIX-safe. Throughput cap is now NVMe IOPS (~450K/disk) instead of `1/WriteAt_latency` (~10K/disk). This is why increasing the window past 10ms gave only marginal gains (93→97 batch_size) — the real bottleneck was the mutex, not the window.

**`--safe-reads` loadtest flag eliminates false "not found" errors during ingestion.** A `writtenHWM atomic.Int64` high-water mark tracks how many keys have been written. Writers claim sequential slots via `Add(1)-1`; readers pick from `[0, hwm)`. Use `--safe-reads` during mixed-mode benchmarks where the keyspace isn't yet fully populated.

**macOS write throughput is hardware-limited, not code-limited.** `F_FULLFSYNC` costs ~7–10 ms on macOS vs ~0.2–0.5 ms on Linux NVMe. Even with the flush window, macOS individual `Put` achieves ~21K writes/s and `MultiPut-1024` achieves ~108K writes/s (1024 entries / 9.44 ms per fdatasync). On Linux NVMe (n2-highmem-64, ~2.4 ms fdatasync) the same `MultiPut-1024` projects to ~426K writes/s; with 8-disk striping the engine targets 500K+ writes/s. Cache-hit `GET` on macOS measured at 1.44M reads/s (693 ns/op).

**`veltrixdb_cache_misses_total` includes key-not-found.** In a load test where `--num-keys=1000000` and only 30% of operations are writes, the keyspace is ~36% populated by end of test. ~78% of all GETs hit keys that don't exist → all counted as misses. To measure true cache behavior, use `--num-keys=100000` or run a dedicated write phase before reading.

**`veltrixdb_reads_total` was zero before the fix.** `Get()` in `storage/engine.go` called `se.metrics.Reads.Add(1)` — wait, it was **missing** this call. Fixed: the line is now the first statement in `Get()`. If you see `reads_total=0` with active traffic, check you're running the patched code.

**`veltrixdb_fd_failed_nodes` noise in single-node mode.** If this counter ticks up continuously despite no actual failures, `SetLocalNode` was not called before `fd.Start()`. See Invariant 11.

**Admission control activates at read EWMA > 4 ms; watch `veltrixdb_storage_write_admission_throttles_total`.** When read P99 EWMA crosses 4 ms, every `Put()` sleeps 2 ms before submitting to WAL+VLog, and the VLog compactor halts. If this counter climbs rapidly, read latency is high enough to impair writes — diagnose with `veltrixdb_storage_read_latency_seconds` histogram. GC bandwidth throttling (60 MB/s cap) kicks in earlier at 3 ms EWMA to reduce pressure before full pause.

**`veltrixdb_storage_write_admission_throttles_total`** counts Put operations delayed by admission control. A non-zero rate means read EWMA has crossed 4 ms. Normal rate during healthy operation: 0.

**Diagnosing `vlog_gc_runs_total = 0` with the new GC diagnostic counters.** Six new counters tell you exactly where VLog GC is stalling:
- `veltrixdb_vlog_gc_skipped_ratio_total` — GCRatio was below the threshold every tick. Check `veltrixdb_vlog_garbage_ratio` and `--gc-threshold`.
- `veltrixdb_vlog_gc_skipped_paused_total` — `GCPaused` was true and read EWMA was NOT stale. A slow read bumped the EWMA above 4 ms and no subsequent fast reads brought it back. Staleness auto-clears after `gcEWMAStaleDuration` (4 min), so this counter should stop rising within 4 min of the workload going read-light.
- `veltrixdb_vlog_gc_skipped_empty_total` — candidates list was empty. All index entries have `DiskOffset = 0` (KV separation off) or all entries are tombstones.
- `veltrixdb_vlog_gc_read_errors_total` — ReadValue is failing inside the GC loop (likely bad magic/CRC on the VLog record). Indicates VLog corruption or a disk-index mismatch.
- `veltrixdb_vlog_gc_cas_fails_total` — CAS is failing because concurrent Puts are updating every candidate before GC can move them. Normal at high write rates; should be < 50% of candidates.
- `veltrixdb_vlog_gc_candidates_total` — total live entries scanned. If this is 0 and `skipped_empty` is rising, the index is empty or `diskIdx` routing is wrong.

---

## What NOT to Change Without Understanding

- `sectorSize = 512` in `storage/segment.go` — tied to O_DIRECT kernel contract AND `ioPool` alignment logic
- `numShards = 8192` — bitmask `& 0x1FFF` is baked into shard routing AND the C++ `kNumShards`; both must be changed atomically; existing on-disk data is not portable across shard-count changes without a migration
- `appendCh` buffer size in `storage/wal.go` — cap=4096 absorbs bursts; too small causes backpressure spikes
- `IOSQE_IO_LINK` on consecutive write SQEs — removes the need for explicit fdatasync SQEs in the C++ hot path; removing it breaks write ordering
- `sqe->ioprio` RT class for reads — removing it re-introduces 100+ ms read tail latency during write bursts
- `RecordHeader` field order in `cpp/include/shard.hpp` — fields are ordered to eliminate implicit compiler padding so the struct is exactly 64 bytes. The explicit `_pad0[2]` after `key_len` replaces the 2-byte implicit gap; `_pad[37]` fills to 64. The static_assert enforces this. Do not reorder fields without recomputing the padding.
- `ArtSlabAllocator` in `cpp/include/art.hpp` — uses 256 KB slabs with 64-byte cache-line alignment via a composed `SegmentedPool`. The removed `allocator.hpp` stub (which inherited from `SegmentedPool` and called `enable_hugepages(true)`) was non-functional — it had no `make<T>()` or `make_leaf()` methods and caused a class redefinition when `art.hpp` was included. Do not re-add the stub.
