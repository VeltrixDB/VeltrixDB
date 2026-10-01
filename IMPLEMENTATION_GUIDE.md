# VeltrixDB Implementation Guide

For developers contributing to or extending VeltrixDB. Explains the internals and key design decisions.

---

## Repository Layout

```
VeltrixDB/
├── cmd/
│   ├── server/         TCP server, binary protocol, pipeline coalescing
│   │   └── pprof.go    --pprof-addr profiling listener (off by default)
│   ├── loadtest/       Concurrent load tester with percentile stats
│   └── admin/          Scan, stats, compact, check, repair, hash-password
├── storage/
│   ├── engine.go       Core PUT/GET/DELETE, admission control
│   ├── batch.go        MultiPut / MultiGet (multiPutKVSep)
│   ├── group_commit.go Adaptive / fixed group-commit pacer shared by WAL and VLog
│   ├── wal.go          Write-Ahead Log with group-commit
│   ├── wal_format.go   WAL record encodings (binary default, legacy text) + the one decoder
│   ├── wal_replay.go   WAL replay on startup, WAL path helpers
│   ├── vlog.go         Value Log (WiscKey KV separation), VLogBatcher
│   ├── batcher.go      Async WriteBatcher (fire-and-forget API)
│   ├── cache.go        LIRS cache, value-aware eviction
│   ├── cache_sharded.go  Up to 256 LIRS shards keyed on high hash bits
│   ├── bloom_shard.go  Lock-free per-shard bloom filters
│   ├── defrag.go       VLog GC, GC bandwidth throttle, admission staleness guard
│   ├── shard.go        8192-shard index
│   ├── index_table.go  entryTable interface + Go map table, VELTRIXDB_INDEX selection
│   ├── index_table_native.go  cgo binding for the off-heap native index (cgo, Go >= 1.21)
│   ├── native_index.cpp    Per-shard C++ index table (+ native_index_capi.h)
│   ├── segment.go      SegmentWriter, O_DIRECT record format
│   ├── segment_linux.go    O_DIRECT file open + ioPool (Linux)
│   ├── segment_other.go    Buffered file open + ioPool (macOS/Windows)
│   ├── cgo_bridge.go       CGO bridge (Go < 1.21)
│   ├── cgo_bridge_pinner.go  CGO bridge with runtime.Pinner (Go >= 1.21)
│   ├── cgo_toggle.go       VELTRIXDB_DISABLE_CGO_ENGINE / VELTRIXDB_URING_BRIDGE switches
│   ├── vlog_flush_bridge_linux.go  Opt-in io_uring VLog write bridge (Linux cgo)
│   ├── hnsw.go / hnsw_pq.go / pq.go / vector_index.go / vector_adj*.go  Vector search
│   ├── text_index.go / hybrid.go / search_hooks.go  BM25, RRF fusion, reserved-key hooks
│   ├── index_hugepage_linux.go  2MB hugepage mmap helper (no caller)
│   └── mlockall_linux.go    mlockall helper (no caller in the server)
├── cpp/
│   ├── include/
│   │   ├── art.hpp         ART index + ArtSlabAllocator
│   │   ├── scheduler.hpp   3-tier io_uring priority scheduler
│   │   ├── vlog.hpp        VLog C++ reader (io_uring SQPOLL)
│   │   ├── batch_engine.hpp  VeltrixBatchEngine C API
│   │   ├── lockfree_index.hpp  Hugepage-backed lock-free hash map
│   │   ├── numa_topology.hpp   NUMA node discovery
│   │   └── uring_reader.hpp    SQPOLL SSTable reader
│   └── src/              C++ implementations (most have no Go call site — see C++ Components)
├── netfront/             Opt-in C++ network front-end (--net=cpp|uring|poll)
│   ├── netfront.cpp      Per-core event loops: io_uring (Linux) or poll()
│   ├── netfront.go       cgo binding; vxnfExec runs one loop iteration's requests
│   └── stats.go          Per-stage latency histograms, logged at shutdown
├── cmd/server/ (more)  coordinator.go (write chokepoint per --mode), raft_fsm*.go, ext_ops.go,
│                       search_fanout.go, rebalancer.go, cluster_setup.go
├── consensus/            Raft: election, replication, snapshots, membership, ReadIndex
├── cluster/
│   ├── partition_map.go    Consistent hash ring, partition assignment, RoutingKey
│   ├── epoch.go            Split-brain epoch fencing
│   ├── gossip.go           TCP gossip (JSON digests)
│   ├── failure_detection.go  Heartbeat-based failure detector
│   └── partition_transfer.go  TransferAgent: key migration to new owners after a rebalance
├── replication/          Async/quorum/strong replication, vector clocks
├── metrics/prometheus.go Prometheus metrics collector
├── hardware/             Hardware detection, auto-config, OS tuning
├── security/             TLS config, auth enforcer, RBAC
└── scripts/
    ├── build.sh          Full build: C++ + Go + tarball
    ├── net-bench.sh      Front-end / storage-config benchmark (.github/workflows/net-bench.yml)
    ├── hugepages.sh       Host prep: hugepages, NVMe scheduler, ulimits
    └── sysctl.conf        Drop-in /etc/sysctl.d/ for production VMs
```

---

## Critical Invariants

These must never be violated. Breaking them corrupts data or causes silent bugs.

### 1. Shard routing must stay consistent

```
shard = FNV-1a(key) & 0x1FFF         (8192 shards, 0–8191)
disk  = shard % numDisks              (which physical disk)
```

Both Go (`numShards = 8192`) and C++ (`kNumShards = 8192`) must always match. Every `IndexEntry.DiskOffset` is only valid on the disk corresponding to `shard % numDisks`. Changing either constant without migrating on-disk data will corrupt all existing data.

### 2. Segment records are 512-byte aligned

O_DIRECT requires 512-byte sector alignment. `sw.end` is only ever incremented by `alignedLen` (rounded up to 512). Never write a partial sector.

### 3. Use WriteAt, not Seek+Write

`pwrite64`/`pread64` are atomic with respect to file position — safe for concurrent goroutines using the same file descriptor. `Seek`+`Write` is not atomic.

### 4. ioPool stores full capacity

```go
pool.put(buf)  // stores b[:cap(b)], NOT b[:len(b)]
```

On Linux, the backing array has extra bytes for alignment padding. Storing the full capacity preserves alignment for the next `get()` caller.

### 5. binPayloadPool is safe after engine.Put returns

`Put` is synchronous — it blocks until both the WAL and the VLog fdatasync complete. The WAL serialiser, the VLog `beginAppend` and segment `WriteRecord` all copy the value bytes before `Put` returns (the binary PUT handler also copies the value before calling it). Safe to pool the buffer after that point.

### 6. WAL does not use O_DIRECT

`O_DIRECT + O_APPEND` has kernel bugs on some Linux versions. WAL uses buffered I/O; group commit compensates for the extra fdatasync cost.

### 7. VLog before WAL in the PUT path

`vl.beginAppend(writeBytes)` (the transformed value) runs before `wal.beginAppend(entry)`, so the WAL record carries the VLog offset. If the process crashes after VLog fdatasync but before WAL fdatasync, the value is orphaned in the VLog but the key is never written to the index — invisible, harmless.

### 8. WAL and VLog flush windows must be equal

Both default to 15 ms. The PUT path submits to both concurrently and waits for `max(WAL_wait, VLog_wait)`. If the windows differ, the shorter one finishes first but the caller still waits for the longer one — you get the latency cost of the longer window with no throughput benefit from the shorter one.

With the default adaptive group commit (`storage/group_commit.go`) the window is only an upper bound: a lone writer is synced immediately and concurrent writers after an idle gap equal to the EWMA fdatasync time, clamped to 20 µs–2 ms (`adaptiveMinGap` / `adaptiveMaxGap`). `--group-commit=fixed` restores the full-window wait. Both flushers use the same pacer, so they still finish a batch at about the same time. `TestGroupCommit_AdaptiveGates` fails if a lone writer waits for the window or concurrent writers stop sharing syncs.

### 9. VLog.MarkDead must be called on every overwrite or delete

`MarkDead(valueLen, packed)` decrements the live-byte counter (`packed` must come from `entry.IsPacked()`) used by `GCRatio()`. Missing this call causes VLog to grow without bound because the GC never thinks there is enough garbage to trigger.

### 10. vlogBlockSize must stay 4096

XFS (default block size = 4096) and 4Kn NVMe drives both require 4096-byte O_DIRECT alignment. 512-byte writes return `EINVAL` on these filesystems. The VLog write path uses `vlogBlockSize = 4096` as its alignment unit.

### 11. SetLocalNode before Start in the failure detector

```go
fd.SetLocalNode(nodeID)  // must come first
fd.Start()
```

Without this, the local node has zero heartbeats in the `nodeHeartbeats` map. `checkNodeHealth` computes `timeSinceHeartbeat = now - 0 = ~epoch_ns` which always exceeds the 10s threshold, causing constant FAILED/Recovering cycles.

### 12. IndexEntry fields double as VLog pointer when KV separation is on

When `KeyValueSeparation = true`:
- `DiskOffset` = byte offset of the VLog record header
- `SegmentID` = disk index (which VLog file)
- `ValueSize` = on-disk blob length (after compression + encryption); `UncompressedSize` is the plaintext length

Do not interpret these as segment-file pointers when KV separation is enabled.

### 13. VLog magic is 0x564C5402, segment magic is 0x564C5401

They use different helpers — VLog: `openVLogFile` + 4096-aligned `vlogIOPool`; segment: `openSegmentFile` + 512-aligned `ioPool` (both O_DIRECT on Linux) — and their headers are completely different:
- VLog: 24-byte header
- Segment: 64-byte header

Do not parse them interchangeably.

---


### 14. Search indexes are updated only by the reserved-key hooks

Vectors (`@vec/`), text documents (`@txt/`), vector namespace settings (`@vecns/`) and replicated index definitions (`@idxdef/`) are ordinary keys; `Put`, `Delete` and `MultiPut` call `onSearchKeyPut` / `onSearchKeyDelete` after the write commits. Never refresh an index next to a Put yourself — the hook already ran, and a second vector insert is an update that leaves a tombstone. A write path that bypasses those three functions must call the hooks.

### 15. Derived keys route as their record

`cluster.RoutingKey` hashes `@vec/<ns>/<id>` and `@txt/<ns>/<id>` as `<id>` and `@idx/<rule>/<value>/<id>` as `<id>`, so a rebalance keeps a record with its vector, text and index entries. `@vecns/` and `@idxdef/` are copied to every node, never moved. Distributed search filters locally and depends on this.

### 16. Distributed search fails closed

A peer that does not answer fails the search (and names the peer) unless `--search-allow-partial`; so does a node whose indexes are still rebuilding after a restart. Do not skip failed peers by default — a partial result looks like a correct one.

### 17. Every read passes `coordinator.readBarrier()`

GET, text MGET, binary MGET and the coalesced binary GET path. In raft mode it makes a newly elected leader wait until its term's no-op is applied, so writes the previous leader acknowledged are visible; with `--linearizable-reads` it runs the ReadIndex fence. A new read path that calls the engine directly loses both.
## Key Data Structures

### IndexEntry (Go, 64 bytes — `storage/types.go`, size checked in `init`)

```go
type IndexEntry struct {
    KeyHash          uint64  // fnv64a(key); the key itself lives in the table
    DiskOffset       uint64  // byte offset in VLog or segment file
    SegmentID        uint32  // disk index
    ValueSize        uint32  // on-disk blob length
    UncompressedSize uint32  // plaintext length
    KeySize          uint32
    WriteTimestampUs int64
    TTLExpiryUs      int64
    CRC32C           uint32  // plaintext CRC
    ShardID          uint16
    Flags            uint8   // Tombstone 0x01, Compressed 0x02, Encrypted 0x04, HasTTL 0x08,
                             // ReadRepairNeeded 0x10, Pinned 0x20, Packed 0x40, Tiered 0x80
    SchemaVersion    uint8
    _reserved        [8]byte
}
```

There is no per-entry version: `se.version` is engine-wide and is written to the WAL record.

### WALEntry

```go
type WALEntry struct {
    Timestamp     int64
    KeyLen        uint32
    Key           string
    ValueLen      uint32   // plaintext length
    Value         []byte   // nil when VLogOffset > 0
    Checksum      uint32   // CRC32C of the plaintext value
    ReplicationID uint32
    Version       uint64
    IsTombstone   bool
    VLogOffset    int64    // > 0 means value is in VLog
    Packed        bool     // VLog record shares a 4 KB block
    DiskValueLen  uint32   // on-disk length after transforms (0 = ValueLen)
    XformFlags    uint8    // FlagCompressed | FlagEncrypted
}
// The response channel is not on the entry: beginAppend returns *chan error.
```

On-disk format (binary, the default — `storage/wal_format.go`): a 48-byte little-endian header, then the key, then the value when it is inline, then a 4-byte CRC32C of every preceding byte.
```
0  magic 0xB1 | 1 version (1) | 2-3 flags (tombstone, packed, inline)
4  keyLen | 8 valueLen (plaintext) | 12 diskLen | 16 CRC32C of value
20 xflags (compressed, encrypted) | 21-23 zero
24 timestamp | 32 version | 40 vlogOffset
48 key bytes [value bytes if inline] CRC32C(record)
```

The legacy text format (`ts|tomb|key|valueLen|crc|version|vlogOffset|packed|diskLen|xflags\n[value\n]`) is still read, mixed record by record with binary. It is written only with `--wal-format=text`. Split on `|` and `\n`, it truncated replay at any key containing either byte. To roll back to a pre-binary build, run once with `--wal-format=text` and stop cleanly. Crash replay, the PITR archiver and PITR restore all decode through the same reader in `wal_format.go`.

### VLog Record (24-byte header)

```
Magic:     [4 bytes]  0x564C5402
ValLen:    [4 bytes]  unpadded value length
CRC32C:    [4 bytes]  CRC32C of value bytes
Reserved:  [4 bytes]
WriteUs:   [8 bytes]  write time, Unix µs
[value bytes; an unpacked record is padded to the next vlogBlockSize (4096)
 boundary, a packed record is followed directly by the next record in its block]
```

### Segment Record (64-byte header)

```
0  Magic     [4 bytes]  0x564C5401
4  Flags     [1 byte]   0x01 tombstone, 0x02 compressed
5  Reserved  [3 bytes]
8  KeyLen    [4 bytes]
12 ValueLen  [4 bytes]
16 CRC32C    [4 bytes]  of key || value
20 Reserved  [4 bytes]
24 WriteUs   [8 bytes]
32 TTLUs     [8 bytes]  absolute µs, 0 = immortal
40 Reserved  [24 bytes]
64 [key bytes][value bytes], record padded to a 512-byte boundary
```

---

## Storage Engine Internals

### Group-Commit WAL

```go
// WAL flusher goroutine (simplified from storage/wal.go)
func (wal *WriteAheadLog) flusher() {
    var bt batchTimer
    for {
        select {
        case item := <-wal.appendCh:
            pending = append(pending, item)
            pendingRecords += item.n          // records, not items
            full := drain()                    // non-blocking; stops at maxBatch records
            if wal.pacer.schedule(&bt, len(pending), full) {
                bt.stop()
                flush()                        // one write(2) + one fdatasync
            }                                  // else: timer armed (gap or window deadline)
        case <-bt.C:
            bt.stop()
            flush()
        case <-wal.doneCh:
            // drain the channel, final flush, return
        }
    }
}
```

`schedule` flushes at once when the window is 0, the batch is full (`WALMaxBatchEntries`, default 4096), or — in adaptive mode — a lone writer is detected (one request and an EWMA batch size < 1.5). Otherwise it arms the timer for the idle gap, never past the window deadline. Fixed mode arms the full window.

One `write(2)` and one `fdatasync` per flush: `flush()` concatenates the batch's records into one reused buffer and writes it once (CLAUDE.md invariant 41). Never reintroduce a per-entry write. `appendAll` sends a whole `MultiPut` batch on the channel in one send (CLAUDE.md invariant 42). `appendCh` has capacity 4096 to absorb write bursts.

### Lock-Free VLog Write Path

```go
func (vl *VLog) beginAppend(value []byte) (int64, *chan error, error) {
    alignedLen := roundUp(24+len(value), vlogBlockSize) // 4096 bytes
    buf := vlogIOPool.get(alignedLen)                   // 4096-aligned
    defer vlogIOPool.put(buf)
    writeHeader(buf, len(value), crc32c(value), nowUs)  // magic, len, CRC, write time
    copy(buf[24:], value)                               // then zero the padding
    offset := vl.end.Add(int64(alignedLen)) - int64(alignedLen)
    // offset is exclusively reserved — no other goroutine will use it
    vl.file.WriteAt(buf, offset)
    vl.flushCh <- &vlogFlushReq{...}                    // group-commit fdatasync
    return offset, rp, nil                              // caller waits on *rp
}
```

POSIX guarantees `pwrite64` to non-overlapping ranges from concurrent goroutines is race-free. Throughput cap is NVMe IOPS (~450K/disk), not mutex serialisation (~10K/disk).

This is the single-key path. A `MultiPut` batch goes through `VLogBatcher`: `Stage` returns offsets relative to the extent, `Commit` reserves the whole extent with one `vl.end.Add`, and `Flush` does one `pwrite` plus one `fdatasync` (CLAUDE.md invariant 42). On Linux cgo builds, `VELTRIXDB_URING_BRIDGE=on|sqpoll` submits that write through io_uring instead. The bridge is off by default. It can save at most one syscall per batch, and if it fails to start (e.g. seccomp blocks `io_uring_setup`), batches fall back to `pwrite`.

### WriteBatcher (Async Fire-and-Forget)

```go
// High-throughput non-blocking write API
engine.BatchPut("key", value, ttl)  // returns immediately (WriteBatcher.Enqueue)

// Flusher goroutine flushes when:
// - 2 MB of data accumulated   (batchFlushBytes)
// - 4096 entries accumulated   (batchFlushCount)
// - 15 ms timer fires          (batchFlushDur)
// Falls back to synchronous Put when channel is full (cap=65536);
// flush runs the C++ batch engine (cgo builds) and then MultiPut.
```

Use this when you don't need per-write durability confirmation — e.g., time-series ingestion, session writes, logging.

### Server-Side Pipeline Coalescing

When the TCP server receives a PUT, it peeks the bufio buffer for additional complete frames before executing:

```go
func tryCoalescePuts(firstKey string, firstVal []byte, br *bufio.Reader) []storage.MultiPutRequest {
    reqs := []storage.MultiPutRequest{{Key: firstKey, Value: firstVal, TTL: -1}}
    for len(reqs) < maxPipelineBatch { // 256
        // only frames already buffered: Peek the 7-byte header, require
        // cmd == binCmdPut and the whole frame in br.Buffered()
        ...
        reqs = append(reqs, storage.MultiPutRequest{Key: key, Value: val, TTL: -1})
    }
    if len(reqs) < 2 {
        return nil // caller uses coord.Put
    }
    return reqs
}
// Executed as one coord.MultiPut, all N responses written, single bw.Flush()
```

`tryCoalesceGets` does the same for buffered GET frames: one `coord.readBarrier()`, then one `engine.MultiGet`. This means pipelined requests from the same connection automatically get batch throughput without any client changes.

### Index table (Go map or off-heap native)

Each shard's key → `IndexEntry` table is an `entryTable`. On cgo builds (Go >= 1.21, including macOS) the default is the off-heap C++ table in `storage/native_index.cpp`: 64-byte records, 8-byte tag|pos slots, a per-shard key arena. Arrays of a page or more are individually mmap'd (mremap growth on Linux), so the index is invisible to the Go GC; arrays from 2 MiB are also `MADV_HUGEPAGE`'d on Linux. `VELTRIXDB_INDEX=map` selects the Go map, `=native` forces the C++ table, and `VELTRIXDB_DISABLE_CGO_ENGINE=1` also selects the map. `CGO_ENABLED=0` builds always use the map.

- Entries cross the boundary **by value**. The `*IndexEntry` passed to `update`/`rangeAll` callbacks is a scratch copy, valid only during the callback (CLAUDE.md invariant 45).
- Large engines need `vm.max_map_count ≥ 262144` (`scripts/sysctl.conf`). Below that, an insert can fail with ENOMEM (CLAUDE.md invariant 46).
- The C++ source lives in `storage/`, not behind an `#include` shim, so the Go build cache tracks it (CLAUDE.md invariant 44).

Measured with 5M keys: full GC 21 ms → 0.27 ms, settled RSS 168 → 142 B/key. Each cache-miss lookup costs ~19 ns more.

### Ordered index

A put calls `ordered.Insert` only when the key was absent or tombstoned. A live key is already in the skiplist (CLAUDE.md invariant 49). The same rule applies to WAL replay and the atomic-op install path.

### Read without I/O

`StorageEngine.GetNoIO(key)` answers from the cache, the index (absent, tombstone, expired) or a dirty value. When the value needs a VLog read, it returns `needIO=true` instead of reading. `GetAfterNoIO(key)` completes the read. The pair counts as one read. The C++ network front-end uses this pair to keep disk reads off its loop threads.

### CGO Batch Engine

The C++ `VeltrixBatchEngine` (Linux cgo builds) handles batch put/get with shard-parallel execution. It is used only by the fire-and-forget `BatchPut`/`WriteBatcher` path. `VELTRIXDB_DISABLE_CGO_ENGINE=1` skips it.

**Go < 1.21 (`cgo_bridge.go`):**
```go
// Pass pointer as uintptr to avoid "Go pointer to Go pointer" CGO rule
ptr := uintptr(unsafe.Pointer(&keys[0]))
C.veltrix_batch_put(engine, C.uintptr_t(ptr), ...)
runtime.KeepAlive(keys) // prevent GC from moving the array
```

**Go >= 1.21 (`cgo_bridge_pinner.go`):**
```go
var p runtime.Pinner
p.Pin(&keys[0])
defer p.Unpin()
C.veltrix_batch_put(engine, (*C.char)(unsafe.Pointer(&keys[0])), ...)
```

`runtime.Pinner` is the correct modern API — it pins the Go object so the GC won't move it while C++ holds a pointer.

---

## C++ Network Front-End (netfront/)

`--net=go` (default) is the Go server. `--net=cpp` picks io_uring when it is available and poll otherwise. `--net=uring` and `--net=poll` force a backend. The front-end runs one event loop per core (`--net-threads`, default NumCPU), each with its own `SO_REUSEPORT` listener. It uses io_uring on Linux (liburing) and `poll()` elsewhere (macOS runs one loop).

- **Scope:** binary PUT GET DEL PING MPUT MGET only, in standalone mode. The server refuses `--auth-config`. The text protocol, AUTH, TLS and the extended binary commands need `--net=go`.
- **One Go call per loop iteration:** `vxnfExec` receives every request that arrived in that iteration.
- **Per-connection strict order:** a connection with a write or deferred read in flight is not parsed further (CLAUDE.md invariant 50).
- **Deferred reads:** GET/MGET probe with `GetNoIO` on the loop. A key that needs a VLog read is finished in a goroutine, and `vxnf_defer` blocks the connection until it is answered. `VELTRIXDB_NET_DEFER_READS=0` restores inline reads for A/B.
- **Shutdown report:** the server logs per-stage latencies in log2 µs buckets: `iter`, `cb_enter`, `exec`, `put_sched`, `put_exec`, `wake`, `defer_sched`, `defer_exec`.

The front-end is opt-in and experimental. It is not faster overall. On the 4-CPU CI runner (tmpfs, client on the same host, 2026-09-25, before read deferral), it cut read P50 by ~45% versus `--net=go`, but read P99 was worse and batch writes were ~12% lower. io_uring did not beat poll. Compare with `scripts/net-bench.sh`. A Mac cannot show networking gains: loopback caps round trips at ~60K/s for both front-ends.

---

## C++ Components

> **Status:** the ART index, the io_uring priority scheduler, the C++ VLog reader, the SQPOLL `UringReader`, the lock-free index and the eBPF GC throttle below are compiled (or header-only) but have **no Go call site**. The live VLog read path is Go (`storage/vlog.go` `ReadValue`). The C++ code that runs is the native index, the batch engine, the opt-in io_uring write bridge and `netfront/`. See [cpp/README.md](cpp/README.md).

### ART Index (cpp/include/art.hpp)

The Adaptive Radix Tree stores key metadata in the C++ layer. Node types:

| Type | Children | When used |
|------|----------|-----------|
| Node4 | 1–4 | Sparse subtrees |
| Node16 | 5–16 | SSE2 SIMD key search |
| Node48 | 17–48 | Indirect child index |
| Node256 | 49–256 | Dense subtrees, O(1) lookup |

Memory: `ArtSlabAllocator` allocates from 256 KB cache-line-aligned slabs (64-byte alignment). Uses `SegmentedPool` base; optionally backed by 2 MB hugepages.

**Janitor compaction:** Rewrites live records in ART key order (lexicographic). After compaction, range scans read sequentially from disk — no random I/O.

### io_uring Priority Scheduler (cpp/include/scheduler.hpp)

Three priority tiers, each with its own deque:
1. **Tier 0** (reads) — `IOPRIO_CLASS_RT` — kernel guarantees these beat writes at the block device level
2. **Tier 1** (writes) — `IOPRIO_CLASS_BE, level 4`
3. **Tier 2** (compaction) — `IOPRIO_CLASS_IDLE`

Write batching: accumulates up to 32 write SQEs (`write_batch_limit`) or 1 ms (`write_batch_window_us`), then submits one `io_uring_submit`. `IOSQE_IO_LINK` chains consecutive write SQEs so they execute in order without explicit fdatasync SQEs.

`write_pending_count_` uses `memory_order_release` on increment and `memory_order_relaxed` on decrement. This is intentional — it's a monitoring hint, not a synchronization primitive.

### VLog Reader — UringReader (cpp/include/uring_reader.hpp)

`IORING_SETUP_SQPOLL` keeps a kernel thread polling the submission ring — no `io_uring_enter` syscall needed per read. Pre-registered fixed buffers (`io_uring_register_buffers`) avoid per-I/O mmap/munmap overhead.

A dedicated completion thread (pinned to the NVMe's NUMA node) drains the completion queue and dispatches results.

### NUMA Topology (cpp/include/numa_topology.hpp)

```cpp
NumaTopology topo;
int node = topo.nvme_preferred_node("/dev/nvme2n1");
pin_thread_to_node(node);
```

Reads `/sys/class/nvme/<dev>/device/local_cpulist` to find which CPUs have IRQ affinity for each NVMe disk. Cross-NUMA I/O adds ~90 ns per access on 2-socket servers.

### Lock-Free Index (cpp/include/lockfree_index.hpp)

Hugepage-backed open-addressing hash map. **Not wired, and not safe as an index**: it stores only a 64-bit key hash (a collision returns another key's entry). The live index is `storage/native_index.cpp`.
- 32-byte buckets (2 per 64-byte cache line)
- 50% load factor
- CAS upsert (`compare_exchange_strong`, release ordering)
- Single `acquire` load per read
- 2 MB hugepages: at 1B keys × 32 bytes = 32 GB → ~16K TLB entries vs ~8M with 4 KB pages

---

## Cluster and Replication

### Partition Map

- 256 partitions by default (`ClusterConfig.PartitionCount`), assigned round-robin over active nodes sorted by ID, with rack-aware replicas (`pickReplicas`); every member computes the same table deterministically in `Rebalance`
- Keys are placed on a consistent-hash ring: FNV-1a + fmix64 finalizer over `RoutingKey(key)`, 64 virtual nodes per physical node
- Gossip digests carry the epoch, the partition-map version and per-node state / heartbeats, not the partition table itself

### Failure Detector

State machine: `Active → Suspect → Failed → Recovering` (plus `Draining` for a graceful leave)

```
HeartbeatInterval: 1 s (health-check tick)
SuspectThreshold:  3 s without heartbeat
FailureThreshold: 10 s without heartbeat
RecoveryInterval:  5 s
```

Critical: call `fd.SetLocalNode(nodeID)` before `fd.Start()` — see Invariant 11.

### Replication Modes

| Mode | Latency | When to use |
|------|---------|-------------|
| Async | + | Best throughput; data may lag on replicas |
| Quorum | ++ | replicationFactor/2+1 copies (local counts as one); balanced durability |
| Strong | +++ | All replicas ACK; strongest guarantee |

---

## Build System

```bash
# Full build (Linux, requires liburing + CMake)
VERSION=1.0.0 ./scripts/build.sh --output ./dist

# Go only (macOS or CI without liburing)
./scripts/build.sh --go-only --output ./dist

# Just compile Go (cgo on by default: native index; on Linux netfront links liburing,
# or add -tags vxnf_nouring for a poll()-only front-end)
go build ./...

# Pure-Go build (Go map index, no C++ at all)
CGO_ENABLED=0 go build ./...
```

After editing C++ that a `storage/cgo_*_linux.cpp` shim `#include`s from `cpp/src/`, test with `go test -a`. The Go build cache does not track included files, so a plain `go test` can re-run a stale object (invariant 44).

`#cgo` directives use `-march=x86-64-v2` on amd64, never `-march=native`. `scripts/build.sh` re-adds `-march=native` through `CGO_CXXFLAGS` when it builds on the target (invariant 48). The Docker image is built with `CGO_ENABLED=1`. Pass `--build-arg CGO_ENABLED=0` for the old static image.

`scripts/build.sh` uses `(cd REPO_ROOT && go build ...)` subshell, not `go build -C`. The `-C` flag requires Go 1.21+; VeltrixDB targets Go 1.19+.

C++ namespace is `veltrix`. Do not use or reintroduce `namespace axon` — all headers were renamed.

---

## Defragmenter Config (Important C++ note)

`Defragmenter::Config` must not use non-static data member initializers (NSDMIs):

```cpp
// WRONG — GCC rejects this (CWG 1497)
struct Config {
    double gcRatio = 0.30;      // NSDI — compiler sees incomplete class
};

// CORRECT — use constructor initializer list
struct Config {
    double gcRatio;
    Config() : gcRatio(0.30) {}
};
```

This applies to any nested struct used as a default argument inside a class body.

---

## Adding a New Command

1. Add a command byte constant in `cmd/server/main.go`. It must be **≤ 0x29**: the first byte of a connection decides binary vs text, and 0x2A+ is printable ASCII. Only 0x1D is still free — add search-style commands as sub-ops of `SEARCH` (0x1C) instead, and dispatch the new byte to `handleExtOp` explicitly
2. Add a `handleXxx` function that reads the frame and calls the storage engine
3. Add the command to the `switch` in the binary protocol dispatch loop
4. Route mutations through the `coordinator` (`cmd/server/coordinator.go`) so raft and replicated modes see them; a new raft op is appended at the END of the `fsmOp` iota in `raft_fsm.go` (values are persisted in the log)
5. Reads must call `coord.readBarrier()` first (invariant 17)
6. Add the op to the in-repo Go client (`client/`); the other SDKs live in the separate Veltrixdb-client repo
7. Add a Prometheus metric to `metrics/prometheus.go` if needed
8. Update the wire protocol section in `README.md`

A new command is served only by `--net=go` unless you also add it to `netfront/`. There it must keep the per-connection ordering rules (invariant 50) and must not read from disk on a loop thread.

---

## Changing the Disk Count

There is no runtime disk add: the storage engine has no `AddDisk`, and the disk count is fixed by `--data-dirs` at startup. Do not change the number of `--data-dirs` on an existing data set. Routing is `shard % numDisks`, and WAL replay skips every record whose key does not route to the disk it was found on under the *current* count (`storage/wal_replay.go`). Restarting with a different count therefore drops those keys from the index. To change the disk count, move the data through the cluster (add a node with the new layout and let rebalance migrate keys) or export/import with `cmd/veltrix-migrate`.

Do not change `numShards` — the shard count is permanently baked into on-disk data.

---

## Prometheus Metrics Reference

| Metric | Type | Description |
|--------|------|-------------|
| `veltrixdb_storage_reads_total` | Counter | Total GET operations |
| `veltrixdb_storage_writes_total` | Counter | Total PUT operations |
| `veltrixdb_storage_deletes_total` | Counter | Total DEL operations |
| `veltrixdb_cache_hits_total` | Counter | LIRS cache hits |
| `veltrixdb_cache_misses_total` | Counter | LIRS cache misses (includes key-not-found) |
| `veltrixdb_storage_wal_flushes_total` | Counter | WAL fdatasync count |
| `veltrixdb_storage_read_latency_seconds` | Histogram | GET latency distribution |
| `veltrixdb_storage_write_admission_throttles_total` | Counter | Writes delayed by admission control |
| `veltrixdb_vlog_gc_runs_total` | Counter | VLog GC cycles |
| `veltrixdb_vlog_garbage_ratio` | Gauge | Fraction of VLog that is dead data |
| `veltrixdb_vlog_gc_skipped_ratio_total` | Counter | GC skipped: ratio below threshold |
| `veltrixdb_vlog_gc_skipped_paused_total` | Counter | GC skipped: admission control active |
| `veltrixdb_vlog_gc_skipped_empty_total` | Counter | GC skipped: no candidates |
| `veltrixdb_vlog_gc_read_errors_total` | Counter | VLog read errors during GC |
| `veltrixdb_vlog_gc_cas_fails_total` | Counter | CAS failures during GC (concurrent writes) |
| `veltrixdb_vlog_gc_candidates_total` | Counter | Live entries scanned by GC (cumulative) |
| `veltrixdb_vlog_gc_emergency_runs_total` | Counter | GC passes that bypassed the admission pause (garbage ≥ 65%) |
| `veltrixdb_storage_native_index_bytes` | Gauge | Off-heap native index memory |
| `veltrixdb_failure_detector_nodes_failed_total` | Counter | Nodes marked FAILED (compare with `..._nodes_recovered_total`) |
| `veltrixdb_cluster_nodes_total` | Gauge | Known cluster members |

WAL batch size efficiency:
```promql
rate(veltrixdb_storage_writes_total[1m]) / rate(veltrixdb_storage_wal_flushes_total[1m])
```
Target > 100 at 10 ms window + 10K writes/s/disk in fixed mode. In adaptive mode (default) the ratio follows concurrency: ~1 for a lone writer, ~N for N concurrent writers (64 writers → 64 writes per flush measured).
