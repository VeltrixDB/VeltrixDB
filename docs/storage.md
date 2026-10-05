# Data Storage in VeltrixDB

VeltrixDB uses a WiscKey-inspired **key-value separation** architecture: keys (with metadata) live in a RAM-resident index, while values live in an append-only on-disk log. This section explains every layer from client write to durable bytes on disk.

---

## Architecture Overview

```
┌────────────────────────────────────────────────────────────────────┐
│  Client write (PUT key value)                                      │
└──────────────────────────┬─────────────────────────────────────────┘
                           │
                    ┌──────▼──────┐
                    │  TCP Server │  binary or text protocol
                    └──────┬──────┘
                           │  StorageEngine.Put(key, value, ttl)
          ┌────────────────▼────────────────────┐
          │         StorageEngine               │
          │  1. Transform: compress → encrypt   │
          │  2. Compute shard = FNV-1a(key)     │
          │                     & 0x1FFF        │
          │  3. diskIdx = shard % numDisks      │
          │  4. vl.beginAppend(value)  ─────────┼──► VLog[diskIdx]
          │  5. wal.beginAppend(entry) ─────────┼──► WAL[diskIdx]
          │  6. await both fdatasyncs           │
          │  7. update Index Vault[shard]       │
          └─────────────────────────────────────┘
```

---

## 1. Sharding (In-Memory Index)

The index is divided into **8192 shards**. Each shard has its own `sync.RWMutex` so reads and writes to different shards run in parallel.

```
shard_id = FNV-1a(key) & 0x1FFF       // 0..8191
disk_idx = shard_id % numDisks        // which NVMe disk owns this shard
```

Anything sized *per shard* is multiplied by 8192. See the sizing note in
[ARCHITECTURE.md](../ARCHITECTURE.md#sharding).

Each shard holds a key → `IndexEntry` table — the full live key space is always in memory (the Index Vault). There is no on-disk B-tree or LSM tree for the index; durability comes from the WAL.

The table implementation is chosen per process:

| Build | Default | Notes |
|-------|---------|-------|
| cgo (incl. macOS) | Off-heap native table (`storage/native_index.cpp`) | Per-shard C++ open-addressing table plus key arena, invisible to the Go GC. Arrays of a page or more are individually mmap'd, so large engines need `vm.max_map_count ≥ 262144` (`scripts/sysctl.conf`); below that an insert can fail with ENOMEM and panic. |
| `CGO_ENABLED=0` | Go `map[string]IndexEntry` | — |

`VELTRIXDB_INDEX=map` opts a cgo build out of the native table (`=native` forces it); `VELTRIXDB_DISABLE_CGO_ENGINE=1` also selects the map. Measured with 5M keys: full GC 21 ms → 0.27 ms and settled RSS 168 → 142 B/key, at ~19 ns more per cache-miss lookup.

A separate ordered skiplist (`storage/ordered_index.go`) serves range scans. A key is inserted into it only when it becomes live (absent or tombstoned before); overwriting a live key does not touch it.

### IndexEntry (64 bytes, one CPU cache line)

| Field | Size | Purpose |
|-------|------|---------|
| `KeyHash` | 8 B | FNV-1a(key) — for bloom filter fast-path |
| `DiskOffset` | 8 B | Byte offset in VLog (KV-sep on) or Segment file |
| `SegmentID` | 4 B | Disk index when KV-sep on; segment file ID otherwise |
| `ValueSize` | 4 B | Compressed/encrypted byte length on disk |
| `UncompressedSize` | 4 B | Original plaintext byte length |
| `KeySize` | 4 B | Key byte length |
| `WriteTimestampUs` | 8 B | Write time (µs since Unix epoch) |
| `TTLExpiryUs` | 8 B | Absolute expiry (0 = immortal) |
| `CRC32C` | 4 B | Castagnoli checksum of value |
| `ShardID` | 2 B | Shard 0–8191 |
| `Flags` | 1 B | Bitmask (see below) |
| `SchemaVersion` | 1 B | Rolling migration version |
| `_reserved` | 8 B | Future use |

**Flags:**

| Bit | Name | Meaning |
|-----|------|---------|
| 0x01 | `FlagTombstone` | Deleted record |
| 0x02 | `FlagCompressed` | Value is zstd-compressed |
| 0x04 | `FlagEncrypted` | Value is AES-256-GCM encrypted |
| 0x08 | `FlagHasTTL` | TTLExpiryUs is valid |
| 0x10 | `FlagReadRepairNeeded` | Replica lagging; cleared on the next segment read |
| 0x20 | `FlagPinned` | Never evict from the Index Vault |
| 0x40 | `FlagPacked` | VLog record is packed (multiple values per 4 KB block) |
| 0x80 | `FlagTiered` | Value demoted to the cold tier; GC does not relocate it |

---

## 2. Value Log (VLog)

Each disk has one `vlog_active.dat` file — a flat append-only log of value bytes (or, with `--raw-vlogs` on Linux, a raw NVMe block device with a 4 KB superblock at offset 0). Values are never overwritten; a superseded value's space is reclaimed by the defragmenter (GC). Key-value separation is on by default (`KeyValueSeparation: true`); with it off, values go to a per-disk segment file instead.

### VLog File Magic

```
Magic: 0x564C5402  ("VLT\x02")
```

### VLog Record Format

```
Offset  Size  Field
  0      4    Magic (0x564C5402)
  4      4    ValLen  (unpadded value byte count)
  8      4    CRC32C  (Castagnoli checksum of value bytes)
 12      4    Reserved (future: compression/schema flags)
 16      8    WriteTimestampUs (int64, µs since epoch)
─────────────────────────────────────────────────────
 24   ValLen  Value bytes as stored (output of the compress → encrypt pipeline)
24+V   pad    Zero-padding to the next 4096-byte block boundary
```

Every record offset is **4 KB-aligned** (`vlogBlockSize` = 4096), which is O_DIRECT-safe on both XFS and 4Kn NVMe drives. A single `Put` pads its record to a whole 4 KB block. The CRC32C here covers the stored bytes; `IndexEntry.CRC32C` is the checksum of the plaintext.

### Block Packing (High-Density Mode)

For small values (≤ 4096 − 24 bytes per record), the `VLogBatcher` packs multiple records into a single 4 KB block. The batcher is used by `MultiPut` and by GC relocation; a single `Put` still uses one block per record. For 128-byte values, up to 26 records fit per block vs. 1 unpacked. A record larger than a block gets its own unpacked, block-aligned span.

- `FlagPacked` is set on the `IndexEntry` for packed records.
- `MarkDead(valueLen, packed)` subtracts only `header+value` (not the full 4 KB block) when `packed` is true; callers pass `entry.IsPacked()`.
- The read path is unchanged: `ReadValue(DiskOffset, ValueSize)` rounds down to the 4 KB block boundary and extracts the record at `offset % 4096`.

### Write Path (Lock-Free)

VLog offset reservation is lock-free via an atomic counter:

```go
offset = vl.end.Add(alignedLen) - alignedLen   // reserves [offset, offset+alignedLen)
pwrite64(fd, value, offset)                     // POSIX-safe concurrent writes
```

Concurrent `pwrite64` calls to non-overlapping ranges are safe by POSIX. No mutex is held during the I/O — throughput is bounded only by NVMe IOPS (~450K/disk), not lock contention.

**Batch writes** (`MultiPut`) go through the `VLogBatcher`, which stages the whole batch's records into one contiguous 4 KB-aligned extent and writes it with **one `pwrite` plus one `fdatasync`** per disk, however many blocks it spans.

On Linux cgo builds the batch write can instead be submitted through an io_uring bridge (one ring per disk). It is **off by default** — the batch is already a single `pwrite`, so the bridge saves at most one syscall. Opt in with `VELTRIXDB_URING_BRIDGE=on` (or `sqpoll` for a kernel polling thread); `VELTRIXDB_DISABLE_CGO_ENGINE=1` forces it off. If ring setup fails (for example, Kubernetes' RuntimeDefault seccomp profile blocks `io_uring_setup`) the engine logs it and falls back to `pwrite`.

### Read Path

`Get` checks, in order: the LIRS cache, the index (absent / tombstone / expired — an expired key is tombstoned on the spot and returns `ErrKeyExpired`), a dirty value still in RAM, and finally the VLog (`VLog.ReadValue`, Go, CRC32C-verified, then decrypt → decompress); with KV separation off, the segment file instead. `GetNoIO` runs the same steps but stops before the VLog read and returns `needIO=true` instead; `GetAfterNoIO` completes such a key. The pair counts as one read. It exists for callers that must not block on disk, such as the C++ network front-end's event loops.

---

## 3. Write-Ahead Log (WAL)

Each disk has one `wal.log` file. The WAL provides crash durability: on every startup `replayWAL()` reads it and the index is rebuilt from it in the background (see §10). On clean shutdown, a checkpoint rewrites `wal.log` as one record per live key (temp file + atomic rename), so the next startup replays O(live keys) instead of the full write history.

### WAL Record Format

New records are binary (`storage/wal_format.go`), all integers little-endian:

```
Offset  Size  Field
  0      1    Magic 0xB1 (a text record always starts with an ASCII digit)
  1      1    Format version (1; 2 = record carries a TTL, see below)
  2      2    Flags: bit0 tombstone, bit1 packed, bit2 value inline
  4      4    Key length
  8      4    valueLen  (plaintext length)
 12      4    diskLen   (on-disk length after compress + encrypt)
 16      4    CRC32C of the plaintext value
 20      1    xflags: FlagCompressed | FlagEncrypted
 21      3    Zero
 24      8    Timestamp (UnixNano)
 32      8    Version
 40      8    vlogOffset (0 = value inline, or none)
─────────────────────────────────────────────────────
 48    keyLen  Key bytes
  …    valueLen Value bytes (only when the inline flag is set)
 end     4    CRC32C of every byte above
```

**TTL (version 2).** A key written with a TTL gets a version-2 record: the
same 48-byte header with byte 1 = 2, then an 8-byte `int64` at offset 48 — the
key's **absolute** expiry in Unix microseconds (`IndexEntry.TTLExpiryUs`, a
deadline, not a duration) — then key, value and CRC as above. Records without
a TTL are still version 1, byte for byte, so existing WAL files and the no-TTL
write path are unchanged. Every write path that sets a TTL carries it: `Put` /
`PUTEX`, `MultiPut` entries (MPUT, TXN, the WriteBatcher, coalesced PUTs),
`PutNS`, `HSET` / `HEXPIRE` (hash fields are ordinary keys), and CAS / INCR /
DECR / SETNX. Before this the WAL had no TTL field, so after any restart —
crash replay or clean checkpoint — every TTL'd key came back immortal, and a
key that had expired came back to life.

Records are length-prefixed, so key bytes are never interpreted: keys
containing `|` or `\n` are safe. (With the text format such a key made replay
stop at that record and drop every later acknowledged write on crash restart.)
The trailing CRC32C covers the whole record, header included — text headers
had no checksum.

Replay also reads the legacy text records below, in any mix with binary ones
(the first byte decides, per record), so an upgraded node keeps appending
binary records after existing text ones. The field meanings are the same in
both.

**Rollback.** No build older than the TTL field can read a version-2 record or
a text record with the 11th (TTL) field: its decoder treats either as a torn
tail, stops replay there and drops that record **and every record after it**.
So to roll back to *any* older build — pre-binary or not — run the new build
once with `--wal-format=text-legacy` and stop cleanly: the checkpoint rewrites
`wal.log` as 8/10-field text with no TTL field, which every build since the
text format reads; then downgrade. The cost is that keys written with a TTL
come back **immortal** on the older build — what that build did to them on
every restart anyway. `--wal-format=text` writes text *with* the TTL field and
is therefore readable only by builds that have it; it is not a rollback mode.
A PITR archive written by this build is likewise unreadable by an older
`restore-pitr` once it holds a TTL'd key (that restore fails with "truncated
or malformed" rather than silently dropping records).

Legacy text record:

```
timestamp|isTombstone|key|valueLen|crc32hex|version|vlogOffset|packed|diskLen|xflags|ttlExpiryUs
[value bytes]
```

Fields 9–10 are emitted **only when they carry information** (a transform was
applied, or the on-disk length differs from the plaintext length), and field
11 only for a key with a TTL (which forces 9–10 out too, as the layout is
positional; `text-legacy` never writes it). An untransformed, TTL-free record
still writes 8 fields, which is what keeps older binaries able to read
newly-written data.

| Field | Type | Meaning |
|-------|------|---------|
| `timestamp` | int64 | ns since Unix epoch |
| `isTombstone` | "1"/"0" | Delete marker |
| `key` | string | Raw key bytes |
| `valueLen` | decimal | Value byte count |
| `crc32hex` | hex | CRC32C of value |
| `version` | decimal | Monotonic MVCC counter |
| `vlogOffset` | decimal | 0 = value bytes follow inline; >0 = value already in VLog |
| `packed` | "1"/"0" | VLog block packing flag |
| `diskLen` | decimal | On-disk blob length **after** compression + encryption |
| `xflags` | hex | Transform bits: `FlagCompressed` (0x02), `FlagEncrypted` (0x04) |
| `ttlExpiryUs` | decimal | Absolute expiry, Unix µs (omitted = no TTL) |

> `valueLen` is the **plaintext** length and `diskLen` is what actually sits
> on disk. Conflating them — or dropping `xflags` — makes replay rebuild an
> index entry that fails its CRC check or hands back ciphertext. Before
> v1.1.0 the WAL carried neither, which is exactly what went wrong; see
> [DR_RUNBOOK.md](DR_RUNBOOK.md) §7.

**KV-separation mode**: `vlogOffset > 0` and no value bytes follow in the WAL — the WAL record is header + key only instead of also carrying the value. The VLog already has the durable value bytes.

### Group Commit

The WAL uses a **group commit** pattern to amortise `fdatasync` cost:

1. Every `Put` call enqueues a `WALEntry` to a channel; a `MultiPut` serializes all its records into one buffer and enqueues it with one channel send.
2. A single flusher goroutine drains up to `WALMaxBatchEntries` (4096, `--wal-max-batch`) records per cycle; reaching the cap flushes at once.
3. The whole batch is written with one `write(2)`, and one `fdatasync` covers it.
4. All pending callers unblock after the single fdatasync.

**When a batch is flushed** (`storage/group_commit.go`, `--group-commit`):

- **adaptive** (default): the flush window (**15 ms**, `--wal-flush-window-ms`; the VLog uses `--vlog-flush-window-ms`, also 15 ms, and the two must match) is only an upper bound. A writer that is alone — the batch holds one request and recent batches held about one — is synced immediately. Otherwise the batch stays open until no request has arrived for an idle gap of about one fdatasync (EWMA, clamped to 20 µs – 2 ms), the window expires, or the batch cap is reached. Requests that arrive during a sync form the next batch.
- **fixed**: every batch waits the whole window (at 1000 writes/s ~15 entries per batch, at 100K writes/s ~1500).

Measured with an emulated 300 µs sync (macOS): a lone writer's P50 is 0.45 ms adaptive vs 16 ms fixed; 64 writers 37.5K vs 4.0K writes/s at the same 64 writes per fdatasync. The VLog flusher uses the same pacer. Durability is identical: no caller is answered before the fdatasync that covers its bytes.

### WAL and VLog Concurrency

In `Put()`, both WAL and VLog `beginAppend()` are called concurrently. The caller waits for `max(WAL_wait, VLog_wait)` — not their sum. Both flush goroutines race to their respective fdatasyncs in parallel.

---

## 4. On-Disk Layout (Per Disk)

```
/data-dir-N/
├── wal.log              — Write-Ahead Log (group-commit, binary records)
├── wal.log.ckpt         — transient: checkpoint being written on clean shutdown
├── vlog_active.dat      — Value Log (24-byte header per record, 4 KB-aligned)
├── seg_active.dat       — Segment file (used only with KV separation off; 64-byte record header)
└── .vlog_punch_wm       — GC punch-hole watermark (8-byte little-endian offset)
```

The first data dir also holds `index_defs.json` (secondary-index definitions). Raft state is not per disk: in `--mode=raft` it lives in `<--data>/raft/` (`raft_state.gob`, `raft_snapshot.gob`); see [replication.md](replication.md#persistence).

With multiple disks (`--data-dirs /mnt/nvme0,...,/mnt/nvme7`), each disk gets its own independent WAL, VLog, segment file, and compaction goroutine. Shard `i` always lives on disk `i % numDisks`.

---

## 5. Value Transform Pipeline

```
Write path:   plaintext  →  compress  →  encrypt  →  VLog bytes on disk
Read  path:   disk bytes →  decrypt   →  decompress →  plaintext
```

**Why this order:**
- Compression before encryption: encrypted ciphertext has high entropy and is incompressible. Compressing after encrypt wastes CPU and gains nothing.
- `FlagCompressed` and `FlagEncrypted` on `IndexEntry` tell the read path which transforms to apply. Records written before encryption was enabled are still readable — flags are per-record.

**Compression**: zstd (level 1) by default (`flate` and `none` also exist), attempted for values ≥ 256 bytes and kept only when it actually shrinks the value. Per-record algorithm-prefix byte allows changing algorithms without an on-disk format change.

**Encryption**: off unless `--encrypt-at-rest`. AES-256-GCM with a random 12-byte nonce per record, stored as `[nonce][ciphertext+tag]`. Key source: `VELTRIXDB_ENCRYPTION_KEY` env (base64), else the `--encryption-key-path` file (raw or base64); startup fails if the key is missing or not 32 bytes.

---

## 6. LIRS Cache

The **LIRS (Low Inter-Reference Recency Set)** cache is scan-resistant — a sequential scan of cold data does not evict hot working-set entries.

- Small values (≤ 256 bytes) get `priority = 2` (scan-resistant, hard to evict).
- Large values get `priority = 1`.
- A 16-entry scan window picks the victim with the highest size ÷ priority.
- The cache is split into independent LIRS shards (each with its own lock and a slice of the budget), selected by the high bits of the key hash.
- Default size: 256 MB (`--cache <MB>`; `--read-heavy` presets 400 GB). Recommended: 256 GB+ in production.

---

## 7. Bloom Filters

Each of the 8192 shards has a **lock-free Bloom filter** backed by atomic `uint64` words (default `BloomFilterShardBits` = 2^19 bits per shard, 512 MB total; 0 disables them). Before taking the shard lock for a lookup, `MayContain(hash)` returns false if the key is definitely absent, so a miss skips the lock and the table probe (counted in `BloomFilterSkipped`).

- Probe positions use double-hashing: `pos = h1 + i × h2`
- Filters are rebuilt from the live index on every defrag pass (`vacuumBloomFilters`)
- False negative rate: 0 by construction — every live key is in the filter

---

## 8. Defragmentation (VLog GC)

Dead VLog space (from overwrites and deletes) is reclaimed by the **Defragmenter**, which wakes every `DefragInterval` (120 s; 300 s with `--read-heavy`). Each pass first reaps tombstones older than `GCGracePeriodSec` (86400 s), then compacts every disk's VLog in parallel — but only a disk whose garbage ratio is at least `--gc-threshold` (default 0.30):

1. Snapshot the VLog end as the GC horizon and collect live index entries below it.
2. Skip keys written in the last 30 s (likely to be overwritten again); sort the rest oldest-first.
3. Read each record and restage it through a `VLogBatcher` — 256 records per pwrite + fdatasync.
4. CAS `IndexEntry.DiskOffset` to the new position; a concurrent `Put` that won the race leaves the new copy as garbage (retried next pass). If more than 20% of a batch loses its CAS, `Put` is delayed 1 ms for 200 ms.
5. `MarkDead` the old copy, then punch out the dead head of the file below the lowest live offset with `fallocate(PUNCH_HOLE)`, or `BLKDISCARD` on a raw device, and persist the watermark to `.vlog_punch_wm`.

After the VLogs, every shard's bloom filter is rebuilt from the live index.

**GC control** (`storage/defrag.go`). Admission control sets `GCPaused` (and throttles writes by 2 ms each) when the sampled read-latency EWMA exceeds 20 ms, and clears it below 10 ms. The bandwidth caps apply only while the read EWMA is above 15 ms; otherwise GC runs at full disk speed.

| Garbage ratio | Behavior |
|---------------|----------|
| < 30% | No compaction on that disk |
| 30%–50% | Normal: 60 MB/s cap when reads are slow; skipped while `GCPaused` |
| 50%–65% | Critical: 200 MB/s cap when reads are slow; defrag interval halved; still skipped while `GCPaused` |
| ≥ 65% | Emergency: `GCPaused` bypassed (logged as `[gc] disk=N EMERGENCY ...`); uncapped; interval quartered |

The emergency tier prevents a "death spiral" where high read EWMA permanently pauses GC, causing more cache misses, keeping reads slow forever. A `GCPaused` flag left over from a write-only period is also cleared when no read has arrived for 4 minutes.

---

## 9. Multi-Disk Routing

```
           key="user:42"
               │
               ▼
    shard = FNV-1a("user:42") & 0x1FFF = 450
    disk  = 450 % 8                    = 2
               │
               ▼
    WAL[2]  ──► /mnt/nvme2/wal.log
    VLog[2] ──► /mnt/nvme2/vlog_active.dat
```

All 8 NVMe disks receive writes in parallel — no single disk is a serialization point.

---

## 10. Crash Recovery

Every startup replays the WAL; after an unclean shutdown (crash, OOM kill, SIGKILL) it is the full write history since the last checkpoint:

1. **`replayWAL()`** opens `wal.log` on each disk (in parallel, one goroutine per disk).
2. For each record (binary or legacy text): decode it and check its CRC32C. Crash replay, the PITR archiver and PITR restore all use the same decoder (`walReader`). A torn or corrupt record ends replay: everything before it is applied, and the server logs `[wal] replay of <path> stopped at byte N of M after K records: <err>`.
3. **`applyWALReplay()`** rebuilds the in-memory `shardedIndex` in the background. The engine accepts traffic immediately; replay uses `replayPut` / `replayMarkTombstone`, so a live write arriving during warm-up always wins over older replayed data. `ReplayDone` is closed when it finishes.
4. For KV-sep records (`vlogOffset > 0`): the VLog already has the value bytes; the WAL entry re-establishes the index pointer (with its on-disk length, packed flag and transform flags) without re-reading the value. A legacy record with inline value bytes is re-appended to the VLog through the compress → encrypt pipeline.
5. Legacy 6-, 7-, 8-, 10- and 11-field text entries are still parsed for backward compatibility. A record with fewer than 10 fields replays with no transform flags, which is correct — it was written untransformed.
6. **TTL.** A record carrying a TTL (binary version 2, text field 11) restores `FlagHasTTL` + `TTLExpiryUs` on the rebuilt entry. One whose expiry has already passed at replay is applied as an expiry — the key is tombstoned exactly as the TTL scanner would have done, superseding any older record for it — so an expired key is never resurrected (neither its TTL'd value nor an older immortal one). Records without a TTL (everything written before the field existed) replay as immortal keys, as they always did.

**Search indexes** are not in the WAL or the checkpoint as such: vectors, text documents and vector-namespace settings are ordinary reserved keys (`@vec/<ns>/<id>`, `@txt/<ns>/<id>`, `@vecns/<ns>`), replayed like any key. After replay the server runs `RebuildSearchIndexes` in the background and refuses searches until it finishes, unless `--search-allow-partial` (`INFO` → `search_ready`). Secondary-index entries (`@idx/...`) are durable keys too; their definitions are re-registered at startup from `index_defs.json` (and, in `--mode=replicated`, travel between nodes as `@idxdef/<name>` keys). See [vector-search.md](vector-search.md#durability-and-restarts).

On clean shutdown (`SIGTERM`): the engine writes a compacted checkpoint WAL (one record per live key, each with its absolute TTL expiry; tombstones and already-expired keys dropped) to `wal.log.ckpt` and atomically renames it over `wal.log`, so a crash mid-checkpoint leaves the old WAL intact. Next startup replays O(numLiveKeys) records instead of the full write history. The checkpoint uses the current `--wal-format`.
