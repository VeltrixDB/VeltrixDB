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
          │                     & 0x3FF         │
          │  3. diskIdx = shard % numDisks      │
          │  4. vl.beginAppend(value)  ─────────┼──► VLog[diskIdx]
          │  5. wal.beginAppend(entry) ─────────┼──► WAL[diskIdx]
          │  6. await both fdatasyncs           │
          │  7. update Index Vault[shard]       │
          └─────────────────────────────────────┘
```

---

## 1. Sharding (In-Memory Index)

The index is divided into **1024 shards**. Each shard has its own `sync.RWMutex` so reads and writes to different shards run in parallel.

```
shard_id = FNV-1a(key) & 0x3FF        // 0..1023
disk_idx = shard_id % numDisks         // which NVMe disk owns this shard
```

Each shard holds a `map[string]IndexEntry` — the full live key space is always in memory (the Index Vault). There is no on-disk B-tree or LSM tree for the index; durability comes from the WAL.

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
| `ShardID` | 2 B | Shard 0–1023 |
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
| 0x40 | `FlagPacked` | VLog record is packed (multiple values per 4 KB block) |

---

## 2. Value Log (VLog)

Each disk has one `vlog_active.dat` file — a flat append-only log of value bytes. Values are never overwritten; a superseded value's space is reclaimed by the defragmenter (GC).

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
 24   ValLen  Value bytes (plaintext after encrypt → compress pipeline)
24+V   pad    Zero-padding to next 512-byte sector boundary
```

Records are **sector-aligned to 512 bytes** to satisfy O_DIRECT requirements on Linux. On NVMe drives with 4Kn geometry, the padding is to 4096 bytes.

### Block Packing (High-Density Mode)

For small values (≤ 4096 − 24 bytes per record), the `VLogBatcher` packs multiple records into a single 4 KB block. For 128-byte values, up to ~26 records fit per block vs. 1 in legacy mode — a 25× density improvement.

- `FlagPacked` is set on the `IndexEntry` for packed records.
- `MarkDead(valueLen)` subtracts only `header+value` (not the full 4 KB block) when `FlagPacked` is set.
- The read path is unchanged: `ReadValue(DiskOffset, ValueSize)` rounds down to the 4 KB block boundary and extracts the record at `offset % 4096`.

### Write Path (Lock-Free)

VLog offset reservation is lock-free via an atomic counter:

```go
offset = vl.end.Add(alignedLen) - alignedLen   // reserves [offset, offset+alignedLen)
pwrite64(fd, value, offset)                     // POSIX-safe concurrent writes
```

Concurrent `pwrite64` calls to non-overlapping ranges are safe by POSIX. No mutex is held during the I/O — throughput is bounded only by NVMe IOPS (~450K/disk), not lock contention.

---

## 3. Write-Ahead Log (WAL)

Each disk has one `wal.log` file. The WAL provides crash durability: on unclean shutdown, `replayWAL()` reads it and rebuilds the index. On clean shutdown, `wal.truncate()` zeroes the file so next startup skips replay.

### WAL Record Format (7-field pipe-delimited text)

```
timestamp|isTombstone|key|valueLen|crc32hex|version|vlogOffset[|packed]
[value bytes]
```

| Field | Type | Meaning |
|-------|------|---------|
| `timestamp` | int64 | µs since Unix epoch |
| `isTombstone` | "1"/"0" | Delete marker |
| `key` | string | Raw key bytes |
| `valueLen` | decimal | Value byte count |
| `crc32hex` | hex | CRC32C of value |
| `version` | decimal | Monotonic MVCC counter |
| `vlogOffset` | decimal | 0 = value bytes follow inline; >0 = value already in VLog |
| `packed` | "1"/"0" (optional 8th field) | VLog block packing flag |

**KV-separation mode**: `vlogOffset > 0` and no value bytes follow in the WAL — the WAL record is ~100 bytes instead of 100 + value_size. The VLog already has the durable value bytes.

### Group Commit

The WAL uses a **group commit** pattern to amortise `fdatasync` cost:

1. Every `Put` call enqueues a `WALEntry` to a channel.
2. A single flusher goroutine drains up to `WALMaxBatchEntries` (4096) entries per cycle.
3. One `fdatasync` covers all entries in the batch.
4. All pending callers unblock after the single fdatasync.

Default flush window: **10 ms**. At 1000 writes/s, the batch is ~10 entries; at 100K writes/s the batch is ~1000 entries. Both pay one fdatasync.

### WAL and VLog Concurrency

In `Put()`, both WAL and VLog `beginAppend()` are called concurrently. The caller waits for `max(WAL_wait, VLog_wait)` — not their sum. Both flush goroutines race to their respective fdatasyncs in parallel.

---

## 4. On-Disk Layout (Per Disk)

```
/data-dir-N/
├── wal.log              — Write-Ahead Log (group-commit, 7-field text)
├── vlog_active.dat      — Value Log (24-byte header + sector-aligned values)
├── seg_XXXXXXXX.dat     — Segment files (O_DIRECT sequential, 64-byte header)
├── vlog_punch_watermark — GC punch offset watermark for defragmentation
└── raft_state.gob       — Raft persistent state (term, votedFor, log entries)
```

With multiple disks (`-data-dirs /mnt/nvme0,...,/mnt/nvme7`), each disk gets its own independent WAL, VLog, segment files, and compaction goroutine. Shard `i` always lives on disk `i % numDisks`.

---

## 5. Value Transform Pipeline

```
Write path:   plaintext  →  compress  →  encrypt  →  VLog bytes on disk
Read  path:   disk bytes →  decrypt   →  decompress →  plaintext
```

**Why this order:**
- Compression before encryption: encrypted ciphertext has high entropy and is incompressible. Compressing after encrypt wastes CPU and gains nothing.
- `FlagCompressed` and `FlagEncrypted` on `IndexEntry` tell the read path which transforms to apply. Records written before encryption was enabled are still readable — flags are per-record.

**Compression**: zstd, enabled for values ≥ 256 bytes. Per-record algorithm-prefix byte allows changing algorithms without an on-disk format change.

**Encryption**: AES-256-GCM with a unique 12-byte nonce per record. Key source: `VELTRIXDB_ENCRYPTION_KEY` env (base64) or `--encryption-key-path` file.

---

## 6. LIRS Cache

The **LIRS (Low Inter-Reference Recency Set)** cache is scan-resistant — a sequential scan of cold data does not evict hot working-set entries.

- Small values (≤ 256 bytes) get `priority = 2` (scan-resistant, hard to evict).
- Large values get `priority = 1`.
- A 16-entry scan window picks the largest cold victim for eviction.
- Default size: configurable via `-cache <MB>`. Recommended: 256 GB+ in production.

---

## 7. Bloom Filters

Each of the 1024 shards has a **lock-free Bloom filter** backed by atomic `uint64` words. Before a full index lookup, `MayContain(key)` returns false if the key is definitely absent — eliminating VLog reads for non-existent keys.

- Probe positions use double-hashing: `pos = h1 + i × h2`
- Filters are rebuilt from the live index on every defrag pass (`vacuumBloomFilters`)
- False negative rate: 0 by construction — every live key is in the filter

---

## 8. Defragmentation (VLog GC)

Dead VLog space (from overwrites and deletes) is reclaimed by the **Defragmenter**:

1. Walk live index entries, read each VLog record.
2. Rewrite live records to a fresh position in the VLog.
3. Update `IndexEntry.DiskOffset` to the new position (CAS, safe under concurrent reads).
4. Call `MarkDead(oldValueLen)` to account for the freed bytes.
5. Issue `fallocate(PUNCH_HOLE)` or `BLKDISCARD` (raw NVMe) to return dead pages to the OS.

**Three-tier GC control:**

| Garbage ratio | Behavior |
|---------------|----------|
| < 50% | Normal: 60 MB/s bandwidth cap; respects `GCPaused` flag |
| 50%–65% | Critical: 200 MB/s cap; defrag interval halved |
| ≥ 65% | Emergency: `GCPaused` bypassed; uncapped bandwidth; interval quartered |

The emergency tier prevents a "death spiral" where high read EWMA permanently pauses GC, causing more cache misses, keeping reads slow forever.

---

## 9. Multi-Disk Routing

```
           key="user:42"
               │
               ▼
    shard = FNV-1a("user:42") & 0x3FF  = 731
    disk  = 731 % 8                    = 3
               │
               ▼
    WAL[3]  ──► /mnt/nvme3/wal.log
    VLog[3] ──► /mnt/nvme3/vlog_active.dat
```

All 8 NVMe disks receive writes in parallel — no single disk is a serialization point. The C++ io_uring layer (Linux only) uses a dedicated `PriorityScheduler` per disk with a 3-tier queue: reads get RT priority to prevent write bursts from causing read tail latency spikes.

---

## 10. Crash Recovery

On unclean shutdown (crash, OOM kill, SIGKILL):

1. **`replayWAL()`** opens `wal.log` on each disk.
2. For each record: parse 7-field format, check CRC32C.
3. **`applyWALReplay()`** rebuilds the in-memory `shardedIndex`.
4. For KV-sep records (`vlogOffset > 0`): the VLog already has the value bytes; the WAL entry re-establishes the index pointer without re-reading the value.
5. Legacy 6-field entries (no vlogOffset) are supported for backward compatibility.

On clean shutdown (`SIGTERM`): `wal.truncate()` zeros the WAL file. Next startup sees an empty WAL and skips replay entirely — startup is O(1) instead of O(numLiveKeys).
