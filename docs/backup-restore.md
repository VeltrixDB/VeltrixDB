# Backup and Restore in VeltrixDB

VeltrixDB supports **full backups**, **incremental backups**, **cloud backups** (S3/GCS/Azure), and **point-in-time recovery (PITR)** via continuous WAL archiving. Backups are designed around the WiscKey KV-separation model: the WAL and VLog are backed up independently, and restoring them reconstructs the full key-value state.

A full or incremental backup restores the database to **the moment that backup ran** — nothing finer. PITR closes the gap: with WAL archiving enabled, the database can be restored to **any** moment between a full backup and the last archived write — exact to a single write (`version:N`) or to a wall-clock RFC3339 timestamp. See [Point-in-Time Recovery](#point-in-time-recovery-pitr) below.

---

## Architecture: What Gets Backed Up

VeltrixDB stores data across two files per disk:

| File | Content | Why Back It Up |
|------|---------|----------------|
| `wal.log` | Compacted WAL: one entry per live key (key + VLog offset) | Contains the index: which keys exist and where their values live |
| `vlog_active.dat` | Value bytes, append-only | Contains the actual data |

The in-memory index is **not** backed up — it is fully rebuilt from the WAL on startup (see Crash Recovery in `storage.md`). Backing up the WAL + VLog is sufficient.

---

## Full Backup

A full backup captures a consistent snapshot of the entire dataset **as of the moment the backup runs**, while the engine keeps serving traffic. It cannot be restored to any other moment — for restore-to-an-arbitrary-moment, combine it with WAL archiving (see PITR below).

### What Happens

```
BackupEngine.FullBackup(destDir)
    │
    ├─ 1. Checkpoint()
    │      ├─ Flush all pending WAL writes
    │      └─ Write a compacted WAL: one entry per live key
    │         (dead entries from overwrites/deletes are excluded)
    │
    ├─ 2. For each disk i:
    │      ├─ snapshot vlogEnd = vlogs[i].end.Load()   (atomic)
    │      ├─ copyFileRange(vlog_active.dat, disk<i>/vlog.dat, 0, vlogEnd)
    │      │    uses pread64 → safe alongside concurrent appends
    │      └─ copyFileFull(wal.log, disk<i>/wal.log)
    │
    └─ 3. Write manifest.json
```

**The engine continues serving traffic during backup.** The VLog copy uses `pread64` (random-position read), which is safe alongside concurrent VLog appends to non-overlapping byte ranges. Appends after `vlogEnd` is snapshotted are simply not included in this backup.

### Backup Directory Structure

```
/backups/full-20260523-140000/
├── manifest.json
├── disk0/
│   ├── wal.log        ← compacted WAL for disk 0
│   └── vlog.dat       ← VLog bytes [0, vlogEnd0)
├── disk1/
│   ├── wal.log
│   └── vlog.dat
└── ...
```

### manifest.json

```json
{
  "backup_id": "full-1748001600000000000",
  "type": "full",
  "timestamp_ns": 1748001600000000000,
  "engine_version": 42,
  "num_disks": 8,
  "disks": [
    {
      "disk_idx": 0,
      "wal_file": "disk0/wal.log",
      "vlog_file": "disk0/vlog.dat",
      "vlog_start_off": 0,
      "vlog_end_off": 10737418240
    },
    ...
  ]
}
```

### Go API

```go
be := storage.NewBackupEngine(se)
manifest, err := be.FullBackup("/backups/full-20260523")
```

### CLI

```bash
# Full backup to local directory
./veltrix backup full --addr :9000 --out /backups/full-20260523

# Full backup + upload to S3 in one step
./veltrix backup full-cloud \
  --addr :9000 \
  --out /tmp/backup \
  --bucket s3://my-bucket/veltrix/ \
  --region us-east-1
```

---

## Incremental Backup

An incremental backup captures only the **delta** — VLog bytes written since the base backup. This is much faster and uses less storage for frequent backups.

### What Happens

```
BackupEngine.IncrementalBackup(destDir, baseManifest)
    │
    ├─ 1. Checkpoint()    ← same as full backup
    │
    ├─ 2. For each disk i:
    │      ├─ baseEnd = baseManifest.Disks[i].VLogEndOff
    │      ├─ curEnd  = vlogs[i].end.Load()
    │      ├─ copyFileRange(vlog_active.dat, disk<i>/vlog_delta.dat,
    │      │               baseEnd, curEnd - baseEnd)
    │      │   ← copies ONLY the new bytes since last backup
    │      └─ copyFileFull(wal.log, disk<i>/wal.log)
    │         ← latest compacted WAL (includes all live keys)
    │
    └─ 3. Write manifest.json with BaseBackupDir = base backup ID
```

**Each incremental WAL is a complete snapshot** — it covers all live keys at that moment, not just changed keys. The WAL is small (one line per live key, no values), so copying it fully each time is cheap.

**The VLog delta is append-only** — it captures exactly `[baseEnd, curEnd)` bytes. Because VLog offsets are stable (values are never moved except by GC), these byte ranges are self-contained.

### Incremental Backup Directory Structure

```
/backups/incr-20260523-150000/
├── manifest.json           ← references base backup ID
├── disk0/
│   ├── wal.log             ← full compacted WAL (all live keys)
│   └── vlog_delta.dat      ← only VLog bytes written since base
└── ...
```

### Manifest for Incremental

```json
{
  "backup_id": "incr-1748005200000000000",
  "type": "incremental",
  "base_backup_dir": "full-1748001600000000000",
  "timestamp_ns": 1748005200000000000,
  "engine_version": 43,
  "num_disks": 8,
  "disks": [
    {
      "disk_idx": 0,
      "wal_file": "disk0/wal.log",
      "vlog_file": "disk0/vlog_delta.dat",
      "vlog_start_off": 10737418240,
      "vlog_end_off":   10838427648
    }
  ]
}
```

### Backup Chain

```
full-T1  ←  incr-T2  ←  incr-T3  ←  incr-T4
```

To restore to `T4`, all four backups in the chain are needed. VLog deltas are concatenated in order: `full_vlog + delta_T2 + delta_T3 + delta_T4`.

---

## Restore

**The engine must NOT be running during restore.** Restore is an offline operation that reconstructs the data directories from a backup chain.

### What Happens

```
storage.Restore(chain, backupRoots, destDirs)
    │
    ├─ chain = [fullManifest, incrManifest1, incrManifest2, ...]
    │           ordered oldest-first
    │
    ├─ For each disk i:
    │      ├─ Step 1: Copy full backup VLog to destDirs[i]/vlog_active.dat
    │      │
    │      ├─ Step 2: Append each incremental delta in order
    │      │    for chainIdx = 1..len(chain)-1:
    │      │        appendFile(destVLog, deltaFile)
    │      │        ← concatenates [full_vlog][delta1][delta2]...
    │      │
    │      └─ Step 3: Copy WAL from the LATEST backup in chain
    │             ← The latest WAL covers all live keys at restore point
    │
    └─ All disks done → start engine
```

After `Restore()` completes, start the engine normally. On startup:
- `replayWAL()` reads the restored `wal.log` and rebuilds the index.
- The VLog byte ranges in the WAL entries correctly map to the reconstructed `vlog_active.dat`.
- The engine is ready to serve traffic.

### Restore Timeline Example

```
Backup chain:
    full-T1  (10 GB VLog)
    incr-T2  (2 GB delta)
    incr-T3  (3 GB delta)

Restore to T3:
    disk0/vlog_active.dat = [10 GB full] + [2 GB delta-T2] + [3 GB delta-T3]
                          = 15 GB total
    disk0/wal.log         = from incr-T3 (latest)

After start:
    replayWAL() → reads WAL from incr-T3
    All VLog offsets in WAL entries point into the 15 GB vlog_active.dat ✓
```

### Go API

```go
chain := []*storage.BackupManifest{fullManifest, incr1, incr2}
roots := []string{"/backups/full", "/backups/incr1", "/backups/incr2"}
dests := []string{"/data/disk0", "/data/disk1", ...}

err := storage.Restore(chain, roots, dests)
// Then start the engine
```

### CLI

```bash
# Restore from local backup chain
./veltrix backup restore \
  --chain /backups/full-20260523,/backups/incr-20260523-150000 \
  --data-dirs /mnt/nvme0,/mnt/nvme1,...

# Download from cloud then restore
./veltrix backup download \
  --bucket s3://my-bucket/veltrix/ \
  --backup-id full-1748001600000000000 \
  --out /backups/full
```

---

## Point-in-Time Recovery (PITR)

PITR = **base full backup + continuously archived WAL**. A background `WALArchiver` copies newly durable WAL entries into an archive directory as they are fsynced; `restore-pitr` later replays them on top of a full backup up to an exact write. Implemented in `storage/pitr.go`.

### Enabling WAL Archiving

Archiving is driven by four `StorageConfig` fields and attached to a running engine:

```go
cfg := storage.DefaultStorageConfig()
cfg.ArchiveDir        = "/backup/wal-archive" // empty = archiving disabled
cfg.ArchiveIntervalMs = 1000                  // archive pass frequency (0 → 1000 ms)
cfg.MaxArchiveAgeSec  = 7 * 86400             // prune segments older than 7 days (0 = keep forever)
cfg.MaxArchiveBytes   = 50 << 30              // prune oldest above 50 GiB total (0 = unbounded)

se, _ := storage.NewStorageEngine(cfg)
arch, _ := storage.StartWALArchiver(se)       // returns (nil, nil) when ArchiveDir is empty
defer arch.Stop()                             // final archive pass; call BEFORE se.Close()
```

Call `StartWALArchiver` immediately after `NewStorageEngine`. Enable archiving **before** taking the base full backup — restore needs archive coverage from the backup point forward (same discipline as PostgreSQL WAL archiving).

### How Archiving Works

```
Put() ─► WAL flusher ─► write batch ─► fdatasync ─► durableOffset += batchBytes
                                                        │ (one atomic add — the only
                                                        │  hot-path cost of archiving)
WALArchiver goroutine (every ArchiveIntervalMs):        ▼
    for each disk:  read wal.log bytes [archived, durableOffset)
                    │   via an independent read-only handle
                    ├─ parse whole WAL records (the boundary is always
                    │  entry-aligned — the flusher syncs whole entries)
                    ├─ resolve KV-sep records: read value bytes from the VLog
                    │  so every archived record is SELF-CONTAINED
                    └─ write seg-<seq>.wal + seg-<seq>.json  (tmp + fsync + rename)
    then: prune oldest segments per MaxArchiveAgeSec / MaxArchiveBytes
```

The archiver never blocks the group-commit path: it runs in its own goroutine, reads the WAL file through its own descriptor, and only ever copies bytes already covered by a completed fdatasync.

**Why self-contained segments?** In KV-separation mode the live WAL stores only a VLog offset per record. VLog GC relocates and discards superseded records, so an archived offset would silently rot. The archiver therefore embeds the value bytes into the segment at archive time — an archive segment is replayable forever, independent of the VLog it came from.

### Archive Directory Layout

```
/backup/wal-archive/
├── disk0/
│   ├── seg-000000000001.wal    ← self-contained WAL records
│   ├── seg-000000000001.json   ← metadata sidecar
│   ├── seg-000000000002.wal
│   └── seg-000000000002.json
└── disk1/ ...
```

Sidecar (`seg-*.json`):

```json
{
  "disk_idx": 0,
  "seq": 17,
  "first_version": 4211,
  "last_version": 4890,
  "first_unix_ns": 1751464805123456789,
  "last_unix_ns": 1751464806012345678,
  "created_unix_ns": 1751464806100000000,
  "entries": 680,
  "size_bytes": 92160,
  "crc32c": "8f3a11c2"
}
```

WAL entries carry wall-clock timestamps (`WALEntry.Timestamp`, UnixNano), so both **version-exact** and **timestamp-exact** restore are supported; the sidecar's version/time bounds let restore skip whole segments without parsing them, and the CRC32C detects archive corruption before it can poison a restore.

### Inspecting the Archive

```bash
veltrixdb-backup archive-status --archive=/backup/wal-archive
```

Prints every segment (disk, seq, version range, entries, size, first/last entry time) plus the total restorable version and time range.

### Restoring to a Point in Time

```bash
# The engine must be STOPPED. --data must be FRESH, EMPTY directories —
# restore-pitr refuses to touch a dir that already contains engine files.

# Exact wall-clock target:
veltrixdb-backup restore-pitr \
  --base-backup=/backup/full-20260702 \
  --archive=/backup/wal-archive \
  --until=2026-07-02T14:30:00Z \
  --data=/data-new

# Exact single-write target:
veltrixdb-backup restore-pitr \
  --base-backup=/backup/full-20260702 \
  --archive=/backup/wal-archive \
  --until=version:123456 \
  --data=/data-new
```

What happens (`storage.RestorePITR`):

1. Read the base manifest (must be a **full** backup) and refuse non-empty target dirs.
2. Restore the base backup (same code path as `restore`).
3. For each disk, walk archive segments in sequence order, verify CRCs, and append every record with `base_version < version ≤ target` (or `timestamp ≤ target`) to the restored `wal.log`.
4. On the next engine startup, normal WAL replay applies them: values are re-appended to the VLog, tombstones re-delete keys, and the engine's version counter resumes past the last applied write.

```go
// Go API
target, _ := storage.ParsePITRTarget("version:123456")
applied, err := storage.RestorePITR(baseDir, archiveDir, target, destDirs)
```

### PITR Guarantees and Limitations

| Property | Detail |
|----------|--------|
| Granularity | Exact to a single write (`version:N`) or to a wall-clock timestamp (entry timestamps are assigned at `Put()` time) |
| Durability boundary | Only fdatasync-covered WAL bytes are archived — a segment never contains a torn record |
| Deletes | Tombstones are archived and replayed; restoring past a delete removes the key |
| Archive corruption | Detected via per-segment CRC32C before replay; restore aborts |
| Lower bound | Targets at or before the base backup's `engine_version` are rejected — restore the base (or an older base) directly |
| Coverage requirement | Archiving must be running from before the base backup until the target moment; pruned segments shrink the restorable window |
| GC race window | If VLog GC reclaims a superseded value in the (interval-sized) window before it is archived, that brief intermediate value is skipped (`EntriesSkipped` counter); the final state of the key is always correct |
| Crash of the archiver / restart | Segment sequence numbers continue across restarts; the current WAL is re-archived from byte 0, which is safe (replay is version-filtered and order-preserving) |

---

## Cloud Backup (S3 / GCS / Azure)

The `backup_cloud.go` module (`storage/backup_cloud.go`) extends backup with cloud storage:

| Command | Description |
|---------|-------------|
| `upload` | Upload a local backup directory to cloud storage |
| `download` | Download a cloud backup to local |
| `list-cloud` | List all cloud backups and their metadata |
| `full-cloud` | Full backup + upload in one step |

Cloud backups use the same manifest format — the manifest is uploaded alongside the backup files.

### Cloud Backup Structure (S3 Example)

```
s3://my-bucket/veltrix/
├── full-1748001600000000000/
│   ├── manifest.json
│   ├── disk0/wal.log
│   ├── disk0/vlog.dat
│   └── ...
└── incr-1748005200000000000/
    ├── manifest.json
    ├── disk0/wal.log
    ├── disk0/vlog_delta.dat
    └── ...
```

---

## Backup Under Load: Safety Guarantees

| Concern | How It's Handled |
|---------|-----------------|
| Concurrent writes during backup | VLog copy uses `pread64(offset, vlogEnd)` — reads only up to the snapshotted end; new appends beyond `vlogEnd` are ignored |
| Partial VLog write mid-copy | WAL checkpoint happens before VLog snapshot; WAL and VLog are consistent with each other at the snapshot point |
| GC compaction moving values during backup | GC rewrites VLog records to new offsets and updates WAL entries via CAS; WAL checkpoint after GC pass ensures WAL offsets are stable |
| Crash during backup | Backup directory is partially written — use the manifest to detect completeness; incomplete backups are unusable, re-run the backup |
| Manifest write failure | Manifest is written via temp-file rename (`manifest.json.tmp` → `manifest.json`) — atomic on POSIX |

---

## Recommended Backup Strategy

### Daily Full + Hourly Incremental

```
00:00  full backup   → 10 GB
01:00  incr backup   → 200 MB
02:00  incr backup   → 200 MB
...
23:00  incr backup   → 200 MB
```

To restore to any hour: apply full + N incrementals. For anything finer than backup granularity — "restore to 14:37:22", "restore to just before the bad deploy's first write" — run continuous WAL archiving alongside and use `restore-pitr` (see above).

### Daily Full + Continuous WAL Archiving (PITR)

```
00:00      full backup            → 10 GB
00:00-24h  WAL archiver runs continuously (segment per archive interval)
```

To restore to any moment of the day: `restore-pitr --base-backup=<daily full> --until=<timestamp|version:N>`.

### Retention Policy

Keep 7 daily full backups + all incrementals for the latest full. Older incrementals are useless once their base full is deleted. For PITR, size `MaxArchiveAgeSec` / `MaxArchiveBytes` so the archive always spans back to the oldest full backup you intend to restore from — archive segments older than the oldest retained full backup are useless.

### Monitoring

Check these after every backup run:
- `manifest.json` exists and `engine_version` matches the live engine.
- `vlog_end_off` per disk increases (new data was captured).
- `disk<N>/wal.log` size is proportional to the number of live keys.

---

## What Is NOT Backed Up

| Item | Why Excluded | Recovery |
|------|-------------|----------|
| Raft log (`raft_state.gob`) | Cluster state; not needed for single-node restore | Recreated on startup |
| In-memory LIRS cache | Volatile by design | Cache warms up after restart |
| Bloom filters | Rebuilt from index on startup | Automatic |
| Segment files (`seg_*.dat`) | Superseded by WAL+VLog | Not needed |
| Prometheus metrics | Ephemeral | Not needed |

A restored node starts with a cold cache but immediately serves correct data from the rebuilt index and VLog.
