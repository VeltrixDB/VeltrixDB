package storage

// pitr.go — continuous WAL archiving + point-in-time recovery (PITR).
//
// Full and incremental backups (backup.go) capture the state at the moment
// the backup runs. PITR closes the gap between backups: a background
// WALArchiver continuously copies newly durable WAL entries into an archive
// directory, so the database can be restored to ANY point between the base
// backup and the last archived entry — exact to a single write (version) or
// to a wall-clock timestamp.
//
// Archive format
//
//	<ArchiveDir>/
//	├── disk0/
//	│   ├── seg-000000000001.wal    ← self-contained WAL records
//	│   ├── seg-000000000001.json   ← ArchiveSegmentMeta sidecar
//	│   ├── seg-000000000002.wal
//	│   └── seg-000000000002.json
//	└── disk1/ ...
//
// Segment files hold records in the legacy 6-field inline-value WAL format
// (timestamp|tombstone|key|valueLen|crc32hex|version\n[value\n]) that
// replayWAL already parses. Records are SELF-CONTAINED: for KV-separation
// entries whose WAL record is header-only (value lives in the VLog), the
// archiver resolves the VLog offset back into value bytes at archive time.
// This is deliberate — VLog GC relocates and discards records, so archived
// vlogOffsets would silently rot; embedded values never do.
//
// Safe sync boundaries: the archiver only reads WAL bytes below
// WriteAheadLog.durableOffset(), which the flusher advances AFTER each
// successful fdatasync. That offset always lands on an entry boundary, so a
// segment never contains a torn record. The archiver runs in its own
// goroutine and reads the WAL through an independent read-only file handle —
// it never touches the group-commit hot path (the only hot-path cost is one
// atomic add per WAL flush).
//
// WAL entries carry wall-clock timestamps (WALEntry.Timestamp, UnixNano), so
// both version-exact and timestamp-exact restore are supported. The sidecar's
// segment-level bounds (first/last version, first/last timestamp) let restore
// skip whole segments without parsing them.
//
// Restore (RestorePITR):
//  1. Restore the base full backup into fresh data dirs (reuses Restore()).
//  2. For each disk, append archived records with
//     baseVersion < version ≤ target (or timestamp ≤ target) to the restored
//     wal.log, in segment order.
//  3. On next engine startup, the normal replayWAL/applyWALReplay path
//     applies them: inline values are re-appended to the VLog, tombstones
//     re-delete keys, and the highest version wins.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const defaultArchiveIntervalMs = 1000

// ArchiveSegmentMeta is the JSON sidecar written next to every archive
// segment. First/Last bounds allow restore to skip whole segments without
// parsing them; CRC32C guards against silent archive corruption.
type ArchiveSegmentMeta struct {
	DiskIdx      int    `json:"disk_idx"`
	Seq          uint64 `json:"seq"`
	FirstVersion uint64 `json:"first_version"`
	LastVersion  uint64 `json:"last_version"`
	FirstUnixNs  int64  `json:"first_unix_ns"` // min WAL-entry wall-clock timestamp
	LastUnixNs   int64  `json:"last_unix_ns"`  // max WAL-entry wall-clock timestamp
	CreatedNs    int64  `json:"created_unix_ns"`
	Entries      int    `json:"entries"`
	SizeBytes    int64  `json:"size_bytes"`
	CRC32C       string `json:"crc32c"` // hex CRC32C of the segment file bytes

	// Populated by ListArchiveSegments; not stored in the sidecar.
	SegPath  string `json:"-"`
	MetaPath string `json:"-"`
}

// WALArchiver continuously copies newly durable WAL entries into ArchiveDir.
// One archiver covers every disk of an engine. Create with StartWALArchiver;
// call Stop() (which runs a final archive pass) before closing the engine.
type WALArchiver struct {
	dir      string
	interval time.Duration
	maxAge   time.Duration
	maxBytes int64
	disks    []*diskArchiver
	stopCh   chan struct{}
	doneCh   chan struct{}

	SegmentsWritten atomic.Uint64
	EntriesArchived atomic.Uint64
	BytesArchived   atomic.Uint64
	// EntriesSkipped counts KV-sep entries whose VLog record was already
	// GC-reclaimed by the time the archiver read it. Such records were
	// superseded — a newer entry for the same key follows in the WAL — so
	// final-state correctness is preserved; only restores targeting the brief
	// window where the old value was live would miss it.
	EntriesSkipped atomic.Uint64
}

// diskArchiver holds per-disk archiving state.
type diskArchiver struct {
	diskIdx  int
	wal      *WriteAheadLog
	vlog     *VLog    // nil when KV separation is off
	reader   *os.File // independent O_RDONLY handle on wal.log
	archived int64    // WAL bytes consumed so far
	seq      uint64   // next segment sequence number
	dir      string   // <ArchiveDir>/disk<N>
}

// StartWALArchiver attaches a WAL archiver to a running engine, driven by the
// engine's config (ArchiveDir, ArchiveIntervalMs, MaxArchiveAgeSec,
// MaxArchiveBytes). Returns (nil, nil) when cfg.ArchiveDir is empty.
//
// Call this immediately after NewStorageEngine, before any Checkpoint(), so
// the read handle is opened on the same WAL file the flusher writes to.
// Segment sequence numbers continue from any segments already present in
// ArchiveDir, so archives accumulate correctly across restarts.
func StartWALArchiver(se *StorageEngine) (*WALArchiver, error) {
	cfg := se.config
	if cfg.ArchiveDir == "" {
		return nil, nil
	}
	intervalMs := cfg.ArchiveIntervalMs
	if intervalMs <= 0 {
		intervalMs = defaultArchiveIntervalMs
	}

	a := &WALArchiver{
		dir:      cfg.ArchiveDir,
		interval: time.Duration(intervalMs) * time.Millisecond,
		maxAge:   time.Duration(cfg.MaxArchiveAgeSec) * time.Second,
		maxBytes: cfg.MaxArchiveBytes,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}

	for i, w := range se.wals {
		diskDir := filepath.Join(cfg.ArchiveDir, fmt.Sprintf("disk%d", i))
		if err := os.MkdirAll(diskDir, 0755); err != nil {
			a.closeReaders()
			return nil, fmt.Errorf("pitr: mkdir %s: %w", diskDir, err)
		}
		reader, err := os.Open(w.walPath)
		if err != nil {
			a.closeReaders()
			return nil, fmt.Errorf("pitr: open wal for archiving: %w", err)
		}
		var vl *VLog
		if cfg.KeyValueSeparation && i < len(se.vlogs) {
			vl = se.vlogs[i]
		}
		a.disks = append(a.disks, &diskArchiver{
			diskIdx: i,
			wal:     w,
			vlog:    vl,
			reader:  reader,
			seq:     nextArchiveSeq(diskDir),
			dir:     diskDir,
		})
	}

	go a.loop()
	log.Printf("[pitr] wal archiver started  dir=%s  disks=%d  interval=%s",
		cfg.ArchiveDir, len(a.disks), a.interval)
	return a, nil
}

// Stop runs a final archive pass (so everything durable at the moment of the
// call is archived), closes the read handles, and waits for the goroutine to
// exit. Call before StorageEngine.Close().
func (a *WALArchiver) Stop() {
	close(a.stopCh)
	<-a.doneCh
}

func (a *WALArchiver) closeReaders() {
	for _, d := range a.disks {
		if d.reader != nil {
			d.reader.Close()
		}
	}
}

func (a *WALArchiver) loop() {
	defer close(a.doneCh)
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.archiveAll()
			a.prune()
		case <-a.stopCh:
			a.archiveAll() // final pass: durable ⇒ archived at Stop()
			a.closeReaders()
			return
		}
	}
}

func (a *WALArchiver) archiveAll() {
	for _, d := range a.disks {
		if err := a.archiveDisk(d); err != nil {
			log.Printf("[pitr] disk %d archive pass failed: %v", d.diskIdx, err)
		}
	}
}

// archiveDisk copies WAL bytes [d.archived, durableOffset) into one new
// segment file. The lower bound of the copy is always an entry boundary
// because the previous pass advanced d.archived by whole parsed records.
func (a *WALArchiver) archiveDisk(d *diskArchiver) error {
	durable := d.wal.durableOffset()
	if durable <= d.archived {
		return nil
	}

	raw := make([]byte, durable-d.archived)
	n, err := d.reader.ReadAt(raw, d.archived)
	if n == 0 {
		if err != nil && err != io.EOF {
			return fmt.Errorf("read wal: %w", err)
		}
		return nil
	}
	recs, consumed := parseWALBuffer(raw[:n])
	if consumed == 0 {
		return nil
	}

	meta := ArchiveSegmentMeta{
		DiskIdx:   d.diskIdx,
		Seq:       d.seq,
		CreatedNs: time.Now().UnixNano(),
	}
	var seg []byte
	for _, r := range recs {
		val := r.value
		if !r.isTombstone && r.valueLen > 0 && r.vlogOffset > 0 {
			// KV-sep header-only record: resolve the value from the VLog so
			// the archived record is self-contained (GC may relocate or
			// discard the VLog record at any time after this).
			if d.vlog == nil {
				a.EntriesSkipped.Add(1)
				continue
			}
			v, rerr := d.vlog.ReadValue(r.vlogOffset, r.valueLen)
			if rerr != nil {
				// Already GC-reclaimed ⇒ superseded by a later WAL entry.
				a.EntriesSkipped.Add(1)
				continue
			}
			val = v
		}
		// Recompute the CRC over the bytes we embed: for KV-sep records the
		// WAL header CRC covers the pre-transform value, while the VLog holds
		// the stored blob. replayWAL verifies inline values against this CRC.
		crc := r.crc
		if len(val) > 0 {
			crc = computeCRC32C(val)
		}
		seg = appendArchiveRecord(seg, r.timestampNs, r.isTombstone, r.key, val, crc, r.version)

		if meta.Entries == 0 {
			meta.FirstVersion, meta.LastVersion = r.version, r.version
			meta.FirstUnixNs, meta.LastUnixNs = r.timestampNs, r.timestampNs
		} else {
			if r.version < meta.FirstVersion {
				meta.FirstVersion = r.version
			}
			if r.version > meta.LastVersion {
				meta.LastVersion = r.version
			}
			if r.timestampNs < meta.FirstUnixNs {
				meta.FirstUnixNs = r.timestampNs
			}
			if r.timestampNs > meta.LastUnixNs {
				meta.LastUnixNs = r.timestampNs
			}
		}
		meta.Entries++
	}

	// Advance past everything parsed even if every record was skipped —
	// skipped records are superseded and will never become archivable.
	d.archived += int64(consumed)
	if meta.Entries == 0 {
		return nil
	}

	meta.SizeBytes = int64(len(seg))
	meta.CRC32C = fmt.Sprintf("%08x", computeCRC32C(seg))
	if err := writeArchiveSegment(d.dir, &meta, seg); err != nil {
		// Roll back so the next pass retries these bytes.
		d.archived -= int64(consumed)
		return err
	}
	d.seq++

	a.SegmentsWritten.Add(1)
	a.EntriesArchived.Add(uint64(meta.Entries))
	a.BytesArchived.Add(uint64(len(seg)))
	return nil
}

// prune enforces MaxArchiveAgeSec / MaxArchiveBytes retention: oldest
// segments (by creation time) are deleted first.
func (a *WALArchiver) prune() {
	if a.maxAge <= 0 && a.maxBytes <= 0 {
		return
	}
	segs, err := ListArchiveSegments(a.dir)
	if err != nil {
		return
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].CreatedNs < segs[j].CreatedNs })

	var total int64
	for _, s := range segs {
		total += s.SizeBytes
	}
	var cutoffNs int64
	if a.maxAge > 0 {
		cutoffNs = time.Now().Add(-a.maxAge).UnixNano()
	}

	for _, s := range segs {
		tooOld := cutoffNs > 0 && s.CreatedNs < cutoffNs
		tooBig := a.maxBytes > 0 && total > a.maxBytes
		if !tooOld && !tooBig {
			break // segs are oldest-first; nothing further qualifies
		}
		os.Remove(s.SegPath)
		os.Remove(s.MetaPath)
		total -= s.SizeBytes
		log.Printf("[pitr] pruned archive segment disk=%d seq=%d (age=%s bytes=%d)",
			s.DiskIdx, s.Seq, time.Duration(time.Now().UnixNano()-s.CreatedNs).Round(time.Second), s.SizeBytes)
	}
}

// ── archive segment I/O ───────────────────────────────────────────────────────

func archiveSegName(seq uint64) string { return fmt.Sprintf("seg-%012d", seq) }

// writeArchiveSegment writes the segment then its sidecar, each crash-safely
// via temp-file + fsync + rename.
func writeArchiveSegment(dir string, meta *ArchiveSegmentMeta, seg []byte) error {
	base := filepath.Join(dir, archiveSegName(meta.Seq))
	if err := writeFileAtomic(base+".wal", seg); err != nil {
		return fmt.Errorf("write segment: %w", err)
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(base+".json", data); err != nil {
		return fmt.Errorf("write segment meta: %w", err)
	}
	return nil
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// nextArchiveSeq scans diskDir for existing segments and returns max(seq)+1,
// so sequence numbers stay monotonic across engine restarts.
func nextArchiveSeq(diskDir string) uint64 {
	entries, err := os.ReadDir(diskDir)
	if err != nil {
		return 1
	}
	var max uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "seg-") || !strings.HasSuffix(name, ".wal") {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "seg-"), ".wal"), 10, 64)
		if err == nil && seq > max {
			max = seq
		}
	}
	return max + 1
}

// ListArchiveSegments reads every sidecar under archiveDir/disk*/ and returns
// the metadata sorted by (disk index, sequence number).
func ListArchiveSegments(archiveDir string) ([]ArchiveSegmentMeta, error) {
	diskDirs, err := filepath.Glob(filepath.Join(archiveDir, "disk*"))
	if err != nil {
		return nil, err
	}
	var segs []ArchiveSegmentMeta
	for _, dd := range diskDirs {
		metaPaths, err := filepath.Glob(filepath.Join(dd, "seg-*.json"))
		if err != nil {
			return nil, err
		}
		for _, mp := range metaPaths {
			data, err := os.ReadFile(mp)
			if err != nil {
				return nil, err
			}
			var m ArchiveSegmentMeta
			if err := json.Unmarshal(data, &m); err != nil {
				return nil, fmt.Errorf("parse %s: %w", mp, err)
			}
			m.MetaPath = mp
			m.SegPath = strings.TrimSuffix(mp, ".json") + ".wal"
			segs = append(segs, m)
		}
	}
	sort.Slice(segs, func(i, j int) bool {
		if segs[i].DiskIdx != segs[j].DiskIdx {
			return segs[i].DiskIdx < segs[j].DiskIdx
		}
		return segs[i].Seq < segs[j].Seq
	})
	return segs, nil
}

// ── WAL record parsing / serialization ────────────────────────────────────────

// archivedWALRecord is one parsed record from a WAL byte buffer (live WAL or
// archive segment — both use the same pipe-delimited framing).
type archivedWALRecord struct {
	timestampNs int64
	isTombstone bool
	key         string
	valueLen    uint32
	crc         uint32
	version     uint64
	vlogOffset  int64
	value       []byte // inline value bytes, when present
}

// parseWALBuffer parses complete 6/7/8-field WAL records from data and
// returns them together with the number of bytes consumed. A trailing partial
// record is left unconsumed (retried on the next pass). Mirrors replayWAL but
// operates on an in-memory buffer instead of a file.
func parseWALBuffer(data []byte) ([]archivedWALRecord, int) {
	var recs []archivedWALRecord
	pos := 0

	for pos < len(data) {
		nl := bytes.IndexByte(data[pos:], '\n')
		if nl < 0 {
			break
		}
		line := string(data[pos : pos+nl])
		if line == "" {
			pos += nl + 1
			continue
		}
		parts := strings.SplitN(line, "|", 8)
		if len(parts) < 6 {
			break
		}

		ts, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			break
		}
		isTombstone := parts[1] == "1"
		key := parts[2]
		valueLen, err := strconv.ParseUint(parts[3], 10, 32)
		if err != nil {
			break
		}
		crcU64, err := strconv.ParseUint(parts[4], 16, 32)
		if err != nil {
			break
		}
		version, err := strconv.ParseUint(parts[5], 10, 64)
		if err != nil {
			break
		}
		var vlogOffset int64
		if len(parts) >= 7 {
			vlogOffset, err = strconv.ParseInt(parts[6], 10, 64)
			if err != nil {
				break
			}
		}

		recEnd := pos + nl + 1
		var value []byte
		if !isTombstone && valueLen > 0 && vlogOffset == 0 {
			need := recEnd + int(valueLen) + 1 // value bytes + '\n'
			if need > len(data) {
				break // partial tail — leave for the next pass
			}
			value = data[recEnd : recEnd+int(valueLen)]
			if data[recEnd+int(valueLen)] != '\n' {
				break
			}
			recEnd = need
		}

		recs = append(recs, archivedWALRecord{
			timestampNs: ts,
			isTombstone: isTombstone,
			key:         key,
			valueLen:    uint32(valueLen),
			crc:         uint32(crcU64),
			version:     version,
			vlogOffset:  vlogOffset,
			value:       value,
		})
		pos = recEnd
	}

	return recs, pos
}

// appendArchiveRecord serialises one self-contained record in the legacy
// 6-field inline-value WAL format that replayWAL parses directly:
// timestamp|tombstone|key|valueLen|crc32hex|version\n[value\n]
func appendArchiveRecord(buf []byte, tsNs int64, tombstone bool, key string, value []byte, crc uint32, version uint64) []byte {
	buf = strconv.AppendInt(buf, tsNs, 10)
	buf = append(buf, '|')
	if tombstone {
		buf = append(buf, '1')
	} else {
		buf = append(buf, '0')
	}
	buf = append(buf, '|')
	buf = append(buf, key...)
	buf = append(buf, '|')
	buf = strconv.AppendUint(buf, uint64(len(value)), 10)
	buf = append(buf, '|')
	buf = strconv.AppendUint(buf, uint64(crc), 16)
	buf = append(buf, '|')
	buf = strconv.AppendUint(buf, version, 10)
	buf = append(buf, '\n')
	if !tombstone && len(value) > 0 {
		buf = append(buf, value...)
		buf = append(buf, '\n')
	}
	return buf
}

// ── point-in-time restore ─────────────────────────────────────────────────────

// PITRTarget selects the restore point. Exactly one of Version / Time must be
// set: Version restores every write up to and including that engine version;
// Time restores every write whose WAL wall-clock timestamp is ≤ Time.
type PITRTarget struct {
	Version uint64
	Time    time.Time
}

// ParsePITRTarget parses the CLI --until syntax: "version:N" or an RFC3339
// timestamp (e.g. 2026-07-02T15:04:05Z).
func ParsePITRTarget(s string) (PITRTarget, error) {
	if rest, ok := cutPrefix(s, "version:"); ok {
		v, err := strconv.ParseUint(rest, 10, 64)
		if err != nil || v == 0 {
			return PITRTarget{}, fmt.Errorf("invalid version target %q (want version:N with N > 0)", s)
		}
		return PITRTarget{Version: v}, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return PITRTarget{}, fmt.Errorf("invalid target %q: want RFC3339 timestamp or version:N", s)
	}
	return PITRTarget{Time: t}, nil
}

// cutPrefix is strings.CutPrefix for Go 1.19 (added to the stdlib in 1.20).
func cutPrefix(s, prefix string) (string, bool) {
	if strings.HasPrefix(s, prefix) {
		return s[len(prefix):], true
	}
	return s, false
}

// RestorePITR restores a base full backup into destDirs and then replays
// archived WAL segments up to target. Returns the number of archived entries
// applied. Offline-only: it refuses to touch a data dir that already contains
// engine files (a live or previously used directory).
//
// Only archive entries with version > base.EngineVersion are applied — the
// base checkpoint already reflects everything at or below that version, and
// replay is order-based, so re-applying an older entry would regress the key.
func RestorePITR(baseBackupDir, archiveDir string, target PITRTarget, destDirs []string) (int, error) {
	if target.Version == 0 && target.Time.IsZero() {
		return 0, fmt.Errorf("pitr: target must set a version or a timestamp")
	}

	manifest, err := ReadManifest(baseBackupDir)
	if err != nil {
		return 0, fmt.Errorf("pitr: read base manifest: %w", err)
	}
	if manifest.Type != BackupTypeFull {
		return 0, fmt.Errorf("pitr: base backup must be a full backup (got %q); restore incremental chains with Restore first", manifest.Type)
	}
	if len(destDirs) != manifest.NumDisks {
		return 0, fmt.Errorf("pitr: destDirs length %d != backup numDisks %d", len(destDirs), manifest.NumDisks)
	}
	if target.Version > 0 && target.Version <= manifest.EngineVersion {
		return 0, fmt.Errorf("pitr: target version %d is not after the base backup (engine_version %d) — restore the base backup directly",
			target.Version, manifest.EngineVersion)
	}

	// Refuse live or previously used data dirs: restoring over a running
	// engine's files corrupts it, and clobbering existing data must be an
	// explicit operator decision (delete the dir first).
	for _, dir := range destDirs {
		for _, f := range []string{"wal.log", "vlog_active.dat"} {
			if _, statErr := os.Stat(filepath.Join(dir, f)); statErr == nil {
				return 0, fmt.Errorf("pitr: %s already contains %s — refusing to overwrite a live or existing data dir; stop the engine and restore into a fresh directory",
					dir, f)
			}
		}
	}

	// Step 1: restore the base backup (reuses the existing restore path).
	if err := Restore([]*BackupManifest{manifest}, []string{baseBackupDir}, destDirs); err != nil {
		return 0, fmt.Errorf("pitr: base restore: %w", err)
	}

	// Step 2: append archived records up to the target onto each restored WAL.
	segs, err := ListArchiveSegments(archiveDir)
	if err != nil {
		return 0, fmt.Errorf("pitr: list archive: %w", err)
	}

	applied := 0
	for diskIdx := 0; diskIdx < len(destDirs); diskIdx++ {
		walPath := walPathForDir(destDirs[diskIdx])
		f, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return applied, fmt.Errorf("pitr: open restored wal disk %d: %w", diskIdx, err)
		}
		bw := bufio.NewWriterSize(f, 1<<20)

		for _, s := range segs {
			if s.DiskIdx != diskIdx {
				continue
			}
			// Segment-level skip via sidecar bounds.
			if s.LastVersion <= manifest.EngineVersion {
				continue // fully covered by the base backup
			}
			if target.Version > 0 && s.FirstVersion > target.Version {
				continue // entirely after the target
			}
			if !target.Time.IsZero() && s.FirstUnixNs > target.Time.UnixNano() {
				continue
			}

			data, err := os.ReadFile(s.SegPath)
			if err != nil {
				f.Close()
				return applied, fmt.Errorf("pitr: read segment %s: %w", s.SegPath, err)
			}
			if got := fmt.Sprintf("%08x", computeCRC32C(data)); got != s.CRC32C {
				f.Close()
				return applied, fmt.Errorf("pitr: segment %s CRC mismatch (meta=%s file=%s) — archive corrupted", s.SegPath, s.CRC32C, got)
			}
			recs, consumed := parseWALBuffer(data)
			if consumed != len(data) {
				f.Close()
				return applied, fmt.Errorf("pitr: segment %s truncated or malformed at byte %d", s.SegPath, consumed)
			}

			var out []byte
			for _, r := range recs {
				if r.version <= manifest.EngineVersion {
					continue
				}
				if target.Version > 0 && r.version > target.Version {
					continue
				}
				if !target.Time.IsZero() && r.timestampNs > target.Time.UnixNano() {
					continue
				}
				out = appendArchiveRecord(out[:0], r.timestampNs, r.isTombstone, r.key, r.value, r.crc, r.version)
				if _, err := bw.Write(out); err != nil {
					f.Close()
					return applied, fmt.Errorf("pitr: append to restored wal disk %d: %w", diskIdx, err)
				}
				applied++
			}
		}

		if err := bw.Flush(); err != nil {
			f.Close()
			return applied, fmt.Errorf("pitr: flush restored wal disk %d: %w", diskIdx, err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return applied, fmt.Errorf("pitr: sync restored wal disk %d: %w", diskIdx, err)
		}
		f.Close()
	}

	log.Printf("[pitr] restore complete  base=%s  entries_applied=%d  disks=%d",
		manifest.BackupID, applied, len(destDirs))
	return applied, nil
}
