package storage

// backup.go — Full and incremental backup / restore for VeltrixDB.
//
// Backup strategy (WiscKey KV-separation model):
//
//   Full backup
//     1. Trigger a WAL checkpoint (compacted WAL, one record per live key).
//     2. For each disk: copy [0, vlogEnd) bytes of vlog_active.dat to
//        backup_dir/disk<N>/vlog.dat.
//     3. Copy the compacted wal.log to backup_dir/disk<N>/wal.log.
//     4. Write backup_dir/manifest.json with vlogEnd offsets and version.
//
//   Incremental backup (relative to a full or previous incremental)
//     1. Trigger a WAL checkpoint.
//     2. For each disk: copy [lastVlogEnd, currentVlogEnd) to
//        backup_dir/disk<N>/vlog_delta.dat.
//     3. Copy the new compacted wal.log (covers all live keys at this moment).
//     4. Write manifest.json linking back to base manifest.
//
//   Restore
//     Offline-only (engine must be stopped):
//     1. Re-create data directories.
//     2. For each disk: reconstruct vlog_active.dat by concatenating the
//        full-backup vlog + each incremental vlog_delta in chain order.
//        Because each delta was captured at the original byte offsets, the
//        offsets in the WAL checkpoint remain valid.
//     3. Copy the latest wal.log (from the most-recent backup in the chain).
//     4. Start the engine — startup WAL replay rebuilds the index in O(numLiveKeys).
//
// Files written per backup:
//
//   manifest.json       — BackupManifest JSON
//   disk<N>/wal.log     — compacted WAL checkpoint (all live keys)
//   disk<N>/vlog.dat    — full VLog copy  (full backup only)
//   disk<N>/vlog_delta.dat — VLog delta   (incremental backup only)

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

// BackupType distinguishes full from incremental backups.
type BackupType string

const (
	BackupTypeFull        BackupType = "full"
	BackupTypeIncremental BackupType = "incremental"
)

// DiskBackupMeta holds per-disk metadata stored in the manifest.
type DiskBackupMeta struct {
	DiskIdx        int    `json:"disk_idx"`
	WALFile        string `json:"wal_file"`         // relative path inside backup dir
	VLogFile       string `json:"vlog_file"`        // relative path; "vlog_delta.dat" for incr
	VLogStartOff   int64  `json:"vlog_start_off"`   // byte offset in original VLog (0 for full)
	VLogEndOff     int64  `json:"vlog_end_off"`     // byte offset in original VLog
}

// BackupManifest is the JSON descriptor written to every backup directory.
type BackupManifest struct {
	BackupID      string           `json:"backup_id"`
	Type          BackupType       `json:"type"`
	BaseBackupDir string           `json:"base_backup_dir,omitempty"` // for incremental
	Timestamp     int64            `json:"timestamp_ns"`
	EngineVersion uint64           `json:"engine_version"`
	NumDisks      int              `json:"num_disks"`
	Disks         []DiskBackupMeta `json:"disks"`
}

// BackupEngine performs full and incremental backups of a running StorageEngine.
type BackupEngine struct {
	se *StorageEngine
}

// NewBackupEngine creates a backup engine wrapping se.
func NewBackupEngine(se *StorageEngine) *BackupEngine {
	return &BackupEngine{se: se}
}

// FullBackup writes a full backup to destDir.
// destDir is created if it does not exist.
// The engine continues serving traffic throughout — the WAL checkpoint is
// crash-safe and VLog reads use pread64, which is safe alongside concurrent
// appends to non-overlapping ranges.
func (be *BackupEngine) FullBackup(destDir string) (*BackupManifest, error) {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", destDir, err)
	}

	// 1. Flush a WAL checkpoint for every disk.
	if err := be.se.Checkpoint(); err != nil {
		return nil, fmt.Errorf("checkpoint: %w", err)
	}

	dataDirs := be.se.GetDataDirs()
	manifest := &BackupManifest{
		BackupID:      fmt.Sprintf("full-%d", time.Now().UnixNano()),
		Type:          BackupTypeFull,
		Timestamp:     time.Now().UnixNano(),
		EngineVersion: be.se.GetVersion(),
		NumDisks:      len(dataDirs),
		Disks:         make([]DiskBackupMeta, len(dataDirs)),
	}

	for i, dataDir := range dataDirs {
		diskDir := filepath.Join(destDir, fmt.Sprintf("disk%d", i))
		if err := os.MkdirAll(diskDir, 0755); err != nil {
			return nil, fmt.Errorf("disk %d mkdir: %w", i, err)
		}

		// 2. Snapshot VLog end offset, then copy [0, end) bytes.
		var vlogEnd int64
		if i < len(be.se.vlogs) {
			vlogEnd = be.se.vlogs[i].end.Load()
		}

		vlogSrc := filepath.Join(dataDir, "vlog_active.dat")
		vlogDst := filepath.Join(diskDir, "vlog.dat")
		if vlogEnd > 0 {
			if err := copyFileRange(vlogSrc, vlogDst, 0, vlogEnd); err != nil {
				return nil, fmt.Errorf("disk %d copy vlog: %w", i, err)
			}
		}

		// 3. Copy the freshly-written compacted WAL.
		walSrc := filepath.Join(dataDir, "wal.log")
		walDst := filepath.Join(diskDir, "wal.log")
		if err := copyFileFull(walSrc, walDst); err != nil {
			return nil, fmt.Errorf("disk %d copy wal: %w", i, err)
		}

		manifest.Disks[i] = DiskBackupMeta{
			DiskIdx:      i,
			WALFile:      filepath.Join(fmt.Sprintf("disk%d", i), "wal.log"),
			VLogFile:     filepath.Join(fmt.Sprintf("disk%d", i), "vlog.dat"),
			VLogStartOff: 0,
			VLogEndOff:   vlogEnd,
		}
	}

	if err := writeManifest(destDir, manifest); err != nil {
		return nil, fmt.Errorf("write manifest: %w", err)
	}

	log.Printf("[backup] full backup done  dir=%s  disks=%d  version=%d",
		destDir, len(dataDirs), manifest.EngineVersion)
	return manifest, nil
}

// IncrementalBackup writes only the changes since baseManifest.
// The base may itself be a full backup or a previous incremental.
// All VLog bytes from DiskBackupMeta.VLogEndOff to the current end are copied.
func (be *BackupEngine) IncrementalBackup(destDir string, baseManifest *BackupManifest) (*BackupManifest, error) {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", destDir, err)
	}

	// Checkpoint so WAL reflects all live keys at this moment.
	if err := be.se.Checkpoint(); err != nil {
		return nil, fmt.Errorf("checkpoint: %w", err)
	}

	dataDirs := be.se.GetDataDirs()
	if len(dataDirs) != len(baseManifest.Disks) {
		return nil, fmt.Errorf("disk count mismatch: engine=%d base=%d",
			len(dataDirs), len(baseManifest.Disks))
	}

	manifest := &BackupManifest{
		BackupID:      fmt.Sprintf("incr-%d", time.Now().UnixNano()),
		Type:          BackupTypeIncremental,
		BaseBackupDir: baseManifest.BackupID,
		Timestamp:     time.Now().UnixNano(),
		EngineVersion: be.se.GetVersion(),
		NumDisks:      len(dataDirs),
		Disks:         make([]DiskBackupMeta, len(dataDirs)),
	}

	for i, dataDir := range dataDirs {
		diskDir := filepath.Join(destDir, fmt.Sprintf("disk%d", i))
		if err := os.MkdirAll(diskDir, 0755); err != nil {
			return nil, fmt.Errorf("disk %d mkdir: %w", i, err)
		}

		baseEnd := baseManifest.Disks[i].VLogEndOff
		var curEnd int64
		if i < len(be.se.vlogs) {
			curEnd = be.se.vlogs[i].end.Load()
		}

		// Copy VLog delta: only bytes written since the base backup.
		vlogDst := filepath.Join(diskDir, "vlog_delta.dat")
		if curEnd > baseEnd {
			vlogSrc := filepath.Join(dataDir, "vlog_active.dat")
			if err := copyFileRange(vlogSrc, vlogDst, baseEnd, curEnd-baseEnd); err != nil {
				return nil, fmt.Errorf("disk %d copy vlog delta: %w", i, err)
			}
		} else {
			// No new VLog data — write an empty sentinel so restore chain is intact.
			if err := os.WriteFile(vlogDst, nil, 0644); err != nil {
				return nil, fmt.Errorf("disk %d write empty delta: %w", i, err)
			}
		}

		// Copy the new compacted WAL (covers all live keys at this moment).
		walSrc := filepath.Join(dataDir, "wal.log")
		walDst := filepath.Join(diskDir, "wal.log")
		if err := copyFileFull(walSrc, walDst); err != nil {
			return nil, fmt.Errorf("disk %d copy wal: %w", i, err)
		}

		manifest.Disks[i] = DiskBackupMeta{
			DiskIdx:      i,
			WALFile:      filepath.Join(fmt.Sprintf("disk%d", i), "wal.log"),
			VLogFile:     filepath.Join(fmt.Sprintf("disk%d", i), "vlog_delta.dat"),
			VLogStartOff: baseEnd,
			VLogEndOff:   curEnd,
		}
	}

	if err := writeManifest(destDir, manifest); err != nil {
		return nil, fmt.Errorf("write manifest: %w", err)
	}

	log.Printf("[backup] incremental backup done  dir=%s  base=%s  version=%d",
		destDir, baseManifest.BackupID, manifest.EngineVersion)
	return manifest, nil
}

// Restore reconstructs data directories from a backup chain and prepares them
// for a fresh engine startup.  The engine must NOT be running.
//
// chain is ordered oldest-first: [fullBackup, incr1, incr2, ...].
// destDirs must have the same length as the number of disks.
func Restore(chain []*BackupManifest, backupRoots []string, destDirs []string) error {
	if len(chain) == 0 {
		return fmt.Errorf("empty backup chain")
	}
	numDisks := chain[0].NumDisks
	if len(destDirs) != numDisks {
		return fmt.Errorf("destDirs length %d != numDisks %d", len(destDirs), numDisks)
	}

	// Create destination directories.
	for _, d := range destDirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			return fmt.Errorf("mkdir %s: %w", d, err)
		}
	}

	for diskIdx := 0; diskIdx < numDisks; diskIdx++ {
		destVLog := filepath.Join(destDirs[diskIdx], "vlog_active.dat")

		// Step 1: Write the full backup VLog.
		fullMeta := chain[0].Disks[diskIdx]
		fullVLogSrc := filepath.Join(backupRoots[0], fullMeta.VLogFile)
		if err := copyFileFull(fullVLogSrc, destVLog); err != nil {
			// VLog may not exist if KV separation was disabled.
			if !os.IsNotExist(err) {
				return fmt.Errorf("disk %d restore full vlog: %w", diskIdx, err)
			}
		}

		// Step 2: Append each incremental delta in order.
		for chainIdx := 1; chainIdx < len(chain); chainIdx++ {
			incrMeta := chain[chainIdx].Disks[diskIdx]
			deltaFile := filepath.Join(backupRoots[chainIdx], incrMeta.VLogFile)
			if err := appendFile(destVLog, deltaFile); err != nil {
				return fmt.Errorf("disk %d apply incremental %d: %w", diskIdx, chainIdx, err)
			}
		}

		// Step 3: Copy the WAL checkpoint from the latest backup in the chain.
		latestIdx := len(chain) - 1
		walSrc := filepath.Join(backupRoots[latestIdx], chain[latestIdx].Disks[diskIdx].WALFile)
		walDst := filepath.Join(destDirs[diskIdx], "wal.log")
		if err := copyFileFull(walSrc, walDst); err != nil {
			return fmt.Errorf("disk %d restore wal: %w", diskIdx, err)
		}

		log.Printf("[restore] disk %d done  dest=%s", diskIdx, destDirs[diskIdx])
	}

	log.Printf("[restore] all disks restored from %d-step chain", len(chain))
	return nil
}

// ReadManifest reads and parses a BackupManifest from destDir/manifest.json.
func ReadManifest(backupDir string) (*BackupManifest, error) {
	data, err := os.ReadFile(filepath.Join(backupDir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	var m BackupManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	return &m, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func writeManifest(dir string, m *BackupManifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, "manifest.json.tmp")
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "manifest.json"))
}

// copyFileRange copies n bytes starting at offset off from src to a new dst file.
func copyFileRange(src, dst string, off, n int64) error {
	sf, err := os.Open(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // VLog not present (KV-sep disabled or empty disk)
		}
		return err
	}
	defer sf.Close()

	if off > 0 {
		if _, err := sf.Seek(off, io.SeekStart); err != nil {
			return err
		}
	}

	df, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer df.Close()

	if _, err := io.CopyN(df, sf, n); err != nil && err != io.EOF {
		return err
	}
	return df.Sync()
}

// copyFileFull copies the entire src to a new dst file.
func copyFileFull(src, dst string) error {
	sf, err := os.Open(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer sf.Close()

	df, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer df.Close()

	if _, err := io.Copy(df, sf); err != nil {
		return err
	}
	return df.Sync()
}

// appendFile appends the contents of src to dst (creates dst if needed).
func appendFile(dst, src string) error {
	sf, err := os.Open(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer sf.Close()

	info, err := sf.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return nil // empty delta — nothing to append
	}

	df, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer df.Close()

	if _, err := io.Copy(df, sf); err != nil {
		return err
	}
	return df.Sync()
}
