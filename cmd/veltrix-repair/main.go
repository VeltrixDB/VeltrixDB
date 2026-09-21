// veltrix-repair — offline repair for value-transform metadata lost by
// pre-fix builds.
//
// # Who needs this
//
// Any node that ran a build predating the value-transform WAL fix, with
// compression enabled (zstd is the default) or --encrypt-at-rest, and that has
// been restarted since writing data. Those restarts rebuilt the index without
// the compression/encryption flags, so:
//
//   - after a CLEAN shutdown, reads SUCCEED but return the raw compressed or
//     encrypted blob — silent wrong data, no error anywhere;
//   - after a CRASH, reads FAIL with a CRC32C mismatch.
//
// The VLog bytes on disk were always correct. Only the metadata describing
// them was lost, which is why this repairs in place instead of requiring
// re-ingestion.
//
// # Usage
//
//	# 1. Always scan first. Read-only, changes nothing.
//	veltrix-repair --data-dirs /mnt/nvme0,/mnt/nvme1 --scan
//
//	# 2. Repair. Backs up each wal.log first.
//	veltrix-repair --data-dirs /mnt/nvme0,/mnt/nvme1 --repair
//
// Encryption: pass the same key the server uses, via VELTRIXDB_ENCRYPTION_KEY
// or --encryption-key-path, plus --encrypt-at-rest. Without the key, encrypted
// records cannot be resolved and will be reported as unresolved.
//
// # Safety
//
// THE SERVER MUST BE STOPPED. This opens the same data directories and
// rewrites the WAL as a corrected checkpoint on exit. Running it against a
// live node races the server's own writes.
//
// Exit codes: 0 = nothing to do / repair succeeded, 1 = scan found work to do,
// 2 = entries could not be resolved, 3 = error.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/VeltrixDB/veltrixdb/storage"
)

const (
	exitClean      = 0
	exitNeedsWork  = 1
	exitUnresolved = 2
	exitError      = 3
)

func main() {
	var (
		dataDir     = flag.String("data", "", "Single data directory. Ignored when --data-dirs is set.")
		dataDirs    = flag.String("data-dirs", "", "Comma-separated data directories, one per disk — must match the server's.")
		rawVlogs    = flag.String("raw-vlogs", "", "(Linux) Comma-separated raw VLog block devices, paired with --data-dirs.")
		scan        = flag.Bool("scan", false, "Read-only: report what a repair would change. Mutates nothing.")
		repair      = flag.Bool("repair", false, "Apply the repair and write a corrected checkpoint on exit.")
		encrypt     = flag.Bool("encrypt-at-rest", false, "Load the at-rest encryption key (needed to resolve encrypted records).")
		encKeyPath  = flag.String("encryption-key-path", "", "Path to the AES-256 key file. VELTRIXDB_ENCRYPTION_KEY takes priority.")
		compression = flag.String("compression", "zstd", "Compression algorithm the data was written with: none|flate|zstd.")
		backupDir   = flag.String("backup-dir", "", "Where to copy each wal.log before repairing. Default: alongside the original.")
	)
	flag.Parse()

	if *scan == *repair {
		fmt.Fprintln(os.Stderr, "error: pass exactly one of --scan or --repair (scan first)")
		flag.Usage()
		os.Exit(exitError)
	}

	dirs := splitList(*dataDirs)
	if len(dirs) == 0 && *dataDir != "" {
		dirs = []string{*dataDir}
	}
	if len(dirs) == 0 {
		fmt.Fprintln(os.Stderr, "error: --data or --data-dirs is required")
		os.Exit(exitError)
	}
	raws := splitList(*rawVlogs)
	if len(raws) > 0 && len(raws) != len(dirs) {
		fmt.Fprintf(os.Stderr, "error: --raw-vlogs has %d entries but --data-dirs has %d\n", len(raws), len(dirs))
		os.Exit(exitError)
	}

	cfg := storage.DefaultStorageConfig()
	cfg.DataDirPath = ""
	cfg.DataDirPaths = dirs
	cfg.RawVLogDevices = raws
	cfg.KeyValueSeparation = true
	cfg.Compression = *compression
	cfg.EncryptionEnabled = *encrypt
	cfg.EncryptionKeyPath = *encKeyPath

	// Keep background work out of the way: GC relocating records mid-scan
	// would invalidate offsets under us, and the scrubber would just add I/O.
	cfg.DefragInterval = 24 * time.Hour
	cfg.ScrubEnabled = false
	cfg.CacheMaxSizeMB = 64

	log.Printf("[repair] opening %d disk(s): %s", len(dirs), strings.Join(dirs, ", "))
	if *encrypt {
		log.Printf("[repair] encryption enabled — encrypted records will be resolved")
	} else {
		log.Printf("[repair] encryption NOT enabled; if this data was written with " +
			"--encrypt-at-rest, pass --encrypt-at-rest and the key or those records " +
			"will be reported unresolved")
	}

	se, err := storage.NewStorageEngine(cfg)
	if err != nil {
		log.Fatalf("[repair] open engine: %v", err)
	}

	// The index is only complete once background WAL replay finishes.
	log.Printf("[repair] waiting for WAL replay to complete…")
	<-se.ReplayDone
	log.Printf("[repair] replay complete; index holds %d live keys", se.GetIndexSize())

	if *scan {
		rep, err := se.RepairTransformMetadata(true)
		if err != nil {
			log.Fatalf("[repair] scan: %v", err)
		}
		fmt.Println(rep.String())

		// Deliberately do NOT Close(): Close writes a checkpoint, which would
		// rewrite the WAL. A scan must leave the data directory byte-identical.
		switch {
		case rep.HasUnresolved():
			fmt.Println("\nSome entries could not be resolved — see above. If these are encrypted,\n" +
				"re-run with --encrypt-at-rest and the correct key. Otherwise they indicate\n" +
				"real corruption and need a restore from backup.")
			os.Exit(exitUnresolved)
		case rep.NeedsRepair():
			fmt.Println("\nRe-run with --repair to fix. The server must be stopped.")
			os.Exit(exitNeedsWork)
		default:
			fmt.Println("\nNothing to repair — all metadata agrees with the on-disk bytes.")
			os.Exit(exitClean)
		}
	}

	// ── Repair ────────────────────────────────────────────────────────────
	// Back up every WAL before touching anything. Close() replaces wal.log
	// with a checkpoint built from the repaired index; if the repair were
	// wrong, that would be the only copy.
	stamp := time.Now().UTC().Format("20060102T150405Z")
	for _, d := range dirs {
		src := filepath.Join(d, "wal.log")
		dstDir := d
		if *backupDir != "" {
			dstDir = *backupDir
		}
		dst := filepath.Join(dstDir, fmt.Sprintf("wal.log.prerepair.%s", stamp))
		if err := copyFile(src, dst); err != nil {
			if os.IsNotExist(err) {
				log.Printf("[repair] %s: no wal.log to back up (skipping)", d)
				continue
			}
			log.Fatalf("[repair] backing up %s → %s: %v (refusing to proceed)", src, dst, err)
		}
		log.Printf("[repair] backed up %s → %s", src, dst)
	}

	rep, err := se.RepairTransformMetadata(false)
	if err != nil {
		log.Fatalf("[repair] repair: %v", err)
	}
	fmt.Println(rep.String())

	// Close writes the corrected checkpoint — this is what makes the repair
	// durable. Without it the fixed metadata dies with the process.
	log.Printf("[repair] writing corrected checkpoint…")
	if err := se.Close(); err != nil {
		log.Fatalf("[repair] close (checkpoint write): %v — WAL backups are intact", err)
	}
	log.Printf("[repair] done")

	if rep.HasUnresolved() {
		fmt.Println("\nRepair applied, but some entries remain unresolved (see above).")
		os.Exit(exitUnresolved)
	}
	os.Exit(exitClean)
}

func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// copyFile copies src to dst and fsyncs it, so the backup is durable before
// the repair proceeds.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err // caller checks os.IsNotExist
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
