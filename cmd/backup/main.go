// veltrixdb-backup — offline backup and restore tool for VeltrixDB.
//
// Usage:
//   veltrixdb-backup full         --data-dirs=<dirs> --dest=<dir>
//   veltrixdb-backup incremental  --data-dirs=<dirs> --dest=<dir> --base=<dir>
//   veltrixdb-backup restore      --chain=<dir1,dir2,...> --data-dirs=<dirs>
//   veltrixdb-backup archive-status --archive=<dir>
//   veltrixdb-backup restore-pitr --base-backup=<dir> --archive=<dir> --until=<RFC3339|version:N> --data=<dirs>
//   veltrixdb-backup upload       --src=<local-backup-dir> --provider=s3|gcs|azure --bucket=<bucket> [cloud flags]
//   veltrixdb-backup download     --cloud-path=<prefix> --dest=<local-dir> --provider=... [cloud flags]
//   veltrixdb-backup list-cloud   --provider=... --bucket=<bucket> [cloud flags]
//   veltrixdb-backup full-cloud   --data-dirs=<dirs> --provider=... --bucket=<bucket> [cloud flags]
//
// Examples:
//   # Full backup of a single-disk server:
//   veltrixdb-backup full --data-dirs=/data --dest=/backup/full-2026-05-02
//
//   # Upload a local backup to S3:
//   veltrixdb-backup upload \
//       --src=/backup/full-2026-05-02 \
//       --provider=s3 --bucket=my-veltrix-backups --region=us-east-1
//
//   # Full backup straight to GCS in one step:
//   veltrixdb-backup full-cloud \
//       --data-dirs=/data \
//       --provider=gcs --bucket=my-bucket --gcs-cred-file=/sa.json
//
//   # Download from Azure and restore:
//   veltrixdb-backup download \
//       --provider=azure --bucket=my-container \
//       --cloud-path=backups/full-2026-05-02 \
//       --dest=/tmp/restored
//   veltrixdb-backup restore --chain=/tmp/restored --data-dirs=/data-new
//
//   # List all cloud backups:
//   veltrixdb-backup list-cloud --provider=s3 --bucket=my-bucket --region=us-east-1
//
//   # Inspect the PITR WAL archive (segments, version + time coverage):
//   veltrixdb-backup archive-status --archive=/backup/wal-archive
//
//   # Point-in-time restore: base full backup + archived WAL up to a moment
//   # (engine must be stopped; --data must be fresh, empty directories):
//   veltrixdb-backup restore-pitr \
//       --base-backup=/backup/full-2026-05-02 \
//       --archive=/backup/wal-archive \
//       --until=2026-05-02T14:30:00Z \
//       --data=/data-new
//   # ...or exact to a single write:  --until=version:123456
//
// Cloud auth (env vars override flags):
//   S3:    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_SESSION_TOKEN
//   GCS:   GOOGLE_APPLICATION_CREDENTIALS (service account file) or GCS_ACCESS_TOKEN
//   Azure: AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, AZURE_STORAGE_CONTAINER
//
// The engine must be STOPPED before running restore.
// Full and incremental backups are safe to run against a live engine.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/VeltrixDB/veltrixdb/storage"
)

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "full":
		cmdFull(os.Args[2:])
	case "incremental":
		cmdIncremental(os.Args[2:])
	case "restore":
		cmdRestore(os.Args[2:])
	case "archive-status":
		cmdArchiveStatus(os.Args[2:])
	case "restore-pitr":
		cmdRestorePITR(os.Args[2:])
	case "upload":
		cmdUpload(os.Args[2:])
	case "download":
		cmdDownload(os.Args[2:])
	case "list-cloud":
		cmdListCloud(os.Args[2:])
	case "full-cloud":
		cmdFullCloud(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `veltrixdb-backup <command> [flags]

Local commands:
  full            Create a full backup (engine can be running)
  incremental     Create an incremental backup relative to a base (engine can be running)
  restore         Restore from a backup chain (engine must be stopped)

Point-in-time recovery:
  archive-status  Show WAL-archive segments, version and time coverage
  restore-pitr    Restore a base full backup + archived WAL up to an exact
                  version or RFC3339 timestamp (engine must be stopped;
                  target data dirs must be fresh)

Cloud commands:
  upload        Upload a local backup directory to S3 / GCS / Azure
  download      Download a cloud backup to a local directory
  list-cloud    List all backups stored in cloud storage
  full-cloud    Full backup + upload to cloud in one step

Run 'veltrixdb-backup <command> -help' for per-command flags.`)
}

// ── full ──────────────────────────────────────────────────────────────────────

func cmdFull(args []string) {
	fs := flag.NewFlagSet("full", flag.ExitOnError)
	dataDirs := fs.String("data-dirs", "", "comma-separated data directories (required)")
	dest := fs.String("dest", "", "destination backup directory (required)")
	cacheSize := fs.Int("cache-mb", 64, "LIRS cache size in MB for opening engine")
	_ = fs.Parse(args)

	if *dataDirs == "" || *dest == "" {
		fs.Usage()
		os.Exit(1)
	}

	se, err := openEngine(*dataDirs, *cacheSize)
	if err != nil {
		log.Fatalf("open engine: %v", err)
	}
	defer se.Close()

	be := storage.NewBackupEngine(se)
	start := time.Now()
	m, err := be.FullBackup(*dest)
	if err != nil {
		log.Fatalf("full backup: %v", err)
	}
	fmt.Printf("full backup complete\n  id:      %s\n  dest:    %s\n  version: %d\n  elapsed: %s\n",
		m.BackupID, *dest, m.EngineVersion, time.Since(start).Round(time.Millisecond))
}

// ── incremental ───────────────────────────────────────────────────────────────

func cmdIncremental(args []string) {
	fs := flag.NewFlagSet("incremental", flag.ExitOnError)
	dataDirs := fs.String("data-dirs", "", "comma-separated data directories (required)")
	dest := fs.String("dest", "", "destination backup directory (required)")
	base := fs.String("base", "", "base backup directory (required)")
	cacheSize := fs.Int("cache-mb", 64, "LIRS cache size in MB")
	_ = fs.Parse(args)

	if *dataDirs == "" || *dest == "" || *base == "" {
		fs.Usage()
		os.Exit(1)
	}

	baseManifest, err := storage.ReadManifest(*base)
	if err != nil {
		log.Fatalf("read base manifest: %v", err)
	}

	se, err := openEngine(*dataDirs, *cacheSize)
	if err != nil {
		log.Fatalf("open engine: %v", err)
	}
	defer se.Close()

	be := storage.NewBackupEngine(se)
	start := time.Now()
	m, err := be.IncrementalBackup(*dest, baseManifest)
	if err != nil {
		log.Fatalf("incremental backup: %v", err)
	}
	fmt.Printf("incremental backup complete\n  id:      %s\n  dest:    %s\n  base:    %s\n  version: %d\n  elapsed: %s\n",
		m.BackupID, *dest, *base, m.EngineVersion, time.Since(start).Round(time.Millisecond))
}

// ── restore ───────────────────────────────────────────────────────────────────

func cmdRestore(args []string) {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	chainFlag := fs.String("chain", "", "comma-separated backup directories in chain order, oldest first (required)")
	dataDirs := fs.String("data-dirs", "", "comma-separated destination data directories (required)")
	_ = fs.Parse(args)

	if *chainFlag == "" || *dataDirs == "" {
		fs.Usage()
		os.Exit(1)
	}

	chainDirs := splitTrim(*chainFlag)
	destDirs := splitTrim(*dataDirs)

	// Read manifests.
	manifests := make([]*storage.BackupManifest, len(chainDirs))
	for i, d := range chainDirs {
		m, err := storage.ReadManifest(d)
		if err != nil {
			log.Fatalf("read manifest[%d] at %s: %v", i, d, err)
		}
		manifests[i] = m
	}

	start := time.Now()
	if err := storage.Restore(manifests, chainDirs, destDirs); err != nil {
		log.Fatalf("restore: %v", err)
	}
	fmt.Printf("restore complete\n  chain:   %d steps\n  dest:    %s\n  elapsed: %s\n",
		len(chainDirs), *dataDirs, time.Since(start).Round(time.Millisecond))
	fmt.Println("You can now start the engine with --data-dirs=" + *dataDirs)
}

// ── archive-status ────────────────────────────────────────────────────────────

func cmdArchiveStatus(args []string) {
	fs := flag.NewFlagSet("archive-status", flag.ExitOnError)
	archive := fs.String("archive", "", "WAL archive directory (required)")
	_ = fs.Parse(args)

	if *archive == "" {
		fs.Usage()
		os.Exit(1)
	}

	segs, err := storage.ListArchiveSegments(*archive)
	if err != nil {
		log.Fatalf("archive-status: %v", err)
	}
	if len(segs) == 0 {
		fmt.Println("no archive segments found")
		return
	}

	fmt.Printf("%-5s %-14s %-24s %-9s %-13s %-25s %-25s\n",
		"DISK", "SEQ", "VERSIONS", "ENTRIES", "SIZE", "FIRST ENTRY", "LAST ENTRY")
	fmt.Println(strings.Repeat("-", 120))

	var totalBytes int64
	var totalEntries int
	perDisk := map[int]int{}
	for _, s := range segs {
		fmt.Printf("%-5d %-14d %-24s %-9d %-13s %-25s %-25s\n",
			s.DiskIdx, s.Seq,
			fmt.Sprintf("%d..%d", s.FirstVersion, s.LastVersion),
			s.Entries, formatBytes(s.SizeBytes),
			time.Unix(0, s.FirstUnixNs).UTC().Format(time.RFC3339),
			time.Unix(0, s.LastUnixNs).UTC().Format(time.RFC3339))
		totalBytes += s.SizeBytes
		totalEntries += s.Entries
		perDisk[s.DiskIdx]++
	}

	fmt.Printf("\ntotal: %d segments across %d disk(s), %d entries, %s\n",
		len(segs), len(perDisk), totalEntries, formatBytes(totalBytes))
	fmt.Printf("restorable range: version %d .. %d, %s .. %s\n",
		segs[0].FirstVersion, maxLastVersion(segs),
		time.Unix(0, minFirstNs(segs)).UTC().Format(time.RFC3339),
		time.Unix(0, maxLastNs(segs)).UTC().Format(time.RFC3339))
}

func maxLastVersion(segs []storage.ArchiveSegmentMeta) uint64 {
	var m uint64
	for _, s := range segs {
		if s.LastVersion > m {
			m = s.LastVersion
		}
	}
	return m
}

func minFirstNs(segs []storage.ArchiveSegmentMeta) int64 {
	m := segs[0].FirstUnixNs
	for _, s := range segs {
		if s.FirstUnixNs < m {
			m = s.FirstUnixNs
		}
	}
	return m
}

func maxLastNs(segs []storage.ArchiveSegmentMeta) int64 {
	var m int64
	for _, s := range segs {
		if s.LastUnixNs > m {
			m = s.LastUnixNs
		}
	}
	return m
}

// ── restore-pitr ──────────────────────────────────────────────────────────────

func cmdRestorePITR(args []string) {
	fs := flag.NewFlagSet("restore-pitr", flag.ExitOnError)
	baseBackup := fs.String("base-backup", "", "base FULL backup directory (required)")
	archive := fs.String("archive", "", "WAL archive directory (required)")
	until := fs.String("until", "", "restore target: RFC3339 timestamp or version:N (required)")
	data := fs.String("data", "", "comma-separated FRESH destination data directories (required)")
	_ = fs.Parse(args)

	if *baseBackup == "" || *archive == "" || *until == "" || *data == "" {
		fs.Usage()
		os.Exit(1)
	}

	target, err := storage.ParsePITRTarget(*until)
	if err != nil {
		log.Fatalf("restore-pitr: %v", err)
	}
	destDirs := splitTrim(*data)

	start := time.Now()
	applied, err := storage.RestorePITR(*baseBackup, *archive, target, destDirs)
	if err != nil {
		log.Fatalf("restore-pitr: %v", err)
	}

	fmt.Printf("point-in-time restore complete\n  base:    %s\n  until:   %s\n  applied: %d archived entries\n  dest:    %s\n  elapsed: %s\n",
		*baseBackup, *until, applied, *data, time.Since(start).Round(time.Millisecond))
	fmt.Println("You can now start the engine with --data-dirs=" + *data)
}

// ── cloud commands ────────────────────────────────────────────────────────────

// cloudFlags adds provider / bucket / auth flags to a FlagSet and returns a
// function that builds a CloudBackupConfig from the parsed values.
func cloudFlags(fs *flag.FlagSet) func() storage.CloudBackupConfig {
	provider := fs.String("provider", "", "cloud provider: s3, gcs, or azure (required)")
	bucket := fs.String("bucket", "", "S3/GCS bucket or Azure container name (required)")
	prefix := fs.String("prefix", "veltrixdb-backups", "key prefix inside the bucket")
	// S3
	region := fs.String("region", "", "S3 region (or AWS_REGION env)")
	accessKey := fs.String("aws-access-key", "", "AWS access key ID (or AWS_ACCESS_KEY_ID env)")
	secretKey := fs.String("aws-secret-key", "", "AWS secret access key (or AWS_SECRET_ACCESS_KEY env)")
	// GCS
	gcsToken := fs.String("gcs-token", "", "GCS raw access token (or GCS_ACCESS_TOKEN env)")
	gcsCredFile := fs.String("gcs-cred-file", "", "Path to GCS service account JSON (or GOOGLE_APPLICATION_CREDENTIALS env)")
	// Azure
	azureAccount := fs.String("azure-account", "", "Azure storage account name (or AZURE_STORAGE_ACCOUNT env)")
	azureKey := fs.String("azure-key", "", "Azure storage account key (or AZURE_STORAGE_KEY env)")

	return func() storage.CloudBackupConfig {
		return storage.CloudBackupConfig{
			Provider:        storage.CloudProvider(*provider),
			Bucket:          *bucket,
			Prefix:          *prefix,
			Region:          *region,
			AccessKeyID:     *accessKey,
			SecretAccessKey: *secretKey,
			GCSAccessToken:  *gcsToken,
			GCSCredFile:     *gcsCredFile,
			AzureAccount:    *azureAccount,
			AzureKey:        *azureKey,
		}
	}
}

func cmdUpload(args []string) {
	fs := flag.NewFlagSet("upload", flag.ExitOnError)
	src := fs.String("src", "", "local backup directory to upload (required)")
	buildCfg := cloudFlags(fs)
	_ = fs.Parse(args)

	if *src == "" {
		fs.Usage()
		os.Exit(1)
	}
	cfg := buildCfg()
	if cfg.Provider == "" || cfg.Bucket == "" {
		log.Fatal("--provider and --bucket are required")
	}

	u := storage.NewCloudBackupUploader(cfg)
	start := time.Now()
	cloudPath, err := u.Upload(*src)
	if err != nil {
		log.Fatalf("upload: %v", err)
	}
	fmt.Printf("upload complete\n  src:        %s\n  cloud-path: %s\n  provider:   %s\n  elapsed:    %s\n",
		*src, cloudPath, cfg.Provider, time.Since(start).Round(time.Millisecond))
}

func cmdDownload(args []string) {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	cloudPath := fs.String("cloud-path", "", "cloud path prefix returned by upload (required)")
	dest := fs.String("dest", "", "local destination directory (required)")
	buildCfg := cloudFlags(fs)
	_ = fs.Parse(args)

	if *cloudPath == "" || *dest == "" {
		fs.Usage()
		os.Exit(1)
	}
	cfg := buildCfg()
	if cfg.Provider == "" || cfg.Bucket == "" {
		log.Fatal("--provider and --bucket are required")
	}

	u := storage.NewCloudBackupUploader(cfg)
	start := time.Now()
	if err := u.Download(*cloudPath, *dest); err != nil {
		log.Fatalf("download: %v", err)
	}
	fmt.Printf("download complete\n  cloud-path: %s\n  dest:       %s\n  elapsed:    %s\n",
		*cloudPath, *dest, time.Since(start).Round(time.Millisecond))
}

func cmdListCloud(args []string) {
	fs := flag.NewFlagSet("list-cloud", flag.ExitOnError)
	buildCfg := cloudFlags(fs)
	_ = fs.Parse(args)

	cfg := buildCfg()
	if cfg.Provider == "" || cfg.Bucket == "" {
		log.Fatal("--provider and --bucket are required")
	}

	u := storage.NewCloudBackupUploader(cfg)
	entries, err := u.ListBackups()
	if err != nil {
		log.Fatalf("list-cloud: %v", err)
	}
	if len(entries) == 0 {
		fmt.Println("no backups found")
		return
	}
	fmt.Printf("%-50s  %-35s  %12s\n", "CLOUD PATH", "BACKUP ID", "SIZE")
	fmt.Println(strings.Repeat("-", 100))
	for _, e := range entries {
		fmt.Printf("%-50s  %-35s  %12s\n", e.CloudPath, e.BackupID, formatBytes(e.SizeBytes))
	}
}

func cmdFullCloud(args []string) {
	fs := flag.NewFlagSet("full-cloud", flag.ExitOnError)
	dataDirs := fs.String("data-dirs", "", "comma-separated data directories (required)")
	cacheSize := fs.Int("cache-mb", 64, "LIRS cache size in MB")
	buildCfg := cloudFlags(fs)
	_ = fs.Parse(args)

	if *dataDirs == "" {
		fs.Usage()
		os.Exit(1)
	}
	cfg := buildCfg()
	if cfg.Provider == "" || cfg.Bucket == "" {
		log.Fatal("--provider and --bucket are required")
	}

	// 1. Open engine and run local full backup to temp dir.
	se, err := openEngine(*dataDirs, *cacheSize)
	if err != nil {
		log.Fatalf("open engine: %v", err)
	}
	defer se.Close()

	tmpDir, err := os.MkdirTemp("", "veltrix-backup-staging-")
	if err != nil {
		log.Fatalf("mktemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	be := storage.NewBackupEngine(se)
	start := time.Now()
	m, err := be.FullBackup(tmpDir)
	if err != nil {
		log.Fatalf("full backup: %v", err)
	}
	fmt.Printf("local backup done  id=%s  elapsed=%s\n", m.BackupID, time.Since(start).Round(time.Millisecond))

	// 2. Upload staging dir to cloud.
	u := storage.NewCloudBackupUploader(cfg)
	uploadStart := time.Now()
	cloudPath, err := u.Upload(tmpDir)
	if err != nil {
		log.Fatalf("cloud upload: %v", err)
	}
	fmt.Printf("upload complete\n  backup-id:  %s\n  cloud-path: %s\n  provider:   %s\n  total:      %s\n",
		m.BackupID, cloudPath, cfg.Provider, time.Since(start).Round(time.Millisecond))
	_ = uploadStart
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func openEngine(dataDirsFlag string, cacheMB int) (*storage.StorageEngine, error) {
	dirs := splitTrim(dataDirsFlag)
	cfg := storage.DefaultStorageConfig()
	cfg.DataDirPaths = dirs
	cfg.DataDirPath = dirs[0]
	cfg.CacheMaxSizeMB = uint32(cacheMB)
	cfg.KeyValueSeparation = true
	// Use very short flush windows so we don't block during backup.
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	return storage.NewStorageEngine(cfg)
}

func splitTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
