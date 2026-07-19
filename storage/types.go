package storage

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// ── VLog constants ───────────────────────────────────────────────────────────

// vlogMagic: "VLT\x02" — VLog format version 2 (value-only, no key stored).
const vlogMagic = uint32(0x564C5402)

// vlogHeaderBytes is the fixed record header size in the VLog.
// Layout (24 bytes, matches the on-disk format in vlog.go):
//   0  4  Magic
//   4  4  ValLen
//   8  4  CRC32C of value
//  12  4  Reserved
//  16  8  WriteTimestampUs
const vlogHeaderBytes = 24

// ── Index entry flags ────────────────────────────────────────────────────────

const (
	FlagTombstone        uint8 = 0x01 // record is deleted
	FlagCompressed       uint8 = 0x02 // value stored with zstd compression
	FlagEncrypted        uint8 = 0x04 // value is encrypted at rest
	FlagHasTTL           uint8 = 0x08 // TTLExpiryUs is a valid absolute deadline
	FlagReadRepairNeeded uint8 = 0x10 // replica is lagging; trigger read repair
	FlagPinned           uint8 = 0x20 // never evict this entry from Index Vault
	FlagPacked           uint8 = 0x40 // VLog record is packed (multiple records share a 4 KB block)
	FlagTiered           uint8 = 0x80 // value has been demoted to cold tier; DiskOffset holds tier handle hash
)

// ── 64-byte Index Vault entry ────────────────────────────────────────────────
//
// Every key in the system maps to exactly one IndexEntry.  The struct is sized
// to fill a single CPU cache line so that one pointer dereference fetches all
// metadata with no additional cache misses.  HugePages on the Index Vault heap
// keep TLB pressure minimal during random-access patterns.
//
// Memory layout (offsets verified by the init() assertion below):
//
//	 0-  7  KeyHash          uint64   FNV-1a fingerprint of raw key
//	 8- 15  DiskOffset       uint64   byte offset within the segment file
//	16- 19  SegmentID        uint32   which segment file (seg_XXXXXXXX.dat)
//	20- 23  ValueSize        uint32   compressed bytes stored on disk
//	24- 27  UncompressedSize uint32   original size (== ValueSize when !Compressed)
//	28- 31  KeySize          uint32   key length in bytes
//	32- 39  WriteTimestampUs int64    write time µs since Unix epoch
//	40- 47  TTLExpiryUs      int64    absolute expiry µs (0 = immortal)
//	48- 51  CRC32C           uint32   CRC32C of the uncompressed value
//	52- 53  ShardID          uint16   owning index shard
//	54      Flags            uint8    FlagXxx bitmask
//	55      SchemaVersion    uint8    format version for rolling upgrades
//	56- 63  _reserved        [8]byte  zero-padded; future HLC/CRDT extension
type IndexEntry struct {
	KeyHash          uint64
	DiskOffset       uint64
	SegmentID        uint32
	ValueSize        uint32
	UncompressedSize uint32
	KeySize          uint32
	WriteTimestampUs int64
	TTLExpiryUs      int64
	CRC32C           uint32
	ShardID          uint16
	Flags            uint8
	SchemaVersion    uint8
	_reserved        [8]byte
}

func init() {
	if unsafe.Sizeof(IndexEntry{}) != 64 {
		panic("IndexEntry must be exactly 64 bytes (one cache line)")
	}
}

func (e *IndexEntry) IsTombstone() bool  { return e.Flags&FlagTombstone != 0 }
func (e *IndexEntry) IsCompressed() bool { return e.Flags&FlagCompressed != 0 }
func (e *IndexEntry) IsEncrypted() bool  { return e.Flags&FlagEncrypted != 0 }
func (e *IndexEntry) HasTTL() bool       { return e.Flags&FlagHasTTL != 0 }
func (e *IndexEntry) IsPacked() bool     { return e.Flags&FlagPacked != 0 }

func (e *IndexEntry) IsExpired(nowUs int64) bool {
	return e.HasTTL() && e.TTLExpiryUs > 0 && nowUs >= e.TTLExpiryUs
}

func (e *IndexEntry) MarkTombstone(nowUs int64) {
	e.Flags |= FlagTombstone
	e.ValueSize = 0
	e.WriteTimestampUs = nowUs
}

// ── WAL ──────────────────────────────────────────────────────────────────────

// WALEntry is one record appended to the Write-Ahead Log.
type WALEntry struct {
	Timestamp     int64
	KeyLen        uint32
	Key           string
	ValueLen      uint32
	Value         []byte
	Checksum      uint32 // CRC32C of Value
	ReplicationID uint32
	Version       uint64
	IsTombstone   bool
	// VLogOffset is the byte offset in the per-disk VLog where the value was
	// written. Non-zero only in KV-separation mode. When > 0, the value bytes
	// are NOT written into the WAL (they are already durable in the VLog after
	// VLog fdatasync completes). On replay this offset is used directly to
	// rebuild the IndexEntry without re-appending the value to the VLog.
	VLogOffset int64
	// Packed is true when the VLog record at VLogOffset is part of a packed
	// 4 KB block (multiple records sharing one block). Replay restores this
	// flag onto IndexEntry so MarkDead later subtracts the correct footprint.
	// Stored as the 8th pipe-delimited field in the on-disk WAL header.
	// Backward-compat: WAL records without an 8th field default to Packed=false.
	Packed bool
}

// ── SSTable metadata ─────────────────────────────────────────────────────────

type SSTableMetadata struct {
	ID              uint64
	MinKey          string
	MaxKey          string
	EntryCount      uint64
	FileSize        uint64
	CreatedAtNs     int64
	Level           int
	BloomFilterSize uint32
	IndexBlockSize  uint32
}

// ── Bloom filter ─────────────────────────────────────────────────────────────

type BloomFilter struct {
	mu   sync.RWMutex
	bits []byte
	k    uint16
	size uint64
}

// ── Compaction ───────────────────────────────────────────────────────────────

type CompactionRequest struct {
	SourceLevel    int
	TargetLevel    int
	PartitionID    uint32
	SequenceNumber uint64
	CreatedAtNs    int64
}

// ── Admission control ────────────────────────────────────────────────────────

// AdmissionControl coordinates write throttling and GC pausing based on the
// read-latency EWMA.  When the EWMA crosses admissionThrottleNs (20 ms), writes
// are delayed 2 ms per Put and VLog GC is paused to give reads full NVMe
// headroom.  Both resume when EWMA falls below admissionResumeNs (10 ms).
// All fields are atomics — no lock required.
type AdmissionControl struct {
	WriteThrottleActive atomic.Bool   // true → add 2ms sleep per Put
	GCPaused            atomic.Bool   // true → compactVLog returns immediately
	WriteThrottleEvents atomic.Uint64 // cumulative throttle activations
	// LastReadNs is the Unix nanosecond timestamp of the most recent Get()
	// call. Used by compactVLog to detect stale EWMA: if no read has arrived
	// in > 60 s, GCPaused is automatically cleared so GC is not permanently
	// suppressed during write-only workloads.
	LastReadNs atomic.Int64

	// GCBackpressureUntilNs is the Unix nanosecond deadline until which each
	// Put() should sleep 1 ms before WAL submission.  Set by compactVLog when
	// a batch flush sees >20% CAS failures — a signal that user writes are
	// overwriting GC candidates faster than GC can commit its relocations.
	// Zero means inactive.  Lighter than WriteThrottleActive (1 ms vs 2 ms)
	// and independent of read latency.
	GCBackpressureUntilNs atomic.Int64
	// GCCASThrottles counts Put() calls delayed by GC CAS backpressure.
	GCCASThrottles atomic.Uint64
}

const (
	admissionThrottleNs int64 = 20_000_000 // 20 ms EWMA → activate throttle + pause GC
	admissionResumeNs   int64 = 10_000_000 // 10 ms EWMA → deactivate throttle + resume GC

	// readEWMASampleEvery is the 1-in-N sample rate for the read-latency EWMA
	// CAS update on the Get() hot path. At 2 M ops/sec the per-Get CAS-loop
	// causes severe cache-line ping-pong across cores; sampling every 64th
	// read gives admission control still-fresh signal (≤32 µs lag at 2 M/s)
	// while reducing CAS pressure 64× — net P99 improvement of ~30 % at the
	// 2 M ops/sec read-heavy ceiling on n2-highmem-64.
	//
	// Must be a power of two so we can mask instead of mod.
	readEWMASampleEvery uint64 = 64
	readEWMASampleMask  uint64 = readEWMASampleEvery - 1
)

// ── Metrics ──────────────────────────────────────────────────────────────────

type EvictionMetrics struct {
	TotalEvictions     *atomic.Uint64
	LIRSEvictions      *atomic.Uint64
	TTLEvictions       *atomic.Uint64
	DefragEvictions    *atomic.Uint64
	EvictionLatencyNs  *atomic.Int64
}

type StorageMetrics struct {
	Writes              *atomic.Uint64
	Reads               *atomic.Uint64
	Deletes             *atomic.Uint64
	WritesLatencyNs     *atomic.Int64
	ReadsLatencyNs      *atomic.Int64
	DeletesLatencyNs    *atomic.Int64
	CacheHits           *atomic.Uint64
	CacheMisses         *atomic.Uint64
	BloomFilterFalsePos *atomic.Uint64
	BloomFilterSkipped  *atomic.Uint64 // negative Gets shortcut by the bloom filter
	CompactionRuns      *atomic.Uint64
	DiskFailures        *atomic.Uint64 // breakers tripped (disk_health.go)
	WALFlushes          *atomic.Uint64
	SSTableCreations    *atomic.Uint64
	DefragRuns          *atomic.Uint64
	TombstonesCollected *atomic.Uint64
	BackPressureEvents  *atomic.Uint64
	EvictionMetrics     *EvictionMetrics

	// VLog metrics (Key-Value Separation path only)
	VLogWrites   *atomic.Uint64 // values appended to VLog
	VLogReads    *atomic.Uint64 // values read from VLog (cache miss path)
	VLogGCRuns   *atomic.Uint64 // VLog compaction passes that reclaimed ≥1 byte
	VLogGCBytes  *atomic.Uint64 // bytes reclaimed by VLog GC

	// VLog GC diagnostic counters — expose why GC is or is not making progress.
	VLogGCSkippedRatio  *atomic.Uint64 // compactVLog exited: GCRatio < threshold
	VLogGCSkippedPaused *atomic.Uint64 // compactVLog exited: GCPaused was true
	VLogGCSkippedEmpty  *atomic.Uint64 // compactVLog exited: zero candidates found
	VLogGCReadErrors    *atomic.Uint64 // ReadValue failures inside GC candidate loop
	VLogGCCASFails      *atomic.Uint64 // CAS misses (concurrent Put won the entry)
	VLogGCCandidates    *atomic.Uint64 // total candidates scanned across all disks
	// VLogGCEmergencyRuns counts compactVLog passes that bypassed GCPaused
	// because garbage ratio crossed gcEmergencyRatio (65%). A persistent
	// non-zero rate is a strong signal that write rate is exceeding sustainable
	// GC throughput — investigate disk IOPS or lower the workload.
	VLogGCEmergencyRuns *atomic.Uint64

	// VLogBlkDiscardErrors counts BLKDISCARD ioctl failures in punchDeadHead.
	// A non-zero rate means the process lacks CAP_SYS_RAWIO — NVMe TRIM is
	// silently skipped so the raw VLog head will grow without being reclaimed.
	// Fix: add SYS_RAWIO to the container's securityContext.capabilities.add.
	VLogBlkDiscardErrors *atomic.Uint64

	// Scrubber metrics — counted by the background goroutine in scrubber.go.
	ScrubRecords    *atomic.Uint64 // VLog records inspected
	ScrubCorruption *atomic.Uint64 // CRC32C or magic mismatches detected
	ScrubBytes      *atomic.Uint64 // bytes read by the scrubber across all disks
	ScrubReadErrors *atomic.Uint64 // pread errors during scrub (transient)

	// Atomic ops (CAS, INCR, DECR, SETNX) counted as a single counter — the
	// breakdown by op is exposed by the wire-protocol layer if needed.
	AtomicOps *atomic.Uint64

	// Audit log dropped records (channel was full).
	AuditDropped *atomic.Uint64

	// Read latency EWMA (α=1/8) in nanoseconds.  Updated by Get() at the
	// readEWMASampleEvery cadence (1-in-N reads) so the CAS loop does not
	// pingpong across all cores at multi-million ops/sec.  Read by the
	// defragmenter to throttle GC writes when reads are slow.
	ReadLatencyEWMANs atomic.Int64

	// readEWMACounter advances on every Get() — the CAS update only runs when
	// (counter & readEWMASampleMask) == 0.  Atomic adds on a hot counter are
	// far cheaper than a contended CAS-loop on the EWMA itself.
	readEWMACounter atomic.Uint64

	// Admission control: write throttle + GC pause driven by read EWMA.
	Admission *AdmissionControl

	// ── Server metrics (wired by cmd/server at startup) ──────────────────────
	// All fields are atomic so the server goroutines can update them without
	// holding any storage lock.
	ActiveConnections  *atomic.Int64  // current open TCP connections (gauge)
	NetworkBytesIn     *atomic.Uint64 // cumulative bytes read from clients
	NetworkBytesOut    *atomic.Uint64 // cumulative bytes written to clients

	// ── Pipeline coalescing (binary protocol tryCoalesce* paths) ─────────────
	MultiPutBatches *atomic.Uint64 // number of vectorized MultiPut batches dispatched
	MultiPutEntries *atomic.Uint64 // total PUT entries across all MultiPut batches
	MultiGetBatches *atomic.Uint64 // number of vectorized MultiGet batches dispatched
	MultiGetEntries *atomic.Uint64 // total GET keys across all MultiGet batches

	// Histogram observers wired from the Prometheus collector at startup.
	// Initialized to no-ops so engine code never needs a nil check.
	ObserveWriteLatency  func(float64)
	ObserveReadLatency   func(float64)
	ObserveDeleteLatency func(float64)
}

// VLogStats holds per-disk VLog file statistics for observability.
type VLogStats struct {
	DiskIdx           int
	Path              string
	FileBytes         int64
	LiveBytes         int64
	GarbageRatio      float64
	WriteBytes        int64 // raw value bytes written (unpadded, excludes header + alignment padding)
	ReadBytes         int64 // raw value bytes returned to callers
	WriteLatencyEWMAs float64 // EWMA of per-write latency in seconds (sampled 1-in-32)
	ReadLatencyEWMAs  float64 // EWMA of per-read latency in seconds (sampled 1-in-32)
	Slow              bool    // true when this disk's EWMA is > 5× the cluster median
}

func newStorageMetrics() *StorageMetrics {
	return &StorageMetrics{
		Writes:              &atomic.Uint64{},
		Reads:               &atomic.Uint64{},
		Deletes:             &atomic.Uint64{},
		WritesLatencyNs:     &atomic.Int64{},
		ReadsLatencyNs:      &atomic.Int64{},
		DeletesLatencyNs:    &atomic.Int64{},
		CacheHits:           &atomic.Uint64{},
		CacheMisses:         &atomic.Uint64{},
		BloomFilterFalsePos: &atomic.Uint64{},
		BloomFilterSkipped:  &atomic.Uint64{},
		CompactionRuns:      &atomic.Uint64{},
		DiskFailures:        &atomic.Uint64{},
		WALFlushes:          &atomic.Uint64{},
		SSTableCreations:    &atomic.Uint64{},
		DefragRuns:          &atomic.Uint64{},
		TombstonesCollected: &atomic.Uint64{},
		BackPressureEvents:  &atomic.Uint64{},
		EvictionMetrics: &EvictionMetrics{
			TotalEvictions:    &atomic.Uint64{},
			LIRSEvictions:     &atomic.Uint64{},
			TTLEvictions:      &atomic.Uint64{},
			DefragEvictions:   &atomic.Uint64{},
			EvictionLatencyNs: &atomic.Int64{},
		},
		VLogWrites:  &atomic.Uint64{},
		VLogReads:   &atomic.Uint64{},
		VLogGCRuns:  &atomic.Uint64{},
		VLogGCBytes: &atomic.Uint64{},

		VLogGCSkippedRatio:  &atomic.Uint64{},
		VLogGCSkippedPaused: &atomic.Uint64{},
		VLogGCSkippedEmpty:  &atomic.Uint64{},
		VLogGCReadErrors:    &atomic.Uint64{},
		VLogGCCASFails:      &atomic.Uint64{},
		VLogGCCandidates:    &atomic.Uint64{},
		VLogGCEmergencyRuns:  &atomic.Uint64{},
		VLogBlkDiscardErrors: &atomic.Uint64{},

		ScrubRecords:    &atomic.Uint64{},
		ScrubCorruption: &atomic.Uint64{},
		ScrubBytes:      &atomic.Uint64{},
		ScrubReadErrors: &atomic.Uint64{},

		AtomicOps:    &atomic.Uint64{},
		AuditDropped: &atomic.Uint64{},

		Admission: &AdmissionControl{},

		ActiveConnections:  &atomic.Int64{},
		NetworkBytesIn:     &atomic.Uint64{},
		NetworkBytesOut:    &atomic.Uint64{},
		MultiPutBatches:    &atomic.Uint64{},
		MultiPutEntries:    &atomic.Uint64{},
		MultiGetBatches:    &atomic.Uint64{},
		MultiGetEntries:    &atomic.Uint64{},

		ObserveWriteLatency:  func(float64) {},
		ObserveReadLatency:   func(float64) {},
		ObserveDeleteLatency: func(float64) {},
	}
}

// ── Storage configuration ─────────────────────────────────────────────────────

type StorageConfig struct {
	// Memory / index
	MaxMemorySizeMB    uint64
	NumShards          int // index shard count; must be power of 2 (default 256)
	WALBufferSizeMB    uint32

	// SSD / storage
	//
	// DataDirPaths: one directory per NVMe disk.  When set, shards are
	// distributed across disks via round-robin (shard % numDisks) and each
	// disk gets its own independent compaction goroutine and segment writer.
	// The WAL is placed on DataDirPaths[0].
	//
	// DataDirPath: single-disk fallback used when DataDirPaths is empty.
	DataDirPaths          []string // multi-disk: one entry per NVMe device
	DataDirPath           string   // single-disk fallback

	// RawVLogDevices: optional list of raw block devices (e.g. /dev/nvme0n1)
	// where the VLog is stored directly, bypassing the filesystem entirely.
	// Each entry pairs index-by-index with DataDirPaths; the matching
	// DataDirPaths[i] still hosts the (small) WAL, segment files, and the
	// punch-watermark file.  When set, len(RawVLogDevices) must equal
	// len(DataDirPaths).  Linux-only — non-Linux builds reject this at startup.
	//
	// On-device layout (raw mode):
	//   bytes [0,           4096)  RawSuperblock (magic, version, vlog start)
	//   bytes [4096, deviceSize)   VLog records (sector-aligned, append-only)
	//
	// GC reclaim uses BLKDISCARD ioctl (NVMe TRIM) instead of fallocate
	// PUNCH_HOLE, freeing flash erase blocks rather than just FS extents.
	// Saves ~25 µs P99 read tail and ~70 µs P99 write tail vs XFS+O_DIRECT.
	RawVLogDevices []string
	SSTableBlockSizeMB    uint32
	SSTableMaxSizeMB      uint64
	BloomFilterBitsPerKey uint16
	KeyValueSeparation    bool

	// Per-shard Bloom filter for negative-lookup acceleration.
	//
	// When enabled, every Get checks the per-shard bloom BEFORE the index map
	// lookup. A definitive miss returns "not found" without touching the shard
	// lock, saving ~500 ns per negative read at 1 M entries/shard.
	//
	// BloomFilterShardBits: bits per shard (rounded up to next power of 2).
	// Default 0 = disabled. 1<<22 (4 M bits = 512 KB/shard, ~512 MB total)
	// gives ~1% FP rate at 400 K keys/shard.
	//
	// BloomFilterHashes: probe count k. 0 = use 7 (optimal for ~10 bits/key).
	BloomFilterShardBits uint64
	BloomFilterHashes    uint8

	// Ordered key index for RangeScan / ScanCursor (ordered_index.go).
	//
	// Enabled by default (zero value): every live key is mirrored into a
	// concurrent keys-only skiplist (~64 B/key) so ordered range scans run
	// in O(log N + limit) instead of an O(N) walk over all shards.  Set
	// true to reclaim the memory on point-lookup-only deployments;
	// RangeScan / ScanCursor then return ErrOrderedIndexDisabled.
	DisableOrderedIndex bool

	// Background scrubber for silent-corruption detection.
	//
	// When ScrubEnabled is true, one goroutine per VLog walks every record
	// at ScrubMBPerSec and re-validates the CRC32C. Mismatches are logged
	// and counted but not auto-repaired (replica resync is a higher-level
	// concern). Honors AdmissionControl.GCPaused so reads are never blocked.
	//
	// Default: enabled at 50 MB/s per disk → ~2 hour full pass on 375 GB SSD.
	ScrubEnabled  bool
	ScrubMBPerSec int

	// At-rest encryption (AES-256-GCM).
	//
	// EncryptionEnabled: master toggle. When true the engine looks up the key
	// from VELTRIXDB_ENCRYPTION_KEY (base64) first; if absent it falls back to
	// EncryptionKeyPath. Startup fails if the key is missing or invalid.
	// Existing unencrypted records remain readable (FlagEncrypted is per-entry).
	EncryptionEnabled bool
	EncryptionKeyPath string

	// Audit log: append-only JSONL of mutating operations.  When AuditLogPath
	// is empty, auditing is disabled.  Designed for compliance frameworks that
	// require tamper-evident records separate from the data plane.
	AuditLogPath        string
	AuditChannelDepth   int           // 0 → 8192 default
	AuditSyncEvery      time.Duration // 0 → 1 s default

	// Compaction
	CompactionThreads  uint16
	CompactionInterval time.Duration
	MaxCompactionLevel uint8
	WriteAmplification float64

	// LIRS cache
	CacheMaxSizeMB  uint32
	LIRRatio        float64 // fraction of cache reserved for LIR set (default 0.95)
	TTLCheckInterval time.Duration

	// GC / defragmentation
	GCGracePeriodSec int64         // tombstone age before physical deletion (default 86400)
	DefragInterval   time.Duration // how often defragmenter scans (default 60s)
	DefragThreshold  float64       // VLog dead-space ratio to trigger GC (default 0.30)

	// Replication
	ReplicaBatchSize       uint32
	ReplicaFlushIntervalMs int32

	// Compression
	Compression      string // "none", "snappy", "zstd"
	CompressionLevel int

	// WAL + VLog group-commit flush windows.
	//
	// WALFlushWindowMs / VLogFlushWindowMs: how long (ms) each flusher waits
	// after the first entry arrives before issuing fdatasync.  Every writer that
	// enqueues during the window shares one fdatasync.
	//
	// batch_size ≈ (writes/s per disk) × window_s
	// For batch_size > 100 at 10 K writes/s/disk: window must be ≥ 10 ms.
	//
	// Effect on P99 write latency (KV-separation path, concurrent WAL+VLog):
	//   P99 ≈ max(WALWindow, VLogWindow) + fdatasync_cost
	//   Linux NVMe (fdatasync ≈ 0.2 ms): 10 ms window → ~10.2 ms P99
	//   macOS    (F_FULLFSYNC ≈ 9 ms):   10 ms window → ~19 ms P99
	//
	// Set to 0 for immediate flush (one fdatasync per burst, low latency at
	// the cost of very small batch sizes under moderate concurrency).
	//
	// WALMaxBatchEntries: safety cap; if pending entries reach this count
	// before the window expires the flusher fires early regardless of the timer.
	WALFlushWindowMs   int // default 10 ms; 0 = immediate
	VLogFlushWindowMs  int // default 10 ms; 0 = immediate (must match WALFlushWindowMs)
	WALMaxBatchEntries int // default 1024

	// Write stall / back-pressure
	//
	// DirtyFlushThreshold: dirty unique keys before a compaction is triggered.
	// Raising this from 10K to 500K gives the memtable room to absorb write
	// bursts without triggering O(256-shard-scan) compactions every few hundred
	// writes (which was the primary source of write stall at high concurrency).
	//
	// BackPressureThreshold: if dirtyCount exceeds this ceiling, Put() sleeps
	// BackPressureSleepMs before appending to the WAL — ScyllaDB-style soft
	// stall that prevents unbounded memtable growth under sustained overload.
	DirtyFlushThreshold  int // default 5_000_000
	BackPressureThreshold int // default 15_000_000
	BackPressureSleepMs  int // default 1

	// Continuous WAL archiving for point-in-time recovery (pitr.go).
	//
	// ArchiveDir: root directory for archived WAL segments (one disk<N>/
	// subdirectory per data dir). Empty = archiving disabled. The archiver is
	// attached to a running engine via StartWALArchiver(se); it copies newly
	// durable WAL entries at fdatasync boundaries into self-contained segment
	// files, each with a JSON metadata sidecar (version + wall-clock bounds,
	// disk index, CRC32C). restore-pitr replays these segments on top of a
	// full backup up to an exact version or timestamp.
	//
	// ArchiveIntervalMs: how often the background archiver goroutine polls
	// each WAL for newly durable bytes. 0 → 1000 ms default. Archiving never
	// blocks the group-commit hot path — it reads the WAL file through an
	// independent read-only handle.
	//
	// Retention pruning (oldest segments first, after every archive pass):
	// MaxArchiveAgeSec: prune segments created more than this many seconds
	// ago (0 = keep forever). MaxArchiveBytes: prune oldest segments while
	// the total archive size exceeds this (0 = unbounded).
	ArchiveDir        string
	ArchiveIntervalMs int
	MaxArchiveAgeSec  int64
	MaxArchiveBytes   int64
}

func DefaultStorageConfig() *StorageConfig {
	return &StorageConfig{
	

		// --- Memory / Index Tuning ---
        MaxMemorySizeMB:       65536,         // 64GB Memtable (Writing 1B keys absorbs better)
        NumShards:             1024,          // High granularity for 64 cores
        WALBufferSizeMB:       512,           // Increased for parallel writes across 8 disks

        // --- SSD / Storage Tuning (8x NVMe RAID-0 logic) ---
        // DataDirPaths should be used for 8 disks as discussed earlier
        DataDirPath:          "/var/lib/veltrixdb/data",
        SSTableBlockSizeMB:    1,             // Small blocks = faster random access for 1B keys
        SSTableMaxSizeMB:      1024,          // Increased to 1GB to keep file count low for 1B keys
        BloomFilterBitsPerKey: 14,            // Increased from 10 to 14 to reduce false positives at 1B scale
        KeyValueSeparation:    true,          // Enable this for better compaction efficiency

        // Per-shard Bloom filter for negative-lookup acceleration.
        // 4 M bits/shard × 1024 shards = 512 MB. ~1% FP rate at 400 K keys/shard.
        // Disable on memory-constrained nodes by setting BloomFilterShardBits=0.
        BloomFilterShardBits:  1 << 22,
        BloomFilterHashes:     7,

        // Background scrubber: 50 MB/s per disk, ~2 hour full pass on 375 GB.
        ScrubEnabled:          true,
        ScrubMBPerSec:         50,

        // --- Compaction (The "Stall" Killer) ---
        CompactionThreads:     16,            // Using 25% of your 64 cores
        CompactionInterval:    5 * time.Second, // More frequent checks for 1B scale
        MaxCompactionLevel:    7,             // Increased for deeper LSM tree
        WriteAmplification:    8.0,           // Tighter control

        // --- LIRS Cache (The 256GB Shield) ---
        CacheMaxSizeMB:        262144,        // 256GB - 80% RAM rule
        LIRRatio:              0.90,          // 90% LIR - Keep most keys immortal in RAM
        TTLCheckInterval:      30 * time.Second, // Less frequent to save CPU cycles

        // --- GC / Defragmentation ---
        GCGracePeriodSec:      86400,
        DefragInterval:        120 * time.Second, // Relaxed for high-throughput
        DefragThreshold:       0.30,          // GC when 30% of VLog is dead — fires earlier with smaller passes vs old 50% bursts

        // --- WAL + VLog Group-Commit Flush Windows ---
        // 10 ms windows give batch_size ≈ (writes/s/disk) × 0.010.
        // At 10 K writes/s/disk: batch_size ≈ 100 — crosses the target.
        // With concurrent WAL+VLog (Put() submits both then waits for both),
        // P99 ≈ max(WALWindow, VLogWindow) + fdatasync ≈ 10.2 ms on NVMe.
        WALFlushWindowMs:      15,            // 15 ms group-commit window; matches WriteBatcher batchFlushDur for ~200 entries/batch
        VLogFlushWindowMs:     15,            // must match WALFlushWindowMs (Invariant 20)
        WALMaxBatchEntries:    4096,          // force early flush at 4096 entries; headroom for 100K+/s bursts

        // --- Write Stall / Back-pressure ---
        // At 1B keys, these must be very aggressive
        DirtyFlushThreshold:   10_000_000,    // 10M keys before flush (Absorb bigger bursts)
        BackPressureThreshold: 30_000_000,    // Stall only when really needed
        BackPressureSleepMs:   1,             // Micro-stalls to prevent OOM

        // --- Compression ---
        Compression:           "zstd",
        CompressionLevel:      1,
	}
}

// ReadHeavyConfig returns a StorageConfig tuned for read-dominant workloads
// targeting P99 < 5 ms at 2 M+ ops/sec on n2-highmem-64 (64 vCPU, 512 GB RAM,
// 8 × 375 GB NVMe). Differences vs DefaultStorageConfig:
//
//   - CacheMaxSizeMB=409600 (400 GB ≈ 78% of RAM) — pushes hit rate >95% for
//     working sets up to ~1.5 B small keys; cache hits are RAM-only ≈ 200 ns.
//   - LIRRatio=0.95 — bigger LIR (immortal hot) set, fewer evictions of hot keys.
//   - TTLCheckInterval=60 s — halves shard-lock pressure from the TTL scanner;
//     read-heavy workloads tolerate up to 60 s lag on TTL-expired key cleanup.
//   - DefragInterval=300 s — slower GC cadence; under pure-read load garbage
//     accumulates slowly so 5-min passes are sufficient.
//   - DefragThreshold=0.20 — start GC earlier with smaller passes; smaller
//     passes mean less foreground I/O contention if writes do arrive.
//   - WAL/VLog flush windows raised to 20 ms — at low write rate this batches
//     tighter, freeing CPU for reads. Both must remain equal (Invariant 20).
//   - DirtyFlushThreshold/BackPressureThreshold lowered — defaults assume
//     write-heavy bursting; for read-heavy a smaller dirty bound recovers
//     RAM faster for cache.
//
// Pair with hardware tuning: vm.nr_hugepages ≥ 2048, mlockall enabled,
// `--local-ssd-interface=NVME` GKE node pool, `--cgo-batch-engine=true`,
// `--numa-aware=true`, `--sqpoll-reader=true`.
func ReadHeavyConfig() *StorageConfig {
	c := DefaultStorageConfig()

	// --- Memory / Cache ---
	c.CacheMaxSizeMB = 409600        // 400 GB — leave 112 GB for index, OS, network buffers
	c.LIRRatio = 0.95                // 95% LIR — protect hot keys harder
	c.TTLCheckInterval = 60 * time.Second

	// --- GC ---
	c.DefragInterval = 300 * time.Second
	c.DefragThreshold = 0.20

	// --- Write side (still tuned for occasional writes) ---
	c.WALFlushWindowMs = 20
	c.VLogFlushWindowMs = 20
	c.DirtyFlushThreshold = 5_000_000
	c.BackPressureThreshold = 15_000_000

	// --- Compression ---
	// Zstd level 1 is already minimal; keep it — decompression is cheap and
	// reduces VLog size, helping cache hit rate.

	return c
}

// ── CacheStats ────────────────────────────────────────────────────────────────

type CacheStats struct {
	Hits             uint64
	Misses           uint64
	Evictions        uint64
	CurrentSizeBytes uint64
	MaxSizeBytes     uint64
	HitRate          float64
}

// ── VersionVector ─────────────────────────────────────────────────────────────

type VersionVector struct {
	mu    sync.RWMutex
	Clock map[string]uint64
}

func (vv *VersionVector) UpdateVersion(nodeID string) {
	vv.mu.Lock()
	defer vv.mu.Unlock()
	vv.Clock[nodeID]++
}

func (vv *VersionVector) HappenedBefore(other *VersionVector) bool {
	vv.mu.RLock()
	other.mu.RLock()
	defer vv.mu.RUnlock()
	defer other.mu.RUnlock()

	atLeastOneLess := false
	for node, ts := range vv.Clock {
		otherTs := other.Clock[node]
		if ts > otherTs {
			return false
		}
		if ts < otherTs {
			atLeastOneLess = true
		}
	}
	return atLeastOneLess
}
