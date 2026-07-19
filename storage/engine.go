package storage

import (
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VeltrixDB/veltrixdb/tracing"
)

// crc32cTable is the Castagnoli CRC32C polynomial table — faster hardware
// acceleration than the IEEE table on modern CPUs, and what NVMe uses internally.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// walEntryPool reuses WALEntry objects across Put and Delete calls.
// Safe because wal.beginAppend() calls serialize() — which reads all fields into
// a byte slice — before returning.  The entry pointer is not retained by the
// flusher goroutine, so the pool can reclaim it immediately after beginAppend().
var walEntryPool = &sync.Pool{New: func() any { return &WALEntry{} }}

func computeCRC32C(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

// ── Storage engine ────────────────────────────────────────────────────────────

// StorageEngine is the core single-node storage layer.  Its design follows
// three principles from the spec:
//
//  1. Index Vault — permanent, RAM-resident shardedIndex; never evicted.
//  2. LIRS Data Cache — scan-resistant; eviction applies only here.
//  3. Log-structured WAL + segment files — sequential writes; fdatasync.
//
// Multi-disk operation:
//
//	When StorageConfig.DataDirPaths has N entries, the engine creates N
//	SegmentWriters (one per disk) and N compaction goroutines.  Shards are
//	spread across disks via shard_id % N so every disk handles 256/N shards.
//	All disk I/O is therefore parallel — no disk is a bottleneck for another.
type StorageEngine struct {
	config   *StorageConfig
	index    *shardedIndex
	wals     []*WriteAheadLog  // one per disk; wals[i] lives on dirs[i]
	cache    Cache
	segments []*SegmentWriter // one per disk; len == numDisks
	vlogs    []*VLog          // one per disk; non-nil when KeyValueSeparation=true
	defrag   *Defragmenter
	metrics  *StorageMetrics
	done     chan struct{}
	version  atomic.Uint64

	// Per-disk compaction queues: compactionQueues[diskIdx] feeds the
	// compaction goroutine that owns diskIdx.
	compactionQueues []chan CompactionRequest
	sst              *SSTManager // retained for metadata; queues moved above

	// batcher collects individual Put calls into vectorized batches and
	// flushes them asynchronously (fire-and-forget with WAL persistence).
	batcher *WriteBatcher

	// cgoBatch is the C++ shard-parallel batch engine (linux+cgo only).
	// Nil on macOS dev builds and when CGO_ENABLED=0.
	cgoBatch *cgoBatchEngine

	// storageBridge is the io_uring 8-ring write bridge used by VLogBatcher.
	// Routes N VLog writes per batch into a single io_uring_submit call.
	// Nil on macOS dev builds and when CGO_ENABLED=0.
	storageBridge *cgoStorageBridge

	// replayDone is closed when background WAL index rebuild finishes.
	// Callers that need a fully-warmed index (e.g. a readiness probe) may
	// select on this channel; normal Get/Put need not wait.
	ReplayDone <-chan struct{}

	// scrubPassCounter tracks how many full scrub passes have completed —
	// used by the admin /scrubstatus endpoint and integration tests.
	scrubPassCounter uint64

	// audit is the engine-wide audit logger.  Always non-nil; a no-op when
	// cfg.AuditLogPath is empty.
	audit *AuditLog

	// cdc broadcasts every successful mutation to streaming subscribers.
	// Always non-nil; subscribers are added via Subscribe().
	cdc *CDCBroker

	// quotas enforces per-namespace token-bucket rate limits and key-count
	// quotas.  Always non-nil; permissive when no quotas registered.
	quotas *QuotaManager

	// tombstones tracks per-replica acknowledgement watermarks so the GC
	// reaper does not drop tombstones a slow replica has not yet seen.
	// Always non-nil; behaves as single-node when no replicas register.
	tombstones *TombstoneCoordinator

	// fieldIdxMu guards fieldIdxDefs: the named secondary-index definitions
	// created via CreateFieldIndex (IDXCREATE) and persisted to
	// <dataDir>/index_defs.json. See field_index.go.
	fieldIdxMu   sync.Mutex
	fieldIdxDefs []FieldIndexDef

	// diskHealth holds one consecutive-error breaker per disk; a tripped
	// breaker fails writes fast and reports the node degraded (disk_health.go).
	diskHealth []diskHealth
}

// SSTManager manages SSTable metadata and the shared done channel.
type SSTManager struct {
	mu      sync.RWMutex
	tables  []SSTableMetadata
	done    chan struct{}
	metrics *StorageMetrics
}

// NewStorageEngine creates and starts a storage engine.
//
// Multi-disk: set cfg.DataDirPaths = []string{"/mnt/nvme0", …, "/mnt/nvme7"}.
// Single-disk: set cfg.DataDirPath (DataDirPaths takes precedence when set).
func NewStorageEngine(cfg *StorageConfig) (*StorageEngine, error) {
	if cfg == nil {
		cfg = DefaultStorageConfig()
	}
	if cfg.NumShards == 0 {
		cfg.NumShards = 256
	}

	// Resolve disk paths: DataDirPaths wins; fall back to single DataDirPath.
	dirs := cfg.DataDirPaths
	if len(dirs) == 0 {
		if cfg.DataDirPath == "" {
			cfg.DataDirPath = DefaultStorageConfig().DataDirPath
		}
		dirs = []string{cfg.DataDirPath}
	}

	// Validate raw VLog device list: when set it must pair 1:1 with dirs.
	rawDevices := cfg.RawVLogDevices
	if len(rawDevices) > 0 {
		if len(rawDevices) != len(dirs) {
			return nil, fmt.Errorf(
				"RawVLogDevices length (%d) must equal DataDirPaths length (%d) — pair index by index",
				len(rawDevices), len(dirs))
		}
		for i, dev := range rawDevices {
			if dev != "" && !IsBlockDevice(dev) {
				return nil, fmt.Errorf(
					"RawVLogDevices[%d]=%q is not under /dev/ — raw mode requires a block-device node", i, dev)
			}
		}
	}

	lirRatio := cfg.LIRRatio
	if lirRatio <= 0 {
		lirRatio = 0.95
	}
	// At-rest encryption: load the key once at startup so the hot path can
	// read globalEncryptor with a single RLock.  When disabled, getEncryptor()
	// returns nil and Encrypt/Decrypt become no-ops (still safe to call).
	if cfg.EncryptionEnabled {
		enc, err := loadEncryptor(cfg.EncryptionKeyPath)
		if err != nil {
			return nil, fmt.Errorf("encryption setup: %w", err)
		}
		if enc == nil {
			return nil, fmt.Errorf("encryption: enabled but no key in %s and no EncryptionKeyPath set",
				EncryptionKeyEnvVar)
		}
		setEncryptor(enc)
		log.Printf("[encryption] AES-256-GCM at-rest encryption enabled")
	}

	// Compression: install the write-path algorithm once at startup. Unknown
	// Compression values fail here instead of silently writing flate.
	if err := SetCompressionAlgo(cfg.Compression); err != nil {
		return nil, err
	}

	cache := NewLIRSCache(cfg.CacheMaxSizeMB, lirRatio)
	index := newShardedIndex()
	if cfg.DisableOrderedIndex {
		// Drop the ordered key view: the shard-lock hooks become no-ops and
		// RangeScan / ScanCursor return ErrOrderedIndexDisabled.
		index.ordered = nil
	}
	if cfg.BloomFilterShardBits > 0 {
		k := cfg.BloomFilterHashes
		if k == 0 {
			k = 7
		}
		index.installBlooms(cfg.BloomFilterShardBits, k)
		log.Printf("[bloom] per-shard bloom enabled  bits/shard=%d  k=%d  total≈%dMB",
			cfg.BloomFilterShardBits, k, (cfg.BloomFilterShardBits/8*numShards)>>20)
	}
	metrics := newStorageMetrics()

	// Emergency pre-punch: apply saved GC punch watermarks before creating
	// WAL/segment files.  If the disks were ~100% full when the previous
	// session ended, the WAL open below would fail with ENOSPC.  Punching
	// the dead VLog head here frees the blocks that GC already relocated in
	// the previous session, without any data loss.  Best-effort: errors are
	// silently ignored (no VLog yet, unsupported FS, etc.).
	for i, dir := range dirs {
		raw := ""
		if i < len(rawDevices) {
			raw = rawDevices[i]
		}
		applyVLogEmergencyPunch(i, dir, raw)
	}

	// One WAL per disk — parallel fdatasyncs, no shared serialisation point.
	flushWindow := time.Duration(cfg.WALFlushWindowMs) * time.Millisecond
	maxBatch := cfg.WALMaxBatchEntries

	wals := make([]*WriteAheadLog, len(dirs))
	for i, dir := range dirs {
		w, err := newWriteAheadLog(dir, metrics.WALFlushes, flushWindow, maxBatch, i)
		if err != nil {
			for j := 0; j < i; j++ {
				wals[j].close()
			}
			return nil, fmt.Errorf("disk %d WAL: %w", i, err)
		}
		wals[i] = w
	}

	// Create one SegmentWriter per disk.
	segments := make([]*SegmentWriter, len(dirs))
	for i, dir := range dirs {
		sw, err := newSegmentWriter(i, dir)
		if err != nil {
			// Close already-opened writers before returning.
			for j := 0; j < i; j++ {
				segments[j].close()
			}
			for _, w := range wals {
				if w != nil {
					w.close()
				}
			}
			return nil, fmt.Errorf("disk %d segment: %w", i, err)
		}
		segments[i] = sw
	}

	// Create one VLog per disk when Key-Value Separation is enabled.
	// Each VLog receives only value bytes; the Index Vault stores the pointer
	// (DiskOffset = VLog offset, SegmentID = disk index, ValueSize = len(value)).
	var vlogs []*VLog
	if cfg.KeyValueSeparation {
		vlogs = make([]*VLog, len(dirs))
		for i, dir := range dirs {
			vlogWindow := time.Duration(cfg.VLogFlushWindowMs) * time.Millisecond
			raw := ""
			if i < len(rawDevices) {
				raw = rawDevices[i]
			}
			vl, err := newVLog(i, dir, raw, vlogWindow)
			if err != nil {
				for j := 0; j < i; j++ {
					vlogs[j].close()
				}
				for _, sw := range segments {
					if sw != nil {
						sw.close()
					}
				}
				for _, w := range wals {
					if w != nil {
						w.close()
					}
				}
				return nil, fmt.Errorf("disk %d vlog: %w", i, err)
			}
			vlogs[i] = vl
		}
	}

	// ── WAL replay ───────────────────────────────────────────────────────────
	// Phase 1 (parallel, sync): read all WAL files from all disks concurrently.
	// Each disk has its own NVMe queue so 8 reads complete in ~1× latency, not 8×.
	// replayWAL only reads the file; no VLog I/O happens here.
	allEntries := make([][]walReplayEntry, len(dirs))
	{
		var rg sync.WaitGroup
		for i, dir := range dirs {
			rg.Add(1)
			go func(idx int, d string) {
				defer rg.Done()
				ents, _ := replayWAL(walPathForDir(d))
				allEntries[idx] = ents
			}(i, dir)
		}
		rg.Wait()
	}

	// Phase 2 (fast, in-memory): scan parsed entries to find the highest version
	// before constructing se so se.version is correct before any live Put arrives.
	var maxReplayVersion uint64
	for _, ents := range allEntries {
		for _, e := range ents {
			if e.version > maxReplayVersion {
				maxReplayVersion = e.version
			}
		}
	}

	// Create the io_uring storage bridge and wire it into every VLog.
	// The bridge owns 8 SQPOLL rings (one per NVMe disk) and fixed-buffer
	// pools registered once with the kernel.  VLogBatcher.Flush will route
	// writes through the bridge (1 io_uring_submit per batch) instead of N
	// separate pwrite syscalls.  nil on macOS dev / CGO_ENABLED=0.
	var storageBridge *cgoStorageBridge
	if cfg.KeyValueSeparation && len(vlogs) > 0 {
		storageBridge = newCGOStorageBridge(len(vlogs), true /*sqPoll*/)
		if storageBridge != nil {
			for _, vl := range vlogs {
				vl.SetStorageBridge(storageBridge)
			}
		} else {
		}
	}

	// Create one compaction queue per disk.
	compactionQueues := make([]chan CompactionRequest, len(dirs))
	for i := range compactionQueues {
		compactionQueues[i] = make(chan CompactionRequest, 256)
	}

	sst := &SSTManager{
		tables:  make([]SSTableMetadata, 0),
		done:    make(chan struct{}),
		metrics: metrics,
	}

	defrag := newDefragmenter(index, wals[0], cfg, metrics, cache, vlogs, len(dirs))

	// C++ batch engine: one OS thread per logical CPU, capped at 256 (numShards).
	// newCGOBatchEngine returns nil on macOS / CGO_ENABLED=0 (stub build tag).
	numCGOThreads := runtime.NumCPU()
	if numCGOThreads > numShards {
		numCGOThreads = numShards
	}
	cgoBatch := newCGOBatchEngine(numCGOThreads)

	se := &StorageEngine{
		config:           cfg,
		index:            index,
		wals:             wals,
		cache:            cache,
		segments:         segments,
		vlogs:            vlogs,
		defrag:           defrag,
		metrics:          metrics,
		done:             make(chan struct{}),
		compactionQueues: compactionQueues,
		sst:              sst,
		cgoBatch:         cgoBatch,
		storageBridge:    storageBridge,
	}
	se.initDiskHealth(len(wals))
	defrag.diskFailed = se.diskIsFailed
	// Restore the global version counter so new writes start strictly above any
	// version that appears in the replayed WAL entries.
	if maxReplayVersion > 0 {
		se.version.Store(maxReplayVersion)
	}

	// Phase 3 (background, async): apply WAL entries to the in-memory index.
	// All VLog and WAL flushers are already running (started inside newVLog /
	// newWriteAheadLog above).  applyWALReplay uses replayPut /
	// replayMarkTombstone so live Puts arriving during warmup always win over
	// stale replayed data.  Server is immediately available to accept traffic.
	replayDone := make(chan struct{})
	se.ReplayDone = replayDone
	hasEntries := false
	for _, ents := range allEntries {
		if len(ents) > 0 {
			hasEntries = true
			break
		}
	}
	if !hasEntries {
		close(replayDone) // nothing to replay — index is immediately fully warm
	} else {
		go func() {
			var rwg sync.WaitGroup
			for i := range dirs {
				if len(allEntries[i]) == 0 {
					continue
				}
				rwg.Add(1)
				entries := allEntries[i]
				diskIdx := i
				var vl *VLog
				if diskIdx < len(vlogs) {
					vl = vlogs[diskIdx]
				}
				go func() {
					defer rwg.Done()
					applyWALReplay(entries, index, vl, diskIdx, len(dirs), cfg.KeyValueSeparation)
				}()
			}
			rwg.Wait()

			// Seed each VLog's vl.end past the highest live record found in the
			// rebuilt index. Critical for raw block-device mode (Stat().Size()=0
			// means newVLog couldn't infer where prior writes ended). Harmless
			// on file mode because vl.end is already at the file size, which
			// is ≥ any index entry's offset, and SetEndAtLeast never retreats.
			//
			// Without this, the first Put after a restart in raw mode reserves
			// vl.end.Add(...) starting at offset 4096 and overwrites existing
			// live VLog records that the index correctly references.
			if cfg.KeyValueSeparation && len(vlogs) > 0 {
				for diskIdx, vl := range vlogs {
					maxEnd := index.maxVLogEndOffset(diskIdx, len(dirs))
					if maxEnd == 0 {
						continue
					}
					before := vl.end.Load()
					vl.SetEndAtLeast(int64(maxEnd))
					after := vl.end.Load()
					if after > before {
						mode := "file"
						if vl.rawDevicePath != "" {
							mode = "raw"
						}
						log.Printf("[vlog] disk=%d %s mode end seeded to %d from index after WAL replay (was %d)",
							diskIdx, mode, after, before)
					}
				}
			}

			close(replayDone)
		}()
	}

	// Audit log: opens path if configured, no-op otherwise.  AuditDropped
	// counter is wired so on-channel-full drops are visible in metrics.
	auditLog, audErr := NewAuditLog(cfg.AuditLogPath, cfg.AuditChannelDepth, cfg.AuditSyncEvery, metrics.AuditDropped)
	if audErr != nil {
		log.Printf("[audit] init failed: %v (continuing without audit)", audErr)
		auditLog = &AuditLog{} // permissive no-op
	}
	se.audit = auditLog
	se.cdc = NewCDCBroker()
	se.quotas = NewQuotaManager()
	se.tombstones = NewTombstoneCoordinator()

	// Start the async write batcher after se is fully initialised so the
	// batcher goroutine can safely call se.MultiPut and se.cgoBatch.
	se.batcher = newWriteBatcher(se)

	go se.backgroundTTLScanner()

	// One compaction goroutine per disk — they run in parallel, never block each other.
	for diskIdx := range segments {
		go se.backgroundCompactionWorkerForDisk(diskIdx)
	}

	defrag.start()

	// Start the per-disk corruption scrubber. No-op when ScrubEnabled=false
	// or when KV-separation is off. Scrubber respects AdmissionControl.GCPaused
	// so reads never compete with scrubbing.
	se.startScrubbers()

	// Re-register persisted secondary-index definitions (index_defs.json) so
	// writes arriving after a restart keep maintaining their "@idx/..."
	// entries. The entries themselves are ordinary durable keys and need no
	// rebuild. Cheap: one small JSON read; no-op when the file is absent.
	if err := se.loadFieldIndexDefs(); err != nil {
		log.Printf("[index] loading persisted index definitions failed: %v (continuing without)", err)
	}

	return se, nil
}

// ── Write path ────────────────────────────────────────────────────────────────

// Put stores key → value with an optional TTL (seconds; -1 = immortal).
//
// Standard write path:
//  1. Append to WAL and fdatasync (crash durability).
//  2. Build IndexEntry and insert into the owning shard of the Index Vault.
//  3. Insert value into LIRS Data Cache as a dirty (pre-flush) value.
//  4. Optionally trigger per-disk compaction if dirty backlog is large.
//
// KV-Separation write path (KeyValueSeparation=true):
//  1. Append to WAL (crash durability for the key+value).
//  2. Append value ONLY to the per-disk VLog → get back VLogOffset.
//  3. Build IndexEntry with DiskOffset=VLogOffset (ValuePointer into VLog).
//  4. Insert IndexEntry with nil dirtyValue — the value is in VLog, not RAM.
//  5. Put value into LIRS cache for hot-path reads.
//
// With KV separation there is no "dirty flush to segment" step; compaction
// only needs to GC the VLog, not merge key-value records.
func (se *StorageEngine) Put(key string, value []byte, ttl int32) error {
	_, span := tracing.Start(context.Background(), "engine.Put")
	defer span.End()
	span.SetAttribute("key.size", len(key))
	span.SetAttribute("value.size", len(value))

	se.applyBackPressure()

	// Admission control: if read P99 EWMA > 20ms, delay each Put by 2ms.
	// The sleep is BEFORE WAL submission so it delays entering the flush window,
	// reducing effective submission rate rather than just adding tail latency.
	if se.metrics.Admission.WriteThrottleActive.Load() {
		se.metrics.Admission.WriteThrottleEvents.Add(1)
		time.Sleep(2 * time.Millisecond)
	}

	// GC CAS backpressure: when GC's batch flush is losing >20% of CAS operations
	// to concurrent Puts, add a brief 1ms delay to give GC a quieter commit
	// window.  Separate from — and lighter than — the read-latency throttle.
	if until := se.metrics.Admission.GCBackpressureUntilNs.Load(); until > 0 {
		if time.Now().UnixNano() < until {
			time.Sleep(1 * time.Millisecond)
			se.metrics.Admission.GCCASThrottles.Add(1)
		} else {
			se.metrics.Admission.GCBackpressureUntilNs.Store(0) // expired — clear
		}
	}

	// Secondary-index maintenance (near-zero cost when no indexes are defined:
	// one atomic load). The old value must be read BEFORE the write so the
	// diff-apply after commit can remove index entries the update obsoletes.
	var oldIdxVal []byte
	maintainIdx := indexRulesActive() && !isInternalIndexKey(key)
	if maintainIdx {
		oldIdxVal, _ = se.Get(key) // nil when the key is absent
	}

	start := time.Now()
	nowUs := start.UnixMicro()
	version := se.version.Add(1)
	crc := computeCRC32C(value)

	shardID := uint16(fnv64a(key) & (numShards - 1))
	diskIdx := diskForShard(shardID, len(se.wals))
	if se.diskIsFailed(diskIdx) {
		return degradedError(diskIdx)
	}

	walEntry := walEntryPool.Get().(*WALEntry)
	walEntry.Timestamp = start.UnixNano()
	walEntry.KeyLen = uint32(len(key))
	walEntry.Key = key
	walEntry.ValueLen = uint32(len(value))
	walEntry.Value = value
	walEntry.Checksum = crc
	walEntry.Version = version
	walEntry.IsTombstone = false
	walEntry.ReplicationID = 0

	entry := &IndexEntry{
		KeyHash:          fnv64a(key),
		DiskOffset:       0,
		SegmentID:        0,
		ValueSize:        uint32(len(value)),
		UncompressedSize: uint32(len(value)),
		KeySize:          uint32(len(key)),
		WriteTimestampUs: nowUs,
		CRC32C:           crc,
		SchemaVersion:    CurrentSchemaVersion,
		ShardID:          shardID,
	}
	if ttl > 0 {
		entry.Flags |= FlagHasTTL
		entry.TTLExpiryUs = nowUs + int64(ttl)*1_000_000
	}

	if se.config.KeyValueSeparation && len(se.vlogs) > 0 {
		if old, _, ok := se.index.get(key); ok && !old.IsTombstone() && old.DiskOffset > 0 {
			se.vlogs[int(old.SegmentID)%len(se.vlogs)].MarkDead(old.ValueSize, old.IsPacked())
		}

		// Compression: try flate when the value crosses the 256-byte threshold.
		// On wins we keep UncompressedSize=len(value) for the read path and store
		// FlagCompressed on the IndexEntry; ValueSize becomes the compressed
		// blob length (1B algo prefix + compressed bytes).
		writeBytes := value
		if se.config.Compression == "zstd" || se.config.Compression == "flate" {
			if cb, ok := MaybeCompress(value, se.config.CompressionLevel); ok {
				writeBytes = cb
				entry.Flags |= FlagCompressed
				entry.ValueSize = uint32(len(cb))
			}
		}
		// Encryption: applied AFTER compression (encrypted data is incompressible)
		// and only when a key is loaded. Adds 12 B nonce + 16 B AEAD tag = 28 B
		// overhead per record. ValueSize tracks the post-encryption blob length.
		if EncryptionEnabled() {
			ct, sealed, encErr := Encrypt(writeBytes)
			if encErr != nil {
				return fmt.Errorf("encrypt: %w", encErr)
			}
			if sealed {
				writeBytes = ct
				entry.Flags |= FlagEncrypted
				entry.ValueSize = uint32(len(ct))
			}
		}

		// Reserve the VLog offset atomically (non-blocking) so we can record it
		// in the WAL entry before submitting to the WAL flusher. Both fdatasyncs
		// still run concurrently — VLog.beginAppend returns the channel immediately
		// without waiting for the flusher goroutine.
		vlogOffset, vlogRp, vlogBeginErr := se.vlogs[diskIdx].beginAppend(writeBytes)
		if vlogBeginErr != nil {
			return fmt.Errorf("vlog begin append: %w", vlogBeginErr)
		}

		// Store the VLog offset in the WAL entry. serialize() will write it as
		// the 7th header field and omit the value bytes from the WAL file — the
		// value is already durable in the VLog after its fdatasync completes.
		walEntry.VLogOffset = vlogOffset
		walEntry.Value = nil // value bytes not needed in WAL when VLogOffset is set
		walRp := se.wals[diskIdx].beginAppend(walEntry)
		walEntryPool.Put(walEntry) // safe: serialize() consumed all fields above

		walErr := <-*walRp
		walRespPool.Put(walRp)
		vlogErr := <-*vlogRp
		vlogRespPool.Put(vlogRp)
		if walErr != nil {
			se.noteDiskError(diskIdx, "WAL append", walErr)
			return fmt.Errorf("WAL append: %w", walErr)
		}
		if vlogErr != nil {
			se.noteDiskError(diskIdx, "vlog fdatasync", vlogErr)
			return fmt.Errorf("vlog fdatasync: %w", vlogErr)
		}
		se.noteDiskOK(diskIdx)

		entry.DiskOffset = uint64(vlogOffset)
		entry.SegmentID = uint32(diskIdx)
		se.index.put(key, entry, nil)
		se.cache.Put(key, value)
		se.metrics.VLogWrites.Add(1)
	} else {
		err := se.wals[diskIdx].append(walEntry)
		walEntryPool.Put(walEntry) // safe: append() calls beginAppend() which serializes first
		if err != nil {
			se.noteDiskError(diskIdx, "WAL append", err)
			return fmt.Errorf("WAL append: %w", err)
		}
		se.noteDiskOK(diskIdx)
		se.index.put(key, entry, value)
		se.cache.Put(key, value)

		if se.shouldCompact() {
			se.triggerCompaction()
		}
	}

	se.metrics.Writes.Add(1)
	elapsed := time.Since(start)
	se.metrics.WritesLatencyNs.Store(elapsed.Nanoseconds())
	se.metrics.ObserveWriteLatency(elapsed.Seconds())
	se.audit.Log(AuditRecord{Op: "PUT", Key: key, Status: "ok"})
	se.cdc.Broadcast(CDCEvent{Op: "PUT", Key: key, Value: value, Timestamp: nowUs})

	// Diff-apply secondary indexes AFTER the primary write committed — the
	// contract documented in secondary_index.go. Best-effort: index write
	// failures never fail the primary Put.
	if maintainIdx {
		se.applySecondaryIndexes(key, oldIdxVal, value)
	}
	return nil
}

// ── Read path ─────────────────────────────────────────────────────────────────

// Get retrieves the value for key.
//
// Read path:
//  1. LIRS Data Cache — O(1), ~200 ns on hit.
//  2. Index Vault — check tombstone / TTL.
//  3. Dirty value still in RAM (written but not yet flushed to segment).
//  4. Segment file read via DiskOffset + SegmentID — CRC32C verified.
func (se *StorageEngine) Get(key string) ([]byte, error) {
	_, span := tracing.Start(context.Background(), "engine.Get")
	defer span.End()
	span.SetAttribute("key.size", len(key))

	start := time.Now()
	se.metrics.Reads.Add(1)
	defer func() {
		elapsed := time.Since(start)
		ns := elapsed.Nanoseconds()
		se.metrics.ReadsLatencyNs.Store(ns)
		se.metrics.ObserveReadLatency(elapsed.Seconds())
		// Stamp the last-read time so compactVLog can detect a stale EWMA
		// when the workload becomes write-only after a brief read burst.
		se.metrics.Admission.LastReadNs.Store(time.Now().UnixNano())

		// EWMA + admission-control flag updates: SAMPLED 1-in-N (default 64) so
		// the CAS-loop on a single global atomic does not pingpong across all
		// cores at multi-M-ops/sec read-heavy throughput. The lag this adds to
		// admission control is ≤ N/throughput — at 2 M ops/sec, ≤ 32 µs — far
		// shorter than the 10–20 ms admission thresholds.
		idx := se.metrics.readEWMACounter.Add(1)
		if idx&readEWMASampleMask != 0 {
			return // skip CAS update on 63/64 reads
		}

		// EWMA update α=1/8: newEWMA = old*7/8 + sample/8 (integer, no float).
		var next int64
		for {
			old := se.metrics.ReadLatencyEWMANs.Load()
			next = old - (old >> 3) + (ns >> 3)
			if se.metrics.ReadLatencyEWMANs.CompareAndSwap(old, next) {
				break
			}
		}
		// Admission control: cross 4ms EWMA → throttle writes + pause GC;
		// cross back below 2ms → resume both.  Single relaxed store is safe
		// here — the flag is only a hint (soft stall, not a mutex).
		ac := se.metrics.Admission
		if next > admissionThrottleNs {
			ac.WriteThrottleActive.Store(true)
			ac.GCPaused.Store(true)
		} else if next < admissionResumeNs {
			ac.WriteThrottleActive.Store(false)
			ac.GCPaused.Store(false)
		}
	}()

	// Step 1: LIRS cache.
	if value, hit := se.cache.Get(key); hit {
		se.metrics.CacheHits.Add(1)
		return value, nil
	}
	se.metrics.CacheMisses.Add(1)

	// Step 2: Index Vault. The shardedIndex.get() call already does a bloom
	// pre-check when blooms are installed; we count its negative hits here so
	// operators can verify the filter is paying for itself.
	entry, dirtyValue, exists := se.index.get(key)
	if !exists {
		if shardBloomEnabled.Load() {
			shard, _ := se.index.shardFor(key)
			if shard.bloom != nil && !shard.bloom.MayContain(fnv64a(key)) {
				se.metrics.BloomFilterSkipped.Add(1)
			}
		}
		return nil, fmt.Errorf("key not found: %s", key)
	}
	if entry.IsTombstone() {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	nowUs := time.Now().UnixMicro()
	if entry.IsExpired(nowUs) {
		se.index.markTombstone(key, nowUs)
		se.cache.Evict(key)
		return nil, fmt.Errorf("key expired: %s", key)
	}

	// Step 3: Dirty value in RAM (pre-flush).
	if dirtyValue != nil {
		se.cache.Put(key, dirtyValue)
		return dirtyValue, nil
	}

	// Step 4: Persistent storage read — VLog (KV-separation) or segment file.
	if entry.DiskOffset > 0 {
		if se.config.KeyValueSeparation && len(se.vlogs) > 0 {
			vlogIdx := int(entry.SegmentID) % len(se.vlogs)
			value, err := se.vlogs[vlogIdx].ReadValue(int64(entry.DiskOffset), entry.ValueSize)
			if err != nil {
				se.noteDiskError(vlogIdx, "vlog read", err)
				return nil, fmt.Errorf("vlog read: %w", err)
			}
			if entry.IsEncrypted() {
				pt, derr := Decrypt(value)
				if derr != nil {
					return nil, fmt.Errorf("vlog decrypt: %w", derr)
				}
				value = pt
			}
			if entry.IsCompressed() {
				decoded, derr := Decompress(value, entry.UncompressedSize)
				if derr != nil {
					return nil, fmt.Errorf("vlog decompress: %w", derr)
				}
				value = decoded
			}
			// Schema migration: lazily upgrade old-version values on read so
			// callers always see CurrentSchemaVersion. No-op when the entry is
			// already current.
			if entry.SchemaVersion < CurrentSchemaVersion {
				migrated, _, merr := MigrateOnRead(entry.SchemaVersion, value)
				if merr != nil {
					return nil, fmt.Errorf("schema migrate: %w", merr)
				}
				value = migrated
			}
			se.metrics.VLogReads.Add(1)
			se.cache.Put(key, value)
			return value, nil
		}
		if int(entry.SegmentID) < len(se.segments) {
			sw := se.segments[entry.SegmentID]
			value, err := sw.ReadValue(int64(entry.DiskOffset), entry.KeySize, entry.ValueSize)
			if err != nil {
				return nil, fmt.Errorf("segment read: %w", err)
			}
			if computeCRC32C(value) != entry.CRC32C {
				return nil, fmt.Errorf("CRC32C mismatch for key %s: data corruption", key)
			}
			if entry.Flags&FlagReadRepairNeeded != 0 {
				entry.Flags &^= FlagReadRepairNeeded
			}
			se.cache.Put(key, value)
			return value, nil
		}
	}

	return nil, fmt.Errorf("key not found: %s", key)
}

// ── Delete path ───────────────────────────────────────────────────────────────

// Delete writes an in-memory tombstone for key.
//
// The tombstone is NOT immediately removed from the Index Vault.  The
// Defragmenter reaps it after GCGracePeriodSec to give lagging replicas time
// to receive the delete before the tombstone disappears.
func (se *StorageEngine) Delete(key string) error {
	// Secondary-index maintenance: capture the old value before tombstoning
	// so the obsolete "@idx/..." entries can be diff-removed afterwards.
	// One atomic load when no indexes are defined.
	var oldIdxVal []byte
	maintainIdx := indexRulesActive() && !isInternalIndexKey(key)
	if maintainIdx {
		oldIdxVal, _ = se.Get(key)
	}

	start := time.Now()
	nowUs := start.UnixMicro()

	walEntry := walEntryPool.Get().(*WALEntry)
	walEntry.Timestamp = start.UnixNano()
	walEntry.KeyLen = uint32(len(key))
	walEntry.Key = key
	walEntry.IsTombstone = true
	walEntry.Version = se.version.Add(1)
	walEntry.ValueLen = 0
	walEntry.Value = nil
	walEntry.Checksum = 0
	walEntry.ReplicationID = 0
	delShardID := uint16(fnv64a(key) & (numShards - 1))
	delDiskIdx := diskForShard(delShardID, len(se.wals))
	err := se.wals[delDiskIdx].append(walEntry)
	walEntryPool.Put(walEntry)
	if err != nil {
		return fmt.Errorf("WAL append (tombstone): %w", err)
	}

	if se.config.KeyValueSeparation && len(se.vlogs) > 0 {
		if old, _, ok := se.index.get(key); ok && !old.IsTombstone() && old.DiskOffset > 0 {
			se.vlogs[int(old.SegmentID)%len(se.vlogs)].MarkDead(old.ValueSize, old.IsPacked())
		}
	}

	se.index.markTombstone(key, nowUs)
	se.cache.Evict(key)

	se.metrics.Deletes.Add(1)
	elapsed := time.Since(start)
	se.metrics.DeletesLatencyNs.Store(elapsed.Nanoseconds())
	se.metrics.ObserveDeleteLatency(elapsed.Seconds())
	se.audit.Log(AuditRecord{Op: "DEL", Key: key, Status: "ok"})
	se.cdc.Broadcast(CDCEvent{Op: "DEL", Key: key, Timestamp: nowUs})

	// Remove the deleted key's secondary-index entries (diff against nil).
	if maintainIdx && oldIdxVal != nil {
		se.applySecondaryIndexes(key, oldIdxVal, nil)
	}
	return nil
}

// ── Namespace operations ──────────────────────────────────────────────────────

// nsSep is the byte separator between a namespace and a key in the internal key
// encoding.  Null byte is safe — it never appears in normal UTF-8 key strings.
const nsSep = "\x00"

// nsKey encodes a namespaced key as "namespace\x00key" for storage.
// Using the same Put/Get/Delete methods underneath means no new WAL format,
// no new on-disk structures — namespaces are purely a key-routing convention.
func nsKey(ns, key string) string { return ns + nsSep + key }

// NSKey exposes the internal namespaced-key encoding so the distributed
// coordinator can replicate namespace writes as plain KV operations.
func NSKey(ns, key string) string { return nsKey(ns, key) }

// NSEntry is one key-value pair returned by ScanNamespace.
type NSEntry struct {
	Key   string // user-visible key (without namespace prefix)
	Value []byte
}

// NSInfo describes a namespace returned by ListNamespaces.
type NSInfo struct {
	Namespace string
	KeyCount  int
}

// PutNS stores key → value inside namespace ns.
// Internally stored as "ns\x00key" so it is completely isolated from the
// default key space and from other namespaces.
//
// Enforces per-namespace quotas: rate limit (writes/sec) and key-count cap.
// Returns ErrRateLimited or ErrQuotaExceeded when the namespace is over limit.
func (se *StorageEngine) PutNS(ns, key string, value []byte, ttl int32) error {
	internalKey := nsKey(ns, key)
	// Determine isNewKey BEFORE the write — overwrites don't count against MaxKeys.
	_, _, exists := se.index.get(internalKey)
	if err := se.quotas.CheckWrite(ns, !exists); err != nil {
		se.audit.Log(AuditRecord{Op: "PUT", Key: key, Ns: ns, Status: "err", Err: err.Error()})
		return err
	}
	if err := se.Put(internalKey, value, ttl); err != nil {
		return err
	}
	if !exists {
		se.quotas.IncKeyCount(ns, 1)
	}
	return nil
}

// GetNS retrieves the value for key inside namespace ns.
func (se *StorageEngine) GetNS(ns, key string) ([]byte, error) {
	return se.Get(nsKey(ns, key))
}

// DeleteNS removes key from namespace ns.
func (se *StorageEngine) DeleteNS(ns, key string) error {
	internalKey := nsKey(ns, key)
	_, _, exists := se.index.get(internalKey)
	if err := se.Delete(internalKey); err != nil {
		return err
	}
	if exists {
		se.quotas.IncKeyCount(ns, -1)
	}
	return nil
}

// DropNamespace deletes every key that belongs to namespace ns.
// It scans all 1024 shards in parallel (one goroutine per shard group),
// collects matching keys, then calls Delete on each.
// Returns the number of keys deleted.
func (se *StorageEngine) DropNamespace(ns string) (int, error) {
	prefix := ns + nsSep
	nowUs := time.Now().UnixMicro()
	var keys []string
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if !entry.IsTombstone() && !entry.IsExpired(nowUs) && len(k) > len(prefix) && k[:len(prefix)] == prefix {
				keys = append(keys, k)
			}
		}
		shard.mu.RUnlock()
	}
	for _, k := range keys {
		if err := se.Delete(k); err != nil {
			return 0, err
		}
	}
	return len(keys), nil
}

// ScanNamespace returns up to limit key-value pairs from namespace ns whose
// user key starts with prefix.  Pass prefix="" to scan all keys in the namespace.
// Pass limit=0 for no limit.
func (se *StorageEngine) ScanNamespace(ns, prefix string, limit int) ([]NSEntry, error) {
	internalPrefix := ns + nsSep + prefix
	nowUs := time.Now().UnixMicro()

	type candidate struct {
		internalKey string
		diskOffset  uint64
		segmentID   uint32
		valueSize   uint32
	}

	var candidates []candidate
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if entry.IsTombstone() || entry.IsExpired(nowUs) {
				continue
			}
			if len(k) < len(internalPrefix) || k[:len(internalPrefix)] != internalPrefix {
				continue
			}
			candidates = append(candidates, candidate{
				internalKey: k,
				diskOffset:  entry.DiskOffset,
				segmentID:   entry.SegmentID,
				valueSize:   entry.ValueSize,
			})
			if limit > 0 && len(candidates) >= limit {
				break
			}
		}
		shard.mu.RUnlock()
		if limit > 0 && len(candidates) >= limit {
			break
		}
	}

	nsKeyLen := len(ns) + len(nsSep)
	result := make([]NSEntry, 0, len(candidates))
	for _, c := range candidates {
		val, err := se.Get(c.internalKey)
		if err != nil {
			continue // key expired or deleted between scan and read
		}
		result = append(result, NSEntry{
			Key:   c.internalKey[nsKeyLen:], // strip "ns\x00" prefix
			Value: val,
		})
	}
	return result, nil
}

// ListNamespaces returns all distinct namespaces that have at least one live key,
// along with their key counts.
func (se *StorageEngine) ListNamespaces() []NSInfo {
	nowUs := time.Now().UnixMicro()
	counts := make(map[string]int)
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if entry.IsTombstone() || entry.IsExpired(nowUs) {
				continue
			}
			idx := strings.Index(k, nsSep)
			if idx <= 0 {
				continue // not a namespaced key
			}
			counts[k[:idx]]++
		}
		shard.mu.RUnlock()
	}
	result := make([]NSInfo, 0, len(counts))
	for ns, n := range counts {
		result = append(result, NSInfo{Namespace: ns, KeyCount: n})
	}
	return result
}

// ── Hash field operations ──────────────────────────────────────────────────────
//
// Hash fields are stored as regular KV entries whose internal key is the
// concatenation of the user-visible hash key, the byte separator "\x01"
// (hashFieldSep — distinct from the namespace separator "\x00"), and the
// field name.  This allows per-field TTLs without a separate data structure:
// each field is just an independent entry in the existing shardedIndex.

const hashFieldSep = "\x01"

func hashFieldKey(key, field string) string { return key + hashFieldSep + field }

// HashFieldKey exposes the internal hash-field key encoding so the distributed
// coordinator can replicate hash-field writes as plain KV operations.
func HashFieldKey(key, field string) string { return hashFieldKey(key, field) }

// HashField is a single field returned by HGetAll.
type HashField struct {
	Field string
	Value []byte
}

// HSet stores field → value in hash key with per-field TTL.
// ttl ≤ 0 means no expiry (-1 canonical). Each field has an independent TTL.
func (se *StorageEngine) HSet(key, field string, value []byte, ttl int32) error {
	return se.Put(hashFieldKey(key, field), value, ttl)
}

// HGet retrieves a single field from hash key. Returns nil if field doesn't exist or is expired.
func (se *StorageEngine) HGet(key, field string) ([]byte, error) {
	return se.Get(hashFieldKey(key, field))
}

// HDel removes a single field from hash key.
func (se *StorageEngine) HDel(key, field string) error {
	return se.Delete(hashFieldKey(key, field))
}

// HGetAll returns all non-expired fields and their values for hash key.
func (se *StorageEngine) HGetAll(key string) ([]HashField, error) {
	prefix := key + hashFieldSep
	nowUs := time.Now().UnixMicro()
	var internalKeys []string
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if !entry.IsTombstone() && !entry.IsExpired(nowUs) && strings.HasPrefix(k, prefix) {
				internalKeys = append(internalKeys, k)
			}
		}
		shard.mu.RUnlock()
	}
	results := make([]HashField, 0, len(internalKeys))
	for _, ik := range internalKeys {
		val, err := se.Get(ik)
		if err != nil || val == nil {
			continue
		}
		results = append(results, HashField{Field: ik[len(prefix):], Value: val})
	}
	return results, nil
}

// HKeys returns the names of all non-expired fields in hash key.
func (se *StorageEngine) HKeys(key string) []string {
	prefix := key + hashFieldSep
	nowUs := time.Now().UnixMicro()
	var fields []string
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if !entry.IsTombstone() && !entry.IsExpired(nowUs) && strings.HasPrefix(k, prefix) {
				fields = append(fields, k[len(prefix):])
			}
		}
		shard.mu.RUnlock()
	}
	return fields
}

// HLen returns the count of non-expired fields in hash key.
func (se *StorageEngine) HLen(key string) int {
	prefix := key + hashFieldSep
	nowUs := time.Now().UnixMicro()
	count := 0
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if !entry.IsTombstone() && !entry.IsExpired(nowUs) && strings.HasPrefix(k, prefix) {
				count++
			}
		}
		shard.mu.RUnlock()
	}
	return count
}

// HExpire updates the TTL on an existing field. Returns error if field not found.
// ttl ≤ 0 removes the expiry (makes the field immortal via -1 sentinel).
func (se *StorageEngine) HExpire(key, field string, ttl int32) error {
	ik := hashFieldKey(key, field)
	val, err := se.Get(ik)
	if err != nil {
		return err
	}
	if val == nil {
		return fmt.Errorf("field not found: %s.%s", key, field)
	}
	return se.Put(ik, val, ttl)
}

// HTTL returns the remaining TTL of a field in seconds.
//
//	-1 = immortal (no expiry)
//	-2 = field not found or already expired
//	 N = remaining seconds (≥ 0)
func (se *StorageEngine) HTTL(key, field string) int32 {
	ik := hashFieldKey(key, field)
	entry, _, ok := se.index.get(ik)
	if !ok || entry.IsTombstone() {
		return -2
	}
	nowUs := time.Now().UnixMicro()
	if entry.IsExpired(nowUs) {
		return -2
	}
	if entry.Flags&FlagHasTTL == 0 || entry.TTLExpiryUs == 0 {
		return -1
	}
	remaining := entry.TTLExpiryUs - nowUs
	if remaining <= 0 {
		return -2
	}
	return int32(remaining / 1_000_000)
}

// ── Accessors ─────────────────────────────────────────────────────────────────

// SetLatencyObservers wires Prometheus histogram observers for write/read/delete
// latency.  Called once from metrics.NewVeltrixCollector after both the engine
// and the collector are constructed.
func (se *StorageEngine) SetLatencyObservers(w, r, d func(float64)) {
	se.metrics.ObserveWriteLatency = w
	se.metrics.ObserveReadLatency = r
	se.metrics.ObserveDeleteLatency = d
}

func (se *StorageEngine) GetVersion() uint64          { return se.version.Load() }
func (se *StorageEngine) GetMetrics() *StorageMetrics { return se.metrics }
func (se *StorageEngine) GetIndexSize() int           { return se.index.size() }
func (se *StorageEngine) GetCacheStats() CacheStats   { return se.cache.Stats() }

// GetDataDirs returns the ordered list of data directory paths (one per disk).
func (se *StorageEngine) GetDataDirs() []string {
	dirs := se.config.DataDirPaths
	if len(dirs) == 0 {
		dirs = []string{se.config.DataDirPath}
	}
	return dirs
}

// Checkpoint writes a compacted WAL for every disk (one record per live key)
// and truncates the live WAL so the next startup replays O(numLiveKeys) instead
// of the full write history.  Safe to call while the engine is serving writes.
func (se *StorageEngine) Checkpoint() error {
	version := se.version.Load()
	numDisks := len(se.wals)
	kvSep := se.config.KeyValueSeparation
	var firstErr error
	for _, w := range se.wals {
		if err := w.checkpoint(se.index, numDisks, kvSep, version); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		log.Printf("[checkpoint] complete  disks=%d  keys=%d  version=%d",
			numDisks, se.index.size(), version)
	}
	return firstErr
}

// ScanKeys returns the keys of all live (non-tombstoned, non-expired) entries.
// The returned slice is a point-in-time snapshot; concurrent writes may not
// appear.  Used by the transfer agent and backup engine.
func (se *StorageEngine) ScanKeys() []string {
	nowUs := time.Now().UnixMicro()
	var keys []string
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if !entry.IsTombstone() && !entry.IsExpired(nowUs) {
				keys = append(keys, k)
			}
		}
		shard.mu.RUnlock()
	}
	return keys
}

// GetTTLForKey returns the remaining TTL in seconds for key, or -1 if immortal,
// or 0 if the key has expired or does not exist.
func (se *StorageEngine) GetTTLForKey(key string) int32 {
	entry, _, ok := se.index.get(key)
	if !ok || entry.IsTombstone() {
		return 0
	}
	if entry.Flags&FlagHasTTL == 0 {
		return -1 // immortal
	}
	nowUs := time.Now().UnixMicro()
	remaining := entry.TTLExpiryUs - nowUs
	if remaining <= 0 {
		return 0
	}
	return int32(remaining / 1_000_000)
}

// GetWALTotals aggregates WAL I/O counters across all per-disk WALs.
func (se *StorageEngine) GetWALTotals() (bytesWritten, entriesWritten uint64) {
	for _, w := range se.wals {
		b, e := w.GetStats()
		bytesWritten += b
		entriesWritten += e
	}
	return
}

// GetBatcherDropped returns the number of synchronous-fallback Put calls
// that occurred because the WriteBatcher's internal channel was saturated.
func (se *StorageEngine) GetBatcherDropped() uint64 {
	if se.batcher == nil {
		return 0
	}
	return se.batcher.Dropped.Load()
}

// GetVLogStats returns per-disk VLog statistics (KV-separation path only).
//
// The Slow flag is computed from the cluster-wide median of write+read EWMA:
// any disk whose combined EWMA exceeds slowDiskMultiplier × median is flagged.
// With < 2 disks the flag is always false (no median to compare against).
func (se *StorageEngine) GetVLogStats() []VLogStats {
	stats := make([]VLogStats, len(se.vlogs))
	for i, vl := range se.vlogs {
		stats[i] = vl.Stats()
	}
	flagSlowDisks(stats)
	return stats
}

// slowDiskMultiplier sets the slow-disk detection threshold: a disk is flagged
// when its (write+read) latency EWMA is at least this multiple of the cluster
// median. 5× is conservative — a healthy NVMe pool has all disks within ~1.5×
// of each other; a disk at 5× the median is well outside normal variance.
const slowDiskMultiplier = 5.0

// flagSlowDisks computes the median (write+read EWMA) across stats and sets
// stats[i].Slow = true on any disk whose own EWMA is at least slowDiskMultiplier
// times that median. Modifies stats in place.
func flagSlowDisks(stats []VLogStats) {
	if len(stats) < 2 {
		return // no median possible with 0 or 1 disk
	}
	combined := make([]float64, len(stats))
	for i, s := range stats {
		combined[i] = s.WriteLatencyEWMAs + s.ReadLatencyEWMAs
	}
	sorted := make([]float64, len(combined))
	copy(sorted, combined)
	// Inline insertion sort — N ≤ 16 in practice, no allocation needed.
	for i := 1; i < len(sorted); i++ {
		v := sorted[i]
		j := i
		for j > 0 && sorted[j-1] > v {
			sorted[j] = sorted[j-1]
			j--
		}
		sorted[j] = v
	}
	median := sorted[len(sorted)/2]
	if median <= 0 {
		return // no measured latency yet
	}
	threshold := median * slowDiskMultiplier
	for i := range stats {
		if combined[i] >= threshold {
			stats[i].Slow = true
		}
	}
}

// GetDiskStats returns per-disk segment file sizes for observability.
func (se *StorageEngine) GetDiskStats() []DiskStat {
	stats := make([]DiskStat, len(se.segments))
	for i, sw := range se.segments {
		stats[i] = DiskStat{
			DiskIdx:       i,
			Path:          sw.DiskPath(),
			SegmentBytes:  sw.DiskSize(),
			ShardsOnDisk:  numShards / len(se.segments),
		}
	}
	return stats
}

// DiskStat holds per-disk storage statistics.
type DiskStat struct {
	DiskIdx      int
	Path         string
	SegmentBytes int64
	ShardsOnDisk int
}

// BatchPut queues a fire-and-forget write via the async WriteBatcher.
//
// The call returns as soon as the request is placed on the batcher channel
// (typically < 1 µs).  WAL persistence and index update are performed
// asynchronously by the batcher goroutine, which flushes batches of up to
// 128 KB every 500 µs.
//
// Use Put for synchronous, immediately-acknowledged writes.
// Use BatchPut for high-throughput write paths where sub-millisecond latency
// matters more than per-write synchronous acknowledgement.
func (se *StorageEngine) BatchPut(key string, value []byte, ttl int32) {
	se.batcher.Enqueue(key, value, ttl)
}

// Close flushes all pending data and stops background goroutines.
func (se *StorageEngine) Close() error {
	// Drain the write batcher first so no queued writes are lost.
	se.batcher.Stop()

	// Shut down the C++ batch engine after the batcher has finished (the
	// batcher may still be calling cgoBatch.batchPutViaCGO during Stop).
	se.cgoBatch.close()

	// Shut down the io_uring storage bridge after the batch engine and VLogs
	// are drained — no new writes can arrive at this point.
	se.storageBridge.close()

	close(se.done)
	se.defrag.stop()
	close(se.sst.done)

	var firstErr error
	for _, sw := range se.segments {
		if err := sw.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, vl := range se.vlogs {
		if err := vl.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, w := range se.wals {
		if err := w.close(); err != nil && firstErr == nil {
			firstErr = err
		}
		// Write a compacted checkpoint WAL (one record per live key) so all
		// keys survive a clean restart.  The checkpoint is written to a temp
		// file and atomically renamed so a crash mid-write leaves the old WAL
		// intact.  Replay on next startup is O(numKeys) not O(totalWrites).
		if err := w.checkpoint(se.index, len(se.wals), se.config.KeyValueSeparation, se.version.Load()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ── Background workers ────────────────────────────────────────────────────────

// backgroundCompactionWorkerForDisk owns one disk.  It compacts only the shards
// assigned to that disk, so 8 disks → 8 goroutines running in parallel with zero
// contention between them on both the compaction queue and the segment file fd.
func (se *StorageEngine) backgroundCompactionWorkerForDisk(diskIdx int) {
	q := se.compactionQueues[diskIdx]
	for {
		select {
		case <-se.sst.done:
			return
		case req := <-q:
			se.compactDiskShards(diskIdx, req)
			se.metrics.CompactionRuns.Add(1)
		}
	}
}

// compactDiskShards flushes all dirty Index Vault entries whose shard belongs
// to diskIdx.  It writes each entry as a binary record to the disk's segment
// file and then atomically updates the IndexEntry.DiskOffset so future reads
// know where to find it.
//
// Lock discipline:
//  1. Acquire shard lock briefly to snapshot dirty entries.
//  2. Release lock before writing to disk (avoids blocking concurrent reads).
//  3. Re-acquire lock to update DiskOffset only if the entry is still valid.
func (se *StorageEngine) compactDiskShards(diskIdx int, _ CompactionRequest) {
	if diskIdx >= len(se.segments) {
		return
	}
	sw := se.segments[diskIdx]
	numDisks := len(se.segments)

	type pending struct {
		key   string
		value []byte
		ttlUs int64
	}

	// Iterate over shards owned by this disk.
	for shardIdx := diskIdx; shardIdx < numShards; shardIdx += numDisks {
		shard := &se.index.shards[shardIdx]

		// 1. Snapshot dirty entries under a brief lock.
		shard.mu.Lock()
		if len(shard.dirtyValues) == 0 {
			shard.mu.Unlock()
			continue
		}
		snap := make([]pending, 0, len(shard.dirtyValues))
		for key, val := range shard.dirtyValues {
			entry := shard.entries[key]
			if entry == nil || entry.IsTombstone() {
				delete(shard.dirtyValues, key)
				se.index.dirtyCount.Add(-1)
				continue
			}
			snap = append(snap, pending{key, val, entry.TTLExpiryUs})
		}
		shard.mu.Unlock()

		// 2. Write each record to the segment file (no lock held — reads can proceed).
		for _, p := range snap {
			diskOffset, err := sw.WriteRecord(p.key, p.value, p.ttlUs, false)
			if err != nil {
				continue
			}

			// 3. Update DiskOffset only if the entry hasn't been overwritten.
			shard.mu.Lock()
			if live := shard.entries[p.key]; live != nil && !live.IsTombstone() {
				live.DiskOffset = uint64(diskOffset)
				live.SegmentID = uint32(diskIdx)
				delete(shard.dirtyValues, p.key)
				se.index.dirtyCount.Add(-1)
			}
			shard.mu.Unlock()
		}
	}
	se.metrics.SSTableCreations.Add(1)
}

// backgroundTTLScanner periodically marks expired entries as tombstones.
func (se *StorageEngine) backgroundTTLScanner() {
	ticker := time.NewTicker(se.config.TTLCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-se.done:
			return
		case <-ticker.C:
			nowUs := time.Now().UnixMicro()
			expired := se.index.expiredTTL()
			if len(expired) > 0 {
				for _, key := range expired {
					se.index.markTombstone(key, nowUs)
					se.cache.Evict(key)
					se.metrics.EvictionMetrics.TTLEvictions.Add(1)
					se.metrics.EvictionMetrics.TotalEvictions.Add(1)
				}
			}
		}
	}
}

// shouldCompact returns true when the dirty-key count exceeds the flush
// threshold.  The check is O(1) via the atomic counter maintained by
// shardedIndex — no shard locks acquired on the write hot path.
func (se *StorageEngine) shouldCompact() bool {
	return se.index.dirtyCount.Load() > int64(se.config.DirtyFlushThreshold)
}

// applyBackPressure sleeps briefly when the dirty memtable has grown past the
// back-pressure ceiling.  This acts as a soft write stall: callers slow down
// naturally instead of letting the dirty map grow without bound.
func (se *StorageEngine) applyBackPressure() {
	if se.config.BackPressureThreshold <= 0 {
		return
	}
	dirty := se.index.dirtyCount.Load()
	if dirty > int64(se.config.BackPressureThreshold) {
		se.metrics.BackPressureEvents.Add(1)
		sleepMs := se.config.BackPressureSleepMs
		if sleepMs <= 0 {
			sleepMs = 1
		}
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}
}

// triggerCompaction enqueues a compaction request to every disk's queue so all
// disks compact their pending shards in parallel.
func (se *StorageEngine) triggerCompaction() {
	seq := se.version.Load()
	for diskIdx, q := range se.compactionQueues {
		select {
		case q <- CompactionRequest{
			SourceLevel:    0,
			TargetLevel:    1,
			PartitionID:    uint32(diskIdx),
			SequenceNumber: seq,
			CreatedAtNs:    time.Now().UnixNano(),
		}:
		default:
		}
	}
}
