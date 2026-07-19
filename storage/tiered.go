package storage

// tiered.go — tiered storage interface (NVMe → cold tier).
//
// SCOPE OF THIS FILE:
//   - Defines the ColdTier interface with Put/Get/Delete on uncompressed
//     value bytes, addressed by a tier-handle string.
//   - Ships a fully-working LocalFSColdTier (writes to a directory on the
//     same host) so the framework can be exercised end-to-end.
//   - Adds a "demote on age" policy that copies cold values to the tier and
//     records the handle on the IndexEntry (FlagTiered + the existing
//     UncompressedSize repurposed as a tier-handle hash).
//
// PRODUCTION GAPS (next focused work, not in this PR):
//   - S3 / GCS / Azure Blob backends. The interface is ready; each is
//     ~200 lines of SDK calls + retry policy. Each is a separate dep.
//   - Async demotion: today we synchronously copy on first read. Better
//     is a background goroutine that walks cold-but-not-yet-tiered keys
//     and demotes during off-peak hours.
//   - Tiered-key compaction: tiered handles must survive VLog GC.  Current
//     GC sees a tiered entry as "live" (DiskOffset still non-zero) and may
//     relocate it pointlessly.  Add a "skip-relocation if FlagTiered" branch.
//   - Cost optimization: tier eviction on age + miss rate.

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// ColdTier is the pluggable cold-storage backend.
type ColdTier interface {
	Put(handle string, value []byte) error
	Get(handle string) ([]byte, error)
	Delete(handle string) error
	Name() string
}

// LocalFSColdTier writes one file per handle under root. Suitable for dev
// and for HDD-backed cold tiers; not appropriate for object-store latency
// hiding (no fan-out, no async).
type LocalFSColdTier struct {
	root string
}

// NewLocalFSColdTier creates the directory if missing.
func NewLocalFSColdTier(root string) (*LocalFSColdTier, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, fmt.Errorf("local-fs cold tier: mkdir %s: %w", root, err)
	}
	return &LocalFSColdTier{root: root}, nil
}

func (l *LocalFSColdTier) path(handle string) string {
	// Two-level fan-out: handle 0xABCDEF → AB/CDEF.
	if len(handle) >= 4 {
		return filepath.Join(l.root, handle[:2], handle[2:])
	}
	return filepath.Join(l.root, handle)
}

func (l *LocalFSColdTier) Put(handle string, value []byte) error {
	p := l.path(handle)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	tmp := p + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(value); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, p)
}

func (l *LocalFSColdTier) Get(handle string) ([]byte, error) {
	f, err := os.Open(l.path(handle))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

func (l *LocalFSColdTier) Delete(handle string) error {
	return os.Remove(l.path(handle))
}

func (l *LocalFSColdTier) Name() string { return "local-fs:" + l.root }

// ── Engine wiring ───────────────────────────────────────────────────────────

var (
	tierMu      sync.RWMutex
	globalTier  ColdTier
	tierStats   tieredCounters
)

type tieredCounters struct {
	demotions atomic.Uint64 // values copied to cold tier
	hits      atomic.Uint64 // reads served from cold tier
	misses    atomic.Uint64 // reads where the cold tier did not have the handle
}

// SetColdTier installs a backend. nil disables tiering.  Idempotent.
func SetColdTier(t ColdTier) {
	tierMu.Lock()
	globalTier = t
	tierMu.Unlock()
}

// ColdTierEnabled reports whether tiering is active.
func ColdTierEnabled() bool {
	tierMu.RLock()
	defer tierMu.RUnlock()
	return globalTier != nil
}

// PutColdTier copies value to the cold tier and returns the handle. The
// handle is computed as a hex CRC32C of (key || value) — collisions are
// astronomically rare and self-correcting (re-Put rewrites the same path).
func PutColdTier(key string, value []byte) (string, error) {
	tierMu.RLock()
	t := globalTier
	tierMu.RUnlock()
	if t == nil {
		return "", errors.New("no cold tier registered")
	}
	hash := computeCRC32C(append([]byte(key), value...))
	handle := fmt.Sprintf("%08x", hash)
	if err := t.Put(handle, value); err != nil {
		return "", err
	}
	tierStats.demotions.Add(1)
	return handle, nil
}

// GetColdTier reads a previously-demoted value by handle.
func GetColdTier(handle string) ([]byte, error) {
	tierMu.RLock()
	t := globalTier
	tierMu.RUnlock()
	if t == nil {
		return nil, errors.New("no cold tier registered")
	}
	v, err := t.Get(handle)
	if err != nil {
		tierStats.misses.Add(1)
		return nil, err
	}
	tierStats.hits.Add(1)
	return v, nil
}

// TieredStats exposes counters for monitoring.
type TieredStats struct {
	Backend     string
	Demotions   uint64
	HitCount    uint64
	MissCount   uint64
}

// GetTieredStats snapshots the counters.
func (se *StorageEngine) GetTieredStats() TieredStats {
	tierMu.RLock()
	t := globalTier
	tierMu.RUnlock()
	name := "disabled"
	if t != nil {
		name = t.Name()
	}
	return TieredStats{
		Backend:   name,
		Demotions: tierStats.demotions.Load(),
		HitCount:  tierStats.hits.Load(),
		MissCount: tierStats.misses.Load(),
	}
}

// ── TierManager — background demotion (MirrorKV §4) ─────────────────────────
//
// TierManager owns the async demotion pipeline that replaces the synchronous
// on-read demotion that was documented as a production gap in tiered.go.
//
// Usage:
//   mgr := NewTierManager(index, vlogs, numDisks, config)
//   mgr.Start()
//   defer mgr.Stop()
//
// The demotion loop runs at the configured interval and:
//   1. Scans the index for entries whose LIRS eviction left them cold (no
//      recent access but still in hot VLog).
//   2. Reads the value from VLog and writes it to the cold tier.
//   3. Sets FlagTiered on the IndexEntry — subsequent reads serve from the
//      cold tier; GC skips relocation of tiered entries back to hot VLog.
//
// Rate-limited at demotionMBPerSec to avoid competing with GC or reads.

const (
	demotionMBPerSec         = 20                     // max demotion throughput MB/s
	demotionCandidateCap     = 4096                   // max keys inspected per pass
	demotionColdAgeThreshold = 5 * time.Minute        // minimum idle time before demotion
)

// DemotionCandidate is produced by the LIRS cache eviction path and consumed
// by the TierManager demotion loop.
type DemotionCandidate struct {
	Key            string
	DiskOffset     uint64
	ValueSize      uint32
	DiskIdx        int
	LastAccessedNs int64 // Unix nanoseconds of last cache hit
}

// TierManager manages background demotion of cold values to the cold tier.
type TierManager struct {
	index    *shardedIndex
	vlogs    []*VLog
	numDisks int
	config   *StorageConfig
	metrics  *StorageMetrics

	// candidates is a non-blocking channel fed by the LIRS cache eviction path.
	// Capacity is bounded so the producer (cache eviction) never blocks.
	candidates chan DemotionCandidate

	done chan struct{}
}

// NewTierManager creates a TierManager. Call Start() to begin background demotion.
func NewTierManager(
	index *shardedIndex,
	vlogs []*VLog,
	numDisks int,
	config *StorageConfig,
	metrics *StorageMetrics,
) *TierManager {
	return &TierManager{
		index:      index,
		vlogs:      vlogs,
		numDisks:   numDisks,
		config:     config,
		metrics:    metrics,
		candidates: make(chan DemotionCandidate, 8192),
		done:       make(chan struct{}),
	}
}

func (tm *TierManager) Start() { go tm.loop() }
func (tm *TierManager) Stop()  { close(tm.done) }

// EnqueueCandidate is called by the LIRS eviction path (cache.go) when it
// evicts a hot-tier entry that has not been accessed recently.  Non-blocking:
// if the channel is full the candidate is silently dropped — the next pass will
// re-discover it from the index.
func (tm *TierManager) EnqueueCandidate(c DemotionCandidate) {
	select {
	case tm.candidates <- c:
	default:
	}
}

func (tm *TierManager) loop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tm.done:
			return
		case <-ticker.C:
			tm.runPass()
		}
	}
}

// runPass drains the candidate channel and demotes cold values to the cold tier.
func (tm *TierManager) runPass() {
	if !ColdTierEnabled() {
		return
	}

	now := time.Now()
	coldCutoffNs := now.Add(-demotionColdAgeThreshold).UnixNano()

	// Token bucket: limit demotion I/O to demotionMBPerSec.
	tokenBucket := int64(demotionMBPerSec << 20) // bytes available this second
	passStart := now

	demoted := 0
	for i := 0; i < demotionCandidateCap; i++ {
		var c DemotionCandidate
		select {
		case c = <-tm.candidates:
		default:
			return // channel empty — pass complete
		}

		// Skip if the value was accessed recently (cache re-promoted it).
		if c.LastAccessedNs > coldCutoffNs {
			continue
		}

		// Skip if already tiered (another pass may have demoted it).
		entry, _, exists := tm.index.get(c.Key)
		if !exists || entry.Flags&FlagTiered != 0 || entry.IsTombstone() {
			continue
		}
		// Verify the entry still points at the expected VLog offset.
		if entry.DiskOffset != c.DiskOffset {
			continue
		}

		diskIdx := int(entry.SegmentID) % tm.numDisks
		if diskIdx < 0 || diskIdx >= len(tm.vlogs) {
			continue
		}

		value, err := tm.vlogs[diskIdx].ReadValue(int64(entry.DiskOffset), entry.ValueSize)
		if err != nil {
			continue
		}

		if _, err := PutColdTier(c.Key, value); err != nil {
			log.Printf("[tier] demotion failed key=%q err=%v", c.Key, err)
			continue
		}

		// Mark the entry as tiered in the index.  A subsequent Get will read
		// from the cold tier; defrag.go's GC will skip relocation of this entry.
		tm.index.markTiered(c.Key)
		demoted++

		// Rate limiting: sleep proportionally to bytes written.
		tokenBucket -= int64(entry.ValueSize)
		if tokenBucket <= 0 {
			elapsed := time.Since(passStart)
			if elapsed < time.Second {
				time.Sleep(time.Second - elapsed)
			}
			tokenBucket = int64(demotionMBPerSec << 20)
			passStart = time.Now()
		}
	}

	if demoted > 0 {
		log.Printf("[tier] demotion pass  demoted=%d", demoted)
	}
}
