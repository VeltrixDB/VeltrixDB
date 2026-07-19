package storage

import (
	"log"
	"runtime"
	"sort"
	"sync"
	"time"
)

const (
	// gcThrottledBPS caps GC write bandwidth when read EWMA exceeds gcLatencyThresholdNs
	// AND garbage ratio is below gcCriticalRatio. 60 MB/s ≈ 15% of a typical
	// 400 MB/s NVMe sequential-write bandwidth, leaving 85% headroom for reads
	// and foreground writes — matching the 15% GC budget target.
	gcThrottledBPS int64 = 60 << 20 // 60 MB/s

	// gcCriticalBPS is the bandwidth cap once garbage ratio crosses gcCriticalRatio.
	// At that point reclaiming dead space is the only way to stop the death-spiral,
	// so we raise the cap to 200 MB/s (~50% of NVMe). Reads still get the remaining
	// 50% headroom — better than letting the system continue to degrade.
	gcCriticalBPS int64 = 200 << 20 // 200 MB/s

	// Anti-starvation floor: GC progresses at least this fast even when throttled.
	// Raised from 4 MB/s → 16 MB/s. At 4 MB/s reclaiming 100 GB takes ~7 hours,
	// long enough for garbage to grow faster than GC can shrink it. 16 MB/s keeps
	// GC ahead of moderate write workloads while still leaving plenty of read headroom.
	gcMinimumBPS int64 = 16 << 20 // 16 MB/s

	// Token-bucket burst: allow up to 16 MB in a burst before refilling.
	gcBurstBytes int64 = 16 << 20 // 16 MB

	// If read EWMA (nanoseconds) exceeds this, apply the 60 MB/s BW cap.
	// 15ms keeps GC running freely up to 3/4 of the 20ms admission-control pause
	// threshold, giving a 5ms buffer before writes are fully throttled.
	gcLatencyThresholdNs int64 = 15_000_000 // 15 ms

	// ── Tiered emergency thresholds ──────────────────────────────────────────
	// As garbage ratio rises, admission control's "pause GC" interaction creates
	// a death spiral: high garbage → bigger VLog → more cache misses → slower
	// reads → admission control pauses GC → garbage keeps growing. The fix is
	// to escalate GC priority as garbage grows, eventually overriding admission
	// control entirely when the system is already in a broken state.
	//
	//   ratio < 30%   no GC (DefragThreshold)
	//   30% ≤ r < 50%  normal GC, full admission-control respect
	//   50% ≤ r < 65%  CRITICAL — bandwidth cap raised to 200 MB/s, defrag
	//                   interval cut in half, but still respects GCPaused.
	//   r ≥ 65%        EMERGENCY — bypass GCPaused, run uncapped. The system
	//                   is already in a broken state; pretending reads will be
	//                   fast helps no one. Operator-visible log line is emitted.
	gcCriticalRatio  = 0.50 // raise BW cap, halve defrag interval
	gcEmergencyRatio = 0.65 // bypass GCPaused entirely
)

// gcRateLimiter is a simple token-bucket limiter for GC write bandwidth.
// It is only called from the single background GC goroutine, so a mutex is fine.
type gcRateLimiter struct {
	mu     sync.Mutex
	tokens int64
	lastAt time.Time
}

func newGCRateLimiter() *gcRateLimiter {
	return &gcRateLimiter{tokens: gcBurstBytes, lastAt: time.Now()}
}

// consume blocks until n bytes of token budget are available at limitBPS.
// limitBPS == 0 means unlimited — returns immediately.
// The effective rate is clamped to [gcMinimumBPS, limitBPS] so GC is never
// fully starved regardless of how bad read latency is.
func (rl *gcRateLimiter) consume(n int, limitBPS int64) {
	if limitBPS <= 0 {
		return // unlimited
	}
	if limitBPS < gcMinimumBPS {
		limitBPS = gcMinimumBPS // anti-starvation floor
	}

	rl.mu.Lock()
	now := time.Now()
	elapsed := now.Sub(rl.lastAt).Seconds()
	rl.lastAt = now
	rl.tokens += int64(elapsed * float64(limitBPS))
	if rl.tokens > gcBurstBytes {
		rl.tokens = gcBurstBytes
	}
	rl.tokens -= int64(n)
	deficit := -rl.tokens
	rl.mu.Unlock()

	if deficit > 0 {
		// Sleep until refill covers the deficit.
		wait := time.Duration(float64(deficit) / float64(limitBPS) * 1e9)
		time.Sleep(wait)
	}
}

// Defragmenter runs as a background goroutine and enforces two GC policies:
//
//  1. Tombstone Reaping — Once a tombstone's age exceeds GCGracePeriodSec, the
//     key is physically removed from the Index Vault.  The grace period ensures
//     that lagging replicas (which may not yet have applied the delete) still
//     see the tombstone long enough to avoid "zombie data" resurrecting a
//     deleted key during Read Repair.
//
//  2. VLog Compaction (WiscKey key-scanning) — When a VLog's GCRatio() exceeds
//     DefragThreshold (default 0.30), the defragmenter scans all Index Vault
//     entries for that disk and rewrites live values to the tail of the VLog.
//     A CAS on DiskOffset guards against concurrent Puts overwriting candidates.
//     GC writes are rate-limited to gcThrottledBPS (10 MB/s) when the read EWMA
//     exceeds gcLatencyThresholdNs (5 ms), with a gcMinimumBPS (1 MB/s) floor
//     so GC is never fully starved.
//
// The defragmenter is pinned to a low-priority goroutine (SCHED_IDLE
// equivalent via runtime.LockOSThread + nice, when enabled).  It yields
// between shards via runtime.Gosched() so that ongoing reads are not impacted.
type Defragmenter struct {
	index    *shardedIndex
	wal      *WriteAheadLog
	config   *StorageConfig
	metrics  *StorageMetrics
	cache    Cache
	vlogs    []*VLog
	numDisks int
	// One rate-limiter per disk so parallel GC goroutines don't share a token
	// bucket — each NVMe has independent bandwidth and its own IOPS budget.
	limiters []*gcRateLimiter
	done     chan struct{}

	// diskFailed reports whether the engine's breaker for a disk has tripped
	// (disk_health.go); GC skips failed disks instead of hammering a dead
	// device. Nil-safe: nil means "never failed" (tests construct
	// Defragmenter directly).
	diskFailed func(int) bool
}

// skipDisk is the nil-safe breaker check used inside run().
func (d *Defragmenter) skipDisk(i int) bool {
	return d.diskFailed != nil && d.diskFailed(i)
}

func newDefragmenter(
	index *shardedIndex,
	wal *WriteAheadLog,
	config *StorageConfig,
	metrics *StorageMetrics,
	cache Cache,
	vlogs []*VLog,
	numDisks int,
) *Defragmenter {
	limiters := make([]*gcRateLimiter, numDisks)
	for i := range limiters {
		limiters[i] = newGCRateLimiter()
	}
	return &Defragmenter{
		index:    index,
		wal:      wal,
		config:   config,
		metrics:  metrics,
		cache:    cache,
		vlogs:    vlogs,
		numDisks: numDisks,
		limiters: limiters,
		done:     make(chan struct{}),
	}
}

func (d *Defragmenter) start() {
	go d.loop()
}

func (d *Defragmenter) stop() {
	close(d.done)
}

// gcEWMAStaleDuration is the time after which a stale ReadLatencyEWMA (no
// reads in this window) is treated as cold-zero and GCPaused is cleared.
// Set to DefragInterval × 2 so that one missed defrag tick is acceptable
// but a permanently write-only workload does not block GC indefinitely.
const gcEWMAStaleDuration = 2 * 120 * time.Second // 4 minutes (2 × default DefragInterval)

func (d *Defragmenter) loop() {
	// Adaptive interval: on every wakeup we recompute the next sleep based on
	// the worst-case garbage ratio across all disks. Faster cadence when
	// garbage is high, normal cadence otherwise.
	//   ratio < 50%   → DefragInterval (default 120 s)
	//   50% ≤ r < 65%  → DefragInterval / 2 (60 s) — critical, shorter window
	//   r ≥ 65%        → DefragInterval / 4 (30 s) — emergency, run hard
	//
	// This is a fixed timer (not ticker) so we can adjust the period dynamically.
	timer := time.NewTimer(d.config.DefragInterval)
	defer timer.Stop()

	for {
		select {
		case <-d.done:
			return
		case <-timer.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[gc] panic recovered — loop continues: %v", r)
						d.metrics.DefragRuns.Add(1)
					}
				}()
				d.run()
			}()
			timer.Reset(d.nextInterval())
		}
	}
}

// nextInterval returns how long to wait before the next defrag pass, scaled by
// the worst-case garbage ratio across all VLogs. A single highly-fragmented
// disk is enough to shorten the cadence — we don't want to wait for all disks
// to be in trouble before reacting.
func (d *Defragmenter) nextInterval() time.Duration {
	if !d.config.KeyValueSeparation || len(d.vlogs) == 0 {
		return d.config.DefragInterval
	}
	maxRatio := 0.0
	for _, vl := range d.vlogs {
		if r := vl.GCRatio(); r > maxRatio {
			maxRatio = r
		}
	}
	switch {
	case maxRatio >= gcEmergencyRatio:
		return d.config.DefragInterval / 4
	case maxRatio >= gcCriticalRatio:
		return d.config.DefragInterval / 2
	default:
		return d.config.DefragInterval
	}
}

// run performs one full defragmentation pass.
// VLog compaction runs on all disks in parallel — each disk has its own NVMe
// queue and independent bandwidth, so sequential per-disk GC is needlessly slow.
func (d *Defragmenter) run() {
	d.reapExpiredTombstones()
	if d.config.KeyValueSeparation {
		var wg sync.WaitGroup
		for i, vl := range d.vlogs {
			wg.Add(1)
			go func(diskIdx int, vl *VLog) {
				defer wg.Done()
				if d.skipDisk(diskIdx) {
					return // dead device — nothing to reclaim, don't add I/O timeouts
				}
				// CaaS-LSM phase 1: pin each GC goroutine to its own OS thread
				// and lower scheduling priority so the Linux CFS scheduler
				// prefers request-handler goroutines.  This prevents EMERGENCY
				// mode from grabbing all CPUs away from in-flight client reads.
				lockDefragThread()
				d.compactVLog(diskIdx, vl)
			}(i, vl)
		}
		wg.Wait()
	}
	// Rebuild per-shard bloom filters from the live index. Drops bits left
	// behind by deletes so the false-positive rate stays close to the design
	// point (~1% at 400 K keys/shard). No-op when blooms are disabled.
	d.index.vacuumBloomFilters()
	d.metrics.DefragRuns.Add(1)
}

// gcRateBPS returns the current GC write rate cap for diskIdx based on the
// global read EWMA latency AND the per-disk garbage ratio. Returns 0 (unlimited)
// when reads are fast OR when garbage ratio is at emergency level. The garbage
// ratio scales the cap because the cost of NOT running GC at high ratios
// (further read amplification, sustained high latency, eventually OOM) is worse
// than the cost of running GC harder.
//
//	reads fast (EWMA ≤ 15 ms)    → unlimited
//	reads slow + ratio < 50%      → 60 MB/s (gcThrottledBPS)
//	reads slow + 50% ≤ r < 65%    → 200 MB/s (gcCriticalBPS)
//	reads slow + r ≥ 65%          → unlimited (emergency override)
func (d *Defragmenter) gcRateBPS(diskIdx int, gcRatio float64) int64 {
	if d.metrics.ReadLatencyEWMANs.Load() <= gcLatencyThresholdNs {
		return 0 // reads are fast — let GC run at full disk speed
	}
	if gcRatio >= gcEmergencyRatio {
		return 0 // emergency — uncapped, system is already degraded
	}
	if gcRatio >= gcCriticalRatio {
		return gcCriticalBPS // critical — raise cap from 60 to 200 MB/s
	}
	return gcThrottledBPS // normal throttle
}

// compactVLog rewrites live VLog records below the GC horizon to the tail of
// the VLog, reclaiming dead space.  It only runs when GCRatio exceeds
// DefragThreshold to avoid unnecessary background I/O.
func (d *Defragmenter) compactVLog(diskIdx int, vl *VLog) {
	gcRatio := vl.GCRatio()
	vlogGB := float64(vl.end.Load()) / float64(1<<30)

	if gcRatio < d.config.DefragThreshold {
		d.metrics.VLogGCSkippedRatio.Add(1)
		log.Printf("[gc] disk=%d skip  garbage=%.1f%% < threshold=%.0f%%  vlog=%.2fGB",
			diskIdx, gcRatio*100, d.config.DefragThreshold*100, vlogGB)
		return
	}

	// Admission control: if read EWMA crossed the 20ms threshold, pause GC
	// entirely so NVMe bandwidth is fully available for reads.
	//
	// EMERGENCY override: at gcRatio ≥ gcEmergencyRatio (65%) the system is
	// already in a broken state — pausing GC just deepens the death spiral.
	// We bypass GCPaused, log loudly so operators see it, and let GC reclaim.
	//
	// Staleness guard: if no Get() has arrived for gcEWMAStaleDuration (4 min
	// default), the EWMA reflects a write-only era and can never self-clear via
	// normal reads.  In that case we treat the EWMA as cold, reset GCPaused, and
	// let GC proceed — otherwise a single slow read early in a write-heavy
	// benchmark permanently blocks all VLog compaction.
	if d.metrics.Admission.GCPaused.Load() {
		if gcRatio >= gcEmergencyRatio {
			ewmaMs := float64(d.metrics.ReadLatencyEWMANs.Load()) / 1e6
			d.metrics.VLogGCEmergencyRuns.Add(1)
			log.Printf("[gc] disk=%d EMERGENCY  garbage=%.1f%% ≥ %.0f%%  ewma=%.1fms  bypassing admission-control pause to escape death spiral",
				diskIdx, gcRatio*100, gcEmergencyRatio*100, ewmaMs)
		} else {
			lastRead := d.metrics.Admission.LastReadNs.Load()
			staleDur := gcEWMAStaleDuration
			stale := lastRead == 0 || time.Since(time.Unix(0, lastRead)) > staleDur
			if stale {
				ewmaMs := float64(d.metrics.ReadLatencyEWMANs.Load()) / 1e6
				log.Printf("[gc] disk=%d stale EWMA cleared (no reads for >%v  ewma=%.1fms)  resuming compaction",
					diskIdx, staleDur, ewmaMs)
				d.metrics.ReadLatencyEWMANs.Store(0)
				d.metrics.Admission.GCPaused.Store(false)
				d.metrics.Admission.WriteThrottleActive.Store(false)
			} else {
				ewmaMs := float64(d.metrics.ReadLatencyEWMANs.Load()) / 1e6
				d.metrics.VLogGCSkippedPaused.Add(1)
				log.Printf("[gc] disk=%d paused  read_ewma=%.1fms > admission_threshold=%dms  garbage=%.1f%% (< emergency %.0f%%)",
					diskIdx, ewmaMs, admissionThrottleNs/1_000_000, gcRatio*100, gcEmergencyRatio*100)
				return
			}
		}
	}

	// Snapshot the current end-offset as the GC horizon.  Any record below this
	// offset is a compaction candidate; records appended during GC land above it.
	gcHorizon := uint64(vl.end.Load())

	candidates := d.index.vlogCandidates(diskIdx, d.numDisks, gcHorizon)
	d.metrics.VLogGCCandidates.Add(uint64(len(candidates)))
	if len(candidates) == 0 {
		d.metrics.VLogGCSkippedEmpty.Add(1)
		log.Printf("[gc] disk=%d skip  no candidates below horizon=%d  (garbage=%.1f%%  vlog=%.2fGB)",
			diskIdx, gcHorizon, gcRatio*100, vlogGB)
		return
	}

	// ── Hot-key filter ────────────────────────────────────────────────────────
	// Skip keys written within gcHotKeyWindow.  These are actively written and
	// will likely be overwritten again before the next GC pass, so compacting
	// them wastes VLog I/O and creates orphans without reducing net garbage.
	// Reads get the most preference: by skipping hot keys GC avoids polluting
	// the NVMe read queue with relocations that benefit no one.
	const gcHotKeyWindow = 30 * time.Second
	hotCutoffUs := time.Now().Add(-gcHotKeyWindow).UnixMicro()
	cold := candidates[:0] // reuse backing array — avoids a second allocation
	hotSkipped := 0
	for _, c := range candidates {
		if c.writeTimestampUs > hotCutoffUs {
			hotSkipped++
			continue
		}
		cold = append(cold, c)
	}
	candidates = cold

	if hotSkipped > 0 {
		log.Printf("[gc] disk=%d hot-skip=%d keys written <%.0fs ago  cold_candidates=%d",
			diskIdx, hotSkipped, gcHotKeyWindow.Seconds(), len(candidates))
	}
	if len(candidates) == 0 {
		d.metrics.VLogGCSkippedEmpty.Add(1)
		log.Printf("[gc] disk=%d skip  all candidates are hot (written <%.0fs ago)  garbage=%.1f%%",
			diskIdx, gcHotKeyWindow.Seconds(), gcRatio*100)
		return
	}

	// ── Temperature-weighted sort (Scavenger+ cold-first ordering) ───────────
	// Sort by writeTimestampUs ascending so GC attacks truly oldest data first,
	// regardless of current disk position.  Unlike a pure diskOffset sort, this
	// correctly prioritises re-compacted records: a value relocated by a prior GC
	// pass now sits at a high offset but may still carry an old writeTimestampUs,
	// making it cold and unlikely to be overwritten again.
	//
	// Tie-break on diskOffset so contiguous low-offset runs are processed first,
	// maximising the PUNCH_HOLE watermark advance within the same temperature tier.
	sortStart := time.Now()
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].writeTimestampUs != candidates[j].writeTimestampUs {
			return candidates[i].writeTimestampUs < candidates[j].writeTimestampUs
		}
		return candidates[i].diskOffset < candidates[j].diskOffset
	})
	log.Printf("[gc] disk=%d cold-first sort  %d candidates in %s  oldest_ts=%d  newest_ts=%d",
		diskIdx, len(candidates), time.Since(sortStart).Round(time.Millisecond),
		candidates[0].writeTimestampUs, candidates[len(candidates)-1].writeTimestampUs)

	// Count this GC pass immediately — it ran and found candidates regardless of
	// how many CAS operations succeed.  Gating on reclaimedBytes > 0 caused
	// gc_runs_total to stay 0 whenever every CAS lost the race to a concurrent
	// Put, even though GC wrote 10L+ relocations to VLog.
	d.metrics.VLogGCRuns.Add(1)

	log.Printf("[gc] disk=%d pass start  vlog=%.2fGB  garbage=%.1f%%  candidates=%d  horizon=%d",
		diskIdx, vlogGB, gcRatio*100, len(candidates), gcHorizon)

	startTime := time.Now()
	casFailsBefore := d.metrics.VLogGCCASFails.Load()

	// Per-pass outcome counters.
	var (
		reclaimedBytes int64
		deadSkipped    int // pre-check fail: record confirmed dead (overwritten above horizon / tombstoned / gone)
		deferPostCAS   int // CAS fail after batch flush: orphan created, write was wasted
		deferReadErr   int // ReadValue failed
		deferIOErr     int // batcher.Stage or Flush failed
		casLogCount    int // how many individual CAS-fail lines printed this pass
	)

	const (
		gcBatchSize    = 256  // records staged per fdatasync — 256 × 4 KB = 1 MB per flush
		gcGoschedEvery = 1000 // yield to scheduler every N candidates, not every 1
		gcCASLogMax    = 5    // max individual CAS-fail log lines per pass
		gcKeyLogLen    = 32   // max key bytes shown in log lines
	)

	// gcReloc holds the state needed to CAS-update the index after a batch flush.
	type gcReloc struct {
		key         string
		oldOffset   uint64
		newOffset   int64
		valueSize   uint32 // used for MarkDead on both success and CAS-fail paths
		oldPacked   bool   // packed flag of the record being relocated (for MarkDead)
		newPacked   bool   // true when relocated copy is packed (always true today — VLogBatcher packs)
	}

	batcher := vl.NewBatcher()
	batch := make([]gcReloc, 0, gcBatchSize)

	// flushBatch issues one fdatasync for all staged records, then CAS-updates
	// the index for each.  A single fdatasync covers gcBatchSize records instead
	// of one per record — this is the primary fix for the 6+ hour GC runtime.
	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		if err := batcher.Flush(); err != nil {
			log.Printf("[gc] disk=%d batch-flush error: %v  abandoning %d relocations",
				diskIdx, err, len(batch))
			for range batch {
				deferIOErr++
			}
			batch = batch[:0]
			return
		}
		batchCASFails := 0
		for _, r := range batch {
			if d.index.updateVLogOffset(r.key, r.oldOffset, uint64(r.newOffset), r.newPacked) {
				vl.MarkDead(r.valueSize, r.oldPacked) // old copy is now dead
				// Reclaimed bytes: the old record's footprint, accounting for
				// whether it was packed (raw size) or unpacked (full block).
				rawLen := int64(r.valueSize) + int64(vlogHeaderBytes)
				var reclaimed int64
				if r.oldPacked {
					reclaimed = rawLen
				} else {
					reclaimed = (rawLen + int64(vlogBlockSize) - 1) &^ int64(vlogBlockSize-1)
				}
				reclaimedBytes += reclaimed
				d.cache.Evict(r.key)
			} else {
				// Concurrent Put moved the index between Stage and CAS.
				// The new copy at r.newOffset is orphaned — mark dead.
				// The orphan is in a packed block (we just wrote it via
				// VLogBatcher), so the orphan's footprint is its raw size.
				d.metrics.VLogGCCASFails.Add(1)
				vl.MarkDead(r.valueSize, true /* newPacked: orphan is in our packed block */)
				deferPostCAS++
				batchCASFails++
				if casLogCount < gcCASLogMax {
					casLogCount++
					k := r.key
					if len(k) > gcKeyLogLen {
						k = k[:gcKeyLogLen] + "..."
					}
					log.Printf("[gc] disk=%d CAS-fail  key=%q  old_offset=%d  cause=concurrent-put-won  orphan-at=%d  will-retry-next-pass",
						diskIdx, k, r.oldOffset, r.newOffset)
				} else if casLogCount == gcCASLogMax {
					casLogCount++
					log.Printf("[gc] disk=%d CAS-fail  (further samples suppressed for this pass)", diskIdx)
				}
			}
		}

		// CAS backpressure: if >20% of this batch failed, writes are racing GC
		// faster than GC can commit.  Signal Put() to add 1ms delay for 200ms so
		// the next batch has a quieter window to win its CAS operations.
		// Lighter than the read-latency throttle (1ms vs 2ms, time-bounded).
		if batchCASFails > len(batch)/5 {
			const gcCASBackpressureDur = 200 * time.Millisecond
			until := time.Now().Add(gcCASBackpressureDur).UnixNano()
			d.metrics.Admission.GCBackpressureUntilNs.Store(until)
			log.Printf("[gc] disk=%d CAS-backpressure  %d/%d records failed CAS this batch  writes delayed 1ms for %.0fms",
				diskIdx, batchCASFails, len(batch), gcCASBackpressureDur.Seconds()*1000)
		}

		batch = batch[:0]
	}

	for idx, cOrig := range candidates {
		select {
		case <-d.done:
			flushBatch() // preserve work done so far
			log.Printf("[gc] disk=%d interrupted by shutdown  reclaimed_so_far=%.1fMB",
				diskIdx, float64(reclaimedBytes)/(1<<20))
			return
		default:
		}

		// Yield to the scheduler every gcGoschedEvery candidates instead of on
		// every single one.  The old per-candidate Gosched added millions of
		// unnecessary scheduler round-trips on large candidate sets.
		if idx%gcGoschedEvery == 0 && idx > 0 {
			runtime.Gosched()
		}

		c := cOrig

		// GC-aware tiering: if this entry has been demoted to the cold tier by
		// TierManager, skip relocating it back to the hot VLog.  The entry's
		// current DiskOffset is a tombstoned hot-tier record that should be
		// reclaimed; its live copy lives in the cold tier.
		{
			entry, _, exists := d.index.get(c.key)
			if exists && entry.Flags&FlagTiered != 0 {
				vl.MarkDead(c.valueSize, c.packed)
				deadSkipped++
				continue
			}
		}

		// Speculative pre-check (read-lock, no I/O): if the index no longer
		// points to c.diskOffset a concurrent Put has already moved this key.
		// One refresh attempt: if the new offset is still below the GC horizon
		// we can compact it now; if above, defer to the next pass.
		if !d.index.checkVLogOffset(c.key, c.diskOffset) {
			d.metrics.VLogGCCASFails.Add(1)
			if fresh, ok := d.index.getVLogEntryIfBelow(c.key, gcHorizon); ok {
				c = fresh // key moved but still below horizon — compact from new offset
				if !d.index.checkVLogOffset(c.key, c.diskOffset) {
					// Moved again between the two checks — confirmed dead, no relocation needed.
					d.metrics.VLogGCCASFails.Add(1)
					vl.MarkDead(c.valueSize, c.packed)
					deadSkipped++
					continue
				}
			} else {
				// Key is deleted/tombstoned or overwritten above the GC horizon.
				// The record at c.diskOffset is confirmed dead — declare it garbage
				// immediately; punch-hole will reclaim the space when the watermark advances.
				vl.MarkDead(c.valueSize, c.packed)
				deadSkipped++
				continue
			}
		}

		// Rate-limit GC writes per disk using its own token bucket. Cap scales
		// with garbage ratio: 60→200 MB/s at critical, unlimited at emergency.
		d.limiters[diskIdx].consume(vlogBlockSize, d.gcRateBPS(diskIdx, gcRatio))

		// Read the live value at the current VLog offset.
		value, err := vl.ReadValue(int64(c.diskOffset), c.valueSize)
		if err != nil {
			d.metrics.VLogGCReadErrors.Add(1)
			deferReadErr++
			continue
		}

		// Stage: reserve a new offset and buffer the record.  No disk I/O yet —
		// the fdatasync happens only at Flush, shared across gcBatchSize records.
		newOffset, err := batcher.Stage(value)
		if err != nil {
			deferIOErr++
			continue
		}

		batch = append(batch, gcReloc{
			key:       c.key,
			oldOffset: c.diskOffset,
			newOffset: newOffset,
			oldPacked: c.packed,
			newPacked: true, // VLogBatcher packs all relocated records

			valueSize: c.valueSize,
		})

		if len(batch) >= gcBatchSize {
			flushBatch()
		}
	}

	// Flush any records that didn't fill a complete batch.
	flushBatch()

	if reclaimedBytes > 0 {
		d.metrics.VLogGCBytes.Add(uint64(reclaimedBytes))
	}

	totalDeferred := deadSkipped + deferPostCAS + deferReadErr + deferIOErr
	relocated := len(candidates) - totalDeferred
	elapsed := time.Since(startTime)
	casFailsThisPass := d.metrics.VLogGCCASFails.Load() - casFailsBefore
	log.Printf(
		"[gc] disk=%d pass done  relocated=%d  skipped=%d (dead_skip=%d post_cas=%d read_err=%d io_err=%d)"+
			"  reclaimed=%.1fMB  cas_fails=%d  elapsed=%s",
		diskIdx, relocated, totalDeferred,
		deadSkipped, deferPostCAS, deferReadErr, deferIOErr,
		float64(reclaimedBytes)/(1<<20), casFailsThisPass,
		elapsed.Round(time.Millisecond),
	)

	// Physical space reclamation: punch out the dead head of the VLog file.
	// All live entries have been relocated above minLiveOffset, so the blocks
	// below that point are safely freeable.  This is the only step that
	// actually reduces disk usage — without it GC only grows the VLog tail.
	minLive := d.index.minLiveVLogOffset(diskIdx, d.numDisks)
	wmBefore := vl.punchWatermark.Load()
	if minLive > 0 && int64(minLive) > wmBefore {
		if err := vl.punchDeadHead(int64(minLive)); err != nil {
			d.metrics.VLogBlkDiscardErrors.Add(1)
			log.Printf("[gc] disk=%d BLKDISCARD error (NVMe TRIM disabled — add SYS_RAWIO capability): %v",
				diskIdx, err)
		} else {
			wmAfter := vl.punchWatermark.Load()
			if wmAfter > wmBefore {
				log.Printf("[gc] disk=%d punch-hole  freed=%.1fMB  watermark=%.2fGB",
					diskIdx, float64(wmAfter-wmBefore)/(1<<20), float64(wmAfter)/(1<<30))
			}
		}
	}
}

// reapExpiredTombstones walks all shards and physically deletes tombstone
// entries that have outlived the GC grace period.
//
// Safety: grace period ≥ max replica lag.  Any replica that still holds a
// live copy of a tombstoned key will receive the delete via anti-entropy or
// Read Repair before the tombstone is reaped here.
func (d *Defragmenter) reapExpiredTombstones() {
	gracePeriodUs := d.config.GCGracePeriodSec * 1_000_000
	expired := d.index.expiredTombstones(gracePeriodUs)

	if len(expired) == 0 {
		return
	}

	log.Printf("[gc] tombstones  found=%d expired  grace=%ds  reaping...",
		len(expired), d.config.GCGracePeriodSec)

	var reaped int
	for _, key := range expired {
		// Double-check under write lock: a concurrent write may have revived
		// the key between the scan and now.
		entry, _, exists := d.index.get(key)
		if !exists || !entry.IsTombstone() {
			continue // key was revived by a concurrent Put; skip
		}

		// Ensure the grace period still holds (clock may have skipped).
		nowUs := time.Now().UnixMicro()
		if nowUs-entry.WriteTimestampUs < gracePeriodUs {
			continue
		}

		d.index.removeTombstone(key)
		d.cache.Evict(key)
		d.metrics.TombstonesCollected.Add(1)
		d.metrics.EvictionMetrics.DefragEvictions.Add(1)
		d.metrics.EvictionMetrics.TotalEvictions.Add(1)
		reaped++

		// Yield between deletions so shard locks don't starve readers.
		runtime.Gosched()
	}

	log.Printf("[gc] tombstones  reaped=%d  skipped=%d (revived or grace not elapsed)",
		reaped, len(expired)-reaped)
}

// ── Segment scoring (stubbed; wired when SegmentManager is ready) ─────────────

// segmentScore holds the result of scoring a segment file for compaction.
type segmentScore struct {
	segmentID    uint32
	totalBytes   uint64
	liveBytes    uint64
	deadBytes    uint64
	garbageRatio float64
}

// scoreSegment computes the garbage ratio for a hypothetical segment.
func scoreSegment(
	si *shardedIndex,
	segID uint32,
	gracePeriodUs int64,
) segmentScore {
	_ = si
	_ = segID
	_ = gracePeriodUs
	return segmentScore{segmentID: segID}
}
