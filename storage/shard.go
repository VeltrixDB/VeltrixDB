package storage

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// numShards is fixed at a power-of-2 so shard selection is a single AND.
// 8192 shards give an 8192-way reduction in lock contention on a 64-core host
// (128× per-core headroom) while distributing evenly across 8 NVMe disks
// (1024 shards per disk).  At 200M keys: ~24K entries/shard × ~80 B ≈ 1.9 MB —
// fits in L3 cache and avoids the cache-miss throughput decay seen at 1024 shards.
// Bit mask: shard = h & 0x1FFF.
const numShards = 8192

// indexShard is one partition of the Index Vault.  Each shard has its own
// RWMutex so reads across shards are fully parallel and writes only serialise
// within the same shard.
//
// `bloom` is optional and lock-free.  When non-nil, Get checks it BEFORE the
// shard RLock — a definitive miss avoids the lock and the map lookup entirely.
// The filter never returns false for keys that were Added; deletes leave bits
// set (false-positive accumulates), so the defragmenter periodically rebuilds
// the filter from the live index in vacuumBloomFilters().
type indexShard struct {
	mu          sync.RWMutex
	entries     map[string]*IndexEntry // key → metadata (64-byte cache-line entry)
	dirtyValues map[string][]byte      // key → value bytes, pre-segment-flush
	bloom       *shardBloom            // nil when blooms are disabled in config
}

func newIndexShard() indexShard {
	return indexShard{
		entries:     make(map[string]*IndexEntry),
		dirtyValues: make(map[string][]byte),
	}
}

// shardedIndex is the Index Vault: a permanent, fully RAM-resident mapping
// from every key to its 64-byte IndexEntry.  Sharding eliminates the global
// mutex that serialised every Put/Get in the original design.
type shardedIndex struct {
	shards     [numShards]indexShard
	dirtyCount atomic.Int64 // total unique keys currently in any shard.dirtyValues
	keyCount   atomic.Int64 // live (non-tombstone) key count; O(1) alternative to size()

	// ordered is the global ordered key view (ordered_index.go) behind
	// RangeScan / ScanCursor.  It is mutated inside the shard critical
	// sections below so "key live in entries ⇔ key in ordered" holds
	// whenever the shard lock is observable.  LOCK ORDER: indexShard.mu →
	// oiNode.mu, never the reverse — see the ordered_index.go file header.
	// nil when StorageConfig.DisableOrderedIndex is set.
	ordered *orderedKeyIndex
}

func newShardedIndex() *shardedIndex {
	si := &shardedIndex{ordered: newOrderedKeyIndex()}
	for i := range si.shards {
		si.shards[i] = newIndexShard()
	}
	return si
}

// installBlooms allocates one shardBloom per shard with the given bit budget
// and probe count k. Idempotent: safe to call once at engine construction
// after newShardedIndex(). Memory cost: 1024 × bitsPerShard / 8 bytes.
func (si *shardedIndex) installBlooms(bitsPerShard uint64, k uint8) {
	for i := range si.shards {
		si.shards[i].bloom = newShardBloom(bitsPerShard, k)
	}
	shardBloomEnabled.Store(true)
}

// shardFor returns the shard responsible for key.  The shard ID is derived
// from FNV-1a — the same hash family used by the cluster package — so key
// distribution is consistent across the system.
func (si *shardedIndex) shardFor(key string) (*indexShard, uint16) {
	h := fnv64a(key)
	id := uint16(h & (numShards - 1))
	return &si.shards[id], id
}

// get returns the IndexEntry and the dirty value (if still in memory) for key.
// The caller must NOT hold any shard lock.
//
// Bloom filter fast path: when shard.bloom is non-nil and reports "definitely
// not present", we return (nil, nil, false) without taking the shard RLock or
// doing the map[string]*IndexEntry lookup. At ~1 M entries/shard the map
// lookup is O(1) but cache-miss-heavy (~500 ns); the bloom check is ~50 ns
// pointer-chase-free. For workloads with many negative lookups this is a 10×
// reduction in the read floor.
func (si *shardedIndex) get(key string) (*IndexEntry, []byte, bool) {
	shard, _ := si.shardFor(key)
	if b := shard.bloom; b != nil && !b.MayContain(fnv64a(key)) {
		return nil, nil, false
	}
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, ok := shard.entries[key]
	if !ok {
		return nil, nil, false
	}
	// Return a copy taken under the RLock, not the shared pointer. Callers read
	// entry fields (IsTombstone/Flags/DiskOffset/...) after get() returns and the
	// lock is released; the in-place mutators (markTombstone, markTiered, defrag
	// relocation) write those same fields under the write lock. Handing back the
	// shared pointer let a reader observe a field mid-mutation — a real data race
	// (caught by -race on concurrent Get vs Delete). The dirty value slice is
	// immutable once stored (replaced wholesale, never mutated), so sharing it is
	// safe. IndexEntry is 64 bytes and this path is cache-miss-only, so the copy
	// is negligible next to the NVMe read that follows.
	snap := *entry
	val := shard.dirtyValues[key]
	return &snap, val, true
}

// put writes or overwrites the IndexEntry and its dirty value for key.
func (si *shardedIndex) put(key string, entry *IndexEntry, value []byte) {
	shard, _ := si.shardFor(key)
	if b := shard.bloom; b != nil {
		b.Add(fnv64a(key))
	}
	shard.mu.Lock()
	old, hadOld := shard.entries[key]
	shard.entries[key] = entry
	if si.ordered != nil {
		si.ordered.Insert(key)
	}
	if !hadOld || old.IsTombstone() {
		si.keyCount.Add(1)
	}
	if value != nil {
		_, hadDirty := shard.dirtyValues[key]
		if !hadDirty {
			si.dirtyCount.Add(1)
		}
		shard.dirtyValues[key] = value
	}
	shard.mu.Unlock()
}

// replayPut writes entry only when no newer live entry already exists.
// Used exclusively by background WAL replay so concurrent Puts (which call
// the plain put()) always win over older replayed data.
func (si *shardedIndex) replayPut(key string, entry *IndexEntry, value []byte) {
	shard, _ := si.shardFor(key)
	if b := shard.bloom; b != nil {
		b.Add(fnv64a(key))
	}
	shard.mu.Lock()
	existing, hadExisting := shard.entries[key]
	if hadExisting && existing.WriteTimestampUs > entry.WriteTimestampUs {
		shard.mu.Unlock()
		return // live write during replay takes precedence — don't clobber it
	}
	shard.entries[key] = entry
	if si.ordered != nil {
		si.ordered.Insert(key)
	}
	if !hadExisting || existing.IsTombstone() {
		si.keyCount.Add(1)
	}
	if value != nil {
		_, hadDirty := shard.dirtyValues[key]
		if !hadDirty {
			si.dirtyCount.Add(1)
		}
		shard.dirtyValues[key] = value
	}
	shard.mu.Unlock()
}

// replayMarkTombstone is the version-aware tombstone setter used by background
// WAL replay.  Skips if a live write arrived after the tombstone timestamp.
func (si *shardedIndex) replayMarkTombstone(key string, nowUs int64) {
	shard, _ := si.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry, ok := shard.entries[key]
	if !ok {
		return
	}
	if entry.WriteTimestampUs > nowUs {
		return // live write supersedes this tombstone
	}
	if !entry.IsTombstone() {
		si.keyCount.Add(-1)
	}
	entry.MarkTombstone(nowUs)
	if si.ordered != nil {
		si.ordered.Remove(key)
	}
	_, hadDirty := shard.dirtyValues[key]
	if hadDirty {
		si.dirtyCount.Add(-1)
	}
	delete(shard.dirtyValues, key)
}

// markTombstone atomically sets the tombstone flag and clears the dirty value.
// Returns false if the key does not exist.
func (si *shardedIndex) markTombstone(key string, nowUs int64) bool {
	shard, _ := si.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.entries[key]
	if !ok {
		return false
	}
	if !entry.IsTombstone() {
		si.keyCount.Add(-1)
	}
	entry.MarkTombstone(nowUs)
	if si.ordered != nil {
		si.ordered.Remove(key)
	}
	_, hadDirty := shard.dirtyValues[key]
	if hadDirty {
		si.dirtyCount.Add(-1)
	}
	delete(shard.dirtyValues, key)
	return true
}

// markTiered sets FlagTiered on an IndexEntry after the TierManager has
// successfully demoted its value to the cold tier.  Subsequent Gets will serve
// the value from the cold tier; defrag.go's GC will skip relocating the entry
// back to the hot VLog.
func (si *shardedIndex) markTiered(key string) bool {
	shard, _ := si.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry, ok := shard.entries[key]
	if !ok || entry.IsTombstone() {
		return false
	}
	entry.Flags |= FlagTiered
	return true
}

// removeTombstone deletes an entry from the index after its GC grace period
// has elapsed.  Called exclusively by the Defragmenter.
func (si *shardedIndex) removeTombstone(key string) {
	shard, _ := si.shardFor(key)
	shard.mu.Lock()
	if si.ordered != nil {
		si.ordered.Remove(key) // no-op normally: markTombstone already removed it
	}
	_, hadDirty := shard.dirtyValues[key]
	if hadDirty {
		si.dirtyCount.Add(-1)
	}
	delete(shard.entries, key)
	delete(shard.dirtyValues, key)
	shard.mu.Unlock()
}

// clearDirty marks a key's value as flushed to disk (removes from dirtyValues).
func (si *shardedIndex) clearDirty(key string) {
	shard, _ := si.shardFor(key)
	shard.mu.Lock()
	_, had := shard.dirtyValues[key]
	if had {
		si.dirtyCount.Add(-1)
	}
	delete(shard.dirtyValues, key)
	shard.mu.Unlock()
}

// expiredTombstones returns all keys whose tombstone has aged past gracePeriodUs.
// It locks each shard only long enough to collect keys, then releases it, so
// ongoing reads are not blocked during the full scan.
func (si *shardedIndex) expiredTombstones(gracePeriodUs int64) []string {
	nowUs := time.Now().UnixMicro()
	cutoff := nowUs - gracePeriodUs

	var expired []string
	for i := range si.shards {
		shard := &si.shards[i]
		shard.mu.RLock()
		for key, entry := range shard.entries {
			if entry.IsTombstone() && entry.WriteTimestampUs < cutoff {
				expired = append(expired, key)
			}
		}
		shard.mu.RUnlock()
	}
	return expired
}

// expiredTTL returns all keys whose TTL has elapsed and that are not already
// tombstoned.  Called by the background TTL scanner.
func (si *shardedIndex) expiredTTL() []string {
	nowUs := time.Now().UnixMicro()

	var expired []string
	for i := range si.shards {
		shard := &si.shards[i]
		shard.mu.RLock()
		for key, entry := range shard.entries {
			if !entry.IsTombstone() && entry.IsExpired(nowUs) {
				expired = append(expired, key)
			}
		}
		shard.mu.RUnlock()
	}
	return expired
}

// dirtyKeys returns all keys that still have dirty (unflushed) values.
func (si *shardedIndex) dirtyKeys() []string {
	var dirty []string
	for i := range si.shards {
		shard := &si.shards[i]
		shard.mu.RLock()
		for key := range shard.dirtyValues {
			dirty = append(dirty, key)
		}
		shard.mu.RUnlock()
	}
	return dirty
}

// vlogGCEntry is a snapshot of the IndexEntry fields needed by the VLog compactor.
// Copying only the needed fields avoids holding a shard lock during VLog I/O.
type vlogGCEntry struct {
	key              string
	diskOffset       uint64
	valueSize        uint32
	writeTimestampUs int64 // for hot-key filtering and LRW sort ordering
	packed           bool  // true when the record shares its 4 KB block with others
}

// vlogCandidates returns live VLog entries on disk diskIdx whose DiskOffset is
// below gcHorizon (the VLog end-offset captured at the start of a GC pass).
// Only entries in shards that route to diskIdx (shardID % numDisks == diskIdx)
// are included.  Each shard is locked only long enough to collect the snapshot.
func (si *shardedIndex) vlogCandidates(diskIdx, numDisks int, gcHorizon uint64) []vlogGCEntry {
	var candidates []vlogGCEntry
	for i := diskIdx; i < numShards; i += numDisks {
		shard := &si.shards[i]
		shard.mu.RLock()
		for key, entry := range shard.entries {
			if !entry.IsTombstone() && entry.DiskOffset > 0 && entry.DiskOffset < gcHorizon {
				candidates = append(candidates, vlogGCEntry{
					key:              key,
					diskOffset:       entry.DiskOffset,
					valueSize:        entry.ValueSize,
					writeTimestampUs: entry.WriteTimestampUs,
					packed:           entry.IsPacked(),
				})
			}
		}
		shard.mu.RUnlock()
	}
	return candidates
}

// maxVLogEndOffset returns one byte past the last byte occupied by any
// (live OR tombstoned) index entry on diskIdx.  Used at startup — particularly
// for raw block-device VLog mode where Stat().Size() returns 0 and there is
// no other way to know how far prior writes advanced the VLog.  After WAL
// replay rebuilds the index, this gives the highest byte position used so
// vl.end can be advanced past it; the next Append will then land safely
// above all existing data instead of overwriting it.
//
// Tombstones are included because the WAL stores tombstone records that may
// still occupy VLog space (legacy 6-field WAL with inline value bytes that
// were re-appended on replay). When in doubt, we round up — never down.
//
// alignedSize must match the encoding used by VLog.beginAppend:
//   alignedLen = (vlogHeaderBytes + valueSize + vlogBlockSize-1) &^ (vlogBlockSize-1)
//
// Returns 0 when no entries reference diskIdx (fresh device).
func (si *shardedIndex) maxVLogEndOffset(diskIdx, numDisks int) uint64 {
	var maxEnd uint64
	for i := diskIdx; i < numShards; i += numDisks {
		shard := &si.shards[i]
		shard.mu.RLock()
		for _, entry := range shard.entries {
			if entry.DiskOffset == 0 {
				continue
			}
			rawLen := uint64(vlogHeaderBytes) + uint64(entry.ValueSize)
			var end uint64
			if entry.IsPacked() {
				// Packed: the record occupies header+value bytes inside a
				// shared 4 KB block; round UP to the next 4 KB boundary so
				// the new vl.end lands past the entire shared block (no
				// other record can be split across the boundary).
				end = entry.DiskOffset + rawLen
				end = (end + uint64(vlogBlockSize) - 1) &^ uint64(vlogBlockSize-1)
			} else {
				// Unpacked: this record owns a full 4 KB block — round up.
				alignedLen := (rawLen + uint64(vlogBlockSize) - 1) &^ uint64(vlogBlockSize-1)
				end = entry.DiskOffset + alignedLen
			}
			if end > maxEnd {
				maxEnd = end
			}
		}
		shard.mu.RUnlock()
	}
	return maxEnd
}

// minLiveVLogOffset returns the lowest DiskOffset among all non-tombstone entries
// assigned to diskIdx.  The GC compactor uses this as the safe punch watermark:
// all bytes below this offset are dead and can be freed via fallocate PUNCH_HOLE.
// Returns math.MaxUint64 if no live entries exist for this disk.
func (si *shardedIndex) minLiveVLogOffset(diskIdx, numDisks int) uint64 {
	min := uint64(1<<63 - 1) // math.MaxInt64 as uint64 sentinel
	for i := diskIdx; i < numShards; i += numDisks {
		shard := &si.shards[i]
		shard.mu.RLock()
		for _, entry := range shard.entries {
			if !entry.IsTombstone() && entry.DiskOffset > 0 && entry.DiskOffset < min {
				min = entry.DiskOffset
			}
		}
		shard.mu.RUnlock()
	}
	return min
}

// getVLogEntryIfBelow returns the current live VLog entry for key if its
// DiskOffset is still strictly below gcHorizon.  Used by the GC retry loop: when
// a pre-check or CAS fails because a concurrent Put changed the offset, this call
// checks whether the new location is still within the compaction window so GC can
// immediately retry from the fresh offset instead of deferring to the next pass.
func (si *shardedIndex) getVLogEntryIfBelow(key string, gcHorizon uint64) (vlogGCEntry, bool) {
	shard, _ := si.shardFor(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	entry, ok := shard.entries[key]
	if !ok || entry.IsTombstone() || entry.DiskOffset == 0 || entry.DiskOffset >= gcHorizon {
		return vlogGCEntry{}, false
	}
	return vlogGCEntry{
		key:        key,
		diskOffset: entry.DiskOffset,
		valueSize:  entry.ValueSize,
		packed:     entry.IsPacked(),
	}, true
}

// checkVLogOffset returns true if the entry for key still has expectedOffset as
// its DiskOffset.  Used by the GC compactor as a cheap speculative pre-check
// before doing the expensive VLog write: if this returns false the candidate is
// already stale and the write can be skipped entirely.
func (si *shardedIndex) checkVLogOffset(key string, expectedOffset uint64) bool {
	shard, _ := si.shardFor(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	entry, ok := shard.entries[key]
	return ok && !entry.IsTombstone() && entry.DiskOffset == expectedOffset
}

// updateVLogOffset CAS-updates DiskOffset for key from oldOffset to newOffset
// under the shard write lock.  Returns false if a concurrent Put or Delete has
// already changed the entry (stale GC candidate — safe to discard).
//
// newPacked sets FlagPacked on the updated entry. GC relocations always go
// through the packed VLogBatcher path, so callers from defrag.go pass true.
// The old FlagPacked bit (whatever it was before) is fully overwritten —
// after this call the entry's packed-ness reflects the relocation result.
func (si *shardedIndex) updateVLogOffset(key string, oldOffset, newOffset uint64, newPacked bool) bool {
	shard, _ := si.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry, ok := shard.entries[key]
	if !ok || entry.IsTombstone() || entry.DiskOffset != oldOffset {
		return false
	}
	entry.DiskOffset = newOffset
	if newPacked {
		entry.Flags |= FlagPacked
	} else {
		entry.Flags &^= FlagPacked
	}
	return true
}

// size returns the total number of live (non-tombstoned) entries across all shards.
func (si *shardedIndex) size() int {
	return int(si.keyCount.Load())
}

// fnv64a computes FNV-1a (64-bit) of key — same hash used by the cluster ring.
func fnv64a(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// vacuumBloomFilters rebuilds every shard's bloom from its live (non-tombstone)
// entries.  Drops accumulated bits left behind by deletes so the false-positive
// rate trends back toward the design point. Cheap (one walk over the map under
// RLock; bloom Reset+Add is in-memory only). Called by the defragmenter on
// every full GC pass.
//
// No-op when blooms are disabled.
func (si *shardedIndex) vacuumBloomFilters() {
	for i := range si.shards {
		shard := &si.shards[i]
		if shard.bloom == nil {
			continue
		}
		shard.mu.RLock()
		shard.bloom.Reset()
		for key, entry := range shard.entries {
			if entry.IsTombstone() {
				continue
			}
			shard.bloom.Add(fnv64a(key))
		}
		shard.mu.RUnlock()
	}
}
