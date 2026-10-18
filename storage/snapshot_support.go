package storage

// snapshot_support.go — engine primitives for raft state-machine snapshots
// (cmd/server/raft_fsm.go, consensus/snapshot.go; CLAUDE.md invariant 64).
//
// Capture (leader or follower, while Apply keeps running):
//
//   - WriteTracker records every key whose index entry changes.  The note is
//     taken AFTER the change is installed (shard.go noteWrite), so "drain the
//     tracker, then read the key" always observes that change or a later one.
//   - SnapshotIterator walks the whole keyspace (ascending key order when the
//     ordered index exists, shard order otherwise) and reads each key with
//     ReadForSnapshot.
//   - ReadForSnapshot reads one key from the index + VLog/segment, bypassing
//     the LIRS cache: the cache is refreshed after the index on every write,
//     so a cached read could return the value a tracked write just replaced.
//     It never fills the cache either (a snapshot must not evict the working
//     set).
//
// Restore (follower install, and every startup that has a snapshot):
//
//   - KeysAfter pages through the keys of the ordered index, so the caller can
//     merge-join the engine's keyspace against a sorted snapshot in O(1) RAM.
//   - Restorer applies raw puts (absolute expiry, no derived writes) and raw
//     deletes in batches, then rebuilds the search RAM indexes from the
//     restored keys when any search key changed, and replaces the secondary
//     index definitions.  Raw because the snapshot holds every derived key
//     ("@idx/…", "@vec/…", "@txt/…") explicitly: re-deriving them would write
//     keys the snapshot does not contain, and the invariant-60 cascade would
//     delete keys it does contain.

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ── capture ─────────────────────────────────────────────────────────────────

// WriteTracker collects the keys written (put, delete, expiry tombstone,
// atomic op, WAL replay) since it was started or last drained.
type WriteTracker struct {
	se       *StorageEngine
	mu       sync.Mutex
	keys     map[string]struct{}
	limit    int
	overflow bool
	closed   bool
}

// ErrTrackerActive is returned by TrackWrites while another tracker runs.
var ErrTrackerActive = errors.New("a write tracker is already active on this engine")

// TrackWrites starts recording written keys.  At most one tracker is active
// per engine.  limit bounds the number of distinct keys held between drains
// (0 = unbounded); past it the tracker stops recording and reports overflow.
func (se *StorageEngine) TrackWrites(limit int) (*WriteTracker, error) {
	t := &WriteTracker{se: se, keys: make(map[string]struct{}), limit: limit}
	if !se.index.tracker.CompareAndSwap(nil, t) {
		return nil, ErrTrackerActive
	}
	return t, nil
}

func (t *WriteTracker) note(key string) {
	t.mu.Lock()
	if !t.closed && !t.overflow {
		if t.limit > 0 && len(t.keys) >= t.limit {
			if _, ok := t.keys[key]; !ok {
				t.overflow = true
				t.keys = nil
			}
		} else {
			t.keys[key] = struct{}{}
		}
	}
	t.mu.Unlock()
}

// Drain returns the keys written since the previous Drain (or the start) and
// starts a new set.  overflow=true means the tracker dropped keys: the set is
// incomplete and the capture relying on it must not be used.
func (t *WriteTracker) Drain() (keys []string, overflow bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.overflow {
		return nil, true
	}
	keys = make([]string, 0, len(t.keys))
	for k := range t.keys {
		keys = append(keys, k)
	}
	t.keys = make(map[string]struct{})
	return keys, false
}

// Close stops the tracker (idempotent).
func (t *WriteTracker) Close() {
	t.se.index.tracker.CompareAndSwap(t, nil)
	t.mu.Lock()
	t.closed = true
	t.keys = nil
	t.mu.Unlock()
}

// SnapshotKV is one live key with its value, absolute expiry (Unix µs,
// 0 = immortal) and write timestamp — its version token (raft_clock.go;
// 0 = unknown: a snapshot format without it).
type SnapshotKV struct {
	Key      string
	Value    []byte
	ExpiryUs int64
	StampUs  int64
}

// ReadForSnapshot returns key's current value and expiry, ok=false when the
// key is absent, tombstoned or expired.  It reads the index and the VLog /
// segment directly: no cache lookup (see the file comment), no cache fill,
// and an expired key is reported absent without being tombstoned.
func (se *StorageEngine) ReadForSnapshot(key string) (SnapshotKV, bool, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		h := fnv64a(key)
		entry, dirty, exists := se.index.getHashed(key, h)
		if !exists || entry.IsTombstone() {
			return SnapshotKV{}, false, nil
		}
		// Left out only when expired at the raft clock (raft_clock.go): the
		// snapshot must hold every key a later command may still see live.
		if entry.IsExpired(se.expiryCutoffUs()) {
			return SnapshotKV{}, false, nil
		}
		kv := SnapshotKV{Key: key, ExpiryUs: entry.CacheExpiryUs(), StampUs: entry.WriteTimestampUs}
		if dirty != nil {
			kv.Value = dirty
			return kv, true, nil
		}
		if entry.DiskOffset == 0 {
			// Same as Get: an entry with neither a RAM value nor a disk
			// location is not readable, i.e. absent.
			return SnapshotKV{}, false, nil
		}
		var (
			val []byte
			err error
		)
		if se.config.KeyValueSeparation && len(se.vlogs) > 0 {
			vlogIdx := int(entry.SegmentID) % len(se.vlogs)
			val, err = se.vlogs[vlogIdx].ReadValue(int64(entry.DiskOffset), entry.ValueSize)
			if err == nil {
				val, err = decodeStoredValue(&entry, val)
			}
		} else if int(entry.SegmentID) < len(se.segments) {
			sw := se.segments[entry.SegmentID]
			val, err = sw.ReadValue(int64(entry.DiskOffset), entry.KeySize, entry.ValueSize)
			if err == nil && computeCRC32C(val) != entry.CRC32C {
				err = fmt.Errorf("CRC32C mismatch for key %q", key)
			}
		} else {
			return SnapshotKV{}, false, nil
		}
		if err == nil {
			if val == nil {
				val = emptyValue
			}
			kv.Value = val
			return kv, true, nil
		}
		// The value may have been relocated by VLog GC between the index
		// read and the disk read: retry while the entry keeps changing.
		lastErr = err
		again, _, ok := se.index.getHashed(key, h)
		if !ok || again.IsTombstone() || again.DiskOffset != entry.DiskOffset || again.SegmentID != entry.SegmentID {
			continue
		}
		break
	}
	return SnapshotKV{}, false, fmt.Errorf("snapshot read %q: %w", key, lastErr)
}

// OrderedIndexEnabled reports whether the ordered key index exists (it does
// unless StorageConfig.DisableOrderedIndex / --no-ordered-index).
func (se *StorageEngine) OrderedIndexEnabled() bool { return se.index.ordered != nil }

// SnapshotIterator walks every live key of the engine.  Keys written while it
// runs may or may not be returned (pair it with a WriteTracker); a key that
// exists and is not written for the whole walk is returned exactly once, with
// its value.
type SnapshotIterator struct {
	se      *StorageEngine
	sorted  bool
	cursor  string
	started bool
	shard   int
	done    bool
}

// NewSnapshotIterator starts a walk.  Sorted() reports whether keys come in
// ascending byte order (ordered index present) or shard order.
func (se *StorageEngine) NewSnapshotIterator() *SnapshotIterator {
	return &SnapshotIterator{se: se, sorted: se.index.ordered != nil}
}

// Sorted reports whether Next returns keys in strictly ascending byte order.
func (it *SnapshotIterator) Sorted() bool { return it.sorted }

// Next returns the next batch of live keys (about max of them; a whole shard
// in shard order), or nil when the walk is complete.
func (it *SnapshotIterator) Next(max int) ([]SnapshotKV, error) {
	if max <= 0 {
		max = 1024
	}
	for !it.done {
		var keys []string
		if it.sorted {
			keys = it.se.keysAfter(it.cursor, it.started, max)
			it.started = true
			if len(keys) < max {
				it.done = true
			}
			if len(keys) > 0 {
				it.cursor = keys[len(keys)-1]
			}
		} else {
			if it.shard >= numShards {
				it.done = true
				break
			}
			keys = it.se.shardKeys(it.shard)
			it.shard++
		}
		out := make([]SnapshotKV, 0, len(keys))
		for _, k := range keys {
			kv, ok, err := it.se.ReadForSnapshot(k)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, kv)
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	return nil, nil
}

// keysAfter returns up to limit keys of the ordered index, ascending, that
// are > cursor (>= cursor when !exclusive).  Includes keys whose TTL elapsed
// but that were not tombstoned yet.
func (se *StorageEngine) keysAfter(cursor string, exclusive bool, limit int) []string {
	oi := se.index.ordered
	if oi == nil {
		return nil
	}
	var out []string
	oi.ascend(cursor, "", func(key string) bool {
		if exclusive && key == cursor {
			return true
		}
		out = append(out, key)
		return limit <= 0 || len(out) < limit
	})
	return out
}

// shardKeys returns the live keys of one index shard (expiry judged as in
// ReadForSnapshot).
func (se *StorageEngine) shardKeys(i int) []string {
	nowUs := se.expiryCutoffUs()
	shard := &se.index.shards[i]
	var keys []string
	shard.mu.RLock()
	shard.entries.rangeAll(func(k string, entry *IndexEntry) bool {
		if !entry.IsTombstone() && !entry.IsExpired(nowUs) {
			keys = append(keys, k)
		}
		return true
	})
	shard.mu.RUnlock()
	return keys
}

// RestoreKeys returns every key with an index entry that is not a tombstone,
// expired or not — the engine side of a restore without the ordered index
// (KeysAfter, its counterpart, includes unreaped expired keys too): a key
// the snapshot does not hold is deleted whatever its TTL, so whether it was
// already expired by this node's clock decides nothing.
func (se *StorageEngine) RestoreKeys() []string {
	var keys []string
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		shard.entries.rangeAll(func(k string, entry *IndexEntry) bool {
			if !entry.IsTombstone() {
				keys = append(keys, k)
			}
			return true
		})
		shard.mu.RUnlock()
	}
	return keys
}

// KeysAfter pages through the ordered index: up to limit keys > cursor in
// ascending byte order ("" starts at the first key).  Keys whose TTL elapsed
// but that were not reaped yet are included.  ErrOrderedIndexDisabled
// without the ordered index.
func (se *StorageEngine) KeysAfter(cursor string, limit int) ([]string, error) {
	if se.index.ordered == nil {
		return nil, ErrOrderedIndexDisabled
	}
	return se.keysAfter(cursor, cursor != "", limit), nil
}

// ── restore ─────────────────────────────────────────────────────────────────

const (
	restoreBatchKeys  = 1024
	restoreBatchBytes = 4 << 20
	restoreDelWorkers = 16
)

// Restorer applies raw writes in batches (see the file comment).  Not safe
// for concurrent use.  Writes to one key keep their call order.
type Restorer struct {
	se       *StorageEngine
	puts     []MultiPutRequest
	exps     []int64
	times    []ApplyTime // per put: StampUs = the key's version (raft_clock.go)
	putBytes int
	nowUs    int64 // SetExpiryNow; 0 = the local clock
	dels     []string
	inBatch  map[string]struct{}

	searchTouched bool
	Puts, Deletes int // applied so far
}

// NewRestorer returns a Restorer for se.
func (se *StorageEngine) NewRestorer() *Restorer {
	return &Restorer{se: se, inBatch: make(map[string]struct{})}
}

func (r *Restorer) touch(key string) error {
	if _, dup := r.inBatch[key]; dup {
		if err := r.Flush(); err != nil {
			return err
		}
	}
	r.inBatch[key] = struct{}{}
	if isSearchKey(key) {
		r.searchTouched = true
	}
	return nil
}

// SetExpiryNow makes Put judge expiry at nowUs (a snapshot's raft clock,
// raft_clock.go) instead of the local clock; 0 restores the local clock.
func (r *Restorer) SetExpiryNow(nowUs int64) { r.nowUs = nowUs }

// ExpiryNowUs is the time Put judges expiry at.
func (r *Restorer) ExpiryNowUs() int64 {
	if r.nowUs > 0 {
		return r.nowUs
	}
	return time.Now().UnixMicro()
}

// Put stores kv raw, with its absolute expiry and its write timestamp
// (kv.StampUs; unknown = the legacy version, raft_clock.go).  A key already
// expired is deleted instead.
func (r *Restorer) Put(kv SnapshotKV) error {
	if kv.Key == appliedMarkerKey {
		// A v1.16 snapshot holds the raft applied-index marker as a key (it
		// installed the WAL record as one); it is not state (raft_applied.go).
		return nil
	}
	if kv.ExpiryUs > 0 && kv.ExpiryUs <= r.ExpiryNowUs() {
		return r.Delete(kv.Key)
	}
	if err := r.touch(kv.Key); err != nil {
		return err
	}
	v := kv.Value
	if v == nil {
		v = []byte{}
	}
	r.puts = append(r.puts, MultiPutRequest{Key: kv.Key, Value: v, TTL: -1})
	r.exps = append(r.exps, kv.ExpiryUs)
	stamp := kv.StampUs
	if stamp <= 0 {
		stamp = legacyStampUs
	}
	r.times = append(r.times, ApplyTime{StampUs: stamp})
	r.putBytes += len(kv.Key) + len(v)
	if len(r.puts)+len(r.dels) >= restoreBatchKeys || r.putBytes >= restoreBatchBytes {
		return r.Flush()
	}
	return nil
}

// Delete removes key raw (no cascade, no index maintenance).
func (r *Restorer) Delete(key string) error {
	if err := r.touch(key); err != nil {
		return err
	}
	r.dels = append(r.dels, key)
	if len(r.puts)+len(r.dels) >= restoreBatchKeys {
		return r.Flush()
	}
	return nil
}

// Flush applies the buffered writes durably.
func (r *Restorer) Flush() error {
	var firstErr error
	if len(r.dels) > 0 {
		var mu sync.Mutex
		work := make(chan string)
		var wg sync.WaitGroup
		n := restoreDelWorkers
		if n > len(r.dels) {
			n = len(r.dels)
		}
		for w := 0; w < n; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := range work {
					if err := r.se.del(k, true); err != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = fmt.Errorf("restore delete %q: %w", k, err)
						}
						mu.Unlock()
					}
				}
			}()
		}
		for _, k := range r.dels {
			work <- k
		}
		close(work)
		wg.Wait()
		r.Deletes += len(r.dels)
	}
	if len(r.puts) > 0 {
		for i, err := range r.se.multiPut(r.puts, r.exps, r.times, true) {
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("restore put %q: %w", r.puts[i].Key, err)
			}
		}
		r.Puts += len(r.puts)
	}
	r.puts, r.exps, r.times, r.dels, r.putBytes = r.puts[:0], r.exps[:0], r.times[:0], r.dels[:0], 0
	for k := range r.inBatch {
		delete(r.inBatch, k)
	}
	return firstErr
}

// SearchTouched reports whether any search key ("@vec/", "@txt/", "@vecns/",
// "@idxdef/") was written or deleted.
func (r *Restorer) SearchTouched() bool { return r.searchTouched }

// Finish flushes, rebuilds the search RAM indexes from the restored keys when
// a search key changed (searches are refused meanwhile, invariant 57), and —
// when defs != nil — makes the secondary-index definitions exactly defs.
func (r *Restorer) Finish(defs []FieldIndexDef) error {
	if err := r.Flush(); err != nil {
		return err
	}
	if defs != nil {
		if err := r.se.ReplaceFieldIndexDefs(defs); err != nil {
			return err
		}
	}
	if r.searchTouched {
		if _, _, err := r.se.ResetSearchIndexes(); err != nil {
			return err
		}
	}
	return nil
}

// ResetSearchIndexes drops every in-RAM vector and text index and rebuilds
// them from the persisted reserved keys — the state a restart would build.
// Used after a raft snapshot restore replaced search keys wholesale.
func (se *StorageEngine) ResetSearchIndexes() (vectors, docs int, err error) {
	se.searchRebuildMu.Lock()
	defer se.searchRebuildMu.Unlock()
	se.BeginSearchRebuild()
	r := &se.vectors
	r.mu.Lock()
	for _, vi := range r.m {
		vi.retire()
	}
	r.m = map[string]*VectorIndex{}
	r.publishNamesLocked()
	r.mu.Unlock()
	t := &se.texts
	t.mu.Lock()
	t.m = map[string]*TextIndex{}
	empty := []string{}
	t.names.Store(&empty)
	t.mu.Unlock()
	return se.rebuildSearchIndexesLocked()
}

// ReplaceFieldIndexDefs makes the secondary-index definitions exactly defs:
// rules are registered / unregistered and the definitions file rewritten,
// but no "@idx/" key is written or deleted (the restored keyspace already
// holds exactly the entries defs describe).
func (se *StorageEngine) ReplaceFieldIndexDefs(defs []FieldIndexDef) error {
	se.fieldIdxMu.Lock()
	old := se.fieldIdxDefs
	se.fieldIdxDefs = append([]FieldIndexDef(nil), defs...)
	se.fieldIdxMu.Unlock()
	want := make(map[string]string, len(defs))
	for _, d := range defs {
		want[d.Name] = d.Field
	}
	for _, d := range old {
		if f, ok := want[d.Name]; !ok || f != d.Field {
			UnregisterIndexRule(d.Name)
		}
	}
	for _, d := range defs {
		RegisterIndexRule(IndexRule{Name: d.Name, Extract: makeFieldExtractor(d.Field)})
	}
	return se.saveFieldIndexDefs(defs)
}
