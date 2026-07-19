package storage

// ordered_index.go — global ordered key index + ordered range-scan APIs.
//
// The Index Vault (shard.go) is a hash map: O(1) point lookups but no key
// order, so prefix walks (ScanKeys / ScanNamespace) are O(N) over all 8192
// shards.  This file adds a second, keys-only view of the same keyspace: a
// concurrent skiplist that keeps every LIVE key in ascending byte order and
// powers RangeScan / ScanCursor in O(log N + limit) instead of O(N).
//
// Design notes:
//
//   - Keys only.  Values stay in the existing index/VLog.  A skiplist node
//     stores the key's string header (the underlying bytes are shared with
//     the map key — Go strings are immutable, so assignment copies only the
//     16 B header), an average of 1.33 next pointers (p = 0.25), one mutex
//     and two flag bytes ≈ 64 B/key of ordering overhead on top of what the
//     Index Vault already keeps resident.
//
//   - Concurrency: the lazy lock-based skiplist of Herlihy & Shavit ("A
//     Simple Optimistic Skiplist Algorithm").  Readers (find / range
//     iteration) are lock-free — they only load atomic next pointers and
//     skip nodes that are marked (logically deleted) or not yet fully
//     linked.  Writers lock only the predecessor nodes of the affected key,
//     validate, splice, and unlock — inserts/removes of disjoint keys
//     proceed in parallel.
//
//   - LOCK ORDER: indexShard.mu → oiNode.mu.  The ordered index is mutated
//     from inside shardedIndex critical sections (shard.go, atomic_ops.go)
//     while the shard write lock is held, so skiplist node locks nest
//     strictly INSIDE shard locks.  The skiplist itself never acquires a
//     shard lock, so no lock cycle — and therefore no deadlock — is
//     possible.  Scans traverse the skiplist without holding any lock and
//     acquire shard RLocks only per-key afterwards (via engine.Get), never
//     while holding a node lock.
//
//   - Consistency: mutating the skiplist under the same shard lock that
//     guards the primary map entry serialises same-key transitions, so the
//     invariant "key is live (non-tombstone) in shard.entries ⇔ key is in
//     the skiplist" holds whenever the shard lock is observable.  The one
//     deliberate exception: keys whose TTL elapsed but that have not been
//     tombstoned yet (lazy expiry) are still present here; scans skip them
//     via the per-key engine.Get liveness check, and that Get lazily
//     tombstones them — which removes them from this index too.

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// oiMaxLevel bounds the skiplist height.  With p = 0.25 the expected number
// of nodes at level L is n/4^L, so 24 levels comfortably cover 1 B+ keys.
const oiMaxLevel = 24

// oiNode is one skiplist node.  key is immutable after construction; next
// pointers are atomic so readers never lock.  marked = logically deleted
// (skipped by readers, unlinked shortly after); fullyLinked = linked at
// every level up to topLevel (readers ignore half-inserted nodes).
type oiNode struct {
	key         string
	mu          sync.Mutex
	marked      atomic.Bool
	fullyLinked atomic.Bool
	topLevel    int
	next        []atomic.Pointer[oiNode] // len == topLevel+1
}

// orderedKeyIndex is the global ordered view of all live keys.
type orderedKeyIndex struct {
	head *oiNode       // sentinel: conceptually -∞; nil next pointer = +∞
	seed atomic.Uint64 // lock-free PRNG state for randomLevel
	size atomic.Int64  // approximate live-key count (monitoring only)
}

func newOrderedKeyIndex() *orderedKeyIndex {
	return &orderedKeyIndex{
		head: &oiNode{
			topLevel: oiMaxLevel - 1,
			next:     make([]atomic.Pointer[oiNode], oiMaxLevel),
		},
	}
}

// randomLevel draws a geometric level (p = 0.25) from a lock-free splitmix
// mixer — math/rand's global lock would serialise all inserts.
func (oi *orderedKeyIndex) randomLevel() int {
	x := oi.seed.Add(0x9E3779B97F4A7C15) // Weyl sequence increment
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	level := 0
	for x&3 == 0 && level < oiMaxLevel-1 {
		level++
		x >>= 2
	}
	return level
}

// find fills preds/succs with the per-level predecessor and successor of key
// and returns the highest level at which key was found (-1 when absent).
// Lock-free: only atomic pointer loads.
func (oi *orderedKeyIndex) find(key string, preds, succs *[oiMaxLevel]*oiNode) int {
	lFound := -1
	pred := oi.head
	for level := oiMaxLevel - 1; level >= 0; level-- {
		curr := pred.next[level].Load()
		for curr != nil && curr.key < key {
			pred = curr
			curr = pred.next[level].Load()
		}
		if lFound == -1 && curr != nil && curr.key == key {
			lFound = level
		}
		preds[level] = pred
		succs[level] = curr
	}
	return lFound
}

// unlockPreds releases the distinct predecessor locks acquired up to
// highestLocked.  Consecutive levels may share one predecessor node; it was
// locked once, so it must be unlocked once.
func unlockPreds(preds *[oiMaxLevel]*oiNode, highestLocked int) {
	var prev *oiNode
	for level := 0; level <= highestLocked; level++ {
		if preds[level] != prev {
			preds[level].mu.Unlock()
			prev = preds[level]
		}
	}
}

// Insert adds key; returns false when it is already present.  Safe for
// concurrent use.  May be called while holding an indexShard write lock
// (see the LOCK ORDER note in the file header).
func (oi *orderedKeyIndex) Insert(key string) bool {
	topLevel := oi.randomLevel()
	var preds, succs [oiMaxLevel]*oiNode
	for {
		lFound := oi.find(key, &preds, &succs)
		if lFound != -1 {
			nodeFound := succs[lFound]
			if !nodeFound.marked.Load() {
				// Present.  Wait until fully linked so a caller that goes on
				// to Remove the key is guaranteed to see it.
				for !nodeFound.fullyLinked.Load() {
					runtime.Gosched()
				}
				return false
			}
			continue // marked for removal — retry once it is unlinked
		}

		// Lock the predecessors bottom-up and validate that nothing moved.
		highestLocked := -1
		var prevPred *oiNode
		valid := true
		for level := 0; valid && level <= topLevel; level++ {
			pred := preds[level]
			succ := succs[level]
			if pred != prevPred {
				pred.mu.Lock()
				highestLocked = level
				prevPred = pred
			}
			valid = !pred.marked.Load() && pred.next[level].Load() == succ &&
				(succ == nil || !succ.marked.Load())
		}
		if !valid {
			unlockPreds(&preds, highestLocked)
			continue // a neighbour changed underneath us — retry
		}

		n := &oiNode{
			key:      key,
			topLevel: topLevel,
			next:     make([]atomic.Pointer[oiNode], topLevel+1),
		}
		for level := 0; level <= topLevel; level++ {
			n.next[level].Store(succs[level])
		}
		for level := 0; level <= topLevel; level++ {
			preds[level].next[level].Store(n)
		}
		n.fullyLinked.Store(true)
		unlockPreds(&preds, highestLocked)
		oi.size.Add(1)
		return true
	}
}

// Remove deletes key; returns false when it is not present.  Safe for
// concurrent use.  Marks the node first (readers skip it immediately), then
// unlinks it under the predecessor locks.
func (oi *orderedKeyIndex) Remove(key string) bool {
	var victim *oiNode
	isMarked := false
	topLevel := -1
	var preds, succs [oiMaxLevel]*oiNode
	for {
		lFound := oi.find(key, &preds, &succs)
		if !isMarked {
			if lFound == -1 {
				return false
			}
			victim = succs[lFound]
			if !victim.fullyLinked.Load() || victim.topLevel != lFound || victim.marked.Load() {
				return false // half-inserted or already being removed
			}
			topLevel = victim.topLevel
			victim.mu.Lock()
			if victim.marked.Load() {
				victim.mu.Unlock()
				return false // another remover won the race
			}
			victim.marked.Store(true) // logical delete — readers skip from here on
			isMarked = true
		}

		highestLocked := -1
		var prevPred *oiNode
		valid := true
		for level := 0; valid && level <= topLevel; level++ {
			pred := preds[level]
			if pred != prevPred {
				pred.mu.Lock()
				highestLocked = level
				prevPred = pred
			}
			valid = !pred.marked.Load() && pred.next[level].Load() == victim
		}
		if !valid {
			unlockPreds(&preds, highestLocked)
			continue // predecessors moved — re-find and retry the unlink
		}

		for level := topLevel; level >= 0; level-- {
			preds[level].next[level].Store(victim.next[level].Load())
		}
		victim.mu.Unlock()
		unlockPreds(&preds, highestLocked)
		oi.size.Add(-1)
		return true
	}
}

// Len returns the approximate number of live keys (monitoring/tests only).
func (oi *orderedKeyIndex) Len() int { return int(oi.size.Load()) }

// contains reports whether key is currently present, linked, and unmarked.
// Diagnostic/test helper — scans use ascend/descend plus the primary-index
// liveness check instead.
func (oi *orderedKeyIndex) contains(key string) bool {
	pred := oi.head
	for level := oiMaxLevel - 1; level >= 0; level-- {
		curr := pred.next[level].Load()
		for curr != nil && curr.key < key {
			pred = curr
			curr = pred.next[level].Load()
		}
		if curr != nil && curr.key == key {
			return curr.fullyLinked.Load() && !curr.marked.Load()
		}
	}
	return false
}

// ascend calls fn for every key in [start, end) in ascending order until fn
// returns false.  end == "" means unbounded above.  Lock-free: an unlinked
// node's next pointer still leads back into the list, so an iterator parked
// on a concurrently-removed node simply continues past it.
func (oi *orderedKeyIndex) ascend(start, end string, fn func(key string) bool) {
	var preds, succs [oiMaxLevel]*oiNode
	oi.find(start, &preds, &succs)
	curr := succs[0] // first node with key >= start
	for curr != nil {
		if end != "" && curr.key >= end {
			return
		}
		if curr.fullyLinked.Load() && !curr.marked.Load() {
			if !fn(curr.key) {
				return
			}
		}
		curr = curr.next[0].Load()
	}
}

// seekLT returns the largest key strictly below bound (bound == "" means +∞,
// i.e. return the maximum key).  Lock-free O(log N) top-down search.
func (oi *orderedKeyIndex) seekLT(bound string) (string, bool) {
	pred := oi.head
	for level := oiMaxLevel - 1; level >= 0; level-- {
		curr := pred.next[level].Load()
		for curr != nil && (bound == "" || curr.key < bound) {
			pred = curr
			curr = pred.next[level].Load()
		}
	}
	if pred == oi.head {
		return "", false
	}
	return pred.key, true
}

// descend calls fn for every key in [start, end) in DESCENDING order until
// fn returns false.  end == "" means "from the maximum key".  The skiplist
// is singly linked, so each step is an O(log N) seekLT re-search — a reverse
// scan of L keys costs O(L·log N), which is what bounded reverse pagination
// needs.  The bound strictly decreases every step, so concurrent inserts or
// removes cannot livelock the walk.
func (oi *orderedKeyIndex) descend(start, end string, fn func(key string) bool) {
	bound := end
	for {
		k, ok := oi.seekLT(bound)
		if !ok || k < start {
			return
		}
		if !fn(k) {
			return
		}
		bound = k
	}
}

// ── Engine APIs ──────────────────────────────────────────────────────────────

// KV is one key-value pair returned by RangeScan / ScanCursor.
type KV struct {
	Key   string
	Value []byte
}

// ErrOrderedIndexDisabled is returned by RangeScan / ScanCursor when the
// engine was built with StorageConfig.DisableOrderedIndex = true.
var ErrOrderedIndexDisabled = errors.New("ordered index disabled (StorageConfig.DisableOrderedIndex)")

// scanLive returns the value for key if it is live right now, or ok=false
// when it is tombstoned, TTL-expired, or was reaped between the skiplist step
// and this check.  Liveness comes from the primary index entry — NOT from
// Get's cache-first path, which can serve a cached value for a key whose TTL
// has since elapsed.  The tombstone/TTL flags are read while the shard RLock
// is held (they are mutated in place under the shard write lock), and the
// RLock is released before any other lock or I/O — consistent with the LOCK
// ORDER note in the file header.  An expired key is lazily tombstoned here
// exactly as on the Get index path (which also removes it from this index).
func (se *StorageEngine) scanLive(key string) ([]byte, bool) {
	nowUs := time.Now().UnixMicro()
	shard, _ := se.index.shardFor(key)
	shard.mu.RLock()
	entry, exists := shard.entries[key]
	live := exists && !entry.IsTombstone()
	expired := live && entry.IsExpired(nowUs)
	shard.mu.RUnlock()
	if !live {
		return nil, false
	}
	if expired {
		se.index.markTombstone(key, nowUs)
		se.cache.Evict(key)
		return nil, false
	}
	val, err := se.Get(key)
	if err != nil {
		return nil, false // reaped between the liveness check and the read
	}
	return val, true
}

// RangeScan returns up to limit live key-value pairs with start ≤ key < end
// in ascending key order (descending when reverse=true, starting just below
// end).  end == "" means unbounded above; start == "" means from the lowest
// key; limit ≤ 0 means no limit.  Tombstoned and TTL-expired keys are
// skipped — an expired key encountered here is lazily tombstoned exactly as
// on the Get path.  Keys are in plain byte order; namespaced keys
// ("ns\x00key") sort grouped by namespace — see RangeScanNS.
//
// The result is a point-in-time-ish snapshot: keys written or deleted while
// the scan runs may or may not appear, but every returned pair was live at
// the moment its value was read.
func (se *StorageEngine) RangeScan(start, end string, limit int, reverse bool) ([]KV, error) {
	oi := se.index.ordered
	if oi == nil {
		return nil, ErrOrderedIndexDisabled
	}
	var out []KV
	collect := func(key string) bool {
		val, ok := se.scanLive(key)
		if !ok {
			return true // dead key — skip and keep scanning
		}
		out = append(out, KV{Key: key, Value: val})
		return limit <= 0 || len(out) < limit
	}
	if reverse {
		oi.descend(start, end, collect)
	} else {
		oi.ascend(start, end, collect)
	}
	return out, nil
}

// ScanCursor returns up to limit live key-value pairs with key > cursor in
// ascending order, plus the cursor for the next page ("" when the keyspace
// is exhausted).  Start pagination with cursor = "".  limit ≤ 0 disables the
// page bound (the whole tail is returned and the next cursor is "").
//
// Like ScanKeys, the walk covers the full internal keyspace, including
// namespaced ("ns\x00key") and hash-field ("key\x01field") internal keys.
func (se *StorageEngine) ScanCursor(cursor string, limit int) ([]KV, string, error) {
	oi := se.index.ordered
	if oi == nil {
		return nil, "", ErrOrderedIndexDisabled
	}
	var out []KV
	oi.ascend(cursor, "", func(key string) bool {
		if key == cursor {
			return true // cursor itself is exclusive
		}
		val, ok := se.scanLive(key)
		if !ok {
			return true // dead key — skip, see RangeScan
		}
		out = append(out, KV{Key: key, Value: val})
		return limit <= 0 || len(out) < limit
	})
	next := ""
	if limit > 0 && len(out) == limit {
		next = out[len(out)-1].Key
	}
	return out, next, nil
}

// RangeScanNS is RangeScan constrained to namespace ns: start/end bound the
// user-visible key (both may be ""), and returned keys have the "ns\x00"
// prefix stripped.  Works because the internal encoding "ns\x00key" keeps
// every namespace contiguous in byte order: all of ns sorts below ns+"\x01".
func (se *StorageEngine) RangeScanNS(ns, start, end string, limit int, reverse bool) ([]KV, error) {
	prefix := ns + nsSep
	internalEnd := prefix + end
	if end == "" {
		internalEnd = ns + "\x01" // one past every "ns\x00..." key
	}
	kvs, err := se.RangeScan(prefix+start, internalEnd, limit, reverse)
	if err != nil {
		return nil, err
	}
	for i := range kvs {
		kvs[i].Key = kvs[i].Key[len(prefix):]
	}
	return kvs, nil
}
