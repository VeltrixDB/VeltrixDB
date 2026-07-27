package storage

import "sync"

// lww.go — last-writer-wins (LWW) conflict resolution for replicated mode.
//
// Problem this solves: in `--mode replicated` every node accepts writes for any
// key and applies received writes in arrival order. Two nodes that receive two
// concurrent writes to the same key in different orders would previously end up
// with DIFFERENT final values — permanent silent divergence, with no conflict
// resolution at all.
//
// Fix: each write carries the ORIGIN wall-clock time (nanoseconds), stamped once
// at the coordinating node and applied identically on every replica. Apply is
// last-writer-wins by that clock, so the outcome is a pure function of the set
// of writes seen — independent of the order they arrive. All replicas that have
// seen the same writes converge to the same value.
//
// Determinism of ties (same origin nanosecond — astronomically rare with real
// clocks, but must still converge): a tombstone beats a put; between two puts
// the higher CRC32C wins; identical CRC / two tombstones are a no-op. Every node
// evaluates the same rule on the same stored metadata, so ties resolve
// identically everywhere.
//
// Caveat (inherent to any wall-clock LWW, documented deliberately): resolution
// is only as good as clock synchronisation across nodes. With badly skewed
// clocks a later write can lose to an earlier one. Use `--mode raft` when you
// need a single global order rather than LWW convergence.

const lwwLockStripes = 1024

func (se *StorageEngine) lwwLock(key string) *sync.Mutex {
	return &se.lwwLocks[fnv64a(key)&(lwwLockStripes-1)]
}

// lwwIncomingWins reports whether an incoming write with origin clock incomingNs
// should overwrite the current entry. cur may be nil (no local entry → incoming
// always wins). The rule is: newer clock wins; on an exact tie a tombstone beats
// a put and, between two puts, the higher CRC wins — a total order every replica
// computes identically.
func lwwIncomingWins(cur *IndexEntry, incomingNs int64, incomingTombstone bool, incomingCRC uint32) bool {
	if cur == nil {
		return true
	}
	curNs := lwwClock(cur)
	if incomingNs > curNs {
		return true
	}
	if incomingNs < curNs {
		return false
	}
	// Exact clock tie — resolve deterministically.
	curTombstone := cur.IsTombstone()
	if incomingTombstone != curTombstone {
		return incomingTombstone // tombstone beats put
	}
	if incomingTombstone {
		return false // two tombstones: no-op
	}
	return incomingCRC > cur.CRC32C // two puts: higher CRC wins
}

// ApplyLWWPut applies a replicated PUT under last-writer-wins. originNs is the
// origin wall-clock time (ns) captured once by the coordinating node. Returns
// applied=false (no error) when a newer local write already won.
func (se *StorageEngine) ApplyLWWPut(key string, value []byte, ttl int32, originNs int64) (bool, error) {
	lock := se.lwwLock(key)
	lock.Lock()
	defer lock.Unlock()

	cur, _, _ := se.index.get(key) // nil when absent
	if !lwwIncomingWins(cur, originNs, false, computeCRC32C(value)) {
		se.metrics.LWWConflictsResolved.Add(1)
		return false, nil
	}
	if err := se.putStamped(key, value, ttl, originNs); err != nil {
		return false, err
	}
	return true, nil
}

// ApplyLWWDelete applies a replicated DELETE under last-writer-wins. It records
// a tombstone even when the key is not present locally, so a delete delivered
// before the put it supersedes still wins on re-ordered delivery. Returns
// applied=false (no error) when a newer local write already won.
func (se *StorageEngine) ApplyLWWDelete(key string, originNs int64) (bool, error) {
	lock := se.lwwLock(key)
	lock.Lock()
	defer lock.Unlock()

	cur, _, _ := se.index.get(key) // nil when absent
	if !lwwIncomingWins(cur, originNs, true, 0) {
		se.metrics.LWWConflictsResolved.Add(1)
		return false, nil
	}
	if err := se.deleteStamped(key, originNs, true /*insertIfAbsent*/); err != nil {
		return false, err
	}
	return true, nil
}
