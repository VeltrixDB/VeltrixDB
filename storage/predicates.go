package storage

// predicates.go — server-side stored predicates.
//
// A predicate is a Go function registered at process startup that runs
// inside the engine on a given key/value. Two use cases:
//
//   1. Filter scans: instead of GET-iterating millions of keys, the client
//      sends a predicate name and gets back only matching keys. This avoids
//      the round-trip per key.
//   2. Conditional updates: "increment counter only if value < 1000".
//      Compare-and-swap covers byte equality but not arithmetic predicates.
//
// Why Go-registered functions instead of WASM/Lua?
//   - WASM (wazero) and Lua (gopher-lua) each pull in 100+ dependency files
//     and require sandboxing, fuel limits, and an eval cache.  Multi-day
//     to implement safely.
//   - For the 80 % case (a handful of well-known predicates compiled into
//     the operator's binary), this Go-function registry covers it without
//     any of that complexity.
//   - Operators who need arbitrary user-supplied code can extend this to
//     wazero in a focused follow-up.
//
// Registration:
//
//   storage.RegisterPredicate("price-under-100",
//       func(key string, value []byte) bool {
//           // parse JSON, return true if price < 100
//           ...
//       })
//
// Then on the wire:
//   PSCAN  pred=price-under-100  prefix=item/  limit=100  →  []key
//   PFILTER  pred=price-under-100  keys=[k1,k2,k3]  →  []bool
//
// (PSCAN/PFILTER wire-protocol verbs are reserved at 0x1C/0x1D — wire-up is
// noted as future work; the engine API below is ready.)

import (
	"errors"
	"sync"
)

// PredicateFunc evaluates one (key, value) pair. Must be pure and fast —
// it runs on the engine's read path.
type PredicateFunc func(key string, value []byte) bool

var (
	predicateMu sync.RWMutex
	predicates  = map[string]PredicateFunc{}
)

// ErrPredicateNotFound is returned when a name has no registered predicate.
var ErrPredicateNotFound = errors.New("predicate not registered")

// RegisterPredicate installs fn under name. Re-registering replaces.
func RegisterPredicate(name string, fn PredicateFunc) {
	predicateMu.Lock()
	defer predicateMu.Unlock()
	predicates[name] = fn
}

// LookupPredicate returns the registered fn for name.
func LookupPredicate(name string) (PredicateFunc, bool) {
	predicateMu.RLock()
	defer predicateMu.RUnlock()
	fn, ok := predicates[name]
	return fn, ok
}

// PredicateScan iterates keys matching keyPrefix and runs predicate on each.
// Returns up to limit matching keys (limit=0 → unlimited). Stops early on
// limit reached.
//
// Cost: one Get per visited key (cache-warmed for hot keys, full VLog read
// for cold). The engine intentionally does not parallelise — predicates are
// expected to be cheap and IO-bound.
func (se *StorageEngine) PredicateScan(name, keyPrefix string, limit int) ([]string, error) {
	fn, ok := LookupPredicate(name)
	if !ok {
		return nil, ErrPredicateNotFound
	}
	kvs := se.predicateScanCollect(fn, keyPrefix, limit)
	matches := make([]string, len(kvs))
	for i, kv := range kvs {
		matches[i] = kv.Key
	}
	return matches, nil
}

// predicateScanCollect is the shared scan core behind PredicateScan and
// QueryNS: walk keys under keyPrefix, run fn on each live (key, value), and
// return up to limit matching pairs (limit ≤ 0 → unlimited). Values are
// returned so callers don't pay a second Get per match.
func (se *StorageEngine) predicateScanCollect(fn PredicateFunc, keyPrefix string, limit int) []KV {
	var matches []KV
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		// Snapshot keys so we don't hold the shard lock during Get.
		var keys []string
		for k, entry := range shard.entries {
			if entry.IsTombstone() {
				continue
			}
			if keyPrefix == "" || hasPrefix(k, keyPrefix) {
				keys = append(keys, k)
			}
		}
		shard.mu.RUnlock()

		for _, k := range keys {
			val, err := se.Get(k)
			if err != nil {
				continue
			}
			if fn(k, val) {
				matches = append(matches, KV{Key: k, Value: val})
				if limit > 0 && len(matches) >= limit {
					return matches
				}
			}
		}
	}
	return matches
}

// PredicateFilter is the bulk-evaluate variant: evaluate predicate against
// the supplied set of keys. Returns parallel slice of bools indicating match.
// Order is preserved.
func (se *StorageEngine) PredicateFilter(name string, keys []string) ([]bool, error) {
	fn, ok := LookupPredicate(name)
	if !ok {
		return nil, ErrPredicateNotFound
	}
	out := make([]bool, len(keys))
	for i, k := range keys {
		val, err := se.Get(k)
		if err != nil {
			out[i] = false
			continue
		}
		out[i] = fn(k, val)
	}
	return out, nil
}
