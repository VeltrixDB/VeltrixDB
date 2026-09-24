package storage

import (
	"os"
	"strings"
)

// entryTable is one index shard's key → IndexEntry store.
//
// It is NOT safe for concurrent use on its own: every method runs under the
// owning indexShard.mu — RLock for get / rangeAll / rangeEntries / len, Lock
// for everything that mutates. That is the same discipline the plain map
// always had; the interface only moves the bytes.
//
// Entries go in and come out BY VALUE. Nothing may hold a pointer into the
// table past the call that produced it, because the native implementation
// (index_table_native.go) keeps entries in C memory that a later insert may
// reallocate. The *IndexEntry handed to an update / range callback is valid
// only for the duration of that callback.
//
// Two implementations:
//
//   - mapTable — map[string]*IndexEntry on the Go heap. The only choice on a
//     CGO_ENABLED=0 build, and the fallback everywhere else.
//   - nativeTable — C++ open-addressing table off the Go heap (mmap-backed,
//     no Go pointers), so the GC neither scans nor pays heap headroom for
//     it. See native_index.cpp.
type entryTable interface {
	// get returns a copy of the entry for key. h must be fnv64a(key).
	get(key string, h uint64) (IndexEntry, bool)
	// swap stores a copy of *e — with KeyHash forced to h, which the bloom
	// rebuild relies on — and returns the entry it replaced, if any.
	swap(key string, h uint64, e *IndexEntry) (old IndexEntry, hadOld bool)
	// del removes key. Returns false if it was absent.
	del(key string, h uint64) bool
	// update runs fn on the stored entry; the stored entry is overwritten
	// with fn's changes only if fn returns true. Returns false if key is
	// absent (fn is not called).
	update(key string, h uint64, fn func(e *IndexEntry) bool) bool
	// rangeAll calls fn for every entry until fn returns false. The key is
	// an ordinary Go string the callback may retain; e is a scratch copy
	// whose changes are discarded.
	rangeAll(fn func(key string, e *IndexEntry) bool)
	// rangeEntries is rangeAll without materialising keys — for background
	// scans (VLog extents, bloom rebuild) that only read entry fields.
	rangeEntries(fn func(e *IndexEntry) bool)
	len() int
	// free releases the table's memory and leaves it empty. It is idempotent,
	// and a table stays usable (as an empty one) afterwards, so a straggling
	// background goroutine after engine Close reads "not found" instead of
	// freed memory.
	free()
}

// IndexImplEnv selects the index implementation: "map" or "native".
// Unset means native when this build has it (cgo) and the C++ layer is not
// disabled via VELTRIXDB_DISABLE_CGO_ENGINE, otherwise map.
const IndexImplEnv = "VELTRIXDB_INDEX"

// newEntryTable returns a fresh table of the implementation chosen for this
// process. Every shard of one index uses the same implementation.
func newEntryTable() entryTable {
	if useNativeIndex() {
		return newNativeTable()
	}
	return newMapTable()
}

func useNativeIndex() bool {
	switch strings.ToLower(os.Getenv(IndexImplEnv)) {
	case "map":
		return false
	case "native":
		return nativeIndexAvailable
	}
	return nativeIndexAvailable && !cgoEngineDisabled()
}

// IndexImpl reports the index implementation this process uses ("map" or
// "native"), for /admin/stats and the startup log.
func IndexImpl() string {
	if useNativeIndex() {
		return "native"
	}
	return "map"
}

// ── mapTable ────────────────────────────────────────────────────────────────

type mapTable struct {
	m map[string]*IndexEntry
}

func newMapTable() *mapTable { return &mapTable{m: make(map[string]*IndexEntry)} }

func (t *mapTable) get(key string, _ uint64) (IndexEntry, bool) {
	e, ok := t.m[key]
	if !ok {
		return IndexEntry{}, false
	}
	return *e, true
}

func (t *mapTable) swap(key string, h uint64, e *IndexEntry) (IndexEntry, bool) {
	cp := new(IndexEntry)
	*cp = *e
	cp.KeyHash = h
	if old, ok := t.m[key]; ok {
		prev := *old
		t.m[key] = cp
		return prev, true
	}
	t.m[key] = cp
	return IndexEntry{}, false
}

func (t *mapTable) del(key string, _ uint64) bool {
	if _, ok := t.m[key]; !ok {
		return false
	}
	delete(t.m, key)
	return true
}

func (t *mapTable) update(key string, _ uint64, fn func(e *IndexEntry) bool) bool {
	e, ok := t.m[key]
	if !ok {
		return false
	}
	cp := *e
	if fn(&cp) {
		*e = cp
	}
	return true
}

func (t *mapTable) rangeAll(fn func(key string, e *IndexEntry) bool) {
	for k, e := range t.m {
		cp := *e
		if !fn(k, &cp) {
			return
		}
	}
}

func (t *mapTable) rangeEntries(fn func(e *IndexEntry) bool) {
	for _, e := range t.m {
		cp := *e
		if !fn(&cp) {
			return
		}
	}
}

func (t *mapTable) len() int { return len(t.m) }
func (t *mapTable) free()    {} // GC-managed; dropped with the engine
