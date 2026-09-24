//go:build cgo && go1.21

package storage

/*
#cgo CXXFLAGS: -std=c++17 -O2
#cgo linux LDFLAGS: -lstdc++
#include "native_index_capi.h"
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"
)

const nativeIndexAvailable = true

// The C table treats IndexEntry as 64 opaque bytes except for the key hash in
// [0, 8) and its own key reference in [56, 64). Pin that layout at compile
// time: if either assertion fails, a field moved and the C side would read
// the wrong bytes.
var (
	_ [unsafe.Sizeof(IndexEntry{}) - C.VX_IDX_ENTRY_SIZE]struct{}
	_ [C.VX_IDX_ENTRY_SIZE - unsafe.Sizeof(IndexEntry{})]struct{}
	_ [0 - unsafe.Offsetof(IndexEntry{}.KeyHash)]struct{}
	_ [unsafe.Offsetof(IndexEntry{}._reserved) - 56]struct{}
	_ [56 - unsafe.Offsetof(IndexEntry{}._reserved)]struct{}
	_ [unsafe.Sizeof(C.VxEntry{}) - unsafe.Sizeof(IndexEntry{})]struct{}
	_ [unsafe.Sizeof(IndexEntry{}) - unsafe.Sizeof(C.VxEntry{})]struct{}
)

// Entries cross the boundary BY VALUE (see VxEntry in the header): handing
// C a pointer to a Go local would move that local to the heap, one
// allocation per lookup. These reinterpret between the two 64-byte layouts.
func toC(e *IndexEntry) C.VxEntry   { return *(*C.VxEntry)(unsafe.Pointer(e)) }
func fromC(e *C.VxEntry) IndexEntry { return *(*IndexEntry)(unsafe.Pointer(e)) }

// nativeTable is an entryTable whose slots and key bytes live in C memory
// (native_index.cpp). Locking is the owning indexShard's, exactly as
// for mapTable.
type nativeTable struct {
	t *C.VxIndexTable
}

func newNativeTable() entryTable {
	t := C.vx_idx_new()
	if t == nil {
		panic("native index: out of memory creating shard table")
	}
	return &nativeTable{t: t}
}

// keyPtr returns the key's bytes for C without copying. Safe because Go
// strings are immutable and the C side only reads them during the call.
func keyPtr(key string) (*C.char, C.size_t) {
	return (*C.char)(unsafe.Pointer(unsafe.StringData(key))), C.size_t(len(key))
}

func (n *nativeTable) get(key string, h uint64) (IndexEntry, bool) {
	if n.t == nil {
		return IndexEntry{}, false
	}
	kp, kl := keyPtr(key)
	r := C.vx_idx_get(n.t, C.uint64_t(h), kp, kl)
	if r.status == 0 {
		return IndexEntry{}, false
	}
	return fromC(&r.e), true
}

func (n *nativeTable) swap(key string, h uint64, e *IndexEntry) (IndexEntry, bool) {
	if n.t == nil { // used after free: come back to life rather than crash
		n.t = C.vx_idx_new()
	}
	in := toC(e) // the C side stamps KeyHash = h into bytes [0, 8)
	kp, kl := keyPtr(key)
	r := C.vx_idx_put(n.t, C.uint64_t(h), kp, kl, in)
	switch r.status {
	case 1:
		return fromC(&r.e), true
	case 0:
		return IndexEntry{}, false
	}
	// Same outcome the Go runtime gives a failed map insert: the process
	// cannot continue with a silently missing index entry.
	panic(fmt.Sprintf("native index: insert failed (out of memory?) key_len=%d", len(key)))
}

func (n *nativeTable) del(key string, h uint64) bool {
	if n.t == nil {
		return false
	}
	kp, kl := keyPtr(key)
	return C.vx_idx_del(n.t, C.uint64_t(h), kp, kl) == 1
}

func (n *nativeTable) update(key string, h uint64, fn func(e *IndexEntry) bool) bool {
	e, ok := n.get(key, h)
	if !ok {
		return false
	}
	if fn(&e) {
		n.swap(key, h, &e) // key exists: overwrites in place, no arena growth
	}
	return true
}

// scanBatch is the reusable buffer set for one batched walk: 256 entries
// (16 KiB) and 64 KiB of key bytes per cgo call.
type scanBatch struct {
	entries [256]IndexEntry
	klens   [256]uint32
	keys    []byte
	// cursor and need live here, not on scan's stack, for the same reason
	// entries cross by value: a Go local passed to C would escape.
	cursor C.uint64_t
	need   C.size_t
}

var scanBatchPool = sync.Pool{New: func() any { return &scanBatch{keys: make([]byte, 64<<10)} }}

func (n *nativeTable) rangeAll(fn func(key string, e *IndexEntry) bool) {
	n.scan(true, func(b *scanBatch, cnt int) bool {
		off := 0
		for i := 0; i < cnt; i++ {
			l := int(b.klens[i])
			key := string(b.keys[off : off+l]) // copy: the callback may keep it
			off += l
			if !fn(key, &b.entries[i]) {
				return false
			}
		}
		return true
	})
}

func (n *nativeTable) rangeEntries(fn func(e *IndexEntry) bool) {
	n.scan(false, func(b *scanBatch, cnt int) bool {
		for i := 0; i < cnt; i++ {
			if !fn(&b.entries[i]) {
				return false
			}
		}
		return true
	})
}

func (n *nativeTable) scan(withKeys bool, each func(b *scanBatch, cnt int) bool) {
	if n.t == nil {
		return
	}
	b := scanBatchPool.Get().(*scanBatch)
	defer scanBatchPool.Put(b)
	b.cursor = 0
	for {
		b.need = 0
		var keyBuf *C.char
		var keyCap C.size_t
		if withKeys {
			keyBuf = (*C.char)(unsafe.Pointer(unsafe.SliceData(b.keys)))
			keyCap = C.size_t(len(b.keys))
		}
		cnt := int(C.vx_idx_scan(n.t, &b.cursor,
			unsafe.Pointer(&b.entries[0]), C.size_t(len(b.entries)),
			keyBuf, keyCap, (*C.uint32_t)(unsafe.Pointer(&b.klens[0])), &b.need))
		if cnt == 0 {
			if b.need == 0 {
				return // walk complete
			}
			b.keys = make([]byte, int(b.need)) // one key larger than the buffer
			continue
		}
		if !each(b, cnt) {
			return
		}
	}
}

func (n *nativeTable) len() int {
	if n.t == nil {
		return 0
	}
	return int(C.vx_idx_len(n.t))
}

// bytes is what this table holds in C memory (slots + key arena).
func (n *nativeTable) bytes() uint64 {
	if n.t == nil {
		return 0
	}
	return uint64(C.vx_idx_bytes(n.t))
}

func (n *nativeTable) free() {
	if n.t != nil {
		C.vx_idx_free(n.t)
		n.t = nil
	}
}

// NativeIndexBytes reports the bytes held by every native index table in the
// process — memory the Go runtime's own stats do not include.
func NativeIndexBytes() uint64 { return uint64(C.vx_idx_total_bytes()) }
