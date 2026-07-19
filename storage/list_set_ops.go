package storage

// list_set_ops.go — Redis-style list and set data types.
//
// Both types follow the same design as hash fields (engine.go): every element
// is a regular KV entry under a reserved separator, so there is no new WAL
// format and no new on-disk structure.
//
//	List element:  key + "\x02" + <8-byte big-endian sequence>
//	List meta:     key + "\x02!"   → [8B head][8B tail] (big-endian int64)
//	Set member:    key + "\x03" + member          (value is empty)
//
// Lists are deques: head/tail sequence counters live in the meta entry; a
// push allocates the next sequence outward, a pop consumes inward.  Sequence
// keys are big-endian so lexicographic scans return list order.
//
// Concurrency: multi-key ops (push/pop touch element + meta) serialize on a
// striped per-list-key mutex.  Individual element writes remain atomic via
// the engine's own shard locks.
//
// Crash-consistency: element is written BEFORE meta commits the new bounds.
// A crash in between leaves an orphan element outside the committed range —
// invisible to reads and overwritten by the next push at that sequence.
//
// Replication: every mutation is reported back to the caller as the exact
// KV writes it performed (ListMutation), so the distributed coordinator can
// replicate list/set ops as plain KV traffic.

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
)

const (
	listSep     = "\x02"
	listMetaKey = listSep + "!"
	setSep      = "\x03"
)

// listSeqCenter is the initial head/tail midpoint, chosen so ~9.2e18 pushes
// fit on either side.
const listSeqCenter = int64(0)

// ListMutation reports one KV write performed by a list/set op so the
// coordinator can replicate it.
type ListMutation struct {
	Key       string
	Value     []byte
	Tombstone bool
}

// listLocks stripes per-list mutexes (multi-key ops need cross-shard
// serialization the per-shard locks can't give).
var listLocks [512]sync.Mutex

func listLockFor(key string) *sync.Mutex {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &listLocks[h.Sum32()&511]
}

func listElemKey(key string, seq int64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(seq)+1<<63) // bias so negatives sort first
	return key + listSep + string(b[:])
}

// listMeta reads (head, tail). Both are EXCLUSIVE bounds: live sequences are
// head < s < tail. An absent meta entry means an empty list centered at 0.
func (se *StorageEngine) listMeta(key string) (head, tail int64) {
	v, err := se.Get(key + listMetaKey)
	if err != nil || len(v) != 16 {
		return listSeqCenter, listSeqCenter + 1
	}
	head = int64(binary.BigEndian.Uint64(v[0:8]))
	tail = int64(binary.BigEndian.Uint64(v[8:16]))
	return head, tail
}

func (se *StorageEngine) putListMeta(key string, head, tail int64) ([]byte, error) {
	var v [16]byte
	binary.BigEndian.PutUint64(v[0:8], uint64(head))
	binary.BigEndian.PutUint64(v[8:16], uint64(tail))
	return v[:], se.Put(key+listMetaKey, v[:], -1)
}

// LPush prepends val; returns the new length and the KV writes performed.
func (se *StorageEngine) LPush(key string, val []byte) (int64, []ListMutation, error) {
	return se.listPush(key, val, true)
}

// RPush appends val; returns the new length and the KV writes performed.
func (se *StorageEngine) RPush(key string, val []byte) (int64, []ListMutation, error) {
	return se.listPush(key, val, false)
}

func (se *StorageEngine) listPush(key string, val []byte, left bool) (int64, []ListMutation, error) {
	mu := listLockFor(key)
	mu.Lock()
	defer mu.Unlock()

	head, tail := se.listMeta(key)
	var seq int64
	if left {
		seq = head
		head--
	} else {
		seq = tail
		tail++
	}
	ek := listElemKey(key, seq)
	if err := se.Put(ek, val, -1); err != nil {
		return 0, nil, err
	}
	metaVal, err := se.putListMeta(key, head, tail)
	if err != nil {
		return 0, nil, err
	}
	muts := []ListMutation{
		{Key: ek, Value: val},
		{Key: key + listMetaKey, Value: metaVal},
	}
	return tail - head - 1, muts, nil
}

// LPop removes and returns the first element; RPop the last.
// found=false on an empty list.
func (se *StorageEngine) LPop(key string) ([]byte, bool, []ListMutation, error) {
	return se.listPop(key, true)
}

func (se *StorageEngine) RPop(key string) ([]byte, bool, []ListMutation, error) {
	return se.listPop(key, false)
}

func (se *StorageEngine) listPop(key string, left bool) ([]byte, bool, []ListMutation, error) {
	mu := listLockFor(key)
	mu.Lock()
	defer mu.Unlock()

	head, tail := se.listMeta(key)
	if tail-head <= 1 {
		return nil, false, nil, nil // empty
	}
	var seq int64
	if left {
		seq = head + 1
		head++
	} else {
		seq = tail - 1
		tail--
	}
	ek := listElemKey(key, seq)
	val, err := se.Get(ek)
	if err != nil {
		// Orphan hole (crash window) — commit the bounds move anyway so the
		// list self-heals instead of wedging on the hole forever.
		val = nil
	}
	if err := se.Delete(ek); err != nil {
		return nil, false, nil, err
	}
	metaVal, merr := se.putListMeta(key, head, tail)
	if merr != nil {
		return nil, false, nil, merr
	}
	muts := []ListMutation{
		{Key: ek, Tombstone: true},
		{Key: key + listMetaKey, Value: metaVal},
	}
	return val, val != nil, muts, nil
}

// LLen returns the number of elements.
func (se *StorageEngine) LLen(key string) int64 {
	head, tail := se.listMeta(key)
	return tail - head - 1
}

// LRange returns elements in [start, stop] with Redis semantics: negative
// indices count from the end; stop is inclusive; out-of-range is clamped.
func (se *StorageEngine) LRange(key string, start, stop int64) ([][]byte, error) {
	head, tail := se.listMeta(key)
	n := tail - head - 1
	if n <= 0 {
		return nil, nil
	}
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return nil, nil
	}
	out := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		v, err := se.Get(listElemKey(key, head+1+i))
		if err != nil {
			continue // crash-window orphan hole — skip
		}
		out = append(out, v)
	}
	return out, nil
}

// ── Sets ─────────────────────────────────────────────────────────────────────

// SAdd inserts member; added=false when it already existed.
func (se *StorageEngine) SAdd(key, member string) (bool, []ListMutation, error) {
	mk := key + setSep + member
	_, err := se.Get(mk)
	existed := err == nil
	if err := se.Put(mk, []byte{}, -1); err != nil {
		return false, nil, err
	}
	return !existed, []ListMutation{{Key: mk, Value: []byte{}}}, nil
}

// SRem removes member; removed=false when it was absent.
func (se *StorageEngine) SRem(key, member string) (bool, []ListMutation, error) {
	mk := key + setSep + member
	if _, err := se.Get(mk); err != nil {
		return false, nil, nil
	}
	if err := se.Delete(mk); err != nil {
		return false, nil, err
	}
	return true, []ListMutation{{Key: mk, Tombstone: true}}, nil
}

// SIsMember reports membership.
func (se *StorageEngine) SIsMember(key, member string) bool {
	_, err := se.Get(key + setSep + member)
	return err == nil
}

// SMembers returns all members (unordered).
func (se *StorageEngine) SMembers(key string) []string {
	prefix := key + setSep
	keys := se.scanKeysWithPrefix(prefix)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, strings.TrimPrefix(k, prefix))
	}
	return out
}

// SCard returns the member count.
func (se *StorageEngine) SCard(key string) int {
	return len(se.scanKeysWithPrefix(key + setSep))
}

// listMutationsString formats muts for debugging/audit.
func listMutationsString(muts []ListMutation) string {
	parts := make([]string, len(muts))
	for i, m := range muts {
		if m.Tombstone {
			parts[i] = fmt.Sprintf("DEL(%q)", m.Key)
		} else {
			parts[i] = fmt.Sprintf("PUT(%q,%dB)", m.Key, len(m.Value))
		}
	}
	return strings.Join(parts, " ")
}
