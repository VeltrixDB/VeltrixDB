package main

// raft_snapshot.go — raft snapshots of the storage engine
// (consensus.StreamingStateMachine; CLAUDE.md invariant 64).
//
// # Record stream
//
// The consensus layer frames, checksums and stores the records
// (consensus/snapshot_file.go); their payloads, first byte = kind:
//
//	recMeta     [format u8][sorted u8]                   first record
//	            format 2: + [uvarint floorUs][uvarint clockUs at capture start]
//	recPut      [uvarint keyLen][key][varint expiryUs][value]       (format 1)
//	recPutV     [uvarint keyLen][key][varint expiryUs][varint stampUs][value]
//	recDel      [key]                                    overlay only
//	recOverlay  (empty)                                  base → overlay
//	recClock    [uvarint clockUs][uvarint floorUs]       the raft clock at the fence
//	recIdxDefs  JSON []storage.FieldIndexDef             last record
//
// Format 2 (invariant 66) carries every key's write timestamp — its version
// token — and the raft clock, so a node that installs or restores the
// snapshot reports the same versions as the node that captured it and
// resumes the same clock.  Format 1 snapshots are still read (versions
// unknown: legacy, clock (0, 0)).
//
// The base section is every live key, in ascending key order when the
// engine has its ordered index (meta.sorted).  The overlay section corrects
// the keys written while the base was being scanned.  Read in order, the
// last record of a key wins.  Every key is carried — reserved "@vec/",
// "@txt/", "@vecns/", "@idx/", "@idxdef/" keys, hash fields, list and set
// internals, namespaces — with its absolute TTL expiry (µs).
//
// # Capture consistency (why Apply is not stalled for the scan)
//
// CaptureSnapshot runs WITHOUT smMu, so Apply continues.  A WriteTracker
// (storage/snapshot_support.go) records every key whose index entry changes,
// noting it only after the change is installed.  Then:
//
//  1. base: walk the keyspace, read each key (uncached, ReadForSnapshot) and
//     write it;
//  2. catch-up rounds: drain the tracker, re-read every drained key and write
//     its value, or a delete when it is gone; repeat while the drained set
//     is large;
//  3. fence (smMu held — no Apply in progress or starting; the index E it
//     returns is lastApplied): drain once more and read the remaining keys
//     (those drained in the last round plus the new ones); the records are
//     written after the fence.
//
// Claim: for every key k, the last record of k equals k's state at E (absent
// = no record or a delete).  If k was never written during the capture its
// state never changed, the base walk returned it exactly when it exists
// (SnapshotIterator's guarantee) with that value, and no later record has
// it.  Otherwise let w be its last write before E.  w was noted after it was
// installed, so k is in the first drain that follows w, and k is read after
// that drain — after w.  Any write to k after a read is noted after the
// read, so k is drained and read again later.  The last read of k happens
// either in the fence, where no write can run, or in a round after which k
// was not written again; both read k's state at E, and that read produces
// k's last record.  Keys created during the capture are covered the same
// way; keys deleted during it get a delete record.
//
// Apply is stalled only for the fence: O(keys written during the last
// catch-up round), which the rounds shrink to ≤ snapFenceKeys under a steady
// write rate.  The tracker is bounded (snapTrackLimit distinct keys between
// drains); past it the capture fails and is retried later.
//
// # Restore = snapshot, exactly
//
// RestoreSnapshot makes the engine's keyspace exactly the snapshot's, so a
// key deleted on the leader cannot survive on the follower, and it works
// whatever the engine held (an interrupted restore is redone at startup):
//
//   - sorted base + ordered index: merge-join the base with the engine's own
//     key order (KeysAfter pages): engine keys absent from the base are
//     deleted, base keys whose value or expiry differ are written.  O(1)
//     RAM.  A key is written only when the next engine key is past it, so a
//     later page never returns a key the restore itself wrote;
//   - otherwise: the engine's live keys go into a set, the base removes what
//     it holds, the rest is deleted (RAM O(keys)).
//
// Then the overlay is applied in order, then the index definitions.  Writes
// are raw (storage.Restorer): no secondary-index diff, no search hook, no
// derived-key cascade — the snapshot states every derived key itself.  When
// any search key changed, the search RAM indexes are rebuilt from the
// restored keys, as after a restart (invariant 54; searches are refused
// meanwhile, invariant 57).  Expired keys are skipped (deleted).

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/VeltrixDB/veltrixdb/consensus"
	"github.com/VeltrixDB/veltrixdb/storage"
)

const (
	recMeta    byte = 1
	recPut     byte = 2
	recDel     byte = 3
	recOverlay byte = 4
	recIdxDefs byte = 5
	recPutV    byte = 6 // format 2: recPut + the write timestamp
	recClock   byte = 7 // format 2: the raft clock at the fence

	snapRecordFormat = 2

	snapPage          = 1024
	snapTrackLimit    = 4 << 20 // distinct keys tracked between two drains
	snapFenceKeys     = 4096    // fence once a drain is at most this large
	snapCatchUpRounds = 8
)

// fsmRestoreHook (crash-injection tests only, installSnapshotCrashHook) is
// called with the number of base records restored so far.
var fsmRestoreHook func(n int)

var errSnapshotTrackerOverflow = errors.New("raft snapshot: more than 4M distinct keys written during one capture round — retrying later")

// snapshotEntry is the v1.16 snapshot record (gob stream of these, no TTL).
type snapshotEntry struct {
	Key   string
	Value []byte
}

func encodePutRec(buf []byte, kv storage.SnapshotKV) []byte {
	buf = append(buf[:0], recPutV)
	buf = binary.AppendUvarint(buf, uint64(len(kv.Key)))
	buf = append(buf, kv.Key...)
	buf = binary.AppendVarint(buf, kv.ExpiryUs)
	buf = binary.AppendVarint(buf, kv.StampUs)
	return append(buf, kv.Value...)
}

// decodePutRec decodes a recPut or recPutV record.
func decodePutRec(rec []byte) (storage.SnapshotKV, error) {
	b := rec[1:]
	kl, n := binary.Uvarint(b)
	if n <= 0 || uint64(len(b)-n) < kl {
		return storage.SnapshotKV{}, errors.New("raft snapshot: bad put record")
	}
	b = b[n:]
	key := string(b[:kl])
	b = b[kl:]
	exp, n := binary.Varint(b)
	if n <= 0 {
		return storage.SnapshotKV{}, errors.New("raft snapshot: bad put record expiry")
	}
	b = b[n:]
	var stamp int64
	if rec[0] == recPutV {
		if stamp, n = binary.Varint(b); n <= 0 {
			return storage.SnapshotKV{}, errors.New("raft snapshot: bad put record version")
		}
		b = b[n:]
	}
	return storage.SnapshotKV{Key: key, ExpiryUs: exp, StampUs: stamp, Value: b}, nil
}

var errSnapshotClockStarted = errors.New("raft snapshot: the raft clock started during the capture — retrying later")

func encodeDelRec(key string) []byte {
	return append([]byte{recDel}, key...)
}

// waitReplay blocks until the engine's background WAL replay finished: the
// index is incomplete before that.
func (f *raftFSM) waitReplay() {
	if f.engine.ReplayDone != nil {
		<-f.engine.ReplayDone
	}
}

// CaptureSnapshot implements consensus.StreamingStateMachine (see the file
// comment for the consistency argument).
func (f *raftFSM) CaptureSnapshot(sink consensus.SnapshotSink, fence consensus.SnapshotFence) error {
	f.waitReplay()
	tr, err := f.engine.TrackWrites(snapTrackLimit)
	if err != nil {
		return err
	}
	defer tr.Close()

	it := f.engine.NewSnapshotIterator()
	sorted := byte(0)
	if it.Sorted() {
		sorted = 1
	}
	// The floor (0 until the first stamped command) and the clock at the
	// start: a restore judges expiry no later than the latter
	// (storage/raft_clock.go).
	clock0, floor0 := f.engine.RaftClock()
	meta := []byte{recMeta, snapRecordFormat, sorted}
	meta = binary.AppendUvarint(meta, uint64(floor0))
	meta = binary.AppendUvarint(meta, uint64(clock0))
	if err := sink.Write(meta); err != nil {
		return err
	}
	buf := make([]byte, 0, 4096)
	for {
		kvs, err := it.Next(snapPage)
		if err != nil {
			return err
		}
		if kvs == nil {
			break
		}
		for _, kv := range kvs {
			buf = encodePutRec(buf, kv)
			if err := sink.Write(buf); err != nil {
				return err
			}
		}
	}
	if err := sink.Write([]byte{recOverlay}); err != nil {
		return err
	}

	// recordFor returns key's current record (put, or delete when absent).
	recordFor := func(key string, dst []byte) ([]byte, error) {
		kv, ok, err := f.engine.ReadForSnapshot(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return append(dst[:0], encodeDelRec(key)...), nil
		}
		return encodePutRec(dst, kv), nil
	}

	var pending []string
	for round := 0; ; round++ {
		keys, overflow := tr.Drain()
		if overflow {
			return errSnapshotTrackerOverflow
		}
		if len(keys) <= snapFenceKeys || round >= snapCatchUpRounds {
			pending = keys
			break
		}
		for _, k := range keys {
			if buf, err = recordFor(k, buf); err != nil {
				return err
			}
			if err := sink.Write(buf); err != nil {
				return err
			}
		}
	}

	var (
		final        [][]byte
		defs         []storage.FieldIndexDef
		ferr         error
		clock, floor int64
	)
	if _, err := fence(func() {
		// Apply is quiesced: the clock is the one at the fence index.
		clock, floor = f.engine.RaftClock()
		if floor != floor0 {
			// The first stamped command was applied during the capture: the
			// base records' versions were read under the old floor, which a
			// restore compares them by.  Once per cluster; just retry.
			ferr = errSnapshotClockStarted
			return
		}
		more, overflow := tr.Drain()
		if overflow {
			ferr = errSnapshotTrackerOverflow
			return
		}
		seen := make(map[string]struct{}, len(pending)+len(more))
		for _, ks := range [][]string{pending, more} {
			for _, k := range ks {
				if _, dup := seen[k]; dup {
					continue
				}
				seen[k] = struct{}{}
				rec, err := recordFor(k, nil)
				if err != nil {
					ferr = err
					return
				}
				final = append(final, rec)
			}
		}
		defs = f.engine.ListFieldIndexes()
	}); err != nil {
		return err
	}
	if ferr != nil {
		return ferr
	}
	for _, rec := range final {
		if err := sink.Write(rec); err != nil {
			return err
		}
	}
	clockRec := binary.AppendUvarint([]byte{recClock}, uint64(clock))
	clockRec = binary.AppendUvarint(clockRec, uint64(floor))
	if err := sink.Write(clockRec); err != nil {
		return err
	}
	if defs == nil {
		defs = []storage.FieldIndexDef{}
	}
	dj, err := json.Marshal(defs)
	if err != nil {
		return err
	}
	return sink.Write(append([]byte{recIdxDefs}, dj...))
}

// restorePut writes kv unless the engine already holds exactly it (value,
// expiry and version — sameVersion); an expired kv deletes the key.  floor
// is the snapshot's raft clock floor (storage/raft_clock.go).
func (f *raftFSM) restorePut(r *storage.Restorer, kv storage.SnapshotKV, floor int64) error {
	cur, ok, err := f.engine.ReadForSnapshot(kv.Key)
	if err != nil {
		return err
	}
	if kv.ExpiryUs > 0 && kv.ExpiryUs <= r.ExpiryNowUs() {
		if ok {
			return r.Delete(kv.Key)
		}
		return nil
	}
	if ok && cur.ExpiryUs == kv.ExpiryUs && bytes.Equal(cur.Value, kv.Value) && sameVersion(cur.StampUs, kv.StampUs, floor) {
		return nil
	}
	return r.Put(kv)
}

// sameVersion reports whether a key written at stamp cur already reports the
// version a snapshot record stamped snap gives it.  Unknown (0, format 1 /
// v1.16) or before the clock started (floor 0): versions are legacy
// per-node timestamps either way — keep the node's, as before, rather than
// rewrite every key.  Below the floor both read as LegacyVersion.
func sameVersion(cur, snap, floor int64) bool {
	if cur == snap || snap == 0 || floor == 0 {
		return true
	}
	return cur < floor && snap < floor
}

// engineKeyCursor pages through the engine's keys in ascending order for the
// restore merge-join.  Once a short page is seen it never fetches again.
type engineKeyCursor struct {
	se        *storage.StorageEngine
	page      []string
	i         int
	cursor    string
	exhausted bool
}

func (c *engineKeyCursor) peek() (string, bool, error) {
	for c.i >= len(c.page) {
		if c.exhausted {
			return "", false, nil
		}
		page, err := c.se.KeysAfter(c.cursor, snapPage)
		if err != nil {
			return "", false, err
		}
		if len(page) < snapPage {
			c.exhausted = true
		}
		c.page, c.i = page, 0
		if len(page) > 0 {
			c.cursor = page[len(page)-1]
		}
	}
	return c.page[c.i], true, nil
}

// RestoreSnapshot implements consensus.StreamingStateMachine (see the file
// comment).
func (f *raftFSM) RestoreSnapshot(src consensus.SnapshotSource, legacy bool) error {
	f.waitReplay()
	if legacy {
		blob, err := src.Next()
		if err != nil {
			return err
		}
		return f.restoreLegacyBlob(blob)
	}
	first, err := src.Next()
	if err != nil {
		return err
	}
	if len(first) < 3 || first[0] != recMeta {
		return errors.New("raft snapshot: missing meta record")
	}
	if first[1] != 1 && first[1] != snapRecordFormat {
		return fmt.Errorf("raft snapshot: unsupported record format %d", first[1])
	}
	sorted := first[2] == 1
	// Format 2: the floor and the clock at capture start (see CaptureSnapshot).
	var floor, clockStart int64
	if first[1] >= 2 && len(first) > 3 {
		v, n := binary.Uvarint(first[3:])
		if n <= 0 {
			return errors.New("raft snapshot: bad meta record")
		}
		c, m := binary.Uvarint(first[3+n:])
		if m <= 0 {
			return errors.New("raft snapshot: bad meta record")
		}
		floor, clockStart = int64(v), int64(c)
	}
	var clock, clockFloor int64 // recClock; (0, 0) for format 1

	r := f.engine.NewRestorer()
	// Expiry is judged no later than the snapshot's own clock, so a key a
	// later command may still see live is never dropped by a restorer whose
	// clock runs ahead (storage/raft_clock.go).
	if clockStart > 0 && clockStart < time.Now().UnixMicro() {
		r.SetExpiryNow(clockStart)
	}
	merge := sorted && f.engine.OrderedIndexEnabled()
	var cur *engineKeyCursor
	var leftover map[string]struct{}
	if merge {
		cur = &engineKeyCursor{se: f.engine}
	} else {
		keys := f.engine.RestoreKeys()
		leftover = make(map[string]struct{}, len(keys))
		for _, k := range keys {
			leftover[k] = struct{}{}
		}
	}

	overlay := false
	var defs []storage.FieldIndexDef
	prev, havePrev := "", false
	nBase := 0
	t0 := time.Now()
	for {
		rec, err := src.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if len(rec) == 0 {
			return errors.New("raft snapshot: empty record")
		}
		switch rec[0] {
		case recPut, recPutV:
			kv, err := decodePutRec(rec)
			if err != nil {
				return err
			}
			if overlay {
				if err := r.Put(kv); err != nil {
					return err
				}
				continue
			}
			if merge {
				if havePrev && kv.Key <= prev {
					return fmt.Errorf("raft snapshot: base not sorted (%q after %q)", kv.Key, prev)
				}
				prev, havePrev = kv.Key, true
				for {
					ek, ok, err := cur.peek()
					if err != nil {
						return err
					}
					if !ok || ek > kv.Key {
						break
					}
					cur.i++
					if ek < kv.Key {
						if err := r.Delete(ek); err != nil {
							return err
						}
					}
				}
			} else {
				delete(leftover, kv.Key)
			}
			if err := f.restorePut(r, kv, floor); err != nil {
				return err
			}
			if fsmRestoreHook != nil {
				nBase++
				if nBase%1000 == 0 {
					if err := r.Flush(); err != nil {
						return err
					}
				}
				fsmRestoreHook(nBase)
			}
		case recDel:
			if !overlay {
				return errors.New("raft snapshot: delete record in the base section")
			}
			if err := r.Delete(string(rec[1:])); err != nil {
				return err
			}
		case recOverlay:
			if overlay {
				return errors.New("raft snapshot: two overlay markers")
			}
			overlay = true
			// Every engine key past the last base key is not in the base.
			if merge {
				for {
					ek, ok, err := cur.peek()
					if err != nil {
						return err
					}
					if !ok {
						break
					}
					cur.i++
					if err := r.Delete(ek); err != nil {
						return err
					}
				}
			} else {
				for k := range leftover {
					if err := r.Delete(k); err != nil {
						return err
					}
				}
				leftover = nil
			}
		case recClock:
			v, n := binary.Uvarint(rec[1:])
			if n <= 0 {
				return errors.New("raft snapshot: bad clock record")
			}
			w, m := binary.Uvarint(rec[1+n:])
			if m <= 0 {
				return errors.New("raft snapshot: bad clock record")
			}
			clock, clockFloor = int64(v), int64(w)
		case recIdxDefs:
			defs = []storage.FieldIndexDef{}
			if err := json.Unmarshal(rec[1:], &defs); err != nil {
				return fmt.Errorf("raft snapshot: index definitions: %w", err)
			}
		default:
			return fmt.Errorf("raft snapshot: unknown record kind %d", rec[0])
		}
	}
	if !overlay {
		return errors.New("raft snapshot: truncated (no overlay marker)")
	}
	if err := r.Finish(defs); err != nil {
		return err
	}
	// The state is the snapshot's: so is the raft clock (invariant 66).
	f.engine.SetRaftClock(clock, clockFloor)
	log.Printf("[raft-fsm] snapshot restored: %d puts, %d deletes (merge=%v) in %s",
		r.Puts, r.Deletes, merge, time.Since(t0).Round(time.Millisecond))
	return nil
}

// restoreLegacyBlob restores a v1.16 snapshot (gob stream of snapshotEntry,
// no TTLs, no index definitions) — sent by a v1.16 leader, or migrated from
// raft_snapshot.gob.  The keyspace becomes exactly the blob's.
func (f *raftFSM) restoreLegacyBlob(blob []byte) error {
	keys := f.engine.RestoreKeys()
	leftover := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		leftover[k] = struct{}{}
	}
	r := f.engine.NewRestorer()
	dec := gob.NewDecoder(bytes.NewReader(blob))
	for {
		var e snapshotEntry
		if err := dec.Decode(&e); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("raft-fsm restore decode: %w", err)
		}
		delete(leftover, e.Key)
		// The v1.16 format carries no TTL.  A key the engine already holds
		// with the same value keeps its own (WAL-replayed) expiry: rewriting
		// it with none made every TTL'd key immortal on the first start of
		// an upgraded node, and the next snapshot made that permanent.  A
		// key whose value differs can only be written without one.
		if cur, ok, err := f.engine.ReadForSnapshot(e.Key); err != nil {
			return err
		} else if ok && bytes.Equal(cur.Value, e.Value) {
			continue
		}
		if err := f.restorePut(r, storage.SnapshotKV{Key: e.Key, Value: e.Value}, 0); err != nil {
			return err
		}
	}
	for k := range leftover {
		if err := r.Delete(k); err != nil {
			return err
		}
	}
	if err := r.Finish(nil); err != nil {
		return err
	}
	// A v1.16 snapshot has no raft clock: versions are legacy (invariant 66).
	f.engine.SetRaftClock(0, 0)
	return nil
}

// EncodeLegacySnapshot implements consensus.LegacySnapshotEncoder: the
// snapshot in the v1.16 format (last record per key wins, deleted and
// expired keys dropped; TTLs cannot be expressed and are lost).
func (f *raftFSM) EncodeLegacySnapshot(src consensus.SnapshotSource, w io.Writer, limit int) error {
	type ent struct {
		val  []byte
		exp  int64
		live bool
	}
	pos := map[string]int{}
	var keys []string
	var ents []ent
	total := 0
	for {
		rec, err := src.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if len(rec) == 0 {
			continue
		}
		var key string
		var e ent
		switch rec[0] {
		case recPut, recPutV:
			kv, err := decodePutRec(rec)
			if err != nil {
				return err
			}
			key, e = kv.Key, ent{val: kv.Value, exp: kv.ExpiryUs, live: true}
			total += len(kv.Key) + len(kv.Value)
			if total > limit {
				return fmt.Errorf("snapshot exceeds %d bytes", limit)
			}
		case recDel:
			key = string(rec[1:])
		default:
			continue
		}
		if i, ok := pos[key]; ok {
			ents[i] = e
		} else {
			pos[key] = len(ents)
			keys = append(keys, key)
			ents = append(ents, e)
		}
	}
	cw := &countingWriter{w: w, limit: limit}
	enc := gob.NewEncoder(cw)
	now := time.Now().UnixMicro()
	for i, k := range keys {
		e := ents[i]
		if !e.live || (e.exp > 0 && e.exp <= now) {
			continue
		}
		if err := enc.Encode(snapshotEntry{Key: k, Value: e.val}); err != nil {
			return err
		}
	}
	return nil
}

// ResetState implements consensus.AppliedIndexStateMachine: the engine
// becomes empty (every key, derived keys and index definitions included) by
// restoring an empty snapshot — consensus then applies the whole log once
// (consensus/applied.go: a data dir from an older build with no snapshot).
func (f *raftFSM) ResetState() error {
	return f.RestoreSnapshot(&recordSlice{recs: [][]byte{
		{recMeta, snapRecordFormat, 1},
		{recOverlay},
		append([]byte{recIdxDefs}, "[]"...),
	}}, false)
}

// recordSlice is a consensus.SnapshotSource over in-memory records.
type recordSlice struct {
	recs [][]byte
	i    int
}

func (r *recordSlice) Next() ([]byte, error) {
	if r.i >= len(r.recs) {
		return nil, io.EOF
	}
	r.i++
	return r.recs[r.i-1], nil
}

type countingWriter struct {
	w     io.Writer
	n     int
	limit int
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += len(p)
	if c.n > c.limit {
		return 0, fmt.Errorf("snapshot exceeds %d bytes", c.limit)
	}
	return c.w.Write(p)
}
