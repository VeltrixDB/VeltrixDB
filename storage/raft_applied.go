package storage

// raft_applied.go — the raft applied index, durable with the engine state it
// describes (CLAUDE.md invariant 65).
//
// # The problem
//
// A raft node's engine is durable on its own, but raft's lastApplied was not:
// on restart the node replayed its whole retained log (or the tail after its
// snapshot) over an engine that already held those entries' effects, so every
// non-idempotent command — INCR / DECR, list push / pop, a version-guarded
// TXN, a relative TTL — was applied twice.
//
// # The design: a WAL marker, and a replay that stops at it
//
// After each applied run the raft FSM calls SetRaftAppliedState(index), which
// appends ONE marker record to the WAL: reserved key appliedMarkerKey, value
// inline (no VLog), never installed in the index.  Its engine version is
// taken AFTER every write of that run has returned — and every write path the
// FSM uses (Put, Delete, MultiPut, the atomic ops, everything built on them)
// returns only once its records are fdatasync'd — and BEFORE the next run
// starts.  So, in engine-version order:
//
//	[writes of entries ≤ A] < marker(A) < [writes of entries > A]
//
// On open with StorageConfig.RaftAppliedIndex, replay finds the newest marker
// (highest version, across every disk's WAL) and replays only the records at
// or below its version: the rebuilt index is EXACTLY the state after entry A
// — writes of later entries that reached the disk before the crash are
// dropped, as if never made, and raft re-applies those entries from its log.
// Nothing is ever applied twice and nothing applied is lost, for every fsmOp,
// including multi-key ops (list push = element + meta, TXN, MultiPut across
// disks, field-index backfill, derived search / index keys): a partly
// durable entry is rolled back as a whole, because the cut is by version,
// not by key or by file.
//
// The marker does not need its own fdatasync: if it is lost, the previous one
// is the newest, the cut is lower, and more entries are re-applied — still
// exactly once.  It rides on the WAL's next group commit, so the apply path
// pays one ~80 B record per run and no extra wait.  Only a restore marks
// durably (BeginRaftRestore before it touches the engine, the snapshot index
// after it).
//
// Writes the cut drops stay in wal.log until the next checkpoint.  New writes
// get versions above them (se.version is restored from ALL records), so a
// later marker would make them visible again; the engine therefore records
// each dropped version range (cut, maxVersion] in the marker it writes next
// ("dead" ranges), and replay skips records in them.  A range is forgotten
// once no record in any WAL falls in it (after a clean shutdown's checkpoint).
//
// Checkpoints rewrite every live record with ONE version — the newest
// marker's own version (checkpointCut) — and each disk's checkpoint carries
// that marker at it, so a checkpointed state is cut exactly at itself, and a
// runtime checkpoint's carried WAL tail (wal_checkpoint_live.go) keeps every
// later record above it.  Every disk, because a crash between two disks'
// renames must still leave a marker at or above every surviving record.  A
// runtime Checkpoint (admin / backup) runs through the gate the raft node
// installs (SetCheckpointGate), with Apply quiesced, so the marker it writes
// matches the state it captures.
//
// # Compatibility
//
// No new WAL record version or field: the marker is an ordinary record with
// an inline value.  An older build replays it as a key named
// "\x00veltrix/raft-applied" and replays everything — including writes the
// new build cut, if it never shut down cleanly since — i.e. it behaves as it
// always did; its clean-shutdown checkpoint rewrites the record at its own
// checkpoint version.  The marker's value embeds the version of the record
// that carries it, so the new build trusts only a record whose version still
// matches (isAppliedMarker): after a downgrade and re-upgrade the copy an
// older build rewrote is not trusted, and the node takes the no-marker path
// below instead of re-applying everything the older build applied.  Every
// record of the reserved key, trusted or not, is dropped from the replay, so
// the key never reaches the keyspace (and a raft snapshot's stray copy of it
// is skipped by Restorer.Put).  An engine without a trusted marker (a v1.16
// data dir, a re-upgraded one, or a node that never applied anything)
// reports no applied state, and raft resets / restores and applies once; the
// first applied run writes the marker.  Standalone and replicated engines
// never set RaftAppliedIndex: they strip marker records and cut nothing.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"sync"
	"time"
)

// appliedMarkerKey is the reserved WAL key of the applied-index marker.  The
// leading NUL keeps it out of every user and reserved ("@…") key family.
const appliedMarkerKey = "\x00veltrix/raft-applied"

const (
	appliedMarkerMagic = "VXAI"
	// appliedMarkerFormat 2 embeds the record's own engine version (format 1,
	// never released, did not; it no longer decodes — see isAppliedMarker).
	appliedMarkerFormat  = 2
	appliedFlagRestoring = 1 << 0
	// appliedDeadRangesWarn: the dead-range list only grows across crashes
	// without a clean shutdown in between; say so when it gets long.
	appliedDeadRangesWarn = 256
)

// AppliedIndexState is the raft applied state an engine opened with
// StorageConfig.RaftAppliedIndex holds: every raft entry ≤ Index (whose term
// is Term) is applied, none after.  Restoring: a snapshot restore started and
// did not finish — the state is NOT the one at Index and must be restored.
type AppliedIndexState struct {
	Index     uint64
	Term      uint64
	Restoring bool
}

type versionRange struct{ lo, hi uint64 } // (lo, hi]

func (r versionRange) contains(v uint64) bool { return v > r.lo && v <= r.hi }

type appliedMarker struct {
	index, term uint64
	flags       uint8
	// ver is the engine version of the WAL record that carries the marker.
	// A record whose version differs was rewritten by a build that does not
	// know markers — see isAppliedMarker.
	ver  uint64
	dead []versionRange
	// clock, floor: the raft clock at index (raft_clock.go, invariant 66),
	// an optional trailer after the dead ranges — a marker without it
	// decodes with (0, 0).
	clock, floor int64
}

// encode: magic, format, flags, index, term, ver, uvarint n, n × (lo, hi),
// CRC32C.
func (m appliedMarker) encode() []byte {
	b := make([]byte, 0, 40+16*len(m.dead))
	b = append(b, appliedMarkerMagic...)
	b = append(b, appliedMarkerFormat, m.flags)
	b = binary.LittleEndian.AppendUint64(b, m.index)
	b = binary.LittleEndian.AppendUint64(b, m.term)
	b = binary.LittleEndian.AppendUint64(b, m.ver)
	b = binary.AppendUvarint(b, uint64(len(m.dead)))
	for _, r := range m.dead {
		b = binary.LittleEndian.AppendUint64(b, r.lo)
		b = binary.LittleEndian.AppendUint64(b, r.hi)
	}
	if m.clock != 0 || m.floor != 0 {
		b = binary.AppendUvarint(b, uint64(m.clock))
		b = binary.AppendUvarint(b, uint64(m.floor))
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
}

func decodeAppliedMarker(b []byte) (appliedMarker, bool) {
	if len(b) < 4+2+24+1+4 || string(b[:4]) != appliedMarkerMagic || b[4] != appliedMarkerFormat {
		return appliedMarker{}, false
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)) != sum {
		return appliedMarker{}, false
	}
	m := appliedMarker{
		flags: b[5],
		index: binary.LittleEndian.Uint64(b[6:]),
		term:  binary.LittleEndian.Uint64(b[14:]),
		ver:   binary.LittleEndian.Uint64(b[22:]),
	}
	rest := body[30:]
	n, k := binary.Uvarint(rest)
	if k <= 0 || n > uint64(len(rest)) || uint64(len(rest)-k) < n*16 {
		return appliedMarker{}, false
	}
	rest = rest[k:]
	for i := uint64(0); i < n; i++ {
		m.dead = append(m.dead, versionRange{
			lo: binary.LittleEndian.Uint64(rest[16*i:]),
			hi: binary.LittleEndian.Uint64(rest[16*i+8:]),
		})
	}
	// Optional raft clock trailer (raft_clock.go).
	if rest = rest[n*16:]; len(rest) > 0 {
		clock, k1 := binary.Uvarint(rest)
		if k1 <= 0 {
			return appliedMarker{}, false
		}
		floor, k2 := binary.Uvarint(rest[k1:])
		if k2 <= 0 || k1+k2 != len(rest) {
			return appliedMarker{}, false
		}
		m.clock, m.floor = int64(clock), int64(floor)
	}
	return m, true
}

// isAppliedMarker reports whether a replayed WAL record is an applied-index
// marker this build can trust: the reserved key, an inline value that
// decodes, and the version embedded in the value equal to the record's.
//
// The version check is what makes a downgrade and re-upgrade safe. A build
// that predates markers (v1.16) replays the record as an ordinary key, and
// its clean-shutdown checkpoint rewrites it at the checkpoint version — in
// KV-separation mode without the inline value, otherwise with it. Trusting
// that rewritten copy would cut nothing and report the pre-downgrade applied
// index while the engine holds the effect of every entry the old build
// applied since: raft would apply them again (INCRs doubled). The copy's
// version is the old build's checkpoint version, which differs from the
// embedded one whenever the old build wrote anything; when it wrote nothing
// the copy still describes the engine exactly. An untrusted copy is dropped
// like a marker (cutWALAtAppliedMarker); with no trusted marker the node
// takes the "no applied state" path (full reset and apply once, or snapshot
// restore — consensus/applied.go).
func isAppliedMarker(e *walReplayEntry) (appliedMarker, bool) {
	if e.key != appliedMarkerKey || e.isTombstone || len(e.value) == 0 {
		return appliedMarker{}, false
	}
	m, ok := decodeAppliedMarker(e.value)
	if !ok || m.ver != e.version {
		return appliedMarker{}, false
	}
	return m, true
}

// appliedTracker is the engine's applied-index state (se.applied).
type appliedTracker struct {
	enabled bool

	mu      sync.Mutex
	present bool          // a marker exists (found at open, or written since)
	m       appliedMarker // current marker value
	ver     uint64        // version of the newest marker record (m's)
	// Cut statistics of the open (tests, logs).
	cutVersion  uint64
	cutDropped  int
	openedState AppliedIndexState
	openedOK    bool

	gateMu sync.Mutex
	gate   func(fn func()) // SetCheckpointGate
}

// cutWALAtAppliedMarker strips applied-index markers from the parsed WAL
// entries (in place, every mode) and, when enabled, drops every record above
// the newest marker's version or inside one of its dead ranges.  maxVersion
// is the highest version of any parsed record (se.version is restored to it,
// so every later write is above every dropped one).
func cutWALAtAppliedMarker(all [][]walReplayEntry, maxVersion uint64, enabled bool) *appliedTracker {
	t := &appliedTracker{enabled: enabled}
	var best appliedMarker
	var bestVer uint64
	found := false
	stale := 0
	for d := range all {
		out := all[d][:0]
		for i := range all[d] {
			e := &all[d][i]
			if e.key != appliedMarkerKey {
				out = append(out, *e)
				continue
			}
			// Every record of the reserved key is dropped from the replay —
			// it never becomes a key — and only a trusted one counts.
			if m, ok := isAppliedMarker(e); ok {
				if !found || e.version >= bestVer {
					best, bestVer, found = m, e.version, true
				}
			} else {
				stale++
			}
		}
		all[d] = out
	}
	if stale > 0 {
		log.Printf("[raft-applied] dropped %d %q records that are not trusted applied-index markers (rewritten by an older build after a downgrade, or written by a client); they are not keys and not the applied index",
			stale, appliedMarkerKey)
	}
	if !enabled || !found {
		return t
	}

	used := make([]bool, len(best.dead))
	dropped := 0
	var droppedAboveCut int
	for d := range all {
		out := all[d][:0]
		for _, e := range all[d] {
			if e.version > bestVer {
				dropped++
				droppedAboveCut++
				continue
			}
			inDead := false
			for i, r := range best.dead {
				if r.contains(e.version) {
					used[i], inDead = true, true
					break
				}
			}
			if inDead {
				dropped++
				continue
			}
			out = append(out, e)
		}
		all[d] = out
	}
	var dead []versionRange
	for i, r := range best.dead {
		if used[i] {
			dead = append(dead, r)
		}
	}
	if droppedAboveCut > 0 && maxVersion > bestVer {
		dead = append(dead, versionRange{lo: bestVer, hi: maxVersion})
	}
	if len(dead) > appliedDeadRangesWarn {
		log.Printf("[raft-applied] WARNING: %d dead WAL version ranges — the node crashed repeatedly without a clean shutdown; a clean restart (checkpoint) clears them", len(dead))
	}
	best.dead = dead
	t.present = true
	t.m = best
	t.ver = bestVer
	t.cutVersion = bestVer
	t.cutDropped = dropped
	t.openedState = AppliedIndexState{Index: best.index, Term: best.term, Restoring: best.flags&appliedFlagRestoring != 0}
	t.openedOK = true
	if droppedAboveCut > 0 {
		log.Printf("[raft-applied] WAL cut at applied index %d (version %d): %d records of later raft entries dropped — raft re-applies those entries from its log",
			best.index, bestVer, droppedAboveCut)
	}
	return t
}

// RaftAppliedState returns the raft applied state recorded with the engine's
// data: at open, the newest durable marker (the replay was cut at it); after
// SetRaftAppliedState, the value last set.  ok=false: the engine was not
// opened with StorageConfig.RaftAppliedIndex, or no marker was ever written
// (an engine from an older build) — the caller must assume nothing is known.
func (se *StorageEngine) RaftAppliedState() (AppliedIndexState, bool) {
	t := se.applied
	if t == nil || !t.enabled {
		return AppliedIndexState{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.present {
		return AppliedIndexState{}, false
	}
	return AppliedIndexState{Index: t.m.index, Term: t.m.term, Restoring: t.m.flags&appliedFlagRestoring != 0}, true
}

// RaftAppliedCutStats reports the open's cut: the marker version replay was
// cut at (0 = no cut) and how many WAL records it dropped.
func (se *StorageEngine) RaftAppliedCutStats() (cutVersion uint64, dropped int) {
	if se.applied == nil {
		return 0, 0
	}
	return se.applied.cutVersion, se.applied.cutDropped
}

// SetRaftAppliedState records that the engine holds the effect of every raft
// entry ≤ st.Index and none after (st.Restoring is ignored: a completed
// apply or restore clears the flag).  The caller must not have any write of
// an entry > st.Index in flight and must call it before starting the next
// entry's writes (the raft applier does so under smMu).  durable=false
// returns as soon as the marker is queued — it becomes durable with the
// WAL's next group commit; durable=true waits for its fdatasync.  A no-op
// unless the engine was opened with StorageConfig.RaftAppliedIndex.
//
// The marker also records the raft clock as it is now — the clock after
// entry st.Index (raft_clock.go) — so a restart resumes it exactly.
func (se *StorageEngine) SetRaftAppliedState(st AppliedIndexState, durable bool) error {
	clock, floor := se.RaftClock()
	return se.writeAppliedMarker(func(m *appliedMarker) {
		m.index, m.term, m.flags = st.Index, st.Term, m.flags&^appliedFlagRestoring
		m.clock, m.floor = clock, floor
	}, durable)
}

// BeginRaftRestore durably marks that a snapshot restore is about to modify
// the engine: until SetRaftAppliedState records the snapshot's index, a
// restart reports Restoring and the restore is redone.  A no-op unless the
// engine was opened with StorageConfig.RaftAppliedIndex.
func (se *StorageEngine) BeginRaftRestore() error {
	return se.writeAppliedMarker(func(m *appliedMarker) { m.flags |= appliedFlagRestoring }, true)
}

var errAppliedMarkerNoWAL = errors.New("raft applied index: engine has no WAL")

func (se *StorageEngine) writeAppliedMarker(update func(*appliedMarker), durable bool) error {
	t := se.applied
	if t == nil || !t.enabled {
		return nil
	}
	if len(se.wals) == 0 {
		return errAppliedMarkerNoWAL
	}
	// Hold t.mu across the version assignment and the enqueue: markers reach
	// the WAL in version order, and a concurrent checkpoint reads a value
	// that is (or is about to be) on disk.
	t.mu.Lock()
	update(&t.m)
	t.present = true
	t.ver = se.version.Add(1)
	t.m.ver = t.ver
	val := t.m.encode()
	we := &WALEntry{
		Timestamp: time.Now().UnixNano(),
		Key:       appliedMarkerKey,
		KeyLen:    uint32(len(appliedMarkerKey)),
		Value:     val,
		ValueLen:  uint32(len(val)),
		Checksum:  computeCRC32C(val),
		Version:   t.ver,
	}
	disk := diskForShard(uint16(fnv64a(appliedMarkerKey)&(numShards-1)), len(se.wals))
	rp := se.wals[disk].beginAppend(we)
	t.mu.Unlock()
	if durable {
		err := <-*rp
		walRespPool.Put(rp)
		if err != nil {
			return fmt.Errorf("raft applied index: %w", err)
		}
		return nil
	}
	go func() {
		if err := <-*rp; err != nil {
			log.Printf("[raft-applied] marker write failed (a restart re-applies more of the log): %v", err)
		}
		walRespPool.Put(rp)
	}()
	return nil
}

// checkpointCut returns the version a checkpoint must be written at and the
// marker record every disk's checkpoint must carry: the newest marker, at its
// own version (ok=false when there is none — not raft mode, or nothing
// applied yet).  Writing the compacted records at the marker's version keeps
// the cut where it was: every record of an entry above the marker has a
// higher version and stays cut, and the version is never inside a dead range
// (it is either a marker written since the open, above every range, or the
// open's cut, which is a range's exclusive lower bound) — se.version right
// after a cutting open IS a range's upper bound, and a checkpoint written
// there had every record dropped at the next open.
func (t *appliedTracker) checkpointCut() (uint64, *WALEntry, bool) {
	if t == nil || !t.enabled {
		return 0, nil, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.present {
		return 0, nil, false
	}
	m := t.m
	m.ver = t.ver
	val := m.encode()
	return t.ver, &WALEntry{
		Timestamp: time.Now().UnixNano(),
		Key:       appliedMarkerKey,
		KeyLen:    uint32(len(appliedMarkerKey)),
		Value:     val,
		ValueLen:  uint32(len(val)),
		Checksum:  computeCRC32C(val),
		Version:   t.ver,
	}, true
}

// SetCheckpointGate installs gate, through which Checkpoint runs: the raft
// node passes a function that runs its argument with Apply quiesced, so the
// applied-index marker a runtime checkpoint writes matches the state it
// captures.  nil removes it.
func (se *StorageEngine) SetCheckpointGate(gate func(fn func())) {
	if se.applied == nil {
		return
	}
	se.applied.gateMu.Lock()
	se.applied.gate = gate
	se.applied.gateMu.Unlock()
}

func (se *StorageEngine) checkpointGate() func(fn func()) {
	if se.applied == nil {
		return nil
	}
	se.applied.gateMu.Lock()
	defer se.applied.gateMu.Unlock()
	return se.applied.gate
}

// EnsureFieldIndex is CreateFieldIndex that also backfills when the index
// already exists with the same field.  The raft FSM applies IDXCREATE with
// it: the definition lives in index_defs.json, outside the WAL, so after a
// crash that cut the backfill's writes (raft_applied.go) the definition is
// already there while its entries are not — and plain CreateFieldIndex would
// return early without writing them.
func (se *StorageEngine) EnsureFieldIndex(name, field string) error {
	se.fieldIdxMu.Lock()
	exists := false
	for _, d := range se.fieldIdxDefs {
		if d.Name == name && d.Field == field {
			exists = true
			break
		}
	}
	se.fieldIdxMu.Unlock()
	if err := se.CreateFieldIndex(name, field); err != nil {
		return err
	}
	if exists {
		se.backfillFieldIndex()
	}
	return nil
}

// PurgeFieldIndexEntries deletes every "@idx/<name>/…" entry.  The raft FSM
// calls it when IDXDROP finds no definition: after a crash that cut the
// drop's deletes the definition is already gone from index_defs.json while
// its entries are back.
func (se *StorageEngine) PurgeFieldIndexEntries(name string) {
	for _, k := range se.scanKeysWithPrefix(secondaryIndexPrefix + name + "/") {
		_ = se.Delete(k)
	}
}
