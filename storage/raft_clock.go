package storage

// raft_clock.go — deterministic time for raft Apply (CLAUDE.md invariant 66).
//
// # The problem
//
// A key's version token (KeyVersion → GETVER / VER, compared by TXN SETIF) is
// its IndexEntry.WriteTimestampUs.  Every node used to set it from its own
// clock when it applied the write, so the nodes of a raft cluster disagreed
// on every key's version: a guarded TXN applied through the log held on the
// node whose token the client had read and failed on the others, and the
// replicas diverged.  The same local clock decided relative-TTL deadlines and
// whether a key had expired (SETNX, CAS, INCR, SETIF "expect absent", list
// and hash ops), so those could differ between nodes too.
//
// # The raft clock
//
// The leader stamps every command with its wall clock (fsmCmd.Ts, µs).  The
// FSM turns that into the command's ApplyTime with NextRaftApplyTime, a pure
// function of the engine's raft clock state and the command:
//
//	E = max(Ts, E_prev + 1)        stamp = now = E
//
// E is strictly increasing in log order, so every write of a later command
// gets a larger WriteTimestampUs than every write of an earlier one: version
// tokens are monotonic per key (and unique per command) whichever leader
// stamped them and however its clock compares with the previous leader's.
// For the duration of the command the FSM installs the ApplyTime
// (SetApplyTime): every write path stamps its entries with StampUs, derives
// TTL deadlines from NowUs, and every expiry decision inside the command —
// including the reads write paths make (Get, the atomic ops' read, the TXN
// guard, DropNamespace's scan) — is taken at NowUs.  The local clock never
// enters an Apply decision.
//
// The clock state (E, and the floor F below) is part of the state machine:
// it is written into every applied-index marker (raft_applied.go), restored
// from it on open, carried by snapshots (cmd/server/raft_snapshot.go) and
// reset with the state.
//
// # Expiry outside Apply
//
// The TTL scanner, Get and RangeScan tombstone expired keys on their own
// time.  A tombstone is state an Apply decision can see, so in raft mode
// (E > 0) they tombstone only keys expired at min(local now, E): such a key
// is expired at every later command's NowUs too, so tombstoning it early
// changes no outcome.  Reads still HIDE a key past its deadline by the local
// clock.  WAL replay uses the same cutoff (the marker's E).
//
// # Legacy commands and keys (compatibility)
//
// A command without Ts (written by a v1.16 leader, or an entry from before
// the upgrade re-applied by a node rebuilt from its log) is applied as
// before: TTLs and expiry from the local clock.  Its writes are stamped
// E_prev + 1 when the clock has started, else legacyStampUs — a constant, so
// a node that re-applies old entries does not give their keys fresh
// local-clock versions.  Keys whose version predates the clock — anything
// written by a v1.16 build, or by legacy commands — have per-node timestamps
// that cannot be reconciled, so once the clock has started (F, the floor,
// is the first stamped command's E) every version below F reads as
// LegacyVersion on every node: one SETIF taken against such a key holds on
// all of them, and its write gives the key a real version.  Before the
// first stamped command (F = 0, a cluster still led by a v1.16 build) tokens
// are the raw timestamps, as before.  Standalone and replicated engines
// never run the clock: F = 0, tokens are local write timestamps, unchanged.

import (
	"sync"
	"sync/atomic"
	"time"
)

// LegacyVersion is the version token of a live key last written before the
// raft clock started (see the file comment).
const LegacyVersion uint64 = 1

// legacyStampUs is the WriteTimestampUs of writes made by legacy commands
// before the clock started: below every clock value, so it reads as
// LegacyVersion.
const legacyStampUs int64 = 1

// ApplyTime is the deterministic time of one raft command.
type ApplyTime struct {
	// StampUs is the WriteTimestampUs (and so the version token) of every
	// write the command makes.
	StampUs int64
	// NowUs is "now" for the command: relative TTLs are converted against it
	// and every expiry decision is taken at it.
	NowUs int64
}

// raftClock is the engine's raft clock state (se.rclock).
type raftClock struct {
	mu    sync.Mutex   // serialises NextRaftApplyTime / SetRaftClock
	last  atomic.Int64 // E: the last stamped command's time (0 = not started)
	floor atomic.Int64 // F: the first stamped command's time (0 = not started)
	cur   atomic.Pointer[ApplyTime]
}

// NextRaftApplyTime advances the raft clock by one command whose leader
// timestamp is leaderTsUs (0 = a legacy command without one) and returns
// the command's ApplyTime.  Called by the raft FSM once per decoded command,
// in log order, before applying it.
func (se *StorageEngine) NextRaftApplyTime(leaderTsUs int64) ApplyTime {
	c := &se.rclock
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.last.Load()
	if leaderTsUs <= 0 {
		local := time.Now().UnixMicro()
		if e == 0 {
			return ApplyTime{StampUs: legacyStampUs, NowUs: local}
		}
		c.last.Store(e + 1)
		return ApplyTime{StampUs: e + 1, NowUs: local}
	}
	next := leaderTsUs
	if next <= e {
		next = e + 1
	}
	c.last.Store(next)
	if c.floor.Load() == 0 {
		c.floor.Store(next)
	}
	return ApplyTime{StampUs: next, NowUs: next}
}

// RaftClock returns the raft clock state: the last stamped command's time and
// the floor (both 0 until the first stamped command).
func (se *StorageEngine) RaftClock() (lastUs, floorUs int64) {
	return se.rclock.last.Load(), se.rclock.floor.Load()
}

// SetRaftClock replaces the raft clock state: a snapshot restore installs the
// snapshot's, a reset (0, 0).
func (se *StorageEngine) SetRaftClock(lastUs, floorUs int64) {
	c := &se.rclock
	c.mu.Lock()
	c.last.Store(lastUs)
	c.floor.Store(floorUs)
	c.mu.Unlock()
}

// SetApplyTime installs t as the time of every write and expiry decision
// until ClearApplyTime.  Only the raft applier calls it (one command at a
// time, Apply serialised); a write that is not part of the command but runs
// meanwhile is stamped with t too, which is harmless.
func (se *StorageEngine) SetApplyTime(t ApplyTime) { se.rclock.cur.Store(&t) }

// ClearApplyTime removes the ApplyTime installed by SetApplyTime.
func (se *StorageEngine) ClearApplyTime() { se.rclock.cur.Store(nil) }

// applyTime returns the installed ApplyTime, nil outside a raft command.
func (se *StorageEngine) applyTime() *ApplyTime { return se.rclock.cur.Load() }

// writeClock returns the stamp and "now" of a write: the raft command's when
// one is being applied, the local clock otherwise.
func (se *StorageEngine) writeClock() (stampUs, nowUs int64) {
	if t := se.rclock.cur.Load(); t != nil {
		return t.StampUs, t.NowUs
	}
	n := time.Now().UnixMicro()
	return n, n
}

// decisionNowUs is the time expiry is judged at: the raft command's NowUs
// while one is applied, the local clock otherwise.
func (se *StorageEngine) decisionNowUs() int64 {
	if t := se.rclock.cur.Load(); t != nil {
		return t.NowUs
	}
	return time.Now().UnixMicro()
}

// expiryCutoffUs is the time up to which an expired key may be TOMBSTONED (or
// left out of a snapshot): the raft command's NowUs while one is applied;
// otherwise, once the raft clock runs, min(local now, E) — never past the
// time the next command will judge expiry at; else the local clock.
func (se *StorageEngine) expiryCutoffUs() int64 {
	if t := se.rclock.cur.Load(); t != nil {
		return t.NowUs
	}
	now := time.Now().UnixMicro()
	if e := se.rclock.last.Load(); e > 0 && e < now {
		return e
	}
	return now
}

// versionToken is the optimistic-concurrency version of entry at nowUs with
// the raft clock floor floorUs: 0 when absent (tombstoned or expired), the
// write timestamp otherwise — LegacyVersion when it predates the floor.
func versionToken(e IndexEntry, nowUs, floorUs int64) uint64 {
	if e.IsTombstone() || e.IsExpired(nowUs) {
		return 0
	}
	s := e.WriteTimestampUs
	if s <= 0 || (floorUs > 0 && s < floorUs) {
		return LegacyVersion
	}
	return uint64(s)
}

// CheckNSWrite runs PutNS's quota check (rate limit and key cap) for a write
// of key in namespace ns without writing.  Raft mode runs it on the leader
// before the command is submitted, and applies the command with
// PutNSUnchecked: a token bucket and the per-node quota configuration are
// local state, so checking them in Apply let one node reject a write the
// others applied (raft_clock.go, invariant 66).
func (se *StorageEngine) CheckNSWrite(ns, key string) error {
	_, _, exists := se.index.get(nsKey(ns, key))
	if err := se.quotas.CheckWrite(ns, !exists); err != nil {
		se.audit.Log(AuditRecord{Op: "PUT", Key: key, Ns: ns, Status: "err", Err: err.Error()})
		return err
	}
	return nil
}

// PutNSUnchecked is PutNS without the quota check (see CheckNSWrite); the
// namespace key count is still maintained.
func (se *StorageEngine) PutNSUnchecked(ns, key string, value []byte, ttl int32) error {
	internalKey := nsKey(ns, key)
	_, _, exists := se.index.get(internalKey)
	if err := se.Put(internalKey, value, ttl); err != nil {
		return err
	}
	if !exists {
		se.quotas.IncKeyCount(ns, 1)
	}
	return nil
}
