package consensus

// applied.go — a state machine that persists its applied index with its state
// (CLAUDE.md invariant 65).
//
// Raft does not persist lastApplied (nor commitIndex): a restarted node
// assumes its state machine holds nothing past the snapshot and re-applies
// the retained log.  That is only right for a state machine that is rebuilt
// from scratch on every start.  The storage engine is durable on its own, so
// re-applying applied entries applied every non-idempotent command twice.
//
// A state machine implementing AppliedIndexStateMachine records, after each
// applied run, the index of the run's last entry, durably together with the
// state it describes (for the storage engine: a WAL marker that replay cuts
// at — storage/raft_applied.go).  On start the node then:
//
//   - skips every entry ≤ that index: lastApplied = commitIndex = index.  The
//     node may not know yet that these entries are committed (commitIndex is
//     not persisted), and its log may not even hold them (a follower applies
//     an entry once the leader says it is committed, which may be before the
//     follower's own copy is fsynced).  Both are safe: an entry is applied
//     only once committed, a committed entry never changes, and the node gets
//     any missing copy from the leader without applying it again
//     (applyOneRun starts at lastApplied+1);
//   - skips the snapshot restore when the recorded index is ≥ the snapshot's
//     lastIncludedIndex and no restore is marked in progress — the restore
//     is O(keys) and only needed when the state is behind the snapshot or a
//     restore was interrupted.  An install or startup restore marks itself
//     (BeginRestore, durable) before it touches the state and records the
//     snapshot index (durably) after, so an interrupted restore is still
//     redone on the next start (invariant 64);
//   - otherwise (nothing recorded — a data dir from an older build; a
//     recorded term that contradicts the log; an interrupted reset): with a
//     snapshot, restore it (the state becomes exactly the snapshot) and
//     apply the tail; without one, the retained log starts at entry 1, so
//     the state is reset to empty (ResetState) and the whole log applied.
//     Either way every entry is applied exactly once — replaying the log
//     over the old state, as before, applied it twice.  A node with an
//     empty log and no snapshot (first start, or a standalone data dir
//     switched to raft) keeps its state: there is nothing to replay.  The
//     first applied run records the index, so this happens once.

import (
	"fmt"
	"log"
)

// AppliedIndexState is what an AppliedIndexStateMachine recorded: every
// entry ≤ Index (of term Term) is applied and none after; Restoring: a
// restore started and did not finish.
type AppliedIndexState struct {
	Index     uint64
	Term      uint64
	Restoring bool
}

// AppliedIndexStateMachine is an optional StateMachine extension (see the
// file comment).
type AppliedIndexStateMachine interface {
	StateMachine
	// DurableAppliedIndex reports the state recorded with the state
	// machine's durable data at startup; ok=false when nothing is recorded.
	DurableAppliedIndex() (st AppliedIndexState, ok bool)
	// SetAppliedIndex records that every entry ≤ index (of term term) is
	// applied and none after, and clears Restoring.  Called with Apply
	// quiesced (smMu) after each applied run, before the next, and after a
	// restore.  It must never become durable before the state it describes;
	// durable=false may return before it is durable itself (a lost record
	// only means more entries are re-applied — exactly once — after a
	// restart), durable=true returns once it is.
	SetAppliedIndex(index, term uint64, durable bool) error
	// BeginRestore durably records that a restore is about to modify the
	// state (DurableAppliedIndex then reports Restoring until the next
	// SetAppliedIndex).
	BeginRestore() error
	// ResetState makes the state empty — the state before entry 1.  Called
	// (inside BeginRestore … SetAppliedIndex(0, 0)) at startup when the node
	// has no snapshot, a non-empty log (which then starts at entry 1) and no
	// trusted record: the state is rebuilt by applying the whole log once.
	ResetState() error
}

// ApplyQuiescer is an optional StateMachine extension: the node passes it a
// function that runs fn with Apply quiesced (between two applied runs,
// after the first run's SetAppliedIndex), e.g. so a storage checkpoint
// captures a state that matches its recorded applied index.
type ApplyQuiescer interface {
	SetApplyQuiescer(run func(fn func()))
}

// withApplyQuiesced runs fn holding smMu: no apply, capture fence or restore
// is in progress.
func (rn *RaftNode) withApplyQuiesced(fn func()) {
	rn.smMu.Lock()
	defer rn.smMu.Unlock()
	fn()
}

// recordApplied records e (the last entry of an applied run) with the state
// machine.  Called with smMu held.
func (rn *RaftNode) recordApplied(e LogEntry) {
	if rn.appliedSM == nil {
		return
	}
	if err := rn.appliedSM.SetAppliedIndex(e.Index, e.Term, false); err != nil && !rn.appliedErrLogged {
		rn.appliedErrLogged = true
		log.Printf("[raft] node=%s recording applied index %d failed (logged once; a restart re-applies from the last recorded index): %v",
			rn.id, e.Index, err)
	}
}

// startupApplied is the state machine's recorded applied state, consulted by
// loadSnapshot and NewRaftNodeWithOptions.
type startupApplied struct {
	st AppliedIndexState
	ok bool
}

// trustApplied reports whether the recorded applied state can be used after
// the snapshot and log are loaded: it must not be marked Restoring and its
// term must match the term this node has for that index, when it has one.
func (rn *RaftNode) trustApplied(a startupApplied) bool {
	if !a.ok || a.st.Restoring {
		return false
	}
	if t := rn.logTerm(a.st.Index); t != 0 && t != a.st.Term {
		log.Printf("[raft] node=%s WARNING: state machine recorded applied index %d term %d but the log has term %d there — ignoring it (restoring / replaying as an unrecorded state machine)",
			rn.id, a.st.Index, a.st.Term, t)
		return false
	}
	return true
}

// resetStateForReplay empties the state machine at startup so the whole
// retained log (which starts at entry 1: there is no snapshot) is applied
// exactly once (see the file comment).  Before any goroutine starts.
func (rn *RaftNode) resetStateForReplay(a startupApplied) error {
	log.Printf("[raft] node=%s state machine has no trusted applied index (recorded=%v restoring=%v) and no snapshot: resetting it and applying the %d-entry log once",
		rn.id, a.ok, a.st.Restoring, len(rn.ps.Log))
	if err := rn.appliedSM.BeginRestore(); err != nil {
		return fmt.Errorf("mark reset in progress: %w", err)
	}
	if err := rn.appliedSM.ResetState(); err != nil {
		return fmt.Errorf("reset state machine: %w", err)
	}
	if err := rn.appliedSM.SetAppliedIndex(0, 0, true); err != nil {
		return fmt.Errorf("record reset: %w", err)
	}
	rn.snapStats.startupResets.Add(1)
	return nil
}
