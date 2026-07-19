package consensus

// snapshot.go — state-machine snapshots and log compaction (Raft §7).
//
// When the retained log reaches Options.SnapshotThreshold entries and the
// state machine implements SnapshotStateMachine, the applier goroutine
// captures a snapshot at lastApplied, persists it to
// <dataDir>/raft_snapshot.gob with the same atomic temp-file + fsync + rename
// discipline as writeStateFile, and truncates the applied log prefix.
// lastIncludedIndex/Term then act as the "virtual log head" for AppendEntries
// consistency checks and election up-to-date comparisons (see the log helpers
// in raft.go).
//
// A follower whose nextIndex predates the leader's log start is caught up
// with a single-shot InstallSnapshot RPC (whole snapshot in one message,
// capped at maxSnapshotBytes).  The snapshot carries the configuration as of
// lastIncludedIndex so membership survives compaction.
//
// Crash recovery order (NewRaftNodeWithOptions): restore the snapshot first
// (state machine + lastIncludedIndex/Term + base config), then load the log
// and replay only the retained tail once commitIndex advances.

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// SnapshotStateMachine is a StateMachine that can capture and restore its
// full state.  Implement it (alongside Apply) to enable log compaction:
//
//	Snapshot() — serialise the current state; called after Apply has been
//	             quiesced, so the bytes correspond exactly to lastApplied.
//	Restore()  — replace the current state with a previously captured
//	             snapshot (leader state transfer or crash recovery).
type SnapshotStateMachine interface {
	StateMachine
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// maxSnapshotBytes caps the single-shot InstallSnapshot payload.
const maxSnapshotBytes = 256 << 20 // 256 MB

// InstallSnapshotArgs is sent by the leader to a follower whose next needed
// entry has been compacted away.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Config            Configuration // configuration as of LastIncludedIndex
	ConfigIndex       uint64
	Data              []byte // complete state-machine snapshot (single shot)
}

// InstallSnapshotReply carries the follower's current term.
type InstallSnapshotReply struct {
	Term uint64
}

// snapshotFile is the on-disk snapshot format (gob).
type snapshotFile struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Config            Configuration
	ConfigIndex       uint64
	Data              []byte
}

func (rn *RaftNode) snapshotPath() string {
	return filepath.Join(rn.dataDir, "raft_snapshot.gob")
}

// writeSnapshotFile persists snap atomically: temp file → fsync → rename.
// Same durability discipline as writeStateFile.
func (rn *RaftNode) writeSnapshotFile(snap snapshotFile) error {
	f, err := os.CreateTemp(rn.dataDir, "raft_snapshot_*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if err := gob.NewEncoder(f).Encode(snap); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	f.Close()
	return os.Rename(tmp, rn.snapshotPath())
}

// readSnapshotFile loads the persisted snapshot.  ok=false means no snapshot
// exists yet.  Safe without locks: the file is only ever replaced atomically.
func (rn *RaftNode) readSnapshotFile() (snap snapshotFile, ok bool, err error) {
	f, err := os.Open(rn.snapshotPath())
	if os.IsNotExist(err) {
		return snapshotFile{}, false, nil
	}
	if err != nil {
		return snapshotFile{}, false, err
	}
	defer f.Close()
	if err := gob.NewDecoder(f).Decode(&snap); err != nil {
		return snapshotFile{}, false, err
	}
	return snap, true, nil
}

// loadSnapshot restores the persisted snapshot during startup, BEFORE the log
// is loaded: state machine contents, lastIncludedIndex/Term, and the base
// configuration.  No-op when no snapshot exists.
// Called from NewRaftNodeWithOptions only (no goroutines running yet).
func (rn *RaftNode) loadSnapshot() error {
	snap, ok, err := rn.readSnapshotFile()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	ssm, is := rn.sm.(SnapshotStateMachine)
	if !is {
		return fmt.Errorf("snapshot %s exists but state machine does not implement SnapshotStateMachine",
			rn.snapshotPath())
	}
	if err := ssm.Restore(snap.Data); err != nil {
		return fmt.Errorf("restore state machine: %w", err)
	}
	rn.lastIncludedIndex = snap.LastIncludedIndex
	rn.lastIncludedTerm = snap.LastIncludedTerm
	rn.baseConfig = snap.Config.clone()
	rn.baseConfigIndex = snap.ConfigIndex
	return nil
}

// maybeSnapshot triggers an automatic snapshot when the retained log has
// reached the threshold and there is applied state beyond the current
// snapshot.  Called from the applier goroutine after applying entries.
func (rn *RaftNode) maybeSnapshot() {
	if rn.snapshotThreshold == 0 {
		return
	}
	ssm, ok := rn.sm.(SnapshotStateMachine)
	if !ok {
		return
	}
	rn.mu.Lock()
	trigger := !rn.snapshotting &&
		uint64(len(rn.ps.Log)) >= rn.snapshotThreshold &&
		rn.lastApplied > rn.lastIncludedIndex
	if trigger {
		rn.snapshotting = true
	}
	rn.mu.Unlock()
	if trigger {
		rn.takeSnapshot(ssm)
	}
}

// takeSnapshot captures the state machine at lastApplied, persists the
// snapshot, then truncates the covered log prefix.  Runs on the applier
// goroutine.  Lock order: smMu before rn.mu (see RaftNode.smMu).
func (rn *RaftNode) takeSnapshot(ssm SnapshotStateMachine) {
	defer func() {
		rn.mu.Lock()
		rn.snapshotting = false
		rn.mu.Unlock()
	}()

	// Capture the snapshot point under smMu so no Apply/Restore can move the
	// state machine between reading lastApplied and serialising the state.
	rn.smMu.Lock()
	rn.mu.Lock()
	idx := rn.lastApplied
	if idx <= rn.lastIncludedIndex {
		rn.mu.Unlock()
		rn.smMu.Unlock()
		return
	}
	term := rn.logTerm(idx)
	cfg, cfgIdx := rn.configAt(idx)
	rn.mu.Unlock()
	if term == 0 {
		rn.smMu.Unlock()
		return // entry at idx not resolvable (shouldn't happen for applied entries)
	}
	data, err := ssm.Snapshot()
	rn.smMu.Unlock()
	if err != nil {
		log.Printf("[raft] node=%s snapshot capture failed: %v", rn.id, err)
		return
	}
	if uint64(len(data)) > maxSnapshotBytes {
		log.Printf("[raft] node=%s snapshot too large (%d bytes > %d cap) — skipping compaction",
			rn.id, len(data), maxSnapshotBytes)
		return
	}

	// Persist the snapshot BEFORE truncating the log: a crash in between
	// leaves overlap, which loadState resolves in the snapshot's favour.
	if err := rn.writeSnapshotFile(snapshotFile{
		LastIncludedIndex: idx,
		LastIncludedTerm:  term,
		Config:            cfg.clone(),
		ConfigIndex:       cfgIdx,
		Data:              data,
	}); err != nil {
		log.Printf("[raft] node=%s snapshot persist failed: %v", rn.id, err)
		return
	}

	// Truncate the covered prefix and persist the shrunken log.
	rn.mu.Lock()
	if idx > rn.lastIncludedIndex {
		first := rn.logFirstIndex()
		if idx >= first {
			cut := idx - first + 1
			if cut >= uint64(len(rn.ps.Log)) {
				rn.ps.Log = nil
			} else {
				rn.ps.Log = append([]LogEntry(nil), rn.ps.Log[cut:]...)
			}
		}
		rn.lastIncludedIndex = idx
		rn.lastIncludedTerm = term
		rn.baseConfig = cfg.clone()
		rn.baseConfigIndex = cfgIdx
		_ = rn.saveState()
		log.Printf("[raft] node=%s snapshot taken  idx=%d term=%d retained_log=%d",
			rn.id, idx, term, len(rn.ps.Log))
	}
	rn.mu.Unlock()
}

// sendSnapshot ships the persisted snapshot to a follower whose nextIndex
// predates the log start.  Called from broadcastAppendEntries without rn.mu.
// snapInFlight ensures at most one in-flight InstallSnapshot per peer.
func (rn *RaftNode) sendSnapshot(peer string, term uint64) {
	rn.mu.Lock()
	if rn.snapInFlight[peer] || rn.role != RoleLeader || rn.ps.CurrentTerm != term {
		rn.mu.Unlock()
		return
	}
	rn.snapInFlight[peer] = true
	rn.mu.Unlock()
	defer func() {
		rn.mu.Lock()
		delete(rn.snapInFlight, peer)
		rn.mu.Unlock()
	}()

	snap, ok, err := rn.readSnapshotFile()
	if err != nil {
		log.Printf("[raft] node=%s read snapshot for peer=%s failed: %v", rn.id, peer, err)
		return
	}
	if !ok {
		return // compacted state without a snapshot file cannot happen; retry next tick
	}

	args := InstallSnapshotArgs{
		Term:              term,
		LeaderID:          rn.id,
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		Config:            snap.Config,
		ConfigIndex:       snap.ConfigIndex,
		Data:              snap.Data,
	}
	reply, err := rn.transport.SendInstallSnapshot(peer, args)
	if err != nil {
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()
	if reply.Term > rn.ps.CurrentTerm {
		rn.becomeFollower(reply.Term)
		return
	}
	if rn.role != RoleLeader || rn.ps.CurrentTerm != term {
		return // stale response
	}
	if snap.LastIncludedIndex > rn.matchIndex[peer] {
		rn.matchIndex[peer] = snap.LastIncludedIndex
	}
	if rn.nextIndex[peer] <= snap.LastIncludedIndex {
		rn.nextIndex[peer] = snap.LastIncludedIndex + 1
	}
	rn.maybeAdvanceCommit()
}

// HandleInstallSnapshot processes an incoming InstallSnapshot RPC: persist
// the snapshot, restore the state machine, discard the covered log prefix.
// Lock order: smMu before rn.mu, matching applyCommitted and takeSnapshot, so
// a restore can never interleave with an Apply.
func (rn *RaftNode) HandleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	rn.smMu.Lock()
	defer rn.smMu.Unlock()
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := InstallSnapshotReply{Term: rn.ps.CurrentTerm}
	if args.Term < rn.ps.CurrentTerm {
		return reply
	}
	if args.Term > rn.ps.CurrentTerm {
		rn.becomeFollower(args.Term)
	}
	rn.role = RoleFollower
	rn.currentRole.Store(int32(RoleFollower))
	rn.LeaderID.Store(args.LeaderID)
	rn.resetElectionTimer()
	reply.Term = rn.ps.CurrentTerm

	if uint64(len(args.Data)) > maxSnapshotBytes {
		log.Printf("[raft] node=%s rejecting oversized snapshot (%d bytes) from %s",
			rn.id, len(args.Data), args.LeaderID)
		return reply
	}
	// Stale snapshot: we already have (applied) state at or past this point.
	if args.LastIncludedIndex <= rn.lastIncludedIndex || args.LastIncludedIndex <= rn.lastApplied {
		return reply
	}
	ssm, ok := rn.sm.(SnapshotStateMachine)
	if !ok {
		log.Printf("[raft] node=%s cannot install snapshot: state machine is not snapshot-capable", rn.id)
		return reply
	}

	// Persist first (crash between persist and state write is resolved by
	// loadState discarding the covered log prefix on restart).
	if err := rn.writeSnapshotFile(snapshotFile{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Config:            args.Config.clone(),
		ConfigIndex:       args.ConfigIndex,
		Data:              args.Data,
	}); err != nil {
		log.Printf("[raft] node=%s persist installed snapshot failed: %v", rn.id, err)
		return reply
	}
	if err := ssm.Restore(args.Data); err != nil {
		log.Printf("[raft] node=%s restore installed snapshot failed: %v", rn.id, err)
		return reply
	}

	// Retain any log suffix that extends past the snapshot and agrees with it
	// (Raft §7); otherwise discard the entire log.
	if args.LastIncludedIndex < rn.lastLogIndex() &&
		rn.logTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		rn.ps.Log = append([]LogEntry(nil), rn.entriesFrom(args.LastIncludedIndex+1)...)
	} else {
		rn.ps.Log = nil
	}
	rn.lastIncludedIndex = args.LastIncludedIndex
	rn.lastIncludedTerm = args.LastIncludedTerm
	rn.lastApplied = args.LastIncludedIndex
	if rn.commitIndex < args.LastIncludedIndex {
		rn.commitIndex = args.LastIncludedIndex
	}
	rn.baseConfig = args.Config.clone()
	rn.baseConfigIndex = args.ConfigIndex
	rn.refreshConfigFromLog()
	_ = rn.saveState()
	rn.resetElectionTimer() // second reset: the restore above may have been slow

	log.Printf("[raft] node=%s installed snapshot from %s  idx=%d term=%d retained_log=%d",
		rn.id, args.LeaderID, args.LastIncludedIndex, args.LastIncludedTerm, len(rn.ps.Log))
	return reply
}
