// Package consensus implements the Raft distributed consensus algorithm
// (Ongaro & Ousterhout, 2014) for VeltrixDB cluster coordination.
//
// A RaftNode drives leader election and log replication for a group of servers.
// Once a log entry is committed (acknowledged by a quorum), it is applied to
// the caller-supplied StateMachine — typically a StorageEngine adapter.
//
// # Wire protocol
//
// All RPCs use gob encoding over persistent TCP connections managed by
// Transport (see transport.go).  Three RPC types exist:
//
//	RequestVote      — used during leader election
//	AppendEntries    — heartbeats and log replication (one call covers both)
//	InstallSnapshot  — leader → follower state transfer after log compaction
//
// # Persistence
//
// Raft requires three fields to survive crashes: currentTerm, votedFor, and
// the log.  RaftNode keeps the log in an append-only, CRC-checked record file
// (<dataDir>/raft_log.dat) and term + vote in a small atomically-replaced file
// (<dataDir>/raft_meta.dat) — see raft_storage.go.  A commit costs one write
// of the new records plus one fsync (shared by every append queued before it),
// independent of the retained log length; the write and fsync run on the
// syncer goroutine outside rn.mu (pipeline.go).
//
// When the log exceeds Options.SnapshotThreshold entries and the state machine
// implements SnapshotStateMachine, the applied prefix of the log is replaced
// by a state-machine snapshot persisted to <dataDir>/raft_snapshot.gob (see
// snapshot.go).  On restart the snapshot is restored before the remaining log
// tail is replayed.
//
// # Membership
//
// Cluster membership changes go through the log as single-server
// configuration-change entries (see membership.go).
//
// # Integration with StorageEngine
//
// Implement StateMachine on top of *storage.StorageEngine:
//
//	type StorageSM struct{ se *storage.StorageEngine }
//
//	func (s *StorageSM) Apply(cmd []byte) error {
//	    var op WriteOp
//	    gob.Unmarshal(cmd, &op)
//	    if op.Delete { return s.se.Delete(op.Key) }
//	    return s.se.Put(op.Key, op.Value, op.TTL)
//	}
//
// All writes from clients then go to the Raft leader (which rejects writes if
// it is a follower) and commit once the quorum has persisted the entry.
package consensus

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ── Types ─────────────────────────────────────────────────────────────────────

// RaftRole is the current role of a Raft server.
type RaftRole int32

const (
	RoleFollower  RaftRole = iota
	RoleCandidate RaftRole = iota
	RoleLeader    RaftRole = iota
)

func (r RaftRole) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// EntryType distinguishes ordinary state-machine commands from cluster
// configuration changes carried through the replicated log.
type EntryType uint8

const (
	// EntryNormal is an opaque command applied via StateMachine.Apply.
	EntryNormal EntryType = iota
	// EntryConfig carries a gob-encoded Configuration (see membership.go).
	EntryConfig
)

// LogEntry is one entry in the Raft replicated log.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Type    EntryType // zero value EntryNormal keeps old persisted logs readable
	Command []byte    // opaque bytes passed to StateMachine.Apply
}

// persistentState is the in-memory copy of Raft's persistent state.  It is
// also the gob layout of the legacy whole-state file raft_state.gob, which
// openLogStore migrates on startup (keep the field names).
type persistentState struct {
	CurrentTerm uint64
	VotedFor    string // "" means no vote this term
	Log         []LogEntry
}

// RequestVoteArgs is sent by a Candidate to solicit votes.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply carries the peer's response to a vote request.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is sent by the Leader for heartbeats and log replication.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
	// WantMatch: the leader reads AppendEntriesReply.MatchIndex, so a
	// follower may answer a heartbeat at once with only its durable prefix.
	// Unset (older leaders): Success means durable through prev+len(Entries).
	WantMatch bool
}

// AppendEntriesReply is sent back by a Follower.
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64 // optimisation: first index of conflicting term
	ConflictTerm  uint64
	// HasMatch/MatchIndex (set by followers of this version on Success): the
	// follower's log matches the leader's through MatchIndex and every entry
	// up to it is durable on the follower.  Older followers leave HasMatch
	// false; the leader then uses prev+len(Entries).
	HasMatch   bool
	MatchIndex uint64
}

// StateMachine is applied once a log entry is committed.
type StateMachine interface {
	Apply(command []byte) error
}

// BatchStateMachine is an optional extension of StateMachine.  A state machine
// that implements it lets the applier hand a contiguous run of committed
// command payloads to ApplyBatch in ONE call, so the state machine can amortise
// an expensive per-command cost (e.g. a storage-engine fdatasync) across the
// whole run via a single group-commit.
//
// Contract — ApplyBatch MUST be semantically identical to calling Apply once
// per command, in the given slice order:
//
//   - each command is applied exactly once, in order;
//   - the returned slice has len(commands) elements, results[i] being the
//     Raft-level outcome of commands[i] (mirroring Apply's return — nil for a
//     successful or op-level-failed apply, non-nil only for a corrupt/undecodable
//     command).  A short or nil slice is treated as all-nil.
//
// The applier only ever passes EntryNormal commands with a non-empty payload to
// ApplyBatch; no-op and configuration entries are always applied singly.  A
// state machine that does not implement BatchStateMachine transparently falls
// back to per-entry Apply, so existing state machines keep working unchanged.
type BatchStateMachine interface {
	StateMachine
	ApplyBatch(commands [][]byte) []error
}

// Transport is the network layer used by RaftNode.
type Transport interface {
	// SendRequestVote sends an RPC to peer and returns the reply.
	SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error)
	// SendAppendEntries sends an RPC to peer and returns the reply.
	SendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error)
	// SendInstallSnapshot sends an RPC to peer and returns the reply.
	SendInstallSnapshot(peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error)
	// Close tears down the transport.
	Close() error
}

const (
	electionTimeoutMin = 400 * time.Millisecond
	electionTimeoutMax = 800 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond

	// defaultSubmitTimeout bounds how long Submit waits for a quorum commit.
	defaultSubmitTimeout = 5 * time.Second

	// maxSubmitBatch caps how many pending Submits one group-commit flush
	// coalesces, bounding the per-flush log-copy and encode cost.
	maxSubmitBatch = 4096

	// submitChBuffer sizes the group-commit intake channel.  Submits block
	// (backpressure) only if this many are already queued unflushed.
	submitChBuffer = 8192
)

// submitReq is one command handed from Submit to the group-commit flusher.
// The flusher appends the command under a single rn.mu critical section, fills
// in idx/ch (or err), then closes ready; the blocked Submit caller then waits
// for its own per-index commit notification via waitApplied.
type submitReq struct {
	command []byte

	// Set by the flusher before closing ready:
	idx uint64     // assigned log index (valid when err == nil)
	ch  chan error // per-index commit waiter (valid when err == nil)
	err error      // non-nil if the append failed (not leader / persist error)

	ready chan struct{} // closed by the flusher once idx/ch/err are populated
}

// DefaultSnapshotThreshold is the log length (in entries) at which a node
// takes a state-machine snapshot and compacts the applied log prefix, when the
// state machine implements SnapshotStateMachine.
const DefaultSnapshotThreshold = 8192

// Options tunes optional RaftNode behaviour.
type Options struct {
	// SnapshotThreshold is the number of retained log entries that triggers an
	// automatic snapshot + log compaction.  0 disables automatic snapshotting.
	// Snapshotting additionally requires the StateMachine passed to
	// NewRaftNodeWithOptions to implement SnapshotStateMachine.
	SnapshotThreshold uint64

	// Pipeline enables pipelined replication (server flag --raft-pipeline,
	// default off): up to PipelineWindow AppendEntries in flight per
	// follower, sent as soon as entries are appended (before the leader's
	// own fsync) and again on every reply.  Off (the zero value): window 1,
	// and one AppendEntries round per leader fsync (paced) — see pipeline.go.
	// Both modes keep every durability rule (followers ack only fsynced
	// entries; the leader counts itself only up to durableIndex) and speak
	// the same wire protocol, so a cluster may mix them.
	Pipeline bool

	// PipelineWindow is the number of entry-carrying AppendEntries the
	// leader keeps in flight per follower when Pipeline is set.
	// 0 = DefaultPipelineWindow.  Ignored (1) when Pipeline is false.
	PipelineWindow int
}

// ── RaftNode ──────────────────────────────────────────────────────────────────

// RaftNode implements the Raft consensus protocol.
type RaftNode struct {
	mu sync.Mutex

	// Identity
	id      string
	peers   []string // derived from config: all servers except self
	dataDir string

	// Persistent state (written to disk before responding to RPCs)
	ps persistentState

	// store persists ps incrementally (raft_storage.go).  Log records are
	// enqueued under rn.mu, so disk order == log order; the syncer goroutine
	// writes + fsyncs them outside the lock (pipeline.go).
	store *logStore
	// durableIndex: every entry of the current log at or below it is fsynced.
	// The leader counts itself toward a commit quorum only up to here, and a
	// follower acknowledges entries only up to here, so an entry is never
	// committed on the strength of a copy that is not on disk.
	durableIndex  uint64
	persistFailed bool // the store failed (logged once)
	syncCh        chan struct{}
	syncerDone    chan struct{}
	// aeWaiters: accepted AppendEntries whose reply waits for durability.
	aeWaiters []aeWaiter

	// Pipelined replication (leader only, pipeline.go).
	repl           map[string]*peerRepl
	pipeline       bool // Options.Pipeline
	pipelineWindow int  // entry-carrying AppendEntries in flight per peer (1 when !pipeline)
	async          AsyncTransport

	// Snapshot / log compaction state (see snapshot.go).
	// lastIncludedIndex/Term describe the entry immediately preceding the
	// first retained log entry — the "virtual log head".
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	snapshotThreshold uint64
	snapshotting      bool            // one snapshot at a time
	snapInFlight      map[string]bool // per-peer InstallSnapshot guard
	// smMu serialises all state-machine access (Apply / Snapshot / Restore).
	// Lock order: smMu before rn.mu, never the reverse.
	smMu sync.Mutex

	// Membership (see membership.go).
	// config is the latest configuration in the log or snapshot; per Raft it
	// takes effect as soon as the entry is appended, not when it commits.
	config          Configuration
	configIndex     uint64 // log index of the entry that produced config (0 = bootstrap)
	baseConfig      Configuration
	baseConfigIndex uint64 // config as of lastIncludedIndex (snapshot) or bootstrap
	removed         atomic.Bool

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64

	// termStartIndex is the index of the no-op this node appended when it
	// last became leader; leaderReady is set once that entry is applied, i.e.
	// every entry committed by earlier leaders is in the state machine
	// (WaitLeaderApplied). Guarded by mu / atomic respectively.
	termStartIndex uint64
	leaderReady    atomic.Bool

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Role management
	role        RaftRole
	currentRole atomic.Int32 // for lock-free reads

	// Election
	electionTimer  *time.Timer
	electionResetC chan struct{}

	// Application
	sm          StateMachine
	batchSM     BatchStateMachine // non-nil if sm implements batch apply
	applyCh     chan LogEntry     // committed entries queued here for applier goroutine
	applierDone chan struct{}

	// Batch-apply metrics (read without lock — approximate; used by tests to
	// assert the applier actually coalesced consecutive writes).
	batchApplyCalls atomic.Uint64 // number of ApplyBatch calls with >1 entry
	maxApplyBatch   atomic.Uint64 // largest single ApplyBatch run observed

	// Commit notification: waiters maps a log index to channels that are
	// signalled once that index has been applied (or leadership was lost).
	waiters       map[uint64][]chan error
	submitTimeout time.Duration

	// Group commit: Submit enqueues a *submitReq here and blocks; a single
	// flusher goroutine coalesces all currently-pending requests into ONE log
	// append per batch; the syncer covers every queued batch with ONE write(2)
	// + ONE fsync, and the replicators ship them (pipeline.go).
	submitCh    chan *submitReq
	flusherDone chan struct{}

	// Transport
	transport Transport

	// Shutdown
	done       chan struct{}
	stopped    atomic.Bool   // set before applyCh is closed to guard notifyApplier
	tickerDone chan struct{} // closed when the ticker goroutine exits
	// bgWG tracks short-lived background goroutines that persist state
	// (elections, replicators, per-peer send queues) so Stop can
	// wait for them — otherwise a goroutine can still write raft files after
	// Stop returns, racing with directory removal.
	bgWG     sync.WaitGroup
	wgMu     sync.Mutex // orders bgWG.Add (spawn) against Stop
	stopOnce sync.Once

	// electionGeneration is incremented every time the election timer is reset.
	// The ticker reads it before waiting for rn.mu; if it changes while the
	// ticker was blocked (e.g. during a slow saveState), the stale timer fire
	// is discarded rather than triggering a spurious election.
	electionGeneration atomic.Uint64

	// Metrics (read without lock — approximate)
	LeaderID atomic.Value // string
}

// NewRaftNode creates and starts a RaftNode with default Options.
//
//	id        — unique string ID for this server (e.g. "node-1")
//	peers     — IDs of ALL other servers (not including self)
//	dataDir   — directory for raft_log.dat / raft_meta.dat / raft_snapshot.gob
//	sm        — state machine to apply committed commands to
//	transport — network layer (use NewTCPTransport from transport.go)
func NewRaftNode(id string, peers []string, dataDir string, sm StateMachine, transport Transport) (*RaftNode, error) {
	return NewRaftNodeWithOptions(id, peers, dataDir, sm, transport,
		Options{SnapshotThreshold: DefaultSnapshotThreshold})
}

// NewRaftNodeWithOptions is NewRaftNode with explicit Options.
func NewRaftNodeWithOptions(id string, peers []string, dataDir string, sm StateMachine, transport Transport, opts Options) (*RaftNode, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dataDir, err)
	}

	rn := &RaftNode{
		id:                id,
		dataDir:           dataDir,
		sm:                sm,
		transport:         transport,
		snapshotThreshold: opts.SnapshotThreshold,
		snapInFlight:      make(map[string]bool),
		electionResetC:    make(chan struct{}, 1),
		applyCh:           make(chan LogEntry, 4096),
		applierDone:       make(chan struct{}),
		submitCh:          make(chan *submitReq, submitChBuffer),
		flusherDone:       make(chan struct{}),
		done:              make(chan struct{}),
		tickerDone:        make(chan struct{}),
		nextIndex:         make(map[string]uint64),
		matchIndex:        make(map[string]uint64),
		waiters:           make(map[uint64][]chan error),
		submitTimeout:     defaultSubmitTimeout,
		syncCh:            make(chan struct{}, 1),
		syncerDone:        make(chan struct{}),
		pipeline:          opts.Pipeline,
	}
	rn.pipelineWindow = 1
	if opts.Pipeline {
		rn.pipelineWindow = opts.PipelineWindow
		if rn.pipelineWindow <= 0 {
			rn.pipelineWindow = DefaultPipelineWindow
		}
	}
	if at, ok := transport.(AsyncTransport); ok {
		rn.async = at
	} else {
		rn.async = &serialSender{rn: rn, queues: make(map[string]chan serialReq)}
	}
	rn.LeaderID.Store("")
	rn.currentRole.Store(int32(RoleFollower))

	// Cache the batch-apply capability once so the applier hot path avoids a
	// type assertion per entry.  A state machine that does not implement
	// BatchStateMachine leaves batchSM nil and falls back to per-entry Apply.
	if bsm, ok := sm.(BatchStateMachine); ok {
		rn.batchSM = bsm
	}

	// Bootstrap configuration: self + static peers.  Overridden below by any
	// configuration found in the snapshot or the persisted log.
	boot := Configuration{Servers: append([]string{id}, peers...)}
	rn.baseConfig = boot.clone()
	rn.baseConfigIndex = 0
	rn.setConfig(boot, 0)

	// Restore the snapshot (state machine + lastIncludedIndex/Term) BEFORE
	// loading the log, then replay only the retained tail.
	if err := rn.loadSnapshot(); err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}

	// Load persisted state (or start fresh, or migrate raft_state.gob).
	if err := rn.loadState(); err != nil {
		if rn.store != nil {
			rn.store.close()
		}
		return nil, fmt.Errorf("load state: %w", err)
	}

	// Everything up to the snapshot is already applied to the state machine;
	// entries beyond it are re-applied once commitIndex advances.
	rn.commitIndex = rn.lastIncludedIndex
	rn.lastApplied = rn.lastIncludedIndex

	// Effective config = latest config entry in the log, else snapshot/bootstrap.
	rn.refreshConfigFromLog()

	rn.electionTimer = time.NewTimer(rn.randomElectionTimeout())

	// Log before starting the goroutines: once they run, reading rn.ps/rn.peers
	// requires rn.mu.
	log.Printf("[raft] node=%s started  peers=%v  term=%d  log_len=%d  snapshot_idx=%d",
		id, rn.peers, rn.ps.CurrentTerm, len(rn.ps.Log), rn.lastIncludedIndex)

	rn.mu.Lock()
	rn.repl = make(map[string]*peerRepl)
	rn.mu.Unlock()

	go rn.ticker()
	go rn.applier()
	go rn.flusher()
	go rn.syncer()

	return rn, nil
}

// ── Public API ────────────────────────────────────────────────────────────────

// Submit appends a command to the Raft log and blocks until it is committed
// and applied to the state machine.  Returns ErrNotLeader if this node is not
// the current leader.
//
// Submit does not touch the log directly: it hands the command to the
// group-commit flusher and blocks.  The flusher coalesces all concurrently
// pending Submits into a single log append; the syncer persists every queued
// batch with one fsync while the replicators ship it to the followers.  Once
// the flusher has
// assigned this command a log index, Submit waits for that index's per-index
// commit notification (signalled by the applier once the entry is quorum-
// committed and applied), exactly as before.
func (rn *RaftNode) Submit(command []byte) error {
	// Fast reject on a non-leader avoids enqueuing work the flusher would only
	// reject anyway.  The flusher re-checks leadership under rn.mu, so this is
	// purely an optimisation and cannot cause a wrong-node append.
	if RaftRole(rn.currentRole.Load()) != RoleLeader {
		return ErrNotLeader
	}

	req := &submitReq{command: command, ready: make(chan struct{})}
	select {
	case rn.submitCh <- req:
	case <-rn.done:
		return fmt.Errorf("raft node stopped")
	}

	// Wait for the flusher to append the entry and assign its index/waiter.
	select {
	case <-req.ready:
	case <-rn.done:
		return fmt.Errorf("raft node stopped")
	}
	if req.err != nil {
		return req.err
	}

	// Per-command commit semantics are preserved: block until THIS entry is
	// applied (or leadership is lost / timeout).
	return rn.waitApplied(req.idx, req.ch)
}

// flusher is the single group-commit goroutine.  It blocks for the first
// pending Submit, drains every other request already queued (up to
// maxSubmitBatch), and appends them as one batch.  A flush does no I/O (the
// syncer writes + fsyncs, the replicators send), so batches here are small;
// the fsync batching happens in the syncer, whose fsync latency is the
// batching window — no artificial delay is added to the write latency.
func (rn *RaftNode) flusher() {
	defer close(rn.flusherDone)
	for {
		select {
		case <-rn.done:
			return
		case first := <-rn.submitCh:
			batch := make([]*submitReq, 1, maxSubmitBatch)
			batch[0] = first
		drain:
			for len(batch) < maxSubmitBatch {
				select {
				case r := <-rn.submitCh:
					batch = append(batch, r)
				default:
					break drain
				}
			}
			rn.flushBatch(batch)
		}
	}
}

// flushBatch appends every command in batch to the log under ONE rn.mu
// critical section and enqueues their records for the syncer (one write(2) +
// one fsync, outside the lock, shared with any other pending appends).  With
// Options.Pipeline it also wakes the per-peer replicators at once, so the
// followers' write+fsync overlaps the leader's own; without it the syncer
// wakes them when that fsync completes (paced, pipeline.go).  Each request is
// then signalled so its Submit caller can wait for that entry's commit.
func (rn *RaftNode) flushBatch(batch []*submitReq) {
	rn.mu.Lock()
	if rn.role != RoleLeader {
		rn.mu.Unlock()
		for _, r := range batch {
			r.err = ErrNotLeader
			close(r.ready)
		}
		return
	}
	firstNew := len(rn.ps.Log)
	for _, r := range batch {
		entry, ch := rn.appendEntryLocked(EntryNormal, r.command)
		r.idx = entry.Index
		r.ch = ch
	}
	if err := rn.store.enqueue(rn.ps.Log[firstNew:]); err != nil {
		// Store failed: undo the append.  Nobody has seen these entries — the
		// replicators read the log under rn.mu, which we still hold.
		rn.ps.Log = rn.ps.Log[:firstNew]
		for _, r := range batch {
			delete(rn.waiters, r.idx)
			r.err = fmt.Errorf("persist log: %w", err)
		}
		rn.mu.Unlock()
		for _, r := range batch {
			close(r.ready)
		}
		return
	}
	rn.kickSyncerLocked()
	rn.kickReplicatorsOnAppendLocked()
	rn.mu.Unlock()

	for _, r := range batch {
		close(r.ready)
	}
}

// appendEntryLocked appends a new entry to the leader's log and registers a
// commit waiter for it.  Must be called with rn.mu held and role == RoleLeader.
func (rn *RaftNode) appendEntryLocked(typ EntryType, command []byte) (LogEntry, chan error) {
	entry := LogEntry{
		Term:    rn.ps.CurrentTerm,
		Index:   rn.lastLogIndex() + 1,
		Type:    typ,
		Command: command,
	}
	rn.ps.Log = append(rn.ps.Log, entry)

	ch := make(chan error, 1)
	rn.waiters[entry.Index] = append(rn.waiters[entry.Index], ch)
	return entry, ch
}

// markDurableLocked records that the log through idx (an entry of term) has
// been fsynced, and lets a leader count itself toward commit up to there.  The
// term check skips a stale report: the entry was truncated (this node became a
// follower and took a conflicting suffix) while the fsync ran.
// Must be called with rn.mu held.
func (rn *RaftNode) markDurableLocked(idx, term uint64) {
	if idx <= rn.durableIndex || rn.logTerm(idx) != term {
		return
	}
	rn.durableIndex = idx
	if rn.role == RoleLeader {
		rn.maybeAdvanceCommit()
	}
}

// markDurableLocked's term check is sufficient by the Log Matching property:
// if the entry at idx still has the term it had when the fsync began, every
// entry up to idx is the one that was written before that fsync.

// persistMetaLocked makes currentTerm/votedFor durable (no-op when unchanged).
// Must be called with rn.mu held, before the node acts on a new term or vote.
func (rn *RaftNode) persistMetaLocked() error {
	err := rn.store.saveMeta(raftMeta{CurrentTerm: rn.ps.CurrentTerm, VotedFor: rn.ps.VotedFor})
	if err != nil {
		log.Printf("[raft] node=%s persist term/vote failed: %v", rn.id, err)
	}
	return err
}

// rewriteLogLocked replaces the log file with the in-memory log (compaction,
// InstallSnapshot).  On success everything retained is durable.
// Must be called with rn.mu held.
func (rn *RaftNode) rewriteLogLocked() error {
	if err := rn.store.rewrite(rn.ps.Log); err != nil {
		log.Printf("[raft] node=%s rewrite log failed: %v", rn.id, err)
		// The old file still holds the retained entries, but the in-memory
		// log may now be shorter (InstallSnapshot discarded a conflicting
		// suffix): never let durableIndex cover indices that will be reused.
		if li := rn.lastLogIndex(); rn.durableIndex > li {
			rn.durableIndex = li
		}
		rn.kickSyncerLocked()
		return err
	}
	rn.durableIndex = rn.lastLogIndex()
	rn.kickSyncerLocked() // parked follower replies may now be answerable
	return nil
}

// waitApplied blocks until the entry at idx is applied (the applier signals
// ch), leadership is lost (ErrNotLeader is signalled), the node stops, or the
// submit timeout expires.
func (rn *RaftNode) waitApplied(idx uint64, ch chan error) error {
	timer := time.NewTimer(rn.submitTimeout)
	defer timer.Stop()
	select {
	case err := <-ch:
		return err
	case <-rn.done:
		rn.removeWaiter(idx, ch)
		return fmt.Errorf("raft node stopped")
	case <-timer.C:
		rn.removeWaiter(idx, ch)
		return fmt.Errorf("submit timeout: entry %d not committed within %v", idx, rn.submitTimeout)
	}
}

// removeWaiter deregisters a commit waiter (timeout / shutdown path).
func (rn *RaftNode) removeWaiter(idx uint64, ch chan error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	ws := rn.waiters[idx]
	for i, c := range ws {
		if c == ch {
			rn.waiters[idx] = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(rn.waiters[idx]) == 0 {
		delete(rn.waiters, idx)
	}
}

// failWaitersLocked signals err to every registered commit waiter and clears
// the map.  Must be called with rn.mu held.
func (rn *RaftNode) failWaitersLocked(err error) {
	for idx, ws := range rn.waiters {
		for _, ch := range ws {
			select {
			case ch <- err:
			default:
			}
		}
		delete(rn.waiters, idx)
	}
}

// IsLeader returns true if this node is the current Raft leader.
func (rn *RaftNode) IsLeader() bool {
	return RaftRole(rn.currentRole.Load()) == RoleLeader
}

// Role returns the current role as a string.
func (rn *RaftNode) Role() string {
	return RaftRole(rn.currentRole.Load()).String()
}

// GetLeaderID returns the ID of the node this server believes is the leader.
func (rn *RaftNode) GetLeaderID() string {
	v := rn.LeaderID.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

// AddPeer registers a new peer with this node so the leader will replicate
// log entries to it via AppendEntries.  Safe to call after NewRaftNode.
//
// Deprecated: AddPeer changes the local view only, without consensus.  Use
// AddServer (membership.go) to change cluster membership through the log.
func (rn *RaftNode) AddPeer(id string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.config.contains(id) {
		return
	}
	newCfg := rn.config.clone()
	newCfg.Servers = append(newCfg.Servers, id)
	// Also extend the fallback config so a log truncation does not drop the
	// locally registered peer.
	if !rn.baseConfig.contains(id) {
		rn.baseConfig.Servers = append(rn.baseConfig.Servers, id)
	}
	rn.setConfig(newCfg, rn.configIndex)
}

// Term returns the current Raft term.
func (rn *RaftNode) Term() uint64 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.ps.CurrentTerm
}

// Pipeline reports whether pipelined replication (Options.Pipeline) is on.
func (rn *RaftNode) Pipeline() bool { return rn.pipeline }

// Stop shuts down the Raft node (idempotent).  applyCh is deliberately never closed:
// notifyApplier may be sending concurrently (a close would race with the
// non-blocking send), so the applier exits via rn.done instead.
func (rn *RaftNode) Stop() { rn.stopOnce.Do(rn.stop) }

func (rn *RaftNode) stop() {
	rn.wgMu.Lock()
	rn.stopped.Store(true)
	rn.wgMu.Unlock()
	close(rn.done)
	rn.electionTimer.Stop()
	<-rn.applierDone
	<-rn.flusherDone
	<-rn.tickerDone
	// Close the transport first so in-flight RPCs fail fast, then wait for the
	// background goroutines (replicators, elections) and the syncer, whose
	// last run writes + fsyncs everything appended before Stop — after this no
	// goroutine can still be writing raft files in dataDir.  An AppendEntries
	// delivered after Stop began is refused and not acknowledged.
	_ = rn.transport.Close()
	rn.bgWG.Wait()
	<-rn.syncerDone
	rn.mu.Lock()
	decided := rn.takeDecidedWaitersLocked() // all fail: stopped
	_ = rn.store.close()
	rn.mu.Unlock()
	respondAll(decided)
}

// ── RPC handlers (called by Transport when a peer sends an RPC) ───────────────

// HandleRequestVote processes an incoming RequestVote RPC.
func (rn *RaftNode) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := RequestVoteReply{Term: rn.ps.CurrentTerm}

	// Membership safeguard (dissertation §4.2.3): refuse to vote for servers
	// outside our current configuration, so a removed server that keeps timing
	// out cannot disrupt the cluster (or even bump our term).
	if !rn.config.contains(args.CandidateID) {
		return reply
	}

	// If the request is from a stale term, reject.
	if args.Term < rn.ps.CurrentTerm {
		return reply
	}

	// A higher term always causes this server to become a follower.
	if args.Term > rn.ps.CurrentTerm {
		rn.becomeFollower(args.Term)
		if rn.persistMetaLocked() != nil {
			return reply // new term not durable: answer nothing for it
		}
	}

	// Grant vote only if:
	// 1. We haven't voted in this term (or already voted for this candidate).
	// 2. The candidate's log is at least as up-to-date as ours.
	canVote := rn.ps.VotedFor == "" || rn.ps.VotedFor == args.CandidateID
	logUpToDate := rn.logUpToDate(args.LastLogTerm, args.LastLogIndex)

	if canVote && logUpToDate {
		prev := rn.ps.VotedFor
		rn.ps.VotedFor = args.CandidateID
		// The vote must be on disk before it is granted (Raft Figure 2).
		if rn.persistMetaLocked() != nil {
			rn.ps.VotedFor = prev // disk still holds prev (atomic replace)
		} else {
			rn.resetElectionTimer()
			reply.VoteGranted = true
		}
	}

	reply.Term = rn.ps.CurrentTerm
	return reply
}

// HandleAppendEntries / HandleAppendEntriesAsync: see pipeline.go.

// ── Core Raft protocol ────────────────────────────────────────────────────────

// ticker drives election timeouts and leader heartbeats.
func (rn *RaftNode) ticker() {
	defer close(rn.tickerDone)
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-rn.done:
			return

		case <-rn.electionTimer.C:
			// Snapshot the generation BEFORE acquiring the lock.  If
			// resetElectionTimer is called while we wait (e.g. while
			// another goroutine holds rn.mu), the generation will have
			// incremented by the time we check below, and we discard the
			// stale fire instead of starting a spurious election.
			gen := rn.electionGeneration.Load()
			rn.mu.Lock()
			role := rn.role
			rn.mu.Unlock()
			if role != RoleLeader && !rn.removed.Load() && rn.electionGeneration.Load() == gen {
				rn.spawn(rn.startElection)
			}

		case <-heartbeat.C:
			rn.mu.Lock()
			if rn.role == RoleLeader {
				rn.heartbeatLocked()
			}
			rn.mu.Unlock()
		}
	}
}

// startElection transitions this server to Candidate and requests votes.
func (rn *RaftNode) startElection() {
	rn.mu.Lock()

	if rn.removed.Load() {
		rn.mu.Unlock()
		return
	}

	rn.ps.CurrentTerm++
	rn.ps.VotedFor = rn.id
	// Term + self-vote must be durable before soliciting votes.  On failure
	// stay follower in the bumped term (the timer retries the election).
	if rn.persistMetaLocked() != nil {
		rn.resetElectionTimer()
		rn.mu.Unlock()
		return
	}
	rn.role = RoleCandidate
	rn.currentRole.Store(int32(RoleCandidate))
	rn.LeaderID.Store("") // no known leader in the new term
	rn.kickSyncerLocked() // fail follower replies parked in the old term

	term := rn.ps.CurrentTerm
	lastIdx := rn.lastLogIndex()
	lastTerm := rn.lastLogTerm()
	peers := rn.peers
	needed := int64(rn.quorumSize())

	rn.resetElectionTimer()

	// A cluster whose quorum is satisfied by our own vote (single-node
	// configuration) wins immediately — there are no peers to solicit.
	if needed <= 1 {
		rn.becomeLeader()
		rn.mu.Unlock()
		return
	}
	rn.mu.Unlock()

	log.Printf("[raft] node=%s → candidate  term=%d", rn.id, term)

	votes := int64(1) // vote for self

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  rn.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply, err := rn.transport.SendRequestVote(peer, args)
			if err != nil {
				return
			}
			rn.mu.Lock()
			defer rn.mu.Unlock()
			if reply.Term > rn.ps.CurrentTerm {
				rn.becomeFollower(reply.Term)
				return
			}
			if reply.VoteGranted && rn.ps.CurrentTerm == term && rn.role == RoleCandidate {
				if atomic.AddInt64(&votes, 1) >= needed {
					rn.becomeLeader()
				}
			}
		}(peer)
	}
	wg.Wait()
}

// becomeLeader transitions this node to Leader and initialises leader state.
// Must be called with rn.mu held.
func (rn *RaftNode) becomeLeader() {
	rn.role = RoleLeader
	rn.currentRole.Store(int32(RoleLeader))
	rn.LeaderID.Store(rn.id)

	// nextIndex starts at the entry AFTER the current log tail.  We set this
	// before appending the no-op so that nextIndex[peer] points at the no-op
	// and the first broadcast includes it.
	nextIdx := rn.lastLogIndex() + 1
	for _, p := range rn.peers {
		rn.nextIndex[p] = nextIdx
		rn.matchIndex[p] = 0
	}
	log.Printf("[raft] node=%s → LEADER  term=%d  log_len=%d",
		rn.id, rn.ps.CurrentTerm, len(rn.ps.Log))

	// Raft §5.4.2: a new leader can only commit entries from its own term.
	// Append a no-op entry so prior-term entries become indirectly committed
	// once a quorum replicates this no-op.  applyCommitted skips nil commands.
	noop := LogEntry{
		Term:    rn.ps.CurrentTerm,
		Index:   rn.lastLogIndex() + 1,
		Command: nil,
	}
	rn.ps.Log = append(rn.ps.Log, noop)
	rn.termStartIndex = noop.Index
	rn.leaderReady.Store(false)

	// Enqueue the no-op (log order); the syncer fsyncs it outside the lock
	// and marks it durable (single-node configurations commit it there).
	// The replicators started here ship it to the followers right away;
	// they persist it themselves before acknowledging.
	if err := rn.store.enqueue([]LogEntry{noop}); err != nil {
		log.Printf("[raft] node=%s persist leader no-op failed: %v", rn.id, err)
	}
	rn.kickSyncerLocked()
	rn.syncReplicatorsLocked()
}

// becomeFollower reverts this node to Follower with the given term.
// Must be called with rn.mu held.
func (rn *RaftNode) becomeFollower(term uint64) {
	if term > rn.ps.CurrentTerm {
		// The leader of the new term is unknown until it contacts us; a
		// deposed leader must not keep naming itself (clients would get
		// MOVED to a node that redirects them back).
		rn.LeaderID.Store("")
	}
	rn.ps.CurrentTerm = term
	rn.ps.VotedFor = ""
	rn.role = RoleFollower
	rn.currentRole.Store(int32(RoleFollower))
	// Any Submit callers blocked on a commit will never be satisfied by us.
	rn.failWaitersLocked(ErrNotLeader)
	rn.syncReplicatorsLocked() // a deposed leader stops replicating
	rn.kickSyncerLocked()      // fail follower replies parked in the old term
	// Persist the new term.  Callers that go on to answer an RPC re-check
	// persistMetaLocked (a retry is a no-op when this succeeded).
	_ = rn.persistMetaLocked()
}

// stepDownLocked demotes a leader to follower within the same term (used when
// the leader commits its own removal from the configuration).
// Must be called with rn.mu held.
func (rn *RaftNode) stepDownLocked() {
	rn.role = RoleFollower
	rn.currentRole.Store(int32(RoleFollower))
	rn.LeaderID.Store("")
	rn.failWaitersLocked(ErrNotLeader)
	rn.syncReplicatorsLocked()
}

// maybeAdvanceCommit checks whether a new quorum exists for a higher commit
// index and, if so, advances commitIndex.  Quorum is computed over the current
// configuration; the leader counts itself only while it is still a member
// (dissertation §4.2.2 — a leader replicating its own removal must reach a
// quorum of the new configuration, which excludes it).
// Must be called with rn.mu held.
func (rn *RaftNode) maybeAdvanceCommit() {
	// Find the highest index N such that:
	//   matchIndex[quorum] >= N AND log[N].term == currentTerm
	n := rn.lastLogIndex()
	for n > rn.commitIndex {
		if rn.logTerm(n) != rn.ps.CurrentTerm {
			n--
			continue
		}
		count := 0
		for _, s := range rn.config.Servers {
			if s == rn.id {
				// The leader's log contains n, but it only counts once n is
				// fsynced (durableIndex); the replicators ship entries
				// before the leader's own fsync has finished.
				if rn.durableIndex >= n {
					count++
				}
			} else if rn.matchIndex[s] >= n {
				count++
			}
		}
		if count >= rn.quorumSize() {
			rn.commitIndex = n
			rn.notifyApplier()
			break
		}
		n--
	}
}

// applier drains applyCh and calls sm.Apply for each committed entry.
func (rn *RaftNode) applier() {
	defer close(rn.applierDone)
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-rn.done:
			// Shutdown — apply whatever is already committed, then exit.
			rn.applyCommitted()
			return
		case <-ticker.C:
			rn.applyCommitted()
			rn.maybeSnapshot()
		case <-rn.applyCh:
			rn.applyCommitted()
			rn.maybeSnapshot()
		}
	}
}

// applyCommitted applies entries in (lastApplied, commitIndex] to the state
// machine, signalling each index's commit waiter as it is applied.
//
// Consecutive write entries (EntryNormal with a non-empty command) are handed
// to the state machine's ApplyBatch in ONE call when it implements
// BatchStateMachine, so a storage-backed FSM can amortise a single fdatasync
// across the whole run instead of paying one per entry — the downstream
// group-commit that the single-entry apply loop could never engage.  Runs are
// split at any non-batchable entry (no-op nil commands, configuration changes),
// which are applied singly; nothing is ever applied out of order.
//
// The entire pass holds smMu for the duration.  Lock order: smMu is taken
// before rn.mu (matching HandleInstallSnapshot and takeSnapshot) so a snapshot
// restore can never interleave with an apply, and — because smMu is the only
// mutator of lastApplied besides this goroutine — lastApplied cannot advance
// underneath us, so a run gathered under it stays valid until applied.
func (rn *RaftNode) applyCommitted() {
	rn.smMu.Lock()
	defer rn.smMu.Unlock()

	for {
		rn.mu.Lock()
		start := rn.lastApplied + 1
		end := rn.commitIndex
		if start > end {
			rn.mu.Unlock()
			return
		}
		// Gather the contiguous committed-but-unapplied run.  Committed entries
		// are never truncated or overwritten, so these copies stay valid after
		// rn.mu is released; the Command slices are treated as immutable.
		entries := make([]LogEntry, 0, end-start+1)
		for idx := start; idx <= end; idx++ {
			e, ok := rn.entryAt(idx)
			if !ok {
				break // compacted or not yet present — apply what we gathered
			}
			entries = append(entries, e)
		}
		rn.mu.Unlock()
		if len(entries) == 0 {
			return
		}
		rn.applyEntries(entries)
	}
}

// batchable reports whether an entry is an ordinary write that ApplyBatch may
// coalesce.  No-op leader entries (nil command) and configuration changes are
// excluded — they carry side effects handled by the single-apply path.
func batchable(e LogEntry) bool {
	return e.Type == EntryNormal && len(e.Command) > 0
}

// applyEntries applies a contiguous, ordered run of committed entries, grouping
// maximal sub-runs of batchable writes into a single ApplyBatch (when the state
// machine supports it) and applying everything else singly.  Must hold smMu.
func (rn *RaftNode) applyEntries(entries []LogEntry) {
	i := 0
	for i < len(entries) {
		if rn.batchSM != nil && batchable(entries[i]) {
			j := i + 1
			for j < len(entries) && batchable(entries[j]) {
				j++
			}
			rn.applyBatchRun(entries[i:j])
			i = j
			continue
		}
		rn.applySingle(entries[i])
		i++
	}
}

// applyBatchRun applies a run of batchable write entries in one ApplyBatch call,
// then signals each entry's commit waiter with its own result.  len(run) >= 1.
func (rn *RaftNode) applyBatchRun(run []LogEntry) {
	cmds := make([][]byte, len(run))
	for i, e := range run {
		cmds[i] = e.Command
	}
	results := rn.batchSM.ApplyBatch(cmds)
	if len(run) > 1 {
		rn.batchApplyCalls.Add(1)
	}
	n := uint64(len(run))
	for cur := rn.maxApplyBatch.Load(); n > cur; cur = rn.maxApplyBatch.Load() {
		if rn.maxApplyBatch.CompareAndSwap(cur, n) {
			break
		}
	}
	for i, e := range run {
		var err error
		if i < len(results) {
			err = results[i]
		}
		if err != nil {
			log.Printf("[raft] state machine apply idx=%d error: %v", e.Index, err)
		}
		rn.finishApplied(e)
	}
}

// applySingle applies one entry through the single-entry Apply path (used for
// no-op/config entries and for state machines without batch support).
func (rn *RaftNode) applySingle(e LogEntry) {
	// Skip no-op entries (nil Command) appended by becomeLeader and config
	// entries (already handled at append time; committed via finishApplied).
	if e.Type == EntryNormal && len(e.Command) > 0 {
		if err := rn.sm.Apply(e.Command); err != nil {
			log.Printf("[raft] state machine apply idx=%d error: %v", e.Index, err)
		}
	}
	rn.finishApplied(e)
}

// finishApplied advances lastApplied to e.Index, wakes any Submit caller blocked
// on that index, and runs the config-committed hook for configuration entries.
// Called with smMu held; takes rn.mu internally.
func (rn *RaftNode) finishApplied(e LogEntry) {
	rn.mu.Lock()
	rn.lastApplied = e.Index
	if rn.role == RoleLeader && e.Index >= rn.termStartIndex {
		rn.leaderReady.Store(true)
	}
	// Commit notification: wake Submit callers blocked on this index.
	for _, ch := range rn.waiters[e.Index] {
		select {
		case ch <- nil:
		default:
		}
	}
	delete(rn.waiters, e.Index)
	if e.Type == EntryConfig {
		rn.onConfigCommitted(e)
	}
	rn.mu.Unlock()
}

func (rn *RaftNode) notifyApplier() {
	if rn.stopped.Load() {
		return
	}
	select {
	case rn.applyCh <- LogEntry{}:
	default:
	}
}

// ── Log helpers (all called with rn.mu held) ──────────────────────────────────

// logFirstIndex returns the index of the first retained log entry, or the
// index one past the snapshot when the log is empty.
func (rn *RaftNode) logFirstIndex() uint64 {
	if len(rn.ps.Log) == 0 {
		return rn.lastIncludedIndex + 1
	}
	return rn.ps.Log[0].Index
}

func (rn *RaftNode) lastLogIndex() uint64 {
	if len(rn.ps.Log) == 0 {
		return rn.lastIncludedIndex
	}
	return rn.ps.Log[len(rn.ps.Log)-1].Index
}

func (rn *RaftNode) lastLogTerm() uint64 {
	if len(rn.ps.Log) == 0 {
		return rn.lastIncludedTerm
	}
	return rn.ps.Log[len(rn.ps.Log)-1].Term
}

// logTerm returns the term of the entry at index idx, the snapshot term for
// the virtual head, or 0 if idx is 0 or out of bounds (used for prevLogTerm
// when the log is empty).
func (rn *RaftNode) logTerm(idx uint64) uint64 {
	if idx == 0 {
		return 0
	}
	if idx == rn.lastIncludedIndex {
		return rn.lastIncludedTerm
	}
	if len(rn.ps.Log) == 0 {
		return 0
	}
	// Log entries have monotonically increasing indices.
	firstIdx := rn.ps.Log[0].Index
	if idx < firstIdx || idx > rn.lastLogIndex() {
		return 0
	}
	return rn.ps.Log[idx-firstIdx].Term
}

func (rn *RaftNode) entryAt(idx uint64) (LogEntry, bool) {
	if len(rn.ps.Log) == 0 {
		return LogEntry{}, false
	}
	firstIdx := rn.ps.Log[0].Index
	if idx < firstIdx || idx > rn.lastLogIndex() {
		return LogEntry{}, false
	}
	return rn.ps.Log[idx-firstIdx], true
}

// entriesFrom returns log entries starting at idx (inclusive).
func (rn *RaftNode) entriesFrom(idx uint64) []LogEntry {
	if len(rn.ps.Log) == 0 || idx > rn.lastLogIndex() {
		return nil
	}
	firstIdx := rn.ps.Log[0].Index
	if idx < firstIdx {
		idx = firstIdx
	}
	return rn.ps.Log[idx-firstIdx:]
}

// logUpToDate returns true if (lastTerm, lastIdx) is at least as up-to-date as
// this server's log (§5.4.1: compare last entry term, then length).
func (rn *RaftNode) logUpToDate(lastTerm, lastIdx uint64) bool {
	myLastTerm := rn.lastLogTerm()
	myLastIdx := rn.lastLogIndex()
	if lastTerm != myLastTerm {
		return lastTerm > myLastTerm
	}
	return lastIdx >= myLastIdx
}

// ── Election timer ────────────────────────────────────────────────────────────

func (rn *RaftNode) randomElectionTimeout() time.Duration {
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
}

func (rn *RaftNode) resetElectionTimer() {
	// Increment the generation so that any ticker goroutine already waiting
	// on rn.mu after a stale timer fire sees a changed generation and aborts.
	rn.electionGeneration.Add(1)
	rn.electionTimer.Reset(rn.randomElectionTimeout())
}

// ── Persistence ───────────────────────────────────────────────────────────────

// loadState opens the persistent store (raft_storage.go) — migrating a legacy
// raft_state.gob if that is all there is — and installs term, vote and log.
// Runs after loadSnapshot, before any goroutine starts.
func (rn *RaftNode) loadState() error {
	store, st, err := openLogStore(rn.dataDir)
	if err != nil {
		return err
	}
	rn.store = store
	rn.ps = persistentState{
		CurrentTerm: st.meta.CurrentTerm,
		VotedFor:    st.meta.VotedFor,
		Log:         st.entries,
	}
	if rn.ps.Log == nil {
		rn.ps.Log = []LogEntry{}
	}
	if st.gapFrom > rn.lastIncludedIndex {
		return fmt.Errorf("corrupt raft log %s: entries after index %d were discarded by an index gap that the snapshot (index %d) does not cover",
			store.logPath(), st.gapFrom, rn.lastIncludedIndex)
	}

	// Discard any log prefix already covered by the snapshot (a crash between
	// the snapshot write and the log rewrite leaves overlap; the snapshot file
	// is authoritative for its range).
	needRewrite := st.gapFrom > 0
	if rn.lastIncludedIndex > 0 && len(rn.ps.Log) > 0 {
		first := rn.ps.Log[0].Index
		if rn.lastIncludedIndex >= first {
			cut := rn.lastIncludedIndex - first + 1
			if cut >= uint64(len(rn.ps.Log)) {
				rn.ps.Log = []LogEntry{}
			} else {
				rn.ps.Log = append([]LogEntry(nil), rn.ps.Log[cut:]...)
			}
			needRewrite = true
		}
	}
	if len(rn.ps.Log) > 0 && rn.ps.Log[0].Index > rn.lastIncludedIndex+1 {
		return fmt.Errorf("corrupt raft log %s: first entry %d does not follow the snapshot (index %d)",
			store.logPath(), rn.ps.Log[0].Index, rn.lastIncludedIndex)
	}

	// currentTerm is never below the term of a log entry: meta is persisted
	// before any entry of a new term is accepted.  Repair defensively if a
	// damaged/restored dataDir says otherwise.
	if lt := rn.lastLogTerm(); lt > rn.ps.CurrentTerm {
		log.Printf("[raft] node=%s persisted term %d is below last log term %d — raising it",
			rn.id, rn.ps.CurrentTerm, lt)
		rn.ps.CurrentTerm, rn.ps.VotedFor = lt, ""
		if err := rn.store.saveMeta(raftMeta{CurrentTerm: lt}); err != nil {
			return err
		}
	}

	if needRewrite {
		if err := rn.store.rewrite(rn.ps.Log); err != nil {
			return err
		}
	}
	rn.durableIndex = rn.lastLogIndex()
	return nil
}

// ── Sentinel errors ───────────────────────────────────────────────────────────

// ErrNotLeader is returned by Submit when the node is not the current leader.
var ErrNotLeader = fmt.Errorf("not the Raft leader")
