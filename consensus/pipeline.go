package consensus

// pipeline.go — the Raft write pipeline: log persistence (syncer), follower
// acknowledgement after durability, and pipelined leader replication.
//
// # Persistence (both roles)
//
// Every log change is encoded into the logStore's write queue while rn.mu is
// held (queue order == log order == file order).  One syncer goroutine per
// node drains the queue with one write(2) + one fsync outside rn.mu and then
// advances durableIndex (markDurableLocked).  Concurrent appends — a leader's
// group-commit batches, or several pipelined AppendEntries on a follower —
// share that fsync.
//
// # Follower: acknowledge only what is durable
//
// HandleAppendEntriesAsync does the consistency check, conflict truncation
// and in-memory append under rn.mu, enqueues the records, and returns.  The
// reply is sent once the entries are durable:
//
//   - entries the follower already has durably → immediate Success;
//   - otherwise an aeWaiter is parked and answered by the syncer when
//     durableIndex covers it.  It fails instead if the term changed, the
//     entry at the target index was truncated (its term differs), the store
//     failed, or the node stopped — so a reply never claims a match for
//     entries that are no longer in the log;
//   - a heartbeat (no entries) from a leader that sends WantMatch is answered
//     at once with MatchIndex = min(prev, durableIndex): it never waits for
//     an fsync, and it never claims more than is durable.  Without WantMatch
//     (an older leader, for which Success means "durable through prev+len")
//     a heartbeat waits like an append.
//
// # Leader: per-peer replicators, two modes (Options.Pipeline)
//
// One replicator goroutine per peer (started by becomeLeader / a membership
// change, stopped on step-down).  It keeps up to its window of
// entry-carrying AppendEntries in flight, advancing nextIndex optimistically
// as it sends.  The window and the send trigger depend on the mode:
//
//   - Pipeline off (the default; server flag --raft-pipeline=false): window 1,
//     and PACED sending — appending an entry does not wake the replicators;
//     the syncer wakes them after each leader fsync, and a successful reply
//     sends again at once only when no leader fsync is pending.  So there is
//     one AppendEntries round per leader fsync, carrying every entry appended
//     so far (including ones the leader's next fsync is still writing), and
//     each follower fsync covers a leader batch or more.  This is the
//     dynamic of the pre-pipeline code (fsync, then broadcast the log tail),
//     which on three nodes sharing one disk (darwin F_FULLFSYNC ≈ 3.6 ms)
//     measured ~1,050 raft loads/s against ~600 for eager sending — sending on
//     every append and every reply splits the stream into small
//     AppendEntries and the extra follower fsyncs saturate the shared device.
//   - Pipeline on: window Options.PipelineWindow (DefaultPipelineWindow), and
//     EAGER sending — every append and every reply wakes the replicators, so
//     entries go out before the leader's own fsync and the leader's and the
//     followers' fsyncs overlap.  Faster when each node has its own disk; not
//     yet measured on separate hosts, hence off by default.
//
// Everything else is the same in both modes: followers acknowledge only
// durable entries, the leader counts itself only up to durableIndex, and the
// wire protocol (stream transport with legacy fallback, WantMatch /
// MatchIndex) is identical.  Requests go through AsyncTransport, which delivers one peer's
// requests in send order (TCPTransport: one multiplexed stream per peer).
// Replies may arrive out of order and are matched by their own args:
//
//   - Success raises matchIndex to the reply's MatchIndex (monotonic);
//   - a rejection or transport error in the current epoch resets the pipeline:
//     epoch++, nextIndex = the conflict hint (or the failed batch's first
//     index), probing = true (one request at a time until a Success);
//     rejections from older epochs are ignored;
//   - a reply carrying a higher term makes the leader step down; replies for
//     an older term are ignored.
//
// Heartbeats bypass the window.  A peer whose next entry was compacted gets
// an InstallSnapshot once its pipeline has drained, after which the pipeline
// restarts in probing mode at lastIncludedIndex+1.
//
// In both modes an AppendEntries may carry entries the leader has not fsynced
// yet (pipeline on: always; off: the ones appended while its last fsync ran).
// The leader counts itself toward a commit quorum only up to its own
// durableIndex, so a commit never rests on a copy that is not on disk.

import (
	"errors"
	"fmt"
	"log"
	"time"
)

const (
	// DefaultPipelineWindow is the number of entry-carrying AppendEntries a
	// leader keeps in flight per follower (Options.PipelineWindow overrides).
	DefaultPipelineWindow = 8

	// maxAppendEntries / maxAppendBytes bound one AppendEntries message.
	maxAppendEntries = 1024
	maxAppendBytes   = 4 << 20
)

// errNodeStopped is reported to async callbacks after Stop.
var errNodeStopped = errors.New("raft node stopped")

// AsyncTransport is an optional Transport extension used for pipelined
// replication.  SendAppendEntriesAsync hands args to the transport and
// returns; requests to one peer must be delivered in call order.  done is
// called exactly once, with the reply or an error (including a timeout), and
// never while the caller of SendAppendEntriesAsync is still inside it holding
// a lock the callback needs (the RaftNode never holds rn.mu when sending).
//
// A Transport that does not implement it is driven through a per-peer FIFO
// that calls SendAppendEntries one request at a time (no pipelining).
type AsyncTransport interface {
	Transport
	SendAppendEntriesAsync(peer string, args AppendEntriesArgs, done func(AppendEntriesReply, error))
}

// AsyncAppendHandler is implemented by *RaftNode.  RPCServer uses it so a
// stream connection keeps reading (and the follower keeps appending) while
// earlier requests wait for their fsync; respond is called exactly once.
type AsyncAppendHandler interface {
	HandleAppendEntriesAsync(args AppendEntriesArgs, respond func(AppendEntriesReply))
}

// ── background goroutines ────────────────────────────────────────────────────

// spawn runs f on a goroutine tracked by bgWG, unless Stop has begun (then it
// returns false).  Taking wgMu makes Add never race with Stop's Wait.
func (rn *RaftNode) spawn(f func()) bool {
	rn.wgMu.Lock()
	defer rn.wgMu.Unlock()
	if rn.stopped.Load() {
		return false
	}
	rn.bgWG.Add(1)
	go func() {
		defer rn.bgWG.Done()
		f()
	}()
	return true
}

// ── syncer ───────────────────────────────────────────────────────────────────

// kickSyncerLocked wakes the syncer (non-blocking; one pending wake-up is
// enough because a run covers everything queued before it starts).
func (rn *RaftNode) kickSyncerLocked() {
	select {
	case rn.syncCh <- struct{}{}:
	default:
	}
}

// syncer persists the write queue and resolves follower replies.  On Stop it
// runs once more, so everything appended before Stop reaches the disk.
func (rn *RaftNode) syncer() {
	defer close(rn.syncerDone)
	for {
		select {
		case <-rn.syncCh:
			rn.syncOnce()
		case <-rn.done:
			rn.syncOnce()
			return
		}
	}
}

// syncOnce: capture the log tail, write + fsync the queue, mark the captured
// tail durable (markDurableLocked re-checks it is still in the log), answer
// any follower reply that is now decided.
func (rn *RaftNode) syncOnce() {
	rn.mu.Lock()
	idx, term := rn.lastLogIndex(), rn.lastLogTerm()
	rn.mu.Unlock()

	err := rn.store.flush()

	rn.mu.Lock()
	if err == nil {
		rn.markDurableLocked(idx, term)
		if !rn.pipeline && rn.role == RoleLeader {
			// Paced sending (pipeline off): one round of AppendEntries per
			// leader fsync, carrying everything appended so far.
			rn.kickReplicatorsLocked()
		}
	} else if !errors.Is(err, errLogStoreClosed) {
		rn.onPersistFailureLocked(err)
	}
	ready := rn.takeDecidedWaitersLocked()
	rn.mu.Unlock()
	respondAll(ready)
}

// onPersistFailureLocked handles a failed log write/fsync (the store is now
// read-only until restart).  A follower drops the non-durable suffix from
// memory (it never acknowledged it); a leader keeps its log — its entries may
// already be on followers, and an index must never be reused for a different
// entry of the same term — but fails the Submit waiters it cannot vouch for.
// Must be called with rn.mu held.
func (rn *RaftNode) onPersistFailureLocked(err error) {
	if !rn.persistFailed {
		rn.persistFailed = true
		log.Printf("[raft] node=%s raft log persistence failed — refusing further log writes: %v", rn.id, err)
	}
	if rn.role == RoleLeader {
		perr := fmt.Errorf("persist log: %w", err)
		for idx, ws := range rn.waiters {
			if idx <= rn.durableIndex {
				continue
			}
			for _, ch := range ws {
				select {
				case ch <- perr:
				default:
				}
			}
			delete(rn.waiters, idx)
		}
		return
	}
	if rn.durableIndex < rn.lastLogIndex() && rn.durableIndex+1 >= rn.logFirstIndex() {
		rn.ps.Log = rn.ps.Log[:rn.durableIndex+1-rn.logFirstIndex()]
		rn.refreshConfigFromLog()
	}
}

// ── follower: AppendEntries with acknowledgement after durability ────────────

// aeWaiter is a successful AppendEntries whose reply waits for durability.
type aeWaiter struct {
	term       uint64 // currentTerm when accepted (the sender's term)
	target     uint64 // last index the request covers (prev + len(entries))
	targetTerm uint64 // term of the entry at target when accepted
	respond    func(AppendEntriesReply)
}

type aeDecided struct {
	respond func(AppendEntriesReply)
	reply   AppendEntriesReply
}

func respondAll(ds []aeDecided) {
	for _, d := range ds {
		d.respond(d.reply)
	}
}

// takeDecidedWaitersLocked removes and returns every parked reply that can
// now be answered.  Must be called with rn.mu held; call respond after
// releasing it.
func (rn *RaftNode) takeDecidedWaitersLocked() []aeDecided {
	if len(rn.aeWaiters) == 0 {
		return nil
	}
	failed := rn.store.failedErr() != nil || rn.stopped.Load()
	var out []aeDecided
	kept := rn.aeWaiters[:0]
	for _, w := range rn.aeWaiters {
		ok := false
		switch {
		case failed, rn.ps.CurrentTerm != w.term:
			// fail (a newer term: the old leader steps down on reply.Term)
		case w.target <= rn.lastIncludedIndex:
			// Covered by a durable snapshot taken / installed in the same term,
			// i.e. from the same leader's history.
			ok = true
		case rn.logTerm(w.target) != w.targetTerm:
			// Truncated (e.g. InstallSnapshot discarded a non-matching log):
			// never claim a match for entries no longer in the log.
		case rn.durableIndex >= w.target:
			ok = true
		default:
			kept = append(kept, w)
			continue
		}
		r := AppendEntriesReply{Term: rn.ps.CurrentTerm}
		if ok {
			r.Success, r.HasMatch, r.MatchIndex = true, true, w.target
		} else {
			r.ConflictIndex = rn.lastLogIndex() + 1
		}
		out = append(out, aeDecided{w.respond, r})
	}
	for i := len(kept); i < len(rn.aeWaiters); i++ {
		rn.aeWaiters[i] = aeWaiter{} // release respond closures
	}
	rn.aeWaiters = kept
	return out
}

// HandleAppendEntriesAsync processes an AppendEntries RPC and calls respond
// exactly once — immediately, or once the appended entries are durable (see
// the file comment).  Requests from one connection must be passed in arrival
// order; the in-memory append happens before this returns.
func (rn *RaftNode) HandleAppendEntriesAsync(args AppendEntriesArgs, respond func(AppendEntriesReply)) {
	rn.mu.Lock()
	reply, w := rn.appendEntriesLocked(args)
	if w != nil {
		w.respond = respond
		rn.aeWaiters = append(rn.aeWaiters, *w)
	}
	decided := rn.takeDecidedWaitersLocked()
	rn.mu.Unlock()
	if w == nil {
		respond(reply)
	}
	respondAll(decided)
}

// HandleAppendEntries processes an AppendEntries RPC and returns the reply
// once it is decided (a successful append only after the entries are durable).
func (rn *RaftNode) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	ch := make(chan AppendEntriesReply, 1)
	rn.HandleAppendEntriesAsync(args, func(r AppendEntriesReply) { ch <- r })
	return <-ch
}

// appendEntriesLocked is the synchronous half of AppendEntries (Raft Figure 2,
// receiver steps 1–5).  It returns either a decided reply (w == nil) or a
// waiter to park until the request's entries are durable.
// Must be called with rn.mu held.
func (rn *RaftNode) appendEntriesLocked(args AppendEntriesArgs) (AppendEntriesReply, *aeWaiter) {
	reply := AppendEntriesReply{Term: rn.ps.CurrentTerm}
	if rn.stopped.Load() {
		return reply, nil
	}
	if args.Term < rn.ps.CurrentTerm {
		return reply, nil
	}

	// Valid leader contact — become/stay follower and reset timer.
	if args.Term > rn.ps.CurrentTerm {
		rn.becomeFollower(args.Term)
		if rn.persistMetaLocked() != nil {
			return reply, nil // Success=false: the new term is not durable
		}
	}
	rn.role = RoleFollower
	rn.currentRole.Store(int32(RoleFollower))
	rn.LeaderID.Store(args.LeaderID)
	rn.resetElectionTimer()
	reply.Term = rn.ps.CurrentTerm

	prev, prevTerm, ents := args.PrevLogIndex, args.PrevLogTerm, args.Entries

	// Snapshot interaction: entries at or below lastIncludedIndex are already
	// covered by our (durable) snapshot.  Drop the covered prefix and treat
	// the snapshot as the virtual log head.
	if prev < rn.lastIncludedIndex {
		covered := rn.lastIncludedIndex - prev
		if uint64(len(ents)) <= covered {
			reply.Success, reply.HasMatch = true, true
			reply.MatchIndex = prev + uint64(len(ents))
			return reply, nil
		}
		ents = ents[covered:]
		prev, prevTerm = rn.lastIncludedIndex, rn.lastIncludedTerm
	}

	// Consistency check: verify PrevLog matches our log.
	if prev > 0 {
		if prev > rn.lastLogIndex() {
			reply.ConflictIndex = rn.lastLogIndex() + 1
			return reply, nil
		}
		if rn.logTerm(prev) != prevTerm {
			// Find first index of the conflicting term for the optimised retry.
			reply.ConflictTerm = rn.logTerm(prev)
			idx := prev
			first := rn.logFirstIndex()
			for idx > first && rn.logTerm(idx-1) == reply.ConflictTerm {
				idx--
			}
			reply.ConflictIndex = idx
			return reply, nil
		}
	}

	// Append any new entries, truncating conflicting ones.
	for i, entry := range ents {
		localIdx := prev + uint64(i) + 1
		if localIdx <= rn.lastLogIndex() && rn.logTerm(localIdx) == entry.Term {
			continue // already have this entry
		}
		if localIdx <= rn.lastLogIndex() {
			// Conflict — truncate from here.  On disk the first new record at
			// localIdx supersedes the old suffix (raft_storage.go).
			rn.ps.Log = rn.ps.Log[:localIdx-rn.logFirstIndex()]
			if rn.durableIndex >= localIdx {
				rn.durableIndex = localIdx - 1
			}
		}
		newEntries := ents[i:]
		rn.ps.Log = append(rn.ps.Log, newEntries...)
		if err := rn.store.enqueue(newEntries); err != nil {
			// Not acknowledged.  Drop the unpersisted suffix from memory;
			// everything below localIdx is unchanged (and the replaced suffix
			// conflicted with this leader, so it cannot have been committed).
			rn.ps.Log = rn.ps.Log[:localIdx-rn.logFirstIndex()]
			rn.refreshConfigFromLog()
			return reply, nil
		}
		// Config entries take effect when appended; truncation may also have
		// removed the entry our current config came from.
		rn.refreshConfigFromLog()
		rn.kickSyncerLocked()
		break
	}

	target := prev + uint64(len(ents)) // index of the last entry of this request

	// Figure 2 step 5: commitIndex = min(leaderCommit, index of last new
	// entry) — never past what this request verified, because a suffix
	// beyond target may still be a stale one from an older term.
	if args.LeaderCommit > rn.commitIndex {
		newCommit := args.LeaderCommit
		if newCommit > target {
			newCommit = target
		}
		if newCommit > rn.commitIndex {
			rn.commitIndex = newCommit
			rn.notifyApplier()
		}
	}

	reply.Success = true
	if len(ents) == 0 && args.WantMatch {
		// Heartbeat: the log through target matches the leader; report only
		// the durable part of it, and do not wait for an fsync.
		reply.HasMatch = true
		reply.MatchIndex = target
		if rn.durableIndex < target {
			reply.MatchIndex = rn.durableIndex
		}
		return reply, nil
	}
	if rn.durableIndex >= target {
		reply.HasMatch, reply.MatchIndex = true, target
		return reply, nil
	}
	return reply, &aeWaiter{term: rn.ps.CurrentTerm, target: target, targetTerm: rn.logTerm(target)}
}

// ── leader: per-peer pipelined replicators ──────────────────────────────────

// peerRepl is one follower's replication state on the leader.  Guarded by
// rn.mu, except kick/stop which are channels.
type peerRepl struct {
	peer string
	term uint64 // leader term this replicator serves

	kick chan struct{} // cap 1: something to send (entries, reply, heartbeat)
	stop chan struct{} // closed when the replicator must exit

	stopped      bool
	inflight     int    // entry-carrying AppendEntries awaiting a reply
	probing      bool   // one request at a time until a Success
	epoch        uint64 // bumped on every pipeline reset
	hbDue        bool   // the ticker asked for a heartbeat
	snapshotting bool
}

func (pr *peerRepl) wake() {
	select {
	case pr.kick <- struct{}{}:
	default:
	}
}

// syncReplicatorsLocked makes the set of replicators match the role and
// configuration: a leader runs exactly one per peer for its current term;
// any other role runs none.  Called after every role or configuration change.
// Must be called with rn.mu held.
func (rn *RaftNode) syncReplicatorsLocked() {
	if rn.repl == nil {
		return // constructor: goroutines not started yet
	}
	leader := rn.role == RoleLeader
	want := make(map[string]bool, len(rn.peers))
	if leader {
		for _, p := range rn.peers {
			want[p] = true
		}
	}
	for p, pr := range rn.repl {
		if !want[p] || pr.term != rn.ps.CurrentTerm {
			pr.stopped = true
			close(pr.stop)
			delete(rn.repl, p)
		}
	}
	if !leader {
		return
	}
	for _, p := range rn.peers {
		if rn.repl[p] != nil {
			continue
		}
		pr := &peerRepl{
			peer:    p,
			term:    rn.ps.CurrentTerm,
			kick:    make(chan struct{}, 1),
			stop:    make(chan struct{}),
			probing: true, // first request confirms where the follower's log stands
		}
		if !rn.spawn(func() { rn.runReplicator(pr) }) {
			return
		}
		rn.repl[p] = pr
		pr.wake()
	}
}

// kickReplicatorsLocked wakes every replicator (new entries to send).
func (rn *RaftNode) kickReplicatorsLocked() {
	for _, pr := range rn.repl {
		pr.wake()
	}
}

// kickReplicatorsOnAppendLocked is called after the leader appends entries.
// Pipeline on: ship them at once, before the leader's own fsync.  Pipeline
// off (paced): do nothing — the syncer kicks the replicators when the fsync
// covering them completes, so one AppendEntries round per leader fsync
// carries everything appended meanwhile.
func (rn *RaftNode) kickReplicatorsOnAppendLocked() {
	if rn.pipeline {
		rn.kickReplicatorsLocked()
	}
}

// heartbeatLocked asks every replicator for a heartbeat.
func (rn *RaftNode) heartbeatLocked() {
	for _, pr := range rn.repl {
		pr.hbDue = true
		pr.wake()
	}
}

func (rn *RaftNode) runReplicator(pr *peerRepl) {
	for {
		select {
		case <-rn.done:
			return
		case <-pr.stop:
			return
		case <-pr.kick:
		}
		rn.replicate(pr)
	}
}

type aeSend struct {
	args    AppendEntriesArgs
	entries bool   // carries entries (counts toward the window)
	epoch   uint64 // pipeline epoch at send time
}

// appendArgsLocked builds an AppendEntries for the entries after prev.
// Must be called with rn.mu held.
func (rn *RaftNode) appendArgsLocked(prev uint64, entries []LogEntry) AppendEntriesArgs {
	return AppendEntriesArgs{
		Term:         rn.ps.CurrentTerm,
		LeaderID:     rn.id,
		PrevLogIndex: prev,
		PrevLogTerm:  rn.logTerm(prev),
		Entries:      entries,
		LeaderCommit: rn.commitIndex,
		WantMatch:    true,
	}
}

// replicate sends whatever the window allows (and a heartbeat if one is due
// and nothing else went out), or an InstallSnapshot once the pipeline drained.
func (rn *RaftNode) replicate(pr *peerRepl) {
	var sends []aeSend
	snap := false

	rn.mu.Lock()
	if pr.stopped || rn.role != RoleLeader || rn.ps.CurrentTerm != pr.term {
		rn.mu.Unlock()
		return
	}
	hb := pr.hbDue
	pr.hbDue = false
	window := rn.pipelineWindow
	if pr.probing {
		window = 1
	}
	for !pr.snapshotting {
		next := rn.nextIndex[pr.peer]
		if next == 0 {
			// Peer added after the last election — start at the log tail.
			next = rn.lastLogIndex() + 1
			rn.nextIndex[pr.peer] = next
		}
		if next <= rn.lastIncludedIndex {
			// The entries this peer needs were compacted: ship the snapshot,
			// but only once every in-flight request has been answered.
			if pr.inflight == 0 {
				pr.snapshotting, snap = true, true
			}
			break
		}
		if next > rn.lastLogIndex() || pr.inflight >= window {
			break
		}
		ents := rn.entriesFrom(next)
		n, bytes := 0, 0
		for n < len(ents) && n < maxAppendEntries && (n == 0 || bytes+len(ents[n].Command) <= maxAppendBytes) {
			bytes += len(ents[n].Command)
			n++
		}
		// Copy so a later truncation or compaction cannot race the encoder.
		batch := append([]LogEntry(nil), ents[:n]...)
		sends = append(sends, aeSend{args: rn.appendArgsLocked(next-1, batch), entries: true, epoch: pr.epoch})
		rn.nextIndex[pr.peer] = next + uint64(n)
		pr.inflight++
	}
	if hb && len(sends) == 0 && !snap && !pr.snapshotting {
		if next := rn.nextIndex[pr.peer]; next > rn.lastIncludedIndex {
			sends = append(sends, aeSend{args: rn.appendArgsLocked(next-1, nil), epoch: pr.epoch})
		}
	}
	rn.mu.Unlock()

	if snap {
		rn.sendSnapshot(pr.peer, pr.term)
		rn.mu.Lock()
		pr.snapshotting = false
		// InstallSnapshot resets the pipeline: restart (probing) after it.
		pr.epoch++
		pr.probing = true
		rn.mu.Unlock()
		pr.wake()
		return
	}
	for _, s := range sends {
		s := s
		rn.async.SendAppendEntriesAsync(pr.peer, s.args, func(reply AppendEntriesReply, err error) {
			rn.onAppendReply(pr, s, reply, err)
		})
	}
}

// onAppendReply processes one AppendEntries reply (or transport error).
func (rn *RaftNode) onAppendReply(pr *peerRepl, s aeSend, reply AppendEntriesReply, err error) {
	rn.mu.Lock()
	// Pipeline on: look for more to send after every reply.  Paced (off): a
	// successful reply re-sends only when no leader fsync is pending — else
	// that fsync's completion kicks the replicators (syncOnce), and the next
	// AppendEntries carries everything appended meanwhile.
	if rn.pipeline || err != nil || !reply.Success || rn.durableIndex >= rn.lastLogIndex() {
		defer pr.wake()
	}
	defer rn.mu.Unlock()

	if s.entries {
		pr.inflight--
	}
	if err != nil {
		// Lost request or reply: what reached the follower is unknown, so
		// resend from this batch (only once per epoch).
		if s.entries && !pr.stopped && s.epoch == pr.epoch {
			pr.epoch++
			pr.probing = true
			if first := s.args.PrevLogIndex + 1; first < rn.nextIndex[pr.peer] {
				rn.nextIndex[pr.peer] = first
			}
		}
		return
	}
	if reply.Term > rn.ps.CurrentTerm {
		rn.becomeFollower(reply.Term)
		return
	}
	if pr.stopped || rn.role != RoleLeader || rn.ps.CurrentTerm != s.args.Term {
		return // stale reply (older term or replaced replicator)
	}
	if reply.Success {
		match := s.args.PrevLogIndex + uint64(len(s.args.Entries)) // pre-WantMatch follower
		if reply.HasMatch {
			match = reply.MatchIndex
		}
		if match > rn.matchIndex[pr.peer] {
			rn.matchIndex[pr.peer] = match
			rn.maybeAdvanceCommit()
		}
		if rn.nextIndex[pr.peer] <= match {
			rn.nextIndex[pr.peer] = match + 1
		}
		if s.entries && s.epoch == pr.epoch {
			pr.probing = false // confirmed: pipeline from here
		}
		return
	}
	if s.epoch != pr.epoch {
		return // rejection of a request sent before the last reset
	}
	// Rejected: drain the pipeline and back off using the conflict hint.
	pr.epoch++
	pr.probing = true
	next := s.args.PrevLogIndex // step back one from the rejected prev
	if reply.ConflictIndex > 0 {
		next = reply.ConflictIndex
	}
	if next < 1 {
		next = 1
	}
	rn.nextIndex[pr.peer] = next
}

// ── fallback for transports without AsyncTransport ──────────────────────────

// serialSender adapts a synchronous Transport: one FIFO + goroutine per peer
// calls SendAppendEntries one request at a time, in order.
type serialSender struct {
	rn     *RaftNode
	queues map[string]chan serialReq // guarded by rn.wgMu
}

type serialReq struct {
	args AppendEntriesArgs
	done func(AppendEntriesReply, error)
}

func (s *serialSender) SendAppendEntriesAsync(peer string, args AppendEntriesArgs, done func(AppendEntriesReply, error)) {
	rn := s.rn
	rn.wgMu.Lock()
	q, ok := s.queues[peer]
	if !ok && !rn.stopped.Load() {
		q = make(chan serialReq, 4*DefaultPipelineWindow)
		s.queues[peer] = q
		rn.bgWG.Add(1)
		go func() {
			defer rn.bgWG.Done()
			for {
				select {
				case <-rn.done:
					return
				case r := <-q:
					reply, err := rn.transport.SendAppendEntries(peer, r.args)
					r.done(reply, err)
				}
			}
		}()
	}
	rn.wgMu.Unlock()
	if q == nil {
		done(AppendEntriesReply{}, errNodeStopped)
		return
	}
	select {
	case q <- serialReq{args, done}:
	case <-rn.done:
		done(AppendEntriesReply{}, errNodeStopped)
	case <-time.After(rpcCallTimeout):
		done(AppendEntriesReply{}, fmt.Errorf("send queue to %s full", peer))
	}
}

func (s *serialSender) SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	return s.rn.transport.SendRequestVote(peer, args)
}
func (s *serialSender) SendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	return s.rn.transport.SendAppendEntries(peer, args)
}
func (s *serialSender) SendInstallSnapshot(peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	return s.rn.transport.SendInstallSnapshot(peer, args)
}
func (s *serialSender) Close() error { return nil }
