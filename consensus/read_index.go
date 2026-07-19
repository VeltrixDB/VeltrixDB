package consensus

// read_index.go — linearizable reads via the Raft ReadIndex protocol (§6.4 of
// the Raft dissertation).
//
// A local read on the leader can still be stale: a partitioned ex-leader may
// serve reads after a new leader has committed writes elsewhere.  ReadIndex
// closes that hole without writing to the log:
//
//   1. Capture commitIndex as the read fence.  The leader must have committed
//      at least one entry in its current term first (the no-op appended on
//      election win guarantees this shortly after any election).
//   2. Confirm leadership with a fresh quorum heartbeat round — proof that no
//      higher-term leader existed at the moment of the round.
//   3. Wait until the state machine has applied up to the fence, then read.
//
// The caller (cmd/server coordinator, --linearizable-reads) performs the
// actual storage read after ReadIndex returns.

import (
	"errors"
	"sync/atomic"
	"time"
)

// ErrReadIndexTimeout is returned when quorum confirmation or apply catch-up
// does not finish within the caller's deadline.
var ErrReadIndexTimeout = errors.New("raft: read-index timed out")

// ErrLeadershipUnconfirmed is returned when the current term has no committed
// entry yet (immediately after election) — retry shortly.
var ErrLeadershipUnconfirmed = errors.New("raft: leadership not yet established in this term (retry)")

// ReadIndex runs the linearizable-read fence and returns the index a read must
// wait for.  It blocks until the fence is applied locally or timeout elapses.
// Returns ErrNotLeader on a non-leader.
func (rn *RaftNode) ReadIndex(timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)

	rn.mu.Lock()
	if rn.role != RoleLeader {
		rn.mu.Unlock()
		return 0, ErrNotLeader
	}
	term := rn.ps.CurrentTerm
	readIdx := rn.commitIndex
	// §6.4: the fence is only valid once the leader has committed an entry in
	// ITS OWN term (otherwise commitIndex may predate a newer leader's writes).
	if readIdx == 0 || rn.logTerm(readIdx) != term {
		rn.mu.Unlock()
		return 0, ErrLeadershipUnconfirmed
	}
	peers := append([]string(nil), rn.peers...)
	quorum := rn.quorumSize()
	commit := rn.commitIndex
	lastIdx := rn.lastLogIndex()
	prevTerm := rn.logTerm(lastIdx)
	rn.mu.Unlock()

	// Single-node cluster: the leader IS the quorum.
	if quorum <= 1 {
		return readIdx, rn.waitAppliedUntil(readIdx, deadline)
	}

	// Quorum heartbeat round.  Any reply whose term does not exceed ours
	// acknowledges our leadership for this round (Success is irrelevant — a
	// log-lagging follower still recognises the leader).
	acks := int64(1) // self
	acked := make(chan struct{}, len(peers))
	for _, peer := range peers {
		rn.bgWG.Add(1)
		go func(peer string) {
			defer rn.bgWG.Done()
			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     rn.id,
				PrevLogIndex: lastIdx,
				PrevLogTerm:  prevTerm,
				LeaderCommit: commit,
			}
			reply, err := rn.transport.SendAppendEntries(peer, args)
			if err != nil {
				return
			}
			if reply.Term > term {
				rn.mu.Lock()
				if reply.Term > rn.ps.CurrentTerm {
					rn.becomeFollower(reply.Term)
				}
				rn.mu.Unlock()
				return
			}
			atomic.AddInt64(&acks, 1)
			acked <- struct{}{}
		}(peer)
	}

	for atomic.LoadInt64(&acks) < int64(quorum) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0, ErrReadIndexTimeout
		}
		select {
		case <-acked:
		case <-time.After(remaining):
			return 0, ErrReadIndexTimeout
		case <-rn.done:
			return 0, ErrNotLeader
		}
	}

	// Confirm we are still the leader of the same term after the round.
	rn.mu.Lock()
	stillLeader := rn.role == RoleLeader && rn.ps.CurrentTerm == term
	rn.mu.Unlock()
	if !stillLeader {
		return 0, ErrNotLeader
	}

	return readIdx, rn.waitAppliedUntil(readIdx, deadline)
}

// waitAppliedUntil blocks until lastApplied >= idx or the deadline passes.
func (rn *RaftNode) waitAppliedUntil(idx uint64, deadline time.Time) error {
	for {
		rn.mu.Lock()
		applied := rn.lastApplied
		rn.mu.Unlock()
		if applied >= idx {
			return nil
		}
		if time.Now().After(deadline) {
			return ErrReadIndexTimeout
		}
		time.Sleep(500 * time.Microsecond)
	}
}
