package replication

// membership.go — replica set changes driven by online cluster membership
// (cmd/server/membership_replicated.go).

import (
	"fmt"
	"log"
	"time"
)

// AddMemberReplica registers a replica that joined the cluster through a
// membership change.  Unlike AddReplica (a replica this node already knew),
// a new member starts FAILED with a catch-up of this node's WHOLE change
// feed (every key this node originated — current value or tombstone), so it
// receives no live batch until it holds everything this node wrote; writes
// made meanwhile are retained and replayed after the catch-up, in order
// (invariant 61).  The new member's watermark is persisted at once, so a
// restart before the catch-up finished resumes it.  Keys this node holds as
// another member's writes are that member's to send; copies of keys whose
// originator left are delivered by the partition migration (put-if-absent).
func (re *ReplicationEngine) AddMemberReplica(nodeID, address string, port int) error {
	if err := re.addReplica(nodeID, address, port, 0); err != nil {
		return err
	}
	re.mu.Lock()
	if r := re.replicaStates[nodeID]; r != nil {
		r.State = ReplicaStateFailed
		r.needsResync = true
		r.resyncFromNs = 1 // the whole change feed
		r.freshMember = true
		r.backoff = re.probeInterval()
		r.nextProbeAt = 0
	}
	re.mu.Unlock()
	now := time.Now()
	re.mu.RLock()
	marks, gen := re.watermarksLocked(now.UnixNano())
	obs := re.watermark
	re.mu.RUnlock()
	re.persistStateIfChanged(marks, gen)
	if obs != nil {
		obs(nodeID, (marks[nodeID]-int64(watermarkMargin))/1000)
	}
	log.Printf("[repl] member %s added: initial copy of this node's change feed before it gets live batches", nodeID)
	return nil
}

// RemoveMemberReplica unregisters a replica that left the cluster and writes
// the recovery state without it at once (instead of at the next lag-monitor
// tick), so a restart never resurrects the departed replica's watermark.
func (re *ReplicationEngine) RemoveMemberReplica(nodeID string) error {
	re.mu.RLock()
	_, ok := re.replicaStates[nodeID]
	re.mu.RUnlock()
	if !ok {
		return fmt.Errorf("replica %s not registered", nodeID)
	}
	if err := re.RemoveReplica(nodeID); err != nil {
		return err
	}
	re.mu.RLock()
	marks, gen := re.watermarksLocked(time.Now().UnixNano())
	re.mu.RUnlock()
	re.persistStateIfChanged(marks, gen)
	log.Printf("[repl] member %s removed from the replica set", nodeID)
	return nil
}

// HasReplica reports whether nodeID is a registered replica.
func (re *ReplicationEngine) HasReplica(nodeID string) bool {
	re.mu.RLock()
	defer re.mu.RUnlock()
	_, ok := re.replicaStates[nodeID]
	return ok
}

// Drained reports whether every write this node originated has been
// acknowledged by every registered replica: nothing is retained, every
// replica is SYNC and none needs (or is running) a catch-up.  A node is
// removed from a replicated cluster only once this holds (with writes
// refused meanwhile), so its departure loses nothing.  why describes the
// first obstacle when it does not hold.
func (re *ReplicationEngine) Drained() (ok bool, why string) {
	re.mu.RLock()
	defer re.mu.RUnlock()
	if n := len(re.pendingWrites); n > 0 {
		return false, fmt.Sprintf("%d write(s) not yet acknowledged by every replica", n)
	}
	for id, r := range re.replicaStates {
		switch {
		case r.State != ReplicaStateSync:
			return false, fmt.Sprintf("replica %s is %s", id, r.State)
		case r.needsResync || r.catchingUpFromNs != 0:
			return false, fmt.Sprintf("replica %s needs a catch-up", id)
		}
	}
	return true, ""
}
