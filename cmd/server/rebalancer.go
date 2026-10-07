package main

// rebalancer.go — wires the partition TransferAgent into the serving path.
//
//   PartitionMap membership event (node added / removed)
//        │  debounce (rebalanceDebounce — coalesce a burst of events)
//        ▼
//   pm.Rebalance(pm.PartitionCount())      — recompute partition ownership
//   ta.MigrateToNewOwners()                — copy keys to replicas that newly
//                                            need them; delete a key here only
//                                            when this node left its replica
//                                            set and every replica acked it
//
// Rules (CLAUDE.md invariant 62):
//   - --mode=raft never migrates: every node holds the full raft-replicated
//     state machine, and migration writes/deletes outside the log. The agent
//     is marked SetRaftManaged (MigrateToNewOwners refuses, inbound batches
//     are rejected) and --auto-rebalance is a no-op.
//   - Node STATE changes (SUSPECT / FAILED / RECOVERING / ACTIVE) never start
//     a migration: a failed node keeps its replica slots until it is removed
//     from the membership, and comes back through RECOVERING → ACTIVE with
//     its data. Only real membership changes (added / removed) do.
//
// A failed migration is retried on the next membership event (keys whose
// replicas did not all acknowledge them are never deleted, and the agent's
// baseline membership is not advanced, so a retry re-sends).

import (
	"log"
	"time"

	"github.com/VeltrixDB/veltrixdb/cluster"
)

const rebalanceDebounce = 3 * time.Second

// setupRebalancer applies the per-mode rules to a node's transfer agent and,
// when allowed and enabled, starts the auto-rebalancer. Returns a stop
// function (a no-op when nothing was started). main calls this; tests call it
// to exercise the same wiring.
func setupRebalancer(deploy deployMode, pm *cluster.PartitionMap, ta *cluster.TransferAgent, localID string, auto bool) func() {
	if deploy == modeRaft {
		ta.SetRaftManaged(true)
		if auto {
			log.Printf("[rebalance] --auto-rebalance has no effect in --mode=raft: every node holds the full " +
				"raft-replicated state, so partition migration is disabled")
		}
		return func() {}
	}
	if !auto {
		log.Printf("[rebalance] auto-rebalance disabled (--auto-rebalance=false)")
		return func() {}
	}
	return startAutoRebalancer(pm, ta, localID)
}

// startAutoRebalancer subscribes to membership changes and runs
// rebalance+migrate after each burst.  Returns a stop function. On a
// raft-managed agent it starts nothing.
func startAutoRebalancer(pm *cluster.PartitionMap, ta *cluster.TransferAgent, localID string) func() {
	if ta.RaftManaged() {
		log.Printf("[rebalance] not started: %v", cluster.ErrMigrationRaftMode)
		return func() {}
	}
	events := pm.SubscribeMembership()
	done := make(chan struct{})

	go func() {
		var timer *time.Timer
		var fire <-chan time.Time
		for {
			select {
			case <-done:
				return
			case ev := <-events:
				// State transitions (failure, recovery, heartbeat flaps) are
				// not membership changes: the failure detector already
				// recomputes partition routing for them, and moving or
				// deleting data on a failure is what lost keys before.
				if ev.Type == "state" {
					continue
				}
				log.Printf("[rebalance] membership event node=%s type=%s — scheduling rebalance",
					ev.NodeID, ev.Type)
				if timer == nil {
					timer = time.NewTimer(rebalanceDebounce)
					fire = timer.C
				} else {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(rebalanceDebounce)
				}
			case <-fire:
				timer = nil
				fire = nil
				if err := pm.Rebalance(pm.PartitionCount()); err != nil {
					log.Printf("[rebalance] ring rebalance failed: %v", err)
					continue
				}
				start := time.Now()
				if err := ta.MigrateToNewOwners(); err != nil {
					log.Printf("[rebalance] migration incomplete (will retry on next event): %v", err)
				} else {
					log.Printf("[rebalance] rebalance + migration done in %s", time.Since(start).Round(time.Millisecond))
				}
			}
		}
	}()
	return func() { close(done) }
}
