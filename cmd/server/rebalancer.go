package main

// rebalancer.go — wires the partition TransferAgent into the serving path.
//
// Before this file existed, cluster/partition_transfer.go was fully
// implemented but never instantiated by the server: node join/leave updated
// the ring (routing) while the data stayed put.  startAutoRebalancer closes
// that gap:
//
//   PartitionMap membership event (add / remove / state change)
//        │  debounce (rebalanceDebounce — coalesce a burst of gossip events)
//        ▼
//   pm.Rebalance(pm.PartitionCount())      — recompute partition ownership
//   ta.MigrateToNewOwners()                — stream re-owned keys, delete local
//
// Migration is retried on the next event if it fails (MigrateToNewOwners only
// deletes keys whose receipt the destination confirmed, so a retry re-sends
// the remainder).

import (
	"log"
	"time"

	"github.com/VeltrixDB/veltrixdb/cluster"
)

const rebalanceDebounce = 3 * time.Second

// startAutoRebalancer subscribes to membership changes and runs
// rebalance+migrate after each burst.  Returns a stop function.
func startAutoRebalancer(pm *cluster.PartitionMap, ta *cluster.TransferAgent, localID string) func() {
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
				if ev.NodeID == localID && ev.Type == "state" {
					continue // local heartbeat state flaps don't move data
				}
				// Only topology-relevant transitions reset the debounce window:
				// joins, removals, hard failures, and recoveries.
				if ev.Type == "state" && ev.NewState != cluster.NodeStateFailed &&
					ev.NewState != cluster.NodeStateActive && ev.NewState != cluster.NodeStateRecovering {
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
