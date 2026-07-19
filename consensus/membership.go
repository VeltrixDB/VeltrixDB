package consensus

// membership.go — single-server cluster membership changes (Raft dissertation §4).
//
// Membership changes go through the replicated log as EntryConfig entries whose
// Command is a gob-encoded Configuration.  Per the dissertation, a
// configuration takes effect on a server as soon as the entry is APPENDED to
// its log — not when it commits.  Because only one server is added or removed
// at a time, any majority of the old configuration overlaps any majority of
// the new one, so two disjoint quorums can never form.
//
// Concurrency control: a leader refuses to start a configuration change while
// a previous one is still uncommitted (configIndex > commitIndex ⇒
// ErrConfigChangeInProgress).
//
// Catch-up choice (documented per task spec): there is NO separate
// learner/non-voting phase.  A newly added server counts toward quorum from
// the moment the config entry is appended.  The leader starts replicating to
// it on the next heartbeat tick; if the entries it needs were already
// compacted, it is caught up with a single InstallSnapshot RPC (snapshot.go).
// Single-server changes keep old and new majorities overlapping, so a
// lagging new server can only hurt availability (commit latency), never
// safety.  AddServer blocks until the config entry commits under the NEW
// quorum, so by the time it returns the cluster has proven it can reach
// majority including the change.
//
// A leader that commits its own removal steps down (stepDownLocked); a
// removed server never starts elections (rn.removed) and other servers refuse
// to vote for candidates outside their configuration (dissertation §4.2.3),
// so a removed server cannot disrupt the cluster.

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
)

// Configuration is the set of voting servers in the cluster.
type Configuration struct {
	Servers []string
}

func (c Configuration) clone() Configuration {
	return Configuration{Servers: append([]string(nil), c.Servers...)}
}

func (c Configuration) contains(id string) bool {
	for _, s := range c.Servers {
		if s == id {
			return true
		}
	}
	return false
}

func encodeConfiguration(c Configuration) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeConfiguration(b []byte) (Configuration, error) {
	var c Configuration
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&c)
	return c, err
}

// ErrConfigChangeInProgress is returned by AddServer/RemoveServer while a
// previous configuration change has not yet committed.
var ErrConfigChangeInProgress = fmt.Errorf("a configuration change is already in progress")

// setConfig installs cfg as the node's effective configuration and derives
// the peer list (all servers except self).  A node absent from its own
// configuration is marked removed and will not start elections.
// Must be called with rn.mu held (or before the node's goroutines start).
func (rn *RaftNode) setConfig(cfg Configuration, index uint64) {
	rn.config = cfg
	rn.configIndex = index
	peers := make([]string, 0, len(cfg.Servers))
	for _, s := range cfg.Servers {
		if s != rn.id {
			peers = append(peers, s)
		}
	}
	rn.peers = peers
	rn.removed.Store(!cfg.contains(rn.id))
}

// refreshConfigFromLog re-derives the effective configuration: the latest
// EntryConfig entry in the retained log, else the snapshot/bootstrap config.
// Called after any operation that appends or truncates log entries.
// Must be called with rn.mu held (or before the node's goroutines start).
func (rn *RaftNode) refreshConfigFromLog() {
	for i := len(rn.ps.Log) - 1; i >= 0; i-- {
		e := rn.ps.Log[i]
		if e.Type != EntryConfig {
			continue
		}
		cfg, err := decodeConfiguration(e.Command)
		if err != nil {
			log.Printf("[raft] node=%s corrupt config entry idx=%d: %v", rn.id, e.Index, err)
			continue
		}
		rn.setConfig(cfg, e.Index)
		return
	}
	rn.setConfig(rn.baseConfig.clone(), rn.baseConfigIndex)
}

// configAt returns the configuration in effect at log index idx (the latest
// config entry with Index <= idx, else the snapshot/bootstrap config).  Used
// when capturing a snapshot at idx.  Must be called with rn.mu held.
func (rn *RaftNode) configAt(idx uint64) (Configuration, uint64) {
	best, bestIdx := rn.baseConfig, rn.baseConfigIndex
	for i := range rn.ps.Log {
		e := rn.ps.Log[i]
		if e.Index > idx {
			break
		}
		if e.Type != EntryConfig {
			continue
		}
		if cfg, err := decodeConfiguration(e.Command); err == nil {
			best, bestIdx = cfg, e.Index
		}
	}
	return best.clone(), bestIdx
}

// quorumSize is the majority of the current configuration.
// Must be called with rn.mu held.
func (rn *RaftNode) quorumSize() int {
	return len(rn.config.Servers)/2 + 1
}

// AddServer adds a server to the cluster configuration through the log.
// It blocks until the configuration entry commits (under the new quorum) or
// the submit timeout expires.  Returns ErrNotLeader on non-leaders, nil if the
// server is already a member, and ErrConfigChangeInProgress while a previous
// change is uncommitted.
func (rn *RaftNode) AddServer(id string) error {
	return rn.changeConfig(func(cur Configuration) (Configuration, bool) {
		if cur.contains(id) {
			return cur, false
		}
		next := cur.clone()
		next.Servers = append(next.Servers, id)
		return next, true
	})
}

// RemoveServer removes a server from the cluster configuration through the
// log.  A leader may remove itself: it keeps leading until the removal entry
// commits under the new configuration's quorum, then steps down.
func (rn *RaftNode) RemoveServer(id string) error {
	return rn.changeConfig(func(cur Configuration) (Configuration, bool) {
		if !cur.contains(id) {
			return cur, false
		}
		next := Configuration{Servers: make([]string, 0, len(cur.Servers)-1)}
		for _, s := range cur.Servers {
			if s != id {
				next.Servers = append(next.Servers, s)
			}
		}
		return next, true
	})
}

// changeConfig appends one EntryConfig entry produced by mutate and waits for
// it to commit.  Mirrors Submit's persist-outside-the-lock discipline.
func (rn *RaftNode) changeConfig(mutate func(Configuration) (Configuration, bool)) error {
	rn.mu.Lock()
	if rn.role != RoleLeader {
		rn.mu.Unlock()
		return ErrNotLeader
	}
	// Reject concurrent membership changes: the previous config entry must
	// have committed before another change may start.
	if rn.configIndex > rn.commitIndex {
		rn.mu.Unlock()
		return ErrConfigChangeInProgress
	}
	newCfg, changed := mutate(rn.config)
	if !changed {
		rn.mu.Unlock()
		return nil
	}
	cmd, err := encodeConfiguration(newCfg)
	if err != nil {
		rn.mu.Unlock()
		return fmt.Errorf("encode configuration: %w", err)
	}
	entry, ch, psCopy := rn.appendLocked(EntryConfig, cmd)
	// The new configuration takes effect NOW (append time, dissertation §4.1).
	rn.setConfig(newCfg, entry.Index)
	log.Printf("[raft] node=%s config change appended idx=%d servers=%v",
		rn.id, entry.Index, newCfg.Servers)
	rn.maybeAdvanceCommit()
	rn.mu.Unlock()

	if err := rn.writeStateFile(psCopy); err != nil {
		rn.removeWaiter(entry.Index, ch)
		return fmt.Errorf("persist log: %w", err)
	}

	go rn.broadcastAppendEntries()

	return rn.waitApplied(entry.Index, ch)
}

// onConfigCommitted runs when an EntryConfig entry is applied (committed).
// If the node's CURRENT configuration no longer includes it, the node marks
// itself removed; a removed leader steps down (dissertation §4.2.2 — it kept
// leading only long enough to commit its own removal).
// Must be called with rn.mu held.
func (rn *RaftNode) onConfigCommitted(e LogEntry) {
	// Decide from the effective config, not e's payload: a later (appended)
	// config entry may already have superseded e.
	if rn.config.contains(rn.id) {
		return
	}
	rn.removed.Store(true)
	if rn.role == RoleLeader {
		log.Printf("[raft] node=%s committed its own removal (idx=%d) — stepping down",
			rn.id, e.Index)
		rn.stepDownLocked()
	}
}
