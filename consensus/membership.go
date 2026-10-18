package consensus

// membership.go — online cluster membership (Raft dissertation §4).
//
// Membership changes go through the replicated log as EntryConfig entries whose
// Command is a gob-encoded Configuration.  Per the dissertation, a
// configuration takes effect on a server as soon as the entry is APPENDED to
// its log — not when it commits.  Only one server's role changes per entry, so
// any majority of the old voter set overlaps any majority of the new one and
// two disjoint quorums can never form.  The configuration lives in the log and,
// after compaction, in the snapshot header (snapshot_file.go), so it survives
// restarts and wins over the bootstrap peer list.
//
// Adding a server (AddMember) is two single-server changes:
//
//  1. the server is appended as a LEARNER: it is replicated to (entries and,
//     when the leader compacted past what it has, a chunked InstallSnapshot)
//     but it does not vote, is not counted toward any quorum and never starts
//     an election — a slow or empty newcomer cannot stall commits;
//  2. once it is caught up, the leader promotes it to a VOTER.  "Caught up" is
//     the dissertation's round rule (§4.2.1): a round ends when the learner's
//     matchIndex reaches the leader's last index as of the round's start; a
//     round that ends within promoteRoundMax proves the remaining lag is
//     below one round, so the promotion cannot stall commits for long.  Any
//     leader promotes (learners are part of the persisted configuration), so
//     a leader change during catch-up does not strand the newcomer.
//
// Concurrency control: a leader refuses to start a configuration change while
// a previous one is still uncommitted (configIndex > commitIndex ⇒
// ErrConfigChangeInProgress), so changes — including promotions — serialise.
// A change whose new voter set has no quorum the leader heard from within
// quorumContactWindow is refused up front (ErrNoQuorum) instead of being
// appended and left uncommittable.
//
// Removal: RemoveMember appends the configuration without the server.  A
// leader may remove itself: it keeps leading (without counting itself) until
// the removal commits under the new configuration's quorum, then steps down
// (onConfigCommitted).
//
// Join safety: a node started with Options.Join has an EMPTY bootstrap
// configuration — it is not a voter in its own view, so it never campaigns
// (it could otherwise elect itself with an empty log) and votes for nobody
// until a leader replicates a configuration that includes it.
//
// Disruption: elections start with a pre-vote round (raft.go preVote); a
// server only grants a pre-vote to a voter of its own configuration whose log
// is up to date and only when it has not heard from a leader within
// electionTimeoutMin.  A removed (or not yet promoted) server therefore never
// increments its term, and members never adopt its term either: real
// RequestVotes from non-voters are refused before the term check
// (HandleRequestVote, dissertation §4.2.3).

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"
)

// Configuration is the cluster membership.
//
// Servers are the voting members.  Learners are non-voting members: they
// receive the log but are not counted toward quorums and never campaign.
// Addrs maps a member ID to an application address (cmd/server stores the
// member's client host:port; raft itself never reads it) so every node learns
// how to reach new members from the log.  The zero value of Learners / Addrs
// keeps configurations written by older builds readable.
type Configuration struct {
	Servers  []string
	Learners []string
	Addrs    map[string]string
}

func (c Configuration) clone() Configuration {
	out := Configuration{
		Servers:  append([]string(nil), c.Servers...),
		Learners: append([]string(nil), c.Learners...),
	}
	if c.Addrs != nil {
		out.Addrs = make(map[string]string, len(c.Addrs))
		for k, v := range c.Addrs {
			out.Addrs[k] = v
		}
	}
	return out
}

// Clone returns a deep copy.
func (c Configuration) Clone() Configuration { return c.clone() }

// contains reports whether id is a VOTER.
func (c Configuration) contains(id string) bool {
	for _, s := range c.Servers {
		if s == id {
			return true
		}
	}
	return false
}

func (c Configuration) isLearner(id string) bool {
	for _, s := range c.Learners {
		if s == id {
			return true
		}
	}
	return false
}

// IsVoter reports whether id is a voting member.
func (c Configuration) IsVoter(id string) bool { return c.contains(id) }

// IsLearner reports whether id is a non-voting member.
func (c Configuration) IsLearner(id string) bool { return c.isLearner(id) }

// IsMember reports whether id is a voter or a learner.
func (c Configuration) IsMember(id string) bool { return c.contains(id) || c.isLearner(id) }

// Members returns every member (voters, then learners).
func (c Configuration) Members() []string {
	return append(append([]string(nil), c.Servers...), c.Learners...)
}

// equal reports whether two configurations have the same members, roles and
// addresses (member order is significant only within each list's set).
func (c Configuration) equal(o Configuration) bool {
	if !sameSet(c.Servers, o.Servers) || !sameSet(c.Learners, o.Learners) || len(c.Addrs) != len(o.Addrs) {
		return false
	}
	for k, v := range c.Addrs {
		if w, ok := o.Addrs[k]; !ok || w != v {
			return false
		}
	}
	return true
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]string(nil), a...)
	y := append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func removeID(ids []string, id string) []string {
	out := make([]string, 0, len(ids))
	for _, s := range ids {
		if s != id {
			out = append(out, s)
		}
	}
	return out
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

// maxConfigBytes bounds an encoded configuration so it always fits the
// snapshot header (snapHeaderCRCOff - snapHeaderFixed bytes, minus slack).
const maxConfigBytes = snapHeaderCRCOff - snapHeaderFixed - 64

var (
	// ErrConfigChangeInProgress is returned while a previous configuration
	// change (including a learner promotion) has not committed yet.
	ErrConfigChangeInProgress = fmt.Errorf("a configuration change is already in progress")
	// ErrMemberExists: AddMember of an ID that is already a voter, or a
	// learner registered with a different address.
	ErrMemberExists = errors.New("raft: member already exists")
	// ErrMemberNotFound: RemoveMember of an ID that is not a member.
	ErrMemberNotFound = errors.New("raft: no such member")
	// ErrNoQuorum: the leader has not heard from a quorum of the voters the
	// change needs (current voters for an add, the new voter set for a
	// removal or promotion) within quorumContactWindow.
	ErrNoQuorum = errors.New("raft: no reachable quorum for this configuration change")
	// ErrLastVoter: removing the only voter would leave no cluster.
	ErrLastVoter = errors.New("raft: cannot remove the last voter")
	// ErrPromotionPending: the learner was added (committed) but has not
	// caught up and been promoted to voter within the wait; the leader keeps
	// trying, so this is progress, not failure.
	ErrPromotionPending = errors.New("raft: added as learner; promotion to voter pending (catching up)")
	// ErrConfigTooLarge: the configuration would not fit the snapshot header.
	ErrConfigTooLarge = errors.New("raft: configuration too large")
)

// Tunables (vars so tests can shorten them).
var (
	// quorumContactWindow: a voter counts as reachable for the ErrNoQuorum
	// pre-check if the leader got a reply from it within this window.
	quorumContactWindow = 2 * electionTimeoutMax
	// promoteRoundMax: a catch-up round that ends within this proves the
	// learner's lag is small enough to promote it.
	promoteRoundMax = time.Second
	// leaderContactGrace: right after an election the leader has replies from
	// nobody yet; for this long the pre-check waits for them.
	leaderContactGrace = 2 * electionTimeoutMax
)

// setConfig installs cfg as the node's effective configuration and derives
// the replication targets (every member except self) and the voters to
// solicit in elections.  A node absent from its own configuration is marked
// removed; a node that is not a voter never starts an election.
// Must be called with rn.mu held (or before the node's goroutines start).
func (rn *RaftNode) setConfig(cfg Configuration, index uint64) {
	changed := index != rn.configIndex || !cfg.equal(rn.config) || !rn.configNotified
	rn.config = cfg
	rn.configIndex = index
	peers := make([]string, 0, len(cfg.Servers)+len(cfg.Learners))
	voters := make([]string, 0, len(cfg.Servers))
	for _, s := range cfg.Servers {
		if s != rn.id {
			peers = append(peers, s)
			voters = append(voters, s)
		}
	}
	for _, s := range cfg.Learners {
		if s != rn.id {
			peers = append(peers, s)
		}
	}
	rn.peers = peers
	rn.voterPeers = voters
	rn.removed.Store(!cfg.IsMember(rn.id))
	rn.voter.Store(cfg.contains(rn.id))
	// The application learns new member addresses before a replicator for
	// them starts sending.
	if changed && rn.onConfig != nil {
		rn.configNotified = true
		rn.onConfig(cfg.clone())
	}
	// A leader starts / stops per-peer replicators for added / removed peers.
	rn.syncReplicatorsLocked()
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

// quorumSize is the majority of the current voter set.
// Must be called with rn.mu held.
func (rn *RaftNode) quorumSize() int {
	return len(rn.config.Servers)/2 + 1
}

// Configuration returns the node's effective configuration, the log index it
// came from (0 = bootstrap) and whether that entry is committed.
func (rn *RaftNode) Configuration() (Configuration, uint64, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.config.clone(), rn.configIndex, rn.configIndex <= rn.commitIndex
}

// IsVoter reports whether this node is a voter in its own configuration.
func (rn *RaftNode) IsVoter() bool { return rn.voter.Load() }

// IsRemoved reports whether this node is absent from its own configuration
// (removed, or a --join node that has not been added yet).
func (rn *RaftNode) IsRemoved() bool { return rn.removed.Load() }

// noteLeaderContactLocked records valid contact from the current leader
// (pre-vote stickiness).  Must be called with rn.mu held.
func (rn *RaftNode) noteLeaderContactLocked() {
	rn.leaderContact = time.Now()
}

// noteAckLocked records that peer answered in this leader's term.
// Must be called with rn.mu held.
func (rn *RaftNode) noteAckLocked(peer string) {
	if rn.lastAck == nil {
		rn.lastAck = make(map[string]time.Time)
	}
	rn.lastAck[peer] = time.Now()
}

// reachableQuorumLocked reports whether the leader heard from a majority of
// cfg's voters (itself included when it is one) within quorumContactWindow.
// Must be called with rn.mu held.
func (rn *RaftNode) reachableQuorumLocked(cfg Configuration) bool {
	now := time.Now()
	n := 0
	for _, s := range cfg.Servers {
		if s == rn.id {
			n++
			continue
		}
		if t, ok := rn.lastAck[s]; ok && now.Sub(t) <= quorumContactWindow {
			n++
		}
	}
	return n >= len(cfg.Servers)/2+1
}

// proposeConfigLocked appends one EntryConfig entry for next and installs it
// (append-time semantics).  Mirrors flushBatch: the record is enqueued under
// rn.mu, the syncer writes + fsyncs it outside the lock while the replicators
// ship it.  Must be called with rn.mu held, as leader, with no change pending.
func (rn *RaftNode) proposeConfigLocked(next Configuration, why string) (LogEntry, chan error, error) {
	cmd, err := encodeConfiguration(next)
	if err != nil {
		return LogEntry{}, nil, fmt.Errorf("encode configuration: %w", err)
	}
	if len(cmd) > maxConfigBytes {
		return LogEntry{}, nil, fmt.Errorf("%w (%d bytes, limit %d)", ErrConfigTooLarge, len(cmd), maxConfigBytes)
	}
	entry, ch := rn.appendEntryLocked(EntryConfig, cmd)
	if err := rn.store.enqueue([]LogEntry{entry}); err != nil {
		rn.ps.Log = rn.ps.Log[:len(rn.ps.Log)-1] // nobody has seen it yet
		delete(rn.waiters, entry.Index)
		return LogEntry{}, nil, fmt.Errorf("persist log: %w", err)
	}
	// The new configuration takes effect NOW (append time, dissertation §4.1);
	// setConfig starts a replicator for an added peer.
	rn.setConfig(next, entry.Index)
	log.Printf("[raft] node=%s config change (%s) appended idx=%d voters=%v learners=%v",
		rn.id, why, entry.Index, next.Servers, next.Learners)
	rn.kickSyncerLocked()
	rn.kickReplicatorsOnAppendLocked()
	return entry, ch, nil
}

// beginChangeLocked checks the common preconditions of a configuration
// change.  Must be called with rn.mu held.
func (rn *RaftNode) beginChangeLocked() error {
	if rn.role != RoleLeader {
		return ErrNotLeader
	}
	if rn.configIndex > rn.commitIndex {
		return ErrConfigChangeInProgress
	}
	return nil
}

// waitQuorumLocked waits (releasing rn.mu while sleeping) up to
// leaderContactGrace for the leader to have heard from a quorum of cfg's
// voters.  Returns with rn.mu held; the caller must re-check leadership.
func (rn *RaftNode) waitQuorumLocked(cfg func() Configuration) bool {
	deadline := time.Now().Add(leaderContactGrace)
	for {
		if rn.reachableQuorumLocked(cfg()) {
			return true
		}
		if time.Now().After(deadline) || rn.role != RoleLeader {
			return false
		}
		rn.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		rn.mu.Lock()
	}
}

// AddMember adds id (address addr, stored in Configuration.Addrs) as a
// learner, waits for that change to commit, then waits up to wait for the
// leader to promote it to voter once it has caught up.  known fills in
// addresses of existing members the configuration does not carry yet
// (clusters bootstrapped by an older build).
//
// Errors: ErrNotLeader, ErrConfigChangeInProgress, ErrNoQuorum,
// ErrMemberExists (already a voter, or a learner with another address),
// ErrPromotionPending (committed as learner, not yet promoted — the leader
// keeps trying), or a commit timeout.
func (rn *RaftNode) AddMember(id, addr string, known map[string]string, wait time.Duration) error {
	if id == "" {
		return fmt.Errorf("raft: empty member id")
	}
	rn.mu.Lock()
	if err := rn.beginChangeLocked(); err != nil {
		rn.mu.Unlock()
		return err
	}
	switch {
	case rn.config.contains(id):
		rn.mu.Unlock()
		return fmt.Errorf("%w: %s is a voter", ErrMemberExists, id)
	case rn.config.isLearner(id):
		if addr != "" && rn.config.Addrs[id] != "" && rn.config.Addrs[id] != addr {
			cur := rn.config.Addrs[id]
			rn.mu.Unlock()
			return fmt.Errorf("%w: %s is a learner at %s", ErrMemberExists, id, cur)
		}
		rn.mu.Unlock()
		return rn.waitPromoted(id, wait)
	}
	if !rn.waitQuorumLocked(func() Configuration { return rn.config }) {
		rn.mu.Unlock()
		return ErrNoQuorum
	}
	if err := rn.beginChangeLocked(); err != nil { // re-check after the wait
		rn.mu.Unlock()
		return err
	}
	next := rn.config.clone()
	next.Learners = append(next.Learners, id)
	if next.Addrs == nil {
		next.Addrs = make(map[string]string)
	}
	for m, a := range known {
		if next.IsMember(m) && next.Addrs[m] == "" && a != "" {
			next.Addrs[m] = a
		}
	}
	if addr != "" {
		next.Addrs[id] = addr
	}
	entry, ch, err := rn.proposeConfigLocked(next, "add learner "+id)
	rn.mu.Unlock()
	if err != nil {
		return err
	}
	if err := rn.waitApplied(entry.Index, ch); err != nil {
		return err
	}
	return rn.waitPromoted(id, wait)
}

// waitPromoted waits until id is a voter in a committed configuration.
func (rn *RaftNode) waitPromoted(id string, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		rn.mu.Lock()
		voter := rn.config.contains(id)
		committed := rn.configIndex <= rn.commitIndex
		member := rn.config.IsMember(id)
		rn.mu.Unlock()
		if voter && committed {
			return nil
		}
		if !member {
			return fmt.Errorf("raft: %s was removed while catching up", id)
		}
		if time.Now().After(deadline) {
			return ErrPromotionPending
		}
		select {
		case <-rn.done:
			return fmt.Errorf("raft node stopped")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// RemoveMember removes id (voter or learner) through the log and waits for
// the change to commit.  A leader may remove itself: it keeps leading until
// the removal commits under the new configuration's quorum, then steps down.
func (rn *RaftNode) RemoveMember(id string) error {
	rn.mu.Lock()
	if err := rn.beginChangeLocked(); err != nil {
		rn.mu.Unlock()
		return err
	}
	if !rn.config.IsMember(id) {
		rn.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrMemberNotFound, id)
	}
	if rn.config.contains(id) && len(rn.config.Servers) == 1 {
		rn.mu.Unlock()
		return ErrLastVoter
	}
	build := func() Configuration {
		next := rn.config.clone()
		next.Servers = removeID(next.Servers, id)
		next.Learners = removeID(next.Learners, id)
		delete(next.Addrs, id)
		return next
	}
	if rn.config.contains(id) && !rn.waitQuorumLocked(build) {
		rn.mu.Unlock()
		return ErrNoQuorum
	}
	if err := rn.beginChangeLocked(); err != nil {
		rn.mu.Unlock()
		return err
	}
	if !rn.config.IsMember(id) {
		rn.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrMemberNotFound, id)
	}
	entry, ch, err := rn.proposeConfigLocked(build(), "remove "+id)
	rn.mu.Unlock()
	if err != nil {
		return err
	}
	return rn.waitApplied(entry.Index, ch)
}

// AddServer adds a server through the log (as a learner, promoted to voter
// once caught up) and blocks until it is a voter in a committed
// configuration or the submit timeout expires.  Returns nil if it is already
// a voter.  See AddMember.
func (rn *RaftNode) AddServer(id string) error {
	err := rn.AddMember(id, "", nil, rn.submitTimeout)
	if errors.Is(err, ErrMemberExists) {
		rn.mu.Lock()
		voter := rn.config.contains(id)
		rn.mu.Unlock()
		if voter {
			return nil
		}
	}
	return err
}

// RemoveServer removes a server through the log.  Returns nil if it is not a
// member.  See RemoveMember.
func (rn *RaftNode) RemoveServer(id string) error {
	err := rn.RemoveMember(id)
	if errors.Is(err, ErrMemberNotFound) {
		return nil
	}
	return err
}

// learnerRound is one catch-up round of a learner (leader only, under mu).
type learnerRound struct {
	target uint64    // leader's last log index when the round started
	start  time.Time // round start
}

// maybePromoteLearnersLocked promotes one caught-up learner to voter (one
// configuration change at a time).  Called by the leader's heartbeat tick.
// Must be called with rn.mu held.
func (rn *RaftNode) maybePromoteLearnersLocked() {
	if rn.role != RoleLeader {
		return
	}
	if len(rn.config.Learners) == 0 {
		rn.learnerRounds = nil
		return
	}
	if rn.learnerRounds == nil {
		rn.learnerRounds = make(map[string]*learnerRound)
	}
	for id := range rn.learnerRounds {
		if !rn.config.isLearner(id) {
			delete(rn.learnerRounds, id)
		}
	}
	if rn.configIndex > rn.commitIndex {
		return // a change is in flight; promote after it commits
	}
	now := time.Now()
	for _, id := range rn.config.Learners {
		if id == rn.id {
			continue
		}
		r := rn.learnerRounds[id]
		if r == nil {
			rn.learnerRounds[id] = &learnerRound{target: rn.lastLogIndex(), start: now}
			continue
		}
		if rn.matchIndex[id] < r.target {
			continue // round in progress
		}
		if now.Sub(r.start) > promoteRoundMax {
			// Caught up to the round's target, but it took too long: start
			// another round from the current tail.
			r.target, r.start = rn.lastLogIndex(), now
			continue
		}
		next := rn.config.clone()
		next.Learners = removeID(next.Learners, id)
		next.Servers = append(next.Servers, id)
		if !rn.reachableQuorumLocked(next) {
			r.target, r.start = rn.lastLogIndex(), now
			continue
		}
		if _, _, err := rn.proposeConfigLocked(next, "promote "+id); err != nil {
			log.Printf("[raft] node=%s promote %s: %v", rn.id, id, err)
			return
		}
		delete(rn.learnerRounds, id)
		return // one change at a time
	}
}

// onConfigCommitted runs when an EntryConfig entry is applied (committed).
// If the node's CURRENT configuration no longer includes it as a voter, a
// leader steps down (dissertation §4.2.2 — it kept leading only long enough
// to commit its own removal); a node absent from it is marked removed.
// Must be called with rn.mu held.
func (rn *RaftNode) onConfigCommitted(e LogEntry) {
	// Decide from the effective config, not e's payload: a later (appended)
	// config entry may already have superseded e.
	if rn.config.contains(rn.id) {
		return
	}
	if !rn.config.IsMember(rn.id) {
		rn.removed.Store(true)
	}
	if rn.role == RoleLeader {
		log.Printf("[raft] node=%s committed its own removal (idx=%d) — stepping down",
			rn.id, e.Index)
		rn.stepDownLocked()
	}
}

// forceSingleNodeConfigLocked implements Options.ForceNewCluster: it appends,
// in a new term, a configuration holding only this node (keeping its
// address), so the node elects itself and commits its own log.  DANGEROUS —
// it bypasses consensus; every other former member must be wiped and
// re-joined.  Runs at startup only (no goroutines).
func (rn *RaftNode) forceSingleNodeConfigLocked(selfAddr string) error {
	addr := rn.config.Addrs[rn.id]
	if addr == "" {
		addr = selfAddr
	}
	cfg := Configuration{Servers: []string{rn.id}}
	if addr != "" {
		cfg.Addrs = map[string]string{rn.id: addr}
	}
	cmd, err := encodeConfiguration(cfg)
	if err != nil {
		return err
	}
	rn.ps.CurrentTerm++
	rn.ps.VotedFor = ""
	if err := rn.store.saveMeta(raftMeta{CurrentTerm: rn.ps.CurrentTerm}); err != nil {
		return err
	}
	e := LogEntry{Term: rn.ps.CurrentTerm, Index: rn.lastLogIndex() + 1, Type: EntryConfig, Command: cmd}
	rn.ps.Log = append(rn.ps.Log, e)
	if err := rn.store.rewrite(rn.ps.Log); err != nil {
		return err
	}
	rn.durableIndex = rn.lastLogIndex()
	log.Printf("[raft] node=%s WARNING: --force-new-cluster: configuration reset to {%s} at idx=%d term=%d — "+
		"wipe every other former member and re-add it with --join", rn.id, rn.id, e.Index, e.Term)
	return nil
}
