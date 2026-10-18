package main

// membership_replicated.go — online membership for --mode=replicated.
//
// There is no consensus log in this mode, so membership is a versioned
// document (replState) persisted on every node in <data>/cluster_membership.json
// (temp → fsync → rename → dir fsync).  Once that file exists it wins over
// --peers; a node without it bootstraps from --peers (version 0, not
// persisted, exactly as before this feature).
//
// Changes are serialised by ONE node, the membership coordinator: the member
// with the lowest ID (every node computes the same one from the same
// version).  Any node accepts an admin request and forwards it there over
// the transfer listener (HMAC, invariant 56).  The coordinator, holding its
// change mutex:
//
//  1. checks every reachable member is still at its version (a concurrent
//     change by another coordinator — possible only while a previous version
//     is still propagating — makes it refuse with 409);
//  2. for a removal, DRAINS the leaving node: it refuses new writes and waits
//     until every write it originated is acknowledged by every replica
//     (ReplicationEngine.Drained, invariant 61) — skipped only with
//     force=true, for a dead node;
//  3. writes version+1 locally and pushes it to every old and new member
//     (/cluster/membership/apply).  Members that miss it pull it: every node
//     asks the members it knows for their version every membershipPull, and
//     at startup before it replicates anything.
//
// Applying a version on a node: partition map AddNode / RemoveNode (the
// auto-rebalancer then runs the RF-aware migration, invariant 62 — the
// placement RF is every member, because the replication engine sends every
// write to every member), replication engine AddMemberReplica (catch-up of
// this node's whole change feed before live batches) / RemoveMemberReplica
// (recovery state rewritten at once), redirect and transfer addresses.  A
// node that finds itself removed refuses writes and its own migration sends
// every key it holds to the remaining members, deleting each locally only
// after every one of them acknowledged it.
//
// Migrated copies are applied put-if-absent and marked as peer writes
// (receiveMigrated): the replication stream is the ordered source of truth
// for each key, so a copy never overwrites a newer write or resurrects a
// delete, and never re-enters this node's catch-up feed as its own write.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/VeltrixDB/veltrixdb/cluster"
)

// replState is the persisted replicated-mode membership.
type replState struct {
	Version     uint64       `json:"version"`
	Coordinator string       `json:"coordinator,omitempty"` // node that wrote this version (tie-break)
	Members     []memberInfo `json:"members"`
	UpdatedAt   int64        `json:"updated_at_ns,omitempty"`
}

type drainRequest struct {
	Cancel bool `json:"cancel,omitempty"`
	WaitMs int  `json:"wait_ms,omitempty"`
}

func (s replState) clone() replState {
	out := s
	out.Members = append([]memberInfo(nil), s.Members...)
	return out
}

func (s replState) has(id string) bool {
	_, ok := s.addr(id)
	return ok
}

func (s replState) addr(id string) (string, bool) {
	for _, m := range s.Members {
		if m.ID == id {
			return m.Addr, true
		}
	}
	return "", false
}

func (s *replState) sortMembers() {
	sort.Slice(s.Members, func(i, j int) bool { return s.Members[i].ID < s.Members[j].ID })
}

// coordinatorID is the member that serialises changes: the lowest ID.
func (s replState) coordinatorID() string {
	if len(s.Members) == 0 {
		return ""
	}
	best := s.Members[0].ID
	for _, m := range s.Members[1:] {
		if m.ID < best {
			best = m.ID
		}
	}
	return best
}

// newer reports whether a supersedes b: a higher version, or the same
// version written by a higher coordinator ID (deterministic tie-break, so
// nodes that saw two conflicting changes converge on one).
func (a replState) newer(b replState) bool {
	if a.Version != b.Version {
		return a.Version > b.Version
	}
	return a.Coordinator > b.Coordinator
}

func loadReplState(path string) (*replState, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var s replState
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	return &s, nil
}

func saveReplState(path string, s replState) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

func (mm *membershipManager) statePath() string { return filepath.Join(mm.dataDir, membershipFile) }

// resolveReplicatedPeers decides this node's starting membership (before the
// replication engine registers any replica): the persisted version, else
// --peers; then any newer version a reachable member has.  It returns the
// peers to register.
func (mm *membershipManager) resolveReplicatedPeers(peers []peerSpec) ([]peerSpec, error) {
	path := mm.statePath()
	if mm.forceReset {
		if _, err := os.Stat(path); err == nil {
			bak := fmt.Sprintf("%s.reset-%d", path, time.Now().Unix())
			if err := os.Rename(path, bak); err != nil {
				return nil, err
			}
			log.Printf("[membership] WARNING --force-new-cluster: persisted membership moved to %s — starting from --peers", bak)
		}
	}
	st, err := loadReplState(path)
	if err != nil {
		return nil, fmt.Errorf("%w (fix or remove the file, or start with --force-new-cluster to fall back to --peers)", err)
	}
	var cur replState
	if st != nil {
		cur = *st
		log.Printf("[membership] persisted membership v%d (%d members) — it wins over --peers", cur.Version, len(cur.Members))
	} else if len(mm.joinSeeds) > 0 {
		cur = replState{Version: 0} // not a member until a coordinator adds it
	} else {
		cur = replState{Version: 0, Members: []memberInfo{{ID: mm.localID, Addr: mm.selfAddr}}}
		for _, p := range peers {
			cur.Members = append(cur.Members, memberInfo{ID: p.id, Addr: p.clientAddr})
		}
		cur.sortMembers()
	}
	// Adopt a newer version from any reachable member or seed, before this
	// node replicates anything (a node removed while it was down must not
	// resume replicating to its old peers).
	if mm.authOK {
		var cands []string
		for _, m := range cur.Members {
			if m.ID != mm.localID && m.Addr != "" {
				cands = append(cands, m.Addr)
			}
		}
		cands = append(cands, mm.joinSeeds...)
		deadline := time.Now().Add(6 * time.Second)
		for _, a := range cands {
			if time.Now().After(deadline) {
				break
			}
			if rs, err := mm.fetchState(deadline, deriveAddr(a, offTransfer)); err == nil && rs.newer(cur) {
				cur = rs
			}
		}
		if (st == nil || cur.newer(*st)) && (cur.has(mm.localID) || len(mm.joinSeeds) == 0) {
			if cur.Version > 0 {
				if err := saveReplState(path, cur); err != nil {
					return nil, fmt.Errorf("persist membership: %w", err)
				}
				log.Printf("[membership] adopted membership v%d from a peer", cur.Version)
			}
		}
	}
	mm.st = cur
	switch {
	case cur.has(mm.localID):
	case cur.Version == 0 || len(mm.joinSeeds) > 0:
		mm.joining.Store(true)
		log.Printf("[membership] %s is joining: writes are refused until it is added", mm.localID)
	default:
		mm.removed.Store(true)
		log.Printf("[membership] WARNING %s is not in membership v%d (it was removed): it refuses writes and replicates nothing. "+
			"Wipe its data and start it with --join to add it back.", mm.localID, cur.Version)
	}
	var out []peerSpec
	if mm.removed.Load() {
		return nil, nil
	}
	for _, m := range mm.st.Members {
		if m.ID == mm.localID {
			continue
		}
		host, port, err := splitClientAddr(m.Addr)
		if err != nil {
			log.Printf("[membership] member %s has a bad address %q: %v", m.ID, m.Addr, err)
			continue
		}
		out = append(out, peerSpec{id: m.ID, clientAddr: m.Addr, host: host, clientPort: port})
	}
	return out, nil
}

func (mm *membershipManager) fetchState(deadline time.Time, transferAddr string) (replState, error) {
	if d := time.Until(deadline); d < 2*time.Second && d > 0 {
		deadline = time.Now().Add(d)
	} else {
		deadline = time.Now().Add(2 * time.Second)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	var rs replState
	err := mm.client.PostJSON(ctx, transferAddr, "/cluster/membership", struct{}{}, &rs)
	return rs, err
}

func (mm *membershipManager) replState() replState {
	mm.stMu.Lock()
	defer mm.stMu.Unlock()
	return mm.st.clone()
}

func (mm *membershipManager) replMember() bool {
	return !mm.joining.Load() && !mm.removed.Load()
}

// writeGateErr: see coordinator.writeGate.
func (mm *membershipManager) writeGateErr() error {
	switch {
	case mm.removed.Load():
		return errors.New("node removed from the cluster: it accepts no writes")
	case mm.joining.Load():
		return errors.New("node is joining the cluster: writes are refused until it is added")
	case time.Now().UnixNano() < mm.drainUntil.Load():
		return errors.New("node is draining for removal: write to another node")
	}
	return nil
}

func (mm *membershipManager) replicatedView() membersView {
	st := mm.replState()
	v := membersView{Mode: "replicated", NodeID: mm.localID, Version: st.Version, Committed: true,
		Coordinator: st.coordinatorID()}
	switch {
	case mm.removed.Load():
		v.SelfStatus = "removed"
	case mm.joining.Load():
		v.SelfStatus = "joining"
	case time.Now().UnixNano() < mm.drainUntil.Load():
		v.SelfStatus = "draining"
	default:
		v.SelfStatus = "member"
	}
	for _, m := range st.Members {
		if mm.joining.Load() && m.ID == mm.localID {
			continue
		}
		v.Members = append(v.Members, memberInfo{ID: m.ID, Addr: m.Addr, Role: "member"})
	}
	return v
}

// replicatedChange runs ch on the coordinator, or forwards it there.
func (mm *membershipManager) replicatedChange(ch memberChange) httpAnswer {
	if !mm.authOK {
		return answerErr(http.StatusServiceUnavailable,
			"replicated-mode membership changes are pushed to every node over the transfer listener: configure --cluster-secret-file (or --cluster-mtls) on every node")
	}
	cur := mm.replState()
	if mm.joining.Load() || mm.removed.Load() {
		// Not a member: its view may be stale — let a member coordinate.
		if ch.Hops > 0 {
			return answerErr(http.StatusMisdirectedRequest, "this node is not a member")
		}
		for _, m := range cur.Members {
			if m.ID != mm.localID {
				if a := mm.forwardTo(ch, m.ID, m.Addr); a.status != http.StatusServiceUnavailable {
					return a
				}
			}
		}
		return answerErr(http.StatusServiceUnavailable, "no member reachable")
	}
	coord := cur.coordinatorID()
	if ch.Op == "remove" && ch.Force && ch.ID == coord {
		// Force-removing the coordinator (it is dead): the next lowest ID
		// runs its removal.
		next := ""
		for _, m := range cur.Members {
			if m.ID != ch.ID && (next == "" || m.ID < next) {
				next = m.ID
			}
		}
		coord = next
	}
	if coord != mm.localID && ch.Hops == 0 {
		a, _ := cur.addr(coord)
		ans := mm.forwardTo(ch, coord, a)
		if ans.status == http.StatusServiceUnavailable && ch.Force {
			log.Printf("[membership] WARNING coordinator %s unreachable — running the forced change here", coord)
		} else {
			return ans
		}
	}

	mm.changeMu.Lock()
	defer mm.changeMu.Unlock()
	cur = mm.replState()
	next := cur.clone()
	next.Version = cur.Version + 1
	next.Coordinator = mm.localID
	next.UpdatedAt = time.Now().UnixNano()
	switch ch.Op {
	case "add", "join":
		if a, ok := cur.addr(ch.ID); ok {
			if ch.Op == "join" && a == ch.Addr {
				return answer(http.StatusOK, changeResult{Status: "member", Detail: "already a member", Version: cur.Version})
			}
			return answerErr(http.StatusConflict, "member %s already exists (at %s)", ch.ID, a)
		}
		next.Members = append(next.Members, memberInfo{ID: ch.ID, Addr: ch.Addr})
	case "remove":
		if !cur.has(ch.ID) {
			return answerErr(http.StatusNotFound, "no such member %s", ch.ID)
		}
		if len(cur.Members) == 1 {
			return answerErr(http.StatusBadRequest, "cannot remove the last member")
		}
		kept := next.Members[:0]
		for _, m := range next.Members {
			if m.ID != ch.ID {
				kept = append(kept, m)
			}
		}
		next.Members = kept
	default:
		return answerErr(http.StatusBadRequest, "unknown op %q", ch.Op)
	}
	next.sortMembers()

	// (1) no other coordinator raced us to this version.
	if !ch.Force {
		for _, m := range cur.Members {
			if m.ID == mm.localID {
				continue
			}
			rs, err := mm.fetchState(time.Now().Add(3*time.Second), deriveAddr(m.Addr, offTransfer))
			if err == nil && rs.Version >= next.Version {
				return answerErr(http.StatusConflict, "member %s already has membership v%d (a concurrent change) — retry", m.ID, rs.Version)
			}
		}
	}
	// (2) drain a node before it leaves.
	if ch.Op == "remove" && !ch.Force {
		wait := waitFor(ch, defaultDrainWait)
		var err error
		if ch.ID == mm.localID {
			err = mm.drainSelf(wait)
		} else {
			a, _ := cur.addr(ch.ID)
			ctx, cancel := context.WithTimeout(context.Background(), wait+30*time.Second)
			err = mm.client.PostJSON(ctx, deriveAddr(a, offTransfer), "/cluster/drain",
				drainRequest{WaitMs: int(wait / time.Millisecond)}, nil)
			cancel()
			if err != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = mm.client.PostJSON(ctx, deriveAddr(a, offTransfer), "/cluster/drain", drainRequest{Cancel: true}, nil)
				cancel()
			}
		}
		if err != nil {
			return answerErr(http.StatusConflict, "drain of %s failed — not removed (use force=true only if it is dead; its un-replicated writes are then lost): %v", ch.ID, err)
		}
	}
	// (3) commit locally, then push.
	if _, err := mm.applyReplState(next); err != nil {
		return answerErr(http.StatusInternalServerError, "persist membership: %v", err)
	}
	failed := mm.broadcast(next, cur)
	log.Printf("[membership] replicated: v%d %s %s — members %v (not yet reached: %v)",
		next.Version, ch.Op, ch.ID, memberIDs(next), failed)
	status := "ok"
	if ch.Op == "remove" {
		status = "removed"
	}
	return answer(http.StatusOK, changeResult{Status: status, Version: next.Version, Members: mm.replicatedView().Members, Failed: failed})
}

func memberIDs(s replState) []string {
	out := make([]string, 0, len(s.Members))
	for _, m := range s.Members {
		out = append(out, m.ID)
	}
	return out
}

func (mm *membershipManager) forwardTo(ch memberChange, id, clientAddr string) httpAnswer {
	ch.Hops++
	var res changeResult
	ctx, cancel := context.WithTimeout(context.Background(), waitFor(ch, defaultDrainWait)+30*time.Second)
	defer cancel()
	err := mm.client.PostJSON(ctx, mm.transferAddrOf(id, clientAddr), "/cluster/members", ch, &res)
	if err == nil {
		return answer(http.StatusOK, res)
	}
	var he *cluster.HTTPError
	if errors.As(err, &he) {
		return httpAnswer{status: he.Status, raw: he.Body}
	}
	return answerErr(http.StatusServiceUnavailable,
		"membership coordinator %s unreachable (%v) — retry; if it is dead and being removed, pass force=true", id, err)
}

// broadcast pushes st to every member of st and prev except self; returns
// the members that did not take it.
func (mm *membershipManager) broadcast(st, prev replState) []string {
	targets := map[string]string{}
	for _, m := range prev.Members {
		targets[m.ID] = m.Addr
	}
	for _, m := range st.Members {
		targets[m.ID] = m.Addr
	}
	delete(targets, mm.localID)
	var mu sync.Mutex
	var failed []string
	var wg sync.WaitGroup
	for id, a := range targets {
		wg.Add(1)
		go func(id, a string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := mm.client.PostJSON(ctx, deriveAddr(a, offTransfer), "/cluster/membership/apply", st, nil); err != nil {
				mu.Lock()
				failed = append(failed, id)
				mu.Unlock()
				log.Printf("[membership] push v%d to %s: %v (it pulls it later)", st.Version, id, err)
			}
		}(id, a)
	}
	wg.Wait()
	sort.Strings(failed)
	return failed
}

// applyReplState installs st if it supersedes the current version: persist
// first, then partition map + replication engine + addresses.
func (mm *membershipManager) applyReplState(st replState) (bool, error) {
	st = st.clone()
	st.sortMembers()
	mm.applyMu.Lock()
	defer mm.applyMu.Unlock()
	mm.stMu.Lock()
	cur := mm.st.clone()
	if !st.newer(cur) {
		mm.stMu.Unlock()
		return false, nil
	}
	// A joining node persists nothing until it is a member (restarted
	// without --join it would otherwise read as removed).
	if st.has(mm.localID) || !mm.joining.Load() {
		if err := saveReplState(mm.statePath(), st); err != nil {
			mm.stMu.Unlock()
			return false, err
		}
	}
	mm.st = st.clone()
	mm.stMu.Unlock()

	wasJoining := mm.joining.Load()
	selfIn := st.has(mm.localID)
	before := map[string]string{}
	for _, m := range cur.Members {
		before[m.ID] = m.Addr
	}
	after := map[string]string{}
	for _, m := range st.Members {
		after[m.ID] = m.Addr
	}
	addrs := map[string]string{}
	for id, a := range after {
		addrs[id] = a
	}
	mm.coord.setPeerAddrs(addrs)

	if !selfIn {
		if wasJoining {
			return true, nil // still waiting to be added
		}
		mm.removed.Store(true)
		log.Printf("[membership] %s was removed from the cluster (v%d): writes refused; its keys go to the remaining members", mm.localID, st.Version)
		for id := range before {
			if id != mm.localID && mm.repl.HasReplica(id) {
				_ = mm.repl.RemoveMemberReplica(id)
			}
		}
		// Removing ourselves from our own map makes this node's migration
		// send every key it holds to the remaining members and delete each
		// one only after all of them acknowledged it (invariant 62).
		_ = mm.pm.RemoveNode(mm.localID)
		_ = mm.pm.Rebalance(mm.pm.PartitionCount())
		return true, nil
	}
	if wasJoining {
		mm.joining.Store(false)
		log.Printf("[membership] %s joined the cluster (v%d)", mm.localID, st.Version)
	}
	mm.removed.Store(false)
	changed := false
	for id, a := range after {
		if id == mm.localID {
			continue
		}
		if _, ok := before[id]; ok && !wasJoining && mm.repl.HasReplica(id) {
			continue
		}
		host, port, err := splitClientAddr(a)
		if err != nil {
			log.Printf("[membership] member %s: bad address %q", id, a)
			continue
		}
		if err := mm.pm.AddNode(id, host, port); err == nil {
			changed = true
		}
		_ = mm.pm.SetNodeGossipAddr(id, deriveAddr(a, offGossip))
		mm.pm.SetNodeTransferAddr(id, deriveAddr(a, offTransfer))
		if !mm.repl.HasReplica(id) {
			if err := mm.repl.AddMemberReplica(id, host, port); err != nil {
				log.Printf("[membership] add replica %s: %v", id, err)
			}
		}
	}
	for id := range before {
		if id == mm.localID {
			continue
		}
		if _, ok := after[id]; ok {
			continue
		}
		if mm.repl.HasReplica(id) {
			_ = mm.repl.RemoveMemberReplica(id)
		}
		mm.engine.ForgetReplicaWatermark(id)
		if err := mm.pm.RemoveNode(id); err == nil {
			changed = true
		}
		mm.pm.ForgetNodeTransferAddr(id)
	}
	if changed {
		_ = mm.pm.Rebalance(mm.pm.PartitionCount())
	}
	log.Printf("[membership] applied membership v%d: %v", st.Version, memberIDs(st))
	return true, nil
}

// drainSelf refuses new writes and waits until every write this node
// originated is acknowledged by every replica.  The refusal outlives the
// call (until removal, or cancel) so nothing slips in before the removal.
func (mm *membershipManager) drainSelf(wait time.Duration) error {
	if wait <= 0 {
		wait = defaultDrainWait
	}
	mm.drainUntil.Store(time.Now().Add(wait + time.Minute).UnixNano())
	log.Printf("[membership] draining %s: writes refused, waiting for replicas to acknowledge every write", mm.localID)
	deadline := time.Now().Add(wait)
	streak := 0
	for {
		ok, why := mm.repl.Drained()
		if ok {
			// Twice in a row, 300 ms apart: a write that passed the gate
			// just before draining began is registered by then.
			if streak++; streak >= 2 {
				log.Printf("[membership] %s drained", mm.localID)
				return nil
			}
		} else {
			streak = 0
		}
		if time.Now().After(deadline) {
			mm.drainUntil.Store(0)
			return fmt.Errorf("not drained after %v: %s", wait, why)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// pullLoop adopts newer membership versions from the members this node knows
// (a node that was down, or missed a push, converges within membershipPull).
func (mm *membershipManager) pullLoop() {
	defer mm.wg.Done()
	<-mm.attached
	if !mm.authOK {
		return
	}
	t := time.NewTicker(membershipPull)
	defer t.Stop()
	for {
		select {
		case <-mm.stopCh:
			return
		case <-t.C:
		}
		cur := mm.replState()
		for _, m := range cur.Members {
			if m.ID == mm.localID || m.Addr == "" {
				continue
			}
			rs, err := mm.fetchState(time.Now().Add(3*time.Second), deriveAddr(m.Addr, offTransfer))
			if err != nil || !rs.newer(mm.replState()) {
				continue
			}
			if ok, err := mm.applyReplState(rs); ok {
				log.Printf("[membership] pulled membership v%d from %s", rs.Version, m.ID)
			} else if err != nil {
				log.Printf("[membership] apply pulled v%d: %v", rs.Version, err)
			}
		}
	}
}

// ── key stripes: replication apply vs migrated put-if-absent ───────────────

var keyStripes [256]sync.Mutex

func lockKey(key string) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	m := &keyStripes[h.Sum32()&255]
	m.Lock()
	return m.Unlock
}

// receiveMigrated applies one key received from another node's partition
// migration (replicated mode): only when this node holds neither a value
// nor a tombstone for it, and marked as a peer write.
func (mm *membershipManager) receiveMigrated(kv cluster.KeyValue) error {
	unlock := lockKey(kv.Key)
	defer unlock()
	if mm.engine.PresentOrDeleted(kv.Key) {
		return nil
	}
	return mm.engine.PutReplicated(kv.Key, kv.Value, kv.TTL)
}
