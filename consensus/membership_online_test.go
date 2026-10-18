package consensus

// membership_online_test.go — online membership: --join safety, learner
// catch-up + promotion, pre-vote (removed / partitioned nodes never bump
// terms), ErrNoQuorum, duplicate / unknown members, serialised changes,
// persistence across restarts and --force-new-cluster.

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// startJoiner starts a node in join mode (empty bootstrap configuration).
func startJoiner(t *testing.T, c *snapCluster, id, dir string) (*RaftNode, *mockSnapSM) {
	t.Helper()
	sm := &mockSnapSM{}
	n, err := NewRaftNodeWithOptions(id, nil, dir, sm, c.hub.forNode(id), testOpts(Options{Join: true}))
	if err != nil {
		t.Fatalf("joiner %s: %v", id, err)
	}
	c.hub.register(id, n)
	t.Cleanup(n.Stop)
	return n, sm
}

func terms(nodes []*RaftNode) []uint64 {
	out := make([]uint64, len(nodes))
	for i, n := range nodes {
		out[i] = n.Term()
	}
	return out
}

func learners(n *RaftNode) []string {
	cfg, _, _ := n.Configuration()
	return cfg.Learners
}

// A --join node on its own never campaigns: no term bump, never leader.
func TestMembership_JoinNodeNeverCampaigns(t *testing.T) {
	hub := newMockTransport()
	id := t.Name() + "-j"
	n, err := NewRaftNodeWithOptions(id, nil, t.TempDir(), &mockSM{}, hub.forNode(id), testOpts(Options{Join: true}))
	if err != nil {
		t.Fatal(err)
	}
	hub.register(id, n)
	defer n.Stop()
	time.Sleep(3 * electionTimeoutMax)
	if n.IsLeader() || n.Term() != 0 {
		t.Fatalf("join node: leader=%v term=%d, want follower at term 0", n.IsLeader(), n.Term())
	}
	if !n.IsRemoved() || n.IsVoter() {
		t.Fatalf("join node: removed=%v voter=%v, want removed (not a member) and not a voter", n.IsRemoved(), n.IsVoter())
	}
	// It also refuses to vote for anyone (not in its configuration).
	r := n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "x", LastLogIndex: 100, LastLogTerm: 4})
	if r.VoteGranted || n.Term() != 0 {
		t.Fatalf("join node granted a vote / adopted a term: %+v term=%d", r, n.Term())
	}
}

// An empty join node is added as a learner, catches up from the log, is
// promoted to voter, and then counts toward quorum.
func TestMembership_JoinLearnerPromoted(t *testing.T) {
	c := newSnapCluster(t, 3, testOpts(Options{}))
	if waitForStableLeader(c.nodes, 5*time.Second) < 0 {
		t.Fatal("no leader")
	}
	for i := 0; i < 50; i++ {
		if err := submitWithRetry(c.nodes, []byte(fmt.Sprintf("pre-%d", i)), 8*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	id := t.Name() + "-new"
	n4, sm4 := startJoiner(t, c, id, t.TempDir())
	if err := changeMembershipWithRetry(c.nodes, func(l *RaftNode) error {
		return l.AddMember(id, "127.0.0.1:9", nil, 10*time.Second)
	}, 30*time.Second); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	li := waitForStableLeader(c.nodes, 5*time.Second)
	cfg, _, committed := c.nodes[li].Configuration()
	if !cfg.IsVoter(id) || !committed || len(cfg.Servers) != 4 || len(cfg.Learners) != 0 {
		t.Fatalf("leader config after add: %+v committed=%v", cfg, committed)
	}
	if cfg.Addrs[id] != "127.0.0.1:9" {
		t.Fatalf("address not recorded: %v", cfg.Addrs)
	}
	if !sm4.contains("pre-49") {
		t.Fatal("new voter did not catch up on entries written before it joined")
	}
	if !n4.IsVoter() || n4.IsRemoved() {
		t.Fatalf("new node's own view: voter=%v removed=%v", n4.IsVoter(), n4.IsRemoved())
	}
}

// A learner that cannot catch up (partitioned) never stalls commits: the add
// commits without it, AddMember reports the pending promotion, writes keep
// committing, and once healed it is promoted.
func TestMembership_SlowLearnerDoesNotStallCommits(t *testing.T) {
	c := newSnapCluster(t, 3, testOpts(Options{}))
	if waitForStableLeader(c.nodes, 5*time.Second) < 0 {
		t.Fatal("no leader")
	}
	id := t.Name() + "-slow"
	_, sm4 := startJoiner(t, c, id, t.TempDir())
	c.hub.cutNode(id)
	err := changeMembershipWithRetry(c.nodes, func(l *RaftNode) error {
		err := l.AddMember(id, "", nil, 500*time.Millisecond)
		if errors.Is(err, ErrPromotionPending) {
			return nil
		}
		if err == nil {
			return fmt.Errorf("promoted while partitioned")
		}
		return err
	}, 20*time.Second)
	if err != nil {
		t.Fatalf("AddMember (partitioned learner): %v", err)
	}
	li := waitForStableLeader(c.nodes, 5*time.Second)
	if l := learners(c.nodes[li]); len(l) != 1 || l[0] != id {
		t.Fatalf("want %s as learner, got %v", id, l)
	}
	t0 := time.Now()
	for i := 0; i < 20; i++ {
		if err := submitWithRetry(c.nodes, []byte(fmt.Sprintf("w-%d", i)), 5*time.Second); err != nil {
			t.Fatalf("commit with a stalled learner: %v", err)
		}
	}
	if d := time.Since(t0); d > 5*time.Second {
		t.Fatalf("20 commits took %v with a stalled learner", d)
	}
	c.hub.healNode(id)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		li = waitForStableLeader(c.nodes, 2*time.Second)
		if li >= 0 {
			if cfg, _, ok := c.nodes[li].Configuration(); ok && cfg.IsVoter(id) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if cfg, _, ok := c.nodes[li].Configuration(); !ok || !cfg.IsVoter(id) {
		t.Fatalf("healed learner not promoted: %+v", cfg)
	}
	if !sm4.contains("w-19") {
		t.Fatal("promoted node missing an entry")
	}
}

// A follower that is removed and keeps running (or is restarted on its old
// data with its old bootstrap peers) never bumps its own or anyone's term.
func TestMembership_RemovedNodeDoesNotDisrupt(t *testing.T) {
	c := newSnapCluster(t, 3, testOpts(Options{}))
	li := waitForStableLeader(c.nodes, 5*time.Second)
	if li < 0 {
		t.Fatal("no leader")
	}
	ri := (li + 1) % 3
	rid := c.ids[ri]
	// Cut it first so it never even sees its own removal entry — the worst
	// case: in its own view it is still a voter.
	c.hub.cutNode(rid)
	if err := changeMembershipWithRetry(c.nodes, func(l *RaftNode) error {
		if l.id == rid {
			return ErrNotLeader
		}
		return l.RemoveMember(rid)
	}, 20*time.Second); err != nil {
		t.Fatalf("remove: %v", err)
	}
	removedTerm := c.nodes[ri].Term()
	// Restart the removed node on its old data dir with its old bootstrap
	// peers, then heal it.
	c.nodes[ri].Stop()
	dir := c.nodes[ri].dataDir
	var peers []string
	for _, id := range c.ids {
		if id != rid {
			peers = append(peers, id)
		}
	}
	rn, err := NewRaftNodeWithOptions(rid, peers, dir, &mockSnapSM{}, c.hub.forNode(rid), testOpts(Options{}))
	if err != nil {
		t.Fatal(err)
	}
	c.hub.register(rid, rn)
	defer rn.Stop()
	c.nodes[ri] = rn
	c.hub.healNode(rid)

	remaining := []*RaftNode{}
	for i, n := range c.nodes {
		if i != ri {
			remaining = append(remaining, n)
		}
	}
	if waitForStableLeader(remaining, 5*time.Second) < 0 {
		t.Fatal("no leader among remaining")
	}
	before := terms(remaining)
	time.Sleep(6 * electionTimeoutMax) // several election timeouts of the removed node
	after := terms(remaining)
	for i := range before {
		if after[i] != before[i] {
			t.Fatalf("member term changed while a removed node ran: %v → %v", before, after)
		}
	}
	if rn.Term() > removedTerm && rn.Term() > after[0] {
		t.Fatalf("removed node bumped its term past the cluster: %d (cluster %d)", rn.Term(), after[0])
	}
	if rn.PreVotesLost() == 0 {
		t.Fatal("removed node never ran (and lost) a pre-vote — was it campaigning at all?")
	}
	if rn.IsLeader() {
		t.Fatal("removed node became leader")
	}
	if err := submitWithRetry(remaining, []byte("after"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
}

// A partitioned follower that rejoins does not depose the leader (pre-vote).
func TestPreVote_PartitionedFollowerDoesNotBumpTerm(t *testing.T) {
	c := newSnapCluster(t, 3, testOpts(Options{}))
	li := waitForStableLeader(c.nodes, 5*time.Second)
	if li < 0 {
		t.Fatal("no leader")
	}
	fi := (li + 1) % 3
	before := c.nodes[li].Term()
	c.hub.cutNode(c.ids[fi])
	time.Sleep(5 * electionTimeoutMax)
	if got := c.nodes[fi].Term(); got != before {
		t.Fatalf("partitioned follower bumped its term %d → %d (pre-vote must prevent it)", before, got)
	}
	c.hub.healNode(c.ids[fi])
	time.Sleep(2 * electionTimeoutMax)
	if !c.nodes[li].IsLeader() || c.nodes[li].Term() != before {
		t.Fatalf("leader disrupted by a rejoining follower: leader=%v term %d → %d",
			c.nodes[li].IsLeader(), before, c.nodes[li].Term())
	}
}

// Changes without a reachable quorum are refused up front.
func TestMembership_NoQuorum(t *testing.T) {
	old := quorumContactWindow
	quorumContactWindow = 300 * time.Millisecond
	defer func() { quorumContactWindow = old }()
	c := newSnapCluster(t, 3, testOpts(Options{}))
	li := waitForStableLeader(c.nodes, 5*time.Second)
	if li < 0 {
		t.Fatal("no leader")
	}
	l := c.nodes[li]
	for i := range c.nodes {
		if i != li {
			c.hub.cutNode(c.ids[i])
		}
	}
	time.Sleep(2 * quorumContactWindow)
	if !l.IsLeader() {
		t.Skip("leadership moved before the partition")
	}
	t0 := time.Now()
	if err := l.AddMember("x", "", nil, time.Second); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("AddMember without quorum: %v, want ErrNoQuorum", err)
	}
	if err := l.RemoveMember(c.ids[(li+1)%3]); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("RemoveMember without quorum: %v, want ErrNoQuorum", err)
	}
	if d := time.Since(t0); d > 2*leaderContactGrace+time.Second {
		t.Fatalf("refusals took %v", d)
	}
	if cfg, idx, _ := l.Configuration(); idx != 0 || len(cfg.Servers) != 3 {
		t.Fatalf("a refused change was appended: idx=%d cfg=%+v", idx, cfg)
	}
}

func TestMembership_DuplicateAndUnknown(t *testing.T) {
	c := newSnapCluster(t, 3, testOpts(Options{}))
	li := waitForStableLeader(c.nodes, 5*time.Second)
	if li < 0 {
		t.Fatal("no leader")
	}
	l := c.nodes[li]
	if err := l.AddMember(c.ids[(li+1)%3], "", nil, time.Second); !errors.Is(err, ErrMemberExists) {
		t.Fatalf("duplicate voter: %v, want ErrMemberExists", err)
	}
	if err := l.RemoveMember("nobody"); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("unknown member: %v, want ErrMemberNotFound", err)
	}
	if err := l.RemoveServer("nobody"); err != nil {
		t.Fatalf("legacy RemoveServer of unknown: %v, want nil", err)
	}
	// Learner with an address: a second add with another address conflicts.
	id := t.Name() + "-l"
	c.hub.cutNode(id) // never catches up
	if err := l.AddMember(id, "h:1", nil, 200*time.Millisecond); !errors.Is(err, ErrPromotionPending) {
		t.Fatalf("add learner: %v", err)
	}
	if err := l.AddMember(id, "h:2", nil, 200*time.Millisecond); !errors.Is(err, ErrMemberExists) {
		t.Fatalf("learner re-added with another address: %v, want ErrMemberExists", err)
	}
	if err := l.RemoveMember(id); err != nil {
		t.Fatalf("remove learner: %v", err)
	}
	if cfg, _, _ := l.Configuration(); cfg.IsMember(id) || cfg.Addrs[id] != "" {
		t.Fatalf("learner still present: %+v", cfg)
	}
}

// Two concurrent changes serialise: one wins, the other gets
// ErrConfigChangeInProgress (and succeeds when retried).
func TestMembership_ConcurrentChangesSerialise(t *testing.T) {
	c := newSnapCluster(t, 3, testOpts(Options{}))
	li := waitForStableLeader(c.nodes, 5*time.Second)
	if li < 0 {
		t.Fatal("no leader")
	}
	ids := []string{t.Name() + "-a", t.Name() + "-b"}
	for _, id := range ids {
		startJoiner(t, c, id, t.TempDir())
	}
	l := c.nodes[li]
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = l.AddMember(ids[i], "", nil, 10*time.Second)
		}(i)
	}
	wg.Wait()
	busy := 0
	for i, err := range errs {
		switch {
		case err == nil:
		case errors.Is(err, ErrConfigChangeInProgress):
			busy++
			if err := changeMembershipWithRetry(c.nodes, func(l *RaftNode) error {
				return l.AddMember(ids[i], "", nil, 10*time.Second)
			}, 30*time.Second); err != nil {
				t.Fatalf("retry %s: %v", ids[i], err)
			}
		default:
			t.Fatalf("add %s: %v", ids[i], err)
		}
	}
	if busy == 0 {
		t.Log("both adds happened to run one after the other (no overlap this run)")
	}
	li = waitForStableLeader(c.nodes, 5*time.Second)
	cfg, _, _ := c.nodes[li].Configuration()
	if len(cfg.Servers) != 5 {
		t.Fatalf("want 5 voters, got %+v", cfg)
	}
}

// The configuration survives a restart of every node and wins over the
// (stale) bootstrap peers they are restarted with — from the log, and from
// the snapshot header once the log was compacted.
func TestMembership_PersistsAcrossRestart(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprintf("compacted=%v", compact), func(t *testing.T) {
			opts := testOpts(Options{})
			if compact {
				opts.SnapshotThreshold = 16
			}
			c := newSnapCluster(t, 3, opts)
			if waitForStableLeader(c.nodes, 5*time.Second) < 0 {
				t.Fatal("no leader")
			}
			id := t.Name() + "-n"
			dir4 := t.TempDir()
			n4, _ := startJoiner(t, c, id, dir4)
			if err := changeMembershipWithRetry(c.nodes, func(l *RaftNode) error {
				return l.AddMember(id, "h:4", nil, 10*time.Second)
			}, 30*time.Second); err != nil {
				t.Fatal(err)
			}
			all := append(append([]*RaftNode{}, c.nodes...), n4)
			// Remove an original member.
			gone := c.ids[0]
			if err := changeMembershipWithRetry(all, func(l *RaftNode) error {
				return l.RemoveMember(gone)
			}, 30*time.Second); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 64; i++ {
				if err := submitWithRetry(all[1:], []byte(fmt.Sprintf("x%d", i)), 5*time.Second); err != nil {
					t.Fatal(err)
				}
			}
			want := map[string]bool{c.ids[1]: true, c.ids[2]: true, id: true}
			// Restart every node with the ORIGINAL bootstrap peers (and the
			// joiner without --join and with stale peers).
			dirs := map[string]string{}
			for _, n := range all {
				dirs[n.id] = n.dataDir
				n.Stop()
			}
			var restarted []*RaftNode
			for _, nid := range []string{c.ids[1], c.ids[2], id} {
				var peers []string
				for _, p := range c.ids {
					if p != nid {
						peers = append(peers, p)
					}
				}
				n, err := NewRaftNodeWithOptions(nid, peers, dirs[nid], &mockSnapSM{}, c.hub.forNode(nid), opts)
				if err != nil {
					t.Fatalf("restart %s: %v", nid, err)
				}
				c.hub.register(nid, n)
				defer n.Stop()
				restarted = append(restarted, n)
				cfg, _, _ := n.Configuration()
				if len(cfg.Servers) != 3 {
					t.Fatalf("%s restarted with config %+v, want the persisted 3 voters", nid, cfg)
				}
				for _, s := range cfg.Servers {
					if !want[s] {
						t.Fatalf("%s restarted with stale member %s: %+v", nid, s, cfg)
					}
				}
				if cfg.Addrs[id] != "h:4" {
					t.Fatalf("%s lost the new member's address: %+v", nid, cfg.Addrs)
				}
			}
			if waitForStableLeader(restarted, 10*time.Second) < 0 {
				t.Fatal("restarted cluster elected no leader")
			}
			if err := submitWithRetry(restarted, []byte("post-restart"), 5*time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// --force-new-cluster: after losing two of three members for good, the
// survivor resets its configuration to itself, elects itself and keeps its
// data.
func TestMembership_ForceNewCluster(t *testing.T) {
	c := newSnapCluster(t, 3, testOpts(Options{}))
	if waitForStableLeader(c.nodes, 5*time.Second) < 0 {
		t.Fatal("no leader")
	}
	if err := submitWithRetry(c.nodes, []byte("keep-me"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // let every node apply it
	survivor := c.nodes[2]
	dir := survivor.dataDir
	for _, n := range c.nodes {
		n.Stop()
	}
	sm := &mockSnapSM{}
	id := survivor.id
	n, err := NewRaftNodeWithOptions(id, []string{c.ids[0], c.ids[1]}, dir, sm, c.hub.forNode(id),
		testOpts(Options{ForceNewCluster: true, SelfAddr: "h:3"}))
	if err != nil {
		t.Fatal(err)
	}
	c.hub.register(id, n)
	defer n.Stop()
	if waitForLeader([]*RaftNode{n}, 5*time.Second) < 0 {
		t.Fatal("forced node did not elect itself")
	}
	if err := n.Submit([]byte("after-force")); err != nil {
		t.Fatal(err)
	}
	if !sm.contains("keep-me") || !sm.contains("after-force") {
		t.Fatal("forced node lost data")
	}
	cfg, _, _ := n.Configuration()
	if len(cfg.Servers) != 1 || cfg.Servers[0] != id || cfg.Addrs[id] != "h:3" {
		t.Fatalf("forced config: %+v", cfg)
	}
}
