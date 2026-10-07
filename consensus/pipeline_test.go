package consensus

// pipeline_test.go — follower acknowledgement after durability, pipelined
// replication over a lossy / reordering transport, crash after a partial
// durable state, and the stream transport.

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── fsync fault injection ─────────────────────────────────────────────────────

// fsyncGate replaces a logStore's fsync: while held, every fsync blocks; fail
// makes fsyncs return an error; syncedSize records the file size covered by
// the last successful fsync (what a crash would keep).
type fsyncGate struct {
	mu         sync.Mutex
	held       bool
	release    chan struct{}
	fail       error
	entered    chan struct{}
	syncedSize atomic.Int64
	calls      atomic.Int64
}

func newFsyncGate() *fsyncGate {
	return &fsyncGate{release: make(chan struct{}), entered: make(chan struct{}, 64)}
}

func (g *fsyncGate) hook(f *os.File) error {
	g.calls.Add(1)
	g.mu.Lock()
	held, rel := g.held, g.release
	g.mu.Unlock()
	if held {
		select {
		case g.entered <- struct{}{}:
		default:
		}
		<-rel
	}
	g.mu.Lock()
	err := g.fail
	g.mu.Unlock()
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	g.syncedSize.Store(fi.Size())
	return nil
}

func (g *fsyncGate) hold() {
	g.mu.Lock()
	g.held = true
	g.release = make(chan struct{})
	g.mu.Unlock()
}

func (g *fsyncGate) unhold() {
	g.mu.Lock()
	if g.held {
		g.held = false
		close(g.release)
	}
	g.mu.Unlock()
}

func (g *fsyncGate) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("fsync never started")
	}
}

func gateNode(n *RaftNode) *fsyncGate {
	g := newFsyncGate()
	n.store.setSyncHook(g.hook)
	return g
}

// asyncAE starts an AppendEntries and returns a channel with its reply.
func asyncAE(n *RaftNode, args AppendEntriesArgs) chan AppendEntriesReply {
	ch := make(chan AppendEntriesReply, 1)
	n.HandleAppendEntriesAsync(args, func(r AppendEntriesReply) { ch <- r })
	return ch
}

func expectNoReply(t *testing.T, ch chan AppendEntriesReply, d time.Duration, what string) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("%s: replied before its fsync completed: %+v", what, r)
	case <-time.After(d):
	}
}

func expectReply(t *testing.T, ch chan AppendEntriesReply, what string) AppendEntriesReply {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: no reply", what)
	}
	return AppendEntriesReply{}
}

func durableOf(n *RaftNode) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.durableIndex
}

// ── follower: ack only after fsync ────────────────────────────────────────────

// TestFollower_NoAckBeforeFsync: an AppendEntries carrying entries is not
// acknowledged until the fsync covering them returns; a failed fsync is
// answered with Success=false, the store then refuses writes, and the
// non-durable suffix is dropped from memory.
func TestFollower_NoAckBeforeFsync(t *testing.T) {
	forEachPipelineMode(t, testFollowerNoAckBeforeFsync)
}

func testFollowerNoAckBeforeFsync(t *testing.T, pipeline bool) {
	t.Run("slow-fsync", func(t *testing.T) {
		n := startFollowerOpts(t, t.TempDir(), Options{Pipeline: pipeline})
		defer n.Stop()
		g := gateNode(n)
		g.hold()
		ch := asyncAE(n, AppendEntriesArgs{Term: 100, LeaderID: "l", Entries: entries(100, 1, 3, "e"), WantMatch: true})
		g.waitEntered(t)
		expectNoReply(t, ch, 150*time.Millisecond, "append 1..3")
		if d := durableOf(n); d != 0 {
			t.Fatalf("durableIndex=%d before the fsync returned", d)
		}
		g.unhold()
		r := expectReply(t, ch, "append 1..3")
		if !r.Success || !r.HasMatch || r.MatchIndex != 3 {
			t.Fatalf("reply after fsync: %+v", r)
		}
	})

	t.Run("failed-fsync", func(t *testing.T) {
		n := startFollowerOpts(t, t.TempDir(), Options{Pipeline: pipeline})
		defer n.Stop()
		g := gateNode(n)
		r := n.HandleAppendEntries(AppendEntriesArgs{Term: 100, LeaderID: "l", Entries: entries(100, 1, 2, "ok"), WantMatch: true})
		if !r.Success || r.MatchIndex != 2 {
			t.Fatalf("durable append: %+v", r)
		}
		g.mu.Lock()
		g.fail = errors.New("injected EIO")
		g.mu.Unlock()
		r = n.HandleAppendEntries(AppendEntriesArgs{Term: 100, LeaderID: "l", PrevLogIndex: 2, PrevLogTerm: 100, Entries: entries(100, 3, 5, "lost"), WantMatch: true})
		if r.Success {
			t.Fatalf("acknowledged entries whose fsync failed: %+v", r)
		}
		sameLog(t, logOf(n), entries(100, 1, 2, "ok"))
		if n.store.failedErr() == nil {
			t.Fatal("store did not become read-only after a failed fsync")
		}
		r = n.HandleAppendEntries(AppendEntriesArgs{Term: 100, LeaderID: "l", PrevLogIndex: 2, PrevLogTerm: 100, Entries: entries(100, 3, 3, "again"), WantMatch: true})
		if r.Success {
			t.Fatalf("append accepted after a failed fsync: %+v", r)
		}
	})
}

// TestFollower_HeartbeatServedDuringFsync: while an fsync is in flight, a
// heartbeat (WantMatch) is answered at once and reports only the durable
// prefix; a RequestVote is served too (rn.mu is not held across the fsync).
// A heartbeat from an older leader (no WantMatch) waits like an append.
func TestFollower_HeartbeatServedDuringFsync(t *testing.T) {
	forEachPipelineMode(t, testFollowerHeartbeatServedDuringFsync)
}

func testFollowerHeartbeatServedDuringFsync(t *testing.T, pipeline bool) {
	n := startFollowerOpts(t, t.TempDir(), Options{Pipeline: pipeline})
	defer n.Stop()
	g := gateNode(n)
	if r := n.HandleAppendEntries(AppendEntriesArgs{Term: 100, LeaderID: "l", Entries: entries(100, 1, 2, "a"), WantMatch: true}); !r.Success {
		t.Fatalf("initial append: %+v", r)
	}
	g.hold()
	pending := asyncAE(n, AppendEntriesArgs{Term: 100, LeaderID: "l", PrevLogIndex: 2, PrevLogTerm: 100, Entries: entries(100, 3, 6, "b"), WantMatch: true})
	g.waitEntered(t)

	start := time.Now()
	hb := asyncAE(n, AppendEntriesArgs{Term: 100, LeaderID: "l", PrevLogIndex: 6, PrevLogTerm: 100, LeaderCommit: 2, WantMatch: true})
	r := expectReply(t, hb, "heartbeat")
	if took := time.Since(start); took > 100*time.Millisecond {
		t.Fatalf("heartbeat waited %v behind an fsync", took)
	}
	if !r.Success || !r.HasMatch || r.MatchIndex != 2 {
		t.Fatalf("heartbeat during fsync: want Success with MatchIndex=2 (durable), got %+v", r)
	}
	if v := n.HandleRequestVote(RequestVoteArgs{Term: 100, CandidateID: "m", LastLogIndex: 6, LastLogTerm: 100}); v.Term != 100 {
		t.Fatalf("RequestVote during fsync: %+v", v)
	}

	legacyHB := asyncAE(n, AppendEntriesArgs{Term: 100, LeaderID: "l", PrevLogIndex: 6, PrevLogTerm: 100})
	expectNoReply(t, pending, 100*time.Millisecond, "append 3..6")
	expectNoReply(t, legacyHB, 10*time.Millisecond, "legacy heartbeat")
	g.unhold()
	if r := expectReply(t, pending, "append 3..6"); !r.Success || r.MatchIndex != 6 {
		t.Fatalf("append after fsync: %+v", r)
	}
	if r := expectReply(t, legacyHB, "legacy heartbeat"); !r.Success {
		t.Fatalf("legacy heartbeat after fsync: %+v", r)
	}
}

// TestFollower_TruncationRacesFsync: a newer leader truncates entries that an
// in-flight fsync covers.  The reply to the old request must not report a
// match (it fails with the newer term), durableIndex must not cover the
// replaced entries when that fsync returns, and the new request is answered
// once ITS entries are durable.
func TestFollower_TruncationRacesFsync(t *testing.T) {
	forEachPipelineMode(t, testFollowerTruncationRacesFsync)
}

func testFollowerTruncationRacesFsync(t *testing.T, pipeline bool) {
	n := startFollowerOpts(t, t.TempDir(), Options{Pipeline: pipeline})
	defer n.Stop()
	g := gateNode(n)
	if r := n.HandleAppendEntries(AppendEntriesArgs{Term: 100, LeaderID: "l", Entries: entries(100, 1, 2, "a"), WantMatch: true}); !r.Success {
		t.Fatalf("initial append: %+v", r)
	}
	g.hold()
	old := asyncAE(n, AppendEntriesArgs{Term: 100, LeaderID: "l", PrevLogIndex: 2, PrevLogTerm: 100, Entries: entries(100, 3, 6, "old"), WantMatch: true})
	g.waitEntered(t) // the fsync covering 3..6 (term 100) is in flight

	// New leader (term 101) keeps 1..3 and replaces 4.. with its own 4..5.
	// Its own write queues behind the held fsync.
	repl := entries(101, 4, 5, "new")
	nw := asyncAE(n, AppendEntriesArgs{Term: 101, LeaderID: "m", PrevLogIndex: 3, PrevLogTerm: 100, Entries: repl, WantMatch: true})

	r := expectReply(t, old, "old-term append")
	if r.Success || r.Term != 101 {
		t.Fatalf("old-term request: want failure with term 101, got %+v", r)
	}
	expectNoReply(t, nw, 50*time.Millisecond, "new-term append")

	g.unhold()
	r = expectReply(t, nw, "new-term append")
	if !r.Success || r.MatchIndex != 5 {
		t.Fatalf("new-term append: %+v", r)
	}
	want := append(append(entries(100, 1, 2, "a"), entries(100, 3, 3, "old")...), repl...)
	sameLog(t, logOf(n), want)
	if d := durableOf(n); d != 5 {
		t.Fatalf("durableIndex=%d, want 5", d)
	}
	n.Stop()
	sameLog(t, diskLog(t, n.dataDir), want)
}

// TestFollower_TruncationDuringFsyncSameIndex: the fsync covering 3..6/term 100
// returns AFTER a newer leader replaced 4..6 (written but not yet synced):
// markDurableLocked must skip the stale (6, 100) report — durableIndex must
// never cover index 4..6 until the new records' own fsync returns.
func TestFollower_TruncationDuringFsyncSameIndex(t *testing.T) {
	forEachPipelineMode(t, testFollowerTruncationDuringFsyncSameIndex)
}

func testFollowerTruncationDuringFsyncSameIndex(t *testing.T, pipeline bool) {
	n := startFollowerOpts(t, t.TempDir(), Options{Pipeline: pipeline})
	defer n.Stop()
	g := gateNode(n)
	g.hold()
	_ = asyncAE(n, AppendEntriesArgs{Term: 100, LeaderID: "l", Entries: entries(100, 1, 6, "old"), WantMatch: true})
	g.waitEntered(t)
	nw := asyncAE(n, AppendEntriesArgs{Term: 101, LeaderID: "m", PrevLogIndex: 3, PrevLogTerm: 100, Entries: entries(101, 4, 7, "new"), WantMatch: true})

	// Release only the first fsync: the second must block again.
	g.mu.Lock()
	rel := g.release
	g.release = make(chan struct{})
	g.mu.Unlock()
	close(rel)
	g.waitEntered(t) // second fsync (covering the new records) in flight
	if d := durableOf(n); d > 3 {
		t.Fatalf("durableIndex=%d covers entries replaced while their fsync ran", d)
	}
	expectNoReply(t, nw, 50*time.Millisecond, "new-term append")
	g.unhold()
	if r := expectReply(t, nw, "new-term append"); !r.Success || r.MatchIndex != 7 {
		t.Fatalf("new-term append: %+v", r)
	}
}

// TestLeader_SingleNodeCommitWaitsForFsync: a leader never counts itself
// toward commit before its own fsync.
func TestLeader_SingleNodeCommitWaitsForFsync(t *testing.T) {
	forEachPipelineMode(t, testLeaderSingleNodeCommitWaitsForFsync)
}

func testLeaderSingleNodeCommitWaitsForFsync(t *testing.T, pipeline bool) {
	n := startSingle(t, "solo", t.TempDir(), &mockSM{}, Options{Pipeline: pipeline})
	defer n.Stop()
	g := gateNode(n)
	g.hold()
	done := make(chan error, 1)
	go func() { done <- n.Submit([]byte("x")) }()
	g.waitEntered(t)
	select {
	case err := <-done:
		t.Fatalf("Submit returned (%v) before the leader's fsync completed", err)
	case <-time.After(150 * time.Millisecond):
	}
	g.unhold()
	if err := <-done; err != nil {
		t.Fatalf("Submit: %v", err)
	}
}

// ── crash after a partial durable state ───────────────────────────────────────

// TestFollower_CrashAfterPartialDurableState: entries 1..3 are fsynced and
// acknowledged; 4..6 are written but their fsync never returns.  A crash
// (modelled by keeping only the bytes the last successful fsync covered)
// restarts with exactly the acknowledged prefix, and the node accepts the
// unacknowledged entries again afterwards.
func TestFollower_CrashAfterPartialDurableState(t *testing.T) {
	dir := t.TempDir()
	n := startFollower(t, dir)
	g := gateNode(n)
	if r := n.HandleAppendEntries(AppendEntriesArgs{Term: 100, LeaderID: "l", Entries: entries(100, 1, 3, "d"), WantMatch: true}); !r.Success || r.MatchIndex != 3 {
		t.Fatalf("durable append: %+v", r)
	}
	g.hold()
	pending := asyncAE(n, AppendEntriesArgs{Term: 100, LeaderID: "l", PrevLogIndex: 3, PrevLogTerm: 100, Entries: entries(100, 4, 6, "v"), WantMatch: true})
	g.waitEntered(t)

	// "Crash": copy the node's files, keeping the log only up to the last
	// successful fsync.
	crash := t.TempDir()
	synced := g.syncedSize.Load()
	logBytes, err := os.ReadFile(filepath.Join(dir, raftLogFileName))
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(logBytes)) <= synced {
		t.Fatalf("expected unsynced bytes in the log (size %d, synced %d)", len(logBytes), synced)
	}
	if err := os.WriteFile(filepath.Join(crash, raftLogFileName), logBytes[:synced], 0644); err != nil {
		t.Fatal(err)
	}
	meta, err := os.ReadFile(filepath.Join(dir, raftMetaFileName))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(crash, raftMetaFileName), meta, 0644); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-pending:
		t.Fatalf("entries 4..6 acknowledged without a durable copy: %+v", r)
	default:
	}
	g.unhold()
	expectReply(t, pending, "append 4..6")
	n.Stop()

	n2 := startFollower(t, crash)
	defer n2.Stop()
	sameLog(t, logOf(n2), entries(100, 1, 3, "d"))
	if r := n2.HandleAppendEntries(AppendEntriesArgs{Term: 100, LeaderID: "l", PrevLogIndex: 3, PrevLogTerm: 100, Entries: entries(100, 4, 6, "v"), WantMatch: true}); !r.Success || r.MatchIndex != 6 {
		t.Fatalf("re-append after restart: %+v", r)
	}
	n2.Stop()
	sameLog(t, diskLog(t, crash), append(entries(100, 1, 3, "d"), entries(100, 4, 6, "v")...))
}

// ── randomised: pipelined leader over a lossy, reordering transport ───────────

// chaosTransport delivers every RPC on its own goroutine after a random delay
// (so requests and replies are reordered), drops requests and replies at
// random (the sender sees an error after a delay, like a timeout), and can
// partition nodes.  It implements AsyncTransport.
type chaosTransport struct {
	mu       sync.Mutex
	nodes    map[string]*RaftNode
	cut      map[string]bool
	rng      *rand.Rand
	dropReq  float64
	dropResp float64
	maxDelay time.Duration
	closed   atomic.Bool
	wg       sync.WaitGroup
}

func (c *chaosTransport) roll() (delay time.Duration, dropReq, dropResp bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Duration(c.rng.Int63n(int64(c.maxDelay))),
		c.rng.Float64() < c.dropReq, c.rng.Float64() < c.dropResp
}

func (c *chaosTransport) target(from, to string) *RaftNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cut[from] || c.cut[to] {
		return nil
	}
	return c.nodes[to]
}

func (c *chaosTransport) setCut(id string, cut bool) {
	c.mu.Lock()
	c.cut[id] = cut
	c.mu.Unlock()
}

type chaosView struct {
	c    *chaosTransport
	self string
}

var errChaos = errors.New("chaos: dropped")

func (v *chaosView) SendAppendEntriesAsync(peer string, args AppendEntriesArgs, done func(AppendEntriesReply, error)) {
	c := v.c
	delay, dReq, dResp := c.roll()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		time.Sleep(delay)
		n := c.target(v.self, peer)
		if n == nil || dReq || c.closed.Load() {
			done(AppendEntriesReply{}, errChaos)
			return
		}
		n.HandleAppendEntriesAsync(args, func(r AppendEntriesReply) {
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				d2, _, _ := c.roll()
				time.Sleep(d2)
				if dResp || c.target(v.self, peer) == nil {
					done(AppendEntriesReply{}, errChaos)
					return
				}
				done(r, nil)
			}()
		})
	}()
}

func (v *chaosView) SendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	type res struct {
		r   AppendEntriesReply
		err error
	}
	ch := make(chan res, 1)
	v.SendAppendEntriesAsync(peer, args, func(r AppendEntriesReply, err error) { ch <- res{r, err} })
	r := <-ch
	return r.r, r.err
}

func (v *chaosView) SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	delay, dReq, dResp := v.c.roll()
	time.Sleep(delay)
	n := v.c.target(v.self, peer)
	if n == nil || dReq || v.c.closed.Load() {
		return RequestVoteReply{}, errChaos
	}
	r := n.HandleRequestVote(args)
	if dResp {
		return RequestVoteReply{}, errChaos
	}
	return r, nil
}

func (v *chaosView) SendInstallSnapshot(peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	n := v.c.target(v.self, peer)
	if n == nil || v.c.closed.Load() {
		return InstallSnapshotReply{}, errChaos
	}
	return n.HandleInstallSnapshot(args), nil
}

func (v *chaosView) Close() error { return nil }

// TestPipeline_ChaosTransport runs a 3-node cluster over chaosTransport for
// many seeds, with the pipeline off (window 1, paced) and on (window 4): concurrent submits, random
// drops and reordering of requests and replies, and a leader partition in the
// middle.  Afterwards: every acknowledged command is applied on every node,
// all nodes applied the same sequence (no divergence, no gaps), logs agree
// through commitIndex, and no node's commitIndex exceeds what a majority
// holds durably.
func TestPipeline_ChaosTransport(t *testing.T) {
	seeds := 12
	if testing.Short() {
		seeds = 3
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		seed := seed
		for _, pipeline := range []bool{false, true} {
			pipeline := pipeline
			t.Run(fmt.Sprintf("pipeline=%v/seed-%d", pipeline, seed), func(t *testing.T) {
				t.Parallel()
				runChaos(t, seed, pipeline)
			})
		}
	}
}

func runChaos(t *testing.T, seed int64, pipeline bool) {
	ct := &chaosTransport{
		nodes: make(map[string]*RaftNode), cut: make(map[string]bool),
		rng: rand.New(rand.NewSource(seed)), dropReq: 0.05, dropResp: 0.05,
		maxDelay: 3 * time.Millisecond,
	}
	ids := []string{fmt.Sprintf("c%d%v-a", seed, pipeline), fmt.Sprintf("c%d%v-b", seed, pipeline), fmt.Sprintf("c%d%v-c", seed, pipeline)}
	nodes := make([]*RaftNode, len(ids))
	for i, id := range ids {
		var peers []string
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		n, err := NewRaftNodeWithOptions(id, peers, t.TempDir(), &mockSM{}, &chaosView{c: ct, self: id},
			Options{Pipeline: pipeline, PipelineWindow: 4})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		ct.mu.Lock()
		ct.nodes[id] = n
		ct.mu.Unlock()
	}
	stopped := false
	stopAll := func() {
		if stopped {
			return
		}
		stopped = true
		ct.closed.Store(true)
		for _, n := range nodes {
			n.Stop()
		}
		ct.wg.Wait()
	}
	defer stopAll()

	if waitForLeader(nodes, 10*time.Second) < 0 {
		t.Fatal("no leader elected")
	}
	var acked sync.Map
	var nAcked atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(2 * time.Second)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; time.Now().Before(deadline); i++ {
				lead := waitForLeader(nodes, 200*time.Millisecond)
				if lead < 0 {
					continue
				}
				cmd := fmt.Sprintf("s%d-w%d-%d", seed, w, i)
				if err := nodes[lead].Submit([]byte(cmd)); err == nil {
					acked.Store(cmd, true)
					nAcked.Add(1)
				}
			}
		}(w)
	}
	// Partition the leader for a while in the middle of the run.
	time.Sleep(500 * time.Millisecond)
	if lead := waitForLeader(nodes, time.Second); lead >= 0 {
		ct.setCut(ids[lead], true)
		time.Sleep(600 * time.Millisecond)
		ct.setCut(ids[lead], false)
	}
	wg.Wait()

	// Quiesce: no drops, let everything catch up.
	ct.mu.Lock()
	ct.dropReq, ct.dropResp = 0, 0
	ct.mu.Unlock()
	if nAcked.Load() == 0 {
		t.Fatal("no command acknowledged")
	}
	var want int
	acked.Range(func(_, _ any) bool { want++; return true })
	settle := time.Now().Add(10 * time.Second)
	for round := 0; ; round++ {
		lead := waitForLeader(nodes, time.Second)
		if lead >= 0 {
			// A new entry of the current term commits everything before it.
			_ = nodes[lead].Submit([]byte(fmt.Sprintf("s%d-final-%d", seed, round)))
		}
		all := true
		for _, n := range nodes {
			got := map[string]bool{}
			for _, c := range smOf(n).all() {
				got[c] = true
			}
			acked.Range(func(k, _ any) bool {
				if !got[k.(string)] {
					all = false
				}
				return all
			})
		}
		if all || time.Now().After(settle) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Commit safety: every node's commitIndex is covered by a majority of
	// durable copies of the same entry.
	for _, n := range nodes {
		n.mu.Lock()
		ci := n.commitIndex
		term := n.logTerm(ci)
		n.mu.Unlock()
		holders := 0
		for _, m := range nodes {
			m.mu.Lock()
			if m.durableIndex >= ci && (m.logTerm(ci) == term || ci <= m.lastIncludedIndex) {
				holders++
			}
			m.mu.Unlock()
		}
		if holders < 2 {
			t.Fatalf("%s: commitIndex %d (term %d) durable on only %d nodes", n.id, ci, term, holders)
		}
	}

	stopAll()

	// State machine safety: identical apply sequences (one is a prefix of the
	// other — a node may lag), every acknowledged command applied everywhere,
	// no command applied twice.
	ref := smOf(nodes[0]).all()
	for _, n := range nodes[1:] {
		got := smOf(n).all()
		short, long := got, ref
		if len(short) > len(long) {
			short, long = long, short
		}
		for i := range short {
			if short[i] != long[i] {
				t.Fatalf("apply order diverges at %d: %q vs %q", i, short[i], long[i])
			}
		}
	}
	for _, n := range nodes {
		seen := map[string]bool{}
		for _, c := range smOf(n).all() {
			if seen[c] {
				t.Fatalf("%s applied %q twice", n.id, c)
			}
			seen[c] = true
		}
		acked.Range(func(k, _ any) bool {
			if !seen[k.(string)] {
				t.Fatalf("%s never applied acknowledged command %q", n.id, k)
			}
			return true
		})
	}
	t.Logf("seed %d: %d acknowledged commands, applied %d", seed, want, len(ref))
}

// ── stream transport ──────────────────────────────────────────────────────────

// slowFirstHandler answers the first AppendEntries only after the second one
// was answered, proving replies are matched by ID, not by order.
type slowFirstHandler struct {
	staticHandler
	mu    sync.Mutex
	first func(AppendEntriesReply)
	order []uint64
}

func (h *slowFirstHandler) HandleAppendEntriesAsync(a AppendEntriesArgs, respond func(AppendEntriesReply)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.order = append(h.order, a.PrevLogIndex)
	if h.first == nil {
		h.first = respond
		return
	}
	respond(AppendEntriesReply{Term: a.Term, Success: true, HasMatch: true, MatchIndex: a.PrevLogIndex})
	h.first(AppendEntriesReply{Term: a.Term, Success: true, HasMatch: true, MatchIndex: 1})
}

func TestTransport_StreamOutOfOrderReplies(t *testing.T) {
	h := &slowFirstHandler{}
	srv, err := NewRPCServer("127.0.0.1:0", h)
	if err != nil {
		t.Fatal(err)
	}
	go srv.ListenAndServe()
	defer srv.Stop()
	tr := NewTCPTransport(map[string]string{"p": srv.Addr()})
	defer tr.Close()

	type res struct {
		r   AppendEntriesReply
		err error
	}
	got := make(chan res, 2)
	tr.SendAppendEntriesAsync("p", AppendEntriesArgs{Term: 3, PrevLogIndex: 1}, func(r AppendEntriesReply, err error) { got <- res{r, err} })
	tr.SendAppendEntriesAsync("p", AppendEntriesArgs{Term: 3, PrevLogIndex: 2}, func(r AppendEntriesReply, err error) { got <- res{r, err} })
	seen := map[uint64]bool{}
	for i := 0; i < 2; i++ {
		select {
		case r := <-got:
			if r.err != nil || !r.r.Success {
				t.Fatalf("reply %d: %+v %v", i, r.r, r.err)
			}
			seen[r.r.MatchIndex] = true
		case <-time.After(5 * time.Second):
			t.Fatal("no reply")
		}
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("replies not matched to requests: %v", seen)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.order) != 2 || h.order[0] != 1 || h.order[1] != 2 {
		t.Fatalf("server saw requests out of send order: %v", h.order)
	}
	// Synchronous calls share the stream.
	if v, err := tr.SendRequestVote("p", RequestVoteArgs{Term: 9}); err != nil || !v.VoteGranted || v.Term != 9 {
		t.Fatalf("RequestVote over stream: %+v %v", v, err)
	}
}

// TestTransport_LegacyServerFallback: a server of an older build closes the
// connection on the stream hello; the client falls back to one-RPC-per-round
// trip framing and still gets answers.
func TestTransport_LegacyServerFallback(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	old := &RPCServer{handler: staticHandler{}, done: make(chan struct{}), activeConns: map[net.Conn]struct{}{}}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				var b [1]byte
				if _, err := io.ReadFull(c, b[:]); err != nil || b[0] > msgInstallSnapshot {
					return // pre-stream server: "unknown message type"
				}
				old.serveLegacy(c, b[0])
			}(c)
		}
	}()
	tr := NewTCPTransport(map[string]string{"p": l.Addr().String()})
	defer tr.Close()
	r, err := tr.SendAppendEntries("p", AppendEntriesArgs{Term: 4})
	if err != nil || !r.Success || r.Term != 4 {
		t.Fatalf("AppendEntries via legacy fallback: %+v %v", r, err)
	}
	done := make(chan AppendEntriesReply, 1)
	tr.SendAppendEntriesAsync("p", AppendEntriesArgs{Term: 5}, func(r AppendEntriesReply, err error) {
		if err != nil {
			t.Errorf("async legacy: %v", err)
		}
		done <- r
	})
	if r := <-done; r.Term != 5 {
		t.Fatalf("async legacy reply: %+v", r)
	}
}
