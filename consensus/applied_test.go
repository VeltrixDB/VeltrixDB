package consensus

// applied_test.go — a state machine that records its applied index with its
// state (applied.go, CLAUDE.md invariant 65).

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ctrSM applies "incr <k>" — NOT idempotent — and records its applied index
// together with its state, as the storage engine does.  The object survives
// a node restart (it models the durable engine).
type ctrSM struct {
	mu        sync.Mutex
	m         map[string]int
	st        AppliedIndexState
	recorded  bool
	noRecord  bool // behaves as an unrecorded (older) state machine
	restores  int
	applies   int
	quiescer  func(func())
	beginRest int
	resets    int
}

func newCtrSM() *ctrSM { return &ctrSM{m: map[string]int{}} }

func (s *ctrSM) Apply(cmd []byte) error {
	parts := strings.Fields(string(cmd))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applies++
	if len(parts) == 2 && parts[0] == "incr" {
		s.m[parts[1]]++
	}
	return nil
}

func (s *ctrSM) get(k string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[k]
}

func (s *ctrSM) DurableAppliedIndex() (AppliedIndexState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.noRecord {
		return AppliedIndexState{}, false
	}
	return s.st, s.recorded
}

func (s *ctrSM) SetAppliedIndex(index, term uint64, durable bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st = AppliedIndexState{Index: index, Term: term}
	s.recorded = true
	return nil
}

func (s *ctrSM) BeginRestore() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.Restoring = true
	s.recorded = true
	s.beginRest++
	return nil
}

func (s *ctrSM) ResetState() error {
	s.mu.Lock()
	s.m = map[string]int{}
	s.resets++
	s.mu.Unlock()
	return nil
}

var _ AppliedIndexStateMachine = (*ctrSM)(nil)

func (s *ctrSM) SetApplyQuiescer(run func(func())) {
	s.mu.Lock()
	s.quiescer = run
	s.mu.Unlock()
}

func (s *ctrSM) CaptureSnapshot(sink SnapshotSink, fence SnapshotFence) error {
	var snap map[string]int
	if _, err := fence(func() {
		s.mu.Lock()
		snap = make(map[string]int, len(s.m))
		for k, v := range s.m {
			snap[k] = v
		}
		s.mu.Unlock()
	}); err != nil {
		return err
	}
	keys := make([]string, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := sink.Write([]byte(k + "\x00" + strconv.Itoa(snap[k]))); err != nil {
			return err
		}
	}
	return nil
}

func (s *ctrSM) RestoreSnapshot(src SnapshotSource, legacy bool) error {
	m := map[string]int{}
	for {
		r, err := src.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		kv := strings.SplitN(string(r), "\x00", 2)
		n, _ := strconv.Atoi(kv[1])
		m[kv[0]] = n
	}
	s.mu.Lock()
	s.m = m
	s.restores++
	s.mu.Unlock()
	return nil
}

type ctrCluster struct {
	t     *testing.T
	ids   []string
	dirs  []string
	sms   []*ctrSM
	nodes []*RaftNode
	hub   *mockTransport
	opts  Options
}

func newCtrCluster(t *testing.T, n int, opts Options) *ctrCluster {
	t.Helper()
	c := &ctrCluster{t: t, hub: newMockTransport(), opts: testOpts(opts)}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("%s-c%d", t.Name(), i))
		c.dirs = append(c.dirs, t.TempDir())
		c.sms = append(c.sms, newCtrSM())
	}
	c.nodes = make([]*RaftNode, n)
	for i := range c.nodes {
		c.start(i)
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			if nd != nil {
				nd.Stop()
			}
		}
	})
	return c
}

func (c *ctrCluster) start(i int) {
	c.t.Helper()
	var peers []string
	for j, id := range c.ids {
		if j != i {
			peers = append(peers, id)
		}
	}
	nd, err := NewRaftNodeWithOptions(c.ids[i], peers, c.dirs[i], c.sms[i], c.hub.forNode(c.ids[i]), c.opts)
	if err != nil {
		c.t.Fatalf("start %s: %v", c.ids[i], err)
	}
	fastSync(nd)
	c.nodes[i] = nd
	c.hub.register(c.ids[i], nd)
}

func (c *ctrCluster) stop(i int) {
	c.nodes[i].Stop()
	c.nodes[i] = nil
}

func (c *ctrCluster) live() []*RaftNode {
	var out []*RaftNode
	for _, n := range c.nodes {
		if n != nil {
			out = append(out, n)
		}
	}
	return out
}

func (c *ctrCluster) incr(n int) {
	c.t.Helper()
	for i := 0; i < n; i++ {
		if err := submitWithRetry(c.live(), []byte("incr ctr"), 10*time.Second); err != nil {
			c.t.Fatal(err)
		}
	}
}

// waitAll waits until every running node's counter is want.
func (c *ctrCluster) waitAll(want int) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ok := true
		for i, n := range c.nodes {
			if n != nil && c.sms[i].get("ctr") != want {
				ok = false
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			var got []int
			for i := range c.nodes {
				got = append(got, c.sms[i].get("ctr"))
			}
			c.t.Fatalf("counters %v, want %d everywhere", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestApplied_RestartDoesNotReapply: with the applied index recorded, a
// restarted node (with or without a snapshot) applies nothing it already
// applied; the same state machine without a record re-applies the log, which
// is the bug this fixes.
func TestApplied_RestartDoesNotReapply(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold uint64
		noRecord  bool
		wantCtr   func(n int) int
	}{
		{"recorded-no-snapshot", 0, false, func(n int) int { return n }},
		{"recorded-snapshot", 8, false, func(n int) int { return n }},
		// No record (a data dir from an older build): the state is reset
		// and the whole log applied once — not replayed on top (2n).
		{"unrecorded-no-snapshot", 0, true, func(n int) int { return n }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const n = 30
			c := newCtrCluster(t, 3, Options{SnapshotThreshold: tc.threshold})
			for _, sm := range c.sms {
				sm.noRecord = tc.noRecord
			}
			c.incr(n)
			c.waitAll(n)
			c.stop(2)
			c.start(2)
			c.incr(1) // the restarted node learns the commit index
			time.Sleep(200 * time.Millisecond)
			if got := c.sms[2].get("ctr"); got != tc.wantCtr(n)+1 {
				t.Fatalf("restarted node counter = %d, want %d", got, tc.wantCtr(n)+1)
			}
			if tc.noRecord {
				if st := c.nodes[2].SnapshotStatus(); st.StartupResets != 1 {
					t.Fatalf("StartupResets = %d, want 1", st.StartupResets)
				}
			} else {
				st := c.nodes[2].SnapshotStatus()
				if st.StartupResets != 0 {
					t.Fatalf("a recorded node was reset (%d)", st.StartupResets)
				}
				if st.StartupRestores != 0 {
					t.Fatalf("restarted node restored its snapshot %d times; its state already held it", st.StartupRestores)
				}
				if tc.threshold > 0 && st.LastIncludedIndex > 0 && st.StartupRestoreSkips != 1 {
					t.Fatalf("StartupRestoreSkips = %d with snapshot idx %d, want 1", st.StartupRestoreSkips, st.LastIncludedIndex)
				}
			}
		})
	}
}

// TestApplied_InterruptedRestoreRedone: a state machine whose record says a
// restore was in progress is restored from the snapshot at startup even
// though its index is past the snapshot, and the record is cleared after.
func TestApplied_InterruptedRestoreRedone(t *testing.T) {
	c := newCtrCluster(t, 3, Options{SnapshotThreshold: 8})
	c.incr(30)
	c.waitAll(30)
	deadline := time.Now().Add(10 * time.Second)
	for c.nodes[2].SnapshotStatus().LastIncludedIndex == 0 {
		if time.Now().After(deadline) {
			t.Fatal("node 2 took no snapshot")
		}
		c.incr(1)
	}
	c.stop(2)
	sm := c.sms[2]
	sm.mu.Lock()
	sm.st.Restoring = true
	sm.m["ctr"] = -1000 // half a restore: garbage
	sm.mu.Unlock()
	c.start(2)
	st := c.nodes[2].SnapshotStatus()
	if st.StartupRestores != 1 || st.StartupRestoreSkips != 0 {
		t.Fatalf("StartupRestores=%d skips=%d, want 1/0: an interrupted restore must be redone", st.StartupRestores, st.StartupRestoreSkips)
	}
	got, ok := sm.DurableAppliedIndex()
	if !ok || got.Restoring || got.Index != st.LastIncludedIndex {
		t.Fatalf("record after the startup restore = %+v ok=%v, want index %d, not restoring", got, ok, st.LastIncludedIndex)
	}
	c.incr(1)
	c.waitAgree()
}

// waitAgree waits until every running node holds the same counter.
func (c *ctrCluster) waitAgree() {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var vals []int
		for i, n := range c.nodes {
			if n != nil {
				vals = append(vals, c.sms[i].get("ctr"))
			}
		}
		same := true
		for _, v := range vals {
			same = same && v == vals[0]
		}
		if same {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("counters %v never agreed", vals)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestApplied_IndexBeyondLocalLog: a follower may apply an entry the leader
// reported committed before its own copy is fsynced; after a crash its log
// is shorter than its recorded applied index.  It must not apply those
// entries again when the leader re-sends them.
func TestApplied_IndexBeyondLocalLog(t *testing.T) {
	c := newCtrCluster(t, 3, Options{})
	c.incr(10)
	c.waitAll(10)
	c.stop(2)
	sm := c.sms[2]
	// Pretend 5 more entries were applied (and recorded) whose log copies
	// were lost: the leader commits 5 INCRs this node never logged.
	c.incr(5)
	li := -1
	for i, n := range c.nodes {
		if n != nil && n.IsLeader() {
			li = i
		}
	}
	if li < 0 {
		t.Fatal("no leader")
	}
	lst := c.nodes[li].SnapshotStatus()
	sm.mu.Lock()
	sm.m["ctr"] = 15
	sm.st = AppliedIndexState{Index: lst.LastApplied, Term: c.nodes[li].Term()}
	sm.mu.Unlock()
	c.start(2)
	if st := c.nodes[2].SnapshotStatus(); st.LastApplied != lst.LastApplied {
		t.Fatalf("restarted node lastApplied = %d, want the recorded %d", st.LastApplied, lst.LastApplied)
	}
	c.incr(1)
	c.waitAll(16)
}

// TestApplied_TermMismatchIgnored: a record whose term contradicts the log
// (a foreign or damaged data dir) is not trusted: the node replays as an
// unrecorded state machine would.
func TestApplied_TermMismatchIgnored(t *testing.T) {
	// (Without a snapshot the state is reset and the log applied once.)
	c := newCtrCluster(t, 3, Options{})
	c.incr(10)
	c.waitAll(10)
	c.stop(2)
	sm := c.sms[2]
	sm.mu.Lock()
	sm.st.Term += 100
	sm.m = map[string]int{}
	sm.mu.Unlock()
	c.start(2)
	if st := c.nodes[2].SnapshotStatus(); st.LastApplied != 0 {
		t.Fatalf("lastApplied = %d after a term mismatch, want 0 (replay)", st.LastApplied)
	}
	c.incr(1)
	c.waitAll(11)
}

// TestApplied_QuiescerInstalled: the node hands the state machine a function
// that runs with Apply quiesced.
func TestApplied_QuiescerInstalled(t *testing.T) {
	c := newCtrCluster(t, 1, Options{})
	c.incr(3)
	sm := c.sms[0]
	sm.mu.Lock()
	q := sm.quiescer
	sm.mu.Unlock()
	if q == nil {
		t.Fatal("no quiescer installed")
	}
	var at AppliedIndexState
	q(func() { at, _ = sm.DurableAppliedIndex() })
	if at.Index != c.nodes[0].SnapshotStatus().LastApplied {
		t.Fatalf("inside the quiescer the record is %d, lastApplied %d", at.Index, c.nodes[0].SnapshotStatus().LastApplied)
	}
}

// TestApplied_EmptyLogKeepsState: a node with no log and no snapshot (first
// start, or a standalone data dir switched to raft) and no record is not
// reset — there is nothing to replay, so nothing can be applied twice.
func TestApplied_EmptyLogKeepsState(t *testing.T) {
	sm := newCtrSM()
	sm.m["preexisting"] = 7
	hub := newMockTransport()
	n, err := NewRaftNodeWithOptions("solo", nil, t.TempDir(), sm, hub.forNode("solo"), testOpts(Options{}))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	if sm.resets != 0 || sm.get("preexisting") != 7 {
		t.Fatalf("resets=%d preexisting=%d: a node with an empty log was reset", sm.resets, sm.get("preexisting"))
	}
}
