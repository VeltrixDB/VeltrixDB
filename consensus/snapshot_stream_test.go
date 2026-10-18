package consensus

// snapshot_stream_test.go — streaming / chunked snapshots (snapshot.go,
// snapshot_file.go): many-chunk transfers, faults (loss, duplication,
// reordering), a leader change and a newer snapshot mid-transfer, heartbeats
// during a slow install, crash injection at every stage, and mixed-version
// (v1.16) compatibility.

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── kvSM: a streaming, "durable" key/value state machine ─────────────────────

// kvSM applies "put <k> <v>" / "del <k>".  Its state survives a node restart
// when the same object is passed to the new node (it models the storage
// engine, which is durable).  RestoreSnapshot clears and refills the map
// record by record, so an interrupted restore leaves a mixed state — as an
// in-place engine restore would.
type kvSM struct {
	mu sync.Mutex
	m  map[string]string

	restoreDelay time.Duration // spread over the restore
	restoreHook  func(n int)   // after every restored record
	captureHook  func()        // inside CaptureSnapshot, before the fence
	restores     atomic.Int32
	legacyBlobs  atomic.Int32     // legacy (v1.16) restores
	applyHook    func(cmd string) // after every Apply
	stateAt      func(idx uint64) map[string]string
}

func newKVSM() *kvSM { return &kvSM{m: map[string]string{}} }

func (s *kvSM) Apply(cmd []byte) error {
	parts := strings.SplitN(string(cmd), " ", 3)
	s.mu.Lock()
	switch parts[0] {
	case "put":
		s.m[parts[1]] = parts[2]
	case "del":
		delete(s.m, parts[1])
	}
	s.mu.Unlock()
	if s.applyHook != nil {
		s.applyHook(string(cmd))
	}
	return nil
}

func (s *kvSM) snapshotMap() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.m))
	for k, v := range s.m {
		out[k] = v
	}
	return out
}

func (s *kvSM) CaptureSnapshot(sink SnapshotSink, fence SnapshotFence) error {
	if s.captureHook != nil {
		s.captureHook()
	}
	var snap map[string]string
	if _, err := fence(func() { snap = s.snapshotMap() }); err != nil {
		return err
	}
	keys := make([]string, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := sink.Write([]byte(k + "\x00" + snap[k])); err != nil {
			return err
		}
	}
	return nil
}

func (s *kvSM) RestoreSnapshot(src SnapshotSource, legacy bool) error {
	s.restores.Add(1)
	if legacy {
		s.legacyBlobs.Add(1)
		blob, err := src.Next()
		if err != nil {
			return err
		}
		var m map[string]string
		if err := gob.NewDecoder(bytes.NewReader(blob)).Decode(&m); err != nil {
			return err
		}
		s.mu.Lock()
		s.m = m
		s.mu.Unlock()
		return nil
	}
	s.mu.Lock()
	s.m = map[string]string{}
	s.mu.Unlock()
	var recs [][]byte
	for {
		r, err := src.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		recs = append(recs, r)
	}
	for i, r := range recs {
		kv := strings.SplitN(string(r), "\x00", 2)
		s.mu.Lock()
		s.m[kv[0]] = kv[1]
		s.mu.Unlock()
		if s.restoreDelay > 0 {
			time.Sleep(s.restoreDelay / time.Duration(len(recs)))
		}
		if s.restoreHook != nil {
			s.restoreHook(i + 1)
		}
	}
	return nil
}

// EncodeLegacySnapshot: the v1.16 format of this SM is a gob map.
func (s *kvSM) EncodeLegacySnapshot(src SnapshotSource, w io.Writer, limit int) error {
	m := map[string]string{}
	n := 0
	for {
		r, err := src.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		n += len(r)
		if n > limit {
			return fmt.Errorf("over %d bytes", limit)
		}
		kv := strings.SplitN(string(r), "\x00", 2)
		m[kv[0]] = kv[1]
	}
	return gob.NewEncoder(w).Encode(m)
}

func sameMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func diffMap(a, b map[string]string) string {
	var d []string
	for k, v := range a {
		if bv, ok := b[k]; !ok {
			d = append(d, "-"+k)
		} else if bv != v {
			d = append(d, "~"+k)
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			d = append(d, "+"+k)
		}
	}
	sort.Strings(d)
	if len(d) > 10 {
		d = append(d[:10], fmt.Sprintf("… (%d more)", len(d)-10))
	}
	return strings.Join(d, " ")
}

// ── cluster helper ──────────────────────────────────────────────────────────

type kvCluster struct {
	t     *testing.T
	ids   []string
	dirs  []string
	nodes []*RaftNode
	sms   []*kvSM
	hub   *mockTransport
	opts  Options
}

func newKVCluster(t *testing.T, n int, opts Options, mutate func(i int, o *Options)) *kvCluster {
	t.Helper()
	c := &kvCluster{t: t, hub: newMockTransport(), opts: opts}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("%s-n%d", t.Name(), i))
		c.dirs = append(c.dirs, t.TempDir())
		c.sms = append(c.sms, newKVSM())
	}
	c.nodes = make([]*RaftNode, n)
	for i := 0; i < n; i++ {
		c.start(i, mutate)
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

func (c *kvCluster) start(i int, mutate func(i int, o *Options)) {
	c.t.Helper()
	var peers []string
	for j, id := range c.ids {
		if j != i {
			peers = append(peers, id)
		}
	}
	o := c.opts
	if mutate != nil {
		mutate(i, &o)
	}
	nd, err := NewRaftNodeWithOptions(c.ids[i], peers, c.dirs[i], c.sms[i], c.hub.forNode(c.ids[i]), o)
	if err != nil {
		c.t.Fatalf("start %s: %v", c.ids[i], err)
	}
	fastSync(nd)
	c.nodes[i] = nd
	c.hub.register(c.ids[i], nd)
}

// fastSync replaces the raft log fsync with a no-op: these tests are about
// snapshots, and a real F_FULLFSYNC per commit makes them minutes long.
func fastSync(n *RaftNode) {
	n.store.setSyncHook(func(*os.File) error { return nil })
}

// submitAllParallel submits cmds from 16 goroutines (group commit batches
// them), failing the test on any error.
func (c *kvCluster) submitAllParallel(cmds []string) {
	c.t.Helper()
	work := make(chan string)
	errs := make(chan error, len(cmds))
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cmd := range work {
				if err := submitWithRetry(c.live(), []byte(cmd), 10*time.Second); err != nil {
					errs <- fmt.Errorf("submit %q: %w", cmd, err)
				}
			}
		}()
	}
	for _, cmd := range cmds {
		work <- cmd
	}
	close(work)
	wg.Wait()
	close(errs)
	for err := range errs {
		c.t.Fatal(err)
	}
}

func (c *kvCluster) live() []*RaftNode {
	var out []*RaftNode
	for i, n := range c.nodes {
		if n != nil && !c.hub.isCut(c.ids[i]) {
			out = append(out, n)
		}
	}
	return out
}

func (c *kvCluster) submit(cmd string) {
	c.t.Helper()
	if err := submitWithRetry(c.live(), []byte(cmd), 10*time.Second); err != nil {
		c.t.Fatalf("submit %q: %v", cmd, err)
	}
}

func (c *kvCluster) leader() int {
	for i, n := range c.nodes {
		if n != nil && n.IsLeader() && !c.hub.isCut(c.ids[i]) {
			return i
		}
	}
	return -1
}

// waitConverged waits until every running node's state equals the leader's
// and returns that state.
func (c *kvCluster) waitConverged(timeout time.Duration) map[string]string {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		li := c.leader()
		if li >= 0 {
			// A fresh entry of the leader's term commits everything before it.
			_ = c.nodes[li].Submit([]byte("put _sync " + fmt.Sprint(time.Now().UnixNano())))
			want := c.sms[li].snapshotMap()
			ok := true
			for i, n := range c.nodes {
				if n == nil || i == li {
					continue
				}
				got := c.sms[i].snapshotMap()
				delete(got, "_sync")
				w := make(map[string]string, len(want))
				for k, v := range want {
					if k != "_sync" {
						w[k] = v
					}
				}
				if !sameMap(got, w) {
					ok = false
					last = fmt.Sprintf("%s differs from leader %s: %s", c.ids[i], c.ids[li], diffMap(w, got))
				}
			}
			if ok {
				return want
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("cluster did not converge within %v: %s", timeout, last)
	return nil
}

func (n *RaftNode) snapIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastIncludedIndex
}

// partitionAndCompact cuts follower fi, writes puts and deletes until the
// leader has compacted past the follower's log, and returns the expected map
// of the writes it made.
func (c *kvCluster) partitionAndCompact(fi, keys, valSize int, tag string) {
	c.t.Helper()
	c.hub.cutNode(c.ids[fi])
	c.writeAndCompact(fi, keys, valSize, tag)
}

// stopAndCompact is partitionAndCompact with the follower stopped instead of
// cut: a stopped node runs no elections, so it comes back at its old term
// instead of disrupting the leader with an inflated one (no pre-vote).
// Restart it with c.start(fi, nil).
func (c *kvCluster) stopAndCompact(fi, keys, valSize int, tag string) {
	c.t.Helper()
	c.nodes[fi].mu.Lock()
	tail := c.nodes[fi].lastLogIndex()
	c.nodes[fi].mu.Unlock()
	c.nodes[fi].Stop()
	c.nodes[fi] = nil
	c.writeAndCompactFrom(tail, keys, valSize, tag)
}

func (c *kvCluster) writeAndCompact(fi, keys, valSize int, tag string) {
	c.t.Helper()
	c.nodes[fi].mu.Lock()
	tail := c.nodes[fi].lastLogIndex()
	c.nodes[fi].mu.Unlock()
	c.writeAndCompactFrom(tail, keys, valSize, tag)
}

func (c *kvCluster) writeAndCompactFrom(tail uint64, keys, valSize int, tag string) {
	c.t.Helper()
	val := strings.Repeat("v", valSize)
	var cmds []string
	for i := 0; i < keys; i++ {
		cmds = append(cmds, fmt.Sprintf("put %s-%04d %s-%d", tag, i, val, i))
	}
	c.submitAllParallel(cmds)
	cmds = nil
	for i := 0; i < keys; i += 3 {
		cmds = append(cmds, fmt.Sprintf("del %s-%04d", tag, i))
	}
	c.submitAllParallel(cmds)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if li := c.leader(); li >= 0 && c.nodes[li].snapIndex() > tail {
			return
		}
		c.submit("put _pad x")
	}
	c.t.Fatalf("leader never compacted past the follower's tail %d", tail)
}

func waitStableLeaderKV(c *kvCluster) int {
	c.t.Helper()
	li := waitForStableLeader(c.live(), 5*time.Second)
	if li < 0 {
		c.t.Fatal("no stable leader")
	}
	return c.leader()
}

// ── tests ───────────────────────────────────────────────────────────────────

// TestSnapshotStream_ManyChunks: a follower that fell behind a compaction is
// caught up by a snapshot far larger than the chunk size (hundreds of
// chunks); its state equals the leader's, deletes included.
func TestSnapshotStream_ManyChunks(t *testing.T) {
	c := newKVCluster(t, 3, testOpts(Options{SnapshotThreshold: 64, SnapshotChunkSize: 512}), nil)
	li := waitStableLeaderKV(c)
	fi := (li + 1) % 3
	c.submit("put before x")
	c.partitionAndCompact(fi, 400, 200, "k")
	c.hub.healNode(c.ids[fi])
	want := c.waitConverged(20 * time.Second)

	var sent uint64
	for _, n := range c.nodes {
		sent += n.snapStats.chunksSent.Load()
	}
	if sent < 100 {
		t.Fatalf("only %d chunks sent; expected a many-chunk transfer", sent)
	}
	if c.nodes[fi].snapStats.installs.Load() == 0 {
		t.Fatal("follower caught up without installing a snapshot")
	}
	if _, ok := want["k-0000"]; ok {
		t.Fatal("deleted key k-0000 present")
	}
	if want["k-0001"] == "" {
		t.Fatal("k-0001 missing")
	}
	t.Logf("snapshot %d bytes, %d chunks of 512 B", c.nodes[c.leader()].snapStats.lastSizeBytes.Load(), sent)
}

// TestSnapshotStream_LossyTransport: chunk loss, duplication and reordering
// (chaosTransport: random drops of requests and replies, random delays, and
// stale duplicate chunks re-delivered later) never corrupt the transfer.
func TestSnapshotStream_LossyTransport(t *testing.T) {
	for seed := int64(1); seed <= 4; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			ct := &chaosTransport{
				nodes: map[string]*RaftNode{}, cut: map[string]bool{},
				rng: rand.New(rand.NewSource(seed)), dropReq: 0.1, dropResp: 0.1,
				maxDelay: 4 * time.Millisecond,
			}
			ids := []string{fmt.Sprintf("L%d-a", seed), fmt.Sprintf("L%d-b", seed), fmt.Sprintf("L%d-c", seed)}
			sms := []*kvSM{newKVSM(), newKVSM(), newKVSM()}
			nodes := make([]*RaftNode, 3)
			for i, id := range ids {
				var peers []string
				for _, p := range ids {
					if p != id {
						peers = append(peers, p)
					}
				}
				n, err := NewRaftNodeWithOptions(id, peers, t.TempDir(), sms[i], &chaosView{c: ct, self: id},
					testOpts(Options{SnapshotThreshold: 64, SnapshotChunkSize: 300}))
				if err != nil {
					t.Fatal(err)
				}
				fastSync(n)
				nodes[i] = n
				ct.mu.Lock()
				ct.nodes[id] = n
				ct.mu.Unlock()
			}
			defer func() {
				ct.closed.Store(true)
				for _, n := range nodes {
					n.Stop()
				}
				ct.wg.Wait()
			}()
			lead := waitForStableLeader(nodes, 10*time.Second)
			if lead < 0 {
				t.Fatal("no leader")
			}
			fi := (lead + 1) % 3
			ct.setCut(ids[fi], true)
			var live []*RaftNode
			for i, n := range nodes {
				if i != fi {
					live = append(live, n)
				}
			}
			var wg sync.WaitGroup
			for w := 0; w < 8; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := w; i < 300; i += 8 {
						if err := submitWithRetry(live, []byte(fmt.Sprintf("put k%03d %s", i, strings.Repeat("x", 100))), 10*time.Second); err != nil {
							t.Error(err)
							return
						}
						if i%4 == 0 {
							_ = submitWithRetry(live, []byte(fmt.Sprintf("del k%03d", i/2)), 10*time.Second)
						}
					}
				}(w)
			}
			wg.Wait()
			ct.setCut(ids[fi], false)
			deadline := time.Now().Add(30 * time.Second)
			for {
				li := waitForLeader(nodes, time.Second)
				if li >= 0 {
					_ = nodes[li].Submit([]byte("put _sync 1"))
					if sameMap(sms[li].snapshotMap(), sms[fi].snapshotMap()) && li != fi {
						break
					}
				}
				if time.Now().After(deadline) {
					t.Fatalf("follower did not converge: %s", diffMap(sms[(fi+1)%3].snapshotMap(), sms[fi].snapshotMap()))
				}
				time.Sleep(50 * time.Millisecond)
			}
			if nodes[fi].snapStats.installs.Load() == 0 {
				t.Fatal("no snapshot installed")
			}
			t.Logf("chunks recv=%d duplicates delivered=%d", nodes[fi].snapStats.chunksRecv.Load(), ct.dupChunks.Load())
		})
	}
}

// slowChunks makes every chunk delivery to peer `to` take d.
func (m *mockTransport) setChunkDelay(d time.Duration) {
	m.mu.Lock()
	m.chunkDelay = d
	m.mu.Unlock()
}

// TestSnapshotStream_LeaderChangeMidTransfer: the leader is partitioned while
// it streams a snapshot; a new leader takes over and catches the follower up
// with its own snapshot.  No corruption, and the follower converges.
func TestSnapshotStream_LeaderChangeMidTransfer(t *testing.T) {
	c := newKVCluster(t, 3, testOpts(Options{SnapshotThreshold: 64, SnapshotChunkSize: 256}), nil)
	li := waitStableLeaderKV(c)
	fi := (li + 1) % 3
	c.partitionAndCompact(fi, 200, 200, "a")
	c.hub.setChunkDelay(10 * time.Millisecond) // ~200 chunks → ~2 s transfer
	c.hub.healNode(c.ids[fi])

	// Wait until the follower is mid-transfer, then cut the leader.
	deadline := time.Now().Add(10 * time.Second)
	for c.nodes[fi].snapStats.chunksRecv.Load() < 20 {
		if time.Now().After(deadline) {
			t.Fatal("transfer never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	old := c.leader()
	c.hub.cutNode(c.ids[old])
	recvAtCut := c.nodes[fi].snapStats.chunksRecv.Load()
	c.hub.setChunkDelay(0)

	deadline = time.Now().Add(15 * time.Second)
	for {
		nl := c.leader()
		if nl >= 0 && nl != old {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no new leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.waitConverged(20 * time.Second)
	if c.nodes[fi].snapStats.installs.Load() == 0 {
		t.Fatal("follower did not install a snapshot")
	}
	c.hub.healNode(c.ids[old])
	c.waitConverged(20 * time.Second)
	t.Logf("chunks received before the leader was cut: %d, total %d", recvAtCut, c.nodes[fi].snapStats.chunksRecv.Load())
}

// TestSnapshotStream_NewerSnapshotMidTransfer: the leader takes a newer
// snapshot (renamed over raft_snapshot.dat) while streaming the older one;
// the transfer finishes from the pinned descriptor and a second transfer
// brings the follower to the newer one.
func TestSnapshotStream_NewerSnapshotMidTransfer(t *testing.T) {
	c := newKVCluster(t, 3, testOpts(Options{SnapshotThreshold: 32, SnapshotChunkSize: 256}), nil)
	li := waitStableLeaderKV(c)
	fi := (li + 1) % 3
	c.partitionAndCompact(fi, 150, 200, "b")
	leader := c.nodes[c.leader()]
	firstSnap := leader.snapIndex()
	capturesBefore := leader.snapStats.captures.Load()
	c.hub.setChunkDelay(5 * time.Millisecond)
	c.hub.healNode(c.ids[fi])

	deadline := time.Now().Add(10 * time.Second)
	for c.nodes[fi].snapStats.chunksRecv.Load() < 10 {
		if time.Now().After(deadline) {
			t.Fatal("transfer never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Write until the leader has taken a newer snapshot mid-transfer.
	for i := 0; leader.snapStats.captures.Load() == capturesBefore; i++ {
		c.submit(fmt.Sprintf("put c-%04d %s", i, strings.Repeat("w", 50)))
		if time.Now().After(deadline) {
			t.Fatal("no newer snapshot taken")
		}
	}
	if leader.snapIndex() <= firstSnap {
		t.Fatalf("newer snapshot %d not past %d", leader.snapIndex(), firstSnap)
	}
	if c.nodes[fi].snapStats.installs.Load() != 0 {
		t.Log("note: first transfer already finished before the newer snapshot")
	}
	c.hub.setChunkDelay(0)
	c.waitConverged(20 * time.Second)
	if got := c.nodes[fi].snapIndex(); got < firstSnap {
		t.Fatalf("follower snapshot index %d < %d", got, firstSnap)
	}
	t.Logf("first snapshot idx=%d, newer idx=%d, follower installs=%d", firstSnap, leader.snapIndex(), c.nodes[fi].snapStats.installs.Load())
}

// TestSnapshotStream_HeartbeatsDuringSlowInstall: a slow transfer followed by
// a 3 s restore on the follower.  Heartbeats keep reaching the follower the
// whole time and no node starts an election (the term never changes).
func TestSnapshotStream_HeartbeatsDuringSlowInstall(t *testing.T) {
	c := newKVCluster(t, 3, testOpts(Options{SnapshotThreshold: 64, SnapshotChunkSize: 1024}), nil)
	li := waitStableLeaderKV(c)
	fi := (li + 1) % 3
	c.stopAndCompact(fi, 200, 100, "h")
	c.sms[fi].restoreDelay = 3 * time.Second
	c.hub.setChunkDelay(20 * time.Millisecond)

	term := c.nodes[c.leader()].Term()
	var hbDuringInstall atomic.Int64
	var follower atomic.Pointer[RaftNode]
	c.hub.setAEHook(func(to string, args AppendEntriesArgs) {
		if f := follower.Load(); f != nil && to == c.ids[fi] && f.installing.Load() > 0 && len(args.Entries) == 0 {
			hbDuringInstall.Add(1)
		}
	})
	c.start(fi, nil)
	follower.Store(c.nodes[fi])

	deadline := time.Now().Add(30 * time.Second)
	for c.nodes[fi].snapStats.installs.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("install never finished")
		}
		for i, n := range c.nodes {
			if got := n.Term(); got != term {
				t.Fatalf("%s moved to term %d (was %d) during the transfer/install — an election happened", c.ids[i], got, term)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.hub.setAEHook(nil)
	for i, n := range c.nodes {
		if got := n.Term(); got != term {
			t.Fatalf("%s at term %d, want %d", c.ids[i], got, term)
		}
	}
	if hb := hbDuringInstall.Load(); hb < 20 {
		t.Fatalf("only %d heartbeats reached the follower during its 3 s restore", hb)
	}
	c.waitConverged(10 * time.Second)
	t.Logf("heartbeats during the restore: %d", hbDuringInstall.Load())
}

// ── crash injection ─────────────────────────────────────────────────────────

// crashImage is a node's on-disk raft dir and its (durable) state machine at
// the moment of a simulated crash.
type crashImage struct {
	stage   string
	dir     string
	state   map[string]string
	applied uint64 // lastApplied when state was read (0 = not recorded)
}

func copyDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0644); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

// snapshotContent decodes a kvSM snapshot file.
func snapshotContent(t *testing.T, path string) (map[string]string, uint64, bool) {
	t.Helper()
	f, m, ok, err := openSnapshotFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return nil, 0, false
	}
	defer f.Close()
	r, _ := newSnapshotReader(f, m, nil)
	out := map[string]string{}
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		kv := strings.SplitN(string(rec), "\x00", 2)
		out[kv[0]] = kv[1]
	}
	return out, m.Index, true
}

// restartImage starts a node alone (no reachable peers) from img and returns
// its state machine's state right after start.
func restartImage(t *testing.T, id string, peers []string, img crashImage) (map[string]string, *RaftNode) {
	t.Helper()
	sm := newKVSM()
	for k, v := range img.state {
		sm.m[k] = v
	}
	hub := newMockTransport()
	n, err := NewRaftNodeWithOptions(id, peers, img.dir, sm, hub.forNode(id), testOpts(Options{SnapshotThreshold: 1 << 30}))
	if err != nil {
		t.Fatalf("restart from %s image: %v", img.stage, err)
	}
	return sm.snapshotMap(), n
}

// TestSnapshotStream_CrashDuringInstall crashes a follower at every stage of
// an install (before the commit rename, after it, after the log rewrite,
// halfway through the state-machine restore, after the restore) and restarts
// it from that exact disk state: its state is either its old state or the
// new snapshot, never a mix.  The live follower then converges.
func TestSnapshotStream_CrashDuringInstall(t *testing.T) {
	c := newKVCluster(t, 3, testOpts(Options{SnapshotThreshold: 64, SnapshotChunkSize: 4096}), nil)
	li := waitStableLeaderKV(c)
	fi := (li + 1) % 3
	for i := 0; i < 40; i++ {
		c.submit(fmt.Sprintf("put old-%02d x%d", i, i))
	}
	c.waitConverged(10 * time.Second)
	follower := c.nodes[fi]
	oldState := c.sms[fi].snapshotMap()
	c.partitionAndCompact(fi, 120, 50, "new")
	// The old keys are deleted on the leader, so a mixed state is visible.
	for i := 0; i < 40; i += 2 {
		c.submit(fmt.Sprintf("del old-%02d", i))
	}
	c.submit("put _bump 1")

	var mu sync.Mutex
	var images []crashImage
	take := func(stage string) {
		img := crashImage{stage: stage, dir: copyDir(t, c.dirs[fi]), state: c.sms[fi].snapshotMap()}
		mu.Lock()
		images = append(images, img)
		mu.Unlock()
	}
	setStageHook(func(rn *RaftNode, stage string) {
		if rn == follower && strings.HasPrefix(stage, "install-") {
			take(stage)
		}
	})
	defer func() { setStageHook(nil) }()
	var total atomic.Int32
	c.sms[fi].restoreHook = func(n int) {
		if n == 60 {
			take("install-restore-partial")
		}
		total.Store(int32(n))
	}
	c.hub.healNode(c.ids[fi])
	c.waitConverged(20 * time.Second)
	setStageHook(nil)
	c.sms[fi].restoreHook = nil

	newState, newIdx, ok := snapshotContent(t, filepath.Join(c.dirs[fi], snapFileName))
	if !ok {
		t.Fatal("follower has no snapshot file")
	}
	stages := map[string]bool{}
	for _, img := range images {
		stages[img.stage] = true
		var peers []string
		for j, id := range c.ids {
			if j != fi {
				peers = append(peers, id)
			}
		}
		got, n := restartImage(t, c.ids[fi]+"-img", peers, img)
		switch {
		case sameMap(got, oldState):
		case sameMap(got, newState):
			if si := n.snapIndex(); si != newIdx {
				t.Errorf("%s: state is the new snapshot but lastIncludedIndex=%d, want %d", img.stage, si, newIdx)
			}
		default:
			t.Errorf("%s: state after restart is neither the old state nor the new snapshot\n  vs old: %s\n  vs new: %s",
				img.stage, diffMap(oldState, got), diffMap(newState, got))
		}
		n.Stop()
	}
	for _, s := range []string{"install-verified", "install-renamed", "install-log-rewritten", "install-restore-partial", "install-restored"} {
		if !stages[s] {
			t.Errorf("no crash image at stage %s", s)
		}
	}
}

// TestSnapshotStream_CrashDuringCapture crashes the leader at each stage of a
// capture (file written but not committed; committed but log not
// compacted) and restarts it from that disk state: it restores either its
// previous snapshot or the new one, and its log covers everything after it.
func TestSnapshotStream_CrashDuringCapture(t *testing.T) {
	c := newKVCluster(t, 1, testOpts(Options{SnapshotThreshold: 40}), nil)
	if waitForLeader(c.nodes, 5*time.Second) < 0 {
		t.Fatal("no leader")
	}
	n := c.nodes[0]
	var mu sync.Mutex
	var images []crashImage
	var prevSnap []map[string]string
	setStageHook(func(rn *RaftNode, stage string) {
		if rn != n || !strings.HasPrefix(stage, "capture-") {
			return
		}
		// The image must be ONE instant: Apply runs concurrently with the
		// capture (the hook holds only snapMu), so the dir copy, the state and
		// lastApplied are taken with Apply quiesced (smMu; lock order snapMu →
		// smMu → rn.mu).  Appends still run (the syncer writes raft_log.dat
		// outside every raft lock), so the copied log may end in a torn record
		// or hold entries past lastApplied — both are what a real crash leaves.
		var img crashImage
		rn.withApplyQuiesced(func() {
			rn.mu.Lock()
			applied := rn.lastApplied
			rn.mu.Unlock()
			img = crashImage{stage: stage, dir: copyDir(t, c.dirs[0]), state: c.sms[0].snapshotMap(), applied: applied}
		})
		mu.Lock()
		images = append(images, img)
		mu.Unlock()
	})
	defer func() { setStageHook(nil) }()
	for i := 0; i < 200; i++ {
		c.submit(fmt.Sprintf("put k%03d v%d", i%50, i))
		if i%7 == 0 {
			c.submit(fmt.Sprintf("del k%03d", (i+3)%50))
		}
		if i%40 == 0 {
			if m, _, ok := snapshotContent(t, filepath.Join(c.dirs[0], snapFileName)); ok {
				prevSnap = append(prevSnap, m)
			}
		}
	}
	setStageHook(nil)
	final := c.sms[0].snapshotMap()
	n.Stop()
	c.nodes[0] = nil
	if len(images) < 4 {
		t.Fatalf("only %d crash images", len(images))
	}
	for _, img := range images {
		// What the crash left durable: the snapshot (if any) and the log
		// records that survive a torn-tail cut.  Every applied entry must be
		// durable (applied ⇒ committed ⇒ in the leader's durable log or
		// covered by its snapshot).  Durable entries past lastApplied are
		// legitimate (fsynced, not yet applied): the restarted single node
		// commits them, so the expected state is the crashed node's applied
		// state plus exactly those entries.
		_, snapIdx, _ := snapshotContent(t, filepath.Join(img.dir, snapFileName))
		tail, lastDurable := durableTail(t, img.dir, img.applied)
		if lastDurable < img.applied && snapIdx < img.applied {
			t.Fatalf("%s: applied index %d is not durable (log ends at %d, snapshot at %d)",
				img.stage, img.applied, lastDurable, snapIdx)
		}
		want := make(map[string]string, len(img.state))
		for k, v := range img.state {
			want[k] = v
		}
		for _, e := range tail {
			applyKV(want, e.Command)
		}
		// Restart alone: the snapshot is restored at once; the retained log
		// tail is replayed as the single node commits it.
		sm := newKVSM()
		for k, v := range img.state {
			sm.m[k] = v
		}
		hub := newMockTransport()
		id := c.ids[0]
		node, err := NewRaftNodeWithOptions(id, nil, img.dir, sm, hub.forNode(id), testOpts(Options{SnapshotThreshold: 1 << 30}))
		if err != nil {
			t.Fatalf("%s: restart: %v", img.stage, err)
		}
		fastSync(node)
		hub.register(id, node)
		snapState, _, ok := snapshotContent(t, filepath.Join(img.dir, snapFileName))
		atStart := sm.snapshotMap()
		if ok && !sameMap(atStart, snapState) {
			t.Errorf("%s: state at restart is not the snapshot on disk: %s", img.stage, diffMap(snapState, atStart))
		}
		if !ok && !sameMap(atStart, img.state) {
			t.Errorf("%s: no snapshot, yet the state changed", img.stage)
		}
		// After replaying its log the node holds exactly what it had applied.
		if waitForLeader([]*RaftNode{node}, 5*time.Second) < 0 {
			t.Fatalf("%s: no leader after restart", img.stage)
		}
		if err := node.Submit([]byte("put _sync 1")); err != nil {
			t.Fatalf("%s: %v", img.stage, err)
		}
		got := sm.snapshotMap()
		delete(got, "_sync")
		if !sameMap(got, want) {
			t.Errorf("%s: state after log replay differs from the crashed node's applied state (applied=%d) + its %d durable unapplied entries: %s",
				img.stage, img.applied, len(tail), diffMap(want, got))
		}
		node.Stop()
	}
	_ = final
	_ = prevSnap
}

// durableTail returns the normal entries with index > after in dir's raft
// log as a restart would load it (torn tail dropped), and the index of the
// last loaded entry.  It reads a private copy, so dir is left untouched.
func durableTail(t *testing.T, dir string, after uint64) ([]LogEntry, uint64) {
	t.Helper()
	s, st, err := openLogStore(copyDir(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	s.close()
	var tail []LogEntry
	var last uint64
	for _, e := range st.entries {
		last = e.Index
		if e.Index > after && e.Type == EntryNormal && len(e.Command) > 0 {
			tail = append(tail, e)
		}
	}
	return tail, last
}

// applyKV applies a kvSM command to m.
func applyKV(m map[string]string, cmd []byte) {
	parts := strings.SplitN(string(cmd), " ", 3)
	switch parts[0] {
	case "put":
		m[parts[1]] = parts[2]
	case "del":
		delete(m, parts[1])
	}
}

// ── mixed versions ──────────────────────────────────────────────────────────

// TestSnapshotStream_LegacyFollowerSingleShot: a v1.16 follower (no chunk
// capability; refuses chunk messages) is caught up with one single-shot
// InstallSnapshot in the old format.
func TestSnapshotStream_LegacyFollowerSingleShot(t *testing.T) {
	var fi int
	c := newKVCluster(t, 3, testOpts(Options{SnapshotThreshold: 64, SnapshotChunkSize: 512}), nil)
	li := waitStableLeaderKV(c)
	fi = (li + 1) % 3
	// Restart the follower as a "v1.16" node.
	c.nodes[fi].Stop()
	c.nodes[fi] = nil
	c.start(fi, func(i int, o *Options) { o.legacyPeer = true })
	c.partitionAndCompact(fi, 200, 100, "l")
	c.hub.healNode(c.ids[fi])
	c.waitConverged(20 * time.Second)
	var legacy, chunks uint64
	for _, n := range c.nodes {
		legacy += n.snapStats.legacySent.Load()
		chunks += n.snapStats.chunksSent.Load()
	}
	if legacy == 0 {
		t.Fatal("no single-shot InstallSnapshot sent to the v1.16 follower")
	}
	if c.sms[fi].legacyBlobs.Load() == 0 {
		t.Fatal("follower did not restore a legacy blob")
	}
	if chunks != 0 {
		t.Fatalf("%d chunks sent to a v1.16 follower", chunks)
	}
}

// TestSnapshotStream_LegacyFollowerTooLarge: a snapshot over the v1.16
// single-shot limit is not sent to a v1.16 follower; the leader logs it and
// keeps running (the rest of the cluster is unaffected).
func TestSnapshotStream_LegacyFollowerTooLarge(t *testing.T) {
	old := legacyMaxSnapshotBytes
	legacyMaxSnapshotBytes = 4 << 10
	t.Cleanup(func() { legacyMaxSnapshotBytes = old }) // after the nodes stop
	c := newKVCluster(t, 3, testOpts(Options{SnapshotThreshold: 64}), func(i int, o *Options) {})
	li := waitStableLeaderKV(c)
	fi := (li + 1) % 3
	c.nodes[fi].Stop()
	c.nodes[fi] = nil
	c.start(fi, func(i int, o *Options) { o.legacyPeer = true })
	c.partitionAndCompact(fi, 200, 100, "t")
	c.hub.healNode(c.ids[fi])
	time.Sleep(2 * time.Second)
	lead := c.nodes[c.leader()]
	lead.mu.Lock()
	refused := lead.legacyRefused[c.ids[fi]]
	lead.mu.Unlock()
	if refused == 0 {
		t.Fatal("leader did not record the refusal")
	}
	if c.nodes[fi].snapStats.installs.Load() != 0 {
		t.Fatal("follower installed a snapshot over the limit")
	}
	c.submit("put still-works 1") // the two-node majority still commits
}

// TestSnapshotStream_LegacyLeaderInstall: a v1.16 leader's single-shot
// InstallSnapshot (old wire format, whole blob) is installed by a new
// follower.
func TestSnapshotStream_LegacyLeaderInstall(t *testing.T) {
	sm := newKVSM()
	sm.m["stale"] = "x"
	hub := newMockTransport()
	n, err := NewRaftNodeWithOptions("f", []string{"old-leader", "other"}, t.TempDir(), sm, hub.forNode("f"),
		testOpts(Options{SnapshotThreshold: 64}))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(map[string]string{"a": "1", "b": "2"})
	reply := n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 5, LeaderID: "old-leader", LastIncludedIndex: 40, LastIncludedTerm: 4,
		Config: Configuration{Servers: []string{"f", "old-leader", "other"}}, ConfigIndex: 0,
		Data: buf.Bytes(),
	})
	if reply.Term != 5 {
		t.Fatalf("reply term %d", reply.Term)
	}
	if got := sm.snapshotMap(); !sameMap(got, map[string]string{"a": "1", "b": "2"}) {
		t.Fatalf("state %v", got)
	}
	if n.snapIndex() != 40 {
		t.Fatalf("lastIncludedIndex %d", n.snapIndex())
	}
	n.mu.Lock()
	applied := n.lastApplied
	n.mu.Unlock()
	if applied != 40 {
		t.Fatalf("lastApplied %d", applied)
	}
}

// TestSnapshotStream_MigrateGob: a raft_snapshot.gob from a pre-streaming
// build is migrated to raft_snapshot.dat and restored.
func TestSnapshotStream_MigrateGob(t *testing.T) {
	dir := t.TempDir()
	var blob bytes.Buffer
	_ = gob.NewEncoder(&blob).Encode(map[string]string{"x": "1"})
	var f bytes.Buffer
	if err := gob.NewEncoder(&f).Encode(legacySnapshotFile{
		LastIncludedIndex: 7, LastIncludedTerm: 2,
		Config: Configuration{Servers: []string{"m"}}, Data: blob.Bytes(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, legacySnapFileName), f.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	sm := newKVSM()
	hub := newMockTransport()
	n, err := NewRaftNodeWithOptions("m", nil, dir, sm, hub.forNode("m"), testOpts(Options{SnapshotThreshold: 64}))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	if !sameMap(sm.snapshotMap(), map[string]string{"x": "1"}) || n.snapIndex() != 7 {
		t.Fatalf("state %v idx %d", sm.snapshotMap(), n.snapIndex())
	}
	if _, err := os.Stat(filepath.Join(dir, legacySnapFileName+legacyMigratedSuffix)); err != nil {
		t.Fatalf("gob not retired: %v", err)
	}
	if _, err := verifySnapshotFile(filepath.Join(dir, snapFileName)); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotFile_CorruptionDetected: every kind of damage to a snapshot
// file is detected by the reader.
func TestSnapshotFile_CorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	w, err := createSnapshotWriter(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		_ = w.Write([]byte(fmt.Sprintf("record-%03d", i)))
	}
	m, err := w.finish(snapshotMeta{Kind: payloadStream, Index: 9, Term: 3, ID: 1, Config: Configuration{Servers: []string{"a"}}})
	if err != nil {
		t.Fatal(err)
	}
	good, _ := os.ReadFile(w.tmp)
	if _, err := verifySnapshotFile(w.tmp); err != nil {
		t.Fatalf("intact file rejected: %v", err)
	}
	for name, mut := range map[string]func([]byte) []byte{
		"header byte":  func(b []byte) []byte { b[20] ^= 1; return b },
		"record byte":  func(b []byte) []byte { b[snapHeaderSize+30] ^= 1; return b },
		"trailer byte": func(b []byte) []byte { b[len(b)-3] ^= 1; return b },
		"truncated":    func(b []byte) []byte { return b[:len(b)-20] },
		"extended":     func(b []byte) []byte { return append(b, 0) },
	} {
		b := mut(append([]byte(nil), good...))
		p := filepath.Join(dir, "bad-"+strings.ReplaceAll(name, " ", "-"))
		_ = os.WriteFile(p, b, 0644)
		if _, err := verifySnapshotFile(p); err == nil {
			t.Errorf("%s: corruption not detected", name)
		}
	}
	_ = m
}

// v116Handler exposes only the RPCs a v1.16 server handles: no chunked
// snapshots (RPCServer closes the connection on msgSnapshotChunk, as v1.16
// did for any unknown message type).
type v116Handler struct{ h *lateHandler }

func (v v116Handler) HandleRequestVote(a RequestVoteArgs) RequestVoteReply {
	return v.h.HandleRequestVote(a)
}
func (v v116Handler) HandleAppendEntries(a AppendEntriesArgs) AppendEntriesReply {
	return v.h.HandleAppendEntries(a)
}
func (v v116Handler) HandleAppendEntriesAsync(a AppendEntriesArgs, respond func(AppendEntriesReply)) {
	v.h.HandleAppendEntriesAsync(a, respond)
}
func (v v116Handler) HandleInstallSnapshot(a InstallSnapshotArgs) InstallSnapshotReply {
	return v.h.HandleInstallSnapshot(a)
}

// TestSnapshotStream_TCPMixedVersion: over the real TCP transport, a
// v1.16-like follower (no capability, server without the chunk RPC) joining
// late is caught up with a single-shot InstallSnapshot, and a current
// follower restarted after a compaction is caught up with chunks; both end
// with the leader's state.  A chunk sent to the old server fails cleanly and
// the stream to it keeps working.
func TestSnapshotStream_TCPMixedVersion(t *testing.T) {
	ids := []string{"tcp-a", "tcp-b", "tcp-c"}
	legacy := map[string]bool{"tcp-c": true}
	handlers := map[string]*lateHandler{}
	servers := map[string]*RPCServer{}
	addrs := map[string]string{}
	newServer := func(id, addr string) {
		var h RPCHandler = handlers[id]
		if legacy[id] {
			h = v116Handler{handlers[id]}
		}
		for try := 0; ; try++ {
			s, err := NewRPCServer(addr, h)
			if err == nil {
				servers[id] = s
				addrs[id] = s.Addr()
				go s.ListenAndServe()
				return
			}
			if try > 100 {
				t.Fatal(err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	for _, id := range ids {
		handlers[id] = &lateHandler{}
		newServer(id, "127.0.0.1:0")
	}
	dirs := map[string]string{}
	sms := map[string]*kvSM{}
	nodes := map[string]*RaftNode{}
	start := func(id string) {
		peers := map[string]string{}
		var pids []string
		for _, p := range ids {
			if p != id {
				peers[p] = addrs[p]
				pids = append(pids, p)
			}
		}
		if dirs[id] == "" {
			dirs[id] = t.TempDir()
			sms[id] = newKVSM()
		}
		n, err := NewRaftNodeWithOptions(id, pids, dirs[id], sms[id], NewTCPTransport(peers),
			testOpts(Options{SnapshotThreshold: 32, SnapshotChunkSize: 1024, legacyPeer: legacy[id]}))
		if err != nil {
			t.Fatal(err)
		}
		fastSync(n)
		handlers[id].set(n)
		nodes[id] = n
	}
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
		for _, n := range nodes {
			n.Stop()
		}
	}()
	start("tcp-a")
	start("tcp-b")
	live := func() []*RaftNode {
		var out []*RaftNode
		for _, n := range nodes {
			out = append(out, n)
		}
		return out
	}
	write := func(tag string, n int) {
		for i := 0; i < n; i++ {
			if err := submitWithRetry(live(), []byte(fmt.Sprintf("put %s-%03d %s", tag, i, strings.Repeat("x", 100))), 10*time.Second); err != nil {
				t.Fatal(err)
			}
		}
	}
	write("a", 120)
	start("tcp-c") // late, empty, v1.16-like
	waitSame := func(want ...string) {
		deadline := time.Now().Add(20 * time.Second)
		for {
			ln := live()
			li := waitForLeader(ln, 2*time.Second)
			if li >= 0 {
				l := ln[li]
				_ = l.Submit([]byte("put _sync x"))
				ok := true
				for _, id := range want {
					if !sameMap(sms[id].snapshotMap(), sms[l.id].snapshotMap()) {
						ok = false
					}
				}
				if ok {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("%v did not converge", want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitSame("tcp-a", "tcp-b", "tcp-c")
	if sms["tcp-c"].legacyBlobs.Load() == 0 {
		t.Fatal("the v1.16-like follower was not caught up with a single-shot snapshot")
	}

	// Restart the current follower of {tcp-a, tcp-b} after a compaction it
	// missed: chunked over TCP from the (current-version) leader.  Stopping
	// the leader instead would let the v1.16-like node win the election.
	ln := live() // map order: index into this one slice only
	li := waitForLeader(ln, 5*time.Second)
	if li < 0 || ln[li].id == "tcp-c" {
		t.Fatalf("unexpected leader before the chunked phase")
	}
	fid := "tcp-a"
	if ln[li].id == "tcp-a" {
		fid = "tcp-b"
	}
	nodes[fid].Stop()
	servers[fid].Stop()
	delete(nodes, fid)
	write("b", 120)
	handlers[fid] = &lateHandler{}
	newServer(fid, addrs[fid])
	start(fid)
	waitSame("tcp-a", "tcp-b", "tcp-c")
	st := nodes[fid].SnapshotStatus()
	if st.Installs == 0 || st.ChunksReceived < 10 {
		t.Fatalf("%s: installs=%d chunks=%d — not a chunked catch-up", fid, st.Installs, st.ChunksReceived)
	}
	var single uint64
	for _, n := range nodes {
		single += n.SnapshotStatus().LegacySent
	}
	if single > 40 {
		t.Fatalf("%d single-shot snapshots sent — the leader is looping on the v1.16 peer", single)
	}

	// A chunk to the old server: an error, and the stream recovers.
	tr := NewTCPTransport(map[string]string{"tcp-c": addrs["tcp-c"]})
	defer tr.Close()
	if _, err := tr.SendInstallSnapshotChunk("tcp-c", InstallSnapshotChunkArgs{Term: 1}); err == nil {
		t.Fatal("a v1.16 server accepted a chunk")
	}
	if _, err := tr.SendRequestVote("tcp-c", RequestVoteArgs{Term: 0, CandidateID: "x"}); err != nil {
		t.Fatalf("stream to the old server did not recover: %v", err)
	}
}
