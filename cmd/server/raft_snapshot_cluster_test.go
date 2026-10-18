package main

// raft_snapshot_cluster_test.go — raft snapshots end to end on real storage
// engines: in-process RaftNodes over the real TCP transport.  These tests
// use only APIs that already existed in v1.16, so they also run (and fail)
// on the pre-streaming code: deleted keys survived a follower's
// InstallSnapshot, TTLs were dropped, and a capture stalled Apply for the
// whole keyspace scan.

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/consensus"
	"github.com/VeltrixDB/veltrixdb/storage"
)

type snapNode struct {
	id      string
	addr    string
	engDir  string
	raftDir string
	engine  *storage.StorageEngine
	fsm     *raftFSM
	node    *consensus.RaftNode
	srv     *consensus.RPCServer
}

type snapTestCluster struct {
	t         *testing.T
	nodes     []*snapNode
	threshold uint64
	mu        sync.Mutex
	// noApplied: start engines without RaftAppliedIndex (pre-fix behaviour).
	noApplied bool
}

// Raft RPC ports for in-process test nodes come from [32000, 32768): below
// the ephemeral range of Linux (32768–60999) and macOS (49152+), and clear of
// tests/jepsen (12000–20000) and tests/integration (20000–32000).  The old
// ":0"-then-close reservation handed out an ephemeral port that any outgoing
// connection could take before the node bound it ("bind: address already in
// use" under load).  A port outside the ephemeral range can only be taken by
// a test that picks it explicitly; this process never hands one out twice,
// and each is checked bindable first.
const (
	snapTestPortLo = 32000
	snapTestPortHi = 32768
)

var (
	snapPortMu   sync.Mutex
	snapPortUsed = map[int]bool{}
)

func reserveAddr(t *testing.T) string {
	t.Helper()
	snapPortMu.Lock()
	defer snapPortMu.Unlock()
	for tries := 0; tries < 1000; tries++ {
		port := snapTestPortLo + rand.Intn(snapTestPortHi-snapTestPortLo)
		if snapPortUsed[port] {
			continue
		}
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		l.Close()
		snapPortUsed[port] = true
		return l.Addr().String()
	}
	t.Fatalf("reserveAddr: no free port in [%d, %d)", snapTestPortLo, snapTestPortHi)
	return ""
}

func snapTestEngine(t testing.TB, dir string) *storage.StorageEngine {
	t.Helper()
	return snapTestEngineApplied(t, dir, true)
}

// snapTestEngineApplied is snapTestEngine with StorageConfig.RaftAppliedIndex
// (what --mode=raft sets) chosen: false = an engine as v1.16 opened it.
func snapTestEngineApplied(t testing.TB, dir string, applied bool) *storage.StorageEngine {
	t.Helper()
	cfg := storage.DefaultStorageConfig()
	cfg.RaftAppliedIndex = applied
	cfg.ScrubEnabled = false
	cfg.BloomFilterShardBits = 1 << 12
	cfg.DataDirPath = dir
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	e, err := storage.NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("storage init: %v", err)
	}
	<-e.ReplayDone
	if _, _, err := e.RebuildSearchIndexes(); err != nil {
		t.Fatalf("search rebuild: %v", err)
	}
	return e
}

func newSnapTestCluster(t *testing.T, n int, threshold uint64) *snapTestCluster {
	t.Helper()
	return newSnapTestClusterApplied(t, n, threshold, true)
}

// newSnapTestClusterApplied: applied=false starts every engine without
// StorageConfig.RaftAppliedIndex (no applied-index marker: a pre-fix build).
func newSnapTestClusterApplied(t *testing.T, n int, threshold uint64, applied bool) *snapTestCluster {
	t.Helper()
	c := &snapTestCluster{t: t, threshold: threshold, noApplied: !applied}
	for i := 0; i < n; i++ {
		root := t.TempDir()
		c.nodes = append(c.nodes, &snapNode{
			id: fmt.Sprintf("s%d", i), addr: reserveAddr(t),
			engDir: filepath.Join(root, "data"), raftDir: filepath.Join(root, "raft"),
		})
	}
	for i := range c.nodes {
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.stop(i)
		}
	})
	return c
}

func (c *snapTestCluster) start(i int) {
	c.t.Helper()
	n := c.nodes[i]
	n.engine = snapTestEngineApplied(c.t, n.engDir, !c.noApplied)
	n.fsm = newRaftFSM(n.engine)
	peers := map[string]string{}
	var pids []string
	for _, p := range c.nodes {
		if p.id != n.id {
			peers[p.id] = p.addr
			pids = append(pids, p.id)
		}
	}
	node, err := consensus.NewRaftNodeWithOptions(n.id, pids, n.raftDir, n.fsm, consensus.NewTCPTransport(peers),
		consensus.Options{SnapshotThreshold: c.threshold})
	if err != nil {
		c.t.Fatalf("raft %s: %v", n.id, err)
	}
	var srv *consensus.RPCServer
	for try := 0; ; try++ {
		srv, err = consensus.NewRPCServer(n.addr, node)
		if err == nil {
			break
		}
		if try > 50 {
			c.t.Fatalf("rpc server %s: %v", n.id, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	go srv.ListenAndServe()
	c.mu.Lock()
	n.node, n.srv = node, srv
	c.mu.Unlock()
}

func (c *snapTestCluster) stop(i int) {
	n := c.nodes[i]
	c.mu.Lock()
	node, srv, e := n.node, n.srv, n.engine
	n.node, n.srv, n.engine = nil, nil, nil
	c.mu.Unlock()
	if srv != nil {
		srv.Stop()
	}
	if node != nil {
		node.Stop()
	}
	if e != nil {
		_ = e.Close()
	}
}

func (c *snapTestCluster) leader() *snapNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		if n.node != nil && n.node.IsLeader() {
			return n
		}
	}
	return nil
}

func (c *snapTestCluster) waitLeader() *snapNode {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if l := c.leader(); l != nil {
			return l
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return nil
}

func (c *snapTestCluster) submit(cmd fsmCmd) {
	c.t.Helper()
	b, err := encodeCmd(cmd)
	if err != nil {
		c.t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		l := c.waitLeader()
		err := l.node.Submit(b)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("submit %+v: %v", cmd.Op, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// submitParallel submits cmds from 16 goroutines (raft group commit batches
// them; order between commands is not needed by the callers).
func (c *snapTestCluster) submitParallel(cmds []fsmCmd) {
	c.t.Helper()
	ch := make(chan fsmCmd)
	var wg sync.WaitGroup
	var firstErr error
	var emu sync.Mutex
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cmd := range ch {
				b, _ := encodeCmd(cmd)
				deadline := time.Now().Add(20 * time.Second)
				for {
					l := c.leader()
					var err error = errors.New("no leader")
					if l != nil {
						err = l.node.Submit(b)
					}
					if err == nil {
						break
					}
					if time.Now().After(deadline) {
						emu.Lock()
						firstErr = err
						emu.Unlock()
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
		}()
	}
	for _, cmd := range cmds {
		ch <- cmd
	}
	close(ch)
	wg.Wait()
	if firstErr != nil {
		c.t.Fatal(firstErr)
	}
}

// kvDump is an engine's whole keyspace: value and remaining TTL (s, -1 =
// immortal) per key.
type kvDump map[string]struct {
	val string
	ttl int32
}

func dumpEngine(e *storage.StorageEngine) kvDump {
	out := kvDump{}
	for _, k := range e.ScanKeys() {
		v, err := e.Get(k)
		if err != nil {
			continue
		}
		out[k] = struct {
			val string
			ttl int32
		}{string(v), e.GetTTLForKey(k)}
	}
	return out
}

// diffDumps describes how b differs from a (TTLs equal within 3 s).
func diffDumps(a, b kvDump) []string { return diffDumpsTol(a, b, 3) }

// diffDumpsTol is diffDumps with a TTL tolerance of tol seconds.
func diffDumpsTol(a, b kvDump, tol float64) []string {
	var d []string
	for k, av := range a {
		bv, ok := b[k]
		switch {
		case !ok:
			d = append(d, "missing "+strconv.Quote(k))
		case av.val != bv.val:
			d = append(d, "value of "+strconv.Quote(k))
		case (av.ttl < 0) != (bv.ttl < 0) || math.Abs(float64(av.ttl-bv.ttl)) > tol:
			d = append(d, fmt.Sprintf("ttl of %q: %d vs %d", k, av.ttl, bv.ttl))
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			d = append(d, "extra "+strconv.Quote(k))
		}
	}
	sort.Strings(d)
	return d
}

func vecFor(i int) []float32 {
	v := make([]float32, 8)
	for j := range v {
		v[j] = float32(math.Sin(float64(i*8+j+1))) + 0.01*float32(j)
	}
	return v
}

func hasSnapshotFile(dir string) bool {
	m, _ := filepath.Glob(filepath.Join(dir, "raft_snapshot.*"))
	for _, p := range m {
		if !strings.HasSuffix(p, ".tmp") && !strings.HasSuffix(p, ".migrated") {
			return true
		}
	}
	return false
}

// TestRaftSnapshot_FollowerCatchUpWithDeletes: a follower is down while the
// leader deletes keys (including records with vectors and text documents),
// overwrites others, writes TTL keys and compacts its log.  The follower
// restarts and can only catch up through InstallSnapshot.  Afterwards its
// keyspace equals the leader's key for key (deleted keys gone, TTLs kept,
// derived "@vec/" / "@txt/" / "@idx/" keys identical) and its search indexes
// answer like the leader's.
//
// On v1.16 this failed: Restore only Put the snapshot's pairs, so every key
// deleted while the follower was down survived on it, and every key came
// back without its TTL.
func TestRaftSnapshot_FollowerCatchUpWithDeletes(t *testing.T) {
	c := newSnapTestCluster(t, 3, 32)
	c.waitLeader()

	var cmds []fsmCmd
	for i := 0; i < 120; i++ {
		cmds = append(cmds, fsmCmd{Op: opPut, Key: fmt.Sprintf("k%03d", i), Value: []byte(fmt.Sprintf(`{"color":"c%d","n":%d}`, i%5, i)), TTL: -1})
	}
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("doc%02d", i)
		cmds = append(cmds,
			fsmCmd{Op: opPut, Key: id, Value: []byte(fmt.Sprintf(`{"color":"d%d"}`, i%3)), TTL: -1},
			fsmCmd{Op: opPut, Key: storage.TextPersistKey("notes", id), Value: []byte(fmt.Sprintf("alpha bravo %s charlie", id)), TTL: -1})
	}
	c.submitParallel(cmds)
	c.submit(fsmCmd{Op: opIdxCreate, Key: "bycolor", Field: "color"})
	for i := 0; i < 12; i++ {
		c.submit(fsmCmd{Op: opVSet, Ns: "emb", Key: fmt.Sprintf("doc%02d", i), Vec: vecFor(i)})
	}

	// Let the followers apply, then take one down.
	l := c.waitLeader()
	fi := 0
	for i, n := range c.nodes {
		if n != l {
			fi = i
			break
		}
	}
	waitEnginesEqual(t, c, 15*time.Second)
	c.stop(fi)

	cmds = nil
	for i := 0; i < 120; i += 2 {
		cmds = append(cmds, fsmCmd{Op: opDelete, Key: fmt.Sprintf("k%03d", i)})
	}
	for i := 0; i < 5; i++ { // records with a vector and a text document
		cmds = append(cmds, fsmCmd{Op: opDelete, Key: fmt.Sprintf("doc%02d", i)})
	}
	for i := 1; i < 120; i += 4 {
		cmds = append(cmds, fsmCmd{Op: opPut, Key: fmt.Sprintf("k%03d", i), Value: []byte(fmt.Sprintf(`{"color":"z%d","v":2}`, i%2)), TTL: -1})
	}
	for i := 0; i < 30; i++ {
		cmds = append(cmds, fsmCmd{Op: opPut, Key: fmt.Sprintf("ttl%02d", i), Value: []byte("expiring"), TTL: 3600})
	}
	c.submitParallel(cmds)
	c.submit(fsmCmd{Op: opIncr, Key: "counter", Delta: 7, TTL: -1})
	c.submit(fsmCmd{Op: opHSet, Key: "h1", Field: "f1", Value: []byte("x"), TTL: 1800})
	// Enough further entries for the leader to compact past everything the
	// follower has.
	cmds = nil
	for i := 0; i < 100; i++ {
		cmds = append(cmds, fsmCmd{Op: opPut, Key: "pad", Value: []byte(strconv.Itoa(i)), TTL: -1})
	}
	c.submitParallel(cmds)
	l = c.waitLeader()
	deadline := time.Now().Add(10 * time.Second)
	for !hasSnapshotFile(l.raftDir) {
		if time.Now().After(deadline) {
			t.Fatal("leader never took a snapshot")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // the compaction after the last batch

	c.start(fi)
	waitEnginesEqual(t, c, 30*time.Second)
	if !hasSnapshotFile(c.nodes[fi].raftDir) {
		t.Fatal("the follower caught up without a snapshot — the test did not exercise InstallSnapshot")
	}

	f := c.nodes[fi].engine
	if _, err := f.Get("k000"); err == nil {
		t.Fatal("k000 was deleted on the leader but exists on the follower")
	}
	if ttl := f.GetTTLForKey("ttl00"); ttl <= 0 || ttl > 3600 {
		t.Fatalf("ttl00 TTL on the follower = %d, want (0, 3600]", ttl)
	}
	// Search answers like the leader: deleted records' vectors are gone.
	lead := c.waitLeader().engine
	for _, e := range []*storage.StorageEngine{lead, f} {
		res, err := e.SearchVector("emb", vecFor(7), 3)
		if err != nil {
			t.Fatalf("vector search: %v", err)
		}
		if len(res) == 0 || res[0].ID != "doc07" {
			t.Fatalf("vector search for doc07 returned %+v", res)
		}
		all, _ := e.SearchVector("emb", vecFor(1), 12)
		for _, m := range all {
			if m.ID < "doc05" {
				t.Fatalf("deleted record %s still has a searchable vector", m.ID)
			}
		}
		txt, err := e.SearchText("notes", "bravo", 20, nil)
		if err != nil {
			t.Fatalf("text search: %v", err)
		}
		if len(txt) != 7 {
			t.Fatalf("text search returned %d docs, want 7", len(txt))
		}
	}
	// The secondary index keeps being maintained on the follower: a new
	// record written after the install produces the same @idx/ entries.
	c.submit(fsmCmd{Op: opPut, Key: "after", Value: []byte(`{"color":"new"}`), TTL: -1})
	waitEnginesEqual(t, c, 15*time.Second)
	if _, err := f.Get("@idx/bycolor/new/after"); err != nil {
		t.Fatalf("secondary index not maintained on the follower after the install: %v", err)
	}
}

// waitEnginesEqual waits until every running node's keyspace equals the
// leader's.
func waitEnginesEqual(t *testing.T, c *snapTestCluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last []string
	for time.Now().Before(deadline) {
		l := c.leader()
		if l != nil {
			ok := true
			want := dumpEngine(l.engine)
			c.mu.Lock()
			nodes := append([]*snapNode(nil), c.nodes...)
			c.mu.Unlock()
			for _, n := range nodes {
				if n == l || n.engine == nil {
					continue
				}
				if d := diffDumps(want, dumpEngine(n.engine)); len(d) > 0 {
					ok = false
					last = append([]string{n.id + ":"}, d...)
				}
			}
			if ok {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(last) > 12 {
		last = append(last[:12], fmt.Sprintf("… %d more", len(last)-12))
	}
	t.Fatalf("follower keyspace differs from the leader's: %s", strings.Join(last, "; "))
}

// TestRaftSnapshot_ApplyNotStalledByCapture: a single raft node on an engine
// already holding many keys keeps committing writes while snapshots of the
// whole keyspace are captured.  v1.16 held the state-machine lock for the
// whole scan, so every Submit issued during a capture waited for it (≈ the
// scan time).  The bound is far below the scan time.
//
// VELTRIX_SNAPSHOT_STALL_KEYS sets the preloaded key count (default 200k;
// the 1M-key measurement in the report used 1000000).
func TestRaftSnapshot_ApplyNotStalledByCapture(t *testing.T) {
	keys := 200_000
	if v := os.Getenv("VELTRIX_SNAPSHOT_STALL_KEYS"); v != "" {
		keys, _ = strconv.Atoi(v)
	}
	if testing.Short() {
		keys = 50_000
	}
	root := t.TempDir()
	e := snapTestEngine(t, filepath.Join(root, "data"))
	defer e.Close()
	val := bytes.Repeat([]byte("v"), 100)
	t0 := time.Now()
	for i := 0; i < keys; i += 1000 {
		reqs := make([]storage.MultiPutRequest, 0, 1000)
		for j := i; j < i+1000 && j < keys; j++ {
			reqs = append(reqs, storage.MultiPutRequest{Key: fmt.Sprintf("pre%08d", j), Value: val, TTL: -1})
		}
		for _, err := range e.MultiPut(reqs) {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("preloaded %d keys in %s", keys, time.Since(t0).Round(time.Millisecond))

	fsm := newRaftFSM(e)
	raftDir := filepath.Join(root, "raft")
	node, err := consensus.NewRaftNodeWithOptions("solo", nil, raftDir, fsm, consensus.NewTCPTransport(nil),
		consensus.Options{SnapshotThreshold: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()
	deadline := time.Now().Add(10 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Write for a fixed time; snapshots trigger every 64 entries.
	start := time.Now()
	var lats []time.Duration
	for i := 0; time.Since(start) < 6*time.Second; i++ {
		b, _ := encodeCmd(fsmCmd{Op: opPut, Key: fmt.Sprintf("w%06d", i), Value: val, TTL: -1})
		s := time.Now()
		if err := node.Submit(b); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		lats = append(lats, time.Since(s))
	}
	fi, err := os.Stat(filepath.Join(raftDir, "raft_snapshot.dat"))
	if err != nil {
		fi, err = os.Stat(filepath.Join(raftDir, "raft_snapshot.gob"))
	}
	if err != nil || fi.ModTime().Before(start) {
		t.Fatalf("no snapshot was captured while writing (err=%v)", err)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	mx := lats[len(lats)-1]
	p99 := lats[len(lats)*99/100]
	t.Logf("%d writes during snapshots of %d keys: p50=%v p99=%v max=%v", len(lats), keys, lats[len(lats)/2], p99, mx)
	const bound = 250 * time.Millisecond
	if mx > bound {
		t.Fatalf("a write waited %v while a snapshot was captured (bound %v): Apply is stalled by the capture", mx, bound)
	}
}

// TestRaftSnapshot_Bench measures, on a 3-node in-process cluster over TCP:
// the time and size of a snapshot of a large keyspace, the slowest write
// while it is captured, and how long a follower that missed it takes to
// catch up.  Env-gated (it preloads the engines with
// VELTRIX_SNAPSHOT_BENCH_KEYS keys of VELTRIX_SNAPSHOT_BENCH_VALUE bytes):
//
//	VELTRIX_SNAPSHOT_BENCH=1 VELTRIX_SNAPSHOT_BENCH_KEYS=1000000 \
//	  go test ./cmd/server -run TestRaftSnapshot_Bench -v -timeout 30m
//
// Uses only APIs that existed in v1.16, so the same file runs on the old
// code for the before/after numbers.
func TestRaftSnapshot_Bench(t *testing.T) {
	if os.Getenv("VELTRIX_SNAPSHOT_BENCH") != "1" {
		t.Skip("set VELTRIX_SNAPSHOT_BENCH=1")
	}
	keys, valSize := 1_000_000, 100
	if v := os.Getenv("VELTRIX_SNAPSHOT_BENCH_KEYS"); v != "" {
		keys, _ = strconv.Atoi(v)
	}
	if v := os.Getenv("VELTRIX_SNAPSHOT_BENCH_VALUE"); v != "" {
		valSize, _ = strconv.Atoi(v)
	}
	catchUpLimit := 10 * time.Minute
	if v := os.Getenv("VELTRIX_SNAPSHOT_BENCH_CATCHUP_LIMIT_S"); v != "" {
		s, _ := strconv.Atoi(v)
		catchUpLimit = time.Duration(s) * time.Second
	}
	runSnapshotBench(t, keys, valSize, catchUpLimit)
}

// TestRaftSnapshot_LargeSnapshot: a ~315 MB snapshot (300,000 keys x 1 KB) —
// past v1.16's 256 MB cap, where compaction was skipped and the log grew
// without bound — is captured, streamed to a follower in 2 MB chunks and
// restored; the follower then matches the leader key for key.  ~11 s on a
// laptop; skipped with -short and under -race.
func TestRaftSnapshot_LargeSnapshot(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("~315 MB of data per node: skipped with -short and under -race")
	}
	size := runSnapshotBench(t, 300_000, 1024, 2*time.Minute)
	if size <= 256<<20 {
		t.Fatalf("snapshot is %d bytes — not past the old 256 MB cap", size)
	}
}

// runSnapshotBench is TestRaftSnapshot_Bench's body; it returns the size of
// the snapshot file.
func runSnapshotBench(t *testing.T, keys, valSize int, catchUpLimit time.Duration) int64 {
	const threshold = 512
	c := newSnapTestCluster(t, 3, threshold)
	l := c.waitLeader()
	var f, o *snapNode
	fi := -1
	for i, n := range c.nodes {
		if n == l {
			continue
		}
		if f == nil {
			f, fi = n, i
		} else {
			o = n
		}
	}
	c.stop(fi)

	// Preload the leader and the other follower identically (outside raft:
	// the dataset a long-running cluster would hold).
	val := bytes.Repeat([]byte("v"), valSize)
	t0 := time.Now()
	for _, e := range []*storage.StorageEngine{l.engine, o.engine} {
		for i := 0; i < keys; i += 1000 {
			reqs := make([]storage.MultiPutRequest, 0, 1000)
			for j := i; j < i+1000 && j < keys; j++ {
				reqs = append(reqs, storage.MultiPutRequest{Key: fmt.Sprintf("bench%09d", j), Value: val, TTL: -1})
			}
			for _, err := range e.MultiPut(reqs) {
				if err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	t.Logf("preloaded %d keys x %d B on two nodes in %v", keys, valSize, time.Since(t0).Round(time.Millisecond))

	snapFile := func(dir string) (time.Time, int64) {
		for _, n := range []string{"raft_snapshot.dat", "raft_snapshot.gob"} {
			if fi, err := os.Stat(filepath.Join(dir, n)); err == nil {
				return fi.ModTime(), fi.Size()
			}
		}
		return time.Time{}, 0
	}
	before, _ := snapFile(l.raftDir)

	// Write until the leader's next snapshot file appears; time every write.
	var lats []time.Duration
	var trigger time.Time
	var snapAt time.Time
	var snapSize int64
	deadline := time.Now().Add(15 * time.Minute)
	for i := 0; ; i++ {
		b, _ := encodeCmd(fsmCmd{Op: opPut, Key: fmt.Sprintf("w%06d", i), Value: val, TTL: -1})
		s := time.Now()
		if err := l.node.Submit(b); err != nil {
			t.Fatalf("submit: %v", err)
		}
		d := time.Since(s)
		if i == threshold-8 {
			trigger = time.Now()
		}
		if !trigger.IsZero() {
			lats = append(lats, d)
		}
		if m, sz := snapFile(l.raftDir); m.After(before) {
			snapAt, snapSize = m, sz
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no snapshot")
		}
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	t.Logf("snapshot of %d keys: file %d bytes, ready %v after the log reached the threshold; %d writes meanwhile: p50=%v max=%v",
		keys, snapSize, snapAt.Sub(trigger).Round(time.Millisecond), len(lats), lats[len(lats)/2], lats[len(lats)-1])

	// Catch the stopped follower up.
	t1 := time.Now()
	c.start(fi)
	want := len(l.engine.ScanKeys())
	for {
		if n := len(c.nodes[fi].engine.ScanKeys()); n >= want {
			break
		}
		if time.Since(t1) > catchUpLimit {
			t.Fatalf("follower NOT caught up after %v (%d of %d keys)", catchUpLimit, len(c.nodes[fi].engine.ScanKeys()), want)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("follower caught up (%d keys) in %v", want, time.Since(t1).Round(time.Millisecond))
	waitEnginesEqual(t, c, 5*time.Minute)
	t.Logf("follower keyspace equals the leader's (checked in %v)", time.Since(t1).Round(time.Millisecond))
	return snapSize
}
