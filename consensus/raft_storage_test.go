package consensus

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

// startSingle starts a one-server node in dir and waits until it leads.
func startSingle(t *testing.T, id, dir string, sm StateMachine, opts Options) *RaftNode {
	t.Helper()
	hub := newMockTransport()
	n, err := NewRaftNodeWithOptions(id, nil, dir, sm, hub.forNode(id), opts)
	if err != nil {
		t.Fatalf("NewRaftNodeWithOptions(%s): %v", id, err)
	}
	hub.register(id, n)
	if waitForLeader([]*RaftNode{n}, 3*time.Second) < 0 {
		n.Stop()
		t.Fatal("single node did not elect itself")
	}
	return n
}

// startFollower starts a node whose only peer is never reachable, so it stays
// a follower; tests drive it with HandleAppendEntries directly.  Terms used
// by the tests (>= 100) are far above anything its own election timer reaches.
func startFollower(t *testing.T, dir string) *RaftNode {
	t.Helper()
	return startFollowerOpts(t, dir, testOpts(Options{}))
}

// startFollowerOpts is startFollower with explicit Options.
func startFollowerOpts(t *testing.T, dir string, opts Options) *RaftNode {
	t.Helper()
	hub := newMockTransport()
	n, err := NewRaftNodeWithOptions("f", []string{"l", "m"}, dir, &mockSM{}, hub.forNode("f"), opts)
	if err != nil {
		t.Fatalf("NewRaftNodeWithOptions(f): %v", err)
	}
	return n
}

// waitApplied waits until sm has applied want commands.
func waitCount(t *testing.T, count func() int, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && count() < want {
		time.Sleep(5 * time.Millisecond)
	}
	if got := count(); got != want {
		t.Fatalf("applied: want %d, got %d", want, got)
	}
}

func assertApplied(t *testing.T, sm *mockSM, want []string) {
	t.Helper()
	waitCount(t, sm.count, len(want))
	got := sm.all()
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("applied[%d]: want %q, got %q (all=%v)", i, want[i], got[i], got)
		}
	}
}

func logOf(n *RaftNode) []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]LogEntry(nil), n.ps.Log...)
}

func entries(term, from, to uint64, tag string) []LogEntry {
	var out []LogEntry
	for i := from; i <= to; i++ {
		out = append(out, LogEntry{Term: term, Index: i, Command: []byte(fmt.Sprintf("%s-%d", tag, i))})
	}
	return out
}

func sameLog(t *testing.T, got, want []LogEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("log length: want %d, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Index != w.Index || g.Term != w.Term || g.Type != w.Type || !bytes.Equal(g.Command, w.Command) {
			t.Fatalf("log[%d]: want %+v, got %+v", i, w, g)
		}
	}
}

// diskLog replays raft_log.dat without opening a node (read-only).
func diskLog(t *testing.T, dir string) []LogEntry {
	t.Helper()
	s := &logStore{dir: dir}
	es, _, _, err := s.replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return es
}

func submitAll(t *testing.T, n *RaftNode, cmds []string) {
	t.Helper()
	for _, c := range cmds {
		if err := n.Submit([]byte(c)); err != nil {
			t.Fatalf("submit %q: %v", c, err)
		}
	}
}

func cmdList(tag string, from, to int) []string {
	var out []string
	for i := from; i < to; i++ {
		out = append(out, fmt.Sprintf("%s-%03d", tag, i))
	}
	return out
}

// ── Crash recovery ────────────────────────────────────────────────────────────

// TestRaftLog_TornTailTruncatedOnLoad: a crash mid-append leaves a partial
// record (or one whose CRC does not match).  Restart must keep every complete
// record, drop the torn one, and physically truncate it so that records
// appended after the restart survive the NEXT restart (a log that kept the
// garbage would stop replay there and lose them).
func TestRaftLog_TornTailTruncatedOnLoad(t *testing.T) {
	for _, mode := range []string{"partial-record", "bad-crc", "garbage-length"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			id := "torn-" + mode
			n := startSingle(t, id, dir, &mockSM{}, testOpts(Options{}))
			first := cmdList("a", 0, 10)
			submitAll(t, n, first)
			n.Stop()

			path := filepath.Join(dir, raftLogFileName)
			before, _ := os.Stat(path)
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				t.Fatal(err)
			}
			rec := encodeRecords(nil, []LogEntry{{Term: 99, Index: 999, Command: bytes.Repeat([]byte("x"), 200)}})
			switch mode {
			case "partial-record":
				rec = rec[:len(rec)/2]
			case "bad-crc":
				rec[len(rec)-1] ^= 0xFF
			case "garbage-length":
				binary.LittleEndian.PutUint32(rec[0:4], 0xFFFFFFF0)
			}
			if _, err := f.Write(rec); err != nil {
				t.Fatal(err)
			}
			f.Close()

			sm := &mockSM{}
			n2 := startSingle(t, id, dir, sm, testOpts(Options{}))
			assertApplied(t, sm, first)
			if fi, _ := os.Stat(path); fi.Size() < before.Size() {
				t.Fatalf("log shrank below the last good record: %d < %d", fi.Size(), before.Size())
			}
			second := cmdList("b", 0, 5)
			submitAll(t, n2, second)
			n2.Stop()

			sm3 := &mockSM{}
			n3 := startSingle(t, id, dir, sm3, testOpts(Options{}))
			defer n3.Stop()
			assertApplied(t, sm3, append(append([]string(nil), first...), second...))
		})
	}
}

// TestRaftLog_ConflictTruncationThenRestart: a follower overwrites a
// conflicting suffix; after a restart the log is the new one (the old suffix
// must not resurrect).  Also: a crash that tears the first record of the
// overwrite leaves the pre-AppendEntries log, which the follower never
// acknowledged as replaced — a valid state.
func TestRaftLog_ConflictTruncationThenRestart(t *testing.T) {
	dir := t.TempDir()
	n := startFollower(t, dir)
	r := n.HandleAppendEntries(AppendEntriesArgs{Term: 100, LeaderID: "l", Entries: entries(100, 1, 5, "old")})
	if !r.Success {
		t.Fatalf("append 1..5 rejected: %+v", r)
	}
	// New leader in term 101 keeps 1..2 and replaces 3.. with a shorter suffix.
	newSuffix := entries(101, 3, 4, "new")
	r = n.HandleAppendEntries(AppendEntriesArgs{Term: 101, LeaderID: "m", PrevLogIndex: 2, PrevLogTerm: 100, Entries: newSuffix})
	if !r.Success {
		t.Fatalf("conflicting append rejected: %+v", r)
	}
	want := append(entries(100, 1, 2, "old"), newSuffix...)
	sameLog(t, logOf(n), want)
	n.Stop()

	sameLog(t, diskLog(t, dir), want)
	n2 := startFollower(t, dir)
	sameLog(t, logOf(n2), want)
	if term := n2.Term(); term < 101 {
		t.Fatalf("term after restart: want >= 101, got %d", term)
	}
	// Further appends after the restart land after the overwrite.
	r = n2.HandleAppendEntries(AppendEntriesArgs{Term: 101, LeaderID: "m", PrevLogIndex: 4, PrevLogTerm: 101, Entries: entries(101, 5, 6, "new")})
	if !r.Success {
		t.Fatalf("append after restart rejected: %+v", r)
	}
	want = append(want, entries(101, 5, 6, "new")...)
	n2.Stop()
	sameLog(t, diskLog(t, dir), want)

	// Torn overwrite: the first record of a conflicting suffix is cut short.
	path := filepath.Join(dir, raftLogFileName)
	rec := encodeRecords(nil, entries(102, 2, 2, "torn"))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(rec[:len(rec)-3])
	f.Close()
	n3 := startFollower(t, dir)
	defer n3.Stop()
	sameLog(t, logOf(n3), want)
}

// TestRaftLog_CrashBetweenAppendAndMeta covers the two orders in which a
// crash can split the log file from the meta file.
func TestRaftLog_CrashBetweenAppendAndMeta(t *testing.T) {
	t.Run("meta-ahead-of-log", func(t *testing.T) {
		// Term/vote persisted (e.g. a vote granted), then crash before any
		// entry of that term arrives: restart keeps both, log unchanged.
		dir := t.TempDir()
		n := startFollower(t, dir)
		n.HandleAppendEntries(AppendEntriesArgs{Term: 100, LeaderID: "l", Entries: entries(100, 1, 3, "e")})
		v := n.HandleRequestVote(RequestVoteArgs{Term: 200, CandidateID: "m", LastLogIndex: 3, LastLogTerm: 100})
		if !v.VoteGranted {
			t.Fatalf("vote not granted: %+v", v)
		}
		n.Stop()
		n2 := startFollower(t, dir)
		defer n2.Stop()
		n2.mu.Lock()
		term, vote := n2.ps.CurrentTerm, n2.ps.VotedFor
		n2.mu.Unlock()
		if term < 200 || (term == 200 && vote != "m") {
			t.Fatalf("term/vote after restart: got term=%d vote=%q, want 200/m", term, vote)
		}
		sameLog(t, logOf(n2), entries(100, 1, 3, "e"))
		// A second candidate in term 200 must not get a vote.
		if v := n2.HandleRequestVote(RequestVoteArgs{Term: 200, CandidateID: "l", LastLogIndex: 3, LastLogTerm: 100}); v.VoteGranted && term == 200 {
			t.Fatal("voted twice in term 200 after restart")
		}
	})

	t.Run("log-ahead-of-meta", func(t *testing.T) {
		// The write order makes this impossible (meta first), but a damaged
		// or hand-restored dataDir must still never leave currentTerm below
		// the last log term.
		dir := t.TempDir()
		n := startFollower(t, dir)
		n.HandleAppendEntries(AppendEntriesArgs{Term: 100, LeaderID: "l", Entries: entries(100, 1, 3, "e")})
		n.Stop()
		if err := (&logStore{dir: dir}).writeMeta(raftMeta{CurrentTerm: 5}); err != nil {
			t.Fatal(err)
		}
		n2 := startFollower(t, dir)
		defer n2.Stop()
		if term := n2.Term(); term < 100 {
			t.Fatalf("term after restart: want >= 100, got %d", term)
		}
		sameLog(t, logOf(n2), entries(100, 1, 3, "e"))
	})

	t.Run("interrupted-migration", func(t *testing.T) {
		// Migration wrote raft_log.dat, then crashed before raft_meta.dat
		// (its commit point): the legacy file is still authoritative.
		dir := t.TempDir()
		legacy := persistentState{CurrentTerm: 7, VotedFor: "x", Log: entries(7, 1, 4, "legacy")}
		writeLegacy(t, dir, legacy)
		if err := (&logStore{dir: dir}).rewriteFile(entries(7, 1, 2, "partial")); err != nil {
			t.Fatal(err)
		}
		n := startFollower(t, dir)
		defer n.Stop()
		sameLog(t, logOf(n), legacy.Log)
		if _, err := os.Stat(filepath.Join(dir, legacyStateFileName)); !os.IsNotExist(err) {
			t.Fatalf("legacy file still present after migration: %v", err)
		}
	})
}

// TestRaftLog_SnapshotCompactionThenRestart: compaction rewrites the log file
// to the retained suffix; restarts before and after further appends recover
// the full state from snapshot + file.
func TestRaftLog_SnapshotCompactionThenRestart(t *testing.T) {
	dir := t.TempDir()
	id := "compact-node"
	opts := testOpts(Options{SnapshotThreshold: 8})
	sm := &mockSnapSM{}
	n := startSingle(t, id, dir, sm, opts)
	first := cmdList("s", 0, 30)
	submitAll(t, n, first)

	deadline := time.Now().Add(5 * time.Second)
	var included uint64
	for time.Now().Before(deadline) {
		n.mu.Lock()
		included = n.lastIncludedIndex
		n.mu.Unlock()
		if included > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if included == 0 {
		n.Stop()
		t.Fatal("no snapshot taken")
	}
	n.Stop()

	// The file holds only the retained suffix (nothing at or below the
	// snapshot index).
	disk := diskLog(t, dir)
	if len(disk) >= len(first) {
		t.Fatalf("log file not compacted: %d records for %d commands", len(disk), len(first))
	}
	snap, ok, err := n.readSnapshotFile()
	if err != nil || !ok {
		t.Fatalf("read snapshot: ok=%v err=%v", ok, err)
	}
	if len(disk) > 0 && disk[0].Index != snap.LastIncludedIndex+1 {
		t.Fatalf("log file starts at %d, snapshot covers through %d", disk[0].Index, snap.LastIncludedIndex)
	}

	sm2 := &mockSnapSM{}
	n2 := startSingle(t, id, dir, sm2, opts)
	waitCount(t, sm2.count, len(first))
	second := cmdList("t", 0, 12)
	submitAll(t, n2, second)
	n2.Stop()

	sm3 := &mockSnapSM{}
	n3 := startSingle(t, id, dir, sm3, opts)
	defer n3.Stop()
	all := append(append([]string(nil), first...), second...)
	assertApplied(t, &sm3.mockSM, all)
}

func writeLegacy(t *testing.T, dir string, ps persistentState) {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(ps); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, legacyStateFileName), buf.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
}

// TestRaftLog_MigrateLegacyState: a dataDir written by the old whole-state
// format (raft_state.gob) is converted on startup and keeps term, vote and
// log; the old file is renamed out of the way and the next start reads only
// the new files.
func TestRaftLog_MigrateLegacyState(t *testing.T) {
	dir := t.TempDir()
	id := "legacy-node"
	var lg []LogEntry
	var want []string
	for i := uint64(1); i <= 20; i++ {
		c := fmt.Sprintf("legacy-%03d", i)
		lg = append(lg, LogEntry{Term: 7, Index: i, Command: []byte(c)})
		want = append(want, c)
	}
	writeLegacy(t, dir, persistentState{CurrentTerm: 7, VotedFor: id, Log: lg})

	hub := newMockTransport()
	sm := &mockSM{}
	n, err := NewRaftNodeWithOptions(id, nil, dir, sm, hub.forNode(id), testOpts(Options{}))
	if err != nil {
		t.Fatalf("start on legacy dir: %v", err)
	}
	hub.register(id, n)
	n.mu.Lock()
	term, logLen := n.ps.CurrentTerm, len(n.ps.Log)
	n.mu.Unlock()
	if term < 7 || logLen < 20 {
		n.Stop()
		t.Fatalf("after migration: term=%d log_len=%d, want >=7 / >=20", term, logLen)
	}
	for _, name := range []string{raftLogFileName, raftMetaFileName, legacyStateFileName + legacyMigratedSuffix} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			n.Stop()
			t.Fatalf("%s missing after migration: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, legacyStateFileName)); !os.IsNotExist(err) {
		n.Stop()
		t.Fatalf("%s still present after migration", legacyStateFileName)
	}
	if waitForLeader([]*RaftNode{n}, 3*time.Second) < 0 {
		n.Stop()
		t.Fatal("no leader after migration")
	}
	assertApplied(t, sm, want)
	submitAll(t, n, []string{"post-migration"})
	n.Stop()

	sm2 := &mockSM{}
	n2 := startSingle(t, id, dir, sm2, testOpts(Options{}))
	defer n2.Stop()
	assertApplied(t, sm2, append(want, "post-migration"))
}

// ── Persistence cost ──────────────────────────────────────────────────────────

// persistBytes is the number of bytes the node has handed to the filesystem
// for Raft state (log records, meta, rewrites).
func persistBytes(n *RaftNode) uint64 { return n.store.bytesWritten.Load() }

// growLog submits count 1 KiB commands with high concurrency (group commit
// keeps this fast).
func growLog(t testing.TB, n *RaftNode, count int, payload []byte) {
	var wg sync.WaitGroup
	const workers = 64
	errs := make(chan error, workers)
	per := (count + workers - 1) / workers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if err := n.Submit(payload); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		t.Fatalf("grow log: %v", err)
	}
}

// bytesPerFlush submits rounds single commands one at a time (one flush
// each) and returns the average bytes persisted per flush.
func bytesPerFlush(t testing.TB, n *RaftNode, payload []byte, rounds int) uint64 {
	before := persistBytes(n)
	for i := 0; i < rounds; i++ {
		if err := n.Submit(payload); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	return (persistBytes(n) - before) / uint64(rounds)
}

// TestRaftPersist_BytesPerFlushIndependentOfLogLength: the bytes persisted by
// one single-entry commit must not grow with the retained log.  The old
// whole-state format rewrote the entire log per flush — ~100 KiB at 100
// entries, ~8 MiB at 8,000 — and fails this test; the append-only log writes
// one ~1 KiB record either way.
func TestRaftPersist_BytesPerFlushIndependentOfLogLength(t *testing.T) {
	if testing.Short() {
		t.Skip("grows an 8,000-entry log")
	}
	payload := bytes.Repeat([]byte("p"), 1024)
	n := startSingle(t, "persist-cost", t.TempDir(), &mockSM{}, testOpts(Options{})) // no compaction
	defer n.Stop()

	growLog(t, n, 100, payload)
	small := bytesPerFlush(t, n, payload, 20)
	growLog(t, n, 7900, payload)
	n.mu.Lock()
	logLen := len(n.ps.Log)
	n.mu.Unlock()
	large := bytesPerFlush(t, n, payload, 20)
	t.Logf("bytes persisted per single-entry flush: %d at ~100 entries, %d at %d entries", small, large, logLen)

	if logLen < 8000 {
		t.Fatalf("log only reached %d entries", logLen)
	}
	limit := uint64(2 * (len(payload) + recHdrLen + recFixedLen))
	if small > limit || large > limit {
		t.Fatalf("per-flush bytes exceed one record: small=%d large=%d (limit %d)", small, large, limit)
	}
	if large > small+small/2 {
		t.Fatalf("per-flush bytes grew with log length: %d → %d", small, large)
	}
}

// BenchmarkRaftPersist_Submit measures one sequential single-node Submit
// (append + fsync + apply) with a retained log of the given length.
func BenchmarkRaftPersist_Submit(b *testing.B) {
	payload := bytes.Repeat([]byte("p"), 1024)
	for _, logLen := range []int{100, 8000} {
		b.Run(fmt.Sprintf("log=%d", logLen), func(b *testing.B) {
			hub := newMockTransport()
			id := fmt.Sprintf("bench-%d", logLen)
			n, err := NewRaftNodeWithOptions(id, nil, b.TempDir(), &mockSM{}, hub.forNode(id), testOpts(Options{}))
			if err != nil {
				b.Fatal(err)
			}
			hub.register(id, n)
			defer n.Stop()
			if waitForLeader([]*RaftNode{n}, 3*time.Second) < 0 {
				b.Fatal("no leader")
			}
			growLog(b, n, logLen, payload)
			before := persistBytes(n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := n.Submit(payload); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(persistBytes(n)-before)/float64(b.N), "persistB/op")
		})
	}
}
