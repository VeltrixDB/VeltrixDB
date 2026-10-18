package jepsen

// runner_test.go — one Jepsen run: start the cluster, run N clients and the
// nemesis for the configured duration, heal, take final reads, check, and
// report / save the history.

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	veltrixclient "github.com/VeltrixDB/veltrixdb/client"
)

// runConfig describes one test.
type runConfig struct {
	cluster clusterConfig
	clients int
	keys    int           // register keys per generation
	rotate  time.Duration // a new generation of keys every rotate (bounds per-key history length)
	faults  []string
	// Workload mix in percent: read, write, cas, incr; the rest are set adds.
	pRead, pWrite, pCAS, pIncr int
	// checkReads: reads go into the linearizability check (raft with
	// --linearizable-reads). Otherwise reads are checked for validity only.
	checkReads bool
	// readsAnywhere: reads go to a random node (raft local reads), not the
	// leader.
	readsAnywhere bool
	opTimeout     time.Duration
	// replicatedCopies: replicated mode, how many nodes must hold every
	// acknowledged write once the cluster has healed (strong 3, quorum 2).
	replicatedCopies int
	// negative: a control run that checks a property the configuration does
	// NOT provide; it passes only if the checker finds a violation.
	negative bool
}

func envDuration(t *testing.T, name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, v, err)
	}
	return d
}

func envSeed(t *testing.T) int64 {
	v := os.Getenv("VELTRIX_JEPSEN_SEED")
	if v == "" {
		return time.Now().UnixNano() % 1_000_000_000
	}
	s, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("VELTRIX_JEPSEN_SEED=%q: %v", v, err)
	}
	return s
}

// shared state the clients use to pick CAS expectations and a write target.
type shared struct {
	mu         sync.Mutex
	last       map[string]string // key → a recently seen value
	leaderHint atomic.Value      // string
	pid        atomic.Int64
	addrs      []string
}

func (s *shared) seen(key, val string) {
	s.mu.Lock()
	s.last[key] = val
	s.mu.Unlock()
}

func (s *shared) lastVal(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.last[key]
	return v, ok
}

func regKey(gen, k int) string { return fmt.Sprintf("r%d.%d", gen, k) }
func ctrKey(gen int) string    { return fmt.Sprintf("c%d", gen) }

func runJepsen(t *testing.T, rc runConfig) {
	if testing.Short() {
		t.Skip("jepsen cluster run skipped in -short mode")
	}
	dur := envDuration(t, "VELTRIX_JEPSEN_DURATION", 20*time.Second)
	checkTimeout := envDuration(t, "VELTRIX_JEPSEN_CHECK_TIMEOUT", 60*time.Second)
	seed := envSeed(t)
	if dl, ok := t.Deadline(); ok {
		// Leave room for setup, heal, final reads and the check.
		if left := time.Until(dl) - 2*time.Minute - checkTimeout; left < dur {
			if left < 10*time.Second {
				t.Fatalf("-timeout too short for a %v run (need ≥ %v more)", dur, 10*time.Second-left)
			}
			t.Logf("duration cut from %v to %v to fit -timeout", dur, left.Round(time.Second))
			dur = left
		}
	}
	rng := rand.New(rand.NewSource(seed))
	t.Logf("seed=%d duration=%v clients=%d mode=%s lin-reads=%v consistency=%s faults=%v",
		seed, dur, rc.clients, rc.cluster.mode, rc.cluster.linReads, rc.cluster.consistency, rc.faults)

	workDir, err := os.MkdirTemp("", "veltrix-jepsen-")
	if err != nil {
		t.Fatal(err)
	}
	keepWork := false
	t.Cleanup(func() {
		if !keepWork {
			os.RemoveAll(workDir)
		}
	})

	var exitMu sync.Mutex
	var unexpected []string
	rc.cluster.unexpectedExit = func(n *node, err error) {
		exitMu.Lock()
		unexpected = append(unexpected, fmt.Sprintf("%s exited unexpectedly: %v\n%s", n.id, err, n.logTail(40)))
		exitMu.Unlock()
	}
	c := startCluster(t, rc.cluster, workDir)
	h := NewHistory()

	sh := &shared{last: map[string]string{}, addrs: c.addrs()}
	sh.leaderHint.Store("")
	if rc.cluster.mode == "raft" {
		l, err := c.waitLeader(30 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		sh.leaderHint.Store(l.addr())
	}

	// ── workload + nemesis ──────────────────────────────────────────────
	stop := make(chan struct{})
	nm := newNemesis(c, h, rng.Int63(), rc.faults, partitionerFromEnv())
	if nm.part != nil {
		nm.faults = append(nm.faults, "partition")
	}
	nemDone := make(chan struct{})
	go func() { nm.run(stop); close(nemDone) }()

	end := time.Now().Add(dur)
	var wg sync.WaitGroup
	for i := 0; i < rc.clients; i++ {
		wg.Add(1)
		go func(id int, seed int64) {
			defer wg.Done()
			runClient(id, seed, rc, c, h, sh, end)
		}(i, rng.Int63())
	}
	wg.Wait()
	close(stop)
	<-nemDone
	workloadOps := len(h.Ops())
	t.Logf("workload done: %d ops in %v (%.0f ops/s); faults: %s", workloadOps, dur, float64(workloadOps)/dur.Seconds(), nm.summary())

	// ── final reads ─────────────────────────────────────────────────────
	var problems []string
	finalProblems, final := finalPhase(t, rc, c, h, sh)
	problems = append(problems, finalProblems...)

	// ── checks ──────────────────────────────────────────────────────────
	ops := h.Ops()
	problems = append(problems, nm.errs...)
	exitMu.Lock()
	problems = append(problems, unexpected...)
	exitMu.Unlock()
	keyResults, checkProblems := checkAll(t, rc, ops, final, checkTimeout)
	problems = append(problems, checkProblems...)

	// ── report ──────────────────────────────────────────────────────────
	t.Logf("ops: %s", opStats(ops))
	var outDir string
	if d := os.Getenv("VELTRIX_JEPSEN_OUT"); d != "" {
		outDir = filepath.Join(d, fmt.Sprintf("%s-%d", strings.ReplaceAll(t.Name(), "/", "_"), seed))
		_ = os.MkdirAll(outDir, 0o755)
	} else {
		outDir, _ = os.MkdirTemp("", "veltrix-jepsen-out-")
	}
	cfg := map[string]string{
		"mode": rc.cluster.mode, "linearizable_reads": strconv.FormatBool(rc.cluster.linReads),
		"consistency": rc.cluster.consistency, "duration": dur.String(), "clients": strconv.Itoa(rc.clients),
		"faults": nm.summary(),
	}
	if err := h.Save(filepath.Join(outDir, "history.json"), t.Name(), seed, cfg, problems); err != nil {
		t.Errorf("save history: %v", err)
	}
	for _, kr := range keyResults {
		if kr.Verdict == NotLinearizable {
			b, _ := json.MarshalIndent(kr.Minimal, "", " ")
			_ = os.WriteFile(filepath.Join(outDir, "minimal-"+kr.Key+".json"), b, 0o644)
		}
	}
	if rc.negative {
		found := 0
		for _, kr := range keyResults {
			if kr.Verdict == NotLinearizable {
				found++
			}
		}
		if found == 0 {
			t.Errorf("negative control: expected the checker to find a violation, found none (%d problems: %v)", len(problems), problems)
			return
		}
		for _, kr := range keyResults {
			if kr.Verdict == NotLinearizable {
				t.Logf("negative control: violation found as expected (%d keys):\n%s", found, FormatResult(kr))
				break
			}
		}
		os.RemoveAll(outDir)
		return
	}
	if len(problems) > 0 {
		keepWork = os.Getenv("VELTRIX_JEPSEN_KEEP_DATA") != ""
		for _, p := range problems {
			t.Error(p)
		}
		t.Errorf("seed %d: %d problem(s); full history saved to %s (server logs: %s)",
			seed, len(problems), filepath.Join(outDir, "history.json"), keepDirNote(keepWork, workDir))
		copyLogs(c, outDir)
		return
	}
	t.Logf("PASS seed=%d: no anomalies in %d ops (%d keys checked); faults: %s", seed, len(ops), len(keyResults), nm.summary())
	if os.Getenv("VELTRIX_JEPSEN_OUT") == "" && os.Getenv("VELTRIX_JEPSEN_KEEP") == "" {
		os.RemoveAll(outDir)
	} else {
		copyLogs(c, outDir)
		t.Logf("history saved to %s", outDir)
	}
}

func keepDirNote(keep bool, dir string) string {
	if keep {
		return dir
	}
	return "copied next to the history"
}

func copyLogs(c *cluster, outDir string) {
	for _, n := range c.nodes {
		if b, err := os.ReadFile(n.logPath); err == nil {
			_ = os.WriteFile(filepath.Join(outDir, n.id+".log"), b, 0o644)
		}
	}
}

// runClient is one client goroutine.
func runClient(id int, seed int64, rc runConfig, c *cluster, h *History, sh *shared, end time.Time) {
	rng := rand.New(rand.NewSource(seed))
	pick := func() string {
		if a, _ := sh.leaderHint.Load().(string); a != "" && rng.Intn(4) != 0 {
			return a
		}
		return sh.addrs[rng.Intn(len(sh.addrs))]
	}
	cl := &client{
		id: id, h: h, cl: c, timeout: rc.opTimeout,
		nextPID: func() int { return int(sh.pid.Add(1)) },
		pick:    pick,
	}
	cl.process = cl.nextPID()
	cl.target = pick()
	if rc.cluster.mode == "replicated" {
		cl.target = sh.addrs[id%len(sh.addrs)]
	}
	defer func() { cl.c.close() }()
	seq := 0
	for time.Now().Before(end) {
		gen := int(time.Duration(h.Now()) / rc.rotate)
		seq++
		val := fmt.Sprintf("%d.%d", id, seq)
		x := rng.Intn(100)
		var op Op
		switch {
		case x < rc.pRead:
			key := regKey(gen, rng.Intn(rc.keys))
			addr := cl.target
			if rc.readsAnywhere {
				addr = sh.addrs[rng.Intn(len(sh.addrs))]
			}
			op = cl.read(key, addr)
			if op.Outcome == OK && op.Found {
				sh.seen(key, op.Value)
			}
		case x < rc.pRead+rc.pWrite:
			key := regKey(gen, rng.Intn(rc.keys))
			op = cl.write(key, val)
			if op.Outcome == OK {
				sh.seen(key, val)
			}
		case x < rc.pRead+rc.pWrite+rc.pCAS:
			key := regKey(gen, rng.Intn(rc.keys))
			exp, ok := sh.lastVal(key)
			if !ok || rng.Intn(5) == 0 {
				exp = fmt.Sprintf("%d.%d", rng.Intn(rc.clients), rng.Intn(seq+1)) // usually stale or absent
			}
			op = cl.cas(key, exp, val)
			if op.Outcome == OK && op.CASRes == CASOK {
				sh.seen(key, val)
			}
		case x < rc.pRead+rc.pWrite+rc.pCAS+rc.pIncr:
			op = cl.incr(ctrKey(gen))
		default:
			op = cl.add(fmt.Sprintf("s/%d/%d", id, seq))
		}
		if a, moved := parseMoved(op.Err); moved && a != "" {
			sh.leaderHint.Store(a)
		}
		h.Add(op)
		if op.Outcome != OK {
			time.Sleep(time.Duration(20+rng.Intn(80)) * time.Millisecond) // back off a little
		}
	}
}

// final holds what finalPhase read after the cluster healed.
type final struct {
	setKeys  map[string]bool            // set keys on the reference node (raft leader / union)
	perNode  map[string]map[string]bool // replicated: set keys per node
	counters map[string]int64
}

// finalPhase waits for the healed cluster, takes final reads (recorded in the
// history so the checkers see them) and gathers the set keyspace.
func finalPhase(t *testing.T, rc runConfig, c *cluster, h *History, sh *shared) ([]string, final) {
	var probs []string
	fin := final{setKeys: map[string]bool{}, counters: map[string]int64{}, perNode: map[string]map[string]bool{}}
	ops := h.Ops()
	regKeys, ctrKeys := map[string]bool{}, map[string]bool{}
	for _, o := range ops {
		switch o.Kind {
		case KindIncr:
			ctrKeys[o.Key] = true
		case KindRead, KindWrite, KindCAS:
			regKeys[o.Key] = true
		}
	}
	readAddr := ""
	if rc.cluster.mode == "raft" {
		l, err := c.waitLeader(60 * time.Second)
		if err != nil {
			return append(probs, "after healing: "+err.Error()), fin
		}
		readAddr = l.addr()
		// A write through the leader commits (and applies on the leader)
		// every entry before it that will ever commit.
		ok := false
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			reply, err := command(readAddr, "PUT jepsen-barrier 1", 10*time.Second)
			if err == nil && reply == "OK" {
				ok = true
				break
			}
			if a, moved := parseMoved(reply); moved && a != "" {
				readAddr = a
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !ok {
			return append(probs, "after healing: barrier write through the leader never succeeded"), fin
		}
	} else {
		time.Sleep(2 * time.Second) // let replicas connect / recover
	}

	// Final reads, recorded as ordinary ops (Process -1).
	cl := &client{id: -1, process: -1, h: h, cl: c, timeout: 10 * time.Second,
		nextPID: func() int { return -1 }, pick: func() string { return readAddr }}
	defer func() { cl.c.close() }()
	var keys []string
	for k := range regKeys {
		keys = append(keys, k)
	}
	for k := range ctrKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if rc.cluster.mode == "raft" {
		for _, k := range keys {
			op := cl.read(k, readAddr)
			op.Final = true
			h.Add(op)
			if op.Outcome != OK {
				probs = append(probs, "final read failed: "+op.String())
				continue
			}
			if ctrKeys[k] {
				v := int64(0)
				if op.Found {
					v, _ = strconv.ParseInt(op.Value, 10, 64)
				}
				fin.counters[k] = v
			}
		}
		ks, err := keyspace(readAddr, "s/", "s0")
		if err != nil {
			probs = append(probs, "final set read: "+err.Error())
		}
		fin.setKeys = ks
		if !rc.cluster.linReads {
			// Followers serve local reads: they must converge to the leader.
			if p := waitConverged(c, 30*time.Second); p != "" {
				probs = append(probs, p)
			}
		}
		return probs, fin
	}

	// Replicated: every node's keyspace, after giving recovery time to
	// catch replicas up (recovery probes back off up to 30 s). Convergence
	// itself is not claimed by the docs (concurrent writes to one key on two
	// nodes are applied in arrival order, and a replica that was down while
	// its primary restarted is never caught up), so it is only reported.
	if p := waitConverged(c, 45*time.Second); p != "" {
		t.Logf("convergence (informational, not claimed for replicated mode): %s", p)
	} else {
		t.Logf("convergence: all nodes serve identical keyspaces")
	}
	for _, n := range c.nodes {
		ks, err := keyspace(n.addr(), "s/", "s0")
		if err != nil {
			probs = append(probs, fmt.Sprintf("final set read on %s: %v", n.id, err))
			continue
		}
		fin.perNode[n.id] = ks
		for k := range ks {
			fin.setKeys[k] = true
		}
	}
	return probs, fin
}

// keyspace returns the keys in [start, end) a node serves (local read,
// RANGE).
func keyspace(addr, start, end string) (map[string]bool, error) {
	bc, err := veltrixclient.DialBinary(addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer bc.Close()
	out := map[string]bool{}
	for {
		kvs, err := bc.RangeScan(start, end, 1000, false)
		if err != nil {
			return nil, err
		}
		for _, kv := range kvs {
			out[kv.Key] = true
		}
		if len(kvs) < 1000 {
			return out, nil
		}
		start = kvs[len(kvs)-1].Key + "\x00"
	}
}

// fullKeyspace returns every key → value a node serves.
func fullKeyspace(addr string) (map[string]string, error) {
	bc, err := veltrixclient.DialBinary(addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer bc.Close()
	out := map[string]string{}
	for start := ""; ; {
		kvs, err := bc.RangeScan(start, "", 1000, false)
		if err != nil {
			return nil, err
		}
		for _, kv := range kvs {
			out[kv.Key] = string(kv.Value)
		}
		if len(kvs) < 1000 {
			return out, nil
		}
		start = kvs[len(kvs)-1].Key + "\x00"
	}
}

// waitConverged waits until every node serves the same keyspace; it returns
// "" or a description of the last difference.
func waitConverged(c *cluster, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		var ref map[string]string
		refID := ""
		last = ""
		for _, n := range c.nodes {
			ks, err := fullKeyspace(n.addr())
			if err != nil {
				last = n.id + ": " + err.Error()
				break
			}
			if ref == nil {
				ref, refID = ks, n.id
				continue
			}
			if d := diffKeyspaces(ref, ks); d != "" {
				last = fmt.Sprintf("%s vs %s: %s", refID, n.id, d)
				break
			}
		}
		if last == "" {
			return ""
		}
		time.Sleep(500 * time.Millisecond)
	}
	return "nodes did not converge within " + timeout.String() + ": " + last
}

func diffKeyspaces(a, b map[string]string) string {
	var d []string
	for k, v := range a {
		if bv, ok := b[k]; !ok {
			d = append(d, "missing "+k)
		} else if bv != v {
			d = append(d, fmt.Sprintf("%s=%q/%q", k, v, bv))
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			d = append(d, "extra "+k)
		}
	}
	sort.Strings(d)
	if len(d) > 8 {
		d = append(d[:8], fmt.Sprintf("… %d more", len(d)-8))
	}
	return strings.Join(d, ", ")
}

const maxCounterInfo = 12

// checkAll runs every checker that applies to the mode.
func checkAll(t *testing.T, rc runConfig, ops []Op, fin final, timeout time.Duration) ([]KeyResult, []string) {
	var probs []string
	groups := byKey(ops)

	if rc.cluster.mode == "raft" {
		// Linearizability per key (registers and counters), in parallel.
		type job struct {
			key string
			m   Model
		}
		var jobs []job
		for k := range groups {
			switch {
			case strings.HasPrefix(k, "r"):
				jobs = append(jobs, job{k, RegisterModel})
			case strings.HasPrefix(k, "c"):
				// Info INCRs commute, so the (linearized set, state) cache
				// cannot merge their orderings: the search is exponential in
				// their number. Above maxCounterInfo the key is covered by
				// CheckCounter (bounds, distinct results, real-time order,
				// gaps) alone.
				info := 0
				for _, o := range groups[k] {
					if o.Kind == KindIncr && o.Outcome == Info {
						info++
					}
				}
				if info <= maxCounterInfo {
					jobs = append(jobs, job{k, CounterModel})
				} else {
					t.Logf("counter %s: %d info INCRs > %d, WGL skipped (CheckCounter only)", k, info, maxCounterInfo)
				}
			}
		}
		sort.Slice(jobs, func(i, j int) bool { return jobs[i].key < jobs[j].key })
		results := make([]KeyResult, len(jobs))
		var wg sync.WaitGroup
		sem := make(chan struct{}, runtime.NumCPU())
		for i, j := range jobs {
			wg.Add(1)
			go func(i int, j job) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				results[i] = CheckKey(j.m, j.key, groups[j.key], rc.checkReads, timeout)
			}(i, j)
		}
		wg.Wait()
		var unknown []string
		totalOps, totalInfo := 0, 0
		var slowest time.Duration
		for _, r := range results {
			totalOps += r.Ops
			totalInfo += r.Info
			if r.Elapsed > slowest {
				slowest = r.Elapsed
			}
			switch r.Verdict {
			case NotLinearizable:
				probs = append(probs, FormatResult(r))
			case Unknown:
				unknown = append(unknown, fmt.Sprintf("%s (%d ops, %d info)", r.Key, r.Ops, r.Info))
			}
		}
		what := "writes, CASes, INCRs and final reads"
		if rc.checkReads {
			what = "reads, writes, CASes and INCRs"
		}
		t.Logf("linearizability (%s): %d keys, %d ops (%d info), slowest key %v; %d unknown %v",
			what, len(results), totalOps, totalInfo, slowest.Round(time.Millisecond), len(unknown), unknown)
		if !rc.checkReads {
			p := CheckReadValidity(ops)
			probs = append(probs, p...)
			reads := 0
			for _, o := range ops {
				if o.Kind == KindRead && o.Outcome == OK && !o.Final {
					reads++
				}
			}
			t.Logf("read validity (stale reads allowed): %d reads, %d invalid", reads, len(p))
		}
		for k, v := range fin.counters {
			p := CheckCounter(k, groups[k], v)
			probs = append(probs, p...)
		}
		t.Logf("counters: %d keys, final values %v", len(fin.counters), fin.counters)
		sr := CheckSet(ops, fin.setKeys)
		t.Logf("set: %d ok adds, %d info adds (%d took effect), %d lost, %d phantom", sr.OK, sr.Info, sr.InfoPresent, len(sr.Lost), len(sr.Phantoms))
		for _, l := range sr.Lost {
			probs = append(probs, "lost acknowledged add: "+l)
		}
		for _, p := range sr.Phantoms {
			probs = append(probs, "set key present but never written: "+p)
		}
		return results, probs
	}

	// Replicated mode.
	sr := CheckSet(ops, fin.setKeys)
	for _, p := range sr.Phantoms {
		probs = append(probs, "set key present but never written: "+p)
	}
	copies := CheckCopies(ops, fin.perNode, rc.replicatedCopies)
	for i, p := range copies {
		if i == 20 {
			probs = append(probs, fmt.Sprintf("… %d more acknowledged adds short of copies", len(copies)-20))
			break
		}
		probs = append(probs, fmt.Sprintf("acknowledged add on fewer than %d nodes: %s", rc.replicatedCopies, p))
	}
	perNodeMissing := map[string]int{}
	for id, ks := range fin.perNode {
		for _, o := range ops {
			if o.Kind == KindAdd && o.Outcome == OK && !ks[o.Key] {
				perNodeMissing[id]++
			}
		}
	}
	t.Logf("set: %d ok adds, %d info adds (%d took effect somewhere), %d phantom; ok adds missing per node %v (need ≥ %d copies each, %d short)",
		sr.OK, sr.Info, sr.InfoPresent, len(sr.Phantoms), perNodeMissing, rc.replicatedCopies, len(copies))
	p := CheckReadValidity(ops)
	probs = append(probs, p...)
	return nil, probs
}

func opStats(ops []Op) string {
	n := map[string]int{}
	for _, o := range ops {
		n[string(o.Kind)+"/"+string(o.Outcome)]++
	}
	var ks []string
	for k, v := range n {
		ks = append(ks, fmt.Sprintf("%s=%d", k, v))
	}
	sort.Strings(ks)
	return strings.Join(ks, " ")
}

func envBool(name string) bool {
	b, _ := strconv.ParseBool(os.Getenv(name))
	return b
}
