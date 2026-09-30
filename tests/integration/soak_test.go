package integration_test

// soak_test.go — long-running and crash tests for search, against real server
// processes, each checked against an oracle of acknowledged writes.
//
//   - TestSearchSoak (VELTRIX_SOAK_DURATION, e.g. 20m): writers upsert and
//     delete vectors, text documents and records over a bounded id space
//     while readers search. Every result must be an id that was live when
//     the search started, self-queries must find their vector, a text marker
//     must find its document, and the server's RSS must level off. It ends
//     with a clean restart and an exact comparison of the rebuilt index with
//     the oracle.
//   - TestSearchCrashRecovery (VELTRIX_CHAOS_CYCLES, e.g. 10): repeatedly
//     SIGKILLs the server in the middle of concurrent writes, restarts it on
//     the same data directory, and requires every acknowledged write — and
//     no acknowledged delete — to be visible to search and GET after the
//     rebuild. Writes that were in flight when the process died may go
//     either way and are not checked.
//
// Both are skipped unless their variable is set; the nightly workflow sets
// them.

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	veltrixclient "github.com/VeltrixDB/veltrixdb/client"
)

const (
	soakDim = 16
	soakNS  = "soak"
)

// proc is one server process on a fixed data dir and ports, restartable.
type proc struct {
	t        *testing.T
	dataDir  string
	addr     string
	metrics  string
	extra    []string
	cmd      *exec.Cmd
	stderrOK bool
}

func newProc(t *testing.T, extra ...string) *proc {
	t.Helper()
	dir, err := os.MkdirTemp("", "veltrix-soak-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return &proc{
		t: t, dataDir: dir, extra: extra,
		addr:    fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		metrics: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
	}
}

// start launches the server and waits until its search indexes are rebuilt.
func (p *proc) start() {
	p.t.Helper()
	bin := serverBinary()
	if bin == "" {
		bin = builtBinary(p.t)
	}
	args := append([]string{"-addr", p.addr, "-metrics-addr", p.metrics, "-data", p.dataDir}, p.extra...)
	p.cmd = exec.Command(bin, args...)
	p.cmd.Dir = moduleRoot(p.t)
	p.cmd.Stderr = os.Stderr
	p.cmd.Stdout = os.Stderr
	if err := p.cmd.Start(); err != nil {
		p.t.Fatalf("start server: %v", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		if info, err := p.info(); err == nil && strings.Contains(info, "search_ready=1") {
			return
		}
		if time.Now().After(deadline) {
			p.t.Fatalf("server %s not search-ready within 60s", p.addr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// info returns the text-protocol INFO line.
func (p *proc) info() (string, error) {
	conn, err := net.DialTimeout("tcp", p.addr, time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprintln(conn, "INFO"); err != nil {
		return "", err
	}
	return bufio.NewReader(conn).ReadString('\n')
}

// kill stops the process with sig and waits for it to exit.
func (p *proc) kill(sig os.Signal) {
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(sig)
	done := make(chan struct{})
	go func() { _ = p.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	}
	p.cmd = nil
}

// rssKB is the process's resident set size.
func (p *proc) rssKB() int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(p.cmd.Process.Pid)).Output()
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	return n
}

func (p *proc) dial() *veltrixclient.BinaryConn {
	p.t.Helper()
	c, err := veltrixclient.DialBinary(p.addr, 5*time.Second)
	if err != nil {
		p.t.Fatalf("dial %s: %v", p.addr, err)
	}
	return c
}

// soakVec is the deterministic vector for (id, version): an id's content is
// recoverable from the oracle without storing it.
func soakVec(id, version int) []float32 {
	r := rand.New(rand.NewSource(int64(id)*1_000_003 + int64(version)))
	v := make([]float32, soakDim)
	var s float64
	for i := range v {
		v[i] = float32(r.NormFloat64())
		s += float64(v[i]) * float64(v[i])
	}
	inv := float32(1 / math.Sqrt(s))
	for i := range v {
		v[i] *= inv
	}
	return v
}

func soakID(id int) string { return fmt.Sprintf("s%05d", id) }

// oracleEntry is the acknowledged state of one id.
type oracleEntry struct {
	live     bool
	version  int
	changed  time.Time // when the last acknowledged change completed
	inflight bool      // a write for this id is between send and ack
}

type oracle struct {
	mu sync.Mutex
	m  map[int]*oracleEntry
}

func newOracle() *oracle { return &oracle{m: map[int]*oracleEntry{}} }

func (o *oracle) entry(id int) *oracleEntry {
	e := o.m[id]
	if e == nil {
		e = &oracleEntry{}
		o.m[id] = e
	}
	return e
}

// write performs one random upsert or delete of id through c and records it
// once acknowledged. It returns false when the id already has a write in
// flight (another writer owns it).
func (o *oracle) write(c *veltrixclient.BinaryConn, id int, del bool) error {
	o.mu.Lock()
	e := o.entry(id)
	if e.inflight {
		o.mu.Unlock()
		return nil
	}
	e.inflight = true
	version := e.version + 1
	o.mu.Unlock()

	var err error
	if del {
		if err = c.VDel(soakNS, soakID(id)); err == nil {
			err = c.TDel(soakNS, soakID(id))
		}
	} else {
		if err = c.Put(soakID(id), []byte(fmt.Sprintf(`{"v":"%d"}`, version)), 0); err == nil {
			if err = c.TSet(soakNS, soakID(id), fmt.Sprintf("doc %s marker%dv%d", soakID(id), id, version)); err == nil {
				err = c.VSetNS(soakNS, soakID(id), soakVec(id, version))
			}
		}
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	e.inflight = false
	if err == nil {
		e.live, e.version, e.changed = !del, version, time.Now()
	} else {
		// Unknown outcome (e.g. the server died mid-write): never check it.
		e.version = version
		e.changed = time.Now()
		e.inflight = true
	}
	return err
}

// deadOrLiveSince reports whether id was live at t: live now, or changed
// (possibly deleted) after t.
func (o *oracle) liveAt(id int, t time.Time) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	e := o.m[id]
	return e != nil && (e.live || e.changed.After(t) || e.inflight)
}

// unchanged reports whether id is still live at version, with no write
// since before t (so a miss on it is the server's fault, not a race with a
// writer).
func (o *oracle) unchanged(id, version int, t time.Time) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	e := o.m[id]
	return e != nil && e.live && !e.inflight && e.version == version && e.changed.Before(t)
}

// stable returns a random id that is live and unchanged for at least age.
func (o *oracle) stable(r *rand.Rand, age time.Duration) (int, int, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	cut := time.Now().Add(-age)
	for tries := 0; tries < 50; tries++ {
		for id, e := range o.m {
			if e.live && !e.inflight && e.changed.Before(cut) && r.Intn(4) == 0 {
				return id, e.version, true
			}
		}
	}
	return 0, 0, false
}

func parseSoakID(s string) (int, bool) {
	if !strings.HasPrefix(s, "s") {
		return 0, false
	}
	n, err := strconv.Atoi(s[1:])
	return n, err == nil
}

// verifyExact checks, on a quiescent server, that the searchable vector set,
// the text documents and the records match the oracle exactly (ids with an
// unknown outcome excluded).
func (o *oracle) verifyExact(t *testing.T, c *veltrixclient.BinaryConn) {
	t.Helper()
	all, err := c.VSearchWithOptions(0, soakVec(1, 1), veltrixclient.VectorSearchOptions{NS: soakNS})
	if err != nil {
		t.Fatalf("VSEARCH k=0: %v", err)
	}
	got := map[int]bool{}
	for _, h := range all {
		id, ok := parseSoakID(h.ID)
		if !ok {
			t.Fatalf("foreign id %q in the index", h.ID)
		}
		if got[id] {
			t.Fatalf("id %s returned twice", h.ID)
		}
		got[id] = true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	checked := 0
	for id, e := range o.m {
		if e.inflight {
			continue
		}
		checked++
		if e.live != got[id] {
			t.Fatalf("id %s: oracle live=%v, index has it=%v (version %d)", soakID(id), e.live, got[id], e.version)
		}
		if e.live {
			hits, err := c.TSearch(1, fmt.Sprintf("marker%dv%d", id, e.version), veltrixclient.TextSearchOptions{NS: soakNS})
			if err != nil || len(hits) != 1 || hits[0].ID != soakID(id) {
				t.Fatalf("text for %s v%d: %+v err=%v", soakID(id), e.version, hits, err)
			}
			val, err := c.Get(soakID(id))
			if err != nil || string(val) != fmt.Sprintf(`{"v":"%d"}`, e.version) {
				t.Fatalf("record %s: %q err=%v, want version %d", soakID(id), val, err, e.version)
			}
		}
	}
	for id := range got {
		if e := o.m[id]; e == nil {
			t.Fatalf("index has %s, which was never written", soakID(id))
		}
	}
	t.Logf("exact check: %d ids compared, %d live", checked, len(got))
}

func TestSearchSoak(t *testing.T) {
	d, err := time.ParseDuration(os.Getenv("VELTRIX_SOAK_DURATION"))
	if err != nil || d <= 0 {
		t.Skip("set VELTRIX_SOAK_DURATION (e.g. 20m)")
	}
	const idSpace = 5000
	p := newProc(t)
	p.start()
	defer p.kill(syscall.SIGKILL)
	setup := p.dial()
	if err := setup.VCreateWithOptions(soakNS, soakDim, veltrixclient.VectorNamespaceOptions{
		Quantization: "pq", PQSubspaces: 4, PQTrainAt: 1000}); err != nil {
		t.Fatal(err)
	}
	setup.Close()

	o := newOracle()
	var stop atomic.Bool
	var ops, searches, bad, selfMiss, selfTotal, textMiss atomic.Int64
	var wg sync.WaitGroup
	errc := make(chan error, 64)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := p.dial()
			defer c.Close()
			r := rand.New(rand.NewSource(int64(w)))
			for !stop.Load() {
				if err := o.write(c, r.Intn(idSpace), r.Intn(5) == 0); err != nil {
					errc <- fmt.Errorf("write: %w", err)
					return
				}
				ops.Add(1)
			}
		}(w)
	}
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := p.dial()
			defer c.Close()
			r := rand.New(rand.NewSource(int64(100 + w)))
			for !stop.Load() {
				start := time.Now()
				q := soakVec(r.Intn(idSpace), r.Intn(3)+1)
				hits, err := c.VSearchWithOptions(10, q, veltrixclient.VectorSearchOptions{NS: soakNS})
				if err != nil {
					errc <- fmt.Errorf("search: %w", err)
					return
				}
				searches.Add(1)
				for _, h := range hits {
					if id, ok := parseSoakID(h.ID); !ok || !o.liveAt(id, start.Add(-50*time.Millisecond)) {
						bad.Add(1)
						errc <- fmt.Errorf("search returned %s, not live when the search started", h.ID)
					}
				}
				if id, version, ok := o.stable(r, time.Second); ok {
					selfTotal.Add(1)
					before := time.Now()
					got, err := c.VSearchWithOptions(1, soakVec(id, version), veltrixclient.VectorSearchOptions{NS: soakNS})
					if err == nil && (len(got) == 0 || got[0].ID != soakID(id)) && o.unchanged(id, version, before) {
						selfMiss.Add(1)
					}
					before = time.Now()
					th, err := c.TSearch(1, fmt.Sprintf("marker%dv%d", id, version), veltrixclient.TextSearchOptions{NS: soakNS})
					if err == nil && (len(th) != 1 || th[0].ID != soakID(id)) && o.unchanged(id, version, before) {
						textMiss.Add(1)
						errc <- fmt.Errorf("text marker%dv%d of stable %s not found: %+v", id, version, soakID(id), th)
					}
				}
			}
		}(w)
	}

	// Sample RSS; the id space is bounded, so memory must level off.
	var rss []int64
	tick := time.NewTicker(d / 20)
	deadline := time.After(d)
loop:
	for {
		select {
		case err := <-errc:
			stop.Store(true)
			wg.Wait()
			t.Fatal(err)
		case <-tick.C:
			rss = append(rss, p.rssKB())
			t.Logf("t=%s ops=%d searches=%d rss=%d MB", time.Duration(len(rss))*(d/20), ops.Load(), searches.Load(), rss[len(rss)-1]/1024)
		case <-deadline:
			break loop
		}
	}
	tick.Stop()
	stop.Store(true)
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}

	t.Logf("ops=%d searches=%d self-queries=%d (missed %d vector, %d text)", ops.Load(), searches.Load(), selfTotal.Load(), selfMiss.Load(), textMiss.Load())
	if n := selfTotal.Load(); n > 0 && float64(selfMiss.Load())/float64(n) > 0.02 {
		t.Errorf("self-query miss rate %.3f > 2%%", float64(selfMiss.Load())/float64(n))
	}
	if textMiss.Load() > 0 {
		t.Errorf("%d text markers of stable documents not found", textMiss.Load())
	}
	if len(rss) >= 8 {
		quarter, last := rss[len(rss)/4], rss[len(rss)-1]
		if float64(last) > 1.5*float64(quarter) {
			t.Errorf("RSS kept growing: %d MB at 25%% of the run, %d MB at the end", quarter/1024, last/1024)
		}
	}

	// Clean restart: the rebuilt state must equal the oracle exactly.
	c := p.dial()
	o.verifyExact(t, c)
	c.Close()
	p.kill(syscall.SIGTERM)
	p.start()
	c = p.dial()
	defer c.Close()
	o.verifyExact(t, c)
}

func TestSearchCrashRecovery(t *testing.T) {
	cycles, _ := strconv.Atoi(os.Getenv("VELTRIX_CHAOS_CYCLES"))
	if cycles <= 0 {
		t.Skip("set VELTRIX_CHAOS_CYCLES (e.g. 10)")
	}
	const idSpace = 2000
	p := newProc(t)
	p.start()
	defer p.kill(syscall.SIGKILL)
	setup := p.dial()
	if err := setup.VCreateWithOptions(soakNS, soakDim, veltrixclient.VectorNamespaceOptions{Quantization: "int8"}); err != nil {
		t.Fatal(err)
	}
	setup.Close()

	o := newOracle()
	r := rand.New(rand.NewSource(99))
	for cycle := 1; cycle <= cycles; cycle++ {
		var stop atomic.Bool
		var wg sync.WaitGroup
		var acked atomic.Int64
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				c, err := veltrixclient.DialBinary(p.addr, 5*time.Second)
				if err != nil {
					return
				}
				defer c.Close()
				wr := rand.New(rand.NewSource(int64(cycle*100 + w)))
				for !stop.Load() {
					if o.write(c, wr.Intn(idSpace), wr.Intn(4) == 0) != nil {
						return // connection died with the server
					}
					acked.Add(1)
				}
			}(w)
		}
		time.Sleep(time.Duration(500+r.Intn(1500)) * time.Millisecond)
		p.kill(syscall.SIGKILL) // mid-write
		stop.Store(true)
		wg.Wait()

		p.start()
		c := p.dial()
		o.verifyExact(t, c)
		c.Close()
		t.Logf("cycle %d/%d: %d acknowledged writes, all visible after SIGKILL + rebuild", cycle, cycles, acked.Load())

		// The in-flight writes are now settled: read back their outcome so
		// later cycles check them too.
		o.mu.Lock()
		for _, e := range o.m {
			e.inflight = false
		}
		o.mu.Unlock()
		o.settle(t, p)
	}
}

// settle aligns entries whose outcome was unknown with what the server has.
func (o *oracle) settle(t *testing.T, p *proc) {
	t.Helper()
	c := p.dial()
	defer c.Close()
	all, err := c.VSearchWithOptions(0, soakVec(1, 1), veltrixclient.VectorSearchOptions{NS: soakNS})
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	have := map[int]bool{}
	for _, h := range all {
		if id, ok := parseSoakID(h.ID); ok {
			have[id] = true
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for id, e := range o.m {
		if e.live == have[id] {
			continue
		}
		// An in-flight write landed (or a delete did): rewrite the id to a
		// known state so the next exact check can include it.
		version := e.version + 1
		o.mu.Unlock()
		err := c.Put(soakID(id), []byte(fmt.Sprintf(`{"v":"%d"}`, version)), 0)
		if err == nil {
			err = c.TSet(soakNS, soakID(id), fmt.Sprintf("doc %s marker%dv%d", soakID(id), id, version))
		}
		if err == nil {
			err = c.VSetNS(soakNS, soakID(id), soakVec(id, version))
		}
		o.mu.Lock()
		if err != nil {
			t.Fatalf("settle %s: %v", soakID(id), err)
		}
		e.live, e.version, e.changed = true, version, time.Now()
	}
}
