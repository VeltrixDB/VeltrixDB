//go:build cgo && go1.21

package netfront

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/client"
	"github.com/VeltrixDB/veltrixdb/storage"
)

// memBackend is an in-memory Backend whose writes are deliberately slow, so
// a write is still in flight while later requests on the same connection
// have already arrived — the case the per-connection ordering rule exists for.
type memBackend struct {
	mu        sync.Mutex
	m         map[string][]byte
	writeWait time.Duration
}

func newMem(wait time.Duration) *memBackend {
	return &memBackend{m: map[string][]byte{}, writeWait: wait}
}

var errNotFound = errors.New("not found")

func (b *memBackend) Get(k string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.m[k]
	if !ok {
		return nil, errNotFound
	}
	return v, nil
}

func (b *memBackend) MultiGet(keys []string) []storage.MultiGetResult {
	out := make([]storage.MultiGetResult, len(keys))
	for i, k := range keys {
		v, err := b.Get(k)
		out[i] = storage.MultiGetResult{Key: k, Value: v, Found: err == nil}
	}
	return out
}

func (b *memBackend) Put(k string, v []byte, _ int32) error {
	time.Sleep(b.writeWait)
	b.mu.Lock()
	b.m[k] = v
	b.mu.Unlock()
	return nil
}

func (b *memBackend) MultiPut(reqs []storage.MultiPutRequest) []error {
	time.Sleep(b.writeWait)
	b.mu.Lock()
	for _, r := range reqs {
		b.m[r.Key] = r.Value
	}
	b.mu.Unlock()
	return make([]error, len(reqs))
}

func (b *memBackend) Delete(k string) error {
	time.Sleep(b.writeWait)
	b.mu.Lock()
	delete(b.m, k)
	b.mu.Unlock()
	return nil
}

// ioBackend picks what to test: poll everywhere; on Linux CI set
// VXNF_TEST_BACKEND=uring to require io_uring.
func ioBackend() string {
	if v := os.Getenv("VXNF_TEST_BACKEND"); v != "" {
		return v
	}
	return "poll"
}

func start(t *testing.T, be Backend, threads int) (*Server, string) {
	t.Helper()
	s, err := Start(Config{Addr: "127.0.0.1:0", Threads: threads, IOBackend: ioBackend()}, be)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if want := ioBackend(); want == "uring" && s.IOBackend() != "io_uring" {
		t.Fatalf("asked for io_uring, running %s", s.IOBackend())
	}
	return s, fmt.Sprintf("127.0.0.1:%d", s.Port())
}

func dial(t *testing.T, addr string) *client.BinaryConn {
	t.Helper()
	c, err := client.DialBinary(addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestNetfront_SingleOps(t *testing.T) {
	_, addr := start(t, newMem(time.Millisecond), 2)
	c := dial(t, addr)
	if err := c.Put("k1", []byte("v1"), -1); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Get("k1"); err != nil || string(v) != "v1" {
		t.Fatalf("Get = %q, %v", v, err)
	}
	if v, err := c.Get("absent"); err != nil || v != nil {
		t.Fatalf("Get(absent) = %q, %v; want nil, nil", v, err)
	}
	if err := c.Delete("k1"); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.Get("k1"); v != nil {
		t.Fatalf("deleted key still readable: %q", v)
	}
}

func TestNetfront_Batches(t *testing.T) {
	_, addr := start(t, newMem(time.Millisecond), 2)
	c := dial(t, addr)
	// Values are never empty: like cmd/server, an empty value reads back as
	// not-found (handleMGet treats Value == nil that way).
	var ents []client.MPutEntry
	var keys []string
	for i := 0; i < 1000; i++ {
		k := fmt.Sprint("b", i)
		ents = append(ents, client.MPutEntry{Key: k, Value: []byte(strings.Repeat("x", 1+i%300)), TTL: -1})
		keys = append(keys, k)
	}
	keys = append(keys, "missing")
	errs, err := c.MPut(ents)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("entry %d: %v", i, e)
		}
	}
	res, err := c.MGet(keys)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if res[i].NotFound || len(res[i].Value) != 1+i%300 {
			t.Fatalf("MGet[%d] = %+v", i, res[i])
		}
	}
	if !res[1000].NotFound {
		t.Fatalf("MGet(missing) = %+v", res[1000])
	}
}

// TestNetfront_PipelineOrder: PUT then GET of the same key in ONE write.
// The GET must see the PUT (read-your-writes on a connection), and answers
// must come back in request order, although the PUT is answered from
// another goroutine long after the GET could have been.
func TestNetfront_PipelineOrder(t *testing.T) {
	_, addr := start(t, newMem(20*time.Millisecond), 1)
	c := dial(t, addr)
	p := client.NewPipeline(c)
	for i := 0; i < 50; i++ {
		k := fmt.Sprint("p", i%5)
		v := fmt.Sprint("v", i)
		p.Put(k, []byte(v), -1)
		p.Get(k)
	}
	p.Delete("p0")
	p.Get("p0")
	res, err := p.Exec()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		put, get := res[2*i], res[2*i+1]
		if put.Err != nil {
			t.Fatalf("PUT %d: %v", i, put.Err)
		}
		if want := fmt.Sprint("v", i); string(get.Value) != want {
			t.Fatalf("GET after PUT %d = %q, want %q (order or read-your-writes broken)", i, get.Value, want)
		}
	}
	if !res[len(res)-1].NotFound {
		t.Fatalf("GET after DEL = %+v, want not found", res[len(res)-1])
	}
}

func TestNetfront_ManyConnections(t *testing.T) {
	_, addr := start(t, newMem(time.Millisecond), 4)
	var wg sync.WaitGroup
	errc := make(chan error, 64)
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			c, err := client.DialBinary(addr, 2*time.Second)
			if err != nil {
				errc <- err
				return
			}
			defer c.Close()
			for i := 0; i < 200; i++ {
				k := fmt.Sprintf("c%d-%d", g, i)
				if err := c.Put(k, []byte(k), -1); err != nil {
					errc <- err
					return
				}
				if v, err := c.Get(k); err != nil || string(v) != k {
					errc <- fmt.Errorf("conn %d: Get(%s) = %q, %v", g, k, v, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
}

// A value far larger than one 64 KiB read, so the frame completes across
// many receives and the input buffer has to grow mid-frame.
func TestNetfront_LargeValue(t *testing.T) {
	_, addr := start(t, newMem(0), 1)
	c := dial(t, addr)
	big := bytes.Repeat([]byte("0123456789abcdef"), 1<<16) // 1 MiB
	if err := c.Put("big", big, -1); err != nil {
		t.Fatal(err)
	}
	v, err := c.Get("big")
	if err != nil || !bytes.Equal(v, big) {
		t.Fatalf("Get(big): %d bytes, err %v", len(v), err)
	}
}

func readFrame(t *testing.T, conn net.Conn) (byte, string) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var h [5]byte
	if _, err := io.ReadFull(conn, h[:]); err != nil {
		t.Fatalf("read frame: %v", err)
	}
	p := make([]byte, binary.LittleEndian.Uint32(h[1:]))
	if _, err := io.ReadFull(conn, p); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	return h[0], string(p)
}

// Unsupported commands get an error and a close — after the answers to the
// requests that preceded them on the connection.
func TestNetfront_UnsupportedCommand(t *testing.T) {
	be := newMem(0)
	be.m["x"] = []byte("1")
	_, addr := start(t, be, 1)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	get := []byte{0x02, 1, 0, 0, 0, 0, 0, 'x'}
	info := []byte{0x05, 0, 0, 0, 0, 0, 0}
	conn.Write(append(get, info...))
	if st, v := readFrame(t, conn); st != statusOK || v != "1" {
		t.Fatalf("GET before INFO = %d %q", st, v)
	}
	if st, msg := readFrame(t, conn); st != statusErr || !strings.Contains(msg, "not served") {
		t.Fatalf("INFO = %d %q, want an error", st, msg)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("connection not closed after protocol error: %v", err)
	}
}

func TestNetfront_TextProtocolRejected(t *testing.T) {
	_, addr := start(t, newMem(0), 1)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write([]byte("GET k\n"))
	if st, msg := readFrame(t, conn); st != statusErr || !strings.Contains(msg, "binary protocol") {
		t.Fatalf("text request = %d %q", st, msg)
	}
}

// Close with writes still in flight: the server must wait for them, drop
// their answers, and not crash or leak.
func TestNetfront_CloseWithWritesInFlight(t *testing.T) {
	s, err := Start(Config{Addr: "127.0.0.1:0", Threads: 2, IOBackend: ioBackend()}, newMem(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", s.Port())
	for i := 0; i < 8; i++ {
		c, err := client.DialBinary(addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		go func(c *client.BinaryConn) { _ = c.Put("slow", []byte("v"), -1); c.Close() }(c)
	}
	time.Sleep(50 * time.Millisecond) // the PUTs are now inside Backend.Put
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
	if _, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
		t.Fatal("still accepting after Close")
	}
}

// End to end against the real engine.
func TestNetfront_StorageEngine(t *testing.T) {
	cfg := storage.DefaultStorageConfig()
	cfg.DataDirPath = t.TempDir()
	cfg.DataDirPaths = nil
	cfg.CacheMaxSizeMB = 16
	cfg.BloomFilterShardBits = 1 << 12
	cfg.WALFlushWindowMs, cfg.VLogFlushWindowMs = 1, 1
	cfg.ScrubEnabled = false
	se, err := storage.NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer se.Close()
	s, addr := start(t, se, 2)
	c := dial(t, addr)
	for i := 0; i < 100; i++ {
		k := fmt.Sprint("e", i)
		if err := c.Put(k, []byte(k), -1); err != nil {
			t.Fatal(err)
		}
	}
	res, err := c.MGet([]string{"e0", "e99", "nope"})
	if err != nil {
		t.Fatal(err)
	}
	if string(res[0].Value) != "e0" || string(res[1].Value) != "e99" || !res[2].NotFound {
		t.Fatalf("MGet = %+v", res)
	}
	it, reqs, batches := s.Stats()
	if reqs < 101 || batches == 0 || it == 0 {
		t.Fatalf("stats: iterations=%d requests=%d batches=%d", it, reqs, batches)
	}
	s.Close()
	// The report is logged after Close, when the C++ server is freed: the
	// loop histograms must come from the snapshot Close took.
	rep := s.LatencyReport()
	for _, stage := range []string{"iter", "put_exec", "wake"} {
		if !strings.Contains(rep, "stage="+stage) || regexp.MustCompile(`stage=`+stage+` +n=0 `).MatchString(rep) {
			t.Fatalf("stage %s empty after Close:\n%s", stage, rep)
		}
	}
}

// diskBackend adds the noIOGetter extension to memBackend: keys starting
// with "d:" are "on disk" — GetNoIO reports needIO and GetAfterNoIO sleeps
// readWait — so the front-end has to answer them from a goroutine.
type diskBackend struct {
	*memBackend
	readWait  time.Duration
	mu        sync.Mutex
	afterNoIO int
}

func newDisk(writeWait, readWait time.Duration) *diskBackend {
	return &diskBackend{memBackend: newMem(writeWait), readWait: readWait}
}

func (b *diskBackend) GetNoIO(k string) ([]byte, bool, error) {
	if strings.HasPrefix(k, "d:") {
		return nil, true, nil
	}
	v, err := b.memBackend.Get(k)
	return v, false, err
}

func (b *diskBackend) GetAfterNoIO(k string) ([]byte, error) {
	b.mu.Lock()
	b.afterNoIO++
	b.mu.Unlock()
	time.Sleep(b.readWait)
	return b.memBackend.Get(k)
}

func (b *diskBackend) afterNoIOCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.afterNoIO
}

// A GET that needs disk is answered from a goroutine; every request behind
// it on the connection — reads that could be answered inline, writes, and
// reads of those writes — must still be answered in request order.
func TestNetfront_DeferredReadOrder(t *testing.T) {
	t.Setenv(DeferReadsEnv, "") // these test deferral itself: default on
	be := newDisk(5*time.Millisecond, 30*time.Millisecond)
	be.m["d:1"] = []byte("disk1")
	be.m["m1"] = []byte("mem1")
	_, addr := start(t, be, 1)
	c := dial(t, addr)

	for round := 0; round < 3; round++ {
		p := client.NewPipeline(c)
		p.Get("d:1") // deferred
		p.Get("m1")  // inline-able, must wait for d:1
		p.Put("m2", []byte(fmt.Sprint("v", round)), -1)
		p.Get("m2")        // read-your-writes behind a deferred read
		p.Get("d:missing") // deferred, not found
		p.Get("m1")
		res, err := p.Exec()
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"disk1", "mem1", "", fmt.Sprint("v", round), "", "mem1"}
		for i, w := range want {
			switch {
			case i == 2:
				if res[i].Err != nil {
					t.Fatalf("round %d PUT: %v", round, res[i].Err)
				}
			case i == 4:
				if !res[i].NotFound {
					t.Fatalf("round %d res[4] = %+v, want not found", round, res[i])
				}
			case string(res[i].Value) != w:
				t.Fatalf("round %d res[%d] = %q, want %q (order broken)", round, i, res[i].Value, w)
			}
		}
	}
	if n := be.afterNoIOCalls(); n != 6 {
		t.Fatalf("GetAfterNoIO calls = %d, want 6 (2 disk keys × 3 rounds)", n)
	}
}

// The point of deferring: a slow disk read on one connection must not stall
// another connection on the same loop.
func TestNetfront_DeferredReadNoHeadOfLine(t *testing.T) {
	t.Setenv(DeferReadsEnv, "") // these test deferral itself: default on
	be := newDisk(0, 400*time.Millisecond)
	be.m["d:slow"] = []byte("slow")
	be.m["fast"] = []byte("fast")
	s, addr := start(t, be, 1) // one loop: both connections share it
	slowC, fastC := dial(t, addr), dial(t, addr)

	slowDone := make(chan error, 1)
	go func() {
		v, err := slowC.Get("d:slow")
		if err == nil && string(v) != "slow" {
			err = fmt.Errorf("Get(d:slow) = %q", v)
		}
		slowDone <- err
	}()
	time.Sleep(50 * time.Millisecond) // the slow read is now inside GetAfterNoIO

	t0 := time.Now()
	if v, err := fastC.Get("fast"); err != nil || string(v) != "fast" {
		t.Fatalf("Get(fast) = %q, %v", v, err)
	}
	if d := time.Since(t0); d > 200*time.Millisecond {
		t.Fatalf("Get(fast) took %v behind another connection's disk read (head-of-line blocking)", d)
	}
	if err := <-slowDone; err != nil {
		t.Fatal(err)
	}
	if rep := s.LatencyReport(); !strings.Contains(rep, "stage=defer_exec") || strings.Contains(rep, "stage=defer_exec   n=0 ") {
		t.Fatalf("latency report has no deferred execution:\n%s", rep)
	}
}

// MGET with some keys on disk: one frame, results in key order, the disk
// keys read once each.
func TestNetfront_DeferredMGet(t *testing.T) {
	t.Setenv(DeferReadsEnv, "") // these test deferral itself: default on
	be := newDisk(0, 10*time.Millisecond)
	be.m["d:a"] = []byte("A")
	be.m["m:b"] = []byte("B")
	be.m["d:c"] = []byte("C")
	_, addr := start(t, be, 1)
	c := dial(t, addr)
	res, err := c.MGet([]string{"d:a", "m:b", "nope", "d:c", "d:gone"})
	if err != nil {
		t.Fatal(err)
	}
	if string(res[0].Value) != "A" || string(res[1].Value) != "B" || !res[2].NotFound ||
		string(res[3].Value) != "C" || !res[4].NotFound {
		t.Fatalf("MGet = %+v", res)
	}
	if n := be.afterNoIOCalls(); n != 3 {
		t.Fatalf("GetAfterNoIO calls = %d, want 3 (only the d: keys)", n)
	}
	// All-inline MGET: no disk reads.
	if res, err := c.MGet([]string{"m:b", "nope"}); err != nil || string(res[0].Value) != "B" || !res[1].NotFound {
		t.Fatalf("inline MGet = %+v, %v", res, err)
	}
	if n := be.afterNoIOCalls(); n != 3 {
		t.Fatalf("inline MGet read disk: GetAfterNoIO calls = %d", n)
	}
}

// The deferral must outlive the batch: a request that arrives in a LATER
// loop iteration, while the deferred read is still running, must not be
// answered inline ahead of it. (Within one batch the chain keeps order on
// its own; this is what vxnf_defer's blocking is for.)
func TestNetfront_DeferredReadBlocksLaterBatches(t *testing.T) {
	t.Setenv(DeferReadsEnv, "") // these test deferral itself: default on
	be := newDisk(0, 150*time.Millisecond)
	be.m["d:1"] = []byte("disk1")
	be.m["m1"] = []byte("mem1")
	_, addr := start(t, be, 1)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	get := func(k string) []byte { return append([]byte{0x02, byte(len(k)), 0, 0, 0, 0, 0}, k...) }

	conn.Write(get("d:1"))
	time.Sleep(40 * time.Millisecond) // d:1 is inside GetAfterNoIO
	conn.Write(get("m1"))             // a new batch on the same connection
	if st, v := readFrame(t, conn); st != statusOK || v != "disk1" {
		t.Fatalf("first answer = %d %q, want the deferred d:1 (\"disk1\") — a later request overtook it", st, v)
	}
	if st, v := readFrame(t, conn); st != statusOK || v != "mem1" {
		t.Fatalf("second answer = %d %q, want \"mem1\"", st, v)
	}
}
