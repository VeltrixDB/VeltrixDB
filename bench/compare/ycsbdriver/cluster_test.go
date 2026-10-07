package ycsbdriver

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magiconair/properties"
)

// fakeNode is a binary-protocol server that answers PUT (0x01), GET (0x02)
// and DEL (0x03) through handle, which returns (status, payload).
type fakeNode struct {
	ln     net.Listener
	addr   string
	handle func(cmd byte, key string) (byte, []byte)
	ops    atomic.Int64
	mu     sync.Mutex
	conns  []net.Conn
	closed bool
}

func newFakeNode(t *testing.T) *fakeNode {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeNode{ln: ln, addr: ln.Addr().String()}
	f.handle = func(byte, string) (byte, []byte) { return 0x00, nil }
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.conns = append(f.conns, c)
			f.mu.Unlock()
			go f.serve(c)
		}
	}()
	t.Cleanup(f.kill)
	return f
}

func (f *fakeNode) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	for {
		var hdr [7]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return
		}
		kl := int(binary.LittleEndian.Uint16(hdr[1:3]))
		vl := int(binary.LittleEndian.Uint32(hdr[3:7]))
		buf := make([]byte, kl+vl)
		if _, err := io.ReadFull(r, buf); err != nil {
			return
		}
		f.ops.Add(1)
		f.mu.Lock()
		h := f.handle
		f.mu.Unlock()
		status, payload := h(hdr[0], string(buf[:kl]))
		var resp [5]byte
		resp[0] = status
		binary.LittleEndian.PutUint32(resp[1:], uint32(len(payload)))
		if _, err := c.Write(append(resp[:], payload...)); err != nil {
			return
		}
	}
}

func (f *fakeNode) setHandle(h func(cmd byte, key string) (byte, []byte)) {
	f.mu.Lock()
	f.handle = h
	f.mu.Unlock()
}

// kill closes the listener and every open connection (a crashed node).
func (f *fakeNode) kill() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	f.ln.Close()
	for _, c := range f.conns {
		c.Close()
	}
}

// follower answers writes with MOVED to leader and serves reads locally.
func follower(leader string) func(byte, string) (byte, []byte) {
	return func(cmd byte, _ string) (byte, []byte) {
		if cmd == 0x01 || cmd == 0x03 {
			return 0x01, []byte("MOVED " + leader + " n1")
		}
		return 0x00, encode(map[string][]byte{"field0": []byte("v")})
	}
}

func newDB(t *testing.T, addrs []string, extra ...string) *db {
	t.Helper()
	p := properties.NewProperties()
	p.Set("veltrixdb.addr", strings.Join(addrs, ","))
	p.Set("veltrixdb.backoff", "5ms")
	for i := 0; i+1 < len(extra); i += 2 {
		p.Set(extra[i], extra[i+1])
	}
	d, err := creator{}.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	return d.(*db)
}

func TestClusterSpreadsThreads(t *testing.T) {
	nodes := []*fakeNode{newFakeNode(t), newFakeNode(t), newFakeNode(t)}
	addrs := []string{nodes[0].addr, nodes[1].addr, nodes[2].addr}
	d := newDB(t, addrs)
	var ctxs []context.Context
	for i := 0; i < 6; i++ {
		ctx := d.InitThread(context.Background(), i, 6)
		if _, err := d.Read(ctx, "t", "k", nil); err != nil {
			t.Fatal(err)
		}
		ctxs = append(ctxs, ctx)
	}
	for _, n := range nodes {
		if got := n.ops.Load(); got != 2 {
			t.Fatalf("node %s served %d reads, want 2 (round-robin)", n.addr, got)
		}
	}
	for _, ctx := range ctxs {
		d.CleanupThread(ctx)
	}
	if d.homes[addrs[0]] != 2 || d.reads[addrs[2]] != 2 {
		t.Fatalf("homes=%v reads=%v", d.homes, d.reads)
	}

	// spread=false: every thread on the first address.
	d2 := newDB(t, addrs, "veltrixdb.spread", "false")
	for i := 0; i < 3; i++ {
		ctx := d2.InitThread(context.Background(), i, 3)
		_, _ = d2.Read(ctx, "t", "k", nil)
		d2.CleanupThread(ctx)
	}
	if d2.homes[addrs[0]] != 3 {
		t.Fatalf("spread=false homes=%v", d2.homes)
	}
}

func TestClusterFollowsMoved(t *testing.T) {
	leader, f1, f2 := newFakeNode(t), newFakeNode(t), newFakeNode(t)
	f1.setHandle(follower(leader.addr))
	f2.setHandle(follower(leader.addr))
	d := newDB(t, []string{f1.addr, f2.addr, leader.addr})

	a := d.InitThread(context.Background(), 0, 2) // home f1
	b := d.InitThread(context.Background(), 1, 2) // home f2
	if err := d.Insert(a, "t", "k1", map[string][]byte{"f": []byte("v")}); err != nil {
		t.Fatalf("insert via follower: %v", err)
	}
	if d.redirects.Load() != 1 || leader.ops.Load() != 1 {
		t.Fatalf("redirects=%d leader ops=%d, want 1/1", d.redirects.Load(), leader.ops.Load())
	}
	// The learned leader is shared: thread b writes straight to it.
	if err := d.Insert(b, "t", "k2", map[string][]byte{"f": []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if d.redirects.Load() != 1 || f2.ops.Load() != 0 {
		t.Fatalf("second thread redirected again: redirects=%d f2 ops=%d", d.redirects.Load(), f2.ops.Load())
	}
	// Reads stay on the home node.
	if _, err := d.Read(b, "t", "k2", nil); err != nil || f2.ops.Load() != 1 {
		t.Fatalf("read not served by home: err=%v f2 ops=%d", err, f2.ops.Load())
	}

	// Leader unknown, then known: wait, then follow.
	var n atomic.Int64
	leader2 := newFakeNode(t)
	f1.setHandle(func(cmd byte, k string) (byte, []byte) {
		if n.Add(1) == 1 {
			return 0x01, []byte("MOVED - (leader unknown, retry)")
		}
		return 0x01, []byte("MOVED " + leader2.addr + " n4")
	})
	d.leader.Store(nil)
	if err := d.Insert(a, "t", "k3", map[string][]byte{"f": []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if d.leaderUnknown.Load() != 1 || d.redirects.Load() != 2 || leader2.ops.Load() != 1 {
		t.Fatalf("unknown=%d redirects=%d leader2 ops=%d", d.leaderUnknown.Load(), d.redirects.Load(), leader2.ops.Load())
	}

	// Redirect loops are bounded.
	f1.setHandle(func(byte, string) (byte, []byte) { return 0x01, []byte("MOVED " + f2.addr + " x") })
	f2.setHandle(func(byte, string) (byte, []byte) { return 0x01, []byte("MOVED " + f1.addr + " x") })
	d.leader.Store(nil)
	if err := d.Delete(a, "t", "k"); err == nil || !strings.Contains(err.Error(), "MOVED") {
		t.Fatalf("redirect loop must end in a MOVED error, got %v", err)
	}
}

func TestClusterFailover(t *testing.T) {
	n1, n2 := newFakeNode(t), newFakeNode(t)
	d := newDB(t, []string{n1.addr, n2.addr})
	ctx := d.InitThread(context.Background(), 0, 1) // home n1
	if _, err := d.Read(ctx, "t", "k", nil); err != nil {
		t.Fatal(err)
	}
	n1.kill()
	time.Sleep(10 * time.Millisecond)
	if _, err := d.Read(ctx, "t", "k", nil); err != nil {
		t.Fatalf("read after node loss: %v", err)
	}
	if d.failovers.Load() != 1 || n2.ops.Load() != 1 {
		t.Fatalf("failovers=%d n2 ops=%d, want 1/1", d.failovers.Load(), n2.ops.Load())
	}
	// The thread stays on its new home.
	if err := d.Insert(ctx, "t", "k", map[string][]byte{"f": nil}); err != nil || n2.ops.Load() != 2 {
		t.Fatalf("write after failover: err=%v n2 ops=%d", err, n2.ops.Load())
	}

	// A dead leader hint is dropped: the write fails over and follows MOVED.
	n3 := newFakeNode(t)
	n2.setHandle(follower(n3.addr))
	dead := n1.addr
	d.leader.Store(&dead)
	if err := d.Insert(ctx, "t", "k", map[string][]byte{"f": nil}); err != nil {
		t.Fatalf("write with dead leader hint: %v", err)
	}
	if l := d.leader.Load(); l == nil || *l != n3.addr || n3.ops.Load() != 1 {
		t.Fatalf("leader hint=%v n3 ops=%d", l, n3.ops.Load())
	}
	d.CleanupThread(ctx)
}

func TestSingleAddressUnchanged(t *testing.T) {
	n := newFakeNode(t)
	d := newDB(t, []string{n.addr})
	ctx := d.InitThread(context.Background(), 0, 1)
	if err := d.Insert(ctx, "t", "k", map[string][]byte{"f": []byte("v")}); err != nil {
		t.Fatal(err)
	}
	n.kill()
	time.Sleep(10 * time.Millisecond)
	if _, err := d.Read(ctx, "t", "k", nil); err == nil {
		t.Fatal("single address: a broken connection must surface, not be retried elsewhere")
	}
	if d.failovers.Load() != 0 || d.redirects.Load() != 0 {
		t.Fatalf("failovers=%d redirects=%d", d.failovers.Load(), d.redirects.Load())
	}
	d.CleanupThread(ctx)
}

func TestParseMoved(t *testing.T) {
	for in, want := range map[string]string{
		"put t:k: MOVED 10.0.0.1:9000 n1": "10.0.0.1:9000",
		"MOVED - (leader unknown, retry)": "",
	} {
		if got, ok := parseMoved(in); !ok || got != want {
			t.Fatalf("parseMoved(%q) = %q,%v", in, got, ok)
		}
	}
	if _, ok := parseMoved("put k: disk full"); ok {
		t.Fatal("not a redirect")
	}
}

// TestClusterLeaderKilled: the leader dies and the surviving followers keep
// naming it until the election ends — the write must wait it out (bounded by
// veltrixdb.retrytime), not give up after a fixed number of hops.
func TestClusterLeaderKilled(t *testing.T) {
	old, f1, f2 := newFakeNode(t), newFakeNode(t), newFakeNode(t)
	d := newDB(t, []string{old.addr, f1.addr, f2.addr}, "veltrixdb.retrytime", "2s")
	ctx := d.InitThread(context.Background(), 0, 1) // home = old leader
	if err := d.Insert(ctx, "t", "k", map[string][]byte{"f": nil}); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	elected := func(byte, string) (byte, []byte) {
		if calls.Add(1) <= 6 { // stale view of the dead leader for a while
			return 0x01, []byte("MOVED " + old.addr + " n1")
		}
		return 0x01, []byte("MOVED " + f2.addr + " n3")
	}
	f1.setHandle(elected)
	old.kill()
	time.Sleep(10 * time.Millisecond)
	if err := d.Insert(ctx, "t", "k2", map[string][]byte{"f": nil}); err != nil {
		t.Fatalf("write across leader loss: %v (redirects=%d failovers=%d)", err, d.redirects.Load(), d.failovers.Load())
	}
	if l := d.leader.Load(); l == nil || *l != f2.addr || d.opErrors.Load() != 0 {
		t.Fatalf("leader=%v errors=%d", l, d.opErrors.Load())
	}

	// Out of time: the error surfaces and is counted.
	d2 := newDB(t, []string{f1.addr, f2.addr}, "veltrixdb.retrytime", "30ms")
	f1.setHandle(func(byte, string) (byte, []byte) { return 0x01, []byte("MOVED - (leader unknown, retry)") })
	ctx2 := d2.InitThread(context.Background(), 0, 1)
	if err := d2.Insert(ctx2, "t", "k", map[string][]byte{"f": nil}); err == nil || d2.opErrors.Load() != 1 {
		t.Fatalf("err=%v errors=%d", err, d2.opErrors.Load())
	}
}

func TestDurationProperties(t *testing.T) {
	d := newDB(t, []string{"127.0.0.1:1"}, "veltrixdb.timeout", "250ms", "veltrixdb.retrytime", "7s")
	if d.timeout != 250*time.Millisecond || d.retryTime != 7*time.Second || d.backoff != 5*time.Millisecond {
		t.Fatalf("timeout=%v retrytime=%v backoff=%v", d.timeout, d.retryTime, d.backoff)
	}
	p := properties.NewProperties()
	p.Set("veltrixdb.timeout", "5")
	if _, err := (creator{}).Create(p); err == nil {
		t.Fatal("a duration without a unit must be rejected, not silently replaced by the default")
	}
}
