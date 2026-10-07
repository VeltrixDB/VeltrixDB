// Package ycsbdriver registers VeltrixDB as a go-ycsb database ("veltrixdb"),
// so the same YCSB workloads and runner drive VeltrixDB, Aerospike
// (go-ycsb's "aerospike") and ScyllaDB (go-ycsb's "cassandra", CQL).
//
// A YCSB record (field → value) is stored as ONE key whose value is the
// fields encoded as [2B nameLen][name][4B valueLen][value]…, so a read or an
// insert is one round trip, as for the other two databases. An update is
// read-modify-write — a GET then a PUT — because VeltrixDB has no server-side
// field merge on plain keys (go-ycsb's aerospike driver does the same; its
// cassandra driver issues one UPDATE). Scans (workload E) use RANGE over the
// ordered key index.
//
// # Clusters
//
// veltrixdb.addr may list several nodes ("h1:9000,h2:9000,h3:9000"). Every
// YCSB thread has its own connection(s):
//
//   - Reads go to the thread's home node. With veltrixdb.spread=true (default)
//     homes are assigned round-robin over the list, so reads are spread over
//     the cluster; spread=false homes every thread on the first address.
//   - Writes go to the thread's home node until some thread learns the raft
//     leader from a "MOVED <leaderAddr> <leaderID>" reply; from then on every
//     thread sends writes straight to that leader (one shared hint), so a raft
//     run costs about one redirect per thread, not one per write. In
//     --mode=replicated no node answers MOVED, so writes stay spread too.
//   - Any op answered with MOVED is retried at the named node; "MOVED -"
//     (leader unknown, election in progress) is retried at the same node after
//     a back-off. A connection error drops that connection and fails over to
//     the next listed node (immediately the first time, then with back-off).
//     MOVED hops are bounded by veltrixdb.retries, waits by veltrixdb.retrytime.
//   - Close prints one "# veltrixdb:" line with the redirect / failover /
//     error counts, the last error, and the reads and writes each node answered
//     (an update's GET counts as a read), so a run that was secretly
//     all redirects, or all on one node, is visible in the raw output.
//
// With a single address and a standalone server the behaviour is the old one:
// one connection per thread, no retries, nothing printed at Close.
//
// Raft-mode reads on a follower are local and may be stale unless the servers
// run with --linearizable-reads (then followers answer MOVED for reads too and
// every read ends up on the leader).
//
// Properties:
//
//	veltrixdb.addr     comma-separated host:port list of the binary protocol (default 127.0.0.1:9000)
//	veltrixdb.spread   spread thread home nodes round-robin over the list (default true)
//	veltrixdb.retries    max MOVED hops in a row without a wait, per op (default 3)
//	veltrixdb.retrytime  no new wait (MOVED -, repeated failover) after this long in one op (default 3s;
//	                     raft elections take 0.4–0.8 s plus failure detection)
//	veltrixdb.backoff    first wait, doubled per wait up to 1s (default 50ms)
//	veltrixdb.timeout  dial timeout (default 5s)
package ycsbdriver

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"

	"github.com/VeltrixDB/veltrixdb/client"
)

type creator struct{}

type db struct {
	addrs     []string
	spread    bool
	retries   int
	retryTime time.Duration
	backoff   time.Duration
	timeout   time.Duration

	nextHome atomic.Uint64
	leader   atomic.Pointer[string] // raft leader learned from MOVED; nil = unknown

	redirects     atomic.Int64 // MOVED <addr> followed
	leaderUnknown atomic.Int64 // MOVED - (waited and retried)
	failovers     atomic.Int64 // connection errors that moved an op to another node
	opErrors      atomic.Int64 // ops that failed after all of the above
	lastErr       atomic.Value // string: the last such error

	mu     sync.Mutex
	reads  map[string]int64 // successful reads per node (merged at CleanupThread)
	writes map[string]int64
	homes  map[string]int // threads homed per node
}

type ctxKey struct{}

// thread is one YCSB worker's connection state.
type thread struct {
	d        *db
	readAddr string
	conns    map[string]*client.BinaryConn
	reads    map[string]int64
	writes   map[string]int64
}

func init() { ycsb.RegisterDBCreator("veltrixdb", creator{}) }

func (creator) Create(p *properties.Properties) (ycsb.DB, error) {
	var addrs []string
	for _, a := range strings.Split(p.GetString("veltrixdb.addr", "127.0.0.1:9000"), ",") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 {
		return nil, errors.New("veltrixdb.addr: no address")
	}
	d := &db{
		addrs:   addrs,
		spread:  p.GetBool("veltrixdb.spread", true),
		retries: p.GetInt("veltrixdb.retries", 3),
		reads:   map[string]int64{},
		writes:  map[string]int64{},
		homes:   map[string]int{},
	}
	for _, o := range []struct {
		key string
		def time.Duration
		dst *time.Duration
	}{
		{"veltrixdb.retrytime", 3 * time.Second, &d.retryTime},
		{"veltrixdb.backoff", 50 * time.Millisecond, &d.backoff},
		{"veltrixdb.timeout", 5 * time.Second, &d.timeout},
	} {
		// GetDuration would read "5s" as an invalid int64 and silently use
		// the default; parse it as a Go duration and refuse a bad value.
		*o.dst = o.def
		if v, ok := p.Get(o.key); ok {
			dur, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", o.key, err)
			}
			*o.dst = dur
		}
	}
	return d, nil
}

// Close prints the routing summary for multi-node runs (or whenever anything
// was redirected or failed over).
func (d *db) Close() error {
	r, u, f := d.redirects.Load(), d.leaderUnknown.Load(), d.failovers.Load()
	if len(d.addrs) == 1 && r == 0 && u == 0 && f == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	last, _ := d.lastErr.Load().(string)
	fmt.Printf("# veltrixdb: nodes=%d spread=%v redirects=%d leader_unknown_retries=%d failovers=%d errors=%d homes=%s reads=%s writes=%s",
		len(d.addrs), d.spread, r, u, f, d.opErrors.Load(), fmtMap(d.homes), fmtMap(d.reads), fmtMap(d.writes))
	if last != "" {
		fmt.Printf(" last_error=%q", last)
	}
	fmt.Println()
	return nil
}

func fmtMap[V int | int64](m map[string]V) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s:%d", k, m[k])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// InitThread gives every YCSB worker its own connection to its home node
// (the next node round-robin, or the first one with spread=false). A home
// that cannot be dialled fails over to the next listed node.
func (d *db) InitThread(ctx context.Context, _ int, _ int) context.Context {
	start := 0
	if d.spread {
		start = int(d.nextHome.Add(1)-1) % len(d.addrs)
	}
	t := &thread{d: d, conns: map[string]*client.BinaryConn{}, reads: map[string]int64{}, writes: map[string]int64{}}
	var firstErr error
	for i := 0; i < len(d.addrs); i++ {
		a := d.addrs[(start+i)%len(d.addrs)]
		if _, err := t.conn(a); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if len(d.addrs) > 1 {
				d.failovers.Add(1)
			}
			continue
		}
		t.readAddr = a
		break
	}
	if t.readAddr == "" {
		panic(fmt.Sprintf("veltrixdb: dial %s: %v", strings.Join(d.addrs, ","), firstErr))
	}
	d.mu.Lock()
	d.homes[t.readAddr]++
	d.mu.Unlock()
	return context.WithValue(ctx, ctxKey{}, t)
}

func (d *db) CleanupThread(ctx context.Context) {
	t := state(ctx)
	for _, c := range t.conns {
		c.Close()
	}
	d.mu.Lock()
	for a, n := range t.reads {
		d.reads[a] += n
	}
	for a, n := range t.writes {
		d.writes[a] += n
	}
	d.mu.Unlock()
}

func state(ctx context.Context) *thread { return ctx.Value(ctxKey{}).(*thread) }

// conn returns the thread's connection to addr, dialling it on first use.
func (t *thread) conn(addr string) (*client.BinaryConn, error) {
	if c, ok := t.conns[addr]; ok {
		return c, nil
	}
	c, err := client.DialBinary(addr, t.d.timeout)
	if err != nil {
		return nil, err
	}
	t.conns[addr] = c
	return c, nil
}

func (t *thread) drop(addr string) {
	if c, ok := t.conns[addr]; ok {
		c.Close()
		delete(t.conns, addr)
	}
}

// nextAddr is the listed node after addr (after the thread's home when addr
// is not in the list, e.g. a leader address learned from MOVED).
func (t *thread) nextAddr(addr string) string {
	for i, a := range t.d.addrs {
		if a == addr {
			return t.d.addrs[(i+1)%len(t.d.addrs)]
		}
	}
	for i, a := range t.d.addrs {
		if a == t.readAddr {
			return t.d.addrs[(i+1)%len(t.d.addrs)]
		}
	}
	return t.d.addrs[0]
}

func (t *thread) target(write bool) string {
	if write {
		if l := t.d.leader.Load(); l != nil {
			return *l
		}
	}
	return t.readAddr
}

// do runs op at the right node, following MOVED and failing over on
// connection errors. Two bounds: at most d.retries MOVED hops in a row
// without a wait (a redirect loop), and no new wait once d.retryTime has
// passed since the op started (a failover or election that does not settle).
// A failed op is counted and its error kept for the Close summary.
func (t *thread) do(write bool, op func(*client.BinaryConn) error) error {
	d := t.d
	addr := t.target(write)
	start := time.Now()
	backoff := d.backoff
	hops, fails := 0, 0
	// wait sleeps before the next attempt; false = out of time, give up.
	wait := func() bool {
		if time.Since(start)+backoff > d.retryTime {
			return false
		}
		time.Sleep(backoff)
		if backoff < time.Second {
			backoff *= 2
		}
		hops = 0
		return true
	}
	fail := func(err error) error {
		d.opErrors.Add(1)
		d.lastErr.Store(err.Error())
		return err
	}
	for {
		c, err := t.conn(addr)
		if err == nil {
			if err = op(c); err == nil {
				if write {
					t.writes[addr]++
				} else {
					t.reads[addr]++
				}
				return nil
			}
		}
		if leader, moved := parseMoved(err.Error()); moved {
			if leader == "" { // election in progress: same node, later
				d.leaderUnknown.Add(1)
				if !wait() {
					return fail(err)
				}
				continue
			}
			if hops++; hops > d.retries {
				return fail(err)
			}
			d.redirects.Add(1)
			if write {
				d.leader.Store(&leader)
			} else {
				t.readAddr = leader
			}
			addr = leader
			continue
		}
		if !isConnErr(err) || len(d.addrs) == 1 {
			return fail(err) // server error, or a single address: no failover, as before
		}
		// The node is gone (or the connection broke): next listed node. The
		// first failover is immediate, later ones back off (a follower may
		// still name the dead leader until the election finishes).
		t.drop(addr)
		next := t.nextAddr(addr)
		if l := d.leader.Load(); l != nil && *l == addr {
			d.leader.CompareAndSwap(l, nil)
		}
		if addr == t.readAddr {
			t.readAddr = next
		}
		if fails++; fails > 1 && !wait() {
			return fail(err)
		}
		d.failovers.Add(1)
		addr = next
	}
}

// parseMoved extracts the redirect target from a "… MOVED <addr> <id>" error
// (client.parseMoved is unexported). addr is "" for "MOVED -".
func parseMoved(msg string) (string, bool) {
	i := strings.Index(msg, "MOVED ")
	if i < 0 {
		return "", false
	}
	f := strings.Fields(msg[i+len("MOVED "):])
	if len(f) == 0 || f[0] == "-" {
		return "", true
	}
	return f[0], true
}

func isConnErr(err error) bool {
	var ne net.Error
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, os.ErrDeadlineExceeded) || errors.As(err, &ne)
}

func key(table, k string) string { return table + ":" + k }

func encode(values map[string][]byte) []byte {
	n := 0
	for f, v := range values {
		n += 6 + len(f) + len(v)
	}
	out := make([]byte, 0, n)
	var b [4]byte
	for f, v := range values {
		binary.LittleEndian.PutUint16(b[:2], uint16(len(f)))
		out = append(out, b[:2]...)
		out = append(out, f...)
		binary.LittleEndian.PutUint32(b[:], uint32(len(v)))
		out = append(out, b[:]...)
		out = append(out, v...)
	}
	return out
}

func decode(buf []byte, fields []string) (map[string][]byte, error) {
	want := map[string]bool{}
	for _, f := range fields {
		want[f] = true
	}
	out := map[string][]byte{}
	for len(buf) > 0 {
		if len(buf) < 2 {
			return nil, errors.New("veltrixdb: truncated record")
		}
		fl := int(binary.LittleEndian.Uint16(buf))
		buf = buf[2:]
		if len(buf) < fl+4 {
			return nil, errors.New("veltrixdb: truncated record")
		}
		f := string(buf[:fl])
		vl := int(binary.LittleEndian.Uint32(buf[fl:]))
		buf = buf[fl+4:]
		if len(buf) < vl {
			return nil, errors.New("veltrixdb: truncated record")
		}
		if len(want) == 0 || want[f] {
			out[f] = buf[:vl]
		}
		buf = buf[vl:]
	}
	return out, nil
}

func (d *db) get(t *thread, k string) (v []byte, err error) {
	err = t.do(false, func(c *client.BinaryConn) error {
		v, err = c.Get(k)
		return err
	})
	return v, err
}

func (d *db) Read(ctx context.Context, table, k string, fields []string) (map[string][]byte, error) {
	v, err := d.get(state(ctx), key(table, k))
	if err != nil {
		return nil, err
	}
	return decode(v, fields)
}

func (d *db) Scan(ctx context.Context, table, start string, count int, fields []string) ([]map[string][]byte, error) {
	var kvs []client.KV
	err := state(ctx).do(false, func(c *client.BinaryConn) (err error) {
		kvs, err = c.RangeScan(key(table, start), table+";", count, false) // ';' = ':'+1
		return err
	})
	if err != nil {
		return nil, err
	}
	out := make([]map[string][]byte, 0, len(kvs))
	for _, kv := range kvs {
		m, err := decode(kv.Value, fields)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func (d *db) Update(ctx context.Context, table, k string, values map[string][]byte) error {
	t := state(ctx)
	old, err := d.get(t, key(table, k))
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "not found") {
		return err
	}
	merged := map[string][]byte{}
	if err == nil {
		if merged, err = decode(old, nil); err != nil {
			return err
		}
	}
	for f, v := range values {
		merged[f] = v
	}
	return d.put(t, key(table, k), encode(merged))
}

func (d *db) put(t *thread, k string, v []byte) error {
	return t.do(true, func(c *client.BinaryConn) error { return c.Put(k, v, 0) })
}

func (d *db) Insert(ctx context.Context, table, k string, values map[string][]byte) error {
	return d.put(state(ctx), key(table, k), encode(values))
}

func (d *db) Delete(ctx context.Context, table, k string) error {
	return state(ctx).do(true, func(c *client.BinaryConn) error { return c.Delete(key(table, k)) })
}
