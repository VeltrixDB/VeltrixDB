package upgrade_test

// workload_test.go — the writes the scenarios load and keep running: plain
// PUTs, deletes, INCR counters and TTL keys, each recorded in the model with
// its outcome (acknowledged or unknown).

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// writer is one client connection's way of sending a write: raftClient
// (follows MOVED) or directClient (one node).
type writer interface {
	write(cmd string, isOK func(string) bool, timeout time.Duration) (string, outcome)
	close()
}

// directClient talks to one node only (standalone, replicated mode).
type directClient struct {
	n *node
	c *conn
}

func (d *directClient) close() {
	if d.c != nil {
		d.c.close()
		d.c = nil
	}
}

func (d *directClient) write(cmd string, isOK func(string) bool, timeout time.Duration) (string, outcome) {
	deadline := time.Now().Add(timeout)
	for d.c == nil {
		c, err := dial(d.n.addr())
		if err == nil {
			d.c = c
			break
		}
		if time.Now().After(deadline) {
			return "dial: " + err.Error(), unknown // never sent
		}
		time.Sleep(100 * time.Millisecond)
	}
	resp, err := d.c.do(cmd)
	if err != nil {
		d.close()
		return err.Error(), unknown
	}
	if isOK(resp) {
		return resp, acked
	}
	return resp, unknown
}

// op is one scripted write.
type op struct {
	kind  byte // 'P' PUT, 'X' PUTEX, 'H' HSET with TTL, 'D' DEL, 'I' INCR
	key   string
	val   string
	ttl   int
	delta int64
}

func (o op) cmd() string {
	switch o.kind {
	case 'P':
		return "PUT " + o.key + " " + o.val
	case 'X':
		return fmt.Sprintf("PUTEX %s %d %s", o.key, o.ttl, o.val)
	case 'H':
		return fmt.Sprintf("HSET %s f %s %d", o.key, o.val, o.ttl)
	case 'D':
		return "DEL " + o.key
	case 'I':
		return fmt.Sprintf("INCR %s %d", o.key, o.delta)
	}
	panic("bad op")
}

// apply sends o and records it in m. Idempotent ops are retried on an unknown
// outcome; INCR is not (a retry could count twice).
func apply(w writer, m *model, o op) outcome {
	isOK := isOKReply
	tries := 3
	if o.kind == 'I' {
		isOK, tries = isNumReply, 1
	}
	var out outcome
	for i := 0; i < tries; i++ {
		_, out = w.write(o.cmd(), isOK, 30*time.Second)
		if out == acked {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	switch o.kind {
	case 'P':
		m.put(o.key, o.val, out)
	case 'X':
		m.put(o.key, o.val, out)
		m.mu.Lock()
		if out == acked {
			if strings.Contains(o.key, "\x01") {
				m.ttlKeys[o.key] = true
			}
			if o.ttl < 600 {
				m.expKeys[o.key] = time.Now().Add(time.Duration(o.ttl) * time.Second)
			}
		}
		m.mu.Unlock()
	case 'H':
		// The field lives in the engine as key\x01f; HTTL reads its TTL.
		m.put(o.key+"\x01f", o.val, out)
		m.mu.Lock()
		if out == acked {
			m.ttlKeys[o.key+"\x01f"] = true
		}
		m.mu.Unlock()
	case 'D':
		m.del(o.key, out)
	case 'I':
		m.incr(o.key, o.delta, out)
	}
	return out
}

// runOps applies ops with `workers` connections. Ops on one key always go to
// the same worker, in order, so "PUT k" then "DEL k" keeps its order.
func runOps(t *testing.T, newW func() writer, m *model, workers int, ops []op) (ackedN, unknownN int) {
	t.Helper()
	queues := make([][]op, workers)
	for _, o := range ops {
		h := fnv.New32a()
		h.Write([]byte(o.key))
		q := int(h.Sum32() % uint32(workers))
		queues[q] = append(queues[q], o)
	}
	var a, u int64
	var wg sync.WaitGroup
	for _, q := range queues {
		wg.Add(1)
		go func(q []op) {
			defer wg.Done()
			w := newW()
			defer w.close()
			for _, o := range q {
				if apply(w, m, o) == acked {
					atomic.AddInt64(&a, 1)
				} else {
					atomic.AddInt64(&u, 1)
				}
			}
		}(q)
	}
	wg.Wait()
	return int(a), int(u)
}

// baseOps is the standard data set under prefix p: n keys, every 10th
// deleted, 16 counters, TTL probes (PUTEX on "p/ttl/i\x01t" and HSET "p/httl/i"
// with a 1-day TTL, read back with HTTL) and short-TTL keys that must expire.
func baseOps(p string, n int, shortTTL int) []op {
	var ops []op
	for i := 0; i < n; i++ {
		ops = append(ops, op{kind: 'P', key: fmt.Sprintf("%s/k/%05d", p, i), val: fmt.Sprintf("v%d", i)})
	}
	for i := 0; i < n; i += 10 {
		ops = append(ops, op{kind: 'D', key: fmt.Sprintf("%s/k/%05d", p, i)})
	}
	for i := 0; i < n/10; i++ {
		ops = append(ops, op{kind: 'I', key: fmt.Sprintf("%s/ctr/%02d", p, i%16), delta: int64(1 + i%3)})
	}
	for i := 0; i < 50; i++ {
		ops = append(ops,
			op{kind: 'X', key: fmt.Sprintf("%s/ttl/%03d\x01t", p, i), ttl: 86400, val: "t" + strconv.Itoa(i)},
			op{kind: 'H', key: fmt.Sprintf("%s/httl/%03d", p, i), ttl: 86400, val: "h" + strconv.Itoa(i)})
	}
	if shortTTL > 0 {
		for i := 0; i < 20; i++ {
			ops = append(ops, op{kind: 'X', key: fmt.Sprintf("%s/exp/%03d", p, i), ttl: shortTTL, val: "e"})
		}
	}
	return ops
}

// fillerOps are plain PUTs used to push a raft log past the snapshot threshold.
func fillerOps(p string, from, n int) []op {
	ops := make([]op, 0, n)
	for i := from; i < from+n; i++ {
		ops = append(ops, op{kind: 'P', key: fmt.Sprintf("%s/fill/%06d", p, i), val: "f"})
	}
	return ops
}

// background keeps writing while nodes are swapped: PUT a new key, every 3rd
// op an INCR, every 5th a DEL of a key it wrote two ops earlier.
type background struct {
	stopC   chan struct{}
	done    chan struct{}
	acked   int64
	unknown int64
	maxGap  time.Duration
	errs    []string
	mu      sync.Mutex
}

func startBackground(newW func() writer, m *model, prefix string) *background {
	b := &background{stopC: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(b.done)
		w := newW()
		defer w.close()
		lastAck := time.Now()
		for i := 0; ; i++ {
			select {
			case <-b.stopC:
				return
			default:
			}
			var o op
			switch {
			case i%5 == 4:
				o = op{kind: 'D', key: fmt.Sprintf("%s/%06d", prefix, i-2)}
			case i%3 == 2:
				o = op{kind: 'I', key: prefix + "/ctr", delta: 1}
			default:
				o = op{kind: 'P', key: fmt.Sprintf("%s/%06d", prefix, i), val: "b" + strconv.Itoa(i)}
			}
			isOK := isOKReply
			if o.kind == 'I' {
				isOK = isNumReply
			}
			resp, out := w.write(o.cmd(), isOK, 20*time.Second)
			b.record(m, o, resp, out, &lastAck)
			time.Sleep(2 * time.Millisecond)
		}
	}()
	return b
}

func (b *background) record(m *model, o op, resp string, out outcome, lastAck *time.Time) {
	switch o.kind {
	case 'P':
		m.put(o.key, o.val, out)
	case 'D':
		m.del(o.key, out)
	case 'I':
		m.incr(o.key, o.delta, out)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if out == acked {
		b.acked++
		if g := time.Since(*lastAck); g > b.maxGap {
			b.maxGap = g
		}
		*lastAck = time.Now()
	} else {
		b.unknown++
		if len(b.errs) < 20 {
			b.errs = append(b.errs, fmt.Sprintf("%s %q → %q", time.Now().Format("15:04:05.000"), o.cmd(), resp))
		}
	}
}

func (b *background) stop(t *testing.T) {
	t.Helper()
	close(b.stopC)
	<-b.done
	b.mu.Lock()
	defer b.mu.Unlock()
	t.Logf("background writer: %d acknowledged, %d unknown outcome, longest gap between acknowledgements %s",
		b.acked, b.unknown, b.maxGap.Round(time.Millisecond))
	for _, e := range b.errs {
		t.Logf("  unacknowledged: %s", e)
	}
}
