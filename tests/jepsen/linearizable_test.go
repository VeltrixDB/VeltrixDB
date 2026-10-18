package jepsen

// linearizable_test.go — a linearizability checker for single-key histories:
// the Wing & Gong search with Lowe's refinements (the algorithm Porcupine and
// Knossos' "wgl" checker implement):
//
//   - The history is a doubly linked list of call and return events in time
//     order. The search walks it: at a call it tries to linearize that op
//     (the model must accept it in the current state), lifts the op's call
//     and return out of the list and restarts from the head; at a return
//     whose op is not yet linearized it backtracks.
//   - Every (set of linearized ops, model state) pair reached is cached; a
//     pair seen before is a dead end and is not explored again. This is what
//     keeps the search tractable on real histories.
//
// Info ops (outcome unknown) get a return event at +∞: they may be
// linearized anywhere after their invocation, and an op placed after every
// other op constrains nothing — that is the "never happened" case. Failed
// ops and info reads are dropped before the search (they have no effect).
//
// Linearizability is local (Herlihy & Wing): a history is linearizable iff
// its per-key sub-histories are, so each key is checked on its own.

import (
	"fmt"
	"hash/fnv"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Model is a sequential specification of one key.
type Model struct {
	Name string
	Init string
	// Step applies op in state. ok=false means op cannot be linearized here.
	Step func(state string, op *Op) (ok bool, next string)
}

// RegisterModel: a register with read / write / compare-and-swap. The state
// is "" for an absent key and "=" + value otherwise.
var RegisterModel = Model{
	Name: "cas-register",
	Init: "",
	Step: func(s string, op *Op) (bool, string) {
		switch op.Kind {
		case KindWrite, KindAdd:
			return true, "=" + op.Value
		case KindRead:
			if !op.Found {
				return s == "", s
			}
			return s == "="+op.Value, s
		case KindCAS:
			match := s == "="+op.Expect
			if op.Outcome == Info {
				// Unknown result: it applies iff the expected value is current.
				if match {
					return true, "=" + op.Value
				}
				return true, s
			}
			switch op.CASRes {
			case CASOK:
				if match {
					return true, "=" + op.Value
				}
				return false, s
			case CASMismatch:
				return s != "" && !match, s
			case CASNotFound:
				return s == "", s
			}
			return false, s
		}
		return false, s
	},
}

// CounterModel: INCR returns the new value; a read returns the value (absent
// reads as 0). The state is the decimal value.
var CounterModel = Model{
	Name: "counter",
	Init: "0",
	Step: func(s string, op *Op) (bool, string) {
		v, _ := strconv.ParseInt(s, 10, 64)
		switch op.Kind {
		case KindIncr:
			n := v + op.Delta
			if op.Outcome == OK && op.IncrRes != n {
				return false, s
			}
			return true, strconv.FormatInt(n, 10)
		case KindRead:
			if !op.Found {
				return v == 0, s
			}
			r, err := strconv.ParseInt(op.Value, 10, 64)
			return err == nil && r == v, s
		}
		return false, s
	},
}

// Verdict of one check.
type Verdict int

const (
	Linearizable Verdict = iota
	NotLinearizable
	Unknown // the search ran out of time
)

func (v Verdict) String() string {
	return [...]string{"linearizable", "NOT linearizable", "unknown (timeout)"}[v]
}

// KeyResult is the outcome of checking one key.
type KeyResult struct {
	Key     string
	Verdict Verdict
	Ops     int // ops in the checked history
	Info    int // of which info
	Steps   int // search steps taken
	Elapsed time.Duration
	// Minimal: for NotLinearizable, a small sub-history that is itself not
	// linearizable (see shrink).
	Minimal []Op
	// Explain: where the search over Minimal got stuck — the state after
	// the longest partial linearization and the ops none of which can be
	// linearized next.
	Explain string
}

// prepare drops the ops that cannot constrain the search: failed ops, info
// reads, and (when reads=false) every read except the final ones.
func prepare(ops []Op, reads bool) []Op {
	var out []Op
	for _, o := range ops {
		if o.Outcome == Fail {
			continue
		}
		if o.Kind == KindRead && (o.Outcome == Info || (!reads && !o.Final)) {
			continue
		}
		out = append(out, o)
	}
	return out
}

// event is one entry in the search's linked list.
type event struct {
	op    *Op
	id    int    // op index; -1 for the head sentinel
	call  bool   // call event (match = its return) or return event
	match *event // call → its return
	time  int64
	prev  *event
	next  *event
}

type bitset []uint64

func newBitset(n int) bitset { return make(bitset, (n+63)/64) }

func (b bitset) set(i int)   { b[i/64] |= 1 << uint(i%64) }
func (b bitset) clear(i int) { b[i/64] &^= 1 << uint(i%64) }
func (b bitset) clone() bitset {
	return append(bitset(nil), b...)
}
func (b bitset) equal(o bitset) bool {
	for i := range b {
		if b[i] != o[i] {
			return false
		}
	}
	return true
}
func (b bitset) count() int {
	n := 0
	for _, w := range b {
		n += bits.OnesCount64(w)
	}
	return n
}

type cacheEntry struct {
	lin   bitset
	state string
}

func cacheKey(lin bitset, state string) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	for _, w := range lin {
		for i := 0; i < 8; i++ {
			buf[i] = byte(w >> (8 * i))
		}
		h.Write(buf[:])
	}
	h.Write([]byte(state))
	return h.Sum64()
}

// buildList makes the event list: one call and one return per op, sorted
// by time. At equal times calls sort before returns (the ops are then
// concurrent, which never produces a false "not linearizable").
func buildList(ops []Op) *event {
	evs := make([]*event, 0, 2*len(ops))
	for i := range ops {
		op := &ops[i]
		ret := &event{op: op, id: i, time: op.end()}
		call := &event{op: op, id: i, call: true, match: ret, time: op.Invoke}
		evs = append(evs, call, ret)
	}
	sort.SliceStable(evs, func(i, j int) bool {
		if evs[i].time != evs[j].time {
			return evs[i].time < evs[j].time
		}
		return evs[i].call && !evs[j].call
	})
	head := &event{id: -1}
	prev := head
	for _, e := range evs {
		prev.next = e
		e.prev = prev
		prev = e
	}
	return head
}

func lift(e *event) {
	e.prev.next = e.next
	e.next.prev = e.prev // a call is always followed by at least its return
	m := e.match
	m.prev.next = m.next
	if m.next != nil {
		m.next.prev = m.prev
	}
}

func unlift(e *event) {
	m := e.match
	m.prev.next = m
	if m.next != nil {
		m.next.prev = m
	}
	e.prev.next = e
	e.next.prev = e
}

// searchResult carries the verdict plus the longest partial linearization
// found (for the report).
type searchResult struct {
	verdict Verdict
	steps   int
	best    []int // op indexes, in linearization order, of the deepest stack
}

// wgl checks ops (already prepared) against m. deadline bounds the search.
func wgl(m Model, ops []Op, deadline time.Time) searchResult {
	n := len(ops)
	if n == 0 {
		return searchResult{verdict: Linearizable}
	}
	head := buildList(ops)
	lin := newBitset(n)
	cache := map[uint64][]cacheEntry{}
	type frame struct {
		e     *event
		state string
	}
	var stack []frame
	state := m.Init
	var best []int
	e := head.next
	steps := 0
	for head.next != nil {
		steps++
		if steps&1023 == 0 && time.Now().After(deadline) {
			return searchResult{verdict: Unknown, steps: steps, best: best}
		}
		if e.call {
			ok, next := m.Step(state, e.op)
			if ok {
				nl := lin.clone()
				nl.set(e.id)
				k := cacheKey(nl, next)
				seen := false
				for _, c := range cache[k] {
					if c.state == next && c.lin.equal(nl) {
						seen = true
						break
					}
				}
				if !seen {
					cache[k] = append(cache[k], cacheEntry{lin: nl, state: next})
					stack = append(stack, frame{e: e, state: state})
					if len(stack) > len(best) {
						best = best[:0]
						for _, f := range stack {
							best = append(best, f.e.id)
						}
					}
					state = next
					lin.set(e.id)
					lift(e)
					e = head.next
					continue
				}
			}
			e = e.next
			continue
		}
		// A return event of an op not yet linearized: backtrack.
		if len(stack) == 0 {
			return searchResult{verdict: NotLinearizable, steps: steps, best: best}
		}
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		state = top.state
		lin.clear(top.e.id)
		unlift(top.e)
		e = top.e.next
	}
	return searchResult{verdict: Linearizable, steps: steps}
}

// CheckKey checks one key's history (ops of a single key) against m.
// reads=false drops reads first (used where reads may be stale by design).
// The search gets timeout; a NotLinearizable verdict is shrunk to a minimal
// sub-history with the remaining budget (at most shrinkBudget).
func CheckKey(m Model, key string, ops []Op, reads bool, timeout time.Duration) KeyResult {
	t0 := time.Now()
	p := prepare(ops, reads)
	r := KeyResult{Key: key, Ops: len(p)}
	for _, o := range p {
		if o.Outcome == Info {
			r.Info++
		}
	}
	// Fast path: without the info ops nothing observes. Placing such an op
	// last ("never happened") turns any linearization of the reduced
	// history into one of the full history, so "linearizable" carries over.
	// Any other verdict is re-checked on the full history.
	var sr searchResult
	if red := dropUnobservedInfo(p); len(red) < len(p) {
		sr = wgl(m, red, t0.Add(timeout))
		if sr.verdict != Linearizable {
			steps := sr.steps
			sr = wgl(m, p, t0.Add(timeout))
			sr.steps += steps
		}
	} else {
		sr = wgl(m, p, t0.Add(timeout))
	}
	r.Verdict, r.Steps = sr.verdict, sr.steps
	if r.Verdict == NotLinearizable {
		r.Minimal = shrink(m, p, 30*time.Second)
		r.Explain = explain(m, r.Minimal)
	}
	r.Elapsed = time.Since(t0)
	return r
}

// dropUnobservedInfo removes info writes / CASes whose value no other op
// reads or expects (register keys only; counter ops are kept).
func dropUnobservedInfo(ops []Op) []Op {
	used := map[string]int{}
	for _, o := range ops {
		if o.Kind == KindRead && o.Outcome == OK && o.Found {
			used[o.Value]++
		}
		if o.Kind == KindCAS {
			used[o.Expect]++
		}
	}
	var out []Op
	for _, o := range ops {
		if o.Outcome == Info && (o.Kind == KindWrite || o.Kind == KindAdd || o.Kind == KindCAS) && used[o.Value] == 0 {
			continue
		}
		out = append(out, o)
	}
	return out
}

// notLin reports whether ops is (provably, within the per-check budget) not
// linearizable.
func notLin(m Model, ops []Op, per time.Duration) bool {
	return wgl(m, ops, time.Now().Add(per)).verdict == NotLinearizable
}

// prefixAt returns the history as it stood at time t: ops invoked at or
// before t; an op that had not completed by t becomes info if it mutates and
// is dropped if it is a pure read. Linearizability is prefix-closed, so a
// non-linearizable prefix proves the whole history non-linearizable.
func prefixAt(ops []Op, t int64) []Op {
	var out []Op
	for _, o := range ops {
		if o.Invoke > t {
			continue
		}
		if o.end() > t && o.Outcome != Info {
			if o.Kind == KindRead {
				continue
			}
			o.Outcome = Info
			o.CASRes, o.IncrRes = "", 0
		}
		out = append(out, o)
	}
	return out
}

// isObservation: an op that never changes the state when it succeeds — a
// read, or a CAS that did not match. Removing one from a history can only
// add linearizations, so if the history without it is still not
// linearizable, so is the original.
func isObservation(o Op) bool {
	if o.Outcome != OK {
		return false
	}
	return o.Kind == KindRead || (o.Kind == KindCAS && o.CASRes != CASOK)
}

// shrink finds a small sub-history that is itself not linearizable and is
// implied by the original: (1) the shortest failing prefix (binary search
// over event times — monotone because linearizability is prefix-closed),
// then (2) greedily drop observations (reads, failed CASes) that are not
// needed for the failure. Writes are never dropped: removing a write could
// make a read unexplainable, and the result would prove nothing.
func shrink(m Model, ops []Op, budget time.Duration) []Op {
	deadline := time.Now().Add(budget)
	per := 2 * time.Second
	var times []int64
	for _, o := range ops {
		times = append(times, o.Invoke)
		if o.Outcome != Info {
			times = append(times, o.Complete)
		}
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	best := ops
	lo, hi := 0, len(times)-1
	for lo < hi && time.Now().Before(deadline) {
		mid := (lo + hi) / 2
		if p := prefixAt(ops, times[mid]); notLin(m, p, per) {
			hi = mid
			best = p
		} else {
			lo = mid + 1
		}
	}
	if p := prefixAt(ops, times[hi]); len(p) < len(best) && notLin(m, p, per) {
		best = p
	}
	for i := len(best) - 1; i >= 0 && time.Now().Before(deadline); i-- {
		if !isObservation(best[i]) {
			continue
		}
		cand := append(append([]Op(nil), best[:i]...), best[i+1:]...)
		if notLin(m, cand, per) {
			best = cand
		}
	}
	return best
}

// explain replays the deepest partial linearization of ops and lists the
// frontier: the unlinearized ops invoked before the earliest return among
// them — the search had to linearize one of these next and none fits.
func explain(m Model, ops []Op) string {
	sr := wgl(m, ops, time.Now().Add(5*time.Second))
	if sr.verdict != NotLinearizable {
		return ""
	}
	state := m.Init
	done := map[int]bool{}
	for _, i := range sr.best {
		_, state = m.Step(state, &ops[i])
		done[i] = true
	}
	minRet := int64(1<<63 - 1)
	for i := range ops {
		if !done[i] && ops[i].end() < minRet {
			minRet = ops[i].end()
		}
	}
	var front []Op
	for i := range ops {
		if !done[i] && ops[i].Invoke <= minRet {
			front = append(front, ops[i])
		}
	}
	st := "(absent)"
	if m.Name == CounterModel.Name || state != "" {
		st = strings.TrimPrefix(state, "=")
	}
	return fmt.Sprintf("after linearizing %d of %d ops the %s holds %s; none of these can be linearized next:\n%s",
		len(sr.best), len(ops), m.Name, st, formatOps(front))
}

// FormatResult renders a failing key result for the test log: the
// explanation and the minimal sub-history (its last 60 ops when longer; the
// whole of it is saved as JSON).
func FormatResult(r KeyResult) string {
	shown := r.Minimal
	note := ""
	if len(shown) > 60 {
		note = fmt.Sprintf("    … %d earlier ops omitted (all in the saved JSON)\n", len(shown)-60)
		shown = shown[len(shown)-60:]
	}
	return fmt.Sprintf("key %s: %s (%d ops, %d info, %d steps, %v)\n  %s\n  minimal failing sub-history (%d ops):\n%s%s",
		r.Key, r.Verdict, r.Ops, r.Info, r.Steps, r.Elapsed.Round(time.Millisecond), r.Explain, len(r.Minimal), note, formatOps(shown))
}
