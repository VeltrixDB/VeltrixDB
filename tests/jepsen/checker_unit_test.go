package jepsen

// checker_unit_test.go — validates the checkers on hand-written histories
// whose verdict is known, and on randomly generated histories that are
// linearizable by construction (each op takes effect at a random instant
// inside its interval). These run in -short mode and need no server.

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

const inf = -1 // marks an info op in the builders below

func w(key, v string, inv, cmp int64) Op {
	return mk(Op{Kind: KindWrite, Key: key, Value: v}, inv, cmp)
}

func r(key, v string, inv, cmp int64) Op { // v == "" → absent
	return mk(Op{Kind: KindRead, Key: key, Value: v, Found: v != ""}, inv, cmp)
}

func cas(key, exp, v, res string, inv, cmp int64) Op {
	return mk(Op{Kind: KindCAS, Key: key, Expect: exp, Value: v, CASRes: res}, inv, cmp)
}

func incr(key string, res int64, inv, cmp int64) Op {
	return mk(Op{Kind: KindIncr, Key: key, Delta: 1, IncrRes: res}, inv, cmp)
}

func mk(o Op, inv, cmp int64) Op {
	o.Invoke = inv
	o.Outcome = OK
	o.Complete = cmp
	if cmp == inf {
		o.Outcome = Info
		o.Complete = inv + 1
		o.CASRes, o.IncrRes = "", 0
	}
	return o
}

func failed(o Op) Op { o.Outcome = Fail; return o }

func number(ops []Op) []Op {
	for i := range ops {
		ops[i].ID = i
	}
	return ops
}

func TestChecker_KnownHistories(t *testing.T) {
	cases := []struct {
		name  string
		model Model
		ops   []Op
		want  Verdict
	}{
		{"empty", RegisterModel, nil, Linearizable},
		{"sequential write read cas read", RegisterModel, []Op{
			w("k", "a", 0, 1), r("k", "a", 2, 3), cas("k", "a", "b", CASOK, 4, 5), r("k", "b", 6, 7),
		}, Linearizable},
		{"read of absent key before any write", RegisterModel, []Op{
			r("k", "", 0, 1), w("k", "a", 2, 3), r("k", "a", 4, 5),
		}, Linearizable},
		{"read concurrent with a write may see old or new", RegisterModel, []Op{
			w("k", "a", 0, 1), w("k", "b", 2, 10), r("k", "a", 3, 4), r("k", "b", 5, 6),
		}, Linearizable},
		{"concurrent reads see new then old (no single point)", RegisterModel, []Op{
			w("k", "a", 0, 1), w("k", "b", 2, 10), r("k", "b", 3, 4), r("k", "a", 5, 6),
		}, NotLinearizable},
		{"stale read after a completed write", RegisterModel, []Op{
			w("k", "a", 0, 1), w("k", "b", 2, 3), r("k", "a", 4, 5),
		}, NotLinearizable},
		{"read of a value never written", RegisterModel, []Op{
			w("k", "a", 0, 1), r("k", "z", 2, 3),
		}, NotLinearizable},
		{"lost acknowledged write: absent after write", RegisterModel, []Op{
			w("k", "a", 0, 1), r("k", "", 2, 3),
		}, NotLinearizable},
		{"info write may take effect long after invoke", RegisterModel, []Op{
			w("k", "a", 0, 1), w("k", "b", 2, inf), r("k", "a", 10, 11), r("k", "b", 20, 21),
		}, Linearizable},
		{"info write may never take effect", RegisterModel, []Op{
			w("k", "a", 0, 1), w("k", "b", 2, inf), r("k", "a", 10, 11),
		}, Linearizable},
		{"info write cannot take effect before its invoke", RegisterModel, []Op{
			r("k", "b", 0, 1), w("k", "b", 2, inf),
		}, NotLinearizable},
		{"info write takes effect once: new, then old again", RegisterModel, []Op{
			w("k", "a", 0, 1), w("k", "b", 2, inf), r("k", "b", 3, 4), r("k", "a", 5, 6),
		}, NotLinearizable},
		{"failed write is ignored, so its value cannot be read", RegisterModel, []Op{
			w("k", "a", 0, 1), failed(w("k", "b", 2, 3)), r("k", "b", 4, 5),
		}, NotLinearizable},
		{"info read is ignored", RegisterModel, []Op{
			w("k", "a", 0, 1), mk(Op{Kind: KindRead, Key: "k", Value: "zzz", Found: true}, 2, inf),
		}, Linearizable},
		{"two CASes from the same value both succeed", RegisterModel, []Op{
			w("k", "a", 0, 1), cas("k", "a", "b", CASOK, 2, 5), cas("k", "a", "c", CASOK, 3, 6),
		}, NotLinearizable},
		{"concurrent CASes chain", RegisterModel, []Op{
			w("k", "a", 0, 1), cas("k", "b", "c", CASOK, 2, 6), cas("k", "a", "b", CASOK, 3, 5), r("k", "c", 7, 8),
		}, Linearizable},
		{"CAS mismatch while the value matched", RegisterModel, []Op{
			w("k", "a", 0, 1), cas("k", "a", "b", CASMismatch, 2, 3),
		}, NotLinearizable},
		{"CAS mismatch concurrent with the write that made it match", RegisterModel, []Op{
			w("k", "x", 0, 1), w("k", "a", 2, 6), cas("k", "a", "b", CASMismatch, 3, 4),
		}, Linearizable},
		{"CAS notfound after a write", RegisterModel, []Op{
			w("k", "a", 0, 1), cas("k", "a", "b", CASNotFound, 2, 3),
		}, NotLinearizable},
		{"CAS notfound on an absent key", RegisterModel, []Op{
			cas("k", "a", "b", CASNotFound, 0, 1), w("k", "a", 2, 3),
		}, Linearizable},
		{"info CAS explains a later read", RegisterModel, []Op{
			w("k", "a", 0, 1), cas("k", "a", "b", "", 2, inf), r("k", "b", 5, 6),
		}, Linearizable},
		{"info CAS applies at most once", RegisterModel, []Op{
			w("k", "a", 0, 1), cas("k", "a", "b", "", 2, inf), r("k", "b", 5, 6), r("k", "a", 7, 8), w("k", "c", 9, 10), r("k", "c", 11, 12),
		}, NotLinearizable},
		{"info CAS must apply if linearized while expected value current", RegisterModel, []Op{
			// The only way to read b is the info CAS, but the CAS expected a
			// and the key held x for its whole lifetime.
			w("k", "x", 0, 1), cas("k", "a", "b", "", 2, inf), r("k", "b", 5, 6),
		}, NotLinearizable},
		{"counter sequential", CounterModel, []Op{
			incr("c", 1, 0, 1), incr("c", 2, 2, 3), r("c", "2", 4, 5),
		}, Linearizable},
		{"counter concurrent increments in either order", CounterModel, []Op{
			incr("c", 2, 0, 5), incr("c", 1, 1, 4),
		}, Linearizable},
		{"counter duplicate result", CounterModel, []Op{
			incr("c", 1, 0, 5), incr("c", 1, 1, 4),
		}, NotLinearizable},
		{"counter info increment counted once at most", CounterModel, []Op{
			incr("c", 1, 0, 1), incr("c", 0, 2, inf), r("c", "2", 5, 6), incr("c", 3, 7, 8),
		}, Linearizable},
		{"counter info increment cannot apply twice", CounterModel, []Op{
			incr("c", 1, 0, 1), incr("c", 0, 2, inf), r("c", "3", 5, 6),
		}, NotLinearizable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops := number(tc.ops)
			res := CheckKey(tc.model, "k", ops, true, 5*time.Second)
			if res.Verdict != tc.want {
				t.Fatalf("got %v, want %v\n%s", res.Verdict, tc.want, formatOps(ops))
			}
			if res.Verdict == NotLinearizable {
				if len(res.Minimal) == 0 {
					t.Fatal("no minimal sub-history")
				}
				if CheckKey(tc.model, "k", res.Minimal, true, 5*time.Second).Verdict != NotLinearizable {
					t.Fatalf("minimal sub-history is linearizable:\n%s", formatOps(res.Minimal))
				}
			}
		})
	}
}

// genHistory simulates nProc clients on one register (or counter). Each op
// takes effect at a random instant inside [invoke, complete], applied to a
// sequential model in that order, so the history is linearizable by
// construction. infoP is the chance an op is reported as info (for an info
// op the effect still happens with probability 1/2, or not at all).
func genHistory(rng *rand.Rand, counter bool, nProc, nOps int, infoP float64) []Op {
	type ev struct {
		at int64
		op int
	}
	var ops []Op
	clock := make([]int64, nProc)
	for i := 0; i < nOps; i++ {
		p := rng.Intn(nProc)
		inv := clock[p] + int64(rng.Intn(20))
		dur := int64(1 + rng.Intn(60))
		o := Op{Process: p, Key: "k", Invoke: inv, Complete: inv + dur, Outcome: OK}
		x := rng.Intn(10)
		switch {
		case counter && x < 7:
			o.Kind, o.Delta = KindIncr, 1
		case counter:
			o.Kind = KindRead
		case x < 4:
			o.Kind, o.Value = KindWrite, fmt.Sprintf("v%d", i)
		case x < 7:
			o.Kind = KindRead
		default:
			o.Kind, o.Value = KindCAS, fmt.Sprintf("v%d", i)
		}
		if o.Kind != KindRead && rng.Float64() < infoP {
			o.Outcome = Info
		}
		clock[p] = o.Complete + 1
		if o.Outcome == Info {
			clock[p] += 1000 // the client gives up; the process is retired
		}
		ops = append(ops, o)
	}
	// Choose effect instants and apply in that order.
	var evs []ev
	applied := make([]bool, len(ops))
	for i, o := range ops {
		if o.Outcome == Info && rng.Intn(2) == 0 {
			continue // never takes effect
		}
		end := o.Complete
		if o.Outcome == Info {
			end = o.Invoke + 2000
		}
		evs = append(evs, ev{at: o.Invoke + rng.Int63n(end-o.Invoke+1), op: i})
		applied[i] = true
	}
	sortEvs := func() {
		for i := 1; i < len(evs); i++ {
			for j := i; j > 0 && evs[j].at < evs[j-1].at; j-- {
				evs[j], evs[j-1] = evs[j-1], evs[j]
			}
		}
	}
	sortEvs()
	val, exists := "", false
	var cnt int64
	var written []string
	for _, e := range evs {
		o := &ops[e.op]
		switch o.Kind {
		case KindWrite:
			val, exists = o.Value, true
			written = append(written, o.Value)
		case KindIncr:
			cnt++
			o.IncrRes = cnt
		case KindRead:
			if counter {
				o.Found, o.Value = cnt > 0, fmt.Sprint(cnt)
			} else {
				o.Found, o.Value = exists, val
			}
		case KindCAS:
			// Expect the current value half the time, a stale one otherwise.
			if exists && rng.Intn(2) == 0 {
				o.Expect = val
			} else if len(written) > 0 {
				o.Expect = written[rng.Intn(len(written))]
			} else {
				o.Expect = "none"
			}
			switch {
			case !exists:
				o.CASRes = CASNotFound
			case o.Expect == val:
				o.CASRes = CASOK
				val = o.Value
				written = append(written, o.Value)
			default:
				o.CASRes = CASMismatch
			}
		}
	}
	for i := range ops {
		if !applied[i] && ops[i].Kind == KindCAS && ops[i].Expect == "" {
			ops[i].Expect = "never"
		}
		if ops[i].Outcome == Info {
			ops[i].CASRes, ops[i].IncrRes = "", 0
		}
	}
	return number(ops)
}

func TestChecker_RandomLinearizable(t *testing.T) {
	n := 300
	if testing.Short() {
		n = 60
	}
	for seed := int64(0); seed < int64(n); seed++ {
		rng := rand.New(rand.NewSource(seed))
		counter := seed%3 == 0
		m := RegisterModel
		if counter {
			m = CounterModel
		}
		ops := genHistory(rng, counter, 2+rng.Intn(5), 20+rng.Intn(120), 0.1)
		res := CheckKey(m, "k", ops, true, 10*time.Second)
		if res.Verdict != Linearizable {
			t.Fatalf("seed %d (%s): got %v for a linearizable-by-construction history:\n%s", seed, m.Name, res.Verdict, formatOps(ops))
		}
	}
}

// TestChecker_RandomMutated: corrupt one ok read of a linearizable history
// to return a value no op wrote; the checker must reject it and the minimal
// sub-history must contain that read.
func TestChecker_RandomMutated(t *testing.T) {
	n := 200
	if testing.Short() {
		n = 40
	}
	rejected := 0
	for seed := int64(0); seed < int64(n); seed++ {
		rng := rand.New(rand.NewSource(seed))
		ops := genHistory(rng, false, 2+rng.Intn(4), 30+rng.Intn(80), 0.1)
		var reads []int
		for i, o := range ops {
			if o.Kind == KindRead {
				reads = append(reads, i)
			}
		}
		if len(reads) == 0 {
			continue
		}
		bad := reads[rng.Intn(len(reads))]
		ops[bad].Found, ops[bad].Value = true, "never-written"
		res := CheckKey(RegisterModel, "k", ops, true, 10*time.Second)
		if res.Verdict != NotLinearizable {
			t.Fatalf("seed %d: corrupted read #%d accepted (%v)", seed, bad, res.Verdict)
		}
		found := false
		for _, o := range res.Minimal {
			if o.ID == bad {
				found = true
			}
		}
		if !found {
			t.Fatalf("seed %d: minimal history lacks the corrupted read #%d:\n%s", seed, bad, formatOps(res.Minimal))
		}
		rejected++
	}
	t.Logf("%d corrupted histories rejected", rejected)
}

// TestChecker_ShrinkIsSmall: a long linearizable history with one stale read
// near the end shrinks to a handful of ops.
func TestChecker_ShrinkIsSmall(t *testing.T) {
	var ops []Op
	tm := int64(0)
	for i := 0; i < 400; i++ {
		ops = append(ops, w("k", fmt.Sprintf("v%d", i), tm, tm+5), r("k", fmt.Sprintf("v%d", i), tm+6, tm+8))
		tm += 10
	}
	ops = append(ops, r("k", "v100", tm, tm+1)) // stale
	res := CheckKey(RegisterModel, "k", number(ops), true, 10*time.Second)
	if res.Verdict != NotLinearizable {
		t.Fatalf("verdict %v", res.Verdict)
	}
	// The shortest failing prefix is the whole history (the stale read is
	// last); dropping observations leaves the writes plus the stale read.
	reads := 0
	for _, o := range res.Minimal {
		if o.Kind == KindRead {
			reads++
		}
	}
	if reads != 1 {
		t.Fatalf("minimal history keeps %d reads, want only the stale one:\n%s", reads, formatOps(res.Minimal))
	}
	t.Logf("%d ops → %d ops", len(ops), len(res.Minimal))
}

func TestChecker_Timeout(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	ops := genHistory(rng, false, 8, 3000, 0.3)
	res := CheckKey(RegisterModel, "k", ops, true, time.Nanosecond)
	if res.Verdict != Unknown {
		t.Fatalf("verdict %v with a 1ns budget, want unknown", res.Verdict)
	}
}

func TestChecker_ReadsDroppedWhenStaleAllowed(t *testing.T) {
	ops := number([]Op{w("k", "a", 0, 1), w("k", "b", 2, 3), r("k", "a", 4, 5)})
	if v := CheckKey(RegisterModel, "k", ops, false, time.Second).Verdict; v != Linearizable {
		t.Fatalf("writes-only check: %v", v)
	}
	if p := CheckReadValidity(ops); len(p) != 0 {
		t.Fatalf("stale read flagged as invalid: %v", p)
	}
	bad := number([]Op{w("k", "a", 0, 1), r("k", "zz", 4, 5), r("k", "c", 6, 7), w("k", "c", 8, 9), failed(w("k", "d", 10, 11)), r("k", "d", 12, 13)})
	p := CheckReadValidity(bad)
	if len(p) != 3 {
		t.Fatalf("want 3 invalid reads (nowhere, future, failed write), got %d: %v", len(p), p)
	}
}

func TestCheckCounter(t *testing.T) {
	ok := []Op{incr("c", 1, 0, 1), incr("c", 2, 2, 3), incr("c", 0, 4, inf), incr("c", 4, 5, 6)}
	if p := CheckCounter("c", ok, 3); len(p) != 0 {
		t.Fatalf("valid counter flagged: %v", p)
	}
	if p := CheckCounter("c", ok, 4); len(p) != 0 {
		t.Fatalf("valid counter (info applied) flagged: %v", p)
	}
	if p := CheckCounter("c", ok, 2); len(p) == 0 || !strings.Contains(p[0], "outside") {
		t.Fatalf("lost increment not flagged: %v", p)
	}
	if p := CheckCounter("c", ok, 5); len(p) == 0 {
		t.Fatal("double-applied increment not flagged")
	}
	dup := []Op{incr("c", 1, 0, 1), incr("c", 1, 2, 3)}
	if p := CheckCounter("c", dup, 2); len(p) == 0 {
		t.Fatal("duplicate INCR result not flagged")
	}
	order := []Op{incr("c", 2, 0, 1), incr("c", 1, 2, 3)}
	if p := CheckCounter("c", order, 2); len(p) == 0 || !strings.Contains(strings.Join(p, " "), "real-time") {
		t.Fatalf("real-time violation not flagged: %v", p)
	}
}

func TestCheckSetAndCopies(t *testing.T) {
	add := func(k string, inv, cmp int64) Op { return mk(Op{Kind: KindAdd, Key: k, Value: "1"}, inv, cmp) }
	ops := []Op{add("s1", 0, 1), add("s2", 2, 3), add("s3", 4, inf), failed(add("s4", 5, 6))}
	r := CheckSet(ops, map[string]bool{"s1": true, "s2": true})
	if len(r.Lost) != 0 || len(r.Phantoms) != 0 || r.InfoPresent != 0 {
		t.Fatalf("valid set flagged: %+v", r)
	}
	r = CheckSet(ops, map[string]bool{"s1": true, "s3": true, "s9": true})
	if len(r.Lost) != 1 || len(r.Phantoms) != 1 || r.InfoPresent != 1 {
		t.Fatalf("want 1 lost (s2), 1 phantom (s9), 1 info present (s3): %+v", r)
	}
	per := map[string]map[string]bool{
		"n1": {"s1": true, "s2": true},
		"n2": {"s1": true},
		"n3": {"s1": true, "s2": true},
	}
	if p := CheckCopies(ops, per, 2); len(p) != 0 {
		t.Fatalf("quorum copies flagged: %v", p)
	}
	if p := CheckCopies(ops, per, 3); len(p) != 1 {
		t.Fatalf("strong copies: want s2 flagged, got %v", p)
	}
}

func TestChecker_ExplainNamesTheStaleRead(t *testing.T) {
	ops := number([]Op{w("k", "a", 0, 1), w("k", "b", 2, 3), r("k", "a", 4, 5)})
	res := CheckKey(RegisterModel, "k", ops, true, time.Second)
	if !strings.Contains(res.Explain, "holds b") || !strings.Contains(res.Explain, "read → a") {
		t.Fatalf("explanation does not name the state and the stale read:\n%s", FormatResult(res))
	}
	t.Logf("\n%s", FormatResult(res))
}

func TestCheckCounter_Gaps(t *testing.T) {
	// Results 1 and 3: value 2 came from somewhere, but there is no info INCR.
	ops := []Op{incr("c", 1, 0, 1), incr("c", 3, 2, 3)}
	if p := CheckCounter("c", ops, 3); len(p) == 0 {
		t.Fatal("gap with no info increment not flagged")
	}
	ops = append(ops, incr("c", 0, 0, inf))
	if p := CheckCounter("c", ops, 3); len(p) != 0 {
		t.Fatalf("gap covered by an info increment flagged: %v", p)
	}
}

func TestReadValiditySkipsCounters(t *testing.T) {
	ops := []Op{incr("c", 1, 0, 1), r("c", "1", 2, 3)}
	if p := CheckReadValidity(ops); len(p) != 0 {
		t.Fatalf("counter read flagged: %v", p)
	}
}
