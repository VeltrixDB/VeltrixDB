package storage

import (
	"fmt"
	"testing"
)

// TestLWW_ConvergesRegardlessOfArrivalOrder is the core regression test for the
// replicated-mode silent-divergence bug: two replicas that apply the same set
// of concurrent writes to one key in OPPOSITE orders must end up with the same
// value. Under the old blind last-writer-by-arrival apply they diverged
// permanently; under LWW-by-origin-clock they converge.
func TestLWW_ConvergesRegardlessOfArrivalOrder(t *testing.T) {
	type write struct {
		val string
		ts  int64 // origin clock (ns)
	}
	// Three concurrent writes to the same key, each with a distinct origin clock.
	writes := []write{
		{"alpha", 1000},
		{"bravo", 3000}, // newest — must win on every replica
		{"charlie", 2000},
	}

	apply := func(order []int) string {
		se := newTestEngine(t)
		for _, i := range order {
			if _, err := se.ApplyLWWPut("k", []byte(writes[i].val), -1, writes[i].ts); err != nil {
				t.Fatalf("ApplyLWWPut: %v", err)
			}
		}
		got, err := se.Get("k")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		return string(got)
	}

	orders := [][]int{
		{0, 1, 2},
		{2, 1, 0},
		{1, 0, 2},
		{2, 0, 1},
	}
	want := "bravo" // highest origin clock always wins
	for _, o := range orders {
		if got := apply(o); got != want {
			t.Errorf("arrival order %v: converged to %q, want %q", o, got, want)
		}
	}
}

// TestLWW_DeleteBeforePut covers the re-ordered-delivery tombstone case: a
// delete with a NEWER origin clock must win even when it is applied BEFORE the
// (older) put it supersedes arrives.
func TestLWW_DeleteBeforePut(t *testing.T) {
	se := newTestEngine(t)

	// Delete arrives first with the newer clock...
	if _, err := se.ApplyLWWDelete("k", 5000); err != nil {
		t.Fatalf("ApplyLWWDelete: %v", err)
	}
	// ...then the older put arrives late and must be rejected.
	applied, err := se.ApplyLWWPut("k", []byte("stale"), -1, 4000)
	if err != nil {
		t.Fatalf("ApplyLWWPut: %v", err)
	}
	if applied {
		t.Errorf("older put won over a newer delete — LWW tombstone not honored")
	}
	if _, err := se.Get("k"); err == nil {
		t.Errorf("key present after a newer delete should have won")
	}

	// A put NEWER than the delete resurrects the key.
	applied, err = se.ApplyLWWPut("k", []byte("fresh"), -1, 6000)
	if err != nil {
		t.Fatalf("ApplyLWWPut(fresh): %v", err)
	}
	if !applied {
		t.Fatalf("newer put should have won over the delete")
	}
	got, err := se.Get("k")
	if err != nil || string(got) != "fresh" {
		t.Errorf("want fresh, got %q err=%v", string(got), err)
	}
}

// TestLWW_TieBreakDeterministic verifies that writes with the SAME origin clock
// resolve identically everywhere: tombstone beats put, and between two puts the
// higher CRC wins.
func TestLWW_TieBreakDeterministic(t *testing.T) {
	// Two puts at the same clock: apply in both orders, must agree.
	a, b := []byte("value-A"), []byte("value-B")
	const ts = 7000

	resolve := func(first, second []byte) string {
		se := newTestEngine(t)
		se.ApplyLWWPut("k", first, -1, ts)
		se.ApplyLWWPut("k", second, -1, ts)
		got, _ := se.Get("k")
		return string(got)
	}
	if resolve(a, b) != resolve(b, a) {
		t.Errorf("tie between two puts not order-independent: %q vs %q", resolve(a, b), resolve(b, a))
	}

	// Tombstone beats a put at the same clock, in either order.
	se1 := newTestEngine(t)
	se1.ApplyLWWPut("k", a, -1, ts)
	se1.ApplyLWWDelete("k", ts)
	if _, err := se1.Get("k"); err == nil {
		t.Errorf("tombstone should beat a same-clock put (put-then-delete)")
	}
	se2 := newTestEngine(t)
	se2.ApplyLWWDelete("k", ts)
	applied, _ := se2.ApplyLWWPut("k", a, -1, ts)
	if applied {
		t.Errorf("same-clock put should not beat an existing tombstone (delete-then-put)")
	}
}

// TestLWW_ManyKeysConverge fuzzes many keys with shuffled arrival orders across
// two independent engines and asserts both reach byte-identical state.
func TestLWW_ManyKeysConverge(t *testing.T) {
	const numKeys, numWrites = 40, 6
	type op struct {
		key string
		val string
		ts  int64
		del bool
	}
	var ops []op
	for k := 0; k < numKeys; k++ {
		key := fmt.Sprintf("key-%02d", k)
		for w := 0; w < numWrites; w++ {
			// Deterministic pseudo-shuffled clock so no two writes to a key tie.
			ts := int64((w*7+k*3)%numWrites+1) * 1000
			ops = append(ops, op{key: key, val: fmt.Sprintf("v-%d-%d", k, w), ts: ts, del: w%5 == 4})
		}
	}

	run := func(order []int) map[string]string {
		se := newTestEngine(t)
		for _, i := range order {
			o := ops[i]
			if o.del {
				se.ApplyLWWDelete(o.key, o.ts)
			} else {
				se.ApplyLWWPut(o.key, []byte(o.val), -1, o.ts)
			}
		}
		state := map[string]string{}
		for k := 0; k < numKeys; k++ {
			key := fmt.Sprintf("key-%02d", k)
			if v, err := se.Get(key); err == nil {
				state[key] = string(v)
			}
		}
		return state
	}

	forward := make([]int, len(ops))
	reverse := make([]int, len(ops))
	for i := range ops {
		forward[i] = i
		reverse[len(ops)-1-i] = i
	}

	s1, s2 := run(forward), run(reverse)
	if len(s1) != len(s2) {
		t.Fatalf("live-key count diverged: forward=%d reverse=%d", len(s1), len(s2))
	}
	for k, v := range s1 {
		if s2[k] != v {
			t.Errorf("key %s diverged: forward=%q reverse=%q", k, v, s2[k])
		}
	}
}
