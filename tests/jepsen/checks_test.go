package jepsen

// checks_test.go — the non-linearizability checkers: the INCR counter bounds,
// the set (no lost / no phantom acknowledged writes), the replicated-mode
// copy count, and the validity of possibly-stale reads.

import (
	"fmt"
	"sort"
)

// CheckCounter checks one counter key whose final value (read once the run
// has quiesced) is final:
//   - final ∈ [Σ ok deltas, Σ ok deltas + Σ info deltas] (all deltas > 0);
//   - every ok INCR returned a distinct value (two equal results mean one
//     increment was applied twice or two were merged);
//   - real-time order: if INCR a completed before INCR b was invoked, a's
//     result is smaller than b's.
func CheckCounter(key string, ops []Op, final int64) []string {
	var probs []string
	var okSum, infoSum int64
	var oks []Op
	for _, o := range ops {
		if o.Kind != KindIncr {
			continue
		}
		switch o.Outcome {
		case OK:
			okSum += o.Delta
			oks = append(oks, o)
		case Info:
			infoSum += o.Delta
		}
	}
	if final < okSum || final > okSum+infoSum {
		probs = append(probs, fmt.Sprintf("counter %s: final value %d outside [ok=%d, ok+info=%d]", key, final, okSum, okSum+infoSum))
	}
	// Every value below the largest ok result was produced by some INCR;
	// the ones no ok INCR returned must be covered by info INCRs.
	var maxRes int64
	for _, o := range oks {
		if o.IncrRes > maxRes {
			maxRes = o.IncrRes
		}
	}
	if gaps := maxRes - int64(len(oks)); maxRes > 0 && gaps > infoSum {
		probs = append(probs, fmt.Sprintf("counter %s: ok INCRs reach %d but only %d ok + %d info increments exist", key, maxRes, len(oks), infoSum))
	}
	seen := map[int64]Op{}
	for _, o := range oks {
		if p, dup := seen[o.IncrRes]; dup {
			probs = append(probs, fmt.Sprintf("counter %s: two ok INCRs returned %d:\n    %s\n    %s", key, o.IncrRes, p, o))
		}
		seen[o.IncrRes] = o
		if o.IncrRes < o.Delta || o.IncrRes > okSum+infoSum {
			probs = append(probs, fmt.Sprintf("counter %s: INCR returned %d, outside [%d, %d]: %s", key, o.IncrRes, o.Delta, okSum+infoSum, o))
		}
	}
	byInvoke := append([]Op(nil), oks...)
	sort.Slice(byInvoke, func(i, j int) bool { return byInvoke[i].Invoke < byInvoke[j].Invoke })
	byComplete := append([]Op(nil), oks...)
	sort.Slice(byComplete, func(i, j int) bool { return byComplete[i].Complete < byComplete[j].Complete })
	var maxOp *Op
	j := 0
	for _, b := range byInvoke {
		for j < len(byComplete) && byComplete[j].Complete < b.Invoke {
			if maxOp == nil || byComplete[j].IncrRes > maxOp.IncrRes {
				maxOp = &byComplete[j]
			}
			j++
		}
		if maxOp != nil && b.IncrRes <= maxOp.IncrRes {
			probs = append(probs, fmt.Sprintf("counter %s: real-time order violated, an INCR that completed earlier returned a larger value:\n    %s\n    %s", key, *maxOp, b))
			if len(probs) > 20 {
				break
			}
		}
	}
	return probs
}

// SetResult summarizes the set check.
type SetResult struct {
	OK, Info       int
	InfoPresent    int // info adds that did take effect
	Lost, Phantoms []string
}

// CheckSet checks the add ops (unique keys, written once) against the keys
// present at the end: every ok add must be present (no lost acknowledged
// write) and every present key must come from an invoked add (nothing
// appears from nowhere). Info adds may be present or not.
func CheckSet(ops []Op, present map[string]bool) SetResult {
	var r SetResult
	invoked := map[string]bool{}
	for _, o := range ops {
		if o.Kind != KindAdd {
			continue
		}
		invoked[o.Key] = true
		switch o.Outcome {
		case OK:
			r.OK++
			if !present[o.Key] {
				r.Lost = append(r.Lost, o.String())
			}
		case Info:
			r.Info++
			if present[o.Key] {
				r.InfoPresent++
			}
		}
	}
	for k := range present {
		if !invoked[k] {
			r.Phantoms = append(r.Phantoms, k)
		}
	}
	sort.Strings(r.Phantoms)
	return r
}

// CheckCopies (replicated mode) requires every ok add to be present on at
// least need of the given nodes' keyspaces. It returns one line per add
// that is short of copies.
func CheckCopies(ops []Op, perNode map[string]map[string]bool, need int) []string {
	var probs []string
	for _, o := range ops {
		if o.Kind != KindAdd || o.Outcome != OK {
			continue
		}
		var on, off []string
		for n, ks := range perNode {
			if ks[o.Key] {
				on = append(on, n)
			} else {
				off = append(off, n)
			}
		}
		if len(on) < need {
			sort.Strings(on)
			sort.Strings(off)
			probs = append(probs, fmt.Sprintf("%s: on %d node(s) %v, missing on %v, need %d", o, len(on), on, off, need))
		}
	}
	return probs
}

// CheckReadValidity is the read check where reads may be stale (raft
// without --linearizable-reads): every value a read returns must have been
// written by a write / add / CAS that was invoked before the read completed
// and that did not definitely fail. It catches values from nowhere and values
// from the future; it does not bound staleness.
func CheckReadValidity(ops []Op) []string {
	written := map[string]map[string]int64{} // key → value → earliest invoke
	for _, o := range ops {
		if o.Outcome == Fail {
			continue
		}
		var v string
		switch o.Kind {
		case KindWrite, KindAdd:
			v = o.Value
		case KindCAS:
			if o.Outcome == OK && o.CASRes != CASOK {
				continue
			}
			v = o.Value
		default:
			continue
		}
		if written[o.Key] == nil {
			written[o.Key] = map[string]int64{}
		}
		if t, ok := written[o.Key][v]; !ok || o.Invoke < t {
			written[o.Key][v] = o.Invoke
		}
	}
	counters := map[string]bool{} // INCR keys hold counts, not written values
	for _, o := range ops {
		if o.Kind == KindIncr {
			counters[o.Key] = true
		}
	}
	var probs []string
	for _, o := range ops {
		if o.Kind != KindRead || o.Outcome != OK || !o.Found || counters[o.Key] {
			continue
		}
		t, ok := written[o.Key][o.Value]
		switch {
		case !ok:
			probs = append(probs, "read of a value no op wrote: "+o.String())
		case t > o.Complete:
			probs = append(probs, "read of a value written only after the read completed: "+o.String())
		}
	}
	return probs
}
