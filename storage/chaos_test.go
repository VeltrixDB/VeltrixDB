package storage

// chaos_test.go — Jepsen-style convergence chaos for replicated last-writer-wins.
//
// This models what the replication delivery layer hands to the apply side of two
// replicas that write concurrently and then exchange writes across a healed
// partition. It exercises the exact production apply path — ApplyLWWPut /
// ApplyLWWDelete on a real StorageEngine — under:
//
//   - concurrent multi-writer traffic on both replicas (goroutines),
//   - a partition window during which each replica sees only its own writes,
//   - a heal that delivers each side's buffered writes to the peer in a
//     SHUFFLED order (different from the order they were produced), so arrival
//     order cannot match production order,
//   - repeated across many rounds and keys.
//
// It asserts the two properties the divergence fix must guarantee:
//
//   1. CONVERGENCE  — after all writes are delivered both ways, the two replicas
//      hold byte-identical state for every key.
//   2. CORRECT WINNER — that converged value equals the last-writer-wins ground
//      truth: for each key, the write with the highest origin clock wins
//      (a tombstone with the highest clock ⇒ the key is absent).
//
// This is the delivery-order-independence property; it deliberately does NOT
// test the (separate, documented) anti-entropy delivery gap — it assumes writes
// are eventually delivered, and checks that ORDER never causes divergence.

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

// chaosOp is one write produced by a replica.
type chaosOp struct {
	key   string
	value []byte
	ts    int64 // origin wall-clock (ns), globally unique & increasing
	del   bool
}

func TestChaos_ReplicatedConvergenceUnderPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test skipped in -short mode")
	}
	const (
		numRounds      = 12
		writersPerNode = 4
		writesPerRound = 60 // per node per round
		keySpace       = 40
		seed           = 0x5EED1234
	)

	engA := newTestEngine(t)
	engB := newTestEngine(t)

	// A shared, monotonically-increasing origin clock. Models loosely-synced
	// wall clocks: every write gets a globally-unique, ordered stamp, so LWW has
	// a single deterministic winner per key. (Genuinely equal stamps are covered
	// by TestLWW_TieBreakDeterministic; here we stress order-independence.)
	var clock atomic.Int64

	// allOps records every op ever produced, from both replicas, for the
	// ground-truth winner computation at the end.
	var opsMu sync.Mutex
	var allOps []chaosOp

	apply := func(eng *StorageEngine, op chaosOp) {
		if op.del {
			eng.ApplyLWWDelete(op.key, op.ts)
		} else {
			eng.ApplyLWWPut(op.key, op.value, -1, op.ts)
		}
	}

	// One RNG per node, seeded deterministically, so the run is reproducible but
	// the two nodes make independent choices.
	rngA := rand.New(rand.NewSource(seed))
	rngB := rand.New(rand.NewSource(seed ^ 0x9E3779B9))

	// produceRound runs writersPerNode goroutines that each apply writes LOCALLY
	// to `local` and return the ops (to be delivered to the peer after a delay,
	// simulating a partition that buffers cross-replica traffic).
	produceRound := func(local *StorageEngine, rng *rand.Rand, tag string, round int) []chaosOp {
		var mu sync.Mutex
		var produced []chaosOp
		var wg sync.WaitGroup
		perWriter := writesPerRound / writersPerNode
		for w := 0; w < writersPerNode; w++ {
			wg.Add(1)
			// Each writer gets its own child RNG seed for deterministic concurrency.
			wseed := rng.Int63()
			go func(wseed int64) {
				defer wg.Done()
				r := rand.New(rand.NewSource(wseed))
				for i := 0; i < perWriter; i++ {
					key := fmt.Sprintf("k-%02d", r.Intn(keySpace))
					op := chaosOp{key: key, ts: clock.Add(1)}
					if r.Intn(5) == 0 {
						op.del = true
					} else {
						op.value = []byte(fmt.Sprintf("%s-r%d-w%d-%d", tag, round, wseed&0xff, i))
					}
					apply(local, op) // apply locally now
					mu.Lock()
					produced = append(produced, op)
					mu.Unlock()
				}
			}(wseed)
		}
		wg.Wait()
		opsMu.Lock()
		allOps = append(allOps, produced...)
		opsMu.Unlock()
		return produced
	}

	// deliver applies ops to the peer in SHUFFLED order (concurrently), so the
	// arrival order on the peer differs from the production order on the origin.
	deliver := func(peer *StorageEngine, ops []chaosOp, rng *rand.Rand) {
		shuffled := make([]chaosOp, len(ops))
		copy(shuffled, ops)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		var wg sync.WaitGroup
		// Fan the delivery out across goroutines to interleave arrivals further.
		chunks := 4
		for c := 0; c < chunks; c++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				for i := c; i < len(shuffled); i += chunks {
					apply(peer, shuffled[i])
				}
			}(c)
		}
		wg.Wait()
	}

	// Buffered (partitioned) cross-replica traffic, delivered a round later.
	var pendingToB, pendingToA []chaosOp

	for round := 0; round < numRounds; round++ {
		// Both replicas write concurrently during the "partition" — each sees
		// only its own writes this round.
		fromA := produceRound(engA, rngA, "A", round)
		fromB := produceRound(engB, rngB, "B", round)

		// Heal the PREVIOUS round's partition: deliver last round's buffered
		// traffic to the peer in shuffled order (arrival order ≠ production order).
		deliver(engB, pendingToB, rngB)
		deliver(engA, pendingToA, rngA)

		// This round's traffic stays buffered until the next heal.
		pendingToB = fromA
		pendingToA = fromB
	}

	// Final heal: flush everything still in flight, both directions.
	deliver(engB, pendingToB, rngB)
	deliver(engA, pendingToA, rngA)

	// ── Ground truth: last-writer-wins winner per key ─────────────────────────
	type winner struct {
		ts    int64
		del   bool
		value []byte
	}
	truth := map[string]winner{}
	for _, op := range allOps {
		if w, ok := truth[op.key]; !ok || op.ts > w.ts {
			truth[op.key] = winner{ts: op.ts, del: op.del, value: op.value}
		}
	}

	// ── Assertions ────────────────────────────────────────────────────────────
	checked := 0
	for key, w := range truth {
		va, errA := engA.Get(key)
		vb, errB := engB.Get(key)

		// Property 1 — convergence: both replicas agree.
		aAbsent, bAbsent := errA != nil, errB != nil
		if aAbsent != bAbsent {
			t.Fatalf("DIVERGENCE key %s: A present=%v B present=%v", key, !aAbsent, !bAbsent)
		}
		if !aAbsent && string(va) != string(vb) {
			t.Fatalf("DIVERGENCE key %s: A=%q B=%q", key, va, vb)
		}

		// Property 2 — correct LWW winner.
		if w.del {
			if !aAbsent {
				t.Errorf("key %s: highest-clock write was a delete (ts=%d) but value present: A=%q", key, w.ts, va)
			}
		} else {
			if aAbsent {
				t.Errorf("key %s: highest-clock write was a put %q (ts=%d) but key absent", key, w.value, w.ts)
			} else if string(va) != string(w.value) {
				t.Errorf("key %s: converged to %q but LWW winner is %q (ts=%d)", key, va, w.value, w.ts)
			}
		}
		checked++
	}
	t.Logf("chaos survived: %d keys converged on both replicas to the correct LWW winner after %d partition rounds (%d total writes)",
		checked, numRounds, len(allOps))
}
