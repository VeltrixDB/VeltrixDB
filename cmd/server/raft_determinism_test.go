package main

// raft_determinism_test.go — every state-machine decision in raft Apply is the
// same on every node (CLAUDE.md invariant 66).  The SETIF / TXN version token
// used to be each node's own write timestamp, taken from its local clock when
// it applied the write: the nodes disagreed on every key's version, so a
// guarded TXN applied through the log succeeded on the node whose token the
// client had read and failed on the others — the replicas diverged.  A node
// rebuilt from the log gave every key a new version, so a guard taken before
// the rebuild failed on it alone.

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/storage"
)

var detReqID atomic.Uint64

// submitResult submits cmd through the current leader and returns the result
// the leader's FSM delivered (the coordinator's submitForResult, without the
// redirect handling).
func (c *snapTestCluster) submitResult(cmd fsmCmd) (fsmResult, error) {
	c.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		l := c.waitLeader()
		cmd.ReqID = 1<<62 | detReqID.Add(1)
		ch := l.fsm.register(cmd.ReqID)
		b, err := encodeCmd(cmd)
		if err != nil {
			l.fsm.unregister(cmd.ReqID)
			return fsmResult{}, err
		}
		err = l.node.Submit(b)
		if err == nil {
			select {
			case res := <-ch:
				l.fsm.unregister(cmd.ReqID)
				return res, nil
			case <-time.After(10 * time.Second):
				l.fsm.unregister(cmd.ReqID)
				return fsmResult{}, errors.New("result not delivered")
			}
		}
		l.fsm.unregister(cmd.ReqID)
		if time.Now().After(deadline) {
			return fsmResult{}, err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// engineOf returns node i's engine (nil while stopped).
func (c *snapTestCluster) engineOf(i int) *storage.StorageEngine {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i].engine
}

// versionsOf returns node i's version token for every key.
func versionsOf(e *storage.StorageEngine, keys []string) map[string]uint64 {
	out := make(map[string]uint64, len(keys))
	for _, k := range keys {
		out[k] = e.KeyVersion(k)
	}
	return out
}

// checkVersionsEqual fails unless every node reports the same version token
// for every key.
func checkVersionsEqual(t *testing.T, c *snapTestCluster, keys []string) {
	t.Helper()
	want := versionsOf(c.engineOf(0), keys)
	var bad []string
	for i := 1; i < len(c.nodes); i++ {
		got := versionsOf(c.engineOf(i), keys)
		for _, k := range keys {
			if got[k] != want[k] {
				bad = append(bad, fmt.Sprintf("%s: node0=%d node%d=%d", k, want[k], i, got[k]))
			}
		}
	}
	if len(bad) > 0 {
		if len(bad) > 8 {
			bad = append(bad[:8], fmt.Sprintf("… %d more", len(bad)-8))
		}
		t.Errorf("version tokens differ between nodes: %s", strings.Join(bad, "; "))
	}
}

type guardedOp struct {
	client, iter int
	key          string
	readFrom     int    // node whose version token the client used
	receipt      string // written by the TXN iff its guard held
	err          error  // the leader's outcome
}

// TestRaftDeterminism_ConcurrentGuardedTxns: six clients race guarded TXNs
// and single-op SETIFs (plus unguarded PUTs that make guards fail) against
// the same four keys, each reading its version token from a different node.
// Afterwards every node must hold exactly the same keyspace — including one
// receipt key per TXN whose guard held — and report the same version token
// for every key, and the leader's outcome of every TXN must match the
// receipt.  Fails on the pre-fix code: the token read from a follower was
// that follower's own write timestamp, so the guard held there and failed on
// the leader (or the other way round).
func TestRaftDeterminism_ConcurrentGuardedTxns(t *testing.T) {
	c := newSnapTestCluster(t, 3, 0)
	c.waitLeader()
	keys := []string{"acct-0", "acct-1", "acct-2", "acct-3"}
	for _, k := range keys {
		c.submit(fsmCmd{Op: opPut, Key: k, Value: []byte("seed"), TTL: -1})
	}
	for i := range c.nodes {
		c.waitApplied(i, 20*time.Second)
	}

	const clients, iters = 6, 30
	var (
		mu  sync.Mutex
		ops []guardedOp
		wg  sync.WaitGroup
	)
	for cl := 0; cl < clients; cl++ {
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(cl) + 1))
			for it := 0; it < iters; it++ {
				key := keys[rng.Intn(len(keys))]
				readFrom := (cl + it) % len(c.nodes)
				e := c.engineOf(readFrom)
				ver := e.KeyVersion(key)
				val := []byte(fmt.Sprintf("c%d-i%d", cl, it))
				op := guardedOp{client: cl, iter: it, key: key, readFrom: readFrom}
				var cmd fsmCmd
				switch it % 4 {
				case 3:
					// An unguarded write: later guards on this key must fail.
					cmd = fsmCmd{Op: opTxn, TxnOps: []fsmTxnOp{{Op: "SET", Key: key, Value: val, TTL: -1}}}
				case 2:
					// A single-op SETIF (its outcome shows in the value).
					cmd = fsmCmd{Op: opTxn, TxnOps: []fsmTxnOp{{Op: "SETIF", Key: key, Value: val, TTL: -1, ExpectedVersion: ver}}}
				default:
					op.receipt = fmt.Sprintf("rcpt-c%d-i%d", cl, it)
					cmd = fsmCmd{Op: opTxn, TxnOps: []fsmTxnOp{
						{Op: "SETIF", Key: key, Value: val, TTL: -1, ExpectedVersion: ver},
						{Op: "SET", Key: op.receipt, Value: []byte(key), TTL: -1},
					}}
				}
				res, err := c.submitResult(cmd)
				if err != nil {
					t.Errorf("client %d: submit: %v", cl, err)
					return
				}
				op.err = res.err
				mu.Lock()
				ops = append(ops, op)
				mu.Unlock()
			}
		}(cl)
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	for i := range c.nodes {
		c.waitApplied(i, 20*time.Second)
	}
	waitEnginesEqual(t, c, 20*time.Second)
	checkVersionsEqual(t, c, keys)

	var held, conflicts, heldFromFollower int
	leader := c.leader()
	for _, op := range ops {
		if op.receipt == "" {
			continue
		}
		if op.err != nil && !errors.Is(op.err, storage.ErrTxnConflict) {
			t.Errorf("client %d iter %d: unexpected error %v", op.client, op.iter, op.err)
			continue
		}
		for i := range c.nodes {
			_, gerr := c.engineOf(i).Get(op.receipt)
			present := gerr == nil
			if present != (op.err == nil) {
				t.Errorf("client %d iter %d (token read from node %d): leader outcome %v, receipt on node %d present=%v",
					op.client, op.iter, op.readFrom, op.err, i, present)
			}
		}
		if op.err == nil {
			held++
			if leader != nil && c.nodes[op.readFrom] != leader {
				heldFromFollower++
			}
		} else {
			conflicts++
		}
	}
	t.Logf("guarded TXNs: %d held (%d with a follower's token), %d conflicts", held, heldFromFollower, conflicts)
	if held == 0 {
		t.Errorf("no guarded TXN held: the version token is not usable")
	}
}

// TestRaftDeterminism_GuardsSurviveRestartAndRebuild: version tokens read
// before a node restarts — cleanly, or rebuilt from the log with its engine
// wiped — are the tokens the node reports afterwards, so a TXN guarded with
// a token from the older incarnation holds on every node.  With snapshots
// the rebuilt node restores the snapshot (whose records carry the versions)
// and applies the tail.  Fails on the pre-fix code: a rebuilt node gave every
// key a new local timestamp.
func TestRaftDeterminism_GuardsSurviveRestartAndRebuild(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold uint64
	}{{"log", 0}, {"snapshot", 16}} {
		t.Run(tc.name, func(t *testing.T) {
			c := newSnapTestCluster(t, 3, tc.threshold)
			c.waitLeader()
			var keys []string
			for i := 0; i < 12; i++ {
				k := fmt.Sprintf("k%02d", i)
				keys = append(keys, k)
				ttl := int32(-1)
				if i%3 == 0 {
					ttl = 3600
				}
				c.submit(fsmCmd{Op: opPut, Key: k, Value: []byte("v1"), TTL: ttl})
				if i%4 == 1 {
					c.submit(fsmCmd{Op: opIncr, Key: k + "-ctr", Delta: 2, TTL: -1})
					keys = append(keys, k+"-ctr")
				}
			}
			c.submit(fsmCmd{Op: opDelete, Key: "k05"})
			for i := 0; i < 20; i++ { // past the snapshot threshold
				c.submit(fsmCmd{Op: opPut, Key: "pad", Value: []byte(fmt.Sprint(i)), TTL: -1})
			}
			for i := range c.nodes {
				c.waitApplied(i, 20*time.Second)
			}
			checkVersionsEqual(t, c, keys)
			before := versionsOf(c.engineOf(0), keys)

			// Node 1: clean restart.  Node 2: rebuilt — its engine wiped, so
			// it restores the snapshot (if any) and applies the log again.
			c.stop(1)
			c.start(1)
			c.stop(2)
			if err := os.RemoveAll(c.nodes[2].engDir); err != nil {
				t.Fatal(err)
			}
			c.start(2)
			for i := range c.nodes {
				c.waitApplied(i, 30*time.Second)
			}
			time.Sleep(200 * time.Millisecond)
			for i := range c.nodes {
				after := versionsOf(c.engineOf(i), keys)
				for _, k := range keys {
					if after[k] != before[k] {
						t.Errorf("node %d: version of %q is %d after the restart/rebuild, was %d", i, k, after[k], before[k])
					}
				}
			}

			// A guard taken against the older incarnation holds everywhere.
			var ops []fsmTxnOp
			for _, k := range []string{"k00", "k01", "k02"} {
				ops = append(ops, fsmTxnOp{Op: "SETIF", Key: k, Value: []byte("v2"), TTL: -1, ExpectedVersion: before[k]})
			}
			ops = append(ops, fsmTxnOp{Op: "SETIF", Key: "k05", Value: []byte("reborn"), TTL: -1, ExpectedVersion: 0})
			res, err := c.submitResult(fsmCmd{Op: opTxn, TxnOps: ops})
			if err != nil {
				t.Fatal(err)
			}
			if res.err != nil {
				t.Errorf("TXN guarded with pre-restart versions: %v", res.err)
			}
			for i := range c.nodes {
				c.waitApplied(i, 20*time.Second)
			}
			waitEnginesEqual(t, c, 20*time.Second)
			for i := range c.nodes {
				if v, err := c.engineOf(i).Get("k01"); err != nil || string(v) != "v2" {
					t.Errorf("node %d: k01 = %q (%v), want v2", i, v, err)
				}
			}
			checkVersionsEqual(t, c, keys)
		})
	}
}

// legacyFsmCmd is the v1.16 fsmCmd (no Ts).
type legacyFsmCmd struct {
	Op       fsmOp
	ReqID    uint64
	Key      string
	Value    []byte
	TTL      int32
	Entries  []storage.MultiPutRequest
	Expected []byte
	Delta    int64
	TxnOps   []fsmTxnOp
	Ns       string
	Field    string
	Vec      []float32
}

func gobBytes(t *testing.T, v interface{}) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestRaftDeterminism_LegacyCommandsAndKeys: a command from a v1.16 leader
// (no Ts) decodes and applies as before; a v1.16 follower decodes a new
// command (Ts ignored).  Two nodes that applied the same legacy prefix at
// different local times — so their keys carry different local timestamps —
// report the same LegacyVersion for those keys once the clock starts, and a
// SETIF taken against it holds on both and gives the key a real version.
func TestRaftDeterminism_LegacyCommandsAndKeys(t *testing.T) {
	// Wire compatibility both ways.
	var old legacyFsmCmd
	if err := gob.NewDecoder(bytes.NewReader(mustEncodeCmd(t, fsmCmd{Op: opPut, Key: "x", Value: []byte("1"), TTL: -1}))).Decode(&old); err != nil || old.Key != "x" {
		t.Fatalf("v1.16 decoder on a new command: %+v %v", old, err)
	}
	legacyPut := func(k, v string) []byte {
		return gobBytes(t, legacyFsmCmd{Op: opPut, Key: k, Value: []byte(v), TTL: -1})
	}

	a, b := newRaftFSM(newFSMTestEngine(t)), newRaftFSM(newFSMTestEngine(t))
	for _, f := range []*raftFSM{a, b} {
		// l1: a legacy command applied by this build; l2: written by a
		// v1.16 build (the engine's own local-clock timestamp).
		if err := f.Apply(legacyPut("l1", "old")); err != nil {
			t.Fatal(err)
		}
		if err := f.engine.Put("l2", []byte("old"), -1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Before the clock starts: tokens are whatever the node has (legacy).
	// The first stamped command starts it on both, at the same time.
	stamped := mustEncodeCmd(t, fsmCmd{Op: opPut, Key: "n", Value: []byte("new"), TTL: -1})
	for _, f := range []*raftFSM{a, b} {
		if err := f.Apply(stamped); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range []string{"l1", "l2", "n"} {
		va, vb := a.engine.KeyVersion(k), b.engine.KeyVersion(k)
		if va != vb {
			t.Fatalf("%s: versions %d vs %d", k, va, vb)
		}
	}
	if v := a.engine.KeyVersion("l1"); v != storage.LegacyVersion {
		t.Fatalf("legacy key version %d, want %d", v, storage.LegacyVersion)
	}
	if a.engine.KeyVersion("n") <= storage.LegacyVersion {
		t.Fatal("stamped key has no real version")
	}
	txn := mustEncodeCmd(t, fsmCmd{Op: opTxn, ReqID: 77, TxnOps: []fsmTxnOp{
		{Op: "SETIF", Key: "l1", Value: []byte("guarded"), TTL: -1, ExpectedVersion: storage.LegacyVersion},
	}})
	for _, f := range []*raftFSM{a, b} {
		ch := f.register(77)
		if err := f.Apply(txn); err != nil {
			t.Fatal(err)
		}
		if res := <-ch; res.err != nil {
			t.Fatalf("SETIF against the legacy version: %v", res.err)
		}
		f.unregister(77)
		if v, err := f.engine.Get("l1"); err != nil || string(v) != "guarded" {
			t.Fatalf("l1 = %q (%v)", v, err)
		}
	}
	if va, vb := a.engine.KeyVersion("l1"), b.engine.KeyVersion("l1"); va != vb || va <= storage.LegacyVersion {
		t.Fatalf("after the guarded write: versions %d vs %d", va, vb)
	}
}
