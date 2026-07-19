package main

// coordinator.go — the write chokepoint that wires the distributed layer into
// the serving path.
//
// Every mutating request handled by the TCP server goes through a *coordinator
// instead of calling the storage engine directly.  The coordinator's behaviour
// depends on the deployment mode selected at startup (--mode):
//
//	standalone  — writes go straight to the local engine.  This is byte-for-byte
//	              the pre-existing single-node behaviour: no Raft, no replication,
//	              no redirects.  100% backward compatible.
//
//	raft        — writes are gob-encoded and submitted to the Raft log via
//	              RaftNode.Submit.  They commit once a quorum has persisted the
//	              entry and are applied on every node through the storage-backed
//	              FSM (raft_fsm.go).  A write received by a follower is rejected
//	              with a MOVED redirect carrying the current leader's client
//	              address, which the cluster-aware client transparently follows.
//	              Reads default to local (fast, possibly stale); see the
//	              consistency notes in coord.leaderReadRequired.
//
//	replicated  — writes are applied to the local engine first, then handed to
//	              the replication engine.  The consistency level decides when the
//	              client is ACKed:
//	                Eventual (async) — ACK immediately after the local write;
//	                                   replicas catch up in the background.
//	                Quorum          — ACK only after a majority of replicas
//	                                   (counting the local copy) have applied it.
//	                Strong          — ACK only after ALL replicas have applied it.
//	              Quorum/Strong surface ErrReplicationTimeout / ErrQuorumNotReached
//	              to the client when the required copies cannot be reached.
//
// # Consistency guarantees (honest statement)
//
//	raft mode:        Linearizable WRITES (single Raft group, quorum commit).
//	                  Reads are eventually-consistent by default (served from the
//	                  local applied state).  There is NO cross-key linearizable
//	                  read path in this phase — we do not claim it.
//	replicated mode:  Primary-copy replication.  Quorum/Strong give durability
//	                  across N copies before ACK but NOT linearizability under
//	                  concurrent writers, because there is no leader election or
//	                  single-writer ordering — every node accepts writes for its
//	                  local keyspace.  Reads are local.

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/VeltrixDB/veltrixdb/cluster"
	"github.com/VeltrixDB/veltrixdb/consensus"
	"github.com/VeltrixDB/veltrixdb/replication"
	"github.com/VeltrixDB/veltrixdb/storage"
)

type deployMode int

const (
	modeStandalone deployMode = iota
	modeRaft
	modeReplicated
)

func parseMode(s string) (deployMode, error) {
	switch s {
	case "", "standalone":
		return modeStandalone, nil
	case "raft":
		return modeRaft, nil
	case "replicated":
		return modeReplicated, nil
	default:
		return modeStandalone, fmt.Errorf("unknown --mode %q (want standalone|raft|replicated)", s)
	}
}

// redirectError signals that the request must be retried against another node
// (the current Raft leader).  Handlers surface it to clients as "MOVED <addr>",
// mirroring the Redis cluster redirect convention.
type redirectError struct {
	leaderID string
	addr     string // client-facing "host:port" of the leader, "" if unknown
}

func (e *redirectError) Error() string {
	if e.addr == "" {
		return "MOVED - (leader unknown, retry)"
	}
	return fmt.Sprintf("MOVED %s %s", e.addr, e.leaderID)
}

// coordinator routes mutating operations according to the deployment mode.
type coordinator struct {
	mode    deployMode
	engine  *storage.StorageEngine
	localID string
	pm      *cluster.PartitionMap // partition map (all modes) for topology lookups

	// raft mode
	raft     *consensus.RaftNode
	fsm      *raftFSM
	reqSeq   atomic.Uint64
	nodeSalt uint64 // per-node high bits so reqIDs never collide across nodes
	// peerClientAddr maps a nodeID to its client-facing storage address so a
	// follower can tell a client where the leader is.
	peerClientAddr map[string]string

	// replicated mode
	repl        *replication.ReplicationEngine
	consistency replication.ConsistencyLevel
	replFactor  int
	replTimeout int // ms

	// linReads (raft mode, --linearizable-reads): GET runs the ReadIndex fence
	// on the leader before reading; followers redirect with MOVED.
	linReads bool
}

// readIndexTimeout bounds one linearizable-read fence (quorum heartbeat round
// plus apply catch-up).
const readIndexTimeout = 2 * time.Second

// Get serves a read.  Default is a local engine read (fast; in raft mode this
// may be stale on followers or a deposed leader).  With linReads set in raft
// mode it is linearizable via ReadIndex.
func (c *coordinator) Get(key string) ([]byte, error) {
	if c.mode != modeRaft || !c.linReads {
		return c.engine.Get(key)
	}
	if !c.raft.IsLeader() {
		return nil, c.leaderRedirect()
	}
	if _, err := c.raft.ReadIndex(readIndexTimeout); err != nil {
		if err == consensus.ErrNotLeader {
			return nil, c.leaderRedirect()
		}
		return nil, err
	}
	return c.engine.Get(key)
}

// newStandaloneCoordinator wraps an engine with pass-through write routing.
// Used for --mode=standalone and by the in-process unit tests.
func newStandaloneCoordinator(engine *storage.StorageEngine) *coordinator {
	return &coordinator{mode: modeStandalone, engine: engine}
}

// nextReqID returns a process-globally-unique request id (node salt || counter).
func (c *coordinator) nextReqID() uint64 {
	return c.nodeSalt ^ c.reqSeq.Add(1)
}

// leaderRedirect builds a redirectError pointing at the current Raft leader.
func (c *coordinator) leaderRedirect() *redirectError {
	leaderID := c.raft.GetLeaderID()
	return &redirectError{leaderID: leaderID, addr: c.peerClientAddr[leaderID]}
}

// submitForResult registers a result slot, submits the command, and waits for
// the FSM to deliver the op result.  Only used for ops that must return a value
// or status (CAS/INCR/DECR/SETNX/TXN).
func (c *coordinator) submitForResult(cmd fsmCmd) (fsmResult, error) {
	if !c.raft.IsLeader() {
		return fsmResult{}, c.leaderRedirect()
	}
	reqID := c.nextReqID()
	cmd.ReqID = reqID
	ch := c.fsm.register(reqID)
	defer c.fsm.unregister(reqID)

	enc, err := encodeCmd(cmd)
	if err != nil {
		return fsmResult{}, err
	}
	if err := c.raft.Submit(enc); err != nil {
		if err == consensus.ErrNotLeader {
			return fsmResult{}, c.leaderRedirect()
		}
		return fsmResult{}, err
	}
	select {
	case res := <-ch:
		return res, nil
	case <-time.After(5 * time.Second):
		return fsmResult{}, fmt.Errorf("raft: result not delivered for committed entry")
	}
}

// submitVoid submits a command whose only outcome is success/failure.
func (c *coordinator) submitVoid(cmd fsmCmd) error {
	if !c.raft.IsLeader() {
		return c.leaderRedirect()
	}
	enc, err := encodeCmd(cmd)
	if err != nil {
		return err
	}
	if err := c.raft.Submit(enc); err != nil {
		if err == consensus.ErrNotLeader {
			return c.leaderRedirect()
		}
		return err
	}
	return nil
}

// ── Replicated-mode helper ──────────────────────────────────────────────────

// replicateWrite hands a completed local write to the replication engine and,
// for Quorum/Strong consistency, blocks until the required copies acknowledge.
func (c *coordinator) replicateWrite(key string, value []byte, ttl int32, tombstone bool) error {
	seq := c.reqSeq.Add(1)
	op := &replication.WriteOperation{
		SeqNum:      seq,
		Key:         key,
		Value:       value,
		Timestamp:   time.Now().UnixNano(),
		TTL:         ttl,
		NodeID:      c.localID,
		IsTombstone: tombstone,
	}
	if err := c.repl.OnLocalWrite(op); err != nil {
		return err
	}
	// Async (Eventual): ACK the client immediately; replicas catch up.
	if c.consistency == replication.EventualConsistency {
		return nil
	}
	// Quorum: majority of replFactor copies (local counts as one).
	// Strong: all copies.
	target := c.replFactor
	if c.consistency == replication.QuorumConsistency {
		target = c.replFactor/2 + 1
	}
	timeout := c.replTimeout
	if timeout <= 0 {
		timeout = 10000
	}
	return c.repl.WaitForReplication(seq, target, timeout)
}

// ── Public write API (called by the TCP handlers) ───────────────────────────

// Put stores key=value.  Returns a *redirectError on a follower in raft mode.
func (c *coordinator) Put(key string, value []byte, ttl int32) error {
	switch c.mode {
	case modeStandalone:
		return c.engine.Put(key, value, ttl)
	case modeRaft:
		return c.submitVoid(fsmCmd{Op: opPut, Key: key, Value: value, TTL: ttl})
	case modeReplicated:
		if err := c.engine.Put(key, value, ttl); err != nil {
			return err
		}
		return c.replicateWrite(key, value, ttl, false)
	}
	return fmt.Errorf("coordinator: bad mode")
}

// Delete removes key.
func (c *coordinator) Delete(key string) error {
	switch c.mode {
	case modeStandalone:
		return c.engine.Delete(key)
	case modeRaft:
		return c.submitVoid(fsmCmd{Op: opDelete, Key: key})
	case modeReplicated:
		if err := c.engine.Delete(key); err != nil {
			return err
		}
		return c.replicateWrite(key, nil, -1, true)
	}
	return fmt.Errorf("coordinator: bad mode")
}

// MultiPut executes a vectorized write batch.  In distributed modes a batch
// commits atomically as one log entry (raft) or is replicated entry-by-entry
// (replicated).  The returned slice has one error per input request.
func (c *coordinator) MultiPut(reqs []storage.MultiPutRequest) []error {
	switch c.mode {
	case modeStandalone:
		return c.engine.MultiPut(reqs)
	case modeRaft:
		err := c.submitVoid(fsmCmd{Op: opMultiPut, Entries: reqs})
		errs := make([]error, len(reqs))
		if err != nil {
			for i := range errs {
				errs[i] = err
			}
		}
		return errs
	case modeReplicated:
		errs := c.engine.MultiPut(reqs)
		for i, r := range reqs {
			if errs[i] == nil {
				if rerr := c.replicateWrite(r.Key, r.Value, r.TTL, false); rerr != nil {
					errs[i] = rerr
				}
			}
		}
		return errs
	}
	errs := make([]error, len(reqs))
	for i := range errs {
		errs[i] = fmt.Errorf("coordinator: bad mode")
	}
	return errs
}

// CompareAndSwap routes an atomic CAS.
func (c *coordinator) CompareAndSwap(key string, expected, newValue []byte, ttl int32) (storage.CASResult, error) {
	switch c.mode {
	case modeStandalone:
		return c.engine.CompareAndSwap(key, expected, newValue, ttl)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opCAS, Key: key, Expected: expected, Value: newValue, TTL: ttl})
		if err != nil {
			return storage.CASMismatch, err
		}
		return res.casRes, res.err
	case modeReplicated:
		res, err := c.engine.CompareAndSwap(key, expected, newValue, ttl)
		if err == nil && res == storage.CASSuccess {
			if rerr := c.replicateWrite(key, newValue, ttl, false); rerr != nil {
				return res, rerr
			}
		}
		return res, err
	}
	return storage.CASMismatch, fmt.Errorf("coordinator: bad mode")
}

// Increment routes an atomic increment.
func (c *coordinator) Increment(key string, delta int64, ttl int32) (int64, error) {
	return c.incrDecr(opIncr, key, delta, ttl)
}

// Decrement routes an atomic decrement.
func (c *coordinator) Decrement(key string, delta int64, ttl int32) (int64, error) {
	return c.incrDecr(opDecr, key, delta, ttl)
}

func (c *coordinator) incrDecr(op fsmOp, key string, delta int64, ttl int32) (int64, error) {
	switch c.mode {
	case modeStandalone:
		if op == opIncr {
			return c.engine.Increment(key, delta, ttl)
		}
		return c.engine.Decrement(key, delta, ttl)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: op, Key: key, Delta: delta, TTL: ttl})
		if err != nil {
			return 0, err
		}
		return res.intVal, res.err
	case modeReplicated:
		var v int64
		var err error
		if op == opIncr {
			v, err = c.engine.Increment(key, delta, ttl)
		} else {
			v, err = c.engine.Decrement(key, delta, ttl)
		}
		if err == nil {
			if cur, gerr := c.engine.Get(key); gerr == nil {
				if rerr := c.replicateWrite(key, cur, ttl, false); rerr != nil {
					return v, rerr
				}
			}
		}
		return v, err
	}
	return 0, fmt.Errorf("coordinator: bad mode")
}

// SetIfNotExists routes an atomic SETNX.
func (c *coordinator) SetIfNotExists(key string, value []byte, ttl int32) (storage.SetNXResult, error) {
	switch c.mode {
	case modeStandalone:
		return c.engine.SetIfNotExists(key, value, ttl)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opSetNX, Key: key, Value: value, TTL: ttl})
		if err != nil {
			return storage.SetNXExists, err
		}
		return res.setnxRes, res.err
	case modeReplicated:
		res, err := c.engine.SetIfNotExists(key, value, ttl)
		if err == nil && res == storage.SetNXCreated {
			if rerr := c.replicateWrite(key, value, ttl, false); rerr != nil {
				return res, rerr
			}
		}
		return res, err
	}
	return storage.SetNXExists, fmt.Errorf("coordinator: bad mode")
}

// Txn routes a one-shot optimistic transaction.  Returns storage.ErrTxnConflict
// when a SETIF version guard fails.
func (c *coordinator) Txn(ops []fsmTxnOp) error {
	switch c.mode {
	case modeStandalone:
		return c.runLocalTxn(ops)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opTxn, TxnOps: ops})
		if err != nil {
			return err
		}
		return res.err
	case modeReplicated:
		if err := c.runLocalTxn(ops); err != nil {
			return err
		}
		// Replicate the effect of each committed op.
		for _, op := range ops {
			switch op.Op {
			case "SET", "SETIF":
				if rerr := c.replicateWrite(op.Key, op.Value, op.TTL, false); rerr != nil {
					return rerr
				}
			case "DEL":
				if rerr := c.replicateWrite(op.Key, nil, -1, true); rerr != nil {
					return rerr
				}
			}
		}
		return nil
	}
	return fmt.Errorf("coordinator: bad mode")
}

// ── Namespace / hash-field / vector / index write API (B3) ──────────────────
//
// These ops all decompose to plain KV writes on composite internal keys
// (storage.NSKey / storage.HashFieldKey / storage.VectorPersistKey), so in
// replicated mode they replicate as ordinary WriteOperations and the replica's
// applyFn reconstructs any derived state (vector RAM index).  In raft mode
// each op is its own log entry applied via the engine's method so quota and
// index side-effects stay deterministic across members.

// PutNS routes a namespaced write.
func (c *coordinator) PutNS(ns, key string, value []byte, ttl int32) error {
	switch c.mode {
	case modeStandalone:
		return c.engine.PutNS(ns, key, value, ttl)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opNSPut, Ns: ns, Key: key, Value: value, TTL: ttl})
		if err != nil {
			return err
		}
		return res.err
	case modeReplicated:
		if err := c.engine.PutNS(ns, key, value, ttl); err != nil {
			return err
		}
		return c.replicateWrite(storage.NSKey(ns, key), value, ttl, false)
	}
	return fmt.Errorf("coordinator: bad mode")
}

// DeleteNS routes a namespaced delete.
func (c *coordinator) DeleteNS(ns, key string) error {
	switch c.mode {
	case modeStandalone:
		return c.engine.DeleteNS(ns, key)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opNSDelete, Ns: ns, Key: key})
		if err != nil {
			return err
		}
		return res.err
	case modeReplicated:
		if err := c.engine.DeleteNS(ns, key); err != nil {
			return err
		}
		return c.replicateWrite(storage.NSKey(ns, key), nil, -1, true)
	}
	return fmt.Errorf("coordinator: bad mode")
}

// DropNamespace routes a namespace drop; returns the number of keys deleted.
func (c *coordinator) DropNamespace(ns string) (int, error) {
	switch c.mode {
	case modeStandalone:
		return c.engine.DropNamespace(ns)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opNSDrop, Ns: ns})
		if err != nil {
			return 0, err
		}
		return int(res.intVal), res.err
	case modeReplicated:
		// Enumerate first so each deleted key ships as its own tombstone.
		entries, err := c.engine.ScanNamespace(ns, "", 0)
		if err != nil {
			return 0, err
		}
		n := 0
		for _, e := range entries {
			if err := c.DeleteNS(ns, e.Key); err != nil {
				return n, err
			}
			n++
		}
		return n, nil
	}
	return 0, fmt.Errorf("coordinator: bad mode")
}

// HSet routes a hash-field write.
func (c *coordinator) HSet(key, field string, value []byte, ttl int32) error {
	switch c.mode {
	case modeStandalone:
		return c.engine.HSet(key, field, value, ttl)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opHSet, Key: key, Field: field, Value: value, TTL: ttl})
		if err != nil {
			return err
		}
		return res.err
	case modeReplicated:
		if err := c.engine.HSet(key, field, value, ttl); err != nil {
			return err
		}
		return c.replicateWrite(storage.HashFieldKey(key, field), value, ttl, false)
	}
	return fmt.Errorf("coordinator: bad mode")
}

// HDel routes a hash-field delete.
func (c *coordinator) HDel(key, field string) error {
	switch c.mode {
	case modeStandalone:
		return c.engine.HDel(key, field)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opHDel, Key: key, Field: field})
		if err != nil {
			return err
		}
		return res.err
	case modeReplicated:
		if err := c.engine.HDel(key, field); err != nil {
			return err
		}
		return c.replicateWrite(storage.HashFieldKey(key, field), nil, -1, true)
	}
	return fmt.Errorf("coordinator: bad mode")
}

// HExpire routes a hash-field TTL update.
func (c *coordinator) HExpire(key, field string, ttl int32) error {
	switch c.mode {
	case modeStandalone:
		return c.engine.HExpire(key, field, ttl)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opHExpire, Key: key, Field: field, TTL: ttl})
		if err != nil {
			return err
		}
		return res.err
	case modeReplicated:
		if err := c.engine.HExpire(key, field, ttl); err != nil {
			return err
		}
		val, err := c.engine.HGet(key, field)
		if err != nil {
			return err
		}
		return c.replicateWrite(storage.HashFieldKey(key, field), val, ttl, false)
	}
	return fmt.Errorf("coordinator: bad mode")
}

// VSet routes a vector upsert into vector namespace ns.
func (c *coordinator) VSet(ns, id string, vec []float32) error {
	switch c.mode {
	case modeStandalone:
		if err := c.engine.RegisterVectorNamespace(ns, len(vec)); err != nil {
			return err
		}
		return c.engine.PutVector(ns, id, vec)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opVSet, Ns: ns, Key: id, Vec: vec})
		if err != nil {
			return err
		}
		return res.err
	case modeReplicated:
		if err := c.engine.RegisterVectorNamespace(ns, len(vec)); err != nil {
			return err
		}
		if err := c.engine.PutVector(ns, id, vec); err != nil {
			return err
		}
		// Replicate the exact persisted (normalized) blob; the replica's
		// applyFn detects the "@vec/" prefix and refreshes its RAM index.
		pk := storage.VectorPersistKey(ns, id)
		blob, err := c.engine.Get(pk)
		if err != nil {
			return err
		}
		return c.replicateWrite(pk, blob, -1, false)
	}
	return fmt.Errorf("coordinator: bad mode")
}

// IdxCreate routes secondary-index creation.  NOTE (replicated mode): index
// metadata is node-local — replicas index incoming writes only if the same
// index is created on them too; raft mode replicates the metadata itself.
func (c *coordinator) IdxCreate(name, field string) error {
	switch c.mode {
	case modeStandalone, modeReplicated:
		return c.engine.CreateFieldIndex(name, field)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opIdxCreate, Key: name, Field: field})
		if err != nil {
			return err
		}
		return res.err
	}
	return fmt.Errorf("coordinator: bad mode")
}

// IdxDrop routes secondary-index removal (see IdxCreate note).
func (c *coordinator) IdxDrop(name string) error {
	switch c.mode {
	case modeStandalone, modeReplicated:
		return c.engine.DropFieldIndex(name)
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opIdxDrop, Key: name})
		if err != nil {
			return err
		}
		return res.err
	}
	return fmt.Errorf("coordinator: bad mode")
}

// ── List / set write API ─────────────────────────────────────────────────────
//
// The engine reports each op's concrete KV effects (storage.ListMutation), so
// replicated mode ships them as ordinary KV traffic; raft mode replays the op
// itself deterministically on every member.

// replicateMuts ships a list/set op's KV effects to the replicas.
func (c *coordinator) replicateMuts(muts []storage.ListMutation) error {
	for _, m := range muts {
		if err := c.replicateWrite(m.Key, m.Value, -1, m.Tombstone); err != nil {
			return err
		}
	}
	return nil
}

// ListPush routes LPUSH/RPUSH; returns the new list length.
func (c *coordinator) ListPush(key string, val []byte, left bool) (int64, error) {
	switch c.mode {
	case modeStandalone, modeReplicated:
		var n int64
		var muts []storage.ListMutation
		var err error
		if left {
			n, muts, err = c.engine.LPush(key, val)
		} else {
			n, muts, err = c.engine.RPush(key, val)
		}
		if err != nil {
			return 0, err
		}
		if c.mode == modeReplicated {
			if rerr := c.replicateMuts(muts); rerr != nil {
				return n, rerr
			}
		}
		return n, nil
	case modeRaft:
		op := opRPush
		if left {
			op = opLPush
		}
		res, err := c.submitForResult(fsmCmd{Op: op, Key: key, Value: val})
		if err != nil {
			return 0, err
		}
		return res.intVal, res.err
	}
	return 0, fmt.Errorf("coordinator: bad mode")
}

// ListPop routes LPOP/RPOP; found=false on an empty list.
func (c *coordinator) ListPop(key string, left bool) ([]byte, bool, error) {
	switch c.mode {
	case modeStandalone, modeReplicated:
		var v []byte
		var found bool
		var muts []storage.ListMutation
		var err error
		if left {
			v, found, muts, err = c.engine.LPop(key)
		} else {
			v, found, muts, err = c.engine.RPop(key)
		}
		if err != nil {
			return nil, false, err
		}
		if c.mode == modeReplicated && len(muts) > 0 {
			if rerr := c.replicateMuts(muts); rerr != nil {
				return v, found, rerr
			}
		}
		return v, found, nil
	case modeRaft:
		op := opRPop
		if left {
			op = opLPop
		}
		res, err := c.submitForResult(fsmCmd{Op: op, Key: key})
		if err != nil {
			return nil, false, err
		}
		return res.bytesVal, res.boolVal, res.err
	}
	return nil, false, fmt.Errorf("coordinator: bad mode")
}

// SetAdd routes SADD; added=false when the member already existed.
func (c *coordinator) SetAdd(key, member string) (bool, error) {
	switch c.mode {
	case modeStandalone, modeReplicated:
		added, muts, err := c.engine.SAdd(key, member)
		if err != nil {
			return false, err
		}
		if c.mode == modeReplicated {
			if rerr := c.replicateMuts(muts); rerr != nil {
				return added, rerr
			}
		}
		return added, nil
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opSAdd, Key: key, Field: member})
		if err != nil {
			return false, err
		}
		return res.boolVal, res.err
	}
	return false, fmt.Errorf("coordinator: bad mode")
}

// SetRem routes SREM; removed=false when the member was absent.
func (c *coordinator) SetRem(key, member string) (bool, error) {
	switch c.mode {
	case modeStandalone, modeReplicated:
		removed, muts, err := c.engine.SRem(key, member)
		if err != nil {
			return false, err
		}
		if c.mode == modeReplicated && len(muts) > 0 {
			if rerr := c.replicateMuts(muts); rerr != nil {
				return removed, rerr
			}
		}
		return removed, nil
	case modeRaft:
		res, err := c.submitForResult(fsmCmd{Op: opSRem, Key: key, Field: member})
		if err != nil {
			return false, err
		}
		return res.boolVal, res.err
	}
	return false, fmt.Errorf("coordinator: bad mode")
}

func (c *coordinator) runLocalTxn(ops []fsmTxnOp) error {
	txn := c.engine.BeginTxn()
	for _, op := range ops {
		switch op.Op {
		case "SET":
			txn.Set(op.Key, op.Value, op.TTL)
		case "SETIF":
			txn.SetIf(op.Key, op.Value, op.TTL, op.ExpectedVersion)
		case "DEL":
			txn.Delete(op.Key)
		default:
			txn.Abort()
			return fmt.Errorf("txn: unknown op %q", op.Op)
		}
	}
	return txn.Commit()
}
