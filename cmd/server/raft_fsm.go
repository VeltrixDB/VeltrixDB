package main

// raft_fsm.go — storage-backed Raft state machine.
//
// raftFSM implements consensus.SnapshotStateMachine (StateMachine + Snapshot /
// Restore) on top of *storage.StorageEngine.  Every mutating client operation in
// raft mode is gob-encoded into an fsmCmd, submitted to the Raft log via
// RaftNode.Submit, and applied — deterministically and in the same order — on
// every node through Apply.  Because Raft guarantees identical log order on all
// members and each node's engine is built from that same log, calling the
// engine's own Put/Delete/atomic methods inside Apply produces byte-identical
// state on every replica.
//
// # Returning results to the caller
//
// RaftNode.Submit returns only an error, but atomic ops (CAS/INCR/DECR/SETNX)
// and TXN need to return a value/status to the client.  We use a per-node
// result side-channel keyed by a globally-unique request ID:
//
//   - Before Submit, the coordinator calls register(reqID) to install a result
//     slot in the FSM.
//   - Apply computes the result and, if reqID is registered locally, stores it.
//   - Submit returns only after Apply has run (the applier signals the commit
//     waiter after Apply), so the coordinator reads the result immediately.
//
// Only the origin node (which is always the leader, since only the leader may
// Submit) has the reqID registered; followers apply the same command but drop
// the result.  reqIDs are unique per node (nodeID-scoped counter) so a follower
// never accidentally matches another node's request.

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/VeltrixDB/veltrixdb/storage"
)

// fsmOp enumerates the mutating operations carried through the Raft log.
type fsmOp uint8

const (
	opPut fsmOp = iota
	opDelete
	opMultiPut
	opCAS
	opIncr
	opDecr
	opSetNX
	opTxn

	// Namespace / hash-field / vector / secondary-index ops (B3 phase): these
	// were previously applied locally only, silently diverging in raft mode.
	opNSPut
	opNSDelete
	opNSDrop
	opHSet
	opHDel
	opHExpire
	opVSet
	opIdxCreate
	opIdxDrop

	// List / set data-type ops (list_set_ops.go)
	opLPush
	opRPush
	opLPop
	opRPop
	opSAdd
	opSRem
)

// fsmTxnOp is one operation inside an opTxn command.
type fsmTxnOp struct {
	Op              string // "SET" | "SETIF" | "DEL"
	Key             string
	Value           []byte
	TTL             int32
	ExpectedVersion uint64
}

// fsmCmd is the gob-encoded payload of a Raft log entry.
type fsmCmd struct {
	Op    fsmOp
	ReqID uint64 // 0 = no result expected (Put/Delete/MultiPut)

	Key   string
	Value []byte
	TTL   int32

	// opMultiPut
	Entries []storage.MultiPutRequest

	// opCAS
	Expected []byte

	// opIncr / opDecr
	Delta int64

	// opTxn
	TxnOps []fsmTxnOp

	// opNSPut / opNSDelete / opNSDrop — namespace; opVSet — vector namespace
	Ns string

	// opHSet / opHDel / opHExpire — hash field; opIdxCreate — indexed JSON field
	Field string

	// opVSet
	Vec []float32
}

func encodeCmd(c fsmCmd) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// fsmResult carries the outcome of an op back to the submitting coordinator.
type fsmResult struct {
	casRes   storage.CASResult
	setnxRes storage.SetNXResult
	intVal   int64
	bytesVal []byte // opLPop / opRPop payload
	boolVal  bool   // opLPop/opRPop found; opSAdd added; opSRem removed
	err      error
}

// raftFSM applies committed log commands to the storage engine.
type raftFSM struct {
	engine *storage.StorageEngine

	mu      sync.Mutex
	pending map[uint64]chan fsmResult
}

func newRaftFSM(engine *storage.StorageEngine) *raftFSM {
	return &raftFSM{
		engine:  engine,
		pending: make(map[uint64]chan fsmResult),
	}
}

// register installs a result slot for reqID and returns the channel Apply will
// signal.  Must be called before Submit.
func (f *raftFSM) register(reqID uint64) chan fsmResult {
	ch := make(chan fsmResult, 1)
	f.mu.Lock()
	f.pending[reqID] = ch
	f.mu.Unlock()
	return ch
}

func (f *raftFSM) unregister(reqID uint64) {
	f.mu.Lock()
	delete(f.pending, reqID)
	f.mu.Unlock()
}

// deliver publishes res to the reqID waiter if one is registered on this node.
func (f *raftFSM) deliver(reqID uint64, res fsmResult) {
	if reqID == 0 {
		return
	}
	f.mu.Lock()
	ch, ok := f.pending[reqID]
	f.mu.Unlock()
	if ok {
		select {
		case ch <- res:
		default:
		}
	}
}

// Apply decodes and executes one committed command against the storage engine.
// It always returns nil for op-level failures (e.g. CAS mismatch) — those are
// legitimate outcomes delivered via the result channel, not Raft-level errors.
// A non-nil return is reserved for decode failures, which indicate corruption.
func (f *raftFSM) Apply(command []byte) error {
	var c fsmCmd
	if err := gob.NewDecoder(bytes.NewReader(command)).Decode(&c); err != nil {
		return fmt.Errorf("raft-fsm decode: %w", err)
	}
	return f.applyOne(c)
}

// applyOne executes a single decoded command against the storage engine and
// delivers its result to the reqID waiter.  It is the shared core of both Apply
// (single entry) and ApplyBatch (non-PUT entries within a batch).
func (f *raftFSM) applyOne(c fsmCmd) error {
	switch c.Op {
	case opPut:
		err := f.engine.Put(c.Key, c.Value, c.TTL)
		f.deliver(c.ReqID, fsmResult{err: err})
		return nil

	case opDelete:
		err := f.engine.Delete(c.Key)
		f.deliver(c.ReqID, fsmResult{err: err})
		return nil

	case opMultiPut:
		errs := f.engine.MultiPut(c.Entries)
		var first error
		for _, e := range errs {
			if e != nil {
				first = e
				break
			}
		}
		f.deliver(c.ReqID, fsmResult{err: first})
		return nil

	case opCAS:
		res, err := f.engine.CompareAndSwap(c.Key, c.Expected, c.Value, c.TTL)
		f.deliver(c.ReqID, fsmResult{casRes: res, err: err})
		return nil

	case opIncr:
		v, err := f.engine.Increment(c.Key, c.Delta, c.TTL)
		f.deliver(c.ReqID, fsmResult{intVal: v, err: err})
		return nil

	case opDecr:
		v, err := f.engine.Decrement(c.Key, c.Delta, c.TTL)
		f.deliver(c.ReqID, fsmResult{intVal: v, err: err})
		return nil

	case opSetNX:
		res, err := f.engine.SetIfNotExists(c.Key, c.Value, c.TTL)
		f.deliver(c.ReqID, fsmResult{setnxRes: res, err: err})
		return nil

	case opTxn:
		err := f.applyTxn(c.TxnOps)
		f.deliver(c.ReqID, fsmResult{err: err})
		return nil

	case opNSPut:
		err := f.engine.PutNS(c.Ns, c.Key, c.Value, c.TTL)
		f.deliver(c.ReqID, fsmResult{err: err})
		return nil

	case opNSDelete:
		err := f.engine.DeleteNS(c.Ns, c.Key)
		f.deliver(c.ReqID, fsmResult{err: err})
		return nil

	case opNSDrop:
		n, err := f.engine.DropNamespace(c.Ns)
		f.deliver(c.ReqID, fsmResult{intVal: int64(n), err: err})
		return nil

	case opHSet:
		err := f.engine.HSet(c.Key, c.Field, c.Value, c.TTL)
		f.deliver(c.ReqID, fsmResult{err: err})
		return nil

	case opHDel:
		err := f.engine.HDel(c.Key, c.Field)
		f.deliver(c.ReqID, fsmResult{err: err})
		return nil

	case opHExpire:
		err := f.engine.HExpire(c.Key, c.Field, c.TTL)
		f.deliver(c.ReqID, fsmResult{err: err})
		return nil

	case opVSet:
		err := f.engine.RegisterVectorNamespace(c.Ns, len(c.Vec))
		if err == nil {
			err = f.engine.PutVector(c.Ns, c.Key, c.Vec)
		}
		f.deliver(c.ReqID, fsmResult{err: err})
		return nil

	case opIdxCreate:
		err := f.engine.CreateFieldIndex(c.Key, c.Field)
		f.deliver(c.ReqID, fsmResult{err: err})
		return nil

	case opIdxDrop:
		err := f.engine.DropFieldIndex(c.Key)
		f.deliver(c.ReqID, fsmResult{err: err})
		return nil

	case opLPush, opRPush:
		var n int64
		var err error
		if c.Op == opLPush {
			n, _, err = f.engine.LPush(c.Key, c.Value)
		} else {
			n, _, err = f.engine.RPush(c.Key, c.Value)
		}
		f.deliver(c.ReqID, fsmResult{intVal: n, err: err})
		return nil

	case opLPop, opRPop:
		var v []byte
		var found bool
		var err error
		if c.Op == opLPop {
			v, found, _, err = f.engine.LPop(c.Key)
		} else {
			v, found, _, err = f.engine.RPop(c.Key)
		}
		f.deliver(c.ReqID, fsmResult{bytesVal: v, boolVal: found, err: err})
		return nil

	case opSAdd:
		added, _, err := f.engine.SAdd(c.Key, c.Field)
		f.deliver(c.ReqID, fsmResult{boolVal: added, err: err})
		return nil

	case opSRem:
		removed, _, err := f.engine.SRem(c.Key, c.Field)
		f.deliver(c.ReqID, fsmResult{boolVal: removed, err: err})
		return nil

	default:
		return fmt.Errorf("raft-fsm: unknown op %d", c.Op)
	}
}

// applyTxn runs a one-shot optimistic transaction against the engine.  It
// returns storage.ErrTxnConflict when a SETIF version guard fails.
func (f *raftFSM) applyTxn(ops []fsmTxnOp) error {
	txn := f.engine.BeginTxn()
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
			return fmt.Errorf("raft-fsm txn: unknown op %q", op.Op)
		}
	}
	return txn.Commit()
}

// ── Snapshot / Restore (consensus.SnapshotStateMachine) ─────────────────────

// snapshotEntry is one key/value pair in a state-machine snapshot.
type snapshotEntry struct {
	Key   string
	Value []byte
}

// Snapshot serialises the entire live keyspace as a gob-encoded stream of
// key/value pairs.  It walks the engine with a paginated ScanCursor so a large
// keyspace never materialises a single giant slice in the scan itself.  TTLs
// are intentionally not preserved across a snapshot (restored keys become
// immortal) — the same simplification the WAL-replay path documents for
// crash recovery of already-expired keys.
func (f *raftFSM) Snapshot() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	cursor := ""
	const page = 1024
	for {
		kvs, next, err := f.engine.ScanCursor(cursor, page)
		if err != nil {
			return nil, fmt.Errorf("raft-fsm snapshot scan: %w", err)
		}
		for _, kv := range kvs {
			if err := enc.Encode(snapshotEntry{Key: kv.Key, Value: kv.Value}); err != nil {
				return nil, fmt.Errorf("raft-fsm snapshot encode: %w", err)
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return buf.Bytes(), nil
}

// Restore rebuilds engine state from a snapshot produced by Snapshot.  It is
// called on a follower that fell too far behind for log replication; the
// engine already reflects the pre-snapshot state, and each Put overwrites or
// inserts as needed to converge on the leader's snapshot.
func (f *raftFSM) Restore(data []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	for {
		var e snapshotEntry
		if err := dec.Decode(&e); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("raft-fsm restore decode: %w", err)
		}
		if err := f.engine.Put(e.Key, e.Value, -1); err != nil {
			return fmt.Errorf("raft-fsm restore put %q: %w", e.Key, err)
		}
		// Vectors arrive in a snapshot as plain "@vec/..." KV pairs; refresh
		// the in-RAM searchable index too.
		if storage.IsVectorKey(e.Key) {
			if err := f.engine.LoadVectorBlob(e.Key, e.Value); err != nil {
				return fmt.Errorf("raft-fsm restore vector %q: %w", e.Key, err)
			}
		}
	}
	return nil
}
