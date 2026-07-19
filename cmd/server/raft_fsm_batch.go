//go:build !nobatch

package main

// raft_fsm_batch.go — batched apply path for the storage-backed Raft FSM.
//
// Defining ApplyBatch here makes *raftFSM satisfy consensus.BatchStateMachine,
// so the Raft applier hands it contiguous runs of committed writes in ONE call.
// The `nobatch` build tag drops this file, leaving raftFSM with only Apply — the
// pre-batching single-entry behaviour — which is used to benchmark the "before"
// baseline against the batched "after" build.

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/VeltrixDB/veltrixdb/storage"
)

// ApplyBatch applies a contiguous run of committed commands (consensus.
// BatchStateMachine).  It is semantically identical to calling Apply once per
// command in order, but coalesces consecutive plain PUTs into a single
// engine.MultiPut so the storage group-commit amortises ONE WAL+VLog fdatasync
// across the whole PUT run instead of paying one per key — the fix for the
// serial-applier fsync bottleneck.  DELETE / atomic / TXN / MultiPut commands
// break the PUT run and are applied individually via applyOne, preserving log
// order exactly.
//
// results[i] is the Raft-level outcome of commands[i], mirroring Apply: nil for
// a successful or op-level-failed apply (op-level failures such as a CAS
// mismatch reach the client through the per-request result channel, not here),
// non-nil only for an undecodable command.
func (f *raftFSM) ApplyBatch(commands [][]byte) []error {
	results := make([]error, len(commands))
	cmds := make([]fsmCmd, len(commands))
	decoded := make([]bool, len(commands))
	for i, b := range commands {
		if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&cmds[i]); err != nil {
			results[i] = fmt.Errorf("raft-fsm decode: %w", err)
			continue
		}
		decoded[i] = true
	}

	i := 0
	for i < len(cmds) {
		// Skip commands that failed to decode; they keep their decode error.
		if !decoded[i] {
			i++
			continue
		}
		// Coalesce a maximal run of plain PUTs into one MultiPut.
		if cmds[i].Op == opPut {
			j := i
			for j < len(cmds) && decoded[j] && cmds[j].Op == opPut {
				j++
			}
			reqs := make([]storage.MultiPutRequest, 0, j-i)
			for k := i; k < j; k++ {
				reqs = append(reqs, storage.MultiPutRequest{
					Key: cmds[k].Key, Value: cmds[k].Value, TTL: cmds[k].TTL,
				})
			}
			errs := f.engine.MultiPut(reqs)
			for k := i; k < j; k++ {
				var e error
				if k-i < len(errs) {
					e = errs[k-i]
				}
				// Mirror the single-PUT Apply path: the storage error is
				// delivered to the reqID waiter, and the Raft-level result stays
				// nil (a failed Put is not a Raft-level/decode failure).
				f.deliver(cmds[k].ReqID, fsmResult{err: e})
			}
			i = j
			continue
		}
		// Non-PUT: apply individually, exactly as the single-entry path would.
		results[i] = f.applyOne(cmds[i])
		i++
	}
	return results
}
