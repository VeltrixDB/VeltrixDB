package integration_test

// antientropy_test.go — end-to-end anti-entropy over the REAL replication
// transport (TCP loopback): a replica that MISSED writes entirely (started
// after the writes, no live delivery) is backfilled to full convergence by the
// digest-based reconcile path. This is the recovery the old retry-only
// anti-entropy could not do.

import (
	"fmt"
	"net"
	"testing"

	"github.com/VeltrixDB/veltrixdb/replication"
	"github.com/VeltrixDB/veltrixdb/storage"
)

// syncBridge wires one engine's anti-entropy handlers onto its replication
// engine and exposes a storage.PeerSync to pull from a peer — the same glue
// cmd/server uses in production, exercised here over real TCP.
func setSyncHandlers(re *replication.ReplicationEngine, eng *storage.StorageEngine) {
	re.SetSyncHandlers(
		eng.ShardDigests,
		func(shardID uint16) ([]replication.SyncEntry, error) {
			entries, err := eng.FetchShard(shardID)
			if err != nil {
				return nil, err
			}
			out := make([]replication.SyncEntry, len(entries))
			for i, e := range entries {
				out[i] = replication.SyncEntry{
					Key: e.Key, Value: e.Value, LWWStampNs: e.LWWStampNs,
					Tombstone: e.Tombstone, TTLSeconds: e.TTLSeconds,
				}
			}
			return out, nil
		},
	)
}

type peerOverTCP struct {
	re *replication.ReplicationEngine
	id string
}

func (p peerOverTCP) ShardDigests() (map[uint16]uint64, error) { return p.re.FetchPeerDigests(p.id) }
func (p peerOverTCP) FetchShard(shardID uint16) ([]storage.SyncEntry, error) {
	wire, err := p.re.FetchPeerShard(p.id, shardID)
	if err != nil {
		return nil, err
	}
	out := make([]storage.SyncEntry, len(wire))
	for i, e := range wire {
		out[i] = storage.SyncEntry{
			Key: e.Key, Value: e.Value, LWWStampNs: e.LWWStampNs,
			Tombstone: e.Tombstone, TTLSeconds: e.TTLSeconds,
		}
	}
	return out, nil
}

func freeReplPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestIntegration_AntiEntropyBackfillOverTCP(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping anti-entropy integration test in short mode")
	}

	engA := newTestEngineInDir(t, mustMkdirTemp(t, "ae-a-"))
	engB := newTestEngineInDir(t, mustMkdirTemp(t, "ae-b-"))

	reA := replication.NewReplicationEngine("A", replication.DefaultReplicationConfig())
	reB := replication.NewReplicationEngine("B", replication.DefaultReplicationConfig())
	reA.Start()
	reB.Start()
	t.Cleanup(func() { reA.Close(); reB.Close() })

	setSyncHandlers(reA, engA)
	setSyncHandlers(reB, engB)

	portA := freeReplPort(t)
	portB := freeReplPort(t)
	applyA := func(op *replication.WriteOperation) error {
		_, err := engA.ApplyLWWPut(op.Key, op.Value, op.TTL, op.Timestamp)
		return err
	}
	applyB := func(op *replication.WriteOperation) error {
		_, err := engB.ApplyLWWPut(op.Key, op.Value, op.TTL, op.Timestamp)
		return err
	}
	if err := reA.StartReplicationServer(fmt.Sprintf("127.0.0.1:%d", portA), applyA); err != nil {
		t.Fatalf("server A: %v", err)
	}
	if err := reB.StartReplicationServer(fmt.Sprintf("127.0.0.1:%d", portB), applyB); err != nil {
		t.Fatalf("server B: %v", err)
	}
	// Each node can reach the other's replication server (replPort = the actual
	// listen port; storage port is unused in this test).
	if err := reA.AddReplicaWithReplPort("B", "127.0.0.1", 0, portB); err != nil {
		t.Fatalf("A add B: %v", err)
	}
	if err := reB.AddReplicaWithReplPort("A", "127.0.0.1", 0, portA); err != nil {
		t.Fatalf("B add A: %v", err)
	}

	// A takes 500 writes that B NEVER receives via the live path (no OnLocalWrite
	// is issued) — B is effectively partitioned for the whole write burst.
	const n = 500
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		if _, err := engA.ApplyLWWPut(key, []byte(fmt.Sprintf("val-%d", i)), -1, int64(1_000_000+i)); err != nil {
			t.Fatalf("A write: %v", err)
		}
	}
	// A deletes a few so tombstones are part of the backfill.
	for i := 0; i < 20; i++ {
		engA.ApplyLWWDelete(fmt.Sprintf("key-%04d", i), int64(2_000_000+i))
	}

	// Sanity: B is empty before reconcile.
	if _, err := engB.Get("key-0100"); err == nil {
		t.Fatal("precondition failed: B already has A's data")
	}

	// Anti-entropy: B reconciles from A over the real TCP transport.
	repaired, err := engB.Reconcile(peerOverTCP{re: reB, id: "A"})
	if err != nil {
		t.Fatalf("B.Reconcile over TCP: %v", err)
	}

	// B must now hold every live key and honour the tombstones — full backfill.
	live := 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		v, gerr := engB.Get(key)
		if i < 20 {
			if gerr == nil {
				t.Errorf("%s should be deleted on B after reconcile", key)
			}
			continue
		}
		if gerr != nil {
			t.Errorf("%s missing on B after reconcile: %v", key, gerr)
			continue
		}
		if string(v) != fmt.Sprintf("val-%d", i) {
			t.Errorf("%s: got %q want val-%d", key, v, i)
		}
		live++
	}
	if live != n-20 {
		t.Errorf("B has %d live keys after backfill, want %d", live, n-20)
	}

	// Digests must now match — provable convergence, not just spot checks.
	da, _ := engA.ShardDigests()
	db, _ := engB.ShardDigests()
	if len(da) != len(db) {
		t.Fatalf("digest shard-count mismatch after reconcile: A=%d B=%d", len(da), len(db))
	}
	for s, d := range da {
		if db[s] != d {
			t.Fatalf("shard %d digest differs after reconcile: A=%x B=%x", s, d, db[s])
		}
	}
	t.Logf("anti-entropy over TCP backfilled a cold replica: %d entries repaired, %d live keys, digests converged",
		repaired, live)
}
