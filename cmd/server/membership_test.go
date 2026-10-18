package main

import (
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/cluster"
	"github.com/VeltrixDB/veltrixdb/storage"
)

func TestReplState_NewerAndCoordinator(t *testing.T) {
	a := replState{Version: 3, Coordinator: "n1", Members: []memberInfo{{ID: "n3"}, {ID: "n1"}, {ID: "n2"}}}
	if got := a.coordinatorID(); got != "n1" {
		t.Fatalf("coordinator = %q, want n1 (lowest id)", got)
	}
	cases := []struct {
		a, b replState
		want bool
	}{
		{replState{Version: 4}, replState{Version: 3}, true},
		{replState{Version: 3}, replState{Version: 4}, false},
		{replState{Version: 3, Coordinator: "n2"}, replState{Version: 3, Coordinator: "n1"}, true}, // tie-break
		{replState{Version: 3, Coordinator: "n1"}, replState{Version: 3, Coordinator: "n1"}, false},
		{replState{Version: 1}, replState{Version: 0}, true},
	}
	for i, c := range cases {
		if got := c.a.newer(c.b); got != c.want {
			t.Errorf("case %d: newer = %v, want %v", i, got, c.want)
		}
	}
}

func TestSelfInPeers(t *testing.T) {
	spec := "n1@h1:9000, n2@h2:9000,n3@h3:9000"
	if !selfInPeers(spec, "n2") || selfInPeers(spec, "n4") || selfInPeers("", "n1") {
		t.Fatal("selfInPeers wrong")
	}
}

func TestValidMemberAddrAndID(t *testing.T) {
	for _, a := range []string{"10.0.0.4:9000", "node-4.svc:9000", "[::1]:9000"} {
		if err := validMemberAddr(a); err != nil {
			t.Errorf("%s: %v", a, err)
		}
	}
	for _, a := range []string{":9000", "0.0.0.0:9000", "h", "h:x", "h:70000"} {
		if validMemberAddr(a) == nil {
			t.Errorf("%s accepted", a)
		}
	}
	for _, id := range []string{"", "a@b", "a,b", "a/b", "a b"} {
		if validMemberID(id) == nil {
			t.Errorf("id %q accepted", id)
		}
	}
}

// A migrated copy (replicated mode) is applied only when the node holds
// neither a value nor a tombstone for the key, and it is marked as a peer
// write: it never enters this node's catch-up feed.
func TestReceiveMigrated_PutIfAbsent(t *testing.T) {
	e := newFSMTestEngine(t)
	mm := &membershipManager{membershipParams: membershipParams{engine: e}}
	if err := e.Put("newer", []byte("live-write"), -1); err != nil {
		t.Fatal(err)
	}
	if err := e.Put("gone", []byte("x"), -1); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete("gone"); err != nil {
		t.Fatal(err)
	}
	for _, kv := range []cluster.KeyValue{
		{Key: "newer", Value: []byte("stale-copy"), TTL: -1},
		{Key: "gone", Value: []byte("stale-copy"), TTL: -1},
		{Key: "absent", Value: []byte("copy"), TTL: -1},
	} {
		if err := mm.receiveMigrated(kv); err != nil {
			t.Fatal(err)
		}
	}
	if v, _ := e.Get("newer"); string(v) != "live-write" {
		t.Fatalf("migrated copy overwrote a newer write: %q", v)
	}
	if v, err := e.Get("gone"); err == nil && v != nil {
		t.Fatalf("migrated copy resurrected a deleted key: %q", v)
	}
	if v, _ := e.Get("absent"); string(v) != "copy" {
		t.Fatalf("absent key not written: %q", v)
	}
	var feed []string
	if err := e.ScanChanges(0, 100, true, func(evs []storage.CDCEvent) error {
		for _, ev := range evs {
			feed = append(feed, ev.Key)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, k := range feed {
		if k == "absent" {
			t.Fatalf("migrated key is in this node's own (local-origin) feed: %v", feed)
		}
	}
}

func TestWriteGate_Replicated(t *testing.T) {
	mm := &membershipManager{}
	c := &coordinator{mode: modeReplicated, mm: mm}
	if err := c.writeGate(); err != nil {
		t.Fatalf("member: %v", err)
	}
	mm.joining.Store(true)
	if err := c.writeGate(); err == nil || !strings.Contains(err.Error(), "joining") {
		t.Fatalf("joining: %v", err)
	}
	mm.joining.Store(false)
	mm.drainUntil.Store(time.Now().Add(time.Minute).UnixNano())
	if err := c.writeGate(); err == nil || !strings.Contains(err.Error(), "draining") {
		t.Fatalf("draining: %v", err)
	}
	mm.drainUntil.Store(time.Now().Add(-time.Second).UnixNano()) // drain expired
	if err := c.writeGate(); err != nil {
		t.Fatalf("expired drain: %v", err)
	}
	mm.removed.Store(true)
	if err := c.writeGate(); err == nil || !strings.Contains(err.Error(), "removed") {
		t.Fatalf("removed: %v", err)
	}
	// Raft and standalone are never gated here.
	if err := (&coordinator{mode: modeRaft, mm: mm}).writeGate(); err != nil {
		t.Fatalf("raft: %v", err)
	}
}

// A migration that fails because the new member is not listening yet is
// retried with backoff, without waiting for another membership event.
func TestAutoRebalancer_RetriesFailedMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-engine test")
	}
	oldMin := rebalanceRetryMin
	rebalanceRetryMin = 200 * time.Millisecond
	defer func() { rebalanceRetryMin = oldMin }()

	cfg := cluster.DefaultClusterConfig()
	cfg.ReplicationFactor = cluster.AllNodesRF
	pm1 := cluster.NewPartitionMap(cfg)
	_ = pm1.AddNode("retry-1", "127.0.0.1", 7201)
	eng1 := newFSMTestEngine(t)
	ta1 := cluster.NewTransferAgent(pm1, "retry-1", eng1, "127.0.0.1:0")
	if err := ta1.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ta1.Stop)
	stop := startAutoRebalancer(pm1, ta1, "retry-1")
	t.Cleanup(stop)
	for i := 0; i < 50; i++ {
		if err := eng1.Put(fmt.Sprintf("retry:%02d", i), []byte("v"), -1); err != nil {
			t.Fatal(err)
		}
	}

	// Reserve an address for node 2 but do not listen yet.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr2 := l.Addr().String()
	l.Close()
	pm1.SetNodeTransferAddr("retry-2", addr2)
	_ = pm1.AddNode("retry-2", "127.0.0.1", 7202) // membership event → migration fails (refused)

	time.Sleep(rebalanceDebounce + 500*time.Millisecond) // first attempt has failed by now

	pm2 := cluster.NewPartitionMap(cfg)
	_ = pm2.AddNode("retry-1", "127.0.0.1", 7201)
	_ = pm2.AddNode("retry-2", "127.0.0.1", 7202)
	eng2 := newFSMTestEngine(t)
	ta2 := cluster.NewTransferAgent(pm2, "retry-2", eng2, addr2)
	var got atomic.Int64
	ta2.SetReceiveFunc(func(kv cluster.KeyValue) error {
		got.Add(1)
		return eng2.Put(kv.Key, kv.Value, kv.TTL)
	})
	if err := ta2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ta2.Stop)

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && got.Load() < 50 {
		time.Sleep(50 * time.Millisecond)
	}
	if got.Load() < 50 {
		t.Fatalf("retried migration delivered %d/50 keys", got.Load())
	}
	// Every node holds every key (AllNodesRF): nothing was deleted on node 1.
	for i := 0; i < 50; i++ {
		if v, _ := eng1.Get(fmt.Sprintf("retry:%02d", i)); string(v) != "v" {
			t.Fatalf("key %d gone from node 1", i)
		}
	}
}
