package replication

import (
	"reflect"
	"testing"
)

// TestAntiEntropy_DigestFetchOverTCP exercises the digest/fetch RPCs over a
// real TCP loopback connection: a server with sync handlers, a client that
// pulls digests and a shard's entries. Validates the extended wire protocol
// (kind byte + framed request/response) end-to-end.
func TestAntiEntropy_DigestFetchOverTCP(t *testing.T) {
	wantDigests := map[uint16]uint64{3: 0xDEADBEEF, 4242: 0x0102030405060708}
	wantShard := []SyncEntry{
		{Key: "k1", Value: []byte("v1"), LWWStampNs: 111, TTLSeconds: -1},
		{Key: "k2", Tombstone: true, LWWStampNs: 222},
		{Key: "k|weird\nkey", Value: []byte("binary\x00\xff"), LWWStampNs: 333, TTLSeconds: 60},
	}

	srv, err := NewReplicationServer("127.0.0.1:0", func(*WriteOperation) error { return nil })
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	var gotShardID uint16
	srv.SetSyncHandlers(
		func() (map[uint16]uint64, error) { return wantDigests, nil },
		func(shardID uint16) ([]SyncEntry, error) { gotShardID = shardID; return wantShard, nil },
	)
	go srv.ListenAndServe()
	defer srv.Stop()

	c := NewReplicationClient(srv.Addr())
	defer c.Close()

	// Digests round-trip.
	gotDigests, err := c.FetchDigests()
	if err != nil {
		t.Fatalf("FetchDigests: %v", err)
	}
	if !reflect.DeepEqual(gotDigests, wantDigests) {
		t.Fatalf("digests: got %v want %v", gotDigests, wantDigests)
	}

	// Shard fetch round-trip (including a binary-unsafe key + a tombstone).
	gotEntries, err := c.FetchShard(4242)
	if err != nil {
		t.Fatalf("FetchShard: %v", err)
	}
	if gotShardID != 4242 {
		t.Fatalf("server saw shardID %d, want 4242", gotShardID)
	}
	if !reflect.DeepEqual(gotEntries, wantShard) {
		t.Fatalf("shard entries mismatch:\n got %+v\nwant %+v", gotEntries, wantShard)
	}

	// A batch push on the SAME connection must still work (kind multiplexing).
	if err := c.Send([]*WriteOperation{{SeqNum: 1, Key: "x", Value: []byte("y"), Timestamp: 1}}); err != nil {
		t.Fatalf("Send after sync RPCs: %v", err)
	}
}

// TestAntiEntropy_NoHandlersError verifies a server without sync handlers
// replies with a clean error rather than hanging or corrupting the stream.
func TestAntiEntropy_NoHandlersError(t *testing.T) {
	srv, err := NewReplicationServer("127.0.0.1:0", func(*WriteOperation) error { return nil })
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go srv.ListenAndServe()
	defer srv.Stop()

	c := NewReplicationClient(srv.Addr())
	defer c.Close()

	if _, err := c.FetchDigests(); err == nil {
		t.Fatal("expected error when anti-entropy handlers are not set")
	}
	// The connection must remain usable for a normal batch afterwards.
	if err := c.Send([]*WriteOperation{{SeqNum: 1, Key: "x", Value: []byte("y"), Timestamp: 1}}); err != nil {
		t.Fatalf("Send after error response: %v", err)
	}
}
