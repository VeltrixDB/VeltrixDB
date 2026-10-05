package main

// client_wire_test.go — the in-repo Go client against the real server:
// TTLs and errors it used to drop on the floor.

import (
	"context"
	"strings"
	"testing"

	"github.com/VeltrixDB/veltrixdb/client"
)

// TestClientTTLReachesServer: BinaryConn.Put, Pipeline.Put and
// client.Client.Put(WithTTL) all used to accept a TTL and write the key
// immortal (the binary PUT frame has no TTL field; Client.Put ignored it).
func TestClientTTLReachesServer(t *testing.T) {
	ts := startTestServer(t, t.TempDir(), nil)
	defer ts.stop(t)

	bc := dialBinary(t, ts.addr)
	defer bc.Close()
	if err := bc.Put("ttl/bin", []byte("v"), 300); err != nil {
		t.Fatalf("binary put: %v", err)
	}
	if err := bc.Put("ttl/none", []byte("v"), -1); err != nil {
		t.Fatalf("binary put no ttl: %v", err)
	}

	pbc := dialBinary(t, ts.addr)
	p := client.NewPipeline(pbc)
	defer p.Close()
	p.Put("ttl/pipe", []byte("v"), 300)
	p.Put("ttl/pipe-none", []byte("v"), 0)
	p.Get("ttl/pipe")
	res, err := p.Exec()
	if err != nil {
		t.Fatalf("pipeline exec: %v", err)
	}
	if len(res) != 3 || res[0].Err != nil || res[1].Err != nil || string(res[2].Value) != "v" {
		t.Fatalf("pipeline results: %+v", res)
	}

	cli, err := client.NewClient([]string{ts.addr}, nil)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cli.Close()
	if err := cli.Put(context.Background(), "ttl/cli", []byte("v"), client.WithTTL(300)); err != nil {
		t.Fatalf("client put: %v", err)
	}
	if v, err := cli.Get(context.Background(), "ttl/cli"); err != nil || string(v) != "v" {
		t.Fatalf("client get: %q %v", v, err)
	}

	for _, k := range []string{"ttl/bin", "ttl/pipe", "ttl/cli"} {
		if got := ts.engine.GetTTLForKey(k); got <= 0 || got > 300 {
			t.Errorf("%s: remaining TTL %d, want (0, 300] — TTL was not sent", k, got)
		}
	}
	for _, k := range []string{"ttl/none", "ttl/pipe-none"} {
		if got := ts.engine.GetTTLForKey(k); got != -1 {
			t.Errorf("%s: remaining TTL %d, want -1 (immortal)", k, got)
		}
	}
}

// TestClientGetSurfacesServerErrors: TCPConn.Get returned (nil, nil) for any
// ERR line and BinaryConn.Get returned an error frame's text as the value, so
// a MOVED / read-barrier failure looked like a miss (or like data).
func TestClientGetSurfacesServerErrors(t *testing.T) {
	ts := startNonLeaderServer(t)

	tc := dialText(t, ts.addr)
	defer tc.Close()
	if v, err := tc.Get("k1"); err == nil || !strings.Contains(err.Error(), "MOVED") {
		t.Errorf("text Get: v=%q err=%v, want MOVED error", v, err)
	}
	bc := dialBinary(t, ts.addr)
	defer bc.Close()
	if v, err := bc.Get("k1"); err == nil || !strings.Contains(err.Error(), "MOVED") {
		t.Errorf("binary Get: v=%q err=%v, want MOVED error", v, err)
	}

	// A genuine miss is still (nil, nil) on a normal server.
	ok := startTestServer(t, t.TempDir(), nil)
	defer ok.stop(t)
	tc2 := dialText(t, ok.addr)
	defer tc2.Close()
	if v, err := tc2.Get("absent"); err != nil || v != nil {
		t.Errorf("text miss: v=%q err=%v, want nil, nil", v, err)
	}
	bc2 := dialBinary(t, ok.addr)
	defer bc2.Close()
	if v, err := bc2.Get("absent"); err != nil || v != nil {
		t.Errorf("binary miss: v=%q err=%v, want nil, nil", v, err)
	}
}
