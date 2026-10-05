package main

// read_barrier_test.go — invariant 58: every read path, text and binary,
// passes coordinator.readBarrier before it touches the engine.
//
// The fixture is a raft-mode coordinator with --linearizable-reads on a node
// that can never become leader (its peers are unreachable), so readBarrier
// always answers MOVED. A read that skips the barrier returns the data that
// was written straight into the engine instead — which is exactly the stale
// read a freshly elected or deposed leader would serve.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/client"
	"github.com/VeltrixDB/veltrixdb/consensus"
	"github.com/VeltrixDB/veltrixdb/storage"
)

// deadTransport fails every RPC: the node stays a follower/candidate forever.
type deadTransport struct{}

var errDeadPeer = errors.New("peer unreachable")

func (deadTransport) SendRequestVote(string, consensus.RequestVoteArgs) (consensus.RequestVoteReply, error) {
	return consensus.RequestVoteReply{}, errDeadPeer
}
func (deadTransport) SendAppendEntries(string, consensus.AppendEntriesArgs) (consensus.AppendEntriesReply, error) {
	return consensus.AppendEntriesReply{}, errDeadPeer
}
func (deadTransport) SendInstallSnapshot(string, consensus.InstallSnapshotArgs) (consensus.InstallSnapshotReply, error) {
	return consensus.InstallSnapshotReply{}, errDeadPeer
}
func (deadTransport) Close() error { return nil }

type nopSM struct{}

func (nopSM) Apply([]byte) error { return nil }

// startNonLeaderServer serves a raft coordinator (linReads) whose node can
// never lead, with data seeded directly into the engine.
func startNonLeaderServer(t *testing.T) *testServer {
	t.Helper()
	node, err := consensus.NewRaftNode("rb-n1", []string{"rb-n2", "rb-n3"}, t.TempDir(), nopSM{}, deadTransport{})
	if err != nil {
		t.Fatalf("raft node: %v", err)
	}
	t.Cleanup(node.Stop)
	ts := startTestServerCoord(t, t.TempDir(), nil, func(e *storage.StorageEngine) *coordinator {
		return &coordinator{mode: modeRaft, engine: e, raft: node, linReads: true, peerClientAddr: map[string]string{}}
	})
	t.Cleanup(func() { ts.stop(t) })

	e := ts.engine
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	must(e.Put("k1", []byte(`{"color":"red"}`), -1))
	must(e.PutNS("ns1", "a", []byte(`{"color":"red"}`), -1))
	must(e.HSet("h1", "f1", []byte("hv"), -1))
	must(e.CreateFieldIndex("byColor", "color"))
	return ts
}

func TestReadBarrier_TextReadsRedirectOnNonLeader(t *testing.T) {
	ts := startNonLeaderServer(t)

	cmds := []string{
		"GET k1",
		"MGET k1",
		"NSGET ns1 a",
		"NSSCAN ns1",
		"NSLIST",
		"HGET h1 f1",
		"HGETALL h1",
		"HKEYS h1",
		"HLEN h1",
		"HTTL h1 f1",
		"RANGE a z",
		"SCANCUR - 10",
		"VER k1",
		"IDXQUERY byColor red",
		"QUERY ns1 WHERE color = red",
		"LLEN l1",
		"LRANGE l1 0 -1",
		"SISMEMBER s1 m",
		"SMEMBERS s1",
		"SCARD s1",
		"TSEARCH 5 hello",
		"VSEARCH 5 1 0",
	}
	for _, c := range cmds {
		// One connection per command so a multi-line answer cannot shift
		// the replies of the next ones.
		conn, err := net.DialTimeout("tcp", ts.addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.WriteString(conn, c+"\n"); err != nil {
			t.Fatalf("%s: write: %v", c, err)
		}
		line, err := bufio.NewReader(conn).ReadString('\n')
		conn.Close()
		if err != nil {
			t.Fatalf("%s: read: %v", c, err)
		}
		if !strings.HasPrefix(line, "ERR MOVED") {
			t.Errorf("%s: got %q, want ERR MOVED (read skipped readBarrier)", c, strings.TrimSpace(line))
		}
	}
}

// rawBinary sends one hand-built frame and returns the reply status and,
// for an error status, the message.
func rawBinary(t *testing.T, conn net.Conn, br *bufio.Reader, frame []byte) (byte, string) {
	t.Helper()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}
	var hdr [5]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		t.Fatalf("read: %v", err)
	}
	if hdr[0] != binStatusErr {
		return hdr[0], ""
	}
	msg := make([]byte, binary.LittleEndian.Uint32(hdr[1:]))
	if _, err := io.ReadFull(br, msg); err != nil {
		t.Fatalf("read msg: %v", err)
	}
	return hdr[0], string(msg)
}

// frame builds [cmd][2B a][4B b] + extra + body.
func frame(cmd byte, a uint16, b uint32, extra []byte, body ...string) []byte {
	f := []byte{cmd, 0, 0, 0, 0, 0, 0}
	binary.LittleEndian.PutUint16(f[1:3], a)
	binary.LittleEndian.PutUint32(f[3:7], b)
	f = append(f, extra...)
	for _, s := range body {
		f = append(f, s...)
	}
	return f
}

func u16(n int) []byte {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], uint16(n))
	return b[:]
}

func TestReadBarrier_BinaryReadsRedirectOnNonLeader(t *testing.T) {
	ts := startNonLeaderServer(t)

	cases := []struct {
		name  string
		frame []byte
	}{
		{"GET", frame(binCmdGet, 2, 0, nil, "k1")},
		{"NSGET", frame(binCmdNSGet, 3, 1, nil, "ns1", "a")},
		{"NSSCAN", frame(binCmdNSScan, 3, 10, u16(0), "ns1")},
		{"NSLIST", frame(binCmdNSList, 0, 0, nil)},
		{"HGET", frame(binCmdHGet, 2, 0, u16(2), "h1", "f1")},
		{"HTTL", frame(binCmdHTTL, 2, 0, u16(2), "h1", "f1")},
		{"HGETALL", frame(binCmdHGetAll, 2, 0, nil, "h1")},
		{"HKEYS", frame(binCmdHKeys, 2, 0, nil, "h1")},
		{"HLEN", frame(binCmdHLen, 2, 0, nil, "h1")},
		{"RANGE", frame(binCmdRange, 1, 10, append(u16(1), 0), "a", "z")},
		{"SCANCUR", frame(binCmdScanCur, 0, 10, nil)},
		{"IDXQUERY", frame(binCmdIdxQuery, 7, 10, u16(3), "byColor", "red")},
		{"QUERY", frame(binCmdQuery, 3, 10, append(append(u16(5), u16(1)...), u16(3)...), "ns1", "color", "=", "red")},
		{"GETVER", frame(binCmdGetVer, 2, 0, nil, "k1")},
	}
	for _, c := range cases {
		// One connection per op: a reply that skipped the barrier has an
		// op-specific layout, so it cannot be drained generically.
		conn, err := net.DialTimeout("tcp", ts.addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		status, msg := rawBinary(t, conn, bufio.NewReader(conn), c.frame)
		conn.Close()
		if status != binStatusErr || !strings.HasPrefix(msg, "MOVED") {
			t.Errorf("%s: status=0x%02x msg=%q, want ERR MOVED (read skipped readBarrier)", c.name, status, msg)
		}
	}

	// Binary MGET (batch frame) through the client.
	bc := dialBinary(t, ts.addr)
	defer bc.Close()
	if _, err := bc.MGet([]string{"k1"}); err == nil || !strings.Contains(err.Error(), "MOVED") {
		t.Errorf("MGET: err=%v, want MOVED", err)
	}
	if _, err := bc.TSearch(5, "hello", client.TextSearchOptions{}); err == nil || !strings.Contains(err.Error(), "MOVED") {
		t.Fatalf("SEARCH/TSEARCH: err=%v, want MOVED", err)
	}
	if _, err := bc.VSearch(5, []float32{1, 0}); err == nil || !strings.Contains(err.Error(), "MOVED") {
		t.Fatalf("VSEARCH: err=%v, want MOVED", err)
	}
}
