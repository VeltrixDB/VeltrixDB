package client

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// ── mock text-protocol server ─────────────────────────────────────────────────

type textServer struct {
	ln    net.Listener
	wg    sync.WaitGroup
	mu    sync.Mutex
	conns []net.Conn

	// store is a simple in-memory KV for GET/PUT/DEL.
	storeMu sync.Mutex
	store   map[string]string

	// authRequired, if set, causes AUTH to check these credentials.
	//
	// These are read by connection-handler goroutines, so they must be set
	// BEFORE serve() is started — hence newTextServerAuth rather than
	// assignment after construction. Assigning post-construction is a data
	// race even when the test happens to dial afterwards, because nothing
	// establishes a happens-before edge with the handler goroutine.
	authRequired bool
	authUser     string
	authPass     string
}

func newTextServer(t *testing.T) *textServer {
	t.Helper()
	return newTextServerAuth(t, false, "", "")
}

// newTextServerAuth builds a mock server with credentials already installed,
// so the handler goroutines observe them without a race.
func newTextServerAuth(t *testing.T, required bool, user, pass string) *textServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ts := &textServer{
		ln:           ln,
		store:        make(map[string]string),
		authRequired: required,
		authUser:     user,
		authPass:     pass,
	}
	ts.wg.Add(1)
	go ts.serve() // starting the goroutine here publishes the fields above
	t.Cleanup(ts.close)
	return ts
}

func (ts *textServer) addr() string { return ts.ln.Addr().String() }

func (ts *textServer) serve() {
	defer ts.wg.Done()
	for {
		conn, err := ts.ln.Accept()
		if err != nil {
			return
		}
		ts.mu.Lock()
		ts.conns = append(ts.conns, conn)
		ts.mu.Unlock()
		ts.wg.Add(1)
		go ts.handle(conn)
	}
}

func (ts *textServer) handle(conn net.Conn) {
	defer ts.wg.Done()
	defer conn.Close()
	sc := bufio.NewScanner(conn)
	w := bufio.NewWriter(conn)
	authed := !ts.authRequired

	flush := func(line string) {
		fmt.Fprintln(w, line)
		_ = w.Flush()
	}

	for sc.Scan() {
		line := sc.Text()
		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "AUTH":
			if len(parts) < 3 {
				flush("ERR AUTH <user> <pass>")
				continue
			}
			if ts.authRequired && (parts[1] != ts.authUser || parts[2] != ts.authPass) {
				flush("ERR invalid credentials")
				continue
			}
			authed = true
			flush("OK")

		case "PUT":
			if !authed {
				flush("ERR not authenticated")
				continue
			}
			if len(parts) < 3 {
				flush("ERR PUT <key> <value>")
				continue
			}
			ts.storeMu.Lock()
			ts.store[parts[1]] = parts[2]
			ts.storeMu.Unlock()
			flush("OK")

		case "GET":
			if !authed {
				flush("ERR not authenticated")
				continue
			}
			if len(parts) < 2 {
				flush("ERR GET <key>")
				continue
			}
			ts.storeMu.Lock()
			v, ok := ts.store[parts[1]]
			ts.storeMu.Unlock()
			switch {
			case parts[1] == "moved-key":
				flush("ERR MOVED 10.0.0.2:9000 node-2")
			case !ok:
				flush("ERR key not found") // the real server's miss reply
			default:
				flush(v)
			}

		case "PUTEX":
			full := strings.SplitN(line, " ", 4)
			if len(full) < 4 {
				flush("ERR usage: PUTEX <key> <ttl-seconds> <value>")
				continue
			}
			ts.storeMu.Lock()
			ts.store[full[1]] = full[3]
			ts.store["ttl:"+full[1]] = full[2]
			ts.storeMu.Unlock()
			flush("OK")

		case "DEL":
			if !authed {
				flush("ERR not authenticated")
				continue
			}
			if len(parts) < 2 {
				flush("ERR DEL <key>")
				continue
			}
			ts.storeMu.Lock()
			delete(ts.store, parts[1])
			ts.storeMu.Unlock()
			flush("OK")

		case "PING":
			flush("PONG")

		case "INFO":
			flush("keys=0 writes=0 reads=0")

		case "QUIT":
			flush("BYE")
			return

		default:
			flush("ERR unknown command")
		}
	}
}

func (ts *textServer) close() {
	ts.ln.Close()
	ts.mu.Lock()
	for _, c := range ts.conns {
		c.Close()
	}
	ts.mu.Unlock()
	ts.wg.Wait()
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestDial_Failure(t *testing.T) {
	_, err := DialTCP("127.0.0.1:1", time.Second)
	if err == nil {
		t.Fatal("expected error dialing a closed port")
	}
}

func TestPut_Get_RoundTrip(t *testing.T) {
	ts := newTextServer(t)
	conn, err := DialTCP(ts.addr(), time.Second)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	defer conn.Close()

	if err := conn.Put("hello", []byte("world")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val, err := conn.Get("hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Fatalf("expected %q, got %q", "world", val)
	}
}

func TestGet_NotFound_ReturnsNilNil(t *testing.T) {
	ts := newTextServer(t)
	conn, err := DialTCP(ts.addr(), time.Second)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	defer conn.Close()

	val, err := conn.Get("no-such-key")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if val != nil {
		t.Fatalf("expected nil for missing key, got %q", val)
	}
}

func TestDelete(t *testing.T) {
	ts := newTextServer(t)
	conn, err := DialTCP(ts.addr(), time.Second)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	defer conn.Close()

	_ = conn.Put("del-me", []byte("v"))
	if err := conn.Delete("del-me"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	val, _ := conn.Get("del-me")
	if val != nil {
		t.Fatal("key should be gone after Delete")
	}
}

func TestPing(t *testing.T) {
	ts := newTextServer(t)
	conn, err := DialTCP(ts.addr(), time.Second)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestInfo(t *testing.T) {
	ts := newTextServer(t)
	conn, err := DialTCP(ts.addr(), time.Second)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	defer conn.Close()

	info, err := conn.Info()
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if !strings.Contains(info, "keys=") {
		t.Fatalf("unexpected info response: %q", info)
	}
}

func TestAuth_Success(t *testing.T) {
	ts := newTextServerAuth(t, true, "alice", "secret")

	conn, err := DialTCP(ts.addr(), time.Second)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	defer conn.Close()

	if err := conn.Auth("alice", "secret"); err != nil {
		t.Fatalf("Auth: %v", err)
	}
	// should be able to operate after auth
	if err := conn.Ping(); err != nil {
		t.Fatalf("Ping after auth: %v", err)
	}
}

func TestAuth_Failure(t *testing.T) {
	ts := newTextServerAuth(t, true, "alice", "wrong-on-purpose")

	conn, err := DialTCP(ts.addr(), time.Second)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	defer conn.Close()

	if err := conn.Auth("alice", "definitely-not-the-password"); err == nil {
		t.Fatal("expected error on bad credentials")
	}
}

func TestRedial(t *testing.T) {
	ts := newTextServer(t)
	conn, err := DialTCP(ts.addr(), time.Second)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}

	// Force-close the underlying connection.
	conn.c.Close()

	// Redial should re-establish and allow normal operation.
	if err := conn.Redial(time.Second); err != nil {
		t.Fatalf("Redial: %v", err)
	}
	if err := conn.Ping(); err != nil {
		t.Fatalf("Ping after Redial: %v", err)
	}
	conn.Close()
}

func TestPut_Overwrite(t *testing.T) {
	ts := newTextServer(t)
	conn, err := DialTCP(ts.addr(), time.Second)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	defer conn.Close()

	_ = conn.Put("k", []byte("v1"))
	_ = conn.Put("k", []byte("v2"))
	val, _ := conn.Get("k")
	if string(val) != "v2" {
		t.Fatalf("expected overwritten value v2, got %q", val)
	}
}

// TestGet_ServerError_ReturnsError: an ERR reply other than a miss (here a
// MOVED redirect) must surface as an error, not as (nil, nil).
func TestGet_ServerError_ReturnsError(t *testing.T) {
	ts := newTextServer(t)
	conn, err := DialTCP(ts.addr(), time.Second)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	defer conn.Close()

	val, err := conn.Get("moved-key")
	if err == nil || !strings.Contains(err.Error(), "MOVED 10.0.0.2:9000") {
		t.Fatalf("Get: val=%q err=%v, want MOVED error", val, err)
	}
	if target, ok := parseMoved(err.Error()); !ok || target != "10.0.0.2:9000" {
		t.Fatalf("parseMoved(%q) = %q, %v", err.Error(), target, ok)
	}
}

// TestPutEx_SendsTTL: PutEx sends PUTEX with the TTL; ttl < 1 is refused
// client-side (the server would reject it).
func TestPutEx_SendsTTL(t *testing.T) {
	ts := newTextServer(t)
	conn, err := DialTCP(ts.addr(), time.Second)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	defer conn.Close()

	if err := conn.PutEx("k", 42, []byte("v w")); err != nil {
		t.Fatalf("PutEx: %v", err)
	}
	ts.storeMu.Lock()
	v, ttl := ts.store["k"], ts.store["ttl:k"]
	ts.storeMu.Unlock()
	if v != "v w" || ttl != "42" {
		t.Fatalf("server saw value=%q ttl=%q, want \"v w\" / 42", v, ttl)
	}
	if err := conn.PutEx("k", 0, []byte("v")); err == nil {
		t.Fatal("PutEx with ttl 0 should fail client-side")
	}
}
