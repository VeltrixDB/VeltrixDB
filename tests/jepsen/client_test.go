package jepsen

// client_test.go — one client connection speaking the text protocol, and the
// rules that turn a reply into an op outcome:
//
//   - A read is safe to retry, so a read follows MOVED (to the named leader,
//     or after a short pause when the leader is unknown) until its deadline;
//     any error leaves it fail (a read that observed nothing).
//   - A write (PUT / CAS / INCR) is never retried. Its outcome is:
//     ok        the server acknowledged it (OK / MISMATCH / NOTFOUND / a number);
//     fail      the request never left the client (dial error);
//     info      everything else — timeouts, connection errors, ERR replies,
//               and MOVED. MOVED is info, not fail, because the raft server
//               answers MOVED both before a write enters the log (the
//               non-leader fast path) and after (consensus.ErrNotLeader from
//               failWaitersLocked when the leader steps down with the entry
//               in its log, which a later leader may still commit).
//     After MOVED the client switches to the named leader for its next op.
//   - After an info op the connection is closed and the client continues as
//     a new process (Jepsen convention: the old op is still pending).

import (
	"bufio"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"
)

type conn struct {
	addr string
	c    net.Conn
	r    *bufio.Reader
}

func dial(addr string, timeout time.Duration) (*conn, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &conn{addr: addr, c: c, r: bufio.NewReaderSize(c, 4096)}, nil
}

func (c *conn) close() {
	if c != nil && c.c != nil {
		c.c.Close()
	}
}

func (c *conn) roundTrip(line string, deadline time.Time) (string, error) {
	_ = c.c.SetDeadline(deadline)
	if _, err := c.c.Write([]byte(line + "\n")); err != nil {
		return "", err
	}
	s, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(s, "\r\n"), nil
}

// parseMoved returns the leader address of an "ERR MOVED <addr> <id>" reply
// ("" when the leader is unknown) and whether the reply is a redirect.
func parseMoved(reply string) (string, bool) {
	i := strings.Index(reply, "MOVED ")
	if i < 0 {
		return "", false
	}
	f := strings.Fields(reply[i+len("MOVED "):])
	if len(f) == 0 || f[0] == "-" {
		return "", true
	}
	return f[0], true
}

// client is one logical client (one goroutine). Not safe for concurrent use.
type client struct {
	id      int
	process int // current process id; bumped after every info op
	nextPID func() int
	cl      *cluster
	h       *History
	c       *conn
	target  string // node the client sends writes to (raft: leader guess)
	timeout time.Duration
	pick    func() string // a node address to try when the target is unusable
}

func (cl *client) ensure(addr string) error {
	if cl.c != nil && cl.c.addr == addr {
		return nil
	}
	cl.c.close()
	cl.c = nil
	c, err := dial(addr, 500*time.Millisecond)
	if err != nil {
		return err
	}
	cl.c = c
	return nil
}

func (cl *client) retire() {
	cl.c.close()
	cl.c = nil
	cl.process = cl.nextPID()
}

// read performs GET key against addr, following MOVED. It returns the op.
func (cl *client) read(key, addr string) Op {
	op := Op{Process: cl.process, Kind: KindRead, Key: key, Invoke: cl.h.Now()}
	deadline := time.Now().Add(cl.timeout)
	for {
		if err := cl.ensure(addr); err != nil {
			op.Outcome, op.Err = Fail, "dial: "+short(err)
			cl.target = cl.pick()
			break
		}
		reply, err := cl.c.roundTrip("GET "+key, deadline)
		if err != nil {
			op.Outcome, op.Err = Fail, short(err)
			cl.c.close()
			cl.c = nil
			cl.target = cl.pick()
			break
		}
		if a, moved := parseMoved(reply); moved {
			if time.Now().After(deadline.Add(-50 * time.Millisecond)) {
				op.Outcome, op.Err = Fail, reply
				break
			}
			if a == "" {
				time.Sleep(50 * time.Millisecond)
				a = cl.pick()
			}
			addr = a
			cl.target = a
			continue
		}
		op.Node = cl.c.addr
		switch {
		case reply == "ERR key not found":
			op.Outcome = OK
		case strings.HasPrefix(reply, "ERR"):
			op.Outcome, op.Err = Fail, reply
		default:
			op.Outcome, op.Found, op.Value = OK, true, reply
		}
		break
	}
	op.Complete = cl.h.Now()
	return op
}

// mutate sends one write-like command to the client's target and classifies
// the reply with parse (which fills op's result fields and returns false for
// an unexpected reply).
func (cl *client) mutate(op Op, line string, parse func(reply string, op *Op) bool) Op {
	op.Process = cl.process
	op.Invoke = cl.h.Now()
	addr := cl.target
	if err := cl.ensure(addr); err != nil {
		op.Outcome, op.Err = Fail, "dial: "+short(err)
		op.Complete = cl.h.Now()
		cl.target = cl.pick()
		return op
	}
	op.Node = addr
	reply, err := cl.c.roundTrip(line, time.Now().Add(cl.timeout))
	op.Complete = cl.h.Now()
	switch {
	case err != nil:
		op.Outcome, op.Err = Info, short(err)
		cl.retire()
		cl.target = cl.pick()
	case parse(reply, &op):
		op.Outcome = OK
	default:
		op.Outcome, op.Err = Info, reply
		if a, moved := parseMoved(reply); moved {
			if a == "" {
				a = cl.pick()
			}
			cl.target = a
		}
		cl.retire()
	}
	return op
}

func (cl *client) write(key, val string) Op {
	return cl.mutate(Op{Kind: KindWrite, Key: key, Value: val}, "PUT "+key+" "+val,
		func(r string, _ *Op) bool { return r == "OK" })
}

func (cl *client) add(key string) Op {
	return cl.mutate(Op{Kind: KindAdd, Key: key, Value: "1"}, "PUT "+key+" 1",
		func(r string, _ *Op) bool { return r == "OK" })
}

func (cl *client) cas(key, expect, val string) Op {
	return cl.mutate(Op{Kind: KindCAS, Key: key, Expect: expect, Value: val}, "CAS "+key+" "+expect+" "+val,
		func(r string, op *Op) bool {
			switch r {
			case "OK":
				op.CASRes = CASOK
			case "MISMATCH":
				op.CASRes = CASMismatch
			case "NOTFOUND":
				op.CASRes = CASNotFound
			default:
				return false
			}
			return true
		})
}

func (cl *client) incr(key string) Op {
	return cl.mutate(Op{Kind: KindIncr, Key: key, Delta: 1}, "INCR "+key,
		func(r string, op *Op) bool {
			v, err := strconv.ParseInt(r, 10, 64)
			if err != nil {
				return false
			}
			op.IncrRes = v
			return true
		})
}

func short(err error) string {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	s := err.Error()
	if len(s) > 80 {
		s = s[:80]
	}
	return s
}
