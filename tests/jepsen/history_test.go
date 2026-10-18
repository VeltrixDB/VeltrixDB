package jepsen

// history_test.go — the operation history every client appends to, and its
// JSON form (saved next to a failing run).

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Kind is what an operation does.
type Kind string

const (
	KindRead  Kind = "read"  // GET key → Value / Found
	KindWrite Kind = "write" // PUT key Value
	KindCAS   Kind = "cas"   // CAS key Expect Value → CASRes
	KindIncr  Kind = "incr"  // INCR key Delta → IncrRes
	KindAdd   Kind = "add"   // set workload: PUT of a unique key, never overwritten
)

// Outcome is what the client knows about an operation once it completed.
type Outcome string

const (
	// OK: the operation definitely took effect (or, for a read, observed the
	// returned value).
	OK Outcome = "ok"
	// Fail: the operation definitely did not take effect.
	Fail Outcome = "fail"
	// Info: unknown — a timeout, a connection error, or a reply that does
	// not say whether the operation was applied. It may take effect at any
	// time after its invocation, or never.
	Info Outcome = "info"
)

// CAS results as the server reports them.
const (
	CASOK       = "ok"
	CASMismatch = "mismatch"
	CASNotFound = "notfound"
)

// Op is one client operation: invocation, completion and outcome.
type Op struct {
	ID      int     `json:"id"`
	Process int     `json:"process"` // client connection id; a new one after every info op
	Node    string  `json:"node,omitempty"`
	Kind    Kind    `json:"kind"`
	Key     string  `json:"key"`
	Value   string  `json:"value,omitempty"`  // write / add / CAS new value; read result
	Found   bool    `json:"found,omitempty"`  // read: the key existed
	Expect  string  `json:"expect,omitempty"` // CAS expected value
	CASRes  string  `json:"cas,omitempty"`
	Delta   int64   `json:"delta,omitempty"`
	IncrRes int64   `json:"incr,omitempty"`
	Outcome Outcome `json:"outcome"`
	Err     string  `json:"err,omitempty"`
	// Final: a read taken from the raft leader after the run healed and a
	// barrier write committed. Kept in the linearizability check even where
	// ordinary reads are excluded (they may be stale).
	Final bool `json:"final,omitempty"`
	// Invoke / Complete: ns since the start of the run. An info op's
	// Complete is when the client gave up; the checkers treat it as +inf.
	Invoke   int64 `json:"invoke_ns"`
	Complete int64 `json:"complete_ns"`
}

// end is the completion time the checkers use: +inf for an info op.
func (o *Op) end() int64 {
	if o.Outcome == Info {
		return math.MaxInt64
	}
	return o.Complete
}

func (o Op) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "#%d p%d [%s .. ", o.ID, o.Process, fmtNs(o.Invoke))
	if o.Outcome == Info {
		b.WriteString("∞")
	} else {
		b.WriteString(fmtNs(o.Complete))
	}
	fmt.Fprintf(&b, "] %-4s %s ", o.Outcome, o.Key)
	switch o.Kind {
	case KindRead:
		if o.Outcome != OK {
			b.WriteString("read")
		} else if o.Found {
			fmt.Fprintf(&b, "read → %s", o.Value)
		} else {
			b.WriteString("read → (absent)")
		}
	case KindWrite, KindAdd:
		fmt.Fprintf(&b, "%s %s", o.Kind, o.Value)
	case KindCAS:
		fmt.Fprintf(&b, "cas %s→%s", o.Expect, o.Value)
		if o.Outcome == OK {
			fmt.Fprintf(&b, " = %s", o.CASRes)
		}
	case KindIncr:
		fmt.Fprintf(&b, "incr %+d", o.Delta)
		if o.Outcome == OK {
			fmt.Fprintf(&b, " = %d", o.IncrRes)
		}
	}
	if o.Err != "" {
		fmt.Fprintf(&b, "  (%s)", o.Err)
	}
	if o.Node != "" {
		fmt.Fprintf(&b, "  @%s", o.Node)
	}
	return b.String()
}

func fmtNs(ns int64) string {
	return fmt.Sprintf("%.3fs", float64(ns)/1e9)
}

// NemesisEvent is one fault the nemesis injected (or healed).
type NemesisEvent struct {
	At     int64  `json:"at_ns"`
	Fault  string `json:"fault"`
	Node   string `json:"node,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// History collects ops and nemesis events from concurrent goroutines.
type History struct {
	start time.Time

	mu      sync.Mutex
	ops     []Op
	nemesis []NemesisEvent
}

func NewHistory() *History { return &History{start: time.Now()} }

// Now is the time since the start of the run, in ns.
func (h *History) Now() int64 { return int64(time.Since(h.start)) }

// Add records a completed op and assigns its ID.
func (h *History) Add(op Op) {
	h.mu.Lock()
	op.ID = len(h.ops)
	h.ops = append(h.ops, op)
	h.mu.Unlock()
}

func (h *History) Nemesis(fault, node, detail string) {
	h.mu.Lock()
	h.nemesis = append(h.nemesis, NemesisEvent{At: h.Now(), Fault: fault, Node: node, Detail: detail})
	h.mu.Unlock()
}

// Ops returns a copy of the ops, sorted by invocation time.
func (h *History) Ops() []Op {
	h.mu.Lock()
	out := append([]Op(nil), h.ops...)
	h.mu.Unlock()
	sort.SliceStable(out, func(i, j int) bool { return out[i].Invoke < out[j].Invoke })
	return out
}

func (h *History) NemesisEvents() []NemesisEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]NemesisEvent(nil), h.nemesis...)
}

// historyFile is the saved JSON document.
type historyFile struct {
	Test     string            `json:"test"`
	Seed     int64             `json:"seed"`
	Config   map[string]string `json:"config,omitempty"`
	Ops      []Op              `json:"ops"`
	Nemesis  []NemesisEvent    `json:"nemesis"`
	Findings []string          `json:"findings,omitempty"`
}

// Save writes the full history as JSON to path.
func (h *History) Save(path, test string, seed int64, cfg map[string]string, findings []string) error {
	doc := historyFile{Test: test, Seed: seed, Config: cfg, Ops: h.Ops(), Nemesis: h.NemesisEvents(), Findings: findings}
	b, err := json.MarshalIndent(doc, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// LoadHistory reads a history saved by Save (for re-checking offline).
func LoadHistory(path string) ([]Op, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc historyFile
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	return doc.Ops, nil
}

// byKey groups ops by key, keeping invocation order.
func byKey(ops []Op) map[string][]Op {
	out := map[string][]Op{}
	for _, o := range ops {
		out[o.Key] = append(out[o.Key], o)
	}
	return out
}

func formatOps(ops []Op) string {
	var b strings.Builder
	for _, o := range ops {
		b.WriteString("    ")
		b.WriteString(o.String())
		b.WriteByte('\n')
	}
	return b.String()
}
