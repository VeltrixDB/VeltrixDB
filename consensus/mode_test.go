package consensus

// mode_test.go — run the suite with pipelined replication on or off.
//
// Options.Pipeline defaults to off (server flag --raft-pipeline).  Every test
// helper builds its Options through testOpts, so
//
//	VELTRIX_RAFT_PIPELINE=1 go test ./consensus/...
//
// runs the whole package with the pipeline on.  The tests that matter most
// for the difference (lossy/reordering transport, ack-after-fsync, heartbeat
// during fsync, truncation racing fsync, leader self-count) run BOTH modes on
// every invocation via forEachPipelineMode, which sets Options.Pipeline
// explicitly.

import (
	"os"
	"strconv"
	"testing"
)

// suitePipeline is the mode for tests that do not choose one explicitly.
var suitePipeline = func() bool {
	v, _ := strconv.ParseBool(os.Getenv("VELTRIX_RAFT_PIPELINE"))
	return v
}()

// testOpts applies the suite's pipeline mode to o.
func testOpts(o Options) Options {
	o.Pipeline = suitePipeline
	return o
}

// forEachPipelineMode runs f as two subtests, pipeline=off and pipeline=on.
func forEachPipelineMode(t *testing.T, f func(t *testing.T, pipeline bool)) {
	t.Helper()
	for _, on := range []bool{false, true} {
		on := on
		name := "pipeline=off"
		if on {
			name = "pipeline=on"
		}
		t.Run(name, func(t *testing.T) { f(t, on) })
	}
}

func TestMode_OptionsWiring(t *testing.T) {
	for _, c := range []struct {
		opts   Options
		window int
	}{
		{Options{}, 1},
		{Options{PipelineWindow: 5}, 1}, // window ignored when off
		{Options{Pipeline: true}, DefaultPipelineWindow},
		{Options{Pipeline: true, PipelineWindow: 3}, 3},
	} {
		n := startSingle(t, "w", t.TempDir(), &mockSM{}, c.opts)
		if n.Pipeline() != c.opts.Pipeline || n.pipelineWindow != c.window {
			t.Errorf("%+v: Pipeline()=%v window=%d, want %v / %d", c.opts, n.Pipeline(), n.pipelineWindow, c.opts.Pipeline, c.window)
		}
		n.Stop()
	}
}

// newTestNode is NewRaftNode in the suite's pipeline mode.
func newTestNode(id string, peers []string, dir string, sm StateMachine, tr Transport) (*RaftNode, error) {
	return NewRaftNodeWithOptions(id, peers, dir, sm, tr, testOpts(Options{SnapshotThreshold: DefaultSnapshotThreshold}))
}
