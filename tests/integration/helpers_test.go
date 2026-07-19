package integration_test

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// serverBinary returns the path to the pre-built veltrixdb binary when
// VELTRIX_SERVER_BIN is set (CI pre-builds it before running tests), or ""
// to trigger a local build via builtBinary().
func serverBinary() string { return os.Getenv("VELTRIX_SERVER_BIN") }

// builtBinary compiles the server binary to a temp file on the first call and
// returns the cached path on subsequent calls.  Compiling once per test-binary
// run avoids the repeated cost of go-build AND eliminates the go-run process
// wrapper whose signal-forwarding behaviour is unreliable across Go versions:
// when the test signals cmd.Process.Pid it must reach the server directly.
var (
	builtServerBin    string
	builtServerBinErr error
	buildBinOnce      sync.Once
)

func builtBinary(t *testing.T) string {
	t.Helper()
	buildBinOnce.Do(func() {
		f, err := os.CreateTemp("", "veltrixdb-test-bin-*")
		if err != nil {
			builtServerBinErr = fmt.Errorf("create temp file for server binary: %w", err)
			return
		}
		f.Close()
		root := moduleRoot(t)
		c := exec.Command("go", "build", "-o", f.Name(), "./cmd/server")
		c.Dir = root
		c.Stdout = os.Stderr
		c.Stderr = os.Stderr
		if err := c.Run(); err != nil {
			builtServerBinErr = fmt.Errorf("go build ./cmd/server: %w", err)
		} else {
			builtServerBin = f.Name()
		}
	})
	if builtServerBinErr != nil {
		t.Fatalf("builtBinary: %v", builtServerBinErr)
	}
	return builtServerBin
}

// testServer holds a running veltrixdb server process started for testing.
type testServer struct {
	// Addr is the TCP address the server listens on (e.g. "127.0.0.1:54321").
	Addr string
	// MetricsAddr is the HTTP address serving /healthz, /readyz, /metrics.
	MetricsAddr string
	// DataDir is the temporary data directory for this server instance.
	DataDir string

	cmd *exec.Cmd
	t   *testing.T
}

// startServer starts a veltrixdb server on a random free port with a temp data
// dir. Any extraArgs are appended to the server's flag list.  The test is
// marked as failed and stopped if the binary cannot be started.
//
// The caller MUST call s.stop() (or register it via t.Cleanup) to kill the
// process and remove the data directory when the test finishes.
func startServer(t *testing.T, extraArgs ...string) *testServer {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "veltrix-integ-")
	if err != nil {
		t.Fatalf("startServer: mkdirtemp: %v", err)
	}

	tcpPort := freePort(t)
	httpPort := freePort(t)

	addr := fmt.Sprintf("127.0.0.1:%d", tcpPort)
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)

	// Build the argument list.  Common server flags regardless of launch mode.
	srvFlags := []string{
		"-addr", addr,
		"-metrics-addr", metricsAddr,
		"-data", dataDir,
		// Use tight flush windows to keep test latency low.
		"-wal-flush-window-ms", "1",
		"-vlog-flush-window-ms", "1",
	}
	srvFlags = append(srvFlags, extraArgs...)

	// Find the module root (two levels up from this file at tests/integration/).
	repoRoot := moduleRoot(t)

	// Always run a pre-built binary so that cmd.Process.Pid IS the server
	// process.  go-run wraps the binary in an extra process layer whose
	// signal-forwarding behaviour is Go-version-dependent; signals sent to the
	// go-run PID may never reach the server, causing graceful-shutdown tests to
	// time out.  builtBinary() compiles once per test-binary run (sync.Once).
	var cmd *exec.Cmd
	bin := serverBinary()
	if bin == "" {
		bin = builtBinary(t)
	}
	cmd = exec.Command(bin, srvFlags...)
	cmd.Dir = repoRoot

	// Capture server stderr so failures can be diagnosed.
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stderr

	if err := cmd.Start(); err != nil {
		os.RemoveAll(dataDir)
		t.Fatalf("startServer: start process: %v", err)
	}

	s := &testServer{
		Addr:        addr,
		MetricsAddr: metricsAddr,
		DataDir:     dataDir,
		cmd:         cmd,
		t:           t,
	}

	if err := s.waitReady(15 * time.Second); err != nil {
		s.stop()
		t.Fatalf("startServer: server not ready: %v", err)
	}

	return s
}

// stop kills the server process and removes the data directory.
func (s *testServer) stop() {
	s.t.Helper()
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_ = s.cmd.Wait()
	}
	if s.DataDir != "" {
		os.RemoveAll(s.DataDir)
	}
}

// waitReady polls the TCP address until the server accepts connections or the
// deadline is exceeded.
func (s *testServer) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", s.Addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("server at %s not ready after %s", s.Addr, timeout)
}

// ── textClient ────────────────────────────────────────────────────────────────

// textClient speaks the line-oriented text protocol to a running server.
// It is NOT goroutine-safe; each goroutine should create its own textClient.
type textClient struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
	t    *testing.T
}

// newTextClient dials addr and returns a ready textClient.
func newTextClient(t *testing.T, addr string) *textClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("newTextClient: dial %s: %v", addr, err)
	}
	return &textClient{
		conn: conn,
		r:    bufio.NewReaderSize(conn, 32*1024),
		w:    bufio.NewWriterSize(conn, 32*1024),
		t:    t,
	}
}

// send writes cmd (which must NOT already end with \n) and returns the single
// response line (newline stripped).
func (c *textClient) send(cmd string) string {
	c.t.Helper()
	if _, err := fmt.Fprintln(c.w, cmd); err != nil {
		c.t.Fatalf("textClient.send write %q: %v", cmd, err)
	}
	if err := c.w.Flush(); err != nil {
		c.t.Fatalf("textClient.send flush %q: %v", cmd, err)
	}
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("textClient.send read response for %q: %v", cmd, err)
	}
	return strings.TrimRight(line, "\r\n")
}

// close closes the underlying connection.
func (c *textClient) close() {
	_, _ = fmt.Fprintln(c.w, "QUIT")
	_ = c.w.Flush()
	c.conn.Close()
}

// ── helpers ───────────────────────────────────────────────────────────────────

// freePort asks the OS for a free TCP port and returns it.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// moduleRoot returns the directory that contains go.mod, walking up from the
// directory of this test file (tests/integration → project root).
func moduleRoot(t *testing.T) string {
	t.Helper()
	// Resolve relative to the package source file location.
	dir, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("moduleRoot: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		// Fall back to current working directory (covers `go test ./...` from root).
		wd, _ := os.Getwd()
		dir = filepath.Join(wd, "../..")
	}
	return dir
}
