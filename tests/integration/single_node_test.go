package integration_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestIntegration_PutGet verifies the basic PUT → GET round-trip over the text
// protocol.
func TestIntegration_PutGet(t *testing.T) {
	s := startServer(t)
	defer s.stop()

	c := newTextClient(t, s.Addr)
	defer c.close()

	resp := c.send("PUT hello world")
	if resp != "OK" {
		t.Fatalf("PUT: want OK, got %q", resp)
	}

	resp = c.send("GET hello")
	if resp != "world" {
		t.Fatalf("GET: want %q, got %q", "world", resp)
	}
}

// TestIntegration_Delete verifies that a DEL removes a key and subsequent GETs
// return an error response.
func TestIntegration_Delete(t *testing.T) {
	s := startServer(t)
	defer s.stop()

	c := newTextClient(t, s.Addr)
	defer c.close()

	c.send("PUT mykey myvalue")

	resp := c.send("GET mykey")
	if resp != "myvalue" {
		t.Fatalf("before DEL: want myvalue, got %q", resp)
	}

	resp = c.send("DEL mykey")
	if resp != "OK" {
		t.Fatalf("DEL: want OK, got %q", resp)
	}

	resp = c.send("GET mykey")
	if !strings.HasPrefix(resp, "ERR") {
		t.Fatalf("after DEL: want ERR*, got %q", resp)
	}
}

// TestIntegration_Ping verifies that the PING command returns PONG.
func TestIntegration_Ping(t *testing.T) {
	s := startServer(t)
	defer s.stop()

	c := newTextClient(t, s.Addr)
	defer c.close()

	resp := c.send("PING")
	if resp != "PONG" {
		t.Fatalf("PING: want PONG, got %q", resp)
	}
}

// TestIntegration_Info verifies that INFO returns a line containing "keys=".
func TestIntegration_Info(t *testing.T) {
	s := startServer(t)
	defer s.stop()

	c := newTextClient(t, s.Addr)
	defer c.close()

	// Put a key so stats are non-trivial.
	c.send("PUT infokey infovalue")

	resp := c.send("INFO")
	if !strings.Contains(resp, "keys=") {
		t.Fatalf("INFO: expected 'keys=' in response, got %q", resp)
	}
}

// TestIntegration_Concurrent runs 10 goroutines each writing 50 keys and
// verifies they all succeed without races or errors.
func TestIntegration_Concurrent(t *testing.T) {
	s := startServer(t)
	defer s.stop()

	const goroutines = 10
	const keysPerGoroutine = 50

	var wg sync.WaitGroup
	errs := make(chan string, goroutines*keysPerGoroutine)

	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := newTextClient(t, s.Addr)
			defer c.close()

			for k := 0; k < keysPerGoroutine; k++ {
				key := fmt.Sprintf("g%d-k%d", g, k)
				val := fmt.Sprintf("val-%d-%d", g, k)
				resp := c.send(fmt.Sprintf("PUT %s %s", key, val))
				if resp != "OK" {
					errs <- fmt.Sprintf("PUT %s: got %q", key, resp)
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for msg := range errs {
		t.Error(msg)
	}
}

// TestIntegration_LargeValue verifies that a 1 KB URL-safe value (no spaces or
// newlines) survives a PUT → GET round-trip.
func TestIntegration_LargeValue(t *testing.T) {
	s := startServer(t)
	defer s.stop()

	c := newTextClient(t, s.Addr)
	defer c.close()

	// Build a 1 KB value using only URL-safe characters so the text protocol
	// parser does not split on whitespace.
	const size = 1024
	sb := strings.Builder{}
	sb.Grow(size)
	alphabet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
	for i := 0; i < size; i++ {
		sb.WriteByte(alphabet[i%len(alphabet)])
	}
	largeVal := sb.String()

	resp := c.send("PUT bigkey " + largeVal)
	if resp != "OK" {
		t.Fatalf("PUT large value: want OK, got %q", resp)
	}

	resp = c.send("GET bigkey")
	if resp != largeVal {
		t.Fatalf("GET large value: length want=%d got=%d; content mismatch", len(largeVal), len(resp))
	}
}

// TestIntegration_CrashRecovery verifies that keys written before a SIGKILL are
// still readable after the server restarts with the same data directory.
func TestIntegration_CrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping crash recovery test in short mode")
	}

	s := startServer(t)
	// Do NOT defer s.stop() here — we want to stop it manually then restart.

	c := newTextClient(t, s.Addr)

	const numKeys = 20
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("crash-key-%d", i)
		val := fmt.Sprintf("crash-val-%d", i)
		resp := c.send(fmt.Sprintf("PUT %s %s", key, val))
		if resp != "OK" {
			t.Fatalf("pre-crash PUT %s: got %q", key, resp)
		}
	}
	c.close()

	// Simulate crash with SIGKILL (no graceful shutdown, WAL not zeroed).
	if err := s.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_ = s.cmd.Wait()

	// Restart with the same data directory (do not clean up yet).
	s2 := startServer(t, "-data", s.DataDir)
	defer s2.stop()
	defer func() { s.DataDir = "" /* prevent double-remove */ }()
	defer s.stop()

	c2 := newTextClient(t, s2.Addr)
	defer c2.close()

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("crash-key-%d", i)
		want := fmt.Sprintf("crash-val-%d", i)
		resp := c2.send("GET " + key)
		if resp != want {
			t.Errorf("after crash recovery GET %s: want %q, got %q", key, want, resp)
		}
	}
}

// TestIntegration_GracefulRestart verifies that keys survive a SIGTERM
// (graceful shutdown) and are readable after restart.
func TestIntegration_GracefulRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping graceful restart test in short mode")
	}

	s := startServer(t)

	c := newTextClient(t, s.Addr)

	const numKeys = 20
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("graceful-key-%d", i)
		val := fmt.Sprintf("graceful-val-%d", i)
		resp := c.send(fmt.Sprintf("PUT %s %s", key, val))
		if resp != "OK" {
			t.Fatalf("pre-SIGTERM PUT %s: got %q", key, resp)
		}
	}
	c.close()

	// Graceful shutdown via SIGTERM.
	if err := s.cmd.Process.Signal(gracefulShutdownSignal()); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case <-done:
		// exited cleanly
	case <-time.After(10 * time.Second):
		_ = s.cmd.Process.Kill()
		t.Fatal("server did not exit within 10s after SIGTERM")
	}

	s2 := startServer(t, "-data", s.DataDir)
	defer s2.stop()
	defer func() { s.DataDir = "" }()
	defer s.stop()

	c2 := newTextClient(t, s2.Addr)
	defer c2.close()

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("graceful-key-%d", i)
		want := fmt.Sprintf("graceful-val-%d", i)
		resp := c2.send("GET " + key)
		if resp != want {
			t.Errorf("after graceful restart GET %s: want %q, got %q", key, want, resp)
		}
	}
}

// TestIntegration_HealthEndpoints verifies that /healthz and /readyz return 200.
func TestIntegration_HealthEndpoints(t *testing.T) {
	s := startServer(t)
	defer s.stop()

	httpBase := "http://" + s.MetricsAddr

	// /healthz must return 200 immediately (it is served before engine init).
	checkHTTP(t, httpBase+"/healthz", http.StatusOK)

	// /readyz may initially return 503 (engine still initializing).
	// Retry for up to 5 seconds.
	deadline := time.Now().Add(5 * time.Second)
	for {
		code := httpStatusCode(t, httpBase+"/readyz")
		if code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("/readyz: still %d after 5s", code)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestIntegration_MetricsEndpoint verifies that /metrics returns 200 and
// contains the "veltrixdb_" namespace.
func TestIntegration_MetricsEndpoint(t *testing.T) {
	s := startServer(t)
	defer s.stop()

	// Do some PUTs so metrics are non-trivial.
	c := newTextClient(t, s.Addr)
	for i := 0; i < 5; i++ {
		c.send(fmt.Sprintf("PUT metrics-key-%d value-%d", i, i))
	}
	c.close()

	resp, err := http.Get("http://" + s.MetricsAddr + "/metrics")
	if err != nil {
		t.Fatalf("/metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics: status %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "veltrixdb_") {
		t.Fatalf("/metrics: expected 'veltrixdb_' in body, got %d bytes with no match", len(body))
	}
}

// ── http helpers ──────────────────────────────────────────────────────────────

func httpStatusCode(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func checkHTTP(t *testing.T, url string, want int) {
	t.Helper()
	code := httpStatusCode(t, url)
	if code != want {
		t.Fatalf("GET %s: want %d, got %d", url, want, code)
	}
}
