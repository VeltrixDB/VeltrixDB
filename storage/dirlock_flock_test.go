//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package storage

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const dirLockHelperEnv = "VELTRIX_DIRLOCK_HELPER_DIR"

// TestDataDirLockHelperProcess is not a test: TestDataDirLock_OtherProcessRefused
// re-executes the test binary with dirLockHelperEnv set, and this function
// then plays the "live server" — it opens an engine on the dir, writes a key,
// prints READY and holds the engine open until stdin closes.
func TestDataDirLockHelperProcess(t *testing.T) {
	dir := os.Getenv(dirLockHelperEnv)
	if dir == "" {
		t.Skip("helper process for TestDataDirLock_OtherProcessRefused")
	}
	se, err := NewStorageEngine(testStorageConfig(dir))
	if err != nil {
		fmt.Printf("OPEN-FAILED %v\n", err)
		os.Exit(2)
	}
	<-se.ReplayDone
	if err := se.Put("from-helper", []byte("helper-value"), -1); err != nil {
		fmt.Printf("PUT-FAILED %v\n", err)
		os.Exit(2)
	}
	fmt.Println("READY")
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n') // until the parent closes stdin
	if err := se.Close(); err != nil {
		fmt.Printf("CLOSE-FAILED %v\n", err)
		os.Exit(2)
	}
	os.Exit(0)
}

// The real failure: veltrixdb-backup is a separate process from the server.
func TestDataDirLock_OtherProcessRefused(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDataDirLockHelperProcess$", "-test.count=1")
	cmd.Env = append(os.Environ(), dirLockHelperEnv+"="+dir)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()

	ready := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			if line == "READY" || strings.HasSuffix(line, "FAILED") || strings.Contains(line, "FAILED ") {
				ready <- line
				return
			}
		}
		ready <- "EOF"
	}()
	select {
	case line := <-ready:
		if line != "READY" {
			t.Fatalf("helper did not become ready: %s", line)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("helper did not become ready within 60s")
	}

	walBefore := fileSize(t, dir+"/wal.log")
	se, err := NewStorageEngine(testStorageConfig(dir))
	if err == nil {
		<-se.ReplayDone
		se.Close()
		t.Fatalf("NewStorageEngine on a dir held by pid %d succeeded; want ErrDataDirLocked "+
			"(wal.log %d -> %d bytes)", cmd.Process.Pid, walBefore, fileSize(t, dir+"/wal.log"))
	}
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("error = %v, want ErrDataDirLocked", err)
	}
	if want := fmt.Sprintf("pid %d", cmd.Process.Pid); !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not name the holder (%s)", err, want)
	}
	if got := fileSize(t, dir+"/wal.log"); got != walBefore {
		t.Errorf("wal.log changed size %d -> %d after a refused open", walBefore, got)
	}

	// Helper closes its engine and exits; the lock goes with it.
	stdin.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("helper exit: %v", err)
	}
	se, err = NewStorageEngine(testStorageConfig(dir))
	if err != nil {
		t.Fatalf("open after the holder exited: %v", err)
	}
	t.Cleanup(func() { se.Close() })
	<-se.ReplayDone
	if got, err := se.Get("from-helper"); err != nil || string(got) != "helper-value" {
		t.Fatalf("Get from-helper = %q, %v", got, err)
	}
}
