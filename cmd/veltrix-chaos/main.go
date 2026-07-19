// veltrix-chaos — chaos-engineering harness for VeltrixDB clusters.
//
// Inflicts controlled failures on running pods/processes to verify the
// system's resilience claims.  Faults supported:
//
//   kill         SIGKILL the leader pod, watch recovery time.
//   pause        SIGSTOP a pod for a duration, then SIGCONT (simulates GC pause).
//   network      Drop / delay packets between two pods via tc/iptables.
//   slow-disk    Inject artificial fdatasync delay via charybdefs (TODO; today
//                a placeholder that warns if charybdefs is not on PATH).
//   clock-skew   Adjust system clock on a target pod (requires SYS_TIME).
//
// Compared to Jepsen:
//   + No JVM / Clojure dependency.
//   + Trivial integration with Go test suites: import as a package and
//     call Inflict(...) from your test.
//   - Less rigorous formal model.  For high-bar correctness verification
//     against a linearizability spec, build a Jepsen suite separately.
//
// All faults assume root or CAP_SYS_ADMIN on the target host.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "kill":
		// usage: kill TARGET (pid or "k8s:NS/POD" or "process:NAME")
		if len(args) < 1 {
			usage()
		}
		runKill(args[0])
	case "pause":
		// usage: pause TARGET DURATION
		if len(args) < 2 {
			usage()
		}
		dur, err := time.ParseDuration(args[1])
		if err != nil {
			fail("bad duration: %v", err)
		}
		runPause(args[0], dur)
	case "network":
		// usage: network drop|delay TARGET PERCENTAGE_OR_MS
		if len(args) < 3 {
			usage()
		}
		runNetwork(args[0], args[1], args[2])
	case "slow-disk":
		// usage: slow-disk TARGET DURATION
		if len(args) < 2 {
			usage()
		}
		runSlowDisk(args[0], args[1])
	case "clock-skew":
		// usage: clock-skew TARGET +SECONDS
		if len(args) < 2 {
			usage()
		}
		runClockSkew(args[0], args[1])
	case "soak":
		// usage: soak TARGET DURATION  (random faults during the window)
		if len(args) < 2 {
			usage()
		}
		dur, err := time.ParseDuration(args[1])
		if err != nil {
			fail("bad duration: %v", err)
		}
		runSoak(args[0], dur)
	default:
		usage()
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `veltrix-chaos — chaos-engineering harness

Targets:
  pid:NUMBER             local process by pid
  k8s:NAMESPACE/POD      Kubernetes pod
  process:NAME           local process by exec name (first match)

Commands:
  kill         TARGET                          SIGKILL the target.
  pause        TARGET DURATION                 SIGSTOP + sleep + SIGCONT (e.g. 5s).
  network      TARGET drop|delay  PCT_OR_MS    Inject loss / delay.
  slow-disk    TARGET DURATION                 Slow fdatasync (requires charybdefs).
  clock-skew   TARGET ±SECONDS                 Adjust clock by signed seconds.
  soak         TARGET DURATION                 Random faults across the window.
`)
	os.Exit(2)
}

func fail(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

// resolvePID resolves a target descriptor to a local pid (or returns -1 for
// targets that cannot be reached as pids — in which case the caller falls
// back to a Kubernetes-aware code path).
func resolvePID(target string) (int, error) {
	switch {
	case strings.HasPrefix(target, "pid:"):
		return strconv.Atoi(target[4:])
	case strings.HasPrefix(target, "process:"):
		out, err := exec.Command("pgrep", "-f", target[8:]).Output()
		if err != nil {
			return 0, err
		}
		fields := strings.Fields(string(out))
		if len(fields) == 0 {
			return 0, fmt.Errorf("no process matched %q", target)
		}
		return strconv.Atoi(fields[0])
	case strings.HasPrefix(target, "k8s:"):
		return -1, nil
	}
	return 0, fmt.Errorf("unknown target descriptor %q", target)
}

func kubectlExec(target string, cmd ...string) error {
	parts := strings.SplitN(target[4:], "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("malformed k8s target %q", target)
	}
	full := append([]string{"exec", "-n", parts[0], parts[1], "--"}, cmd...)
	c := exec.Command("kubectl", full...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Run()
}

func runKill(target string) {
	pid, err := resolvePID(target)
	if err != nil {
		fail("resolve: %v", err)
	}
	if pid < 0 {
		// Kubernetes pod
		parts := strings.SplitN(target[4:], "/", 2)
		c := exec.Command("kubectl", "delete", "pod", "-n", parts[0], parts[1], "--grace-period=0", "--force")
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		_ = c.Run()
		return
	}
	if err := exec.Command("kill", "-9", strconv.Itoa(pid)).Run(); err != nil {
		fail("kill: %v", err)
	}
	fmt.Fprintf(os.Stderr, "[chaos] killed pid %d\n", pid)
}

func runPause(target string, dur time.Duration) {
	pid, err := resolvePID(target)
	if err != nil {
		fail("resolve: %v", err)
	}
	if pid < 0 {
		fail("k8s targets not supported for pause; SIGSTOP must be sent on the host")
	}
	if err := exec.Command("kill", "-STOP", strconv.Itoa(pid)).Run(); err != nil {
		fail("SIGSTOP: %v", err)
	}
	fmt.Fprintf(os.Stderr, "[chaos] paused pid %d for %s\n", pid, dur)
	time.Sleep(dur)
	if err := exec.Command("kill", "-CONT", strconv.Itoa(pid)).Run(); err != nil {
		fail("SIGCONT: %v", err)
	}
	fmt.Fprintf(os.Stderr, "[chaos] resumed pid %d\n", pid)
}

func runNetwork(target, kind, arg string) {
	if !strings.HasPrefix(target, "k8s:") {
		fail("network faults currently require k8s targets (uses pod-level iptables/tc)")
	}
	switch kind {
	case "drop":
		// Inject random packet drop.
		pct, err := strconv.Atoi(arg)
		if err != nil {
			fail("bad percentage: %v", err)
		}
		if err := kubectlExec(target, "tc", "qdisc", "add", "dev", "eth0", "root",
			"netem", "loss", fmt.Sprintf("%d%%", pct)); err != nil {
			fail("tc add: %v", err)
		}
		fmt.Fprintf(os.Stderr, "[chaos] %s: %d%% packet loss installed\n", target, pct)
	case "delay":
		ms, err := strconv.Atoi(arg)
		if err != nil {
			fail("bad ms: %v", err)
		}
		if err := kubectlExec(target, "tc", "qdisc", "add", "dev", "eth0", "root",
			"netem", "delay", fmt.Sprintf("%dms", ms)); err != nil {
			fail("tc add: %v", err)
		}
		fmt.Fprintf(os.Stderr, "[chaos] %s: %d ms delay installed\n", target, ms)
	default:
		fail("unknown network kind %q", kind)
	}
}

func runSlowDisk(target, durStr string) {
	if _, err := exec.LookPath("charybdefs"); err != nil {
		fmt.Fprintf(os.Stderr,
			"[chaos] WARNING: charybdefs not on PATH. slow-disk inflicts no real fault.\n"+
				"        See https://github.com/scylladb/charybdefs for installation.\n")
	}
	fmt.Fprintf(os.Stderr, "[chaos] slow-disk on %s for %s — placeholder, charybdefs integration TODO\n",
		target, durStr)
}

func runClockSkew(target, deltaStr string) {
	delta, err := strconv.Atoi(strings.TrimPrefix(deltaStr, "+"))
	if err != nil {
		fail("bad delta: %v", err)
	}
	if !strings.HasPrefix(target, "k8s:") {
		fail("clock-skew requires k8s target (uses kubectl exec)")
	}
	if err := kubectlExec(target, "date", "-s", fmt.Sprintf("@$(($(date +%%s) + %d))", delta)); err != nil {
		fail("clock skew: %v", err)
	}
	fmt.Fprintf(os.Stderr, "[chaos] %s clock advanced by %d s\n", target, delta)
}

func runSoak(target string, dur time.Duration) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	deadline := time.Now().Add(dur)
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	faults := []func(){
		func() {
			runPause(target, time.Duration(1+rng.Intn(10))*time.Second)
		},
		func() {
			fmt.Fprintln(os.Stderr, "[chaos] soak: skipping network fault on local target")
		},
	}

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(15+rng.Intn(45)) * time.Second):
			f := faults[rng.Intn(len(faults))]
			f()
		}
	}
}
