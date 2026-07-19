// kubectl-veltrix: a kubectl plugin for VeltrixDB operations.
//
// Install: place the compiled binary anywhere on PATH named exactly
// `kubectl-veltrix`. kubectl auto-discovers plugins via the kubectl-PLUGIN
// naming convention.  Then run:
//
//   kubectl veltrix stats                            # cluster-wide engine stats
//   kubectl veltrix gc-status                        # GC + scrubber status
//   kubectl veltrix checkpoint                       # force WAL checkpoint
//   kubectl veltrix migrate                          # run schema migrations
//   kubectl veltrix quota set tenant_42 --writes-per-sec 5000 --max-keys 1000000
//   kubectl veltrix quotas                           # list all quotas
//   kubectl veltrix cdc tail --prefix orders/         # stream CDC events
//   kubectl veltrix backup s3://...                  # one-shot backup (TODO)
//
// All commands work by:
//   1. Resolving the target pod via `kubectl get pods -l app=veltrixdb`.
//   2. Using `kubectl port-forward` to reach the metrics/admin port.
//   3. Hitting the admin HTTP API on localhost.
//
// Kept dependency-free: no client-go, no cobra. We shell out to kubectl
// directly to avoid a 50 MB plugin binary and to keep the kubeconfig /
// auth path consistent with the user's normal workflow.

package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

const usageText = `kubectl-veltrix — admin plugin for VeltrixDB

Usage:
  kubectl veltrix [--namespace NS] [--label-selector SEL] [--admin-port PORT] COMMAND

Commands:
  stats                   Print engine stats from the first matching pod.
  gc-status               Print VLog GC + scrubber status.
  checkpoint              Force a WAL checkpoint (POST /admin/checkpoint).
  migrate                 Run schema migrations (POST /admin/migrate).
  quotas                  List per-namespace quotas (GET /admin/quotas).
  quota-set NS WPS MAX    Set quota: writes_per_sec=WPS, max_keys=MAX.
  cdc-tail [--prefix=]    Stream CDC events to stdout until interrupted.
  version                 Print engine version + schema version.

Flags:
  --namespace, -n         Kubernetes namespace (default: veltrixdb)
  --label-selector, -l    Pod selector (default: app.kubernetes.io/name=veltrixdb)
  --admin-port            Admin HTTP port on the pod (default: 2112)
`

type opts struct {
	namespace string
	selector  string
	adminPort int
	prefix    string
}

func main() {
	var o opts
	flag.StringVar(&o.namespace, "namespace", "veltrixdb", "Kubernetes namespace")
	flag.StringVar(&o.namespace, "n", "veltrixdb", "Kubernetes namespace (short)")
	flag.StringVar(&o.selector, "label-selector", "app.kubernetes.io/name=veltrixdb", "pod selector")
	flag.StringVar(&o.selector, "l", "app.kubernetes.io/name=veltrixdb", "pod selector (short)")
	flag.IntVar(&o.adminPort, "admin-port", 2112, "admin HTTP port on the pod")
	flag.StringVar(&o.prefix, "prefix", "", "key prefix for cdc-tail")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usageText) }
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	pod, err := pickPod(o.namespace, o.selector)
	if err != nil {
		fail("could not find a VeltrixDB pod: %v", err)
	}

	localPort, stop, err := portForward(o.namespace, pod, o.adminPort)
	if err != nil {
		fail("port-forward failed: %v", err)
	}
	defer stop()
	base := fmt.Sprintf("http://127.0.0.1:%d", localPort)

	cmd := args[0]
	tail := args[1:]
	switch cmd {
	case "stats":
		print200(fmt.Sprintf("%s/admin/stats", base))
	case "gc-status":
		// Filter the metrics endpoint for GC-relevant lines.
		printGrep(fmt.Sprintf("%s/metrics", base), []string{
			"vlog_gc_runs_total", "vlog_gc_emergency_runs_total",
			"vlog_garbage_ratio", "scrub_records_total", "scrub_corruption_total",
			"storage_write_admission_throttles_total",
		})
	case "checkpoint":
		post(fmt.Sprintf("%s/admin/checkpoint", base))
	case "migrate":
		post(fmt.Sprintf("%s/admin/migrate", base))
	case "quotas":
		print200(fmt.Sprintf("%s/admin/quotas", base))
	case "quota-set":
		if len(tail) < 3 {
			fail("usage: kubectl veltrix quota-set NS WRITES_PER_SEC MAX_KEYS")
		}
		body := fmt.Sprintf("ns=%s&writes_per_sec=%s&max_keys=%s", tail[0], tail[1], tail[2])
		postForm(fmt.Sprintf("%s/admin/quotas", base), body)
	case "cdc-tail":
		streamCDC(fmt.Sprintf("%s/admin/cdc?prefix=%s", base, o.prefix))
	case "version":
		print200(fmt.Sprintf("%s/admin/version", base))
	default:
		fail("unknown command: %s", cmd)
	}
}

func pickPod(ns, selector string) (string, error) {
	out, err := exec.Command("kubectl", "get", "pods", "-n", ns,
		"-l", selector, "-o", "jsonpath={.items[0].metadata.name}").Output()
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(string(out))
	if name == "" {
		return "", fmt.Errorf("no pods matched selector %q in namespace %q", selector, ns)
	}
	return name, nil
}

// portForward runs `kubectl port-forward POD :REMOTE` and returns the local
// port that kubectl chose plus a stop func that kills the kubectl child.
func portForward(ns, pod string, remote int) (int, func(), error) {
	cmd := exec.Command("kubectl", "port-forward",
		"-n", ns, "pod/"+pod, fmt.Sprintf(":%d", remote))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, nil, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return 0, nil, err
	}
	// First line is "Forwarding from 127.0.0.1:NNNNN -> REMOTE"
	buf := make([]byte, 256)
	n, _ := stdout.Read(buf)
	line := string(buf[:n])
	idx := strings.Index(line, "127.0.0.1:")
	if idx < 0 {
		_ = cmd.Process.Kill()
		return 0, nil, fmt.Errorf("could not parse port-forward stdout: %q", line)
	}
	end := strings.IndexAny(line[idx+10:], " \r\n\t-")
	if end < 0 {
		end = len(line) - idx - 10
	}
	port := line[idx+10 : idx+10+end]
	var p int
	if _, err := fmt.Sscanf(port, "%d", &p); err != nil {
		_ = cmd.Process.Kill()
		return 0, nil, err
	}
	stop := func() { _ = cmd.Process.Kill() }
	// Give kubectl a moment to actually start listening.
	time.Sleep(500 * time.Millisecond)
	return p, stop, nil
}

func print200(url string) {
	resp, err := http.Get(url)
	if err != nil {
		fail("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
}

func printGrep(url string, needles []string) {
	resp, err := http.Get(url)
	if err != nil {
		fail("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(body), "\n") {
		for _, n := range needles {
			if strings.Contains(line, n) {
				fmt.Println(line)
				break
			}
		}
	}
}

func post(url string) {
	resp, err := http.Post(url, "application/x-www-form-urlencoded", nil)
	if err != nil {
		fail("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
}

func postForm(url, body string) {
	resp, err := http.Post(url, "application/x-www-form-urlencoded", strings.NewReader(body))
	if err != nil {
		fail("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
}

func streamCDC(url string) {
	resp, err := http.Get(url)
	if err != nil {
		fail("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
