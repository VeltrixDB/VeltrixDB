// kubectl-veltrix: a kubectl plugin for VeltrixDB operations.
//
// Install: place the compiled binary anywhere on PATH named exactly
// `kubectl-veltrix`. kubectl auto-discovers plugins via the kubectl-PLUGIN
// naming convention.  Then run:
//
//   kubectl veltrix stats                            # engine stats (GET /admin/stats)
//   kubectl veltrix gc-status                        # GC + scrubber status
//   kubectl veltrix checkpoint                       # force WAL checkpoint
//   kubectl veltrix migrate                          # run schema migrations
//   kubectl veltrix quota-set tenant_42 5000 1000000 --burst 10000
//   kubectl veltrix quotas                           # list all quotas
//   kubectl veltrix cdc-tail --prefix orders/        # stream CDC events
//   kubectl veltrix version
//
// All commands work by:
//   1. Resolving the target pod via `kubectl get pods -l <selector>`.
//   2. Using `kubectl port-forward` to reach the metrics/admin port.
//   3. Hitting the admin HTTP API on localhost, sending --admin-token (env
//      VELTRIX_ADMIN_TOKEN) as "Authorization: Bearer" when set — required
//      when the server runs with --admin-token (adminapi/guard.go).
//
// --admin-url skips steps 1–2 and talks to an admin URL directly (an existing
// port-forward, or a reachable Service).
//
// Kept dependency-free: no client-go, no cobra. We shell out to kubectl
// directly to avoid a 50 MB plugin binary and to keep the kubeconfig /
// auth path consistent with the user's normal workflow.

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const usageText = `kubectl-veltrix — admin plugin for VeltrixDB

Usage:
  kubectl veltrix [flags] COMMAND [args] [flags]

Flags may appear before or after the command.

Commands:
  stats                   Print engine stats (GET /admin/stats).
  gc-status               Print VLog GC + scrubber metrics (GET /metrics).
  checkpoint              Force a WAL checkpoint (POST /admin/checkpoint).
  migrate                 Run schema migrations (POST /admin/migrate).
  quotas                  List per-namespace quotas (GET /admin/quotas).
  quota-set NS WPS MAX    Set quota for namespace NS: writes_per_sec=WPS,
                          max_keys=MAX (0 = unlimited), burst=--burst.
  cdc-tail                Stream CDC events (JSON Lines) until interrupted.
  version                 Print schema version + encryption state.

Flags:
  --namespace, -n         Kubernetes namespace (default: veltrixdb)
  --label-selector, -l    Pod selector (default: app.kubernetes.io/name=veltrixdb)
  --admin-port            Admin HTTP port on the pod (default: 2112)
  --admin-token           Token for /admin/* (Authorization: Bearer); required when
                          the server runs with --admin-token (env VELTRIX_ADMIN_TOKEN)
  --admin-url             Use this admin URL directly (skip pod lookup/port-forward)
  --burst                 quota-set: token-bucket burst in writes (default: WPS)
  --prefix                cdc-tail: key-prefix filter
`

type opts struct {
	namespace string
	selector  string
	adminPort int
	adminURL  string
	token     string
	prefix    string
	burst     int
}

var (
	out    io.Writer = os.Stdout
	errOut io.Writer = os.Stderr
	// discover resolves the admin base URL; replaced in tests.
	discover = discoverPod
)

// parseArgs accepts flags before, between or after positional arguments
// (Go's flag package alone stops at the first positional). "--" ends flags.
func parseArgs(argv []string, stderr io.Writer) (opts, []string, error) {
	o := opts{token: os.Getenv("VELTRIX_ADMIN_TOKEN")}
	fs := flag.NewFlagSet("kubectl-veltrix", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usageText) }
	fs.StringVar(&o.namespace, "namespace", "veltrixdb", "Kubernetes namespace")
	fs.StringVar(&o.namespace, "n", "veltrixdb", "Kubernetes namespace (short)")
	fs.StringVar(&o.selector, "label-selector", "app.kubernetes.io/name=veltrixdb", "pod selector")
	fs.StringVar(&o.selector, "l", "app.kubernetes.io/name=veltrixdb", "pod selector (short)")
	fs.IntVar(&o.adminPort, "admin-port", 2112, "admin HTTP port on the pod")
	fs.StringVar(&o.adminURL, "admin-url", "", "admin base URL; skips pod lookup and port-forward")
	fs.StringVar(&o.token, "admin-token", o.token, "admin API token (env VELTRIX_ADMIN_TOKEN)")
	fs.StringVar(&o.prefix, "prefix", "", "key prefix for cdc-tail")
	fs.IntVar(&o.burst, "burst", 0, "quota-set token-bucket burst (0 = same as writes_per_sec)")

	var pos []string
	for {
		if err := fs.Parse(argv); err != nil {
			return o, nil, err
		}
		rest := fs.Args()
		if len(rest) == 0 {
			break
		}
		if consumed := len(argv) - len(rest); consumed > 0 && argv[consumed-1] == "--" {
			pos = append(pos, rest...)
			break
		}
		pos = append(pos, rest[0])
		argv = rest[1:]
	}
	return o, pos, nil
}

func main() { os.Exit(run(os.Args[1:])) }

func run(argv []string) int {
	o, args, err := parseArgs(argv, errOut)
	if err == flag.ErrHelp {
		return 0
	}
	if err != nil {
		return 2
	}
	if len(args) == 0 {
		fmt.Fprint(errOut, usageText)
		return 2
	}
	cmd, tail := args[0], args[1:]
	if !knownCommand(cmd) {
		fmt.Fprintf(errOut, "unknown command: %s\n%s", cmd, usageText)
		return 2
	}
	if cmd == "quota-set" && len(tail) < 3 {
		fmt.Fprintln(errOut, "usage: kubectl veltrix quota-set NS WRITES_PER_SEC MAX_KEYS [--burst N]")
		return 2
	}

	base := strings.TrimRight(o.adminURL, "/")
	if base == "" {
		b, stop, err := discover(o)
		if err != nil {
			fmt.Fprintln(errOut, err)
			return 1
		}
		defer stop()
		base = b
	}
	if err := runCommand(base, o, cmd, tail); err != nil {
		fmt.Fprintln(errOut, "error:", err)
		return 1
	}
	return 0
}

func knownCommand(c string) bool {
	switch c {
	case "stats", "gc-status", "checkpoint", "migrate", "quotas", "quota-set", "cdc-tail", "version":
		return true
	}
	return false
}

// discoverPod finds a pod and port-forwards to its admin port.
func discoverPod(o opts) (string, func(), error) {
	pod, err := pickPod(o.namespace, o.selector)
	if err != nil {
		return "", nil, fmt.Errorf("could not find a VeltrixDB pod: %w", err)
	}
	localPort, stop, err := portForward(o.namespace, pod, o.adminPort)
	if err != nil {
		return "", nil, fmt.Errorf("port-forward failed: %w", err)
	}
	return fmt.Sprintf("http://127.0.0.1:%d", localPort), stop, nil
}

// runCommand executes one admin command against base (e.g. http://127.0.0.1:NNNN).
func runCommand(base string, o opts, cmd string, tail []string) error {
	c := &adminClient{base: base, token: o.token}
	switch cmd {
	case "stats":
		return c.do(http.MethodGet, "/admin/stats", nil, out)
	case "gc-status":
		var buf strings.Builder
		if err := c.do(http.MethodGet, "/metrics", nil, &buf); err != nil {
			return err
		}
		needles := []string{
			"vlog_gc_runs_total", "vlog_gc_emergency_runs_total",
			"vlog_garbage_ratio", "scrub_records_total", "scrub_corruption_total",
			"storage_write_admission_throttles_total",
		}
		for _, line := range strings.Split(buf.String(), "\n") {
			for _, n := range needles {
				if strings.Contains(line, n) {
					fmt.Fprintln(out, line)
					break
				}
			}
		}
		return nil
	case "checkpoint":
		return c.do(http.MethodPost, "/admin/checkpoint", nil, out)
	case "migrate":
		return c.do(http.MethodPost, "/admin/migrate", nil, out)
	case "quotas":
		return c.do(http.MethodGet, "/admin/quotas", nil, out)
	case "quota-set":
		form, err := quotaForm(tail[0], tail[1], tail[2], o.burst)
		if err != nil {
			return err
		}
		return c.do(http.MethodPost, "/admin/quotas", form, out)
	case "cdc-tail":
		q := url.Values{}
		if o.prefix != "" {
			q.Set("prefix", o.prefix)
		}
		path := "/admin/cdc"
		if len(q) > 0 {
			path += "?" + q.Encode()
		}
		return c.do(http.MethodGet, path, nil, out)
	case "version":
		return c.do(http.MethodGet, "/admin/version", nil, out)
	}
	return fmt.Errorf("unknown command: %s", cmd)
}

// quotaForm builds the POST /admin/quotas form (adminapi.handleQuotas reads
// ns, writes_per_sec, burst, max_keys). An unset --burst sends burst =
// writes_per_sec (one second of headroom), matching storage.QuotaManager's
// treatment of burst=0.
func quotaForm(ns, wps, maxKeys string, burst int) (url.Values, error) {
	w, err := strconv.Atoi(wps)
	if err != nil || w < 0 {
		return nil, fmt.Errorf("WRITES_PER_SEC must be a non-negative integer, got %q", wps)
	}
	m, err := strconv.ParseInt(maxKeys, 10, 64)
	if err != nil || m < 0 {
		return nil, fmt.Errorf("MAX_KEYS must be a non-negative integer, got %q", maxKeys)
	}
	if burst < 0 {
		return nil, errors.New("--burst must be >= 0")
	}
	if burst == 0 {
		// Same as the server's own default for burst=0, but explicit, so the
		// request (and the GET /admin/quotas BurstWrites that follows) says
		// what the bucket really is.
		burst = w
	}
	v := url.Values{}
	v.Set("ns", ns)
	v.Set("writes_per_sec", strconv.Itoa(w))
	v.Set("burst", strconv.Itoa(burst))
	v.Set("max_keys", strconv.FormatInt(m, 10))
	return v, nil
}

type adminClient struct {
	base, token string
}

// do issues one request and copies the body to w. Non-2xx responses become
// errors (with a hint for adminapi.Guard's 401/403).
func (c *adminClient) do(method, path string, form url.Values, w io.Writer) error {
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequest(method, c.base+path, body)
	if err != nil {
		return err
	}
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		msg := fmt.Sprintf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(b)))
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			msg += " (pass --admin-token or set VELTRIX_ADMIN_TOKEN to the server's token)"
		case http.StatusForbidden:
			msg += " (server has no --admin-token and only serves /admin/* to loopback)"
		}
		return errors.New(msg)
	}
	_, err = io.Copy(w, resp.Body)
	return err
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
