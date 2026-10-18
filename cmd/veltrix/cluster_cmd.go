package main

// cluster_cmd.go — `veltrix cluster members | add-node | remove-node` over
// GET / POST / DELETE /admin/cluster/members (cmd/server/membership.go).

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type clusterMember struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
	Role string `json:"role"`
}

type clusterMembers struct {
	Mode        string          `json:"mode"`
	NodeID      string          `json:"node_id"`
	Version     uint64          `json:"version"`
	Committed   bool            `json:"committed"`
	Leader      string          `json:"leader"`
	Coordinator string          `json:"coordinator"`
	SelfStatus  string          `json:"self_status"`
	Members     []clusterMember `json:"members"`
}

func cmdCluster(c config, args []string, fail func(error) int, usage func(string) int) int {
	if len(args) == 0 {
		return usage("veltrix cluster members | add-node ID@HOST:PORT | remove-node ID [--force]")
	}
	switch args[0] {
	case "members", "ls":
		return fail(cmdClusterMembers(c.adminAddr, c.rawJSON))
	case "add-node", "add":
		if len(args) < 2 {
			return usage("veltrix cluster add-node ID@HOST:PORT")
		}
		at := strings.Index(args[1], "@")
		if at <= 0 || at == len(args[1])-1 {
			return usage("veltrix cluster add-node ID@HOST:PORT (HOST:PORT is the node's client --addr)")
		}
		body := map[string]interface{}{"id": args[1][:at], "addr": args[1][at+1:]}
		if c.waitMs > 0 {
			body["wait_ms"] = c.waitMs
		}
		b, _ := json.Marshal(body)
		return fail(clusterCall(c.adminAddr, http.MethodPost, "/admin/cluster/members", string(b), c.rawJSON))
	case "remove-node", "rm":
		if len(args) < 2 {
			return usage("veltrix cluster remove-node ID [--force]")
		}
		q := url.Values{}
		if c.force {
			q.Set("force", "true")
		}
		if c.waitMs > 0 {
			q.Set("wait_ms", strconv.Itoa(c.waitMs))
		}
		path := "/admin/cluster/members/" + url.PathEscape(args[1])
		if len(q) > 0 {
			path += "?" + q.Encode()
		}
		return fail(clusterCall(c.adminAddr, http.MethodDelete, path, "", c.rawJSON))
	}
	return usage("veltrix cluster members | add-node ID@HOST:PORT | remove-node ID [--force]")
}

func cmdClusterMembers(adminAddr string, raw bool) error {
	data, err := adminGet(adminAddr, "/admin/cluster/members")
	if err != nil {
		return err
	}
	if raw {
		fmt.Fprintln(out, strings.TrimSpace(string(data)))
		return nil
	}
	var m clusterMembers
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	head := fmt.Sprintf("mode=%s  version=%d", m.Mode, m.Version)
	if m.Mode == "raft" {
		head += fmt.Sprintf("  committed=%v  leader=%s", m.Committed, m.Leader)
	} else if m.Coordinator != "" {
		head += "  coordinator=" + m.Coordinator
	}
	fmt.Fprintln(out, bold(head))
	fmt.Fprintf(out, "this node: %s (%s)\n\n", m.NodeID, m.SelfStatus)
	fmt.Fprintf(out, "%-20s %-28s %s\n", "ID", "ADDRESS", "ROLE")
	for _, mb := range m.Members {
		fmt.Fprintf(out, "%-20s %-28s %s\n", mb.ID, mb.Addr, mb.Role)
	}
	return nil
}

// clusterCall sends a membership change; 202 (learner, promotion pending) is
// success too.
func clusterCall(adminAddr, method, path, body string, raw bool) error {
	u := baseURL(adminAddr) + path
	var rb io.Reader
	if body != "" {
		rb = strings.NewReader(body)
	}
	req, err := newRequest(method, u, rb)
	if err != nil {
		return err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, u, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return httpError(resp.StatusCode, u, data)
	}
	if raw {
		fmt.Fprintln(out, strings.TrimSpace(string(data)))
		return nil
	}
	var r struct {
		Status  string          `json:"status"`
		Detail  string          `json:"detail"`
		Version uint64          `json:"version"`
		Members []clusterMember `json:"members"`
		Failed  []string        `json:"failed"`
	}
	_ = json.Unmarshal(data, &r)
	fmt.Fprintf(out, "%s %s\n", green("ok:"), r.Status)
	if r.Detail != "" {
		fmt.Fprintln(out, r.Detail)
	}
	if len(r.Failed) > 0 {
		fmt.Fprintf(out, "not yet reached (they pull the new membership within seconds): %s\n", strings.Join(r.Failed, ", "))
	}
	for _, mb := range r.Members {
		fmt.Fprintf(out, "  %-20s %-28s %s\n", mb.ID, mb.Addr, mb.Role)
	}
	return nil
}
