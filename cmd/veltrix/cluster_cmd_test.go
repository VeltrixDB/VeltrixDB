package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The cluster subcommands speak GET / POST / DELETE /admin/cluster/members
// (cmd/server/membership.go).
func TestClusterCommands(t *testing.T) {
	var gotMethod, gotPath, gotQuery, gotBody string
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/cluster/members", func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"mode":"raft","node_id":"n1","version":12,"committed":true,"leader":"n1","self_status":"voter",
			  "members":[{"id":"n1","addr":"h1:9000","role":"voter"},{"id":"n4","addr":"h4:9000","role":"learner"}]}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"learner","detail":"promotion pending"}`))
	})
	mux.HandleFunc("/admin/cluster/members/", func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotQuery = r.Method, r.URL.Path, r.URL.RawQuery
		if strings.HasSuffix(r.URL.Path, "/nobody") {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"no such member nobody"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"removed"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, e, code := capture(t, srv, "cluster", "members")
	if code != 0 || !strings.Contains(o, "n4") || !strings.Contains(o, "learner") || !strings.Contains(o, "version=12") {
		t.Fatalf("members: code=%d out=%q err=%q", code, o, e)
	}
	o, e, code = capture(t, srv, "cluster", "add-node", "n5@10.0.0.5:9000", "--wait-ms", "1500")
	if code != 0 || gotMethod != http.MethodPost || !strings.Contains(o, "learner") {
		t.Fatalf("add-node: code=%d out=%q err=%q method=%s", code, o, e, gotMethod)
	}
	var body map[string]interface{}
	if err := json.Unmarshal([]byte(gotBody), &body); err != nil || body["id"] != "n5" || body["addr"] != "10.0.0.5:9000" || body["wait_ms"] != float64(1500) {
		t.Fatalf("add-node body %q (%v)", gotBody, err)
	}
	_, _, code = capture(t, srv, "cluster", "remove-node", "n2", "--force")
	if code != 0 || gotMethod != http.MethodDelete || gotPath != "/admin/cluster/members/n2" || gotQuery != "force=true" {
		t.Fatalf("remove-node: code=%d %s %s?%s", code, gotMethod, gotPath, gotQuery)
	}
	_, e, code = capture(t, srv, "cluster", "remove-node", "nobody")
	if code != 1 || !strings.Contains(e, "404") {
		t.Fatalf("remove unknown: code=%d err=%q", code, e)
	}
	if _, _, code = capture(t, srv, "cluster", "add-node", "bad"); code != 2 {
		t.Fatalf("bad add-node arg: code=%d, want usage (2)", code)
	}
}
