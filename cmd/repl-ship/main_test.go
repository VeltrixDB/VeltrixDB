package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/VeltrixDB/veltrixdb/adminapi"
	"github.com/VeltrixDB/veltrixdb/storage"
)

func TestResolveTokenAndCDCURL(t *testing.T) {
	cases := []struct{ src, admin, want string }{
		{"", "", ""}, {"a", "", "a"}, {"", "b", "b"}, {"a", "b", "b"},
	}
	for _, c := range cases {
		if got := resolveToken(c.src, c.admin); got != c.want {
			t.Errorf("resolveToken(%q,%q)=%q", c.src, c.admin, got)
		}
	}
	if got := cdcURL("http://h:2112/", "a b&c"); got != "http://h:2112/admin/cdc?prefix=a+b%26c" {
		t.Errorf("cdcURL = %s", got)
	}
	if got := cdcURL("http://h:2112", ""); got != "http://h:2112/admin/cdc" {
		t.Errorf("cdcURL = %s", got)
	}
}

// The live stream and catch-up feed encode storage.CDCEvent with no json tags.
func TestCDCEventDecodesServerShape(t *testing.T) {
	for _, src := range []storage.CDCEvent{
		{Op: "PUT", Key: "k", Value: []byte{0, 1, 'x'}, Timestamp: 42},
		{Op: "DEL", Key: "k", Timestamp: 43},
	} {
		b, _ := json.Marshal(src)
		var ev cdcEvent
		if err := json.Unmarshal(b, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.Op != src.Op || ev.Key != src.Key || string(ev.Value) != string(src.Value) || ev.Timestamp != src.Timestamp {
			t.Errorf("%s -> %+v", b, ev)
		}
	}
}

func TestAdminGetSendsGuardToken(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("fine")) })
	srv := httptest.NewServer(adminapi.Guard("s3cret", ok))
	defer srv.Close()

	old := srcToken
	defer func() { srcToken = old }()
	for _, tc := range []struct {
		tok  string
		code int
	}{{"", 401}, {"nope", 401}, {"s3cret", 200}} {
		srcToken = tc.tok
		resp, err := adminGet(srv.URL + "/admin/cdc")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.code {
			t.Errorf("token %q: HTTP %d, want %d", tc.tok, resp.StatusCode, tc.code)
		}
	}
}

func TestCatchUpErrorIncludesBody(t *testing.T) {
	srv := httptest.NewServer(adminapi.Guard("s3cret", http.NotFoundHandler()))
	defer srv.Close()
	old := srcToken
	srcToken = ""
	defer func() { srcToken = old }()
	err := catchUp(srv.URL, "", newDstConn("127.0.0.1:1"), json.NewEncoder(&strings.Builder{}), 1,
		&checkpointFile{}, func(int64) {})
	if err == nil || !strings.Contains(err.Error(), "401") || !strings.Contains(err.Error(), "token") {
		t.Errorf("err = %v", err)
	}
}
