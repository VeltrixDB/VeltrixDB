package adminapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func guardedOK(token string) http.Handler {
	return Guard(token, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
}

func doReq(t *testing.T, h http.Handler, remoteAddr string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("GET", "/admin/stats", nil)
	req.RemoteAddr = remoteAddr
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestGuard_NoToken_LoopbackAllowed(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:5000", "[::1]:5000"} {
		if rec := doReq(t, guardedOK(""), addr, nil); rec.Code != 200 {
			t.Fatalf("loopback %s: got %d, want 200", addr, rec.Code)
		}
	}
}

func TestGuard_NoToken_RemoteRejected(t *testing.T) {
	for _, addr := range []string{"10.1.2.3:5000", "192.168.1.9:80", "[2001:db8::1]:443", "garbage"} {
		if rec := doReq(t, guardedOK(""), addr, nil); rec.Code != http.StatusForbidden {
			t.Fatalf("remote %s: got %d, want 403", addr, rec.Code)
		}
	}
}

func TestGuard_Token_ValidAllowsRemote(t *testing.T) {
	h := guardedOK("s3cret")
	if rec := doReq(t, h, "10.1.2.3:5000", map[string]string{"Authorization": "Bearer s3cret"}); rec.Code != 200 {
		t.Fatalf("bearer: got %d, want 200", rec.Code)
	}
	if rec := doReq(t, h, "10.1.2.3:5000", map[string]string{"X-Admin-Token": "s3cret"}); rec.Code != 200 {
		t.Fatalf("x-admin-token: got %d, want 200", rec.Code)
	}
}

func TestGuard_Token_InvalidOrMissingRejected(t *testing.T) {
	h := guardedOK("s3cret")
	cases := []map[string]string{
		nil,
		{"Authorization": "Bearer wrong"},
		{"Authorization": "s3cret"}, // missing Bearer prefix
		{"X-Admin-Token": ""},
	}
	for i, hdr := range cases {
		if rec := doReq(t, h, "10.1.2.3:5000", hdr); rec.Code != http.StatusUnauthorized {
			t.Fatalf("case %d: got %d, want 401", i, rec.Code)
		}
	}
}

func TestGuard_Token_LoopbackAlsoNeedsToken(t *testing.T) {
	h := guardedOK("s3cret")
	if rec := doReq(t, h, "127.0.0.1:5000", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("loopback without token: got %d, want 401", rec.Code)
	}
}
