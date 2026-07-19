// guard.go — access control for the /admin/* HTTP surface.
//
// The admin endpoints can read every record (/admin/changes streams full
// values and tombstones) and destroy data, so they are never served
// unauthenticated to non-loopback clients:
//
//   - token == ""  → loopback-only. Requests from any non-loopback address
//     are rejected 403 with a hint to set --admin-token.
//   - token != ""  → the request must carry the token in
//     "Authorization: Bearer <token>" or "X-Admin-Token: <token>".
//     Loopback requests must present it too — one rule, no carve-outs.
//
// /metrics, /healthz and /readyz are NOT behind this guard; Prometheus
// scrapes and Kubernetes probes keep working with no configuration.
package adminapi

import (
	"crypto/subtle"
	"net"
	"net/http"
	"strings"
)

// Guard wraps next with the admin access policy described in the package
// comment. token may be empty (loopback-only mode).
func Guard(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" {
			if subtle.ConstantTimeCompare([]byte(bearerToken(r)), []byte(token)) == 1 {
				next.ServeHTTP(w, r)
				return
			}
			http.Error(w, "admin API: missing or invalid token (Authorization: Bearer or X-Admin-Token)", http.StatusUnauthorized)
			return
		}
		if isLoopback(r.RemoteAddr) {
			next.ServeHTTP(w, r)
			return
		}
		http.Error(w, "admin API is restricted to loopback; set --admin-token to allow remote access", http.StatusForbidden)
	})
}

// bearerToken extracts the credential from Authorization: Bearer <t> or the
// X-Admin-Token header, preferring the former.
func bearerToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if strings.HasPrefix(h, "Bearer ") {
			return strings.TrimPrefix(h, "Bearer ")
		}
	}
	return r.Header.Get("X-Admin-Token")
}

// isLoopback reports whether remoteAddr ("host:port") is a loopback IP.
// Unparseable addresses are treated as non-loopback (fail closed).
func isLoopback(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
