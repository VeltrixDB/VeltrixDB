package cluster

// signed_client.go — node-to-node requests to another node's transfer
// listener by ADDRESS (not by a node ID the partition map knows), signed with
// the cluster secret exactly like TransferAgent's own requests.  Membership
// traffic uses it: a joining node asks a seed to add it, a node forwards a
// membership change to the raft leader / replicated-mode coordinator, and
// replicated-mode nodes exchange membership versions — before the peer is in
// the local partition map, and (at startup) before the agent is running.
// The receiving side is TransferAgent.authMiddleware (CLAUDE.md invariant 56).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// SignedClient posts JSON to transfer listeners, signing every request with
// the cluster secret (when one is set) and using the transfer TLS settings.
type SignedClient struct {
	secret []byte
	scheme string
	client *http.Client
}

// HTTPError is a non-200 answer from a transfer listener.
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body) }

// NewSignedClient builds a client.  tlsCfg nil / disabled → plain HTTP.
func NewSignedClient(secret []byte, tlsCfg *TransferTLSConfig, timeout time.Duration) (*SignedClient, error) {
	c := &SignedClient{
		secret: append([]byte(nil), secret...),
		scheme: "http",
		client: &http.Client{Timeout: timeout},
	}
	if tlsCfg != nil && tlsCfg.Enabled {
		_, clientTLS, err := buildTransferTLS(tlsCfg)
		if err != nil {
			return nil, fmt.Errorf("transfer TLS: %w", err)
		}
		c.scheme = "https"
		c.client = &http.Client{Timeout: timeout, Transport: &http.Transport{TLSClientConfig: clientTLS}}
	}
	return c, nil
}

// PostJSON sends in as JSON to path on the transfer listener at addr and
// decodes a 200 reply into out (out may be nil).  A non-200 reply is an
// *HTTPError carrying the status and body text.
func (c *SignedClient) PostJSON(ctx context.Context, addr, path string, in, out interface{}) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.scheme+"://"+addr+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if len(c.secret) > 0 {
		ts := fmt.Sprint(time.Now().Unix())
		req.Header.Set(clusterTimeHeader, ts)
		req.Header.Set(clusterAuthHeader, signClusterRequest(c.secret, ts, req.Method, req.URL.Path, body))
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &HTTPError{Status: resp.StatusCode, Body: strings.TrimSpace(string(msg))}
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Authenticated reports whether requests on this agent's listener must be
// signed (a cluster secret is set).  Membership endpoints refuse to work on
// a listener that is neither signed nor mutual-TLS.
func (ta *TransferAgent) Authenticated() bool { return len(ta.secret) > 0 }

// ForgetNodeTransferAddr drops the transfer address registered for nodeID
// (a member that left).
func (pm *PartitionMap) ForgetNodeTransferAddr(nodeID string) {
	transferAddrs.Delete(nodeID)
}

// ReceiveFunc applies one received key; see SetReceiveFunc.
type ReceiveFunc func(kv KeyValue) error

// SetReceiveFunc replaces how /transfer/keys applies each received key
// (default: LocalStore.Put).  cmd/server's replicated mode installs a
// put-if-absent that marks the key as applied from a peer, so a migrated
// copy never overwrites a newer write the replication stream delivered and
// never re-enters this node's catch-up feed as its own write.  Call before
// Start.
func (ta *TransferAgent) SetReceiveFunc(fn ReceiveFunc) { ta.receive = fn }
