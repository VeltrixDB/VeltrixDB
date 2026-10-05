# VeltrixDB Clients

The client SDKs have moved to the dedicated **[Veltrixdb-client](../../Veltrixdb-client/)** repository.

| Language | Location |
|----------|----------|
| Go       | `Veltrixdb-client/go/` |
| Java     | `Veltrixdb-client/java/` |
| Python   | `Veltrixdb-client/python/` |
| Node.js  | `Veltrixdb-client/nodejs/` |
| Rust     | `Veltrixdb-client/rust/` |
| C++      | `Veltrixdb-client/cpp/` |

## Search API support

The Go client in this repository (`client/`, used by the server's own tests,
`cmd/loadtest`, `cmd/admin` and `bench/compare`) supports the full search API
on `client.BinaryConn` (binary protocol) and `client.TCPConn` (text
protocol): `VCreateWithOptions`, `VCreate`, `VSetNS`, `VSet`, `VDel`,
`VSearchWithOptions`, `VSearch`, `TSet`, `TDel`, `TSearch`, `HSearch`,
`IdxCreate`, `IdxDrop`, `IdxQuery` (`VSet` / `VSearch` use namespace
`default`). The cluster-aware `client.Client` has no search methods. The
SDKs in Veltrixdb-client were not updated in the same change; from any
language the text-protocol commands (`VCREATE`, `VSET`, `VSEARCH`, `VDEL`,
`TSET`, `TDEL`, `TSEARCH`, `HSEARCH`, `IDXCREATE`, `IDXQUERY`) work over a
plain TCP socket, one command per line (so `TSET` text cannot contain
newlines and filter values cannot contain spaces). See
[docs/vector-search.md](../docs/vector-search.md).

## Go client (`client/`) wire behaviour

- **TTL.** The binary PUT frame (0x01) has no TTL field. `BinaryConn.Put`
  and `Pipeline.Put` with `ttl > 0` send a one-entry MPUT (0x06), whose
  per-entry header carries the TTL; `ttl ≤ 0` (no expiry) uses the plain PUT
  frame. `client.Client.Put(..., WithTTL(n))` sends `PUTEX <key> <n> <value>`
  for `n ≥ 1`; `TCPConn.PutEx` is the text-protocol call.
- **Consistency.** The protocol carries no per-request consistency level.
  `WithConsistency` and `ClientConfig.ConsistencyLevel` are kept for source
  compatibility, have no effect, and are marked deprecated; the guarantee is
  set on the server (`--mode`, `--consistency`, `--linearizable-reads`).
- **Errors vs misses.** `TCPConn.Get` / `BinaryConn.Get` return `(nil, nil)`
  only for a genuine miss (`ERR key not found` / `ERR key expired`, binary
  status 0x02). Any other error reply — `MOVED` from a raft follower under
  `--linearizable-reads`, a read-index timeout, an RBAC denial — is returned
  as an error, and `client.Client.Get` follows `MOVED` like `Put` does.
  `BinaryConn.MGet` / `MPut` surface the server's whole-frame error message.
- **Binary frame layouts** for the namespace and hash commands are
  documented above the opcode constants in `cmd/server/main.go`, written from
  the parser (`NSPUT` is `[0x0A][2B nsLen][4B valLen][2B keyLen][4B ttl]` +
  ns + key + value; `NSGET` / `NSDEL` are `[op][2B nsLen][4B keyLen]` + ns +
  key; `HSET` is `[0x10][2B keyLen][4B valLen][2B fieldLen][4B ttl]` + key +
  field + value).
