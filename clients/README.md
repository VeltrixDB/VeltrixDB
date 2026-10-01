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
