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

The Go client in this repository (`client/`, used by the server's own tests
and `bench/compare`) supports the full search API: `VCreateWithOptions`,
`VSetNS`, `VDel`, `VSearchWithOptions`, `TSet`, `TDel`, `TSearch`, `HSearch`
(binary and text protocol). The SDKs in Veltrixdb-client were not updated in
the same change; from any language the text-protocol commands (`VCREATE`,
`VSET`, `VSEARCH`, `TSET`, `TSEARCH`, `HSEARCH`, `VDEL`, `TDEL`) work over a
plain TCP socket. See [docs/vector-search.md](../docs/vector-search.md).
