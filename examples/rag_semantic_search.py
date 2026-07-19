#!/usr/bin/env python3
"""RAG / semantic search on VeltrixDB — end to end, stdlib only.

The pattern:
  1. Chunk your documents; store each chunk's text + metadata as a KV record.
  2. Store the chunk's embedding under the SAME key with VSET (HNSW index).
  3. At question time: embed the question, VSEARCH top-k, GET the texts,
     assemble the prompt for your LLM.

One database holds both sides, so a deleted document can never keep serving
from a stale vector index — VDEL and DELETE share one write path, one WAL,
one backup.

This demo uses a deterministic hashing bag-of-words embedder so it runs with
no dependencies. In production, swap `embed()` for a real model
(voyage-3, text-embedding-3-small, all-MiniLM-L6-v2, ...) — every other line
stays the same.

Run:  python3 rag_semantic_search.py [host] [port]     (default 127.0.0.1 9000)
"""
import hashlib
import json
import math
import re
import socket
import sys

DIM = 64  # demo dimension; production models are 384–1536

# ── Tiny text-protocol client (the wire format is telnet-debuggable) ─────────
class Veltrix:
    def __init__(self, host="127.0.0.1", port=9000):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.buf = b""

    def _line(self):
        while b"\n" not in self.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("server closed connection")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\n", 1)
        return line.decode().rstrip("\r")

    def cmd(self, line):
        self.sock.sendall(line.encode() + b"\n")
        return self._line()

    def cmd_multi(self, line):
        """For commands that answer with N lines then END (VSEARCH, ...)."""
        self.sock.sendall(line.encode() + b"\n")
        out = []
        while True:
            ln = self._line()
            if ln == "END":
                return out
            if ln.startswith("ERR"):
                raise RuntimeError(ln)
            out.append(ln)

    def put(self, key, value: str):
        resp = self.cmd(f"PUT {key} {value}")
        if not resp.startswith("OK"):
            raise RuntimeError(resp)

    def get(self, key):
        resp = self.cmd(f"GET {key}")
        if resp.startswith("ERR") or resp == "NOT_FOUND":
            return None
        return resp[6:] if resp.startswith("VALUE ") else resp

    def vset(self, key, vec):
        resp = self.cmd("VSET " + key + " " + " ".join(f"{x:.6f}" for x in vec))
        if not resp.startswith("OK"):
            raise RuntimeError(resp)

    def vsearch(self, vec, k=3):
        lines = self.cmd_multi(
            f"VSEARCH {k} " + " ".join(f"{x:.6f}" for x in vec))
        out = []
        for ln in lines:
            key, score = ln.rsplit(" ", 1)
            out.append((key, float(score)))
        return out

# ── Demo embedder: deterministic hashing bag-of-words ────────────────────────
# Production: replace this function with a call to your embedding model.
def embed(text: str):
    v = [0.0] * DIM
    for tok in re.findall(r"[a-z0-9]+", text.lower()):
        h = int.from_bytes(hashlib.md5(tok.encode()).digest()[:4], "little")
        v[h % DIM] += 1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]

# ── 1. The corpus: chunks of documentation ───────────────────────────────────
CHUNKS = [
    ("ops/zones",    "Rack awareness: start each node with --rack-id set to its cloud zone. Replica placement never puts two copies of a partition in the same zone, so a zone outage cannot take both copies of any key."),
    ("ops/failure",  "Disk failure handling: five consecutive I/O errors trip a per-disk breaker. Writes to the failed disk fail fast, the readiness probe degrades, and the operator replaces the disk without downtime."),
    ("dur/wal",      "Durability: the group-commit write-ahead log makes one fdatasync per 10 ms window. Every acknowledged write survives kill -9; crash recovery replays the WAL on start."),
    ("dur/pitr",     "Backups support point-in-time recovery: full plus incremental backups let you restore the database to a microsecond timestamp, not just to the last nightly snapshot."),
    ("data/ttl",     "TTL: PUTEX stores a key with an expiry; hash fields support per-field TTL via HEXPIRE. A background scanner removes expired records continuously."),
    ("vec/hnsw",     "Vector search: the built-in HNSW index answers approximate nearest-neighbour queries in logarithmic time at 0.996 recall@10. Vectors are durable and included in backups."),
    ("repl/cdc",     "Cross-region replication: repl-ship tails the CDC stream and forwards writes to a remote cluster. With --checkpoint it resumes after downtime replaying exactly the missed delta, deletes included."),
    ("sec/admin",    "Admin security: the admin HTTP API is loopback-only by default. Setting --admin-token requires a bearer token on every request, while metrics and health probes stay open."),
]

def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000
    db = Veltrix(host, port)

    # ── 2. Ingest: text as KV, embedding as vector — same key ────────────────
    for key, text in CHUNKS:
        doc_key = f"doc:{key}"
        db.put(doc_key, json.dumps({"text": text, "source": key}))
        db.vset(doc_key, embed(text))
    print(f"ingested {len(CHUNKS)} chunks (text via PUT, embedding via VSET)\n")

    # ── 3. Ask ────────────────────────────────────────────────────────────────
    question = "how does the database survive a zone outage without losing data?"
    print(f"Q: {question}\n")

    hits = db.vsearch(embed(question), k=3)

    # ── 4. Fetch the matched chunks and assemble the LLM prompt ──────────────
    context = []
    for key, score in hits:
        doc = json.loads(db.get(key))
        context.append((key, score, doc["text"]))
        print(f"  {score:0.3f}  {key}")
        print(f"         {doc['text'][:96]}...")

    prompt = (
        "Answer using ONLY the context below.\n\n"
        + "\n\n".join(f"[{k}] {t}" for k, _, t in context)
        + f"\n\nQuestion: {question}\nAnswer:"
    )
    print("\n── prompt for the LLM ────────────────────────────────────────────")
    print(prompt)

if __name__ == "__main__":
    main()
