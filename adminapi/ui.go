package adminapi

// ui.go — single-file static admin dashboard served from /admin/ui.
//
// Zero build dependencies: pure HTML + vanilla JS that calls /admin/stats,
// /admin/quotas, etc. Operators get a real dashboard without bundling React.
// For richer UX, replace this with a frontend build pipeline pointing at the
// same admin endpoints.

import (
	"net/http"
)

const adminHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>VeltrixDB Admin</title>
<style>
  body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;margin:0;background:#0e1116;color:#e6edf3}
  header{background:#161b22;padding:14px 24px;border-bottom:1px solid #30363d;display:flex;justify-content:space-between;align-items:center}
  h1{margin:0;font-size:18px;font-weight:500}
  .container{padding:24px;max-width:1280px;margin:0 auto}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px;margin-bottom:24px}
  .card{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:16px}
  .card h2{margin:0 0 12px;font-size:13px;font-weight:500;color:#8b949e;text-transform:uppercase;letter-spacing:.05em}
  .metric{font-size:28px;font-weight:600;color:#58a6ff}
  .label{font-size:12px;color:#8b949e;margin-top:4px}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{text-align:left;padding:8px 12px;border-bottom:1px solid #30363d}
  th{color:#8b949e;font-weight:500;text-transform:uppercase;letter-spacing:.05em;font-size:11px}
  tr:hover{background:#1c2129}
  .btn{background:#238636;color:white;border:none;padding:8px 14px;border-radius:6px;cursor:pointer;font-size:13px;margin-right:8px}
  .btn:hover{background:#2ea043}
  .btn-warn{background:#9e6a03}
  .btn-warn:hover{background:#bb8009}
  pre{background:#0d1117;border:1px solid #30363d;padding:12px;border-radius:6px;overflow-x:auto;font-size:12px;color:#c9d1d9;max-height:320px}
  .stream{height:240px;overflow-y:scroll;font-family:monospace;font-size:11px;background:#0d1117;border:1px solid #30363d;padding:8px;border-radius:6px}
  .stream div{margin-bottom:2px;border-bottom:1px solid #21262d;padding-bottom:2px}
  .ok{color:#3fb950}.err{color:#f85149}
  input{background:#0d1117;border:1px solid #30363d;color:#e6edf3;padding:6px 10px;border-radius:6px;font-size:13px;margin-right:8px}
  .row{margin-bottom:12px}
</style>
</head>
<body>
<header>
  <h1>VeltrixDB Admin</h1>
  <div id="version" style="font-size:12px;color:#8b949e"></div>
</header>
<div class="container">

  <div class="grid">
    <div class="card"><h2>Index keys</h2><div class="metric" id="m-keys">—</div></div>
    <div class="card"><h2>Writes</h2><div class="metric" id="m-writes">—</div></div>
    <div class="card"><h2>Reads</h2><div class="metric" id="m-reads">—</div></div>
    <div class="card"><h2>Atomic ops</h2><div class="metric" id="m-atomic">—</div></div>
    <div class="card"><h2>Cache hit rate</h2><div class="metric" id="m-cache">—</div></div>
    <div class="card"><h2>CDC subscribers</h2><div class="metric" id="m-cdc">—</div></div>
  </div>

  <div class="card" style="margin-bottom:16px">
    <h2>Per-disk VLog</h2>
    <table id="vlog-table"><thead><tr>
      <th>Disk</th><th>File bytes</th><th>Live</th><th>Garbage %</th>
      <th>Write EWMA</th><th>Read EWMA</th><th>Slow</th>
    </tr></thead><tbody></tbody></table>
  </div>

  <div class="card" style="margin-bottom:16px">
    <h2>Quotas</h2>
    <div class="row">
      <input id="q-ns" placeholder="namespace">
      <input id="q-wps" placeholder="writes/sec" type="number" style="width:120px">
      <input id="q-max" placeholder="max keys" type="number" style="width:120px">
      <button class="btn" onclick="setQuota()">Set quota</button>
    </div>
    <table id="quota-table"><thead><tr>
      <th>Namespace</th><th>writes/sec</th><th>burst</th><th>max keys</th><th>live keys</th><th>tokens left</th>
    </tr></thead><tbody></tbody></table>
  </div>

  <div class="card" style="margin-bottom:16px">
    <h2>Operations</h2>
    <button class="btn" onclick="runOp('checkpoint','POST')">Force checkpoint</button>
    <button class="btn-warn btn" onclick="runOp('migrate','POST')">Run schema migrations</button>
    <pre id="op-result"></pre>
  </div>

  <div class="card" style="margin-bottom:16px">
    <h2>CDC live tail (prefix filter optional)</h2>
    <div class="row">
      <input id="cdc-prefix" placeholder="key prefix">
      <button class="btn" onclick="startCDC()">Start</button>
      <button class="btn-warn btn" onclick="stopCDC()">Stop</button>
    </div>
    <div class="stream" id="cdc-stream"></div>
  </div>

  <div class="card">
    <h2>Recent traces (slow ops + sampled)</h2>
    <button class="btn" onclick="loadTraces()">Refresh</button>
    <pre id="traces"></pre>
  </div>
</div>

<script>
async function refresh() {
  const v = await fetch('/admin/version').then(r => r.json());
  document.getElementById('version').textContent = 'schema v' + v.current_schema_version + (v.encryption_enabled ? ' · encrypted at rest' : '');

  const s = await fetch('/admin/stats').then(r => r.json());
  document.getElementById('m-keys').textContent = s.index_keys.toLocaleString();
  document.getElementById('m-writes').textContent = s.writes_total.toLocaleString();
  document.getElementById('m-reads').textContent = s.reads_total.toLocaleString();
  document.getElementById('m-atomic').textContent = s.atomic_ops_total.toLocaleString();
  document.getElementById('m-cache').textContent = (s.cache.HitRate * 100).toFixed(1) + '%';
  document.getElementById('m-cdc').textContent = s.cdc_subscribers;

  const tbody = document.querySelector('#vlog-table tbody');
  tbody.innerHTML = '';
  for (const d of s.vlogs) {
    tbody.innerHTML += '<tr>' +
      '<td>' + d.DiskIdx + '</td>' +
      '<td>' + (d.FileBytes/1024/1024).toFixed(1) + ' MB</td>' +
      '<td>' + (d.LiveBytes/1024/1024).toFixed(1) + ' MB</td>' +
      '<td>' + (d.GarbageRatio*100).toFixed(1) + '%</td>' +
      '<td>' + (d.WriteLatencyEWMAs*1000).toFixed(2) + ' ms</td>' +
      '<td>' + (d.ReadLatencyEWMAs*1000).toFixed(2) + ' ms</td>' +
      '<td>' + (d.Slow ? '<span class="err">⚠ slow</span>' : '<span class="ok">ok</span>') + '</td>' +
      '</tr>';
  }

  const q = await fetch('/admin/quotas').then(r => r.json());
  const qbody = document.querySelector('#quota-table tbody');
  qbody.innerHTML = '';
  for (const x of (q || [])) {
    qbody.innerHTML += '<tr><td>' + x.Namespace + '</td>' +
      '<td>' + x.WritesPerSec + '</td>' +
      '<td>' + x.BurstWrites + '</td>' +
      '<td>' + x.MaxKeys + '</td>' +
      '<td>' + x.KeyCount + '</td>' +
      '<td>' + x.TokensLeft.toFixed(2) + '</td></tr>';
  }
}

async function setQuota() {
  const ns = document.getElementById('q-ns').value;
  const wps = document.getElementById('q-wps').value || 0;
  const max = document.getElementById('q-max').value || 0;
  const body = 'ns=' + encodeURIComponent(ns) + '&writes_per_sec=' + wps + '&max_keys=' + max;
  await fetch('/admin/quotas', {method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'}, body});
  refresh();
}

async function runOp(name, method) {
  const r = await fetch('/admin/' + name, {method});
  const txt = await r.text();
  document.getElementById('op-result').textContent = txt;
  refresh();
}

let cdcCtl = null;
async function startCDC() {
  stopCDC();
  const pref = document.getElementById('cdc-prefix').value;
  cdcCtl = new AbortController();
  const stream = document.getElementById('cdc-stream');
  stream.innerHTML = '';
  try {
    const resp = await fetch('/admin/cdc?prefix=' + encodeURIComponent(pref), {signal: cdcCtl.signal});
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    for (;;) {
      const {value, done} = await reader.read();
      if (done) break;
      buf += decoder.decode(value, {stream:true});
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl); buf = buf.slice(nl+1);
        if (line) {
          const div = document.createElement('div');
          div.textContent = line;
          stream.appendChild(div);
          stream.scrollTop = stream.scrollHeight;
        }
      }
    }
  } catch (e) { /* aborted */ }
}
function stopCDC() { if (cdcCtl) { cdcCtl.abort(); cdcCtl = null; } }

async function loadTraces() {
  const r = await fetch('/traces?limit=20');
  document.getElementById('traces').textContent = await r.text();
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>`

func handleUI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", 405)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write([]byte(adminHTML))
}
