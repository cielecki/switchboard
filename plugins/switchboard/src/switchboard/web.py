from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from . import core
from .db import Database

HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Switchboard</title>
  <style>
    :root { color-scheme: dark; font-family: ui-sans-serif, system-ui, sans-serif; }
    body { margin: 0; background: #0b1020; color: #eef2ff; }
    main { max-width: 1180px; margin: 0 auto; padding: 40px 24px; }
    header { display:flex; align-items:end; justify-content:space-between; gap:20px; }
    h1 { margin:0; font-size:32px; } .muted { color:#9aa6c6; }
    .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; margin:28px 0; }
    .card, section { background:#131a2f; border:1px solid #263153; border-radius:14px; padding:16px; }
    .value { font-size:28px; font-weight:700; margin-top:8px; }
    section { margin-top:16px; overflow:auto; }
    table { width:100%; border-collapse:collapse; font-size:14px; }
    th,td { text-align:left; padding:10px 8px; border-bottom:1px solid #263153; white-space:nowrap; }
    th { color:#9aa6c6; font-weight:600; } code { color:#b9c8ff; }
  </style>
</head>
<body><main>
  <header><div><h1>Switchboard</h1><div class="muted">Read-only operations view</div></div><div id="db" class="muted"></div></header>
  <div id="counts" class="grid"></div>
  <section><h2>Open deliveries</h2><div id="deliveries"></div></section>
  <section><h2>Active waits</h2><div id="waits"></div></section>
  <section><h2>Adapter runs</h2><div id="adapters"></div></section>
  <section><h2>Recent events</h2><div id="events"></div></section>
</main><script>
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function table(rows, cols) {
  if (!rows.length) return '<p class="muted">None</p>';
  return '<table><thead><tr>'+cols.map(c=>'<th>'+esc(c)+'</th>').join('')+'</tr></thead><tbody>'+
    rows.map(r=>'<tr>'+cols.map(c=>'<td><code>'+esc(typeof r[c]==='object'?JSON.stringify(r[c]):r[c])+'</code></td>').join('')+'</tr>').join('')+'</tbody></table>';
}
async function load() {
  const [status, deliveries, waits, adapters, events] = await Promise.all(['/api/status','/api/deliveries','/api/waits','/api/adapters','/api/events'].map(u=>fetch(u).then(r=>r.json())));
  document.querySelector('#db').textContent = status.database;
  document.querySelector('#counts').innerHTML = Object.entries(status.counts).map(([k,v])=>`<div class="card"><div class="muted">${esc(k.replaceAll('_',' '))}</div><div class="value">${v}</div></div>`).join('');
  document.querySelector('#deliveries').innerHTML = table(deliveries.filter(x=>['pending','accepted'].includes(x.state)), ['id','state','consumer','event_id','created_at']);
  document.querySelector('#waits').innerHTML = table(waits.filter(x=>x.state==='active'), ['id','consumer','purpose','predicate','created_at']);
  document.querySelector('#adapters').innerHTML = table(adapters, ['id','adapter','state','discovered_sources','emitted_events','deduplicated_events','started_at']);
  document.querySelector('#events').innerHTML = table(events, ['id','source_id','event_type','external_id','observed_at']);
}
load(); setInterval(load, 5000);
</script></body></html>"""


def handler_for(db: Database) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def send_json(self, value: object, status: int = 200) -> None:
            body = json.dumps(value, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            path = urlparse(self.path).path
            try:
                if path == "/":
                    body = HTML.encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                endpoints = {
                    "/api/status": lambda: core.status(db),
                    "/api/events": lambda: core.list_events(db),
                    "/api/waits": lambda: core.list_waits(db),
                    "/api/deliveries": lambda: core.list_deliveries(db),
                    "/api/sources": lambda: core.list_sources(db),
                    "/api/spaces": lambda: core.list_spaces(db),
                    "/api/adapters": lambda: core.list_adapter_runs(db),
                }
                if path in endpoints:
                    self.send_json(endpoints[path]())
                    return
                self.send_json({"error": "not found"}, status=404)
            except Exception as exc:  # noqa: BLE001  # pragma: no cover - final HTTP boundary
                self.send_json({"error": str(exc)}, status=500)

        def do_POST(self) -> None:
            self.send_json({"error": "web interface is read-only; use the switchboard CLI"}, status=405)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def serve(db: Database, host: str = "127.0.0.1", port: int = 8765) -> None:
    db.initialize()
    server = ThreadingHTTPServer((host, port), handler_for(db))
    print(f"Switchboard read-only UI: http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
