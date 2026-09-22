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
    td.wrap { white-space:normal; min-width:220px; }
    th { color:#9aa6c6; font-weight:600; } code { color:#b9c8ff; }
  </style>
</head>
<body><main>
  <header><div><h1>Switchboard</h1><div class="muted">Read-only operations view</div></div><div id="db" class="muted"></div></header>
  <div id="counts" class="grid"></div>
  <section><h2>Supervisor</h2><div id="supervisor"></div></section>
  <section><h2>Spaces</h2><div id="space-nav"></div><div id="spaces"></div></section>
  <section><h2>Sources</h2><div id="sources"></div></section>
  <section><h2>Schedules</h2><div id="schedules"></div></section>
  <section><h2>Open deliveries</h2><div id="deliveries"></div></section>
  <section><h2>Processor bindings</h2><div id="bindings"></div></section>
  <section><h2>Processor consumers</h2><div id="consumers"></div></section>
  <section><h2>Processor deliveries</h2><div id="processor-deliveries"></div></section>
  <section><h2>Processor alerts</h2><div id="processor-alerts"></div></section>
  <section><h2>Processor queue and outcomes</h2><div id="processors"></div></section>
  <section><h2>Routing table</h2><div id="routes"></div></section>
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
function richTable(rows, cols, wrap=[]) {
  if (!rows.length) return '<p class="muted">None</p>';
  return '<table><thead><tr>'+cols.map(c=>'<th>'+esc(c)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(r=>'<tr>'+cols.map(c=>'<td class="'+(wrap.includes(c)?'wrap':'')+'"><code>'+esc(typeof r[c]==='object'?JSON.stringify(r[c]):r[c])+'</code></td>').join('')+'</tr>').join('')+'</tbody></table>';
}
async function load() {
  const [status, spaces, sources, schedules, deliveries, bindings, consumers, processorDeliveries, processorAlerts, processors, routes, waits, adapters, events] = await Promise.all(['/api/status','/api/spaces','/api/sources','/api/schedules','/api/deliveries','/api/processor-bindings','/api/processor-consumers','/api/processor-deliveries','/api/processor-alerts','/api/processors','/api/routes','/api/waits','/api/adapters','/api/events'].map(u=>fetch(u).then(r=>r.json())));
  document.querySelector('#db').textContent = status.database;
  const selectedSpace = new URLSearchParams(location.search).get('space');
  const scoped = rows => selectedSpace ? rows.filter(row => row.space_id === selectedSpace || row.config?.space_id === selectedSpace) : rows;
  document.querySelector('#space-nav').innerHTML = ['<a href="/">All spaces</a>', ...spaces.map(space => `<a href="/?space=${encodeURIComponent(space.id)}">${esc(space.name)}</a>`)].join(' &middot; ');
  document.querySelector('#counts').innerHTML = Object.entries(status.counts).map(([k,v])=>`<div class="card"><div class="muted">${esc(k.replaceAll('_',' '))}</div><div class="value">${v}</div></div>`).join('');
  document.querySelector('#supervisor').innerHTML = status.supervisor ? table([status.supervisor], ['state','pid','heartbeat_at','last_cycle_at','dispatch_enabled','web_url','last_error']) : '<p class="muted">Never started</p>';
  document.querySelector('#spaces').innerHTML = table(spaces, ['id','name','created_at']);
  document.querySelector('#sources').innerHTML = richTable(scoped(sources), ['id','space_id','kind','state','config','created_at'], ['config']);
  document.querySelector('#schedules').innerHTML = table(scoped(schedules), ['id','adapter','enabled','every_seconds','next_run_at','last_state','last_error']);
  document.querySelector('#deliveries').innerHTML = table(deliveries.filter(x=>['pending','accepted'].includes(x.state)), ['id','state','consumer','event_id','created_at']);
  document.querySelector('#bindings').innerHTML = table(scoped(bindings), ['state','space_id','processor','consumer','lease_seconds','activate_inactive','updated_at']);
  document.querySelector('#consumers').innerHTML = table(consumers, ['consumer','status','backlog','oldest_pending_at','active_runs','accepted_wakes','completed_last_hour','completed_last_day']);
  document.querySelector('#processor-deliveries').innerHTML = table(scoped(processorDeliveries).filter(x=>['pending','accepted'].includes(x.state)), ['id','state','space_id','processor','consumer','processor_run_id','generation','created_at','last_error']);
  document.querySelector('#processor-alerts').innerHTML = table(processorAlerts.filter(x=>x.state==='open'), ['state','delivery_id','generation','detail','opened_at']);
  document.querySelector('#processors').innerHTML = richTable(scoped(processors), ['id','space_id','state','processor','summary','facts','decision','actions','updated_at'], ['summary','facts','decision','actions']);
  document.querySelector('#routes').innerHTML = richTable(scoped(routes), ['priority','state','name','space_id','predicate','target'], ['predicate','target']);
  document.querySelector('#waits').innerHTML = table(waits.filter(x=>x.state==='active'), ['id','consumer','purpose','predicate','created_at']);
  document.querySelector('#adapters').innerHTML = table(adapters, ['id','adapter','state','discovered_sources','emitted_events','deduplicated_events','started_at']);
  document.querySelector('#events').innerHTML = table(scoped(events), ['id','space_id','source_id','event_type','external_id','observed_at']);
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
                    "/api/processor-bindings": lambda: core.list_processor_bindings(db),
                    "/api/processor-consumers": lambda: core.list_processor_consumers(db),
                    "/api/processor-deliveries": lambda: core.list_processor_deliveries(db),
                    "/api/processor-alerts": lambda: core.list_processor_alerts(db),
                    "/api/processors": lambda: core.list_processor_runs(db),
                    "/api/routes": lambda: core.list_routes(db),
                    "/api/sources": lambda: core.list_sources(db),
                    "/api/spaces": lambda: core.list_spaces(db),
                    "/api/adapters": lambda: core.list_adapter_runs(db),
                    "/api/schedules": lambda: core.list_schedules(db),
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
