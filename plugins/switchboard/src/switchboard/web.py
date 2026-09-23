from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import core
from .db import Database

HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Switchboard</title>
  <style>
    :root { color-scheme:dark; font-family:Inter,ui-sans-serif,system-ui,sans-serif; }
    * { box-sizing:border-box; } body { margin:0; background:#090d18; color:#eef2ff; }
    main { max-width:1240px; margin:auto; padding:32px 22px 64px; }
    header,.row { display:flex; align-items:center; justify-content:space-between; gap:16px; flex-wrap:wrap; }
    h1 { margin:0; font-size:30px; } h2 { margin:0 0 14px; font-size:19px; } h3 { margin:0 0 8px; }
    a { color:#a8c5ff; } .muted { color:#91a0bf; } .tiny { font-size:12px; }
    .nav { margin:22px 0; display:flex; flex-wrap:wrap; gap:8px; }
    .nav a,.pill { padding:7px 11px; border:1px solid #2b385e; border-radius:999px; text-decoration:none; }
    .nav a.active { background:#27437b; color:white; }
    .metrics,.lanes { display:grid; grid-template-columns:repeat(auto-fit,minmax(185px,1fr)); gap:12px; }
    .metric,.card,section,details { background:#11182a; border:1px solid #253251; border-radius:13px; }
    .metric,.card,section { padding:15px; } section { margin-top:14px; }
    .metric strong { display:block; font-size:28px; margin-top:5px; }
    .card { margin-bottom:9px; } .danger { border-color:#8e3e52; } .warning { border-color:#8c6a2b; }
    .badge { font-size:11px; text-transform:uppercase; letter-spacing:.07em; color:#aab8d7; }
    .summary { margin:8px 0; line-height:1.45; }
    table { width:100%; border-collapse:collapse; font-size:13px; }
    th,td { padding:9px 7px; border-bottom:1px solid #253251; text-align:left; vertical-align:top; }
    th { color:#91a0bf; } code { color:#c1d0ff; overflow-wrap:anywhere; }
    details { margin-top:14px; padding:12px 15px; overflow:auto; } summary { cursor:pointer; font-weight:650; }
    pre { white-space:pre-wrap; overflow-wrap:anywhere; color:#c1d0ff; }
    .empty { color:#71809e; margin:8px 0; } .good { color:#7ee2ad; }
    #run-detail:empty { display:none; }
  </style>
</head>
<body><main>
  <header><div><h1>Switchboard</h1><div class="muted">Durable work intake · read-only</div></div><div id="heartbeat" class="muted tiny"></div></header>
  <nav id="spaces" class="nav"></nav>
  <div id="metrics" class="metrics"></div>
  <section><h2>Needs attention</h2><div id="reviews"></div><div id="trouble"></div></section>
  <section><h2>Workers</h2><div id="workers"></div></section>
  <section><h2>Queue</h2><div class="lanes"><div><h3>Waiting</h3><div id="pending"></div></div><div><h3>In progress</h3><div id="running"></div></div><div><h3>Recently completed</h3><div id="completed"></div></div></div></section>
  <section id="run-detail"></section>
  <section><h2>Source health</h2><div id="health"></div></section>
  <details><summary>Technical inventory</summary>
    <h3>Bindings</h3><div id="bindings"></div><h3>Schedules</h3><div id="schedules"></div>
    <h3>Routes</h3><div id="routes"></div><h3>Recent events</h3><div id="events"></div>
  </details>
</main><script>
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmt=v=>v?new Date(v).toLocaleString():'';
const params=new URLSearchParams(location.search), selected=params.get('space'), selectedRun=params.get('run');
const scoped=rows=>selected?rows.filter(r=>r.space_id===selected||r.config?.space_id===selected):rows;
function link(url,label){return url&&/^(https?|claude|codex):/i.test(url)?`<a href="${esc(url)}">${esc(label)}</a>`:esc(label)}
function table(rows,cols){if(!rows.length)return '<p class="empty">None</p>';return '<table><thead><tr>'+cols.map(c=>`<th>${esc(c)}</th>`).join('')+'</tr></thead><tbody>'+rows.map(r=>'<tr>'+cols.map(c=>`<td><code>${esc(typeof r[c]==='object'?JSON.stringify(r[c]):r[c])}</code></td>`).join('')+'</tr>').join('')+'</tbody></table>'}
function runCard(r){return `<div class="card ${r.state==='failed'?'danger':''}"><div class="row"><span class="badge">${esc(r.processor)} · ${esc(r.state)}</span><span class="tiny muted">${fmt(r.updated_at)}</span></div><div class="summary">${esc(r.summary||r.id)}</div><a href="?${selected?'space='+encodeURIComponent(selected)+'&':''}run=${encodeURIComponent(r.id)}">Lifecycle</a></div>`}
function reviewCard(r){return `<div class="card warning"><div class="row"><span class="badge">${esc(r.space_id)} · ${r.open_run_count} item${r.open_run_count===1?'':'s'}</span><span class="tiny muted">${fmt(r.updated_at)}</span></div><div class="summary"><strong>${esc(r.title)}</strong><br>${esc(r.summary)}</div>${link(r.url,r.url?'Open decision chat':'No linked chat')}<div class="tiny muted">Resolve via CLI · <code>${esc(r.id)}</code></div></div>`}
async function get(url){const r=await fetch(url);if(!r.ok)throw new Error((await r.json()).error||r.statusText);return r.json()}
async function load(){
  const urls=['/api/status','/api/spaces','/api/sources','/api/schedules','/api/processor-bindings','/api/processor-consumers','/api/processor-deliveries','/api/processor-alerts','/api/processors?limit=100','/api/reviews?state=open','/api/routes','/api/events'];
  const [status,spaces,sources,schedules,bindings,consumers,deliveries,alerts,runs,reviews,routes,events]=await Promise.all(urls.map(get));
  document.querySelector('#spaces').innerHTML=[`<a class="${!selected?'active':''}" href="/">All spaces</a>`,...spaces.map(s=>`<a class="${selected===s.id?'active':''}" href="/?space=${encodeURIComponent(s.id)}">${esc(s.name)}</a>`)].join('');
  const sr=scoped(runs), sd=scoped(deliveries), sb=scoped(bindings), ss=scoped(schedules), sv=scoped(reviews), sa=scoped(alerts);
  const open=sr.filter(r=>['pending','running','needs-review','failed'].includes(r.state));
  document.querySelector('#metrics').innerHTML=[['Open work',open.length],['Decisions',sv.length],['Pending wakes',sd.filter(d=>d.state==='pending').length],['Open alerts',sa.filter(a=>a.state==='open').length]].map(([k,v])=>`<div class="metric"><span class="muted">${k}</span><strong>${v}</strong></div>`).join('');
  document.querySelector('#heartbeat').textContent=status.supervisor?`Supervisor ${status.supervisor.state} · ${fmt(status.supervisor.heartbeat_at)}`:'Supervisor not started';
  document.querySelector('#reviews').innerHTML=sv.map(reviewCard).join('')||'<p class="empty good">No decisions waiting.</p>';
  const failures=sr.filter(r=>r.state==='failed'), openAlerts=sa.filter(a=>a.state==='open');
  document.querySelector('#trouble').innerHTML=[...failures.map(runCard),...openAlerts.map(a=>`<div class="card danger"><strong>Delivery unreachable</strong><div>${esc(a.detail)}</div><code>${esc(a.delivery_id)}</code></div>`)].join('');
  document.querySelector('#workers').innerHTML=table(consumers.filter(c=>!selected||sb.some(b=>b.consumer===c.consumer)),['label','status','backlog','active_runs','accepted_wakes','completed_last_hour','oldest_pending_at']);
  document.querySelector('#pending').innerHTML=sr.filter(r=>r.state==='pending').slice(0,12).map(runCard).join('')||'<p class="empty">Empty</p>';
  document.querySelector('#running').innerHTML=sr.filter(r=>r.state==='running').slice(0,12).map(runCard).join('')||'<p class="empty">Empty</p>';
  document.querySelector('#completed').innerHTML=sr.filter(r=>r.state==='completed').slice(0,8).map(runCard).join('')||'<p class="empty">None yet</p>';
  document.querySelector('#health').innerHTML=table(scoped(sources).map(s=>({...s,health_state:s.health?.state,health_at:s.health?.observed_at,health_detail:s.health?.detail})),['id','kind','state','health_state','health_at','health_detail']);
  document.querySelector('#bindings').innerHTML=table(sb,['label','space_id','processor','state','consumer','lease_seconds','url']);
  document.querySelector('#schedules').innerHTML=table(ss,['id','adapter','enabled','every_seconds','next_run_at','last_state','last_error']);
  document.querySelector('#routes').innerHTML=table(scoped(routes),['priority','state','name','space_id','predicate','target']);
  document.querySelector('#events').innerHTML=table(scoped(events),['id','space_id','source_id','event_type','external_id','observed_at']);
  if(selectedRun){const r=await get('/api/processors/'+encodeURIComponent(selectedRun));const lifecycle=[{stage:'event observed',at:r.event?.observed_at,detail:r.event?.event_type},{stage:'run queued',at:r.created_at,detail:r.id},...(r.delivery?[{stage:'wake created',at:r.delivery.created_at,detail:r.delivery.state},{stage:'wake accepted',at:r.delivery.accepted_at,detail:r.delivery.consumer},{stage:'wake acknowledged',at:r.delivery.acknowledged_at,detail:''}]:[]),...r.attempts.map(a=>({stage:'worker '+a.state,at:a.started_at,detail:a.worker+' · '+(a.detail||'')})),{stage:r.state,at:r.completed_at||r.updated_at,detail:r.summary}].filter(x=>x.at);document.querySelector('#run-detail').innerHTML=`<h2>Run lifecycle</h2>${table(lifecycle,['stage','at','detail'])}<pre>${esc(JSON.stringify({facts:r.facts,decision:r.decision,actions:r.actions,error:r.error},null,2))}</pre>`}
}
load().catch(e=>document.querySelector('#trouble').innerHTML=`<div class="card danger">${esc(e.message)}</div>`);setInterval(load,5000);
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
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            try:
                if path == "/":
                    body = HTML.encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if path.startswith("/api/processors/"):
                    self.send_json(core.get_processor_run(db, path.rsplit("/", 1)[1]))
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
                    "/api/processors": lambda: core.list_processor_runs(
                        db,
                        space_id=query.get("space", [None])[0],
                        limit=int(query.get("limit", ["100"])[0]),
                    ),
                    "/api/reviews": lambda: core.list_review_groups(
                        db,
                        space_id=query.get("space", [None])[0],
                        state=query.get("state", [None])[0],
                    ),
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
            except (ValueError, TypeError) as exc:
                self.send_json({"error": str(exc)}, status=400)
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
