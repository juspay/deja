#!/usr/bin/env python3
"""Visualize a deja record/replay run: event order + how each is substituted.

Renders, per request (correlation), the ordered side-effect events and whether
REPLAY served each from the lookup table (✓ substituted, by which rank) or fell
through to live execution. Produces:
  - a compact colored terminal timeline (the pipeline log), and
  - a self-contained INTERACTIVE HTML (click any event to inspect its lookup
    key/args, recorded result, call site, and substitution status; filter to
    divergences; drill into body diffs).

Usage:
  demo/visualize-replay.py [STATE_DIR] [--full] [--serve[=PORT]] [--open]
    STATE_DIR  harness-state run dir (default: newest under demo/harness-state)
    --full     print every event in the terminal (default collapses to signal)
    --serve    after rendering, serve the dir over HTTP and print the URL
"""
import sys, os, json, glob, html as _html, http.server, socketserver, functools
from collections import defaultdict, Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRUNC = 600  # max chars of args/result embedded per event


def latest_state_dir():
    base = os.path.join(ROOT, "demo", "harness-state")
    dirs = [d for d in glob.glob(os.path.join(base, "*")) if os.path.isdir(d)]
    return max(dirs, key=os.path.getmtime) if dirs else None


def load_jsonl(path):
    out = []
    if not path or not os.path.exists(path):
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except Exception:
                    pass
    return out

def load_json(path):
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def scorecard_for_run(state, run_id):
    if not run_id or run_id == "run":
        return None
    return load_json(os.path.join(state, "runs", f"{run_id}.scorecard.json"))



def find_one(pattern):
    hits = sorted(glob.glob(pattern))
    return hits[-1] if hits else None


def trunc(v):
    s = v if isinstance(v, str) else json.dumps(v, default=str)
    return s if len(s) <= TRUNC else s[:TRUNC] + " …"


C = dict(reset="\033[0m", dim="\033[2m", bold="\033[1m", green="\033[32m",
         red="\033[31m", yellow="\033[33m", cyan="\033[36m", mag="\033[35m",
         gray="\033[90m", blue="\033[34m")


def col(s, c):
    return f"{C[c]}{s}{C['reset']}"


BICON = {"db": "🗄", "redis": "⚡", "http_outgoing": "🌐", "http_incoming": "📥",
         "time": "🕐", "id": "🆔", "id_generation": "🆔", "crypto": "🔒", "locking": "🔑"}
BCOL = {"db": "#3b82f6", "redis": "#ef4444", "http_outgoing": "#10b981", "time": "#f59e0b",
        "id": "#8b5cf6", "id_generation": "#8b5cf6", "crypto": "#ec4899",
        "locking": "#14b8a6", "http_incoming": "#64748b"}


def build(state, run_id=None):
    rec_path = find_one(os.path.join(state, "recordings", "*", "events.jsonl"))
    if run_id:
        obs_path = os.path.join(state, "observed", f"{run_id}.jsonl")
        diff_path = os.path.join(state, "http-diffs", f"{run_id}.jsonl")
        scorecard = load_json(os.path.join(state, "runs", f"{run_id}.scorecard.json"))
    else:
        obs_path = find_one(os.path.join(state, "observed", "*.jsonl"))
        diff_path = find_one(os.path.join(state, "http-diffs", "*.jsonl"))
        run_id = os.path.splitext(os.path.basename(obs_path or diff_path or "run"))[0]
        scorecard = scorecard_for_run(state, run_id)
    recording, observed, diffs = load_jsonl(rec_path), load_jsonl(obs_path), load_jsonl(diff_path)

    obs_by_src, novel = {}, []
    for o in observed:
        src = o.get("source_event_global_sequence")
        if o.get("resolved") and src is not None:
            obs_by_src[src] = o
        elif not o.get("resolved"):
            novel.append(o)

    by_corr = defaultdict(list)
    for e in recording:
        by_corr[e.get("correlation_id")].append(e)
    for cid in by_corr:
        by_corr[cid].sort(key=lambda e: e.get("global_sequence", 0))
    diff_by_corr = {d.get("correlation_id"): d for d in diffs}

    request_corrs = sorted((c for c in by_corr if c),
                           key=lambda c: min(e.get("global_sequence", 0) for e in by_corr[c]))

    total = len(recording)
    by_boundary = Counter(e.get("boundary") for e in recording)
    sub_by_boundary = Counter(o.get("boundary") for o in obs_by_src.values())
    by_rank = Counter(o.get("resolved_rank") for o in obs_by_src.values())

    requests = []
    for cid in request_corrs:
        events = by_corr[cid]
        d = diff_by_corr.get(cid)
        sc = d.get("status_candidate") if d else None
        sb = d.get("status_baseline") if d else None
        ndiff = len(d.get("body_diff", [])) if d else 0
        evs = []
        for e in events:
            if e.get("boundary") == "http_incoming":
                continue
            gs = e.get("global_sequence")
            o = obs_by_src.get(gs)
            evs.append(dict(
                gs=gs, rs=e.get("request_sequence"), b=e.get("boundary", "?"),
                m=e.get("method_name", "?"), t=e.get("trait_name", ""),
                loc=f"{os.path.basename(e.get('call_file',''))}:{e.get('call_line','')}",
                args=trunc(e.get("args")), result=trunc(e.get("result")),
                sub=bool(o), rank=(o.get("resolved_rank") if o else None),
                err=bool(e.get("is_error"))))
        requests.append(dict(
            cid=cid, path=(d.get("request_path") if d else "?"),
            sc=sc, sb=sb, ndiff=ndiff, matched=(bool(d) and sc == sb and ndiff == 0),
            diffs=[dict(p=bd.get("json_path"), base=trunc(bd.get("baseline")), cand=trunc(bd.get("candidate")))
                   for bd in (d.get("body_diff", []) if d else [])][:25],
            events=evs))

    verdict = scorecard.get("verdict", {}) if isinstance(scorecard, dict) else {}
    if isinstance(verdict.get("pass"), bool):
        overall_pass = verdict["pass"]
        overall_reason = verdict.get("reason")
    else:
        overall_pass = all(r["matched"] for r in requests) if requests else False
        overall_reason = None
    return dict(run=run_id, total=total, substituted=len(obs_by_src), novel=len(novel),
                by_boundary={b: [sub_by_boundary[b], n] for b, n in by_boundary.items()},
                by_rank={str(r): c for r, c in by_rank.items()},
                requests=requests, overall_pass=overall_pass, overall_reason=overall_reason)


def terminal(data, full):
    print()
    print(col("═" * 80, "cyan"))
    if data["overall_pass"]:
        suffix = f" — {data['overall_reason']}" if data.get("overall_reason") else " — byte-exact self-replay"
        verdict = col("✓ PASS" + suffix, "green")
    else:
        verdict = col("✗ divergences remain", "yellow")
    print(col("  DEJA RECORD → REPLAY  ", "bold") + col(f"· {data['run']}  ", "dim") + verdict)
    print(col("═" * 80, "cyan"))
    print(f"  recorded events {col(data['total'],'bold')}   "
          f"substituted {col(data['substituted'],'green')}   "
          f"replay-only(novel) {col(data['novel'],'red' if data['novel'] else 'dim')}   "
          f"requests {col(len(data['requests']),'bold')}")
    print("  " + "  ".join(
        f"{BICON.get(b,'•')}{b} {col(f'{s}/{n}','green' if s==n else 'yellow')}"
        for b, (s, n) in sorted(data["by_boundary"].items(), key=lambda x: -x[1][1])))
    print()
    for r in data["requests"]:
        sub_n = sum(1 for e in r["events"] if e["sub"])
        if r["sc"] is None:
            badge = col("(not driven)", "dim")
        elif r["matched"]:
            badge = col(f"✓ {r['sc']} MATCH", "green")
        else:
            badge = col(f"✗ {r['sc']} vs {r['sb']}, {r['ndiff']} diff(s)", "red")
        ratio = col(f"{sub_n}/{len(r['events'])} subst", "green" if sub_n == len(r["events"]) else "yellow")
        print(f"{col('▶','blue')} {col(r['path'],'bold')}  {badge}  {ratio}")
        if not full:
            fd = next((e for e in r["events"] if not e["sub"]), None)
            if fd is None and r["events"]:
                print("    " + col("all side-effects served from the lookup table ✓", "green"))
            elif fd:
                note = col("(live but deterministic — still byte-exact)", "dim") if r["matched"] \
                    else col("← likely divergence source", "red")
                print(f"    {col('first live call','dim')} {col('#'+str(fd['gs']),'yellow')} "
                      f"{BICON.get(fd['b'],'•')}{col(fd['b'],'cyan')} {fd['m'][:36]} {note}")
        else:
            for e in r["events"]:
                tag = col(f"✓ rank{e['rank']}", "green") if e["sub"] else col("✗ live", "yellow")
                gs = str(e["gs"]).rjust(4)
                icon = BICON.get(e["b"], "•")
                print(f"    {col(gs,'dim')} {icon} {col(e['b'],'cyan'):<9} {e['m'][:44]:<44} {tag}")
        print()


HTML_TMPL = r"""<!doctype html><html><head><meta charset=utf-8>
<title>deja replay · __RUN__</title><style>
*{box-sizing:border-box} body{background:#0b0f17;color:#e5e7eb;font:13px/1.5 ui-monospace,Menlo,monospace;margin:0;padding:24px}
h1{font-size:19px;margin:0} .sub{color:#64748b;margin:2px 0 16px}
.verdict{display:inline-block;padding:4px 12px;border-radius:8px;font-weight:700;font-size:14px}
.verdict.ok{background:#064e3b;color:#6ee7b7} .verdict.bad{background:#78350f;color:#fcd34d}
.bar{display:flex;flex-wrap:wrap;gap:18px;color:#94a3b8;margin:10px 0} .bar b{color:#e5e7eb}
.legend{display:flex;flex-wrap:wrap;gap:6px;margin:8px 0 16px}
.tag{display:inline-block;padding:2px 8px;border-radius:6px;color:#fff;font-size:11px}
.controls{margin:8px 0 14px} button{background:#1e293b;color:#cbd5e1;border:1px solid #334155;border-radius:6px;padding:5px 11px;cursor:pointer;font:inherit;margin-right:6px}
button.on{background:#2563eb;color:#fff;border-color:#2563eb}
.req{border:1px solid #1e293b;border-radius:10px;margin:8px 0;background:#0f172a;overflow:hidden}
.rhead{display:flex;align-items:center;gap:10px;padding:10px 12px;cursor:pointer;user-select:none}
.rhead:hover{background:#0b1220} .rhead code{color:#cbd5e1;font-size:13px}
.cid{color:#475569;font-size:11px;margin-left:auto}
.badge{padding:2px 9px;border-radius:6px;font-size:12px;font-weight:700}
.badge.ok{background:#064e3b;color:#6ee7b7} .badge.bad{background:#7f1d1d;color:#fca5a5} .badge.dim{background:#1e293b;color:#64748b}
.lane{display:flex;flex-wrap:wrap;gap:3px;padding:0 12px 10px}
.chip{padding:2px 7px;border-radius:6px;color:#fff;font-size:11px;cursor:pointer;border:1px solid transparent}
.chip.live{opacity:.34;border-style:dashed;border-color:#475569}
.chip:hover{outline:2px solid #fff6} .chip sub{font-size:8px;opacity:.85;margin-left:3px}
.body{display:none;border-top:1px solid #1e293b;padding:10px 12px;background:#0b1220}
.body.open{display:block} .ev{display:none}
table{border-collapse:collapse;width:100%;margin:4px 0} td,th{text-align:left;padding:4px 8px;border-bottom:1px solid #1e293b;vertical-align:top}
th{color:#64748b;font-weight:600} .k{color:#7dd3fc} .val{color:#cbd5e1;white-space:pre-wrap;word-break:break-all;max-width:760px}
.evrow{cursor:pointer} .evrow:hover{background:#0f1b2e}
.detail{display:none;background:#070b12;border-left:3px solid #334155} .detail.open{display:table-row}
.diff{margin-top:10px} .diff h4{color:#fca5a5;margin:6px 0} .b-base{color:#6ee7b7} .b-cand{color:#fca5a5}
.pill{font-size:10px;padding:1px 5px;border-radius:4px;background:#1e293b;color:#94a3b8;margin-left:5px}
.subok{color:#6ee7b7} .sublive{color:#fcd34d}
</style></head><body>
<h1>deja · record → replay</h1>
<div class=sub>run __RUN__ · same candidate router image · <b>click a request</b> to expand, <b>click an event</b> to inspect its lookup key &amp; recorded value</div>
<div id=top></div>
<div class=controls>
 <button id=fAll class=on onclick="setFilter('all')">all requests</button>
 <button id=fDiv onclick="setFilter('div')">divergences only</button>
 <button onclick="toggleAll(true)">expand all</button>
 <button onclick="toggleAll(false)">collapse all</button>
</div>
<div id=reqs></div>
<script>
const D = __DATA__;
const BCOL = __BCOL__, BICON = __BICON__;
function esc(s){return (s==null?'':String(s)).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}
function top(){
 const vc = D.overall_pass?'ok':'bad', vt = D.overall_pass?('✓ PASS'+(D.overall_reason?' — '+D.overall_reason:' — byte-exact self-replay')):'✗ divergences remain';
 let leg = Object.entries(D.by_boundary).sort((a,b)=>b[1][1]-a[1][1]).map(([b,[s,n]])=>
   `<span class=tag style="background:${BCOL[b]||'#999'}">${BICON[b]||''} ${b} ${s}/${n}</span>`).join('');
 document.getElementById('top').innerHTML =
   `<span class="verdict ${vc}">${vt}</span>`+
   `<div class=bar><span>recorded <b>${D.total}</b></span><span>substituted <b style="color:#6ee7b7">${D.substituted}</b></span>`+
   `<span>replay-only <b style="color:#fca5a5">${D.novel}</b></span><span>requests <b>${D.requests.length}</b></span>`+
   `<span>ranks ${Object.entries(D.by_rank).map(([r,c])=>'rank'+r+'='+c).join(' ')}</span></div>`+
   `<div class=legend>${leg}</div>`;
}
function reqCard(r,i){
 const badge = r.sc==null?'<span class="badge dim">—</span>':
   r.matched?`<span class="badge ok">✓ ${r.sc}</span>`:`<span class="badge bad">✗ ${r.sc} vs ${r.sb} · ${r.ndiff} diff</span>`;
 const subN = r.events.filter(e=>e.sub).length;
 const chips = r.events.map((e,j)=>`<span class="chip ${e.sub?'':'live'}" style="background:${BCOL[e.b]||'#999'}"
    onclick="event.stopPropagation();showEv(${i},${j})" title="#${e.gs} ${e.b}::${e.m}">${esc(e.b.slice(0,3))}<sub>${e.sub?'r'+e.rank:'live'}</sub></span>`).join('');
 const rows = r.events.map((e,j)=>`
   <tr class=evrow id="er_${i}_${j}" onclick="showEv(${i},${j})">
     <td class=k>#${e.gs}</td><td><span class=tag style="background:${BCOL[e.b]||'#999'}">${BICON[e.b]||''} ${e.b}</span></td>
     <td>${esc(e.m)}<span class=pill>${esc(e.loc)}</span></td>
     <td>${e.sub?`<span class=subok>✓ substituted · rank ${e.rank}</span>`:'<span class=sublive>✗ executed live</span>'}</td></tr>
   <tr class=detail id="det_${i}_${j}"><td colspan=4>
     <table><tr><th>lookup key (args)</th><td class=val>${esc(e.args)}</td></tr>
     <tr><th>recorded result ${e.sub?'(served on replay)':'(not used)'}</th><td class=val>${esc(e.result)}</td></tr>
     <tr><th>trait</th><td class=val>${esc(e.t)} · req_seq ${e.rs}${e.err?' · <span style=color:#fca5a5>is_error</span>':''}</td></tr></table>
   </td></tr>`).join('');
 const diffs = r.diffs.length?`<div class=diff><h4>${r.diffs.length} body diff(s)</h4><table>
   <tr><th>path</th><th>baseline (recorded)</th><th>candidate (replay)</th></tr>`+
   r.diffs.map(x=>`<tr><td class=k>${esc(x.p)}</td><td class="val b-base">${esc(x.base)}</td><td class="val b-cand">${esc(x.cand)}</td></tr>`).join('')+`</table></div>`:'';
 return `<div class="req" data-div="${r.sc!=null && !r.matched?1:0}">
   <div class=rhead onclick="this.parentNode.querySelector('.body').classList.toggle('open')">
     ${badge}<code>${esc(r.path)}</code><span class=pill>${subN}/${r.events.length} substituted</span><span class=cid>${esc(r.cid)}</span></div>
   <div class=lane>${chips}</div>
   <div class=body><table>${rows}</table>${diffs}</div></div>`;
}
function render(){document.getElementById('reqs').innerHTML=D.requests.map(reqCard).join('')}
function showEv(i,j){const d=document.getElementById(`det_${i}_${j}`);
 d.classList.toggle('open'); d.previousElementSibling.scrollIntoView({block:'nearest'});
 // ensure the request body is open
 d.closest('.body').classList.add('open');}
function setFilter(m){document.getElementById('fAll').classList.toggle('on',m=='all');
 document.getElementById('fDiv').classList.toggle('on',m=='div');
 document.querySelectorAll('.req').forEach(r=>r.style.display=(m=='all'||r.dataset.div=='1')?'':'none');}
function toggleAll(open){document.querySelectorAll('.body').forEach(b=>b.classList.toggle('open',open));}
top();render();
</script></body></html>"""


def write_html(state, data, run_id=None):
    filename = f"replay-visualization-{run_id}.html" if run_id else "replay-visualization.html"
    path = os.path.join(state, filename)
    doc = (HTML_TMPL
           .replace("__RUN__", _html.escape(data["run"]))
           .replace("__DATA__", json.dumps(data))
           .replace("__BCOL__", json.dumps(BCOL))
           .replace("__BICON__", json.dumps(BICON)))
    with open(path, "w") as f:
        f.write(doc)
    return path


def serve(state, html_path, port):
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=state)
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("0.0.0.0", port), handler) as httpd:
        url = f"http://localhost:{port}/{os.path.basename(html_path)}"
        print(col(f"\n  ◆ interactive timeline live → {url}", "mag"))
        print(col("    (open the URL in your browser; Ctrl-C here to stop)\n", "dim"))
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print(col("  visualizer server stopped.", "dim"))


def parse_args(argv):
    state, run_id = None, None
    full = "--full" in argv
    serve_flag = any(a == "--serve" or a.startswith("--serve=") for a in argv)
    port = 8099
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--run":
            i += 1
            if i >= len(argv):
                print("--run requires a run id", file=sys.stderr)
                sys.exit(2)
            run_id = argv[i]
        elif a.startswith("--run="):
            run_id = a.split("=", 1)[1]
        elif a.startswith("--serve="):
            try:
                port = int(a.split("=", 1)[1])
            except ValueError:
                pass
        elif not a.startswith("-") and state is None:
            state = a
        i += 1
    return state, run_id, full, serve_flag, port


def main():
    state, run_id, full, serve_flag, port = parse_args(sys.argv[1:])
    state = state if state else latest_state_dir()
    if not state or not os.path.isdir(state):
        print("no harness-state run dir found", file=sys.stderr)
        sys.exit(1)

    data = build(state, run_id)
    terminal(data, full)
    html_path = write_html(state, data, run_id)
    print(col(f"  HTML timeline → {html_path}", "mag"))
    if serve_flag:
        serve(state, html_path, port)


if __name__ == "__main__":
    main()
