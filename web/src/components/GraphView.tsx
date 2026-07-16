import React from "react";
import { useQuery } from "@tanstack/react-query";
import { api, GraphNode } from "../lib/api";
import { dropHealthTrees, dropInternalSpans, keepAll, unwrapIngress } from "../lib/graphalign";

type TreeNode = GraphNode & { children: TreeNode[] };

function buildForest(nodes: GraphNode[]): TreeNode[] {
  const map = new Map<number, TreeNode>();
  nodes.forEach((n) => map.set(n.node_id, { ...n, children: [] }));
  const roots: TreeNode[] = [];
  for (const n of map.values()) {
    const parent = n.parent_id != null ? map.get(n.parent_id) : undefined;
    if (parent) parent.children.push(n);
    else roots.push(n);
  }
  const bySeq = (a: TreeNode, b: TreeNode) => a.sequence - b.sequence;
  roots.sort(bySeq);
  for (const n of map.values()) n.children.sort(bySeq);
  return roots;
}

// node_id is per-process (record 514 vs replay 427 for the same span), so we
// merge the two trees by SPAN-PATH, not id: a unified node carries the record
// node and/or the replay node that share a root→leaf span-name chain.
type Uni = { name: string; path: string; rec?: TreeNode; rep?: TreeNode; children: Uni[] };

// Merge key: span name + http.route when present. Request roots are all
// named "HTTP request", so bare-name grouping zips DIFFERENT endpoints
// together by index (a /health probe on one side shifts every pairing after
// it). The route pins each root to its endpoint; within one route, index
// order matches because the agent drives requests in recorded order.
// (request_id is deliberately NOT used: the replay router mints fresh ids.)
function mergeKey(n: TreeNode): string {
  const route = n.fields?.["http.route"];
  return typeof route === "string" ? `${n.span_name}|${route}` : n.span_name;
}

function mergeLevel(rec: TreeNode[], rep: TreeNode[], parentPath: string): Uni[] {
  const group = (ns: TreeNode[]) => {
    const m = new Map<string, TreeNode[]>();
    for (const n of ns) {
      const k = mergeKey(n);
      (m.get(k) ?? m.set(k, []).get(k)!).push(n);
    }
    return m;
  };
  const recBy = group(rec), repBy = group(rep);
  const keys: string[] = [];
  for (const n of [...rec, ...rep]) {
    const k = mergeKey(n);
    if (!keys.includes(k)) keys.push(k);
  }
  const out: Uni[] = [];
  for (const key of keys) {
    const rs = recBy.get(key) ?? [], ps = repBy.get(key) ?? [];
    const name = (rs[0] ?? ps[0])!.span_name;
    for (let i = 0; i < Math.max(rs.length, ps.length); i++) {
      const r = rs[i], p = ps[i];
      // Suffix repeated same-key siblings so paths (React keys + collapse
      // ids) stay unique per occurrence.
      const base = parentPath ? `${parentPath}>${key}` : key;
      const path = i > 0 ? `${base}#${i}` : base;
      out.push({ name, path, rec: r, rep: p, children: mergeLevel(r?.children ?? [], p?.children ?? [], path) });
    }
  }
  return out;
}

function ms(n?: TreeNode): number {
  if (!n || n.closed_ns == null) return 0;
  return (n.closed_ns - n.started_ns) / 1e6;
}
function reqLabel(n?: TreeNode): string | null {
  const rid = n?.fields?.["request_id"] ?? n?.fields?.["http.route"];
  return typeof rid === "string" ? rid : null;
}
function bump(m: Map<number, Map<string, number>>, id: number | undefined, b: string) {
  if (id == null) return;
  const inner = m.get(id) ?? new Map<string, number>();
  inner.set(b, (inner.get(b) ?? 0) + 1);
  m.set(id, inner);
}

// Earliest capture time of a side's nodes — the unfakeable discriminator:
// recording always happens before replay, so record's min < replay's min.
function minStartedNs(nodes: GraphNode[]): number | null {
  let min: number | null = null;
  for (const n of nodes) {
    if (typeof n.started_ns === "number" && (min == null || n.started_ns < min)) min = n.started_ns;
  }
  return min;
}
function fmtCaptureTime(ns: number | null): string {
  if (ns == null) return "—";
  return new Date(ns / 1e6).toLocaleString();
}

export default function GraphView({ runId }: { runId: string }) {
  const [focus, setFocus] = React.useState(true);
  const [showInternal, setShowInternal] = React.useState(false);
  const [collapsed, setCollapsed] = React.useState<Set<string>>(new Set());
  const [expandedVals, setExpandedVals] = React.useState<Set<string>>(new Set());
  const graph = useQuery({ queryKey: ["graph", runId], queryFn: () => api.graph(runId) });
  const calls = useQuery({ queryKey: ["calls", runId], queryFn: () => api.calls(runId) });
  const firstFork = React.useRef<HTMLDivElement | null>(null);
  const seenFork = React.useRef(false);

  const model = React.useMemo(() => {
    if (!graph.data) return null;
    // Record-mode wraps each request in deja's synthetic ingress span; replay
    // does not. Unwrap so the two trees align at "HTTP request". Then drop
    // trace/debug plumbing spans (tokio polls, codec frames) unless asked —
    // record envs capture them, replay routers usually don't, so they render
    // as a wall of record-only noise.
    const recFull = dropHealthTrees(unwrapIngress(graph.data.record));
    const repFull = dropHealthTrees(graph.data.replay);
    const rec = showInternal ? keepAll(recFull) : dropInternalSpans(recFull);
    const rep = showInternal ? keepAll(repFull) : dropInternalSpans(repFull);
    const hiddenSpans = recFull.length - rec.nodes.length + (repFull.length - rep.nodes.length);
    const merged = mergeLevel(buildForest(rec.nodes), buildForest(rep.nodes), "");
    const cs = calls.data ?? [];
    // Mark divergence by graph_node_id (exact, per-side) — node ids are unique
    // WITHIN a side; observed ids index the replay tree, recorded ids the record
    // tree. Path strings don't line up (the graph chain isn't the logical chain).
    const novelIds = new Set<number>(), omittedIds = new Set<number>();
    const recBadges = new Map<number, Map<string, number>>(), repBadges = new Map<number, Map<string, number>>();
    // group by site to find the modified pairs (novel+omitted same span·boundary·method)
    const sites = new Map<string, { rec?: number; rep?: number }>();
    // Value-divergence chips (old -> new), per replay graph node. A value_diverged
    // call ran the REAL boundary so its observed.result is an independent value.
    // The origin (read) + consequence (write) often collapse onto ONE span node
    // (boundary-only granularity), so we collect a LIST per node id.
    type VChip = { method: string; from: unknown; to: unknown; origin: boolean };
    const valueDivIds = new Set<number>();
    const valueDivByNode = new Map<number, VChip[]>();
    for (const c of cs) {
      const oid = rep.resolve(c.observed?.graph_node_id), rid = rec.resolve(c.recorded?.graph_node_id);
      const key = `${c.observed?.logical_span_path ?? c.recorded?.logical_span_path}|${c.boundary}|${c.method_name}`;
      if (c.kind === "value_diverged") {
        const nid = oid ?? rid;
        if (nid != null) {
          valueDivIds.add(nid);
          const chip: VChip = { method: c.method_name, from: c.recorded?.result, to: c.observed?.result, origin: !!c.origin };
          (valueDivByNode.get(nid) ?? valueDivByNode.set(nid, []).get(nid)!).push(chip);
        }
        bump(repBadges, oid, c.boundary); bump(recBadges, rid, c.boundary);
      }
      else if (c.kind === "novel") { if (oid != null) novelIds.add(oid); bump(repBadges, oid, c.boundary); const s = sites.get(key) ?? {}; s.rep = oid; sites.set(key, s); }
      else if (c.kind === "omitted") { if (rid != null) omittedIds.add(rid); bump(recBadges, rid, c.boundary); const s = sites.get(key) ?? {}; s.rec = rid; sites.set(key, s); }
      else { bump(repBadges, oid, c.boundary); bump(recBadges, rid, c.boundary); }
    }
    const originRec = new Set<number>(), originRep = new Set<number>();
    for (const s of sites.values()) {
      if (s.rec != null && s.rep != null) { originRec.add(s.rec); originRep.add(s.rep); }
    }
    // A value-divergence ORIGIN node is also a fork point (⭑) on both sides.
    for (const [nid, chips] of valueDivByNode) {
      if (chips.some((ch) => ch.origin)) { originRec.add(nid); originRep.add(nid); }
    }
    const maxDur = Math.max(1, ...merged.map((u) => Math.max(ms(u.rec), ms(u.rep))));
    // Capture times straight off the raw sides (before internal-span filtering)
    // so the discriminator is robust even when one side is all plumbing.
    const recStart = minStartedNs(graph.data.record);
    const repStart = minStartedNs(graph.data.replay);
    const sidesSwapped = recStart != null && repStart != null && recStart > repStart;
    return { merged, novelIds, omittedIds, valueDivIds, valueDivByNode, originRec, originRep, recBadges, repBadges, maxDur, hiddenSpans, replayEmpty: repFull.length === 0, recStart, repStart, sidesSwapped };
  }, [graph.data, calls.data, showInternal]);

  if (graph.isLoading || calls.isLoading) return <p className="hint">loading graph…</p>;
  if (graph.error || !graph.data || !model) return <p className="err">{String(graph.error)}</p>;

  const { merged, novelIds, omittedIds, valueDivIds, valueDivByNode, originRec, originRep, recBadges, repBadges, maxDur, hiddenSpans, replayEmpty, recStart, repStart, sidesSwapped } = model;
  const recDiv = (u: Uni) => !!u.rec && omittedIds.has(u.rec.node_id);
  const repDiv = (u: Uni) => !!u.rep && novelIds.has(u.rep.node_id);
  const valDiv = (u: Uni) =>
    (!!u.rep && valueDivIds.has(u.rep.node_id)) || (!!u.rec && valueDivIds.has(u.rec.node_id));
  const valChipsFor = (u: Uni) =>
    (u.rep && valueDivByNode.get(u.rep.node_id)) || (u.rec && valueDivByNode.get(u.rec.node_id)) || [];
  const isOrigin = (u: Uni) => (!!u.rec && originRec.has(u.rec.node_id)) || (!!u.rep && originRep.has(u.rep.node_id));
  const subtreeMarked = (u: Uni): boolean => recDiv(u) || repDiv(u) || valDiv(u) || isOrigin(u) || u.children.some(subtreeMarked);
  seenFork.current = false;
  const roots = focus ? merged.filter(subtreeMarked) : merged;
  const hidden = merged.length - roots.length;

  const badges = (m: Map<string, number>) =>
    [...m.entries()].map(([b, n]) => <span className="bbadge" key={b}>{b}{n > 1 ? `×${n}` : ""}</span>);

  function Cell({ u, side }: { u: Uni; side: "rec" | "rep" }) {
    // Diff convention: novel (candidate added) = green/added; omitted (candidate
    // skipped a recorded call) = red/removed; modified pair = amber.
    const kind = repDiv(u) ? "added" : recDiv(u) ? "removed" : "";
    const n = side === "rec" ? u.rec : u.rep;
    if (!n) {
      // Name the span from the PRESENT side so a column of absences still
      // reads as "what was skipped", not anonymous chips.
      return (
        <div className={`zcell absent ${kind}`}>
          <span className="gspan dim">{u.name}</span>
          <span className={`chip ${kind || "muted"}`}>{kind === "added" ? "added on replay" : "skipped"}</span>
        </div>
      );
    }
    const diverged = side === "rec" ? recDiv(u) : repDiv(u);
    const valueChanged = valDiv(u);
    const origin = isOrigin(u);
    const b = (side === "rec" ? recBadges : repBadges).get(n.node_id);
    const dur = ms(n);
    // Chip values can be whole error payloads; clamp so they never bleed
    // across the column divider (the full text rides the title tooltip).
    const fmt = (v: unknown) => {
      const s = typeof v === "string" ? v : JSON.stringify(v) ?? "∅";
      return s.length > 90 ? `${s.slice(0, 90)}…` : s;
    };
    const captureFork = (el: HTMLDivElement | null) => {
      if (el && origin && !seenFork.current) { seenFork.current = true; firstFork.current = el; }
    };
    return (
      <div className={`zcell ${diverged ? `diverged ${kind}` : ""} ${valueChanged ? "diverged valuediv" : ""} ${origin ? "origin" : ""}`} ref={side === "rec" ? captureFork : undefined}>
        {origin && <span className="forkstar" title="fork point — a value diverged here (origin of the cascade)">⭑</span>}
        <span className="gspan">{u.name}</span>
        {reqLabel(n) && <span className="greq">{reqLabel(n)}</span>}
        {b && badges(b)}
        {valueChanged && valChipsFor(u).map((ch, i) => (
          <span
            className={`chip vchip ${ch.origin ? "fail" : "removed"}`}
            key={i}
            title={`${ch.origin ? "divergence ORIGIN — executed read returned a new value" : "CONSEQUENCE — write carried the new value downstream"} — click to expand the full values`}
            style={{ marginLeft: 4 }}
            onClick={(e) => {
              e.stopPropagation();
              setExpandedVals((prev) => {
                const next = new Set(prev);
                next.has(u.path) ? next.delete(u.path) : next.add(u.path);
                return next;
              });
            }}
          >
            {ch.origin ? "origin " : "→ "}{ch.method}: {fmt(ch.from)} → {fmt(ch.to)}
          </span>
        ))}
        {dur > 0 && <span className="durbar" style={{ width: `${Math.max(2, (dur / maxDur) * 80)}px` }} />}
        <span className="gdur">{dur > 0 ? `${dur.toFixed(1)}ms` : ""}</span>
      </div>
    );
  }

  const prettyVal = (v: unknown): string => {
    if (typeof v === "string") return v; // error strings carry \n formatting
    if (v === undefined) return "∅";
    return JSON.stringify(v, null, 2) ?? "∅";
  };

  function Row({ u, depth }: { u: Uni; depth: number }) {
    const hasKids = u.children.length > 0;
    const isCollapsed = collapsed.has(u.path);
    const toggle = () =>
      setCollapsed((prev) => {
        const next = new Set(prev);
        next.has(u.path) ? next.delete(u.path) : next.add(u.path);
        return next;
      });
    // BOTH columns indent by depth so a span aligns across the center divider;
    // the caret toggle is on the left, the right gets a matching spacer.
    const rails = () => Array.from({ length: depth }).map((_, i) => <span className="rail" key={i} />);
    return (
      <>
        <div className="ziprow">
          <div className="zcell" style={{ flex: 1, padding: 0 }}>
            <div style={{ display: "flex", alignItems: "center", paddingLeft: 4, minWidth: 0 }}>
              {rails()}
              <span className="caret" onClick={hasKids ? toggle : undefined}>{hasKids ? (isCollapsed ? "▸" : "▾") : ""}</span>
              <Cell u={u} side="rec" />
            </div>
          </div>
          <div className="zcell" style={{ flex: 1, padding: 0, borderLeft: "1px solid var(--border)" }}>
            <div style={{ display: "flex", alignItems: "center", paddingLeft: 4, minWidth: 0 }}>
              {rails()}
              <span className="caret" />
              <Cell u={u} side="rep" />
            </div>
          </div>
        </div>
        {expandedVals.has(u.path) && valChipsFor(u).length > 0 && (
          <div className="valdetail">
            {valChipsFor(u).map((ch, i) => (
              <div key={i}>
                <div className="vdhead">
                  {ch.origin ? "⭑ origin" : "→ consequence"} · <code>{ch.method}</code>
                </div>
                <div className="vdpair">
                  <div><h4>recorded</h4><pre>{prettyVal(ch.from)}</pre></div>
                  <div><h4>replayed</h4><pre>{prettyVal(ch.to)}</pre></div>
                </div>
              </div>
            ))}
          </div>
        )}
        {!isCollapsed && u.children.map((c) => <Row key={c.path} u={c} depth={depth + 1} />)}
      </>
    );
  }

  const jump = () => firstFork.current?.scrollIntoView({ behavior: "smooth", block: "center" });

  return (
    <>
      <div className="graphtoolbar">
        <label className="toggle">
          <input type="checkbox" checked={focus} onChange={(e) => setFocus(e.target.checked)} />
          focus diverging request{focus ? "" : " (showing all spans)"}
        </label>
        {originRec.size > 0 && <button onClick={jump} style={{ padding: "2px 10px" }}>⭑ jump to fork</button>}
        {(hiddenSpans > 0 || showInternal) && (
          <label className="toggle">
            <input type="checkbox" checked={showInternal} onChange={(e) => setShowInternal(e.target.checked)} />
            show internal spans{hiddenSpans > 0 ? ` (${hiddenSpans} hidden)` : ""}
          </label>
        )}
        <button onClick={() => setCollapsed(new Set())} style={{ background: "var(--surface-overlay)", color: "var(--text-muted)", padding: "2px 10px" }}>expand all</button>
        <span className="hint">⭑ = fork point · record shows omitted (recording made it) · replay shows novel (candidate made it)</span>
      </div>
      {sidesSwapped && (
        <p className="err" style={{ padding: "6px 12px" }}>
          ⚠ the record side's earliest span is <b>newer</b> than the replay side's —
          the graph sources may be mislabeled for this run.
        </p>
      )}
      <div className="graphwrap">
        <div className="graphhdr">
          <div><b>record</b> <span className="hint">what it used to do · captured {fmtCaptureTime(recStart)}</span> {omittedIds.size > 0 && <span className="chip removed">{omittedIds.size} omitted spans</span>}</div>
          <div><b>replay</b> <span className="hint">what it does now · captured {fmtCaptureTime(repStart)}</span> {replayEmpty && <span className="chip muted" title="the replay router emitted no graph nodes — check ROUTER__DEJA__RECORDING__GRAPH">no replay graph captured</span>} {novelIds.size > 0 && <span className="chip added">{novelIds.size} novel spans</span>} {valueDivIds.size > 0 && <span className="chip fail">{valueDivIds.size} value-diverged span{valueDivIds.size > 1 ? "s" : ""}</span>}</div>
        </div>
        {roots.length === 0 && <p className="hint" style={{ padding: 12 }}>no diverging request to focus</p>}
        {roots.map((u) => <Row key={u.path} u={u} depth={0} />)}
      </div>
      {focus && hidden > 0 && <p className="hint">{hidden} clean request tree{hidden > 1 ? "s" : ""} hidden — toggle off focus to see all.</p>}
    </>
  );
}
