// THE SPINE: one tree per request, built from the CALL LEDGER, with the
// execution graph hung off it as enrichment.
//
// WHY THE LEDGER AND NOT THE GRAPH. Measured on
// rp-dev-dcb9f9e955-1785331134782268537-0805095859062 (live, 2026-08-05):
//
//   record graph 1,267 nodes -> 3 roots, all `ROOT_SPAN`
//   replay graph 3,809 nodes -> 493 roots: 318 with no `parent_id` at all and
//                               175 more naming a parent absent from the
//                               payload, silently promoted. Top root names are
//                               `poll` (166), `reserve_capacity` (55),
//                               `send_data` (55) — HTTP/2 framing, not requests.
//
// The two sides do not root at the same name, so a name-keyed merge of the two
// forests pairs NOTHING: I re-ran the shipped `mergeLevel` over this payload and
// got 5,076 unified rows, 0 of them two-sided, and 5,076 cells reading
// "skipped". That is a property of the algorithm, not of the run.
//
// The ledger, by contrast, is complete: `span_path` and `graph_node_id` are on
// 536 of 536 call sides and `correlation_id` on 301 of 301 rows. It also carries
// ancestry the graph has lost — the graph chain above an anchored replay node is
// TRUNCATED on 100 of 236 sides (it starts mid-path at
// `find_api_key_by_hash_optional`, missing `HTTP request>ROOT_SPAN>…`), while
// `span_path` always carries the whole chain.
//
// The one structural relation that does hold, verified on 534 of 534 anchored
// sides: THE GRAPH CHAIN IS ALWAYS A SUFFIX OF `span_path`. That is what lets
// this module walk up from an anchored node and label the spine's ancestors with
// real graph node ids — 324 of 325 rows end up addressable — without ever
// trusting the graph for structure.

import { CallRecord, GraphNode, HttpDiff } from "./api";

export type Side = "rec" | "rep";

/** What kind of evidence a row carries. Ordered worst-first. */
export type Mark =
  | "origin"
  | "consequence"
  | "response"
  | "omitted"
  | "novel"
  | "environmental"
  | "matched";

export const MARK_ORDER: Mark[] = [
  "origin",
  "consequence",
  "response",
  "omitted",
  "novel",
  "environmental",
  "matched",
];

const RANK = new Map(MARK_ORDER.map((m, i) => [m, i]));

function worse(a: Mark | null, b: Mark | null): Mark | null {
  if (a == null) return b;
  if (b == null) return a;
  return RANK.get(a)! <= RANK.get(b)! ? a : b;
}

/** The ledger's own evidence for a span, per side. Never a verdict. */
export type Presence = "both" | "record-only" | "replay-only" | "no-evidence";

/**
 * A ledger row as seen FROM one span, with the sides of it that sit here.
 *
 * Usually both, but not always: on the live run one `generic_update_with_results`
 * row has its recorded side at `…>update_payment_attempt_with_attempt_id` and its
 * observed side at `…>update_payment_intent`. That row appears at BOTH spans —
 * hiding it at one of them would hide the structural half of the divergence —
 * and `sides` is what lets the panel say which half you are looking at.
 */
export type CallEntry = { call: CallRecord; sides: Side[] };

/** A run of graph spans hidden beneath a spine row, kept countable. */
export type HiddenGroup = { name: string; target: string; count: number; ms: number };

export type SpineNode = {
  /** `${caseId}|${path}` — unique across the run. */
  key: string;
  caseId: string;
  /** The `span_path` prefix this row stands for. */
  path: string;
  name: string;
  depth: number;
  children: SpineNode[];
  /** Ledger rows with at least one side whose `span_path` is exactly this path. */
  calls: CallEntry[];
  /** Response diffs for the request. Only ever on the case root. */
  http: HttpDiff[];
  /**
   * Graph node ids for this row, per side. A row is a SITE, not an instance:
   * 36 of 168 (correlation, span_path) sites on this run carry more than one
   * distinct record node id, five at the worst (`get_or_populate_in_memory`,
   * `get_key`, `set_key`, `get_or_populate_redis`). Selection therefore names a
   * node id, never a span path.
   */
  recNodes: number[];
  repNodes: number[];
  /** Wall time summed over this row's own nodes on that side. */
  recMs: number | null;
  repMs: number | null;
  hiddenRec: HiddenGroup[];
  hiddenRep: HiddenGroup[];
  presence: Presence;
  /** Strongest evidence AT this row, `matched` included. */
  mark: Mark | null;
  /**
   * Strongest FINDING at or below this row — `matched` deliberately excluded, so
   * that "only paths with a finding" prunes. With `matched` folded in it would
   * keep every row on this run (217 of 301 ledger rows are matched) and the
   * filter would silently do nothing.
   */
  worst: Mark | null;
};

export type CaseModel = {
  caseId: string;
  root: SpineNode;
  rows: number;
  counts: Partial<Record<Mark, number>>;
  /** node id -> row, per side, for `?node=` resolution. */
  byNode: { rec: Map<number, SpineNode>; rep: Map<number, SpineNode> };
};

/** Graph spans that belong to no request at all. Reported, never dropped. */
export type Unattributed = { side: Side; total: number; groups: HiddenGroup[] };

export type SpineModel = {
  cases: CaseModel[];
  unattributed: Unattributed[];
  /** Every graph span not shown as a row, with a reason. */
  hiddenTotals: { rec: number; rep: number; transport: number; internal: number };
  graphMissing: boolean;
};

/* ------------------------------------------------------------------ helpers */

// `parent_id` is DECLARED `number | null` in api.ts but is OMITTED from the JSON
// on 318 of 3,809 live replay nodes, so it arrives as `undefined`. Normalising
// here rather than widening the shared type keeps lib/api.ts untouched.
function parentOf(n: GraphNode): number | null {
  return n.parent_id ?? null;
}

function msOf(n: GraphNode | undefined): number | null {
  if (!n || n.closed_ns == null) return null;
  return (n.closed_ns - n.started_ns) / 1e6;
}

/** Framing/transport spans: HTTP/2 wire machinery and SDK plumbing. */
export function isTransport(n: GraphNode): boolean {
  const t = (n.target || "").split("::")[0];
  return t === "h2" || t === "hyper" || t.startsWith("aws_");
}

function markOf(c: CallRecord): Mark | null {
  switch (c.kind) {
    case "value_diverged":
      return c.origin ? "origin" : "consequence";
    case "omitted":
      return "omitted";
    case "novel":
      return "novel";
    case "environmental":
      return "environmental";
    case "matched":
    case "recovered":
    case "deterministic":
      return "matched";
    default:
      return null;
  }
}

/**
 * A transport failure is ONE failure, however many fields it flattens.
 *
 * All three live response diffs read `status_candidate: 0` with
 * `candidate_body: {"error":"no header/body separator"}` — the kernel's own HTTP
 * parse failure. Each carries 54 body leaves; 53 of them are simply
 * `candidate: null` because there was no response to compare against, and the
 * 54th is the error marker itself. 3 x 54 = 162 raw leaves, which the scorecard
 * counts as 156 `BodyMismatch` + 3 `StatusMismatch`. They are 3 findings, one
 * per request, not 159.
 */
export function transportFailure(d: HttpDiff): string | null {
  const body = d.candidate_body as { error?: unknown } | null | undefined;
  const err = body && typeof body.error === "string" ? body.error : null;
  if (err && d.status_candidate === 0) return err;
  // Fall back to the shape rather than the marker: no status, and every leaf
  // that is not the marker reports the candidate had nothing.
  const leaves = d.body_diff.filter((f) => f.json_path !== "$.error");
  if (d.status_candidate === 0 && leaves.length > 0 && leaves.every((f) => f.candidate === null)) {
    return err ?? "the candidate produced no parseable HTTP response";
  }
  return null;
}

/* -------------------------------------------------------------------- build */

export function buildSpine(
  calls: CallRecord[],
  graph: { record: GraphNode[]; replay: GraphNode[] } | undefined,
  https: HttpDiff[],
): SpineModel {
  const idx = {
    rec: new Map<number, GraphNode>((graph?.record ?? []).map((n) => [n.node_id, n])),
    rep: new Map<number, GraphNode>((graph?.replay ?? []).map((n) => [n.node_id, n])),
  };

  const UNCORRELATED = "(uncorrelated)";
  const order: string[] = [];
  const nodes = new Map<string, SpineNode>(); // key -> row
  const caseRoots = new Map<string, SpineNode>();

  const fresh = (caseId: string, path: string, depth: number): SpineNode => ({
    key: `${caseId}|${path}`,
    caseId,
    path,
    name: path.split(">").pop() ?? path,
    depth,
    children: [],
    calls: [],
    http: [],
    recNodes: [],
    repNodes: [],
    recMs: null,
    repMs: null,
    hiddenRec: [],
    hiddenRep: [],
    presence: "no-evidence",
    mark: null,
    worst: null,
  });

  /** Get or create the row for a `span_path` prefix, creating its ancestors. */
  function rowFor(caseId: string, segs: string[], depth: number): SpineNode {
    const path = segs.slice(0, depth + 1).join(">");
    const key = `${caseId}|${path}`;
    const found = nodes.get(key);
    if (found) return found;
    const node = fresh(caseId, path, depth);
    nodes.set(key, node);
    if (depth === 0) {
      caseRoots.set(caseId, node);
      if (!order.includes(caseId)) order.push(caseId);
    } else {
      rowFor(caseId, segs, depth - 1).children.push(node);
    }
    return node;
  }

  const sidePresence = new Map<string, { rec: boolean; rep: boolean }>();

  for (const c of calls) {
    const caseId = c.correlation_id ?? UNCORRELATED;
    const seen = new Map<SpineNode, CallEntry>();
    for (const [side, cs] of [
      ["rec", c.recorded],
      ["rep", c.observed],
    ] as const) {
      if (!cs?.span_path) continue;
      const segs = cs.span_path.split(">");
      const site = rowFor(caseId, segs, segs.length - 1);
      const at = seen.get(site);
      if (at) at.sides.push(side);
      else {
        const entry: CallEntry = { call: c, sides: [side] };
        site.calls.push(entry);
        seen.set(site, entry);
      }
      const p = sidePresence.get(site.key) ?? { rec: false, rep: false };
      p[side] = true;
      sidePresence.set(site.key, p);

      // Label this row AND its ancestors with real graph node ids, using the
      // suffix relation verified on 534/534 anchored sides: the chain walked up
      // from `graph_node_id` is always a suffix of `span_path`, so the node
      // `k` levels above the anchor belongs to the row at depth len-1-k.
      const gid = cs.graph_node_id;
      if (gid == null) continue;
      let cur: number | null = gid;
      let depth = segs.length - 1;
      const guard = new Set<number>();
      while (cur != null && depth >= 0 && !guard.has(cur)) {
        guard.add(cur);
        const gn = idx[side].get(cur);
        if (!gn) break;
        const row = rowFor(caseId, segs, depth);
        const bucket = side === "rec" ? row.recNodes : row.repNodes;
        if (!bucket.includes(cur)) bucket.push(cur);
        cur = parentOf(gn);
        depth -= 1;
      }
    }
  }

  // Response diffs belong to the request, so they hang on the case root — the
  // `HTTP request` span. /http-diffs carries correlation_id, request_path and
  // request_sequence and NO span path, so there is nowhere deeper to put them
  // that the data actually supports.
  for (const d of https) {
    const root = caseRoots.get(d.correlation_id);
    if (root) root.http.push(d);
    else if (d.correlation_id) {
      // A response with no ledger rows still deserves a case.
      const r = rowFor(d.correlation_id, ["HTTP request"], 0);
      r.http.push(d);
    }
  }

  /* ---- enrichment: durations, presence, marks ---- */
  for (const n of nodes.values()) {
    const sum = (ids: number[], side: Side) => {
      let t: number | null = null;
      for (const id of ids) {
        const v = msOf(idx[side].get(id));
        if (v != null) t = (t ?? 0) + v;
      }
      return t;
    };
    n.recMs = sum(n.recNodes, "rec");
    n.repMs = sum(n.repNodes, "rep");
    const p = sidePresence.get(n.key);
    n.presence = !p
      ? "no-evidence"
      : p.rec && p.rep
        ? "both"
        : p.rec
          ? "record-only"
          : "replay-only";
    for (const e of n.calls) n.mark = worse(n.mark, markOf(e.call));
    if (n.http.length > 0) n.mark = worse(n.mark, "response");
  }

  /* ---- hidden graph spans: attach each to its nearest spine row ---- */
  const owner = { rec: new Map<number, SpineNode>(), rep: new Map<number, SpineNode>() };
  for (const n of nodes.values()) {
    for (const id of n.recNodes) if (!owner.rec.has(id)) owner.rec.set(id, n);
    for (const id of n.repNodes) if (!owner.rep.has(id)) owner.rep.set(id, n);
  }

  const unattributed: Unattributed[] = [];
  let hidRec = 0;
  let hidRep = 0;
  let hidTransport = 0;
  let hidInternal = 0;

  for (const side of ["rec", "rep"] as const) {
    const all = side === "rec" ? (graph?.record ?? []) : (graph?.replay ?? []);
    const loose = new Map<string, HiddenGroup>();
    let looseTotal = 0;
    for (const n of all) {
      if (owner[side].has(n.node_id)) continue;
      // Walk up to the nearest ancestor that IS on the spine.
      let cur = parentOf(n);
      let host: SpineNode | undefined;
      const guard = new Set<number>();
      while (cur != null && !guard.has(cur)) {
        guard.add(cur);
        host = owner[side].get(cur);
        if (host) break;
        const up = idx[side].get(cur);
        if (!up) break;
        cur = parentOf(up);
      }
      const ms = msOf(n) ?? 0;
      if (isTransport(n)) hidTransport += 1;
      else hidInternal += 1;
      if (side === "rec") hidRec += 1;
      else hidRep += 1;
      const into = host ? (side === "rec" ? host.hiddenRec : host.hiddenRep) : null;
      if (into) {
        const g = into.find((x) => x.name === n.span_name && x.target === n.target);
        if (g) {
          g.count += 1;
          g.ms += ms;
        } else into.push({ name: n.span_name, target: n.target, count: 1, ms });
      } else {
        looseTotal += 1;
        const k = `${n.span_name}|${n.target}`;
        const g = loose.get(k);
        if (g) {
          g.count += 1;
          g.ms += ms;
        } else loose.set(k, { name: n.span_name, target: n.target, count: 1, ms });
      }
    }
    if (looseTotal > 0) {
      unattributed.push({
        side,
        total: looseTotal,
        groups: [...loose.values()].sort((a, b) => b.count - a.count),
      });
    }
  }

  const byCount = (a: HiddenGroup, b: HiddenGroup) => b.count - a.count;
  for (const n of nodes.values()) {
    n.hiddenRec.sort(byCount);
    n.hiddenRep.sort(byCount);
  }

  /* ---- roll marks up, order children, count ---- */
  const cases: CaseModel[] = [];
  for (const caseId of order) {
    const root = caseRoots.get(caseId)!;
    const counts: Partial<Record<Mark, number>> = {};
    let rows = 0;
    const byNode = { rec: new Map<number, SpineNode>(), rep: new Map<number, SpineNode>() };
    // A row can appear at two spans (see `CallEntry`), so count the ROW, once.
    const counted = new Set<CallRecord>();
    const visit = (n: SpineNode): Mark | null => {
      rows += 1;
      for (const id of n.recNodes) if (!byNode.rec.has(id)) byNode.rec.set(id, n);
      for (const id of n.repNodes) if (!byNode.rep.has(id)) byNode.rep.set(id, n);
      for (const e of n.calls) {
        if (counted.has(e.call)) continue;
        counted.add(e.call);
        const m = markOf(e.call);
        if (m) counts[m] = (counts[m] ?? 0) + 1;
      }
      if (n.http.length) counts.response = (counts.response ?? 0) + n.http.length;
      // Children in first-seen order — the ledger's order, which follows the
      // recorded call sequence. The graph's `sequence` is per-side and cannot
      // order a merged row.
      let w: Mark | null = n.mark === "matched" ? null : n.mark;
      for (const ch of n.children) w = worse(w, visit(ch));
      n.worst = w;
      return w;
    };
    visit(root);
    cases.push({ caseId, root, rows, counts, byNode });
  }

  return {
    cases,
    unattributed,
    hiddenTotals: { rec: hidRec, rep: hidRep, transport: hidTransport, internal: hidInternal },
    graphMissing: !graph || (graph.record.length === 0 && graph.replay.length === 0),
  };
}

/** Depth-first row list for a case, honouring collapse and the marks filter. */
export function flatten(
  root: SpineNode,
  collapsed: Set<string>,
  onlyMarked: boolean,
): SpineNode[] {
  const out: SpineNode[] = [];
  const walk = (n: SpineNode) => {
    if (onlyMarked && n.worst == null) return;
    out.push(n);
    if (collapsed.has(n.key)) return;
    for (const c of n.children) walk(c);
  };
  walk(root);
  return out;
}

/** The ancestors of a row, so a deep link can expand its way down to it. */
export function ancestorKeys(n: SpineNode): string[] {
  const segs = n.path.split(">");
  const out: string[] = [];
  for (let i = 1; i < segs.length; i++) out.push(`${n.caseId}|${segs.slice(0, i).join(">")}`);
  return out;
}
