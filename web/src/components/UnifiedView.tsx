import React from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { api, CallRecord, HttpDiff, Scorecard } from "../lib/api";
import {
  ancestorKeys,
  buildSpine,
  CallEntry,
  CaseModel,
  flatten,
  Mark,
  MARK_ORDER,
  Side,
  SpineNode,
  transportFailure,
} from "../lib/spine";
import { diffArgs, LeafDiff } from "../lib/argdiff";
import { JsonView, ValuePair } from "./JsonView";
import { JsonDiff } from "./JsonDiff";
import { ConfidenceBadge, levelForRank } from "./Confidence";

/* ------------------------------------------------------------------ layout --
 *
 * ONE MERGED TREE WITH PER-SIDE MARKS, not two trees side by side: pairing here
 * is an identity from the ledger — 133 of 168 sites on the live run carry both a
 * recorded and an observed side on the same row — whereas two independent trees
 * can only be aligned by name and ordinal, which is exactly the algorithm that
 * pairs 0 of 5,076 rows and prints "skipped" 5,076 times today; the merged tree
 * also stops spending half the width re-printing the 79% of span names that are
 * identical on both sides, leaving room for the two duration columns that are
 * the only genuinely per-side facts.
 */

const MARK_LABEL: Record<Mark, string> = {
  origin: "origin",
  consequence: "consequence",
  response: "response",
  omitted: "omitted",
  novel: "novel",
  environmental: "environmental",
  matched: "matched",
};

const PRESENCE_LABEL: Record<SpineNode["presence"], string> = {
  both: "both sides",
  // NEVER "did not run during replay". /calls carries no span-presence evidence
  // whatsoever: every one of the 35 record-only sites on this run is record-only
  // solely because its own calls were classified `omitted`. A confident absence
  // claim would be a verdict invented out of missing data.
  "record-only": "no replay-side evidence",
  "replay-only": "no record-side evidence",
  "no-evidence": "no boundary events",
};

function fmtMs(v: number | null): string {
  if (v == null) return "";
  return v >= 100 ? `${v.toFixed(0)}ms` : v >= 1 ? `${v.toFixed(1)}ms` : `${v.toFixed(2)}ms`;
}

function siteOf(c: CallRecord): string {
  const s = c.recorded ?? c.observed;
  return s?.call_file ? `${s.call_file}:${s.call_line ?? "?"}` : "";
}

/* ------------------------------------------------------------- leaf diffing */

/**
 * A ciphertext byte array. `argdiff`'s `isObj` already excludes arrays, so one
 * of these is a single leaf rather than 47 — but rendered raw it is still 47
 * integers of noise. It is CLASSIFIED here, not deleted: the leaf still appears,
 * labelled with why it cannot match.
 */
function ciphertextBytes(v: unknown): number | null {
  return Array.isArray(v) && v.length > 0 && v.every((x) => typeof x === "number") ? v.length : null;
}

function LeafRow({ d }: { d: LeafDiff }) {
  const a = ciphertextBytes(d.recorded);
  const b = ciphertextBytes(d.candidate);
  return (
    <div className="leafdiff">
      <div className="jpath">{d.path || "(value)"}</div>
      {a != null && b != null ? (
        <div className="suppressed">
          ciphertext — {a} bytes recorded, {b} replayed. Encrypted with a fresh per-value nonce, so
          the bytes differ by construction; this is not evidence about the candidate.
        </div>
      ) : d.highlight ? (
        <div className="strhl">
          <span className="ctx">{d.highlight.before}</span>
          {d.highlight.recordedMid && <del>{d.highlight.recordedMid}</del>}
          {d.highlight.candidateMid && <ins>{d.highlight.candidateMid}</ins>}
          <span className="ctx">{d.highlight.after}</span>
        </div>
      ) : (
        <ValuePair baseline={d.recorded} candidate={d.candidate} />
      )}
    </div>
  );
}

function FieldDiff({ recorded, candidate }: { recorded: unknown; candidate: unknown }) {
  const leaves = React.useMemo(() => diffArgs(recorded, candidate), [recorded, candidate]);
  const cipher = leaves.filter(
    (d) => ciphertextBytes(d.recorded) != null && ciphertextBytes(d.candidate) != null,
  ).length;
  if (leaves.length === 0)
    return <p className="hint">the two values are structurally identical at every leaf.</p>;
  return (
    <>
      <p className="hint">
        {leaves.length} changed leaf{leaves.length === 1 ? "" : "s"}
        {cipher > 0 ? ` — ${cipher} of them ciphertext that cannot match` : ""}.
      </p>
      <div className="argdiff">
        {leaves.map((d, i) => (
          <LeafRow key={i} d={d} />
        ))}
      </div>
    </>
  );
}

/* ---------------------------------------------------------- evidence panels */

function CallHead({ c }: { c: CallRecord }) {
  const rank = c.resolved_rank;
  return (
    <div className="evhead">
      <span className="bm">
        <b>{c.boundary}</b>·{c.method_name}
      </span>
      {rank != null && (
        <ConfidenceBadge level={levelForRank(rank)} title={`resolved at rank ${rank}`} />
      )}
      {c.blocking && <span className="chip fail">blocking</span>}
      <span className="where">{siteOf(c)}</span>
    </div>
  );
}

/**
 * The two sides of one ledger row sitting at two different spans. Shown at both,
 * and named at both, because it is a structural divergence in its own right —
 * and, when the result types also disagree, a strong hint the pair is wrong.
 */
function SplitSpans({ e }: { e: CallEntry }) {
  const rec = e.call.recorded?.span_path;
  const rep = e.call.observed?.span_path;
  if (!rec || !rep || rec === rep) return null;
  const here = e.sides[0] === "rec" ? "recorded" : "replayed";
  const other = e.sides[0] === "rec" ? rep : rec;
  return (
    <p className="evwarn">
      This row&rsquo;s two sides sit at <b>different spans</b>. You are looking at its {here} side;
      the other is at <code>{other.split(">").pop()}</code>. The same row is shown there too.
    </p>
  );
}

function typeNameOf(v: unknown): string | null {
  const o = v as { type_name?: unknown } | null | undefined;
  return o && typeof o.type_name === "string" ? o.type_name : null;
}

/** VALUE DIVERGENCE — the boundary ran for real and returned something else. */
function ValueDiverged({ e }: { e: CallEntry }) {
  const c = e.call;
  const origin = !!c.origin;
  const tr = typeNameOf(c.recorded?.result);
  const to = typeNameOf(c.observed?.result);
  return (
    <div className="evidence ev-value">
      <CallHead c={c} />
      <SplitSpans e={e} />
      <p className="evwhat">
        {origin ? (
          <>
            <b>Origin.</b> This boundary was declared <code>Execute</code>, so it really ran during
            replay instead of serving the recorded value — and what it returned differs. Everything
            downstream that carries this value is a consequence of it, not an independent finding.
          </>
        ) : (
          <>
            <b>Consequence.</b> This call carried a value that had already changed upstream. It is
            reported so the cascade is visible; the cause is at an origin above it.
          </>
        )}
      </p>
      {tr && to && tr !== to && (
        <p className="evwarn">
          The two sides report different result types — <code>{tr}</code> against <code>{to}</code>.
          Consequence pairing keys on (correlation, boundary, method) and not on the span path, so
          this row may be a mis-pairing rather than a divergence. Read it as a question, not a
          finding.
        </p>
      )}
      <h4>result</h4>
      <FieldDiff recorded={c.recorded?.result} candidate={c.observed?.result} />
      <details className="evraw">
        <summary>arguments</summary>
        <FieldDiff recorded={c.recorded?.args} candidate={c.observed?.args} />
      </details>
    </div>
  );
}

/** OMITTED — a call the recording made that the replay ledger does not carry. */
function Omitted({ e }: { e: CallEntry }) {
  const c = e.call;
  return (
    <div className="evidence ev-omitted">
      <CallHead c={c} />
      <SplitSpans e={e} />
      <p className="evwhat">
        The recording made this call. The replay ledger has no call to pair it with.
      </p>
      <p className="evlimit">
        That is the absence of a <i>call</i>, not proof the span never ran: <code>/calls</code>{" "}
        carries no span-presence evidence of any kind. The candidate may have taken a different path
        here, or reached the same span and made no boundary call.
      </p>
      <h4>recorded arguments</h4>
      <JsonView value={c.recorded?.args} />
      <h4>recorded result</h4>
      <JsonView value={c.recorded?.result} />
      <p className="hint">
        Nothing is shown for the replay side because nothing was published for it.
      </p>
    </div>
  );
}

/** ENVIRONMENTAL — the replay sandbox, not the candidate. */
function Environmental({ e, note }: { e: CallEntry; note: string | null }) {
  const c = e.call;
  return (
    <div className="evidence ev-env">
      <CallHead c={c} />
      <SplitSpans e={e} />
      <p className="evwhat">
        The candidate made this call and the recording has none to pair with it, at a boundary the
        scorer tiers <b>environmental</b>. It is <b>not</b> attributed to the candidate.
      </p>
      {note && <p className="evnote">The scorer&rsquo;s own note on {c.boundary}: &ldquo;{note}&rdquo;</p>}
      <h4>arguments the candidate sent</h4>
      <JsonView value={c.observed?.args} />
    </div>
  );
}

/** NOVEL — a call the candidate made that the recording did not. */
function Novel({ e }: { e: CallEntry }) {
  const c = e.call;
  return (
    <div className="evidence ev-novel">
      <CallHead c={c} />
      <SplitSpans e={e} />
      <p className="evwhat">The candidate made this call; the recording has none to pair with it.</p>
      <h4>arguments the candidate sent</h4>
      <JsonView value={c.observed?.args} />
    </div>
  );
}

/** MATCHED — reconciled. Shown so that clicking any span answers something. */
function Matched({ e }: { e: CallEntry }) {
  const c = e.call;
  return (
    <div className="evidence ev-matched">
      <CallHead c={c} />
      <SplitSpans e={e} />
      <p className="evwhat">Reconciled: this call was found on both sides and its values agree.</p>
      <details className="evraw">
        <summary>recorded value</summary>
        <JsonView value={c.recorded?.result} />
      </details>
    </div>
  );
}

/**
 * RESPONSE — what the caller saw.
 *
 * ONE FAILURE, NOT 156. Each of the three live diffs carries 54 body leaves of
 * which 53 are `candidate: null` and one is the parse error itself; the scorer
 * counts 156 `BodyMismatch` + 3 `StatusMismatch` across the run. They all say
 * the same single thing — there was no response to compare — so that is what
 * this renders, with the flattened fields available but subordinate.
 */
function Response({ d }: { d: HttpDiff }) {
  const failed = transportFailure(d);
  const missing = d.body_diff.filter((f) => f.candidate === null).length;
  return (
    <div className="evidence ev-http">
      <div className="evhead">
        <span className="bm">
          <b>{d.request_path}</b> · seq {d.request_sequence}
        </span>
        <span className="statuspill">
          <span className="ok">{d.status_baseline}</span> <span className="sep">→</span>{" "}
          <span className="bad">{failed ? "no response" : d.status_candidate}</span>
        </span>
      </div>
      {failed ? (
        <>
          <p className="evwhat">
            <b>The candidate returned no HTTP response that could be parsed</b> — &ldquo;{failed}
            &rdquo;. This is one failure.
          </p>
          <p className="evlimit">
            The {missing} recorded response field{missing === 1 ? "" : "s"} below are reported as
            differing only because there was nothing on the other side to compare them to. They are
            a consequence of the missing response, not {missing} separate findings — and because the
            response never parsed, this run cannot say whether the candidate&rsquo;s behaviour
            changed.
          </p>
          <details className="evraw">
            <summary>
              the {missing} recorded field{missing === 1 ? "" : "s"} and the recorded body
            </summary>
            <JsonView value={d.baseline_body} clamp={false} />
          </details>
        </>
      ) : (
        <>
          <p className="evwhat">
            {d.status_match
              ? `The status matched; ${d.body_diff.length} response field${d.body_diff.length === 1 ? "" : "s"} differ.`
              : `The recorded response was ${d.status_baseline}; the candidate answered ${d.status_candidate}.`}
          </p>
          <JsonDiff before={d.baseline_body} after={d.candidate_body} />
        </>
      )}
    </div>
  );
}

/** NO EVIDENCE — a span on the path, with nothing of its own to report. */
function NoEvidence({ n }: { n: SpineNode }) {
  const sites = React.useMemo(() => {
    const out: SpineNode[] = [];
    const walk = (x: SpineNode) => {
      if (x !== n && x.calls.length > 0) out.push(x);
      x.children.forEach(walk);
    };
    walk(n);
    return out;
  }, [n]);
  const noNode = n.recNodes.length === 0 && n.repNodes.length === 0;
  return (
    <div className="evidence ev-none">
      <p className="evwhat">
        <b>No boundary event was recorded at this span on either side.</b> It is here because it lies
        on the <code>span_path</code> of spans that do have evidence — it is a waypoint, not a
        finding, and nothing here says whether it ran.
      </p>
      {noNode ? (
        <p className="evlimit">
          The execution graph has no node for it on either side either, so not even its duration is
          known. On this run that is true of exactly one row per request: the synthetic{" "}
          <code>HTTP request</code> root, which the ledger names and the record graph does not
          contain.
        </p>
      ) : (
        <p className="hint">
          The execution graph does carry it:
          {n.recNodes.length > 0 && ` record ${n.recNodes.length} span(s), ${fmtMs(n.recMs)}`}
          {n.repNodes.length > 0 && ` replay ${n.repNodes.length} span(s), ${fmtMs(n.repMs)}`}.
        </p>
      )}
      {sites.length > 0 && (
        <>
          <h4>{sites.length} span(s) below it do carry evidence</h4>
          <ul className="evlist">
            {sites.slice(0, 12).map((s) => (
              <li key={s.key}>
                <code>{s.name}</code>{" "}
                <span className="hint">
                  {s.calls.length} call{s.calls.length === 1 ? "" : "s"}
                  {s.worst ? ` · ${MARK_LABEL[s.worst]}` : ""}
                </span>
              </li>
            ))}
          </ul>
          {sites.length > 12 && <p className="hint">and {sites.length - 12} more.</p>}
        </>
      )}
    </div>
  );
}

/* ------------------------------------------------------------ detail panel  */

function HiddenList({ label, groups }: { label: string; groups: SpineNode["hiddenRec"] }) {
  if (groups.length === 0) return null;
  const total = groups.reduce((a, g) => a + g.count, 0);
  return (
    <details className="evraw">
      <summary>
        {total} {label} span{total === 1 ? "" : "s"} beneath this one
      </summary>
      <table className="hiddentbl">
        <tbody>
          {groups.map((g) => (
            <tr key={`${g.name}|${g.target}`}>
              <td className="mono">{g.name}</td>
              <td className="hint">{g.target}</td>
              <td className="num">×{g.count}</td>
              <td className="num">{fmtMs(g.ms)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </details>
  );
}

function SpanDetail({
  node,
  selected,
  onPick,
  boundaryNotes,
}: {
  node: SpineNode;
  selected: { nodeId: number | null; side: Side };
  onPick: (nodeId: number, side: Side) => void;
  boundaryNotes: Record<string, string | null>;
}) {
  // A row is a SITE. When it holds several instances the panel enumerates them
  // and each one is separately addressable, because `(correlation, span_path)`
  // is not an instance key: 36 of 168 sites here carry more than one node id.
  const instances = React.useMemo(() => {
    const out: { nodeId: number; side: Side }[] = [];
    for (const id of node.recNodes) out.push({ nodeId: id, side: "rec" });
    for (const id of node.repNodes) out.push({ nodeId: id, side: "rep" });
    return out;
  }, [node]);

  const empty = node.calls.length === 0 && node.http.length === 0;

  return (
    <aside className="uvdetail">
      <div className="uvdhead">
        <code className="uvdname">{node.name}</code>
        <span className={`presbadge p-${node.presence}`}>{PRESENCE_LABEL[node.presence]}</span>
      </div>
      <code className="spanpath">{node.path.replace(/>/g, " › ")}</code>

      <div className="uvdurs">
        <span>
          <i>record</i> {node.recNodes.length ? `${fmtMs(node.recMs)} over ${node.recNodes.length} span(s)` : "—"}
        </span>
        <span>
          <i>replay</i> {node.repNodes.length ? `${fmtMs(node.repMs)} over ${node.repNodes.length} span(s)` : "—"}
        </span>
      </div>

      {instances.length > 1 && (
        <div className="uvinst">
          <span className="hint">this span ran {instances.length} time(s) — pick one to link to:</span>
          {instances.map((i) => (
            <button
              key={`${i.side}${i.nodeId}`}
              type="button"
              className={
                selected.nodeId === i.nodeId && selected.side === i.side
                  ? "instchip active"
                  : "instchip"
              }
              onClick={() => onPick(i.nodeId, i.side)}
            >
              {i.side === "rec" ? "rec" : "rep"} #{i.nodeId}
            </button>
          ))}
        </div>
      )}

      <div className="uvdbody">
        {empty && <NoEvidence n={node} />}
        {node.http.map((d, i) => (
          <Response key={`h${i}`} d={d} />
        ))}
        {node.calls.map((e, i) => {
          const k = e.call.kind;
          if (k === "value_diverged") return <ValueDiverged key={i} e={e} />;
          if (k === "omitted") return <Omitted key={i} e={e} />;
          if (k === "environmental")
            return <Environmental key={i} e={e} note={boundaryNotes[e.call.boundary] ?? null} />;
          if (k === "novel") return <Novel key={i} e={e} />;
          return <Matched key={i} e={e} />;
        })}
        <HiddenList label="record-side framing/internal" groups={node.hiddenRec} />
        <HiddenList label="replay-side framing/internal" groups={node.hiddenRep} />
      </div>
    </aside>
  );
}

/* ------------------------------------------------------------------- rows  */

function Row({
  n,
  selected,
  collapsed,
  showHidden,
  onSelect,
  onToggle,
}: {
  n: SpineNode;
  selected: boolean;
  collapsed: boolean;
  showHidden: boolean;
  onSelect: (n: SpineNode) => void;
  onToggle: (key: string) => void;
}) {
  const hid = n.hiddenRec.reduce((a, g) => a + g.count, 0) + n.hiddenRep.reduce((a, g) => a + g.count, 0);
  return (
    <div
      className={`uvrow${selected ? " sel" : ""}${n.mark ? ` m-${n.mark}` : ""}`}
      role="button"
      tabIndex={0}
      onClick={() => onSelect(n)}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onSelect(n);
        }
      }}
    >
      <span className="uvindent" style={{ width: n.depth * 11 }} />
      <span
        className="caret"
        onClick={(e) => {
          e.stopPropagation();
          if (n.children.length) onToggle(n.key);
        }}
      >
        {n.children.length ? (collapsed ? "▸" : "▾") : ""}
      </span>
      <span className="uvname">{n.name}</span>
      {n.mark && <span className={`mchip m-${n.mark}`}>{MARK_LABEL[n.mark]}</span>}
      {n.calls.length > 1 && <span className="uvn">×{n.calls.length}</span>}
      {n.presence === "record-only" && <span className="presdot p-rec" title={PRESENCE_LABEL["record-only"]} />}
      {n.presence === "replay-only" && <span className="presdot p-rep" title={PRESENCE_LABEL["replay-only"]} />}
      {showHidden && hid > 0 && <span className="uvhid" title="framing / internal spans beneath">+{hid}</span>}
      <span className="uvdur rec">{fmtMs(n.recMs)}</span>
      <span className="uvdur rep">{fmtMs(n.repMs)}</span>
    </div>
  );
}

/* ------------------------------------------------------------------- view  */

export default function UnifiedView({
  runId,
  scorecard,
}: {
  runId: string;
  scorecard: Scorecard | null;
}) {
  const [params, setParams] = useSearchParams();
  const [collapsed, setCollapsed] = React.useState<Set<string>>(new Set());
  const [onlyMarked, setOnlyMarked] = React.useState(false);
  const [showHidden, setShowHidden] = React.useState(false);

  const calls = useQuery({ queryKey: ["calls", runId], queryFn: () => api.calls(runId) });
  const https = useQuery({ queryKey: ["httpdiffs", runId], queryFn: () => api.httpDiffs(runId) });
  const graph = useQuery({ queryKey: ["graph", runId], queryFn: () => api.graph(runId) });

  const model = React.useMemo(
    () => buildSpine(calls.data ?? [], graph.data, https.data ?? []),
    [calls.data, graph.data, https.data],
  );

  const boundaryNotes = React.useMemo(() => {
    const out: Record<string, string | null> = {};
    for (const [k, v] of Object.entries(scorecard?.per_boundary ?? {}))
      out[k] = typeof v.note === "string" ? v.note : null;
    return out;
  }, [scorecard]);

  const caseId = params.get("case");
  const active: CaseModel | undefined =
    model.cases.find((c) => c.caseId === caseId) ?? model.cases[0];

  // Selection is `?case=<correlation>&node=<graph_node_id>&side=<rec|rep>`.
  // NOT span_path: a span path names a SITE, and 36 of 168 sites here hold more
  // than one instance. When a row is not node-addressable the link falls back to
  // the case root, which is exactly and only where that happens — the graph
  // chain is always a suffix of span_path, so unaddressable rows are the
  // shallowest ones (1 of 325 on this run: the `HTTP request` root).
  const wantNode = params.get("node") ? Number(params.get("node")) : null;
  const wantSide: Side = params.get("side") === "rep" ? "rep" : "rec";
  const selectedNode: SpineNode | undefined =
    (wantNode != null
      ? (active?.byNode[wantSide].get(wantNode) ?? active?.byNode[wantSide === "rec" ? "rep" : "rec"].get(wantNode))
      : undefined) ?? active?.root;

  // A deep link must be able to see what it addressed: opening one expands its
  // ancestors rather than leaving the row hidden inside a collapsed branch.
  React.useEffect(() => {
    if (!selectedNode) return;
    const anc = ancestorKeys(selectedNode);
    setCollapsed((prev) => {
      if (!anc.some((k) => prev.has(k))) return prev;
      const next = new Set(prev);
      for (const k of anc) next.delete(k);
      return next;
    });
  }, [selectedNode?.key]);

  const select = React.useCallback(
    (n: SpineNode, pick?: { nodeId: number; side: Side }) => {
      const next = new URLSearchParams(params);
      next.set("case", n.caseId);
      // Default to the record side when the row has one: it is the side that
      // holds the baseline, and the side that exists for the 35 record-only
      // sites where the replay side is what is missing.
      const chosen =
        pick ??
        (n.recNodes.length
          ? { nodeId: n.recNodes[0], side: "rec" as Side }
          : n.repNodes.length
            ? { nodeId: n.repNodes[0], side: "rep" as Side }
            : null);
      if (chosen) {
        next.set("node", String(chosen.nodeId));
        next.set("side", chosen.side);
      } else {
        next.delete("node");
        next.delete("side");
      }
      setParams(next, { replace: true });
    },
    [params, setParams],
  );

  if (calls.isLoading || https.isLoading || graph.isLoading)
    return <p className="hint">loading execution…</p>;
  if (calls.error) return <p className="err">{String(calls.error)}</p>;
  // The tree below is spined on the call ledger, so it renders without the
  // graph — but a missing graph changes what the view can claim (no span
  // durations, no hidden-span counts), and silence here once let a wholly
  // missing record graph read as an ordinary run. Say it, with the server's
  // stated reason when there is one.
  const graphNote =
    (graph.data?.record_note ?? null) ||
    (graph.error ? `the graph could not be read: ${String(graph.error)}` : null);
  const graphBanner =
    model.graphMissing || graphNote ? (
      <p className="hint uv-graphgap">
        The execution graph is not available for this run
        {graphNote ? ` — ${graphNote}` : " — the run published no graph nodes"}. The tree below
        is built from the call ledger, so structure and evidence are complete; span timings and
        span-only rows are what is missing.
      </p>
    ) : null;
  if (model.cases.length === 0)
    return (
      <p className="hint">
        No reconciled calls and no response diffs were published for this run, so there is no
        execution to compare. That is a statement about the artifacts, not about the candidate.
      </p>
    );
  if (!active) return null;

  const rows = flatten(active.root, collapsed, onlyMarked);
  const hidTotal = model.hiddenTotals.rec + model.hiddenTotals.rep;
  const unattrib = model.unattributed.reduce((a, u) => a + u.total, 0);

  return (
    <div className="uv">
      {graphBanner}
      <div className="uvtabs">
        {model.cases.map((c) => {
          const on = c.caseId === active.caseId;
          const bad = MARK_ORDER.filter((m) => m !== "matched").reduce(
            (a, m) => a + (c.counts[m] ?? 0),
            0,
          );
          return (
            <button
              key={c.caseId}
              type="button"
              className={on ? "uvtab active" : "uvtab"}
              onClick={() => select(c.root)}
              title={c.caseId}
            >
              <b>{c.caseId.slice(0, 8)}</b>
              <span className="hint">
                {c.rows} spans{bad > 0 ? ` · ${bad} finding${bad === 1 ? "" : "s"}` : ""}
              </span>
            </button>
          );
        })}
      </div>

      <p className="uvexpl">
        One request per tab. The tree is built from the <b>call ledger</b> — <code>span_path</code>{" "}
        and <code>graph_node_id</code> are on every call side, and the ledger keeps ancestry the
        execution graph has lost. Durations come from the graph, hung off the ledger by node id.
      </p>

      <div className="uvtoolbar">
        <label className="toggle">
          <input
            type="checkbox"
            checked={onlyMarked}
            onChange={(e) => setOnlyMarked(e.target.checked)}
          />
          only paths with a finding
        </label>
        <label className="toggle">
          <input
            type="checkbox"
            checked={showHidden}
            onChange={(e) => setShowHidden(e.target.checked)}
          />
          reveal framing &amp; transport spans
        </label>
        <span className="hint">
          {rows.length} of {active.rows} spans shown
        </span>
      </div>

      {/* Never silently: the count, the composition, and the reason, always on
          screen. Revealed they are COUNTS AND LISTS, not rows — 4,268 extra
          rows would reproduce the failure this view exists to fix, and the
          3,082 unattributable ones have no honest place in any request's tree
          because graph nodes carry no correlation id. */}
      <p className="uvhidden">
        <b>{hidTotal.toLocaleString()}</b> execution-graph spans are not on any boundary path and are
        not rows here: <b>{model.hiddenTotals.transport.toLocaleString()}</b> transport and framing
        (HTTP/2, hyper, AWS SDK) and <b>{model.hiddenTotals.internal.toLocaleString()}</b> internal
        application spans that made no instrumented call.
        {unattrib > 0 && (
          <>
            {" "}
            <b>{unattrib.toLocaleString()}</b> of them belong to no request at all — the ledger
            anchors nothing in their trees and graph nodes carry no correlation id, so they cannot be
            placed.
          </>
        )}{" "}
        Tick <i>reveal framing &amp; transport spans</i> to count them per span, or open a span to
        list them by name and duration.
      </p>

      {showHidden && model.unattributed.length > 0 && (
        <div className="uvunattr">
          {model.unattributed.map((u) => (
            <details key={u.side}>
              <summary>
                {u.total.toLocaleString()} {u.side === "rec" ? "record" : "replay"}-side spans
                attributable to no request
              </summary>
              <table className="hiddentbl">
                <tbody>
                  {u.groups.slice(0, 25).map((g) => (
                    <tr key={`${g.name}|${g.target}`}>
                      <td className="mono">{g.name}</td>
                      <td className="hint">{g.target}</td>
                      <td className="num">×{g.count}</td>
                      <td className="num">{fmtMs(g.ms)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {u.groups.length > 25 && (
                <p className="hint">and {u.groups.length - 25} more span names.</p>
              )}
            </details>
          ))}
        </div>
      )}

      <div className="uvbody">
        <div className="uvtree">
          <div className="uvhdr">
            <span className="uvname">span</span>
            <span className="uvdur rec">record</span>
            <span className="uvdur rep">replay</span>
          </div>
          {rows.length === 0 && <p className="hint uvempty">no span on a path to a finding.</p>}
          {rows.map((n) => (
            <Row
              key={n.key}
              n={n}
              selected={selectedNode?.key === n.key}
              collapsed={collapsed.has(n.key)}
              showHidden={showHidden}
              onSelect={(x) => select(x)}
              onToggle={(k) =>
                setCollapsed((prev) => {
                  const next = new Set(prev);
                  if (next.has(k)) next.delete(k);
                  else next.add(k);
                  return next;
                })
              }
            />
          ))}
        </div>
        {selectedNode && (
          <SpanDetail
            node={selectedNode}
            selected={{ nodeId: wantNode, side: wantSide }}
            onPick={(nodeId, side) => select(selectedNode, { nodeId, side })}
            boundaryNotes={boundaryNotes}
          />
        )}
      </div>
    </div>
  );
}
