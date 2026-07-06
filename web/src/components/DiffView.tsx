import React from "react";
import { useQuery } from "@tanstack/react-query";
import { api, CallRecord, HttpDiff } from "../lib/api";
import { JsonView, ValuePair } from "./JsonView";
import { JsonDiff } from "./JsonDiff";
import { diffArgs, LeafDiff } from "../lib/argdiff";
import { ConfidenceBadge, overallConfidence, levelForRank } from "./Confidence";

function leaf(path?: string): string {
  return path ? path.split(">").pop()! : "";
}
function where(c: CallRecord): string {
  const s = c.observed ?? c.recorded;
  return s?.call_file ? `${s.call_file}:${s.call_line ?? "?"}` : "";
}
function spanOf(c: CallRecord): string {
  return c.observed?.logical_span_path ?? c.recorded?.logical_span_path ?? "";
}

type Group = { key: string; boundary: string; method: string; span: string; novel?: CallRecord; omitted?: CallRecord };

function groupDivergences(calls: CallRecord[]) {
  const by = new Map<string, Group>();
  for (const c of calls) {
    if (c.kind !== "novel" && c.kind !== "omitted") continue;
    const span = spanOf(c);
    const key = `${span}|${c.boundary}|${c.method_name}`;
    const g = by.get(key) ?? { key, boundary: c.boundary, method: c.method_name, span };
    if (c.kind === "novel") g.novel = c;
    else g.omitted = c;
    by.set(key, g);
  }
  const modified: Group[] = [], added: Group[] = [], removed: Group[] = [];
  for (const g of by.values()) {
    if (g.novel && g.omitted) modified.push(g);
    else if (g.novel) added.push(g);
    else removed.push(g);
  }
  return { modified, added, removed };
}

function isEnvironmental(g: Group): boolean {
  return leaf(g.span) === "get_or_populate_redis" || g.method === "get_or_populate_redis";
}

function LeafDiffRow({ d }: { d: LeafDiff }) {
  return (
    <div className="leafdiff">
      <div className="jpath">{d.path || "(value)"}</div>
      {d.highlight ? (
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

function ModifiedCard({ g }: { g: Group }) {
  const diffs = diffArgs(g.omitted!.recorded?.args, g.novel!.observed?.args);
  const rank = g.novel!.resolved_rank ?? g.omitted!.resolved_rank;
  return (
    <div className="rootcard">
      <div className="rchead">
        <span className="chip modified">modified</span>
        <span className="bm"><b>{g.boundary}</b>·{g.method}</span>
        <span className="rcspan">@ {leaf(g.span)}</span>
        {rank != null && <ConfidenceBadge level={levelForRank(rank)} title={`resolved at rank ${rank}`} />}
        <span className="where">{where(g.novel!)}</span>
      </div>
      <p className="hint">the candidate made this call with <b>different arguments</b> than the recording — its behavior changed here.</p>
      <div className="argdiff">
        {diffs.length === 0 && <p className="hint">args structurally differ (no leaf delta extracted)</p>}
        {diffs.map((d, i) => <LeafDiffRow key={i} d={d} />)}
      </div>
      <details className="spandetails">
        <summary>call path</summary>
        <code className="spanpath">{g.span.replace(/>/g, " › ")}</code>
      </details>
    </div>
  );
}

function CascadeRow({ g, kind }: { g: Group; kind: "added" | "removed" }) {
  const c = (kind === "added" ? g.novel : g.omitted)!;
  const env = kind === "added" && isEnvironmental(g);
  return (
    <div className={`callrow kind-${kind === "added" ? "novel" : "omitted"}`}>
      <div className="callhead">
        <span className={`chip ${env ? "muted" : kind}`}>{kind}{env ? " · env" : ""}</span>
        <span className="bm"><b>{g.boundary}</b>·{g.method}</span>
        <span className="rcspan">@ {leaf(g.span)}</span>
        <span className="where">{where(c)}</span>
      </div>
      {kind === "added" && (
        <div className="callbody">
          <span className="lbl added">candidate issued this call — not in the recording{env ? " (cold-cache fallback; likely environmental)" : ""}</span>
          <JsonView value={g.novel!.observed?.args} />
        </div>
      )}
      {kind === "removed" && (
        <div className="callbody">
          <span className="lbl removed">recording made this call — candidate's forked path skipped it</span>
          <div className="kv"><span>args</span><JsonView value={g.omitted!.recorded?.args} /></div>
          <div className="kv"><span>result</span><JsonView value={g.omitted!.recorded?.result} /></div>
        </div>
      )}
    </div>
  );
}

// Reconstruct a per-side body object from changed leaves, for older diffs that
// predate the kernel persisting full bodies.
function reconstruct(d: HttpDiff, side: "baseline" | "candidate"): unknown {
  const obj: Record<string, unknown> = {};
  for (const f of d.body_diff) obj[f.json_path] = (side === "baseline" ? f.baseline : f.candidate);
  return obj;
}

function HttpBlock({ d }: { d: HttpDiff }) {
  const bad = !d.status_match;
  const before = d.baseline_body ?? reconstruct(d, "baseline");
  const after = d.candidate_body ?? reconstruct(d, "candidate");
  const droppedFields = d.body_diff.filter((f) => f.candidate === null).length;
  const isErrorFlip = bad && d.status_candidate >= 400;

  return (
    <div className="httpblock">
      <div className="httphead">
        <span className="method">{d.request_path}</span>
        {bad ? (
          <span className="statuspill"><span className="ok">{d.status_baseline}</span> <span className="sep">→</span> <span className="bad">{d.status_candidate}</span></span>
        ) : (
          <span className="statuspill"><span className="ok">{d.status_baseline}</span> match</span>
        )}
      </div>
      {isErrorFlip ? (
        // Success body wholly replaced by an error — a full LCS diff would be a
        // wall of red, and the error is the point. Show the error response, and
        // offer the recorded success body separately (not a line diff).
        <div className="rollup">
          <p className="hint">the request that returned <b>{d.status_baseline}</b> in the recording now returns <b>{d.status_candidate}</b> — the candidate's error response:</p>
          <JsonView value={after} clamp={false} />
          {droppedFields > 0 && <div className="drop">{droppedFields} recorded response field{droppedFields > 1 ? "s" : ""} no longer present (the success body was replaced by the error).</div>}
          <details className="changed-fields">
            <summary>view the recorded success body</summary>
            <JsonView value={before} clamp={false} />
          </details>
        </div>
      ) : (
        <JsonDiff before={before} after={after} />
      )}
    </div>
  );
}

export default function DiffView({ runId }: { runId: string }) {
  const [showCascade, setShowCascade] = React.useState(false);
  const calls = useQuery({ queryKey: ["calls", runId], queryFn: () => api.calls(runId) });
  const https = useQuery({ queryKey: ["httpdiffs", runId], queryFn: () => api.httpDiffs(runId) });
  const card = useQuery({ queryKey: ["scorecard", runId], queryFn: () => api.scorecard(runId) });

  if (calls.isLoading || https.isLoading || card.isLoading) return <p className="hint">loading diff…</p>;
  if (calls.error) return <p className="err">{String(calls.error)}</p>;

  const all = calls.data ?? [];
  const { modified, added, removed } = groupDivergences(all);
  const httpBad = (https.data ?? []).filter((d) => !d.status_match || d.body_diff.length > 0);
  const cascadeCount = added.length + removed.length;
  const envCount = added.filter(isEnvironmental).length;
  const s = card.data?.summary;
  const v = card.data?.verdict;
  const verdict = v?.inconclusive ? "inconclusive" : v?.pass ? "pass" : "fail";
  const conf = s ? overallConfidence(s.resolved_by_rank ?? {}) : "unmatched";

  const clean = modified.length === 0 && cascadeCount === 0 && httpBad.length === 0;

  return (
    <>
      <div className={`scorestrip ${clean ? "clean" : "diverged"}`}>
        <span className={`chip solid ${verdict}`}>{verdict}</span>
        <ConfidenceBadge level={conf} title="overall run confidence = weakest address rank relied on" />
        <span className="lede">
          {clean
            ? `clean replay — ${s?.matched_correlations ?? 0}/${s?.total_correlations ?? 0} correlations matched, 0 side-effect divergences`
            : modified.length > 0
              ? <>behavior forked at <b>{modified.map((g) => leaf(g.span)).join(", ")}</b> — {modified.length} call{modified.length > 1 ? "s" : ""} with changed arguments</>
              : cascadeCount > 0
                ? <>{cascadeCount} side-effect{cascadeCount > 1 ? "s" : ""} {added.length === 0 ? "dropped" : removed.length === 0 ? "added" : "changed"} (no argument-level change){httpBad.length === 0 ? " — response unchanged" : ""}</>
                : <><b>response value changed</b> — {httpBad.length} request{httpBad.length > 1 ? "s" : ""} differ, every side-effect matched</>}
        </span>
        {!clean && (
          <span className="meta">
            {added.length} added · {removed.length} removed downstream
            {envCount ? <> · <span title="cold-cache fallback — likely infra noise, not a behavior change">{envCount} environmental</span></> : null}
            {" "}· {httpBad.length} response diff{httpBad.length === 1 ? "" : "s"}
          </span>
        )}
      </div>

      {clean && <p className="hint">Every recorded response and side-effect call was reproduced exactly. Confidence is {conf.toUpperCase()} — the weakest address rank this run relied on.</p>}

      {modified.length > 0 && (
        <section>
          <h2>Root cause — changed calls</h2>
          {modified.map((g) => <ModifiedCard key={g.key} g={g} />)}
        </section>
      )}

      {httpBad.length > 0 && (
        <section>
          <h2>Resulting response divergence</h2>
          {httpBad.map((d, i) => <HttpBlock key={i} d={d} />)}
        </section>
      )}

      {cascadeCount > 0 && (
        <section>
          <h2 className="clicky" onClick={() => setShowCascade((x) => !x)}>Downstream cascade ({cascadeCount}) {showCascade ? "▾" : "▸"}</h2>
          <p className="hint">consequences of the fork above — calls the candidate's diverged control flow added or skipped.{envCount > 0 && ` ${envCount} look like cold-cache fallback (environmental).`}</p>
          {showCascade && (
            <div className="calls">
              {added.map((g) => <CascadeRow key={g.key} g={g} kind="added" />)}
              {removed.map((g) => <CascadeRow key={g.key} g={g} kind="removed" />)}
            </div>
          )}
        </section>
      )}
    </>
  );
}
