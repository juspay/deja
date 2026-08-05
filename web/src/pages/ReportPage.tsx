import React from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { api, ArtifactRow, CallRecord, HttpDiff, RunRow, StageRow, runParams } from "../lib/api";
import { candidateRef, resultOf, RunResult } from "../lib/result";
import { useDebug, withDebug } from "../lib/debug";
import { VerdictBanner } from "../components/Result";
import { ConfidenceBadge, ConfidenceLadder, overallConfidence } from "../components/Confidence";
import { KillRun } from "../components/KillRun";
import UnifiedView from "../components/UnifiedView";
import { Side, transportFailure } from "../lib/spine";

/* ---------------------------------------------------------------- header --- */

function CopyButton({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = React.useState(false);
  return (
    <button
      type="button"
      className="btn"
      onClick={() => {
        void navigator.clipboard.writeText(text).then(() => {
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1600);
        });
      }}
    >
      {copied ? "copied" : label}
    </button>
  );
}

function took(run: RunRow): string | null {
  if (!run.started_at || !run.finished_at) return null;
  const ms = new Date(run.finished_at).getTime() - new Date(run.started_at).getTime();
  return ms < 1000 ? "<1s" : `${(ms / 1000).toFixed(0)}s`;
}

function RunHeader({ run }: { run: RunRow }) {
  const cand = candidateRef(run);
  const params = runParams(run);
  // What was SCORED, else what was ASKED FOR. The scorecard's scope is the
  // stronger claim (it names the cases the verdict judged), but a run that
  // never scored still knows what it was scoped to, because the request is on
  // the row. `null` only for rows created before requests were persisted.
  const scored = run.scorecard?.correlation_scope;
  const scope = scored ?? params?.correlation_filter ?? null;
  const scopeSource = scored ? "as scored" : "as requested";
  const wholeSession = !scope && params !== null;
  const dur = took(run);
  return (
    <div className="rhead">
      <div className="rhead-top">
        <code className="rhead-id">{run.run_id}</code>
        <CopyButton text={`${window.location.origin}/r/${run.run_id}`} label="copy link" />
        {/* Renders itself only while the run is still running, and stays to
            report what the kill removed once it is not. */}
        <KillRun run={run} />
      </div>
      <dl className="rhead-facts">
        <div>
          <dt>candidate</dt>
          <dd className="mono" title={cand.full}>{cand.label}</dd>
        </div>
        <div>
          <dt>recording</dt>
          <dd className="mono">{run.recording_id ?? "—"}</dd>
        </div>
        <div>
          <dt>scope</dt>
          {/* "whole session" is only sayable when the row carries the request
              that says so. Without one — rows created before requests were
              persisted — the scope is genuinely unknown, and claiming either
              answer would be the class of unsupported claim this page exists to
              remove. */}
          <dd title={scope?.join("\n")}>
            {scope
              ? `${scope.length} correlation${scope.length === 1 ? "" : "s"} (${scopeSource})`
              : wholeSession
                ? "whole session"
                : "not recorded"}
          </dd>
        </div>
        <div>
          <dt>run by</dt>
          <dd>{run.created_by}</dd>
        </div>
        <div>
          <dt>when</dt>
          <dd title={new Date(run.created_at).toISOString()}>
            {new Date(run.created_at).toLocaleString()}
            {dur ? ` · ${dur}` : ""}
          </dd>
        </div>
      </dl>
    </div>
  );
}

/* -------------------------------------------------------------- findings --- */

type FindingRank = "origin" | "consequence" | "http" | "omitted" | "novel" | "environmental";

type Finding = {
  rank: FindingRank;
  correlation: string | null;
  title: string;
  where: string;
  span: string;
  /**
   * Where this finding lives in the unified view. A finding is an ANCHOR, not a
   * copy of the evidence — clicking it selects the span below and the evidence
   * renders once, in the detail panel. `nodeId` is null only for response
   * findings, which carry correlation_id / request_path / request_sequence and
   * no span path at all, so the case root is the deepest honest target.
   */
  nodeId: number | null;
  side: Side;
};

// The order a reader should meet them: what changed, what that changed, what the
// caller saw, then what did not happen, then what was never the candidate's
// fault. The last two are consequences of the first, not independent findings.
const RANKS: FindingRank[] = ["origin", "consequence", "http", "omitted", "novel", "environmental"];

const RANK_BLURB: Record<FindingRank, string> = {
  origin:
    "an Execute boundary ran during replay and returned a value that differs from the recording — the cause",
  consequence: "a call downstream of an origin, carrying the changed value",
  http: "what the caller saw: the recorded response and the replayed response differ",
  omitted:
    "the recording made this call and the candidate did not. NOT a claim the span never ran — /calls carries no span-presence evidence",
  novel: "the candidate made a call the recording did not",
  environmental:
    "an environment miss, not a candidate change (egress is blocked in the replay sandbox)",
};

function leafOf(path?: string): string {
  return path ? path.split(">").pop()! : "";
}
function siteOf(c: CallRecord): string {
  const s = c.observed ?? c.recorded;
  return s?.call_file ? `${s.call_file}:${s.call_line ?? "?"}` : "";
}
function spanOf(c: CallRecord): string {
  return c.observed?.span_path ?? c.recorded?.span_path ?? "";
}

function buildFindings(calls: CallRecord[], https: HttpDiff[]): Finding[] {
  const out: Finding[] = [];
  for (const c of calls) {
    let rank: FindingRank | null = null;
    if (c.kind === "value_diverged") rank = c.origin ? "origin" : "consequence";
    else if (c.kind === "omitted") rank = "omitted";
    else if (c.kind === "novel") rank = "novel";
    else if (c.kind === "environmental") rank = "environmental";
    if (!rank) continue; // matched / recovered / deterministic are not findings
    // Anchor on the side that owns the evidence: the recording for a call the
    // candidate did not make, the candidate for one the recording did not. The
    // two sides carry DIFFERENT node ids — on all 18 value-divergence rows here
    // they differ — so the side must travel with the id, never be inferred.
    const rec = c.recorded?.graph_node_id ?? null;
    const rep = c.observed?.graph_node_id ?? null;
    const order: [number | null, Side][] =
      rank === "novel" || rank === "environmental"
        ? [[rep, "rep"], [rec, "rec"]]
        : [[rec, "rec"], [rep, "rep"]];
    const [nodeId, side] = order.find(([id]) => id != null) ?? [null, "rec" as Side];
    out.push({
      rank,
      correlation: c.correlation_id ?? null,
      title: `${c.boundary}·${c.method_name}`,
      where: siteOf(c),
      span: spanOf(c),
      nodeId,
      side,
    });
  }
  for (const d of https) {
    if (d.status_match && d.body_diff.length === 0) continue;
    const failed = transportFailure(d);
    out.push({
      rank: "http",
      correlation: d.correlation_id,
      title: `${d.request_path} · seq ${d.request_sequence}`,
      // ONE failure, not 54 fields. When the candidate produced no parseable
      // response every recorded field is reported as differing; saying "54
      // fields differ" would present one failure as 54.
      where: failed
        ? `no response — ${failed}`
        : d.status_match
          ? `${d.body_diff.length} field${d.body_diff.length === 1 ? "" : "s"} differ`
          : `${d.status_baseline} → ${d.status_candidate}`,
      span: "",
      nodeId: null,
      side: "rep",
    });
  }
  return out.sort((a, b) => RANKS.indexOf(a.rank) - RANKS.indexOf(b.rank));
}

function FindingRow({ f, onOpen }: { f: Finding; onOpen: (f: Finding) => void }) {
  return (
    <li className={`finding rank-${f.rank}`}>
      <button className="finding-head" type="button" onClick={() => onOpen(f)}>
        <span className="fcaret">→</span>
        <span className={`fchip rank-${f.rank}`}>{f.rank}</span>
        <span className="ftitle mono">{f.title}</span>
        {f.span && <span className="fspan">@ {leafOf(f.span)}</span>}
        <span className="fwhere mono">{f.where}</span>
        {f.correlation && <span className="fcorr mono">{f.correlation.slice(0, 8)}</span>}
      </button>
    </li>
  );
}

function FindingList({ findings, onOpen }: { findings: Finding[]; onOpen: (f: Finding) => void }) {
  const [show, setShow] = React.useState<FindingRank | "all">("all");
  const counts = React.useMemo(() => {
    const m = new Map<FindingRank, number>();
    for (const f of findings) m.set(f.rank, (m.get(f.rank) ?? 0) + 1);
    return m;
  }, [findings]);
  const rows = show === "all" ? findings : findings.filter((f) => f.rank === show);

  if (findings.length === 0)
    return <p className="hint">no divergence rows were published for this run.</p>;

  return (
    <>
      <div className="ranktabs">
        <button
          type="button"
          className={show === "all" ? "ranktab active" : "ranktab"}
          onClick={() => setShow("all")}
        >
          all {findings.length}
        </button>
        {RANKS.filter((r) => counts.get(r)).map((r) => (
          <button
            key={r}
            type="button"
            className={show === r ? `ranktab rank-${r} active` : `ranktab rank-${r}`}
            onClick={() => setShow(r)}
            title={RANK_BLURB[r]}
          >
            {r} {counts.get(r)}
          </button>
        ))}
      </div>
      {show !== "all" && <p className="hint">{RANK_BLURB[show]}</p>}
      <p className="hint">Click a finding to open it in the execution view below.</p>
      <ul className="findings">
        {rows.slice(0, 200).map((f, i) => <FindingRow key={i} f={f} onOpen={onOpen} />)}
      </ul>
      {rows.length > 200 && (
        <p className="hint">showing the first 200 of {rows.length}.</p>
      )}
    </>
  );
}

/* ----------------------------------------------------------------- trust --- */

/**
 * What the verdict rests on. Everything here comes from the run's own embedded
 * scorecard — including `warnings` and `correlation_scope`, which the server has
 * always emitted and which nothing rendered before this.
 */
function TrustStrip({ run }: { run: RunRow }) {
  const sc = run.scorecard;
  const rec = useQuery({ queryKey: ["recordings"], queryFn: api.recordings });
  if (!sc) return null;
  const s = sc.summary;
  const scope = sc.correlation_scope ?? [];
  const source = rec.data?.find((r) => r.recording_id === (run.recording_id ?? "").trim());
  const conf = overallConfidence(s.resolved_by_rank ?? {});

  return (
    <div className="trust">
      <div className="trustrow">
        <span className="tkey">scope</span>
        <span className="tval">
          {scope.length > 0 ? (
            <>
              <b>{scope.length}</b> correlation{scope.length === 1 ? "" : "s"} driven
              {source?.correlation_count
                ? <> out of <b>{source.correlation_count.toLocaleString()}</b> in the recording</>
                : null}
              {source?.correlation_count
                ? <span className="hint"> — the verdict judges the driven subset only</span>
                : null}
            </>
          ) : (
            <>the whole session</>
          )}
        </span>
      </div>
      <div className="trustrow">
        <span className="tkey">matching</span>
        <span className="tval">
          <b>{(s.matched_side_effect_calls ?? 0).toLocaleString()}</b> side-effect calls reconciled ·{" "}
          <ConfidenceBadge level={conf} title="the weakest address rank this run relied on" />
        </span>
      </div>
      {(sc.warnings ?? []).map((w, i) => (
        <div className="trustrow warn" key={i}>
          <span className="tkey">note</span>
          <span className="tval">{w}</span>
        </div>
      ))}
    </div>
  );
}

/* --------------------------------------------------------------- summary --- */

function Counters({ run }: { run: RunRow }) {
  const s = run.scorecard?.summary;
  if (!s) return null;
  const vd = s.value_divergences ?? 0;
  const cells: [number | string, string, boolean?][] = [
    [vd, "value divergences", vd > 0],
    [`${s.matched_correlations}/${s.total_correlations}`, "requests reproduced"],
    [s.http_status_mismatches, "http status mismatches"],
    [s.http_body_mismatches, "http body mismatches"],
    [s.side_effect_divergences, "side-effect divergences"],
    [s.omitted_calls ?? 0, "omitted calls"],
    [s.novel_calls ?? 0, "novel calls"],
    [s.inconclusive_seed_gaps ?? 0, "inconclusive seed gaps"],
  ];
  return (
    <div className="panel grid">
      {cells.map(([v, k, hot]) => (
        <div className={`metric${hot ? " hot" : ""}`} key={k}>
          <div className="v">{v}</div>
          <div className="k">{k}</div>
        </div>
      ))}
    </div>
  );
}

function BoundaryTable({ run }: { run: RunRow }) {
  const entries = Object.entries(run.scorecard?.per_boundary ?? {});
  if (entries.length === 0) return null;
  return (
    <table>
      <thead>
        <tr>
          <th>boundary</th><th>tier</th><th className="num">matched</th>
          <th className="num">diverged</th><th>kinds</th>
        </tr>
      </thead>
      <tbody>
        {entries.map(([name, b]) => (
          <tr key={name}>
            <td className="mono">{name}</td>
            <td>{b.tier ?? "—"}</td>
            <td className="num">{b.matched ?? 0}</td>
            <td className="num">
              {b.diverged ? <span className="chip fail">{b.diverged}</span> : 0}
            </td>
            <td>
              {Object.entries(b.kinds ?? {}).filter(([, n]) => n > 0).map(([k, n]) => (
                <span key={k} className="chip muted" style={{ marginRight: 4 }}>{k}: {n}</span>
              ))}
              {typeof b.note === "string" && <div className="hint">{b.note}</div>}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function CorrelationTable({ run }: { run: RunRow }) {
  const rows = run.scorecard?.per_correlation ?? [];
  if (rows.length === 0) return null;
  return (
    <table>
      <thead>
        <tr>
          <th>request (correlation)</th><th>http status</th><th>http body</th>
          <th className="num">side-effects</th><th>outcome</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={row.correlation_id}>
            <td className="mono">{row.correlation_id}</td>
            <td><span className={`chip ${row.http_status_match ? "pass" : "fail"}`}>{row.http_status_match ? "match" : "mismatch"}</span></td>
            <td><span className={`chip ${row.http_body_match ? "pass" : "fail"}`}>{row.http_body_match ? "match" : "mismatch"}</span></td>
            <td className="num">{row.side_effect_divergences ?? 0}</td>
            <td><span className={`chip ${row.passed ? "pass" : "fail"}`}>{row.passed ? "reproduced" : "diverged"}</span></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/* ----------------------------------------------------------------- debug --- */

function StageTimeline({ stages }: { stages: StageRow[] }) {
  if (stages.length === 0) return <p className="hint">no stage history</p>;
  return (
    <div className="stages">
      {stages.map((s) => {
        const ms =
          (s.finished_at ? new Date(s.finished_at).getTime() : Date.now()) -
          new Date(s.started_at).getTime();
        return (
          <div className="stagebar" key={s.id}>
            <span className={`chip ${s.status}`}>{s.status}</span>
            <span className="name">
              {s.step != null && s.steps_total ? `[${s.step}/${s.steps_total}] ` : ""}{s.stage}
            </span>
            <span className="dur">{ms < 1000 ? "<1s" : `${(ms / 1000).toFixed(0)}s`}</span>
          </div>
        );
      })}
    </div>
  );
}

function ArtifactTable({ list }: { list: ArtifactRow[] }) {
  if (list.length === 0) return <p className="hint">no artifacts registered</p>;
  return (
    <table>
      <thead><tr><th>kind</th><th className="num">bytes</th><th>uri</th><th /></tr></thead>
      <tbody>
        {list.map((a) => (
          <tr key={a.id}>
            <td className="mono">{a.kind}</td>
            <td className="num">{a.bytes?.toLocaleString() ?? "—"}</td>
            <td className="hint" title={a.uri}>{a.uri.length > 64 ? `…${a.uri.slice(-64)}` : a.uri}</td>
            <td><a href={`/api/v1/artifacts/${a.id}/raw`} target="_blank" rel="noreferrer">open</a></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Logs({ runId, active }: { runId: string; active: boolean }) {
  const logs = useQuery({
    queryKey: ["logs", runId],
    queryFn: () => api.logs(runId),
    refetchInterval: active ? 2000 : false,
  });
  const text = logs.data?.map((c) => c.lines).join("\n") ?? (logs.isLoading ? "loading…" : "");
  return <pre className="log">{text || "no logs captured"}</pre>;
}

function DebugSection({ run, result }: { run: RunRow; result: RunResult }) {
  const runId = run.run_id;
  const active = result.state === "RUNNING";
  const stages = useQuery({
    queryKey: ["stages", runId],
    queryFn: () => api.stages(runId),
    refetchInterval: active ? 2000 : false,
  });
  const artifacts = useQuery({
    queryKey: ["artifacts", runId],
    queryFn: () => api.artifacts(runId),
    enabled: !active,
  });
  return (
    <section className="debugzone">
      <h2>Raw sources <span className="hint">(?debug=1)</span></h2>
      <details open><summary>stages</summary><StageTimeline stages={stages.data ?? []} /></details>
      <details><summary>logs</summary><Logs runId={runId} active={active} /></details>
      <details><summary>artifacts</summary><ArtifactTable list={artifacts.data ?? []} /></details>
      {/* The raw execution graph is no longer a separate view: it is the
          enrichment layer of the unified view above, where it can be read
          against the ledger that gives it structure. */}
      <details>
        <summary>run row (json)</summary>
        <pre className="log">{JSON.stringify(run, null, 2)}</pre>
      </details>
    </section>
  );
}

/* ---------------------------------------------------------------- report --- */

/**
 * THE REPORT. `/r/:runId` — the shareable link.
 *
 * It opens on a summary, not a tab bar: result, then what the result rests on,
 * then the ranked findings, then the evidence. Everything a reader needs to
 * decide whether to trust the verdict is above the first fold; nothing they need
 * is behind `?debug=1`, which only adds raw sources.
 */
export default function ReportPage() {
  const { runId = "" } = useParams();
  const debug = useDebug();
  const [, setParams] = useSearchParams();

  const run = useQuery({
    queryKey: ["run", runId],
    queryFn: () => api.run(runId),
    refetchInterval: (q) =>
      q.state.data && ["completed", "failed"].includes(q.state.data.state) ? false : 2000,
  });
  const isReplay = run.data?.mode === "replay";
  const result = run.data ? resultOf(run.data) : null;
  const scored = !!run.data?.scorecard;

  const calls = useQuery({
    queryKey: ["calls", runId],
    queryFn: () => api.calls(runId),
    enabled: isReplay && scored,
  });
  const https = useQuery({
    queryKey: ["httpdiffs", runId],
    queryFn: () => api.httpDiffs(runId),
    enabled: isReplay && scored,
  });

  const findings = React.useMemo(
    () => buildFindings(calls.data ?? [], https.data ?? []),
    [calls.data, https.data],
  );

  // A finding is an anchor into the one execution view — the same
  // `?case=&node=&side=` selection a shared URL carries.
  const openFinding = React.useCallback(
    (f: Finding) => {
      setParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          if (f.correlation) next.set("case", f.correlation);
          if (f.nodeId != null) {
            next.set("node", String(f.nodeId));
            next.set("side", f.side);
          } else {
            next.delete("node");
            next.delete("side");
          }
          return next;
        },
        { replace: true },
      );
      document.getElementById("execution")?.scrollIntoView({ behavior: "smooth", block: "start" });
    },
    [setParams],
  );

  if (run.isLoading) return <p className="hint">loading…</p>;
  if (run.error || !run.data || !result) return <p className="err">{String(run.error)}</p>;
  const r = run.data;

  return (
    <>
      <div className="crumb">
        <Link to={withDebug("/runs", debug)}>Runs</Link> <span>/</span>{" "}
        <span className="hint">report</span>
      </div>

      <RunHeader run={r} />
      <VerdictBanner result={result} />

      {isReplay && scored && (
        <>
          <section>
            <h2>Summary</h2>
            <TrustStrip run={r} />
            <Counters run={r} />
          </section>

          <section>
            <h2>What diverged</h2>
            <FindingList findings={findings} onOpen={openFinding} />
          </section>

          <section>
            <h2>Per request</h2>
            <CorrelationTable run={r} />
          </section>

          <section>
            <h2>Per boundary</h2>
            <BoundaryTable run={r} />
            <h3>Resolution confidence</h3>
            <div className="panel">
              <ConfidenceLadder byRank={r.scorecard?.summary.resolved_by_rank ?? {}} />
              <p className="hint" style={{ marginTop: "var(--s3)" }}>
                Confidence is the weakest address rank a match relied on. CERTAIN/HIGH are
                version-independent; UNMATCHED (positional) survived on ordering alone.
              </p>
            </div>
          </section>

          {/* ONE VIEW. The execution graph and the response/side-effect diff
              were two tabs answering halves of the same question — "where did
              this go wrong" and "what exactly changed there". They are one
              tree with one detail panel now. */}
          <section id="execution">
            <h2>Execution — recorded against replayed</h2>
            <UnifiedView runId={runId} scorecard={r.scorecard} />
          </section>
        </>
      )}

      {isReplay && !scored && (
        <p className="hint">
          Nothing was scored, so there is no evidence to report. The failure above is all that is
          known — add <code>?debug=1</code> to this URL for the stage history and logs.
        </p>
      )}

      {debug && <DebugSection run={r} result={result} />}
      {!debug && (
        <p className="hint debughint">
          Need the raw sources? Add <code>?debug=1</code> to this URL for stages, logs, artifacts
          and the execution graph.
        </p>
      )}
    </>
  );
}
