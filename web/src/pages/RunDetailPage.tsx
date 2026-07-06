import React from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { api, ArtifactRow, Scorecard, StageRow } from "../lib/api";
import DiffView from "../components/DiffView";
import GraphView from "../components/GraphView";
import { ConfidenceBadge, ConfidenceLadder, overallConfidence } from "../components/Confidence";

function duration(s: StageRow): string {
  const start = new Date(s.started_at).getTime();
  const end = s.finished_at ? new Date(s.finished_at).getTime() : Date.now();
  const ms = end - start;
  return ms < 1000 ? "<1s" : `${(ms / 1000).toFixed(0)}s`;
}

function StageTimeline({ stages }: { stages: StageRow[] }) {
  return (
    <div className="stages">
      {stages.map((s) => (
        <div className="stagebar" key={s.id}>
          <span className={`chip ${s.status}`}>{s.status}</span>
          <span className="name">
            {s.step != null && s.steps_total != null && s.steps_total > 0
              ? `[${s.step}/${s.steps_total}] `
              : ""}
            {s.stage}
          </span>
          <span className="dur">{duration(s)}</span>
        </div>
      ))}
      {stages.length === 0 && <p className="hint">no stage history</p>}
    </div>
  );
}

function ScorecardSummary({ runId }: { runId: string }) {
  const card = useQuery<Scorecard>({ queryKey: ["scorecard", runId], queryFn: () => api.scorecard(runId) });
  if (!card.data) return null;
  const s = card.data.summary;
  const v = card.data.verdict;
  const verdict = v.inconclusive ? "inconclusive" : v.pass ? "pass" : "fail";
  const conf = overallConfidence(s.resolved_by_rank ?? {});
  const valueDivergences = s.value_divergences ?? 0;
  const caught = valueDivergences > 0;
  return (
    <>
      <h2>Scorecard</h2>
      <div className={`scorestrip ${verdict === "pass" ? "clean" : "diverged"}`}>
        <span className={`chip solid ${verdict}`}>{verdict}</span>
        {caught && <span className="chip solid fail">CAUGHT</span>}
        <ConfidenceBadge level={conf} title="overall run confidence = weakest address rank relied on" />
        <span className="lede">{v.reason}</span>
      </div>
      <div className="panel grid">
        <div className={`metric${caught ? " hot" : ""}`}><div className="v">{valueDivergences}</div><div className="k">value divergences (total-derivative catches)</div></div>
        <div className="metric"><div className="v">{s.inconclusive_seed_gaps ?? 0}</div><div className="k">inconclusive seed gaps</div></div>
        <div className="metric"><div className="v">{s.matched_correlations}/{s.total_correlations}</div><div className="k">correlations matched</div></div>
        <div className="metric"><div className="v">{s.http_status_mismatches}</div><div className="k">http status mismatches</div></div>
        <div className="metric"><div className="v">{s.http_body_mismatches}</div><div className="k">http body mismatches</div></div>
        <div className="metric"><div className="v">{s.side_effect_divergences}</div><div className="k">side-effect divergences</div></div>
        <div className="metric"><div className="v">{s.omitted_calls ?? 0}</div><div className="k">omitted calls</div></div>
        <div className="metric"><div className="v">{s.novel_calls ?? 0}</div><div className="k">novel calls</div></div>
      </div>
      <h2>Resolution confidence</h2>
      <div className="panel"><ConfidenceLadder byRank={s.resolved_by_rank ?? {}} /></div>
    </>
  );
}

function Artifacts({ list }: { list: ArtifactRow[] }) {
  const viz = list.find((a) => a.kind === "visualization_html");
  return (
    <>
      <table>
        <thead>
          <tr><th>kind</th><th>bytes</th><th>path</th><th /></tr>
        </thead>
        <tbody>
          {list.map((a) => (
            <tr key={a.id}>
              <td>{a.kind}</td>
              <td>{a.bytes?.toLocaleString() ?? "—"}</td>
              <td className="hint" title={a.uri}>{a.uri.length > 60 ? `…${a.uri.slice(-60)}` : a.uri}</td>
              <td><a href={`/api/v1/artifacts/${a.id}/raw`} target="_blank" rel="noreferrer">open</a></td>
            </tr>
          ))}
          {list.length === 0 && (
            <tr><td colSpan={4} className="hint">no artifacts registered (store offline during the run?)</td></tr>
          )}
        </tbody>
      </table>
      {viz && (
        <>
          <h2>Replay visualization</h2>
          <iframe className="viz" title="replay visualization" src={`/api/v1/artifacts/${viz.id}/raw`} />
        </>
      )}
    </>
  );
}

function Logs({ runId, active }: { runId: string; active: boolean }) {
  const logs = useQuery({ queryKey: ["logs", runId], queryFn: () => api.logs(runId), refetchInterval: active ? 2000 : false });
  const ref = React.useRef<HTMLPreElement>(null);
  React.useEffect(() => {
    if (ref.current && active) ref.current.scrollTop = ref.current.scrollHeight;
  }, [logs.data, active]);
  const text = logs.data?.map((c) => c.lines).join("\n") ?? (logs.isLoading ? "loading…" : "");
  return <pre className="log" ref={ref}>{text || "no logs captured"}</pre>;
}

const TABS = ["overview", "diff", "graph", "artifacts"] as const;
type Tab = (typeof TABS)[number];

export default function RunDetailPage() {
  const { runId = "" } = useParams();
  const [params, setParams] = useSearchParams();
  const tab = (params.get("tab") as Tab) || "overview";
  const setTab = (t: Tab) => setParams(t === "overview" ? {} : { tab: t }, { replace: true });

  const run = useQuery({
    queryKey: ["run", runId],
    queryFn: () => api.run(runId),
    refetchInterval: (q) =>
      q.state.data && ["completed", "failed"].includes(q.state.data.state) ? false : 2000,
  });
  const terminal = !!run.data && ["completed", "failed"].includes(run.data.state);
  const stages = useQuery({
    queryKey: ["stages", runId],
    queryFn: () => api.stages(runId),
    refetchInterval: terminal ? false : 2000,
  });
  const artifacts = useQuery({
    queryKey: ["artifacts", runId],
    queryFn: () => api.artifacts(runId),
    enabled: terminal,
  });

  if (run.isLoading) return <p className="hint">loading…</p>;
  if (run.error || !run.data) return <p className="err">{String(run.error)}</p>;
  const r = run.data;
  const active = !terminal;
  const isReplay = r.mode === "replay";

  return (
    <>
      <div className="crumb">
        <Link to="/runs">Runs</Link> <span>/</span> <span className="hint">{r.run_id}</span>{" "}
        <span>/</span> <span>{tab}</span>
      </div>
      <h1>
        {r.mode} run{" "}
        <span className={`chip ${r.state}`}>{r.state}</span>{" "}
        {r.verdict && <span className={`chip solid ${r.verdict}`}>{r.verdict}</span>}
      </h1>

      <nav className="subtabs">
        {TABS.map((t) => {
          // diff/graph only make sense for a scored replay run
          if ((t === "diff" || t === "graph") && !isReplay) return null;
          return (
            <button key={t} className={t === tab ? "subtab active" : "subtab"} onClick={() => setTab(t)}>
              {t}
            </button>
          );
        })}
      </nav>

      {tab === "overview" && (
        <>
          <div className="panel grid">
            <div className="metric"><div className="v">{r.recording_id ?? "—"}</div><div className="k">recording</div></div>
            <div className="metric"><div className="v">{r.created_by}</div><div className="k">actor</div></div>
            <div className="metric"><div className="v">{r.expectation ?? "—"}</div><div className="k">expectation</div></div>
            <div className="metric"><div className="v" title={r.candidate_sha256 ?? undefined}>{r.candidate_sha256 ? r.candidate_sha256.slice(0, 12) : "—"}</div><div className="k">candidate sha256</div></div>
          </div>
          {r.live?.candidate_image && (
            <p className="hint">candidate image: {r.live.candidate_image.docker_image} (from {r.live.candidate_image.source_ref})</p>
          )}
          {(r.failure?.message || r.live?.failure_reason) && (
            <div className="panel"><h2>Failure</h2><pre className="cmd err">{r.failure?.message || r.live?.failure_reason}</pre></div>
          )}
          {isReplay && terminal && <ScorecardSummary runId={runId} />}
          <h2>Stages</h2>
          <StageTimeline stages={stages.data ?? []} />
          <h2>Logs</h2>
          <Logs runId={runId} active={active} />
        </>
      )}

      {tab === "diff" && (terminal ? <DiffView runId={runId} /> : <p className="hint">run still in progress…</p>)}
      {tab === "graph" && (terminal ? <GraphView runId={runId} /> : <p className="hint">run still in progress…</p>)}
      {tab === "artifacts" && <Artifacts list={artifacts.data ?? []} />}
    </>
  );
}
