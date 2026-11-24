import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";
import { api, runParams, type ReviewContext as Context, type RunRow } from "../lib/api";
import { useReviewNavigation } from "../lib/navigation";
import { RunListResult } from "./Result";

export function headFreshness(context?: Context, readError?: unknown): { label: string; detail: string; current: boolean } {
  if (readError) return { label: "Current PR head unavailable", detail: "The latest source-head read failed. Previously cached metadata cannot establish current freshness.", current: false };
  if (!context) return { label: "Verifying current PR head…", detail: "Reading source identity and stored replay history.", current: false };
  const captured = context.github?.head_sha;
  const current = context.current_head;
  if (!captured) return { label: "No captured PR head", detail: "This run does not identify a pull request head.", current: false };
  if (!context.source_revision) return { label: "Candidate source unverified", detail: context.source_unavailable ?? "The captured PR head could not be bound to the declared candidate source.", current: false };
  if (current?.status !== "verified") return { label: "Current PR head unverified", detail: current?.reason ?? "A live source-host read is unavailable. Stored run metadata cannot establish freshness.", current: false };
  if (context.source_revision.sha.toLowerCase() !== current.sha.toLowerCase()) return { label: "Historical PR head", detail: `Current GitHub head is ${current.sha}; checked ${current.checked_at}. This run does not validate that head.`, current: false };
  return { label: "Source matches current PR head", detail: `GitHub head checked ${current.checked_at}. This compares declared source identity; image contents and deployment freshness are not attested.`, current: true };
}

export function ReviewContextPanel({ run }: { run: RunRow }) {
  const gh = runParams(run)?.github;
  const nav = useReviewNavigation();
  const [params, setParams] = useSearchParams();
  const context = useQuery({ queryKey: ["review-context", run.run_id], queryFn: () => api.reviewContext(run.run_id), enabled: !!gh, staleTime: 60000, refetchInterval: 60000 });
  const selectedHead = params.get("review_head") ?? gh?.head_sha ?? "";
  const cursor = params.get("review_cursor") ?? "";
  const attempts = useQuery({
    queryKey: ["pr-attempts", gh?.repo, gh?.pr_number, selectedHead, cursor],
    queryFn: () => api.runsPage({ repo: gh!.repo, pr: gh!.pr_number, head: selectedHead === "all" ? undefined : selectedHead, cursor, limit: 10 }),
    enabled: !!gh,
  });
  const update = (key: string, value: string) => setParams((previous) => {
    const next = new URLSearchParams(previous);
    if (value) next.set(key, value); else next.delete(key);
    if (key === "review_head") next.delete("review_cursor");
    return next;
  });
  if (!gh) return null;
  const freshness = headFreshness(context.data, context.error);
  const heads = context.data?.heads ?? [];
  const latest = context.data?.current_head;
  const extraHead = !context.data?.heads_unavailable && latest?.status === "verified" && !heads.some((head) => head.head_sha === latest.sha) ? latest.sha : null;
  const allRuns = new URLSearchParams({ repo: gh.repo, pr: String(gh.pr_number) });
  return <section className="review-context" aria-label="PR head and replay attempts">
    <div className="review-context-heading">
      <h2><a href={`https://github.com/${gh.repo}/pull/${gh.pr_number}`} target="_blank" rel="noreferrer">{gh.repo} #{gh.pr_number}</a></h2>
      <span className={`chip ${freshness.current ? "good" : "warn"}`}>{freshness.label}</span>
      <button className="btn quiet" disabled={context.isFetching} onClick={() => void context.refetch()}>Refresh head</button>
    </div>
    <p className="hint">Captured PR head <code>{gh.head_sha}</code></p>
    <p className="hint">{freshness.detail}</p>
    {context.error && <p className="err">Current-head evidence unavailable: {String(context.error)}</p>}
    {context.data?.source_revision && <p className="hint">{context.data.source_revision.evidence}</p>}
    <details>
      <summary>Replay attempts and exact baseline pairs</summary>
      <p className="hint">Stored runs for this PR across recordings and systems. A matching source head does not establish an identical engine, policy or deployment.</p>
      {context.data?.heads_unavailable && <p className="err">{context.data.heads_unavailable}</p>}
      <div className="runfilters">
        <label>Captured head <select value={selectedHead} onChange={(event) => update("review_head", event.target.value)}>
          <option value="all">all stored heads</option>
          {!heads.some((head) => head.head_sha === gh.head_sha) && <option value={gh.head_sha}>{gh.head_sha.slice(0, 10)} · this run</option>}
          {heads.map((head) => <option key={head.head_sha} value={head.head_sha}>{head.head_sha.slice(0, 10)} · {head.runs} stored run{head.runs === 1 ? "" : "s"}</option>)}
          {extraHead && extraHead !== gh.head_sha && <option value={extraHead}>{extraHead.slice(0, 10)} · current GitHub head, no stored runs</option>}
        </select></label>
        <Link to={`/runs?${allRuns}`}>Open full PR history →</Link>
      </div>
      {attempts.isLoading && <p className="hint">Loading replay attempts…</p>}
      {attempts.error && <p className="err">Replay attempts unavailable: {String(attempts.error)}</p>}
      {attempts.data && <table className="review-attempts"><thead><tr><th>Candidate attempt</th><th>Captured head · recording</th><th>Exact baseline</th><th>PR impact / status</th></tr></thead><tbody>
        {attempts.data.rows.map((attempt) => {
          const p = runParams(attempt);
          return <tr key={attempt.run_id} aria-current={attempt.run_id === run.run_id ? "true" : undefined}>
            <td><Link to={nav.href(`/r/${attempt.run_id}`)}>{attempt.run_id}</Link><div className="hint">{new Date(attempt.created_at).toLocaleString()}</div></td>
            <td><code>{p?.github?.head_sha.slice(0, 10) ?? "unpublished"}</code><div>{attempt.recording_id ?? "recording unavailable"}</div><div className="hint">{p?.system_under_test ?? "default system"}</div></td>
            <td>{p?.delta_against ? <Link to={nav.href(`/r/${p.delta_against}`)}>{p.delta_against}</Link> : "no baseline named"}</td>
            <td><RunListResult run={attempt} /></td>
          </tr>;
        })}
        {!attempts.data.rows.length && <tr><td colSpan={4}>No stored replay for this selected PR head. An older head’s verdict does not validate it.</td></tr>}
      </tbody></table>}
      <div className="runfilters">
        {cursor && <button className="btn" onClick={() => update("review_cursor", "")}>Newest attempts</button>}
        <button className="btn" disabled={!attempts.data?.next_cursor || attempts.isFetching} onClick={() => update("review_cursor", attempts.data?.next_cursor ?? "")}>Older attempts</button>
      </div>
    </details>
  </section>;
}
