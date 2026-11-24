import { useEffect, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";
import { api, runParams } from "../lib/api";
import { useDebug, withDebug } from "../lib/debug";
import {
  candidateRef,
  systemUnderTest,
  RESULT_STATES,
  resultOf,
  whatHappened,
} from "../lib/result";
import { RunListResult } from "../components/Result";
import { KillRun } from "../components/KillRun";
import { useSystems } from "../lib/systems";
import { reviewHref, useRunListScroll } from "../lib/navigation";
import { MERGE_QUEUE, changeOf } from "../lib/scope";

function when(iso: string): { short: string; full: string } {
  const d = new Date(iso);
  const now = Date.now();
  const mins = Math.round((now - d.getTime()) / 60000);
  const short =
    mins < 1 ? "just now"
      : mins < 60 ? `${mins}m ago`
        : mins < 60 * 24 ? `${Math.round(mins / 60)}h ago`
          : `${Math.round(mins / 1440)}d ago`;
  return { short, full: d.toLocaleString() };
}

export default function RunsPage() {
  const debug = useDebug();
  const [params, setParams] = useSearchParams();
  const scope = params.get("system") ?? "";
  const q = params.get("q") ?? "";
  const cursor = params.get("cursor") ?? "";
  const [search, setSearch] = useState(q);
  useEffect(() => {
    const timer = setTimeout(() => setSearch(q), 250);
    return () => clearTimeout(timer);
  }, [q]);
  const selectedResult = params.get("result") ?? "";
  const filter = RESULT_STATES.find((state) => state === selectedResult) ?? "";
  const update = (key: string, value: string, replace = false) => {
    const next = new URLSearchParams(params);
    if (value) next.set(key, value);
    else next.delete(key);
    if (["system", "q", "repo", "pr", "head"].includes(key)) next.delete("cursor");
    setParams(next, { replace });
  };
  const repo = params.get("repo") ?? "";
  const pr = params.get("pr") ?? "";
  const head = params.get("head") ?? "";
  const query = { system: scope, q: search, cursor, limit: 50,
    ...(repo ? { repo } : {}), ...(pr ? { pr: Number(pr) } : {}), ...(head ? { head } : {}),
  };
  const runs = useQuery({
    queryKey: ["runs", "page", query],
    queryFn: () => api.runsPage(query),
    enabled: q === search,
    placeholderData: keepPreviousData,
    // Older pages have an immutable position. Keep the newest page live.
    refetchInterval: cursor ? false : 5000,
  });
  const systems = useSystems();
  const choices = [
    ...systems.all.map((s) => ({ key: s.name, label: s.name })),
    { key: MERGE_QUEUE, label: "merge queue" },
  ];
  if (scope && !choices.some((choice) => choice.key === scope)) {
    choices.push({ key: scope, label: scope });
  }
  const loading = q !== search || runs.isLoading || runs.isPlaceholderData;
  const page = loading ? [] : runs.data?.rows ?? [];
  // Result classification remains shared with the report. This optional
  // filter is explicitly page-local; system and text search cover history.
  const rows = filter ? page.filter((r) => resultOf(r).state === filter) : page;
  const runsUrl = `/runs${params.size ? `?${params}` : ""}`;
  useRunListScroll(!loading && !runs.error);
  const prHistory = (repo: string, pr: number, head?: string) => {
    const next = new URLSearchParams();
    if (scope) next.set("system", scope);
    if (debug) next.set("debug", "1");
    next.set("repo", repo); next.set("pr", String(pr));
    if (head) next.set("head", head);
    return `/runs?${next}`;
  };

  return (
    <>
      <div className="pagehead">
        <h1>Runs</h1>
        <Link className="btn" to={withDebug("/", debug)}>New run</Link>
      </div>

      <nav className="systempick" aria-label="System">
        <button className={scope ? "tab" : "tab active"} onClick={() => update("system", "")}>
          all systems
        </button>
        {choices.map((c) => (
          <button key={c.key} className={scope === c.key ? "tab active" : "tab"} onClick={() => update("system", c.key)}>
            {c.label}
          </button>
        ))}
      </nav>

      <div className="runfilters">
        <input
          className="search"
          type="search"
          placeholder="search all history: run, PR, recording, candidate, actor…"
          maxLength={512}
          value={q}
          onChange={(e) => update("q", e.target.value, true)}
          aria-label="search run history"
        />
        <select
          value={filter}
          onChange={(e) => update("result", e.target.value)}
          aria-label="filter this page by recording outcome or run status"
        >
          <option value="">all recording outcomes on this page</option>
          {RESULT_STATES.map((s) => (
            <option key={s} value={s}>
              {s === "DIVERGED" ? "recording differs" : s === "REPRODUCED" ? "recording reproduced" : s.toLowerCase().replace("_", " ")}
            </option>
          ))}
        </select>
        <span className="hint">
          {loading ? "loading matching history…" : <>
            {filter ? `${rows.length} of ${page.length}` : page.length} runs on this page
            {runs.data?.next_cursor ? " · older runs available" : " · end of matching history"}
          </>}
        </span>
      </div>

      {(repo || pr || head) && <div className="runfilters">
        <span>PR history: {repo || "any repository"}{pr && ` #${pr}`}{head && <> · captured head <code>{head}</code></>}</span>
        <button className="btn quiet" onClick={() => setParams((previous) => { const next = new URLSearchParams(previous); for (const key of ["repo", "pr", "head", "cursor"]) next.delete(key); return next; })}>Clear PR scope</button>
        {head && <Link to={prHistory(repo, Number(pr))}>All captured heads →</Link>}
      </div>}
      <p className="hint">PR impact compares with the named baseline. Recording fidelity compares with the captured traffic.</p>
      {!loading && runs.error && <p className="err">{String(runs.error)} — could not load run history.</p>}
      <div className="runlist" aria-busy={loading}>
        <div className="runrow runhead" aria-hidden="true">
          <span>when</span>
          <span>PR impact / run status</span>
          <span>pull request · label</span>
          <span>candidate</span>
          <span>recording</span>
          <span>recording / run details</span>
          <span className="num">role</span>
          {/* The action column. Reserved on every row, occupied only on the
              ones that are still running, so the columns above never shift. */}
          <span />
        </div>

        {rows.map((r) => {
          const cand = candidateRef(r);
          const said = whatHappened(r);
          const p = runParams(r);
          const purpose = p?.purpose;
          const github = p?.github;
          const change = changeOf(r);
          const t = when(r.created_at);
          // The whole row is the link: middle-click, copy-link-address and
          // keyboard focus all work, and there is no dead zone between cells.
          //
          // The kill control cannot live INSIDE it — a <button> inside an <a>
          // is invalid and the click would be the link's. It sits over the
          // reserved last column instead, so the row stays one anchor.
          return (
            <div className="runrowwrap" key={r.run_id} id={r.run_id}>
              <Link
                className="runrow"
                to={reviewHref(withDebug(`/r/${r.run_id}`, debug), runsUrl, { runs: `${runsUrl}#${encodeURIComponent(r.run_id)}`, trail: [] })}
                state={{ runsUrl }}
                title={`${r.run_id} · ${r.created_by} · ${t.full}`}
              >
                <span className="rl-when">{t.short}</span>
                <span className="rl-result">
                  <RunListResult run={r} />
                </span>
                <span className="rl-change" title={change ?? undefined}>{change ?? "—"}</span>
                <span className="rl-cand mono" title={cand.full}>
                  {cand.label}
                  {/* Only a non-default system is worth a badge — every legacy
                      row named nothing and would just repeat itself. Presence
                      IS the test: the wire contract says an absent value means
                      the default, so this needs no name to compare against. */}
                  {systemUnderTest(r) && <> · {systemUnderTest(r)}</>}
                </span>
                <span className="rl-rec mono">{r.recording_id ?? "—"}</span>
                <span className="rl-said" title={said ?? undefined}>{said ?? "—"}</span>
                <span className="rl-attempt num" title="Run purpose, as published; attempts are not inferred from a partial history page">
                  {purpose ?? (runParams(r)?.delta_against ? "candidate" : "—")}
                </span>
                <span />
              </Link>
              {(github || p?.delta_against) && <div className="run-pair-links">
                {github && <><Link to={prHistory(github.repo, github.pr_number)}>PR #{github.pr_number} history</Link><span> · </span><Link to={prHistory(github.repo, github.pr_number, github.head_sha)}>captured head {github.head_sha.slice(0, 10)}</Link></>}
                {p?.delta_against && <><span> · </span><Link to={reviewHref(`/r/${p.delta_against}`, runsUrl, { runs: `${runsUrl}#${encodeURIComponent(r.run_id)}`, trail: [] })}>baseline {p.delta_against}</Link></>}
              </div>}
              <div className="rl-kill">
                <KillRun run={r} compact />
              </div>
            </div>
          );
        })}

        {!loading && !runs.error && rows.length === 0 && (
          <p className="hint runempty">
            {page.length === 0 ? (
              <>no matching runs on this page — <Link to="/">start one</Link></>
            ) : (
              <>no run on this page has the selected result; older pages may contain matches</>
            )}
          </p>
        )}
      </div>
      <nav className="runfilters" aria-label="Run history pages">
        {cursor && <button className="btn" onClick={() => update("cursor", "")}>Newest matching runs</button>}
        <button className="btn" disabled={loading || !!runs.error || !runs.data?.next_cursor} onClick={() => update("cursor", runs.data?.next_cursor ?? "")}>
          Older runs
        </button>
        <span className="hint">System and search cover all history. The recording outcome filter applies to this page.</span>
      </nav>
    </>
  );
}
