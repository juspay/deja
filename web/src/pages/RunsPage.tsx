import React from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, RunRow } from "../lib/api";
import { useDebug, withDebug } from "../lib/debug";
import {
  attemptOrdinals,
  candidateRef,
  RESULT_STATES,
  ResultState,
  resultOf,
  whatHappened,
} from "../lib/result";
import { ResultChip } from "../components/Result";
import { KillRun } from "../components/KillRun";

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

/** Everything a substring search should reach on one row. */
function haystack(r: RunRow): string {
  return [
    r.run_id,
    r.recording_id ?? "",
    candidateRef(r).full,
    r.created_by,
    r.failure?.message ?? "",
    r.scorecard?.verdict?.reason ?? "",
    r.expectation ?? "",
  ]
    .join(" ")
    .toLowerCase();
}

export default function RunsPage() {
  const debug = useDebug();
  const [q, setQ] = React.useState("");
  const [filter, setFilter] = React.useState<ResultState | "">("");

  const runs = useQuery({ queryKey: ["runs"], queryFn: api.runs, refetchInterval: 5000 });

  const all = runs.data ?? [];
  // Attempt ordinals are derived over the WHOLE list, not the filtered view —
  // "attempt 3 of 4" must not change because you typed in the search box.
  const attempts = React.useMemo(() => attemptOrdinals(all), [all]);

  const rows = React.useMemo(() => {
    const needle = q.trim().toLowerCase();
    return all.filter((r) => {
      if (filter && resultOf(r).state !== filter) return false;
      if (!needle) return true;
      return haystack(r).includes(needle);
    });
  }, [all, q, filter]);

  if (runs.isLoading) return <p className="hint">loading…</p>;
  if (runs.error)
    return (
      <p className="err">
        {String(runs.error)} — is the orchestrator reachable?
      </p>
    );

  return (
    <>
      <div className="pagehead">
        <h1>Runs</h1>
        <Link className="btn" to={withDebug("/", debug)}>New run</Link>
      </div>

      <div className="runfilters">
        <input
          className="search"
          type="search"
          placeholder="search run id, recording, candidate, actor, what happened…"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          aria-label="filter runs"
        />
        <select
          value={filter}
          onChange={(e) => setFilter(e.target.value as ResultState | "")}
          aria-label="filter by result"
        >
          <option value="">all results</option>
          {RESULT_STATES.map((s) => (
            <option key={s} value={s}>
              {s.toLowerCase().replace("_", " ")}
            </option>
          ))}
        </select>
        <span className="hint">
          {rows.length === all.length
            ? `${all.length} run${all.length === 1 ? "" : "s"}`
            : `${rows.length} of ${all.length}`}
        </span>
      </div>

      <div className="runlist">
        <div className="runrow runhead" aria-hidden="true">
          <span>when</span>
          <span>result</span>
          <span>candidate</span>
          <span>recording</span>
          <span>what happened</span>
          <span className="num">attempt</span>
          {/* The action column. Reserved on every row, occupied only on the
              ones that are still running, so the columns above never shift. */}
          <span />
        </div>

        {rows.map((r) => {
          const res = resultOf(r);
          const cand = candidateRef(r);
          const said = whatHappened(r);
          const at = attempts.get(r.run_id);
          const t = when(r.created_at);
          // The whole row is the link: middle-click, copy-link-address and
          // keyboard focus all work, and there is no dead zone between cells.
          //
          // The kill control cannot live INSIDE it — a <button> inside an <a>
          // is invalid and the click would be the link's. It sits over the
          // reserved last column instead, so the row stays one anchor.
          return (
            <div className="runrowwrap" key={r.run_id}>
              <Link
                className="runrow"
                to={withDebug(`/r/${r.run_id}`, debug)}
                title={`${r.run_id} · ${r.created_by} · ${t.full}`}
              >
                <span className="rl-when">{t.short}</span>
                <span className="rl-result"><ResultChip result={res} /></span>
                <span className="rl-cand mono" title={cand.full}>{cand.label}</span>
                <span className="rl-rec mono">{r.recording_id ?? "—"}</span>
                <span className="rl-said" title={said ?? undefined}>{said ?? "—"}</span>
                <span
                  className="rl-attempt num"
                  title={
                    at
                      ? `derived client-side: run ${at.attempt} of ${at.of} with this mode + recording + candidate. The server does not persist a subject key.`
                      : undefined
                  }
                >
                  {at ? (at.of > 1 ? `${at.attempt}/${at.of}` : "1") : "—"}
                </span>
                <span />
              </Link>
              <div className="rl-kill">
                <KillRun run={r} compact />
              </div>
            </div>
          );
        })}

        {rows.length === 0 && (
          <p className="hint runempty">
            {all.length === 0 ? (
              <>no runs yet — <Link to="/">start one</Link></>
            ) : (
              <>no run matches this search</>
            )}
          </p>
        )}
      </div>
    </>
  );
}
