import React from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { Acknowledgement, DeltaRow, DeltaSide, api, runParams } from "../lib/api";
import { useDebug, withDebug } from "../lib/debug";
import { Group, effectiveOf, groupCharged, requestsOf, useAckActions, useAcknowledgements } from "../lib/acknowledge";
import { useHasRole, useMe } from "../lib/me";

/**
 * `/r/:runId/acknowledge`: the divergences a pull request is charged with,
 * one row per shape, and the two decisions on them: the author marks a
 * shape as intended with a note; someone else acknowledges it. Three tabs
 * split what needs a decision, what is done, and the trail.
 */

type Tab = "todo" | "done" | "history";

function side(s: DeltaSide): string {
  if (s === "tape") return "as recorded";
  if (s === "absent") return "absent";
  return `value ${s.hash.slice(0, 8)}…`;
}

function when(ts: string | null): string {
  if (!ts) return "";
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? ts : d.toLocaleString();
}

export default function AcknowledgePage() {
  const { runId = "" } = useParams();
  const debug = useDebug();
  const run = useQuery({ queryKey: ["run", runId], queryFn: () => api.run(runId) });
  const against = run.data ? (runParams(run.data)?.delta_against ?? "") : "";
  const delta = useQuery({
    queryKey: ["delta", runId, against],
    queryFn: () => api.delta(runId, against),
    enabled: !!against,
  });
  const acks = useAcknowledgements(runId);
  const [tab, setTab] = React.useState<Tab>("todo");
  const [selected, setSelected] = React.useState<Set<string>>(new Set());
  const [note, setNote] = React.useState("");
  const [err, setErr] = React.useState<string | null>(null);
  const [shown, setShown] = React.useState<Group | null>(null);
  const { propose, confirm, withdraw, me } = useAckActions(runId, against, setErr, () => {
    setSelected(new Set());
    setErr(null);
  });
  const whoami = useMe();
  const maintainer = useHasRole("maintainer");
  const signedInRequired = !!whoami.data?.configured && !whoami.data.authenticated;

  const d = delta.data && !("unavailable" in delta.data) ? delta.data : null;
  const crumbs = (
    <div className="crumb">
      <Link to={withDebug("/runs", debug)}>Runs</Link> <span>/</span>{" "}
      <Link to={withDebug(`/r/${runId}`, debug)}>report</Link> <span>/</span> <span className="hint">acknowledge</span>
    </div>
  );
  if (run.isLoading || acks.isLoading || (against && delta.isLoading)) return <>{crumbs}<p className="hint">loading…</p></>;
  if (run.error) return <>{crumbs}<p className="err">{String(run.error)}</p></>;
  if (!against) return <>{crumbs}<div className="delta-unavailable">This run was not measured against main, so there is nothing to acknowledge.</div></>;
  if (!acks.data) return <>{crumbs}<div className="delta-unavailable">This run names no pull request; acknowledgements belong to one.</div></>;
  if (!d) return <>{crumbs}<div className="delta-unavailable">The delta is not available yet.</div></>;

  const gh = acks.data.github;
  const history = acks.data.acknowledgements;
  const groups = groupCharged(d);
  const effective = effectiveOf(d);
  const v = d.verdict;
  const isMine = (g: Group) => !!g.ack && g.ack.by.trim().toLowerCase() === me.trim().toLowerCase();
  const todo = groups.filter((g) => !g.ack || g.ack.state !== "acknowledged");
  const done = groups.filter((g) => g.ack?.state === "acknowledged");
  const markable = todo.filter((g) => !g.ack || g.ack.state === "stale");
  const confirmable = todo.filter((g) => g.ack?.state === "proposed" && !isMine(g) && maintainer);
  // The server's rule: a maintainer, or the proposer, may withdraw.
  const withdrawable = (g: Group) => maintainer || isMine(g);
  const uncoveredRows = markable.reduce((n, g) => n + g.rows.length, 0);
  const label = runParams(run.data!)?.label ?? "";
  const step = effective === "pass" ? 4 : effective === "acknowledged" ? 4 : markable.length > 0 ? 2 : 3;
  const toggle = (key: string) =>
    setSelected((s) => {
      const n = new Set(s);
      n.has(key) ? n.delete(key) : n.add(key);
      return n;
    });
  const selectedGroups = markable.filter((g) => selected.has(g.key));
  const selectedRows = selectedGroups.flatMap((g) => g.rows.map((r) => r.address));
  const selectedProposed = confirmable.filter((g) => selected.has(g.key));

  const list = tab === "todo" ? todo : done;
  return (
    <>
      {crumbs}
      <div className="ack-head">
        <h1>Acknowledge divergences · PR #{gh.pr_number}</h1>
        {label && <span className="chip muted" title={label}>{label.replace(/^PR #\d+\s*·\s*/, "")}</span>}
        <div className="right">
          <Link className="btn" to={withDebug(`/r/${runId}`, debug)}>Back to the report</Link>
          <a className="btn" href={`https://github.com/${gh.repo}/pull/${gh.pr_number}`} target="_blank" rel="noreferrer">Open the PR</a>
        </div>
      </div>
      <div className="ack-steps">
        <span className="done">1 · the replay found {groups.length} shapes in {requestsOf(groups)} requests</span>
        <span className={step > 2 ? "done" : step === 2 ? "now" : ""}>2 · the author marks what is intended</span>
        <span className={step > 3 ? "done" : step === 3 ? "now" : ""}>3 · a second person acknowledges</span>
        <span className={step === 4 ? "done" : ""}>4 · the check turns green</span>
      </div>
      {!me && !signedInRequired && <p className="hint">Set your name in the top right to mark or acknowledge.</p>}
      {signedInRequired && <p className="hint">Sign in (top right) to mark or acknowledge.</p>}
      {whoami.data?.authenticated && !maintainer && todo.some((g) => g.ack?.state === "proposed") && (
        <p className="hint">Only a maintainer may acknowledge; you can mark shapes as intended and withdraw your own.</p>
      )}
      {err && <p className="err">{err}</p>}
      <div className="ack-grid">
        <div className="ack-main">
          <div className="ack-tabs">
            <button className={tab === "todo" ? "on" : ""} onClick={() => setTab("todo")}>To act on · {todo.length}</button>
            <button className={tab === "done" ? "on" : ""} onClick={() => setTab("done")}>Acknowledged · {done.length}</button>
            <button className={tab === "history" ? "on" : ""} onClick={() => setTab("history")}>History · {history.length}</button>
          </div>
          {tab !== "history" && list.length === 0 && (
            <p className="hint">{tab === "todo" ? "Nothing waits for a decision." : "Nothing is acknowledged yet."}</p>
          )}
          {tab !== "history" && list.length > 0 && (
            <table className="delta-ack-table">
              <thead>
                <tr>
                  <th className="sel"></th>
                  <th>connector · flow</th>
                  <th>what diverges</th>
                  <th className="num">requests</th>
                  <th>status</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {list.map((g) => {
                  const a = g.ack;
                  const selectable = (!a || a.state === "stale" || (a.state === "proposed" && !isMine(g) && maintainer)) && tab === "todo";
                  return (
                    <tr key={g.key} className={a ? `st-${a.state}` : "st-none"}>
                      <td className="sel">
                        {selectable && (
                          <input type="checkbox" id={`ack-${g.key}`} checked={selected.has(g.key)} onChange={() => toggle(g.key)} disabled={!me} aria-label={`select ${g.what} ${g.where}`} />
                        )}
                      </td>
                      <td className="lane">{g.lane}</td>
                      <td>
                        <span className="what">{g.what}</span>
                        <span className="sub mono">{g.where}{g.where ? " · " : ""}{g.family === "changed" ? <span className="fam changed">conflicts with main</span> : <span className="fam">new in this PR</span>}</span>
                      </td>
                      <td className="num">{g.rows.length}</td>
                      <td>
                        {a ? (
                          <span className="ackstate">
                            <span className={`chip ${a.state}`}>{a.state}</span>
                            <span className="by">by {a.by}</span>
                            {a.note && <span className="note" title={a.note}>“{a.note}”</span>}
                            {a.state === "stale" && <span className="hint">given on an earlier version of this change</span>}
                            {g.valueChanged && <span className="hint">value changed since</span>}
                            {a.state === "proposed" && isMine(g) && <span className="hint">waiting for a second person</span>}
                          </span>
                        ) : (
                          <span className="chip grey">not marked</span>
                        )}
                      </td>
                      <td className="act">
                        <button className="btn quiet" onClick={() => setShown(shown?.key === g.key ? null : g)}>{shown?.key === g.key ? "hide" : "show"}</button>
                        {a && withdrawable(g) && <button className="btn quiet" disabled={!me || withdraw.isPending} onClick={() => withdraw.mutate(a.id)}>Withdraw</button>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
          {shown && tab !== "history" && <Example g={shown} runId={runId} />}
          {tab === "todo" && (confirmable.length > 0 || markable.length > 0) && (
            <div className="ack-actions">
              {selectedProposed.length > 0 && (
                <button className="btn primary" disabled={!me || confirm.isPending} onClick={() => selectedProposed.forEach((g) => g.ack && confirm.mutate(g.ack.id))}>
                  Acknowledge {selectedProposed.length} shape{selectedProposed.length === 1 ? "" : "s"} · {requestsOf(selectedProposed)} requests
                </button>
              )}
              {selectedGroups.length > 0 && (
                <>
                  <input id="ack-note" type="text" placeholder="why this divergence is intended" value={note} onChange={(e) => setNote(e.target.value)} disabled={!me || propose.isPending} />
                  <button className="btn primary" disabled={!me || note.trim().length === 0 || propose.isPending} onClick={() => propose.mutate({ rows: selectedRows, note: note.trim() })}>
                    Mark {selectedGroups.length} shape{selectedGroups.length === 1 ? "" : "s"} as intended
                  </button>
                </>
              )}
              {selectedProposed.length === 0 && selectedGroups.length === 0 && (
                <span className="hint">
                  {confirmable.length > 0 ? `Tick the shapes to acknowledge; ${confirmable.length} proposed by someone else.` : `Tick the shapes that are intended and say why; ${markable.length} not yet marked.`}
                </span>
              )}
              {me && <span className="hint">Recorded as {me}.</span>}
            </div>
          )}
          {tab === "history" && (
            <table className="delta-ack-history">
              <thead>
                <tr><th>#</th><th>shape</th><th>note</th><th>proposed</th><th>acknowledged</th><th>withdrawn</th><th>change</th></tr>
              </thead>
              <tbody>
                {history.length === 0 && <tr><td colSpan={7} className="hint">Nothing yet.</td></tr>}
                {history.map((h: Acknowledgement) => (
                  <tr key={h.id} className={h.withdrawn_at ? "gone" : ""}>
                    <td className="mono">{h.id}</td>
                    <td>
                      {h.lane ? `${h.lane.connector} · ${h.lane.flow}` : "—"}
                      <span className="mono where"> {h.pattern.kind === "body" ? h.pattern.json_path : h.pattern.kind === "status" ? "status" : `${h.pattern.operation} on ${h.pattern.boundary}`}</span>
                    </td>
                    <td>{h.note}</td>
                    <td>{h.proposed_by} <span className="hint">{when(h.proposed_at)}</span></td>
                    <td>{h.acknowledged_by ? <>{h.acknowledged_by} <span className="hint">{when(h.acknowledged_at)}</span></> : <span className="hint">—</span>}</td>
                    <td>{h.withdrawn_by ? <>{h.withdrawn_by} <span className="hint">{when(h.withdrawn_at)}</span></> : <span className="hint">—</span>}</td>
                    <td className="mono">{h.change_id || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        <aside className="ack-rail">
          <div className="ack-card">
            <h3>Where it stands</h3>
            <dl>
              <dt>effective</dt><dd><span className={`chip solid ${effective === "fail" ? "fail" : "pass"}`}>{effective}</span>{effective === "fail" && <span className="hint"> until every shape is acknowledged</span>}</dd>
              <dt>acknowledged</dt><dd>{done.length} shapes · {v.acknowledged ?? 0} rows</dd>
              <dt>proposed</dt><dd>{groups.filter((g) => g.ack?.state === "proposed").length} shapes · {v.proposed ?? 0} rows</dd>
              <dt>stale</dt><dd>{groups.filter((g) => g.ack?.state === "stale").length} shapes · {v.stale ?? 0} rows</dd>
              <dt>not marked</dt><dd>{groups.filter((g) => !g.ack).length} shapes · {uncoveredRows - groups.filter((g) => g.ack?.state === "stale").reduce((n, g) => n + g.rows.length, 0)} rows</dd>
            </dl>
          </div>
          <div className="ack-card">
            <h3>This pull request</h3>
            <dl>
              <dt>repo</dt><dd className="mono">{gh.repo}</dd>
              <dt>head</dt><dd className="mono">{gh.head_sha.slice(0, 10)}</dd>
              <dt>change</dt><dd className="mono">{gh.change_id ?? "—"}</dd>
              <dt>against</dt><dd className="mono">{against.split("-")[2] ?? against}</dd>
              <dt>recording</dt><dd className="mono">{d.tape ?? run.data?.recording_id ?? "—"}</dd>
            </dl>
          </div>
          <div className="ack-card">
            <h3>What acknowledging does</h3>
            <p className="hint">It accepts the shape, not the value: the same divergence in any request, on any recording, stays accepted. A push that changes the PR's own diff marks it stale. The proposer cannot acknowledge their own proposal.</p>
          </div>
        </aside>
      </div>
    </>
  );
}

/** One request of the group, with its three sides, and the way to the case. */
function Example({ g, runId }: { g: Group; runId: string }) {
  const debug = useDebug();
  const r: DeltaRow = g.rows[0];
  return (
    <div className="ack-example">
      <div className="t">one of the {g.rows.length} requests · <span className="mono">{r.address.correlation}</span> · {g.lane}</div>
      <div className="r"><b>recorded</b><span>as recorded</span></div>
      <div className="r"><b>main</b><span className={r.m === "tape" ? "" : "old"}>{side(r.m)}</span></div>
      <div className="r"><b>this PR</b><span className="new">{side(r.y)}</span></div>
      <Link to={withDebug(`/r/${runId}?case=${encodeURIComponent(r.address.correlation)}`, debug)}>open the case in the report →</Link>
    </div>
  );
}
