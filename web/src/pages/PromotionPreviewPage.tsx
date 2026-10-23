// Promote a version: the page behind the Promote button. What the promotion
// carries (the merges since the target's release, each with what it changed
// on its merge-queue run and how it reads against the target), what the
// target will see change (the version's promotion replay, as a three-way
// delta), the checks, and the request itself. A second page lays that delta
// out by pull request.
//
//   /deployments/versions/:version/promote?to=<env>   the preview
//   /deployments/versions/:version/diff?to=<env>      the delta, by merge
//
// Everything here is read from one endpoint, `/cd/versions/{v}/promotion-preview`,
// except the checks and the request, which are the calls the card made.

import React from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";

import { useMe } from "../lib/me";
import { cdApi, deltaFamily, deltaUnavailable, type ApiError, type CdCarried, type CdConfig, type CdPromotionPreview, type CdQueueSummary, type DeltaResponse, type DeltaRow } from "../lib/api";
import { ago, carries, pullRequestOf, when } from "../lib/cd";
import { describe } from "../lib/acknowledge";
import { AddressCell, sideLabel } from "./DeltaPage";
import { Frame, PrCell, PromotionReplayChip, VersionLink, href, useMayPromote } from "./DeploymentsPage";

/** The environment a promotion targets: `?to=`, else the first promotable one. */
function useTarget(cfg: Extract<CdConfig, { enabled: true }>): string {
  const [params] = useSearchParams();
  return params.get("to") || cfg.promotable[0] || cfg.environments[1] || "";
}

/** The environment a promotion comes from: the one before the target. */
function sourceOf(cfg: Extract<CdConfig, { enabled: true }>, target: string): string {
  const i = cfg.environments.indexOf(target);
  return i > 0 ? cfg.environments[i - 1] : cfg.environments[0];
}

function usePreview(system: string, version: string, target: string) {
  return useQuery({
    queryKey: ["cd", "promotion-preview", system, version, target],
    queryFn: () => cdApi.promotionPreview(system, version, target),
    enabled: !!version && !!target,
    staleTime: 30_000,
    refetchInterval: 60_000,
  });
}

/** What a merge changed, from its queue run's delta: one chip. */
function QueueChip({ q }: { q: CdQueueSummary | null }) {
  if (!q) return <span className="hint">no run</span>;
  if (q.unavailable) return <span className="chip muted" title={q.unavailable.unavailable}>{q.unavailable.unavailable_kind === "pending" ? "pending" : "no delta"}</span>;
  const v = q.verdict;
  if (!v) return <span className="chip muted">unknown</span>;
  const rows = v.introduced + v.changed;
  const word = v.effective ?? (v.pass ? "pass" : "fail");
  const tone = word === "fail" ? "failed" : word === "acknowledged" ? "ok" : "ok";
  return (
    <span className={`chip ${tone}`} title={`${v.reason}${q.source === "pr" ? " · from a pull-request run; no merge-queue run recorded" : ""}`}>
      {word}
      {rows > 0 ? ` · ${rows} row${rows === 1 ? "" : "s"}` : ""}
      {q.source === "pr" ? " · PR run" : ""}
    </span>
  );
}

/** `k of n` charged rows acknowledged on the merge's own run. */
function AckCell({ q, rows }: { q: CdQueueSummary | null; rows: DeltaRow[] }) {
  const n = rows.length;
  if (!q?.verdict || n === 0) return <span className="hint">—</span>;
  const k = rows.filter((r) => r.acknowledgement?.state === "acknowledged").length;
  const tone = k === n ? "ok" : k === 0 ? "failed" : "pending";
  return (
    <span className={`chip ${tone}`}>
      {k} of {n}
    </span>
  );
}

function SideCell({ s }: { s: DeltaRow["m"] }) {
  const l = sideLabel(s);
  return <span className={`cd-side ${l.cls}`}>{l.text}</span>;
}

/** A row's acknowledgement, as a chip with the note on hover. */
function RowAck({ r }: { r: DeltaRow }) {
  const a = r.acknowledgement;
  if (!a) return <span className="chip muted">unmarked</span>;
  const tone = a.state === "acknowledged" ? "ok" : a.state === "proposed" ? "pending" : "inconclusive";
  return (
    <span className={`chip ${tone}`} title={`${a.by}: ${a.note}${a.value_changed ? " · the value differs from the one acknowledged" : ""}`}>
      {a.state}
    </span>
  );
}

/** The rows a merge changed on its own run, under its line in the table. */
function QueueRows({ rows, runId }: { rows: DeltaRow[]; runId: string | null }) {
  if (!runId) return <p className="hint">déjà has no merge-queue or pull-request run for this merge, so what it changed alone is not known.</p>;
  if (rows.length === 0) return <p className="hint">Its run changed nothing against main's base.</p>;
  const notes = [...new Set(rows.map((r) => r.acknowledgement).filter((a): a is NonNullable<typeof a> => !!a).map((a) => `${a.by}: ${a.note}`))];
  return (
    <div className="cd-qrows">
      <table className="cd-qtable">
        <thead>
          <tr>
            <th>lane</th>
            <th>what</th>
            <th>where</th>
            <th>main's base</th>
            <th>this merge</th>
            <th>bucket</th>
            <th>status</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => {
            const d = describe(r.address);
            return (
              <tr key={i}>
                <td className="hint">{r.lane ? `${r.lane.connector} · ${r.lane.flow}` : "—"}</td>
                <td>{d.what}</td>
                <td className="mono cd-where">{d.where || <span className="hint">—</span>}</td>
                <td>
                  <SideCell s={r.m} />
                </td>
                <td>
                  <SideCell s={r.y} />
                </td>
                <td className="hint">{deltaFamily(r.bucket)}</td>
                <td>
                  <RowAck r={r} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      <div className="hint">
        {notes.map((n) => (
          <div key={n}>"{n}"</div>
        ))}
        {runId && (
          <>
            <Link to={`/r/${runId}`}>run</Link> · <Link to={`/r/${runId}/acknowledge`}>acknowledge page</Link>
          </>
        )}
      </div>
    </div>
  );
}

// ---- the preview ---------------------------------------------------------------

export function PromotePreviewPage() {
  const { version = "" } = useParams();
  return (
    <Frame
      title="Deployments"
      crumb={
        <>
          <Link to="/deployments/versions">Versions</Link>
          <span>/</span>
          <Link to={`/deployments/versions/${encodeURIComponent(version)}`} className="mono">
            {version}
          </Link>
          <span>/</span>
          Promote
        </>
      }
    >
      {(cfg, system) => <Preview cfg={cfg} system={system} version={version} />}
    </Frame>
  );
}

/** The banner: the version's promotion replay, as what the target will see change. */
function ChangeBanner({ p, target, system, cfg, version }: { p: CdPromotionPreview; target: string; system: string; cfg: CdConfig; version: string }) {
  const h = p.promotion_replay;
  const d = p.diff;
  const un = deltaUnavailable(d as DeltaResponse);
  const diffHref = href(`/deployments/versions/${encodeURIComponent(version)}/diff?to=${encodeURIComponent(target)}`, system, cfg);
  return (
    <section className="cd-panel">
      <div className="cd-panelhead">
        <h2>What {target} will see change</h2>
        <span className="hint">
          {p.target_live
            ? `this version against ${p.target_live.version}, on one recording`
            : `${target} runs nothing recorded`}
          {h ? ` · ${h.requested_by}${h.jenkins_build ? ` · ${h.jenkins_build}` : ""} · ${when(h.at)}` : ""}
        </span>
        <div className="cd-panelright">
          {!un && <Link className="btn" to={diffHref}>Open the diff</Link>}
          {h?.candidate_run && (
            <Link className="btn" to={`/r/${h.candidate_run}`}>
              candidate run
            </Link>
          )}
          {h?.baseline_run && (
            <Link className="btn" to={`/r/${h.baseline_run}`}>
              baseline run
            </Link>
          )}
        </div>
      </div>
      {un ? (
        <p className="hint cd-panelbody">
          {un.pending ? "Not yet: " : "No diff: "}
          {un.why}
          {!h && p.target_live ? " The promotion-replay job runs one after a version goes live on the first environment." : ""}
        </p>
      ) : "verdict" in d ? (
        (() => {
          const v = d.verdict;
          const changedReq = (v.changed_requests ?? 0) + (v.introduced_requests ?? 0);
          const fixedReq = v.resolved_requests ?? 0;
          const total = d.covered_correlations || 1;
          const same = Math.max(0, total - changedReq - fixedReq);
          const acked = v.acknowledged ?? 0;
          const charged = v.introduced + v.changed;
          return (
            <>
              <div className="cd-kpis">
                <div className="cd-kpi">
                  <span className={`n ${changedReq ? "bad" : ""}`}>
                    {changedReq} <small>of {d.covered_correlations} requests</small>
                  </span>
                  <span className="l">differ on {sourceOf(cfg as Extract<CdConfig, { enabled: true }>, target)}, not on {target}</span>
                  <span className="hint">what a promotion changes</span>
                </div>
                <div className="cd-kpi">
                  <span className="n">{charged}</span>
                  <span className="l">fields and calls changed</span>
                  <span className="hint">{acked} of them acknowledged on their merge</span>
                </div>
                <div className="cd-kpi">
                  <span className={`n ${fixedReq ? "good" : ""}`}>{fixedReq}</span>
                  <span className="l">differ on {target}, not here</span>
                  <span className="hint">fixed by these merges</span>
                </div>
                <div className="cd-kpi">
                  <span className="n dim">{v.inherited}</span>
                  <span className="l">differ on both</span>
                  <span className="hint">inherited from the recording</span>
                </div>
              </div>
              <div className="cd-bar" aria-label={`${same} same, ${changedReq} changed, ${fixedReq} fixed`}>
                <span className="same" style={{ width: `${(same / total) * 100}%` }} />
                <span className="changed" style={{ width: `${(changedReq / total) * 100}%` }} />
                <span className="fixed" style={{ width: `${(fixedReq / total) * 100}%` }} />
              </div>
              <div className="hint cd-barlegend">
                <span>
                  <i className="same" /> {same} same as {target}
                </span>
                <span>
                  <i className="changed" /> {changedReq} changed
                  {d.lanes.length ? ` · by lane: ${d.lanes.slice(0, 4).map((l) => `${l.lane.connector} ${(l.requests_by_family?.changed ?? 0) + (l.requests_by_family?.introduced ?? 0)}`).join(" · ")}` : ""}
                </span>
                <span>
                  <i className="fixed" /> {fixedReq} fixed
                </span>
              </div>
            </>
          );
        })()
      ) : null}
    </section>
  );
}

function Preview({ cfg, system, version }: { cfg: Extract<CdConfig, { enabled: true }>; system: string; version: string }) {
  const target = useTarget(cfg);
  const source = sourceOf(cfg, target);
  const navigate = useNavigate();
  const may = useMayPromote(target);
  const me = useMe();
  const q = usePreview(system, version, target);
  const envs = useQuery({ queryKey: ["cd", "environments", system], queryFn: () => cdApi.environments(system), refetchInterval: 15_000 });
  const check = useQuery({
    queryKey: ["cd", "promotion-check", system, target, version],
    queryFn: () => cdApi.promotionCheck(system, target, version),
    enabled: !!version && !!me.data?.authenticated,
  });
  const [note, setNote] = React.useState("");
  const [open, setOpen] = React.useState<string | null>(version);
  const go = useMutation({
    mutationFn: () => cdApi.promote(system, target, version, note.trim()),
    onSuccess: (r) => navigate(href(`/deployments/promotions/${r.id}`, system, cfg)),
  });
  if (q.isLoading) return <p className="hint">loading…</p>;
  if (q.error) return (q.error as ApiError).status === 404 ? <div className="empty"><div className="et">No such version</div></div> : <p className="err">{String(q.error)}</p>;
  if (!q.data) return null;
  const p = q.data;
  const v = p.version;
  const pr = pullRequestOf(v);
  const sourceRow = envs.data?.environments.find((e) => e.environment === source);
  const targetRow = envs.data?.environments.find((e) => e.environment === target);
  const ranAt = sourceRow?.live?.version === v.version ? sourceRow.live.at : null;
  const unacked = p.carried.reduce((n, c) => n + c.queue_rows.filter((r) => r.acknowledgement?.state !== "acknowledged").length, 0);
  const unackedIn = p.carried.filter((c) => c.queue_rows.some((r) => r.acknowledgement?.state !== "acknowledged")).map((c) => pullRequestOf(c.version)?.number).filter((n): n is number => !!n);
  return (
    <>
      <section className="cd-panel cd-prevhead">
        <div className="cd-prevtitle">
          <h2 className="cd-prevh">
            Promote <VersionLink version={v.version} system={system} cfg={cfg} /> to {target}
          </h2>
          <span className={`chip cd-kindchip ${v.kind === "release" ? "inconclusive" : "muted"}`}>{v.kind}</span>
          {ranAt && (
            <span className="hint">
              ran on {source} · {when(ranAt)} · {ago(ranAt)}
            </span>
          )}
          {v.kind === "merge" && pr && (
            <div className="cd-pr">
              <Link className="cd-prnum" to={`/deployments/pull-requests/${v.prs[0]?.repo}/${pr.number}`}>
                #{pr.number}
              </Link>
              <span className="cd-prtitle" title={pr.title}>
                {pr.title}
              </span>
              <span className="hint">
                · {v.prs[0]?.author ?? "—"} · merged {v.merged_at ? when(v.merged_at) : "—"}
              </span>
            </div>
          )}
          <div className="cd-prevfacts">
            <span>
              <span className="hint">Replaces</span>{" "}
              {p.target_live ? (
                <>
                  <VersionLink version={p.target_live.version} system={system} cfg={cfg} />
                  {targetRow?.live && (
                    <span className="hint">
                      {" "}
                      on {target} since {when(targetRow.live.at)}, {ago(targetRow.live.at)}
                    </span>
                  )}
                </>
              ) : (
                <span className="hint">nothing recorded on {target}</span>
              )}
            </span>
            <span>
              <span className="hint">Carries</span>{" "}
              {p.carried_known ? (
                <b>
                  {p.carried.length} merge{p.carried.length === 1 ? "" : "s"}
                </b>
              ) : (
                <span className="hint">not known: {p.target_live ? `${p.target_live.version} names no merge it was cut on` : "nothing to count from"}</span>
              )}
              {p.carried_known && p.target_live?.released_from && (
                <span className="hint">
                  {" "}
                  since the merge {p.target_live.version} was cut on, <span className="mono">{p.target_live.released_from}</span>
                </span>
              )}
            </span>
          </div>
        </div>
        <div className="cd-prevaction">
          {may ? (
            <button className="btn primary" disabled={!check.data?.ok || go.isPending} onClick={() => go.mutate()}>
              {go.isPending ? "Promoting…" : `Promote ${v.version} to ${target}`}
            </button>
          ) : (
            <span className="hint">Sign in as a {target} upgrader to promote.</span>
          )}
          {check.data && <span className={`chip ${check.data.ok ? "ok" : "failed"}`}>{check.data.checks.filter((c) => c.ok).length} of {check.data.checks.length} checks pass</span>}
          <span className="hint">Writes a request; the promote job deploys the release and syncs {target}. Steps show on the Promotions tab.</span>
          {go.error && <span className="err">{String(go.error)}</span>}
        </div>
      </section>

      <ChangeBanner p={p} target={target} system={system} cfg={cfg} version={version} />

      <section className="cd-panel">
        <div className="cd-panelhead">
          <h2>Pull requests riding along</h2>
          <span className="hint">
            {p.carried_known ? `${p.carried.length} merge${p.carried.length === 1 ? "" : "s"}, newest first · each ran on ${source}` : "only this one can be listed"} · open a row for what it changed
          </span>
          <div className="cd-panelright hint">This PR changed = its merge-queue run against main · vs {target} = its own promotion replay</div>
        </div>
        <table className="cd-ride">
          <thead>
            <tr>
              <th />
              <th>version</th>
              <th>pull request</th>
              <th>merged</th>
              <th>this PR changed</th>
              <th>vs {target}</th>
              <th>acknowledged</th>
            </tr>
          </thead>
          <tbody>
            {p.carried.map((c: CdCarried) => {
              const isOpen = open === c.version.version;
              return (
                <React.Fragment key={c.version.version}>
                  <tr className={isOpen ? "open" : ""}>
                    <td>
                      <button type="button" className="cd-caret" aria-expanded={isOpen} aria-label={isOpen ? "hide what it changed" : "show what it changed"} onClick={() => setOpen(isOpen ? null : c.version.version)}>
                        {isOpen ? "▾" : "▸"}
                      </button>
                    </td>
                    <td>
                      <VersionLink version={c.version.version} system={system} cfg={cfg} />
                      {c.version.version === v.version && <span className="chip inconclusive cd-picked">picked</span>}
                    </td>
                    <td>
                      <PrCell v={c.version} />
                    </td>
                    <td className="hint" title={c.version.merged_at ?? ""}>
                      {c.version.merged_at ? when(c.version.merged_at) : "—"}
                    </td>
                    <td>
                      <QueueChip q={c.queue} />
                    </td>
                    <td>
                      <PromotionReplayChip replay={c.promotion_replay} />
                    </td>
                    <td>
                      <AckCell q={c.queue} rows={c.queue_rows} />
                    </td>
                  </tr>
                  {isOpen && (
                    <tr className="cd-ridex">
                      <td />
                      <td colSpan={6}>
                        <QueueRows rows={c.queue_rows} runId={c.queue?.run_id ?? null} />
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
        {p.carried_known && p.carried.length > 1 && (
          <div className="hint cd-panelbody">
            <b>Pick an older merge</b> to promote fewer:{" "}
            {p.carried
              .filter((c) => c.version.version !== v.version)
              .map((c, i) => (
                <React.Fragment key={c.version.version}>
                  {i > 0 && " · "}
                  <Link to={href(`/deployments/versions/${encodeURIComponent(c.version.version)}/promote?to=${encodeURIComponent(target)}`, system, cfg)}>
                    {c.version.version} ({p.carried.length - p.carried.findIndex((x) => x.version.version === c.version.version)})
                  </Link>
                </React.Fragment>
              ))}
            . Merges after the picked one stay on {source}.
          </div>
        )}
      </section>

      <div className="cd-two">
        <section className="cd-panel">
          <div className="cd-panelhead">
            <h2>Checks</h2>
            <span className="hint">run now, and again when you press Promote</span>
          </div>
          <div className="cd-panelbody">
            {!me.data?.authenticated && <p className="hint">Sign in to see the checks for your account.</p>}
            {check.isLoading && <p className="hint">checking…</p>}
            {check.error && <p className="err">{String(check.error)}</p>}
            {check.data && (
              <ul className="cd-checks">
                {check.data.checks.map((c) => (
                  <li key={c.id} className={c.ok ? "ok" : "no"}>
                    <span className={`chip ${c.ok ? "ok" : "failed"}`}>{c.ok ? "pass" : "fail"}</span> {c.why}
                  </li>
                ))}
              </ul>
            )}
          </div>
        </section>
        <section className="cd-panel">
          <div className="cd-panelhead">
            <h2>Before you press</h2>
          </div>
          <div className="cd-panelbody cd-prevform">
            <label htmlFor="cd-preview-note" className="hint">
              Note, shown on the request
            </label>
            <input id="cd-preview-note" value={note} onChange={(e) => setNote(e.target.value)} placeholder="e.g. QA signed off the Ilixium error mapping" />
            <p className="hint">
              What happens: the request is written as pending; the promote job picks it within a minute, confirms the release's image and chart, runs deploy-apps for {target} and syncs it. About ten minutes to live. Cancel works until it is picked.
            </p>
            {unacked > 0 && (
              <p className="hint">
                Unacknowledged rows left in this set: <b>{unacked}</b>
                {unackedIn.length ? ` across ${unackedIn.map((n) => `#${n}`).join(", ")}` : ""}. Evidence for your sign-off, not a block.
              </p>
            )}
            <div className="cd-promoactions">
              <Link className="btn" to={href(`/deployments/versions/${encodeURIComponent(version)}`, system, cfg)}>
                Back
              </Link>
              {may && (
                <button className="btn primary" disabled={!check.data?.ok || go.isPending} onClick={() => go.mutate()}>
                  {go.isPending ? "Promoting…" : `Promote ${v.version} to ${target}`}
                </button>
              )}
            </div>
          </div>
        </section>
      </div>
    </>
  );
}

// ---- the diff, by pull request -------------------------------------------------

export function PromotionDiffPage() {
  const { version = "" } = useParams();
  return (
    <Frame
      title="Deployments"
      crumb={
        <>
          <Link to="/deployments/versions">Versions</Link>
          <span>/</span>
          <Link to={`/deployments/versions/${encodeURIComponent(version)}`} className="mono">
            {version}
          </Link>
          <span>/</span>
          Diff
        </>
      }
    >
      {(cfg, system) => <Diff cfg={cfg} system={system} version={version} />}
    </Frame>
  );
}

type Show = "changed" | "fixed" | "unacknowledged";

function DiffRows({ rows, target, source }: { rows: (DeltaRow & { also_changed_by?: string[] })[]; target: string; source: string }) {
  return (
    <table className="cd-dtable">
      <thead>
        <tr>
          <th>lane</th>
          <th>address</th>
          <th>{target}</th>
          <th>{source}</th>
          <th>bucket</th>
          <th>status</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={i}>
            <td className="hint">{r.lane ? `${r.lane.connector} · ${r.lane.flow}` : "—"}</td>
            <td className="cd-addr">
              <AddressCell a={r.address} />
            </td>
            <td>
              <SideCell s={r.m} />
            </td>
            <td>
              <SideCell s={r.y} />
            </td>
            <td className="hint">
              {deltaFamily(r.bucket)}
              {!r.blocking && " · non-blocking"}
              {r.also_changed_by?.length ? ` · changed again by ${r.also_changed_by.join(", ")}` : ""}
            </td>
            <td>
              <RowAck r={r} />
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Diff({ cfg, system, version }: { cfg: Extract<CdConfig, { enabled: true }>; system: string; version: string }) {
  const target = useTarget(cfg);
  const source = sourceOf(cfg, target);
  const q = usePreview(system, version, target);
  const [show, setShow] = React.useState<Show>("changed");
  if (q.isLoading) return <p className="hint">loading…</p>;
  if (q.error) return <p className="err">{String(q.error)}</p>;
  if (!q.data) return null;
  const p = q.data;
  const h = p.promotion_replay;
  const d = p.diff;
  const un = deltaUnavailable(d as DeltaResponse);
  const back = href(`/deployments/versions/${encodeURIComponent(version)}/promote?to=${encodeURIComponent(target)}`, system, cfg);
  if (un || !("verdict" in d)) {
    return (
      <section className="cd-panel">
        <div className="cd-panelhead">
          <h2>What {target} will see change</h2>
          <div className="cd-panelright">
            <Link className="btn" to={back}>
              Back to Promote
            </Link>
          </div>
        </div>
        <p className="hint cd-panelbody">
          {un?.pending ? "Not yet: " : "No diff: "}
          {un?.why ?? "unavailable"}
        </p>
      </section>
    );
  }
  const v = d.verdict;
  const byVersion = new Map(p.carried.map((c) => [c.version.version, c]));
  // Oldest first from the server; the page reads newest first.
  const groups = [...d.groups].reverse();
  const changedRows = groups.reduce((n, g) => n + g.rows.length, 0) + d.unattributed.length;
  const unackedRows = groups.reduce((n, g) => n + g.rows.filter((r) => r.acknowledgement?.state !== "acknowledged").length, 0) + d.unattributed.length;
  const keep = (r: DeltaRow) => show !== "unacknowledged" || r.acknowledgement?.state !== "acknowledged";
  return (
    <>
      <section className="cd-panel">
        <div className="cd-panelhead">
          <h2>What {target} will see change</h2>
          <span className="hint">
            {source}'s <span className="mono">{version}</span> against {target}'s <span className="mono">{p.target_live?.version ?? "—"}</span>, both replayed on recording <span className="mono">{d.tape ?? "—"}</span>
          </span>
          <div className="cd-panelright">
            {h?.candidate_run && (
              <Link className="btn" to={`/r/${h.candidate_run}/delta?against=${encodeURIComponent(h.baseline_run ?? "")}`}>
                plain delta
              </Link>
            )}
            <Link className="btn primary" to={back}>
              Back to Promote
            </Link>
          </div>
        </div>
        <div className="cd-kpis cd-panelbody">
          <div className="cd-kpi">
            <span className={`n ${changedRows ? "bad" : ""}`}>{(v.changed_requests ?? 0) + (v.introduced_requests ?? 0)}</span>
            <span className="l">requests changed by {source}</span>
            <span className="hint">differ from the recording on {source} only</span>
          </div>
          <div className="cd-kpi">
            <span className="n">{v.introduced + v.changed}</span>
            <span className="l">fields and calls</span>
            <span className="hint">
              across {d.lanes.length} lane{d.lanes.length === 1 ? "" : "s"}, {groups.filter((g) => g.rows.length).length} pull request{groups.filter((g) => g.rows.length).length === 1 ? "" : "s"}
            </span>
          </div>
          <div className="cd-kpi">
            <span className={`n ${d.fixed.length ? "good" : ""}`}>{v.resolved_requests ?? d.fixed.length}</span>
            <span className="l">fixed on {source}</span>
            <span className="hint">differ on {target} only</span>
          </div>
          <div className="cd-kpi">
            <span className="n dim">{d.clean}</span>
            <span className="l">same on both</span>
            <span className="hint">not shown below</span>
          </div>
        </div>
        <div className="cd-panelbody cd-pills">
          <span className="hint">Show</span>
          <button type="button" className={`cd-pill${show === "changed" ? " on" : ""}`} onClick={() => setShow("changed")}>
            changed by {source} · {changedRows}
          </button>
          <button type="button" className={`cd-pill${show === "fixed" ? " on" : ""}`} onClick={() => setShow("fixed")}>
            fixed on {source} · {d.fixed.length}
          </button>
          <button type="button" className={`cd-pill${show === "unacknowledged" ? " on" : ""}`} onClick={() => setShow("unacknowledged")}>
            unacknowledged only · {unackedRows}
          </button>
        </div>
      </section>

      {show === "fixed" ? (
        <section className="cd-panel">
          <div className="cd-panelhead">
            <h2>Fixed on {source}</h2>
            <span className="hint">differ from the recording on {target} only; a promotion removes these</span>
          </div>
          {d.fixed.length === 0 ? <p className="hint cd-panelbody">None.</p> : <DiffRows rows={d.fixed} target={target} source={source} />}
        </section>
      ) : (
        <section className="cd-panel">
          <div className="cd-panelhead">
            <h2>Changed by {source}, by pull request</h2>
            <span className="hint">each divergence is attributed to the merge whose merge-queue run first showed it; the rest are below</span>
            <div className="cd-panelright hint">
              recording = what both were asked · {target} = baseline reply · {source} = candidate reply
            </div>
          </div>
          {groups.map((g) => {
            const c = byVersion.get(g.version);
            const rows = g.rows.filter(keep);
            if (rows.length === 0 && show === "unacknowledged") return null;
            const pr = c ? pullRequestOf(c.version) : null;
            const acked = g.rows.filter((r) => r.acknowledgement?.state === "acknowledged").length;
            return (
              <div className="cd-dgroup" key={g.version}>
                <div className="cd-dgrouphead">
                  <span className="cd-pr">
                    {pr && c ? (
                      <>
                        <Link className="cd-prnum" to={`/deployments/pull-requests/${c.version.prs[0]?.repo}/${pr.number}`}>
                          #{pr.number}
                        </Link>
                        <span className="cd-prtitle" title={pr.title}>
                          {pr.title}
                        </span>
                      </>
                    ) : (
                      <span className="hint">{c ? carries(c.version) : "—"}</span>
                    )}
                    <VersionLink version={g.version} system={system} cfg={cfg} />
                  </span>
                  <span className="cd-dgroupright">
                    <span className={`chip ${g.rows.length ? "failed" : "ok"}`}>
                      {g.rows.length} row{g.rows.length === 1 ? "" : "s"}
                    </span>
                    {g.rows.length > 0 && <span className={`chip ${acked === g.rows.length ? "ok" : acked ? "pending" : "muted"}`}>{acked} of {g.rows.length} acknowledged</span>}
                    {c?.queue?.run_id && (
                      <Link className="hint" to={`/r/${c.queue.run_id}/acknowledge`}>
                        acknowledge page
                      </Link>
                    )}
                  </span>
                </div>
                {rows.length === 0 ? <p className="hint cd-panelbody">Nothing of the change {target} will see is first seen on this merge's run.</p> : <DiffRows rows={rows} target={target} source={source} />}
              </div>
            );
          })}
          {d.unattributed.filter(keep).length > 0 && (
            <div className="cd-dgroup">
              <div className="cd-dgrouphead">
                <span className="cd-pr">
                  <span className="cd-prtitle">Not attributed to one merge: seen on {source}, on no merge-queue run</span>
                </span>
                <span className="cd-dgroupright">
                  <span className="chip pending">{d.unattributed.length} row{d.unattributed.length === 1 ? "" : "s"}</span>
                </span>
              </div>
              <DiffRows rows={d.unattributed.filter(keep)} target={target} source={source} />
            </div>
          )}
          {changedRows === 0 && <p className="hint cd-panelbody">Nothing. {source} produced what {target} produces at every address.</p>}
        </section>
      )}
      <p className="hint">
        "= tape" means the run reproduced the recorded value at that address; "absent" means it never reached it; a hash is the canonical form of what it produced instead. Values themselves are not stored, so the before and after are compared by hash.
      </p>
    </>
  );
}
