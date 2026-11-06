import React from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import {
  api,
  Delta,
  DeltaFamily,
  DeltaSideInfo,
  RunRow,
  deltaFamily,
  deltaUnavailable,
  runParams,
} from "../lib/api";
import { candidateRef } from "../lib/result";
import { useDebug, withDebug } from "../lib/debug";
import { comparedDiffs, pollPending, readState, sealLine } from "../lib/deltaPanel";
import { AcknowledgeLine } from "./Acknowledgements";
import { deltaRequestCounts, joinDeltaResponses, responseKey, SAME_AS_RECORDING, type DeltaOutcome as Outcome, type DeltaRequest as Req, type FieldSide } from "../lib/comparison";

/**
 * "Compared with main": what a pull request's run changed relative to main
 * at its merge-base, replayed on the same recording.
 *
 * Three things are compared at every point: the recording, main, and this
 * PR. Every outcome is shown as a picture of those three, so the page needs
 * no key. Counts are in requests, the unit the tape verdict uses, with the
 * fields beneath. Where the delta says main and this PR differ from the
 * recording, the run's own http diffs supply the field values, so a reader
 * sees WHAT changed, not a hash.
 */

export const FAMILY_ORDER: DeltaFamily[] = ["changed", "introduced", "resolved", "inherited", "clean"];

/** The outcome of one request, from the families of its addresses. */
export type { DeltaOutcome as Outcome } from "../lib/comparison";

export const OUTCOME_LABEL: Record<Outcome, string> = {
  new: "only this PR differs from the recording",
  conflict: "main and this PR differ, differently",
  restored: "main differs, this PR is back on the recording",
  same: "main and this PR differ from the recording the same way",
  clean: "all three agree",
};

const OUTCOME_ORDER: Outcome[] = ["conflict", "new", "restored", "same", "clean"];

/** Kept for the standalone delta page. */
export const FAMILY_MEANING: Record<DeltaFamily, string> = {
  changed: "Main had already moved this and this PR moved it again, differently. The conflict case: flagged, counted against the PR.",
  introduced: "Main reproduced the recording here; only this PR differs. Counted against the PR.",
  resolved: "Main had moved it; this PR is back on the recording. Not charged.",
  inherited: "Main and this PR both moved it the same way. Expected on main; not charged.",
  clean: "Both reproduced the recording.",
};

// ---------------------------------------------------------------------------
// the three-dot glyph: recording · main · this PR, coloured by who agrees

type Dot = "rec" | "same" | "a" | "b";
function Glyph({ dots }: { dots: [Dot, Dot, Dot] }) {
  return (
    <svg viewBox="0 0 84 26" className="delta-glyph" aria-hidden="true">
      {dots.map((d, i) => (
        <circle key={i} cx={12 + 30 * i} cy={10} r={6} className={`dot ${d}`} />
      ))}
      <text x="12" y="25" textAnchor="middle" className="glyph-lbl">rec</text>
      <text x="42" y="25" textAnchor="middle" className="glyph-lbl">main</text>
      <text x="72" y="25" textAnchor="middle" className="glyph-lbl">PR</text>
    </svg>
  );
}
const GLYPH: Record<Outcome, [Dot, Dot, Dot]> = {
  new: ["rec", "rec", "a"],
  conflict: ["rec", "same", "b"],
  restored: ["rec", "same", "rec"],
  same: ["rec", "same", "same"],
  clean: ["rec", "rec", "rec"],
};

// ---------------------------------------------------------------------------
// per-request view of the delta, joined with both runs' http diffs

export function SideCell({ v, sameAsMain = false }: { v: FieldSide; sameAsMain?: boolean }) {
  if (v === undefined) return <td className="val faint">not published</td>;
  if (v === null) return <td className="val gone">null / absent</td>;
  if (v === SAME_AS_RECORDING) return <td className="val rec">= recording</td>;
  const text = typeof v === "string" ? v : JSON.stringify(v);
  const same = sameAsMain;
  return (
    <td className={`val${same ? " samev" : ""}`} title={text.length > 160 ? text.slice(0, 2000) : undefined}>
      {text.length > 160 ? `${text.slice(0, 160)}…` : text}
      {same && <span className="cmp"> = main</span>}
    </td>
  );
}

function short(id: string) {
  return id.length > 14 ? `…${id.slice(-12)}` : id;
}

function groupByLane(reqs: Req[]): [string, Req[]][] {
  const lanes = new Map<string, Req[]>();
  for (const r of reqs) lanes.set(r.lane, [...(lanes.get(r.lane) ?? []), r]);
  const rank = (rs: Req[]) => Math.min(...rs.map((r) => OUTCOME_ORDER.indexOf(r.outcome)));
  return [...lanes.entries()].sort((a, b) => rank(a[1]) - rank(b[1]) || b[1].length - a[1].length);
}

// ---------------------------------------------------------------------------
// blocks

function Facts({ d, prRun, mainRun }: { d: Delta; prRun?: RunRow; mainRun?: RunRow }) {
  const debug = useDebug();
  const pr = prRun ? runParams(prRun) : null;
  const mp = mainRun ? runParams(mainRun) : null;
  const prCand = prRun ? candidateRef(prRun) : { label: "—", full: "" };
  const mainCand = mainRun ? candidateRef(mainRun) : { label: "—", full: "" };
  const calls = d.rows.filter((r) => r.address.kind === "call").length;
  const notCompared = d.uncovered.m_only.length + d.uncovered.y_only.length;
  return (
    <div className="delta-facts">
      <div className="fact">
        <b>this PR</b>
        <span className="v mono" title={prCand.full}>{prCand.label}</span>
        <span className="s">{pr?.label ?? "the run being judged"} · run {short(d.y_run)}</span>
      </div>
      <div className="fact">
        <b>main at</b>
        <span className="v mono" title={mainCand.full}>{mainCand.label}</span>
        <span className="s">
          {mp?.label ?? "this branch's merge-base"} ·{" "}
          <Link to={withDebug(`/r/${d.m_run}`, debug)} className="mono">main's run {short(d.m_run)}</Link>
        </span>
      </div>
      <div className="fact">
        <b>recording</b>
        <span className="v mono">{d.tape ?? prRun?.recording_id ?? "—"}</span>
        <span className="s">
          {d.covered_correlations} requests both runs drove
          {notCompared > 0 && ` · ${notCompared} driven by only one run (or the recording grew between them), not compared`}
        </span>
        <span className="s mono">{sealLine(d)}</span>
      </div>
      <div className="fact">
        <b>compared</b>
        <span className="v">{d.rows.length + d.clean} points per run</span>
        <span className="s">
          outgoing connector calls, response statuses and response fields{calls > 0 && ` · ${calls} calls differ on some side`} · time and id seams excluded
        </span>
      </div>
    </div>
  );
}

function counts(reqs: Req[]) {
  const n = (o: Outcome) => reqs.filter((r) => r.outcome === o).length;
  return {
    n,
    recVsMain: n("same") + n("conflict") + n("restored"),
    recVsPr: n("same") + n("conflict") + n("new"),
    mainVsPr: n("new") + n("conflict") + n("restored"),
  };
}

export function Headline({ d }: { d: Delta }) {
  const v = d.verdict;
  const total = d.covered_correlations;
  const { n, recVsMain, mainVsPr } = deltaRequestCounts(d);
  const tone = v.pass ? "good" : "bad";
  const chip = v.pass ? "no new blocking difference" : `${n("new") + n("conflict")} requests changed by this PR`;
  const head = v.pass
    ? `The delta attributes no new blocking difference to this PR.${recVsMain > 0 ? ` Main has findings relative to the recording on ${recVsMain} request${recVsMain === 1 ? "" : "s"}.` : ""}`
    : n("conflict") > 0
      ? `${n("conflict")} requests differ from both the recording and main.`
      : `${n("new")} requests differ from the recording on this PR but not on main.`;
  const sha = (run: string) => run.split("-")[2] ?? "";
  return (
    <div className={`delta-headline tone-${tone}`}>
      <div className="txt">
        <div><span className={`rchip ${tone}`}>{chip}</span></div>
        <span className="h">{head}</span>
        <span className="sub">{v.reason}</span>
        <span className="fine">The banner's verdict is against the recording alone and still decides the check.</span>
      </div>
      <svg viewBox="0 0 400 120" role="img" aria-label={`recording to main: ${recVsMain} of ${total} differ; main to this PR: ${mainVsPr} of ${total} differ`}>
        <rect x="8" y="30" width="110" height="60" rx="6" className="node rec" />
        <text x="63" y="50" textAnchor="middle" className="lbl">recording</text>
        <text x="63" y="72" textAnchor="middle" className="small">{(d.tape ?? "").slice(0, 14)}</text>
        <rect x="145" y="30" width="110" height="60" rx="6" className="node" />
        <text x="200" y="50" textAnchor="middle" className="lbl">main at</text>
        <text x="200" y="72" textAnchor="middle" className="small">{sha(d.m_run)}</text>
        <rect x="282" y="30" width="110" height="60" rx="6" className="node" />
        <text x="337" y="50" textAnchor="middle" className="lbl">this PR</text>
        <text x="337" y="72" textAnchor="middle" className="small">{sha(d.y_run)}</text>
        <line x1="118" y1="60" x2="145" y2="60" className={recVsMain ? "link-diff" : "link-same"} />
        <text x="131" y="24" textAnchor="middle" className={`mark ${recVsMain ? "diff" : "same"}`}>{recVsMain ? "≠" : "="}</text>
        <text x="131" y="108" textAnchor="middle" className="small">{recVsMain} of {total}</text>
        <line x1="255" y1="60" x2="282" y2="60" className={mainVsPr ? "link-diff" : "link-same"} />
        <text x="268" y="24" textAnchor="middle" className={`mark ${mainVsPr ? "diff" : "same"}`}>{mainVsPr ? "≠" : "="}</text>
        <text x="268" y="108" textAnchor="middle" className="small">{mainVsPr ? `${mainVsPr} of ${total}` : `${total} of ${total}`}</text>
      </svg>
    </div>
  );
}

function Fold({ title, count, open, children }: { title: string; count?: string; open?: boolean; children: React.ReactNode }) {
  return (
    <details className="delta-box" open={open}>
      <summary className="head">
        <span className="t">{title}</span>
        {count && <span className="c">{count}</span>}
      </summary>
      <div className="body">{children}</div>
    </details>
  );
}

function Matrix({ d, mainKnown }: { d: Delta; mainKnown: boolean }) {
  const total = d.covered_correlations;
  const { recVsMain, recVsPr, mainVsPr } = deltaRequestCounts(d);
  const addresses = (kind: "body" | "status", families: DeltaFamily[]) =>
    d.rows.filter((row) => row.address.kind === kind && families.includes(deltaFamily(row.bucket))).length;
  const word = (i: DeltaSideInfo) => (!i.tape_verdict ? "unscored" : i.tape_verdict.inconclusive ? "inconclusive" : i.tape_verdict.pass ? "accepted" : "diverged");
  // null: main's response diffs are not known, so a count would be a guess
  const cell = (x: number | null, hot: boolean) =>
    x === null ? (
      <td className="num" title="main's response diffs could not be read">—</td>
    ) : (
      <td className={`num ${x ? (hot ? "hot" : "") : "zero"}`}>{x}</td>
    );
  const onMain = (x: number) => (mainKnown ? x : null);
  const row = (k: string, requests: number, fields: number | null, statuses: number | null, verdict: React.ReactNode, hot: boolean) => (
    <tr>
      <td className="k">{k}</td>
      <td className={`num ${requests ? (hot ? "hot" : "") : "zero"}`}>{requests} of {total}</td>
      {cell(fields, hot)}
      {cell(statuses, hot)}
      <td>{verdict}</td>
    </tr>
  );
  return (
    <table className="delta-matrix">
      <thead>
        <tr><th></th><th className="num">requests differing</th><th className="num">response fields</th><th className="num">response statuses</th><th>verdict</th></tr>
      </thead>
      <tbody>
        {row("recording → main", recVsMain, onMain(addresses("body", ["inherited", "changed", "resolved"])), onMain(addresses("status", ["inherited", "changed", "resolved"])), `${word(d.sides.m)} · main's own run against the recording`, true)}
        {row("recording → this PR", recVsPr, addresses("body", ["inherited", "changed", "introduced"]), addresses("status", ["inherited", "changed", "introduced"]), `${word(d.sides.y)} · the banner above`, true)}
        {row("main → this PR", mainVsPr, addresses("body", ["introduced", "changed", "resolved"]), onMain(addresses("status", ["introduced", "changed", "resolved"])),
          <span className={`rchip ${d.verdict.pass ? "good" : "bad"}`}>{d.verdict.pass ? "nothing new" : "changed by this PR"}</span>, !d.verdict.pass)}
      </tbody>
    </table>
  );
}

function Tiles({ d }: { d: Delta }) {
  const { n } = deltaRequestCounts(d);
  const fields: Record<Outcome, number> = { new: d.verdict.introduced, conflict: d.verdict.changed, restored: d.verdict.resolved, same: d.verdict.inherited, clean: 0 };
  return (
    <div className="delta-tiles">
      {OUTCOME_ORDER.map((o) => (
        <div key={o} className={`tile o-${o}${n(o) === 0 ? " zero" : ""}`}>
          <span className="n">{n(o)}{o !== "clean" && <small>{fields[o]} points</small>}</span>
          <span className="g"><Glyph dots={GLYPH[o]} /></span>
          <span className="k">{OUTCOME_LABEL[o]}</span>
        </div>
      ))}
    </div>
  );
}

function ByLane({ reqs }: { reqs: Req[] }) {
  const cell = (rs: Req[], o: Outcome, hot?: string) => {
    const xs = rs.filter((r) => r.outcome === o);
    const fields = xs.reduce((a, r) => a + r.fields.length, 0);
    return (
      <td className={`num ${xs.length ? hot ?? "" : "zero"}`}>
        {xs.length}
        {xs.length > 0 && o !== "clean" && <span className="cmp"> ({fields})</span>}
      </td>
    );
  };
  return (
    <table className="delta-lanes">
      <thead>
        <tr><th>connector · flow</th><th>methods</th><th className="num">responses</th><th className="num">only this PR</th><th className="num">differ differently</th><th className="num">back on recording</th><th className="num">same as main</th><th className="num">all agree</th></tr>
      </thead>
      <tbody>
        {groupByLane(reqs).map(([lane, rs]) => {
          const ms = new Map<string, number>();
          for (const r of rs) ms.set(r.method, (ms.get(r.method) ?? 0) + 1);
          return (
            <tr key={lane}>
              <td className="mono">{lane}</td>
              <td className="mono dim">{[...ms.entries()].map(([m, c]) => `${m} ×${c}`).join(", ")}</td>
              <td className="num">{rs.length}</td>
              {cell(rs, "new", "hot")}
              {cell(rs, "conflict", "hot")}
              {cell(rs, "restored")}
              {cell(rs, "same")}
              {cell(rs, "clean")}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function RequestMap({ reqs, runId }: { reqs: Req[]; runId: string }) {
  const debug = useDebug();
  const { n } = counts(reqs);
  return (
    <div className="delta-map">
      {groupByLane(reqs).map(([lane, rs]) => (
        <div className="row" key={lane}>
          <span className="lane">{lane}</span>
          <div className="cells">
            {rs.map((r) => (
              <Link
                key={responseKey(r.correlation, r.sequence)}
                className={`cell ${r.outcome}`}
                title={`${r.correlation} · ${r.method} · ${r.fields.length} fields differ`}
                to={withDebug(`/r/${runId}?case=${encodeURIComponent(r.correlation)}`, debug)}
              />
            ))}
          </div>
        </div>
      ))}
      <div className="legend">
        <span className="same">same as main · {n("same")}</span>
        <span className="clean">all three agree · {n("clean")}</span>
        <span className="new">only this PR · {n("new")}</span>
        <span className="conflict">differ differently · {n("conflict")}</span>
        {n("restored") > 0 && <span className="restored">back on recording · {n("restored")}</span>}
      </div>
    </div>
  );
}

/** The response fields a group of requests differ on, as presence per side. */
export function FieldTable({ reqs, mainSha }: { reqs: Req[]; mainSha: string }) {
  const byPath = new Map<string, { recording: number; main: number; pr: number; recordingUnknown: number; mainUnknown: number; prUnknown: number; total: number; sameAsMain: number }>();
  for (const r of reqs) {
    for (const f of r.fields) {
      const e = byPath.get(f.path) ?? { recording: 0, main: 0, pr: 0, recordingUnknown: 0, mainUnknown: 0, prUnknown: 0, total: 0, sameAsMain: 0 };
      e.total += 1;
      for (const side of ["recording", "main", "pr"] as const) {
        const value = f[side] === SAME_AS_RECORDING ? f.recording : f[side];
        if (value === undefined) e[`${side}Unknown`] += 1;
        else if (value !== null) e[side] += 1;
      }
      if (f.sameAsMain) e.sameAsMain += 1;
      byPath.set(f.path, e);
    }
  }
  const state = (values: number, unknown: number, total: number) =>
    unknown === total ? <td className="faint">not published</td>
      : unknown > 0 ? <td className="mixed">not published on {unknown} of {total}</td>
      : values === 0 ? <td className="absent">null / absent</td>
      : values === total ? <td className="present">value published</td>
      : <td className="mixed">value on {values} of {total}; rest null / absent</td>;
  return (
    <table className="delta-fields">
      <thead>
        <tr><th>response field</th><th className="col-rec">recording</th><th className="col-main">main at {mainSha}</th><th className="col-pr">this PR</th><th>responses</th></tr>
      </thead>
      <tbody>
        {[...byPath.entries()].sort((a, b) => b[1].total - a[1].total).map(([path, e]) => (
          <tr key={path}>
            <td className="f">{path}</td>
            {state(e.recording, e.recordingUnknown, e.total)}
            {state(e.main, e.mainUnknown, e.total)}
            {state(e.pr, e.prUnknown, e.total)}
            <td className="cnt">
              {e.total} of {reqs.length}
              {e.sameAsMain !== e.total && <span className="cmp"> · {e.sameAsMain} same as main</span>}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function RequestList({ reqs, runId, mainSha }: { reqs: Req[]; runId: string; mainSha: string }) {
  const debug = useDebug();
  const [shown, setShown] = React.useState(40);
  const opened = new Set<string>();
  return (
    <details className="delta-list">
      <summary className="listhead">
        <span>Show the {reqs.length} responses</span>
        <span className="c">expand a row for its field values on all three sides</span>
      </summary>
      <div className="reqhead">
        <span>request</span><span>method</span><span>connector · flow</span><span>status rec / main / PR</span><span>fields differing</span><span></span>
      </div>
      {reqs.slice(0, shown).map((r) => {
        const key = r.fields.map((f) => f.path).join("|");
        const first = !opened.has(key);
        opened.add(key);
        return (
          <details className="req" key={responseKey(r.correlation, r.sequence)} open={first}>
            <summary>
              <span className="id">{short(r.correlation)}</span>
              <span>{r.method}</span>
              <span className="mono">{r.lane}</span>
              <span className="st">{r.status[0]} / {r.status[1] ?? "—"} / {r.status[2]}</span>
              <span className="fl">
                <b>{r.fields.length} field{r.fields.length === 1 ? "" : "s"}</b>
                {r.fields.length > 0 && ` · ${r.fields.map((f) => f.path.replace(/^\$\./, "")).join(", ")}`}
              </span>
              <span className="lk">
                <Link to={withDebug(`/r/${runId}?case=${encodeURIComponent(r.correlation)}`, debug)}>open case →</Link>
              </span>
            </summary>
            {r.notes.map((note) => <p className="hint" key={note}>{note}</p>)}
            {r.fields.length > 0 && (
              <table className="delta-vals">
                <thead>
                  <tr><th>field</th><th className="col-rec">recording</th><th className="col-main">main at {mainSha}</th><th className="col-pr">this PR</th></tr>
                </thead>
                <tbody>
                  {r.fields.map((f) => (
                    <tr key={f.path}>
                      <td className="p">{f.path}</td>
                      <SideCell v={f.recording} />
                      <SideCell v={f.main} />
                      <SideCell v={f.pr} sameAsMain={f.sameAsMain} />
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </details>
        );
      })}
      {shown < reqs.length && (
        <div className="more">
          {reqs.length - shown} more ·{" "}
          <button type="button" onClick={() => setShown((k) => k + 40)}>show next {Math.min(40, reqs.length - shown)}</button>
        </div>
      )}
    </details>
  );
}

const GROUP_TITLE: Record<Outcome, string> = {
  new: "What this PR changed, and main did not",
  conflict: "Where this PR and main both changed the recording, differently",
  restored: "What main changed that this PR put back",
  same: "What main changed from the recording, and this PR carries",
  clean: "Responses with no published delta differences",
};

function OutcomeGroup({ outcome, reqs, runId, mainSha, open }: { outcome: Outcome; reqs: Req[]; runId: string; mainSha: string; open: boolean }) {
  if (reqs.length === 0) return null;
  const fields = reqs.reduce((a, r) => a + r.fields.length, 0);
  const patterns = new Set(reqs.map((r) => r.fields.map((f) => f.path).join("|")));
  return (
    <Fold title={GROUP_TITLE[outcome]} count={`${reqs.length} responses · ${fields} judged response differences · ${patterns.size} pattern${patterns.size === 1 ? "" : "s"}`} open={open}>
      {fields > 0 && <FieldTable reqs={reqs} mainSha={mainSha} />}
      <RequestList reqs={reqs} runId={runId} mainSha={mainSha} />
    </Fold>
  );
}

function CleanList({ reqs }: { reqs: Req[] }) {
  if (reqs.length === 0) return null;
  return (
    <Fold title={GROUP_TITLE.clean} count={`${reqs.length} responses have no published delta differences`}>
      <table>
        <thead><tr><th>request</th><th>method</th><th>connector · flow</th><th>status</th></tr></thead>
        <tbody>
          {reqs.map((r) => (
            <tr key={responseKey(r.correlation, r.sequence)}><td className="mono">{short(r.correlation)}</td><td>{r.method}</td><td className="mono">{r.lane}</td><td className="mono">{r.status[0]}</td></tr>
          ))}
        </tbody>
      </table>
    </Fold>
  );
}

function Calls({ d, reqs }: { d: Delta; reqs: Req[] }) {
  const calls = d.rows.filter((r) => r.address.kind === "call");
  const laneOf = new Map(reqs.map((r) => [r.correlation, r.lane.split(" · ")[0]]));
  const byConn = new Map<string, number>();
  for (const r of calls) {
    const c = laneOf.get(r.address.correlation) ?? r.lane?.connector ?? "—";
    byConn.set(c, (byConn.get(c) ?? 0) + 1);
  }
  return (
    <Fold title="Outgoing connector calls" count={calls.length === 0 ? "identical on all three, for every request" : `${calls.length} calls differ on some side`}>
      {byConn.size === 0 ? (
        <p className="hint">
          Every connector request main and this PR sent matches the recording after canonicalisation. The divergence, if any, is only in what the service returned to its caller.
        </p>
      ) : (
        <table>
          <thead><tr><th>connector</th><th className="num">calls differing</th></tr></thead>
          <tbody>{[...byConn.entries()].map(([c, k]) => <tr key={c}><td className="mono">{c}</td><td className="num">{k}</td></tr>)}</tbody>
        </table>
      )}
    </Fold>
  );
}

// ---------------------------------------------------------------------------
// exported blocks

/** One line for a run that exists to be measured against, not judged. */
export function BaselineNote({ run, label }: { run: string; label?: string }) {
  return (
    <p className="hint delta-baseline-note">
      This is a <b>baseline</b> run{label ? `: ${label}` : ""}. Main replayed on the recording so that pull requests branched
      from it can be measured against it. Its own verdict against the recording is expected to fail whenever main
      has moved since the recording; that is not a finding about main.
      <span className="mono"> {run}</span>
    </p>
  );
}

/** The verdict blocks the standalone delta page still uses. */
export function TapeVerdict({ info }: { info: DeltaSideInfo }) {
  const v = info.tape_verdict;
  const tone = !v ? "neutral" : v.inconclusive ? "neutral" : v.pass ? "good" : "bad";
  const chip = !v ? "unscored" : v.inconclusive ? "inconclusive" : v.pass ? "pass" : "fail";
  return (
    <div className={`delta-verdict tone-${tone}`}>
      <span className="vq">this run against the recording</span>
      <div className="vrow"><span className={`chip solid ${chip}`}>{chip}</span></div>
      <span className="vreason">{v?.reason ?? "The run has no scorecard."}</span>
    </div>
  );
}
export function DeltaVerdict({ d }: { d: Delta }) {
  const v = d.verdict;
  return (
    <div className={`delta-verdict tone-${v.pass ? "good" : "bad"}`}>
      <span className="vq">this run against the other</span>
      <div className="vrow">
        <span className={`chip solid ${v.pass ? "pass" : "fail"}`}>{v.pass ? "pass" : "fail"}</span>
        <span className="hint">
          {v.introduced_requests ?? v.introduced} new · {v.changed_requests ?? v.changed} differ differently · {v.inherited_requests ?? v.inherited} same as the other · {v.resolved_requests ?? v.resolved} back on recording
        </span>
      </div>
      <span className="vreason">{v.reason}</span>
    </div>
  );
}
export function Buckets({ d }: { d: Delta }) {
  const byFamily: Record<DeltaFamily, number> = { clean: d.clean, inherited: 0, introduced: 0, resolved: 0, changed: 0 };
  for (const r of d.rows) byFamily[deltaFamily(r.bucket)] += 1;
  return (
    <div className="delta-buckets">
      {FAMILY_ORDER.map((f) => (
        <div key={f} className={`delta-bucket f-${f}${byFamily[f] === 0 ? " zero" : ""}`}>
          <span className="bn">{byFamily[f]}</span>
          <span className="bk">{f}</span>
          <span className="bw">{FAMILY_MEANING[f].split(".")[0]}.</span>
        </div>
      ))}
    </div>
  );
}
export function Coverage({ d }: { d: Delta }) {
  const u = d.uncovered;
  if (u.m_only.length === 0 && u.y_only.length === 0) {
    return <p className="hint">Compared over all {d.covered_correlations} requests both runs drove.</p>;
  }
  return (
    <p className="hint delta-coverage">
      Compared over the {d.covered_correlations} requests both runs drove. <b>Not compared:</b>{" "}
      {u.m_only.length > 0 && <span title={u.m_only.join("\n")}>{u.m_only.length} only main drove</span>}
      {u.m_only.length > 0 && u.y_only.length > 0 ? ", " : ""}
      {u.y_only.length > 0 && <span title={u.y_only.join("\n")}>{u.y_only.length} only this PR drove</span>} ({u.addresses} points).
    </p>
  );
}
export function Lanes({ d }: { d: Delta }) {
  if (d.lanes.length === 0) return <p className="hint">No connector call placed a request in a lane.</p>;
  return (
    <table className="delta-lanes">
      <thead>
        <tr><th>connector · flow</th><th className="num">requests</th><th className="num">differ differently</th><th className="num">only this run</th><th className="num">back on recording</th><th className="num">same as the other</th><th className="num">all agree</th></tr>
      </thead>
      <tbody>
        {d.lanes.map((l) => {
          const r = l.requests_by_family ?? {};
          const n = (f: DeltaFamily) => r[f] ?? 0;
          return (
            <tr key={`${l.lane.connector}/${l.lane.flow}`}>
              <td className="mono">{l.lane.connector} · {l.lane.flow}</td>
              <td className="num">{l.requests}</td>
              <td className={`num ${n("changed") ? "hot" : "zero"}`}>{n("changed")}</td>
              <td className={`num ${n("introduced") ? "hot" : "zero"}`}>{n("introduced")}</td>
              <td className={`num ${n("resolved") ? "" : "zero"}`}>{n("resolved")}</td>
              <td className={`num ${n("inherited") ? "" : "zero"}`}>{n("inherited")}</td>
              <td className={`num ${n("clean") ? "" : "zero"}`}>{n("clean")}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

/**
 * The panel on a pull request's report: everything above, on the run's own
 * baseline. Pending while a side is still being scored, polled only then.
 */
export function DeltaPanel({ runId, against }: { runId: string; against: string }) {
  const delta = useQuery({
    queryKey: ["delta", runId, against],
    queryFn: () => api.delta(runId, against),
    refetchInterval: (q) => (pollPending(q.state.data, q.state.dataUpdateCount) ? 15000 : false),
  });
  const d = delta.data && !("unavailable" in delta.data) ? delta.data : null;
  const unavailable = deltaUnavailable(delta.data);
  const prRun = useQuery({ queryKey: ["run", runId], queryFn: () => api.run(runId) });
  const mainRun = useQuery({ queryKey: ["run", against], queryFn: () => api.run(against) });
  const prDiffs = useQuery({ queryKey: ["httpdiffs", runId], queryFn: () => api.httpDiffs(runId), enabled: !!d });
  const mainDiffs = useQuery({ queryKey: ["httpdiffs", against], queryFn: () => api.httpDiffs(against), enabled: !!d });
  const prRead = readState({ data: prDiffs.data, error: prDiffs.error });
  const mainRead = readState({ data: mainDiffs.data, error: mainDiffs.error });
  const reqs = React.useMemo(
    () =>
      d && prDiffs.data && !prDiffs.error
        ? joinDeltaResponses(d, comparedDiffs(d, prDiffs.data), mainDiffs.error ? [] : mainDiffs.data ?? [], prRun.data?.scorecard, mainRun.data?.scorecard)
        : [],
    [d, prDiffs.data, prDiffs.error, mainDiffs.data, mainDiffs.error, prRun.data?.scorecard, mainRun.data?.scorecard],
  );
  const deltaUpdates = useQueryClient().getQueryState(["delta", runId, against])?.dataUpdateCount ?? 0;
  const stillPolling = pollPending(delta.data, deltaUpdates);
  const mainSha = against.split("-")[2] ?? against;
  return (
    <section className="delta-panel">
      <h2>Compared with main</h2>
      {delta.isLoading && <p className="hint">comparing…</p>}
      {delta.error && <p className="err">{String(delta.error)}</p>}
      {unavailable &&
        (unavailable.tapeMismatch ? (
          <div className="delta-unavailable warn">
            <b>Not comparable</b>: {unavailable.why}. The two runs scored different recordings, so neither can be read
            against the other.
          </div>
        ) : (
          <div className="delta-unavailable">
            {unavailable.pending
              ? stillPolling
                ? "Delta pending"
                : "Delta still pending after ten minutes; reload to check again"
              : "No delta"}
            : {unavailable.why}
          </div>
        ))}
      {d && (
        <>
          <Facts d={d} prRun={prRun.data} mainRun={mainRun.data} />
          {prRead.state === "failed" ? (
            <p className="err">
              This PR's response diffs could not be read, so the panel cannot say which requests differ: {prRead.reason}
            </p>
          ) : prRead.state === "pending" ? (
            <p className="hint">loading the responses…</p>
          ) : (
            <Headline d={d} />
          )}
          <AcknowledgeLine runId={runId} d={d} />
          {mainRead.state === "failed" && (
            <p className="err">
              Main's response diffs could not be read, so its field and status columns are unknown (—): {mainRead.reason}
            </p>
          )}
          {mainRead.state === "pending" && prRead.state === "ok" && <p className="hint">loading main's responses…</p>}
          {prRead.state === "ok" && reqs.length > 0 && (
            <>
              <Fold title="The three comparisons, in numbers" count="requests, then what differed inside them" open>
                <Matrix d={d} mainKnown={mainRead.state === "ok"} />
              </Fold>
              <Tiles d={d} />
              <p className="hint">The response groups below use published status and body delta addresses. Call changes are listed separately.</p>
              <Fold title="Responses by connector and flow" count="responses per outcome; judged differences in parentheses" open>
                <ByLane reqs={reqs} />
              </Fold>
              <Fold title="Every response" count="one square per response, in recording order; hover for the response, click to open its case" open>
                <RequestMap reqs={reqs} runId={runId} />
              </Fold>
              {OUTCOME_ORDER.filter((o) => o !== "clean").map((o) => (
                <OutcomeGroup key={o} outcome={o} reqs={reqs.filter((r) => r.outcome === o)} runId={runId} mainSha={mainSha} open={o !== "same" || d.verdict.pass} />
              ))}
              <CleanList reqs={reqs.filter((r) => r.outcome === "clean")} />
              <Calls d={d} reqs={reqs} />
            </>
          )}
          <Coverage d={d} />
        </>
      )}
    </section>
  );
}
