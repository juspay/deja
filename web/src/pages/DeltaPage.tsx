import React from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams, useSearchParams } from "react-router-dom";
import {
  api,
  DeltaAddress,
  DeltaFamily,
  DeltaRow,
  DeltaSide,
  DeltaSideInfo,
  deltaFamily,
  deltaUnavailable,
  RunSummaryRow,
} from "../lib/api";
import { candidateRef } from "../lib/result";
import { useDebug, withDebug } from "../lib/debug";
import {
  Buckets,
  Coverage,
  DeltaVerdict,
  FAMILY_MEANING,
  FAMILY_ORDER,
  Lanes,
  TapeVerdict,
} from "../components/DeltaSummary";

/**
 * The delta page: what this run (Y) changed relative to another run of the
 * same tape (M), three-way against the tape.
 *
 * It is a separate page because it answers a different question from the
 * report. The report asks "does Y reproduce the tape?"; this page asks "does Y
 * behave as M does?", which is what a PR should be judged on once main itself
 * has moved away from the tape. The tape-relative verdict is shown beside the
 * delta verdict, never replaced by it.
 */

function sideLabel(s: DeltaSide): { text: string; cls: string } {
  if (s === "tape") return { text: "= tape", cls: "tape" };
  if (s === "absent") return { text: "absent", cls: "absent" };
  return { text: s.hash, cls: "hash" };
}

function sideEq(a: DeltaSide, b: DeltaSide): boolean {
  if (typeof a === "string" || typeof b === "string") return a === b;
  return a.hash === b.hash;
}

/** The address, read as a reader would: request, then the site under it. */
function AddressCell({ a }: { a: DeltaAddress }) {
  const corr = a.correlation.length > 18 ? `…${a.correlation.slice(-14)}` : a.correlation;
  if (a.kind === "status") {
    return (
      <span>
        <span className="corr" title={a.correlation}>{corr}</span>{" › "}
        <span className="leaf">response status</span>
        <span className="site"> #{a.request_sequence}</span>
      </span>
    );
  }
  if (a.kind === "body") {
    return (
      <span>
        <span className="corr" title={a.correlation}>{corr}</span>{" › "}
        <span className="leaf">response body</span>{" "}
        <span className="site">{a.json_path}</span>
      </span>
    );
  }
  const spans = a.span_path.split(">").filter(Boolean);
  const leaf = spans[spans.length - 1] ?? "";
  return (
    <span>
      <span className="corr" title={a.correlation}>{corr}</span>{" › "}
      <span className="site" title={a.span_path}>{leaf}</span>{" › "}
      <span className="leaf">
        {a.boundary}·{a.operation}
      </span>
      {a.occurrence > 0 && <span className="site"> #{a.occurrence + 1}</span>}
    </span>
  );
}

function bucketWord(b: DeltaRow["bucket"]): string {
  return b.replace("_", " ");
}

const PAGE = 40;

function FamilyFold({
  family,
  rows,
  open,
}: {
  family: DeltaFamily;
  rows: DeltaRow[];
  open: boolean;
}) {
  const [shown, setShown] = React.useState(PAGE);
  if (rows.length === 0) return null;
  const visible = rows.slice(0, shown);
  const blocking = rows.filter((r) => r.blocking).length;
  return (
    <details className={`delta-family f-${family}`} open={open}>
      <summary>
        <span className="fn">{rows.length}</span>
        <span>{family}</span>
        <span className="fw">
          {FAMILY_MEANING[family]}
          {blocking !== rows.length && ` ${blocking} of these are on blocking boundaries.`}
        </span>
      </summary>
      <table>
        <thead>
          <tr>
            <th>address</th>
            <th>M produced</th>
            <th>Y produced</th>
            <th>bucket</th>
            <th>lane</th>
          </tr>
        </thead>
        <tbody>
          {visible.map((r, i) => {
            const m = sideLabel(r.m);
            const y = sideLabel(r.y);
            const same = sideEq(r.m, r.y);
            return (
              <tr key={i}>
                <td className="addr"><AddressCell a={r.address} /></td>
                <td className={`val ${m.cls}${same ? " same" : ""}`}>{m.text}</td>
                <td className={`val ${y.cls}${same ? " same" : ""}`}>{y.text}</td>
                <td className="bk">
                  {bucketWord(r.bucket)}
                  {!r.blocking && <span className="nb"> · non-blocking</span>}
                </td>
                <td className="bk">{r.lane ? `${r.lane.connector} · ${r.lane.flow}` : "—"}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {shown < rows.length && (
        <div className="more">
          {rows.length - shown} more ·{" "}
          <button type="button" onClick={() => setShown((n) => n + PAGE)}>
            show next {Math.min(PAGE, rows.length - shown)}
          </button>
        </div>
      )}
    </details>
  );
}

function SideCard({ role, info, hint }: { role: string; info: DeltaSideInfo; hint: string }) {
  const debug = useDebug();
  // The candidate rides the delta when the orchestrator knows the run's
  // parameters; otherwise the run row names it.
  const row = useQuery({
    queryKey: ["run", info.run],
    queryFn: () => api.run(info.run),
    enabled: !info.candidate,
  });
  const candidate = info.candidate ?? row.data?.candidate ?? null;
  const cand = candidate ? candidateRef({ candidate } as never) : { label: "—", full: "" };
  return (
    <div className="delta-side">
      <span className="role">{role}</span>
      <Link className="rid" to={withDebug(`/r/${info.run}`, debug)}>{info.run}</Link>
      <span className="cand" title={cand.full}>{cand.label}</span>
      <span className="hint">{hint}</span>
    </div>
  );
}

/** Sibling runs on the same tape, newest first, so M can be picked rather than pasted. */
function AgainstPicker({
  runId,
  tape,
  value,
  onChange,
}: {
  runId: string;
  tape: string | null;
  value: string;
  onChange: (v: string) => void;
}) {
  const runs = useQuery({ queryKey: ["runs"], queryFn: () => api.runs() });
  const siblings = (runs.data ?? []).filter(
    (r: RunSummaryRow) =>
      r.run_id !== runId && r.mode === "replay" && (!tape || r.recording_id === tape),
  );
  const [typed, setTyped] = React.useState(value);
  React.useEffect(() => setTyped(value), [value]);
  return (
    <div className="delta-head">
      <label className="field">
        <span>against (M)</span>
        <select
          id="delta-against"
          value={siblings.some((r) => r.run_id === value) ? value : ""}
          onChange={(e) => e.target.value && onChange(e.target.value)}
        >
          <option value="">{siblings.length ? "pick a run on this tape…" : "no other run on this tape"}</option>
          {siblings.map((r) => {
            const c = candidateRef(r as never);
            return (
              <option key={r.run_id} value={r.run_id}>
                {r.run_id} · {c.label} · {r.verdict ?? r.state}
              </option>
            );
          })}
        </select>
      </label>
      <label className="field">
        <span>or a run id</span>
        <input
          id="delta-against-id"
          type="text"
          value={typed}
          placeholder="rp-…"
          onChange={(e) => setTyped(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && typed.trim()) onChange(typed.trim());
          }}
          onBlur={() => typed.trim() && typed.trim() !== value && onChange(typed.trim())}
        />
      </label>
    </div>
  );
}

export default function DeltaPage() {
  const { runId = "" } = useParams();
  const [params, setParams] = useSearchParams();
  const debug = useDebug();
  const against = params.get("against") ?? "";

  const run = useQuery({ queryKey: ["run", runId], queryFn: () => api.run(runId) });
  const delta = useQuery({
    queryKey: ["delta", runId, against],
    queryFn: () => api.delta(runId, against),
    enabled: !!against,
  });

  const setAgainst = (v: string) =>
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (v) next.set("against", v);
        else next.delete("against");
        return next;
      },
      { replace: true },
    );

  const tape = run.data?.recording_id ?? null;
  const d = delta.data && !("unavailable" in delta.data) ? delta.data : null;
  const unavailable = deltaUnavailable(delta.data);

  const byFamily = React.useMemo(() => {
    const out: Record<DeltaFamily, DeltaRow[]> = { changed: [], introduced: [], resolved: [], inherited: [], clean: [] };
    for (const r of d?.rows ?? []) out[deltaFamily(r.bucket)].push(r);
    return out;
  }, [d]);

  return (
    <>
      <div className="crumb">
        <Link to={withDebug("/runs", debug)}>Runs</Link> <span>/</span>{" "}
        <Link to={withDebug(`/r/${runId}`, debug)}>report</Link> <span>/</span>{" "}
        <span className="hint">delta</span>
      </div>
      <h1>Delta</h1>
      <p className="hint" style={{ maxWidth: "80ch" }}>
        What <code>{runId}</code> (Y) changed relative to another run of the same tape (M), with the
        tape as the common ancestor. A divergence M already carries is expected on main and is not
        charged to Y; only what Y introduced, or changed where M had already moved, counts.
      </p>

      <AgainstPicker runId={runId} tape={tape} value={against} onChange={setAgainst} />

      {!against && (
        <div className="delta-unavailable">Pick the run to measure against: main at the merge-base, or any other run of this tape.</div>
      )}
      {against && delta.isLoading && <p className="hint">comparing…</p>}
      {against && delta.error && <p className="err">{String(delta.error)}</p>}
      {unavailable && (
        <div className="delta-unavailable">
          {unavailable.pending ? "Delta pending" : "No delta"}: {unavailable.why}
        </div>
      )}

      {d && (
        <>
          <div className="delta-sides">
            <SideCard role="M · baseline" info={d.sides.m} hint="What main (or the chosen run) produced on this tape." />
            <SideCard role="Y · this run" info={d.sides.y} hint="The run being judged." />
          </div>
          <div className="delta-verdicts">
            <TapeVerdict info={d.sides.y} />
            <DeltaVerdict d={d} />
          </div>

          <section>
            <h2>Addresses by bucket</h2>
            <Buckets d={d} />
            <Coverage d={d} />
          </section>

          <section>
            <h2>Lanes</h2>
            <Lanes d={d} />
            <p className="delta-legend" style={{ marginTop: "var(--s2)" }}>
              A lane is one connector and one flow, read off the connector call. Lanes with a
              conflict come first, then lanes Y moved on its own.
            </p>
          </section>

          <section>
            <h2>Every address that separates M from Y</h2>
            {d.rows.length === 0 ? (
              <p className="hint">None. Y produced exactly what M produced at every address.</p>
            ) : (
              FAMILY_ORDER.filter((f) => f !== "clean").map((f) => (
                <FamilyFold key={f} family={f} rows={byFamily[f]} open={f === "changed" || f === "introduced"} />
              ))
            )}
            <p className="delta-legend">
              "= tape" means the run reproduced the recorded value at that address; "absent" means
              the run never reached it; a hash is the canonical form of what the run produced
              instead. Tape {d.tape ?? tape ?? "—"}, canonicalisation v{d.canon_version}.
            </p>
          </section>
        </>
      )}
    </>
  );
}
