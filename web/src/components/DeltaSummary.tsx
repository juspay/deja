import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, Delta, DeltaFamily, DeltaSideInfo, deltaFamily, deltaUnavailable } from "../lib/api";
import { useDebug, withDebug } from "../lib/debug";

/**
 * The delta's summary blocks: the two verdicts, the buckets, the coverage
 * line and the lanes. Shared by the report page (as a panel under the
 * verdict banner, when the run was created with `delta_against`) and by the
 * delta page (above the address-level detail).
 */

export const FAMILY_ORDER: DeltaFamily[] = ["changed", "introduced", "resolved", "inherited", "clean"];

export const FAMILY_MEANING: Record<DeltaFamily, string> = {
  changed: "M had already moved this and Y moved it again, differently. The conflict case: flagged, counted against Y.",
  introduced: "M reproduced the tape here; only Y moved it. Counted against Y.",
  resolved: "M had moved it; Y is back on the tape. Not charged, but worth a look.",
  inherited: "M and Y both moved it the same way. Expected on main; not charged.",
  clean: "Both reproduced the tape.",
};

export function TapeVerdict({ info }: { info: DeltaSideInfo }) {
  const v = info.tape_verdict;
  const tone = !v ? "neutral" : v.inconclusive ? "neutral" : v.pass ? "good" : "bad";
  const chip = !v ? "unscored" : v.inconclusive ? "inconclusive" : v.pass ? "pass" : "fail";
  return (
    <div className={`delta-verdict tone-${tone}`}>
      <span className="vq">Y against the tape — today's verdict</span>
      <div className="vrow">
        <span className={`chip solid ${chip}`}>{chip}</span>
      </div>
      <span className="vreason">{v?.reason ?? "The run has no scorecard."}</span>
    </div>
  );
}

export function DeltaVerdict({ d }: { d: Delta }) {
  const v = d.verdict;
  const tone = v.pass ? "good" : "bad";
  return (
    <div className={`delta-verdict tone-${tone}`}>
      <span className="vq">Y against M — the delta</span>
      <div className="vrow">
        <span className={`chip solid ${v.pass ? "pass" : "fail"}`}>{v.pass ? "pass" : "fail"}</span>
        <span className="hint">
          {v.introduced} introduced · {v.changed} changed · {v.inherited} inherited · {v.resolved} resolved
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

/** The comparison's domain, and what fell outside it. A run that stopped
 *  early drove fewer requests; their addresses are not scored either way. */
export function Coverage({ d }: { d: Delta }) {
  const u = d.uncovered;
  if (u.m_only.length === 0 && u.y_only.length === 0) {
    return (
      <p className="hint">
        Compared over all {d.covered_correlations} requests both runs drove.
      </p>
    );
  }
  const part = (who: string, ids: string[]) =>
    ids.length > 0 ? (
      <span title={ids.join("\n")}>
        {ids.length} request{ids.length === 1 ? "" : "s"} only {who} drove
      </span>
    ) : null;
  return (
    <p className="hint delta-coverage">
      Compared over the {d.covered_correlations} requests both runs drove.{" "}
      <b>Not compared:</b> {part("M", u.m_only)}
      {u.m_only.length > 0 && u.y_only.length > 0 ? ", " : ""}
      {part("Y", u.y_only)} ({u.addresses} addresses). A run that stops early is
      not scored as if it had reproduced what it never reached.
    </p>
  );
}

export function Lanes({ d }: { d: Delta }) {
  if (d.lanes.length === 0) return <p className="hint">No connector call placed a request in a lane.</p>;
  return (
    <table className="delta-lanes">
      <thead>
        <tr>
          <th>lane</th>
          <th className="num">requests</th>
          <th className="num">changed</th>
          <th className="num">introduced</th>
          <th className="num">resolved</th>
          <th className="num">inherited</th>
          <th className="num">clean</th>
          <th>reads as</th>
        </tr>
      </thead>
      <tbody>
        {d.lanes.map((l) => {
          const b = l.buckets;
          const n = (f: DeltaFamily) => b[f] ?? 0;
          const note =
            n("changed") > 0
              ? "Y conflicts with M here: both moved this lane, differently."
              : n("introduced") > 0
                ? "Y moved this lane; M had not."
                : n("inherited") > 0 && n("resolved") === 0
                  ? "Inherited from M, Y carries it unchanged."
                  : n("resolved") > 0
                    ? "Y restores the tape where M had moved."
                    : "Same as M.";
          return (
            <tr key={`${l.lane.connector}/${l.lane.flow}`}>
              <td className="lane">{l.lane.connector} · {l.lane.flow}</td>
              <td className="num">{l.requests}</td>
              <td className={`num ${n("changed") ? "hot-changed" : "dim"}`}>{n("changed")}</td>
              <td className={`num ${n("introduced") ? "hot-introduced" : "dim"}`}>{n("introduced")}</td>
              <td className={`num ${n("resolved") ? "hot-resolved" : "dim"}`}>{n("resolved")}</td>
              <td className={`num ${n("inherited") ? "" : "dim"}`}>{n("inherited")}</td>
              <td className={`num ${n("clean") ? "" : "dim"}`}>{n("clean")}</td>
              <td className="note">{note}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}


/** One line for a run that exists to be measured against, not judged. */
export function BaselineNote({ run }: { run: string }) {
  return (
    <p className="hint delta-baseline-note">
      This is a <b>baseline</b> run: main replayed on the tape so that pull requests branched
      from it can be measured against it. Its own verdict against the tape is expected to fail
      whenever main has moved since the recording; that is not a finding about main.
      <span className="mono"> {run}</span>
    </p>
  );
}

/**
 * The delta panel on a run's report: what this run changed relative to the
 * baseline it was created against. Pending while a side is still being
 * scored, and polled only then; a refused pairing says so and stops. The
 * full address-level page is one link away.
 */
export function DeltaPanel({ runId, against }: { runId: string; against: string }) {
  const debug = useDebug();
  const delta = useQuery({
    queryKey: ["delta", runId, against],
    queryFn: () => api.delta(runId, against),
    // only a side still being scored can change the answer; a refusal is final
    refetchInterval: (q) => (deltaUnavailable(q.state.data)?.pending ? 15000 : false),
  });
  const d = delta.data && !("unavailable" in delta.data) ? delta.data : null;
  const unavailable = deltaUnavailable(delta.data);
  return (
    <section className="delta-panel">
      <h2>Against main</h2>
      <p className="hint">
        Measured against <Link to={withDebug(`/r/${against}`, debug)} className="mono">{against}</Link>,
        main at this branch's merge-base on the same tape. A divergence main already carries is
        not charged to this run.
      </p>
      {delta.isLoading && <p className="hint">comparing…</p>}
      {delta.error && <p className="err">{String(delta.error)}</p>}
      {unavailable && (
        <div className="delta-unavailable">
          {unavailable.pending ? "Delta pending" : "No delta"}: {unavailable.why}
        </div>
      )}
      {d && (
        <>
          <div className="delta-verdicts">
            <TapeVerdict info={d.sides.y} />
            <DeltaVerdict d={d} />
          </div>
          <p className="hint delta-baseline-verdict">
            The baseline itself{" "}
            {d.sides.m.tape_verdict
              ? d.sides.m.tape_verdict.pass
                ? "reproduces the tape: main has not moved from what was recorded, so the two verdicts agree."
                : "diverges from the tape: main has moved since the recording, and those divergences read as inherited here."
              : "has no scorecard yet."}
          </p>
          <Buckets d={d} />
          <Coverage d={d} />
          <Lanes d={d} />
          <p className="hint" style={{ marginTop: "var(--s2)" }}>
            <Link to={withDebug(`/r/${runId}/delta?against=${encodeURIComponent(against)}`, debug)}>
              Every address that separates the two runs →
            </Link>
          </p>
        </>
      )}
    </section>
  );
}
