import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { api } from "../lib/api";
import { ConfidenceBadge, ConfidenceLadder, overallConfidence } from "../components/Confidence";

export default function ScorecardPage() {
  const { runId = "" } = useParams();
  const card = useQuery({ queryKey: ["scorecard", runId], queryFn: () => api.scorecard(runId) });
  const calls = useQuery({ queryKey: ["calls", runId], queryFn: () => api.calls(runId) });

  if (card.isLoading) return <p className="hint">loading…</p>;
  if (card.error || !card.data) return <p className="err">{String(card.error)}</p>;
  const c = card.data;
  const s = c.summary;
  const verdict = c.verdict.inconclusive ? "inconclusive" : c.verdict.pass ? "pass" : "fail";
  const conf = overallConfidence(s.resolved_by_rank ?? {});
  const valueDivergences = s.value_divergences ?? 0;
  const seedGaps = s.inconclusive_seed_gaps ?? 0;
  const caught = valueDivergences > 0;
  const boundaries = Object.entries(c.per_boundary ?? {});

  // Value-divergence cascade: calls grouped by correlation and ordered origin
  // (read) first, then consequences (writes), then by recorded seq.
  const fmt = (v: unknown) => (typeof v === "string" ? v : v == null ? "∅" : JSON.stringify(v));
  const cascades = new Map<string, typeof calls.data>();
  for (const r of calls.data ?? []) {
    if (r.kind !== "value_diverged") continue;
    const key = r.correlation_id ?? "(uncorrelated)";
    (cascades.get(key) ?? cascades.set(key, []).get(key)!)!.push(r);
  }
  for (const rows of cascades.values()) {
    rows!.sort(
      (a, b) =>
        Number(!!b.origin) - Number(!!a.origin) ||
        (a.source_event_global_sequence ?? 0) - (b.source_event_global_sequence ?? 0),
    );
  }

  return (
    <>
      <div className="crumb">
        <Link to="/runs">Runs</Link> <span>/</span> <Link to={`/runs/${runId}`}>{runId}</Link>{" "}
        <span>/</span> <span>scorecard</span>
      </div>
      <h1>Scorecard</h1>

      <div className={`scorestrip ${verdict === "pass" ? "clean" : "diverged"}`}>
        <span className={`chip solid ${verdict}`}>{verdict}</span>
        <ConfidenceBadge level={conf} title="overall run confidence = weakest address rank relied on" />
        <span className="lede">{c.verdict.reason}</span>
      </div>

      {caught && (
        <div className="catchbanner">
          <span className="chip solid fail">CAUGHT</span>
          <div className="catchbanner-body">
            <strong>
              {valueDivergences} value divergence{valueDivergences === 1 ? "" : "s"} — Execute boundary differed
            </strong>
            <span className="hint">
              An Execute boundary ran during replay and produced a value differing from the recorded
              baseline at the same call-site. Lookup/Substitute boundaries serve recorded values.
            </span>
          </div>
        </div>
      )}

      {cascades.size > 0 && (
        <>
          <h2>Value-divergence cascade</h2>
          {[...cascades.entries()].map(([corr, rows]) => (
            <div className="panel" key={corr} style={{ marginBottom: "var(--s3)" }}>
              <div className="mono hint" style={{ marginBottom: "var(--s2)" }}>{corr}</div>
              <div className="cascade">
                {rows!.map((r, i) => (
                  <div key={i} className="cascade-step" style={{ display: "flex", alignItems: "center", gap: "var(--s2)", marginBottom: 6 }}>
                    <span className={`chip solid ${r.origin ? "fail" : "removed"}`}>
                      {r.origin ? "ORIGIN" : "CONSEQUENCE"}
                    </span>
                    <span className="mono">{r.boundary}.{r.method_name}</span>
                    <span className="diffval">
                      <span className="chip removed">{fmt(r.recorded?.result)}</span>
                      <span style={{ margin: "0 6px" }}>→</span>
                      <span className="chip added">{fmt(r.observed?.result)}</span>
                    </span>
                    {i < rows!.length - 1 && <span className="hint">↓ flows to</span>}
                  </div>
                ))}
              </div>
              <p className="hint" style={{ marginTop: "var(--s2)" }}>
                Causal link inferred at the boundary cut — dataflow BETWEEN boundaries is not traced.
                The read (origin) and write (consequence) fire under the same span (boundary-only
                granularity), so the order/edge is the inference, not a recorded edge.
              </p>
            </div>
          ))}
        </>
      )}

      <div className="panel grid">
        <div className={`metric${caught ? " hot" : ""}`}>
          <div className="v">{valueDivergences}</div>
          <div className="k">value divergences (Execute boundary differed)</div>
        </div>
        <div className="metric">
          <div className="v">{seedGaps}</div>
          <div className="k">inconclusive seed gaps</div>
        </div>
      </div>

      <div className="panel grid">
        <div className="metric"><div className="v">{s.matched_correlations}/{s.total_correlations}</div><div className="k">correlations matched</div></div>
        <div className="metric"><div className="v">{s.http_status_mismatches}</div><div className="k">http status mismatches</div></div>
        <div className="metric"><div className="v">{s.http_body_mismatches}</div><div className="k">http body mismatches</div></div>
        <div className="metric"><div className="v">{s.side_effect_divergences}</div><div className="k">side-effect divergences</div></div>
        <div className="metric"><div className="v">{s.omitted_calls ?? 0}</div><div className="k">omitted calls</div></div>
        <div className="metric"><div className="v">{s.novel_calls ?? 0}</div><div className="k">novel calls</div></div>
      </div>

      <h2>Resolution confidence</h2>
      <div className="panel">
        <ConfidenceLadder byRank={s.resolved_by_rank ?? {}} />
        <p className="hint" style={{ marginTop: "var(--s3)" }}>
          Confidence is the weakest address rank a match relied on. CERTAIN/HIGH are version-independent;
          UNMATCHED (positional) survived on ordering alone and is the fragility signal.
        </p>
      </div>

      {boundaries.length > 0 && (
        <>
          <h2>Per-boundary channels</h2>
          <table>
            <thead>
              <tr>
                <th>boundary</th>
                <th>tier</th>
                <th className="num">matched</th>
                <th className="num">diverged</th>
                <th>kinds</th>
              </tr>
            </thead>
            <tbody>
              {boundaries.map(([name, b]) => {
                const kinds = Object.entries(b.kinds ?? {}).filter(([, n]) => n > 0);
                return (
                  <tr key={name}>
                    <td className="mono">{name}</td>
                    <td>{b.tier ?? "—"}</td>
                    <td className="num">{b.matched ?? 0}</td>
                    <td className="num">
                      {b.diverged ? (
                        <span className="chip fail">{b.diverged}</span>
                      ) : (
                        0
                      )}
                    </td>
                    <td>
                      {kinds.length === 0
                        ? "—"
                        : kinds.map(([k, n]) => (
                            <span key={k} className="chip muted" style={{ marginRight: 4 }}>
                              {k}: {n}
                            </span>
                          ))}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </>
      )}

      {c.per_correlation && c.per_correlation.length > 0 && (
        <>
          <h2>Per-request</h2>
          <table>
            <thead>
              <tr>
                <th>correlation</th>
                <th>http status</th>
                <th>http body</th>
                <th className="num">side-effects</th>
                <th>outcome</th>
              </tr>
            </thead>
            <tbody>
              {c.per_correlation.map((row) => (
                <tr key={row.correlation_id} className="clickable" onClick={() => (window.location.href = `/runs/${runId}?tab=diff`)}>
                  <td className="mono">{row.correlation_id}</td>
                  <td><span className={`chip ${row.http_status_match ? "pass" : "fail"}`}>{row.http_status_match ? "match" : "mismatch"}</span></td>
                  <td><span className={`chip ${row.http_body_match ? "pass" : "fail"}`}>{row.http_body_match ? "match" : "mismatch"}</span></td>
                  <td className="num">{row.side_effect_divergences ?? 0}</td>
                  <td><span className={`chip ${row.passed ? "pass" : "fail"}`}>{row.passed ? "pass" : "diverged"}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </>
  );
}
