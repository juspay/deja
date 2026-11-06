import type { CallRecord, HttpDiff, Scorecard } from "../lib/api";
import { acceptedBodyReasons, callComparisonText, httpJudgment } from "../lib/comparison";
import { transportFailure } from "../lib/spine";
import { JsonDiff } from "./JsonDiff";
import { ValuePair } from "./JsonView";

export function CallComparison({ call }: { call: CallRecord }) {
  return <p className="hint">{callComparisonText(call)}</p>;
}

export function CorrelationOutcome({ row }: { row: NonNullable<Scorecard["per_correlation"]>[number] }) {
  const text = row.inconclusive ? "inconclusive" : row.passed === undefined ? "not published" : row.passed ? "accepted" : "diverged";
  const tone = row.inconclusive || row.passed === undefined ? "muted" : row.passed ? "pass" : "fail";
  return <span className={`chip ${tone}`}>{text}</span>;
}

/** Captured bodies remain inspectable; only published residuals are findings. */
export function ResponseEvidence({ diff, scorecard }: { diff: HttpDiff; scorecard?: Scorecard | null }) {
  const judgment = httpJudgment(scorecard, diff);
  const failed = transportFailure(diff);
  const accepted = judgment.kind === "published" ? acceptedBodyReasons(judgment.body) : [];
  return (
    <div className="evidence ev-http">
      <div className="evhead">
        <span className="bm"><b>{diff.request_path}</b> · seq {diff.request_sequence}</span>
        <span className="statuspill">
          <span>{diff.status_baseline}</span> <span className="sep">→</span>{" "}
          <span className={diff.status_match ? "ok" : "bad"}>{failed ? "no response" : diff.status_candidate}</span>
        </span>
      </div>
      {failed && <p className="evwhat">The candidate produced no parseable response: {failed}. This is one response failure.</p>}
      {!failed && !diff.status_match && <p className="evwhat">The recorded status was {diff.status_baseline}; the candidate answered {diff.status_candidate}.</p>}
      {judgment.kind === "published" ? (
        <>
          <p className="evwhat">
            {failed
              ? "The scorer's body differences accompany this response failure; the raw evidence remains available below."
              : judgment.body.blocking.length
              ? `${judgment.body.blocking.length} blocking response difference${judgment.body.blocking.length === 1 ? "" : "s"} published by the scorer.`
              : judgment.body.race_absorbed_paths?.length
                ? "No blocking body differences; race attribution is published below."
                : "Body accepted by the scorer's comparison."}
          </p>
          {!failed && judgment.body.blocking.map((field, index) => (
            <div className="leafdiff" key={`${field.json_path}:${index}`}>
              <div className="jpath">{field.json_path}</div>
              <ValuePair baseline={field.baseline} candidate={field.candidate} />
            </div>
          ))}
          {accepted.length > 0 && (
            <div className="evnote">
              <b>Differences set aside by the scorer</b>
              <ul>{accepted.map((item, index) => <li key={`${item.path}:${index}`}><code>{item.path}</code> — {item.reason}</li>)}</ul>
            </div>
          )}
          {judgment.body.canon_conflicts.length > 0 && <p className="evwarn">Comparison declarations conflict at {judgment.body.canon_conflicts.join(", ")}.</p>}
        </>
      ) : judgment.kind === "historical" ? (
        <p className="hint">
          {judgment.accepted === true ? "Body accepted by the stored scorer judgment. " : ""}
          Classified response details were not published for this historical run. The raw inspection below does not determine the verdict.
        </p>
      ) : <p className="evwarn">{judgment.reason} The run's published verdict remains authoritative.</p>}
      <details className="evraw">
        <summary>Raw captured response · {diff.body_diff.length} raw diff row{diff.body_diff.length === 1 ? "" : "s"}</summary>
        <p className="hint">Captured values and their raw rendering. These highlights are not behavioral findings.</p>
        {diff.body_diff_address_error !== undefined && <p className="hint">Raw field addresses are unavailable: {diff.body_diff_address_error || "the server could not safely address these rows."}</p>}
        <JsonDiff before={diff.baseline_body} after={diff.candidate_body} />
      </details>
    </div>
  );
}
