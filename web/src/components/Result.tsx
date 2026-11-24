import { RunResult, ScoredRun, resultOf } from "../lib/result";
import { runParams } from "../lib/api";

/** The run result as a chip. One vocabulary, five words, used everywhere. */
export function ResultChip({ result }: { result: RunResult }) {
  return (
    <span className={`rchip tone-${result.tone}`} title={result.headline}>
      {result.label}
    </span>
  );
}

/** A list row names both comparisons, without changing either published result. */
export function RunListResult({ run }: { run: ScoredRun }) {
  const result = resultOf(run);
  const params = runParams(run);
  const against = params?.delta_against;
  const scored = result.state === "REPRODUCED" || result.state === "DIVERGED";
  if (!scored) {
    return <><ResultChip result={result} />{against && <span className="recording-outcome">PR: {result.state === "RUNNING" ? "pending" : "unavailable"}</span>}</>;
  }
  const fidelity = result.state === "REPRODUCED" ? "reproduced" : "differs";
  if (!against || params?.purpose === "baseline") {
    return params?.purpose === "baseline"
      ? <><span className="rchip tone-neutral">baseline reference</span><span className="recording-outcome" title={result.headline}>Recording: {fidelity}</span></>
      : <ResultChip result={{ ...result, label: `recording ${fidelity}` }} />;
  }
  const impact = run.delta_verdict === "pass" ? { label: "no new blocking diff", tone: "good" }
    : run.delta_verdict === "acknowledged" ? { label: "changes acknowledged", tone: "good" }
    : run.delta_verdict === "fail" ? { label: "differences to review", tone: "bad" }
    : run.delta_verdict === "pending" ? { label: "pending", tone: "neutral" }
    : run.delta_verdict === "refused" ? { label: "not comparable", tone: "warn" }
    : { label: "unavailable", tone: "warn" };
  return <>
    <span className={`rchip tone-${impact.tone}`} title={`PR impact compared with baseline ${against}: ${impact.label}. Limited to the traffic these runs compared.`}>PR: {impact.label}</span>
    <span className="recording-outcome" title={`${result.headline}. This is separate from PR impact.`}>Recording: {fidelity}</span>
  </>;
}

/**
 * The verdict, stated once, at the top of the report.
 *
 * Everything it says comes from `resultOf(run)` — the run row alone. It cannot
 * render green without `scorecard.verdict.pass === true`, and it always shows
 * the server's own sentence (`verdict.reason` or `failure.message`) rather than
 * a sentence the client made up from array lengths.
 */
export function VerdictBanner({ result }: { result: RunResult }) {
  return (
    <div className={`verdict tone-${result.tone}`}>
      <div className="verdict-top">
        <ResultChip result={result} />
        <span className="verdict-headline">{result.headline}</span>
      </div>
      {result.guard && <div className="verdict-guard">{result.guard}</div>}
      {result.detail && <pre className="verdict-detail">{result.detail}</pre>}
    </div>
  );
}
