import { RunResult } from "../lib/result";

/** The run result as a chip. One vocabulary, five words, used everywhere. */
export function ResultChip({ result }: { result: RunResult }) {
  return (
    <span className={`rchip tone-${result.tone}`} title={result.headline}>
      {result.label}
    </span>
  );
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
