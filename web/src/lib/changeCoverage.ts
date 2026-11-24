import { runParams, type ChangeCoverage, type ChangedItem, type RunRow } from "./api";

export type CoverageBucket = "observed" | "not_exercised" | "unproven";
export type CoverageFilter = CoverageBucket | "needs_review" | "all";

export function coverageBucket(item: ChangedItem): CoverageBucket {
  if (item.reach === "exercised") return "observed";
  return item.reach === "not_exercised" ? "not_exercised" : "unproven";
}

const fullSha = (sha: string | undefined): sha is string => !!sha && /^[a-f0-9]{40}$/i.test(sha);
const sameScope = (a: string[], b: string[]) => {
  const left = [...new Set(a)].sort(), right = [...new Set(b)].sort();
  return left.length === right.length && left.every((id, i) => id === right[i]);
};

/** A source link identifies a published item location, never a coverage heatmap. */
export function coverageSourceHref(coverage: ChangeCoverage, item: ChangedItem): string | null {
  if (!/^[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+$/.test(coverage.repo) || coverage.repo.split("/").some((part) => part === "." || part === "..") || !fullSha(coverage.head)) return null;
  const segments = item.path.split("/");
  if (!item.path || segments.some((part) => !part || part === "." || part === "..") || /[\\\u0000-\u001f\u007f]/.test(item.path)) return null;
  const match = /^(\d+)(?:-(\d+))?$/.exec(item.lines);
  if (!match) return null;
  const start = Number(match[1]), end = Number(match[2] ?? match[1]);
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 1 || end < start) return null;
  return `https://github.com/${coverage.repo}/blob/${coverage.head}/${segments.map(encodeURIComponent).join("/")}#L${start}${end !== start ? `-L${end}` : ""}`;
}

export function summarizeChangeCoverage(coverage: ChangeCoverage, run: RunRow) {
  const counts: Record<CoverageBucket, number> = { observed: 0, not_exercised: 0, unproven: 0 };
  for (const item of coverage.items) counts[coverageBucket(item)]++;
  const warnings: string[] = [];
  const params = runParams(run);
  const captured = params?.github;
  const spec = params?.candidate_spec ?? run.candidate;
  const sourceHead = captured?.head_sha ?? (spec.kind === "repo_sha" && typeof spec.sha === "string" ? spec.sha : undefined);
  const sourceRepo = captured?.repo ?? (spec.kind === "repo_sha" && typeof spec.repo === "string" ? spec.repo : undefined);
  let sourceMatches = fullSha(sourceHead) && fullSha(coverage.head) && !!sourceRepo && sourceHead.toLowerCase() === coverage.head.toLowerCase() && sourceRepo.toLowerCase() === coverage.repo.toLowerCase();
  if (coverage.assessment_version !== 3 || !coverage.base || !fullSha(coverage.base.resolved_sha) || !/^[a-f0-9]{64}$/i.test(coverage.inputs_digest ?? "") || !Array.isArray(coverage.request_scope)) {
    warnings.push("Historical or incomplete assessment: immutable source and request-scope provenance are not fully published. Treat item reach as provisional.");
  }
  if (!fullSha(sourceHead) || !fullSha(coverage.head) || !sourceRepo) {
    warnings.push("Coverage source cannot be verified against a complete captured candidate repository and commit.");
  } else if (sourceHead.toLowerCase() !== coverage.head.toLowerCase() || sourceRepo.toLowerCase() !== coverage.repo.toLowerCase()) {
    warnings.push("Coverage source differs from the captured candidate repository or commit. This assessment does not establish this candidate’s reach.");
  }
  if (spec.kind === "repo_sha" && (typeof spec.sha !== "string" || !fullSha(spec.sha) || typeof spec.repo !== "string" || spec.sha.toLowerCase() !== coverage.head.toLowerCase() || spec.repo.toLowerCase() !== coverage.repo.toLowerCase())) {
    sourceMatches = false;
    warnings.push("The declared repo_sha candidate is incomplete or conflicts with the coverage source, even if captured PR metadata agrees.");
  }
  if (coverage.base?.source === "named_baseline" && (!coverage.base.baseline_run || coverage.base.baseline_run !== params?.delta_against)) {
    warnings.push("Coverage’s named baseline does not match this replay’s declared baseline pair.");
  }
  const driven = run.scorecard?.summary.total_correlations;
  if (coverage.never_ran !== counts.not_exercised || coverage.unproven !== counts.unproven) {
    warnings.push("Published aggregate reach counts disagree with the item list. The counts above are derived from the listed items.");
  }
  if (driven != null && driven !== coverage.driven_requests) {
    warnings.push(`Coverage reports ${coverage.driven_requests} driven requests; this scorecard reports ${driven}. The totals disagree.`);
  }
  if (!run.scorecard?.correlation_scope) {
    warnings.push("The scorecard’s admitted request IDs are unavailable; exact request-scope agreement cannot be verified.");
  }
  if (coverage.request_scope) {
    if (new Set(coverage.request_scope).size !== coverage.request_scope.length || coverage.request_scope.length !== coverage.driven_requests) {
      warnings.push("Coverage’s published request IDs do not agree with its driven-request count.");
    }
    const admitted = run.scorecard?.correlation_scope;
    if (admitted && !sameScope(coverage.request_scope, admitted)) {
      warnings.push("Coverage request IDs differ from the scorecard’s admitted scope, even if the totals agree.");
    }
  }
  // Aggregate reach prose duplicates the item-derived counts and can overstate
  // unknown reach. Keep the original notes in provenance, not as primary claims.
  const aggregateReachNote = /^(?:\d+ of \d+ changed item\(s\) never ran on this tape — a pass says nothing about them|\d+ of \d+ changed item\(s\) sit on a path that ran, but their own execution is unproven at span granularity)$/;
  // Empty assessments can also mean an unsupported/truncated comparison.
  const caveats = coverage.caveats.filter((c) => !aggregateReachNote.test(c) && !(coverage.items.length === 0 && /^no Rust source changed/i.test(c)));
  return { counts, warnings, caveats, assessorNotes: coverage.caveats, sourceMatches, provenanceComplete: warnings.length === 0 };
}

export function filterCoverageItems(items: ChangedItem[], filter: CoverageFilter): ChangedItem[] {
  return items.filter((item) => filter === "all" || (filter === "needs_review" ? coverageBucket(item) !== "observed" : coverageBucket(item) === filter));
}
