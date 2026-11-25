import { runParams, type ChangeCoverage, type ChangedItem, type RunRow } from "./api";

export type CoverageBucket = "observed" | "not_exercised" | "unproven";
export type CoverageFilter = CoverageBucket | "needs_review" | "all";

export function coverageBucket(item: ChangedItem, reachApplicable = true): CoverageBucket {
  if (!reachApplicable) return "unproven";
  if (item.reach === "exercised") return "observed";
  return item.reach === "not_exercised" ? "not_exercised" : "unproven";
}

const fullSha = (sha: unknown): sha is string => typeof sha === "string" && /^[a-f0-9]{40}$/i.test(sha);
const nonempty = (value: unknown): value is string => typeof value === "string" && value.trim().length > 0;
const validRepo = (repo: unknown): repo is string => typeof repo === "string" && /^[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+$/.test(repo) && repo.split("/").every((part) => part !== "." && part !== "..");
const validScope = (scope: unknown): scope is string[] => Array.isArray(scope) && scope.length > 0 && scope.every((id) => typeof id === "string" && id.length > 0) && new Set(scope).size === scope.length;
const positiveCount = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value > 0;
const sameScope = (a: string[], b: string[]) => {
  const left = [...new Set(a)].sort(), right = [...new Set(b)].sort();
  return left.length === right.length && left.every((id, i) => id === right[i]);
};

/** A source link identifies a published item location, never a coverage heatmap. */
export function coverageSourceHref(coverage: ChangeCoverage, item: ChangedItem): string | null {
  if (!validRepo(coverage.repo) || !fullSha(coverage.head)) return null;
  const segments = item.path.split("/");
  if (!item.path || segments.some((part) => !part || part === "." || part === "..") || /[\\\u0000-\u001f\u007f]/.test(item.path)) return null;
  const match = /^(\d+)(?:-(\d+))?$/.exec(item.lines);
  if (!match) return null;
  const start = Number(match[1]), end = Number(match[2] ?? match[1]);
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 1 || end < start) return null;
  return `https://github.com/${coverage.repo}/blob/${coverage.head}/${segments.map(encodeURIComponent).join("/")}#L${start}${end !== start ? `-L${end}` : ""}`;
}

export function summarizeChangeCoverage(coverage: ChangeCoverage, run: RunRow) {
  const publishedCounts: Record<CoverageBucket, number> = { observed: 0, not_exercised: 0, unproven: 0 };
  for (const item of coverage.items) publishedCounts[coverageBucket(item)]++;
  const warnings: string[] = [];
  const params = runParams(run);
  const captured = params?.github;
  const spec = params?.candidate_spec ?? run.candidate;
  const sourceHead = captured?.head_sha ?? (spec.kind === "repo_sha" && typeof spec.sha === "string" ? spec.sha : undefined);
  const sourceRepo = captured?.repo ?? (spec.kind === "repo_sha" && typeof spec.repo === "string" ? spec.repo : undefined);
  let sourceMatches = fullSha(sourceHead) && fullSha(coverage.head) && validRepo(sourceRepo) && validRepo(coverage.repo) && sourceHead.toLowerCase() === coverage.head.toLowerCase() && sourceRepo.toLowerCase() === coverage.repo.toLowerCase();
  const base = coverage.base;
  const assessmentSupported = coverage.assessment_version === 3 && typeof coverage.inputs_digest === "string" && /^[a-f0-9]{64}$/i.test(coverage.inputs_digest);
  const immutableBase = !!base && fullSha(base.resolved_sha) && nonempty(base.declared_ref) && nonempty(base.resolved_at) && (base.source === "named_baseline" || base.source === "configured_ref") && (base.resolution === "declared_immutable_commit" || base.resolution === "github_commit_lookup");
  if (!assessmentSupported || !immutableBase || !Array.isArray(coverage.request_scope)) {
    warnings.push("Historical or incomplete assessment: immutable source and request-scope provenance are not fully published. Treat item reach as provisional.");
  }
  if (!fullSha(sourceHead) || !fullSha(coverage.head) || !validRepo(sourceRepo) || !validRepo(coverage.repo)) {
    warnings.push("Coverage source cannot be verified against a complete captured candidate repository and commit.");
  } else if (sourceHead.toLowerCase() !== coverage.head.toLowerCase() || sourceRepo.toLowerCase() !== coverage.repo.toLowerCase()) {
    warnings.push("Coverage source differs from the captured candidate repository or commit. This assessment does not establish this candidate’s reach.");
  }
  if (spec.kind === "repo_sha" && (!fullSha(spec.sha) || !fullSha(coverage.head) || !validRepo(spec.repo) || !validRepo(coverage.repo) || spec.sha.toLowerCase() !== coverage.head.toLowerCase() || spec.repo.toLowerCase() !== coverage.repo.toLowerCase())) {
    sourceMatches = false;
    warnings.push("The declared repo_sha candidate is incomplete or conflicts with the coverage source, even if captured PR metadata agrees.");
  }
  const baseline = params?.delta_against;
  const baselineMatches = baseline !== undefined
    ? nonempty(baseline) && baseline !== run.run_id && base?.source === "named_baseline" && base.baseline_run === baseline
    : base?.source === "configured_ref" && base.baseline_run === undefined;
  if (!baselineMatches) {
    warnings.push("Coverage’s named baseline does not match this replay’s declared baseline pair.");
  }
  const driven = run.scorecard?.summary?.total_correlations;
  if (coverage.never_ran !== publishedCounts.not_exercised || coverage.unproven !== publishedCounts.unproven) {
    warnings.push("Published aggregate reach counts disagree with the item list. The counts above are derived from the listed items.");
  }
  const denominatorMatches = positiveCount(driven) && positiveCount(coverage.driven_requests) && driven === coverage.driven_requests;
  if (!denominatorMatches) {
    warnings.push(`Coverage reports ${coverage.driven_requests} driven requests; this scorecard reports ${driven}. The totals disagree.`);
  }
  const admitted = run.scorecard?.correlation_scope;
  const publishedScope = coverage.request_scope;
  const publishedScopeValid = validScope(publishedScope);
  const admittedScopeValid = validScope(admitted);
  if (!admittedScopeValid) {
    warnings.push("The scorecard’s admitted request IDs are unavailable; exact request-scope agreement cannot be verified.");
  }
  if (!publishedScopeValid || publishedScope.length !== coverage.driven_requests) {
    warnings.push("Coverage’s published request IDs do not agree with its driven-request count.");
  }
  if (admittedScopeValid && admitted.length !== driven) {
    warnings.push("The scorecard’s admitted request IDs do not agree with its driven-request count.");
  }
  const scopesMatch = publishedScopeValid && admittedScopeValid && sameScope(publishedScope, admitted);
  if (publishedScopeValid && admittedScopeValid && !scopesMatch) {
    warnings.push("Coverage request IDs differ from the scorecard’s admitted scope, even if the totals agree.");
  }
  // Advisory aggregate discrepancies and assessor caveats do not invalidate an
  // otherwise exact source/base/request binding. Invalid bindings do: preserve
  // published item reach for inspection, but do not apply it to this replay.
  const reachApplicable = assessmentSupported && immutableBase && sourceMatches && baselineMatches && denominatorMatches && scopesMatch && publishedScopeValid && publishedScope.length === coverage.driven_requests && admittedScopeValid && admitted.length === driven;
  const counts = reachApplicable ? publishedCounts : { observed: 0, not_exercised: 0, unproven: coverage.items.length };
  // Aggregate reach prose duplicates the item-derived counts and can overstate
  // unknown reach. Keep the original notes in provenance, not as primary claims.
  const aggregateReachNote = /^(?:\d+ of \d+ changed item\(s\) never ran on this tape — a pass says nothing about them|\d+ of \d+ changed item\(s\) sit on a path that ran, but their own execution is unproven at span granularity)$/;
  // Empty assessments can also mean an unsupported/truncated comparison.
  const caveats = coverage.caveats.filter((c) => !aggregateReachNote.test(c) && !(coverage.items.length === 0 && /^no Rust source changed/i.test(c)));
  return { counts, warnings, caveats, assessorNotes: coverage.caveats, sourceMatches, reachApplicable, provenanceComplete: reachApplicable };
}

export function filterCoverageItems(items: ChangedItem[], filter: CoverageFilter, reachApplicable = true): ChangedItem[] {
  return items.filter((item) => filter === "all" || (filter === "needs_review" ? coverageBucket(item, reachApplicable) !== "observed" : coverageBucket(item, reachApplicable) === filter));
}
