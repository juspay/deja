import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, type ChangeCoverage, type ChangeCoverageResponse, type ChangeReach, type RunRow } from "../lib/api";
import { coverageBucket, coverageSourceHref, filterCoverageItems, summarizeChangeCoverage, type CoverageFilter } from "../lib/changeCoverage";
import { useReviewNavigation } from "../lib/navigation";
import "./ChangeCoverage.css";

const reachLabel: Record<ChangeReach, string> = {
  exercised: "Execution observed", not_exercised: "Not exercised", flow_ran: "Flow ran; item unproven",
  flow_ran_weak: "Weak flow evidence; item unproven", module_ran: "Module ran; item unproven", unknown: "Execution unknown",
};

function isAssessment(value: ChangeCoverageResponse | undefined): value is ChangeCoverage {
  return value !== undefined && !("unavailable" in value);
}

export function ChangeCoveragePanel({ run }: { run: RunRow }) {
  const navigation = useReviewNavigation();
  const [selectedFilter, setFilter] = useState<CoverageFilter | null>(null);
  const query = useQuery({ queryKey: ["change-coverage", run.run_id], queryFn: () => api.changeCoverage(run.run_id), staleTime: Infinity });
  const coverage = query.data;
  const assessment = isAssessment(coverage) ? coverage : null;
  const summary = assessment ? summarizeChangeCoverage(assessment, run) : null;
  const filter = selectedFilter ?? (summary && summary.counts.not_exercised + summary.counts.unproven === 0 ? "all" : "needs_review");
  const items = assessment ? filterCoverageItems(assessment.items, filter) : [];
  return <section className="change-coverage" aria-label="Changed-code execution" id="changed-code-execution">
    <h3>Did this replay exercise the changed code?</h3>
    <p className="hint">This is reach evidence for enclosing Rust items, not changed-line or branch coverage. A clean behavior comparison alone does not establish that the changed behavior was tested.</p>
    {query.isLoading ? <p className="hint">Assessing changed-code execution…</p>
      : query.error ? <p className="err">Changed-code execution is unproven: the assessment could not be read. {String(query.error)}</p>
      : !coverage ? <p className="hint">Changed-code execution is unproven: no assessment was published.</p>
      : "unavailable" in coverage ? <p className="hint">Changed-code execution not assessed — {coverage.unavailable}</p>
      : assessment && summary && <>
        <div className="change-coverage-counts" aria-label="Changed item reach counts">
          <div><strong>{summary.counts.observed}</strong><span>Execution observed</span></div>
          <div><strong>{summary.counts.not_exercised}</strong><span>Not exercised</span></div>
          <div><strong>{summary.counts.unproven}</strong><span>Execution unproven</span></div>
        </div>
        {!assessment.items.length && <p className="change-coverage-warning">No supported changed-code items were assessed. This does not establish coverage of the PR.</p>}
        <p className="hint">{assessment.items.length} item{assessment.items.length === 1 ? "" : "s"} in the source comparison from common ancestor <code>{assessment.merge_base.slice(0, 10)}</code> to candidate <code>{assessment.head.slice(0, 10)}</code> · {assessment.driven_requests} requests driven. This comparison may include changes beyond the PR.</p>
        <p className="hint">Execution observed means a span or boundary call site reached the enclosing item. Flow or module activity alone leaves that item’s execution unproven.</p>
        {summary.warnings.map((warning) => <p className="change-coverage-warning" key={warning}>{warning}</p>)}
        {summary.caveats.map((caveat, i) => <p className="change-coverage-warning" key={i}>{caveat}</p>)}
        {assessment.items.length > 0 && <details className="change-coverage-items" open={summary.counts.not_exercised + summary.counts.unproven > 0}>
          <summary>Inspect changed items and execution evidence</summary>
          <label>Show <select value={filter} onChange={(event) => setFilter(event.target.value as CoverageFilter)}>
            <option value="needs_review">Not exercised or unproven</option><option value="all">All changed items</option>
            <option value="observed">Execution observed</option><option value="not_exercised">Not exercised</option><option value="unproven">Execution unproven</option>
          </select></label>
          <div className="change-coverage-table"><table className="fieldtbl"><thead><tr><th>Execution evidence</th><th>Changed item / function</th><th>Source location</th><th>Why</th></tr></thead><tbody>
            {items.map((item, i) => { const href = summary.sourceMatches ? coverageSourceHref(assessment, item) : null; return <tr key={`${item.path}:${item.lines}:${item.item}:${i}`} className={coverageBucket(item) !== "observed" ? "reach-unproven" : undefined}>
              <td>{reachLabel[item.reach] ?? "Execution unproven"}</td><td><code>{item.item}</code>{(item.connector || item.flow) && <div className="hint">{[item.connector, item.flow].filter(Boolean).join(" · ")}</div>}</td>
              <td>{href ? <a href={href} target="_blank" rel="noreferrer"><code>{item.path}:{item.lines}</code></a> : <code>{item.path}:{item.lines}</code>}</td><td>{item.why}</td>
            </tr>; })}
            {!items.length && <tr><td colSpan={4}>No items in this filter. Choose “All changed items” to inspect the assessment.</td></tr>}
          </tbody></table></div>
        </details>}
        <details className="change-coverage-provenance"><summary>Source and request-scope provenance</summary>
          <p className="hint">Repository <code>{assessment.repo}</code>; source comparison head <code>{assessment.head}</code>. Source identity is declared metadata; candidate image contents are not attested.</p>
          {assessment.base ? <p className="hint">Requested base <code>{assessment.base.declared_ref}</code> resolved to <code>{assessment.base.resolved_sha}</code> at {assessment.base.resolved_at}. {assessment.base.source === "named_baseline" && assessment.base.baseline_run ? <>Pinned to the <Link to={navigation.href(`/r/${assessment.base.baseline_run}`)}>named baseline run</Link>.</> : <>Configured comparison ref resolved at assessment time.</>} The common ancestor may differ from this requested base.</p> : <p className="hint">Immutable base provenance was not published.</p>}
          <p className="hint">Assessment version {assessment.assessment_version ?? "unpublished"}; published scope {assessment.request_scope ? `${assessment.request_scope.length} request IDs` : "unavailable"}.</p>
          {assessment.inputs_digest && <p className="hint">Evidence input digest <code>{assessment.inputs_digest}</code></p>}
          {summary.assessorNotes.length > 0 && <div className="change-coverage-assessor-notes">
            <h4>Published assessor notes</h4>
            <p className="hint">Original notes may use broader or legacy reach heuristics. Review the individual item evidence above.</p>
            {summary.assessorNotes.map((note, i) => <p className="hint" key={i}>{note}</p>)}
          </div>}
        </details>
      </>}
  </section>;
}
