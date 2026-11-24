import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { describe, expect, it, vi } from "vitest";
import type { ChangeCoverage, ChangedItem, ChangeCoverageResponse, RunRow } from "./api";
import { ChangeCoveragePanel } from "../components/ChangeCoverage";
import { coverageSourceHref, filterCoverageItems, summarizeChangeCoverage } from "./changeCoverage";

vi.mock("@tanstack/react-query", () => ({ useQuery: vi.fn() }));
const sha = "a".repeat(40);
const item = (reach: ChangedItem["reach"] = "exercised"): ChangedItem => ({ path: "crates/router/src/webhooks.rs", item: "incoming_webhook", lines: "10-20", reach, why: "span inside enclosing function" });
const coverage = (over: Partial<ChangeCoverage> = {}): ChangeCoverage => ({ assessment_version: 3, request_scope: ["request-a", "request-b"], inputs_digest: "b".repeat(64), base: { source: "named_baseline", baseline_run: "baseline", declared_ref: "main", resolved_sha: "c".repeat(40), resolved_at: "now", resolution: "github_commit_lookup" }, system: "hyperswitch", repo: "example/system", base_ref: "main", merge_base: "c".repeat(40), head: sha, driven_requests: 2, items: [item()], never_ran: 0, unproven: 0, caveats: [], ...over });
const run = { run_id: "candidate", candidate: { kind: "prebuilt_image", image: "example" }, params: { candidate_spec: { kind: "prebuilt_image", image: "example" }, github: { repo: "example/system", pr_number: 42, head_sha: sha }, delta_against: "baseline" }, scorecard: { summary: { total_correlations: 2 }, correlation_scope: ["request-b", "request-a"] } } as RunRow;
const render = (data?: ChangeCoverageResponse, extra = {}) => {
  vi.mocked(useQuery).mockReturnValue({ data, isLoading: false, ...extra } as never);
  return renderToStaticMarkup(<MemoryRouter><ChangeCoveragePanel run={run} /></MemoryRouter>);
};

describe("changed-code reach summary", () => {
  it("derives disjoint counts from items rather than trusting legacy aggregate totals", () => {
    const assessment = coverage({ items: [item("exercised"), item("not_exercised"), item("flow_ran"), item("flow_ran_weak"), item("module_ran"), item("unknown")], never_ran: 100, unproven: 100 });
    expect(summarizeChangeCoverage(assessment, run).counts).toEqual({ observed: 1, not_exercised: 1, unproven: 4 });
    expect(filterCoverageItems(assessment.items, "needs_review")).toHaveLength(5);
    expect(filterCoverageItems(assessment.items, "unproven")).toHaveLength(4);
    expect(filterCoverageItems(assessment.items, "all")).toHaveLength(6);
  });
  it("accepts exact immutable source and equal admitted request sets regardless of ordering", () => {
    expect(summarizeChangeCoverage(coverage(), run).warnings).toEqual([]);
    expect(summarizeChangeCoverage(coverage(), run).provenanceComplete).toBe(true);
  });
  it("does not let matching PR metadata override conflicting or incomplete repo_sha candidate identity", () => {
    for (const spec of [{ kind: "repo_sha", repo: "example/system", sha: "d".repeat(40) }, { kind: "repo_sha", repo: "different/system", sha }, { kind: "repo_sha", repo: "example/system", sha: sha.slice(0, 10) }]) {
      const conflictingRun = { ...run, params: { ...run.params, candidate_spec: spec } };
      const summary = summarizeChangeCoverage(coverage(), conflictingRun);
      expect(summary.sourceMatches).toBe(false);
      expect(summary.warnings.join(" ")).toContain("declared repo_sha candidate");
    }
    expect(summarizeChangeCoverage(coverage(), { ...run, params: { ...run.params, candidate_spec: { kind: "repo_sha", repo: "example/system", sha } } }).sourceMatches).toBe(true);
  });
  it("does not confuse equal totals with an identical admitted request scope", () => {
    const summary = summarizeChangeCoverage(coverage({ request_scope: ["request-a", "outside-request"] }), run);
    expect(summary.warnings.join(" ")).toContain("differ from the scorecard’s admitted scope");
    expect(summary.provenanceComplete).toBe(false);
    expect(summarizeChangeCoverage(coverage(), { ...run, scorecard: { ...run.scorecard!, correlation_scope: undefined } }).warnings.join(" ")).toContain("exact request-scope agreement cannot be verified");
  });
  it("guards legacy inputs, mismatched source, baseline and request counts independently", () => {
    const legacy = summarizeChangeCoverage(coverage({ assessment_version: undefined, base: undefined, inputs_digest: undefined, request_scope: undefined, head: sha.slice(0, 10), driven_requests: 18 }), run);
    expect(legacy.warnings.join(" ")).toContain("Historical or incomplete");
    expect(legacy.warnings.join(" ")).toContain("complete captured");
    expect(legacy.warnings.join(" ")).toContain("18 driven requests; this scorecard reports 2");
    expect(summarizeChangeCoverage(coverage({ repo: "different/system" }), run).warnings.join(" ")).toContain("differs from the captured candidate");
    expect(summarizeChangeCoverage(coverage({ head: "d".repeat(40) }), run).warnings.join(" ")).toContain("differs from the captured candidate");
    expect(summarizeChangeCoverage(coverage({ base: { ...coverage().base!, baseline_run: "other" } }), run).warnings.join(" ")).toContain("declared baseline pair");
    expect(summarizeChangeCoverage(coverage({ request_scope: ["request-a", "request-a"] }), run).warnings.join(" ")).toContain("do not agree with its driven-request count");
  });
  it("does not promote empty-item Rust caveats into a claim that the PR has no source change", () => {
    const empty = coverage({ items: [], caveats: ["no Rust source changed between base and head — nothing to reach", "diff may be truncated"] });
    expect(summarizeChangeCoverage(empty, run).caveats).toEqual(["diff may be truncated"]);
    const html = render(empty);
    expect(html).toContain("No supported changed-code items were assessed");
    expect(html.split('<details class="change-coverage-provenance"')[0]).not.toContain("no Rust source changed");
    expect(html).toContain("Published assessor notes");
    expect(html).toContain(empty.caveats[0]);
    expect(html).not.toContain("every changed item");
  });
  it("keeps unknown item reach distinct from generated path-ran aggregate notes", () => {
    const pathNote = "1 of 2 changed item(s) sit on a path that ran, but their own execution is unproven at span granularity";
    const neverNote = "1 of 2 changed item(s) never ran on this tape — a pass says nothing about them";
    const substantive = "source diff may be truncated; additional changed items may be absent";
    const assessment = coverage({ items: [item("unknown"), item("not_exercised")], unproven: 1, never_ran: 1, caveats: [pathNote, neverNote, substantive] });
    const summary = summarizeChangeCoverage(assessment, run);
    expect(summary.caveats).toEqual([substantive]);
    expect(summary.assessorNotes).toEqual([pathNote, neverNote, substantive]);
    const html = render(assessment);
    const primary = html.split('<details class="change-coverage-provenance"')[0];
    expect(primary).not.toContain("sit on a path that ran");
    expect(primary).not.toContain("never ran on this tape");
    expect(primary).toContain("Execution unknown");
    expect(primary).toContain(substantive);
    expect(html).toContain(pathNote);
    expect(html).toContain(neverNote);
  });
});

describe("immutable changed-item source links", () => {
  it("links the published item span on the exact commit", () => {
    expect(coverageSourceHref(coverage(), item())).toBe(`https://github.com/example/system/blob/${sha}/crates/router/src/webhooks.rs#L10-L20`);
    expect(coverageSourceHref(coverage(), { ...item(), lines: "12" })).toContain("#L12");
  });
  it("rejects mutable or malformed source metadata", () => {
    for (const bad of ["main", "a".repeat(10)]) expect(coverageSourceHref(coverage({ head: bad }), item())).toBeNull();
    for (const bad of ["../..", "https://github.com/example/system", "example/system/more"]) expect(coverageSourceHref(coverage({ repo: bad }), item())).toBeNull();
    for (const path of ["../secret", "/absolute", "crates\\router.rs", "crates/./router.rs", "crates//router.rs"]) expect(coverageSourceHref(coverage(), { ...item(), path })).toBeNull();
    for (const lines of ["0-2", "2-1", "1#injected", "9007199254740992"]) expect(coverageSourceHref(coverage(), { ...item(), lines })).toBeNull();
  });
});

describe("changed-code panel", () => {
  it("puts line/module evidence in a visible item table and preserves source provenance", () => {
    const html = render(coverage({ items: [item("module_ran")] }));
    expect(html).toContain("Did this replay exercise the changed code?");
    expect(html).toContain("not changed-line or branch coverage");
    expect(html).toContain("may include changes beyond the PR");
    expect(html).toContain("<details class=\"change-coverage-items\" open=\"\"");
    expect(html).toContain("Module ran; item unproven");
    expect(html).toContain("incoming_webhook");
    expect(html).toContain("webhooks.rs:10-20");
    expect(html).toContain("named baseline run");
    expect(html).toContain("Evidence input digest");
    expect(vi.mocked(useQuery).mock.calls.at(-1)![0].queryKey).toEqual(["change-coverage", "candidate"]);
  });
  it("keeps absent, loading, unavailable and failed reads visibly unproven", () => {
    expect(render()).toContain("no assessment was published");
    expect(render(undefined, { isLoading: true })).toContain("Assessing changed-code execution");
    expect(render({ unavailable: "source host unavailable" })).toContain("not assessed — source host unavailable");
    expect(render(coverage(), { error: new Error("read failed") })).toContain("assessment could not be read");
    expect(render(coverage(), { error: new Error("read failed") })).not.toContain("change-coverage-counts");
  });
  it("escapes source-host strings and never presents observed execution as complete line coverage", () => {
    const html = render(coverage({ items: [{ ...item("unknown"), item: "<script>alert(1)</script>" }] }));
    expect(html).not.toContain("<script>");
    expect(html).toContain("&lt;script&gt;");
    expect(render(coverage())).not.toContain("every changed item exercised");
  });
});
