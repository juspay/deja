import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useQuery } from "@tanstack/react-query";
import { api, type RunSummaryRow } from "../lib/api";
import RunsPage from "./RunsPage";
import { RunListResult } from "../components/Result";

vi.mock("@tanstack/react-query", () => ({ useQuery: vi.fn(), keepPreviousData: vi.fn() }));
vi.mock("../lib/systems", () => ({ useSystems: () => ({ all: [{ name: "example-system" }] }) }));
vi.mock("../components/KillRun", () => ({ KillRun: () => null }));

const row: RunSummaryRow = {
  run_id: "older-candidate", mode: "replay", recording_id: "tape",
  candidate: { kind: "repo_sha", repo: "example/repo", sha: "abc123" },
  candidate_sha256: null, params: { candidate_spec: { kind: "repo_sha", repo: "example/repo", sha: "abc123" }, delta_against: "exact-baseline" },
  state: "completed", verdict: "fail", delta_verdict: "pass",
  scorecard_digest: { pass: false, inconclusive: false, reason: "shared drift", total_correlations: 100, matched_correlations: 93 },
  failure: null, expectation: null, created_by: "test", created_at: "2026-11-24T08:00:00Z", started_at: null, finished_at: null,
};

function render(path = "/runs?system=example-system&q=needle&cursor=position&result=DIVERGED") {
  return renderToStaticMarkup(<MemoryRouter initialEntries={[path]}><RunsPage /></MemoryRouter>);
}

afterEach(() => vi.unstubAllGlobals());

describe("run history navigation", () => {
  it("uses the server page, exposes partial-history counts, and names the baseline separately", async () => {
    vi.mocked(useQuery).mockReturnValue({ data: { rows: [row], next_cursor: "next-position" }, isLoading: false, isPlaceholderData: false } as never);
    const html = render();
    // The search was applied by the server; this row need not contain the query.
    expect(html).toContain("older-candidate");
    expect(html).toContain("1 of 1");
    expect(html).toContain("runs on this page");
    expect(html).toContain("PR: no new blocking diff");
    expect(html).toContain("Recording: differs");
    expect(html.indexOf("PR: no new blocking diff")).toBeLessThan(html.indexOf("Recording: differs"));
    expect(html).not.toContain(">diverged</span>");
    expect(html).toContain("exact-baseline");
    expect(html).toContain("shared drift");
    expect(html).not.toContain("main: pass");
    expect(html).not.toContain("attempt 1");
    expect(html).toContain("Newest matching runs");
    expect(html).toContain("Older runs");
    const options = vi.mocked(useQuery).mock.calls.at(-1)![0];
    expect(options.queryKey).toEqual(["runs", "page", { system: "example-system", q: "needle", cursor: "position", limit: 50 }]);
  });

  it("keeps search controls mounted but hides stale rows while the next scope loads", () => {
    vi.mocked(useQuery).mockReturnValue({ data: { rows: [row], next_cursor: "next-position" }, isLoading: false, isPlaceholderData: true } as never);
    const html = render();
    expect(html).toContain('aria-label="search run history"');
    expect(html).toContain("loading matching history");
    expect(html).not.toContain("older-candidate");
    expect(html).not.toContain("end of matching history");
    expect(html).toContain('disabled=""');
  });

  it("encodes filters and opaque cursor without changing their meaning", async () => {
    const fetch = vi.fn().mockResolvedValue(Response.json({ rows: [], next_cursor: null }));
    vi.stubGlobal("fetch", fetch);
    const cursor = JSON.stringify({ created_at: "2026-11-24T08:13:00.123456Z", run_id: "run+b" });
    await api.runsPage({ system: "example-system", q: "PR #42 & 100%", cursor, limit: 50 });
    const url = new URL(fetch.mock.calls[0][0], "https://example.test");
    expect(url.pathname).toBe("/api/v1/runs/page");
    expect(url.searchParams.get("system")).toBe("example-system");
    expect(url.searchParams.get("q")).toBe("PR #42 & 100%");
    expect(url.searchParams.get("cursor")).toBe(cursor);
  });
});


describe("separate PR impact and recording fidelity", () => {
  const chip = (over: Partial<RunSummaryRow> = {}) => renderToStaticMarkup(<RunListResult run={{ ...row, ...over }} />);

  it.each([
    ["pass", "no new blocking diff", "good"], ["acknowledged", "changes acknowledged", "acknowledged"], ["fail", "differences to review", "bad"],
    ["pending", "pending", "warn"], ["refused", "not comparable", "warn"], ["unknown", "unavailable", "warn"], [null, "unavailable", "warn"],
  ] as const)("names a %s baseline result separately from recording drift", (delta_verdict, label, tone) => {
    const html = chip({ delta_verdict });
    expect(html).toContain(`PR: ${label}`);
    expect(html).toContain("Recording: differs");
    expect(html).not.toContain(">diverged</span>");
    expect(html).toContain(`tone-${tone}`);
    if (delta_verdict !== "pass") expect(html).not.toContain("tone-good");
  });

  it("does not turn a failed, unscored, zero-request or inconclusive run green from a stale delta", () => {
    for (const over of [
      { state: "failed", failure: { message: "candidate startup failed" } },
      { state: "running" }, { scorecard_digest: null },
      { scorecard_digest: { ...row.scorecard_digest!, pass: true, total_correlations: 0 } },
      { scorecard_digest: { ...row.scorecard_digest!, inconclusive: true } },
    ]) {
      const html = chip(over);
      expect(html).not.toContain("tone-good");
      expect(html).not.toContain("no new blocking diff");
    }
  });

  it("does not assign a PR outcome to reference or unpaired runs", () => {
    const baseline = chip({ params: { ...row.params, purpose: "baseline", delta_against: undefined } });
    expect(baseline).toContain("baseline reference");
    expect(baseline).toContain("Recording: differs");
    expect(baseline).not.toContain("PR:");
    const unpaired = chip({ params: { ...row.params, delta_against: undefined } });
    expect(unpaired).toContain("recording differs");
    expect(unpaired).not.toContain("no new blocking diff");
  });
});
