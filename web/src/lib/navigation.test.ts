import { describe, expect, it } from "vitest";
import { readReviewNavigation, reviewHref } from "./navigation";

const decode = (href: string) => readReviewNavigation(new URL(href, "https://example.test").search);

describe("review navigation", () => {
  it("preserves a filtered page and exact selections across baseline and acknowledgment roundtrips", () => {
    const runs = "/runs?system=other&q=PR+42&cursor=opaque&result=DIVERGED#candidate";
    const candidate = "/r/candidate?case=request&node=912&side=observed";
    const baseline = reviewHref("/r/baseline?case=request&node=17&side=recorded", candidate, { runs, trail: [] });
    const acknowledgment = reviewHref("/r/baseline/acknowledge", baseline, decode(baseline));
    expect(decode(acknowledgment)).toEqual({ runs, trail: [candidate, "/r/baseline?case=request&node=17&side=recorded"] });
    const backToBaseline = reviewHref(decode(acknowledgment).trail.at(-1)!, acknowledgment, decode(acknowledgment));
    expect(decode(backToBaseline)).toEqual({ runs, trail: [candidate] });
    const backToCandidate = reviewHref(candidate, backToBaseline, decode(backToBaseline));
    expect(decode(backToCandidate)).toEqual({ runs, trail: [] });
    expect(backToCandidate).toContain("node=912");
  });

  it("does not grow nested context or accumulate cycles", () => {
    let href = "/r/one";
    for (let i = 0; i < 50; i++) href = reviewHref(i % 2 ? "/r/one" : "/r/two", href, decode(href));
    expect(decode(href).trail.length).toBeLessThanOrEqual(1);
    expect(href.length).toBeLessThan(250);
  });

  it("rejects external, malformed, traversal and oversized return contexts", () => {
    for (const runs of ["//evil.test/runs", "https://evil.test/runs", "javascript:alert(1)", "data:text/html,<script>alert(1)</script>", "/%2f%2fevil.test/runs", "/runs/../r/other", "/runs\\evil", "/runs?" + "x".repeat(12000)]) {
      expect(readReviewNavigation(`?nav=${encodeURIComponent(JSON.stringify({ runs, trail: [] }))}`).runs).toBe("/runs");
    }
    expect(readReviewNavigation("?nav=not-json")).toEqual({ runs: "/runs", trail: [] });
    expect(readReviewNavigation("", { runsUrl: "/runs?system=example" }).runs).toBe("/runs?system=example");
  });
});
