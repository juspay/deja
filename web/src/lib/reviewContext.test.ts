import { describe, expect, it } from "vitest";
import { headFreshness } from "../components/ReviewContext";
import type { ReviewContext } from "./api";

const head = "a".repeat(40);
const context = (over: Partial<ReviewContext> = {}): ReviewContext => ({
  github: { repo: "example/system", pr_number: 42, head_sha: head },
  source_revision: { sha: head, evidence: "declared source agrees" }, source_unavailable: null,
  current_head: { status: "verified", sha: head, checked_at: "2026-11-24T10:00:00Z" }, heads: [], ...over,
});

describe("PR head freshness", () => {
  it("only calls a replay current after a separate live read and source binding", () => {
    const fresh = headFreshness(context());
    expect(fresh.current).toBe(true);
    expect(fresh.detail).toContain("2026-11-24T10:00:00Z");
    expect(fresh.detail).toContain("deployment freshness are not attested");
  });

  it("does not apply an older head's result to a newer head", () => {
    const old = headFreshness(context({ current_head: { status: "verified", sha: "b".repeat(40), checked_at: "now" } }));
    expect(old.current).toBe(false);
    expect(old.label).toBe("Historical PR head");
    expect(old.detail).toContain("does not validate that head");
  });

  it("does not retain a current-head claim after a failed refresh", () => {
    const failed = headFreshness(context(), new Error("source read failed"));
    expect(failed.current).toBe(false);
    expect(failed.label).toBe("Current PR head unavailable");
    expect(failed.detail).toContain("cached metadata cannot establish");
    expect(headFreshness().label).toContain("Verifying");
  });

  it("keeps unavailable, unbound and absent identities unknown instead of trusting captured metadata", () => {
    expect(headFreshness(context({ current_head: null })).current).toBe(false);
    expect(headFreshness(context({ current_head: { status: "unavailable", reason: "403", checked_at: "now" } })).detail).toBe("403");
    const unbound = headFreshness(context({ source_revision: null, source_unavailable: "image and head conflict" }));
    expect(unbound.current).toBe(false);
    expect(unbound.detail).toContain("conflict");
    expect(headFreshness(context({ github: null })).current).toBe(false);
  });
});
