// The "compared with main" panel may claim agreement only from reads that
// answered. These pin the decisions it makes from its reads.

import { describe, expect, it } from "vitest";
import type { Delta, HttpDiff } from "./api";
import { comparedDiffs, PENDING_POLLS, pollPending, readState, sealLine } from "./deltaPanel";

const delta = (over: Partial<Delta> & Record<string, unknown> = {}) =>
  ({ uncovered: { m_only: [], y_only: [], addresses: 0 }, ...over }) as unknown as Delta;
const diff = (c: string) => ({ correlation_id: c }) as HttpDiff;

describe("readState", () => {
  it("a failed read carries the server's reason, not an empty list", () => {
    const r = readState({ data: undefined, error: new Error("no http_diffs artifact for run-x: never registered") });
    expect(r).toEqual({ state: "failed", reason: "no http_diffs artifact for run-x: never registered" });
  });
  it("a read that has not answered is pending, not empty", () => {
    expect(readState({ data: undefined, error: null }).state).toBe("pending");
  });
  it("an answered empty read is a fact", () => {
    expect(readState({ data: [], error: null })).toEqual({ state: "ok", data: [] });
  });
});

describe("comparedDiffs", () => {
  it("drops requests only one run drove, so they cannot read as agreement", () => {
    const d = delta({ uncovered: { m_only: ["m1"], y_only: ["y1"], addresses: 2 } });
    const kept = comparedDiffs(d, [diff("both"), diff("y1"), diff("m1")]);
    expect(kept.map((x) => x.correlation_id)).toEqual(["both"]);
  });
});

describe("sealLine", () => {
  const side = (seal?: string, correlations = 78) => ({
    members: ["rec-a"],
    correlations,
    member_seals: seal ? [{ recording_id: "rec-a", seal_id: seal }] : [],
  });
  it("names the shared seal", () => {
    expect(sealLine(delta({ tape_check: { y: side("abc"), m: side("abc") } }))).toBe("seal abc, read by both runs");
  });
  it("names both seals when they differ, rather than one recording", () => {
    const line = sealLine(delta({ tape_check: { y: side("abc"), m: side("def") } }));
    expect(line).toContain("abc");
    expect(line).toContain("def");
  });
  it("says a missing seal is missing, and what was compared instead", () => {
    const line = sealLine(delta({ tape_check: { y: side(), m: side("abc") } }));
    expect(line).toContain("not recorded");
    expect(line).toContain("78");
  });
  it("says so when the delta carries no tape check at all", () => {
    expect(sealLine(delta())).toContain("not recorded");
  });
});

describe("pollPending", () => {
  const pending = { unavailable: "report not fetched", unavailable_kind: "pending" as const };
  it("polls a pending delta, then stops", () => {
    expect(pollPending(pending, 0)).toBe(true);
    expect(pollPending(pending, PENDING_POLLS - 1)).toBe(true);
    expect(pollPending(pending, PENDING_POLLS)).toBe(false);
  });
  it("never polls a refusal or a tape mismatch", () => {
    expect(pollPending({ unavailable: "x", unavailable_kind: "refused" }, 0)).toBe(false);
    expect(pollPending({ unavailable: "x", unavailable_kind: "tape_mismatch" }, 0)).toBe(false);
  });
});
