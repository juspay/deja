// `summarizeLeaves` renders the one line a reader sees before opening a row.
// A leaf whose two sides differ only in TYPE used to render as though nothing
// had changed: strings were printed bare and everything else JSON-stringified,
// so the number 5 and the string "5" both came out as `5` and the row read
// `amount: 5 -> 5`.
//
// That is the worst shape a diff viewer can have. The detection was correct —
// `diffArgs` found the leaf and recorded both sides — and the rendering threw
// the difference away, so a real divergence was reported as agreement.

import { describe, expect, test } from "vitest";
import { diffArgs, summarizeLeaves } from "./argdiff";

/** The single line a row shows for the first changed leaf. */
function line(recorded: unknown, candidate: unknown): string {
  const s = summarizeLeaves(diffArgs(recorded, candidate), 1);
  const l = s.shown[0];
  return l ? `${l.path}: ${l.recorded} -> ${l.candidate}` : "(nothing shown)";
}

describe("a type change is visible in the summary line", () => {
  // Each case is a leaf that genuinely changed. The assertion is that the two
  // rendered sides are NOT equal — the property that failed before.
  const CASES: Array<{ name: string; rec: unknown; cand: unknown }> = [
    { name: "number vs string", rec: { amount: 5 }, cand: { amount: "5" } },
    { name: "boolean vs string", rec: { flag: true }, cand: { flag: "true" } },
    { name: "null vs string", rec: { x: null }, cand: { x: "null" } },
    { name: "number vs string, zero", rec: { n: 0 }, cand: { n: "0" } },
  ];

  for (const c of CASES) {
    test(c.name, () => {
      const s = summarizeLeaves(diffArgs(c.rec, c.cand), 1);
      expect(s.shown).toHaveLength(1);
      const { recorded, candidate } = s.shown[0];
      // The point of the fix: the reader can tell the two apart.
      expect(recorded).not.toBe(candidate);
    });
  }

  test("the rendered line names both types", () => {
    expect(line({ amount: 5 }, { amount: "5" })).toBe('amount: 5 -> "5"');
  });
});

describe("same-type leaves still render bare", () => {
  // The quoting is conditional on purpose: a header value should read as
  // itself, not as a quoted blob, which is the whole reason strings were bare.
  test("string to string carries no quotes", () => {
    expect(line({ h: "return=representation" }, { h: "return=minimal" })).toBe(
      "h: return=representation -> return=minimal",
    );
  });

  test("number to number carries no quotes", () => {
    expect(line({ n: 1 }, { n: 2 })).toBe("n: 1 -> 2");
  });
});

describe("an absent key is still distinguishable from a present one", () => {
  // `undefined` has its own typeof, so the mixed-type path also covers this.
  // A key the candidate added must not render as though it held the empty
  // string.
  test("absent vs present", () => {
    expect(line({ a: 1 }, { a: 1, b: 2 })).toBe("b: ∅ -> 2");
  });

  test("absent vs empty string", () => {
    const s = summarizeLeaves(diffArgs({ a: 1 }, { a: 1, b: "" }), 1);
    expect(s.shown[0].recorded).not.toBe(s.shown[0].candidate);
  });
});
