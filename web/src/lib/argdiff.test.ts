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

describe("a change past the truncation point is still visible", () => {
  // The second route to "a real change renders as no change", and the likelier
  // one in a Hyperswitch report: `short()` cuts at 40 characters, so two long
  // values that share a prefix render identically. The type-based quoting above
  // does not help — both sides are the same type, so `mixed` is false.
  const blob = (v: string) =>
    `PaymentAttemptNew { payment_id: "pay_abcdefghijklmnop", status: ${v}, amount: 100 }`;

  test("two long strings differing past char 40", () => {
    const s = summarizeLeaves(diffArgs({ b: blob("Charged") }, { b: blob("Pending") }), 1);
    expect(s.shown).toHaveLength(1);
    expect(s.shown[0].recorded).not.toBe(s.shown[0].candidate);
  });

  test("the differing region itself is shown, not the shared prefix", () => {
    const s = summarizeLeaves(diffArgs({ b: blob("Charged") }, { b: blob("Pending") }), 1);
    expect(s.shown[0].recorded).toContain("Charged");
    expect(s.shown[0].candidate).toContain("Pending");
  });

  test("arrays differing at a late element", () => {
    const rec = Array.from({ length: 14 }, (_, i) => `item-${i}`);
    const cand = [...rec];
    cand[11] = "item-CHANGED";
    const s = summarizeLeaves(diffArgs({ xs: rec }, { xs: cand }), 1);
    expect(s.shown).toHaveLength(1);
    expect(s.shown[0].recorded).not.toBe(s.shown[0].candidate);
  });
});

describe("a header list that starts empty", () => {
  // `isPairList` required length > 0, so an empty list was not recognised as
  // one and the pair-by-name comparison never ran. Adding the FIRST header
  // dumped both whole lists as a single leaf — the thirty-lines-each rendering
  // this module exists to avoid, reached through the one case where the list
  // is shortest.
  test("adding the first header names that header", () => {
    const s = summarizeLeaves(diffArgs({ h: [] }, { h: [["authorization", "Bearer x"]] }), 1);
    expect(s.shown[0].path).toBe("h.authorization");
  });

  test("removing the last header names that header", () => {
    const s = summarizeLeaves(diffArgs({ h: [["prefer", "return=minimal"]] }, { h: [] }), 1);
    expect(s.shown[0].path).toBe("h.prefer");
  });

  test("two empty lists are not a difference", () => {
    expect(diffArgs({ h: [] }, { h: [] })).toHaveLength(0);
  });
});

describe("an array that only changed order", () => {
  // Object KEY reorder already produces no leaves, which is how both views
  // learned to say "order only". An array ELEMENT reorder did not: it reported
  // each moved position as an independent content change, so a reordered
  // `payment_methods_enabled` read as "the candidate sent a different payment
  // method at position 0" — worse than saying nothing, because it names a
  // change that did not happen.
  test("a pure reorder produces no leaves", () => {
    expect(diffArgs({ pm: ["card", "upi"] }, { pm: ["upi", "card"] })).toHaveLength(0);
  });

  test("a longer reorder produces no leaves", () => {
    const a = ["a", "b", "c", "d", "e"];
    expect(diffArgs({ pm: a }, { pm: [...a].reverse() })).toHaveLength(0);
  });

  test("duplicates are compared by multiplicity, not by set", () => {
    // Same elements, same count, different order — a reorder.
    expect(diffArgs({ x: ["a", "a", "b"] }, { x: ["a", "b", "a"] })).toHaveLength(0);
    // Same SET but different multiplicity — a real change, not a reorder.
    expect(diffArgs({ x: ["a", "a", "b"] }, { x: ["a", "b", "b"] })).not.toHaveLength(0);
  });

  test("a reorder alongside a real change reports only the real change", () => {
    const leaves = diffArgs(
      { pm: ["card", "upi"], amount: 1 },
      { pm: ["upi", "card"], amount: 2 },
    );
    expect(leaves).toHaveLength(1);
    expect(leaves[0].path).toBe("amount");
  });

  test("a genuinely changed element is still a content change", () => {
    const leaves = diffArgs({ pm: ["card", "upi"] }, { pm: ["card", "netbanking"] });
    expect(leaves).toHaveLength(1);
    expect(leaves[0].path).toBe("pm[1]");
  });
});
