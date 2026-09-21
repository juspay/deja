// The precedence ladder in `resultOf` is the only thing standing between a run
// that produced no evidence and a green chip on the dashboard. Until this file
// it had no test of any kind: `web/` had no runner, and `just verify` is Rust.
//
// Two of its rungs are unreachable from production data. A sample of 200 live
// runs on 2026-09-21 was 94 pass / 65 fail / 41 failed-with-no-scorecard and
// contained no inconclusive run at all, so rung 5 (`verdict.inconclusive`) and
// rung 6 (the `total_correlations === 0` guard that forbids green) had never
// been exercised by anything, in any environment. Waiting for production to
// produce one is not a plan; this file is the alternative.
//
// EVERY RUNG IS CHECKED IN BOTH ROW SHAPES. `digestOf` branches on
// `"scorecard_digest" in run` to serve the list's `RunSummaryRow` and the report
// page's full `RunRow`, and the whole point of the digest change is that the two
// reach the SAME verdict from the same underlying facts. Nothing else states
// that, so `agree` below states it directly.

import { describe, expect, test } from "vitest";
import { resultOf, type ResultState } from "./result";
import type { RunRow, RunSummaryRow, Scorecard, ScorecardDigest } from "./api";

/**
 * What a run is, reduced to what the ladder actually consults.
 *
 * `scored: null` means NO SCORECARD — the thing rung 4 tests. It is distinct
 * from a scorecard whose fields are null, which is rung 5/6 territory, and
 * keeping the two apart is the distinction the SQL `CASE` also has to make.
 */
type Facts = {
  state: string;
  mode: "record" | "replay";
  scored: ScorecardDigest | null;
  failure?: string;
  verdict?: RunRow["verdict"];
  stage?: string;
};

const BASE = {
  run_id: "run-under-test",
  recording_id: "rec-under-test",
  candidate: { kind: "prebuilt_image", image: "img" },
  candidate_sha256: null,
  params: {},
  expectation: null,
  created_by: "result.test.ts",
  created_at: "2026-09-21T00:00:00Z",
  started_at: null,
  finished_at: null,
} as const;

// The two builders place the given values VERBATIM into the two wire shapes and
// normalise nothing. That is deliberate: a fixture has to be able to express a
// scorecard that says `pass: true` over zero compared correlations, because that
// is exactly the state rung 6 exists to refuse. A helper that kept the invariant
// would make rung 6 untestable and its test a tautology.

function asSummaryRow(f: Facts): RunSummaryRow {
  return {
    ...BASE,
    state: f.state,
    mode: f.mode,
    verdict: f.verdict ?? null,
    failure: f.failure === undefined ? null : { message: f.failure },
    scorecard_digest: f.scored,
    ...(f.stage ? { live: { stage: f.stage } } : {}),
  } as unknown as RunSummaryRow;
}

function asFullRow(f: Facts): RunRow {
  // The full row carries a whole Scorecard; the digest is its projection. Built
  // out here rather than shared with the summary builder so the two shapes are
  // genuinely independent constructions of the same facts — if they shared a
  // code path, `agree` would be testing that path rather than the two shapes.
  const scorecard =
    f.scored === null
      ? null
      : ({
          verdict: {
            pass: f.scored.pass,
            inconclusive: f.scored.inconclusive,
            reason: f.scored.reason,
          },
          summary: {
            total_correlations: f.scored.total_correlations,
            matched_correlations: f.scored.matched_correlations,
          },
        } as unknown as Scorecard);
  return {
    ...BASE,
    state: f.state,
    mode: f.mode,
    verdict: f.verdict ?? null,
    failure: f.failure === undefined ? null : { message: f.failure },
    scorecard,
    ...(f.stage ? { live: { stage: f.stage } } : {}),
  } as unknown as RunRow;
}

/** A scorecard digest, stated in full so no field is silently defaulted. */
const digest = (d: Partial<ScorecardDigest>): ScorecardDigest => ({
  pass: null,
  inconclusive: null,
  reason: null,
  total_correlations: null,
  matched_correlations: null,
  ...d,
});

type Rung = {
  rung: number;
  name: string;
  facts: Facts;
  expected: ResultState;
  /** Rungs that downgrade must SAY why. `null` asserts the absence of a guard. */
  guard: RegExp | null;
};

const LADDER: Rung[] = [
  {
    rung: 1,
    name: "not terminal is RUNNING, whatever a scorecard claims",
    // A scorecard that says "pass" must not promote a run that is still moving.
    facts: {
      state: "running",
      mode: "replay",
      stage: "driving recorded requests",
      scored: digest({ pass: true, inconclusive: false, total_correlations: 9, matched_correlations: 9 }),
    },
    expected: "RUNNING",
    guard: null,
  },
  {
    rung: 2,
    name: "failed outranks a PASSING scorecard",
    // The first rule in the module docs. If this regresses, a run that died
    // holding a synthesised scorecard renders green.
    facts: {
      state: "failed",
      mode: "replay",
      failure: "session not found under s3://",
      scored: digest({ pass: true, inconclusive: false, total_correlations: 42, matched_correlations: 42 }),
    },
    expected: "NO_VERDICT",
    guard: null,
  },
  {
    rung: 3,
    name: "a record run is never scored",
    facts: {
      state: "completed",
      mode: "record",
      scored: digest({ pass: true, inconclusive: false, total_correlations: 7, matched_correlations: 7 }),
    },
    expected: "NO_VERDICT",
    guard: null,
  },
  {
    rung: 4,
    name: "terminal with no scorecard at all",
    facts: { state: "completed", mode: "replay", verdict: "pass", scored: null },
    expected: "NO_VERDICT",
    guard: /no scorecard artifact/i,
  },
  {
    rung: 5,
    name: "the scorer called it inconclusive",
    // Unreachable from live data; this is the only thing that exercises it.
    facts: {
      state: "completed",
      mode: "replay",
      scored: digest({
        pass: false,
        inconclusive: true,
        reason: "no artifacts ingested for this run yet",
        total_correlations: 3,
        matched_correlations: 0,
      }),
    },
    expected: "INCONCLUSIVE",
    guard: null,
  },
  {
    rung: 6,
    name: "THE GUARD: pass over zero correlations is vacuous, not green",
    // The sharp case. `pass: true` with nothing compared is precisely the state
    // that must NOT reach REPRODUCED, and the only fixture here that would
    // render green if the guard were removed.
    facts: {
      state: "completed",
      mode: "replay",
      scored: digest({ pass: true, inconclusive: false, reason: "nothing to compare", total_correlations: 0, matched_correlations: 0 }),
    },
    expected: "INCONCLUSIVE",
    guard: /compared 0 correlations/i,
  },
  {
    rung: 7,
    name: "pass over real correlations is the only green",
    facts: {
      state: "completed",
      mode: "replay",
      scored: digest({ pass: true, inconclusive: false, reason: "all matched", total_correlations: 100, matched_correlations: 100 }),
    },
    expected: "REPRODUCED",
    guard: null,
  },
  {
    rung: 8,
    name: "scored and did not pass",
    facts: {
      state: "completed",
      mode: "replay",
      scored: digest({ pass: false, inconclusive: false, reason: "3 value divergence(s)", total_correlations: 93, matched_correlations: 91 }),
    },
    expected: "DIVERGED",
    guard: null,
  },
];

describe("resultOf: the precedence ladder", () => {
  for (const c of LADDER) {
    describe(`rung ${c.rung} — ${c.name}`, () => {
      test("full RunRow", () => {
        const r = resultOf(asFullRow(c.facts));
        expect(r.state).toBe(c.expected);
        if (c.guard) expect(r.guard).toMatch(c.guard);
        else expect(r.guard).toBeNull();
      });

      test("list RunSummaryRow", () => {
        const r = resultOf(asSummaryRow(c.facts));
        expect(r.state).toBe(c.expected);
        if (c.guard) expect(r.guard).toMatch(c.guard);
        else expect(r.guard).toBeNull();
      });
    });
  }
});

describe("the two row shapes agree", () => {
  // The property the digest change actually asserted: projecting a scorecard to
  // five scalars must not change what the ladder concludes. Stated over the
  // whole ladder rather than per rung, so a shape that diverges anywhere fails.
  for (const c of LADDER) {
    test(`rung ${c.rung} reaches the same result from either shape`, () => {
      expect(resultOf(asSummaryRow(c.facts))).toEqual(resultOf(asFullRow(c.facts)));
    });
  }
});

describe("REPRODUCED is reachable from exactly one place", () => {
  // A blunter statement of the same safety property: of every fixture above,
  // only rung 7 may be green. If a change makes any other rung reachable to
  // REPRODUCED, this fails even if that rung's own assertion was also updated.
  test("only rung 7 produces a good tone", () => {
    const green = LADDER.filter((c) => resultOf(asFullRow(c.facts)).tone === "good");
    expect(green.map((c) => c.rung)).toEqual([7]);
  });
});
