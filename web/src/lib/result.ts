// The single place that answers "what happened to this run".
//
// COMPUTED FROM THE RUN OBJECT ALONE — `run.state`, `run.failure`, and the
// scorecard the API embeds on the run row. Never from `api.scorecard()`.
//
// That endpoint SYNTHESISES a scorecard when no artifact exists. Verified live
// on run-18c888854e44e86e (state=failed, "session not found under s3://…"):
// `GET /runs/{id}/scorecard` returns 200 with
//     correlation_scope: [3 ids]        <- looks like a scope was applied
//     summary.total_correlations: 0     <- nothing was ever compared
//     verdict.inconclusive: true, reason: "no artifacts ingested for this run yet"
// Reading it is how a UI lands on "3 correlations matched 0 of 42,310" for a run
// that died in stage 1 of 6 and never started a candidate.
//
// FIRST RULE: `state === "failed"` plus a failure message outranks any scorecard,
// synthesised or not. A run that failed did not produce evidence.
//
// `total_correlations === 0` is a GUARD THAT FORBIDS GREEN, not a sixth state:
// it downgrades to INCONCLUSIVE and carries the reason.

import { RunRow, Scorecard, runParams } from "./api";

export type ResultState =
  /** Not terminal yet. Nothing is known. */
  | "RUNNING"
  /** Terminal, but the run produced no verdict: it failed, or it is a record run. */
  | "NO_VERDICT"
  /** Scored, but the evidence does not support a judgement either way. */
  | "INCONCLUSIVE"
  /** Scored: the candidate did not reproduce the recording. */
  | "DIVERGED"
  /** Scored: the candidate reproduced the recording. The ONLY green state. */
  | "REPRODUCED";

/** Every state, in the order a filter dropdown should list them. */
export const RESULT_STATES: ResultState[] = [
  "REPRODUCED",
  "DIVERGED",
  "INCONCLUSIVE",
  "NO_VERDICT",
  "RUNNING",
];

export type ResultTone = "neutral" | "warn" | "bad" | "good";

export type RunResult = {
  state: ResultState;
  /** Short word for a chip. */
  label: string;
  /** Palette key. `good` is reachable from REPRODUCED and nowhere else. */
  tone: ResultTone;
  /** One sentence stating what is known — never what is guessed. */
  headline: string;
  /** The server's own words: `verdict.reason` or `failure.message`. */
  detail: string | null;
  /** Why a verdict could not be trusted, when that is what decided the state. */
  guard: string | null;
};

const TERMINAL = new Set(["completed", "failed"]);

function failureText(run: RunRow): string | null {
  const m = run.failure?.message?.trim();
  if (m) return m;
  const live = run.live?.failure_reason?.trim();
  return live || null;
}

/**
 * The result of a run, from the run row alone.
 *
 * Precedence, in order — each rule is a reason a later rule must not be reached:
 *   1. not terminal                -> RUNNING
 *   2. state === "failed"          -> NO_VERDICT   (outranks any scorecard)
 *   3. mode !== "replay"           -> NO_VERDICT   (a recording is not scored)
 *   4. no embedded scorecard       -> NO_VERDICT   (terminal with no evidence)
 *   5. verdict.inconclusive        -> INCONCLUSIVE
 *   6. total_correlations === 0    -> INCONCLUSIVE (the guard; forbids green)
 *   7. verdict.pass === true       -> REPRODUCED
 *   8. otherwise                   -> DIVERGED
 */
export function resultOf(run: RunRow): RunResult {
  const state = (run.state || "").toLowerCase();

  // 1. Still moving.
  if (!TERMINAL.has(state)) {
    return {
      state: "RUNNING",
      label: "running",
      tone: "neutral",
      headline: run.live?.stage
        ? `in progress — ${run.live.stage}`
        : `in progress — ${state || "queued"}`,
      detail: null,
      guard: null,
    };
  }

  // 2. THE FIRST RULE. A failed run did not produce evidence, so no scorecard —
  // real or synthesised — may speak for it.
  if (state === "failed") {
    return {
      state: "NO_VERDICT",
      label: "no verdict",
      tone: "bad",
      headline: "the run did not complete — the candidate was never judged",
      detail: failureText(run) ?? "the run failed and recorded no reason",
      guard: null,
    };
  }

  // 3. A record run produces a recording. There is nothing to compare it to.
  if (run.mode !== "replay") {
    return {
      state: "NO_VERDICT",
      label: "no verdict",
      tone: "neutral",
      headline: "record run — a recording is produced, not scored",
      detail: null,
      guard: null,
    };
  }

  const sc: Scorecard | null = run.scorecard;

  // 4. Terminal, but nothing was scored.
  if (!sc) {
    return {
      state: "NO_VERDICT",
      label: "no verdict",
      tone: "warn",
      headline: "the run completed but no scorecard was produced",
      detail: run.verdict ? `the run row records verdict "${run.verdict}" with no scorecard behind it` : null,
      guard: "no scorecard artifact — there is no evidence to report",
    };
  }

  const reason = sc.verdict?.reason?.trim() || null;
  const total = sc.summary?.total_correlations ?? 0;
  const matched = sc.summary?.matched_correlations ?? 0;

  // 5. The scorer said so itself.
  if (sc.verdict?.inconclusive) {
    return {
      state: "INCONCLUSIVE",
      label: "inconclusive",
      tone: "warn",
      headline: "the evidence does not support a judgement either way",
      detail: reason,
      guard: null,
    };
  }

  // 6. THE GUARD. Nothing was compared, so nothing may be claimed. This is not a
  // sixth state — it downgrades to INCONCLUSIVE and says why.
  if (total === 0) {
    return {
      state: "INCONCLUSIVE",
      label: "inconclusive",
      tone: "warn",
      headline: "no test case was compared — a pass here would be vacuous",
      detail: reason,
      guard: "the scorecard compared 0 correlations",
    };
  }

  // 7. The only green.
  if (sc.verdict?.pass === true) {
    return {
      state: "REPRODUCED",
      label: "reproduced",
      tone: "good",
      headline: `${matched} of ${total} recorded request${total === 1 ? "" : "s"} reproduced exactly`,
      detail: reason,
      guard: null,
    };
  }

  // 8. Scored and it did not pass.
  return {
    state: "DIVERGED",
    label: "diverged",
    tone: "bad",
    headline: `${total - matched} of ${total} recorded request${total === 1 ? "" : "s"} did not reproduce`,
    detail: reason,
    guard: null,
  };
}

/**
 * The WHAT HAPPENED column: the server's own sentence about this run.
 * `verdict.reason` when it was scored, `failure.message` when it was not.
 */
export function whatHappened(run: RunRow): string | null {
  return run.scorecard?.verdict?.reason?.trim() || failureText(run);
}

/** A candidate ref rendered for a table cell, for every CandidateSpec kind. */
export function candidateRef(run: RunRow): { label: string; full: string } {
  const c = (run.candidate ?? {}) as Record<string, unknown>;
  const s = (k: string) => (typeof c[k] === "string" ? (c[k] as string) : "");
  switch (c.kind) {
    case "prebuilt_image": {
      const image = s("image");
      // Registry host and repo path are the same on every run here; the tag is
      // the only part that identifies the candidate.
      return { label: image.split("/").pop() || image || "—", full: image };
    }
    case "repo_sha": {
      const full = `${s("repo")}@${s("sha")}`;
      return { label: `${s("repo").split("/").pop()}@${s("sha").slice(0, 12)}`, full };
    }
    case "repo_branch": {
      const full = `${s("repo")}#${s("branch")}`;
      return { label: `${s("repo").split("/").pop()}#${s("branch")}`, full };
    }
    case "repo_pr": {
      const full = `${s("repo")} PR ${c.pr}`;
      return { label: `${s("repo").split("/").pop()} PR ${c.pr}`, full };
    }
    case "local_path": {
      const p = s("binary_or_source");
      return { label: p.split("/").pop() || p || "—", full: p };
    }
    default:
      return { label: String(c.kind ?? "—"), full: JSON.stringify(c) };
  }
}

/**
 * Attempt ordinals, DERIVED CLIENT-SIDE from what the list endpoint carries.
 *
 * The subject is what makes two runs the same experiment: mode + recording +
 * candidate ref + the correlation scope that was driven. The scope comes from
 * the run's persisted request; rows created before requests were persisted
 * carry none, and two of those differing only in scope still group together.
 * Labelled "derived" in the UI for that reason.
 */
export function attemptOrdinals(runs: RunRow[]): Map<string, { attempt: number; of: number }> {
  const subject = (r: RunRow) => {
    const scope = runParams(r)?.correlation_filter;
    const scopeKey = scope ? [...scope].sort().join(",") : "*";
    return `${r.mode}|${(r.recording_id ?? "").trim()}|${candidateRef(r).full}|${scopeKey}`;
  };
  const groups = new Map<string, RunRow[]>();
  for (const r of runs) {
    const k = subject(r);
    const g = groups.get(k);
    if (g) g.push(r);
    else groups.set(k, [r]);
  }
  const out = new Map<string, { attempt: number; of: number }>();
  for (const g of groups.values()) {
    const ordered = [...g].sort(
      (a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime(),
    );
    ordered.forEach((r, i) => out.set(r.run_id, { attempt: i + 1, of: ordered.length }));
  }
  return out;
}
