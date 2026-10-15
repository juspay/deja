import { runParams, type RunSummaryRow } from "./api";

/**
 * Which list a run belongs on: the system it drove, or the merge queue.
 *
 * The merge queue is not a system. Its runs drive prism like any pull request
 * does, but they answer a different question — will this PR, rebased onto the
 * queue, still behave — and mixed into the PR runs they read as retries of the
 * same PR. So they get a list of their own, and a system's list is that
 * system's runs minus the queue's.
 */
export const MERGE_QUEUE = "merge-queue";

/**
 * How the pipeline marks a queue run today: its label starts with this.
 *
 * The one place the label is read rather than displayed. There is no
 * structured field for it yet; when the pipeline sends one, test it here and
 * keep the prefix as the fallback for the runs already stored.
 */
const QUEUE_LABEL = "MQ · ";

/**
 * Every run's list, keyed by run id.
 *
 * A queue run's baseline carries no queue mark of its own, but it exists only
 * because that queue run named it in `delta_against`, so it goes where the
 * run that needed it goes. A baseline shared with a plain PR run stays on the
 * system's list.
 *
 * `defaultSystem` is where a run that names no system lands, as the wire
 * contract says; while the registry is still loading it is undefined and such
 * runs land under "".
 */
export function scopesOf(runs: RunSummaryRow[], defaultSystem?: string): Map<string, string> {
  const queue = new Set<string>();
  for (const r of runs) {
    const p = runParams(r);
    if ((p?.label ?? "").startsWith(QUEUE_LABEL)) {
      queue.add(r.run_id);
      if (p?.delta_against) queue.add(p.delta_against);
    }
  }
  for (const r of runs) {
    const against = runParams(r)?.delta_against;
    if (against && queue.has(against) && !queue.has(r.run_id)) queue.delete(against);
  }
  const out = new Map<string, string>();
  for (const r of runs) {
    out.set(r.run_id, queue.has(r.run_id) ? MERGE_QUEUE : runParams(r)?.system_under_test || defaultSystem || "");
  }
  return out;
}

/**
 * The repository each system's runs build from, as the runs themselves say
 * (`github.repo` on a pull request run, `candidate_repo` otherwise) — so the
 * picker can say `hyperswitch-prism` where the registry only says `prism`.
 */
export function reposOf(runs: RunSummaryRow[], defaultSystem?: string): Map<string, string> {
  const out = new Map<string, string>();
  for (const r of runs) {
    const p = runParams(r);
    const system = p?.system_under_test || defaultSystem;
    const repo = p?.github?.repo || p?.candidate_repo;
    if (system && repo && !out.has(system)) out.set(system, repo.split("/").pop() || repo);
  }
  return out;
}

/** What the candidate is, in the creator's words, for the list's PR column. */
export function changeOf(run: RunSummaryRow): string | null {
  const p = runParams(run);
  if (p?.label) return p.label;
  return p?.github?.pr_number ? `PR #${p.github.pr_number}` : null;
}
