// The evidence views are built from two reads, the call ledger and the HTTP
// diffs. The API answers an artifact that is not there with an error naming
// why; a view that reads `data ?? []` turns that answer back into zero
// findings. This is the one place the views get their rows, so a failed read
// arrives with them as a named gap rather than as an empty list.

import type { CallRecord, HttpDiff } from "./api";

export type Read<T> = { data?: T; error?: unknown };

export type Evidence = {
  calls: CallRecord[];
  https: HttpDiff[];
  /** Set when the view cannot be built at all: the ledger it is spined on is missing. */
  blocking: string | null;
  /** What is missing from a view that can still be built. */
  notes: string[];
  /** True only when every read succeeded, so an empty result is a fact about the run. */
  complete: boolean;
  /** A read has neither answered nor failed yet. */
  pending: boolean;
};

/** What an empty findings list may claim: a fact about the run only when every read succeeded. */
export function emptyFindingsText(e: Evidence): string {
  if (e.complete) return "no divergence rows were published for this run.";
  if (e.pending) return "still reading the run's evidence…";
  return "no divergence rows in what could be read; what could not be read is named above.";
}

export function reasonOf(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

const settled = (r: Read<unknown>) => r.data !== undefined || !!r.error;

export function evidenceOf(calls: Read<CallRecord[]>, https: Read<HttpDiff[]>): Evidence {
  const notes: string[] = [];
  const pending = !settled(calls) || !settled(https);
  if (https.error)
    notes.push(`HTTP differences are not shown, because they could not be read: ${reasonOf(https.error)}`);
  return {
    calls: calls.data ?? [],
    // A failed refetch can leave an earlier answer cached; the note says the
    // diffs are not shown, so they are not.
    https: https.error ? [] : (https.data ?? []),
    blocking: calls.error ? `The call ledger could not be read: ${reasonOf(calls.error)}` : null,
    notes,
    complete: !pending && !calls.error && !https.error,
    pending,
  };
}
