// WHERE A RUN'S CANDIDATE CORRELATIONS COME FROM — the only module that answers
// it, so the answer can be replaced in one place.
//
// It is now `GET /api/v1/recordings/{id}/correlations`, served from the sealed
// index (manifest + sidecar, no tape pull). That endpoint replaced the previous
// derivation, which unioned `scorecard.correlation_scope` and
// `scorecard.per_correlation[]` over the run list — the only ids a client could
// see before it existed, and circular by construction: a correlation only became
// selectable once some run had already driven it, so a fresh recording offered
// nothing. Nothing of that derivation is kept. It cannot answer the question the
// index answers, it needed a second truth-level (`complete: false`) threaded
// through every state the picker renders, and it made the home page fetch the
// whole run list. Being superseded, it is deleted rather than demoted to a
// fallback.
//
// THREE ANSWERS, KEPT THREE. Sealed, unsealed, unknown — see `CorrelationState`.
// The middle one is the one worth guarding: a recording whose manifest has not
// been written yet has correlations that simply cannot be listed cheaply, and
// rendering that as an empty picker would say "this recording has no
// correlations", which is false. A fourth outcome, "the lookup itself failed",
// is `error` and is likewise never an absence.

import React from "react";
import { useQuery } from "@tanstack/react-query";
import { ApiError, recordingCorrelations } from "./api";

/**
 * THE HARD CAP: correlations one run may drive.
 *
 * Two different things lean on it and they must not be confused.
 *
 *  - An EXPLICIT selection over the cap is REFUSED, never trimmed. A scorecard
 *    from a silently-truncated selection reads as complete and is not. See
 *    `propose` in components/CorrelationPicker.tsx.
 *  - NO selection takes the cap as a DEFAULT: the first `CORRELATION_CAP`
 *    correlations in the recording's own order. Correlation ids are
 *    time-ordered, so that is the earliest requests in the recording, not an
 *    arbitrary hundred — which is what makes it a defensible default and
 *    something the form can state in words.
 *
 * `RunSpec` has no limit field, so a client bounds a run only by naming ids.
 * When the ids are knowable this module names them, so the run's scope is
 * exactly what the form showed. When they are not (an unsealed recording), the
 * request carries no filter and the ORCHESTRATOR applies the same limit — the
 * one case where the dashboard cannot say in advance which hundred will run, and
 * says that instead of guessing.
 */
export const CORRELATION_CAP = 100;

/**
 * How much of the index to pull for the picker.
 *
 * Well above the cap, so searching and range-selecting have room, and far below
 * the largest recordings (42,310 and 170,568 correlations live) so the picker
 * never waits on a multi-megabyte list. Offset 0 in the server's order means the
 * head of this page is the head of the index, so the default window is exact
 * even when the page is a fraction of the whole.
 */
export const CANDIDATE_PAGE = 1000;

/** How a candidate list was obtained. */
export type CorrelationOrigin =
  /** The recording's own sealed correlations index. */
  | "sealed-index";

/** Which of the index's answers this is. */
export type CorrelationState =
  /** No recording chosen yet, or the lookup is still in flight. */
  | "pending"
  /** The index is final; `candidates` is the recording's own list. */
  | "sealed"
  /** The recording exists but its manifest is not written yet. NOT "empty". */
  | "unsealed"
  /** The index has no entry under this id. */
  | "unknown";

export type CorrelationCandidate = {
  id: string;
  /** Position in the recording's own order. 1 is the earliest request. */
  ordinal: number;
};

export type CorrelationSource = {
  /** Ids that can be offered for selection right now, in the index's order. */
  candidates: CorrelationCandidate[];
  /** How many correlations the RECORDING holds. Null = genuinely not known. */
  total: number | null;
  origin: CorrelationOrigin;
  /** True when `candidates` is the recording's whole list, not a page of it. */
  complete: boolean;
  state: CorrelationState;
  loading: boolean;
  /** A failure to LOOK. Rendered as that, never as an empty list. */
  error: string | null;
  /**
   * What a run with NO explicit selection will drive, named exactly.
   *
   * The first `CORRELATION_CAP` of the index when the index can be read, and
   * empty when it cannot — in which case the server applies the same limit and
   * `defaultIsServerSide` is true.
   */
  defaultScope: string[];
  /** True when the cap is applied by the orchestrator because this page cannot name the ids. */
  defaultIsServerSide: boolean;
};

/**
 * The recording's correlations, plus the default scope a run inherits when
 * nothing is picked.
 *
 * A 404 is "no such recording", which is a real answer and is reported as one. A
 * 5xx or a network failure is "could not look", which is not an answer at all
 * and is reported as `error` — the same distinction `RecordingPicker` draws for
 * the bucket listing, and the reason `request()` carries a status.
 */
export function useCorrelationCandidates(recordingId: string): CorrelationSource {
  const id = recordingId.trim();
  const q = useQuery({
    queryKey: ["recording-correlations", id],
    queryFn: () => recordingCorrelations(id, CANDIDATE_PAGE),
    enabled: !!id,
    // A recording seals once and stays sealed, and an unsealed one is not going
    // to seal while a form is open. Nothing here is worth re-fetching often.
    staleTime: 5 * 60_000,
    // "No such recording" is an answer. Retrying it is just a slower answer.
    retry: (count, err) => ((err as ApiError).status === 404 ? false : count < 1),
  });

  const notFound = (q.error as ApiError | null)?.status === 404;
  const data = q.data;
  const sealed = !!data && (data.sealed === true || data.status === "sealed");
  const listed = data?.correlations;

  const candidates = React.useMemo<CorrelationCandidate[]>(
    () =>
      // The index's order is arrival order and is not re-sorted here: a client
      // sort could only disagree with the index it is displaying.
      (sealed ? (listed ?? []) : []).map((cid, i) => ({ id: cid, ordinal: i + 1 })),
    [listed, sealed],
  );

  const state: CorrelationState = !id || q.isLoading
    ? "pending"
    : notFound
      ? "unknown"
      : sealed
        ? "sealed"
        : data
          ? "unsealed"
          : "pending";

  // Exact even when `candidates` is only a page: the page starts at offset 0 in
  // the index's own order, so its first N are the index's first N.
  const defaultScope = candidates.slice(0, CORRELATION_CAP).map((c) => c.id);

  return {
    candidates,
    total: data?.total ?? null,
    origin: "sealed-index",
    complete: sealed && candidates.length >= (data?.total ?? candidates.length),
    state,
    loading: !!id && q.isLoading,
    // A 404 is an answer, not a failure to look, so it is not reported here.
    error: q.error && !notFound ? String(q.error) : null,
    defaultScope,
    defaultIsServerSide: defaultScope.length === 0,
  };
}
