// What the "compared with main" panel may claim, decided from its reads.
//
// The panel joins a delta with each run's HTTP diffs. The diffs endpoint
// answers an absent artifact with an error naming why, so a failed read must
// reach the reader as that reason, never as an empty list the headline then
// reads as agreement. The same three states as `evidence.ts`: answered,
// failed with a reason, or not yet known.

import type { Delta, DeltaResponse, HttpDiff } from "./api";
import { reasonOf, type Read } from "./evidence";

export type ReadState<T> =
  | { state: "ok"; data: T }
  | { state: "pending" }
  | { state: "failed"; reason: string };

export function readState<T>(r: Read<T>): ReadState<T> {
  if (r.error) return { state: "failed", reason: reasonOf(r.error) };
  if (r.data === undefined) return { state: "pending" };
  return { state: "ok", data: r.data };
}

/** The requests the delta compared: both runs drove them. A request only one
 *  run drove has no delta rows, and must not read as agreement. */
export function comparedDiffs(d: Delta, diffs: HttpDiff[]): HttpDiff[] {
  const outside = new Set([...d.uncovered.y_only, ...d.uncovered.m_only]);
  return diffs.filter((x) => !outside.has(x.correlation_id));
}

type TapeSide = { members?: string[]; correlations?: number; member_seals?: { recording_id: string; seal_id: string }[] };

/** The seal each side read, as one line for the recording card. The seal is a
 *  content address: equal seals are equal tapes, and it is the only field that
 *  shows a pair straddling a re-seal. */
export function sealLine(d: Delta): string {
  const check = (d as Delta & { tape_check?: { y?: TapeSide; m?: TapeSide } }).tape_check;
  const seals = (s?: TapeSide) => (s?.member_seals ?? []).map((m) => m.seal_id).sort().join(", ");
  const y = seals(check?.y);
  const m = seals(check?.m);
  if (!check) return "seal not recorded for this comparison";
  if (y && m) return y === m ? `seal ${y}, read by both runs` : `this PR read ${y}, main read ${m}`;
  const count = (s?: TapeSide) => s?.correlations ?? "?";
  return `seal not recorded (a run predates seal recording); both read ${count(check.y)} requests of the same members`;
}

/** A pending delta is polled for ten minutes, then left for the reader to
 *  reload: a persistently unfetchable report must not poll forever. */
export const PENDING_POLLS = 40;
export function pollPending(r: DeltaResponse | undefined, updates: number): boolean {
  return !!r && "unavailable" in r && r.unavailable_kind === "pending" && updates < PENDING_POLLS;
}
