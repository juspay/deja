// Reading the bucket index the way a person has to read it.
//
// `GET /api/v1/recordings/available` lists what is IN THE BUCKET; the catalog
// (`GET /api/v1/recordings`) lists what has been PULLED, which is a property of
// what has been replayed. Neither is a superset of the other, so every view
// that offers a recording joins them here rather than picking one and hoping.
//
// The hard fact this module exists to state plainly: a recording id is minted
// ONCE PER ROUTER PROCESS. A long-lived pod therefore produces ONE recording
// for its whole lifetime — the live index has a single id spanning seven daily
// partitions and 42,310 correlations. "Pick a recording" is offering a week of
// production traffic as one item, and the only honest fix available to a
// client is to say so on the row.

import type { AvailableRecording, RecordingIdentity, RecordingRow } from "./api";

const DAY_MS = 86_400_000;

export type Span = {
  /** First and last dated partition, ISO. Empty when the keys carry no `dt=`. */
  from: string | null;
  to: string | null;
  /** Partitions that actually hold objects. */
  partitions: number;
  /** Calendar days from first to last inclusive — larger than `partitions`
   *  when the session went quiet for a day and came back. */
  calendarDays: number;
  /** More than one partition: this is not "a recording", it is a pod's life. */
  multiDay: boolean;
  /** `2026-07-29 → 2026-08-04`, or a single date, or a stated absence. */
  range: string;
  /** The range plus its duration, spelling out gaps when there are any. */
  text: string;
};

/**
 * Describe the daily partitions a session's objects landed in.
 *
 * `dates` comes back sorted and de-duplicated (the server builds it from a
 * BTreeSet), so first and last are the ends of the span.
 */
export function spanOf(dates: string[]): Span {
  const partitions = dates.length;
  if (partitions === 0) {
    return {
      from: null,
      to: null,
      partitions: 0,
      calendarDays: 0,
      multiDay: false,
      range: "undated",
      text: "undated — the keys carry no dt= partition",
    };
  }
  const from = dates[0];
  const to = dates[partitions - 1];
  const calendarDays = Math.max(
    1,
    Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / DAY_MS) + 1,
  );
  const range = partitions === 1 ? from : `${from} → ${to}`;
  const text =
    partitions === 1
      ? `${range} · 1 day`
      : calendarDays === partitions
        ? `${range} · ${partitions} days`
        : `${range} · ${partitions} days with traffic, across ${calendarDays}`;
  return {
    from,
    to,
    partitions,
    calendarDays,
    multiDay: partitions > 1 || calendarDays > 1,
    range,
    text,
  };
}

/**
 * Render what a recording's id claims about its own provenance.
 *
 * `identity` is null on every recording made before ids carried a revision —
 * that is the normal past, NOT an error, so absence renders as nothing at all
 * rather than as a warning. `recorded_at` is `MMDDhhmm` UTC with no year, and
 * is shown as exactly that much.
 */
export function identityText(identity: RecordingIdentity | null): string | null {
  if (!identity) return null;
  const { revision, recorded_at, instance } = identity;
  const when =
    recorded_at.length === 8
      ? `${recorded_at.slice(0, 2)}-${recorded_at.slice(2, 4)} ${recorded_at.slice(
          4,
          6,
        )}:${recorded_at.slice(6, 8)} UTC`
      : recorded_at;
  return `rev ${revision} · recorded ${when} · instance ${instance}`;
}

/** Catalog rows by recording id, for joining onto the bucket index. */
export function catalogById(rows: RecordingRow[] | undefined): Map<string, RecordingRow> {
  return new Map((rows ?? []).map((r) => [r.recording_id, r]));
}

/**
 * How big is this, in the unit the reader actually cares about?
 *
 * `objects` counts LANDING OBJECTS — gzipped envelope batches — and is a poor
 * proxy for traffic: the live index has 22,936 objects holding 170,568
 * correlations and 1,680 objects holding 42,310. So the correlation count is
 * used when the catalog has it, and when it does not the answer is "not known
 * yet", never an estimate derived from object count.
 */
export function scaleText(rec: AvailableRecording, catalog: RecordingRow | undefined): string {
  const objects = `${rec.objects.toLocaleString()} landing object${rec.objects === 1 ? "" : "s"}`;
  const corrs = catalog?.correlation_count;
  if (corrs == null) return `${objects} · correlation count not known until it is pulled`;
  return `${objects} · ${corrs.toLocaleString()} correlation${corrs === 1 ? "" : "s"}`;
}

/** The one-line form for a picker row: span, then scale. */
export function rowSummary(rec: AvailableRecording, catalog: RecordingRow | undefined): string {
  return `${spanOf(rec.dates).text} · ${scaleText(rec, catalog)}`;
}
