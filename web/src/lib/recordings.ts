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
  // The UCS boot-derived shape: the id IS the boot instant.
  if (identity.booted_at_nanos) {
    const ms = Number(identity.booted_at_nanos.slice(0, -6));
    const when = Number.isFinite(ms)
      ? new Date(ms).toISOString().replace("T", " ").slice(5, 16) + " UTC"
      : identity.booted_at_nanos;
    return `recorder booted ${when}`;
  }
  if (!identity.revision || !identity.recorded_at || !identity.instance) return null;
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
  // The catalog answers first — a pulled recording was counted event by event —
  // and the SEAL answers when it has not been pulled, which is most of the time.
  // Only when neither knows is the count unknown, and the sentence says SEALED
  // rather than PULLED because sealing is what now makes it knowable.
  const corrs = catalog?.correlation_count ?? rec.correlations;
  if (corrs == null) return `${objects} · correlation count not known until it is sealed`;
  return `${objects} · ${corrs.toLocaleString()} correlation${corrs === 1 ? "" : "s"}`;
}

/** The one-line form for a picker row: span, then scale. */
export function rowSummary(rec: AvailableRecording, catalog: RecordingRow | undefined): string {
  return `${spanOf(rec.dates).text} · ${scaleText(rec, catalog)}`;
}

// -- DEPLOYMENT DAYS ---------------------------------------------------------
//
// The answer to the problem this module's header states. A recording id is
// minted once per router process, so "pick a recording" offers one pod's
// arbitrary slice of a deployment's traffic — and until now the only honest
// thing a client could do was SAY so on the row. It can now do better: the
// server reports which deployment-day each session belongs to, so the client
// can offer the day.

/** A deployment's day: every session `<revision>` wrote on `<MMDD>`. */
export type DeploymentDay = {
  /** `<revision>-<MMDD>`, exactly as the server reports it and as a run's
   *  `recording_group` must name it. Never reconstructed here. */
  group: string;
  revision: string;
  /** `MMDD`, with no year — the same amount the id carries. */
  day: string;
  /** Newest first, in the order the caller supplied. */
  members: AvailableRecording[];
  sealed: number;
  unsealed: number;
  /** Distinct pods that wrote it. */
  instances: number;
  /** Correlations across sealed members. NULL when nothing is sealed yet —
   *  not zero, by the same rule `scaleText` follows for one recording. */
  correlations: number | null;
  /** The partitions the day's objects actually landed in. Usually one; two
   *  when a member straddled midnight, which is why this is derived from the
   *  members' dates rather than assumed from the group name. */
  span: Span;
  /**
   * Every member is sealed, so replaying this day costs no compaction.
   *
   * THE RULE THE PIPELINE USES, and for the same reason: resolving a group
   * hands all its members to one pull, and a member without a manifest is
   * compacted INLINE inside the replay run. A day still being written always
   * has unsealed members, so offering it would make the run pay for sealing
   * the sealer has already scheduled.
   */
  complete: boolean;
};

export type GroupedRecordings = {
  /** Newest first. */
  days: DeploymentDay[];
  /** Sessions the server declined to group — a boot-derived id has no day.
   *  Kept as themselves rather than bundled into a synthetic group. */
  ungrouped: AvailableRecording[];
};

/**
 * Club sessions into the deployment-days they belong to.
 *
 * ORDER IS TAKEN FROM THE INPUT, never from the group name. `3093f22-0910`
 * sorts before `3093f22-0911`, so ordering days by their name reads the
 * second-newest as the newest — the same trap that makes `group_by` the wrong
 * tool in the pipeline's own selection. The server already returns sessions
 * newest-first; distinct days come out in first-appearance order, which
 * preserves that.
 */
export function groupRecordings(recordings: AvailableRecording[]): GroupedRecordings {
  const byGroup = new Map<string, AvailableRecording[]>();
  const ungrouped: AvailableRecording[] = [];
  for (const rec of recordings) {
    const g = rec.group;
    if (!g) {
      ungrouped.push(rec);
      continue;
    }
    const bucket = byGroup.get(g);
    if (bucket) bucket.push(rec);
    else byGroup.set(g, [rec]);
  }
  // Map preserves insertion order, which is first-appearance order.
  const days = [...byGroup.entries()].map(([group, members]) => {
    const sealed = members.filter((m) => m.sealed === true);
    // Summed over SEALED members only. An unsealed member contributes null,
    // and adding null as zero would report a day as smaller than it is.
    const correlations = sealed.length
      ? sealed.reduce((n, m) => n + (m.correlations ?? 0), 0)
      : null;
    const dash = group.lastIndexOf("-");
    return {
      group,
      revision: dash > 0 ? group.slice(0, dash) : group,
      day: dash > 0 ? group.slice(dash + 1) : "",
      members,
      sealed: sealed.length,
      unsealed: members.length - sealed.length,
      instances: new Set(members.flatMap((m) => m.instances ?? [])).size,
      correlations,
      span: spanOf([...new Set(members.flatMap((m) => m.dates))].sort()),
      complete: members.length > 0 && sealed.length === members.length,
    };
  });
  return { days, ungrouped };
}

/** `09-11 · 68 pods · 735 correlations`, or what is known instead. */
export function daySummary(day: DeploymentDay): string {
  const when = day.day.length === 4 ? `${day.day.slice(0, 2)}-${day.day.slice(2)}` : day.day;
  const pods = `${day.instances || day.members.length} pod${
    (day.instances || day.members.length) === 1 ? "" : "s"
  }`;
  // Same rule as one recording: unknown is not zero. A day with nothing sealed
  // has a correlation count nobody has counted, not a count of none.
  const scale =
    day.correlations == null
      ? "correlation count not known until it is sealed"
      : `${day.correlations.toLocaleString()} correlation${day.correlations === 1 ? "" : "s"}`;
  // Deliberately not the words the caller's own badge uses. A band that reads
  // "still sealing · 4 still sealing" has said one thing twice and counted
  // nothing; the badge carries the state, this carries the number.
  const pending = day.unsealed ? ` · ${day.unsealed} not sealed yet` : "";
  return `${when} · ${pods} · ${scale}${pending}`;
}
