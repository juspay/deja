import React from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, availableRecordings, AvailableRecording, RecordingRow } from "../lib/api";
import {
  catalogById,
  daySummary,
  groupRecordings,
  identityText,
  spanOf,
  type DeploymentDay,
} from "../lib/recordings";
import { useSystems } from "../lib/systems";

/* The seal/coverage badges read the compactor's manifest: a sealed session
   with zero per-instance gseq gaps is replay-grade; gaps or no manifest mean
   the recording may be partial. A recording that has never been pulled has no
   manifest to read, which is a different statement from "unsealed" — so it is
   not given a badge at all. */
function CoverageBadges({ r }: { r: RecordingRow }) {
  const m = r.manifest;
  if (!m) return <span className="chip">unsealed</span>;
  const gaps = m.instances.reduce((n, i) => n + i.gaps.length, 0);
  const dupes = m.counts.duplicates_dropped;
  return (
    <>
      <span className="chip pass">sealed</span>{" "}
      <span className={`chip ${gaps === 0 ? "pass" : "fail"}`}>
        {gaps === 0 ? "0 gaps" : `${gaps} gaps`}
      </span>
      {dupes > 0 && <span className="chip"> {dupes} dupes dropped</span>}
    </>
  );
}

/* The same coverage statement for a recording that has NOT been pulled.
   Until the seal existed this cell could only say "—", because the manifest
   above is read from the catalog and the catalog holds only what some previous
   replay ingested. The seal knows the same facts without an ingest, so a
   recording in the bucket can now say whether it is replay-grade.

   `gaps == null` on a sealed row is treated as UNKNOWN, not as zero: claiming
   "0 gaps" from a field that never arrived would be the one badge a reader
   most needs to trust. */
function SealBadges({ r }: { r: AvailableRecording }) {
  if (!r.sealed) return <span className="chip">unsealed</span>;
  if (r.gaps == null) return <span className="chip pass">sealed</span>;
  return (
    <>
      <span className="chip pass">sealed</span>{" "}
      <span className={`chip ${r.gaps === 0 ? "pass" : "fail"}`}>
        {r.gaps === 0 ? "0 gaps" : `${r.gaps} gaps`}
      </span>
    </>
  );
}

function Span({ dates }: { dates: string[] }) {
  const span = spanOf(dates);
  return (
    <>
      <span className="recspan-range">{span.range}</span>
      {span.multiDay && (
        <span className="chip modified" title="one recording id, many days of traffic">
          {span.partitions} days
        </span>
      )}
    </>
  );
}

function Identity({ rec }: { rec: AvailableRecording }) {
  const parts: string[] = [];
  const text = identityText(rec.identity);
  if (text) parts.push(text);
  // The `inst=` discriminators — for a UCS session, the recorder's pod name,
  // which the id itself does not carry.
  if (rec.instances?.length) parts.push(`pod ${rec.instances.join(", ")}`);
  // No identity means the id predates ids naming a revision. That is the
  // ordinary past — the facts still live in the recording's envelopes — so it
  // reads as a plain dash, not as a missing-data warning.
  if (!parts.length) return <span className="recdash">—</span>;
  return <span className="recident">{parts.join(" · ")}</span>;
}

/* THE DAY a set of recordings belongs to, as a band across the table.

   A recording id is minted once per router process, so a row below the band is
   one pod's arbitrary slice: pods are replaced roughly every thirty minutes,
   and the traffic a deployment served in a day is spread across dozens of them
   — 140 on the day this was written. The band is the unit a replay actually
   wants, and its link sends the GROUP rather than any one member.

   `complete` decides whether that link is offered at all. Resolving a group
   hands every member to one pull, and a member without a manifest is compacted
   INLINE inside the replay run, so offering a day still being written would
   make the run pay for sealing the sealer has already scheduled. A partial day
   says what it is waiting on instead of offering a slow replay. */
function DayBand({ day, cols }: { day: DeploymentDay; cols: number }) {
  return (
    <tr className="recdayband">
      <td className="mono" colSpan={cols - 1}>
        <b>{day.group}</b>{" "}
        <span className={day.complete ? "chip pass" : "chip muted"}>
          {day.complete ? "fully sealed" : "still sealing"}
        </span>{" "}
        <span className="hint">{daySummary(day)}</span>
      </td>
      <td>
        {day.complete ? (
          <Link to={`/?group=${encodeURIComponent(day.group)}`}>replay this day &rarr;</Link>
        ) : (
          <span
            className="hint"
            title="every recording in a day must be sealed before the day can be replayed without re-compacting inside the run"
          >
            &mdash;
          </span>
        )}
      </td>
    </tr>
  );
}

/**
 * RECORDINGS = WHAT IS IN THE BUCKET.
 *
 * This page used to list `GET /api/v1/recordings`, the catalog — which holds
 * only recordings that have been PULLED, and a recording is pulled when
 * something replays it. So the page answered "what has been replayed?" while
 * being titled with, and read as, "what exists?". Live, the catalog holds 2 and
 * the bucket holds 7.
 *
 * The bucket is now the row set and the catalog is joined onto it, supplying
 * the counts only an ingested recording can have. Catalog rows with no
 * counterpart in the bucket are kept and marked, because dropping them would
 * lose information this page used to show.
 *
 * SIZE IS REPORTED IN TWO UNITS ON PURPOSE. `objects` counts landing objects
 * (gzipped envelope batches) and is known for everything; `requests` counts
 * correlations and is known only once pulled. They are not proportional —
 * 22,936 objects hold 170,568 correlations while 1,680 hold 42,310 — so the
 * unpulled rows say the count is unknown rather than showing a guess.
 */
export default function RecordingsPage() {
  // Which system's bucket to list. The buckets are deliberately separate —
  // prism tapes carry payment payloads on a tighter retention — so this is a
  // SOURCE switch, not a client-side filter of one listing.
  // Empty means the deployment's default bucket. Any other value is a system
  // name from `/api/v1/systems` — a string rather than a union, so a third
  // system needs no change to this file.
  const [system, setSystem] = React.useState<string>("");
  const systems = useSystems();
  const available = useQuery({
    queryKey: ["recordings-available", system],
    queryFn: () => availableRecordings(200, 0, system || undefined),
  });
  const recs = useQuery({ queryKey: ["recordings"], queryFn: api.recordings });

  const rows = available.data?.recordings ?? [];
  const byId = catalogById(recs.data);

  // The table is CLUBBED BY DEPLOYMENT DAY. One row renderer serves both a
  // day's members and the ungrouped tail, so a recording looks the same
  // wherever it appears and the two renderings cannot drift apart.
  const COLS = 10;
  const grouped = groupRecordings(rows);
  const bucketRow = (r: AvailableRecording) => {
    const cat = byId.get(r.recording_id);
    return (
      <tr key={r.recording_id}>
        <td className="mono">
          {r.recording_id}
          {/* Only a non-default system is worth a badge — same rule
              as the runs list. Which name is default comes from the
              orchestrator, so a third system badges itself. */}
          {r.system && !systems.isDefault(r.system) && (
            <span className="chip muted" style={{ marginLeft: 6 }}>{r.system}</span>
          )}
        </td>
        <td className="recspan">
          <Span dates={r.dates} />
        </td>
        <td>
          <span className={r.pulled ? "chip pass" : "chip muted"}>
            {r.pulled ? "pulled" : "in bucket"}
          </span>
        </td>
        <td className="num">{r.objects.toLocaleString()}</td>
        {/* The catalog answers first because a pulled recording has
            been counted event by event. Failing that the SEAL
            answers, which it can do for anything sealed and costs no
            ingest. Only when neither knows is this unknown — and it
            stays a dash rather than being approximated from the
            object count, which is not proportional to either number.
            `??` and not `||`: a genuine zero is an answer. */}
        <td className="num">
          {(cat?.correlation_count ?? r.correlations)?.toLocaleString() ?? "—"}
        </td>
        <td className="num">
          {(cat?.event_count ?? r.events)?.toLocaleString() ?? "—"}
        </td>
        <td className="num">
          {cat?.byte_size ? `${(cat.byte_size / 1048576).toFixed(0)} MB` : "—"}
        </td>
        <td>{cat ? <CoverageBadges r={cat} /> : <SealBadges r={r} />}</td>
        <td>
          <Identity rec={r} />
        </td>
        <td>
          {/* A scoped row replays from ITS bucket: the link carries
              the system + s3 source so the form needs no retyping. */}
          <Link
            to={
              system
                ? `/?recording=${r.recording_id}&system=${system}&s3=${encodeURIComponent(
                    `s3://${r.bucket}/${r.prefix}`,
                  )}`
                : `/?recording=${r.recording_id}`
            }
          >
            replay →
          </Link>
        </td>
      </tr>
    );
  };
  const inBucket = new Set(rows.map((r) => r.recording_id));
  const orphans = (recs.data ?? []).filter((r) => !inBucket.has(r.recording_id));

  if (available.isLoading) {
    return (
      <>
        <h1>Recordings</h1>
        <p className="hint">listing the bucket… (this reads S3 and takes a moment)</p>
      </>
    );
  }

  // A FAILURE TO LOOK IS NOT AN ABSENCE. An empty table here would be read as
  // "no recordings exist"; a 502 from an S3 listing means only that the listing
  // failed. Say which one happened.
  if (available.error) {
    return (
      <>
        <h1>Recordings</h1>
        <div className="recfail">
          <p className="err">
            <b>Could not list the bucket.</b> {String(available.error)}
          </p>
          <p className="hint">
            This says nothing about whether recordings exist — only that the listing did not
            complete. It reads S3 live, so a slow or unreachable bucket, or missing credentials on
            the orchestrator, produce exactly this.
          </p>
          <p>
            <button
              type="button"
              className="btn"
              onClick={() => void available.refetch()}
              disabled={available.isFetching}
            >
              {available.isFetching ? "retrying…" : "retry"}
            </button>
          </p>
        </div>
        {orphans.length > 0 && (
          <p className="hint">
            The catalog still lists {orphans.length} pulled recording
            {orphans.length === 1 ? "" : "s"}, which remain replayable from the ingested tape.
          </p>
        )}
      </>
    );
  }

  return (
    <>
      <h1>Recordings</h1>
      <p>
        <label className="hint">
          system{" "}
          <select value={system} onChange={(e) => setSystem(e.target.value)}>
            <option value="">
              {systems.defaultSystem
                ? `${systems.defaultSystem.name} (default bucket)`
                : "default bucket"}
            </option>
            {systems.selectable
              .filter((s) => !s.is_default)
              .map((s) => (
                <option key={s.name} value={s.name}>
                  {s.name}
                  {s.s3_bucket ? ` (${s.s3_bucket} bucket)` : ""}
                </option>
              ))}
          </select>
        </label>
      </p>
      <p className="hint recintro">
        What is in the bucket. <b>pulled</b> marks the ones the catalog has already ingested —
        which is a record of what has been replayed, not of what exists. Every id here is one
        router process's whole lifetime, so a row spanning several days is several days of that
        pod's traffic under a single name.{" "}
        <b>Rows are clubbed under the deployment day they belong to</b> — one revision's traffic
        for one day, across every pod that served it — because that, and not any single pod's
        slice, is the unit a replay wants. A day becomes replayable as one run once every
        recording in it is sealed.
      </p>

      {rows.length === 0 && orphans.length === 0 ? (
        <p className="hint">
          <b>The bucket holds no recordings.</b> The listing succeeded and found nothing under the
          deployment's recording root — schedule a record run, or run demo/run-deja-demo.sh.
        </p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>recording</th>
              <th>span</th>
              <th>state</th>
              <th className="num">objects</th>
              <th className="num">requests</th>
              <th className="num">events</th>
              <th className="num">size</th>
              <th>coverage</th>
              <th>identity</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {grouped.days.flatMap((day) => [
              <DayBand key={`band-${day.group}`} day={day} cols={COLS} />,
              ...day.members.map(bucketRow),
            ])}
            {/* Sessions the server declined to group: a boot-derived id
                carries no day, so it is shown as itself rather than bundled
                into a day it may not belong to. */}
            {grouped.ungrouped.map(bucketRow)}

            {/* In the catalog, absent from the bucket: ingested earlier and
                since expired or moved out of the recording root. Still
                replayable from the pulled tape, so it is shown rather than
                dropped — but it is not claimed to be in the bucket. */}
            {orphans.map((r) => (
              <tr key={r.recording_id} className="recorphan">
                <td className="mono">{r.recording_id}</td>
                <td className="recspan">
                  <span className="recdash">—</span>
                </td>
                <td>
                  <span className="chip inconclusive" title="in the catalog, not in the bucket">
                    pulled only
                  </span>
                </td>
                <td className="num">
                  <span className="recdash">—</span>
                </td>
                <td className="num">{r.correlation_count?.toLocaleString() ?? "—"}</td>
                <td className="num">{r.event_count?.toLocaleString() ?? "—"}</td>
                <td className="num">
                  {r.byte_size ? `${(r.byte_size / 1048576).toFixed(0)} MB` : "—"}
                </td>
                <td>
                  <CoverageBadges r={r} />
                </td>
                <td>
                  <span className="recdash">—</span>
                </td>
                <td>
                  <Link to={`/?recording=${r.recording_id}`}>replay →</Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {available.data && available.data.total > rows.length && (
        <p className="hint">
          Showing {rows.length} of {available.data.total} in the bucket.
        </p>
      )}
      {recs.error && (
        <p className="hint">
          Catalog unavailable ({String(recs.error)}) — the bucket listing above is complete, but
          nothing can be said about which recordings have been pulled, or about their request and
          event counts.
        </p>
      )}
    </>
  );
}
