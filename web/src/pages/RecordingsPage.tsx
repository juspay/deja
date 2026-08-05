import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, availableRecordings, AvailableRecording, RecordingRow } from "../lib/api";
import { catalogById, identityText, spanOf } from "../lib/recordings";

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
  const text = identityText(rec.identity);
  // No identity means the id predates ids naming a revision. That is the
  // ordinary past — the facts still live in the recording's envelopes — so it
  // reads as a plain dash, not as a missing-data warning.
  if (!text) return <span className="recdash">—</span>;
  return <span className="recident">{text}</span>;
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
  const available = useQuery({
    queryKey: ["recordings-available"],
    queryFn: () => availableRecordings(),
  });
  const recs = useQuery({ queryKey: ["recordings"], queryFn: api.recordings });

  const rows = available.data?.recordings ?? [];
  const byId = catalogById(recs.data);
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
      <p className="hint recintro">
        What is in the bucket. <b>pulled</b> marks the ones the catalog has already ingested —
        which is a record of what has been replayed, not of what exists. Every id here is one
        router process's whole lifetime, so a row spanning several days is several days of that
        pod's traffic under a single name.
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
            {rows.map((r) => {
              const cat = byId.get(r.recording_id);
              return (
                <tr key={r.recording_id}>
                  <td className="mono">{r.recording_id}</td>
                  <td className="recspan">
                    <Span dates={r.dates} />
                  </td>
                  <td>
                    <span className={r.pulled ? "chip pass" : "chip muted"}>
                      {r.pulled ? "pulled" : "in bucket"}
                    </span>
                  </td>
                  <td className="num">{r.objects.toLocaleString()}</td>
                  {/* Unknown until ingest, and left unknown rather than
                      approximated from the object count. */}
                  <td className="num">{cat?.correlation_count?.toLocaleString() ?? "—"}</td>
                  <td className="num">{cat?.event_count?.toLocaleString() ?? "—"}</td>
                  <td className="num">
                    {cat?.byte_size ? `${(cat.byte_size / 1048576).toFixed(0)} MB` : "—"}
                  </td>
                  <td>{cat ? <CoverageBadges r={cat} /> : <span className="recdash">—</span>}</td>
                  <td>
                    <Identity rec={r} />
                  </td>
                  <td>
                    <Link to={`/?recording=${r.recording_id}`}>replay →</Link>
                  </td>
                </tr>
              );
            })}

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
