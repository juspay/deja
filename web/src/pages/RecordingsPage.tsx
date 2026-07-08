import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, RecordingRow } from "../lib/api";

/* The seal/coverage badges read the compactor's manifest: a sealed session
   with zero per-instance gseq gaps is replay-grade; gaps or no manifest mean
   the recording may be partial. */
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

export default function RecordingsPage() {
  const recs = useQuery({ queryKey: ["recordings"], queryFn: api.recordings });

  if (recs.isLoading) return <p className="hint">loading…</p>;
  if (recs.error) return <p className="err">{String(recs.error)}</p>;

  return (
    <>
      <h1>Recordings</h1>
      <table>
        <thead>
          <tr>
            <th>recording</th>
            <th>kind</th>
            <th>coverage</th>
            <th>events</th>
            <th>requests</th>
            <th>size</th>
            <th>by</th>
            <th>created</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {recs.data?.map((r) => (
            <tr key={r.recording_id}>
              <td>{r.recording_id}</td>
              <td>{r.kind}</td>
              <td>
                <CoverageBadges r={r} />
              </td>
              <td>{r.event_count?.toLocaleString() ?? "—"}</td>
              <td>{r.correlation_count?.toLocaleString() ?? "—"}</td>
              <td>{r.byte_size ? `${(r.byte_size / 1024).toFixed(0)} KB` : "—"}</td>
              <td>{r.created_by}</td>
              <td>{new Date(r.created_at).toLocaleString()}</td>
              <td>
                <Link to={`/replays/new?recording=${r.recording_id}`}>replay →</Link>
              </td>
            </tr>
          ))}
          {recs.data?.length === 0 && (
            <tr>
              <td colSpan={9} className="hint">
                no recordings yet — schedule a record run or run demo/run-deja-demo.sh
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </>
  );
}
