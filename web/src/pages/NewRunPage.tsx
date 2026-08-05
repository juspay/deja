import React from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { api, availableRecordings } from "../lib/api";
import { useDebug } from "../lib/debug";
import { RunLaunchModal } from "../components/RunLaunchModal";
import { RecordingPicker, RecordingSummary } from "../components/RecordingPicker";
import { spanOf } from "../lib/recordings";

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

function CopyButton({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = React.useState(false);
  return (
    <button
      type="button"
      className="btn"
      onClick={() => {
        void navigator.clipboard.writeText(text).then(() => {
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1500);
        });
      }}
    >
      {copied ? "copied" : label}
    </button>
  );
}

/**
 * HOME. The form, and nothing else.
 *
 * Gone with this rewrite: the build-command block (a shell recipe for building a
 * router binary in a vendored tree, on the page whose job is to launch a run),
 * the cross-version scenario selector that drove only that recipe's file name,
 * and the candidate binary path — `local_path` is 0 of 29 live runs, and the
 * field's presence made the real input (an image ref) look optional.
 *
 * Gone with the picker: the two path fields this form used to open with — an
 * S3 path and a session id — which asked the caller to know a bucket layout in
 * order to name something the orchestrator can resolve from its own
 * environment. Every one of the 33 live runs was launched with a bare
 * `recording_id` and `s3_source: null`, so the fields were load-bearing for
 * nobody and confusing for everybody. The explicit prefix survives behind
 * `?debug=1`, because `s3_source` remains a supported spec field.
 */
export default function NewRunPage() {
  const [params] = useSearchParams();
  const debug = useDebug();
  const [mode, setMode] = React.useState<"record" | "replay">("replay");
  const [recordingId, setRecordingId] = React.useState(params.get("recording") ?? "");
  const [imageRef, setImageRef] = React.useState("");
  const [candidateRepo, setCandidateRepo] = React.useState("");
  const [s3Path, setS3Path] = React.useState("");
  const [corrFilter, setCorrFilter] = React.useState("");
  const [iterations, setIterations] = React.useState(1);
  const [expectation, setExpectation] = React.useState("");
  const [launched, setLaunched] = React.useState<string | null>(null);

  // What EXISTS (the bucket) and what has been PULLED (the catalog). The picker
  // offers the first; the second is joined on for the counts only it knows.
  const available = useQuery({
    queryKey: ["recordings-available"],
    queryFn: () => availableRecordings(),
  });
  const recordings = useQuery({ queryKey: ["recordings"], queryFn: api.recordings });

  // Memoized so the preselect effect below depends on a stable array rather
  // than a fresh `[]` every render.
  const rows = React.useMemo(() => available.data?.recordings ?? [], [available.data]);
  const chosen = rows.find((r) => r.recording_id === recordingId.trim());
  const picked = recordings.data?.find((r) => r.recording_id === recordingId.trim());

  // Latest preselected. The server returns newest first (last partition, then
  // id), so the head of the list IS the latest — no client-side re-sort, which
  // could only disagree with it. Runs once: a refetch must not overwrite a
  // choice the caller has since made, and `?recording=` arrives already set.
  React.useEffect(() => {
    if (recordingId.trim() || rows.length === 0) return;
    setRecordingId(rows[0].recording_id);
  }, [rows, recordingId]);

  const corrs = React.useMemo(
    () => corrFilter.split(",").map((s) => s.trim()).filter(Boolean),
    [corrFilter],
  );

  const triggerSpec = React.useMemo(() => {
    const candidate = imageRef
      ? { kind: "prebuilt_image", image: imageRef }
      : { kind: "prebuilt_image", image: "deja-demo" };
    const spec: Record<string, unknown> =
      mode === "record"
        ? { mode, candidate_spec: candidate, recording_id: null, workload: { iterations } }
        : {
            mode,
            candidate_spec: candidate,
            // With an S3 source the id is the session filter and may be empty
            // (auto-resolved when the prefix holds exactly one session).
            recording_id: recordingId.trim() || (s3Path ? null : "<recording_id>"),
          };
    if (mode === "replay" && s3Path) spec.s3_source = { path: s3Path };
    if (candidateRepo.trim()) spec.candidate_repo = candidateRepo.trim();
    if (mode === "replay" && corrs.length) spec.correlation_filter = corrs;
    if (expectation) spec.expectation = expectation;
    return spec;
  }, [candidateRepo, corrs, expectation, imageRef, iterations, mode, recordingId, s3Path]);

  const curlCommand = React.useMemo(
    () =>
      [
        `curl -sS -X POST ${window.location.origin}/api/v1/runs`,
        "  -H 'content-type: application/json'",
        `  -H ${shellQuote("X-Deja-Actor: user:<name>")}`,
        `  --data ${shellQuote(JSON.stringify(triggerSpec))}`,
      ].join(" \\\n"),
    [triggerSpec],
  );

  const create = useMutation({
    mutationFn: () => api.createRun(triggerSpec),
    onSuccess: (resp) => setLaunched(resp.run_id),
  });

  const wholeSession = mode === "replay" && corrs.length === 0;

  return (
    <>
      <h1>New run</h1>

      <form
        className="runform"
        onSubmit={(e) => {
          e.preventDefault();
          create.mutate();
        }}
      >
        <label>
          mode
          <select value={mode} onChange={(e) => setMode(e.target.value as "record" | "replay")}>
            <option value="record">record — drive the workload, produce a recording</option>
            <option value="replay">replay — drive a recording against a candidate</option>
          </select>
        </label>

        {mode === "record" && (
          <label>
            workload iterations
            <input
              type="number"
              min={1}
              value={iterations}
              onChange={(e) => setIterations(Number(e.target.value))}
            />
          </label>
        )}

        {mode === "replay" && (
          <>
            <div className="recfield">
              <span className="reclabel">
                recording{" "}
                <span className="hint">(what is in the bucket — newest first)</span>
              </span>

              {available.isLoading && (
                <p className="hint">listing the bucket… (this reads S3 and takes a moment)</p>
              )}

              {/* A FAILURE TO LOOK IS NOT AN ABSENCE. This endpoint lists S3 and
                  answers 502 when it cannot; rendering that as an empty picker
                  would read as "no recordings exist" and is the one outcome
                  that must never happen here. It also must not block a caller
                  who knows the id, so the fallback is a plain field. */}
              {available.error && (
                <div className="recfail">
                  <p className="err">
                    <b>Could not list the bucket.</b> {String(available.error)}
                  </p>
                  <p className="hint">
                    This is a failure to look, not an empty bucket — recordings may well exist.
                    Retry, or name one directly if you already know its id.
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
                  <input
                    type="text"
                    placeholder="run-1785331134782268537"
                    value={recordingId}
                    onChange={(e) => setRecordingId(e.target.value)}
                  />
                </div>
              )}

              {available.isSuccess && rows.length === 0 && (
                <div className="recfail">
                  <p className="hint">
                    <b>The bucket holds no recordings.</b> The listing succeeded and found nothing
                    under the deployment's recording root — nothing has landed yet. Schedule a
                    record run, or point the orchestrator at the bucket that has them.
                  </p>
                </div>
              )}

              {available.isSuccess && rows.length > 0 && (
                <>
                  <RecordingPicker
                    recordings={rows}
                    catalog={recordings.data}
                    value={recordingId}
                    onChange={setRecordingId}
                    truncated={
                      available.data && available.data.total > rows.length
                        ? available.data.total
                        : 0
                    }
                  />
                  <RecordingSummary rec={chosen} catalog={picked} />
                  {/* The catalog is a nicety here (it supplies correlation
                      counts for pulled recordings); its failure must not
                      degrade the picker, so it is reported quietly. */}
                  {recordings.error && (
                    <p className="hint">
                      catalog unavailable ({String(recordings.error)}) — correlation counts are not
                      shown for recordings that have already been pulled.
                    </p>
                  )}
                </>
              )}
            </div>

            {/* ESCAPE HATCH. `s3_source` is still a supported spec field — an
                arbitrary bucket/prefix in the deployed aggregator layout — and
                is the only way to reach a recording the index cannot name. It
                is not on the default form because supplying it is knowing a
                deployment's bucket layout by hand. */}
            {debug && (
              <label>
                s3 source override <span className="hint">(?debug=1)</span>
                <span className="hint">
                  A full <code>bucket/prefix</code>. Set it and the recording above becomes the
                  session FILTER, which may be left empty when the prefix holds exactly one
                  session. The index reports prefixes without a bucket
                  {chosen ? (
                    <>
                      {" "}
                      — the selected one is <code className="mono">{chosen.prefix}</code>, so a
                      path here is <code className="mono">s3://&lt;bucket&gt;/{chosen.prefix}</code>
                    </>
                  ) : null}
                  .
                </span>
                <input
                  type="text"
                  placeholder="s3://hyperswitch-art/landing/v1/dt=2026-08-04/session=run-…"
                  value={s3Path}
                  onChange={(e) => setS3Path(e.target.value)}
                />
              </label>
            )}

            <label>
              candidate image{" "}
              <span className="hint">(a deployed image ref, e.g. the ECR build)</span>
              <input
                type="text"
                placeholder="223655089699.dkr.ecr.ap-south-1.amazonaws.com/hyperswitch-router:<tag>"
                value={imageRef}
                onChange={(e) => setImageRef(e.target.value)}
              />
            </label>

            <label>
              candidate repo{" "}
              <span className="hint">
                (optional, owner/name — the image's source repo, used to fetch its migrations for
                the schema gate; empty = the server default)
              </span>
              <input
                type="text"
                placeholder="juspay/hyperswitch"
                value={candidateRepo}
                onChange={(e) => setCandidateRepo(e.target.value)}
              />
            </label>

            <label>
              correlation filter{" "}
              <span className="hint">
                (comma-separated correlation ids — the test cases to drive; the verdict judges only
                the driven subset)
              </span>
              <input
                type="text"
                placeholder="019fae07-b8d4-7080-aa62-9ecc4f816dd8, …"
                value={corrFilter}
                onChange={(e) => setCorrFilter(e.target.value)}
              />
            </label>

            {/* SCOPE. Leaving the filter empty is not "a small default" — the live
                recordings hold 42,310 and 170,568 correlations. Say the number.
                When the catalog has no count (the recording has never been
                pulled) the number is genuinely unknown; the span is stated
                instead, and no count is estimated from the object count —
                22,936 objects hold 170,568 correlations and 1,680 hold 42,310,
                so any such estimate would be invention. */}
            {wholeSession && (
              <p className="scopewarn">
                <b>No scope set — this drives the entire session.</b>{" "}
                {picked?.correlation_count
                  ? `${picked.recording_id} holds ${picked.correlation_count.toLocaleString()} correlations; every one of them will be driven and scored.`
                  : chosen && spanOf(chosen.dates).multiDay
                    ? `${chosen.recording_id} is ${spanOf(chosen.dates).partitions} days of one pod's traffic and has never been pulled, so its correlation count is not yet known — it will be whatever those days recorded, and every one will be driven and scored.`
                    : "A session recording can hold tens of thousands of correlations; every one will be driven and scored."}{" "}
                List the correlation ids you actually want to test.
              </p>
            )}
            {!wholeSession && (
              <p className="hint">
                {corrs.length} test case{corrs.length === 1 ? "" : "s"} will be driven
                {picked?.correlation_count
                  ? ` — of ${picked.correlation_count.toLocaleString()} in the recording`
                  : ""}
                . Everything outside this list is excluded from the verdict, not counted omitted.
              </p>
            )}

            <label>
              expectation <span className="hint">(a note for the audit trail: pass / diverge)</span>
              <input
                type="text"
                placeholder="pass"
                value={expectation}
                onChange={(e) => setExpectation(e.target.value)}
              />
            </label>
          </>
        )}

        <button
          className="btn primary"
          disabled={create.isPending || (mode === "replay" && !recordingId.trim() && !s3Path)}
        >
          {create.isPending ? "scheduling…" : "schedule run"}
        </button>
        {create.error && <p className="err">{String(create.error)}</p>}
      </form>

      {debug && (
        <>
          <h2>Trigger curl <span className="hint">(?debug=1)</span></h2>
          <div className="copyhead">
            <span className="hint">Uses the current form values.</span>
            <CopyButton text={curlCommand} label="copy curl" />
          </div>
          <pre className="cmd">{curlCommand}</pre>
        </>
      )}

      <RunLaunchModal runId={launched} onClose={() => setLaunched(null)} />
    </>
  );
}
