import React from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { api } from "../lib/api";
import { useDebug } from "../lib/debug";
import { RunLaunchModal } from "../components/RunLaunchModal";

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

  const recordings = useQuery({ queryKey: ["recordings"], queryFn: api.recordings });
  const picked = recordings.data?.find((r) => r.recording_id === recordingId.trim());

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
            <label>
              recording S3 path{" "}
              <span className="hint">
                (a deployed bucket/prefix; empty = pick from the catalog below)
              </span>
              <input
                type="text"
                placeholder="s3://hyperswitch-art/2026/07/11"
                value={s3Path}
                onChange={(e) => setS3Path(e.target.value)}
              />
            </label>

            {s3Path ? (
              <label>
                recording session id{" "}
                <span className="hint">
                  (optional — auto-resolved when the path holds exactly one session)
                </span>
                <input
                  type="text"
                  placeholder="sandbox-art-2026-07-11"
                  value={recordingId}
                  onChange={(e) => setRecordingId(e.target.value)}
                />
              </label>
            ) : (
              <label>
                recording
                <select value={recordingId} onChange={(e) => setRecordingId(e.target.value)}>
                  <option value="">— pick a recording —</option>
                  {recordings.data?.map((r) => (
                    <option key={r.recording_id} value={r.recording_id}>
                      {r.recording_id} ({(r.correlation_count ?? 0).toLocaleString()} requests)
                    </option>
                  ))}
                </select>
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
                recordings hold 42,310 and 170,568 correlations. Say the number. */}
            {wholeSession && (
              <p className="scopewarn">
                <b>No scope set — this drives the entire session.</b>{" "}
                {picked?.correlation_count
                  ? `${picked.recording_id} holds ${picked.correlation_count.toLocaleString()} correlations; every one of them will be driven and scored.`
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
