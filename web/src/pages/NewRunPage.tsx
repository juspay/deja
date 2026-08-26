import React from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { api, availableRecordings } from "../lib/api";
import { useDebug } from "../lib/debug";
import { RunLaunchModal } from "../components/RunLaunchModal";
import { RecordingPicker, RecordingSummary } from "../components/RecordingPicker";
import { CorrelationPicker } from "../components/CorrelationPicker";
import { spanOf } from "../lib/recordings";
import { CORRELATION_CAP, useCorrelationCandidates } from "../lib/correlations";

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
 *
 * Gone with this pass: the mode selector. Recording happens on live router
 * pods; this dashboard cannot start one, and the `record` option scheduled a
 * run that could only fail here. `mode: "replay"` is still sent because
 * `RunSpec.mode` has no serde default and the request is rejected without it —
 * it is a wire constant now, not a question put to the caller.
 */
export default function NewRunPage() {
  const [params] = useSearchParams();
  const debug = useDebug();
  const [recordingId, setRecordingId] = React.useState(params.get("recording") ?? "");
  const [systemUnderTest, setSystemUnderTest] = React.useState("hyperswitch");
  const [imageRef, setImageRef] = React.useState("");
  const [candidateRepo, setCandidateRepo] = React.useState("");
  const [s3Path, setS3Path] = React.useState("");
  const [corrs, setCorrs] = React.useState<string[]>([]);
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

  // THE ONE PLACE candidate correlations come from. Swapping in the sealed
  // correlations index later is a change to that hook and to nothing here.
  const corrSource = useCorrelationCandidates(recordingId);

  // Changing the recording invalidates the selection: an id from one recording
  // names nothing in another, and carrying it over would scope a run to a case
  // that cannot exist in it.
  const lastRecording = React.useRef(recordingId);
  React.useEffect(() => {
    if (lastRecording.current === recordingId) return;
    lastRecording.current = recordingId;
    setCorrs([]);
  }, [recordingId]);

  // WHAT WILL ACTUALLY BE DRIVEN. An explicit selection when there is one, the
  // first CORRELATION_CAP of the recording otherwise. The two are held apart —
  // `corrs` is only ever what the caller picked — so "nothing picked" never gets
  // mistaken for "picked and trimmed", which are different situations with
  // different rules.
  const usingDefault = corrs.length === 0;
  const scope = usingDefault ? corrSource.defaultScope : corrs;
  const overCap = corrs.length > CORRELATION_CAP;

  const triggerSpec = React.useMemo(() => {
    const candidate = imageRef
      ? { kind: "prebuilt_image", image: imageRef }
      : { kind: "prebuilt_image", image: "deja-demo" };
    const spec: Record<string, unknown> = {
      // Required by the wire contract (`RunSpec.mode`, no serde default).
      // Replay is the only mode this dashboard can produce.
      mode: "replay",
      candidate_spec: candidate,
      // With an S3 source the id is the session filter and may be empty
      // (auto-resolved when the prefix holds exactly one session).
      recording_id: recordingId.trim() || (s3Path ? null : "<recording_id>"),
    };
    // hyperswitch is the wire default; only a non-default system is sent, so
    // existing curl recipes and stored rows keep meaning what they meant.
    if (systemUnderTest !== "hyperswitch") spec.system_under_test = systemUnderTest;
    // Prism declares its instrumentation contract: every recorded ucs::* /
    // connector::* span must replay, with equal field values (the span-shape
    // check). Sent by default because a prism run without it silently skips
    // that verification tier.
    if (systemUnderTest === "prism") spec.scored_span_namespaces = ["ucs::", "connector::"];
    if (s3Path) spec.s3_source = { path: s3Path };
    if (candidateRepo.trim()) spec.candidate_repo = candidateRepo.trim();
    // The DEFAULT IS SENT, not left implicit, whenever the ids are knowable:
    // then the run's recorded scope is exactly the hundred this form displayed,
    // rather than a hundred the server chose that the page merely described. On
    // an unsealed recording they are not knowable, so no filter goes out and the
    // orchestrator applies the same limit itself.
    if (scope.length) spec.correlation_filter = scope;
    if (expectation) spec.expectation = expectation;
    return spec;
  }, [candidateRepo, expectation, imageRef, recordingId, s3Path, scope, systemUnderTest]);

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
          system under test{" "}
          <span className="hint">
            (which recorded system this run replays — selects the candidate's
            env-binding profile; recordings from either system replay under the
            same harness)
          </span>
          <select
            value={systemUnderTest}
            onChange={(e) => setSystemUnderTest(e.target.value)}
          >
            <option value="hyperswitch">hyperswitch</option>
            <option value="prism">prism (UCS)</option>
          </select>
        </label>

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

        <div className="recfield">
          <span className="reclabel">
            correlations to drive{" "}
            <span className="hint">
              (each recorded request is one independent test case; the verdict judges only the
              driven subset)
            </span>
          </span>
          <CorrelationPicker
            source={corrSource}
            cap={CORRELATION_CAP}
            value={corrs}
            onChange={setCorrs}
          />
        </div>

        {/* THE DEFAULT, STATED BEFORE IT IS USED. Nothing selected does not mean
            "the whole session" — the live recordings hold 42,310 and 170,568
            correlations, and the spec has no limit field, so an unfiltered run
            would drive every one of them. It means the first CORRELATION_CAP.

            "First" is worth a sentence rather than a term: correlation ids sort
            by when the request arrived, so the head of the index is the opening
            stretch of the recording. That is a window a reader can picture, and
            it is why the default is defensible where "some hundred" would not
            be. The ids themselves are one disclosure away, because a scope
            nobody can inspect is a scope nobody can check. */}
        {usingDefault && (
          <div className="scopewarn scopedefault">
            <p>
              <b>
                Nothing selected — this run will drive the first{" "}
                {scope.length ? scope.length.toLocaleString() : CORRELATION_CAP} correlation
                {(scope.length || CORRELATION_CAP) === 1 ? "" : "s"}
                {corrSource.total != null ? ` of ${corrSource.total.toLocaleString()}` : ""}.
              </b>{" "}
              Correlations are ordered by when the request arrived, so these are the earliest
              requests in the recording — its opening minutes — not a hundred picked at random.
              {picked?.correlation_count && corrSource.total == null
                ? ` ${picked.recording_id} holds ${picked.correlation_count.toLocaleString()} in all.`
                : ""}
              {chosen && spanOf(chosen.dates).multiDay && corrSource.total == null
                ? ` This recording is ${spanOf(chosen.dates).partitions} days of one pod's traffic, so the rest of it is not touched.`
                : ""}{" "}
              Pick specific correlations above to replace this.
            </p>

            {corrSource.defaultIsServerSide ? (
              <p className="hint">
                Which {CORRELATION_CAP} cannot be named here: this recording's index is not readable
                yet, so the request goes out without a filter and the orchestrator applies the limit
                itself.
              </p>
            ) : (
              <>
                <details className="scopeids">
                  <summary>
                    show the {scope.length.toLocaleString()} that will run — {scope[0]} …{" "}
                    {scope[scope.length - 1]}
                  </summary>
                  <ol className="scopeidlist">
                    {scope.map((id) => (
                      <li key={id} className="mono">
                        {id}
                      </li>
                    ))}
                  </ol>
                </details>
                <button
                  type="button"
                  className="btn"
                  onClick={() => setCorrs(corrSource.defaultScope)}
                  title="turns the default into an ordinary selection you can add to and remove from"
                >
                  start from these {scope.length.toLocaleString()}
                </button>
              </>
            )}
          </div>
        )}
        {overCap && (
          <p className="scopewarn">
            <b>
              {corrs.length.toLocaleString()} correlations selected, over the limit of{" "}
              {CORRELATION_CAP}.
            </b>{" "}
            Nothing will be sent until the selection fits — it is not trimmed to the first{" "}
            {CORRELATION_CAP}, because a run that drove {CORRELATION_CAP} of{" "}
            {corrs.length.toLocaleString()} would still score as if it had driven them all.
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

        {/* Only an EXPLICIT over-cap selection blocks a launch. Selecting
            nothing does not: it takes the default. */}
        <button
          className="btn primary"
          disabled={create.isPending || (!recordingId.trim() && !s3Path) || overCap}
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
