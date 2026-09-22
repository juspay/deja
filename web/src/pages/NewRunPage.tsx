import React from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { api, availableRecordings } from "../lib/api";
import { useDebug } from "../lib/debug";
import { RunLaunchModal } from "../components/RunLaunchModal";
import { RecordingPicker, RecordingSummary } from "../components/RecordingPicker";
import { CorrelationPicker } from "../components/CorrelationPicker";
import { spanOf } from "../lib/recordings";
import { useSystems } from "../lib/systems";
import { useCorrelationCandidates } from "../lib/correlations";
import { daySummary, groupRecordings } from "../lib/recordings";

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
  // A DEPLOYMENT DAY, arriving from the recordings page's day band. It names
  // `<revision>-<MMDD>` and the orchestrator resolves it to every recording that
  // revision wrote that day, driving them as one run. Mutually exclusive with a
  // recording id — the orchestrator REFUSES a payload naming both rather than
  // resolving one silently, so exactly one leaves this form.
  const [recordingGroup, setRecordingGroup] = React.useState(params.get("group") ?? "");
  // A recordings-page link from a scoped (per-system bucket) listing arrives
  // with `?system=` + `?s3=` already resolved — the row knows its own source,
  // and retyping either is how the wrong-bucket replay happened.
  // Empty until the registry loads, then the deployment's default. This app no
  // longer names a system: which one is default is the orchestrator's to state,
  // and hard-coding it here made adding a system a change in two repositories.
  const systems = useSystems();
  const [systemUnderTest, setSystemUnderTest] = React.useState(
    params.get("system") ?? "",
  );
  React.useEffect(() => {
    if (!systemUnderTest && systems.defaultSystem) {
      setSystemUnderTest(systems.defaultSystem.name);
    }
  }, [systemUnderTest, systems.defaultSystem]);
  const [imageRef, setImageRef] = React.useState("");
  const [candidateRepo, setCandidateRepo] = React.useState("");
  const [s3Path, setS3Path] = React.useState(params.get("s3") ?? "");
  const [corrs, setCorrs] = React.useState<string[]>([]);
  // How many correlations an UNFILTERED run drives here.
  //
  // Null is "did not choose", which is not the same as choosing whatever number
  // this form is currently showing: the spec then carries no cap at all and the
  // orchestrator applies its own. A deployment that raises its default is
  // therefore not silently overridden by a value this bundle rendered.
  const [maxCorr, setMaxCorr] = React.useState<number | null>(null);
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

  // The days this bucket page holds, newest first, and the one chosen. Derived
  // from the same rows the picker below lists, so the two cannot disagree about
  // what exists.
  const { days } = React.useMemo(() => groupRecordings(rows), [rows]);
  const chosenDay = days.find((d) => d.group === recordingGroup.trim());
  const chosen = rows.find((r) => r.recording_id === recordingId.trim());
  const picked = recordings.data?.find((r) => r.recording_id === recordingId.trim());

  // Latest SEALED preselected, not latest outright. The server returns newest
  // first, so the head of the list is the newest recording — and the newest
  // recording is always UNSEALED, because sealing waits for the pod to stop
  // writing plus a quiescence window plus a sealer tick. Preselecting it left
  // the form in its least useful state: the correlation filter below lists
  // candidates only for a sealed recording, so the default selection could
  // never populate it. On the live catalog the first sealed row sat SIXTEEN
  // rows down, so reaching a usable filter meant scrolling past every
  // recording still being written.
  //
  // Falls back to the head of the list when nothing is sealed, so a fresh
  // bucket still preselects something rather than nothing. No client-side
  // re-sort either way: the order is the server's and a local sort could only
  // disagree with it.
  //
  // Runs once — a refetch must not overwrite a choice the caller has since
  // made, and `?recording=` arrives already set.
  React.useEffect(() => {
    // NOT while a deployment day is chosen. Clearing the recording is how
    // choosing a day makes the two mutually exclusive, and this effect used to
    // undo that on the very next render — which left `recordingId` populated,
    // fetched THAT recording's correlation index, and sent its ids as a
    // `correlation_filter` alongside `recording_group`. The run then drove a
    // hundred correlations out of one arbitrary pod while reporting itself a
    // replay of the whole day, and nothing on the page or in the payload said
    // so.
    if (recordingGroup.trim() || recordingId.trim() || rows.length === 0) return;
    const sealed = rows.find((r) => r.sealed === true && (r.correlations ?? 0) > 0);
    setRecordingId((sealed ?? rows[0]).recording_id);
  }, [rows, recordingId, recordingGroup]);

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
  // The cap in force: the caller's if they named one, else this deployment's
  // own default as the server reported it.
  const cap = maxCorr ?? corrSource.defaultPerRun;
  // Re-sliced here against that cap rather than taken from the hook, so the
  // list the form SHOWS stays the list the run will DRIVE when the cap moves.
  const defaultScope = React.useMemo(
    () => corrSource.candidates.slice(0, cap).map((c) => c.id),
    [corrSource.candidates, cap],
  );
  const scope = usingDefault ? defaultScope : corrs;
  const overCap = corrs.length > cap;

  const triggerSpec = React.useMemo(() => {
    const candidate = imageRef
      ? { kind: "prebuilt_image", image: imageRef }
      : { kind: "prebuilt_image", image: "deja-demo" };
    const spec: Record<string, unknown> = {
      // Required by the wire contract (`RunSpec.mode`, no serde default).
      // Replay is the only mode this dashboard can produce.
      mode: "replay",
      candidate_spec: candidate,
    };
    if (recordingGroup.trim()) {
      // A day, not a pod's slice of one. Sent INSTEAD of `recording_id`: naming
      // both is refused by the orchestrator, which is the behaviour we want —
      // a caller that sent both has not decided, and picking one silently
      // leaves which one won to be discovered from the tape afterwards.
      spec.recording_group = recordingGroup.trim();
    } else {
      // With an S3 source the id is the session filter and may be empty
      // (auto-resolved when the prefix holds exactly one session).
      spec.recording_id = recordingId.trim() || (s3Path ? null : "<recording_id>");
    }
    // Only a non-default system is sent, so existing curl recipes and stored
    // rows keep meaning what they meant. Which name that is comes from the
    // orchestrator rather than from a literal here.
    if (systemUnderTest && !systems.isDefault(systemUnderTest)) {
      spec.system_under_test = systemUnderTest;
    }
    // `scored_span_namespaces` is deliberately NOT set here any more. Which
    // spans a system scores is that system's instrumentation contract, and it
    // used to be sent from this form — so a run started from the API or from a
    // pipeline arrived with an empty list and silently skipped that whole
    // verification tier, for the same system and the same recording. The
    // orchestrator now applies the system's declared namespaces to every run,
    // whoever started it. A run still overrides them by sending its own.
    if (s3Path) spec.s3_source = { path: s3Path };
    if (candidateRepo.trim()) spec.candidate_repo = candidateRepo.trim();
    // The DEFAULT IS SENT, not left implicit, whenever the ids are knowable:
    // then the run's recorded scope is exactly the hundred this form displayed,
    // rather than a hundred the server chose that the page merely described. On
    // an unsealed recording they are not knowable, so no filter goes out and the
    // orchestrator applies the same limit itself.
    // A GROUP DRIVES THE WHOLE DAY. Correlation candidates are read from ONE
    // recording's sealed index, so an id list gathered here names cases from a
    // single member; sending it with a group would scope a day's replay to one
    // pod's requests and call the result a replay of the day. Refused at the
    // payload rather than trusted to be empty, because the picker's default is
    // a non-empty list.
    if (!recordingGroup.trim() && scope.length) spec.correlation_filter = scope;
    // Only when the caller actually chose one — see `maxCorr` above.
    if (maxCorr != null) spec.max_correlations = maxCorr;
    if (expectation) spec.expectation = expectation;
    return spec;
  }, [candidateRepo, expectation, imageRef, maxCorr, recordingGroup, recordingId, s3Path, scope, systemUnderTest]);

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
        {/* CHOOSE A DEPLOYMENT DAY, or one recording below.

            Offered FIRST because it is the better default and the page should
            say so by its order: a recording id is minted once per router
            process, so picking one row picks an arbitrary pod's slice of the
            traffic a deployment served. The day is the whole of it.

            Only fully-sealed days are offered. Resolving a group hands every
            member to one pull and a member without a manifest is compacted
            INLINE inside the run, so a day still being written would make the
            run pay for sealing the sealer has already scheduled. A day that is
            still sealing is listed as unavailable WITH its reason rather than
            hidden, because a day missing from a list reads as a day that does
            not exist. */}
        {days.length > 0 && (
          <div className="dayfield">
            <span className="reclabel">
              deployment day{" "}
              <span className="hint">(one revision's traffic for one day, every pod)</span>
            </span>
            <ul className="daylist">
              {days.map((d) => {
                const chosen = d.group === recordingGroup.trim();
                return (
                  <li key={d.group} className={chosen ? "daypick chosen" : "daypick"}>
                    <button
                      type="button"
                      className="btn"
                      disabled={!d.complete}
                      title={
                        d.complete
                          ? undefined
                          : `${d.unsealed} of this day's recordings are still sealing — replaying it now would re-compact them inside the run`
                      }
                      onClick={() => {
                        // Mutually exclusive on the wire: the orchestrator
                        // refuses a payload naming both, so choosing a day
                        // clears the recording rather than leaving both set.
                        setRecordingGroup(chosen ? "" : d.group);
                        if (!chosen) setRecordingId("");
                      }}
                    >
                      {chosen ? "chosen" : d.complete ? "choose" : "still sealing"}
                    </button>{" "}
                    <b className="mono">{d.group}</b>{" "}
                    <span className="hint">{daySummary(d)}</span>
                  </li>
                );
              })}
            </ul>

            {/* WHAT IS IN THE DAY. The members are listed because "replay a
                day" is otherwise an instruction to trust a name — and the
                list is qualified rather than presented as final, since the
                run resolves the group again when it pulls. A day that has
                sealed more recordings since it was chosen has more members,
                and the run reports the ones it actually drove. */}
            {chosenDay && (
              <details className="daymembers" open>
                <summary>
                  {chosenDay.members.length} recording
                  {chosenDay.members.length === 1 ? "" : "s"} in{" "}
                  <b className="mono">{chosenDay.group}</b> — as the bucket reads right now
                </summary>
                <ul>
                  {chosenDay.members.map((m) => (
                    <li key={m.recording_id}>
                      <span className="mono">{m.recording_id}</span>{" "}
                      <span className="hint">
                        {m.correlations == null
                          ? "not counted until sealed"
                          : `${m.correlations.toLocaleString()} correlation${
                              m.correlations === 1 ? "" : "s"
                            }`}
                        {m.instances?.length ? ` · ${m.instances[0]}` : ""}
                      </span>
                    </li>
                  ))}
                </ul>
                <p className="hint">
                  Resolved again when the run pulls, so this is what the day holds now rather than
                  a promise about what it will drive. A day drives every correlation in it: the
                  correlation picker below applies to a single recording and is deliberately not
                  sent with a day, because the ids it offers come from one member's index and would
                  scope a whole day's replay to that one pod's requests.
                </p>
              </details>
            )}

            {recordingGroup.trim() && (
              <p>
                <button type="button" className="btn" onClick={() => setRecordingGroup("")}>
                  clear the day and pick a single recording
                </button>
              </p>
            )}
          </div>
        )}

        {/* MUTUALLY EXCLUSIVE, so the form offers one at a time. The
            orchestrator refuses a payload naming both a group and a recording,
            and a form showing a chosen day above a chosen recording is showing
            a payload it cannot send — with nothing on screen saying which of
            the two would win. */}
        {recordingGroup.trim() ? (
          <p className="hint recinstead">
            Replaying the whole of <b className="mono">{recordingGroup.trim()}</b>. Clear the day
            above to pick a single recording instead.
          </p>
        ) : (
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
        )}

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
            {systems.selectable.map((sys) => (
              <option key={sys.name} value={sys.name}>
                {sys.name}
                {sys.is_default ? " (default)" : ""}
              </option>
            ))}
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

        <label>
          how many to drive{" "}
          <span className="hint">
            (the ceiling on this run — default {corrSource.defaultPerRun}, at most{" "}
            {corrSource.ceiling} here; leave blank to let the orchestrator apply its own)
          </span>
          <input
            type="number"
            min={1}
            max={corrSource.ceiling}
            step={1}
            placeholder={String(corrSource.defaultPerRun)}
            value={maxCorr ?? ""}
            onChange={(e) => {
              const raw = e.target.value.trim();
              if (!raw) return setMaxCorr(null);
              const n = Number.parseInt(raw, 10);
              // A blank or unparseable box is "did not choose", never zero: zero
              // is refused by the server and would read here as "drive nothing".
              setMaxCorr(Number.isFinite(n) && n > 0 ? n : null);
            }}
          />
        </label>

        {maxCorr != null && maxCorr > corrSource.ceiling && (
          <div className="scopewarn">
            <p>
              <b>
                {maxCorr.toLocaleString()} is above this deployment's ceiling of{" "}
                {corrSource.ceiling.toLocaleString()}.
              </b>{" "}
              The run will be refused rather than trimmed — a run that drove fewer cases than it
              was asked for would score the ones it skipped as though they had passed. Raise
              <code> DEJA_MAX_CORRELATIONS_PER_RUN </code> on the orchestrator, or split the work
              across runs.
            </p>
          </div>
        )}

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
            cap={cap}
            value={corrs}
            onChange={setCorrs}
          />
        </div>

        {/* THE DEFAULT, STATED BEFORE IT IS USED. Nothing selected does not mean
            "the whole session" — the live recordings hold 42,310 and 170,568
            correlations, and the spec has no limit field, so an unfiltered run
            would drive every one of them. It means the first `cap` — this
            deployment's default unless the caller raised it above.

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
                {scope.length ? scope.length.toLocaleString() : cap} correlation
                {(scope.length || cap) === 1 ? "" : "s"}
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

            {defaultScope.length === 0 ? (
              <p className="hint">
                Which {cap} cannot be named here: this recording's index is not readable
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
              {corrs.length.toLocaleString()} correlations selected, over this run's limit of{" "}
              {cap}.
            </b>{" "}
            Nothing will be sent until the selection fits — it is not trimmed to the first{" "}
            {cap}, because a run that drove {cap} of {corrs.length.toLocaleString()} would still
            score as if it had driven them all. Raise “how many to drive” above (up to{" "}
            {corrSource.ceiling}) to keep this selection.
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
          // A DAY IS A NAMED RECORDING TOO. The gate asks whether this run
          // names something to drive; a group names a whole day of it, so
          // requiring `recordingId` specifically left the button dead for
          // every group — the one path the day picker above exists to offer.
          disabled={
            create.isPending ||
            (!recordingGroup.trim() && !recordingId.trim() && !s3Path) ||
            overCap
          }
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
