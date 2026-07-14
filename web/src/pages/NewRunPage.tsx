import React from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useNavigate, useSearchParams } from "react-router-dom";
import { api } from "../lib/api";

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

function CopyButton({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = React.useState(false);
  return (
    <button
      type="button"
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

function recordingIdFromUri(uri: string): string {
  const clean = uri.trim().replace(/\/+$/, "");
  if (!clean) return "";
  const last = clean.split("/").filter(Boolean).pop() ?? "";
  return last
    .replace(/\.log\.gz$/i, "")
    .replace(/\.jsonl\.gz$/i, "")
    .replace(/\.jsonl$/i, "")
    .replace(/\.gz$/i, "");
}

type RefType = "branch" | "commit" | "tag" | "image";

const REF_HINT: Record<RefType, string> = {
  branch: "CI must have pushed the router image for this branch to ECR; migrations + superposition seed run from the branch",
  commit: "full or short commit SHA; migrations are pinned to the same commit",
  tag: "release tag (e.g. v1.121.0); migrations are pinned to the same tag",
  image: "full image reference, e.g. 123.dkr.ecr.us-east-1.amazonaws.com/router:abc1234",
};

export default function NewRunPage() {
  const nav = useNavigate();
  const [params] = useSearchParams();
  const [mode, setMode] = React.useState<"record" | "replay">("replay");
  const [recordingId, setRecordingId] = React.useState(params.get("recording") ?? "");
  const [recordingUri, setRecordingUri] = React.useState(
    params.get("recording_uri") ?? params.get("uri") ?? "",
  );
  const [refType, setRefType] = React.useState<RefType>("commit");
  const [refValue, setRefValue] = React.useState("");
  const [repo, setRepo] = React.useState("juspay/hyperswitch");
  const [postgresTag, setPostgresTag] = React.useState("17-alpine");
  const [redisTag, setRedisTag] = React.useState("7.2-alpine");
  const [superpositionTag, setSuperpositionTag] = React.useState("0.112.0");
  const [iterations, setIterations] = React.useState(1);
  const [expectation, setExpectation] = React.useState("");
  const effectiveRecordingId = React.useMemo(
    () => recordingId.trim() || recordingIdFromUri(recordingUri),
    [recordingId, recordingUri],
  );

  const triggerSpec = React.useMemo(() => {
    const candidate =
      mode === "record"
        ? { kind: "prebuilt_image", image: "deja-demo" }
        : refType === "image"
          ? { kind: "prebuilt_image", image: refValue || "<image>" }
          : refType === "branch"
            ? { kind: "repo_branch", repo, branch: refValue || "<branch>" }
            : refType === "commit"
              ? { kind: "repo_sha", repo, sha: refValue || "<sha>" }
              : { kind: "repo_tag", repo, tag: refValue || "<tag>" };
    const runtimeVersions = {
      postgres: postgresTag.trim(),
      redis: redisTag.trim(),
      superposition: superpositionTag.trim(),
    };
    const spec: Record<string, unknown> =
      mode === "record"
        ? { mode, candidate_spec: candidate, recording_id: null, workload: { iterations } }
        : {
            mode,
            candidate_spec: candidate,
            recording_id: effectiveRecordingId || "<recording_id>",
            runtime_versions: Object.fromEntries(
              Object.entries(runtimeVersions).filter(([, value]) => value),
            ),
          };
    if (mode === "replay" && recordingUri.trim()) {
      spec.recording_uri = recordingUri.trim();
    }
    if (expectation) spec.expectation = expectation;
    return spec;
  }, [
    effectiveRecordingId,
    expectation,
    iterations,
    mode,
    postgresTag,
    recordingUri,
    redisTag,
    refType,
    refValue,
    repo,
    superpositionTag,
  ]);

  const curlCommand = React.useMemo(() => {
    const body = JSON.stringify(triggerSpec);
    return [
      `curl -sS -X POST ${window.location.origin}/api/v1/runs`,
      "  -H 'content-type: application/json'",
      `  -H ${shellQuote("X-Deja-Actor: user:<name>")}`,
      `  -H ${shellQuote("Authorization: Bearer <service-token>")}`,
      `  --data ${shellQuote(body)}`,
    ].join(" \\\n");
  }, [triggerSpec]);

  const prSnippet = React.useMemo(
    () =>
      [
        "### Deja replay request",
        "",
        `- Recording: \`${effectiveRecordingId || "<recording_id>"}\``,
        `- Recording URI: \`${recordingUri || "<catalog/S3 source>"}\``,
        `- Candidate ${refType}: \`${refValue || "<ref>"}\``,
        `- Postgres: \`${postgresTag || "<postgres_tag>"}\``,
        `- Redis: \`${redisTag || "<redis_tag>"}\``,
        `- Superposition: \`${superpositionTag || "<superposition_tag>"}\``,
        `- Expected result: \`${expectation || "pass/diverge"}\``,
        "",
        "After it finishes, update this comment with:",
        `- Run: ${window.location.origin}/runs/<run_id>`,
        `- Scorecard: ${window.location.origin}/runs/<run_id>/scorecard`,
      ].join("\n"),
    [
      effectiveRecordingId,
      expectation,
      postgresTag,
      recordingUri,
      redisTag,
      refType,
      refValue,
      superpositionTag,
    ],
  );
  const recordings = useQuery({ queryKey: ["recordings"], queryFn: api.recordings });

  const create = useMutation({
    mutationFn: () => api.createRun(triggerSpec),
    onSuccess: (resp) => nav(`/runs/${resp.run_id}`),
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
              recording id{" "}
              <span className="hint">
                (logical label; auto-derived from the S3 URI if left empty)
              </span>
              <input
                list="recording-options"
                type="text"
                placeholder="1783608630-c0c03516-30ea-47aa-88ca-8f3106aaf25d"
                value={recordingId}
                onChange={(e) => setRecordingId(e.target.value)}
              />
              <datalist id="recording-options">
                {recordings.data?.map((r) => (
                  <option key={r.recording_id} value={r.recording_id}>
                    {r.recording_id} ({r.event_count ?? "?"} events)
                  </option>
                ))}
              </datalist>
            </label>
            <label>
              S3 recording URI or prefix{" "}
              <span className="hint">
                (object or prefix; the agent merges all matching files before replay)
              </span>
              <input
                type="text"
                placeholder="s3://hyperswitch-art/2026/07/09/1783608630-c0c03516-30ea-47aa-88ca-8f3106aaf25d.log.gz"
                value={recordingUri}
                onChange={(e) => setRecordingUri(e.target.value)}
              />
            </label>
            <label>
              candidate ref type
              <select value={refType} onChange={(e) => setRefType(e.target.value as RefType)}>
                <option value="branch">branch</option>
                <option value="commit">commit</option>
                <option value="tag">tag</option>
                <option value="image">image (direct reference)</option>
              </select>
            </label>
            <label>
              {refType === "image" ? "candidate image" : `candidate ${refType}`}{" "}
              <span className="hint">({REF_HINT[refType]})</span>
              <input
                type="text"
                placeholder={
                  refType === "branch"
                    ? "feature/my-change"
                    : refType === "commit"
                      ? "abc1234"
                      : refType === "tag"
                        ? "v1.121.0"
                        : "…amazonaws.com/router:abc1234"
                }
                value={refValue}
                onChange={(e) => setRefValue(e.target.value)}
              />
            </label>
            {refType !== "image" && (
              <label>
                source repository{" "}
                <span className="hint">(migrations + superposition seed are fetched from it)</span>
                <input type="text" value={repo} onChange={(e) => setRepo(e.target.value)} />
              </label>
            )}
            <label>
              Postgres tag <span className="hint">(Docker Hub tag)</span>
              <input
                type="text"
                placeholder="17-alpine"
                value={postgresTag}
                onChange={(e) => setPostgresTag(e.target.value)}
              />
            </label>
            <label>
              Redis tag <span className="hint">(Docker Hub tag)</span>
              <input
                type="text"
                placeholder="7.2-alpine"
                value={redisTag}
                onChange={(e) => setRedisTag(e.target.value)}
              />
            </label>
            <label>
              Superposition tag <span className="hint">(GHCR tag under ghcr.io/juspay/superposition)</span>
              <input
                type="text"
                placeholder="0.112.0"
                value={superpositionTag}
                onChange={(e) => setSuperpositionTag(e.target.value)}
              />
            </label>
            <label>
              expectation <span className="hint">(note for the audit trail: pass / diverge)</span>
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
          disabled={
            create.isPending ||
            (mode === "replay" && (!refValue.trim() || (!recordingId.trim() && !recordingUri.trim())))
          }
        >
          {create.isPending ? "scheduling…" : "schedule run"}
        </button>
        {create.error && <p className="err">{String(create.error)}</p>}
      </form>

      {mode === "replay" && (
        <>
          <h2>Manual trigger curl</h2>
          <p className="hint">
            The dashboard resolves the ref to the candidate router image in ECR
            (tag = ref with <code>/</code> → <code>-</code>), installs a
            per-run sandbox, and the in-sandbox agent replays the recording
            against it. Its verdict lands back on this run automatically.
          </p>
          <div className="copyhead">
            <span className="hint">Uses the current form values.</span>
            <CopyButton text={curlCommand} label="copy curl" />
          </div>
          <pre className="cmd">{curlCommand}</pre>

          <h2>PR comment snippet</h2>
          <div className="copyhead">
            <span className="hint">Manual posting only.</span>
            <CopyButton text={prSnippet} label="copy snippet" />
          </div>
          <pre className="cmd">{prSnippet}</pre>
        </>
      )}
    </>
  );
}
