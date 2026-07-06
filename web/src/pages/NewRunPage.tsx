import React from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useNavigate, useSearchParams } from "react-router-dom";
import { api } from "../lib/api";

// The fast-iteration build profile demo/lib.sh uses (DEMO_CARGO_PROFILE).
const BUILD_CMD = (patch: string) => `# 1. apply the candidate patch to the vendored Hyperswitch tree
git -C vendor/hyperswitch-deja-clean apply ${patch}

# 2. build the candidate router with the fast profile
( cd vendor/hyperswitch-deja-clean && \\
  env CARGO_PROFILE_RELEASE_LTO=false \\
      CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256 \\
      CARGO_PROFILE_RELEASE_OPT_LEVEL=2 \\
      CARGO_PROFILE_RELEASE_INCREMENTAL=true \\
  cargo build --release -p router --features deja,v1 --bin router )

# 3. copy the binary somewhere stable and paste its path below
cp vendor/hyperswitch-deja-clean/target/release/router /tmp/router-candidate
# → candidate binary path: /tmp/router-candidate

# 4. revert the patch (the vendor tree goes back to V1)
git -C vendor/hyperswitch-deja-clean apply -R ${patch}`;

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

export default function NewRunPage() {
  const nav = useNavigate();
  const [params] = useSearchParams();
  const [mode, setMode] = React.useState<"record" | "replay">("replay");
  const [recordingId, setRecordingId] = React.useState(params.get("recording") ?? "");
  const [binaryPath, setBinaryPath] = React.useState("");
  const [iterations, setIterations] = React.useState(1);
  const [expectation, setExpectation] = React.useState("");
  const [scenario, setScenario] = React.useState("benign-line-shift");


  const triggerSpec = React.useMemo(() => {
    const candidate = binaryPath
      ? { kind: "local_path", binary_or_source: binaryPath }
      : { kind: "prebuilt_image", image: "deja-demo" };
    const spec: Record<string, unknown> =
      mode === "record"
        ? { mode, candidate_spec: candidate, recording_id: null, workload: { iterations } }
        : { mode, candidate_spec: candidate, recording_id: recordingId || "<recording_id>" };
    if (expectation) spec.expectation = expectation;
    return spec;
  }, [binaryPath, expectation, iterations, mode, recordingId]);

  const curlCommand = React.useMemo(() => {
    const body = JSON.stringify(triggerSpec);
    return [
      `curl -sS -X POST ${window.location.origin}/api/v1/runs`,
      "  -H 'content-type: application/json'",
      `  -H ${shellQuote("X-Deja-Actor: user:<name>")}`,
      `  --data ${shellQuote(body)}`,
    ].join(" \\\n");
  }, [triggerSpec]);

  const prSnippet = React.useMemo(
    () =>
      [
        "### Deja replay request",
        "",
        `- Recording: \`${recordingId || "<recording_id>"}\``,
        `- Candidate binary: \`${binaryPath || "/tmp/router-candidate"}\``,
        `- Expected result: \`${expectation || "pass/diverge"}\``,
        "",
        "Trigger locally from the orchestrator worktree:",
        "",
        "```sh",
        curlCommand,
        "```",
        "",
        "After it finishes, update this comment with:",
        `- Run: ${window.location.origin}/runs/<run_id>`,
        `- Scorecard: ${window.location.origin}/runs/<run_id>/scorecard`,
        "- Artifact URL(s): copy from the run Artifacts tab",
      ].join("\n"),
    [binaryPath, curlCommand, expectation, recordingId],
  );
  const recordings = useQuery({ queryKey: ["recordings"], queryFn: api.recordings });

  const create = useMutation({
    mutationFn: () => {
      const spec = triggerSpec;
      return api.createRun(spec);
    },
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
              recording
              <select value={recordingId} onChange={(e) => setRecordingId(e.target.value)}>
                <option value="">— pick a recording —</option>
                {recordings.data?.map((r) => (
                  <option key={r.recording_id} value={r.recording_id}>
                    {r.recording_id} ({r.event_count ?? "?"} events)
                  </option>
                ))}
              </select>
            </label>
            <label>
              candidate router binary path{" "}
              <span className="hint">
                (empty = the default local build, i.e. self-replay; later this
                field becomes PR/branch/commit/tag)
              </span>
              <input
                type="text"
                placeholder="/tmp/router-candidate"
                value={binaryPath}
                onChange={(e) => setBinaryPath(e.target.value)}
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

        <button disabled={create.isPending || (mode === "replay" && !recordingId)}>
          {create.isPending ? "scheduling…" : "schedule run"}
        </button>
        {create.error && <p className="err">{String(create.error)}</p>}
      </form>

      {mode === "replay" && (
        <>
          <h2>Build a candidate binary (copy into a terminal)</h2>
          <label>
            cross-version scenario
            <select value={scenario} onChange={(e) => setScenario(e.target.value)}>
              <option value="benign-line-shift">benign-line-shift (expect pass)</option>
              <option value="real-change">real-change (expect diverge)</option>
            </select>
          </label>
          <pre className="cmd">{BUILD_CMD(`demo/cross-version/${scenario}.patch`)}</pre>
          <p className="hint">
            If the new binary's sha256 equals the previous candidate's, the patch
            was compile-neutral — the run page shows the sha for comparison.
          </p>

          <h2>Manual trigger curl</h2>
          <p className="hint">
            This is the real v1 path: build or copy a local router binary, then
            POST the run spec with <code>candidate_spec.kind=local_path</code>.
            PR/branch/SHA resolution is a later candidate-resolver step, not a
            dashboard gate yet.
          </p>
          <div className="copyhead">
            <span className="hint">Uses the current form values.</span>
            <CopyButton text={curlCommand} label="copy curl" />
          </div>
          <pre className="cmd">{curlCommand}</pre>

          <h2>PR comment snippet</h2>
          <p className="hint">
            Paste this into a PR while v1 remains manual. No webhook is implied;
            update the run and scorecard links after the replay completes.
          </p>
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
