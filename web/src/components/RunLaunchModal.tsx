import React from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, StageRow } from "../lib/api";
import { resultOf } from "../lib/result";
import { ResultChip } from "./Result";
import { KillRun } from "./KillRun";

function CopyLink({ url }: { url: string }) {
  const [copied, setCopied] = React.useState(false);
  return (
    <button
      type="button"
      className="btn"
      onClick={() => {
        void navigator.clipboard.writeText(url).then(() => {
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1600);
        });
      }}
    >
      {copied ? "copied" : "copy link"}
    </button>
  );
}

function elapsed(s: StageRow): string {
  const start = new Date(s.started_at).getTime();
  const end = s.finished_at ? new Date(s.finished_at).getTime() : Date.now();
  const ms = end - start;
  return ms < 1000 ? "<1s" : `${(ms / 1000).toFixed(0)}s`;
}

/**
 * The lifecycle ladder.
 *
 * `/runs/{id}/stages` is append-only and only ever returns stages that have
 * STARTED — a run that died in stage 1 returns exactly one row (verified on
 * run-18c888854e44e86e). So the rows that exist are shown with their real
 * status, and the remainder up to `steps_total` are shown as unnamed "pending"
 * placeholders. A stage name is never invented before the server has emitted it.
 */
function StageLadder({ stages }: { stages: StageRow[] }) {
  const total = stages.reduce((m, s) => Math.max(m, s.steps_total ?? 0), 0);
  const pending = Math.max(0, total - stages.length);
  return (
    <ol className="stageladder">
      {stages.map((s) => (
        <li key={s.id} className={`stagerow ${s.status}${s.status === "running" ? " active" : ""}`}>
          <span className="lstep">{s.step ?? "·"}</span>
          <span className="lname">{s.stage}</span>
          <span className="lstatus">{s.status}</span>
          <span className="ldur">{elapsed(s)}</span>
        </li>
      ))}
      {Array.from({ length: pending }, (_, i) => (
        <li key={`p${i}`} className="stagerow pending">
          <span className="lstep">{stages.length + i + 1}</span>
          <span className="lname">pending</span>
          <span className="lstatus">—</span>
          <span className="ldur" />
        </li>
      ))}
      {stages.length === 0 && (
        <li className="stagerow pending">
          <span className="lstep">1</span>
          <span className="lname">queued — no stage has started yet</span>
          <span className="lstatus">—</span>
          <span className="ldur" />
        </li>
      )}
    </ol>
  );
}

/**
 * Shown the instant `POST /runs` returns. Carries the one thing the user needs
 * to keep — the run id and its shareable URL — so closing this is always safe.
 *
 * Native <dialog>: focus trap, Escape, backdrop and inertness of the page behind
 * come from the platform rather than from a generic Modal wrapper (there is one
 * modal in this application).
 *
 * The create response is `{ run_id, status }` and carries no prior-attempt
 * advisory, so none is shown. Inventing one here would be a claim the server has
 * not made.
 */
export function RunLaunchModal({ runId, onClose }: { runId: string | null; onClose: () => void }) {
  const ref = React.useRef<HTMLDialogElement>(null);

  React.useEffect(() => {
    const d = ref.current;
    if (!d) return;
    if (runId && !d.open) d.showModal();
    if (!runId && d.open) d.close();
  }, [runId]);

  const run = useQuery({
    queryKey: ["run", runId],
    queryFn: () => api.run(runId!),
    enabled: !!runId,
    refetchInterval: (q) =>
      q.state.data && ["completed", "failed"].includes(q.state.data.state) ? false : 2000,
  });
  const terminal = !!run.data && ["completed", "failed"].includes(run.data.state);
  const stages = useQuery({
    queryKey: ["stages", runId],
    queryFn: () => api.stages(runId!),
    enabled: !!runId,
    refetchInterval: terminal ? false : 2000,
  });

  const url = runId ? `${window.location.origin}/r/${runId}` : "";

  return (
    <dialog ref={ref} className="launch" onClose={onClose}>
      {runId && (
        <>
          <div className="launchhead">
            <div>
              <div className="launchlabel">run scheduled</div>
              <code className="launchid">{runId}</code>
            </div>
            {run.data && <ResultChip result={resultOf(run.data)} />}
          </div>

          <div className="launchlink">
            <code className="launchurl">{url}</code>
            <CopyLink url={url} />
          </div>

          <StageLadder stages={stages.data ?? []} />

          <div className="launchfoot">
            <p className="hint">
              This link is the run. Closing here does not stop or lose anything — open it whenever.
            </p>
            <div className="launchbtns">
              <Link className="btn primary" to={`/r/${runId}`} onClick={onClose}>
                open the report
              </Link>
              <button type="button" className="btn" onClick={() => ref.current?.close()}>
                close
              </button>
              {/* The first place a run that will never start becomes visible.
                  Self-gating: nothing renders once the run is terminal. */}
              {run.data && <KillRun run={run.data} />}
            </div>
          </div>
        </>
      )}
    </dialog>
  );
}
