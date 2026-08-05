import React from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api, RunRow } from "../lib/api";
import { resultOf } from "../lib/result";

/**
 * STOP A RUN. `POST /runs/{id}/kill`.
 *
 * A replay pod holds a candidate, a runner and two stores, and the Job is kept
 * after it finishes so its logs can be read. A run that is wedged therefore sits
 * on all of that until somebody reaches for kubectl — which, before this, was
 * the only way to stop one from anywhere.
 *
 * FOUR THINGS THIS REFUSES TO DO.
 *
 * 1. Offer itself on a run that is not running. Killing a finished run removes
 *    nothing and would only invite pressing it; the control renders only while
 *    `resultOf(run).state === "RUNNING"`.
 * 2. Fire on one click. The Job and its pods do not come back, and the run is
 *    settled as failed, so the button ARMS first and the armed state says what
 *    will happen.
 * 3. Claim more than the server did. The response reports what was actually
 *    removed; an idempotent kill of an already-dead run removes nothing and is
 *    reported as exactly that, not as "killed". `problems` is a partial kill and
 *    is shown in full.
 * 4. Swallow a failure. Errors render through the same `.err` line the run form
 *    uses, and the button becomes "retry kill" rather than disappearing.
 *
 * The outcome shows up on the run itself: the server settles it failed with
 * `killed by {actor}`, and the queries this invalidates are the ones the list,
 * the report and the launch modal read.
 */
export function KillRun({ run, compact = false }: { run: RunRow; compact?: boolean }) {
  const qc = useQueryClient();
  const [armed, setArmed] = React.useState(false);
  const [dismissed, setDismissed] = React.useState(false);

  const kill = useMutation({
    mutationFn: () => api.killRun(run.run_id),
    onSuccess: () => {
      setArmed(false);
      void qc.invalidateQueries({ queryKey: ["runs"] });
      void qc.invalidateQueries({ queryKey: ["run", run.run_id] });
      void qc.invalidateQueries({ queryKey: ["stages", run.run_id] });
    },
  });

  const live = resultOf(run).state === "RUNNING";
  const report = kill.data;
  const note = !dismissed && (report || kill.error);

  // Once the kill lands the run stops being live, so the control would vanish
  // and take its own outcome with it. It stays mounted while it has something
  // to report.
  if (!live && !note) return null;

  const removed = report
    ? [
        report.job_deleted ? `job ${report.job_deleted}` : null,
        report.pods_deleted.length
          ? `${report.pods_deleted.length} pod${report.pods_deleted.length === 1 ? "" : "s"}`
          : null,
      ].filter(Boolean)
    : [];

  const outcome = report ? (
    <>
      {removed.length > 0 ? (
        <>
          <b>killed</b> — removed {removed.join(" and ")}.
        </>
      ) : (
        <>
          <b>nothing left to remove</b> — the Job and its pods were already gone. The run is
          settled either way.
        </>
      )}
      {report.problems.length > 0 && (
        <>
          {" "}
          <span className="killwarn">
            {report.problems.length} problem{report.problems.length === 1 ? "" : "s"}:{" "}
            {report.problems.join("; ")}
          </span>
        </>
      )}
    </>
  ) : null;

  const body = (
    <>
      {kill.error && (
        <p className="err killnote">
          <b>Kill failed.</b> {String(kill.error)}
        </p>
      )}
      {outcome && <p className="killnote">{outcome}</p>}
      {note && (
        <button type="button" className="killdismiss" onClick={() => setDismissed(true)}>
          dismiss
        </button>
      )}
    </>
  );

  return (
    <div className={compact ? "killwrap compact" : "killwrap"}>
      <div className="killrow">
        {live && !armed && (
          <button
            type="button"
            className="btn killbtn"
            onClick={() => {
              setDismissed(true);
              setArmed(true);
            }}
            title={`stop run ${run.run_id} and delete its Job and pods`}
          >
            {kill.error ? "retry kill" : "kill"}
          </button>
        )}
        {live && armed && (
          <>
            <button
              type="button"
              className="btn killbtn armed"
              disabled={kill.isPending}
              onClick={() => {
                setDismissed(false);
                kill.mutate();
              }}
            >
              {kill.isPending ? "killing…" : "confirm kill"}
            </button>
            <button type="button" className="btn killcancel" onClick={() => setArmed(false)}>
              cancel
            </button>
            {!compact && (
              <span className="hint killarmed">
                deletes the Job and its pods and settles the run as failed. Not reversible.
              </span>
            )}
          </>
        )}
      </div>

      {compact && armed && (
        <p className="killpop hint">
          deletes the Job and its pods and settles the run as failed. Not reversible.
        </p>
      )}
      {compact ? note ? <div className="killpop">{body}</div> : null : body}
    </div>
  );
}
