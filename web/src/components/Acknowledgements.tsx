import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { Delta, RunRow, api, runParams } from "../lib/api";
import { useActor } from "../lib/me";
import { useDebug, withDebug } from "../lib/debug";
import { badgeOf, effectiveOf, groupCharged, requestsOf, useAcknowledgements } from "../lib/acknowledge";

/**
 * The two small faces of acknowledgement on the report: the button in the
 * header, with what is waiting for the person looking, and the one line in
 * the "Compared with main" panel. The work itself happens on the acknowledge
 * page (`/r/:id/acknowledge`).
 */

/** The header button. Renders nothing on a run that names no pull request,
 *  has no baseline, or introduced nothing. */
export function AcknowledgeButton({ run }: { run: RunRow }) {
  const debug = useDebug();
  const against = runParams(run)?.delta_against ?? "";
  const delta = useQuery({
    queryKey: ["delta", run.run_id, against],
    queryFn: () => api.delta(run.run_id, against),
    enabled: !!against,
  });
  const acks = useAcknowledgements(run.run_id);
  const me = useActor();
  const d = delta.data && !("unavailable" in delta.data) ? delta.data : null;
  if (!d) return null;
  // An overlay that could not happen, or acknowledgements that could not be
  // read, still get the button: the page says what went wrong.
  if (d.verdict.overlay_failure || acks.error) {
    return (
      <Link className="btn ack-btn todo" to={withDebug(`/r/${run.run_id}/acknowledge`, debug)}>
        Acknowledge <span className="count todo">unknown</span>
      </Link>
    );
  }
  if (!acks.data) return null;
  const badge = badgeOf(groupCharged(d), d, me);
  if (!badge) return null;
  return (
    <Link className={`btn ack-btn ${badge.tone}`} to={withDebug(`/r/${run.run_id}/acknowledge`, debug)}>
      Acknowledge <span className={`count ${badge.tone}`}>{badge.label}</span>
    </Link>
  );
}

/** The one line in the delta panel: where the acknowledgements stand, and
 *  the way to the page. */
export function AcknowledgeLine({ runId, d }: { runId: string; d: Delta }) {
  const debug = useDebug();
  const acks = useAcknowledgements(runId);
  const v = d.verdict;
  const page = (
    <span className="sub">
      <Link to={withDebug(`/r/${runId}/acknowledge`, debug)}>open the acknowledge page →</Link>
    </span>
  );
  // The failures come first, before any read of the acknowledgements: the
  // conditions that stop the overlay (the store away, rows unreadable) are
  // the same ones that fail the acknowledgements request, and a line that
  // waited for that request would never show them.
  if (v.overlay_failure) {
    return (
      <div className="delta-ack-verdict tone-bad">
        <span className="chip solid fail">unknown</span>
        <div className="txt">
          <span className="h">The acknowledgements could not be laid over this delta: {v.overlay_failure}. What is shown is the bare comparison, not a decision.</span>
          {page}
        </div>
      </div>
    );
  }
  if (acks.error) {
    return (
      <div className="delta-ack-verdict tone-bad">
        <span className="chip solid fail">unknown</span>
        <div className="txt">
          <span className="h">The pull request's acknowledgements could not be read: {String((acks.error as Error).message)}.</span>
          {page}
        </div>
      </div>
    );
  }
  if (!acks.data) return null;
  const groups = groupCharged(d);
  if (groups.length === 0) return null;
  const effective = effectiveOf(d);
  const gh = acks.data.github;
  const uncovered = groups.filter((g) => !g.ack).reduce((n, g) => n + g.rows.length, 0);
  const unread = v.unread_acknowledgements ?? 0;
  const text =
    effective === "acknowledged"
      ? `Every divergence this PR introduces is acknowledged: ${groups.length} shapes in ${requestsOf(groups)} requests. The check reads as passing.`
      : `${groups.length} shapes of divergence in ${requestsOf(groups)} requests: ${v.acknowledged ?? 0} rows acknowledged, ${v.proposed ?? 0} proposed, ${v.stale ?? 0} stale, ${uncovered} not yet marked. The check reads as failing until every shape is acknowledged.${unread > 0 ? ` ${unread} acknowledgement${unread === 1 ? "" : "s"} could not be read and did not count.` : ""}`;
  return (
    <div className={`delta-ack-verdict tone-${effective === "acknowledged" ? "good" : "bad"}`}>
      <span className={`chip solid ${effective === "acknowledged" ? "pass" : "fail"}`}>{effective}</span>
      <div className="txt">
        <span className="h">{text}</span>
        <span className="sub">
          PR {gh.repo}#{gh.pr_number} ·{" "}
          <Link to={withDebug(`/r/${runId}/acknowledge`, debug)}>open the acknowledge page →</Link>
        </span>
      </div>
    </div>
  );
}

