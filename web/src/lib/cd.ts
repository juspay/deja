// Deployments: the reading of what `/api/v1/cd/*` answers, kept out of the
// components so each rule has a test. The orchestrator stores facts (versions,
// an append-only deployment log per environment); what a page SAYS about
// them -- "live since", "six merges not yet on sandbox", "made outside déjà"
// -- is decided here.

import type { CdDeploymentRow, CdPromotionReplayRow, CdVersionRow, CdVersionWhere } from "./api";

/** A deployment-log state that means the version is what the environment runs. */
export function isRunning(state: string): boolean {
  return state === "live" || state === "rolled_back";
}

/** One PR's `#N` and title, for a merge version. A release carries many and
 *  shows a count instead; see `carries`. */
export function pullRequestOf(v: Pick<CdVersionRow, "prs">): { number: number; title: string } | null {
  const pr = v.prs[0];
  if (!pr) return null;
  return { number: pr.pr_number, title: pr.title ?? "" };
}

/** What a version carries, as one phrase: `#2488 add avs check…` for a merge,
 *  `6 merges, from 8eaddb0b85` for a release. */
export function carries(v: CdVersionRow): string {
  if (v.kind === "release") {
    const n = v.prs.length;
    // A release the pipeline never posted (first seen on an environment)
    // has no recorded contents; saying "0 pull requests" would be a claim.
    if (n === 0 && !v.released_from) return "contents not recorded";
    const from = v.released_from ? `, from ${v.released_from}` : "";
    return `${n} pull request${n === 1 ? "" : "s"}${from}`;
  }
  const pr = pullRequestOf(v);
  return pr ? `#${pr.number} ${pr.title}`.trim() : "—";
}

/**
 * How a deployment-log row came to be. Rows a job wrote name the job's actor
 * and build; rows reconcile wrote from an Argo observation name `observed`,
 * and say in `detail.out_of_band` whether anyone had asked for the version.
 */
export type How =
  | { kind: "job"; by: string; build: string | null }
  | { kind: "observed"; outOfBand: boolean };

export function howOf(row: Pick<CdDeploymentRow, "requested_by" | "jenkins_build" | "detail">): How {
  if (row.requested_by === "observed") {
    const d = row.detail as { out_of_band?: unknown; asked_by?: unknown } | null;
    // An arrival somebody asked for is theirs: the observation row carries
    // the asker and the build it copied from the request.
    if (typeof d?.asked_by === "string" && d.asked_by) return { kind: "job", by: d.asked_by, build: row.jenkins_build ?? null };
    return { kind: "observed", outOfBand: d?.out_of_band === true };
  }
  return { kind: "job", by: row.requested_by, build: row.jenkins_build ?? null };
}

/** Argo's sync and health as the row recorded them, when it recorded them:
 *  only observation rows carry these. */
export function argoOf(row: Pick<CdDeploymentRow, "detail"> | null | undefined): { sync: string; health: string } | null {
  const d = row?.detail as { sync?: unknown; health?: unknown } | null | undefined;
  if (typeof d?.sync === "string" && typeof d?.health === "string") return { sync: d.sync, health: d.health };
  return null;
}

/**
 * The merges that ran on `from` (integ) and are not yet in what `to` (sandbox)
 * runs: every merge version newer than the merge the live release was tagged
 * on. `null` when that cannot be known -- the live release names no
 * `released_from`, or that merge is not in the list: a release first seen on
 * the environment by observation (today's `2026.09.28.1-hotfix1`) is one.
 */
export function mergesSince(
  versions: CdVersionRow[],
  liveRelease: CdVersionRow | null | undefined,
): CdVersionRow[] | null {
  if (!liveRelease?.released_from) return null;
  const base = versions.find((v) => v.version === liveRelease.released_from);
  if (!base?.merged_at) return null;
  const since = Date.parse(base.merged_at);
  return versions
    .filter((v) => v.kind === "merge" && v.merged_at && Date.parse(v.merged_at) > since)
    .sort((a, b) => Date.parse(b.merged_at!) - Date.parse(a.merged_at!));
}

/** Historical rows say where a version was; environment observations identify
 * the last confirmed live version. A newer syncing/failed intent does not
 * replace that observation. */
export function placesOf(
  where: CdVersionWhere[],
  environments: string[],
  observed: { environment: string; version: string | null }[] = [],
): { environment: string; state: string | null; present: boolean; lastConfirmed: boolean }[] {
  return environments.map((environment) => {
    const row = where.find((w) => w.environment === environment);
    const liveVersion = observed.find((o) => o.environment === environment)?.version;
    const lastConfirmed = liveVersion != null && liveVersion === row?.version;
    return { environment, state: row?.state ?? null, present: lastConfirmed, lastConfirmed };
  });
}

/** `3 days` / `41 min` / `just now`, for a duration since `iso`. */
export function ago(iso: string, now: number = Date.now()): string {
  const mins = Math.max(0, Math.round((now - Date.parse(iso)) / 60000));
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins} min`;
  const hours = Math.round(mins / 60);
  if (hours < 48) return `${hours} h`;
  return `${Math.round(hours / 24)} days`;
}

/** `today 07:12` / `Oct 09 14:20`, in the viewer's zone. */
export function when(iso: string, now: number = Date.now()): string {
  const d = new Date(iso);
  const n = new Date(now);
  const hm = d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
  const sameDay = d.getFullYear() === n.getFullYear() && d.getMonth() === n.getMonth() && d.getDate() === n.getDate();
  if (sameDay) return `today ${hm}`;
  return `${d.toLocaleDateString(undefined, { month: "short", day: "2-digit" })} ${hm}`;
}

/** The chip class for a deployment state. */
export function stateTone(state: string): string {
  switch (state) {
    case "live":
      return "ok";
    case "failed":
      return "failed";
    case "rolled_back":
      return "inconclusive";
    case "requested":
    case "committed":
    case "syncing":
      return "pending";
    default:
      return "muted";
  }
}

/**
 * What a promotion replay says, as one word and a chip tone. The candidate run's
 * own delta verdict wins when it has one (`pass`, `fail`, `acknowledged`):
 * it is current, an acknowledgement given after the job finished counts.
 * Otherwise the row's state speaks: a promotion replay still `requested` is running (or
 * never came back), a `skipped` or `failed` one says so, and a `done` one
 * without a run word falls back to what the job read.
 */
export function promotionReplayWord(h: Pick<CdPromotionReplayRow, "state" | "verdict" | "run_state" | "run_delta_verdict">): { word: string; tone: string } {
  // The row's state comes first: a replay that failed, was skipped or has not
  // finished has no verdict, whatever its candidate run says today. Only a
  // finished replay reads the candidate run's live verdict (the store hands it
  // over only while the run is still measured against this row's baseline),
  // which can turn a recorded fail into an acknowledgement.
  if (h.state === "requested") {
    return h.run_state === "failed" ? { word: "failed", tone: "failed" } : { word: "running", tone: "pending" };
  }
  if (h.state === "skipped") return { word: "skipped", tone: "muted" };
  if (h.state === "failed") return { word: "failed", tone: "failed" };
  const live = h.run_delta_verdict;
  if (live === "pass" || live === "acknowledged") return { word: live, tone: "ok" };
  if (live === "fail") return { word: "fail", tone: "failed" };
  switch (h.verdict) {
    case "pass":
    case "acknowledged":
      return { word: h.verdict, tone: "ok" };
    case "fail":
      return { word: "fail", tone: "failed" };
    case "inconclusive":
      return { word: "inconclusive", tone: "inconclusive" };
    default:
      return { word: "unknown", tone: "muted" };
  }
}
