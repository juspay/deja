// Deployments: the reading of what `/api/v1/cd/*` answers, kept out of the
// components so each rule has a test. The orchestrator stores facts (versions,
// an append-only deployment log per environment); what a page SAYS about
// them -- "live since", "six merges not yet on sandbox", "made outside déjà"
// -- is decided here.

import type { CdDeploymentRow, CdPromotionReplayRow, CdPromotion, CdPromotionStep, CdVersionRow, CdVersionWhere } from "./api";

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
    // Placed on main by reconcile but cut outside the pipeline: where it
    // sits is known, the pull requests it carries were never posted.
    if (n === 0) return `cut on ${v.released_from}`;
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
  if (!liveRelease) return null;
  // Dated by the merge it was cut on, else by its own time: reconcile places
  // a release cut outside the pipeline on main, and that merge may predate
  // what déjà tracks.
  const base = liveRelease.released_from ? versions.find((v) => v.version === liveRelease.released_from) : undefined;
  const at = base?.merged_at ?? (liveRelease.released_from ? liveRelease.merged_at : null);
  if (!at) return null;
  const since = Date.parse(at);
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

/** The versions a promotion from `source` may pick: merges live there, newest
 *  first, after what the target runs when that is known. deja checks each
 *  again when one is picked; this only decides what the picker lists. */
export function promotionCandidates(
  versions: { version: CdVersionRow; where: CdVersionWhere[] }[],
  source: string,
  targetLive: CdVersionRow | null | undefined,
): CdVersionRow[] {
  const ran = (w: CdVersionWhere[]) => w.some((r) => r.environment === source && isRunning(r.state));
  const everLive = versions.filter((x) => x.version.kind === "merge" && (ran(x.where) || x.where.some((r) => r.environment === source)));
  const since = mergesSince(versions.map((x) => x.version), targetLive);
  const allowed = since ? new Set(since.map((v) => v.version)) : null;
  return everLive.map((x) => x.version).filter((v) => !allowed || allowed.has(v.version));
}

/** A request's steps for the progress line, each done, current or to come. */
export const PROMOTION_STEPS = ["pending", "picked", "tagged", "built", "committed", "live"] as const;

export function promotionProgress(p: Pick<CdPromotion, "state">): { step: string; at: "done" | "now" | "todo" }[] {
  const i = PROMOTION_STEPS.indexOf(p.state as (typeof PROMOTION_STEPS)[number]);
  return PROMOTION_STEPS.map((step, j) => ({
    step,
    at: i < 0 ? "todo" : j < i || (j === i && p.state === "live") ? "done" : j === i ? "now" : "todo",
  }));
}

/** Whether a request still holds its environment. */
export function inFlight(p: Pick<CdPromotion, "state">): boolean {
  return !["live", "failed", "cancelled"].includes(p.state);
}

/** "5 h ago", or "just now" without a second "ago". */
export function agoText(iso: string, now: number = Date.now()): string {
  const a = ago(iso, now);
  return a === "just now" ? a : `${a} ago`;
}

// ---- a promotion, step by step ---------------------------------------------

/** What each step of a request is called on the page. `pending` is the
 *  request itself; the rest are the steps the promote job posts. */
export const PROMOTION_STEP_LABELS: Record<(typeof PROMOTION_STEPS)[number], string> = {
  pending: "Requested",
  picked: "Picked by the job",
  tagged: "Release tag",
  built: "Image and chart",
  committed: "Chart version committed",
  live: "Live",
};

/** The steps grouped as the page draws them: the request, the build, the deploy. */
export const PROMOTION_STEP_GROUPS: { title: string; steps: (typeof PROMOTION_STEPS)[number][] }[] = [
  { title: "Request", steps: ["pending", "picked"] },
  { title: "Build", steps: ["tagged", "built"] },
  { title: "Deploy", steps: ["committed", "live"] },
];

/** What the job is doing while a step is the next one to post. */
export const PROMOTION_STEP_RUNNING: Record<(typeof PROMOTION_STEPS)[number], string> = {
  pending: "requested",
  picked: "waiting for the job",
  tagged: "finding the tag",
  built: "confirming the image and chart",
  committed: "committing the chart version",
  live: "syncing",
};

/** The phrase for a request's state: where it ended, or what the job is on. */
export function promotionPhrase(p: Pick<CdPromotion, "state" | "step_detail">): string {
  if (p.state === "live" || p.state === "failed" || p.state === "cancelled") return p.state;
  const next = PROMOTION_STEPS[Math.min(promotionReached(p) + 1, PROMOTION_STEPS.length - 1)];
  return PROMOTION_STEP_RUNNING[next];
}

export type RailStep = {
  step: (typeof PROMOTION_STEPS)[number];
  label: string;
  /** done: posted; now: the job is on it; todo: not reached; failed or
   *  cancelled: where the request ended. */
  at: "done" | "now" | "todo" | "failed" | "cancelled";
  detail: CdPromotionStep | null;
};

/** The last step a request posted, as an index into PROMOTION_STEPS: its
 *  state when that is a step, else (failed, cancelled) the furthest step
 *  `step_detail` holds. `pending` counts as reached from the start. */
export function promotionReached(p: Pick<CdPromotion, "state" | "step_detail">): number {
  const i = PROMOTION_STEPS.indexOf(p.state as (typeof PROMOTION_STEPS)[number]);
  if (i >= 0) return i;
  return PROMOTION_STEPS.reduce((best, step, j) => (p.step_detail[step] ? j : best), 0);
}

/**
 * The rail: every step with its state. Steps up to the last one posted are
 * done; the one after it is what the job is on now. A failed request stops
 * at the step it was working on, with the failure's reason there; a
 * cancelled one stops at the request itself, which is the only time Cancel
 * works.
 */
export function promotionRail(p: Pick<CdPromotion, "state" | "step_detail" | "created_at" | "requested_by">): { title: string; steps: RailStep[] }[] {
  const reached = promotionReached(p);
  const ended = p.state === "failed" || p.state === "cancelled";
  const stopAt = p.state === "cancelled" ? reached : p.state === "failed" ? Math.min(reached + 1, PROMOTION_STEPS.length - 1) : -1;
  const end = p.step_detail[p.state === "failed" ? "failed" : "cancelled"] ?? null;
  return PROMOTION_STEP_GROUPS.map(({ title, steps }) => ({
    title,
    steps: steps.map((step) => {
      const j = PROMOTION_STEPS.indexOf(step);
      let at: RailStep["at"];
      if (ended && j === stopAt) at = p.state as "failed" | "cancelled";
      else if (j <= reached && !(ended && j > stopAt)) at = "done";
      else if (!ended && j === reached + 1) at = "now";
      else at = "todo";
      const own = step === "pending" ? { at: p.created_at, by: p.requested_by } : (p.step_detail[step] ?? null);
      const detail = ended && j === stopAt ? { ...(own ?? {}), ...(end ?? {}) } : own;
      return { step, label: PROMOTION_STEP_LABELS[step], at, detail: detail && Object.keys(detail).length ? detail : null };
    }),
  }));
}

/** The step to open first: the one the job is on, or where it ended, else the last. */
export function promotionFocus(rail: { steps: RailStep[] }[]): RailStep {
  const all = rail.flatMap((g) => g.steps);
  return all.find((s) => s.at === "now" || s.at === "failed" || s.at === "cancelled") ?? [...all].reverse().find((s) => s.at === "done") ?? all[0];
}

/** The request's activity, newest first: one line per step posted. */
export function promotionActivity(p: Pick<CdPromotion, "state" | "step_detail" | "created_at" | "requested_by" | "from_version" | "environment" | "release">): { at: string; by: string; what: string }[] {
  const out: { at: string; by: string; what: string }[] = [
    { at: p.created_at, by: p.requested_by, what: `requested ${p.from_version} → ${p.environment}` },
  ];
  const said: Record<string, string> = {
    picked: "picked the request",
    tagged: p.release ? `took ${p.release}, the tag on ${p.from_version}` : "took the release tag",
    built: p.release ? `confirmed the image and chart of ${p.release}` : "confirmed the image and chart",
    committed: `committed the chart version for ${p.environment}`,
    live: `${p.release ?? p.from_version} live on ${p.environment}`,
    failed: "ended the request as failed",
    cancelled: "cancelled the request",
  };
  for (const [step, d] of Object.entries(p.step_detail)) {
    if (!d || !d.at || !said[step]) continue;
    const what = step === "failed" && d.reason ? `${said[step]}: ${d.reason}` : said[step];
    out.push({ at: d.at, by: d.jenkins_build ?? d.by ?? "the promote job", what });
  }
  return out.sort((a, b) => Date.parse(b.at) - Date.parse(a.at));
}

/**
 * The merges a request carries: the one promoted and every merge before it
 * back to the merge the target's release was cut on, newest first. `null`
 * when that cannot be known (see mergesSince), or when the promoted version
 * is not among them (an older target than the request assumed).
 */
export function carriedBy(versions: CdVersionRow[], fromVersion: string, targetLive: CdVersionRow | null | undefined): CdVersionRow[] | null {
  const since = mergesSince(versions, targetLive);
  if (!since) return null;
  const i = since.findIndex((v) => v.version === fromVersion);
  return i < 0 ? null : since.slice(i);
}

// ---- the release strip -------------------------------------------------------

export type StripCell = {
  role: "earlier" | "previous" | "live" | "promoting" | "next";
  /** The version in that place, or null for an empty place. */
  version: string | null;
  /** The request moving, for `promoting`. */
  request: CdPromotion | null;
  /** The row the place comes from, for `earlier`, `previous` and `live`. */
  row: CdDeploymentRow | null;
  /** The merge a release was cut on, when the version says. */
  from: string | null;
  /** How many merges the version carries, or the next promotion would. */
  merges: number | null;
  /** One short line about the place. */
  note: string;
};

/**
 * A promotable environment's releases in order, as a strip: the two before
 * the live one, the live one, the request moving, and what comes next.
 * `history` is the environment's deployment log newest first; a version's
 * place is its newest row, so a failed promotion or an out-of-band change
 * keeps its slot.
 */
export function releaseStrip(
  history: CdDeploymentRow[],
  versions: CdVersionRow[],
  promotions: CdPromotion[],
  source: string,
): StripCell[] {
  const byName = new Map(versions.map((v) => [v.version, v]));
  const fromOf = (version: string | null) => (version && byName.get(version)?.released_from) || null;
  const mergesOf = (version: string | null) => {
    const v = version ? byName.get(version) : null;
    // A release the pipeline recorded says how many merges it carries; one
    // with no recorded contents says nothing rather than "0".
    return v?.kind === "release" && v.released_from && v.prs.length > 0 ? v.prs.length : null;
  };
  const live = history.find((r) => isRunning(r.state)) ?? null;
  // The newest row of each version older than the live row, newest first.
  const seen = new Set<string>(live ? [live.version] : []);
  const before: CdDeploymentRow[] = [];
  for (const r of history) {
    if (live && r.id >= live.id) continue;
    if (seen.has(r.version)) continue;
    seen.add(r.version);
    before.push(r);
  }
  const place = (role: "earlier" | "previous", r: CdDeploymentRow | undefined): StripCell => {
    if (!r) return { role, version: null, request: null, row: null, from: null, merges: null, note: "nothing recorded" };
    const how = howOf(r);
    const note =
      r.state === "failed"
        ? "failed"
        : how.kind === "observed"
          ? how.outOfBand
            ? "replaced · outside déjà"
            : "replaced"
          : isRunning(r.state)
            ? "replaced"
            : r.state.replace("_", " ");
    return { role, version: r.version, request: null, row: r, from: fromOf(r.version), merges: mergesOf(r.version), note };
  };
  const liveNote = (r: CdDeploymentRow) => {
    const how = howOf(r);
    if (how.kind === "job") return `promoted by ${how.by}`;
    return how.outOfBand ? "arrived outside déjà" : "seen by reconcile";
  };
  const moving = promotions.find(inFlight) ?? null;
  const liveVersion = live ? byName.get(live.version) ?? null : null;
  const pending = mergesSince(versions, liveVersion);
  const next: StripCell = moving
    ? { role: "next", version: null, request: null, row: null, from: null, merges: null, note: `any merge after ${moving.from_version}, once it runs on ${source}` }
    : pending === null
      ? { role: "next", version: null, request: null, row: null, from: null, merges: null, note: live ? `not known: ${live.version} names no merge it was cut on` : "nothing live yet" }
      : pending.length === 0
        ? { role: "next", version: null, request: null, row: null, from: null, merges: 0, note: "waiting for a merge" }
        : { role: "next", version: pending[0].version, request: null, row: null, from: null, merges: pending.length, note: `${pending.length} merge${pending.length === 1 ? "" : "s"} on ${source}, not requested` };
  return [
    place("earlier", before[1]),
    place("previous", before[0]),
    live
      ? { role: "live", version: live.version, request: null, row: live, from: fromOf(live.version), merges: mergesOf(live.version), note: liveNote(live) }
      : { role: "live", version: null, request: null, row: null, from: null, merges: null, note: "nothing recorded yet" },
    moving
      ? { role: "promoting", version: moving.release ?? moving.from_version, request: moving, row: null, from: moving.from_version, merges: null, note: promotionPhrase(moving) }
      : { role: "promoting", version: null, request: null, row: null, from: null, merges: null, note: "nothing moving" },
    next,
  ];
}

/**
 * A build as a row names it. The pipeline names builds `<job> #<n>` (D10),
 * never by URL; rows written before that carry a URL, which stays a link.
 */
export function buildLink(build: string | null | undefined): { name: string; href: string | null } | null {
  if (!build) return null;
  return /^https?:\/\//.test(build) ? { name: "build", href: build } : { name: build, href: null };
}

/**
 * The release tag integ-deploy cut as a merge landed, from the merge's
 * deployment rows: `detail.tag` on the request row, or `detail.candidate_error`
 * when it could not. The image and chart under that tag are built in the
 * background, so a promotion finds them ready (D8, D15).
 */
export function releaseTagOf(rows: (Pick<CdDeploymentRow, "detail"> | null | undefined)[]): { tag: string | null; error: string | null } | null {
  for (const r of rows) {
    const d = r?.detail as { tag?: unknown; candidate_error?: unknown } | null | undefined;
    const tag = typeof d?.tag === "string" && d.tag ? d.tag : null;
    const error = typeof d?.candidate_error === "string" && d.candidate_error ? d.candidate_error : null;
    if (tag || error) return { tag, error };
  }
  return null;
}
