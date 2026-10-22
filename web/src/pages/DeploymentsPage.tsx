// Deployments: what runs in each environment, how it got there, and where
// each version and pull request is. Every fact comes from the CD pipeline's
// posts and the reconcile job's observations, through `/api/v1/cd/*`. The
// controls are the pause switch and, on a promotable environment, Promote:
// a request the promote job picks up and reports step by step.
//
// BETA, for maintainers and environment upgraders: the API answers a signed-in
// maintainer or upgrader (or the pipeline's token) only, and the tab is drawn
// for them only.
//
// Seven views under /deployments, all keyed by `?system=` so a link carries
// the system it was read from:
//   /deployments                                   pipeline, with the release strip
//   /deployments/versions                          merges and releases
//   /deployments/versions/:version                 one version, its history
//   /deployments/environments/:env                 one environment's history
//   /deployments/promotions                        every promotion request
//   /deployments/promotions/:id                    one request, step by step
//   /deployments/pull-requests/:owner/:repo/:n     where a pull request is

import React from "react";
import { useMutation, useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useLocation, useParams, useSearchParams } from "react-router-dom";

import { loginUrl, useMe } from "../lib/me";
import {
  cdApi,
  type ApiError,
  type CdConfig,
  type CdDeploymentRow,
  type CdEnvironment,
  type CdPromotion,
  type CdPromotionCheck,
  type CdPromotionReplayRow,
  type CdVersionRow,
  type CdVersionWhere,
} from "../lib/api";
import {
  ago,
  agoText,
  argoOf,
  carriedBy,
  carries,
  howOf,
  inFlight,
  isRunning,
  mergesSince,
  placesOf,
  promotionActivity,
  promotionCandidates,
  promotionFocus,
  promotionPhrase,
  promotionProgress,
  PROMOTION_STEP_RUNNING,
  promotionRail,
  promotionReplayWord,
  pullRequestOf,
  releaseStrip,
  stateTone,
  when,
  type RailStep,
  type StripCell,
} from "../lib/cd";

// ---- shared ---------------------------------------------------------------

function useCdConfig() {
  // Polled, so a pause made by someone else shows up here too.
  return useQuery({ queryKey: ["cd", "config"], queryFn: cdApi.config, staleTime: 15_000, refetchInterval: 30_000, retry: false });
}

/** The system the page reads, from `?system=`, else the first tracked one. */
function useSystem(cfg: CdConfig | undefined): [string, (s: string) => void] {
  const [params, setParams] = useSearchParams();
  const first = cfg?.enabled ? cfg.systems[0] : "";
  const system = params.get("system") || first;
  const set = (s: string) => {
    const next = new URLSearchParams(params);
    if (s === first) next.delete("system");
    else next.set("system", s);
    setParams(next, { replace: true });
  };
  return [system, set];
}

/** A link under /deployments that keeps the system. */
function href(path: string, system: string, cfg: CdConfig | undefined): string {
  const first = cfg?.enabled ? cfg.systems[0] : "";
  return system && system !== first ? `${path}?system=${encodeURIComponent(system)}` : path;
}

function Off({ why }: { why?: string }) {
  return (
    <div className="empty">
      <div className="et">Deployment tracking is off</div>
      <p className="hint">
        {why ?? "This orchestrator has no [cd] block, so nothing posts what runs where."}
      </p>
    </div>
  );
}

/** What someone who is neither a maintainer nor an upgrader sees: the reason, and the way in. */
function MaintainersOnly({ err }: { err: ApiError }) {
  const signIn = err.status === 401 && !String(err.message).includes("not configured");
  return (
    <div className="empty">
      <div className="et">
        Deployments <span className="beta">beta</span>
      </div>
      <p className="hint">
        Deployments is in beta and open to maintainers and environment upgraders only.{" "}
        {err.status === 403
          ? "Your account is on neither list; ask to be added to one in the déjà configuration."
          : signIn
            ? "Sign in with a maintainer or upgrader account."
            : "Sign-in is not configured on this deployment, so no one can open it here."}
      </p>
      {signIn && (
        <a className="btn primary" href={loginUrl()}>
          Sign in
        </a>
      )}
    </div>
  );
}

/** The pipeline's pause switch: a banner while paused for everyone who reads
 *  the page; the control for a maintainer only, as the API takes it. */
function PauseSwitch({ cfg, system }: { cfg: Extract<CdConfig, { enabled: true }>; system: string }) {
  const me = useMe();
  // Drawn for a maintainer only: the API refuses the flip from anyone else,
  // and an upgrader reading the page should not be offered a button that fails.
  const maintainer = (me.data?.roles ?? []).includes("maintainer");
  const qc = useQueryClient();
  const paused = cfg.paused?.[system];
  const [asking, setAsking] = React.useState(false);
  const [reason, setReason] = React.useState("");
  const flip = useMutation({
    mutationFn: (next: boolean) => cdApi.setPause(system, next, reason.trim()),
    onSuccess: () => {
      setAsking(false);
      setReason("");
      void qc.invalidateQueries({ queryKey: ["cd", "config"] });
    },
  });
  if (paused) {
    return (
      <div className="cd-pause on" role="status">
        <div>
          <b>Pipeline paused</b> for {system}. No merge is deployed to integ, and no promotion replay, release or promotion is
          started, until it is resumed; one already past its first step finishes. Reconcile keeps reporting what runs.
          <div className="hint">
            by {paused.changed_by} · {when(paused.changed_at)} · {agoText(paused.changed_at)}{paused.reason ? ` · ${paused.reason}` : ""}
          </div>
          {flip.error && <div className="err">{String(flip.error)}</div>}
        </div>
        {maintainer && (
          <button className="btn primary" disabled={flip.isPending} onClick={() => flip.mutate(false)}>
            Resume
          </button>
        )}
      </div>
    );
  }
  return (
    <div className="cd-pause">
      {!asking ? (
        <>
          <span className="hint">Pipeline running for {system}.</span>
          {maintainer && (
            <button className="btn" onClick={() => setAsking(true)}>
              Pause pipeline
            </button>
          )}
        </>
      ) : (
        <form
          className="cd-pauseform"
          onSubmit={(e) => {
            e.preventDefault();
            if (reason.trim()) flip.mutate(true);
          }}
        >
          <label htmlFor="cd-pause-reason">
            Why pause? integ-deploy, promotion replays, releases and promotions stop before their next step; a step already running finishes.
          </label>
          <input
            id="cd-pause-reason"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder="e.g. integ is broken by #2501, investigating"
            autoFocus
          />
          <div className="cd-pauseactions">
            <button className="btn danger" type="submit" disabled={!reason.trim() || flip.isPending}>
              Pause
            </button>
            <button className="btn" type="button" onClick={() => setAsking(false)}>
              Cancel
            </button>
          </div>
          {flip.error && <div className="err">{String(flip.error)}</div>}
        </form>
      )}
    </div>
  );
}

/** The page frame: title, system switcher, sub-navigation. Children render
 *  only once the config says tracking is on. */
function Frame({ title, crumb, children }: { title: React.ReactNode; crumb?: React.ReactNode; children: (cfg: Extract<CdConfig, { enabled: true }>, system: string) => React.ReactNode }) {
  const cfg = useCdConfig();
  const [system, setSystem] = useSystem(cfg.data);
  const here = useLocation().pathname;
  // How many requests are moving, for the Promotions tab's badge.
  const moving = useQuery({
    queryKey: ["cd", "promotions", system],
    queryFn: () => cdApi.promotionsAll(system, 20),
    enabled: !!system && !!cfg.data?.enabled,
    refetchInterval: 30_000,
    retry: false,
  });
  const running = (moving.data ?? []).filter(inFlight).length;
  const sub = (path: string, label: string, exact = false, badge?: React.ReactNode) => {
    const on = exact ? here === path : here.startsWith(path);
    return (
      <Link className={on ? "cd-sub active" : "cd-sub"} to={href(path, system, cfg.data)}>
        {label}
        {badge}
      </Link>
    );
  };
  if (cfg.isLoading) return <p className="hint">loading…</p>;
  const status = (cfg.error as ApiError | null)?.status;
  if (cfg.error && (status === 401 || status === 403)) return <MaintainersOnly err={cfg.error as ApiError} />;
  if (cfg.error) return <Off why={String(cfg.error)} />;
  if (!cfg.data || !cfg.data.enabled) return <Off />;
  const c = cfg.data;
  return (
    <>
      <div className="pagehead">
        <h1>
          {title} <span className="beta" title="In beta, for maintainers">beta</span>
        </h1>
        <div className="cd-switch">
          <label className="hint" htmlFor="cd-system">
            System
          </label>
          <select id="cd-system" value={system} onChange={(e) => setSystem(e.target.value)} aria-label="system">
            {c.systems.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>
      </div>
      <div className="cd-subnav">
        {sub("/deployments", "Pipeline", true)}
        {sub("/deployments/versions", "Versions")}
        {sub("/deployments/environments", "Environments")}
        {sub("/deployments/promotions", "Promotions", false, running > 0 ? <i className="cd-subbadge">{running} running</i> : null)}
      </div>
      <PauseSwitch cfg={c} system={system} />
      {crumb && <div className="crumb">{crumb}</div>}
      {children(c, system)}
    </>
  );
}

function StateChip({ state }: { state: string }) {
  return <span className={`chip ${stateTone(state)}`}>{state.replace("_", " ")}</span>;
}

function ArgoChip({ row }: { row: CdDeploymentRow | null | undefined }) {
  const a = argoOf(row);
  if (!a) return <span className="hint">—</span>;
  const ok = a.sync === "Synced" && a.health === "Healthy";
  return (
    <span className={`chip ${ok ? "ok" : "pending"}`}>
      {a.sync} · {a.health}
    </span>
  );
}

function HowCell({ row }: { row: CdDeploymentRow }) {
  const how = howOf(row);
  if (how.kind === "observed") {
    return how.outOfBand ? (
      <span className="cd-oob" title="A change nobody asked for through the pipeline; seen by the reconcile job">
        outside déjà · seen by reconcile
      </span>
    ) : (
      <span className="hint">seen by reconcile</span>
    );
  }
  return (
    <>
      {how.by}
      {how.build && (
        <>
          {" · "}
          <a href={how.build} target="_blank" rel="noreferrer">
            build
          </a>
        </>
      )}
    </>
  );
}

function VersionLink({ version, system, cfg }: { version: string; system: string; cfg: CdConfig | undefined }) {
  return (
    <Link className="mono" to={href(`/deployments/versions/${encodeURIComponent(version)}`, system, cfg)}>
      {version}
    </Link>
  );
}

/** A promotion replay's one word, with the reason on hover. */
function PromotionReplayChip({ replay }: { replay: CdPromotionReplayRow | null | undefined }) {
  if (!replay) return <span className="hint">—</span>;
  const { word, tone } = promotionReplayWord(replay);
  const repointed = replay.run_delta_against && replay.baseline_run && replay.run_delta_against !== replay.baseline_run
    ? `candidate run since re-pointed at ${replay.run_delta_against}; the recorded verdict stands`
    : "";
  const title = [`against ${replay.against_version}`, replay.reason ?? "", repointed].filter(Boolean).join(" · ");
  return (
    <span className={`chip ${tone}`} title={title}>
      {word}
    </span>
  );
}

/** Links to the two runs of a promotion replay, when it got as far as creating them. */
function PromotionReplayRuns({ replay }: { replay: CdPromotionReplayRow }) {
  if (!replay.candidate_run) return <span className="hint">—</span>;
  return (
    <span className="cd-replay-runs">
      <Link className="mono" to={`/runs/${replay.candidate_run}`}>
        candidate
      </Link>
      {replay.baseline_run && (
        <>
          {" · "}
          <Link className="mono" to={`/runs/${replay.baseline_run}`}>
            baseline
          </Link>
        </>
      )}
    </span>
  );
}

function PrCell({ v }: { v: CdVersionRow }) {
  if (v.kind === "release") return <span className="hint">{carries(v)}</span>;
  const pr = pullRequestOf(v);
  if (!pr) return <span className="hint">—</span>;
  const repo = v.prs[0]?.repo ?? "";
  return (
    <span className="cd-pr">
      <Link className="cd-prnum" to={`/deployments/pull-requests/${repo}/${pr.number}`}>
        #{pr.number}
      </Link>
      <span className="cd-prtitle" title={pr.title}>
        {pr.title}
      </span>
    </span>
  );
}

// ---- pipeline ---------------------------------------------------------------

export default function DeploymentsPage() {
  return (
    <Frame title="Deployments">
      {(cfg, system) => <Pipeline cfg={cfg} system={system} />}
    </Frame>
  );
}

function Pipeline({ cfg, system }: { cfg: Extract<CdConfig, { enabled: true }>; system: string }) {
  const envs = useQuery({
    queryKey: ["cd", "environments", system],
    queryFn: () => cdApi.environments(system),
    refetchInterval: 15_000,
  });
  const versions = useQuery({
    queryKey: ["cd", "versions", system],
    queryFn: () => cdApi.versions(system),
    refetchInterval: 30_000,
  });
  if (envs.isLoading || versions.isLoading) return <p className="hint">loading…</p>;
  if (envs.error || versions.error) return <p className="err">{String(envs.error ?? versions.error)}</p>;
  const rows = envs.data?.environments ?? [];
  const byName = new Map(rows.map((r) => [r.environment, r]));
  const all = versions.data?.versions.map((x) => x.version) ?? [];

  // "On the first environment, not yet on the second": the merges a
  // promotion would carry. Only between the first two environments, which
  // is where promotion happens.
  const [first, second] = cfg.environments;
  const pending = second ? mergesSince(all, byName.get(second)?.live_version) : null;

  const strips = cfg.environments.filter((env, i) => i > 0 && cfg.promotable.includes(env));

  return (
    <>
      {strips.map((env) => (
        <ReleaseStrip key={env} env={env} source={cfg.environments[cfg.environments.indexOf(env) - 1]} system={system} cfg={cfg} versions={versions.data?.versions.map((x) => x.version) ?? []} />
      ))}
      <div className="cd-pipeline">
        {cfg.environments.map((env, i) => (
          <React.Fragment key={env}>
            {i > 0 && (
              <div className="cd-gap" aria-hidden="true">
                <svg viewBox="0 0 60 16">
                  <path d="M2 8 H52" stroke="currentColor" strokeWidth="2" fill="none" />
                  <path d="M50 3 L58 8 L50 13 z" fill="currentColor" />
                </svg>
                {i === 1 && pending && (
                  <span className="hint">
                    <b>{pending.length}</b> merge{pending.length === 1 ? "" : "s"}
                    <br />
                    not on {second}
                  </span>
                )}
              </div>
            )}
            <EnvCard
              env={env}
              row={byName.get(env)}
              system={system}
              cfg={cfg}
              promotable={cfg.promotable.includes(env)}
              source={i > 0 ? cfg.environments[i - 1] : null}
              versions={versions.data?.versions ?? []}
            />
          </React.Fragment>
        ))}
      </div>

      {second && (
        <section>
          <h2>
            On {first}, not yet on {second}
          </h2>
          {pending === null ? (
            <p className="hint">
              Not known: {second} runs {byName.get(second)?.live?.version ?? "nothing recorded"}, which names no merge it was
              released from. Once a release cut by the pipeline is live on {second}, the merges after it are listed here.
            </p>
          ) : pending.length === 0 ? (
            <p className="hint">Nothing — {second} is up to date with {first}.</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>version</th>
                  <th>pull request</th>
                  <th>merged</th>
                </tr>
              </thead>
              <tbody>
                {pending.map((v) => (
                  <tr key={v.version}>
                    <td>
                      <VersionLink version={v.version} system={system} cfg={cfg} />
                    </td>
                    <td>
                      <PrCell v={v} />
                    </td>
                    <td className="hint" title={v.merged_at ?? ""}>
                      {v.merged_at ? `${ago(v.merged_at)} ago` : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      )}
    </>
  );
}

function EnvCard({
  env,
  row,
  system,
  cfg,
  promotable,
  source,
  versions,
}: {
  env: string;
  row: CdEnvironment | undefined;
  system: string;
  cfg: CdConfig;
  promotable: boolean;
  source: string | null;
  versions: { version: CdVersionRow; where: CdVersionWhere[] }[];
}) {
  const live = row?.live ?? null;
  const latest = row?.latest ?? null;
  const lv = row?.live_version ?? null;
  const moving = latest && live && latest.id !== live.id && latest.version !== live.version ? latest : latest && !live ? latest : null;
  const pr = lv ? pullRequestOf(lv) : null;
  return (
    <div className={`cd-env cd-env-${env}`}>
      <div className="cd-envhead">
        <b>{env}</b>
        <span className="cd-envkind">{promotable ? "promotable" : lv?.kind === "merge" ? "every merge" : ""}</span>
      </div>
      <div className="cd-envbody">
        {live ? (
          <>
            <VersionLink version={live.version} system={system} cfg={cfg} />
            {lv?.kind === "merge" && pr && (
              <div className="cd-pr">
                <Link className="cd-prnum" to={`/deployments/pull-requests/${lv.prs[0]?.repo}/${pr.number}`}>
                  #{pr.number}
                </Link>
                <span className="cd-prtitle" title={pr.title}>
                  {pr.title}
                </span>
              </div>
            )}
            {lv?.kind === "release" && lv.released_from && (
              <div className="cd-row">
                <span className="l">Released from</span>
                <VersionLink version={lv.released_from} system={system} cfg={cfg} />
              </div>
            )}
            <div className="cd-row">
              <span className="l">Last confirmed live</span>
              <span title={live.at}>
                {when(live.at)} · {ago(live.at)}
              </span>
            </div>
            <div className="cd-row">
              <span className="l">By</span>
              <span>
                <HowCell row={live} />
              </span>
            </div>
            <div className="cd-row">
              <span className="l">Argo</span>
              <ArgoChip row={live} />
            </div>
            {row?.live_promotion_replay && (
              <div className="cd-row">
                <span className="l" title={`This version replayed against what ${row.next_environment ?? "the next environment"} runs now`}>
                  Promotion replay vs {row.live_promotion_replay.against_version}
                </span>
                <PromotionReplayChip replay={row.live_promotion_replay} />
              </div>
            )}
            {live.state === "rolled_back" && (
              <div className="cd-row">
                <span className="l">Note</span>
                <span>a return to an earlier version</span>
              </div>
            )}
          </>
        ) : (
          <span className="hint">nothing recorded yet</span>
        )}
        {moving && (
          <div className="cd-inflight">
            <b>
              {moving.version} · {moving.state.replace("_", " ")}
            </b>
            <span>
              since {when(moving.at)} · <HowCell row={moving} />
            </span>
          </div>
        )}
      </div>
      {promotable && source && <Promotions env={env} source={source} system={system} cfg={cfg} targetLive={row?.live_version ?? null} versions={versions} />}
      <div className="cd-envfoot">
        <Link className="btn" to={href(`/deployments/environments/${encodeURIComponent(env)}`, system, cfg)}>
          History
        </Link>
      </div>
    </div>
  );
}

// ---- promotion ----------------------------------------------------------------

/** Whether the signed-in person may promote to `env`: `<env>_upgrader`, or in
 *  beta a maintainer. The API decides; this only decides what is drawn. */
function useMayPromote(env: string): boolean {
  const me = useMe();
  const roles = me.data?.roles ?? [];
  return !!me.data?.authenticated && (roles.includes(`${env}_upgrader`) || roles.includes("maintainer"));
}

/** The promotable card's promotion section: the request in flight with its
 *  step, or the Promote control. */
function Promotions({
  env,
  source,
  system,
  cfg,
  targetLive,
  versions,
}: {
  env: string;
  source: string;
  system: string;
  cfg: CdConfig;
  targetLive: CdVersionRow | null;
  versions: { version: CdVersionRow; where: CdVersionWhere[] }[];
}) {
  const may = useMayPromote(env);
  const me = useMe();
  const qc = useQueryClient();
  const [open, setOpen] = React.useState(false);
  const q = useQuery({ queryKey: ["cd", "promotions", system, env], queryFn: () => cdApi.promotions(system, env), refetchInterval: 10_000 });
  const current = (q.data ?? []).find(inFlight) ?? null;
  const last = !current ? (q.data ?? [])[0] ?? null : null;
  const refresh = () => {
    void qc.invalidateQueries({ queryKey: ["cd", "promotions", system, env] });
    void qc.invalidateQueries({ queryKey: ["cd", "environments", system] });
  };
  const cancel = useMutation({ mutationFn: (id: number) => cdApi.cancelPromotion(id), onSuccess: refresh });
  const fail = useMutation({ mutationFn: ({ id, reason }: { id: number; reason: string }) => cdApi.markPromotionFailed(id, reason), onSuccess: refresh });
  const maintainer = (me.data?.roles ?? []).includes("maintainer");
  return (
    <div className="cd-promo">
      {current ? (
        <div className="cd-promoreq">
          <div>
            <b>
              Promoting <VersionLink version={current.from_version} system={system} cfg={cfg} />
            </b>
            {current.release && (
              <>
                {" as "}
                <VersionLink version={current.release} system={system} cfg={cfg} />
              </>
            )}
          </div>
          <ol className="cd-steps">
            {promotionProgress(current).map((s) => (
              <li key={s.step} className={s.at}>
                {s.step}
              </li>
            ))}
          </ol>
          <div className="hint">
            <Link to={href(`/deployments/promotions/${current.id}`, system, cfg)}>#{current.id}</Link> by {current.requested_by} · {agoText(current.created_at)}
            {current.note ? ` · ${current.note}` : ""}
          </div>
          <div className="cd-promoactions">
            {current.state === "pending" && (
              <button className="btn" disabled={cancel.isPending} onClick={() => cancel.mutate(current.id)}>
                Cancel
              </button>
            )}
            {current.state !== "pending" && maintainer && (
              <button
                className="btn"
                disabled={fail.isPending}
                title="For a request the promote job will not finish: frees the environment for the next one"
                onClick={() => {
                  const reason = window.prompt("Why mark this promotion failed?");
                  if (reason && reason.trim()) fail.mutate({ id: current.id, reason: reason.trim() });
                }}
              >
                Mark failed
              </button>
            )}
          </div>
          {(cancel.error || fail.error) && <div className="err">{String(cancel.error ?? fail.error)}</div>}
        </div>
      ) : (
        <>
          {last && (
            <div className="hint">
              Last: #{last.id} {last.from_version}
              {last.release ? ` as ${last.release}` : ""} · <span className={`chip ${last.state === "live" ? "ok" : last.state === "failed" ? "failed" : "muted"}`}>{last.state}</span>
              {last.state === "failed" && (last.step_detail.failed?.reason ? ` · ${last.step_detail.failed.reason}` : "")}
            </div>
          )}
          {may && (
            <button className="btn primary" onClick={() => setOpen(true)}>
              Promote to {env}
            </button>
          )}
        </>
      )}
      {open && (
        <PromoteDialog
          env={env}
          source={source}
          system={system}
          candidates={promotionCandidates(versions, source, targetLive)}
          onClose={() => setOpen(false)}
          onDone={() => {
            setOpen(false);
            refresh();
          }}
        />
      )}
    </div>
  );
}

function PromoteDialog({
  env,
  source,
  system,
  candidates,
  onClose,
  onDone,
}: {
  env: string;
  source: string;
  system: string;
  candidates: CdVersionRow[];
  onClose: () => void;
  onDone: () => void;
}) {
  const [pick, setPick] = React.useState(candidates[0]?.version ?? "");
  const [note, setNote] = React.useState("");
  const check = useQuery({
    queryKey: ["cd", "promotion-check", system, env, pick],
    queryFn: () => cdApi.promotionCheck(system, env, pick),
    enabled: !!pick,
  });
  const go = useMutation({ mutationFn: () => cdApi.promote(system, env, pick, note.trim()), onSuccess: onDone });
  const picked = candidates.find((c) => c.version === pick);
  const riding = candidates.slice(candidates.findIndex((c) => c.version === pick));
  return (
    <div className="cd-dialog" role="dialog" aria-modal="true" aria-labelledby="cd-promote-title">
      <div className="cd-dialogbox">
        <h2 id="cd-promote-title">
          Promote to {env} <span className="beta">beta</span>
        </h2>
        {candidates.length === 0 ? (
          <p className="hint">No merge has reached {source} since what {env} runs.</p>
        ) : (
          <>
            <label htmlFor="cd-promote-pick">
              Version, from what ran on {source}. It and every merge before it go to {env}; merges after it stay on {source}.
            </label>
            <select id="cd-promote-pick" value={pick} onChange={(e) => setPick(e.target.value)}>
              {candidates.map((c) => (
                <option key={c.version} value={c.version}>
                  {c.version} · {carries(c)}
                </option>
              ))}
            </select>
            {picked && (
              <p className="hint">
                {riding.length === 1 ? `Only this merge goes to ${env}.` : `${riding.length} merges go to ${env}, this one and the ${riding.length - 1} before it.`} A release (CalVer tag on {picked.version}) is cut if
                none exists yet, then deployed and synced.
              </p>
            )}
            <h3>Checks</h3>
            {check.isLoading && <p className="hint">checking…</p>}
            {check.error && <p className="err">{String(check.error)}</p>}
            {check.data && (
              <ul className="cd-checks">
                {check.data.checks.map((c) => (
                  <li key={c.id} className={c.ok ? "ok" : "no"}>
                    <span className={`chip ${c.ok ? "ok" : "failed"}`}>{c.ok ? "pass" : "fail"}</span> {c.why}
                  </li>
                ))}
              </ul>
            )}
            <label htmlFor="cd-promote-note">Note (optional, shown on the request)</label>
            <input id="cd-promote-note" value={note} onChange={(e) => setNote(e.target.value)} placeholder="e.g. QA signed off the avs check" />
            {go.error && <p className="err">{String(go.error)}</p>}
          </>
        )}
        <div className="cd-promoactions">
          <button className="btn primary" disabled={!check.data?.ok || go.isPending} onClick={() => go.mutate()}>
            {go.isPending ? "Promoting…" : `Promote ${pick || ""}`}
          </button>
          <button className="btn" onClick={onClose}>
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}

// ---- the release strip -------------------------------------------------------

/** A promotable environment's releases in order: the two before the live
 *  one, the live one, the request moving, and what comes next. */
function ReleaseStrip({ env, source, system, cfg, versions }: { env: string; source: string; system: string; cfg: CdConfig; versions: CdVersionRow[] }) {
  const history = useQuery({
    queryKey: ["cd", "environment", system, env],
    queryFn: () => cdApi.environmentDeployments(system, env),
    refetchInterval: 15_000,
  });
  const promos = useQuery({ queryKey: ["cd", "promotions", system, env], queryFn: () => cdApi.promotions(system, env), refetchInterval: 10_000 });
  if (!history.data || !promos.data) return null;
  const cells = releaseStrip(history.data, versions, promos.data, source);
  return (
    <section className="cd-strippanel" aria-label={`${env} releases`}>
      <div className="cd-striphead">
        <h2>{env} releases</h2>
        <span className="hint">earlier → live → moving → next</span>
        <Link className="btn" to={href(`/deployments/environments/${encodeURIComponent(env)}`, system, cfg)}>
          History
        </Link>
      </div>
      <div className="cd-strip">
        {cells.map((c) => (
          <StripPlace key={c.role} c={c} system={system} cfg={cfg} />
        ))}
      </div>
    </section>
  );
}

const STRIP_HEADS: Record<StripCell["role"], string> = { earlier: "Earlier", previous: "Previous", live: "Live", promoting: "Promoting", next: "Next" };

function StripPlace({ c, system, cfg }: { c: StripCell; system: string; cfg: CdConfig }) {
  const row = c.row;
  const how = row ? howOf(row) : null;
  const top =
    c.request ? `Requested ${when(c.request.created_at)} · ${c.request.requested_by}`
    : row ? `${how?.kind === "job" ? "Promoted" : "Seen"} ${when(row.at)}`
    : c.role === "next" ? "Not requested"
    : "";
  const foot = () => {
    if (c.request) {
      return (
        <>
          <span className="chip pending">{c.note}</span>
          <Link className="hint" to={href(`/deployments/promotions/${c.request.id}`, system, cfg)}>
            #{c.request.id}
          </Link>
        </>
      );
    }
    if (c.role === "live" && row) {
      return (
        <>
          <ArgoChip row={row} />
          <span className="hint">{ago(row.at)}</span>
        </>
      );
    }
    if (row) {
      const [word, rest] = c.note.split(" · ");
      return (
        <>
          <span className={`chip ${row.state === "failed" ? "failed" : "muted"}`}>{word}</span>
          {rest && <span className="hint">{rest}</span>}
        </>
      );
    }
    if (c.role === "next" && !c.version) return <span className="hint">{c.note}</span>;
    return <span className={`chip ${c.role === "next" && c.merges ? "pending" : "muted"}`}>{c.note}</span>;
  };
  return (
    <div className={`cd-place cd-place-${c.role}`}>
      <div className="cd-placehead">{STRIP_HEADS[c.role]}</div>
      <div className="cd-placebody">
        <span className="hint">{top}</span>
        {c.version ? <VersionLink version={c.version} system={system} cfg={cfg} /> : <span className="cd-placeempty">—</span>}
        <span className="hint">
          {c.from ? (
            <>
              from <span className="mono">{c.from}</span>
              {c.merges != null ? ` · ${c.merges} merge${c.merges === 1 ? "" : "s"}` : ""}
            </>
          ) : c.role === "live" && row ? (
            c.note
          ) : (
            ""
          )}
        </span>
        <div className="cd-placefoot">{foot()}</div>
      </div>
    </div>
  );
}

// ---- promotions ---------------------------------------------------------------

export function PromotionsPage() {
  return (
    <Frame title="Deployments">
      {(cfg, system) => <PromotionsList cfg={cfg} system={system} />}
    </Frame>
  );
}

/** What a request is doing, in a few words, for a list. */
function requestWord(p: CdPromotion): { word: string; tone: string } {
  if (p.state === "live") return { word: "live", tone: "ok" };
  if (p.state === "failed") return { word: "failed", tone: "failed" };
  if (p.state === "cancelled") return { word: "cancelled", tone: "muted" };
  return { word: promotionPhrase(p), tone: "pending" };
}

function PromotionsList({ cfg, system }: { cfg: Extract<CdConfig, { enabled: true }>; system: string }) {
  const q = useQuery({ queryKey: ["cd", "promotions", system, "all"], queryFn: () => cdApi.promotionsAll(system, 100), refetchInterval: 10_000 });
  if (q.isLoading) return <p className="hint">loading…</p>;
  if (q.error) return <p className="err">{String(q.error)}</p>;
  const rows = q.data ?? [];
  const running = rows.filter(inFlight);
  return (
    <>
      <p className="hint">
        {running.length === 0 ? "Nothing moving." : `${running.length} moving.`} Every request made with Promote, newest first; open one for its steps.
      </p>
      {rows.length === 0 ? (
        <div className="empty">
          <div className="et">No promotions yet</div>
          <p className="hint">Promote on the pipeline page writes one; the promote job picks it up and posts each step here.</p>
        </div>
      ) : (
        <table className="cd-promotions">
          <thead>
            <tr>
              <th>request</th>
              <th>to</th>
              <th>version</th>
              <th>as</th>
              <th>state</th>
              <th>by</th>
              <th>when</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((p) => {
              const { word, tone } = requestWord(p);
              return (
                <tr key={p.id}>
                  <td>
                    <Link to={href(`/deployments/promotions/${p.id}`, system, cfg)}>#{p.id}</Link>
                  </td>
                  <td>{p.environment}</td>
                  <td>
                    <VersionLink version={p.from_version} system={system} cfg={cfg} />
                  </td>
                  <td>{p.release ? <VersionLink version={p.release} system={system} cfg={cfg} /> : <span className="hint">—</span>}</td>
                  <td>
                    <span className={`chip ${tone}`}>{word}</span>
                    {p.state === "failed" && p.step_detail.failed?.reason && (
                      <span className="hint" title={p.step_detail.failed.reason}>
                        {" "}
                        {p.step_detail.failed.reason.length > 60 ? p.step_detail.failed.reason.slice(0, 60) + "…" : p.step_detail.failed.reason}
                      </span>
                    )}
                  </td>
                  <td>
                    {p.requested_by}
                    {p.note && <div className="hint">{p.note}</div>}
                  </td>
                  <td className="hint" title={p.created_at}>
                    {when(p.created_at)} · {agoText(p.created_at)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </>
  );
}

// ---- one promotion, step by step ---------------------------------------------

export function PromotionPage() {
  const { id = "" } = useParams();
  const n = Number(id);
  return (
    <Frame
      title="Deployments"
      crumb={
        <>
          <Link to="/deployments/promotions">Promotions</Link>
          <span>/</span>#{id}
        </>
      }
    >
      {(cfg, system) => (Number.isFinite(n) ? <PromotionDetail cfg={cfg} system={system} id={n} /> : <p className="err">not a request id</p>)}
    </Frame>
  );
}

const STEP_TONE: Record<RailStep["at"], { word: string; tone: string }> = {
  done: { word: "done", tone: "ok" },
  now: { word: "running", tone: "pending" },
  todo: { word: "not yet", tone: "muted" },
  failed: { word: "failed", tone: "failed" },
  cancelled: { word: "cancelled", tone: "muted" },
};

function StepMark({ s, index }: { s: RailStep; index: number }) {
  const text = s.at === "done" ? "✓" : s.at === "failed" ? "✕" : s.at === "cancelled" ? "–" : String(index + 1);
  return (
    <span className={`cd-si ${s.at}`} aria-hidden="true">
      {text}
    </span>
  );
}

function PromotionDetail({ cfg, system, id }: { cfg: Extract<CdConfig, { enabled: true }>; system: string; id: number }) {
  const q = useQuery({ queryKey: ["cd", "promotion", id], queryFn: () => cdApi.promotion(id), refetchInterval: 10_000 });
  const versions = useQuery({ queryKey: ["cd", "versions", system], queryFn: () => cdApi.versions(system), refetchInterval: 30_000 });
  const me = useMe();
  const qc = useQueryClient();
  const [sel, setSel] = React.useState<string | null>(null);
  const refresh = () => {
    void qc.invalidateQueries({ queryKey: ["cd", "promotion", id] });
    void qc.invalidateQueries({ queryKey: ["cd", "promotions"] });
    void qc.invalidateQueries({ queryKey: ["cd", "environments", system] });
  };
  const cancel = useMutation({ mutationFn: () => cdApi.cancelPromotion(id), onSuccess: refresh });
  const fail = useMutation({ mutationFn: (reason: string) => cdApi.markPromotionFailed(id, reason), onSuccess: refresh });
  if (q.isLoading || versions.isLoading) return <p className="hint">loading…</p>;
  if (q.error) return (q.error as ApiError).status === 404 ? <div className="empty"><div className="et">No such request</div></div> : <p className="err">{String(q.error)}</p>;
  if (versions.error) return <p className="err">{String(versions.error)}</p>;
  if (!q.data) return null;
  const p = q.data;
  const rail = promotionRail(p);
  const steps = rail.flatMap((g) => g.steps);
  const current = steps.find((s) => s.step === sel) ?? promotionFocus(rail);
  const all = versions.data?.versions ?? [];
  const byName = new Map(all.map((x) => [x.version.version, x]));
  const ev = p.evidence as { checks?: CdPromotionCheck[]; target_live?: string | null; from?: string };
  const targetLive = ev.target_live ? byName.get(ev.target_live)?.version ?? null : null;
  const carried = carriedBy(all.map((x) => x.version), p.from_version, targetLive);
  const maintainer = (me.data?.roles ?? []).includes("maintainer");
  const mine = me.data?.authenticated && me.data.email === p.requested_by;
  const activity = promotionActivity(p);
  const { word, tone } = STEP_TONE[current.at];
  const d = current.detail;
  const before = steps[steps.indexOf(current) - 1]?.detail?.at ?? null;
  const running = PROMOTION_STEP_RUNNING[current.step];
  const title = current.at === "now" ? running.charAt(0).toUpperCase() + running.slice(1) : current.label;
  return (
    <div className="cd-prom">
      <aside className="cd-srail" aria-label="Promotion steps">
        <div className="cd-srailhead">
          <VersionLink version={p.release ?? p.from_version} system={system} cfg={cfg} />
          <span className="hint">
            from <VersionLink version={p.from_version} system={system} cfg={cfg} /> → {p.environment}
          </span>
          {ev.target_live && <span className="hint">replaces {ev.target_live}</span>}
        </div>
        {rail.map((g) => (
          <div className="cd-sgrp" key={g.title}>
            <span className="cd-cap">{g.title}</span>
            {g.steps.map((s) => (
              <button
                key={s.step}
                type="button"
                className={`cd-sitem ${s.at}${s.step === current.step ? " on" : ""}`}
                aria-current={s.step === current.step ? "step" : undefined}
                onClick={() => setSel(s.step)}
              >
                <StepMark s={s} index={steps.indexOf(s)} />
                <span>{s.label}</span>
                <span className="hint">{s.detail?.at ? when(s.detail.at) : s.at === "now" && steps.indexOf(s) > 0 && steps[steps.indexOf(s) - 1].detail?.at ? ago(steps[steps.indexOf(s) - 1].detail!.at!) : ""}</span>
              </button>
            ))}
          </div>
        ))}
        <div className="cd-srailfoot">
          {p.state === "pending" && (mine || maintainer) && (
            <button className="btn" disabled={cancel.isPending} onClick={() => cancel.mutate()}>
              Cancel this promotion
            </button>
          )}
          {inFlight(p) && p.state !== "pending" && maintainer && (
            <button
              className="btn"
              disabled={fail.isPending}
              title="For a request the promote job will not finish: frees the environment for the next one"
              onClick={() => {
                const reason = window.prompt("Why mark this promotion failed?");
                if (reason && reason.trim()) fail.mutate(reason.trim());
              }}
            >
              Mark failed
            </button>
          )}
          <span className="hint">
            {p.state === "pending"
              ? "Cancel works while the request is pending, before the job picks it up."
              : inFlight(p)
                ? "Once picked, the job finishes or fails the request; a maintainer can end one the job will not finish."
                : p.finished_at
                  ? `Ended ${when(p.finished_at)}.`
                  : ""}
          </span>
          {(cancel.error || fail.error) && <div className="err">{String(cancel.error ?? fail.error)}</div>}
        </div>
      </aside>

      <div className="cd-pane">
        <h2 className="cd-panetitle">
          {title} <span className={`chip ${tone}`}>{word}</span>
        </h2>
        <dl className="cd-facts">
          {d?.at && (
            <div>
              <dt>when</dt>
              <dd title={d.at}>
                {when(d.at)} · {agoText(d.at)}
              </dd>
            </div>
          )}
          {d?.by && (
            <div>
              <dt>by</dt>
              <dd>{d.by}</dd>
            </div>
          )}
          {(d?.jenkins_build || (current.step !== "pending" && p.picked_by_build)) && (
            <div>
              <dt>jenkins</dt>
              <dd className="mono">{d?.jenkins_build ?? p.picked_by_build}</dd>
            </div>
          )}
          {current.step === "tagged" && p.release && (
            <div>
              <dt>tag</dt>
              <dd>
                <span className="mono">{p.release}</span> on <span className="mono">{p.from_version}</span>
              </dd>
            </div>
          )}
          {current.step === "built" && p.release && (
            <div>
              <dt>image · chart</dt>
              <dd className="mono">{byName.get(p.release)?.version.image_tag ?? p.release} · {byName.get(p.release)?.version.chart ?? "—"}</dd>
            </div>
          )}
          {current.step === "pending" && p.note && (
            <div>
              <dt>note</dt>
              <dd>{p.note}</dd>
            </div>
          )}
          {d?.reason && (
            <div>
              <dt>reason</dt>
              <dd className="cd-reason">{d.reason}</dd>
            </div>
          )}
          {!d && current.at === "now" && before && (
            <div>
              <dt>since</dt>
              <dd title={before}>
                {when(before)} · {ago(before)}
              </dd>
            </div>
          )}
          {!d && current.at === "todo" && <div className="hint">Not reached.</div>}
        </dl>

        <section>
          <h2>What this promotion carries</h2>
          {carried === null ? (
            <p className="hint">
              Not known: {ev.target_live ? `${ev.target_live}, which ${p.environment} ran, names no merge it was cut on` : `nothing was recorded on ${p.environment} when it was requested`}.
            </p>
          ) : (
            <>
              <p className="hint">
                {carried.length} merge{carried.length === 1 ? "" : "s"} since {ev.target_live}, newest first.
              </p>
              <table>
                <thead>
                  <tr>
                    <th>version</th>
                    <th>pull request</th>
                    <th>promotion replay</th>
                    <th>merged</th>
                  </tr>
                </thead>
                <tbody>
                  {carried.map((v) => (
                    <tr key={v.version}>
                      <td>
                        <VersionLink version={v.version} system={system} cfg={cfg} />
                      </td>
                      <td>
                        <PrCell v={v} />
                      </td>
                      <td>
                        <PromotionReplayChip replay={byName.get(v.version)?.promotion_replay} />
                      </td>
                      <td className="hint" title={v.merged_at ?? ""}>
                        {v.merged_at ? when(v.merged_at) : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </section>

        <div className="cd-two">
          <section>
            <h2>Activity</h2>
            <ol className="cd-events">
              {activity.map((e, i) => (
                <li key={i}>
                  <span className="hint" title={e.at}>
                    {when(e.at)}
                  </span>
                  <span>
                    <b>{e.by}</b> {e.what}
                  </span>
                </li>
              ))}
            </ol>
          </section>
          <section>
            <h2>Checks at press</h2>
            {ev.checks?.length ? (
              <ul className="cd-checks">
                {ev.checks.map((c) => (
                  <li key={c.id} className={c.ok ? "ok" : "no"}>
                    <span className={`chip ${c.ok ? "ok" : "failed"}`}>{c.ok ? "pass" : "fail"}</span> {c.why}
                  </li>
                ))}
              </ul>
            ) : (
              <p className="hint">Not recorded.</p>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

// ---- versions ---------------------------------------------------------------

export function VersionsPage() {
  return (
    <Frame title="Deployments">
      {(cfg, system) => <Versions cfg={cfg} system={system} />}
    </Frame>
  );
}

function Versions({ cfg, system }: { cfg: Extract<CdConfig, { enabled: true }>; system: string }) {
  const [kind, setKind] = React.useState<"" | "release" | "merge">("");
  const q = useQuery({ queryKey: ["cd", "versions", system], queryFn: () => cdApi.versions(system), refetchInterval: 30_000 });
  const envs = useQuery({ queryKey: ["cd", "environments", system], queryFn: () => cdApi.environments(system), refetchInterval: 15_000 });
  if (q.isLoading || envs.isLoading) return <p className="hint">loading…</p>;
  if (q.error || envs.error) return <p className="err">{String(q.error ?? envs.error)}</p>;
  const observed = envs.data?.environments.map((e) => ({ environment: e.environment, version: e.live?.version ?? null })) ?? [];
  const rows = (q.data?.versions ?? []).filter((x) => !kind || x.version.kind === kind);
  return (
    <>
      <div className="runfilters">
        <select value={kind} onChange={(e) => setKind(e.target.value as typeof kind)} aria-label="filter by kind">
          <option value="">all versions</option>
          <option value="release">releases</option>
          <option value="merge">merges</option>
        </select>
        <span className="hint">{rows.length} shown · newest first</span>
      </div>
      {rows.length === 0 ? (
        <div className="empty">
          <div className="et">No versions yet</div>
          <p className="hint">The pipeline posts one for every merge it deploys and every release it cuts.</p>
        </div>
      ) : (
        <table className="cd-versions">
          <thead>
            <tr>
              <th>version</th>
              <th>kind</th>
              <th>carries</th>
              <th>{cfg.environments.join(" · ")}</th>
              <th>promotion replay</th>
              <th>merged</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(({ version: v, where, promotion_replay }) => (
              <tr key={v.version}>
                <td>
                  <VersionLink version={v.version} system={system} cfg={cfg} />
                </td>
                <td>
                  <span className={`chip ${v.kind === "release" ? "inconclusive" : "muted"}`}>{v.kind}</span>
                </td>
                <td>
                  <PrCell v={v} />
                </td>
                <td>
                  <WhereBar where={where} environments={cfg.environments} observed={observed} />
                </td>
                <td>
                  <PromotionReplayChip replay={promotion_replay} />
                </td>
                <td className="hint" title={v.merged_at ?? v.created_at}>
                  {when(v.merged_at ?? v.created_at)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}

function placeLabel(p: { state: string | null; lastConfirmed: boolean }): string {
  if (p.lastConfirmed) return "last confirmed live";
  if (p.state && isRunning(p.state)) return "previously live";
  return p.state?.replace("_", " ") ?? "never";
}

/** One segment per environment: solid for its last confirmed live version,
 *  faded where the version was or is on its way, empty where it never was. */
function WhereBar({ where, environments, observed = [] }: { where: CdVersionWhere[]; environments: string[]; observed?: { environment: string; version: string | null }[] }) {
  const places = placesOf(where, environments, observed);
  return (
    <span className="cd-where" aria-label={places.map((p) => `${p.environment}: ${placeLabel(p)}`).join(", ")}>
      {places.map((p) => (
        <span
          key={p.environment}
          className={`cd-seg cd-seg-${p.environment} ${p.present ? "on" : p.state ? "was" : ""}`}
          title={`${p.environment}: ${placeLabel(p)}`}
        >
          {p.lastConfirmed ? `${p.environment} · last confirmed live` : p.state ? `${p.environment} · ${placeLabel(p)}` : p.environment}
        </span>
      ))}
    </span>
  );
}

// ---- one version --------------------------------------------------------------

export function VersionPage() {
  const { version = "" } = useParams();
  return (
    <Frame
      title={<span className="mono">{version}</span>}
      crumb={
        <>
          <Link to="/deployments/versions">Versions</Link>
          <span>/</span>
          {version}
        </>
      }
    >
      {(cfg, system) => <VersionDetail cfg={cfg} system={system} version={version} />}
    </Frame>
  );
}

function VersionDetail({ cfg, system, version }: { cfg: Extract<CdConfig, { enabled: true }>; system: string; version: string }) {
  const q = useQuery({ queryKey: ["cd", "version", system, version], queryFn: () => cdApi.version(system, version), refetchInterval: 15_000 });
  const envs = useQuery({ queryKey: ["cd", "environments", system], queryFn: () => cdApi.environments(system), refetchInterval: 15_000 });
  if (q.isLoading || envs.isLoading) return <p className="hint">loading…</p>;
  if (q.error || envs.error) return <p className="err">{String(q.error ?? envs.error)}</p>;
  if (!q.data) return null;
  const v = q.data.version;
  const rows = q.data.deployments;
  const seen = new Set<string>();
  const now = rows.filter((r) => (seen.has(r.environment) ? false : (seen.add(r.environment), true)));

  return (
    <>
      <dl className="cd-facts">
        <div>
          <dt>kind</dt>
          <dd>
            <span className={`chip ${v.kind === "release" ? "inconclusive" : "muted"}`}>{v.kind}</span>
          </dd>
        </div>
        {v.released_from && (
          <div>
            <dt>released from</dt>
            <dd>
              <VersionLink version={v.released_from} system={system} cfg={cfg} />
            </dd>
          </div>
        )}
        {v.commit_sha && (
          <div>
            <dt>commit</dt>
            <dd className="mono">{v.commit_sha}</dd>
          </div>
        )}
        {v.image_tag && (
          <div>
            <dt>image</dt>
            <dd className="mono">{v.image_tag}</dd>
          </div>
        )}
        {v.chart && (
          <div>
            <dt>chart</dt>
            <dd className="mono">{v.chart}</dd>
          </div>
        )}
        <div>
          <dt>merged</dt>
          <dd>{v.merged_at ? when(v.merged_at) : "—"}</dd>
        </div>
      </dl>

      <section>
        <h2>Deployment history</h2>
        {now.length === 0 ? (
          <p className="hint">Not deployed anywhere yet.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>environment</th>
                <th>state</th>
                <th>since</th>
                <th>by</th>
              </tr>
            </thead>
            <tbody>
              {cfg.environments
                .map((e) => now.find((r) => r.environment === e))
                .filter((r): r is CdDeploymentRow => !!r)
                .map((r) => (
                  <tr key={r.id}>
                    <td>
                      <Link to={href(`/deployments/environments/${encodeURIComponent(r.environment)}`, system, cfg)}>{r.environment}</Link>
                    </td>
                    <td>
                      <StateChip state={r.state} />
                      {envs.data?.environments.find((e) => e.environment === r.environment)?.live?.version === v.version
                        ? <span className="hint"> · last confirmed live</span>
                        : null}
                    </td>
                    <td title={r.at}>{when(r.at)} · {ago(r.at)}</td>
                    <td>
                      <HowCell row={r} />
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
        )}
      </section>

      <section>
        <h2>{v.kind === "release" ? "Pull requests it carries" : "Pull request"}</h2>
        {v.prs.length === 0 ? (
          <p className="hint">None recorded.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>pull request</th>
                <th>author</th>
                <th>merged</th>
              </tr>
            </thead>
            <tbody>
              {v.prs.map((p) => (
                <tr key={`${p.repo}#${p.pr_number}`}>
                  <td>
                    <span className="cd-pr">
                      <Link className="cd-prnum" to={`/deployments/pull-requests/${p.repo}/${p.pr_number}`}>
                        #{p.pr_number}
                      </Link>
                      <span className="cd-prtitle">{p.title ?? ""}</span>
                    </span>
                  </td>
                  <td>{p.author ?? "—"}</td>
                  <td className="hint">{p.merged_at ? when(p.merged_at) : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section>
        <h2>Promotion replays</h2>
        <p className="hint">
          This version replayed against the release the next environment runs, both on one recording, and judged on what it
          changes between the two.
        </p>
        {q.data.promotion_replays.length === 0 ? (
          <p className="hint">None yet. The promotion-replay job runs one after the version goes live on the first environment.</p>
        ) : (
          // Only the newest row per release is the promotion replay as it stands; an older
          // `requested` row is history (the start of a promotion replay a later row ended).
          <table className="cd-promotion-replays">
            <thead>
              <tr>
                <th>against</th>
                <th>verdict</th>
                <th>state</th>
                <th>runs</th>
                <th>when</th>
                <th>by</th>
              </tr>
            </thead>
            <tbody>
              {q.data.promotion_replays.map((h, i) => (
                <tr key={h.id}>
                  <td>
                    {h.against_version === "-" ? <span className="hint">—</span> : <VersionLink version={h.against_version} system={system} cfg={cfg} />}
                  </td>
                  <td>
                    {h.state === "requested" && q.data.promotion_replays.findIndex((x) => x.against_version === h.against_version) < i ? (
                      <span className="chip muted">started</span>
                    ) : (
                      <PromotionReplayChip replay={h} />
                    )}
                  </td>
                  <td>
                    <span className="hint">{h.state}</span>
                    {h.reason && <div className="hint cd-replay-reason">{h.reason}</div>}
                  </td>
                  <td>
                    <PromotionReplayRuns replay={h} />
                  </td>
                  <td className="hint" title={h.at}>
                    {when(h.at)} · {ago(h.at)}
                  </td>
                  <td>
                    {h.requested_by}
                    {h.jenkins_build && (
                      <>
                        {" · "}
                        <a href={h.jenkins_build} target="_blank" rel="noreferrer">
                          build
                        </a>
                      </>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section>
        <h2>History</h2>
        <DeploymentRows rows={rows} system={system} cfg={cfg} showEnvironment />
      </section>
    </>
  );
}

// ---- one environment ---------------------------------------------------------

export function EnvironmentsPage() {
  return (
    <Frame title="Deployments">
      {(cfg, system) => (
        <section>
          <h2>Environments</h2>
          <ul className="cd-envlist">
            {cfg.environments.map((e) => (
              <li key={e}>
                <Link to={href(`/deployments/environments/${encodeURIComponent(e)}`, system, cfg)}>{e}</Link>
              </li>
            ))}
          </ul>
        </section>
      )}
    </Frame>
  );
}

export function EnvironmentPage() {
  const { env = "" } = useParams();
  return (
    <Frame
      title={env}
      crumb={
        <>
          <Link to="/deployments">Pipeline</Link>
          <span>/</span>
          {env}
        </>
      }
    >
      {(cfg, system) => <EnvironmentHistory cfg={cfg} system={system} env={env} />}
    </Frame>
  );
}

function EnvironmentHistory({ cfg, system, env }: { cfg: Extract<CdConfig, { enabled: true }>; system: string; env: string }) {
  const q = useQuery({
    queryKey: ["cd", "environment", system, env],
    queryFn: () => cdApi.environmentDeployments(system, env),
    refetchInterval: 15_000,
  });
  if (q.isLoading) return <p className="hint">loading…</p>;
  if (q.error) return <p className="err">{String(q.error)}</p>;
  const rows = q.data ?? [];
  const live = rows.find((r) => isRunning(r.state));
  return (
    <>
      {live && (
        <p className="hint">
          Runs <VersionLink version={live.version} system={system} cfg={cfg} /> since {when(live.at)} · <ArgoChip row={live} />
        </p>
      )}
      <section>
        <h2>Every change, newest first</h2>
        <DeploymentRows rows={rows} system={system} cfg={cfg} />
      </section>
    </>
  );
}

function DeploymentRows({ rows, system, cfg, showEnvironment = false }: { rows: CdDeploymentRow[]; system: string; cfg: CdConfig; showEnvironment?: boolean }) {
  if (rows.length === 0) return <p className="hint">Nothing recorded yet.</p>;
  return (
    <table className="cd-history">
      <thead>
        <tr>
          <th>when</th>
          {showEnvironment && <th>environment</th>}
          <th>version</th>
          <th>state</th>
          <th>how</th>
          <th>argo</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => {
          const d = r.detail as { reason?: unknown };
          return (
            <tr key={r.id}>
              <td title={r.at}>{when(r.at)}</td>
              {showEnvironment && (
                <td>
                  <Link to={href(`/deployments/environments/${encodeURIComponent(r.environment)}`, system, cfg)}>{r.environment}</Link>
                </td>
              )}
              <td>
                <VersionLink version={r.version} system={system} cfg={cfg} />
              </td>
              <td>
                <StateChip state={r.state} />
                {typeof d.reason === "string" && (
                  <span className="hint" title={d.reason}>
                    {" "}
                    {d.reason.length > 60 ? d.reason.slice(0, 60) + "…" : d.reason}
                  </span>
                )}
              </td>
              <td>
                <HowCell row={r} />
              </td>
              <td>
                <ArgoChip row={r} />
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// ---- one pull request --------------------------------------------------------

export function PullRequestPage() {
  const { owner = "", repo = "", number = "" } = useParams();
  const n = Number(number);
  const q = useQuery({
    queryKey: ["cd", "pr", owner, repo, n],
    queryFn: () => cdApi.pullRequest(owner, repo, n),
    enabled: !!owner && !!repo && Number.isFinite(n),
    refetchInterval: 30_000,
  });
  const cfg = useCdConfig();
  const environments = cfg.data?.enabled ? cfg.data.environments : [];
  const systems = [...new Set(q.data?.versions.map(({ version }) => version.system) ?? [])];
  const observations = useQueries({
    queries: systems.map((system) => ({
      queryKey: ["cd", "environments", system],
      queryFn: () => cdApi.environments(system),
      enabled: !!cfg.data?.enabled,
      refetchInterval: 15_000,
    })),
  });
  const observedBySystem = new Map(
    systems.flatMap((system, index) => {
      const result = observations[index];
      return result?.data
        ? [[system, result.data.environments.map((e) => ({ environment: e.environment, version: e.live?.version ?? null }))] as const]
        : [];
    }),
  );
  const title = q.data?.versions.flatMap((x) => x.version.prs).find((p) => p.pr_number === n)?.title ?? "";
  const observationsLoading = systems.some((_, index) => observations[index]?.isLoading);
  const observationsError = observations.find((result) => result?.error)?.error;
  return (
    <>
      <div className="crumb">
        <Link to="/deployments">Deployments</Link>
        <span>/</span>
        {owner}/{repo} #{number}
      </div>
      <h1>
        #{number} {title}
      </h1>
      <p className="hint">
        <a href={`https://github.com/${owner}/${repo}/pull/${number}`} target="_blank" rel="noreferrer">
          on GitHub
        </a>
        {" · "}
        <Link to="/runs">replays</Link>
      </p>
      {q.isLoading || cfg.isLoading || (!!q.data?.versions.length && observationsLoading) ? (
        <p className="hint">loading…</p>
      ) : q.error ? (
        (q.error as { status?: number }).status === 404 ? (
          <Off />
        ) : (
          <p className="err">{String(q.error)}</p>
        )
      ) : cfg.error ? (
        <p className="err">{String(cfg.error)}</p>
      ) : observationsError ? (
        <p className="err">{String(observationsError)}</p>
      ) : (q.data?.versions.length ?? 0) === 0 ? (
        <div className="empty">
          <div className="et">Not deployed anywhere yet</div>
          <p className="hint">No version the pipeline posted carries this pull request. A merge is posted when it deploys to the first environment.</p>
        </div>
      ) : (
        <section>
          <h2>Versions carrying this pull request</h2>
          <table>
            <thead>
              <tr>
                <th>as</th>
                <th>kind</th>
                <th>{environments.join(" · ")}</th>
                <th>where</th>
              </tr>
            </thead>
            <tbody>
              {q.data!.versions.map(({ version: v, where }) => {
                const observed = observedBySystem.get(v.system);
                return (
                  <tr key={`${v.system}/${v.version}`}>
                    <td>
                      <VersionLink version={v.version} system={v.system} cfg={cfg.data} />
                      {systems.length > 1 && <span className="hint"> · {v.system}</span>}
                    </td>
                    <td>
                      <span className={`chip ${v.kind === "release" ? "inconclusive" : "muted"}`}>{v.kind}</span>
                    </td>
                    <td>
                      <WhereBar where={where} environments={environments} observed={observed} />
                    </td>
                    <td className="hint">
                      {placesOf(where, environments, observed)
                        .filter((p) => p.state)
                        .map((p) => {
                          const row = where.find((w) => w.environment === p.environment)!;
                          return `${p.environment}: ${placeLabel(p)} (last event ${when(row.at)})`;
                        })
                        .join(" · ") || "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </section>
      )}
    </>
  );
}
