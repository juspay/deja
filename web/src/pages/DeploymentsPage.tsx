// Deployments: what runs in each environment, how it got there, and where
// each version and pull request is. Every fact comes from the CD pipeline's
// posts and the reconcile job's observations, through `/api/v1/cd/*`.
// Promotion (phase 5) is not here yet, and no control for it is drawn until
// it is. The one control is the pause switch.
//
// BETA, for maintainers: the API answers a signed-in maintainer (or the
// pipeline's token) only, and the tab is drawn for maintainers only.
//
// Five views under /deployments, all keyed by `?system=` so a link carries
// the system it was read from:
//   /deployments                                   pipeline
//   /deployments/versions                          merges and releases
//   /deployments/versions/:version                 one version, its history
//   /deployments/environments/:env                 one environment's history
//   /deployments/pull-requests/:owner/:repo/:n     where a pull request is

import React from "react";
import { useMutation, useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useLocation, useParams, useSearchParams } from "react-router-dom";

import { loginUrl } from "../lib/me";
import {
  cdApi,
  type ApiError,
  type CdConfig,
  type CdDeploymentRow,
  type CdEnvironment,
  type CdPromotionReplayRow,
  type CdVersionRow,
  type CdVersionWhere,
} from "../lib/api";
import { ago, argoOf, carries, promotionReplayWord, howOf, isRunning, mergesSince, placesOf, pullRequestOf, stateTone, when } from "../lib/cd";

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

/** What someone who is not a maintainer sees: the reason, and the way in. */
function MaintainersOnly({ err }: { err: ApiError }) {
  const signIn = err.status === 401 && !String(err.message).includes("not configured");
  return (
    <div className="empty">
      <div className="et">
        Deployments <span className="beta">beta</span>
      </div>
      <p className="hint">
        Deployments is in beta and open to maintainers only.{" "}
        {err.status === 403
          ? "Your account is not on the maintainer list; ask to be added to it in the déjà configuration."
          : signIn
            ? "Sign in with a maintainer account."
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

/** The pipeline's pause switch: a banner while paused, a control either way.
 *  The API takes it from a signed-in maintainer only. */
function PauseSwitch({ cfg, system }: { cfg: Extract<CdConfig, { enabled: true }>; system: string }) {
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
          <b>Pipeline paused</b> for {system}. No merge is deployed to integ, and no promotion replay or release is started, until it
          is resumed. Reconcile keeps reporting what runs.
          <div className="hint">
            by {paused.changed_by} · {when(paused.changed_at)} · {ago(paused.changed_at) === "just now" ? "just now" : `${ago(paused.changed_at)} ago`}{paused.reason ? ` · ${paused.reason}` : ""}
          </div>
          {flip.error && <div className="err">{String(flip.error)}</div>}
        </div>
        <button className="btn primary" disabled={flip.isPending} onClick={() => flip.mutate(false)}>
          Resume
        </button>
      </div>
    );
  }
  return (
    <div className="cd-pause">
      {!asking ? (
        <>
          <span className="hint">Pipeline running for {system}.</span>
          <button className="btn" onClick={() => setAsking(true)}>
            Pause pipeline
          </button>
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
            Why pause? integ-deploy, promotion replays and releases stop before their next step; a step already running finishes.
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
  const sub = (path: string, label: string, exact = false) => {
    const on = exact ? here === path : here.startsWith(path);
    return (
      <Link className={on ? "cd-sub active" : "cd-sub"} to={href(path, system, cfg.data)}>
        {label}
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
  const title = [`against ${replay.against_version}`, replay.reason ?? ""].filter(Boolean).join(" · ");
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

  return (
    <>
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
            <EnvCard env={env} row={byName.get(env)} system={system} cfg={cfg} promotable={cfg.promotable.includes(env)} />
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

function EnvCard({ env, row, system, cfg, promotable }: { env: string; row: CdEnvironment | undefined; system: string; cfg: CdConfig; promotable: boolean }) {
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
                <span className="l" title="This version replayed against the release the next environment runs">
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
      <div className="cd-envfoot">
        <Link className="btn" to={href(`/deployments/environments/${encodeURIComponent(env)}`, system, cfg)}>
          History
        </Link>
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
