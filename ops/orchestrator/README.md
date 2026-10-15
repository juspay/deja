# Replay-orchestrator image (Plan B: ArgoCD / in-cluster DinD)

Pairs with the drafted `helm-charts/charts/replay-orchestrator` chart in
`~/hyperswitch-infra` (see its `DEPLOY-NOTES.md` for the approval checklist,
Secrets, and the DinD egress caveat: DockerHub is NOT squid-whitelisted —
mirror `docker:27-dind` and any DockerHub compose images to ECR, or extend
`whitelisted-domains/`).

## Build (from the repo root)

```sh
# .dockerignore must exclude the heavy state: demo/* except the four bundled
# assets, vendor/*/target, vendor/*/.git, target/
docker build -f ops/orchestrator/Dockerfile \
  -t 223655089699.dkr.ecr.ap-south-1.amazonaws.com/hyperswitch-replay-orchestrator:2026.07.11.0 .
```

## Push (GATED — ask before pushing anywhere, including ECR)

```sh
aws ecr get-login-password --region ap-south-1 \
  | docker login --username AWS --password-stdin 223655089699.dkr.ecr.ap-south-1.amazonaws.com
# create the repo once: aws ecr create-repository --repository-name hyperswitch-replay-orchestrator --region ap-south-1
docker push 223655089699.dkr.ecr.ap-south-1.amazonaws.com/hyperswitch-replay-orchestrator:2026.07.11.0
```

## What the container expects at runtime (provided by the chart)

| thing | source |
|---|---|
| `/var/run/docker.sock` | shared emptyDir with the DinD sidecar |
| `/workspace/state` | shared workspace emptyDir (`HARNESS_STATE_DIR`) |
| `DEJA_S3_*` env | `replay-orchestrator-aws` Secret + ConfigMap |
| `DEJA_API_SERVICE_TOKEN` | `replay-orchestrator-api` Secret |
| `[auth]` block of the config (`DEJA_CONFIG_TOML` or `DEJA__AUTH__*`) | sign-in: `enabled`, `client_id`, `client_secret` + `session_secret` (Secrets, both required when enabled), `domain_allowlist` (`@`-prefixed suffixes), `roles.<role>` (ConfigMap, re-read every 15 s), `cookie_secure` (default true; false only for a plain-http dev server). A declared block that does not resolve refuses to start; `enabled` itself only changes with a restart (the 15 s reload moves the lists, never the switch). |
| candidate image pulls | DinD daemon needs ECR auth — run `docker login` against the sock at boot, or pre-provision `~/.docker/config.json` via the chart |


## Scheduled recording sealer

The chart's independent sealer CronJob runs `deja-compactor pass` every 30
minutes (`Forbid`, 1,500-second deadline), separate from any router restart.
It scans each declared system's S3 landing and indexes object fingerprints,
request ingress days, and source dependencies in the same Postgres database as
the orchestrator (`DEJA_DB_URL` is required). A long-lived pod contributes to
multiple UTC request-start-day views without rotating its session ID. The S3
`dt=` prefix locates raw objects; it does not decide request day. A cross-midnight
tail belongs to the request's ingress day, even if it arrives several partitions
later. Missing ingress refuses an exact day seal instead of assigning a guessed
date. Indexed day manifests have immutable database revisions and a transactional
current head; old whole-session `sessions/v1` manifests remain readable.
Same-count object replacement invalidates the affected source snapshot. Raw
source deletion marks the session incomplete and refuses a partial reseal; old
pinned day revisions remain readable until their immutable S3 parts expire.

Earlier draft smoke (before the Postgres index) against disposable MinIO plus API:
initial D7/D8 pass sealed both; a later D9 tail for D7 caused reseal (seal ID
`7a52560118d34354` → `8a404437ea48ee45`, events 1 → 2), D9 published a
terminal empty marker, and the next pass planned zero. In another session, a
late D7 ingress replaced D8's orphan-tail seal with an empty manifest (no parts)
and resealed D7 at three events. API checks returned D7 total 1/events 2 and an
empty group total 0; bare replay split and empty replay were refused. Grouped
replay persisted `recording_group` and ingested its member seal, but synthetic
tape lookup rendering failed (`trait_name` missing). Corrupt manifest returned
HTTP 502. This is local evidence only: no live sandbox smoke or deployment.
Separately, a terminal empty manifest in a named system's prefixed bucket
returned HTTP 200 from `/recordings/{id}/correlations` with `status=empty`,
zero correlations; the same ID in the wrong system returned HTTP 404.

Indexed local smoke against disposable MinIO and Postgres (not a deployment):
one unchanged Hyperswitch pod yielded D7/D8 seals and a no-change pass planned
zero; a D9 object carrying a late D8 tail resealed D8 while Prism's separate
bucket remained unchanged. Replacing one D8 object under the same key/count
changed the seal. An ingress timestamp on D8 stored under physical `dt=D9`
produced the D8 view. Actual HTTP `/available?group=…` returned only that
group's member; grouped replay ingested its pinned immutable seal, then the
synthetic fixture failed lookup rendering for missing `trait_name` (no full
replay claim). Deleting a previously indexed raw object marked that session
incomplete, removed its current group, and retained a separately admitted
Prism run's historical seal through `/runs/{id}/recording-snapshot`.
Additional local checks: two concurrent passes over one new Prism day produced
one immutable database revision; a stale source listing was rejected after
another worker indexed an object; and removing the final raw object tombstoned
its current view while retaining the historical seal. An indexed day and a
legacy whole-session member in the same saved `revision-MMDD` group replayed
once each, while a legacy-only group still ingested its S3 seal with Postgres
unavailable. A prefixed Prism bucket sealed and ingested from its own root.
The UI was rendered in Chromium; without a sandbox Kubernetes context it
correctly displayed the recommendation as unavailable rather than guessing
an active deployment. No live rollout/rollback was exercised.

Follow-up local HTTP smoke with disposable Postgres and tmpfs-backed MinIO:
`/recordings/{id}/correlations` returned the current indexed day's seal-specific
sidecar (HTTP 200, `status=sealed`, one correlation). After the source was marked
incomplete, the same request returned `status=unsealed` despite an old S3
manifest and sidecar remaining; a separate legacy-only day still returned its
S3 correlation index as sealed. The real-Postgres regression also confirms
that an orphan-only source object blocks day publication and hides existing
current heads until another object supplies its ingress. No Kubernetes
recommendation or full stage-two replay was exercised by this smoke.

An oversized recording is a named refusal in the pass ledger, not necessarily
a failed Job; the current refusal path can exit successfully. Alert on ledger
refusals/incomplete or missing pass output, not Job success alone.

Before rollout: this branch carries only PR #281's schema migrations
`0010_cd.sql` and `0011_cd_controls.sql` ahead of `0012_view_index.sql` so
Postgres applies them in order. This worktree does not import #281's CD API/UI;
integrating its code from main requires keeping the applied `0010`/`0011`
migration identities unchanged. Do not deploy the indexed sealer to a database
that has already applied different migrations under either number. The CD
deployment log has no verified live writer/observer in inspected infra, so
automatic recommendation reads configured Kubernetes serving pods and fails
closed on an ambiguous or unknown revision. ART's strict recommendation consumer must
deploy only after the API, database index/backfill, workload descriptors/RBAC,
and an eligible sealed day are present. No rollout is authorized by this note.

The index avoids full-session decoding once per day. Compressed source-object
reads stop after the configured byte ceiling, but materialization still retains
the selected day's decoded objects, lines, and collated events in memory. Verify
actual peak memory against the configured 4 GiB decoded allowance / 16 GiB pod
limit and alert on failed/refused day rows and missing pass summaries. Source
cleanup is a terminal incomplete state until backfill; it must never
masquerade as a smaller current tape.

Read-only verification against the checked-out infra chart (no cluster access):

```sh
grep -nE 'schedule:|concurrencyPolicy:|activeDeadlineSeconds:|quietSeconds:|maxLandingBytes:|memory:' \
  ~/hyperswitch-infra/helm-charts/charts/replay-orchestrator/{templates/sealer-cronjob.yaml,values.yaml}
grep -RniE 'restartPolicy|hourly|schedule:' ~/hyperswitch-infra/argo-sandbox/apps
```

Operationally, inspect per-record pass ledger outcomes (especially
`too_large`/failed rows), planned-versus-accounted totals, pass duration and
missed/overlapping ticks, and manifest reseal counts/IDs. No live metrics or
hourly restart registration was confirmed in the local Argo configuration
search; verify those in the target environment before relying on them.

## Known-unverified (test before relying on it)

- The DinD daemon's ECR auth path (login lands in the APP container's docker
  config; the daemon pulls with it only when the CLI passes the creds —
  verify with a real `docker pull` through the sock).
- Compose relative-path resolution from `/workspace/repo` inside the pod
  (bind mounts resolve against the DinD daemon's filesystem — the shared
  workspace volume must be mounted at the SAME path in both containers,
  which the drafted chart does).
- Total image size (vendor tree ~224M + debian + docker CLI ≈ 500M+).
