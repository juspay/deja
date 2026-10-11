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
minutes (`Forbid`, 1,500-second deadline); this is separate from
any hourly router Rollout restart and runs the same chart-selected image. It visits
each declared system. Date-partitioned sessions produce one recording per UTC
request-start day: ingress (`request_sequence` 0) assigns the whole request,
including a late tail in another partition, to its start day. A day with no
selected records gets a non-replayable empty manifest. Added or removed
landing objects replan the affected day, allowing late tails to replace its seal.
If sequence zero is missing, the earliest observed partition retains the
request; `lines_without_ingress` names those incomplete lines within `lines_in`.

Local smoke against disposable MinIO plus API: initial D7/D8 pass sealed both;
a later D9 tail for D7 caused reseal (seal ID
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

An oversized recording is a named refusal in the pass ledger, not necessarily
a failed Job; the current refusal path can exit successfully. Alert on ledger
refusals/incomplete or missing pass output, not Job success alone.

Before any rollout, gate on bounded streaming/indexed selection instead of
re-reading every session partition per day (currently O(D²)), per-day rather
than whole-session decoded-byte budgeting (a 512 MiB aggregate ceiling can
reject daily seals), producer routing by request start day, and conditional
(CAS) manifest publication to prevent an older concurrent scan replacing a
newer seal. These are unresolved design/implementation gates, not shipped
behavior. No deploy is authorized by this note.

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
