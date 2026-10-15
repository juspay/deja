# Replay-orchestrator image (Plan B: ArgoCD / in-cluster DinD)

Pairs with the drafted `helm-charts/charts/replay-orchestrator` chart in
`~/hyperswitch-infra` (see its `DEPLOY-NOTES.md` for the approval checklist,
Secrets, and the DinD egress caveat: DockerHub is NOT squid-whitelisted —
mirror `docker:27-dind` and any DockerHub compose images to ECR, or extend
`whitelisted-domains/`).

## Build (from the repo root)

Set `ORCHESTRATOR_IMAGE_REPO` to the deployment's ECR repository for this image
(not the candidate/router repository):

```sh
: "${ORCHESTRATOR_IMAGE_REPO:?Set ORCHESTRATOR_IMAGE_REPO to the orchestrator ECR repository URI}"
ORCHESTRATOR_IMAGE="${ORCHESTRATOR_IMAGE_REPO}:2026.07.11.0"
# .dockerignore must exclude the heavy state: demo/* except the four bundled
# assets, vendor/*/target, vendor/*/.git, target/
docker build -f ops/orchestrator/Dockerfile -t "$ORCHESTRATOR_IMAGE" .
```

## Push (GATED — ask before pushing anywhere, including ECR)

```sh
aws ecr get-login-password --region ap-south-1 \
  | docker login --username AWS --password-stdin "${ORCHESTRATOR_IMAGE_REPO%%/*}"
# create the repo once: aws ecr create-repository --repository-name hyperswitch-replay-orchestrator --region ap-south-1
docker push "$ORCHESTRATOR_IMAGE"
```

## What the container expects at runtime (provided by the chart)

| thing | source |
|---|---|
| `/var/run/docker.sock` | shared emptyDir with the DinD sidecar |
| `/workspace/state` | shared workspace emptyDir (`HARNESS_STATE_DIR`) |
| `DEJA_S3_*` env | `replay-orchestrator-aws` Secret + ConfigMap |
| `DEJA_API_SERVICE_TOKEN` | `replay-orchestrator-api` Secret |
| `[auth]` block of the config (`DEJA_CONFIG_TOML` or `DEJA__AUTH__*`) | sign-in: `enabled`, `client_id`, `client_secret` + `session_secret` (Secrets, both required when enabled), `domain_allowlist` (`@`-prefixed suffixes), `roles.<role>` (ConfigMap, re-read every 15 s), `cookie_secure` (default true; false only for a plain-http dev server). A declared block that does not resolve refuses to start; `enabled` itself only changes with a restart (the 15 s reload moves the lists, never the switch). |
| `[cd]` block of the config (`DEJA_CONFIG_TOML` or `DEJA__CD__*`) | deployment tracking: `systems` (default: the default system), `environments` in pipeline order (default `integ, sandbox, prod`), `promotable`. Read once at start. Absent: `/api/v1/cd/*` reads 404 and the pipeline's posts are refused. The pipeline posts with the service token plus `X-Deja-Actor`, like run events. |
| candidate image pulls | DinD daemon needs ECR auth — run `docker login` against the sock at boot, or pre-provision `~/.docker/config.json` via the chart |

CD reads require either a signed-in maintainer or the **configured** service
token. An arbitrary bearer is never accepted when that token is unset. The
Deployments pages distinguish historical rows from the last version confirmed
live by an observation; a pending or failed intent is not proof of what runs
now. Until a CD writer and Argo observer are deployed, use the live Kubernetes
provider for the current-version picker rather than treating CD history as
current deployment state.

Use one orchestrator writer per `HARNESS_STATE_DIR`: baseline re-points and
push-back events that rewrite a run record share an in-process lock, so a late
recording event cannot restore an older baseline. The lock is not distributed.

## Known-unverified (test before relying on it)

- The DinD daemon's ECR auth path (login lands in the APP container's docker
  config; the daemon pulls with it only when the CLI passes the creds —
  verify with a real `docker pull` through the sock).
- Compose relative-path resolution from `/workspace/repo` inside the pod
  (bind mounts resolve against the DinD daemon's filesystem — the shared
  workspace volume must be mounted at the SAME path in both containers,
  which the drafted chart does).
- Total image size (vendor tree ~224M + debian + docker CLI ≈ 500M+).
