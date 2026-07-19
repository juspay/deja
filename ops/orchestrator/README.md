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
| candidate image pulls | DinD daemon needs ECR auth — run `docker login` against the sock at boot, or pre-provision `~/.docker/config.json` via the chart |

## Known-unverified (test before relying on it)

- The DinD daemon's ECR auth path (login lands in the APP container's docker
  config; the daemon pulls with it only when the CLI passes the creds —
  verify with a real `docker pull` through the sock).
- Compose relative-path resolution from `/workspace/repo` inside the pod
  (bind mounts resolve against the DinD daemon's filesystem — the shared
  workspace volume must be mounted at the SAME path in both containers,
  which the drafted chart does).
- Total image size (vendor tree ~224M + debian + docker CLI ≈ 500M+).
