# Morning Demo Runbook

Operator target: a Linux host with Docker Compose, Rust/Cargo, Git, AWS CLI, `curl`, and `jq` available. Run all commands from the repository root unless a command explicitly changes directory.

This runbook supports three safe candidate flows for the morning demo:

1. **LocalPath / local binary flow through the orchestrator API/UI**: build a router binary from the vendored Hyperswitch tree, then submit a local binary path as the candidate.
2. **PrebuiltImage replay flow through the orchestrator API/UI**: current `deja-orchestrator` source resolves a non-empty image that is not `deja-demo` by running `docker pull`, recording it as the run candidate image, and passing it to Compose as `CANDIDATE_IMAGE`. The host must already be logged into ECR, and the operator must verify the image with `docker image inspect "$CANDIDATE_IMAGE"` before smoke.
3. **Manual Docker Compose image drop-in**: run the Hyperswitch replay service with `CANDIDATE_IMAGE=<operator-supplied image>` using `demo/overlays/hyperswitch/docker-compose.deja.yml`.

Mode boundary: use the demo/local image for **record** runs, then replay that recording with the Jenkins/ECR prebuilt image. Do not treat a Jenkins/ECR router image as a drop-in record image: `drive_record` executes `/workload.sh` inside `hyperswitch-server`, and vendor/Jenkins router images are not proven to contain that script. If you must record with a Jenkins image, add a record-only mount/command that supplies `/workload.sh` first.

The historical `deja-demo` candidate value is not an ECR image selection; it means the legacy local Compose build path. For a Jenkins/ECR image, use a real non-empty image URI or digest supplied by the operator.

Do not print secrets. Use interactive prompts for the Stripe key and S3 secret key.

## 0. Variables used below

```bash
# Repo-local paths.
export REPO_ROOT="$(pwd)"
export VENDOR="vendor/hyperswitch-deja-clean"
export BASE="$VENDOR/docker-compose.yml"
export OVERLAY="$REPO_ROOT/demo/overlays/hyperswitch/docker-compose.deja.yml"

# Demo API + state. Keep the state dir stable for the whole demo.
export HARNESS_BIND="127.0.0.1:8070"
export HARNESS_STATE_DIR="$REPO_ROOT/demo/harness-state/morning"
export API="http://127.0.0.1:8070"
export DEMO_PROJECT="deja-demo"
export DEMO_REPLAY_PORT="8090"
export RUN_TAG="morning-$(date +%Y%m%d-%H%M%S)"
export RECORDING_ID="rec-$RUN_TAG"

mkdir -p "$HARNESS_STATE_DIR"
```

If `8090` or `8070` is busy, choose another replay/API port before starting services:

```bash
export HARNESS_BIND="127.0.0.1:18070"
export API="http://127.0.0.1:18070"
export DEMO_REPLAY_PORT="18090"
```

## 1. Prerequisites and preflight

Required tools:

```bash
for tool in docker cargo git curl jq aws; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 1; }
done

docker compose version
cargo --version
aws --version
```

Required access:

- Docker daemon access for the current user.
- AWS credentials that can read/write `s3://hyperswitch-art` and read from ECR registry `223655089699.dkr.ecr.ap-south-1.amazonaws.com`.
- A Stripe **test** secret key for the record workload. The workload passes it on stdin and does not put it in the curl argument list.

Set the Stripe key without echoing it:

```bash
read -rsp 'Stripe test secret key: ' STRIPE_API_KEY; echo
export STRIPE_API_KEY
```

## 2. AWS/S3 environment

The deployed S3 target for tomorrow is `hyperswitch-art` in `ap-south-1`. The orchestrator reads these `DEJA_S3_*` variables when it pulls recordings from AWS S3, including `RunSpec.s3_source` replay runs.

Important mode boundary:

- Use the AWS values below for **replay-from-S3** demo runs.
- For a **local record→replay** run that uses the bundled Compose `vector` + `minio` path, do not pass these AWS values to the orchestrator. The current `demo/overlays/hyperswitch/vector.deja.yaml` lands recordings in the local MinIO bucket `deja-recordings`, while `DEJA_S3_*` controls where the orchestrator waits/pulls. Mixing AWS `DEJA_S3_*` with the local MinIO vector sink makes record runs wait in the wrong bucket.

AWS replay-from-S3 values:
```bash
export DEJA_S3_REGION="ap-south-1"
export DEJA_S3_BUCKET="hyperswitch-art"
export DEJA_S3_ENDPOINT="https://s3.ap-south-1.amazonaws.com"
export DEJA_S3_ALLOW_HTTP="false"

# Prefer values from the active AWS profile/session. These commands do not print secrets.
export DEJA_S3_ACCESS_KEY="$(aws configure get aws_access_key_id)"
read -rsp 'DEJA_S3_SECRET_KEY: ' DEJA_S3_SECRET_KEY; echo
export DEJA_S3_SECRET_KEY

# Sanity check only non-secret values.
printf 'S3 bucket=%s region=%s endpoint=%s allow_http=%s\n' \
  "$DEJA_S3_BUCKET" "$DEJA_S3_REGION" "$DEJA_S3_ENDPOINT" "$DEJA_S3_ALLOW_HTTP"
```

If the secret key is already available in a secure environment variable, set `DEJA_S3_SECRET_KEY` from that source instead of typing it. Do not run `env`, `set`, or any command that dumps the shell environment after this point.

## 3. ECR login command

Do not run this runbook step unless the host is expected to pull the Jenkins image and AWS credentials are already configured. The command below does not print the token; it streams it to Docker.

```bash
aws ecr get-login-password --region ap-south-1 \
  | docker login --username AWS --password-stdin \
      223655089699.dkr.ecr.ap-south-1.amazonaws.com
```

## 4. Vendor checkout must match the candidate image source SHA

The Hyperswitch base Compose file runs `migration_runner` with `./:/app` and `working_dir: /app`. Because the first compose file is `vendor/hyperswitch-deja-clean/docker-compose.yml`, that `./` is the vendor tree. For a Jenkins/ECR image drop-in, check out the vendor tree at the same source SHA used to build the candidate image; otherwise the replay router may boot against migrations/config from a different commit.

Use the candidate source SHA supplied by the Jenkins job, image metadata, or release notes. The repo-proven frozen source ref for the Jenkins drop-in is `juspay/hyperswitch:deja-pr @ ff191d7f79`; treat the exact ECR URI and binary path as operator-supplied unless verified from Jenkins or image metadata.

```bash
export CANDIDATE_SOURCE_SHA='<operator-supplied-source-sha-for-candidate-image>'

git -C "$VENDOR" fetch --all --tags
git -C "$VENDOR" checkout --detach "$CANDIDATE_SOURCE_SHA"
git -C "$VENDOR" rev-parse HEAD
```

The printed SHA must equal `CANDIDATE_SOURCE_SHA`. If it does not, stop and fix the vendor checkout before running migrations or replay.

## 5. Build the demo binaries

Build only the binaries needed by the orchestrator and replay kernel:

```bash
cargo build --release -p deja-orchestrator -p deja-kernel
```

For the LocalPath flow, also build the Hyperswitch router from the vendor tree with Deja features:

```bash
(
  cd "$VENDOR"
  cargo build --release -p router --features deja,v1 --bin router
)
```

The LocalPath candidate binary is then:

```bash
export LOCAL_ROUTER_BINARY="$REPO_ROOT/$VENDOR/target/release/router"
test -x "$LOCAL_ROUTER_BINARY"
```

## 6. Start orchestrator Postgres

The UI run list, stages, logs, artifacts, and audit views are Postgres-backed. Start the dedicated orchestrator database separately from the Hyperswitch demo stack:

```bash
docker compose -p deja-orchestrator \
  -f demo/docker-compose.orchestrator.yml \
  up -d --wait
```

Health check:

```bash
docker compose -p deja-orchestrator \
  -f demo/docker-compose.orchestrator.yml \
  ps
```

## 7. Start the orchestrator API/UI

Start the API from the repo root. Keep this process running in its terminal.

First set the compose/kernel variables common to both modes:

```bash
export DEMO_COMPOSE_BASE="$BASE"
export DEMO_COMPOSE_OVERLAY="$OVERLAY"
export DEMO_KERNEL_BIN="$REPO_ROOT/target/release/deja-kernel"
export DEMO_KAFKA_TOPIC="hyperswitch-deja-recording-events"
export DEJA_VENDOR_PATH="$VENDOR"
export VERGEN_GIT_SHA="$(git -C "$VENDOR" rev-parse HEAD 2>/dev/null || echo unknown)"
```

### 7.1 Start API for AWS replay-from-S3

Use this for the primary Jenkins/ECR demo path in Section 8.1. The API process receives AWS `DEJA_S3_*` because replay scans/pulls a deployed S3 prefix.

```bash
HARNESS_BIND="$HARNESS_BIND" \
HARNESS_STATE_DIR="$HARNESS_STATE_DIR" \
DEMO_COMPOSE_BASE="$DEMO_COMPOSE_BASE" \
DEMO_COMPOSE_OVERLAY="$DEMO_COMPOSE_OVERLAY" \
DEMO_PROJECT="$DEMO_PROJECT" \
DEMO_REPLAY_PORT="$DEMO_REPLAY_PORT" \
DEMO_KERNEL_BIN="$DEMO_KERNEL_BIN" \
DEMO_KAFKA_TOPIC="$DEMO_KAFKA_TOPIC" \
DEJA_VENDOR_PATH="$DEJA_VENDOR_PATH" \
VERGEN_GIT_SHA="$VERGEN_GIT_SHA" \
DEJA_S3_ACCESS_KEY="$DEJA_S3_ACCESS_KEY" \
DEJA_S3_SECRET_KEY="$DEJA_S3_SECRET_KEY" \
DEJA_S3_REGION="$DEJA_S3_REGION" \
DEJA_S3_BUCKET="$DEJA_S3_BUCKET" \
DEJA_S3_ENDPOINT="$DEJA_S3_ENDPOINT" \
DEJA_S3_ALLOW_HTTP="$DEJA_S3_ALLOW_HTTP" \
STRIPE_API_KEY="$STRIPE_API_KEY" \
  ./target/release/deja-orchestrator
```

### 7.2 Start API for local record→replay

Use this only for the local MinIO-backed record path in Section 8.2 or the scripted self-demo. It explicitly removes AWS `DEJA_S3_*` from the process environment so `S3Config::from_env()` falls back to the demo MinIO defaults (`http://127.0.0.1:9100`, bucket `deja-recordings`, `minioadmin` credentials). If the API is already running from Section 7.1, stop it with `Ctrl-C` and restart it with this command before creating a local record run.

```bash
env \
  -u DEJA_S3_ACCESS_KEY \
  -u DEJA_S3_SECRET_KEY \
  -u DEJA_S3_REGION \
  -u DEJA_S3_BUCKET \
  -u DEJA_S3_ENDPOINT \
  -u DEJA_S3_ALLOW_HTTP \
  HARNESS_BIND="$HARNESS_BIND" \
  HARNESS_STATE_DIR="$HARNESS_STATE_DIR" \
  DEMO_COMPOSE_BASE="$DEMO_COMPOSE_BASE" \
  DEMO_COMPOSE_OVERLAY="$DEMO_COMPOSE_OVERLAY" \
  DEMO_PROJECT="$DEMO_PROJECT" \
  DEMO_REPLAY_PORT="$DEMO_REPLAY_PORT" \
  DEMO_KERNEL_BIN="$DEMO_KERNEL_BIN" \
  DEMO_KAFKA_TOPIC="$DEMO_KAFKA_TOPIC" \
  DEJA_VENDOR_PATH="$DEJA_VENDOR_PATH" \
  VERGEN_GIT_SHA="$VERGEN_GIT_SHA" \
  STRIPE_API_KEY="$STRIPE_API_KEY" \
    ./target/release/deja-orchestrator
```

API health check from another terminal:

```bash
curl -fsS "$API/api/v1/healthz" | jq .
```

Expected response:

```json
{
  "status": "ok"
}
```

Browser URLs:

- Dashboard home: `http://127.0.0.1:8070/`
- Runs list: `http://127.0.0.1:8070/runs`
- Run detail after a run is created: `http://127.0.0.1:8070/runs/<run_id>`
- API health: `http://127.0.0.1:8070/api/v1/healthz`
- API runs list: `http://127.0.0.1:8070/api/v1/runs`

If you changed `HARNESS_BIND`, replace `8070` with that port.

## 8. API replay/record commands

Set the audit actor once for all API write calls:

```bash
export DEJA_ACTOR="operator:${USER:-morning-demo}"
```

### 8.1 PrebuiltImage replay from AWS S3

This is the primary Jenkins/ECR morning-demo flow: pull an existing deployed recording from AWS S3 and replay it against the Jenkins router image. It does not need `/workload.sh` because the host `deja-kernel` drives the replayed HTTP requests.

Use this flow only after ECR login and `docker image inspect "$CANDIDATE_IMAGE"` have succeeded. `CANDIDATE_IMAGE` must be a real operator-supplied image URI or digest; it must not be empty and must not be `deja-demo`.

```bash
export CANDIDATE_IMAGE='<operator-supplied-ECR-image-uri-or-digest>'
docker pull "$CANDIDATE_IMAGE"
docker image inspect "$CANDIDATE_IMAGE" \
  --format 'Entrypoint={{json .Config.Entrypoint}} Cmd={{json .Config.Cmd}} WorkingDir={{json .Config.WorkingDir}} ExposedPorts={{json .Config.ExposedPorts}}'
```

Point `S3_SOURCE_PATH` at the deployed aggregator prefix that contains Deja envelope lines. If the prefix contains exactly one session, leave `S3_SESSION_ID` empty and the orchestrator auto-resolves it. If it contains multiple sessions, set `S3_SESSION_ID` to the desired envelope `capture.session_id`.

```bash
export S3_SOURCE_PATH='s3://hyperswitch-art/<prefix-containing-deja-envelope-lines>'
export S3_SESSION_ID=''   # optional; set only when the prefix contains multiple sessions

PREBUILT_AWS_REP_RUN_ID="$(
  if [ -n "$S3_SESSION_ID" ]; then
    jq -nc \
      --arg image "$CANDIDATE_IMAGE" \
      --arg path "$S3_SOURCE_PATH" \
      --arg session "$S3_SESSION_ID" \
      '{
        mode:"replay",
        candidate_spec:{kind:"prebuilt_image", image:$image},
        recording_id:$session,
        s3_source:{path:$path, region:"ap-south-1", endpoint:"https://s3.ap-south-1.amazonaws.com"}
      }'
  else
    jq -nc \
      --arg image "$CANDIDATE_IMAGE" \
      --arg path "$S3_SOURCE_PATH" \
      '{
        mode:"replay",
        candidate_spec:{kind:"prebuilt_image", image:$image},
        s3_source:{path:$path, region:"ap-south-1", endpoint:"https://s3.ap-south-1.amazonaws.com"}
      }'
  fi \
  | curl -fsS \
      -H 'content-type: application/json' \
      -H "X-Deja-Actor: $DEJA_ACTOR" \
      --data-binary @- \
      "$API/api/v1/runs" \
  | jq -r .run_id
)"
printf 'prebuilt AWS replay run: %s\n' "$PREBUILT_AWS_REP_RUN_ID"
printf 'dashboard: %s/runs/%s\n' "$API" "$PREBUILT_AWS_REP_RUN_ID"
```

Poll and fetch the scorecard:

```bash
watch -n 2 "curl -fsS '$API/api/v1/runs/$PREBUILT_AWS_REP_RUN_ID' | jq '{state, live, failure_reason}'"

curl -fsS "$API/api/v1/runs/$PREBUILT_AWS_REP_RUN_ID/scorecard" \
  | jq '{verdict, summary: {matched_correlations: .summary.matched_correlations, total_correlations: .summary.total_correlations, http_status_mismatches: .summary.http_status_mismatches, http_body_mismatches: .summary.http_body_mismatches, side_effect_divergences: .summary.side_effect_divergences, resolved_by_rank: .summary.resolved_by_rank}}'
```

### 8.2 LocalPath local record/replay candidate

Use this flow when the candidate is a local router binary and you want to create a fresh local recording. The API process must have been started with Section 7.2, not Section 7.1. Unsetting `DEJA_S3_*` in the shell after the API has already started is not enough; those variables are read by the running orchestrator process during `drive_record` and `pull_recording`.

Create a record run:

```bash
REC_RUN_ID="$(
  jq -nc \
    --arg r "$RECORDING_ID" \
    --arg p "$LOCAL_ROUTER_BINARY" \
    '{
      mode:"record",
      candidate_spec:{kind:"local_path", binary_or_source:$p},
      recording_id:$r,
      workload:{iterations:1}
    }' \
  | curl -fsS \
      -H 'content-type: application/json' \
      -H "X-Deja-Actor: $DEJA_ACTOR" \
      --data-binary @- \
      "$API/api/v1/runs" \
  | jq -r .run_id
)"
printf 'record run: %s\n' "$REC_RUN_ID"
printf 'dashboard: %s/runs/%s\n' "$API" "$REC_RUN_ID"
```

Poll until `state` is `completed`:

```bash
watch -n 2 "curl -fsS '$API/api/v1/runs/$REC_RUN_ID' | jq '{state, live, failure_reason}'"
```

Create a replay run against the recording:

```bash
REP_RUN_ID="$(
  jq -nc \
    --arg r "$RECORDING_ID" \
    --arg p "$LOCAL_ROUTER_BINARY" \
    '{
      mode:"replay",
      candidate_spec:{kind:"local_path", binary_or_source:$p},
      recording_id:$r
    }' \
  | curl -fsS \
      -H 'content-type: application/json' \
      -H "X-Deja-Actor: $DEJA_ACTOR" \
      --data-binary @- \
      "$API/api/v1/runs" \
  | jq -r .run_id
)"
printf 'replay run: %s\n' "$REP_RUN_ID"
printf 'dashboard: %s/runs/%s\n' "$API" "$REP_RUN_ID"
```

Poll and fetch the scorecard:

```bash
watch -n 2 "curl -fsS '$API/api/v1/runs/$REP_RUN_ID' | jq '{state, live, failure_reason}'"

curl -fsS "$API/api/v1/runs/$REP_RUN_ID/scorecard" \
  | jq '{verdict, summary: {matched_correlations: .summary.matched_correlations, total_correlations: .summary.total_correlations, http_status_mismatches: .summary.http_status_mismatches, http_body_mismatches: .summary.http_body_mismatches, side_effect_divergences: .summary.side_effect_divergences, resolved_by_rank: .summary.resolved_by_rank}}'
```

### 8.3 PrebuiltImage replay against an existing local recording

Use this only after a local record run has completed and `RECORDING_ID` exists under `HARNESS_STATE_DIR`. It replays that existing recording with the Jenkins/ECR image.

Do **not** use the Jenkins/ECR prebuilt router image for the record run unless you have also supplied `/workload.sh` to the record container. The normal local flow is:

1. Create the recording with the LocalPath/demo image flow above.
2. Replay that `RECORDING_ID` with the Jenkins/ECR prebuilt image below.

```bash
export CANDIDATE_IMAGE='<operator-supplied-ECR-image-uri-or-digest>'
docker pull "$CANDIDATE_IMAGE"
docker image inspect "$CANDIDATE_IMAGE" \
  --format 'Entrypoint={{json .Config.Entrypoint}} Cmd={{json .Config.Cmd}} WorkingDir={{json .Config.WorkingDir}} ExposedPorts={{json .Config.ExposedPorts}}'
```

Create a replay run against the local recording:

```bash
PREBUILT_REP_RUN_ID="$(
  jq -nc \
    --arg r "$RECORDING_ID" \
    --arg image "$CANDIDATE_IMAGE" \
    '{
      mode:"replay",
      candidate_spec:{kind:"prebuilt_image", image:$image},
      recording_id:$r
    }' \
  | curl -fsS \
      -H 'content-type: application/json' \
      -H "X-Deja-Actor: $DEJA_ACTOR" \
      --data-binary @- \
      "$API/api/v1/runs" \
  | jq -r .run_id
)"
printf 'prebuilt replay run: %s\n' "$PREBUILT_REP_RUN_ID"
printf 'dashboard: %s/runs/%s\n' "$API" "$PREBUILT_REP_RUN_ID"
```

Poll and fetch the prebuilt replay scorecard:

```bash
watch -n 2 "curl -fsS '$API/api/v1/runs/$PREBUILT_REP_RUN_ID' | jq '{state, live, failure_reason}'"

curl -fsS "$API/api/v1/runs/$PREBUILT_REP_RUN_ID/scorecard" \
  | jq '{verdict, summary: {matched_correlations: .summary.matched_correlations, total_correlations: .summary.total_correlations, http_status_mismatches: .summary.http_status_mismatches, http_body_mismatches: .summary.http_body_mismatches, side_effect_divergences: .summary.side_effect_divergences, resolved_by_rank: .summary.resolved_by_rank}}'
```

## 9. Jenkins/ECR image drop-in with manual Compose

Use this section for a Jenkins-built router image in ECR when you want to prove the overlay contract directly. The repo-proven source fact is `juspay/hyperswitch:deja-pr @ ff191d7f79`; the exact image URI is operator-supplied unless you have independently proven it from Jenkins or image metadata.

```bash
export CANDIDATE_IMAGE='<operator-supplied-ECR-image-uri-or-digest>'
export REPLAY_HOST_PORT="${DEMO_REPLAY_PORT:-8090}"
export HARNESS_STATE="$HARNESS_STATE_DIR"
export RECORDING_ID="$RECORDING_ID"
export DEJA_RECORDING_TOPIC="hyperswitch-deja-recording-events"
```

Before starting Compose, inspect the image locally. Pull first if needed, after ECR login:

```bash
docker pull "$CANDIDATE_IMAGE"
docker image inspect "$CANDIDATE_IMAGE" \
  --format 'Entrypoint={{json .Config.Entrypoint}} Cmd={{json .Config.Cmd}} WorkingDir={{json .Config.WorkingDir}} ExposedPorts={{json .Config.ExposedPorts}}'
```

Compatibility facts to check:

- The overlay runs `hyperswitch-replay` as image `${CANDIDATE_IMAGE}`.
- The replay service keeps `entrypoint: ["/local/bin/router"]` and `command: ["-f", "/local/config/docker_compose.toml"]`, with `working_dir: /local`.
- Port mapping is `${REPLAY_HOST_PORT:-8090}:8080`; the candidate image must expose/listen on router port `8080`.
- Volumes are `./config:/local/config` from the vendor tree and `${HARNESS_STATE}:/harness-state`.
- Dependencies are `pg`, `redis-standalone`, `migration_runner`, and `superposition-init`.
- The frozen vendor Dockerfile final image exposes `8080`, copies the router binary to `/local/bin/${BINARY}`, sets `WORKDIR /local/bin`, and uses `CMD ./${BINARY}`. It copies `payment_required_fields_v2.toml` only and does not carry migrations; the vendor checkout supplies migrations through the Compose `./:/app` mount.
- The base Compose command for the stock service is `/local/bin/router -f /local/config/docker_compose.toml` and mounts `./config:/local/config`.

Do **not** remove the replay entrypoint. A command-only override such as `command: ["-f", "/local/config/docker_compose.toml"]` can replace the image CMD and fail if no entrypoint is present. Safe semantics are either:

```yaml
entrypoint: ["/local/bin/router"]
command: ["-f", "/local/config/docker_compose.toml"]
```

or a single command that includes the binary:

```yaml
command: ["/local/bin/router", "-f", "/local/config/docker_compose.toml"]
```

The committed overlay already uses the first safe form for `hyperswitch-replay`.

Recommended replay env delta for a Jenkins/ECR smoke: make Superposition fallback independent of the image working directory by adding this under `hyperswitch-replay.environment` in a temporary compose override or in the overlay before the demo:

```yaml
environment:
  ROUTER__SUPERPOSITION__BACKUP_FILE_PATH: /local/config/superposition_seed.toml
```

Keep the existing typed replay env:

```yaml
environment:
  ROUTER__DEJA__MODE: replay
  ROUTER__DEJA__REPLAY__SOURCE: /harness-state/lookup-tables/${RUN_ID:-demo}.jsonl
  ROUTER__DEJA__REPLAY__OBSERVED_SINK: /harness-state/observed/${RUN_ID:-demo}.jsonl
  ROUTER__DEJA__RUN_ID: ${RUN_ID:-demo}
  RUST_MIN_STACK: "16777216"
```

Manual Compose does not pull S3 or render the lookup table. Satisfy the replay source precondition before `docker compose up`; otherwise `deja_boot::install_replay` fails at startup and `/health` never comes up.

Option A — reuse a completed API replay run's rendered lookup table:

```bash
export RUN_ID="${PREBUILT_AWS_REP_RUN_ID:-${PREBUILT_REP_RUN_ID:-}}"
test -n "$RUN_ID"
test -s "$HARNESS_STATE/lookup-tables/$RUN_ID.jsonl"
```

Option B — copy any existing rendered lookup table to the manual run id:

```bash
export RUN_ID="manual-$RUN_TAG"
export EXISTING_LOOKUP_TABLE='<path-to-existing-rendered-lookup-table.jsonl>'
mkdir -p "$HARNESS_STATE/lookup-tables"
cp "$EXISTING_LOOKUP_TABLE" "$HARNESS_STATE/lookup-tables/$RUN_ID.jsonl"
test -s "$HARNESS_STATE/lookup-tables/$RUN_ID.jsonl"
```

Start the replay service and dependencies manually. Use `--no-build`; the overlay still has a `build:` block, and `--build` can rebuild the local context instead of using the pulled Jenkins image.

```bash
docker compose -p "$DEMO_PROJECT" \
  -f "$BASE" \
  -f "$OVERLAY" \
  up -d --wait --no-build \
  pg redis-standalone migration_runner superposition superposition-init hyperswitch-replay
```

Smoke check the replay router port:

```bash
curl -fsS "http://127.0.0.1:${REPLAY_HOST_PORT}/health" | jq . || \
  curl -fsS "http://127.0.0.1:${REPLAY_HOST_PORT}/health"
```

Inspect logs without printing secrets:

```bash
docker compose -p "$DEMO_PROJECT" \
  -f "$BASE" \
  -f "$OVERLAY" \
  logs --tail=120 hyperswitch-replay migration_runner
```

Manual Compose proves the pulled image can boot with the overlay contract for an already-rendered replay lookup table. API/UI PrebuiltImage runs are the normal path for AWS S3 replay: they pull S3, render the lookup table, start Compose without rebuilding the candidate image, drive the kernel, and score the run.

## 10. Full scripted self-demo alternative

For a local self-replay smoke path, the existing driver builds router + kernel + orchestrator and drives record then replay. It is useful when you are not exercising Jenkins/ECR image drop-in. Run it with AWS `DEJA_S3_*` removed; the script's local recording pipeline writes to MinIO.

```bash
env \
  -u DEJA_S3_ACCESS_KEY \
  -u DEJA_S3_SECRET_KEY \
  -u DEJA_S3_REGION \
  -u DEJA_S3_BUCKET \
  -u DEJA_S3_ENDPOINT \
  -u DEJA_S3_ALLOW_HTTP \
  STRIPE_API_KEY="$STRIPE_API_KEY" \
  VENDOR="$VENDOR" \
  RUN_TAG="$RUN_TAG" \
  demo/run-deja-demo.sh --iterations 1 --keep
```

`--keep` leaves the Hyperswitch stack running for inspection. Omit it when you want automatic teardown.

## 11. Troubleshooting

### Orchestrator API does not start

Check the bind port and state dir:

```bash
curl -fsS "$API/api/v1/healthz" | jq .
docker compose -p deja-orchestrator -f demo/docker-compose.orchestrator.yml ps
```

If the API logs say the store is unavailable, restart orchestrator Postgres:

```bash
docker compose -p deja-orchestrator \
  -f demo/docker-compose.orchestrator.yml \
  up -d --wait
```

### Runs are rejected with `X-Deja-Actor header required`

Mutating API calls must include an actor header:

```bash
-H "X-Deja-Actor: operator:${USER:-morning-demo}"
```

If `DEJA_API_SERVICE_TOKEN` was set when starting the API, mutating calls also need `Authorization: Bearer <token>`. Do not set that token for the local morning demo unless you intentionally want auth enforcement.

### Migration runner fails

Confirm the vendor checkout is at the candidate source SHA and that the base Compose mount resolves to the vendor root:

```bash
git -C "$VENDOR" rev-parse HEAD

docker compose -p "$DEMO_PROJECT" \
  -f "$BASE" \
  -f "$OVERLAY" \
  logs --tail=160 migration_runner
```

If the logs show missing migrations or config, stop and fix the vendor checkout. Do not edit vendor files.

### Replay router is unhealthy

Check the effective candidate image and the overlay contract:

```bash
docker image inspect "$CANDIDATE_IMAGE" \
  --format 'Entrypoint={{json .Config.Entrypoint}} Cmd={{json .Config.Cmd}} WorkingDir={{json .Config.WorkingDir}} ExposedPorts={{json .Config.ExposedPorts}}'

docker compose -p "$DEMO_PROJECT" \
  -f "$BASE" \
  -f "$OVERLAY" \
  ps

docker compose -p "$DEMO_PROJECT" \
  -f "$BASE" \
  -f "$OVERLAY" \
  logs --tail=160 hyperswitch-replay
```

Common causes:

- `CANDIDATE_IMAGE` was not exported in the shell that ran `docker compose`.
- The host is not logged into ECR.
- Vendor checkout does not match the candidate image source SHA.
- The image does not contain `/local/bin/router` or does not listen on `8080`.
- The replay entrypoint/command was changed from the safe overlay form.

### Recording did not land in S3

Check non-secret S3 settings and vector/minio logs:

```bash
printf 'bucket=%s region=%s endpoint=%s allow_http=%s\n' \
  "$DEJA_S3_BUCKET" "$DEJA_S3_REGION" "$DEJA_S3_ENDPOINT" "$DEJA_S3_ALLOW_HTTP"

docker compose -p "$DEMO_PROJECT" \
  -f "$BASE" \
  -f "$OVERLAY" \
  logs --tail=160 vector minio minio-setup
```

For the local MinIO path used by the scripted demo, list landing objects:

```bash
docker compose -p "$DEMO_PROJECT" \
  -f "$BASE" \
  -f "$OVERLAY" \
  run --rm -T mc \
  "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; mc ls --recursive local/deja-recordings/landing/v1/session=${RECORDING_ID}/"
```

### Port conflicts

Change ports before starting the API/Compose stack:

```bash
export HARNESS_BIND="127.0.0.1:18070"
export API="http://127.0.0.1:18070"
export DEMO_REPLAY_PORT="18090"
export REPLAY_HOST_PORT="$DEMO_REPLAY_PORT"
```

Then restart the affected processes.

## 12. Teardown

Stop manual Hyperswitch demo stacks:

```bash
docker compose -p "$DEMO_PROJECT" \
  -f "$BASE" \
  -f "$OVERLAY" \
  down -v
```

Replay runs submitted through the orchestrator may use isolated project names like `deja-run-<suffix>` and normally tear themselves down. If one is left behind, list compose projects and remove only the Deja demo projects you own:

```bash
docker compose ls
# Example for an abandoned isolated replay project:
# docker compose -p deja-run-abcdef12 -f "$BASE" -f "$OVERLAY" down -v
```

Stop orchestrator Postgres when the demo is over:

```bash
docker compose -p deja-orchestrator \
  -f demo/docker-compose.orchestrator.yml \
  down
```

Stop the foreground `deja-orchestrator` process with `Ctrl-C` in its terminal.

Local state remains under:

```bash
printf '%s\n' "$HARNESS_STATE_DIR"
```

Delete that directory only when you no longer need run artifacts, recordings, scorecards, or logs.
