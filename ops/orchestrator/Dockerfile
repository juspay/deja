# Replay-orchestrator image — dual role from one build:
#   * the orchestrator Deployment (API + dashboard). In compose mode it shells
#     out to the docker CLI (daemon via DOCKER_HOST); in k8s mode it POSTs Jobs
#     to the in-cluster apiserver (no docker).
#   * a replay Job's runner / migrations-init containers: the command is
#     overridden to deja-runner. The migrations init runs `deja-runner
#     stage-codebundle` (pulls + unpacks the candidate's migrations tar); the
#     runner's migrate stage shells out to the bundled diesel CLI (below).
#
# The compose-mode app container also needs the repo subset the compose
# lifecycle bind-mounts at runtime:
#   - vendor/hyperswitch-deja-clean (minus target/.git, ~224M): the stock
#     migration_runner mounts `./:/app` and runs `just migrate`, so the whole
#     checkout ships; also compose files + ./config mounts.
#   - demo/{migration-runner.sh,workload.sh,superposition_seed.toml,
#     .diesel-cli/}: the overlay's resilient-migration + workload mounts
#     (demo/ itself holds 43G of run state — never COPY it wholesale).
#
# Two build targets (BuildKit prunes the context per target, so `k8s` never even
# uploads the vendor tree). Build FROM THE REPO ROOT:
#   * DEPLOYED (k8s) — lean, binaries only, no docker CLI / vendor / demo assets:
#       docker buildx build --target k8s -f ops/orchestrator/Dockerfile -t deja:<tag> .
#   * LOCAL (compose, the default last stage) — adds the docker CLI + the compose
#     rig's bind-mount subset (vendor/hyperswitch-deja-clean + demo assets):
#       docker buildx build -f ops/orchestrator/Dockerfile -t deja-compose:<tag> .
# A .dockerignore excludes demo/* (except the four assets), vendor/*/target,
# vendor/*/.git, target/.
#
# WORKDIR is the bundled repo root, so the orchestrator's relative defaults
# (DEMO_COMPOSE_BASE=vendor/hyperswitch-deja-clean/docker-compose.yml,
# DEMO_KERNEL_BIN=target/release/deja-kernel) resolve without overrides.

# ── diesel_cli stage ───────────────────────────────────────────────────────
# The migration tool the in-pod runner shells out to at the migrate stage
# (RUNNER_MIGRATE_CMD = `diesel migration run …`). Built on a DEDICATED, newer
# rustc — diesel_cli 2.3.5 requires rustc >= 1.86, and this keeps the app's
# proven 1.85 toolchain (below) untouched. Bookworm base so the binary's ABI
# matches the bookworm-slim runtime; pinned to the version the compose rig uses
# (demo/.diesel-cli). postgres-only — no mysql/sqlite client libs needed.
FROM rust:1.88-slim-bookworm AS diesel-build
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libpq-dev && rm -rf /var/lib/apt/lists/*
RUN cargo install diesel_cli --version 2.3.5 --no-default-features --features postgres --root /diesel

# ── build stage ──────────────────────────────────────────────────────────────
FROM rust:1.88-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev libpq-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# web/dist is committed, embedded at compile time by rust_embed.
COPY web/dist ./web/dist
RUN cargo build --release -p deja-orchestrator -p deja-kernel

# ── runtime base (shared by both targets) ────────────────────────────────────
# libpq5: the runtime shared lib the bundled diesel CLI dynamically links
# (libpq.so.5 + its own transitive deps, resolved by apt) for the postgres
# migrate stage. Without it, `diesel migration run` fails to load.
FROM debian:bookworm-slim AS runtime-base
# postgresql-client (psql) + redis-tools (redis-cli): the in-pod runner's
# StoreExec::Direct shells out to both for seed/probe/readback against the Job's
# pg/redis sidecars (store_exec.rs). Without them the schema probe fails with
# "No such file or directory (os error 2)". libpq5 alone only covers diesel.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl bash jq libpq5 postgresql-client redis-tools \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace/repo
COPY --from=build /src/target/release/deja-orchestrator /usr/local/bin/deja-orchestrator
# Same binary, two container roles: the Deployment runs the default entrypoint
# (orchestrator API); a replay Job's runner container overrides the command to
# deja-runner (one run, exit code = Job result).
COPY --from=build /src/target/release/deja-runner /usr/local/bin/deja-runner
# The migration tool the runner's migrate stage shells out to — on PATH.
COPY --from=diesel-build /diesel/bin/diesel /usr/local/bin/diesel
COPY --from=build /src/target/release/deja-kernel ./target/release/deja-kernel
ENV HARNESS_BIND=0.0.0.0:8080 \
    HARNESS_STATE_DIR=/workspace/state
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/deja-orchestrator"]

# ── k8s target: the DEPLOYED image (lean) ────────────────────────────────────
# In k8s mode the orchestrator POSTs Jobs to the apiserver (no docker) and the
# in-pod runner stages the CANDIDATE's migrations from S3 (stage-codebundle) then
# shells to the bundled diesel — so NONE of the compose-mode baggage (docker CLI,
# ~224M vendor tree, demo assets) is needed. BuildKit prunes the build context
# per target, so this build never even uploads vendor/. Build with `--target k8s`.
FROM runtime-base AS k8s

# ── compose target: LOCAL TESTING / demo rig (default last stage) ─────────────
# Kept as the default stage so a plain `docker build` (no --target) still yields
# the compose-capable image. Adds the docker CLI + compose plugin (the local
# lifecycle shells out to compose via DOCKER_HOST) and the repo subset the
# compose lifecycle bind-mounts at its recorded relative paths.
FROM runtime-base AS compose
RUN apt-get update && apt-get install -y --no-install-recommends gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg \
         -o /etc/apt/keyrings/docker.asc \
    && echo "deb [signed-by=/etc/apt/keyrings/docker.asc] \
         https://download.docker.com/linux/debian bookworm stable" \
         > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
    && rm -rf /var/lib/apt/lists/*
ENV DOCKER_HOST=unix:///var/run/docker.sock
# NOTE: the migrations under this vendor tree are the compose *record* rig's
# schema ONLY — deliberately NOT the source of truth for an in-pod replay's
# migrate stage (that schema is the CANDIDATE's, applied from the candidate ref
# and verified against RUNNER_EXPECTED_MIGRATIONS (P1); the runner owns diesel
# and never imposes its own baked set (A1)).
COPY vendor/hyperswitch-deja-clean ./vendor/hyperswitch-deja-clean
COPY demo/migration-runner.sh demo/workload.sh demo/superposition_seed.toml ./demo/
COPY demo/.diesel-cli ./demo/.diesel-cli
# The CANONICAL compose overlay (typed ROUTER__DEJA__* env) lives out of the
# vendor tree; the vendor copy is a stale pre-typed shape.
COPY demo/overlays ./demo/overlays
