# Shared plumbing for the deja demo drivers (run-deja-demo.sh, run-deja-matrix.sh).
# Source from the REPO ROOT (the drivers `cd "$(dirname "$0")/.."` first), after
# setting VENDOR. Defines the stack constants, the run identifiers, and the
# build / orchestrator / polling / candidate-patch helpers both drivers share.

API_PORT=8070
API="http://127.0.0.1:${API_PORT}"
PROJECT="deja-demo"
# Reuse Hyperswitch's OWN compose + the thin deja overlay.
VENDOR="${VENDOR:-vendor/hyperswitch-deja-clean}"
BASE="${VENDOR}/docker-compose.yml"
OVERLAY="$(pwd)/demo/overlays/hyperswitch/docker-compose.deja.yml"
# Compose build args read process environment, not shell locals; export the
# selected vendor path so direct driver `docker compose ... --build` calls and
# deja-orchestrator worker compose calls bake the same router binary.
export DEJA_VENDOR_PATH="$VENDOR"
candidate='{"kind":"prebuilt_image","image":"deja-demo"}'

PATCH_APPLIED=0
CURRENT_PATCH=""

require_tools() {
  : "${STRIPE_API_KEY:?set STRIPE_API_KEY (test key) — the 9-step workload calls Stripe during record}"
  local tool
  for tool in docker cargo curl jq; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 1; }
  done
}

# RUN_TAG is the run identifier that every artifact path derives from: the
# recording id (`rec-<tag>`, the MinIO/S3 key prefix) and the on-disk state dir
# (demo/harness-state/<tag>, holding the recording, lookup table, observed
# calls, and the replay visualization). It defaults to a timestamp, but can be
# pinned via the RUN_TAG env var so post-run commands use a known, stable value.
# (Reusing a tag reuses its state dir; pick a fresh one for an independent run.)
init_run_state() {
  RUN_TAG="${RUN_TAG:-$(date +%s)}"
  REC_ID="rec-${RUN_TAG}"
  STATE_DIR="$(pwd)/demo/harness-state/${RUN_TAG}"
  mkdir -p "$STATE_DIR"
}

# Upstream Hyperswitch's release profile (lto = true, codegen-units = 1) is
# tuned for production binaries and costs ~40 serial minutes per router build.
# The demo loop overrides it via cargo env vars — vendor files stay pristine —
# trading binary size/speed margins for the fastest possible iteration:
#   lto off + 256 codegen units  → all cores in codegen
#   opt-level 2                  → cheaper codegen, runtime-safe for the demo
#   incremental                  → candidate rebuilds reuse prior codegen
#   mold                         → fastest linker for the huge router binary
DEMO_CARGO_PROFILE=(env
  CARGO_PROFILE_RELEASE_LTO=false
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256
  CARGO_PROFILE_RELEASE_OPT_LEVEL=2
  CARGO_PROFILE_RELEASE_INCREMENTAL=true
  RUSTFLAGS="-C link-arg=-fuse-ld=mold"
)
# mold is optional — fall back to the default linker when absent.
command -v mold >/dev/null || DEMO_CARGO_PROFILE=(env
  CARGO_PROFILE_RELEASE_LTO=false
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256
  CARGO_PROFILE_RELEASE_OPT_LEVEL=2
  CARGO_PROFILE_RELEASE_INCREMENTAL=true
)

# DEMO_UCS=1: build + stage the real Unified Connector Service (connector-service
# grpc-server) at the SAME pinned tag the router's UCS client types come from,
# so the gate records real PaymentService/* gRPC through the deja boundary.
# The service rides the `ucs` compose profile; router-side client config is
# always present in the overlay (replay parity — connect_lazy never dials).
UCS_CHECKOUT="${DEJA_UCS_CHECKOUT:-$HOME/.cargo/git/checkouts/connector-service-eea45f477e53ea08/a070ef7}"
UCS_BUILD_DIR="$(pwd)/demo/.ucs-build"

build_ucs_image() {
  [ -d "$UCS_CHECKOUT" ] || { echo "DEMO_UCS=1 but UCS checkout missing: $UCS_CHECKOUT" >&2; exit 1; }
  mkdir -p "$UCS_BUILD_DIR"
  if [ ! -x "$UCS_BUILD_DIR/grpc-server" ]; then
    ( cd "$UCS_CHECKOUT" && "${DEMO_CARGO_PROFILE[@]}"         cargo build --release -p grpc-server --target-dir "$UCS_BUILD_DIR/target" )
    cp "$UCS_BUILD_DIR/target/release/grpc-server" "$UCS_BUILD_DIR/grpc-server"
  fi
  cp -r "$UCS_CHECKOUT/config" "$UCS_BUILD_DIR/"
  printf '[workspace]\n' > "$UCS_BUILD_DIR/Cargo.toml"
  cp demo/Dockerfile.ucs-server "$UCS_BUILD_DIR/Dockerfile"
  docker build -t deja-ucs-local:latest "$UCS_BUILD_DIR"
}

build_binaries() {
  ( cd "$VENDOR" && "${DEMO_CARGO_PROFILE[@]}" cargo build --release -p router --features deja,v1 --bin router )
  if [ "${DEMO_UCS:-0}" = "1" ]; then
    build_ucs_image
    export COMPOSE_PROFILES="${COMPOSE_PROFILES:+$COMPOSE_PROFILES,}ucs"
  fi
  cargo build --release -p deja-kernel -p deja-orchestrator
  # deja-tui: the interactive record→replay substitution explorer (final stage).
  cargo build --release -p deja-tui

  # Build the diesel CLI once for migrations. HS's migration_runner normally
  # downloads it from GitHub release assets at runtime, but those assets
  # intermittently 504 (transient GitHub CDN) and fail the whole stack. Building
  # from crates.io and mounting it (see docker-compose.deja.yml) removes that
  # dependency. Idempotent; best-effort — if it can't build, migration_runner
  # falls back to the GitHub install with retries.
  if [ ! -x demo/.diesel-cli/bin/diesel ]; then
    echo "── building diesel CLI (one-time, for HS migrations) ──"
    cargo install diesel_cli@2.3.5 --no-default-features --features postgres --locked --root demo/.diesel-cli \
      || echo "   (diesel host-build skipped; migration_runner will fall back to GitHub install)"
  fi
}

# Starts the orchestrator (sets API_PID) and waits for /healthz. Fails LOUDLY
# if the API does not come up — every later step depends on it, so silently
# proceeding only defers the failure to a confusing place.
start_api() {
  # Dedicated orchestrator Postgres (run state, stage history, logs, artifacts,
  # audit — what the web UI reads). Separate compose project so it survives the
  # per-run stack's `down -v`. Best-effort: the API runs file-only without it.
  echo "── starting orchestrator postgres (deja-orchestrator) ──"
  docker compose -p deja-orchestrator -f demo/docker-compose.orchestrator.yml up -d --wait 2>/dev/null \
    || echo "   (orchestrator pg unavailable; runs won't appear in the web UI)"

  echo "── starting orchestrator API on ${API} ──"
  HARNESS_BIND="127.0.0.1:${API_PORT}" \
  HARNESS_STATE_DIR="$STATE_DIR" \
  DEMO_COMPOSE_BASE="$BASE" \
  DEMO_COMPOSE_OVERLAY="$OVERLAY" \
  DEMO_PROJECT="$PROJECT" \
  DEMO_REPLAY_PORT="8090" \
  DEMO_KERNEL_BIN="$(pwd)/target/release/deja-kernel" \
  DEMO_KAFKA_TOPIC="hyperswitch-deja-recording-events" \
  DEJA_VENDOR_PATH="$VENDOR" \
  VERGEN_GIT_SHA="$(git -C "$VENDOR" rev-parse HEAD 2>/dev/null || echo unknown)" \
  STRIPE_API_KEY="$STRIPE_API_KEY" \
  DEJA_S3_ENDPOINT="http://127.0.0.1:9100" \
    ./target/release/deja-orchestrator &
  API_PID=$!
  local _i
  for _i in $(seq 1 30); do
    curl -fsS "${API}/api/v1/healthz" >/dev/null 2>&1 && return 0
    kill -0 "$API_PID" 2>/dev/null || break
    sleep 1
  done
  echo "ERROR: orchestrator API did not become healthy on ${API} (see process output above)"
  exit 1
}

# POST a run spec. Sends the audit actor (decision 8); the optional second
# argument is a human expectation note ("pass" / "diverge") recorded on the
# run row for the dashboard.
DEJA_ACTOR="script:${USER:-unknown}"
post_run() {
  local spec="$1" expectation="${2:-}"
  if [ -n "$expectation" ]; then
    spec=$(echo "$spec" | jq -c --arg e "$expectation" '. + {expectation: $e}')
  fi
  curl -fsS -H 'content-type: application/json' -H "X-Deja-Actor: ${DEJA_ACTOR}" \
    -d "$spec" "${API}/api/v1/runs" | jq -r .run_id
}


# Print the dashboard deep link for a run (the web UI serves at the API root).
run_url() { echo "${API}/runs/$1"; }

poll() { # poll <run_id> ; live progress bar on stderr, terminal status on stdout
  local rid="$1" resp st stage step total start now el bar i
  start=$(date +%s)
  while :; do
    resp=$(curl -fsS "${API}/api/v1/runs/${rid}" 2>/dev/null) || { sleep 2; continue; }
    # v1 merged shape: the store row's `state` is terminal truth; the worker's
    # live snapshot carries stage/step.
    st=$(echo "$resp"    | jq -r '.state // .live.status // "pending"')
    stage=$(echo "$resp" | jq -r '.live.stage // "…"')
    step=$(echo "$resp"  | jq -r '.live.step // 0')
    total=$(echo "$resp" | jq -r '.live.steps_total // 0')
    now=$(date +%s); el=$((now - start))
    if [ "${total:-0}" -gt 0 ] 2>/dev/null; then
      bar=""
      for i in $(seq 1 "$total"); do
        if [ "$i" -le "${step:-0}" ]; then bar="${bar}█"; else bar="${bar}░"; fi
      done
      printf '\r  [%s] %s/%s  %-54s %3ss ' "$bar" "$step" "$total" "$stage" "$el" >&2
    else
      printf '\r  %-10s %-54s %3ss ' "$st" "$stage" "$el" >&2
    fi
    case "$st" in
      completed|failed) printf '\n' >&2; echo "$st"; return ;;
    esac
    sleep 2
  done
}

# Apply a V2 candidate patch to the vendor tree (guarded) and arm the revert.
# The deja instrumentation (parent crates/deja*) MUST be byte-identical across
# V1 and V2 — only the vendored Hyperswitch application code may differ — else a
# divergence would be an instrumentation artifact, not a real version diff.
# Match the `diff --git a/<x> b/<y>` header (present for adds, modifies, deletes
# AND renames, carrying BOTH paths) so a delete or rename can't slip a
# crates/deja* edit past a +++-only check.
check_candidate_patch() { # check_candidate_patch <abs-patch-path>
  local patch="$1"
  if grep -qE '^diff --git .*crates/deja' "$patch"; then
    echo "ERROR: candidate patch touches parent crates/deja* instrumentation (must be vendor-only)"
    exit 1
  fi
  git -C "$VENDOR" apply --check "$patch" \
    || { echo "candidate patch does not apply cleanly to $VENDOR: $patch"; exit 1; }
}

apply_candidate_patch() { # apply_candidate_patch <abs-patch-path>
  local patch="$1"
  check_candidate_patch "$patch"
  git -C "$VENDOR" apply "$patch"
  PATCH_APPLIED=1
  CURRENT_PATCH="$patch"
}

# Rebuild the host router after a candidate patch; warn if the patch was
# compile-neutral. A compile-NEUTRAL edit (e.g. a comment that shifts no
# #[track_caller]/panic Location line) can yield a byte-identical binary under
# `[profile.release] strip = true`. That is not a false-pass risk — replay then
# runs code equivalent to V1, so a clean verdict is CORRECT — but the run
# exercises no behavioral difference, so warn loudly rather than abort.
rebuild_router_v2() { # rebuild_router_v2 <label>
  local label="$1" v1 v2
  echo "── rebuilding host router binary as V2 (${label}) ──"
  v1="$(sha256sum "$VENDOR/target/release/router" | cut -d' ' -f1)"
  ( cd "$VENDOR" && "${DEMO_CARGO_PROFILE[@]}" cargo build --release -p router --features deja,v1 --bin router )
  v2="$(sha256sum "$VENDOR/target/release/router" | cut -d' ' -f1)"
  if [ "$v1" = "$v2" ]; then
    echo "   ⚠️  WARNING: V2 router is byte-identical to V1 (patch is compile-neutral)."
    echo "       Replay will run code equivalent to V1; this run does not exercise a V2 diff."
  else
    echo "   V2 binary built (router sha ${v1:0:12} → ${v2:0:12})"
  fi
}

# Revert the in-flight candidate patch, if any. Called from the drivers' EXIT
# traps (so the dirty vendor tree is restored on success, set -e failure, and
# Ctrl-C alike) and between matrix candidates.
revert_candidate_patch() {
  if [ "$PATCH_APPLIED" -eq 1 ]; then
    echo "── reverting V2 candidate patch ──"
    git -C "$VENDOR" apply -R "$CURRENT_PATCH" 2>/dev/null \
      || echo "   WARNING: patch revert failed — inspect $VENDOR manually"
    PATCH_APPLIED=0
    CURRENT_PATCH=""
  fi
}
