#!/usr/bin/env bash
# Deja ISOLATED-RUN MATRIX: record ONE V1 baseline (exactly like
# run-deja-matrix.sh), then submit each candidate as its own orchestrator replay
# run against that SAME shared recording. Each replay run is isolated by docker
# compose project (deja-run-<run8>) → its own pg + redis-standalone +
# migration_runner + superposition(+init) + hyperswitch-replay, and its own host
# replay port.
#
#   STRIPE_API_KEY=<stripe-test-key> demo/run-deja-parallel.sh [--iterations N] [--keep] [--max-parallel K]
#
# Differences vs run-deja-matrix.sh (which stays the sequential reference):
#   - submits through the isolated-stack worker path rather than reusing one stack
#   - keeps --max-parallel parsing for operator compatibility, though patched
#     candidates drain before the next image rebuild so each candidate boots the
#     intended binary
#   - polls every submitted run to completion, then prints the combined matrix
#   - the orchestrator tears each replay stack down (docker compose down -v) in the
#     worker's finally; this script tears down only the shared record-side project
#     (deja-demo) at the end
#
# IMPORTANT (cross-project): replay needs only the harness-state mount + the lookup
# table the orchestrator already RENDERED from the shared MinIO before container
# bring-up. The per-run replay compose subset (hyperswitch-replay + its pg/redis/
# superposition deps) does NOT depend on the record-side services (kafka0/vector/
# minio), so each isolated project is self-contained.
#
# Run from the repo root. Requires: docker (+ compose), cargo, curl, jq.
set -euo pipefail
cd "$(dirname "$0")/.."   # repo root

ITERATIONS=1
KEEP=0
MAX_PARALLEL=3
VENDOR="${VENDOR:-vendor/hyperswitch-deja-clean}"
while [ $# -gt 0 ]; do
  case "$1" in
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --max-parallel) MAX_PARALLEL="$2"; shift 2 ;;
    *) echo "unknown arg: $1"; exit 2 ;;
  esac
done

# The active default candidate patches (vendor-only; see demo/cross-version/README.md).
# Same set as the sequential matrix.
BENIGN_PATCH="$(pwd)/demo/cross-version/benign-line-shift.patch"
REAL_PATCH="$(pwd)/demo/cross-version/real-change.patch"
EARLIER_FORK_PATCH="$(pwd)/demo/cross-version/earlier-fork.patch"
DROPPED_WRITE_PATCH="$(pwd)/demo/cross-version/dropped-write.patch"
RESPONSE_ONLY_PATCH="$(pwd)/demo/cross-version/response-only.patch"
EXTRA_CALL_PATCH="$(pwd)/demo/cross-version/extra-call.patch"
for p in "$BENIGN_PATCH" "$REAL_PATCH" "$EARLIER_FORK_PATCH" "$DROPPED_WRITE_PATCH" "$RESPONSE_ONLY_PATCH" "$EXTRA_CALL_PATCH"; do
  [ -f "$p" ] || { echo "missing candidate patch: $p"; exit 1; }
done

# Shared constants + build/orchestrator/poll/candidate-patch helpers.
source demo/lib.sh

require_tools
init_run_state
echo "── run tag: ${RUN_TAG}  ·  baseline recording: ${REC_ID}  ·  state: ${STATE_DIR} ──"
echo "── PARALLEL replay: max ${MAX_PARALLEL} concurrent isolated stacks ──"

API_PID=""
cleanup() {
  revert_candidate_patch
  [ -n "$API_PID" ] && kill "$API_PID" 2>/dev/null || true
  if [ "$KEEP" -eq 0 ]; then
    echo "── tearing down shared record-side project (deja-demo) ──"
    docker compose -p "$PROJECT" -f "$BASE" -f "$OVERLAY" down -v >/dev/null 2>&1 || true
    # Per-run replay stacks are torn down by the orchestrator worker; sweep any
    # that leaked (e.g. an orchestrator crash) so a re-run starts clean.
    echo "── sweeping any leaked per-run replay stacks (deja-run-*) ──"
    docker compose ls --all --format json 2>/dev/null \
      | jq -r '.[].Name // empty' 2>/dev/null \
      | grep '^deja-run-' \
      | while read -r proj; do
          docker compose -p "$proj" -f "$BASE" -f "$OVERLAY" down -v >/dev/null 2>&1 || true
        done || true
  else
    echo "── stacks left running (--keep); state in $STATE_DIR ──"
  fi
}
trap cleanup EXIT

echo "── building deja router (V1) + kernel + orchestrator + tui ──"
build_binaries
# Tell the orchestrator to SKIP per-run `--build` on replay: this script bakes
# the replay image once (and re-bakes per candidate), so concurrent isolated
# projects must reuse the existing tag rather than racing the build cache.
# Inherited by the API process started in start_api. (Consumed in drive_replay;
# the sequential run-deja-matrix.sh never sets it, so it keeps rebuilding.)
export DEMO_REPLAY_NO_BUILD=1
start_api

# ── RECORD ONCE: the golden V1 baseline ──────────────────────────────────────
echo "── RECORD (V1 baseline): drive ${ITERATIONS} workload iteration(s); HS → Kafka → Vector → MinIO ──"
REC_RUN=$(post_run "$(jq -nc --arg r "$REC_ID" --argjson c "$candidate" --argjson it "$ITERATIONS" \
  '{mode:"record", candidate_spec:$c, recording_id:$r, workload:{iterations:$it}}')")
[ "$(poll "$REC_RUN")" = "completed" ] || { echo "RECORD run failed"; curl -fsS "${API}/api/v1/runs/${REC_RUN}" | jq .; exit 1; }
echo "   baseline recorded → recording_id=${REC_ID}"

# Build the replay image ONCE so concurrent isolated projects don't race the
# build cache rebuilding the SAME deja-router-local:latest tag. The RECORD run
# above already built it (hyperswitch-server shares the tag), but build the
# replay service explicitly to be safe, then tell the orchestrator to SKIP
# per-run --build via DEMO_REPLAY_NO_BUILD (consumed in drive_replay).
echo "── pre-building replay image once (parallel runs reuse it) ──"
RECORDING_ID="$REC_ID" REPLAY_HOST_PORT=0 \
  docker compose -p "$PROJECT" -f "$BASE" -f "$OVERLAY" build hyperswitch-replay >/dev/null 2>&1 \
  || echo "   (replay pre-build skipped/failed; runs will build per-project)"


# Run bookkeeping, parallel arrays indexed together.
RUN_ID=()      # orchestrator run id
RUN_LABEL=()
RUN_EXPECT=()  # expect_div 0|1
# Wait until fewer than MAX_PARALLEL replay runs are still in-flight.
# In-flight = submitted run whose store state is neither completed nor failed.
throttle() {
  while :; do
    local inflight=0 i st
    for i in "${!RUN_ID[@]}"; do
      st=$(curl -fsS "${API}/api/v1/runs/${RUN_ID[$i]}" 2>/dev/null \
            | jq -r '.state // .live.status // "pending"')
      case "$st" in completed|failed) ;; *) inflight=$((inflight+1)) ;; esac
    done
    [ "$inflight" -lt "$MAX_PARALLEL" ] && return 0
    sleep 2
  done
}

# Submit ONE candidate replay. The orchestrator isolates it by project+port.
# DEMO_REPLAY_NO_BUILD is set on the long-lived API process (start_api), so this
# just records bookkeeping + posts.
submit_run() { # submit_run <label> <expect_div 0|1>
  local label="$1" expect_div="$2" rid
  local expect_note=$( [ "$expect_div" -eq 1 ] && echo "diverge" || echo "pass" )
  throttle
  rid=$(post_run "$(jq -nc --arg r "$REC_ID" --argjson c "$candidate" \
    '{mode:"replay", candidate_spec:$c, recording_id:$r}')" "$expect_note")
  echo "  ↪ submitted ${label} → run ${rid}"
  RUN_ID+=("$rid")
  RUN_LABEL+=("$label")
  RUN_EXPECT+=("$expect_div")
}

# Submit one candidate. The candidate binary must be built BEFORE submitting (the
# orchestrator boots whatever the current image is). Since all candidates share one
# replay image tag in this demo, patched-binary variants can't run concurrently
# with a DIFFERENT patched binary under the same tag.
submit_candidate() { # submit_candidate <label> <patch|""> <expect_div 0|1>
  local label="$1" patch="$2" expect_div="$3"
  echo
  echo "════════════════════════════════════════════════════════════"
  echo "  CANDIDATE: ${label}  (expect $( [ "$expect_div" -eq 1 ] && echo DIVERGENCE || echo PASS ))"
  echo "════════════════════════════════════════════════════════════"
  if [ -n "$patch" ]; then
    apply_candidate_patch "$patch"
    rebuild_router_v2 "$label"
    # Re-bake the replay image from the patched binary so the per-run project
    # boots THIS candidate.
    echo "── re-baking replay image for ${label} ──"
    RECORDING_ID="$REC_ID" REPLAY_HOST_PORT=0 \
      docker compose -p "$PROJECT" -f "$BASE" -f "$OVERLAY" build hyperswitch-replay >/dev/null 2>&1 \
      || echo "   (re-bake failed; run may boot a stale image)"
  fi
  submit_run "$label" "$expect_div"
  # Wait for THIS candidate's run before reverting+rebuilding the next, so the
  # shared image isn't swapped out from under an in-flight run.
  drain_candidate_runs
  revert_candidate_patch
}

# Poll submitted runs until they finish. Keeps the shared image stable before the
# next candidate rebuild.
drain_candidate_runs() {
  local i st
  while :; do
    local pending=0
    for i in "${!RUN_ID[@]}"; do
      st=$(curl -fsS "${API}/api/v1/runs/${RUN_ID[$i]}" 2>/dev/null \
            | jq -r '.state // .live.status // "pending"')
      case "$st" in completed|failed) ;; *) pending=$((pending+1)) ;; esac
    done
    [ "$pending" -eq 0 ] && return 0
    sleep 2
  done
}

# self FIRST (V1 binary as built — the pre-built replay image), then the V2s.
submit_candidate "self"          ""                     0
submit_candidate "benign"        "$BENIGN_PATCH"        0
submit_candidate "real"          "$REAL_PATCH"          1
submit_candidate "earlier-fork"  "$EARLIER_FORK_PATCH"  1
submit_candidate "dropped-write" "$DROPPED_WRITE_PATCH" 1
submit_candidate "response-only" "$RESPONSE_ONLY_PATCH" 1
submit_candidate "extra-call"    "$EXTRA_CALL_PATCH"    1

# All runs are terminal now (each submit_candidate drained its own). Collect.
echo
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  DEJA PARALLEL MATRIX — one V1 recording (${REC_ID}); each run isolated by project+port"
echo "════════════════════════════════════════════════════════════════════════════════"
printf "  %-13s %-12s %-9s %-7s %-7s %-9s %s\n" "CAND" "EXPECT" "VERDICT" "CAUGHT" "OK?" "MATCHED" "DIVERG"
ALL_OK=1
for i in "${!RUN_ID[@]}"; do
  rid="${RUN_ID[$i]}"; label="${RUN_LABEL[$i]}"
  expect_div="${RUN_EXPECT[$i]}"
  card=$(curl -fsS "${API}/api/v1/runs/${rid}/scorecard" 2>/dev/null || echo '{}')
  pass=$(echo "$card"    | jq -r '.verdict.pass // "false"')
  matched=$(echo "$card" | jq -r '.summary.matched_correlations // 0')
  total=$(echo "$card"   | jq -r '.summary.total_correlations // 0')
  diverg=$(echo "$card"  | jq -r '.summary.side_effect_divergences // 0')
  caught=$( [ "$pass" = "true" ] && echo 0 || echo 1 )
  ok=0
  if [ "$expect_div" -eq 1 ]; then [ "$caught" -eq 1 ] && ok=1; else [ "$caught" -eq 0 ] && ok=1; fi
  exp=$( [ "$expect_div" -eq 1 ] && echo "diverge" || echo "pass" )
  vd=$( [ "$pass" = "true" ] && echo "PASS" || echo "DIVERGE" )
  caughtmark=$( [ "$caught" -eq 1 ] && echo "YES" || echo "no" )
  if [ "$ok" -eq 1 ]; then okmark="OK"; else okmark="XX"; ALL_OK=0; fi
  printf "  %-13s %-12s %-9s %-7s %-7s %-9s %s\n" "$label" "$exp" "$vd" "$caughtmark" "$okmark" "${matched}/${total}" "$diverg"
done
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  Each run used its OWN isolated pg/redis/superposition/replay stack (project"
echo "  deja-run-<run8>, a free host port). The orchestrator tore each down (down -v)."
echo "════════════════════════════════════════════════════════════════════════════════"
for i in "${!RUN_ID[@]}"; do
  echo "    ${RUN_LABEL[$i]}:"
  echo "          web  → $(run_url "${RUN_ID[$i]}")"
  echo "          card → ${API}/api/v1/runs/${RUN_ID[$i]}/scorecard"
done
echo "════════════════════════════════════════════════════════════════════════"

[ "$ALL_OK" -eq 1 ]
