#!/usr/bin/env bash
# Deja TRUE CROSS-VERSION PARALLEL replay (declared per-boundary routing model).
#
# Record ONE golden V1 baseline, build N candidate VERSIONS (each scenario git patch
# → its own router binary), then replay ALL of them CONCURRENTLY against the SAME
# recording. Each version is submitted as a LocalPath candidate_spec, so the
# orchestrator bakes it a DISTINCT image (deja-candidate:<run8>) and runs it in its
# OWN isolated sandbox: compose project deja-run-<run8>, a free host port, a fresh
# pg + redis + migrations + superposition. So K versions run the full replay pipeline
# AT ONCE, fully isolated — no shared DB, redis, port, or image.
#
# Unlike run-deja-parallel.sh (shared image tag → had to drain each candidate before
# the next), distinct per-run images let ALL versions overlap. One replay per version
# (Substitute/Execute are FIXED per-boundary declarations, identical across versions;
# only the candidate CODE differs).
#
#   STRIPE_API_KEY=<stripe-test-key> demo/run-deja-parallel-versions.sh \
#       [--iterations N] [--keep] [--max-parallel K] [--candidates "self real extra-call ..."]
#
# Run from the repo root. Requires: docker (+ compose), cargo, curl, jq.
set -euo pipefail
cd "$(dirname "$0")/.."

ITERATIONS=1; KEEP=0; MAX_PARALLEL=4
VENDOR="${VENDOR:-vendor/hyperswitch-deja-clean}"
CANDS="self benign real earlier-fork dropped-write response-only extra-call transitive-chain"
PATCH_DIR="$(pwd)/demo/cross-version"
while [ $# -gt 0 ]; do case "$1" in
  --iterations) ITERATIONS="$2"; shift 2 ;;
  --keep) KEEP=1; shift ;;
  --max-parallel) MAX_PARALLEL="$2"; shift 2 ;;
  --candidates) CANDS="$2"; shift 2 ;;
  *) echo "unknown arg: $1"; exit 2 ;;
esac; done

patch_for() { case "$1" in
  self) echo "" ;;
  benign) echo "${PATCH_DIR}/benign-line-shift.patch" ;;
  real) echo "${PATCH_DIR}/real-change.patch" ;;
  earlier-fork) echo "${PATCH_DIR}/earlier-fork.patch" ;;
  dropped-write) echo "${PATCH_DIR}/dropped-write.patch" ;;
  response-only) echo "${PATCH_DIR}/response-only.patch" ;;
  extra-call) echo "${PATCH_DIR}/extra-call.patch" ;;
  transitive-chain) echo "${PATCH_DIR}/transitive-chain.patch" ;;
  eu-overcharge) echo "RETIRED: eu-overcharge targeted removed eu_settlement_read code; retarget before use" ;;
  *) echo "UNKNOWN" ;;
esac; }
expect_for() { case "$1" in self|benign) echo 0 ;; *) echo 1 ;; esac; }   # 0=PASS 1=CAUGHT
for label in $CANDS; do
  patch="$(patch_for "$label")"
  [ "$patch" = "UNKNOWN" ] && { echo "unknown candidate: $label"; exit 2; }
  case "$patch" in RETIRED:*) echo "$patch"; exit 2 ;; esac
  [ -z "$patch" ] || [ -f "$patch" ] || { echo "missing candidate patch: $patch"; exit 1; }
done

source demo/lib.sh
require_tools
echo "── preflighting selected candidate patches against ${VENDOR} ──"
for label in $CANDS; do
  patch="$(patch_for "$label")"
  [ -z "$patch" ] || check_candidate_patch "$patch"
done

init_run_state
echo "── run tag: ${RUN_TAG} · recording: ${REC_ID} · state: ${STATE_DIR} ──"
echo "── TRUE PARALLEL cross-version replay · max ${MAX_PARALLEL} concurrent isolated sandboxes ──"
echo "── versions: ${CANDS} ──"

BIN_DIR="$STATE_DIR/cand-bins"; mkdir -p "$BIN_DIR"
API_PID=""
cleanup() {
  revert_candidate_patch
  [ -n "$API_PID" ] && kill "$API_PID" 2>/dev/null || true
  if [ "$KEEP" -eq 0 ]; then
    echo "── tearing down shared record-side project (${PROJECT}) ──"
    docker compose -p "$PROJECT" -f "$BASE" -f "$OVERLAY" down -v >/dev/null 2>&1 || true
    echo "── sweeping any leaked per-run replay stacks (deja-run-*) ──"
    docker compose ls --all --format json 2>/dev/null | jq -r '.[].Name // empty' 2>/dev/null \
      | grep '^deja-run-' | while read -r p; do
          docker compose -p "$p" -f "$BASE" -f "$OVERLAY" down -v >/dev/null 2>&1 || true
        done || true
    echo "── removing per-run candidate images (deja-candidate:*) ──"
    docker images --format '{{.Repository}}:{{.Tag}}' 2>/dev/null \
      | grep '^deja-candidate:' | xargs -r docker rmi >/dev/null 2>&1 || true
  else echo "── stacks left running (--keep); state in $STATE_DIR ──"; fi
}
trap cleanup EXIT

echo "── building deja router (V1) + kernel + orchestrator + tui ──"
build_binaries
start_api

# ── RECORD ONCE: the golden V1 baseline (shared record stack) ────────────────
echo "── RECORD (V1 baseline): ${ITERATIONS} iteration(s); HS → Kafka → Vector → MinIO ──"
REC_RUN=$(post_run "$(jq -nc --arg r "$REC_ID" --argjson c "$candidate" --argjson it "$ITERATIONS" \
  '{mode:"record", candidate_spec:$c, recording_id:$r, workload:{iterations:$it}}')")
[ "$(poll "$REC_RUN")" = "completed" ] || { echo "RECORD failed"; curl -fsS "${API}/api/v1/runs/${REC_RUN}" | jq .; exit 1; }
echo "   baseline recorded → ${REC_ID}"

# ── BUILD N DISTINCT CANDIDATE BINARIES (sequential; build is the expensive part) ──
# self uses the V1 binary as-is; preserve it BEFORE any patched rebuild clobbers it.
declare -A CAND_BIN
if echo " $CANDS " | grep -q " self "; then
  mkdir -p "$BIN_DIR/self"; cp -f "$VENDOR/target/release/router" "$BIN_DIR/self/router"
  CAND_BIN[self]="$BIN_DIR/self/router"
fi
echo "── building distinct candidate binaries for: ${CANDS} ──"
for label in $CANDS; do
  [ "$label" = "self" ] && { echo "   self → V1 binary ($(sha256sum "$BIN_DIR/self/router" | cut -c1-12))"; continue; }
  patch=$(patch_for "$label")
  [ "$patch" = "UNKNOWN" ] && { echo "unknown candidate: $label"; exit 2; }
  apply_candidate_patch "$patch"
  rebuild_router_v2 "$label"
  revert_candidate_patch
  mkdir -p "$BIN_DIR/$label"
  cp -f "$VENDOR/target/release/router" "$BIN_DIR/$label/router"
  CAND_BIN[$label]="$BIN_DIR/$label/router"
  echo "   ${label} → ${CAND_BIN[$label]} ($(sha256sum "$BIN_DIR/$label/router" | cut -c1-12))"
done

# ── SUBMIT ALL VERSIONS AS CONCURRENT, ISOLATED LocalPath REPLAYS ────────────
CELL_RUNID=(); CELL_LABEL=(); CELL_EXPECT=()
throttle() {  # block until fewer than MAX_PARALLEL replays are in-flight
  while :; do
    local inflight=0 i st
    for i in "${!CELL_RUNID[@]}"; do
      st=$(curl -fsS "${API}/api/v1/runs/${CELL_RUNID[$i]}" 2>/dev/null | jq -r '.state // .live.status // "pending"')
      case "$st" in completed|failed) ;; *) inflight=$((inflight+1)) ;; esac
    done
    [ "$inflight" -lt "$MAX_PARALLEL" ] && return 0
    sleep 2
  done
}
echo
echo "── submitting ${CANDS} as CONCURRENT isolated LocalPath replays (same recording ${REC_ID}) ──"
for label in $CANDS; do
  throttle
  spec=$(jq -nc --arg p "${CAND_BIN[$label]}" '{kind:"local_path", binary_or_source:$p}')
  rid=$(post_run "$(jq -nc --arg r "$REC_ID" --argjson c "$spec" '{mode:"replay", candidate_spec:$c, recording_id:$r}')")
  echo "  ↪ ${label} → run ${rid}  (own sandbox: deja-run-$(echo "$rid" | tr -cd '[:alnum:]' | tail -c 8))"
  CELL_RUNID+=("$rid"); CELL_LABEL+=("$label"); CELL_EXPECT+=("$(expect_for "$label")")
done

# ── DRAIN ALL (they overlap; this just waits for the last one) ────────────────
echo
echo "── waiting for all ${#CELL_RUNID[@]} parallel replays to finish ──"
while :; do
  pending=0
  for i in "${!CELL_RUNID[@]}"; do
    st=$(curl -fsS "${API}/api/v1/runs/${CELL_RUNID[$i]}" 2>/dev/null | jq -r '.state // .live.status // "pending"')
    case "$st" in completed|failed) ;; *) pending=$((pending+1)) ;; esac
  done
  echo "   …${pending}/${#CELL_RUNID[@]} still running"
  [ "$pending" -eq 0 ] && break
  sleep 5
done

# ── COLLECT + SHOW ───────────────────────────────────────────────────────────
echo
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  DEJA CROSS-VERSION PARALLEL — one recording (${REC_ID}); each version its OWN sandbox"
echo "════════════════════════════════════════════════════════════════════════════════"
printf "  %-14s %-10s %-9s %-7s %-7s %-9s %-7s %s\n" "VERSION" "EXPECT" "VERDICT" "CAUGHT" "OK?" "MATCHED" "SIDEFX" "SANDBOX"
ALL_OK=1
for i in "${!CELL_RUNID[@]}"; do
  rid="${CELL_RUNID[$i]}"; label="${CELL_LABEL[$i]}"; expect_div="${CELL_EXPECT[$i]}"
  card=$(curl -fsS "${API}/api/v1/runs/${rid}/scorecard" 2>/dev/null || echo '{}')
  pass=$(echo "$card" | jq -r '.verdict.pass // "false"')
  matched=$(echo "$card" | jq -r '.summary.matched_correlations // 0')
  total=$(echo "$card" | jq -r '.summary.total_correlations // 0')
  diverg=$(echo "$card" | jq -r '.summary.side_effect_divergences // 0')
  caught=$( [ "$pass" = "true" ] && echo 0 || echo 1 )
  ok=0; if [ "$expect_div" -eq 1 ]; then [ "$caught" -eq 1 ] && ok=1; else [ "$caught" -eq 0 ] && ok=1; fi
  [ "$ok" -eq 1 ] && okmark="OK" || { okmark="XX"; ALL_OK=0; }
  exp=$( [ "$expect_div" -eq 1 ] && echo "caught" || echo "pass" )
  vd=$( [ "$pass" = "true" ] && echo "PASS" || echo "DIVERGE" )
  cm=$( [ "$caught" -eq 1 ] && echo "YES" || echo "no" )
  sb="deja-run-$(echo "$rid" | tr -cd '[:alnum:]' | tail -c 8)"
  printf "  %-14s %-10s %-9s %-7s %-7s %-9s %-7s %s\n" "$label" "$exp" "$vd" "$cm" "$okmark" "${matched}/${total}" "$diverg" "$sb"
done
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  Each version ran its OWN isolated stack (pg+redis+migrations+superposition+router),"
echo "  its OWN host port, its OWN candidate image (deja-candidate:<run8>), CONCURRENTLY,"
echo "  against the ONE shared recording. The orchestrator tore each sandbox down (down -v)."
echo "════════════════════════════════════════════════════════════════════════════════"
[ "$ALL_OK" -eq 1 ]
