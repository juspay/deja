#!/usr/bin/env bash
# Self-replay fidelity confirmation: record ONE V1 baseline, then replay the SAME
# unchanged V1 (PrebuiltImage, the faithful path the matrix uses) against it. A
# faithful self-replay MUST be PASS (0 side-effect divergences). If it diverges,
# the regression is in the merged code (seed-materialization/identity), NOT the
# LocalPath candidate image. Cheap controlled test before fixing.
#
#   STRIPE_API_KEY=<stripe-test-key> demo/run-self-check.sh
set -euo pipefail
cd "$(dirname "$0")/.."
VENDOR="${VENDOR:-vendor/hyperswitch-deja-clean}"   # build_binaries (lib.sh) needs this
source demo/lib.sh
require_tools
init_run_state
echo "── SELF-CHECK · recording ${REC_ID} · state ${STATE_DIR} ──"

API_PID=""
cleanup() {
  [ -n "$API_PID" ] && kill "$API_PID" 2>/dev/null || true
  if [ "${DEJA_KEEP_UP:-0}" = "1" ]; then
    echo "── DEJA_KEEP_UP=1 → leaving ${PROJECT} UP for inspection (tear down: docker compose -p ${PROJECT} -f ${BASE} -f ${OVERLAY} down -v) ──"
    return
  fi
  echo "── tearing down (${PROJECT}) ──"
  docker compose -p "$PROJECT" -f "$BASE" -f "$OVERLAY" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "── building deja router (V1) + kernel + orchestrator + tui ──"
build_binaries
start_api

echo "── RECORD V1 baseline (1 iteration) ──"
REC_RUN=$(post_run "$(jq -nc --arg r "$REC_ID" --argjson c "$candidate" --argjson it 1 \
  '{mode:"record", candidate_spec:$c, recording_id:$r, workload:{iterations:$it}}')")
[ "$(poll "$REC_RUN")" = "completed" ] || { echo "RECORD failed"; curl -fsS "${API}/api/v1/runs/${REC_RUN}" | jq .; exit 1; }
echo "   recorded → ${REC_ID}"

echo "── REPLAY self (same V1 binary, PrebuiltImage) ──"
REP=$(post_run "$(jq -nc --arg r "$REC_ID" --argjson c "$candidate" \
  '{mode:"replay", candidate_spec:$c, recording_id:$r}')" "pass")
[ "$(poll "$REP")" = "completed" ] || { echo "REPLAY failed"; curl -fsS "${API}/api/v1/runs/${REP}" | jq .; exit 1; }

echo
echo "════════════════ SELF-REPLAY SCORECARD (expect PASS, 0 side-effects) ════════════════"
card=$(curl -fsS "${API}/api/v1/runs/${REP}/scorecard")
echo "$card" | jq '{pass:.verdict.pass, reason:.verdict.reason, matched:.summary.matched_correlations, total:.summary.total_correlations, side_effect_divergences:.summary.side_effect_divergences, value_divergences:(.summary.value_divergences//0), http_body_mismatches:.summary.http_body_mismatches}'
pass=$(echo "$card" | jq -r '.verdict.pass')
if [ "$pass" = "true" ]; then
  echo "VERDICT: self PASSES under PrebuiltImage → regression was LocalPath-specific."
else
  echo "VERDICT: self DIVERGES under PrebuiltImage → CONFIRMED merged-code self-replay regression (seed/identity)."
fi
echo "   run_id=${REP}  state=${STATE_DIR}"