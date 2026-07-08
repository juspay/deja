#!/usr/bin/env bash
# Deja CROSS-VERSION MATRIX: record ONE V1 baseline, then replay each candidate
# once against that SAME recording. Replay routing is declared per boundary:
# Execute boundaries run and compare, Substitute boundaries serve recorded hits
# and fail-stop on misses.
#
#   STRIPE_API_KEY=<stripe-test-key> demo/run-deja-matrix.sh [--iterations N] [--keep]
#
#   self   — V1, unchanged                 → expect PASS        (faithful self-replay)
#   benign — V2, comment/line-shift edit   → expect PASS        (no false divergence)
#   real   — V2, changed persisted value   → expect DIVERGENCE  (the gate catches it)
#
# Why one recording, many replays: this is the correct mental model for
# regression detection — a single GOLDEN baseline recording, multiple candidate
# builds scored against it. The expensive real-Stripe workload runs ONCE, and every
# verdict is apples-to-apples against IDENTICAL expected behavior.
#
# Pipeline: HS(record) → Kafka → Vector → MinIO (recorded once); then for each
# candidate: rebuild the host router → orchestrator renders the lookup table from
# the SAME recording, boots the candidate in typed replay mode
# (ROUTER__DEJA__MODE=replay), drives the
# recorded requests, scores byte-exact divergence.
#
# Run from the repo root. Requires: docker (+ compose), cargo, curl, jq.
set -euo pipefail
cd "$(dirname "$0")/.."   # repo root

ITERATIONS=1
KEEP=0
VENDOR="${VENDOR:-vendor/hyperswitch-deja-clean}"
while [ $# -gt 0 ]; do
  case "$1" in
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    *) echo "unknown arg: $1"; exit 2 ;;
  esac
done

# The active default candidate patches (vendor-only; see demo/cross-version/README.md).
BENIGN_PATCH="$(pwd)/demo/cross-version/benign-line-shift.patch"
REAL_PATCH="$(pwd)/demo/cross-version/real-change.patch"
# Extra regression scenarios — each exercises a DISTINCT detector cell:
#   earlier-fork  — arg change at the payment_INTENT insert (before the attempt):
#                   modified pair (novel+omitted) on db, fork origin EARLIER than `real`.
#   dropped-write — candidate skips a fire-and-forget redis cache populate:
#                   pure OMITTED on redis, HTTP response IDENTICAL (silent regression).
#   response-only — overrides one response field, no boundary-call change:
#                   HTTP body mismatch with ZERO side-effect divergences.
#   extra-call    — candidate issues a db find V1 never made: pure NOVEL, no omitted pair.
#   transitive-chain — mutates ConfirmUpdate payment_method_data; returned attempt
#                      should alter the downstream ResponseUpdate args.
EARLIER_FORK_PATCH="$(pwd)/demo/cross-version/earlier-fork.patch"
DROPPED_WRITE_PATCH="$(pwd)/demo/cross-version/dropped-write.patch"
RESPONSE_ONLY_PATCH="$(pwd)/demo/cross-version/response-only.patch"
EXTRA_CALL_PATCH="$(pwd)/demo/cross-version/extra-call.patch"
TRANSITIVE_CHAIN_PATCH="$(pwd)/demo/cross-version/transitive-chain.patch"
for p in "$BENIGN_PATCH" "$REAL_PATCH" "$EARLIER_FORK_PATCH" "$DROPPED_WRITE_PATCH" "$RESPONSE_ONLY_PATCH" "$EXTRA_CALL_PATCH" "$TRANSITIVE_CHAIN_PATCH"; do
  [ -f "$p" ] || { echo "missing candidate patch: $p"; exit 1; }
done

# Shared constants + build/orchestrator/poll/candidate-patch helpers.
source demo/lib.sh

require_tools
echo "── preflighting active candidate patches against ${VENDOR} ──"
for p in "$(pwd)"/demo/cross-version/*.patch; do
  check_candidate_patch "$p"
done

init_run_state
echo "── run tag: ${RUN_TAG}  ·  baseline recording: ${REC_ID}  ·  state: ${STATE_DIR} ──"

API_PID=""
cleanup() {
  # Revert any in-flight candidate patch FIRST, on every exit path, so the dirty
  # vendor tree is restored even on failure/Ctrl-C.
  revert_candidate_patch
  [ -n "$API_PID" ] && kill "$API_PID" 2>/dev/null || true
  if [ "$KEEP" -eq 0 ]; then
    echo "── tearing down (use --keep to inspect) ──"
    docker compose -p "$PROJECT" -f "$BASE" -f "$OVERLAY" down -v >/dev/null 2>&1 || true
  else
    echo "── stack left running (--keep); state in $STATE_DIR ──"
  fi
}
trap cleanup EXIT

echo "── building deja router (V1) + kernel + orchestrator + tui ──"
build_binaries
start_api

# ── RECORD ONCE: the golden V1 baseline ──────────────────────────────────────
echo "── RECORD (V1 baseline): drive ${ITERATIONS} workload iteration(s); HS → Kafka → Vector → MinIO ──"
REC_RUN=$(post_run "$(jq -nc --arg r "$REC_ID" --argjson c "$candidate" --argjson it "$ITERATIONS" \
  '{mode:"record", candidate_spec:$c, recording_id:$r, workload:{iterations:$it}}')")
[ "$(poll "$REC_RUN")" = "completed" ] || { echo "RECORD run failed"; curl -fsS "${API}/api/v1/runs/${REC_RUN}" | jq .; exit 1; }
echo "   baseline recorded → recording_id=${REC_ID}"
RECORDING_ID="$REC_ID" docker compose -p "$PROJECT" -f "$BASE" -f "$OVERLAY" run --rm -T mc \
  "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; \
   mc ls --recursive local/deja-recordings/landing/v1/session=${REC_ID}/" || true

# PARTIAL-FUNCTION MODEL: there is no global routing axis. Replay honors each
# per-site declared strategy (Execute = run + diff; Substitute = serve on a hit,
# fail-stop on a miss), intermixed. A single run per candidate is the complete
# check.

# Assemble a clean, single-run view dir for one candidate so deja-tui /
# visualize-replay.py (which each expect ONE run per dir) render it unambiguously.
assemble_view() { # assemble_view <label> <run_id>
  local label="$1" rid="$2" vdir="$STATE_DIR/view-$label"
  mkdir -p "$vdir/observed" "$vdir/http-diffs" "$vdir/lookup-tables" "$vdir/runs"
  ln -sfn ../recordings "$vdir/recordings" 2>/dev/null || true
  ln -sfn ../recording  "$vdir/recording"  2>/dev/null || true
  for sub in observed http-diffs lookup-tables; do
    [ -f "$STATE_DIR/$sub/$rid.jsonl" ] && cp -f "$STATE_DIR/$sub/$rid.jsonl" "$vdir/$sub/" || true
  done
  for ext in json scorecard.json; do
    [ -f "$STATE_DIR/runs/$rid.$ext" ] && cp -f "$STATE_DIR/runs/$rid.$ext" "$vdir/runs/" || true
  done
  python3 demo/visualize-replay.py "$vdir" >/dev/null 2>&1 || true
}

# ── REPLAY one candidate against the SAME baseline recording ─────────────────
RESULTS=()  # "label|expect_div|pass|caught|ok|matched|total|diverg|httpbody|run_id|reason"

replay_run() { # replay_run <label> <expect_divergence 0|1>
  local label="$1" expect_div="$2"
  echo
  echo "  ── ${label} · expect $( [ "$expect_div" -eq 1 ] && echo DIVERGENCE || echo PASS ) ──"

  local rep card pass reason matched total diverg httpbody caught ok
  local expect_note=$( [ "$expect_div" -eq 1 ] && echo "diverge" || echo "pass" )
  rep=$(post_run "$(jq -nc --arg r "$REC_ID" --argjson c "$candidate" \
    '{mode:"replay", candidate_spec:$c, recording_id:$r}')" "$expect_note")
  [ "$(poll "$rep")" = "completed" ] || { echo "REPLAY ($label) run failed"; curl -fsS "${API}/api/v1/runs/${rep}" | jq .; exit 1; }
  card=$(curl -fsS "${API}/api/v1/runs/${rep}/scorecard")
  pass=$(echo "$card"   | jq -r '.verdict.pass')
  reason=$(echo "$card" | jq -r '.verdict.reason')
  matched=$(echo "$card" | jq -r '.summary.matched_correlations')
  total=$(echo "$card"   | jq -r '.summary.total_correlations')
  diverg=$(echo "$card"  | jq -r '.summary.side_effect_divergences')
  httpbody=$(echo "$card" | jq -r '.summary.http_body_mismatches')

  caught=$( [ "$pass" = "true" ] && echo 0 || echo 1 )

  ok=0
  if [ "$expect_div" -eq 1 ]; then [ "$caught" -eq 1 ] && ok=1; else [ "$caught" -eq 0 ] && ok=1; fi
  if [ "$ok" -eq 1 ]; then echo "  ✅ ${label}: outcome matched expectation"; else echo "  ❌ ${label}: outcome did NOT match expectation — ${reason}"; fi
  echo "$card" | jq '{verdict: .verdict.pass, matched: .summary.matched_correlations, total: .summary.total_correlations, side_effect_divergences: .summary.side_effect_divergences, http_body_mismatches: .summary.http_body_mismatches, value_diverged: (.summary.value_divergences // 0), resolved_by_rank: .summary.resolved_by_rank}'

  assemble_view "$label" "$rep"
  RESULTS+=("$label|$expect_div|$pass|$caught|$ok|$matched|$total|$diverg|$httpbody|$rep|$reason")
}

# Replay one candidate against the SAME baseline recording.
replay_candidate() { # replay_candidate <label> <patch|""> <expect_div 0|1>
  local label="$1" patch="$2" expect_div="$3"
  echo
  echo "════════════════════════════════════════════════════════════"
  echo "  CANDIDATE: ${label}  (expect $( [ "$expect_div" -eq 1 ] && echo DIVERGENCE || echo PASS ))"
  echo "════════════════════════════════════════════════════════════"

  if [ -n "$patch" ]; then
    apply_candidate_patch "$patch"
    rebuild_router_v2 "$label"
  fi

  replay_run "$label" "$expect_div"

  # Revert the candidate patch so the source is back to V1 for the next candidate.
  revert_candidate_patch
}

# self FIRST (V1 binary as built at startup — no rebuild), then the V2 candidates.
replay_candidate "self"          ""                     0
replay_candidate "benign"        "$BENIGN_PATCH"        0
replay_candidate "real"          "$REAL_PATCH"          1
replay_candidate "earlier-fork"  "$EARLIER_FORK_PATCH"  1
replay_candidate "dropped-write" "$DROPPED_WRITE_PATCH" 1
replay_candidate "response-only" "$RESPONSE_ONLY_PATCH" 1
replay_candidate "extra-call"    "$EXTRA_CALL_PATCH"    1
replay_candidate "transitive-chain" "$TRANSITIVE_CHAIN_PATCH" 1

# ── SUMMARY MATRIX ───────────────────────────────────────────────────────────
echo
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  DEJA MATRIX — one V1 recording (${REC_ID}); one replay per candidate"
echo "════════════════════════════════════════════════════════════════════════════════"
printf "  %-13s %-12s %-9s %-7s %-7s %-9s %s\n" "CAND" "EXPECT" "VERDICT" "CAUGHT" "OK?" "MATCHED" "DIVERG"
ALL_OK=1
for r in "${RESULTS[@]}"; do
  IFS='|' read -r label expect_div pass caught ok matched total diverg httpbody run_id reason <<<"$r"
  exp=$( [ "$expect_div" -eq 1 ] && echo "diverge" || echo "pass" )
  vd=$( [ "$pass" = "true" ] && echo "PASS" || echo "DIVERGE" )
  caughtmark=$( [ "$caught" -eq 1 ] && echo "YES" || echo "no" )
  if [ "$ok" -eq 1 ]; then okmark="✅"; else okmark="❌"; ALL_OK=0; fi
  printf "  %-13s %-12s %-9s %-7s %-7s %-9s %s\n" "$label" "$exp" "$vd" "$caughtmark" "$okmark" "${matched}/${total}" "$diverg"
done
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  CAUGHT = replay flagged a divergence."
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  per-candidate replay reports:"
for r in "${RESULTS[@]}"; do
  IFS='|' read -r label _ _ _ _ _ _ _ _ run_id _ <<<"$r"
  echo "    ${label}:"
  echo "          web  → $(run_url "$run_id")"
  echo "          card → ${API}/api/v1/runs/${run_id}/scorecard"
  echo "          TUI  → target/release/deja-tui \"$STATE_DIR/view-${label}\""
  echo "          HTML → $STATE_DIR/view-${label}/replay-visualization.html"
done
echo "════════════════════════════════════════════════════════════════════════"

# Exit 0 iff EVERY candidate's outcome matched its expectation — a correct gate
# for the whole matrix.
[ "$ALL_OK" -eq 1 ]
