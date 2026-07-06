#!/usr/bin/env bash
# One-click deja record→Kafka→MinIO→replay demo.
#
#   STRIPE_API_KEY=<stripe-test-key> demo/run-deja-demo.sh [--iterations N] [--keep]
#
# Pipeline:
#   1. build the kernel + orchestrator binaries; start the orchestrator API
#   2. POST a RECORD run  → Hyperswitch(record) drives workload.sh; events flow
#      HS → Kafka → Vector → MinIO  (S3-compatible)
#   3. POST a REPLAY run  → orchestrator pulls the recording back OUT of MinIO,
#      renders the lookup table, boots the SAME image under DEJA_MODE=replay,
#      drives the recorded requests, and scores byte-exact divergence
#   4. print the PASS/FAIL verdict
#
# Run from the repo root. Requires: docker (+ compose), cargo, curl, jq.
set -euo pipefail

cd "$(dirname "$0")/.."   # repo root

ITERATIONS=1
KEEP=0
# Cross-version mode: record on V1 (current source), then rebuild the host router
# binary from a patched ("V2 candidate") source tree and replay THAT against the
# V1 recording. `--candidate-patch <file>` takes a raw patch; `--cross-version
# <scenario>` resolves to demo/cross-version/<scenario>.patch. Empty = self-replay.
CANDIDATE_PATCH=""
VENDOR="${VENDOR:-vendor/hyperswitch-deja-clean}"
# Whether the V2 candidate is EXPECTED to diverge. For a benign candidate the
# success outcome is PASS (no false divergence); for a real-change candidate the
# success outcome is a DETECTED divergence. This flips the final verdict + exit
# code so the script is a correct CI gate either way. Inferred for a
# `--cross-version` scenario whose name contains "real"; override with
# `--expect-divergence` / `--expect-pass`.
EXPECT_DIVERGENCE=0
EXPECT_SET=0
while [ $# -gt 0 ]; do
  case "$1" in
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --candidate-patch) CANDIDATE_PATCH="$2"; shift 2 ;;
    --cross-version)
      CANDIDATE_PATCH="demo/cross-version/$2.patch"
      case "$2" in *real*) [ "$EXPECT_SET" -eq 0 ] && EXPECT_DIVERGENCE=1 ;; esac
      shift 2 ;;
    --expect-divergence) EXPECT_DIVERGENCE=1; EXPECT_SET=1; shift ;;
    --expect-pass) EXPECT_DIVERGENCE=0; EXPECT_SET=1; shift ;;
    *) echo "unknown arg: $1"; exit 2 ;;
  esac
done
if [ -n "$CANDIDATE_PATCH" ]; then
  if [ ! -f "$CANDIDATE_PATCH" ]; then
    echo "candidate patch not found: $CANDIDATE_PATCH"; exit 2
  fi
  # Resolve to an ABSOLUTE path: it is consumed by `git -C "$VENDOR" apply`, which
  # would otherwise look for it relative to the vendor dir, not the repo root.
  CANDIDATE_PATCH="$(cd "$(dirname "$CANDIDATE_PATCH")" && pwd)/$(basename "$CANDIDATE_PATCH")"
fi

# Shared constants + build/orchestrator/poll/candidate-patch helpers.
source demo/lib.sh

require_tools
init_run_state
echo "── run tag: ${RUN_TAG}  ·  recording id: ${REC_ID}  ·  state: ${STATE_DIR} ──"

API_PID=""
cleanup() {
  # Revert the V2 candidate patch FIRST, on every exit path (success / set -e
  # failure / Ctrl-C), so the dirty vendor working tree is restored to its
  # pre-run state.
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

echo "── building deja router + kernel + orchestrator + tui ──"
build_binaries
start_api

echo "── RECORD: drive ${ITERATIONS} workload iteration(s); HS → Kafka → Vector → MinIO ──"
REC_RUN=$(post_run "$(jq -nc --arg r "$REC_ID" --argjson c "$candidate" --argjson it "$ITERATIONS" \
  '{mode:"record", candidate_spec:$c, recording_id:$r, workload:{iterations:$it}}')")
[ "$(poll "$REC_RUN")" = "completed" ] || { echo "RECORD run failed — $(run_url "$REC_RUN")"; curl -fsS "${API}/api/v1/runs/${REC_RUN}" | jq .; exit 1; }
echo "   record complete → recording_id=${REC_ID}  ·  $(run_url "$REC_RUN")"

echo "── verify recording landed in MinIO (S3) ──"
RECORDING_ID="$REC_ID" docker compose -p "$PROJECT" -f "$BASE" -f "$OVERLAY" run --rm -T mc \
  "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; \
   mc ls --recursive local/deja-recordings/landing/v1/session=${REC_ID}/" || true

# ── CROSS-VERSION: apply the V2 candidate patch and rebuild the host binary ──
# The V1 recording is now frozen in MinIO (verified above). Patching + rebuilding
# the host router HERE means the replay container's `up --build` bakes V2, while
# the still-running V1 record container stays pinned to its V1 image ID. No
# orchestrator/compose change needed — the Dockerfile COPYs the host binary.
if [ -n "$CANDIDATE_PATCH" ]; then
  echo "── CROSS-VERSION: applying V2 candidate patch ($CANDIDATE_PATCH) ──"
  apply_candidate_patch "$CANDIDATE_PATCH"
  rebuild_router_v2 "candidate"
fi

echo "── REPLAY: pull from MinIO, byte-exact compare ──"
REP_RUN=$(post_run "$(jq -nc --arg r "$REC_ID" --argjson c "$candidate" \
  '{mode:"replay", candidate_spec:$c, recording_id:$r}')")
[ "$(poll "$REP_RUN")" = "completed" ] || { echo "REPLAY run failed — $(run_url "$REP_RUN")"; curl -fsS "${API}/api/v1/runs/${REP_RUN}" | jq .; exit 1; }
echo "   replay run: $(run_url "$REP_RUN")  ·  scorecard: ${API}/api/v1/runs/${REP_RUN}/scorecard"


CARD=$(curl -fsS "${API}/api/v1/runs/${REP_RUN}/scorecard")
PASS=$(echo "$CARD" | jq -r .verdict.pass)
REASON=$(echo "$CARD" | jq -r .verdict.reason)

# Decide whether the OBSERVED outcome matched the EXPECTED one. For a benign
# candidate, success = no divergence (PASS); for a real-change candidate, success
# = a detected divergence (!PASS). Self-replay success = PASS.
OUTCOME_OK=0
if [ -n "$CANDIDATE_PATCH" ] && [ "$EXPECT_DIVERGENCE" -eq 1 ]; then
  [ "$PASS" = "true" ] || OUTCOME_OK=1
else
  [ "$PASS" = "true" ] && OUTCOME_OK=1
fi

echo
echo "════════════════════════════════════════════════════════════"
if [ -n "$CANDIDATE_PATCH" ]; then
  # Cross-version run: V1 recorded, V2 ($CANDIDATE_PATCH) replayed against it.
  if [ "$EXPECT_DIVERGENCE" -eq 1 ]; then
    if [ "$OUTCOME_OK" -eq 1 ]; then
      echo "  ✅ CROSS-VERSION (real change): divergence correctly DETECTED — ${REASON}"
    else
      echo "  ❌ CROSS-VERSION (real change): gate MISSED it — V2 did not diverge"
    fi
  else
    if [ "$OUTCOME_OK" -eq 1 ]; then
      echo "  ✅ CROSS-VERSION (benign): V2 replayed V1's recording with NO false divergence"
    else
      echo "  ❌ CROSS-VERSION (benign): FALSE DIVERGENCE — a benign edit diverged — ${REASON}"
    fi
  fi
  echo "     candidate: ${CANDIDATE_PATCH}"
elif [ "$OUTCOME_OK" -eq 1 ]; then
  echo "  ✅ SELF-REPLAY PASS — same code, zero divergence"
else
  echo "  ❌ SELF-REPLAY FAIL — ${REASON}"
fi
echo "════════════════════════════════════════════════════════════"
echo "$CARD" | jq '{verdict, summary: {matched_correlations: .summary.matched_correlations,
  total_correlations: .summary.total_correlations,
  http_status_mismatches: .summary.http_status_mismatches,
  http_body_mismatches: .summary.http_body_mismatches,
  side_effect_divergences: .summary.side_effect_divergences,
  resolved_by_rank: .summary.resolved_by_rank}}'

# ── final stage: interactive record→replay substitution explorer ──────────────
# deja-tui is the real ratatui UI: ordered recorded events with how each was
# substituted (✓ from the lookup table) or executed live, a verdict banner, a
# per-boundary substitution panel, and the full substitution timeline. It needs
# a real TTY. On a non-interactive/background run we print the command to launch
# it and still generate the static Python HTML as a fallback.
echo
echo "── interactive replay explorer (record → replay substitution) ──"
TUI_BIN="$(pwd)/target/release/deja-tui"
if [ -t 1 ] && [ "${DEJA_NO_SERVE:-0}" != "1" ] && [ -x "$TUI_BIN" ]; then
  "$TUI_BIN" "$STATE_DIR" || true
else
  if [ -x "$TUI_BIN" ]; then
    echo "   ▶ interactive TUI:  $TUI_BIN \"$STATE_DIR\""
  fi
  python3 demo/visualize-replay.py "$STATE_DIR" || true
  echo "   ▶ static HTML:      $STATE_DIR/replay-visualization.html"
fi

# Exit 0 iff the observed outcome matched the expectation (benign→PASS,
# real-change→divergence, self-replay→PASS) — a correct CI gate either way.
[ "$OUTCOME_OK" -eq 1 ]
