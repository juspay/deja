#!/usr/bin/env bash
# Drive the SANDBOX custom pod so it produces a Deja recording.
#
# This is a thin wrapper over demo/workload.sh: same flow definition, different
# target. It exists because three things differ from the local compose rig:
#
#   1. Routing. All three custom pods share `sandbox.hyperswitch.io`; Istio
#      picks one by an exact `x-feature` header match. Every request in the flow
#      must carry it — one that doesn't lands on the stock sandbox router, which
#      has no Deja instrumentation. That request is then missing from the tape
#      while its side effects are real.
#   2. Provisioning. Sandbox is shared. An org + merchant + API key per flow is
#      antisocial and needs the environment's admin key, so the default here is
#      to reuse an existing merchant (WORKLOAD_API_KEY).
#   3. Correlation identity. The recorded correlation id is derived from the
#      request id, and this workload mints request ids deterministically from
#      WORKLOAD_RUN_LABEL. Reusing a label across two recordings collides their
#      correlation ids, so the label defaults to a timestamp.
#
# Recording itself needs no per-request trigger: the pod runs `deja.mode=record`
# with `sampler.enabled=true, fail_closed=false`, so it records what it serves.
# Events go Kafka -> Vector -> S3.
#
# Secrets: STRIPE_API_KEY / WORKLOAD_API_KEY / ADMIN_API_KEY are read from the
# environment and never echoed.
#
# Usage:
#   export WORKLOAD_API_KEY=<merchant api key>       # reuse an existing merchant
#   export WORKLOAD_MERCHANT_ID=<merchant id>
#   export WORKLOAD_SKIP_MCA=true                    # if it already has a Stripe connector
#   demo/workload-sandbox.sh [iterations]
#
#   demo/workload-sandbox.sh --pod c2 --dry-run
#
# To provision instead of reuse, leave WORKLOAD_API_KEY unset and export
# ADMIN_API_KEY plus STRIPE_API_KEY.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

POD="sandbox-custom"
DRY_RUN=false
ITERATIONS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pod)
      case "${2:-}" in
        ""|sandbox-custom|default) POD="sandbox-custom" ;;
        c2) POD="sandbox-custom-c2" ;;
        c3) POD="sandbox-custom-c3" ;;
        *)  POD="$2" ;;
      esac
      shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'; exit 0 ;;
    *) ITERATIONS="$1"; shift ;;
  esac
done

SANDBOX_BASE_URL="${SANDBOX_BASE_URL:-https://sandbox.hyperswitch.io}"
ROUTE_HEADER="${SANDBOX_ROUTE_HEADER:-x-feature: ${POD}}"

# A fresh label per recording, so correlation ids never collide across runs.
RUN_LABEL="${WORKLOAD_RUN_LABEL:-sbx-$(date +%Y%m%d-%H%M%S)}"

if [[ -z "${WORKLOAD_API_KEY:-}" && -z "${ADMIN_API_KEY:-}" ]]; then
  echo "ERROR: set WORKLOAD_API_KEY (reuse an existing merchant, preferred on a" >&2
  echo "       shared environment) or ADMIN_API_KEY (provision a new one)." >&2
  exit 2
fi

MODE="reuse merchant"
[[ -z "${WORKLOAD_API_KEY:-}" ]] && MODE="provision org+merchant+key (ADMIN_API_KEY)"

cat <<SUMMARY
── sandbox recording workload ──────────────────────────────────
  target       : $SANDBOX_BASE_URL
  route header : ${ROUTE_HEADER}
  mode         : ${MODE}
  mca create   : $([[ "${WORKLOAD_SKIP_MCA:-false}" == "true" ]] && echo "skipped" || echo "yes (needs STRIPE_API_KEY)")
  run label    : ${RUN_LABEL}
  iterations   : ${ITERATIONS:-100} (stops after ${WORKLOAD_MAX_SUCCESS:-5} successes)
  request ids  : deja-${RUN_LABEL}-flow-<NNN>-<step>
────────────────────────────────────────────────────────────────
SUMMARY

# Reachability preflight.
#
# Deliberately sent WITHOUT the route header. `/health` sits behind the same
# App-level middleware as every other route, so a probe carrying the header
# would land on the deja pod and be recorded — one junk correlation in every
# tape. Omitting it costs no coverage: an unmatched `x-feature` simply falls
# through to the default route, so a header-carrying probe returns 200 either
# way and proves nothing extra.
#
# Consequently this proves the gateway answers; it CANNOT prove the header
# steered us to the deja-enabled pod, because nothing in the response identifies
# the upstream. Confirm that from the recording side (see below).
probe() {
  # curl already emits `000` for a connection failure; a `|| echo 000` fallback
  # would concatenate a second one onto it.
  local out
  out="$(curl -s -o /dev/null -w '%{http_code}' \
    --connect-timeout 5 --max-time 15 \
    "$SANDBOX_BASE_URL/health" 2>/dev/null)"
  printf '%s' "${out:-000}"
}
code="$(probe)"
if [[ "$code" != "200" ]]; then
  echo "ERROR: GET /health returned ${code} (expected 200). Refusing to drive traffic." >&2
  echo "       Check VPN/network reachability to ${SANDBOX_BASE_URL}." >&2
  exit 1
fi
echo "preflight: GET /health -> 200 (unrouted; keeps the probe out of the tape)"
echo
echo "NOTE: a 200 does not prove the '${ROUTE_HEADER}' match steers you to the"
echo "      deja pod — the response carries no upstream identity. Verify by"
echo "      confirming the recording landed (see the tail of this run)."
echo

if [[ "$DRY_RUN" == "true" ]]; then
  echo "--dry-run: stopping before any payment is created."
  exit 0
fi

export BASE_URL="$SANDBOX_BASE_URL"
export WORKLOAD_RUN_LABEL="$RUN_LABEL"
export WORKLOAD_EXTRA_HEADERS="$ROUTE_HEADER"
export WORKLOAD_ARTIFACT_DIR="${WORKLOAD_ARTIFACT_DIR:-/tmp/deja-sandbox/${RUN_LABEL}}"
# Remote hop: the local compose timeouts are too tight.
export CURL_CONNECT_TIMEOUT_SECS="${CURL_CONNECT_TIMEOUT_SECS:-10}"
export CURL_MAX_TIME_SECS="${CURL_MAX_TIME_SECS:-30}"
export MCA_CREATE_MAX_TIME_SECS="${MCA_CREATE_MAX_TIME_SECS:-45}"
export PAYMENT_CREATE_MAX_TIME_SECS="${PAYMENT_CREATE_MAX_TIME_SECS:-45}"
export PAYMENT_CONFIRM_MAX_TIME_SECS="${PAYMENT_CONFIRM_MAX_TIME_SECS:-60}"

bash "$HERE/workload.sh" ${ITERATIONS:+"$ITERATIONS"}
rc=$?

echo
echo "── recording verification ──────────────────────────────────────"
echo "artifacts : ${WORKLOAD_ARTIFACT_DIR}"
echo "flows     : ${WORKLOAD_ARTIFACT_DIR}/payment-workload.jsonl"
echo
echo "The correlation ids for the replay filter are the request ids above."
echo "List them with:"
echo "  jq -r '.request_ids | to_entries[] | .value' \\"
echo "    ${WORKLOAD_ARTIFACT_DIR}/payment-workload.jsonl"
echo
echo "The recording reaches S3 via Kafka -> Vector. Confirm objects landed under"
echo "the run's prefix before starting a replay; an empty prefix means the route"
echo "header missed the deja pod, or the sampler declined."
echo "────────────────────────────────────────────────────────────────"
exit $rc
