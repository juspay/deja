#!/usr/bin/env bash
# Realistic Hyperswitch API workload:
# Org → Merchant → API Key → Stripe MCA → Payment Create → Payment Confirm
#
# Secrets:
# - Stripe credentials must be supplied via STRIPE_API_KEY.
# - The Stripe key is sent to Hyperswitch on curl stdin (--data-binary @-),
#   never as a literal command-line argument or log line.

set -uo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
ADMIN_API_KEY="${ADMIN_API_KEY:-test_admin}"
ARTIFACT_DIR="${WORKLOAD_ARTIFACT_DIR:-/tmp/deja-pipeline}"
LOG_FILE="${WORKLOAD_LOG:-$ARTIFACT_DIR/workload.log}"
WORKLOAD_RUN_LABEL="${WORKLOAD_RUN_LABEL:-workload}"

if [[ -z "${STRIPE_API_KEY:-}" ]]; then
  echo "ERROR: STRIPE_API_KEY is required for the Stripe payment-confirm workload." >&2
  echo "Set it without exposing it in shell history:" >&2
  echo "  read -rsp 'Stripe test key: ' STRIPE_API_KEY; echo; export STRIPE_API_KEY" >&2
  exit 2
fi

PAYMENT_WORKLOAD_JSONL="${PAYMENT_WORKLOAD_JSONL:-$ARTIFACT_DIR/payment-workload.jsonl}"
PAYMENT_WORKLOAD_SUMMARY="${PAYMENT_WORKLOAD_SUMMARY:-$ARTIFACT_DIR/workload-summary.json}"
PAYMENT_CREATE_LATENCY_FILE="${PAYMENT_CREATE_LATENCY_FILE:-$ARTIFACT_DIR/payment_create_latency.txt}"
PAYMENT_CONFIRM_LATENCY_FILE="${PAYMENT_CONFIRM_LATENCY_FILE:-$ARTIFACT_DIR/payment_confirm_latency.txt}"
PAYMENT_FLOW_LATENCY_FILE="${PAYMENT_FLOW_LATENCY_FILE:-$ARTIFACT_DIR/payment_flow_latency.txt}"
CURL_CONNECT_TIMEOUT_SECS="${CURL_CONNECT_TIMEOUT_SECS:-5}"
CURL_MAX_TIME_SECS="${CURL_MAX_TIME_SECS:-20}"
MCA_CREATE_MAX_TIME_SECS="${MCA_CREATE_MAX_TIME_SECS:-30}"
PAYMENT_CREATE_MAX_TIME_SECS="${PAYMENT_CREATE_MAX_TIME_SECS:-30}"
PAYMENT_CONFIRM_MAX_TIME_SECS="${PAYMENT_CONFIRM_MAX_TIME_SECS:-30}"
WORKLOAD_REQUIRE_CONFIRM_SUCCESS="${WORKLOAD_REQUIRE_CONFIRM_SUCCESS:-false}"
WORKLOAD_FAIL_ON_ANY_ERROR="${WORKLOAD_FAIL_ON_ANY_ERROR:-false}"

mkdir -p "$ARTIFACT_DIR" "$(dirname "$LOG_FILE")"

# Clear per-run artifacts.
: > "$LOG_FILE"
: > "$PAYMENT_WORKLOAD_JSONL"
: > "$PAYMENT_CREATE_LATENCY_FILE"
: > "$PAYMENT_CONFIRM_LATENCY_FILE"
: > "$PAYMENT_FLOW_LATENCY_FILE"

log() { printf "%s %s\n" "$(date +%H:%M:%S 2>/dev/null || echo '??:??:??')" "$1" >> "$LOG_FILE"; }

random_id() { openssl rand -hex 8 2>/dev/null || dd if=/dev/urandom bs=8 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n'; }

CURRENT_FLOW_ID=""

semantic_slug() {
  printf "%s" "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | tr -c 'a-z0-9-' '-' \
    | sed -E 's/^-+//; s/-+$//; s/-+/-/g'
}

semantic_request_id() {
  local run_label flow_id step
  run_label="$(semantic_slug "$WORKLOAD_RUN_LABEL")"
  flow_id="$(semantic_slug "$CURRENT_FLOW_ID")"
  step="$(semantic_slug "$1")"
  printf "deja-%s-%s-%s" "$run_label" "$flow_id" "$step"
}

now_ms() {
  local ns
  ns=$(date +%s%N 2>/dev/null || true)
  if [[ "$ns" =~ ^[0-9]+$ ]]; then
    echo $((ns / 1000000))
  else
    echo $(( $(date +%s) * 1000 ))
  fi
}

ms_from_seconds() {
  awk -v s="$1" 'BEGIN { if (s == "") s = 0; printf "%.3f", s * 1000 }'
}

is_2xx() { [[ "$1" =~ ^2[0-9][0-9]$ ]]; }

# User Signup
user_signup() {
  local email="$1"
  curl -sf --connect-timeout "$CURL_CONNECT_TIMEOUT_SECS" --max-time "$CURL_MAX_TIME_SECS" -X POST "$BASE_URL/user/signup" \
    -H "Content-Type: application/json" \
    -H "x-request-id: $(semantic_request_id user-signup)" \
    -d "{\"email\":\"$email\",\"password\":\"DejaDemo123!\"}" 2>/dev/null | jq -r '.token' 2>/dev/null || echo ""
}

# User Signin
user_signin() {
  local email="$1"
  curl -sf --connect-timeout "$CURL_CONNECT_TIMEOUT_SECS" --max-time "$CURL_MAX_TIME_SECS" -X POST "$BASE_URL/user/signin" \
    -H "Content-Type: application/json" \
    -H "x-request-id: $(semantic_request_id user-signin)" \
    -d "{\"email\":\"$email\",\"password\":\"DejaDemo123!\"}" 2>/dev/null | jq -r '.token' 2>/dev/null || echo ""
}

# Org Create
org_create() {
  local org_name="org_$(random_id)"
  curl -sf --connect-timeout "$CURL_CONNECT_TIMEOUT_SECS" --max-time "$CURL_MAX_TIME_SECS" -X POST "$BASE_URL/organization" \
    -H "Content-Type: application/json" \
    -H "api-key: $ADMIN_API_KEY" \
    -H "x-request-id: $(semantic_request_id org-create)" \
    -d "{\"organization_name\":\"$org_name\"}" 2>/dev/null | jq -r '.organization_id' 2>/dev/null || echo ""
}

# Merchant Create
merchant_create() {
  local org_id="$1"
  local merch_id="merch_$(random_id)"
  curl -sf --connect-timeout "$CURL_CONNECT_TIMEOUT_SECS" --max-time "$CURL_MAX_TIME_SECS" -X POST "$BASE_URL/accounts" \
    -H "Content-Type: application/json" \
    -H "api-key: $ADMIN_API_KEY" \
    -H "x-request-id: $(semantic_request_id merchant-create)" \
    -d "{\"merchant_id\":\"$merch_id\",\"merchant_name\":\"$merch_id\",\"organization_id\":\"$org_id\",\"return_url\":\"http://localhost:8080\"}" 2>/dev/null | jq -r '.merchant_id' 2>/dev/null || echo ""
}

# API Key Create
apikey_create() {
  local merch_id="$1"
  curl -sf --connect-timeout "$CURL_CONNECT_TIMEOUT_SECS" --max-time "$CURL_MAX_TIME_SECS" -X POST "$BASE_URL/api_keys/$merch_id" \
    -H "Content-Type: application/json" \
    -H "api-key: $ADMIN_API_KEY" \
    -H "x-request-id: $(semantic_request_id api-key-create)" \
    -d '{"name":"benchmark-key","expiration":"never","description":"load test"}' 2>/dev/null | jq -r '.api_key' 2>/dev/null || echo ""
}

# Merchant Retrieve (exercises admin read path; Redis is still used elsewhere
# in the flow through auth/cache/locking/router behavior)
merchant_retrieve() {
  local merch_id="$1"
  curl -sf --connect-timeout "$CURL_CONNECT_TIMEOUT_SECS" --max-time "$CURL_MAX_TIME_SECS" "$BASE_URL/accounts/$merch_id" \
    -H "x-request-id: $(semantic_request_id merchant-retrieve)" \
    -H "api-key: $ADMIN_API_KEY" 2>/dev/null | jq -r '.merchant_id' 2>/dev/null || echo ""
}

# Create Merchant Connector Account (Stripe)
mca_create() {
  local merch_id="$1"
  local api_key="$2"
  local response code elapsed_s elapsed_ms

  # Stripe key is expanded into stdin only; it is not present in curl argv.
  response=$(curl -sS --connect-timeout "$CURL_CONNECT_TIMEOUT_SECS" --max-time "$MCA_CREATE_MAX_TIME_SECS" -o /dev/null -w "%{http_code} %{time_total}" \
    -X POST "$BASE_URL/account/$merch_id/connectors" \
    -H "Content-Type: application/json" \
    -H "api-key: $api_key" \
    -H "x-request-id: $(semantic_request_id mca-create)" \
    --data-binary @- 2>/dev/null <<EOF
{"connector_type":"payment_processor","connector_name":"stripe","connector_account_details":{"auth_type":"HeaderKey","api_key":"$STRIPE_API_KEY"}}
EOF
  ) || response="000 0"

  code=$(echo "$response" | awk '{print $1}')
  elapsed_s=$(echo "$response" | awk '{print $2}')
  elapsed_ms=$(ms_from_seconds "$elapsed_s")
  printf "%s %s\n" "${code:-000}" "$elapsed_ms"
}

# Create Payment (PG write + Redis API locking)
payment_create() {
  local api_key="$1"
  local amount="${2:-100}"
  local body_file response code elapsed_s elapsed_ms payment_id status

  body_file=$(mktemp)
  response=$(curl -sS --connect-timeout "$CURL_CONNECT_TIMEOUT_SECS" --max-time "$PAYMENT_CREATE_MAX_TIME_SECS" -o "$body_file" -w "%{http_code} %{time_total}" \
    -X POST "$BASE_URL/payments" \
    -H "Content-Type: application/json" \
    -H "api-key: $api_key" \
    -H "x-request-id: $(semantic_request_id payment-create)" \
    --data-binary @- 2>/dev/null <<JSON
{"amount":$amount,"currency":"USD","confirm":false,"capture_method":"automatic","authentication_type":"no_three_ds","connector":["stripe"],"return_url":"https://example.com"}
JSON
  ) || response="000 0"

  code=$(echo "$response" | awk '{print $1}')
  elapsed_s=$(echo "$response" | awk '{print $2}')
  elapsed_ms=$(ms_from_seconds "$elapsed_s")
  payment_id=$(jq -r '.payment_id // empty' "$body_file" 2>/dev/null || echo "")
  status=$(jq -r '.status // empty' "$body_file" 2>/dev/null || echo "")
  rm -f "$body_file"

  printf "%s %s %s %s\n" "${code:-000}" "$elapsed_ms" "$payment_id" "$status"
}

# Confirm Payment (real connector call with Stripe test card + browser info)
payment_confirm() {
  local api_key="$1"
  local payment_id="$2"
  local body_file response code elapsed_s elapsed_ms confirmed_payment_id status

  body_file=$(mktemp)
  response=$(curl -sS --connect-timeout "$CURL_CONNECT_TIMEOUT_SECS" --max-time "$PAYMENT_CONFIRM_MAX_TIME_SECS" -o "$body_file" -w "%{http_code} %{time_total}" \
    -X POST "$BASE_URL/payments/$payment_id/confirm" \
    -H "Content-Type: application/json" \
    -H "api-key: $api_key" \
    -H "x-request-id: $(semantic_request_id payment-confirm)" \
    --data-binary @- 2>/dev/null <<'JSON'
{
  "payment_method": "card",
  "payment_method_type": "credit",
  "payment_method_data": {
    "card": {
      "card_number": "4242424242424242",
      "card_exp_month": "10",
      "card_exp_year": "40",
      "card_holder_name": "morino",
      "card_cvc": "737"
    }
  },
  "browser_info": {
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36",
    "accept_header": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "language": "en-US",
    "color_depth": 24,
    "screen_height": 723,
    "screen_width": 1536,
    "time_zone": 0,
    "java_enabled": true,
    "java_script_enabled": true,
    "ip_address": "127.0.0.1"
  },
  "return_url": "https://example.com"
}
JSON
  ) || response="000 0"

  code=$(echo "$response" | awk '{print $1}')
  elapsed_s=$(echo "$response" | awk '{print $2}')
  elapsed_ms=$(ms_from_seconds "$elapsed_s")
  confirmed_payment_id=$(jq -r '.payment_id // empty' "$body_file" 2>/dev/null || echo "")
  status=$(jq -r '.status // empty' "$body_file" 2>/dev/null || echo "")
  rm -f "$body_file"

  printf "%s %s %s %s\n" "${code:-000}" "$elapsed_ms" "$confirmed_payment_id" "$status"
}

record_payment_metrics() {
  local email="$1" org_id="$2" merch_id="$3" payment_id="$4" payment_status="$5"
  local mca_ms="$6" create_ms="$7" confirm_ms="$8" total_flow_ms="$9"
  local run_label="$WORKLOAD_RUN_LABEL" flow_id="$CURRENT_FLOW_ID"

  jq -nc \
    --arg run_label "$run_label" \
    --arg flow_id "$flow_id" \
    --arg req_user_signup "$(semantic_request_id user-signup)" \
    --arg req_user_signin "$(semantic_request_id user-signin)" \
    --arg req_org_create "$(semantic_request_id org-create)" \
    --arg req_merchant_create "$(semantic_request_id merchant-create)" \
    --arg req_api_key_create "$(semantic_request_id api-key-create)" \
    --arg req_merchant_retrieve "$(semantic_request_id merchant-retrieve)" \
    --arg req_mca_create "$(semantic_request_id mca-create)" \
    --arg req_payment_create "$(semantic_request_id payment-create)" \
    --arg req_payment_confirm "$(semantic_request_id payment-confirm)" \
    --arg email "$email" \
    --arg org_id "$org_id" \
    --arg merch_id "$merch_id" \
    --arg payment_id "$payment_id" \
    --arg payment_status "$payment_status" \
    --arg mca_ms "$mca_ms" \
    --arg create_ms "$create_ms" \
    --arg confirm_ms "$confirm_ms" \
    --arg total_flow_ms "$total_flow_ms" \
    '{
      ts: now | todateiso8601,
      run_label: $run_label,
      flow_id: $flow_id,
      request_ids: {
        user_signup: $req_user_signup,
        user_signin: $req_user_signin,
        org_create: $req_org_create,
        merchant_create: $req_merchant_create,
        api_key_create: $req_api_key_create,
        merchant_retrieve: $req_merchant_retrieve,
        mca_create: $req_mca_create,
        payment_create: $req_payment_create,
        payment_confirm: $req_payment_confirm
      },
      email: $email,
      org_id: $org_id,
      merchant_id: $merch_id,
      payment_id: $payment_id,
      payment_status: $payment_status,
      mca_create_ms: ($mca_ms | tonumber),
      payment_create_ms: ($create_ms | tonumber),
      payment_confirm_ms: ($confirm_ms | tonumber),
      total_flow_ms: ($total_flow_ms | tonumber)
    }' \
    >> "$PAYMENT_WORKLOAD_JSONL"
}

# Full flow
run_flow() {
  local iteration="${1:-1}"
  local email="user_$(random_id)@deja.dev"
  CURRENT_FLOW_ID="$(printf "flow-%03d" "$iteration")"

  # Signup (writes user to PG)
  local token
  token=$(user_signup "$email")
  [[ -z "$token" ]] && { log "FAIL user_signup"; return 1; }

  # Signin (reads user from PG + Redis)
  token=$(user_signin "$email")
  [[ -z "$token" ]] && { log "FAIL user_signin"; return 1; }

  # Create org (admin API, writes to PG)
  local org_id
  org_id=$(org_create)
  [[ -z "$org_id" ]] && { log "FAIL org_create"; return 1; }

  # Create merchant (admin API, writes to PG + Redis)
  local merch_id
  merch_id=$(merchant_create "$org_id")
  [[ -z "$merch_id" ]] && { log "FAIL merchant_create"; return 1; }

  # Create API key (admin API, writes to PG)
  local api_key
  api_key=$(apikey_create "$merch_id")
  [[ -z "$api_key" ]] && { log "FAIL apikey_create"; return 1; }

  # Retrieve merchant (admin API, exercises a read before payment setup)
  local retrieved
  retrieved=$(merchant_retrieve "$merch_id")
  [[ -z "$retrieved" ]] && { log "FAIL merchant_retrieve"; return 1; }

  # Do not enable merchant KV here: the current Hyperswitch payment-confirm
  # path fails on cached PaymentAttempt deserialization. Redis still remains
  # active in the flow through locks and other router/cache behavior.

  local flow_start_ms flow_end_ms total_flow_ms
  flow_start_ms=$(now_ms)

  # Create MCA with Stripe (required for payments, uses merchant API key)
  local mca_status mca_ms
  read -r mca_status mca_ms <<< "$(mca_create "$merch_id" "$api_key")"
  if ! is_2xx "$mca_status"; then
    log "FAIL mca_create status=$mca_status"
    return 1
  fi

  # Create payment intent using the Stripe connector.
  local create_status create_ms pay_id pay_status
  read -r create_status create_ms pay_id pay_status <<< "$(payment_create "$api_key" 100)"
  if ! is_2xx "$create_status" || [[ -z "$pay_id" ]]; then
    log "FAIL payment_create status=$create_status payment_id=${pay_id:-empty}"
    return 1
  fi

  # Confirm payment with Stripe test card and browser info.
  # Note: Payment confirm is best-effort — if Stripe is unreachable, we still
  # record the payment create latency so the benchmark can complete.
  local confirm_status confirm_ms confirmed_pay_id confirmed_status confirm_ok=false
  read -r confirm_status confirm_ms confirmed_pay_id confirmed_status <<< "$(payment_confirm "$api_key" "$pay_id")"
  if is_2xx "$confirm_status" && [[ "$confirmed_status" != "failed" && -n "$confirmed_status" ]]; then
    confirm_ok=true
    CONFIRM_SUCCESS=$((CONFIRM_SUCCESS + 1))
  else
    CONFIRM_FAIL=$((CONFIRM_FAIL + 1))
    log "WARN payment_confirm status=${confirm_status:-empty} payment_status=${confirmed_status:-empty} — continuing with create metrics only"
    if [[ "$WORKLOAD_REQUIRE_CONFIRM_SUCCESS" == "true" ]]; then
      return 1
    fi
  fi

  flow_end_ms=$(now_ms)
  total_flow_ms=$(awk -v start="$flow_start_ms" -v end="$flow_end_ms" 'BEGIN { printf "%.3f", end - start }')

  # Always record create latency (this is the core metric we need)
  printf "%s\n" "$create_ms" >> "$PAYMENT_CREATE_LATENCY_FILE"
  
  # Record confirm latency only if it succeeded
  if [[ "$confirm_ok" == "true" ]]; then
    printf "%s\n" "$confirm_ms" >> "$PAYMENT_CONFIRM_LATENCY_FILE"
  fi
  
  # Record flow latency (total time even if confirm failed)
  printf "%s\n" "$total_flow_ms" >> "$PAYMENT_FLOW_LATENCY_FILE"
  
  record_payment_metrics "$email" "$org_id" "$merch_id" "$pay_id" "${confirmed_status:-pending}" "$mca_ms" "$create_ms" "${confirm_ms:-0}" "$total_flow_ms"

  log "OK flow_complete email=$email org=$org_id merch=$merch_id pay=$pay_id status=${confirmed_status:-pending} mca_ms=$mca_ms create_ms=$create_ms confirm_ms=${confirm_ms:-N/A} total_ms=$total_flow_ms"
  return 0
}

# Main: run N iterations
ITERATIONS="${1:-100}"
SUCCESS=0
FAIL=0
CONFIRM_SUCCESS=0
CONFIRM_FAIL=0

log "Starting payment workload: $ITERATIONS iterations"

for i in $(seq 1 "$ITERATIONS"); do
  if run_flow "$i"; then
    SUCCESS=$((SUCCESS + 1))
  else
    FAIL=$((FAIL + 1))
    # Continue on failure — we need at least some successful iterations for benchmark
    log "WARN iteration $i failed, continuing..."
  fi
  
  # Stop early if we've collected enough successful samples
  if [ "$SUCCESS" -ge 5 ]; then
    log "Collected $SUCCESS successful samples, stopping early"
    break
  fi
done

log "Complete: SUCCESS=$SUCCESS FAIL=$FAIL"
echo "$SUCCESS $FAIL"

create_samples=$(wc -l < "$PAYMENT_CREATE_LATENCY_FILE" 2>/dev/null || echo 0)
confirm_samples=$(wc -l < "$PAYMENT_CONFIRM_LATENCY_FILE" 2>/dev/null || echo 0)
flow_samples=$(wc -l < "$PAYMENT_FLOW_LATENCY_FILE" 2>/dev/null || echo 0)

jq -nc \
  --arg run_label "$WORKLOAD_RUN_LABEL" \
  --argjson requested_iterations "$ITERATIONS" \
  --argjson success "$SUCCESS" \
  --argjson fail "$FAIL" \
  --argjson confirm_success "$CONFIRM_SUCCESS" \
  --argjson confirm_fail "$CONFIRM_FAIL" \
  --argjson create_samples "$create_samples" \
  --argjson confirm_samples "$confirm_samples" \
  --argjson flow_samples "$flow_samples" \
  --arg require_confirm_success "$WORKLOAD_REQUIRE_CONFIRM_SUCCESS" \
  --arg fail_on_any_error "$WORKLOAD_FAIL_ON_ANY_ERROR" \
  '{
    schema_version: "deja.payment-workload.summary/v1",
    run_label: $run_label,
    latency_unit: "ms",
    requested_iterations: $requested_iterations,
    success: $success,
    fail: $fail,
    confirm_success: $confirm_success,
    confirm_fail: $confirm_fail,
    samples: {
      payment_create: $create_samples,
      payment_confirm: $confirm_samples,
      payment_flow: $flow_samples
    },
    policy: {
      require_confirm_success: ($require_confirm_success == "true"),
      fail_on_any_error: ($fail_on_any_error == "true")
    }
  }' > "$PAYMENT_WORKLOAD_SUMMARY"

# Only fail if we got ZERO successful iterations
if [ "$SUCCESS" -eq 0 ]; then
  log "ERROR: zero successful iterations"
  exit 1
fi

if [[ "$WORKLOAD_FAIL_ON_ANY_ERROR" == "true" && "$FAIL" -gt 0 ]]; then
  log "ERROR: workload failures present under WORKLOAD_FAIL_ON_ANY_ERROR=true"
  exit 1
fi

if [[ "$WORKLOAD_REQUIRE_CONFIRM_SUCCESS" == "true" && "$CONFIRM_FAIL" -gt 0 ]]; then
  log "ERROR: connector confirm failures present under WORKLOAD_REQUIRE_CONFIRM_SUCCESS=true"
  exit 1
fi
