#!/usr/bin/env bash
# Create one replay sandbox (Helm release in its own namespace) for a run.
#
# The dashboard triggers this per run. All environment-level settings —
# especially the recording-bucket S3 keys — come from the dashboard config
# TOML (config/dashboard/development.toml by default, override with
# DEJA_DASHBOARD_CONFIG). DEJA_S3__* env vars override the file's [s3] table,
# mirroring deja_replay_core::config::load_dashboard_config.
#
# Usage:
#   scripts/sandbox-create.sh RUN_ID RECORDING_ID \
#     [--branch BRANCH] [--image REPO:TAG] [--build-ref REF] [--set k=v ...]
set -euo pipefail

usage() {
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
}

[ $# -ge 2 ] || usage
run_id="$1"
recording_id="$2"
shift 2

branch=""
image=""
build_ref=""
extra_sets=()
while [ $# -gt 0 ]; do
  case "$1" in
    --branch) branch="$2"; shift 2 ;;
    --image) image="$2"; shift 2 ;;
    --build-ref) build_ref="$2"; shift 2 ;;
    --set) extra_sets+=("--set" "$2"); shift 2 ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
config_file="${DEJA_DASHBOARD_CONFIG:-$repo_root/config/dashboard/development.toml}"
[ -f "$config_file" ] || { echo "dashboard config not found: $config_file" >&2; exit 2; }

for cmd in helm kubectl python3; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "missing required command: $cmd" >&2; exit 127; }
done

# Read the dashboard config; DEJA_S3__* env overrides the [s3] table.
cfg_json="$(python3 - "$config_file" <<'PY'
import json, os, sys, tomllib

cfg = tomllib.load(open(sys.argv[1], "rb"))
s3 = cfg.get("s3", {})
for key in ("region", "access_key", "secret_key", "bucket", "prefix", "endpoint"):
    env = os.environ.get(f"DEJA_S3__{key.upper()}")
    if env is not None:
        s3[key] = env
cfg["s3"] = s3
sandbox = cfg.get("sandbox")
if sandbox is None:
    sys.exit("dashboard config has no [sandbox] table; sandbox driver disabled")
print(json.dumps(cfg))
PY
)"

jq_get() { printf '%s' "$cfg_json" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d$1)"; }

chart="$(jq_get "['sandbox']['chart']")"
ns_prefix="$(jq_get "['sandbox'].get('namespace_prefix','deja-run-')")"
callback_base_url="$(jq_get "['sandbox'].get('callback_base_url','')")"
callback_token="$(jq_get "['sandbox'].get('callback_token','')")"
s3_region="$(jq_get "['s3'].get('region','us-east-1')")"
s3_access_key="$(jq_get "['s3'].get('access_key','')")"
s3_secret_key="$(jq_get "['s3'].get('secret_key','')")"
s3_bucket="$(jq_get "['s3'].get('bucket','')")"
s3_prefix="$(jq_get "['s3'].get('prefix','')")"
s3_endpoint="$(jq_get "['s3'].get('endpoint','')")"

case "$chart" in /*) ;; *) chart="$repo_root/$chart" ;; esac
[ -f "$chart/Chart.yaml" ] || { echo "sandbox chart not found at $chart" >&2; exit 2; }

# Namespace / release name from the run id (RFC 1123 label).
suffix="$(printf '%s' "$run_id" | tr '[:upper:]' '[:lower:]' | tr -c '[:alnum:]-' '-' | sed 's/^-*//; s/-*$//')"
suffix="${suffix:-run}"
namespace="${ns_prefix}${suffix:0:48}"
release="replay"

state_dir="${DEJA_SANDBOX_STATE_DIR:-${HARNESS_STATE:-/tmp}/sandboxes/$run_id}"
mkdir -p "$state_dir"
values_file="$state_dir/values.yaml"

# Secrets go through a 0600 values file, not --set (visible in process args).
umask 077
cat > "$values_file" <<YAML
run:
  id: "$run_id"
  recordingId: "$recording_id"

s3:
  region: "$s3_region"
  bucket: "$s3_bucket"
  prefix: "$s3_prefix"
  endpoint: "$s3_endpoint"
  accessKey: "$s3_access_key"
  secretKey: "$s3_secret_key"

callback:
  baseUrl: "$callback_base_url"
  token: "$callback_token"

candidate:
  branch: "$branch"
  buildRef: "$build_ref"
YAML

if [ -n "$image" ]; then
  cat >> "$values_file" <<YAML
  image:
    repository: "${image%:*}"
    tag: "${image##*:}"
YAML
fi

echo "creating sandbox: namespace=$namespace chart=$chart" >&2
helm upgrade --install "$release" "$chart" \
  --namespace "$namespace" --create-namespace \
  -f "$values_file" \
  ${extra_sets+"${extra_sets[@]}"} \
  --wait --timeout 10m >&2

echo "$namespace"
