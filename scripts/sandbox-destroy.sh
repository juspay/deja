#!/usr/bin/env bash
# Tear down a replay sandbox created by scripts/sandbox-create.sh.
#
# Usage: scripts/sandbox-destroy.sh RUN_ID
set -euo pipefail

[ $# -eq 1 ] || { echo "usage: $0 RUN_ID" >&2; exit 2; }
run_id="$1"

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
config_file="${DEJA_DASHBOARD_CONFIG:-$repo_root/config/dashboard/development.toml}"

ns_prefix="deja-run-"
if [ -f "$config_file" ]; then
  ns_prefix="$(python3 -c "
import sys, tomllib
cfg = tomllib.load(open('$config_file', 'rb'))
print(cfg.get('sandbox', {}).get('namespace_prefix', 'deja-run-'))
")"
fi

suffix="$(printf '%s' "$run_id" | tr '[:upper:]' '[:lower:]' | tr -c '[:alnum:]-' '-' | sed 's/^-*//; s/-*$//')"
suffix="${suffix:-run}"
namespace="${ns_prefix}${suffix:0:48}"

helm uninstall replay --namespace "$namespace" --ignore-not-found >&2 || true
kubectl delete namespace "$namespace" --ignore-not-found >&2
rm -rf "${DEJA_SANDBOX_STATE_DIR:-${HARNESS_STATE:-/tmp}/sandboxes/$run_id}"
echo "destroyed $namespace"
