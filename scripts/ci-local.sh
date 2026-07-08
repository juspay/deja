#!/usr/bin/env bash
# Local replica of the upstream Hyperswitch PR gates that decide merge-readiness
# of the deja integration, plus the deja battery. Run against the vendor tree
# after EVERY improvement (the merge-readiness program's per-change gate).
#
#   scripts/ci-local.sh [--vendor <path>] [--fast]
#
# --fast skips the two long cargo checks (release-features and v2) — use for
# inner-loop iterations; the full run is required before any fold/commit.
# Migration-consistency (just migrate --locked-schema) needs a running pg, so
# it runs only when DATABASE_URL is set; otherwise it is reported as SKIPPED.
set -uo pipefail
cd "$(dirname "$0")/.."

VENDOR="vendor/hyperswitch-deja-clean"
FAST=0
while [ $# -gt 0 ]; do case "$1" in
  --vendor) VENDOR="$2"; shift 2 ;;
  --fast) FAST=1; shift ;;
  *) echo "unknown arg: $1"; exit 2 ;;
esac; done

declare -a RESULTS
FAIL=0
gate() { # gate <name> <cmd...>
  local name="$1"; shift
  echo "──[ $name ]──"
  if "$@" >/tmp/ci-local-gate.log 2>&1; then
    RESULTS+=("PASS  $name")
  else
    RESULTS+=("FAIL  $name")
    FAIL=1
    tail -20 /tmp/ci-local-gate.log | sed 's/^/    /'
  fi
}

pushd "$VENDOR" >/dev/null

# ── upstream CI-pr replicas ──────────────────────────────────────────────────
gate "fmt (nightly, upstream job)"        cargo +nightly fmt --all --check
gate "clippy v1 (just clippy)"            just clippy
gate "clippy v2 (just clippy_v2)"         just clippy_v2
if [ "$FAST" -eq 0 ]; then
  gate "check release features"           cargo check --features release
  gate "check v2 no-default"              cargo check --no-default-features --features release,v2,redis-rs
fi
if [ -n "${DATABASE_URL:-}" ]; then
  gate "migrations locked-schema (v1)"    just migrate run --locked-schema
  gate "migrations locked-schema (v2)"    just migrate_v2 run --locked-schema
else
  RESULTS+=("SKIP  migrations (set DATABASE_URL to enable)")
fi

# ── deja battery ─────────────────────────────────────────────────────────────
gate "deja check (deja,v1)"               cargo check -p router --features deja,v1
gate "kafka envelope test"                cargo test -p router --features deja,v1 --lib envelope_serializes_artifact_record_v2_shape
gate "kafka marker test"                  cargo test -p router --features deja,v1 --lib marker_envelope_serializes_sink_marker_shape
gate "router_env deja tests"              cargo test -p router_env --features deja

# default-build purity: zero deja crates in the default dependency graph
echo "──[ default-graph purity ]──"
if [ "$(cargo tree -p router -e normal 2>/dev/null | grep -cE 'deja v[0-9]|deja-(runtime|core|context|derive) v')" = "0" ]; then
  RESULTS+=("PASS  default-graph purity (0 deja crates)")
else
  RESULTS+=("FAIL  default-graph purity"); FAIL=1
fi

popd >/dev/null

# ── root workspace gate (when the deja library itself changed) ───────────────
gate "root just verify"                   just verify

echo; echo "════════ ci-local summary ════════"
printf '  %s\n' "${RESULTS[@]}"
echo "═════════════════════════════════"
exit $FAIL
