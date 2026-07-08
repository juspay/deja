#!/usr/bin/env bash
# Resilient drop-in for Hyperswitch's stock migration_runner command.
#
# HS's stock migration_runner downloads the diesel CLI from GitHub *release
# assets* at runtime, with no retry. Those assets intermittently 504 (transient
# GitHub CDN, with sustained multi-minute windows), which fails the whole stack
# (the candidate services `depends_on: migration_runner` /
# `service_completed_successfully`). diesel itself is fine — only the GitHub
# download is unreliable. crates.io, by contrast, is reliable (it's where the
# router's own dependencies come from).
#
# Strategy, most-reliable first — same real diesel CLI, same `just migrate`:
#   1. Use a diesel binary mounted from the host at $DIESEL_MOUNT, built once via
#      `cargo install diesel_cli` from crates.io (see run-deja-demo.sh). Normal
#      path: no runtime download, no in-container compile, and glibc-compatible
#      (host glibc <= the trixie-slim runtime's, same as the host-built router).
#   2. Fallback: install from the GitHub release assets with retries — covers a
#      checkout that didn't pre-build the host binary.
set -u

DIESEL_VERSION="${DIESEL_VERSION:-v2.3.5}"
DIESEL_MOUNT="${DIESEL_MOUNT:-/opt/diesel-cli/diesel}"
ATTEMPTS="${INSTALL_ATTEMPTS:-8}"

apt-get update
apt-get install -y curl xz-utils

# 1) Preferred: host-built binary mounted in (a file, not the empty dir Docker
#    creates when the bind source is missing — hence the -x file test).
if [ -f "$DIESEL_MOUNT" ] && [ -x "$DIESEL_MOUNT" ]; then
  echo "deja: using host-built diesel CLI mounted at $DIESEL_MOUNT"
  apt-get install -y libpq5   # the postgres-feature binary links libpq.so.5
  install -m 0755 "$DIESEL_MOUNT" /usr/local/bin/diesel
fi

export PATH="${PATH}:${HOME}/.cargo/bin"

# 2) Fallback: GitHub release install, retried until diesel is actually on PATH.
if ! command -v diesel >/dev/null 2>&1; then
  echo "deja: no mounted diesel binary; falling back to GitHub release install"
  for i in $(seq 1 "$ATTEMPTS"); do
    command -v diesel >/dev/null 2>&1 && break
    echo "deja: installing diesel CLI ${DIESEL_VERSION} (attempt ${i}/${ATTEMPTS})…"
    curl --proto '=https' --tlsv1.2 -fLsS --retry 6 --retry-all-errors --retry-delay 3 \
      "https://github.com/diesel-rs/diesel/releases/download/${DIESEL_VERSION}/diesel_cli-installer.sh" | sh || true
    command -v diesel >/dev/null 2>&1 || sleep 5
  done
fi
command -v diesel >/dev/null 2>&1 || {
  echo "deja: diesel unavailable — no host mount and GitHub assets 504ing."
  echo "deja: pre-build it with: cargo install diesel_cli --no-default-features --features postgres --root demo/.diesel-cli"
  exit 1
}

# `just` is a thin wrapper; if its (also-GitHub) install fails, run diesel direct.
if ! command -v just >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -fLsS --retry 6 --retry-all-errors --retry-delay 3 \
    https://just.systems/install.sh | bash -s -- --to /usr/local/bin || true
fi

echo "deja: diesel=$(command -v diesel)"
if command -v just >/dev/null 2>&1; then
  exec just migrate
else
  echo "deja: 'just' unavailable; running 'diesel migration run' directly"
  exec diesel migration run \
    --database-url "$DATABASE_URL" \
    --migration-dir /app/migrations \
    --config-file /app/diesel.toml
fi
