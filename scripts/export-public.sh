#!/usr/bin/env bash
# Fresh-history export for publishing Déjà.
#
# WHY THIS EXISTS — the development clone's git history contains material that
# must never be published, and `git rm --cached` cannot remove history:
#   * external test credentials inside historical recording blobs and a deleted
#     demo/.env  (ROTATE THEM BEFORE PUBLISHING, even with this export);
#   * ~600 MB of accidental payload (cargo target/, benchmark artifacts,
#     harness-state recordings, two full vendored Hyperswitch snapshots);
#   * internal branch/session names and "saving work" commit messages.
# A fresh single-commit export sidesteps all of it: the published repo starts
# from the cleaned tree only (~a few MB), with none of the old blobs reachable.
#
#   Usage: scripts/export-public.sh <destination-dir> [public-repo-url]
#
# The export contains the CURRENT COMMITTED TREE of this branch only. A dirty or
# untracked root worktree blocks export, because otherwise freshly validated local
# changes would be silently omitted. Ignored state such as demo/.env,
# harness-state, and banks is never copied. The nested vendor/hyperswitch-deja-clean
# repo is NOT exported here:
# publish its `deja-integration` branch separately (it is a fork of upstream
# Hyperswitch; push ONLY that branch, never deja-lean).
set -euo pipefail

DEST="${1:?usage: scripts/export-public.sh <destination-dir> [public-repo-url]}"
URL="${2:-}"

cd "$(dirname "$0")/.."
[ -e "$DEST" ] && { echo "ERROR: $DEST already exists"; exit 1; }

# The command below exports HEAD, not the working tree. Refuse dirty state so a
# release cannot silently omit freshly validated local changes.
if [ -n "$(git status --porcelain --untracked-files=normal -- .)" ]; then
  echo "ERROR: working tree has uncommitted or untracked files; commit or remove them before export."
  git status --short --untracked-files=normal -- .
  exit 1
fi


# Pre-flight: refuse to export if a secret-looking token is present in any
# TRACKED file (the tree being exported).
if git grep -lE '(sk|rk)_(test|live)_[A-Za-z0-9]{20,}|whsec_[A-Za-z0-9]{20,}' -- . >/dev/null 2>&1; then
  echo "ERROR: tracked files contain a Stripe-style secret token:"
  git grep -lE '(sk|rk)_(test|live)_[A-Za-z0-9]{20,}|whsec_[A-Za-z0-9]{20,}' -- .
  exit 1
fi

mkdir -p "$DEST"
# Tracked files only — exactly the committed tree, no untracked/ignored state.
git archive HEAD | tar -x -C "$DEST"

cd "$DEST"
git init -q -b main
git add -A
git commit -q -m "Déjà: deterministic record/replay for service boundaries

Initial public release. Developed in a private repository; history starts
here."
echo "── exported $(git ls-files | wc -l) files to $DEST (single commit) ──"

if [ -n "$URL" ]; then
  git remote add origin "$URL"
  echo "── remote 'origin' set to $URL — review, then: git push -u origin main ──"
else
  echo "── no remote set; review the tree, then add one and push ──"
fi

echo
echo "Post-export checklist:"
echo "  1. External test credentials used for demos are rotated."
echo "  2. Spot-check: git -C $DEST grep -E '(sk|rk)_(test|live)_[A-Za-z0-9]{20,}|whsec_[A-Za-z0-9]{20,}|/home/[A-Za-z0-9._-]+|/Users/[A-Za-z0-9._-]+' returns nothing."
echo "  3. Fill [workspace.package] repository = in Cargo.toml with the public URL."
echo "  4. Vendor PR: push vendor/hyperswitch-deja-clean's deja-integration branch"
echo "     to your Hyperswitch fork (NOT deja-lean; NOT --all)."
