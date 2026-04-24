#!/bin/sh
# One-time per-clone setup for pgrust dev environment.
# Safe to re-run.

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Activate the versioned pre-commit hook in .githooks/.
# Each git worktree has its own config, so this needs to run once per worktree too.
git config --local core.hooksPath .githooks
echo "Set core.hooksPath -> .githooks (pre-commit hook now active for this clone)."
echo "  - Hook runs 'cargo fmt -- --check' and a content-hygiene check"
echo "    against scripts/internal/redactions.txt on every commit."
echo "  - Bypass once: git commit --no-verify"
echo "  - Opt out:     git config --unset core.hooksPath"
