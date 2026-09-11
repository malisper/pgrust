#!/usr/bin/env bash
# lint-walker-arms.sh — CI rail for the "missing case arm" bug class
# (tools/walker-arms/walker_arms.py; PR #2090, PR1604-1, walsender's
# dropped guard: a C `switch (nodeTag(...))` / enum switch / IsA chain has an
# arm the Rust port's `match` lacks, hidden by `_ =>`).
#
# Diffs every paired C/Rust dispatch function's handled-label sets and fails
# on a gap not adjudicated in crates/_support/seams_init/tests/lint-walker-arms.allow
# (override: LINT_WALKER_ARMS_ALLOWLIST). Rows whose gap has closed print a
# stale NOTE. The P-NODEWALKER unit rail (nodes_core expr_tags_test.rs) covers
# the nodes_core accessors by construction; this generalises the check to every
# ported walker/mutator/switch mechanically.
#
# Standalone:   crates/_support/seams_init/tests/lint-walker-arms.sh   (exit 0 = clean)
# Unit-shaped:  cargo test -p seams_init --test lint_walker_arms
# Full report:  python3 tools/walker-arms/walker_arms.py audit --tsv out.tsv --md out.md
set -u

REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
TOOL="$REPO/tools/walker-arms/walker_arms.py"
ALLOWLIST="${LINT_WALKER_ARMS_ALLOWLIST:-$REPO/crates/_support/seams_init/tests/lint-walker-arms.allow}"
[ -f "$TOOL" ] || { echo "lint-walker-arms: $TOOL not found"; exit 2; }
[ -d "$REPO/crates/postgres-18.6-reference/src/backend" ] || {
    echo "lint-walker-arms: reference tree missing (sparse checkout?) — skipping"; exit 0; }

exec python3 "$TOOL" audit --check --allow "$ALLOWLIST"
