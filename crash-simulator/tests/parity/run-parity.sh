#!/usr/bin/env bash
# G-O2 classifier-parity gate (decision-logic leg) — WS-ORACLE.
#
# Regenerates screen+ladder fixtures from the PINNED triage.py
# (ef070d066 — the contract §1.3 vocabulary pin; the t26 tree carries an
# older triage.py without harness-fetch, so the pin is extracted via
# `git show`, never taken from the worktree) over the pinned repro corpora,
# diffs them against the checked-in fixtures (drift guard), then runs the
# Rust parity test against them.
#
# Re-run whenever src/vocab.rs or src/oracle/classifier.rs changes
# (contract §3.4 G-O2). The live-engine leg (same statements through two
# booted engines, Rust classifier vs triage.py end-to-end) rides
# WS-RUNNER's session driver on harness/h1-integration.
set -euo pipefail

TRIAGE_PIN=${TRIAGE_PIN:-ef070d066}
HERE=$(cd "$(dirname "$0")" && pwd)
CRATE=$(cd "$HERE/../.." && pwd)
REPO=$(cd "$CRATE/../.." && pwd)

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

git -C "$REPO" show "$TRIAGE_PIN:scripts/sqlsmith/triage.py" > "$TMP/triage_pinned.py"

CORPORA=(
  "$REPO/scripts/sqlsmith/repros-campaign1.sql"
  "$REPO/scripts/sqlsmith/repros-campaign3.sql"
  "$REPO/scripts/sqlsmith/repros-planner-families.sql"
  "$REPO/scripts/sqlsmith/repros-wrongresults39.sql"
  "$REPO/scripts/sqlsmith/replay-residuals.sql"
)

python3 "$HERE/gen_fixtures.py" \
  --triage "$TMP/triage_pinned.py" \
  --corpus "${CORPORA[@]}" \
  --matrix-limit 100000 \
  --out "$TMP/fixtures-screens.jsonl"

if ! diff -q "$TMP/fixtures-screens.jsonl" "$HERE/fixtures-screens.jsonl"; then
  echo "PARITY-GATE|FAIL|fixture drift vs checked-in fixtures-screens.jsonl" >&2
  echo "  (regenerate with tests/parity/run-parity.sh --refresh and re-review)" >&2
  if [[ "${1:-}" == "--refresh" ]]; then
    cp "$TMP/fixtures-screens.jsonl" "$HERE/fixtures-screens.jsonl"
    echo "PARITY-GATE|refreshed checked-in fixtures" >&2
  else
    exit 1
  fi
fi

cd "$CRATE"
PGRUST_FORCE_NO_RE2=1 cargo test --test simharness_oracle_parity -- --nocapture

echo "PARITY-GATE|PASS|triage=$TRIAGE_PIN"
