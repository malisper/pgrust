#!/bin/bash
# replay-rail.sh — the lane-0B fuzz regression rail: replay every committed
# corpus through its differential target (-runs=0 = corpus-only). Any
# divergence panics the target and fails the rail. Nightly-only (libFuzzer).
set -eu
cd "$(dirname "$0")"
for t in float_in_diff float_out_diff geo_diff float_math_diff float_math2_diff \
         uuid_diff mac_diff name_diff cash_diff; do
  [ -d "corpus/$t" ] || continue
  echo "== replay $t ($(ls corpus/$t | wc -l | tr -d ' ') inputs)"
  cargo +nightly fuzz run "$t" "corpus/$t" -- -runs=0
done
echo "replay rail GREEN"
