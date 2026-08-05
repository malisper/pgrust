#!/bin/sh
# Run every equivalence harness individually, print per-harness verdict +
# wall time. kissat for expected-green single-claim runs (solver law);
# the must-fail control runs separately with DEFAULT. From the crate dir:
#   sh run-all.sh [timeout-secs]
set -u
cd "$(dirname "$0")"
CAP="${1:-600}"
HARNESSES="
eq_array_get_n_items
ndim_wider_than_slice_always_errors
eq_array_check_bounds
eq_array_get_offset
eq_mda_get_range
eq_mda_get_prod
eq_mda_get_offset_values
eq_mda_next_tuple
"
for h in $HARNESSES; do
    t0=$(date +%s)
    out=$(timeout "$CAP" "$HOME/.cargo/bin/cargo-kani" kani -Z c-ffi --c-lib c_arrayutils.c --harness "$h" --solver kissat 2>&1)
    rc=$?
    t1=$(date +%s)
    verdict=$(printf '%s\n' "$out" | grep 'VERIFICATION:' | tail -1)
    echo "$h rc=$rc wall=$((t1 - t0))s ${verdict:-NO-VERDICT (timeout/crash)}"
done
