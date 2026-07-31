#!/bin/sh
# Run every equivalence harness individually, print per-harness verdict +
# wall time. kissat for expected-green single-claim runs (solver law);
# the must-fail control runs separately with DEFAULT. From the crate dir:
#   sh run-all.sh [timeout-secs]
set -u
cd "$(dirname "$0")"
CAP="${1:-600}"
HARNESSES="
eq_seed
eq_seed_check
eq_next_u64
eq_next_i64
eq_next_nonnegative_i64
eq_next_u32
eq_next_i32
eq_next_nonnegative_i32
eq_next_bool
eq_nonnegative_signs
eq_u64_range_empty
eq_i64_range_empty
eq_u64_range_span1
eq_u64_range_span256
eq_u64_range_span2p32
eq_u64_range_full
eq_i64_range_pow2_cell
"
for h in $HARNESSES; do
    t0=$(date +%s)
    out=$(timeout "$CAP" "$HOME/.cargo/bin/cargo-kani" kani -Z c-ffi --c-lib c_pg_prng.c --harness "$h" --solver kissat 2>&1)
    rc=$?
    t1=$(date +%s)
    verdict=$(printf '%s\n' "$out" | grep 'VERIFICATION:' | tail -1)
    echo "$h rc=$rc wall=$((t1 - t0))s ${verdict:-NO-VERDICT (timeout/crash)}"
done
