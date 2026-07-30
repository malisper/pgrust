#!/bin/sh
# Run every equivalence harness individually with a 600s cap, print
# per-harness verdict + wall time. From the crate directory:
#   sh run-all.sh
set -u
cd "$(dirname "$0")"
HARNESSES="
eq_popcount32 eq_popcount64
eq_leftmost_one_pos32 eq_leftmost_one_pos64
eq_rightmost_one_pos32 eq_rightmost_one_pos64
eq_nextpower2_32 eq_nextpower2_64
eq_prevpower2_32 eq_prevpower2_64
eq_ceil_log2_32 eq_ceil_log2_64
eq_rotate_right32 eq_rotate_left32
eq_popcount_buf_small eq_popcount_masked_buf_small
eq_popcount_buf_full eq_popcount_masked_buf_full
"
for h in $HARNESSES; do
    t0=$(date +%s)
    out=$(timeout 600 "$HOME/.cargo/bin/cargo-kani" kani -Z c-ffi --c-lib c_bitutils.c --harness "$h" 2>&1)
    rc=$?
    t1=$(date +%s)
    verdict=$(printf '%s\n' "$out" | grep 'VERIFICATION:' | tail -1)
    echo "$h rc=$rc wall=$((t1 - t0))s ${verdict:-NO-VERDICT (timeout/crash)}"
done
