#!/bin/sh
# Run every STANDING intout equivalence harness individually with a 30s
# cap (prove-target hard budget), print per-harness verdict + wall time.
# From the crate directory:  sh run-all.sh
#
# Deliberately NOT run here:
#   wall_probe_ultoa_n_u32_d8a / d8b  -- measured WALL / boundary-flaky
#     (kept in src/lib.rs for re-measurement only)
#   control_intout_mismatch          -- negative control: run it manually;
#     it MUST report VERIFICATION FAILED (x=99: C "99" vs Rust "100").
#     If it ever passes, the rig is vacuous -- fix before trusting greens.
set -u
cd "$(dirname "$0")"
HARNESSES="
eq_itoa_i16_small eq_itoa_i16_big cover_itoa_i16_split
eq_ultoa_n_u32_r1_lt1e4 eq_ultoa_n_u32_d5 eq_ultoa_n_u32_d6
eq_ultoa_n_u32_d7a eq_ultoa_n_u32_d7b cover_ultoa_n_u32_split
eq_ultoa_n_u32_spots
eq_ltoa_i32_abs_lt1e6 eq_ltoa_i32_spots
eq_ulltoa_n_u64_r1_lt1e4 eq_ulltoa_n_u64_d5 eq_ulltoa_n_u64_d6
eq_ulltoa_n_u64_d7a eq_ulltoa_n_u64_d7b cover_ulltoa_n_u64_split
eq_ulltoa_n_u64_spots
eq_lltoa_i64_neg_1e6 eq_lltoa_i64_pos_1e6 cover_lltoa_i64_split
eq_lltoa_i64_spots
"
for h in $HARNESSES; do
    t0=$(date +%s)
    out=$(timeout 30 "$HOME/.cargo/bin/cargo-kani" kani -Z c-ffi --c-lib c/pg_intout.c --solver kissat --harness "$h" 2>&1)
    rc=$?
    t1=$(date +%s)
    verdict=$(printf '%s\n' "$out" | grep 'VERIFICATION:' | tail -1)
    echo "$h rc=$rc wall=$((t1 - t0))s ${verdict:-NO-VERDICT (timeout/crash)}"
    pkill -f '[c]bmc' 2>/dev/null
    pkill -f '[k]issat' 2>/dev/null
done
