#!/bin/sh
# Run every standing equivalence harness individually (30s hard cap each),
# print per-harness verdict + wall time, then run the negative control,
# which MUST FAIL (non-vacuity check). Greens run under kissat (the measured
# recipe); the control runs under the DEFAULT solver — suite rule: controls
# validate by counterexample, and kissat does not terminate usefully on
# failing harnesses. From the crate directory:
#   sh run-all.sh
set -u
cd "$(dirname "$0")"
KANI="${HOME}/.cargo/bin/cargo-kani"
run() {
    # $1 = harness; remaining args = extra kani flags (solver, -Z stubbing).
    h=$1
    shift
    # Retry on external SIGTERM contamination ("CBMC failed with status 15"):
    # concurrent proof agents pkill stray cbmc processes between their own
    # harnesses, which can snipe an unrelated in-flight run. Not a verdict.
    for _try in 1 2 3 4 5; do
        t0=$(date +%s)
        out=$(timeout 30 "$KANI" kani -Z c-ffi --c-lib c/pg_bool.c "$@" --harness "$h" 2>&1)
        rc=$?
        t1=$(date +%s)
        if printf '%s\n' "$out" | grep -q 'CBMC failed with status 15'; then
            echo "$h sniped (external SIGTERM), retrying"
            sleep 2
            continue
        fi
        verdict=$(printf '%s\n' "$out" | grep 'VERIFICATION:' | tail -1)
        echo "$h rc=$rc wall=$((t1 - t0))s ${verdict:-NO-VERDICT (timeout/crash)}"
        return
    done
    echo "$h NO-VERDICT (sniped 5x)"
}
HARNESSES="
eq_boolout
eq_booleq eq_boolne eq_boollt eq_boolgt eq_boolle eq_boolge
eq_parse_bool_with_len_l0 eq_parse_bool_with_len_l1 eq_parse_bool_with_len_l2
eq_parse_bool_with_len_l3 eq_parse_bool_with_len_l4 eq_parse_bool_with_len_l5
eq_parse_bool_with_len_l6 cover_parse_bool_with_len_split
eq_bool_accum eq_bool_alltrue eq_bool_anytrue eq_int4_bool eq_bool_int4
"
for h in $HARNESSES; do run "$h" --solver kissat; done
# eq_bool_accum_inv stubs the Rust-side accumulator seam: needs -Z stubbing.
run eq_bool_accum_inv -Z stubbing --solver kissat
echo "--- negative control (expected: VERIFICATION:- FAILED; DEFAULT solver) ---"
run control_negative_boollt_vs_c_boolle
