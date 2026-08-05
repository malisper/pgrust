#!/bin/sh
# Run the brin-minmax proof suite, one harness per invocation (memory
# protocol: strictly serial; pair with the suite's 6 GiB RSS watchdog).
#
# Solver recipe (measured 2026-07-28; matches src/lib.rs header):
#   - kissat for the scalar (int4/date) harnesses and all consistent
#     harnesses: the DEFAULT solver rss-killed int4 add_value at 6.7-8.7 GiB
#     even with --slice-formula, while kissat stays at 3.1-4.0 GiB;
#   - DEFAULT solver for the uuid add_value/union harnesses (both solvers
#     green; default ~50-57s vs kissat ~81s) and for the MUST-FAIL control
#     (kissat never terminates on failing harnesses).
#
# Usage: ./run-all.sh [timeout-seconds]   (default 600 — several harnesses
# are release-gate tier, not per-commit tier; see src/lib.rs header)

T="${1:-600}"
KANI="${KANI:-$HOME/.cargo/bin/cargo-kani}"

GREEN="
proofs::eq_opcinfo
proofs::eq_int4_add_value
proofs::eq_date_add_value
proofs::eq_uuid_add_value
proofs::eq_uuid_add_value_nulls
proofs::eq_int4_consistent_lt
proofs::eq_int4_consistent_le
proofs::eq_int4_consistent_eq
proofs::eq_int4_consistent_ge
proofs::eq_int4_consistent_gt
proofs::eq_date_consistent_lt
proofs::eq_date_consistent_le
proofs::eq_date_consistent_eq
proofs::eq_date_consistent_ge
proofs::eq_date_consistent_gt
proofs::eq_uuid_consistent_lt
proofs::eq_uuid_consistent_le
proofs::eq_uuid_consistent_eq
proofs::eq_uuid_consistent_ge
proofs::eq_uuid_consistent_gt
proofs::eq_int4_union
proofs::eq_date_union
proofs::eq_uuid_union
"

solver_for() {
    case "$1" in
        proofs::eq_uuid_add_value|proofs::eq_uuid_add_value_nulls|proofs::eq_uuid_union)
            echo default ;;
        *)  echo kissat ;;
    esac
}

fail=0
for h in $GREEN; do
    solver=$(solver_for "$h")
    SARG=""
    [ "$solver" = "kissat" ] && SARG="--solver kissat"
    printf '== %s (%s) ==\n' "$h" "$solver"
    start=$(date +%s)
    if ! timeout "$T" "$KANI" kani -Z c-ffi -Z stubbing \
        --c-lib c/pg_brin_minmax.c --harness "$h" --exact $SARG 2>&1 |
        grep -E "VERIFICATION:|Verification Time"; then
        echo "FAILED-OR-TIMEOUT: $h"
        fail=1
    fi
    echo "wall: $(( $(date +%s) - start ))s"
done

echo "== proofs::control_seam_skew (MUST FAIL; default solver) =="
timeout "$T" "$KANI" kani -Z c-ffi -Z stubbing \
    --c-lib c/pg_brin_minmax.c --harness proofs::control_seam_skew --exact 2>&1 |
    grep -E "VERIFICATION:|Verification Time"

exit $fail
