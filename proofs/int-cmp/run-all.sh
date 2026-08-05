#!/bin/sh
# Batch-run every int-cmp harness, one at a time, 30s hard timeout each.
# Verdict + solver time per harness -> results.txt.
#
# Solver discipline (prove-target skill): kissat for expected-green harnesses;
# the negative control runs on the DEFAULT incremental solver (external kissat
# never terminates on failing properties).
#
# On timeout we kill only cbmc/kissat processes working on THIS family's
# target dir (never a bare `pkill -f cbmc` — that murders other agents' runs).

cd "$(dirname "$0")" || exit 1
RESULTS=results.txt
: > "$RESULTS"

CLIB="c/pg_intcmp.c"
run_one() { # $1 = harness (unqualified), $2 = extra solver args
    # shellcheck disable=SC2086
    out=$(timeout 30 cargo kani -Z c-ffi --c-lib "$CLIB" \
          --harness "proofs::$1" --exact $2 2>&1)
    rc=$?
    t=$(printf '%s\n' "$out" | sed -n 's/^Verification Time: //p' | tail -1)
    if [ $rc -eq 124 ]; then
        # timeout orphans cbmc children: kill only ours (matched on our path)
        pkill -f 'int-cmp/target' 2>/dev/null
        echo "TIMEOUT(30s)"
    elif printf '%s' "$out" | grep -q 'VERIFICATION:- SUCCESSFUL'; then
        echo "SUCCESSFUL ${t}"
    elif printf '%s' "$out" | grep -q 'VERIFICATION:- FAILED'; then
        echo "FAILED ${t}"
    else
        echo "ERROR(rc=$rc)"
    fi
}

# green-expected harnesses, extracted from src/lib.rs macro invocations
HARNESSES=$(grep -oE '^[[:space:]]+eq_[a-z0-9]+' src/lib.rs | tr -d ' ')

for h in $HARNESSES; do
    v=$(run_one "$h" "--solver kissat")
    printf '%-16s %s\n' "$h" "$v" | tee -a "$RESULTS"
done

# negative control: DEFAULT solver, FAILED == rig is non-vacuous
v=$(run_one neg_control_int4lt_is_not_le "")
case "$v" in
    FAILED*) printf '%-16s %s  (control: PASS - failed as designed)\n' \
                 neg_control_int4lt_is_not_le "$v" | tee -a "$RESULTS" ;;
    *)       printf '%-16s %s  (control: BROKEN GATE - expected FAILED)\n' \
                 neg_control_int4lt_is_not_le "$v" | tee -a "$RESULTS" ;;
esac
