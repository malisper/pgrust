#!/bin/sh
# Run every standing strfam equivalence harness individually (120s hard cap
# each), print per-harness verdict + wall time, then run the negative
# control, which MUST FAIL (non-vacuity check). Greens run under kissat
# (the measured recipe: all <=6s at low load, 2026-07-30); the control runs
# under the DEFAULT solver — suite rule: controls validate by counterexample,
# and kissat does not terminate usefully on failing harnesses.
#   sh run-all.sh
set -u
cd "$(dirname "$0")"
KANI="${HOME}/.cargo/bin/cargo-kani"
run() {
    h=$1
    shift
    for _try in 1 2 3 4 5; do
        t0=$(date +%s)
        out=$(timeout 120 "$KANI" kani -Z c-ffi --c-lib c/pg_strfam_kani.c "$@" --harness "$h" 2>&1)
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
eq_wifexited eq_wexitstatus eq_wifsignaled eq_wtermsig
eq_wait_result_is_signal eq_wait_result_is_any_signal
eq_wait_result_to_exit_code
eq_forkname_chars_main eq_forkname_chars_fsm eq_forkname_chars_vm
eq_forkname_chars_init cover_forkname_chars_split
eq_isspace_c_locale
"
for h in $HARNESSES; do
    run "$h" --solver kissat
done
echo "--- negative control (must FAIL on the intended assert) ---"
run control_negative_is_signal_drops_shell_arm
