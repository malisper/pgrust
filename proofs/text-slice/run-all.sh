#!/bin/bash
# proofs/text-slice standing suite.
#
# Green harnesses run with kissat (expected-UNSAT); the two negative
# controls run with the DEFAULT solver (kissat never terminates on failing
# harnesses) and MUST report VERIFICATION FAILED.
#
# Solve-time tiers (measured under multi-agent load 2026-07-28, inflated
# ~2-3x per TRIAGE calibration): the *_latin1 / bytea / octetlen harnesses
# are per-commit tier; the *_utf8 mb-walk harnesses are release-gate tier.
set -u
cd "$(dirname "$0")"

run() { # run <timeout> [solver args] <harness>
    local t="$1"; shift
    local h="${@: -1}"
    timeout "$t" cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_text_slice.c \
        --exact --harness "proofs::$h" "${@:1:$#-1}"
}

# per-commit tier
for h in eq_textoctetlen eq_byteaoctetlen eq_textlen_latin1 eq_byteapos \
         eq_byteacat eq_textcat eq_bytea_substr eq_bytea_substr_no_len \
         eq_textpos_latin1 eq_text_substr_latin1 eq_text_substr_no_len_latin1 \
         eq_text_starts_with_latin1 eq_text_left_latin1 eq_text_right_latin1; do
    run 350 --solver kissat "$h" || echo "FAILED: $h"
done

# release-gate tier (mb char walks; slow-class)
for h in eq_textlen_utf8 eq_textpos_utf8 eq_text_substr_utf8 \
         eq_text_substr_no_len_utf8 eq_text_starts_with_utf8 \
         eq_text_left_utf8 eq_text_right_utf8 cover_text_substr_regimes \
         cover_textpos_regimes; do
    run 600 --solver kissat "$h" || echo "FAILED: $h"
done

# negative controls: MUST FAIL (default solver)
for h in control_byteapos_short_needle control_starts_with_unfenced; do
    if run 350 "$h"; then
        echo "BROKEN GATE: negative control $h PASSED"
    else
        echo "control $h failed as expected"
    fi
done
