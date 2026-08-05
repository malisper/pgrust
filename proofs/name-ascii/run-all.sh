#!/bin/sh
# Standing-gate runner for the name/ascii family proofs.
# Positive harnesses must be VERIFICATION:- SUCCESSFUL.
# The two control_* harnesses must be VERIFICATION:- FAILED (negative
# controls: they prove the rig can detect a divergence; a PASS there is a
# broken gate).
#
# NOTE (measured 2026-07-28): "CBMC failed with status 15" is a killed
# solver (e.g. a concurrent agent's `pkill -f cbmc` hygiene loop), NOT a
# verdict — rerun the harness.
set -u
cd "$(dirname "$0")"

POSITIVE="eq_btnamecmp_full64 eq_btnamecmp_cap16 eq_nameeq_full64 eq_namene_full64 \
eq_namelt_full64 eq_namele_full64 eq_namegt_full64 eq_namege_full64 \
eq_namein_sym_len_le_71 eq_text_name_sym_len_le_71 eq_name_text_payload \
eq_nameout_terminated \
eq_to_ascii_latin1 eq_to_ascii_latin2 eq_to_ascii_latin9 eq_to_ascii_win1250 \
cover_to_ascii_enc_split"
NEGATIVE="control_name_mismatch_must_fail control_ascii_enc_mismatch_must_fail"

fail=0
for h in $POSITIVE; do
    out=$(timeout 60 cargo kani -Z c-ffi --c-lib c/pg_name_ascii.c --solver kissat --harness "$h" 2>&1)
    if printf '%s' "$out" | grep -q "VERIFICATION:- SUCCESSFUL"; then
        echo "PASS  $h"
    else
        echo "FAIL  $h"; fail=1
    fi
done
# Controls run with the DEFAULT solver (suite rule: controls validate by
# counterexample; kissat does not terminate usefully on failing harnesses).
for h in $NEGATIVE; do
    out=$(timeout 60 cargo kani -Z c-ffi --c-lib c/pg_name_ascii.c --harness "$h" 2>&1)
    if printf '%s' "$out" | grep -q "VERIFICATION:- FAILED" && ! printf '%s' "$out" | grep -q "status 15"; then
        echo "PASS  $h (failed as required: rig non-vacuous)"
    else
        echo "FAIL  $h (negative control did not fail — broken gate)"; fail=1
    fi
done
exit $fail
