#!/bin/sh
# Standing-gate runner for the text/bpchar/name-cross comparator proofs.
# Positive harnesses must be VERIFICATION:- SUCCESSFUL (kissat).
# The two control_* harnesses must be VERIFICATION:- FAILED (negative
# controls, DEFAULT solver: they prove the rig can detect a divergence; a
# PASS there is a broken gate).
#
# NOTE: "CBMC failed with status 15" is a killed solver (e.g. a concurrent
# agent's cbmc hygiene loop), NOT a verdict — rerun the harness.
set -u
cd "$(dirname "$0")"

POSITIVE="eq_texteq eq_textne eq_text_lt eq_text_le eq_text_gt eq_text_ge \
eq_bttextcmp eq_text_larger eq_text_smaller \
eq_text_pattern_lt eq_text_pattern_le eq_text_pattern_ge eq_text_pattern_gt \
eq_bttext_pattern_cmp \
eq_bpchareq eq_bpcharne eq_bpcharlt eq_bpcharle eq_bpchargt eq_bpcharge \
eq_bpcharcmp eq_bpchar_larger eq_bpchar_smaller \
eq_bpchar_pattern_lt eq_bpchar_pattern_le eq_bpchar_pattern_ge \
eq_bpchar_pattern_gt eq_btbpchar_pattern_cmp \
eq_nameeqtext eq_namenetext eq_namelttext eq_nameletext eq_namegttext \
eq_namegetext eq_btnametextcmp \
eq_texteqname eq_textnename eq_textltname eq_textlename eq_textgtname \
eq_textgename eq_bttextnamecmp"
NEGATIVE="control_bttextcmp_short_c_len control_bpchareq_untrimmed_c"

fail=0
for h in $POSITIVE; do
    out=$(timeout 90 cargo kani -Z c-ffi --c-lib c/pg_text_cmp.c --solver kissat --harness "$h" 2>&1)
    t=$(printf '%s' "$out" | grep "Verification Time" | tail -1)
    if printf '%s' "$out" | grep -q "VERIFICATION:- SUCCESSFUL"; then
        echo "PASS  $h  ($t)"
    else
        echo "FAIL  $h"; fail=1
    fi
done
for h in $NEGATIVE; do
    out=$(timeout 90 cargo kani -Z c-ffi --c-lib c/pg_text_cmp.c --harness "$h" 2>&1)
    if printf '%s' "$out" | grep -q "VERIFICATION:- FAILED"; then
        echo "PASS  $h (failed as required)"
    else
        echo "BROKEN-GATE  $h (negative control did not fail)"; fail=1
    fi
done
exit $fail
