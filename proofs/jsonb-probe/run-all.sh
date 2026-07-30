#!/bin/zsh
# jsonb-probe suite runner. Default solver (cadical) — measured: these
# builder harnesses carry many properties and external kissat re-solves per
# property batch (~70s each), while cadical one-passes them. Per-harness
# timeout with own-process-group kill only (never pkill by name).
cd "$(dirname "$0")"
OUT=/tmp/jsonb-probe-suite.tsv
: > $OUT
HARNESSES=(
  eq_typeof_array_n0 eq_typeof_array_n1 eq_typeof_array_n2
  eq_typeof_object_n0 eq_typeof_object_n1 eq_typeof_object_n2
  eq_typeof_scalar
  eq_arraylen_array_n0 eq_arraylen_array_n3 eq_arraylen_scalar
  eq_arraylen_object_n0 eq_arraylen_object_n2
  eq_object_field_n0 eq_object_field_n1 eq_object_field_n2_eqlen
  eq_object_field_n2_mixlen eq_object_field_n3
  eq_array_element_n0 eq_array_element_n1 eq_array_element_n2
  eq_array_element_n3 eq_array_element_raw
  eq_exists_object_n0 eq_exists_object_n1 eq_exists_object_n2_eqlen
  eq_exists_object_n2_mixlen eq_exists_object_n3
  eq_exists_array_n0 eq_exists_array_n1 eq_exists_array_n2
  eq_exists_array_n3 eq_exists_array_raw
  eq_cmp_bool_0_0 eq_cmp_bool_1_1 eq_cmp_bool_2_2 eq_cmp_bool_1_2
  eq_cmp_bool_raw_vs_arr eq_cmp_bool_raw_raw
  eq_cmp_string_1_1 eq_cmp_string_2_2
  eq_cmp_obj_obj_0_0 eq_cmp_obj_obj_1_1 eq_cmp_obj_obj_2_2
  eq_cmp_obj_obj_1_2 eq_cmp_obj_arr_1_1
)
# Round-2 cmp greens run through run-cmp.sh (exact unwindsets; see its
# header): EXTRA="--solver kissat --no-assertion-reach-checks
# --no-unwinding-checks" CBMCX="--slice-formula" and per-cell bounds:
#   eq_cmp_bool_0_0 3 / eq_cmp_bool_1_2 3 / eq_cmp_bool_raw_vs_arr 2 /
#   eq_cmp_bool_raw_vs_empty 2 / eq_cmp_obj_obj_0_0 3 /
#   eq_cmp_obj_obj_1_2 2 (OFFB=5) / eq_cmp_obj_arr_1_1 2 /
#   eq_cmp_nested_1i1_1i2 3 nested
# control_cmp_swapped 3 (DEFAULT solver, MUST FAIL).
# Equal-shape full-walk cells wall(memory>6GiB) — see src/lib.rs ROUND 2.
for h in $HARNESSES; do
  log=/tmp/jp-suite-$h.log
  start=$(date +%s)
  ( timeout 240 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_jsonb.c --harness "$h" > $log 2>&1 ) &
  PID=$!
  wait $PID
  rc=$?
  end=$(date +%s)
  wall=$((end - start))
  verdict=$(grep -m1 "VERIFICATION:-" $log | sed 's/.*VERIFICATION:- //')
  vtime=$(grep -m1 "Verification Time:" $log | sed 's/.*Verification Time: //')
  [ -z "$verdict" ] && verdict="TIMEOUT(rc=$rc)"
  echo "$h\t$verdict\t$vtime\twall=${wall}s" >> $OUT
done
echo DONE >> $OUT
