#!/bin/sh
# Standing gate: all pg_lsn equivalence harnesses must be green and the
# negative control must FAIL (non-vacuity). Solve times measured 2026-07-28:
# cmp/ops <0.1s; out/drift ~2.5s; in_reject ~16s; coverage ~16s;
# in_accept_* 8-15s each. -Z stubbing is required (std from_utf8 /
# from_str_radix stubs in the pg_lsn_in harnesses; see src/lib.rs).
set -u
cd "$(dirname "$0")"
run() { timeout 60 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_pg_lsn.c --solver kissat --harness "$1"; }
fail=0
H="eq_pg_lsn_cmp eq_pg_lsn_eq eq_pg_lsn_ne eq_pg_lsn_lt eq_pg_lsn_gt eq_pg_lsn_le eq_pg_lsn_ge eq_pg_lsn_larger eq_pg_lsn_smaller eq_pg_lsn_out drift_pg_lsn_out_master_format eq_pg_lsn_in_reject cover_pg_lsn_in_partition"
for a in 1 2 3 4 5 6 7 8; do for b in 1 2 3 4 5 6 7 8; do H="$H eq_pg_lsn_in_accept_${a}_${b}"; done; done
for h in $H; do
  if ! run "$h" > /tmp/pg_lsn_gate_$h.log 2>&1; then echo "RED: $h"; fail=1; else echo "green: $h"; fi
done
# Negative control: must FAIL by COUNTEREXAMPLE. Runs with the DEFAULT
# solver (suite rule: kissat never terminates on failing harnesses, so a
# kissat timeout would be a vacuous "pass" of this gate). We require the
# explicit "VERIFICATION:- FAILED" verdict — a timeout or crash is RED.
timeout 60 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_pg_lsn.c \
  --harness control_pg_lsn_out_mismatch > /tmp/pg_lsn_gate_control.log 2>&1
if grep -q 'VERIFICATION:- FAILED' /tmp/pg_lsn_gate_control.log; then
  echo "green: control_pg_lsn_out_mismatch refuted by counterexample as required"
else
  echo "RED: control_pg_lsn_out_mismatch did not produce a counterexample (passed, timed out, or crashed) — rig is vacuous or wedged"; fail=1
fi
exit $fail
