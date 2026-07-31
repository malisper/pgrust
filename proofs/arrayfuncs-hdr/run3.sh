#!/bin/zsh
# Re-run driver for the corruption-plane WIDENING (fix/array-hdr-reconcile).
# New *_ndim_corrupt / *_nonpos / over_maxdim harnesses first, then the whole
# pre-existing family (shipped read_dims_lbounds changed under them).
RES=results3.txt
run_one() {
  h=$1; shift
  echo "=== $h start=$(date +%H:%M:%S) load=$(sysctl -n vm.loadavg | awk '{print $2}')" | tee -a $RES
  ( timeout 600 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_arrayhdr.c --harness $h --exact "$@" > "out4_${h//:/_}.log" 2>&1 )
  rc=$?
  echo "rc=$rc" | tee -a $RES
  grep -E "VERIFICATION:|Verification Time" "out4_${h//:/_}.log" | tail -2 | tee -a $RES
}
for h in "$@"; do
  case $h in
    *must_fail*) run_one $h ;;
    *) run_one $h --solver kissat ;;
  esac
done
