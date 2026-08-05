#!/bin/zsh
RES=results2.txt
run_one() {
  h=$1; shift
  echo "=== $h $(date +%H:%M:%S) load=$(sysctl -n vm.loadavg | awk '{print $2}')" | tee -a $RES
  ( timeout 450 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_arrayhdr.c --harness $h --exact "$@" > "out2_${h//:/_}.log" 2>&1 ) &
  PID=$!
  while kill -0 $PID 2>/dev/null; do sleep 15; done
  wait $PID
  grep -E "VERIFICATION:|Verification Time" "out2_${h//:/_}.log" | tail -2 | tee -a $RES
}
run_one proofs::eq_array_ndims --solver kissat
run_one proofs::eq_array_lower --solver kissat
run_one proofs::eq_array_length --solver kissat
run_one proofs::eq_array_upper --solver kissat
