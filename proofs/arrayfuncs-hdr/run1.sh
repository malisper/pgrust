#!/bin/zsh
RES=results.txt
run_one() {
  h=$1; shift
  extra=("$@")
  echo "=== $h $(date +%H:%M:%S) load=$(sysctl -n vm.loadavg | awk '{print $2}')" | tee -a $RES
  ( timeout 450 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_arrayhdr.c --harness $h --exact "${extra[@]}" > out_$h.log 2>&1 ) &
  PID=$!
  while kill -0 $PID 2>/dev/null; do
    sleep 15
    for cp in $(pgrep -f "cbmc.*arrayfuncs-hdr" 2>/dev/null); do
      rss=$(ps -o rss= -p $cp 2>/dev/null | tr -d ' ')
      if [ -n "$rss" ] && [ "$rss" -gt 6291456 ]; then
        echo "RSS>6GB kill $h" | tee -a $RES; kill -- -$PID 2>/dev/null
      fi
    done
  done
  wait $PID
  grep -E "VERIFICATION|Verification Time" out_$h.log | tail -3 | tee -a $RES
}
run_one proofs::eq_array_ndims --solver kissat
run_one proofs::eq_array_lower --solver kissat
run_one proofs::eq_array_upper --solver kissat
run_one proofs::eq_array_length --solver kissat
