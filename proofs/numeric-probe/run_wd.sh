#!/bin/zsh
# Watchdog runner (memory-pressure protocol): ONE kani at a time; poll the
# run's OWN descendant tree RSS every 15s; kill only that tree at >6GB and
# record wall(memory). Never kills by bare binary name.
cd "$(dirname "$0")"
h=$1; t=${2:-540}; solver=${3:-kissat}
if [ "$solver" = "default" ]; then sflag=(); else sflag=(--solver $solver); fi
log=/tmp/np_$h.log
timeout $t ~/.cargo/bin/cargo-kani kani -Z c-ffi -Z stubbing \
  --c-lib c/pg_numeric_cmp.c --c-lib c/pg_numeric_rows.c \
  $sflag --harness proofs::$h --exact > $log 2>&1 &
PID=$!
while kill -0 $PID 2>/dev/null; do
  out=$(python3 wd_tree.py $PID)
  rssk=${out%% *}
  if [ -n "$rssk" ] && [ "$rssk" -gt 6291456 ]; then
    for p in ${=out#* }; do kill $p 2>/dev/null; done
    echo "WALL(MEMORY): killed own run tree at rss=${rssk}KB" >> $log
    break
  fi
  sleep 15
done
wait $PID 2>/dev/null
grep -E "VERIFICATION:|Verification Time|WALL\(MEMORY\)|Failed Checks|of [0-9]+ cover|Complete - " $log | tail -8
