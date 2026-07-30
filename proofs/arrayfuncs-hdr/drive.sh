#!/bin/zsh
# process queue.txt entries not yet in done.txt; stop before exceeding $1 seconds
BUDGET=${1:-540}
START=$SECONDS
touch done.txt
while read -r h solver; do
  grep -q "^$h " done.txt 2>/dev/null && continue
  [ $((SECONDS-START)) -gt $((BUDGET-320)) ] && { echo "BUDGET-STOP"; exit 0; }
  args=(--solver kissat); [ "$solver" = default ] && args=()
  echo "=== $h $(date +%H:%M:%S) load=$(sysctl -n vm.loadavg | awk '{print $2}')"
  timeout 450 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_arrayhdr.c --harness $h --exact "${args[@]}" > "out3_${h##*::}.log" 2>&1
  v=$(grep -E "^VERIFICATION:" "out3_${h##*::}.log" | tail -1)
  t=$(grep -E "^Verification Time" "out3_${h##*::}.log" | tail -1)
  [ -z "$v" ] && v="TIMEOUT-OR-ERROR"
  echo "$h $v $t" | tee -a done.txt
done < queue.txt
echo "QUEUE-DRAINED"
