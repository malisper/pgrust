#!/bin/zsh
# run-one.sh <harness> [extra cargo-kani args...]
# Memory-protocol runner (coordinator mandate 2026-07-28):
#  - ONE kani/cbmc at a time (callers run this sequentially);
#  - vm_stat gate before launch: wait until free+inactive+purgeable >= 4GiB;
#  - RSS watchdog: poll own family's processes every 15s, kill own process
#    tree at > 6 GiB and record wall(memory) as the verdict;
#  - own-family kills only (pattern = this crate's mangled target name).
cd "$(dirname "$0")"
h=$1; shift
log=/tmp/jp2-$h.log

# vm_stat gate (page size 16384 on Apple Silicon)
until vm_stat | awk -v need=$((4 * 1024 * 1024 * 1024)) '
  /page size of/ { ps = $8 }
  /Pages free/ { f = $3 }
  /Pages inactive/ { i = $3 }
  /Pages purgeable/ { p = $3 }
  END { gsub(/\./, "", f); gsub(/\./, "", i); gsub(/\./, "", p);
        exit ((f + i + p) * ps >= need) ? 0 : 1 }'; do
  sleep 30
done

# wait out any of our own dying cbmc from a previous kill (their RSS would
# false-fire the watchdog at t=0)
while pgrep -f "proof_jsonb_probe.*\.out" >/dev/null 2>&1; do sleep 5; done

start=$(date +%s)
# extra args AFTER --harness so --cbmc-args (which swallows the rest) works
timeout 240 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_jsonb.c --harness "$h" "$@" > $log 2>&1 &
PID=$!
memkill=0
sleep 15   # grace period before the first RSS check
while kill -0 $PID 2>/dev/null; do
  rss=$(ps axo rss=,command= | grep -E "proof_jsonb_probe|jsonb-probe" | grep -vE "grep|run-one" | awk '{s+=$1} END {print s+0}')
  if [ "${rss:-0}" -gt 6291456 ]; then
    memkill=1
    kill -- -$PID 2>/dev/null || kill $PID 2>/dev/null
    pkill -f "proof_jsonb_probe.*\.out" 2>/dev/null
    break
  fi
  sleep 15
done
wait $PID 2>/dev/null
rc=$?
end=$(date +%s)
verdict=$(grep -m1 "VERIFICATION:-" $log | sed 's/.*VERIFICATION:- //')
vtime=$(grep -m1 "Verification Time:" $log | sed 's/.*Verification Time: //')
[ $memkill -eq 1 ] && verdict="wall(memory>6GiB)"
[ -z "$verdict" ] && verdict="TIMEOUT(rc=$rc)"
echo "$h\t$verdict\t$vtime\twall=$((end-start))s" | tee -a /tmp/jsonb-probe-final.tsv
