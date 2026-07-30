#!/bin/zsh
# run-w2.sh <harness> [--c-lib files...] [extra cargo-kani args...]
# Wave-2 runner (2026-07-30): as run-one.sh but 450s timeout (saturated-box
# protocol) and caller-chosen --c-lib set so the cast cells can link
# pg_jsonb.c + pg_jsonb_casts.c and gin links only pg_gin_cmp.c
# (C-shim-hygiene: prune per-harness C files).
cd "$(dirname "$0")"
h=$1; shift
log=/tmp/w2j-$h.log

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

while pgrep -f "proof_jsonb_probe.*\.out" >/dev/null 2>&1; do sleep 5; done

start=$(date +%s)
timeout 450 cargo kani -Z c-ffi -Z stubbing --harness "$h" "$@" > $log 2>&1 &
PID=$!
memkill=0
sleep 15
while kill -0 $PID 2>/dev/null; do
  rss=$(ps axo rss=,command= | grep -E "proof_jsonb_probe|jsonb-probe" | grep -vE "grep|run-w2" | awk '{s+=$1} END {print s+0}')
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
load=$(uptime | sed 's/.*load averages: //' | awk '{print $1}')
verdict=$(grep -m1 "VERIFICATION:-" $log | sed 's/.*VERIFICATION:- //')
vtime=$(grep -m1 "Verification Time:" $log | sed 's/.*Verification Time: //')
[ $memkill -eq 1 ] && verdict="wall(memory>6GiB)"
[ -z "$verdict" ] && verdict="TIMEOUT(rc=$rc,450s)"
echo "$h\t$verdict\t$vtime\twall=$((end-start))s\tload=$load" | tee -a /tmp/w2-jsonb-results.tsv
