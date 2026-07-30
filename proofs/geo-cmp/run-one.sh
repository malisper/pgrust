#!/bin/bash
# run-one.sh <harness> <timeout-s> [extra cargo-kani args...]
# Memory protocol per prove-target skill: one solve at a time, vm_stat gate,
# 6 GiB RSS watchdog scoped to THIS run's process group only (never pkill by
# binary name on a shared machine).
set -m
H=$1; T=$2; shift 2
DIR="$(cd "$(dirname "$0")" && pwd)"
LOGDIR=${GEO_CMP_LOGDIR:-/tmp/geo-cmp-logs}
mkdir -p "$LOGDIR"
LOG="$LOGDIR/$H.log"

# vm_stat gate: refuse to launch when free+inactive < 4 GiB
freegb=$(vm_stat | awk '/Pages free/{f=$3} /Pages inactive/{i=$3} END{gsub(/\./,"",f); gsub(/\./,"",i); printf "%.1f", (f+i)*16384/1073741824}')
awk -v g="$freegb" 'BEGIN{exit !(g < 4.0)}' && { echo "VMGATE-FAIL free+inactive=${freegb}GiB"; exit 97; }

cd "$DIR"
start=$(date +%s)
timeout "$T" cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_geo_cmp.c --no-overflow-checks --harness "$H" --exact "$@" >"$LOG" 2>&1 &
PID=$!
while kill -0 "$PID" 2>/dev/null; do
    rss=$(ps -axo pgid=,rss= | awk -v g="$PID" '$1==g{s+=$2} END{print s+0}')
    if [ "$rss" -gt $((6 * 1024 * 1024)) ]; then
        echo "RSS-KILL pgid=$PID rss_kb=$rss"
        kill -- -"$PID" 2>/dev/null
    fi
    sleep 2
done
wait "$PID"; rc=$?
end=$(date +%s)
echo "harness=$H rc=$rc wall=$((end - start))s free_gate=${freegb}GiB"
grep -E "VERIFICATION:|Verification Time:|Failed Checks|CBMC failed|error\[|error:" "$LOG" | head -8
exit $rc
