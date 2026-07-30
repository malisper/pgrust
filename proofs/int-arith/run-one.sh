#!/bin/bash
# run-one.sh <harness> [extra kani flags...] — one serial kani run under the
# mandatory memory protocol: 30s hard cap (timeout), 6 GiB RSS watchdog over
# the run's own process group (never pkill by binary name), one solver at a
# time. Prints: harness<TAB>status<TAB>wall_s<TAB>verification_time<TAB>flags
set -u
cd "$(dirname "$0")"
H="$1"; shift
FLAGS=("$@")
LOG="${RUNLOG_DIR:-/tmp}/kani-$H$(echo "${FLAGS[*]:-}" | tr -cd 'a-z').log"
set -m
START=$(date +%s)
timeout 30 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_int_arith.c --harness "proofs::$H" --exact ${FLAGS[@]+"${FLAGS[@]}"} >"$LOG" 2>&1 &
PID=$!
MEMKILL=0
while kill -0 "$PID" 2>/dev/null; do
    RSS=$(ps ax -o pgid=,rss= | awk -v p="$PID" '$1==p{s+=$2}END{print s+0}')
    if [ "$RSS" -gt 6291456 ]; then
        kill -- "-$PID" 2>/dev/null
        MEMKILL=1
        break
    fi
    sleep 2
done
wait "$PID" 2>/dev/null
RC=$?
WALL=$(( $(date +%s) - START ))
VT=$(grep -o 'Verification Time: .*' "$LOG" | tail -1 | sed 's/Verification Time: //')
if [ "$MEMKILL" = 1 ]; then STATUS="MEMKILL(>6GiB)"
elif grep -q 'VERIFICATION:- SUCCESSFUL' "$LOG"; then STATUS=PROVED
elif grep -q 'VERIFICATION:- FAILED' "$LOG"; then STATUS=FAILED
elif [ "$RC" = 124 ]; then STATUS="TIMEOUT(30s)"
else STATUS="ERROR(rc=$RC)"
fi
printf '%s\t%s\t%ss\t%s\t%s\n' "$H" "$STATUS" "$WALL" "${VT:-n/a}" "${FLAGS[*]:-default}"
