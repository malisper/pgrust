#!/bin/bash
# run-one.sh <timeout_s> <harness> [extra kani args...]
# ONE run at a time, own-process-group RSS watchdog (>6GB => kill, verdict
# wall(memory)) per the 2026-07-28 memory protocol.
set -u
cd "$(dirname "$0")"
T="$1"; H="$2"; shift 2
LOG=/tmp/f_${H}.log
( exec cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_text_slice.c \
      --exact --harness "proofs::$H" "$@" > "$LOG" 2>&1 ) &
PID=$!
START=$(date +%s)
VERDICT=""
while kill -0 $PID 2>/dev/null; do
    NOW=$(date +%s)
    if (( NOW - START > T )); then VERDICT="timeout"; break; fi
    # RSS of our whole process group (KB)
    TOT=$(ps -o rss= -g $(ps -o pgid= -p $PID | tr -d ' ') 2>/dev/null | awk '{s+=$1} END {print s+0}')
    if (( TOT > 6000000 )); then VERDICT="wall(memory)"; break; fi
    sleep 15
done
if [ -n "$VERDICT" ]; then
    PGID=$(ps -o pgid= -p $PID 2>/dev/null | tr -d ' ')
    [ -n "$PGID" ] && kill -- -"$PGID" 2>/dev/null
    kill $PID 2>/dev/null
    echo "$H $VERDICT"
else
    wait $PID; EC=$?
    echo "$H exit=$EC $(grep -E 'VERIFICATION:|Verification Time|status 15' "$LOG" | tr '\n' ' ')"
fi
