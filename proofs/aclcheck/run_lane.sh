#!/bin/bash
# aclcheck lane driver (2026-07-30): resumable, one harness at a time,
# 450s wall cap per harness (coordinator rule), 6GB RSS watchdog scoped to
# our own process group, stops before the tool-call budget expires.
# Usage: ./run_lane.sh <results.tsv> <call_budget_seconds>
set -u
cd "$(dirname "$0")"
RES="${1:?results file}"
BUDGET="${2:-480}"
START=$(date +%s)
touch "$RES"

run_one() {
  local name="$1"; shift
  local cmd="$*"
  # replace any timeout in the row with 450
  cmd=$(echo "$cmd" | sed -E 's/timeout [0-9]+ /timeout 450 /')
  echo "[heartbeat] SOLVE $name : $cmd" >&2
  local log="/tmp/aclcheck_lane_$$.log"
  local t0=$(date +%s)
  eval "$cmd" > "$log" 2>&1 &
  local PID=$!
  # RSS watchdog on our own process tree
  while kill -0 $PID 2>/dev/null; do
    sleep 10
    local rss=0
    for p in $(pgrep -g $(ps -o pgid= -p $PID | tr -d ' ') 2>/dev/null); do
      r=$(ps -o rss= -p $p 2>/dev/null | tr -d ' '); rss=$((rss + ${r:-0}))
    done
    if [ $rss -gt 6291456 ]; then
      kill -- -$(ps -o pgid= -p $PID | tr -d ' ') 2>/dev/null
      echo -e "$name\twall(memory>6GB)\t$(( $(date +%s) - t0 ))s" >> "$RES"
      echo "[verdict] $name wall(memory>6GB)" >&2
      return
    fi
  done
  wait $PID; local rc=$?
  local dt=$(( $(date +%s) - t0 ))
  local verdict
  if grep -q "VERIFICATION:- SUCCESSFUL" "$log"; then
    verdict="SUCCESSFUL"
  elif grep -q "VERIFICATION:- FAILED" "$log"; then
    if grep -q "unwinding assertion" "$log"; then verdict="FAILED(unwind)"; else verdict="FAILED"; fi
  elif [ $rc -eq 124 ]; then
    verdict="wall(timeout-450s)"
  else
    verdict="ERROR(rc=$rc)"
  fi
  echo -e "$name\t$verdict\t${dt}s" >> "$RES"
  echo "[verdict] $name $verdict ${dt}s" >&2
  # keep the last failing/erroring log for inspection
  if [ "$verdict" != "SUCCESSFUL" ]; then cp "$log" "logs_$(echo "$name" | tr ':' '_').log" 2>/dev/null; fi
}

grep -E '^proofs::' runqueue.txt | while IFS='|' read -r name expected tier cmd; do
  name=$(echo "$name" | tr -d ' ')
  cmd=$(echo "$cmd" | sed 's/#.*//')
  grep -q "^$name	" "$RES" && continue   # already done
  now=$(date +%s)
  if [ $(( now - START )) -gt "$BUDGET" ]; then
    echo "[budget] call budget exhausted, resume later" >&2
    exit 3
  fi
  run_one "$name" "$cmd"
done
echo "[done] queue pass complete" >&2
