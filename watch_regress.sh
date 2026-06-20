#!/bin/bash
# Watch the pgrust regression measure, refreshing every 5s. Shows pass-rate + a
# live profile (slowest files by query time + timeouts). Defaults to the latest
# timestamped run via the `latest` symlink.
# Usage: ./watch_regress.sh        (Ctrl-C to stop)
S=${1:-/private/tmp/qmeasure_runs/latest/_status.tsv}
INT=${2:-5}
while :; do
  clear
  echo "── regression progress $(date +%H:%M:%S) ──  ($S)"
  if [ ! -f "$S" ]; then echo "no status file yet (build/early phase)"; sleep "$INT"; continue; fi
  run=$(pgrep -f '/qmeasure/run_sharded.sh' >/dev/null && echo RUNNING || echo DONE/stopped)
  echo "recorded: $(grep -c . "$S")/229   [$run]   last: $(tail -1 "$S" | cut -f1,2)"
  echo
  awk -F'\t' '{c[$2]++;t+=$3;m+=$4} END{
    printf "FILES: PASS=%d FAIL=%d CRASHED=%d TIMEOUT=%d INFRA=%d BOOTFAIL=%d\n",c["PASS"],c["FAIL"],c["CRASHED"],c["TIMEOUT"],c["INFRA"],c["BOOTFAIL"];
    if(t>0)printf "QUERY incl-crash: %.1f%%  (%d/%d stmts)\n",100*m/t,m,t}' "$S"
  echo
  echo "── PROFILE ──"
  echo "TIMEOUTs (slow/hung — planner/perf targets): $(awk -F'\t' '$2=="TIMEOUT"{print $1}' "$S" | tr '\n' ' ')"
  echo "slowest 8 files by query time (run_s):"
  awk -F'\t' 'NF>=7{printf "  %4ds  %-20s %s\n",$7,$1,$2}' "$S" 2>/dev/null | sort -rn | head -8
  echo "(refresh ${INT}s · Ctrl-C to stop)"
  sleep "$INT"
done
