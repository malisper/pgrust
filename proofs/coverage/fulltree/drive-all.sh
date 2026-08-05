#!/bin/bash
# drive-all.sh — serial, load-aware full-tree capture driver.
# Runs every family in families.txt through run-family.sh, banking each with a
# commit+push. Circuit breaker per the run charter: if cumulative
# FAILED-TO-RUN (structural: name resolution, build failures — NOT walls)
# exceeds 15% of harnesses attempted so far after >=3 families, STOP and leave
# STOPPED-STRUCTURAL for the operator instead of grinding through.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
DONE="$HERE/logs/families.done"
touch "$DONE"

# single-driver lock: two drivers racing one census file corrupted a family
# (observed 08:44 — a pkill'd driver survived mid-solve and resumed).
LOCK="$HERE/logs/driver.pid"
if [ -f "$LOCK" ] && kill -0 "$(cat "$LOCK")" 2>/dev/null; then
    echo "FATAL: driver already running (pid $(cat "$LOCK")); refusing to race it" >&2
    exit 7
fi
echo $$ > "$LOCK"

total=0; failed=0; nfam=0
while IFS=$'\t' read -r FAM T NH COST; do
    [ -z "$FAM" ] && continue
    if grep -qxF "$FAM" "$DONE"; then
        echo "skip $FAM (already done)"; continue
    fi
    "$HERE/run-family.sh" "$FAM" "$T"
    RFRC=$?
    C="$HERE/census/census-$FAM.tsv"
    t=$(awk 'NR>1{n++} END{print n+0}' "$C")
    f=$(awk -F'\t' 'NR>1 && ($7=="FAILED-TO-RUN" || $7=="NOFLAGS"){n++} END{print n+0}' "$C")
    total=$((total+t)); failed=$((failed+f)); nfam=$((nfam+1))
    if [ "$RFRC" -eq 0 ]; then
        echo "$FAM" >> "$DONE"
    else
        echo "family $FAM INCOMPLETE (rc=$RFRC) — left out of families.done for re-run"
    fi
    if [ "$nfam" -ge 3 ] && [ "$total" -gt 0 ] && \
       [ $((failed*100)) -gt $((total*15)) ]; then
        echo "STRUCTURAL FAILURE BREAKER: $failed/$total harnesses FAILED-TO-RUN (>15%)." \
            | tee "$HERE/STOPPED-STRUCTURAL"
        exit 9
    fi
done < "$HERE/families.txt"
echo "ALL FAMILIES DONE: $nfam families, $total harnesses, $failed failed-to-run"
