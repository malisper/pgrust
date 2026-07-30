#!/bin/bash
# Sequential harness runner with one retry on cross-agent "status 15" kills.
# Usage: run-queue.sh <outfile> <harness...>   (kissat; controls run separately
# with the default solver).
set -u
cd "$(dirname "$0")"
OUT="$1"; shift
: > "$OUT"
for h in "$@"; do
    for attempt in 1 2; do
        log=/tmp/tsq_${h}_${attempt}.log
        timeout 350 cargo kani -Z c-ffi -Z stubbing --solver kissat \
            --c-lib c/pg_text_slice.c --harness "proofs::$h" --exact \
            > "$log" 2>&1
        ec=$?
        # kill any solver orphans from OUR crate only
        pgrep -f "text-slice/target" | xargs -I{} kill {} 2>/dev/null
        if grep -q "status 15" "$log" && ! grep -q "VERIFICATION:- SUCCESSFUL" "$log"; then
            echo "$h attempt$attempt: status15 (cross-agent kill), retrying" >> "$OUT"
            continue
        fi
        echo "$h exit=$ec $(grep -E 'VERIFICATION:-|Verification Time' "$log" | tr '\n' ' ')" >> "$OUT"
        break
    done
done
echo ALL-DONE >> "$OUT"
