#!/usr/bin/env bash
# Runs every objkv end-to-end script in turn and reports. Prerequisites are in
# server.sh. OBJKV_ONLY="a b" runs a subset; OBJKV_SKIP="c d" leaves some out;
# OBJKV_TIMING=1 includes the load-sensitive timing checks.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="${OBJKV_TEST_ROOT:-/tmp/objkv-tests}"
mkdir -p "$ROOT"

scripts=()
for f in "$HERE"/*.sh; do
    n=$(basename "$f" .sh)
    case "$n" in server|run_all) continue;; esac
    if [ -n "${OBJKV_ONLY:-}" ]; then case " $OBJKV_ONLY " in *" $n "*) ;; *) continue;; esac; fi
    case " ${OBJKV_SKIP:-} " in *" $n "*) continue;; esac
    scripts+=("$n")
done

passed=(); failed=()
for n in "${scripts[@]}"; do
    log="$ROOT/$n.out"
    start=$(date +%s)
    echo "=== $n"
    if "$HERE/$n.sh" >"$log" 2>&1; then
        passed+=("$n"); echo "    PASS ($(( $(date +%s) - start ))s)"
    else
        failed+=("$n"); echo "    FAIL ($(( $(date +%s) - start ))s) -- $log"
        grep -E "FAIL" "$log" | head -5 | sed 's/^/      /'
    fi
done

echo
echo "${#passed[@]} passed, ${#failed[@]} failed"
[ "${#failed[@]}" = 0 ] || { echo "failed: ${failed[*]}"; exit 1; }
