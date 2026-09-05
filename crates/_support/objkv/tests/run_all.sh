#!/usr/bin/env bash
# Runs every objkv end-to-end script in turn and reports. Prerequisites are in
# server.sh. OBJKV_ONLY="a b" runs a subset; OBJKV_SKIP="c d" leaves some out;
# OBJKV_TIMING=1 includes the load-sensitive timing checks.
# OBJKV_SCRIPT_TIMEOUT (seconds, default 600) bounds each script: a hung
# server or psql cannot hold the whole suite.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="${OBJKV_TEST_ROOT:-/tmp/objkv-tests}"
LIMIT="${OBJKV_SCRIPT_TIMEOUT:-600}"
mkdir -p "$ROOT"

# run_timed <seconds> <cmd...>: SIGTERM at the limit, so the script's EXIT
# trap stops its server; SIGKILL 90s later if that did not end it. GNU or
# BSD timeout(1) where there is one (macOS ships one from 13 on), else perl.
run_timed() {
    local t="$1"; shift
    if command -v timeout >/dev/null 2>&1; then
        timeout -k 90 "$t" "$@"
    else
        perl -e '
            my $t = shift; my $pid = fork;
            if (!$pid) { exec @ARGV or die "exec: $!" }
            $SIG{ALRM} = sub {
                kill "TERM", $pid;
                for (1..90) { exit 124 if waitpid($pid, 1) == $pid; sleep 1 }
                kill "KILL", $pid; waitpid $pid, 0; exit 124;
            };
            alarm $t; waitpid $pid, 0;
            exit(($? & 127) ? 128 + ($? & 127) : ($? >> 8));
        ' "$t" "$@"
    fi
}

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
    run_timed "$LIMIT" "$HERE/$n.sh" >"$log" 2>&1; st=$?
    if [ "$st" = 0 ]; then
        passed+=("$n"); echo "    PASS ($(( $(date +%s) - start ))s)"
    else
        failed+=("$n")
        if [ "$st" = 124 ] || [ "$st" = 137 ]; then
            echo "    TIMEOUT after ${LIMIT}s -- $log"
            echo "  FAIL: timed out after ${LIMIT}s" >>"$log"
            # A server the trap did not reach would hold the port for next time.
            pkill -9 -f -- "-D $ROOT/$n/pgdata" 2>/dev/null || true
        else
            echo "    FAIL ($(( $(date +%s) - start ))s) -- $log"
        fi
        grep -E "FAIL" "$log" | head -5 | sed 's/^/      /'
    fi
done

echo
echo "${#passed[@]} passed, ${#failed[@]} failed"
[ "${#failed[@]}" = 0 ] || { echo "failed: ${failed[*]}"; exit 1; }
