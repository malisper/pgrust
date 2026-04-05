#!/bin/bash
# Profile the full_scan_bench binary with dtrace.
# Usage: bench/profile.sh [--rows N] [--iterations N] [--clients N] [--pool-size N]
# Defaults: --rows 10000 --iterations 100 --clients 1 --pool-size 16384
set -e

cd "$(dirname "$0")/.."

ROWS=10000
ITERATIONS=100
CLIENTS=1
POOL_SIZE=16384
OUT=/tmp/dtrace_stacks.out

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rows) ROWS="$2"; shift 2 ;;
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --clients) CLIENTS="$2"; shift 2 ;;
        --pool-size) POOL_SIZE="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

ANALYSIS_OUT="${OUT%.out}_analysis.txt"

cargo build --release
sudo rm -rf /tmp/pgrust_flamegraph_bench

# Load data (single-threaded, untimed).
./target/release/full_scan_bench \
    --dir /tmp/pgrust_flamegraph_bench \
    --rows "${ROWS}" \
    --iterations 1 \
    --pool-size "${POOL_SIZE}"

# Profile the scan phase.
sudo dtrace -x ustackframes=100 \
    -n 'profile-997 /pid == $target/ { @[ustack()] = count(); }' \
    -c "./target/release/full_scan_bench --preserve-existing --skip-load --dir /tmp/pgrust_flamegraph_bench --rows ${ROWS} --iterations ${ITERATIONS} --clients ${CLIENTS} --pool-size ${POOL_SIZE}" \
    -o "${OUT}"

echo "Done. Output in ${OUT}"

bench/analyze_profile.sh "${OUT}" > "${ANALYSIS_OUT}" 2>&1
echo "Analysis saved to ${ANALYSIS_OUT}"
