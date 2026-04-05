#!/bin/bash
# Profile the insert benchmark after initialization.
# Usage: ./profile_insert.sh [rows] [output_file]
#
# Must be run from a terminal with sudo available for dtrace.
set -euo pipefail

AUTOCOMMIT=""
ROWS=""
OUT=""

for arg in "$@"; do
    case "${arg}" in
        --autocommit) AUTOCOMMIT="--autocommit" ;;
        *)
            if [[ -z "${ROWS}" ]]; then
                ROWS="${arg}"
            elif [[ -z "${OUT}" ]]; then
                OUT="${arg}"
            fi
            ;;
    esac
done

ROWS="${ROWS:-2000000}"
OUT="${OUT:-/tmp/dtrace_insert_stacks_$(date +%s).out}"
ANALYSIS_OUT="${OUT%.out}_analysis.txt"

BENCH_PID=""
DTRACE_PID=""

cleanup() {
    if [[ -n "${DTRACE_PID}" ]] && kill -0 "${DTRACE_PID}" 2>/dev/null; then
        sudo kill -INT "${DTRACE_PID}" 2>/dev/null || true
        wait "${DTRACE_PID}" 2>/dev/null || true
    fi
    if [[ -n "${BENCH_PID}" ]] && kill -0 "${BENCH_PID}" 2>/dev/null; then
        kill "${BENCH_PID}" 2>/dev/null || true
        wait "${BENCH_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

cd "$(dirname "$0")/.."

cargo build --release

# Cache sudo credentials up front.
sudo -v

# Launch benchmark. It SIGSTOPs itself after setup when --wait is passed.
./target/release/bench_insert --rows "${ROWS}" ${AUTOCOMMIT} --wait &
BENCH_PID=$!

# Wait for it to stop after initialization.
while kill -0 "${BENCH_PID}" 2>/dev/null; do
    STATE="$(ps -o state= -p "${BENCH_PID}" 2>/dev/null || echo gone)"
    if [[ "${STATE}" == *T* ]]; then
        break
    fi
    sleep 0.1
done

if ! kill -0 "${BENCH_PID}" 2>/dev/null; then
    echo "Benchmark exited before dtrace could attach" >&2
    exit 1
fi

echo "Benchmark initialized (pid=${BENCH_PID}). Starting dtrace..."

# Attach dtrace directly to the benchmark PID. When the benchmark exits,
# dtrace should exit and flush the aggregated stacks to disk.
sudo dtrace \
    -p "${BENCH_PID}" \
    -x ustackframes=100 \
    -n 'profile-997 /pid == $target/ { @[ustack()] = count(); }' \
    -o "${OUT}" &
DTRACE_PID=$!

sleep 1

# Resume the benchmark and wait for both processes to finish.
kill -CONT "${BENCH_PID}"
wait "${BENCH_PID}" || true
BENCH_PID=""

echo "Benchmark finished. Waiting for dtrace to flush..."
wait "${DTRACE_PID}" || true
DTRACE_PID=""

trap - EXIT

echo "Done. Output in ${OUT}"

bench/analyze_profile.sh "${OUT}" > "${ANALYSIS_OUT}" 2>&1
echo "Analysis saved to ${ANALYSIS_OUT}"
