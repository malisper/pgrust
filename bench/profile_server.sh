#!/bin/bash
# Profile a running pgrust_server (or any server) during a wire-protocol benchmark.
# Usage: bench/profile_server.sh [bench_select_wire.sh args...]
# Example: bench/profile_server.sh --port 5444 --password x --rows 10000 --iterations 10 --clients 5
set -euo pipefail

cd "$(dirname "$0")/.."

OUT="/tmp/pgrust_server_profile.out"
ANALYSIS_OUT="/tmp/pgrust_server_profile_analysis.txt"

# Find the server PID (oldest match to skip cargo wrappers).
SERVER_PID=$(pgrep -of pgrust_server || true)
if [[ -z "${SERVER_PID}" ]]; then
    echo "No pgrust_server process found." >&2
    exit 1
fi
echo "Attaching dtrace to pgrust_server (pid=${SERVER_PID})..."

# Cache sudo credentials.
sudo -v

# Start dtrace in the background. Write its PID to a file so we can find it.
sudo dtrace \
    -p "${SERVER_PID}" \
    -x ustackframes=100 \
    -n 'profile-997 /pid == $target/ { @[ustack()] = count(); }' \
    -o "${OUT}" >/dev/null 2>/dev/null &
sleep 2

# Find the actual dtrace process.
DTRACE_PID=$(pgrep -n dtrace || true)
if [[ -z "${DTRACE_PID}" ]]; then
    echo "Warning: could not find dtrace process" >&2
fi

# Run the benchmark.
bench/bench_select_wire.sh "$@"

# Stop dtrace.
sleep 1
if [[ -n "${DTRACE_PID}" ]]; then
    sudo kill -INT "${DTRACE_PID}" 2>/dev/null || true
    # Wait for dtrace to flush output.
    while kill -0 "${DTRACE_PID}" 2>/dev/null; do
        sleep 0.5
    done
fi

echo "Profile saved to ${OUT}"

# Analyze.
bench/analyze_profile.sh "${OUT}" > "${ANALYSIS_OUT}" 2>&1
echo "Analysis saved to ${ANALYSIS_OUT}"
cat "${ANALYSIS_OUT}"
