#!/bin/bash
# Profile a running pgrust_server (or any server) during a wire-protocol benchmark.
# Usage: bench/profile_server.sh [bench_select_wire.sh args...]
# Example: bench/profile_server.sh --port 5444 --password x --rows 10000 --iterations 10 --clients 5
set -euo pipefail

cd "$(dirname "$0")/.."

OUT="/tmp/pgrust_server_profile.out"
ANALYSIS_OUT="/tmp/pgrust_server_profile_analysis.txt"

# Remove stale profile so we can detect if dtrace fails.
# File may be owned by root (created by sudo dtrace).
sudo rm -f "${OUT}"

# Find the server PID (oldest match to skip cargo wrappers).
SERVER_PID=$(pgrep -of pgrust_server || true)
if [[ -z "${SERVER_PID}" ]]; then
    echo "No pgrust_server process found." >&2
    exit 1
fi
echo "Attaching dtrace to pgrust_server (pid=${SERVER_PID})..."

# Cache sudo credentials.
sudo -v

# Start dtrace in the background.  Keep stderr visible so failures are obvious.
sudo dtrace \
    -p "${SERVER_PID}" \
    -x ustackframes=100 \
    -n 'profile-997 /pid == $target/ { @[ustack()] = count(); }' \
    -o "${OUT}" 2>&1 &
DTRACE_PID=$!
sleep 2

# Verify dtrace is actually running.
if ! kill -0 "${DTRACE_PID}" 2>/dev/null; then
    echo "ERROR: dtrace failed to start (pid=${DTRACE_PID})" >&2
    exit 1
fi
echo "dtrace running (pid=${DTRACE_PID})"

# Run the benchmark.
bench/bench_select_wire.sh "$@"

# Stop dtrace and wait for it to flush.
sleep 1
sudo kill -INT "${DTRACE_PID}" 2>/dev/null || true
wait "${DTRACE_PID}" 2>/dev/null || true

if [[ ! -s "${OUT}" ]]; then
    echo "ERROR: dtrace produced no output in ${OUT}" >&2
    exit 1
fi

echo "Profile saved to ${OUT}"

# Analyze.
bench/analyze_profile.sh "${OUT}" > "${ANALYSIS_OUT}" 2>&1
echo "Analysis saved to ${ANALYSIS_OUT}"
cat "${ANALYSIS_OUT}"
