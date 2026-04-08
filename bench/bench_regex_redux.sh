#!/bin/bash
# Benchmark regex-redux patterns: pgrust vs PostgreSQL.
# Uses the 9 DNA 8-mer patterns from the Benchmarks Game regex-redux challenge
# against a table of DNA sequence data.
#
# Prerequisites:
#   - pgrust server running on port 5433
#   - PostgreSQL running on port 5432
#   - psql available on PATH
#
# Usage:
#   bench/bench_regex_redux.sh [--iterations N] [--lines N] [--skip-load]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

HOST="${PGHOST:-127.0.0.1}"
USER="${PGUSER:-postgres}"
PASSWORD="${PGPASSWORD:-postgres}"
ITERATIONS=25
DNA_LINES=500000
SKIP_LOAD=false

PGRUST_PORT=5433
PG_PORT=5432

# The 9 regex-redux patterns: DNA 8-mers + reverse complement with one wildcard.
# Disable globbing so bracket expressions aren't treated as filename globs.
set -o noglob
PATTERN_NAMES=(
    "agggtaaa|tttaccct"
    "[cgt]gggtaaa|tttaccc[acg]"
    "a[act]ggtaaa|tttacc[agt]t"
    "ag[act]gtaaa|tttac[agt]ct"
    "agg[act]taaa|ttta[agt]cct"
    "aggg[acg]aaa|ttt[cgt]ccct"
    "agggt[cgt]aa|tt[acg]accct"
    "agggta[cgt]a|t[acg]taccct"
    "agggtaa[cgt]|[acg]ttaccct"
)
# Patterns are the same as names for this benchmark
PATTERNS=("${PATTERN_NAMES[@]}")
set +o noglob

while [[ $# -gt 0 ]]; do
    case "$1" in
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --lines) DNA_LINES="$2"; shift 2 ;;
        --skip-load) SKIP_LOAD=true; shift ;;
        --pgrust-port) PGRUST_PORT="$2"; shift 2 ;;
        --pg-port) PG_PORT="$2"; shift 2 ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

export PGPASSWORD="${PASSWORD}"

psql_cmd() {
    local port="$1"; shift
    psql -w -h "${HOST}" -p "${port}" -U "${USER}" "$@"
}

# ── Load data ───────────────────────────────────────────────────────────────
if [[ "${SKIP_LOAD}" == "false" ]]; then
    echo "Generating ${DNA_LINES} lines of DNA sequence data..." >&2
    SQL_FILE=$(mktemp)
    python3 "${SCRIPT_DIR}/data/generate_dna_data.py" --lines "${DNA_LINES}" > "${SQL_FILE}"

    echo "Loading into pgrust (port ${PGRUST_PORT})..." >&2
    psql_cmd "${PGRUST_PORT}" -q < "${SQL_FILE}"

    echo "Loading into PostgreSQL (port ${PG_PORT})..." >&2
    psql_cmd "${PG_PORT}" -q < "${SQL_FILE}"

    rm -f "${SQL_FILE}"
fi

# ── Verify row counts ──────────────────────────────────────────────────────
PGRUST_COUNT=$(psql_cmd "${PGRUST_PORT}" -t -A -c "SELECT COUNT(*) FROM dnabench;")
PG_COUNT=$(psql_cmd "${PG_PORT}" -t -A -c "SELECT COUNT(*) FROM dnabench;")
echo "Row counts — pgrust: ${PGRUST_COUNT}, postgres: ${PG_COUNT}" >&2

if [[ "${PGRUST_COUNT}" != "${PG_COUNT}" ]]; then
    echo "WARNING: row counts differ!" >&2
fi

# ── Disable parallel query on PostgreSQL ────────────────────────────────────
psql_cmd "${PG_PORT}" -c "ALTER SYSTEM SET max_parallel_workers_per_gather = 0;" >/dev/null 2>&1 || true
psql_cmd "${PG_PORT}" -c "SELECT pg_reload_conf();" >/dev/null 2>&1 || true

# ── Benchmark functions ─────────────────────────────────────────────────────
run_bench() {
    local port="$1"
    local iters="$2"
    local pattern="$3"

    local qfile
    qfile=$(mktemp)
    for ((i=0; i<iters; i++)); do
        echo "EXPLAIN (ANALYZE, TIMING OFF) SELECT COUNT(*) FROM dnabench WHERE seq ~ '${pattern}';" >> "${qfile}"
    done

    local tmpfile
    tmpfile=$(mktemp)
    psql_cmd "${port}" -t -A -f "${qfile}" > "${tmpfile}" 2>&1

    python3 -c "
import re, sys

times = []
with open('${tmpfile}') as f:
    for line in f:
        m = re.search(r'Execution Time:\s*([\d.]+)\s*ms', line)
        if m:
            times.append(float(m.group(1)))

if not times:
    print('NO_DATA', file=sys.stderr)
    sys.exit(0)

times.sort()
n = len(times)
avg = sum(times) / n
p50 = times[n // 2]
mn = times[0]
p99 = times[int(n * 0.99)]
print(f'{avg:.3f}\t{p50:.3f}\t{mn:.3f}\t{p99:.3f}\t{n}')
"

    rm -f "${tmpfile}" "${qfile}"
}

get_match_count() {
    local port="$1"
    local pattern="$2"
    psql_cmd "${port}" -t -A -c "SELECT COUNT(*) FROM dnabench WHERE seq ~ '${pattern}';"
}

# ── Warmup ──────────────────────────────────────────────────────────────────
echo "" >&2
echo "Warming up..." >&2
# Warm up with just the first pattern
set -o noglob
psql_cmd "${PGRUST_PORT}" -c "SELECT COUNT(*) FROM dnabench WHERE seq ~ '${PATTERNS[0]}';" >/dev/null 2>&1
psql_cmd "${PG_PORT}" -c "SELECT COUNT(*) FROM dnabench WHERE seq ~ '${PATTERNS[0]}';" >/dev/null 2>&1
set +o noglob

# ── Benchmark + Report ──────────────────────────────────────────────────────
echo ""
echo "Regex-Redux Benchmark: pgrust vs PostgreSQL (${ITERATIONS} iterations, ${PGRUST_COUNT} rows)"
echo "================================================================================"
echo "DNA sequence data: ${PGRUST_COUNT} rows x 60 chars = ~$((PGRUST_COUNT * 60 / 1000000))MB"
echo ""

printf "  %-35s  %6s  %6s  |  %8s  %8s  |  %8s  %8s  |  %7s\n" \
    "pattern" "pgrust" "pg" "pgrust" "pg" "pgrust" "pg" "speedup"
printf "  %-35s  %6s  %6s  |  %8s  %8s  |  %8s  %8s  |  %7s\n" \
    "" "match" "match" "avg_ms" "avg_ms" "min_ms" "min_ms" ""
printf "  %s\n" "$(printf '%.0s-' {1..108})"

set -o noglob
for idx in "${!PATTERNS[@]}"; do
    pattern="${PATTERNS[$idx]}"

    # Match counts
    pgrust_matches=$(get_match_count "${PGRUST_PORT}" "${pattern}")
    pg_matches=$(get_match_count "${PG_PORT}" "${pattern}")

    # Benchmark
    pgrust_result=$(run_bench "${PGRUST_PORT}" "${ITERATIONS}" "${pattern}" 2>/dev/null)
    pg_result=$(run_bench "${PG_PORT}" "${ITERATIONS}" "${pattern}" 2>/dev/null)

    pgrust_avg="FAIL"
    pgrust_min="FAIL"
    pg_avg="FAIL"
    pg_min="FAIL"
    speedup="-"

    if [[ -n "${pgrust_result}" ]]; then
        pgrust_avg=$(echo "${pgrust_result}" | cut -f1)
        pgrust_min=$(echo "${pgrust_result}" | cut -f3)
    fi
    if [[ -n "${pg_result}" ]]; then
        pg_avg=$(echo "${pg_result}" | cut -f1)
        pg_min=$(echo "${pg_result}" | cut -f3)
    fi

    if [[ "${pgrust_avg}" != "FAIL" && "${pg_avg}" != "FAIL" ]]; then
        speedup=$(python3 -c "
pa = float('${pgrust_avg}')
ga = float('${pg_avg}')
if pa > 0:
    r = ga / pa
    if r >= 1:
        print(f'{r:.1f}x')
    else:
        print(f'{1/r:.1f}x PG')
")
    fi

    match_note=""
    if [[ "${pgrust_matches}" != "${pg_matches}" ]]; then
        match_note=" MISMATCH!"
    fi

    printf "  %-35s  %6s  %6s  |  %8s  %8s  |  %8s  %8s  |  %7s%s\n" \
        "${pattern}" "${pgrust_matches}" "${pg_matches}" \
        "${pgrust_avg}" "${pg_avg}" "${pgrust_min}" "${pg_min}" \
        "${speedup}" "${match_note}"
done
set +o noglob

echo ""
