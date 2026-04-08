#!/bin/bash
# Benchmark regex performance: pgrust vs PostgreSQL.
# Uses the mariomka/regex-benchmark patterns (email, URI, IP) against a table
# of text data, comparing execution time via EXPLAIN ANALYZE.
#
# Prerequisites:
#   - pgrust server running on port 5433
#   - PostgreSQL running on port 5432
#   - psql available on PATH
#
# Usage: bench/bench_regex_compare.sh [--iterations N] [--skip-load]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

HOST="${PGHOST:-127.0.0.1}"
USER="${PGUSER:-postgres}"
PASSWORD="${PGPASSWORD:-postgres}"
ITERATIONS=25

PGRUST_PORT=5433
PG_PORT=5432
SKIP_LOAD=false

# mariomka regex-benchmark patterns
# Disable globbing so bracket expressions aren't interpreted as filename globs.
set -o noglob
PATTERN_NAMES=("Email" "URI" "IP")
PATTERNS=(
    '[\w\.+-]+@[\w\.-]+\.[\w\.-]+'
    '[\w]+://[^/\s?#]+[^\s?#]+(?:\?[^\s#]*)?(?:#[^\s]*)?'
    '(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9])'
)
set +o noglob

while [[ $# -gt 0 ]]; do
    case "$1" in
        --iterations) ITERATIONS="$2"; shift 2 ;;
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
    echo "Generating regex benchmark data..." >&2
    SQL_FILE=$(mktemp)
    python3 "${SCRIPT_DIR}/data/generate_regex_data.py" > "${SQL_FILE}"

    echo "Loading into pgrust (port ${PGRUST_PORT})..." >&2
    psql_cmd "${PGRUST_PORT}" -q < "${SQL_FILE}"

    echo "Loading into PostgreSQL (port ${PG_PORT})..." >&2
    psql_cmd "${PG_PORT}" -q < "${SQL_FILE}"

    rm -f "${SQL_FILE}"
fi

# ── Verify row counts ──────────────────────────────────────────────────────
PGRUST_COUNT=$(psql_cmd "${PGRUST_PORT}" -t -A -c "SELECT COUNT(*) FROM regexbench;")
PG_COUNT=$(psql_cmd "${PG_PORT}" -t -A -c "SELECT COUNT(*) FROM regexbench;")
echo "Row counts — pgrust: ${PGRUST_COUNT}, postgres: ${PG_COUNT}" >&2

if [[ "${PGRUST_COUNT}" != "${PG_COUNT}" ]]; then
    echo "WARNING: row counts differ!" >&2
fi

# ── Disable parallel query on PostgreSQL ────────────────────────────────────
psql_cmd "${PG_PORT}" -c "ALTER SYSTEM SET max_parallel_workers_per_gather = 0;" >/dev/null 2>&1 || true
psql_cmd "${PG_PORT}" -c "SELECT pg_reload_conf();" >/dev/null 2>&1 || true

# ── Run benchmark ───────────────────────────────────────────────────────────
run_bench() {
    local port="$1"
    local iters="$2"
    local pattern="$3"

    local qfile
    qfile=$(mktemp)
    for ((i=0; i<iters; i++)); do
        echo "EXPLAIN (ANALYZE, TIMING OFF) SELECT COUNT(*) FROM regexbench WHERE content ~ '${pattern}';" >> "${qfile}"
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
    psql_cmd "${port}" -t -A -c "SELECT COUNT(*) FROM regexbench WHERE content ~ '${pattern}';"
}

# ── Warmup ──────────────────────────────────────────────────────────────────
echo "" >&2
echo "Warming up..." >&2
for pattern in "${PATTERNS[@]}"; do
    psql_cmd "${PGRUST_PORT}" -c "SELECT COUNT(*) FROM regexbench WHERE content ~ '${pattern}';" >/dev/null 2>&1
    psql_cmd "${PG_PORT}" -c "SELECT COUNT(*) FROM regexbench WHERE content ~ '${pattern}';" >/dev/null 2>&1
done

# ── Benchmark + Report ──────────────────────────────────────────────────────
echo ""
echo "Regex Benchmark: pgrust vs PostgreSQL (${ITERATIONS} iterations, ${PGRUST_COUNT} rows)"
echo "================================================================================"

for idx in "${!PATTERN_NAMES[@]}"; do
    name="${PATTERN_NAMES[$idx]}"
    pattern="${PATTERNS[$idx]}"

    echo ""
    echo "Pattern: ${name}"
    echo "  regex: ${pattern}"

    # Match counts
    pgrust_matches=$(get_match_count "${PGRUST_PORT}" "${pattern}")
    pg_matches=$(get_match_count "${PG_PORT}" "${pattern}")

    if [[ "${pgrust_matches}" != "${pg_matches}" ]]; then
        echo "  NOTE: match counts differ — pgrust=${pgrust_matches}, postgres=${pg_matches}"
    else
        echo "  matches: ${pgrust_matches}"
    fi

    printf "  %-10s  %8s  %8s  %8s  %8s  %5s\n" \
        "engine" "avg_ms" "p50_ms" "min_ms" "p99_ms" "n"
    printf "  %s\n" "$(printf '%.0s-' {1..56})"

    pgrust_result=$(run_bench "${PGRUST_PORT}" "${ITERATIONS}" "${pattern}" 2>/dev/null)
    pg_result=$(run_bench "${PG_PORT}" "${ITERATIONS}" "${pattern}" 2>/dev/null)

    if [[ -n "${pgrust_result}" ]]; then
        printf "  %-10s  %s\n" "pgrust" \
            "$(echo "${pgrust_result}" | awk -F'\t' '{printf "%8s  %8s  %8s  %8s  %5s", $1, $2, $3, $4, $5}')"
    else
        printf "  %-10s  %8s\n" "pgrust" "FAILED"
    fi

    if [[ -n "${pg_result}" ]]; then
        printf "  %-10s  %s\n" "postgres" \
            "$(echo "${pg_result}" | awk -F'\t' '{printf "%8s  %8s  %8s  %8s  %5s", $1, $2, $3, $4, $5}')"
    else
        printf "  %-10s  %8s\n" "postgres" "FAILED"
    fi

    # Speedup ratio
    if [[ -n "${pgrust_result}" && -n "${pg_result}" ]]; then
        python3 -c "
pgrust_avg = float('${pgrust_result}'.split('\t')[0])
pg_avg = float('${pg_result}'.split('\t')[0])
if pgrust_avg > 0:
    ratio = pg_avg / pgrust_avg
    if ratio >= 1:
        print(f'  speedup: {ratio:.2f}x (pgrust is faster)')
    else:
        print(f'  speedup: {1/ratio:.2f}x (postgres is faster)')
"
    fi
done

echo ""
