#!/bin/bash
# Benchmark using EXPLAIN ANALYZE to measure server-side execution time
# without network overhead.
# Usage: bench/bench_explain.sh [--port PORT] [--rows ROWS] [--iterations ITERS] [--count]
set -euo pipefail

PORT=5432
HOST="${PGHOST:-127.0.0.1}"
USER="${PGUSER:-postgres}"
PASSWORD="${PGPASSWORD:-postgres}"
ROWS=10000
ITERATIONS=10
COUNT_ONLY=false
SKIP_LOAD=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        --host) HOST="$2"; shift 2 ;;
        --user) USER="$2"; shift 2 ;;
        --password) PASSWORD="$2"; shift 2 ;;
        --rows) ROWS="$2"; shift 2 ;;
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --count) COUNT_ONLY=true; shift ;;
        --skip-load) SKIP_LOAD=true; shift ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

export PGPASSWORD="${PASSWORD}"
PG_ARGS=(-w -h "${HOST}" -p "${PORT}" -U "${USER}")

psql_cmd() {
    psql "${PG_ARGS[@]}" "$@"
}

if [[ "${SKIP_LOAD}" == "false" ]]; then
    EXISTING=$(psql_cmd -t -A -c "SELECT count(*) FROM scanbench;" 2>/dev/null || echo "0")
    EXISTING=$(echo "${EXISTING}" | tr -d '[:space:]')

    if [[ "${EXISTING}" == "${ROWS}" ]]; then
        echo "Table scanbench already has ${ROWS} rows, skipping load."
    else
        echo "Loading ${ROWS} rows..."
        psql_cmd -c "DROP TABLE IF EXISTS scanbench;" >/dev/null 2>&1 || true
        psql_cmd -c "CREATE TABLE scanbench (id int NOT NULL, payload text NOT NULL);" >/dev/null

        if psql_cmd -c "DO \$\$ BEGIN INSERT INTO scanbench VALUES (0, 'probe'); DELETE FROM scanbench WHERE id = 0; END \$\$;" >/dev/null 2>&1; then
            psql_cmd -q <<SQL
DO \$\$
BEGIN
    FOR i IN 0..${ROWS}-1 LOOP
        INSERT INTO scanbench (id, payload) VALUES (i, 'row-' || i);
    END LOOP;
END
\$\$;
SQL
        else
            SQL_FILE=$(mktemp)
            trap "rm -f ${SQL_FILE}" EXIT
            echo "BEGIN;" > "${SQL_FILE}"
            python3 -c "
for i in range(${ROWS}):
    print(f\"INSERT INTO scanbench (id, payload) VALUES ({i}, 'row-{i}');\")
" >> "${SQL_FILE}"
            echo "COMMIT;" >> "${SQL_FILE}"
            psql_cmd -q -f "${SQL_FILE}"
        fi
        echo "Load complete."
    fi
fi

if [[ "${COUNT_ONLY}" == "true" ]]; then
    QUERY="SELECT count(*) FROM scanbench"
else
    QUERY="SELECT * FROM scanbench"
fi

# Build query file: N iterations of EXPLAIN ANALYZE
QUERY_FILE=$(mktemp)
trap "rm -f ${QUERY_FILE}" EXIT
for ((i=0; i<ITERATIONS; i++)); do
    echo "EXPLAIN ANALYZE ${QUERY};" >> "${QUERY_FILE}"
done

echo "Running EXPLAIN ANALYZE benchmark (${ROWS} rows, ${ITERATIONS} iterations, port ${PORT})..."

# Run all EXPLAIN ANALYZE queries in a single connection and capture output.
OUTPUT=$(psql_cmd -t -A -f "${QUERY_FILE}" 2>&1)

# Parse execution times from output.
python3 -c "
import re, sys

lines = '''${OUTPUT}'''.strip().split('\n')
times = []
for line in lines:
    # pgrust format: 'Execution Time: 1.234 ms'
    # postgres format: 'Execution Time: 1.234 ms'
    m = re.search(r'Execution Time:\s*([\d.]+)\s*ms', line)
    if m:
        times.append(float(m.group(1)))

if not times:
    print('No Execution Time found in output', file=sys.stderr)
    print('Output:', file=sys.stderr)
    for line in lines[:20]:
        print(f'  {line}', file=sys.stderr)
    sys.exit(1)

print(f'port: ${PORT}')
print(f'query: EXPLAIN ANALYZE ${QUERY}')
print(f'rows: ${ROWS}')
print(f'iterations: {len(times)}')
print(f'min_ms: {min(times):.3f}')
print(f'max_ms: {max(times):.3f}')
print(f'avg_ms: {sum(times)/len(times):.3f}')
print(f'median_ms: {sorted(times)[len(times)//2]:.3f}')
print(f'p99_ms: {sorted(times)[int(len(times)*0.99)]:.3f}')
print()
print('=== Per-iteration execution time ===')
for i, t in enumerate(times):
    print(f'  iter {i+1:4d}: {t:.3f} ms')
"
