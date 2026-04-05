#!/bin/bash
# Benchmark INSERT performance against PostgreSQL with autocommit (one txn per insert).
# Usage: ./bench_insert_postgres_autocommit.sh [rows]
set -euo pipefail

ROWS="${1:-100000}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"

BENCH_DB="pgrust_insert_bench_$$"
PG_ARGS=(-h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}")

psql_cmd() {
    psql "${PG_ARGS[@]}" "$@"
}

psql_bench() {
    psql "${PG_ARGS[@]}" -d "${BENCH_DB}" "$@"
}

cleanup() {
    psql_cmd -c "DROP DATABASE IF EXISTS ${BENCH_DB};" 2>/dev/null || true
    rm -f "${SQL_FILE}"
}
trap cleanup EXIT

psql_cmd -c "DROP DATABASE IF EXISTS ${BENCH_DB};"
psql_cmd -c "CREATE DATABASE ${BENCH_DB};"
psql_bench -c "CREATE TABLE insertbench (id int NOT NULL, payload text NOT NULL);"

# Generate SQL file with one INSERT per line (each is its own transaction in autocommit).
SQL_FILE=$(mktemp)
python3 -c "
for i in range(${ROWS}):
    print(f\"INSERT INTO insertbench (id, payload) VALUES ({i}, 'row-{i}');\")
" > "${SQL_FILE}"

echo "Running benchmark (${ROWS} rows, autocommit — one txn per INSERT)..."

START_NS=$(python3 -c 'import time; print(int(time.time_ns()))')

psql_bench -q -f "${SQL_FILE}"

END_NS=$(python3 -c 'import time; print(int(time.time_ns()))')

ELAPSED_MS=$(python3 -c "print(f'{(${END_NS} - ${START_NS}) / 1e6:.3f}')")
AVG_MS=$(python3 -c "print(f'{(${END_NS} - ${START_NS}) / 1e6 / ${ROWS}:.3f}')")
IPS=$(python3 -c "print(f'{${ROWS} / ((${END_NS} - ${START_NS}) / 1e9):.0f}')")

echo ""
echo "engine: postgresql (autocommit)"
echo "rows: ${ROWS}"
echo "total_ms: ${ELAPSED_MS}"
echo "avg_ms_per_insert: ${AVG_MS}"
echo "inserts_per_sec: ${IPS}"
