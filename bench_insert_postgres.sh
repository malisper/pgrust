#!/bin/bash
# Benchmark INSERT performance against a real PostgreSQL instance.
# Usage: ./bench_insert_postgres.sh [rows] [conninfo]
#
# Requires: psql, python3, a running PostgreSQL server.
# The script creates a temporary database, runs the benchmark, and cleans up.
set -euo pipefail

ROWS="${1:-2000000}"
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
}
trap cleanup EXIT

# Create a fresh database.
psql_cmd -c "DROP DATABASE IF EXISTS ${BENCH_DB};"
psql_cmd -c "CREATE DATABASE ${BENCH_DB};"

# Create the table (matches bench_insert.rs schema).
psql_bench -c "CREATE TABLE insertbench (id int NOT NULL, payload text NOT NULL);"

echo "Running benchmark (${ROWS} rows via PL/pgSQL)..."

START_NS=$(python3 -c 'import time; print(int(time.time_ns()))')

psql_bench -q <<SQL
DO \$\$
BEGIN
    FOR i IN 0..${ROWS}-1 LOOP
        INSERT INTO insertbench (id, payload) VALUES (i, 'row-' || i);
    END LOOP;
END
\$\$;
SQL

END_NS=$(python3 -c 'import time; print(int(time.time_ns()))')

ELAPSED_MS=$(python3 -c "print(f'{(${END_NS} - ${START_NS}) / 1e6:.3f}')")
AVG_MS=$(python3 -c "print(f'{(${END_NS} - ${START_NS}) / 1e6 / ${ROWS}:.3f}')")
IPS=$(python3 -c "print(f'{${ROWS} / ((${END_NS} - ${START_NS}) / 1e9):.0f}')")

echo ""
echo "engine: postgresql"
echo "rows: ${ROWS}"
echo "total_ms: ${ELAPSED_MS}"
echo "avg_ms_per_insert: ${AVG_MS}"
echo "inserts_per_sec: ${IPS}"
