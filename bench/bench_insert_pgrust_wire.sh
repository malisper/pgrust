#!/bin/bash
# Benchmark INSERT performance against a running pgrust server over the wire.
# Usage: ./bench/bench_insert_pgrust_wire.sh [rows] [port]
#
# Requires: psql, python3, a running pgrust_server.
# The table 'insertbench' must already exist. It will be truncated before the run.
set -euo pipefail

ROWS="${1:-10000}"
PORT="${2:-5444}"
HOST="${PGHOST:-127.0.0.1}"

export PGPASSWORD="x"
PG_ARGS=(-w -h "${HOST}" -p "${PORT}" -U postgres)

psql "${PG_ARGS[@]}" -c "TRUNCATE insertbench;" >/dev/null 2>&1 || true

SQL_FILE=$(mktemp)
trap "rm -f ${SQL_FILE}" EXIT

python3 -c "
for i in range(${ROWS}):
    print(f\"INSERT INTO insertbench (id, payload) VALUES ({i}, 'row-{i}');\")
" > "${SQL_FILE}"

echo "Running benchmark (${ROWS} rows, autocommit over wire to pgrust:${PORT})..."

START_NS=$(python3 -c 'import time; print(int(time.time_ns()))')

psql "${PG_ARGS[@]}" -q -f "${SQL_FILE}" >/dev/null

END_NS=$(python3 -c 'import time; print(int(time.time_ns()))')

python3 -c "
rows=${ROWS}
elapsed_ns = ${END_NS} - ${START_NS}
print(f'engine: pgrust (wire protocol)')
print(f'rows: {rows}')
print(f'total_ms: {elapsed_ns / 1e6:.3f}')
print(f'avg_ms_per_insert: {elapsed_ns / 1e6 / rows:.3f}')
print(f'inserts_per_sec: {rows / (elapsed_ns / 1e9):.0f}')
"
