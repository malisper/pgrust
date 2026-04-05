#!/bin/bash
# Benchmark SELECT * throughput over the wire against PostgreSQL or pgrust.
# Usage: bench/bench_select_wire.sh [--port PORT] [--rows ROWS] [--iterations ITERS] [--clients CLIENTS] [--skip-load]
#
# Requires: psql, python3
# The script creates and loads the table unless --skip-load is passed.
set -euo pipefail

PORT=5432
HOST="${PGHOST:-127.0.0.1}"
USER="${PGUSER:-postgres}"
PASSWORD="${PGPASSWORD:-postgres}"
ROWS=10000
ITERATIONS=10
CLIENTS=1
SKIP_LOAD=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        --host) HOST="$2"; shift 2 ;;
        --user) USER="$2"; shift 2 ;;
        --password) PASSWORD="$2"; shift 2 ;;
        --rows) ROWS="$2"; shift 2 ;;
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --clients) CLIENTS="$2"; shift 2 ;;
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
    # Check if table already has the right number of rows.
    EXISTING=$(psql_cmd -t -A -c "SELECT count(*) FROM scanbench;" 2>/dev/null || echo "0")
    EXISTING=$(echo "${EXISTING}" | tr -d '[:space:]')

    if [[ "${EXISTING}" == "${ROWS}" ]]; then
        echo "Table scanbench already has ${ROWS} rows, skipping load."
    else
        echo "Loading ${ROWS} rows..."
        psql_cmd -c "DROP TABLE IF EXISTS scanbench;" >/dev/null 2>&1 || true
        psql_cmd -c "CREATE TABLE scanbench (id int NOT NULL, payload text NOT NULL);" >/dev/null

        # Bulk load via PL/pgSQL (or plain inserts for pgrust which lacks DO blocks).
        # Try DO block first; fall back to individual inserts.
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
            # pgrust: use plain inserts in a transaction
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

# Build the query file: N iterations of SELECT * FROM scanbench
QUERY_FILE=$(mktemp)
trap "rm -f ${QUERY_FILE}" EXIT
for ((i=0; i<ITERATIONS; i++)); do
    echo "SELECT * FROM scanbench;" >> "${QUERY_FILE}"
done

TOTAL_ITERATIONS=$((ITERATIONS * CLIENTS))
echo "Running benchmark (${ROWS} rows, ${ITERATIONS} iterations x ${CLIENTS} clients = ${TOTAL_ITERATIONS} total queries, port ${PORT})..."

# Launch clients in parallel
START_NS=$(python3 -c 'import time; print(int(time.time_ns()))')

COMPLETED=0
pids=()
for ((c=0; c<CLIENTS; c++)); do
    psql_cmd -q -t -A -f "${QUERY_FILE}" >/dev/null &
    pids+=($!)
done

# Wait for all clients, reporting as each finishes
for pid in "${pids[@]}"; do
    wait "$pid"
    COMPLETED=$((COMPLETED + ITERATIONS))
    echo -ne "\r  completed: ${COMPLETED} / ${TOTAL_ITERATIONS}" >&2
done
echo "" >&2

END_NS=$(python3 -c 'import time; print(int(time.time_ns()))')

TOTAL_QUERIES=${TOTAL_ITERATIONS}
TOTAL_ROWS=$((ROWS * TOTAL_QUERIES))

python3 -c "
total_queries = ${TOTAL_QUERIES}
total_rows = ${TOTAL_ROWS}
rows_per_table = ${ROWS}
clients = ${CLIENTS}
iterations = ${ITERATIONS}
elapsed_ns = ${END_NS} - ${START_NS}
elapsed_s = elapsed_ns / 1e9
print(f'port: ${PORT}')
print(f'rows_per_table: {rows_per_table}')
print(f'iterations: {iterations}')
print(f'clients: {clients}')
print(f'total_queries: {total_queries}')
print(f'total_rows: {total_rows}')
print(f'total_ms: {elapsed_ns / 1e6:.3f}')
print(f'queries_per_sec: {total_queries / elapsed_s:.1f}')
print(f'rows_per_sec: {total_rows / elapsed_s:.0f}')
print(f'avg_ms_per_query: {elapsed_ns / 1e6 / total_queries:.3f}')
"
