#!/bin/bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "Usage: $0 <prefix_count> [port]" >&2
    exit 2
fi

PREFIX_COUNT="$1"
PORT="${2:-5580}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="/tmp/pgrust_ctl_probe_data_${PORT}"
OUT_DIR="/tmp/pgrust_ctl_probe_${PORT}"
TARGET_DIR="$("$PGRUST_DIR/scripts/cargo_target_dir.sh")"
SERVER_BIN="$TARGET_DIR/release/pgrust_server"
SPLIT_DIR="/tmp/ctl_one_by_one/tmp/create_table_like"
DRIVER_SQL="$OUT_DIR/driver.sql"

if [[ ! -x "$SERVER_BIN" ]]; then
    echo "missing server binary: $SERVER_BIN" >&2
    exit 1
fi

if [[ ! -d "$SPLIT_DIR" ]]; then
    echo "missing split statements: $SPLIT_DIR" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

"$SERVER_BIN" "$DATA_DIR" "$PORT" > "$OUT_DIR/server.log" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 120); do
    if psql -X -h 127.0.0.1 -p "$PORT" -U postgres -c "select 1" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

psql -X -h 127.0.0.1 -p "$PORT" -U postgres -v ON_ERROR_STOP=1 -q \
    < "$PGRUST_DIR/scripts/test_setup_pgrust.sql"

: > "$DRIVER_SQL"
for stmt in "$SPLIT_DIR"/*.sql; do
    name="$(basename "$stmt" .sql)"
    [[ "$name" == "driver" ]] && continue
    if (( 10#$name > PREFIX_COUNT )); then
        break
    fi
    echo "\\i $stmt" >> "$DRIVER_SQL"
done

psql -X -h 127.0.0.1 -p "$PORT" -U postgres -a -q -v abs_srcdir=../postgres/src/test/regress \
    -f "$DRIVER_SQL" > "$OUT_DIR/prefix.out" 2>&1 || true

echo "prefix_count=$PREFIX_COUNT"
echo "probe:"
psql -X -h 127.0.0.1 -p "$PORT" -U postgres -Atqc \
    "select count(*) from public.int2_tbl; select count(*) from public.int4_tbl; select count(*) from pg_namespace;"
