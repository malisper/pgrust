#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

PORT=""
DATA_DIR="/tmp/pgrust-profile"
PROFILE_OUT="/tmp/pgrust_select_500.dtrace.txt"
DTRACE_ERR=""
ROWS=100
SELECT_COUNT=500
USTACKFRAMES=100
PROFILE_HZ=997
SKIP_BUILD=false
SERVER_PID=""
DTRACE_PID=""

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --port PORT               Server port (default: auto-pick a free port)
  --data-dir DIR            Data directory for the temporary server (default: $DATA_DIR)
  --profile-out FILE        Where to write dtrace output (default: $PROFILE_OUT)
  --rows N                  Rows to insert into bench_select (default: $ROWS)
  --select-count N          Number of SELECT * statements to run (default: $SELECT_COUNT)
  --ustackframes N          dtrace ustackframes setting (default: $USTACKFRAMES)
  --profile-hz N            dtrace profile frequency (default: $PROFILE_HZ)
  --skip-build              Do not rebuild target/release/pgrust_server
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        --data-dir) DATA_DIR="$2"; shift 2 ;;
        --profile-out) PROFILE_OUT="$2"; shift 2 ;;
        --rows) ROWS="$2"; shift 2 ;;
        --select-count) SELECT_COUNT="$2"; shift 2 ;;
        --ustackframes) USTACKFRAMES="$2"; shift 2 ;;
        --profile-hz) PROFILE_HZ="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown flag: $1" >&2; usage; exit 1 ;;
    esac
done

pick_free_port() {
    python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

cleanup() {
    if [[ -n "$DTRACE_PID" ]] && kill -0 "$DTRACE_PID" 2>/dev/null; then
        kill -INT "$DTRACE_PID" 2>/dev/null || true
        wait "$DTRACE_PID" 2>/dev/null || true
    fi

    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

if [[ "$SKIP_BUILD" == false ]]; then
    (cd "$PGRUST_DIR" && cargo build --release --bin pgrust_server >/dev/null)
fi

SERVER_BIN="$PGRUST_DIR/target/release/pgrust_server"
if [[ ! -x "$SERVER_BIN" ]]; then
    echo "missing server binary: $SERVER_BIN" >&2
    exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
    echo "psql is required" >&2
    exit 1
fi

if ! command -v dtrace >/dev/null 2>&1; then
    echo "dtrace is required" >&2
    exit 1
fi

rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

if [[ -z "$PORT" ]]; then
    PORT="$(pick_free_port)"
fi

"$SERVER_BIN" "$DATA_DIR" "$PORT" &
SERVER_PID=$!

for _ in $(seq 1 60); do
    if psql -X -h 127.0.0.1 -p "$PORT" -U postgres -Atqc "select 1" >/dev/null 2>&1; then
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "server exited unexpectedly" >&2
        exit 1
    fi
    sleep 0.2
done

if ! psql -X -h 127.0.0.1 -p "$PORT" -U postgres -Atqc "select 1" >/dev/null 2>&1; then
    echo "server did not become ready" >&2
    exit 1
fi

SEED_SQL="$(mktemp /tmp/pgrust-seed-XXXX.sql)"
WORKLOAD_SQL="$(mktemp /tmp/pgrust-workload-XXXX.sql)"
DTRACE_ERR="$(mktemp /tmp/pgrust-dtrace-XXXX.log)"
trap 'rm -f "$SEED_SQL" "$WORKLOAD_SQL" "$DTRACE_ERR"; cleanup' EXIT

cat > "$SEED_SQL" <<EOF
DROP TABLE IF EXISTS bench_select;
CREATE TABLE bench_select (id int4, payload text);
INSERT INTO bench_select
SELECT i, 'row-' || i::text
FROM generate_series(1, $ROWS) AS s(i);
EOF

psql -X -h 127.0.0.1 -p "$PORT" -U postgres -v ON_ERROR_STOP=1 -q -f "$SEED_SQL" postgres >/dev/null

{
    for _ in $(seq 1 "$SELECT_COUNT"); do
        echo "SELECT * FROM bench_select;"
    done
} > "$WORKLOAD_SQL"

rm -f "$PROFILE_OUT"

if ! sudo -n true 2>/dev/null; then
    echo "Acquiring sudo for dtrace..."
    sudo -v
fi

sudo -n dtrace -q -x "ustackframes=$USTACKFRAMES" -n "
profile-$PROFILE_HZ /pid == \$target/ { @[ustack()] = count(); }
" -p "$SERVER_PID" > "$PROFILE_OUT" 2>"$DTRACE_ERR" &
DTRACE_PID=$!

sleep 0.5

if ! kill -0 "$DTRACE_PID" 2>/dev/null; then
    echo "dtrace exited before workload started" >&2
    if [[ -s "$DTRACE_ERR" ]]; then
        cat "$DTRACE_ERR" >&2
    fi
    exit 1
fi

/usr/bin/time -p \
    psql -X -h 127.0.0.1 -p "$PORT" -U postgres -v ON_ERROR_STOP=1 -q -f "$WORKLOAD_SQL" postgres >/dev/null

kill -INT "$DTRACE_PID" 2>/dev/null || true
wait "$DTRACE_PID" 2>/dev/null || true
DTRACE_PID=""

if [[ ! -s "$PROFILE_OUT" ]]; then
    echo "dtrace captured no samples" >&2
    if [[ -s "$DTRACE_ERR" ]]; then
        cat "$DTRACE_ERR" >&2
    fi
    exit 1
fi

echo "profile written to $PROFILE_OUT"
echo "server port was $PORT"
