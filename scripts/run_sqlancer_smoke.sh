#!/usr/bin/env bash
# Run a short SQLancer smoke test against a local pgrust server.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SQLANCER_DIR="${SQLANCER_DIR:-/Users/jasonseibel/dev/2026/your-projects-parent/sqlancer}"
PORT="${PGRUST_SQLANCER_PORT:-5433}"
DATA_DIR="${PGRUST_SQLANCER_DATA_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/pgrust-sqlancer-data.XXXXXX")}"
LOG_FILE="${PGRUST_SQLANCER_LOG_FILE:-$(mktemp "${TMPDIR:-/tmp}/pgrust-sqlancer-server.XXXXXX")}"
JAVA_CMD="${JAVA_CMD:-}"
NUM_THREADS="${PGRUST_SQLANCER_THREADS:-1}"
NUM_TRIES="${PGRUST_SQLANCER_TRIES:-1}"
NUM_QUERIES="${PGRUST_SQLANCER_QUERIES:-25}"
MAX_DATABASES="${PGRUST_SQLANCER_MAX_DATABASES:-1}"
TIMEOUT_SECONDS="${PGRUST_SQLANCER_TIMEOUT_SECONDS:-30}"
RANDOM_SEED="${PGRUST_SQLANCER_SEED:-1}"

if [[ -z "$JAVA_CMD" ]] && command -v brew >/dev/null 2>&1; then
    OPENJDK_PREFIX="$(brew --prefix openjdk 2>/dev/null || true)"
    if [[ -n "$OPENJDK_PREFIX" && -x "$OPENJDK_PREFIX/bin/java" ]]; then
        JAVA_CMD="$OPENJDK_PREFIX/bin/java"
    fi
fi

if [[ -z "$JAVA_CMD" ]]; then
    JAVA_CMD="$(command -v java || true)"
fi

if [[ -z "$JAVA_CMD" ]]; then
    echo "missing java; install OpenJDK or set JAVA_CMD" >&2
    exit 1
fi

if [[ ! -d "$SQLANCER_DIR" ]]; then
    echo "missing SQLancer checkout: $SQLANCER_DIR" >&2
    exit 1
fi

SQLANCER_JAR="$(find "$SQLANCER_DIR/target" -maxdepth 1 -name 'sqlancer-*.jar' 2>/dev/null | head -1 || true)"
if [[ -z "$SQLANCER_JAR" ]]; then
    if ! command -v mvn >/dev/null 2>&1; then
        echo "missing SQLancer jar and mvn is not on PATH" >&2
        echo "build SQLancer first, or install Maven and rerun this script" >&2
        exit 1
    fi
    (cd "$SQLANCER_DIR" && mvn -DskipTests package)
    SQLANCER_JAR="$(find "$SQLANCER_DIR/target" -maxdepth 1 -name 'sqlancer-*.jar' | head -1 || true)"
fi

cargo run --quiet --bin pgrust_server -- --dir "$DATA_DIR" --port "$PORT" >"$LOG_FILE" 2>&1 &
SERVER_PID="$!"

cleanup() {
    kill "$SERVER_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in {1..600}; do
    if grep -q "pgrust: listening on" "$LOG_FILE"; then
        break
    fi
    if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
        echo "pgrust server exited before listening; log: $LOG_FILE" >&2
        exit 1
    fi
    sleep 0.25
done

if ! grep -q "pgrust: listening on" "$LOG_FILE"; then
    echo "pgrust server did not start; log: $LOG_FILE" >&2
    exit 1
fi

"$JAVA_CMD" -jar "$SQLANCER_JAR" \
    --num-threads "$NUM_THREADS" \
    --num-tries "$NUM_TRIES" \
    --num-queries "$NUM_QUERIES" \
    --max-generated-databases "$MAX_DATABASES" \
    --timeout-seconds "$TIMEOUT_SECONDS" \
    --random-seed "$RANDOM_SEED" \
    pgrust \
    --oracle WHERE \
    --connection-url "jdbc:postgresql://127.0.0.1:$PORT/postgres"
