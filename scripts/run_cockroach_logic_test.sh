#!/usr/bin/env bash
# Convert a CockroachDB logictest file into sqllogictest format and run it
# against pgrust.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PGRUST_DIR/.." && pwd)"

COCKROACH_DIR="${PGRUST_COCKROACH_DIR:-}"
SQLLOGICTEST_DIR="${PGRUST_SQLLOGICTEST_DIR:-}"
TEST_NAME=""
FILE_PATH=""
PORT=5440
SKIP_BUILD=false
SKIP_SERVER=false
RESULTS_DIR=""
DATA_DIR=""
KEEP_CONVERTED=false
POSTGRES_ORACLE=false
POSTGRES_PORT="${PGRUST_POSTGRES_ORACLE_PORT:-55432}"
POSTGRES_PID=""

usage() {
    cat <<'EOF'
Usage:
  scripts/run_cockroach_logic_test.sh --test float
  scripts/run_cockroach_logic_test.sh --file /path/to/cockroach/pkg/sql/logictest/testdata/logic_test/float
  scripts/run_cockroach_logic_test.sh --test float --postgres-oracle
EOF
}

resolve_cockroach_dir() {
    local candidate=""

    if [[ -n "$COCKROACH_DIR" ]]; then
        if [[ -d "$COCKROACH_DIR/pkg/sql/logictest/testdata/logic_test" ]]; then
            (cd "$COCKROACH_DIR" && pwd)
            return 0
        fi
        echo "ERROR: --cockroach-dir/PGRUST_COCKROACH_DIR is invalid: $COCKROACH_DIR" >&2
        return 1
    fi

    for candidate in \
        "$REPO_ROOT/cockroach" \
        "$PGRUST_DIR/../../cockroach" \
        "$HOME/cockroach" \
        "$HOME/src/cockroach" \
        "$HOME/dev/cockroach"
    do
        if [[ -d "$candidate/pkg/sql/logictest/testdata/logic_test" ]]; then
            (cd "$candidate" && pwd)
            return 0
        fi
    done

    return 1
}

resolve_sqllogictest_dir() {
    local candidate=""

    if [[ -n "$SQLLOGICTEST_DIR" ]]; then
        if [[ -f "$SQLLOGICTEST_DIR/Cargo.toml" ]]; then
            (cd "$SQLLOGICTEST_DIR" && pwd)
            return 0
        fi
        echo "ERROR: --sqllogictest-dir/PGRUST_SQLLOGICTEST_DIR is invalid: $SQLLOGICTEST_DIR" >&2
        return 1
    fi

    for candidate in \
        "$REPO_ROOT/sqllogictest-rs" \
        "$PGRUST_DIR/../../sqllogictest-rs" \
        "$HOME/sqllogictest-rs" \
        "$HOME/src/sqllogictest-rs" \
        "$HOME/dev/sqllogictest-rs" \
        "/tmp/sqllogictest-rs"
    do
        if [[ -f "$candidate/Cargo.toml" ]]; then
            (cd "$candidate" && pwd)
            return 0
        fi
    done

    return 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cockroach-dir) COCKROACH_DIR="$2"; shift 2 ;;
        --sqllogictest-dir) SQLLOGICTEST_DIR="$2"; shift 2 ;;
        --test) TEST_NAME="$2"; shift 2 ;;
        --file) FILE_PATH="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --skip-server) SKIP_SERVER=true; shift ;;
        --results-dir) RESULTS_DIR="$2"; shift 2 ;;
        --data-dir) DATA_DIR="$2"; shift 2 ;;
        --keep-converted) KEEP_CONVERTED=true; shift ;;
        --postgres-oracle) POSTGRES_ORACLE=true; shift ;;
        --postgres-port) POSTGRES_PORT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown flag: $1" >&2; usage >&2; exit 1 ;;
    esac
done

if [[ -n "$TEST_NAME" && -n "$FILE_PATH" ]]; then
    echo "ERROR: use either --test or --file" >&2
    exit 1
fi

if [[ -z "$TEST_NAME" && -z "$FILE_PATH" ]]; then
    echo "ERROR: one of --test or --file is required" >&2
    exit 1
fi

if [[ -n "$TEST_NAME" ]]; then
    if ! COCKROACH_DIR="$(resolve_cockroach_dir)"; then
        echo "ERROR: could not find a CockroachDB checkout" >&2
        exit 1
    fi
    FILE_PATH="$COCKROACH_DIR/pkg/sql/logictest/testdata/logic_test/$TEST_NAME"
fi

if [[ ! -f "$FILE_PATH" ]]; then
    echo "ERROR: logictest file not found: $FILE_PATH" >&2
    exit 1
fi

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pgrust_cockroach_logic.XXXXXX")"
CONVERTED_FILE="$TMP_DIR/$(basename "$FILE_PATH").slt"
POSTGRES_DATA_DIR="$TMP_DIR/postgres-data"
POSTGRES_LOG="$TMP_DIR/postgres.log"

cleanup() {
    if [[ -n "$POSTGRES_PID" ]] && kill -0 "$POSTGRES_PID" 2>/dev/null; then
        pg_ctl -D "$POSTGRES_DATA_DIR" stop >/dev/null 2>&1 || true
    fi
    if [[ "$KEEP_CONVERTED" != true ]]; then
        rm -rf "$TMP_DIR"
    fi
}
trap cleanup EXIT

echo "Converting Cockroach logictest: $FILE_PATH"
CONVERT_ARGS=()
if [[ "$POSTGRES_ORACLE" == true ]]; then
    # Cockroach error records are not safe to preserve when PostgreSQL is the
    # oracle: sqllogictest --override rewrites outputs but does not flip an
    # expected-error record to expected-success if PostgreSQL accepts it.
    CONVERT_ARGS+=(--success-only)
fi
python3 "$SCRIPT_DIR/convert_cockroach_logic_test.py" "${CONVERT_ARGS[@]}" "$FILE_PATH" "$CONVERTED_FILE"
echo "Converted file: $CONVERTED_FILE"

if ! SQLLOGICTEST_DIR="$(resolve_sqllogictest_dir)"; then
    echo "ERROR: could not find sqllogictest-rs checkout for the wrapper" >&2
    exit 1
fi

start_postgres_oracle() {
    export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
    if ! command -v initdb >/dev/null 2>&1 || ! command -v pg_ctl >/dev/null 2>&1; then
        echo "ERROR: PostgreSQL initdb/pg_ctl not found; install PostgreSQL or disable --postgres-oracle" >&2
        return 1
    fi

    if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$POSTGRES_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "ERROR: PostgreSQL oracle port $POSTGRES_PORT is already in use" >&2
        return 1
    fi

    initdb -D "$POSTGRES_DATA_DIR" -A trust >/dev/null
    pg_ctl -D "$POSTGRES_DATA_DIR" -o "-p $POSTGRES_PORT -k $TMP_DIR" -l "$POSTGRES_LOG" start >/dev/null
    POSTGRES_PID="$(head -1 "$POSTGRES_DATA_DIR/postmaster.pid")"
}

if [[ "$POSTGRES_ORACLE" == true ]]; then
    echo "Materializing expected output with PostgreSQL on port $POSTGRES_PORT..."
    start_postgres_oracle
    "$SCRIPT_DIR/run_sqllogictest.sh" \
        --skip-build \
        --skip-server \
        --sqllogictest-dir "$SQLLOGICTEST_DIR" \
        --host 127.0.0.1 \
        --port "$POSTGRES_PORT" \
        --user "$(id -un)" \
        --password "" \
        --files "$CONVERTED_FILE" \
        --override \
        --junit-name ""
fi

RUN_ARGS=(
    --port "$PORT"
    --sqllogictest-dir "$SQLLOGICTEST_DIR"
    --files "$CONVERTED_FILE"
)

if [[ "$SKIP_BUILD" == true ]]; then
    RUN_ARGS+=(--skip-build)
fi
if [[ "$SKIP_SERVER" == true ]]; then
    RUN_ARGS+=(--skip-server)
fi
if [[ -n "$RESULTS_DIR" ]]; then
    RUN_ARGS+=(--results-dir "$RESULTS_DIR")
fi
if [[ -n "$DATA_DIR" ]]; then
    RUN_ARGS+=(--data-dir "$DATA_DIR")
fi

"$SCRIPT_DIR/run_sqllogictest.sh" "${RUN_ARGS[@]}"
