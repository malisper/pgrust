#!/usr/bin/env bash
# Run sqllogictest corpora against pgrust over the PostgreSQL wire protocol.
#
# Usage:
#   scripts/run_sqllogictest.sh --files './path/to/**/*.slt'
#   scripts/run_sqllogictest.sh --suite-dir ../sqllogictest-rs/tests/slt
#
# By default, this script:
#   1. Builds pgrust_server in release mode
#   2. Starts it on a fresh data directory
#   3. Locates sqllogictest either on PATH or via a local sqllogictest-rs checkout
#   4. Runs the requested .slt files against pgrust
#   5. Writes logs and optional JUnit output to a temp results directory

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PGRUST_DIR/.." && pwd)"
WORKTREE_NAME="$(basename "$PGRUST_DIR")"

PORT=5435
HOST="127.0.0.1"
DB_NAME="${PGRUST_SQLLOGICTEST_DB:-postgres}"
DB_USER="${PGRUST_SQLLOGICTEST_USER:-postgres}"
DB_PASSWORD="${PGRUST_SQLLOGICTEST_PASSWORD:-x}"
ENGINE="${PGRUST_SQLLOGICTEST_ENGINE:-postgres}"
OPTIONS="${PGRUST_SQLLOGICTEST_OPTIONS:-}"
SKIP_BUILD=false
SKIP_SERVER=false
RESULTS_DIR=""
DATA_DIR=""
SERVER_PID=""
SERVER_BIN=""
STARTUP_WAIT_SECS="${PGRUST_STARTUP_WAIT_SECS:-300}"
SQLLOGICTEST_BIN="${PGRUST_SQLLOGICTEST_BIN:-}"
SQLLOGICTEST_DIR="${PGRUST_SQLLOGICTEST_DIR:-}"
SUITE_DIR=""
JOBS=""
SKIP_REGEX=""
SHUTDOWN_TIMEOUT=""
JUNIT_NAME="sqllogictest"
KEEP_DB_ON_FAILURE=false
FAIL_FAST=false
PRESET=""
SKIP_FILE=""
OVERRIDE=false

declare -a FILE_GLOBS=()
declare -a LABELS=()

usage() {
    sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
}

list_presets() {
    cat <<'EOF'
Available presets:
  upstream-postgres-simple
    Runs sqllogictest-rs's real Postgres simple engine test.

  upstream-postgres-extended
    Runs sqllogictest-rs's Postgres extended-engine type coverage test.

  upstream-postgres-both
    Runs both upstream Postgres engine tests together.
EOF
}

make_temp_dir() {
    local prefix="$1"
    mktemp -d "${TMPDIR:-/tmp}/${prefix}.${WORKTREE_NAME}.XXXXXX"
}

resolve_sqllogictest_dir() {
    local candidate=""

    if [[ -n "$SQLLOGICTEST_DIR" ]]; then
        if [[ -f "$SQLLOGICTEST_DIR/Cargo.toml" ]]; then
            (cd "$SQLLOGICTEST_DIR" && pwd)
            return 0
        fi
        echo "ERROR: PGRUST_SQLLOGICTEST_DIR/--sqllogictest-dir does not look like sqllogictest-rs: $SQLLOGICTEST_DIR" >&2
        return 1
    fi

    for candidate in \
        "$REPO_ROOT/sqllogictest-rs" \
        "$PGRUST_DIR/../../sqllogictest-rs" \
        "$HOME/sqllogictest-rs" \
        "$HOME/src/sqllogictest-rs" \
        "$HOME/dev/sqllogictest-rs"
    do
        if [[ -f "$candidate/Cargo.toml" ]]; then
            (cd "$candidate" && pwd)
            return 0
        fi
    done

    return 1
}

append_suite_dir_default_glob() {
    if [[ -n "$SUITE_DIR" && ${#FILE_GLOBS[@]} -eq 0 ]]; then
        FILE_GLOBS+=("$SUITE_DIR/**/*.slt")
    fi
}

resolve_preset() {
    local preset="$1"
    local root=""

    if ! root="$(resolve_sqllogictest_dir)"; then
        echo "ERROR: preset '$preset' requires a sqllogictest-rs checkout." >&2
        return 1
    fi

    case "$preset" in
        upstream-postgres-simple)
            FILE_GLOBS=("$root/sqllogictest-engines/src/postgres/postgres_simple_test.slt")
            ;;
        upstream-postgres-extended)
            FILE_GLOBS=("$root/sqllogictest-engines/src/postgres/postgres_extended_test.slt")
            ;;
        upstream-postgres-both)
            FILE_GLOBS=(
                "$root/sqllogictest-engines/src/postgres/postgres_simple_test.slt"
                "$root/sqllogictest-engines/src/postgres/postgres_extended_test.slt"
            )
            ;;
        *)
            echo "ERROR: unknown preset: $preset" >&2
            return 1
            ;;
    esac

    SQLLOGICTEST_DIR="$root"
    return 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        --host) HOST="$2"; shift 2 ;;
        --db) DB_NAME="$2"; shift 2 ;;
        --user) DB_USER="$2"; shift 2 ;;
        --password|--pass) DB_PASSWORD="$2"; shift 2 ;;
        --engine) ENGINE="$2"; shift 2 ;;
        --options) OPTIONS="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --skip-server) SKIP_SERVER=true; shift ;;
        --results-dir) RESULTS_DIR="$2"; shift 2 ;;
        --data-dir) DATA_DIR="$2"; shift 2 ;;
        --files|--glob) FILE_GLOBS+=("$2"); shift 2 ;;
        --suite-dir) SUITE_DIR="$2"; shift 2 ;;
        --sqllogictest-bin) SQLLOGICTEST_BIN="$2"; shift 2 ;;
        --sqllogictest-dir) SQLLOGICTEST_DIR="$2"; shift 2 ;;
        --jobs) JOBS="$2"; shift 2 ;;
        --skip) SKIP_REGEX="$2"; shift 2 ;;
        --shutdown-timeout) SHUTDOWN_TIMEOUT="$2"; shift 2 ;;
        --junit-name) JUNIT_NAME="$2"; shift 2 ;;
        --label) LABELS+=("$2"); shift 2 ;;
        --preset) PRESET="$2"; shift 2 ;;
        --skip-file) SKIP_FILE="$2"; shift 2 ;;
        --override) OVERRIDE=true; shift ;;
        --list-presets) list_presets; exit 0 ;;
        --keep-db-on-failure) KEEP_DB_ON_FAILURE=true; shift ;;
        --fail-fast) FAIL_FAST=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown flag: $1" >&2; usage >&2; exit 1 ;;
    esac
done

if [[ -n "$PRESET" ]]; then
    if [[ -n "$SUITE_DIR" || ${#FILE_GLOBS[@]} -gt 0 ]]; then
        echo "ERROR: --preset cannot be combined with --suite-dir/--files" >&2
        exit 1
    fi
    resolve_preset "$PRESET"
fi

append_suite_dir_default_glob

if [[ ${#FILE_GLOBS[@]} -eq 0 ]]; then
    cat >&2 <<EOF
ERROR: no sqllogictest files were provided.

Pass either:
  --files './path/to/**/*.slt'
or:
  --suite-dir /path/to/slt-root
EOF
    exit 1
fi

if [[ -z "$RESULTS_DIR" ]]; then
    RESULTS_DIR="$(make_temp_dir pgrust_sqllogictest_results)"
fi

if [[ -z "$DATA_DIR" ]]; then
    DATA_DIR="$(make_temp_dir pgrust_sqllogictest_data)"
fi

mkdir -p "$RESULTS_DIR"

if [[ -n "$SQLLOGICTEST_BIN" ]]; then
    if [[ ! -x "$SQLLOGICTEST_BIN" ]]; then
        echo "ERROR: --sqllogictest-bin is not executable: $SQLLOGICTEST_BIN" >&2
        exit 1
    fi
    SQLLOGICTEST_CMD=("$SQLLOGICTEST_BIN")
elif command -v sqllogictest >/dev/null 2>&1; then
    SQLLOGICTEST_CMD=("$(command -v sqllogictest)")
else
    if ! SQLLOGICTEST_DIR="$(resolve_sqllogictest_dir)"; then
        cat >&2 <<EOF
ERROR: could not find sqllogictest.

Install it with:
  cargo install sqllogictest-bin

or clone the source and point this script at it:
  git clone https://github.com/risinglightdb/sqllogictest-rs ../sqllogictest-rs
  scripts/run_sqllogictest.sh --sqllogictest-dir ../sqllogictest-rs --files '...'
EOF
        exit 1
    fi
    SQLLOGICTEST_CMD=(
        cargo run -q
        --manifest-path "$SQLLOGICTEST_DIR/Cargo.toml"
        -p sqllogictest-bin --
    )
fi

stop_server() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Stopping pgrust server (PID $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}

cleanup() {
    stop_server
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

port_is_listening() {
    lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

wait_for_server_ready() {
    local pid="$1"
    local attempts=$((STARTUP_WAIT_SECS * 2))

    echo "Waiting for server to accept connections..."
    for _ in $(seq 1 "$attempts"); do
        if PGPASSWORD="$DB_PASSWORD" psql -X -h "$HOST" -p "$PORT" -U "$DB_USER" "$DB_NAME" -c "SELECT 1" >/dev/null 2>&1; then
            echo "Server ready."
            return 0
        fi
        if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
            return 1
        fi
        sleep 0.5
    done

    return 1
}

write_server_config() {
    cat > "$DATA_DIR/postgresql.conf" <<'EOF'
fsync = off
EOF
}

start_server() {
    if port_is_listening "$PORT"; then
        echo "ERROR: port $PORT is already in use" >&2
        lsof -nP -iTCP:"$PORT" -sTCP:LISTEN || true
        return 1
    fi

    echo "Starting pgrust server on port $PORT (data: $DATA_DIR)..."
    "$SERVER_BIN" "$DATA_DIR" "$PORT" &
    SERVER_PID=$!

    wait_for_server_ready "$SERVER_PID"
}

if [[ "$SKIP_BUILD" == false ]]; then
    echo "Building pgrust_server (release)..."
    (cd "$PGRUST_DIR" && cargo build --release --bin pgrust_server 2>&1) || {
        echo "ERROR: pgrust_server build failed" >&2
        exit 1
    }
fi

TARGET_DIR="$("$PGRUST_DIR/scripts/cargo_target_dir.sh")"
SERVER_BIN="$TARGET_DIR/release/pgrust_server"
if [[ "$SKIP_SERVER" == false && ! -x "$SERVER_BIN" ]]; then
    echo "ERROR: $SERVER_BIN not found. Run without --skip-build." >&2
    exit 1
fi

echo "sqllogictest results dir: $RESULTS_DIR"
echo "sqllogictest data dir: $DATA_DIR"
echo "sqllogictest db target: postgres://$DB_USER@$HOST:$PORT/$DB_NAME"

if [[ "$SKIP_SERVER" == false ]]; then
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    write_server_config

    if ! start_server; then
        echo "ERROR: pgrust server did not become ready" >&2
        exit 1
    fi
fi

RUN_ARGS=(
    --engine "$ENGINE"
    --host "$HOST"
    --port "$PORT"
    --db "$DB_NAME"
    --user "$DB_USER"
    --pass "$DB_PASSWORD"
)

if [[ -n "$OPTIONS" ]]; then
    RUN_ARGS+=(--options "$OPTIONS")
fi

if [[ -n "$JOBS" ]]; then
    RUN_ARGS+=(--jobs "$JOBS")
fi

if [[ "$KEEP_DB_ON_FAILURE" == true ]]; then
    RUN_ARGS+=(--keep-db-on-failure)
fi

if [[ "$FAIL_FAST" == true ]]; then
    RUN_ARGS+=(--fail-fast)
fi

if [[ "$OVERRIDE" == true ]]; then
    RUN_ARGS+=(--override)
fi

if [[ -n "$SKIP_REGEX" ]]; then
    RUN_ARGS+=(--skip "$SKIP_REGEX")
fi

if [[ -n "$SKIP_FILE" ]]; then
    if [[ ! -f "$SKIP_FILE" ]]; then
        echo "ERROR: --skip-file not found: $SKIP_FILE" >&2
        exit 1
    fi
    while IFS= read -r skip_entry || [[ -n "$skip_entry" ]]; do
        if [[ -z "$skip_entry" || "$skip_entry" =~ ^[[:space:]]*# ]]; then
            continue
        fi
        RUN_ARGS+=(--skip "$skip_entry")
    done < "$SKIP_FILE"
fi

if [[ -n "$SHUTDOWN_TIMEOUT" ]]; then
    RUN_ARGS+=(--shutdown-timeout "$SHUTDOWN_TIMEOUT")
fi

if [[ -n "$JUNIT_NAME" ]]; then
    mkdir -p "$RESULTS_DIR/junit"
    RUN_ARGS+=(--junit "$RESULTS_DIR/junit/$JUNIT_NAME")
fi

if [[ ${#LABELS[@]} -gt 0 ]]; then
    for label in "${LABELS[@]}"; do
        RUN_ARGS+=(--label "$label")
    done
fi

for pattern in "${FILE_GLOBS[@]}"; do
    RUN_ARGS+=("$pattern")
done

LOG_FILE="$RESULTS_DIR/sqllogictest.log"

echo "Running sqllogictest..."
echo "  command: ${SQLLOGICTEST_CMD[*]} ${RUN_ARGS[*]}"

set +e
"${SQLLOGICTEST_CMD[@]}" "${RUN_ARGS[@]}" 2>&1 | tee "$LOG_FILE"
STATUS=${PIPESTATUS[0]}
set -e

if [[ -n "$JUNIT_NAME" ]]; then
    echo "JUnit: $RESULTS_DIR/junit/${JUNIT_NAME}-junit.xml"
fi
echo "Log: $LOG_FILE"

exit "$STATUS"
