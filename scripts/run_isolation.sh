#!/bin/bash
# Run PostgreSQL isolation tests against pgrust and report pass/fail statistics.
#
# Usage: scripts/run_isolation.sh [--port PORT] [--skip-build] [--skip-server]
#                                 [--timeout SECS] [--test TESTNAME]
#                                 [--schedule FILE]
#
# By default, this script:
#   1. Builds/locates isolationtester (see scripts/build_pg_isolation_tools.sh)
#   2. Builds pgrust_server in release mode
#   3. Starts it on a fresh data directory (port 5434 by default, distinct from
#      run_regression.sh's 5433 so the two suites can coexist)
#   4. Loops over upstream's isolation_schedule, invoking isolationtester per
#      spec file and diffing each result against the expected .out
#   5. Reports pass / fail / skip statistics
#
# Options:
#   --port PORT       Port for pgrust server (default: 5434)
#   --skip-build      Don't rebuild pgrust_server (still builds isolationtester)
#   --skip-server     Assume server is already running (don't start/stop it)
#   --timeout SECS    Per-step wait timeout in seconds; passed via
#                     PG_TEST_TIMEOUT_DEFAULT (default: 60)
#   --test TESTNAME   Run only this spec (without .spec extension)
#   --schedule FILE   Schedule file to read test names from (default:
#                     $POSTGRES_DIR/src/test/isolation/isolation_schedule)
#   --results-dir DIR Directory for results (default: unique temp dir)
#   --data-dir DIR    Directory for the pgrust cluster (default: unique temp dir)
#
# Environment:
#   PGRUST_POSTGRES_DIR         Override postgres source discovery
#   PGRUST_ISOLATION_OVERRIDE=1 Bypass the pg_locks/function gate if it is
#                               re-enabled for harness iteration

set -euo pipefail

# The gate itself fires after argument parsing so that --help and invalid
# flags still produce the right messages if the gate is re-enabled.
ISOLATION_REQUIRES_PG_LOCKS=0

# ---- paths + args ----------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PGRUST_DIR/.." && pwd)"
WORKTREE_NAME="$(basename "$PGRUST_DIR")"

resolve_postgres_dir() {
    local candidate
    # If the user explicitly set PGRUST_POSTGRES_DIR, treat it as authoritative:
    # fail loudly rather than silently fall through to a default.
    if [[ -n "${PGRUST_POSTGRES_DIR:-}" ]]; then
        if [[ -d "$PGRUST_POSTGRES_DIR/src/test/isolation" ]]; then
            (cd "$PGRUST_POSTGRES_DIR" && pwd)
            return 0
        fi
        echo "ERROR: PGRUST_POSTGRES_DIR=$PGRUST_POSTGRES_DIR does not contain src/test/isolation" >&2
        return 1
    fi
    # The 2-levels-up candidate handles pgrust-worktrees/<name>/ checkouts,
    # where $REPO_ROOT is pgrust-worktrees/ rather than your-projects-parent/.
    for candidate in \
        "$REPO_ROOT/postgres" \
        "$PGRUST_DIR/../../postgres" \
        "$HOME/postgres" \
        "$HOME/src/postgres" \
        "$HOME/dev/postgres"
    do
        if [[ -d "$candidate/src/test/isolation" ]]; then
            (cd "$candidate" && pwd)
            return 0
        fi
    done
    return 1
}

if ! POSTGRES_DIR="$(resolve_postgres_dir)"; then
    echo "ERROR: could not find postgres source tree. See build_pg_isolation_tools.sh for paths checked." >&2
    exit 1
fi

ISOLATION_DIR="$POSTGRES_DIR/src/test/isolation"
ISOLATIONTESTER="$POSTGRES_DIR/build/src/test/isolation/isolationtester"
SPEC_DIR="$ISOLATION_DIR/specs"
EXPECTED_DIR="$ISOLATION_DIR/expected"
DEFAULT_SCHEDULE="$ISOLATION_DIR/isolation_schedule"

PORT=5434
SKIP_BUILD=false
SKIP_SERVER=false
TIMEOUT=60
SINGLE_TEST=""
SCHEDULE=""
RESULTS_DIR=""
DATA_DIR=""
SERVER_PID=""
REGRESS_USER="${PGRUST_REGRESS_USER:-${PGUSER:-$(id -un)}}"
STARTUP_WAIT_SECS="${PGRUST_STARTUP_WAIT_SECS:-300}"

# Skips with a reason. Keep entries alphabetised; add "# reason" alongside.
# shellcheck disable=SC2034
declare -a SKIP_TESTS=(
    # (empty for now; populate as failing specs are triaged)
)

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --skip-server) SKIP_SERVER=true; shift ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --test) SINGLE_TEST="$2"; shift 2 ;;
        --schedule) SCHEDULE="$2"; shift 2 ;;
        --results-dir) RESULTS_DIR="$2"; shift 2 ;;
        --data-dir) DATA_DIR="$2"; shift 2 ;;
        -h|--help) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown flag: $1" >&2; exit 1 ;;
    esac
done

[[ -z "$SCHEDULE" ]] && SCHEDULE="$DEFAULT_SCHEDULE"

# ---- gate ------------------------------------------------------------------
# After arg parsing so --help and bad flags still work.
if [[ "$ISOLATION_REQUIRES_PG_LOCKS" == "1" && -z "${PGRUST_ISOLATION_OVERRIDE:-}" ]]; then
    cat >&2 <<EOF
ERROR: isolation tests cannot run yet.

The upstream isolationtester harness needs these pgrust features enabled:
  1. pg_locks wait rows (granted=false entries while a session is blocked)
  2. pg_catalog.pg_isolation_test_session_is_blocked(int, int[]) RETURNS bool

Without these, isolationtester never sees blocked sessions and tests hang
or produce nonsense output. The default pgrust build now provides them; this
message only appears if ISOLATION_REQUIRES_PG_LOCKS is manually re-enabled.

To iterate on the harness itself (e.g. against real PostgreSQL), bypass
the gate:
    PGRUST_ISOLATION_OVERRIDE=1 scripts/run_isolation.sh ...
EOF
    exit 2
fi

make_temp_dir() {
    local prefix="$1"
    mktemp -d "${TMPDIR:-/tmp}/${prefix}.${WORKTREE_NAME}.XXXXXX"
}

[[ -z "$RESULTS_DIR" ]] && RESULTS_DIR="$(make_temp_dir pgrust_isolation_results)"
[[ -z "$DATA_DIR" ]] && DATA_DIR="$(make_temp_dir pgrust_isolation_data)"

# ---- build isolationtester -------------------------------------------------
if [[ ! -x "$ISOLATIONTESTER" ]]; then
    echo "isolationtester not found; building..."
    "$SCRIPT_DIR/build_pg_isolation_tools.sh"
fi

if [[ ! -x "$ISOLATIONTESTER" ]]; then
    echo "ERROR: isolationtester still missing after build: $ISOLATIONTESTER" >&2
    exit 1
fi

# isolationtester dynamically links @rpath/libpq.5.dylib. Stage the dynamic
# loader path so only isolationtester invocations pick up the libpq we just
# built — not psql, which uses its own (potentially different) libpq and
# breaks our readiness probe if DYLD_LIBRARY_PATH is exported globally.
ISOLATIONTESTER_LIBPQ_DIR="$POSTGRES_DIR/build/src/interfaces/libpq"
if [[ "$(uname -s)" == "Darwin" ]]; then
    ISOLATIONTESTER_LIB_ENV="DYLD_LIBRARY_PATH=$ISOLATIONTESTER_LIBPQ_DIR${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
else
    ISOLATIONTESTER_LIB_ENV="LD_LIBRARY_PATH=$ISOLATIONTESTER_LIBPQ_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
fi

# ---- build pgrust_server ---------------------------------------------------
if [[ "$SKIP_BUILD" == false ]]; then
    echo "Building pgrust_server (release)..."
    (cd "$PGRUST_DIR" && cargo build --release --bin pgrust_server 2>&1) || {
        echo "ERROR: pgrust_server build failed" >&2
        exit 1
    }
fi

CARGO_TARGET_DIR="$(cd "$PGRUST_DIR" && cargo metadata --no-deps --format-version=1 \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')"
SERVER_BIN="$CARGO_TARGET_DIR/release/pgrust_server"
if [[ "$SKIP_SERVER" == false && ! -x "$SERVER_BIN" ]]; then
    echo "ERROR: $SERVER_BIN not found. Run without --skip-build." >&2
    exit 1
fi

# ---- cluster lifecycle -----------------------------------------------------
cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Stopping pgrust server (PID $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

port_is_listening() {
    lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

wait_for_server_ready() {
    local pid="$1"
    local attempts=$((STARTUP_WAIT_SECS * 2))
    echo "Waiting for server to accept connections on port $PORT..."
    for _ in $(seq 1 "$attempts"); do
        if psql -X -h 127.0.0.1 -p "$PORT" -U "$REGRESS_USER" postgres -c "SELECT 1" >/dev/null 2>&1; then
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

if [[ "$SKIP_SERVER" == false ]]; then
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    write_server_config
    if ! start_server; then
        echo "ERROR: pgrust server did not become ready" >&2
        exit 1
    fi
fi

export PGPASSWORD="x"
export PG_TEST_TIMEOUT_DEFAULT="$TIMEOUT"

# ---- schedule ------------------------------------------------------------
read_schedule() {
    # isolation_schedule lines look like: "test: foo" or "test: foo bar baz"
    # (multiple names on one line = run in parallel; we treat each as a
    # separate test for simplicity).
    grep -E '^test:' "$SCHEDULE" | sed 's/^test:[[:space:]]*//' | tr ' ' '\n' | grep -v '^$'
}

is_skipped() {
    local name="$1"
    local entry
    # Guarded expansion — `set -u` + bash 3 blows up on an empty array.
    for entry in ${SKIP_TESTS[@]+"${SKIP_TESTS[@]}"}; do
        [[ "$entry" == "$name" ]] && return 0
    done
    return 1
}

# ---- run -----------------------------------------------------------------
mkdir -p "$RESULTS_DIR/output" "$RESULTS_DIR/diff"
echo "Isolation results dir: $RESULTS_DIR"
echo "Data dir: $DATA_DIR"
echo "User: $REGRESS_USER"
echo "isolationtester: $ISOLATIONTESTER"

PASS=0
FAIL=0
SKIP=0
MISSING=0
FAILED_TESTS=()

run_one() {
    local name="$1"
    local spec="$SPEC_DIR/$name.spec"
    local expected="$EXPECTED_DIR/$name.out"
    local actual="$RESULTS_DIR/output/$name.out"
    local diff_file="$RESULTS_DIR/diff/$name.diff"

    if is_skipped "$name"; then
        printf '  %-45s SKIP\n' "$name"
        SKIP=$((SKIP + 1))
        return
    fi

    if [[ ! -f "$spec" ]]; then
        printf '  %-45s MISSING SPEC\n' "$name"
        MISSING=$((MISSING + 1))
        return
    fi
    if [[ ! -f "$expected" ]]; then
        printf '  %-45s MISSING EXPECTED\n' "$name"
        MISSING=$((MISSING + 1))
        return
    fi

    # :HACK: max_protocol_version=3.0 keeps libpq (built from PG master, 3.2+)
    # speaking the older wire protocol that pgrust currently implements.
    # Harmless against real PG. Remove once pgrust supports v3.2+.
    local conninfo="host=127.0.0.1 port=$PORT user=$REGRESS_USER dbname=postgres max_protocol_version=3.0"
    if env "$ISOLATIONTESTER_LIB_ENV" "$ISOLATIONTESTER" "$conninfo" < "$spec" > "$actual" 2>&1; then
        if diff -u "$expected" "$actual" > "$diff_file" 2>&1; then
            printf '  %-45s PASS\n' "$name"
            rm -f "$diff_file"
            PASS=$((PASS + 1))
        else
            printf '  %-45s FAIL (diff)\n' "$name"
            FAIL=$((FAIL + 1))
            FAILED_TESTS+=("$name")
        fi
    else
        printf '  %-45s FAIL (exit)\n' "$name"
        FAIL=$((FAIL + 1))
        FAILED_TESTS+=("$name")
        # Keep the output as-is; it already contains the error text.
        rm -f "$diff_file"
    fi
}

if [[ -n "$SINGLE_TEST" ]]; then
    echo "Running: $SINGLE_TEST"
    run_one "$SINGLE_TEST"
else
    echo "Running isolation schedule: $SCHEDULE"
    while IFS= read -r name; do
        [[ -z "$name" ]] && continue
        run_one "$name"
    done < <(read_schedule)
fi

echo
echo "===================================================="
echo "pass=$PASS  fail=$FAIL  skip=$SKIP  missing=$MISSING"
echo "Results: $RESULTS_DIR"
if [[ "$FAIL" -gt 0 ]]; then
    echo "Failed tests:"
    printf '  %s\n' "${FAILED_TESTS[@]}"
    echo "Diffs in: $RESULTS_DIR/diff/"
    exit 1
fi
