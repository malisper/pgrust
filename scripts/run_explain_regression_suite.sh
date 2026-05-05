#!/usr/bin/env bash
# Fast regression suite for query-plan diffs.
#
# The full regression files contain a lot of result-checking SELECTs that are
# unrelated to EXPLAIN output. This runner builds temporary per-test fixtures
# that keep setup/state-changing SQL, suppress setup output, and compare only
# the extracted EXPLAIN plan outputs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PGRUST_DIR/.." && pwd)"
PLAN_QUERY_SQL="$PGRUST_DIR/scripts/regression_plan_diff_queries_2026_05_04.sql"
PG_REGRESS=""

for candidate in \
    "$REPO_ROOT/postgres/src/test/regress" \
    "$PGRUST_DIR/../../postgres/src/test/regress"
do
    if [[ -d "$candidate" ]]; then
        PG_REGRESS="$(cd "$candidate" && pwd)"
        break
    fi
done

if [[ -z "$PG_REGRESS" ]]; then
    echo "ERROR: could not find postgres regression checkout." >&2
    exit 1
fi

RESULTS_DIR=""
TIMEOUT=120
STATEMENT_TIMEOUT=5
COPY_DIFFS_DIR="/tmp/diffs"
COPY_DIFFS=true
SKIP_BUILD=false
LIST_ONLY=false
TEST_FILTERS=()

usage() {
    cat <<'EOF'
Usage: scripts/run_explain_regression_suite.sh [options]

Runs the EXPLAIN statements extracted from the 2026-05-04 plan diffs, while
keeping only the setup SQL needed to make those EXPLAINs succeed.

Options:
  --results-dir DIR   Directory for generated fixtures, output, and diffs.
  --timeout SECS      Per-file timeout for bootstrap/deps/test psql runs (default: 120).
  --statement-timeout SECS
                      Server-side statement_timeout for psql sessions (default: 5).
  --copy-diffs DIR    Copy produced .diff files into DIR (default: /tmp/diffs).
  --no-copy-diffs     Do not copy produced .diff files into a shared directory.
  --test NAME         Run only one extracted source regression file; repeatable.
  --skip-build        Do not build pgrust_server before running.
  --list              Print the extracted source regression files and exit.
  -h, --help          Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --results-dir) RESULTS_DIR="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --statement-timeout) STATEMENT_TIMEOUT="$2"; shift 2 ;;
        --copy-diffs) COPY_DIFFS_DIR="$2"; COPY_DIFFS=true; shift 2 ;;
        --no-copy-diffs) COPY_DIFFS=false; shift ;;
        --test) TEST_FILTERS+=("$2"); shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --list) LIST_ONLY=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown flag: $1" >&2; usage >&2; exit 1 ;;
    esac
done

if [[ ! -f "$PLAN_QUERY_SQL" ]]; then
    echo "ERROR: plan query SQL not found: $PLAN_QUERY_SQL" >&2
    exit 1
fi
if ! [[ "$TIMEOUT" =~ ^[0-9]+$ ]] || [[ "$TIMEOUT" -lt 1 ]]; then
    echo "ERROR: --timeout must be a positive integer" >&2
    exit 1
fi
if ! [[ "$STATEMENT_TIMEOUT" =~ ^[0-9]+$ ]] || [[ "$STATEMENT_TIMEOUT" -lt 1 ]]; then
    echo "ERROR: --statement-timeout must be a positive integer" >&2
    exit 1
fi

mapfile -t PLAN_TESTS < <(
    awk '
        /^\\echo ==== / {
            sub(/^\\echo ==== /, "");
            sub(/ ====$/, "");
            print;
        }
    ' "$PLAN_QUERY_SQL"
)

contains_test() {
    local needle="$1"
    local item=""
    for item in "${PLAN_TESTS[@]}"; do
        [[ "$item" == "$needle" ]] && return 0
    done
    return 1
}

SELECTED_TESTS=()
if [[ ${#TEST_FILTERS[@]} -gt 0 ]]; then
    for test_name in "${TEST_FILTERS[@]}"; do
        if ! contains_test "$test_name"; then
            echo "ERROR: $test_name is not present in $PLAN_QUERY_SQL" >&2
            exit 1
        fi
        SELECTED_TESTS+=("$test_name")
    done
else
    SELECTED_TESTS=("${PLAN_TESTS[@]}")
fi

if [[ "$LIST_ONLY" == true ]]; then
    printf '%s\n' "${SELECTED_TESTS[@]}"
    exit 0
fi

if [[ -z "$RESULTS_DIR" ]]; then
    RESULTS_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pgrust_explain_regress.XXXXXX")"
fi

mkdir -p "$RESULTS_DIR"/{data,fixtures,logs,output,diff,status,tablespaces}
if [[ "$COPY_DIFFS" == true ]]; then
    mkdir -p "$COPY_DIFFS_DIR"
fi

direct_test_dependencies() {
    local test_name="$1"
    case "$test_name" in
        multirangetypes) echo "rangetypes" ;;
        geometry) echo "point lseg line box path polygon circle" ;;
        horology) echo "date time timetz timestamp timestamptz interval" ;;
        aggregates) echo "create_aggregate" ;;
        numeric_big) echo "numeric" ;;
        join) echo "create_index create_misc" ;;
        memoize) echo "create_index" ;;
        select) echo "create_index" ;;
        select_parallel|with) echo "create_misc" ;;
        psql|event_trigger) echo "create_am" ;;
        amutils) echo "geometry create_index_spgist hash_index brin" ;;
        select_views) echo "create_view" ;;
        brin_bloom|brin_multi) echo "brin" ;;
        brin) echo "create_index" ;;
        alter_table) echo "create_index" ;;
        create_index_spgist|index_including|index_including_gist) echo "create_index" ;;
        btree_index) echo "create_index" ;;
        stats_ext) echo "create_misc create_aggregate" ;;
        *) ;;
    esac
}

collect_dependencies() {
    local test_name="$1"
    local dep=""
    for dep in $(direct_test_dependencies "$test_name"); do
        collect_dependencies "$dep"
        printf '%s\n' "$dep"
    done
}

unique_dependencies_for_selection() {
    local test_name=""
    for test_name in "${SELECTED_TESTS[@]}"; do
        collect_dependencies "$test_name"
    done | awk '!seen[$0]++'
}

mapfile -t DEPENDENCY_TESTS < <(unique_dependencies_for_selection)

collect_deps_for_test() {
    local test_name="$1"
    collect_dependencies "$test_name" | awk '!seen[$0]++'
}

dependency_seed_name() {
    local dep_key="$1"
    if [[ "$dep_key" == "__base__" ]]; then
        printf 'test_setup'
        return
    fi
    printf 'post_%s' "$dep_key" | tr ' /' '__'
}

if [[ -z "${CARGO_TARGET_DIR:-}" ]]; then
    export CARGO_TARGET_DIR="$("$PGRUST_DIR/scripts/cargo_isolated.sh" --print-target-dir)"
fi

if [[ "$SKIP_BUILD" == false ]]; then
    echo "Building pgrust_server (debug, opt-level 0)..."
    (cd "$PGRUST_DIR" && CARGO_PROFILE_DEV_OPT_LEVEL=0 cargo build --bin pgrust_server)
fi

TARGET_DIR="$("$PGRUST_DIR/scripts/cargo_target_dir.sh")"
SERVER_BIN="$TARGET_DIR/debug/pgrust_server"
if [[ ! -x "$SERVER_BIN" ]]; then
    echo "ERROR: $SERVER_BIN not found. Run without --skip-build." >&2
    exit 1
fi

echo "Generating EXPLAIN-only fixtures..."
fixture_args=(
    --plan-query-sql "$PLAN_QUERY_SQL"
    --pg-regress "$PG_REGRESS"
    --out-dir "$RESULTS_DIR/fixtures"
)
for test_name in "${SELECTED_TESTS[@]}"; do
    fixture_args+=(--test "$test_name")
done
for dep in "${DEPENDENCY_TESTS[@]}"; do
    fixture_args+=(--dependency-test "$dep")
done
"$PGRUST_DIR/scripts/extract_regression_plan_diff_fixtures.py" "${fixture_args[@]}"

if command -v timeout >/dev/null 2>&1; then
    TIMEOUT_CMD=timeout
elif command -v gtimeout >/dev/null 2>&1; then
    TIMEOUT_CMD=gtimeout
else
    TIMEOUT_CMD=""
fi

next_port() {
    local port=55432
    while lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; do
        port=$((port + 1))
    done
    printf '%s\n' "$port"
}

run_with_timeout() {
    local timeout_secs="$1"
    shift
    if [[ -n "$TIMEOUT_CMD" ]]; then
        "$TIMEOUT_CMD" "$timeout_secs" "$@"
        return $?
    fi
    "$@" &
    local child=$!
    local elapsed=0
    while kill -0 "$child" 2>/dev/null; do
        if [[ "$elapsed" -ge "$timeout_secs" ]]; then
            kill "$child" 2>/dev/null || true
            sleep 1
            kill -9 "$child" 2>/dev/null || true
            wait "$child" 2>/dev/null || true
            return 124
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    wait "$child"
}

SERVER_PID=""
stop_server() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}
trap stop_server EXIT

start_server() {
    local data_dir="$1"
    local port="$2"
    local log_file="$3"

    mkdir -p "$data_dir"
    printf 'fsync = off\n' > "$data_dir/postgresql.conf"
    "$SERVER_BIN" "$data_dir" "$port" > "$log_file" 2>&1 &
    SERVER_PID=$!

    for _ in $(seq 1 600); do
        if psql -X -h 127.0.0.1 -p "$port" -U postgres postgres -c "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            return 1
        fi
        sleep 0.5
    done
    return 1
}

copy_seed_data() {
    local source_data_dir="$1"
    local source_tablespace_dir="$2"
    local target_data_dir="$3"
    local target_tablespace_dir="$4"
    local tblspc_entry=""
    local tblspc_name=""

    rm -rf "$target_data_dir" "$target_tablespace_dir"
    mkdir -p "$(dirname "$target_data_dir")" "$(dirname "$target_tablespace_dir")"
    cp -a "$source_data_dir" "$target_data_dir"
    if [[ -d "$source_tablespace_dir" ]]; then
        mkdir -p "$target_tablespace_dir"
        cp -a "$source_tablespace_dir/." "$target_tablespace_dir/"
    fi

    if [[ -d "$target_data_dir/pg_tblspc" ]]; then
        for tblspc_entry in "$target_data_dir"/pg_tblspc/*; do
            [[ -e "$tblspc_entry" || -L "$tblspc_entry" ]] || continue
            tblspc_name="$(basename "$tblspc_entry")"
            if [[ ! -L "$source_data_dir/pg_tblspc/$tblspc_name" ]]; then
                continue
            fi
            rm -rf "$tblspc_entry"
            ln -s "$target_tablespace_dir" "$target_data_dir/pg_tblspc/$tblspc_name"
        done
    fi

    printf 'fsync = off\n' > "$target_data_dir/postgresql.conf"
}

export PGPASSWORD="x"
export PG_ABS_SRCDIR="$PG_REGRESS"
export PGTZ="America/Los_Angeles"
export PGDATESTYLE="Postgres, MDY"

SEED_ROOT="$RESULTS_DIR/seeds"
mkdir -p "$SEED_ROOT"

declare -A TEST_DEP_KEYS=()
declare -A SEED_DEPS_BY_KEY=()
SEED_KEYS=()

add_seed_key() {
    local dep_key="$1"
    local dep_list="$2"
    local existing=""
    for existing in "${SEED_KEYS[@]}"; do
        [[ "$existing" == "$dep_key" ]] && return 0
    done
    SEED_KEYS+=("$dep_key")
    SEED_DEPS_BY_KEY["$dep_key"]="$dep_list"
}

for test_name in "${SELECTED_TESTS[@]}"; do
    dep_list="$(collect_deps_for_test "$test_name" | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
    dep_key="$(printf '%s' "$dep_list" | tr ' ' '+')"
    if [[ -z "$dep_key" ]]; then
        dep_key="__base__"
    fi
    TEST_DEP_KEYS["$test_name"]="$dep_key"
    add_seed_key "$dep_key" "$dep_list"
done

build_test_setup_seed() {
    local seed_name="test_setup"
    local seed_dir="$SEED_ROOT/$seed_name"
    local data_dir="$seed_dir/data"
    local tablespace_dir="$seed_dir/tablespaces/regress_tblspace"
    local log_file="$RESULTS_DIR/logs/seed.$seed_name.server.log"
    local setup_log="$RESULTS_DIR/logs/seed.$seed_name.setup.log"
    local port="$(next_port)"

    echo "Building seed: $seed_name"
    rm -rf "$seed_dir"
    mkdir -p "$data_dir" "$tablespace_dir"
    export PG_ABS_BUILDDIR="$seed_dir"
    export PGRUST_REGRESS_TABLESPACE_DIR="$tablespace_dir"
    export PGOPTIONS="-c intervalstyle=postgres_verbose -c statement_timeout=${STATEMENT_TIMEOUT}s"
    PG_ARGS=(-X -h 127.0.0.1 -p "$port" -U postgres -v "abs_srcdir=$PG_REGRESS" -v "abs_builddir=$seed_dir" -v HIDE_TOAST_COMPRESSION=on)

    if ! start_server "$data_dir" "$port" "$log_file"; then
        echo "ERROR: failed to start server for seed $seed_name; see $log_file" >&2
        return 1
    fi
    if ! run_with_timeout "$TIMEOUT" psql "${PG_ARGS[@]}" -v ON_ERROR_STOP=1 -q -f "$PGRUST_DIR/scripts/test_setup_pgrust.sql" > "$setup_log" 2>&1; then
        echo "ERROR: bootstrap failed for seed $seed_name; see $setup_log" >&2
        stop_server
        return 1
    fi
    stop_server
}

build_dependency_seed() {
    local dep_key="$1"
    local dep_list="$2"
    local seed_name
    seed_name="$(dependency_seed_name "$dep_key")"
    local seed_dir="$SEED_ROOT/$seed_name"
    local data_dir="$seed_dir/data"
    local tablespace_dir="$seed_dir/tablespaces/regress_tblspace"
    local log_file="$RESULTS_DIR/logs/seed.$seed_name.server.log"
    local port="$(next_port)"
    local dep=""

    if [[ "$dep_key" == "__base__" ]]; then
        return 0
    fi

    echo "Building seed: $seed_name ($dep_list)"
    copy_seed_data \
        "$SEED_ROOT/test_setup/data" \
        "$SEED_ROOT/test_setup/tablespaces/regress_tblspace" \
        "$data_dir" \
        "$tablespace_dir"
    export PG_ABS_BUILDDIR="$seed_dir"
    export PGRUST_REGRESS_TABLESPACE_DIR="$tablespace_dir"
    export PGOPTIONS="-c intervalstyle=postgres_verbose -c statement_timeout=${STATEMENT_TIMEOUT}s"
    PG_ARGS=(-X -h 127.0.0.1 -p "$port" -U postgres -v "abs_srcdir=$PG_REGRESS" -v "abs_builddir=$seed_dir" -v HIDE_TOAST_COMPRESSION=on)

    if ! start_server "$data_dir" "$port" "$log_file"; then
        echo "ERROR: failed to start server for seed $seed_name; see $log_file" >&2
        return 1
    fi

    for dep in $dep_list; do
        dep_sql="$RESULTS_DIR/fixtures/deps/$dep.sql"
        dep_log="$RESULTS_DIR/logs/seed.$seed_name.dep.$dep.log"
        if [[ ! -f "$dep_sql" ]]; then
            echo "ERROR: dependency fixture not found for seed $seed_name: $dep_sql" >&2
            stop_server
            return 1
        fi
        if ! run_with_timeout "$TIMEOUT" psql "${PG_ARGS[@]}" -q -f "$dep_sql" > "$dep_log" 2>&1; then
            echo "ERROR: dependency $dep failed while building seed $seed_name; see $dep_log" >&2
            stop_server
            return 1
        fi
        psql "${PG_ARGS[@]}" -q -c "RESET ROLE; SET search_path = public;" >> "$dep_log" 2>&1 || {
            echo "ERROR: failed to reset session after dependency $dep for seed $seed_name; see $dep_log" >&2
            stop_server
            return 1
        }
    done
    stop_server
}

echo "Selected tests: ${#SELECTED_TESTS[@]}"
echo "Dependencies: ${DEPENDENCY_TESTS[*]:-none}"
echo "Seed variants: ${#SEED_KEYS[@]}"
echo "Results dir: $RESULTS_DIR"

build_test_setup_seed
for dep_key in "${SEED_KEYS[@]}"; do
    build_dependency_seed "$dep_key" "${SEED_DEPS_BY_KEY[$dep_key]}"
done

SUMMARY_TSV="$RESULTS_DIR/explain_regression_summary.tsv"
printf 'test\tstatus\tplans\tdiff_lines\trun_dir\toutput\tdiff\n' > "$SUMMARY_TSV"

passed=0
failed=0
errored=0
timed_out=0

for test_name in "${SELECTED_TESTS[@]}"; do
    echo "==> $test_name"
    run_dir="$RESULTS_DIR/runs/$test_name"
    data_dir="$run_dir/data"
    tablespace_dir="$run_dir/tablespaces/regress_tblspace"
    server_log="$RESULTS_DIR/logs/$test_name.server.log"
    output_file="$RESULTS_DIR/output/$test_name.out"
    expected_file="$RESULTS_DIR/fixtures/expected/$test_name.out"
    actual_sql="$RESULTS_DIR/fixtures/sql/$test_name.sql"
    diff_file="$RESULTS_DIR/diff/$test_name.diff"
    status_file="$RESULTS_DIR/status/$test_name.status"
    port="$(next_port)"
    status="pass"
    diff_lines=0
    plan_count=0
    dep_key="${TEST_DEP_KEYS[$test_name]}"
    seed_name="$(dependency_seed_name "$dep_key")"
    seed_dir="$SEED_ROOT/$seed_name"

    rm -rf "$run_dir"
    mkdir -p "$run_dir"
    copy_seed_data \
        "$seed_dir/data" \
        "$seed_dir/tablespaces/regress_tblspace" \
        "$data_dir" \
        "$tablespace_dir"
    export PG_ABS_BUILDDIR="$run_dir"
    export PGRUST_REGRESS_TABLESPACE_DIR="$tablespace_dir"
    export PGOPTIONS="-c intervalstyle=postgres_verbose -c statement_timeout=${STATEMENT_TIMEOUT}s"
    PG_ARGS=(-X -h 127.0.0.1 -p "$port" -U postgres -v "abs_srcdir=$PG_REGRESS" -v "abs_builddir=$run_dir" -v HIDE_TOAST_COMPRESSION=on)

    if ! start_server "$data_dir" "$port" "$server_log"; then
        status="error"
        echo "ERROR: server failed to start for $test_name; see $server_log" >&2
    fi

    if [[ "$status" == "pass" ]]; then
        set +e
        run_with_timeout "$TIMEOUT" psql "${PG_ARGS[@]}" -q -f "$actual_sql" > "$output_file" 2>&1
        rc=$?
        set -e
        if [[ "$rc" -eq 124 ]]; then
            status="timeout"
            echo "TIMEOUT" >> "$output_file"
        elif [[ "$rc" -ne 0 ]]; then
            status="error"
        fi
    fi

    if [[ -f "$expected_file" ]]; then
        plan_count="$(grep -c '^---- plan-diff ' "$expected_file" || true)"
    fi

    if [[ "$status" == "pass" ]]; then
        if diff -u "$expected_file" "$output_file" > "$diff_file"; then
            rm -f "$diff_file"
        else
            status="fail"
            diff_lines="$(wc -l < "$diff_file" | tr -d ' ')"
            if [[ "$COPY_DIFFS" == true ]]; then
                cp "$diff_file" "$COPY_DIFFS_DIR/$test_name.diff"
            fi
        fi
    else
        if [[ -f "$output_file" ]]; then
            diff -u "$expected_file" "$output_file" > "$diff_file" || true
            diff_lines="$(wc -l < "$diff_file" 2>/dev/null | tr -d ' ' || echo 0)"
            if [[ "$COPY_DIFFS" == true && -s "$diff_file" ]]; then
                cp "$diff_file" "$COPY_DIFFS_DIR/$test_name.diff"
            fi
        fi
    fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$test_name" "$status" "$plan_count" "$diff_lines" "$run_dir" "$output_file" "$diff_file" > "$status_file"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$test_name" "$status" "$plan_count" "$diff_lines" "$run_dir" "$output_file" "$diff_file" >> "$SUMMARY_TSV"

    case "$status" in
        pass) passed=$((passed + 1)) ;;
        fail) failed=$((failed + 1)) ;;
        timeout) timed_out=$((timed_out + 1)) ;;
        *) errored=$((errored + 1)) ;;
    esac

    stop_server
done

echo
echo "Summary:"
echo "  passed:  $passed"
echo "  failed:  $failed"
echo "  errored: $errored"
echo "  timeout: $timed_out"
echo "  details: $SUMMARY_TSV"
echo "  output:  $RESULTS_DIR/output"
echo "  diffs:   $RESULTS_DIR/diff"
if [[ "$COPY_DIFFS" == true ]]; then
    echo "  copied:  $COPY_DIFFS_DIR"
fi

if [[ "$failed" -gt 0 || "$errored" -gt 0 || "$timed_out" -gt 0 ]]; then
    exit 1
fi
