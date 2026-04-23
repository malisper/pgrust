#!/bin/bash
# Run PostgreSQL regression tests against pgrust and report pass/fail statistics.
#
# Usage: scripts/run_regression.sh [--port PORT] [--skip-build] [--skip-server] [--timeout SECS] [--test TESTNAME] [--upstream-setup]
#
# By default, this script:
#   1. Builds pgrust_server in release mode
#   2. Starts it on a fresh data directory
#   3. Runs each .sql regression test via psql with statement_timeout = '5s'
#   4. Compares output against expected .out files
#   5. Reports pass/fail/error statistics
#
# Options:
#   --port PORT       Port for pgrust server (default: 5433)
#   --skip-build      Don't rebuild pgrust_server
#   --skip-server     Assume server is already running (don't start/stop it)
#   --timeout SECS    Per-test timeout in seconds (default: 30)
#   --test TESTNAME   Run only this test (without .sql extension)
#   --results-dir DIR Directory for results (default: unique temp dir)
#   --data-dir DIR    Directory for the pgrust cluster (default: unique temp dir)
#   --upstream-setup Use upstream test_setup.sql instead of the pgrust bootstrap (default: use pgrust bootstrap)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PGRUST_DIR/.." && pwd)"
PG_REGRESS=""
for candidate in \
    "$REPO_ROOT/postgres/src/test/regress" \
    "$PGRUST_DIR/../../postgres/src/test/regress"
do
    if [[ -d "$candidate" ]]; then
        PG_REGRESS="$candidate"
        break
    fi
done

if [[ -z "$PG_REGRESS" ]]; then
    echo "ERROR: could not find postgres regression checkout."
    echo "Looked in:"
    echo "  $REPO_ROOT/postgres/src/test/regress"
    echo "  $PGRUST_DIR/../../postgres/src/test/regress"
    exit 1
fi

SQL_DIR="$PG_REGRESS/sql"
EXPECTED_DIR="$PG_REGRESS/expected"
PG_REGRESS_ABS="$(cd "$PG_REGRESS" && pwd)"
SCHEDULE_FILE="$PG_REGRESS/parallel_schedule"
WORKTREE_NAME="$(basename "$PGRUST_DIR")"
TABLESPACE_VERSION_DIRECTORY="PG_18_202406281"
REGRESS_TABLESPACE_DIR=""
PREPARED_SETUP_SQL=""

setup_pg_regress_env() {
    if command -v pg_config >/dev/null 2>&1; then
        export PG_LIBDIR
        PG_LIBDIR="$(pg_config --pkglibdir)"
    fi

    if [[ -z "${PG_DLSUFFIX:-}" ]]; then
        case "$(uname -s)" in
            Darwin) export PG_DLSUFFIX=".dylib" ;;
            MINGW*|MSYS*|CYGWIN*) export PG_DLSUFFIX=".dll" ;;
            *) export PG_DLSUFFIX=".so" ;;
        esac
    fi
}

transform_conversion_fixture() {
    local input_path="$1"
    local output_path="$2"

    perl -0pe "
        s/CREATE FUNCTION test_enc_setup\\(\\) RETURNS void\\n\\s+AS :'regresslib', 'test_enc_setup'\\n\\s+LANGUAGE C STRICT;\\nSELECT FROM test_enc_setup\\(\\);/SELECT pg_rust_test_enc_setup();/s;
        s/SELECT pg_rust_test_enc_setup\\(\\);\\n--\\n\\(1 row\\)/SELECT pg_rust_test_enc_setup();\\n pg_rust_test_enc_setup \\n------------------------\\n \\n(1 row)/s;
        s/CREATE FUNCTION test_enc_conversion\\(bytea, name, name, bool, validlen OUT int, result OUT bytea\\)\\n\\s+AS :'regresslib', 'test_enc_conversion'\\n\\s+LANGUAGE C STRICT;\\n//s;
        s/\\btest_enc_conversion\\s*\\(/pg_rust_test_enc_conversion(/g;
        s/\\bcreate\\s+or\\s+replace\\s+function\\s+test_conv\\(/create function test_conv(/i;
    " "$input_path" > "$output_path"
}

transform_foreign_data_fixture() {
    local input_path="$1"
    local output_path="$2"

    perl -0pe "
        s/CREATE FUNCTION test_fdw_handler\\(\\)\\n\\s+RETURNS fdw_handler\\n\\s+AS :'regresslib', 'test_fdw_handler'\\n\\s+LANGUAGE C;\\n//s;
        s/\\btest_fdw_handler\\b/pg_rust_test_fdw_handler/g;
    " "$input_path" > "$output_path"
}

transform_alter_generic_fixture() {
    local input_path="$1"
    local output_path="$2"

    perl -0pe "
        s/CREATE FUNCTION test_opclass_options_func\\(internal\\)\\n\\s+RETURNS void\\n\\s+AS :'regresslib', 'test_opclass_options_func'\\n\\s+LANGUAGE C;\\n//s;
        s/\\btest_opclass_options_func\\b/pg_rust_test_opclass_options_func/g;
    " "$input_path" > "$output_path"
}

transform_triggers_fixture() {
    local input_path="$1"
    local output_path="$2"

    perl -0pe "
        s/CREATE FUNCTION trigger_return_old \\(\\)\\n\\s+RETURNS trigger\\n\\s+AS :'regresslib'\\n\\s+LANGUAGE C;/CREATE FUNCTION trigger_return_old ()\\n        RETURNS trigger\\n        AS \\\$\\\$\\nBEGIN\\n    IF TG_OP = 'INSERT' THEN\\n        RETURN NEW;\\n    END IF;\\n    RETURN OLD;\\nEND\\n\\\$\\\$\\n        LANGUAGE plpgsql;/s;
    " "$input_path" > "$output_path"
}

prepare_setup_fixture() {
    local input_path="$1"
    local output_path="$2"

    perl -0pe '
        my $tablespace_dir = $ENV{"PGRUST_REGRESS_TABLESPACE_DIR"};
        my $version_dir = $tablespace_dir . "/" . $ENV{"PGRUST_TABLESPACE_VERSION_DIRECTORY"};
        s{CREATE TABLESPACE regress_tblspace LOCATION '\''/tmp/pgrust_regress_tblspace'\'';}
         {"CREATE TABLESPACE regress_tblspace LOCATION '\''$tablespace_dir'\'';"}ge;
        END {
            if ($tablespace_dir eq q{}) {
                die "PGRUST_REGRESS_TABLESPACE_DIR must be set\n";
            }
        }
    ' "$input_path" > "$output_path"

    rm -rf "$REGRESS_TABLESPACE_DIR/$TABLESPACE_VERSION_DIRECTORY"
    mkdir -p "$REGRESS_TABLESPACE_DIR"
}

prepare_test_fixture() {
    local sql_file="$1"
    local expected_file="$2"
    local test_name="$3"

    PREPARED_SQL_FILE="$sql_file"
    PREPARED_EXPECTED_FILE="$expected_file"

    local fixture_dir="$RESULTS_DIR/fixtures"
    case "$test_name" in
        conversion)
            mkdir -p "$fixture_dir"
            PREPARED_SQL_FILE="$fixture_dir/${test_name}.sql"
            PREPARED_EXPECTED_FILE="$fixture_dir/${test_name}.out"
            transform_conversion_fixture "$sql_file" "$PREPARED_SQL_FILE"
            transform_conversion_fixture "$expected_file" "$PREPARED_EXPECTED_FILE"
            ;;
        foreign_data)
            mkdir -p "$fixture_dir"
            PREPARED_SQL_FILE="$fixture_dir/${test_name}.sql"
            PREPARED_EXPECTED_FILE="$fixture_dir/${test_name}.out"
            transform_foreign_data_fixture "$sql_file" "$PREPARED_SQL_FILE"
            transform_foreign_data_fixture "$expected_file" "$PREPARED_EXPECTED_FILE"
            ;;
        alter_generic)
            mkdir -p "$fixture_dir"
            PREPARED_SQL_FILE="$fixture_dir/${test_name}.sql"
            PREPARED_EXPECTED_FILE="$fixture_dir/${test_name}.out"
            transform_alter_generic_fixture "$sql_file" "$PREPARED_SQL_FILE"
            transform_alter_generic_fixture "$expected_file" "$PREPARED_EXPECTED_FILE"
            ;;
        triggers)
            mkdir -p "$fixture_dir"
            PREPARED_SQL_FILE="$fixture_dir/${test_name}.sql"
            PREPARED_EXPECTED_FILE="$fixture_dir/${test_name}.out"
            transform_triggers_fixture "$sql_file" "$PREPARED_SQL_FILE"
            transform_triggers_fixture "$expected_file" "$PREPARED_EXPECTED_FILE"
            ;;
        *)
            ;;
    esac
}

build_ordered_test_files() {
    local sql_dir="$1"
    local schedule_file="$2"
    local include_setup="$3"
    local -a ordered_files=()
    local -A seen=()

    if [[ -f "$schedule_file" ]]; then
        while IFS= read -r test_name; do
            [[ -n "$test_name" ]] || continue
            if [[ "$include_setup" != true && "$test_name" == "test_setup" ]]; then
                continue
            fi
            local sql_file="$sql_dir/${test_name}.sql"
            if [[ -f "$sql_file" && -z "${seen[$sql_file]:-}" ]]; then
                ordered_files+=("$sql_file")
                seen["$sql_file"]=1
            fi
        done < <(
            awk '
                /^test:[[:space:]]*/ {
                    sub(/^test:[[:space:]]*/, "");
                    for (i = 1; i <= NF; i++) {
                        print $i;
                    }
                }
            ' "$schedule_file"
        )
    fi

    while IFS= read -r sql_file; do
        [[ -n "$sql_file" ]] || continue
        if [[ "$include_setup" != true && "$(basename "$sql_file")" == "test_setup.sql" ]]; then
            continue
        fi
        if [[ -z "${seen[$sql_file]:-}" ]]; then
            ordered_files+=("$sql_file")
            seen["$sql_file"]=1
        fi
    done < <(find "$sql_dir" -maxdepth 1 -type f -name '*.sql' | sort)

    printf '%s\n' "${ordered_files[@]}"
}

add_aggregate_dependencies() {
    local -a expanded_files=()
    local saw_create_aggregate=false
    local sql_file=""
    local test_name=""
    local create_aggregate_file="$SQL_DIR/create_aggregate.sql"

    for sql_file in "${TEST_FILES[@]}"; do
        test_name="$(basename "$sql_file" .sql)"
        if [[ "$test_name" == "create_aggregate" ]]; then
            saw_create_aggregate=true
        fi
        if [[ "$test_name" == "aggregates" && "$saw_create_aggregate" == false ]]; then
            if [[ ! -f "$create_aggregate_file" ]]; then
                echo "ERROR: aggregate dependency not found: $create_aggregate_file"
                exit 1
            fi
            expanded_files+=("$create_aggregate_file")
            saw_create_aggregate=true
        fi
        expanded_files+=("$sql_file")
    done

    TEST_FILES=("${expanded_files[@]}")
}

PORT=5433
SKIP_BUILD=false
SKIP_SERVER=false
TIMEOUT=30
SINGLE_TEST=""
RESULTS_DIR=""
DATA_DIR=""
SERVER_PID=""
USE_PGRUST_SETUP=true
REGRESS_USER="${PGRUST_REGRESS_USER:-${PGUSER:-$(id -un)}}"
REGRESS_TABLESPACE_DIR=""
STARTUP_WAIT_SECS="${PGRUST_STARTUP_WAIT_SECS:-300}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --skip-server) SKIP_SERVER=true; shift ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --test) SINGLE_TEST="$2"; shift 2 ;;
        --results-dir) RESULTS_DIR="$2"; shift 2 ;;
        --data-dir) DATA_DIR="$2"; shift 2 ;;
        --pgrust-setup) USE_PGRUST_SETUP=true; shift ;;
        --upstream-setup) USE_PGRUST_SETUP=false; shift ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

make_temp_dir() {
    local prefix="$1"
    mktemp -d "${TMPDIR:-/tmp}/${prefix}.${WORKTREE_NAME}.XXXXXX"
}

if [[ -z "$RESULTS_DIR" ]]; then
    RESULTS_DIR="$(make_temp_dir pgrust_regress_results)"
fi

if [[ -z "$DATA_DIR" ]]; then
    DATA_DIR="$(make_temp_dir pgrust_regress_data)"
fi

REGRESS_TABLESPACE_DIR="$RESULTS_DIR/tablespaces/regress_tblspace"
export PGRUST_REGRESS_TABLESPACE_DIR="$REGRESS_TABLESPACE_DIR"
export PGRUST_TABLESPACE_VERSION_DIRECTORY="$TABLESPACE_VERSION_DIRECTORY"
PREPARED_SETUP_SQL="$RESULTS_DIR/fixtures/test_setup_pgrust.sql"

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

    echo "Waiting for server to accept connections..."
    # Fresh durable clusters rebuild shared catalog state before binding the
    # socket, so cold startup can legitimately take a few minutes on this
    # branch. Keep the wait window large enough to avoid false negatives.
    for i in $(seq 1 "$attempts"); do
        if psql -X -h 127.0.0.1 -p "$PORT" -U "$REGRESS_USER" postgres -c "SELECT 1" >/dev/null 2>&1; then
            echo "Server ready."
            return 0
        fi
        if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
            return 1
        fi
        sleep 0.5
    done

    psql -X -h 127.0.0.1 -p "$PORT" -U "$REGRESS_USER" postgres -c "SELECT 1" >/dev/null 2>&1
}

ensure_port_available() {
    if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "ERROR: port $PORT is already in use by another listener"
        lsof -nP -iTCP:"$PORT" -sTCP:LISTEN || true
        return 1
    fi
    return 0
}

write_regression_config() {
    cat > "$DATA_DIR/postgresql.conf" <<'EOF'
fsync = off
EOF
}

start_server() {
    if ! ensure_port_available; then
        return 1
    fi
    echo "Starting pgrust server on port $PORT (data: $DATA_DIR)..."
    if port_is_listening "$PORT"; then
        echo "ERROR: port $PORT is already in use; refusing to treat another server as ready."
        return 1
    fi
    "$SERVER_BIN" "$DATA_DIR" "$PORT" &
    SERVER_PID=$!

    if ! wait_for_server_ready "$SERVER_PID"; then
        return 1
    fi

    return 0
}

restart_server() {
    echo "  -> Server crashed, restarting..."
    cleanup
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    write_regression_config

    if ! start_server; then
        echo "  -> Restart failed; aborting run to avoid contaminating later results."
        return 1
    fi

    # Rebuild the shared regression fixtures after wiping the data directory.
    # Otherwise later tests run against an empty cluster and report misleading
    # "unknown table" failures for setup objects like INT2_TBL.
    if ! run_bootstrap_setup; then
        echo "  -> Bootstrap replay failed after restart; aborting run."
        return 1
    fi

    return 0
}

# Resolve a working timeout command (GNU coreutils on macOS installs as gtimeout)
if command -v timeout >/dev/null 2>&1; then
    TIMEOUT_CMD=timeout
elif command -v gtimeout >/dev/null 2>&1; then
    TIMEOUT_CMD=gtimeout
else
    TIMEOUT_CMD=""
fi

# Build pgrust_server
if [[ "$SKIP_BUILD" == false ]]; then
    echo "Building pgrust_server (release)..."
    (cd "$PGRUST_DIR" && cargo build --release --bin pgrust_server 2>&1) || {
        echo "ERROR: Build failed"
        exit 1
    }
fi

TARGET_DIR="$("$PGRUST_DIR/scripts/cargo_target_dir.sh")"
SERVER_BIN="$TARGET_DIR/release/pgrust_server"
if [[ ! -x "$SERVER_BIN" ]]; then
    echo "ERROR: $SERVER_BIN not found. Run without --skip-build."
    exit 1
fi

# Set up results directory
mkdir -p "$RESULTS_DIR/output" "$RESULTS_DIR/diff"
echo "Regression results dir: $RESULTS_DIR"
echo "Regression data dir: $DATA_DIR"
echo "Regression user: $REGRESS_USER"

# Start pgrust server
if [[ "$SKIP_SERVER" == false ]]; then
    # Fresh data directory for each run
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    write_regression_config

    if ! start_server; then
        echo "ERROR: Server did not become ready in time"
        exit 1
    fi
fi

export PGPASSWORD="x"
export PG_ABS_SRCDIR="$PG_REGRESS_ABS"
export PGRUST_REGRESS_TABLESPACE_DIR="$REGRESS_TABLESPACE_DIR"
export PGTZ="America/Los_Angeles"
export PGDATESTYLE="Postgres, MDY"
setup_pg_regress_env
export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c statement_timeout=5s"
# PG18 psql adds a verbose \d+ Compression column by default. Keep the
# regression client surface aligned with the checked-in expected files until
# the repo moves those fixtures to the new default shape.
PG_ARGS=(-X -h 127.0.0.1 -p "$PORT" -U postgres -v "abs_srcdir=$PG_REGRESS_ABS" -v HIDE_TOAST_COMPRESSION=on)

run_bootstrap_setup() {
    local setup_sql=""
    local setup_out=""
    local setup_label=""

    if [[ "$USE_PGRUST_SETUP" == true ]]; then
        setup_sql="$PGRUST_DIR/scripts/test_setup_pgrust.sql"
        setup_out="$RESULTS_DIR/output/test_setup_pgrust.out"
        setup_label="pgrust setup bootstrap"
        mkdir -p "$RESULTS_DIR/fixtures"
        prepare_setup_fixture "$setup_sql" "$PREPARED_SETUP_SQL"
        setup_sql="$PREPARED_SETUP_SQL"
    else
        setup_sql="$SQL_DIR/test_setup.sql"
        setup_out="$RESULTS_DIR/output/test_setup.out"
        setup_label="upstream setup bootstrap"
    fi

    if [[ ! -f "$setup_sql" ]]; then
        echo "ERROR: setup file not found: $setup_sql"
        return 1
    fi

    echo "Running $setup_label..."
    if [[ -n "$TIMEOUT_CMD" ]]; then
        if ! $TIMEOUT_CMD "$TIMEOUT" psql "${PG_ARGS[@]}" -v ON_ERROR_STOP=1 -a -q < "$setup_sql" > "$setup_out" 2>&1; then
            echo "ERROR: $setup_label failed"
            echo "See: $setup_out"
            return 1
        fi
    else
        if ! psql "${PG_ARGS[@]}" -v ON_ERROR_STOP=1 -a -q < "$setup_sql" > "$setup_out" 2>&1; then
            echo "ERROR: $setup_label failed"
            echo "See: $setup_out"
            return 1
        fi
    fi

    return 0
}

echo "Per-query statement_timeout: 5s"

if ! run_bootstrap_setup; then
    exit 1
fi

# Collect test files
if [[ -n "$SINGLE_TEST" ]]; then
    TEST_FILES=("$SQL_DIR/${SINGLE_TEST}.sql")
    if [[ ! -f "${TEST_FILES[0]}" ]]; then
        echo "ERROR: Test file not found: ${TEST_FILES[0]}"
        exit 1
    fi
else
    mapfile -t TEST_FILES < <(
        build_ordered_test_files \
            "$SQL_DIR" \
            "$SCHEDULE_FILE" \
            "$([[ "$USE_PGRUST_SETUP" == false ]] && echo true || echo false)"
    )
fi

if [[ "$USE_PGRUST_SETUP" == true ]]; then
    filtered_test_files=()
    for sql_file in "${TEST_FILES[@]}"; do
        if [[ "$(basename "$sql_file")" == "test_setup.sql" ]]; then
            continue
        fi
        filtered_test_files+=("$sql_file")
    done
    TEST_FILES=("${filtered_test_files[@]}")
else
    TEST_SETUP_FILE="$SQL_DIR/test_setup.sql"
    if [[ -f "$TEST_SETUP_FILE" ]]; then
        ordered_test_files=("$TEST_SETUP_FILE")
        for sql_file in "${TEST_FILES[@]}"; do
            if [[ "$sql_file" == "$TEST_SETUP_FILE" ]]; then
                continue
            fi
            ordered_test_files+=("$sql_file")
        done
        TEST_FILES=("${ordered_test_files[@]}")
    fi
fi

add_aggregate_dependencies

TOTAL=0
PASSED=0
FAILED=0
ERRORED=0

TOTAL_QUERIES=0
QUERIES_MATCHED=0
QUERIES_MISMATCHED=0

pass_list=()
fail_list=()
error_list=()

echo ""
echo "Running ${#TEST_FILES[@]} regression tests..."
echo "=============================================="
echo ""

count_matching_queries() {
    local expected_path="$1"
    local actual_path="$2"
    local sql_path="$3"

    perl -e '
        use strict;
        use warnings;

        my ($expected_path, $actual_path, $sql_path) = @ARGV;

        sub normalize_line {
            my ($line) = @_;
            $line =~ s/[ \t]+/ /g;
            $line =~ s/^ //;
            $line =~ s/[ \t]+$//;
            return $line;
        }

        sub read_lines {
            my ($path) = @_;
            open my $fh, "<", $path or die $!;
            my @lines = <$fh>;
            close $fh;
            chomp @lines;
            s/\r$// for @lines;
            return \@lines;
        }

        sub parse_sql_statements {
            my ($path) = @_;
            my $lines = read_lines($path);
            my @stmts;
            my @current;
            my $in_copy_data = 0;

            for my $line (@$lines) {
                if ($in_copy_data) {
                    if ($line =~ /^\s*\\\.\s*$/) {
                        $in_copy_data = 0;
                    }
                    next;
                }

                if (!@current) {
                    next if $line =~ /^\s*$/;
                    next if $line =~ /^\s*--/;

                    if ($line =~ /^\s*\\/) {
                        next;
                    }
                }

                push @current, normalize_line($line);

                if ($line =~ /;([[:space:]]*--.*)?[[:space:]]*$/ || $line =~ /(^|[^\\])\\[[:alpha:]]/) {
                    push @stmts, [ @current ];
                    if ($line =~ /^\s*copy\b.*\bfrom\s+stdin\b.*;([[:space:]]*--.*)?[[:space:]]*$/i) {
                        $in_copy_data = 1;
                    }
                    @current = ();
                }
            }

            push @stmts, [ @current ] if @current;
            return \@stmts;
        }

        sub find_statement_start {
            my ($lines, $stmt_lines, $start_idx) = @_;
            my $stmt_len = scalar @$stmt_lines;
            return undef if $stmt_len == 0;

            LINE:
            for (my $i = $start_idx; $i + $stmt_len - 1 <= $#$lines; $i++) {
                for (my $j = 0; $j < $stmt_len; $j++) {
                    next LINE if normalize_line($lines->[$i + $j]) ne $stmt_lines->[$j];
                }
                return $i;
            }

            return undef;
        }

        sub split_output_blocks {
            my ($path, $stmts) = @_;
            my $lines = read_lines($path);
            my @starts;
            my $search_from = 0;

            for my $stmt (@$stmts) {
                my $start = find_statement_start($lines, $stmt, $search_from);
                push @starts, $start;
                if (defined $start) {
                    $search_from = $start + scalar(@$stmt);
                }
            }

            my @blocks;
            for (my $i = 0; $i < @$stmts; $i++) {
                my $start = $starts[$i];
                if (!defined $start) {
                    push @blocks, undef;
                    next;
                }

                my $end = $#$lines;
                for (my $j = $i + 1; $j < @starts; $j++) {
                    if (defined $starts[$j]) {
                        $end = $starts[$j] - 1;
                        last;
                    }
                }

                my @block = map { normalize_line($_) } @$lines[$start .. $end];
                push @blocks, join("\n", @block);
            }

            return \@blocks;
        }

        my $stmts = parse_sql_statements($sql_path);
        my $expected_blocks = split_output_blocks($expected_path, $stmts);
        my $actual_blocks = split_output_blocks($actual_path, $stmts);

        my $total = scalar @$stmts;
        my $matched = 0;
        for (my $i = 0; $i < $total; $i++) {
            next if !defined $expected_blocks->[$i];
            next if !defined $actual_blocks->[$i];
            $matched++ if $expected_blocks->[$i] eq $actual_blocks->[$i];
        }

        my $mismatched = $total - $matched;
        print "$matched $mismatched $total\n";
    ' "$expected_path" "$actual_path" "$sql_path"
}

for sql_file in "${TEST_FILES[@]}"; do
    test_name="$(basename "$sql_file" .sql)"
    expected_file="$EXPECTED_DIR/${test_name}.out"
    output_file="$RESULTS_DIR/output/${test_name}.out"
    diff_file="$RESULTS_DIR/diff/${test_name}.diff"

    TOTAL=$((TOTAL + 1))

    # Check if expected output exists
    if [[ ! -f "$expected_file" ]]; then
        printf "%-40s SKIP (no expected output)\n" "$test_name"
        TOTAL=$((TOTAL - 1))
        continue
    fi

    prepare_test_fixture "$sql_file" "$expected_file" "$test_name"
    sql_file="$PREPARED_SQL_FILE"
    expected_file="$PREPARED_EXPECTED_FILE"

    # Run the test with timeout (if available)
    # -a = echo all input, -q = quiet mode (matches PG regression test runner)
    if [[ -n "$TIMEOUT_CMD" ]]; then
        if $TIMEOUT_CMD "$TIMEOUT" psql "${PG_ARGS[@]}" -a -q < "$sql_file" > "$output_file" 2>&1; then
            :
        else
            exit_code=$?
            if [[ $exit_code -eq 124 ]]; then
                echo "TIMEOUT" >> "$output_file"
            fi
        fi
    else
        psql "${PG_ARGS[@]}" -a -q < "$sql_file" > "$output_file" 2>&1 || true
    fi

    # Compare output to expected.
    # Some tests have multiple expected outputs (e.g., boolean.out, boolean_1.out).
    # Restrict alternates to numbered variants for the same test so we do not
    # accidentally match unrelated siblings like psql_crosstab.out for psql.out.
    matched=false
    best_diff_lines=999999
    query_expected_file="$expected_file"

    candidates=("$expected_file")
    if [[ "$expected_file" == "$EXPECTED_DIR/${test_name}.out" ]]; then
        shopt -s nullglob
        for candidate in "$EXPECTED_DIR/${test_name}_"[0-9]*.out; do
            candidates+=("$candidate")
        done
        shopt -u nullglob
    fi

    for candidate in "${candidates[@]}"; do
        [[ -f "$candidate" ]] || continue

        # Use diff, ignoring trailing whitespace
        if diff -u -b "$candidate" "$output_file" > "$diff_file.tmp" 2>&1; then
            matched=true
            query_expected_file="$candidate"
            rm -f "$diff_file.tmp"
            break
        else
            # Track the closest match
            diff_lines=$(wc -l < "$diff_file.tmp")
            if [[ $diff_lines -lt $best_diff_lines ]]; then
                best_diff_lines=$diff_lines
                query_expected_file="$candidate"
                mv "$diff_file.tmp" "$diff_file"
            else
                rm -f "$diff_file.tmp"
            fi
        fi
    done

    read -r q_matched q_mismatched q_total < <(count_matching_queries "$query_expected_file" "$output_file" "$sql_file")
    if [[ "$matched" == true ]]; then
        q_matched="$q_total"
        q_mismatched=0
    fi
    TOTAL_QUERIES=$((TOTAL_QUERIES + q_total))
    QUERIES_MATCHED=$((QUERIES_MATCHED + q_matched))
    QUERIES_MISMATCHED=$((QUERIES_MISMATCHED + q_mismatched))

    if [[ "$matched" == true ]]; then
        printf "%-40s PASS  (%d queries)\n" "$test_name" "$q_total"
        PASSED=$((PASSED + 1))
        pass_list+=("$test_name")
        rm -f "$diff_file"
    else
        # Check if it was an error (connection refused, crash, etc.) vs just wrong output
        if grep -q "connection refused\|could not connect\|server closed the connection unexpectedly\|TIMEOUT" "$output_file" 2>/dev/null; then
            printf "%-40s ERROR (%d/%d queries matched)\n" "$test_name" "$q_matched" "$q_total"
            ERRORED=$((ERRORED + 1))
            error_list+=("$test_name")

            # If server crashed, try to restart it
            if [[ "$SKIP_SERVER" == false ]] && ! kill -0 "$SERVER_PID" 2>/dev/null; then
                if ! restart_server; then
                    break
                fi
            fi
        else
            printf "%-40s FAIL  (%d/%d queries matched, %d diff lines)\n" "$test_name" "$q_matched" "$q_total" "$best_diff_lines"
            FAILED=$((FAILED + 1))
            fail_list+=("$test_name")
        fi
    fi
done

echo ""
echo "=============================================="
echo "RESULTS SUMMARY"
echo "=============================================="
echo ""
echo "Test files:"
echo "  Total:   $TOTAL"
echo "  Passed:  $PASSED"
echo "  Failed:  $FAILED"
echo "  Errored: $ERRORED"

if [[ $TOTAL -gt 0 ]]; then
    pass_pct=$((PASSED * 100 / TOTAL))
    echo "  Pass rate: ${pass_pct}% ($PASSED / $TOTAL)"
fi

echo ""
echo "Individual queries:"
echo "  Total:     $TOTAL_QUERIES"
echo "  Matched:   $QUERIES_MATCHED"
echo "  Mismatched:$QUERIES_MISMATCHED"

if [[ $TOTAL_QUERIES -gt 0 ]]; then
    query_pct=$((QUERIES_MATCHED * 100 / TOTAL_QUERIES))
    echo "  Match rate: ${query_pct}% ($QUERIES_MATCHED / $TOTAL_QUERIES)"
fi

echo ""
echo "Results directory: $RESULTS_DIR"
echo "  output/  — actual test output"
echo "  diff/    — diffs for failed tests"

if [[ ${#pass_list[@]} -gt 0 ]]; then
    echo ""
    echo "PASSED TESTS (${#pass_list[@]}):"
    for t in "${pass_list[@]}"; do
        echo "  $t"
    done
fi

if [[ ${#fail_list[@]} -gt 0 ]]; then
    echo ""
    echo "FAILED TESTS (${#fail_list[@]}):"
    for t in "${fail_list[@]}"; do
        echo "  $t"
    done
fi

if [[ ${#error_list[@]} -gt 0 ]]; then
    echo ""
    echo "ERRORED TESTS (${#error_list[@]}):"
    for t in "${error_list[@]}"; do
        echo "  $t"
    done
fi

# Write machine-readable summary
cat > "$RESULTS_DIR/summary.json" <<EOF
{
  "tests": {
    "total": $TOTAL,
    "passed": $PASSED,
    "failed": $FAILED,
    "errored": $ERRORED,
    "pass_rate_pct": ${pass_pct:-0}
  },
  "queries": {
    "total": $TOTAL_QUERIES,
    "matched": $QUERIES_MATCHED,
    "mismatched": $QUERIES_MISMATCHED,
    "match_rate_pct": ${query_pct:-0}
  }
}
EOF

echo ""
echo "Machine-readable summary: $RESULTS_DIR/summary.json"
