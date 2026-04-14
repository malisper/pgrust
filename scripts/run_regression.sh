#!/bin/bash
# Run PostgreSQL regression tests against pgrust and report pass/fail statistics.
#
# Usage: scripts/run_regression.sh [--port PORT] [--skip-build] [--skip-server] [--timeout SECS] [--test TESTNAME] [--upstream-setup]
#
# By default, this script:
#   1. Builds pgrust_server in release mode
#   2. Starts it on a fresh data directory
#   3. Runs each .sql regression test via psql
#   4. Compares output against expected .out files
#   5. Reports pass/fail/error statistics
#
# Options:
#   --port PORT       Port for pgrust server (default: 5433)
#   --skip-build      Don't rebuild pgrust_server
#   --skip-server     Assume server is already running (don't start/stop it)
#   --timeout SECS    Per-test timeout in seconds (default: 30)
#   --test TESTNAME   Run only this test (without .sql extension)
#   --results-dir DIR Directory for results (default: /tmp/pgrust_regress)
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

PORT=5433
SKIP_BUILD=false
SKIP_SERVER=false
TIMEOUT=30
SINGLE_TEST=""
RESULTS_DIR="/tmp/pgrust_regress"
DATA_DIR="/tmp/pgrust_regress_data"
SERVER_PID=""
USE_PGRUST_SETUP=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --skip-server) SKIP_SERVER=true; shift ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --test) SINGLE_TEST="$2"; shift 2 ;;
        --results-dir) RESULTS_DIR="$2"; shift 2 ;;
        --pgrust-setup) USE_PGRUST_SETUP=true; shift ;;
        --upstream-setup) USE_PGRUST_SETUP=false; shift ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Stopping pgrust server (PID $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

wait_for_server_ready() {
    local pid="$1"

    echo "Waiting for server to accept connections..."
    for i in $(seq 1 30); do
        if psql -X -h 127.0.0.1 -p "$PORT" -U postgres -c "SELECT 1" >/dev/null 2>&1; then
            echo "Server ready."
            return 0
        fi
        if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
            return 1
        fi
        sleep 0.5
    done

    psql -X -h 127.0.0.1 -p "$PORT" -U postgres -c "SELECT 1" >/dev/null 2>&1
}

start_server() {
    echo "Starting pgrust server on port $PORT (data: $DATA_DIR)..."
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

    if ! start_server; then
        echo "  -> Restart failed; aborting run to avoid contaminating later results."
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

SERVER_BIN="$PGRUST_DIR/target/release/pgrust_server"
if [[ ! -x "$SERVER_BIN" ]]; then
    echo "ERROR: $SERVER_BIN not found. Run without --skip-build."
    exit 1
fi

# Set up results directory
mkdir -p "$RESULTS_DIR/output" "$RESULTS_DIR/diff"

# Start pgrust server
if [[ "$SKIP_SERVER" == false ]]; then
    # Fresh data directory for each run
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"

    if ! start_server; then
        echo "ERROR: Server did not become ready in time"
        exit 1
    fi
fi

export PGPASSWORD="x"
export PG_ABS_SRCDIR="$PG_REGRESS_ABS"
PG_ARGS=(-X -h 127.0.0.1 -p "$PORT" -U postgres -v "abs_srcdir=$PG_REGRESS_ABS")

if [[ "$USE_PGRUST_SETUP" == true ]]; then
    PGRUST_SETUP_SQL="$PGRUST_DIR/scripts/test_setup_pgrust.sql"
    PGRUST_SETUP_OUT="$RESULTS_DIR/output/test_setup_pgrust.out"

    if [[ ! -f "$PGRUST_SETUP_SQL" ]]; then
        echo "ERROR: pgrust setup file not found: $PGRUST_SETUP_SQL"
        exit 1
    fi

    echo "Running pgrust setup bootstrap..."
    if [[ -n "$TIMEOUT_CMD" ]]; then
        if ! $TIMEOUT_CMD "$TIMEOUT" psql "${PG_ARGS[@]}" -v ON_ERROR_STOP=1 -a -q < "$PGRUST_SETUP_SQL" > "$PGRUST_SETUP_OUT" 2>&1; then
            echo "ERROR: pgrust setup bootstrap failed"
            echo "See: $PGRUST_SETUP_OUT"
            exit 1
        fi
    else
        if ! psql "${PG_ARGS[@]}" -v ON_ERROR_STOP=1 -a -q < "$PGRUST_SETUP_SQL" > "$PGRUST_SETUP_OUT" 2>&1; then
            echo "ERROR: pgrust setup bootstrap failed"
            echo "See: $PGRUST_SETUP_OUT"
            exit 1
        fi
    fi
fi

# Collect test files
if [[ -n "$SINGLE_TEST" ]]; then
    TEST_FILES=("$SQL_DIR/${SINGLE_TEST}.sql")
    if [[ ! -f "${TEST_FILES[0]}" ]]; then
        echo "ERROR: Test file not found: ${TEST_FILES[0]}"
        exit 1
    fi
else
    TEST_FILES=("$SQL_DIR"/*.sql)
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

SKIPPED_HANGING_TESTS=("join" "join_hash" "memoize" "subselect" "tablespace")

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

    if [[ " ${SKIPPED_HANGING_TESTS[*]} " == *" ${test_name} "* ]]; then
        # :HACK: `join.sql` currently contains pathological queries that can
        # spin indefinitely in pgrust. Record the test as a full mismatch so
        # the regression summary completes while executor work continues.
        {
            echo "SKIPPED: known hanging regression test in pgrust"
            echo "Test file: $sql_file"
        } > "$output_file"
        read -r q_matched q_mismatched q_total < <(count_matching_queries "$expected_file" /dev/null "$sql_file")
        TOTAL_QUERIES=$((TOTAL_QUERIES + q_total))
        QUERIES_MATCHED=$((QUERIES_MATCHED + q_matched))
        QUERIES_MISMATCHED=$((QUERIES_MISMATCHED + q_mismatched))
        printf "%-40s FAIL  (%d/%d queries matched, skipped known hang)\n" "$test_name" "$q_matched" "$q_total"
        FAILED=$((FAILED + 1))
        fail_list+=("$test_name")
        continue
    fi

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

    candidates=("$EXPECTED_DIR/${test_name}.out")
    shopt -s nullglob
    for candidate in "$EXPECTED_DIR/${test_name}_"[0-9]*.out; do
        candidates+=("$candidate")
    done
    shopt -u nullglob

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
