#!/bin/bash
# Run PostgreSQL regression tests against pgrust, executing one statement at a
# time inside a single psql session per test, with per-statement timings and a
# server-side statement timeout.
#
# Usage:
#   scripts/run_regression_one_by_one.sh [--port PORT]
#     [--skip-server] [--test TESTNAME]
#     [--results-dir DIR] [--upstream-setup]
#
# This variant differs from scripts/run_regression.sh by:
#   1. Splitting each .sql file into one-statement fragments
#   2. Running them sequentially through \i in a single psql session
#   3. Enabling \timing so every statement is timed
#   4. Applying statement_timeout = '5s' for every psql session

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

PORT=5433
SKIP_SERVER=false
SINGLE_TEST=""
RESULTS_DIR="/tmp/pgrust_regress_one_by_one"
WORKTREE_NAME="$(basename "$PGRUST_DIR")"
DATA_DIR="/tmp/pgrust_regress_one_by_one_data_${WORKTREE_NAME}"
SERVER_PID=""
USE_PGRUST_SETUP=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        --skip-server) SKIP_SERVER=true; shift ;;
        --test) SINGLE_TEST="$2"; shift 2 ;;
        --results-dir) RESULTS_DIR="$2"; shift 2 ;;
        --pgrust-setup) USE_PGRUST_SETUP=true; shift ;;
        --upstream-setup) USE_PGRUST_SETUP=false; shift ;;
        -h|--help)
            sed -n '1,17p' "$0"
            exit 0
            ;;
        *)
            echo "Unknown flag: $1"
            exit 1
            ;;
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

run_pgrust_setup_one_by_one() {
    if [[ "$USE_PGRUST_SETUP" != true ]]; then
        return 0
    fi

    PGRUST_SETUP_SQL="$PGRUST_DIR/scripts/test_setup_pgrust.sql"
    PGRUST_SETUP_OUT="$RESULTS_DIR/output/test_setup_pgrust.out"
    PGRUST_SETUP_RAW="$RESULTS_DIR/output_raw/test_setup_pgrust.out"
    PGRUST_SETUP_TIMINGS="$RESULTS_DIR/timings/test_setup_pgrust.tsv"
    PGRUST_SETUP_TMP="$RESULTS_DIR/tmp/test_setup_pgrust"

    if [[ ! -f "$PGRUST_SETUP_SQL" ]]; then
        echo "ERROR: pgrust setup file not found: $PGRUST_SETUP_SQL"
        exit 1
    fi

    echo "Running pgrust setup bootstrap one statement at a time..."
    if ! run_sql_one_by_one \
        "$PGRUST_SETUP_SQL" \
        "$PGRUST_SETUP_OUT" \
        "$PGRUST_SETUP_RAW" \
        "$PGRUST_SETUP_TIMINGS" \
        "$PGRUST_SETUP_TMP" \
        true >/dev/null; then
        :
    fi

    if grep -qi "statement timeout\|could not connect\|server closed the connection unexpectedly" "$PGRUST_SETUP_RAW"; then
        echo "ERROR: pgrust setup bootstrap failed"
        echo "See:"
        echo "  output:  $PGRUST_SETUP_OUT"
        echo "  raw:     $PGRUST_SETUP_RAW"
        echo "  timings: $PGRUST_SETUP_TIMINGS"
        exit 1
    fi
}

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

echo "Building pgrust_server (release)..."
(cd "$PGRUST_DIR" && cargo build --release --bin pgrust_server 2>&1) || {
    echo "ERROR: Build failed"
    exit 1
}

SERVER_BIN="$PGRUST_DIR/target/release/pgrust_server"
if [[ ! -x "$SERVER_BIN" ]]; then
    echo "ERROR: $SERVER_BIN not found after build."
    exit 1
fi

mkdir -p \
    "$RESULTS_DIR/output" \
    "$RESULTS_DIR/output_raw" \
    "$RESULTS_DIR/diff" \
    "$RESULTS_DIR/timings" \
    "$RESULTS_DIR/tmp"

if [[ "$SKIP_SERVER" == false ]]; then
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"

    if ! start_server; then
        echo "ERROR: Server did not become ready in time"
        exit 1
    fi
fi

export PGPASSWORD="x"
export PG_ABS_SRCDIR="$PG_REGRESS_ABS"
export PGTZ="America/Los_Angeles"
export PGDATESTYLE="Postgres, MDY"
setup_pg_regress_env
export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c statement_timeout=5s"
PG_ARGS=(-X -h 127.0.0.1 -p "$PORT" -U postgres -v "abs_srcdir=$PG_REGRESS_ABS")

split_sql_statements() {
    local sql_path="$1"
    local split_dir="$2"

    rm -rf "$split_dir"
    mkdir -p "$split_dir"

    perl -e '
        use strict;
        use warnings;

        my ($input_path, $out_dir) = @ARGV;
        open my $in, "<", $input_path or die "open $input_path: $!";

        my @current;
        my $in_copy_data = 0;
        my $count = 0;
        my $dollar_tag;

        sub write_stmt {
            my ($out_dir, $count_ref, $lines_ref) = @_;
            return if !@$lines_ref;
            $$count_ref++;
            my $path = sprintf("%s/%05d.sql", $out_dir, $$count_ref);
            open my $out, ">", $path or die "open $path: $!";
            print {$out} join("", @$lines_ref);
            close $out or die "close $path: $!";
        }

        sub update_dollar_quote_state {
            my ($line, $tag_ref) = @_;
            while ($line =~ /(\$[A-Za-z_][A-Za-z_0-9]*\$|\$\$)/g) {
                my $tag = $1;
                if (!defined $$tag_ref) {
                    $$tag_ref = $tag;
                } elsif ($tag eq $$tag_ref) {
                    undef $$tag_ref;
                }
            }
        }

        while (my $line = <$in>) {
            if ($in_copy_data) {
                push @current, $line;
                if ($line =~ /^\s*\\\.\s*$/) {
                    write_stmt($out_dir, \$count, \@current);
                    @current = ();
                    $in_copy_data = 0;
                }
                next;
            }

            if (!@current) {
                next if $line =~ /^\s*$/;
                next if $line =~ /^\s*--/;
            }

            push @current, $line;

            if ($line =~ /^\s*copy\b.*\bfrom\s+stdin\b.*;([[:space:]]*--.*)?[[:space:]]*$/i) {
                $in_copy_data = 1;
                next;
            }

            update_dollar_quote_state($line, \$dollar_tag);
            next if defined $dollar_tag;

            if ($line =~ /;([[:space:]]*--.*)?[[:space:]]*$/ || $line =~ /(^|[^\\])\\[[:alpha:]]/) {
                write_stmt($out_dir, \$count, \@current);
                @current = ();
            }
        }

        write_stmt($out_dir, \$count, \@current);
        close $in or die "close $input_path: $!";
        print "$count\n";
    ' "$sql_path" "$split_dir"
}

build_driver_script() {
    local split_dir="$1"
    local driver_path="$2"
    local start_idx="$3"
    local skipped_ids_path="$4"

    {
        echo "\\timing on"
        while IFS= read -r stmt_path; do
            stmt_name="$(basename "$stmt_path")"
            stmt_id="${stmt_name%.sql}"
            stmt_num=$((10#$stmt_id))
            if [[ -f "$skipped_ids_path" ]] && grep -qx "$stmt_id" "$skipped_ids_path"; then
                continue
            fi
            if [[ $stmt_num -lt $start_idx ]]; then
                echo "\\i ${stmt_path}"
                continue
            fi
            echo "\\echo __PGRUST_QUERY_BEGIN__ ${stmt_id}"
            echo "\\i ${stmt_path}"
            echo "\\echo __PGRUST_QUERY_END__ ${stmt_id}"
        done < <(find "$split_dir" -type f -name '*.sql' ! -name 'driver.sql' | sort)
    } > "$driver_path"
}

extract_clean_output_and_timings() {
    local raw_output="$1"
    local clean_output="$2"
    local timings_output="$3"

    perl -e '
        use strict;
        use warnings;

        my ($raw_path, $clean_path, $timings_path) = @ARGV;
        open my $in, "<", $raw_path or die "open $raw_path: $!";
        my $append = -e $clean_path;
        open my $clean, ($append ? ">>" : ">"), $clean_path or die "open $clean_path: $!";
        open my $timings, ($append ? ">>" : ">"), $timings_path or die "open $timings_path: $!";

        print {$timings} "query_id\tstatus\telapsed_ms\n" if !$append;

        my $current;
        my %timing = ();

        sub flush_query {
            my ($fh, $timing_ref, $query_id, $final_status) = @_;
            return if !defined $query_id;
            my $status = $timing_ref->{status} // $final_status // "ok";
            my $elapsed = defined $timing_ref->{elapsed_ms} ? $timing_ref->{elapsed_ms} : "";
            print {$fh} "$query_id\t$status\t$elapsed\n";
        }

        while (my $line = <$in>) {
            if ($line =~ /^__PGRUST_QUERY_BEGIN__\s+(\S+)/) {
                flush_query($timings, \%timing, $current, undef);
                $current = $1;
                %timing = ();
                next;
            }

            if ($line =~ /^__PGRUST_QUERY_END__\s+(\S+)/) {
                flush_query($timings, \%timing, $current, undef);
                $current = undef;
                %timing = ();
                next;
            }

            if (defined $current && $line =~ /^Time:\s+([0-9]+(?:\.[0-9]+)?)\s+ms\b/) {
                $timing{elapsed_ms} = $1;
                next;
            }

            if (defined $current && $line =~ /statement timeout/i) {
                $timing{status} = "timeout";
            } elsif (defined $current && $line =~ /^ERROR:/) {
                $timing{status} = "error" if !defined $timing{status};
            }

            next if $line =~ /^Time:\s+[0-9]+(?:\.[0-9]+)?\s+ms\b/;
            next if $line =~ /^\\timing on$/;
            next if $line =~ /^\\echo __PGRUST_QUERY_(?:BEGIN|END)__\s+\S+/;
            next if $line =~ /^__PGRUST_QUERY_(?:BEGIN|END)__\s+\S+/;
            next if $line =~ m{^\\i .*/(?:driver|[0-9]{5})\.sql$};
            print {$clean} $line;
        }

        flush_query($timings, \%timing, $current, "crash");

        close $in or die "close $raw_path: $!";
        close $clean or die "close $clean_path: $!";
        close $timings or die "close $timings_path: $!";
    ' "$raw_output" "$clean_output" "$timings_output"
}

normalize_regression_output() {
    local input_path="$1"
    local output_path="$2"

    perl -e '
        use strict;
        use warnings;

        my ($input_path, $output_path) = @ARGV;
        open my $in, "<", $input_path or die "open $input_path: $!";
        open my $out, ">", $output_path or die "open $output_path: $!";

        my @normalized;
        while (my $line = <$in>) {
            $line =~ s/\r$//;
            $line =~ s/[ \t]+$//;

            # The one-by-one harness executes temp statement files, so psql
            # prefixes errors with the temp file path. Strip that wrapper so the
            # output compares against upstream regression expectations.
            $line =~ s{^psql:[^:]+/[0-9]{5}\.sql:\d+:\s+}{};

            next if $line =~ /^\s*--/;
            push @normalized, $line;
        }

        my @collapsed;
        my $prev_blank = 1;
        for my $line (@normalized) {
            my $is_blank = $line =~ /^\s*$/;
            next if $is_blank && $prev_blank;
            push @collapsed, $line;
            $prev_blank = $is_blank ? 1 : 0;
        }
        pop @collapsed while @collapsed && $collapsed[-1] =~ /^\s*$/;

        print {$out} @collapsed;

        close $in or die "close $input_path: $!";
        close $out or die "close $output_path: $!";
    ' "$input_path" "$output_path"
}

run_sql_one_by_one() {
    local sql_path="$1"
    local clean_output="$2"
    local raw_output="$3"
    local timings_output="$4"
    local split_dir="$5"
    local on_error_stop="$6"

    local stmt_count
    stmt_count="$(split_sql_statements "$sql_path" "$split_dir")"
    : > "$clean_output"
    : > "$raw_output"
    rm -f "$timings_output"

    local skipped_ids_path="$split_dir/skipped_ids.txt"
    : > "$skipped_ids_path"
    local start_idx=1

    while [[ $start_idx -le $stmt_count ]]; do
        local driver_path="$split_dir/driver.sql"
        local chunk_raw="$split_dir/chunk_${start_idx}.raw"
        local chunk_clean="$split_dir/chunk_${start_idx}.clean"
        local chunk_timings="$split_dir/chunk_${start_idx}.tsv"

        rm -f "$chunk_raw" "$chunk_clean" "$chunk_timings"
        build_driver_script "$split_dir" "$driver_path" "$start_idx" "$skipped_ids_path"

        if [[ "$on_error_stop" == true ]]; then
            if ! psql "${PG_ARGS[@]}" -v ON_ERROR_STOP=1 -a -q -f "$driver_path" > "$chunk_raw" 2>&1; then
                :
            fi
        else
            if ! psql "${PG_ARGS[@]}" -a -q -f "$driver_path" > "$chunk_raw" 2>&1; then
                :
            fi
        fi

        extract_clean_output_and_timings "$chunk_raw" "$chunk_clean" "$chunk_timings"
        cat "$chunk_raw" >> "$raw_output"
        cat "$chunk_clean" >> "$clean_output"
        tail -n +2 "$chunk_timings" >> "$timings_output"

        local crash_id=""
        crash_id="$(awk -F'\t' 'NR > 1 && $2 == "crash" { print $1; exit }' "$chunk_timings")"
        if [[ -z "$crash_id" ]]; then
            break
        fi

        echo "$crash_id" >> "$skipped_ids_path"

        if [[ "$SKIP_SERVER" == false ]]; then
            if ! restart_server; then
                break
            fi
            if [[ "$on_error_stop" == true ]]; then
                break
            fi
            run_pgrust_setup_one_by_one
        else
            break
        fi

        start_idx=$((10#$crash_id + 1))
    done

    echo "$stmt_count"
}

run_pgrust_setup_one_by_one

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

pass_list=()
fail_list=()
error_list=()

echo ""
echo "Running ${#TEST_FILES[@]} regression tests one statement at a time..."
echo "=================================================================="
echo "Per-query statement_timeout: 5s"
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
            my $dollar_tag;

            my $update_dollar_quote_state = sub {
                my ($line, $tag_ref) = @_;
                while ($line =~ /(\$[A-Za-z_][A-Za-z_0-9]*\$|\$\$)/g) {
                    my $tag = $1;
                    if (!defined $$tag_ref) {
                        $$tag_ref = $tag;
                    } elsif ($tag eq $$tag_ref) {
                        undef $$tag_ref;
                    }
                }
            };

            for my $line (@$lines) {
                if ($in_copy_data) {
                    push @current, normalize_line($line);
                    if ($line =~ /^\s*\\\.\s*$/) {
                        push @stmts, [ @current ];
                        @current = ();
                        $in_copy_data = 0;
                    }
                    next;
                }

                if (!@current) {
                    next if $line =~ /^\s*$/;
                    next if $line =~ /^\s*--/;
                }

                push @current, normalize_line($line);

                if ($line =~ /^\s*copy\b.*\bfrom\s+stdin\b.*;([[:space:]]*--.*)?[[:space:]]*$/i) {
                    $in_copy_data = 1;
                    next;
                }

                $update_dollar_quote_state->($line, \$dollar_tag);
                next if defined $dollar_tag;

                if ($line =~ /;([[:space:]]*--.*)?[[:space:]]*$/ || $line =~ /(^|[^\\])\\[[:alpha:]]/) {
                    push @stmts, [ @current ];
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

summarize_timings() {
    local timings_file="$1"
    perl -F'\t' -lane '
        BEGIN {
            $total = 0;
            $timed = 0;
            $timeout = 0;
            $error = 0;
            $sum = 0.0;
            $max = "";
            $max_id = "";
        }

        next if $. == 1;
        $total++;
        my ($id, $status, $elapsed) = @F;
        $timeout++ if $status eq "timeout";
        $error++ if $status eq "error";
        if (defined $elapsed && $elapsed ne "") {
            $timed++;
            $sum += $elapsed;
            if ($max eq "" || $elapsed > $max) {
                $max = $elapsed;
                $max_id = $id;
            }
        }

        END {
            my $avg = $timed ? sprintf("%.3f", $sum / $timed) : "n/a";
            my $max_elapsed = $max eq "" ? "n/a" : sprintf("%.3f", $max);
            my $max_query = $max_id eq "" ? "n/a" : $max_id;
            print "$total $timed $timeout $error $avg $max_elapsed $max_query";
        }
    ' "$timings_file"
}

for sql_file in "${TEST_FILES[@]}"; do
    test_name="$(basename "$sql_file" .sql)"
    expected_file="$EXPECTED_DIR/${test_name}.out"
    output_file="$RESULTS_DIR/output/${test_name}.out"
    raw_output_file="$RESULTS_DIR/output_raw/${test_name}.out"
    diff_file="$RESULTS_DIR/diff/${test_name}.diff"
    timings_file="$RESULTS_DIR/timings/${test_name}.tsv"
    tmp_dir="$RESULTS_DIR/tmp/${test_name}"

    TOTAL=$((TOTAL + 1))

    if [[ ! -f "$expected_file" ]]; then
        printf "%-40s SKIP (no expected output)\n" "$test_name"
        TOTAL=$((TOTAL - 1))
        continue
    fi

    # :HACK: Some earlier regression files currently leave the cluster in a
    # state where shared setup fixtures are no longer visible to int2.sql.
    # Rehydrate the baseline cluster state before int2 so the one-by-one
    # harness reports int2 semantics instead of unrelated prior-suite fallout.
    if [[ "$USE_PGRUST_SETUP" == true && "$SKIP_SERVER" == false && "$test_name" == "int2" ]]; then
        cleanup
        rm -rf "$DATA_DIR"
        mkdir -p "$DATA_DIR"
        if ! start_server; then
            echo "ERROR: Server did not become ready in time"
            exit 1
        fi
        run_pgrust_setup_one_by_one
    fi

    stmt_count="$(run_sql_one_by_one \
        "$sql_file" \
        "$output_file" \
        "$raw_output_file" \
        "$timings_file" \
        "$tmp_dir" \
        false)"

    matched=false
    best_diff_lines=999999
    query_expected_file="$expected_file"
    normalized_output_file="$tmp_dir/normalized_actual.out"

    candidates=("$EXPECTED_DIR/${test_name}.out")
    shopt -s nullglob
    for candidate in "$EXPECTED_DIR/${test_name}_"[0-9]*.out; do
        candidates+=("$candidate")
    done
    shopt -u nullglob

    for candidate in "${candidates[@]}"; do
        [[ -f "$candidate" ]] || continue

        normalized_expected_file="$tmp_dir/normalized_expected.out"
        normalize_regression_output "$candidate" "$normalized_expected_file"
        normalize_regression_output "$output_file" "$normalized_output_file"

        if diff -u -b "$normalized_expected_file" "$normalized_output_file" > "$diff_file.tmp" 2>&1; then
            matched=true
            query_expected_file="$candidate"
            rm -f "$diff_file.tmp"
            break
        else
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
    read -r timing_total timing_timed timing_timeouts timing_errors timing_avg timing_max timing_max_query < <(summarize_timings "$timings_file")

    if [[ "$matched" == true ]]; then
        printf "%-40s PASS  (%d stmts, avg %sms, max %sms @ %s)\n" \
            "$test_name" "$stmt_count" "$timing_avg" "$timing_max" "$timing_max_query"
        PASSED=$((PASSED + 1))
        pass_list+=("$test_name")
        rm -f "$diff_file"
    else
        if grep -qi "connection refused\|could not connect\|server closed the connection unexpectedly\|statement timeout" "$raw_output_file" 2>/dev/null; then
            printf "%-40s ERROR (%d/%d queries matched, %d timeouts)\n" \
                "$test_name" "$q_matched" "$q_total" "$timing_timeouts"
            ERRORED=$((ERRORED + 1))
            error_list+=("$test_name")

            if [[ "$SKIP_SERVER" == false ]] && ! kill -0 "$SERVER_PID" 2>/dev/null; then
                if ! restart_server; then
                    break
                fi
            fi
        else
            printf "%-40s FAIL  (%d/%d queries matched, %d diff lines, max %sms @ %s)\n" \
                "$test_name" "$q_matched" "$q_total" "$best_diff_lines" "$timing_max" "$timing_max_query"
            FAILED=$((FAILED + 1))
            fail_list+=("$test_name")
        fi
    fi
done

echo ""
echo "=================================================================="
echo "RESULTS SUMMARY"
echo "=================================================================="
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
echo "Artifacts:"
echo "  Clean output: $RESULTS_DIR/output"
echo "  Raw output:   $RESULTS_DIR/output_raw"
echo "  Timings:      $RESULTS_DIR/timings"
echo "  Diffs:        $RESULTS_DIR/diff"

echo ""
if [[ ${#error_list[@]} -gt 0 ]]; then
    echo "Errored tests:"
    printf '  %s\n' "${error_list[@]}"
fi

if [[ ${#fail_list[@]} -gt 0 ]]; then
    echo "Failed tests:"
    printf '  %s\n' "${fail_list[@]}"
fi

if [[ $FAILED -gt 0 || $ERRORED -gt 0 ]]; then
    exit 1
fi
