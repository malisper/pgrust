#!/usr/bin/env bash
# Run the full regression files that contain extracted query-plan diffs.
#
# For the faster EXPLAIN-only suite, use scripts/run_explain_regression_suite.sh.
#
# This intentionally uses scripts/run_regression.sh instead of psql-ing the
# extracted EXPLAIN statements directly: many of those EXPLAINs depend on
# objects created earlier in their source regression file, plus normal harness
# dependencies such as create_index, create_misc, and create_aggregate.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PLAN_QUERY_SQL="$PGRUST_DIR/scripts/regression_plan_diff_queries_2026_05_04.sql"

RESULTS_DIR=""
TIMEOUT=120
COPY_DIFFS_DIR="/tmp/diffs"
COPY_DIFFS=true
SKIP_BUILD=false
UPSTREAM_SETUP=false
IGNORE_DEPS=false
LIST_ONLY=false
TEST_FILTERS=()

usage() {
    cat <<'EOF'
Usage: scripts/run_regression_plan_diff_queries.sh [options]

Runs every regression file that has a query-plan diff in
scripts/regression_plan_diff_queries_2026_05_04.sql, using the normal
regression harness so bootstrap and per-test dependencies are present.

Options:
  --results-dir DIR   Directory for combined runner output.
  --timeout SECS      Per-regression-file timeout passed through to the harness (default: 120).
  --copy-diffs DIR    Copy produced .diff files into DIR (default: /tmp/diffs).
  --no-copy-diffs     Do not copy produced .diff files into a shared directory.
  --test NAME         Run only one extracted source regression file; repeatable.
  --skip-build        Pass --skip-build to every underlying harness run.
  --upstream-setup    Use upstream test_setup.sql instead of pgrust bootstrap.
  --ignore-deps       Continue if an underlying dependency setup fails.
  --list              Print the extracted source regression files and exit.
  -h, --help          Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --results-dir) RESULTS_DIR="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --copy-diffs) COPY_DIFFS_DIR="$2"; COPY_DIFFS=true; shift 2 ;;
        --no-copy-diffs) COPY_DIFFS=false; shift ;;
        --test) TEST_FILTERS+=("$2"); shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --upstream-setup) UPSTREAM_SETUP=true; shift ;;
        --ignore-deps) IGNORE_DEPS=true; shift ;;
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

mapfile -t PLAN_TESTS < <(
    awk '
        /^\\echo ==== / {
            sub(/^\\echo ==== /, "");
            sub(/ ====$/, "");
            print;
        }
    ' "$PLAN_QUERY_SQL"
)

if [[ ${#PLAN_TESTS[@]} -eq 0 ]]; then
    echo "ERROR: no source regression files found in $PLAN_QUERY_SQL" >&2
    exit 1
fi

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
    RESULTS_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pgrust_plan_diff_regress.XXXXXX")"
fi

mkdir -p "$RESULTS_DIR/runs" "$RESULTS_DIR/logs" "$RESULTS_DIR/status" "$RESULTS_DIR/output" "$RESULTS_DIR/diff"
if [[ "$COPY_DIFFS" == true ]]; then
    mkdir -p "$COPY_DIFFS_DIR"
fi

SUMMARY_TSV="$RESULTS_DIR/plan_diff_runner.tsv"
printf 'test\tstatus\trc\tqueries_matched\tqueries_mismatched\tqueries_total\tdiff_lines\trun_dir\tlog\n' > "$SUMMARY_TSV"

echo "Plan query source: $PLAN_QUERY_SQL"
echo "Selected regression files: ${#SELECTED_TESTS[@]}"
echo "Combined results dir: $RESULTS_DIR"
if [[ "$COPY_DIFFS" == true ]]; then
    echo "Copying produced diffs to: $COPY_DIFFS_DIR"
fi

debug_built=false
release_built=false
failed=0
errored=0
timed_out=0
passed=0

for test_name in "${SELECTED_TESTS[@]}"; do
    run_dir="$RESULTS_DIR/runs/$test_name"
    log_file="$RESULTS_DIR/logs/$test_name.log"
    status_file="$run_dir/status/$test_name.status"
    output_file="$run_dir/output/$test_name.out"
    diff_file="$run_dir/diff/$test_name.diff"
    profile="debug"
    status="missing"
    q_matched=0
    q_mismatched=0
    q_total=0
    diff_lines=0
    rc=0

    case "$test_name" in
        alter_table|tablespace|triggers) profile="release" ;;
    esac

    args=(--test "$test_name" --results-dir "$run_dir" --timeout "$TIMEOUT" --jobs 1)
    if [[ "$UPSTREAM_SETUP" == true ]]; then
        args+=(--upstream-setup)
    fi
    if [[ "$IGNORE_DEPS" == true ]]; then
        args+=(--ignore-deps)
    fi
    if [[ "$SKIP_BUILD" == true ]] \
        || [[ "$profile" == "debug" && "$debug_built" == true ]] \
        || [[ "$profile" == "release" && "$release_built" == true ]]; then
        args+=(--skip-build)
    fi

    echo "==> $test_name ($profile)"
    set +e
    "$PGRUST_DIR/scripts/run_regression.sh" "${args[@]}" > "$log_file" 2>&1
    rc=$?
    set -e

    if [[ "$profile" == "debug" ]]; then
        debug_built=true
    else
        release_built=true
    fi

    if [[ -f "$status_file" ]]; then
        IFS=$'\t' read -r status _ q_matched q_mismatched q_total diff_lines < "$status_file"
        cp "$status_file" "$RESULTS_DIR/status/$test_name.status"
    else
        status="error"
    fi

    if [[ -f "$output_file" ]]; then
        cp "$output_file" "$RESULTS_DIR/output/$test_name.out"
    fi
    if [[ -f "$diff_file" ]]; then
        cp "$diff_file" "$RESULTS_DIR/diff/$test_name.diff"
        if [[ "$COPY_DIFFS" == true ]]; then
            cp "$diff_file" "$COPY_DIFFS_DIR/$test_name.diff"
        fi
    fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$test_name" "$status" "$rc" "$q_matched" "$q_mismatched" "$q_total" "$diff_lines" "$run_dir" "$log_file" \
        >> "$SUMMARY_TSV"

    case "$status" in
        pass) passed=$((passed + 1)) ;;
        fail) failed=$((failed + 1)) ;;
        timeout) timed_out=$((timed_out + 1)) ;;
        *) errored=$((errored + 1)) ;;
    esac
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
