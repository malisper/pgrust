#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

MODE="sample"
PROFILE_OUT=""
SAMPLE_SECONDS=15
USTACKFRAMES=100
PROFILE_HZ=997
SKIP_BUILD=false
DRY_RUN=false
TEST_NAME=""
TEST_BIN=""
TEST_PID=""
DTRACE_PID=""
DTRACE_ERR=""

usage() {
    cat <<EOF
Usage: $0 [options] <exact-test-name>

Profiles a single Rust lib test by running the compiled lib test binary
directly, so cargo is not in the hot path while sampling.

Options:
  --mode MODE              Profiler to use: sample or dtrace (default: $MODE)
  --profile-out FILE       Output path for the profile (default: per-test file in /tmp)
  --seconds N              sample duration in seconds (default: $SAMPLE_SECONDS)
  --ustackframes N         dtrace ustackframes setting (default: $USTACKFRAMES)
  --profile-hz N           dtrace profile frequency (default: $PROFILE_HZ)
  --skip-build             Reuse the newest existing target/release lib test binary
  --dry-run                Print the resolved commands without running them
  -h, --help               Show this help

Examples:
  $0 pgrust::database::tests::pgbench_style_accounts_workload_completes
  $0 --mode dtrace --profile-out /tmp/pgbench.dtrace.txt \\
     pgrust::database::tests::pgbench_style_accounts_workload_completes
EOF
}

sanitize_name() {
    printf '%s' "$1" | tr -c '[:alnum:]' '_'
}

resolve_profile_out() {
    local suffix
    case "$MODE" in
        sample) suffix="sample.txt" ;;
        dtrace) suffix="dtrace.txt" ;;
        *)
            echo "unsupported mode: $MODE" >&2
            exit 1
            ;;
    esac
    if [[ -z "$PROFILE_OUT" ]]; then
        PROFILE_OUT="/tmp/$(sanitize_name "$TEST_NAME").$suffix"
    fi
}

discover_test_bin_from_build() {
    local cargo_json
    cargo_json="$(cd "$PGRUST_DIR" && cargo test --release --lib --no-run --message-format=json 2>/dev/null)"
    TEST_BIN="$(
        printf '%s\n' "$cargo_json" | python3 -c '
import json
import sys

exe = None
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        msg = json.loads(line)
    except json.JSONDecodeError:
        continue
    if msg.get("reason") != "compiler-artifact":
        continue
    if not msg.get("profile", {}).get("test"):
        continue
    target = msg.get("target", {})
    kinds = set(target.get("kind", []))
    if not {"lib", "rlib", "cdylib"} & kinds:
        continue
    executable = msg.get("executable")
    if executable:
        exe = executable

if not exe:
    sys.exit(1)
print(exe)
'
    )"
}

discover_existing_test_bin() {
    TEST_BIN="$(
        cd "$PGRUST_DIR" &&
            python3 -c '
from pathlib import Path
import sys

deps = Path("target/release/deps")
candidates = []
for path in deps.glob("pgrust-*"):
    if not path.is_file():
        continue
    if path.suffix in {".d", ".rlib", ".rmeta", ".o", ".a", ".dSYM"}:
        continue
    if not path.stat().st_mode & 0o111:
        continue
    candidates.append(path)

if not candidates:
    sys.exit(1)

candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
print(candidates[0])
'
    )"
}

wait_for_pid() {
    local pid="$1"
    for _ in $(seq 1 50); do
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

cleanup() {
    if [[ -n "$DTRACE_PID" ]] && kill -0 "$DTRACE_PID" 2>/dev/null; then
        kill -INT "$DTRACE_PID" 2>/dev/null || true
        wait "$DTRACE_PID" 2>/dev/null || true
    fi
    if [[ -n "$TEST_PID" ]] && kill -0 "$TEST_PID" 2>/dev/null; then
        kill "$TEST_PID" 2>/dev/null || true
        wait "$TEST_PID" 2>/dev/null || true
    fi
    if [[ -n "$DTRACE_ERR" ]]; then
        rm -f "$DTRACE_ERR"
    fi
}
trap cleanup EXIT

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode) MODE="$2"; shift 2 ;;
        --profile-out) PROFILE_OUT="$2"; shift 2 ;;
        --seconds) SAMPLE_SECONDS="$2"; shift 2 ;;
        --ustackframes) USTACKFRAMES="$2"; shift 2 ;;
        --profile-hz) PROFILE_HZ="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        -h|--help) usage; exit 0 ;;
        --)
            shift
            break
            ;;
        -*)
            echo "unknown flag: $1" >&2
            usage
            exit 1
            ;;
        *)
            if [[ -n "$TEST_NAME" ]]; then
                echo "unexpected extra argument: $1" >&2
                usage
                exit 1
            fi
            TEST_NAME="$1"
            shift
            ;;
    esac
done

if [[ -z "$TEST_NAME" ]] && [[ $# -gt 0 ]]; then
    TEST_NAME="$1"
    shift
fi

if [[ -z "$TEST_NAME" ]]; then
    usage
    exit 1
fi

case "$MODE" in
    sample)
        command -v sample >/dev/null 2>&1 || {
            echo "sample is required for --mode sample" >&2
            exit 1
        }
        ;;
    dtrace)
        command -v dtrace >/dev/null 2>&1 || {
            echo "dtrace is required for --mode dtrace" >&2
            exit 1
        }
        ;;
    *)
        echo "unsupported mode: $MODE" >&2
        exit 1
        ;;
esac

if [[ "$SKIP_BUILD" == true ]]; then
    discover_existing_test_bin || {
        echo "failed to find an existing target/release lib test binary; rerun without --skip-build" >&2
        exit 1
    }
else
    discover_test_bin_from_build || {
        echo "failed to resolve lib test binary from cargo output" >&2
        exit 1
    }
fi

if [[ ! -x "$TEST_BIN" ]]; then
    echo "resolved test binary is not executable: $TEST_BIN" >&2
    exit 1
fi

resolve_profile_out

TEST_CMD="\"$TEST_BIN\" \"$TEST_NAME\" --exact --nocapture"

if [[ "$MODE" == "sample" ]]; then
    PROFILE_CMD="sample <pid> $SAMPLE_SECONDS 1 -file \"$PROFILE_OUT\""
else
    PROFILE_CMD="sudo dtrace -q -x ustackframes=$USTACKFRAMES -n 'profile-$PROFILE_HZ /pid == \$target/ { @[ustack()] = count(); }' -p <pid> -o \"$PROFILE_OUT\""
fi

if [[ "$DRY_RUN" == true ]]; then
    echo "test binary: $TEST_BIN"
    echo "test command: $TEST_CMD"
    echo "profile command: $PROFILE_CMD"
    echo "profile output: $PROFILE_OUT"
    exit 0
fi

rm -f "$PROFILE_OUT"

"$TEST_BIN" "$TEST_NAME" --exact --nocapture &
TEST_PID=$!

if ! wait_for_pid "$TEST_PID"; then
    echo "test process exited before profiler could attach" >&2
    wait "$TEST_PID"
    exit 1
fi

test_status=0

case "$MODE" in
    sample)
        sample_status=0
        if ! sample "$TEST_PID" "$SAMPLE_SECONDS" 1 -file "$PROFILE_OUT"; then
            sample_status=$?
        fi
        wait "$TEST_PID" || test_status=$?
        if [[ $sample_status -ne 0 ]] && [[ ! -s "$PROFILE_OUT" ]]; then
            echo "sample failed and wrote no profile" >&2
            exit $sample_status
        fi
        ;;
    dtrace)
        if ! sudo -n true 2>/dev/null; then
            echo "Acquiring sudo for dtrace..."
            sudo -v
        fi
        DTRACE_ERR="$(mktemp /tmp/pgrust-dtrace-XXXX.log)"
        sudo -n dtrace -q -x "ustackframes=$USTACKFRAMES" -n "
profile-$PROFILE_HZ /pid == \$target/ { @[ustack()] = count(); }
" -p "$TEST_PID" -o "$PROFILE_OUT" 2>"$DTRACE_ERR" &
        DTRACE_PID=$!
        sleep 0.5
        if ! kill -0 "$DTRACE_PID" 2>/dev/null; then
            echo "dtrace exited before sampling began" >&2
            if [[ -s "$DTRACE_ERR" ]]; then
                cat "$DTRACE_ERR" >&2
            fi
            exit 1
        fi
        wait "$TEST_PID" || test_status=$?
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
        ;;
esac

echo "profile written to $PROFILE_OUT"
echo "test binary was $TEST_BIN"
exit "$test_status"
