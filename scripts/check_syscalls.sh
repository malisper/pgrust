#!/usr/bin/env bash
# check_syscalls.sh — verify WAL syscall behaviour.
#
# Runs wal_syscall_check under a syscall tracer and asserts:
#
#   PASS  fdatasync is called exactly once per committed DML statement
#         (one WAL flush per commit).
#   PASS  fdatasync is NOT called on heap data files — all data-page
#         writes go through the OS page cache; WAL provides durability.
#
# Linux:  uses strace(1) — no elevated privileges required.
#         strace -y annotates each fd with its path, e.g.:
#             fdatasync(5</tmp/pgrust.../pg_wal/wal.log>) = 0
#
# macOS:  uses dtruss(1) — requires sudo.
#         dtruss does not show paths; we count total fdatasync calls
#         instead of filtering by file path.
#
# Usage:
#   ./scripts/check_syscalls.sh              # auto-detect platform
#   ./scripts/check_syscalls.sh --linux      # force strace path
#   ./scripts/check_syscalls.sh --macos      # force dtruss path

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$("$ROOT_DIR/scripts/cargo_target_dir.sh")/debug/wal_syscall_check"
EXPECTED_DML_COMMITS=5   # INSERT×3, UPDATE×1, DELETE×1

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

pass() { echo -e "${GREEN}  PASS${NC}  $*"; }
fail() { echo -e "${RED}  FAIL${NC}  $*"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "       $*"; }
header() { echo -e "\n${YELLOW}=== $* ===${NC}"; }

FAILURES=0

# ── Build ─────────────────────────────────────────────────────────────────────
header "Build"
if ! (cd "$ROOT_DIR" && cargo build --bin wal_syscall_check 2>&1); then
    echo "Build failed" >&2; exit 1
fi
pass "binary built: $BINARY"

# ── Platform detection ────────────────────────────────────────────────────────
PLATFORM="${1:-auto}"
if [[ "$PLATFORM" == "auto" ]]; then
    case "$(uname)" in
        Linux)  PLATFORM="linux"  ;;
        Darwin) PLATFORM="macos"  ;;
        *)      echo "Unknown platform: $(uname)" >&2; exit 1 ;;
    esac
fi

# ── Run under tracer ──────────────────────────────────────────────────────────
header "Syscall trace ($PLATFORM)"

RAW_TRACE=""

if [[ "$PLATFORM" == "linux" ]]; then
    if ! command -v strace &>/dev/null; then
        echo "strace not found — install with: apt-get install strace" >&2
        exit 1
    fi
    info "Running: strace -e trace=fdatasync,fsync -y $BINARY"
    # strace writes the trace to stderr; the binary's output goes to stdout.
    # Capture strace's stderr while discarding the binary's stdout.
    RAW_TRACE=$(strace -e trace=fdatasync,fsync -y "$BINARY" 2>&1 >/dev/null)
    pass "strace completed"

elif [[ "$PLATFORM" == "macos" ]]; then
    if ! command -v dtruss &>/dev/null; then
        echo "dtruss not found (should be in /usr/bin/dtruss on macOS)" >&2
        exit 1
    fi
    info "Running: sudo dtruss -t fdatasync $BINARY"
    info "(sudo required for dtruss on macOS)"
    # dtruss writes trace to stderr; program output goes to stdout.
    RAW_TRACE=$(sudo dtruss -t fdatasync "$BINARY" 2>&1 >/dev/null || true)
    pass "dtruss completed"

else
    echo "Unknown platform mode: $PLATFORM (use auto, linux, or macos)" >&2
    exit 1
fi

# ── Parse and check ───────────────────────────────────────────────────────────
header "Assertions"

if [[ "$PLATFORM" == "linux" ]]; then
    # strace -y annotates fds with paths: fdatasync(N</path/to/file>) = 0
    # Split trace into WAL syncs vs data-file syncs by filename.

    WAL_LINES=$(echo "$RAW_TRACE" | grep -E 'fdatasync|fsync' | grep 'wal\.log' || true)
    DATA_LINES=$(echo "$RAW_TRACE" | grep -E 'fdatasync|fsync' | grep -v 'wal\.log' || true)

    WAL_COUNT=$(echo "$WAL_LINES" | grep -c 'fdatasync\|fsync' || true)
    DATA_COUNT=$(echo "$DATA_LINES" | grep -c 'fdatasync\|fsync' || true)

    info "WAL file (pg_wal/wal.log) fsyncs : $WAL_COUNT"
    if [[ -n "$WAL_LINES" ]]; then
        while IFS= read -r line; do
            info "  $line"
        done <<< "$WAL_LINES"
    fi

    info "Data file fsyncs               : $DATA_COUNT"
    if [[ -n "$DATA_LINES" ]]; then
        while IFS= read -r line; do
            info "  $line"
        done <<< "$DATA_LINES"
    fi

    if [[ "$WAL_COUNT" -eq "$EXPECTED_DML_COMMITS" ]]; then
        pass "WAL fsynced exactly $WAL_COUNT times (once per DML commit)"
    else
        fail "Expected $EXPECTED_DML_COMMITS WAL fsyncs, got $WAL_COUNT"
    fi

    if [[ "$DATA_COUNT" -eq 0 ]]; then
        pass "No fsyncs on heap data files — WAL provides durability"
    else
        fail "Expected 0 data-file fsyncs, got $DATA_COUNT"
        info "Data file syncs indicate skip_fsync is not being honoured"
        info "Lines:"
        while IFS= read -r line; do
            info "  $line"
        done <<< "$DATA_LINES"
    fi

elif [[ "$PLATFORM" == "macos" ]]; then
    # dtruss does not show file paths; count total fdatasync calls.
    # With WAL, ALL fdatasyncs come from WalWriter::flush() — one per commit.
    TOTAL=$(echo "$RAW_TRACE" | grep -c 'fdatasync' || true)

    info "Total fdatasync calls: $TOTAL (expected $EXPECTED_DML_COMMITS)"

    if [[ "$TOTAL" -eq "$EXPECTED_DML_COMMITS" ]]; then
        pass "fdatasync called exactly $TOTAL times (once per DML commit)"
        pass "No extra fsyncs — data pages deferred to OS page cache"
    else
        fail "Expected $EXPECTED_DML_COMMITS fdatasync calls, got $TOTAL"
        info "Raw trace lines:"
        echo "$RAW_TRACE" | grep 'fdatasync' | while IFS= read -r line; do
            info "  $line"
        done
    fi
fi

# ── Summary ───────────────────────────────────────────────────────────────────
header "Summary"
if [[ "$FAILURES" -eq 0 ]]; then
    echo -e "${GREEN}All checks passed.${NC}"
else
    echo -e "${RED}$FAILURES check(s) failed.${NC}"
    exit 1
fi
