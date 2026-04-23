#!/bin/bash
# Build the upstream `isolationtester` binary from a PostgreSQL source checkout.
#
# Usage: scripts/build_pg_isolation_tools.sh [--force]
#
# Idempotent: if the binary already exists, exits 0 immediately. Pass --force
# to rebuild anyway (useful after updating the postgres/ checkout).
#
# Postgres source discovery order (first match wins):
#   1. $PGRUST_POSTGRES_DIR
#   2. $REPO_ROOT/postgres        (sibling to pgrust)
#   3. $HOME/postgres
#   4. $HOME/src/postgres
#   5. $HOME/dev/postgres
#
# Build deps (first-time setup):
#   macOS:  brew install meson ninja bison flex
#   Ubuntu: sudo apt install meson ninja-build bison flex build-essential python3 pkg-config
#
# Output: $POSTGRES_DIR/build/src/test/isolation/isolationtester

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PGRUST_DIR/.." && pwd)"

FORCE=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=true; shift ;;
        -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown flag: $1" >&2; exit 1 ;;
    esac
done

resolve_postgres_dir() {
    local candidate
    # The 2-levels-up candidate handles pgrust-worktrees/<name>/ checkouts,
    # where $REPO_ROOT is pgrust-worktrees/ rather than pagerfreeglobal/.
    for candidate in \
        "${PGRUST_POSTGRES_DIR:-}" \
        "$REPO_ROOT/postgres" \
        "$PGRUST_DIR/../../postgres" \
        "$HOME/postgres" \
        "$HOME/src/postgres" \
        "$HOME/dev/postgres"
    do
        [[ -z "$candidate" ]] && continue
        if [[ -d "$candidate/src/test/isolation" ]]; then
            (cd "$candidate" && pwd)
            return 0
        fi
    done
    return 1
}

if ! POSTGRES_DIR="$(resolve_postgres_dir)"; then
    cat >&2 <<EOF
ERROR: could not find a PostgreSQL source checkout.
Looked in: \$PGRUST_POSTGRES_DIR, \$REPO_ROOT/postgres, \$HOME/postgres,
           \$HOME/src/postgres, \$HOME/dev/postgres.

Either clone postgres as a sibling of pgrust:
    git clone --depth 1 https://github.com/postgres/postgres.git $REPO_ROOT/postgres

Or set PGRUST_POSTGRES_DIR to an existing checkout:
    export PGRUST_POSTGRES_DIR=/path/to/postgres
EOF
    exit 1
fi

BUILD_DIR="$POSTGRES_DIR/build"
BINARY="$BUILD_DIR/src/test/isolation/isolationtester"

if [[ "$FORCE" == false && -x "$BINARY" ]]; then
    echo "isolationtester already built at: $BINARY"
    exit 0
fi

check_dep() {
    local name="$1"
    if ! command -v "$name" >/dev/null 2>&1; then
        echo "ERROR: missing build dependency: $name" >&2
        case "$(uname -s)" in
            Darwin)
                echo "Install with: brew install meson ninja bison flex" >&2 ;;
            Linux)
                echo "Install with: sudo apt install meson ninja-build bison flex build-essential python3 pkg-config" >&2 ;;
            *)
                echo "Install meson, ninja, bison, flex, a C compiler, perl, python3, pkg-config using your package manager." >&2 ;;
        esac
        exit 1
    fi
}

for dep in meson ninja bison flex perl python3 pkg-config cc; do
    check_dep "$dep"
done

echo "Building isolationtester from: $POSTGRES_DIR"

if [[ ! -f "$BUILD_DIR/build.ninja" ]]; then
    echo "Running meson setup (first time; this configures the tree)..."
    # Disable everything we don't need; we only want the frontend libpq +
    # isolationtester. `ssl` is a combo option, not a feature option, so it
    # takes `none`/`auto`/`openssl` rather than `disabled`.
    meson setup "$BUILD_DIR" "$POSTGRES_DIR" \
        --buildtype=release \
        -Dicu=disabled \
        -Dreadline=disabled \
        -Dzlib=disabled \
        -Dssl=none \
        -Dllvm=disabled
fi

echo "Compiling isolationtester (this builds libpq + fe_utils transitively)..."
meson compile -C "$BUILD_DIR" isolationtester

if [[ ! -x "$BINARY" ]]; then
    echo "ERROR: expected binary not produced at: $BINARY" >&2
    exit 1
fi

echo "Built: $BINARY"
