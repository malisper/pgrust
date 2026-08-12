#!/usr/bin/env bash
# ld10-pair: boot the LD10 lane's A/B differential pair and run one
# diffrunner invocation against it. Mirrors scripts/diffpair.sh's boot
# (C initdb both sides, autovacuum off, io_method=sync + deep stack on B,
# shared tzdata via PGRUST_TZDIR) but with lane-unique ports and SHORT
# socket dirs (103-byte sockaddr limit — the LD3 trap).
#
#   fuzz/ld10-pair.sh [diffrunner args...]
#
# Env: LD10_KEEP=1 keeps the datadirs; PGRUST_BIN overrides the B binary
# (default: the lane's pinned origin/main copy, sha recorded next to it).
set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PGBIN="$REPO/cpg-ref/install/bin"
PGRUST_BIN="${PGRUST_BIN:-$REPO/pgrust-b-postgres}"
PORT_A=55951
PORT_B=55952
WORK="${LD10_WORK:-/tmp/ld10pair}"
DR="$REPO/target-ld10/debug/diffrunner"

die() { echo "ld10-pair: $*" >&2; exit 1; }

[ -x "$PGBIN/postgres" ] || die "no cpg-ref install (run scripts/pgref-build.sh)"
[ -x "$PGRUST_BIN" ] || die "PGRUST_BIN not executable: $PGRUST_BIN"
[ -x "$DR" ] || die "diffrunner not built (CARGO_TARGET_DIR=target-ld10 cargo build -p fuzzgen --bins)"

for p in $PORT_A $PORT_B; do
    if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
        die "port $p already in use (stale pair?)"
    fi
done

rm -rf "$WORK"; mkdir -p "$WORK/socka" "$WORK/sockb"
"$PGBIN/initdb" -D "$WORK/dda" --no-locale --encoding=UTF8 -U postgres -A trust \
    >"$WORK/initdb-a.log" 2>&1 || die "initdb A failed"
"$PGBIN/initdb" -D "$WORK/ddb" --no-locale --encoding=UTF8 -U postgres -A trust \
    >"$WORK/initdb-b.log" 2>&1 || die "initdb B failed"
for tz in "$PGBIN/../share/postgresql/timezone"; do
    [ -d "$tz" ] && export PGRUST_TZDIR="$tz" && export PGRUST_PGSHAREDIR="$(dirname "$tz")"
done
ulimit -s 65520 2>/dev/null

("$PGBIN/postgres" -D "$WORK/dda" -k "$WORK/socka" -p $PORT_A \
    -c listen_addresses=127.0.0.1 -c autovacuum=off \
    >>"$WORK/a.log" 2>&1) &
SRV_A=$!
(RUST_MIN_STACK=67108864 RUST_BACKTRACE=1 \
    "$PGRUST_BIN" -D "$WORK/ddb" -k "$WORK/sockb" -p $PORT_B \
    -c listen_addresses=127.0.0.1 -c autovacuum=off \
    -c max_stack_depth=60000 -c io_method=sync \
    >>"$WORK/b.log" 2>&1) &
SRV_B=$!
cleanup() {
    kill -INT "$SRV_A" "$SRV_B" 2>/dev/null
    for _ in $(seq 1 40); do
        kill -0 "$SRV_A" 2>/dev/null || kill -0 "$SRV_B" 2>/dev/null || break
        sleep 0.5
    done
    kill -9 "$SRV_A" "$SRV_B" 2>/dev/null
    [ -n "${LD10_KEEP:-}" ] || rm -rf "$WORK"
}
trap cleanup EXIT

wait_ready() {
    for _ in $(seq 1 60); do
        kill -0 "$2" 2>/dev/null || { tail -30 "$3" >&2; return 1; }
        "$PGBIN/psql" -h 127.0.0.1 -p "$1" -U postgres -X -c 'SELECT 1' >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    tail -30 "$3" >&2; return 1
}
wait_ready $PORT_A "$SRV_A" "$WORK/a.log" || die "A not ready"
wait_ready $PORT_B "$SRV_B" "$WORK/b.log" || die "B not ready"

# Version witness (provenance: the A side IS the conformance oracle).
"$PGBIN/psql" -h 127.0.0.1 -p $PORT_A -U postgres -X -qAt -c 'SELECT version()' | head -1
echo "ld10-pair: B sha $(cat "$REPO/pgrust-b-postgres.sha" 2>/dev/null || echo UNKNOWN)"

"$PGBIN/psql" -h 127.0.0.1 -p $PORT_A -U postgres -X -qc 'CREATE DATABASE fuzz' || die "createdb A"
"$PGBIN/psql" -h 127.0.0.1 -p $PORT_B -U postgres -X -qc 'CREATE DATABASE fuzz' || die "createdb B"

"$DR" --a 127.0.0.1:$PORT_A --b 127.0.0.1:$PORT_B --user postgres "$@"
rc=$?
exit $rc
