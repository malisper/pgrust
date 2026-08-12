#!/usr/bin/env bash
# w4e-handverify: apply a deck to BOTH engines (A = in-lane cpg-ref
# REL_18_3, B = pgrust origin/main) in a single psql session each with
# VERBOSITY=verbose, then diff the LOCATION-stripped transcripts.
# Usage: fuzz/w4e-handverify.sh <deck.sql> <outdir>
set -u
REPO="$(cd "$(dirname "$0")/.." && pwd)"
DECK="${1:?deck}"; OUT="${2:?outdir}"
PGBIN="$REPO/cpg-ref/install/bin"
PGRUST_BIN="${PGRUST_BIN:-$REPO/target-w4err/debug/postgres}"
PORT_A="${W4E_PORT_A:-55981}"
PORT_B="${W4E_PORT_B:-55982}"
WORK="$(mktemp -d /tmp/w4e.XXXX)"
mkdir -p "$OUT"
trap 'pkill -9 -f "$WORK" 2>/dev/null; sleep 1; rm -rf "$WORK"' EXIT

"$PGBIN/initdb" -D "$WORK/dda" --no-locale --encoding=UTF8 -U postgres -A trust >"$WORK/ia.log" 2>&1 || { tail "$WORK/ia.log"; exit 1; }
"$PGBIN/initdb" -D "$WORK/ddb" --no-locale --encoding=UTF8 -U postgres -A trust >"$WORK/ib.log" 2>&1 || { tail "$WORK/ib.log"; exit 1; }
for tz in "$PGBIN/../share/postgresql/timezone"; do
    [ -d "$tz" ] && export PGRUST_TZDIR="$tz" && export PGRUST_PGSHAREDIR="$(dirname "$tz")"
done
ulimit -s 65520 2>/dev/null
mkdir -p "$WORK/ska" "$WORK/skb"
( exec "$PGBIN/postgres" -D "$WORK/dda" -k "$WORK/ska" -p "$PORT_A" \
    -c listen_addresses=127.0.0.1 -c autovacuum=off >"$WORK/a.log" 2>&1 ) &
( RUST_MIN_STACK=67108864 RUST_BACKTRACE=1 \
  exec "$PGRUST_BIN" -D "$WORK/ddb" -k "$WORK/skb" -p "$PORT_B" \
    -c listen_addresses=127.0.0.1 -c autovacuum=off \
    -c max_stack_depth=60000 -c io_method=sync >"$WORK/b.log" 2>&1 ) &
for i in $(seq 1 60); do
    "$PGBIN/psql" -h 127.0.0.1 -p "$PORT_A" -U postgres -X -c 'SELECT 1' >/dev/null 2>&1 && break; sleep 0.5
done
for i in $(seq 1 60); do
    "$PGBIN/psql" -h 127.0.0.1 -p "$PORT_B" -U postgres -X -c 'SELECT 1' >/dev/null 2>&1 && break; sleep 0.5
done
"$PGBIN/psql" -h 127.0.0.1 -p "$PORT_A" -U postgres -X -c 'CREATE DATABASE fuzz' >/dev/null 2>&1
"$PGBIN/psql" -h 127.0.0.1 -p "$PORT_B" -U postgres -X -c 'CREATE DATABASE fuzz' >/dev/null 2>&1

PIN='SET parallel_setup_cost = 1000; SET parallel_tuple_cost = 0.1; SET max_parallel_workers_per_gather = 2; SET jit_above_cost = 100000; SET TimeZone = '\''UTC'\''; SET DateStyle = '\''ISO, MDY'\''; SET IntervalStyle = '\''postgres'\'';'

run_side() { # port outfile
    { echo '\set VERBOSITY verbose'; echo '\set ECHO queries'; echo "$PIN"; cat "$DECK"; } | \
    "$PGBIN/psql" -h 127.0.0.1 -p "$1" -U postgres -X -d fuzz > "$2" 2>&1
}
run_side "$PORT_A" "$OUT/a.txt"
run_side "$PORT_B" "$OUT/b.txt"
# identity witness
"$PGBIN/psql" -h 127.0.0.1 -p "$PORT_A" -U postgres -X -qAt -c 'SELECT version()' > "$OUT/version-a.txt"
"$PGBIN/psql" -h 127.0.0.1 -p "$PORT_B" -U postgres -X -qAt -c 'SELECT version()' > "$OUT/version-b.txt"
grep -v '^LOCATION:' "$OUT/a.txt" > "$OUT/a.stripped"
grep -v '^LOCATION:' "$OUT/b.txt" > "$OUT/b.stripped"
diff -u "$OUT/a.stripped" "$OUT/b.stripped" > "$OUT/diff.txt"
echo "diff lines: $(grep -c '^[+-]' "$OUT/diff.txt" || true)  (see $OUT/diff.txt)"
