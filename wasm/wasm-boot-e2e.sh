#!/usr/bin/env bash
# P5 wasm BOOT gate (wasm/p5-boot): the wasm32-wasip1 postgres binary boots a
# prebuilt datadir under wasmtime and passes a serial interpreter-only subset
# through --single, byte-compared against the NATIVE --single on an identical
# fresh datadir. The ladder (notes/wasm-boot-lane.md):
#   (2) starts under wasmtime with a preopened datadir;
#   (3) --single boot reaches the read-query loop (startup recovery + banner);
#   (4) serial subset — DDL, unique index, multi-row DML, catalog lookup,
#       int arithmetic, aggregates, ORDER BY, BEGIN/ROLLBACK — with stdout
#       byte-identical to native;
#   (5) two thrown-error statements (parse-analysis + executor-thrown
#       division by zero) unwind in-situ and the session answers afterwards;
#       the clean EOF exit itself is the ProcExitThread catch_unwind proof
#       (shutdown checkpoint runs, pg_controldata reports "shut down").
# Plus: the wasm-written datadir reboots under the NATIVE binary and the
# wasm-inserted rows read back (cross-target durability round trip).
#
# Prereqs: wasmtime >= 46 (exceptions proposal), C PostgreSQL 18 initdb,
# native pgrust binary at target/debug/postgres (or $PGRUST_NATIVE_BIN),
# wasm binary at target/wasm32-wasip1/debug/postgres.wasm (wasm/wasm-build.sh).
#
# Trap ledger (found the hard way):
#   * tz/share data must be COPIED with symlinks dereferenced (cp -RL):
#     homebrew's zoneinfo entries are absolute symlinks into the Cellar and
#     WASI's preopen sandbox refuses symlink escapes (EPERM, os error 63).
#   * wal_sync_method=fdatasync: wasmtime rejects O_DSYNC opens (the macOS
#     native default open_datasync fails the shutdown checkpoint's WAL open).
#   * max_stack_depth=60000 needs the 64MiB shadow stack wasm-build.sh links
#     with, and a raised wasmtime value stack (max-wasm-stack).
#   * USER env supplies the identity (no uids/passwd db on WASI).
set -u

REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
PGINSTALL="${PGINSTALL:-}"
for cand in "$PGINSTALL" /tmp/pgrust_pginstall/bin /opt/homebrew/bin; do
    [ -n "$cand" ] && [ -x "$cand/initdb" ] && PGBIN="$cand" && break
done
[ -n "${PGBIN:-}" ] || { echo "no PostgreSQL 18 initdb found (set PGINSTALL)"; exit 2; }
command -v wasmtime >/dev/null || { echo "wasmtime not installed"; exit 2; }

NATIVE_BIN="${PGRUST_NATIVE_BIN:-$REPO/target/debug/postgres}"
WASM_BIN="${PGRUST_WASM_BIN:-$REPO/target/wasm32-wasip1/debug/postgres.wasm}"
[ -x "$NATIVE_BIN" ] || { echo "no native binary at $NATIVE_BIN"; exit 2; }
[ -f "$WASM_BIN" ] || { echo "no wasm binary at $WASM_BIN (wasm/wasm-build.sh)"; exit 2; }

. "$REPO/wasm/lib/scratch.sh"
WORK="${WASM_BOOT_E2E_DIR:-$(scratch_datadir pgrust-fast-wasm-boot-e2e)}"
scratch_adopt "$WORK"
rm -rf "$WORK"; mkdir -p "$WORK"

for tz in "$PGBIN/../share/postgresql/timezone" "$PGBIN/../share/postgresql@18/timezone"; do
    [ -d "$tz" ] && TZSRC="$(dirname "$tz")" && break
done
[ -n "${TZSRC:-}" ] || { echo "no timezone share dir under $PGBIN/../share"; exit 2; }

# WASI preopens cannot follow symlinks out of the sandbox: dereference.
mkdir -p "$WORK/share"
cp -RL "$TZSRC/timezone" "$WORK/share/timezone"
[ -d "$TZSRC/timezonesets" ] && cp -RL "$TZSRC/timezonesets" "$WORK/share/timezonesets"

ulimit -s 65520 2>/dev/null
export PGRUST_RUNTIME=0
export RUST_MIN_STACK=67108864

GUCS=(-c max_stack_depth=60000 -c io_method=sync -c autovacuum=off -c wal_sync_method=fdatasync)

fail=0
miss() { echo "FAIL: $*"; fail=1; }

cat > "$WORK/subset.sql" <<'SQL'
CREATE TABLE wb_tenk (
    unique1 int4 NOT NULL,
    unique2 int4,
    stringu1 name,
    even int4
);

INSERT INTO wb_tenk
SELECT i, 9 - i, 'row' || i, (i % 2) * 2
FROM generate_series(0, 9) AS g(i);

CREATE UNIQUE INDEX wb_tenk_unique1 ON wb_tenk (unique1);

SELECT count(*) AS cnt FROM wb_tenk;

SELECT 1 + 2 * 3 AS arith, (7 % 3)::int2 AS modw, -5 / 2 AS trunca;

SELECT relname, relkind FROM pg_class WHERE relname = 'wb_tenk';

SELECT t.unique1, t.stringu1 FROM wb_tenk t WHERE t.unique1 < 3 ORDER BY t.unique1;

SELECT sum(unique1) AS s, min(unique2) AS mn, max(unique2) AS mx FROM wb_tenk;

SELECT no_such_column FROM wb_tenk;

SELECT 1 / 0 AS boom;

SELECT 'alive after errors' AS marker;

BEGIN;

UPDATE wb_tenk SET even = even + 1 WHERE unique1 = 0;

ROLLBACK;

SELECT even FROM wb_tenk WHERE unique1 = 0;

SQL

echo "=== initdb x2 (C PostgreSQL 18) ==="
"$PGBIN/initdb" -D "$WORK/dd-native" --no-locale --encoding=UTF8 -U postgres -A trust >"$WORK/initdb1.log" 2>&1 || { echo "initdb failed"; exit 2; }
"$PGBIN/initdb" -D "$WORK/dd-wasm"   --no-locale --encoding=UTF8 -U postgres -A trust >"$WORK/initdb2.log" 2>&1 || { echo "initdb failed"; exit 2; }

echo "=== native --single -j subset ==="
PGRUST_TZDIR="$TZSRC/timezone" PGRUST_PGSHAREDIR="$TZSRC" \
    "$NATIVE_BIN" --single -j "${GUCS[@]}" -D "$WORK/dd-native" postgres \
    < "$WORK/subset.sql" > "$WORK/native.out" 2> "$WORK/native.err"
[ $? -eq 0 ] || miss "native --single exited nonzero"

echo "=== wasm --single -j subset (wasmtime) ==="
wasmtime run -W exceptions=y,max-wasm-stack=16777216 \
    --dir "$WORK::/work" \
    --env USER=postgres \
    --env PGRUST_TZDIR=/work/share/timezone \
    --env PGRUST_PGSHAREDIR=/work/share \
    --env PGRUST_RUNTIME=0 \
    --env RUST_BACKTRACE=1 \
    "$WASM_BIN" --single -j "${GUCS[@]}" -D /work/dd-wasm postgres \
    < "$WORK/subset.sql" > "$WORK/wasm.out" 2> "$WORK/wasm.err"
[ $? -eq 0 ] || miss "wasm --single exited nonzero"

if diff -u "$WORK/native.out" "$WORK/wasm.out" > "$WORK/out.diff"; then
    echo "stdout byte-identical (native vs wasm)"
else
    miss "stdout differs (see $WORK/out.diff)"
fi

grep -q 'ERROR:  column "no_such_column" does not exist' "$WORK/wasm.err" \
    || miss "wasm: parse-analysis error unwind not observed"
grep -q 'ERROR:  division by zero' "$WORK/wasm.err" \
    || miss "wasm: executor-thrown error unwind not observed"
grep -q 'checkpoint complete' "$WORK/wasm.err" \
    || miss "wasm: no shutdown checkpoint in the log"
if grep -q 'panicked' "$WORK/wasm.err"; then
    miss "wasm: panic lines in stderr"
fi

"$PGBIN/pg_controldata" "$WORK/dd-wasm" | grep -q 'shut down' \
    || miss "wasm datadir not in 'shut down' state"
[ -f "$WORK/dd-wasm/postmaster.pid" ] && miss "wasm: postmaster.pid not removed"

echo "=== cross-boot: native binary reads the wasm-written datadir ==="
CROSS=$(echo "SELECT count(*) AS wasm_written_rows FROM wb_tenk;" | \
    PGRUST_TZDIR="$TZSRC/timezone" PGRUST_PGSHAREDIR="$TZSRC" \
    "$NATIVE_BIN" --single "${GUCS[@]}" -D "$WORK/dd-wasm" postgres 2>"$WORK/cross.err")
echo "$CROSS" | grep -q 'wasm_written_rows = "10"' \
    || miss "cross-boot readback missed (wanted 10 rows)"

if [ "$fail" -eq 0 ]; then
    echo "VERDICT: wasm-boot-e2e PASS"
    exit 0
fi
echo "VERDICT: wasm-boot-e2e FAIL"
exit 1
