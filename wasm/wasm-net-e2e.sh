#!/usr/bin/env bash
# P5 wasm NET gate (wasm/p5-net-seam): the postgres binary serves one full
# pgwire session over the stdio transport provider (§2.4 transport seam,
# docs/design/dst-and-wasm.md) — startup handshake, simple-query protocol,
# clean Terminate — on BOTH targets, with the canonical response transcripts
# byte-identical:
#   (1) native --stdio-wire: the differential arm (same binary the socket
#       path ships in; the stdio provider only installs under this switch);
#   (2) wasm  --stdio-wire under wasmtime: the host driver pumps pgwire
#       bytes through the preopened stdio pipes (WASI p1 has no socket()).
# The driver (wasm/pgwire_stdio_driver.py) checks AuthenticationOk +
# BackendKeyData + ReadyForQuery, runs SELECT 1 / DDL / DML / SELECT / an
# ERROR statement / BEGIN-UPDATE-ROLLBACK, sends Terminate, and requires
# exit 0. Post-exit: pg_controldata says "shut down" (the proc_exit shutdown
# checkpoint ran) and no panic lines in the wasm log.
#
# Prereqs: same as wasm/wasm-boot-e2e.sh (wasmtime >= 46, C PostgreSQL 18
# initdb, native binary, postgres.wasm from wasm/wasm-build.sh); the trap
# ledger there (cp -RL tz share, wal_sync_method=fdatasync, 64MiB stacks,
# USER env) applies verbatim.
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
WORK="${WASM_NET_E2E_DIR:-$(scratch_datadir pgrust-fast-wasm-net-e2e)}"
scratch_adopt "$WORK"
rm -rf "$WORK"; mkdir -p "$WORK"

for tz in "$PGBIN/../share/postgresql/timezone" "$PGBIN/../share/postgresql@18/timezone"; do
    [ -d "$tz" ] && TZSRC="$(dirname "$tz")" && break
done
[ -n "${TZSRC:-}" ] || { echo "no timezone share dir under $PGBIN/../share"; exit 2; }

mkdir -p "$WORK/share"
cp -RL "$TZSRC/timezone" "$WORK/share/timezone"
[ -d "$TZSRC/timezonesets" ] && cp -RL "$TZSRC/timezonesets" "$WORK/share/timezonesets"

ulimit -s 65520 2>/dev/null
export PGRUST_RUNTIME=0
export RUST_MIN_STACK=67108864

# TimeZone pinned: it is a GUC_REPORT parameter (a ParameterStatus message in
# the transcript) and would otherwise differ native-vs-wasm.
GUCS=(-c max_stack_depth=60000 -c io_method=sync -c autovacuum=off
      -c wal_sync_method=fdatasync -c timezone=UTC -c log_timezone=UTC)

fail=0
miss() { echo "FAIL: $*"; fail=1; }

cat > "$WORK/wire.sql" <<'SQL'
SELECT 1
SELECT 1 + 2 * 3 AS arith, 'txt' || 'cat' AS cat
CREATE TABLE wn_t (id int4 PRIMARY KEY, v text)
INSERT INTO wn_t SELECT i, 'row' || i FROM generate_series(1, 5) g(i)
SELECT id, v FROM wn_t WHERE id < 4 ORDER BY id
SELECT count(*) AS n, sum(id) AS s FROM wn_t
SELECT no_such_col FROM wn_t
SELECT 'alive after error' AS marker
BEGIN
UPDATE wn_t SET v = 'patched' WHERE id = 1
ROLLBACK
SELECT v FROM wn_t WHERE id = 1
SQL

echo "=== initdb x2 (C PostgreSQL 18) ==="
"$PGBIN/initdb" -D "$WORK/dd-native" --no-locale --encoding=UTF8 -U postgres -A trust >"$WORK/initdb1.log" 2>&1 || { echo "initdb failed"; exit 2; }
"$PGBIN/initdb" -D "$WORK/dd-wasm"   --no-locale --encoding=UTF8 -U postgres -A trust >"$WORK/initdb2.log" 2>&1 || { echo "initdb failed"; exit 2; }

echo "=== native --stdio-wire session ==="
PGRUST_TZDIR="$TZSRC/timezone" PGRUST_PGSHAREDIR="$TZSRC" \
python3 "$REPO/wasm/pgwire_stdio_driver.py" \
    --sql "$WORK/wire.sql" --stderr "$WORK/native.err" -- \
    "$NATIVE_BIN" --stdio-wire "${GUCS[@]}" -D "$WORK/dd-native" \
    > "$WORK/native.transcript"
[ $? -eq 0 ] || miss "native --stdio-wire driver failed (see $WORK/native.err)"

echo "=== wasm --stdio-wire session (wasmtime) ==="
python3 "$REPO/wasm/pgwire_stdio_driver.py" \
    --sql "$WORK/wire.sql" --stderr "$WORK/wasm.err" -- \
    wasmtime run -W exceptions=y,max-wasm-stack=16777216 \
    --dir "$WORK::/work" \
    --env USER=postgres \
    --env PGRUST_TZDIR=/work/share/timezone \
    --env PGRUST_PGSHAREDIR=/work/share \
    --env PGRUST_RUNTIME=0 \
    --env RUST_BACKTRACE=1 \
    "$WASM_BIN" --stdio-wire "${GUCS[@]}" -D /work/dd-wasm \
    > "$WORK/wasm.transcript"
[ $? -eq 0 ] || miss "wasm --stdio-wire driver failed (see $WORK/wasm.err)"

if diff -u "$WORK/native.transcript" "$WORK/wasm.transcript" > "$WORK/transcript.diff"; then
    echo "transcripts byte-identical (native vs wasm)"
else
    miss "transcripts differ (see $WORK/transcript.diff)"
fi

# Session-content spot checks (guards a driver bug that would pass an
# empty==empty diff).
grep -q '^R auth=0$'                "$WORK/wasm.transcript" || miss "no AuthenticationOk"
grep -q '^D 1$'                     "$WORK/wasm.transcript" || miss "SELECT 1 row missing"
grep -q '^C CREATE TABLE$'          "$WORK/wasm.transcript" || miss "CREATE TABLE tag missing"
grep -q '^C INSERT 0 5$'            "$WORK/wasm.transcript" || miss "INSERT tag missing"
grep -q '^D 5|15$'                  "$WORK/wasm.transcript" || miss "aggregate row missing"
grep -q 'no_such_col'               "$WORK/wasm.transcript" || miss "ERROR message missing"
grep -q '^D row1$'                  "$WORK/wasm.transcript" || miss "post-rollback row missing"
grep -q '^=== exit 0$'              "$WORK/wasm.transcript" || miss "wasm exit nonzero"

if grep -q 'panicked' "$WORK/wasm.err"; then
    miss "wasm: panic lines in stderr"
fi

"$PGBIN/pg_controldata" "$WORK/dd-wasm" | grep -q 'shut down' \
    || miss "wasm datadir not in 'shut down' state"
"$PGBIN/pg_controldata" "$WORK/dd-native" | grep -q 'shut down' \
    || miss "native datadir not in 'shut down' state"

if [ "$fail" -eq 0 ]; then
    echo "VERDICT: wasm-net-e2e PASS"
    exit 0
fi
echo "VERDICT: wasm-net-e2e FAIL"
exit 1
