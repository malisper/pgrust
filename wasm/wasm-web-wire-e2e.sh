#!/usr/bin/env bash
# WASM-NEXT protocol-mode gate: the browser worker's WIRE engine — ONE
# long-lived `postgres --stdio-wire` wasm instance spoken to in pgwire frames
# over the stdin/stdout host pipes via JSPI (wasm/wiresession.js) —
# answers a MULTI-STATEMENT SESSION battery with the canonical transcript
# byte-identical to the NATIVE --stdio-wire session (scripts/
# pgwire_stdio_driver.py, the wasm-net-e2e driver).
#
# What the battery proves that --single-per-statement cannot: SESSION state
# spans REPL lines — a temp table created in one statement is read in later
# ones, a prepared statement EXECUTEs lines later, BEGIN/UPDATE/ROLLBACK
# spans three statements — plus error-then-recovery inside the one session.
# The tail is the reviewer's adversarial leg (promoted from the inc1 review's
# one-off battery): savepoints, aborted-transaction `Z E` state across lines
# ("current transaction is aborted" on the follow-up), duplicate-key error +
# ROLLBACK TO SAVEPOINT recovery, a second transaction, DEALLOCATE ALL.
# The wasm arm is INTERACTIVE (each Q sent only after the previous
# ReadyForQuery; the guest suspends on its blocking stdin read in between),
# exercising the exact suspend/resume path the browser REPL uses.
#
# Checks:
#   1. wasm/build.sh assembles assets (initdb + pack-vfs);
#   2. native --stdio-wire (python driver) and wasm-under-node
#      (run-node-wire.mjs over wiresession.js) canonical transcripts are
#      byte-identical;
#   3. ONE handshake in the transcript (one session, not per-statement boots);
#   4. session-state spot checks (temp table rows, EXECUTE rows, rollback);
#   5. clean Terminate: exit 0, no leftover bytes, both datadirs report
#      "shut down" in pg_controldata, no panic lines in wasm stderr.
#
# Prereqs: node >= 24 class with JSPI (WebAssembly.Suspending), python3,
# C PostgreSQL 18 initdb (PGINSTALL or /opt/homebrew/bin), native binary
# (PGRUST_NATIVE_BIN or target/debug/postgres), wasm module (PGRUST_WASM_BIN
# or target/wasm32-wasip1/{wasm-release,debug}/postgres.wasm).
set -u

REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
WEB="$REPO/wasm"

command -v node >/dev/null || { echo "node not installed"; exit 2; }
command -v python3 >/dev/null || { echo "python3 not installed"; exit 2; }
node -e 'process.exit(typeof WebAssembly.Suspending === "function" && typeof WebAssembly.promising === "function" ? 0 : 1)' \
    || { echo "this node has no JSPI (WebAssembly.Suspending/promising)"; exit 2; }

PGBIN=""
for cand in "${PGINSTALL:-}" /tmp/pgrust_pginstall/bin /opt/homebrew/bin; do
    [ -n "$cand" ] && [ -x "$cand/initdb" ] && PGBIN="$cand" && break
done
[ -n "$PGBIN" ] || { echo "no PostgreSQL 18 initdb found (set PGINSTALL)"; exit 2; }

NATIVE_BIN="${PGRUST_NATIVE_BIN:-$REPO/target/debug/postgres}"
[ -x "$NATIVE_BIN" ] || { echo "no native binary at $NATIVE_BIN"; exit 2; }
WASM_BIN="${PGRUST_WASM_BIN:-}"
if [ -z "$WASM_BIN" ]; then
    for cand in "$REPO/target/wasm32-wasip1/wasm-release/postgres.wasm" \
                "$REPO/target/wasm32-wasip1/debug/postgres.wasm"; do
        [ -f "$cand" ] && WASM_BIN="$cand" && break
    done
fi
[ -n "$WASM_BIN" ] && [ -f "$WASM_BIN" ] || { echo "no wasm binary (wasm/wasm-build.sh)"; exit 2; }

for tz in "$PGBIN/../share/postgresql/timezone" "$PGBIN/../share/postgresql@18/timezone"; do
    [ -d "$tz" ] && TZSRC="$(dirname "$tz")" && break
done
[ -n "${TZSRC:-}" ] || { echo "no timezone share dir under $PGBIN/../share"; exit 2; }

. "$REPO/wasm/lib/scratch.sh"
WORK="${WASM_WIRE_E2E_DIR:-$(scratch_datadir pgrust-fast-wasm-wire-e2e)}"
scratch_adopt "$WORK"
rm -rf "$WORK"; mkdir -p "$WORK"

fail=0
miss() { echo "FAIL: $*"; fail=1; }

# --- the SESSION battery: one statement per line. ----------------------------
# Everything here depends on statements sharing ONE backend session.
cat > "$WORK/wire.sql" <<'SQL'
SELECT 1
BEGIN
CREATE TEMP TABLE wt_tmp (k int4, v text)
INSERT INTO wt_tmp SELECT i, 'v' || i FROM generate_series(1, 4) g(i)
SELECT count(*) AS in_txn FROM wt_tmp
COMMIT
SELECT k, v FROM wt_tmp WHERE k <= 2 ORDER BY k
PREPARE getv (int4) AS SELECT v FROM wt_tmp WHERE k = $1
EXECUTE getv(3)
EXECUTE getv(1)
BEGIN
UPDATE wt_tmp SET v = 'patched' WHERE k = 4
SELECT v AS mid_txn FROM wt_tmp WHERE k = 4
ROLLBACK
SELECT v AS after_rollback FROM wt_tmp WHERE k = 4
SELECT no_such FROM wt_tmp
EXECUTE getv(2)
SELECT 'alive after error' AS marker
CREATE TEMP TABLE adv (k int4 PRIMARY KEY, v text)
INSERT INTO adv VALUES (1, 'one'), (2, 'two')
BEGIN
SAVEPOINT sp1
UPDATE adv SET v = 'ONE' WHERE k = 1
SELECT 1/0
SELECT 'dead in aborted tx' AS probe
ROLLBACK TO SAVEPOINT sp1
SELECT v AS after_sp_rollback FROM adv WHERE k = 1
INSERT INTO adv VALUES (1, 'dup')
ROLLBACK TO SAVEPOINT sp1
COMMIT
SELECT k, v FROM adv ORDER BY k
BEGIN
CREATE TEMP TABLE adv2 (x int4)
INSERT INTO adv2 VALUES (7)
COMMIT
SELECT x FROM adv2
DEALLOCATE ALL
SELECT 'end of adversarial battery' AS fin
SQL

echo "=== assets: wasm/build.sh (initdb + pack) ==="
PGRUST_ASSETS="$WORK/assets" PGRUST_WASM="$WASM_BIN" PGRUST_COMPRESS=0 PGINSTALL="$PGBIN" \
    "$WEB/build.sh" > "$WORK/build.log" 2>&1 || { tail -20 "$WORK/build.log"; echo "VERDICT: wasm-web-wire-e2e FAIL (build.sh)"; exit 1; }

echo "=== initdb (native arm datadir) ==="
"$PGBIN/initdb" -D "$WORK/dd-native" --no-locale --encoding=UTF8 -U postgres -A trust >"$WORK/initdb.log" 2>&1 \
    || { echo "initdb failed"; exit 2; }

ulimit -s 65520 2>/dev/null
export PGRUST_RUNTIME=0
export RUST_MIN_STACK=67108864

# GUCs mirror the worker's wire argv (wiresession.js defaultWireArgv) plus the
# transcript pins (TimeZone is GUC_REPORT — a ParameterStatus in the
# transcript — and must match native-vs-wasm).
GUCS=(-c max_stack_depth=60000 -c io_method=sync -c autovacuum=off
      -c wal_sync_method=fdatasync -c shared_buffers=32MB
      -c timezone=UTC -c log_timezone=UTC)

echo "=== native --stdio-wire session (python driver) ==="
PGRUST_TZDIR="$TZSRC/timezone" PGRUST_PGSHAREDIR="$TZSRC" \
python3 "$REPO/wasm/pgwire_stdio_driver.py" \
    --sql "$WORK/wire.sql" --stderr "$WORK/native.err" -- \
    "$NATIVE_BIN" --stdio-wire "${GUCS[@]}" -D "$WORK/dd-native" \
    > "$WORK/native.transcript"
[ $? -eq 0 ] || miss "native --stdio-wire driver failed (see $WORK/native.err)"

echo "=== wasm --stdio-wire session (run-node-wire.mjs, JSPI) ==="
PGRUST_WASM="$WORK/assets/postgres.wasm" PGRUST_VFS="$WORK/assets/vfs" \
    node "$WEB/run-node-wire.mjs" --sql "$WORK/wire.sql" --stderr "$WORK/wasm.err" \
    > "$WORK/wasm.transcript"
[ $? -eq 0 ] || miss "run-node-wire.mjs driver failed (see $WORK/wasm.err)"

if diff -u "$WORK/native.transcript" "$WORK/wasm.transcript" > "$WORK/transcript.diff"; then
    echo "transcripts byte-identical (native vs wasm-under-node)"
else
    miss "transcripts differ (see $WORK/transcript.diff)"
fi

# --- session-content spot checks (guard an empty==empty diff). ---------------
grep -q '^R auth=0$'                 "$WORK/wasm.transcript" || miss "no AuthenticationOk"
[ "$(grep -c '^K <pid+cancelkey redacted>$' "$WORK/wasm.transcript")" -eq 1 ] \
    || miss "expected exactly ONE handshake (one long-lived session)"
grep -q '^C CREATE TABLE$'           "$WORK/wasm.transcript" || miss "CREATE TEMP TABLE tag missing"
grep -q '^C INSERT 0 4$'             "$WORK/wasm.transcript" || miss "INSERT tag missing"
grep -q '^D 4$'                      "$WORK/wasm.transcript" || miss "in-transaction temp-table count missing"
grep -q '^D 1|v1$'                   "$WORK/wasm.transcript" || miss "temp-table row after COMMIT missing (session state lost?)"
grep -q '^C PREPARE$'                "$WORK/wasm.transcript" || miss "PREPARE tag missing"
grep -q '^D v3$'                     "$WORK/wasm.transcript" || miss "EXECUTE row missing (prepared statement lost?)"
grep -q '^D patched$'                "$WORK/wasm.transcript" || miss "mid-transaction UPDATE row missing"
grep -q '^D v4$'                     "$WORK/wasm.transcript" || miss "post-ROLLBACK row missing"
grep -q 'no_such'                    "$WORK/wasm.transcript" || miss "ERROR message missing"
grep -q '^D v2$'                     "$WORK/wasm.transcript" || miss "EXECUTE after error missing (session did not recover)"
grep -q '^D alive after error$'      "$WORK/wasm.transcript" || miss "post-error marker row missing"
# Adversarial leg (promoted from the inc1 review battery).
grep -q '^Z E$'                      "$WORK/wasm.transcript" || miss "aborted-transaction ReadyForQuery state (Z E) never observed"
grep -q 'current transaction is aborted' "$WORK/wasm.transcript" || miss "follow-up-in-aborted-txn error missing"
grep -q '^D one$'                    "$WORK/wasm.transcript" || miss "ROLLBACK TO SAVEPOINT did not undo the update"
grep -q 'duplicate key value'        "$WORK/wasm.transcript" || miss "duplicate-key error missing"
grep -q '^D 1|one$'                  "$WORK/wasm.transcript" || miss "post-COMMIT savepoint-battery row 1 wrong (rolled-back update leaked?)"
grep -q '^D 2|two$'                  "$WORK/wasm.transcript" || miss "post-COMMIT savepoint-battery row 2 missing"
grep -q '^D 7$'                      "$WORK/wasm.transcript" || miss "second-transaction temp-table row missing"
grep -q '^C DEALLOCATE ALL$'         "$WORK/wasm.transcript" || miss "DEALLOCATE ALL tag missing"
grep -q '^D end of adversarial battery$' "$WORK/wasm.transcript" || miss "adversarial-battery fin row missing"
grep -q '^=== exit 0$'               "$WORK/wasm.transcript" || miss "wasm exit nonzero"

if grep -q 'panicked' "$WORK/wasm.err"; then
    miss "wasm: panic lines in stderr"
fi

# Both arms exited through the shutdown checkpoint. (The wasm arm's datadir
# lives in the in-memory VFS and dies with the node process, so its shutdown
# state is proven via the session's OWN log lines — the same evidence class
# wasm-web-e2e uses.)
"$PGBIN/pg_controldata" "$WORK/dd-native" | grep -q 'shut down' \
    || miss "native datadir not in 'shut down' state"
# "shutdown immediate" = IS_SHUTDOWN|IMMEDIATE — the Terminate-path shutdown
# checkpoint (the engine logs C's human-readable flag names, not the old raw
# "flags 0x5" form). (No "database system is shut down" line exists on this
# engine's single-process path — the native arm ends at the same line.)
grep -q 'checkpoint starting: shutdown immediate' "$WORK/wasm.err" \
    || miss "wasm: no shutdown checkpoint start in the session log"
grep -q 'checkpoint complete' "$WORK/wasm.err" \
    || miss "wasm: no shutdown checkpoint completion in the session log"

if [ "$fail" -eq 0 ]; then
    echo "VERDICT: wasm-web-wire-e2e PASS"
    exit 0
fi
echo "VERDICT: wasm-web-wire-e2e FAIL"
exit 1
