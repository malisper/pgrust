#!/usr/bin/env bash
# WEB-DEMO ground-truth gate (wasm/p5-web-demo): the wasm JS WASI
# harness boots postgres.wasm under NODE (no browser) over a packed in-memory
# VFS and answers a query battery byte-identically to the NATIVE `postgres
# --single` on an identically minted fresh datadir. This must be green before
# any browser claim.
#
# Checks:
#   1. wasm/build.sh assembles assets (initdb + pack-vfs) from this
#      repo's wasm32-wasip1 module;
#   2. run-node.mjs --raw over the battery == native --single stdout, byte-for-byte
#      (same GUC set as pgrust-wasi.js's argv, same normalized stdin);
#   3. thrown-error statements (parse-analysis + division by zero) unwind and
#      the session answers afterwards; no panic lines in wasm stderr;
#   4. worker-model persistence: run #1 CREATE TABLE + INSERT, run #2 (fresh
#      module instance, SAME long-lived Vfs) reads the rows back; a reset Vfs
#      does not see them.
#   5. the SITE's diagnostic parsers (backend.js parseDiagnostics, format.js
#      formatRun) surface the engine's stderr errors — the stderr lines carry
#      the log_line_prefix default '%m [%p] ', and an anchored parser that
#      misses it renders errors invisible and failed writes as success.
#
# Prereqs: node, C PostgreSQL 18 initdb (PGINSTALL or /opt/homebrew/bin),
# native binary at target/debug/postgres (or PGRUST_NATIVE_BIN), wasm module
# at target/wasm32-wasip1/{wasm-release,debug}/postgres.wasm (or PGRUST_WASM_BIN;
# build with PGRUST_WASM_PROFILE=wasm-release wasm/wasm-build.sh).
set -u

REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
WEB="$REPO/wasm"

command -v node >/dev/null || { echo "node not installed"; exit 2; }
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
WORK="${WASM_WEB_E2E_DIR:-$(scratch_datadir pgrust-fast-wasm-web-e2e)}"
scratch_adopt "$WORK"
rm -rf "$WORK"; mkdir -p "$WORK"

fail=0
miss() { echo "FAIL: $*"; fail=1; }

# --- the battery: one statement per line (the REPL/normalize contract). -------
# wasm-boot-e2e.sh's serial subset + float8 (the old demo's wasm32 gap) +
# strings/jsonb/window/recursive-CTE from the site's example sidebar.
cat > "$WORK/battery.sql" <<'SQL'
CREATE TABLE wb_tenk ( unique1 int4 NOT NULL, unique2 int4, stringu1 name, even int4 );
INSERT INTO wb_tenk SELECT i, 9 - i, 'row' || i, (i % 2) * 2 FROM generate_series(0, 9) AS g(i);
CREATE UNIQUE INDEX wb_tenk_unique1 ON wb_tenk (unique1);
SELECT count(*) AS cnt FROM wb_tenk;
SELECT 1 + 2 * 3 AS arith, (7 % 3)::int2 AS modw, -5 / 2 AS trunca;
SELECT relname, relkind FROM pg_class WHERE relname = 'wb_tenk';
SELECT t.unique1, t.stringu1 FROM wb_tenk t WHERE t.unique1 < 3 ORDER BY t.unique1;
SELECT sum(unique1) AS s, min(unique2) AS mn, max(unique2) AS mx FROM wb_tenk;
SELECT pi() AS pi, sqrt(2::float8) AS root2, 1.5e300 * 2 AS big;
SELECT 'pg' || 'rust' AS name, upper('postgres in rust') AS shout, regexp_replace('postgres in rust', 'postgres', 'pgrust') AS rewrite;
SELECT data ->> 'name' AS name, (data ->> 'stars')::int AS stars FROM (VALUES ('{"name":"pgrust","stars":4200}'::jsonb)) AS repos(data);
SELECT n, n * n AS square, sum(n * n) OVER (ORDER BY n) AS running_sum FROM generate_series(1, 8) AS n;
WITH RECURSIVE fib(i, a, b) AS ( SELECT 1, 0::bigint, 1::bigint UNION ALL SELECT i + 1, b, a + b FROM fib WHERE i < 15 ) SELECT i, a AS fib FROM fib;
CREATE TABLE measurement (city text, logdate date, peaktemp int) PARTITION BY RANGE (logdate);
CREATE TABLE measurement_2024 PARTITION OF measurement FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE measurement_2025 PARTITION OF measurement FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
INSERT INTO measurement VALUES ('sf', '2024-06-01', 30), ('nyc', '2025-06-01', 33);
SELECT tableoid::regclass AS part, city, logdate, peaktemp FROM measurement ORDER BY logdate;
WITH pts AS (SELECT x::real AS r FROM generate_series(1, 3) x), fin AS (SELECT r FROM pts WHERE r > 1.5) SELECT p.r, CASE WHEN EXISTS (SELECT 1 FROM fin f WHERE f.r = p.r) THEN '**' ELSE '  ' END AS m FROM pts p ORDER BY p.r;
SELECT int8range(1, 5) AS r8, int8range(4294967296, 8589934592) AS r8hi;
CREATE TABLE big_part (k bigint) PARTITION BY RANGE (k);
CREATE TABLE big_part_lo PARTITION OF big_part FOR VALUES FROM (0) TO (4294967296);
CREATE TABLE big_part_hi PARTITION OF big_part FOR VALUES FROM (4294967296) TO (8589934592);
INSERT INTO big_part VALUES (1), (4294967297), (8589934591);
SELECT count(*) AS hi_rows FROM big_part WHERE k > 4294967296;
SELECT no_such_column FROM wb_tenk;
SELECT 1 / 0 AS boom;
INSERT INTO wb_tenk VALUES (0, 99, 'dup', 0);
SELECT 'alive after errors' AS marker;
BEGIN;
UPDATE wb_tenk SET even = even + 1 WHERE unique1 = 0;
ROLLBACK;
SELECT even FROM wb_tenk WHERE unique1 = 0;
SQL

echo "=== assets: wasm/build.sh (initdb + pack) ==="
PGRUST_ASSETS="$WORK/assets" PGRUST_WASM="$WASM_BIN" PGRUST_COMPRESS=0 PGINSTALL="$PGBIN" \
    "$WEB/build.sh" > "$WORK/build.log" 2>&1 || { tail -20 "$WORK/build.log"; echo "VERDICT: wasm-web-e2e FAIL (build.sh)"; exit 1; }

echo "=== native --single (identical fresh datadir, same GUCs) ==="
"$PGBIN/initdb" -D "$WORK/dd-native" --no-locale --encoding=UTF8 -U postgres -A trust >"$WORK/initdb-native.log" 2>&1 \
    || { echo "initdb failed"; exit 2; }
ulimit -s 65520 2>/dev/null
export PGRUST_RUNTIME=0
export RUST_MIN_STACK=67108864
# GUCs must mirror wasm/pgrust-wasi.js's default argv exactly.
GUCS=(-c max_stack_depth=60000 -c io_method=sync -c autovacuum=off -c wal_sync_method=fdatasync -c shared_buffers=32MB)
PGRUST_TZDIR="$TZSRC/timezone" PGRUST_PGSHAREDIR="$TZSRC" \
    "$NATIVE_BIN" --single "${GUCS[@]}" -D "$WORK/dd-native" postgres \
    < "$WORK/battery.sql" > "$WORK/native.out" 2> "$WORK/native.err"
[ $? -eq 0 ] || miss "native --single exited nonzero"

echo "=== wasm under node (run-node.mjs --raw, packed VFS) ==="
PGRUST_WASM="$WORK/assets/postgres.wasm" PGRUST_VFS="$WORK/assets/vfs" \
    node "$WEB/run-node.mjs" --raw \
    < "$WORK/battery.sql" > "$WORK/wasm.out" 2> "$WORK/wasm.err"
[ $? -eq 0 ] || miss "run-node.mjs exited nonzero"

if diff -u "$WORK/native.out" "$WORK/wasm.out" > "$WORK/out.diff"; then
    echo "stdout byte-identical (native vs wasm-under-node)"
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
# WASM-PARTFIX regression: declarative partitioning (relpartbound node-string
# round trip; a pointer-width _outDatum emission dies here on wasm32).
grep -q 'bad integer token' "$WORK/wasm.err" \
    && miss "wasm: readfuncs datum-width regression (bad integer token)"
grep -q 'part = "measurement_2025"' "$WORK/wasm.out" \
    || miss "wasm: partitioned insert did not route to measurement_2025"
# WASM-SUBPLANFIX regression: the CASE WHEN EXISTS probe forces an expression
# SubPlan; make_subplan copyObject's the sub-Query via a queryToString/
# stringToNode round trip (rewrite_manip::copy_query_node) — the same
# _outDatum writer as relpartbound. A pointer-width byval emission on wasm32
# dies here with `bad integer token "]"` before any row comes back.
[ "$(grep -c 'm = "\*\*"' "$WORK/wasm.out")" -eq 2 ] \
    || miss "wasm: EXISTS-SubPlan CASE probe rows missing (copyObject node-string round trip)"
# WASM-SUBPLANFIX sweep finds: range_serialize byval width (int8range panics
# on wasm32) and planner DatumImage byval width (bigint partition pruning
# compared truncated bounds; hi_rows came back 0).
grep -q 'r8hi = "\[4294967296,8589934592)"' "$WORK/wasm.out" \
    || miss "wasm: int8range with high-word bounds missing (range_serialize datum width)"
grep -q 'hi_rows = "2"' "$WORK/wasm.out" \
    || miss "wasm: bigint partition pruning wrong (DatumImage byval width)"

echo "=== full-demo SubPlan leg: Mandelbrot REPL query (fixture gated) ==="
# Michael's Mandelbrot REPL query reaches the same make_subplan boundary via
# its EXISTS-in-CASE leg (plus recursive CTEs needing the complete raw-walker
# vocabulary). The fixture ships with fix/rawwalker-vocab, so this leg
# self-activates once that lane's commits are on this lineage.
MANDEL="$REPO/fixtures/rawwalker-mandelbrot.sql"
if [ -f "$MANDEL" ]; then
    # Native --single ends a statement at every newline: flatten the fixture
    # to one line and feed the SAME bytes to both arms.
    { tr '\n' ' ' < "$MANDEL"; echo; } > "$WORK/mandel.sql"
    PGRUST_TZDIR="$TZSRC/timezone" PGRUST_PGSHAREDIR="$TZSRC" \
        "$NATIVE_BIN" --single "${GUCS[@]}" -D "$WORK/dd-native" postgres \
        < "$WORK/mandel.sql" > "$WORK/mandel-native.out" 2> "$WORK/mandel-native.err"
    [ $? -eq 0 ] || miss "native --single (mandelbrot) exited nonzero"
    PGRUST_WASM="$WORK/assets/postgres.wasm" PGRUST_VFS="$WORK/assets/vfs" \
        node "$WEB/run-node.mjs" --raw \
        < "$WORK/mandel.sql" > "$WORK/mandel-wasm.out" 2> "$WORK/mandel-wasm.err"
    [ $? -eq 0 ] || miss "run-node.mjs (mandelbrot) exited nonzero"
    if diff -u "$WORK/mandel-native.out" "$WORK/mandel-wasm.out" > "$WORK/mandel.diff"; then
        echo "mandelbrot stdout byte-identical (native vs wasm-under-node)"
    else
        miss "mandelbrot stdout differs (see $WORK/mandel.diff)"
    fi
    grep -q 'bad integer token' "$WORK/mandel-wasm.err" \
        && miss "wasm: mandelbrot readfuncs datum-width regression (bad integer token)"
    grep -q 'ERROR' "$WORK/mandel-wasm.err" \
        && miss "wasm: mandelbrot errored (see $WORK/mandel-wasm.err)"
else
    echo "SKIP: $MANDEL absent (pre fix/rawwalker-vocab merge; leg self-activates with the fixture)"
fi

echo "=== worker-model persistence (fresh instance, same long-lived Vfs) ==="
PGRUST_WEB_DIR="$WEB" node --input-type=module - "$WORK/assets" <<'EOF' > "$WORK/persist.out" 2> "$WORK/persist.err"
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
const assets = process.argv[2];
const web = process.env.PGRUST_WEB_DIR;
const { run, Vfs } = await import(pathToFileURL(path.join(web, 'pgrust-wasi.js')));
const mod = await WebAssembly.compile(fs.readFileSync(path.join(assets, 'postgres.wasm')));
const image = new Uint8Array(fs.readFileSync(path.join(assets, 'vfs.img')));
const manifest = JSON.parse(fs.readFileSync(path.join(assets, 'vfs.json'), 'utf8'));
async function exec(vfs, sql) {
  const out = [];
  const res = await run({ wasmModule: mod, vfs, stdinBytes: new TextEncoder().encode(sql),
    onStdout: (b) => out.push(Buffer.from(b)), onStderr: () => {} });
  return { code: res.exitCode, text: Buffer.concat(out).toString() };
}
const vfs = new Vfs(image.slice(), manifest);           // the worker's long-lived datadir
const r1 = await exec(vfs, "CREATE TABLE persist_t (x int);\nINSERT INTO persist_t SELECT generate_series(1, 7);\n");
const r2 = await exec(vfs, 'SELECT count(*) AS persisted FROM persist_t;\n'); // fresh instance, same Vfs
const fresh = new Vfs(image.slice(), manifest);         // the worker's reset path
const r3 = await exec(fresh, 'SELECT count(*) AS persisted FROM persist_t;\n');
console.log('run1 exit=' + r1.code + ' run2 exit=' + r2.code + ' run3 exit=' + r3.code);
console.log('PERSIST=' + r2.text.includes('persisted = "7"'));
console.log('RESET_CLEAN=' + !r3.text.includes('persisted = "7"'));
EOF
grep -q 'PERSIST=true' "$WORK/persist.out" || miss "persistence across module instances not observed"
grep -q 'RESET_CLEAN=true' "$WORK/persist.out" || miss "reset VFS unexpectedly saw persisted rows"

echo "=== site diagnostic parsers surface the engine's stderr errors ==="
# Drive the REAL parsers the site uses (not a grep re-implementation) over the
# wasm session's captured stderr: backend.js parseDiagnostics feeds the REPL's
# error rendering; format.js formatRun is what worker.js/run-node.mjs display.
PGRUST_WEB_DIR="$WEB" node --input-type=module - "$WORK/wasm.err" <<'EOF' > "$WORK/diag.out" 2> "$WORK/diag.node.err"
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
const web = process.env.PGRUST_WEB_DIR;
const { parseDiagnostics } = await import(pathToFileURL(path.join(web, 'backend.js')));
const { formatRun } = await import(pathToFileURL(path.join(web, 'format.js')));
const err = fs.readFileSync(process.argv[2], 'utf8');
const d = parseDiagnostics(err);
console.log('FIRST_ERROR=' + (d.error || '(none)'));
console.log('DUP_DETAIL=' + d.notices.some((n) => /^DETAIL:\s+Key \(unique1\)=\(0\) already exists\./.test(n)));
const fmt = formatRun('', err);
console.log('FMT_DIVZERO=' + /(^|\n)ERROR:\s+division by zero/.test(fmt));
console.log('FMT_DUPKEY=' + /(^|\n)ERROR:\s+duplicate key value violates unique constraint "wb_tenk_unique1"/.test(fmt));
EOF
grep -q 'FIRST_ERROR=column "no_such_column" does not exist' "$WORK/diag.out" \
    || miss "parseDiagnostics did not surface the first ERROR (stderr log_line_prefix regression?)"
grep -q 'DUP_DETAIL=true' "$WORK/diag.out" || miss "parseDiagnostics missed the duplicate-key DETAIL line"
grep -q 'FMT_DIVZERO=true' "$WORK/diag.out" || miss "formatRun did not render ERROR: division by zero"
grep -q 'FMT_DUPKEY=true' "$WORK/diag.out" || miss "formatRun did not render the duplicate-key ERROR"

if [ "$fail" -eq 0 ]; then
    echo "VERDICT: wasm-web-e2e PASS"
    exit 0
fi
echo "VERDICT: wasm-web-e2e FAIL"
exit 1
