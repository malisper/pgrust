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
SELECT no_such_column FROM wb_tenk;
SELECT 1 / 0 AS boom;
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

if [ "$fail" -eq 0 ]; then
    echo "VERDICT: wasm-web-e2e PASS"
    exit 0
fi
echo "VERDICT: wasm-web-e2e FAIL"
exit 1
