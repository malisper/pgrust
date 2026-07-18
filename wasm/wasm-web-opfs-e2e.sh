#!/usr/bin/env bash
# WASM-NEXT persistence gate: the OPFS snapshot machinery (wasm/
# snapshot.js) — serialize the worker's datadir VFS at a quiesce point,
# restore it "after a page reload", and survive corruption — driven through
# the REAL modules (snapshot.js + wiresession.js + pgrust-wasi.js run()) under
# Node. Node has no OPFS, so the store is the harness's in-memory Map — the
# worker's OPFS adapter is the thin openOpfsStore() shim on the same
# store-contract (read/write/remove), verified in-browser via verify.html.
#
# Legs:
#   1. WIRE persist/reload: session A (protocol mode) creates + fills a
#      table, CHECKPOINTs (worker.js's quiesce), snapshot; "reload" =
#      restore into a fresh Vfs, scrub the stale postmaster.pid (the worker's
#      container-restart convention), boot session B over it → the rows
#      survive; the boot log shows the crash-recovery arm engaged (the
#      snapshot is DB_IN_PRODUCTION by design).
#   2. SINGLE persist/reload: same flow through the --single engine (the
#      WebKit/iOS fallback path) — its snapshots are CLEAN images (every run
#      exits through the shutdown checkpoint), restore boots normally.
#   3. CORRUPT-SNAPSHOT recovery: a bit-flipped snapshot fails CRC
#      validation, restoreVfs throws, the caller falls back to the pristine
#      packed image and the engine still boots — never a wedged page.
#   4. SLOT/GENERATION pick: with slot A at gen N (valid) and slot B at gen
#      N+1 (corrupt — a torn write), the loader returns gen N from slot A.
#
# Prereqs: node with JSPI (leg 1; legs 2-4 need only plain Node), C
# PostgreSQL 18 initdb, wasm module (PGRUST_WASM_BIN or target/
# wasm32-wasip1/{wasm-release,debug}/postgres.wasm).
set -u

REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
WEB="$REPO/wasm"

command -v node >/dev/null || { echo "node not installed"; exit 2; }
node -e 'process.exit(typeof WebAssembly.Suspending === "function" && typeof WebAssembly.promising === "function" ? 0 : 1)' \
    || { echo "this node has no JSPI (WebAssembly.Suspending/promising)"; exit 2; }

PGBIN=""
for cand in "${PGINSTALL:-}" /tmp/pgrust_pginstall/bin /opt/homebrew/bin; do
    [ -n "$cand" ] && [ -x "$cand/initdb" ] && PGBIN="$cand" && break
done
[ -n "$PGBIN" ] || { echo "no PostgreSQL 18 initdb found (set PGINSTALL)"; exit 2; }

WASM_BIN="${PGRUST_WASM_BIN:-}"
if [ -z "$WASM_BIN" ]; then
    for cand in "$REPO/target/wasm32-wasip1/wasm-release/postgres.wasm" \
                "$REPO/target/wasm32-wasip1/debug/postgres.wasm"; do
        [ -f "$cand" ] && WASM_BIN="$cand" && break
    done
fi
[ -n "$WASM_BIN" ] && [ -f "$WASM_BIN" ] || { echo "no wasm binary (wasm/wasm-build.sh)"; exit 2; }

. "$REPO/wasm/lib/scratch.sh"
WORK="${WASM_OPFS_E2E_DIR:-$(scratch_datadir pgrust-fast-wasm-opfs-e2e)}"
scratch_adopt "$WORK"
rm -rf "$WORK"; mkdir -p "$WORK"

fail=0
miss() { echo "FAIL: $*"; fail=1; }

echo "=== assets: wasm/build.sh (initdb + pack) ==="
PGRUST_ASSETS="$WORK/assets" PGRUST_WASM="$WASM_BIN" PGRUST_COMPRESS=0 PGINSTALL="$PGBIN" \
    "$WEB/build.sh" > "$WORK/build.log" 2>&1 || { tail -20 "$WORK/build.log"; echo "VERDICT: wasm-web-opfs-e2e FAIL (build.sh)"; exit 1; }

echo "=== snapshot legs (real snapshot.js + wiresession.js + run()) ==="
PGRUST_WEB_DIR="$WEB" node --input-type=module - "$WORK/assets" <<'EOF' > "$WORK/opfs.out" 2> "$WORK/opfs.err"
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
const assets = process.argv[2];
const web = process.env.PGRUST_WEB_DIR;
const { run, Vfs } = await import(pathToFileURL(path.join(web, 'pgrust-wasi.js')));
const { WireSession, defaultWireArgv } = await import(pathToFileURL(path.join(web, 'wiresession.js')));
const { parseMessage } = await import(pathToFileURL(path.join(web, 'wire.js')));
const snap = await import(pathToFileURL(path.join(web, 'snapshot.js')));

const mod = await WebAssembly.compile(fs.readFileSync(path.join(assets, 'postgres.wasm')));
const image = new Uint8Array(fs.readFileSync(path.join(assets, 'vfs.img')));
const manifest = JSON.parse(fs.readFileSync(path.join(assets, 'vfs.json'), 'utf8'));

// The harness store: the same read/write/remove contract openOpfsStore()
// implements over OPFS in the worker.
function memStore() {
  const m = new Map();
  return {
    async read(name) { return m.has(name) ? m.get(name) : null; },
    async write(name, bytes) { m.set(name, bytes.slice()); },
    async remove(name) { m.delete(name); },
    _raw: m,
  };
}

async function wireBoot(vfs, errSink) {
  const s = new WireSession({
    wasmModule: mod, vfs, argv: defaultWireArgv(['-c', 'timezone=UTC', '-c', 'log_timezone=UTC']),
    onStderr: (b) => errSink.push(Buffer.from(b)),
  });
  await s.start();
  return s;
}

// ---------- Leg 1: wire persist -> "reload" -> data survives ----------------
{
  const errA = [], errB = [];
  const vfsA = new Vfs(image.slice(), manifest);
  const store = memStore();
  const sA = await wireBoot(vfsA, errA);
  await sA.query('CREATE TABLE persist_w (x int4);');
  await sA.query('INSERT INTO persist_w SELECT generate_series(1, 9);');
  await sA.query('CHECKPOINT;');            // worker.js's quiesce
  const saved = await snap.saveSnapshot(store, vfsA, 0, null);
  console.log('WIRE_SNAP_BYTES=' + saved.bytes + ' gen=' + saved.generation + ' slot=' + saved.slot);
  // The page "reloads": the live session is gone (no Terminate), only the
  // snapshot survives.
  const best = await snap.loadLatestSnapshot(store);
  const vfsB = new Vfs(best.image, best.manifest);
  vfsB.unlink('/pgdata/postmaster.pid');     // worker.js scrubStaleLockfile()
  const sB = await wireBoot(vfsB, errB);
  const msgs = await sB.query('SELECT count(*) AS n FROM persist_w;');
  const dRow = msgs.find((m) => m.t === 'D');
  const count = dRow ? parseMessage(dRow.t, dRow.body).values[0] : '?';
  console.log('WIRE_COUNT=' + count);
  console.log('WIRE_PERSISTED=' + (count === '9'));
  const bootLog = Buffer.concat(errB).toString();
  console.log('WIRE_RECOVERY_RAN=' + /database system was interrupted|automatic recovery in progress/.test(bootLog));
  const rc = await sB.terminate();
  console.log('WIRE_SESSION_B_EXIT=' + rc);
  const panics = /panicked/.test(Buffer.concat(errA).toString()) || /panicked/.test(bootLog);
  console.log('WIRE_PANICS=' + panics);
}

// ---------- Leg 2: single-engine persist -> reload (the iOS path) -----------
{
  async function single(vfs, sql) {
    const out = [];
    const res = await run({ wasmModule: mod, vfs, stdinBytes: new TextEncoder().encode(sql),
      onStdout: (b) => out.push(Buffer.from(b)), onStderr: () => {} });
    return { code: res.exitCode, text: Buffer.concat(out).toString() };
  }
  const store = memStore();
  const vfsA = new Vfs(image.slice(), manifest);
  const r1 = await single(vfsA, "CREATE TABLE persist_s (x int);\nINSERT INTO persist_s SELECT generate_series(1, 5);\n");
  const saved = await snap.saveSnapshot(store, vfsA, 0, null); // clean image: --single exited via shutdown checkpoint
  const best = await snap.loadLatestSnapshot(store);
  const vfsB = new Vfs(best.image, best.manifest);
  vfsB.unlink('/pgdata/postmaster.pid');
  const r2 = await single(vfsB, 'SELECT count(*) AS persisted FROM persist_s;\n');
  console.log('SINGLE_PERSISTED=' + (r1.code === 0 && r2.code === 0 && r2.text.includes('persisted = "5"')));
  console.log('SINGLE_SNAP_BYTES=' + saved.bytes);
}

// ---------- Leg 3: corrupt snapshot -> validation fails -> fresh boot -------
{
  const store = memStore();
  const vfsA = new Vfs(image.slice(), manifest);
  await (async () => {
    const out = [];
    await run({ wasmModule: mod, vfs: vfsA, stdinBytes: new TextEncoder().encode('CREATE TABLE corrupt_t (x int);\n'),
      onStdout: (b) => out.push(b), onStderr: () => {} });
  })();
  const saved = await snap.saveSnapshot(store, vfsA, 0, null);
  // Bit-flip mid-data (a torn/corrupted write).
  const raw = store._raw.get(saved.slot);
  raw[Math.floor(raw.length / 2)] ^= 0xff;
  let threw = false;
  try { snap.restoreVfs(raw); } catch (e) { threw = e.constructor.name === 'SnapshotCorruptError'; }
  console.log('CORRUPT_DETECTED=' + threw);
  const best = await snap.loadLatestSnapshot(store);   // both slots bad/absent -> null
  console.log('CORRUPT_SKIPPED=' + (best === null));
  // The worker's fallback: pristine image still boots.
  const fresh = new Vfs(image.slice(), manifest);
  const out = [];
  const res = await run({ wasmModule: mod, vfs: fresh, stdinBytes: new TextEncoder().encode('SELECT 41 + 1 AS ok;\n'),
    onStdout: (b) => out.push(Buffer.from(b)), onStderr: () => {} });
  console.log('CORRUPT_FRESH_BOOT=' + (res.exitCode === 0 && Buffer.concat(out).toString().includes('ok = "42"')));
}

// ---------- Leg 4: generation slots — torn write never eats the good copy ---
{
  const store = memStore();
  const vfsA = new Vfs(image.slice(), manifest);
  const s1 = await snap.saveSnapshot(store, vfsA, 0, null);        // gen 1 -> slot a
  const s2 = await snap.saveSnapshot(store, vfsA, s1.generation, s1.slot); // gen 2 -> slot b
  console.log('SLOTS_DISTINCT=' + (s1.slot !== s2.slot));
  const rawB = store._raw.get(s2.slot);
  rawB[rawB.length - 2] ^= 0xff;                                    // tear the NEWER write
  const best = await snap.loadLatestSnapshot(store);
  console.log('SLOT_FALLBACK_GEN=' + (best ? best.generation : 'none') + ' slot=' + (best ? best.slot : 'none'));
  console.log('SLOT_FALLBACK_OK=' + (best !== null && best.generation === 1 && best.slot === s1.slot));
}
EOF
rc=$?
cat "$WORK/opfs.out"
[ $rc -eq 0 ] || { tail -30 "$WORK/opfs.err"; miss "node snapshot harness exited $rc"; }

grep -q 'WIRE_PERSISTED=true'      "$WORK/opfs.out" || miss "wire snapshot: rows did not survive the reload"
grep -q 'WIRE_RECOVERY_RAN=true'   "$WORK/opfs.out" || miss "wire snapshot: crash-recovery arm did not engage"
grep -q 'WIRE_SESSION_B_EXIT=0'    "$WORK/opfs.out" || miss "wire session B did not exit clean"
grep -q 'WIRE_PANICS=false'        "$WORK/opfs.out" || miss "wire legs: panic lines in stderr"
grep -q 'SINGLE_PERSISTED=true'    "$WORK/opfs.out" || miss "single-engine snapshot: rows did not survive"
grep -q 'CORRUPT_DETECTED=true'    "$WORK/opfs.out" || miss "corrupt snapshot not detected by CRC validation"
grep -q 'CORRUPT_SKIPPED=true'     "$WORK/opfs.out" || miss "loader returned a corrupt snapshot"
grep -q 'CORRUPT_FRESH_BOOT=true'  "$WORK/opfs.out" || miss "fresh-image fallback did not boot"
grep -q 'SLOTS_DISTINCT=true'      "$WORK/opfs.out" || miss "consecutive snapshots reused one slot"
grep -q 'SLOT_FALLBACK_OK=true'    "$WORK/opfs.out" || miss "torn newer write ate the good older snapshot"

if [ "$fail" -eq 0 ]; then
    echo "VERDICT: wasm-web-opfs-e2e PASS"
    exit 0
fi
echo "VERDICT: wasm-web-opfs-e2e FAIL"
exit 1
