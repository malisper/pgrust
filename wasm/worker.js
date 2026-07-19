// worker.js — boots the pgrust wasm engine off the UI thread.
//
// TWO engine modes over the same module + long-lived in-memory VFS (datadir):
//
//   'wire'   (default where JSPI exists): ONE long-lived `postgres
//            --stdio-wire` instance serving a real pgwire session
//            (wiresession.js). The worker speaks protocol frames over the
//            stdin/stdout host pipes; SESSION state — temp tables, prepared
//            statements, transactions spanning REPL lines — persists across
//            statements. This is the browser port of the wasm-net-e2e
//            wasmtime driver.
//   'single' (fallback; ?engineMode=single forces it): the original model —
//            each statement runs `postgres --single` to completion in a
//            fresh instance over the SHARED VFS. Datadir state persists,
//            session state does not. Engines without JSPI (WebKit/Safari as
//            of mid-2026) always take this path via feature detection.
//
// PERSISTENCE (OPFS, snapshot-class — snapshot.js): when the persist toggle
// is on, the worker serializes the VFS to an OPFS snapshot after each
// completed statement (wire mode runs CHECKPOINT first, so a restore replays
// only the WAL tail) and restores the newest valid snapshot at boot. Corrupt
// snapshots and quota failures degrade to a fresh datadir / persistence-off
// with a status message — never a wedged page.
import { run, Vfs } from './pgrust-wasi.js';
import { decodeUtf8Chunks, formatRun, normalizeSingleUserInput } from './format.js';
import { WireSession, WireSessionDead, jspiSupported, parseMessage } from './wiresession.js';
import { openOpfsStore, loadLatestSnapshot, saveSnapshot, clearSnapshots } from './snapshot.js';

let wasmModule = null;
let baseImage = null;     // immutable packed file bytes (the pristine template)
let manifest = null;
let vfs = null;           // long-lived datadir — PERSISTS across runs until reset
// One asset leg: the wasm32-wasip1 module runs everywhere we target (its
// exnref wasm-EH encoding + feature set is accepted by Chrome, Firefox, and
// WebKit/Safari incl. iOS — no desktop/safari dual-asset split needed).
const PARAMS = new URLSearchParams(self.location.search);
const ASSET_PREFIX = (() => {
  const mode = PARAMS.get('assetEncoding');
  if (mode === 'raw') return './raw/assets';
  if (mode === 'gzip') return './gzip/assets';
  if (mode === 'br') return './br/assets';
  return './assets';
})();
// Engine selection: explicit ?engineMode= wins; otherwise protocol mode
// wherever the engine has JSPI (Chrome/Edge; Node), --single elsewhere
// (WebKit/Safari incl. iOS — no JSPI as of mid-2026).
const ENGINE = (() => {
  const m = PARAMS.get('engineMode');
  if (m === 'single' || m === 'wire') return m;
  return jspiSupported() ? 'wire' : 'single';
})();
const WASM_CACHE_DB = 'pgrust-wasm-cache-v1';
const WASM_CACHE_STORE = 'modules';

let session = null;       // wire mode: the live WireSession
let sessionStderr = [];   // wire mode: stderr chunks since the last statement
let runChain = Promise.resolve(); // serializes wire queries (runs + checkpoints)

// Persistence state.
let store = null;         // OPFS store or null (unavailable)
let persist = false;
let snapGen = 0;
let snapSlot = null;
let restoredFromSnapshot = false;

function post(m) { self.postMessage(m); }

function openWasmCache() {
  if (!self.indexedDB) return Promise.resolve(null);
  return new Promise((resolve) => {
    const req = indexedDB.open(WASM_CACHE_DB, 1);
    req.onupgradeneeded = () => req.result.createObjectStore(WASM_CACHE_STORE);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => resolve(null);
    req.onblocked = () => resolve(null);
  });
}

async function withWasmCache(mode, key, value) {
  const db = await openWasmCache();
  if (!db) return undefined;
  return new Promise((resolve) => {
    let tx, req;
    try {
      tx = db.transaction(WASM_CACHE_STORE, mode);
      const store = tx.objectStore(WASM_CACHE_STORE);
      req = value === undefined ? store.get(key) : store.put(value, key);
    } catch {
      try { db.close(); } catch {}
      resolve(undefined);
      return;
    }
    req.onsuccess = () => resolve(value === undefined ? req.result : true);
    req.onerror = () => resolve(undefined);
    tx.oncomplete = () => db.close();
    tx.onerror = () => { try { db.close(); } catch {} };
  });
}

async function assetFingerprint(url) {
  try {
    const resp = await fetch(url, { method: 'HEAD' });
    if (!resp.ok) return url;
    const etag = resp.headers.get('etag') || '';
    const length = resp.headers.get('content-length') || '';
    const encoding = resp.headers.get('content-encoding') || 'identity';
    const modified = resp.headers.get('last-modified') || '';
    return `${url}|${encoding}|${length}|${etag}|${modified}`;
  } catch {
    return url;
  }
}

// (Re)build the persistent VFS from the pristine packed image — a fresh,
// just-initdb'd datadir. Called once on boot and on every Reset. We copy the
// base image so the VFS owns its bytes (writes mutate subarrays of it); the
// pristine template stays intact for the next reset.
function resetVfs() {
  vfs = new Vfs(baseImage.slice(), manifest);
}

function prepareSql(sql) {
  if (!sql.trim()) sql = 'SELECT 1;';
  // The single-user backend ends a statement at every newline, so flatten each
  // SQL statement onto one line before feeding it in.
  // (The old harness appended an explicit CHECKPOINT here because the fabled
  // build's shutdown checkpoint never completed; pgrust-fast's --single exits
  // through a completed shutdown checkpoint — pg_control is DB_SHUTDOWNED —
  // so the next boot over the mutated VFS is a normal clean restart.)
  return normalizeSingleUserInput(sql) || 'SELECT 1;\n';
}

async function runPreparedSql(sql) {
  const outChunks = [], errChunks = [];
  const result = await run({
    wasmModule,
    vfs,
    stdinBytes: new TextEncoder().encode(prepareSql(sql)),
    onStdout: (b) => outChunks.push(b),
    onStderr: (b) => errChunks.push(b),
  });
  // Streaming decode: a multi-byte UTF-8 character (e.g. Chinese) can be split
  // across two output chunks; one-shot per-chunk decoding garbles it (#34).
  return {
    stdout: decodeUtf8Chunks(outChunks),
    stderr: decodeUtf8Chunks(errChunks),
    exitCode: result.exitCode,
  };
}

// ---- wire engine ------------------------------------------------------------

// A restored snapshot (and any crashed-session restart) boots over a datadir
// whose previous owner never exited: drop its lockfile host-side (the
// container-restart convention) and let StartupXLOG's normal recovery run.
function scrubStaleLockfile() {
  vfs.unlink('/pgdata/postmaster.pid');
}

async function startWireSession() {
  session = new WireSession({
    wasmModule,
    vfs,
    onStderr: (b) => sessionStderr.push(b),
  });
  await session.start(); // handshake through the first ReadyForQuery
}

async function stopWireSession() {
  if (!session) return;
  try { await session.terminate(); } catch { /* already dead */ }
  session = null;
}

// Summarize one simple-query cycle's messages for the UI.
function summarizeWire(msgs) {
  const out = { columns: null, rows: [], tag: null, error: null, notices: [], status: 'I', resultSets: 0 };
  for (const { t, body } of msgs) {
    const m = parseMessage(t, body);
    if (t === 'T') { out.columns = m.columns; out.rows = []; out.resultSets++; }
    else if (t === 'D') { out.rows.push(m.values); }
    else if (t === 'C') { out.tag = m.tag; }
    else if (t === 'E') { if (!out.error) out.error = { severity: m.severity, message: m.message, fields: m.fields }; }
    else if (t === 'N') { out.notices.push(`${m.severity}:  ${m.message}`); }
    else if (t === 'Z') { out.status = m.status; }
  }
  return out;
}

async function wireQuery(sql) {
  sessionStderr = [];
  const msgs = await session.query(sql);
  // Streaming decode (#34 class): a multi-byte UTF-8 character can be split
  // across two stderr chunks; one-shot per-chunk decoding garbles it.
  return {
    wire: summarizeWire(msgs),
    stderr: decodeUtf8Chunks(sessionStderr),
  };
}

// Recover a dead wire session in place (e.g. a FATAL took the backend down):
// the datadir VFS survives; boot a fresh session over it.
async function reviveWireSession() {
  session = null;
  scrubStaleLockfile();
  await startWireSession();
}

// ---- persistence ------------------------------------------------------------

async function snapshotNow() {
  if (!persist || !store) return;
  try {
    if (ENGINE === 'wire' && session && !session.dead) {
      // Quiesce: a completed CHECKPOINT bounds restore-time WAL replay.
      await session.query('CHECKPOINT;');
    }
    const r = await saveSnapshot(store, vfs, snapGen, snapSlot);
    snapGen = r.generation;
    snapSlot = r.slot;
    post({ type: 'persist-state', persist: true, note: `snapshot saved (${(r.bytes / (1024 * 1024)).toFixed(1)} MB)` });
  } catch (e) {
    // Quota or any other storage failure: degrade to persistence-off, keep
    // the live session untouched.
    persist = false;
    post({ type: 'persist-state', persist: false, note: `persistence disabled: snapshot failed (${String(e && e.message || e)})` });
  }
}

function scheduleSnapshot() {
  if (!persist || !store) return;
  runChain = runChain.then(() => snapshotNow());
}

// ---- boot -------------------------------------------------------------------

async function fetchAsset(url) {
  let resp;
  try {
    resp = await fetch(url);
  } catch (e) {
    throw new Error(`${url} fetch failed: ${String(e && e.message || e)}`);
  }
  if (!resp.ok) throw new Error(`${url} returned HTTP ${resp.status}`);
  return resp;
}

async function loadWasmModule(url) {
  const cacheKey = await assetFingerprint(url);
  const cachedModule = await withWasmCache('readonly', cacheKey);
  if (cachedModule instanceof WebAssembly.Module) {
    post({ type: 'status', text: 'Using cached wasm module…' });
    return cachedModule;
  }

  const resp = await fetchAsset(url);
  const fallbackResp = resp.clone();
  const size = resp.headers.get('content-length');
  const encoding = resp.headers.get('content-encoding') || 'identity';
  post({ type: 'status', text: `Compiling wasm module${size ? ` (${size} bytes ${encoding})` : ''}…` });
  let module;
  if (WebAssembly.compileStreaming) {
    try {
      module = await WebAssembly.compileStreaming(resp);
      await withWasmCache('readwrite', cacheKey, module);
      return module;
    } catch (e) {
      post({ type: 'status', text: `Streaming compile failed; retrying buffered compile (${String(e && e.message || e)})…` });
    }
  }
  const buf = await fallbackResp.arrayBuffer();
  module = await WebAssembly.compile(buf);
  await withWasmCache('readwrite', cacheKey, module);
  return module;
}

// Restore the newest valid snapshot into the live VFS; false = pristine boot.
async function tryRestoreSnapshot() {
  if (!store) return false;
  const snap = await loadLatestSnapshot(store, (slot, e) => {
    post({ type: 'status', text: `Ignoring corrupt snapshot ${slot} (${String(e && e.message || e)})…` });
  });
  if (!snap) return false;
  vfs = new Vfs(snap.image, snap.manifest);
  scrubStaleLockfile();
  snapGen = snap.generation;
  snapSlot = snap.slot;
  return true;
}

async function warmEngine() {
  if (ENGINE === 'wire') {
    await startWireSession();
    const r = await wireQuery('SELECT 1;');
    if (r.wire.error) throw new Error(`warm-up failed: ${r.wire.error.message}`);
  } else {
    const r = await runPreparedSql('SELECT 1;');
    if (r.exitCode !== 0) throw new Error(`warm-up exited with code ${r.exitCode}`);
  }
}

async function init() {
  try {
    post({ type: 'build', build: 'wasip1', engine: ENGINE });
    post({ type: 'status', text: 'Fetching wasm module…' });
    wasmModule = await loadWasmModule(`${ASSET_PREFIX}/postgres.wasm`);
    post({ type: 'status', text: 'Fetching datadir VFS…' });
    const [imgBuf, manResp] = await Promise.all([
      fetchAsset(`${ASSET_PREFIX}/vfs.img`).then((r) => r.arrayBuffer()),
      fetchAsset(`${ASSET_PREFIX}/vfs.json`).then((r) => r.json()),
    ]);
    baseImage = new Uint8Array(imgBuf);
    manifest = manResp;

    // Persistence: a present snapshot means the toggle was on last visit.
    store = await openOpfsStore();
    restoredFromSnapshot = await tryRestoreSnapshot();
    if (restoredFromSnapshot) {
      persist = true;
      post({ type: 'status', text: 'Restored persisted datadir from OPFS snapshot…' });
    } else {
      resetVfs(); // initialize the pristine datadir
    }

    const warmStart = performance.now();
    post({ type: 'status', text: ENGINE === 'wire' ? 'Starting protocol session…' : 'Warming query engine…' });
    try {
      await warmEngine();
    } catch (e) {
      if (!restoredFromSnapshot) throw e;
      // GUARD: a snapshot that validates but cannot boot must not wedge the
      // page — discard it, boot the pristine image, tell the user.
      post({ type: 'status', text: `Persisted snapshot unusable (${String(e && e.message || e)}); starting fresh…` });
      await stopWireSession();
      await clearSnapshots(store);
      persist = false;
      restoredFromSnapshot = false;
      snapGen = 0; snapSlot = null;
      resetVfs();
      await warmEngine();
    }
    post({ type: 'status', text: `Query engine warmed in ${Math.round(performance.now() - warmStart)}ms…` });
    post({ type: 'ready', engine: ENGINE, persist, persistAvailable: !!store, restored: restoredFromSnapshot });
  } catch (e) {
    post({ type: 'error', message: String(e && e.stack || e) });
  }
}

// ---- message handlers -------------------------------------------------------

async function handleRunSingle(id, sql, t0) {
  const result = await runPreparedSql(sql);
  post({
    type: 'result',
    id,
    engine: 'single',
    stdout: result.stdout,
    formatted: formatRun(result.stdout, result.stderr),
    stderr: result.stderr,
    exitCode: result.exitCode,
    ms: Math.round(performance.now() - t0),
  });
  if (result.exitCode === 0) scheduleSnapshot();
}

async function handleRunWire(id, sql, t0) {
  let r;
  try {
    r = await wireQuery(sql);
  } catch (e) {
    if (!(e instanceof WireSessionDead)) throw e;
    // The backend exited under us (FATAL/crash class). Revive over the same
    // datadir and surface the failure; the next statement gets a live session.
    const note = `session exited (code ${session ? session.exitCode : '?'}); restarted — session state (temp tables, prepared statements) was lost`;
    await reviveWireSession();
    post({
      type: 'result',
      id,
      engine: 'wire',
      wire: { columns: null, rows: [], tag: null, error: { severity: 'FATAL', message: note }, notices: [], status: 'I', resultSets: 0 },
      stderr: '',
      exitCode: null,
      ms: Math.round(performance.now() - t0),
    });
    return;
  }
  post({
    type: 'result',
    id,
    engine: 'wire',
    wire: r.wire,
    stderr: r.stderr,
    exitCode: null, // session still alive
    ms: Math.round(performance.now() - t0),
  });
  if (!r.wire.error) scheduleSnapshot();
}

self.onmessage = (ev) => {
  const id = ev.data.id;
  const kind = ev.data.type;
  // Serialize EVERYTHING through the run chain: wire queries must never
  // overlap (one in-flight simple-query cycle per session), and single-mode
  // keeps its historical one-at-a-time behavior.
  runChain = runChain.then(async () => {
    if (kind === 'reset') {
      try {
        await stopWireSession();
        resetVfs();
        if (persist && store) {
          // Reset wipes the durable state too, then snapshots the fresh
          // datadir so a reload lands where the user left it: reset.
          await clearSnapshots(store);
          snapGen = 0; snapSlot = null;
        }
        await warmEngine();
        if (persist && store) await snapshotNow();
        post({ type: 'reset-done', id });
      } catch (e) {
        post({ type: 'error', id, message: String(e && e.stack || e) });
      }
      return;
    }
    if (kind === 'persist') {
      try {
        if (ev.data.on) {
          if (!store) {
            post({ type: 'persist-state', id, persist: false, note: 'persistence unavailable (no OPFS in this browser/profile)' });
            return;
          }
          persist = true;
          await snapshotNow();
          post({ type: 'persist-state', id, persist, note: persist ? 'persistence on — datadir snapshots to OPFS after each statement' : undefined });
        } else {
          persist = false;
          if (store) await clearSnapshots(store);
          snapGen = 0; snapSlot = null;
          post({ type: 'persist-state', id, persist: false, note: 'persistence off — stored snapshots cleared' });
        }
      } catch (e) {
        post({ type: 'error', id, message: String(e && e.stack || e) });
      }
      return;
    }
    if (kind !== 'run') return;
    const t0 = performance.now();
    try {
      if (ENGINE === 'wire' && session) await handleRunWire(id, ev.data.sql || '', t0);
      else await handleRunSingle(id, ev.data.sql || '', t0);
    } catch (e) {
      post({ type: 'error', id, message: String(e && e.stack || e) });
    }
  });
};

init();
