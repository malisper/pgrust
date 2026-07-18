// worker.js — boots the pgrust wasm module off the UI thread.
//
// Loads postgres.wasm + the VFS image/manifest once, then keeps ONE long-lived
// in-memory VFS (datadir). On each `run` message it instantiates a fresh module
// instance against that SHARED VFS and feeds it the SQL. `postgres --single`
// runs to completion (clean shutdown checkpoint) and exits per invocation,
// but the writes it made (CREATE TABLE, INSERT, …) PERSIST in the VFS, so the
// next Run boots over the mutated datadir and sees them — REPL-style state.
// `reset` rebuilds the VFS from the pristine packed image (fresh initdb'd dir).
import { run, Vfs } from './pgrust-wasi.js';
import { formatRun, normalizeSingleUserInput } from './format.js';

let wasmModule = null;
let baseImage = null;     // immutable packed file bytes (the pristine template)
let manifest = null;
let vfs = null;           // long-lived datadir — PERSISTS across runs until reset
// One asset leg: the wasm32-wasip1 module runs everywhere we target (its
// exnref wasm-EH encoding + feature set is accepted by Chrome, Firefox, and
// WebKit/Safari incl. iOS — no desktop/safari dual-asset split needed).
const ASSET_PREFIX = (() => {
  const params = new URLSearchParams(self.location.search);
  const mode = params.get('assetEncoding');
  if (mode === 'raw') return './raw/assets';
  if (mode === 'gzip') return './gzip/assets';
  if (mode === 'br') return './br/assets';
  return './assets';
})();
const WASM_CACHE_DB = 'pgrust-wasm-cache-v1';
const WASM_CACHE_STORE = 'modules';

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
  const dec = new TextDecoder();
  return {
    stdout: outChunks.map((b) => dec.decode(b)).join(''),
    stderr: errChunks.map((b) => dec.decode(b)).join(''),
    exitCode: result.exitCode,
  };
}

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

async function init() {
  try {
    post({ type: 'build', build: 'wasip1' });
    post({ type: 'status', text: 'Fetching wasm module…' });
    wasmModule = await loadWasmModule(`${ASSET_PREFIX}/postgres.wasm`);
    post({ type: 'status', text: 'Fetching datadir VFS…' });
    const [imgBuf, manResp] = await Promise.all([
      fetchAsset(`${ASSET_PREFIX}/vfs.img`).then((r) => r.arrayBuffer()),
      fetchAsset(`${ASSET_PREFIX}/vfs.json`).then((r) => r.json()),
    ]);
    baseImage = new Uint8Array(imgBuf);
    manifest = manResp;
    resetVfs(); // initialize the persistent datadir
    const warmStart = performance.now();
    post({ type: 'status', text: 'Warming query engine…' });
    await runPreparedSql('SELECT 1;');
    post({ type: 'status', text: `Query engine warmed in ${Math.round(performance.now() - warmStart)}ms…` });
    post({ type: 'ready' });
  } catch (e) {
    post({ type: 'error', message: String(e && e.stack || e) });
  }
}

self.onmessage = async (ev) => {
  const id = ev.data.id;
  if (ev.data.type === 'reset') {
    try {
      // True reset: throw away all accumulated state, restore a fresh datadir.
      resetVfs();
      await runPreparedSql('SELECT 1;');
      post({ type: 'reset-done', id });
    } catch (e) {
      post({ type: 'error', id, message: String(e && e.stack || e) });
    }
    return;
  }
  if (ev.data.type !== 'run') return;

  const t0 = performance.now();
  try {
    // Boot over the PERSISTENT VFS: this Run's writes mutate it in place and
    // remain visible to the next Run (until Reset). `postgres --single` shut
    // down cleanly last time, so this is a normal clean restart of the datadir.
    const result = await runPreparedSql(ev.data.sql || '');
    post({
      type: 'result',
      id,
      stdout: result.stdout,
      formatted: formatRun(result.stdout, result.stderr),
      stderr: result.stderr,
      exitCode: result.exitCode,
      ms: Math.round(performance.now() - t0),
    });
  } catch (e) {
    post({ type: 'error', id, message: String(e && e.stack || e) });
  }
};

init();
