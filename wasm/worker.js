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
// with a status message — never a wedged page. psql client mode persists too,
// keyed to the server's ReadyForQuery instead of statement boundaries the
// worker cannot see (psqlSnapshotSoon).
//
// RESET: swap in a pristine VFS and respawn whatever is running over it — the
// wire session (stopWireSession + warmEngine) or, in psql client mode, psql
// and its server instance (resetPsqlSession). Never a page reload: the
// terminal scrollback and the compiled modules survive.
import { run, Vfs } from './pgrust-wasi.js';
import { decodeUtf8Chunks, formatRun, normalizeSingleUserInput } from './format.js';
import { WireSession, WireSessionDead, jspiSupported, parseMessage } from './wiresession.js';
import { openOpfsStore, loadLatestSnapshot, saveSnapshot, clearSnapshots } from './snapshot.js';
import { PsqlSession, makePushStream } from './psqlsession.js';

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
// Client selection: 'psql' = the REAL Rust psql.wasm drives the terminal
// (cross-piped to postgres --stdio-wire instances; needs JSPI); 'js' = the
// legacy JS REPL. The page (repl.js) decides and passes it through.
const CLIENT = PARAMS.get('client') === 'psql' && jspiSupported() ? 'psql' : 'js';
const WASM_CACHE_DB = 'pgrust-wasm-cache-v1';
const WASM_CACHE_STORE = 'modules';

let session = null;       // wire mode: the live WireSession
let sessionStderr = [];   // wire mode: stderr chunks since the last statement
let runChain = Promise.resolve(); // serializes wire queries (runs + checkpoints)

// psql client mode: the Rust psql.wasm cross-piped to server instances.
let psqlSession = null;
let psqlStdin = null;
let psqlModule = null;
let psqlDebugTimer = null;
const psqlEnc = new TextEncoder();

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
    const t0 = performance.now();
    if (ENGINE === 'wire' && session && !session.dead) {
      // Quiesce: a completed CHECKPOINT bounds restore-time WAL replay.
      await session.query('CHECKPOINT;');
    }
    const r = await saveSnapshot(store, vfs, snapGen, snapSlot);
    snapGen = r.generation;
    snapSlot = r.slot;
    post({ type: 'persist-state', persist: true, note: `snapshot saved (${(r.bytes / (1024 * 1024)).toFixed(1)} MB in ${Math.round(performance.now() - t0)}ms)` });
  } catch (e) {
    // Quota or any other storage failure: degrade to persistence-off, keep
    // the live session untouched.
    persist = false;
    post({ type: 'persist-state', persist: false, note: `persistence disabled: snapshot failed (${String(e && e.message || e)})` });
  }
}

// Statement-boundary durability for the js client (wire/single engines): the
// run handlers call this BEFORE posting a successful result, so the page can
// only render output whose datadir state is already flushed — the same
// "you saw it, it's durable" contract the psql client's ReadyForQuery gate
// provides. Both handlers already execute INSIDE runChain, so calling
// snapshotNow() directly is the serialized position.
async function snapshotBeforeReport() {
  if (!persist || !store) return;
  await snapshotNow();
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

// psql client mode: boot the Rust psql against server instance(s) on the
// live VFS. psql's terminal fds stream to the page; the server connection
// is the fd 4/5 pipe pair inside PsqlSession.
//
// Every callback is fenced on `psqlSession === sess`: a session being retired
// (reset) must not paint the page or report its exit as a crash, and the
// retired instances must not be reachable from any live handler afterwards.
async function startPsqlSession() {
  const stdin = makePushStream();
  let sess = null;
  const mine = () => psqlSession === sess;
  sess = new PsqlSession({
    psqlModule,
    serverModule: wasmModule,
    vfs,
    psqlStdin: stdin,
    psqlArgv: ['psql'],
    psqlEnv: { USER: 'postgres', PSQL_INTERACTIVE: '1' },
    onPsqlStdout: (b) => { if (mine()) post({ type: 'psql-out', data: b }); },
    onPsqlStderr: (b) => { if (mine()) post({ type: 'psql-err', data: b }); },
    onServerStderr: (b) => { if (mine()) post({ type: 'psql-log', data: b }); },
    onServerIdle: () => (mine() ? psqlSnapshotSoon() : undefined),
  });
  psqlSession = sess;
  psqlStdin = stdin;
  if (PARAMS.get('debug') === '1' && !psqlDebugTimer) {
    let hb = 0;
    psqlDebugTimer = setInterval(() => post({ type: 'status', text: `heartbeat ${hb++}` }), 1000);
  }
  post({ type: 'status', text: 'Starting psql… (spawning server instance)' });
  await sess.start();
  post({ type: 'status', text: 'Starting psql… (psql instance launched)' });
  sess.psqlExit
    .then((code) => { if (mine()) post({ type: 'psql-exit', code }); })
    .catch((e) => { if (mine()) post({ type: 'psql-exit', code: null, error: String(e && e.message || e) }); });
}

// Reset in psql client mode: retire psql AND its server instance, swap in a
// pristine VFS, then boot a fresh psql. No page reload, no orphaned instance.
//
// Teardown order (see PsqlSession.stop): psql goes down FIRST and takes the
// server with it through Terminate + shutdown checkpoint. Only then is the VFS
// replaced — a psql left running across the swap would be talking to a backend
// whose datadir vanished mid-session.
async function resetPsqlSession() {
  const dying = psqlSession;
  // Unfence first: from here on the old session is invisible to the page, so
  // its farewell bytes and its exit are not mistaken for a live psql's.
  psqlSession = null;
  psqlStdin = null;
  let retired = { psqlExited: true, serverExited: true };
  if (dying) {
    try { retired = await dying.stop(); }
    catch (e) { retired = { psqlExited: false, serverExited: false, error: String(e && e.message || e) }; }
  }
  resetVfs();
  if (persist && store) {
    // Reset wipes the durable copy too, then re-snapshots the fresh datadir so
    // a reload lands where the user left it: reset.
    await clearSnapshots(store);
    snapGen = 0; snapSlot = null;
  }
  await startPsqlSession();
  if (persist && store) await snapshotNow();
  return retired;
}

// Persistence cadence in psql client mode. The worker no longer sees statement
// boundaries (psql owns the REPL), so the trigger is the server's
// ReadyForQuery, and every one snapshots IMMEDIATELY. The RETURN VALUE is the
// durability gate: PsqlSession withholds the bytes carrying that ReadyForQuery
// from psql until the returned promise resolves, so a statement's result can
// only reach the terminal once the snapshot covering it is flushed to OPFS.
// "You saw it, it's durable" — a reload the instant a result paints cannot
// lose that statement; a reload BEFORE it paints loses only a statement whose
// completion nothing ever reported (the same contract as killing any database
// client mid-statement). Persist off returns undefined: no gate, no latency.
//
// Bursts (psql's \d catalog chatter is several round trips) are bounded by
// dedup: at most one snapshot RUNNING plus one QUEUED; an idle report arriving
// while one is queued rides along with it (and gates on it), since the queued
// snapshot serializes whatever state the datadir has when it runs.
//
// The image is never torn: serializeVfs() is synchronous and the guest only
// advances when the JS stack yields, so a snapshot is a byte-exact copy of the
// datadir at one instant, with the guest's writes applied in issue order.
// Restoring it is therefore an ordinary crash recovery — committed work is in
// the WAL and StartupXLOG replays it (no CHECKPOINT to inject, which we could
// not do here anyway: the connection belongs to psql).
let psqlSnapQueued = null; // promise of the queued-but-not-started snapshot
function psqlSnapshotSoon() {
  if (!persist || !store) return undefined;
  if (psqlSnapQueued) return psqlSnapQueued; // it will cover this state too
  const p = runChain.then(() => {
    psqlSnapQueued = null;
    return snapshotNow();
  });
  psqlSnapQueued = p;
  runChain = p;
  return p;
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
    post({ type: 'build', build: 'wasip1', engine: CLIENT === 'psql' ? 'psql' : ENGINE });
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

    if (CLIENT === 'psql') {
      // The REAL psql drives the terminal. Persistence works here too: the
      // snapshot cadence just moves off statement boundaries (psql owns the
      // REPL) and onto the server's ReadyForQuery — see psqlSnapshotSoon.
      post({ type: 'status', text: 'Fetching psql.wasm…' });
      psqlModule = await loadWasmModule(`${ASSET_PREFIX}/psql.wasm`);
      post({ type: 'status', text: 'Starting psql…' });
      try {
        await startPsqlSession();
      } catch (e) {
        if (!restoredFromSnapshot) throw e;
        // GUARD (same rule as the wire path): a snapshot that validates but
        // cannot boot must not wedge the page.
        post({ type: 'status', text: `Persisted snapshot unusable (${String(e && e.message || e)}); starting fresh…` });
        if (psqlSession) { try { await psqlSession.stop(); } catch { /* going away */ } psqlSession = null; psqlStdin = null; }
        await clearSnapshots(store);
        persist = false;
        restoredFromSnapshot = false;
        snapGen = 0; snapSlot = null;
        resetVfs();
        await startPsqlSession();
      }
      post({ type: 'ready', engine: 'psql', persist, persistAvailable: !!store, restored: restoredFromSnapshot });
      return;
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
  if (result.exitCode === 0) await snapshotBeforeReport();
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
  if (!r.wire.error) await snapshotBeforeReport();
  post({
    type: 'result',
    id,
    engine: 'wire',
    wire: r.wire,
    stderr: r.stderr,
    exitCode: null, // session still alive
    ms: Math.round(performance.now() - t0),
  });
}

self.onmessage = (ev) => {
  const id = ev.data.id;
  const kind = ev.data.type;
  // psql client mode: terminal keystrokes bypass the run chain — they are
  // stream bytes, not statements.
  if (kind === 'psql-line') {
    if (psqlStdin) psqlStdin.push(psqlEnc.encode(ev.data.text));
    return;
  }
  // `run` has no meaning in psql client mode — statements arrive as keystrokes
  // on psql's stdin, not as worker requests. `reset` and `persist` DO work
  // (respawn / OPFS snapshots against the same VFS).
  if (CLIENT === 'psql' && kind === 'run') {
    post({ type: 'error', id, message: `"${kind}" is not available in psql client mode` });
    return;
  }
  // Page-teardown flush (pagehide / visibility:hidden): snapshot NOW,
  // best-effort — the page is going away and will never read a reply.
  if (kind === 'flush') {
    if (persist && store) runChain = runChain.then(() => snapshotNow());
    return;
  }
  // Serialize EVERYTHING through the run chain: wire queries must never
  // overlap (one in-flight simple-query cycle per session), and single-mode
  // keeps its historical one-at-a-time behavior.
  runChain = runChain.then(async () => {
    if (kind === 'reset') {
      try {
        if (CLIENT === 'psql') {
          const retired = await resetPsqlSession();
          post({ type: 'reset-done', id, retired });
          return;
        }
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
          post({
            type: 'persist-state', id, persist,
            note: persist
              ? `persistence on — datadir snapshots to OPFS ${CLIENT === 'psql' ? 'when the session goes idle' : 'after each statement'}`
              : undefined,
          });
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
