// backend.js — bridges the design's `window.pgrust.exec(sql)` REPL contract to
// OUR wasm32-wasip1 single-user engine running in worker.js.
//
// The design (project/pgrust.dc.html) ships a mock backend and documents a
// pluggable hook:
//   window.pgrust.setBackend({ async exec(sql) -> result })
// where result is one of:
//   { kind:'table',  columns, rows, aligns, durationMs }
//   { kind:'command', text }      // psql-style command tag (CREATE TABLE, …)
//   { kind:'notice'|'error', text }
//   { kind:'raw', text, rowCount } // pre-rendered multi-line text
//
// Our engine is `postgres --single` over a long-lived in-memory VFS (worker.js):
// state PERSISTS across statements/Runs until Reset. We honor that single-user
// model by routing each SQL statement through the SAME long-lived worker, so the
// whole session is one persistent database — exactly the existing semantics.
//
// We parse the raw single-user `printtup` debug stream ourselves (rather than
// using format.js's ASCII table) so we can hand the design structured
// columns/rows/aligns and let ITS terminal renderer draw the table pixel-perfect.

import { stripLogPrefix } from './format.js';

// ---- worker plumbing: one long-lived worker, request/response by id ----------
let worker = null;
let readyPromise = null;
let nextId = 1;
const pending = new Map(); // id -> { resolve, reject }

// One build for every engine: the wasm32-wasip1 module (standard exnref
// wasm-EH + baseline post-MVP features, no SIMD/memory64) loads in Chrome,
// Firefox, and WebKit/Safari including iOS — the old wasm64-desktop /
// wasm32-safari dual-asset split is gone.
//
// TWO engine MODES over that one asset (worker.js picks by JSPI feature
// detection, ?engineMode= overrides): 'wire' = one long-lived protocol
// session (temp tables / prepared statements / transactions span REPL
// lines); 'single' = the original per-statement --single model (WebKit/
// Safari fallback — no JSPI there as of mid-2026).
const BUILD_INFO = {
  wire: {
    build: 'wasip1',
    label: 'wasm32-wasip1 · protocol session',
    shortLabel: 'wasm · session',
    note: '',
  },
  single: {
    build: 'wasip1',
    label: 'wasm32-wasip1 · single-user',
    shortLabel: 'wasm · single',
    note: '',
  },
};

let engineMode = 'single'; // updated by the worker's build/ready messages

export function getBuildInfo() {
  return BUILD_INFO[engineMode] || BUILD_INFO.single;
}

export function getEngineMode() { return engineMode; }

function ensureWorker() {
  if (worker) return readyPromise;
  const params = new URLSearchParams(window.location.search);
  const assetEncoding = params.get('assetEncoding');
  const workerParams = new URLSearchParams();
  if (assetEncoding) workerParams.set('assetEncoding', assetEncoding);
  const engineOverride = params.get('engineMode');
  if (engineOverride) workerParams.set('engineMode', engineOverride);
  const qs = workerParams.toString();
  const workerUrl = qs ? `./worker.js?${qs}` : './worker.js';
  worker = new Worker(workerUrl, { type: 'module' });
  readyPromise = new Promise((resolve, reject) => {
    worker.onmessage = (ev) => {
      const m = ev.data;
      if (m.type === 'build') { if (m.engine) engineMode = m.engine; return; }
      if (m.type === 'ready') {
        if (m.engine) engineMode = m.engine;
        persistState = { persist: !!m.persist, available: !!m.persistAvailable, restored: !!m.restored };
        if (onPersist) onPersist(persistState, null);
        resolve();
        return;
      }
      if (m.type === 'persist-state') {
        persistState = { ...persistState, persist: !!m.persist };
        if (onPersist) onPersist(persistState, m.note || null);
        if (m.id != null && pending.has(m.id)) {
          const p = pending.get(m.id); pending.delete(m.id); p.resolve({ persist: !!m.persist, note: m.note || null });
        }
        return;
      }
      if (m.type === 'status') { /* boot progress — surfaced via onStatus hook */ if (onStatus) onStatus(m.text); return; }
      if (m.type === 'reset-done') {
        const p = pending.get(m.id); if (p) { pending.delete(m.id); p.resolve({ ok: true }); }
        return;
      }
      if (m.type === 'result') {
        const p = pending.get(m.id);
        if (p) { pending.delete(m.id); p.resolve(m); }
        return;
      }
      if (m.type === 'error') {
        // A boot-time error rejects the ready promise; a per-request error
        // rejects that request.
        if (m.id != null && pending.has(m.id)) {
          const p = pending.get(m.id); pending.delete(m.id); p.reject(new Error(m.message));
        } else {
          reject(new Error(m.message));
        }
        return;
      }
    };
    worker.onerror = (e) => reject(new Error(e.message || 'worker error'));
  });
  return readyPromise;
}

let onStatus = null;
export function setStatusListener(fn) { onStatus = fn; }

// ---- persistence (OPFS snapshot toggle; worker owns the mechanics) ----------
let persistState = { persist: false, available: false, restored: false };
let onPersist = null;
export function setPersistListener(fn) { onPersist = fn; }
export function getPersistState() { return persistState; }

export async function setPersist(on) {
  await ensureWorker();
  const id = nextId++;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    worker.postMessage({ type: 'persist', id, on: !!on });
  });
}

export async function bootBackend() { return ensureWorker(); }

function postRun(sql) {
  const id = nextId++;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    worker.postMessage({ type: 'run', id, sql });
  });
}

export async function resetBackend() {
  await ensureWorker();
  const id = nextId++;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    worker.postMessage({ type: 'reset', id });
  });
}

// ---- structured parser of the single-user `printtup` raw stream --------------
// Same shape format.js parses, but we keep typeids so we can right-align numeric
// columns the way psql/the design do, and we detect NULLs (no `= "..."`).
// Returns null when the statement produced no result tuples (DDL/DML/empty).
const NUMERIC_TYPEIDS = new Set([
  20, 21, 23, // int8, int2, int4
  700, 701,   // float4, float8
  1700,       // numeric
  26, 28, 29, // oid, xid, cid
  790,        // money
]);

function parseTable(raw) {
  if (!raw) return null;
  const text = raw.replace(/^(?:backend>\s?)+/gm, '');
  const startRe = /^\t *(?:\d+:|----)/gm;
  const records = [];
  const marks = [];
  let mark;
  while ((mark = startRe.exec(text)) !== null) marks.push(mark.index);
  for (let k = 0; k < marks.length; k++) {
    const end = k + 1 < marks.length ? marks[k + 1] : text.length;
    records.push(text.slice(marks[k], end));
  }

  const cols = [];      // [{ name, typeid }]
  const rows = [];
  let curRow = null;
  let sawSeparator = false;
  let anyTable = false;

  for (const rec of records) {
    const body = rec.replace(/^\t/, '');

    if (body.startsWith('----')) {
      if (curRow) { rows.push(curRow); curRow = null; }
      sawSeparator = true;
      continue;
    }
    const m = /^\s*(\d+):\s?([\s\S]*)$/.exec(body);
    if (!m) continue;
    const idx = parseInt(m[1], 10) - 1;
    let rest = m[2];
    // pull the typeid out of the trailing "(typeid = NN, ...)" descriptor
    let typeid = null;
    const desc = /\(typeid\s*=\s*(\d+)/.exec(rest);
    if (desc) typeid = parseInt(desc[1], 10);
    const tabAt = rest.lastIndexOf('\t(');
    if (tabAt !== -1) rest = rest.slice(0, tabAt);

    if (!sawSeparator) {
      cols[idx] = { name: rest.trim(), typeid };
      anyTable = true;
    } else {
      if (!curRow) curRow = new Array(cols.length).fill(null);
      const vm = /=\s*"([\s\S]*)"\s*$/.exec(rest);
      curRow[idx] = vm ? vm[1] : null; // no quoted value => NULL
    }
  }
  if (curRow) rows.push(curRow);
  if (!anyTable) return null;

  const columns = cols.map((c) => (c ? c.name : '?'));
  const aligns = cols.map((c) => (c && NUMERIC_TYPEIDS.has(c.typeid) ? 'r' : 'l'));
  // Render NULL as empty string for the table cells (psql shows blank; the
  // design's renderer stringifies, so '' keeps NULLs blank).
  const outRows = rows.map((r) => r.map((v) => (v == null ? '' : v)));
  return { columns, rows: outRows, aligns };
}

// Diagnostics that the single-user backend writes to STDERR. Each line carries
// the engine's default log_line_prefix '%m [%p] ' (C-faithful), so strip it
// before anchoring on the severity keyword — see stripLogPrefix in format.js.
// Exported so the gates (wasm-web-e2e.sh, verify.html) can drive the REAL
// parser against the engine's real stderr.
export function parseDiagnostics(stderr) {
  if (!stderr) return { error: null, notices: [] };
  let error = null;
  const notices = [];
  for (const lineRaw of stderr.split('\n')) {
    const line = stripLogPrefix(lineRaw.trim());
    const m = /^(ERROR|FATAL|PANIC):\s*([\s\S]*)$/.exec(line);
    if (m) { if (!error) error = m[2]; continue; }
    const w = /^(WARNING|NOTICE|HINT|DETAIL):\s*([\s\S]*)$/.exec(line);
    if (w) notices.push(w[0]);
  }
  return { error, notices };
}

// Derive a psql-style command tag for a non-SELECT statement (single-user mode
// doesn't print one). Best-effort; covers the common verbs.
function commandTag(sql) {
  const s = sql.trim().replace(/^\(+/, '').toLowerCase();
  const word = (s.match(/^[a-z]+/) || [''])[0];
  switch (word) {
    case 'create': {
      const what = (s.match(/^create\s+(?:or\s+replace\s+)?(?:temp(?:orary)?\s+|unlogged\s+|global\s+|local\s+)?([a-z]+)/) || [])[1];
      return 'CREATE ' + (what ? what.toUpperCase() : 'OBJECT');
    }
    case 'drop': {
      const what = (s.match(/^drop\s+([a-z]+)/) || [])[1];
      return 'DROP ' + (what ? what.toUpperCase() : 'OBJECT');
    }
    case 'alter': {
      const what = (s.match(/^alter\s+([a-z]+)/) || [])[1];
      return 'ALTER ' + (what ? what.toUpperCase() : 'OBJECT');
    }
    case 'insert': return 'INSERT 0';
    case 'update': return 'UPDATE';
    case 'delete': return 'DELETE';
    case 'truncate': return 'TRUNCATE TABLE';
    case 'begin': case 'start': return 'BEGIN';
    case 'commit': case 'end': return 'COMMIT';
    case 'rollback': case 'abort': return 'ROLLBACK';
    case 'set': return 'SET';
    case 'reset': return 'RESET';
    case 'copy': return 'COPY';
    case 'vacuum': return 'VACUUM';
    case 'analyze': return 'ANALYZE';
    case 'comment': return 'COMMENT';
    case 'grant': return 'GRANT';
    case 'revoke': return 'REVOKE';
    case 'explain': return null; // EXPLAIN prints rows (handled as a table)
    case 'with': case 'select': case 'values': case 'table': case 'show': return null;
    default: return word ? word.toUpperCase() : null;
  }
}

// Map a wire-mode result (worker.js summarizeWire) onto the design's result
// kinds. Errors/notices arrive as protocol messages (ErrorResponse/
// NoticeResponse), the command tag is the REAL backend tag — no stderr
// scraping and no tag guessing on this path.
function execWireResult(m) {
  const w = m.wire;
  if (w.error) return { kind: 'error', text: w.error.message, durationMs: m.ms };
  if (w.columns && w.columns.length) {
    const rows = w.rows.map((r) => r.map((v) => (v == null ? '' : v)));
    if (rows.length === 1 && w.columns.length === 1 &&
        typeof rows[0][0] === 'string' && rows[0][0].includes('\n')) {
      return { kind: 'raw', text: rows[0][0], rowCount: 1, durationMs: m.ms };
    }
    const columns = w.columns.map((c) => c.name);
    const aligns = w.columns.map((c) => (NUMERIC_TYPEIDS.has(c.typeid) ? 'r' : 'l'));
    return { kind: 'table', columns, rows, aligns, durationMs: m.ms };
  }
  if (w.notices.length) return { kind: 'notice', text: w.notices.join('\n'), durationMs: m.ms };
  return { kind: 'command', text: w.tag || '', durationMs: m.ms };
}

// ---- the exec() the design's REPL calls --------------------------------------
// One SQL statement in (the design splits on ';' and calls us per statement);
// one result object out.
async function exec(sql) {
  await ensureWorker();
  const m = await postRun(sql);
  if (m.engine === 'wire' && m.wire) return execWireResult(m);
  const { error, notices } = parseDiagnostics(m.stderr);
  if (error) return { kind: 'error', text: error, durationMs: m.ms };

  const table = parseTable(m.stdout);
  if (table) {
    // A single 1x1 cell whose value is itself multi-line text (for example the
    // Mandelbrot demo) renders as intended when shown as raw preformatted text.
    if (table.rows.length === 1 && table.columns.length === 1 &&
        typeof table.rows[0][0] === 'string' && table.rows[0][0].includes('\n')) {
      return { kind: 'raw', text: table.rows[0][0], rowCount: 1, durationMs: m.ms };
    }
    return { kind: 'table', columns: table.columns, rows: table.rows, aligns: table.aligns, durationMs: m.ms };
  }
  // No result tuples. If the backend emitted notices, show them; otherwise a
  // command tag for the verb (CREATE TABLE, INSERT 0, …).
  if (notices.length) return { kind: 'notice', text: notices.join('\n'), durationMs: m.ms };
  const tag = commandTag(sql);
  if (tag) return { kind: 'command', text: tag, durationMs: m.ms };
  return { kind: 'command', text: '', durationMs: m.ms };
}

// Install ourselves as the design's pluggable backend.
export function installBackend() {
  if (!window.pgrust) {
    window.pgrust = {
      _backend: { exec },
      setBackend(b) { this._backend = b; },
      exec(sql) { return this._backend.exec(sql); },
    };
  } else {
    window.pgrust.setBackend({ exec });
  }
}
