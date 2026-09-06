#!/usr/bin/env node
// run-node-wire-threads.mjs — drive the wasm32-wasip1-threads build of
// postgres.wasm in protocol mode under Node, with NO JSPI anywhere.
//
// Shape (spike/wasip1-threads, checkpoint (a)):
//
//   main thread (this file)          process worker            session worker
//   ------------------------         ---------------           --------------
//   compile module                   instantiate                (prewarmed)
//   create SHARED memory  ---------> same memory   -----------> same memory
//   create SabPipe pair   ---------> fd 0 / fd 1   -----------> fd 0 / fd 1
//   drive pgwire frames              _start()
//                                      pg_main
//                                        --stdio-wire-threaded
//                                          thread::spawn -----> wasi_thread_start
//                                          join (futex park)      the WHOLE
//                                                                 postgres
//                                                                 session
//
// The wasm `_start` runs in a worker_threads Worker rather than here so that
// (a) Atomics.wait is legal on the guest's blocking read, and (b) this thread
// stays free to pump the pipes — the guest parks the JS thread it runs on the
// moment it joins.
//
// Usage: node run-node-wire-threads.mjs [--stderr FILE] [--pool N]
// Env: PGRUST_WASM_THREADS (path to the threads postgres.wasm),
//      PGRUST_VFS (prefix for vfs.img/vfs.json),
//      PGRUST_WIRE_GUCS (comma-separated extra -c GUCs; default pins
//      timezone=UTC,log_timezone=UTC for transcript identity with the native
//      arm), PGRUST_THREADS_TIMEOUT_MS.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { SabPipe } from './sab-pipe.js';
import {
  createSharedMemory,
  inspectImports,
  makeWorker,
  onWorkerMessage,
  onWorkerError,
  newMessageChannel,
  onPortMessage,
  threadWorkerUrl,
} from './threads-host.js';
import {
  WireReader,
  encodeStartup,
  encodeQuery,
  TERMINATE,
  canonMessage,
  parseMessage,
} from './wire.js';
// argv ONLY — nothing from wiresession.js's JSPI session class is
// constructed here (see the no-JSPI note in threads-host.js).
import { defaultWireArgv } from './wiresession.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const wasmPath =
  process.env.PGRUST_WASM_THREADS ||
  path.join(here, '..', 'target', 'wasm32-wasip1-threads', 'wasm-release', 'postgres.wasm');
const vfsPrefix = process.env.PGRUST_VFS || path.join(here, 'assets', 'vfs');
const TIMEOUT_MS = Number(process.env.PGRUST_THREADS_TIMEOUT_MS || 180000);

function argAfter(flag) {
  const i = process.argv.indexOf(flag);
  return i !== -1 ? process.argv[i + 1] : undefined;
}
const stderrFile = argAfter('--stderr');
// --dispatch stdio-wire is the CONTROL arm (same module, same worker, session
// on the process instance's own thread, thread-spawn never called); --trace N
// keeps the last N WASI calls per instance and dumps them with any guest abort.
const dispatch = argAfter('--dispatch') || 'stdio-wire-threaded';
const TRACE = Number(argAfter('--trace') || 0);
// --pool N sizes the prewarmed wasi-thread pool. The guest asks for more than
// one thread now: the session thread (threaded dispatch only) AND the
// pg-timeout-timer thread that statement_timeout needs, plus whatever the
// executor runtime wants if it is ever enabled here. thread-spawn cannot
// create a worker on demand (it may not await), so an undersized pool is a
// hard -EAGAIN — every refusal is logged below.
const POOL_SIZE = Number(argAfter('--pool') || 4);
const errFd = stderrFile ? fs.openSync(stderrFile, 'w') : 2;

const failures = [];
const note = (line) => process.stdout.write(line + '\n');

// The same GUC argv as the single-threaded wire lane, with the threaded
// dispatch mode in argv[1].
const extraGucs = (process.env.PGRUST_WIRE_GUCS || 'timezone=UTC,log_timezone=UTC')
  .split(',')
  .filter(Boolean)
  .flatMap((g) => ['-c', g]);
const argv = defaultWireArgv(extraGucs);
if (argv[1] !== '--stdio-wire') throw new Error('defaultWireArgv changed shape');
argv[1] = `--${dispatch}`;

// ---------------------------------------------------------------------------

const wasmModule = await WebAssembly.compile(fs.readFileSync(wasmPath));
const info = inspectImports(wasmModule);
note(`imports: ${info.all.length} (memory ${info.memory.module}.${info.memory.name}, ` +
     `thread-spawn ${info.threadSpawn ? info.threadSpawn.module + '.' + info.threadSpawn.name : 'MISSING'})`);
if (!info.threadSpawn) failures.push('module does not import wasi.thread-spawn');

const image = fs.readFileSync(vfsPrefix + '.img');
const imageBuf = image.buffer.slice(image.byteOffset, image.byteOffset + image.byteLength);
const manifest = JSON.parse(fs.readFileSync(vfsPrefix + '.json', 'utf8'));

const memory = createSharedMemory();
note(`shared memory: ${memory.buffer.byteLength / 1048576}MiB initial, shared=${memory.buffer instanceof SharedArrayBuffer}`);

const stdinPipe = SabPipe.create(1 << 20);
const stdoutPipe = SabPipe.create(1 << 22);

const spawnedTids = [];
const spawnRefusals = [];
let exitCode = null;
let exitFrom = null;
const stderrChunks = [];

const worker = makeWorker(threadWorkerUrl(import.meta.url), { name: 'pgrust-process' });

// One relay channel per pool slot: a spawned thread reports to THIS thread
// directly, because the process worker is parked in a futex for the whole
// session and could not relay anything until after the fact.
const relayChannels = Array.from({ length: POOL_SIZE }, () => newMessageChannel());

let poolReady = null;
const poolReadyPromise = new Promise((r) => { poolReady = r; });
let exited = null;
const exitedPromise = new Promise((r) => { exited = r; });

function handleMessage(m) {
  switch (m.type) {
    case 'pool-ready':
      note(`host: thread pool ready (${m.size})`);
      poolReady();
      break;
    case 'imports':
      note(`host: instance imports memory=${m.memory} thread-spawn=${m.threadSpawn}`);
      break;
    case 'instantiated':
      note(`host: process instance created; exports include ${m.exports.join(',')}`);
      break;
    case 'spawn':
      spawnedTids.push(m.tid);
      note(`host: wasi thread-spawn(start_arg=${m.startArg}) by ${m.from} -> tid ${m.tid} (slot ${m.slot})`);
      break;
    case 'spawn-refused':
      spawnRefusals.push(m);
      note(`host: wasi thread-spawn(start_arg=${m.startArg}) by ${m.from} -> -EAGAIN (pool of ${m.poolSize} exhausted)`);
      break;
    case 'thread-entered':
      note(`host: wasi_thread_start(tid=${m.tid}, start_arg=${m.startArg}) entered`);
      break;
    case 'thread-done':
      note(`host: thread ${m.tid} returned from wasi_thread_start`);
      for (const line of m.trace || []) note(`host: trace ${line}`);
      break;
    case 'stderr': {
      const b = Buffer.from(m.bytes);
      stderrChunks.push(b);
      fs.writeSync(errFd, b);
      break;
    }
    case 'exit':
      note(`host: guest exit ${m.code} (from ${m.from}${m.tid ? ' tid ' + m.tid : ''})`);
      exitCode = m.code;
      exitFrom = m.from;
      exited();
      break;
    case 'error':
      failures.push(`worker error (${m.from || '?'}): ${m.message}`);
      note(`host: ERROR ${m.message}`);
      for (const line of m.trace || []) note(`host: trace ${line}`);
      exited();
      break;
    case 'log':
      note(`host: ${m.text}`);
      break;
    default:
      note(`host: unhandled message ${JSON.stringify(m.type)}`);
  }
}
onWorkerMessage(worker, handleMessage);
onWorkerError(worker, (e) => {
  failures.push(`process worker threw: ${e && e.stack ? e.stack : e}`);
  exited();
});

worker.postMessage(
  {
    role: 'process',
    module: wasmModule,
    memory,
    image: imageBuf,
    manifest,
    stdin: stdinPipe.descriptor(),
    stdout: stdoutPipe.descriptor(),
    argv,
    env: {
      USER: 'postgres',
      PGRUST_TZDIR: '/share/timezone',
      PGRUST_PGSHAREDIR: '/share',
      PGRUST_RUNTIME: '0',
      RUST_BACKTRACE: '1',
    },
    poolSize: POOL_SIZE,
    trace: TRACE,
    relayPorts: relayChannels.map((c) => c.port2),
  },
  [imageBuf, ...relayChannels.map((c) => c.port2)],
);
for (const c of relayChannels) onPortMessage(c.port1, (m) => handleMessage(m));

// ---------------------------------------------------------------------------
// The pgwire pump: this thread never blocks (Atomics.waitAsync inside
// SabPipe.readAsync).
// ---------------------------------------------------------------------------
const reader = new WireReader();
const inbox = [];
let pumpDone = false;

const pump = (async () => {
  const scratch = new Uint8Array(65536);
  for (;;) {
    const n = await stdoutPipe.readAsync(scratch, scratch.length);
    if (n === 0) break;
    reader.feed(scratch.slice(0, n));
    for (;;) {
      const msg = reader.next();
      if (!msg) break;
      inbox.push(msg);
    }
  }
  pumpDone = true;
})();

function send(bytes) {
  const n = stdinPipe.write(bytes, { block: false });
  if (n !== bytes.length) throw new Error(`stdin ring full (${n}/${bytes.length})`);
}

const deadline = Date.now() + TIMEOUT_MS;
async function untilReadyForQuery(what) {
  const out = [];
  for (;;) {
    while (inbox.length) {
      const m = inbox.shift();
      out.push(m);
      if (m.t === 'Z') return out;
    }
    if (pumpDone) throw new Error(`stdout closed while waiting for ReadyForQuery after ${what}`);
    if (Date.now() > deadline) throw new Error(`timeout waiting for ReadyForQuery after ${what}`);
    await new Promise((r) => setTimeout(r, 2));
  }
}

const rows = new Map(); // sql -> [values...]
const timings = new Map(); // sql -> ms from sending Q to ReadyForQuery
const sqlstates = new Map(); // sql -> [SQLSTATE of every ErrorResponse]

// One simple-query round trip, timed from the Q write to ReadyForQuery. This
// is the measurement the timeout tests turn on: the guest's wall time, seen
// from outside, with no clock of the guest's involved.
async function runQuery(sql, { expectError = false } = {}) {
  note(`>>> Q ${sql}`);
  const t0 = Date.now();
  send(encodeQuery(sql));
  const msgs = await untilReadyForQuery(sql);
  const ms = Date.now() - t0;
  const got = [];
  const states = [];
  for (const { t, body } of msgs) {
    note(canonMessage(t, body));
    if (t === 'D') got.push(canonMessage(t, body).slice(2));
    if (t === 'E') {
      states.push(parseMessage(t, body).fields.C || '?');
      if (!expectError) failures.push(`error running ${sql}`);
    }
  }
  rows.set(sql, got);
  timings.set(sql, ms);
  sqlstates.set(sql, states);
  note(`--- ${sql} took ${ms}ms${states.length ? ` sqlstate=${states.join(',')}` : ''}`);
  return { msgs, ms, rows: got, sqlstates: states };
}

try {
  await Promise.race([
    poolReadyPromise,
    new Promise((_, rej) => setTimeout(() => rej(new Error('pool prewarm timeout')), 60000)),
  ]);

  send(encodeStartup({ user: 'postgres', database: 'postgres', application_name: 'threads-spike' }));
  const handshake = await untilReadyForQuery('startup');
  let sawAuthOk = false;
  let sawKey = false;
  for (const { t, body } of handshake) {
    note(canonMessage(t, body));
    if (t === 'R') sawAuthOk = new DataView(body.buffer, body.byteOffset).getInt32(0, false) === 0;
    else if (t === 'K') sawKey = true;
    else if (t === 'E') failures.push('error during handshake');
  }
  if (!sawAuthOk) failures.push('no AuthenticationOk in handshake');
  if (!sawKey) failures.push('no BackendKeyData in handshake');

  const statements = [
    'SELECT 1',
    'SELECT count(*) FROM pg_class',
    'CREATE TABLE spike_t(a int)',
    'INSERT INTO spike_t SELECT generate_series(1,1000)',
    'SELECT sum(a) FROM spike_t',
  ];
  for (const sql of statements) {
    await runQuery(sql);
  }

  // ---- timed waits and timeouts -----------------------------------------
  // Both exercise the same two pieces: the pg-timeout-timer thread (a real
  // wasi thread now) and the timed latch park pg_sleep loops on. The first
  // proves the park actually sleeps rather than spinning or returning at
  // once; the second proves a DIFFERENT thread can interrupt it.
  await runQuery('SELECT pg_sleep(0.2)');
  await runQuery("SET statement_timeout = '300ms'");
  await runQuery('SELECT pg_sleep(5)', { expectError: true });
  await runQuery('RESET statement_timeout');
  await runQuery('SELECT 2');

  note('>>> X');
  send(TERMINATE);
  stdinPipe.close();
  await Promise.race([
    exitedPromise,
    new Promise((_, rej) => setTimeout(() => rej(new Error('timeout waiting for guest exit')), 60000)),
  ]);
  await Promise.race([pump, new Promise((r) => setTimeout(r, 2000))]);
  note(`=== exit ${exitCode} (from ${exitFrom})`);
} catch (e) {
  failures.push(`driver exception: ${e && e.stack ? e.stack : e}`);
}

// ---- assertions -----------------------------------------------------------
const expect = (cond, msg) => { if (!cond) failures.push(msg); };

// The control arm (dispatch stdio-wire) runs the session on the wasm main
// thread and never spawns; it passes on the wire results alone.
if (dispatch === 'stdio-wire-threaded') {
  expect(spawnedTids.length >= 1, 'no thread was spawned via wasi.thread-spawn');
}
expect(spawnedTids.every((t) => t > 0), `thread ids must be positive: ${spawnedTids}`);
note(`spawned thread ids: [${spawnedTids.join(', ')}] (pool ${POOL_SIZE}, ${spawnRefusals.length} EAGAIN refusals)`);
expect(spawnRefusals.length === 0, `${spawnRefusals.length} thread-spawn refusals (pool of ${POOL_SIZE} too small)`);

const r1 = rows.get('SELECT 1') || [];
expect(r1.length === 1 && r1[0] === '1', `SELECT 1 returned ${JSON.stringify(r1)}`);

const rc = rows.get('SELECT count(*) FROM pg_class') || [];
expect(rc.length === 1 && Number(rc[0]) > 0, `pg_class count returned ${JSON.stringify(rc)}`);

const rs = rows.get('SELECT sum(a) FROM spike_t') || [];
expect(rs.length === 1 && rs[0] === '500500', `sum(a) returned ${JSON.stringify(rs)}`);

// pg_sleep(0.2) must actually sleep: a poll_oneoff that answers "already
// fired" or a latch park that returns immediately would come back in ~0ms.
const sleepMs = timings.get('SELECT pg_sleep(0.2)');
note(`timing: pg_sleep(0.2) round trip ${sleepMs}ms`);
expect(sleepMs !== undefined && sleepMs >= 190, `pg_sleep(0.2) took ${sleepMs}ms (want >= 190)`);

// statement_timeout must cancel a running pg_sleep(5) from the timer thread.
const cancelMs = timings.get('SELECT pg_sleep(5)');
const cancelStates = sqlstates.get('SELECT pg_sleep(5)') || [];
note(`timing: pg_sleep(5) under statement_timeout=300ms cancelled after ${cancelMs}ms, sqlstate ${cancelStates.join(',') || 'NONE'}`);
expect(
  cancelStates.includes('57014'),
  `pg_sleep(5) under statement_timeout returned sqlstates ${JSON.stringify(cancelStates)} (want 57014)`,
);
expect(
  cancelMs !== undefined && cancelMs < 2000,
  `statement_timeout cancel took ${cancelMs}ms (want < 2000)`,
);

// ...and the session must survive the cancel.
const r2 = rows.get('SELECT 2') || [];
expect(r2.length === 1 && r2[0] === '2', `SELECT 2 after the cancel returned ${JSON.stringify(r2)}`);

expect(exitCode === 0, `guest exit code ${exitCode} (want 0)`);

try { worker.terminate(); } catch { /* already gone */ }

if (failures.length) {
  for (const f of failures) note(`DRIVER-FAIL: ${f}`);
  note('VERDICT: threads-node FAIL');
  process.exit(1);
}
note('VERDICT: threads-node PASS');
process.exit(0);
