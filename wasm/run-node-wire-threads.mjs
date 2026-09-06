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
// STORAGE (--fs). `--fs copy` (the default) is checkpoint (a)'s arrangement: every
// worker builds its OWN Vfs from its own copy of the packed image, so the process
// instance and the session instance have independent filesystems. `--fs broker`
// replaces that with ONE store in a dedicated coordinator worker
// (wasm/storage-worker.js): this thread mints a doorbell and POOL_SIZE+1
// SharedArrayBuffer channels, starts the coordinator FIRST and waits for it to seed
// the store from the packed image, and only then creates the process worker — which
// hands one channel to each prewarmed pool worker and keeps one for itself. The
// packed image is transferred to the coordinator and to nobody else.
//
// THE POSTMASTER LANE (--dispatch postmaster). The two dispatches above run ONE
// session on fds 0/1. `--host-pipes` runs the REAL postmaster instead
// (crates/backend/libpq/pqcomm_hostpipes): this driver creates a listener pipe
// and one (in, out) SabPipe pair per session, registers all of them in the
// host's pipe-fd registry (wasm/threads-host.js), starts the guest, waits for
// "ready to accept connections" on stderr, and then announces each connection
// with a 16-byte HPGP record on the listener. Two backends on two wasi threads
// then run wasm/hostpipes-scenario.js — the same scenario the native arm runs.
// fd numbering is threads-host.js's: listener 1000, session k at 1001+2k /
// 1002+2k. `--fs copy` only in this lane.
//
// Usage: node run-node-wire-threads.mjs [--stderr FILE] [--pool N]
//        [--dispatch stdio-wire|stdio-wire-threaded|postmaster] [--fs copy|broker]
//        [--trace N]
// Env: PGRUST_WASM_THREADS (path to the threads postgres.wasm),
//      PGRUST_VFS (prefix for vfs.img/vfs.json),
//      PGRUST_REPACKED_BUNDLE (URL of the @pgxsinkit/pglite-opfs-repacked browser
//      bundle; defaults to ./vendor/pglite-opfs-repacked.js — see wasm/broker-fs.js),
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
  storageWorkerUrl,
  PipeRegistry,
  HOSTPIPES_LISTEN_FD,
  sessionFds,
} from './threads-host.js';
import { runHostPipesScenario } from './hostpipes-scenario.js';
import { loadRepackedBundle, repackedBundleUrl } from './broker-fs.js';
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
if (!['stdio-wire', 'stdio-wire-threaded', 'postmaster'].includes(dispatch)) {
  throw new Error(`unknown --dispatch ${dispatch}`);
}
// The postmaster lane: a real PostmasterMain over host-pipes fds, N backends.
const POSTMASTER = dispatch === 'postmaster';
const TRACE = Number(argAfter('--trace') || 0);
// --pool N sizes the prewarmed wasi-thread pool. The guest asks for more than
// one thread now: the session thread (threaded dispatch only) AND the
// pg-timeout-timer thread that statement_timeout needs, plus whatever the
// executor runtime wants if it is ever enabled here. thread-spawn cannot
// create a worker on demand (it may not await), so an undersized pool is a
// hard -EAGAIN — every refusal is logged below.
// The postmaster wants far more than the wire lanes: the startup process, the
// checkpointer, the background writer, the WAL writer, the memory watchdog
// sampler, the pg-timeout-timer, the autovacuum launcher when it is not off,
// two session backends — plus headroom. Slots are RECLAIMED when a guest
// thread returns (the startup process does), held for the life of the process
// when it does not.
const POOL_SIZE = Number(argAfter('--pool') || (POSTMASTER ? 12 : 4));
// --fs broker routes every guest FILE call to one store in a coordinator worker; --fs copy
// (default) keeps checkpoint (a)'s per-worker private copy of the packed image.
const FS_MODE = argAfter('--fs') || 'copy';
if (FS_MODE !== 'copy' && FS_MODE !== 'broker') throw new Error(`unknown --fs ${FS_MODE}`);
// The postmaster lane's default is `--fs copy`, and that is the arm this bite
// scores. `--fs broker` is NOT refused: it was probed here and passes too, and
// it is the arm on which the shutdown path's explicit CHECKPOINT actually
// completes (under copy the checkpointer has its own private Vfs and cannot
// see the backends' relation files — see postmasterShutdown below).
const BUNDLE_URL = process.env.PGRUST_REPACKED_BUNDLE || repackedBundleUrl(import.meta.url);
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
if (POSTMASTER) {
  // `--host-pipes` picks a TRANSPORT and then falls through to the normal
  // postmaster dispatch, so the argv is PostmasterMain's, not a single
  // backend's: no trailing dbname (main_entry's getopt would reject it as an
  // invalid argument), and the two GUCs that make the host fd the only way in.
  argv[1] = '--host-pipes';
  if (argv[argv.length - 1] !== 'postgres') throw new Error('defaultWireArgv changed shape');
  argv.pop();
  argv.push('-c', 'listen_addresses=', '-c', 'unix_socket_directories=');
  // So the checkpoint the shutdown path can (or cannot) run is visible in the log.
  argv.push('-c', 'log_checkpoints=on');
}

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
note(`storage: --fs ${FS_MODE}`);

// ---------------------------------------------------------------------------
// --fs broker: the storage coordinator, started BEFORE anything else. Its store must be
// seeded and every channel attached before the first backend can ask for a file — and once
// its blocking serveForever() loop is entered it never reaches its event loop again, so
// there is no attaching anything afterwards.
// ---------------------------------------------------------------------------
let storageWorker = null;
let doorbell = null;
let channels = [];
let storageStoppedResolve = null;
const storageStoppedPromise = new Promise((r) => { storageStoppedResolve = r; });

if (FS_MODE === 'broker') {
  const bundle = await loadRepackedBundle(BUNDLE_URL);
  note(`storage: bundle ${BUNDLE_URL}`);
  doorbell = bundle.RepackedDoorbell.create();
  // One channel per pool slot PLUS one for the process instance: the protocol is one request
  // in flight per channel, so two agents may never share one.
  channels = Array.from({ length: POOL_SIZE + 1 }, (_unused, i) =>
    bundle.RepackedChannel.create({ id: i + 1, doorbell }),
  );
  storageWorker = makeWorker(storageWorkerUrl(import.meta.url), { name: 'pgrust-storage' });
  let storageReady = null;
  const storageReadyPromise = new Promise((r) => { storageReady = r; });
  onWorkerMessage(storageWorker, (m) => {
    switch (m.type) {
      case 'storage-ready':
        note(
          `storage: coordinator ready — seeded ${m.files} files / ${m.dirs} dirs ` +
            `(${m.bytes} bytes) in ${m.seedMs}ms; /pgdata holds ${m.datadirFiles} files ` +
            `(${m.datadirBytes} bytes); arena ${(m.arenaBytes / 1048576).toFixed(1)}MiB ` +
            `at ${m.extentSize}B extents; channels [${m.channels.join(', ')}]`,
        );
        storageReady();
        break;
      case 'storage-log':
        note(`storage: ${m.text}`);
        break;
      case 'storage-stopped':
        note(
          `storage: coordinator stopped — /pgdata now holds ${m.datadirFiles} files ` +
            `(${m.datadirBytes} bytes) against ${m.seededFiles} files (${m.seededBytes} bytes) at seed: ` +
            `the session's writes landed in the ONE store (delta ${m.datadirFiles - m.seededFiles} files, ` +
            `${m.datadirBytes - m.seededBytes} bytes)`,
        );
        storageStoppedResolve();
        break;
      case 'storage-error':
        failures.push(`storage worker error: ${m.message}`);
        note(`storage: ERROR ${m.message}`);
        storageReady();
        storageStoppedResolve();
        break;
      default:
        note(`storage: unhandled message ${JSON.stringify(m.type)}`);
    }
  });
  onWorkerError(storageWorker, (e) => {
    failures.push(`storage worker threw: ${e && e.stack ? e.stack : e}`);
    storageStoppedResolve();
  });
  storageWorker.postMessage(
    {
      kind: 'boot',
      bundleUrl: BUNDLE_URL,
      image: imageBuf,
      manifest,
      channels: channels.map((c) => c.transfer()),
      doorbell: doorbell.buffer,
      options: {},
    },
    [imageBuf],
  );
  await Promise.race([
    storageReadyPromise,
    new Promise((_, rej) => setTimeout(() => rej(new Error('storage coordinator seed timeout')), 120000)),
  ]);
}

// With --fs broker the packed image now lives in the coordinator's store (and its ArrayBuffer
// was transferred there); every instance gets an EMPTY base Vfs, which the WASI adapter sits on
// top of and never consults.
const guestImage = FS_MODE === 'broker' ? new ArrayBuffer(0) : imageBuf;
const guestManifest = FS_MODE === 'broker' ? { dirs: ['/'], files: [] } : manifest;

const stdinPipe = SabPipe.create(1 << 20);
const stdoutPipe = SabPipe.create(1 << 22);

// ---------------------------------------------------------------------------
// --dispatch postmaster: the host side of the host-pipes fd contract. Every
// pipe is created HERE, before the guest starts, and travels to the process
// worker and to every pool worker as SharedArrayBuffer descriptors — a backend
// thread must read the same ring the driver writes, and its `secure_close`
// must be visible to us (wasm/threads-host.js, PipeRegistry).
// ---------------------------------------------------------------------------
// Two for the scenario (A, B) plus one for the shutdown path's explicit
// CHECKPOINT (C): every pipe has to exist before the guest starts, because the
// registry is handed to the pool workers at prewarm.
const SESSION_COUNT = 3;
const CONN_MAGIC = 0x50475048; // "HPGP" in stream order
const pipeRegistry = new PipeRegistry();
let listenerPipe = null;
const sessionPipes = [];
if (POSTMASTER) {
  listenerPipe = SabPipe.create(1 << 12); // 16-byte records; a page is plenty
  pipeRegistry.register(HOSTPIPES_LISTEN_FD, { in: listenerPipe });
  for (let k = 0; k < SESSION_COUNT; k++) {
    const { inFd, outFd } = sessionFds(k);
    const toGuest = SabPipe.create(1 << 20); // driver -> backend (guest READS)
    const fromGuest = SabPipe.create(1 << 22); // backend -> driver (guest WRITES)
    pipeRegistry.register(inFd, { in: toGuest });
    pipeRegistry.register(outFd, { out: fromGuest });
    sessionPipes.push({ k, inFd, outFd, toGuest, fromGuest });
  }
  note(
    `host-pipes: listener fd ${HOSTPIPES_LISTEN_FD}; sessions ` +
      sessionPipes.map((p) => `${p.k}=(in ${p.inFd}, out ${p.outFd})`).join(', '),
  );
}
const pipeDescriptors = pipeRegistry.descriptors();

const spawnedTids = [];
const spawnRefusals = [];
let exitCode = null;
let exitFrom = null;
const stderrChunks = [];
let serverLog = '';

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
      // The postmaster lane waits on the server LOG (stderr) for "ready to
      // accept connections" exactly as the native driver does.
      serverLog += b.toString('utf8');
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
    image: guestImage,
    manifest: guestManifest,
    fs: FS_MODE,
    bundleUrl: BUNDLE_URL,
    channel: channels.length ? channels[0].transfer() : null,
    poolChannels: channels.slice(1).map((c) => c.transfer()),
    stdin: stdinPipe.descriptor(),
    stdout: stdoutPipe.descriptor(),
    pipes: pipeDescriptors,
    argv,
    env: {
      USER: 'postgres',
      PGRUST_TZDIR: '/share/timezone',
      PGRUST_PGSHAREDIR: '/share',
      PGRUST_RUNTIME: '0',
      RUST_BACKTRACE: '1',
      // The ONLY channel that can name a host-owned listener fd
      // (pqcomm_hostpipes::LISTEN_FD_ENV); without it PostmasterMain FATALs.
      ...(POSTMASTER
        ? {
            PGRUST_HOSTPIPES_LISTEN_FD: String(HOSTPIPES_LISTEN_FD),
            // The postmaster keeps a WARM STANDBY POOL of max_parallel_workers
            // (8) pre-spawned backend threads, and `wpool::maintain()` loops
            // `while POPULATION < target()` — POPULATION is charged by the
            // CHILD once it runs, so on a host whose thread start is not
            // instant the loop overshoots and spawns until thread-spawn fails.
            // Measured here: it claimed EVERY pool slot (12 of 12, then 20 of
            // 20) before the startup process could get its timeout-timer
            // thread, and the startup process panicked on the -EAGAIN. This is
            // the pool's own documented kill switch — no Rust change, and a
            // fixed prewarmed pool has no room for warm standbys anyway.
            PGRUST_NO_WORKER_POOL: '1',
          }
        : {}),
    },
    poolSize: POOL_SIZE,
    trace: TRACE,
    relayPorts: relayChannels.map((c) => c.port2),
  },
  [guestImage, ...relayChannels.map((c) => c.port2)],
);
for (const c of relayChannels) onPortMessage(c.port1, (m) => handleMessage(m));

// ---------------------------------------------------------------------------
// The pgwire pump: this thread never blocks (Atomics.waitAsync inside
// SabPipe.readAsync).
// ---------------------------------------------------------------------------
const reader = new WireReader();
const inbox = [];
let pumpDone = false;

// Not started in the postmaster lane: there the wire lives on the session
// pipes, and fd 1 carries nothing.
const pump = POSTMASTER
  ? Promise.resolve()
  : (async () => {
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

// ---------------------------------------------------------------------------
// --dispatch postmaster: one pgwire session per host-pipe pair.
// ---------------------------------------------------------------------------

// The one try/catch below carries both lanes; this is how the postmaster lane
// leaves it without running the wire lane's body (and without a second nested
// try that would swallow its own failures).
class LaneDone extends Error {}

const T0 = Date.now();
const stamp = () => String(Date.now() - T0).padStart(6);
const plog = (line) => note(`[${stamp()}ms] ${line}`);

function announceConnection(inFd, outFd) {
  const rec = new Uint8Array(16);
  const view = new DataView(rec.buffer);
  view.setUint32(0, CONN_MAGIC, true);
  view.setInt32(4, inFd, true);
  view.setInt32(8, outFd, true);
  view.setUint32(12, 0, true);
  const n = listenerPipe.write(rec, { block: false });
  if (n !== 16) throw new Error(`listener ring would not take a whole record (${n}/16)`);
}

// One session over one (in, out) SabPipe pair. Same shape as the native
// driver's Session, over shared-memory rings instead of OS pipes: this thread
// never blocks (readAsync), the backend thread on the other side does.
class PipeSession {
  constructor(name, toGuest, fromGuest) {
    this.name = name;
    this.toGuest = toGuest;
    this.fromGuest = fromGuest;
    this.reader = new WireReader();
    this.collector = null;
    this.closed = false;
    this.pump = (async () => {
      const scratch = new Uint8Array(65536);
      for (;;) {
        const n = await this.fromGuest.readAsync(scratch, scratch.length);
        if (n === 0) break; // the backend's secure_close closed both fds
        this._feed(scratch.slice(0, n));
      }
      this.closed = true;
    })();
  }

  _feed(bytes) {
    this.reader.feed(bytes);
    for (;;) {
      const m = this.reader.next();
      if (!m) break;
      if (!this.collector) continue; // unsolicited (NoticeResponse etc.)
      this.collector.msgs.push(m);
      if (m.t === 'Z') {
        const c = this.collector;
        this.collector = null;
        c.resolve({ msgs: c.msgs, elapsed: Date.now() - c.started });
      }
    }
  }

  _collect() {
    if (this.collector) throw new Error(`${this.name}: overlapping collection`);
    let resolve;
    const p = new Promise((r) => { resolve = r; });
    this.collector = { msgs: [], resolve, started: Date.now() };
    return p;
  }

  _write(bytes) {
    const n = this.toGuest.write(bytes, { block: false });
    if (n !== bytes.length) throw new Error(`${this.name}: session ring full (${n}/${bytes.length})`);
  }

  startup(params) {
    const p = this._collect();
    this._write(encodeStartup(params));
    return p;
  }

  // Fires without awaiting: the returned promise settles at ReadyForQuery,
  // which for a query blocked on another backend's row lock is the point.
  send(sql) {
    const p = this._collect();
    this._write(encodeQuery(sql));
    return p;
  }

  query(sql) {
    return this.send(sql);
  }

  terminate() {
    this._write(TERMINATE);
  }

  async waitClosed(timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (!this.closed && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 10));
    }
    return this.closed;
  }
}

async function waitForServerLog(re, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (re.test(serverLog)) return true;
    if (exitCode !== null) return false;
    await new Promise((r) => setTimeout(r, 20));
  }
  return false;
}

let nextSession = 0;
async function openPipeSession(name) {
  const slot = sessionPipes[nextSession++];
  if (!slot) throw new Error(`no host-pipe pair left for session ${name}`);
  plog(`announcing session ${name} on (in=${slot.inFd}, out=${slot.outFd})`);
  announceConnection(slot.inFd, slot.outFd);
  const s = new PipeSession(name, slot.toGuest, slot.fromGuest);
  slot.session = s;
  const hello = await Promise.race([
    s.startup({ user: 'postgres', database: 'postgres', application_name: `hostpipes-${name}` }),
    new Promise((_, rej) =>
      setTimeout(() => rej(new Error(`session ${name}: no handshake within 60s`)), 60000),
    ),
  ]);
  return {
    name,
    handshake: hello.msgs,
    send: (sql) => s.send(sql),
    query: (sql) => s.query(sql),
    terminate: () => s.terminate(),
    waitClosed: (ms) => s.waitClosed(ms),
  };
}

// SHUTDOWN, and what this target can honestly offer. There are no signals on
// wasm: `main_entry`'s `pqsignal` is a documented no-op there, the postmaster
// registers no THREAD signal handlers (`pqsignal_thread`) and holds no
// procsignal slot, so nothing a backend or the host can do reaches
// `handle_pm_shutdown_request_signal`. Closing the listener does NOT shut the
// postmaster down either: `accept_connection`'s EOF arm logs, backs off 100ms
// and returns Err, which ServerLoop drops on the floor. So there is NO
// SHUTDOWN CHECKPOINT here, and there cannot be one without a Rust-side wasm
// shutdown entry point. What this does instead, least destructive first:
//
//   1. ask a third session for an explicit CHECKPOINT — the closest analogue
//      that exists. Under `--fs copy` it is EXPECTED to fail: the checkpointer
//      thread has its own private Vfs copy and cannot see the relation files
//      the backends created in theirs. Recorded as a note with the exact
//      server-log reason, never as a pass/fail (it is a storage-lane property,
//      not a postmaster one);
//   2. close the listener pipe and check the postmaster NOTICED — that EOF is
//      the only thing resembling a stop signal this target can deliver, and
//      seeing it logged proves the postmaster is still alive and looping;
//   3. terminate the workers.
async function postmasterShutdown() {
  const notes = [];
  const checks = [];
  try {
    const C = await openPipeSession('C');
    const t = Date.now();
    const r = await C.query('CHECKPOINT');
    const tagList = r.msgs.filter((m) => m.t === 'C').map((m) => parseMessage('C', m.body).tag);
    const errList = r.msgs
      .filter((m) => m.t === 'E')
      .map((m) => {
        const f = parseMessage('E', m.body).fields;
        return `${f.C} ${f.M}`;
      });
    plog(`  C CHECKPOINT ${Date.now() - t}ms: ${tagList.join(',') || errList.join(' | ')}`);
    notes.push(
      tagList.includes('CHECKPOINT')
        ? 'explicit CHECKPOINT completed (checkpointer thread is live and shares the store)'
        : `explicit CHECKPOINT failed: ${errList.join(' | ') || 'no CommandComplete'} — ` +
          'expected under --fs copy, where the checkpointer has its own private Vfs',
    );
    C.terminate();
    await C.waitClosed(5000);
  } catch (e) {
    notes.push(`explicit CHECKPOINT could not be attempted: ${e && e.message ? e.message : e}`);
  }

  const before = serverLog.length;
  listenerPipe.close();
  await new Promise((r) => setTimeout(r, 400));
  const sawEof = /listener reached end of file/i.test(serverLog.slice(before));
  checks.push({
    ok: sawEof,
    what: 'postmaster observed the listener EOF (still alive and looping at shutdown time)',
  });
  notes.push(
    'closing the listener does NOT stop the postmaster: accept_connection logs the EOF, ' +
      'backs off 100ms and returns Err, which ServerLoop discards',
  );
  notes.push(
    'no signal exists on wasm, so no fast shutdown and no SHUTDOWN CHECKPOINT: ' +
      'the workers are terminated instead',
  );
  return { notes, checks };
}

async function runPostmasterLane() {
  await Promise.race([
    poolReadyPromise,
    new Promise((_, rej) => setTimeout(() => rej(new Error('pool prewarm timeout')), 120000)),
  ]);
  plog(`pool of ${POOL_SIZE} prewarmed; waiting for the postmaster`);
  const ready = await waitForServerLog(/database system is ready to accept connections/, TIMEOUT_MS);
  plog(`postmaster ready: ${ready}`);
  if (!ready) {
    failures.push('postmaster never reached "ready to accept connections"');
    return;
  }
  const result = await runHostPipesScenario({
    openSession: openPipeSession,
    shutdown: postmasterShutdown,
    log: plog,
  });
  for (const c of result.checks) if (!c.ok) failures.push(c.what);
  note(
    `timing: lock wait ${result.timings.bWaitMs?.toFixed(0)}ms, ` +
      `lock_timeout ${result.timings.lockTimeoutMs?.toFixed(0)}ms, ` +
      `pids A=${result.timings.pidA} B=${result.timings.pidB}`,
  );
}

try {
  if (POSTMASTER) {
    await runPostmasterLane();
    throw new LaneDone();
  }
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
  if (!(e instanceof LaneDone)) failures.push(`driver exception: ${e && e.stack ? e.stack : e}`);
}

// ---- assertions -----------------------------------------------------------
const expect = (cond, msg) => { if (!cond) failures.push(msg); };

// The postmaster lane's assertions are the scenario's (already folded into
// `failures`) plus the thread bookkeeping below; the five-statement wire
// assertions that follow are the wire lanes' and do not apply.
note(`spawned thread ids: [${spawnedTids.join(', ')}] (pool ${POOL_SIZE}, ${spawnRefusals.length} EAGAIN refusals)`);
expect(spawnedTids.every((t) => t > 0), `thread ids must be positive: ${spawnedTids}`);
expect(spawnRefusals.length === 0, `${spawnRefusals.length} thread-spawn refusals (pool of ${POOL_SIZE} too small)`);
if (POSTMASTER) {
  expect(
    spawnedTids.length >= 4,
    `the postmaster spawned only ${spawnedTids.length} thread(s); expected the aux ` +
      'threads plus two backends',
  );
  try { worker.terminate(); } catch { /* already gone */ }
  if (failures.length) {
    for (const f of failures) note(`DRIVER-FAIL: ${f}`);
    note(`VERDICT: postmaster-node FAIL fs=${FS_MODE}`);
    process.exit(1);
  }
  note(`VERDICT: postmaster-node PASS fs=${FS_MODE}`);
  process.exit(0);
}

// The control arm (dispatch stdio-wire) runs the session on the wasm main
// thread and never spawns; it passes on the wire results alone.
if (dispatch === 'stdio-wire-threaded') {
  expect(spawnedTids.length >= 1, 'no thread was spawned via wasi.thread-spawn');
}

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

const fiveStatementMs = [
  'SELECT 1',
  'SELECT count(*) FROM pg_class',
  'CREATE TABLE spike_t(a int)',
  'INSERT INTO spike_t SELECT generate_series(1,1000)',
  'SELECT sum(a) FROM spike_t',
].reduce((sum, sql) => sum + (timings.get(sql) ?? 0), 0);
note(`timing: five-statement block total ${fiveStatementMs}ms (fs=${FS_MODE})`);

try { worker.terminate(); } catch { /* already gone */ }

// The coordinator is parked in Atomics.wait and cannot be reached by postMessage; the doorbell
// is the only way to ask its loop to return, which is exactly why it lives in shared memory.
if (storageWorker) {
  doorbell.requestStop();
  await Promise.race([storageStoppedPromise, new Promise((r) => setTimeout(r, 5000))]);
  try { storageWorker.terminate(); } catch { /* already gone */ }
}

if (failures.length) {
  for (const f of failures) note(`DRIVER-FAIL: ${f}`);
  note(`VERDICT: threads-node FAIL fs=${FS_MODE}`);
  process.exit(1);
}
note(`VERDICT: threads-node PASS fs=${FS_MODE}`);
process.exit(0);
