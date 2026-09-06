// threads-page.js — the browser twin of run-node-wire-threads.mjs.
//
// The page creates the PROCESS worker; the process worker's `wasi`
// `thread-spawn` creates the SESSION worker; stdin/stdout ride the two
// SharedArrayBuffer ring pipes, stderr comes back over postMessage. No JSPI
// anywhere: the guest's blocking read(0) blocks a Worker in Atomics.wait,
// which is exactly what a Worker is allowed to do.
//
// Everything the run needs is cross-origin isolation (COOP+COEP — that is what
// serve-coi.mjs is for), because SharedArrayBuffer and a shared
// WebAssembly.Memory are gated on it. The page logs `self.crossOriginIsolated`
// first so a run that silently lost isolation cannot masquerade as a pass.
//
// STORAGE (?fs). `?fs=copy` (the default) is checkpoint (a)'s arrangement: every worker
// builds its OWN Vfs from its own copy of the packed image. `?fs=broker` replaces that with
// ONE store in a dedicated coordinator worker (wasm/storage-worker.js): the page mints a
// doorbell and pool+1 SharedArrayBuffer channels, starts the coordinator FIRST and waits for
// it to seed the store, then creates the process worker — which hands one channel to each
// prewarmed pool worker and keeps one for itself.
//
// Assets (all overridable by query string):
//   ?wasm=assets/postgres-threads.wasm   the wasm32-wasip1-threads build
//   ?vfs=assets/vfs                      prefix for vfs.img + vfs.json
//   ?bundle=vendor/pglite-opfs-repacked.js  the @pgxsinkit/pglite-opfs-repacked ESM bundle
// Neither the threads build nor the library bundle is a packed asset, so link both in first:
//   ln -s ../../target/wasm32-wasip1-threads/wasm-release/postgres.wasm \
//         wasm/assets/postgres-threads.wasm
//   mkdir -p wasm/vendor && ln -sfn \
//     /path/to/pgxsinkit/packages/pglite-opfs-repacked/dist/browser-bundle.js \
//     wasm/vendor/pglite-opfs-repacked.js
//   # the bundle is emitted by `bun run build:public-packages` in the pgxsinkit repo

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
} from './threads-host.js';
import { loadRepackedBundle, repackedBundleUrl } from './broker-fs.js';
import {
  WireReader,
  encodeStartup,
  encodeQuery,
  TERMINATE,
  canonMessage,
  parseMessage,
} from './wire.js';

const params = new URLSearchParams(location.search);
const wasmUrl = params.get('wasm') || 'assets/postgres-threads.wasm';
const vfsPrefix = params.get('vfs') || 'assets/vfs';
// ?dispatch=stdio-wire is the CONTROL arm: the same threads module, the same
// worker, the same SAB pipes — but the session runs on the process instance's
// own thread and `wasi` `thread-spawn` is never called. It isolates "does this
// build run in this engine at all" from "does the spawned thread work".
const dispatch = params.get('dispatch') || 'stdio-wire-threaded';
// ?pool=N sizes the prewarmed wasi-thread pool. More than one thread is asked
// for now: the session thread (threaded dispatch only) AND the
// pg-timeout-timer thread statement_timeout needs. thread-spawn cannot create
// a worker on demand (it may not await), so an undersized pool is a hard
// -EAGAIN; every refusal is logged and fails the run.
const POOL_SIZE = Number(params.get('pool') || 4);
// ?trace=N keeps the last N WASI calls of each instance and dumps them with any
// guest abort — the only way to attribute a bare `RuntimeError: unreachable`.
const TRACE = Number(params.get('trace') || 0);
// ?fs=broker routes every guest FILE call to one store in a coordinator worker; ?fs=copy
// (default) keeps checkpoint (a)'s per-worker private copy of the packed image.
const FS_MODE = params.get('fs') || 'copy';
const BUNDLE_URL = params.get('bundle')
  ? new URL(params.get('bundle'), location.href).href
  : repackedBundleUrl(import.meta.url);

const out = document.getElementById('log');
const failures = [];
function note(line) {
  console.log(line);
  if (out) {
    out.textContent += line + '\n';
    out.scrollTop = out.scrollHeight;
  }
}
function verdict(ok, reason) {
  if (ok) console.log(`VERDICT: threads-browser PASS fs=${FS_MODE}`);
  else console.log(`VERDICT: threads-browser FAIL fs=${FS_MODE} ${reason}`);
  document.title = ok ? 'threads-browser PASS' : 'threads-browser FAIL';
  window.__pgrustThreadsVerdict = ok ? 'PASS' : `FAIL ${reason}`;
}

// The same GUC argv as the single-threaded wire lane (wiresession.js
// defaultWireArgv), with the threaded dispatch mode in argv[1]. Spelled out
// here rather than imported so this page pulls in nothing from the JSPI
// session module.
function threadedWireArgv() {
  return [
    'postgres',
    `--${dispatch}`,
    '-D', '/pgdata',
    '-c', 'max_stack_depth=60000',
    '-c', 'io_method=sync',
    '-c', 'autovacuum=off',
    '-c', 'wal_sync_method=fdatasync',
    '-c', 'shared_buffers=32MB',
    '-c', 'timezone=UTC',
    '-c', 'log_timezone=UTC',
    'postgres',
  ];
}

const STATEMENTS = [
  'SELECT 1',
  'SELECT count(*) FROM pg_class',
  'CREATE TABLE spike_t(a int)',
  'INSERT INTO spike_t SELECT generate_series(1,1000)',
  'SELECT sum(a) FROM spike_t',
];

async function main() {
  note(`page: crossOriginIsolated=${self.crossOriginIsolated}`);
  if (!self.crossOriginIsolated) {
    failures.push('page is not cross-origin isolated (serve with serve-coi.mjs)');
    return;
  }
  note(`page: SharedArrayBuffer=${typeof SharedArrayBuffer === 'function'}`);

  note(`page: fetching ${wasmUrl}`);
  const [wasmBytes, imageBuf, manifest] = await Promise.all([
    fetch(wasmUrl).then((r) => {
      if (!r.ok) throw new Error(`${wasmUrl}: HTTP ${r.status}`);
      return r.arrayBuffer();
    }),
    fetch(vfsPrefix + '.img').then((r) => {
      if (!r.ok) throw new Error(`${vfsPrefix}.img: HTTP ${r.status}`);
      return r.arrayBuffer();
    }),
    fetch(vfsPrefix + '.json').then((r) => {
      if (!r.ok) throw new Error(`${vfsPrefix}.json: HTTP ${r.status}`);
      return r.json();
    }),
  ]);
  note(`page: wasm ${wasmBytes.byteLength} bytes, vfs image ${imageBuf.byteLength} bytes`);

  const wasmModule = await WebAssembly.compile(wasmBytes);
  const info = inspectImports(wasmModule);
  note(
    `page: imports=${info.all.length} memory=${info.memory.module}.${info.memory.name} ` +
      `thread-spawn=${info.threadSpawn ? info.threadSpawn.module + '.' + info.threadSpawn.name : 'MISSING'}`,
  );
  if (!info.threadSpawn) failures.push('module does not import wasi.thread-spawn');

  const memory = createSharedMemory();
  note(
    `page: shared memory ${memory.buffer.byteLength / 1048576}MiB, ` +
      `SharedArrayBuffer=${memory.buffer instanceof SharedArrayBuffer}`,
  );
  note(`page: storage --fs ${FS_MODE}`);

  // ---- the storage coordinator, started BEFORE anything else ---------------
  // Its store must be seeded and every channel attached before the first backend can ask for
  // a file; once its blocking serveForever() loop is entered it never reaches its event loop
  // again, so there is no attaching anything afterwards.
  let storageWorker = null;
  let doorbell = null;
  let channels = [];
  let storageStoppedResolve;
  const storageStoppedPromise = new Promise((r) => { storageStoppedResolve = r; });

  if (FS_MODE === 'broker') {
    const bundle = await loadRepackedBundle(BUNDLE_URL);
    note(`page: storage bundle ${BUNDLE_URL}`);
    doorbell = bundle.RepackedDoorbell.create();
    // One channel per pool slot PLUS one for the process instance: the protocol is one request
    // in flight per channel, so two agents may never share one.
    channels = Array.from({ length: POOL_SIZE + 1 }, (_unused, i) =>
      bundle.RepackedChannel.create({ id: i + 1, doorbell }),
    );
    storageWorker = makeWorker(storageWorkerUrl(import.meta.url), { name: 'pgrust-storage' });
    let storageReady;
    const storageReadyPromise = new Promise((r) => { storageReady = r; });
    onWorkerMessage(storageWorker, (m) => {
      switch (m.type) {
        case 'storage-ready':
          note(
            `page: storage coordinator ready — seeded ${m.files} files / ${m.dirs} dirs ` +
              `(${m.bytes} bytes) in ${m.seedMs}ms; /pgdata holds ${m.datadirFiles} files ` +
              `(${m.datadirBytes} bytes); arena ${(m.arenaBytes / 1048576).toFixed(1)}MiB ` +
              `at ${m.extentSize}B extents; channels [${m.channels.join(', ')}]`,
          );
          storageReady();
          break;
        case 'storage-log':
          note(`page: storage ${m.text}`);
          break;
        case 'storage-stopped':
          note(
            `page: storage coordinator stopped — /pgdata now holds ${m.datadirFiles} files ` +
              `(${m.datadirBytes} bytes) against ${m.seededFiles} files (${m.seededBytes} bytes) at ` +
              `seed: the session's writes landed in the ONE store (delta ` +
              `${m.datadirFiles - m.seededFiles} files, ${m.datadirBytes - m.seededBytes} bytes)`,
          );
          storageStoppedResolve();
          break;
        case 'storage-error':
          failures.push(`storage worker error: ${m.message}`);
          note(`page: storage ERROR ${m.message}`);
          storageReady();
          storageStoppedResolve();
          break;
        default:
          note(`page: storage unhandled message ${JSON.stringify(m.type)}`);
      }
    });
    onWorkerError(storageWorker, (e) => {
      failures.push(`storage worker threw: ${e && e.message ? e.message : e}`);
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
      new Promise((_, rej) => setTimeout(() => rej(new Error('storage coordinator seed timeout')), 180000)),
    ]);
  }

  // With ?fs=broker the packed image now lives in the coordinator's store (and its ArrayBuffer
  // was transferred there); every instance gets an EMPTY base Vfs the WASI adapter sits on top
  // of and never consults.
  const guestImage = FS_MODE === 'broker' ? new ArrayBuffer(0) : imageBuf;
  const guestManifest = FS_MODE === 'broker' ? { dirs: ['/'], files: [] } : manifest;

  const stdinPipe = SabPipe.create(1 << 20);
  const stdoutPipe = SabPipe.create(1 << 22);

  const spawnedTids = [];
  const spawnRefusals = [];
  let exitCode = null;
  let exitFrom = null;
  let poolReady;
  const poolReadyPromise = new Promise((r) => { poolReady = r; });
  let exited;
  const exitedPromise = new Promise((r) => { exited = r; });
  const dec = new TextDecoder('utf-8', { fatal: false });

  function handleMessage(m) {
    switch (m.type) {
      case 'pool-ready':
        note(`page: thread pool ready (${m.size})`);
        poolReady();
        break;
      case 'imports':
        note(`page: instance imports memory=${m.memory} thread-spawn=${m.threadSpawn}`);
        break;
      case 'instantiated':
        note(`page: process instance created; exports ${m.exports.join(',')}`);
        break;
      case 'spawn':
        spawnedTids.push(m.tid);
        note(`page: wasi thread-spawn(start_arg=${m.startArg}) by ${m.from} -> tid ${m.tid} (slot ${m.slot})`);
        break;
      case 'spawn-refused':
        spawnRefusals.push(m);
        note(`page: wasi thread-spawn(start_arg=${m.startArg}) by ${m.from} -> -EAGAIN (pool of ${m.poolSize} exhausted)`);
        break;
      case 'thread-entered':
        note(`page: wasi_thread_start(tid=${m.tid}, start_arg=${m.startArg}) entered`);
        break;
      case 'thread-done':
        note(`page: thread ${m.tid} returned from wasi_thread_start`);
        break;
      case 'stderr':
        for (const line of dec.decode(m.bytes).split('\n')) {
          if (line.trim()) note(`guest-stderr[${m.from}]: ${line}`);
        }
        break;
      case 'exit':
        note(`page: guest exit ${m.code} (from ${m.from})`);
        exitCode = m.code;
        exitFrom = m.from;
        exited();
        break;
      case 'error':
        failures.push(`worker error (${m.from || '?'}): ${m.message}`);
        note(`page: ERROR ${m.message}`);
        for (const line of m.trace || []) note(`page: trace ${line}`);
        exited();
        break;
      case 'log':
        note(`page: ${m.text}`);
        break;
      default:
        note(`page: unhandled message ${JSON.stringify(m.type)}`);
    }
  }

  const worker = makeWorker(threadWorkerUrl(import.meta.url), { name: 'pgrust-process' });
  onWorkerMessage(worker, handleMessage);
  onWorkerError(worker, (e) => {
    failures.push(`process worker threw: ${e && e.message ? e.message : e}`);
    exited();
  });

  // A spawned thread reports here DIRECTLY: from the moment the guest joins,
  // the process worker is parked in a futex and relays nothing.
  const relayChannels = Array.from({ length: POOL_SIZE }, () => newMessageChannel());
  for (const c of relayChannels) onPortMessage(c.port1, handleMessage);

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
      argv: threadedWireArgv(),
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
    [guestImage, ...relayChannels.map((c) => c.port2)],
  );

  // ---- the pgwire pump (this thread never blocks) --------------------------
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

  const deadline = Date.now() + 180000;
  async function untilReadyForQuery(what) {
    const got = [];
    for (;;) {
      while (inbox.length) {
        const m = inbox.shift();
        got.push(m);
        if (m.t === 'Z') return got;
      }
      if (pumpDone) throw new Error(`stdout closed while waiting for ReadyForQuery after ${what}`);
      if (Date.now() > deadline) throw new Error(`timeout waiting for ReadyForQuery after ${what}`);
      await new Promise((r) => setTimeout(r, 2));
    }
  }

  await Promise.race([
    poolReadyPromise,
    new Promise((_, rej) => setTimeout(() => rej(new Error('pool prewarm timeout')), 120000)),
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

  const rows = new Map();
  const timings = new Map();
  const sqlstates = new Map();

  // One simple-query round trip, timed from the Q write to ReadyForQuery —
  // the guest's wall time as seen from outside, with no clock of the guest's
  // involved. Same shape and same assertions as run-node-wire-threads.mjs.
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
  }

  for (const sql of STATEMENTS) {
    await runQuery(sql);
  }

  // ---- timed waits and timeouts -------------------------------------------
  // Both exercise the pg-timeout-timer thread (a real wasi thread now) and
  // the timed latch park pg_sleep loops on: the first proves the park sleeps
  // instead of spinning, the second that ANOTHER thread can interrupt it.
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

  const expect = (cond, msg) => { if (!cond) failures.push(msg); };
  note(`page: spawned thread ids: [${spawnedTids.join(', ')}] (pool ${POOL_SIZE}, ${spawnRefusals.length} EAGAIN refusals)`);
  expect(spawnRefusals.length === 0, `${spawnRefusals.length} thread-spawn refusals (pool of ${POOL_SIZE} too small)`);
  // The control arm (dispatch stdio-wire) runs the session on the wasm main
  // thread and never spawns; it passes on the wire results alone.
  if (dispatch === 'stdio-wire-threaded') {
    expect(spawnedTids.length >= 1, 'no thread was spawned via wasi.thread-spawn');
  }
  expect(spawnedTids.every((t) => t > 0), `thread ids must be positive: ${spawnedTids}`);
  const r1 = rows.get('SELECT 1') || [];
  expect(r1.length === 1 && r1[0] === '1', `SELECT 1 returned ${JSON.stringify(r1)}`);
  const rcnt = rows.get('SELECT count(*) FROM pg_class') || [];
  expect(rcnt.length === 1 && Number(rcnt[0]) > 0, `pg_class count returned ${JSON.stringify(rcnt)}`);
  const rsum = rows.get('SELECT sum(a) FROM spike_t') || [];
  expect(rsum.length === 1 && rsum[0] === '500500', `sum(a) returned ${JSON.stringify(rsum)}`);

  // pg_sleep(0.2) must actually sleep: a poll_oneoff answering "already
  // fired", or a latch park returning at once, would come back in ~0ms.
  const sleepMs = timings.get('SELECT pg_sleep(0.2)');
  note(`page: timing pg_sleep(0.2) round trip ${sleepMs}ms`);
  expect(sleepMs !== undefined && sleepMs >= 190, `pg_sleep(0.2) took ${sleepMs}ms (want >= 190)`);

  // statement_timeout must cancel a running pg_sleep(5) from the timer thread.
  const cancelMs = timings.get('SELECT pg_sleep(5)');
  const cancelStates = sqlstates.get('SELECT pg_sleep(5)') || [];
  note(`page: timing pg_sleep(5) under statement_timeout=300ms cancelled after ${cancelMs}ms, sqlstate ${cancelStates.join(',') || 'NONE'}`);
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

  const fiveStatementMs = STATEMENTS.reduce((sum, sql) => sum + (timings.get(sql) ?? 0), 0);
  note(`page: timing five-statement block total ${fiveStatementMs}ms (fs=${FS_MODE})`);

  try { worker.terminate(); } catch { /* already gone */ }

  // The coordinator is parked in Atomics.wait and cannot be reached by postMessage; the
  // doorbell is the only way to ask its loop to return, which is exactly why it lives in
  // shared memory.
  if (storageWorker) {
    doorbell.requestStop();
    await Promise.race([storageStoppedPromise, new Promise((r) => setTimeout(r, 5000))]);
    try { storageWorker.terminate(); } catch { /* already gone */ }
  }
}

try {
  await main();
} catch (e) {
  failures.push(`page exception: ${e && e.stack ? e.stack : e}`);
}
for (const f of failures) note(`DRIVER-FAIL: ${f}`);
verdict(failures.length === 0, failures.join(' | '));
