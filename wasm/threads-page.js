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
// STORAGE (?fs). `?fs=copy` (the default for the wire lanes) is checkpoint (a)'s arrangement:
// every worker builds its OWN Vfs from its own copy of the packed image. `?fs=broker` replaces
// that with ONE store in a dedicated coordinator worker (wasm/storage-worker.js): the page mints
// a doorbell and pool+1 SharedArrayBuffer channels, starts the coordinator FIRST and waits for
// it to seed the store, then creates the process worker — which hands one channel to each
// prewarmed pool worker and keeps one for itself.
//
// THE POSTMASTER LANE (?dispatch=postmaster). The two dispatches above run ONE session on
// fds 0/1. `?dispatch=postmaster` runs the REAL postmaster instead
// (crates/backend/libpq/pqcomm_hostpipes), and is the browser twin of
// run-node-wire-threads.mjs's `--dispatch postmaster`, step for step: the page creates a
// listener pipe and one (in, out) SabPipe pair per session, registers all of them in the
// host's pipe-fd registry (wasm/threads-host.js), starts the storage coordinator FIRST and the
// process worker SECOND, waits for "ready to accept connections" on the guest's stderr, and
// then announces each connection with a 16-byte HPGP record on the listener plus a token on
// the wake fd. Two backends on two wasi threads run wasm/hostpipes-scenario.js — the SAME
// module the Node driver and the native arm run. fd numbering is threads-host.js's: wake 999,
// listener 1000, session k at 1001+2k / 1002+2k.
//
// The lane ENDS with a real shutdown: a third session asks for an explicit CHECKPOINT, then
// closing the listener pipe is EOF on the postmaster's listener, which pqcomm_hostpipes turns
// into a fast-shutdown request. The page waits for the guest to exit ON ITS OWN and asserts
// exit 0 plus the shutdown checkpoint lines — nothing here calls worker.terminate() to end the
// server.
//
// THE PAGE'S MAIN THREAD NEVER BLOCKS. Every wait on a SharedArrayBuffer here goes through
// SabPipe.readAsync (Atomics.waitAsync) or a setTimeout poll; Atomics.wait belongs to the
// workers, which are allowed to use it.
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
  PipeRegistry,
  HOSTPIPES_LISTEN_FD,
  HOSTPIPES_WAKE_FD,
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

const params = new URLSearchParams(location.search);
const wasmUrl = params.get('wasm') || 'assets/postgres-threads.wasm';
const vfsPrefix = params.get('vfs') || 'assets/vfs';
// ?dispatch=stdio-wire is the CONTROL arm: the same threads module, the same
// worker, the same SAB pipes — but the session runs on the process instance's
// own thread and `wasi` `thread-spawn` is never called. It isolates "does this
// build run in this engine at all" from "does the spawned thread work".
const dispatch = params.get('dispatch') || 'stdio-wire-threaded';
if (!['stdio-wire', 'stdio-wire-threaded', 'postmaster'].includes(dispatch)) {
  throw new Error(`unknown ?dispatch=${dispatch}`);
}
// The postmaster lane: a real PostmasterMain over host-pipes fds, N backends.
const POSTMASTER = dispatch === 'postmaster';
// ?pool=N sizes the prewarmed wasi-thread pool. More than one thread is asked
// for now: the session thread (threaded dispatch only) AND the
// pg-timeout-timer thread statement_timeout needs. thread-spawn cannot create
// a worker on demand (it may not await), so an undersized pool is a hard
// -EAGAIN; every refusal is logged and fails the run.
// The postmaster wants far more than the wire lanes: the startup process, the
// checkpointer, the background writer, the WAL writer, the memory watchdog
// sampler, the pg-timeout-timer, the autovacuum launcher when it is not off,
// two session backends — plus headroom. Slots are RECLAIMED when a guest
// thread returns (the startup process does), held for the life of the process
// when it does not.
const POOL_SIZE = Number(params.get('pool') || (POSTMASTER ? 12 : 4));
// ?trace=N keeps the last N WASI calls of each instance and dumps them with any
// guest abort — the only way to attribute a bare `RuntimeError: unreachable`.
const TRACE = Number(params.get('trace') || 0);
// ?fs=broker routes every guest FILE call to one store in a coordinator worker; ?fs=copy
// keeps checkpoint (a)'s per-worker private copy of the packed image. The postmaster lane
// defaults to broker: its shutdown is real, and the shutdown CHECKPOINT can only complete
// against the ONE store the backends wrote. The wire lanes default to copy.
const FS_MODE = params.get('fs') || (POSTMASTER ? 'broker' : 'copy');
if (FS_MODE !== 'copy' && FS_MODE !== 'broker') throw new Error(`unknown ?fs=${FS_MODE}`);
// max_parallel_workers, and therefore the size of the postmaster's WARM STANDBY POOL
// (launch_backend::wpool: `target()` IS max_parallel_workers). The guest's boot default is 16
// and autotune would put it at the core count; either is more standby threads than a FIXED
// host pool of ?pool=N slots can give without starving the aux processes. ?workers=N sizes it
// the way any host with a bounded thread supply must. Mirrors the driver's --workers.
const POOL_WORKERS = Number(params.get('workers') ?? 2);
// ?nowake=1 withholds PGRUST_HOSTPIPES_WAKE_FD from the guest, so the postmaster falls back to
// its timed accept probe. The A/B that measures what the host-driven wake is worth: same
// module, same browser, one query parameter. Mirrors the driver's --no-wake.
const NO_WAKE = params.get('nowake') === '1' || params.get('wake') === '0';
// The page's equivalent of PGRUST_THREADS_TIMEOUT_MS.
const TIMEOUT_MS = Number(params.get('timeout') || 180000);
// The page's equivalent of PGRUST_WIRE_GUCS: comma-separated extra -c GUCs. The default pins
// timezone=UTC,log_timezone=UTC for transcript identity with the native and Node arms.
const EXTRA_GUCS = params.get('gucs') || 'timezone=UTC,log_timezone=UTC';
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
// The two lanes have their own verdict noun so a grep for one can never match
// the other: `threads-browser` for the single-session wire lanes,
// `postmaster-browser` for the host-pipes postmaster lane.
const LANE = POSTMASTER ? 'postmaster-browser' : 'threads-browser';
function verdict(ok, reason) {
  if (ok) console.log(`VERDICT: ${LANE} PASS fs=${FS_MODE}`);
  else console.log(`VERDICT: ${LANE} FAIL fs=${FS_MODE} ${reason}`);
  document.title = ok ? `${LANE} PASS` : `${LANE} FAIL`;
  window.__pgrustThreadsVerdict = ok ? 'PASS' : `FAIL ${reason}`;
}

// The same GUC argv as the single-threaded wire lane (wiresession.js
// defaultWireArgv), with the dispatch mode in argv[1]. Spelled out here rather
// than imported so this page pulls in nothing from the JSPI session module.
function guestArgv() {
  const extra = EXTRA_GUCS.split(',')
    .filter(Boolean)
    .flatMap((g) => ['-c', g]);
  const argv = [
    'postgres',
    `--${dispatch}`,
    '-D', '/pgdata',
    '-c', 'max_stack_depth=60000',
    '-c', 'io_method=sync',
    '-c', 'autovacuum=off',
    '-c', 'wal_sync_method=fdatasync',
    '-c', 'shared_buffers=32MB',
    ...extra,
    'postgres',
  ];
  if (!POSTMASTER) return argv;
  // `--host-pipes` picks a TRANSPORT and then falls through to the normal
  // postmaster dispatch, so the argv is PostmasterMain's, not a single
  // backend's: no trailing dbname (main_entry's getopt would reject it as an
  // invalid argument), and the two GUCs that make the host fd the only way in.
  argv[1] = '--host-pipes';
  argv.pop();
  argv.push('-c', 'listen_addresses=', '-c', 'unix_socket_directories=');
  // So the checkpoint the shutdown path can (or cannot) run is visible in the log.
  argv.push('-c', 'log_checkpoints=on');
  // The warm standby pool is ON in this lane (that is the point of sizing it):
  // `wpool::maintain()` spawns at most `target - population` standbys per
  // postmaster lap, so a bounded target is a bounded thread claim.
  argv.push('-c', `max_parallel_workers=${POOL_WORKERS}`);
  return argv;
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

  // ---- ?dispatch=postmaster: the host side of the host-pipes fd contract ----
  // Every pipe is created HERE, before the guest starts, and travels to the process worker and
  // to every pool worker as SharedArrayBuffer descriptors — a backend thread must read the same
  // ring the page writes, and its `secure_close` must be visible to us (threads-host.js,
  // PipeRegistry). Two pairs for the scenario (A, B) plus one for the shutdown path's explicit
  // CHECKPOINT (C): every pipe has to exist before the guest starts, because the registry is
  // handed to the pool workers at prewarm.
  const SESSION_COUNT = 3;
  const CONN_MAGIC = 0x50475048; // "HPGP" in stream order
  const pipeRegistry = new PipeRegistry();
  let listenerPipe = null;
  let wakePipe = null;
  const sessionPipes = [];
  if (POSTMASTER) {
    listenerPipe = SabPipe.create(1 << 12); // 16-byte records; a page is plenty
    pipeRegistry.register(HOSTPIPES_LISTEN_FD, { in: listenerPipe });
    // The postmaster's wake channel: BOTH ends of one ring on one fd (see threads-host.js
    // HOSTPIPES_WAKE_FD). Sized far past anything that can queue — the postmaster drains it on
    // every wake, and a full ring would make a guest thread's SetLatch block, which SetLatch
    // may never do.
    wakePipe = SabPipe.create(1 << 16);
    pipeRegistry.register(HOSTPIPES_WAKE_FD, { in: wakePipe, out: wakePipe });
    for (let k = 0; k < SESSION_COUNT; k++) {
      const { inFd, outFd } = sessionFds(k);
      const toGuest = SabPipe.create(1 << 20); // page -> backend (guest READS)
      const fromGuest = SabPipe.create(1 << 22); // backend -> page (guest WRITES)
      pipeRegistry.register(inFd, { in: toGuest });
      pipeRegistry.register(outFd, { out: fromGuest });
      sessionPipes.push({ k, inFd, outFd, toGuest, fromGuest });
    }
    note(
      `page: host-pipes listener fd ${HOSTPIPES_LISTEN_FD}, wake fd ${HOSTPIPES_WAKE_FD}; sessions ` +
        sessionPipes.map((p) => `${p.k}=(in ${p.inFd}, out ${p.outFd})`).join(', '),
    );
  }
  const pipeDescriptors = pipeRegistry.descriptors();

  const spawnedTids = [];
  const spawnRefusals = [];
  let exitCode = null;
  let exitFrom = null;
  let poolFinal = null;
  // The postmaster lane waits on the server LOG (stderr) for "ready to accept
  // connections" exactly as the native and Node drivers do.
  let serverLog = '';
  let poolReady;
  const poolReadyPromise = new Promise((r) => { poolReady = r; });
  let exited;
  const exitedPromise = new Promise((r) => { exited = r; });
  // `bytes` arrives as a freshly allocated (NON-shared) Uint8Array from the
  // host's fd_write gather — TextDecoder refuses a SharedArrayBuffer-backed
  // view, so this must never be pointed at the guest's memory directly.
  const dec = new TextDecoder('utf-8', { fatal: false });

  function handleMessage(m) {
    switch (m.type) {
      case 'pool-final':
        poolFinal = m.slots;
        note(`page: pool slots at guest exit: ${m.slots.map((x) => `${x.slot}=${x.state}`).join(' ')}`);
        break;
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
        for (const line of m.trace || []) note(`page: trace ${line}`);
        break;
      case 'stderr': {
        const text = dec.decode(m.bytes);
        serverLog += text;
        for (const line of text.split('\n')) {
          if (line.trim()) note(`guest-stderr[${m.from}]: ${line}`);
        }
        break;
      }
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
      pipes: pipeDescriptors,
      argv: guestArgv(),
      env: {
        USER: 'postgres',
        PGRUST_TZDIR: '/share/timezone',
        PGRUST_PGSHAREDIR: '/share',
        PGRUST_RUNTIME: '0',
        RUST_BACKTRACE: '1',
        // The two channels that can name a host-owned fd. The listener is
        // required (pqcomm_hostpipes::LISTEN_FD_ENV; PostmasterMain FATALs
        // without it); the wake fd is OPTIONAL — drop it and the postmaster
        // falls back to its 50ms accept probe.
        ...(POSTMASTER
          ? {
              PGRUST_HOSTPIPES_LISTEN_FD: String(HOSTPIPES_LISTEN_FD),
              ...(NO_WAKE ? {} : { PGRUST_HOSTPIPES_WAKE_FD: String(HOSTPIPES_WAKE_FD) }),
            }
          : {}),
      },
      poolSize: POOL_SIZE,
      trace: TRACE,
      relayPorts: relayChannels.map((c) => c.port2),
    },
    [guestImage, ...relayChannels.map((c) => c.port2)],
  );

  // ---- the pgwire pump (this thread never blocks) --------------------------
  // Not started in the postmaster lane: there the wire lives on the session
  // pipes, and fd 1 carries nothing.
  const reader = new WireReader();
  const inbox = [];
  let pumpDone = false;
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

  // =========================================================================
  // ?dispatch=postmaster — one pgwire session per host-pipe pair.
  // Every helper below is the browser twin of the same-named one in
  // run-node-wire-threads.mjs; the differences are `note` vs `process.stdout`
  // and where the module URLs come from.
  // =========================================================================

  const acceptLatencies = [];
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
    wakePostmaster();
  }

  // The host half of the accept wake. Without it a connection record just sits
  // in the listener ring until the postmaster's next probe; with it the
  // postmaster's single `poll` on the wake fd returns at once (its waiter is in
  // fd-park mode on this very fd, so a guest SetLatch lands here too).
  function wakePostmaster() {
    if (!wakePipe) return;
    wakePipe.write(new Uint8Array([0]), { block: false });
  }

  // One session over one (in, out) SabPipe pair. Same shape as the native and
  // Node drivers' Session, over shared-memory rings: THIS thread never blocks
  // (readAsync -> Atomics.waitAsync), the backend thread on the other side does.
  class PipeSession {
    constructor(name, toGuest, fromGuest) {
      this.name = name;
      this.toGuest = toGuest;
      this.fromGuest = fromGuest;
      this.reader = new WireReader();
      this.collector = null;
      this.closed = false;
      this.onFirstByte = null;
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
      // The first byte the backend writes is the accept-to-first-response
      // latency: the postmaster had to WAKE, accept the record, spawn the
      // backend thread and let it read the startup packet. It is the number
      // the host-driven wake moves.
      if (this.onFirstByte) {
        const cb = this.onFirstByte;
        this.onFirstByte = null;
        cb();
      }
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

    startup(params_) {
      const p = this._collect();
      this._write(encodeStartup(params_));
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
      const until = Date.now() + timeoutMs;
      while (!this.closed && Date.now() < until) {
        await new Promise((r) => setTimeout(r, 10));
      }
      return this.closed;
    }
  }

  async function waitForServerLog(re, timeoutMs) {
    const until = Date.now() + timeoutMs;
    while (Date.now() < until) {
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
    const announcedAt = Date.now();
    announceConnection(slot.inFd, slot.outFd);
    const s = new PipeSession(name, slot.toGuest, slot.fromGuest);
    s.onFirstByte = () => acceptLatencies.push({ name, ms: Date.now() - announcedAt });
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

  // MEMORY. `performance.memory` is Chrome's per-page JS heap only (non-standard, and NOT the
  // wasm linear memory); `performance.measureUserAgentSpecificMemory()` is the standardised
  // cross-agent measure and needs cross-origin isolation, which this page has anyway. The one
  // exact number everywhere is the shared WebAssembly.Memory's own byteLength — the memory all
  // POOL_SIZE+1 instances share, and the one the guest's thread stacks are carved out of.
  let peakSharedBytes = 0;
  let peakJsHeap = 0;
  function sampleMemory() {
    peakSharedBytes = Math.max(peakSharedBytes, memory.buffer.byteLength);
    const pm = performance.memory;
    if (pm) peakJsHeap = Math.max(peakJsHeap, pm.usedJSHeapSize);
  }
  const MiB = (b) => (b / 1048576).toFixed(1);
  // measureUserAgentSpecificMemory() settles at the NEXT major GC, which in a
  // headless run that allocates nothing after the lane may be a long way off —
  // so it is started at the top of the lane and merely collected at the end,
  // never awaited on the critical path to the verdict.
  let uaMemoryPromise = null;
  function startMemoryMeasurement() {
    if (typeof performance.measureUserAgentSpecificMemory !== 'function') return;
    try {
      uaMemoryPromise = performance.measureUserAgentSpecificMemory();
    } catch (e) {
      note(`page: measureUserAgentSpecificMemory rejected at start: ${e && e.message ? e.message : e}`);
    }
  }
  async function reportMemory() {
    sampleMemory();
    note(
      `page: shared WebAssembly.Memory final ${memory.buffer.byteLength} bytes ` +
        `(${MiB(memory.buffer.byteLength)}MiB, peak ${MiB(peakSharedBytes)}MiB), ` +
        `SharedArrayBuffer=${memory.buffer instanceof SharedArrayBuffer}`,
    );
    const pm = performance.memory;
    note(
      pm
        ? `page: performance.memory usedJSHeap ${MiB(pm.usedJSHeapSize)}MiB ` +
          `(sampled peak ${MiB(peakJsHeap)}MiB), totalJSHeap ${MiB(pm.totalJSHeapSize)}MiB, ` +
          `limit ${MiB(pm.jsHeapSizeLimit)}MiB`
        : 'page: performance.memory unavailable — page JS heap NOT measured',
    );
    if (!uaMemoryPromise) {
      note('page: performance.measureUserAgentSpecificMemory unavailable — cross-agent memory NOT measured');
      return;
    }
    try {
      const r = await Promise.race([
        uaMemoryPromise,
        new Promise((res) => setTimeout(() => res(null), 8000)),
      ]);
      if (!r) {
        note('page: measureUserAgentSpecificMemory had not settled 8s after the lane (it waits for the next major GC) — not measured');
        return;
      }
      note(`page: measureUserAgentSpecificMemory total ${r.bytes} bytes (${MiB(r.bytes)}MiB)`);
      for (const b of r.breakdown || []) {
        if (!b.bytes) continue;
        const where = (b.attribution || []).map((a) => a.url || a.scope || '?').join(' ');
        note(`page:   ${MiB(b.bytes)}MiB ${(b.types || []).join('+') || 'unknown'} ${where}`);
      }
    } catch (e) {
      note(`page: measureUserAgentSpecificMemory failed: ${e && e.message ? e.message : e}`);
    }
  }

  // SHUTDOWN — a REAL one, and the same one the native and Node drivers perform.
  //
  // There are no signals on wasm, and that no longer matters: the STOP SIGNAL of this transport
  // is the listener reaching EOF. `pqcomm_hostpipes::accept_connection` turns that EOF into
  // `postmaster_seams::signal_postmaster_fast_shutdown` — the very handler a SIGINT runs — so
  // the postmaster raises PENDING_PM_FAST_SHUTDOWN_REQUEST, sets its own latch, and walks the
  // ordinary PM_STOP_BACKENDS -> PM_WAIT_BACKENDS -> shutdown checkpoint -> exit(0) ceremony.
  //
  // Order here, least destructive first:
  //   1. ask a third session for an explicit CHECKPOINT. Under ?fs=broker this completes; under
  //      ?fs=copy it is EXPECTED to fail (the checkpointer thread has its own private Vfs and
  //      cannot see the relation files the backends created in theirs). Recorded as a note with
  //      the server-log reason, never as a pass/fail — it is a storage-lane property, not a
  //      postmaster one;
  //   2. close the listener pipe and poke the wake fd, then WAIT FOR THE GUEST TO EXIT ON ITS
  //      OWN. Assert exit code 0, the fast-shutdown request, the shutdown checkpoint and
  //      "database system is shut down" in the log, and that every pool slot came back idle.
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
            'expected under ?fs=copy, where the checkpointer has its own private Vfs',
      );
      C.terminate();
      await C.waitClosed(5000);
    } catch (e) {
      notes.push(`explicit CHECKPOINT could not be attempted: ${e && e.message ? e.message : e}`);
    }

    const shutStart = Date.now();
    plog('closing the host-pipes listener: that EOF IS the fast-shutdown request');
    listenerPipe.close();
    wakePostmaster();
    const gone = await Promise.race([
      exitedPromise.then(() => true),
      new Promise((r) => setTimeout(() => r(false), 60000)),
    ]);
    const shutMs = Date.now() - shutStart;
    plog(`  postmaster stopped in ${shutMs}ms (exit ${exitCode} from ${exitFrom})`);
    // The last stderr chunks can still be in flight on the worker's message
    // queue when `exit` lands; give them one turn.
    await new Promise((r) => setTimeout(r, 300));

    // Pool slots at exit. `idle`/`empty` = the guest thread returned (or the slot was never
    // used); `running` = a guest thread that never came back. Some of those are expected, and
    // none of them is a postmaster CHILD: four process-lifetime threads (pg-timeout-timer,
    // pg:memwatchdog, pg-bgjobs-dispatcher, pg-slease-sweeper), none of which holds a pmchild
    // slot, plus up to `max_parallel_workers` PARKED WARM STANDBYS, which a clean shutdown does
    // not retire. Anything BEYOND that bound is a real child that failed to stop.
    const LIFETIME_THREADS = 4 + POOL_WORKERS;
    const running = (poolFinal || []).filter((x) => x.state === 'running');
    const midSpawn = (poolFinal || []).filter((x) => x.state === 'claimed' || x.state === 'start');
    notes.push(`listener close -> guest exit in ${shutMs}ms`);
    notes.push(
      poolFinal
        ? `pool slots at exit: ${poolFinal.map((x) => `${x.slot}=${x.state}`).join(' ')} ` +
          `(${running.length} still running; up to ${LIFETIME_THREADS} are expected and ` +
          'are NOT postmaster children: 4 process-lifetime threads — pg-timeout-timer, ' +
          `pg:memwatchdog, pg-bgjobs-dispatcher, pg-slease-sweeper — plus up to ${POOL_WORKERS} ` +
          'parked warm standbys)'
        : 'the process worker never reported its pool slots (guest did not return from _start)',
    );
    checks.push(
      { ok: gone, what: 'postmaster exited after the listener closed (no terminate(), no signal)' },
      { ok: exitCode === 0, what: `exit code 0 (got ${exitCode})` },
      {
        ok: /received fast shutdown request/i.test(serverLog),
        what: 'log shows "received fast shutdown request" (the SIGINT flags, raised by the EOF)',
      },
      {
        ok: /host-pipes listener closed: fast shutdown requested/i.test(serverLog),
        what: 'log shows the transport attributing the shutdown to the listener close',
      },
      {
        ok: /checkpoint starting: shutdown/i.test(serverLog),
        what: 'log shows the SHUTDOWN CHECKPOINT',
      },
      {
        ok: /database system is shut down/i.test(serverLog),
        what: 'log shows "database system is shut down"',
      },
      {
        ok: !!poolFinal && midSpawn.length === 0,
        what: `no pool slot is stuck mid-spawn at exit (${midSpawn.length} claimed/start)`,
      },
      {
        ok: !!poolFinal && running.length <= LIFETIME_THREADS,
        what:
          `every postmaster CHILD thread returned: ${running.length} slot(s) still running, ` +
          `at most ${LIFETIME_THREADS} non-child threads allowed ` +
          `(4 process-lifetime + ${POOL_WORKERS} parked standbys)`,
      },
    );
    return { notes, checks };
  }

  async function runPostmasterLane() {
    await Promise.race([
      poolReadyPromise,
      new Promise((_, rej) => setTimeout(() => rej(new Error('pool prewarm timeout')), 120000)),
    ]);
    plog(`pool of ${POOL_SIZE} prewarmed; waiting for the postmaster`);
    startMemoryMeasurement();
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

  if (POSTMASTER) {
    const sampler = setInterval(sampleMemory, 250);
    try {
      await runPostmasterLane();
    } finally {
      clearInterval(sampler);
    }

    // The postmaster lane's assertions are the scenario's (already folded into
    // `failures`) plus the thread bookkeeping below.
    const expectPm = (cond, msg) => { if (!cond) failures.push(msg); };
    note(`page: spawned thread ids: [${spawnedTids.join(', ')}] (pool ${POOL_SIZE}, ${spawnRefusals.length} EAGAIN refusals)`);
    expectPm(spawnedTids.every((t) => t > 0), `thread ids must be positive: ${spawnedTids}`);
    expectPm(spawnRefusals.length === 0, `${spawnRefusals.length} thread-spawn refusals (pool of ${POOL_SIZE} too small)`);
    expectPm(
      spawnedTids.length >= 4,
      `the postmaster spawned only ${spawnedTids.length} thread(s); expected the aux ` +
        'threads plus two backends',
    );
    note(
      `accept latency (announce -> backend's first byte, wake fd ${NO_WAKE ? 'OFF' : 'ON'}): ` +
        acceptLatencies.map((a) => `${a.name}=${a.ms}ms`).join(' '),
    );
    const freeSlots = (poolFinal || []).filter((x) => x.state === 'empty' || x.state === 'idle').length;
    note(
      `pool: ${POOL_SIZE} slots, ${spawnedTids.length} thread(s) started ` +
        `(max_parallel_workers=${POOL_WORKERS} warm standbys), ${freeSlots} slot(s) free at exit`,
    );
    await reportMemory();
    // BROWSER-ONLY ORDERING, and the one place this lane deliberately differs
    // from the Node driver. The driver calls process.exit() the instant it has
    // its verdict, so nothing of the guest outlives the store. A PAGE does not
    // exit: the process worker and its POOL_SIZE pool workers are still there,
    // and some of those are parked inside process-lifetime guest threads
    // (pg-timeout-timer, pg:memwatchdog, pg-bgjobs-dispatcher,
    // pg-slease-sweeper) that wake on a ~1s timer and touch the filesystem. So
    // the process worker is terminated FIRST — which takes its pool workers
    // with it, they are dedicated workers it owns — and the coordinator is
    // stopped after. Chrome does not reap a worker parked inside a guest thread
    // instantly (measured: up to ~2s), so a couple of `path_open failed -> EIO`
    // lines from an ALREADY-EXITED guest can still trail the verdict. They are
    // an artefact of a page outliving its run, not a lane failure.
    try { worker.terminate(); } catch { /* already gone */ }
    // The coordinator holds the ONE store the checkpointer just wrote through;
    // it must be stopped by its doorbell (it is parked in Atomics.wait and
    // cannot be reached by postMessage), and only AFTER the guest is gone.
    if (storageWorker) {
      doorbell.requestStop();
      await Promise.race([storageStoppedPromise, new Promise((r) => setTimeout(r, 5000))]);
      try { storageWorker.terminate(); } catch { /* already gone */ }
    }
    return;
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
