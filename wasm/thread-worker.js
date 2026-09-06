// thread-worker.js — the worker bootstrap for the wasm32-wasip1-threads build
// (spike/wasip1-threads, checkpoint (a)). ONE file, three roles, because the
// process instance and a spawned thread's instance differ only in which export
// they call:
//
//   role 'process'        — created by the driver (Node) or the page (browser).
//                           Instantiates FIRST, then prewarms the thread pool,
//                           then runs `_start()`. This is where `wasi`
//                           `thread-spawn` is answered.
//   role 'thread-prewarm' — created by the process worker BEFORE the guest is
//                           started. Instantiates over the same shared memory
//                           and answers `ready`; then parks on its message
//                           queue.
//   role 'thread-start'   — the actual spawn: call
//                           `wasi_thread_start(tid, start_arg)`.
//
// Why the process instance MUST be instantiated before any pool worker: with
// --shared-memory, wasm-ld's start function (__wasm_init_memory) runs in every
// instance, but only the FIRST instance to run it initializes the passive data
// segments — and, in that same instance only, the main thread's TLS block
// (its per-instance `__tls_base` global is set and the TLS template copied).
// Every later instance finds memory initialized and skips both. A spawned
// thread gets its TLS from `wasi_thread_start` (__wasm_init_tls), so pool
// instances never notice; the process instance runs `_start` on the wasm main
// thread with whatever `__tls_base` it was left with. When a pool worker
// instantiated first, that was garbage: every lazy `thread_local!` on the main
// thread reported "cannot access a Thread Local Storage value during or after
// destruction" (std's TLS state byte read as Destroyed) before the first line
// of output. Root-caused 2026-09-06; the order below is the fix.
//
// Why prewarm: by the time the guest calls thread-spawn it is microseconds
// away from parking in pthread_join (memory.atomic.wait), which blocks the
// entire JS thread the process instance runs on. A spawn handler that awaited
// anything — a dynamic import, a worker's module load — would deadlock. So all
// of that happens up front and thread-spawn is a bare postMessage.
//
// Node entry is thread-worker.mjs (a one-line re-export of this file) so
// Node's module resolution sees ESM without relying on syntax detection.

import { makeThreadsHost, makeSpawner, GuestExit, IS_NODE } from './threads-host.js';
import { SabPipe } from './sab-pipe.js';

const nodeWt = IS_NODE ? await import('node:worker_threads') : null;
const port = IS_NODE ? nodeWt.parentPort : self;

// A spawned thread reports on its own MessagePort straight to the driver
// (threads-host.js newMessageChannel explains why): the process worker is
// parked in a futex for the whole session and cannot relay anything.
let relay = null;

function post(msg, transfer) {
  const p = relay || port;
  if (transfer && transfer.length) p.postMessage(msg, transfer);
  else p.postMessage(msg);
}

function onMessage(cb) {
  if (IS_NODE) port.on('message', cb);
  else self.addEventListener('message', (e) => cb(e.data));
}

function log(text) {
  post({ type: 'log', text });
}

// ---------------------------------------------------------------------------

let prewarmed = null; // { instance, host, exports } for a spawned-thread worker

async function runProcess(msg) {
  const stdin = SabPipe.from(msg.stdin);
  const stdout = SabPipe.from(msg.stdout);

  const spawner = makeSpawner({
    wasmModule: msg.module,
    memory: msg.memory,
    image: msg.image,
    manifest: msg.manifest,
    stdinDesc: msg.stdin,
    stdoutDesc: msg.stdout,
    argv: msg.argv,
    env: msg.env,
    base: import.meta.url,
    relayPorts: msg.relayPorts || [],
    trace: msg.trace || 0,
    // Spawn bookkeeping the PROCESS instance itself observes (it is not yet
    // parked when it posts these); everything the spawned thread observes
    // goes out on that thread's own relay port instead.
    onEvent: (e) => post(e),
  });

  const host = makeThreadsHost({
    wasmModule: msg.module,
    memory: msg.memory,
    image: msg.image,
    manifest: msg.manifest,
    stdin,
    stdout,
    onStderr: (bytes) => post({ type: 'stderr', from: 'process', bytes }),
    argv: msg.argv,
    env: msg.env,
    spawn: (startArg) => spawner.spawn(startArg),
    label: 'process',
    trace: msg.trace || 0,
  });

  post({
    type: 'imports',
    memory: `${host.info.memory.module}.${host.info.memory.name}`,
    threadSpawn: host.info.threadSpawn
      ? `${host.info.threadSpawn.module}.${host.info.threadSpawn.name}`
      : null,
    modules: host.info.modules,
    count: host.info.all.length,
  });

  const instance = await WebAssembly.instantiate(msg.module, host.imports);
  post({ type: 'instantiated', exports: Object.keys(instance.exports).slice(0, 32) });

  // Pool AFTER the process instance (see the header: the first instantiation
  // owns memory + main-thread TLS init) and BEFORE `_start` (thread-spawn
  // cannot await).
  await spawner.prewarm(msg.poolSize || 1);
  post({ type: 'pool-ready', size: msg.poolSize || 1 });

  let code = 0;
  try {
    instance.exports._start();
  } catch (e) {
    if (e instanceof GuestExit) code = e.code;
    else {
      post({
        type: 'error',
        from: 'process',
        message: String(e && e.stack ? e.stack : e),
        trace: host.traceRing.slice(),
      });
      code = 70;
    }
  }
  // stdout EOF: the driver's async reader must not hang after the guest goes.
  stdout.close();
  post({ type: 'exit', from: 'process', code });
}

async function prewarmThread(msg) {
  if (msg.relay) {
    if (!IS_NODE) msg.relay.start();
    relay = msg.relay;
  }
  const stdin = SabPipe.from(msg.stdin);
  const stdout = SabPipe.from(msg.stdout);
  const host = makeThreadsHost({
    wasmModule: msg.module,
    memory: msg.memory,
    image: msg.image,
    manifest: msg.manifest,
    stdin,
    stdout,
    onStderr: (bytes) => post({ type: 'stderr', from: 'thread', bytes }),
    argv: msg.argv,
    env: msg.env,
    // Nested spawn (a thread spawning a thread) is out of scope for
    // checkpoint (a): answered as EAGAIN, loudly, rather than silently.
    spawn: null,
    label: 'thread',
    trace: msg.trace || 0,
  });
  const instance = await WebAssembly.instantiate(msg.module, host.imports);
  if (typeof instance.exports.wasi_thread_start !== 'function') {
    post({
      type: 'error',
      from: 'thread',
      message: 'module has no wasi_thread_start export — not a wasi-threads build',
    });
    port.postMessage({ type: 'ready' });
    return;
  }
  prewarmed = { instance, host, stdout };
  port.postMessage({ type: 'ready' }); // to the spawner, not the driver
}

function startThread(msg) {
  if (!prewarmed) {
    post({ type: 'error', from: 'thread', message: 'thread-start before prewarm' });
    return;
  }
  const { instance } = prewarmed;
  post({ type: 'thread-entered', tid: msg.tid, startArg: msg.startArg });
  try {
    instance.exports.wasi_thread_start(msg.tid, msg.startArg);
    post({
      type: 'thread-done',
      tid: msg.tid,
      trace: prewarmed.host.traceHead.slice(),
    });
  } catch (e) {
    if (e instanceof GuestExit) {
      // proc_exit from the spawned thread: the process is over, and nobody is
      // left to join us — announce it so the driver does not hang.
      prewarmed.stdout.close();
      post({ type: 'exit', from: 'thread', tid: msg.tid, code: e.code });
      return;
    }
    post({
      type: 'error',
      from: 'thread',
      tid: msg.tid,
      message: String(e && e.stack ? e.stack : e),
      trace: prewarmed.host.traceHead.slice(),
    });
  }
}

onMessage((msg) => {
  if (!msg || !msg.role) return;
  if (msg.role === 'process') {
    runProcess(msg).catch((e) =>
      post({ type: 'error', from: 'process', message: String(e && e.stack ? e.stack : e) }),
    );
  } else if (msg.role === 'thread-prewarm') {
    prewarmThread(msg).catch((e) =>
      post({ type: 'error', from: 'thread', message: String(e && e.stack ? e.stack : e) }),
    );
  } else if (msg.role === 'thread-start') {
    startThread(msg);
  } else {
    log(`unknown role ${msg.role}`);
  }
});
