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
//                           started. Instantiates over the same shared memory,
//                           answers `ready`, and then parks in `Atomics.wait`
//                           on its SpawnDesk slot — NOT on its message queue:
//                           the spawn that starts it may come from a thread
//                           that cannot await (see threads-host.js SpawnDesk),
//                           and every start command therefore arrives through
//                           shared memory. Each `wasi_thread_start(tid,
//                           start_arg)` runs to completion and the slot goes
//                           back to idle.
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
// STORAGE. With `--fs copy` (the default) each role still builds its own Vfs
// from its own copy of the packed image. With `--fs broker` the driver hands
// this worker ONE SharedArrayBuffer channel to the storage coordinator, and
// `brokerFsFor()` below turns it into a WASI filesystem adapter composed over
// the host's WASI object — so every file call lands on the ONE store while fd
// 0/1/2 and every non-filesystem import are untouched. Each role releases its
// store descriptors when its guest thread returns (`releaseFs`).
//
// Node entry is thread-worker.mjs (a one-line re-export of this file) so
// Node's module resolution sees ESM without relying on syntax detection.

import { makeThreadsHost, makeSpawner, SpawnDesk, GuestExit, IS_NODE, EAGAIN } from './threads-host.js';
import { SabPipe } from './sab-pipe.js';
import { createBrokerFs, loadRepackedBundle } from './broker-fs.js';

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

// `--fs broker`: build THIS instance's filesystem seam onto the coordinator's one store. Awaited
// here, in the worker's async bootstrap, precisely because it may not be awaited later — a
// thread-spawn handler is microseconds from a futex park and can await nothing.
async function brokerFsFor(msg, label) {
  if (msg.fs !== 'broker') return null;
  if (!msg.channel) throw new Error(`${label}: --fs broker but no broker channel was handed to this worker`);
  const bundle = await loadRepackedBundle(msg.bundleUrl);
  return createBrokerFs({
    bundle,
    channel: msg.channel,
    memory: msg.memory,
    label,
    onLog: (text) => post({ type: 'log', text }),
  });
}

async function runProcess(msg) {
  const stdin = SabPipe.from(msg.stdin);
  const stdout = SabPipe.from(msg.stdout);
  const fs = await brokerFsFor(msg, 'process');

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
    poolSize: msg.poolSize || 1,
    trace: msg.trace || 0,
    fsMode: msg.fs || 'copy',
    bundleUrl: msg.bundleUrl || null,
    poolChannels: msg.poolChannels || [],
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
    fs,
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
  releaseFs(host, 'process');
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
  const desk = SpawnDesk.from(msg.desk);
  const slot = msg.slot | 0;
  const fs = await brokerFsFor(msg, `thread-slot-${slot}`);
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
    // Nested spawn: a thread spawning a thread is the NORMAL case now — the
    // pg-timeout-timer thread is created by whichever backend arms the first
    // timeout, which under --stdio-wire-threaded is the session thread. The
    // desk makes that a bare shared-memory publish, so this handler still
    // awaits nothing (threads-host.js SpawnDesk).
    spawn: (startArg) => {
      const { tid, slot: target } = desk.claim(startArg);
      if (tid < 0) {
        post({
          type: 'spawn-refused',
          from: 'thread',
          startArg,
          poolSize: desk.poolSize,
          errno: EAGAIN,
        });
        return -EAGAIN;
      }
      post({ type: 'spawn', from: 'thread', tid, slot: target, startArg });
      return tid;
    },
    label: 'thread',
    trace: msg.trace || 0,
    fs,
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
  prewarmed = { instance, host, stdout, desk, slot };
  port.postMessage({ type: 'ready' }); // to the spawner, not the driver
  // From here this worker's event loop is BLOCKED for good: every start
  // command arrives through shared memory instead (see the header).
  threadCommandLoop();
}

// `--fs broker`: a guest thread that has returned holds nothing, but the COORDINATOR still holds
// every store descriptor it opened until someone says otherwise. This is that someone; without it
// the coordinator leaks a thread's fds until the whole channel detaches.
function releaseFs(host, who) {
  if (!host || !host.fs) return;
  try {
    const released = host.fs.closeAll();
    post({ type: 'log', text: `${who}: released ${released} store descriptor(s) on the coordinator` });
  } catch (e) {
    post({ type: 'log', text: `${who}: releasing store descriptors failed: ${e && e.message ? e.message : e}` });
  }
}

// One `wasi_thread_start` run. Returns false once the process is over.
function runThread(tid, startArg) {
  const { instance } = prewarmed;
  post({ type: 'thread-entered', tid, startArg });
  try {
    instance.exports.wasi_thread_start(tid, startArg);
    releaseFs(prewarmed.host, `thread ${tid}`);
    post({ type: 'thread-done', tid, trace: prewarmed.host.traceHead.slice() });
    return true;
  } catch (e) {
    if (e instanceof GuestExit) {
      // proc_exit from a spawned thread: the process is over, and nobody is
      // left to join us — announce it so the driver does not hang.
      releaseFs(prewarmed.host, `thread ${tid}`);
      prewarmed.stdout.close();
      post({ type: 'exit', from: 'thread', tid, code: e.code });
      return false;
    }
    releaseFs(prewarmed.host, `thread ${tid}`);
    post({
      type: 'error',
      from: 'thread',
      tid,
      message: String(e && e.stack ? e.stack : e),
      trace: prewarmed.host.traceHead.slice(),
    });
    return false;
  }
}

function threadCommandLoop() {
  const { desk, slot } = prewarmed;
  desk.markIdle(slot);
  for (;;) {
    const cmd = desk.awaitCommand(slot);
    if (!runThread(cmd.tid, cmd.startArg)) return;
    // The guest thread returned normally (pthread exit): this instance is
    // reusable, exactly as a real pool thread would be.
    desk.markDone(slot);
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
  } else {
    log(`unknown role ${msg.role}`);
  }
});
