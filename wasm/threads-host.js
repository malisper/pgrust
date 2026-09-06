// threads-host.js — the JS host for the `wasm32-wasip1-threads` build of
// postgres.wasm (spike/wasip1-threads, checkpoint (a)).
//
// Difference from the shipped single-threaded host (pgrust-wasi.js +
// wiresession.js), in three bullets:
//
//   1. MEMORY IS IMPORTED AND SHARED. The wasm32-wasip1-threads target spec
//      pins `--import-memory --shared-memory`, so the module declares no
//      memory of its own: the host creates one
//      `new WebAssembly.Memory({ initial, maximum, shared: true })` and passes
//      the SAME object to every instance. Its limits must match the module's
//      declared limits (wasm/wasm-build.sh pins them with `--initial-memory` /
//      `--max-memory`); the import's module/name is read off the module itself
//      (`WebAssembly.Module.imports`) rather than hardcoded.
//
//   2. `wasi` `thread-spawn` IS THE HOST'S JOB. wasi-libc's pthread_create
//      lowers to an import `(start_arg: i32) -> i32` returning a positive
//      thread id or a negative errno. We answer it by instantiating the SAME
//      WebAssembly.Module in another worker over the SAME shared memory (its
//      own copy of every other import) and calling the module's
//      `wasi_thread_start(tid, start_arg)` export there. wasm-ld's
//      --shared-memory output initialises data segments exactly once (an
//      atomic guard), and `wasi_thread_start` sets up that thread's TLS and
//      stack itself, so there is nothing else for us to do.
//
//   3. NO JSPI. The guest's between-statements `read(0)` is a plain blocking
//      read again: it runs on a Worker, and a Worker may call Atomics.wait, so
//      fd 0 lands on SabPipe.readInto and simply BLOCKS — the same shape the
//      wasmtime host gives it. Nothing here touches WebAssembly.Suspending or
//      WebAssembly.promising.
//
// THE ONE NON-OBVIOUS CONSTRAINT: by the time the guest calls thread-spawn it
// is about to park in a futex (pthread_join -> memory.atomic.wait), and that
// parks the WHOLE JS thread the instance runs on. So the spawn handler may not
// await anything — its worker must already exist and already be instantiated.
// Hence the prewarmed pool below: workers are created and instantiated while
// the guest is still cold, and thread-spawn is a bare shared-memory publish
// (SpawnDesk) that any instance — including an already-spawned thread — can
// perform, because the second spawner on this target is the session thread
// creating the pg-timeout-timer thread while the process instance is parked.
//
// KNOWN LIMIT, checkpoint (a): every worker builds its OWN Vfs from its own
// copy of the packed image, so the process instance and the session instance
// have INDEPENDENT filesystems that diverge the moment either writes. That is
// fine here because --stdio-wire-threaded runs the whole ladder (boot half
// included) on the one spawned session thread and the main thread only joins.
// A shared storage coordinator (one authoritative VFS behind a lock, or the
// VFS bytes themselves in shared memory) is checkpoint (b)+ work, not this
// spike's.

import { makeWasi, GuestExit, monotonicNs } from './pgrust-wasi.js';
import { SabPipe } from './sab-pipe.js';

export const PAGE_BYTES = 65536;
// Must equal wasm/wasm-build.sh's PGRUST_WASM_INITIAL_MEMORY / _MAX_MEMORY.
export const DEFAULT_INITIAL_BYTES = 268435456; // 256MiB
export const DEFAULT_MAX_BYTES = 4294967296; // 4GiB (the wasm32 ceiling)

export const IS_NODE =
  typeof process !== 'undefined' && !!process.versions && !!process.versions.node;

// Resolved at module load, never inside a spawn: see THE ONE NON-OBVIOUS
// CONSTRAINT above — a spawn handler must not await.
const NodeWorkerThreads = IS_NODE ? await import('node:worker_threads') : null;

export function createSharedMemory({
  initialBytes = DEFAULT_INITIAL_BYTES,
  maxBytes = DEFAULT_MAX_BYTES,
} = {}) {
  return new WebAssembly.Memory({
    initial: initialBytes / PAGE_BYTES,
    maximum: maxBytes / PAGE_BYTES,
    shared: true,
  });
}

// What does this module actually want from us? Used to (a) find the memory
// import's real module/name instead of assuming "env"."memory", (b) find the
// thread-spawn import, and (c) fail loudly on an import we do not supply.
export function inspectImports(wasmModule) {
  const all = WebAssembly.Module.imports(wasmModule);
  const memory = all.find((i) => i.kind === 'memory') || null;
  const threadSpawn =
    all.find((i) => i.kind === 'function' && /thread[-_]spawn/.test(i.name)) || null;
  const tables = all.filter((i) => i.kind === 'table');
  const globals = all.filter((i) => i.kind === 'global');
  const modules = [...new Set(all.map((i) => i.module))];
  return { all, memory, threadSpawn, tables, globals, modules };
}

// wasi-libc turns a negative thread-spawn result into a failed pthread_create.
const EAGAIN = 6;

// ---------------------------------------------------------------------------
// SpawnDesk — the pool, addressed through shared memory instead of ports.
//
// Checkpoint (a) routed `thread-spawn` through the process worker's
// `postMessage`, which only works for the ONE spawn the process instance
// makes. Two things break that now:
//
//   * a SPAWNED thread must be able to spawn. The pg-timeout-timer thread is
//     created lazily by whichever backend first arms a timeout, and under
//     `--stdio-wire-threaded` that backend is the session thread, not the
//     process instance;
//   * the process worker cannot broker it. From the moment the guest joins,
//     that JS thread is parked in a futex for the whole session and will not
//     turn its event loop again — a spawn request relayed to it would be
//     answered after the fact, if at all. And `thread-spawn` may not await
//     (the caller is microseconds from parking in `pthread_join`).
//
// So the pool lives in a SharedArrayBuffer every instance holds. A spawner
// CASes a slot from IDLE to CLAIMED, allocates a tid with Atomics.add, writes
// the start_arg, publishes START and notifies; the slot's worker is parked in
// `Atomics.wait` on that same word and runs `wasi_thread_start`. No awaits,
// no ports, and it works identically from any agent.
//
// Layout (Int32Array): header [0]=nextTid, [1]=poolSize; then one 4-slot
// record per pool slot at HDR + slot*SLOT: [state, tid, startArg, spare].
const DESK_HDR = 4;
const DESK_SLOT = 4;
const D_NEXT_TID = 0;
const D_POOL_SIZE = 1;
const S_STATE = 0;
const S_TID = 1;
const S_ARG = 2;
// EMPTY: the slot's worker has not finished instantiating yet.
const SLOT_EMPTY = 0;
const SLOT_IDLE = 1;
const SLOT_CLAIMED = 2;
const SLOT_START = 3;
const SLOT_RUNNING = 4;

export class SpawnDesk {
  static create(poolSize) {
    const sab = new SharedArrayBuffer((DESK_HDR + poolSize * DESK_SLOT) * 4);
    const desk = new SpawnDesk(sab);
    Atomics.store(desk.a, D_POOL_SIZE, poolSize);
    return desk;
  }

  static from(desc) {
    return new SpawnDesk(desc.sab);
  }

  constructor(sab) {
    this.sab = sab;
    this.a = new Int32Array(sab);
  }

  descriptor() {
    return { sab: this.sab };
  }

  get poolSize() {
    return Atomics.load(this.a, D_POOL_SIZE);
  }

  _base(slot) {
    return DESK_HDR + slot * DESK_SLOT;
  }

  // Worker side: this slot is instantiated and about to park on its state
  // word. Called once, before the command loop.
  markIdle(slot) {
    const b = this._base(slot);
    Atomics.store(this.a, b + S_STATE, SLOT_IDLE);
    Atomics.notify(this.a, b + S_STATE);
  }

  // Worker side: block until a spawner publishes START on this slot; returns
  // { tid, startArg }. Blocks in Atomics.wait, so worker agents only.
  awaitCommand(slot) {
    const b = this._base(slot);
    for (;;) {
      const state = Atomics.load(this.a, b + S_STATE);
      if (state === SLOT_START) {
        const cmd = {
          tid: Atomics.load(this.a, b + S_TID),
          startArg: Atomics.load(this.a, b + S_ARG),
        };
        Atomics.store(this.a, b + S_STATE, SLOT_RUNNING);
        return cmd;
      }
      // Wait on the value we just observed: a publish that lands in this
      // window makes the wait return 'not-equal' instead of sleeping.
      Atomics.wait(this.a, b + S_STATE, state);
    }
  }

  // Spawner side: claim a free slot for `startArg`. Returns the positive tid,
  // or -1 when the pool is exhausted (the caller answers -EAGAIN, which is
  // what wasi-libc turns into a failed pthread_create). Synchronous: no
  // await, no worker creation — a spawn handler may do neither.
  claim(startArg) {
    const n = this.poolSize;
    for (let slot = 0; slot < n; slot++) {
      const b = this._base(slot);
      if (Atomics.compareExchange(this.a, b + S_STATE, SLOT_IDLE, SLOT_CLAIMED) !== SLOT_IDLE) {
        continue;
      }
      const tid = Atomics.add(this.a, D_NEXT_TID, 1) + 1;
      Atomics.store(this.a, b + S_TID, tid);
      Atomics.store(this.a, b + S_ARG, startArg);
      Atomics.store(this.a, b + S_STATE, SLOT_START);
      Atomics.notify(this.a, b + S_STATE);
      return { tid, slot };
    }
    return { tid: -1, slot: -1 };
  }

  // Worker side: `wasi_thread_start` returned, so this instance is reusable.
  // (A thread that never returns — the timer thread — holds its slot for the
  // life of the process, which is exactly a real pthread's behaviour.)
  markDone(slot) {
    this.markIdle(slot);
  }
}

// The import object + WASI state for ONE instance (the process instance or one
// spawned thread's instance).
export function makeThreadsHost({
  wasmModule,
  memory,
  image,
  manifest,
  stdin,
  stdout,
  onStderr,
  argv,
  env,
  spawn,
  label = 'guest',
  trace = 0,
}) {
  const info = inspectImports(wasmModule);
  if (!info.memory) {
    throw new Error(
      'threads-host: module declares its own memory — this is not a ' +
        'wasm32-wasip1-threads (--import-memory --shared-memory) build',
    );
  }
  if (info.tables.length || info.globals.length) {
    // Neither is structured-cloneable, so a build that needs them cannot be
    // shared across workers this way. Say so rather than emitting a mystery
    // LinkError.
    throw new Error(
      `threads-host: module imports ${info.tables.length} table(s) and ` +
        `${info.globals.length} global(s); only an imported memory is supported`,
    );
  }

  const imageU8 = image instanceof Uint8Array ? image : new Uint8Array(image);

  // fd 1: blocking-pipe semantics — park if the ring is full, exactly like a
  // real pipe whose reader is behind.
  const onStdout = (bytes) => {
    stdout.write(bytes);
  };

  const h = makeWasi({
    image: imageU8,
    manifest,
    onStdout,
    onStderr,
    argv,
    env,
  });
  // Imported memory: the host owns it, so it is known BEFORE instantiation
  // (the single-threaded host sets it from instance.exports.memory after).
  h.setMemory(memory);

  const wasi = h.wasi;
  const u8 = () => new Uint8Array(memory.buffer);
  const dv = () => new DataView(memory.buffer);
  function* iovs(ptr, n) {
    const view = dv();
    for (let i = 0; i < n; i++) {
      const base = ptr + i * 8;
      yield { ptr: view.getUint32(base, true), len: view.getUint32(base + 4, true) };
    }
  }

  const innerRead = wasi.fd_read;
  // fd 0: the ONE blocking primitive of the whole design. Everything else
  // delegates to the stock host.
  wasi.fd_read = (fd, iovsPtr, iovsLen, nreadPtr) => {
    if (fd !== 0) return innerRead(fd, iovsPtr, iovsLen, nreadPtr);
    let total = 0;
    for (const { ptr, len } of iovs(iovsPtr, iovsLen)) {
      if (len === 0) continue;
      const scratch = new Uint8Array(len);
      const n = stdin.readInto(scratch, len); // BLOCKS; 0 == EOF
      if (n > 0) {
        u8().set(scratch.subarray(0, n), ptr);
        total = n;
      }
      break; // one pipe read per call, like read(2) on a pipe
    }
    dv().setUint32(nreadPtr, total, true);
    return 0; // ESUCCESS; total == 0 is EOF, which is what the guest wants
  };

  // Chrome refuses crypto.getRandomValues() on a view backed by a
  // SharedArrayBuffer ("The provided ArrayBufferView value must not be
  // shared" — the WebCrypto argument is NOT [AllowShared]). The stock host
  // fills guest memory in place, which is fine for a private memory and a
  // hard TypeError for this one; it surfaces as a JS exception thrown out of
  // a WASI import, which the guest's nounwind frames turn into
  // "thread caused non-unwinding panic. aborting." Fill a private buffer and
  // copy. (Node's webcrypto happens to accept the shared view, so this ONLY
  // shows up in the browser — hence the control arm in threads-page.js.)
  wasi.random_get = (ptr, len) => {
    const scratch = new Uint8Array(len);
    const g = typeof crypto !== 'undefined' && crypto.getRandomValues ? crypto : null;
    for (let off = 0; off < len; off += 65536) {
      const chunk = scratch.subarray(off, Math.min(off + 65536, len));
      if (g) g.getRandomValues(chunk);
      else for (let i = 0; i < chunk.length; i++) chunk[i] = (Math.random() * 256) | 0;
    }
    u8().set(scratch, ptr);
    return 0;
  };

  // poll_oneoff, for real. Two callers matter on this target:
  //   * the emulated-noblock probe (fdnb::poll_ready -> poll(2)) — must
  //     answer honestly for fd 0, or a "non-blocking" read would park in
  //     Atomics.wait;
  //   * every timed sleep std lowers to a clock subscription
  //     (std::thread::sleep, nanosleep) — the single-threaded host answers
  //     those "already fired", which turns a sleep into a busy spin. A
  //     backend that must honour statement_timeout while another thread runs
  //     the timer cannot spin: it has to give the CPU up.
  //
  // Subscription layout (48 bytes): userdata u64 @0, tag u8 @8; clock arm =
  // id u32 @16, timeout u64 ns @24, precision u64 @32, flags u16 @40 (bit 0
  // = SUBSCRIPTION_CLOCK_ABSTIME); fd arm = fd u32 @16. Event layout (32
  // bytes): userdata u64 @0, errno u16 @8, type u8 @10, nbytes u64 @16,
  // flags u16 @24.
  //
  // Every fd OTHER than 0 keeps the stock always-ready answer (the guest
  // only ever polls its stdio here, and fd 1/2 writes never block).
  const parkWord = new Int32Array(new SharedArrayBuffer(4));
  wasi.poll_oneoff = (inPtr, outPtr, nsubs, neventsPtr) => {
    const view = dv();
    const subs = [];
    let nearestMs = null; // ms from now to the earliest clock deadline
    for (let i = 0; i < nsubs; i++) {
      const p = inPtr + i * 48;
      const s = {
        lo: view.getUint32(p, true),
        hi: view.getUint32(p + 4, true),
        tag: view.getUint8(p + 8),
      };
      if (s.tag === 0 /* clock */) {
        const clockId = view.getUint32(p + 16, true);
        const timeoutNs = view.getBigUint64(p + 24, true);
        const abstime = (view.getUint16(p + 40, true) & 1) !== 0;
        if (abstime) {
          const nowNs = clockId === 1 ? monotonicNs() : BigInt(Date.now()) * 1000000n;
          s.deadlineMs = Number(timeoutNs - nowNs) / 1e6;
        } else {
          s.deadlineMs = Number(timeoutNs) / 1e6;
        }
        if (nearestMs === null || s.deadlineMs < nearestMs) nearestMs = s.deadlineMs;
      } else {
        s.fd = view.getUint32(p + 16, true);
      }
      subs.push(s);
    }

    // A subscription is ready now if it is a non-fd-0 fd (always), fd 0 with
    // data or at EOF, or a clock whose deadline has already passed.
    const ready = (s, elapsedMs) =>
      s.tag === 0
        ? s.deadlineMs <= elapsedMs
        : s.fd !== 0 || stdin.available() > 0 || stdin.closed;

    if (nsubs === 0) {
      view.setUint32(neventsPtr, 0, true);
      return 0;
    }

    const start = Date.now();
    let elapsed = 0;
    if (!subs.some((s) => ready(s, 0))) {
      // Nothing is ready: block. Bounded by the nearest clock deadline, or
      // forever-ish when the guest asked for an untimed wait (clamped well
      // under the 2^31 ms Atomics.wait ceiling; the guest re-polls).
      const budget = nearestMs === null ? 60000 : Math.min(nearestMs, 60000);
      const fdWait = subs.some((s) => s.tag === 1 && s.fd === 0);
      if (budget > 0) {
        if (fdWait) stdin.waitReadable(budget);
        else Atomics.wait(parkWord, 0, 0, budget);
      }
      elapsed = Date.now() - start;
      // We slept the whole budget unless an fd woke us (in which case that
      // fd is ready below). Date.now()'s granularity must not be allowed to
      // report "0 events" on a pure sleep — the caller would re-poll for a
      // deadline it has already reached, forever.
      if (budget > 0 && !subs.some((s) => ready(s, elapsed))) elapsed = budget;
    }

    let fired = 0;
    for (const s of subs) {
      if (!ready(s, elapsed)) continue;
      const evt = outPtr + fired * 32;
      fired++;
      view.setUint32(evt, s.lo, true);
      view.setUint32(evt + 4, s.hi, true);
      view.setUint16(evt + 8, 0, true);
      view.setUint8(evt + 10, s.tag);
      view.setBigUint64(evt + 16, 0n, true);
      view.setUint16(evt + 24, 0, true);
    }
    view.setUint32(neventsPtr, fired, true);
    return 0;
  };

  // Diagnostics: keep the last `trace` WASI calls in a ring so a guest abort
  // (which arrives as a bare `RuntimeError: unreachable`, and whose Rust-side
  // message the elog panic hook suppresses for PgError payloads) can still be
  // attributed to the host call that preceded it. Off by default — zero cost.
  const traceRing = [];
  const traceHead = [];
  if (trace > 0) {
    // (name, [pathPtrIdx, pathLenIdx]) for the calls whose interesting
    // argument is a guest string.
    const PATHARG = {
      path_open: [2, 3],
      path_filestat_get: [2, 3],
      path_create_directory: [1, 2],
      path_remove_directory: [1, 2],
      path_unlink_file: [1, 2],
      path_readlink: [1, 2],
      path_rename: [1, 2],
    };
    const dec = new TextDecoder('utf-8', { fatal: false });
    for (const name of Object.keys(wasi)) {
      const inner = wasi[name];
      const pa = PATHARG[name];
      wasi[name] = (...args) => {
        let what = '';
        if (pa) {
          try {
            what = ' "' + dec.decode(u8().subarray(args[pa[0]], args[pa[0]] + args[pa[1]])) + '"';
          } catch {
            what = ' <unreadable>';
          }
        }
        let rc;
        try {
          rc = inner(...args);
        } catch (e) {
          // A THROWING import is the interesting case: it leaves wasm through
          // a foreign exception, which the guest's nounwind frames turn into
          // "panic in a function that cannot unwind" with no Rust message at
          // all. Record it before it escapes.
          const bad = `${name}(${args.join(',')})${what} -> THREW ${e}`;
          if (traceHead.length < trace) traceHead.push(bad);
          traceRing.push(bad);
          if (traceRing.length > trace) traceRing.shift();
          throw e;
        }
        const line = `${name}(${args.join(',')})${what} -> ${rc}`;
        if (traceHead.length < trace) traceHead.push(line);
        traceRing.push(line);
        if (traceRing.length > trace) traceRing.shift();
        return rc;
      };
    }
  }

  const imports = {};
  const put = (mod, name, value) => {
    imports[mod] = imports[mod] || {};
    imports[mod][name] = value;
  };
  put(info.memory.module, info.memory.name, memory);
  for (const i of info.all) {
    if (i.kind !== 'function') continue;
    if (info.threadSpawn && i.module === info.threadSpawn.module && i.name === info.threadSpawn.name) {
      put(i.module, i.name, (startArg) => {
        if (!spawn) {
          console.error(`[${label}] thread-spawn refused (no spawn desk on this instance)`);
          return -EAGAIN;
        }
        return spawn(startArg | 0);
      });
      continue;
    }
    const fn = wasi[i.name];
    if (typeof fn !== 'function') {
      throw new Error(`threads-host: unsupported import ${i.module}.${i.name}`);
    }
    put(i.module, i.name, fn);
  }

  return { imports, wasi, vfs: h.vfs, info, traceRing, traceHead };
}

// ---------------------------------------------------------------------------
// Worker plumbing. Node's worker_threads and the browser's Worker differ in
// three places only (constructor, message channel, entry file extension), so
// runtime-detect and move on.
// ---------------------------------------------------------------------------

export function makeWorker(url, { name } = {}) {
  if (IS_NODE) return new NodeWorkerThreads.Worker(url, { name });
  return new Worker(url, { type: 'module', name });
}

export function onWorkerMessage(worker, cb) {
  if (IS_NODE) worker.on('message', cb);
  else worker.addEventListener('message', (e) => cb(e.data));
}

export function onWorkerError(worker, cb) {
  if (IS_NODE) worker.on('error', cb);
  else worker.addEventListener('error', (e) => cb(e.error || new Error(e.message || String(e))));
}

// A spawned thread must report to the DRIVER directly, not through the process
// worker: from the moment the guest joins, the process worker's JS thread is
// parked in a futex and will not turn its event loop again until the session
// is over — anything relayed through it (stderr above all) would arrive only
// after the fact, if at all. So the driver mints one MessageChannel per pool
// slot and the thread worker gets the far end.
export function newMessageChannel() {
  if (IS_NODE) return new NodeWorkerThreads.MessageChannel();
  return new MessageChannel();
}

export function onPortMessage(port, cb) {
  if (IS_NODE) {
    port.on('message', cb);
  } else {
    port.addEventListener('message', (e) => cb(e.data));
    port.start();
  }
}

export function threadWorkerUrl(base) {
  // Node wants a .mjs entry (do not rely on its ESM syntax detection for a
  // bare .js worker entry); the browser is happy with the .js module.
  return new URL(IS_NODE ? './thread-worker.mjs' : './thread-worker.js', base);
}

// The thread-spawn implementation for whichever instance owns spawning (the
// process instance in this spike). Thread ids start at 1 and stay small, as
// the wasi-threads ABI asks.
//
// `prewarm(n)` MUST be awaited before the guest's `_start`: see THE ONE
// NON-OBVIOUS CONSTRAINT at the top of this file.
export function makeSpawner({
  wasmModule,
  memory,
  image,
  manifest,
  stdinDesc,
  stdoutDesc,
  argv,
  env,
  base,
  onEvent,
  relayPorts = [],
  poolSize = 1,
  trace = 0,
}) {
  const url = threadWorkerUrl(base);
  const workers = [];
  const spawned = [];
  // Created here (the process worker knows the pool size) and handed to every
  // pool worker, so each of them can spawn too — see SpawnDesk above.
  const desk = SpawnDesk.create(poolSize);

  function newWorker(slot) {
    const w = makeWorker(url, { name: `wasi-thread-slot-${slot}` });
    const rec = { worker: w, ready: null, slot };
    rec.ready = new Promise((resolve, reject) => {
      onWorkerMessage(w, (m) => {
        if (m && m.type === 'ready') {
          resolve(rec);
          return;
        }
        onEvent(Object.assign({ slot }, m));
      });
      onWorkerError(w, (e) => {
        reject(e);
        onEvent({ type: 'error', slot, message: String(e && e.message ? e.message : e) });
      });
    });
    // Each spawned thread gets its own VFS, hence its own copy of the image
    // bytes (see the KNOWN LIMIT note at the top of this file).
    const imageCopy = image.slice(0);
    const relay = relayPorts[slot] || null;
    const payload = {
      role: 'thread-prewarm',
      module: wasmModule,
      memory,
      image: imageCopy,
      manifest,
      stdin: stdinDesc,
      stdout: stdoutDesc,
      argv,
      env,
      relay,
      trace,
      desk: desk.descriptor(),
      slot,
    };
    w.postMessage(payload, relay ? [imageCopy, relay] : [imageCopy]);
    return rec;
  }

  async function prewarm(n = 1) {
    const recs = [];
    for (let i = 0; i < n; i++) recs.push(newWorker(i));
    for (const r of recs) workers.push(await r.ready);
  }

  // Synchronous by construction — no await, no worker creation.
  function spawn(startArg) {
    const { tid, slot } = desk.claim(startArg);
    if (tid < 0) {
      onEvent({
        type: 'spawn-refused',
        from: 'process',
        startArg,
        poolSize: desk.poolSize,
        errno: EAGAIN,
      });
      return -EAGAIN;
    }
    spawned.push(tid);
    onEvent({ type: 'spawn', from: 'process', tid, slot, startArg });
    return tid;
  }

  function terminateAll() {
    for (const rec of workers) {
      try {
        rec.worker.terminate();
      } catch {
        /* already gone */
      }
    }
  }

  return { prewarm, spawn, spawned, desk, terminateAll };
}

export { SabPipe, GuestExit, EAGAIN };
