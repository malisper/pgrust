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
// the guest is still cold, and thread-spawn is just a postMessage.
//
// KNOWN LIMIT, checkpoint (a): every worker builds its OWN Vfs from its own
// copy of the packed image, so the process instance and the session instance
// have INDEPENDENT filesystems that diverge the moment either writes. That is
// fine here because --stdio-wire-threaded runs the whole ladder (boot half
// included) on the one spawned session thread and the main thread only joins.
// A shared storage coordinator (one authoritative VFS behind a lock, or the
// VFS bytes themselves in shared memory) is checkpoint (b)+ work, not this
// spike's.

import { makeWasi, GuestExit } from './pgrust-wasi.js';
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

  // The emulated-noblock probe (fdnb::poll_ready -> poll(2) -> poll_oneoff)
  // must answer honestly for fd 0, or a "non-blocking" read would park in
  // Atomics.wait. Everything else keeps the stock always-ready answer, which
  // is what the single-threaded host does too.
  wasi.poll_oneoff = (inPtr, outPtr, nsubs, neventsPtr) => {
    const view = dv();
    let fired = 0;
    for (let i = 0; i < nsubs; i++) {
      const sub = inPtr + i * 48;
      const userdataLo = view.getUint32(sub, true);
      const userdataHi = view.getUint32(sub + 4, true);
      const tag = view.getUint8(sub + 8);
      if (tag === 1 /* fd_read */) {
        const subFd = view.getUint32(sub + 16, true);
        if (subFd === 0 && stdin.available() === 0 && !stdin.closed) continue;
      }
      const evt = outPtr + fired * 32;
      fired++;
      view.setUint32(evt, userdataLo, true);
      view.setUint32(evt + 4, userdataHi, true);
      view.setUint16(evt + 8, 0, true);
      view.setUint8(evt + 10, tag);
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
          console.error(`[${label}] thread-spawn refused (nested spawn is out of scope)`);
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
  trace = 0,
}) {
  const url = threadWorkerUrl(base);
  const idle = [];
  const byTid = new Map();
  const spawned = [];
  let nextTid = 1;

  function newWorker(slot) {
    const w = makeWorker(url, { name: `wasi-thread-slot-${slot}` });
    const rec = { worker: w, ready: null, tid: null };
    rec.ready = new Promise((resolve, reject) => {
      onWorkerMessage(w, (m) => {
        if (m && m.type === 'ready') {
          resolve(rec);
          return;
        }
        onEvent(Object.assign({ tid: rec.tid }, m));
      });
      onWorkerError(w, (e) => {
        reject(e);
        onEvent({ type: 'error', tid: rec.tid, message: String(e && e.message ? e.message : e) });
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
    };
    w.postMessage(payload, relay ? [imageCopy, relay] : [imageCopy]);
    return rec;
  }

  async function prewarm(n = 1) {
    const recs = [];
    for (let i = 0; i < n; i++) recs.push(newWorker(i));
    for (const r of recs) idle.push(await r.ready);
  }

  // Synchronous by construction — no await, no worker creation.
  function spawn(startArg) {
    const rec = idle.shift();
    if (!rec) {
      onEvent({ type: 'error', message: 'thread-spawn: prewarm pool exhausted' });
      return -EAGAIN;
    }
    const tid = nextTid++;
    rec.tid = tid;
    byTid.set(tid, rec);
    spawned.push(tid);
    onEvent({ type: 'spawn', tid, startArg });
    rec.worker.postMessage({ role: 'thread-start', tid, startArg });
    return tid;
  }

  function terminateAll() {
    for (const rec of [...idle, ...byTid.values()]) {
      try {
        rec.worker.terminate();
      } catch {
        /* already gone */
      }
    }
  }

  return { prewarm, spawn, spawned, byTid, terminateAll };
}

export { SabPipe, GuestExit };
