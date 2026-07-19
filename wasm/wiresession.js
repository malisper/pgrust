// wiresession.js — ONE long-lived `postgres --stdio-wire` wasm instance,
// spoken to in pgwire frames over the stdin/stdout host pipes.
//
// This is the browser/Node port of the wasm-net-e2e host driver shape
// (wasm/pgwire_stdio_driver.py under wasmtime): the guest serves a full
// wire-protocol session — startup handshake, simple-query cycles, Terminate —
// so session state (temp tables, prepared statements, open transactions)
// PERSISTS across REPL statements, which `--single`-per-statement cannot do.
//
// The blocking primitive: the guest's between-statements read is a plain
// blocking `read(0)` (pqcomm_stdio::secure_read -> WASI fd_read). A browser
// worker has no way to block a synchronous import, so the fd_read import is
// wrapped in WebAssembly.Suspending and `_start` in WebAssembly.promising —
// JS Promise Integration (JSPI). When stdin has no queued frames, fd_read
// returns a Promise and the GUEST SUSPENDS; the worker's event loop keeps
// running; pushing the next frame resolves the promise and resumes the guest.
// Engines without JSPI (WebKit/Safari as of mid-2026) can't run this class at
// all — worker.js feature-detects and falls back to the --single engine.
//
// Environment-agnostic (browser worker + Node): only WebAssembly + the
// pgrust-wasi.js host are used.

import { makeWasi, GuestExit } from './pgrust-wasi.js';
import { WireReader, encodeStartup, encodeQuery, TERMINATE, parseMessage, canonMessage } from './wire.js';

export function jspiSupported() {
  return typeof WebAssembly.Suspending === 'function' &&
         typeof WebAssembly.promising === 'function';
}

// Default argv: the same engine GUCs as the --single worker path (see
// pgrust-wasi.js makeWasi), with --stdio-wire instead of --single. Callers
// (the e2e harness) may override argv wholesale, e.g. to pin timezone=UTC for
// transcript identity with the native arm.
export function defaultWireArgv(extraGucs = []) {
  return [
    'postgres', '--stdio-wire',
    '-D', '/pgdata',
    '-c', 'max_stack_depth=60000',
    '-c', 'io_method=sync',
    '-c', 'autovacuum=off',
    '-c', 'wal_sync_method=fdatasync',
    '-c', 'shared_buffers=32MB',
    ...extraGucs,
    'postgres',
  ];
}

export class WireSessionDead extends Error {}

export class WireSession {
  constructor({ wasmModule, vfs, argv, env, onStderr, onMessage }) {
    this.wasmModule = wasmModule;
    this.vfs = vfs;
    this.argv = argv || defaultWireArgv();
    this.env = env;
    this.onStderr = onStderr || (() => {});
    this.onMessage = onMessage || null; // optional tap: (t, body) per backend message
    this.reader = new WireReader();
    this.exitCode = null;   // set once the guest exits
    this.dead = false;
    this.startPromise = null;
    // stdin stream state
    this._q = [];           // queued Uint8Array chunks
    this._qPos = 0;
    this._eof = false;
    this._stdinWaiters = [];
    // message collector state
    this._inbox = [];       // parsed-but-undelivered { t, body }
    this._collector = null; // { msgs, resolve, reject } until next ReadyForQuery
  }

  // ---- stdin stream given to makeWasi ---------------------------------------
  _stdinStream() {
    const s = this;
    return {
      ready() { return s._q.length > 0; },
      isEof() { return s._eof; },
      take(maxLen) {
        if (s._q.length === 0) return null;
        const head = s._q[0];
        const avail = head.length - s._qPos;
        const k = Math.min(maxLen, avail);
        const chunk = head.subarray(s._qPos, s._qPos + k);
        s._qPos += k;
        if (s._qPos >= head.length) { s._q.shift(); s._qPos = 0; }
        return chunk;
      },
      wait() {
        return new Promise((resolve) => { s._stdinWaiters.push(resolve); });
      },
    };
  }

  _pushStdin(bytes) {
    if (bytes && bytes.length) this._q.push(bytes);
    const waiters = this._stdinWaiters;
    this._stdinWaiters = [];
    for (const w of waiters) w();
  }

  _closeStdin() {
    this._eof = true;
    const waiters = this._stdinWaiters;
    this._stdinWaiters = [];
    for (const w of waiters) w();
  }

  // ---- backend message pump -------------------------------------------------
  _onStdout(bytes) {
    this.reader.feed(bytes);
    let m;
    while ((m = this.reader.next()) !== null) {
      if (this.onMessage) this.onMessage(m.t, m.body);
      if (this._collector) {
        this._collector.msgs.push(m);
        if (m.t === 'Z') {
          const c = this._collector;
          this._collector = null;
          c.resolve(c.msgs);
        }
      } else {
        this._inbox.push(m);
      }
    }
  }

  _collectUntilReady() {
    return new Promise((resolve, reject) => {
      const c = { msgs: [], resolve, reject };
      // Drain anything that arrived before the collector was armed.
      while (this._inbox.length) {
        const m = this._inbox.shift();
        c.msgs.push(m);
        if (m.t === 'Z') { resolve(c.msgs); return; }
      }
      if (this.dead) { reject(new WireSessionDead(`session exited (code ${this.exitCode})`)); return; }
      this._collector = c;
    });
  }

  _fail(err) {
    this.dead = true;
    if (this._collector) {
      const c = this._collector;
      this._collector = null;
      c.reject(err);
    }
  }

  // ---- lifecycle ------------------------------------------------------------
  async start({ user = 'postgres', database = 'postgres' } = {}) {
    if (this.startPromise) throw new Error('session already started');
    const h = makeWasi({
      vfs: this.vfs,
      stdinStream: this._stdinStream(),
      onStdout: (b) => this._onStdout(b),
      onStderr: (b) => this.onStderr(b),
      argv: this.argv,
      env: this.env,
    });
    const imports = {
      wasi_snapshot_preview1: {
        ...h.wasi,
        // The ONE suspension point: a blocking stdin read with no queued
        // frames returns a Promise (see pgrust-wasi.js fd_read).
        fd_read: new WebAssembly.Suspending(h.wasi.fd_read),
      },
    };
    const instance = await WebAssembly.instantiate(this.wasmModule, imports);
    h.setMemory(instance.exports.memory);
    const startAsync = WebAssembly.promising(instance.exports._start);
    this.startPromise = startAsync()
      .then(() => { this.exitCode = 0; this._fail(new WireSessionDead('session exited (code 0)')); return 0; })
      .catch((e) => {
        if (e instanceof GuestExit) {
          this.exitCode = e.code;
          this._fail(new WireSessionDead(`session exited (code ${e.code})`));
          return e.code;
        }
        this.exitCode = -1;
        this._fail(e instanceof Error ? e : new Error(String(e)));
        throw e;
      });
    // Handshake: StartupMessage -> R(auth) ... S* ... K ... Z(I)
    const collect = this._collectUntilReady();
    this._pushStdin(encodeStartup({ user, database }));
    return collect;
  }

  // One simple-query cycle: Q(sql) -> ... -> ReadyForQuery. Returns the raw
  // { t, body } message list (parse with parseMessage / canonMessage).
  async query(sql) {
    if (this.dead) throw new WireSessionDead(`session exited (code ${this.exitCode})`);
    if (this._collector) throw new Error('a query is already in flight on this session');
    const collect = this._collectUntilReady();
    this._pushStdin(encodeQuery(sql));
    return collect;
  }

  // Terminate (X) and wait for the guest's clean exit (shutdown checkpoint
  // runs on this path). Returns the exit code.
  async terminate() {
    if (!this.dead) {
      this._pushStdin(TERMINATE);
      this._closeStdin();
    }
    try { await this.startPromise; } catch { /* non-exit failure already recorded */ }
    return this.exitCode;
  }

  // Bytes that arrived after the last complete message (should be 0 after a
  // clean Terminate — the e2e checks it like the python driver's leftover read).
  leftoverBytes() { return this.reader.buffered; }
}

export { parseMessage, canonMessage };
