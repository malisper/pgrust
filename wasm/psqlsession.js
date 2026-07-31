// psqlsession.js — run the Rust psql.wasm as the REPL frontend, cross-wired
// to `postgres --stdio-wire` wasm instance(s) over host pipes.
//
// Topology (increment 2/3 of the Rust-psql plan):
//   psql fd 0/1/2 = terminal (host-provided input stream / output callbacks)
//   psql fd 4     = READ side of the server connection (server's stdout)
//   psql fd 5     = WRITE side of the server connection (server's stdin)
//   (fd 3 is the WASI "/" preopen — see pgrust-wasi.js.)
//
// --stdio-wire serves exactly ONE wire session and exits at Terminate. psql's
// \c therefore terminates the old session FIRST, then sends a fresh startup
// packet; this manager parses the psql->server frame stream, and when it sees
// the Terminate ('X') frame it closes the current instance's stdin, lets the
// guest exit (shutdown checkpoint included), and respawns a new instance ON
// THE SAME VFS for the next session's bytes. Database state persists in the
// VFS across sessions — which is what makes `\c otherdb` REAL (issue #46).
//
// Requires JSPI (WebAssembly.Suspending/promising), like wiresession.js.

import { makeWasi, GuestExit } from './pgrust-wasi.js';
import { defaultWireArgv } from './wiresession.js';

export function makePushStream() {
  const s = {
    q: [], qPos: 0, eof: false, waiters: [],
    ready() { return s.q.length > 0; },
    isEof() { return s.eof; },
    take(maxLen) {
      if (s.q.length === 0) return null;
      const head = s.q[0];
      const k = Math.min(maxLen, head.length - s.qPos);
      const chunk = head.subarray(s.qPos, s.qPos + k);
      s.qPos += k;
      if (s.qPos >= head.length) { s.q.shift(); s.qPos = 0; }
      return chunk;
    },
    wait() { return new Promise((r) => s.waiters.push(r)); },
    push(bytes) {
      if (bytes && bytes.length) s.q.push(bytes);
      const w = s.waiters; s.waiters = [];
      for (const f of w) f();
    },
    close() {
      s.eof = true;
      const w = s.waiters; s.waiters = [];
      for (const f of w) f();
    },
  };
  return s;
}

// Split the psql->server byte stream into pgwire frames, so the manager can
// spot session boundaries. At session start the first frame is a startup
// packet (untyped, 4-byte big-endian length INCLUDING itself); afterwards
// frames are typed (1 byte + 4-byte length including itself). 'X' ends the
// session; the next frame is a new session's startup packet.
class FrameSplitter {
  constructor(onFrame) {
    this.buf = new Uint8Array(0);
    this.awaitingStartup = true;
    this.onFrame = onFrame; // (bytes, { terminate })
  }
  feed(bytes) {
    const merged = new Uint8Array(this.buf.length + bytes.length);
    merged.set(this.buf); merged.set(bytes, this.buf.length);
    this.buf = merged;
    for (;;) {
      if (this.awaitingStartup) {
        if (this.buf.length < 4) return;
        const len = new DataView(this.buf.buffer, this.buf.byteOffset).getUint32(0, false);
        if (this.buf.length < len) return;
        this.onFrame(this.buf.slice(0, len), { terminate: false });
        this.buf = this.buf.slice(len);
        this.awaitingStartup = false;
      } else {
        if (this.buf.length < 5) return;
        const t = this.buf[0];
        const len = new DataView(this.buf.buffer, this.buf.byteOffset).getUint32(1, false);
        if (this.buf.length < 1 + len) return;
        const terminate = t === 0x58; // 'X'
        this.onFrame(this.buf.slice(0, 1 + len), { terminate });
        this.buf = this.buf.slice(1 + len);
        if (terminate) this.awaitingStartup = true;
      }
    }
  }
}

// Split the server->psql byte stream far enough to spot ReadyForQuery ('Z').
// Backend messages are ALWAYS typed (1 byte + 4-byte length including itself)
// — there is no untyped startup frame on this direction — so one shape does.
// A 'Z' means the server is idle waiting for the next client message, which is
// the only quiesce point where the host may serialize the VFS (see worker.js's
// persistence cadence in psql client mode). onType may RETURN A PROMISE: the
// chunk carrying that message is then withheld from psql until it resolves —
// the durability gate (results paint only after the snapshot covering them is
// flushed).
class BackendFrameSplitter {
  constructor(onType) {
    this.buf = new Uint8Array(0);
    this.onType = onType;
  }
  feed(bytes) {
    const merged = new Uint8Array(this.buf.length + bytes.length);
    merged.set(this.buf); merged.set(bytes, this.buf.length);
    this.buf = merged;
    for (;;) {
      if (this.buf.length < 5) return;
      const t = this.buf[0];
      const len = new DataView(this.buf.buffer, this.buf.byteOffset).getUint32(1, false);
      if (len < 4 || this.buf.length < 1 + len) return;
      this.buf = this.buf.slice(1 + len);
      this.onType(t);
    }
  }
}

export class PsqlSession {
  /**
   * @param {object} opts
   *   psqlModule / serverModule — compiled WebAssembly.Module objects
   *   vfs                      — shared Vfs (server datadir + share tree)
   *   psqlArgv / psqlEnv       — psql argv/env (defaults provided)
   *   serverArgv / serverEnv   — postgres argv/env (--stdio-wire defaults)
   *   onPsqlStdout/onPsqlStderr— psql terminal output callbacks (Uint8Array)
   *   onServerStderr           — server log callback
   *   onServerIdle             — called after every ReadyForQuery the server
   *                              sends psql (a VFS quiesce point). May return
   *                              a promise; the bytes carrying that
   *                              ReadyForQuery are NOT delivered to psql until
   *                              it resolves. worker.js returns the snapshot
   *                              write here when persist is on, so a result
   *                              can only appear on screen once the datadir
   *                              state that produced it is durable.
   *   psqlStdin                — optional push stream for interactive input;
   *                              when absent, psqlStdinBytes is read to EOF
   *   psqlStdinBytes           — fixed script input (headless mode)
   */
  constructor(opts) {
    this.o = opts;
    this.serverStdin = null;      // current instance's stdin push stream
    this.serverExit = null;       // promise of current instance exit
    this.psqlRead = makePushStream(); // server->psql bytes (fd 4)
    this.pendingServerBytes = []; // frames awaiting the next instance
    this.serverAlive = false;
    this.terminatedByPsql = false;
    this.sessionClosed = false;
    this.spawning = false;
    this.stopping = false;        // stop() called: no more respawns
    this.psqlExit = null;
    this.splitter = new FrameSplitter((frame, { terminate }) => {
      this._routeFrame(frame, terminate);
    });
    // Server->psql delivery is serialized through deliverChain so a gated
    // chunk (see onServerIdle) cannot be overtaken by a later one.
    this.deliverChain = Promise.resolve();
    this._chunkGates = null; // gates collected while feeding ONE chunk
    this.backendSplitter = opts.onServerIdle
      ? new BackendFrameSplitter((t) => {
          if (t !== 0x5A) return;
          const p = opts.onServerIdle();
          if (p && typeof p.then === 'function' && this._chunkGates) this._chunkGates.push(p);
        })
      : null;
  }

  // Deliver one server-stdout chunk to psql, after any gates its messages
  // raised. A failed gate (snapshot error) must never wedge psql's read side.
  _deliverToPsql(bytes, gates) {
    this.deliverChain = this.deliverChain.then(async () => {
      if (gates && gates.length) {
        try { await Promise.all(gates); } catch { /* reported via persist-state */ }
      }
      this.psqlRead.push(bytes);
    });
  }

  _routeFrame(frame, terminate) {
    // sessionClosed: psql already sent Terminate — the instance may still be
    // winding down (serverAlive true), but every subsequent frame belongs to
    // the NEXT session and must wait for the respawn.
    if (this.serverAlive && !this.sessionClosed) {
      this.serverStdin.push(frame);
      if (terminate) {
        this.sessionClosed = true;
        this.terminatedByPsql = true;
        this.serverStdin.close();
      }
    } else {
      this.pendingServerBytes.push(frame);
      this._ensureServer();
    }
  }

  async _ensureServer() {
    if (this.spawning || this.stopping) return;
    if (this.serverAlive && !this.sessionClosed) return;
    this.spawning = true;
    try {
      if (this.serverExit) { try { await this.serverExit; } catch { /* logged */ } }
      await this._spawnServer();
    } finally {
      this.spawning = false;
    }
    // Feed anything that queued while we were spawning.
    const pend = this.pendingServerBytes;
    this.pendingServerBytes = [];
    for (const f of pend) this.serverStdin.push(f);
  }

  async _spawnServer() {
    const o = this.o;
    this.serverStdin = makePushStream();
    this.terminatedByPsql = false;
    this.sessionClosed = false;
    // A fresh instance's output starts at a message boundary; drop any partial
    // bytes the previous instance left in the splitter.
    if (this.backendSplitter) this.backendSplitter.buf = new Uint8Array(0);
    const h = makeWasi({
      vfs: o.vfs,
      stdinStream: this.serverStdin,
      onStdout: (b) => {
        if (!this.backendSplitter) { this._deliverToPsql(b, null); return; }
        this._chunkGates = [];
        this.backendSplitter.feed(b);
        const gates = this._chunkGates;
        this._chunkGates = null;
        this._deliverToPsql(b, gates);
      },
      onStderr: o.onServerStderr || (() => {}),
      // NOT makeWasi's default argv — that is --single, which would eat the
      // startup packet as SQL text. The server MUST speak the wire protocol.
      argv: o.serverArgv || defaultWireArgv(),
      env: o.serverEnv,
    });
    const imports = {
      wasi_snapshot_preview1: {
        ...h.wasi,
        fd_read: new WebAssembly.Suspending(h.wasi.fd_read),
      },
    };
    const instance = await WebAssembly.instantiate(o.serverModule, imports);
    h.setMemory(instance.exports.memory);
    const startAsync = WebAssembly.promising(instance.exports._start);
    this.serverAlive = true;
    this.serverExit = startAsync()
      .then(() => 0)
      .catch((e) => {
        if (e instanceof GuestExit) return e.code;
        throw e;
      })
      .then((code) => {
        this.serverAlive = false;
        // Unsolicited death (crash / FATAL exit): psql must see EOF on its
        // read side — but only AFTER every already-produced chunk has cleared
        // the delivery chain. A psql-driven Terminate keeps the pipe open for
        // the next session instead.
        if (!this.terminatedByPsql && this.pendingServerBytes.length === 0) {
          this.deliverChain = this.deliverChain.then(() => this.psqlRead.close());
        }
        return code;
      });
  }

  async start() {
    const o = this.o;
    await this._ensureServer();
    const pipes = new Map([
      [4, { kind: 'piperead', stream: this.psqlRead }],
      [5, { kind: 'pipewrite', onWrite: (b) => this.splitter.feed(b) }],
    ]);
    const h = makeWasi({
      vfs: o.vfs,
      stdinStream: o.psqlStdin || undefined,
      stdinBytes: o.psqlStdin ? undefined : (o.psqlStdinBytes || new Uint8Array(0)),
      onStdout: o.onPsqlStdout || (() => {}),
      onStderr: o.onPsqlStderr || (() => {}),
      argv: o.psqlArgv || ['psql'],
      env: o.psqlEnv || { USER: 'postgres', PSQL_INTERACTIVE: '0' },
      pipes,
    });
    const imports = {
      wasi_snapshot_preview1: {
        ...h.wasi,
        fd_read: new WebAssembly.Suspending(h.wasi.fd_read),
      },
    };
    const instance = await WebAssembly.instantiate(o.psqlModule, imports);
    h.setMemory(instance.exports.memory);
    const startAsync = WebAssembly.promising(instance.exports._start);
    // NOTE: not returned from start() — an async fn's `await` would unwrap
    // the promise and block until psql exits.
    this.psqlExit = startAsync()
      .then(() => 0)
      .catch((e) => {
        if (e instanceof GuestExit) return e.code;
        throw e;
      });
  }

  /**
   * Tear the whole session down: psql first, then the server instance.
   *
   * ORDER MATTERS. Closing psql's stdin gives it EOF, which is psql's normal
   * end-of-input exit: it runs PQfinish, whose Terminate frame reaches the
   * splitter, closes the server's stdin, and lets the guest exit through its
   * shutdown checkpoint. Killing the server first instead would leave psql
   * holding a dead connection, and (for a reset) looking at a datadir that
   * changed under it.
   *
   * `stopping` blocks the respawn path, so a late frame from a dying psql
   * cannot resurrect a server instance we are trying to retire.
   *
   * Returns { psqlExited, serverExited } — the caller's evidence that BOTH
   * guests really finished rather than being abandoned at the timeout. An
   * abandoned instance is the leak: it stays suspended in fd_read, reachable
   * from a JSPI continuation, holding its whole linear memory.
   */
  async stop(timeoutMs = 15000) {
    this.stopping = true;
    const TIMEOUT = Symbol('timeout');
    const deadline = new Promise((r) => setTimeout(() => r(TIMEOUT), timeoutMs));
    if (this.o.psqlStdin) this.o.psqlStdin.close();
    const psqlExited = this.psqlExit
      ? (await Promise.race([this.psqlExit.catch(() => 0), deadline])) !== TIMEOUT
      : true;
    if (this.serverAlive && this.serverStdin) this.serverStdin.close();
    const serverExited = this.serverExit
      ? (await Promise.race([this.serverExit.catch(() => 0), deadline])) !== TIMEOUT
      : true;
    // Wake anything blocked on the server->psql pipe so no fd_read waiter is
    // left parked on a stream that will never be fed again.
    this.psqlRead.close();
    return { psqlExited, serverExited };
  }

  /** Wait for psql to finish, then let the last server wind down. */
  async wait() {
    if (!this.psqlExit) throw new Error('PsqlSession.wait() before start()');
    const code = await this.psqlExit;
    if (this.serverAlive && this.serverStdin) this.serverStdin.close();
    if (this.serverExit) { try { await this.serverExit; } catch { /* ignore */ } }
    return code;
  }
}
