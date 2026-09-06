#!/usr/bin/env node
// run-native-hostpipes.mjs — the NATIVE proof of the host-pipes transport
// (`postgres --host-pipes`): a real postmaster with N backend threads, serving
// concurrent pgwire sessions over nothing but file descriptors its host handed
// it. No socket(), no bind(), no listen(), no accept(), no unix socket file —
// exactly the surface wasm32-wasip1-threads can offer, proven here with OS
// descriptors so the later wasm host only has to honour an fd contract.
//
// The contract (crates/backend/libpq/pqcomm_hostpipes):
//   * PGRUST_HOSTPIPES_LISTEN_FD names the listener fd. The server does not
//     bind it; it only READS it.
//   * One 16-byte little-endian record per new connection, written by the
//     host to that fd:
//         u32 magic 0x50475048 ("HPGP")  |  i32 in_fd  |  i32 out_fd  |  u32 reserved
//     `in_fd` is where the server reads client->server bytes, `out_fd` where
//     it writes server->client bytes.
//   * The server closes both session fds when the session ends.
//
// Here the descriptors come from `child_process.spawn`'s stdio array: index N
// of the array is fd N in the child, so fd 3 is the listener and 4/5, 6/7 are
// the two sessions' (in, out) pairs. NOTE the direction convention: an fd the
// CHILD reads from is one this driver WRITES to. (Node/libuv backs an stdio
// entry above 2 with a socketpair end rather than a pipe; the server never
// asks what kind of fd it is — read/write/poll is the whole contract, which is
// precisely what makes a SharedArrayBuffer pipe a legal backing on wasm.)
//
// The SCENARIO is not here: it is wasm/hostpipes-scenario.js, so that the wasm
// arm (run-node-wire-threads.mjs --dispatch postmaster) runs the SAME two
// sessions over SharedArrayBuffer pipes and worker threads. This file supplies
// only the two host-specific halves — `openSession` over spawn()'s stdio fds,
// and a `shutdown` that is a real SIGINT to a real process.
//
// Usage:
//   node wasm/run-native-hostpipes.mjs [--fresh] [--scratch DIR] [--datadir DIR]
//                                      [--bin PATH] [--pgbin DIR]

import { spawn, spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { encodeQuery, encodeStartup, TERMINATE, WireReader } from './wire.js';
import { runHostPipesScenario } from './hostpipes-scenario.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');

const args = process.argv.slice(2);
const opt = (name, dflt) => {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] ? args[i + 1] : dflt;
};
const FRESH = args.includes('--fresh');
// Repo-local scratch by default (datadir + server log live together);
// --scratch relocates it, e.g. when the checkout is a linked worktree whose
// target/ is shared with the main one.
const SCRATCH = path.resolve(opt('--scratch', path.join(ROOT, 'tmp/agents/hostpipes')));
const DATADIR = path.resolve(opt('--datadir', path.join(SCRATCH, 'datadir')));
const BIN = path.resolve(opt('--bin', path.join(ROOT, 'target/debug/postgres')));
const PGBIN = opt('--pgbin', '/usr/lib/postgresql/18/bin');
const LOGFILE = path.resolve(opt('--logfile', path.join(SCRATCH, 'postmaster.log')));

const CONN_MAGIC = 0x50475048;

const failures = [];
const T0 = performance.now();
const ms = () => (performance.now() - T0).toFixed(0).padStart(6);
const log = (s) => console.log(`[${ms()}ms] ${s}`);
const check = (cond, what) => {
  if (cond) log(`  ok   ${what}`);
  else {
    log(`  FAIL ${what}`);
    failures.push(what);
  }
};

// ---- datadir ----------------------------------------------------------------

fs.mkdirSync(SCRATCH, { recursive: true });
if (FRESH && fs.existsSync(DATADIR)) fs.rmSync(DATADIR, { recursive: true, force: true });
if (!fs.existsSync(path.join(DATADIR, 'PG_VERSION'))) {
  log(`initdb ${DATADIR}`);
  const r = spawnSync(
    path.join(PGBIN, 'initdb'),
    ['-D', DATADIR, '--no-locale', '--encoding=UTF8', '-U', 'postgres', '-A', 'trust'],
    { encoding: 'utf8' },
  );
  if (r.status !== 0) {
    console.error(r.stdout, r.stderr);
    console.log('VERDICT: hostpipes-native FAIL (initdb)');
    process.exit(1);
  }
} else {
  log(`reusing datadir ${DATADIR}`);
}

// ---- the server -------------------------------------------------------------

const GUCS = [
  '-c', 'max_stack_depth=60000',
  '-c', 'io_method=sync',
  '-c', 'autovacuum=off',
  '-c', 'wal_sync_method=fdatasync',
  '-c', 'shared_buffers=32MB',
  // The whole point: no TCP listener, no unix socket directory. The ONLY way
  // in is the host-provided listener fd.
  '-c', 'listen_addresses=',
  '-c', 'unix_socket_directories=',
  '-c', 'log_checkpoints=on',
];
const ARGV = ['--host-pipes', '-D', DATADIR, ...GUCS];

// ulimit -s 65520: dev-profile frames are large and max_stack_depth=60000kB
// must fit under the thread stack (the previous native runs use the same).
// `exec` keeps the pid ours, so SIGINT below reaches the postmaster itself.
const child = spawn(
  'bash',
  ['-c', 'ulimit -s 65520; exec "$@"', 'bash', BIN, ...ARGV],
  {
    stdio: [
      'ignore', // 0
      'pipe',   // 1 stdout
      'pipe',   // 2 stderr (the server log)
      'pipe',   // 3 LISTENER  (driver writes connection records)
      'pipe',   // 4 session A in   (server reads  <- driver writes)
      'pipe',   // 5 session A out  (server writes -> driver reads)
      'pipe',   // 6 session B in
      'pipe',   // 7 session B out
    ],
    env: {
      ...process.env,
      PGRUST_HOSTPIPES_LISTEN_FD: '3',
      PGRUST_TZDIR: '/usr/share/zoneinfo',
      PGRUST_PGSHAREDIR: '/usr/share/postgresql/18',
    },
  },
);

let serverLog = '';
const logStream = fs.createWriteStream(LOGFILE);
for (const s of [child.stdout, child.stderr]) {
  s.setEncoding('utf8');
  s.on('data', (d) => {
    serverLog += d;
    logStream.write(d);
  });
}
let exitCode = null;
let exitSignal = null;
child.on('exit', (code, signal) => {
  exitCode = code;
  exitSignal = signal;
  log(`postmaster exited code=${code} signal=${signal}`);
});

const sleep = (n) => new Promise((r) => setTimeout(r, n));

async function waitForLog(re, timeoutMs) {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (re.test(serverLog)) return true;
    if (exitCode !== null) return false;
    await sleep(20);
  }
  return false;
}

// ---- the host side of the contract -----------------------------------------

function announceConnection(inFd, outFd) {
  const rec = Buffer.alloc(16);
  rec.writeUInt32LE(CONN_MAGIC, 0);
  rec.writeInt32LE(inFd, 4);
  rec.writeInt32LE(outFd, 8);
  rec.writeUInt32LE(0, 12);
  child.stdio[3].write(rec);
}

class Session {
  constructor(name, wr, rd) {
    this.name = name;
    this.wr = wr;
    this.rd = rd;
    this.reader = new WireReader();
    this.collector = null;
    this.closed = false;
    rd.on('data', (b) => this._feed(b));
    rd.on('end', () => {
      this.closed = true;
    });
  }

  _feed(buf) {
    this.reader.feed(new Uint8Array(buf));
    for (;;) {
      const m = this.reader.next();
      if (!m) break;
      if (!this.collector) continue; // unsolicited (NoticeResponse etc.)
      this.collector.msgs.push(m);
      if (m.t === 'Z') {
        const c = this.collector;
        this.collector = null;
        c.resolve({ msgs: c.msgs, elapsed: performance.now() - c.started });
      }
    }
  }

  _collect() {
    if (this.collector) throw new Error(`${this.name}: overlapping collection`);
    let resolve;
    const p = new Promise((r) => {
      resolve = r;
    });
    this.collector = { msgs: [], resolve, started: performance.now() };
    return p;
  }

  startup(params) {
    const p = this._collect();
    this.wr.write(Buffer.from(encodeStartup(params)));
    return p;
  }

  // Fire a simple query WITHOUT waiting: the returned promise settles at the
  // ReadyForQuery, which for a blocked statement is the whole point.
  send(sql) {
    const p = this._collect();
    this.wr.write(Buffer.from(encodeQuery(sql)));
    return p;
  }

  async query(sql) {
    const r = await this.send(sql);
    return r;
  }

  terminate() {
    this.wr.write(Buffer.from(TERMINATE));
  }
}

// ---- the run ----------------------------------------------------------------

// The scenario's session factory, native flavour: fds 4/5 for A and 6/7 for B,
// announced on the listener (fd 3) as a 16-byte HPGP record.
let nextSession = 0;
async function openSession(name) {
  const k = nextSession++;
  const inFd = 4 + 2 * k;
  const outFd = 5 + 2 * k;
  log(`announcing session ${name} on (in=${inFd}, out=${outFd})`);
  announceConnection(inFd, outFd);
  const s = new Session(name, child.stdio[inFd], child.stdio[outFd]);
  const hello = await s.startup({
    user: 'postgres',
    database: 'postgres',
    application_name: `hostpipes-${name}`,
  });
  return {
    name,
    handshake: hello.msgs,
    send: (sql) => s.send(sql),
    query: (sql) => s.query(sql),
    terminate: () => s.terminate(),
    waitClosed: (timeoutMs) => waitForClose(s, timeoutMs),
  };
}

// Fast shutdown, native flavour: an actual SIGINT to an actual pid.
async function shutdown() {
  const shutStart = performance.now();
  child.kill('SIGINT');
  const gone = await waitForExit(30000);
  log(`  postmaster stopped in ${(performance.now() - shutStart).toFixed(0)}ms`);
  const ckpt = /checkpoint starting: shutdown/i.test(serverLog) ||
    /checkpoint complete/i.test(serverLog);
  return {
    notes: [`SIGINT -> exit code ${exitCode}${exitSignal ? `/${exitSignal}` : ''}`],
    checks: [
      { ok: gone, what: 'postmaster exited after SIGINT' },
      { ok: exitCode === 0, what: `exit code 0 (got ${exitCode}${exitSignal ? `/${exitSignal}` : ''})` },
      { ok: ckpt, what: 'log shows the shutdown checkpoint' },
      {
        ok: /database system is shut down/i.test(serverLog),
        what: 'log shows "database system is shut down"',
      },
    ],
  };
}

async function main() {
  log(`spawned ${BIN} --host-pipes (pid ${child.pid}); listener = fd 3`);

  const ready = await waitForLog(/database system is ready to accept connections/, 120000);
  check(ready, 'postmaster reached "ready to accept connections" with NO listen socket');
  if (!ready) return;

  const result = await runHostPipesScenario({ openSession, shutdown, log });
  for (const c of result.checks) {
    if (!c.ok) failures.push(c.what);
  }
}

function waitForClose(session, timeoutMs) {
  return new Promise((resolve) => {
    if (session.closed) return resolve(true);
    const timer = setTimeout(() => resolve(session.closed), timeoutMs);
    session.rd.on('end', () => {
      clearTimeout(timer);
      resolve(true);
    });
  });
}

function waitForExit(timeoutMs) {
  return new Promise((resolve) => {
    if (exitCode !== null) return resolve(true);
    const timer = setTimeout(() => resolve(exitCode !== null), timeoutMs);
    child.on('exit', () => {
      clearTimeout(timer);
      resolve(true);
    });
  });
}

try {
  await main();
} catch (e) {
  failures.push(`exception: ${e && e.stack ? e.stack : e}`);
  log(`EXCEPTION ${e && e.stack ? e.stack : e}`);
}

if (exitCode === null) {
  try {
    child.kill('SIGKILL');
  } catch {}
}
logStream.end();
log(`server log: ${LOGFILE}`);
if (failures.length === 0) {
  console.log('VERDICT: hostpipes-native PASS');
  process.exit(0);
}
console.log(`VERDICT: hostpipes-native FAIL ${failures.join(' | ')}`);
process.exit(1);
