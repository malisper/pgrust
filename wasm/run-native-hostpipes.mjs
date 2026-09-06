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
// Scenario (each step wall-clock logged, every assertion hard):
//   1. cross-session visibility: A creates + inserts, B sees the committed row
//   2. row-lock BLOCKING: B's UPDATE must not complete while A's transaction
//      holds the row, and must complete once A commits — proving two backend
//      THREADS really do block on each other through shared memory
//   3. lock_timeout: B gives up with SQLSTATE 55P03 in ~300ms
//   4. distinct backend pids, clean 'X' termination, SIGINT fast shutdown with
//      exit code 0 and a shutdown checkpoint in the log
//
// Usage:
//   node wasm/run-native-hostpipes.mjs [--fresh] [--scratch DIR] [--datadir DIR]
//                                      [--bin PATH] [--pgbin DIR]

import { spawn, spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { encodeQuery, encodeStartup, parseMessage, TERMINATE, WireReader } from './wire.js';

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

function withTimeout(promise, timeoutMs) {
  // Resolves { done: true, value } or { done: false } — never rejects, and
  // never cancels `promise` (a blocked query is awaited again later).
  let timer;
  return Promise.race([
    promise.then((value) => {
      clearTimeout(timer);
      return { done: true, value };
    }),
    new Promise((r) => {
      timer = setTimeout(() => r({ done: false }), timeoutMs);
    }),
  ]);
}

const rows = (msgs) => msgs.filter((m) => m.t === 'D').map((m) => parseMessage('D', m.body).values);
const errs = (msgs) => msgs.filter((m) => m.t === 'E').map((m) => parseMessage('E', m.body).fields);
const tags = (msgs) => msgs.filter((m) => m.t === 'C').map((m) => parseMessage('C', m.body).tag);
const summarize = (msgs) => msgs.map((m) => m.t).join('');

// ---- the run ----------------------------------------------------------------

async function main() {
  log(`spawned ${BIN} --host-pipes (pid ${child.pid}); listener = fd 3`);

  const ready = await waitForLog(/database system is ready to accept connections/, 120000);
  check(ready, 'postmaster reached "ready to accept connections" with NO listen socket');
  if (!ready) return;

  // --- session A ---
  log('announcing session A on (in=4, out=5)');
  const tA = performance.now();
  announceConnection(4, 5);
  const A = new Session('A', child.stdio[4], child.stdio[5]);
  const aHello = await A.startup({
    user: 'postgres',
    database: 'postgres',
    application_name: 'hostpipes-A',
  });
  log(`  A handshake ${(performance.now() - tA).toFixed(0)}ms: ${summarize(aHello.msgs)}`);
  check(aHello.msgs.some((m) => m.t === 'Z'), 'A: ReadyForQuery after startup');

  // --- session B ---
  log('announcing session B on (in=6, out=7)');
  const tB = performance.now();
  announceConnection(6, 7);
  const B = new Session('B', child.stdio[6], child.stdio[7]);
  const bHello = await B.startup({
    user: 'postgres',
    database: 'postgres',
    application_name: 'hostpipes-B',
  });
  log(`  B handshake ${(performance.now() - tB).toFixed(0)}ms: ${summarize(bHello.msgs)}`);
  check(bHello.msgs.some((m) => m.t === 'Z'), 'B: ReadyForQuery after startup');

  // ---- 1. cross-session visibility ----
  log('step 1: A creates + inserts, B reads');
  let t = performance.now();
  const create = await A.query('CREATE TABLE lock_t(id int primary key, v int)');
  log(`  A CREATE TABLE ${(performance.now() - t).toFixed(0)}ms: ${tags(create.msgs)}`);
  check(tags(create.msgs).includes('CREATE TABLE'), 'A: CREATE TABLE');
  t = performance.now();
  const insert = await A.query('INSERT INTO lock_t VALUES (1, 0)');
  log(`  A INSERT ${(performance.now() - t).toFixed(0)}ms: ${tags(insert.msgs)}`);
  check(tags(insert.msgs).includes('INSERT 0 1'), 'A: INSERT 0 1');
  t = performance.now();
  const count = await B.query('SELECT count(*) FROM lock_t');
  log(`  B SELECT count(*) ${(performance.now() - t).toFixed(0)}ms -> ${JSON.stringify(rows(count.msgs))}`);
  check(rows(count.msgs)[0]?.[0] === '1', "B sees A's committed row (count = 1)");

  // ---- 2. row lock: B must BLOCK on A ----
  log('step 2: A holds a row lock, B must block on it');
  check(tags((await A.query('BEGIN')).msgs).includes('BEGIN'), 'A: BEGIN');
  const upd1 = await A.query('UPDATE lock_t SET v = 1 WHERE id = 1');
  check(tags(upd1.msgs).includes('UPDATE 1'), 'A: UPDATE 1 (transaction stays open)');

  const bStart = performance.now();
  const bUpdate = B.send('UPDATE lock_t SET v = 2 WHERE id = 1');
  const early = await withTimeout(bUpdate, 500);
  log(`  B UPDATE still blocked after ${(performance.now() - bStart).toFixed(0)}ms: ${!early.done}`);
  check(!early.done, 'B: UPDATE does NOT complete while A holds the row lock (500ms)');

  t = performance.now();
  const commit = await A.query('COMMIT');
  log(`  A COMMIT ${(performance.now() - t).toFixed(0)}ms: ${tags(commit.msgs)}`);
  check(tags(commit.msgs).includes('COMMIT'), 'A: COMMIT');

  const released = await withTimeout(bUpdate, 1000);
  const bWait = performance.now() - bStart;
  log(`  B UPDATE completed ${bWait.toFixed(0)}ms after it was sent: ${released.done ? tags(released.value.msgs) : 'TIMED OUT'}`);
  check(released.done, "B: UPDATE completes within 1s of A's COMMIT");
  check(released.done && tags(released.value.msgs).includes('UPDATE 1'), 'B: UPDATE 1');
  check(bWait >= 500, `B's wall time >= 500ms (measured ${bWait.toFixed(0)}ms)`);

  t = performance.now();
  const v2 = await B.query('SELECT v FROM lock_t WHERE id = 1');
  log(`  B SELECT v ${(performance.now() - t).toFixed(0)}ms -> ${JSON.stringify(rows(v2.msgs))}`);
  check(rows(v2.msgs)[0]?.[0] === '2', "B's update won: v = 2");

  // ---- 3. lock_timeout ----
  log('step 3: lock_timeout gives up with 55P03');
  check(tags((await A.query('BEGIN')).msgs).includes('BEGIN'), 'A: BEGIN (2nd)');
  check(
    tags((await A.query('UPDATE lock_t SET v = 3 WHERE id = 1')).msgs).includes('UPDATE 1'),
    'A: UPDATE 1 (holding again)',
  );
  check(tags((await B.query("SET lock_timeout = '300ms'")).msgs).includes('SET'), 'B: SET lock_timeout');

  const ltStart = performance.now();
  const timedOut = await B.query('UPDATE lock_t SET v = 4 WHERE id = 1');
  const ltMs = performance.now() - ltStart;
  const fields = errs(timedOut.msgs)[0] || {};
  log(`  B UPDATE errored after ${ltMs.toFixed(0)}ms: ${fields.C} ${fields.M}`);
  check(fields.C === '55P03', `B: SQLSTATE 55P03 (got ${fields.C})`);
  check(ltMs >= 250 && ltMs <= 900, `B: lock_timeout fired in ~300ms (measured ${ltMs.toFixed(0)}ms)`);

  check(tags((await A.query('ROLLBACK')).msgs).includes('ROLLBACK'), 'A: ROLLBACK');
  check(tags((await B.query('RESET lock_timeout')).msgs).includes('RESET'), 'B: RESET lock_timeout');
  const vAfter = await B.query('SELECT v FROM lock_t WHERE id = 1');
  log(`  B SELECT v -> ${JSON.stringify(rows(vAfter.msgs))}`);
  check(rows(vAfter.msgs)[0]?.[0] === '2', 'v is still 2 after the rolled-back UPDATE');

  // ---- 4. distinct backends, clean shutdown ----
  log('step 4: distinct backend pids, clean termination, fast shutdown');
  const pidA = rows((await A.query('SELECT pg_backend_pid()')).msgs)[0]?.[0];
  const pidB = rows((await B.query('SELECT pg_backend_pid()')).msgs)[0]?.[0];
  log(`  pg_backend_pid: A=${pidA} B=${pidB}`);
  check(!!pidA && !!pidB && pidA !== pidB, `two distinct backends (A=${pidA}, B=${pidB})`);

  A.terminate();
  B.terminate();
  const closedA = await waitForClose(A, 5000);
  const closedB = await waitForClose(B, 5000);
  check(closedA, 'A: server closed the session fds after Terminate (EOF)');
  check(closedB, 'B: server closed the session fds after Terminate (EOF)');

  const shutStart = performance.now();
  child.kill('SIGINT'); // fast shutdown
  const gone = await waitForExit(30000);
  log(`  postmaster stopped in ${(performance.now() - shutStart).toFixed(0)}ms`);
  check(gone, 'postmaster exited after SIGINT');
  check(exitCode === 0, `exit code 0 (got ${exitCode}${exitSignal ? `/${exitSignal}` : ''})`);
  const ckpt = /checkpoint starting: shutdown/i.test(serverLog) ||
    /checkpoint complete/i.test(serverLog);
  check(ckpt, 'log shows the shutdown checkpoint');
  check(/database system is shut down/i.test(serverLog), 'log shows "database system is shut down"');
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
