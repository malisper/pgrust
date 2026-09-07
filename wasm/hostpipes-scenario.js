// hostpipes-scenario.js — the ONE two-session scenario both host-pipes drivers
// run, so the wasm arm proves the same property the native arm proved and not a
// weaker cousin of it.
//
// It came out of wasm/run-native-hostpipes.mjs verbatim. What it exercises has
// nothing to do with transport plumbing and everything to do with there being
// two REAL backends sharing one shared-memory postgres:
//
//   1. cross-session visibility — A creates + inserts, B sees the committed row
//   2. row-lock BLOCKING — B's UPDATE must NOT complete while A's transaction
//      holds the row, and must complete once A commits. Two backend THREADS
//      really blocking on each other through shared memory is the whole claim
//   3. lock_timeout — B gives up with SQLSTATE 55P03 in ~300ms, which needs a
//      timer running on a third thread while B is parked
//   3.5 cross-session NOTIFY — B notifies a channel an IDLE A is LISTENing on,
//      and the gap is timed over 20 rounds at swept cadences. That gap is the
//      transport's: it is how long A's park in `secure_read` takes to notice a
//      latch another backend set (0-100ms with the interrupt poll alone, a
//      notify with a wake fd)
//   4. distinct pg_backend_pid(), clean 'X' termination of both, then whatever
//      shutdown the host can offer
//
// The host supplies two things and nothing else:
//
//   openSession(name) -> {
//     name,
//     handshake,                       // the startup messages, for the Z check
//     send(sql)    -> Promise<{ msgs, elapsed }>,   // fires, does NOT await
//     query(sql)   -> Promise<{ msgs, elapsed }>,   // send + await
//     terminate()  -> void,                          // write 'X'
//     waitClosed(timeoutMs) -> Promise<boolean>,     // server closed our end?
//     onNotify?(cb)-> void,   // OPTIONAL: cb(msg, atMs) per NotificationResponse
//   }
//   shutdown() -> Promise<{ checks?: [{ ok, what }], notes?: [string] }>
//
// `send` settling at ReadyForQuery is load-bearing: step 2 measures a query
// that is deliberately still in flight.
//
// Native (OS pipes, a child process) and wasm (SharedArrayBuffer pipes, worker
// threads over one shared linear memory) differ only inside those two.

import { parseMessage } from './wire.js';

export const rows = (msgs) => msgs.filter((m) => m.t === 'D').map((m) => parseMessage('D', m.body).values);
export const errs = (msgs) => msgs.filter((m) => m.t === 'E').map((m) => parseMessage('E', m.body).fields);
export const tags = (msgs) => msgs.filter((m) => m.t === 'C').map((m) => parseMessage('C', m.body).tag);
export const summarize = (msgs) => msgs.map((m) => m.t).join('');

/** Rounds in the notify-latency step (3.5). Ten of them cover one poll period. */
const NOTIFY_ROUNDS = 20;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** NotificationResponse body: pid i32, then channel and payload as C strings. */
export function notifyPayload(body) {
  const s = new TextDecoder().decode(body.subarray(4));
  return s.split('\0')[1] || '';
}

/** min / median / p95 / max / mean, each to one decimal, over a ms sample. */
export function stats(xs) {
  if (xs.length === 0) return { min: 0, median: 0, p95: 0, max: 0, mean: 0 };
  const v = [...xs].sort((a, b) => a - b);
  const at = (q) => v[Math.min(v.length - 1, Math.floor(q * v.length))];
  const r = (x) => Number(x.toFixed(1));
  return {
    min: r(v[0]),
    median: r(at(0.5)),
    p95: r(at(0.95)),
    max: r(v[v.length - 1]),
    mean: r(v.reduce((a, b) => a + b, 0) / v.length),
  };
}

/**
 * Resolves { done: true, value } or { done: false } — never rejects, and never
 * cancels `promise` (a blocked query is awaited again later).
 */
export function withTimeout(promise, timeoutMs) {
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

/**
 * Run the scenario. Returns { ok, failures, checks, timings }; every step is
 * wall-clock logged through `log`.
 */
export async function runHostPipesScenario({
  openSession,
  shutdown,
  log = () => {},
  // OPTIONAL, and only for measurement: hold both sessions idle for this long
  // before terminating them, sampling `cpu()` across the window. What a
  // BLOCKED backend costs when nothing is happening is the other half of the
  // wake-fd question — the interrupt poll buys its 100 ms bound with one wake
  // per session per period, and a park on a wake fd buys its notify with
  // none. `cpu()` returns { user, system } in MICROseconds over the whole
  // process (Node's `process.cpuUsage`), which on this host counts every
  // guest thread: they are worker threads of it.
  idleMs = 0,
  cpu = null,
}) {
  // `log` is the HOST's logger and owns the wall-clock stamp (both drivers
  // already print one, and two clocks in one transcript is a bug report
  // waiting to happen). Every line below carries its own step timing.
  const say = (s) => log(s);
  const failures = [];
  const checks = [];
  const timings = {};
  const check = (cond, what) => {
    checks.push({ ok: !!cond, what });
    if (cond) say(`  ok   ${what}`);
    else {
      say(`  FAIL ${what}`);
      failures.push(what);
    }
  };
  const done = () => ({ ok: failures.length === 0, failures, checks, timings });

  // --- the two sessions ---
  const tA = performance.now();
  const A = await openSession('A');
  timings.handshakeAMs = performance.now() - tA;
  say(`  A handshake ${timings.handshakeAMs.toFixed(0)}ms: ${summarize(A.handshake)}`);
  check(A.handshake.some((m) => m.t === 'Z'), 'A: ReadyForQuery after startup');

  const tB = performance.now();
  const B = await openSession('B');
  timings.handshakeBMs = performance.now() - tB;
  say(`  B handshake ${timings.handshakeBMs.toFixed(0)}ms: ${summarize(B.handshake)}`);
  check(B.handshake.some((m) => m.t === 'Z'), 'B: ReadyForQuery after startup');

  // ---- 1. cross-session visibility ----
  say('step 1: A creates + inserts, B reads');
  let t = performance.now();
  const create = await A.query('CREATE TABLE lock_t(id int primary key, v int)');
  say(`  A CREATE TABLE ${(performance.now() - t).toFixed(0)}ms: ${tags(create.msgs)}`);
  check(tags(create.msgs).includes('CREATE TABLE'), 'A: CREATE TABLE');
  t = performance.now();
  const insert = await A.query('INSERT INTO lock_t VALUES (1, 0)');
  say(`  A INSERT ${(performance.now() - t).toFixed(0)}ms: ${tags(insert.msgs)}`);
  check(tags(insert.msgs).includes('INSERT 0 1'), 'A: INSERT 0 1');
  t = performance.now();
  const count = await B.query('SELECT count(*) FROM lock_t');
  say(`  B SELECT count(*) ${(performance.now() - t).toFixed(0)}ms -> ${JSON.stringify(rows(count.msgs))}`);
  check(rows(count.msgs)[0]?.[0] === '1', "B sees A's committed row (count = 1)");

  // ---- 2. row lock: B must BLOCK on A ----
  say('step 2: A holds a row lock, B must block on it');
  check(tags((await A.query('BEGIN')).msgs).includes('BEGIN'), 'A: BEGIN');
  const upd1 = await A.query('UPDATE lock_t SET v = 1 WHERE id = 1');
  check(tags(upd1.msgs).includes('UPDATE 1'), 'A: UPDATE 1 (transaction stays open)');

  const bStart = performance.now();
  const bUpdate = B.send('UPDATE lock_t SET v = 2 WHERE id = 1');
  const early = await withTimeout(bUpdate, 500);
  say(`  B UPDATE still blocked after ${(performance.now() - bStart).toFixed(0)}ms: ${!early.done}`);
  check(!early.done, 'B: UPDATE does NOT complete while A holds the row lock (500ms)');

  t = performance.now();
  const commit = await A.query('COMMIT');
  say(`  A COMMIT ${(performance.now() - t).toFixed(0)}ms: ${tags(commit.msgs)}`);
  check(tags(commit.msgs).includes('COMMIT'), 'A: COMMIT');

  const released = await withTimeout(bUpdate, 1000);
  const bWait = performance.now() - bStart;
  timings.bWaitMs = bWait;
  say(`  B UPDATE completed ${bWait.toFixed(0)}ms after it was sent: ${released.done ? tags(released.value.msgs) : 'TIMED OUT'}`);
  check(released.done, "B: UPDATE completes within 1s of A's COMMIT");
  check(released.done && tags(released.value.msgs).includes('UPDATE 1'), 'B: UPDATE 1');
  check(bWait >= 500, `B's wall time >= 500ms (measured ${bWait.toFixed(0)}ms)`);
  if (!released.done) return done(); // B is wedged; nothing after this is meaningful

  t = performance.now();
  const v2 = await B.query('SELECT v FROM lock_t WHERE id = 1');
  say(`  B SELECT v ${(performance.now() - t).toFixed(0)}ms -> ${JSON.stringify(rows(v2.msgs))}`);
  check(rows(v2.msgs)[0]?.[0] === '2', "B's update won: v = 2");

  // ---- 3. lock_timeout ----
  say('step 3: lock_timeout gives up with 55P03');
  check(tags((await A.query('BEGIN')).msgs).includes('BEGIN'), 'A: BEGIN (2nd)');
  check(
    tags((await A.query('UPDATE lock_t SET v = 3 WHERE id = 1')).msgs).includes('UPDATE 1'),
    'A: UPDATE 1 (holding again)',
  );
  check(tags((await B.query("SET lock_timeout = '300ms'")).msgs).includes('SET'), 'B: SET lock_timeout');

  const ltStart = performance.now();
  const timedOut = await B.query('UPDATE lock_t SET v = 4 WHERE id = 1');
  const ltMs = performance.now() - ltStart;
  timings.lockTimeoutMs = ltMs;
  const fields = errs(timedOut.msgs)[0] || {};
  say(`  B UPDATE errored after ${ltMs.toFixed(0)}ms: ${fields.C} ${fields.M}`);
  check(fields.C === '55P03', `B: SQLSTATE 55P03 (got ${fields.C})`);
  check(ltMs >= 250 && ltMs <= 900, `B: lock_timeout fired in ~300ms (measured ${ltMs.toFixed(0)}ms)`);

  check(tags((await A.query('ROLLBACK')).msgs).includes('ROLLBACK'), 'A: ROLLBACK');
  check(tags((await B.query('RESET lock_timeout')).msgs).includes('RESET'), 'B: RESET lock_timeout');
  const vAfter = await B.query('SELECT v FROM lock_t WHERE id = 1');
  say(`  B SELECT v -> ${JSON.stringify(rows(vAfter.msgs))}`);
  check(rows(vAfter.msgs)[0]?.[0] === '2', 'v is still 2 after the rolled-back UPDATE');

  // ---- 3.5 cross-session NOTIFY to an IDLE listener ----
  //
  // The one property the transport itself decides. A `NOTIFY` from B sets
  // A's latch; A is parked in `secure_read` on its session pipe, and how
  // long the payload takes to come out the other side is exactly how long
  // that park takes to notice a latch. With the interrupt poll alone it is
  // uniform over [0, 100) ms; with a wake fd in the connection record (or a
  // native backend's own `pipe(2)`) the park ends on the SetLatch itself.
  //
  // Skipped when the host's session objects cannot report an unsolicited
  // message — the scenario contract's `onNotify` is optional.
  if (typeof A.onNotify === 'function') {
    say('step 3.5: NOTIFY from B reaches an IDLE listening A');
    check(tags((await A.query('LISTEN probe')).msgs).includes('LISTEN'), 'A: LISTEN probe');
    let pending = null;
    A.onNotify((m, at) => {
      if (pending) {
        const p = pending;
        pending = null;
        p({ payload: notifyPayload(m.body), at });
      }
    });
    const gaps = [];
    let lost = 0;
    for (let i = 0; i < NOTIFY_ROUNDS; i += 1) {
      const arrival = new Promise((r) => {
        pending = r;
      });
      // B's statement RESOLVING is t0, so what is measured is the gap a
      // second session introduces and nothing of B's own work.
      await B.query(`NOTIFY probe, 'p${i}'`);
      const t0 = performance.now();
      const got = await withTimeout(arrival, 5000);
      if (!got.done) {
        lost += 1;
        pending = null;
        continue;
      }
      gaps.push(Math.max(0, got.value.at - t0));
      // Sweep the gap between rounds across a whole 100 ms poll period: a
      // FIXED cadence phase-locks to the poll and samples one point of the
      // distribution fifty times over.
      await sleep(20 + 10 * (i % 10));
    }
    const st = stats(gaps);
    timings.notify = { rounds: NOTIFY_ROUNDS, delivered: gaps.length, lost, ...st };
    say(
      `  NOTIFY -> idle listener over ${gaps.length}/${NOTIFY_ROUNDS} rounds: ` +
        `min ${st.min}ms median ${st.median}ms p95 ${st.p95}ms max ${st.max}ms mean ${st.mean}ms`,
    );
    check(lost === 0, `every NOTIFY reached the idle listener (${lost} lost)`);
    check(tags((await A.query('UNLISTEN probe')).msgs).includes('UNLISTEN'), 'A: UNLISTEN probe');
  }

  // ---- 3.6 what two IDLE backends cost (opt-in) ----
  if (idleMs > 0) {
    say(`step 3.6: both sessions idle for ${idleMs}ms`);
    const before = cpu ? cpu() : null;
    const w0 = performance.now();
    await sleep(idleMs);
    const wall = performance.now() - w0;
    if (before) {
      const after = cpu();
      const usedMs = (after.user - before.user + after.system - before.system) / 1000;
      timings.idle = {
        wallMs: Number(wall.toFixed(0)),
        cpuMs: Number(usedMs.toFixed(1)),
        cpuPct: Number(((usedMs / wall) * 100).toFixed(2)),
      };
      say(
        `  idle ${wall.toFixed(0)}ms cost ${usedMs.toFixed(1)}ms of CPU across every thread ` +
          `(${timings.idle.cpuPct}% of one core)`,
      );
    }
  }

  // ---- 4. distinct backends, clean termination, shutdown ----
  say('step 4: distinct backend pids, clean termination, shutdown');
  const pidA = rows((await A.query('SELECT pg_backend_pid()')).msgs)[0]?.[0];
  const pidB = rows((await B.query('SELECT pg_backend_pid()')).msgs)[0]?.[0];
  say(`  pg_backend_pid: A=${pidA} B=${pidB}`);
  check(!!pidA && !!pidB && pidA !== pidB, `two distinct backends (A=${pidA}, B=${pidB})`);
  timings.pidA = pidA;
  timings.pidB = pidB;

  A.terminate();
  B.terminate();
  const tClose = performance.now();
  const closedA = await A.waitClosed(5000);
  const closedB = await B.waitClosed(5000);
  say(`  session fds closed after ${(performance.now() - tClose).toFixed(0)}ms (A=${closedA} B=${closedB})`);
  check(closedA, 'A: server closed the session fds after Terminate (EOF)');
  check(closedB, 'B: server closed the session fds after Terminate (EOF)');

  const shutStart = performance.now();
  const result = (await shutdown()) || {};
  timings.shutdownMs = performance.now() - shutStart;
  say(`  shutdown returned after ${timings.shutdownMs.toFixed(0)}ms`);
  for (const n of result.notes || []) say(`  note ${n}`);
  for (const c of result.checks || []) check(c.ok, c.what);

  return done();
}
