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
export async function runHostPipesScenario({ openSession, shutdown, log = () => {} }) {
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
