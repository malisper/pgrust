// fanout-scenario.js — the READ FAN-OUT scenario: N sessions, each running the
// same number of point SELECTs by primary key, all at once, timed as one wall.
//
// Why it exists beside hostpipes-scenario.js. That one proves the postmaster is
// a real server: cross-session visibility, row locks, lock_timeout, NOTIFY.
// This one asks the other question — what N REAL backends cost per statement
// when they are all busy at the same time — because that is the shape a browser
// benchmark reported as pathological on one JavaScript engine and fine on
// another, and a host-side scenario that runs under BOTH `node` and `bun` (which
// embeds JavaScriptCore) is the cheapest way to tell an engine problem from a
// browser problem. Nothing here is browser-specific; nothing here is
// node-specific.
//
// It takes the same two things from its host that the other scenario does, and
// nothing else:
//
//   openSession(name) -> { name, handshake, query(sql) -> Promise<{ msgs }>,
//                          terminate(), waitClosed(ms) }
//   shutdown() -> Promise<{ checks?, notes? }>
//
// so any driver that can run one can run the other.
//
// SHAPE. One untimed setup on the first session builds `rows` indexed rows with
// a 100-byte payload — the dataset of the browser suite's Concurrency Test 1,
// spelled the same way. Then every client session runs `statements` point
// SELECTs, drawn from one deterministic generator so that two runtimes execute
// the SAME keys in the SAME order: the only difference between a `node` run and
// a `bun` run is the engine underneath.
//
// WHAT IT REPORTS. The wall of the concurrent phase, the per-statement mean and
// p95 per client, and the derived per-statement cost `wall / statements` — which
// at one client is the serial round-trip cost and at N clients is what N
// backends did to each other. A run whose wall grows LINEARLY with N has found
// per-statement cost; one whose wall grows faster than N has found contention.

import { parseMessage } from './wire.js';

/** Rows the untimed setup builds — the browser suite's Concurrency dataset. */
export const DEFAULT_ROWS = 100_000;
/** Point SELECTs per client — the browser suite's Test 1. */
export const DEFAULT_STATEMENTS = 500;

/** min / median / p95 / max / mean, each to three decimals, over a ms sample. */
export function stats(xs) {
  if (xs.length === 0) return { min: 0, median: 0, p95: 0, max: 0, mean: 0 };
  const v = [...xs].sort((a, b) => a - b);
  const at = (q) => v[Math.min(v.length - 1, Math.floor(q * v.length))];
  const r = (x) => Number(x.toFixed(3));
  return {
    min: r(v[0]),
    median: r(at(0.5)),
    p95: r(at(0.95)),
    max: r(v[v.length - 1]),
    mean: r(v.reduce((a, b) => a + b, 0) / v.length),
  };
}

/**
 * The key generator, seeded per client.
 *
 * A 32-bit xorshift rather than `Math.random`, so the statement a client runs is
 * a function of (seed, index) alone: two runtimes, two hosts and two machines
 * all execute the identical sequence, which is what makes a wall comparable.
 */
export function keyStream(seed, rows) {
  let state = (seed * 2654435761) >>> 0 || 1;
  return () => {
    state ^= state << 13;
    state >>>= 0;
    state ^= state >>> 17;
    state ^= state << 5;
    state >>>= 0;
    return (state % rows) + 1;
  };
}

const errorsOf = (msgs) =>
  msgs.filter((m) => m.t === 'E').map((m) => {
    const f = parseMessage('E', m.body).fields;
    return `${f.C} ${f.M}`;
  });

const rowCount = (msgs) => msgs.filter((m) => m.t === 'D').length;

/**
 * Run the fan-out. Returns { ok, failures, checks, timings }.
 *
 * `clients` sessions are opened up front and the setup runs on the first of
 * them, so the concurrent phase measures established backends rather than
 * accept latency.
 */
export async function runFanOutScenario({
  openSession,
  shutdown,
  log = () => {},
  clients = 4,
  statements = DEFAULT_STATEMENTS,
  rows = DEFAULT_ROWS,
}) {
  const say = (s) => log(s);
  const failures = [];
  const checks = [];
  const timings = {};
  const check = (cond, what) => {
    checks.push({ ok: !!cond, what });
    say(`  ${cond ? 'ok  ' : 'FAIL'} ${what}`);
  };

  say(`fan-out: ${clients} client(s) x ${statements} point SELECTs over ${rows} rows`);
  const sessions = [];
  for (let i = 0; i < clients; i++) sessions.push(await openSession(`c${i}`));
  check(
    sessions.every((s) => s.handshake.some((m) => m.t === 'Z')),
    `every one of the ${clients} sessions reached ReadyForQuery`,
  );

  // The untimed setup, on the first session. Identical to the browser suite's
  // Concurrency dataset: 100-byte payloads behind a primary key.
  const setupStarted = performance.now();
  for (const sql of [
    'DROP TABLE IF EXISTS fanout',
    'CREATE TABLE fanout (id integer PRIMARY KEY, payload text NOT NULL)',
    `INSERT INTO fanout (id, payload) SELECT g, repeat('x', 100) FROM generate_series(1, ${rows}) AS g`,
    'ANALYZE fanout',
  ]) {
    const r = await sessions[0].query(sql);
    const errs = errorsOf(r.msgs);
    if (errs.length > 0) failures.push(`setup ${JSON.stringify(sql.slice(0, 40))}: ${errs.join(' | ')}`);
  }
  timings.setupMs = Number((performance.now() - setupStarted).toFixed(1));
  say(`  setup: ${rows} rows in ${timings.setupMs}ms (untimed)`);

  // THE MEASUREMENT. Every client fires its own sequential stream on its own
  // session; the wall is from the first statement of any client to the last.
  const latencies = sessions.map(() => []);
  const wallStarted = performance.now();
  const runs = sessions.map(async (session, index) => {
    const nextKey = keyStream(index + 1, rows);
    let seen = 0;
    for (let i = 0; i < statements; i++) {
      const started = performance.now();
      const r = await session.query(`SELECT payload FROM fanout WHERE id = ${nextKey()}`);
      latencies[index].push(performance.now() - started);
      seen += rowCount(r.msgs);
      const errs = errorsOf(r.msgs);
      if (errs.length > 0) {
        failures.push(`client ${index}: ${errs[0]}`);
        break;
      }
    }
    return seen;
  });
  const seenRows = await Promise.all(runs);
  const wallMs = performance.now() - wallStarted;

  timings.wallMs = Number(wallMs.toFixed(1));
  timings.clients = clients;
  timings.statements = statements;
  timings.perStatementMs = Number((wallMs / statements).toFixed(3));
  timings.totalStatements = clients * statements;
  timings.statementsPerSecond = Number(((clients * statements * 1000) / wallMs).toFixed(0));
  timings.perClient = latencies.map((xs, i) => ({ client: i, ...stats(xs) }));
  check(
    seenRows.every((n) => n === statements),
    `every client read one row per statement (${seenRows.join(',')} of ${statements})`,
  );
  say(
    `  WALL ${timings.wallMs}ms for ${clients}x${statements} = ${timings.totalStatements} statements; ` +
      `${timings.statementsPerSecond} stmt/s; ${timings.perStatementMs}ms per statement per client`,
  );
  for (const c of timings.perClient) {
    say(`    client ${c.client}: mean ${c.mean}ms median ${c.median}ms p95 ${c.p95}ms max ${c.max}ms`);
  }

  for (const s of sessions) s.terminate();
  for (const s of sessions) await s.waitClosed(5000);

  const after = await shutdown();
  for (const c of after.checks || []) checks.push(c);
  const notes = after.notes || [];
  return { ok: failures.length === 0 && checks.every((c) => c.ok), failures, checks, notes, timings };
}
