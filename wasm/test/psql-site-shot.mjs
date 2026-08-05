#!/usr/bin/env node
// psql-site-shot.mjs — drive the REAL site page in headless Chrome over CDP:
// type into the page's own input with real keydown events (so the whole path
// repl.js -> backend.js -> worker.js -> psql.wasm/postgres.wasm is under test),
// click the page's own toolbar buttons, reload the page, and screenshot.
//
//   node tools/wasm-web/test/psql-site-shot.mjs [--port 8095] [--out FILE]
//                                              [--timeout 240] [--client psql|js]
//                                              [--nojspi]
//
// --client psql (default) drives the DEFAULT page (index.html, no query
// param) — the REAL Rust psql cross-piped to postgres --stdio-wire is the
// default client now. --client js drives index.html?client=js (the JS REPL
// opt-out, also the Safari/no-JSPI path) as an unregression check that the
// legacy client still runs, resets, and persists.
//
// --nojspi is the Safari-fallback leg: it deletes WebAssembly.Suspending/
// promising from the page world BEFORE any page script runs (a real
// feature-detect exercise, not UA sniffing), loads the DEFAULT URL, and
// asserts the page fell back to the JS REPL with the subtle fallback note.
//
// Phases (both clients):
//   1. banner/prompt + a statement battery
//   2. RESET: pristine datadir with NO page reload — a table created before the
//      reset is gone afterwards, and psql mode comes back with the REAL psql
//      banner (not a JS-synthesized one)
//   3. RESET AGAIN: the second consecutive reset must work exactly as the first
//      (a one-shot reset that wedges the second time is the classic failure)
//   4. PERSIST: toggle on, create a table, RELOAD the page, and the table is
//      still there (OPFS snapshot restore)
//
// Exits 0 when every check passed AND the screenshots were written; 1 on a
// content mismatch; 2 on setup/timeout trouble.
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const webRoot = path.join(here, '..');

function argAfter(flag, dflt) {
  const i = process.argv.indexOf(flag);
  return i !== -1 ? process.argv[i + 1] : dflt;
}
const port = Number(argAfter('--port', '8095'));
const cdpPort = port + 1;
const outFile = argAfter('--out', path.join(os.tmpdir(), 'psql-site.png'));
const timeoutMs = Number(argAfter('--timeout', '240')) * 1000;
const NOJSPI = process.argv.includes('--nojspi');
const client = NOJSPI ? 'nojspi' : argAfter('--client', 'psql');
if (client !== 'psql' && client !== 'js' && client !== 'nojspi') {
  console.error(`psql-site-shot: --client must be psql or js (got ${client})`);
  process.exit(2);
}
// psql is the site default: it gets the bare URL. The JS REPL is the opt-out.
// The nojspi leg also loads the bare URL — the FALLBACK must pick js.
const PSQL = client === 'psql';
const pageUrl = `http://localhost:${port}/${client === 'js' ? '?client=js' : ''}`;
// Screenshot siblings of --out, so one run leaves an evidence trail.
function shotPath(tag) {
  const ext = path.extname(outFile) || '.png';
  const base = outFile.slice(0, outFile.length - ext.length);
  return tag ? `${base}-${tag}${ext}` : outFile;
}

const CHROME = process.env.CHROME ||
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
if (!fs.existsSync(CHROME)) {
  console.error(`psql-site-shot: no Chrome at ${CHROME} (set CHROME=...)`);
  process.exit(2);
}

const children = [];
function cleanup() { for (const c of children) { try { c.kill('SIGKILL'); } catch { /* gone */ } } }
// Chrome can still be flushing its profile when we unlink it — a cleanup race
// must never turn a PASS into a nonzero exit.
function rmProfile() {
  try { fs.rmSync(profile, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 }); }
  catch { /* leftover temp dir; the OS reaps it */ }
}
process.on('exit', cleanup);
process.on('SIGINT', () => { cleanup(); process.exit(2); });

// --root DIR: serve a different tree (e.g. a staged deploy bundle) instead of
// the source tree. The bundle carries serve.mjs as _serve-for-testing.mjs
// (renamed so it can never be mistaken for site content).
const rootDir = argAfter('--root', webRoot);
const serveScript = ['_serve-for-testing.mjs', 'serve.mjs']
  .map((n) => path.join(rootDir, n)).find((p) => fs.existsSync(p));
if (!serveScript) {
  console.error(`psql-site-shot: no serve script under ${rootDir}`);
  process.exit(2);
}
const server = spawn(process.execPath, [serveScript, String(port)],
                     { stdio: ['ignore', 'ignore', 'pipe'] });
children.push(server);
await new Promise((r) => { server.stderr.once('data', r); setTimeout(r, 2000); });

const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'psql-shot-chrome-'));
const chrome = spawn(CHROME, [
  '--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check',
  '--disable-background-timer-throttling', '--disable-renderer-backgrounding',
  '--disable-backgrounding-occluded-windows',
  '--window-size=1400,900',
  `--remote-debugging-port=${cdpPort}`, `--user-data-dir=${profile}`,
  'about:blank',
], { stdio: ['ignore', 'ignore', 'pipe'] });
children.push(chrome);
let chromeErr = '';
chrome.stderr.on('data', (d) => { chromeErr += d; });

// ---- CDP plumbing ----------------------------------------------------------
let msgId = 0;
const pending = new Map();
let ws = null;
function send(method, params = {}) {
  return new Promise((resolve, reject) => {
    const id = ++msgId;
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ id, method, params }));
  });
}
async function evaluate(expression) {
  const r = await send('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
  if (r.exceptionDetails) throw new Error('page exception: ' + JSON.stringify(r.exceptionDetails).slice(0, 400));
  return r.result && r.result.value;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function connect() {
  const deadline = Date.now() + 30000;
  for (;;) {
    try {
      const list = await (await fetch(`http://127.0.0.1:${cdpPort}/json/list`)).json();
      const t = list.find((x) => x.type === 'page');
      if (t) {
        ws = new WebSocket(t.webSocketDebuggerUrl);
        await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });
        ws.onmessage = (ev) => {
          const m = JSON.parse(ev.data);
          if (m.id && pending.has(m.id)) {
            const { resolve, reject } = pending.get(m.id);
            pending.delete(m.id);
            m.error ? reject(new Error(m.error.message)) : resolve(m.result);
          } else if (m.method === 'Runtime.consoleAPICalled') {
            const txt = (m.params.args || []).map((a) => a.value ?? a.description ?? '').join(' ');
            if (process.env.VERBOSE) console.error('[console] ' + txt);
          }
        };
        return;
      }
    } catch { /* Chrome not up yet */ }
    if (Date.now() > deadline) throw new Error('no CDP page target (chrome stderr: ' + chromeErr.slice(0, 300) + ')');
    await sleep(200);
  }
}

// Terminal text as the page shows it (the scrollback plus the live prompt).
const TERM_TEXT = `(() => {
  const scroll = document.getElementById('scroll') || document.querySelector('.scroll') || document.body;
  return scroll.innerText;
})()`;

async function waitFor(pred, label, ms, pollMs = 250) {
  const deadline = Date.now() + ms;
  for (;;) {
    const text = await evaluate(TERM_TEXT);
    if (pred(text || '')) return text;
    if (Date.now() > deadline) {
      console.error(`--- terminal text at timeout (${label}) ---\n${(text || '').slice(-2000)}`);
      throw new Error(`timeout waiting for ${label}`);
    }
    await sleep(pollMs);
  }
}

// Type a line into the real input element and dispatch a real Enter keydown,
// so the whole page path (repl.js keydown handler) is exercised.
async function typeLine(line) {
  await evaluate(`(() => {
    const el = document.getElementById('input') || document.querySelector('input, textarea');
    el.focus();
    el.value = ${JSON.stringify(line)};
    el.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true, cancelable: true }));
    return true;
  })()`);
}

// Dispatch a real ArrowUp keydown to the input (the page's own history nav).
async function pressUp() {
  await evaluate(`(() => {
    const el = document.getElementById('input');
    el.focus();
    el.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowUp', bubbles: true, cancelable: true }));
    return true;
  })()`);
}

function inputValue() {
  return evaluate(`document.getElementById('input').value`);
}

async function clearInput() {
  // Walk history nav back to "past the end" the way a user would (ArrowDown),
  // then make sure the box is empty.
  await evaluate(`(() => {
    const el = document.getElementById('input');
    for (let i = 0; i < 50; i++) el.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true, cancelable: true }));
    el.value = '';
    return true;
  })()`);
}

// Drag-select across the scrollback with real mouse events (press, move,
// release — Chrome synthesizes the click), and return what is selected AFTER
// the whole sequence, i.e. after the page's click handler ran. The old
// refocus-on-click collapsed the selection here — the "text cannot be copied
// out of the terminal" bug.
async function dragSelectScrollback() {
  const box = await evaluate(`(() => {
    const lines = document.getElementById('lines');
    const r = lines.getBoundingClientRect();
    return { x: Math.ceil(r.left) + 2, y: Math.ceil(r.top) + 6 };
  })()`);
  const x2 = box.x + 320, y2 = box.y + 40;
  await send('Input.dispatchMouseEvent', { type: 'mousePressed', x: box.x, y: box.y, button: 'left', buttons: 1, clickCount: 1 });
  for (const f of [0.25, 0.5, 0.75, 1]) {
    await send('Input.dispatchMouseEvent', {
      type: 'mouseMoved', button: 'left', buttons: 1,
      x: box.x + (x2 - box.x) * f, y: box.y + (y2 - box.y) * f,
    });
  }
  await send('Input.dispatchMouseEvent', { type: 'mouseReleased', x: x2, y: y2, button: 'left', buttons: 0, clickCount: 1 });
  await sleep(200); // let the click handler run — the selection must SURVIVE it
  return evaluate('window.getSelection().toString()');
}

async function clickButton(id) {
  const ok = await evaluate(`(() => {
    const b = document.getElementById(${JSON.stringify(id)});
    if (!b || b.disabled) return false;
    b.click();
    return true;
  })()`);
  if (!ok) throw new Error(`button #${id} missing or disabled`);
}

function buttonLabel(id) {
  return evaluate(`(() => {
    const b = document.getElementById(${JSON.stringify(id)});
    return b ? b.textContent.trim() : null;
  })()`);
}

async function shoot(tag) {
  const p = shotPath(tag);
  const shot = await send('Page.captureScreenshot', { format: 'png', captureBeyondViewport: false });
  fs.writeFileSync(p, Buffer.from(shot.data, 'base64'));
  return p;
}

let failures = 0;
const shots = [];
function ok(msg) { console.log(`OK   ${msg}`); }
function bad(msg) { failures++; console.log(`FAIL ${msg}`); }
async function check(label, fn) {
  try { await fn(); ok(label); } catch (e) { bad(`${label} — ${e.message}`); }
}

async function waitLabel(id, want, ms = 60000) {
  const deadline = Date.now() + ms;
  for (;;) {
    const got = await buttonLabel(id);
    if (got === want) return;
    if (Date.now() > deadline) throw new Error(`#${id} reads ${JSON.stringify(got)}, wanted ${JSON.stringify(want)}`);
    await sleep(250);
  }
}

async function runLine(line, expect, label = line, ms = 60000) {
  await typeLine(line);
  await check(label, () => waitFor((t) => expect.test(t), `output of ${JSON.stringify(line)}`, ms));
}

// Wait until the terminal is a freshly (re)built session, then insist the reset
// stranded nothing: repl.js prints a warning when a retired psql/server guest
// had to be abandoned at the teardown timeout, which is the leaked-wasm-instance
// signature. The terminal is cleared per reset, so this must be checked per
// reset, not once at the end.
async function waitFreshSession(goneRe, label) {
  const text = await waitFreshBanner(goneRe, label);
  if (/did not exit within the teardown window/.test(text)) {
    throw new Error('the reset stranded a wasm instance (teardown timed out)');
  }
}

async function waitFreshBanner(goneRe, label) {
  if (PSQL) {
    // The REAL psql banner: "psql (18.3)" + the libpq-style help line. A JS
    // synthesized banner would carry neither.
    return waitFor((t) => /psql \(18\.3\)/.test(t) && /Type "help" for help\./.test(t) &&
                          /postgres=#/.test(t) && !goneRe.test(t), label, 120000);
  }
  return waitFor((t) => /pgrust \(PostgreSQL 18\.3 compatible\)/.test(t) &&
                        /Type \\\? for psql meta-commands/.test(t) &&
                        /pgrust=#/.test(t) && !goneRe.test(t), label, 120000);
}

// Does the DB still know this table? Both clients surface the backend's own
// error text for a missing relation.
function missingRelation(name) {
  return new RegExp(`relation "${name}" does not exist`);
}

try {
  await connect();
  await send('Runtime.enable');
  await send('Page.enable');
  if (NOJSPI) {
    // Remove JSPI from the page world before ANY page script runs, so the
    // site's real feature detection (wiresession.js jspiSupported) is what
    // routes us — exactly what a WebKit/Safari visitor exercises.
    await send('Page.addScriptToEvaluateOnNewDocument', {
      source: 'delete WebAssembly.Suspending; delete WebAssembly.promising;',
    });
  }
  await send('Page.navigate', { url: pageUrl });

  // ---- nojspi leg: the DEFAULT URL must fall back to the JS REPL -----------
  if (NOJSPI) {
    await waitFor((t) => /engine ready/.test(t) && /pgrust=#/.test(t),
                  'fallback JS REPL banner + prompt', timeoutMs);
    ok('default URL without JSPI booted the JS REPL (feature-detect fallback)');
    await check('the subtle fallback note appeared', () =>
      waitFor((t) => /this browser has no JSPI, so the JS REPL emulates psql here/.test(t),
              'fallback note', 15000));
    await check('the fallback did NOT try to boot psql.wasm', async () => {
      const t = (await evaluate(TERM_TEXT)) || '';
      if (/psql \(18\.3\)/.test(t)) throw new Error('real psql banner present — fallback failed to reroute');
    });
    await typeLine('SELECT 1 AS one;');
    await check('the fallback REPL answers queries', () =>
      waitFor((t) => /\bone\b[\s\S]*\(1 row\)/.test(t), 'SELECT 1 output', 60000));
    shots.push(await shoot());
    console.log(`\nscreenshots:\n  ${shots.join('\n  ')}`);
    cleanup();
    rmProfile();
    console.log(failures === 0
      ? 'VERDICT: psql-site-shot(nojspi-fallback) PASS'
      : `VERDICT: psql-site-shot(nojspi-fallback) FAIL (${failures})`);
    process.exit(failures === 0 ? 0 : 1);
  }

  // ---- phase 1: boot + battery ---------------------------------------------
  if (PSQL) {
    await waitFor((t) => /psql \(18\.3\)/.test(t) && /postgres=#/.test(t),
                  'psql banner + prompt', timeoutMs);
    ok('psql banner + prompt (real psql.wasm stdout)');
  } else {
    await waitFor((t) => /engine ready/.test(t) && /pgrust=#/.test(t),
                  'JS REPL banner + prompt', timeoutMs);
    ok('JS REPL banner + prompt');
  }

  const battery = PSQL ? [
    ['SELECT 1 AS one;', /\bone\b[\s\S]*\(1 row\)/],
    ['CREATE TABLE site_t(a int, b text);', /CREATE TABLE/],
    ["INSERT INTO site_t VALUES (1,'from-the-site'),(2,'second');", /INSERT 0 2/],
    ['SELECT * FROM site_t ORDER BY a;', /from-the-site[\s\S]*\(2 rows\)/],
    ['\\dt', /site_t/],
    ['\\d site_t', /Table "public\.site_t"/],
    ['SELECT * FROM nosuchtable;', /ERROR:  relation "nosuchtable" does not exist/],
    ['SELECT version();', /pgrust 0\.2 \(PostgreSQL 18\.3 compatible\) on wasm32-wasip1/],
  ] : [
    ['SELECT 1 AS one;', /\bone\b[\s\S]*\(1 row\)/],
    ['CREATE TABLE site_t(a int, b text);', /CREATE TABLE/],
    ["INSERT INTO site_t VALUES (1,'from-the-site'),(2,'second');", /INSERT 0 2/],
    ['SELECT * FROM site_t ORDER BY a;', /from-the-site[\s\S]*\(2 rows\)/],
    ['SELECT * FROM nosuchtable;', /ERROR:  relation "nosuchtable" does not exist/],
    ['SELECT version();', /pgrust 0\.2 \(PostgreSQL 18\.3 compatible\) on wasm32-wasip1/],
  ];
  for (const [line, expect] of battery) await runLine(line, expect);
  shots.push(await shoot('battery'));

  // ---- history grain ---------------------------------------------------------
  // psql mode: STATEMENT-grained, matching real psql readline (observed on
  // PGDG psql 18.4 over a PTY): a 3-line statement is ONE entry with newlines
  // preserved; Up-Up walks to the PREVIOUS STATEMENT, never a middle line.
  // js mode keeps its historical line-grained behavior.
  if (PSQL) {
    await typeLine('select 501,');
    await typeLine(' 502,');
    await typeLine(' 503;');
    await check('3-line statement executed', () =>
      waitFor((t) => /501[\s\S]*503[\s\S]*\(1 row\)/.test(t), '3-line result', 60000));
    await runLine('select 42 as sentinel;', /sentinel[\s\S]*42/);
    await pressUp();
    await check('Up recalls the last statement whole', async () => {
      const v = await inputValue();
      if (v !== 'select 42 as sentinel;') throw new Error(`got ${JSON.stringify(v)}`);
    });
    await pressUp();
    await check('second Up recalls the FULL 3-line statement (newlines preserved), not a middle line', async () => {
      const v = await inputValue();
      if (v !== 'select 501,\n 502,\n 503;') throw new Error(`got ${JSON.stringify(v)}`);
    });
    await clearInput();
  }

  if (PSQL) {
    // A sidebar EXAMPLE: multi-line SQL fed line-by-line, so this also
    // exercises psql's own continuation prompt and the queued-line pacing.
    await evaluate(`(() => {
      const el = [...document.querySelectorAll('#examples *')]
        .find((n) => n.textContent.trim() === 'Recursive CTE: Fibonacci');
      (el.closest('button') || el.closest('[role=button]') || el).click();
      return true;
    })()`);
    await check('sidebar example: Recursive CTE: Fibonacci', () =>
      waitFor((t) => /\bfib\b/.test(t) && /\b377\b/.test(t) && /\(15 rows\)/.test(t),
              'sidebar example output', 90000));
    const exText = await evaluate(TERM_TEXT);
    if (/-#/.test(exText)) ok("psql's own continuation prompt (…-#) appeared");
    else bad('no continuation prompt in the transcript');

    // The pasted multi-line example must land as ONE history entry.
    await pressUp();
    await check('sidebar example landed as ONE history entry', async () => {
      const v = await inputValue();
      if (!/^with recursive fib/.test(v) || !/select i, a as fib from fib;$/.test(v) || !v.includes('\n')) {
        throw new Error(`got ${JSON.stringify(v && v.slice(0, 80))}...`);
      }
    });
    await clearInput();
  }

  // ---- copy-out-of-terminal ---------------------------------------------------
  // Drag-select in the scrollback; the selection must survive the mouseup +
  // click (the click-to-refocus handler used to collapse it, making copy
  // impossible).
  {
    const selected = await dragSelectScrollback();
    await check('drag-selection in the scrollback survives the click (copyable)', async () => {
      if (!selected || selected.trim().length < 3) throw new Error(`selection is ${JSON.stringify(selected)}`);
    });
    // A plain click (no drag) must still refocus the input — the good UX half.
    // (A real click collapses the selection at mousedown; a synthetic .click()
    // does not, so collapse it explicitly to model the plain-click case.)
    await evaluate(`(() => {
      window.getSelection().removeAllRanges();
      document.getElementById('scroll').click();
      return true;
    })()`);
    await check('plain click still refocuses the input', async () => {
      const focused = await evaluate(`document.activeElement && document.activeElement.id`);
      if (focused !== 'input') throw new Error(`focus is on ${JSON.stringify(focused)}`);
    });
  }

  // ---- paste ------------------------------------------------------------------
  // Real-psql paste spec (observed over a PTY): complete lines execute as they
  // arrive, a trailing unterminated line stays in the editing buffer. And a
  // paste must work even when the input is NOT focused (the state right after
  // copying text out of the scrollback) — that was the "paste does not work"
  // bug: no document-level routing existed.
  {
    // a) UNFOCUSED multi-line paste with a trailing partial line
    await evaluate(`(() => {
      document.getElementById('input').blur();
      const dt = new DataTransfer();
      dt.setData('text/plain', 'select 701,\\n 702;\\nselect 703');
      document.body.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true }));
      return true;
    })()`);
    await check('unfocused paste: complete statement executed', () =>
      waitFor((t) => /701[\s\S]*702[\s\S]*\(1 row\)/.test(t), 'pasted statement result', 60000));
    await check('unfocused paste: trailing partial line stays in the input, unexecuted', async () => {
      const v = await inputValue();
      if (v !== 'select 703') throw new Error(`input holds ${JSON.stringify(v)}`);
      const t = (await evaluate(TERM_TEXT)) || '';
      if (/\b703\b[\s\S]*\(1 row\)/.test(t)) throw new Error('the partial line executed');
    });
    await check('unfocused paste: input took focus (terminal-emulator routing)', async () => {
      const f = await evaluate(`document.activeElement && document.activeElement.id`);
      if (f !== 'input') throw new Error(`focus on ${JSON.stringify(f)}`);
    });
    await evaluate(`(() => { const el = document.getElementById('input'); el.value = ''; return true; })()`);
    if (PSQL) {
      await pressUp();
      await check('pasted statement is ONE history entry (newlines preserved)', async () => {
        const v = await inputValue();
        if (v !== 'select 701,\n 702;') throw new Error(`got ${JSON.stringify(v)}`);
      });
      await clearInput();
    }
    // b) FOCUSED multi-line paste ending in a newline: everything executes
    await evaluate(`(() => {
      const el = document.getElementById('input');
      el.focus();
      const dt = new DataTransfer();
      dt.setData('text/plain', 'select 42*10 as pasted_focused;\\n');
      el.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true }));
      return true;
    })()`);
    await check('focused multi-line paste executed in full', () =>
      waitFor((t) => /pasted_focused[\s\S]*420/.test(t), 'focused paste result', 60000));
    await check('focused paste left an empty input', async () => {
      const v = await inputValue();
      if (v !== '') throw new Error(`input holds ${JSON.stringify(v)}`);
    });
    // c) paste into ANOTHER text field keeps native handling (not rerouted)
    await check('paste aimed at a different text field is not hijacked', async () => {
      const r = await evaluate(`(() => {
        const el = document.getElementById('updates-email');
        if (!el) return { skip: true };
        const dt = new DataTransfer();
        dt.setData('text/plain', 'user@example.com');
        const ev = new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true });
        el.dispatchEvent(ev);
        return { prevented: ev.defaultPrevented, terminal: document.getElementById('input').value };
      })()`);
      if (r.skip) return;
      if (r.prevented) throw new Error('terminal handler stole the email field paste');
      if (r.terminal.includes('example.com')) throw new Error('email paste leaked into the terminal input');
    });
  }

  // ---- phase 2: reset gives a PRISTINE datadir, in place -------------------
  // site_t exists right now (the battery created and read it back).
  await clickButton('btn-reset');
  await check('reset rebuilt the session in place (fresh banner, scrollback cleared)',
              () => waitFreshSession(/from-the-site/, 'post-reset fresh session'));
  await runLine('SELECT * FROM site_t;', missingRelation('site_t'),
                'reset #1 produced a pristine DB (site_t is gone)');
  // …and the fresh datadir is a WORKING one, not a husk.
  await runLine('CREATE TABLE after_reset(a int);', /CREATE TABLE/,
                'the post-reset datadir accepts new DDL');
  shots.push(await shoot('reset1'));

  // ---- phase 3: reset TWICE in a row --------------------------------------
  await clickButton('btn-reset');
  await check('second consecutive reset rebuilt the session',
              () => waitFreshSession(/after_reset/, 'post-reset-2 fresh session'));
  await runLine('SELECT * FROM after_reset;', missingRelation('after_reset'),
                'reset #2 produced a pristine DB (after_reset is gone)');
  await runLine('SELECT 42 AS still_alive;', /still_alive[\s\S]*42/,
                'the session still answers queries after two resets');
  shots.push(await shoot('reset2'));

  // …and keep going: resets 3 and 4, each still landing on a live session.
  for (const n of [3, 4]) {
    await runLine(`CREATE TABLE loop_t${n}(a int);`, /CREATE TABLE/, `pre-reset-${n} DDL`);
    await clickButton('btn-reset');
    await check(`reset #${n} rebuilt the session`,
                () => waitFreshSession(new RegExp(`loop_t${n}\\b`), `post-reset-${n} fresh session`));
    await runLine(`SELECT * FROM loop_t${n};`, missingRelation(`loop_t${n}`),
                  `reset #${n} produced a pristine DB`);
  }

  ok('no reset stranded a psql/server instance (checked after each reset)');

  // ---- phase 4: persist across a REAL page reload -------------------------
  const persistLabel = await buttonLabel('btn-persist');
  if (persistLabel === 'persist: n/a') {
    bad(`persist is unavailable in this ${client} client (button reads "persist: n/a") — no OPFS?`);
  } else {
    await clickButton('btn-persist');
    await check('persist toggled on', () => waitLabel('btn-persist', 'persist: on'));
    await runLine('CREATE TABLE keeper(a int, b text);', /CREATE TABLE/);
    // Reload IMMEDIATELY after the write lands — no waiting for the "snapshot
    // saved" note, and a tight 20ms poll so the reload chases the output as
    // hard as CDP can. This is the loss-window check: snapshots are
    // leading-edge (queued the moment the server reports idle, i.e. BEFORE
    // the result is even painted), so by the time the INSERT's output is
    // visible in the DOM the snapshot must already be durable (or torn, in
    // which case the two-slot store falls back to an older image and this
    // check FAILS — by design).
    await typeLine("INSERT INTO keeper VALUES (7,'survived-a-reload');");
    await check('INSERT output painted', () =>
      waitFor((t) => /INSERT 0 1/.test(t), 'INSERT output', 60000, 20));
    await send('Page.reload', { ignoreCache: false });
    ok('reloaded IMMEDIATELY after the INSERT painted (no snapshot-note wait)');
    await check('page came back after a full reload', () =>
      PSQL ? waitFor((t) => /psql \(18\.3\)/.test(t) && /postgres=#/.test(t), 'post-reload prompt', timeoutMs)
           : waitFor((t) => /engine ready/.test(t) && /pgrust=#/.test(t), 'post-reload prompt', timeoutMs));
    await check('the page reports the datadir was restored from OPFS', () =>
      waitFor((t) => /restored your persisted datadir/.test(t), 'restored note', 30000));
    await runLine("SELECT * FROM keeper;", /survived-a-reload/,
                  'persist: the table survived a page reload');
    shots.push(await shoot('persist-after-reload'));

    // Leave the profile clean: persist off wipes the stored snapshots.
    await clickButton('btn-persist');
    await check('persist toggled back off (snapshots cleared)',
                () => waitLabel('btn-persist', 'persist: off', 30000));
  }

  const finalText = await evaluate(TERM_TEXT);
  shots.push(await shoot());
  console.log(`\nscreenshots:\n  ${shots.join('\n  ')}`);
  console.log('--- terminal transcript (tail) ---');
  console.log(finalText.slice(-4000));
} catch (e) {
  console.error('psql-site-shot: SETUP/DRIVE FAILURE: ' + e.message);
  cleanup();
  rmProfile();
  process.exit(2);
}
cleanup();
rmProfile();
console.log(failures === 0
  ? `VERDICT: psql-site-shot(${client}) PASS`
  : `VERDICT: psql-site-shot(${client}) FAIL (${failures})`);
process.exit(failures === 0 ? 0 : 1);
