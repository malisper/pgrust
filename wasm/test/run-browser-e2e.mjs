#!/usr/bin/env node
// run-browser-e2e.mjs — headless-Chrome driver for test/psql-e2e.html
// (increment 3 verification: the REAL Rust psql.wasm driving the terminal
// against postgres.wasm in a browser worker).
//
// Pattern: the PAGE decides its own pass/fail and POSTs the verdict +
// transcript to a listener this harness runs — polling `--dump-dom`
// snapshots the DOM too early and races the async battery.
//
//   node test/run-browser-e2e.mjs [--port 8093] [--timeout 180]
//
// Spawns serve.mjs on --port, a verdict listener on --port+1, then Chrome
// headless on the test page with ?client=psql&reportTo=<listener>. Exits 0
// on PASS (verdict + transcript on stdout), 1 on FAIL, 2 on setup trouble.
// All children are killed on the way out.
import http from 'node:http';
import path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import fs from 'node:fs';
import os from 'node:os';

const here = path.dirname(fileURLToPath(import.meta.url));
const webRoot = path.join(here, '..');

function argAfter(flag, dflt) {
  const i = process.argv.indexOf(flag);
  return i !== -1 ? process.argv[i + 1] : dflt;
}
const port = Number(argAfter('--port', '8093'));
const reportPort = port + 1;
const timeoutSec = Number(argAfter('--timeout', '180'));

const CHROME = process.env.CHROME ||
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
if (!fs.existsSync(CHROME)) {
  console.error(`run-browser-e2e: no Chrome at ${CHROME} (set CHROME=...)`);
  process.exit(2);
}

const t0 = Date.now();
const children = [];
function cleanup() {
  for (const c of children) { try { c.kill('SIGKILL'); } catch { /* gone */ } }
}
process.on('exit', cleanup);
process.on('SIGINT', () => { cleanup(); process.exit(2); });

// 1. static server
const server = spawn(process.execPath, [path.join(webRoot, 'serve.mjs'), String(port)], {
  stdio: ['ignore', 'ignore', 'pipe'],
});
children.push(server);
await new Promise((resolve, reject) => {
  server.stderr.on('data', () => resolve()); // "pgrust wasm webapp: http://..."
  server.on('exit', (c) => reject(new Error(`serve.mjs exited (${c}) — port ${port} busy?`)));
  setTimeout(resolve, 2000);
});

// 2. verdict listener
let settle;
const verdictArrived = new Promise((r) => { settle = r; });
const listener = http.createServer((req, res) => {
  let body = '';
  req.on('data', (d) => { body += d; });
  req.on('end', () => {
    res.writeHead(200, { 'Access-Control-Allow-Origin': '*' });
    res.end('ok');
    if (req.method !== 'POST') return;
    if (req.url === '/log') { console.error(`[page +${((Date.now() - t0) / 1000).toFixed(1)}s] ${body}`); return; }
    settle(body);
  });
});
await new Promise((r, j) => listener.listen(reportPort, (e) => e ? j(e) : r()));

// 3. headless Chrome on the test page
const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'psql-e2e-chrome-'));
const debug = process.argv.includes('--debug') ? '&debug=1' : '';
const url = `http://localhost:${port}/test/psql-e2e.html?client=psql${debug}` +
  `&reportTo=${encodeURIComponent(`http://localhost:${reportPort}/verdict`)}`;
const chrome = spawn(CHROME, [
  '--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check',
  // A headless page is never visible: without these, Chrome's intensive
  // timer throttling freezes the page's setTimeout/setInterval (including
  // the test page's own fallback timer), which reads as a silent hang.
  '--disable-background-timer-throttling', '--disable-renderer-backgrounding',
  '--disable-backgrounding-occluded-windows',
  `--user-data-dir=${profile}`, url,
], { stdio: ['ignore', 'ignore', 'pipe'] });
children.push(chrome);
let chromeErr = '';
chrome.stderr.on('data', (d) => { chromeErr += d; });

// 4. wait (bounded) for the page's verdict
const body = await Promise.race([
  verdictArrived,
  new Promise((r) => setTimeout(() => r(null), timeoutSec * 1000)),
]);

cleanup();
listener.close();
// Chrome can still be flushing its profile dir when we unlink it (ENOTEMPTY):
// a cleanup race must never turn a PASS into a nonzero exit.
try { fs.rmSync(profile, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 }); }
catch { /* leftover temp dir; the OS reaps it */ }

if (body === null) {
  console.error(`run-browser-e2e: TIMEOUT after ${timeoutSec}s (no verdict POST)`);
  if (chromeErr) console.error('--- chrome stderr ---\n' + chromeErr);
  process.exit(2);
}
console.log(body);
process.exit(body.startsWith('VERDICT: psql-browser-e2e PASS') ? 0 : 1);
