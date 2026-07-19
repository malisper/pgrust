#!/usr/bin/env node
// run-node-wire.mjs — drive postgres.wasm in PROTOCOL MODE (--stdio-wire)
// under Node, through the SAME wiresession.js the browser worker uses.
//
// This is the JS twin of wasm/pgwire_stdio_driver.py: it runs one
// long-lived pgwire session — startup handshake, one INTERACTIVE
// simple-query cycle per statement (each Q is sent only after the previous
// ReadyForQuery arrived, exercising the JSPI suspend/resume between
// statements), then Terminate — and prints the IDENTICAL canonical
// transcript, so the wire e2e can byte-diff it against the native
// --stdio-wire arm driven by the python driver.
//
// Requires JSPI (WebAssembly.Suspending/promising) — Node >= 24 class V8.
//
// Usage: node run-node-wire.mjs --sql FILE [--stderr FILE]
//   FILE holds one SQL statement per line ('--' comment lines skipped).
// Env: PGRUST_WASM (path to postgres.wasm), PGRUST_VFS (prefix for vfs.img/json),
//      PGRUST_WIRE_GUCS (comma-separated extra -c GUCs, default pins
//      timezone=UTC,log_timezone=UTC for transcript identity).
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Vfs } from './pgrust-wasi.js';
import { WireSession, jspiSupported, canonMessage, defaultWireArgv } from './wiresession.js';

if (!jspiSupported()) {
  console.error('run-node-wire: this Node has no JSPI (WebAssembly.Suspending/promising)');
  process.exit(2);
}

const here = path.dirname(fileURLToPath(import.meta.url));
const defaultAssetDir = path.join(here, 'assets');
const wasmPath = process.env.PGRUST_WASM || path.join(defaultAssetDir, 'postgres.wasm');
const vfsPrefix = process.env.PGRUST_VFS || path.join(defaultAssetDir, 'vfs');

function argAfter(flag) {
  const i = process.argv.indexOf(flag);
  return i !== -1 ? process.argv[i + 1] : undefined;
}
const sqlFile = argAfter('--sql');
const stderrFile = argAfter('--stderr');
if (!sqlFile) {
  console.error('usage: run-node-wire.mjs --sql FILE [--stderr FILE]');
  process.exit(2);
}

const stmts = fs.readFileSync(sqlFile, 'utf8')
  .split('\n')
  .map((l) => l.trim())
  .filter((l) => l && !l.startsWith('--'));

const errFd = stderrFile ? fs.openSync(stderrFile, 'w') : 2;

const wasmModule = await WebAssembly.compile(fs.readFileSync(wasmPath));
const image = new Uint8Array(fs.readFileSync(vfsPrefix + '.img'));
const manifest = JSON.parse(fs.readFileSync(vfsPrefix + '.json', 'utf8'));
const vfs = new Vfs(image, manifest);

const extraGucs = (process.env.PGRUST_WIRE_GUCS || 'timezone=UTC,log_timezone=UTC')
  .split(',')
  .filter(Boolean)
  .flatMap((g) => ['-c', g]);

const failures = [];
function note(line) { process.stdout.write(line + '\n'); }

const session = new WireSession({
  wasmModule,
  vfs,
  argv: defaultWireArgv(extraGucs),
  onStderr: (b) => fs.writeSync(errFd, b),
});

try {
  // Handshake: R(0) ... S* ... K ... Z(I) — canon lines match the python driver.
  const handshake = await session.start();
  let sawAuthOk = false, sawKey = false;
  for (const { t, body } of handshake) {
    note(canonMessage(t, body));
    if (t === 'R') sawAuthOk = new DataView(body.buffer, body.byteOffset).getInt32(0, false) === 0;
    else if (t === 'K') sawKey = true;
    else if (t === 'E') failures.push('error during handshake');
  }
  if (!sawAuthOk) failures.push('no AuthenticationOk in handshake');
  if (!sawKey) failures.push('no BackendKeyData in handshake');

  // Simple-query cycles — INTERACTIVE: each Q goes down only after the
  // previous cycle's ReadyForQuery (the guest suspends on its blocking stdin
  // read in between; JSPI resumes it).
  for (const sql of stmts) {
    note(`>>> Q ${sql}`);
    const msgs = await session.query(sql);
    for (const { t, body } of msgs) note(canonMessage(t, body));
  }

  note('>>> X');
  const rc = await session.terminate();
  if (session.leftoverBytes() > 0) {
    failures.push(`${session.leftoverBytes()} unexpected bytes after Terminate`);
  }
  note(`=== exit ${rc}`);
  if (rc !== 0) failures.push(`server exit code ${rc}`);
} catch (e) {
  failures.push(`driver exception: ${String(e && e.message || e)}`);
}

if (failures.length) {
  for (const f of failures) console.error(`DRIVER-FAIL: ${f}`);
  process.exit(1);
}
process.exit(0);
