#!/usr/bin/env node
// run-node-psql.mjs — headless verification of the wasm Rust psql
// (increment 2): TWO wasm instances (psql.wasm + postgres --stdio-wire)
// cross-connected through host pipes, driven by a psql SCRIPT on stdin;
// psql's stdout/stderr land in files for byte-comparison against a native
// run of the same script.
//
// Usage: node run-node-psql.mjs --script FILE [--out FILE] [--err FILE]
// Env: PGRUST_WASM (postgres.wasm), PGRUST_PSQL_WASM (psql.wasm),
//      PGRUST_VFS (prefix for vfs.img/vfs.json), PGRUST_SERVER_LOG (file).
//
// Requires JSPI (Node >= 24 class V8), like run-node-wire.mjs.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Vfs } from './pgrust-wasi.js';
import { jspiSupported, defaultWireArgv } from './wiresession.js';
import { PsqlSession } from './psqlsession.js';

if (!jspiSupported()) {
  console.error('run-node-psql: this Node has no JSPI (WebAssembly.Suspending/promising)');
  process.exit(2);
}

const here = path.dirname(fileURLToPath(import.meta.url));
const assets = path.join(here, 'assets');
const serverWasmPath = process.env.PGRUST_WASM || path.join(assets, 'postgres.wasm');
const psqlWasmPath = process.env.PGRUST_PSQL_WASM ||
  path.join(here, '../target/wasm32-wasip1/release/psql.wasm');
const vfsPrefix = process.env.PGRUST_VFS || path.join(assets, 'vfs');

function argAfter(flag) {
  const i = process.argv.indexOf(flag);
  return i !== -1 ? process.argv[i + 1] : undefined;
}
const scriptFile = argAfter('--script');
const outFile = argAfter('--out');
const errFile = argAfter('--err');
const vfsFile = argAfter('--vfs-file'); // "HOSTPATH:GUESTPATH" extra file (e.g. \i script)
if (!scriptFile) {
  console.error('usage: run-node-psql.mjs --script FILE [--out FILE] [--err FILE]');
  process.exit(2);
}

const script = fs.readFileSync(scriptFile);
const outFd = outFile ? fs.openSync(outFile, 'w') : 1;
const errFd = errFile ? fs.openSync(errFile, 'w') : 2;
const logFd = process.env.PGRUST_SERVER_LOG
  ? fs.openSync(process.env.PGRUST_SERVER_LOG, 'w') : null;

// Serial compiles: two concurrent WebAssembly.compile calls (one of them a
// multi-hundred-MB dev module) have been seen to wedge Node.
const psqlModule = await WebAssembly.compile(fs.readFileSync(psqlWasmPath));
const serverModule = await WebAssembly.compile(fs.readFileSync(serverWasmPath));
const image = new Uint8Array(fs.readFileSync(vfsPrefix + '.img'));
const manifest = JSON.parse(fs.readFileSync(vfsPrefix + '.json', 'utf8'));
const vfs = new Vfs(image, manifest);
if (vfsFile) {
  const [hostPath, guestPath] = vfsFile.split(':');
  const node = vfs.create(guestPath);
  node.data = new Uint8Array(fs.readFileSync(hostPath));
  node.owned = true;
}

// Pin the timezone GUCs like the wire e2e, for transcript identity with the
// native arm.
const serverArgv = defaultWireArgv(['-c', 'timezone=UTC', '-c', 'log_timezone=UTC']);

const session = new PsqlSession({
  psqlModule,
  serverModule,
  vfs,
  serverArgv,
  psqlStdinBytes: script,
  psqlArgv: ['psql', '-X'],
  psqlEnv: {
    USER: 'postgres',
    PSQL_INTERACTIVE: '0',
    PGRUST_TZDIR: '/share/timezone',
    PGRUST_PGSHAREDIR: '/share',
  },
  serverEnv: {
    USER: 'postgres',
    PGRUST_TZDIR: '/share/timezone',
    PGRUST_PGSHAREDIR: '/share',
    PGRUST_RUNTIME: '0',
    RUST_BACKTRACE: '1',
  },
  onPsqlStdout: (b) => fs.writeSync(outFd, b),
  onPsqlStderr: (b) => fs.writeSync(errFd, b),
  onServerStderr: (b) => { if (logFd !== null) fs.writeSync(logFd, b); },
});

// JSPI suspensions are not "pending work" to Node's event loop: with both
// guests suspended on each other, the loop can drain and the process exits
// with an unsettled top-level await. Park a timer until psql finishes.
const keepalive = setInterval(() => {}, 1000);
await session.start();
const code = await session.wait();
clearInterval(keepalive);
process.exit(code ?? 0);
