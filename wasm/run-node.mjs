#!/usr/bin/env node
// run-node.mjs — boot pgrust single-user in Node using the shared JS harness.
//
// The same pgrust-harness.js that the browser page uses runs here too. This is
// the ground-truth runner: it boots the bundled datadir VFS and runs SQL from
// stdin (or --sql "..."), printing the single-user `backend>` output.
//
// Usage:
//   echo "SELECT 1;" | node run-node.mjs
//   node run-node.mjs --sql "SELECT * FROM (VALUES (1,'a'),(2,'b')) v(i,t);"
//
// Env: PGRUST_WASM (path to postgres.wasm), PGRUST_VFS (prefix for vfs.img/json).
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { run } from './pgrust-wasi.js';
import { formatRun, normalizeSingleUserInput } from './format.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const defaultAssetDir = path.join(here, 'assets');
const wasmPath = process.env.PGRUST_WASM || path.join(defaultAssetDir, 'postgres.wasm');
const vfsPrefix = process.env.PGRUST_VFS || path.join(defaultAssetDir, 'vfs');

function readStdinSync() {
  try { return fs.readFileSync(0); } catch { return Buffer.alloc(0); }
}

let sql;
const sqlFlag = process.argv.indexOf('--sql');
if (sqlFlag !== -1 && process.argv[sqlFlag + 1]) {
  sql = process.argv[sqlFlag + 1];
  if (!sql.trimEnd().endsWith(';')) sql += ';';
  sql += '\n';
} else {
  sql = readStdinSync().toString('utf8');
}
if (!sql || !sql.trim()) sql = 'SELECT 1;\n';
// The single-user backend ends a statement at every newline, so flatten each
// SQL statement onto one line before feeding it in (see normalizeSingleUserInput).
sql = normalizeSingleUserInput(sql) || 'SELECT 1;\n';

const t0 = Date.now();
process.stderr.write(`[node-runner] loading ${wasmPath}...\n`);
const wasmBytes = fs.readFileSync(wasmPath);
const wasmModule = await WebAssembly.compile(wasmBytes);
process.stderr.write(`[node-runner] compiled in ${Date.now() - t0}ms; loading VFS...\n`);

const image = new Uint8Array(fs.readFileSync(vfsPrefix + '.img'));
const manifest = JSON.parse(fs.readFileSync(vfsPrefix + '.json', 'utf8'));
process.stderr.write(`[node-runner] VFS: ${manifest.files.length} files, ${image.length} bytes. Booting...\n`);

const outChunks = [];
const errChunks = [];
const raw = process.argv.includes('--raw');
const result = await run({
  wasmModule,
  image,
  manifest,
  stdinBytes: new TextEncoder().encode(sql),
  onStdout: (b) => { outChunks.push(b); },
  onStderr: (b) => { errChunks.push(Buffer.from(b)); process.stderr.write(Buffer.from(b)); },
});

const stdout = Buffer.concat(outChunks).toString('utf8');
const stderr = Buffer.concat(errChunks).toString('utf8');
process.stdout.write(raw ? stdout : formatRun(stdout, stderr) + '\n');
process.stderr.write(`\n[node-runner] guest exited with code ${result.exitCode} (${Date.now() - t0}ms total)\n`);
process.exit(result.exitCode);
