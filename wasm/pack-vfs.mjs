#!/usr/bin/env node
// pack-vfs.mjs — pack a directory tree into a single binary image + JSON manifest
// for the browser/Node VFS. Usage: node pack-vfs.mjs <root-dir> <out-prefix>
//
// Output:
//   <out-prefix>.img   — all file contents concatenated
//   <out-prefix>.json  — { dirs: [path,...], files: [{path,off,len,mode,mtime}] }
//
// Paths in the manifest are absolute guest paths ("/base/1/...", "/tmp/pgrust_share/...").
import fs from 'node:fs';
import path from 'node:path';

const root = process.argv[2];
const outPrefix = process.argv[3];
if (!root || !outPrefix) {
  console.error('usage: node pack-vfs.mjs <root-dir> <out-prefix>');
  process.exit(2);
}

const dirs = [];
const files = [];
const chunks = [];
let offset = 0;

function walk(absDir, guestDir) {
  const entries = fs.readdirSync(absDir, { withFileTypes: true });
  for (const e of entries) {
    const abs = path.join(absDir, e.name);
    const guest = guestDir === '/' ? '/' + e.name : guestDir + '/' + e.name;
    const st = fs.lstatSync(abs);
    if (e.isDirectory()) {
      dirs.push(guest);
      walk(abs, guest);
    } else if (e.isFile()) {
      const data = fs.readFileSync(abs);
      chunks.push(data);
      files.push({ path: guest, off: offset, len: data.length, mode: st.mode, mtime: Math.floor(st.mtimeMs / 1000) });
      offset += data.length;
    }
    // symlinks/others skipped (datadir template has none that matter for single-user boot)
  }
}

dirs.push('/');
walk(root, '/');

fs.writeFileSync(outPrefix + '.img', Buffer.concat(chunks));
fs.writeFileSync(outPrefix + '.json', JSON.stringify({ dirs, files }));
console.error(`packed ${files.length} files (${offset} bytes), ${dirs.length} dirs`);
