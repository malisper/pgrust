#!/usr/bin/env node
// tablespace-host-proof.mjs — the HOST half of the tablespace proof, at the WASI seam.
//
// WHY THIS EXISTS. `wasm/tablespace-proof.sql` is the real proof and it is currently BLOCKED in
// the GUEST, not in the host: crates/backend/commands/tablespace/src/lib.rs refuses on wasm before
// it ever reaches a WASI call —
//
//     #[cfg(target_family = "wasm")]
//     let link_result: std::io::Result<()> = Err(std::io::Error::from_raw_os_error(52));
//
// (52 is WASI ENOSYS, which is why the SQL lane reports `could not create symbolic link
// "pg_tblspc/<oid>": Function not implemented`). Nothing the host does can change that answer.
//
// So this script makes exactly the calls that stanza would have made if it were allowed to, in the
// same order, through the SAME machinery the guest uses: the storage coordinator worker
// (wasm/storage-worker.js) owning a root store plus a memory-backed MOUNT at /pgeph, the
// SharedArrayBuffer broker, and the WASI preview1 adapter over it. If every step below passes, the
// host is ready and the one remaining change is the guest's.
//
// The sequence is create_tablespace_directories() plus one relation write:
//
//   1. stat(location)                      — the wasm branch's stand-in for chmod(location)
//   2. mkdir(location/PG_18_<catver>)
//   3. lstat(pg_tblspc/<oid>)              — must be ENOENT before the link is made
//   4. symlink(location, pg_tblspc/<oid>)  — the call that is refused in the guest today
//   5. readlink(pg_tblspc/<oid>)           — what pg_tablespace_location() reads
//   6. lstat / stat of the link            — symlink_metadata() vs metadata()
//   7. mkdir + open + write + fsync THROUGH the link, at a real relfilenode path
//   8. readdir(pg_tblspc)                  — the link must appear, as a link
//
// ...and then the coordinator's per-port file counts settle where the bytes went: the relation must
// be in the MOUNT's store and NOT in the root's.
//
// Usage: node tablespace-host-proof.mjs
// Env:   PGRUST_REPACKED_BUNDLE (defaults to ./vendor/pglite-opfs-repacked.js)
import { makeWorker, onWorkerMessage, onWorkerError, storageWorkerUrl } from './threads-host.js';
import { loadRepackedBundle, repackedBundleUrl } from './broker-fs.js';

const BUNDLE_URL = process.env.PGRUST_REPACKED_BUNDLE || repackedBundleUrl(import.meta.url);
const MOUNT = '/pgeph';
const CATALOG_DIR = 'PG_18_202506291';
const TABLESPACE_OID = 16385;
const LINK = `/pgdata/pg_tblspc/${TABLESPACE_OID}`;
const RELATION = `${LINK}/${CATALOG_DIR}/5/16386`;
const PAGE_BYTES = 8192;

const note = (line) => process.stdout.write(line + '\n');
const failures = [];
function check(ok, what) {
  note(`${ok ? 'ok  ' : 'FAIL'} ${what}`);
  if (!ok) failures.push(what);
}

const bundle = await loadRepackedBundle(BUNDLE_URL);
const { WASI_ERRNO, WASI_FILETYPE } = bundle;

// ---- the coordinator: a root store plus one volatile mount -----------------
// The manifest seeds only the directories the sequence needs, so this proof does not depend on the
// 41 MB packed datadir image at all.
const doorbell = bundle.RepackedDoorbell.create();
const channel = bundle.RepackedChannel.create({ id: 1, doorbell });
const worker = makeWorker(storageWorkerUrl(import.meta.url), { name: 'pgrust-storage-proof' });

let readyResolve = null;
const readyPromise = new Promise((r) => { readyResolve = r; });
let stoppedResolve = null;
const stoppedPromise = new Promise((r) => { stoppedResolve = r; });
let ports = [];

onWorkerMessage(worker, (m) => {
  switch (m.type) {
    case 'storage-ready':
      note(`storage: ready — port ${m.port}, mounts ` +
        (m.mounts || []).map((x) => `${x.prefix}=${x.port}${x.durable ? '' : ' (volatile)'}`).join(', '));
      readyResolve();
      break;
    case 'storage-stopped':
      ports = m.ports || [];
      stoppedResolve();
      break;
    case 'storage-log':
      note(`storage: ${m.text}`);
      break;
    case 'storage-error':
      failures.push(`storage worker error: ${m.message}`);
      note(`storage: ERROR ${m.message}`);
      readyResolve();
      stoppedResolve();
      break;
    default:
      note(`storage: unhandled ${JSON.stringify(m.type)}`);
  }
});
onWorkerError(worker, (e) => {
  failures.push(`storage worker threw: ${e && e.stack ? e.stack : e}`);
  readyResolve();
  stoppedResolve();
});

worker.postMessage({
  kind: 'boot',
  bundleUrl: BUNDLE_URL,
  image: new ArrayBuffer(0),
  manifest: { dirs: ['/pgdata', '/pgdata/base', '/pgdata/pg_tblspc'], files: [] },
  channels: [channel.transfer()],
  doorbell: doorbell.buffer,
  options: { mounts: [{ prefix: MOUNT, port: 'memory' }] },
});
await Promise.race([
  readyPromise,
  new Promise((_, rej) => setTimeout(() => rej(new Error('storage coordinator boot timeout')), 60000)),
]);

// ---- the guest side: a WASI adapter over the broker ------------------------
const client = new bundle.RepackedSyncClient(channel, { requestTimeoutMs: 60000 });
const memory = new WebAssembly.Memory({ initial: 4 });
const wasi = bundle.createWasiPreview1Fs({
  client,
  memory: () => memory.buffer,
  onError: (call, cause) => failures.push(`${call} threw: ${String(cause)}`),
});

// A bump allocator over the guest's linear memory: every WASI call takes pointers, so a proof that
// did not marshal through memory would not be testing the seam at all.
let cursor = 1024;
const alloc = (n) => { const at = (cursor + 7) & ~7; cursor = at + n; return at; };
const bytes = () => new Uint8Array(memory.buffer);
const view = () => new DataView(memory.buffer);
function str(value) {
  const encoded = new TextEncoder().encode(value);
  const ptr = alloc(encoded.byteLength);
  bytes().set(encoded, ptr);
  return { ptr, len: encoded.byteLength };
}
const FOLLOW = 1; // LOOKUPFLAGS_SYMLINK_FOLLOW
const PREOPEN = 3;

function filestat(path, flags) {
  const p = str(path);
  const out = alloc(64);
  const errno = wasi.path_filestat_get(PREOPEN, flags, p.ptr, p.len, out);
  return { errno, filetype: bytes()[out + 16], size: view().getBigUint64(out + 32, true) };
}
function mkdir(path) {
  const p = str(path);
  return wasi.path_create_directory(PREOPEN, p.ptr, p.len);
}
function symlink(target, path) {
  const t = str(target);
  const p = str(path);
  return wasi.path_symlink(t.ptr, t.len, PREOPEN, p.ptr, p.len);
}
function readlink(path) {
  const p = str(path);
  const buf = alloc(512);
  const usedPtr = alloc(4);
  const errno = wasi.path_readlink(PREOPEN, p.ptr, p.len, buf, 512, usedPtr);
  const used = view().getUint32(usedPtr, true);
  return { errno, target: new TextDecoder().decode(bytes().slice(buf, buf + used)) };
}

try {
  // 1. The location must already be a directory. It is — it is the mount's own root.
  const location = filestat(MOUNT, FOLLOW);
  check(location.errno === WASI_ERRNO.SUCCESS && location.filetype === WASI_FILETYPE.DIRECTORY,
    `stat(${MOUNT}) is a directory (errno ${location.errno}, filetype ${location.filetype})`);

  // 2. <location>/PG_18_<catver>, created inside the MOUNT's store.
  check(mkdir(`${MOUNT}/${CATALOG_DIR}`) === WASI_ERRNO.SUCCESS, `mkdir(${MOUNT}/${CATALOG_DIR})`);

  // 3. symlink_metadata(linkloc) before the link exists.
  check(filestat(LINK, 0).errno === WASI_ERRNO.NOENT, `lstat(${LINK}) is ENOENT before the link`);

  // 4. THE CALL THE GUEST REFUSES TODAY.
  check(symlink(MOUNT, LINK) === WASI_ERRNO.SUCCESS, `symlink(${MOUNT}, ${LINK})`);

  // 5. pg_tablespace_location() is a readlink().
  const link = readlink(LINK);
  check(link.errno === WASI_ERRNO.SUCCESS && link.target === MOUNT,
    `readlink(${LINK}) === ${MOUNT} (got ${JSON.stringify(link.target)})`);

  // 6. symlink_metadata() sees a link; metadata() sees the directory it points at.
  check(filestat(LINK, 0).filetype === WASI_FILETYPE.SYMBOLIC_LINK, `lstat(${LINK}) is a symbolic link`);
  check(filestat(LINK, FOLLOW).filetype === WASI_FILETYPE.DIRECTORY, `stat(${LINK}) is a directory`);

  // 7. A relation file, opened THROUGH the link at the path Postgres would use.
  check(mkdir(`${LINK}/${CATALOG_DIR}/5`) === WASI_ERRNO.SUCCESS, `mkdir(${LINK}/${CATALOG_DIR}/5)`);
  const relPath = str(RELATION);
  const fdOut = alloc(4);
  const opened = wasi.path_open(PREOPEN, FOLLOW, relPath.ptr, relPath.len, 1 | 8, -1n, -1n, 0, fdOut);
  check(opened === WASI_ERRNO.SUCCESS, `open(${RELATION}, O_CREAT|O_TRUNC) (errno ${opened})`);
  const fd = view().getUint32(fdOut, true);
  const page = alloc(PAGE_BYTES);
  bytes().fill(0xab, page, page + PAGE_BYTES);
  const iovs = alloc(8);
  view().setUint32(iovs, page, true);
  view().setUint32(iovs + 4, PAGE_BYTES, true);
  const written = alloc(4);
  check(wasi.fd_write(fd, iovs, 1, written) === WASI_ERRNO.SUCCESS && view().getUint32(written, true) === PAGE_BYTES,
    `write(${PAGE_BYTES} bytes) through the link`);
  // A store-wide fsync: it must SKIP the volatile mount and still succeed.
  check(wasi.fd_sync(fd) === WASI_ERRNO.SUCCESS, 'fsync across a root plus a volatile mount');
  check(wasi.fd_close(fd) === WASI_ERRNO.SUCCESS, 'close');
  const relStat = filestat(RELATION, FOLLOW);
  check(relStat.errno === WASI_ERRNO.SUCCESS && relStat.size === BigInt(PAGE_BYTES),
    `stat(${RELATION}).size === ${PAGE_BYTES} (got ${relStat.size})`);

  // 8. The listing recovery walks: pg_tblspc/* must show the link AS a link.
  const dirPath = str('/pgdata/pg_tblspc');
  const dirOut = alloc(4);
  check(wasi.path_open(PREOPEN, FOLLOW, dirPath.ptr, dirPath.len, 2, -1n, -1n, 0, dirOut) === WASI_ERRNO.SUCCESS,
    'open(/pgdata/pg_tblspc, O_DIRECTORY)');
  const dirFd = view().getUint32(dirOut, true);
  const dirBuf = alloc(512);
  bytes().fill(0, dirBuf, dirBuf + 512);
  const usedPtr = alloc(4);
  check(wasi.fd_readdir(dirFd, dirBuf, 512, 0n, usedPtr) === WASI_ERRNO.SUCCESS, 'readdir(/pgdata/pg_tblspc)');
  const used = view().getUint32(usedPtr, true);
  const namlen = view().getUint32(dirBuf + 16, true);
  const filetype = bytes()[dirBuf + 20];
  const name = new TextDecoder().decode(bytes().slice(dirBuf + 24, dirBuf + 24 + namlen));
  check(used > 0 && name === String(TABLESPACE_OID) && filetype === WASI_FILETYPE.SYMBOLIC_LINK,
    `pg_ls_dir('pg_tblspc') lists ${TABLESPACE_OID} as a symbolic link (got ${JSON.stringify(name)}, filetype ${filetype})`);
  wasi.fd_close(dirFd);
} catch (e) {
  failures.push(`proof exception: ${e && e.stack ? e.stack : e}`);
}

// ---- where did the bytes go? ----------------------------------------------
doorbell.requestStop();
await Promise.race([stoppedPromise, new Promise((r) => setTimeout(r, 10000))]);
for (const p of ports) {
  note(`storage: port ${p.name} (${p.port}${p.durable ? '' : ', volatile'}) holds ${p.files} files ` +
    `(${p.bytes} bytes), ${p.links} symlink(s)`);
}
const root = ports.find((p) => p.name === 'root');
const mount = ports.find((p) => p.name === MOUNT);
check(!!mount && mount.files === 1 && mount.bytes === PAGE_BYTES,
  `the relation file is in the MOUNT store (${mount ? `${mount.files} files, ${mount.bytes} bytes` : 'no mount reported'})`);
check(!!root && root.links === 1, `the symlink is in the ROOT store (${root ? `${root.links} link(s)` : 'no root reported'})`);
check(!!root && root.bytes === 0, `the ROOT store holds none of the relation's bytes (${root ? root.bytes : '?'} bytes)`);

try { worker.terminate(); } catch { /* already gone */ }

if (failures.length) {
  for (const f of failures) note(`PROOF-FAIL: ${f}`);
  note('VERDICT: tablespace-host-proof FAIL');
  process.exit(1);
}
note('VERDICT: tablespace-host-proof PASS');
process.exit(0);
