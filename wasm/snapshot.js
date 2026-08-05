// snapshot.js — durable datadir snapshots for the wasm-web worker.
//
// The worker's whole database state is its in-memory VFS. Persistence across
// page reloads is SNAPSHOT-CLASS (the v1 design the OPFS support matrix
// forces): serialize the VFS to one self-validating binary blob at a quiesce
// point, restore it at boot. Never per-write OPFS I/O — sync access handles
// are worker-only and iOS Safari's OPFS surface is exactly
// getDirectory()+createSyncAccessHandle-in-a-dedicated-worker (supported
// since Safari 15.2; createWritable shipped much later), which this module
// limits itself to.
//
// Blob format (little-endian):
//   magic "PGRSNP1\0" (8) | version u32 | generation u32 | manifestLen u32 |
//   manifest JSON (UTF-8) | file data (concatenated, manifest order) |
//   crc32 u32 over everything before it
// The manifest is the SAME {dirs, files:[{path,off,len,mtime}]} shape
// pack-vfs.mjs emits, so a restored snapshot feeds the Vfs constructor as-is.
//
// Durability without rename(): TWO fixed slots (a/b) written alternately,
// each carrying a monotonically increasing generation. A torn/corrupt write
// only ever ruins the slot being written; the loader picks the
// highest-generation slot that VALIDATES (magic + version + lengths + CRC),
// so the previous good snapshot survives any interrupted save. Corruption of
// both slots degrades to "no snapshot" — the worker boots the pristine
// packed image. Never a wedged page.

const MAGIC = 'PGRSNP1\0';
const VERSION = 1;
const HEADER_LEN = 8 + 4 + 4 + 4;

export const SNAPSHOT_SLOTS = ['pgrust-snap-a.bin', 'pgrust-snap-b.bin'];

export class SnapshotCorruptError extends Error {}

// ---- crc32 (IEEE, table-based) ----------------------------------------------
const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    t[n] = c >>> 0;
  }
  return t;
})();

export function crc32(bytes, seed = 0) {
  let c = (seed ^ 0xFFFFFFFF) >>> 0;
  for (let i = 0; i < bytes.length; i++) {
    c = CRC_TABLE[(c ^ bytes[i]) & 0xFF] ^ (c >>> 8);
  }
  return (c ^ 0xFFFFFFFF) >>> 0;
}

// ---- serialize / restore ----------------------------------------------------

// vfs: the pgrust-wasi.js Vfs (nodes: Map path -> {type, mtime, data?}).
export function serializeVfs(vfs, generation) {
  const dirs = [];
  const files = [];
  let off = 0;
  const datas = [];
  for (const [path, node] of vfs.nodes.entries()) {
    if (node.type === 'dir') {
      dirs.push(path);
    } else {
      files.push({ path, off, len: node.data.length, mtime: node.mtime || 0 });
      datas.push(node.data);
      off += node.data.length;
    }
  }
  const manifestBytes = new TextEncoder().encode(JSON.stringify({ dirs, files }));
  const total = HEADER_LEN + manifestBytes.length + off + 4;
  const out = new Uint8Array(total);
  const view = new DataView(out.buffer);
  for (let i = 0; i < 8; i++) out[i] = MAGIC.charCodeAt(i);
  view.setUint32(8, VERSION, true);
  view.setUint32(12, generation >>> 0, true);
  view.setUint32(16, manifestBytes.length, true);
  out.set(manifestBytes, HEADER_LEN);
  let p = HEADER_LEN + manifestBytes.length;
  for (const d of datas) { out.set(d, p); p += d.length; }
  view.setUint32(p, crc32(out.subarray(0, p)), true);
  return out;
}

// Validates structure + CRC; returns { image, manifest, generation }.
// The image/manifest pair feeds `new Vfs(image, manifest)` directly.
export function restoreVfs(bytes) {
  if (!(bytes instanceof Uint8Array) || bytes.length < HEADER_LEN + 4) {
    throw new SnapshotCorruptError('snapshot too short');
  }
  for (let i = 0; i < 8; i++) {
    if (bytes[i] !== MAGIC.charCodeAt(i)) throw new SnapshotCorruptError('bad magic');
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const version = view.getUint32(8, true);
  if (version !== VERSION) throw new SnapshotCorruptError(`unsupported version ${version}`);
  const generation = view.getUint32(12, true);
  const manifestLen = view.getUint32(16, true);
  if (HEADER_LEN + manifestLen + 4 > bytes.length) throw new SnapshotCorruptError('manifest overruns blob');
  const crcAt = bytes.length - 4;
  const stored = view.getUint32(crcAt, true);
  if (crc32(bytes.subarray(0, crcAt)) !== stored) throw new SnapshotCorruptError('crc mismatch');
  let manifest;
  try {
    manifest = JSON.parse(new TextDecoder().decode(bytes.subarray(HEADER_LEN, HEADER_LEN + manifestLen)));
  } catch {
    throw new SnapshotCorruptError('manifest not JSON');
  }
  if (!manifest || !Array.isArray(manifest.dirs) || !Array.isArray(manifest.files)) {
    throw new SnapshotCorruptError('manifest shape wrong');
  }
  const dataStart = HEADER_LEN + manifestLen;
  const dataLen = crcAt - dataStart;
  for (const f of manifest.files) {
    if (typeof f.off !== 'number' || typeof f.len !== 'number' || f.off + f.len > dataLen) {
      throw new SnapshotCorruptError('file entry overruns data');
    }
  }
  // Slice so file offsets are 0-based over the image, like pack-vfs.mjs output.
  const image = bytes.slice(dataStart, crcAt);
  return { image, manifest, generation };
}

// ---- store-agnostic slot logic ---------------------------------------------
// store: { read(name) -> Promise<Uint8Array|null>, write(name, bytes),
//          remove(name) } — OPFS in the worker, an in-memory Map in the e2e.

// Best valid snapshot across the slots, or null. Corrupt/absent slots are
// skipped (reported via onBad for diagnostics).
export async function loadLatestSnapshot(store, onBad) {
  let best = null;
  for (const slot of SNAPSHOT_SLOTS) {
    let bytes = null;
    try { bytes = await store.read(slot); } catch { bytes = null; }
    if (!bytes || bytes.length === 0) continue;
    try {
      const snap = restoreVfs(bytes);
      if (!best || snap.generation > best.generation) best = { ...snap, slot };
    } catch (e) {
      if (onBad) onBad(slot, e);
    }
  }
  return best;
}

// Write the next snapshot to the slot NOT holding the current best generation.
export async function saveSnapshot(store, vfs, prevGeneration, prevSlot) {
  const generation = (prevGeneration >>> 0) + 1;
  const target = prevSlot === SNAPSHOT_SLOTS[0] ? SNAPSHOT_SLOTS[1] : SNAPSHOT_SLOTS[0];
  const bytes = serializeVfs(vfs, generation);
  await store.write(target, bytes);
  return { generation, slot: target, bytes: bytes.length };
}

export async function clearSnapshots(store) {
  for (const slot of SNAPSHOT_SLOTS) {
    try { await store.remove(slot); } catch { /* best effort */ }
  }
}

// ---- OPFS store (dedicated-worker only) -------------------------------------
// Uses exactly the WebKit-supported surface: navigator.storage.getDirectory()
// + FileSystemFileHandle.createSyncAccessHandle(). Returns null when the
// environment has no OPFS (Node, private-mode Safari, main thread) — the
// caller treats that as "persistence unavailable", never an error page.
export async function openOpfsStore() {
  try {
    if (typeof navigator === 'undefined' || !navigator.storage || !navigator.storage.getDirectory) return null;
    const root = await navigator.storage.getDirectory();
    // Probe for sync access handles (worker-only; absent on some engines).
    const probeName = 'pgrust-snap-probe.tmp';
    try {
      const probe = await root.getFileHandle(probeName, { create: true });
      const ah = await probe.createSyncAccessHandle();
      ah.close();
      await root.removeEntry(probeName);
    } catch {
      try { await root.removeEntry(probeName); } catch { /* ignore */ }
      return null;
    }
    return {
      async read(name) {
        let fh;
        try { fh = await root.getFileHandle(name); } catch { return null; }
        const ah = await fh.createSyncAccessHandle();
        try {
          const size = ah.getSize();
          const buf = new Uint8Array(size);
          ah.read(buf, { at: 0 });
          return buf;
        } finally {
          ah.close();
        }
      },
      async write(name, bytes) {
        const fh = await root.getFileHandle(name, { create: true });
        const ah = await fh.createSyncAccessHandle();
        try {
          ah.truncate(0);
          ah.write(bytes, { at: 0 });
          ah.flush();
        } finally {
          ah.close();
        }
      },
      async remove(name) {
        try { await root.removeEntry(name); } catch { /* absent is fine */ }
      },
    };
  } catch {
    return null;
  }
}
