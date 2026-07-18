// pgrust-wasi.js — hand-rolled WASI preview1 host for the pgrust-fast
// wasm32-wasip1 single-user `postgres.wasm` module, over an in-memory VFS.
//
// This replaces the pgrust-harness.js `pgvfs::host_*` layer the site used for
// the old wasm32/64-unknown-unknown builds: pgrust-fast builds postgres.wasm
// for wasm32-wasip1 (panic=unwind lowered through standard Wasm exception
// handling), so the module's entire import surface is the 33
// `wasi_snapshot_preview1` functions enumerated below — no env::* stubs at
// all. We hand-roll them (rather than vendoring a browser WASI shim library)
// because the needed subset is small, we already own the in-memory VFS +
// packed-image model from the original site, and this keeps the page
// dependency-free.
//
// Contract mirrored from wasm/wasm-boot-e2e.sh (the wasmtime host):
//   - ONE preopened directory: "/" (fd 3) — the whole VFS. The datadir lives
//     at /pgdata and the share tree (timezone etc.) at /share; env supplies
//     USER=postgres, PGRUST_TZDIR=/share/timezone, PGRUST_PGSHAREDIR=/share,
//     PGRUST_RUNTIME=0.
//   - argv = ["postgres","--single","-D","/pgdata", GUCs..., "postgres"] with
//     the boot lane's GUC set (max_stack_depth=60000, io_method=sync,
//     autovacuum=off, wal_sync_method=fdatasync).
//   - stdin (fd 0) = the SQL text, read once to EOF; stdout/stderr (fd 1/2)
//     are captured into the provided sinks.
//   - proc_exit raises a typed JS exception carrying the exit code; with the
//     module's wasm-EH enabled, Rust cleanup pads (catch_all+rethrow) pass a
//     foreign JS exception through, so it propagates out to run() intact.
//   - poll_oneoff reports clock subscriptions as immediately fired (no
//     blocking primitive in a COEP-less browser worker); --single never
//     sleeps on the boot/battery path. sock_recv/sock_send return NOTSUP.

// WASI preview1 errno values.
const E = {
  SUCCESS: 0, BADF: 8, EXIST: 20, INVAL: 28, IO: 29, ISDIR: 31, NOENT: 44,
  NOSYS: 52, NOTDIR: 54, NOTEMPTY: 55, NOTSUP: 58, PERM: 63, SPIPE: 70,
};

// WASI filetypes.
const FT = { UNKNOWN: 0, BLOCK: 1, CHAR: 2, DIR: 3, REG: 4 };

// path_open oflags / fdflags bits.
const OFLAG = { CREAT: 1, DIRECTORY: 2, EXCL: 4, TRUNC: 8 };
const FDFLAG = { APPEND: 1 };

class GuestExit extends Error {
  constructor(code) { super(`guest exited with code ${code}`); this.code = code; }
}

// ---------------------------------------------------------------------------
// In-memory VFS (same packed image + manifest model as the original site:
// pack-vfs.mjs emits { dirs:[path], files:[{path,off,len,mode,mtime}] } over a
// single concatenated byte image).
// ---------------------------------------------------------------------------
class Vfs {
  constructor(image, manifest) {
    this.nodes = new Map(); // path -> { type:'dir'|'file', mtime(sec), data?:Uint8Array, ino }
    let ino = 100;
    for (const d of manifest.dirs) {
      this.nodes.set(this._norm(d), { type: 'dir', mtime: 0, ino: ino++ });
    }
    for (const f of manifest.files) {
      const data = image.subarray(f.off, f.off + f.len);
      this.nodes.set(this._norm(f.path), { type: 'file', mtime: f.mtime || 0, data, ino: ino++ });
    }
    this.nextIno = ino;
    for (const p of [...this.nodes.keys()]) this._ensureAncestors(p);
  }

  _norm(p) {
    p = String(p).replace(/\0+$/g, '');
    if (p === '' || p === '.') return '/';
    if (!p.startsWith('/')) p = '/' + p;
    const parts = [];
    for (const seg of p.split('/')) {
      if (seg === '' || seg === '.') continue;
      if (seg === '..') { parts.pop(); continue; }
      parts.push(seg);
    }
    return '/' + parts.join('/');
  }

  _ensureAncestors(p) {
    let cur = '';
    const parts = p.split('/').filter(Boolean);
    parts.pop();
    for (const seg of parts) {
      cur += '/' + seg;
      if (!this.nodes.has(cur)) this.nodes.set(cur, { type: 'dir', mtime: 0, ino: this.nextIno++ });
    }
    if (!this.nodes.has('/')) this.nodes.set('/', { type: 'dir', mtime: 0, ino: 1 });
  }

  get(p) { return this.nodes.get(this._norm(p)); }

  create(p) {
    const key = this._norm(p);
    const node = { type: 'file', mtime: nowSec(), data: new Uint8Array(0), ino: this.nextIno++ };
    this.nodes.set(key, node);
    this._ensureAncestors(key);
    return node;
  }

  mkdir(p) {
    const key = this._norm(p);
    if (this.nodes.has(key)) return E.EXIST;
    this.nodes.set(key, { type: 'dir', mtime: nowSec(), ino: this.nextIno++ });
    this._ensureAncestors(key);
    return 0;
  }

  unlink(p) {
    const key = this._norm(p);
    const node = this.nodes.get(key);
    if (!node) return E.NOENT;
    if (node.type === 'dir') return E.ISDIR;
    this.nodes.delete(key);
    return 0;
  }

  rmdir(p) {
    const key = this._norm(p);
    const node = this.nodes.get(key);
    if (!node) return E.NOENT;
    if (node.type !== 'dir') return E.NOTDIR;
    const prefix = key === '/' ? '/' : key + '/';
    for (const k of this.nodes.keys()) {
      if (k !== key && k.startsWith(prefix)) return E.NOTEMPTY;
    }
    this.nodes.delete(key);
    return 0;
  }

  rename(from, to) {
    const fk = this._norm(from), tk = this._norm(to);
    const node = this.nodes.get(fk);
    if (!node) return E.NOENT;
    if (node.type === 'dir') {
      const prefix = fk + '/';
      for (const [k, v] of [...this.nodes.entries()]) {
        if (k === fk) { this.nodes.delete(k); this.nodes.set(tk, v); }
        else if (k.startsWith(prefix)) { this.nodes.delete(k); this.nodes.set(tk + k.slice(fk.length), v); }
      }
    } else {
      this.nodes.delete(fk);
      this.nodes.set(tk, node);
    }
    this._ensureAncestors(tk);
    return 0;
  }

  listDir(p) {
    const key = this._norm(p);
    const prefix = key === '/' ? '/' : key + '/';
    const names = [];
    for (const [k, v] of this.nodes.entries()) {
      if (k === key || !k.startsWith(prefix)) continue;
      const rest = k.slice(prefix.length);
      if (rest.includes('/')) continue;
      names.push({ name: rest, node: v });
    }
    names.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    return names;
  }
}

function nowSec() { return Math.floor(Date.now() / 1000); }

// ---------------------------------------------------------------------------
// The WASI host: fd table + the 33 imported functions.
// ---------------------------------------------------------------------------
const PREOPEN_FD = 3;
const ALL_RIGHTS = 0xFFFFFFFFFFFFFFFFn;

export function makeWasi({ image, manifest, vfs: existingVfs, stdinBytes, stdinStream, onStdout, onStderr, argv: argvOverride, env: envOverride }) {
  const vfs = existingVfs || new Vfs(image, manifest);

  const argv = argvOverride || [
    'postgres', '--single',
    '-D', '/pgdata',
    '-c', 'max_stack_depth=60000',
    '-c', 'io_method=sync',
    '-c', 'autovacuum=off',
    '-c', 'wal_sync_method=fdatasync',
    // 32MB shared_buffers keeps peak linear memory ~118MiB (vs ~220MiB at the
    // initdb default 128MB) — inside iOS Safari's comfort zone.
    '-c', 'shared_buffers=32MB',
    'postgres',
  ];
  const envp = Object.entries(envOverride || {
    USER: 'postgres',
    PGRUST_TZDIR: '/share/timezone',
    PGRUST_PGSHAREDIR: '/share',
    PGRUST_RUNTIME: '0',
    RUST_BACKTRACE: '1',
  }).map(([k, v]) => `${k}=${v}`);

  const enc = new TextEncoder();
  const dec = new TextDecoder('utf-8', { fatal: false });
  const stdin = stdinBytes instanceof Uint8Array ? stdinBytes : enc.encode(stdinBytes || '');
  let stdinPos = 0;

  // fd table. 0/1/2 = stdio, 3 = the "/" preopen, >=4 open files/dirs.
  // file entry: { kind:'file', path, node, pos, append }
  // dir entry:  { kind:'dir',  path, node, entries?:snapshot }
  const fds = new Map();
  fds.set(0, { kind: 'stdin' });
  fds.set(1, { kind: 'stdout' });
  fds.set(2, { kind: 'stderr' });
  fds.set(PREOPEN_FD, { kind: 'dir', path: '/', node: vfs.get('/') });
  let nextFd = PREOPEN_FD + 1;

  let memory = null;
  const u8 = () => new Uint8Array(memory.buffer);
  const dv = () => new DataView(memory.buffer);
  const num = (x) => (typeof x === 'bigint' ? Number(x) : x);

  function readStr(ptr, len) {
    return dec.decode(u8().subarray(ptr, ptr + len));
  }
  // Resolve a WASI (dirfd, path) pair. Guest paths arrive preopen-relative;
  // wasi-libc maps absolute paths onto the "/" preopen for us, but be liberal
  // and accept absolute paths too (Vfs._norm roots either form).
  function atPath(dirfd, ptr, len) {
    const h = fds.get(dirfd);
    if (!h || h.kind !== 'dir') return null;
    const rel = readStr(ptr, len);
    if (rel.startsWith('/')) return rel;
    return (h.path === '/' ? '/' : h.path + '/') + rel;
  }
  function writeFilestat(ptr, node) {
    const view = dv();
    const size = node.type === 'file' ? node.data.length : 4096;
    const mtimNs = BigInt(node.mtime || 0) * 1000000000n;
    view.setBigUint64(ptr + 0, 1n, true);                       // dev
    view.setBigUint64(ptr + 8, BigInt(node.ino || 1), true);    // ino
    view.setUint8(ptr + 16, node.type === 'dir' ? FT.DIR : FT.REG);
    view.setBigUint64(ptr + 24, 1n, true);                      // nlink
    view.setBigUint64(ptr + 32, BigInt(size), true);            // size
    view.setBigUint64(ptr + 40, mtimNs, true);                  // atim
    view.setBigUint64(ptr + 48, mtimNs, true);                  // mtim
    view.setBigUint64(ptr + 56, mtimNs, true);                  // ctim
  }
  // grow() — extend a file to needLen bytes. Files start as subarrays of the
  // packed image, whose bytes BEYOND len belong to other files — so capacity
  // is only reused once the node owns a private buffer (node.owned). Doubling
  // keeps repeated appends (WAL writes) from being O(n^2).
  function grow(node, needLen) {
    if (node.data.length >= needLen) return;
    if (node.owned && node.data.buffer.byteLength - node.data.byteOffset >= needLen) {
      node.data = new Uint8Array(node.data.buffer, node.data.byteOffset, needLen);
      return;
    }
    const grown = new Uint8Array(Math.max(needLen, node.data.length * 2, 64));
    grown.set(node.data);
    node.data = grown.subarray(0, needLen);
    node.owned = true;
  }

  function* iovs(ptr, n) {
    const view = dv();
    for (let i = 0; i < n; i++) {
      const base = ptr + i * 8;
      yield { ptr: view.getUint32(base, true), len: view.getUint32(base + 4, true) };
    }
  }

  function doWrite(h, bytes) {
    if (h.kind === 'stdout') { onStdout(bytes.slice()); return bytes.length; }
    if (h.kind === 'stderr') { onStderr(bytes.slice()); return bytes.length; }
    if (h.kind !== 'file') return -E.BADF;
    const node = h.node;
    const at = h.append ? node.data.length : h.pos;
    grow(node, at + bytes.length);
    node.data.set(bytes, at);
    h.pos = at + bytes.length;
    node.mtime = nowSec();
    return bytes.length;
  }

  const wasi = {
    args_sizes_get(argcPtr, bufSizePtr) {
      const view = dv();
      view.setUint32(argcPtr, argv.length, true);
      view.setUint32(bufSizePtr, argv.reduce((s, a) => s + enc.encode(a).length + 1, 0), true);
      return E.SUCCESS;
    },
    args_get(argvPtr, bufPtr) {
      const view = dv(); const mem = u8();
      let p = bufPtr;
      for (let i = 0; i < argv.length; i++) {
        view.setUint32(argvPtr + i * 4, p, true);
        const b = enc.encode(argv[i]);
        mem.set(b, p); mem[p + b.length] = 0;
        p += b.length + 1;
      }
      return E.SUCCESS;
    },
    environ_sizes_get(countPtr, bufSizePtr) {
      const view = dv();
      view.setUint32(countPtr, envp.length, true);
      view.setUint32(bufSizePtr, envp.reduce((s, a) => s + enc.encode(a).length + 1, 0), true);
      return E.SUCCESS;
    },
    environ_get(envPtr, bufPtr) {
      const view = dv(); const mem = u8();
      let p = bufPtr;
      for (let i = 0; i < envp.length; i++) {
        view.setUint32(envPtr + i * 4, p, true);
        const b = enc.encode(envp[i]);
        mem.set(b, p); mem[p + b.length] = 0;
        p += b.length + 1;
      }
      return E.SUCCESS;
    },
    clock_time_get(id, _precision, outPtr) {
      let ns;
      if (id === 1) {
        const now = (typeof performance !== 'undefined' ? performance.now() : Date.now());
        ns = BigInt(Math.round(now * 1e6));
      } else {
        ns = BigInt(Date.now()) * 1000000n;
      }
      dv().setBigUint64(outPtr, ns, true);
      return E.SUCCESS;
    },
    random_get(ptr, len) {
      const mem = u8();
      const g = (typeof crypto !== 'undefined' && crypto.getRandomValues) ? crypto : null;
      for (let off = 0; off < len; off += 65536) {
        const chunk = mem.subarray(ptr + off, ptr + Math.min(off + 65536, len));
        if (g) g.getRandomValues(chunk);
        else for (let i = 0; i < chunk.length; i++) chunk[i] = (Math.random() * 256) | 0;
      }
      return E.SUCCESS;
    },
    sched_yield() { return E.SUCCESS; },
    proc_exit(code) { throw new GuestExit(num(code)); },

    fd_prestat_get(fd, bufPtr) {
      if (fd !== PREOPEN_FD) return E.BADF;
      const view = dv();
      view.setUint8(bufPtr, 0); // preopentype::dir
      view.setUint32(bufPtr + 4, 1, true); // strlen("/")
      return E.SUCCESS;
    },
    fd_prestat_dir_name(fd, pathPtr, pathLen) {
      if (fd !== PREOPEN_FD) return E.BADF;
      if (pathLen < 1) return E.INVAL;
      u8()[pathPtr] = 0x2f; // '/'
      return E.SUCCESS;
    },
    fd_fdstat_get(fd, bufPtr) {
      const h = fds.get(fd);
      if (!h) return E.BADF;
      const view = dv();
      let ft = FT.CHAR;
      if (h.kind === 'dir') ft = FT.DIR;
      else if (h.kind === 'file') ft = FT.REG;
      view.setUint8(bufPtr, ft);
      view.setUint16(bufPtr + 2, h.append ? FDFLAG.APPEND : 0, true);
      view.setBigUint64(bufPtr + 8, ALL_RIGHTS, true);
      view.setBigUint64(bufPtr + 16, ALL_RIGHTS, true);
      return E.SUCCESS;
    },
    fd_fdstat_set_flags(fd, flags) {
      const h = fds.get(fd);
      if (!h) return E.BADF;
      if (h.kind === 'file') h.append = !!(flags & FDFLAG.APPEND);
      return E.SUCCESS;
    },
    fd_filestat_get(fd, bufPtr) {
      const h = fds.get(fd);
      if (!h) return E.BADF;
      if (h.kind === 'file' || h.kind === 'dir') { writeFilestat(bufPtr, h.node); return E.SUCCESS; }
      // stdio: report a character device
      const view = dv();
      new Uint8Array(memory.buffer, bufPtr, 64).fill(0);
      view.setUint8(bufPtr + 16, FT.CHAR);
      return E.SUCCESS;
    },
    fd_filestat_set_size(fd, size) {
      const h = fds.get(fd);
      if (!h || h.kind !== 'file') return E.BADF;
      size = num(size);
      const node = h.node;
      if (size <= node.data.length) node.data = node.data.subarray(0, size);
      else grow(node, size);
      node.mtime = nowSec();
      return E.SUCCESS;
    },
    fd_close(fd) {
      if (!fds.has(fd)) return E.BADF;
      if (fd <= PREOPEN_FD) { return E.SUCCESS; } // keep stdio + preopen alive
      fds.delete(fd);
      return E.SUCCESS;
    },
    fd_seek(fd, offset, whence, outPtr) {
      const h = fds.get(fd);
      if (!h) return E.BADF;
      if (h.kind !== 'file') return h.kind === 'dir' ? E.BADF : E.SPIPE;
      offset = num(offset);
      let pos;
      if (whence === 0) pos = offset;                       // SET
      else if (whence === 1) pos = h.pos + offset;          // CUR
      else if (whence === 2) pos = h.node.data.length + offset; // END
      else return E.INVAL;
      if (pos < 0) return E.INVAL;
      h.pos = pos;
      dv().setBigUint64(outPtr, BigInt(pos), true);
      return E.SUCCESS;
    },
    fd_read(fd, iovsPtr, iovsLen, nreadPtr) {
      const h = fds.get(fd);
      if (!h) return E.BADF;
      // Streaming stdin (protocol mode): the session's frontend bytes arrive
      // incrementally. When nothing is queued and the stream is still open,
      // return a Promise — under a JSPI Suspending wrapper this SUSPENDS the
      // guest's blocking read until the host pushes the next pgwire frame
      // (the browser-worker analog of the wasmtime host's blocking pipe).
      // Without JSPI this arm is never taken (worker.js falls back to the
      // --single engine); the fixed stdinBytes path below is unchanged.
      if (h.kind === 'stdin' && stdinStream) {
        const attempt = () => {
          const view = dv(); const mem = u8();
          let total = 0;
          for (const { ptr, len } of iovs(iovsPtr, iovsLen)) {
            if (len === 0) continue;
            const chunk = stdinStream.take(len);
            if (!chunk || chunk.length === 0) break;
            mem.set(chunk, ptr);
            total += chunk.length;
            if (chunk.length < len) break;
          }
          view.setUint32(nreadPtr, total, true);
          return E.SUCCESS;
        };
        if (stdinStream.ready() || stdinStream.isEof()) return attempt();
        return stdinStream.wait().then(attempt);
      }
      let total = 0;
      const mem = u8();
      for (const { ptr, len } of iovs(iovsPtr, iovsLen)) {
        if (len === 0) continue;
        let chunk;
        if (h.kind === 'stdin') {
          const k = Math.min(len, stdin.length - stdinPos);
          if (k <= 0) break;
          chunk = stdin.subarray(stdinPos, stdinPos + k);
          stdinPos += k;
        } else if (h.kind === 'file') {
          const node = h.node;
          const k = Math.min(len, node.data.length - h.pos);
          if (k <= 0) break;
          chunk = node.data.subarray(h.pos, h.pos + k);
          h.pos += k;
        } else {
          return E.BADF;
        }
        mem.set(chunk, ptr);
        total += chunk.length;
        if (chunk.length < len) break;
      }
      dv().setUint32(nreadPtr, total, true);
      return E.SUCCESS;
    },
    fd_pread(fd, iovsPtr, iovsLen, offset, nreadPtr) {
      const h = fds.get(fd);
      if (!h || h.kind !== 'file') return E.BADF;
      let off = num(offset);
      let total = 0;
      const mem = u8();
      const node = h.node;
      for (const { ptr, len } of iovs(iovsPtr, iovsLen)) {
        if (len === 0) continue;
        const k = Math.min(len, Math.max(0, node.data.length - off));
        if (k <= 0) break;
        mem.set(node.data.subarray(off, off + k), ptr);
        off += k; total += k;
        if (k < len) break;
      }
      dv().setUint32(nreadPtr, total, true);
      return E.SUCCESS;
    },
    fd_write(fd, iovsPtr, iovsLen, nwrittenPtr) {
      const h = fds.get(fd);
      if (!h) return E.BADF;
      const mem = u8();
      let total = 0;
      for (const { ptr, len } of iovs(iovsPtr, iovsLen)) {
        if (len === 0) continue;
        const r = doWrite(h, mem.subarray(ptr, ptr + len));
        if (r < 0) return -r;
        total += r;
      }
      dv().setUint32(nwrittenPtr, total, true);
      return E.SUCCESS;
    },
    fd_pwrite(fd, iovsPtr, iovsLen, offset, nwrittenPtr) {
      const h = fds.get(fd);
      if (!h || h.kind !== 'file') return E.BADF;
      let off = num(offset);
      const mem = u8();
      const node = h.node;
      let total = 0;
      for (const { ptr, len } of iovs(iovsPtr, iovsLen)) {
        if (len === 0) continue;
        grow(node, off + len);
        node.data.set(mem.subarray(ptr, ptr + len), off);
        off += len; total += len;
      }
      node.mtime = nowSec();
      dv().setUint32(nwrittenPtr, total, true);
      return E.SUCCESS;
    },
    fd_sync(fd) { return fds.has(fd) ? E.SUCCESS : E.BADF; },
    fd_datasync(fd) { return fds.has(fd) ? E.SUCCESS : E.BADF; },
    fd_readdir(fd, bufPtr, bufLen, cookie, bufusedPtr) {
      const h = fds.get(fd);
      if (!h || h.kind !== 'dir') return h ? E.NOTDIR : E.BADF;
      // Snapshot on first read (cookie 0) so pagination is stable even if the
      // guest mutates the directory between calls.
      cookie = num(cookie);
      if (cookie === 0 || !h.entries) h.entries = vfs.listDir(h.path);
      const view = dv(); const mem = u8();
      let used = 0;
      for (let i = cookie; i < h.entries.length; i++) {
        const { name, node } = h.entries[i];
        const nameBytes = enc.encode(name);
        const entrySize = 24 + nameBytes.length;
        if (used + 24 >= bufLen) { used = bufLen; break; } // no room for a header: signal full
        const p = bufPtr + used;
        view.setBigUint64(p, BigInt(i + 1), true);                 // d_next cookie
        view.setBigUint64(p + 8, BigInt(node.ino || 1), true);     // d_ino
        view.setUint32(p + 16, nameBytes.length, true);            // d_namlen
        view.setUint8(p + 20, node.type === 'dir' ? FT.DIR : FT.REG);
        view.setUint8(p + 21, 0); view.setUint8(p + 22, 0); view.setUint8(p + 23, 0);
        const room = bufLen - used - 24;
        mem.set(nameBytes.subarray(0, Math.min(nameBytes.length, room)), p + 24);
        used += Math.min(entrySize, 24 + room);
        if (entrySize > 24 + room) break; // truncated: caller grows buffer
      }
      view.setUint32(bufusedPtr, used, true);
      return E.SUCCESS;
    },
    path_open(dirfd, _dirflags, pathPtr, pathLen, oflags, _rightsBase, _rightsInh, fdflags, outPtr) {
      const path = atPath(dirfd, pathPtr, pathLen);
      if (path === null) return E.BADF;
      let node = vfs.get(path);
      if (!node) {
        if (!(oflags & OFLAG.CREAT)) return E.NOENT;
        if (oflags & OFLAG.DIRECTORY) return E.NOENT;
        node = vfs.create(path);
      } else if ((oflags & OFLAG.CREAT) && (oflags & OFLAG.EXCL)) {
        return E.EXIST;
      }
      let fd;
      if (node.type === 'dir') {
        fd = nextFd++;
        fds.set(fd, { kind: 'dir', path: vfs._norm(path), node });
      } else {
        if (oflags & OFLAG.DIRECTORY) return E.NOTDIR;
        if (oflags & OFLAG.TRUNC) { node.data = new Uint8Array(0); node.mtime = nowSec(); }
        fd = nextFd++;
        fds.set(fd, {
          kind: 'file', path: vfs._norm(path), node,
          pos: 0, append: !!(num(fdflags) & FDFLAG.APPEND),
        });
      }
      dv().setUint32(outPtr, fd, true);
      return E.SUCCESS;
    },
    path_filestat_get(dirfd, _flags, pathPtr, pathLen, bufPtr) {
      const path = atPath(dirfd, pathPtr, pathLen);
      if (path === null) return E.BADF;
      const node = vfs.get(path);
      if (!node) return E.NOENT;
      writeFilestat(bufPtr, node);
      return E.SUCCESS;
    },
    path_create_directory(dirfd, pathPtr, pathLen) {
      const path = atPath(dirfd, pathPtr, pathLen);
      if (path === null) return E.BADF;
      return vfs.mkdir(path);
    },
    path_remove_directory(dirfd, pathPtr, pathLen) {
      const path = atPath(dirfd, pathPtr, pathLen);
      if (path === null) return E.BADF;
      return vfs.rmdir(path);
    },
    path_unlink_file(dirfd, pathPtr, pathLen) {
      const path = atPath(dirfd, pathPtr, pathLen);
      if (path === null) return E.BADF;
      return vfs.unlink(path);
    },
    path_rename(dirfd, fromPtr, fromLen, toDirfd, toPtr, toLen) {
      const from = atPath(dirfd, fromPtr, fromLen);
      const to = atPath(toDirfd, toPtr, toLen);
      if (from === null || to === null) return E.BADF;
      return vfs.rename(from, to);
    },
    path_readlink(_dirfd, _pathPtr, _pathLen, _bufPtr, _bufLen, _usedPtr) {
      return E.INVAL; // the packed image contains no symlinks (built with cp -RL)
    },
    poll_oneoff(inPtr, outPtr, nsubs, neventsPtr) {
      // No blocking primitive here (a COEP-less browser worker cannot build a
      // SharedArrayBuffer to Atomics.wait on). Report every clock subscription
      // as already fired; fd subscriptions report the fd ready. --single never
      // waits on this path (see notes/wasm-boot-lane.md WaitEventSet arm).
      //
      // Protocol mode (stdinStream set): a FD_READ subscription on stdin
      // reports fired ONLY when frontend bytes are queued (or the stream hit
      // EOF) — this keeps the guest's emulated-noblock probe (fdnb::poll_ready
      // over wasi-libc poll(2)) faithful: an empty stdin answers "not ready"
      // (rc 0 after the zero-timeout clock fires) instead of luring a
      // would-be-nonblocking read into a JSPI suspend. Fired events are
      // written COMPACTLY per the WASI contract (nevents = fired count).
      const view = dv();
      let fired = 0;
      for (let i = 0; i < nsubs; i++) {
        const sub = inPtr + i * 48;
        const userdataLo = view.getUint32(sub, true);
        const userdataHi = view.getUint32(sub + 4, true);
        const tag = view.getUint8(sub + 8);
        if (stdinStream && tag === 1 /* fd_read */) {
          const subFd = view.getUint32(sub + 16, true);
          if (subFd === 0 && !stdinStream.ready() && !stdinStream.isEof()) continue;
        }
        const evt = outPtr + fired * 32;
        fired++;
        view.setUint32(evt, userdataLo, true);
        view.setUint32(evt + 4, userdataHi, true);
        view.setUint16(evt + 8, E.SUCCESS, true);
        view.setUint8(evt + 10, tag);
        view.setBigUint64(evt + 16, 0n, true); // nbytes
        view.setUint16(evt + 24, 0, true);     // flags
      }
      view.setUint32(neventsPtr, fired, true);
      return E.SUCCESS;
    },
    sock_recv() { return E.NOTSUP; },
    sock_send() { return E.NOTSUP; },
  };

  return { vfs, wasi, setMemory: (m) => { memory = m; } };
}

// Instantiate + run the module to completion. Returns { exitCode }.
export async function run({ wasmModule, image, manifest, vfs, stdinBytes, onStdout, onStderr, argv, env }) {
  const h = makeWasi({ image, manifest, vfs, stdinBytes, onStdout, onStderr, argv, env });
  const instance = await WebAssembly.instantiate(wasmModule, { wasi_snapshot_preview1: h.wasi });
  const ex = instance.exports;
  h.setMemory(ex.memory);
  let exitCode = 0;
  try {
    ex._start();
  } catch (e) {
    if (e instanceof GuestExit) exitCode = e.code;
    else throw e;
  }
  return { exitCode, memoryBytes: ex.memory.buffer.byteLength };
}

export { Vfs, GuestExit };
