// storage-worker.js — the dedicated STORAGE COORDINATOR for the `--fs broker` lane.
//
// One worker, one store, and nothing else on the thread. It opens a repacked store, seeds it
// from the packed image if it is fresh, attaches every channel the driver minted, and then
// parks in the broker's blocking `serveForever()` loop answering file requests from the
// process instance and from every wasi-thread worker. That is what replaces "every worker
// builds its own Vfs from its own copy of vfs.img" — the limitation the previous checkpoint
// left behind and the reason a second backend was impossible.
//
// TWO THINGS ABOUT THE BLOCKING LOOP
//
//   1. Once `serveForever()` is entered this thread NEVER reaches its event loop again, so no
//      postMessage can be delivered to it. Every channel must be attached BEFORE the loop, and
//      the only way out is `doorbell.requestStop()` from another agent — which is exactly why
//      the doorbell lives in a SharedArrayBuffer. The `stop` message below therefore only ever
//      lands in the window before the loop starts; the driver's real stop is the doorbell.
//   2. postMessage OUT still works while parked (sending does not need this thread's event
//      loop), so the broker's detach log and the final `stopped` reach the driver.
//
// THE PORT (`options.port`). The store core is port-agnostic, so the SAME coordinator serves
// two very different lanes:
//
//   'memory'  (default) `MemoryRepackedPort` — the store lives in this worker's heap and dies
//             with it. Every run starts from the packed image. This is what Node uses (Node
//             has no OPFS) and what every lane used before this change.
//   'opfs'    `OpfsRepackedPort` over ONE dedicated OPFS directory (`options.opfsDir`, created
//             under the OPFS root). Four `FileSystemSyncAccessHandle`s, and the datadir
//             SURVIVES the page. Requires a scope where `createSyncAccessHandle()` actually
//             succeeds — a dedicated worker is such a scope, the window main thread is not.
//
// FRESH vs EXISTING. The store itself opens either way: `RepackedVfs.open` bootstraps an empty
// store from an empty directory and recovers an activated one from a populated directory,
// without the caller saying which it expected. So the coordinator does not ask the STORE
// whether it is new, it asks the DATADIR: `/pgdata` present => this is a reopen, skip the seed
// and report `restored: true`; absent => seed the packed image in, exactly as before. That
// keeps the decision on the only thing that matters to the guest (is there a datadir to boot
// from) rather than on a store-format detail.
//
// DURABILITY (`options.durability`). The broker's `fd_sync` is ALREADY a store-wide
// `strictSync()` — the guest's own fsyncs are durability boundaries in both modes, and the
// seed and the close are strict in both modes. The mode selects what happens BETWEEN those:
//
//   'relaxed' (default) nothing. Writes accumulate in the arena and the metadata log and reach
//             the platform when the guest fsyncs, when the store amortizes, or at close. A
//             termination that skips the close can lose the tail; recovery keeps the longest
//             valid metadata-log prefix and never crosses extent owners (the library README's
//             documented loss window).
//   'strict'  every MUTATING broker request is followed by a `strictSync()`. This is the
//             broker-level reading of what the package's PGlite adapter does in strict mode
//             (`opfs-repacked-fs.ts`: strict flushes arena-before-metadata on every awaited
//             host sync). A real postmaster has no host sync to hang that off — nothing awaits
//             a query completion here — so the request boundary is the only equivalent seam.
//             `strictSync()` is a no-op on a clean store, so read traffic costs nothing.
//
// SEEDING. The packed image is the same { dirs, files:[{path,off,len,mode,mtime}] } manifest
// over one concatenated byte image that pgrust-wasi.js's `Vfs` reads (wasm/pack-vfs.mjs
// writes it). Every directory is created and every file written once, in manifest order.
//
// MOUNTS (`options.mounts`). A list of { prefix, port } the storage OWNER declares — the guest is
// never told a mount exists. Each one serves that path prefix from a SECOND store, and the
// coordinator hands the broker one `MountedRepackedVfs` over the lot; the broker cannot tell it from
// a single store. A `memory` mount is declared VOLATILE, so a store-wide `strictSync()` skips it:
// there is nothing durable behind it, and charging every guest fsync for it would buy nothing.
// The point of the arrangement is that a symlink can cross the boundary — `pg_tblspc/<oid>` lives in
// the durable root and points at the mount — and the per-port file counts reported at stop are what
// prove a relation file really landed in the mount's store rather than in the root's.
//
// EXTENT SIZE. Default 8 KiB, the store's minimum, rather than its own 64 KiB default: the
// datadir is 1,477 mostly-tiny files and each one rounds up to a whole extent, so 64 KiB
// extents cost 107 MiB of arena for 41 MB of data where 8 KiB costs 43 MiB. Override with
// `options.extentSize`. It is an identity of an EXISTING store: reopening with a different
// one raises `ExtentSizeMismatchError`, which is reported by name like every other store error.

const IS_NODE = typeof process !== 'undefined' && !!process.versions && !!process.versions.node;
const nodeWt = IS_NODE ? await import('node:worker_threads') : null;
const port = IS_NODE ? nodeWt.parentPort : self;

function post(msg) {
  port.postMessage(msg);
}

function onMessage(cb) {
  if (IS_NODE) port.on('message', cb);
  else self.addEventListener('message', (e) => cb(e.data));
}

// The store rejects a path that is not absolute and canonical; the manifest's already are, but
// a hand-written one need not be.
function normalize(bundle, path) {
  return bundle.normalizeWasiPath(String(path));
}

let doorbell = null;

/**
 * Recursive file count + byte total under one path, for the before/after datadir report.
 *
 * `lstat`, and symlinks are counted but never descended into: a tablespace link points at a whole
 * second tree, and following it would count that tree twice (once here, once in its own port's
 * walk) and would break outright on a dangling link.
 */
function walk(vfs, path) {
  let files = 0;
  let bytes = 0;
  let links = 0;
  const stack = [path];
  while (stack.length) {
    const dir = stack.pop();
    for (const name of vfs.readdir(dir)) {
      const child = dir === '/' ? `/${name}` : `${dir}/${name}`;
      const stat = vfs.lstat(child);
      if (stat.kind === 'directory') stack.push(child);
      else if (stat.kind === 'symlink') links += 1;
      else {
        files += 1;
        bytes += Number(stat.size);
      }
    }
  }
  return { files, bytes, links };
}

/** `true` iff `path` does not exist; any other rejection is the caller's problem. */
function absent(bundle, vfs, path) {
  try {
    vfs.stat(path);
    return false;
  } catch (e) {
    if (e && e.code === bundle.WASI_ERRNO.NOENT) return true;
    throw e;
  }
}

/**
 * Open the OPFS directory this store owns, optionally emptying it first.
 *
 * `reset` is the "start over" switch the persistence lanes need: the store fails CLOSED on a
 * directory whose format identity it does not accept (`StoreRecreationRequiredError`) and the
 * only sanctioned repair is deleting the WHOLE directory, so a reset removes every entry
 * rather than the four files it happens to know about.
 */
async function openOpfsDirectory(name, reset) {
  if (typeof navigator === 'undefined' || !navigator.storage || !navigator.storage.getDirectory) {
    throw new Error('OPFS is unavailable in this scope (navigator.storage.getDirectory missing)');
  }
  const root = await navigator.storage.getDirectory();
  if (reset) {
    // removeEntry on the ROOT, not the directory: `recursive` on a directory whose sync access
    // handles a previous run may still hold is the one call that can fail; a whole-directory
    // removal followed by a fresh create is the clean slate the store documents.
    try {
      await root.removeEntry(name, { recursive: true });
    } catch (e) {
      if (!e || e.name !== 'NotFoundError') throw e;
    }
  }
  return root.getDirectoryHandle(name, { create: true });
}

// Every request opcode that can CHANGE the store. `open` is in the list because O_CREAT/O_TRUNC
// mutate metadata before a single byte is written; `close` is not, because it only drops a
// descriptor. A `strictSync()` on a clean store returns without touching a handle, so a read
// that follows a read costs nothing even here.
const MUTATING_OPS = ['write', 'truncate', 'unlink', 'rename', 'mkdir', 'rmdir', 'open'];

/**
 * The strict-durability view of a store, for the broker to own.
 *
 * The broker takes a `RepackedVfs` and calls its public methods; it offers no per-request hook,
 * so strict mode is expressed where it belongs — between the store and its only caller. A Proxy
 * rather than a hand-written facade so a future opcode cannot silently escape the wrapper: every
 * property is forwarded, and only the names in MUTATING_OPS gain the trailing sync. Methods are
 * bound to the real instance, whose private fields the Proxy could never carry.
 */
function strictView(vfs) {
  const bound = new Map();
  return new Proxy(vfs, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, target);
      if (typeof value !== 'function') return value;
      let wrapper = bound.get(prop);
      if (!wrapper) {
        wrapper = MUTATING_OPS.includes(prop)
          ? (...args) => {
              const result = value.apply(target, args);
              target.strictSync();
              return result;
            }
          : value.bind(target);
        bound.set(prop, wrapper);
      }
      return wrapper;
    },
  });
}

async function boot(msg) {
  const bundle = await import(msg.bundleUrl);
  const image = msg.image instanceof Uint8Array ? msg.image : new Uint8Array(msg.image);
  const options = msg.options || {};
  const extentSize = options.extentSize || 8192;
  const portKind = options.port || 'memory';
  const durability = options.durability || 'relaxed';
  const opfsDir = options.opfsDir || 'pgrust-pgdata';
  if (portKind !== 'memory' && portKind !== 'opfs') throw new Error(`unknown storage port ${portKind}`);
  if (durability !== 'relaxed' && durability !== 'strict') {
    throw new Error(`unknown storage durability ${durability}`);
  }

  const tOpen = Date.now();
  let storePort;
  if (portKind === 'opfs') {
    const directory = await openOpfsDirectory(opfsDir, options.reset === true);
    storePort = new bundle.OpfsRepackedPort(directory);
  } else {
    storePort = new bundle.MemoryRepackedPort();
  }
  const rootVfs = await bundle.RepackedVfs.open(storePort, { extentSize });

  // Every declared mount is its own store on its own port. `durable: false` for a memory mount is a
  // DECLARATION, not an inference: the library never guesses durability from the port.
  const mountSpecs = Array.isArray(options.mounts) ? options.mounts : [];
  const mounted = [];
  for (const spec of mountSpecs) {
    const prefix = normalize(bundle, spec.prefix);
    const kind = spec.port || 'memory';
    if (kind !== 'memory') throw new Error(`unknown mount port ${kind} at ${prefix}`);
    const mountVfs = await bundle.RepackedVfs.open(new bundle.MemoryRepackedPort(), { extentSize });
    mounted.push({ prefix, port: kind, durable: false, vfs: mountVfs });
  }
  const vfs =
    mounted.length === 0
      ? rootVfs
      : new bundle.MountedRepackedVfs({
          root: rootVfs,
          mounts: mounted.map((m) => ({ prefix: m.prefix, vfs: m.vfs, durable: m.durable })),
        });
  const openMs = Date.now() - tOpen;
  const nowMs = BigInt(Date.now());
  const EEXIST = bundle.WASI_ERRNO.EXIST;

  // THE FRESH/EXISTING FORK. See the header: the datadir decides, not the store.
  const fresh = absent(bundle, vfs, '/pgdata');

  // `mkdir` reports EEXIST for a path that is already there even with `recursive`, so an
  // idempotent seed has to swallow exactly that one errno and nothing else.
  const mkdir = (path) => {
    try {
      vfs.mkdir(path, { recursive: true, nowMs });
      return 1;
    } catch (e) {
      if (e && e.code === EEXIST) return 0;
      throw e;
    }
  };

  const tSeed = Date.now();
  let dirs = 0;
  let files = 0;
  let bytes = 0;
  if (fresh) {
    for (const d of msg.manifest.dirs || []) {
      const path = normalize(bundle, d);
      if (path !== '/') dirs += mkdir(path);
    }
    for (const f of msg.manifest.files || []) {
      const path = normalize(bundle, f.path);
      const parent = path.replace(/\/[^/]*$/, '');
      if (parent) dirs += mkdir(parent);
      vfs.writeFile(path, image.subarray(f.off, f.off + f.len), { nowMs, mode: f.mode });
      files += 1;
      bytes += f.len;
    }
    // One strict sync so the seed is a durability boundary before any backend touches it.
    vfs.strictSync();
  }
  const seedMs = Date.now() - tSeed;
  const seeded = walk(vfs, '/pgdata');

  doorbell = bundle.RepackedDoorbell.attach(msg.doorbell);
  const broker = new bundle.RepackedSyncBroker({
    vfs: durability === 'strict' ? strictView(vfs) : vfs,
    doorbell,
    // A detach is the broker dropping a client (a protocol violation, or the host asking): it
    // is never routine, so it goes to the driver rather than to a console nobody reads.
    log: (text) => post({ type: 'storage-log', text }),
  });
  for (const transfer of msg.channels) broker.attach(bundle.RepackedChannel.attach(transfer));

  const metrics = vfs.metrics();
  post({
    type: 'storage-ready',
    port: portKind,
    opfsDir: portKind === 'opfs' ? opfsDir : null,
    durability,
    restored: !fresh,
    openMs,
    files,
    dirs,
    bytes,
    seedMs,
    datadirFiles: seeded.files,
    datadirBytes: seeded.bytes,
    extentSize,
    arenaBytes: Number(metrics.totalExtents) * extentSize,
    mounts: mounted.map((m) => ({ prefix: m.prefix, port: m.port, durable: m.durable })),
    channels: broker.attachedIds(),
  });

  // Turn the event loop once so `storage-ready` is actually flushed: the very next statement
  // parks this thread and it never reaches its event loop again.
  await new Promise((r) => setTimeout(r, 0));
  broker.serveForever();

  // Only `doorbell.requestStop()` gets us here.
  broker.detachAll('storage coordinator stopping');
  // The proof that this really was ONE store: every backend wrote into it, so the datadir the
  // coordinator holds at the end is BIGGER than the one it seeded. (Under --fs copy those writes
  // died with each worker's private Vfs.)
  const after = walk(vfs, '/pgdata');
  // Strict sync BEFORE close, explicitly. `close()` performs one itself on an open store, but
  // the two are separately timed here and an OPFS store's whole point is that the next open
  // finds this state — including finding its four handles RELEASED, so a later open does not
  // meet `StoreOwnedError`.
  const tSync = Date.now();
  vfs.strictSync();
  const syncMs = Date.now() - tSync;
  // The store's own flush counters are the only OBJECTIVE evidence that `durability` did
  // anything: relaxed reaches the platform on the guest's fsyncs and on amortization, strict
  // adds one arena+metadata pair per mutating request.
  const flushes = vfs.metrics().flushes;
  // PER-PORT counts, walked against each underlying store directly rather than through the
  // composite: which store a file actually landed in is the only thing that tells a real mount
  // apart from a path-prefix illusion. The root store holds an EMPTY placeholder directory at each
  // prefix (a name, so a listing of its parent shows it), so it contributes nothing there.
  const ports = [
    { name: 'root', port: portKind, durable: true, root: '/', ...walk(rootVfs, '/') },
    ...mounted.map((m) => ({ name: m.prefix, port: m.port, durable: m.durable, root: '/', ...walk(m.vfs, '/') })),
  ];
  const tClose = Date.now();
  vfs.close();
  post({
    type: 'storage-stopped',
    port: portKind,
    durability,
    syncMs,
    flushes,
    closeMs: Date.now() - tClose,
    seededFiles: seeded.files,
    seededBytes: seeded.bytes,
    datadirFiles: after.files,
    datadirBytes: after.bytes,
    ports,
  });
}

/**
 * The store's typed failures carry an actionable remedy in their NAME (StoreOwnedError: another
 * live owner; StoreRecreationRequiredError: delete the directory; ExtentSizeMismatchError: use
 * the stored size; CorruptStoreError: recreate or restore). Flattening them to a stack string
 * throws that away, so the name travels beside the message and the page prints the remedy.
 */
function describe(e) {
  return {
    errorName: e && e.name ? String(e.name) : 'Error',
    message: String(e && e.stack ? e.stack : e),
  };
}

onMessage((msg) => {
  if (!msg || !msg.kind) return;
  if (msg.kind === 'boot') {
    boot(msg).catch((e) => post({ type: 'storage-error', ...describe(e) }));
    return;
  }
  if (msg.kind === 'stop') {
    // Reachable only before the loop is entered (see the header); the driver's own
    // `doorbell.requestStop()` is what actually stops a parked coordinator.
    if (doorbell) doorbell.requestStop();
    return;
  }
  post({ type: 'storage-log', text: `unknown storage message ${JSON.stringify(msg.kind)}` });
});
