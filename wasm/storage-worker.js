// storage-worker.js — the dedicated STORAGE COORDINATOR for the `--fs broker` lane.
//
// One worker, one store, and nothing else on the thread. It seeds a repacked store from the
// packed image, attaches every channel the driver minted, and then parks in the broker's
// blocking `serveForever()` loop answering file requests from the process instance and from
// every wasi-thread worker. That is what replaces "every worker builds its own Vfs from its
// own copy of vfs.img" — the limitation the previous checkpoint left behind and the reason a
// second backend was impossible.
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
// SEEDING. The packed image is the same { dirs, files:[{path,off,len,mode,mtime}] } manifest
// over one concatenated byte image that pgrust-wasi.js's `Vfs` reads (wasm/pack-vfs.mjs
// writes it). Every directory is created and every file written once, in manifest order.
//
// EXTENT SIZE. Default 8 KiB, the store's minimum, rather than its own 64 KiB default: the
// datadir is 1,477 mostly-tiny files and each one rounds up to a whole extent, so 64 KiB
// extents cost 107 MiB of arena for 41 MB of data where 8 KiB costs 43 MiB. Override with
// `options.extentSize`.

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

/** Recursive file count + byte total under one path, for the before/after datadir report. */
function walk(vfs, path) {
  let files = 0;
  let bytes = 0;
  const stack = [path];
  while (stack.length) {
    const dir = stack.pop();
    for (const name of vfs.readdir(dir)) {
      const child = dir === '/' ? `/${name}` : `${dir}/${name}`;
      const stat = vfs.stat(child);
      if (stat.kind === 'directory') stack.push(child);
      else {
        files += 1;
        bytes += Number(stat.size);
      }
    }
  }
  return { files, bytes };
}

async function boot(msg) {
  const bundle = await import(msg.bundleUrl);
  const image = msg.image instanceof Uint8Array ? msg.image : new Uint8Array(msg.image);
  const options = msg.options || {};
  const extentSize = options.extentSize || 8192;

  const t0 = Date.now();
  const vfs = await bundle.RepackedVfs.open(new bundle.MemoryRepackedPort(), { extentSize });
  const nowMs = BigInt(Date.now());
  const EEXIST = bundle.WASI_ERRNO.EXIST;

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

  let dirs = 0;
  let files = 0;
  let bytes = 0;
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
  const seedMs = Date.now() - t0;
  const seeded = walk(vfs, '/pgdata');

  doorbell = bundle.RepackedDoorbell.attach(msg.doorbell);
  const broker = new bundle.RepackedSyncBroker({
    vfs,
    doorbell,
    // A detach is the broker dropping a client (a protocol violation, or the host asking): it
    // is never routine, so it goes to the driver rather than to a console nobody reads.
    log: (text) => post({ type: 'storage-log', text }),
  });
  for (const transfer of msg.channels) broker.attach(bundle.RepackedChannel.attach(transfer));

  const metrics = vfs.metrics();
  post({
    type: 'storage-ready',
    files,
    dirs,
    bytes,
    seedMs,
    datadirFiles: seeded.files,
    datadirBytes: seeded.bytes,
    extentSize,
    arenaBytes: Number(metrics.totalExtents) * extentSize,
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
  vfs.close();
  post({
    type: 'storage-stopped',
    seededFiles: seeded.files,
    seededBytes: seeded.bytes,
    datadirFiles: after.files,
    datadirBytes: after.bytes,
  });
}

onMessage((msg) => {
  if (!msg || !msg.kind) return;
  if (msg.kind === 'boot') {
    boot(msg).catch((e) =>
      post({ type: 'storage-error', message: String(e && e.stack ? e.stack : e) }),
    );
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
