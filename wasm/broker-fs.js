// broker-fs.js — the `--fs broker` seam: one instance's WASI FILE calls, routed to the
// coordinator worker's single repacked store instead of to a private copy of the packed image.
//
// WHY THIS EXISTS. Until now every worker built its OWN `Vfs` from its own copy of
// wasm/assets/vfs.img, so the process instance and the session instance had INDEPENDENT
// filesystems that diverged the moment either wrote (the KNOWN LIMIT at the top of
// threads-host.js). A second backend is impossible on that footing. Here the store lives
// alone in wasm/storage-worker.js and every other thread reaches it over a
// SharedArrayBuffer channel, blocking in Atomics.wait for the answer — which is the only
// shape that works, because a thread running wasm parks in futexes and can never observe a
// promise.
//
// WHAT THIS FILE IS. Nothing but wiring. The two halves both come from
// @pgxsinkit/pglite-opfs-repacked:
//
//   RepackedSyncClient    the synchronous half of the broker protocol (blocks on the channel)
//   createWasiPreview1Fs  the WASI preview1 filesystem adapter over that client
//
// The adapter owns fd 3 (the "/" preopen) and every fd >= 4, and `compose(base)` merges it
// over the host's own WASI object so fds 0/1/2 and every non-filesystem import —
// clock_time_get, poll_oneoff, random_get, proc_exit, thread-spawn — stay EXACTLY as
// threads-host.js built them.
//
// THE BUNDLE. The library is TypeScript in another repo; what we import here is its
// self-contained ESM redistribution bundle (`dist/browser-bundle.js`), which has no bare
// specifiers and no static imports at all, so it loads with no bundler in both hosts:
//
//   Node     import('file:///…/pgrust/wasm/vendor/pglite-opfs-repacked.js')
//   browser  import('http://localhost:8081/vendor/pglite-opfs-repacked.js')   (serve-coi.mjs)
//
// Both resolve from the SAME expression — `new URL('./vendor/pglite-opfs-repacked.js',
// import.meta.url)` — because the driver and the page sit at the same place relative to it.
// wasm/vendor/ is gitignored like wasm/assets/; link it in first:
//
//   mkdir -p wasm/vendor && ln -sfn \
//     /path/to/pgxsinkit/packages/pglite-opfs-repacked/dist/browser-bundle.js \
//     wasm/vendor/pglite-opfs-repacked.js
//   # the bundle is emitted by `bun run build:public-packages` in the pgxsinkit repo
//
// Override with PGRUST_REPACKED_BUNDLE (Node) or ?bundle= (browser).

export const VENDOR_BUNDLE_PATH = './vendor/pglite-opfs-repacked.js';

// A guest that talks to a coordinator which has gone away must fail loudly rather than park
// forever; 60s is far longer than any real request and far shorter than "never".
export const DEFAULT_REQUEST_TIMEOUT_MS = 60000;

/** The bundle URL both hosts resolve from their own module URL. */
export function repackedBundleUrl(base) {
  return new URL(VENDOR_BUNDLE_PATH, base).href;
}

/**
 * Load the library bundle. Kept as a function (rather than a static import) because the URL
 * is a runtime decision and because a worker must be able to load it before it builds a host.
 */
export async function loadRepackedBundle(url) {
  return import(url);
}

/**
 * Build one instance's filesystem seam: a client on `channel`, a WASI adapter over it, and a
 * `compose` the host applies to its own WASI object.
 *
 * `memory` is the SHARED WebAssembly.Memory every instance imports. The adapter re-reads
 * `memory.buffer` on every call by design: a shared memory grows underneath the host and a
 * cached view goes stale.
 */
export function createBrokerFs({ bundle, channel, memory, label = 'guest', onLog, requestTimeoutMs, fdBase }) {
  const attached = bundle.RepackedChannel.attach(channel);
  const client = new bundle.RepackedSyncClient(attached, {
    requestTimeoutMs: requestTimeoutMs || DEFAULT_REQUEST_TIMEOUT_MS,
  });
  const adapter = bundle.createWasiPreview1Fs({
    client,
    memory: () => memory.buffer,
    // Per-agent fd base (wasm/threads-host.js, "PIPE FDS, AND THE FD NUMBER
    // PLAN"): the adapter owns fd 3 and every fd >= fdBase, and N instances
    // sharing one guest fd table must not allocate overlapping fd numbers.
    // Undefined keeps the library default (preopenFd + 1 = 4).
    fdBase,
    // A JS exception thrown out of a WASI import leaves wasm as a foreign exception, which the
    // guest's nounwind frames turn into a bare `RuntimeError: unreachable` with no Rust message
    // at all. The adapter already converts every throw into EIO; this is where the stack we
    // would otherwise never see comes out.
    onError: (call, cause) => {
      const detail = cause && cause.stack ? cause.stack : String(cause);
      if (onLog) onLog(`[${label}] wasi ${call} failed -> EIO: ${detail}`);
      else console.error(`[${label}] wasi ${call} failed -> EIO: ${detail}`);
    },
  });
  return {
    channelId: attached.id,
    client,
    adapter,
    compose: (base) => adapter.compose(base),
    // Called when a guest thread returns: without it the coordinator holds this thread's store
    // descriptors until the whole channel detaches.
    closeAll: () => adapter.closeAll(),
    openFdCount: () => adapter.openFdCount(),
  };
}
