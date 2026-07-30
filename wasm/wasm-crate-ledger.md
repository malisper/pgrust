# wasm32-wasip1 crate ledger (P5)

Owned by `wasm/wasm-build.sh` (the blocking gate): every workspace crate
is either BUILT by that script for wasm32-wasip1 with panic=unwind, or listed
here with a reason. **RATCHET-ONLY: rows may only be removed** (a crate that
becomes buildable leaves the table); adding a row means a wasm regression and
fails review. A build that silently drops crates fails the gate by
construction — the include set is computed as workspace-members minus this
table, so an unlisted breakage breaks the build.

Coverage (2026-07-17, branch wasm/p5-boot): **788/789 workspace crates
compile** for wasm32-wasip1 with panic=unwind, and the `postgres` binary
(main_main) **links** and boots a prebuilt datadir under wasmtime (the P5
boot gate). The toolchain
increment's six direct blockers (miscinit, pg_locale, pqcomm, hba, adt_xml,
zstd-chain) were cleared with per-site cfg arms and the `ip::sys` socket
compat module; the one remaining row is below.

## Remaining rows

| crate | class | reason |
|---|---|---|
| be_secure_openssl | direct | vendored OpenSSL C (openssl-src) does not build for wasm32; consumers fenced via the `ssl` feature + target gate (be_secure, backend_status) — the crate is already absent from the wasm dependency graph, only the workspace member itself cannot be check-built |

## Boot-increment functional notes (not compile blockers)

Compile-clean is not feature-complete; these are the honest functional
fences the wasm arms document in code:

- **cbstore ZSTD frames**: `zstd` is target-gated out (links C); the wasm
  codec arm refuses ZSTD frames at read and degrades Zstd/Auto codec choice
  to LZ4 at write. A pure-Rust decoder (ruzstd) is the documented out.
- **cbstore SegMap**: no mmap on WASI — the wasm arm materializes parts
  into an owned heap buffer (whole-part reads; paged reader if ever needed).
- **timeouts are inert**: no threads and no SIGALRM on wasm32-wasip1; armed
  timeouts never fire (statement_timeout/lock_timeout).
- **WaitEventSet**: time-only backend (single thread, no wake sources);
  infinite latch waits report a clean ERROR instead of deadlocking; socket
  events cannot be registered.
- **no signals**: the --single real-signal bridge is a no-op; Ctrl-C kills
  the runtime (crash recovery on next boot), C's no-graceful-signal story.
- **xml / ICU**: no dlopen on WASI; both report clean "not supported" when
  actually requested (like native environments without the .so).
- **snapbuild on-disk layout**: pointer-width-dependent (C's own shape);
  wasm-written logical-decoding snapshots are not byte-compatible with
  native ones. Logical decoding is not on the wasm ladder.
- **synthetic identity**: no pids (process id = 1), no uids (ownership
  checks skipped, C's WIN32 arm shape), username from `--env USER`.
