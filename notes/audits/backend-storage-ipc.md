# Audit: backend-storage-ipc

- **Unit:** `backend-storage-ipc`
- **C source:** `src/backend/storage/ipc/ipci.c` (PostgreSQL 18.3) — the IPC
  initialization driver. (`ipc.c` is *not* in scope: it is owned by
  `backend-storage-ipc-dsm-core`, reached here through
  `backend-storage-ipc-seams`.)
- **Port crate:** `crates/backend-storage-ipc` (modules `ipci_core`,
  `ipci_seams_storage_access`, `ipci_seams_xlog_clog`,
  `ipci_seams_bgworker_repl_stats`).
- **c2rust:** no `c2rust-runs/` entry exists for this unit; inventory
  re-derived directly from the C source (every function in the single TU
  enumerated below).
- **Date:** 2026-06-13
- **Model:** Claude Fable 5 (Opus 4.8, 1M)
- **Verdict:** **PASS** (one finding raised and fixed in this audit round;
  re-derived clean afterward)

## 1. Function inventory (every definition in ipci.c)

| # | C function (location)                                | Port location                                          | Verdict |
|---|------------------------------------------------------|--------------------------------------------------------|---------|
| 1 | `RequestAddinShmemSpace` (ipci.c:73)                 | `ipci_core::request_addin_shmem_space` (:86)           | MATCH   |
| 2 | `CalculateShmemSize` (ipci.c:88)                     | `ipci_core::calculate_shmem_size` (:110)               | MATCH   |
| 3 | `AttachSharedMemoryStructs` (ipci.c:172, `#ifdef EXEC_BACKEND`) | `ipci_core::attach_shared_memory_structs` (:242) | MATCH   |
| 4 | `CreateSharedMemoryAndSemaphores` (ipci.c:199)       | `ipci_core::create_shared_memory_and_semaphores` (:188)| MATCH   |
| 5 | `CreateOrAttachShmemStructs` (ipci.c:267, file-static)| `ipci_core::create_or_attach_shmem_structs` (:277)    | MATCH   |
| 6 | `InitializeShmemGUCs` (ipci.c:356)                   | `ipci_core::initialize_shmem_gucs` (:352)              | MATCH   |

File-scope state:
- `static Size total_addin_request = 0;` (ipci.c:60) → `TOTAL_ADDIN_REQUEST`
  `thread_local Cell<Size>` (per-backend, no ambient global). MATCH.
- `int shared_memory_type = DEFAULT_SHARED_MEMORY_TYPE;` (ipci.c:56) →
  `SharedMemoryType` `#[repr(i32)]` enum. Discriminants verified against
  `src/include/storage/pg_shmem.h:62-64` (`SHMEM_TYPE_WINDOWS`=0, `SYSV`=1,
  `MMAP`=2); default `MMAP` per `pg_shmem.h:76`. MATCH.
- `shmem_startup_hook_type shmem_startup_hook = NULL;` (ipci.c:58) →
  `SHMEM_STARTUP_HOOK` `thread_local Cell<Option<ShmemStartupHook>>`, hook typed
  `fn() -> PgResult<()>` (the hook may `ereport(ERROR)`). MATCH.

### Per-function detail

**1. `RequestAddinShmemSpace`** — `!process_shmem_requests_in_progress` →
`elog(FATAL, "cannot request additional shared memory outside
shmem_request_hook")` (string verified verbatim), else
`total_addin_request = add_size(total_addin_request, size)`. The C reads the
miscinit.c global `process_shmem_requests_in_progress`; the port threads it as
an explicit `bool` parameter (no-ambient-global rule). `add_size` routed to
`backend-storage-ipc-shmem-seams` (overflow `ereport(ERROR)` preserved via
`?`). MATCH.

**2. `CalculateShmemSize`** — `numSemas = ProcGlobalSemas()`; optional
`*num_semaphores` out-param folded into the returned `(Size, i32)` tuple (caller
ignores when unwanted, matching the C `NULL` test). Base `size = 100000`, then
41 `add_size` accumulations. I checked the call sequence position-by-position
against ipci.c:111-152 — all 41 subsystem `*ShmemSize` calls appear in the exact
same order (PGSemaphore, hash_estimate_size(SHMEM_INDEX_SIZE,
sizeof(ShmemIndexEnt)), dsm_estimate, DSMRegistry, BufferManager, LockManager,
PredicateLock, ProcGlobal, XLogPrefetch, Varsup, XLOG, XLogRecovery, CLOG,
CommitTs, SUBTRANS, TwoPhase, BackgroundWorker, MultiXact, LWLock, ProcArray,
BackendStatus, SharedInval, PMSignal, ProcSignal, Checkpointer, AutoVacuum,
ReplicationSlots, ReplicationOrigin, WalSnd, WalRcv, WalSummarizer, PgArch,
ApplyLauncher, BTree, SyncScan, Async, Stats, WaitEventCustom, InjectionPoint,
SlotSync, Aio). Then `+ total_addin_request`, then the page-round
`add_size(size, 8192 - (size % 8192))` (verified identical, incl. operator
precedence). MATCH.

**3. `AttachSharedMemoryStructs`** (`#ifdef EXEC_BACKEND`) — `Assert(MyProc !=
NULL)` rendered as `my_proc_number != INVALID_PROC_NUMBER` debug_assert;
`Assert(IsUnderPostmaster)`; `InitializeFastPathLocks()`;
`CreateOrAttachShmemStructs()`; tail `shmem_startup_hook`. Order and predicates
match ipci.c:174-191. Symbol retained for the EXEC_BACKEND path (no-op inherit
otherwise), faithful to the C `#ifdef`. MATCH.

**4. `CreateSharedMemoryAndSemaphores`** — `Assert(!IsUnderPostmaster)`;
`size = CalculateShmemSize(&numSemas)`; `elog(DEBUG3, "invoking
IpcMemoryCreate(size=%zu)", size)` (format reproduced); `PGSharedMemoryCreate`
returning `(seghdr, shim)` (the C `PGShmemHeader **shim` out-param folded into
the pair, both real shared-memory pointers — inherited opacity, no invented
handle); the `huge_pages_status != "unknown"` assert; `InitShmemAccess`;
`PGReserveSemaphores(numSemas)`; `InitShmemAllocation`;
`CreateOrAttachShmemStructs`; `dsm_postmaster_startup(shim)`; tail
`shmem_startup_hook`. Order matches ipci.c:207-249. MATCH.

**5. `CreateOrAttachShmemStructs`** (file-static) — the load-bearing init order
checked line-by-line against ipci.c:274-347: `CreateLWLocks`, `InitShmemIndex`,
`dsm_shmem_init`, `DSMRegistryShmemInit`; xlog/clog/buffers block (Varsup, XLOG,
XLogPrefetch, XLogRecovery, CLOG, CommitTs, SUBTRANS, MultiXact,
BufferManager); `LockManagerShmemInit`; `PredicateLockShmemInit`;
`if (!IsUnderPostmaster) InitProcGlobal()` (predicate preserved), ProcArray,
BackendStatus, TwoPhase, BackgroundWorker; SharedInval; PMSignal, ProcSignal,
Checkpointer, AutoVacuum, ReplicationSlots, ReplicationOrigin, WalSnd, WalRcv,
WalSummarizer, PgArch, ApplyLauncher, SlotSync; then BTree, SyncScan, Async,
Stats, WaitEventCustom, InjectionPoint, Aio. Every `*ShmemInit` present, none
extra, exact order. MATCH.

**6. `InitializeShmemGUCs`** — `size_b = CalculateShmemSize(&num_semas)`;
`size_mb = add_size(size_b, 1024*1024 - 1) / (1024*1024)`;
`SetConfigOption("shared_memory_size", buf, PGC_INTERNAL,
PGC_S_DYNAMIC_DEFAULT)`; `GetHugePageSize(&hp_size, NULL)` and, when
`hp_size != 0`, `hp_required = add_size(size_b / hp_size, 1)` →
`SetConfigOption("shared_memory_size_in_huge_pages", ...)`; then
`SetConfigOption("num_os_semaphores", ...)`. GUC names, both `PGC_INTERNAL` /
`PGC_S_DYNAMIC_DEFAULT` arguments, the `hp_size != 0` guard, and the integer
divisions/`add_size` overflow paths all match ipci.c:368-389. `sprintf(buf,
"%zu"/"%d", ...)` → `format!`. MATCH.

## 2. Seam audit

**Owned seam crates** (by C-source coverage; `X` mapping to ipci.c):
- `backend-storage-ipc-ipci-seams` — `X = backend-storage-ipc-ipci` → ipci.c.
  Declares one inward seam, `create_shared_memory_and_semaphores` (infallible
  `fn()`, because the C creation path `ereport(FATAL)`s — never recoverable).
  Consumer: `backend-bootstrap-bootstrap/src/lib.rs:412` calls it across the
  bootstrap ↔ shmem-setup dependency cycle (a real cycle: bootstrap drives
  startup which builds shmem, which transitively reaches bootstrap-owned
  subsystems). **FINDING (raised + fixed this round):** the assembled crate's
  `init_seams()` was empty, leaving this owned seam uninstalled — an automatic
  FAIL per step 3. Fixed by installing it from `backend_storage_ipc::init_seams`
  via an `.expect()` adapter over the port's `PgResult<()>` (the FATAL becomes
  process termination, faithful to the C), adding the dep edge, and wiring
  `backend_storage_ipc::init_seams()` into `seams-init::init_all()`. Re-derived
  after the fix: the installer contains only `set()` calls; `seams-init` calls
  it. Clean.

`backend-storage-ipc-seams` maps to **ipc.c**, owned by
`backend-storage-ipc-dsm-core`, not this unit — out of scope here.

**Outward seam routing** (`ipci_seams_storage_access`, `ipci_seams_xlog_clog`,
`ipci_seams_bgworker_repl_stats`, and the direct boundary owners in
`ipci_core`): every wrapper is a thin marshal+delegate — argument pass-through,
exactly one owner `::call()`, result pass-through. No branching, node
construction, or computation lives in any seam path; all driver logic
(ordering, the `add_size` accumulation, the `!IsUnderPostmaster` /
`hp_size != 0` predicates, the hook invocation) lives in `ipci_core`, never in a
seam. Each `*ShmemSize`/`*ShmemInit` routes to the correct owner per the C call
site. The two infallible delegations (`ReplicationSlotsShmemInit` → `()`,
`SlotSyncShmemSize` → `Size`) match the pre-existing owner seam signatures
(slot.c / slotsync.c), and ipci.c calls them faithfully — their fallibility is
the owner's concern, not this unit's.

## 3. Design conformance

- **Inherited opacity, never introduced:** `PGShmemHeader *seghdr`/`*shim`
  carried as real raw pointers in the `(seghdr, shim)` return; the `shim`
  out-param fold introduces no stand-in handle. PASS.
- **Allocating entry points / `Mcx` + `PgResult`:** ipci.c's drivers do not
  themselves allocate palloc memory (they hand sizes to subsystem owners), so no
  `Mcx` parameter is required; the fallible drivers all return `PgResult`. PASS.
- **Per-backend globals as `thread_local`:** `total_addin_request`,
  `shared_memory_type`'s state, and `shmem_startup_hook` are all `thread_local`,
  not shared statics. PASS.
- **No ambient-global seams:** `process_shmem_requests_in_progress` is an
  explicit parameter rather than an ambient read. PASS.
- **No own-logic stubbing / restructuring-to-dodge:** every C body is fully
  ported; only genuinely cross-cycle subsystem callees are seamed (panic until
  owner lands). No `todo!()`/`unimplemented!()` in own logic. PASS.

## 4. Gate

- `cargo check --workspace`: clean (warnings only, all pre-existing/unrelated).
- `cargo test --workspace`: 0 failed.

## Verdict: PASS

All six ipci.c functions plus file-scope state MATCH; outward seams are pure
marshal+delegate; the one owned inward seam is now installed and wired. No
residual `todo!()`/`unimplemented!()` in own logic. Design-conformance clean.
