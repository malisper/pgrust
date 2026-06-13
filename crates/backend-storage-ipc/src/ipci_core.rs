//! `ipci.c` proper — the IPC initialization driver.
//!
//! Faithful port surface; bodies are `todo!()` pending the implementation
//! pass. Each shared-memory subsystem boundary is reached through that
//! owner's per-owner `*-seams` crate, grouped under the sibling
//! [`super::ipci_seams_storage_access`], [`super::ipci_seams_xlog_clog`] and
//! [`super::ipci_seams_bgworker_repl_stats`] modules.

use std::cell::Cell;

use types_core::Size;
use types_error::PgResult;
use types_storage::PGShmemHeader;

// ---------------------------------------------------------------------------
// GUCs and process-global state (ipci.c file scope)
// ---------------------------------------------------------------------------

thread_local! {
    /// `static Size total_addin_request = 0;` (ipci.c) — running total of
    /// shared memory requested by preload libraries via
    /// [`request_addin_shmem_space`]. Per-backend thread_local per the
    /// no-ambient-global rule; only the postmaster meaningfully accumulates it.
    static TOTAL_ADDIN_REQUEST: Cell<Size> = const { Cell::new(0) };
}

/// `int shared_memory_type = DEFAULT_SHARED_MEMORY_TYPE;` (ipci.c GUC) —
/// which shared-memory implementation to use (mmap vs sysv). Discriminants
/// match the C `SHMEM_TYPE_*` enum.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SharedMemoryType {
    /// `SHMEM_TYPE_WINDOWS` (win32 only).
    Windows = 0,
    /// `SHMEM_TYPE_SYSV`.
    SysV = 1,
    /// `SHMEM_TYPE_MMAP` (the `DEFAULT_SHARED_MEMORY_TYPE` on Unix).
    Mmap = 2,
}

/// `shmem_startup_hook_type shmem_startup_hook = NULL;` (ipci.c) — optional
/// callback a loadable module installs to set up its own shmem allocations,
/// run at the tail of [`create_shared_memory_and_semaphores`] /
/// [`attach_shared_memory_structs`]. Stored as a fallible callback because the
/// hook may `ereport(ERROR)`.
pub type ShmemStartupHook = fn() -> PgResult<()>;

thread_local! {
    static SHMEM_STARTUP_HOOK: Cell<Option<ShmemStartupHook>> = const { Cell::new(None) };
}

// ---------------------------------------------------------------------------
// ipci.c functions
// ---------------------------------------------------------------------------

/// `RequestAddinShmemSpace(Size size)` (ipci.c) — request extra shmem space
/// for a loadable module.
///
/// Valid only from a library's `shmem_request_hook`
/// (`process_shmem_requests_in_progress`); otherwise the C `elog(FATAL,
/// "cannot request additional shared memory outside shmem_request_hook")`,
/// carried on `Err`. Accumulates into [`TOTAL_ADDIN_REQUEST`] via the
/// shmem.c `add_size` overflow check.
pub fn request_addin_shmem_space(_size: Size) -> PgResult<()> {
    let _ = &TOTAL_ADDIN_REQUEST;
    todo!("ipci.c:RequestAddinShmemSpace")
}

/// `CalculateShmemSize(int *num_semaphores)` (ipci.c) — total shared-memory
/// bytes and semaphore count for the running configuration.
///
/// Returns `(size, num_semaphores)`. The C `num_semaphores` out-parameter is
/// optional; here it is always returned and the caller ignores it when not
/// wanted. `Err` carries the `add_size`/`mul_size` overflow `ereport(ERROR)`
/// accumulated across every subsystem `*ShmemSize` (routed through the three
/// `ipci_seams_*` modules) plus [`TOTAL_ADDIN_REQUEST`].
pub fn calculate_shmem_size() -> PgResult<(Size, i32)> {
    todo!("ipci.c:CalculateShmemSize")
}

/// `CreateSharedMemoryAndSemaphores(void)` (ipci.c) — create and initialize
/// shared memory and semaphores (postmaster / standalone backend; asserts
/// `!IsUnderPostmaster`).
///
/// Drives: [`calculate_shmem_size`] -> `PGSharedMemoryCreate`
/// (`backend-port-sysv-shmem-seams`) -> `InitShmemAccess`
/// (`backend-storage-ipc-shmem-seams`) -> `PGReserveSemaphores`
/// (`backend-port-pg-sema-seams`) -> `InitShmemAllocation`
/// (`backend-storage-ipc-shmem-seams`) -> [`create_or_attach_shmem_structs`]
/// -> `dsm_postmaster_startup(shim)` (`backend-storage-ipc-dsm-core-seams`)
/// -> the optional [`SHMEM_STARTUP_HOOK`]. `Err` carries any subsystem
/// `ereport(ERROR)`.
pub fn create_shared_memory_and_semaphores() -> PgResult<()> {
    let _ = &SHMEM_STARTUP_HOOK;
    todo!("ipci.c:CreateSharedMemoryAndSemaphores")
}

/// `AttachSharedMemoryStructs(void)` (ipci.c, `#ifdef EXEC_BACKEND`) —
/// initialize a postmaster child's access to the already-created shared
/// structures.
///
/// Asserts `MyProc != NULL` and `IsUnderPostmaster`, recomputes the fast-path
/// lock groups via `InitializeFastPathLocks`
/// (`backend-storage-lmgr-proc-seams`), runs
/// [`create_or_attach_shmem_structs`], then the optional
/// [`SHMEM_STARTUP_HOOK`]. In `!EXEC_BACKEND` builds the child inherits
/// everything through `fork()` and this is unused, but the symbol is kept for
/// the EXEC_BACKEND path.
pub fn attach_shared_memory_structs() -> PgResult<()> {
    todo!("ipci.c:AttachSharedMemoryStructs")
}

/// `CreateOrAttachShmemStructs(void)` (ipci.c, file-static) — allocate-or-
/// attach every subsystem's shared structures, in the exact C order.
///
/// The ordering is load-bearing: `CreateLWLocks`
/// (`backend-storage-lmgr-lwlock-seams`) must run first (LWLocks back
/// `InitShmemIndex`), then `InitShmemIndex`
/// (`backend-storage-ipc-shmem-seams`), then the `dsm`/`DSMRegistry`,
/// WAL/CLOG, lock-manager, predicate, process-table, signaling, bgworker,
/// replication, stats and misc subsystems via their per-owner
/// `*ShmemInit` seams (grouped in the three `ipci_seams_*` modules).
/// `InitProcGlobal` (`backend-storage-lmgr-proc-seams`) runs only when
/// `!IsUnderPostmaster`. `Err` carries any subsystem out-of-shmem
/// `ereport(ERROR)`.
pub fn create_or_attach_shmem_structs() -> PgResult<()> {
    todo!("ipci.c:CreateOrAttachShmemStructs")
}

/// `InitializeShmemGUCs(void)` (ipci.c) — set the runtime-computed
/// shared-memory GUCs.
///
/// Computes the total size via [`calculate_shmem_size`] and writes
/// `shared_memory_size` (rounded up to MB), `shared_memory_size_in_huge_pages`
/// (when `GetHugePageSize` reports a non-zero page size) and
/// `num_os_semaphores`, each with `SetConfigOption(..., PGC_INTERNAL,
/// PGC_S_DYNAMIC_DEFAULT)` (`backend-utils-misc-guc-seams`). `Err` carries the
/// `add_size` overflow `ereport(ERROR)`.
pub fn initialize_shmem_gucs() -> PgResult<()> {
    todo!("ipci.c:InitializeShmemGUCs")
}

/// Install the loadable-module `shmem_startup_hook` (test/extension support).
/// Mirrors a module assigning the C `shmem_startup_hook` global.
pub fn set_shmem_startup_hook(hook: Option<ShmemStartupHook>) {
    SHMEM_STARTUP_HOOK.with(|h| h.set(hook));
}

/// Per-module aggregator; ipci.c installs no inward seams (see crate docs).
pub fn init_seams() {
    let _: Option<PGShmemHeader> = None;
}
