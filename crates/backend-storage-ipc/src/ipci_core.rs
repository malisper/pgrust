//! `ipci.c` proper — the IPC initialization driver.
//!
//! Each shared-memory subsystem boundary is reached through that owner's
//! per-owner `*-seams` crate, grouped under the sibling
//! [`super::ipci_seams_storage_access`], [`super::ipci_seams_xlog_clog`] and
//! [`super::ipci_seams_bgworker_repl_stats`] modules.

use std::cell::Cell;

use backend_utils_error::elog;
use types_core::Size;
use types_error::{PgResult, DEBUG3, FATAL};
use types_guc::guc::{PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT};

use crate::ipci_seams_bgworker_repl_stats as bg;
use crate::ipci_seams_storage_access as sa;
use crate::ipci_seams_xlog_clog as xc;

// Boundary owners reached directly from ipci.c's drivers (not in the three
// `ipci_seams_*` grouping modules above).
use backend_port_pg_sema_seams as pg_sema;
use backend_port_sysv_shmem_seams as sysv_shmem;
use backend_storage_ipc_dsm_core_seams as dsm;
use backend_storage_ipc_dsm_registry_seams as dsm_registry;
use backend_storage_ipc_shmem_seams as shmem;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_storage_lmgr_proc_seams as proc;
use backend_utils_hash_dynahash_seams as dynahash;
use backend_utils_misc_guc_seams as guc;

// `SHMEM_INDEX_SIZE` / `sizeof(ShmemIndexEnt)` come from the shmem.c owner
// (no dependency cycle: shmem.c does not call ipci.c).
use backend_storage_ipc_shmem::{ShmemIndexEnt, SHMEM_INDEX_SIZE};

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
/// Valid only from a library's `shmem_request_hook`; otherwise the C
/// `elog(FATAL, "cannot request additional shared memory outside
/// shmem_request_hook")`, carried on `Err`. Accumulates into
/// [`TOTAL_ADDIN_REQUEST`] via the shmem.c `add_size` overflow check.
/// `process_shmem_requests_in_progress` is the caller's view of miscinit.c's
/// flag of that name (an explicit parameter per the no-ambient-globals rule,
/// matching `RequestNamedLWLockTranche`).
pub fn request_addin_shmem_space(
    size: Size,
    process_shmem_requests_in_progress: bool,
) -> PgResult<()> {
    if !process_shmem_requests_in_progress {
        elog(
            FATAL,
            "cannot request additional shared memory outside shmem_request_hook",
        )?;
    }
    let total = TOTAL_ADDIN_REQUEST.with(|t| t.get());
    let total = shmem::add_size::call(total, size)?;
    TOTAL_ADDIN_REQUEST.with(|t| t.set(total));
    Ok(())
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
    // Compute number of semaphores we'll need.
    let num_semas = proc::proc_global_semas::call();

    // The C `*num_semaphores` out-parameter is always returned here; the
    // caller ignores it when not wanted.

    // Size of the Postgres shared-memory block is estimated via moderately-
    // accurate estimates for the big hogs, plus 100K for the stuff that's too
    // small to bother with estimating. add_size guards against size_t overflow.
    let mut size: Size = 100000;
    size = shmem::add_size::call(size, pg_sema::pg_semaphore_shmem_size::call(num_semas)?)?;
    size = shmem::add_size::call(
        size,
        dynahash::hash_estimate_size::call(SHMEM_INDEX_SIZE, core::mem::size_of::<ShmemIndexEnt>()),
    )?;
    size = shmem::add_size::call(size, dsm::dsm_estimate_size::call()?)?;
    size = shmem::add_size::call(size, dsm_registry::dsm_registry_shmem_size::call()?)?;
    size = shmem::add_size::call(size, sa::buffer_manager_shmem_size()?)?;
    size = shmem::add_size::call(size, sa::lock_manager_shmem_size()?)?;
    size = shmem::add_size::call(size, sa::predicate_lock_shmem_size()?)?;
    size = shmem::add_size::call(size, proc::proc_global_shmem_size::call()?)?;
    size = shmem::add_size::call(size, xc::xlog_prefetch_shmem_size()?)?;
    size = shmem::add_size::call(size, xc::varsup_shmem_size()?)?;
    size = shmem::add_size::call(size, xc::xlog_shmem_size()?)?;
    size = shmem::add_size::call(size, xc::xlog_recovery_shmem_size()?)?;
    size = shmem::add_size::call(size, xc::clog_shmem_size()?)?;
    size = shmem::add_size::call(size, xc::commit_ts_shmem_size()?)?;
    size = shmem::add_size::call(size, xc::sub_trans_shmem_size()?)?;
    size = shmem::add_size::call(size, xc::two_phase_shmem_size())?;
    size = shmem::add_size::call(size, bg::background_worker_shmem_size())?;
    size = shmem::add_size::call(size, xc::multi_xact_shmem_size()?)?;
    size = shmem::add_size::call(size, lwlock::lwlock_shmem_size::call()?)?;
    size = shmem::add_size::call(size, sa::proc_array_shmem_size()?)?;
    size = shmem::add_size::call(size, sa::backend_status_shmem_size()?)?;
    size = shmem::add_size::call(size, sa::shared_inval_shmem_size()?)?;
    size = shmem::add_size::call(size, sa::pm_signal_shmem_size()?)?;
    size = shmem::add_size::call(size, sa::proc_signal_shmem_size()?)?;
    size = shmem::add_size::call(size, bg::checkpointer_shmem_size()?)?;
    size = shmem::add_size::call(size, bg::auto_vacuum_shmem_size())?;
    size = shmem::add_size::call(size, bg::replication_slots_shmem_size()?)?;
    size = shmem::add_size::call(size, bg::replication_origin_shmem_size())?;
    size = shmem::add_size::call(size, bg::wal_snd_shmem_size()?)?;
    size = shmem::add_size::call(size, bg::wal_rcv_shmem_size()?)?;
    size = shmem::add_size::call(size, bg::wal_summarizer_shmem_size())?;
    size = shmem::add_size::call(size, bg::pg_arch_shmem_size())?;
    size = shmem::add_size::call(size, bg::apply_launcher_shmem_size())?;
    size = shmem::add_size::call(size, sa::btree_shmem_size()?)?;
    size = shmem::add_size::call(size, sa::sync_scan_shmem_size()?)?;
    size = shmem::add_size::call(size, bg::async_shmem_size()?)?;
    size = shmem::add_size::call(size, bg::stats_shmem_size()?)?;
    size = shmem::add_size::call(size, bg::wait_event_custom_shmem_size()?)?;
    size = shmem::add_size::call(size, bg::injection_point_shmem_size()?)?;
    size = shmem::add_size::call(size, bg::slot_sync_shmem_size())?;
    size = shmem::add_size::call(size, sa::aio_shmem_size()?)?;

    // include additional requested shmem from preload libraries
    let total_addin_request = TOTAL_ADDIN_REQUEST.with(|t| t.get());
    size = shmem::add_size::call(size, total_addin_request)?;

    // might as well round it off to a multiple of a typical page size
    size = shmem::add_size::call(size, 8192 - (size % 8192))?;

    Ok((size, num_semas))
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
    // Assert(!IsUnderPostmaster);
    debug_assert!(!backend_utils_init_small_seams::is_under_postmaster::call());

    // Compute the size of the shared-memory block.
    let (size, num_semas) = calculate_shmem_size()?;
    elog(DEBUG3, format!("invoking IpcMemoryCreate(size={size})"))?;

    // Create the shmem segment. The C `PGShmemHeader **shim` out-parameter is
    // folded into the returned `(seghdr, shim)` pair (both are genuinely
    // shared memory, opacity inherited as raw pointers).
    let (seghdr, shim) = sysv_shmem::pg_shared_memory_create::call(size)?;

    // Make sure that huge pages are never reported as "unknown" while the
    // server is running.
    debug_assert!(
        guc::get_config_option::call("huge_pages_status".to_string(), false, false).as_deref()
            != Some("unknown")
    );

    shmem::init_shmem_access::call(seghdr);

    // Create semaphores.  (This is done here for historical reasons.  We used
    // to support emulating spinlocks with semaphores, which required
    // initializing semaphores early.)
    pg_sema::pg_reserve_semaphores::call(num_semas)?;

    // Set up shared memory allocation mechanism.
    shmem::init_shmem_allocation::call();

    // Initialize subsystems.
    create_or_attach_shmem_structs()?;

    // Initialize dynamic shared memory facilities.
    dsm::dsm_postmaster_startup::call(shim)?;

    // Now give loadable modules a chance to set up their shmem allocations.
    if let Some(hook) = SHMEM_STARTUP_HOOK.with(|h| h.get()) {
        hook()?;
    }
    Ok(())
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
    // InitProcess must've been called already: Assert(MyProc != NULL).
    debug_assert_ne!(
        backend_utils_init_small_seams::my_proc_number::call(),
        types_core::INVALID_PROC_NUMBER
    );
    // Assert(IsUnderPostmaster).
    debug_assert!(backend_utils_init_small_seams::is_under_postmaster::call());

    // In EXEC_BACKEND mode, backends don't inherit the number of fast-path
    // groups we calculated before setting the shmem up, so recalculate it.
    proc::initialize_fast_path_locks::call();

    create_or_attach_shmem_structs()?;

    // Now give loadable modules a chance to set up their shmem allocations.
    if let Some(hook) = SHMEM_STARTUP_HOOK.with(|h| h.get()) {
        hook()?;
    }
    Ok(())
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
    // Now initialize LWLocks, which do shared memory allocation and are
    // needed for InitShmemIndex.
    lwlock::create_lwlocks::call()?;

    // Set up shmem.c index hashtable.
    shmem::init_shmem_index::call()?;

    dsm::dsm_shmem_init::call()?;
    dsm_registry::dsm_registry_shmem_init::call()?;

    // Set up xlog, clog, and buffers.
    xc::varsup_shmem_init()?;
    xc::xlog_shmem_init()?;
    xc::xlog_prefetch_shmem_init()?;
    xc::xlog_recovery_shmem_init()?;
    xc::clog_shmem_init()?;
    xc::commit_ts_shmem_init()?;
    xc::sub_trans_shmem_init()?;
    xc::multi_xact_shmem_init()?;
    sa::buffer_manager_shmem_init()?;

    // Set up lock manager.
    sa::lock_manager_shmem_init()?;

    // Set up predicate lock manager.
    sa::predicate_lock_shmem_init()?;

    // Set up process table.
    if !backend_utils_init_small_seams::is_under_postmaster::call() {
        proc::init_proc_global::call()?;
    }
    sa::proc_array_shmem_init()?;
    sa::backend_status_shmem_init()?;
    xc::two_phase_shmem_init()?;
    bg::background_worker_shmem_init()?;

    // Set up shared-inval messaging.
    sa::shared_inval_shmem_init()?;

    // Set up interprocess signaling mechanisms.
    sa::pm_signal_shmem_init()?;
    sa::proc_signal_shmem_init()?;
    bg::checkpointer_shmem_init()?;
    bg::auto_vacuum_shmem_init()?;
    bg::replication_slots_shmem_init();
    bg::replication_origin_shmem_init()?;
    bg::wal_snd_shmem_init()?;
    bg::wal_rcv_shmem_init()?;
    bg::wal_summarizer_shmem_init()?;
    bg::pg_arch_shmem_init();
    bg::apply_launcher_shmem_init()?;
    bg::slot_sync_shmem_init()?;

    // Set up other modules that need some shared memory space.
    sa::btree_shmem_init()?;
    sa::sync_scan_shmem_init()?;
    bg::async_shmem_init()?;
    bg::stats_shmem_init()?;
    bg::wait_event_custom_shmem_init()?;
    bg::injection_point_shmem_init()?;
    sa::aio_shmem_init()?;

    Ok(())
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
    // Calculate the shared memory size and round up to the nearest megabyte.
    let (size_b, num_semas) = calculate_shmem_size()?;
    let size_mb = shmem::add_size::call(size_b, (1024 * 1024) - 1)? / (1024 * 1024);
    let buf = format!("{size_mb}");
    guc::set_config_option::call(
        "shared_memory_size",
        &buf,
        PGC_INTERNAL,
        PGC_S_DYNAMIC_DEFAULT,
    )?;

    // Calculate the number of huge pages required.
    let (hp_size, _mmap_flags) = sysv_shmem::get_huge_page_size::call();
    if hp_size != 0 {
        let hp_required = shmem::add_size::call(size_b / hp_size, 1)?;
        let buf = format!("{hp_required}");
        guc::set_config_option::call(
            "shared_memory_size_in_huge_pages",
            &buf,
            PGC_INTERNAL,
            PGC_S_DYNAMIC_DEFAULT,
        )?;
    }

    let buf = format!("{num_semas}");
    guc::set_config_option::call(
        "num_os_semaphores",
        &buf,
        PGC_INTERNAL,
        PGC_S_DYNAMIC_DEFAULT,
    )?;
    Ok(())
}

/// Install the loadable-module `shmem_startup_hook` (test/extension support).
/// Mirrors a module assigning the C `shmem_startup_hook` global.
pub fn set_shmem_startup_hook(hook: Option<ShmemStartupHook>) {
    SHMEM_STARTUP_HOOK.with(|h| h.set(hook));
}

/// Per-module aggregator; ipci.c installs no inward seams (see crate docs).
pub fn init_seams() {}
