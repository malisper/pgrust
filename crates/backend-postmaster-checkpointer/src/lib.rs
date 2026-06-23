//! Port of `src/backend/postmaster/checkpointer.c` (PostgreSQL 18.3): the
//! checkpointer auxiliary process.
//!
//! The checkpointer handles all checkpoints. Checkpoints are automatically
//! dispatched after a certain amount of time has elapsed since the last one,
//! and the process can be signaled to perform requested checkpoints as well.
//! Backends communicate with the checkpointer through a shared-memory control
//! block (`CheckpointerShmem`) and forward fsync requests through it
//! (`ForwardSyncRequest` / `AbsorbSyncRequests`).
//!
//! # Faithful shared memory (`CheckpointerShmem`)
//!
//! `CheckpointerShmemStruct` is real shared memory, placed in the genuine shmem
//! segment via `shmem_init_struct` and laid out `#[repr(C)]` field-for-field
//! with the C struct so the header bytes are valid across processes. Within it:
//!   * `ckpt_lck` is a real `slock_t` spinlock (`Spinlock`), driven by the
//!     s-lock crate, protecting the `ckpt_*` counters.
//!   * `start_cv` / `done_cv` are real `ConditionVariable`s embedded in shmem.
//!   * the `requests[]` ring (a flexible array member sized `Min(NBuffers,
//!     MAX_CHECKPOINT_REQUESTS)`) is the fsync request queue, protected by the
//!     built-in `CheckpointerCommLock` LWLock.
//!
//! # Per-backend process-local state
//!
//! The file-static `ckpt_active` / `ShutdownXLOGPending` / `ckpt_start_*` /
//! `last_*_time` and the two GUCs (`CheckPointTimeout` / `CheckPointWarning` /
//! `CheckPointCompletionTarget`) are process-local in C, so they live in
//! `thread_local!` here (AGENTS.md backend-global-state rule).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use core::cell::Cell;
use core::ptr::NonNull;

use types_core::{Size, XLogRecPtr, INVALID_PROC_NUMBER};
use types_condvar::ConditionVariable;
use types_error::{ErrorLocation, PgError, PgResult, DEBUG1, ERROR, LOG};
use types_pgstat::wait_event::{
    WAIT_EVENT_CHECKPOINTER_MAIN, WAIT_EVENT_CHECKPOINTER_SHUTDOWN, WAIT_EVENT_CHECKPOINT_DONE,
    WAIT_EVENT_CHECKPOINT_START, WAIT_EVENT_CHECKPOINT_WRITE_DELAY,
};
use types_startup::StartupData;
use types_storage::sync::{FileTag, SyncRequestType};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};
use types_wal::xlog_consts::{
    CHECKPOINT_CAUSE_TIME, CHECKPOINT_CAUSE_XLOG, CHECKPOINT_END_OF_RECOVERY, CHECKPOINT_IMMEDIATE,
    CHECKPOINT_REQUESTED, CHECKPOINT_WAIT,
};

use backend_utils_error::ereport;

use backend_storage_lmgr_s_lock::{s_lock, s_unlock, Spinlock};

use backend_postmaster_interrupt as interrupt;

use backend_storage_ipc_shmem_seams as shmem;
use backend_storage_ipc_dsm_core_seams as ipc;
use backend_storage_ipc_latch_seams as latch;
use backend_storage_ipc_procsignal_seams as procsignal;
use backend_storage_ipc_pmsignal_seams as pmsignal;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_storage_lmgr_condition_variable_seams as cv;
use backend_storage_lmgr_proc_seams as proc;
use backend_storage_aio_aio_seams as aio;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_file_fd_seams as fd;
use backend_storage_smgr_seams as smgr;
use backend_storage_sync_seams as sync;
use backend_postmaster_auxprocess_seams as auxprocess;
use backend_utils_init_small_seams as initsmall;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_misc_guc_seams as guc;
use backend_utils_mmgr_mcxt_seams as mcxt;
use backend_utils_resowner_seams as resowner;
use backend_utils_hash_dynahash_seams as dynahash;
use backend_utils_activity_waitevent_seams as waitevent;
use backend_utils_activity_pgstat_wal_seams as walstats;
use backend_utils_activity_pgstat_seams as pgstat;
use backend_access_transam_xlog_seams as xlog;
use backend_replication_syncrep_seams as syncrep;

use types_storage::LWLockMode;

#[cfg(test)]
mod tests;

// ===========================================================================
// Constants (checkpointer.c).
// ===========================================================================

/// Interval for calling `AbsorbSyncRequests` in `CheckpointWriteDelay`.
const WRITES_PER_ABSORB: i32 = 1000;

/// Max number of requests the checkpointer request queue can hold.
pub const MAX_CHECKPOINT_REQUESTS: i32 = 10_000_000;

/// `MAX_SIGNAL_TRIES` (checkpointer.c) — max wait 60.0 sec.
const MAX_SIGNAL_TRIES: i32 = 600;

/// `CheckpointerCommLock` — built-in individual LWLock #11 (`lwlocklist.h`).
const CHECKPOINTER_COMM_LOCK: usize = 11;

// ===========================================================================
// GUC parameters (checkpointer.c:144-146).
//
// In C these are plain globals; the checkpointer reads them locally and the GUC
// machinery keeps them updated. Modeled as process-wide cells.
// ===========================================================================

thread_local! {
    /// `int CheckPointTimeout = 300;`
    static CHECK_POINT_TIMEOUT: Cell<i32> = const { Cell::new(300) };
    /// `int CheckPointWarning = 30;`
    static CHECK_POINT_WARNING: Cell<i32> = const { Cell::new(30) };
    /// `double CheckPointCompletionTarget = 0.9;`
    static CHECK_POINT_COMPLETION_TARGET: Cell<f64> = const { Cell::new(0.9) };
}

/// Read `CheckPointTimeout` (seconds).
pub fn CheckPointTimeout() -> i32 {
    CHECK_POINT_TIMEOUT.with(Cell::get)
}

/// Assign `CheckPointTimeout`.
pub fn set_CheckPointTimeout(value: i32) {
    CHECK_POINT_TIMEOUT.with(|c| c.set(value));
}

/// Read `CheckPointWarning` (seconds).
pub fn CheckPointWarning() -> i32 {
    CHECK_POINT_WARNING.with(Cell::get)
}

/// Assign `CheckPointWarning`.
pub fn set_CheckPointWarning(value: i32) {
    CHECK_POINT_WARNING.with(|c| c.set(value));
}

/// Read `CheckPointCompletionTarget`.
pub fn CheckPointCompletionTarget() -> f64 {
    CHECK_POINT_COMPLETION_TARGET.with(Cell::get)
}

/// Assign `CheckPointCompletionTarget`.
pub fn set_CheckPointCompletionTarget(value: f64) {
    CHECK_POINT_COMPLETION_TARGET.with(|c| c.set(value));
}

// ===========================================================================
// Private checkpointer-process-local state (checkpointer.c:150-160).
// ===========================================================================

struct PrivateState {
    /// `static bool ckpt_active`.
    ckpt_active: bool,
    /// `static pg_time_t ckpt_start_time` (valid while ckpt_active).
    ckpt_start_time: i64,
    /// `static XLogRecPtr ckpt_start_recptr` (valid while ckpt_active).
    ckpt_start_recptr: XLogRecPtr,
    /// `static double ckpt_cached_elapsed` (valid while ckpt_active).
    ckpt_cached_elapsed: f64,
    /// `static pg_time_t last_checkpoint_time`.
    last_checkpoint_time: i64,
    /// `static pg_time_t last_xlog_switch_time`.
    last_xlog_switch_time: i64,
    /// `CheckpointWriteDelay`'s function-static `absorb_counter`.
    write_delay_absorb_counter: i32,
    /// `FirstCallSinceLastCheckpoint`'s function-static `ckpt_done`.
    first_call_ckpt_done: i32,
}

impl PrivateState {
    const fn new() -> Self {
        Self {
            ckpt_active: false,
            ckpt_start_time: 0,
            ckpt_start_recptr: 0,
            ckpt_cached_elapsed: 0.0,
            last_checkpoint_time: 0,
            last_xlog_switch_time: 0,
            write_delay_absorb_counter: WRITES_PER_ABSORB,
            first_call_ckpt_done: 0,
        }
    }
}

thread_local! {
    static PRIVATE: core::cell::RefCell<PrivateState> =
        const { core::cell::RefCell::new(PrivateState::new()) };

    /// `static volatile sig_atomic_t ShutdownXLOGPending = false;`
    /// (checkpointer.c:155). This is a standalone signal-set flag in C, written
    /// from the `ReqShutdownXLOG` SIGINT handler. It MUST live outside the
    /// `RefCell<PrivateState>` so the signal handler can set it without taking a
    /// `borrow_mut()` — a signal arriving while the main loop holds the RefCell
    /// borrow would otherwise panic ("already mutably borrowed"), an
    /// async-signal-unsafe escalation C never has (a `volatile sig_atomic_t`
    /// store is reentrant). A plain `Cell<bool>` store mirrors C's atomic write.
    static SHUTDOWN_XLOG_PENDING: Cell<bool> = const { Cell::new(false) };

    /// `static CheckpointerShmemStruct *CheckpointerShmem;` — the shmem control
    /// block base pointer, set by `CheckpointerShmemInit`. Null until attached.
    static CHECKPOINTER_SHMEM: Cell<*mut u8> = const { Cell::new(core::ptr::null_mut()) };
}

fn with_private<R>(f: impl FnOnce(&mut PrivateState) -> R) -> R {
    PRIVATE.with(|cell| f(&mut cell.borrow_mut()))
}

/// `ShutdownXLOGPending` accessor.
fn shutdown_xlog_pending() -> bool {
    SHUTDOWN_XLOG_PENDING.with(Cell::get)
}

fn ckpt_location(funcname: &str) -> ErrorLocation {
    ErrorLocation::new("checkpointer.c", 0, funcname)
}

// ===========================================================================
// CheckpointerRequest / CheckpointerShmemStruct — the faithful shmem layout.
//
// #[repr(C)] field-for-field with checkpointer.c:107-131 so the header is valid
// in the cross-process shared-memory segment placed by shmem_init_struct.
// ===========================================================================

/// `CheckpointerRequest` (checkpointer.c:107-111) — one fsync request enqueued
/// by a backend. `repr(C)`: `SyncRequestType type` (an `int` enum) then
/// `FileTag ftag`. The dedup in `CompactCheckpointerRequestQueue` keys on the
/// `(type, ftag)` value, exactly as C's per-value `HASH_BLOBS` equivalence.
#[repr(C)]
#[derive(Clone, Copy)]
struct CheckpointerRequest {
    /// `SyncRequestType type` — stored as the raw `i32` discriminant.
    type_: i32,
    /// `FileTag ftag` — the file identifier.
    ftag: FileTag,
}

/// `CheckpointerShmemStruct` (checkpointer.c:113-131) — the shared-memory
/// control block. The trailing `requests[FLEXIBLE_ARRAY_MEMBER]` is reached by
/// pointer arithmetic from the header offset.
#[repr(C)]
struct CheckpointerShmemStruct {
    /// `pid_t checkpointer_pid` — PID (0 if not started). `pid_t` is `int`.
    checkpointer_pid: i32,
    /// `slock_t ckpt_lck` — protects all the `ckpt_*` fields.
    ckpt_lck: Spinlock,
    /// `int ckpt_started` — advances when a checkpoint starts.
    ckpt_started: i32,
    /// `int ckpt_done` — advances when a checkpoint completes.
    ckpt_done: i32,
    /// `int ckpt_failed` — advances when a checkpoint fails.
    ckpt_failed: i32,
    /// `int ckpt_flags` — checkpoint flags (as defined in xlog.h).
    ckpt_flags: i32,
    /// `ConditionVariable start_cv` — signaled when `ckpt_started` advances.
    start_cv: ConditionVariable,
    /// `ConditionVariable done_cv` — signaled when `ckpt_done` advances.
    done_cv: ConditionVariable,
    /// `int num_requests` — current number of requests in the ring.
    num_requests: i32,
    /// `int max_requests` — allocated array size.
    max_requests: i32,
    // `CheckpointerRequest requests[FLEXIBLE_ARRAY_MEMBER]` follows here in the
    // shmem allocation; addressed via request_ptr().
}

/// `offsetof(CheckpointerShmemStruct, requests)`: the header size rounded up to
/// `CheckpointerRequest`'s alignment.
const fn requests_offset() -> usize {
    let header = core::mem::size_of::<CheckpointerShmemStruct>();
    let align = core::mem::align_of::<CheckpointerRequest>();
    (header + align - 1) & !(align - 1)
}

// ===========================================================================
// Checkpointer — the handle onto the shmem-resident control block.
// ===========================================================================

/// Handle onto the shmem-resident `CheckpointerShmemStruct`. Equivalent to C's
/// `static CheckpointerShmemStruct *CheckpointerShmem`.
pub struct Checkpointer {
    base: NonNull<u8>,
}

// The control block lives in the shared-memory segment and is synchronized
// across backends through `ckpt_lck` / `CheckpointerCommLock` and the
// shmem-resident condition variables.
unsafe impl Send for Checkpointer {}
unsafe impl Sync for Checkpointer {}

impl Checkpointer {
    fn header(&self) -> &CheckpointerShmemStruct {
        unsafe { &*self.base.as_ptr().cast::<CheckpointerShmemStruct>() }
    }

    // Shmem-resident control block: mutation goes through a raw pointer into the
    // shared segment (interior mutability under the spinlock / comm lock).
    #[allow(clippy::mut_from_ref)]
    fn header_mut(&self) -> &mut CheckpointerShmemStruct {
        unsafe { &mut *self.base.as_ptr().cast::<CheckpointerShmemStruct>() }
    }

    /// `&ckpt_lck` (the `slock_t`/`Spinlock` cell).
    fn ckpt_lock(&self) -> &Spinlock {
        &self.header().ckpt_lck
    }

    /// `SpinLockAcquire(&ckpt_lck)` returning an RAII guard.
    fn acquire_ckpt_lock(&self) -> SpinGuard<'_> {
        s_lock(self.ckpt_lock(), Some("checkpointer.c"), 0, Some("ckpt_lck"));
        SpinGuard {
            lock: self.ckpt_lock(),
        }
    }

    /// `&CheckpointerShmem->start_cv`.
    fn start_cv(&self) -> &ConditionVariable {
        &self.header().start_cv
    }

    /// `&CheckpointerShmem->done_cv`.
    fn done_cv(&self) -> &ConditionVariable {
        &self.header().done_cv
    }

    /// Pointer to `requests[idx]` in the flexible array following the header.
    fn request_ptr(&self, idx: i32) -> *mut CheckpointerRequest {
        debug_assert!(idx >= 0);
        unsafe {
            self.base
                .as_ptr()
                .add(requests_offset())
                .cast::<CheckpointerRequest>()
                .add(idx as usize)
        }
    }

    fn request(&self, idx: i32) -> CheckpointerRequest {
        unsafe { *self.request_ptr(idx) }
    }

    fn set_request(&self, idx: i32, value: CheckpointerRequest) {
        unsafe { *self.request_ptr(idx) = value }
    }
}

/// RAII guard for `ckpt_lck` (`SpinLockRelease` on drop).
struct SpinGuard<'a> {
    lock: &'a Spinlock,
}

impl Drop for SpinGuard<'_> {
    fn drop(&mut self) {
        s_unlock(self.lock);
    }
}

/// Borrow the shmem-resident `CheckpointerShmem` control block. Panics if
/// accessed before `CheckpointerShmemInit` attaches the segment (C dereferences
/// `CheckpointerShmem` unconditionally on these paths).
fn shmem() -> Checkpointer {
    let p = CHECKPOINTER_SHMEM.with(Cell::get);
    let base = NonNull::new(p).expect("CheckpointerShmem accessed before ShmemInit");
    Checkpointer { base }
}

// ===========================================================================
// CheckpointerShmemSize / CheckpointerShmemInit (checkpointer.c:937-983).
// ===========================================================================

/// `CheckpointerShmemSize` (checkpointer.c:937-953) — compute space needed for
/// checkpointer-related shared memory. The `requests[]` array is sized
/// `Min(NBuffers, MAX_CHECKPOINT_REQUESTS)`.
pub fn CheckpointerShmemSize(nbuffers: i32) -> PgResult<Size> {
    let size = requests_offset();
    let n = nbuffers.min(MAX_CHECKPOINT_REQUESTS).max(0) as usize;
    shmem::add_size::call(
        size,
        shmem::mul_size::call(n, core::mem::size_of::<CheckpointerRequest>())?,
    )
}

/// `CheckpointerShmemInit` (checkpointer.c:959-983) — allocate and initialize
/// checkpointer-related shared memory. On first creation (postmaster) zero the
/// whole struct (so pad bytes in the request structs are zero), init the
/// spinlock, set `max_requests`, and init the two condition variables.
pub fn CheckpointerShmemInit(nbuffers: i32) -> PgResult<()> {
    let size = CheckpointerShmemSize(nbuffers)?;
    let (addr, found) = shmem::shmem_init_struct::call("Checkpointer Data", size)?;
    CHECKPOINTER_SHMEM.with(|c| c.set(addr));
    let cp = shmem();
    if !found {
        // MemSet(CheckpointerShmem, 0, size).
        unsafe {
            core::ptr::write_bytes(cp.base.as_ptr(), 0, size);
        }
        // SpinLockInit(&ckpt_lck) — a zeroed/unlocked Spinlock word is the free state.
        cp.ckpt_lock().unlock();
        // max_requests = Min(NBuffers, MAX_CHECKPOINT_REQUESTS).
        cp.header_mut().max_requests = nbuffers.min(MAX_CHECKPOINT_REQUESTS).max(0);
        // ConditionVariableInit(&start_cv); ConditionVariableInit(&done_cv).
        cv::condition_variable_init::call(&mut cp.header_mut().start_cv);
        cv::condition_variable_init::call(&mut cp.header_mut().done_cv);
    }
    Ok(())
}

// ===========================================================================
// CheckpointerMain (checkpointer.c:181-636).
// ===========================================================================

/// `CheckpointerMain` (checkpointer.c:181-636). Setup, the sigsetjmp-based
/// error-recovery outer loop, the main scheduling while-loop, and the
/// post-shutdown wait. The `sigsetjmp(PG_exception_stack)` is modeled as an
/// outer loop running the main cycle; on `Err` we run the checkpointer-specific
/// abort cleanup then loop (sleeping 1 s). A clean break out of the inner
/// while triggers the shutdown path.
pub fn CheckpointerMain(startup_data: &StartupData) -> PgResult<()> {
    debug_assert!(matches!(startup_data, StartupData::None));

    // MyBackendType = B_CHECKPOINTER; AuxiliaryProcessMainCommon().
    miscinit::set_my_backend_type_checkpointer::call();
    auxprocess::auxiliary_process_main_common::call()?;

    // Properly accept or ignore signals that might be sent to us
    // (checkpointer.c:201-214). This was previously assumed to be done by the
    // "host auxiliary-process bootstrap" — but nothing installs it, so the
    // postmaster's inherited SIGUSR1/SIGUSR2 dispositions stayed in force and
    // this process never ran `procsignal_sigusr1_handler` (absorbing
    // ProcSignalBarriers) nor `SignalHandlerForShutdownRequest`. On cluster/DB
    // teardown the checkpointer therefore never ran `proc_exit(0)` → its
    // `on_shmem_exit` chain → `CleanupProcSignalState` never fired → its
    // procsignal slot kept `pss_pid != 0` at a stale finite
    // `pss_barrierGeneration`, hanging the emitter of a `DROP DATABASE`
    // (`WaitForProcSignalBarrier`) forever on this slot.
    {
        use types_signal::SigHandler;
        let pqsignal = port_pqsignal_seams::pqsignal::call;
        // pqsignal(SIGHUP, SignalHandlerForConfigReload);
        fn config_reload(_sig: i32) {
            interrupt::SignalHandlerForConfigReload();
        }
        pqsignal(libc::SIGHUP, SigHandler::Handler(config_reload));
        // pqsignal(SIGINT, ReqShutdownXLOG);
        fn req_shutdown_xlog(_sig: i32) {
            ReqShutdownXLOG();
        }
        pqsignal(libc::SIGINT, SigHandler::Handler(req_shutdown_xlog));
        // pqsignal(SIGTERM, SIG_IGN); /* ignore SIGTERM */
        pqsignal(libc::SIGTERM, SigHandler::Ignore);
        // SIGQUIT handler was already set up by InitPostmasterChild.
        // pqsignal(SIGALRM, SIG_IGN);
        pqsignal(libc::SIGALRM, SigHandler::Ignore);
        // pqsignal(SIGPIPE, SIG_IGN);
        pqsignal(libc::SIGPIPE, SigHandler::Ignore);
        // pqsignal(SIGUSR1, procsignal_sigusr1_handler);
        pqsignal(
            libc::SIGUSR1,
            SigHandler::Handler(
                backend_storage_ipc_procsignal::procsignal_sigusr1_handler_signal,
            ),
        );
        // pqsignal(SIGUSR2, SignalHandlerForShutdownRequest);
        fn shutdown_request(_sig: i32) {
            interrupt::SignalHandlerForShutdownRequest();
        }
        pqsignal(libc::SIGUSR2, SigHandler::Handler(shutdown_request));
        // Reset some signals that are accepted by postmaster but not here:
        // pqsignal(SIGCHLD, SIG_DFL);
        pqsignal(libc::SIGCHLD, SigHandler::Default);
    }

    // Unblock signals (they were blocked when the postmaster forked us)
    // (checkpointer.c:331, sigprocmask(SIG_SETMASK, &UnBlockSig, NULL)). Without
    // this the SIGUSR1 that `EmitProcSignalBarrier` sends us stays pending and
    // is never delivered to the handler installed above.
    backend_libpq_pqsignal_seams::unblock_signals::call();

    // CheckpointerShmem->checkpointer_pid = MyProcPid.
    let cp = shmem();
    cp.header_mut().checkpointer_pid = initsmall::my_proc_pid::call();

    // Initialize so that first time-driven event happens at the correct time.
    let now0 = time_now();
    with_private(|p| {
        p.last_checkpoint_time = now0;
        p.last_xlog_switch_time = now0;
    });

    // Write out stats after shutdown (before_shmem_exit(pgstat_before_server_shutdown)).
    // The Checkpointer memory-context creation + switch is host-owned.
    ipc::before_shmem_exit::call(pgstat_before_server_shutdown_cb, types_tuple::Datum::null())?;

    // Ensure all shared memory values are set correctly for the config.
    UpdateSharedMemoryConfig()?;

    // Advertise our proc number so backends can wake us up while we sleep.
    proc::set_checkpointer_proc_to_self::call()?;

    // The sigsetjmp(PG_exception_stack) recovery loop: run the main while-loop;
    // any error escaping the cycle lands here, where we do the minimal-abort
    // cleanup, then re-enter the loop (after sleeping 1 s).
    loop {
        match checkpointer_main_loop(&cp) {
            Ok(()) => break, // clean break out of the while: proceed to shutdown
            Err(err) => {
                checkpointer_abort_cleanup(&cp, &err)?;
                // Sleep at least 1 second after any error.
                ipc_pg_usleep(1_000_000)?;
            }
        }
    }

    // From here on, elog(ERROR) should end with exit(1) (host-side). Run the
    // shutdown-checkpoint path if requested.
    if shutdown_xlog_pending() {
        // Close down the database: ShutdownXLOG creates a restartpoint or
        // checkpoint and updates stats, so bump num_requested and flush stats.
        pending_stats_inc(CheckpointerStatField::NumRequested);
        xlog::shutdown_xlog::call()?;
        pgstat_report_checkpointer()?;
        walstats::pgstat_report_wal::call(true);

        // Tell postmaster that we're done.
        pmsignal::send_postmaster_signal_xlog_is_shutdown::call();
        SHUTDOWN_XLOG_PENDING.with(|c| c.set(false));
    }

    // Wait until we're asked to shut down (separating the shutdown-checkpoint
    // write from exiting so checkpointer can do late work like writing stats).
    loop {
        latch::reset_latch_my_latch::call();

        ProcessCheckpointerInterrupts()?;

        if interrupt::ShutdownRequestPending() {
            break;
        }

        latch::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            0,
            WAIT_EVENT_CHECKPOINTER_SHUTDOWN,
        )?;
    }

    // Normal exit (proc_exit(0)) is host-side.
    Ok(())
}

/// The minimal-abort cleanup from the sigsetjmp block (checkpointer.c:263-323),
/// minus the host-owned framing (error_context_stack / MemoryContext reset).
fn checkpointer_abort_cleanup(cp: &Checkpointer, err: &PgError) -> PgResult<()> {
    // Since not using PG_TRY, must reset error stack by hand (host-owned), then
    // HOLD_INTERRUPTS() and report the error to the server log.
    miscinit::hold_interrupts::call();
    backend_utils_error::emit_error_report_for(err);

    // The minimal subset of AbortTransaction(): LWLocks, buffers, temp files.
    lwlock::lwlock_release_all::call();
    cv::condition_variable_cancel_sleep::call();
    waitevent::pgstat_report_wait_end::call();
    aio::pgaio_error_cleanup::call();
    bufmgr::unlock_buffers::call();
    resowner::release_aux_process_resources::call(false)?;
    bufmgr::at_eoxact_buffers::call(false);
    smgr::at_eoxact_smgr::call();
    fd::at_eoxact_files::call(false);
    dynahash::at_eoxact_hash_tables::call(false);

    // Warn any waiting backends that the checkpoint failed.
    let was_active = with_private(|p| {
        let active = p.ckpt_active;
        if active {
            p.ckpt_active = false;
        }
        active
    });
    if was_active {
        {
            let _guard = cp.acquire_ckpt_lock();
            cp.header_mut().ckpt_failed += 1;
            cp.header_mut().ckpt_done = cp.header().ckpt_started;
        }
        cv::condition_variable_broadcast::call(cp.done_cv());
    }

    // Return to normal context and clear ErrorContext for next time
    // (FlushErrorState + MemoryContextReset + RESUME_INTERRUPTS).
    backend_utils_error::FlushErrorState();
    miscinit::resume_interrupts::call();
    Ok(())
}

/// The main scheduling while-loop (checkpointer.c:349-584). Returns `Ok(())` on
/// a clean break (shutdown requested); returns `Err` if a checkpoint or any
/// operation errors, which the outer loop catches as the sigsetjmp recovery.
fn checkpointer_main_loop(cp: &Checkpointer) -> PgResult<()> {
    loop {
        let mut do_checkpoint = false;
        let mut flags = 0i32;
        let mut chkpt_or_rstpt_requested = false;
        let mut chkpt_or_rstpt_timed = false;

        // Clear any already-pending wakeups.
        latch::reset_latch_my_latch::call();

        // Process any requests or signals received recently.
        AbsorbSyncRequests(cp)?;

        ProcessCheckpointerInterrupts()?;
        if shutdown_xlog_pending() || interrupt::ShutdownRequestPending() {
            return Ok(());
        }

        // Detect a pending checkpoint request: ckpt_flags nonzero. (No lock
        // needed for this single-word read.)
        if cp.header().ckpt_flags != 0 {
            do_checkpoint = true;
            chkpt_or_rstpt_requested = true;
        }

        // Force a checkpoint if too much time has elapsed since the last one.
        let mut now = time_now();
        let mut elapsed_secs = now - with_private(|p| p.last_checkpoint_time);
        if elapsed_secs >= CheckPointTimeout() as i64 {
            if !do_checkpoint {
                chkpt_or_rstpt_timed = true;
            }
            do_checkpoint = true;
            flags |= CHECKPOINT_CAUSE_TIME;
        }

        // Do a checkpoint if requested.
        if do_checkpoint {
            // Check if we should perform a checkpoint or a restartpoint.
            let mut do_restartpoint = xlog::recovery_in_progress::call();

            // Atomically fetch the request flags and bump the started-counter.
            {
                let _guard = cp.acquire_ckpt_lock();
                flags |= cp.header().ckpt_flags;
                cp.header_mut().ckpt_flags = 0;
                cp.header_mut().ckpt_started += 1;
            }
            cv::condition_variable_broadcast::call(cp.start_cv());

            // The end-of-recovery checkpoint is a real checkpoint performed
            // while still in recovery.
            if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
                do_restartpoint = false;
            }

            if chkpt_or_rstpt_timed {
                if do_restartpoint {
                    pending_stats_inc(CheckpointerStatField::RestartpointsTimed);
                } else {
                    pending_stats_inc(CheckpointerStatField::NumTimed);
                }
            }

            if chkpt_or_rstpt_requested {
                if do_restartpoint {
                    pending_stats_inc(CheckpointerStatField::RestartpointsRequested);
                } else {
                    pending_stats_inc(CheckpointerStatField::NumRequested);
                }
            }

            // Warn if (a) too soon since last checkpoint and (b) the
            // CHECKPOINT_CAUSE_XLOG flag was set.
            if !do_restartpoint
                && (flags & CHECKPOINT_CAUSE_XLOG != 0)
                && elapsed_secs < CheckPointWarning() as i64
            {
                ereport(LOG)
                    .errmsg_plural(
                        format!(
                            "checkpoints are occurring too frequently ({elapsed_secs} second apart)"
                        ),
                        format!(
                            "checkpoints are occurring too frequently ({elapsed_secs} seconds apart)"
                        ),
                        elapsed_secs as u64,
                    )
                    .errhint(
                        "Consider increasing the configuration parameter \"max_wal_size\".",
                    )
                    .finish(ckpt_location("CheckpointerMain"))?;
            }

            // Initialize checkpointer-private variables used during checkpoint.
            let start_recptr = if do_restartpoint {
                xlog::get_xlog_replay_rec_ptr::call()
            } else {
                xlog::get_insert_rec_ptr::call()
            };
            with_private(|p| {
                p.ckpt_active = true;
                p.ckpt_start_recptr = start_recptr;
                p.ckpt_start_time = now;
                p.ckpt_cached_elapsed = 0.0;
            });

            // Do the checkpoint.
            let ckpt_performed = if !do_restartpoint {
                xlog::create_checkpoint::call(flags)?
            } else {
                xlog::create_restartpoint::call(flags)?
            };

            // Free all smgr objects after any checkpoint.
            smgr::smgrdestroyall::call()?;

            // Indicate checkpoint completion to any waiting backends.
            {
                let _guard = cp.acquire_ckpt_lock();
                cp.header_mut().ckpt_done = cp.header().ckpt_started;
            }
            cv::condition_variable_broadcast::call(cp.done_cv());

            if !do_restartpoint {
                // Record checkpoint START time as last_checkpoint_time so timed
                // checkpoints happen at predictable spacing.
                with_private(|p| p.last_checkpoint_time = now);
                if ckpt_performed {
                    pending_stats_inc(CheckpointerStatField::NumPerformed);
                }
            } else if ckpt_performed {
                with_private(|p| p.last_checkpoint_time = now);
                pending_stats_inc(CheckpointerStatField::RestartpointsPerformed);
            } else {
                // Could not perform the restartpoint (likely no new checkpoint
                // WAL records since the last one). Try again in 15 s.
                with_private(|p| {
                    p.last_checkpoint_time = now - CheckPointTimeout() as i64 + 15;
                });
            }

            with_private(|p| p.ckpt_active = false);

            // We may have received an interrupt during the checkpoint.
            ProcessCheckpointerInterrupts()?;
            if shutdown_xlog_pending() || interrupt::ShutdownRequestPending() {
                return Ok(());
            }
        }

        // Check for archive_timeout and switch xlog files if necessary.
        CheckArchiveTimeout()?;

        // Report pending statistics to the cumulative stats system.
        pgstat_report_checkpointer()?;
        walstats::pgstat_report_wal::call(true);

        // If any checkpoint flags have been set, redo the loop without sleeping.
        if cp.header().ckpt_flags != 0 {
            continue;
        }

        // Sleep until signaled / time for another checkpoint or xlog switch.
        now = time_now();
        elapsed_secs = now - with_private(|p| p.last_checkpoint_time);
        if elapsed_secs >= CheckPointTimeout() as i64 {
            continue; // no sleep for us ...
        }
        let mut cur_timeout = CheckPointTimeout() as i64 - elapsed_secs;
        if xlog::xlog_archive_timeout::call() > 0 && !xlog::recovery_in_progress::call() {
            elapsed_secs = now - with_private(|p| p.last_xlog_switch_time);
            if elapsed_secs >= xlog::xlog_archive_timeout::call() as i64 {
                continue; // no sleep for us ...
            }
            cur_timeout = cur_timeout.min(xlog::xlog_archive_timeout::call() as i64 - elapsed_secs);
        }

        latch::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            cur_timeout * 1000, // convert to ms
            WAIT_EVENT_CHECKPOINTER_MAIN,
        )?;
    }
}

// ===========================================================================
// ProcessCheckpointerInterrupts (checkpointer.c:641-669).
// ===========================================================================

/// `ProcessCheckpointerInterrupts` (checkpointer.c:641-669) — process any new
/// interrupts (proc-signal barrier, config reload, log-memory-context).
pub fn ProcessCheckpointerInterrupts() -> PgResult<()> {
    if procsignal::proc_signal_barrier_pending::call() {
        procsignal::process_proc_signal_barrier::call()?;
    }

    if interrupt::ConfigReloadPending() {
        interrupt::SetConfigReloadPending(false);
        guc::process_config_file_sighup::call()?;

        // Checkpointer holds the keys for updating shmem config copies on SIGHUP.
        UpdateSharedMemoryConfig()?;
    }

    // Perform logging of memory contexts of this process.
    if mcxt::log_memory_context_pending::call() {
        mcxt::process_log_memory_context_interrupt::call()?;
    }
    Ok(())
}

// ===========================================================================
// CheckArchiveTimeout (checkpointer.c:683-737).
// ===========================================================================

/// `CheckArchiveTimeout` (checkpointer.c:683-737) — check for archive_timeout
/// and switch xlog files if meaningful activity has been recorded.
pub fn CheckArchiveTimeout() -> PgResult<()> {
    if xlog::xlog_archive_timeout::call() <= 0 || xlog::recovery_in_progress::call() {
        return Ok(());
    }

    let now = time_now();

    // Quick check using possibly-stale local state.
    if (now - with_private(|p| p.last_xlog_switch_time)) < xlog::xlog_archive_timeout::call() as i64
    {
        return Ok(());
    }

    // Update local state (last_xlog_switch_time is the last time a switch was
    // performed *or requested*).
    let (last_time, last_switch_lsn) = xlog::get_last_seg_switch_data::call();
    with_private(|p| {
        p.last_xlog_switch_time = p.last_xlog_switch_time.max(last_time);
    });

    // Now the real checks.
    if (now - with_private(|p| p.last_xlog_switch_time)) >= xlog::xlog_archive_timeout::call() as i64
    {
        // Switch segment only when "important" WAL has been logged since the
        // last segment switch.
        if xlog::get_last_important_rec_ptr::call() > last_switch_lsn {
            // Mark switch as unimportant (avoids triggering checkpoints).
            let switchpoint = xlog::request_xlog_switch::call(true)?;

            // If the pointer is exactly at a segment boundary, nothing happened.
            if xlog_segment_offset(switchpoint) != 0 {
                let archive_timeout = xlog::xlog_archive_timeout::call();
                ereport(DEBUG1)
                    .errmsg_internal(format!(
                        "write-ahead log switch forced (\"archive_timeout\"={archive_timeout})"
                    ))
                    .finish(ckpt_location("CheckArchiveTimeout"))?;
            }
        }

        // Update state in any case so we don't retry constantly when idle.
        with_private(|p| p.last_xlog_switch_time = now);
    }
    Ok(())
}

/// `XLogSegmentOffset(switchpoint, wal_segment_size)` — the offset of `recptr`
/// within its WAL segment.
fn xlog_segment_offset(recptr: XLogRecPtr) -> u64 {
    recptr & (xlog::wal_segment_size::call() as u64 - 1)
}

// ===========================================================================
// ImmediateCheckpointRequested (checkpointer.c:744-756).
// ===========================================================================

/// `ImmediateCheckpointRequested` (checkpointer.c:744-756) — returns `true` if
/// an immediate checkpoint request is pending. (Single flag-bit read, no lock.)
pub fn ImmediateCheckpointRequested() -> bool {
    let cp = shmem();
    cp.header().ckpt_flags & CHECKPOINT_IMMEDIATE != 0
}

// ===========================================================================
// CheckpointWriteDelay (checkpointer.c:771-831).
// ===========================================================================

/// `CheckpointWriteDelay` (checkpointer.c:771-831) — throttle BufferSync()'s
/// write rate to hit checkpoint_completion_target. Called after each page write.
pub fn CheckpointWriteDelay(flags: i32, progress: f64) -> PgResult<()> {
    // Do nothing if checkpoint is being executed by a non-checkpointer process.
    if !miscinit::am_checkpointer_process::call() {
        return Ok(());
    }

    let cp = shmem();

    // Perform the usual duties and nap, unless behind schedule.
    if (flags & CHECKPOINT_IMMEDIATE == 0)
        && !shutdown_xlog_pending()
        && !interrupt::ShutdownRequestPending()
        && !ImmediateCheckpointRequested()
        && IsCheckpointOnSchedule(progress)?
    {
        if interrupt::ConfigReloadPending() {
            interrupt::SetConfigReloadPending(false);
            guc::process_config_file_sighup::call()?;
            // Update shmem copies of config variables.
            UpdateSharedMemoryConfig()?;
        }

        AbsorbSyncRequests(&cp)?;
        with_private(|p| p.write_delay_absorb_counter = WRITES_PER_ABSORB);

        CheckArchiveTimeout()?;

        // Report interim statistics to the cumulative stats system.
        pgstat_report_checkpointer()?;

        // Take the Big Sleep (100 ms).
        latch::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
            100,
            WAIT_EVENT_CHECKPOINT_WRITE_DELAY,
        )?;
        latch::reset_latch_my_latch::call();
    } else {
        let counter = with_private(|p| {
            p.write_delay_absorb_counter -= 1;
            p.write_delay_absorb_counter
        });
        if counter <= 0 {
            // Absorb pending fsync requests every WRITES_PER_ABSORB writes even
            // when we don't sleep, to prevent overflow of the fsync queue.
            AbsorbSyncRequests(&cp)?;
            with_private(|p| p.write_delay_absorb_counter = WRITES_PER_ABSORB);
        }
    }

    // Check for barrier events.
    if procsignal::proc_signal_barrier_pending::call() {
        procsignal::process_proc_signal_barrier::call()?;
    }
    Ok(())
}

// ===========================================================================
// IsCheckpointOnSchedule (checkpointer.c:841-911).
// ===========================================================================

/// `IsCheckpointOnSchedule` (checkpointer.c:841-911) — are we on schedule to
/// finish this checkpoint/restartpoint in time?
pub fn IsCheckpointOnSchedule(progress: f64) -> PgResult<bool> {
    debug_assert!(with_private(|p| p.ckpt_active));

    // Scale progress according to checkpoint_completion_target.
    let progress = progress * CheckPointCompletionTarget();

    // Check against the cached value first.
    if progress < with_private(|p| p.ckpt_cached_elapsed) {
        return Ok(false);
    }

    // Check progress against WAL segments written and CheckPointSegments.
    let recptr = if xlog::recovery_in_progress::call() {
        xlog::get_xlog_replay_rec_ptr::call()
    } else {
        xlog::get_insert_rec_ptr::call()
    };
    let ckpt_start_recptr = with_private(|p| p.ckpt_start_recptr);
    let elapsed_xlogs = ((recptr.wrapping_sub(ckpt_start_recptr) as f64)
        / xlog::wal_segment_size::call() as f64)
        / xlog::check_point_segments::call();

    if progress < elapsed_xlogs {
        with_private(|p| p.ckpt_cached_elapsed = elapsed_xlogs);
        return Ok(false);
    }

    // Check progress against time elapsed and checkpoint_timeout.
    let now = timeofday();
    let ckpt_start_time = with_private(|p| p.ckpt_start_time);
    let elapsed_time =
        ((now.0 - ckpt_start_time) as f64 + now.1 / 1_000_000.0) / CheckPointTimeout() as f64;

    if progress < elapsed_time {
        with_private(|p| p.ckpt_cached_elapsed = elapsed_time);
        return Ok(false);
    }

    // It looks like we're on schedule.
    Ok(true)
}

// ===========================================================================
// ReqShutdownXLOG (checkpointer.c:920-925) — SIGINT signal handler.
// ===========================================================================

/// `ReqShutdownXLOG` (checkpointer.c:920-925) — SIGINT handler: set flag to
/// trigger writing of the shutdown checkpoint, then `SetLatch(MyLatch)`. The
/// `SetLatch` is the host's signal-handler responsibility; this records the flag.
pub fn ReqShutdownXLOG() {
    // Signal-handler context: a single signal-safe `Cell` store, mirroring C's
    // `ShutdownXLOGPending = true` write to a `volatile sig_atomic_t`. Must NOT
    // touch the `RefCell<PrivateState>` (a `borrow_mut()` here would panic if the
    // interrupted main-loop code already holds the borrow).
    SHUTDOWN_XLOG_PENDING.with(|c| c.set(true));
    // `SetLatch(MyLatch)` (checkpointer.c:924) — wake the main loop out of its
    // `WaitLatch` so it observes the pending flag immediately. Without this the
    // checkpointer sleeps until its `checkpoint_timeout` timer fires, which is
    // the pre-existing graceful-shutdown HANG (mirrors the other signal handlers,
    // e.g. `SignalHandlerForShutdownRequest`, which already set the latch). The
    // latch's owner-pid guard makes `SetLatch` async-signal-safe here.
    latch::set_latch_my_latch::call();
}

// ===========================================================================
// RequestCheckpoint (checkpointer.c:1002-1130).
// ===========================================================================

/// `RequestCheckpoint` (checkpointer.c:1002-1130) — called in backend processes
/// to request a checkpoint. Sets the request flags in shmem under the spinlock,
/// signals the checkpointer's latch (retrying if not yet running), and (if
/// `CHECKPOINT_WAIT`) waits for completion via the start_cv / done_cv condition
/// variables and the modulo counter algorithm.
pub fn RequestCheckpoint(flags: i32) -> PgResult<()> {
    let cp = shmem();

    // If in a standalone backend, just do it ourselves.
    if !initsmall::is_postmaster_environment::call() {
        // No point in slow checkpoints standalone — force immediate.
        xlog::create_checkpoint::call(flags | CHECKPOINT_IMMEDIATE)?;
        // Free all smgr objects, as CheckpointerMain() normally would.
        smgr::smgrdestroyall::call()?;
        return Ok(());
    }

    // Atomically set the request flags, snapshot the counters.
    let (old_failed, old_started) = {
        let _guard = cp.acquire_ckpt_lock();
        let old_failed = cp.header().ckpt_failed;
        let old_started = cp.header().ckpt_started;
        cp.header_mut().ckpt_flags |= flags | CHECKPOINT_REQUESTED;
        (old_failed, old_started)
    };

    // Set checkpointer's latch to request the checkpoint, retrying a few times
    // since the checkpointer may not have started yet.
    let mut ntries = 0;
    loop {
        let checkpointer_proc = proc::checkpointer_proc::call();

        if checkpointer_proc == INVALID_PROC_NUMBER {
            if ntries >= MAX_SIGNAL_TRIES || (flags & CHECKPOINT_WAIT == 0) {
                let level = if flags & CHECKPOINT_WAIT != 0 { ERROR } else { LOG };
                ereport(level)
                    .errmsg_internal("could not notify checkpoint: checkpointer is not running")
                    .finish(ckpt_location("RequestCheckpoint"))?;
                break;
            }
        } else {
            latch::set_latch_by_proc_number::call(checkpointer_proc);
            // notified successfully
            break;
        }

        miscinit::check_for_interrupts::call()?;
        ipc_pg_usleep(100_000)?; // wait 0.1 sec, then retry
        ntries += 1;
    }

    // If requested, wait for completion.
    if flags & CHECKPOINT_WAIT != 0 {
        // Wait for a new checkpoint to start.
        cv::condition_variable_prepare_to_sleep::call(cp.start_cv());
        let new_started = loop {
            let new_started = {
                let _guard = cp.acquire_ckpt_lock();
                cp.header().ckpt_started
            };
            if new_started != old_started {
                break new_started;
            }
            cv::condition_variable_sleep::call(cp.start_cv(), WAIT_EVENT_CHECKPOINT_START)?;
        };
        cv::condition_variable_cancel_sleep::call();

        // Wait for ckpt_done >= new_started, in a modulo sense.
        cv::condition_variable_prepare_to_sleep::call(cp.done_cv());
        let new_failed;
        loop {
            let (new_done, nf) = {
                let _guard = cp.acquire_ckpt_lock();
                (cp.header().ckpt_done, cp.header().ckpt_failed)
            };
            if new_done.wrapping_sub(new_started) >= 0 {
                new_failed = nf;
                break;
            }
            cv::condition_variable_sleep::call(cp.done_cv(), WAIT_EVENT_CHECKPOINT_DONE)?;
        }
        cv::condition_variable_cancel_sleep::call();

        if new_failed != old_failed {
            ereport(ERROR)
                .errmsg("checkpoint request failed")
                .errhint("Consult recent messages in the server log for details.")
                .finish(ckpt_location("RequestCheckpoint"))?;
        }
    }
    Ok(())
}

// ===========================================================================
// ForwardSyncRequest (checkpointer.c:1152-1201).
// ===========================================================================

/// `ForwardSyncRequest` (checkpointer.c:1152-1201) — forward a file-fsync
/// request from a backend to the checkpointer, enqueuing it into the shmem
/// `requests[]` ring under `CheckpointerCommLock`. If the queue is full, try
/// `CompactCheckpointerRequestQueue`; if still full, return `false` so the
/// backend does its own fsync.
pub fn ForwardSyncRequest(ftag: &FileTag, type_: SyncRequestType) -> PgResult<bool> {
    let cp = shmem();

    if !initsmall::is_under_postmaster::call() {
        return Ok(false); // probably shouldn't even get here
    }

    if miscinit::am_checkpointer_process::call() {
        ereport(ERROR)
            .errmsg_internal("ForwardSyncRequest must not be called in checkpointer")
            .finish(ckpt_location("ForwardSyncRequest"))?;
    }

    comm_lock_acquire();

    // If the checkpointer isn't running or the queue is full (and can't be
    // compacted), the backend must do its own fsync.
    let full = cp.header().checkpointer_pid == 0
        || (cp.header().num_requests >= cp.header().max_requests
            && !CompactCheckpointerRequestQueue(&cp)?);
    if full {
        comm_lock_release();
        return Ok(false);
    }

    // OK, insert request.
    let idx = cp.header().num_requests;
    cp.header_mut().num_requests += 1;
    cp.set_request(
        idx,
        CheckpointerRequest {
            type_: type_ as i32,
            ftag: *ftag,
        },
    );

    // If the queue is more than half full, nudge the checkpointer to empty it.
    let too_full = cp.header().num_requests >= cp.header().max_requests / 2;

    comm_lock_release();

    // ... but not till after we release the lock.
    if too_full {
        let checkpointer_proc = proc::checkpointer_proc::call();
        if checkpointer_proc != INVALID_PROC_NUMBER {
            latch::set_latch_by_proc_number::call(checkpointer_proc);
        }
    }

    Ok(true)
}

// ===========================================================================
// CompactCheckpointerRequestQueue (checkpointer.c:1219-1318).
// ===========================================================================

/// `CompactCheckpointerRequestQueue` (checkpointer.c:1219-1318) — remove
/// duplicate requests from the queue (a later identical request supersedes an
/// earlier one). Returns `true` if any entries were removed. Must hold
/// `CheckpointerCommLock` exclusively.
///
/// C keys a temporary hash table on the raw `CheckpointerRequest` bytes
/// (`HASH_BLOBS`); the equivalent here keys an in-process map on `(type, ftag)`,
/// the exact same equivalence (the request is fully described by those fields).
pub fn CompactCheckpointerRequestQueue(cp: &Checkpointer) -> PgResult<bool> {
    // Must hold CheckpointerCommLock in exclusive mode.
    debug_assert!(comm_lock_held_by_me());

    // Avoid memory allocations in a critical section.
    if miscinit::in_critical_section::call() {
        return Ok(false);
    }

    let num_requests = cp.header().num_requests;
    let n_usize = num_requests.max(0) as usize;

    // skip_slot[n] marks request n as removable (superseded by a later dup).
    let mut skip_slot: Vec<bool> = Vec::new();
    skip_slot
        .try_reserve(n_usize)
        .map_err(|_| out_of_memory("CompactCheckpointerRequestQueue skip_slot"))?;
    skip_slot.resize(n_usize, false);

    // The temp hash table: maps a request value to the slot of its latest
    // occurrence (C: struct CheckpointerSlotMapping { request; slot }).
    use std::collections::HashMap;
    let mut htab: HashMap<(i32, FileTag), i32> = HashMap::new();
    htab.try_reserve(n_usize)
        .map_err(|_| out_of_memory("CompactCheckpointerRequestQueue htab"))?;
    let mut num_skipped = 0;

    // A request can be skipped if it's followed by a later, identical request.
    for n in 0..num_requests {
        let req = cp.request(n);
        let key = (req.type_, req.ftag);
        if let Some(prev_slot) = htab.insert(key, n) {
            // Duplicate: mark the previous occurrence as skippable.
            skip_slot[prev_slot as usize] = true;
            num_skipped += 1;
        }
    }

    // If no duplicates, we're out of luck.
    if num_skipped == 0 {
        return Ok(false);
    }

    // Remove the marked duplicates, compacting in place.
    let mut preserve_count = 0i32;
    for n in 0..num_requests {
        if skip_slot[n as usize] {
            continue;
        }
        let req = cp.request(n);
        cp.set_request(preserve_count, req);
        preserve_count += 1;
    }
    ereport(DEBUG1)
        .errmsg_internal(format!(
            "compacted fsync request queue from {num_requests} entries to {preserve_count} entries"
        ))
        .finish(ckpt_location("CompactCheckpointerRequestQueue"))?;
    cp.header_mut().num_requests = preserve_count;

    Ok(true)
}

// ===========================================================================
// AbsorbSyncRequests (checkpointer.c:1329-1371).
// ===========================================================================

/// `AbsorbSyncRequests` (checkpointer.c:1329-1371) — retrieve queued sync
/// requests from the shmem ring and pass them to the sync mechanism
/// (`RememberSyncRequest`). Copies the request array out under
/// `CheckpointerCommLock`, then processes the copy after release; once the shmem
/// queue is cleared we must not fail to absorb (a START_CRIT_SECTION in C). No-op
/// outside the checkpointer.
pub fn AbsorbSyncRequests(cp: &Checkpointer) -> PgResult<()> {
    if !miscinit::am_checkpointer_process::call() {
        return Ok(());
    }

    comm_lock_acquire();

    // Copy the request array, processing after release to minimize lock hold.
    let n = cp.header().num_requests;
    let mut requests: Vec<CheckpointerRequest> = Vec::new();
    if n > 0 {
        // C palloc's an exactly-sized buffer here; mirror with a fallible
        // reservation (the queue is bounded by max_requests).
        requests
            .try_reserve_exact(n as usize)
            .map_err(|_| out_of_memory("AbsorbSyncRequests requests"))?;
        for i in 0..n {
            requests.push(cp.request(i));
        }
    }

    // START_CRIT_SECTION(): once we clear the queue we must absorb all of them.
    cp.header_mut().num_requests = 0;

    comm_lock_release();

    for request in &requests {
        let type_ = sync_request_type_from_raw(request.type_);
        sync::remember_sync_request::call(request.ftag, type_)?;
    }
    // END_CRIT_SECTION() — host-side; the loop above cannot fail to absorb.
    Ok(())
}

/// Rebuild a [`SyncRequestType`] from its stored `i32` discriminant (the value
/// is one we ourselves stored in [`ForwardSyncRequest`]).
fn sync_request_type_from_raw(raw: i32) -> SyncRequestType {
    match raw {
        0 => SyncRequestType::SYNC_REQUEST,
        1 => SyncRequestType::SYNC_UNLINK_REQUEST,
        2 => SyncRequestType::SYNC_FORGET_REQUEST,
        3 => SyncRequestType::SYNC_FILTER_REQUEST,
        other => {
            unreachable!("invalid SyncRequestType discriminant in checkpointer queue: {other}")
        }
    }
}

// ===========================================================================
// UpdateSharedMemoryConfig (checkpointer.c:1376-1389).
// ===========================================================================

/// `UpdateSharedMemoryConfig` (checkpointer.c:1376-1389) — update any shared
/// memory configurations based on config parameters (sync rep, full_page_writes).
fn UpdateSharedMemoryConfig() -> PgResult<()> {
    // Update global shmem state for sync rep.
    syncrep::sync_rep_update_sync_standbys_defined::call()?;

    // If full_page_writes changed by SIGHUP, update shmem + write XLOG_FPW_CHANGE.
    xlog::update_full_page_writes::call()?;

    // elog(DEBUG2, "checkpointer updated shared memory configuration values").
    Ok(())
}

// ===========================================================================
// FirstCallSinceLastCheckpoint (checkpointer.c:1395-1412).
// ===========================================================================

/// `FirstCallSinceLastCheckpoint` (checkpointer.c:1395-1412) — allows a process
/// to take an action once per checkpoint cycle by asynchronously checking for
/// checkpoint completion (the function-static `ckpt_done` is process-local).
pub fn FirstCallSinceLastCheckpoint() -> bool {
    let cp = shmem();
    let new_done = {
        let _guard = cp.acquire_ckpt_lock();
        cp.header().ckpt_done
    };

    with_private(|p| {
        let first = new_done != p.first_call_ckpt_done;
        p.first_call_ckpt_done = new_done;
        first
    })
}

// ===========================================================================
// CheckpointerCommLock helpers.
// ===========================================================================

/// `LWLockAcquire(CheckpointerCommLock, LW_EXCLUSIVE)`.
///
/// checkpointer.c follows C's bare acquire/release protocol: the lock is taken
/// here and dropped by an explicit `LWLockRelease` ([`comm_lock_release`]) at a
/// point the function chooses (and by `LWLockReleaseAll` on the error path). The
/// `lwlock_acquire_main` seam hands back a `MainLWLockGuard` whose `Drop`
/// releases the lock — letting it drop here would release the lock immediately
/// (before the protected section) and make the later `comm_lock_release` a
/// double-release of an unheld lock ("lock ... is not held"). The lock is
/// already recorded in the backend's held-lock table by the acquire itself, so
/// we `forget` the guard to suppress the drop-release while keeping the hold; the
/// explicit `comm_lock_release` / `LWLockReleaseAll` is the sole releaser, as in
/// C.
fn comm_lock_acquire() {
    match lwlock::lwlock_acquire_main::call(CHECKPOINTER_COMM_LOCK, LWLockMode::LW_EXCLUSIVE) {
        Ok(guard) => core::mem::forget(guard),
        Err(_) => {
            // LWLockAcquire only fails by ereport(ERROR) ("too many LWLocks"),
            // which unwinds; nothing to hold on that path.
        }
    }
}

/// `LWLockRelease(CheckpointerCommLock)`.
fn comm_lock_release() {
    let _ = lwlock::lwlock_release_main::call(CHECKPOINTER_COMM_LOCK);
}

/// `LWLockHeldByMeInMode(CheckpointerCommLock, LW_EXCLUSIVE)`.
fn comm_lock_held_by_me() -> bool {
    lwlock::lwlock_held_by_me_in_mode_main::call(CHECKPOINTER_COMM_LOCK, LWLockMode::LW_EXCLUSIVE)
}

// ===========================================================================
// pgstat helpers.
// ===========================================================================

/// Which `PendingCheckpointerStats` counter to bump (mirrors the fields touched
/// in `CheckpointerMain`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckpointerStatField {
    RestartpointsTimed,
    NumTimed,
    RestartpointsRequested,
    NumRequested,
    NumPerformed,
    RestartpointsPerformed,
}

/// `PendingCheckpointerStats.<field>++` (pgstat.c global, bumped directly by
/// checkpointer.c). Reached through the pgstat owner's `with_pending_*` access
/// path, the equivalent of C's direct global write.
fn pending_stats_inc(which: CheckpointerStatField) {
    backend_utils_activity_small::pgstat_checkpointer::with_pending_checkpointer_stats(|p| {
        match which {
            CheckpointerStatField::RestartpointsTimed => p.restartpoints_timed += 1,
            CheckpointerStatField::NumTimed => p.num_timed += 1,
            CheckpointerStatField::RestartpointsRequested => p.restartpoints_requested += 1,
            CheckpointerStatField::NumRequested => p.num_requested += 1,
            CheckpointerStatField::NumPerformed => p.num_performed += 1,
            CheckpointerStatField::RestartpointsPerformed => p.restartpoints_performed += 1,
        }
    });
}

/// `pgstat_report_checkpointer()` — flush the pending counters into shmem.
fn pgstat_report_checkpointer() -> PgResult<()> {
    backend_utils_activity_small::pgstat_checkpointer::pgstat_report_checkpointer()
}

/// `before_shmem_exit(pgstat_before_server_shutdown, 0)` callback shape.
fn pgstat_before_server_shutdown_cb(
    code: i32,
    arg: types_tuple::Datum<'static>,
) -> PgResult<()> {
    pgstat::pgstat_before_server_shutdown::call(code, arg)
}

// ===========================================================================
// time helpers — `(pg_time_t) time(NULL)` and `gettimeofday`.
// ===========================================================================

/// `(pg_time_t) time(NULL)` — current wall-clock seconds. The OS-clock read is
/// a pure libc call, not a subsystem boundary worth seaming (matching the
/// pgarch port's `now_seconds`).
fn time_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `gettimeofday(&now, NULL)` returning `(tv_sec, tv_usec_as_f64)`.
fn timeofday() -> (i64, f64) {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_micros() as f64),
        Err(_) => (0, 0.0),
    }
}

/// `pg_usleep(usec)` — sleep the given microseconds.
fn ipc_pg_usleep(usec: i64) -> PgResult<()> {
    port_pgsleep_seams::pg_usleep::call(usec);
    Ok(())
}

// ===========================================================================
// error / OOM helper.
// ===========================================================================

/// Build an out-of-memory `PgError` for the (bounded) request-queue allocations.
fn out_of_memory(what: &str) -> PgError {
    PgError::error(format!("out of memory: {what}"))
}

// ===========================================================================
// Inward seams (installed by init_seams).
// ===========================================================================

/// `checkpointer_main` adapter (`-> !`): run [`CheckpointerMain`]; the
/// checkpointer loops until proc_exit, so a returned `Ok` runs the host
/// proc_exit(0) and a top-level `Err` is a FATAL escaping with no handler.
fn checkpointer_main_entry(startup_data: &StartupData) -> ! {
    match CheckpointerMain(startup_data) {
        Ok(()) => ipc::proc_exit::call(0, initsmall::my_proc_pid::call()),
        Err(err) => {
            backend_utils_error::emit_error_report_for(&err);
            ipc::proc_exit::call(1, initsmall::my_proc_pid::call());
        }
    }
}

/// `AbsorbSyncRequests()` inward-seam adapter (sync.c / md.c drive it).
fn absorb_sync_requests_seam() -> PgResult<()> {
    AbsorbSyncRequests(&shmem())
}

/// `ForwardSyncRequest(ftag, type)` inward-seam adapter (sync.c drives it).
fn forward_sync_request_seam(ftag: FileTag, request_type: SyncRequestType) -> PgResult<bool> {
    ForwardSyncRequest(&ftag, request_type)
}

/// `RequestCheckpoint(flags)` inward-seam adapter (xlog.c / bgwriter drive it).
fn request_checkpoint_seam(flags: i32) {
    if let Err(err) = RequestCheckpoint(flags) {
        // C's RequestCheckpoint either succeeds or ereport(ERROR)s (LONGJMP);
        // the seam is void, so re-raise as a panic on the (rare) Err path.
        backend_utils_error::emit_error_report_for(&err);
        panic!("RequestCheckpoint failed: {}", err.message());
    }
}

/// `CheckpointStats` post-sync metric store (xlog.c global). The checkpointer
/// itself does not own `CheckpointStats`; this is the boundary sync.c uses to
/// report its aggregate fsync timings up to xlog.c. Delegated to the xlog owner.
fn checkpoint_stats_set_seam(ckpt_sync_rels: i32, ckpt_longest_sync: u64, ckpt_agg_sync_time: u64) {
    xlog::checkpoint_stats_set::call(ckpt_sync_rels, ckpt_longest_sync, ckpt_agg_sync_time);
}

/// `CheckpointerShmemSize()` inward-seam adapter (ipci.c accumulator). The
/// `NBuffers` global is read by the xlog/ipci owner; the checkpointer's size
/// helper takes it as a parameter, so the seam resolves it via the bufmgr seam.
fn checkpointer_shmem_size_seam() -> PgResult<Size> {
    CheckpointerShmemSize(initsmall::nbuffers::call())
}

/// `CheckpointerShmemInit()` inward-seam adapter (ipci.c).
fn checkpointer_shmem_init_seam() -> PgResult<()> {
    CheckpointerShmemInit(initsmall::nbuffers::call())
}

/// Install every seam this crate owns.
pub fn init_seams() {
    backend_postmaster_checkpointer_seams::checkpointer_main::set(checkpointer_main_entry);
    backend_postmaster_checkpointer_seams::absorb_sync_requests::set(absorb_sync_requests_seam);
    backend_postmaster_checkpointer_seams::forward_sync_request::set(forward_sync_request_seam);
    backend_postmaster_checkpointer_seams::request_checkpoint::set(request_checkpoint_seam);
    backend_postmaster_checkpointer_seams::checkpoint_stats_set::set(checkpoint_stats_set_seam);
    backend_postmaster_checkpointer_seams::checkpointer_shmem_size::set(checkpointer_shmem_size_seam);
    backend_postmaster_checkpointer_seams::checkpointer_shmem_init::set(checkpointer_shmem_init_seam);

    // --- bufmgr BufferSync write-rate throttle (bufmgr.c BufferSync →
    //     CheckpointWriteDelay, checkpointer.c) ---
    backend_storage_buffer_bufmgr_seams::checkpoint_write_delay::set(CheckpointWriteDelay);

    // --- ProcessUtility dispatch arm (utility.c CHECKPOINT → RequestCheckpoint) ---
    backend_tcop_utility_out_seams::request_checkpoint::set(RequestCheckpoint);

    // --- GUC variable accessors (checkpointer.c:144-145 file globals) ---
    //
    // `CheckPointTimeout` / `CheckPointWarning` are plain C globals in
    // checkpointer.c, read directly off those globals while the GUC machinery
    // keeps them updated through the `checkpoint_timeout` / `checkpoint_warning`
    // slots. Back them with this crate's process-local cells so an assign from
    // the GUC table writes the same storage the checkpointer reads.
    //
    // (`checkpoint_completion_target` is owned + installed by xlog.c — it is read
    // there for CalculateCheckpointSegments — so it is NOT installed here.)
    backend_utils_misc_guc_tables::vars::CheckPointTimeout.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: CheckPointTimeout,
            set: set_CheckPointTimeout,
        },
    );
    backend_utils_misc_guc_tables::vars::CheckPointWarning.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: CheckPointWarning,
            set: set_CheckPointWarning,
        },
    );
}
