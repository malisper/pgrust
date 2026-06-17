//! Port of `src/backend/postmaster/bgwriter.c` (PostgreSQL 18.3): the
//! background writer auxiliary process.
//!
//! The background writer keeps regular backends from having to write out dirty
//! shared buffers themselves: each cycle it scans the buffer pool's LRU
//! (`BgBufferSync`) and pages dirty buffers out so foreground backends find
//! clean victims. It is an *auxiliary* process started by the postmaster via
//! `AuxiliaryProcessMain`, which has already built the basic execution
//! environment but has not enabled signals yet. As of PostgreSQL 9.2 the
//! bgwriter no longer handles checkpoints. Normal termination is by `SIGTERM`
//! (`exit(0)`); emergency termination is by `SIGQUIT`. If the bgwriter exits
//! unexpectedly the postmaster treats it like a backend crash.
//!
//! # Process structure
//!
//! `BackgroundWriterMain` is ported faithfully, mirroring the sibling
//! `checkpointer.c` / `walsummarizer.c` conventions:
//!
//!   * `MyBackendType = B_BG_WRITER` then `AuxiliaryProcessMainCommon()`; the
//!     `pqsignal()` block (SIGHUP/SIGINT/SIGTERM/SIGALRM/SIGPIPE/SIGUSR1/SIGUSR2
//!     and the SIGCHLD reset) is performed by the host's auxiliary-process
//!     bootstrap, exactly as in the checkpointer port.
//!   * the file-scope `last_snapshot_ts` / `last_snapshot_lsn` statics and the
//!     `prev_hibernate` local are per-backend process state, held in
//!     [`LoopState`] (AGENTS.md backend-global-state rule).
//!   * the `sigsetjmp(local_sigjmp_buf, 1)` error-recovery landing pad is
//!     modeled as an outer loop whose body returns [`PgResult`]: a returned
//!     `Err` (PostgreSQL's `ereport(ERROR)` longjmp) runs the minimal-abort
//!     cleanup, sleeps 1 s, resets `prev_hibernate`, and re-enters the loop —
//!     exactly as the C re-enters `for(;;)` after the longjmp.
//!   * the per-cycle pacing (clear latch, process interrupts, one
//!     `BgBufferSync`, report stats, free smgr after a checkpoint, log a standby
//!     snapshot on the `LOG_SNAPSHOT_INTERVAL_MS` cadence, then `WaitLatch` for
//!     `BgWriterDelay` ms) and the two-consecutive-idle hibernation condition.
//!
//! # What this crate drives directly vs. via seams
//!
//! `BgBufferSync` / `WritebackContextInit` / `StrategyNotifyBgWriter` are the
//! fully-ported buffer-manager LRU machinery, driven directly through the real
//! `backend-storage-buffer-bufmgr` crate: the [`BgBufferSyncState`] cross-call
//! state is designed to be owned by *this* main loop and threaded back in on
//! every call (replacing the C function-static variables).
//! `FirstCallSinceLastCheckpoint` is the ported checkpointer accessor, and
//! `pgstat_report_bgwriter` is the ported cumulative-stats report. The remaining
//! cross-subsystem calls (aux-process setup, latch syscalls, the minimal-abort
//! cleanup substrate, xlog standby-snapshot machinery, smgr destroy, the WAL
//! stats flush, the post-error sleep) reach their owners through `*-seams`
//! crates, panicking loudly until the owner installs them.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::Cell;

use types_core::{ProcNumber, TimestampTz, XLogRecPtr, InvalidXLogRecPtr, INVALID_PROC_NUMBER};
use types_error::{PgError, PgResult};
use types_pgstat::wait_event::{WAIT_EVENT_BGWRITER_HIBERNATE, WAIT_EVENT_BGWRITER_MAIN};
use types_startup::StartupData;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use backend_storage_buffer_bufmgr::{
    writeback_context_init, BgBufferSyncState, BufferManager, WritebackContext,
};

use backend_postmaster_interrupt as interrupt;

use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_ipc_dsm_core_seams as ipc;
use backend_storage_ipc_latch_seams as latch;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_storage_lmgr_condition_variable_seams as cv;
use backend_storage_aio_aio_seams as aio;
use backend_storage_file_fd_seams as fd;
use backend_storage_smgr_seams as smgr;
use backend_postmaster_auxprocess_seams as auxprocess;
use backend_utils_init_small_seams as initsmall;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_resowner_seams as resowner;
use backend_utils_hash_dynahash_seams as dynahash;
use backend_utils_activity_waitevent_seams as waitevent;
use backend_utils_activity_pgstat_wal_seams as walstats;
use backend_utils_adt_timestamp_seams as timestamp;
use backend_access_transam_xlog_seams as xlog;
use port_pgsleep_seams as pgsleep;

#[cfg(test)]
mod tests;

// ===========================================================================
// GUC parameters (bgwriter.c:60).
// ===========================================================================

thread_local! {
    /// `int BgWriterDelay = 200;` — the bgwriter's sleep between rounds, in
    /// milliseconds. In C a plain global the GUC machinery keeps updated;
    /// process-local here.
    static BGWRITER_DELAY: Cell<i32> = const { Cell::new(200) };
}

/// Read `BgWriterDelay` (milliseconds).
pub fn BgWriterDelay() -> i32 {
    BGWRITER_DELAY.with(Cell::get)
}

/// Assign `BgWriterDelay` (used by the GUC machinery / tests).
pub fn set_BgWriterDelay(value: i32) {
    BGWRITER_DELAY.with(|c| c.set(value));
}

// ===========================================================================
// Constants (bgwriter.c:64-78).
// ===========================================================================

/// `HIBERNATE_FACTOR` (bgwriter.c:65) — multiplier to apply to `BgWriterDelay`
/// when we decide to hibernate.
const HIBERNATE_FACTOR: i32 = 50;

/// `LOG_SNAPSHOT_INTERVAL_MS` (bgwriter.c:71) — interval in which standby
/// snapshots are logged into the WAL stream, in milliseconds.
const LOG_SNAPSHOT_INTERVAL_MS: TimestampTz = 15000;

/// `TimestampTzPlusMilliseconds(tz, ms)` (utils/timestamp.h): add a millisecond
/// count to a `TimestampTz` (which counts microseconds).
fn TimestampTzPlusMilliseconds(tz: TimestampTz, ms: TimestampTz) -> TimestampTz {
    tz + ms * 1000
}

// ===========================================================================
// Per-backend process state (the file-scope statics + the prev_hibernate local).
// ===========================================================================

/// State carried across iterations of the main loop and across error recovery.
///
/// In C these are: the `prev_hibernate` local, plus the file-scope statics
/// `last_snapshot_ts` / `last_snapshot_lsn` (bgwriter.c:76-77) recording when we
/// last issued a `LogStandbySnapshot()`, plus the `WritebackContext wb_context`
/// and the `BgBufferSync` cross-call state.
struct LoopState {
    /// `bool prev_hibernate` — whether the previous cycle reported it was OK to
    /// hibernate (we only hibernate after two consecutive idle cycles).
    prev_hibernate: bool,
    /// `static TimestampTz last_snapshot_ts` — timestamp at which we last issued
    /// a `LogStandbySnapshot()`.
    last_snapshot_ts: TimestampTz,
    /// `static XLogRecPtr last_snapshot_lsn` — LSN just past the end of the last
    /// snapshot record (`InvalidXLogRecPtr` at start).
    last_snapshot_lsn: XLogRecPtr,
    /// `WritebackContext wb_context` — the writeback accumulator passed to every
    /// `BgBufferSync`. Re-initialised after any error.
    wb_context: WritebackContext,
    /// The `BgBufferSync` function-static cross-call state (`saved_info_valid`,
    /// `prev_strategy_*`, `next_to_clean`, `next_passes`, `smoothed_*`), owned by
    /// this loop and threaded back in each cycle.
    bg_buffer_sync_state: BgBufferSyncState,
}

// ===========================================================================
// BackgroundWriterMain (bgwriter.c:85-339).
// ===========================================================================

/// `BackgroundWriterMain(startup_data, startup_data_len)` (bgwriter.c:85-339).
///
/// Invoked from `AuxiliaryProcessMain`, which has already created the basic
/// execution environment but not enabled signals yet. `startup_data` is always
/// empty for the bgwriter (`Assert(startup_data_len == 0)`).
///
/// In a live build the `for(;;)` loop never returns: `ProcessMainLoopInterrupts`
/// `proc_exit(0)`s on `ShutdownRequestPending`, and `SIGQUIT` aborts the
/// process. Returning `Ok(())` here would only happen if the interrupt seam's
/// installed implementation diverges via `proc_exit`; the `PgResult` return type
/// exists so the structure stays testable.
pub fn BackgroundWriterMain(startup_data: &StartupData) -> PgResult<()> {
    debug_assert!(matches!(startup_data, StartupData::None));

    // MyBackendType = B_BG_WRITER; AuxiliaryProcessMainCommon(). The pqsignal()
    // block (SIGHUP/SIGINT/SIGTERM/SIGALRM/SIGPIPE/SIGUSR1/SIGUSR2) and the
    // SIGCHLD reset are performed by the host's auxiliary-process bootstrap (it
    // routes SignalHandlerForConfigReload / SignalHandlerForShutdownRequest /
    // procsignal_sigusr1_handler), exactly as in the checkpointer port.
    miscinit::set_my_backend_type_bg_writer::call();
    auxprocess::auxiliary_process_main_common::call()?;

    // We just started, assume there has been either a shutdown or
    // end-of-recovery snapshot.
    //
    // Create a memory context that we will do all our work in (in C,
    // AllocSetContextCreate(TopMemoryContext, "Background Writer", ...) +
    // MemoryContextSwitchTo). In this port the per-cycle allocations are owned
    // by the cycle, so there is no separate long-lived context to reset; the
    // WritebackContextInit below is the buffer subsystem's writeback context.
    let mut wb_context = WritebackContext::default();
    writeback_context_init(&mut wb_context, bufmgr::bgwriter_flush_after::call());

    let mut state = LoopState {
        prev_hibernate: false,
        last_snapshot_ts: timestamp::get_current_timestamp::call(),
        last_snapshot_lsn: InvalidXLogRecPtr,
        wb_context,
        bg_buffer_sync_state: BgBufferSyncState::default(),
    };

    // The sigsetjmp(local_sigjmp_buf, 1) landing pad: in safe Rust the outer
    // loop re-runs the body after running the abort cleanup on error, just as
    // the C re-enters for(;;) after longjmp. The HOLD_INTERRUPTS/RESUME pair
    // (inside error_recovery) protects the cleanup, as in C.
    //
    // The C `prev_hibernate = false;` after PG_exception_stack is set, and again
    // implicitly each time the loop re-enters from the landing pad, is the reset
    // performed in the `Err` arm below.
    loop {
        match main_loop_cycle(&mut state) {
            Ok(()) => {}
            Err(err) => {
                error_recovery(&mut state, &err)?;
                // Reset hibernation state after any error.
                state.prev_hibernate = false;
            }
        }
    }
}

/// The minimal-abort cleanup block that runs when the cycle body returns `Err`
/// (the equivalent of PostgreSQL's `sigsetjmp` landing pad, bgwriter.c:163-218).
/// Mirrors the C error branch step-for-step, minus the host-owned framing
/// (`error_context_stack` reset and the MemoryContext switch/reset).
fn error_recovery(state: &mut LoopState, err: &PgError) -> PgResult<()> {
    // Since not using PG_TRY, must reset error stack by hand (host-owned), then
    // HOLD_INTERRUPTS() and report the error to the server log.
    miscinit::hold_interrupts::call();
    backend_utils_error::emit_error_report_for(err);

    // These operations are really just a minimal subset of AbortTransaction().
    // We don't have very many resources to worry about in bgwriter, but we do
    // have LWLocks, buffers, and temp files.
    lwlock::lwlock_release_all::call();
    let _ = cv::condition_variable_cancel_sleep::call();
    aio::pgaio_error_cleanup::call();
    buffer_manager().UnlockBuffers();
    resowner::release_aux_process_resources::call(false)?;
    buffer_manager().AtEOXact_Buffers(false)?;
    smgr::at_eoxact_smgr::call();
    fd::at_eoxact_files::call(false);
    dynahash::at_eoxact_hash_tables::call(false);

    // Now return to normal top-level context and clear ErrorContext for next
    // time (FlushErrorState; the C MemoryContextSwitchTo(bgwriter_context) +
    // MemoryContextReset is host-owned — the per-cycle work allocations are
    // owned by the cycle in this port, so there is no long-lived leak to reset).
    backend_utils_error::FlushErrorState();

    // re-initialize to avoid repeated errors causing problems.
    writeback_context_init(&mut state.wb_context, bufmgr::bgwriter_flush_after::call());

    // Now we can allow interrupts again.
    miscinit::resume_interrupts::call();

    // Sleep at least 1 second after any error. A write error is likely to be
    // repeated, and we don't want to be filling the error logs as fast as we
    // can.
    pgsleep::pg_usleep::call(1_000_000);

    // Report wait end here, when there is no further possibility of wait.
    waitevent::pgstat_report_wait_end::call();

    Ok(())
}

/// One iteration of the bgwriter `for(;;)` main loop (bgwriter.c:230-338). A
/// returned `Err` corresponds to a PostgreSQL `ereport(ERROR)` longjmp back to
/// the sigsetjmp landing pad.
fn main_loop_cycle(state: &mut LoopState) -> PgResult<()> {
    // Clear any already-pending wakeups. (ResetLatch(MyLatch))
    latch::reset_latch_my_latch::call();

    interrupt::ProcessMainLoopInterrupts()?;

    // Do one cycle of dirty-buffer writing.
    let can_hibernate = buffer_manager()
        .BgBufferSync(&mut state.wb_context, &mut state.bg_buffer_sync_state)?;

    // Report pending statistics to the cumulative stats system.
    backend_utils_activity_small::pgstat_report_bgwriter()?;
    walstats::pgstat_report_wal::call(true);

    if backend_postmaster_checkpointer::FirstCallSinceLastCheckpoint() {
        // After any checkpoint, free all smgr objects. Otherwise we would never
        // do so for dropped relations, as the bgwriter does not process shared
        // invalidation messages or call AtEOXact_SMgr().
        smgr::smgrdestroyall::call()?;
    }

    // Log a new xl_running_xacts every now and then so replication can get into
    // a consistent state faster (think of suboverflowed snapshots) and clean up
    // resources (locks, KnownXids*) more frequently. The costs of this are
    // relatively low, so doing it LOG_SNAPSHOT_INTERVAL_MS-often seems fine.
    //
    // We assume the interval for writing xl_running_xacts is significantly
    // bigger than BgWriterDelay, so we don't complicate the overall timeout
    // handling but just assume we're going to get called often enough even if
    // hibernation mode is active. To make sure we're not waking the disk up
    // unnecessarily on an idle system we check whether there has been any WAL
    // inserted since the last time we've logged a running xacts.
    //
    // We do this logging in the bgwriter as it is the only process that is run
    // regularly and returns to its mainloop all the time.
    if xlog::xlog_standby_info_active::call() && !xlog::recovery_in_progress::call() {
        let now = timestamp::get_current_timestamp::call();
        let timeout =
            TimestampTzPlusMilliseconds(state.last_snapshot_ts, LOG_SNAPSHOT_INTERVAL_MS);

        // Only log if enough time has passed and interesting records have been
        // inserted since the last snapshot. Have to compare with <= instead of <
        // because GetLastImportantRecPtr() points at the start of a record,
        // whereas last_snapshot_lsn points just past the end of the record.
        if now >= timeout && state.last_snapshot_lsn <= xlog::get_last_important_rec_ptr::call() {
            state.last_snapshot_lsn = xlog::log_standby_snapshot::call()?;
            state.last_snapshot_ts = now;
        }
    }

    // Sleep until we are signaled or BgWriterDelay has elapsed.
    //
    // Note: the feedback control loop in BgBufferSync() expects that we will
    // call it every BgWriterDelay msec. While it's not critical for correctness
    // that that be exact, the feedback loop might misbehave if we stray too far
    // from that. Hence, avoid loading this process down with latch events that
    // are likely to happen frequently during normal operation.
    let rc = latch::wait_latch_my_latch::call(
        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
        BgWriterDelay() as i64, /* ms */
        WAIT_EVENT_BGWRITER_MAIN,
    )?;

    // If no latch event and BgBufferSync says nothing's happening, extend the
    // sleep in "hibernation" mode, where we sleep for much longer than
    // bgwriter_delay says. Fewer wakeups save electricity. When a backend starts
    // using buffers again, it will wake us up by setting our latch. Because the
    // extra sleep will persist only as long as no buffer allocations happen,
    // this should not distort the behavior of BgBufferSync's control loop too
    // badly; essentially, it will think that the system-wide idle interval
    // didn't exist.
    //
    // There is a race condition here, in that a backend might allocate a buffer
    // between the time BgBufferSync saw the alloc count as zero and the time we
    // call StrategyNotifyBgWriter. While it's not critical that we not hibernate
    // anyway, we try to reduce the odds of that by only hibernating when
    // BgBufferSync says nothing's happening for two consecutive cycles. Also, we
    // mitigate any possible consequences of a missed wakeup by not hibernating
    // forever.
    if rc == WL_TIMEOUT && can_hibernate && state.prev_hibernate {
        // Ask for notification at next buffer allocation.
        buffer_manager().StrategyNotifyBgWriter(MyProcNumber())?;
        // Sleep ...
        let _ = latch::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            BgWriterDelay() as i64 * HIBERNATE_FACTOR as i64,
            WAIT_EVENT_BGWRITER_HIBERNATE,
        )?;
        // Reset the notification request in case we timed out.
        buffer_manager().StrategyNotifyBgWriter(INVALID_PROC_NUMBER)?;
    }

    state.prev_hibernate = can_hibernate;

    Ok(())
}

/// `MyProcNumber` — the current process's `ProcNumber`. `StrategyNotifyBgWriter`
/// registers this process for a next-allocation wakeup.
fn MyProcNumber() -> ProcNumber {
    initsmall::my_proc_number::call()
}

/// This backend's ambient buffer manager (`BufferManager::global`), which the
/// aux-process bootstrap (`BufferManagerShmemInit` / `register_global`) has
/// published before the bgwriter loop runs.
fn buffer_manager() -> &'static BufferManager {
    BufferManager::global()
        .expect("bgwriter: the buffer manager is not registered for this process")
}

// ===========================================================================
// Inward seam (installed by init_seams).
// ===========================================================================

/// `BackgroundWriterMain` child-entry adapter installed into the
/// `background_writer_main` seam. The seam is `-> !` (a `ChildMainFn` slot in
/// the postmaster child-launch table): run the main loop and, should it ever
/// return, `proc_exit` like the C `void` entry point would after `proc_exit(0)`.
fn background_writer_main_entry(startup_data: &StartupData) -> ! {
    match BackgroundWriterMain(startup_data) {
        Ok(()) => ipc::proc_exit::call(0, initsmall::my_proc_pid::call()),
        Err(err) => {
            backend_utils_error::emit_error_report_for(&err);
            ipc::proc_exit::call(1, initsmall::my_proc_pid::call());
        }
    }
}

/// Install this unit's inward seams. Must be called from the global seam
/// bootstrap before any postmaster child launch reaches the bgwriter.
pub fn init_seams() {
    backend_postmaster_bgwriter_seams::background_writer_main::set(background_writer_main_entry);

    // `int BgWriterDelay = 200;` (bgwriter.c:58) is a plain GUC int the engine
    // keeps updated through `conf->variable` (guc_tables.c:3217, `&BgWriterDelay`).
    // It is read straight from the GUC slot — never the ControlFile. Bind the
    // GUC engine's `vars::BgWriterDelay` accessors to this unit's backing so
    // SET / SIGHUP reload reaches it, and install the buffer-manager consumer
    // seam (`BgBufferSync` scan-pace math, buf_flush.rs) that reads the same.
    {
        use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
        vars::BgWriterDelay.install(GucVarAccessors {
            get: BgWriterDelay,
            set: set_BgWriterDelay,
        });
    }
    bufmgr::bgwriter_delay::set(BgWriterDelay);
}
