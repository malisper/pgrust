//! Port of `src/backend/postmaster/walwriter.c` (PostgreSQL 18.3): the WAL
//! writer auxiliary process.
//!
//! The walwriter keeps regular backends from having to write out (and fsync)
//! WAL pages, and guarantees that asynchronously-committed transactions reach
//! disk within a bounded time (at most three `wal_writer_delay` cycles). It is
//! started by the postmaster as soon as the startup subprocess finishes and
//! runs until told to terminate. Because it is non-essential (backends can
//! still issue their own WAL writes), it shuts down quickly when requested.
//!
//! # Process structure
//!
//! This crate owns the *process structure* faithfully:
//!   * the `pqsignal()` block, performed by the host auxiliary-process bootstrap
//!     (it routes `SignalHandlerForConfigReload` / `SignalHandlerForShutdownRequest`
//!     / `procsignal_sigusr1_handler` and the `SIG_IGN`/`SIG_DFL` dispositions),
//!   * the `sigsetjmp(local_sigjmp_buf, 1)` error-recovery loop, modeled as an
//!     outer loop whose body returns [`PgResult`]: a returned `Err`
//!     (PostgreSQL's `ereport(ERROR)` longjmp) runs the minimal abort cleanup,
//!     replays the post-recovery resets, and re-enters the loop body, exactly as
//!     the C re-enters `for(;;)` after the landing pad,
//!   * the `left_till_hibernate` cycle counting, the hibernation-flag
//!     advertisement (`SetWalWriterSleeping`), and the `WalWriterDelay` /
//!     `HIBERNATE_FACTOR` timeout arithmetic,
//!   * the `ResetLatch(MyLatch)` / `WaitLatch(MyLatch, ...)` cycle.
//!
//! # Boundaries
//!
//! `XLogBackgroundFlush` is reached through the ported
//! [`transam_xlog`] crate (it returns `PgResult<bool>` here; an
//! `Err` is the C `ereport(ERROR)` longjmp, propagated to the recovery loop).
//! `SetWalWriterSleeping` is in the same crate (its real impl needs the XLogCtl
//! shmem substrate). `ProcessMainLoopInterrupts` is the ported interrupt crate
//! (its `ShutdownRequestPending` branch calls `proc_exit(0)`, which never
//! returns). Everything else (aux-process setup, latch, the minimal abort
//! cleanup, `pgstat_report_wal`, `ProcGlobal->walwriterProc`, `pg_usleep`) is
//! reached through the owning crates' seams.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::Cell;

use types_error::PgResult;
use types_pgstat::wait_event::WAIT_EVENT_WAL_WRITER_MAIN;
use types_startup::StartupData;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};
use wal::xlog_consts::XLOG_BLCKSZ;

use transam_xlog::{SetWalWriterSleeping, XLogBackgroundFlush};
use interrupt as interrupt;

use aio_seams as aio;
use bufmgr_seams as bufmgr;
use fd_seams as fd;
use dsm_core_seams as ipc;
use latch_seams as latch;
use condition_variable_seams as cv;
use lwlock_seams as lwlock;
use lmgr_proc_seams as proc;
use smgr_seams as smgr;
use auxprocess_seams as auxprocess;
use pgstat_wal_seams as walstats;
use waitevent_seams as waitevent;
use dynahash_seams as dynahash;
use miscinit_seams as miscinit;
use init_small_seams as initsmall;
use resowner_seams_2 as resowner;

#[cfg(test)]
mod tests;

// ===========================================================================
// Constants (walwriter.c).
// ===========================================================================

/// `DEFAULT_WAL_WRITER_FLUSH_AFTER == (1024 * 1024) / XLOG_BLCKSZ`
/// (`include/access/xlog.h`); 1 MB / 8192 == 128.
pub const DEFAULT_WAL_WRITER_FLUSH_AFTER: i32 = (1024 * 1024) / XLOG_BLCKSZ as i32;

/// `LOOPS_UNTIL_HIBERNATE` (walwriter.c:72) — number of do-nothing loops before
/// lengthening the delay time.
const LOOPS_UNTIL_HIBERNATE: i32 = 50;

/// `HIBERNATE_FACTOR` (walwriter.c:73) — multiplier to apply to `WalWriterDelay`
/// when we decide to hibernate.
const HIBERNATE_FACTOR: i32 = 25;

// ===========================================================================
// GUC parameters (walwriter.c:65-66).
//
// In C these are plain `int` globals written by the GUC machinery; the walwriter
// reads them locally. Modeled as process-wide cells (single-threaded backend).
// ===========================================================================

thread_local! {
    /// `int WalWriterDelay = 200;`
    static WAL_WRITER_DELAY: Cell<i32> = const { Cell::new(200) };
    /// `int WalWriterFlushAfter = DEFAULT_WAL_WRITER_FLUSH_AFTER;`
    static WAL_WRITER_FLUSH_AFTER: Cell<i32> =
        const { Cell::new(DEFAULT_WAL_WRITER_FLUSH_AFTER) };
}

/// Read `WalWriterDelay` (milliseconds).
pub fn WalWriterDelay() -> i32 {
    WAL_WRITER_DELAY.with(Cell::get)
}

/// Assign `WalWriterDelay`.
pub fn set_WalWriterDelay(value: i32) {
    WAL_WRITER_DELAY.with(|c| c.set(value));
}

/// Read `WalWriterFlushAfter`.
pub fn WalWriterFlushAfter() -> i32 {
    WAL_WRITER_FLUSH_AFTER.with(Cell::get)
}

/// Assign `WalWriterFlushAfter`.
pub fn set_WalWriterFlushAfter(value: i32) {
    WAL_WRITER_FLUSH_AFTER.with(|c| c.set(value));
}

// ===========================================================================
// Per-cycle loop state (the locals of WalWriterMain's for(;;) loop).
// ===========================================================================

/// State carried across loop iterations and across error recovery
/// (`left_till_hibernate` / `hibernating` in `WalWriterMain`).
struct LoopState {
    left_till_hibernate: i32,
    hibernating: bool,
}

impl LoopState {
    /// `if (hibernating != (left_till_hibernate <= 1))` (walwriter.c:206-210) —
    /// recompute the "might hibernate this cycle" flag, returning
    /// `Some(new_value)` exactly when it changed (the caller must then call
    /// `SetWalWriterSleeping`), or `None` when unchanged. Pure.
    fn recompute_hibernation(&mut self) -> Option<bool> {
        let want = self.left_till_hibernate <= 1;
        if self.hibernating != want {
            self.hibernating = want;
            Some(want)
        } else {
            None
        }
    }

    /// `if (XLogBackgroundFlush()) left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
    /// else if (left_till_hibernate > 0) left_till_hibernate--;`
    /// (walwriter.c:221-225) — fold the flush result into the hibernation
    /// counter. Pure.
    fn apply_flush_result(&mut self, found_work: bool) {
        if found_work {
            self.left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
        } else if self.left_till_hibernate > 0 {
            self.left_till_hibernate -= 1;
        }
    }

    /// The latch timeout for this cycle (walwriter.c:235-238): `WalWriterDelay`
    /// (ms) while still active, else `WalWriterDelay * HIBERNATE_FACTOR` once
    /// hibernating. Pure.
    fn cur_timeout(&self) -> i64 {
        if self.left_till_hibernate > 0 {
            WalWriterDelay() as i64 // in ms
        } else {
            WalWriterDelay() as i64 * HIBERNATE_FACTOR as i64
        }
    }
}

// ===========================================================================
// WalWriterMain (walwriter.c:80-243).
// ===========================================================================

/// `WalWriterMain(startup_data, startup_data_len)` (walwriter.c:80-243).
///
/// Invoked from `AuxiliaryProcessMain`, which has already created the basic
/// execution environment but not enabled signals yet. Sets up the process, then
/// loops forever; the `Ok(())` return path exists so the structure is testable
/// (in a live build, the shutdown path inside [`ProcessMainLoopInterrupts`]
/// calls `proc_exit(0)`, which never returns).
pub fn WalWriterMain(startup_data: &StartupData) -> PgResult<()> {
    debug_assert!(matches!(startup_data, StartupData::None));

    // MyBackendType = B_WAL_WRITER; AuxiliaryProcessMainCommon().
    miscinit::set_my_backend_type_wal_writer::call();
    auxprocess::auxiliary_process_main_common::call()?;

    // Properly accept or ignore signals that might be sent to us
    // (walwriter.c:105-118). This was previously assumed to be done by the
    // "host auxiliary-process bootstrap" — but nothing installs it, so the
    // postmaster's inherited SIGUSR1/SIGTERM dispositions stayed in force and
    // this process never ran `procsignal_sigusr1_handler` (absorbing
    // ProcSignalBarriers) nor `SignalHandlerForShutdownRequest`. On cluster/DB
    // teardown the walwriter therefore never ran `proc_exit(0)` → its
    // `on_shmem_exit` chain → `CleanupProcSignalState` never fired → its
    // procsignal slot kept `pss_pid != 0` at a stale finite
    // `pss_barrierGeneration`, hanging the emitter of a `DROP DATABASE`
    // (`WaitForProcSignalBarrier`) forever on this slot.
    {
        use signal::SigHandler;
        let pqsignal = port_pqsignal_seams::pqsignal::call;
        // pqsignal(SIGHUP, SignalHandlerForConfigReload);
        fn config_reload(_sig: i32) {
            interrupt::SignalHandlerForConfigReload();
        }
        pqsignal(libc::SIGHUP, SigHandler::Handler(config_reload));
        // pqsignal(SIGINT, SignalHandlerForShutdownRequest);
        // pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
        fn shutdown_request(_sig: i32) {
            interrupt::SignalHandlerForShutdownRequest();
        }
        pqsignal(libc::SIGINT, SigHandler::Handler(shutdown_request));
        pqsignal(libc::SIGTERM, SigHandler::Handler(shutdown_request));
        // SIGQUIT handler was already set up by InitPostmasterChild.
        // pqsignal(SIGALRM, SIG_IGN);
        pqsignal(libc::SIGALRM, SigHandler::Ignore);
        // pqsignal(SIGPIPE, SIG_IGN);
        pqsignal(libc::SIGPIPE, SigHandler::Ignore);
        // pqsignal(SIGUSR1, procsignal_sigusr1_handler);
        pqsignal(
            libc::SIGUSR1,
            SigHandler::Handler(
                procsignal::procsignal_sigusr1_handler_signal,
            ),
        );
        // pqsignal(SIGUSR2, SIG_IGN); /* not used */
        pqsignal(libc::SIGUSR2, SigHandler::Ignore);
        // Reset some signals that are accepted by postmaster but not here:
        // pqsignal(SIGCHLD, SIG_DFL);
        pqsignal(libc::SIGCHLD, SigHandler::Default);
    }

    // Unblock signals (they were blocked when the postmaster forked us)
    // (walwriter.c:203, sigprocmask(SIG_SETMASK, &UnBlockSig, NULL)). Without
    // this the SIGUSR1 that `EmitProcSignalBarrier` sends us stays pending and
    // is never delivered to the handler installed above.
    libpq_pqsignal_seams::unblock_signals::call();

    // Create a memory context that we will do all our work in (so we can reset
    // it during error recovery and avoid memory leaks). The "Wal Writer"
    // AllocSetContextCreate + MemoryContextSwitchTo are host-owned per-process
    // context lifecycle.

    // Reset hibernation state (also re-done after any error, below).
    let mut state = LoopState {
        left_till_hibernate: LOOPS_UNTIL_HIBERNATE,
        hibernating: false,
    };
    // SetWalWriterSleeping(false).
    SetWalWriterSleeping(false);

    // Advertise our proc number so backends can wake us up while we're sleeping
    // (ProcGlobal->walwriterProc = MyProcNumber).
    proc::set_walwriter_proc_to_self::call()?;

    // The sigsetjmp(local_sigjmp_buf, 1) landing pad. The C landing is OUTSIDE
    // the for(;;) (walwriter.c:188), but the post-recovery resets (L196-201)
    // re-run on every error recovery, so the outer loop runs the abort cleanup
    // on error, replays the resets, and re-enters the body.
    loop {
        match main_loop_cycle(&mut state) {
            Ok(()) => {}
            Err(err) => {
                walwriter_abort_cleanup(&err)?;
                // Reset hibernation state after any error.
                state.left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
                state.hibernating = false;
                SetWalWriterSleeping(false);
                proc::set_walwriter_proc_to_self::call()?;
            }
        }
    }
}

/// One iteration of the walwriter main loop (the body of `for (;;)`,
/// walwriter.c:203-242). A returned `Err` corresponds to an `ereport(ERROR)`
/// longjmp back to the sigsetjmp landing pad.
fn main_loop_cycle(state: &mut LoopState) -> PgResult<()> {
    // Advertise whether we might hibernate in this cycle. We do this before
    // resetting the latch to ensure that any async commits will see the flag set
    // if they might possibly need to wake us up, and that we won't miss any
    // signal they send us. But avoid touching the global flag if it doesn't need
    // to change.
    if let Some(hibernating) = state.recompute_hibernation() {
        SetWalWriterSleeping(hibernating);
    }

    // Clear any already-pending wakeups.
    latch::reset_latch_my_latch::call();

    // Process any signals received recently. (ShutdownRequestPending here calls
    // proc_exit(0) inside the ported interrupt crate, which never returns.)
    interrupt::ProcessMainLoopInterrupts()?;

    // Do what we're here for; then, if XLogBackgroundFlush() found useful work to
    // do, reset the hibernation counter. (XLogBackgroundFlush returns
    // PgResult<bool>; an Err is the C ereport(ERROR) longjmp, propagated to the
    // recovery loop.)
    let found_work = XLogBackgroundFlush()?;
    state.apply_flush_result(found_work);

    // Report pending statistics to the cumulative stats system.
    walstats::pgstat_report_wal::call(false);

    // Sleep until we are signaled or WalWriterDelay has elapsed. If we haven't
    // done anything useful for quite some time, lengthen the sleep time so as to
    // reduce the server's idle power consumption.
    let cur_timeout = state.cur_timeout();

    latch::wait_latch_my_latch::call(
        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
        cur_timeout,
        WAIT_EVENT_WAL_WRITER_MAIN,
    )?;

    Ok(())
}

/// The minimal-abort cleanup from the sigsetjmp block (walwriter.c:147-187),
/// minus the host-owned framing (error_context_stack / MemoryContext reset).
///
/// "These operations are really just a minimal subset of AbortTransaction(). We
/// don't have very many resources to worry about in walwriter, but we do have
/// LWLocks, and perhaps buffers?"
fn walwriter_abort_cleanup(err: &types_error::PgError) -> PgResult<()> {
    // Since not using PG_TRY, must reset error stack by hand (error_context_stack
    // = NULL is host-owned, folded into FlushErrorState below).

    // Prevent interrupts while cleaning up.
    miscinit::hold_interrupts::call();

    // Report the error to the server log.
    utils_error::emit_error_report_for(err);

    // The minimal subset of AbortTransaction().
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

    // Now return to normal top-level context and clear ErrorContext for next
    // time (MemoryContextSwitchTo(walwriter_context) + FlushErrorState +
    // MemoryContextReset(walwriter_context) reduce to FlushErrorState here, as
    // the host owns the per-process context lifecycle).
    utils_error::FlushErrorState();

    // Now we can allow interrupts again.
    miscinit::resume_interrupts::call();

    // Sleep at least 1 second after any error. A write error is likely to be
    // repeated, and we don't want to be filling the error logs as fast as we can.
    ipc_pg_usleep(1_000_000)?;

    Ok(())
}

/// `pg_usleep(usec)` — sleep the given microseconds.
fn ipc_pg_usleep(usec: i64) -> PgResult<()> {
    pgsleep_seams::pg_usleep::call(usec);
    Ok(())
}

// ===========================================================================
// Inward seams (installed by init_seams).
// ===========================================================================

/// `wal_writer_main` adapter (`-> !`): run [`WalWriterMain`]; the walwriter loops
/// until proc_exit, so a returned `Ok` runs the host proc_exit(0) and a
/// top-level `Err` (a FATAL escaping with no handler) reports and exits(1).
fn wal_writer_main_entry(startup_data: &StartupData) -> ! {
    match WalWriterMain(startup_data) {
        Ok(()) => ipc::proc_exit::call(0, initsmall::my_proc_pid::call()),
        Err(err) => {
            utils_error::emit_error_report_for(&err);
            ipc::proc_exit::call(1, initsmall::my_proc_pid::call());
        }
    }
}

/// Install every seam this crate owns.
pub fn init_seams() {
    walwriter_seams::wal_writer_main::set(wal_writer_main_entry);

    // `int WalWriterDelay` / `int WalWriterFlushAfter` GUC backing storage
    // (walwriter.c:65-66). Both are plain int GUC globals: the GUC engine seeds
    // them from boot_val and writes them on SIGHUP reload, while WalWriterMain
    // reads them locally each cycle (timeout = WalWriterDelay; flush bound =
    // WalWriterFlushAfter). Neither comes from the ControlFile. Bridge the GUC
    // slots to this crate's process-wide cells.
    {
        use guc_tables::{vars, GucVarAccessors};
        vars::WalWriterDelay.install(GucVarAccessors {
            get: WalWriterDelay,
            set: set_WalWriterDelay,
        });
        vars::WalWriterFlushAfter.install(GucVarAccessors {
            get: WalWriterFlushAfter,
            set: set_WalWriterFlushAfter,
        });
    }
}
