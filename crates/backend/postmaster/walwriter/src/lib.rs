//! walwriter.c; signal dispositions are process-wide (thread model).
//!
//! M4 bgjobs (docs/design/m4-bgjobs.md §4 row 2): the daemon's control
//! state is an explicit [`WalWriterState`] and the loop body is the
//! reusable [`walwriter_cycle`] — ONE BODY, TWO DRIVERS. The thread driver
//! here ([`WalWriterMain`] → `walwriter_loop`) is behaviorally identical
//! to the C loop; the job driver ([`job`]) runs the same cycle on pool
//! workers under Maintenance RGs. GUC cells stay thread-local (the job
//! mode stamps its overlay at cycle entry).
//!
//! C ordering that must survive both drivers (walwriter.c:226-241): the
//! hibernation flag (XLogCtl WalWriterSleeping — what tells async commits
//! to SetLatch us) is updated at cycle START, BEFORE ResetLatch, from the
//! PREVIOUS cycle's counter, so a wake sent between the flag update and
//! the reset is never lost. [`walwriter_cycle`] preserves that order
//! verbatim; the tail wait is the DRIVER's (WaitLatch here, re-arm
//! deadline in job mode).

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use std::cell::Cell;
use std::sync::atomic::Ordering::Relaxed;

use init_small::globals as g;
use types_error::{PgError, PgResult};
use types_startup::StartupData;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

#[cfg(test)]
mod tests;

pub mod job;

pub const LOOPS_UNTIL_HIBERNATE: i32 = 50;
pub const HIBERNATE_FACTOR: i64 = 25;

// wait_event_names.txt Activity section ordering.
const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const WAIT_EVENT_WAL_WRITER_MAIN: u32 = PG_WAIT_ACTIVITY + 17;

pub const DEFAULT_WAL_WRITER_FLUSH_AFTER: i32 = 128;

thread_local! {
    // GUC cells (accessor-installed): deliberately stay thread-local — the
    // job mode stamps its overlay into the executing thread's cells at
    // cycle entry (docs/design/m4-bgjobs.md §3.1).
    static WAL_WRITER_DELAY: Cell<i32> = const { Cell::new(200) };
    static WAL_WRITER_FLUSH_AFTER: Cell<i32> = const { Cell::new(DEFAULT_WAL_WRITER_FLUSH_AFTER) };
}

pub fn WalWriterDelay() -> i32 {
    WAL_WRITER_DELAY.get()
}

/// The daemon's per-instance control state (C locals + the xlog.c
/// function-static whose continuity the behavior depends on): the
/// hibernation counter/flag pair and the XLogBackgroundFlush pacing clock.
/// The thread main owns one on its frame; the job envelope owns one so
/// cycles may run on any pool worker.
pub struct WalWriterState {
    pub left_till_hibernate: i32,
    pub hibernating: bool,
    pub pacing: transam_xlog::WalFlushPacing,
}

impl WalWriterState {
    pub fn new() -> WalWriterState {
        WalWriterState {
            left_till_hibernate: LOOPS_UNTIL_HIBERNATE,
            hibernating: false,
            pacing: transam_xlog::WalFlushPacing::new(),
        }
    }
}

impl Default for WalWriterState {
    fn default() -> Self {
        WalWriterState::new()
    }
}

fn fatal_exit(e: &PgError) -> ! {
    elog::emit_error_report_for(e);
    ipc::proc_exit(1, g::MyProcPid())
}

pub fn WalWriterMain(startup_data: &StartupData) -> ! {
    debug_assert!(matches!(startup_data, StartupData::None));

    miscinit::SetMyBackendType(types_core::BackendType::WalWriter);
    if let Err(e) = auxprocess::AuxiliaryProcessMainCommon() {
        fatal_exit(&e);
    }

    {
        use procsignal::ThreadSignalHandler::{Ignore, Simple};
        procsignal::pqsignal_thread(procsignal::signums::SIGHUP, Simple(interrupt::SignalHandlerForConfigReload));
        procsignal::pqsignal_thread(
            procsignal::signums::SIGINT,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        procsignal::pqsignal_thread(
            procsignal::signums::SIGTERM,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        procsignal::pqsignal_thread(procsignal::signums::SIGALRM, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGUSR2, Ignore);
    }

    libpq_pqsignal::unblock_signals();

    let mut state = WalWriterState::new();
    transam_xlog::SetWalWriterSleeping(false);

    lmgr_proc::ProcGlobal().walwriterProc.store(g::MyProcNumber(), Relaxed);

    // sigsetjmp(PG_exception_stack) equivalent.
    loop {
        match walwriter_loop(&mut state) {
            Ok(never) => match never {},
            Err(err) => {
                walwriter_error_recovery(&mut state, &err);
            }
        }
    }
}

enum Never {}

/// The daemon's uniform error leg (shared by both drivers): minimal abort
/// cleanup, 1s backoff, hibernation forgotten (C walwriter.c:206-210). The
/// thread driver sleeps inline; the job driver maps the backoff to its
/// re-arm deadline and skips the inline sleep.
pub fn walwriter_error_recovery(state: &mut WalWriterState, err: &PgError) {
    abort_cleanup(err);
    std::thread::sleep(std::time::Duration::from_secs(1));
    state.left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
    state.hibernating = false;
    transam_xlog::SetWalWriterSleeping(false);
}

pub(crate) fn abort_cleanup(err: &PgError) {
    g::SetInterruptHoldoffCount(0);
    g::SetCritSectionCount(0);
    g::HoldInterrupts();

    elog::emit_error_report_for(err);

    let _ = lwlock::LWLockReleaseAll();
    if condition_variable_seams::condition_variable_cancel_sleep::is_installed() {
        condition_variable_seams::condition_variable_cancel_sleep::call();
    }
    waitevent_seams::pgstat_report_wait_end::call();
    if aio_seams::pgaio_error_cleanup::is_installed() {
        aio_seams::pgaio_error_cleanup::call();
    }
    bufmgr::UnlockBuffers();
    let _ = resowner::ReleaseAuxProcessResources(false);
    bufmgr::AtEOXact_Buffers(false);
    let _ = smgr::AtEOXact_SMgr();
    let _ = fd::AtEOXact_Files(false);
    dynahash::AtEOXact_HashTables(false);

    elog::FlushErrorState();
    g::ResumeInterrupts();
}

/// Pure hibernation-FSM pieces (the C loop's flag/counter/timeout lines),
/// extracted for the deterministic unit tests (the walwriter analog of
/// bufmgr::bgw_plan_scan — coordinator decision #2 on the bgjobs design).
///
/// Cycle-top flag step (walwriter.c:234-238): returns the new flag and
/// whether SetWalWriterSleeping must be called (only on change).
pub fn hibernate_flag_step(left_till_hibernate: i32, hibernating: bool) -> (bool, bool) {
    let new = left_till_hibernate <= 1;
    (new, new != hibernating)
}

/// Post-body counter step (walwriter.c:250-253).
pub fn hibernate_count_step(left_till_hibernate: i32, did_work: bool) -> i32 {
    if did_work {
        LOOPS_UNTIL_HIBERNATE
    } else if left_till_hibernate > 0 {
        left_till_hibernate - 1
    } else {
        left_till_hibernate
    }
}

/// Tail timeout (walwriter.c:263-266), ms.
pub fn cycle_timeout_ms(left_till_hibernate: i32, delay_ms: i32) -> i64 {
    if left_till_hibernate > 0 {
        delay_ms as i64
    } else {
        delay_ms as i64 * HIBERNATE_FACTOR
    }
}

/// ONE loop-body iteration (both drivers): hibernation-flag publication →
/// ResetLatch → main-loop interrupts → XLogBackgroundFlush → hibernation
/// counter → pgstat report. Returns the timeout (ms) the C loop tail would
/// pass to WaitLatch; the tail wait is the DRIVER's.
pub fn walwriter_cycle(state: &mut WalWriterState) -> PgResult<i64> {
    let (flag, changed) = hibernate_flag_step(state.left_till_hibernate, state.hibernating);
    if changed {
        state.hibernating = flag;
        transam_xlog::SetWalWriterSleeping(flag);
    }

    if let Some(l) = g::MyLatch() {
        latch::ResetLatch(l);
    }

    interrupt::ProcessMainLoopInterrupts()?;

    let did_work = transam_xlog::XLogBackgroundFlush(&mut state.pacing)?;
    state.left_till_hibernate = hibernate_count_step(state.left_till_hibernate, did_work);

    if pgstat_seams::pgstat_report_wal::is_installed() {
        pgstat_seams::pgstat_report_wal::call(false);
    }

    Ok(cycle_timeout_ms(state.left_till_hibernate, WalWriterDelay()))
}

fn walwriter_loop(state: &mut WalWriterState) -> PgResult<Never> {
    loop {
        let cur_timeout = walwriter_cycle(state)?;

        latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            cur_timeout,
            WAIT_EVENT_WAL_WRITER_MAIN,
        )?;
    }
}

pub fn init_seams() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::WalWriterDelay.install(GucVarAccessors {
        get: WalWriterDelay,
        set: |v| WAL_WRITER_DELAY.set(v),
    });
    guc_tables::vars::WalWriterFlushAfter.install(GucVarAccessors {
        get: || WAL_WRITER_FLUSH_AFTER.get(),
        set: |v| WAL_WRITER_FLUSH_AFTER.set(v),
    });
}
