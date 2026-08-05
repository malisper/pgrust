//! bgwriter.c; signal dispositions are process-wide (thread model), and the
//! WritebackContext lives in bufmgr::bgwriter_sync.
//!
//! M4 bgjobs increment 3 (docs/design/m4-bgjobs.md §3.1/§5): the daemon's
//! control state is an explicit [`BgWriterState`] and the loop body is the
//! reusable [`bgwriter_cycle`] — ONE BODY, TWO DRIVERS. The thread driver
//! here ([`BackgroundWriterMain`] → `bgwriter_loop`) is behaviorally
//! identical to the C loop; the job driver (increment 4) runs the same
//! cycle on pool workers under the job envelope bind. GUC cells stay
//! thread-local (the job mode stamps its overlay at cycle entry).

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use std::cell::Cell;

use init_small::globals as g;
use types_core::XLogRecPtr;
use types_error::{PgError, PgResult};
use types_startup::StartupData;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

#[cfg(test)]
mod tests;

pub mod job;

const HIBERNATE_FACTOR: i64 = 50;
const LOG_SNAPSHOT_INTERVAL_MS: i64 = 15000;

// wait_event_names.txt Activity section ordering.
const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const WAIT_EVENT_BGWRITER_HIBERNATE: u32 = PG_WAIT_ACTIVITY + 2;
const WAIT_EVENT_BGWRITER_MAIN: u32 = PG_WAIT_ACTIVITY + 3;

thread_local! {
    // GUC cell (accessor-installed): deliberately stays thread-local — the
    // job mode stamps its GUC overlay into the executing thread's cells at
    // cycle entry (docs/design/m4-bgjobs.md §3.1).
    static BG_WRITER_DELAY: Cell<i32> = const { Cell::new(200) };
}

pub fn BgWriterDelay() -> i32 {
    BG_WRITER_DELAY.get()
}

/// The daemon's per-instance control state (C statics + frame slots whose
/// continuity the behavior depends on): the BgBufferSync clock-sweep/EWMA
/// block and the standby-snapshot trackers. The thread main owns one on
/// its frame; the job envelope (increment 4) owns one so cycles may run on
/// any pool worker.
pub struct BgWriterState {
    pub sync: bufmgr::BgwSyncState,
    last_snapshot_ts: i64,
    last_snapshot_lsn: XLogRecPtr,
    pub prev_hibernate: bool,
}

impl Default for BgWriterState {
    fn default() -> Self {
        Self::new()
    }
}

impl BgWriterState {
    pub fn new() -> BgWriterState {
        BgWriterState {
            sync: bufmgr::BgwSyncState::new(),
            last_snapshot_ts: 0,
            last_snapshot_lsn: 0,
            prev_hibernate: false,
        }
    }
}

fn fatal_exit(e: &PgError) -> ! {
    elog::emit_error_report_for(e);
    ipc::proc_exit(1, g::MyProcPid())
}

pub fn BackgroundWriterMain(startup_data: &StartupData) -> ! {
    debug_assert!(matches!(startup_data, StartupData::None));

    miscinit::SetMyBackendType(types_core::BackendType::BgWriter);
    if let Err(e) = auxprocess::AuxiliaryProcessMainCommon() {
        fatal_exit(&e);
    }

    {
        use procsignal::ThreadSignalHandler::{Ignore, Simple};
        procsignal::pqsignal_thread(procsignal::signums::SIGHUP, Simple(interrupt::SignalHandlerForConfigReload));
        procsignal::pqsignal_thread(procsignal::signums::SIGINT, Ignore);
        procsignal::pqsignal_thread(
            procsignal::signums::SIGTERM,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        procsignal::pqsignal_thread(procsignal::signums::SIGALRM, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGUSR2, Ignore);
    }

    let mut state = BgWriterState::new();
    state.last_snapshot_ts = timestamp_seams::get_current_timestamp::call();

    state.sync.reset_writeback_context();

    libpq_pqsignal::unblock_signals();

    // sigsetjmp(PG_exception_stack) equivalent.
    loop {
        match bgwriter_loop(&mut state) {
            Ok(never) => match never {},
            Err(err) => {
                bgwriter_error_recovery(&mut state, &err);
            }
        }
    }
}

enum Never {}

/// The daemon's uniform error leg (shared by both drivers): minimal abort
/// cleanup, writeback-context re-init, 1s backoff, hibernation forgotten.
/// The thread driver sleeps inline; the job driver maps the backoff to its
/// re-arm deadline (increment 4).
pub fn bgwriter_error_recovery(state: &mut BgWriterState, err: &PgError) {
    abort_cleanup(err);
    state.sync.reset_writeback_context();
    std::thread::sleep(std::time::Duration::from_secs(1));
    waitevent_seams::pgstat_report_wait_end::call();
    state.prev_hibernate = false;
}

fn abort_cleanup(err: &PgError) {
    g::SetInterruptHoldoffCount(0);
    g::SetCritSectionCount(0);
    g::HoldInterrupts();

    elog::emit_error_report_for(err);

    let _ = lwlock::LWLockReleaseAll();
    if condition_variable_seams::condition_variable_cancel_sleep::is_installed() {
        condition_variable_seams::condition_variable_cancel_sleep::call();
    }
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

/// ONE loop-body iteration (both drivers): ResetLatch → main-loop
/// interrupts → BgBufferSync → pgstat flushes → post-checkpoint smgr sweep
/// → standby-snapshot logging. Returns `can_hibernate` — what the C loop
/// feeds the hibernation leg. The tail wait is the DRIVER's: the thread
/// driver WaitLatches below; the job driver turns it into a re-arm
/// deadline.
pub fn bgwriter_cycle(state: &mut BgWriterState) -> PgResult<bool> {
    if let Some(l) = g::MyLatch() {
        latch::ResetLatch(l);
    }

    interrupt::ProcessMainLoopInterrupts()?;

    let can_hibernate = bufmgr::BgBufferSync(&mut state.sync)?;

    if pgstat_seams::pgstat_report_bgwriter::is_installed() {
        pgstat_seams::pgstat_report_bgwriter::call();
    }
    if pgstat_seams::pgstat_report_wal::is_installed() {
        pgstat_seams::pgstat_report_wal::call(true);
    }

    if checkpointer::FirstCallSinceLastCheckpoint() {
        smgr::smgrdestroyall()?;
    }

    if transam_xlog::XLogStandbyInfoActive() && !transam_xlog::RecoveryInProgress() {
        let now = timestamp_seams::get_current_timestamp::call();
        let timeout = state.last_snapshot_ts + LOG_SNAPSHOT_INTERVAL_MS * 1000;

        if now >= timeout && state.last_snapshot_lsn <= transam_xlog::GetLastImportantRecPtr() {
            state.last_snapshot_lsn = standby_seams::log_standby_snapshot::call()?;
            state.last_snapshot_ts = now;
        }
    }

    Ok(can_hibernate)
}

fn bgwriter_loop(state: &mut BgWriterState) -> PgResult<Never> {
    loop {
        let can_hibernate = bgwriter_cycle(state)?;

        let rc = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            BgWriterDelay() as i64,
            WAIT_EVENT_BGWRITER_MAIN,
        )?;

        if rc == WL_TIMEOUT && can_hibernate && state.prev_hibernate {
            bufmgr::StrategyNotifyBgWriter(g::MyProcNumber());
            let _ = latch::WaitLatch(
                g::MyLatch(),
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                BgWriterDelay() as i64 * HIBERNATE_FACTOR,
                WAIT_EVENT_BGWRITER_HIBERNATE,
            )?;
            bufmgr::StrategyNotifyBgWriter(-1);
        }

        state.prev_hibernate = can_hibernate;
    }
}

pub fn init_seams() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::BgWriterDelay.install(GucVarAccessors {
        get: BgWriterDelay,
        set: |v| BG_WRITER_DELAY.set(v),
    });
}
