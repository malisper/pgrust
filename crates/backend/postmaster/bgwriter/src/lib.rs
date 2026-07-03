//! bgwriter.c; signal dispositions are process-wide (thread model), and the
//! WritebackContext lives in bufmgr::bgwriter_sync.

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

const HIBERNATE_FACTOR: i64 = 50;
const LOG_SNAPSHOT_INTERVAL_MS: i64 = 15000;

// wait_event_names.txt Activity section ordering.
const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const WAIT_EVENT_BGWRITER_HIBERNATE: u32 = PG_WAIT_ACTIVITY + 2;
const WAIT_EVENT_BGWRITER_MAIN: u32 = PG_WAIT_ACTIVITY + 3;

thread_local! {
    static BG_WRITER_DELAY: Cell<i32> = const { Cell::new(200) };
    static LAST_SNAPSHOT_TS: Cell<i64> = const { Cell::new(0) };
    static LAST_SNAPSHOT_LSN: Cell<XLogRecPtr> = const { Cell::new(0) };
}

pub fn BgWriterDelay() -> i32 {
    BG_WRITER_DELAY.get()
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
        procsignal::pqsignal_thread(libc::SIGHUP, Simple(interrupt::SignalHandlerForConfigReload));
        procsignal::pqsignal_thread(libc::SIGINT, Ignore);
        procsignal::pqsignal_thread(
            libc::SIGTERM,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        procsignal::pqsignal_thread(libc::SIGALRM, Ignore);
        procsignal::pqsignal_thread(libc::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(libc::SIGUSR2, Ignore);
    }

    LAST_SNAPSHOT_TS.set(timestamp_seams::get_current_timestamp::call());

    bufmgr::bgwriter_writeback_context_init();

    libpq_pqsignal::unblock_signals();

    let mut prev_hibernate = false;

    // sigsetjmp(PG_exception_stack) equivalent.
    loop {
        match bgwriter_loop(&mut prev_hibernate) {
            Ok(never) => match never {},
            Err(err) => {
                abort_cleanup(&err);
                bufmgr::bgwriter_writeback_context_init();
                std::thread::sleep(std::time::Duration::from_secs(1));
                waitevent_seams::pgstat_report_wait_end::call();
                prev_hibernate = false;
            }
        }
    }
}

enum Never {}

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

fn bgwriter_loop(prev_hibernate: &mut bool) -> PgResult<Never> {
    loop {
        if let Some(l) = g::MyLatch() {
            latch::ResetLatch(l);
        }

        interrupt::ProcessMainLoopInterrupts()?;

        let can_hibernate = bufmgr::BgBufferSync()?;

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
            let timeout = LAST_SNAPSHOT_TS.get() + LOG_SNAPSHOT_INTERVAL_MS * 1000;

            if now >= timeout && LAST_SNAPSHOT_LSN.get() <= transam_xlog::GetLastImportantRecPtr()
            {
                LAST_SNAPSHOT_LSN.set(standby_seams::log_standby_snapshot::call()?);
                LAST_SNAPSHOT_TS.set(now);
            }
        }

        let rc = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            BgWriterDelay() as i64,
            WAIT_EVENT_BGWRITER_MAIN,
        )?;

        if rc == WL_TIMEOUT && can_hibernate && *prev_hibernate {
            bufmgr::StrategyNotifyBgWriter(g::MyProcNumber());
            let _ = latch::WaitLatch(
                g::MyLatch(),
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                BgWriterDelay() as i64 * HIBERNATE_FACTOR,
                WAIT_EVENT_BGWRITER_HIBERNATE,
            )?;
            bufmgr::StrategyNotifyBgWriter(-1);
        }

        *prev_hibernate = can_hibernate;
    }
}

pub fn init_seams() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::BgWriterDelay.install(GucVarAccessors {
        get: BgWriterDelay,
        set: |v| BG_WRITER_DELAY.set(v),
    });
}
