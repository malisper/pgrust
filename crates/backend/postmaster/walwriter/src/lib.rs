//! walwriter.c; signal dispositions are process-wide (thread model).

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

const LOOPS_UNTIL_HIBERNATE: i32 = 50;
const HIBERNATE_FACTOR: i64 = 25;

// wait_event_names.txt Activity section ordering.
const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const WAIT_EVENT_WAL_WRITER_MAIN: u32 = PG_WAIT_ACTIVITY + 17;

const DEFAULT_WAL_WRITER_FLUSH_AFTER: i32 = 128;

thread_local! {
    static WAL_WRITER_DELAY: Cell<i32> = const { Cell::new(200) };
    static WAL_WRITER_FLUSH_AFTER: Cell<i32> = const { Cell::new(DEFAULT_WAL_WRITER_FLUSH_AFTER) };
}

pub fn WalWriterDelay() -> i32 {
    WAL_WRITER_DELAY.get()
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
        procsignal::pqsignal_thread(libc::SIGHUP, Simple(interrupt::SignalHandlerForConfigReload));
        procsignal::pqsignal_thread(
            libc::SIGINT,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        procsignal::pqsignal_thread(
            libc::SIGTERM,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        procsignal::pqsignal_thread(libc::SIGALRM, Ignore);
        procsignal::pqsignal_thread(libc::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(libc::SIGUSR2, Ignore);
    }

    libpq_pqsignal::unblock_signals();

    let mut left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
    let mut hibernating = false;
    transam_xlog::SetWalWriterSleeping(false);

    lmgr_proc::ProcGlobal().walwriterProc.store(g::MyProcNumber(), Relaxed);

    // sigsetjmp(PG_exception_stack) equivalent.
    loop {
        match walwriter_loop(&mut left_till_hibernate, &mut hibernating) {
            Ok(never) => match never {},
            Err(err) => {
                abort_cleanup(&err);
                std::thread::sleep(std::time::Duration::from_secs(1));
                left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
                hibernating = false;
                transam_xlog::SetWalWriterSleeping(false);
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

fn walwriter_loop(left_till_hibernate: &mut i32, hibernating: &mut bool) -> PgResult<Never> {
    loop {
        if *hibernating != (*left_till_hibernate <= 1) {
            *hibernating = *left_till_hibernate <= 1;
            transam_xlog::SetWalWriterSleeping(*hibernating);
        }

        if let Some(l) = g::MyLatch() {
            latch::ResetLatch(l);
        }

        interrupt::ProcessMainLoopInterrupts()?;

        if transam_xlog::XLogBackgroundFlush()? {
            *left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
        } else if *left_till_hibernate > 0 {
            *left_till_hibernate -= 1;
        }

        if pgstat_seams::pgstat_report_wal::is_installed() {
            pgstat_seams::pgstat_report_wal::call(false);
        }

        let cur_timeout = if *left_till_hibernate > 0 {
            WalWriterDelay() as i64
        } else {
            WalWriterDelay() as i64 * HIBERNATE_FACTOR
        };

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
