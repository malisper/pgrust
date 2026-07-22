//! IoWorkerMain (storage/aio/method_worker.c): the aux-thread scaffolding for
//! an IO worker. The worker registry, submission ring, and the execute loop
//! body live in aio_core::method_worker; this crate exists because the
//! aux-process bring-up (auxprocess -> postinit -> bufmgr) sits ABOVE aio_core
//! in the crate DAG — the same layering as checkpointer/walwriter.

#![allow(non_snake_case)]

use init_small::globals as g;
use types_error::PgError;
use types_startup::StartupData;

fn fatal_exit(e: &PgError) -> ! {
    elog::emit_error_report_for(e);
    ipc::proc_exit(1, g::MyProcPid())
}

pub fn IoWorkerMain(startup_data: &StartupData) -> ! {
    debug_assert!(matches!(startup_data, StartupData::None));

    miscinit::SetMyBackendType(types_core::BackendType::IoWorker);
    if let Err(e) = auxprocess::AuxiliaryProcessMainCommon() {
        fatal_exit(&e);
    }

    {
        use procsignal::ThreadSignalHandler::{Ignore, Simple};
        procsignal::pqsignal_thread(
            procsignal::signums::SIGHUP,
            Simple(interrupt::SignalHandlerForConfigReload),
        );
        // C: SIGINT = die (manual worker restart). The thread rendering exits
        // at the next drain point instead of longjmping mid-IO.
        procsignal::pqsignal_thread(
            procsignal::signums::SIGINT,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        // Explicit shutdown comes via SIGUSR2 late in the sequence, like
        // checkpointer; SIGTERM is ignored.
        procsignal::pqsignal_thread(procsignal::signums::SIGTERM, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGALRM, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(
            procsignal::signums::SIGUSR2,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
    }

    libpq_pqsignal::unblock_signals();

    if let Err(e) = aio_core::pgaio_worker_register() {
        fatal_exit(&e);
    }

    while !interrupt::ShutdownRequestPending() {
        if let Err(e) = aio_core::pgaio_worker_cycle() {
            // Reopen/execution failures are already downgraded to a failed IO
            // inside the cycle; anything surfacing here is unexpected —
            // exit(1), the postmaster relaunches a fresh worker (C's
            // sigsetjmp + proc_exit(1) arm).
            fatal_exit(&e);
        }
    }

    // On exit, the executed-IO count goes to the server log at DEBUG1: the
    // deterministic flowed-through-workers witness for the read-path e2e.
    let _ = elog::elog(
        types_error::DEBUG1,
        format!("io worker executed {} IOs", aio_core::pgaio_worker_executed_count()),
    );

    ipc::proc_exit(0, g::MyProcPid())
}
