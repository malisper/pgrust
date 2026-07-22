//! IoWorkerMain (method_worker.c): aux-thread scaffolding only; the ring and
//! loop body live in aio_core (auxprocess sits above aio_core in the DAG).

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
        // C: SIGINT = die; SIGUSR2 = late explicit shutdown (checkpointer-like).
        procsignal::pqsignal_thread(
            procsignal::signums::SIGINT,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
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
            // C's sigsetjmp arm: reopen/IO failures already completed the IO
            // inside the cycle; exit(1) relaunches a fresh worker.
            fatal_exit(&e);
        }
    }

    ipc::proc_exit(0, g::MyProcPid())
}
