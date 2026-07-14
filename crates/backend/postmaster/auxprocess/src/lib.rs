//! auxprocess.c (PostmasterContext release: thread-model no-op).

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use types_core::ProcessingMode;
use types_error::PgResult;

#[cfg(test)]
mod tests;

pub fn AuxiliaryProcessMainCommon() -> PgResult<()> {
    debug_assert!(init_small::globals::IsUnderPostmaster());

    ps_status::init_ps_display(None);

    debug_assert!(miscinit::IsInitProcessingMode());

    miscinit::SetIgnoreSystemIndexes(true);

    lmgr_proc::InitAuxiliaryProcess()?;

    postinit::BaseInit()?;

    procsignal::ProcSignalInit(&[])?;

    resowner::CreateAuxProcessResourceOwner()?;

    backend_status_seams::pgstat_beinit::call()?;
    backend_status_seams::pgstat_bestart_initial::call()?;
    backend_status_seams::pgstat_bestart_final::call()?;

    ipc::before_shmem_exit(ShutdownAuxiliaryProcess, datum::Datum::null())?;

    miscinit::SetProcessingMode(ProcessingMode::NormalProcessing);
    Ok(())
}

/// M4 bgjobs (docs/design/m4-bgjobs.md §3.6): the PER-LIFECYCLE half of
/// [`AuxiliaryProcessMainCommon`] for a job re-acquiring aux identity on
/// the process-lifetime dispatcher thread after a previous lifecycle
/// (clean teardown released everything through the shmem-exit chain;
/// crash-abandon was followed by a wholesale shmem reset). The
/// ONCE-PER-THREAD pieces — BaseInit's InitFileAccess etc. — must NOT
/// re-run ("call me only once"); everything identity-shaped re-runs.
/// Keep this list in lockstep with AuxiliaryProcessMainCommon above.
pub fn AuxiliaryProcessRejoinCommon() -> PgResult<()> {
    debug_assert!(init_small::globals::IsUnderPostmaster());
    debug_assert!(miscinit::IsInitProcessingMode());

    miscinit::SetIgnoreSystemIndexes(true);

    lmgr_proc::InitAuxiliaryProcess()?;

    procsignal::ProcSignalInit(&[])?;

    resowner::CreateAuxProcessResourceOwner()?;

    backend_status_seams::pgstat_beinit::call()?;
    backend_status_seams::pgstat_bestart_initial::call()?;
    backend_status_seams::pgstat_bestart_final::call()?;

    ipc::before_shmem_exit(ShutdownAuxiliaryProcess, datum::Datum::null())?;

    miscinit::SetProcessingMode(ProcessingMode::NormalProcessing);
    Ok(())
}

pub(crate) fn ShutdownAuxiliaryProcess(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    lwlock::LWLockReleaseAll()?;
    if condition_variable_seams::condition_variable_cancel_sleep::is_installed() {
        condition_variable_seams::condition_variable_cancel_sleep::call();
    }
    waitevent_seams::pgstat_report_wait_end::call();
    Ok(())
}

pub fn init_seams() {}
