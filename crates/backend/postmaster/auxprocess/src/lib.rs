//! auxprocess.c. Thread-model divergence: the PostmasterContext release is a
//! no-op (no forked copy of the postmaster's context exists to free); the
//! caller sets MyBackendType before entry, as in C.

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

pub(crate) fn ShutdownAuxiliaryProcess(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    lwlock::LWLockReleaseAll()?;
    // ConditionVariableCancelSleep: while the CV unit is unported no thread
    // can have prepared a sleep (any sleep call panics first), so skipping
    // the uninstalled seam is exactly C's no-pending-sleep no-op arm.
    if condition_variable_seams::condition_variable_cancel_sleep::is_installed() {
        condition_variable_seams::condition_variable_cancel_sleep::call();
    }
    waitevent_seams::pgstat_report_wait_end::call();
    Ok(())
}

pub fn init_seams() {}
