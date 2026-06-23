#![allow(non_snake_case)]
// `PgError` is large and `PgResult` is the project-wide error contract; accept
// the large-`Err` lint crate-wide, matching the sibling postmaster crates.
#![allow(clippy::result_large_err)]

//! `backend-postmaster-auxprocess` — common bootstrap for auxiliary processes.
//!
//! Faithful Rust port of `src/backend/postmaster/auxprocess.c` (PostgreSQL
//! 18.3). [`AuxiliaryProcessMainCommon`] is the common initialization code run
//! by every auxiliary process — the bgwriter, walwriter, walreceiver,
//! checkpointer, the startup process, the walsummarizer, and the bootstrapper.
//! Unlike a regular backend, an aux process does not go through the full
//! `InitPostgres` pushups; it lights up only the few things it needs (a
//! `PGPROC`, `BaseInit`, `ProcSignal`, an aux resource owner, and backend
//! status), then registers the [`ShutdownAuxiliaryProcess`] before-shutdown
//! callback that releases LWLocks during an error exit.
//!
//! ## What this crate owns vs. what it reaches outside
//!
//! This crate owns the *orchestration* of `AuxiliaryProcessMainCommon` and the
//! body of `ShutdownAuxiliaryProcess`, exactly mirroring the C: the ordering of
//! the init steps, the `IgnoreSystemIndexes = true` side effect, and the
//! `InitProcessing -> NormalProcessing` mode transition at the end. The two C
//! `Assert`s (`IsUnderPostmaster` and `GetProcessingMode() == InitProcessing`)
//! are modeled as error returns — this codebase's faithful stand-in for an
//! aborting `Assert`.
//!
//! Two steps live in ported, directly-callable crates and are invoked directly
//! (genuine reuse, not a seam):
//!
//!  * `InitAuxiliaryProcess()` -> [`::lmgr_proc::InitAuxiliaryProcess`]
//!  * `ProcSignalInit(NULL, 0)` -> [`::procsignal::ProcSignalInit`]
//!    (no cancel key for an aux process; `MyProcNumber`/`MyProcPid` come from the
//!    globals seams, matching the postinit.c caller convention)
//!
//! Every other step lives in a subsystem reached through that subsystem's
//! owner seam (the project-wide call discipline for cross-crate effects):
//! deleting the inherited `PostmasterContext`, the ps-status display, the
//! processing-mode / ignore-system-indexes globals, `BaseInit`, the aux
//! resource owner, the cumulative-stats backend status, the before-shmem-exit
//! registration, and — for the shutdown callback — LWLock release, the
//! condition-variable cancel, and the wait-event end. Each seam panics loudly
//! ("seam not initialized: …") until its owner installs a real provider at
//! single-threaded startup. Nothing is silently stubbed.

use ::types_error::{PgError, PgResult};
use ::types_tuple::Datum;

use ::procsignal::ProcSignalInit;
use ::lmgr_proc::proc_lifecycle::InitAuxiliaryProcess;

/// `AuxiliaryProcessMainCommon`
///
/// Common initialization code for auxiliary processes, such as the bgwriter,
/// walwriter, walreceiver, and the startup process.
///
/// Faithful to `auxprocess.c:AuxiliaryProcessMainCommon`. The two C `Assert`s
/// (`IsUnderPostmaster` and `GetProcessingMode() == InitProcessing`) are modeled
/// as error returns, since a returned `Err` is this codebase's faithful
/// stand-in for an aborting `Assert` failure.
pub fn AuxiliaryProcessMainCommon() -> PgResult<()> {
    // Assert(IsUnderPostmaster);
    if !init_small_seams::is_under_postmaster::call() {
        return Err(PgError::error(
            "AuxiliaryProcessMainCommon: IsUnderPostmaster is false",
        ));
    }

    // Release postmaster's working memory context.
    //   if (PostmasterContext) { MemoryContextDelete(PostmasterContext);
    //                            PostmasterContext = NULL; }
    postmaster_seams::delete_postmaster_context::call();

    // init_ps_display(NULL);
    more_seams::init_ps_display::call(None);

    // Assert(GetProcessingMode() == InitProcessing);
    if !miscinit_seams::is_init_processing_mode::call() {
        return Err(PgError::error(
            "AuxiliaryProcessMainCommon: processing mode is not InitProcessing",
        ));
    }

    // IgnoreSystemIndexes = true;
    miscinit_seams::set_ignore_system_indexes::call(true);

    // As an auxiliary process, we aren't going to do the full InitPostgres
    // pushups, but there are a couple of things that need to get lit up even in
    // an auxiliary process.

    // Create a PGPROC so we can use LWLocks and access shared memory.
    //
    // InitAuxiliaryProcess() runs in the long-lived backend context; the ported
    // owner ignores the passed Mcx but expects a valid one, so we hand it
    // TopMemoryContext, matching the C (which allocates the PGPROC bookkeeping
    // out of the backend-lifetime context).
    let top_mcx = mcxt_seams::top_memory_context::call();
    InitAuxiliaryProcess(top_mcx)?;

    // BaseInit();
    postinit_seams::base_init::call()?;

    // ProcSignalInit(NULL, 0): no cancel key for an aux process. The ported
    // owner reads MyProcNumber/MyProcPid explicitly (the C reads the globals).
    ProcSignalInit(
        init_small_seams::my_proc_number::call(),
        init_small_seams::my_proc_pid::call(),
        &[],
    )?;

    // Auxiliary processes don't run transactions, but they may need a resource
    // owner anyway to manage buffer pins acquired outside transactions (and,
    // perhaps, other things in future).
    resowner_seams::create_aux_process_resource_owner::call()?;

    // Initialize backend status information.
    status_seams::pgstat_beinit::call()?;
    status_seams::pgstat_bestart_initial::call()?;
    status_seams::pgstat_bestart_final::call()?;

    // Register a before-shutdown callback for LWLock cleanup.
    //   before_shmem_exit(ShutdownAuxiliaryProcess, 0);
    dsm_core_seams::before_shmem_exit::call(
        ShutdownAuxiliaryProcess,
        Datum::from_i32(0),
    )?;

    // SetProcessingMode(NormalProcessing);
    miscinit_seams::set_processing_mode_normal::call();

    Ok(())
}

/// Begin shutdown of an auxiliary process. This is approximately the equivalent
/// of `ShutdownPostgres()` in postinit.c. We can't run transactions in an
/// auxiliary process, so most of the work of `AbortTransaction()` is not needed,
/// but we do need to make sure we've released any LWLocks we are holding. (This
/// is only critical during an error exit.)
///
/// Registered with `before_shmem_exit`, so its signature matches the callback
/// type `fn(i32, Datum<'static>) -> PgResult<()>`. Faithful to
/// `auxprocess.c:ShutdownAuxiliaryProcess`, which ignores its `code` and `arg`
/// parameters and is a `void` C function: it runs the three cleanup steps and
/// returns `Ok` unconditionally.
fn ShutdownAuxiliaryProcess(_code: i32, _arg: Datum<'static>) -> PgResult<()> {
    // LWLockReleaseAll();
    lwlock_seams::lwlock_release_all::call();
    // ConditionVariableCancelSleep();
    condition_variable_seams::condition_variable_cancel_sleep::call();
    // pgstat_report_wait_end();
    waitevent_seams::pgstat_report_wait_end::call();
    Ok(())
}

/// Install this unit's inward seams. Called once from `seams-init` at
/// single-threaded startup.
pub fn init_seams() {
    auxprocess_seams::auxiliary_process_main_common::set(
        AuxiliaryProcessMainCommon,
    );
}
