//! startup.c; the PostmasterIsAlive poll is dropped (one address space).

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};

use init_small::globals as g;
use timeout_seams::{
    STANDBY_DEADLOCK_TIMEOUT, STANDBY_LOCK_TIMEOUT, STANDBY_TIMEOUT, STARTUP_PROGRESS_TIMEOUT,
};
use types_core::TimestampTz;
use types_error::{PgError, PgResult};
use types_startup::StartupData;

#[cfg(test)]
mod tests;

// C `volatile sig_atomic_t` handler flags; the per-thread delivery design may
// run the handlers off-thread, hence process atomics.
static GOT_SIGHUP: AtomicBool = AtomicBool::new(false);
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
static PROMOTE_SIGNALED: AtomicBool = AtomicBool::new(false);
static IN_RESTORE_COMMAND: AtomicBool = AtomicBool::new(false);
static STARTUP_PROGRESS_TIMER_EXPIRED: AtomicBool = AtomicBool::new(false);
static POSTMASTER_POLL_COUNT: AtomicU32 = AtomicU32::new(0);

thread_local! {
    static LOG_STARTUP_PROGRESS_INTERVAL: Cell<i32> = const { Cell::new(10000) };
    static STARTUP_PROGRESS_PHASE_START_TIME: Cell<TimestampTz> = const { Cell::new(0) };
}

pub fn log_startup_progress_interval() -> i32 {
    LOG_STARTUP_PROGRESS_INTERVAL.get()
}

fn WakeupRecovery() {
    xlogrecovery_seams::wakeup_recovery::call();
}

/// SIGUSR2 handler body (promotion trigger).
pub fn StartupProcTriggerHandler() {
    PROMOTE_SIGNALED.store(true, Relaxed);
    WakeupRecovery();
}

/// SIGHUP handler body.
pub fn StartupProcSigHupHandler() {
    GOT_SIGHUP.store(true, Relaxed);
    WakeupRecovery();
}

/// SIGTERM handler body.
pub fn StartupProcShutdownHandler() {
    if IN_RESTORE_COMMAND.load(Relaxed) {
        ipc::proc_exit(1, g::MyProcPid());
    }
    SHUTDOWN_REQUESTED.store(true, Relaxed);
    WakeupRecovery();
}

// StartupRereadConfig (startup.c:157): a walreceiver-parameter change
// across the reload forces a walreceiver restart so the new
// primary_conninfo/slot takes effect (048_vacuum_horizon_floor relies on the
// walreceiver dying when primary_conninfo goes invalid). The C diff of this
// process's private pre/post-reload GUC copies is impossible here — string
// GUC backings are process-shared, the postmaster's reload is visible before
// ours runs — so xlogrecovery diffs against the walreceiver's started-with
// values instead.
fn StartupRereadConfig() -> PgResult<()> {
    guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;

    if xlogrecovery_seams::startup_reread_walrcv_config::is_installed() {
        xlogrecovery_seams::startup_reread_walrcv_config::call();
    }
    Ok(())
}

pub fn ProcessStartupProcInterrupts() -> PgResult<()> {
    if GOT_SIGHUP.swap(false, Relaxed) {
        StartupRereadConfig()?;
    }

    if SHUTDOWN_REQUESTED.load(Relaxed) {
        ipc::proc_exit(1, g::MyProcPid());
    }

    POSTMASTER_POLL_COUNT.fetch_add(1, Relaxed);

    if procsignal_seams::proc_signal_barrier_pending::call() {
        procsignal_seams::process_proc_signal_barrier::call()?;
    }

    // mcxt.c flag half unported => never set; skip is C's false arm.
    if mcxt_seams::log_memory_context_pending::is_installed()
        && mcxt_seams::log_memory_context_pending::call()
    {
        mcxt_seams::process_log_memory_context_interrupt::call()?;
    }
    Ok(())
}

fn StartupProcExit(_code: i32, _arg: usize) {
    if xlogutils::standby_state() != xlogutils::STANDBY_DISABLED {
        standby_seams::shutdown_recovery_transaction_environment::call()
            .unwrap_or_else(|e| panic!("ShutdownRecoveryTransactionEnvironment: {e:?}"));
    }
}

fn fatal_exit(e: &PgError) -> ! {
    elog::emit_error_report_for(e);
    ipc::proc_exit(1, g::MyProcPid())
}

pub fn StartupProcessMain(startup_data: &StartupData) -> ! {
    debug_assert!(matches!(startup_data, StartupData::None));

    miscinit::SetMyBackendType(types_core::BackendType::Startup);
    if let Err(e) = auxprocess::AuxiliaryProcessMainCommon() {
        fatal_exit(&e);
    }

    ipc::on_shmem_exit(StartupProcExit, 0);

    {
        use procsignal::ThreadSignalHandler::{Ignore, Simple};
        procsignal::pqsignal_thread(procsignal::signums::SIGHUP, Simple(StartupProcSigHupHandler));
        procsignal::pqsignal_thread(procsignal::signums::SIGINT, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGTERM, Simple(StartupProcShutdownHandler));
        procsignal::pqsignal_thread(procsignal::signums::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGUSR2, Simple(StartupProcTriggerHandler));
    }

    timeout::InitializeTimeouts();

    timeout::RegisterTimeout(STANDBY_DEADLOCK_TIMEOUT, standby::StandbyDeadLockHandler);
    timeout::RegisterTimeout(STANDBY_TIMEOUT, standby::StandbyTimeoutHandler);
    timeout::RegisterTimeout(STANDBY_LOCK_TIMEOUT, standby::StandbyLockTimeoutHandler);

    libpq_pqsignal::unblock_signals();

    if let Err(e) = transam_xlog::StartupXLOG() {
        fatal_exit(&e);
    }

    // Exit code 0 tells the postmaster recovery completed successfully.
    ipc::proc_exit(0, g::MyProcPid())
}

pub fn PreRestoreCommand() {
    IN_RESTORE_COMMAND.store(true, Relaxed);
    if SHUTDOWN_REQUESTED.load(Relaxed) {
        ipc::proc_exit(1, g::MyProcPid());
    }
}

pub fn PostRestoreCommand() {
    IN_RESTORE_COMMAND.store(false, Relaxed);
}

pub fn IsPromoteSignaled() -> bool {
    PROMOTE_SIGNALED.load(Relaxed)
}

pub fn ResetPromoteSignaled() {
    PROMOTE_SIGNALED.store(false, Relaxed);
}

pub fn startup_progress_timeout_handler() {
    STARTUP_PROGRESS_TIMER_EXPIRED.store(true, Relaxed);
}

fn register_startup_progress_timeout() {
    if !miscinit::IsBootstrapProcessingMode() {
        timeout::RegisterTimeout(STARTUP_PROGRESS_TIMEOUT, startup_progress_timeout_handler);
    }
}

pub fn disable_startup_progress_timeout() {
    if log_startup_progress_interval() == 0 {
        return;
    }
    timeout::disable_timeout(STARTUP_PROGRESS_TIMEOUT, false);
    STARTUP_PROGRESS_TIMER_EXPIRED.store(false, Relaxed);
}

pub fn enable_startup_progress_timeout() {
    let interval = log_startup_progress_interval();
    if interval == 0 {
        return;
    }
    let start = timestamp_seams::get_current_timestamp::call();
    STARTUP_PROGRESS_PHASE_START_TIME.set(start);
    let fin_time = start + interval as i64 * 1000;
    timeout::enable_timeout_every(STARTUP_PROGRESS_TIMEOUT, fin_time, interval);
}

pub fn begin_startup_progress_phase() {
    if log_startup_progress_interval() == 0 {
        return;
    }
    disable_startup_progress_timeout();
    enable_startup_progress_timeout();
}

pub fn has_startup_progress_timeout_expired() -> Option<(i64, i32)> {
    if !STARTUP_PROGRESS_TIMER_EXPIRED.load(Relaxed) {
        return None;
    }
    let now = timestamp_seams::get_current_timestamp::call();
    let diff = now - STARTUP_PROGRESS_PHASE_START_TIME.get();
    STARTUP_PROGRESS_TIMER_EXPIRED.store(false, Relaxed);
    Some((diff / 1_000_000, (diff % 1_000_000) as i32))
}

pub fn init_seams() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::log_startup_progress_interval.install(GucVarAccessors {
        get: log_startup_progress_interval,
        set: |v| LOG_STARTUP_PROGRESS_INTERVAL.set(v),
    });
    startup_seams::begin_startup_progress_phase::set(begin_startup_progress_phase);
    startup_seams::register_startup_progress_timeout::set(register_startup_progress_timeout);
    startup_seams::process_startup_proc_interrupts::set(ProcessStartupProcInterrupts);
    startup_seams::is_promote_signaled::set(IsPromoteSignaled);
    startup_seams::reset_promote_signaled::set(ResetPromoteSignaled);
    startup_seams::disable_startup_progress_timeout::set(disable_startup_progress_timeout);
    startup_seams::pre_restore_command::set(PreRestoreCommand);
    startup_seams::post_restore_command::set(PostRestoreCommand);
}
