//! Port of `src/backend/postmaster/startup.c` (PostgreSQL 18.3): the Startup
//! (recovery) process.
//!
//! The Startup process initialises the server and performs any recovery
//! actions that have been specified. There is no "main loop" since the
//! Startup process ends as soon as initialisation is complete (in standby
//! mode one can think of the replay loop inside `StartupXLOG` as a main
//! loop).
//!
//! startup.c's `volatile sig_atomic_t` flags and the progress-phase start
//! timestamp are per-backend process state (not shared memory), so they are
//! thread-locals here, as is the `log_startup_progress_interval` GUC.

#![allow(non_snake_case)]

use std::cell::Cell;

use mcx::Mcx;
use types_core::TimestampTz;
use types_error::PgResult;
use signal::SigHandler;
use types_timeout::TimeoutId::{
    STANDBY_DEADLOCK_TIMEOUT, STANDBY_LOCK_TIMEOUT, STANDBY_TIMEOUT, STARTUP_PROGRESS_TIMEOUT,
};

#[cfg(test)]
mod tests;

/// On systems that need to make a system call to find out if the postmaster
/// has gone away, we'll do so only every Nth call to
/// [`ProcessStartupProcInterrupts`]. This only affects how long it takes us
/// to detect the condition while we're busy replaying WAL. Latch waits and
/// similar should react immediately through the usual techniques.
///
/// C compiles this (and the modulo poll below) only `#ifndef
/// USE_POSTMASTER_DEATH_SIGNAL`; `storage/pmsignal.h` defines that macro
/// where a parent-death signal exists (`PR_SET_PDEATHSIG` on Linux,
/// `PROC_PDEATHSIG_CTL` on FreeBSD), and there the probe runs on every call.
#[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
const POSTMASTER_POLL_RATE_LIMIT: u32 = 1024;

thread_local! {
    /// `static volatile sig_atomic_t got_SIGHUP = false;`
    static GOT_SIGHUP: Cell<bool> = const { Cell::new(false) };

    /// `static volatile sig_atomic_t shutdown_requested = false;`
    static SHUTDOWN_REQUESTED: Cell<bool> = const { Cell::new(false) };

    /// `static volatile sig_atomic_t promote_signaled = false;`
    static PROMOTE_SIGNALED: Cell<bool> = const { Cell::new(false) };

    /// Flag set when executing a restore command, to tell the SIGTERM signal
    /// handler that it's safe to just proc_exit.
    static IN_RESTORE_COMMAND: Cell<bool> = const { Cell::new(false) };

    /// Time at which the most recent startup operation started.
    static STARTUP_PROGRESS_PHASE_START_TIME: Cell<TimestampTz> = const { Cell::new(0) };

    /// Indicates whether the startup progress interval mentioned by the user
    /// has elapsed: `true` if the timeout occurred, `false` otherwise.
    static STARTUP_PROGRESS_TIMER_EXPIRED: Cell<bool> = const { Cell::new(false) };

    /// `int log_startup_progress_interval = 10000;` — time between progress
    /// updates for long-running startup operations, in milliseconds (10 sec
    /// by default; 0 disables the feature). A `PGC_SIGHUP` GUC.
    static LOG_STARTUP_PROGRESS_INTERVAL: Cell<i32> = const { Cell::new(10000) };
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
thread_local! {
    /// `static uint32 postmaster_poll_count` in
    /// `ProcessStartupProcInterrupts` (the rate-limited death-probe branch).
    static POSTMASTER_POLL_COUNT: Cell<u32> = const { Cell::new(0) };
}

/// Read the `log_startup_progress_interval` GUC.
#[inline]
pub fn log_startup_progress_interval() -> i32 {
    LOG_STARTUP_PROGRESS_INTERVAL.get()
}

/// Write the `log_startup_progress_interval` GUC (the GUC assignment path).
#[inline]
pub fn set_log_startup_progress_interval(value: i32) {
    LOG_STARTUP_PROGRESS_INTERVAL.set(value);
}

// --------------------------------
//		signal handler routines
// --------------------------------

/// SIGUSR2: set flag to finish recovery (`StartupProcTriggerHandler`).
fn StartupProcTriggerHandler(_postgres_signal_arg: i32) {
    PROMOTE_SIGNALED.set(true);
    xlogrecovery_seams::wakeup_recovery::call();
}

/// SIGHUP: set flag to re-read config file at next convenient time
/// (`StartupProcSigHupHandler`).
fn StartupProcSigHupHandler(_postgres_signal_arg: i32) {
    GOT_SIGHUP.set(true);
    xlogrecovery_seams::wakeup_recovery::call();
}

/// SIGTERM: set flag to abort redo and exit (`StartupProcShutdownHandler`).
fn StartupProcShutdownHandler(_postgres_signal_arg: i32) {
    if IN_RESTORE_COMMAND.get() {
        dsm_core_seams::proc_exit::call(1, init_small_seams::my_proc_pid::call());
    } else {
        SHUTDOWN_REQUESTED.set(true);
    }
    xlogrecovery_seams::wakeup_recovery::call();
}

/// `StartupRereadConfig()` — re-read the config file.
///
/// If one of the critical walreceiver options has changed, flag xlog.c to
/// restart it.
fn StartupRereadConfig(mcx: Mcx<'_>) -> PgResult<()> {
    // char *conninfo = pstrdup(PrimaryConnInfo); — copied into mcx, the
    // caller's current context, exactly as the C pstrdup.
    let conninfo = xlogrecovery_seams::primary_conninfo::call(mcx)?;
    let slotname = xlogrecovery_seams::primary_slot_name::call(mcx)?;
    let temp_slot =
        xlogrecovery_seams::wal_receiver_create_temp_slot::call();
    let mut temp_slot_changed = false;

    guc_file_seams::process_config_file::call(types_guc::PGC_SIGHUP)?;

    let conninfo_changed =
        conninfo != xlogrecovery_seams::primary_conninfo::call(mcx)?;
    let slotname_changed =
        slotname != xlogrecovery_seams::primary_slot_name::call(mcx)?;

    // wal_receiver_create_temp_slot is used only when we have no slot
    // configured. We do not need to track this change if it has no effect.
    if !slotname_changed
        && xlogrecovery_seams::primary_slot_name::call(mcx)?.is_empty()
    {
        temp_slot_changed = temp_slot
            != xlogrecovery_seams::wal_receiver_create_temp_slot::call();
    }

    if conninfo_changed || slotname_changed || temp_slot_changed {
        xlogrecovery_seams::startup_request_wal_receiver_restart::call();
    }
    Ok(())
}

/// Whether the (rate-limited, where no postmaster-death signal exists)
/// postmaster-death probe is due on this call.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
#[inline]
fn postmaster_poll_due() -> bool {
    true
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
#[inline]
fn postmaster_poll_due() -> bool {
    // C: postmaster_poll_count++ % POSTMASTER_POLL_RATE_LIMIT == 0
    POSTMASTER_POLL_COUNT.with(|count| {
        let n = count.get();
        count.set(n.wrapping_add(1));
        n % POSTMASTER_POLL_RATE_LIMIT == 0
    })
}

/// `ProcessStartupProcInterrupts()` — process various signals that might be
/// sent to the startup process. `mcx` is the caller's current context, used
/// for the transient GUC-snapshot copies in the config-reload path.
pub fn ProcessStartupProcInterrupts(mcx: Mcx<'_>) -> PgResult<()> {
    // Process any requests or signals received recently.
    if GOT_SIGHUP.get() {
        GOT_SIGHUP.set(false);
        StartupRereadConfig(mcx)?;
    }

    // Check if we were requested to exit without finishing recovery.
    if SHUTDOWN_REQUESTED.get() {
        dsm_core_seams::proc_exit::call(1, init_small_seams::my_proc_pid::call());
    }

    // Emergency bailout if postmaster has died. This is to avoid the
    // necessity for manual cleanup of all postmaster children. Do this less
    // frequently on systems for which we don't have signals to make that
    // cheap.
    if init_small_seams::is_under_postmaster::call()
        && postmaster_poll_due()
        && !pmsignal_seams::postmaster_is_alive::call()
    {
        // C: exit(1) — deliberately NOT proc_exit(): the postmaster is gone,
        // so die immediately without running the proc_exit callback chain.
        unsafe { libc::exit(1) }
    }

    // Process barrier events.
    if procsignal_seams::proc_signal_barrier_pending::call() {
        procsignal_seams::process_proc_signal_barrier::call()?;
    }

    // Perform logging of memory contexts of this process.
    if mcxt_seams::log_memory_context_pending::call() {
        mcxt_seams::process_log_memory_context_interrupt::call()?;
    }

    Ok(())
}

/// `StartupProcExit(int code, Datum arg)` — on_shmem_exit callback: shut
/// down the recovery environment.
fn StartupProcExit(_code: i32, _arg: types_tuple::Datum<'static>) -> PgResult<()> {
    if xlogutils_seams::standby_state::call() != wal::STANDBY_DISABLED
    {
        standby_seams::shutdown_recovery_transaction_environment::call()?;
    }
    Ok(())
}

// ----------------------------------
//	Startup Process main entry point
// ----------------------------------

/// `StartupProcessMain(const void *startup_data, size_t startup_data_len)` —
/// Startup Process main entry point.
///
/// `pg_noreturn` in C: ends with `proc_exit(0)` (exit code 0 tells the
/// postmaster that recovery completed successfully). The `Ok` arm is
/// unreachable; an `Err` is an `ereport(ERROR)` escaping with no handler,
/// which the caller's elog machinery promotes to FATAL as in C.
pub fn StartupProcessMain(startup_data: &[u8]) -> PgResult<()> {
    // Assert(startup_data_len == 0);
    assert_eq!(startup_data.len(), 0);

    // MyBackendType = B_STARTUP;
    init_small_seams::set_my_backend_type::call(types_core::init::BackendType::Startup);
    auxprocess_seams::auxiliary_process_main_common::call()?;

    // Arrange to clean up at startup process exit.
    dsm_core_seams::on_shmem_exit::call(
        StartupProcExit,
        types_tuple::Datum::null(),
    )?;

    // Properly accept or ignore signals the postmaster might send us.
    port_pqsignal_seams::pqsignal::call(
        libc::SIGHUP,
        SigHandler::Handler(StartupProcSigHupHandler),
    ); // reload config file
    port_pqsignal_seams::pqsignal::call(libc::SIGINT, SigHandler::Ignore); // ignore query cancel
    port_pqsignal_seams::pqsignal::call(
        libc::SIGTERM,
        SigHandler::Handler(StartupProcShutdownHandler),
    ); // request shutdown
    // SIGQUIT handler was already set up by InitPostmasterChild
    timeout_seams::initialize_timeouts::call(); // establishes SIGALRM handler
    port_pqsignal_seams::pqsignal::call(libc::SIGPIPE, SigHandler::Ignore);
    port_pqsignal_seams::pqsignal::call(
        libc::SIGUSR1,
        SigHandler::Handler(
            procsignal_seams::procsignal_sigusr1_handler::call,
        ),
    );
    port_pqsignal_seams::pqsignal::call(
        libc::SIGUSR2,
        SigHandler::Handler(StartupProcTriggerHandler),
    );

    // Reset some signals that are accepted by postmaster but not here.
    port_pqsignal_seams::pqsignal::call(libc::SIGCHLD, SigHandler::Default);

    // Register timeouts needed for standby mode.
    timeout_seams::register_timeout::call(
        STANDBY_DEADLOCK_TIMEOUT,
        standby_seams::standby_dead_lock_handler::call,
    );
    timeout_seams::register_timeout::call(
        STANDBY_TIMEOUT,
        standby_seams::standby_timeout_handler::call,
    );
    timeout_seams::register_timeout::call(
        STANDBY_LOCK_TIMEOUT,
        standby_seams::standby_lock_timeout_handler::call,
    );

    // Unblock signals (they were blocked when the postmaster forked us).
    // C: sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);
    let masks = libpq_pqsignal::signal_masks();
    // SAFETY: setting this thread's signal mask from an initialized sigset_t.
    unsafe {
        libc::sigprocmask(libc::SIG_SETMASK, masks.unblock_sig(), std::ptr::null_mut());
    }

    // Do what we came for.
    transam_xlog_seams::startup_xlog::call()?;

    // Exit normally. Exit code 0 tells postmaster that we completed recovery
    // successfully.
    dsm_core_seams::proc_exit::call(0, init_small_seams::my_proc_pid::call())
}

/// `PreRestoreCommand()`.
pub fn PreRestoreCommand() {
    // Set in_restore_command to tell the signal handler that we should exit
    // right away on SIGTERM. We know that we're at a safe point to do that.
    // Check if we had already received the signal, so that we don't miss a
    // shutdown request received just before this.
    IN_RESTORE_COMMAND.set(true);
    if SHUTDOWN_REQUESTED.get() {
        dsm_core_seams::proc_exit::call(1, init_small_seams::my_proc_pid::call());
    }
}

/// `PostRestoreCommand()`.
pub fn PostRestoreCommand() {
    IN_RESTORE_COMMAND.set(false);
}

/// `IsPromoteSignaled()`.
pub fn IsPromoteSignaled() -> bool {
    PROMOTE_SIGNALED.get()
}

/// `ResetPromoteSignaled()`.
pub fn ResetPromoteSignaled() {
    PROMOTE_SIGNALED.set(false);
}

/// `startup_progress_timeout_handler()` — set a flag indicating that it's
/// time to log a progress report. Registered against
/// `STARTUP_PROGRESS_TIMEOUT` by xlog.c.
pub fn startup_progress_timeout_handler() {
    STARTUP_PROGRESS_TIMER_EXPIRED.set(true);
}

/// `disable_startup_progress_timeout()`.
pub fn disable_startup_progress_timeout() {
    // Feature is disabled.
    if log_startup_progress_interval() == 0 {
        return;
    }

    timeout_seams::disable_timeout::call(STARTUP_PROGRESS_TIMEOUT, false);
    STARTUP_PROGRESS_TIMER_EXPIRED.set(false);
}

/// `enable_startup_progress_timeout()` — set the start timestamp of the
/// current operation and enable the timeout.
pub fn enable_startup_progress_timeout() {
    // Feature is disabled.
    if log_startup_progress_interval() == 0 {
        return;
    }

    let start_time = timestamp_seams::get_current_timestamp::call();
    STARTUP_PROGRESS_PHASE_START_TIME.set(start_time);
    let fin_time =
        TimestampTzPlusMilliseconds(start_time, log_startup_progress_interval() as i64);
    timeout_seams::enable_timeout_every::call(
        STARTUP_PROGRESS_TIMEOUT,
        fin_time,
        log_startup_progress_interval(),
    );
}

/// `begin_startup_progress_phase()` — a thin wrapper to first disable and
/// then enable the startup progress timeout.
pub fn begin_startup_progress_phase() {
    // Feature is disabled.
    if log_startup_progress_interval() == 0 {
        return;
    }

    disable_startup_progress_timeout();
    enable_startup_progress_timeout();
}

/// `has_startup_progress_timeout_expired(long *secs, int *usecs)` — report
/// whether the startup progress timeout has occurred. If it did, reset the
/// timer flag and return `Some((secs, usecs))` (the elapsed time in the
/// current phase, the C out-parameters); otherwise return `None`.
pub fn has_startup_progress_timeout_expired() -> Option<(i64, i32)> {
    // No timeout has occurred.
    if !STARTUP_PROGRESS_TIMER_EXPIRED.get() {
        return None;
    }

    // Calculate the elapsed time.
    let now = timestamp_seams::get_current_timestamp::call();
    let (seconds, useconds) = timestamp_seams::timestamp_difference::call(
        STARTUP_PROGRESS_PHASE_START_TIME.get(),
        now,
    );

    STARTUP_PROGRESS_TIMER_EXPIRED.set(false);

    Some((seconds, useconds))
}

/// `TimestampTzPlusMilliseconds(tz, ms)` (`utils/timestamp.h`):
/// `((tz) + (ms) * (int64) 1000)`.
#[inline]
fn TimestampTzPlusMilliseconds(tz: TimestampTz, ms: i64) -> TimestampTz {
    tz + ms * 1000
}

/// Seam-compatible entry point: adapts `StartupProcessMain` to the
/// `fn(&types_startup::StartupData) -> !` signature required by
/// `backend-postmaster-startup-seams`.
///
/// `startup.c`'s `StartupProcessMain` always asserts `startup_data_len == 0`
/// (the Startup process receives no typed startup data from the postmaster).
/// The call to `StartupProcessMain` ends with `proc_exit(0)` and never
/// returns `Ok`; the unreachable below mirrors that contract.
fn startup_process_main_entry(startup_data: &types_startup::StartupData) -> ! {
    let bytes: &[u8] = match startup_data {
        types_startup::StartupData::None => &[],
        _ => panic!("startup_process_main: unexpected non-None StartupData"),
    };
    let _ = StartupProcessMain(bytes);
    unreachable!("StartupProcessMain ended without calling proc_exit")
}

/// Install this crate's implementations into `backend-postmaster-startup-seams`.
pub fn init_seams() {
    startup_seams::process_startup_proc_interrupts::set(
        ProcessStartupProcInterrupts,
    );
    startup_seams::pre_restore_command::set(PreRestoreCommand);
    startup_seams::post_restore_command::set(PostRestoreCommand);
    startup_seams::is_promote_signaled::set(IsPromoteSignaled);
    startup_seams::reset_promote_signaled::set(ResetPromoteSignaled);
    startup_seams::begin_startup_progress_phase::set(
        begin_startup_progress_phase,
    );
    startup_seams::disable_startup_progress_timeout::set(
        disable_startup_progress_timeout,
    );
    startup_seams::has_startup_progress_timeout_expired::set(
        has_startup_progress_timeout_expired,
    );
    startup_seams::startup_progress_timeout_handler::set(
        startup_progress_timeout_handler,
    );
    startup_seams::log_startup_progress_interval::set(
        log_startup_progress_interval,
    );
    startup_seams::set_log_startup_progress_interval::set(
        set_log_startup_progress_interval,
    );
    startup_seams::startup_process_main::set(startup_process_main_entry);
    // `int log_startup_progress_interval` (startup.c GUC, boot 10000) — install
    // the guc-tables slot over this crate's backing accessors.
    guc_tables::vars::log_startup_progress_interval.install(
        guc_tables::GucVarAccessors {
            get: log_startup_progress_interval,
            set: set_log_startup_progress_interval,
        },
    );
}
