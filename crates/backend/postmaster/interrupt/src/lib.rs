//! Port of `src/backend/postmaster/interrupt.c` (PostgreSQL 18.3): the
//! generic interrupt-handling helpers shared by background / auxiliary
//! processes — a main-loop interrupt processor and three signal-handler
//! bodies.
//!
//! `interrupt.c` defines two `volatile sig_atomic_t` flags,
//! `ConfigReloadPending` and `ShutdownRequestPending`. They are per-backend
//! state (each C backend is a process), so they are thread-locals here, never
//! shared statics. Everything else the file touches is owned elsewhere and
//! reached through the owners' seam crates.

#![allow(non_snake_case)]

use std::cell::Cell;

use types_error::PgResult;

thread_local! {
    /// `volatile sig_atomic_t ConfigReloadPending = false;`
    static CONFIG_RELOAD_PENDING: Cell<bool> = const { Cell::new(false) };

    /// `volatile sig_atomic_t ShutdownRequestPending = false;`
    static SHUTDOWN_REQUEST_PENDING: Cell<bool> = const { Cell::new(false) };
}

/// Read `ConfigReloadPending` (the global other modules `extern`-reference).
#[inline]
pub fn ConfigReloadPending() -> bool {
    CONFIG_RELOAD_PENDING.get()
}

/// Write `ConfigReloadPending`.
#[inline]
pub fn SetConfigReloadPending(value: bool) {
    CONFIG_RELOAD_PENDING.set(value);
}

/// Read `ShutdownRequestPending` (the global other modules `extern`-reference).
#[inline]
pub fn ShutdownRequestPending() -> bool {
    SHUTDOWN_REQUEST_PENDING.get()
}

/// Write `ShutdownRequestPending`.
#[inline]
pub fn SetShutdownRequestPending(value: bool) {
    SHUTDOWN_REQUEST_PENDING.set(value);
}

/// `ProcessMainLoopInterrupts(void)` — simple interrupt handler for main
/// loops of background processes.
///
/// `ProcessProcSignalBarrier`, `ProcessConfigFile`, and
/// `ProcessLogMemoryContextInterrupt` can all `ereport(ERROR)` in C, which
/// longjmps out of this function; here that is the `Err` return. When a
/// shutdown request is pending this calls `proc_exit(0)`, which does not
/// return.
pub fn ProcessMainLoopInterrupts() -> PgResult<()> {
    if procsignal_seams::proc_signal_barrier_pending::call() {
        procsignal_seams::process_proc_signal_barrier::call()?;
    }

    if ConfigReloadPending() {
        SetConfigReloadPending(false);
        guc_file_seams::process_config_file::call(types_guc::PGC_SIGHUP)?;
    }

    if ShutdownRequestPending() {
        dsm_core_seams::proc_exit::call(
            0,
            init_small_seams::my_proc_pid::call(),
        );
    }

    // Perform logging of memory contexts of this process
    if mcxt_seams::log_memory_context_pending::call() {
        mcxt_seams::process_log_memory_context_interrupt::call()?;
    }

    Ok(())
}

/// `SignalHandlerForConfigReload(SIGNAL_ARGS)` — simple signal handler for
/// triggering a configuration reload.
///
/// Normally, this handler would be used for SIGHUP. The idea is that code
/// which uses it would arrange to check the [`ConfigReloadPending`] flag at
/// convenient places inside main loops, or else call
/// [`ProcessMainLoopInterrupts`].
pub fn SignalHandlerForConfigReload() {
    SetConfigReloadPending(true);
    latch_seams::set_latch_my_latch::call();
}

/// `SignalHandlerForCrashExit(SIGNAL_ARGS)` — simple signal handler for
/// exiting quickly as if due to a crash. Normally, this would be used for
/// handling SIGQUIT.
///
/// We DO NOT want to run `proc_exit()` or atexit() callbacks — we're here
/// because shared memory may be corrupted, so we don't want to try to clean
/// up our transaction. Just nail the windows shut and get out of town. The
/// callbacks wouldn't be safe to run from a signal handler, anyway.
///
/// Note we do `_exit(2)` not `_exit(0)`. This is to force the postmaster
/// into a system reset cycle if someone sends a manual SIGQUIT to a random
/// backend. This is necessary precisely because we don't clean up our shared
/// memory state. (The "dead man switch" mechanism in pmsignal.c should
/// ensure the postmaster sees this as a crash, too, but no harm in being
/// doubly sure.)
pub fn SignalHandlerForCrashExit() -> ! {
    unsafe { libc::_exit(2) }
}

/// `SignalHandlerForShutdownRequest(SIGNAL_ARGS)` — simple signal handler
/// for triggering a long-running background process to shut down and exit.
///
/// Typically, this handler would be used for SIGTERM, but some processes use
/// other signals. In particular, the checkpointer and parallel apply worker
/// exit on SIGUSR2, and the WAL writer exits on either SIGINT or SIGTERM.
///
/// [`ShutdownRequestPending`] should be checked at a convenient place within
/// the main loop, or else the main loop should call
/// [`ProcessMainLoopInterrupts`].
pub fn SignalHandlerForShutdownRequest() {
    SetShutdownRequestPending(true);
    latch_seams::set_latch_my_latch::call();
}

/// `fn(int)` wrapper around [`SignalHandlerForCrashExit`] (which is `-> !`) so
/// it matches the `void (*)(int)` C `pqsigfunc` shape that `pqsignal()` installs.
fn crash_exit_handler(_signo: i32) {
    SignalHandlerForCrashExit()
}

/// Install this unit's inward seams: the miscinit.c child-startup signal-mask
/// wrappers whose seam decls live in `backend-postmaster-interrupt-seams` and
/// which compose interrupt.c's `SignalHandlerForCrashExit` with the pqsignal.c
/// signal-mask machinery.
pub fn init_seams() {
    use interrupt_seams as s;

    // miscinit.c InitPostmasterChild (lines 152-155): every postmaster child
    // responds promptly to SIGQUIT — `pqsignal(SIGQUIT, SignalHandlerForCrashExit)`
    // installs the crash handler (interrupt.c's body), then `sigdelset(&BlockSig,
    // SIGQUIT); sigprocmask(SIG_SETMASK, &BlockSig, NULL)` unblocks it. The
    // signal-mask globals belong to pqsignal.c (backend-libpq-pqsignal); the
    // handler installer is src/port/pqsignal.c (reached through its seam).
    s::install_crash_exit_sigquit_handler::set(|| {
        port_pqsignal_seams::pqsignal::call(
            libc::SIGQUIT,
            signal::SigHandler::Handler(crash_exit_handler),
        );
        libpq_pqsignal::block_sig_delete(libc::SIGQUIT);
        libpq_pqsignal::set_block_sig_mask();
        Ok(())
    });

    // miscinit.c InitStandaloneProcess (lines 199-200): `pqinitmask();
    // sigprocmask(SIG_SETMASK, &BlockSig, NULL)` — initialize the masks and
    // install BlockSig (no SIGQUIT unblock or default handler here).
    s::pqinitmask_set_blocksig::set(|| {
        libpq_pqsignal::pqinitmask();
        libpq_pqsignal::set_block_sig_mask();
        Ok(())
    });
}

#[cfg(test)]
mod tests;
