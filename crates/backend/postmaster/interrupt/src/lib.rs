#![allow(non_snake_case)]

use std::sync::atomic::{AtomicBool, Ordering};

use types_error::PgResult;
use types_guc::PGC_SIGHUP;

pub fn init_seams() {}

thread_local! {
    // volatile sig_atomic_t in C; handler stores must publish before the latch
    // wake is observed, so Release stores / Acquire loads (docs/graviton.md §2).
    static CONFIG_RELOAD_PENDING: AtomicBool = const { AtomicBool::new(false) };
    static SHUTDOWN_REQUEST_PENDING: AtomicBool = const { AtomicBool::new(false) };
}

#[inline]
pub fn ConfigReloadPending() -> bool {
    CONFIG_RELOAD_PENDING.with(|f| f.load(Ordering::Acquire))
}

#[inline]
pub fn SetConfigReloadPending(value: bool) {
    CONFIG_RELOAD_PENDING.with(|f| f.store(value, Ordering::Release));
}

#[inline]
pub fn ShutdownRequestPending() -> bool {
    SHUTDOWN_REQUEST_PENDING.with(|f| f.load(Ordering::Acquire))
}

#[inline]
pub fn SetShutdownRequestPending(value: bool) {
    SHUTDOWN_REQUEST_PENDING.with(|f| f.store(value, Ordering::Release));
}

pub fn ProcessMainLoopInterrupts() -> PgResult<()> {
    if procsignal_seams::proc_signal_barrier_pending::call() {
        procsignal_seams::process_proc_signal_barrier::call()?;
    }

    if ConfigReloadPending() {
        SetConfigReloadPending(false);
        guc_file_seams::process_config_file::call(PGC_SIGHUP)?;
    }

    if ShutdownRequestPending() {
        ipc_seams::proc_exit::call(0, init_small_seams::my_proc_pid::call());
    }

    // Flag owner (mcxt.c half) unported => the flag can never be set; the
    // uninstalled skip is C's false arm.
    if mcxt_seams::log_memory_context_pending::is_installed()
        && mcxt_seams::log_memory_context_pending::call()
    {
        mcxt_seams::process_log_memory_context_interrupt::call()?;
    }

    Ok(())
}

pub fn SignalHandlerForConfigReload() {
    SetConfigReloadPending(true);
    latch_seams::set_latch_my_latch::call();
}

pub fn SignalHandlerForCrashExit() -> ! {
    // _exit(2)'s thread rendering, never proc_exit callbacks: shmem may be
    // corrupt, and exit code 2 keeps the postmaster in its crash-reset cycle.
    ipc::exit_thread_raw(2)
}

pub fn SignalHandlerForShutdownRequest() {
    SetShutdownRequestPending(true);
    latch_seams::set_latch_my_latch::call();
}

#[cfg(test)]
mod tests;
