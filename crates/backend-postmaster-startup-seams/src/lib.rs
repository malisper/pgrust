//! Seam declarations for the `backend-postmaster-startup` unit
//! (`postmaster/startup.c` — the recovery startup *process*, distinct from
//! `tcop/backend_startup.c`).
//!
//! Installed by `backend_postmaster_startup::init_seams()`. Cyclic callers:
//! the WAL units (`xlog.c`, `xlogrecovery.c`, `xlogarchive.c`), the
//! file-manager startup paths (`fd.c`, `reinit.c`), and the GUC machinery
//! (the `log_startup_progress_interval` variable).

seam_core::seam!(
    /// `ProcessStartupProcInterrupts()` — service SIGHUP/shutdown/postmaster
    /// death/barrier/memory-context-log requests in the redo loop. `mcx` is
    /// the caller's current context (the config-reload path `pstrdup`s GUC
    /// snapshots into it). The config-reload and interrupt-service paths can
    /// `ereport(ERROR)`.
    pub fn process_startup_proc_interrupts<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PreRestoreCommand()` — arm the SIGTERM handler to `proc_exit(1)`
    /// immediately while a restore command runs (may itself `proc_exit(1)`
    /// if a shutdown request already arrived; never returns an error).
    pub fn pre_restore_command()
);

seam_core::seam!(
    /// `PostRestoreCommand()` — disarm what `pre_restore_command` armed.
    pub fn post_restore_command()
);

seam_core::seam!(
    /// `IsPromoteSignaled()` — has SIGUSR2 (promotion) been received?
    pub fn is_promote_signaled() -> bool
);

seam_core::seam!(
    /// `ResetPromoteSignaled()` — clear the promotion flag.
    pub fn reset_promote_signaled()
);

seam_core::seam!(
    /// `begin_startup_progress_phase()` — restart the startup-progress
    /// reporting cadence for a new long-running startup operation.
    pub fn begin_startup_progress_phase()
);

seam_core::seam!(
    /// `disable_startup_progress_timeout()` — stop the startup-progress
    /// timer and clear its expiry flag.
    pub fn disable_startup_progress_timeout()
);

seam_core::seam!(
    /// `has_startup_progress_timeout_expired(long *secs, int *usecs)` —
    /// `Some((secs, usecs))` (elapsed time in the current phase, with the
    /// expiry flag reset) if the progress interval elapsed, else `None`.
    pub fn has_startup_progress_timeout_expired() -> Option<(i64, i32)>
);

seam_core::seam!(
    /// `startup_progress_timeout_handler()` — the `STARTUP_PROGRESS_TIMEOUT`
    /// expiry callback (xlog.c registers it via `RegisterTimeout`).
    pub fn startup_progress_timeout_handler()
);

seam_core::seam!(
    /// Read the `log_startup_progress_interval` GUC (milliseconds between
    /// progress updates; 0 disables), owned by startup.c.
    pub fn log_startup_progress_interval() -> i32
);

seam_core::seam!(
    /// Write the `log_startup_progress_interval` GUC (the GUC machinery's
    /// assignment path for the variable startup.c owns).
    pub fn set_log_startup_progress_interval(value: i32)
);
