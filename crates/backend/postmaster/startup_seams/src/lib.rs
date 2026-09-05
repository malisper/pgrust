seam_core::seam!(
    // `begin_startup_progress_phase()` (postmaster/startup.c).
    pub fn begin_startup_progress_phase()
);

seam_core::seam!(
    // RegisterTimeout(STARTUP_PROGRESS_TIMEOUT, startup_progress_timeout_handler)
    // (xlog.c StartupXLOG registers; startup.c owns the handler).
    pub fn register_startup_progress_timeout()
);

seam_core::seam!(
    // ProcessStartupProcInterrupts() (postmaster/startup.c): the redo loop's
    // per-record interrupt poll.
    pub fn process_startup_proc_interrupts() -> types_error::PgResult<()>
);

seam_core::seam!(
    // IsPromoteSignaled() (postmaster/startup.c).
    pub fn is_promote_signaled() -> bool
);

seam_core::seam!(
    // ResetPromoteSignaled() (postmaster/startup.c).
    pub fn reset_promote_signaled()
);

seam_core::seam!(
    // disable_startup_progress_timeout() (postmaster/startup.c): standby mode
    // suppresses redo-progress reporting.
    pub fn disable_startup_progress_timeout()
);

seam_core::seam!(
    // has_startup_progress_timeout_expired(&secs, &usecs) (postmaster/startup.c):
    // Some((secs, usecs)) once per expiry of the phase's progress timer — the
    // ereport_startup_progress() gate.
    pub fn has_startup_progress_timeout_expired() -> Option<(i64, i32)>
);

seam_core::seam!(
    // PreRestoreCommand() (postmaster/startup.c).
    pub fn pre_restore_command()
);

seam_core::seam!(
    // PostRestoreCommand() (postmaster/startup.c).
    pub fn post_restore_command()
);
