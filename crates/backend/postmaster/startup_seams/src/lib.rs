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
