//! Seam declarations for the signal-mask / crash-exit handler setup
//! `InitPostmasterChild` / `InitStandaloneProcess` perform
//! (`SignalHandlerForCrashExit` in `postmaster/interrupt.c`, `pqinitmask` /
//! `BlockSig` in `libpq/pqsignal.c`). Calls panic until the owners land.

seam_core::seam!(
    /// Postmaster-child SIGQUIT setup (`miscinit.c:152-155`): install
    /// `SignalHandlerForCrashExit` for SIGQUIT, remove SIGQUIT from `BlockSig`,
    /// and `sigprocmask(SIG_SETMASK, &BlockSig, NULL)`.
    pub fn install_crash_exit_sigquit_handler() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// Standalone signal-mask setup (`miscinit.c:199-200`): `pqinitmask()` then
    /// `sigprocmask(SIG_SETMASK, &BlockSig, NULL)` (no SIGQUIT unblock).
    pub fn pqinitmask_set_blocksig() -> types_error::PgResult<()>
);
