//! Seam declarations for the `src/port/pqsignal.c` unit (the backend's
//! reliable-signal `pqsignal()` wrapper; catalog rows `port-batch21+`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `void pqsignal(int signo, pqsigfunc func)` (`src/port/pqsignal.c`,
    /// symbol `pqsignal_be`) — install a signal handler via `sigaction` with
    /// `SA_RESTART` (plus `SA_NOCLDSTOP` for `SIGCHLD`). The backend variant
    /// returns `void` (unlike the legacy libpq `pqsignal`, which reports the
    /// previous disposition); a failing `sigaction(2)` is a coding error
    /// (`Assert(false)` in C), not an ereport, so the seam is infallible.
    pub fn pqsignal(signo: i32, func: types_signal::SigHandler)
);

seam_core::seam!(
    /// The `BackgroundWorkerMain` signal-handler block: install
    /// `StatementCancelHandler`/`procsignal_sigusr1_handler`/
    /// `FloatExceptionHandler` (database-connection workers) or `SIG_IGN`
    /// (others) for SIGINT/SIGUSR1/SIGFPE, the `bgworker_die` SIGTERM handler,
    /// `SIG_IGN` for SIGHUP/SIGPIPE/SIGUSR2, `SIG_DFL` for SIGCHLD, and run
    /// `InitializeTimeouts()`. Composite because the handler fn-pointers and
    /// the timeout manager are owned by other subsystems; `db_connection`
    /// selects the connection-handler variant.
    pub fn install_bgworker_signal_handlers(db_connection: bool)
);

seam_core::seam!(
    /// `sigprocmask(SIG_SETMASK, &BlockSig, NULL)` — block all signals
    /// (`BackgroundWorkerBlockSignals`). `BlockSig` is owned by the signal
    /// setup; infallible.
    pub fn block_signals()
);

seam_core::seam!(
    /// `sigprocmask(SIG_SETMASK, &UnBlockSig, NULL)` — restore the normal
    /// signal mask (`BackgroundWorkerUnblockSignals`). Infallible.
    pub fn unblock_signals()
);
