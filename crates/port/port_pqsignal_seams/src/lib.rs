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
    pub fn pqsignal(signo: i32, func: signal::SigHandler)
);
