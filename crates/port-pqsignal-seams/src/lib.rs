//! Seam declarations for the `src/port/pqsignal.c` unit (the backend's
//! reliable-signal `pqsignal()` wrapper; catalog rows `port-batch21+`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_signal::SigDisposition;

seam_core::seam!(
    /// `pqsignal(signo, func)` (`src/port/pqsignal.c`) — install a signal
    /// handler via `sigaction` with `SA_RESTART` (plus `SA_NOCLDSTOP` for
    /// `SIGCHLD`); returns the previous disposition, or
    /// `SigDisposition::Error` (C `SIG_ERR`) on failure. Infallible at the
    /// ereport level.
    pub fn pqsignal(
        signo: i32,
        func: types_signal::SigHandler
    ) -> types_signal::SigDisposition
);
