//! Seam declarations for the `src/port/pqsignal.c` unit (the modern,
//! backend/frontend reliable-signal installer — distinct from both
//! `backend/libpq/pqsignal.c` (mask setup, `crates/backend-libpq-pqsignal`)
//! and the frozen `interfaces/libpq/legacy-pqsignal.c` shim).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_signal::SigDisposition;

seam_core::seam!(
    /// `pqsigfunc pqsignal(int signo, pqsigfunc func)` (`src/port/pqsignal.c`)
    /// — install `func` as the handler for `signo` via `sigaction(2)` with
    /// `SA_RESTART` (modern semantics: including for `SIGALRM`), returning
    /// the previous disposition (`SigDisposition::Error` = C's `SIG_ERR`).
    pub fn pqsignal(signo: i32, func: SigDisposition) -> SigDisposition
);
