//! Seam declarations for the `src/port/pqsignal.c` unit (the modern,
//! backend/frontend reliable-signal installer — distinct from both
//! `backend/libpq/pqsignal.c` (mask setup, `crates/backend-libpq-pqsignal`)
//! and the frozen `interfaces/libpq/legacy-pqsignal.c` shim).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_signal::SigDisposition;

seam_core::seam!(
    /// `void pqsignal(int signo, pqsigfunc func)` (`src/port/pqsignal.c`,
    /// compiled as `pqsignal_be` in the backend) — install `func` as the
    /// handler for `signo` via `sigaction(2)` with `SA_RESTART` (modern
    /// semantics: including for `SIGALRM`). Since PostgreSQL 18 this returns
    /// nothing; only the frozen `interfaces/libpq/legacy-pqsignal.c` shim
    /// still returns the previous disposition.
    pub fn pqsignal(signo: i32, func: SigDisposition)
);
