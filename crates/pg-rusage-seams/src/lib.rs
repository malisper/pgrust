//! Seams for the resource-usage measurement routines
//! (`src/backend/utils/misc/pg_rusage.c`).
//!
//! Both functions are infallible in C (`pg_rusage_show` formats into a
//! `static char[100]` — no palloc, no ereport), so the seams return bare
//! values. The start-of-measurement snapshot is the caller's own value: the
//! consumer holds the [`PgRUsage`] returned by [`pg_rusage_init`] and passes it
//! back to [`pg_rusage_show`] to format the elapsed delta.

use types_rusage::PgRUsage;

seam_core::seam!(
    /// `pg_rusage_init(&ru0)` — capture the start-of-measurement resource-usage
    /// snapshot and return it to the caller.
    pub fn pg_rusage_init() -> PgRUsage
);

seam_core::seam!(
    /// `pg_rusage_show(&ru0)` — capture a fresh snapshot and format the elapsed
    /// user/system/wall deltas since `ru0` as the "CPU: ..." string.
    pub fn pg_rusage_show(ru0: PgRUsage) -> String
);
