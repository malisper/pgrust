//! Seam declarations for the `port-pgsleep` unit (`src/port/pgsleep.c`).
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `pg_usleep(microsec)` — sleep for the given number of microseconds.
    /// Infallible in C (no `ereport` path); a signal may shorten the sleep.
    pub fn pg_usleep(microsec: i64)
);
