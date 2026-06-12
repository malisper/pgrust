//! Seam declarations for `src/port/pgsleep.c` (catalog `port-batch*` units).
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `pg_usleep(microsec)` — sleep the given number of microseconds; may
    /// return early if a signal is caught.
    pub fn pg_usleep(microsec: i64)
);
