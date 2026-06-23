//! Seam declarations for the `port-pgsleep` unit (`src/port/pgsleep.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `void pg_usleep(long microsec)` — sleep for the given number of
    /// microseconds (not interruptible by signals in any guaranteed way).
    /// Never ereports.
    pub fn pg_usleep(microsec: i64)
);
