//! Seam declarations for the `backend-timezone-pgtz` unit
//! (`src/timezone/pgtz.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// C `pg_open_tzfile(name, canonname)` (pgtz.c): open a timezone data
    /// file under the server's timezone directory, searching directory levels
    /// case-insensitively when an exact open fails. `want_canonical` mirrors a
    /// non-NULL `canonname` out-buffer: when true, the canonical
    /// (case-corrected) spelling of `name` is returned alongside the open
    /// file. `None` mirrors C's `-1` return (file not found / not openable);
    /// the C function never `ereport`s.
    pub fn pg_open_tzfile(name: &str, want_canonical: bool) -> Option<(std::fs::File, Option<String>)>
);
