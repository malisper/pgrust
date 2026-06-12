//! Seam declarations for the `backend-timezone-pgtz` unit
//! (`src/timezone/pgtz.c`), which owns the `log_timezone` /
//! `session_timezone` globals.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `pg_localtime(&t, log_timezone)` (`timezone/localtime.c` +
    /// the `log_timezone` global from `pgtz.c`) — broken-down local time in
    /// the log timezone. `None` mirrors the C NULL return (C callers
    /// dereference without checking; treat `None` as a loud failure). The
    /// global is resolved by the owner, same pattern as
    /// `set_latch_my_latch`.
    pub fn pg_localtime_log_timezone(
        t: types_core::pg_time_t
    ) -> Option<types_pgtime::pg_tm>
);
