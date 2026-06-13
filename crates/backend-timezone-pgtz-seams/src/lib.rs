//! Seam declarations for the `backend-timezone-pgtz` unit
//! (`src/timezone/pgtz.c` + `localtime.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `pg_localtime(&t, tz)` (`timezone/localtime.c`) — broken-down local
    /// time in the given timezone. The timezone is passed explicitly (no
    /// ambient `log_timezone`/`session_timezone` binding in the seam shape);
    /// callers read the timezone off their own config/state. `None` mirrors
    /// the C NULL return (C callers dereference without checking; treat
    /// `None` as a loud failure).
    pub fn pg_localtime(
        t: types_core::pg_time_t,
        tz: &types_pgtime::pg_tz
    ) -> Option<types_pgtime::pg_tm>
);
