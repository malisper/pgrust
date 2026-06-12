//! Idiomatic Rust port of PostgreSQL's `src/timezone/localtime.c`: the IANA
//! TZif parser, POSIX `TZ` string parser, and the timezone conversion /
//! DST-boundary / abbreviation-lookup API (`pg_localtime`, `pg_gmtime`,
//! `pg_next_dst_boundary`, ...).
//!
//! Timezone loading from disk (`pg_open_tzfile`) belongs to the
//! `backend-timezone-pgtz` unit and is reached through its seam crate.
//! The shared `pg_tm`/`pg_tz`/`state` vocabulary (pgtime.h/pgtz.h) lives in
//! the `types-pgtime` crate and is re-exported here for convenience;
//! `tzload`/`tzparse` are exported for the pgtz unit, which owns `pg_tzset`
//! and the timezone cache.

mod localtime;

pub use localtime::{
    pg_get_next_timezone_abbrev, pg_get_timezone_name, pg_get_timezone_offset, pg_gmtime,
    pg_interpret_timezone_abbrev, pg_localtime, pg_next_dst_boundary,
    pg_next_dst_boundary_tristate, pg_time_t, pg_timezone_abbrev_is_known, pg_tm, pg_tz,
    pg_tz_acceptable, state, tzload, tzparse, DstBoundary, KnownTimezoneAbbrev, NextDstBoundary,
    TimezoneAbbrev, TzLoadError, TZ_STRLEN_MAX,
};
pub use types_pgtime::{lsinfo, ttinfo};

/// This crate has no inward seam crate yet (it sits at the bottom of the
/// timezone dependency chain; consumers depend on it directly).
pub fn init_seams() {}
