//! Boundary to the tz engine (backend/timezone: localtime.c/pgtz.c) and to the
//! not-yet-ported timestamp.c/xact endpoints this file's C code calls. Those
//! units are unported; every entry here either panics loudly or reproduces the
//! exact C behavior of an uninitialized backend (NULL `session_timezone`,
//! NULL `zoneabbrevtbl`). When they land, this module becomes direct calls.

use crate::consts::{fsec_t, pg_tm, DateTimeErrorExtra, DateTkn};

/// C `pg_tz`. Uninhabited until the tz engine is ported: no value can exist,
/// so any code path that would consult a zone is unreachable by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgTz {}

/// C global `session_timezone` (NULL before `pg_timezone_initialize`).
#[inline]
pub fn session_timezone() -> Option<&'static PgTz> {
    None
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("backend-timezone unit not ported: {what} (adt_datetime tz boundary)");
}

pub fn pg_tzset(_name: &[u8]) -> Option<&'static PgTz> {
    unported("pg_tzset");
}

#[allow(non_snake_case)]
pub fn DetermineTimeZoneOffset(_tm: &mut pg_tm, tzp: &PgTz) -> i32 {
    match *tzp {}
}

#[allow(non_snake_case)]
pub fn DetermineTimeZoneAbbrevOffset(_tm: &mut pg_tm, _abbr: &[u8], tzp: &PgTz) -> i32 {
    match *tzp {}
}

#[allow(non_snake_case)]
pub fn pg_get_timezone_offset(tzp: &PgTz, _gmtoff: &mut i64) -> bool {
    match *tzp {}
}

/// C `TimeZoneAbbrevIsKnown` probe of `session_timezone`; with no session zone
/// there is nothing to consult (C skips the probe when the global is NULL).
pub fn session_tz_abbrev_probe(_lowtoken: &[u8]) -> Option<(bool, i32, i32)> {
    debug_assert!(session_timezone().is_none());
    None
}

/// C `zoneabbrevtbl` (installed by the `timezone_abbreviations` GUC via
/// `InstallTimeZoneAbbrevs`; NULL until then).
pub struct ZoneAbbrevTable {
    pub abbrevs: &'static [DateTkn],
}

#[inline]
pub fn zoneabbrevtbl() -> Option<&'static ZoneAbbrevTable> {
    None
}

#[allow(non_snake_case)]
pub fn FetchDynamicTimeZone(
    _tbl: &ZoneAbbrevTable,
    _tp: &DateTkn,
    _extra: &mut DateTimeErrorExtra<'_>,
) -> Option<&'static PgTz> {
    unported("FetchDynamicTimeZone");
}

/// C `GetCurrentDateTime` (needs timestamp2tm + session_timezone).
/// Port requirement when implemented: the `cache_ts`/`cache_timezone`
/// per-backend memo in C's GetCurrentTimeUsec, not a recompute per call.
#[allow(non_snake_case)]
pub fn GetCurrentDateTime(_tm: &mut pg_tm) {
    unported("GetCurrentDateTime (timestamp2tm/xact)");
}

#[allow(non_snake_case)]
pub fn GetCurrentTimeUsec(_tm: &mut pg_tm, _fsec: &mut fsec_t, _tzp: Option<&mut i32>) {
    unported("GetCurrentTimeUsec (timestamp2tm/xact)");
}
