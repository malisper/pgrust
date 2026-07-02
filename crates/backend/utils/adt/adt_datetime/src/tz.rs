//! Boundary to the tz engine (backend/timezone: localtime.c/pgtz.c) and to the
//! not-yet-ported endpoints this file's C code calls. The engine's builtin GMT
//! arm (localtime.c gmtload/gmtsub — no tzdata files) is live here so the
//! timestamp unit and a `timezone=GMT` session behave exactly as C; every
//! IANA-zone path panics loudly until backend-timezone lands.

use core::cell::Cell;

use crate::consts::{fsec_t, pg_tm, DateTimeErrorExtra, DateTkn, SECS_PER_DAY};

/// C `pg_tz`. Only the builtin GMT zone exists until the tz engine is ported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgTz {
    Gmt,
}

pub static GMT: PgTz = PgTz::Gmt;

thread_local! {
    static SESSION_TIMEZONE: Cell<Option<&'static PgTz>> = const { Cell::new(None) };
}

/// C global `session_timezone` (NULL before `pg_timezone_initialize`).
#[inline]
pub fn session_timezone() -> Option<&'static PgTz> {
    SESSION_TIMEZONE.with(Cell::get)
}

pub fn set_session_timezone(tz: Option<&'static PgTz>) {
    SESSION_TIMEZONE.with(|c| c.set(tz));
}

/// `pg_timezone_initialize` (pgtz.c): the GUC boot value is "GMT".
pub fn pg_timezone_initialize() {
    set_session_timezone(pg_tzset(b"GMT"));
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("backend-timezone unit not ported: {what} (adt_datetime tz boundary)");
}

pub fn pg_tzset(name: &[u8]) -> Option<&'static PgTz> {
    if name.eq_ignore_ascii_case(b"gmt") {
        return Some(&GMT);
    }
    unported("pg_tzset (non-GMT zone)");
}

#[allow(non_snake_case)]
pub fn DetermineTimeZoneOffset(tm: &mut pg_tm, tzp: &PgTz) -> i32 {
    match *tzp {
        PgTz::Gmt => {
            tm.tm_isdst = 0;
            0
        }
    }
}

#[allow(non_snake_case)]
pub fn DetermineTimeZoneAbbrevOffset(_tm: &mut pg_tm, _abbr: &[u8], tzp: &PgTz) -> i32 {
    match *tzp {
        PgTz::Gmt => unported("DetermineTimeZoneAbbrevOffset"),
    }
}

#[allow(non_snake_case)]
pub fn pg_get_timezone_offset(tzp: &PgTz, gmtoff: &mut i64) -> bool {
    match *tzp {
        PgTz::Gmt => {
            *gmtoff = 0;
            true
        }
    }
}

/// C `TimeZoneAbbrevIsKnown` probe of `session_timezone`; returns
/// (isfixed, offset, isdst) with the sign convention of zoneabbrevtbl's
/// caller (C flips once more at the call site — net zero for GMT).
pub fn session_tz_abbrev_probe(lowtoken: &[u8]) -> Option<(bool, i32, i32)> {
    match session_timezone()? {
        PgTz::Gmt => lowtoken.eq_ignore_ascii_case(b"gmt").then_some((true, 0, 0)),
    }
}

/// POSIX-convention broken-down time (localtime.c `struct pg_tm` semantics):
/// tm_year is year-1900, tm_mon is 0-based — unlike the datetime-convention
/// `pg_tm` elsewhere in this crate. Converted at the timestamp2tm boundary.
#[allow(non_snake_case)]
pub fn pg_localtime(t: i64, tzp: &PgTz) -> Option<pg_tm> {
    match *tzp {
        PgTz::Gmt => Some(gmtsub(t)),
    }
}

pub fn pg_gmtime(t: i64) -> Option<pg_tm> {
    Some(gmtsub(t))
}

// localtime.c gmtsub/timesub for the zero-offset zone.
fn gmtsub(t: i64) -> pg_tm {
    let days = t.div_euclid(SECS_PER_DAY as i64);
    let rem = t.rem_euclid(SECS_PER_DAY as i64);

    // civil-from-days, exact over the full Julian range.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };

    pg_tm {
        tm_sec: (rem % 60) as i32,
        tm_min: ((rem / 60) % 60) as i32,
        tm_hour: (rem / 3600) as i32,
        tm_mday: d as i32,
        tm_mon: (m - 1) as i32,
        tm_year: (year - 1900) as i32,
        tm_wday: (days + 4).rem_euclid(7) as i32,
        tm_yday: 0,
        tm_isdst: 0,
        tm_gmtoff: 0,
        tm_zone: Some("GMT"),
    }
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
