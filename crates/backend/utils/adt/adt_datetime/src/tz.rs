//! Boundary to the tz engine (backend/timezone: localtime/pgtz crates) plus
//! the datetime.c helpers that sit directly on it (DetermineTimeZoneOffset
//! family, TimeZoneAbbrevIsKnown). The engine's PgTm is POSIX-convention
//! (tm_year-1900, 0-based tm_mon) and pg_localtime/pg_gmtime results keep
//! that convention, exactly as C's callers expect; timestamp2tm converts.

use core::cell::Cell;
use core::sync::atomic::{AtomicPtr, Ordering};

use crate::calendar::date2j;
use crate::consts::{
    fsec_t, pg_tm, DateTimeErrorExtra, DateTkn, DTZ, DYNTZ, IS_VALID_JULIAN, MINS_PER_HOUR,
    SECS_PER_DAY, SECS_PER_MINUTE, TOKMAXLEN, TZ, UNIX_EPOCH_JDATE,
};

pub use localtime::PgTz;
pub use pgtz::{
    log_timezone, pg_timezone_initialize, pg_tzset, pg_tzset_offset, session_timezone,
    set_log_timezone, set_session_timezone,
};

use localtime::{NextDstBoundary, TZ_STRLEN_MAX};

pub fn pg_tz_acceptable(tz: &PgTz) -> bool {
    localtime::pg_tz_acceptable(tz)
}

pub fn pg_get_timezone_name(tz: &'static PgTz) -> Option<&'static str> {
    core::str::from_utf8(localtime::pg_get_timezone_name(tz)).ok()
}

/// datetime.c `DetermineTimeZoneOffset`: GMT offset and DST status for the
/// datetime-convention y/m/d/h/m/s in `tm` under `tzp`; sets tm_isdst.
/// Out-of-range dates yield offset 0 / isdst 0 (no error here).
#[allow(non_snake_case)]
pub fn DetermineTimeZoneOffset(tm: &mut pg_tm, tzp: &PgTz) -> i32 {
    determine_time_zone_offset_internal(tm, tzp).0
}

// datetime.c DetermineTimeZoneOffsetInternal: also returns the UTC time
// imputed to the date/time (0 on overflow). DST boundaries assumed >= 48
// hours apart, zone offsets < 24h, so back up 24h and find the next boundary.
fn determine_time_zone_offset_internal(tm: &mut pg_tm, tzp: &PgTz) -> (i32, i64) {
    #[cold]
    fn overflow(tm: &mut pg_tm) -> (i32, i64) {
        // Given date is out of range, so assume UTC.
        tm.tm_isdst = 0;
        (0, 0)
    }

    if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        return overflow(tm);
    }
    let date = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - UNIX_EPOCH_JDATE;

    let Some(day) = (date as i64).checked_mul(SECS_PER_DAY as i64) else {
        return overflow(tm);
    };
    let sec = tm.tm_sec as i64
        + (tm.tm_min as i64 + tm.tm_hour as i64 * MINS_PER_HOUR as i64) * SECS_PER_MINUTE as i64;
    let mytime = day.wrapping_add(sec);
    // since sec >= 0, overflow could only be from +day to -mytime
    if mytime < 0 && day > 0 {
        return overflow(tm);
    }

    let prevtime = mytime.wrapping_sub(SECS_PER_DAY as i64);
    if mytime < 0 && prevtime > 0 {
        return overflow(tm);
    }

    let b = match localtime::pg_next_dst_boundary(prevtime, tzp) {
        NextDstBoundary::Overflow => return overflow(tm),
        NextDstBoundary::NoTransition {
            before_gmtoff,
            before_isdst,
        } => {
            // Non-DST zone, life is simple.
            tm.tm_isdst = before_isdst;
            return (-(before_gmtoff as i32), mytime - before_gmtoff);
        }
        NextDstBoundary::Boundary(b) => b,
    };

    let beforetime = mytime.wrapping_sub(b.before_gmtoff);
    if (b.before_gmtoff > 0 && mytime < 0 && beforetime > 0)
        || (b.before_gmtoff <= 0 && mytime > 0 && beforetime < 0)
    {
        return overflow(tm);
    }
    let aftertime = mytime.wrapping_sub(b.after_gmtoff);
    if (b.after_gmtoff > 0 && mytime < 0 && aftertime > 0)
        || (b.after_gmtoff <= 0 && mytime > 0 && aftertime < 0)
    {
        return overflow(tm);
    }

    // The boundary instant itself counts as after the transition.
    if beforetime < b.boundary && aftertime < b.boundary {
        tm.tm_isdst = b.before_isdst;
        return (-(b.before_gmtoff as i32), beforetime);
    }
    if beforetime > b.boundary && aftertime >= b.boundary {
        tm.tm_isdst = b.after_isdst;
        return (-(b.after_gmtoff as i32), aftertime);
    }

    // Invalid or ambiguous time at a transition: spring-forward prefers the
    // "before" interpretation, fall-back prefers "after" (not "standard
    // time" — Europe/Moscow Oct 2014, Europe/Dublin).
    if beforetime > aftertime {
        tm.tm_isdst = b.before_isdst;
        return (-(b.before_gmtoff as i32), beforetime);
    }
    tm.tm_isdst = b.after_isdst;
    (-(b.after_gmtoff as i32), aftertime)
}

fn upcase_abbrev(abbr: &[u8]) -> ([u8; TZ_STRLEN_MAX + 1], usize) {
    let mut up = [0u8; TZ_STRLEN_MAX + 1];
    let n = abbr.len().min(TZ_STRLEN_MAX);
    for (dst, src) in up.iter_mut().zip(abbr[..n].iter()) {
        *dst = src.to_ascii_uppercase();
    }
    (up, n)
}

/// datetime.c `DetermineTimeZoneAbbrevOffset`: offset/DST flag for a dynamic
/// abbreviation at the local time in `tm`; a std/dst abbreviation forces its
/// own offset even when the zone was then in the other mode; otherwise falls
/// back to DetermineTimeZoneOffset's answers.
#[allow(non_snake_case)]
pub fn DetermineTimeZoneAbbrevOffset(tm: &mut pg_tm, abbr: &[u8], tzp: &PgTz) -> i32 {
    let (zone_offset, t) = determine_time_zone_offset_internal(tm, tzp);

    let (up, n) = upcase_abbrev(abbr);
    if let Some((gmtoff, isdst)) = localtime::pg_interpret_timezone_abbrev(&up[..n], t, tzp) {
        tm.tm_isdst = isdst;
        // Change sign to agree with DetermineTimeZoneOffset().
        return -(gmtoff as i32);
    }

    zone_offset
}

#[allow(non_snake_case)]
pub fn pg_get_timezone_offset(tzp: &PgTz, gmtoff: &mut i64) -> bool {
    match localtime::pg_get_timezone_offset(tzp) {
        Some(off) => {
            *gmtoff = off;
            true
        }
        None => false,
    }
}

/// datetime.c `TimeZoneAbbrevIsKnown` probe of `session_timezone`: returns
/// (isfixed, offset, isdst) with the sign flipped to the
/// DetermineTimeZoneOffset convention (the caller flips once more to match
/// zoneabbrevtbl's convention).
pub fn session_tz_abbrev_probe(lowtoken: &[u8]) -> Option<(bool, i32, i32)> {
    let tz = session_timezone()?;
    let (up, n) = upcase_abbrev(lowtoken);
    let (isfixed, gmtoff, isdst) = localtime::pg_timezone_abbrev_is_known(&up[..n], tz)?;
    Some((isfixed, -(gmtoff as i32), isdst))
}

fn convert(tx: localtime::PgTm<'static>) -> pg_tm {
    pg_tm {
        tm_sec: tx.tm_sec,
        tm_min: tx.tm_min,
        tm_hour: tx.tm_hour,
        tm_mday: tx.tm_mday,
        tm_mon: tx.tm_mon,
        tm_year: tx.tm_year,
        tm_wday: tx.tm_wday,
        tm_yday: tx.tm_yday,
        tm_isdst: tx.tm_isdst,
        tm_gmtoff: tx.tm_gmtoff,
        tm_zone: tx.tm_zone,
    }
}

/// POSIX-convention result (see the engine's PgTm doc): tm_year is year-1900,
/// tm_mon 0-based — converted at the timestamp2tm boundary, not here.
#[allow(non_snake_case)]
pub fn pg_localtime(t: i64, tzp: &'static PgTz) -> Option<pg_tm> {
    localtime::pg_localtime(t, tzp).map(convert)
}

pub fn pg_gmtime(t: i64) -> Option<pg_tm> {
    localtime::pg_gmtime(t).map(convert)
}

/// tzparser's tzEntry, minus the source-location fields ConvertTimeZoneAbbrevs
/// ignores. `abbrev` must be downcased and the slice sorted by strcmp order.
pub struct TzEntry<'a> {
    pub abbrev: &'a [u8],
    pub zone: Option<&'a [u8]>,
    pub offset: i32,
    pub is_dst: bool,
}

pub struct DynamicZoneAbbrev {
    tz: AtomicPtr<PgTz>,
    zone: &'static [u8],
}

/// C `zoneabbrevtbl` (installed by the `timezone_abbreviations` GUC via
/// `InstallTimeZoneAbbrevs`; NULL until then). DYNTZ tokens hold an index
/// into `dynamic` (C: a byte offset into the same guc_malloc chunk).
pub struct ZoneAbbrevTable {
    pub abbrevs: &'static [DateTkn],
    dynamic: &'static [DynamicZoneAbbrev],
}

thread_local! {
    static ZONEABBREVTBL: Cell<Option<&'static ZoneAbbrevTable>> = const { Cell::new(None) };
}

#[inline]
pub fn zoneabbrevtbl() -> Option<&'static ZoneAbbrevTable> {
    ZONEABBREVTBL.with(Cell::get)
}

// DIVERGENCE: C guc_mallocs one chunk freed with the superseded GUC extra;
// here the table leaks (cold, once per SET of timezone_abbreviations —
// pgtz's permanent-entry precedent).
#[allow(non_snake_case)]
pub fn ConvertTimeZoneAbbrevs(abbrevs: &[TzEntry<'_>]) -> &'static ZoneAbbrevTable {
    let mut tokens: Vec<DateTkn> = Vec::with_capacity(abbrevs.len());
    let mut dynamic: Vec<DynamicZoneAbbrev> = Vec::new();
    for abbr in abbrevs {
        let mut token = [0u8; TOKMAXLEN + 1];
        let n = abbr.abbrev.len().min(TOKMAXLEN);
        token[..n].copy_from_slice(&abbr.abbrev[..n]);
        let (typ, value) = match abbr.zone {
            Some(zone) => {
                dynamic.push(DynamicZoneAbbrev {
                    tz: AtomicPtr::new(core::ptr::null_mut()),
                    zone: Box::leak(zone.to_vec().into_boxed_slice()),
                });
                (DYNTZ, (dynamic.len() - 1) as i32)
            }
            None => (if abbr.is_dst { DTZ } else { TZ }, abbr.offset),
        };
        tokens.push(DateTkn { token, typ: typ as i8, value });
    }
    debug_assert!(crate::decode::CheckDateTokenTable(&tokens));
    Box::leak(Box::new(ZoneAbbrevTable {
        abbrevs: Box::leak(tokens.into_boxed_slice()),
        dynamic: Box::leak(dynamic.into_boxed_slice()),
    }))
}

#[allow(non_snake_case)]
pub fn InstallTimeZoneAbbrevs(tbl: &'static ZoneAbbrevTable) {
    ZONEABBREVTBL.with(|c| c.set(Some(tbl)));
    crate::decode::ClearTimeZoneAbbrevCache();
}

#[allow(non_snake_case)]
pub fn FetchDynamicTimeZone<'a>(
    tbl: &'a ZoneAbbrevTable,
    tp: &'a DateTkn,
    extra: &mut DateTimeErrorExtra<'a>,
) -> Option<&'static PgTz> {
    debug_assert_eq!(tp.typ as i32, DYNTZ);
    let dtza = &tbl.dynamic[tp.value as usize];
    // Acquire/Release: the table is process-shared, so the pointee's bytes
    // (built by whichever thread resolved the zone first) must be published
    // with the pointer. pg_tzset's entries are process-permanent, which is
    // what makes caching the pointer here sound at all.
    let cached = dtza.tz.load(Ordering::Acquire);
    if !cached.is_null() {
        // SAFETY: the slot only ever holds &'static PgTz from pg_tzset below.
        return Some(unsafe { &*cached });
    }
    match pg_tzset(dtza.zone) {
        Some(tz) => {
            dtza.tz.store(tz as *const PgTz as *mut PgTz, Ordering::Release);
            Some(tz)
        }
        None => {
            extra.dtee_timezone = Some(dtza.zone);
            extra.dtee_abbrev = Some(tp.token_bytes());
            None
        }
    }
}

/// C `pg_interpret_timezone_abbrev` with the upcase C's callers apply first
/// (DetermineTimeZoneAbbrevOffsetTS needs it from adt_timestamp).
pub fn interpret_timezone_abbrev_at(abbr: &[u8], t: i64, tzp: &PgTz) -> Option<(i64, i32)> {
    let (up, n) = upcase_abbrev(abbr);
    localtime::pg_interpret_timezone_abbrev(&up[..n], t, tzp)
}

fn from_snapshot(s: &timestamp_seams::CurrentTimeUsec, tm: &mut pg_tm) {
    tm.tm_sec = s.tm_sec;
    tm.tm_min = s.tm_min;
    tm.tm_hour = s.tm_hour;
    tm.tm_mday = s.tm_mday;
    tm.tm_mon = s.tm_mon;
    tm.tm_year = s.tm_year;
    tm.tm_wday = s.tm_wday;
    tm.tm_yday = s.tm_yday;
    tm.tm_isdst = s.tm_isdst;
    tm.tm_gmtoff = s.tm_gmtoff;
    tm.tm_zone = s.tm_zone;
}

// DIVERGENCE: C ereports "timestamp out of range" (22008); the callers here
// (DecodeDateTime and friends) return dterr codes and cannot carry a PgError,
// so an out-of-range transaction timestamp panics instead.
#[allow(non_snake_case)]
pub fn GetCurrentDateTime(tm: &mut pg_tm) {
    let s = timestamp_seams::get_current_datetime::call().expect("timestamp out of range");
    from_snapshot(&s, tm);
}

#[allow(non_snake_case)]
pub fn GetCurrentTimeUsec(tm: &mut pg_tm, fsec: &mut fsec_t, tzp: Option<&mut i32>) {
    let s = timestamp_seams::get_current_time_usec::call().expect("timestamp out of range");
    from_snapshot(&s, tm);
    *fsec = s.fsec;
    if let Some(tzp) = tzp {
        *tzp = s.tz;
    }
}
