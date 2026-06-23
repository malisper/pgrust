//! Datetime SQL entry-point cores: `datetime_to_char_body`, the
//! `to_char(timestamp/timestamptz/interval)` producers, and the
//! `to_timestamp` / `to_date` / `parse_datetime` / `do_to_timestamp` consumers.
//!
//! Faithful idiomatic port of formatting.c:3942-4867 (PG 18.3). These are the
//! *computational cores* of the SQL functions; the fmgr `Datum`/varlena
//! wrapping is the seamed boundary (callers pass broken-down values + a format
//! byte slice and receive owned text bytes / typed values, per the project
//! systemic deferral).
//!
//! ## Seams
//!
//! The broken-down-time / timezone / calendar conversions live in the datetime
//! and timezone sibling subsystems (separate ports); each routes through the
//! centralized `seams::formatting` slots: `timestamp2tm`,
//! `tm2timestamp`, `interval2itm`, `tm2time`, `tm2timetz`,
//! `adjust_timestamp_for_typmod`, `adjust_time_for_typmod`,
//! `determine_time_zone_offset`, `determine_time_zone_abbrev_offset`, `j2date`,
//! `isoweek2date`, `isoweekdate2date`, `isoweek2j`, `validate_date`, plus the
//! `date2j` calendar seam (shared with the DCH producer). `DateTimeParseError`
//! (the DTERR -> ereport/errsave mapper) is pure logic and is ported in-crate.

use ::mcx::Mcx;
use types_error::{PgError, PgResult, SoftErrorContext};
use ::pgtime::pg_tm;
use types_datetime::{TzHandle, YmdDate};
use types_datetime::{
    DateTimeErrorExtra, Interval, TimeTzADT, DAYS_PER_MONTH, DTERR_BAD_FORMAT, DTERR_BAD_TIMEZONE,
    DTERR_BAD_ZONE_ABBREV, DTERR_FIELD_OVERFLOW, DTERR_INTERVAL_OVERFLOW, DTERR_MD_FIELD_OVERFLOW,
    DTERR_TZDISP_OVERFLOW, HOURS_PER_DAY, MAX_TZDISP_HOUR, MINS_PER_HOUR, MONTHS_PER_YEAR,
    SECS_PER_HOUR, SECS_PER_MINUTE, USECS_PER_SEC,
};
use types_datetime::{
    DATETIME_MIN_JULIAN, DATE_END_JULIAN, JULIAN_MAXMONTH, JULIAN_MAXYEAR, JULIAN_MINMONTH,
    JULIAN_MINYEAR, POSTGRES_EPOCH_JDATE,
};
use types_datetime::{DAY, MONTH, YEAR};
use types_error::{
    ERRCODE_CONFIG_FILE_ERROR, ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
    ERRCODE_INTERVAL_FIELD_OVERFLOW, ERRCODE_INVALID_DATETIME_FORMAT, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
};
use types_datetime::{fsec_t, DateADT, TimeADT, Timestamp};
use ::types_core::Oid;

use crate::cache::dch_cache_fetch;
use crate::dch::{dch_to_char, FmtTm, FmtTz, TmToChar};
use crate::dch_fromchar::{dch_datetime_type, dch_from_char, TmFromChar};
use crate::tables::*;

/// Local `errsave` helper mirroring C's `errsave(escontext, ...)`: routes a
/// complete [`PgError`] through the shared soft-error context discipline.
fn errsave(escontext: Option<&mut SoftErrorContext>, err: PgError) -> PgResult<()> {
    ::types_error::ereturn(escontext, (), err)
}

/// C: `ZERO_tm` (formatting.c) — a zeroed `pg_tm` with `mday`/`mon` set to 1.
fn zero_tm() -> pg_tm {
    pg_tm {
        tm_mday: 1,
        tm_mon: 1,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Seam wrappers (genuine cross-subsystem calls).
// ---------------------------------------------------------------------------

fn date2j(year: i32, month: i32, day: i32) -> i32 {
    adt_datetime::seam_impls::seam_date2j(year, month, day)
}
fn timestamp2tm(
    dt: Timestamp,
    want_tz: bool,
) -> Result<::types_datetime::Timestamp2TmResult, ()> {
    timestamp_seams::timestamp2tm::call(dt, want_tz)
}
fn tm2timestamp(tm: &pg_tm, fsec: fsec_t, tz: Option<i32>) -> Result<Timestamp, ()> {
    timestamp_seams::tm2timestamp::call(tm, fsec, tz)
}
fn interval2itm(span: Interval) -> ::types_datetime::pg_itm {
    timestamp_seams::interval2itm::call(span)
}
fn tm2time(tm: &pg_tm, fsec: fsec_t) -> TimeADT {
    timestamp_seams::tm2time::call(tm, fsec)
}
fn tm2timetz(tm: &pg_tm, fsec: fsec_t, tz: i32) -> TimeTzADT {
    timestamp_seams::tm2timetz::call(tm, fsec, tz)
}
fn adjust_timestamp_for_typmod(value: Timestamp, typmod: i32) -> PgResult<Timestamp> {
    timestamp_seams::adjust_timestamp_for_typmod::call(value, typmod)
}
fn adjust_time_for_typmod(time: TimeADT, typmod: i32) -> TimeADT {
    timestamp_seams::adjust_time_for_typmod::call(time, typmod)
}
fn determine_time_zone_offset(tm: &mut pg_tm) -> i32 {
    adt_datetime::seam_impls::seam_determine_time_zone_offset(tm)
}
fn determine_time_zone_abbrev_offset(tm: &mut pg_tm, abbr: &str, tzp: TzHandle) -> i32 {
    adt_datetime::seam_impls::seam_determine_time_zone_abbrev_offset(tm, abbr, tzp)
}
fn j2date_seam(jd: i32) -> YmdDate {
    adt_datetime::seam_impls::seam_j2date(jd)
}
fn isoweek2date_seam(woy: i32, year: i32) -> YmdDate {
    isoweek_seams::isoweek2date::call(woy, year)
}
fn isoweekdate2date_seam(isoweek: i32, wday: i32, year: i32) -> YmdDate {
    isoweek_seams::isoweekdate2date::call(isoweek, wday, year)
}
fn isoweek2j(year: i32, week: i32) -> i32 {
    isoweek_seams::isoweek2j::call(year, week)
}
fn validate_date(fmask: i32, is2digits: bool, bc: bool, tm: &mut pg_tm) -> i32 {
    adt_datetime::seam_impls::seam_validate_date(fmask, is2digits, bc, tm)
}

// ---------------------------------------------------------------------------
// Infinity helpers (datatype/timestamp.h macros).
// ---------------------------------------------------------------------------

/// C: `TIMESTAMP_NOT_FINITE(t)` — `t == DT_NOBEGIN || t == DT_NOEND`.
#[inline]
fn timestamp_not_finite(t: Timestamp) -> bool {
    t == ::types_datetime::DT_NOBEGIN || t == ::types_datetime::DT_NOEND
}

/// C: `INTERVAL_NOT_FINITE(i)` — true when the interval is ±infinity.
/// `INTERVAL_IS_NOBEGIN` requires all three fields at their int min, and
/// `INTERVAL_IS_NOEND` all three at their int max.
#[inline]
fn interval_not_finite(it: &Interval) -> bool {
    (it.month == i32::MIN && it.day == i32::MIN && it.time == i64::MIN)
        || (it.month == i32::MAX && it.day == i32::MAX && it.time == i64::MAX)
}

// ---------------------------------------------------------------------------
// datetime_to_char_body + the to_char producers.
// ---------------------------------------------------------------------------

/// C: `datetime_to_char_body` (formatting.c:3942). Returns the formatted text
/// bytes.
pub fn datetime_to_char_body<'mcx>(
    mcx: Mcx<'mcx>,
    tmtc: &TmToChar,
    fmt: &[u8],
    is_interval: bool,
    collid: Oid,
) -> PgResult<Vec<u8>> {
    let fmt_len = fmt.len();

    let format = if fmt_len > DCH_CACHE_SIZE {
        // Bigger than the cache: parse directly.
        crate::parse::parse_format(fmt, DCH_KEYWORDS, DCH_SUFF, &DCH_INDEX, DCH_FLAG, None)?
    } else {
        dch_cache_fetch(fmt, false)?
    };

    dch_to_char(mcx, &format, is_interval, tmtc, collid)
}

/// C: `timestamp_to_char` (formatting.c:4011) core. `dt` is the timestamp; the
/// fmt is the picture bytes. Returns `None` for the SQL-NULL cases (empty fmt
/// or non-finite input).
pub fn timestamp_to_char<'mcx>(mcx: Mcx<'mcx>, dt: Timestamp, fmt: &[u8], collid: Oid) -> PgResult<Option<Vec<u8>>> {
    if fmt.is_empty() || timestamp_not_finite(dt) {
        return Ok(None);
    }

    let mut tmtc = TmToChar::zero();

    let mut r = match timestamp2tm(dt, false) {
        Ok(r) => r,
        Err(()) => return Err(timestamp_out_of_range()),
    };

    // calculate wday and yday (timestamp2tm doesn't)
    let thisdate = date2j(r.tm.tm_year, r.tm.tm_mon, r.tm.tm_mday);
    r.tm.tm_wday = (thisdate + 1) % 7;
    r.tm.tm_yday = thisdate - date2j(r.tm.tm_year, 1, 1) + 1;

    copy_tm(&mut tmtc.tm, &r.tm);
    tmtc.fsec = r.fsec;

    Ok(Some(datetime_to_char_body(mcx, &tmtc, fmt, false, collid)?))
}

/// C: `timestamptz_to_char` (formatting.c:4046) core. Returns the formatted
/// text (the zone name is carried internally through `tm.tzn`); `None` for
/// SQL-NULL cases.
pub fn timestamptz_to_char<'mcx>(mcx: Mcx<'mcx>, dt: Timestamp, fmt: &[u8], collid: Oid) -> PgResult<Option<Vec<u8>>> {
    if fmt.is_empty() || timestamp_not_finite(dt) {
        return Ok(None);
    }

    let mut tmtc = TmToChar::zero();

    let mut r = match timestamp2tm(dt, true) {
        Ok(r) => r,
        Err(()) => return Err(timestamp_out_of_range()),
    };

    let thisdate = date2j(r.tm.tm_year, r.tm.tm_mon, r.tm.tm_mday);
    r.tm.tm_wday = (thisdate + 1) % 7;
    r.tm.tm_yday = thisdate - date2j(r.tm.tm_year, 1, 1) + 1;

    let gmtoff = r.tm.tm_gmtoff;
    copy_tm(&mut tmtc.tm, &r.tm);
    tmtc.fsec = r.fsec;
    tmtc.tzn = r.tzn;
    tmtc.tm.tm_gmtoff = gmtoff;

    Ok(Some(datetime_to_char_body(mcx, &tmtc, fmt, false, collid)?))
}

/// C: `interval_to_char` (formatting.c:4087) core.
pub fn interval_to_char<'mcx>(mcx: Mcx<'mcx>, it: &Interval, fmt: &[u8], collid: Oid) -> PgResult<Option<Vec<u8>>> {
    if fmt.is_empty() || interval_not_finite(it) {
        return Ok(None);
    }

    let mut tmtc = TmToChar::zero();
    let itm = interval2itm(*it);
    tmtc.fsec = itm.tm_usec;
    tmtc.tm.tm_sec = itm.tm_sec;
    tmtc.tm.tm_min = itm.tm_min;
    tmtc.tm.tm_hour = itm.tm_hour;
    tmtc.tm.tm_mday = itm.tm_mday;
    tmtc.tm.tm_mon = itm.tm_mon;
    tmtc.tm.tm_year = itm.tm_year;

    // wday meaningless, yday approximates the total span in days.
    tmtc.tm.tm_yday =
        (tmtc.tm.tm_year * MONTHS_PER_YEAR + tmtc.tm.tm_mon) * DAYS_PER_MONTH + tmtc.tm.tm_mday;

    Ok(Some(datetime_to_char_body(mcx, &tmtc, fmt, true, collid)?))
}

// ---------------------------------------------------------------------------
// to_timestamp / to_date consumers.
// ---------------------------------------------------------------------------

/// Output of `to_timestamp`: a `Timestamp` and the resolved tz / typmod info
/// the SQL wrapper needs.
#[derive(Clone, Copy, Debug)]
pub struct ToTimestampResult {
    pub timestamp: Timestamp,
    /// GMT offset if the format/input carried a timezone.
    pub tz: i32,
    pub fprec: i32,
}

/// C: `to_timestamp` (formatting.c:4129) core. Returns the timestamp adjusted
/// for any fractional-precision typmod.
pub fn to_timestamp<'mcx>(mcx: Mcx<'mcx>, date_txt: &[u8], fmt: &[u8], collid: Oid) -> PgResult<ToTimestampResult> {
    let mut tm = zero_tm();
    let mut ftz = FmtTz::default();
    let mut fsec: fsec_t = 0;
    let mut fprec: i32 = 0;

    do_to_timestamp(
        mcx,
        date_txt,
        fmt,
        collid,
        false,
        &mut tm,
        &mut fsec,
        &mut ftz,
        Some(&mut fprec),
        None,
        None,
    )?;

    // Use the specified time zone, if any.
    let tz = if ftz.has_tz {
        ftz.gmtoffset
    } else {
        determine_time_zone_offset(&mut tm)
    };

    let mut result: Timestamp = match tm2timestamp(&tm, fsec, Some(tz)) {
        Ok(v) => v,
        Err(()) => return Err(timestamp_out_of_range()),
    };

    // C: AdjustTimestampForTypmod(&result, fprec, NULL) — a hard error here
    // (escontext is NULL). Propagate its original error
    // (ERRCODE_INVALID_PARAMETER_VALUE "precision must be between") unchanged
    // rather than remapping it to "timestamp out of range".
    if fprec != 0 {
        result = adjust_timestamp_for_typmod(result, fprec)?;
    }

    Ok(ToTimestampResult {
        timestamp: result,
        tz,
        fprec,
    })
}

/// C: `to_date` (formatting.c:4168) core. Returns the `DateADT`.
pub fn to_date<'mcx>(mcx: Mcx<'mcx>, date_txt: &[u8], fmt: &[u8], collid: Oid) -> PgResult<DateADT> {
    let mut tm = zero_tm();
    let mut ftz = FmtTz::default();
    let mut fsec: fsec_t = 0;

    do_to_timestamp(
        mcx,
        date_txt, fmt, collid, false, &mut tm, &mut fsec, &mut ftz, None, None, None,
    )?;

    // Prevent overflow in Julian-day routines.
    if !is_valid_julian(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        return Err(date_out_of_range(date_txt));
    }

    let result = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

    if !is_valid_date(result) {
        return Err(date_out_of_range(date_txt));
    }

    Ok(result)
}

/// The typed result of [`parse_datetime`], discriminated by the date/time/zone
/// components present in the format (C returns a `Datum` + `*typid`/`*typmod`).
#[derive(Clone, Copy, Debug)]
pub enum ParseDatetimeResult {
    Timestamptz { value: Timestamp, typmod: i32 },
    Timestamp { value: Timestamp, typmod: i32 },
    Date(DateADT),
    Timetz { value: TimeTzADT, typmod: i32 },
    Time { value: TimeADT, typmod: i32 },
}

/// C: `parse_datetime` (formatting.c:4216). Converts `date_txt` per `fmt`,
/// determining the result type from the format's components. Returns
/// `Ok(Some(result))` on success, `Ok(None)` if a soft error was recorded in
/// `escontext`, or `Err` on a hard error. `*tz` receives the GMT offset when a
/// timezone component is present.
#[allow(clippy::too_many_arguments)]
pub fn parse_datetime<'mcx>(
    mcx: Mcx<'mcx>,
    date_txt: &[u8],
    fmt: &[u8],
    collid: Oid,
    strict: bool,
    tz: &mut i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<ParseDatetimeResult>> {
    let mut tm = zero_tm();
    let mut ftz = FmtTz::default();
    let mut fsec: fsec_t = 0;
    let mut fprec: i32 = 0;
    let mut flags: u32 = 0;

    if !do_to_timestamp(
        mcx,
        date_txt,
        fmt,
        collid,
        strict,
        &mut tm,
        &mut fsec,
        &mut ftz,
        Some(&mut fprec),
        Some(&mut flags),
        escontext.as_deref_mut(),
    )? {
        return Ok(None);
    }

    let typmod = if fprec != 0 { fprec } else { -1 };

    if flags & DCH_DATED as u32 != 0 {
        if flags & DCH_TIMED as u32 != 0 {
            if flags & DCH_ZONED as u32 != 0 {
                if ftz.has_tz {
                    *tz = ftz.gmtoffset;
                } else {
                    debug_assert!(!strict);
                    errsave(
                        escontext.as_deref_mut(),
                        PgError::error(
                            "missing time zone in input string for type timestamptz".to_string(),
                        )
                        .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT),
                    )?;
                    return Ok(None);
                }
                let mut result: Timestamp = match tm2timestamp(&tm, fsec, Some(*tz)) {
                    Ok(v) => v,
                    Err(()) => {
                        errsave(
                            escontext.as_deref_mut(),
                            PgError::error("timestamptz out of range".to_string())
                                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        )?;
                        return Ok(None);
                    }
                };
                // C: AdjustTimestampForTypmod(&result, *typmod, escontext) — a
                // soft error routed via escontext. The only error path is the
                // ERRCODE_INVALID_PARAMETER_VALUE "precision must be between"
                // check; preserve that error and route it through escontext.
                match adjust_timestamp_for_typmod(result, typmod) {
                    Ok(v) => result = v,
                    Err(e) => {
                        errsave(escontext.as_deref_mut(), e)?;
                        return Ok(None);
                    }
                }
                Ok(Some(ParseDatetimeResult::Timestamptz {
                    value: result,
                    typmod,
                }))
            } else {
                let mut result: Timestamp = match tm2timestamp(&tm, fsec, None) {
                    Ok(v) => v,
                    Err(()) => {
                        errsave(escontext.as_deref_mut(), timestamp_out_of_range())?;
                        return Ok(None);
                    }
                };
                // C: AdjustTimestampForTypmod(&result, *typmod, escontext).
                match adjust_timestamp_for_typmod(result, typmod) {
                    Ok(v) => result = v,
                    Err(e) => {
                        errsave(escontext.as_deref_mut(), e)?;
                        return Ok(None);
                    }
                }
                Ok(Some(ParseDatetimeResult::Timestamp {
                    value: result,
                    typmod,
                }))
            }
        } else if flags & DCH_ZONED as u32 != 0 {
            errsave(
                escontext.as_deref_mut(),
                PgError::error("datetime format is zoned but not timed".to_string())
                    .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT),
            )?;
            Ok(None)
        } else {
            if !is_valid_julian(tm.tm_year, tm.tm_mon, tm.tm_mday) {
                errsave(escontext.as_deref_mut(), date_out_of_range(date_txt))?;
                return Ok(None);
            }
            let result = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;
            if !is_valid_date(result) {
                errsave(escontext.as_deref_mut(), date_out_of_range(date_txt))?;
                return Ok(None);
            }
            Ok(Some(ParseDatetimeResult::Date(result)))
        }
    } else if flags & DCH_TIMED as u32 != 0 {
        if flags & DCH_ZONED as u32 != 0 {
            if ftz.has_tz {
                *tz = ftz.gmtoffset;
            } else {
                debug_assert!(!strict);
                errsave(
                    escontext.as_deref_mut(),
                    PgError::error("missing time zone in input string for type timetz".to_string())
                        .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT),
                )?;
                return Ok(None);
            }
            let mut result = tm2timetz(&tm, fsec, *tz);
            result.time = adjust_time_for_typmod(result.time, typmod);
            Ok(Some(ParseDatetimeResult::Timetz {
                value: result,
                typmod,
            }))
        } else {
            let mut result = tm2time(&tm, fsec);
            result = adjust_time_for_typmod(result, typmod);
            Ok(Some(ParseDatetimeResult::Time {
                value: result,
                typmod,
            }))
        }
    } else {
        errsave(
            escontext,
            PgError::error("datetime format is not dated and not timed".to_string())
                .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT),
        )?;
        Ok(None)
    }
}

/// C: `datetime_format_has_tz` (formatting.c:4379).
pub fn datetime_format_has_tz(fmt_str: &[u8]) -> PgResult<bool> {
    let fmt_len = fmt_str.len();
    let format = if fmt_len > DCH_CACHE_SIZE {
        crate::parse::parse_format(fmt_str, DCH_KEYWORDS, DCH_SUFF, &DCH_INDEX, DCH_FLAG, None)?
    } else {
        dch_cache_fetch(fmt_str, false)?
    };
    let result = dch_datetime_type(&format);
    Ok(result & DCH_ZONED != 0)
}

/// C: `do_to_timestamp` (formatting.c:4442).
///
/// Returns `Ok(true)` on success; `Ok(false)` when a soft error was recorded
/// (escontext); `Err` on a hard error.
#[allow(clippy::too_many_arguments)]
pub fn do_to_timestamp<'mcx>(
    mcx: Mcx<'mcx>,
    date_txt: &[u8],
    fmt: &[u8],
    collid: Oid,
    std: bool,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tz: &mut FmtTz,
    mut fprec: Option<&mut i32>,
    mut flags: Option<&mut u32>,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    let mut tmfc = TmFromChar::default();
    *tm = zero_tm();
    *fsec = 0;
    tz.has_tz = false;
    if let Some(f) = fprec.as_deref_mut() {
        *f = 0;
    }
    if let Some(f) = flags.as_deref_mut() {
        *f = 0;
    }
    let mut fmask: i32 = 0; // bit mask for ValidateDate()

    let date_str = date_txt; // already the data bytes

    let fmt_len = fmt.len();

    if fmt_len != 0 {
        let format = if fmt_len > DCH_CACHE_SIZE {
            crate::parse::parse_format(
                fmt,
                DCH_KEYWORDS,
                DCH_SUFF,
                &DCH_INDEX,
                DCH_FLAG | if std { STD_FLAG } else { 0 },
                None,
            )?
        } else {
            dch_cache_fetch(fmt, std)?
        };

        let ok = dch_from_char(mcx, &format, date_str, &mut tmfc, collid, std, escontext.as_deref_mut())?;
        if !ok {
            return Ok(false);
        }

        if let Some(f) = flags {
            *f = dch_datetime_type(&format) as u32;
        }
    }

    // Convert to_date/to_timestamp input fields to standard 'tm'.
    if tmfc.ssss != 0 {
        let mut x = tmfc.ssss;
        tm.tm_hour = x / SECS_PER_HOUR;
        x %= SECS_PER_HOUR;
        tm.tm_min = x / SECS_PER_MINUTE;
        x %= SECS_PER_MINUTE;
        tm.tm_sec = x;
    }
    if tmfc.ss != 0 {
        tm.tm_sec = tmfc.ss;
    }
    if tmfc.mi != 0 {
        tm.tm_min = tmfc.mi;
    }
    if tmfc.hh != 0 {
        tm.tm_hour = tmfc.hh;
    }

    if tmfc.clock == CLOCK_12_HOUR {
        if tm.tm_hour < 1 || tm.tm_hour > HOURS_PER_DAY / 2 {
            errsave(
                escontext.as_deref_mut(),
                PgError::error(format!(
                    "hour \"{}\" is invalid for the 12-hour clock",
                    tm.tm_hour
                ))
                .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT)
                .with_hint("Use the 24-hour clock, or give an hour between 1 and 12."),
            )?;
            return Ok(false);
        }
        if tmfc.pm != 0 && tm.tm_hour < HOURS_PER_DAY / 2 {
            tm.tm_hour += HOURS_PER_DAY / 2;
        } else if tmfc.pm == 0 && tm.tm_hour == HOURS_PER_DAY / 2 {
            tm.tm_hour = 0;
        }
    }

    if tmfc.year != 0 {
        if tmfc.cc != 0 && tmfc.yysz <= 2 {
            if tmfc.bc != 0 {
                tmfc.cc = -tmfc.cc;
            }
            tm.tm_year = tmfc.year % 100;
            if tm.tm_year != 0 {
                if tmfc.cc >= 0 {
                    // tm->tm_year += (tmfc.cc - 1) * 100;
                    let tmp = tmfc.cc - 1;
                    match tmp.checked_mul(100).and_then(|t| tm.tm_year.checked_add(t)) {
                        Some(v) => tm.tm_year = v,
                        None => {
                            date_time_parse_error(
                                DTERR_FIELD_OVERFLOW,
                                None,
                                &lossy(date_txt),
                                "timestamp",
                                escontext.as_deref_mut(),
                            )?;
                            return Ok(false);
                        }
                    }
                } else {
                    // tm->tm_year = (tmfc.cc + 1) * 100 - tm->tm_year + 1;
                    let tmp = tmfc.cc + 1;
                    match tmp
                        .checked_mul(100)
                        .and_then(|t| t.checked_sub(tm.tm_year))
                        .and_then(|t| t.checked_add(1))
                    {
                        Some(v) => tm.tm_year = v,
                        None => {
                            date_time_parse_error(
                                DTERR_FIELD_OVERFLOW,
                                None,
                                &lossy(date_txt),
                                "timestamp",
                                escontext.as_deref_mut(),
                            )?;
                            return Ok(false);
                        }
                    }
                }
            } else {
                // find century year for dates ending in "00"
                tm.tm_year = tmfc.cc * 100 + (if tmfc.cc >= 0 { 0 } else { 1 });
            }
        } else {
            // 4-digit year provided; use that and ignore CC.
            tm.tm_year = tmfc.year;
            if tmfc.bc != 0 {
                tm.tm_year = -tm.tm_year;
            }
            if tm.tm_year < 0 {
                tm.tm_year += 1;
            }
        }
        fmask |= dtk_m(YEAR);
    } else if tmfc.cc != 0 {
        if tmfc.bc != 0 {
            tmfc.cc = -tmfc.cc;
        }
        if tmfc.cc >= 0 {
            match (tmfc.cc - 1).checked_mul(100).and_then(|t| t.checked_add(1)) {
                Some(v) => tm.tm_year = v,
                None => {
                    date_time_parse_error(
                        DTERR_FIELD_OVERFLOW,
                        None,
                        &lossy(date_txt),
                        "timestamp",
                        escontext.as_deref_mut(),
                    )?;
                    return Ok(false);
                }
            }
        } else {
            match tmfc.cc.checked_mul(100).and_then(|t| t.checked_add(1)) {
                Some(v) => tm.tm_year = v,
                None => {
                    date_time_parse_error(
                        DTERR_FIELD_OVERFLOW,
                        None,
                        &lossy(date_txt),
                        "timestamp",
                        escontext.as_deref_mut(),
                    )?;
                    return Ok(false);
                }
            }
        }
        fmask |= dtk_m(YEAR);
    }

    if tmfc.j != 0 {
        let YmdDate { year, mon, mday } = j2date_seam(tmfc.j);
        tm.tm_year = year;
        tm.tm_mon = mon;
        tm.tm_mday = mday;
        fmask |= dtk_date_m();
    }

    if tmfc.ww != 0 {
        if tmfc.mode == FromCharDateMode::Isoweek {
            let ymd = if tmfc.d != 0 {
                isoweekdate2date_seam(tmfc.ww, tmfc.d, tm.tm_year)
            } else {
                isoweek2date_seam(tmfc.ww, tm.tm_year)
            };
            tm.tm_year = ymd.year;
            tm.tm_mon = ymd.mon;
            tm.tm_mday = ymd.mday;
            fmask |= dtk_date_m();
        } else {
            // tmfc.ddd = (tmfc.ww - 1) * 7 + 1
            match tmfc
                .ww
                .checked_sub(1)
                .and_then(|t| t.checked_mul(7))
                .and_then(|t| t.checked_add(1))
            {
                Some(v) => tmfc.ddd = v,
                None => {
                    date_time_parse_error(
                        DTERR_FIELD_OVERFLOW,
                        None,
                        &lossy(date_str),
                        "timestamp",
                        escontext.as_deref_mut(),
                    )?;
                    return Ok(false);
                }
            }
        }
    }

    if tmfc.w != 0 {
        match tmfc
            .w
            .checked_sub(1)
            .and_then(|t| t.checked_mul(7))
            .and_then(|t| t.checked_add(1))
        {
            Some(v) => tmfc.dd = v,
            None => {
                date_time_parse_error(
                    DTERR_FIELD_OVERFLOW,
                    None,
                    &lossy(date_str),
                    "timestamp",
                    escontext.as_deref_mut(),
                )?;
                return Ok(false);
            }
        }
    }
    if tmfc.dd != 0 {
        tm.tm_mday = tmfc.dd;
        fmask |= dtk_m(DAY);
    }
    if tmfc.mm != 0 {
        tm.tm_mon = tmfc.mm;
        fmask |= dtk_m(MONTH);
    }

    if tmfc.ddd != 0 && (tm.tm_mon <= 1 || tm.tm_mday <= 1) {
        if tm.tm_year == 0 && tmfc.bc == 0 {
            errsave(
                escontext.as_deref_mut(),
                PgError::error("cannot calculate day of year without year information".to_string())
                    .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT),
            )?;
            return Ok(false);
        }

        if tmfc.mode == FromCharDateMode::Isoweek {
            let j0 = isoweek2j(tm.tm_year, 1) - 1;
            let YmdDate { year, mon, mday } = j2date_seam(j0 + tmfc.ddd);
            tm.tm_year = year;
            tm.tm_mon = mon;
            tm.tm_mday = mday;
            fmask |= dtk_date_m();
        } else {
            const YSUM: [[i32; 13]; 2] = [
                [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365],
                [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366],
            ];
            let y = &YSUM[if isleap(tm.tm_year) { 1 } else { 0 }];
            let mut i = 1;
            while i <= MONTHS_PER_YEAR {
                if tmfc.ddd <= y[i as usize] {
                    break;
                }
                i += 1;
            }
            if tm.tm_mon <= 1 {
                tm.tm_mon = i;
            }
            if tm.tm_mday <= 1 {
                tm.tm_mday = tmfc.ddd - y[(i - 1) as usize];
            }
            fmask |= dtk_m(MONTH) | dtk_m(DAY);
        }
    }

    if tmfc.ms != 0 {
        // *fsec += tmfc.ms * 1000
        match tmfc.ms.checked_mul(1000).and_then(|t| (*fsec).checked_add(t)) {
            Some(v) => *fsec = v,
            None => {
                date_time_parse_error(
                    DTERR_FIELD_OVERFLOW,
                    None,
                    &lossy(date_str),
                    "timestamp",
                    escontext.as_deref_mut(),
                )?;
                return Ok(false);
            }
        }
    }
    if tmfc.us != 0 {
        *fsec += tmfc.us;
    }
    if let Some(f) = fprec {
        *f = tmfc.ff;
    }

    // Range-check date fields.
    if fmask != 0 {
        let dterr = validate_date(fmask, false, false, tm);
        if dterr != 0 {
            date_time_parse_error(
                DTERR_FIELD_OVERFLOW,
                None,
                &lossy(date_str),
                "timestamp",
                escontext.as_deref_mut(),
            )?;
            return Ok(false);
        }
    }

    // Range-check time fields.
    if tm.tm_hour < 0
        || tm.tm_hour >= HOURS_PER_DAY
        || tm.tm_min < 0
        || tm.tm_min >= MINS_PER_HOUR
        || tm.tm_sec < 0
        || tm.tm_sec >= SECS_PER_MINUTE
        || (*fsec as i64) < 0
        || (*fsec as i64) >= USECS_PER_SEC
    {
        date_time_parse_error(
            DTERR_FIELD_OVERFLOW,
            None,
            &lossy(date_str),
            "timestamp",
            escontext.as_deref_mut(),
        )?;
        return Ok(false);
    }

    // Reduce timezone info to a GMT offset.
    if tmfc.tzsign != 0 {
        if tmfc.tzh < 0 || tmfc.tzh > MAX_TZDISP_HOUR || tmfc.tzm < 0 || tmfc.tzm >= MINS_PER_HOUR {
            date_time_parse_error(
                DTERR_TZDISP_OVERFLOW,
                None,
                &lossy(date_str),
                "timestamp",
                escontext,
            )?;
            return Ok(false);
        }
        tz.has_tz = true;
        tz.gmtoffset = (tmfc.tzh * MINS_PER_HOUR + tmfc.tzm) * SECS_PER_MINUTE;
        if tmfc.tzsign > 0 {
            tz.gmtoffset = -tz.gmtoffset;
        }
    } else if tmfc.has_tz {
        tz.has_tz = true;
        match tmfc.tzp {
            // fixed-offset abbreviation; flip the sign convention
            None => tz.gmtoffset = -tmfc.gmtoffset,
            // dynamic-offset abbreviation, resolve using specified time
            Some(tzp) => {
                let abbr = tmfc.abbrev.clone().unwrap_or_default();
                tz.gmtoffset = determine_time_zone_abbrev_offset(tm, &abbr, tzp);
            }
        }
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// Small helpers mirroring C macros / inline routines.
// ---------------------------------------------------------------------------

/// C: `COPY_tm` (formatting.c:534) — pg_tm -> fmt_tm.
fn copy_tm(dst: &mut FmtTm, src: &pg_tm) {
    dst.tm_sec = src.tm_sec;
    dst.tm_min = src.tm_min;
    dst.tm_hour = src.tm_hour as i64;
    dst.tm_mday = src.tm_mday;
    dst.tm_mon = src.tm_mon;
    dst.tm_year = src.tm_year;
    dst.tm_wday = src.tm_wday;
    dst.tm_yday = src.tm_yday;
    dst.tm_gmtoff = src.tm_gmtoff;
}

fn lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn timestamp_out_of_range() -> PgError {
    PgError::error("timestamp out of range".to_string())
        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
}

fn date_out_of_range(date_txt: &[u8]) -> PgError {
    PgError::error(format!("date out of range: \"{}\"", lossy(date_txt)))
        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
}

// DTK field-mask helpers (datetime.h). DTK_M(t) == 1 << t.
// YEAR/MONTH/DAY token values come from types_datetime (datetime.h: MONTH=1,
// YEAR=2, DAY=3) and MUST match what ValidateDate() tests, or the field-mask
// bits set here won't line up with the range checks there.
#[inline]
fn dtk_m(t: i32) -> i32 {
    1 << t
}
/// C: `DTK_DATE_M` == DTK_M(YEAR)|DTK_M(MONTH)|DTK_M(DAY).
#[inline]
fn dtk_date_m() -> i32 {
    dtk_m(YEAR) | dtk_m(MONTH) | dtk_m(DAY)
}

/// C: `isleap(y)` (datetime.h).
#[inline]
fn isleap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// C: `IS_VALID_JULIAN(y, m, d)` (include/datatype/timestamp.h:227).
///
/// This is a PURE year/month range comparison; the day argument is unused and,
/// crucially, `date2j` is NOT called here (calling it on an unvalidated year
/// would overflow).
#[inline]
fn is_valid_julian(y: i32, m: i32, _d: i32) -> bool {
    (y > JULIAN_MINYEAR || (y == JULIAN_MINYEAR && m >= JULIAN_MINMONTH))
        && (y < JULIAN_MAXYEAR || (y == JULIAN_MAXYEAR && m < JULIAN_MAXMONTH))
}

/// C: `IS_VALID_DATE(d)` (date.h).
#[inline]
fn is_valid_date(d: DateADT) -> bool {
    (DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) <= d && d < (DATE_END_JULIAN - POSTGRES_EPOCH_JDATE)
}

/// C: `DateTimeParseError` (datetime.c:4214). Maps a `DTERR_*` code to the
/// matching ereport/errsave call. In `formatting.c` only `DTERR_FIELD_OVERFLOW`
/// and `DTERR_TZDISP_OVERFLOW` are passed, but the full mapping is ported for
/// fidelity. This is pure logic (no external call), so it lives in-crate.
fn date_time_parse_error(
    dterr: i32,
    extra: Option<&DateTimeErrorExtra>,
    str: &str,
    datatype: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<()> {
    let err = match dterr {
        DTERR_FIELD_OVERFLOW => {
            PgError::error(format!("date/time field value out of range: \"{str}\""))
                .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW)
        }
        DTERR_MD_FIELD_OVERFLOW => {
            PgError::error(format!("date/time field value out of range: \"{str}\""))
                .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW)
                .with_hint("Perhaps you need a different \"DateStyle\" setting.")
        }
        DTERR_INTERVAL_OVERFLOW => {
            PgError::error(format!("interval field value out of range: \"{str}\""))
                .with_sqlstate(ERRCODE_INTERVAL_FIELD_OVERFLOW)
        }
        DTERR_TZDISP_OVERFLOW => {
            PgError::error(format!("time zone displacement out of range: \"{str}\""))
                .with_sqlstate(ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE)
        }
        DTERR_BAD_TIMEZONE => {
            let tz = extra.and_then(|e| e.dtee_timezone.clone()).unwrap_or_default();
            PgError::error(format!("time zone \"{tz}\" not recognized"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        }
        DTERR_BAD_ZONE_ABBREV => {
            let tz = extra.and_then(|e| e.dtee_timezone.clone()).unwrap_or_default();
            let abbrev = extra.and_then(|e| e.dtee_abbrev.clone()).unwrap_or_default();
            PgError::error(format!("time zone \"{tz}\" not recognized"))
                .with_sqlstate(ERRCODE_CONFIG_FILE_ERROR)
                .with_detail(format!(
                    "This time zone name appears in the configuration file for time zone abbreviation \"{abbrev}\"."
                ))
        }
        DTERR_BAD_FORMAT => bad_format(datatype, str),
        _ => bad_format(datatype, str),
    };
    errsave(escontext, err)
}

fn bad_format(datatype: &str, str: &str) -> PgError {
    PgError::error(format!("invalid input syntax for type {datatype}: \"{str}\""))
        .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT)
}
