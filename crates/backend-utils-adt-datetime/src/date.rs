//! The DATE value type, ported from `src/backend/utils/adt/date.c` (idiomatic,
//! safe Rust).
//!
//! This is the *core* (plain-Rust computational) half of date.c's DATE ADT:
//! the `date_in`/`date_out` conversion cores, the comparison primitives, the
//! integer-day arithmetic, `make_date`, and the `extract_date` field-extraction
//! core (both the `int64` result and the BC/Infinity special-casing).  We do
//! NOT port the fmgr `Datum` shims (those follow the project systemic deferral).
//!
//! The cross-type date<->timestamp[tz] *computational* cores live at the bottom
//! of this file: the non-tz conversion/comparison
//! (`date2timestamp[_opt_overflow]`, `date_cmp_timestamp_internal`) plus the
//! TIMESTAMPTZ-flavoured siblings (`date2timestamptz[_opt_overflow]`,
//! `date2timestamp_no_overflow`, `date_cmp_timestamptz_internal`) and the
//! `date_pl_interval` / `date_mi_interval` arithmetic cores (which promote the
//! date to a timestamp then defer to `timestamp_pl_interval` /
//! `timestamp_mi_interval`).  It also hosts the shared `DateTimeParseError`
//! mapping ([`datetime_parse_error_for`]) used by every date/time scalar.
//!
//! Idiomatic surface: plain `i32`/`i64`/`f64`, owned values, `Option`,
//! `Result`, `&str`.  No raw pointers, `extern "C"`, `c_int`, `libc`,
//! `CStr`/`CString`, or `pgrust_pg_ffi`.


use types_pgtime::{pg_tm};
use state_pgtz::session_timezone;
use types_datetime::{
    Interval, TimeTzADT, DTERR_BAD_FORMAT, DTK_DATE, DTK_EARLY, DTK_EPOCH, DTK_LATE, DT_NOBEGIN,
    DT_NOEND, MIN_TIMESTAMP, POSTGRES_EPOCH_JDATE, SECS_PER_DAY, TIMESTAMP_END_JULIAN,
    UNIX_EPOCH_JDATE, USECS_PER_DAY, USECS_PER_SEC,
};
use types_error::{
    ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE,
};
use types_datetime::{fsec_t, DateADT, TimeADT, Timestamp, TimestampTz};
use types_error::{PgError, PgResult};

use crate::calendar::{date2j, j2date, j2day};
use crate::consts::DTK_DATE_M;
use crate::decode::{
    DecodeDateTime, DecodeUnits, DetermineTimeZoneOffset, ParseDateTime, ValidateDate,
};
use crate::encode::{EncodeDateOnly, EncodeSpecialDate};
use crate::isoweek::{date2isoweek, date2isoyear};
use crate::settings::date_style;
use crate::timestamp::{
    timestamp_cmp_internal, timestamp_mi_interval, timestamp_pl_interval, IS_VALID_TIMESTAMP,
    TIMESTAMP_IS_NOBEGIN, TIMESTAMP_IS_NOEND,
};

const MAXDATEFIELDS: usize = types_datetime::MAXDATEFIELDS as usize;

// ---------------------------------------------------------------------------
// date.h / timestamp.h constants and macros (not part of the shared ABI).
// ---------------------------------------------------------------------------

/// `DATEVAL_NOBEGIN` -- reserved DateADT for `-infinity`. (`utils/date.h`)
pub const DATEVAL_NOBEGIN: DateADT = i32::MIN;
/// `DATEVAL_NOEND` -- reserved DateADT for `infinity`. (`utils/date.h`)
pub const DATEVAL_NOEND: DateADT = i32::MAX;

/// `DATE_IS_NOBEGIN(j)` (`utils/date.h`)
#[inline]
pub fn DATE_IS_NOBEGIN(j: DateADT) -> bool {
    j == DATEVAL_NOBEGIN
}

/// `DATE_IS_NOEND(j)` (`utils/date.h`)
#[inline]
pub fn DATE_IS_NOEND(j: DateADT) -> bool {
    j == DATEVAL_NOEND
}

/// `DATE_NOT_FINITE(j)` (`utils/date.h`)
#[inline]
pub fn DATE_NOT_FINITE(j: DateADT) -> bool {
    DATE_IS_NOBEGIN(j) || DATE_IS_NOEND(j)
}

/// `IS_VALID_JULIAN(y, m, d)` (`datatype/timestamp.h`).  Re-exported from
/// [`crate::convert`] (the canonical, seam-free home).
pub use crate::convert::{IS_VALID_DATE, IS_VALID_JULIAN};

// ---------------------------------------------------------------------------
// Small error constructors mirroring the C ereport sites.
// ---------------------------------------------------------------------------

#[inline]
fn date_out_of_range() -> PgError {
    PgError::error("date out of range").with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
}

// ---------------------------------------------------------------------------
// date_in() / date_out() cores
// ---------------------------------------------------------------------------

/// `date_in()` CORE -- parse a date text string into the internal `DateADT`.
///
/// `DTK_EPOCH` maps to 1970-01-01 (the Unix epoch as a DateADT).
pub fn date_in(str: &str) -> PgResult<DateADT> {
    let mut field: Vec<String> = Vec::new();
    let mut ftype: Vec<i32> = Vec::new();
    let mut nf = 0usize;

    let mut tt = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut dtype: i32 = 0;
    let mut tzp: i32 = 0;
    let mut extra = types_datetime::DateTimeErrorExtra::default();

    // C date_in: workbuf[MAXDATELEN + 1] (date.c:128).
    let mut dterr = ParseDateTime(
        str,
        types_datetime::MAXDATELEN as usize + 1,
        &mut field,
        &mut ftype,
        MAXDATEFIELDS,
        &mut nf,
    );
    if dterr == 0 {
        dterr = DecodeDateTime(
            &mut field,
            &mut ftype,
            nf,
            &mut dtype,
            &mut tt,
            &mut fsec,
            Some(&mut tzp),
            &mut extra,
        );
    }
    if dterr != 0 {
        return Err(date_parse_error(dterr, str, &extra));
    }

    match dtype {
        d if d == DTK_DATE => {
            // tm already holds the decoded y/m/d.
        }
        d if d == DTK_EPOCH => {
            // GetEpochTime(tm) sets tm to 1970-01-01 00:00:00.
            tt.tm_year = 1970;
            tt.tm_mon = 1;
            tt.tm_mday = 1;
        }
        d if d == DTK_LATE => return Ok(DATEVAL_NOEND),
        d if d == DTK_EARLY => return Ok(DATEVAL_NOBEGIN),
        _ => return Err(date_parse_error(DTERR_BAD_FORMAT, str, &extra)),
    }

    // Prevent overflow in Julian-day routines.
    if !IS_VALID_JULIAN(tt.tm_year, tt.tm_mon, tt.tm_mday) {
        return Err(PgError::error(format!("date out of range: \"{str}\""))
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }

    let date = date2j(tt.tm_year, tt.tm_mon, tt.tm_mday) - POSTGRES_EPOCH_JDATE;

    // Now check for just-out-of-range dates.
    if !IS_VALID_DATE(date) {
        return Err(PgError::error(format!("date out of range: \"{str}\""))
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }

    Ok(date)
}

/// `date_out()` CORE -- render a `DateADT` to a text string.
pub fn date_out(date: DateADT) -> String {
    let mut buf = String::new();
    if DATE_NOT_FINITE(date) {
        EncodeSpecialDate(date, &mut buf);
    } else {
        let (y, m, d) = j2date(date + POSTGRES_EPOCH_JDATE);
        let tm = pg_tm {
            tm_year: y,
            tm_mon: m,
            tm_mday: d,
            ..Default::default()
        };
        EncodeDateOnly(&tm, date_style(), &mut buf);
    }
    buf
}

/// `make_date()` CORE -- date constructor from (year, month, day).
///
/// Negative years are treated as BC, exactly as the SQL function does.
pub fn make_date(year: i32, month: i32, day: i32) -> PgResult<DateADT> {
    let mut tm = pg_tm {
        tm_year: year,
        tm_mon: month,
        tm_mday: day,
        ..Default::default()
    };

    let mut bc = false;

    // Handle negative years as BC.
    if tm.tm_year < 0 {
        bc = true;
        // pg_neg_s32_overflow: -INT32_MIN overflows.
        match tm.tm_year.checked_neg() {
            Some(y) => tm.tm_year = y,
            None => return Err(make_date_field_overflow(year, month, day)),
        }
    }

    let dterr = ValidateDate(DTK_DATE_M, false, false, bc, &mut tm);
    if dterr != 0 {
        // C prints the post-ValidateDate (BC-normalized) tm fields here.
        return Err(make_date_field_overflow(tm.tm_year, tm.tm_mon, tm.tm_mday));
    }

    // Prevent overflow in Julian-day routines.
    if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        return Err(PgError::error(format!(
            "date out of range: {}-{:02}-{:02}",
            tm.tm_year, tm.tm_mon, tm.tm_mday
        ))
        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }

    let date = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

    if !IS_VALID_DATE(date) {
        return Err(PgError::error(format!(
            "date out of range: {}-{:02}-{:02}",
            tm.tm_year, tm.tm_mon, tm.tm_mday
        ))
        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }

    Ok(date)
}

#[inline]
fn make_date_field_overflow(year: i32, month: i32, day: i32) -> PgError {
    PgError::error(format!(
        "date field value out of range: {year}-{month:02}-{day:02}"
    ))
    .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
}

// ---------------------------------------------------------------------------
// Comparison cores
// ---------------------------------------------------------------------------

/// `date_cmp()` CORE.
#[inline]
pub fn date_cmp(d1: DateADT, d2: DateADT) -> i32 {
    if d1 < d2 {
        -1
    } else if d1 > d2 {
        1
    } else {
        0
    }
}

/// `date_eq()` CORE.
#[inline]
pub fn date_eq(d1: DateADT, d2: DateADT) -> bool {
    d1 == d2
}

/// `date_ne()` CORE.
#[inline]
pub fn date_ne(d1: DateADT, d2: DateADT) -> bool {
    d1 != d2
}

/// `date_lt()` CORE.
#[inline]
pub fn date_lt(d1: DateADT, d2: DateADT) -> bool {
    d1 < d2
}

/// `date_le()` CORE.
#[inline]
pub fn date_le(d1: DateADT, d2: DateADT) -> bool {
    d1 <= d2
}

/// `date_gt()` CORE.
#[inline]
pub fn date_gt(d1: DateADT, d2: DateADT) -> bool {
    d1 > d2
}

/// `date_ge()` CORE.
#[inline]
pub fn date_ge(d1: DateADT, d2: DateADT) -> bool {
    d1 >= d2
}

/// `date_larger()` CORE.
#[inline]
pub fn date_larger(d1: DateADT, d2: DateADT) -> DateADT {
    if d1 > d2 {
        d1
    } else {
        d2
    }
}

/// `date_smaller()` CORE.
#[inline]
pub fn date_smaller(d1: DateADT, d2: DateADT) -> DateADT {
    if d1 < d2 {
        d1
    } else {
        d2
    }
}

/// `date_finite()` CORE.
#[inline]
pub fn date_finite(date: DateADT) -> bool {
    !DATE_NOT_FINITE(date)
}

// ---------------------------------------------------------------------------
// Arithmetic cores
// ---------------------------------------------------------------------------

/// `date_mi()` CORE -- difference between two dates in days.  Errors on infinity.
pub fn date_mi(d1: DateADT, d2: DateADT) -> PgResult<i32> {
    if DATE_NOT_FINITE(d1) || DATE_NOT_FINITE(d2) {
        return Err(PgError::error("cannot subtract infinite dates")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }
    // C computes (int32)(d1 - d2); both are i32, so wrap to match.
    Ok(d1.wrapping_sub(d2))
}

/// `date_pli()` CORE -- add a number of days to a date.
pub fn date_pli(date: DateADT, days: i32) -> PgResult<DateADT> {
    if DATE_NOT_FINITE(date) {
        return Ok(date); // can't change infinity
    }
    let result = date.wrapping_add(days);
    // Check for integer overflow and out-of-allowed-range.
    let overflowed = if days >= 0 { result < date } else { result > date };
    if overflowed || !IS_VALID_DATE(result) {
        return Err(date_out_of_range());
    }
    Ok(result)
}

/// `date_mii()` CORE -- subtract a number of days from a date.
pub fn date_mii(date: DateADT, days: i32) -> PgResult<DateADT> {
    if DATE_NOT_FINITE(date) {
        return Ok(date); // can't change infinity
    }
    let result = date.wrapping_sub(days);
    let overflowed = if days >= 0 { result > date } else { result < date };
    if overflowed || !IS_VALID_DATE(result) {
        return Err(date_out_of_range());
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// extract_date() core
// ---------------------------------------------------------------------------

/// The result of [`extract_date`]: either a finite `int64` value (the common
/// case), or one of the `±Infinity` numeric sentinels for monotone units on an
/// infinite date.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExtractDateResult {
    /// A finite integer result to be wrapped with `int64_to_numeric`.
    Int(i64),
    /// Field is undefined for an infinite date (`PG_RETURN_NULL`).
    Null,
    /// `numeric_in("Infinity")` for a monotone unit on `+infinity` date.
    PosInfinity,
    /// `numeric_in("-Infinity")` for a monotone unit on `-infinity` date.
    NegInfinity,
}

/// `extract_date()` CORE -- extract the field named by `lowunits` from `date`.
///
/// `lowunits` must be the already-lowercased unit name.
pub fn extract_date(lowunits: &str, date: DateADT) -> PgResult<ExtractDateResult> {
    use types_datetime::{
        DTK_CENTURY, DTK_DAY, DTK_DECADE, DTK_DOW, DTK_DOY, DTK_ISODOW, DTK_ISOYEAR, DTK_JULIAN,
        DTK_MILLENNIUM, DTK_MONTH, DTK_QUARTER, DTK_WEEK, DTK_YEAR, RESERV, UNITS, UNKNOWN_FIELD,
    };

    let mut val: i32 = 0;
    let mut typ = DecodeUnits(0, lowunits, &mut val);
    if typ == UNKNOWN_FIELD {
        typ = crate::decode::DecodeSpecial(0, lowunits, &mut val);
    }

    if DATE_NOT_FINITE(date) && (typ == UNITS || typ == RESERV) {
        match val {
            // Oscillating units -> NULL.
            v if v == DTK_DAY
                || v == DTK_MONTH
                || v == DTK_QUARTER
                || v == DTK_WEEK
                || v == DTK_DOW
                || v == DTK_ISODOW
                || v == DTK_DOY =>
            {
                Ok(ExtractDateResult::Null)
            }
            // Monotonically-increasing units -> ±Infinity.
            v if v == DTK_YEAR
                || v == DTK_DECADE
                || v == DTK_CENTURY
                || v == DTK_MILLENNIUM
                || v == DTK_JULIAN
                || v == DTK_ISOYEAR
                || v == DTK_EPOCH =>
            {
                if DATE_IS_NOBEGIN(date) {
                    Ok(ExtractDateResult::NegInfinity)
                } else {
                    Ok(ExtractDateResult::PosInfinity)
                }
            }
            _ => Err(unit_not_supported(lowunits)),
        }
    } else if typ == UNITS {
        let (year, mon, mday) = j2date(date + POSTGRES_EPOCH_JDATE);
        let intresult: i64 = match val {
            v if v == DTK_DAY => mday as i64,
            v if v == DTK_MONTH => mon as i64,
            v if v == DTK_QUARTER => ((mon - 1) / 3 + 1) as i64,
            v if v == DTK_WEEK => date2isoweek(year, mon, mday) as i64,
            v if v == DTK_YEAR => {
                if year > 0 {
                    year as i64
                } else {
                    // there is no year 0, just 1 BC and 1 AD
                    (year - 1) as i64
                }
            }
            v if v == DTK_DECADE => {
                if year >= 0 {
                    (year / 10) as i64
                } else {
                    -(((8 - (year - 1)) / 10) as i64)
                }
            }
            v if v == DTK_CENTURY => {
                if year > 0 {
                    ((year + 99) / 100) as i64
                } else {
                    -(((99 - (year - 1)) / 100) as i64)
                }
            }
            v if v == DTK_MILLENNIUM => {
                if year > 0 {
                    ((year + 999) / 1000) as i64
                } else {
                    -(((999 - (year - 1)) / 1000) as i64)
                }
            }
            v if v == DTK_JULIAN => (date + POSTGRES_EPOCH_JDATE) as i64,
            v if v == DTK_ISOYEAR => {
                let mut r = date2isoyear(year, mon, mday) as i64;
                if r <= 0 {
                    r -= 1; // Adjust BC years
                }
                r
            }
            v if v == DTK_DOW || v == DTK_ISODOW => {
                let mut r = j2day(date + POSTGRES_EPOCH_JDATE) as i64;
                if v == DTK_ISODOW && r == 0 {
                    r = 7;
                }
                r
            }
            v if v == DTK_DOY => (date2j(year, mon, mday) - date2j(year, 1, 1) + 1) as i64,
            _ => return Err(unit_not_supported(lowunits)),
        };
        Ok(ExtractDateResult::Int(intresult))
    } else if typ == RESERV {
        if val == DTK_EPOCH {
            let intresult = (date as i64 + POSTGRES_EPOCH_JDATE as i64 - UNIX_EPOCH_JDATE as i64)
                * SECS_PER_DAY as i64;
            return Ok(ExtractDateResult::Int(intresult));
        }
        Err(unit_not_supported(lowunits))
    } else {
        Err(unit_not_recognized(lowunits))
    }
}

#[inline]
fn unit_not_supported(lowunits: &str) -> PgError {
    PgError::error(format!("unit \"{lowunits}\" not supported for type date"))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

#[inline]
fn unit_not_recognized(lowunits: &str) -> PgError {
    PgError::error(format!("unit \"{lowunits}\" not recognized for type date"))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

// ---------------------------------------------------------------------------
// Decode helper plumbing.
// ---------------------------------------------------------------------------

/// Map a `DTERR_*` code to a [`PgError`] for the date type.
fn date_parse_error(dterr: i32, str: &str, extra: &types_datetime::DateTimeErrorExtra) -> PgError {
    datetime_parse_error_for(dterr, str, "date", extra)
}

/// Shared `DateTimeParseError` mapping used by both the date and the
/// timestamp/timestamptz/interval cores (`datatype` selects the bad-format
/// message label).  Mirrors datetime.c:4214 case-by-case.
pub(crate) fn datetime_parse_error_for(
    dterr: i32,
    str: &str,
    datatype: &str,
    extra: &types_datetime::DateTimeErrorExtra,
) -> PgError {
    use types_datetime::{
        DTERR_BAD_TIMEZONE, DTERR_BAD_ZONE_ABBREV, DTERR_FIELD_OVERFLOW, DTERR_INTERVAL_OVERFLOW,
        DTERR_MD_FIELD_OVERFLOW, DTERR_TZDISP_OVERFLOW,
    };
    use types_error::{
        ERRCODE_CONFIG_FILE_ERROR, ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_INTERVAL_FIELD_OVERFLOW,
        ERRCODE_INVALID_DATETIME_FORMAT, ERRCODE_INVALID_PARAMETER_VALUE,
        ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
    };

    // ERRCODE_DATETIME_FIELD_OVERFLOW shares SQLSTATE 22008 with
    // ERRCODE_DATETIME_VALUE_OUT_OF_RANGE.
    if dterr == DTERR_FIELD_OVERFLOW {
        return PgError::error(format!("date/time field value out of range: \"{str}\""))
            .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW);
    }
    if dterr == DTERR_MD_FIELD_OVERFLOW {
        // Same as above, but add the DateStyle hint (datetime.c:4232).
        return PgError::error(format!("date/time field value out of range: \"{str}\""))
            .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW)
            .with_hint("Perhaps you need a different \"DateStyle\" setting.");
    }
    if dterr == DTERR_INTERVAL_OVERFLOW {
        return PgError::error(format!("interval field value out of range: \"{str}\""))
            .with_sqlstate(ERRCODE_INTERVAL_FIELD_OVERFLOW);
    }
    if dterr == DTERR_TZDISP_OVERFLOW {
        return PgError::error(format!("time zone displacement out of range: \"{str}\""))
            .with_sqlstate(ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE);
    }
    if dterr == DTERR_BAD_TIMEZONE {
        // datetime.c:4246: names the offending zone (extra->dtee_timezone).
        let tzname = extra.dtee_timezone.as_deref().unwrap_or(str);
        return PgError::error(format!("time zone \"{tzname}\" not recognized"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE);
    }
    if dterr == DTERR_BAD_ZONE_ABBREV {
        // datetime.c:4252: names the underlying zone, with a detail about the
        // abbreviation that referenced it.
        let tzname = extra.dtee_timezone.as_deref().unwrap_or(str);
        let abbrev = extra.dtee_abbrev.as_deref().unwrap_or("");
        return PgError::error(format!("time zone \"{tzname}\" not recognized"))
            .with_sqlstate(ERRCODE_CONFIG_FILE_ERROR)
            .with_detail(format!(
                "This time zone name appears in the configuration file for time zone abbreviation \"{abbrev}\"."
            ));
    }
    // DTERR_BAD_FORMAT and default.
    PgError::error(format!(
        "invalid input syntax for type {datatype}: \"{str}\""
    ))
    .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT)
}

// ---------------------------------------------------------------------------
// Cross-type date <-> timestamp[tz] conversion / comparison cores.
// ---------------------------------------------------------------------------

/// `date2timestamp_opt_overflow()` (date.c:628) -- promote a date to a
/// `Timestamp` (microseconds since 2000-01-01).
pub fn date2timestamp_opt_overflow(date_val: DateADT) -> (Timestamp, i32) {
    if DATE_IS_NOBEGIN(date_val) {
        (DT_NOBEGIN, 0)
    } else if DATE_IS_NOEND(date_val) {
        (DT_NOEND, 0)
    } else if date_val >= (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE) {
        // Since dates have the same minimum values as timestamps, only the
        // upper boundary needs the overflow check.
        (DT_NOEND, 1)
    } else {
        // date is days since 2000, timestamp is microseconds since same...
        (date_val as Timestamp * USECS_PER_DAY, 0)
    }
}

/// `date2timestamp()` (date.c:671) -- promote a date to `Timestamp`, throwing an
/// out-of-range error for the overflow case.
pub fn date2timestamp(date_val: DateADT) -> PgResult<Timestamp> {
    let (result, overflow) = date2timestamp_opt_overflow(date_val);
    if overflow != 0 {
        return Err(PgError::error("date out of range for timestamp")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }
    Ok(result)
}

/// `date_cmp_timestamp_internal()` (date.c:808) -- cross-type comparison of a
/// date against a `Timestamp`.
pub fn date_cmp_timestamp_internal(date_val: DateADT, dt2: Timestamp) -> i32 {
    let (dt1, overflow) = date2timestamp_opt_overflow(date_val);
    if overflow > 0 {
        // dt1 is larger than any finite timestamp, but less than infinity.
        return if TIMESTAMP_IS_NOEND(dt2) { -1 } else { 1 };
    }
    debug_assert_eq!(overflow, 0); // the -1 case cannot occur
    timestamp_cmp_internal(dt1, dt2)
}

/// `date2timestamptz_opt_overflow()` (date.c:689) -- promote a date to a
/// `TimestampTz` (microseconds since 2000-01-01 UTC), rotating midnight in the
/// session zone to UTC via `DetermineTimeZoneOffset`.
pub fn date2timestamptz_opt_overflow(date_val: DateADT) -> (TimestampTz, i32) {
    if DATE_IS_NOBEGIN(date_val) {
        return (DT_NOBEGIN, 0);
    }
    if DATE_IS_NOEND(date_val) {
        return (DT_NOEND, 0);
    }
    // Since dates have the same minimum values as timestamps, only the upper
    // boundary need be checked for overflow.
    if date_val >= (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE) {
        return (DT_NOEND, 1);
    }

    let (y, mo, d) = j2date(date_val + POSTGRES_EPOCH_JDATE);
    let mut tm = pg_tm {
        tm_year: y,
        tm_mon: mo,
        tm_mday: d,
        tm_hour: 0,
        tm_min: 0,
        tm_sec: 0,
        ..Default::default()
    };
    let tz = DetermineTimeZoneOffset(&mut tm, &session_timezone());

    let result: TimestampTz = date_val as TimestampTz * USECS_PER_DAY + tz as i64 * USECS_PER_SEC;

    // Since it is possible to go beyond allowed timestamptz range because of the
    // time zone, check for allowed timestamp range after adding tz.
    if !IS_VALID_TIMESTAMP(result) {
        if result < MIN_TIMESTAMP {
            return (DT_NOBEGIN, -1);
        }
        return (DT_NOEND, 1);
    }

    (result, 0)
}

/// `date2timestamptz()` (date.c:768) -- promote a date to `TimestampTz`,
/// throwing an out-of-range error for the overflow case.
pub fn date2timestamptz(date_val: DateADT) -> PgResult<TimestampTz> {
    let (result, overflow) = date2timestamptz_opt_overflow(date_val);
    if overflow != 0 {
        return Err(PgError::error("date out of range for timestamp")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }
    Ok(result)
}

/// `date2timestamp_no_overflow()` (date.c:785) -- numeric (f64) equivalent of
/// the corresponding `Timestamp` value, never throwing on overflow.
pub fn date2timestamp_no_overflow(date_val: DateADT) -> f64 {
    if DATE_IS_NOBEGIN(date_val) {
        f64::MAX.copysign(-1.0) // -DBL_MAX
    } else if DATE_IS_NOEND(date_val) {
        f64::MAX // DBL_MAX
    } else {
        // date is days since 2000, timestamp is microseconds since same...
        date_val as f64 * USECS_PER_DAY as f64
    }
}

/// `date_cmp_timestamptz_internal()` (date.c:888) -- cross-type comparison of a
/// date against a `TimestampTz`.
pub fn date_cmp_timestamptz_internal(date_val: DateADT, dt2: TimestampTz) -> i32 {
    let (dt1, overflow) = date2timestamptz_opt_overflow(date_val);
    if overflow > 0 {
        // dt1 is larger than any finite timestamp, but less than infinity.
        return if TIMESTAMP_IS_NOEND(dt2) { -1 } else { 1 };
    }
    if overflow < 0 {
        // dt1 is less than any finite timestamp, but more than -infinity.
        return if TIMESTAMP_IS_NOBEGIN(dt2) { 1 } else { -1 };
    }
    // timestamptz_cmp_internal is identical to timestamp_cmp_internal.
    timestamp_cmp_internal(dt1, dt2)
}

/// `date_pl_interval()` (date.c:1311) CORE -- add an `Interval` to a date,
/// giving a `Timestamp`.
pub fn date_pl_interval(date_val: DateADT, span: &Interval) -> PgResult<Timestamp> {
    let date_stamp = date2timestamp(date_val)?;
    timestamp_pl_interval(date_stamp, span)
}

/// `date_mi_interval()` (date.c:1331) CORE -- subtract an `Interval` from a
/// date, giving a `Timestamp`.
pub fn date_mi_interval(date_val: DateADT, span: &Interval) -> PgResult<Timestamp> {
    let date_stamp = date2timestamp(date_val)?;
    timestamp_mi_interval(date_stamp, span)
}

/// `timestamp_date()` (date.c:1362) CORE -- the date portion of a `Timestamp`.
pub fn timestamp_date(timestamp: Timestamp) -> PgResult<DateADT> {
    use crate::timestamp::timestamp2tm;

    if TIMESTAMP_IS_NOBEGIN(timestamp) {
        return Ok(DATEVAL_NOBEGIN);
    }
    if TIMESTAMP_IS_NOEND(timestamp) {
        return Ok(DATEVAL_NOEND);
    }

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err() {
        return Err(date_out_of_range_for_timestamp());
    }
    Ok(date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE)
}

/// `timestamptz_date()` (date.c:1407) CORE -- the local date portion of a
/// `TimestampTz` (rotated into the session zone).
pub fn timestamptz_date(timestamp: TimestampTz) -> PgResult<DateADT> {
    use crate::timestamp::timestamp2tm;

    if TIMESTAMP_IS_NOBEGIN(timestamp) {
        return Ok(DATEVAL_NOBEGIN);
    }
    if TIMESTAMP_IS_NOEND(timestamp) {
        return Ok(DATEVAL_NOEND);
    }

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    if timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, None).is_err() {
        return Err(date_out_of_range_for_timestamp());
    }
    Ok(date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE)
}

/// The `timestamp_date`/`timestamptz_date` out-of-range error: C uses
/// "timestamp out of range" at these sites.
#[inline]
fn date_out_of_range_for_timestamp() -> PgError {
    PgError::error("timestamp out of range").with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
}

// ---------------------------------------------------------------------------
// GetSQLCurrentDate / GetSQLCurrentTime / GetSQLLocalTime (date.c)
// ---------------------------------------------------------------------------

/// `GetSQLCurrentDate()` (date.c:317) CORE -- implements `CURRENT_DATE`.
pub fn GetSQLCurrentDate() -> DateADT {
    let mut tm = pg_tm::default();
    crate::decode::GetCurrentDateTime(&mut tm);
    date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE
}

/// `GetSQLCurrentTime()` (date.c:350) CORE -- implements `CURRENT_TIME`,
/// `CURRENT_TIME(n)`.
pub fn GetSQLCurrentTime(typmod: i32) -> TimeTzADT {
    use crate::time::AdjustTimeForTypmod;
    use crate::timetz::tm2timetz;

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    crate::decode::GetCurrentTimeUsec(&mut tm, &mut fsec, Some(&mut tz));

    let mut result = tm2timetz(&tm, fsec, tz);
    AdjustTimeForTypmod(&mut result.time, typmod);
    result
}

/// `GetSQLLocalTime()` (date.c:370) CORE -- implements `LOCALTIME`,
/// `LOCALTIME(n)`.
pub fn GetSQLLocalTime(typmod: i32) -> TimeADT {
    use crate::time::{tm2time, AdjustTimeForTypmod};

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    crate::decode::GetCurrentTimeUsec(&mut tm, &mut fsec, Some(&mut tz));

    let mut result = tm2time(&tm, fsec);
    AdjustTimeForTypmod(&mut result, typmod);
    result
}

// ---------------------------------------------------------------------------
// time_timetz (date.c:2892)
// ---------------------------------------------------------------------------

/// `time_timetz()` (date.c:2892) CORE -- promote a `TimeADT` to a `TimeTzADT`,
/// using the session-zone offset that applies on the current date.
pub fn time_timetz(time: TimeADT) -> TimeTzADT {
    use crate::time::time2tm;

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;

    crate::decode::GetCurrentDateTime(&mut tm);
    time2tm(time, &mut tm, &mut fsec);
    let tz = DetermineTimeZoneOffset(&mut tm, &session_timezone());

    TimeTzADT { time, zone: tz }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::{set_date_order, DATE_ORDER_TEST_LOCK};
    use types_datetime::DATEORDER_MDY;

    #[test]
    fn date_in_out_round_trip() {
        let _guard = DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        set_date_order(DATEORDER_MDY);

        for s in ["2000-01-01", "1970-01-01", "2024-02-29", "0001-01-01"] {
            let d = date_in(s).unwrap();
            assert_eq!(date_out(d), s, "round trip failed for {s}");
        }
    }

    #[test]
    fn date_in_known_anchors() {
        let _guard = DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        set_date_order(DATEORDER_MDY);

        assert_eq!(date_in("2000-01-01").unwrap(), 0);
        assert_eq!(
            date_in("1970-01-01").unwrap(),
            UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE
        );
    }

    #[test]
    fn date_out_infinities() {
        assert_eq!(date_out(DATEVAL_NOEND), "infinity");
        assert_eq!(date_out(DATEVAL_NOBEGIN), "-infinity");
    }

    #[test]
    fn date_arithmetic_int_days() {
        let _guard = DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        set_date_order(DATEORDER_MDY);
        let base = date_in("2000-01-01").unwrap();
        let plus = date_pli(base, 31).unwrap();
        assert_eq!(date_out(plus), "2000-02-01");
        let back = date_mii(plus, 31).unwrap();
        assert_eq!(back, base);
        assert_eq!(date_mi(plus, base).unwrap(), 31);
    }

    #[test]
    fn date_pli_infinity_is_inert() {
        assert_eq!(date_pli(DATEVAL_NOEND, 5).unwrap(), DATEVAL_NOEND);
        assert_eq!(date_mii(DATEVAL_NOBEGIN, 5).unwrap(), DATEVAL_NOBEGIN);
    }

    #[test]
    fn date_mi_rejects_infinity() {
        assert!(date_mi(DATEVAL_NOEND, 0).is_err());
    }

    #[test]
    fn date_comparisons() {
        let a = date_in_mdy("2000-01-01");
        let b = date_in_mdy("2000-01-02");
        assert_eq!(date_cmp(a, b), -1);
        assert_eq!(date_cmp(b, a), 1);
        assert_eq!(date_cmp(a, a), 0);
        assert!(date_lt(a, b));
        assert!(date_le(a, a));
        assert!(date_gt(b, a));
        assert!(date_ge(a, a));
        assert!(date_eq(a, a));
        assert!(date_ne(a, b));
        assert_eq!(date_larger(a, b), b);
        assert_eq!(date_smaller(a, b), a);
        assert!(date_finite(a));
        assert!(!date_finite(DATEVAL_NOEND));
    }

    #[test]
    fn make_date_builds_dates() {
        let d = make_date(2024, 2, 29).unwrap();
        assert_eq!(date_out_mdy(d), "2024-02-29");
        assert!(make_date(2023, 2, 29).is_err());
        let bc = make_date(-1, 1, 1).unwrap();
        let (y, _, _) = j2date(bc + POSTGRES_EPOCH_JDATE);
        assert_eq!(y, 0);
    }

    #[test]
    fn make_date_field_overflow_message_uses_normalized_fields() {
        let err = make_date(-5, 13, 1).unwrap_err();
        assert_eq!(err.message(), "date field value out of range: -4-13-01");
        assert_eq!(err.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
    }

    #[test]
    fn extract_date_fields() {
        let d = make_date(2024, 3, 15).unwrap();
        assert_eq!(
            extract_date("year", d).unwrap(),
            ExtractDateResult::Int(2024)
        );
        assert_eq!(extract_date("month", d).unwrap(), ExtractDateResult::Int(3));
        assert_eq!(extract_date("day", d).unwrap(), ExtractDateResult::Int(15));
        assert_eq!(
            extract_date("day", DATEVAL_NOEND).unwrap(),
            ExtractDateResult::Null
        );
        assert_eq!(
            extract_date("year", DATEVAL_NOEND).unwrap(),
            ExtractDateResult::PosInfinity
        );
        assert_eq!(
            extract_date("year", DATEVAL_NOBEGIN).unwrap(),
            ExtractDateResult::NegInfinity
        );
        assert!(extract_date("nonsense", d).is_err());
    }

    #[test]
    fn date2timestamp_promotes_and_overflows() {
        assert_eq!(date2timestamp(0).unwrap(), 0);
        let d = make_date(2000, 1, 2).unwrap();
        assert_eq!(date2timestamp(d).unwrap(), d as i64 * USECS_PER_DAY);
        assert_eq!(date2timestamp(DATEVAL_NOEND).unwrap(), DT_NOEND);
        assert_eq!(date2timestamp(DATEVAL_NOBEGIN).unwrap(), DT_NOBEGIN);
        let beyond = TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE;
        let (val, overflow) = date2timestamp_opt_overflow(beyond);
        assert_eq!(overflow, 1);
        assert_eq!(val, DT_NOEND);
        assert!(date2timestamp(beyond).is_err());
    }

    #[test]
    fn date_cmp_timestamp_orders_cross_type() {
        let d = make_date(2000, 1, 2).unwrap();
        let ts = date2timestamp(d).unwrap();
        assert_eq!(date_cmp_timestamp_internal(d, ts), 0);
        assert_eq!(date_cmp_timestamp_internal(d, ts + 1), -1);
        assert_eq!(date_cmp_timestamp_internal(d, ts - 1), 1);
        let beyond = TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE;
        assert_eq!(date_cmp_timestamp_internal(beyond, ts), 1);
        assert_eq!(date_cmp_timestamp_internal(beyond, DT_NOEND), -1);
    }

    #[test]
    #[ignore = "needs tzdb via get_share_path (common/path.c) which is not yet ported"]
    fn time_timetz_uses_session_zone_offset() {
        crate::test_install_seams();
        use crate::time::time_in;
        use types_datetime::USECS_PER_HOUR;

        // The session zone is pinned to GMT, so the session-derived offset is 0.
        let t: TimeADT = USECS_PER_HOUR * 12; // 12:00:00
        let r = time_timetz(t);
        assert_eq!(r.time, t, "time-of-day must be preserved verbatim");
        assert_eq!(r.zone, 0, "session zone is GMT -> offset 0");

        let parsed = {
            let _g = DATE_ORDER_TEST_LOCK
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            crate::settings::set_date_style(types_datetime::USE_ISO_DATES);
            time_in("12:00:00", -1).unwrap()
        };
        let r2 = time_timetz(parsed);
        assert_eq!(r2.time, parsed);
        assert_eq!(r2.zone, 0);

        // Pin the date-dependent zone-offset WIRING (DetermineTimeZoneOffset must
        // compute a real offset) WITHOUT depending on ambient tzdb completeness.
        let fixed = backend_timezone_pgtz::pg_tzset_offset(5 * 3600)
            .expect("fixed-offset zone must synthesize from a POSIX TZ string without tzdata")
            .expect("fixed-offset zone must synthesize from a POSIX TZ string without tzdata");
        let mut winter = pg_tm {
            tm_year: 2024,
            tm_mon: 1,
            tm_mday: 15,
            tm_hour: 12,
            ..Default::default()
        };
        let mut summer = pg_tm {
            tm_year: 2024,
            tm_mon: 7,
            tm_mday: 15,
            tm_hour: 12,
            ..Default::default()
        };
        let off_winter = DetermineTimeZoneOffset(&mut winter, &fixed);
        let off_summer = DetermineTimeZoneOffset(&mut summer, &fixed);
        assert_eq!(off_winter.abs(), 5 * 3600, "fixed +/-05 zone -> |offset| = 5h");
        assert_eq!(
            off_winter, off_summer,
            "a fixed-offset zone has no DST -> date-independent offset"
        );
    }

    // --- test helpers that pin DateOrder under the shared lock ---
    fn date_in_mdy(s: &str) -> DateADT {
        let _guard = DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        set_date_order(DATEORDER_MDY);
        date_in(s).unwrap()
    }

    fn date_out_mdy(d: DateADT) -> String {
        let _guard = DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        set_date_order(DATEORDER_MDY);
        date_out(d)
    }
}
