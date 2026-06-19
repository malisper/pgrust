//! TIMESTAMP / TIMESTAMPTZ value cores, ported from
//! `src/backend/utils/adt/timestamp.c` (idiomatic, safe Rust).
//!
//! This module ports the plain-Rust computational cores: the
//! [`timestamp2tm`]/[`tm2timestamp`] conversions (with `pg_localtime` rotation
//! and the POSIX<->PG `tm_year+1900`/`tm_mon+1` adjust at the boundary), the
//! `timestamp_in`/`timestamp_out` cores (DecodeDateTime + EncodeDateTime), the
//! `timestamptz_in`/`_out` cores, [`GetCurrentTimestamp`]/[`SetEpochTimestamp`],
//! the comparison cores, the timestamp +/- interval arithmetic, the `age`
//! cores, [`AdjustTimestampForTypmod`], the timestamp<->timestamptz session-zone
//! rotation, the constructors (`make_timestamp*`/`make_interval`), `date_bin`,
//! `to_timestamp`, the AT TIME ZONE family, and the cross-type conversions.
//!
//! The fmgr `Datum` shims (`PG_FUNCTION_ARGS` wrappers) are NOT ported -- these
//! are the cores those shims would call.  Functions that the C code reports
//! out-of-range from return a `PgError` ([`DtResult`]); the caller (an fmgr
//! shim) maps that to `ereport`.
//!
//! `interval_um_internal` and the `Interval` field/limit helpers live in
//! [`crate::interval`]; this module re-uses them.  The tiny conversion cores
//! (`dt2time`/`time2t`/`dt2local`/`timestamptz_to_time_t`/`IS_VALID_JULIAN`)
//! live in [`crate::convert`] and are re-exported here so callers that expect
//! them in `crate::timestamp` (matching the C `timestamp.c` home) still resolve.
//!
//! Idiomatic surface: plain `i32`/`i64`/`f64`, owned values, `Option`,
//! `Result`, `&str`.  No raw pointers, `extern "C"`, `c_int`, `libc`, `CStr`/
//! `CString`, or `pgrust_pg_ffi`.

use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};


use types_pgtime::{pg_tm, pg_tz};
use types_core::pg_time_t;
use state_pgtz::session_timezone;
use backend_timezone_localtime::{pg_localtime};
use types_datetime::{
    DATETIME_MIN_JULIAN, DAYS_PER_WEEK, DT_NOBEGIN, DT_NOEND, END_TIMESTAMP, HOURS_PER_DAY,
    Interval, MAX_TIMESTAMP_PRECISION, MINS_PER_HOUR, MIN_TIMESTAMP, MONTHS_PER_YEAR,
    POSTGRES_EPOCH_JDATE, SECS_PER_DAY, SECS_PER_MINUTE, TIMESTAMP_END_JULIAN, UNIX_EPOCH_JDATE,
    USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE, USECS_PER_SEC,
};
use types_error::{
    ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use types_datetime::{fsec_t, Timestamp, TimestampTz};
use types_error::{PgError, PgResult};

use crate::calendar::{date2j, isleap, j2date};

// Re-export the shared conversion cores from `crate::convert` so callers that
// reach for them in `crate::timestamp` (their C home, `timestamp.c`) resolve to
// the single canonical, seam-free definitions rather than re-staged copies.
pub use crate::convert::{
    dt2local, dt2time, time2t, timestamptz_to_time_t, IS_VALID_DATE, IS_VALID_JULIAN,
};

// ---------------------------------------------------------------------------
// Error helpers.
//
// The C cores `ereport(ERROR, ...)`; in safe Rust we surface that via the
// project-wide `PgResult` / `PgError` contract.  `DtResult` is an alias; the
// `dt_err_*` constructors mirror the C `errcode(...) + errmsg(...)` pairs.
// ---------------------------------------------------------------------------

/// `Result` alias for the date/time cores: the project-wide `PgResult`.
pub type DtResult<T> = PgResult<T>;

/// "timestamp out of range" (`ERRCODE_DATETIME_VALUE_OUT_OF_RANGE`).
pub fn timestamp_out_of_range() -> PgError {
    PgError::error("timestamp out of range").with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
}

/// "interval out of range" (`ERRCODE_DATETIME_VALUE_OUT_OF_RANGE`).
pub fn interval_out_of_range() -> PgError {
    PgError::error("interval out of range").with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
}

/// A parameter-value error (`ERRCODE_INVALID_PARAMETER_VALUE`).
pub fn invalid_parameter(message: impl Into<String>) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

/// A feature-not-supported error (`ERRCODE_FEATURE_NOT_SUPPORTED`).  C uses this
/// errcode for the date/time `date_trunc`/`date_part` "unit not supported"
/// branches (e.g. timestamp.c:4723, :4814).
pub fn feature_not_supported(message: impl Into<String>) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

/// An internal error mirroring C `elog(ERROR, ...)` (`ERRCODE_INTERNAL_ERROR`).
pub fn internal_error(message: impl Into<String>) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// Map a `DTERR_*` code to a [`PgError`] for a date/time scalar type, a faithful
/// split of C `DateTimeParseError` (datetime.c:4214) with the type-specific
/// `datatype` label for the bad-format case.  Delegates to the single shared
/// mapping in [`crate::date::datetime_parse_error_for`] so the SQLSTATE / message
/// / hint per code stays identical to the date type's.  Shared by the timestamp,
/// timestamptz and interval cores.
pub(crate) fn datetime_parse_error(
    dterr: i32,
    str: &str,
    datatype: &str,
    extra: &types_datetime::DateTimeErrorExtra,
) -> PgError {
    crate::date::datetime_parse_error_for(dterr, str, datatype, extra)
}

// ---------------------------------------------------------------------------
// Header-macro equivalents (datatype/timestamp.h).
// ---------------------------------------------------------------------------

/// `TIMESTAMP_IS_NOBEGIN(j)`.
#[inline]
pub fn TIMESTAMP_IS_NOBEGIN(j: Timestamp) -> bool {
    j == DT_NOBEGIN
}

/// `TIMESTAMP_IS_NOEND(j)`.
#[inline]
pub fn TIMESTAMP_IS_NOEND(j: Timestamp) -> bool {
    j == DT_NOEND
}

/// `TIMESTAMP_NOT_FINITE(j)`.
#[inline]
pub fn TIMESTAMP_NOT_FINITE(j: Timestamp) -> bool {
    TIMESTAMP_IS_NOBEGIN(j) || TIMESTAMP_IS_NOEND(j)
}

/// `IS_VALID_TIMESTAMP(t)`.
#[inline]
pub fn IS_VALID_TIMESTAMP(t: Timestamp) -> bool {
    (MIN_TIMESTAMP..END_TIMESTAMP).contains(&t)
}

// ---------------------------------------------------------------------------
// Overflow-checked integer arithmetic (pg_*_overflow analogues).
// ---------------------------------------------------------------------------

#[inline]
pub(crate) fn pg_add_s64_overflow(a: i64, b: i64, res: &mut i64) -> bool {
    match a.checked_add(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = 0;
            true
        }
    }
}

#[inline]
pub(crate) fn pg_sub_s64_overflow(a: i64, b: i64, res: &mut i64) -> bool {
    match a.checked_sub(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = 0;
            true
        }
    }
}

#[inline]
pub(crate) fn pg_mul_s64_overflow(a: i64, b: i64, res: &mut i64) -> bool {
    match a.checked_mul(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = 0;
            true
        }
    }
}

#[inline]
pub(crate) fn pg_add_s32_overflow(a: i32, b: i32, res: &mut i32) -> bool {
    match a.checked_add(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = 0;
            true
        }
    }
}

#[inline]
pub(crate) fn pg_mul_s32_overflow(a: i32, b: i32, res: &mut i32) -> bool {
    match a.checked_mul(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = 0;
            true
        }
    }
}

// ---------------------------------------------------------------------------
// timestamp2tm / tm2timestamp
// ---------------------------------------------------------------------------

/// `timestamp2tm()` -- convert a `Timestamp`/`TimestampTz` to a broken-down
/// POSIX-style `pg_tm`.
///
/// Note that `tm.tm_year` is the explicit full value (not 1900-based) and
/// `tm.tm_mon` is one-based on output, matching the C contract.  When `tzp` is
/// `Some`, the time is rotated to `attimezone` (or the session zone if `None`)
/// via `pg_localtime`, and `*tzp` is set to the numeric offset; `tzn`, if
/// `Some`, receives the resolved zone abbreviation (or `None` when no zone is
/// available).
///
/// Returns `Ok(())` on success, `Err(())` if out of the supported range
/// (mirroring the C `0` / `-1` return convention).
///
/// (`utils/adt/timestamp.c`)
#[allow(clippy::result_unit_err)]
pub fn timestamp2tm(
    dt: Timestamp,
    tzp: Option<&mut i32>,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tzn: Option<&mut Option<String>>,
    attimezone: Option<&pg_tz>,
) -> Result<(), ()> {
    // Use session timezone if caller asks for default.
    let session_tz;
    let attimezone: &pg_tz = match attimezone {
        Some(z) => z,
        None => {
            session_tz = session_timezone();
            &session_tz
        }
    };

    let mut time = dt;
    // TMODULO(time, date, USECS_PER_DAY)
    let mut date: Timestamp = time / USECS_PER_DAY;
    if date != 0 {
        time -= date * USECS_PER_DAY;
    }

    if time < 0 {
        time += USECS_PER_DAY;
        date -= 1;
    }

    /* add offset to go from J2000 back to standard Julian date */
    date += POSTGRES_EPOCH_JDATE as i64;

    /* Julian day routine does not work for negative Julian days */
    if date < 0 || date > i32::MAX as i64 {
        return Err(());
    }

    let (y, mo, d) = j2date(date as i32);
    tm.tm_year = y;
    tm.tm_mon = mo;
    tm.tm_mday = d;
    dt2time(time, &mut tm.tm_hour, &mut tm.tm_min, &mut tm.tm_sec, fsec);

    /* Done if no TZ conversion wanted */
    let tzp = match tzp {
        None => {
            tm.tm_isdst = -1;
            tm.tm_gmtoff = 0;
            tm.tm_zone = None;
            if let Some(slot) = tzn {
                *slot = None;
            }
            return Ok(());
        }
        Some(p) => p,
    };

    /*
     * If the time falls within the range of pg_time_t, use pg_localtime() to
     * rotate to the local time zone.
     */
    let dt_secs = (dt - *fsec as i64) / USECS_PER_SEC
        + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY as i64;
    let utime: pg_time_t = dt_secs;
    if utime == dt_secs {
        if let Some(tx) = pg_localtime(utime, attimezone) {
            // pg_localtime returns tm_year as (year-1900) and tm_mon 0-based;
            // datetime.c uses the full year / 1-based month at this boundary.
            tm.tm_year = tx.tm_year + 1900;
            tm.tm_mon = tx.tm_mon + 1;
            tm.tm_mday = tx.tm_mday;
            tm.tm_hour = tx.tm_hour;
            tm.tm_min = tx.tm_min;
            tm.tm_sec = tx.tm_sec;
            tm.tm_isdst = tx.tm_isdst;
            tm.tm_gmtoff = tx.tm_gmtoff;
            tm.tm_zone = tx.tm_zone.clone();
            *tzp = -(tm.tm_gmtoff as i32);
            if let Some(slot) = tzn {
                *slot = tx.tm_zone;
            }
            return Ok(());
        }
    }

    /* When out of range of pg_time_t, treat as GMT */
    *tzp = 0;
    tm.tm_isdst = -1;
    tm.tm_gmtoff = 0;
    tm.tm_zone = None;
    if let Some(slot) = tzn {
        *slot = None;
    }
    Ok(())
}

/// `tm2timestamp()` -- convert a broken-down `pg_tm` (+ fsec) to a `Timestamp`.
///
/// `tm.tm_year` is the explicit full value and `tm.tm_mon` is one-based, per
/// the C contract.  Returns `Err(())` on overflow / out-of-range (the C `-1`).
///
/// (`utils/adt/timestamp.c`)
#[allow(clippy::result_unit_err)]
pub fn tm2timestamp(
    tm: &pg_tm,
    fsec: fsec_t,
    tzp: Option<i32>,
    result: &mut Timestamp,
) -> Result<(), ()> {
    /* Prevent overflow in Julian-day routines */
    if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        *result = 0;
        return Err(());
    }

    let date = (date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE) as i64;
    let time = time2t(tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);

    if pg_mul_s64_overflow(date, USECS_PER_DAY, result)
        || pg_add_s64_overflow(*result, time, result)
    {
        *result = 0;
        return Err(());
    }
    if let Some(tz) = tzp {
        *result = dt2local(*result, -tz);
    }

    /* final range check catches just-out-of-range timestamps */
    if !IS_VALID_TIMESTAMP(*result) {
        *result = 0;
        return Err(());
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// timestamp_in / timestamp_out cores
// ---------------------------------------------------------------------------

/// `timestamp_in()` core: parse `str` into a `Timestamp`, applying `typmod`.
///
/// This is the body of `timestamp_in` minus the fmgr glue / error-context
/// reporting; the `DecodeDateTime` step uses the session decode engine.  A
/// decode failure (DTERR_*) is surfaced as a `PgError`.
///
/// (`utils/adt/timestamp.c`)
pub fn timestamp_in(str: &str, typmod: i32) -> DtResult<Timestamp> {
    use types_datetime::{DTK_DATE, DTK_EARLY, DTK_EPOCH, DTK_LATE, MAXDATEFIELDS, MAXDATELEN};

    let mut fsec: fsec_t = 0;
    let mut tm = pg_tm::default();
    let mut tz: i32 = 0;
    let mut dtype: i32 = 0;
    let mut nf: usize = 0;
    let mut field: Vec<String> = Vec::new();
    let mut ftype: Vec<i32> = Vec::new();
    let mut extra = types_datetime::DateTimeErrorExtra::default();

    // C timestamp_in: workbuf[MAXDATELEN + MAXDATEFIELDS] (timestamp.c:184).
    let mut dterr = crate::decode::ParseDateTime(
        str,
        MAXDATELEN as usize + MAXDATEFIELDS as usize,
        &mut field,
        &mut ftype,
        MAXDATEFIELDS as usize,
        &mut nf,
    );
    if dterr == 0 {
        dterr = crate::decode::DecodeDateTime(
            &mut field,
            &mut ftype,
            nf,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tz),
            &mut extra,
        );
    }
    if dterr != 0 {
        return Err(datetime_parse_error(dterr, str, "timestamp", &extra));
    }

    let mut result: Timestamp = match dtype {
        DTK_DATE => {
            let mut r = 0;
            if tm2timestamp(&tm, fsec, None, &mut r).is_err() {
                return Err(PgError::error(format!("timestamp out of range: \"{str}\""))
                    .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
            }
            r
        }
        DTK_EPOCH => SetEpochTimestamp(),
        DTK_LATE => DT_NOEND,
        DTK_EARLY => DT_NOBEGIN,
        _ => {
            return Err(internal_error(format!(
                "unexpected dtype {dtype} while parsing timestamp \"{str}\""
            )))
        }
    };

    AdjustTimestampForTypmod(&mut result, typmod)?;
    Ok(result)
}

/// `timestamp_out()` core: format a `Timestamp` to its textual form using the
/// session `DateStyle`.  (`utils/adt/timestamp.c`)
pub fn timestamp_out(timestamp: Timestamp) -> DtResult<String> {
    let mut buf = String::new();
    if TIMESTAMP_NOT_FINITE(timestamp) {
        crate::encode::EncodeSpecialTimestamp(timestamp, &mut buf);
    } else {
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_ok() {
            crate::encode::EncodeDateTime(
                &mut tm,
                fsec,
                false,
                0,
                None,
                crate::settings::date_style(),
                &mut buf,
            );
        } else {
            return Err(timestamp_out_of_range());
        }
    }
    Ok(buf)
}

// ---------------------------------------------------------------------------
// timestamptz_in / timestamptz_out cores
// ---------------------------------------------------------------------------

/// `timestamptz_in()` core: parse `str` into a `TimestampTz`, applying `typmod`.
///
/// (`utils/adt/timestamp.c`)
pub fn timestamptz_in(str: &str, typmod: i32) -> DtResult<TimestampTz> {
    use types_datetime::{DTK_DATE, DTK_EARLY, DTK_EPOCH, DTK_LATE, MAXDATEFIELDS, MAXDATELEN};

    let mut fsec: fsec_t = 0;
    let mut tm = pg_tm::default();
    let mut tz: i32 = 0;
    let mut dtype: i32 = 0;
    let mut nf: usize = 0;
    let mut field: Vec<String> = Vec::new();
    let mut ftype: Vec<i32> = Vec::new();
    let mut extra = types_datetime::DateTimeErrorExtra::default();

    // C timestamptz_in: workbuf[MAXDATELEN + MAXDATEFIELDS] (timestamp.c:436).
    let mut dterr = crate::decode::ParseDateTime(
        str,
        MAXDATELEN as usize + MAXDATEFIELDS as usize,
        &mut field,
        &mut ftype,
        MAXDATEFIELDS as usize,
        &mut nf,
    );
    if dterr == 0 {
        dterr = crate::decode::DecodeDateTime(
            &mut field,
            &mut ftype,
            nf,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tz),
            &mut extra,
        );
    }
    if dterr != 0 {
        return Err(datetime_parse_error(dterr, str, "timestamp with time zone", &extra));
    }

    let mut result: TimestampTz = match dtype {
        DTK_DATE => {
            let mut r = 0;
            if tm2timestamp(&tm, fsec, Some(tz), &mut r).is_err() {
                return Err(PgError::error(format!("timestamp out of range: \"{str}\""))
                    .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
            }
            r
        }
        DTK_EPOCH => SetEpochTimestamp(),
        DTK_LATE => DT_NOEND,
        DTK_EARLY => DT_NOBEGIN,
        _ => {
            return Err(internal_error(format!(
                "unexpected dtype {dtype} while parsing timestamptz \"{str}\""
            )))
        }
    };

    AdjustTimestampForTypmod(&mut result, typmod)?;
    Ok(result)
}

/// `timestamptz_out()` core: format a `TimestampTz` to its textual form using
/// the session `DateStyle`.  (`utils/adt/timestamp.c`)
pub fn timestamptz_out(dt: TimestampTz) -> DtResult<String> {
    let mut buf = String::new();
    if TIMESTAMP_NOT_FINITE(dt) {
        crate::encode::EncodeSpecialTimestamp(dt, &mut buf);
    } else {
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        let mut tz: i32 = 0;
        let mut tzn: Option<String> = None;
        if timestamp2tm(dt, Some(&mut tz), &mut tm, &mut fsec, Some(&mut tzn), None).is_ok() {
            crate::encode::EncodeDateTime(
                &mut tm,
                fsec,
                true,
                tz,
                tzn.as_deref(),
                crate::settings::date_style(),
                &mut buf,
            );
        } else {
            return Err(timestamp_out_of_range());
        }
    }
    Ok(buf)
}

// ---------------------------------------------------------------------------
// AdjustTimestampForTypmod
// ---------------------------------------------------------------------------

const TIMESTAMP_SCALES: [i64; (MAX_TIMESTAMP_PRECISION + 1) as usize] =
    [1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];
const TIMESTAMP_OFFSETS: [i64; (MAX_TIMESTAMP_PRECISION + 1) as usize] =
    [500_000, 50_000, 5_000, 500, 50, 5, 0];

/// `anytimestamp_typmodin(istz, ta)` (timestamp.c:104) — the `timestamptypmodin`
/// (pg_proc 2905) / `timestamptztypmodin` (pg_proc 2907) core over the already-
/// parsed integer typmod list (the fmgr boundary performs the
/// `ArrayGetIntegerTypmods` cstring→int parse, exactly as the numeric arm does).
pub fn anytimestamp_typmodin(istz: bool, tl: &[i32]) -> DtResult<i32> {
    // C: if (n != 1) ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
    //                        "invalid type modifier");
    if tl.len() != 1 {
        return Err(invalid_parameter("invalid type modifier"));
    }
    anytimestamp_typmod_check(istz, tl[0])
}

/// `anytimestamp_typmodout(istz, typmod)` (timestamp.c) — render a
/// `timestamp`/`timestamptz` typmod as its printable suffix.
pub fn anytimestamp_typmodout(istz: bool, typmod: i32) -> String {
    let tz = if istz {
        " with time zone"
    } else {
        " without time zone"
    };
    if typmod >= 0 {
        format!("({typmod}){tz}")
    } else {
        tz.to_string()
    }
}

/// `anytimestamp_typmod_check(istz, typmod)` (timestamp.c:124) — validate + clamp
/// a TIMESTAMP / TIMESTAMPTZ typmod.  Negative is an error; over-max clamps to
/// MAX_TIMESTAMP_PRECISION (C also emits a WARNING there).
pub fn anytimestamp_typmod_check(istz: bool, typmod: i32) -> DtResult<i32> {
    if typmod < 0 {
        return Err(invalid_parameter(format!(
            "TIMESTAMP({typmod}){} precision must not be negative",
            if istz { " WITH TIME ZONE" } else { "" }
        )));
    }
    if typmod > MAX_TIMESTAMP_PRECISION {
        return Ok(MAX_TIMESTAMP_PRECISION);
    }
    Ok(typmod)
}

/// `AdjustTimestampForTypmod()` -- round a timestamp to suit `typmod`.  Works
/// for either timestamp or timestamptz.  (`utils/adt/timestamp.c`)
pub fn AdjustTimestampForTypmod(time: &mut Timestamp, typmod: i32) -> DtResult<()> {
    if !TIMESTAMP_NOT_FINITE(*time) && typmod != -1 && typmod != MAX_TIMESTAMP_PRECISION {
        if !(0..=MAX_TIMESTAMP_PRECISION).contains(&typmod) {
            return Err(invalid_parameter(format!(
                "timestamp({typmod}) precision must be between {} and {}",
                0, MAX_TIMESTAMP_PRECISION
            )));
        }

        let tp = typmod as usize;
        if *time >= 0 {
            *time = ((*time + TIMESTAMP_OFFSETS[tp]) / TIMESTAMP_SCALES[tp]) * TIMESTAMP_SCALES[tp];
        } else {
            *time = -((((-*time) + TIMESTAMP_OFFSETS[tp]) / TIMESTAMP_SCALES[tp])
                * TIMESTAMP_SCALES[tp]);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// GetCurrentTimestamp / SetEpochTimestamp
// ---------------------------------------------------------------------------

/// `GetCurrentTimestamp()` -- current transaction-independent wall-clock time as
/// a `TimestampTz` (microseconds since the Postgres epoch, 2000-01-01 UTC).
///
/// The C original uses `gettimeofday()`; we use `SystemTime` since the Unix
/// epoch, which is equivalent.  (`utils/adt/timestamp.c`)
pub fn GetCurrentTimestamp() -> TimestampTz {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let tv_sec = dur.as_secs() as i64;
    let tv_usec = dur.subsec_micros() as i64;

    let result = tv_sec - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY as i64;
    result * USECS_PER_SEC + tv_usec
}

/// `SetEpochTimestamp()` -- the `Timestamp` for the Unix epoch (1970-01-01
/// 00:00:00).  (`utils/adt/timestamp.c`)
pub fn SetEpochTimestamp() -> Timestamp {
    // GetEpochTime fills a tm for 1970-01-01 00:00:00; tm2timestamp converts.
    let tm = pg_tm {
        tm_year: 1970,
        tm_mon: 1,
        tm_mday: 1,
        tm_hour: 0,
        tm_min: 0,
        tm_sec: 0,
        ..Default::default()
    };
    let mut dt = 0;
    let _ = tm2timestamp(&tm, 0, None, &mut dt);
    dt
}

// ---------------------------------------------------------------------------
// Comparisons
// ---------------------------------------------------------------------------

/// `timestamp_cmp_internal()` -- three-way compare of two timestamps.
///
/// (`utils/adt/timestamp.c`)
pub fn timestamp_cmp_internal(dt1: Timestamp, dt2: Timestamp) -> i32 {
    if dt1 < dt2 {
        -1
    } else if dt1 > dt2 {
        1
    } else {
        0
    }
}

/// `timestamp_eq` core.
pub fn timestamp_eq(dt1: Timestamp, dt2: Timestamp) -> bool {
    timestamp_cmp_internal(dt1, dt2) == 0
}
/// `timestamp_ne` core.
pub fn timestamp_ne(dt1: Timestamp, dt2: Timestamp) -> bool {
    timestamp_cmp_internal(dt1, dt2) != 0
}
/// `timestamp_lt` core.
pub fn timestamp_lt(dt1: Timestamp, dt2: Timestamp) -> bool {
    timestamp_cmp_internal(dt1, dt2) < 0
}
/// `timestamp_gt` core.
pub fn timestamp_gt(dt1: Timestamp, dt2: Timestamp) -> bool {
    timestamp_cmp_internal(dt1, dt2) > 0
}
/// `timestamp_le` core.
pub fn timestamp_le(dt1: Timestamp, dt2: Timestamp) -> bool {
    timestamp_cmp_internal(dt1, dt2) <= 0
}
/// `timestamp_ge` core.
pub fn timestamp_ge(dt1: Timestamp, dt2: Timestamp) -> bool {
    timestamp_cmp_internal(dt1, dt2) >= 0
}
/// `timestamp_cmp` core.
pub fn timestamp_cmp(dt1: Timestamp, dt2: Timestamp) -> i32 {
    timestamp_cmp_internal(dt1, dt2)
}

/// `timestamp_smaller` core.
pub fn timestamp_smaller(dt1: Timestamp, dt2: Timestamp) -> Timestamp {
    if dt1 < dt2 {
        dt1
    } else {
        dt2
    }
}
/// `timestamp_larger` core.
pub fn timestamp_larger(dt1: Timestamp, dt2: Timestamp) -> Timestamp {
    if dt1 > dt2 {
        dt1
    } else {
        dt2
    }
}

// ---------------------------------------------------------------------------
// timestamp +/- interval  (timestamp_pl_interval / timestamp_mi_interval)
// ---------------------------------------------------------------------------

/// `timestamp_pl_interval()` core -- add `span` to `timestamp`.
///
/// (`utils/adt/timestamp.c`)
pub fn timestamp_pl_interval(mut timestamp: Timestamp, span: &Interval) -> DtResult<Timestamp> {
    use crate::interval::{INTERVAL_IS_NOBEGIN, INTERVAL_IS_NOEND};

    let result: Timestamp;

    if INTERVAL_IS_NOBEGIN(span) {
        if TIMESTAMP_IS_NOEND(timestamp) {
            return Err(timestamp_out_of_range());
        }
        result = DT_NOBEGIN;
    } else if INTERVAL_IS_NOEND(span) {
        if TIMESTAMP_IS_NOBEGIN(timestamp) {
            return Err(timestamp_out_of_range());
        }
        result = DT_NOEND;
    } else if TIMESTAMP_NOT_FINITE(timestamp) {
        result = timestamp;
    } else {
        if span.month != 0 {
            let mut tm = pg_tm::default();
            let mut fsec: fsec_t = 0;

            if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err() {
                return Err(timestamp_out_of_range());
            }

            if pg_add_s32_overflow(tm.tm_mon, span.month, &mut tm.tm_mon) {
                return Err(timestamp_out_of_range());
            }
            if tm.tm_mon > MONTHS_PER_YEAR {
                tm.tm_year += (tm.tm_mon - 1) / MONTHS_PER_YEAR;
                tm.tm_mon = (tm.tm_mon - 1) % MONTHS_PER_YEAR + 1;
            } else if tm.tm_mon < 1 {
                tm.tm_year += tm.tm_mon / MONTHS_PER_YEAR - 1;
                tm.tm_mon = tm.tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
            }

            /* adjust for end of month boundary problems... */
            let mlen = crate::calendar::day_tab[isleap(tm.tm_year)][(tm.tm_mon - 1) as usize];
            if tm.tm_mday > mlen {
                tm.tm_mday = mlen;
            }

            if tm2timestamp(&tm, fsec, None, &mut timestamp).is_err() {
                return Err(timestamp_out_of_range());
            }
        }

        if span.day != 0 {
            let mut tm = pg_tm::default();
            let mut fsec: fsec_t = 0;

            if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err() {
                return Err(timestamp_out_of_range());
            }

            let mut julian = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday);
            if pg_add_s32_overflow(julian, span.day, &mut julian) || julian < 0 {
                return Err(timestamp_out_of_range());
            }
            let (y, mo, d) = j2date(julian);
            tm.tm_year = y;
            tm.tm_mon = mo;
            tm.tm_mday = d;

            if tm2timestamp(&tm, fsec, None, &mut timestamp).is_err() {
                return Err(timestamp_out_of_range());
            }
        }

        if pg_add_s64_overflow(timestamp, span.time, &mut timestamp) {
            return Err(timestamp_out_of_range());
        }

        if !IS_VALID_TIMESTAMP(timestamp) {
            return Err(timestamp_out_of_range());
        }

        result = timestamp;
    }

    Ok(result)
}

/// `timestamp_mi_interval()` core -- subtract `span` from `timestamp`.
///
/// (`utils/adt/timestamp.c`)
pub fn timestamp_mi_interval(timestamp: Timestamp, span: &Interval) -> DtResult<Timestamp> {
    let mut tspan = Interval {
        time: 0,
        day: 0,
        month: 0,
    };
    crate::interval::interval_um_internal(span, &mut tspan)?;
    timestamp_pl_interval(timestamp, &tspan)
}

/// `timestamp_mi()` core -- subtract one timestamp from another, yielding an
/// `Interval`.  (`utils/adt/timestamp.c`)
pub fn timestamp_mi(dt1: Timestamp, dt2: Timestamp) -> DtResult<Interval> {
    if TIMESTAMP_NOT_FINITE(dt1) || TIMESTAMP_NOT_FINITE(dt2) {
        // The C code special-cases infinities; surface as out-of-range here
        // when both are infinite, else builds a NOBEGIN/NOEND interval.
        if TIMESTAMP_IS_NOBEGIN(dt1) {
            if TIMESTAMP_IS_NOBEGIN(dt2) {
                return Err(interval_out_of_range());
            }
            return Ok(crate::interval::interval_nobegin());
        }
        if TIMESTAMP_IS_NOEND(dt1) {
            if TIMESTAMP_IS_NOEND(dt2) {
                return Err(interval_out_of_range());
            }
            return Ok(crate::interval::interval_noend());
        }
        if TIMESTAMP_IS_NOBEGIN(dt2) {
            return Ok(crate::interval::interval_noend());
        }
        if TIMESTAMP_IS_NOEND(dt2) {
            return Ok(crate::interval::interval_nobegin());
        }
    }

    let mut result = Interval {
        time: 0,
        day: 0,
        month: 0,
    };
    if pg_sub_s64_overflow(dt1, dt2, &mut result.time) {
        return Err(interval_out_of_range());
    }
    result.month = 0;
    result.day = 0;

    // result = interval_justify_hours(result)
    crate::interval::interval_justify_hours(&result)
}

// ---------------------------------------------------------------------------
// timestamp_age / timestamptz_age
// ---------------------------------------------------------------------------

/// Shared body of `timestamp_age`/`timestamptz_age`: form the symbolic
/// difference `tm1 - tm2` into an `Interval`.  The `_age` variants only differ
/// in whether `timestamp2tm` is called with a tz pointer; since the resulting
/// `Interval` deliberately ignores any tz difference, the computation is shared.
fn age_common(dt1: Timestamp, dt2: Timestamp, want_tz: bool) -> DtResult<Interval> {
    use crate::interval::{interval_nobegin, interval_noend, itm2interval};
    use types_datetime::pg_itm;

    if TIMESTAMP_IS_NOBEGIN(dt1) {
        if TIMESTAMP_IS_NOBEGIN(dt2) {
            return Err(interval_out_of_range());
        }
        return Ok(interval_nobegin());
    } else if TIMESTAMP_IS_NOEND(dt1) {
        if TIMESTAMP_IS_NOEND(dt2) {
            return Err(interval_out_of_range());
        }
        return Ok(interval_noend());
    } else if TIMESTAMP_IS_NOBEGIN(dt2) {
        return Ok(interval_noend());
    } else if TIMESTAMP_IS_NOEND(dt2) {
        return Ok(interval_nobegin());
    }

    let mut tm1 = pg_tm::default();
    let mut tm2 = pg_tm::default();
    let mut fsec1: fsec_t = 0;
    let mut fsec2: fsec_t = 0;
    let mut tz1: i32 = 0;
    let mut tz2: i32 = 0;

    let r1 = if want_tz {
        timestamp2tm(dt1, Some(&mut tz1), &mut tm1, &mut fsec1, None, None)
    } else {
        timestamp2tm(dt1, None, &mut tm1, &mut fsec1, None, None)
    };
    let r2 = if want_tz {
        timestamp2tm(dt2, Some(&mut tz2), &mut tm2, &mut fsec2, None, None)
    } else {
        timestamp2tm(dt2, None, &mut tm2, &mut fsec2, None, None)
    };

    if r1.is_ok() && r2.is_ok() {
        let mut tm = pg_itm {
            tm_usec: 0,
            tm_sec: 0,
            tm_min: 0,
            tm_hour: 0,
            tm_mday: 0,
            tm_mon: 0,
            tm_year: 0,
        };
        /* form the symbolic difference */
        tm.tm_usec = fsec1 - fsec2;
        tm.tm_sec = tm1.tm_sec - tm2.tm_sec;
        tm.tm_min = tm1.tm_min - tm2.tm_min;
        tm.tm_hour = (tm1.tm_hour - tm2.tm_hour) as i64;
        tm.tm_mday = tm1.tm_mday - tm2.tm_mday;
        tm.tm_mon = tm1.tm_mon - tm2.tm_mon;
        tm.tm_year = tm1.tm_year - tm2.tm_year;

        /* flip sign if necessary... */
        if dt1 < dt2 {
            tm.tm_usec = -tm.tm_usec;
            tm.tm_sec = -tm.tm_sec;
            tm.tm_min = -tm.tm_min;
            tm.tm_hour = -tm.tm_hour;
            tm.tm_mday = -tm.tm_mday;
            tm.tm_mon = -tm.tm_mon;
            tm.tm_year = -tm.tm_year;
        }

        /* propagate any negative fields into the next higher field */
        while tm.tm_usec < 0 {
            tm.tm_usec += USECS_PER_SEC as i32;
            tm.tm_sec -= 1;
        }
        while tm.tm_sec < 0 {
            tm.tm_sec += SECS_PER_MINUTE;
            tm.tm_min -= 1;
        }
        while tm.tm_min < 0 {
            tm.tm_min += MINS_PER_HOUR;
            tm.tm_hour -= 1;
        }
        while tm.tm_hour < 0 {
            tm.tm_hour += HOURS_PER_DAY as i64;
            tm.tm_mday -= 1;
        }
        while tm.tm_mday < 0 {
            if dt1 < dt2 {
                tm.tm_mday +=
                    crate::calendar::day_tab[isleap(tm1.tm_year)][(tm1.tm_mon - 1) as usize];
                tm.tm_mon -= 1;
            } else {
                tm.tm_mday +=
                    crate::calendar::day_tab[isleap(tm2.tm_year)][(tm2.tm_mon - 1) as usize];
                tm.tm_mon -= 1;
            }
        }
        while tm.tm_mon < 0 {
            tm.tm_mon += MONTHS_PER_YEAR;
            tm.tm_year -= 1;
        }

        /* recover sign if necessary... */
        if dt1 < dt2 {
            tm.tm_usec = -tm.tm_usec;
            tm.tm_sec = -tm.tm_sec;
            tm.tm_min = -tm.tm_min;
            tm.tm_hour = -tm.tm_hour;
            tm.tm_mday = -tm.tm_mday;
            tm.tm_mon = -tm.tm_mon;
            tm.tm_year = -tm.tm_year;
        }

        let mut result = Interval {
            time: 0,
            day: 0,
            month: 0,
        };
        if itm2interval(&tm, &mut result).is_err() {
            return Err(interval_out_of_range());
        }
        Ok(result)
    } else {
        Err(timestamp_out_of_range())
    }
}

/// `timestamp_age()` core -- symbolic difference `dt1 - dt2` (no tz).
///
/// (`utils/adt/timestamp.c`)
pub fn timestamp_age(dt1: Timestamp, dt2: Timestamp) -> DtResult<Interval> {
    age_common(dt1, dt2, false)
}

/// `timestamptz_age()` core -- symbolic difference `dt1 - dt2`, rotating each
/// operand to local time first (the tz difference is deliberately ignored, per
/// the C original).  (`utils/adt/timestamp.c`)
pub fn timestamptz_age(dt1: TimestampTz, dt2: TimestampTz) -> DtResult<Interval> {
    age_common(dt1, dt2, true)
}

// ---------------------------------------------------------------------------
// timestamp <-> timestamptz session-zone rotation cores.
// ---------------------------------------------------------------------------

/// `timestamp2timestamptz_opt_overflow()` core, parametrized over the target
/// `pg_tz` (timestamp.c:6466).  Threading an explicit `tz` (rather than always
/// reading `session_timezone()`) makes the genuine `overflow < 0` (east-of-GMT)
/// branch reachable for testing.
fn timestamp2timestamptz_opt_overflow_tz(
    timestamp: Timestamp,
    tz_zone: &pg_tz,
) -> DtResult<(TimestampTz, i32)> {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok((timestamp, 0));
    }

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    // We don't expect this to fail, but check it pro forma.  In C the failure
    // path falls through to ereport(ERROR) regardless of the overflow pointer.
    if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_ok() {
        let tz = crate::decode::DetermineTimeZoneOffset(&mut tm, tz_zone);
        let result = dt2local(timestamp, -tz);
        if IS_VALID_TIMESTAMP(result) {
            return Ok((result, 0));
        }
        if result < MIN_TIMESTAMP {
            return Ok((DT_NOBEGIN, -1));
        }
        return Ok((DT_NOEND, 1));
    }

    Err(timestamp_out_of_range())
}

/// `timestamp2timestamptz_opt_overflow()` (timestamp.c:6466) -- convert a local
/// `Timestamp` to a `TimestampTz` by interpreting it in the session zone.
///
/// `overflow` is `0` on success, `+1`/`-1` if the rotated value falls outside
/// the valid timestamp range (with the corresponding infinity returned).  Non-
/// finite inputs pass through unchanged with `overflow = 0`.
///
/// The C original's `timestamp2tm`-failure path is an `ereport(ERROR)`.  This
/// `(value, overflow)` API has no error channel, so that case is encoded as the
/// poison overflow value `2` with a `0` value; the throwing wrappers turn it
/// into the same hard error C raises.
pub fn timestamp2timestamptz_opt_overflow(timestamp: Timestamp) -> (TimestampTz, i32) {
    timestamp2timestamptz_opt_overflow_tz(timestamp, &session_timezone()).unwrap_or((0, 2))
}

/// `timestamp2timestamptz()` (timestamp.c:6518) -- promote a `Timestamp` to a
/// `TimestampTz` in the session zone, throwing an out-of-range error on
/// overflow (the C `static` helper that passes `NULL` as the overflow pointer).
pub fn timestamp2timestamptz(timestamp: Timestamp) -> DtResult<TimestampTz> {
    let (result, overflow) = timestamp2timestamptz_opt_overflow_tz(timestamp, &session_timezone())?;
    if overflow != 0 {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

/// `timestamptz2timestamp()` (timestamp.c:6535) -- convert a `TimestampTz` to a
/// local `Timestamp` by rotating it into the session zone and dropping the zone.
pub fn timestamptz2timestamp(timestamp: TimestampTz) -> DtResult<Timestamp> {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    if timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }
    let mut result: Timestamp = 0;
    if tm2timestamp(&tm, fsec, None, &mut result).is_err() {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Cross-type comparison: timestamp vs timestamptz (timestamp.c:2383)
// ---------------------------------------------------------------------------

/// `timestamp_cmp_timestamptz_internal()` core, parametrized over the `pg_tz`
/// the lhs `Timestamp` is interpreted in (timestamp.c:2383).
fn timestamp_cmp_timestamptz_tz(
    timestamp_val: Timestamp,
    dt2: TimestampTz,
    tz_zone: &pg_tz,
) -> DtResult<i32> {
    let (dt1, overflow) = timestamp2timestamptz_opt_overflow_tz(timestamp_val, tz_zone)?;
    if overflow > 0 {
        // dt1 is larger than any finite timestamp, but less than infinity.
        return Ok(if TIMESTAMP_IS_NOEND(dt2) { -1 } else { 1 });
    }
    if overflow < 0 {
        // dt1 is less than any finite timestamp, but more than -infinity.
        return Ok(if TIMESTAMP_IS_NOBEGIN(dt2) { 1 } else { -1 });
    }

    // timestamptz_cmp_internal is identical to timestamp_cmp_internal.
    Ok(timestamp_cmp_internal(dt1, dt2))
}

/// `timestamp_cmp_timestamptz_internal()` (timestamp.c:2383) -- cross-type
/// comparison of a `Timestamp` (interpreted in the session zone) against a
/// `TimestampTz`.  Returns `-1`/`0`/`+1`.
pub fn timestamp_cmp_timestamptz_internal(timestamp_val: Timestamp, dt2: TimestampTz) -> i32 {
    timestamp_cmp_timestamptz_tz(timestamp_val, dt2, &session_timezone())
        .expect("timestamp2tm cannot fail for a finite timestamp (timestamp.c:6480)")
}

// ---------------------------------------------------------------------------
// GetSQLCurrentTimestamp / GetSQLLocalTimestamp (timestamp.c:1662 / 1676)
// ---------------------------------------------------------------------------

/// `GetSQLCurrentTimestamp()` (timestamp.c:1662) CORE -- implements
/// `CURRENT_TIMESTAMP`, `CURRENT_TIMESTAMP(n)`.
pub fn GetSQLCurrentTimestamp(typmod: i32) -> DtResult<TimestampTz> {
    let mut ts = backend_access_transam_xact::GetCurrentTransactionStartTimestamp();
    if typmod >= 0 {
        AdjustTimestampForTypmod(&mut ts, typmod)?;
    }
    Ok(ts)
}

/// `GetSQLLocalTimestamp()` (timestamp.c:1676) CORE -- implements
/// `LOCALTIMESTAMP`, `LOCALTIMESTAMP(n)`.
pub fn GetSQLLocalTimestamp(typmod: i32) -> DtResult<Timestamp> {
    let mut ts =
        timestamptz2timestamp(backend_access_transam_xact::GetCurrentTransactionStartTimestamp())?;
    if typmod >= 0 {
        AdjustTimestampForTypmod(&mut ts, typmod)?;
    }
    Ok(ts)
}

// ---------------------------------------------------------------------------
// make_timestamp_internal / make_timestamp / make_timestamptz
// ---------------------------------------------------------------------------

/// `make_timestamp_internal()` (timestamp.c:573) -- workhorse for
/// `make_timestamp` and `make_timestamptz`: build a `Timestamp` from
/// year/month/day/hour/min/sec, with the C field range checks and overflow
/// guards.  Negative years are interpreted as BC.
pub fn make_timestamp_internal(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: f64,
) -> DtResult<Timestamp> {
    use crate::consts::DTK_DATE_M;
    use crate::decode::ValidateDate;
    use crate::time::float_time_overflows;

    let mut tm = pg_tm {
        tm_year: year,
        tm_mon: month,
        tm_mday: day,
        ..Default::default()
    };

    // Handle negative years as BC.
    let mut bc = false;
    if tm.tm_year < 0 {
        bc = true;
        tm.tm_year = -tm.tm_year;
    }

    let dterr = ValidateDate(DTK_DATE_M, false, false, bc, &mut tm);
    if dterr != 0 {
        return Err(PgError::error(format!(
            "date field value out of range: {year}-{month:02}-{day:02}"
        ))
        .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW));
    }

    if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        return Err(
            PgError::error(format!("date out of range: {year}-{month:02}-{day:02}"))
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        );
    }

    let date: i64 = (date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE) as i64;

    // C formats the seconds field with %02g (= %g with the `0` flag and min
    // field width 2), which zero-pads short outputs (5.0 -> "05", 0.0 -> "00"),
    // not a plain fixed-point pad.  Computed lazily so the success path (which
    // never reads it) pays no allocation.
    let sec_field = || {
        let sec_g = fmt_g(sec);
        if sec_g.len() < 2 {
            format!("{sec_g:0>2}")
        } else {
            sec_g
        }
    };

    // Check for time overflow.
    if float_time_overflows(hour, min, sec) {
        // C: errmsg("time field value out of range: %d:%02d:%02g", ...).
        return Err(PgError::error(format!(
            "time field value out of range: {hour}:{min:02}:{}",
            sec_field()
        ))
        .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW));
    }

    // This should match tm2time.
    let time: i64 = (((hour as i64 * MINS_PER_HOUR as i64 + min as i64) * SECS_PER_MINUTE as i64)
        * USECS_PER_SEC)
        + (sec * USECS_PER_SEC as f64).round_ties_even() as i64;

    let mut result: Timestamp = 0;
    if pg_mul_s64_overflow(date, USECS_PER_DAY, &mut result)
        || pg_add_s64_overflow(result, time, &mut result)
    {
        // C: errmsg("timestamp out of range: %d-%02d-%02d %d:%02d:%02g", ...).
        return Err(PgError::error(format!(
            "timestamp out of range: {year}-{month:02}-{day:02} {hour}:{min:02}:{}",
            sec_field()
        ))
        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }

    // Final range check catches just-out-of-range timestamps.
    if !IS_VALID_TIMESTAMP(result) {
        return Err(PgError::error(format!(
            "timestamp out of range: {year}-{month:02}-{day:02} {hour}:{min:02}:{}",
            sec_field()
        ))
        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }

    Ok(result)
}

/// `make_timestamp()` (timestamp.c:644) CORE -- timestamp constructor.
pub fn make_timestamp(
    year: i32,
    month: i32,
    mday: i32,
    hour: i32,
    min: i32,
    sec: f64,
) -> DtResult<Timestamp> {
    make_timestamp_internal(year, month, mday, hour, min, sec)
}

/// `make_timestamptz()` (timestamp.c:664) CORE -- timestamp-with-time-zone
/// constructor in the session zone.
pub fn make_timestamptz(
    year: i32,
    month: i32,
    mday: i32,
    hour: i32,
    min: i32,
    sec: f64,
) -> DtResult<TimestampTz> {
    let result = make_timestamp_internal(year, month, mday, hour, min, sec)?;
    timestamp2timestamptz(result)
}

/// `parse_sane_timezone()` (timestamp.c:490) -- look up the requested timezone
/// for the local time in `tm`, returning the GMT offset (seconds, internal
/// `dt2local` sign convention).
fn parse_sane_timezone(tm: &mut pg_tm, zone: &str) -> DtResult<i32> {
    use types_datetime::{
        DTERR_BAD_FORMAT, DTERR_TZDISP_OVERFLOW, TZNAME_DYNTZ, TZNAME_FIXED_OFFSET,
    };

    let tzname = zone;

    // A digit in the first position would let pg_tzset accept input that should
    // be seen as invalid.
    if tzname.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        return Err(PgError::error(format!(
            "invalid input syntax for type {}: \"{}\"",
            "numeric time zone", tzname
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .with_hint("Numeric time zones must have \"-\" or \"+\" as first character."));
    }

    let mut tz: i32 = 0;
    let dterr = crate::decode::DecodeTimezone(tzname, &mut tz);
    if dterr != 0 {
        if dterr == DTERR_TZDISP_OVERFLOW {
            return Err(
                PgError::error(format!("numeric time zone \"{tzname}\" out of range"))
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
            );
        } else if dterr != DTERR_BAD_FORMAT {
            return Err(
                PgError::error(format!("time zone \"{tzname}\" not recognized"))
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
            );
        }

        let mut val: i32 = 0;
        let mut tzp: Option<Rc<pg_tz>> = None;
        let type_ = crate::decode::DecodeTimezoneName(tzname, &mut val, &mut tzp)?;

        if type_ == TZNAME_FIXED_OFFSET {
            // fixed-offset abbreviation
            tz = -val;
        } else if type_ == TZNAME_DYNTZ {
            // dynamic-offset abbreviation, resolve using specified time
            tz = crate::decode::DetermineTimeZoneAbbrevOffset(
                tm,
                tzname,
                &tzp.expect("DecodeTimezoneName sets *tz for DYNTZ"),
            );
        } else {
            // full zone name
            tz = crate::decode::DetermineTimeZoneOffset(
                tm,
                &tzp.expect("DecodeTimezoneName sets *tz for ZONE"),
            );
        }
    }

    Ok(tz)
}

/// `make_timestamptz_at_timezone()` (timestamp.c:686) CORE -- as
/// [`make_timestamptz`], but the time zone is given as a name in `zone`.
pub fn make_timestamptz_at_timezone(
    year: i32,
    month: i32,
    mday: i32,
    hour: i32,
    min: i32,
    sec: f64,
    zone: &str,
) -> DtResult<TimestampTz> {
    let timestamp = make_timestamp_internal(year, month, mday, hour, min, sec)?;

    let mut tt = pg_tm::default();
    let mut fsec: fsec_t = 0;
    if timestamp2tm(timestamp, None, &mut tt, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }

    let tz = parse_sane_timezone(&mut tt, zone)?;

    let result = dt2local(timestamp, -tz);

    if !IS_VALID_TIMESTAMP(result) {
        return Err(timestamp_out_of_range());
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// make_interval
// ---------------------------------------------------------------------------

/// `make_interval()` (timestamp.c:1529) CORE -- numeric Interval constructor with
/// overflow-checked field arithmetic.  Rejects any input that would overflow the
/// corresponding interval field.
pub fn make_interval(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hours: i32,
    mins: i32,
    secs: f64,
) -> DtResult<Interval> {
    use crate::interval::INTERVAL_NOT_FINITE;

    // Reject out-of-range inputs that cause integer overflow of the fields.
    if secs.is_infinite() || secs.is_nan() {
        return Err(interval_out_of_range());
    }

    let mut result = Interval {
        time: 0,
        day: 0,
        month: 0,
    };

    // years and months -> months.
    if pg_mul_s32_overflow(years, MONTHS_PER_YEAR, &mut result.month)
        || pg_add_s32_overflow(result.month, months, &mut result.month)
    {
        return Err(interval_out_of_range());
    }

    // weeks and days -> days.
    if pg_mul_s32_overflow(weeks, DAYS_PER_WEEK, &mut result.day)
        || pg_add_s32_overflow(result.day, days, &mut result.day)
    {
        return Err(interval_out_of_range());
    }

    // hours and mins -> usecs (cannot overflow 64-bit).
    result.time = hours as i64 * USECS_PER_HOUR + mins as i64 * USECS_PER_MINUTE;

    // secs -> usecs.  C's float8_mul throws "value out of range: overflow" on a
    // non-finite product; mirror that distinct error.
    let secs_usec = float8_mul(secs, USECS_PER_SEC as f64)?.round_ties_even();
    if !float8_fits_in_int64(secs_usec)
        || pg_add_s64_overflow(result.time, secs_usec as i64, &mut result.time)
    {
        return Err(interval_out_of_range());
    }

    // Make sure that the result is finite.
    if INTERVAL_NOT_FINITE(&result) {
        return Err(interval_out_of_range());
    }

    Ok(result)
}

/// `float8_mul()` (float.h) -- multiply two doubles, raising "value out of range:
/// overflow" if a finite product overflows to infinity.
#[inline]
fn float8_mul(a: f64, b: f64) -> DtResult<f64> {
    let result = a * b;
    if result.is_infinite() && a.is_finite() && b.is_finite() {
        // C float_overflow_error() uses ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE.
        return Err(PgError::error("value out of range: overflow")
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
    }
    Ok(result)
}

/// `FLOAT8_FITS_IN_INT64(num)` (c.h) -- does `num` fit when truncated to i64.
#[inline]
fn float8_fits_in_int64(num: f64) -> bool {
    num >= i64::MIN as f64 && num < -(i64::MIN as f64)
}

// ---------------------------------------------------------------------------
// float8_timestamptz  (to_timestamp(double precision))
// ---------------------------------------------------------------------------

/// `float8_timestamptz()` (timestamp.c:726) CORE -- `to_timestamp(double
/// precision)`: convert a UNIX epoch seconds value to a `TimestampTz`.
pub fn float8_timestamptz(seconds: f64) -> DtResult<TimestampTz> {
    // Deal with NaN and infinite inputs.
    if seconds.is_nan() {
        return Err(PgError::error("timestamp cannot be NaN")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }

    if seconds.is_infinite() {
        return Ok(if seconds < 0.0 { DT_NOBEGIN } else { DT_NOEND });
    }

    // Out of range?
    if seconds < SECS_PER_DAY as f64 * (DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE) as f64
        || seconds >= SECS_PER_DAY as f64 * (TIMESTAMP_END_JULIAN - UNIX_EPOCH_JDATE) as f64
    {
        return Err(
            PgError::error(format!("timestamp out of range: \"{}\"", fmt_g(seconds)))
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        );
    }

    // Convert UNIX epoch to Postgres epoch.
    let shifted =
        seconds - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as f64 * SECS_PER_DAY as f64);
    let usec = (shifted * USECS_PER_SEC as f64).round_ties_even();
    let result = usec as i64;

    // Recheck in case roundoff produces something just out of range.
    if !IS_VALID_TIMESTAMP(result) {
        return Err(
            PgError::error(format!("timestamp out of range: \"{}\"", fmt_g(seconds)))
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        );
    }

    Ok(result)
}

/// Render a `f64` the way C's `printf("%g", v)` does (default precision 6).
///
/// C `%g` chooses `%e` style when the decimal exponent is `< -4` or `>=
/// precision`, otherwise `%f` style, then strips trailing zeros and a trailing
/// decimal point.  These out-of-range datetime error messages embed the value
/// with `%g`, so e.g. `to_timestamp(1e16)` must render `"1e+16"`, not the full
/// decimal expansion.
pub(crate) fn fmt_g(v: f64) -> String {
    fmt_g_prec(v, 6)
}

/// C `%.*g` with the given (significant-digit) precision.  A precision of 0 is
/// treated as 1, matching C.
fn fmt_g_prec(v: f64, precision: usize) -> String {
    let prec = precision.max(1);

    if v == 0.0 {
        // C %g of -0.0 prints "-0", so honor the sign bit.
        return if v.is_sign_negative() {
            "-0".to_string()
        } else {
            "0".to_string()
        };
    }
    if v.is_nan() {
        return "nan".to_string();
    }
    if v.is_infinite() {
        return if v < 0.0 { "-inf" } else { "inf" }.to_string();
    }

    // Round to `prec` significant digits, then decide style from the resulting
    // decimal exponent (matching glibc: the exponent is taken after rounding).
    let neg = v < 0.0;
    let mag = v.abs();

    // Format in scientific with prec-1 fractional digits to recover the rounded
    // mantissa and exponent.
    let sci = format!("{:.*e}", prec - 1, mag); // e.g. "1.00000e16"
    let (mantissa_str, exp_str) = sci.split_once('e').expect("scientific form has 'e'");
    let exp: i32 = exp_str.parse().expect("valid exponent");

    let body = if exp < -4 || exp >= prec as i32 {
        // %e style: mantissa with trailing zeros stripped, exponent "e+NN"/"e-NN"
        // (at least two digits).
        let mantissa = strip_trailing_zeros(mantissa_str);
        let exp_sign = if exp < 0 { '-' } else { '+' };
        format!("{mantissa}e{exp_sign}{:02}", exp.unsigned_abs())
    } else {
        // %f style with (prec - 1 - exp) fractional digits, then strip zeros.
        let frac_digits = (prec as i32 - 1 - exp).max(0) as usize;
        let f = format!("{:.*}", frac_digits, mag);
        strip_trailing_zeros(&f)
    };

    if neg {
        format!("-{body}")
    } else {
        body
    }
}

/// Strip trailing zeros (and a trailing '.') from a fixed/scientific mantissa
/// string, matching C `%g`'s suppression of insignificant trailing zeros.
fn strip_trailing_zeros(s: &str) -> String {
    if !s.contains('.') {
        return s.to_string();
    }
    let trimmed = s.trim_end_matches('0');
    let trimmed = trimmed.trim_end_matches('.');
    trimmed.to_string()
}

// ---------------------------------------------------------------------------
// timestamp_bin / timestamptz_bin  (date_bin)
// ---------------------------------------------------------------------------

/// Shared body of `timestamp_bin`/`timestamptz_bin` (timestamp.c:4606 /
/// timestamp.c:4841): bin a timestamp into a `stride` interval measured from
/// `origin`, rounding toward negative infinity.  The two C functions are
/// byte-for-byte identical, so a single core serves both.
fn timestamp_bin_common(
    stride: &Interval,
    timestamp: Timestamp,
    origin: Timestamp,
) -> DtResult<Timestamp> {
    use crate::interval::INTERVAL_NOT_FINITE;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }

    if TIMESTAMP_NOT_FINITE(origin) {
        return Err(PgError::error("origin out of range")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }

    if INTERVAL_NOT_FINITE(stride) {
        return Err(
            PgError::error("timestamps cannot be binned into infinite intervals")
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        );
    }

    if stride.month != 0 {
        return Err(PgError::error(
            "timestamps cannot be binned into intervals containing months or years",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    let mut stride_usecs: i64 = 0;
    if pg_mul_s64_overflow(stride.day as i64, USECS_PER_DAY, &mut stride_usecs)
        || pg_add_s64_overflow(stride_usecs, stride.time, &mut stride_usecs)
    {
        return Err(interval_out_of_range());
    }

    if stride_usecs <= 0 {
        return Err(PgError::error("stride must be greater than zero")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }

    let mut tm_diff: i64 = 0;
    if pg_sub_s64_overflow(timestamp, origin, &mut tm_diff) {
        return Err(interval_out_of_range());
    }

    // These calculations cannot overflow.
    let tm_modulo = tm_diff % stride_usecs;
    let tm_delta = tm_diff - tm_modulo;
    let mut result = origin + tm_delta;

    // Round towards -infinity, not 0, when tm_diff is negative and not a multiple
    // of stride_usecs.  This adjustment *can* overflow.
    if tm_modulo < 0
        && (pg_sub_s64_overflow(result, stride_usecs, &mut result) || !IS_VALID_TIMESTAMP(result))
    {
        return Err(timestamp_out_of_range());
    }

    Ok(result)
}

/// `timestamp_bin()` (timestamp.c:4606) CORE -- `date_bin` for timestamp.
pub fn timestamp_bin(
    stride: &Interval,
    timestamp: Timestamp,
    origin: Timestamp,
) -> DtResult<Timestamp> {
    timestamp_bin_common(stride, timestamp, origin)
}

/// `timestamptz_bin()` (timestamp.c:4841) CORE -- `date_bin` for timestamptz.
pub fn timestamptz_bin(
    stride: &Interval,
    timestamp: TimestampTz,
    origin: TimestampTz,
) -> DtResult<TimestampTz> {
    timestamp_bin_common(stride, timestamp, origin)
}

// ---------------------------------------------------------------------------
// timestamptz +/- interval  (session-zone path)
// ---------------------------------------------------------------------------

/// `timestamptz_pl_interval_internal()` (timestamp.c:3251) CORE.  Add `span` to a
/// `TimestampTz` honoring the qualitative month/day units by rotating to local
/// time, mutating the broken-down `tm`, re-resolving the zone offset via
/// `DetermineTimeZoneOffset`, and reassembling.  `attimezone` selects the zone:
/// `None` uses the session zone (C: `attimezone == NULL`), `Some(z)` uses the
/// given named zone (C: the `*_at_zone` callers).
pub fn timestamptz_pl_interval_internal(
    mut timestamp: TimestampTz,
    span: &Interval,
    attimezone: Option<&pg_tz>,
) -> DtResult<TimestampTz> {
    use crate::interval::{INTERVAL_IS_NOBEGIN, INTERVAL_IS_NOEND};

    // Handle infinities.  "infinity - infinity" is an error.
    if INTERVAL_IS_NOBEGIN(span) {
        if TIMESTAMP_IS_NOEND(timestamp) {
            return Err(timestamp_out_of_range());
        }
        return Ok(DT_NOBEGIN);
    }
    if INTERVAL_IS_NOEND(span) {
        if TIMESTAMP_IS_NOBEGIN(timestamp) {
            return Err(timestamp_out_of_range());
        }
        return Ok(DT_NOEND);
    }
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }

    // Use session timezone if caller asks for default.
    let session_tz;
    let attimezone: &pg_tz = match attimezone {
        Some(z) => z,
        None => {
            session_tz = session_timezone();
            &session_tz
        }
    };

    if span.month != 0 {
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        let mut tz: i32 = 0;

        if timestamp2tm(
            timestamp,
            Some(&mut tz),
            &mut tm,
            &mut fsec,
            None,
            Some(attimezone),
        )
        .is_err()
        {
            return Err(timestamp_out_of_range());
        }

        if pg_add_s32_overflow(tm.tm_mon, span.month, &mut tm.tm_mon) {
            return Err(timestamp_out_of_range());
        }
        if tm.tm_mon > MONTHS_PER_YEAR {
            tm.tm_year += (tm.tm_mon - 1) / MONTHS_PER_YEAR;
            tm.tm_mon = (tm.tm_mon - 1) % MONTHS_PER_YEAR + 1;
        } else if tm.tm_mon < 1 {
            tm.tm_year += tm.tm_mon / MONTHS_PER_YEAR - 1;
            tm.tm_mon = tm.tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
        }

        // Adjust for end of month boundary problems...
        let mlen = crate::calendar::day_tab[isleap(tm.tm_year)][(tm.tm_mon - 1) as usize];
        if tm.tm_mday > mlen {
            tm.tm_mday = mlen;
        }

        let tz = crate::decode::DetermineTimeZoneOffset(&mut tm, attimezone);

        if tm2timestamp(&tm, fsec, Some(tz), &mut timestamp).is_err() {
            return Err(timestamp_out_of_range());
        }
    }

    if span.day != 0 {
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        let mut tz: i32 = 0;

        if timestamp2tm(
            timestamp,
            Some(&mut tz),
            &mut tm,
            &mut fsec,
            None,
            Some(attimezone),
        )
        .is_err()
        {
            return Err(timestamp_out_of_range());
        }

        // Add days by converting to and from Julian.  -1 is allowed (see
        // timestamp.h) to avoid timezone-dependent failures.
        let mut julian = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday);
        if pg_add_s32_overflow(julian, span.day, &mut julian) || julian < -1 {
            return Err(timestamp_out_of_range());
        }
        let (y, mo, d) = j2date(julian);
        tm.tm_year = y;
        tm.tm_mon = mo;
        tm.tm_mday = d;

        let tz = crate::decode::DetermineTimeZoneOffset(&mut tm, attimezone);

        if tm2timestamp(&tm, fsec, Some(tz), &mut timestamp).is_err() {
            return Err(timestamp_out_of_range());
        }
    }

    if pg_add_s64_overflow(timestamp, span.time, &mut timestamp) {
        return Err(timestamp_out_of_range());
    }

    if !IS_VALID_TIMESTAMP(timestamp) {
        return Err(timestamp_out_of_range());
    }

    Ok(timestamp)
}

/// `timestamptz_mi_interval_internal()` (timestamp.c:3383) CORE: subtract `span`
/// from a `TimestampTz` in `attimezone` (`None` == session zone).
pub fn timestamptz_mi_interval_internal(
    timestamp: TimestampTz,
    span: &Interval,
    attimezone: Option<&pg_tz>,
) -> DtResult<TimestampTz> {
    let mut tspan = Interval {
        time: 0,
        day: 0,
        month: 0,
    };
    crate::interval::interval_um_internal(span, &mut tspan)?;
    timestamptz_pl_interval_internal(timestamp, &tspan, attimezone)
}

/// `timestamptz_pl_interval()` (timestamp.c:3398) CORE -- add `span` to a
/// `TimestampTz` in the session zone.
pub fn timestamptz_pl_interval(timestamp: TimestampTz, span: &Interval) -> DtResult<TimestampTz> {
    timestamptz_pl_interval_internal(timestamp, span, None)
}

/// `timestamptz_mi_interval()` (timestamp.c:3407) CORE -- subtract `span` from a
/// `TimestampTz` in the session zone.
pub fn timestamptz_mi_interval(timestamp: TimestampTz, span: &Interval) -> DtResult<TimestampTz> {
    timestamptz_mi_interval_internal(timestamp, span, None)
}

/// `timestamptz_pl_interval_at_zone()` (timestamp.c:3419) CORE -- add `span` to a
/// `TimestampTz` in the named zone `zone`.
pub fn timestamptz_pl_interval_at_zone(
    timestamp: TimestampTz,
    span: &Interval,
    zone: &str,
) -> DtResult<TimestampTz> {
    let attimezone = crate::decode::DecodeTimezoneNameToTz(zone)?;
    timestamptz_pl_interval_internal(timestamp, span, Some(&attimezone))
}

/// `timestamptz_mi_interval_at_zone()` (timestamp.c:3430) CORE -- subtract `span`
/// from a `TimestampTz` in the named zone `zone`.
pub fn timestamptz_mi_interval_at_zone(
    timestamp: TimestampTz,
    span: &Interval,
    zone: &str,
) -> DtResult<TimestampTz> {
    let attimezone = crate::decode::DecodeTimezoneNameToTz(zone)?;
    timestamptz_mi_interval_internal(timestamp, span, Some(&attimezone))
}

// ---------------------------------------------------------------------------
// timestamptz_trunc  (session-zone) / timestamptz_trunc_zone  (named zone)
// ---------------------------------------------------------------------------

/// `timestamptz_trunc()` (timestamp.c:5092) CORE -- `date_trunc` on a
/// `TimestampTz` in the session zone.  `lowunits` is the already-downcased unit.
pub fn timestamptz_trunc(lowunits: &str, timestamp: TimestampTz) -> DtResult<TimestampTz> {
    timestamptz_trunc_internal(lowunits, timestamp, &session_timezone())
}

/// `timestamptz_trunc_zone()` (timestamp.c:5108) CORE -- `date_trunc` on a
/// `TimestampTz` with respect to the named zone `zone`.
pub fn timestamptz_trunc_zone(
    lowunits: &str,
    timestamp: TimestampTz,
    zone: &str,
) -> DtResult<TimestampTz> {
    let tzp = crate::decode::DecodeTimezoneNameToTz(zone)?;
    timestamptz_trunc_internal(lowunits, timestamp, &tzp)
}

/// `timestamptz_trunc_internal()` (timestamp.c:4915) CORE: `date_trunc` on a
/// `TimestampTz` with respect to zone `tzp`, rotating to local time, truncating,
/// then re-resolving the zone offset (when truncating to DAY or coarser, or to
/// WEEK) before reassembling.  `lowunits` is the already-downcased unit string.
pub fn timestamptz_trunc_internal(
    lowunits: &str,
    timestamp: TimestampTz,
    tzp: &pg_tz,
) -> DtResult<TimestampTz> {
    use crate::decode::{DecodeUnits, DetermineTimeZoneOffset};
    use types_datetime::{
        DTK_CENTURY, DTK_DAY, DTK_DECADE, DTK_HOUR, DTK_MICROSEC, DTK_MILLENNIUM, DTK_MILLISEC,
        DTK_MINUTE, DTK_MONTH, DTK_QUARTER, DTK_SECOND, DTK_WEEK, DTK_YEAR, UNITS,
    };

    // C: format_type_be(TIMESTAMPTZOID).
    const TYPE_NAME: &str = "timestamp with time zone";

    let mut val: i32 = 0;
    let type_ = DecodeUnits(0, lowunits, &mut val);

    if type_ != UNITS {
        return Err(invalid_parameter(format!(
            "unit \"{lowunits}\" not recognized for type {TYPE_NAME}"
        )));
    }

    if TIMESTAMP_NOT_FINITE(timestamp) {
        // Errors here must match the finite path's; the listed units pass
        // infinity through unchanged.
        match val {
            DTK_WEEK | DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER
            | DTK_MONTH | DTK_DAY | DTK_HOUR | DTK_MINUTE | DTK_SECOND | DTK_MILLISEC
            | DTK_MICROSEC => return Ok(timestamp),
            _ => {
                return Err(feature_not_supported(format!(
                    "unit \"{lowunits}\" not supported for type {TYPE_NAME}"
                )));
            }
        }
    }

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    if timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, Some(tzp)).is_err() {
        return Err(timestamp_out_of_range());
    }

    // redotz is set for DTK_WEEK and for all cases >= DAY (i.e. DTK_DAY plus the
    // coarser year/quarter/month fields).
    let redotz = matches!(
        val,
        DTK_WEEK
            | DTK_MILLENNIUM
            | DTK_CENTURY
            | DTK_DECADE
            | DTK_YEAR
            | DTK_QUARTER
            | DTK_MONTH
            | DTK_DAY
    );

    crate::extract::timestamp_trunc_apply_pub(val, &mut tm, &mut fsec, lowunits, TYPE_NAME)?;

    if redotz {
        tz = DetermineTimeZoneOffset(&mut tm, tzp);
    }

    let mut result: TimestampTz = 0;
    if tm2timestamp(&tm, fsec, Some(tz), &mut result).is_err() {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// timestamp_izone / timestamptz_izone  (fixed-offset AT TIME ZONE)
// ---------------------------------------------------------------------------

/// `timestamp_izone()` (timestamp.c:6391) CORE -- re-express a local `Timestamp`
/// at the fixed offset given by an `Interval`, yielding a `TimestampTz`.
pub fn timestamp_izone(zone: &Interval, timestamp: Timestamp) -> DtResult<TimestampTz> {
    use crate::interval::INTERVAL_NOT_FINITE;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }

    if INTERVAL_NOT_FINITE(zone) {
        return Err(PgError::error(format!(
            "interval time zone \"{}\" must be finite",
            crate::interval::interval_out(zone)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    if zone.month != 0 || zone.day != 0 {
        return Err(PgError::error(format!(
            "interval time zone \"{}\" must not include months or days",
            crate::interval::interval_out(zone)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    let tz = (zone.time / USECS_PER_SEC) as i32;

    let result = dt2local(timestamp, tz);

    if !IS_VALID_TIMESTAMP(result) {
        return Err(timestamp_out_of_range());
    }

    Ok(result)
}

/// `timestamptz_izone()` (timestamp.c:6628) CORE -- re-express a `TimestampTz` at
/// the fixed offset given by an `Interval`, yielding a local `Timestamp`.
pub fn timestamptz_izone(zone: &Interval, timestamp: TimestampTz) -> DtResult<Timestamp> {
    use crate::interval::INTERVAL_NOT_FINITE;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }

    if INTERVAL_NOT_FINITE(zone) {
        return Err(PgError::error(format!(
            "interval time zone \"{}\" must be finite",
            crate::interval::interval_out(zone)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    if zone.month != 0 || zone.day != 0 {
        return Err(PgError::error(format!(
            "interval time zone \"{}\" must not include months or days",
            crate::interval::interval_out(zone)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // C: tz = -(zone->time / USECS_PER_SEC) with wrapping signed negation.
    let tz = ((zone.time / USECS_PER_SEC) as i32).wrapping_neg();

    let result = dt2local(timestamp, tz);

    if !IS_VALID_TIMESTAMP(result) {
        return Err(timestamp_out_of_range());
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// timestamp_zone / timestamptz_zone / timestamptz_at_local  (named AT TIME ZONE)
// ---------------------------------------------------------------------------

/// `timestamp_zone()` (timestamp.c:6326) CORE -- interpret a local `Timestamp`
/// as being in the named zone `zone`, returning the equivalent `TimestampTz`.
pub fn timestamp_zone(zone: &str, timestamp: Timestamp) -> DtResult<TimestampTz> {
    use crate::decode::{
        DecodeTimezoneName, DetermineTimeZoneAbbrevOffset, DetermineTimeZoneOffset,
    };
    use types_datetime::{TZNAME_DYNTZ, TZNAME_FIXED_OFFSET};

    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }

    let mut val: i32 = 0;
    let mut tzp: Option<Rc<pg_tz>> = None;
    let type_ = DecodeTimezoneName(zone, &mut val, &mut tzp)?;

    let result;
    if type_ == TZNAME_FIXED_OFFSET {
        // fixed-offset abbreviation
        let tz = val;
        result = dt2local(timestamp, tz);
    } else if type_ == TZNAME_DYNTZ {
        // dynamic-offset abbreviation, resolve using specified time
        let tzp = tzp.expect("DecodeTimezoneName sets *tz for DYNTZ");
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, Some(&tzp)).is_err() {
            return Err(timestamp_out_of_range());
        }
        let tz = -DetermineTimeZoneAbbrevOffset(&mut tm, zone, &tzp);
        result = dt2local(timestamp, tz);
    } else {
        // full zone name, rotate to that zone
        let tzp = tzp.expect("DecodeTimezoneName sets *tz for ZONE");
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, Some(&tzp)).is_err() {
            return Err(timestamp_out_of_range());
        }
        let tz = DetermineTimeZoneOffset(&mut tm, &tzp);
        let mut r: TimestampTz = 0;
        if tm2timestamp(&tm, fsec, Some(tz), &mut r).is_err() {
            return Err(timestamp_out_of_range());
        }
        result = r;
    }

    if !IS_VALID_TIMESTAMP(result) {
        return Err(timestamp_out_of_range());
    }

    Ok(result)
}

/// `timestamptz_zone()` (timestamp.c:6564) CORE -- evaluate a `TimestampTz` at
/// the named zone `zone`, returning a local `Timestamp`.
pub fn timestamptz_zone(zone: &str, timestamp: TimestampTz) -> DtResult<Timestamp> {
    use crate::decode::{DecodeTimezoneName, DetermineTimeZoneAbbrevOffsetTS};
    use types_datetime::{TZNAME_DYNTZ, TZNAME_FIXED_OFFSET};

    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }

    let mut val: i32 = 0;
    let mut tzp: Option<Rc<pg_tz>> = None;
    let type_ = DecodeTimezoneName(zone, &mut val, &mut tzp)?;

    let result;
    if type_ == TZNAME_FIXED_OFFSET {
        // fixed-offset abbreviation
        let tz = -val;
        result = dt2local(timestamp, tz);
    } else if type_ == TZNAME_DYNTZ {
        // dynamic-offset abbreviation, resolve using specified time
        let tzp = tzp.expect("DecodeTimezoneName sets *tz for DYNTZ");
        let mut isdst: i32 = 0;
        let tz = DetermineTimeZoneAbbrevOffsetTS(timestamp, zone, &tzp, &mut isdst)?;
        result = dt2local(timestamp, tz);
    } else {
        // full zone name, rotate from that zone
        let tzp = tzp.expect("DecodeTimezoneName sets *tz for ZONE");
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        let mut tz: i32 = 0;
        if timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, Some(&tzp)).is_err() {
            return Err(timestamp_out_of_range());
        }
        let mut r: Timestamp = 0;
        if tm2timestamp(&tm, fsec, None, &mut r).is_err() {
            return Err(timestamp_out_of_range());
        }
        result = r;
    }

    if !IS_VALID_TIMESTAMP(result) {
        return Err(timestamp_out_of_range());
    }

    Ok(result)
}

/// `timestamptz_at_local()` (timestamp.c:6947) CORE -- `AT LOCAL` for a
/// `TimestampTz`: identical to `timestamptz_timestamp` (rotate to the session
/// zone), since the type flips to plain `timestamp`.
pub fn timestamptz_at_local(timestamp: TimestampTz) -> DtResult<Timestamp> {
    timestamptz2timestamp(timestamp)
}

// ---------------------------------------------------------------------------
// Cross-type conversion cores (date.c): timestamp_time / timestamptz_time /
// datetime_timestamp / timestamptz_timetz / datetimetz_timestamptz.
// ---------------------------------------------------------------------------

/// `timestamp_time()` (date.c:1970) CORE -- the time-of-day portion of a
/// `Timestamp`.  `None` mirrors the C `PG_RETURN_NULL()` for non-finite inputs.
pub fn timestamp_time(timestamp: Timestamp) -> DtResult<Option<crate::TimeADT>> {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(None);
    }
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }
    let result = ((((tm.tm_hour as i64 * MINS_PER_HOUR as i64 + tm.tm_min as i64)
        * SECS_PER_MINUTE as i64)
        + tm.tm_sec as i64)
        * USECS_PER_SEC)
        + fsec as i64;
    Ok(Some(result))
}

/// `timestamptz_time()` (date.c:2000) CORE -- the local-time-of-day portion of a
/// `TimestampTz` (rotated into the session zone).  `None` for non-finite inputs.
pub fn timestamptz_time(timestamp: TimestampTz) -> DtResult<Option<crate::TimeADT>> {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(None);
    }
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    if timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }
    let result = ((((tm.tm_hour as i64 * MINS_PER_HOUR as i64 + tm.tm_min as i64)
        * SECS_PER_MINUTE as i64)
        + tm.tm_sec as i64)
        * USECS_PER_SEC)
        + fsec as i64;
    Ok(Some(result))
}

/// `datetime_timestamp()` (date.c:2031) CORE -- combine a date and a time-of-day
/// into a `Timestamp`.
pub fn datetime_timestamp(date: crate::DateADT, time: crate::TimeADT) -> DtResult<Timestamp> {
    let mut result = crate::date::date2timestamp(date)?;
    if !TIMESTAMP_NOT_FINITE(result) {
        result += time;
        if !IS_VALID_TIMESTAMP(result) {
            return Err(timestamp_out_of_range());
        }
    }
    Ok(result)
}

/// `timestamptz_timetz()` (date.c:2919) CORE -- the local time-with-zone portion
/// of a `TimestampTz`.  `None` for non-finite inputs.
pub fn timestamptz_timetz(timestamp: TimestampTz) -> DtResult<Option<crate::TimeTzADT>> {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(None);
    }
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    if timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }
    Ok(Some(crate::timetz::tm2timetz(&tm, fsec, tz)))
}

/// `datetimetz_timestamptz()` (date.c:2951) CORE -- combine a date and a
/// time-with-zone into a `TimestampTz` (stored in GMT, adding the timetz's zone).
pub fn datetimetz_timestamptz(
    date: crate::DateADT,
    time: &crate::TimeTzADT,
) -> DtResult<TimestampTz> {
    use crate::date::{DATE_IS_NOBEGIN, DATE_IS_NOEND};

    if DATE_IS_NOBEGIN(date) {
        return Ok(DT_NOBEGIN);
    }
    if DATE_IS_NOEND(date) {
        return Ok(DT_NOEND);
    }

    // Date's range is wider than timestamp's; only the upper boundary needs the
    // overflow check (dates share the same minimum value as timestamps).
    if date >= (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE) {
        return Err(PgError::error("date out of range for timestamp")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }
    let result = date as TimestampTz * USECS_PER_DAY + time.time + time.zone as i64 * USECS_PER_SEC;

    // Going beyond the timestamptz range because of the time zone is possible.
    if !IS_VALID_TIMESTAMP(result) {
        return Err(PgError::error("date out of range for timestamp")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dt2time_time2t_round_trip() {
        // 10:30:45.123456 == this many usecs into the day.
        let usecs = time2t(10, 30, 45, 123_456);
        let mut h = 0;
        let mut m = 0;
        let mut s = 0;
        let mut f: fsec_t = 0;
        dt2time(usecs, &mut h, &mut m, &mut s, &mut f);
        assert_eq!((h, m, s, f), (10, 30, 45, 123_456));
    }

    #[test]
    fn set_epoch_timestamp_is_unix_epoch() {
        // 1970-01-01 in Postgres usecs == (2440588 - 2451545) days * USECS_PER_DAY.
        let expected = (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE) as i64 * USECS_PER_DAY;
        assert_eq!(SetEpochTimestamp(), expected);
    }

    #[test]
    fn tm2timestamp_timestamp2tm_round_trip_no_tz() {
        let tm = pg_tm {
            tm_year: 2024,
            tm_mon: 1,
            tm_mday: 15,
            tm_hour: 10,
            tm_min: 30,
            tm_sec: 45,
            ..Default::default()
        };
        let mut ts = 0;
        tm2timestamp(&tm, 123_456, None, &mut ts).unwrap();

        let mut out = pg_tm::default();
        let mut fsec: fsec_t = 0;
        timestamp2tm(ts, None, &mut out, &mut fsec, None, None).unwrap();
        assert_eq!(out.tm_year, 2024);
        assert_eq!(out.tm_mon, 1);
        assert_eq!(out.tm_mday, 15);
        assert_eq!(out.tm_hour, 10);
        assert_eq!(out.tm_min, 30);
        assert_eq!(out.tm_sec, 45);
        assert_eq!(fsec, 123_456);
    }

    fn iso_lock() -> std::sync::MutexGuard<'static, ()> {
        let g = crate::settings::DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        crate::settings::set_date_style(types_datetime::USE_ISO_DATES);
        g
    }

    #[test]
    fn timestamp_in_out_round_trip() {
        let _guard = iso_lock();
        let ts = timestamp_in("2024-01-15 10:30:45", -1).unwrap();
        let s = timestamp_out(ts).unwrap();
        assert_eq!(s, "2024-01-15 10:30:45");
    }

    #[test]
    fn timestamp_in_out_with_fraction() {
        let _guard = iso_lock();
        let ts = timestamp_in("2024-01-15 10:30:45.5", -1).unwrap();
        let s = timestamp_out(ts).unwrap();
        assert_eq!(s, "2024-01-15 10:30:45.5");
    }

    #[test]
    fn get_current_timestamp_is_plausible() {
        let now = GetCurrentTimestamp();
        let mut y2020 = 0;
        let tm = pg_tm {
            tm_year: 2020,
            tm_mon: 1,
            tm_mday: 1,
            ..Default::default()
        };
        tm2timestamp(&tm, 0, None, &mut y2020).unwrap();
        assert!(now > y2020, "GetCurrentTimestamp should be after 2020");
    }

    #[test]
    fn timestamp_plus_interval_days() {
        let _g = iso_lock();
        let ts = timestamp_in("2024-01-15 10:30:45", -1).unwrap();
        let span = Interval {
            time: 0,
            day: 10,
            month: 0,
        };
        let r = timestamp_pl_interval(ts, &span).unwrap();
        assert_eq!(timestamp_out(r).unwrap(), "2024-01-25 10:30:45");
    }

    #[test]
    fn timestamp_plus_interval_months_clamps_end_of_month() {
        let _g = iso_lock();
        let ts = timestamp_in("2024-01-31 00:00:00", -1).unwrap();
        let span = Interval {
            time: 0,
            day: 0,
            month: 1,
        };
        let r = timestamp_pl_interval(ts, &span).unwrap();
        assert_eq!(timestamp_out(r).unwrap(), "2024-02-29 00:00:00");
    }

    #[test]
    fn adjust_timestamp_for_typmod_rounds() {
        let _g = iso_lock();
        let mut ts = timestamp_in("2024-01-15 10:30:45.4", -1).unwrap();
        AdjustTimestampForTypmod(&mut ts, 0).unwrap();
        assert_eq!(timestamp_out(ts).unwrap(), "2024-01-15 10:30:45");
    }

    #[test]
    fn date_bin_matches_pg_regress() {
        let _g = iso_lock();
        let ts = timestamp_in("2020-02-11 15:44:17.71393", -1).unwrap();
        let origin = timestamp_in("2001-01-01", -1).unwrap();

        let cases = [
            ("15 days", "2020-02-06 00:00:00"),
            ("2 hours", "2020-02-11 14:00:00"),
            ("1 hour 30 minutes", "2020-02-11 15:00:00"),
            ("15 minutes", "2020-02-11 15:30:00"),
            ("10 seconds", "2020-02-11 15:44:10"),
            ("100 milliseconds", "2020-02-11 15:44:17.7"),
            ("250 microseconds", "2020-02-11 15:44:17.71375"),
        ];
        for (stride_str, want) in cases {
            let stride = crate::interval::interval_in(stride_str, -1).unwrap();
            let r = timestamp_bin(&stride, ts, origin).unwrap();
            assert_eq!(timestamp_out(r).unwrap(), want, "stride {stride_str}");
        }
    }

    #[test]
    fn date_bin_rejects_months_and_zero_and_negative() {
        let _g = iso_lock();
        let ts = timestamp_in("2020-02-01 01:01:01", -1).unwrap();
        let origin = timestamp_in("2001-01-01", -1).unwrap();

        let months = crate::interval::interval_in("5 months", -1).unwrap();
        let err = timestamp_bin(&months, ts, origin).unwrap_err();
        assert_eq!(
            err.message(),
            "timestamps cannot be binned into intervals containing months or years"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);

        let zero = crate::interval::interval_in("0 days", -1).unwrap();
        let err = timestamp_bin(&zero, ts, origin).unwrap_err();
        assert_eq!(err.message(), "stride must be greater than zero");

        let neg = crate::interval::interval_in("-2 days", -1).unwrap();
        let err = timestamp_bin(&neg, ts, origin).unwrap_err();
        assert_eq!(err.message(), "stride must be greater than zero");
    }

    #[test]
    fn make_timestamp_matches_pg_regress() {
        let _g = iso_lock();
        let r = make_timestamp(2014, 12, 28, 6, 30, 45.887).unwrap();
        assert_eq!(timestamp_out(r).unwrap(), "2014-12-28 06:30:45.887");

        let bc = make_timestamp(-44, 3, 15, 12, 30, 15.0).unwrap();
        assert_eq!(timestamp_out(bc).unwrap(), "0044-03-15 12:30:15 BC");

        let h24 = make_timestamp(1999, 12, 31, 24, 0, 0.0).unwrap();
        assert_eq!(timestamp_out(h24).unwrap(), "2000-01-01 00:00:00");
    }

    #[test]
    fn make_timestamp_year_zero_errors() {
        let err = make_timestamp(0, 7, 15, 12, 30, 15.0).unwrap_err();
        assert_eq!(err.message(), "date field value out of range: 0-07-15");
    }

    #[test]
    fn make_interval_overflow_cases() {
        assert_eq!(
            make_interval(178956971, 0, 0, 0, 0, 0, 0.0)
                .unwrap_err()
                .message(),
            "interval out of range"
        );
        assert_eq!(
            make_interval(0, 0, 306783379, 0, 0, 0, 0.0)
                .unwrap_err()
                .message(),
            "interval out of range"
        );
        assert_eq!(
            make_interval(0, 0, 0, 0, 0, 0, 1e308)
                .unwrap_err()
                .message(),
            "value out of range: overflow"
        );
        assert_eq!(
            make_interval(0, 0, 0, 0, 0, 0, 1e18).unwrap_err().message(),
            "interval out of range"
        );
        assert_eq!(
            make_interval(0, 0, 0, 0, 0, 0, f64::INFINITY)
                .unwrap_err()
                .message(),
            "interval out of range"
        );
        assert_eq!(
            make_interval(0, 0, 0, 0, 0, 0, f64::NAN)
                .unwrap_err()
                .message(),
            "interval out of range"
        );
    }

    #[test]
    fn float8_mul_overflow_sqlstate_is_22003() {
        let err = make_interval(0, 0, 0, 0, 0, 0, 1e308).unwrap_err();
        assert_eq!(err.message(), "value out of range: overflow");
        assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }

    #[test]
    fn float8_timestamptz_matches_pg_regress() {
        assert_eq!(float8_timestamptz(0.0).unwrap(), SetEpochTimestamp());
        assert_eq!(float8_timestamptz(946684800.0).unwrap(), 0);

        let r = float8_timestamptz(1262349296.7890123).unwrap();
        let _g = iso_lock();
        assert_eq!(timestamp_out(r).unwrap(), "2010-01-01 12:34:56.789012");
    }

    #[test]
    fn float8_timestamptz_nonfinite() {
        assert_eq!(float8_timestamptz(f64::INFINITY).unwrap(), DT_NOEND);
        assert_eq!(float8_timestamptz(f64::NEG_INFINITY).unwrap(), DT_NOBEGIN);
        let err = float8_timestamptz(f64::NAN).unwrap_err();
        assert_eq!(err.message(), "timestamp cannot be NaN");
    }

    #[test]
    fn fmt_g_matches_c_printf() {
        assert_eq!(fmt_g(1e16), "1e+16");
        assert_eq!(fmt_g(9_224_320_000_000.0), "9.22432e+12");
        assert_eq!(fmt_g(-210_867_000_000.0), "-2.10867e+11");
        assert_eq!(fmt_g(1e7), "1e+07");
        assert_eq!(fmt_g(1_234_567.0), "1.23457e+06");
        assert_eq!(fmt_g(60.5), "60.5");
        assert_eq!(fmt_g(99.0), "99");
        assert_eq!(fmt_g(0.5), "0.5");
        assert_eq!(fmt_g(0.0001), "0.0001");
        assert_eq!(fmt_g(0.00001), "1e-05");
        assert_eq!(fmt_g(100_000.0), "100000");
        assert_eq!(fmt_g(1_000_000.0), "1e+06");
        assert_eq!(fmt_g(123_456.0), "123456");
        assert_eq!(fmt_g(0.1), "0.1");
        assert_eq!(fmt_g(0.0), "0");
        assert_eq!(fmt_g(-0.0), "-0");
    }

    #[test]
    fn float8_timestamptz_out_of_range_message_uses_g() {
        let err = float8_timestamptz(1e16).unwrap_err();
        assert_eq!(err.message(), "timestamp out of range: \"1e+16\"");
        assert_eq!(err.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
    }

    #[test]
    fn make_timestamp_time_field_out_of_range_uses_g() {
        let err = make_timestamp_internal(2020, 1, 1, 0, 0, 1e7).unwrap_err();
        assert_eq!(err.message(), "time field value out of range: 0:00:1e+07");
        assert_eq!(err.sqlstate(), ERRCODE_DATETIME_FIELD_OVERFLOW);
    }

    #[test]
    fn make_timestamp_out_of_range_seconds_use_g() {
        let err = make_timestamp_internal(294277, 12, 31, 23, 59, 6.5000001).unwrap_err();
        assert_eq!(
            err.message(),
            "timestamp out of range: 294277-12-31 23:59:6.5"
        );
        assert_eq!(err.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
    }

    #[test]
    fn make_timestamp_out_of_range_zero_pads_whole_seconds() {
        let err = make_timestamp_internal(294277, 12, 31, 23, 59, 0.0).unwrap_err();
        assert_eq!(
            err.message(),
            "timestamp out of range: 294277-12-31 23:59:00"
        );
        assert_eq!(err.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
    }

    #[test]
    fn timestamptz_trunc_unit_errors_match_c() {
        let _g = iso_lock();
        let ts = timestamptz_in("2024-03-15 10:30:45+00", -1).unwrap();

        let err = timestamptz_trunc("fortnight", ts).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"fortnight\" not recognized for type timestamp with time zone"
        );
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);

        let err = timestamptz_trunc("timezone", ts).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"timezone\" not supported for type timestamp with time zone"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);

        let err = timestamptz_trunc("timezone", DT_NOEND).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"timezone\" not supported for type timestamp with time zone"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    }

    #[test]
    fn timestamptz_pl_mi_interval_gmt() {
        let _g = iso_lock();
        let ts = timestamptz_in("2024-01-15 10:30:45+00", -1).unwrap();

        let span_days = Interval {
            time: 0,
            day: 10,
            month: 0,
        };
        let r = timestamptz_pl_interval(ts, &span_days).unwrap();
        assert_eq!(timestamptz_out(r).unwrap(), "2024-01-25 10:30:45+00");

        let eom = timestamptz_in("2024-01-31 00:00:00+00", -1).unwrap();
        let span_mon = Interval {
            time: 0,
            day: 0,
            month: 1,
        };
        let r = timestamptz_pl_interval(eom, &span_mon).unwrap();
        assert_eq!(timestamptz_out(r).unwrap(), "2024-02-29 00:00:00+00");

        let back =
            timestamptz_mi_interval(timestamptz_pl_interval(ts, &span_days).unwrap(), &span_days)
                .unwrap();
        assert_eq!(back, ts);
    }

    #[test]
    fn timestamptz_trunc_gmt() {
        let _g = iso_lock();
        let ts = timestamptz_in("2024-03-15 10:30:45.123456+00", -1).unwrap();

        let r = timestamptz_trunc("day", ts).unwrap();
        assert_eq!(timestamptz_out(r).unwrap(), "2024-03-15 00:00:00+00");

        let r = timestamptz_trunc("month", ts).unwrap();
        assert_eq!(timestamptz_out(r).unwrap(), "2024-03-01 00:00:00+00");

        let r = timestamptz_trunc("hour", ts).unwrap();
        assert_eq!(timestamptz_out(r).unwrap(), "2024-03-15 10:00:00+00");

        let r = timestamptz_trunc("year", ts).unwrap();
        assert_eq!(timestamptz_out(r).unwrap(), "2024-01-01 00:00:00+00");

        assert_eq!(timestamptz_trunc("day", DT_NOEND).unwrap(), DT_NOEND);
    }

    #[test]
    fn timestamp_izone_fixed_offset() {
        let _g = iso_lock();
        let ts = timestamp_in("2024-01-15 12:00:00", -1).unwrap();
        let zone = Interval {
            month: 0,
            day: 0,
            time: USECS_PER_HOUR,
        };
        let r = timestamp_izone(&zone, ts).unwrap();
        assert_eq!(timestamptz_out(r).unwrap(), "2024-01-15 11:00:00+00");
    }

    #[test]
    fn timestamptz_izone_fixed_offset() {
        let _g = iso_lock();
        let ts = timestamptz_in("2024-01-15 12:00:00+00", -1).unwrap();
        let zone = Interval {
            month: 0,
            day: 0,
            time: USECS_PER_HOUR,
        };
        let r = timestamptz_izone(&zone, ts).unwrap();
        assert_eq!(timestamp_out(r).unwrap(), "2024-01-15 13:00:00");
    }

    #[test]
    fn timestamptz_izone_wraps_without_panic() {
        let ts: TimestampTz = i64::MIN + 1;
        let zone = Interval {
            month: 0,
            day: 0,
            time: -500_000 * USECS_PER_SEC,
        };
        let err = timestamptz_izone(&zone, ts).unwrap_err();
        assert_eq!(err.message(), "timestamp out of range");
        assert_eq!(err.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
    }

    #[test]
    fn izone_rejects_months_or_days_with_rendered_interval() {
        let _g = crate::settings::DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        crate::settings::set_interval_style(types_datetime::INTSTYLE_POSTGRES);
        let ts = timestamp_in("2024-01-15 12:00:00", -1).unwrap();
        let zone = Interval {
            month: 1,
            day: 0,
            time: 0,
        };
        let err = timestamp_izone(&zone, ts).unwrap_err();
        assert_eq!(
            err.message(),
            "interval time zone \"1 mon\" must not include months or days"
        );
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    }

    #[test]
    fn cross_type_conversions() {
        let _g = iso_lock();
        let ts = timestamp_in("2024-03-15 10:30:45.5", -1).unwrap();

        let t = timestamp_time(ts).unwrap().unwrap();
        assert_eq!(t, time2t(10, 30, 45, 500_000));

        let d = crate::date::timestamp_date(ts).unwrap();
        assert_eq!(crate::date::date_out(d), "2024-03-15");

        let recombined = datetime_timestamp(d, t).unwrap();
        assert_eq!(recombined, ts);

        assert!(timestamp_time(DT_NOEND).unwrap().is_none());

        let tstz = timestamptz_in("2024-03-15 10:30:45.5+00", -1).unwrap();
        assert_eq!(crate::date::timestamptz_date(tstz).unwrap(), d);
        assert_eq!(timestamptz_time(tstz).unwrap().unwrap(), t);

        let ttz = timestamptz_timetz(tstz).unwrap().unwrap();
        assert_eq!(ttz.time, t);
        assert_eq!(ttz.zone, 0);

        let back = datetimetz_timestamptz(d, &ttz).unwrap();
        assert_eq!(back, tstz);
    }

    #[test]
    fn parse_error_sqlstates_match_c() {
        use types_datetime::{
            DTERR_BAD_FORMAT, DTERR_FIELD_OVERFLOW, DTERR_INTERVAL_OVERFLOW,
            DTERR_MD_FIELD_OVERFLOW, DTERR_TZDISP_OVERFLOW,
        };
        use types_error::{
            ERRCODE_INTERVAL_FIELD_OVERFLOW, ERRCODE_INVALID_DATETIME_FORMAT,
            ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
        };

        let e = datetime_parse_error(DTERR_FIELD_OVERFLOW, "x", "timestamp", &types_datetime::DateTimeErrorExtra::default());
        assert_eq!(e.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
        assert_eq!(e.message(), "date/time field value out of range: \"x\"");
        assert_eq!(e.hint(), None);

        let e = datetime_parse_error(DTERR_MD_FIELD_OVERFLOW, "x", "timestamp", &types_datetime::DateTimeErrorExtra::default());
        assert_eq!(e.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
        assert_eq!(e.message(), "date/time field value out of range: \"x\"");
        assert_eq!(
            e.hint(),
            Some("Perhaps you need a different \"DateStyle\" setting.")
        );

        let e = datetime_parse_error(DTERR_INTERVAL_OVERFLOW, "x", "interval", &types_datetime::DateTimeErrorExtra::default());
        assert_eq!(e.sqlstate(), ERRCODE_INTERVAL_FIELD_OVERFLOW);
        assert_eq!(e.message(), "interval field value out of range: \"x\"");

        let e = datetime_parse_error(DTERR_TZDISP_OVERFLOW, "x", "timestamp with time zone", &types_datetime::DateTimeErrorExtra::default());
        assert_eq!(e.sqlstate(), ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE);
        assert_eq!(e.message(), "time zone displacement out of range: \"x\"");

        let e = datetime_parse_error(DTERR_BAD_FORMAT, "x", "timestamp", &types_datetime::DateTimeErrorExtra::default());
        assert_eq!(e.sqlstate(), ERRCODE_INVALID_DATETIME_FORMAT);
        assert_eq!(
            e.message(),
            "invalid input syntax for type timestamp: \"x\""
        );
    }

    #[test]
    fn timestamp_cmp_timestamptz_in_range_matches_convert_then_cmp() {
        let ts = timestamp_in("2024-06-15 12:00:00", -1).unwrap();
        let (dt1, overflow) = timestamp2timestamptz_opt_overflow(ts);
        assert_eq!(overflow, 0, "in-range value must not overflow");

        assert_eq!(timestamp_cmp_timestamptz_internal(ts, dt1), 0);
        assert_eq!(
            timestamp_cmp_timestamptz_internal(ts, dt1),
            timestamp_cmp_internal(dt1, dt1),
        );

        assert_eq!(
            timestamp_cmp_timestamptz_internal(ts, dt1 + 1),
            timestamp_cmp_internal(dt1, dt1 + 1),
        );
        assert_eq!(timestamp_cmp_timestamptz_internal(ts, dt1 + 1), -1);

        assert_eq!(
            timestamp_cmp_timestamptz_internal(ts, dt1 - 1),
            timestamp_cmp_internal(dt1, dt1 - 1),
        );
        assert_eq!(timestamp_cmp_timestamptz_internal(ts, dt1 - 1), 1);

        assert_eq!(timestamp_cmp_timestamptz_internal(ts, DT_NOEND), -1);
        assert_eq!(timestamp_cmp_timestamptz_internal(ts, DT_NOBEGIN), 1);
    }

    #[test]
    fn timestamp_cmp_timestamptz_high_overflow_tiebreak() {
        let hv = END_TIMESTAMP; // first value past the valid range
        let (_dt1, overflow) = timestamp2timestamptz_opt_overflow(hv);
        assert_eq!(overflow, 1, "END_TIMESTAMP must report high overflow");

        assert_eq!(timestamp_cmp_timestamptz_internal(hv, DT_NOEND), -1);
        assert_eq!(timestamp_cmp_timestamptz_internal(hv, DT_NOBEGIN), 1);
        assert_eq!(timestamp_cmp_timestamptz_internal(hv, 0), 1);
        let finite = timestamp_in("2024-06-15 12:00:00", -1).unwrap();
        let (fdt, _) = timestamp2timestamptz_opt_overflow(finite);
        assert_eq!(timestamp_cmp_timestamptz_internal(hv, fdt), 1);
    }

    #[test]
    #[ignore = "needs tzdb via get_share_path (common/path.c) which is not yet ported"]
    fn timestamp_cmp_timestamptz_low_overflow_tiebreak() {
        crate::test_install_seams();
        use backend_timezone_pgtz::pg_tzset_offset;

        let east = pg_tzset_offset(-18000)
            .expect("fixed +05:00 zone must build")
            .expect("fixed +05:00 zone must build");

        let (dt1, overflow) =
            super::timestamp2timestamptz_opt_overflow_tz(MIN_TIMESTAMP, &east).unwrap();
        assert_eq!(overflow, -1, "east-of-GMT rotation of MIN must underflow");
        assert_eq!(dt1, DT_NOBEGIN, "underflow returns -infinity");

        let cmp = |dt2: TimestampTz| {
            super::timestamp_cmp_timestamptz_tz(MIN_TIMESTAMP, dt2, &east).unwrap()
        };
        assert_eq!(cmp(DT_NOBEGIN), 1);
        assert_eq!(cmp(DT_NOEND), -1);
        assert_eq!(cmp(0), -1);
        assert_eq!(cmp(MIN_TIMESTAMP), -1);
    }

    #[test]
    #[ignore = "SetParallelStartTimestamps asserts is_parallel_worker; not unit-isolatable"]
    fn get_sql_current_and_local_timestamp() {
        crate::test_install_seams();
        use backend_access_transam_xact::{
            GetCurrentTransactionStartTimestamp, SetParallelStartTimestamps,
        };

        let _g = crate::settings::DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        let pinned = timestamp_in("2024-06-15 12:00:00.123456", -1).unwrap();
        SetParallelStartTimestamps(pinned, pinned);

        let cur_raw = GetSQLCurrentTimestamp(-1).unwrap();
        assert_eq!(cur_raw, GetCurrentTransactionStartTimestamp());
        assert_eq!(cur_raw, pinned);

        let cur_0 = GetSQLCurrentTimestamp(0).unwrap();
        let mut expected = pinned;
        AdjustTimestampForTypmod(&mut expected, 0).unwrap();
        assert_eq!(cur_0, expected);
        assert_ne!(cur_0, pinned, "typmod>=0 must apply rounding");
        assert_eq!(cur_0 % USECS_PER_SEC, 0);

        let cur_3 = GetSQLCurrentTimestamp(3).unwrap();
        let mut expected3 = pinned;
        AdjustTimestampForTypmod(&mut expected3, 3).unwrap();
        assert_eq!(cur_3, expected3);

        let local_raw = GetSQLLocalTimestamp(-1).unwrap();
        assert_eq!(
            local_raw,
            timestamptz2timestamp(GetCurrentTransactionStartTimestamp()).unwrap(),
        );

        let local_0 = GetSQLLocalTimestamp(0).unwrap();
        let mut lexpected = timestamptz2timestamp(pinned).unwrap();
        AdjustTimestampForTypmod(&mut lexpected, 0).unwrap();
        assert_eq!(local_0, lexpected);
        assert_eq!(local_0 % USECS_PER_SEC, 0);

        SetParallelStartTimestamps(0, 0);
    }
}
