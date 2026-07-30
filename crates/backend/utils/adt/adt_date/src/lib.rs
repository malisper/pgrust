//! date.c core: date/time/timetz text I/O over adt_datetime, comparison and
//! arithmetic cores, the date<->timestamp conversions and interval
//! arithmetic/zone rotation through adt_timestamp, and the
//! extract/date_part arms. Zero-allocation I/O like adt_timestamp: parse
//! fields borrow a caller workbuf, output writes into a caller-owned
//! MAXDATELEN buffer. typmod in/out and sortsupport/skipsupport still defer;
//! their OIDs stay out of DATE_BUILTINS so fmgr resolves them to its loud
//! not-ported panic. recv/send ride the binary-wire fmgr frame.

#![allow(non_snake_case)]

use adt_datetime::tz::{self};
use adt_datetime::{
    date2isoweek, date2isoyear, date2j, fsec_t, j2date, j2day, pg_tm, DateTimeErrorExtra,
    DateTimeParseError, DecodeDateTime, DecodeSpecial, DecodeTimeOnly, DecodeUnits,
    EncodeDateOnly, EncodeTimeOnly, Interval, ParseDateTime, Timestamp, TimeOffset,
    ValidateDate, DTERR_BAD_FORMAT, DTK_CENTURY, DTK_DATE, DTK_DATE_M, DTK_DAY, DTK_DECADE,
    DTK_DOW, DTK_DOY, DTK_EARLY, DTK_EPOCH, DTK_HOUR, DTK_ISODOW, DTK_ISOYEAR, DTK_JULIAN,
    DTK_LATE, DTK_MICROSEC, DTK_MILLENNIUM, DTK_MILLISEC, DTK_MINUTE, DTK_MONTH, DTK_QUARTER,
    DTK_SECOND, DTK_TZ, DTK_TZ_HOUR, DTK_TZ_MINUTE, DTK_WEEK, DTK_YEAR, IS_VALID_JULIAN,
    MAXDATEFIELDS, MAXDATELEN, MAX_TIME_PRECISION, MINS_PER_HOUR, POSTGRES_EPOCH_JDATE, RESERV,
    SECS_PER_DAY, SECS_PER_HOUR, SECS_PER_MINUTE, UNITS, UNIX_EPOCH_JDATE, UNKNOWN_FIELD,
    USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE, USECS_PER_SEC,
};
use adt_timestamp::{
    interval, timestamp2tm, DecodeTimezoneName, DetermineTimeZoneAbbrevOffsetTS, GetEpochTime,
    PartValue, TzLookup, DT_NOBEGIN, DT_NOEND, IS_VALID_TIMESTAMP, MIN_TIMESTAMP,
    TIMESTAMP_IS_NOBEGIN, TIMESTAMP_IS_NOEND, TIMESTAMP_NOT_FINITE,
};
use numeric::{int64_div_fast_to_numeric, int64_to_numeric, numeric_in};
use adt_datetime::consts::TZDISP_LIMIT;
use datum::Bytea;
use mcx::Mcx;
use stringinfo::StringInfo;
use types_core::TimestampTz;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_DATETIME_FIELD_OVERFLOW,
    ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
};
use xact::GetCurrentTransactionStartTimestamp;

pub mod builtins;

#[cfg(test)]
mod interval_corpus;
#[cfg(test)]
mod tests;

pub type DateADT = i32;
pub type TimeADT = i64;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimeTzADT {
    pub time: TimeADT,
    /// numeric time zone, in seconds
    pub zone: i32,
}

pub const DATEVAL_NOBEGIN: DateADT = i32::MIN;
pub const DATEVAL_NOEND: DateADT = i32::MAX;

pub fn date_decrement(existing: datum::Datum, underflow: &mut bool) -> datum::Datum {
    let d: DateADT = existing.as_i32();
    if d == DATEVAL_NOBEGIN {
        *underflow = true;
        return datum::Datum::null();
    }
    *underflow = false;
    datum::Datum::from_i32(d - 1)
}

pub fn date_increment(existing: datum::Datum, overflow: &mut bool) -> datum::Datum {
    let d: DateADT = existing.as_i32();
    if d == DATEVAL_NOEND {
        *overflow = true;
        return datum::Datum::null();
    }
    *overflow = false;
    datum::Datum::from_i32(d + 1)
}

pub const DATETIME_MIN_JULIAN: i32 = 0;
pub const DATE_END_JULIAN: i32 = 2_147_483_494;
pub const TIMESTAMP_END_JULIAN: i32 = 109_203_528;

#[inline(always)]
pub const fn DATE_IS_NOBEGIN(j: DateADT) -> bool {
    j == DATEVAL_NOBEGIN
}

#[inline(always)]
pub const fn DATE_IS_NOEND(j: DateADT) -> bool {
    j == DATEVAL_NOEND
}

#[inline(always)]
pub const fn DATE_NOT_FINITE(j: DateADT) -> bool {
    DATE_IS_NOBEGIN(j) || DATE_IS_NOEND(j)
}

#[inline(always)]
pub const fn IS_VALID_DATE(d: DateADT) -> bool {
    DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE <= d && d < DATE_END_JULIAN - POSTGRES_EPOCH_JDATE
}

pub type DateBuf = [u8; MAXDATELEN + 1];
pub const DATE_WORKBUF: usize = MAXDATELEN + MAXDATEFIELDS;

pub const EARLY: &[u8] = b"-infinity";
pub const LATE: &[u8] = b"infinity";

#[track_caller]
#[cold]
fn date_out_of_range(s: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("date out of range: \"{s}\""))
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

#[track_caller]
#[cold]
fn timestamp_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("timestamp out of range")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

#[track_caller]
#[cold]
fn date_out_of_range_for_timestamp() -> Box<PgError> {
    Box::new(
        PgError::error("date out of range for timestamp")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

// DateTimeErrorExtra borrows the parse workbuf; the error path owns its
// copies so the buffer can die with the frame (cold path, adt_timestamp
// precedent).
struct ExtraOwned {
    timezone: Option<Vec<u8>>,
    abbrev: Option<Vec<u8>>,
}

impl ExtraOwned {
    fn capture(extra: &DateTimeErrorExtra<'_>) -> Self {
        Self {
            timezone: extra.dtee_timezone.map(<[u8]>::to_vec),
            abbrev: extra.dtee_abbrev.map(<[u8]>::to_vec),
        }
    }

    fn parse_error(
        &self,
        dterr: i32,
        s: &str,
        datatype: &str,
        escontext: Option<&mut SoftErrorContext>,
    ) -> PgResult<()> {
        let extra = DateTimeErrorExtra {
            dtee_timezone: self.timezone.as_deref(),
            dtee_abbrev: self.abbrev.as_deref(),
        };
        DateTimeParseError(dterr, Some(&extra), s, datatype, escontext)
    }
}

struct Decoded {
    dtype: i32,
    tm: pg_tm,
    fsec: fsec_t,
    tz: i32,
}

// Inlined: a by-value Decoded return across an outlined call is an sret
// buffer whose narrow stores stall the caller's reload on Neoverse V2
// (bench-crate §3b; date_in_iso measured 1.20x ns at instr parity before).
#[inline(always)]
fn decode_str(
    s: &str,
    workbuf: &mut [u8; DATE_WORKBUF],
    time_only: bool,
) -> Result<Decoded, (i32, ExtraOwned)> {
    let mut field: [&[u8]; MAXDATEFIELDS] = [b""; MAXDATEFIELDS];
    let mut ftype = [0i32; MAXDATEFIELDS];
    let mut nf = 0usize;
    let mut d = Decoded { dtype: 0, tm: pg_tm::default(), fsec: 0, tz: 0 };

    let mut dterr =
        ParseDateTime(s.as_bytes(), workbuf, &mut field, &mut ftype, MAXDATEFIELDS, &mut nf);
    let mut extra = DateTimeErrorExtra::default();
    if dterr == 0 {
        dterr = if time_only {
            DecodeTimeOnly(
                &field[..nf],
                &mut ftype[..nf],
                nf,
                &mut d.dtype,
                &mut d.tm,
                &mut d.fsec,
                Some(&mut d.tz),
                &mut extra,
            )
        } else {
            DecodeDateTime(
                &field[..nf],
                &ftype[..nf],
                nf,
                &mut d.dtype,
                &mut d.tm,
                &mut d.fsec,
                Some(&mut d.tz),
                &mut extra,
            )
        };
    }
    if dterr != 0 {
        return Err((dterr, ExtraOwned::capture(&extra)));
    }
    Ok(d)
}

/// On soft error the sentinel is `Ok(0)` with `escontext.error_occurred()`
/// set (adt_timestamp convention).
pub fn date_in(s: &str, mut escontext: Option<&mut SoftErrorContext>) -> PgResult<DateADT> {
    let mut workbuf = [0u8; DATE_WORKBUF];
    let mut d = match decode_str(s, &mut workbuf, false) {
        Ok(d) => d,
        Err((dterr, extra)) => {
            extra.parse_error(dterr, s, "date", escontext)?;
            return Ok(0);
        }
    };

    match d.dtype {
        DTK_DATE => {}
        DTK_EPOCH => GetEpochTime(&mut d.tm),
        DTK_LATE => return Ok(DATEVAL_NOEND),
        DTK_EARLY => return Ok(DATEVAL_NOBEGIN),
        _ => {
            DateTimeParseError(DTERR_BAD_FORMAT, None, s, "date", escontext)?;
            return Ok(0);
        }
    }

    if !IS_VALID_JULIAN(d.tm.tm_year, d.tm.tm_mon, d.tm.tm_mday) {
        return ereturn(escontext.as_deref_mut(), 0, *date_out_of_range(s));
    }

    let date = date2j(d.tm.tm_year, d.tm.tm_mon, d.tm.tm_mday) - POSTGRES_EPOCH_JDATE;

    if !IS_VALID_DATE(date) {
        return ereturn(escontext, 0, *date_out_of_range(s));
    }

    Ok(date)
}

pub fn EncodeSpecialDate(dt: DateADT, buf: &mut [u8]) -> usize {
    let s: &[u8] = if DATE_IS_NOBEGIN(dt) {
        EARLY
    } else if DATE_IS_NOEND(dt) {
        LATE
    } else {
        panic!("invalid argument for EncodeSpecialDate");
    };
    buf[..s.len()].copy_from_slice(s);
    s.len()
}

pub fn date_out(date: DateADT, buf: &mut DateBuf) -> usize {
    if DATE_NOT_FINITE(date) {
        return EncodeSpecialDate(date, buf);
    }
    let mut tm = pg_tm::default();
    j2date(
        date + POSTGRES_EPOCH_JDATE,
        &mut tm.tm_year,
        &mut tm.tm_mon,
        &mut tm.tm_mday,
    );
    EncodeDateOnly(&tm, adt_datetime::date_style(), buf)
}

pub fn make_date(year: i32, month: i32, day: i32) -> PgResult<DateADT> {
    let mut tm = pg_tm { tm_year: year, tm_mon: month, tm_mday: day, ..pg_tm::default() };
    let mut bc = false;

    #[track_caller]
    #[cold]
    fn field_out_of_range(y: i32, m: i32, d: i32) -> Box<PgError> {
        Box::new(
            PgError::error(format!("date field value out of range: {y}-{m:02}-{d:02}"))
                .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW),
        )
    }

    if tm.tm_year < 0 {
        bc = true;
        let Some(neg) = tm.tm_year.checked_neg() else {
            return Err(field_out_of_range(year, month, day));
        };
        tm.tm_year = neg;
    }

    // C (date.c make_date) prints the CURRENT tm values in every error below
    // — ValidateDate has already folded BC years into the internal
    // convention by the time a later field fails, so e.g. make_date(-1,-1,-1)
    // reports "0--1--1" (1 BC = internal year 0), not "-1--1--1".
    if ValidateDate(DTK_DATE_M, false, false, bc, &mut tm) != 0 {
        return Err(field_out_of_range(tm.tm_year, tm.tm_mon, tm.tm_mday));
    }

    if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        return Err(date_out_of_range_ymd(tm.tm_year, tm.tm_mon, tm.tm_mday));
    }

    let date = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

    if !IS_VALID_DATE(date) {
        return Err(date_out_of_range_ymd(tm.tm_year, tm.tm_mon, tm.tm_mday));
    }

    Ok(date)
}

#[track_caller]
#[cold]
fn date_out_of_range_ymd(y: i32, m: i32, d: i32) -> Box<PgError> {
    Box::new(
        PgError::error(format!("date out of range: {y}-{m:02}-{d:02}"))
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

#[inline]
pub fn date_cmp_internal(d1: DateADT, d2: DateADT) -> i32 {
    if d1 < d2 {
        -1
    } else if d1 > d2 {
        1
    } else {
        0
    }
}

pub fn date_mi(d1: DateADT, d2: DateADT) -> PgResult<i32> {
    if DATE_NOT_FINITE(d1) || DATE_NOT_FINITE(d2) {
        return Err(Box::new(
            PgError::error("cannot subtract infinite dates")
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        ));
    }
    Ok(d1.wrapping_sub(d2))
}

#[track_caller]
#[cold]
fn date_out_of_range_plain() -> Box<PgError> {
    Box::new(
        PgError::error("date out of range").with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

pub fn date_pli(date: DateADT, days: i32) -> PgResult<DateADT> {
    if DATE_NOT_FINITE(date) {
        return Ok(date);
    }
    let result = date.wrapping_add(days);
    if (if days >= 0 { result < date } else { result > date }) || !IS_VALID_DATE(result) {
        return Err(date_out_of_range_plain());
    }
    Ok(result)
}

pub fn date_mii(date: DateADT, days: i32) -> PgResult<DateADT> {
    if DATE_NOT_FINITE(date) {
        return Ok(date);
    }
    let result = date.wrapping_sub(days);
    if (if days >= 0 { result > date } else { result < date }) || !IS_VALID_DATE(result) {
        return Err(date_out_of_range_plain());
    }
    Ok(result)
}

pub fn date2timestamp_opt_overflow(
    date: DateADT,
    mut overflow: Option<&mut i32>,
) -> PgResult<Timestamp> {
    if let Some(o) = overflow.as_deref_mut() {
        *o = 0;
    }

    if DATE_IS_NOBEGIN(date) {
        return Ok(DT_NOBEGIN);
    }
    if DATE_IS_NOEND(date) {
        return Ok(DT_NOEND);
    }
    // dates share timestamps' lower bound; only the upper needs checking
    if date >= TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE {
        if let Some(o) = overflow {
            *o = 1;
            return Ok(DT_NOEND);
        }
        return Err(date_out_of_range_for_timestamp());
    }
    Ok(date as i64 * USECS_PER_DAY)
}

pub fn date2timestamp(date: DateADT) -> PgResult<Timestamp> {
    date2timestamp_opt_overflow(date, None)
}

pub fn date2timestamptz_opt_overflow(
    date: DateADT,
    mut overflow: Option<&mut i32>,
) -> PgResult<TimestampTz> {
    if let Some(o) = overflow.as_deref_mut() {
        *o = 0;
    }

    if DATE_IS_NOBEGIN(date) {
        return Ok(DT_NOBEGIN);
    }
    if DATE_IS_NOEND(date) {
        return Ok(DT_NOEND);
    }
    if date >= TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE {
        if let Some(o) = overflow {
            *o = 1;
            return Ok(DT_NOEND);
        }
        return Err(date_out_of_range_for_timestamp());
    }

    let mut tm = pg_tm::default();
    j2date(
        date + POSTGRES_EPOCH_JDATE,
        &mut tm.tm_year,
        &mut tm.tm_mon,
        &mut tm.tm_mday,
    );
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    let z = tz::session_timezone()
        .unwrap_or_else(|| panic!("session timezone not initialized (pg_timezone_initialize) — date2timestamptz"));
    let tzoff = tz::DetermineTimeZoneOffset(&mut tm, z);

    let result = date as i64 * USECS_PER_DAY + tzoff as i64 * USECS_PER_SEC;

    // tz shift can push past the timestamptz range; re-check after adding it
    if !IS_VALID_TIMESTAMP(result) {
        if let Some(o) = overflow {
            if result < MIN_TIMESTAMP {
                *o = -1;
                return Ok(DT_NOBEGIN);
            }
            *o = 1;
            return Ok(DT_NOEND);
        }
        return Err(date_out_of_range_for_timestamp());
    }

    Ok(result)
}

pub fn date2timestamptz(date: DateADT) -> PgResult<TimestampTz> {
    date2timestamptz_opt_overflow(date, None)
}

pub fn date2timestamp_no_overflow(date: DateADT) -> f64 {
    if DATE_IS_NOBEGIN(date) {
        -f64::MAX
    } else if DATE_IS_NOEND(date) {
        f64::MAX
    } else {
        date as f64 * USECS_PER_DAY as f64
    }
}

// timestamp.c timestamp_cmp_internal; moves to adt_timestamp when it grows
// comparison entry points.
#[inline]
pub fn timestamp_cmp_internal(dt1: Timestamp, dt2: Timestamp) -> i32 {
    if dt1 < dt2 {
        -1
    } else if dt1 > dt2 {
        1
    } else {
        0
    }
}

pub fn date_cmp_timestamp_internal(date: DateADT, dt2: Timestamp) -> i32 {
    let mut overflow = 0;
    let dt1 = date2timestamp_opt_overflow(date, Some(&mut overflow))
        .expect("date2timestamp_opt_overflow cannot fail with overflow out");
    if overflow > 0 {
        // dt1 is larger than any finite timestamp, but less than infinity
        return if TIMESTAMP_IS_NOEND(dt2) { -1 } else { 1 };
    }
    debug_assert!(overflow == 0);
    timestamp_cmp_internal(dt1, dt2)
}

pub fn date_cmp_timestamptz_internal(date: DateADT, dt2: TimestampTz) -> i32 {
    let mut overflow = 0;
    let dt1 = date2timestamptz_opt_overflow(date, Some(&mut overflow))
        .expect("date2timestamptz_opt_overflow cannot fail with overflow out");
    if overflow > 0 {
        return if TIMESTAMP_IS_NOEND(dt2) { -1 } else { 1 };
    }
    if overflow < 0 {
        return if TIMESTAMP_IS_NOBEGIN(dt2) { 1 } else { -1 };
    }
    timestamp_cmp_internal(dt1, dt2)
}

pub fn timestamp_date(timestamp: Timestamp) -> PgResult<DateADT> {
    if TIMESTAMP_IS_NOBEGIN(timestamp) {
        return Ok(DATEVAL_NOBEGIN);
    }
    if TIMESTAMP_IS_NOEND(timestamp) {
        return Ok(DATEVAL_NOEND);
    }
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }
    Ok(date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE)
}

pub fn timestamptz_date(timestamp: TimestampTz) -> PgResult<DateADT> {
    if TIMESTAMP_IS_NOBEGIN(timestamp) {
        return Ok(DATEVAL_NOBEGIN);
    }
    if TIMESTAMP_IS_NOEND(timestamp) {
        return Ok(DATEVAL_NOEND);
    }
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tzoff = 0;
    if timestamp2tm(timestamp, Some(&mut tzoff), &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }
    Ok(date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE)
}

pub fn datetime_timestamp(date: DateADT, time: TimeADT) -> PgResult<Timestamp> {
    let mut result = date2timestamp(date)?;
    if !TIMESTAMP_NOT_FINITE(result) {
        result += time;
        if !IS_VALID_TIMESTAMP(result) {
            return Err(timestamp_out_of_range());
        }
    }
    Ok(result)
}

pub fn datetimetz_timestamptz(date: DateADT, time: &TimeTzADT) -> PgResult<TimestampTz> {
    if DATE_IS_NOBEGIN(date) {
        return Ok(DT_NOBEGIN);
    }
    if DATE_IS_NOEND(date) {
        return Ok(DT_NOEND);
    }
    if date >= TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE {
        return Err(date_out_of_range_for_timestamp());
    }
    let result = date as i64 * USECS_PER_DAY + time.time + time.zone as i64 * USECS_PER_SEC;
    if !IS_VALID_TIMESTAMP(result) {
        return Err(date_out_of_range_for_timestamp());
    }
    Ok(result)
}

pub fn tm2time(tm: &pg_tm, fsec: fsec_t) -> TimeADT {
    ((tm.tm_hour * MINS_PER_HOUR + tm.tm_min) * SECS_PER_MINUTE + tm.tm_sec) as i64
        * USECS_PER_SEC
        + fsec as i64
}

pub use adt_datetime::float_time_overflows;

pub fn time2tm(mut time: TimeADT, tm: &mut pg_tm, fsec: &mut fsec_t) {
    tm.tm_hour = (time / USECS_PER_HOUR) as i32;
    time -= tm.tm_hour as i64 * USECS_PER_HOUR;
    tm.tm_min = (time / USECS_PER_MINUTE) as i32;
    time -= tm.tm_min as i64 * USECS_PER_MINUTE;
    tm.tm_sec = (time / USECS_PER_SEC) as i32;
    time -= tm.tm_sec as i64 * USECS_PER_SEC;
    *fsec = time as fsec_t;
}

pub fn time_in(
    s: &str,
    typmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<TimeADT> {
    let mut workbuf = [0u8; DATE_WORKBUF];
    let d = match decode_str(s, &mut workbuf, true) {
        Ok(d) => d,
        Err((dterr, extra)) => {
            extra.parse_error(dterr, s, "time", escontext)?;
            return Ok(0);
        }
    };

    let mut result = tm2time(&d.tm, d.fsec);
    AdjustTimeForTypmod(&mut result, typmod);
    Ok(result)
}

pub fn time_out(time: TimeADT, buf: &mut DateBuf) -> usize {
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    time2tm(time, &mut tm, &mut fsec);
    EncodeTimeOnly(&tm, fsec, false, 0, adt_datetime::date_style(), buf)
}

pub fn make_time(hour: i32, min: i32, sec: f64) -> PgResult<TimeADT> {
    if float_time_overflows(hour, min, sec) {
        // C (date.c make_time): "%d:%02d:%02g" — the seconds render through
        // PG's snprintf %g (Infinity/NaN spellings, precision 6).
        let sec = adt_datetime::errors::fmt_sec_g02(sec);
        return Err(Box::new(
            PgError::error(format!("time field value out of range: {hour}:{min:02}:{sec}"))
                .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW),
        ));
    }
    Ok(
        ((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE) as i64 * USECS_PER_SEC
            + (sec * USECS_PER_SEC as f64).round_ties_even() as i64,
    )
}

const TIME_SCALES: [i64; MAX_TIME_PRECISION as usize + 1] =
    [1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];
const TIME_OFFSETS: [i64; MAX_TIME_PRECISION as usize + 1] =
    [500_000, 50_000, 5_000, 500, 50, 5, 0];

/// C: `anytime_typmod_check` (date.c:70).
pub fn anytime_typmod_check(istz: bool, typmod: i32) -> PgResult<i32> {
    let with_tz = if istz { " WITH TIME ZONE" } else { "" };
    if typmod < 0 {
        return Err(Box::new(
            PgError::error(format!(
                "TIME({typmod}){with_tz} precision must not be negative"
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if typmod > MAX_TIME_PRECISION {
        elog::ereport(types_error::WARNING)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "TIME({typmod}){with_tz} precision reduced to maximum allowed, {MAX_TIME_PRECISION}"
            ))
            .finish(types_error::ErrorLocation::new(
                "date.c",
                0,
                "anytime_typmod_check",
            ))?;
        return Ok(MAX_TIME_PRECISION);
    }
    Ok(typmod)
}

pub fn AdjustTimeForTypmod(time: &mut TimeADT, typmod: i32) {
    if typmod >= 0 && typmod <= MAX_TIME_PRECISION {
        let scale = TIME_SCALES[typmod as usize];
        let offset = TIME_OFFSETS[typmod as usize];
        if *time >= 0 {
            *time = ((*time + offset) / scale) * scale;
        } else {
            *time = -(((-*time + offset) / scale) * scale);
        }
    }
}

pub fn time_scale(time: TimeADT, typmod: i32) -> TimeADT {
    let mut result = time;
    AdjustTimeForTypmod(&mut result, typmod);
    result
}

#[inline]
pub fn time_cmp_internal(t1: TimeADT, t2: TimeADT) -> i32 {
    if t1 < t2 {
        -1
    } else if t1 > t2 {
        1
    } else {
        0
    }
}

pub fn timestamp_time(timestamp: Timestamp) -> PgResult<Option<TimeADT>> {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(None);
    }
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }
    Ok(Some(tm2time(&tm, fsec)))
}

pub fn timestamptz_time(timestamp: TimestampTz) -> PgResult<Option<TimeADT>> {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(None);
    }
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tzoff = 0;
    if timestamp2tm(timestamp, Some(&mut tzoff), &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }
    Ok(Some(tm2time(&tm, fsec)))
}

pub fn tm2timetz(tm: &pg_tm, fsec: fsec_t, tz: i32, result: &mut TimeTzADT) {
    result.time = tm2time(tm, fsec);
    result.zone = tz;
}

pub fn timetz_in(
    s: &str,
    typmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<TimeTzADT> {
    let mut workbuf = [0u8; DATE_WORKBUF];
    let d = match decode_str(s, &mut workbuf, true) {
        Ok(d) => d,
        Err((dterr, extra)) => {
            extra.parse_error(dterr, s, "time with time zone", escontext)?;
            return Ok(TimeTzADT::default());
        }
    };

    let mut result = TimeTzADT::default();
    tm2timetz(&d.tm, d.fsec, d.tz, &mut result);
    AdjustTimeForTypmod(&mut result.time, typmod);
    Ok(result)
}

pub fn timetz2tm(time: &TimeTzADT, tm: &mut pg_tm, fsec: &mut fsec_t, tzp: Option<&mut i32>) {
    let mut trem: TimeOffset = time.time;
    tm.tm_hour = (trem / USECS_PER_HOUR) as i32;
    trem -= tm.tm_hour as i64 * USECS_PER_HOUR;
    tm.tm_min = (trem / USECS_PER_MINUTE) as i32;
    trem -= tm.tm_min as i64 * USECS_PER_MINUTE;
    tm.tm_sec = (trem / USECS_PER_SEC) as i32;
    *fsec = (trem - tm.tm_sec as i64 * USECS_PER_SEC) as fsec_t;
    if let Some(tzp) = tzp {
        *tzp = time.zone;
    }
}

pub fn timetz_out(time: &TimeTzADT, buf: &mut DateBuf) -> usize {
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tzoff = 0;
    timetz2tm(time, &mut tm, &mut fsec, Some(&mut tzoff));
    EncodeTimeOnly(&tm, fsec, true, tzoff, adt_datetime::date_style(), buf)
}

pub fn timetz_scale(time: &TimeTzADT, typmod: i32) -> TimeTzADT {
    let mut result = *time;
    AdjustTimeForTypmod(&mut result.time, typmod);
    result
}

pub fn timetz_cmp_internal(time1: &TimeTzADT, time2: &TimeTzADT) -> i32 {
    // primary sort is by true (GMT-equivalent) time
    let t1 = time1.time + time1.zone as i64 * USECS_PER_SEC;
    let t2 = time2.time + time2.zone as i64 * USECS_PER_SEC;
    if t1 > t2 {
        return 1;
    }
    if t1 < t2 {
        return -1;
    }
    if time1.zone > time2.zone {
        return 1;
    }
    if time1.zone < time2.zone {
        return -1;
    }
    0
}

pub fn timetz_time(timetz: &TimeTzADT) -> TimeADT {
    timetz.time
}

pub fn time_timetz(time: TimeADT) -> TimeTzADT {
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    tz::GetCurrentDateTime(&mut tm);
    time2tm(time, &mut tm, &mut fsec);
    let z = tz::session_timezone()
        .unwrap_or_else(|| panic!("session timezone not initialized (pg_timezone_initialize) — time_timetz"));
    let tzoff = tz::DetermineTimeZoneOffset(&mut tm, z);
    TimeTzADT { time, zone: tzoff }
}

pub fn timestamptz_timetz(timestamp: TimestampTz) -> PgResult<Option<TimeTzADT>> {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(None);
    }
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tzoff = 0;
    if timestamp2tm(timestamp, Some(&mut tzoff), &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }
    let mut result = TimeTzADT::default();
    tm2timetz(&tm, fsec, tzoff, &mut result);
    Ok(Some(result))
}

std::thread_local! {
    // C's static cache in GetSQLCurrentDate: date2j is several divisions and
    // only changes across local midnight.
    static SQL_CURRENT_DATE_CACHE: core::cell::Cell<(i32, i32, i32, DateADT)> =
        const { core::cell::Cell::new((0, 0, 0, 0)) };
}

pub fn GetSQLCurrentDate() -> DateADT {
    let mut tm = pg_tm::default();
    tz::GetCurrentDateTime(&mut tm);

    SQL_CURRENT_DATE_CACHE.with(|c| {
        let (y, m, d, date) = c.get();
        if (tm.tm_year, tm.tm_mon, tm.tm_mday) == (y, m, d) {
            return date;
        }
        let date = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;
        c.set((tm.tm_year, tm.tm_mon, tm.tm_mday, date));
        date
    })
}

pub fn GetSQLCurrentTime(typmod: i32) -> TimeTzADT {
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tzoff = 0;
    tz::GetCurrentTimeUsec(&mut tm, &mut fsec, Some(&mut tzoff));
    let mut result = TimeTzADT::default();
    tm2timetz(&tm, fsec, tzoff, &mut result);
    AdjustTimeForTypmod(&mut result.time, typmod);
    result
}

pub fn GetSQLLocalTime(typmod: i32) -> TimeADT {
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tzoff = 0;
    tz::GetCurrentTimeUsec(&mut tm, &mut fsec, Some(&mut tzoff));
    let mut result = tm2time(&tm, fsec);
    AdjustTimeForTypmod(&mut result, typmod);
    result
}

// hashfunc.c hashint8's fold of int64 to a hashable u32 (hashfunc.c unit
// unported; time_hash's value core needs it).
#[inline]
pub fn int64_hash_fold(val: i64) -> u32 {
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    lohalf ^ if val >= 0 { hihalf } else { !hihalf }
}

// C's TimeTzADT has typlen 12; the trailing 4 bytes are padding that on-disk
// values do not carry, so tuple-borrowed values are read field-by-field
// (builtins::arg_timetz), never as a whole-struct reference.
const _: () = {
    assert!(core::mem::offset_of!(TimeTzADT, time) == 0);
    assert!(core::mem::offset_of!(TimeTzADT, zone) == 8);
};

// Binary wire (date.c recv/send); range checks and typmod adjustment exactly
// as C. TimeTzADT is returned by value; the fc wrapper builds the 12-byte
// by-ref image.
pub fn date_recv(buf: &mut StringInfo<'_>) -> PgResult<DateADT> {
    let result = pqformat::pq_getmsgint(buf, 4)? as i32;
    if !DATE_NOT_FINITE(result) && !IS_VALID_DATE(result) {
        return Err(datetime_out_of_range("date out of range"));
    }
    Ok(result)
}

pub fn date_send<'mcx>(mcx: Mcx<'mcx>, date: DateADT) -> PgResult<Bytea<'mcx>> {
    let mut b = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint32(&mut b, date as u32)?;
    Ok(pqformat::pq_endtypsend(b))
}

pub fn time_recv(buf: &mut StringInfo<'_>, typmod: i32) -> PgResult<TimeADT> {
    let mut result = pqformat::pq_getmsgint64(buf)?;
    if result < 0 || result > USECS_PER_DAY {
        return Err(datetime_out_of_range("time out of range"));
    }
    AdjustTimeForTypmod(&mut result, typmod);
    Ok(result)
}

pub fn time_send<'mcx>(mcx: Mcx<'mcx>, time: TimeADT) -> PgResult<Bytea<'mcx>> {
    let mut b = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint64(&mut b, time as u64)?;
    Ok(pqformat::pq_endtypsend(b))
}

pub fn timetz_recv(buf: &mut StringInfo<'_>, typmod: i32) -> PgResult<TimeTzADT> {
    let mut time = pqformat::pq_getmsgint64(buf)?;
    if time < 0 || time > USECS_PER_DAY {
        return Err(datetime_out_of_range("time out of range"));
    }
    let zone = pqformat::pq_getmsgint(buf, 4)? as i32;
    if zone <= -TZDISP_LIMIT || zone >= TZDISP_LIMIT {
        return Err(Box::new(
            PgError::error("time zone displacement out of range")
                .with_sqlstate(ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE),
        ));
    }
    AdjustTimeForTypmod(&mut time, typmod);
    Ok(TimeTzADT { time, zone })
}

pub fn timetz_send<'mcx>(mcx: Mcx<'mcx>, t: &TimeTzADT) -> PgResult<Bytea<'mcx>> {
    let mut b = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint64(&mut b, t.time as u64)?;
    pqformat::pq_sendint32(&mut b, t.zone as u32)?;
    Ok(pqformat::pq_endtypsend(b))
}

#[track_caller]
#[cold]
#[inline(never)]
fn datetime_out_of_range(msg: &'static str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE))
}

pub fn date_pl_interval(date: DateADT, span: &Interval) -> PgResult<Timestamp> {
    interval::timestamp_pl_interval(date2timestamp(date)?, span)
}

pub fn date_mi_interval(date: DateADT, span: &Interval) -> PgResult<Timestamp> {
    interval::timestamp_mi_interval(date2timestamp(date)?, span)
}

pub fn time_interval(time: TimeADT) -> Interval {
    Interval { time, day: 0, month: 0 }
}

/// Fractional-day portion of the interval; negatives wrap ('-2 hours' -> 22:00).
pub fn interval_time(span: &Interval) -> PgResult<TimeADT> {
    if span.not_finite() {
        return Err(Box::new(
            PgError::error("cannot convert infinite interval to time")
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        ));
    }
    let mut result = span.time % USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }
    Ok(result)
}

pub fn time_mi_time(time1: TimeADT, time2: TimeADT) -> Interval {
    Interval { time: time1 - time2, day: 0, month: 0 }
}

#[track_caller]
#[cold]
#[inline(never)]
fn infinite_interval_time_err(msg: &'static str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE))
}

pub fn time_pl_interval(time: TimeADT, span: &Interval) -> PgResult<TimeADT> {
    if span.not_finite() {
        return Err(infinite_interval_time_err("cannot add infinite interval to time"));
    }
    let mut result = time.wrapping_add(span.time);
    result -= result / USECS_PER_DAY * USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }
    Ok(result)
}

pub fn time_mi_interval(time: TimeADT, span: &Interval) -> PgResult<TimeADT> {
    if span.not_finite() {
        return Err(infinite_interval_time_err("cannot subtract infinite interval from time"));
    }
    let mut result = time.wrapping_sub(span.time);
    result -= result / USECS_PER_DAY * USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }
    Ok(result)
}

pub fn timetz_pl_interval(time: &TimeTzADT, span: &Interval) -> PgResult<TimeTzADT> {
    if span.not_finite() {
        return Err(infinite_interval_time_err("cannot add infinite interval to time"));
    }
    let mut t = time.time.wrapping_add(span.time);
    t -= t / USECS_PER_DAY * USECS_PER_DAY;
    if t < 0 {
        t += USECS_PER_DAY;
    }
    Ok(TimeTzADT { time: t, zone: time.zone })
}

pub fn timetz_mi_interval(time: &TimeTzADT, span: &Interval) -> PgResult<TimeTzADT> {
    if span.not_finite() {
        return Err(infinite_interval_time_err("cannot subtract infinite interval from time"));
    }
    let mut t = time.time.wrapping_sub(span.time);
    t -= t / USECS_PER_DAY * USECS_PER_DAY;
    if t < 0 {
        t += USECS_PER_DAY;
    }
    Ok(TimeTzADT { time: t, zone: time.zone })
}

// C99 modulo has the wrong sign convention for negative input (C comment).
fn timetz_rotate(t: &TimeTzADT, tz: i32) -> TimeTzADT {
    let mut time = t.time + (t.zone - tz) as i64 * USECS_PER_SEC;
    while time < 0 {
        time += USECS_PER_DAY;
    }
    if time >= USECS_PER_DAY {
        time %= USECS_PER_DAY;
    }
    TimeTzADT { time, zone: tz }
}

// C text_to_cstring_buffer(zone, buf, TZ_STRLEN_MAX + 1); adt_timestamp's
// byte-truncation divergence note applies.
fn text_to_tzname(zone: &[u8]) -> &[u8] {
    let z = &zone[..zone.len().min(255)];
    match z.iter().position(|&b| b == 0) {
        Some(i) => &z[..i],
        None => z,
    }
}

/// C `timetz_zone` on the zone text payload.
pub fn timetz_zone(zone: &[u8], t: &TimeTzADT) -> PgResult<TimeTzADT> {
    let tzname = text_to_tzname(zone);
    let tz = match DecodeTimezoneName(tzname)? {
        TzLookup::FixedOffset(val) => -val,
        TzLookup::DynTz(tzp) => {
            let now = GetCurrentTransactionStartTimestamp();
            let mut isdst = 0;
            DetermineTimeZoneAbbrevOffsetTS(now, tzname, tzp, &mut isdst)?
        }
        TzLookup::Zone(tzp) => {
            let now = GetCurrentTransactionStartTimestamp();
            let mut tm = pg_tm::default();
            let mut fsec: fsec_t = 0;
            let mut tz = 0;
            if timestamp2tm(now, Some(&mut tz), &mut tm, &mut fsec, None, Some(tzp)).is_err() {
                return Err(timestamp_out_of_range());
            }
            tz
        }
    };
    Ok(timetz_rotate(t, tz))
}

pub fn timetz_izone(zone: &Interval, time: &TimeTzADT) -> PgResult<TimeTzADT> {
    let tz = -(interval::izone_offset(zone)?) as i32;
    Ok(timetz_rotate(time, tz))
}

pub fn timetz_at_local(t: &TimeTzADT) -> PgResult<TimeTzADT> {
    let z = tz::session_timezone()
        .unwrap_or_else(|| panic!("session timezone not initialized (pg_timezone_initialize) — timetz_at_local"));
    let tzn = tz::pg_get_timezone_name(z).unwrap_or("");
    timetz_zone(tzn.as_bytes(), t)
}

fn downcase_ident<'a>(src: &[u8], out: &'a mut [u8; 64]) -> &'a [u8] {
    let n = src.len().min(63);
    for (dst, b) in out.iter_mut().zip(&src[..n]) {
        *dst = b.to_ascii_lowercase();
    }
    &out[..n]
}

#[track_caller]
#[cold]
fn unit_not_supported(lowunits: &[u8], typename: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "unit \"{}\" not supported for type {typename}",
            String::from_utf8_lossy(lowunits)
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[track_caller]
#[cold]
fn unit_not_recognized(lowunits: &[u8], typename: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "unit \"{}\" not recognized for type {typename}",
            String::from_utf8_lossy(lowunits)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

pub fn extract_date(units: &[u8], date: DateADT) -> PgResult<PartValue> {
    let mut low = [0u8; 64];
    let lowunits = downcase_ident(units, &mut low);

    let mut val = 0;
    let mut type_ = DecodeUnits(0, lowunits, &mut val);
    if type_ == UNKNOWN_FIELD {
        type_ = DecodeSpecial(0, lowunits, &mut val);
    }

    if DATE_NOT_FINITE(date) && (type_ == UNITS || type_ == RESERV) {
        return match val {
            // Oscillating units
            DTK_DAY | DTK_MONTH | DTK_QUARTER | DTK_WEEK | DTK_DOW | DTK_ISODOW | DTK_DOY => {
                Ok(PartValue::Null)
            }
            // Monotonically-increasing units
            DTK_YEAR | DTK_DECADE | DTK_CENTURY | DTK_MILLENNIUM | DTK_JULIAN | DTK_ISOYEAR
            | DTK_EPOCH => {
                let lit = if DATE_IS_NOBEGIN(date) { "-Infinity" } else { "Infinity" };
                Ok(PartValue::Numeric(
                    numeric_in(lit, -1, None)?.expect("infinity literal parses"),
                ))
            }
            _ => Err(unit_not_supported(lowunits, "date")),
        };
    }

    let intresult: i64 = if type_ == UNITS {
        let (mut year, mut mon, mut mday) = (0, 0, 0);
        j2date(date + POSTGRES_EPOCH_JDATE, &mut year, &mut mon, &mut mday);
        match val {
            DTK_DAY => mday as i64,
            DTK_MONTH => mon as i64,
            DTK_QUARTER => ((mon - 1) / 3 + 1) as i64,
            DTK_WEEK => date2isoweek(year, mon, mday) as i64,
            DTK_YEAR => {
                if year > 0 {
                    year as i64
                } else {
                    // there is no year 0, just 1 BC and 1 AD
                    (year - 1) as i64
                }
            }
            DTK_DECADE => {
                if year >= 0 {
                    (year / 10) as i64
                } else {
                    -(((8 - (year - 1)) / 10) as i64)
                }
            }
            DTK_CENTURY => {
                if year > 0 {
                    ((year + 99) / 100) as i64
                } else {
                    -(((99 - (year - 1)) / 100) as i64)
                }
            }
            DTK_MILLENNIUM => {
                if year > 0 {
                    ((year + 999) / 1000) as i64
                } else {
                    -(((999 - (year - 1)) / 1000) as i64)
                }
            }
            DTK_JULIAN => (date + POSTGRES_EPOCH_JDATE) as i64,
            DTK_ISOYEAR => {
                let mut r = date2isoyear(year, mon, mday) as i64;
                // Adjust BC years
                if r <= 0 {
                    r -= 1;
                }
                r
            }
            DTK_DOW | DTK_ISODOW => {
                let mut r = j2day(date + POSTGRES_EPOCH_JDATE) as i64;
                if val == DTK_ISODOW && r == 0 {
                    r = 7;
                }
                r
            }
            DTK_DOY => (date2j(year, mon, mday) - date2j(year, 1, 1) + 1) as i64,
            _ => return Err(unit_not_supported(lowunits, "date")),
        }
    } else if type_ == RESERV {
        match val {
            DTK_EPOCH => {
                (date as i64 + POSTGRES_EPOCH_JDATE as i64 - UNIX_EPOCH_JDATE as i64)
                    * SECS_PER_DAY as i64
            }
            _ => return Err(unit_not_supported(lowunits, "date")),
        }
    } else {
        return Err(unit_not_recognized(lowunits, "date"));
    };

    Ok(PartValue::Numeric(int64_to_numeric(intresult)))
}

fn part_units_time(
    val: i32,
    sec: i32,
    fsec: fsec_t,
    min: i32,
    hour: i32,
    retnumeric: bool,
) -> PgResult<Option<PartValue>> {
    Ok(Some(match val {
        DTK_MICROSEC => finish_time_part(sec as i64 * 1_000_000 + fsec as i64, retnumeric),
        DTK_MILLISEC => {
            if retnumeric {
                PartValue::Numeric(int64_div_fast_to_numeric(
                    sec as i64 * 1_000_000 + fsec as i64,
                    3,
                )?)
            } else {
                PartValue::Float(sec as f64 * 1000.0 + fsec as f64 / 1000.0)
            }
        }
        DTK_SECOND => {
            if retnumeric {
                PartValue::Numeric(int64_div_fast_to_numeric(
                    sec as i64 * 1_000_000 + fsec as i64,
                    6,
                )?)
            } else {
                PartValue::Float(sec as f64 + fsec as f64 / 1_000_000.0)
            }
        }
        DTK_MINUTE => finish_time_part(min as i64, retnumeric),
        DTK_HOUR => finish_time_part(hour as i64, retnumeric),
        _ => return Ok(None),
    }))
}

fn finish_time_part(intresult: i64, retnumeric: bool) -> PartValue {
    if retnumeric {
        PartValue::Numeric(int64_to_numeric(intresult))
    } else {
        PartValue::Float(intresult as f64)
    }
}

pub fn time_part_common(units: &[u8], time: TimeADT, retnumeric: bool) -> PgResult<PartValue> {
    let mut low = [0u8; 64];
    let lowunits = downcase_ident(units, &mut low);

    let mut val = 0;
    let mut type_ = DecodeUnits(0, lowunits, &mut val);
    if type_ == UNKNOWN_FIELD {
        type_ = DecodeSpecial(0, lowunits, &mut val);
    }

    if type_ == UNITS {
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        time2tm(time, &mut tm, &mut fsec);
        match part_units_time(val, tm.tm_sec, fsec, tm.tm_min, tm.tm_hour, retnumeric)? {
            Some(v) => Ok(v),
            None => Err(unit_not_supported(lowunits, "time without time zone")),
        }
    } else if type_ == RESERV && val == DTK_EPOCH {
        if retnumeric {
            Ok(PartValue::Numeric(int64_div_fast_to_numeric(time, 6)?))
        } else {
            Ok(PartValue::Float(time as f64 / 1_000_000.0))
        }
    } else {
        Err(unit_not_recognized(lowunits, "time without time zone"))
    }
}

pub fn timetz_part_common(
    units: &[u8],
    time: &TimeTzADT,
    retnumeric: bool,
) -> PgResult<PartValue> {
    let mut low = [0u8; 64];
    let lowunits = downcase_ident(units, &mut low);

    let mut val = 0;
    let mut type_ = DecodeUnits(0, lowunits, &mut val);
    if type_ == UNKNOWN_FIELD {
        type_ = DecodeSpecial(0, lowunits, &mut val);
    }

    if type_ == UNITS {
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        let mut tz = 0;
        timetz2tm(time, &mut tm, &mut fsec, Some(&mut tz));
        match val {
            DTK_TZ => Ok(finish_time_part(-tz as i64, retnumeric)),
            DTK_TZ_MINUTE => {
                Ok(finish_time_part(((-tz / SECS_PER_MINUTE) % MINS_PER_HOUR) as i64, retnumeric))
            }
            DTK_TZ_HOUR => Ok(finish_time_part((-tz / SECS_PER_HOUR) as i64, retnumeric)),
            _ => match part_units_time(val, tm.tm_sec, fsec, tm.tm_min, tm.tm_hour, retnumeric)? {
                Some(v) => Ok(v),
                None => Err(unit_not_supported(lowunits, "time with time zone")),
            },
        }
    } else if type_ == RESERV && val == DTK_EPOCH {
        if retnumeric {
            Ok(PartValue::Numeric(int64_div_fast_to_numeric(
                time.time + time.zone as i64 * 1_000_000,
                6,
            )?))
        } else {
            Ok(PartValue::Float(time.time as f64 / 1_000_000.0 + time.zone as f64))
        }
    } else {
        Err(unit_not_recognized(lowunits, "time with time zone"))
    }
}
