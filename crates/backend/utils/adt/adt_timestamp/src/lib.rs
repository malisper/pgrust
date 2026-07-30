//! timestamp.c core: current-time entry points, timestamp/timestamptz text
//! and binary I/O over adt_datetime, timestamp2tm/tm2timestamp, the timezone
//! surface (zone/at-local/trunc/part/make_*), and datetime.c's
//! DecodeTimezoneName/GetCurrentTimeUsec (they need timestamp2tm). Interval
//! half deferred with datetime's interval note. Zero-allocation I/O: parse
//! fields borrow a caller workbuf, output writes into a caller-owned
//! MAXDATELEN buffer (no cstring detour).

#![allow(non_snake_case)]

use adt_datetime::tz::{self, PgTz};
use adt_datetime::{
    date2isoweek, date2isoyear, date2j, dt2time, isoweek2date, j2date, j2day, pg_tm,
    DateTimeErrorExtra, DateTimeParseError, DecodeDateTime, DecodeSpecial, DecodeTimezone,
    DecodeTimezoneAbbrev, DecodeUnits, EncodeDateTime, ParseDateTime, ValidateDate,
    float_time_overflows, fsec_t, Timestamp, TimeOffset, DTERR_BAD_FORMAT,
    DTERR_TZDISP_OVERFLOW, DTK_CENTURY, DTK_DATE, DTK_DATE_M, DTK_DAY, DTK_DECADE, DTK_DOW,
    DTK_DOY, DTK_EARLY, DTK_EPOCH, DTK_HOUR, DTK_ISODOW, DTK_ISOYEAR, DTK_JULIAN, DTK_LATE,
    DTK_MICROSEC, DTK_MILLENNIUM, DTK_MILLISEC, DTK_MINUTE, DTK_MONTH, DTK_QUARTER, DTK_SECOND,
    DTK_TZ, DTK_TZ_HOUR, DTK_TZ_MINUTE, DTK_WEEK, DTK_YEAR, DTZ, DYNTZ, IS_VALID_JULIAN,
    MAXDATEFIELDS, MAXDATELEN, MAX_TIMESTAMP_PRECISION, MINS_PER_HOUR, MONTHS_PER_YEAR,
    POSTGRES_EPOCH_JDATE, RESERV, SECS_PER_DAY, SECS_PER_HOUR, SECS_PER_MINUTE, TZ,
    UNIX_EPOCH_JDATE, UNITS, UNKNOWN_FIELD, USECS_PER_DAY, USECS_PER_SEC,
};
use localtime::TZ_STRLEN_MAX;
use numeric::{
    int64_div_fast_to_numeric, int64_to_numeric, numeric_add_common, numeric_div_common,
    numeric_in, numeric_round_common, numeric_sub_common, NumericImage,
};
use types_core::TimestampTz;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_DATETIME_FIELD_OVERFLOW,
    ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE,
};

pub mod builtins;
pub mod interval;

#[cfg(test)]
mod tests;

pub const DT_NOBEGIN: Timestamp = i64::MIN;
pub const DT_NOEND: Timestamp = i64::MAX;
pub const MIN_TIMESTAMP: Timestamp = -211_813_488_000_000_000;
pub const END_TIMESTAMP: Timestamp = 9_223_371_331_200_000_000;

pub const EARLY: &[u8] = b"-infinity";
pub const LATE: &[u8] = b"infinity";

#[inline(always)]
pub const fn TIMESTAMP_IS_NOBEGIN(j: Timestamp) -> bool {
    j == DT_NOBEGIN
}

#[inline(always)]
pub const fn TIMESTAMP_IS_NOEND(j: Timestamp) -> bool {
    j == DT_NOEND
}

#[inline(always)]
pub const fn TIMESTAMP_NOT_FINITE(j: Timestamp) -> bool {
    TIMESTAMP_IS_NOBEGIN(j) || TIMESTAMP_IS_NOEND(j)
}

#[inline(always)]
pub const fn IS_VALID_TIMESTAMP(t: Timestamp) -> bool {
    MIN_TIMESTAMP <= t && t < END_TIMESTAMP
}

pub type TsBuf = [u8; MAXDATELEN + 1];
pub const TS_WORKBUF: usize = MAXDATELEN + MAXDATEFIELDS;

#[cold]
pub(crate) fn timestamp_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("timestamp out of range")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

pub fn GetCurrentTimestamp() -> TimestampTz {
    // DST P2 (contract §1.2): the SEMANTIC wall read rides pg_clock; the
    // timestamp_seams::get_current_timestamp backend collapses onto this
    // (the seam survives as API; test_boot stub behavior preserved).
    let (tv_sec, tv_usec) = pg_clock::wall_timeval();

    let mut result =
        tv_sec - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY as i64);
    result = result * USECS_PER_SEC + tv_usec as i64;
    result
}

pub fn GetSQLCurrentTimestamp(typmod: i32) -> TimestampTz {
    let mut ts = xact::GetCurrentTransactionStartTimestamp();
    if typmod >= 0 {
        AdjustTimestampForTypmod(&mut ts, typmod, None)
            .expect("AdjustTimestampForTypmod: hard error without escontext");
    }
    ts
}

const TIMESTAMP_SCALES: [i64; MAX_TIMESTAMP_PRECISION as usize + 1] =
    [1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];
const TIMESTAMP_OFFSETS: [i64; MAX_TIMESTAMP_PRECISION as usize + 1] =
    [500_000, 50_000, 5_000, 500, 50, 5, 0];

pub fn AdjustTimestampForTypmod(
    time: &mut Timestamp,
    typmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    if !TIMESTAMP_NOT_FINITE(*time) && typmod != -1 && typmod != MAX_TIMESTAMP_PRECISION {
        if typmod < 0 || typmod > MAX_TIMESTAMP_PRECISION {
            return ereturn(
                escontext,
                false,
                PgError::error(format!(
                    "timestamp({typmod}) precision must be between {} and {}",
                    0, MAX_TIMESTAMP_PRECISION
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
            );
        }

        let scale = TIMESTAMP_SCALES[typmod as usize];
        let offset = TIMESTAMP_OFFSETS[typmod as usize];
        if *time >= 0 {
            *time = ((*time + offset) / scale) * scale;
        } else {
            *time = -((((-*time) + offset) / scale) * scale);
        }
    }

    Ok(true)
}

/// C: `anytimestamp_typmod_check` (timestamp.c:105).
pub fn anytimestamp_typmod_check(istz: bool, typmod: i32) -> PgResult<i32> {
    let with_tz = if istz { " WITH TIME ZONE" } else { "" };
    if typmod < 0 {
        return Err(Box::new(
            PgError::error(format!(
                "TIMESTAMP({typmod}){with_tz} precision must not be negative"
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if typmod > MAX_TIMESTAMP_PRECISION {
        elog::ereport(types_error::WARNING)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "TIMESTAMP({typmod}){with_tz} precision reduced to maximum allowed, {MAX_TIMESTAMP_PRECISION}"
            ))
            .finish(types_error::ErrorLocation::new(
                "timestamp.c",
                0,
                "anytimestamp_typmod_check",
            ))?;
        return Ok(MAX_TIMESTAMP_PRECISION);
    }
    Ok(typmod)
}

pub fn EncodeSpecialTimestamp(dt: Timestamp, buf: &mut [u8]) -> usize {
    let s: &[u8] = if TIMESTAMP_IS_NOBEGIN(dt) {
        EARLY
    } else if TIMESTAMP_IS_NOEND(dt) {
        LATE
    } else {
        panic!("invalid argument for EncodeSpecialTimestamp");
    };
    buf[..s.len()].copy_from_slice(s);
    s.len()
}

struct Decoded {
    dtype: i32,
    tm: pg_tm,
    fsec: fsec_t,
    tz: i32,
}

fn decode_timestamp_str(
    s: &str,
    workbuf: &mut [u8; TS_WORKBUF],
) -> Result<Decoded, (i32, DateTimeErrorExtraOwned)> {
    let mut field: [&[u8]; MAXDATEFIELDS] = [b""; MAXDATEFIELDS];
    let mut ftype = [0i32; MAXDATEFIELDS];
    let mut nf = 0usize;
    let mut d = Decoded { dtype: 0, tm: pg_tm::default(), fsec: 0, tz: 0 };

    let mut dterr =
        ParseDateTime(s.as_bytes(), workbuf, &mut field, &mut ftype, MAXDATEFIELDS, &mut nf);
    let mut extra = DateTimeErrorExtra::default();
    if dterr == 0 {
        dterr = DecodeDateTime(
            &field[..nf],
            &ftype[..nf],
            nf,
            &mut d.dtype,
            &mut d.tm,
            &mut d.fsec,
            Some(&mut d.tz),
            &mut extra,
        );
    }
    if dterr != 0 {
        return Err((dterr, DateTimeErrorExtraOwned::capture(&extra)));
    }
    Ok(d)
}

// DateTimeErrorExtra borrows the workbuf; the error path owns its copies so
// the buffer can die with the frame (cold path, two small copies).
struct DateTimeErrorExtraOwned {
    timezone: Option<Vec<u8>>,
    abbrev: Option<Vec<u8>>,
}

impl DateTimeErrorExtraOwned {
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

fn timestamp_in_common(
    s: &str,
    typmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
    with_tz: bool,
) -> PgResult<Timestamp> {
    let datatype = if with_tz { "timestamp with time zone" } else { "timestamp" };
    let mut workbuf = [0u8; TS_WORKBUF];
    let d = match decode_timestamp_str(s, &mut workbuf) {
        Ok(d) => d,
        Err((dterr, extra)) => {
            extra.parse_error(dterr, s, datatype, escontext)?;
            return Ok(0);
        }
    };

    let mut result: Timestamp;
    match d.dtype {
        DTK_DATE => {
            let mut r = 0;
            let tzp = with_tz.then_some(d.tz);
            if tm2timestamp(&d.tm, d.fsec, tzp, &mut r).is_err() {
                return ereturn(
                    escontext.as_deref_mut(),
                    0,
                    PgError::error(format!("timestamp out of range: \"{s}\""))
                        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                );
            }
            result = r;
        }
        DTK_EPOCH => result = SetEpochTimestamp(),
        DTK_LATE => result = DT_NOEND,
        DTK_EARLY => result = DT_NOBEGIN,
        other => {
            return Err(Box::new(PgError::error(format!(
                "unexpected dtype {other} while parsing {datatype} \"{s}\""
            ))));
        }
    }

    AdjustTimestampForTypmod(&mut result, typmod, escontext)?;
    Ok(result)
}

/// On soft error (escontext captured it) the C body returns a NULL datum;
/// here the sentinel is `Ok(0)` with `escontext.error_occurred()` set.
pub fn timestamp_in(
    s: &str,
    typmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Timestamp> {
    timestamp_in_common(s, typmod, escontext, false)
}

pub fn timestamptz_in(
    s: &str,
    typmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<TimestampTz> {
    timestamp_in_common(s, typmod, escontext, true)
}

pub fn timestamp_out(timestamp: Timestamp, buf: &mut TsBuf) -> PgResult<usize> {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(EncodeSpecialTimestamp(timestamp, buf));
    }
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }
    Ok(EncodeDateTime(&mut tm, fsec, false, 0, None, adt_datetime::date_style(), buf))
}

pub fn timestamptz_out(dt: TimestampTz, buf: &mut TsBuf) -> PgResult<usize> {
    if TIMESTAMP_NOT_FINITE(dt) {
        return Ok(EncodeSpecialTimestamp(dt, buf));
    }
    let mut tz: i32 = 0;
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tzn: Option<&'static str> = None;
    if timestamp2tm(dt, Some(&mut tz), &mut tm, &mut fsec, Some(&mut tzn), None).is_err() {
        return Err(timestamp_out_of_range());
    }
    let tzn = tzn.map(str::as_bytes);
    Ok(EncodeDateTime(&mut tm, fsec, true, tz, tzn, adt_datetime::date_style(), buf))
}

/// C contract: `tm_year` full value, `tm_mon` one-based. `Err(())` is the C
/// `-1` out-of-range return.
#[allow(clippy::result_unit_err)]
// TimestampTimestampTzRequiresRewrite (timestamp.c): rewrite-free only when
// the session timezone is a fixed zero offset from UTC.
pub fn TimestampTimestampTzRequiresRewrite() -> bool {
    let Some(zone) = tz::session_timezone() else {
        return true;
    };
    let mut offset: i64 = 0;
    if tz::pg_get_timezone_offset(zone, &mut offset) && offset == 0 {
        return false;
    }
    true
}

pub fn timestamp2tm(
    dt: Timestamp,
    tzp: Option<&mut i32>,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tzn: Option<&mut Option<&'static str>>,
    attimezone: Option<&'static PgTz>,
) -> Result<(), ()> {
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

    date += POSTGRES_EPOCH_JDATE as i64;

    if date < 0 || date > i32::MAX as i64 {
        return Err(());
    }

    j2date(date as i32, &mut tm.tm_year, &mut tm.tm_mon, &mut tm.tm_mday);
    dt2time(time, &mut tm.tm_hour, &mut tm.tm_min, &mut tm.tm_sec, fsec);

    let Some(tzp) = tzp else {
        tm.tm_isdst = -1;
        tm.tm_gmtoff = 0;
        tm.tm_zone = None;
        if let Some(slot) = tzn {
            *slot = None;
        }
        return Ok(());
    };

    // C resolves NULL attimezone to session_timezone only on this branch.
    let attimezone = match attimezone {
        Some(z) => z,
        None => tz::session_timezone()
            .unwrap_or_else(|| panic!("timestamp2tm: session_timezone not initialized")),
    };

    let dt_secs = (dt - *fsec as i64) / USECS_PER_SEC
        + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY as i64;
    if let Some(tx) = tz::pg_localtime(dt_secs, attimezone) {
        tm.tm_year = tx.tm_year + 1900;
        tm.tm_mon = tx.tm_mon + 1;
        tm.tm_mday = tx.tm_mday;
        tm.tm_hour = tx.tm_hour;
        tm.tm_min = tx.tm_min;
        tm.tm_sec = tx.tm_sec;
        tm.tm_isdst = tx.tm_isdst;
        tm.tm_gmtoff = tx.tm_gmtoff;
        tm.tm_zone = tx.tm_zone;
        *tzp = -(tm.tm_gmtoff as i32);
        if let Some(slot) = tzn {
            *slot = tx.tm_zone;
        }
    } else {
        // out of pg_time_t range: treat as GMT (C comment)
        *tzp = 0;
        tm.tm_isdst = -1;
        tm.tm_gmtoff = 0;
        tm.tm_zone = None;
        if let Some(slot) = tzn {
            *slot = None;
        }
    }

    Ok(())
}

#[allow(clippy::result_unit_err)]
pub fn tm2timestamp(
    tm: &pg_tm,
    fsec: fsec_t,
    tzp: Option<i32>,
    result: &mut Timestamp,
) -> Result<(), ()> {
    if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        *result = 0;
        return Err(());
    }

    let date: TimeOffset =
        (date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE) as i64;
    let time = time2t(tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);

    let Some(r) = date.checked_mul(USECS_PER_DAY).and_then(|v| v.checked_add(time)) else {
        *result = 0;
        return Err(());
    };
    *result = r;
    if let Some(tz) = tzp {
        *result = dt2local(*result, -tz);
    }

    if !IS_VALID_TIMESTAMP(*result) {
        *result = 0;
        return Err(());
    }

    Ok(())
}

#[inline]
fn time2t(hour: i32, min: i32, sec: i32, fsec: fsec_t) -> TimeOffset {
    ((((hour * MINS_PER_HOUR) + min) * SECS_PER_MINUTE) + sec) as i64 * USECS_PER_SEC
        + fsec as i64
}

#[inline]
pub fn dt2local(dt: Timestamp, timezone: i32) -> Timestamp {
    dt.wrapping_sub(timezone as i64 * USECS_PER_SEC)
}

pub fn GetEpochTime(tm: &mut pg_tm) {
    let t0 = tz::pg_gmtime(0).expect("could not convert epoch to timestamp");

    tm.tm_year = t0.tm_year;
    tm.tm_mon = t0.tm_mon;
    tm.tm_mday = t0.tm_mday;
    tm.tm_hour = t0.tm_hour;
    tm.tm_min = t0.tm_min;
    tm.tm_sec = t0.tm_sec;

    tm.tm_year += 1900;
    tm.tm_mon += 1;
}

pub fn SetEpochTimestamp() -> Timestamp {
    let mut tm = pg_tm::default();
    let mut dt = 0;
    GetEpochTime(&mut tm);
    let _ = tm2timestamp(&tm, 0, None, &mut dt);
    dt
}

pub fn TimestampDifference(start_time: TimestampTz, stop_time: TimestampTz) -> (i64, i32) {
    let diff = stop_time - start_time;
    if diff <= 0 {
        (0, 0)
    } else {
        (diff / USECS_PER_SEC, (diff % USECS_PER_SEC) as i32)
    }
}

pub fn TimestampDifferenceMilliseconds(start_time: TimestampTz, stop_time: TimestampTz) -> i64 {
    if start_time >= stop_time {
        return 0;
    }
    let Some(diff) = stop_time.checked_sub(start_time) else {
        return i32::MAX as i64;
    };
    if diff >= i32::MAX as i64 * 1000 - 999 {
        i32::MAX as i64
    } else {
        (diff + 999) / 1000
    }
}

pub fn TimestampDifferenceExceeds(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    msec: i32,
) -> bool {
    stop_time - start_time >= msec as i64 * 1000
}

pub fn TimestampDifferenceExceedsSeconds(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    threshold_sec: i32,
) -> bool {
    TimestampDifference(start_time, stop_time).0 >= threshold_sec as i64
}

// Binary wire (timestamp.c recv/send): int8 from the wire, the range check
// timestamp_out would apply, then typmod rounding, exactly as C.
pub fn timestamp_recv(buf: &mut stringinfo::StringInfo<'_>, typmod: i32) -> PgResult<Timestamp> {
    let mut timestamp = pqformat::pq_getmsgint64(buf)?;
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    if !TIMESTAMP_NOT_FINITE(timestamp)
        && (timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err()
            || !IS_VALID_TIMESTAMP(timestamp))
    {
        return Err(timestamp_out_of_range());
    }
    AdjustTimestampForTypmod(&mut timestamp, typmod, None)?;
    Ok(timestamp)
}

pub fn timestamptz_recv(
    buf: &mut stringinfo::StringInfo<'_>,
    typmod: i32,
) -> PgResult<TimestampTz> {
    let mut timestamp = pqformat::pq_getmsgint64(buf)?;
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz = 0;
    if !TIMESTAMP_NOT_FINITE(timestamp)
        && (timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, None).is_err()
            || !IS_VALID_TIMESTAMP(timestamp))
    {
        return Err(timestamp_out_of_range());
    }
    AdjustTimestampForTypmod(&mut timestamp, typmod, None)?;
    Ok(timestamp)
}

pub fn timestamp_send<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    timestamp: Timestamp,
) -> PgResult<datum::Bytea<'mcx>> {
    let mut b = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint64(&mut b, timestamp as u64)?;
    Ok(pqformat::pq_endtypsend(b))
}

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

pub fn timestamp2timestamptz_opt_overflow(
    timestamp: Timestamp,
    mut overflow: Option<&mut i32>,
) -> PgResult<TimestampTz> {
    if let Some(o) = overflow.as_deref_mut() {
        *o = 0;
    }

    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_ok() {
        let tz = tz::DetermineTimeZoneOffset(&mut tm, require_session_timezone());
        let result = dt2local(timestamp, -tz);
        if IS_VALID_TIMESTAMP(result) {
            return Ok(result);
        }
        if let Some(o) = overflow {
            if result < MIN_TIMESTAMP {
                *o = -1;
                return Ok(DT_NOBEGIN);
            }
            *o = 1;
            return Ok(DT_NOEND);
        }
    }

    Err(timestamp_out_of_range())
}

pub fn timestamp2timestamptz(timestamp: Timestamp) -> PgResult<TimestampTz> {
    timestamp2timestamptz_opt_overflow(timestamp, None)
}

pub fn timestamptz2timestamp(timestamp: TimestampTz) -> PgResult<Timestamp> {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz = 0;
    if timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }
    let mut result = 0;
    if tm2timestamp(&tm, fsec, None, &mut result).is_err() {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

pub fn timestamp_cmp_timestamptz_internal(timestamp_val: Timestamp, dt2: TimestampTz) -> i32 {
    let mut overflow = 0;
    let dt1 = timestamp2timestamptz_opt_overflow(timestamp_val, Some(&mut overflow))
        .expect("timestamp2timestamptz_opt_overflow cannot fail with overflow out");
    if overflow > 0 {
        // dt1 is larger than any finite timestamp, but less than infinity
        return if TIMESTAMP_IS_NOEND(dt2) { -1 } else { 1 };
    }
    if overflow < 0 {
        return if TIMESTAMP_IS_NOBEGIN(dt2) { 1 } else { -1 };
    }
    timestamp_cmp_internal(dt1, dt2)
}

#[cold]
fn no_session_timezone() -> ! {
    panic!("session timezone not initialized (pg_timezone_initialize)")
}

fn require_session_timezone() -> &'static PgTz {
    tz::session_timezone().unwrap_or_else(|| no_session_timezone())
}

// C text_to_cstring_buffer(zone, buf, TZ_STRLEN_MAX + 1): at most
// TZ_STRLEN_MAX bytes, C-string semantics end at an embedded NUL.
// DIVERGENCE: C clips at a multibyte character boundary; zone names past 255
// bytes are already unresolvable, so byte truncation only changes the error
// text.
fn text_to_tzname(zone: &[u8]) -> &[u8] {
    let z = &zone[..zone.len().min(TZ_STRLEN_MAX)];
    match z.iter().position(|&b| b == 0) {
        Some(i) => &z[..i],
        None => z,
    }
}

// C downcase_truncate_identifier to NAMEDATALEN-1.
// DIVERGENCE: C also tolower()s high-bit bytes under single-byte encodings
// and clips multibyte-aware; unit/zone keywords are ASCII, so only error
// text for non-ASCII garbage input can differ.
pub fn downcase_ident<'a>(src: &[u8], out: &'a mut [u8; 64]) -> &'a [u8] {
    let n = src.len().min(63);
    for (dst, b) in out.iter_mut().zip(&src[..n]) {
        *dst = b.to_ascii_lowercase();
    }
    &out[..n]
}

#[track_caller]
#[cold]
fn tz_not_recognized(tzname: &[u8]) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "time zone \"{}\" not recognized",
            String::from_utf8_lossy(tzname)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

pub enum TzLookup {
    FixedOffset(i32),
    DynTz(&'static PgTz),
    Zone(&'static PgTz),
}

pub fn DecodeTimezoneName(tzname: &[u8]) -> PgResult<TzLookup> {
    // Abbreviation table first, then the timezone database: matches timestamp
    // input's order (tzdb reuses a few names identical to abbreviations).
    let mut low = [0u8; 64];
    let lowzone = downcase_ident(tzname, &mut low);

    let mut ftype = 0;
    let mut offset = 0;
    let mut ztz: Option<&'static PgTz> = None;
    let mut extra = DateTimeErrorExtra::default();
    let dterr = DecodeTimezoneAbbrev(0, lowzone, &mut ftype, &mut offset, &mut ztz, &mut extra);
    if dterr != 0 {
        DateTimeParseError(dterr, Some(&extra), "", "", None)?;
        unreachable!("DateTimeParseError returned without escontext");
    }

    if ftype == TZ || ftype == DTZ {
        Ok(TzLookup::FixedOffset(offset))
    } else if ftype == DYNTZ {
        Ok(TzLookup::DynTz(ztz.expect("DYNTZ abbreviation without zone")))
    } else {
        match tz::pg_tzset(tzname) {
            Some(t) => Ok(TzLookup::Zone(t)),
            None => Err(tz_not_recognized(tzname)),
        }
    }
}

pub fn DecodeTimezoneNameToTz(tzname: &[u8]) -> PgResult<&'static PgTz> {
    match DecodeTimezoneName(tzname)? {
        // flip to the POSIX sign convention
        TzLookup::FixedOffset(offset) => Ok(tz::pg_tzset_offset(-offset as i64)
            .expect("fixed abbreviation offset representable as a zone")),
        TzLookup::DynTz(t) | TzLookup::Zone(t) => Ok(t),
    }
}

/// C `lookup_timezone`: text payload instead of a text Datum.
pub fn lookup_timezone(zone: &[u8]) -> PgResult<&'static PgTz> {
    DecodeTimezoneNameToTz(text_to_tzname(zone))
}

pub fn parse_sane_timezone(tm: &mut pg_tm, zone: &[u8]) -> PgResult<i32> {
    #[track_caller]
    #[cold]
    fn digit_first(tzname: &[u8]) -> Box<PgError> {
        Box::new(
            PgError::error(format!(
                "invalid input syntax for type {}: \"{}\"",
                "numeric time zone",
                String::from_utf8_lossy(tzname)
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_hint("Numeric time zones must have \"-\" or \"+\" as first character."),
        )
    }
    #[track_caller]
    #[cold]
    fn numeric_tz_out_of_range(tzname: &[u8]) -> Box<PgError> {
        Box::new(
            PgError::error(format!(
                "numeric time zone \"{}\" out of range",
                String::from_utf8_lossy(tzname)
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        )
    }

    let tzname = text_to_tzname(zone);

    // pg_tzset happily parses numeric input DecodeTimezone rejects; a leading
    // digit is disallowed so such input stays invalid.
    if tzname.first().is_some_and(u8::is_ascii_digit) {
        return Err(digit_first(tzname));
    }

    let mut tz = 0;
    let dterr = DecodeTimezone(tzname, &mut tz);
    if dterr != 0 {
        if dterr == DTERR_TZDISP_OVERFLOW {
            return Err(numeric_tz_out_of_range(tzname));
        }
        if dterr != DTERR_BAD_FORMAT {
            return Err(tz_not_recognized(tzname));
        }

        tz = match DecodeTimezoneName(tzname)? {
            TzLookup::FixedOffset(val) => -val,
            TzLookup::DynTz(tzp) => tz::DetermineTimeZoneAbbrevOffset(tm, tzname, tzp),
            TzLookup::Zone(tzp) => tz::DetermineTimeZoneOffset(tm, tzp),
        };
    }

    Ok(tz)
}

pub fn timestamptz_to_time_t(t: TimestampTz) -> i64 {
    t / USECS_PER_SEC + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY as i64
}

pub fn DetermineTimeZoneAbbrevOffsetTS(
    ts: TimestampTz,
    abbr: &[u8],
    tzp: &'static PgTz,
    isdst: &mut i32,
) -> PgResult<i32> {
    let t = timestamptz_to_time_t(ts);
    if let Some((gmtoff, dst)) = tz::interpret_timezone_abbrev_at(abbr, t, tzp) {
        *isdst = dst;
        // Change sign to agree with DetermineTimeZoneOffset().
        return Ok(-(gmtoff as i32));
    }

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz_ = 0;
    if timestamp2tm(ts, Some(&mut tz_), &mut tm, &mut fsec, None, Some(tzp)).is_err() {
        return Err(timestamp_out_of_range());
    }
    let zone_offset = tz::DetermineTimeZoneOffset(&mut tm, tzp);
    *isdst = tm.tm_isdst;
    Ok(zone_offset)
}

/// C `timestamp_zone` on the zone text payload.
pub fn timestamp_zone(zone: &[u8], timestamp: Timestamp) -> PgResult<TimestampTz> {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }

    let tzname = text_to_tzname(zone);
    let result = match DecodeTimezoneName(tzname)? {
        TzLookup::FixedOffset(val) => dt2local(timestamp, val),
        TzLookup::DynTz(tzp) => {
            let mut tm = pg_tm::default();
            let mut fsec: fsec_t = 0;
            if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, Some(tzp)).is_err() {
                return Err(timestamp_out_of_range());
            }
            let tz = -tz::DetermineTimeZoneAbbrevOffset(&mut tm, tzname, tzp);
            dt2local(timestamp, tz)
        }
        TzLookup::Zone(tzp) => {
            let mut tm = pg_tm::default();
            let mut fsec: fsec_t = 0;
            if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, Some(tzp)).is_err() {
                return Err(timestamp_out_of_range());
            }
            let tz = tz::DetermineTimeZoneOffset(&mut tm, tzp);
            let mut r = 0;
            if tm2timestamp(&tm, fsec, Some(tz), &mut r).is_err() {
                return Err(timestamp_out_of_range());
            }
            r
        }
    };

    if !IS_VALID_TIMESTAMP(result) {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

/// C `timestamptz_zone` on the zone text payload.
pub fn timestamptz_zone(zone: &[u8], timestamp: TimestampTz) -> PgResult<Timestamp> {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }

    let tzname = text_to_tzname(zone);
    let result = match DecodeTimezoneName(tzname)? {
        TzLookup::FixedOffset(val) => dt2local(timestamp, -val),
        TzLookup::DynTz(tzp) => {
            let mut isdst = 0;
            let tz = DetermineTimeZoneAbbrevOffsetTS(timestamp, tzname, tzp, &mut isdst)?;
            dt2local(timestamp, tz)
        }
        TzLookup::Zone(tzp) => {
            let mut tm = pg_tm::default();
            let mut fsec: fsec_t = 0;
            let mut tz = 0;
            if timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, Some(tzp))
                .is_err()
            {
                return Err(timestamp_out_of_range());
            }
            let mut r = 0;
            if tm2timestamp(&tm, fsec, None, &mut r).is_err() {
                return Err(timestamp_out_of_range());
            }
            r
        }
    };

    if !IS_VALID_TIMESTAMP(result) {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

pub fn make_timestamp_internal(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: f64,
) -> PgResult<Timestamp> {
    #[track_caller]
    #[cold]
    fn date_field_out_of_range(y: i32, m: i32, d: i32) -> Box<PgError> {
        Box::new(
            PgError::error(format!("date field value out of range: {y}-{m:02}-{d:02}"))
                .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW),
        )
    }
    #[track_caller]
    #[cold]
    fn date_out_of_range(y: i32, m: i32, d: i32) -> Box<PgError> {
        Box::new(
            PgError::error(format!("date out of range: {y}-{m:02}-{d:02}"))
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        )
    }
    #[track_caller]
    #[cold]
    fn ts_out_of_range(y: i32, m: i32, d: i32, h: i32, mi: i32, s: f64) -> Box<PgError> {
        // C (timestamp.c make_timestamp_internal): "%d-%02d-%02d %d:%02d:%02g".
        let s = adt_datetime::errors::fmt_sec_g02(s);
        Box::new(
            PgError::error(format!("timestamp out of range: {y}-{m:02}-{d:02} {h}:{mi:02}:{s}"))
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        )
    }

    let mut tm = pg_tm { tm_year: year, tm_mon: month, tm_mday: day, ..pg_tm::default() };

    // Handle negative years as BC.
    let mut bc = false;
    if tm.tm_year < 0 {
        bc = true;
        tm.tm_year = tm.tm_year.wrapping_neg();
    }

    if ValidateDate(DTK_DATE_M, false, false, bc, &mut tm) != 0 {
        return Err(date_field_out_of_range(year, month, day));
    }

    if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        return Err(date_out_of_range(year, month, day));
    }

    let date: TimeOffset =
        (date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE) as i64;

    #[track_caller]
    #[cold]
    fn time_field_out_of_range(h: i32, m: i32, s: f64) -> Box<PgError> {
        Box::new(
            PgError::error(format!(
                "time field value out of range: {h}:{m:02}:{}",
                adt_datetime::errors::fmt_sec_g02(s)
            ))
                .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW),
        )
    }

    if float_time_overflows(hour, min, sec) {
        return Err(time_field_out_of_range(hour, min, sec));
    }

    // This should match tm2time.
    let time = ((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE) as i64 * USECS_PER_SEC
        + (sec * USECS_PER_SEC as f64).round_ties_even() as i64;

    let Some(result) = date.checked_mul(USECS_PER_DAY).and_then(|v| v.checked_add(time)) else {
        return Err(ts_out_of_range(year, month, day, hour, min, sec));
    };

    if !IS_VALID_TIMESTAMP(result) {
        return Err(ts_out_of_range(year, month, day, hour, min, sec));
    }

    Ok(result)
}

pub fn make_timestamp(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: f64,
) -> PgResult<Timestamp> {
    make_timestamp_internal(year, month, day, hour, min, sec)
}

pub fn make_timestamptz(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: f64,
) -> PgResult<TimestampTz> {
    timestamp2timestamptz(make_timestamp_internal(year, month, day, hour, min, sec)?)
}

pub fn make_timestamptz_at_timezone(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: f64,
    zone: &[u8],
) -> PgResult<TimestampTz> {
    let timestamp = make_timestamp_internal(year, month, day, hour, min, sec)?;

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

pub const TIMESTAMP_TYPE_NAME: &str = "timestamp without time zone";
pub const TIMESTAMPTZ_TYPE_NAME: &str = "timestamp with time zone";

fn type_name(is_tz: bool) -> &'static str {
    // format_type_be(TIMESTAMP[TZ]OID) output for these constant OIDs; a
    // catalog probe on this cold error path buys nothing.
    if is_tz {
        TIMESTAMPTZ_TYPE_NAME
    } else {
        TIMESTAMP_TYPE_NAME
    }
}

#[track_caller]
#[cold]
fn unit_not_supported(lowunits: &[u8], is_tz: bool) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "unit \"{}\" not supported for type {}",
            String::from_utf8_lossy(lowunits),
            type_name(is_tz)
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[track_caller]
#[cold]
fn unit_not_recognized(lowunits: &[u8], is_tz: bool) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "unit \"{}\" not recognized for type {}",
            String::from_utf8_lossy(lowunits),
            type_name(is_tz)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

fn trunc_unit_supported(val: i32) -> bool {
    matches!(
        val,
        DTK_WEEK
            | DTK_MILLENNIUM
            | DTK_CENTURY
            | DTK_DECADE
            | DTK_YEAR
            | DTK_QUARTER
            | DTK_MONTH
            | DTK_DAY
            | DTK_HOUR
            | DTK_MINUTE
            | DTK_SECOND
            | DTK_MILLISEC
            | DTK_MICROSEC
    )
}

// The switch fall-through chain shared by timestamp_trunc and
// timestamptz_trunc_internal; redotz is Some only for the tz variant.
fn trunc_apply(val: i32, tm: &mut pg_tm, fsec: &mut fsec_t, mut redotz: Option<&mut bool>) {
    const CHAIN: [i32; 10] = [
        DTK_MILLENNIUM, DTK_CENTURY, DTK_DECADE, DTK_YEAR, DTK_QUARTER, DTK_MONTH, DTK_DAY,
        DTK_HOUR, DTK_MINUTE, DTK_SECOND,
    ];

    match val {
        DTK_WEEK => {
            let woy = date2isoweek(tm.tm_year, tm.tm_mon, tm.tm_mday);
            // Week 52/53 in January belongs to the previous year; some
            // December dates belong to the next.
            if woy >= 52 && tm.tm_mon == 1 {
                tm.tm_year -= 1;
            }
            if woy <= 1 && tm.tm_mon == MONTHS_PER_YEAR {
                tm.tm_year += 1;
            }
            isoweek2date(woy, &mut tm.tm_year, &mut tm.tm_mon, &mut tm.tm_mday);
            tm.tm_hour = 0;
            tm.tm_min = 0;
            tm.tm_sec = 0;
            *fsec = 0;
            if let Some(r) = redotz {
                *r = true;
            }
        }
        DTK_MILLISEC => *fsec = (*fsec / 1000) * 1000,
        DTK_MICROSEC => {}
        _ => {
            let start = CHAIN.iter().position(|&v| v == val).expect("trunc_unit_supported");
            for &step in &CHAIN[start..] {
                match step {
                    // first year of the millennium: -1000, 1, 1001, 2001...
                    DTK_MILLENNIUM => {
                        if tm.tm_year > 0 {
                            tm.tm_year = ((tm.tm_year + 999) / 1000) * 1000 - 999;
                        } else {
                            tm.tm_year = -((999 - (tm.tm_year - 1)) / 1000) * 1000 + 1;
                        }
                    }
                    DTK_CENTURY => {
                        if tm.tm_year > 0 {
                            tm.tm_year = ((tm.tm_year + 99) / 100) * 100 - 99;
                        } else {
                            tm.tm_year = -((99 - (tm.tm_year - 1)) / 100) * 100 + 1;
                        }
                    }
                    // must not apply when the year was truncated above
                    DTK_DECADE if val != DTK_MILLENNIUM && val != DTK_CENTURY => {
                        if tm.tm_year > 0 {
                            tm.tm_year = (tm.tm_year / 10) * 10;
                        } else {
                            tm.tm_year = -((8 - (tm.tm_year - 1)) / 10) * 10;
                        }
                    }
                    DTK_DECADE => {}
                    DTK_YEAR => tm.tm_mon = 1,
                    DTK_QUARTER => tm.tm_mon = 3 * ((tm.tm_mon - 1) / 3) + 1,
                    DTK_MONTH => tm.tm_mday = 1,
                    DTK_DAY => {
                        tm.tm_hour = 0;
                        if let Some(r) = redotz.as_deref_mut() {
                            *r = true;
                        }
                    }
                    DTK_HOUR => tm.tm_min = 0,
                    DTK_MINUTE => tm.tm_sec = 0,
                    DTK_SECOND => *fsec = 0,
                    _ => unreachable!(),
                }
            }
        }
    }
}

pub fn timestamp_trunc(units: &[u8], timestamp: Timestamp) -> PgResult<Timestamp> {
    let mut low = [0u8; 64];
    let lowunits = downcase_ident(units, &mut low);

    let mut val = 0;
    let type_ = DecodeUnits(0, lowunits, &mut val);
    if type_ != UNITS {
        return Err(unit_not_recognized(lowunits, false));
    }

    if TIMESTAMP_NOT_FINITE(timestamp) {
        if trunc_unit_supported(val) {
            return Ok(timestamp);
        }
        return Err(unit_not_supported(lowunits, false));
    }

    if !trunc_unit_supported(val) {
        return Err(unit_not_supported(lowunits, false));
    }

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }

    trunc_apply(val, &mut tm, &mut fsec, None);

    let mut result = 0;
    if tm2timestamp(&tm, fsec, None, &mut result).is_err() {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

pub fn timestamptz_trunc_internal(
    units: &[u8],
    timestamp: TimestampTz,
    tzp: &'static PgTz,
) -> PgResult<TimestampTz> {
    let mut low = [0u8; 64];
    let lowunits = downcase_ident(units, &mut low);

    let mut val = 0;
    let type_ = DecodeUnits(0, lowunits, &mut val);
    if type_ != UNITS {
        return Err(unit_not_recognized(lowunits, true));
    }

    if TIMESTAMP_NOT_FINITE(timestamp) {
        if trunc_unit_supported(val) {
            return Ok(timestamp);
        }
        return Err(unit_not_supported(lowunits, true));
    }

    if !trunc_unit_supported(val) {
        return Err(unit_not_supported(lowunits, true));
    }

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz = 0;
    if timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, Some(tzp)).is_err() {
        return Err(timestamp_out_of_range());
    }

    let mut redotz = false;
    trunc_apply(val, &mut tm, &mut fsec, Some(&mut redotz));

    if redotz {
        tz = tz::DetermineTimeZoneOffset(&mut tm, tzp);
    }

    let mut result = 0;
    if tm2timestamp(&tm, fsec, Some(tz), &mut result).is_err() {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

pub fn timestamptz_trunc(units: &[u8], timestamp: TimestampTz) -> PgResult<TimestampTz> {
    timestamptz_trunc_internal(units, timestamp, require_session_timezone())
}

pub fn timestamptz_trunc_zone(
    units: &[u8],
    timestamp: TimestampTz,
    zone: &[u8],
) -> PgResult<TimestampTz> {
    let tzp = lookup_timezone(zone)?;
    timestamptz_trunc_internal(units, timestamp, tzp)
}

pub enum PartValue {
    Null,
    Float(f64),
    Numeric(NumericImage),
}

impl core::fmt::Debug for PartValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PartValue::Null => f.write_str("Null"),
            PartValue::Float(v) => write!(f, "Float({v})"),
            PartValue::Numeric(_) => f.write_str("Numeric(..)"),
        }
    }
}

fn NonFiniteTimestampTzPart(
    type_: i32,
    unit: i32,
    lowunits: &[u8],
    is_negative: bool,
    is_tz: bool,
) -> PgResult<f64> {
    if type_ != UNITS && type_ != RESERV {
        return Err(unit_not_recognized(lowunits, is_tz));
    }

    match unit {
        // Oscillating units
        DTK_MICROSEC | DTK_MILLISEC | DTK_SECOND | DTK_MINUTE | DTK_HOUR | DTK_DAY
        | DTK_MONTH | DTK_QUARTER | DTK_WEEK | DTK_DOW | DTK_ISODOW | DTK_DOY | DTK_TZ
        | DTK_TZ_MINUTE | DTK_TZ_HOUR => Ok(0.0),

        // Monotonically-increasing units
        DTK_YEAR | DTK_DECADE | DTK_CENTURY | DTK_MILLENNIUM | DTK_JULIAN | DTK_ISOYEAR
        | DTK_EPOCH => Ok(if is_negative { f64::NEG_INFINITY } else { f64::INFINITY }),

        _ => Err(unit_not_supported(lowunits, is_tz)),
    }
}

fn nonfinite_part_value(r: f64, retnumeric: bool) -> PgResult<PartValue> {
    if r == 0.0 {
        return Ok(PartValue::Null);
    }
    if !retnumeric {
        return Ok(PartValue::Float(r));
    }
    let lit = if r < 0.0 { "-Infinity" } else { "Infinity" };
    Ok(PartValue::Numeric(
        numeric_in(lit, -1, None)?.expect("infinity literal parses"),
    ))
}

fn part_julian(tm: &pg_tm, fsec: fsec_t, retnumeric: bool) -> PgResult<PartValue> {
    let day_secs = ((tm.tm_hour * MINS_PER_HOUR + tm.tm_min) * SECS_PER_MINUTE) + tm.tm_sec;
    if retnumeric {
        let jd = int64_to_numeric(date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) as i64);
        let usecs = int64_to_numeric(day_secs as i64 * 1_000_000 + fsec as i64);
        let day_usecs = int64_to_numeric(SECS_PER_DAY as i64 * 1_000_000);
        let frac = numeric_div_common(usecs.num(), day_usecs.num())?;
        Ok(PartValue::Numeric(numeric_add_common(jd.num(), frac.num())?))
    } else {
        Ok(PartValue::Float(
            date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) as f64
                + (day_secs as f64 + fsec as f64 / 1_000_000.0) / SECS_PER_DAY as f64,
        ))
    }
}

fn part_epoch(timestamp: i64, retnumeric: bool) -> PgResult<PartValue> {
    let epoch = SetEpochTimestamp();
    // C computes PG_INT64_MAX + epoch in wrapping int64; epoch is negative so
    // the guard means timestamp - epoch cannot overflow.
    let no_overflow = timestamp < i64::MAX.wrapping_add(epoch);
    if retnumeric {
        if no_overflow {
            Ok(PartValue::Numeric(int64_div_fast_to_numeric(timestamp - epoch, 6)?))
        } else {
            let t = int64_to_numeric(timestamp);
            let e = int64_to_numeric(epoch);
            let diff = numeric_sub_common(t.num(), e.num())?;
            let m = int64_to_numeric(1_000_000);
            let q = numeric_div_common(diff.num(), m.num())?;
            Ok(PartValue::Numeric(numeric_round_common(q.num(), 6)?))
        }
    } else if no_overflow {
        Ok(PartValue::Float((timestamp - epoch) as f64 / 1_000_000.0))
    } else {
        Ok(PartValue::Float((timestamp as f64 - epoch as f64) / 1_000_000.0))
    }
}

// The UNITS arms shared verbatim between timestamp_part_common and
// timestamptz_part_common (the tz variant adds DTK_TZ* before these).
fn part_units_common(
    val: i32,
    tm: &pg_tm,
    fsec: fsec_t,
    retnumeric: bool,
) -> PgResult<Option<Result<i64, PartValue>>> {
    let intresult: i64 = match val {
        DTK_MICROSEC => tm.tm_sec as i64 * 1_000_000 + fsec as i64,

        DTK_MILLISEC => {
            // tm_sec * 1000 + fsec / 1000 = (tm_sec * 1'000'000 + fsec) / 1000
            return Ok(Some(Err(if retnumeric {
                PartValue::Numeric(int64_div_fast_to_numeric(
                    tm.tm_sec as i64 * 1_000_000 + fsec as i64,
                    3,
                )?)
            } else {
                PartValue::Float(tm.tm_sec as f64 * 1000.0 + fsec as f64 / 1000.0)
            })));
        }

        DTK_SECOND => {
            return Ok(Some(Err(if retnumeric {
                PartValue::Numeric(int64_div_fast_to_numeric(
                    tm.tm_sec as i64 * 1_000_000 + fsec as i64,
                    6,
                )?)
            } else {
                PartValue::Float(tm.tm_sec as f64 + fsec as f64 / 1_000_000.0)
            })));
        }

        DTK_MINUTE => tm.tm_min as i64,
        DTK_HOUR => tm.tm_hour as i64,
        DTK_DAY => tm.tm_mday as i64,
        DTK_MONTH => tm.tm_mon as i64,
        DTK_QUARTER => ((tm.tm_mon - 1) / 3 + 1) as i64,
        DTK_WEEK => date2isoweek(tm.tm_year, tm.tm_mon, tm.tm_mday) as i64,

        DTK_YEAR => {
            if tm.tm_year > 0 {
                tm.tm_year as i64
            } else {
                // there is no year 0, just 1 BC and 1 AD
                (tm.tm_year - 1) as i64
            }
        }

        // decade 199 is 1990 thru 1999; decade 0 starts on year 1 BC
        DTK_DECADE => {
            if tm.tm_year >= 0 {
                (tm.tm_year / 10) as i64
            } else {
                -(((8 - (tm.tm_year - 1)) / 10) as i64)
            }
        }

        // centuries AD c>0: [(c-1)*100+1, c*100]; no century 0
        DTK_CENTURY => {
            if tm.tm_year > 0 {
                ((tm.tm_year + 99) / 100) as i64
            } else {
                -(((99 - (tm.tm_year - 1)) / 100) as i64)
            }
        }

        DTK_MILLENNIUM => {
            if tm.tm_year > 0 {
                ((tm.tm_year + 999) / 1000) as i64
            } else {
                -(((999 - (tm.tm_year - 1)) / 1000) as i64)
            }
        }

        DTK_JULIAN => return Ok(Some(Err(part_julian(tm, fsec, retnumeric)?))),

        DTK_ISOYEAR => {
            let mut r = date2isoyear(tm.tm_year, tm.tm_mon, tm.tm_mday) as i64;
            // Adjust BC years
            if r <= 0 {
                r -= 1;
            }
            r
        }

        DTK_DOW | DTK_ISODOW => {
            let mut r = j2day(date2j(tm.tm_year, tm.tm_mon, tm.tm_mday)) as i64;
            if val == DTK_ISODOW && r == 0 {
                r = 7;
            }
            r
        }

        DTK_DOY => {
            (date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - date2j(tm.tm_year, 1, 1) + 1) as i64
        }

        _ => return Ok(None),
    };
    Ok(Some(Ok(intresult)))
}

fn finish_part(intresult: i64, retnumeric: bool) -> PartValue {
    if retnumeric {
        PartValue::Numeric(int64_to_numeric(intresult))
    } else {
        PartValue::Float(intresult as f64)
    }
}

pub fn timestamp_part_common(
    units: &[u8],
    timestamp: Timestamp,
    retnumeric: bool,
) -> PgResult<PartValue> {
    let mut low = [0u8; 64];
    let lowunits = downcase_ident(units, &mut low);

    let mut val = 0;
    let mut type_ = DecodeUnits(0, lowunits, &mut val);
    if type_ == UNKNOWN_FIELD {
        type_ = DecodeSpecial(0, lowunits, &mut val);
    }

    if TIMESTAMP_NOT_FINITE(timestamp) {
        let r = NonFiniteTimestampTzPart(
            type_,
            val,
            lowunits,
            TIMESTAMP_IS_NOBEGIN(timestamp),
            false,
        )?;
        return nonfinite_part_value(r, retnumeric);
    }

    if type_ == UNITS {
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err() {
            return Err(timestamp_out_of_range());
        }

        match part_units_common(val, &tm, fsec, retnumeric)? {
            Some(Ok(intresult)) => Ok(finish_part(intresult, retnumeric)),
            Some(Err(v)) => Ok(v),
            // DTK_TZ / DTK_TZ_MINUTE / DTK_TZ_HOUR land here too
            None => Err(unit_not_supported(lowunits, false)),
        }
    } else if type_ == RESERV {
        if val == DTK_EPOCH {
            part_epoch(timestamp, retnumeric)
        } else {
            Err(unit_not_supported(lowunits, false))
        }
    } else {
        Err(unit_not_recognized(lowunits, false))
    }
}

pub fn timestamptz_part_common(
    units: &[u8],
    timestamp: TimestampTz,
    retnumeric: bool,
) -> PgResult<PartValue> {
    let mut low = [0u8; 64];
    let lowunits = downcase_ident(units, &mut low);

    let mut val = 0;
    let mut type_ = DecodeUnits(0, lowunits, &mut val);
    if type_ == UNKNOWN_FIELD {
        type_ = DecodeSpecial(0, lowunits, &mut val);
    }

    if TIMESTAMP_NOT_FINITE(timestamp) {
        let r = NonFiniteTimestampTzPart(
            type_,
            val,
            lowunits,
            TIMESTAMP_IS_NOBEGIN(timestamp),
            true,
        )?;
        return nonfinite_part_value(r, retnumeric);
    }

    if type_ == UNITS {
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        let mut tz = 0;
        if timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, None).is_err() {
            return Err(timestamp_out_of_range());
        }

        let intresult: i64 = match val {
            DTK_TZ => -tz as i64,
            DTK_TZ_MINUTE => ((-tz / SECS_PER_MINUTE) % MINS_PER_HOUR) as i64,
            DTK_TZ_HOUR => (-tz / SECS_PER_HOUR) as i64,
            _ => match part_units_common(val, &tm, fsec, retnumeric)? {
                Some(Ok(intresult)) => intresult,
                Some(Err(v)) => return Ok(v),
                None => return Err(unit_not_supported(lowunits, true)),
            },
        };
        Ok(finish_part(intresult, retnumeric))
    } else if type_ == RESERV {
        if val == DTK_EPOCH {
            part_epoch(timestamp, retnumeric)
        } else {
            Err(unit_not_supported(lowunits, true))
        }
    } else {
        Err(unit_not_recognized(lowunits, true))
    }
}

pub fn GetSQLLocalTimestamp(typmod: i32) -> PgResult<Timestamp> {
    let mut ts = timestamptz2timestamp(xact::GetCurrentTransactionStartTimestamp())?;
    if typmod >= 0 {
        AdjustTimestampForTypmod(&mut ts, typmod, None)?;
    }
    Ok(ts)
}

/// C `timeofday` body: formatted text into `buf`, returns the length.
pub fn timeofday_into(buf: &mut [u8; 128]) -> usize {
    // DST P2 (contract §1.2): gettimeofday -> pg_clock::wall_timeval().
    let (tv_sec, tv_usec) = pg_clock::wall_timeval();

    let zone = require_session_timezone();
    let tx = localtime::pg_localtime(tv_sec, zone)
        .expect("current time within pg_localtime range");
    let mut templ = [0u8; 128];
    let n = strftime::pg_strftime(&mut templ, b"%a %b %d %H:%M:%S.%%06d %Y %Z", &tx)
        .expect("timeofday template fits");

    // C's second step: snprintf(buf, templ, tv_usec) fills the %06d hole.
    let pos = templ[..n]
        .windows(4)
        .position(|w| w == b"%06d")
        .expect("template keeps the %06d hole");
    buf[..pos].copy_from_slice(&templ[..pos]);
    let mut usec = tv_usec;
    for i in (0..6).rev() {
        buf[pos + i] = b'0' + (usec % 10) as u8;
        usec /= 10;
    }
    let tail = n - (pos + 4);
    buf[pos + 6..pos + 6 + tail].copy_from_slice(&templ[pos + 4..n]);
    pos + 6 + tail
}

#[derive(Clone, Copy)]
struct CurrentTmCache {
    ts: TimestampTz,
    zone: &'static PgTz,
    tm: pg_tm,
    fsec: fsec_t,
    tz: i32,
}

std::thread_local! {
    // C's cache_ts/cache_timezone memo in GetCurrentTimeUsec: now() is fixed
    // within a transaction, so the breakdown recomputes only when the
    // timestamp or the session timezone (identified by pointer, unique per
    // pg_tzset entry) changes.
    static CURRENT_TM_CACHE: core::cell::Cell<Option<CurrentTmCache>> =
        const { core::cell::Cell::new(None) };
}

pub fn GetCurrentTimeUsec(
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tzp: Option<&mut i32>,
) -> PgResult<()> {
    let cur_ts = xact::GetCurrentTransactionStartTimestamp();
    let zone = require_session_timezone();

    let cached = CURRENT_TM_CACHE.with(core::cell::Cell::get);
    let e = match cached {
        Some(e) if e.ts == cur_ts && core::ptr::eq(e.zone, zone) => e,
        _ => {
            // invalidate first so an error inside timestamp2tm cannot leave a
            // partially-updated entry marked valid
            CURRENT_TM_CACHE.with(|c| c.set(None));
            let mut e =
                CurrentTmCache { ts: cur_ts, zone, tm: pg_tm::default(), fsec: 0, tz: 0 };
            if timestamp2tm(cur_ts, Some(&mut e.tz), &mut e.tm, &mut e.fsec, None, Some(zone))
                .is_err()
            {
                return Err(timestamp_out_of_range());
            }
            CURRENT_TM_CACHE.with(|c| c.set(Some(e)));
            e
        }
    };

    *tm = e.tm;
    *fsec = e.fsec;
    if let Some(tzp) = tzp {
        *tzp = e.tz;
    }
    Ok(())
}

pub fn GetCurrentDateTime(tm: &mut pg_tm) -> PgResult<()> {
    let mut fsec: fsec_t = 0;
    GetCurrentTimeUsec(tm, &mut fsec, None)
}

fn current_time_usec_snapshot() -> PgResult<timestamp_seams::CurrentTimeUsec> {
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz = 0;
    GetCurrentTimeUsec(&mut tm, &mut fsec, Some(&mut tz))?;
    Ok(timestamp_seams::CurrentTimeUsec {
        tm_sec: tm.tm_sec,
        tm_min: tm.tm_min,
        tm_hour: tm.tm_hour,
        tm_mday: tm.tm_mday,
        tm_mon: tm.tm_mon,
        tm_year: tm.tm_year,
        tm_wday: tm.tm_wday,
        tm_yday: tm.tm_yday,
        tm_isdst: tm.tm_isdst,
        tm_gmtoff: tm.tm_gmtoff,
        tm_zone: tm.tm_zone,
        fsec,
        tz,
    })
}

pub fn init_seams() {
    timestamp_seams::get_current_timestamp::set(GetCurrentTimestamp);
    timestamp_seams::get_current_datetime::set(current_time_usec_snapshot);
    timestamp_seams::get_current_time_usec::set(current_time_usec_snapshot);
    timestamp_seams::timestamptz_to_str::set(timestamptz_to_str);
}

// timestamp.c timestamptz_to_str (elog/debug support; NULL attimezone =
// session timezone, like C).
pub fn timestamptz_to_str(t: TimestampTz) -> String {
    let mut buf = [0u8; adt_datetime::MAXDATELEN + 1];
    let n = if TIMESTAMP_NOT_FINITE(t) {
        EncodeSpecialTimestamp(t, &mut buf)
    } else {
        let mut tz: i32 = 0;
        let mut tm = adt_datetime::consts::pg_tm::default();
        let mut fsec: adt_datetime::consts::fsec_t = 0;
        let mut tzn: Option<&'static str> = None;
        if timestamp2tm(t, Some(&mut tz), &mut tm, &mut fsec, Some(&mut tzn), None).is_ok() {
            adt_datetime::EncodeDateTime(
                &mut tm,
                fsec,
                true,
                tz,
                tzn.map(str::as_bytes),
                adt_datetime::USE_ISO_DATES,
                &mut buf,
            )
        } else {
            let msg = b"(timestamp out of range)";
            buf[..msg.len()].copy_from_slice(msg);
            msg.len()
        }
    };
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

pub fn timestamp_scale(timestamp: Timestamp, typmod: i32) -> PgResult<Timestamp> {
    let mut result = timestamp;
    AdjustTimestampForTypmod(&mut result, typmod, None)?;
    Ok(result)
}

// C %g (precision 6) for the float8_timestamptz range error text.
#[cold]
fn fmt_g6(v: f64) -> String {
    let e_str = format!("{:.5e}", v);
    let exp: i32 = e_str.rsplit('e').next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let s = if exp < -4 || exp >= 6 {
        let (mant, _) = e_str.split_once('e').unwrap();
        let mant = mant.trim_end_matches('0').trim_end_matches('.');
        format!("{mant}e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.abs())
    } else {
        let fixed = format!("{:.*}", (5 - exp).max(0) as usize, v);
        if fixed.contains('.') {
            fixed.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            fixed
        }
    };
    s
}

#[track_caller]
#[cold]
fn float_timestamp_out_of_range(seconds: f64) -> Box<PgError> {
    Box::new(
        PgError::error(format!("timestamp out of range: \"{}\"", fmt_g6(seconds)))
            .with_sqlstate(::types_error::ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

pub fn float8_timestamptz(seconds: f64) -> PgResult<TimestampTz> {
    use adt_datetime::consts::{SECS_PER_DAY, UNIX_EPOCH_JDATE, USECS_PER_SEC};
    const DATETIME_MIN_JULIAN: i32 = 0;
    const TIMESTAMP_END_JULIAN: i32 = 109_203_528;

    if seconds.is_nan() {
        return Err(Box::new(
            PgError::error("timestamp cannot be NaN")
                .with_sqlstate(::types_error::ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        ));
    }
    if seconds.is_infinite() {
        return Ok(if seconds < 0.0 { DT_NOBEGIN } else { DT_NOEND });
    }

    let arg = seconds;
    if seconds < SECS_PER_DAY as f64 * (DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE) as f64
        || seconds >= SECS_PER_DAY as f64 * (TIMESTAMP_END_JULIAN - UNIX_EPOCH_JDATE) as f64
    {
        return Err(float_timestamp_out_of_range(arg));
    }

    let seconds = seconds - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as f64 * SECS_PER_DAY as f64);
    let seconds = (seconds * USECS_PER_SEC as f64).round_ties_even();
    let result = seconds as i64;

    if !IS_VALID_TIMESTAMP(result) {
        return Err(float_timestamp_out_of_range(arg));
    }
    Ok(result)
}
