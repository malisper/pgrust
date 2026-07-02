//! timestamp.c core: GetCurrentTimestamp/GetSQLCurrentTimestamp, the
//! timestamp/timestamptz text I/O over adt_datetime, and timestamp2tm/
//! tm2timestamp. Zero-allocation I/O: parse fields borrow a caller workbuf,
//! output writes into a caller-owned MAXDATELEN buffer (no cstring detour).
//! Interval half deferred with datetime's interval note; timestamptz paths
//! resolve zones through the adt_datetime tz boundary (GMT arm live, IANA
//! zones panic until backend-timezone lands).

#![allow(non_snake_case)]

use adt_datetime::tz::{self, PgTz};
use adt_datetime::{
    date2j, dt2time, j2date, pg_tm, DateTimeErrorExtra, DateTimeParseError, DecodeDateTime,
    EncodeDateTime, ParseDateTime, fsec_t, Timestamp, TimeOffset, DTK_DATE, DTK_EARLY, DTK_EPOCH,
    DTK_LATE, MAXDATEFIELDS, MAXDATELEN, MAX_TIMESTAMP_PRECISION, IS_VALID_JULIAN,
    POSTGRES_EPOCH_JDATE, SECS_PER_DAY, UNIX_EPOCH_JDATE, USECS_PER_DAY, USECS_PER_SEC,
    MINS_PER_HOUR, SECS_PER_MINUTE,
};
use types_core::TimestampTz;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
    ERRCODE_INVALID_PARAMETER_VALUE,
};

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
fn timestamp_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("timestamp out of range")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

pub fn GetCurrentTimestamp() -> TimestampTz {
    let mut tp = libc::timeval { tv_sec: 0, tv_usec: 0 };
    // SAFETY: valid pointer to a timeval; NULL timezone as in C.
    unsafe { libc::gettimeofday(&mut tp, core::ptr::null_mut()) };

    let mut result = tp.tv_sec as i64
        - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY as i64);
    result = result * USECS_PER_SEC + tp.tv_usec as i64;
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
fn dt2local(dt: Timestamp, timezone: i32) -> Timestamp {
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

pub fn init_seams() {
    timestamp_seams::get_current_timestamp::set(GetCurrentTimestamp);
}
