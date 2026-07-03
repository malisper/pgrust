//! timestamp.c interval half: interval I/O cores, typmod, comparisons,
//! arithmetic, justify family, and datetime +/- interval. interval_in error
//! surface matches C (DTERR mapping incl. FIELD->INTERVAL overflow rewrite,
//! 22015 on itmin2interval/AdjustIntervalForTypmod failures).

use adt_datetime::tz::{self, PgTz};
use adt_datetime::{
    date2j, fsec_t, isleap, j2date, pg_itm, pg_itm_in, pg_tm, DateTimeErrorExtra,
    DecodeISO8601Interval, DecodeInterval, EncodeInterval, Interval, ParseDateTime, Timestamp,
    DAY, DAYS_PER_MONTH, DAY_TAB, DTERR_BAD_FORMAT, DTERR_FIELD_OVERFLOW,
    DTERR_INTERVAL_OVERFLOW, DTK_DELTA, DTK_EARLY, DTK_LATE, HOUR, INTERVAL_FULL_RANGE,
    INTERVAL_MASK, MAXDATEFIELDS, MAXDATELEN, MAX_INTERVAL_PRECISION, MINUTE, MONTH,
    MONTHS_PER_YEAR, SECOND, SECS_PER_DAY, USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE,
    USECS_PER_SEC, YEAR,
};
use types_core::TimestampTz;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
    ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_PARAMETER_VALUE,
};

use crate::{
    timestamp2tm, timestamp_out_of_range, tm2timestamp, TsBuf, DT_NOBEGIN, DT_NOEND, EARLY,
    IS_VALID_TIMESTAMP, LATE, MIN_TIMESTAMP, TIMESTAMP_IS_NOBEGIN, TIMESTAMP_IS_NOEND,
    TIMESTAMP_NOT_FINITE, TS_WORKBUF,
};

pub const INTERVAL_FULL_PRECISION: i32 = 0xFFFF;
const INTERVAL_PRECISION_MASK: i32 = 0xFFFF;

#[allow(non_snake_case)]
#[inline(always)]
pub const fn INTERVAL_TYPMOD(p: i32, r: i32) -> i32 {
    ((r & INTERVAL_FULL_RANGE) << 16) | (p & INTERVAL_PRECISION_MASK)
}

#[allow(non_snake_case)]
#[inline(always)]
pub const fn INTERVAL_PRECISION(t: i32) -> i32 {
    t & INTERVAL_PRECISION_MASK
}

#[allow(non_snake_case)]
#[inline(always)]
pub const fn INTERVAL_RANGE(t: i32) -> i32 {
    (t >> 16) & INTERVAL_FULL_RANGE
}

#[cold]
#[inline(never)]
pub(crate) fn interval_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("interval out of range").with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

pub fn interval2itm(span: Interval, itm: &mut pg_itm) {
    itm.tm_year = span.month / MONTHS_PER_YEAR;
    itm.tm_mon = span.month % MONTHS_PER_YEAR;
    itm.tm_mday = span.day;
    let mut time = span.time;

    let mut tfrac = time / USECS_PER_HOUR;
    time -= tfrac * USECS_PER_HOUR;
    itm.tm_hour = tfrac;
    tfrac = time / USECS_PER_MINUTE;
    time -= tfrac * USECS_PER_MINUTE;
    itm.tm_min = tfrac as i32;
    tfrac = time / USECS_PER_SEC;
    time -= tfrac * USECS_PER_SEC;
    itm.tm_sec = tfrac as i32;
    itm.tm_usec = time as i32;
}

#[allow(clippy::result_unit_err)]
pub fn itm2interval(itm: &pg_itm, span: &mut Interval) -> Result<(), ()> {
    let total_months = itm.tm_year as i64 * MONTHS_PER_YEAR as i64 + itm.tm_mon as i64;
    if total_months > i32::MAX as i64 || total_months < i32::MIN as i64 {
        return Err(());
    }
    span.month = total_months as i32;
    span.day = itm.tm_mday;
    // tm_min/tm_sec are 32 bits: their products can't overflow i64
    span.time = itm
        .tm_hour
        .checked_mul(USECS_PER_HOUR)
        .and_then(|t| t.checked_add(itm.tm_min as i64 * USECS_PER_MINUTE))
        .and_then(|t| t.checked_add(itm.tm_sec as i64 * USECS_PER_SEC))
        .and_then(|t| t.checked_add(itm.tm_usec as i64))
        .ok_or(())?;
    if span.not_finite() {
        return Err(());
    }
    Ok(())
}

/// Infinite results are NOT overflow here (pre-17 dump/reload hazard, per C).
#[allow(clippy::result_unit_err)]
pub fn itmin2interval(itm_in: &pg_itm_in, span: &mut Interval) -> Result<(), ()> {
    let total_months = itm_in.tm_year as i64 * MONTHS_PER_YEAR as i64 + itm_in.tm_mon as i64;
    if total_months > i32::MAX as i64 || total_months < i32::MIN as i64 {
        return Err(());
    }
    span.month = total_months as i32;
    span.day = itm_in.tm_mday;
    span.time = itm_in.tm_usec;
    Ok(())
}

static INTERVAL_SCALES: [i64; MAX_INTERVAL_PRECISION as usize + 1] =
    [1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];
static INTERVAL_OFFSETS: [i64; MAX_INTERVAL_PRECISION as usize + 1] =
    [500_000, 50_000, 5_000, 500, 50, 5, 0];

#[allow(non_snake_case)]
pub fn AdjustIntervalForTypmod(
    interval: &mut Interval,
    typmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<()> {
    if interval.not_finite() {
        return Ok(());
    }
    if typmod < 0 {
        return Ok(());
    }

    let range = INTERVAL_RANGE(typmod);
    let precision = INTERVAL_PRECISION(typmod);

    // Fields right of the last one specified are zeroed; those left of it
    // remain valid (post-8.4 truncation semantics, per C).
    if range == INTERVAL_FULL_RANGE {
        // do nothing
    } else if range == INTERVAL_MASK(YEAR) {
        interval.month = (interval.month / MONTHS_PER_YEAR) * MONTHS_PER_YEAR;
        interval.day = 0;
        interval.time = 0;
    } else if range == INTERVAL_MASK(MONTH) || range == INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH)
    {
        interval.day = 0;
        interval.time = 0;
    } else if range == INTERVAL_MASK(DAY) {
        interval.time = 0;
    } else if range == INTERVAL_MASK(HOUR) || range == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) {
        interval.time = (interval.time / USECS_PER_HOUR) * USECS_PER_HOUR;
    } else if range == INTERVAL_MASK(MINUTE)
        || range == INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)
        || range == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)
    {
        interval.time = (interval.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
    } else if range == INTERVAL_MASK(SECOND)
        || range == INTERVAL_MASK(DAY)
            | INTERVAL_MASK(HOUR)
            | INTERVAL_MASK(MINUTE)
            | INTERVAL_MASK(SECOND)
        || range == INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)
        || range == INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)
    {
        // fractional-second rounding is dealt with below
    } else {
        return Err(Box::new(PgError::error(format!(
            "unrecognized interval typmod: {typmod}"
        ))));
    }

    if precision != INTERVAL_FULL_PRECISION {
        if !(0..=MAX_INTERVAL_PRECISION).contains(&precision) {
            return ereturn(
                escontext.as_deref_mut(),
                (),
                PgError::error(format!(
                    "interval({precision}) precision must be between 0 and {MAX_INTERVAL_PRECISION}"
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
            );
        }
        let p = precision as usize;
        let adjusted = if interval.time >= 0 {
            interval.time.checked_add(INTERVAL_OFFSETS[p])
        } else {
            interval.time.checked_sub(INTERVAL_OFFSETS[p])
        };
        let Some(t) = adjusted else {
            return ereturn(escontext, (), *interval_out_of_range());
        };
        interval.time = t - t % INTERVAL_SCALES[p];
    }
    Ok(())
}

#[allow(non_snake_case)]
pub fn EncodeSpecialInterval(itv: &Interval, buf: &mut [u8]) -> usize {
    let s: &[u8] = if itv.is_nobegin() {
        EARLY
    } else if itv.is_noend() {
        LATE
    } else {
        panic!("invalid argument for EncodeSpecialInterval");
    };
    buf[..s.len()].copy_from_slice(s);
    s.len()
}

/// On soft error the sentinel is `Ok(zero interval)` with escontext set.
pub fn interval_in(
    s: &str,
    typmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Interval> {
    let mut itm_in = pg_itm_in::default();
    let mut dtype = 0i32;
    let mut workbuf = [0u8; 256];
    let mut field: [&[u8]; MAXDATEFIELDS] = [b""; MAXDATEFIELDS];
    let mut ftype = [0i32; MAXDATEFIELDS];
    let mut nf = 0usize;

    let range = if typmod >= 0 { INTERVAL_RANGE(typmod) } else { INTERVAL_FULL_RANGE };

    let mut dterr = ParseDateTime(
        s.as_bytes(),
        &mut workbuf,
        &mut field,
        &mut ftype,
        MAXDATEFIELDS,
        &mut nf,
    );
    if dterr == 0 {
        dterr = DecodeInterval(&field[..nf], &ftype[..nf], nf, range, &mut dtype, &mut itm_in);
    }

    // if those functions think it's a bad format, try ISO8601 style
    if dterr == DTERR_BAD_FORMAT {
        dterr = DecodeISO8601Interval(s.as_bytes(), &mut dtype, &mut itm_in);
    }

    if dterr != 0 {
        let dterr = if dterr == DTERR_FIELD_OVERFLOW { DTERR_INTERVAL_OVERFLOW } else { dterr };
        let extra = DateTimeErrorExtra::default();
        adt_datetime::DateTimeParseError(dterr, Some(&extra), s, "interval", escontext)?;
        return Ok(Interval::default());
    }

    let mut result = Interval::default();
    match dtype {
        d if d == DTK_DELTA => {
            if itmin2interval(&itm_in, &mut result).is_err() {
                return ereturn(
                    escontext.as_deref_mut(),
                    Interval::default(),
                    *interval_out_of_range(),
                );
            }
        }
        d if d == DTK_LATE => result = Interval::NOEND,
        d if d == DTK_EARLY => result = Interval::NOBEGIN,
        other => {
            return Err(Box::new(PgError::error(format!(
                "unexpected dtype {other} while parsing interval \"{s}\""
            ))));
        }
    }

    AdjustIntervalForTypmod(&mut result, typmod, escontext)?;
    Ok(result)
}

pub fn interval_out(span: &Interval, buf: &mut TsBuf) -> usize {
    if span.not_finite() {
        return EncodeSpecialInterval(span, buf);
    }
    let mut itm = pg_itm::default();
    interval2itm(*span, &mut itm);
    EncodeInterval(&itm, adt_datetime::interval_style(), buf)
}

pub fn interval_cmp_value(interval: &Interval) -> i128 {
    let days = interval.month as i64 * 30 + interval.day as i64;
    interval.time as i128 + days as i128 * USECS_PER_DAY as i128
}

pub fn interval_cmp_internal(interval1: &Interval, interval2: &Interval) -> i32 {
    let span1 = interval_cmp_value(interval1);
    let span2 = interval_cmp_value(interval2);
    match span1.cmp(&span2) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

pub fn interval_sign(interval: &Interval) -> i32 {
    let span = interval_cmp_value(interval);
    match span.cmp(&0) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

pub fn interval_um_internal(interval: &Interval, result: &mut Interval) -> PgResult<()> {
    if interval.is_nobegin() {
        *result = Interval::NOEND;
    } else if interval.is_noend() {
        *result = Interval::NOBEGIN;
    } else {
        let (Some(time), Some(day), Some(month)) = (
            0i64.checked_sub(interval.time),
            0i32.checked_sub(interval.day),
            0i32.checked_sub(interval.month),
        ) else {
            return Err(interval_out_of_range());
        };
        *result = Interval { time, day, month };
        if result.not_finite() {
            return Err(interval_out_of_range());
        }
    }
    Ok(())
}

pub fn interval_um(interval: &Interval) -> PgResult<Interval> {
    let mut result = Interval::default();
    interval_um_internal(interval, &mut result)?;
    Ok(result)
}

pub fn interval_smaller(i1: Interval, i2: Interval) -> Interval {
    if interval_cmp_internal(&i1, &i2) < 0 {
        i1
    } else {
        i2
    }
}

pub fn interval_larger(i1: Interval, i2: Interval) -> Interval {
    if interval_cmp_internal(&i1, &i2) > 0 {
        i1
    } else {
        i2
    }
}

fn finite_interval_pl(span1: &Interval, span2: &Interval) -> PgResult<Interval> {
    let (Some(month), Some(day), Some(time)) = (
        span1.month.checked_add(span2.month),
        span1.day.checked_add(span2.day),
        span1.time.checked_add(span2.time),
    ) else {
        return Err(interval_out_of_range());
    };
    let result = Interval { time, day, month };
    if result.not_finite() {
        return Err(interval_out_of_range());
    }
    Ok(result)
}

fn finite_interval_mi(span1: &Interval, span2: &Interval) -> PgResult<Interval> {
    let (Some(month), Some(day), Some(time)) = (
        span1.month.checked_sub(span2.month),
        span1.day.checked_sub(span2.day),
        span1.time.checked_sub(span2.time),
    ) else {
        return Err(interval_out_of_range());
    };
    let result = Interval { time, day, month };
    if result.not_finite() {
        return Err(interval_out_of_range());
    }
    Ok(result)
}

// "infinity - infinity" style combinations error: interval has no NaN.
pub fn interval_pl(span1: &Interval, span2: &Interval) -> PgResult<Interval> {
    if span1.is_nobegin() {
        if span2.is_noend() {
            Err(interval_out_of_range())
        } else {
            Ok(Interval::NOBEGIN)
        }
    } else if span1.is_noend() {
        if span2.is_nobegin() {
            Err(interval_out_of_range())
        } else {
            Ok(Interval::NOEND)
        }
    } else if span2.not_finite() {
        Ok(*span2)
    } else {
        finite_interval_pl(span1, span2)
    }
}

pub fn interval_mi(span1: &Interval, span2: &Interval) -> PgResult<Interval> {
    if span1.is_nobegin() {
        if span2.is_nobegin() {
            Err(interval_out_of_range())
        } else {
            Ok(Interval::NOBEGIN)
        }
    } else if span1.is_noend() {
        if span2.is_noend() {
            Err(interval_out_of_range())
        } else {
            Ok(Interval::NOEND)
        }
    } else if span2.is_nobegin() {
        Ok(Interval::NOEND)
    } else if span2.is_noend() {
        Ok(Interval::NOBEGIN)
    } else {
        finite_interval_mi(span1, span2)
    }
}

#[inline]
fn ts_round(j: f64) -> f64 {
    (j * 1_000_000.0).round_ties_even() / 1_000_000.0
}

#[inline]
fn float8_fits_in_int32(num: f64) -> bool {
    // exclusive upper bound per C FLOAT8_FITS_IN_INT32
    num >= -2147483648.0 && num < 2147483648.0
}

#[inline]
fn float8_fits_in_int64(num: f64) -> bool {
    num >= -9223372036854775808.0 && num < 9223372036854775808.0
}

pub fn interval_mul(span: &Interval, factor: f64) -> PgResult<Interval> {
    // 0 * infinity and infinity * 0 error: interval has no NaN
    if factor.is_nan() {
        return Err(interval_out_of_range());
    }
    if span.not_finite() {
        if factor == 0.0 {
            return Err(interval_out_of_range());
        }
        if factor < 0.0 {
            return interval_um(span);
        }
        return Ok(*span);
    }
    if factor.is_infinite() {
        let isign = interval_sign(span);
        if isign == 0 {
            return Err(interval_out_of_range());
        }
        return Ok(if factor * (isign as f64) < 0.0 { Interval::NOBEGIN } else { Interval::NOEND });
    }

    let orig_month = span.month;
    let orig_day = span.day;
    let mut result = Interval::default();

    let mut result_double = span.month as f64 * factor;
    if result_double.is_nan() || !float8_fits_in_int32(result_double) {
        return Err(interval_out_of_range());
    }
    result.month = result_double as i32;

    result_double = span.day as f64 * factor;
    if result_double.is_nan() || !float8_fits_in_int32(result_double) {
        return Err(interval_out_of_range());
    }
    result.day = result_double as i32;

    // cascade fractional month/day parts down (never up), per C
    let mut month_remainder_days =
        (orig_month as f64 * factor - result.month as f64) * DAYS_PER_MONTH as f64;
    month_remainder_days = ts_round(month_remainder_days);
    let mut sec_remainder = (orig_day as f64 * factor - result.day as f64 + month_remainder_days
        - (month_remainder_days as i32) as f64)
        * SECS_PER_DAY as f64;
    sec_remainder = ts_round(sec_remainder);

    // may exceed a day due to rounding or cascade
    if sec_remainder.abs() >= SECS_PER_DAY as f64 {
        let Some(day) = result.day.checked_add((sec_remainder / SECS_PER_DAY as f64) as i32)
        else {
            return Err(interval_out_of_range());
        };
        result.day = day;
        sec_remainder -= ((sec_remainder / SECS_PER_DAY as f64) as i32) as f64 * SECS_PER_DAY as f64;
    }

    let Some(day) = result.day.checked_add(month_remainder_days as i32) else {
        return Err(interval_out_of_range());
    };
    result.day = day;
    result_double =
        (span.time as f64 * factor + sec_remainder * USECS_PER_SEC as f64).round_ties_even();
    if result_double.is_nan() || !float8_fits_in_int64(result_double) {
        return Err(interval_out_of_range());
    }
    result.time = result_double as i64;

    if result.not_finite() {
        return Err(interval_out_of_range());
    }
    Ok(result)
}

pub fn interval_div(span: &Interval, factor: f64) -> PgResult<Interval> {
    if factor == 0.0 {
        return Err(Box::new(
            PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO),
        ));
    }
    // infinity / infinity errors; dividing by infinity zeroes all fields
    if factor.is_nan() {
        return Err(interval_out_of_range());
    }
    if span.not_finite() {
        if factor.is_infinite() {
            return Err(interval_out_of_range());
        }
        if factor < 0.0 {
            return interval_um(span);
        }
        return Ok(*span);
    }

    let orig_month = span.month;
    let orig_day = span.day;
    let mut result = Interval::default();

    let mut result_double = span.month as f64 / factor;
    if result_double.is_nan() || !float8_fits_in_int32(result_double) {
        return Err(interval_out_of_range());
    }
    result.month = result_double as i32;

    result_double = span.day as f64 / factor;
    if result_double.is_nan() || !float8_fits_in_int32(result_double) {
        return Err(interval_out_of_range());
    }
    result.day = result_double as i32;

    let mut month_remainder_days =
        (orig_month as f64 / factor - result.month as f64) * DAYS_PER_MONTH as f64;
    month_remainder_days = ts_round(month_remainder_days);
    let mut sec_remainder = (orig_day as f64 / factor - result.day as f64 + month_remainder_days
        - (month_remainder_days as i32) as f64)
        * SECS_PER_DAY as f64;
    sec_remainder = ts_round(sec_remainder);
    if sec_remainder.abs() >= SECS_PER_DAY as f64 {
        let Some(day) = result.day.checked_add((sec_remainder / SECS_PER_DAY as f64) as i32)
        else {
            return Err(interval_out_of_range());
        };
        result.day = day;
        sec_remainder -= ((sec_remainder / SECS_PER_DAY as f64) as i32) as f64 * SECS_PER_DAY as f64;
    }

    let Some(day) = result.day.checked_add(month_remainder_days as i32) else {
        return Err(interval_out_of_range());
    };
    result.day = day;
    result_double =
        (span.time as f64 / factor + sec_remainder * USECS_PER_SEC as f64).round_ties_even();
    if result_double.is_nan() || !float8_fits_in_int64(result_double) {
        return Err(interval_out_of_range());
    }
    result.time = result_double as i64;

    if result.not_finite() {
        return Err(interval_out_of_range());
    }
    Ok(result)
}

/// 0 <= abs(time) < 24h, 0 <= abs(day) < 30, all three signs equal.
pub fn interval_justify_interval(span: &Interval) -> PgResult<Interval> {
    let mut result = *span;
    if result.not_finite() {
        return Ok(result);
    }

    // pre-justify days if it might prevent overflow
    if (result.day > 0 && result.time > 0) || (result.day < 0 && result.time < 0) {
        let wholemonth = result.day / DAYS_PER_MONTH;
        result.day -= wholemonth * DAYS_PER_MONTH;
        let Some(m) = result.month.checked_add(wholemonth) else {
            return Err(interval_out_of_range());
        };
        result.month = m;
    }

    // TMODULO; abs(wholeday) can't exceed ~1.07e8, so day addition is safe
    let wholeday = result.time / USECS_PER_DAY;
    if wholeday != 0 {
        result.time -= wholeday * USECS_PER_DAY;
    }
    result.day += wholeday as i32;

    let wholemonth = result.day / DAYS_PER_MONTH;
    result.day -= wholemonth * DAYS_PER_MONTH;
    let Some(m) = result.month.checked_add(wholemonth) else {
        return Err(interval_out_of_range());
    };
    result.month = m;

    if result.month > 0 && (result.day < 0 || (result.day == 0 && result.time < 0)) {
        result.day += DAYS_PER_MONTH;
        result.month -= 1;
    } else if result.month < 0 && (result.day > 0 || (result.day == 0 && result.time > 0)) {
        result.day -= DAYS_PER_MONTH;
        result.month += 1;
    }

    if result.day > 0 && result.time < 0 {
        result.time += USECS_PER_DAY;
        result.day -= 1;
    } else if result.day < 0 && result.time > 0 {
        result.time -= USECS_PER_DAY;
        result.day += 1;
    }

    Ok(result)
}

pub fn interval_justify_hours(span: &Interval) -> PgResult<Interval> {
    let mut result = *span;
    if result.not_finite() {
        return Ok(result);
    }

    let wholeday = result.time / USECS_PER_DAY;
    if wholeday != 0 {
        result.time -= wholeday * USECS_PER_DAY;
    }
    let Some(day) = result.day.checked_add(wholeday as i32) else {
        return Err(interval_out_of_range());
    };
    result.day = day;

    if result.day > 0 && result.time < 0 {
        result.time += USECS_PER_DAY;
        result.day -= 1;
    } else if result.day < 0 && result.time > 0 {
        result.time -= USECS_PER_DAY;
        result.day += 1;
    }
    Ok(result)
}

pub fn interval_justify_days(span: &Interval) -> PgResult<Interval> {
    let mut result = *span;
    if result.not_finite() {
        return Ok(result);
    }

    let wholemonth = result.day / DAYS_PER_MONTH;
    result.day -= wholemonth * DAYS_PER_MONTH;
    let Some(m) = result.month.checked_add(wholemonth) else {
        return Err(interval_out_of_range());
    };
    result.month = m;

    if result.month > 0 && result.day < 0 {
        result.day += DAYS_PER_MONTH;
        result.month -= 1;
    } else if result.month < 0 && result.day > 0 {
        result.day -= DAYS_PER_MONTH;
        result.month += 1;
    }
    Ok(result)
}

/// timestamp - timestamp -> interval ("infinity - infinity" errors).
pub fn timestamp_mi(dt1: Timestamp, dt2: Timestamp) -> PgResult<Interval> {
    if TIMESTAMP_NOT_FINITE(dt1) || TIMESTAMP_NOT_FINITE(dt2) {
        let result = if TIMESTAMP_IS_NOBEGIN(dt1) {
            if TIMESTAMP_IS_NOBEGIN(dt2) {
                return Err(interval_out_of_range());
            }
            Interval::NOBEGIN
        } else if TIMESTAMP_IS_NOEND(dt1) {
            if TIMESTAMP_IS_NOEND(dt2) {
                return Err(interval_out_of_range());
            }
            Interval::NOEND
        } else if TIMESTAMP_IS_NOBEGIN(dt2) {
            Interval::NOEND
        } else {
            Interval::NOBEGIN
        };
        return Ok(result);
    }

    let Some(time) = dt1.checked_sub(dt2) else {
        return Err(interval_out_of_range());
    };
    let result = Interval { time, day: 0, month: 0 };
    // wrong, but removing it breaks a lot of regression tests (per C)
    interval_justify_hours(&result)
}

fn month_day_carry(
    tm: &mut pg_tm,
    span_month: i32,
) -> PgResult<()> {
    let Some(mon) = tm.tm_mon.checked_add(span_month) else {
        return Err(timestamp_out_of_range());
    };
    tm.tm_mon = mon;
    if tm.tm_mon > MONTHS_PER_YEAR {
        tm.tm_year += (tm.tm_mon - 1) / MONTHS_PER_YEAR;
        tm.tm_mon = ((tm.tm_mon - 1) % MONTHS_PER_YEAR) + 1;
    } else if tm.tm_mon < 1 {
        tm.tm_year += tm.tm_mon / MONTHS_PER_YEAR - 1;
        tm.tm_mon = tm.tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
    }
    // adjust for end-of-month boundary problems
    if tm.tm_mday > DAY_TAB[usize::from(isleap(tm.tm_year))][(tm.tm_mon - 1) as usize] {
        tm.tm_mday = DAY_TAB[usize::from(isleap(tm.tm_year))][(tm.tm_mon - 1) as usize];
    }
    Ok(())
}

pub fn timestamp_pl_interval(timestamp: Timestamp, span: &Interval) -> PgResult<Timestamp> {
    let mut timestamp = timestamp;
    if span.is_nobegin() {
        if TIMESTAMP_IS_NOEND(timestamp) {
            return Err(timestamp_out_of_range());
        }
        return Ok(DT_NOBEGIN);
    }
    if span.is_noend() {
        if TIMESTAMP_IS_NOBEGIN(timestamp) {
            return Err(timestamp_out_of_range());
        }
        return Ok(DT_NOEND);
    }
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }

    if span.month != 0 {
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err() {
            return Err(timestamp_out_of_range());
        }
        month_day_carry(&mut tm, span.month)?;
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
        // add days via Julian; j2date needs a non-negative input
        let julian = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday);
        let Some(julian) = julian.checked_add(span.day).filter(|&j| j >= 0) else {
            return Err(timestamp_out_of_range());
        };
        j2date(julian, &mut tm.tm_year, &mut tm.tm_mon, &mut tm.tm_mday);
        if tm2timestamp(&tm, fsec, None, &mut timestamp).is_err() {
            return Err(timestamp_out_of_range());
        }
    }

    let Some(t) = timestamp.checked_add(span.time) else {
        return Err(timestamp_out_of_range());
    };
    timestamp = t;

    if !IS_VALID_TIMESTAMP(timestamp) {
        return Err(timestamp_out_of_range());
    }
    Ok(timestamp)
}

pub fn timestamp_mi_interval(timestamp: Timestamp, span: &Interval) -> PgResult<Timestamp> {
    let mut tspan = Interval::default();
    interval_um_internal(span, &mut tspan)?;
    timestamp_pl_interval(timestamp, &tspan)
}

pub fn timestamptz_pl_interval_internal(
    timestamp: TimestampTz,
    span: &Interval,
    attimezone: Option<&'static PgTz>,
) -> PgResult<TimestampTz> {
    let mut timestamp = timestamp;
    if span.is_nobegin() {
        if TIMESTAMP_IS_NOEND(timestamp) {
            return Err(timestamp_out_of_range());
        }
        return Ok(DT_NOBEGIN);
    }
    if span.is_noend() {
        if TIMESTAMP_IS_NOBEGIN(timestamp) {
            return Err(timestamp_out_of_range());
        }
        return Ok(DT_NOEND);
    }
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return Ok(timestamp);
    }

    // C resolves NULL attimezone to session_timezone
    let attimezone = match attimezone {
        Some(z) => z,
        None => tz::session_timezone().unwrap_or_else(|| {
            panic!("session timezone not initialized (pg_timezone_initialize) — timestamptz_pl_interval")
        }),
    };

    if span.month != 0 {
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        let mut tzv = 0i32;
        if timestamp2tm(timestamp, Some(&mut tzv), &mut tm, &mut fsec, None, Some(attimezone))
            .is_err()
        {
            return Err(timestamp_out_of_range());
        }
        month_day_carry(&mut tm, span.month)?;
        let tzv = tz::DetermineTimeZoneOffset(&mut tm, attimezone);
        if tm2timestamp(&tm, fsec, Some(tzv), &mut timestamp).is_err() {
            return Err(timestamp_out_of_range());
        }
    }

    if span.day != 0 {
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        let mut tzv = 0i32;
        if timestamp2tm(timestamp, Some(&mut tzv), &mut tm, &mut fsec, None, Some(attimezone))
            .is_err()
        {
            return Err(timestamp_out_of_range());
        }
        // julian >= -1 allowed to dodge timezone-dependent failures, per C
        let julian = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday);
        let Some(julian) = julian.checked_add(span.day).filter(|&j| j >= -1) else {
            return Err(timestamp_out_of_range());
        };
        j2date(julian, &mut tm.tm_year, &mut tm.tm_mon, &mut tm.tm_mday);
        let tzv = tz::DetermineTimeZoneOffset(&mut tm, attimezone);
        if tm2timestamp(&tm, fsec, Some(tzv), &mut timestamp).is_err() {
            return Err(timestamp_out_of_range());
        }
    }

    let Some(t) = timestamp.checked_add(span.time) else {
        return Err(timestamp_out_of_range());
    };
    timestamp = t;

    if !IS_VALID_TIMESTAMP(timestamp) {
        return Err(timestamp_out_of_range());
    }
    Ok(timestamp)
}

pub fn timestamptz_mi_interval_internal(
    timestamp: TimestampTz,
    span: &Interval,
    attimezone: Option<&'static PgTz>,
) -> PgResult<TimestampTz> {
    let mut tspan = Interval::default();
    interval_um_internal(span, &mut tspan)?;
    timestamptz_pl_interval_internal(timestamp, &tspan, attimezone)
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
    // we don't expect this to fail, but check it pro forma
    if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_ok() {
        let attimezone = tz::session_timezone().unwrap_or_else(|| {
            panic!("session timezone not initialized (pg_timezone_initialize) — timestamp2timestamptz")
        });
        let tzv = tz::DetermineTimeZoneOffset(&mut tm, attimezone);
        let result = timestamp.wrapping_sub(-(tzv as i64) * USECS_PER_SEC);
        if IS_VALID_TIMESTAMP(result) {
            return Ok(result);
        }
        if let Some(o) = overflow {
            if result < MIN_TIMESTAMP {
                *o = -1;
                return Ok(DT_NOBEGIN);
            } else {
                *o = 1;
                return Ok(DT_NOEND);
            }
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
    let mut tzv = 0i32;
    if timestamp2tm(timestamp, Some(&mut tzv), &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }
    let mut result = 0;
    if tm2timestamp(&tm, fsec, None, &mut result).is_err() {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

pub fn timestamp_cmp_timestamptz_internal(
    timestamp_val: Timestamp,
    dt2: TimestampTz,
) -> PgResult<i32> {
    let mut overflow = 0i32;
    let dt1 = timestamp2timestamptz_opt_overflow(timestamp_val, Some(&mut overflow))?;
    if overflow > 0 {
        // dt1 is larger than any finite timestamp, but less than infinity
        return Ok(if TIMESTAMP_IS_NOEND(dt2) { -1 } else { 1 });
    }
    if overflow < 0 {
        return Ok(if TIMESTAMP_IS_NOBEGIN(dt2) { 1 } else { -1 });
    }
    Ok(timestamptz_cmp_internal(dt1, dt2))
}

#[inline]
fn timestamptz_cmp_internal(dt1: TimestampTz, dt2: TimestampTz) -> i32 {
    match dt1.cmp(&dt2) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

/// interval_scale: copy + AdjustIntervalForTypmod with hard error surface.
pub fn interval_scale(interval: &Interval, typmod: i32) -> PgResult<Interval> {
    let mut result = *interval;
    AdjustIntervalForTypmod(&mut result, typmod, None)?;
    Ok(result)
}

pub fn interval_recv(
    buf: &mut ::stringinfo::StringInfo<'_>,
    typmod: i32,
) -> PgResult<Interval> {
    let time = ::pqformat::pq_getmsgint64(buf)?;
    let day = ::pqformat::pq_getmsgint(buf, 4)? as i32;
    let month = ::pqformat::pq_getmsgint(buf, 4)? as i32;
    let mut interval = Interval { time, day, month };
    AdjustIntervalForTypmod(&mut interval, typmod, None)?;
    Ok(interval)
}

pub fn interval_send<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    interval: &Interval,
) -> PgResult<::datum::Bytea<'mcx>> {
    let mut b = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint64(&mut b, interval.time as u64)?;
    ::pqformat::pq_sendint32(&mut b, interval.day as u32)?;
    ::pqformat::pq_sendint32(&mut b, interval.month as u32)?;
    Ok(::pqformat::pq_endtypsend(b))
}

const _: () = {
    // decode_timestamp_str's workbuf is dimensioned for datetime parsing; the
    // C interval_in workbuf is 256, asserted where it is declared.
    assert!(TS_WORKBUF <= 256);
    assert!(MAXDATELEN + 1 >= 64);
};
