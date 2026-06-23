//! The TIME (without time zone) value type — the seam-free arithmetic cores,
//! ported from `src/backend/utils/adt/date.c`.
//!
//! This round ports the pure-arithmetic, decode/encode-free half of date.c's
//! TIME ADT: the broken-down <-> `TimeADT` conversions (`tm2time`/`time2tm`),
//! the range checks (`time_overflows`/`float_time_overflows`), the typmod
//! rounder (`AdjustTimeForTypmod`), `make_time`, the comparison primitives, and
//! the interval-arithmetic cores (`time_interval`/`interval_time`/
//! `time_mi_time`/`time_pl_interval`/`time_mi_interval`).  It also re-homes the
//! tiny `INTERVAL_NOT_FINITE` macro (timestamp.h) that interval-aware TIME ops
//! need, plus the `time_overflows` helper that the date/time decode engine
//! depends on (`decode.rs` does `use crate::time::time_overflows`).
//!
//! The text-driven entry points (`time_in`/`time_out`, EXTRACT) are ported here
//! and lean on the decode/encode parsing engine (`decode.rs`).  The fmgr
//! `Datum`/`PG_FUNCTION_ARGS` shims follow the project-wide systemic deferral.
//!
//! Idiomatic surface: plain `i32`/`i64`/`f64`, owned values, `Option`,
//! `Result`, `&str`.  No raw pointers, `extern "C"`, `c_int`, `libc`, or
//! `CStr`/`CString`.  Fallible cores return [`::types_error::PgResult`].


use ::mcx::Mcx;
use ::types_numeric::var::NumericVar;
use types_datetime::{
    Interval, HOURS_PER_DAY, MAX_TIME_PRECISION, MINS_PER_HOUR, SECS_PER_MINUTE, USECS_PER_DAY,
    USECS_PER_SEC,
};
use types_error::{
    ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE,
};
use types_datetime::{fsec_t, TimeADT};
use types_error::{ereturn, PgError, PgResult, SoftErrorContext};

use ::pgtime::pg_tm;

use crate::decode::{DecodeTimeOnly, DecodeUnits, ParseDateTime};
use crate::encode::EncodeTimeOnly;
use crate::numeric_helpers::int64_div_fast_to_numericvar;
use crate::settings::date_style;

const MAXDATEFIELDS: usize = ::types_datetime::MAXDATEFIELDS as usize;

// ---------------------------------------------------------------------------
// tm2time / time2tm
// ---------------------------------------------------------------------------

/// `tm2time()` -- convert a broken-down `pg_tm` (+ fsec) into a `TimeADT`.
/// (`utils/adt/date.c`)
pub fn tm2time(tm: &pg_tm, fsec: fsec_t) -> TimeADT {
    ((((tm.tm_hour as i64 * MINS_PER_HOUR as i64 + tm.tm_min as i64) * SECS_PER_MINUTE as i64)
        + tm.tm_sec as i64)
        * USECS_PER_SEC)
        + fsec as i64
}

/// `time2tm()` -- convert a `TimeADT` into hour/min/sec fields of `tm` + fsec.
///
/// Only the hour/min/sec fields of `tm` are touched.  (`utils/adt/date.c`)
pub fn time2tm(time: TimeADT, tm: &mut pg_tm, fsec: &mut fsec_t) {
    const USECS_PER_HOUR: i64 = 3_600_000_000;
    const USECS_PER_MINUTE: i64 = 60_000_000;

    let mut t = time;
    tm.tm_hour = (t / USECS_PER_HOUR) as i32;
    t -= tm.tm_hour as i64 * USECS_PER_HOUR;
    tm.tm_min = (t / USECS_PER_MINUTE) as i32;
    t -= tm.tm_min as i64 * USECS_PER_MINUTE;
    tm.tm_sec = (t / USECS_PER_SEC) as i32;
    t -= tm.tm_sec as i64 * USECS_PER_SEC;
    *fsec = t as fsec_t;
}

// ---------------------------------------------------------------------------
// time_overflows / float_time_overflows
// ---------------------------------------------------------------------------

/// `time_overflows()` -- range-check a broken-down time-of-day.
///
/// The date/time decode engine (`decode.rs`) depends on this core, so it is
/// ported here first.  (`utils/adt/date.c`)
pub fn time_overflows(hour: i32, min: i32, sec: i32, fsec: fsec_t) -> bool {
    // Range-check the fields individually.
    if !(0..=HOURS_PER_DAY).contains(&hour)
        || !(0..MINS_PER_HOUR).contains(&min)
        || !(0..=SECS_PER_MINUTE).contains(&sec)
        || fsec < 0
        || (fsec as i64) > USECS_PER_SEC
    {
        return true;
    }

    // Because we allow, eg, hour = 24 or sec = 60, we must check separately
    // that the total time value doesn't exceed 24:00:00.
    let total = ((((hour as i64 * MINS_PER_HOUR as i64 + min as i64) * SECS_PER_MINUTE as i64)
        + sec as i64)
        * USECS_PER_SEC)
        + fsec as i64;
    total > USECS_PER_DAY
}

/// `float_time_overflows()` -- like [`time_overflows`] but seconds are an `f64`.
///
/// Uses round-ties-even (`rint`) on `sec * USECS_PER_SEC`, matching the C code.
/// (`utils/adt/date.c`)
pub fn float_time_overflows(hour: i32, min: i32, sec: f64) -> bool {
    // Range-check the fields individually.
    if !(0..=HOURS_PER_DAY).contains(&hour) || !(0..MINS_PER_HOUR).contains(&min) {
        return true;
    }

    // "sec" requires extra care: cope with NaN, and round off before applying
    // the range check.
    if sec.is_nan() {
        return true;
    }
    let sec = rint(sec * USECS_PER_SEC as f64);
    if sec < 0.0 || sec > (SECS_PER_MINUTE as f64 * USECS_PER_SEC as f64) {
        return true;
    }

    // Total must not exceed 24:00:00.
    let total = (((hour as i64 * MINS_PER_HOUR as i64 + min as i64) * SECS_PER_MINUTE as i64)
        * USECS_PER_SEC)
        + sec as i64;
    total > USECS_PER_DAY
}

/// `rint()` -- round to nearest, ties to even.
#[inline]
fn rint(x: f64) -> f64 {
    x.round_ties_even()
}

// ---------------------------------------------------------------------------
// time_in / time_out cores
// ---------------------------------------------------------------------------

/// `anytime_typmod_check(istz, typmod)` (date.c:72) — validate + clamp a TIME /
/// TIMETZ typmod.  Negative is an error; over-max clamps to MAX_TIME_PRECISION.
pub fn anytime_typmod_check(istz: bool, typmod: i32) -> PgResult<i32> {
    if typmod < 0 {
        return Err(PgError::error(format!(
            "TIME({typmod}){} precision must not be negative",
            if istz { " WITH TIME ZONE" } else { "" }
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    if typmod > MAX_TIME_PRECISION {
        return Ok(MAX_TIME_PRECISION);
    }
    Ok(typmod)
}

/// `anytime_typmodin(istz, ta)` (date.c:54) — the shared `timetypmodin` /
/// `timetztypmodin` body over the already-`ArrayGetIntegerTypmods`-parsed
/// typmod list (the `cstring[]` deconstruction is the fmgr boundary's job).
pub fn anytime_typmodin(istz: bool, tl: &[i32]) -> PgResult<i32> {
    // C: if (n != 1) ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
    //        "invalid type modifier")  (date.c:61).
    if tl.len() != 1 {
        return Err(PgError::error("invalid type modifier")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    anytime_typmod_check(istz, tl[0])
}

/// `anytime_typmodout(istz, typmod)` (date.c) — render a `time`/`timetz` typmod
/// as its printable suffix (e.g. `"(2) without time zone"`).
pub fn anytime_typmodout(istz: bool, typmod: i32) -> String {
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

/// `time_in()` CORE -- parse a TIME text string at the given typmod.
pub fn time_in(str: &str, typmod: i32) -> PgResult<TimeADT> {
    time_in_safe(str, typmod, None)
}

/// `time_in()` CORE with a soft-error sink (see `date_in_safe`).
pub fn time_in_safe(
    str: &str,
    typmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<TimeADT> {
    let mut field: Vec<String> = Vec::new();
    let mut ftype: Vec<i32> = Vec::new();
    let mut nf = 0usize;

    let mut tt = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut dtype: i32 = 0;
    let mut tz: i32 = 0;
    let mut extra = ::types_datetime::DateTimeErrorExtra::default();

    // C time_in: workbuf[MAXDATELEN + 1] (date.c:1454).
    let mut dterr = ParseDateTime(
        str,
        ::types_datetime::MAXDATELEN as usize + 1,
        &mut field,
        &mut ftype,
        MAXDATEFIELDS,
        &mut nf,
    );
    if dterr == 0 {
        dterr = DecodeTimeOnly(
            &mut field,
            &mut ftype,
            nf,
            &mut dtype,
            &mut tt,
            &mut fsec,
            Some(&mut tz),
            &mut extra,
        );
    }
    if dterr != 0 {
        // C: DateTimeParseError(dterr, &extra, str, "time", escontext) maps each
        // dterr code to its own SQLSTATE and ereturns: with a soft sink the error
        // is saved and a discarded value returned Ok; without one it throws.
        return ereturn(
            escontext,
            TimeADT::default(),
            crate::date::datetime_parse_error_for(dterr, str, "time", &extra),
        );
    }

    let mut result = tm2time(&tt, fsec);
    AdjustTimeForTypmod(&mut result, typmod);
    Ok(result)
}

/// `time_out()` CORE -- render a `TimeADT` to a text string.
pub fn time_out(time: TimeADT) -> String {
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    time2tm(time, &mut tm, &mut fsec);
    let mut buf = String::new();
    EncodeTimeOnly(&tm, fsec, false, 0, date_style(), &mut buf);
    buf
}

// ---------------------------------------------------------------------------
// AdjustTimeForTypmod
// ---------------------------------------------------------------------------

const TIME_SCALES: [i64; (MAX_TIME_PRECISION + 1) as usize] =
    [1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];
const TIME_OFFSETS: [i64; (MAX_TIME_PRECISION + 1) as usize] =
    [500_000, 50_000, 5_000, 500, 50, 5, 0];

/// `AdjustTimeForTypmod()` -- force the precision of a time value to `typmod`.
/// (`utils/adt/date.c`)
pub fn AdjustTimeForTypmod(time: &mut TimeADT, typmod: i32) {
    if (0..=MAX_TIME_PRECISION).contains(&typmod) {
        let t = typmod as usize;
        if *time >= 0 {
            *time = ((*time + TIME_OFFSETS[t]) / TIME_SCALES[t]) * TIME_SCALES[t];
        } else {
            *time = -((((-*time) + TIME_OFFSETS[t]) / TIME_SCALES[t]) * TIME_SCALES[t]);
        }
    }
}

// ---------------------------------------------------------------------------
// make_time
// ---------------------------------------------------------------------------

/// `make_time()` CORE -- construct a TIME from hour/min/(float)seconds.
/// (`utils/adt/date.c`)
pub fn make_time(tm_hour: i32, tm_min: i32, sec: f64) -> PgResult<TimeADT> {
    if float_time_overflows(tm_hour, tm_min, sec) {
        // C: errmsg("time field value out of range: %d:%02d:%02g", ...).  The
        // seconds field uses %02g (= %g with the `0` flag and minimum field
        // width 2), which zero-pads short outputs (5.0 -> "05", 0.0 -> "00"),
        // not a plain fixed-point pad, so reuse the shared %g formatter.
        let sec_g = fmt_g(sec);
        let sec_field = if sec_g.len() < 2 {
            format!("{sec_g:0>2}")
        } else {
            sec_g
        };
        return Err(PgError::error(format!(
            "time field value out of range: {tm_hour}:{tm_min:02}:{sec_field}"
        ))
        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }

    // This should match tm2time.
    let time = (((tm_hour as i64 * MINS_PER_HOUR as i64 + tm_min as i64) * SECS_PER_MINUTE as i64)
        * USECS_PER_SEC)
        + rint(sec * USECS_PER_SEC as f64) as i64;
    Ok(time)
}

// ---------------------------------------------------------------------------
// Comparison cores
// ---------------------------------------------------------------------------

/// `time_cmp()` CORE.
#[inline]
pub fn time_cmp(t1: TimeADT, t2: TimeADT) -> i32 {
    if t1 < t2 {
        -1
    } else if t1 > t2 {
        1
    } else {
        0
    }
}

/// `time_eq()` CORE.
#[inline]
pub fn time_eq(t1: TimeADT, t2: TimeADT) -> bool {
    t1 == t2
}

/// `time_ne()` CORE.
#[inline]
pub fn time_ne(t1: TimeADT, t2: TimeADT) -> bool {
    t1 != t2
}

/// `time_lt()` CORE.
#[inline]
pub fn time_lt(t1: TimeADT, t2: TimeADT) -> bool {
    t1 < t2
}

/// `time_le()` CORE.
#[inline]
pub fn time_le(t1: TimeADT, t2: TimeADT) -> bool {
    t1 <= t2
}

/// `time_gt()` CORE.
#[inline]
pub fn time_gt(t1: TimeADT, t2: TimeADT) -> bool {
    t1 > t2
}

/// `time_ge()` CORE.
#[inline]
pub fn time_ge(t1: TimeADT, t2: TimeADT) -> bool {
    t1 >= t2
}

/// `time_larger()` CORE.
#[inline]
pub fn time_larger(t1: TimeADT, t2: TimeADT) -> TimeADT {
    if t1 > t2 {
        t1
    } else {
        t2
    }
}

/// `time_smaller()` CORE.
#[inline]
pub fn time_smaller(t1: TimeADT, t2: TimeADT) -> TimeADT {
    if t1 < t2 {
        t1
    } else {
        t2
    }
}

// ---------------------------------------------------------------------------
// Conversions / interval-arithmetic cores
// ---------------------------------------------------------------------------

/// `time_interval()` CORE -- convert a TIME to an [`Interval`] (time-only).
/// (`utils/adt/date.c`)
pub fn time_interval(time: TimeADT) -> Interval {
    Interval {
        time,
        day: 0,
        month: 0,
    }
}

/// `interval_time()` CORE -- the fractional-day portion of an interval as a TIME.
/// (`utils/adt/date.c`)
pub fn interval_time(span: &Interval) -> PgResult<TimeADT> {
    if INTERVAL_NOT_FINITE(span) {
        return Err(PgError::error("cannot convert infinite interval to time")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }
    let mut result = span.time % USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }
    Ok(result)
}

/// `time_mi_time()` CORE -- difference of two TIMEs as an [`Interval`].
/// (`utils/adt/date.c`)
pub fn time_mi_time(time1: TimeADT, time2: TimeADT) -> Interval {
    Interval {
        month: 0,
        day: 0,
        time: time1 - time2,
    }
}

/// `time_pl_interval()` CORE -- add an interval to a TIME (wrapping into a day).
/// (`utils/adt/date.c`)
pub fn time_pl_interval(time: TimeADT, span: &Interval) -> PgResult<TimeADT> {
    if INTERVAL_NOT_FINITE(span) {
        return Err(PgError::error("cannot add infinite interval to time")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }
    // C: `result = time + span->time` -- plain int64 add that relies on
    // 2s-complement wraparound; use wrapping ops to match and avoid a debug panic.
    let mut result = time.wrapping_add(span.time);
    result = result.wrapping_sub(result / USECS_PER_DAY * USECS_PER_DAY);
    if result < 0 {
        result = result.wrapping_add(USECS_PER_DAY);
    }
    Ok(result)
}

/// `time_mi_interval()` CORE -- subtract an interval from a TIME (wrapping).
/// (`utils/adt/date.c`)
pub fn time_mi_interval(time: TimeADT, span: &Interval) -> PgResult<TimeADT> {
    if INTERVAL_NOT_FINITE(span) {
        return Err(
            PgError::error("cannot subtract infinite interval from time")
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        );
    }
    // C: `result = time - span->time` -- plain int64 subtract that relies on
    // 2s-complement wraparound; use wrapping ops to match and avoid a debug panic.
    let mut result = time.wrapping_sub(span.time);
    result = result.wrapping_sub(result / USECS_PER_DAY * USECS_PER_DAY);
    if result < 0 {
        result = result.wrapping_add(USECS_PER_DAY);
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// extract_time() core
// ---------------------------------------------------------------------------

/// The result of [`time_part_common`]: a float8 value, an int64 (wrap with
/// `int64_to_numeric` for the numeric path), or a pre-built [`NumericVar`] for
/// the millisecond/second/epoch fractional cases.
#[derive(Clone, Debug)]
pub enum TimePartResult<'mcx> {
    /// `PG_RETURN_FLOAT8` value (the `retnumeric == false` path).
    Float(f64),
    /// An integer field value.
    Int(i64),
    /// A pre-built numeric (the `int64_div_fast_to_numeric` fractional path).
    Numeric(NumericVar<'mcx>),
}

/// `time_part_common()` CORE -- extract `lowunits` from a TIME.
///
/// `retnumeric` selects the EXTRACT (numeric) vs `date_part` (float8) result
/// shape, exactly as the C `time_part_common` does.  `lowunits` must already be
/// lowercased.
pub fn time_part_common<'mcx>(
    mcx: Mcx<'mcx>,
    lowunits: &str,
    time: TimeADT,
    retnumeric: bool,
) -> PgResult<TimePartResult<'mcx>> {
    use types_datetime::{
        DTK_EPOCH, DTK_HOUR, DTK_MICROSEC, DTK_MILLISEC, DTK_MINUTE, DTK_SECOND, RESERV, UNITS,
        UNKNOWN_FIELD,
    };

    let mut val: i32 = 0;
    let mut typ = DecodeUnits(0, lowunits, &mut val);
    if typ == UNKNOWN_FIELD {
        typ = crate::decode::DecodeSpecial(0, lowunits, &mut val);
    }

    if typ == UNITS {
        let mut fsec: fsec_t = 0;
        let mut tm = pg_tm::default();
        time2tm(time, &mut tm, &mut fsec);

        let intresult: i64 = match val {
            v if v == DTK_MICROSEC => tm.tm_sec as i64 * 1_000_000 + fsec as i64,
            v if v == DTK_MILLISEC => {
                return if retnumeric {
                    Ok(TimePartResult::Numeric(int64_div_fast_to_numericvar(
                        mcx,
                        tm.tm_sec as i64 * 1_000_000 + fsec as i64,
                        3,
                    )?))
                } else {
                    Ok(TimePartResult::Float(
                        tm.tm_sec as f64 * 1000.0 + fsec as f64 / 1000.0,
                    ))
                };
            }
            v if v == DTK_SECOND => {
                return if retnumeric {
                    Ok(TimePartResult::Numeric(int64_div_fast_to_numericvar(
                        mcx,
                        tm.tm_sec as i64 * 1_000_000 + fsec as i64,
                        6,
                    )?))
                } else {
                    Ok(TimePartResult::Float(
                        tm.tm_sec as f64 + fsec as f64 / 1_000_000.0,
                    ))
                };
            }
            v if v == DTK_MINUTE => tm.tm_min as i64,
            v if v == DTK_HOUR => tm.tm_hour as i64,
            _ => return Err(time_unit_not_supported(lowunits)),
        };
        finalize_int(intresult, retnumeric)
    } else if typ == RESERV && val == DTK_EPOCH {
        if retnumeric {
            Ok(TimePartResult::Numeric(int64_div_fast_to_numericvar(
                mcx, time, 6,
            )?))
        } else {
            Ok(TimePartResult::Float(time as f64 / 1_000_000.0))
        }
    } else {
        Err(time_unit_not_recognized(lowunits))
    }
}

#[inline]
fn finalize_int<'mcx>(intresult: i64, retnumeric: bool) -> PgResult<TimePartResult<'mcx>> {
    if retnumeric {
        Ok(TimePartResult::Int(intresult))
    } else {
        Ok(TimePartResult::Float(intresult as f64))
    }
}

#[inline]
fn time_unit_not_supported(lowunits: &str) -> PgError {
    PgError::error(format!(
        "unit \"{lowunits}\" not supported for type time without time zone"
    ))
    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

#[inline]
fn time_unit_not_recognized(lowunits: &str) -> PgError {
    PgError::error(format!(
        "unit \"{lowunits}\" not recognized for type time without time zone"
    ))
    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// `INTERVAL_NOT_FINITE(i)` (`datatype/timestamp.h`).  An infinite interval is
/// encoded by setting all three fields to the integer min (`-infinity`) or max
/// (`+infinity`).
#[inline]
pub fn INTERVAL_NOT_FINITE(i: &Interval) -> bool {
    (i.month == i32::MIN && i.day == i32::MIN && i.time == i64::MIN)
        || (i.month == i32::MAX && i.day == i32::MAX && i.time == i64::MAX)
}

/// Render an `f64` the way C's `printf("%g", v)` does (default precision 6).
///
/// C `%g` chooses `%e` style when the decimal exponent is `< -4` or `>=
/// precision`, otherwise `%f` style, then strips trailing zeros and a trailing
/// decimal point.  `make_time`'s out-of-range error message embeds the seconds
/// field with `%02g`, so this faithful renderer is needed for byte-identical
/// error text.  (A local copy of timestamp.c's `fmt_g`; will be de-duplicated
/// against the canonical home once `timestamp.rs` is ported.)
fn fmt_g(v: f64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    const USECS_PER_HOUR: i64 = 3_600_000_000;

    #[test]
    fn tm2time_time2tm_round_trip() {
        let mut tm = pg_tm {
            tm_hour: 12,
            tm_min: 34,
            tm_sec: 56,
            ..pg_tm::default()
        };
        let fsec: fsec_t = 789_000;
        let t = tm2time(&tm, fsec);
        let mut back_fsec: fsec_t = 0;
        tm = pg_tm::default();
        time2tm(t, &mut tm, &mut back_fsec);
        assert_eq!((tm.tm_hour, tm.tm_min, tm.tm_sec), (12, 34, 56));
        assert_eq!(back_fsec, 789_000);
    }

    #[test]
    fn time_in_known_value() {
        // tm2time of "01:00:00" == one hour of microseconds.
        let tm = pg_tm {
            tm_hour: 1,
            ..pg_tm::default()
        };
        assert_eq!(tm2time(&tm, 0), USECS_PER_HOUR);
    }

    #[test]
    fn time_overflows_boundaries() {
        // 24:00:00.000000 exactly is the max allowed (== USECS_PER_DAY).
        assert!(!time_overflows(24, 0, 0, 0));
        // 24:00:00.000001 overflows.
        assert!(time_overflows(24, 0, 0, 1));
        // Field-level overflows.
        assert!(time_overflows(25, 0, 0, 0));
        assert!(time_overflows(0, 60, 0, 0));
        assert!(time_overflows(0, 0, 61, 0));
        assert!(time_overflows(0, 0, 0, -1));
        // A normal value.
        assert!(!time_overflows(12, 34, 56, 0));
        // Leap-second-ish: sec == 60 is allowed field-wise, total still <= 24h.
        assert!(!time_overflows(0, 0, 60, 0));
    }

    #[test]
    fn float_time_overflows_nan_and_round() {
        assert!(float_time_overflows(0, 0, f64::NAN));
        assert!(!float_time_overflows(12, 0, 0.0));
        assert!(float_time_overflows(25, 0, 0.0));
        assert!(float_time_overflows(0, 0, 61.0));
    }

    #[test]
    fn adjust_time_for_typmod_rounds() {
        // 1 second + 0.5 ms at precision 0 rounds to whole seconds.
        let mut t: TimeADT = 1_500_000; // 1.5 s
        AdjustTimeForTypmod(&mut t, 0);
        assert_eq!(t, 2_000_000);
        // Negative value rounds symmetrically.
        let mut n: TimeADT = -1_500_000;
        AdjustTimeForTypmod(&mut n, 0);
        assert_eq!(n, -2_000_000);
        // typmod outside [0, MAX] leaves the value untouched.
        let mut u: TimeADT = 1_234_567;
        AdjustTimeForTypmod(&mut u, -1);
        assert_eq!(u, 1_234_567);
    }

    #[test]
    fn make_time_overflow_message() {
        let err = make_time(25, 0, 0.0).unwrap_err();
        assert!(err
            .message()
            .contains("time field value out of range: 25:00:00"));
    }

    #[test]
    fn make_time_ok() {
        assert_eq!(make_time(1, 0, 0.0).unwrap(), USECS_PER_HOUR);
        assert_eq!(make_time(0, 0, 1.5).unwrap(), 1_500_000);
    }

    #[test]
    fn comparison_cores() {
        assert_eq!(time_cmp(1, 2), -1);
        assert_eq!(time_cmp(2, 2), 0);
        assert_eq!(time_cmp(3, 2), 1);
        assert!(time_eq(5, 5));
        assert!(time_ne(5, 6));
        assert!(time_lt(1, 2) && time_le(2, 2) && time_gt(3, 2) && time_ge(2, 2));
        assert_eq!(time_larger(1, 2), 2);
        assert_eq!(time_smaller(1, 2), 1);
    }

    #[test]
    fn interval_arithmetic_cores() {
        // time_interval / time_mi_time build pure time-only intervals.
        let iv = time_interval(USECS_PER_HOUR);
        assert_eq!((iv.time, iv.day, iv.month), (USECS_PER_HOUR, 0, 0));
        let d = time_mi_time(3 * USECS_PER_HOUR, USECS_PER_HOUR);
        assert_eq!(d.time, 2 * USECS_PER_HOUR);

        // interval_time wraps into [0, 1 day).
        let neg = Interval {
            time: -USECS_PER_HOUR,
            day: 0,
            month: 0,
        };
        assert_eq!(interval_time(&neg).unwrap(), USECS_PER_DAY - USECS_PER_HOUR);

        // time + interval wraps within the day.
        let plus = time_pl_interval(23 * USECS_PER_HOUR, &time_interval(2 * USECS_PER_HOUR))
            .unwrap();
        assert_eq!(plus, USECS_PER_HOUR);
        // time - interval wraps within the day.
        let minus =
            time_mi_interval(USECS_PER_HOUR, &time_interval(2 * USECS_PER_HOUR)).unwrap();
        assert_eq!(minus, USECS_PER_DAY - USECS_PER_HOUR);
    }

    #[test]
    fn interval_not_finite_sentinels() {
        let pos = Interval {
            month: i32::MAX,
            day: i32::MAX,
            time: i64::MAX,
        };
        let neg = Interval {
            month: i32::MIN,
            day: i32::MIN,
            time: i64::MIN,
        };
        let finite = Interval {
            month: 1,
            day: 2,
            time: 3,
        };
        assert!(INTERVAL_NOT_FINITE(&pos));
        assert!(INTERVAL_NOT_FINITE(&neg));
        assert!(!INTERVAL_NOT_FINITE(&finite));
    }

    #[test]
    fn infinite_interval_conversions_error() {
        let inf = Interval {
            month: i32::MAX,
            day: i32::MAX,
            time: i64::MAX,
        };
        assert!(interval_time(&inf).is_err());
        assert!(time_pl_interval(0, &inf).is_err());
        assert!(time_mi_interval(0, &inf).is_err());
    }

    #[test]
    fn fmt_g_matches_g_style() {
        // Plain small magnitude renders %f-style.
        assert_eq!(fmt_g(5.0), "5");
        assert_eq!(fmt_g(0.0), "0");
        assert_eq!(fmt_g(1.5), "1.5");
        // Large magnitude switches to %e-style with 2-digit exponent.
        assert_eq!(fmt_g(1e16), "1e+16");
    }
}
