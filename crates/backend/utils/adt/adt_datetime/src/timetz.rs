//! The TIMETZ (time with time zone) value type, ported from
//! `src/backend/utils/adt/date.c` (idiomatic, safe Rust).
//!
//! Core (plain-Rust) half of date.c's TIMETZ ADT: `tm2timetz`/`timetz2tm`,
//! `timetz_in`/`timetz_out` cores, `timetz_cmp_internal` (the GMT-adjusted
//! then zone comparison), the comparison primitives, the `timetz ± interval`
//! arithmetic, `time2timetz`/`timetz_time` conversions, the `timetz_izone`
//! at-time-zone math (interval-offset variant), the `timetz_zone` (named-zone
//! variant), and the `extract_timetz` field-extraction core (float8 + numeric
//! paths).
//!
//! We do NOT port the fmgr `Datum` shims (those follow the project systemic
//! deferral).
//!
//! Idiomatic surface: plain `i32`/`i64`/`f64`, owned values, `Option`,
//! `Result`, `&str`.  No raw pointers, `extern "C"`, `c_int`, `libc`,
//! `CStr`/`CString`, or `pg_ffi_fgram`.


use std::rc::Rc;

use pgtime::pg_tm;
use mcx::Mcx;
use types_numeric::var::NumericVar;
use types_datetime::{
    Interval, TimeTzADT, MINS_PER_HOUR, SECS_PER_HOUR, SECS_PER_MINUTE, USECS_PER_DAY,
    USECS_PER_HOUR, USECS_PER_MINUTE, USECS_PER_SEC,
};
use types_error::{
    ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE,
};
use types_datetime::{fsec_t, TimeADT, TimeOffset};
use types_error::{ereturn, PgError, PgResult, SoftErrorContext};

use crate::decode::{DecodeTimeOnly, DecodeUnits, ParseDateTime};
use crate::encode::EncodeTimeOnly;
use crate::numeric_helpers::int64_div_fast_to_numericvar;
use crate::settings::date_style;
use crate::time::{AdjustTimeForTypmod, INTERVAL_NOT_FINITE};

const MAXDATEFIELDS: usize = types_datetime::MAXDATEFIELDS as usize;

// ---------------------------------------------------------------------------
// tm2timetz / timetz2tm
// ---------------------------------------------------------------------------

/// `tm2timetz()` -- convert a broken-down `pg_tm` (+ fsec + tz) to a `TimeTzADT`.
pub fn tm2timetz(tm: &pg_tm, fsec: fsec_t, tz: i32) -> TimeTzADT {
    TimeTzADT {
        time: ((((tm.tm_hour as i64 * MINS_PER_HOUR as i64 + tm.tm_min as i64)
            * SECS_PER_MINUTE as i64)
            + tm.tm_sec as i64)
            * USECS_PER_SEC)
            + fsec as i64,
        zone: tz,
    }
}

/// `timetz2tm()` -- convert a `TimeTzADT` to hour/min/sec fields of `tm`, plus
/// fsec and the stored zone.
pub fn timetz2tm(time: &TimeTzADT, tm: &mut pg_tm, fsec: &mut fsec_t, tzp: &mut i32) {
    let mut trem: TimeOffset = time.time;
    tm.tm_hour = (trem / USECS_PER_HOUR) as i32;
    trem -= tm.tm_hour as i64 * USECS_PER_HOUR;
    tm.tm_min = (trem / USECS_PER_MINUTE) as i32;
    trem -= tm.tm_min as i64 * USECS_PER_MINUTE;
    tm.tm_sec = (trem / USECS_PER_SEC) as i32;
    *fsec = (trem - tm.tm_sec as i64 * USECS_PER_SEC) as fsec_t;
    *tzp = time.zone;
}

// ---------------------------------------------------------------------------
// timetz_in / timetz_out cores
// ---------------------------------------------------------------------------

/// `timetz_in()` CORE -- parse a TIMETZ text string at the given typmod.
pub fn timetz_in(
    str: &str,
    typmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<TimeTzADT> {
    let mut field: Vec<String> = Vec::new();
    let mut ftype: Vec<i32> = Vec::new();
    let mut nf = 0usize;

    let mut tt = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut dtype: i32 = 0;
    let mut tz: i32 = 0;
    let mut extra = types_datetime::DateTimeErrorExtra::default();

    // C timetz_in: workbuf[MAXDATELEN + 1] (date.c:2353).
    let mut dterr = ParseDateTime(
        str,
        types_datetime::MAXDATELEN as usize + 1,
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
        // C: DateTimeParseError(dterr, &extra, str, "time with time zone",
        //    NULL, escontext) — maps each dterr code to its own SQLSTATE and
        //    ereturns: with a soft sink the error is saved and a (discarded)
        //    value returned Ok; without one it throws. pg_input_is_valid /
        //    pg_input_error_info on a timetz literal rely on the soft path.
        return ereturn(
            escontext,
            TimeTzADT::default(),
            crate::date::datetime_parse_error_for(dterr, str, "time with time zone", &extra),
        );
    }

    let mut result = tm2timetz(&tt, fsec, tz);
    AdjustTimeForTypmod(&mut result.time, typmod);
    Ok(result)
}

/// `timetz_out()` CORE -- render a `TimeTzADT` to a text string.
pub fn timetz_out(time: &TimeTzADT) -> String {
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    timetz2tm(time, &mut tm, &mut fsec, &mut tz);
    let mut buf = String::new();
    EncodeTimeOnly(&tm, fsec, true, tz, date_style(), &mut buf);
    buf
}

// ---------------------------------------------------------------------------
// timetz_scale
// ---------------------------------------------------------------------------

/// `timetz_scale()` CORE -- adjust a TIMETZ to the precision in `typmod`.
pub fn timetz_scale(time: &TimeTzADT, typmod: i32) -> TimeTzADT {
    let mut result = TimeTzADT {
        time: time.time,
        zone: time.zone,
    };
    AdjustTimeForTypmod(&mut result.time, typmod);
    result
}

// ---------------------------------------------------------------------------
// timetz_cmp_internal + comparison cores
// ---------------------------------------------------------------------------

/// `timetz_cmp_internal()` CORE -- compare two TIMETZ values.
///
/// Primary sort is by true (GMT-equivalent) time; ties break by the stored
/// zone, so two timetz values are equal only if both time and zone match.
pub fn timetz_cmp_internal(time1: &TimeTzADT, time2: &TimeTzADT) -> i32 {
    // Primary sort is by true (GMT-equivalent) time.
    let t1: TimeOffset = time1.time + (time1.zone as i64 * USECS_PER_SEC);
    let t2: TimeOffset = time2.time + (time2.zone as i64 * USECS_PER_SEC);

    if t1 > t2 {
        return 1;
    }
    if t1 < t2 {
        return -1;
    }

    // If same GMT time, sort by timezone.
    if time1.zone > time2.zone {
        return 1;
    }
    if time1.zone < time2.zone {
        return -1;
    }

    0
}

/// `timetz_eq()` CORE.
#[inline]
pub fn timetz_eq(a: &TimeTzADT, b: &TimeTzADT) -> bool {
    timetz_cmp_internal(a, b) == 0
}

/// `timetz_ne()` CORE.
#[inline]
pub fn timetz_ne(a: &TimeTzADT, b: &TimeTzADT) -> bool {
    timetz_cmp_internal(a, b) != 0
}

/// `timetz_lt()` CORE.
#[inline]
pub fn timetz_lt(a: &TimeTzADT, b: &TimeTzADT) -> bool {
    timetz_cmp_internal(a, b) < 0
}

/// `timetz_le()` CORE.
#[inline]
pub fn timetz_le(a: &TimeTzADT, b: &TimeTzADT) -> bool {
    timetz_cmp_internal(a, b) <= 0
}

/// `timetz_gt()` CORE.
#[inline]
pub fn timetz_gt(a: &TimeTzADT, b: &TimeTzADT) -> bool {
    timetz_cmp_internal(a, b) > 0
}

/// `timetz_ge()` CORE.
#[inline]
pub fn timetz_ge(a: &TimeTzADT, b: &TimeTzADT) -> bool {
    timetz_cmp_internal(a, b) >= 0
}

/// `timetz_cmp()` CORE.
#[inline]
pub fn timetz_cmp(a: &TimeTzADT, b: &TimeTzADT) -> i32 {
    timetz_cmp_internal(a, b)
}

/// `timetz_larger()` CORE.
#[inline]
pub fn timetz_larger(a: TimeTzADT, b: TimeTzADT) -> TimeTzADT {
    if timetz_cmp_internal(&a, &b) > 0 {
        a
    } else {
        b
    }
}

/// `timetz_smaller()` CORE.
#[inline]
pub fn timetz_smaller(a: TimeTzADT, b: TimeTzADT) -> TimeTzADT {
    if timetz_cmp_internal(&a, &b) < 0 {
        a
    } else {
        b
    }
}

// ---------------------------------------------------------------------------
// Arithmetic cores
// ---------------------------------------------------------------------------

/// `timetz_pl_interval()` CORE -- add an interval to a TIMETZ (wrapping in-day).
pub fn timetz_pl_interval(time: &TimeTzADT, span: &Interval) -> PgResult<TimeTzADT> {
    if INTERVAL_NOT_FINITE(span) {
        return Err(PgError::error("cannot add infinite interval to time")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
    }
    // C: `result->time = time->time + span->time` -- plain int64 add that relies
    // on 2s-complement wraparound; use wrapping ops to match.
    let mut result = TimeTzADT {
        time: time.time.wrapping_add(span.time),
        zone: time.zone,
    };
    result.time = result
        .time
        .wrapping_sub(result.time / USECS_PER_DAY * USECS_PER_DAY);
    if result.time < 0 {
        result.time = result.time.wrapping_add(USECS_PER_DAY);
    }
    Ok(result)
}

/// `timetz_mi_interval()` CORE -- subtract an interval from a TIMETZ (wrapping).
pub fn timetz_mi_interval(time: &TimeTzADT, span: &Interval) -> PgResult<TimeTzADT> {
    if INTERVAL_NOT_FINITE(span) {
        return Err(
            PgError::error("cannot subtract infinite interval from time")
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        );
    }
    let mut result = TimeTzADT {
        time: time.time.wrapping_sub(span.time),
        zone: time.zone,
    };
    result.time = result
        .time
        .wrapping_sub(result.time / USECS_PER_DAY * USECS_PER_DAY);
    if result.time < 0 {
        result.time = result.time.wrapping_add(USECS_PER_DAY);
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

/// `timetz_time()` CORE -- drop the zone, returning the bare TIME.
#[inline]
pub fn timetz_time(timetz: &TimeTzADT) -> TimeADT {
    timetz.time
}

/// `time2timetz()` CORE -- attach a fixed zone offset (seconds) to a TIME.
///
/// This is the zone-supplied core of `time_timetz`; the fmgr `time_timetz`
/// additionally derives `tz` from the session timezone at "today".
pub fn time2timetz(time: TimeADT, tz: i32) -> TimeTzADT {
    TimeTzADT { time, zone: tz }
}

/// `timetz_izone()` CORE -- re-express a TIMETZ in the zone given by an interval
/// offset.  The interval must be finite and contain no months/days.
pub fn timetz_izone(zone: &Interval, time: &TimeTzADT) -> PgResult<TimeTzADT> {
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

    // C (date.c:3208) negates in int64 then narrows to int; match that order.
    let tz = (-(zone.time / USECS_PER_SEC)) as i32;

    let mut result = TimeTzADT {
        time: time.time + (time.zone as i64 - tz as i64) * USECS_PER_SEC,
        zone: tz,
    };
    // C99 modulo has the wrong sign convention for negative input.
    while result.time < 0 {
        result.time += USECS_PER_DAY;
    }
    if result.time >= USECS_PER_DAY {
        result.time %= USECS_PER_DAY;
    }

    Ok(result)
}

/// `timetz_zone()` (date.c:3124) CORE -- re-express a TIMETZ in the named zone
/// `zone`, applying DST rules as of the transaction start time.
pub fn timetz_zone(zone: &str, t: &TimeTzADT) -> PgResult<TimeTzADT> {
    use crate::decode::{DecodeTimezoneName, DetermineTimeZoneAbbrevOffsetTS};
    use types_datetime::{TZNAME_DYNTZ, TZNAME_FIXED_OFFSET};

    let mut val: i32 = 0;
    let mut tzp: Option<Rc<pgtime::pg_tz>> = None;
    let type_ = DecodeTimezoneName(zone, &mut val, &mut tzp)?;

    let tz: i32;
    if type_ == TZNAME_FIXED_OFFSET {
        // fixed-offset abbreviation
        tz = -val;
    } else if type_ == TZNAME_DYNTZ {
        // dynamic-offset abbreviation, resolve using transaction start time
        let now = transam_xact::GetCurrentTransactionStartTimestamp();
        let tzp = tzp.ok_or_else(|| {
            PgError::error("timetz_zone: DecodeTimezoneName sets *tz for DYNTZ")
        })?;
        let mut isdst: i32 = 0;
        tz = DetermineTimeZoneAbbrevOffsetTS(now, zone, &tzp, &mut isdst)?;
    } else {
        // Get the offset-from-GMT that is valid now for the zone name.
        let now = transam_xact::GetCurrentTransactionStartTimestamp();
        let tzp = tzp.ok_or_else(|| {
            PgError::error("timetz_zone: DecodeTimezoneName sets *tz for ZONE")
        })?;
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        let mut z: i32 = 0;
        if crate::timestamp::timestamp2tm(now, Some(&mut z), &mut tm, &mut fsec, None, Some(&tzp))
            .is_err()
        {
            return Err(PgError::error("timestamp out of range")
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE));
        }
        tz = z;
    }

    let mut result = TimeTzADT {
        time: t.time + (t.zone as i64 - tz as i64) * USECS_PER_SEC,
        zone: tz,
    };
    // C99 modulo has the wrong sign convention for negative input.
    while result.time < 0 {
        result.time += USECS_PER_DAY;
    }
    if result.time >= USECS_PER_DAY {
        result.time %= USECS_PER_DAY;
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// extract_timetz() core
// ---------------------------------------------------------------------------

/// The result of [`timetz_part_common`]: a float8 value, an int64 (wrap with
/// `int64_to_numeric` for the numeric path), or a pre-built [`NumericVar`].
#[derive(Clone, Debug)]
pub enum TimetzPartResult<'mcx> {
    /// `PG_RETURN_FLOAT8` value (the `retnumeric == false` path).
    Float(f64),
    /// An integer field value.
    Int(i64),
    /// A pre-built numeric (the `int64_div_fast_to_numeric` fractional path).
    Numeric(NumericVar<'mcx>),
}

/// `timetz_part_common()` CORE -- extract `lowunits` from a TIMETZ.
pub fn timetz_part_common<'mcx>(
    mcx: Mcx<'mcx>,
    lowunits: &str,
    time: &TimeTzADT,
    retnumeric: bool,
) -> PgResult<TimetzPartResult<'mcx>> {
    use types_datetime::{
        DTK_EPOCH, DTK_HOUR, DTK_MICROSEC, DTK_MILLISEC, DTK_MINUTE, DTK_SECOND, DTK_TZ,
        DTK_TZ_HOUR, DTK_TZ_MINUTE, RESERV, UNITS, UNKNOWN_FIELD,
    };

    let mut val: i32 = 0;
    let mut typ = DecodeUnits(0, lowunits, &mut val);
    if typ == UNKNOWN_FIELD {
        typ = crate::decode::DecodeSpecial(0, lowunits, &mut val);
    }

    if typ == UNITS {
        let mut tz: i32 = 0;
        let mut fsec: fsec_t = 0;
        let mut tm = pg_tm::default();
        timetz2tm(time, &mut tm, &mut fsec, &mut tz);

        let intresult: i64 = match val {
            v if v == DTK_TZ => -tz as i64,
            v if v == DTK_TZ_MINUTE => ((-tz / SECS_PER_MINUTE) % MINS_PER_HOUR) as i64,
            v if v == DTK_TZ_HOUR => (-tz / SECS_PER_HOUR) as i64,
            v if v == DTK_MICROSEC => tm.tm_sec as i64 * 1_000_000 + fsec as i64,
            v if v == DTK_MILLISEC => {
                return if retnumeric {
                    Ok(TimetzPartResult::Numeric(int64_div_fast_to_numericvar(
                        mcx,
                        tm.tm_sec as i64 * 1_000_000 + fsec as i64,
                        3,
                    )?))
                } else {
                    Ok(TimetzPartResult::Float(
                        tm.tm_sec as f64 * 1000.0 + fsec as f64 / 1000.0,
                    ))
                };
            }
            v if v == DTK_SECOND => {
                return if retnumeric {
                    Ok(TimetzPartResult::Numeric(int64_div_fast_to_numericvar(
                        mcx,
                        tm.tm_sec as i64 * 1_000_000 + fsec as i64,
                        6,
                    )?))
                } else {
                    Ok(TimetzPartResult::Float(
                        tm.tm_sec as f64 + fsec as f64 / 1_000_000.0,
                    ))
                };
            }
            v if v == DTK_MINUTE => tm.tm_min as i64,
            v if v == DTK_HOUR => tm.tm_hour as i64,
            _ => return Err(timetz_unit_not_supported(lowunits)),
        };
        finalize_int(intresult, retnumeric)
    } else if typ == RESERV && val == DTK_EPOCH {
        if retnumeric {
            // (time->time + time->zone * 1'000'000) / 1'000'000
            Ok(TimetzPartResult::Numeric(int64_div_fast_to_numericvar(
                mcx,
                time.time + time.zone as i64 * 1_000_000,
                6,
            )?))
        } else {
            Ok(TimetzPartResult::Float(
                time.time as f64 / 1_000_000.0 + time.zone as f64,
            ))
        }
    } else {
        Err(timetz_unit_not_recognized(lowunits))
    }
}

#[inline]
fn finalize_int<'mcx>(intresult: i64, retnumeric: bool) -> PgResult<TimetzPartResult<'mcx>> {
    if retnumeric {
        Ok(TimetzPartResult::Int(intresult))
    } else {
        Ok(TimetzPartResult::Float(intresult as f64))
    }
}

#[inline]
fn timetz_unit_not_supported(lowunits: &str) -> PgError {
    PgError::error(format!(
        "unit \"{lowunits}\" not supported for type time with time zone"
    ))
    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

#[inline]
fn timetz_unit_not_recognized(lowunits: &str) -> PgError {
    PgError::error(format!(
        "unit \"{lowunits}\" not recognized for type time with time zone"
    ))
    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::{set_date_style, DATE_ORDER_TEST_LOCK};
    use types_datetime::USE_ISO_DATES;

    fn parse(s: &str) -> TimeTzADT {
        let _guard = DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        set_date_style(USE_ISO_DATES);
        timetz_in(s, -1, None).unwrap()
    }

    #[test]
    fn timetz_in_out_round_trip() {
        let _guard = DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        set_date_style(USE_ISO_DATES);
        for s in ["12:00:00+00", "12:00:00-05", "23:59:59+10", "00:00:00+00"] {
            let t = timetz_in(s, -1, None).unwrap();
            assert_eq!(timetz_out(&t), s, "round trip failed for {s}");
        }
    }

    #[test]
    fn timetz_in_maps_dterr_to_specific_sqlstate() {
        crate::test_install_seams();
        use types_error::{
            ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_INVALID_DATETIME_FORMAT,
            ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
        };
        let _guard = DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        set_date_style(USE_ISO_DATES);

        let err = timetz_in("25:00:00+00", -1, None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_DATETIME_FIELD_OVERFLOW);

        let err = timetz_in("12:00:00+16:00", -1, None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE);

        let err = timetz_in("not a time", -1, None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_DATETIME_FORMAT);
    }

    #[test]
    fn timetz_compare_gmt_order() {
        let a = parse("12:00:00+00");
        let b = parse("12:00:00-01");
        assert_eq!(timetz_cmp_internal(&a, &b), -1);
        assert_eq!(timetz_cmp_internal(&b, &a), 1);
        assert!(timetz_lt(&a, &b));
        assert!(timetz_gt(&b, &a));

        let c = parse("12:00:00+00");
        let d = parse("13:00:00+01");
        assert_eq!(
            c.time + c.zone as i64 * USECS_PER_SEC,
            d.time + d.zone as i64 * USECS_PER_SEC,
            "should be identical GMT instants"
        );
        assert_eq!(timetz_cmp_internal(&c, &d), -(timetz_cmp_internal(&d, &c)));
        assert_ne!(timetz_cmp_internal(&c, &d), 0);
    }

    #[test]
    fn tm2timetz_timetz2tm_round_trip() {
        let tm = pg_tm {
            tm_hour: 8,
            tm_min: 30,
            tm_sec: 15,
            ..Default::default()
        };
        let tz = -5 * SECS_PER_HOUR;
        let v = tm2timetz(&tm, 250_000, tz);
        let mut tm2 = pg_tm::default();
        let mut fsec = 0;
        let mut tzp = 0;
        timetz2tm(&v, &mut tm2, &mut fsec, &mut tzp);
        assert_eq!(
            (tm2.tm_hour, tm2.tm_min, tm2.tm_sec, fsec, tzp),
            (8, 30, 15, 250_000, tz)
        );
    }

    #[test]
    fn timetz_plus_interval_wraps() {
        let t = parse("23:00:00+00");
        let span = Interval {
            month: 0,
            day: 0,
            time: 2 * USECS_PER_HOUR,
        };
        let r = timetz_pl_interval(&t, &span).unwrap();
        assert_eq!(timetz_out(&r), "01:00:00+00");
    }

    #[test]
    fn time2timetz_attaches_zone() {
        let v = time2timetz(USECS_PER_HOUR * 12, 0);
        assert_eq!(v.time, USECS_PER_HOUR * 12);
        assert_eq!(v.zone, 0);
        assert_eq!(timetz_time(&v), USECS_PER_HOUR * 12);
    }

    #[test]
    fn timetz_izone_shifts_zone() {
        let t = parse("12:00:00+00");
        let zone = Interval {
            month: 0,
            day: 0,
            time: USECS_PER_HOUR, // +1 hour
        };
        let r = timetz_izone(&zone, &t).unwrap();
        assert_eq!(r.zone, -SECS_PER_HOUR);
        assert_eq!(
            r.time + r.zone as i64 * USECS_PER_SEC,
            t.time + t.zone as i64 * USECS_PER_SEC
        );
    }

    #[test]
    fn extract_timetz_fields() {
        let ctx = mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let t = parse("13:45:30+00");
        match timetz_part_common(mcx, "hour", &t, false).unwrap() {
            TimetzPartResult::Float(f) => assert_eq!(f, 13.0),
            other => panic!("expected Float, got {other:?}"),
        }
        match timetz_part_common(mcx, "minute", &t, true).unwrap() {
            TimetzPartResult::Int(i) => assert_eq!(i, 45),
            other => panic!("expected Int, got {other:?}"),
        }
        assert!(timetz_part_common(mcx, "nope", &t, false).is_err());
    }

    #[test]
    fn timetz_zone_no_resolver_unknown_name_errors() {
        crate::test_install_seams();
        let _g = crate::tz_resolver::TZ_RESOLVER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let prev = crate::tz_resolver::set_timezone_resolver(None);
        let t = parse("12:00:00+00");
        let err = timetz_zone("Nowhere/Land", &t).unwrap_err();
        assert_eq!(err.message(), "time zone \"Nowhere/Land\" not recognized");
        crate::tz_resolver::set_timezone_resolver(prev);
    }

    #[test]
    fn timetz_zone_with_resolver_fixed_abbrev() {
        static R: crate::tz_resolver::TestTimezoneResolver =
            crate::tz_resolver::TestTimezoneResolver;
        let _g = crate::tz_resolver::TZ_RESOLVER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let prev = crate::tz_resolver::set_timezone_resolver(Some(&R));

        let t = parse("12:00:00+00");
        let r = timetz_zone("xyz", &t).unwrap();
        assert_eq!(timetz_out(&r), "13:00:00+01");
        assert_eq!(
            r.time + r.zone as i64 * USECS_PER_SEC,
            t.time + t.zone as i64 * USECS_PER_SEC
        );

        crate::tz_resolver::set_timezone_resolver(prev);
    }
}
