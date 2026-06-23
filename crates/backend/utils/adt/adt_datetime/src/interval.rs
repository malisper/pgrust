//! INTERVAL value cores, ported from `src/backend/utils/adt/timestamp.c`
//! (idiomatic, safe Rust).
//!
//! Ports the plain-Rust cores: [`interval2itm`]/[`itm2interval`]/
//! [`itmin2interval`], the `interval_in`/`interval_out` cores
//! (DecodeInterval + EncodeInterval), the comparison cores (using native `i128`
//! for [`interval_cmp_value`]), the arithmetic cores (+, -, *, /, unary minus),
//! [`interval_justify_hours`]/[`interval_justify_days`]/
//! [`interval_justify_interval`], and [`AdjustIntervalForTypmod`].
//!
//! Fmgr `Datum` shims are NOT ported.  Out-of-range conditions surface as a
//! `PgError` ([`DtResult`]) for a P5 fmgr shim to map to `ereport`.
//!
//! Idiomatic surface: plain `i32`/`i64`/`i128`, owned values.  No raw pointers,
//! `extern "C"`, `c_int`, `libc`, or `pg_ffi_fgram`.


use types_datetime::{
    pg_itm, pg_itm_in, Interval, DAYS_PER_MONTH, INTERVAL_FULL_PRECISION, INTERVAL_FULL_RANGE,
    INTERVAL_MASK, MAX_INTERVAL_PRECISION, MONTHS_PER_YEAR, USECS_PER_DAY, USECS_PER_HOUR,
    USECS_PER_MINUTE, USECS_PER_SEC,
};
use types_datetime::{DAY, HOUR, MINUTE, MONTH, SECOND, YEAR};
use types_error::{ereturn, PgError, SoftErrorContext};

use crate::timestamp::{pg_add_s32_overflow, pg_add_s64_overflow, pg_sub_s64_overflow, DtResult};

// ---------------------------------------------------------------------------
// Infinite-interval helpers (datatype/timestamp.h macros).
// ---------------------------------------------------------------------------

/// `INTERVAL_IS_NOBEGIN(i)`.
#[inline]
pub fn INTERVAL_IS_NOBEGIN(i: &Interval) -> bool {
    i.month == i32::MIN && i.day == i32::MIN && i.time == i64::MIN
}

/// `INTERVAL_IS_NOEND(i)`.
#[inline]
pub fn INTERVAL_IS_NOEND(i: &Interval) -> bool {
    i.month == i32::MAX && i.day == i32::MAX && i.time == i64::MAX
}

/// `INTERVAL_NOT_FINITE(i)`.
#[inline]
pub fn INTERVAL_NOT_FINITE(i: &Interval) -> bool {
    INTERVAL_IS_NOBEGIN(i) || INTERVAL_IS_NOEND(i)
}

/// An `-infinity` interval (`INTERVAL_NOBEGIN`).
#[inline]
pub fn interval_nobegin() -> Interval {
    Interval {
        time: i64::MIN,
        day: i32::MIN,
        month: i32::MIN,
    }
}

/// An `infinity` interval (`INTERVAL_NOEND`).
#[inline]
pub fn interval_noend() -> Interval {
    Interval {
        time: i64::MAX,
        day: i32::MAX,
        month: i32::MAX,
    }
}

#[inline]
fn set_nobegin(i: &mut Interval) {
    i.time = i64::MIN;
    i.day = i32::MIN;
    i.month = i32::MIN;
}

#[inline]
fn set_noend(i: &mut Interval) {
    i.time = i64::MAX;
    i.day = i32::MAX;
    i.month = i32::MAX;
}

// ---------------------------------------------------------------------------
// interval2itm / itm2interval / itmin2interval
// ---------------------------------------------------------------------------

/// `interval2itm()` -- convert an `Interval` to a broken-down `pg_itm`.
/// Overflow is impossible (the `pg_itm` fields are wide enough).
///
/// (`utils/adt/timestamp.c`)
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

/// `itm2interval()` -- convert a `pg_itm` to an `Interval`.  Returns `Err(())`
/// on overflow (including any infinite result, which is treated as overflow).
///
/// (`utils/adt/timestamp.c`)
#[allow(clippy::result_unit_err)]
pub fn itm2interval(itm: &pg_itm, span: &mut Interval) -> Result<(), ()> {
    let total_months: i64 = itm.tm_year as i64 * MONTHS_PER_YEAR as i64 + itm.tm_mon as i64;

    if total_months > i32::MAX as i64 || total_months < i32::MIN as i64 {
        return Err(());
    }
    span.month = total_months as i32;
    span.day = itm.tm_mday;
    if pg_mul_s64_overflow(itm.tm_hour, USECS_PER_HOUR, &mut span.time) {
        return Err(());
    }
    if pg_add_s64_overflow(
        span.time,
        itm.tm_min as i64 * USECS_PER_MINUTE,
        &mut span.time,
    ) {
        return Err(());
    }
    if pg_add_s64_overflow(span.time, itm.tm_sec as i64 * USECS_PER_SEC, &mut span.time) {
        return Err(());
    }
    if pg_add_s64_overflow(span.time, itm.tm_usec as i64, &mut span.time) {
        return Err(());
    }
    if INTERVAL_NOT_FINITE(span) {
        return Err(());
    }
    Ok(())
}

/// `itmin2interval()` -- convert a `pg_itm_in` to an `Interval`.  Returns
/// `Err(())` on overflow.  Note: infinite results are NOT treated as overflow
/// (they pass through), matching the C dump/reload-compat behavior.
///
/// (`utils/adt/timestamp.c`)
#[allow(clippy::result_unit_err)]
pub fn itmin2interval(itm_in: &pg_itm_in, span: &mut Interval) -> Result<(), ()> {
    let total_months: i64 = itm_in.tm_year as i64 * MONTHS_PER_YEAR as i64 + itm_in.tm_mon as i64;

    if total_months > i32::MAX as i64 || total_months < i32::MIN as i64 {
        return Err(());
    }
    span.month = total_months as i32;
    span.day = itm_in.tm_mday;
    span.time = itm_in.tm_usec;
    Ok(())
}

#[inline]
fn pg_mul_s64_overflow(a: i64, b: i64, res: &mut i64) -> bool {
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
// interval_in / interval_out cores
// ---------------------------------------------------------------------------

/// `interval_in()` core: parse `str` into an `Interval`, applying `typmod`.
///
/// (`utils/adt/timestamp.c`)
pub fn interval_in(str: &str, typmod: i32) -> DtResult<Interval> {
    interval_in_safe(str, typmod, None)
}

/// `interval_in()` core with a soft-error sink (see `date_in_safe`).
pub fn interval_in_safe(
    str: &str,
    typmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> DtResult<Interval> {
    use types_datetime::{
        DTERR_BAD_FORMAT, DTERR_FIELD_OVERFLOW, DTERR_INTERVAL_OVERFLOW, DTK_DELTA, DTK_EARLY,
        DTK_LATE, INTERVAL_RANGE, MAXDATEFIELDS,
    };

    let mut itm_in = pg_itm_in {
        tm_usec: 0,
        tm_mday: 0,
        tm_mon: 0,
        tm_year: 0,
    };
    let mut dtype: i32 = 0;
    let mut nf: usize = 0;
    let mut field: Vec<String> = Vec::new();
    let mut ftype: Vec<i32> = Vec::new();

    let range = if typmod >= 0 {
        INTERVAL_RANGE(typmod)
    } else {
        INTERVAL_FULL_RANGE
    };

    // C interval_in: workbuf[256] (timestamp.c:908).
    let mut dterr = crate::decode::ParseDateTime(
        str,
        256,
        &mut field,
        &mut ftype,
        MAXDATEFIELDS as usize,
        &mut nf,
    );
    if dterr == 0 {
        dterr = crate::decode::DecodeInterval(
            &mut field,
            &mut ftype,
            nf,
            range,
            &mut dtype,
            &mut itm_in,
        );
    }

    /* if those functions think it's a bad format, try ISO8601 style */
    if dterr == DTERR_BAD_FORMAT {
        dterr = crate::decode::DecodeISO8601Interval(str, &mut dtype, &mut itm_in);
    }

    if dterr != 0 {
        // C remaps a plain field overflow to an interval-specific overflow so
        // the error reads "interval out of range" rather than a generic
        // date/time field message.
        if dterr == DTERR_FIELD_OVERFLOW {
            dterr = DTERR_INTERVAL_OVERFLOW;
        }
        return ereturn(
            escontext.as_deref_mut(),
            Interval { time: 0, day: 0, month: 0 },
            crate::timestamp::datetime_parse_error(
                dterr,
                str,
                "interval",
                &::types_datetime::DateTimeErrorExtra::default(),
            ),
        );
    }

    let mut result = Interval {
        time: 0,
        day: 0,
        month: 0,
    };

    match dtype {
        DTK_DELTA => {
            if itmin2interval(&itm_in, &mut result).is_err() {
                return ereturn(
                    escontext.as_deref_mut(),
                    Interval { time: 0, day: 0, month: 0 },
                    crate::timestamp::interval_out_of_range(),
                );
            }
        }
        DTK_LATE => set_noend(&mut result),
        DTK_EARLY => set_nobegin(&mut result),
        _ => {
            return Err(crate::timestamp::internal_error(format!(
                "unexpected dtype {dtype} while parsing interval \"{str}\""
            )))
        }
    }

    AdjustIntervalForTypmod(&mut result, typmod)?;
    Ok(result)
}

/// `interval_out()` core: format an `Interval` to its textual form using the
/// session `IntervalStyle`.  (`utils/adt/timestamp.c`)
pub fn interval_out(span: &Interval) -> String {
    let mut buf = String::new();
    if INTERVAL_NOT_FINITE(span) {
        EncodeSpecialInterval(span, &mut buf);
    } else {
        let mut itm = pg_itm {
            tm_usec: 0,
            tm_sec: 0,
            tm_min: 0,
            tm_hour: 0,
            tm_mday: 0,
            tm_mon: 0,
            tm_year: 0,
        };
        interval2itm(*span, &mut itm);
        crate::encode::EncodeInterval(&itm, crate::settings::interval_style(), &mut buf);
    }
    buf
}

/// `EncodeSpecialInterval()` -- convert a reserved (infinity) interval to its
/// string form.  (`utils/adt/timestamp.c`)
pub fn EncodeSpecialInterval(itv: &Interval, str: &mut String) {
    if INTERVAL_IS_NOBEGIN(itv) {
        str.push_str(crate::consts::EARLY);
    } else if INTERVAL_IS_NOEND(itv) {
        str.push_str(crate::consts::LATE);
    }
    /* otherwise the C code elog(ERROR)s; we leave str untouched */
}

// ---------------------------------------------------------------------------
// intervaltypmodin
// ---------------------------------------------------------------------------

/// `intervaltypmodin(cstring[])` (timestamp.c:1135) core — validate + pack an
/// `interval(fields, precision)` typmod, over the already-parsed integer list
/// (the fmgr boundary performs the `ArrayGetIntegerTypmods` cstring→int parse).
/// `tl[0]` is the grammar's `INTERVAL_MASK` fields bitmask; `tl[1]` the optional
/// precision.  Over-max precision clamps to `MAX_INTERVAL_PRECISION` (C also
/// emits a WARNING there — same silent-clamp deferral as
/// `anytimestamp_typmod_check`).
pub fn intervaltypmodin(tl: &[i32]) -> DtResult<i32> {
    use crate::timestamp::invalid_parameter;
    use ::types_datetime::INTERVAL_TYPMOD;
    // C: the valid-mask switch (timestamp.c:1147-1175).
    const VALID_RANGES: [i32; 14] = [
        INTERVAL_MASK(YEAR),
        INTERVAL_MASK(MONTH),
        INTERVAL_MASK(DAY),
        INTERVAL_MASK(HOUR),
        INTERVAL_MASK(MINUTE),
        INTERVAL_MASK(SECOND),
        INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH),
        INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR),
        INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE),
        INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND),
        INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE),
        INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND),
        INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND),
        INTERVAL_FULL_RANGE,
    ];
    if !tl.is_empty() && !VALID_RANGES.contains(&tl[0]) {
        return Err(invalid_parameter("invalid INTERVAL type modifier"));
    }
    match tl {
        // C: n == 1 — fields only; INTERVAL_FULL_RANGE alone means typmod -1.
        [range] if *range != INTERVAL_FULL_RANGE => {
            Ok(INTERVAL_TYPMOD(INTERVAL_FULL_PRECISION, *range))
        }
        [_] => Ok(-1),
        // C: n == 2 — fields + precision (negative errors, over-max clamps).
        [range, prec] => {
            if *prec < 0 {
                return Err(invalid_parameter(format!(
                    "INTERVAL({prec}) precision must not be negative"
                )));
            }
            Ok(INTERVAL_TYPMOD((*prec).min(MAX_INTERVAL_PRECISION), *range))
        }
        // C: anything else — "invalid INTERVAL type modifier".
        _ => Err(invalid_parameter("invalid INTERVAL type modifier")),
    }
}

/// `intervaltypmodout(int4)` (timestamp.c) core — render an interval typmod as
/// its printable suffix (e.g. `" day to second"`, `" hour(3)"`, or `""`).
pub fn intervaltypmodout(typmod: i32) -> DtResult<String> {
    use types_datetime::{INTERVAL_PRECISION, INTERVAL_RANGE};

    if typmod < 0 {
        return Ok(String::new());
    }

    let fields = INTERVAL_RANGE(typmod);
    let precision = INTERVAL_PRECISION(typmod);

    let fieldstr: &str = if fields == INTERVAL_MASK(YEAR) {
        " year"
    } else if fields == INTERVAL_MASK(MONTH) {
        " month"
    } else if fields == INTERVAL_MASK(DAY) {
        " day"
    } else if fields == INTERVAL_MASK(HOUR) {
        " hour"
    } else if fields == INTERVAL_MASK(MINUTE) {
        " minute"
    } else if fields == INTERVAL_MASK(SECOND) {
        " second"
    } else if fields == (INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH)) {
        " year to month"
    } else if fields == (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR)) {
        " day to hour"
    } else if fields == (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)) {
        " day to minute"
    } else if fields
        == (INTERVAL_MASK(DAY)
            | INTERVAL_MASK(HOUR)
            | INTERVAL_MASK(MINUTE)
            | INTERVAL_MASK(SECOND))
    {
        " day to second"
    } else if fields == (INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)) {
        " hour to minute"
    } else if fields == (INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)) {
        " hour to second"
    } else if fields == (INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)) {
        " minute to second"
    } else if fields == INTERVAL_FULL_RANGE {
        ""
    } else {
        return Err(crate::timestamp::internal_error(format!(
            "invalid INTERVAL typmod: 0x{typmod:x}"
        )));
    };

    if precision != INTERVAL_FULL_PRECISION {
        Ok(format!("{fieldstr}({precision})"))
    } else {
        Ok(fieldstr.to_string())
    }
}

// ---------------------------------------------------------------------------
// AdjustIntervalForTypmod
// ---------------------------------------------------------------------------

const INTERVAL_SCALES: [i64; (MAX_INTERVAL_PRECISION + 1) as usize] =
    [1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];
const INTERVAL_OFFSETS: [i64; (MAX_INTERVAL_PRECISION + 1) as usize] =
    [500_000, 50_000, 5_000, 500, 50, 5, 0];

/// `AdjustIntervalForTypmod()` -- truncate/round interval fields per `typmod`.
///
/// (`utils/adt/timestamp.c`)
//
// `clippy::if_same_then_else`: this is a 1:1 port of the C `if/else if` chain
// that enumerates every distinct `INTERVAL_RANGE` mask value.  Several distinct
// masks legitimately share an action (e.g. `INTERVAL MONTH` and
// `INTERVAL YEAR TO MONTH` both zero `day`/`time`; the various `... TO SECOND`
// masks all defer to the fractional-second rounding below).  Collapsing the
// branches would erase the per-mask mapping and diverge from upstream, so we
// keep the explicit cases and silence the lint locally.
#[allow(clippy::if_same_then_else)]
pub fn AdjustIntervalForTypmod(interval: &mut Interval, typmod: i32) -> DtResult<()> {
    use types_datetime::{INTERVAL_PRECISION, INTERVAL_RANGE};

    /* Typmod has no effect on infinite intervals */
    if INTERVAL_NOT_FINITE(interval) {
        return Ok(());
    }

    if typmod >= 0 {
        let range = INTERVAL_RANGE(typmod);
        let precision = INTERVAL_PRECISION(typmod);

        if range == INTERVAL_FULL_RANGE {
            /* Do nothing... */
        } else if range == INTERVAL_MASK(YEAR) {
            interval.month = (interval.month / MONTHS_PER_YEAR) * MONTHS_PER_YEAR;
            interval.day = 0;
            interval.time = 0;
        } else if range == INTERVAL_MASK(MONTH) {
            interval.day = 0;
            interval.time = 0;
        } else if range == (INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH)) {
            interval.day = 0;
            interval.time = 0;
        } else if range == INTERVAL_MASK(DAY) {
            interval.time = 0;
        } else if range == INTERVAL_MASK(HOUR) {
            interval.time = (interval.time / USECS_PER_HOUR) * USECS_PER_HOUR;
        } else if range == INTERVAL_MASK(MINUTE) {
            interval.time = (interval.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
        } else if range == INTERVAL_MASK(SECOND) {
            /* fractional-second rounding will be dealt with below */
        } else if range == (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR)) {
            interval.time = (interval.time / USECS_PER_HOUR) * USECS_PER_HOUR;
        } else if range == (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)) {
            interval.time = (interval.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
        } else if range
            == (INTERVAL_MASK(DAY)
                | INTERVAL_MASK(HOUR)
                | INTERVAL_MASK(MINUTE)
                | INTERVAL_MASK(SECOND))
        {
            /* fractional-second rounding will be dealt with below */
        } else if range == (INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)) {
            interval.time = (interval.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
        } else if range == (INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)) {
            /* fractional-second rounding will be dealt with below */
        } else if range == (INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)) {
            /* fractional-second rounding will be dealt with below */
        } else {
            /* unrecognized interval typmod: C does elog(ERROR) (XX000) */
            return Err(crate::timestamp::internal_error(format!(
                "unrecognized interval typmod: {typmod}"
            )));
        }

        /* Need to adjust sub-second precision? */
        if precision != INTERVAL_FULL_PRECISION {
            if !(0..=MAX_INTERVAL_PRECISION).contains(&precision) {
                return Err(crate::timestamp::invalid_parameter(format!(
                    "interval({precision}) precision must be between {} and {}",
                    0, MAX_INTERVAL_PRECISION
                )));
            }

            let p = precision as usize;
            if interval.time >= 0 {
                if pg_add_s64_overflow(interval.time, INTERVAL_OFFSETS[p], &mut interval.time) {
                    return Err(crate::timestamp::interval_out_of_range());
                }
                interval.time -= interval.time % INTERVAL_SCALES[p];
            } else {
                if pg_sub_s64_overflow(interval.time, INTERVAL_OFFSETS[p], &mut interval.time) {
                    return Err(crate::timestamp::interval_out_of_range());
                }
                interval.time -= interval.time % INTERVAL_SCALES[p];
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Comparisons (interval_cmp_value uses native i128)
// ---------------------------------------------------------------------------

/// `interval_cmp_value()` -- map an interval to its 128-bit linear microsecond
/// representation (days == 24h, months == 30 days).  (`utils/adt/timestamp.c`)
pub fn interval_cmp_value(interval: &Interval) -> i128 {
    /* Combine the month and day fields into an integral number of days. */
    let days: i64 = interval.month as i64 * 30 + interval.day as i64;

    /* Widen time field to 128 bits, then add days * USECS_PER_DAY. */
    interval.time as i128 + (days as i128) * (USECS_PER_DAY as i128)
}

/// `interval_cmp_internal()` -- three-way compare of two intervals.
///
/// (`utils/adt/timestamp.c`)
pub fn interval_cmp_internal(interval1: &Interval, interval2: &Interval) -> i32 {
    let span1 = interval_cmp_value(interval1);
    let span2 = interval_cmp_value(interval2);
    match span1.cmp(&span2) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

/// `interval_sign()` -- sign of an interval (-1, 0, +1).
///
/// (`utils/adt/timestamp.c`)
pub fn interval_sign(interval: &Interval) -> i32 {
    let span = interval_cmp_value(interval);
    match span.cmp(&0) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

/// `interval_eq` core.
pub fn interval_eq(i1: &Interval, i2: &Interval) -> bool {
    interval_cmp_internal(i1, i2) == 0
}
/// `interval_ne` core.
pub fn interval_ne(i1: &Interval, i2: &Interval) -> bool {
    interval_cmp_internal(i1, i2) != 0
}
/// `interval_lt` core.
pub fn interval_lt(i1: &Interval, i2: &Interval) -> bool {
    interval_cmp_internal(i1, i2) < 0
}
/// `interval_gt` core.
pub fn interval_gt(i1: &Interval, i2: &Interval) -> bool {
    interval_cmp_internal(i1, i2) > 0
}
/// `interval_le` core.
pub fn interval_le(i1: &Interval, i2: &Interval) -> bool {
    interval_cmp_internal(i1, i2) <= 0
}
/// `interval_ge` core.
pub fn interval_ge(i1: &Interval, i2: &Interval) -> bool {
    interval_cmp_internal(i1, i2) >= 0
}
/// `interval_cmp` core.
pub fn interval_cmp(i1: &Interval, i2: &Interval) -> i32 {
    interval_cmp_internal(i1, i2)
}

/// `interval_smaller` core.
pub fn interval_smaller(i1: Interval, i2: Interval) -> Interval {
    if interval_cmp_internal(&i1, &i2) < 0 {
        i1
    } else {
        i2
    }
}
/// `interval_larger` core.
pub fn interval_larger(i1: Interval, i2: Interval) -> Interval {
    if interval_cmp_internal(&i1, &i2) > 0 {
        i1
    } else {
        i2
    }
}

// ---------------------------------------------------------------------------
// justify_hours / justify_days / justify_interval
// ---------------------------------------------------------------------------

/// `interval_justify_hours()` core -- pull whole days out of the time field.
///
/// (`utils/adt/timestamp.c`)
pub fn interval_justify_hours(span: &Interval) -> DtResult<Interval> {
    let mut result = *span;

    if INTERVAL_NOT_FINITE(&result) {
        return Ok(result);
    }

    // TMODULO(result.time, wholeday, USECS_PER_DAY)
    let wholeday = result.time / USECS_PER_DAY;
    if wholeday != 0 {
        result.time -= wholeday * USECS_PER_DAY;
    }
    if pg_add_s32_overflow(result.day, wholeday as i32, &mut result.day) {
        return Err(crate::timestamp::interval_out_of_range());
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

/// `interval_justify_days()` core -- pull whole months out of the day field.
///
/// (`utils/adt/timestamp.c`)
pub fn interval_justify_days(span: &Interval) -> DtResult<Interval> {
    let mut result = *span;

    if INTERVAL_NOT_FINITE(&result) {
        return Ok(result);
    }

    let wholemonth = result.day / DAYS_PER_MONTH;
    result.day -= wholemonth * DAYS_PER_MONTH;
    if pg_add_s32_overflow(result.month, wholemonth, &mut result.month) {
        return Err(crate::timestamp::interval_out_of_range());
    }

    if result.month > 0 && result.day < 0 {
        result.day += DAYS_PER_MONTH;
        result.month -= 1;
    } else if result.month < 0 && result.day > 0 {
        result.day -= DAYS_PER_MONTH;
        result.month += 1;
    }

    Ok(result)
}

/// `interval_justify_interval()` core -- normalize so `0 <= |time| < 24h` and
/// `0 <= |day| < 30d`, and the signs of all three fields agree.
///
/// (`utils/adt/timestamp.c`)
pub fn interval_justify_interval(span: &Interval) -> DtResult<Interval> {
    let mut result = *span;

    if INTERVAL_NOT_FINITE(&result) {
        return Ok(result);
    }

    /* pre-justify days if it might prevent overflow */
    if (result.day > 0 && result.time > 0) || (result.day < 0 && result.time < 0) {
        let wholemonth = result.day / DAYS_PER_MONTH;
        result.day -= wholemonth * DAYS_PER_MONTH;
        if pg_add_s32_overflow(result.month, wholemonth, &mut result.month) {
            return Err(crate::timestamp::interval_out_of_range());
        }
    }

    // TMODULO(result.time, wholeday, USECS_PER_DAY)
    let wholeday = result.time / USECS_PER_DAY;
    if wholeday != 0 {
        result.time -= wholeday * USECS_PER_DAY;
    }
    result.day += wholeday as i32;

    let wholemonth = result.day / DAYS_PER_MONTH;
    result.day -= wholemonth * DAYS_PER_MONTH;
    if pg_add_s32_overflow(result.month, wholemonth, &mut result.month) {
        return Err(crate::timestamp::interval_out_of_range());
    }

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

// ---------------------------------------------------------------------------
// Arithmetic: unary minus, +, -, *, /
// ---------------------------------------------------------------------------

/// `interval_um_internal()` core -- unary negation of an interval.
///
/// (`utils/adt/timestamp.c`)
pub fn interval_um_internal(interval: &Interval, result: &mut Interval) -> DtResult<()> {
    if INTERVAL_IS_NOBEGIN(interval) {
        set_noend(result);
    } else if INTERVAL_IS_NOEND(interval) {
        set_nobegin(result);
    } else {
        let mut time = 0i64;
        let mut day = 0i32;
        let mut month = 0i32;
        if pg_sub_s64_overflow(0, interval.time, &mut time)
            || pg_sub_s32_overflow(0, interval.day, &mut day)
            || pg_sub_s32_overflow(0, interval.month, &mut month)
        {
            return Err(crate::timestamp::interval_out_of_range());
        }
        result.time = time;
        result.day = day;
        result.month = month;
        if INTERVAL_NOT_FINITE(result) {
            return Err(crate::timestamp::interval_out_of_range());
        }
    }
    Ok(())
}

/// `interval_um()` core -- unary negation, returning the result.
pub fn interval_um(interval: &Interval) -> DtResult<Interval> {
    let mut result = Interval {
        time: 0,
        day: 0,
        month: 0,
    };
    interval_um_internal(interval, &mut result)?;
    Ok(result)
}

#[inline]
fn pg_sub_s32_overflow(a: i32, b: i32, res: &mut i32) -> bool {
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

fn finite_interval_pl(span1: &Interval, span2: &Interval, result: &mut Interval) -> DtResult<()> {
    if pg_add_s32_overflow(span1.month, span2.month, &mut result.month)
        || pg_add_s32_overflow(span1.day, span2.day, &mut result.day)
        || pg_add_s64_overflow(span1.time, span2.time, &mut result.time)
        || INTERVAL_NOT_FINITE(result)
    {
        return Err(crate::timestamp::interval_out_of_range());
    }
    Ok(())
}

fn finite_interval_mi(span1: &Interval, span2: &Interval, result: &mut Interval) -> DtResult<()> {
    if pg_sub_s32_overflow(span1.month, span2.month, &mut result.month)
        || pg_sub_s32_overflow(span1.day, span2.day, &mut result.day)
        || pg_sub_s64_overflow(span1.time, span2.time, &mut result.time)
        || INTERVAL_NOT_FINITE(result)
    {
        return Err(crate::timestamp::interval_out_of_range());
    }
    Ok(())
}

/// `interval_pl()` core -- add two intervals.  (`utils/adt/timestamp.c`)
pub fn interval_pl(span1: &Interval, span2: &Interval) -> DtResult<Interval> {
    let mut result = Interval {
        time: 0,
        day: 0,
        month: 0,
    };

    if INTERVAL_IS_NOBEGIN(span1) {
        if INTERVAL_IS_NOEND(span2) {
            return Err(crate::timestamp::interval_out_of_range());
        }
        set_nobegin(&mut result);
    } else if INTERVAL_IS_NOEND(span1) {
        if INTERVAL_IS_NOBEGIN(span2) {
            return Err(crate::timestamp::interval_out_of_range());
        }
        set_noend(&mut result);
    } else if INTERVAL_NOT_FINITE(span2) {
        result = *span2;
    } else {
        finite_interval_pl(span1, span2, &mut result)?;
    }

    Ok(result)
}

/// `interval_mi()` core -- subtract two intervals.  (`utils/adt/timestamp.c`)
pub fn interval_mi(span1: &Interval, span2: &Interval) -> DtResult<Interval> {
    let mut result = Interval {
        time: 0,
        day: 0,
        month: 0,
    };

    if INTERVAL_IS_NOBEGIN(span1) {
        if INTERVAL_IS_NOBEGIN(span2) {
            return Err(crate::timestamp::interval_out_of_range());
        }
        set_nobegin(&mut result);
    } else if INTERVAL_IS_NOEND(span1) {
        if INTERVAL_IS_NOEND(span2) {
            return Err(crate::timestamp::interval_out_of_range());
        }
        set_noend(&mut result);
    } else if INTERVAL_IS_NOBEGIN(span2) {
        set_noend(&mut result);
    } else if INTERVAL_IS_NOEND(span2) {
        set_nobegin(&mut result);
    } else {
        finite_interval_mi(span1, span2, &mut result)?;
    }

    Ok(result)
}

/// `TSROUND(j)` (datatype/timestamp.h): `rint(j * 1e6) / 1e6`.
///
/// C's `rint()` rounds half-to-even, so use `round_ties_even()` rather than
/// `f64::round` (which rounds half away from zero).
#[inline]
fn ts_round(j: f64) -> f64 {
    const TS_PREC_INV: f64 = 1_000_000.0;
    (j * TS_PREC_INV).round_ties_even() / TS_PREC_INV
}

#[inline]
fn float8_fits_in_int32(num: f64) -> bool {
    num >= i32::MIN as f64 && num < -(i32::MIN as f64)
}

#[inline]
fn float8_fits_in_int64(num: f64) -> bool {
    num >= i64::MIN as f64 && num < -(i64::MIN as f64)
}

/// `interval_mul()` core -- multiply an interval by a `float8` factor.
///
/// (`utils/adt/timestamp.c`)
pub fn interval_mul(span: &Interval, factor: f64) -> DtResult<Interval> {
    use ::types_datetime::SECS_PER_DAY;

    let mut result = Interval {
        time: 0,
        day: 0,
        month: 0,
    };
    let orig_month = span.month;
    let orig_day = span.day;

    if factor.is_nan() {
        return Err(crate::timestamp::interval_out_of_range());
    }

    if INTERVAL_NOT_FINITE(span) {
        if factor == 0.0 {
            return Err(crate::timestamp::interval_out_of_range());
        }
        if factor < 0.0 {
            interval_um_internal(span, &mut result)?;
        } else {
            result = *span;
        }
        return Ok(result);
    }
    if factor.is_infinite() {
        let isign = interval_sign(span);
        if isign == 0 {
            return Err(crate::timestamp::interval_out_of_range());
        }
        if factor * (isign as f64) < 0.0 {
            set_nobegin(&mut result);
        } else {
            set_noend(&mut result);
        }
        return Ok(result);
    }

    let mut result_double = span.month as f64 * factor;
    if result_double.is_nan() || !float8_fits_in_int32(result_double) {
        return Err(crate::timestamp::interval_out_of_range());
    }
    result.month = result_double as i32;

    result_double = span.day as f64 * factor;
    if result_double.is_nan() || !float8_fits_in_int32(result_double) {
        return Err(crate::timestamp::interval_out_of_range());
    }
    result.day = result_double as i32;

    let mut month_remainder_days =
        (orig_month as f64 * factor - result.month as f64) * DAYS_PER_MONTH as f64;
    month_remainder_days = ts_round(month_remainder_days);
    let mut sec_remainder = (orig_day as f64 * factor - result.day as f64 + month_remainder_days
        - month_remainder_days as i32 as f64)
        * SECS_PER_DAY as f64;
    sec_remainder = ts_round(sec_remainder);

    if sec_remainder.abs() >= SECS_PER_DAY as f64 {
        if pg_add_s32_overflow(
            result.day,
            (sec_remainder / SECS_PER_DAY as f64) as i32,
            &mut result.day,
        ) {
            return Err(crate::timestamp::interval_out_of_range());
        }
        sec_remainder -= (sec_remainder / SECS_PER_DAY as f64) as i32 as f64 * SECS_PER_DAY as f64;
    }

    if pg_add_s32_overflow(result.day, month_remainder_days as i32, &mut result.day) {
        return Err(crate::timestamp::interval_out_of_range());
    }
    // C uses rint() (round half-to-even), not round-half-away-from-zero.
    result_double =
        (span.time as f64 * factor + sec_remainder * USECS_PER_SEC as f64).round_ties_even();
    if result_double.is_nan() || !float8_fits_in_int64(result_double) {
        return Err(crate::timestamp::interval_out_of_range());
    }
    result.time = result_double as i64;

    if INTERVAL_NOT_FINITE(&result) {
        return Err(crate::timestamp::interval_out_of_range());
    }

    Ok(result)
}

/// `interval_div()` core -- divide an interval by a `float8` factor.
///
/// (`utils/adt/timestamp.c`)
pub fn interval_div(span: &Interval, factor: f64) -> DtResult<Interval> {
    use ::types_datetime::SECS_PER_DAY;
    use ::types_error::ERRCODE_DIVISION_BY_ZERO;

    let mut result = Interval {
        time: 0,
        day: 0,
        month: 0,
    };
    let orig_month = span.month;
    let orig_day = span.day;

    if factor == 0.0 {
        return Err(PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO));
    }

    if factor.is_nan() {
        return Err(crate::timestamp::interval_out_of_range());
    }

    if INTERVAL_NOT_FINITE(span) {
        if factor.is_infinite() {
            return Err(crate::timestamp::interval_out_of_range());
        }
        if factor < 0.0 {
            interval_um_internal(span, &mut result)?;
        } else {
            result = *span;
        }
        return Ok(result);
    }

    let mut result_double = span.month as f64 / factor;
    if result_double.is_nan() || !float8_fits_in_int32(result_double) {
        return Err(crate::timestamp::interval_out_of_range());
    }
    result.month = result_double as i32;

    result_double = span.day as f64 / factor;
    if result_double.is_nan() || !float8_fits_in_int32(result_double) {
        return Err(crate::timestamp::interval_out_of_range());
    }
    result.day = result_double as i32;

    let mut month_remainder_days =
        (orig_month as f64 / factor - result.month as f64) * DAYS_PER_MONTH as f64;
    month_remainder_days = ts_round(month_remainder_days);
    let mut sec_remainder = (orig_day as f64 / factor - result.day as f64 + month_remainder_days
        - month_remainder_days as i32 as f64)
        * SECS_PER_DAY as f64;
    sec_remainder = ts_round(sec_remainder);
    if sec_remainder.abs() >= SECS_PER_DAY as f64 {
        if pg_add_s32_overflow(
            result.day,
            (sec_remainder / SECS_PER_DAY as f64) as i32,
            &mut result.day,
        ) {
            return Err(crate::timestamp::interval_out_of_range());
        }
        sec_remainder -= (sec_remainder / SECS_PER_DAY as f64) as i32 as f64 * SECS_PER_DAY as f64;
    }

    if pg_add_s32_overflow(result.day, month_remainder_days as i32, &mut result.day) {
        return Err(crate::timestamp::interval_out_of_range());
    }
    // C uses rint() (round half-to-even), not round-half-away-from-zero.
    result_double =
        (span.time as f64 / factor + sec_remainder * USECS_PER_SEC as f64).round_ties_even();
    if result_double.is_nan() || !float8_fits_in_int64(result_double) {
        return Err(crate::timestamp::interval_out_of_range());
    }
    result.time = result_double as i64;

    if INTERVAL_NOT_FINITE(&result) {
        return Err(crate::timestamp::interval_out_of_range());
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// avg(interval) / sum(interval) aggregate transition state
// (utils/adt/timestamp.c: IntervalAggState + do_interval_accum/discard,
//  interval_avg_combine/serialize/deserialize/avg/sum)
// ---------------------------------------------------------------------------

/// `IntervalAggState` — the `internal` transition value for sum()/avg(interval).
///
/// Infinite inputs are tallied separately and are *not* counted in `N`; use
/// [`IntervalAggState::total_count`] (C `IA_TOTAL_COUNT`) when the combined
/// count is needed.
#[derive(Clone, Copy, Debug, Default)]
pub struct IntervalAggState {
    /// count of finite intervals processed
    pub n: i64,
    /// sum of finite intervals processed
    pub sum_x: Interval,
    /// count of +infinity intervals
    pub p_inf_count: i64,
    /// count of -infinity intervals
    pub n_inf_count: i64,
}

impl IntervalAggState {
    /// `IA_TOTAL_COUNT(state)` — finite plus infinite inputs.
    #[inline]
    pub fn total_count(&self) -> i64 {
        self.n + self.p_inf_count + self.n_inf_count
    }
}

/// `do_interval_accum()` — accumulate a new input value.
pub fn do_interval_accum(state: &mut IntervalAggState, newval: &Interval) -> DtResult<()> {
    // Infinite inputs are counted separately, and do not affect "N".
    if INTERVAL_IS_NOBEGIN(newval) {
        state.n_inf_count += 1;
        return Ok(());
    }
    if INTERVAL_IS_NOEND(newval) {
        state.p_inf_count += 1;
        return Ok(());
    }

    let mut sum = state.sum_x;
    finite_interval_pl(&state.sum_x, newval, &mut sum)?;
    state.sum_x = sum;
    state.n += 1;
    Ok(())
}

/// `do_interval_discard()` — remove the given interval value from the state
/// (inverse transition for moving-window aggregation).
pub fn do_interval_discard(state: &mut IntervalAggState, newval: &Interval) -> DtResult<()> {
    // Infinite inputs are counted separately, and do not affect "N".
    if INTERVAL_IS_NOBEGIN(newval) {
        state.n_inf_count -= 1;
        return Ok(());
    }
    if INTERVAL_IS_NOEND(newval) {
        state.p_inf_count -= 1;
        return Ok(());
    }

    // Handle the to-be-discarded finite value.
    state.n -= 1;
    if state.n > 0 {
        let mut diff = state.sum_x;
        finite_interval_mi(&state.sum_x, newval, &mut diff)?;
        state.sum_x = diff;
    } else {
        // All values discarded, reset the state.
        debug_assert_eq!(state.n, 0);
        state.sum_x = Interval::default();
    }
    Ok(())
}

/// `interval_avg_combine()` core — combine `state2` into `state1`, returning the
/// merged state (C places the combination in the first argument).
pub fn interval_avg_combine(
    state1: Option<IntervalAggState>,
    state2: Option<IntervalAggState>,
) -> DtResult<Option<IntervalAggState>> {
    let state2 = match state2 {
        None => return Ok(state1),
        Some(s) => s,
    };

    let mut state1 = match state1 {
        // manually copy all fields from state2 to state1
        None => return Ok(Some(state2)),
        Some(s) => s,
    };

    state1.n += state2.n;
    state1.p_inf_count += state2.p_inf_count;
    state1.n_inf_count += state2.n_inf_count;

    // Accumulate finite interval values, if any.
    if state2.n > 0 {
        let mut sum = state1.sum_x;
        finite_interval_pl(&state1.sum_x, &state2.sum_x, &mut sum)?;
        state1.sum_x = sum;
    }

    Ok(Some(state1))
}

/// `interval_avg_serialize()` core — serialize the state to wire bytes (the
/// `bytea` payload `pq_endtypsend` produces, minus the varlena header).
pub fn interval_avg_serialize(state: &IntervalAggState) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + 8 + 4 + 4 + 8 + 8);
    // N
    buf.extend_from_slice(&state.n.to_be_bytes());
    // sumX
    buf.extend_from_slice(&state.sum_x.time.to_be_bytes());
    buf.extend_from_slice(&state.sum_x.day.to_be_bytes());
    buf.extend_from_slice(&state.sum_x.month.to_be_bytes());
    // pInfcount
    buf.extend_from_slice(&state.p_inf_count.to_be_bytes());
    // nInfcount
    buf.extend_from_slice(&state.n_inf_count.to_be_bytes());
    buf
}

/// `interval_avg_deserialize()` core — read the state back from wire bytes.
pub fn interval_avg_deserialize(buf: &mut crate::binio::WireReader<'_>) -> DtResult<IntervalAggState> {
    let n = buf.get_i64()?;
    let time = buf.get_i64()?;
    let day = buf.get_i32()?;
    let month = buf.get_i32()?;
    let p_inf_count = buf.get_i64()?;
    let n_inf_count = buf.get_i64()?;
    Ok(IntervalAggState {
        n,
        sum_x: Interval { time, day, month },
        p_inf_count,
        n_inf_count,
    })
}

/// `interval_avg()` final function core — `avg(interval)`. Returns `None` for
/// the SQL NULL produced when there were no non-null inputs.
pub fn interval_avg(state: Option<&IntervalAggState>) -> DtResult<Option<Interval>> {
    let state = match state {
        Some(s) if s.total_count() != 0 => s,
        _ => return Ok(None),
    };

    // Aggregating infinities that all have the same sign produces infinity with
    // that sign; differing signs is an error.
    if state.p_inf_count > 0 || state.n_inf_count > 0 {
        if state.p_inf_count > 0 && state.n_inf_count > 0 {
            return Err(crate::timestamp::interval_out_of_range());
        }
        let mut result = Interval::default();
        if state.p_inf_count > 0 {
            set_noend(&mut result);
        } else {
            set_nobegin(&mut result);
        }
        return Ok(Some(result));
    }

    Ok(Some(interval_div(&state.sum_x, state.n as f64)?))
}

/// `interval_sum()` final function core — `sum(interval)`. Returns `None` for
/// the SQL NULL produced when there were no non-null inputs.
pub fn interval_sum(state: Option<&IntervalAggState>) -> DtResult<Option<Interval>> {
    let state = match state {
        Some(s) if s.total_count() != 0 => s,
        _ => return Ok(None),
    };

    // Differing-sign infinities are an error.
    if state.p_inf_count > 0 && state.n_inf_count > 0 {
        return Err(crate::timestamp::interval_out_of_range());
    }

    let mut result = Interval::default();
    if state.p_inf_count > 0 {
        set_noend(&mut result);
    } else if state.n_inf_count > 0 {
        set_nobegin(&mut result);
    } else {
        result = state.sum_x;
    }
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zero_interval() -> Interval {
        Interval {
            time: 0,
            day: 0,
            month: 0,
        }
    }

    #[test]
    fn interval_in_out_round_trip() {
        let i = interval_in("1 year 2 months 3 days 04:05:06", -1).unwrap();
        assert_eq!(interval_out(&i), "1 year 2 mons 3 days 04:05:06");
    }

    #[test]
    fn interval_in_out_fractional_seconds() {
        let i = interval_in("04:05:06.5", -1).unwrap();
        assert_eq!(interval_out(&i), "04:05:06.5");
    }

    #[test]
    fn interval2itm_round_trip() {
        // 1y 2mon 3d 4h5m6.5s
        let mut span = zero_interval();
        span.month = 14; // 1y 2mon
        span.day = 3;
        span.time = 4 * USECS_PER_HOUR + 5 * USECS_PER_MINUTE + 6 * USECS_PER_SEC + 500_000;
        let mut itm = pg_itm {
            tm_usec: 0,
            tm_sec: 0,
            tm_min: 0,
            tm_hour: 0,
            tm_mday: 0,
            tm_mon: 0,
            tm_year: 0,
        };
        interval2itm(span, &mut itm);
        assert_eq!(itm.tm_year, 1);
        assert_eq!(itm.tm_mon, 2);
        assert_eq!(itm.tm_mday, 3);
        assert_eq!(itm.tm_hour, 4);
        assert_eq!(itm.tm_min, 5);
        assert_eq!(itm.tm_sec, 6);
        assert_eq!(itm.tm_usec, 500_000);

        let mut back = zero_interval();
        itm2interval(&itm, &mut back).unwrap();
        assert_eq!(back, span);
    }

    #[test]
    fn justify_hours_pulls_days() {
        // 36 hours -> 1 day 12:00:00
        let mut span = zero_interval();
        span.time = 36 * USECS_PER_HOUR;
        let r = interval_justify_hours(&span).unwrap();
        assert_eq!(r.day, 1);
        assert_eq!(r.time, 12 * USECS_PER_HOUR);
    }

    #[test]
    fn justify_days_pulls_months() {
        // 35 days -> 1 month 5 days
        let mut span = zero_interval();
        span.day = 35;
        let r = interval_justify_days(&span).unwrap();
        assert_eq!(r.month, 1);
        assert_eq!(r.day, 5);
    }

    #[test]
    fn interval_cmp_orders_by_linear_value() {
        // 1 day vs 24 hours compare equal (days == 24h in comparison).
        let mut a = zero_interval();
        a.day = 1;
        let mut b = zero_interval();
        b.time = 24 * USECS_PER_HOUR;
        assert_eq!(interval_cmp(&a, &b), 0);

        // 1 month (== 30 days) > 29 days.
        let mut m = zero_interval();
        m.month = 1;
        let mut d = zero_interval();
        d.day = 29;
        assert_eq!(interval_cmp(&m, &d), 1);
    }

    #[test]
    fn interval_um_negates() {
        let mut span = zero_interval();
        span.month = 5;
        span.day = -3;
        span.time = 1_000_000;
        let r = interval_um(&span).unwrap();
        assert_eq!(r.month, -5);
        assert_eq!(r.day, 3);
        assert_eq!(r.time, -1_000_000);
    }

    #[test]
    fn interval_pl_mi_basic() {
        let a = interval_in("1 day 02:00:00", -1).unwrap();
        let b = interval_in("12:00:00", -1).unwrap();
        let sum = interval_pl(&a, &b).unwrap();
        assert_eq!(interval_out(&sum), "1 day 14:00:00");
        let diff = interval_mi(&a, &b).unwrap();
        assert_eq!(interval_out(&diff), "1 day -10:00:00");
    }

    #[test]
    fn interval_mul_div() {
        let a = interval_in("1 hour", -1).unwrap();
        let doubled = interval_mul(&a, 2.0).unwrap();
        assert_eq!(doubled.time, 2 * USECS_PER_HOUR);
        let halved = interval_div(&a, 2.0).unwrap();
        assert_eq!(halved.time, USECS_PER_HOUR / 2);
    }

    // Regression: the final interval_mul time uses rint() (round half-to-even),
    // matching C, not round-half-away-from-zero.  '0.000001' (1 us) * 0.5 yields
    // an exact 0.5 us product, which rint() rounds to 0 (ties to even), so the
    // result is 00:00:00 -- not 00:00:00.000001.
    #[test]
    fn interval_mul_rounds_half_to_even() {
        let a = interval_in("0.000001", -1).unwrap();
        assert_eq!(a.time, 1);
        let r = interval_mul(&a, 0.5).unwrap();
        assert_eq!(r.time, 0);
        assert_eq!(interval_out(&r), "00:00:00");

        // 3 us * 0.5 = 1.5 -> rint rounds to even -> 2 us.
        let b = interval_in("0.000003", -1).unwrap();
        assert_eq!(b.time, 3);
        let r = interval_mul(&b, 0.5).unwrap();
        assert_eq!(r.time, 2);
    }

    // Regression: the final interval_div time uses rint() (round half-to-even).
    // 1 us / 2 = 0.5 -> rint -> 0.
    #[test]
    fn interval_div_rounds_half_to_even() {
        let a = interval_in("0.000001", -1).unwrap();
        assert_eq!(a.time, 1);
        let r = interval_div(&a, 2.0).unwrap();
        assert_eq!(r.time, 0);
        assert_eq!(interval_out(&r), "00:00:00");
    }

    // Regression: ts_round() (TSROUND) uses rint() half-to-even.
    #[test]
    fn ts_round_is_half_to_even() {
        assert_eq!(ts_round(0.5e-6), 0.0); // 0.5 us -> 0 (even)
        assert_eq!(ts_round(1.5e-6), 2.0e-6); // 1.5 us -> 2 us (even)
        assert_eq!(ts_round(2.5e-6), 2.0e-6); // 2.5 us -> 2 us (even)
    }
}
