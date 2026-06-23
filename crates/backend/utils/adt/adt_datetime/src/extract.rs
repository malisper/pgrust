//! EXTRACT / date_part / date_trunc cores for timestamp / timestamptz /
//! interval, ported from `src/backend/utils/adt/timestamp.c` (idiomatic, safe
//! Rust).
//!
//! These are the `timestamp_part_common` / `timestamptz_part_common` /
//! `interval_part_common` cores (the shared `date_part` float8 path PLUS the
//! `retnumeric` path used by SQL `EXTRACT`, returning a
//! [`::adt_numeric::NumericVar`]), and the `*_trunc` cores.
//!
//! Fmgr `Datum` shims are NOT ported.  The `units` string is decoded with the
//! shared decode engine ([`crate::decode::DecodeUnits`] /
//! [`crate::decode::DecodeSpecial`]); the caller passes the already-downcased
//! unit string.
//!
//! Idiomatic surface: plain `i32`/`i64`/`f64`, owned values, `Result`, `&str`.
//! No raw pointers, `extern "C"`, `c_int`, `libc`, or `pg_ffi_fgram`.

use ::pgtime::pg_tm;
use ::mcx::Mcx;
use ::adt_numeric::kernel_transcendental::int64_to_numericvar;
use ::adt_numeric::kernel_var::{add_var, div_var, round_var, select_div_scale, sub_var};
use ::types_numeric::var::NumericVar;
use types_datetime::{
    pg_itm, Interval, DAYS_PER_MONTH, DTK_CENTURY, DTK_DAY, DTK_DECADE, DTK_DOW, DTK_DOY, DTK_EPOCH,
    DTK_HOUR, DTK_ISODOW, DTK_ISOYEAR, DTK_JULIAN, DTK_MICROSEC, DTK_MILLENNIUM, DTK_MILLISEC,
    DTK_MINUTE, DTK_MONTH, DTK_QUARTER, DTK_SECOND, DTK_TZ, DTK_TZ_HOUR, DTK_TZ_MINUTE, DTK_WEEK,
    DTK_YEAR, MINS_PER_HOUR, MONTHS_PER_YEAR, RESERV, SECS_PER_DAY, SECS_PER_HOUR, SECS_PER_MINUTE,
    UNITS, UNKNOWN_FIELD,
};
use types_datetime::{fsec_t, Timestamp, TimestampTz};
use types_error::{PgError, PgResult};

use crate::calendar::{date2j, j2day};
use crate::decode::{DecodeSpecial, DecodeUnits};
use crate::interval::{interval2itm, itm2interval, INTERVAL_IS_NOBEGIN, INTERVAL_NOT_FINITE};
use crate::isoweek::{date2isoweek, date2isoyear, isoweek2date};
use crate::numeric_helpers::int64_div_fast_to_numericvar;
use crate::timestamp::{
    pg_add_s64_overflow, pg_mul_s64_overflow, timestamp2tm, tm2timestamp, DtResult,
    SetEpochTimestamp, TIMESTAMP_IS_NOBEGIN, TIMESTAMP_NOT_FINITE,
};

/// The float8/numeric `EXTRACT` result.  `Null` mirrors the C `PG_RETURN_NULL()`
/// path (oscillating field of an infinite input).
#[derive(Clone, Debug)]
pub enum ExtractResult<'mcx> {
    /// `date_part(...)` float8 result.
    Float8(f64),
    /// SQL `EXTRACT(...)` numeric result.
    Numeric(NumericVar<'mcx>),
    /// SQL NULL (oscillating field of infinity).
    Null,
}

/// Wrap an integer extract result into the requested representation.
fn int_result<'mcx>(mcx: Mcx<'mcx>, intresult: i64, retnumeric: bool) -> PgResult<ExtractResult<'mcx>> {
    if retnumeric {
        Ok(ExtractResult::Numeric(int64_to_numericvar(mcx, intresult)?))
    } else {
        Ok(ExtractResult::Float8(intresult as f64))
    }
}

// ---------------------------------------------------------------------------
// Type labels (C: format_type_be(...)) used in the date_part/EXTRACT errors.
// ---------------------------------------------------------------------------

/// `format_type_be(TIMESTAMPOID)`.
const TYPE_TIMESTAMP: &str = "timestamp without time zone";
/// `format_type_be(TIMESTAMPTZOID)`.
const TYPE_TIMESTAMPTZ: &str = "timestamp with time zone";
/// `format_type_be(INTERVALOID)`.
const TYPE_INTERVAL: &str = "interval";

/// Pick the timestamp type label based on `is_tz`.
fn ts_type_name(is_tz: bool) -> &'static str {
    if is_tz {
        TYPE_TIMESTAMPTZ
    } else {
        TYPE_TIMESTAMP
    }
}

/// `errmsg("unit \"%s\" not recognized for type %s", ...)` with
/// ERRCODE_INVALID_PARAMETER_VALUE.
fn unit_not_recognized(lowunits: &str, type_name: &str) -> PgError {
    crate::timestamp::invalid_parameter(format!(
        "unit \"{lowunits}\" not recognized for type {type_name}"
    ))
}

/// `errmsg("unit \"%s\" not supported for type %s", ...)` with
/// ERRCODE_FEATURE_NOT_SUPPORTED.
fn unit_not_supported(lowunits: &str, type_name: &str) -> PgError {
    crate::timestamp::feature_not_supported(format!(
        "unit \"{lowunits}\" not supported for type {type_name}"
    ))
}

/// `interval_trunc`'s unsupported-unit error: like [`unit_not_supported`] for
/// the interval type, but with the C `(val == DTK_WEEK)` errdetail attached.
fn interval_trunc_unsupported(lowunits: &str, val: i32) -> PgError {
    let err = unit_not_supported(lowunits, TYPE_INTERVAL);
    if val == DTK_WEEK {
        err.with_detail("Months usually have fractional weeks.")
    } else {
        err
    }
}

// ---------------------------------------------------------------------------
// NonFinite*Part helpers.
// ---------------------------------------------------------------------------

/// `NonFiniteTimestampTzPart()` -- result for an infinite timestamp[tz].  An
/// `Ok(0.0)` ultimately means "return NULL".  (`utils/adt/timestamp.c`)
fn non_finite_timestamptz_part(
    type_: i32,
    unit: i32,
    lowunits: &str,
    is_negative: bool,
    is_tz: bool,
) -> DtResult<f64> {
    let type_name = ts_type_name(is_tz);

    if type_ != UNITS && type_ != RESERV {
        return Err(unit_not_recognized(lowunits, type_name));
    }

    match unit {
        // Oscillating units
        DTK_MICROSEC | DTK_MILLISEC | DTK_SECOND | DTK_MINUTE | DTK_HOUR | DTK_DAY | DTK_MONTH
        | DTK_QUARTER | DTK_WEEK | DTK_DOW | DTK_ISODOW | DTK_DOY | DTK_TZ | DTK_TZ_MINUTE
        | DTK_TZ_HOUR => Ok(0.0),

        // Monotonically-increasing units
        DTK_YEAR | DTK_DECADE | DTK_CENTURY | DTK_MILLENNIUM | DTK_JULIAN | DTK_ISOYEAR
        | DTK_EPOCH => {
            if is_negative {
                Ok(f64::NEG_INFINITY)
            } else {
                Ok(f64::INFINITY)
            }
        }

        _ => Err(unit_not_supported(lowunits, type_name)),
    }
}

/// `NonFiniteIntervalPart()` -- result for an infinite interval.
/// (`utils/adt/timestamp.c`)
fn non_finite_interval_part(
    type_: i32,
    unit: i32,
    lowunits: &str,
    is_negative: bool,
) -> DtResult<f64> {
    if type_ != UNITS && type_ != RESERV {
        return Err(unit_not_recognized(lowunits, TYPE_INTERVAL));
    }

    match unit {
        // Oscillating units
        DTK_MICROSEC | DTK_MILLISEC | DTK_SECOND | DTK_MINUTE | DTK_WEEK | DTK_MONTH
        | DTK_QUARTER => Ok(0.0),

        // Monotonically-increasing units
        DTK_HOUR | DTK_DAY | DTK_YEAR | DTK_DECADE | DTK_CENTURY | DTK_MILLENNIUM | DTK_EPOCH => {
            if is_negative {
                Ok(f64::NEG_INFINITY)
            } else {
                Ok(f64::INFINITY)
            }
        }

        _ => Err(unit_not_supported(lowunits, TYPE_INTERVAL)),
    }
}

/// Convert the infinite-input float8 result to the requested representation,
/// matching the C `retnumeric` handling (`±Infinity`/NULL).
fn non_finite_result<'mcx>(mcx: Mcx<'mcx>, r: f64, retnumeric: bool) -> ExtractResult<'mcx> {
    use ::types_numeric::var::NumericSign;
    if r == 0.0 {
        ExtractResult::Null
    } else if retnumeric {
        if r < 0.0 {
            ExtractResult::Numeric(NumericVar::special(mcx, NumericSign::NInf))
        } else {
            ExtractResult::Numeric(NumericVar::special(mcx, NumericSign::PInf))
        }
    } else {
        ExtractResult::Float8(r)
    }
}

// ---------------------------------------------------------------------------
// timestamp_part_common
// ---------------------------------------------------------------------------

/// Shared `timestamp_part_common` / `timestamptz_part_common` core.
///
/// `is_tz` selects the timestamptz behavior (rotates to local time, supports the
/// DTK_TZ* units).  `retnumeric` selects the SQL-`EXTRACT` numeric path.
///
/// (`utils/adt/timestamp.c`)
fn timestamp_part_common<'mcx>(
    mcx: Mcx<'mcx>,
    timestamp: Timestamp,
    lowunits: &str,
    retnumeric: bool,
    is_tz: bool,
) -> DtResult<ExtractResult<'mcx>> {
    let mut val: i32 = 0;
    let mut type_ = DecodeUnits(0, lowunits, &mut val);
    if type_ == UNKNOWN_FIELD {
        type_ = DecodeSpecial(0, lowunits, &mut val);
    }

    if TIMESTAMP_NOT_FINITE(timestamp) {
        let r = non_finite_timestamptz_part(
            type_,
            val,
            lowunits,
            TIMESTAMP_IS_NOBEGIN(timestamp),
            is_tz,
        )?;
        return Ok(non_finite_result(mcx, r, retnumeric));
    }

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;

    if type_ == UNITS {
        let r = if is_tz {
            timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, None)
        } else {
            timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None)
        };
        if r.is_err() {
            return Err(crate::timestamp::timestamp_out_of_range());
        }

        let intresult: i64 = match val {
            // timestamptz-only TZ units.
            DTK_TZ if is_tz => -(tz as i64),
            DTK_TZ_MINUTE if is_tz => (-tz / SECS_PER_MINUTE) as i64 % MINS_PER_HOUR as i64,
            DTK_TZ_HOUR if is_tz => (-tz / SECS_PER_HOUR) as i64,

            DTK_MICROSEC => tm.tm_sec as i64 * 1_000_000 + fsec as i64,

            DTK_MILLISEC => {
                return Ok(if retnumeric {
                    ExtractResult::Numeric(int64_div_fast_to_numericvar(
                        mcx,
                        tm.tm_sec as i64 * 1_000_000 + fsec as i64,
                        3,
                    )?)
                } else {
                    ExtractResult::Float8(tm.tm_sec as f64 * 1000.0 + fsec as f64 / 1000.0)
                });
            }

            DTK_SECOND => {
                return Ok(if retnumeric {
                    ExtractResult::Numeric(int64_div_fast_to_numericvar(
                        mcx,
                        tm.tm_sec as i64 * 1_000_000 + fsec as i64,
                        6,
                    )?)
                } else {
                    ExtractResult::Float8(tm.tm_sec as f64 + fsec as f64 / 1_000_000.0)
                });
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
                    (tm.tm_year - 1) as i64
                }
            }

            DTK_DECADE => {
                if tm.tm_year >= 0 {
                    (tm.tm_year / 10) as i64
                } else {
                    -(((8 - (tm.tm_year - 1)) / 10) as i64)
                }
            }

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

            DTK_JULIAN => {
                let day = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday);
                let frac_num = ((((tm.tm_hour as i64 * MINS_PER_HOUR as i64) + tm.tm_min as i64)
                    * SECS_PER_MINUTE as i64)
                    + tm.tm_sec as i64)
                    * 1_000_000
                    + fsec as i64;
                return Ok(if retnumeric {
                    let base = int64_to_numericvar(mcx, day as i64)?;
                    let num = int64_to_numericvar(mcx, frac_num)?;
                    let den = int64_to_numericvar(mcx, SECS_PER_DAY as i64 * 1_000_000)?;
                    let rscale = select_div_scale(&num, &den);
                    let q = div_var(mcx, &num, &den, rscale, true, false)?;
                    ExtractResult::Numeric(add_var(mcx, &base, &q)?)
                } else {
                    ExtractResult::Float8(
                        day as f64
                            + ((((tm.tm_hour as i64 * MINS_PER_HOUR as i64) + tm.tm_min as i64)
                                * SECS_PER_MINUTE as i64
                                + tm.tm_sec as i64) as f64
                                + (fsec as f64 / 1_000_000.0))
                                / SECS_PER_DAY as f64,
                    )
                });
            }

            DTK_ISOYEAR => {
                let mut r = date2isoyear(tm.tm_year, tm.tm_mon, tm.tm_mday) as i64;
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

            // C: DTK_TZ / DTK_TZ_MINUTE / DTK_TZ_HOUR (when !is_tz) plus any
            // other UNITS value fall through to the unsupported default.
            _ => {
                return Err(unit_not_supported(lowunits, ts_type_name(is_tz)));
            }
        };

        int_result(mcx, intresult, retnumeric)
    } else if type_ == RESERV && val == DTK_EPOCH {
        let epoch = SetEpochTimestamp();
        if retnumeric {
            let result = if (timestamp as i128) < (i64::MAX as i128 + epoch as i128) {
                int64_div_fast_to_numericvar(mcx, timestamp - epoch, 6)?
            } else {
                let sub = sub_var(
                    mcx,
                    &int64_to_numericvar(mcx, timestamp)?,
                    &int64_to_numericvar(mcx, epoch)?,
                )?;
                let den = int64_to_numericvar(mcx, 1_000_000)?;
                let rscale = select_div_scale(&sub, &den);
                let mut q = div_var(mcx, &sub, &den, rscale, true, false)?;
                round_var(&mut q, 6);
                q
            };
            Ok(ExtractResult::Numeric(result))
        } else {
            let result = if (timestamp as i128) < (i64::MAX as i128 + epoch as i128) {
                (timestamp - epoch) as f64 / 1_000_000.0
            } else {
                (timestamp as f64 - epoch as f64) / 1_000_000.0
            };
            Ok(ExtractResult::Float8(result))
        }
    } else if type_ == RESERV {
        // C: any RESERV unit other than DTK_EPOCH hits the switch default.
        Err(unit_not_supported(lowunits, ts_type_name(is_tz)))
    } else {
        Err(unit_not_recognized(lowunits, ts_type_name(is_tz)))
    }
}

/// `timestamp_part()` / `extract_timestamp()` core.  (`utils/adt/timestamp.c`)
pub fn timestamp_part<'mcx>(
    mcx: Mcx<'mcx>,
    timestamp: Timestamp,
    lowunits: &str,
    retnumeric: bool,
) -> DtResult<ExtractResult<'mcx>> {
    timestamp_part_common(mcx, timestamp, lowunits, retnumeric, false)
}

/// `timestamptz_part()` / `extract_timestamptz()` core.  (`utils/adt/timestamp.c`)
pub fn timestamptz_part<'mcx>(
    mcx: Mcx<'mcx>,
    timestamp: TimestampTz,
    lowunits: &str,
    retnumeric: bool,
) -> DtResult<ExtractResult<'mcx>> {
    timestamp_part_common(mcx, timestamp, lowunits, retnumeric, true)
}

// ---------------------------------------------------------------------------
// interval_part_common
// ---------------------------------------------------------------------------

/// `interval_part_common` core: `interval_part()` / `extract_interval()`.
///
/// (`utils/adt/timestamp.c`)
pub fn interval_part<'mcx>(
    mcx: Mcx<'mcx>,
    interval: &Interval,
    lowunits: &str,
    retnumeric: bool,
) -> DtResult<ExtractResult<'mcx>> {
    let mut val: i32 = 0;
    let mut type_ = DecodeUnits(0, lowunits, &mut val);
    if type_ == UNKNOWN_FIELD {
        type_ = DecodeSpecial(0, lowunits, &mut val);
    }

    if INTERVAL_NOT_FINITE(interval) {
        let r = non_finite_interval_part(type_, val, lowunits, INTERVAL_IS_NOBEGIN(interval))?;
        return Ok(non_finite_result(mcx, r, retnumeric));
    }

    if type_ == UNITS {
        let mut tm = pg_itm {
            tm_usec: 0,
            tm_sec: 0,
            tm_min: 0,
            tm_hour: 0,
            tm_mday: 0,
            tm_mon: 0,
            tm_year: 0,
        };
        interval2itm(*interval, &mut tm);

        let intresult: i64 = match val {
            DTK_MICROSEC => tm.tm_sec as i64 * 1_000_000 + tm.tm_usec as i64,

            DTK_MILLISEC => {
                return Ok(if retnumeric {
                    ExtractResult::Numeric(int64_div_fast_to_numericvar(
                        mcx,
                        tm.tm_sec as i64 * 1_000_000 + tm.tm_usec as i64,
                        3,
                    )?)
                } else {
                    ExtractResult::Float8(tm.tm_sec as f64 * 1000.0 + tm.tm_usec as f64 / 1000.0)
                });
            }

            DTK_SECOND => {
                return Ok(if retnumeric {
                    ExtractResult::Numeric(int64_div_fast_to_numericvar(
                        mcx,
                        tm.tm_sec as i64 * 1_000_000 + tm.tm_usec as i64,
                        6,
                    )?)
                } else {
                    ExtractResult::Float8(tm.tm_sec as f64 + tm.tm_usec as f64 / 1_000_000.0)
                });
            }

            DTK_MINUTE => tm.tm_min as i64,
            DTK_HOUR => tm.tm_hour,
            DTK_DAY => tm.tm_mday as i64,
            DTK_WEEK => (tm.tm_mday / 7) as i64,
            DTK_MONTH => tm.tm_mon as i64,

            DTK_QUARTER => {
                if interval.month >= 0 {
                    (tm.tm_mon / 3 + 1) as i64
                } else {
                    -((((-interval.month % MONTHS_PER_YEAR) / 3) + 1) as i64)
                }
            }

            DTK_YEAR => tm.tm_year as i64,
            DTK_DECADE => (tm.tm_year / 10) as i64,
            DTK_CENTURY => (tm.tm_year / 100) as i64,
            DTK_MILLENNIUM => (tm.tm_year / 1000) as i64,

            _ => {
                return Err(unit_not_supported(lowunits, TYPE_INTERVAL));
            }
        };

        int_result(mcx, intresult, retnumeric)
    } else if type_ == RESERV && val == DTK_EPOCH {
        // To keep integer arithmetic with the fractional DAYS_PER_YEAR
        // (365.25), multiply by 4 and divide at the end.
        let four_days_per_year: i64 = (4.0 * 365.25) as i64; // == 1461
        let secs_from_day_month: i64 = (four_days_per_year
            * (interval.month / MONTHS_PER_YEAR) as i64
            + (4 * DAYS_PER_MONTH) as i64 * (interval.month % MONTHS_PER_YEAR) as i64
            + 4 * interval.day as i64)
            * (SECS_PER_DAY / 4) as i64;

        if retnumeric {
            let mut v: i64 = 0;
            let result = if !pg_mul_s64_overflow(secs_from_day_month, 1_000_000, &mut v)
                && !pg_add_s64_overflow(v, interval.time, &mut v)
            {
                int64_div_fast_to_numericvar(mcx, v, 6)?
            } else {
                add_var(
                    mcx,
                    &int64_div_fast_to_numericvar(mcx, interval.time, 6)?,
                    &int64_to_numericvar(mcx, secs_from_day_month)?,
                )?
            };
            Ok(ExtractResult::Numeric(result))
        } else {
            let mut result = interval.time as f64 / 1_000_000.0;
            result += (365.25 * SECS_PER_DAY as f64) * (interval.month / MONTHS_PER_YEAR) as f64;
            result += (DAYS_PER_MONTH as f64 * SECS_PER_DAY as f64)
                * (interval.month % MONTHS_PER_YEAR) as f64;
            result += SECS_PER_DAY as f64 * interval.day as f64;
            Ok(ExtractResult::Float8(result))
        }
    } else {
        // C interval_part_common (timestamp.c:6289-6296): unlike
        // timestamp_part_common, there is no `type == RESERV` not-supported
        // arm, so any RESERV unit other than DTK_EPOCH falls through here.
        Err(unit_not_recognized(lowunits, TYPE_INTERVAL))
    }
}

// ---------------------------------------------------------------------------
// date_trunc cores
// ---------------------------------------------------------------------------

/// `timestamp_trunc()` core -- truncate `timestamp` to `lowunits`.
///
/// (`utils/adt/timestamp.c`)
pub fn timestamp_trunc(lowunits: &str, timestamp: Timestamp) -> DtResult<Timestamp> {
    // C: format_type_be(TIMESTAMPOID).
    const TYPE_NAME: &str = "timestamp without time zone";

    let mut val: i32 = 0;
    let type_ = DecodeUnits(0, lowunits, &mut val);

    if type_ != UNITS {
        return Err(crate::timestamp::invalid_parameter(format!(
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
                return Err(crate::timestamp::feature_not_supported(format!(
                    "unit \"{lowunits}\" not supported for type {TYPE_NAME}"
                )));
            }
        }
    }

    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err() {
        return Err(crate::timestamp::timestamp_out_of_range());
    }

    timestamp_trunc_apply(val, &mut tm, &mut fsec, lowunits, TYPE_NAME)?;

    let mut result = 0;
    if tm2timestamp(&tm, fsec, None, &mut result).is_err() {
        return Err(crate::timestamp::timestamp_out_of_range());
    }
    Ok(result)
}

/// The fall-through truncation cascade shared by the `timestamp[tz]_trunc`
/// cores, operating on a broken-down `pg_tm` + `fsec`.  Exposed crate-internally
/// for the timestamptz session-zone variant in [`crate::timestamp`].
pub(crate) fn timestamp_trunc_apply_pub(
    val: i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    lowunits: &str,
    type_name: &str,
) -> DtResult<()> {
    timestamp_trunc_apply(val, tm, fsec, lowunits, type_name)
}

/// The fall-through truncation cascade shared by the `timestamp[tz]_trunc`
/// cores, operating on a broken-down `pg_tm` + `fsec`.
fn timestamp_trunc_apply(
    val: i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    lowunits: &str,
    type_name: &str,
) -> DtResult<()> {
    match val {
        DTK_WEEK => {
            let woy = date2isoweek(tm.tm_year, tm.tm_mon, tm.tm_mday);
            if woy >= 52 && tm.tm_mon == 1 {
                tm.tm_year -= 1;
            }
            if woy <= 1 && tm.tm_mon == MONTHS_PER_YEAR {
                tm.tm_year += 1;
            }
            let mut y = tm.tm_year;
            let mut mo = 0;
            let mut d = 0;
            isoweek2date(woy, &mut y, &mut mo, &mut d);
            tm.tm_year = y;
            tm.tm_mon = mo;
            tm.tm_mday = d;
            tm.tm_hour = 0;
            tm.tm_min = 0;
            tm.tm_sec = 0;
            *fsec = 0;
        }
        DTK_MICROSEC => { /* nothing */ }
        DTK_MILLISEC => {
            *fsec = (*fsec / 1000) * 1000;
        }
        _ => {
            // Field cascade (highest field truncated; all lower fields zeroed).
            if matches!(val, DTK_MILLENNIUM) {
                if tm.tm_year > 0 {
                    tm.tm_year = ((tm.tm_year + 999) / 1000) * 1000 - 999;
                } else {
                    tm.tm_year = -((999 - (tm.tm_year - 1)) / 1000) * 1000 + 1;
                }
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY) {
                if tm.tm_year > 0 {
                    tm.tm_year = ((tm.tm_year + 99) / 100) * 100 - 99;
                } else {
                    tm.tm_year = -((99 - (tm.tm_year - 1)) / 100) * 100 + 1;
                }
            }
            if matches!(val, DTK_DECADE) {
                if tm.tm_year > 0 {
                    tm.tm_year = (tm.tm_year / 10) * 10;
                } else {
                    tm.tm_year = -((8 - (tm.tm_year - 1)) / 10) * 10;
                }
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR) {
                tm.tm_mon = 1;
            }
            if matches!(
                val,
                DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER
            ) {
                tm.tm_mon = 3 * ((tm.tm_mon - 1) / 3) + 1;
            }
            if matches!(
                val,
                DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH
            ) {
                tm.tm_mday = 1;
            }
            if matches!(
                val,
                DTK_MILLENNIUM
                    | DTK_CENTURY
                    | DTK_DECADE
                    | DTK_YEAR
                    | DTK_QUARTER
                    | DTK_MONTH
                    | DTK_DAY
            ) {
                tm.tm_hour = 0;
            }
            if matches!(
                val,
                DTK_MILLENNIUM
                    | DTK_CENTURY
                    | DTK_DECADE
                    | DTK_YEAR
                    | DTK_QUARTER
                    | DTK_MONTH
                    | DTK_DAY
                    | DTK_HOUR
            ) {
                tm.tm_min = 0;
            }
            if matches!(
                val,
                DTK_MILLENNIUM
                    | DTK_CENTURY
                    | DTK_DECADE
                    | DTK_YEAR
                    | DTK_QUARTER
                    | DTK_MONTH
                    | DTK_DAY
                    | DTK_HOUR
                    | DTK_MINUTE
            ) {
                tm.tm_sec = 0;
            }
            if matches!(
                val,
                DTK_MILLENNIUM
                    | DTK_CENTURY
                    | DTK_DECADE
                    | DTK_YEAR
                    | DTK_QUARTER
                    | DTK_MONTH
                    | DTK_DAY
                    | DTK_HOUR
                    | DTK_MINUTE
                    | DTK_SECOND
            ) {
                *fsec = 0;
            } else {
                return Err(crate::timestamp::feature_not_supported(format!(
                    "unit \"{lowunits}\" not supported for type {type_name}"
                )));
            }
        }
    }
    Ok(())
}

/// `interval_trunc()` core -- truncate `interval` to `lowunits`.
///
/// (`utils/adt/timestamp.c`)
pub fn interval_trunc(lowunits: &str, interval: &Interval) -> DtResult<Interval> {
    let mut val: i32 = 0;
    let type_ = DecodeUnits(0, lowunits, &mut val);

    if type_ != UNITS {
        return Err(unit_not_recognized(lowunits, TYPE_INTERVAL));
    }

    if INTERVAL_NOT_FINITE(interval) {
        match val {
            DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH
            | DTK_DAY | DTK_HOUR | DTK_MINUTE | DTK_SECOND | DTK_MILLISEC | DTK_MICROSEC => {
                return Ok(*interval);
            }
            _ => {
                return Err(interval_trunc_unsupported(lowunits, val));
            }
        }
    }

    let mut tm = pg_itm {
        tm_usec: 0,
        tm_sec: 0,
        tm_min: 0,
        tm_hour: 0,
        tm_mday: 0,
        tm_mon: 0,
        tm_year: 0,
    };
    interval2itm(*interval, &mut tm);

    // Cascade fall-through (caution: C division may have negative remainder).
    if matches!(val, DTK_MILLENNIUM) {
        tm.tm_year = (tm.tm_year / 1000) * 1000;
    }
    if matches!(val, DTK_MILLENNIUM | DTK_CENTURY) {
        tm.tm_year = (tm.tm_year / 100) * 100;
    }
    if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE) {
        tm.tm_year = (tm.tm_year / 10) * 10;
    }
    if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR) {
        tm.tm_mon = 0;
    }
    if matches!(
        val,
        DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER
    ) {
        tm.tm_mon = 3 * (tm.tm_mon / 3);
    }
    if matches!(
        val,
        DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH
    ) {
        tm.tm_mday = 0;
    }
    if matches!(
        val,
        DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH | DTK_DAY
    ) {
        tm.tm_hour = 0;
    }
    if matches!(
        val,
        DTK_MILLENNIUM
            | DTK_CENTURY
            | DTK_DECADE
            | DTK_YEAR
            | DTK_QUARTER
            | DTK_MONTH
            | DTK_DAY
            | DTK_HOUR
    ) {
        tm.tm_min = 0;
    }
    if matches!(
        val,
        DTK_MILLENNIUM
            | DTK_CENTURY
            | DTK_DECADE
            | DTK_YEAR
            | DTK_QUARTER
            | DTK_MONTH
            | DTK_DAY
            | DTK_HOUR
            | DTK_MINUTE
    ) {
        tm.tm_sec = 0;
    }
    if matches!(
        val,
        DTK_MILLENNIUM
            | DTK_CENTURY
            | DTK_DECADE
            | DTK_YEAR
            | DTK_QUARTER
            | DTK_MONTH
            | DTK_DAY
            | DTK_HOUR
            | DTK_MINUTE
            | DTK_SECOND
    ) {
        tm.tm_usec = 0;
    } else if matches!(val, DTK_MILLISEC) {
        tm.tm_usec = (tm.tm_usec / 1000) * 1000;
    } else if matches!(val, DTK_MICROSEC) {
        /* nothing */
    } else {
        return Err(interval_trunc_unsupported(lowunits, val));
    }

    let mut result = Interval {
        time: 0,
        day: 0,
        month: 0,
    };
    if itm2interval(&tm, &mut result).is_err() {
        return Err(crate::timestamp::interval_out_of_range());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::adt_numeric::io::get_str_from_var;

    fn ts(s: &str) -> Timestamp {
        crate::timestamp::timestamp_in(s, -1).unwrap()
    }

    fn as_f64(r: ExtractResult) -> f64 {
        match r {
            ExtractResult::Float8(v) => v,
            other => panic!("expected float8, got {other:?}"),
        }
    }

    fn as_numeric_str(r: ExtractResult) -> String {
        match r {
            ExtractResult::Numeric(v) => get_str_from_var(&v),
            other => panic!("expected numeric, got {other:?}"),
        }
    }

    #[test]
    fn extract_year_from_timestamp() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let t = ts("2024-06-15 10:30:45");
        assert_eq!(as_f64(timestamp_part(mcx, t, "year", false).unwrap()), 2024.0);
        assert_eq!(
            as_numeric_str(timestamp_part(mcx, t, "year", true).unwrap()),
            "2024"
        );
    }

    #[test]
    fn extract_dow_from_timestamp() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let t = ts("2024-06-15 10:30:45");
        assert_eq!(as_f64(timestamp_part(mcx, t, "dow", false).unwrap()), 6.0);
        assert_eq!(as_f64(timestamp_part(mcx, t, "isodow", false).unwrap()), 6.0);
    }

    #[test]
    fn extract_epoch_from_timestamp() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let t = ts("1970-01-01 00:00:00");
        assert_eq!(as_f64(timestamp_part(mcx, t, "epoch", false).unwrap()), 0.0);
        let t = ts("1970-01-01 00:00:01");
        assert_eq!(as_f64(timestamp_part(mcx, t, "epoch", false).unwrap()), 1.0);
    }

    #[test]
    fn extract_hour_from_interval() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let i = crate::interval::interval_in("3 days 04:05:06", -1).unwrap();
        assert_eq!(as_f64(interval_part(mcx, &i, "hour", false).unwrap()), 4.0);
        assert_eq!(
            as_numeric_str(interval_part(mcx, &i, "hour", true).unwrap()),
            "4"
        );
    }

    #[test]
    fn extract_second_numeric_from_timestamp() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let t = ts("2024-06-15 10:30:45.5");
        assert_eq!(
            as_numeric_str(timestamp_part(mcx, t, "second", true).unwrap()),
            "45.500000"
        );
    }

    #[test]
    fn date_trunc_timestamp_to_day() {
        let _guard = crate::settings::DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        crate::settings::set_date_style(::types_datetime::USE_ISO_DATES);
        let t = ts("2024-06-15 10:30:45.5");
        let r = timestamp_trunc("day", t).unwrap();
        assert_eq!(
            crate::timestamp::timestamp_out(r).unwrap(),
            "2024-06-15 00:00:00"
        );
    }

    #[test]
    fn date_trunc_timestamp_to_month() {
        let _guard = crate::settings::DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        crate::settings::set_date_style(::types_datetime::USE_ISO_DATES);
        let t = ts("2024-06-15 10:30:45");
        let r = timestamp_trunc("month", t).unwrap();
        assert_eq!(
            crate::timestamp::timestamp_out(r).unwrap(),
            "2024-06-01 00:00:00"
        );
    }

    #[test]
    fn interval_trunc_to_hour() {
        let i = crate::interval::interval_in("1 day 04:05:06", -1).unwrap();
        let r = interval_trunc("hour", &i).unwrap();
        assert_eq!(crate::interval::interval_out(&r), "1 day 04:00:00");
    }

    use types_error::{ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE};

    fn inf_interval() -> Interval {
        Interval {
            month: i32::MAX,
            day: i32::MAX,
            time: i64::MAX,
        }
    }

    #[test]
    fn timestamp_part_units_unsupported_is_0a000() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let t = ts("2024-06-15 10:30:45");
        let err = timestamp_part(mcx, t, "timezone", false).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"timezone\" not supported for type timestamp without time zone"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    }

    #[test]
    fn timestamptz_part_reserv_non_epoch_is_0a000() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let t = crate::timestamp::timestamptz_in("2024-06-15 10:30:45+00", -1).unwrap();
        let err = timestamptz_part(mcx, t, "now", false).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"now\" not supported for type timestamp with time zone"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    }

    #[test]
    fn timestamp_part_reserv_non_epoch_is_0a000() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let t = ts("2024-06-15 10:30:45");
        let err = timestamp_part(mcx, t, "now", false).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"now\" not supported for type timestamp without time zone"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    }

    #[test]
    fn timestamp_part_unrecognized_is_22023() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let t = ts("2024-06-15 10:30:45");
        let err = timestamp_part(mcx, t, "fortnight", false).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"fortnight\" not recognized for type timestamp without time zone"
        );
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    }

    #[test]
    fn interval_part_units_unsupported_is_0a000() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let i = crate::interval::interval_in("3 days 04:05:06", -1).unwrap();
        let err = interval_part(mcx, &i, "timezone", false).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"timezone\" not supported for type interval"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    }

    #[test]
    fn interval_part_reserv_non_epoch_is_22023() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let i = crate::interval::interval_in("3 days 04:05:06", -1).unwrap();
        let err = interval_part(mcx, &i, "now", false).unwrap_err();
        assert_eq!(err.message(), "unit \"now\" not recognized for type interval");
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    }

    #[test]
    fn interval_part_unrecognized_is_22023() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let i = crate::interval::interval_in("3 days 04:05:06", -1).unwrap();
        let err = interval_part(mcx, &i, "fortnight", false).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"fortnight\" not recognized for type interval"
        );
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    }

    #[test]
    fn non_finite_timestamp_part_sqlstates() {
        use ::types_datetime::DT_NOEND;
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let err = timestamp_part(mcx, DT_NOEND, "now", false).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"now\" not supported for type timestamp without time zone"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);

        let err = timestamptz_part(mcx, DT_NOEND, "now", false).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"now\" not supported for type timestamp with time zone"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);

        let err = timestamp_part(mcx, DT_NOEND, "fortnight", false).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
        assert_eq!(
            err.message(),
            "unit \"fortnight\" not recognized for type timestamp without time zone"
        );
    }

    #[test]
    fn non_finite_interval_part_sqlstates() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let inf = inf_interval();
        let err = interval_part(mcx, &inf, "julian", false).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"julian\" not supported for type interval"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);

        let err = interval_part(mcx, &inf, "fortnight", false).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"fortnight\" not recognized for type interval"
        );
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    }

    #[test]
    fn interval_trunc_unsupported_is_0a000_with_week_detail() {
        let i = crate::interval::interval_in("3 days 04:05:06", -1).unwrap();

        let err = interval_trunc("week", &i).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"week\" not supported for type interval"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.detail(), Some("Months usually have fractional weeks."));

        let inf = inf_interval();
        let err = interval_trunc("week", &inf).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"week\" not supported for type interval"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.detail(), Some("Months usually have fractional weeks."));

        let err = interval_trunc("timezone", &i).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"timezone\" not supported for type interval"
        );
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.detail(), None);

        let err = interval_trunc("fortnight", &i).unwrap_err();
        assert_eq!(
            err.message(),
            "unit \"fortnight\" not recognized for type interval"
        );
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    }
}
