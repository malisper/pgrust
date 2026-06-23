//! The jsonpath_exec.c datetime substrate: `parse_datetime` text-parsing,
//! `compareDatetime` cross-type comparison, and the `executeDateTimeMethod`
//! cast switch (`.date()/.time()/.time_tz()/.timestamp()/.timestamp_tz()`).
//!
//! These were previously routed through this unit's own seams crate
//! (`parse_datetime` / `compare_datetime` / `datetime_method_cast`); they are
//! now wired in-crate against the real ported `backend-utils-adt-formatting`
//! (`parse_datetime`) and `backend-utils-adt-datetime` (date/time/timestamp
//! casts + comparisons) leaf units, exactly as C calls them through
//! `DirectFunctionCall*`.
//!
//! # Datetime value carrier
//!
//! A jsonpath datetime item is `(value: Datum word, typid, typmod, tz)` —
//! [`DateTimeValue`]. The repo models a datetime `Datum` as a **by-value
//! machine word** (date = `int32`, time/timestamp/timestamptz = `int64`),
//! matching how `JsonbValue.val.datetime.value` is stored as a `usize`. C's
//! `timetz` is a by-reference 12-byte `{ TimeADT time, int32 zone }`; here the
//! lossless split used is `value = time word`, `tz = zone`, so cross-type
//! comparison reconstructs the full `TimeTzADT` without a by-reference `Datum`.
//! (Rendering a `timetz` jsonpath item back to text still bottoms out on the
//! by-reference-`Datum` json.c encoder, the separate by-ref-`Datum` campaign.)

#![allow(clippy::too_many_arguments)]

extern crate alloc;

use ::utils_error::{ereport, PgError, PgResult};
use ::types_core::Oid;
use ::datum::Datum;
use ::types_error::{
    SoftErrorContext, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION, ERROR,
};
use ::types_tuple::heaptuple::{DATEOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID};

use ::types_datetime::{fsec_t, DateADT, TimeADT, TimeTzADT, Timestamp, TimestampTz};
use ::types_datetime::POSTGRES_EPOCH_JDATE;
use ::pgtime::pg_tm;

use ::mcx::Mcx;

use crate::seam::DateTimeValue;

// The jsonpath JsonPathItemType discriminants the cast switch needs. These
// mirror `jpiDate`/`jpiTime`/`jpiTimeTz`/`jpiTimestamp`/`jpiTimestampTz` from
// `::adt_jsonpath::JsonPathItemType`.
use ::adt_jsonpath::JsonPathItemType;

// ---------------------------------------------------------------------------
// parse_datetime (formatting.c:4216) wrapper — text -> DateTimeValue.
// ---------------------------------------------------------------------------

/// C: `parse_datetime(text *date_txt, text *fmt, Oid collid, bool strict,
/// Oid *typid, int32 *typmod, int *tz, Node *escontext)` (formatting.c), as
/// called by `executeDateTimeMethod`.
///
/// `strict` is always `true` at the call sites. On a soft error
/// (`throw_error == false`) returns `Ok(None)`; on success returns the parsed
/// value/typid/typmod/tz. The result `Datum` word is by-value for
/// date/time/timestamp/timestamptz and the `time` word for `timetz` (its zone
/// lands in `DateTimeValue::tz`).
pub fn parse_datetime(
    mcx: Mcx<'_>,
    datetime: &[u8],
    template: &[u8],
    collid: Oid,
    throw_error: bool,
) -> PgResult<Option<DateTimeValue>> {
    use ::formatting::ParseDatetimeResult as R;

    let mut tz: i32 = 0;
    let mut escontext = if throw_error {
        None
    } else {
        Some(SoftErrorContext::new(false))
    };

    let parsed = ::formatting::parse_datetime(
        mcx,
        datetime,
        template,
        collid,
        true, // strict
        &mut tz,
        escontext.as_mut(),
    )?;

    // A soft error recorded in escontext (or a None from a hard error routed as
    // soft) means the input did not fit this template.
    let Some(parsed) = parsed else {
        return Ok(None);
    };

    let dtv = match parsed {
        R::Timestamptz { value, typmod } => DateTimeValue {
            value: Datum::from_i64(value),
            typid: TIMESTAMPTZOID,
            typmod,
            tz,
        },
        R::Timestamp { value, typmod } => DateTimeValue {
            value: Datum::from_i64(value),
            typid: TIMESTAMPOID,
            typmod,
            tz: 0,
        },
        R::Date(d) => DateTimeValue {
            value: Datum::from_i32(d),
            typid: DATEOID,
            typmod: -1,
            tz: 0,
        },
        R::Timetz { value, typmod } => DateTimeValue {
            // Lossless split: time word in `value`, zone in `tz`.
            value: Datum::from_i64(value.time),
            typid: TIMETZOID,
            typmod,
            tz: value.zone,
        },
        R::Time { value, typmod } => DateTimeValue {
            value: Datum::from_i64(value),
            typid: TIMEOID,
            typmod,
            tz: 0,
        },
    };

    Ok(Some(dtv))
}

// ---------------------------------------------------------------------------
// checkTimezoneIsUsedForCast (jsonpath_exec.c:3664) and the timetz helper.
// ---------------------------------------------------------------------------

/// C: `checkTimezoneIsUsedForCast` (jsonpath_exec.c:3664). Throws when a cast
/// that needs timezone support is attempted without it.
fn check_timezone_is_used_for_cast(use_tz: bool, type1: &str, type2: &str) -> PgResult<()> {
    if !use_tz {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(alloc::format!(
                "cannot convert value from {type1} to {type2} without time zone usage"
            ))
            .errhint("Use *_tz() function for time zone support.")
            .into_error());
    }
    Ok(())
}

/// C: `castTimeToTimeTz` (jsonpath_exec.c:3676). Promote a `TimeADT` word to a
/// `TimeTzADT` (using the session-zone offset on the current date).
fn cast_time_to_timetz(time: TimeADT, use_tz: bool) -> PgResult<TimeTzADT> {
    check_timezone_is_used_for_cast(use_tz, "time", "timetz")?;
    Ok(::adt_datetime::date::time_timetz(time))
}

// ---------------------------------------------------------------------------
// cmpDateToTimestamp / cmpDateToTimestampTz / cmpTimestampToTimestampTz
// (jsonpath_exec.c:3687..3712) — the three jsonpath-private helpers.
// ---------------------------------------------------------------------------

/// C: `cmpDateToTimestamp` (jsonpath_exec.c:3687).
fn cmp_date_to_timestamp(date1: DateADT, ts2: Timestamp, _use_tz: bool) -> i32 {
    ::adt_datetime::date::date_cmp_timestamp_internal(date1, ts2)
}

/// C: `cmpDateToTimestampTz` (jsonpath_exec.c:3696).
fn cmp_date_to_timestamptz(date1: DateADT, tstz2: TimestampTz, use_tz: bool) -> PgResult<i32> {
    check_timezone_is_used_for_cast(use_tz, "date", "timestamptz")?;
    Ok(::adt_datetime::date::date_cmp_timestamptz_internal(
        date1, tstz2,
    ))
}

/// C: `cmpTimestampToTimestampTz` (jsonpath_exec.c:3707).
fn cmp_timestamp_to_timestamptz(ts1: Timestamp, tstz2: TimestampTz, use_tz: bool) -> PgResult<i32> {
    check_timezone_is_used_for_cast(use_tz, "timestamp", "timestamptz")?;
    Ok(
        ::adt_datetime::timestamp::timestamp_cmp_timestamptz_internal(ts1, tstz2),
    )
}

// ---------------------------------------------------------------------------
// compareDatetime (jsonpath_exec.c:3721).
// ---------------------------------------------------------------------------

/// One operand of [`compare_datetime`]: the by-value word plus the `tz`
/// (zone) carried alongside `timetz` (and ignored for the others).
#[derive(Clone, Copy)]
pub struct DtOperand {
    pub value: Datum,
    pub typid: Oid,
    /// The numeric zone for a `timetz` item (the second half of its
    /// by-reference `TimeTzADT`); unused for the other datetime types.
    pub tz: i32,
}

impl DtOperand {
    fn date(&self) -> DateADT {
        self.value.as_i32()
    }
    fn time(&self) -> TimeADT {
        self.value.as_i64()
    }
    fn timetz(&self) -> TimeTzADT {
        TimeTzADT {
            time: self.value.as_i64(),
            zone: self.tz,
        }
    }
    fn timestamp(&self) -> Timestamp {
        self.value.as_i64()
    }
    fn timestamptz(&self) -> TimestampTz {
        self.value.as_i64()
    }
}

/// C: `compareDatetime` (jsonpath_exec.c:3721) — cross-type comparison of two
/// datetime SQL/JSON items. Returns `Ok(None)` when the items are uncomparable
/// (`*cast_error = true`), else `Ok(Some(cmp))`. Throws when a cast requires
/// timezone usage and `use_tz` is false.
pub fn compare_datetime(val1: DtOperand, val2: DtOperand, use_tz: bool) -> PgResult<Option<i32>> {
    use ::adt_datetime::date::date_cmp;
    use ::adt_datetime::time::time_cmp;
    use ::adt_datetime::timestamp::timestamp_cmp;
    use ::adt_datetime::timetz::timetz_cmp;

    // The comparator, deferred so the `*cast_error`/return-early arms can short
    // circuit exactly as the C switch does.
    enum Cmp {
        Date(DateADT, DateADT),
        Time(TimeADT, TimeADT),
        Timetz(TimeTzADT, TimeTzADT),
        Timestamp(Timestamp, Timestamp),
    }

    let cmp = match val1.typid {
        DATEOID => match val2.typid {
            DATEOID => Cmp::Date(val1.date(), val2.date()),
            TIMESTAMPOID => {
                return Ok(Some(cmp_date_to_timestamp(
                    val1.date(),
                    val2.timestamp(),
                    use_tz,
                )));
            }
            TIMESTAMPTZOID => {
                return Ok(Some(cmp_date_to_timestamptz(
                    val1.date(),
                    val2.timestamptz(),
                    use_tz,
                )?));
            }
            TIMEOID | TIMETZOID => return Ok(None), // uncomparable
            other => return Err(unrecognized_dt_oid(other)),
        },
        TIMEOID => match val2.typid {
            TIMEOID => Cmp::Time(val1.time(), val2.time()),
            TIMETZOID => Cmp::Timetz(cast_time_to_timetz(val1.time(), use_tz)?, val2.timetz()),
            DATEOID | TIMESTAMPOID | TIMESTAMPTZOID => return Ok(None),
            other => return Err(unrecognized_dt_oid(other)),
        },
        TIMETZOID => match val2.typid {
            TIMEOID => Cmp::Timetz(val1.timetz(), cast_time_to_timetz(val2.time(), use_tz)?),
            TIMETZOID => Cmp::Timetz(val1.timetz(), val2.timetz()),
            DATEOID | TIMESTAMPOID | TIMESTAMPTZOID => return Ok(None),
            other => return Err(unrecognized_dt_oid(other)),
        },
        TIMESTAMPOID => match val2.typid {
            DATEOID => {
                return Ok(Some(-cmp_date_to_timestamp(
                    val2.date(),
                    val1.timestamp(),
                    use_tz,
                )));
            }
            TIMESTAMPOID => Cmp::Timestamp(val1.timestamp(), val2.timestamp()),
            TIMESTAMPTZOID => {
                return Ok(Some(cmp_timestamp_to_timestamptz(
                    val1.timestamp(),
                    val2.timestamptz(),
                    use_tz,
                )?));
            }
            TIMEOID | TIMETZOID => return Ok(None),
            other => return Err(unrecognized_dt_oid(other)),
        },
        TIMESTAMPTZOID => match val2.typid {
            DATEOID => {
                return Ok(Some(-cmp_date_to_timestamptz(
                    val2.date(),
                    val1.timestamptz(),
                    use_tz,
                )?));
            }
            TIMESTAMPOID => {
                return Ok(Some(-cmp_timestamp_to_timestamptz(
                    val2.timestamp(),
                    val1.timestamptz(),
                    use_tz,
                )?));
            }
            // C: `cmpfunc = timestamp_cmp` (TimestampTz uses the same comparator).
            TIMESTAMPTZOID => Cmp::Timestamp(val1.timestamptz(), val2.timestamptz()),
            TIMEOID | TIMETZOID => return Ok(None),
            other => return Err(unrecognized_dt_oid(other)),
        },
        other => return Err(unrecognized_dt_oid(other)),
    };

    let res = match cmp {
        Cmp::Date(a, b) => date_cmp(a, b),
        Cmp::Time(a, b) => time_cmp(a, b),
        Cmp::Timetz(a, b) => timetz_cmp(&a, &b),
        Cmp::Timestamp(a, b) => timestamp_cmp(a, b),
    };
    Ok(Some(res))
}

fn unrecognized_dt_oid(oid: Oid) -> PgError {
    crate::elog_error(&alloc::format!(
        "unrecognized SQL/JSON datetime type oid: {oid}"
    ))
}

// ---------------------------------------------------------------------------
// The executeDateTimeMethod cast switch (jsonpath_exec.c:2503..2745).
// ---------------------------------------------------------------------------

/// The `.date()/.time()/.time_tz()/.timestamp()/.timestamp_tz()` cast switch of
/// `executeDateTimeMethod` (jsonpath_exec.c). Convert `parsed` to the method's
/// target type, applying the optional time-precision typmod and the `use_tz`
/// checks. Returns the converted value, or `Ok(None)` to signal a soft
/// (suppressed) error.
///
/// `target` is the method's [`JsonPathItemType`] discriminant; `datetime_cstr`
/// is the source text used in "format is not recognized" messages.
pub fn datetime_method_cast(
    target: JsonPathItemType,
    parsed: DateTimeValue,
    mut time_precision: i32,
    use_tz: bool,
    datetime_cstr: &str,
    throw_error: bool,
) -> PgResult<Option<DateTimeValue>> {
    use ::adt_datetime::date::{
        date2timestamp, date2timestamptz, time_timetz, timestamp_date, timestamptz_date,
    };
    use ::adt_datetime::time::AdjustTimeForTypmod;
    use ::adt_datetime::timestamp::{
        timestamp2timestamptz, timestamp_time, timestamptz_time,
        timestamptz_timetz, timestamptz2timestamp, AdjustTimestampForTypmod,
    };
    use ::adt_datetime::timetz::timetz_time;
    use JsonPathItemType::*;

    let typid = parsed.typid;
    let mut value = parsed.value;
    let mut typmod = parsed.typmod;
    let mut tz = parsed.tz;

    let fmt_err = |t: &str| -> PgError {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION)
            .errmsg(alloc::format!("{t} format is not recognized: \"{datetime_cstr}\""))
            .into_error()
    };

    let new_typid: Oid = match target {
        jpiDatetime => {
            // C: nothing to do for DATETIME (handled by the caller already).
            return Ok(Some(parsed));
        }
        jpiDate => {
            match typid {
                DATEOID => {} // nothing to do
                TIMEOID | TIMETZOID => {
                    return soft_or_throw(throw_error, fmt_err("date"));
                }
                TIMESTAMPOID => {
                    value = Datum::from_i32(timestamp_date(value.as_i64())?);
                }
                TIMESTAMPTZOID => {
                    check_timezone_is_used_for_cast(use_tz, "timestamptz", "date")?;
                    value = Datum::from_i32(timestamptz_date(value.as_i64())?);
                }
                other => return Err(unsupported_oid(other)),
            }
            DATEOID
        }
        jpiTime => {
            match typid {
                DATEOID => return soft_or_throw(throw_error, fmt_err("time")),
                TIMEOID => {} // nothing to do
                TIMETZOID => {
                    check_timezone_is_used_for_cast(use_tz, "timetz", "time")?;
                    value = Datum::from_i64(timetz_time(&DateTimeValue_as_timetz(value, tz)));
                }
                TIMESTAMPOID => {
                    value = Datum::from_i64(opt_time(timestamp_time(value.as_i64())?));
                }
                TIMESTAMPTZOID => {
                    check_timezone_is_used_for_cast(use_tz, "timestamptz", "time")?;
                    value = Datum::from_i64(opt_time(timestamptz_time(value.as_i64())?));
                }
                other => return Err(unsupported_oid(other)),
            }

            if time_precision != -1 {
                time_precision = anytime_typmod_check_warn(false, time_precision)?;
                let mut result: TimeADT = value.as_i64();
                AdjustTimeForTypmod(&mut result, time_precision);
                value = Datum::from_i64(result);
                typmod = time_precision;
            }
            TIMEOID
        }
        jpiTimeTz => {
            match typid {
                DATEOID | TIMESTAMPOID => return soft_or_throw(throw_error, fmt_err("time_tz")),
                TIMEOID => {
                    check_timezone_is_used_for_cast(use_tz, "time", "timetz")?;
                    let tt = time_timetz(value.as_i64());
                    value = Datum::from_i64(tt.time);
                    tz = tt.zone;
                }
                TIMETZOID => {} // nothing to do
                TIMESTAMPTZOID => {
                    let tt = opt_timetz(timestamptz_timetz(value.as_i64())?);
                    value = Datum::from_i64(tt.time);
                    tz = tt.zone;
                }
                other => return Err(unsupported_oid(other)),
            }

            if time_precision != -1 {
                time_precision = anytime_typmod_check_warn(true, time_precision)?;
                let mut tt = TimeTzADT {
                    time: value.as_i64(),
                    zone: tz,
                };
                AdjustTimeForTypmod(&mut tt.time, time_precision);
                value = Datum::from_i64(tt.time);
                typmod = time_precision;
            }
            TIMETZOID
        }
        jpiTimestamp => {
            match typid {
                DATEOID => {
                    value = Datum::from_i64(date2timestamp(value.as_i32())?);
                }
                TIMEOID | TIMETZOID => return soft_or_throw(throw_error, fmt_err("timestamp")),
                TIMESTAMPOID => {} // nothing to do
                TIMESTAMPTZOID => {
                    check_timezone_is_used_for_cast(use_tz, "timestamptz", "timestamp")?;
                    value = Datum::from_i64(timestamptz2timestamp(value.as_i64())?);
                }
                other => return Err(unsupported_oid(other)),
            }

            if time_precision != -1 {
                time_precision = anytimestamp_typmod_check_warn(false, time_precision)?;
                let mut result: Timestamp = value.as_i64();
                if AdjustTimestampForTypmod(&mut result, time_precision).is_err() {
                    // C: "should not happen" — a hard error regardless of soft mode.
                    return Err(timestamp_precision_invalid(target)?);
                }
                value = Datum::from_i64(result);
                typmod = time_precision;
            }
            TIMESTAMPOID
        }
        jpiTimestampTz => {
            match typid {
                DATEOID => {
                    check_timezone_is_used_for_cast(use_tz, "date", "timestamptz")?;
                    // C: get the tz explicitly since JsonbValue keeps it separate.
                    let date: DateADT = value.as_i32();
                    let (y, m, d) =
                        ::adt_datetime::calendar::j2date(date + POSTGRES_EPOCH_JDATE);
                    let mut tm = pg_tm {
                        tm_year: y,
                        tm_mon: m,
                        tm_mday: d,
                        tm_hour: 0,
                        tm_min: 0,
                        tm_sec: 0,
                        ..Default::default()
                    };
                    tz = ::adt_datetime::decode::DetermineTimeZoneOffset(
                        &mut tm,
                        &state_pgtz::session_timezone(),
                    );
                    value = Datum::from_i64(date2timestamptz(date)?);
                }
                TIMEOID | TIMETZOID => return soft_or_throw(throw_error, fmt_err("timestamp_tz")),
                TIMESTAMPOID => {
                    check_timezone_is_used_for_cast(use_tz, "timestamp", "timestamptz")?;
                    // C: get the tz explicitly since JsonbValue keeps it separate.
                    let ts: Timestamp = value.as_i64();
                    let mut tm = pg_tm::default();
                    let mut fsec: fsec_t = 0;
                    if ::adt_datetime::timestamp::timestamp2tm(
                        ts, None, &mut tm, &mut fsec, None, None,
                    )
                    .is_ok()
                    {
                        tz = ::adt_datetime::decode::DetermineTimeZoneOffset(
                            &mut tm,
                            &state_pgtz::session_timezone(),
                        );
                    }
                    value = Datum::from_i64(timestamp2timestamptz(ts)?);
                }
                TIMESTAMPTZOID => {} // nothing to do
                other => return Err(unsupported_oid(other)),
            }

            if time_precision != -1 {
                time_precision = anytimestamp_typmod_check_warn(true, time_precision)?;
                let mut result: TimestampTz = value.as_i64();
                if AdjustTimestampForTypmod(&mut result, time_precision).is_err() {
                    return Err(timestamp_precision_invalid(target)?);
                }
                value = Datum::from_i64(result);
                typmod = time_precision;
            }
            TIMESTAMPTZOID
        }
        other => {
            return Err(crate::elog_error(&alloc::format!(
                "unrecognized jsonpath item type: {}",
                other as i32
            )));
        }
    };

    Ok(Some(DateTimeValue {
        value,
        typid: new_typid,
        typmod,
        tz,
    }))
}

/// Reconstruct a `TimeTzADT` from the `(value, tz)` split for the `timetz`
/// source arm of the `.time()` cast.
fn DateTimeValue_as_timetz(value: Datum, tz: i32) -> TimeTzADT {
    TimeTzADT {
        time: value.as_i64(),
        zone: tz,
    }
}

/// The `Option`-returning `timestamp_time`/`timestamptz_time` (None for the
/// non-finite inputs, which `parse_datetime` never yields here) coerced to a
/// `TimeADT` word (0 for the impossible non-finite case, matching C's
/// `TimeADTGetDatum(0)` for `DT_NOBEGIN/DT_NOEND`).
fn opt_time(v: Option<TimeADT>) -> TimeADT {
    v.unwrap_or(0)
}

fn opt_timetz(v: Option<TimeTzADT>) -> TimeTzADT {
    v.unwrap_or(TimeTzADT { time: 0, zone: 0 })
}

fn unsupported_oid(oid: Oid) -> PgError {
    crate::elog_error(&alloc::format!("type with oid {oid} not supported"))
}

/// C: the "time precision ... is invalid" error (the AdjustTimestampForTypmod
/// "should not happen" arm). Always a hard error.
fn timestamp_precision_invalid(target: JsonPathItemType) -> PgResult<PgError> {
    let name = crate::op_name(target)?;
    Ok(ereport(ERROR)
        .errcode(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION)
        .errmsg(alloc::format!(
            "time precision of jsonpath item method .{name}() is invalid"
        ))
        .into_error())
}

/// `anytime_typmod_check(istz, typmod)` (date.c:72) wrapper that emits the
/// `ereport(WARNING, "TIME(%d)%s precision reduced to maximum allowed, %d")`
/// that the leaf `backend-utils-adt-datetime` arithmetic core (which has no
/// backend ereport facility) defers to its caller. C emits this WARNING inside
/// `anytime_typmod_check` itself; here we reproduce it at the jsonpath_exec
/// call site, identically gated on `typmod > MAX_TIME_PRECISION`.
fn anytime_typmod_check_warn(istz: bool, typmod: i32) -> PgResult<i32> {
    use ::adt_datetime::time::anytime_typmod_check;
    use ::types_datetime::MAX_TIME_PRECISION;
    if typmod > MAX_TIME_PRECISION {
        ereport(::types_error::WARNING)
            .errcode(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!(
                "TIME({typmod}){} precision reduced to maximum allowed, {MAX_TIME_PRECISION}",
                if istz { " WITH TIME ZONE" } else { "" }
            ))
            .finish(::types_error::ErrorLocation::new(
                "date.c",
                83,
                "anytime_typmod_check",
            ))?;
    }
    anytime_typmod_check(istz, typmod)
}

/// `anytimestamp_typmod_check(istz, typmod)` (timestamp.c:120) wrapper emitting
/// the `ereport(WARNING, "TIMESTAMP(%d)%s precision reduced to maximum allowed,
/// %d")` deferred by the leaf datetime core, identically gated on
/// `typmod > MAX_TIMESTAMP_PRECISION`.
fn anytimestamp_typmod_check_warn(istz: bool, typmod: i32) -> PgResult<i32> {
    use ::adt_datetime::timestamp::anytimestamp_typmod_check;
    use ::types_datetime::MAX_TIMESTAMP_PRECISION;
    if typmod > MAX_TIMESTAMP_PRECISION {
        ereport(::types_error::WARNING)
            .errcode(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!(
                "TIMESTAMP({typmod}){} precision reduced to maximum allowed, {MAX_TIMESTAMP_PRECISION}",
                if istz { " WITH TIME ZONE" } else { "" }
            ))
            .finish(::types_error::ErrorLocation::new(
                "timestamp.c",
                136,
                "anytimestamp_typmod_check",
            ))?;
    }
    anytimestamp_typmod_check(istz, typmod)
}

/// Route a hard `PgError` either to a throw (`Err`) or, in soft mode, to the
/// suppressed `Ok(None)` that signals `res = jperError`.
fn soft_or_throw(throw_error: bool, err: PgError) -> PgResult<Option<DateTimeValue>> {
    if throw_error {
        Err(err)
    } else {
        Ok(None)
    }
}
