//! `backend-utils-adt-datetime` — wave-5 core-datatypes port (idiomatic).
//!
//! Faithful, idiomatic-safe-Rust rewrite of PostgreSQL's date/time subsystem:
//! `src/backend/utils/adt/{datetime,date,timestamp}.c`, ported MODULE BY MODULE.
//!
//! The shared date/time ABI structs and constants (`Interval`, `pg_itm`,
//! `pg_itm_in`, `TimeTzADT`, `datetkn`, `DateTimeErrorExtra`, the unit/field/
//! token constants) are owned by `types_datetime` and re-exported here so the
//! whole subsystem speaks one vocabulary. Fallible cores return
//! `types_error::PgResult<_>`. Timezone rotation crosses
//! `backend-timezone-pgtz`/`-localtime`; `pg_strftime` crosses its seam.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Fallible cores return the shared `PgResult` whose `Err` variant (`PgError`)
// is a faithful, un-boxed port of `ErrorData`; this is the project-wide error
// contract every sibling crate matches, so we accept the large-`Err` lint
// crate-wide rather than diverge per-function.
#![allow(clippy::result_large_err)]
// The decode/encode cores mirror C functions that take the broken-down time
// fields by value; keeping the same parameter list preserves the 1:1 mapping.
#![allow(clippy::too_many_arguments)]

// ---------------------------------------------------------------------------
// Ported modules (leaf-first decomposition).
// ---------------------------------------------------------------------------

// Calendar core (date2j / j2date / j2day + the month-length table). Pure
// arithmetic, no external seams. (datetime.c)
pub mod calendar;
// ISO-8601 week-date helpers, built on the calendar core. Pure arithmetic.
// (timestamp.c)
pub mod isoweek;
// Crate-local convenience masks + default output-token strings. Pure data.
// (utils/datetime.h)
pub mod consts;
// DateStyle / DateOrder / IntervalStyle formatting globals (C int globals).
pub mod settings;
// Static date/time keyword tables (datetktbl / deltatktbl / months / days).
// Pure data; order is load-bearing (pre-sorted for datebsearch). (datetime.c)
pub mod tables;
// Date/time *encoders* (the ENCODE half of datetime.c + EncodeSpecial*).
// Self-contained string formatting; no timezone resolution. (datetime.c/date.c/timestamp.c)
pub mod encode;
// Numeric-building helper for the EXTRACT cores (int64_div_fast_to_numericvar).
pub mod numeric_helpers;
// TIME (without time zone) value type — the seam-free arithmetic cores (date.c):
// tm2time / time2tm / time_overflows / float_time_overflows / AdjustTimeForTypmod
// / make_time / comparisons / interval-arithmetic.  The text-driven (decode/
// encode) entry points are ported here too, over the `decode` engine module.
pub mod time;
// Broken-down-time conversion cores shared across the subsystem (timestamp.c +
// the range-check macros): IS_VALID_JULIAN / IS_VALID_DATE / dt2time / time2t /
// dt2local / timestamptz_to_time_t.  Ported ahead of the full timestamp/date
// modules so the decode engine has its prerequisite cores in a canonical,
// seam-free home.
pub mod convert;
// Timezone-resolution hook for the decode engine: a crate-local trait +
// thread-local Cell (the idiomatic analogue of backend-regex-core's
// RegexCollationResolver), used only for the runtime abbreviation-table leg of
// DecodeTimezoneAbbrev.  Not a seam (no algorithm here, just a provider hook).
pub mod tz_resolver;
// InstallTimeZoneAbbrevs (datetime.c) + the production TimezoneResolver over the
// runtime abbreviation table loaded from the timezone_abbreviations GUC.
pub mod tz_abbrev_install;
// The date/time decode engine (datetime.c): ParseDateTime + DecodeDateTime /
// DecodeTimeOnly / DecodeInterval / DecodeISO8601Interval + the numeric/time/tz
// field decoders, datebsearch, and the DetermineTimeZoneOffset family.  Wires
// the tz field decoders directly to the reused backend-timezone-localtime API.
pub mod decode;
// TIMESTAMP / TIMESTAMPTZ value cores (timestamp.c): timestamp2tm/tm2timestamp,
// timestamp_in/out, the comparison/arithmetic cores, age, the session-zone
// rotation, the constructors, date_bin, to_timestamp, AT TIME ZONE, and the
// cross-type conversions.
pub mod timestamp;
// INTERVAL value cores (timestamp.c): interval_in/out, cmp, +/-/*//, justify,
// AdjustIntervalForTypmod, interval2itm/itm2interval.
pub mod interval;
// DATE value type (date.c): date_in/out, comparisons, integer-day arithmetic,
// make_date, extract_date, and the cross-type date<->timestamp[tz] cores +
// the shared DateTimeParseError mapping + CURRENT_* combiners.
pub mod date;
// TIMETZ value type (date.c): tm2timetz/timetz2tm, timetz_in/out, comparison,
// arithmetic, conversions, izone/zone, extract_timetz.
pub mod timetz;
// EXTRACT / date_part / date_trunc cores for timestamp / timestamptz /
// interval (timestamp.c).
pub mod extract;
// SQL OVERLAPS cores (timestamp.c / date.c).
pub mod overlaps;
// Window-frame in_range cores (timestamp.c / date.c).
pub mod in_range;
// The now-family of current-time cores (timestamp.c).  `timeofday` routes its
// pg_strftime call through the centralized strftime seam.
pub mod current;
// Binary (wire) protocol I/O cores: the *_recv / *_send computational halves
// (date.c / timestamp.c), expressed over owned byte buffers (no raw bytea).
pub mod binio;
// Hash opclass cores (date.c / timestamp.c) + the integer-hash folds, built on
// the ported `common-hashfn`.  ABI-exact.
pub mod hash;

// Inward seam implementations (timestamp.c / datetime.c entry points other
// crates reach through their -seams crates) + init_seams().
mod seam_impls;
pub use seam_impls::init_seams;

// fmgr builtin layer: the `Datum xxx(PG_FUNCTION_ARGS)` shims for every
// SQL-callable function in date.c / timestamp.c / datetime.c. Registered into
// the fmgr-core builtin table from init_seams().
mod fmgr_builtins;

// ---------------------------------------------------------------------------
// Re-exports of the shared ABI vocabulary (owned by `types::datetime`).
// ---------------------------------------------------------------------------

pub use types_datetime::{
    datetkn, fsec_t, pg_itm, pg_itm_in, DateADT, DateTimeErrorExtra, Interval, TimeADT, TimeOffset,
    TimeTzADT, Timestamp, TimestampTz,
};

// Calendar core.
pub use calendar::{date2j, day_tab, isleap, j2date, j2day, IS_LEAP_YEAR};

// ISO-8601 week-date helpers.
pub use isoweek::{
    date2isoweek, date2isoyear, date2isoyearday, isoweek2date, isoweek2j, isoweekdate2date,
};

// Static keyword tables.
pub use tables::{datetktbl, days, deltatktbl, months};

// Encoders.
pub use encode::{
    EncodeDateOnly, EncodeDateTime, EncodeInterval, EncodeSpecialDate, EncodeSpecialTimestamp,
    EncodeTimeOnly,
};

// EXTRACT numeric helper.
pub use numeric_helpers::int64_div_fast_to_numericvar;

// TIME value-type arithmetic cores.
pub use time::{
    float_time_overflows, interval_time, make_time, time2tm, time_cmp, time_eq, time_ge, time_gt,
    time_interval, time_larger, time_le, time_lt, time_mi_interval, time_mi_time, time_ne,
    time_overflows, time_pl_interval, time_smaller, tm2time, AdjustTimeForTypmod,
    INTERVAL_NOT_FINITE,
};

// Broken-down-time conversion cores.
pub use convert::{
    dt2local, dt2time, time2t, timestamptz_to_time_t, IS_VALID_DATE, IS_VALID_JULIAN,
};

// Timezone-resolution hook (abbreviation-table provider for the decode engine).
pub use tz_resolver::{set_timezone_resolver, TimezoneResolver, TzAbbrev};

// The date/time decode engine.
pub use decode::{
    datebsearch, DecodeDateTime, DecodeISO8601Interval, DecodeInterval, DecodeSpecial,
    DecodeTimeOnly, DecodeTimezone, DecodeTimezoneAbbrev, DecodeTimezoneAbbrevPrefix,
    DecodeTimezoneName,
    DecodeTimezoneNameToTz, DecodeUnits, DetermineTimeZoneAbbrevOffset,
    DetermineTimeZoneAbbrevOffsetTS, DetermineTimeZoneOffset, ParseDateTime, ValidateDate,
};

// TIMESTAMP / TIMESTAMPTZ cores.
pub use timestamp::{
    AdjustTimestampForTypmod, DtResult, GetCurrentTimestamp, SetEpochTimestamp, IS_VALID_TIMESTAMP,
    TIMESTAMP_IS_NOBEGIN, TIMESTAMP_IS_NOEND, TIMESTAMP_NOT_FINITE,
};
pub use timestamp::{tm2timestamp, timestamp2tm};
pub use timestamp::{
    timestamp2timestamptz, timestamp2timestamptz_opt_overflow, timestamptz2timestamp,
};
pub use timestamp::{
    timestamp_cmp_timestamptz_internal, GetSQLCurrentTimestamp, GetSQLLocalTimestamp,
};
pub use timestamp::{
    datetime_timestamp, datetimetz_timestamptz, float8_timestamptz, make_interval, make_timestamp,
    make_timestamp_internal, make_timestamptz, make_timestamptz_at_timezone, timestamp_bin,
    timestamp_izone, timestamp_time, timestamp_zone, timestamptz_at_local, timestamptz_bin,
    timestamptz_izone, timestamptz_mi_interval, timestamptz_mi_interval_at_zone,
    timestamptz_mi_interval_internal, timestamptz_pl_interval, timestamptz_pl_interval_at_zone,
    timestamptz_pl_interval_internal, timestamptz_time, timestamptz_timetz,
    timestamptz_trunc, timestamptz_trunc_internal, timestamptz_trunc_zone, timestamptz_zone,
};

// INTERVAL cores.
pub use interval::{
    interval2itm, interval_cmp, interval_cmp_internal, interval_cmp_value, interval_div,
    interval_in, interval_justify_days, interval_justify_hours, interval_justify_interval,
    interval_mi, interval_mul, interval_out, interval_pl, interval_sign, interval_um,
    interval_um_internal, itm2interval, itmin2interval, AdjustIntervalForTypmod,
    EncodeSpecialInterval, INTERVAL_IS_NOBEGIN, INTERVAL_IS_NOEND,
};

// DATE + TIME-value cross-type / current-time cores (date.c).
pub use date::{
    date2timestamp, date2timestamp_no_overflow, date2timestamp_opt_overflow, date2timestamptz,
    date2timestamptz_opt_overflow, date_cmp_timestamp_internal, date_cmp_timestamptz_internal,
    date_in, date_mi_interval, date_out, date_pl_interval, extract_date, make_date, time_timetz,
    timestamp_date, timestamptz_date, ExtractDateResult, GetSQLCurrentDate, GetSQLCurrentTime,
    GetSQLLocalTime,
};

// TIME value-type text + EXTRACT cores (date.c).
pub use time::{time_in, time_out, time_part_common, TimePartResult};

// TIMETZ value-type cores (date.c).
pub use timetz::{
    tm2timetz, timetz_in, timetz_izone, timetz_out, timetz_part_common, timetz_zone,
    TimetzPartResult,
};

// EXTRACT / date_part / date_trunc cores (timestamp.c).
pub use extract::{
    interval_part, interval_trunc, timestamp_part, timestamp_trunc, timestamptz_part, ExtractResult,
};

// SQL OVERLAPS cores (timestamp.c / date.c).
pub use overlaps::{overlaps_time, overlaps_timestamp, overlaps_timetz};

// Window-frame in_range cores (timestamp.c / date.c).
pub use in_range::{
    in_range_date_interval, in_range_interval_interval, in_range_time_interval,
    in_range_timestamp_interval, in_range_timestamptz_interval, in_range_timetz_interval,
};

// The now-family of current-time cores (timestamp.c).
pub use current::{clock_timestamp, now, statement_timestamp, timeofday, transaction_timestamp};

// Binary (wire) protocol I/O cores (date.c / timestamp.c).  The Datum
// PG_FUNCTION_ARGS shims stay deferred.
pub use binio::{
    date_recv, date_send, interval_recv, interval_send, time_recv, time_send, timestamp_recv,
    timestamp_send, timestamptz_recv, timestamptz_send, timetz_recv, timetz_send, WireReader,
};

// Hash opclass cores (date.c / timestamp.c) + the integer-hash folds.  The
// Datum PG_FUNCTION_ARGS shims stay deferred.
pub use hash::{
    hash_uint32, hash_uint32_extended, hashdate, hashdateextended, hashint4, hashint4extended,
    hashint8, hashint8extended, interval_hash, interval_hash_extended, time_hash,
    time_hash_extended, timestamp_hash, timestamp_hash_extended, timestamptz_hash,
    timestamptz_hash_extended, timetz_hash, timetz_hash_extended,
};

#[cfg(test)]
pub(crate) fn test_install_seams() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // pg_localtime (consumed by the timezone-rotation legs).
        backend_timezone_localtime::init_seams();
        // pg_open_tzfile (tzdb loader used by pg_tzset / pg_tzset_offset).
        backend_timezone_pgtz::init_seams();
        // The parallel transaction-timestamp seam read by xact's
        // SetParallelStartTimestamps / GetCurrent*StartTimestamp.
        backend_access_transam_parallel::init_seams();
    });
}
