//! ABI structs and constants for the PostgreSQL date/time subsystem.
//!
//! These mirror the C declarations in
//! `src/include/datatype/timestamp.h`, `src/include/utils/{date,timestamp,datetime}.h`.
//!
//! `pg_tm` / `pg_tz` are intentionally NOT defined here; they are reused from
//! `types_pgtime`.
//!
//! The scalar type aliases (`DateADT`, `TimeADT`, `Timestamp`, `TimeOffset`,
//! `fsec_t`) are defined here; `TimestampTz` and `pg_time_t` are re-used from
//! `types_core`.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::string::String;

// Re-export the shared scalar already homed in types-core.
pub use types_core::primitive::{pg_time_t, TimestampTz};

/// `DateADT` (`utils/date.h`) — days since `POSTGRES_EPOCH_JDATE`.
pub type DateADT = i32;
/// `TimeADT` (`utils/date.h`) — microseconds since midnight.
pub type TimeADT = i64;
/// `Timestamp` (`datatype/timestamp.h`) — microseconds since
/// `POSTGRES_EPOCH_JDATE`.
pub type Timestamp = i64;
/// `TimeOffset` (`datatype/timestamp.h`).
pub type TimeOffset = i64;
/// `fsec_t` (`datatype/timestamp.h`) — fractional seconds, in microseconds.
pub type fsec_t = i32;

// ---------------------------------------------------------------------------
// Storage format for type interval.  (datatype/timestamp.h)
// ---------------------------------------------------------------------------

/// Storage format for type `interval`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Interval {
    /// All time units other than days, months and years.
    pub time: TimeOffset,
    /// Days, after `time` for alignment.
    pub day: i32,
    /// Months and years, after `time` for alignment.
    pub month: i32,
}

/// Broken-down interval; modeled on `struct pg_tm`. (datatype/timestamp.h)
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct pg_itm {
    pub tm_usec: i32,
    pub tm_sec: i32,
    pub tm_min: i32,
    /// Needs to be wide; gets 4 bytes of pre-padding for 8-byte alignment.
    pub tm_hour: i64,
    pub tm_mday: i32,
    pub tm_mon: i32,
    pub tm_year: i32,
}

/// Data structure for decoding intervals. (datatype/timestamp.h)
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct pg_itm_in {
    /// Needs to be wide.
    pub tm_usec: i64,
    pub tm_mday: i32,
    pub tm_mon: i32,
    pub tm_year: i32,
}

// ---------------------------------------------------------------------------
// SQL "date" and "time" types.  (utils/date.h)
// ---------------------------------------------------------------------------

/// Storage format for type `timetz`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TimeTzADT {
    /// All time units other than months and years.
    pub time: TimeADT,
    /// Numeric time zone, in seconds.
    pub zone: i32,
    // 4 bytes of tail padding to reach align-8 size of 16.
}

// ---------------------------------------------------------------------------
// datetkn + DateTimeErrorExtra.  (utils/datetime.h)
// ---------------------------------------------------------------------------

/// Token table entry for time parsing/decoding. (utils/datetime.h)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct datetkn {
    /// Always NUL-terminated (`TOKMAXLEN + 1`).
    pub token: [u8; 11],
    /// See field type codes above.
    pub r#type: i8,
    /// Meaning depends on `type`; lands at offset 12 (no padding -- offset is already 4-aligned).
    pub value: i32,
}

/// Auxiliary info carried out of datetime parse errors. (utils/datetime.h)
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DateTimeErrorExtra {
    /// Incorrect time zone name (DTERR_BAD_TIMEZONE / DTERR_BAD_ZONE_ABBREV).
    pub dtee_timezone: Option<String>,
    /// Relevant time zone abbreviation (DTERR_BAD_ZONE_ABBREV).
    pub dtee_abbrev: Option<String>,
}

// ---------------------------------------------------------------------------
// Infinity sentinels.  (datatype/timestamp.h)
// ---------------------------------------------------------------------------

/// Timestamp -infinity (== `PG_INT64_MIN`).
pub const TIMESTAMP_MINUS_INFINITY: i64 = i64::MIN;
/// Timestamp +infinity (== `PG_INT64_MAX`).
pub const TIMESTAMP_INFINITY: i64 = i64::MAX;
/// Historical alias for timestamp -infinity.
pub const DT_NOBEGIN: i64 = TIMESTAMP_MINUS_INFINITY;
/// Historical alias for timestamp +infinity.
pub const DT_NOEND: i64 = TIMESTAMP_INFINITY;

/// Interval -infinity (all fields min). The `time` field value.
pub const INTERVAL_NOBEGIN: i64 = i64::MIN;
/// Interval +infinity (all fields max). The `time` field value.
pub const INTERVAL_NOEND: i64 = i64::MAX;

// ---------------------------------------------------------------------------
// Assorted constants for datetime-related calculations. (datatype/timestamp.h)
//
// The "MD/HR/MIN/SEC per-unit" conversion constants live here.
// ---------------------------------------------------------------------------

pub const MONTHS_PER_YEAR: i32 = 12;
/// Assumes exactly 30 days per month ("MD").
pub const DAYS_PER_MONTH: i32 = 30;
pub const DAYS_PER_WEEK: i32 = 7;
pub const HOURS_PER_DAY: i32 = 24;

pub const SECS_PER_YEAR: i32 = 36525 * 864;
pub const SECS_PER_DAY: i32 = 86400;
/// Seconds per hour ("HR").
pub const SECS_PER_HOUR: i32 = 3600;
/// Seconds per minute ("MIN"/"SEC").
pub const SECS_PER_MINUTE: i32 = 60;
pub const MINS_PER_HOUR: i32 = 60;

pub const USECS_PER_DAY: i64 = 86_400_000_000;
pub const USECS_PER_HOUR: i64 = 3_600_000_000;
pub const USECS_PER_MINUTE: i64 = 60_000_000;
pub const USECS_PER_SEC: i64 = 1_000_000;

pub const MAX_TZDISP_HOUR: i32 = 15;
pub const TZDISP_LIMIT: i32 = (MAX_TZDISP_HOUR + 1) * SECS_PER_HOUR;

pub const MAX_TIMESTAMP_PRECISION: i32 = 6;
pub const MAX_INTERVAL_PRECISION: i32 = 6;
pub const MAX_TIME_PRECISION: i32 = 6;

// ---------------------------------------------------------------------------
// Fundamental time field definitions for parsing. (utils/datetime.h)
// ---------------------------------------------------------------------------

pub const AM: i32 = 0;
pub const PM: i32 = 1;
pub const HR24: i32 = 2;

pub const AD: i32 = 0;
pub const BC: i32 = 1;

// ---------------------------------------------------------------------------
// Field types for time decoding (the "field-type group"). (utils/datetime.h)
//
// Values for YEAR/MONTH/DAY/HOUR/MINUTE/SECOND must stay in 0..14 so the
// associated bitmasks fit in the left half of an INTERVAL's typmod value.
// ---------------------------------------------------------------------------

pub const RESERV: i32 = 0;
pub const MONTH: i32 = 1;
pub const YEAR: i32 = 2;
pub const DAY: i32 = 3;
pub const JULIAN: i32 = 4;
/// Fixed-offset timezone abbreviation.
pub const TZ: i32 = 5;
/// Fixed-offset timezone abbrev, DST.
pub const DTZ: i32 = 6;
/// Dynamic timezone abbreviation.
pub const DYNTZ: i32 = 7;
pub const IGNORE_DTF: i32 = 8;
pub const AMPM: i32 = 9;
pub const HOUR: i32 = 10;
pub const MINUTE: i32 = 11;
pub const SECOND: i32 = 12;
pub const MILLISECOND: i32 = 13;
pub const MICROSECOND: i32 = 14;
pub const DOY: i32 = 15;
pub const DOW: i32 = 16;
pub const UNITS: i32 = 17;
pub const ADBC: i32 = 18;
/* these are only for relative dates */
pub const AGO: i32 = 19;
pub const ABS_BEFORE: i32 = 20;
pub const ABS_AFTER: i32 = 21;
/* generic fields to help with parsing */
pub const ISODATE: i32 = 22;
pub const ISOTIME: i32 = 23;
/* these are only for parsing intervals */
pub const WEEK: i32 = 24;
pub const DECADE: i32 = 25;
pub const CENTURY: i32 = 26;
pub const MILLENNIUM: i32 = 27;
/// "DST" as a separate word.
pub const DTZMOD: i32 = 28;
/// Reserved for unrecognized string values.
pub const UNKNOWN_FIELD: i32 = 31;

// ---------------------------------------------------------------------------
// Token field definitions (the DTK_* codes). (utils/datetime.h)
// ---------------------------------------------------------------------------

pub const DTK_NUMBER: i32 = 0;
pub const DTK_STRING: i32 = 1;

pub const DTK_DATE: i32 = 2;
pub const DTK_TIME: i32 = 3;
pub const DTK_TZ: i32 = 4;
pub const DTK_AGO: i32 = 5;

pub const DTK_SPECIAL: i32 = 6;
pub const DTK_EARLY: i32 = 9;
pub const DTK_LATE: i32 = 10;
pub const DTK_EPOCH: i32 = 11;
pub const DTK_NOW: i32 = 12;
pub const DTK_YESTERDAY: i32 = 13;
pub const DTK_TODAY: i32 = 14;
pub const DTK_TOMORROW: i32 = 15;
pub const DTK_ZULU: i32 = 16;

pub const DTK_DELTA: i32 = 17;
pub const DTK_SECOND: i32 = 18;
pub const DTK_MINUTE: i32 = 19;
pub const DTK_HOUR: i32 = 20;
pub const DTK_DAY: i32 = 21;
pub const DTK_WEEK: i32 = 22;
pub const DTK_MONTH: i32 = 23;
pub const DTK_QUARTER: i32 = 24;
pub const DTK_YEAR: i32 = 25;
pub const DTK_DECADE: i32 = 26;
pub const DTK_CENTURY: i32 = 27;
pub const DTK_MILLENNIUM: i32 = 28;
pub const DTK_MILLISEC: i32 = 29;
pub const DTK_MICROSEC: i32 = 30;
pub const DTK_JULIAN: i32 = 31;

pub const DTK_DOW: i32 = 32;
pub const DTK_DOY: i32 = 33;
pub const DTK_TZ_HOUR: i32 = 34;
pub const DTK_TZ_MINUTE: i32 = 35;
pub const DTK_ISOYEAR: i32 = 36;
pub const DTK_ISODOW: i32 = 37;

/// Bit mask for a field type: `0x01 << t`. (utils/datetime.h)
#[inline]
pub const fn DTK_M(t: i32) -> i32 {
    0x01 << t
}

// ---------------------------------------------------------------------------
// Working-buffer / token sizes. (utils/datetime.h)
// ---------------------------------------------------------------------------

/// Working buffer size for input/output of interval, timestamp, etc.
pub const MAXDATELEN: i32 = 128;
/// Maximum possible number of fields in a date string.
pub const MAXDATEFIELDS: i32 = 25;
/// Only this many chars are stored in `datetktbl`.
pub const TOKMAXLEN: i32 = 10;

// ---------------------------------------------------------------------------
// Result codes for DecodeTimezoneName(). (utils/datetime.h)
// ---------------------------------------------------------------------------

pub const TZNAME_FIXED_OFFSET: i32 = 0;
pub const TZNAME_DYNTZ: i32 = 1;
pub const TZNAME_ZONE: i32 = 2;

// ---------------------------------------------------------------------------
// DateTimeParseError negative result codes. (utils/datetime.h)
// ---------------------------------------------------------------------------

pub const DTERR_BAD_FORMAT: i32 = -1;
pub const DTERR_FIELD_OVERFLOW: i32 = -2;
/// Triggers hint about DateStyle.
pub const DTERR_MD_FIELD_OVERFLOW: i32 = -3;
pub const DTERR_INTERVAL_OVERFLOW: i32 = -4;
pub const DTERR_TZDISP_OVERFLOW: i32 = -5;
pub const DTERR_BAD_TIMEZONE: i32 = -6;
pub const DTERR_BAD_ZONE_ABBREV: i32 = -7;

// ---------------------------------------------------------------------------
// DateStyle output styles. (miscadmin.h — these feed `DateStyle`)
// ---------------------------------------------------------------------------

pub const USE_POSTGRES_DATES: i32 = 0;
pub const USE_ISO_DATES: i32 = 1;
pub const USE_SQL_DATES: i32 = 2;
pub const USE_GERMAN_DATES: i32 = 3;
pub const USE_XSD_DATES: i32 = 4;

pub const DATEORDER_YMD: i32 = 0;
pub const DATEORDER_DMY: i32 = 1;
pub const DATEORDER_MDY: i32 = 2;

pub const INTSTYLE_POSTGRES: i32 = 0;
pub const INTSTYLE_POSTGRES_VERBOSE: i32 = 1;
pub const INTSTYLE_SQL_STANDARD: i32 = 2;
pub const INTSTYLE_ISO_8601: i32 = 3;

/// `MAXTZLEN` (`miscadmin.h`) — max TZ name len, not counting trailing null.
pub const MAXTZLEN: i32 = 10;

// ---------------------------------------------------------------------------
// Interval typmod (range/precision) packing. (utils/timestamp.h)
// ---------------------------------------------------------------------------

/// `INTERVAL_MASK(b)` == `1 << b`, where `b` is a field-type code
/// (YEAR/MONTH/DAY/HOUR/MINUTE/SECOND).
#[inline]
pub const fn INTERVAL_MASK(b: i32) -> i32 {
    1 << b
}

pub const INTERVAL_FULL_RANGE: i32 = 0x7FFF;
pub const INTERVAL_RANGE_MASK: i32 = 0x7FFF;
pub const INTERVAL_FULL_PRECISION: i32 = 0xFFFF;
pub const INTERVAL_PRECISION_MASK: i32 = 0xFFFF;

/// Pack precision `p` and range `r` into an interval typmod.
#[inline]
pub const fn INTERVAL_TYPMOD(p: i32, r: i32) -> i32 {
    ((r & INTERVAL_RANGE_MASK) << 16) | (p & INTERVAL_PRECISION_MASK)
}

/// Extract the precision component of an interval typmod.
#[inline]
pub const fn INTERVAL_PRECISION(t: i32) -> i32 {
    t & INTERVAL_PRECISION_MASK
}

/// Extract the range component of an interval typmod.
#[inline]
pub const fn INTERVAL_RANGE(t: i32) -> i32 {
    (t >> 16) & INTERVAL_RANGE_MASK
}

// ---------------------------------------------------------------------------
// Julian-date support and range limits. (datatype/timestamp.h)
// ---------------------------------------------------------------------------

pub const JULIAN_MINYEAR: i32 = -4713;
pub const JULIAN_MINMONTH: i32 = 11;
pub const JULIAN_MINDAY: i32 = 24;
pub const JULIAN_MAXYEAR: i32 = 5_874_898;
pub const JULIAN_MAXMONTH: i32 = 6;
pub const JULIAN_MAXDAY: i32 = 3;

pub const UNIX_EPOCH_JDATE: i32 = 2_440_588;
pub const POSTGRES_EPOCH_JDATE: i32 = 2_451_545;

pub const DATETIME_MIN_JULIAN: i32 = 0;
pub const DATE_END_JULIAN: i32 = 2_147_483_494;
pub const TIMESTAMP_END_JULIAN: i32 = 109_203_528;

pub const MIN_TIMESTAMP: i64 = -211_813_488_000_000_000;
pub const END_TIMESTAMP: i64 = 9_223_371_331_200_000_000;

// ---------------------------------------------------------------------------
// Static keyword-table entry. (utils/datetime.h)
// ---------------------------------------------------------------------------

/// A date/time keyword table entry, the idiomatic analogue of C `datetkn` as
/// used for the *static* `datetktbl` / `deltatktbl` tables.
///
/// In C this is `struct datetkn { char token[TOKMAXLEN+1]; char type; int32 value; }`
/// with `token` pre-truncated to `TOKMAXLEN` (10) chars by its initializer.
/// Here `token` is a `&'static str` (already truncated) and `type` / `value`
/// are plain `i32`; the decode engine only ever reads them, and `datebsearch`
/// compares the (already-truncated) token bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DateToken {
    /// The keyword, truncated to at most `TOKMAXLEN` (10) characters.
    pub token: &'static str,
    /// Field type code (`RESERV`, `MONTH`, `UNITS`, ...).
    pub r#type: i32,
    /// Meaning depends on `type`.
    pub value: i32,
}

// ---------------------------------------------------------------------------
// Seam-vocabulary carriers (formatting.c <-> datetime/timestamp/isoweek).
// ---------------------------------------------------------------------------

/// Year/month/day triple produced by the `j2date` / isoweek seams.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct YmdDate {
    pub year: i32,
    pub mon: i32,
    pub mday: i32,
}

/// An owned, seam-friendly handle to a resolved `pg_tz` (timezone) object.
///
/// In `formatting.c` the `TmFromChar.tzp` field and the
/// `DecodeTimezoneAbbrevPrefix` output are `pg_tz *` — pointers into the
/// timezone subsystem's interned zone table. The owned surface does not name
/// that `*mut pg_tz`; the timezone seam provider returns this stable id, which
/// it can later map back to its interned `pg_tz` when the DCH consumer asks for
/// `DetermineTimeZoneAbbrevOffset`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TzHandle(pub u32);

/// Result of the `decode_timezone_abbrev_prefix` seam — the owned form of C's
/// `DecodeTimezoneAbbrevPrefix(str, &gmtoffset, &tzp)` outputs (datetime.c).
#[derive(Clone, Copy, Debug, Default)]
pub struct TzAbbrevMatch {
    /// Number of input bytes consumed (`> 0` on a match, `<= 0` on no match).
    pub tzlen: i32,
    /// For a fixed-offset abbreviation: the GMT offset in seconds.
    pub gmtoffset: i32,
    /// For a dynamic abbreviation: the resolved-later timezone handle. `None`
    /// for a fixed-offset abbreviation (or no match).
    pub tzp: Option<TzHandle>,
}

/// Result of the `timestamp2tm` seam — the owned form of C's
/// `timestamp2tm(dt, &tzp, tm, &fsec, &tzn, attimezone)` outputs (timestamp.c).
#[derive(Clone, Debug, Default)]
pub struct Timestamp2TmResult {
    /// The broken-down time.
    pub tm: types_pgtime::pg_tm,
    /// Fractional seconds (microseconds).
    pub fsec: fsec_t,
    /// GMT offset (seconds), when the caller requested timezone resolution.
    pub tz: i32,
    /// Zone-abbreviation name, when the caller requested it.
    pub tzn: Option<String>,
}
