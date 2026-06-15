//! ABI structs and constants for the PostgreSQL date/time subsystem.
//!
//! These mirror the C declarations in
//! `src/include/datatype/timestamp.h`, `src/include/utils/{date,timestamp,datetime}.h`.
//! Layout is locked down with const-assert size/offset gates so the structs stay
//! ABI-compatible with the C definitions on the same target.
//!
//! `pg_tm` / `pg_tz` are intentionally NOT defined here; they are reused from
//! `backend_timezone_localtime`.

use core::ffi::{c_char, c_int};

use crate::types::{TimeADT, TimeOffset};

// ---------------------------------------------------------------------------
// const-assert helper
// ---------------------------------------------------------------------------

/// Compile-time assertion: forces a build error when `$cond` is false.
macro_rules! const_assert {
    ($cond:expr) => {
        const _: [(); 0 - !{
            const ASSERT: bool = $cond;
            ASSERT
        } as usize] = [];
    };
}

// ---------------------------------------------------------------------------
// Storage format for type interval.  (datatype/timestamp.h)
// ---------------------------------------------------------------------------

/// Storage format for type `interval`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Interval {
    /// All time units other than days, months and years.
    pub time: TimeOffset,
    /// Days, after `time` for alignment.
    pub day: i32,
    /// Months and years, after `time` for alignment.
    pub month: i32,
}

const_assert!(core::mem::size_of::<Interval>() == 16);
const_assert!(core::mem::align_of::<Interval>() == 8);
const_assert!(core::mem::offset_of!(Interval, time) == 0);
const_assert!(core::mem::offset_of!(Interval, day) == 8);
const_assert!(core::mem::offset_of!(Interval, month) == 12);

/// Broken-down interval; modeled on `struct pg_tm`. (datatype/timestamp.h)
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

const_assert!(core::mem::size_of::<pg_itm>() == 40);
const_assert!(core::mem::align_of::<pg_itm>() == 8);
const_assert!(core::mem::offset_of!(pg_itm, tm_usec) == 0);
const_assert!(core::mem::offset_of!(pg_itm, tm_sec) == 4);
const_assert!(core::mem::offset_of!(pg_itm, tm_min) == 8);
const_assert!(core::mem::offset_of!(pg_itm, tm_hour) == 16);
const_assert!(core::mem::offset_of!(pg_itm, tm_mday) == 24);
const_assert!(core::mem::offset_of!(pg_itm, tm_mon) == 28);
const_assert!(core::mem::offset_of!(pg_itm, tm_year) == 32);

/// Data structure for decoding intervals. (datatype/timestamp.h)
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct pg_itm_in {
    /// Needs to be wide.
    pub tm_usec: i64,
    pub tm_mday: i32,
    pub tm_mon: i32,
    pub tm_year: i32,
}

const_assert!(core::mem::size_of::<pg_itm_in>() == 24);
const_assert!(core::mem::align_of::<pg_itm_in>() == 8);
const_assert!(core::mem::offset_of!(pg_itm_in, tm_usec) == 0);
const_assert!(core::mem::offset_of!(pg_itm_in, tm_mday) == 8);
const_assert!(core::mem::offset_of!(pg_itm_in, tm_mon) == 12);
const_assert!(core::mem::offset_of!(pg_itm_in, tm_year) == 16);

// ---------------------------------------------------------------------------
// SQL "date" and "time" types.  (utils/date.h)
// ---------------------------------------------------------------------------

/// Storage format for type `timetz`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeTzADT {
    /// All time units other than months and years.
    pub time: TimeADT,
    /// Numeric time zone, in seconds.
    pub zone: i32,
    // 4 bytes of tail padding to reach align-8 size of 16.
}

const_assert!(core::mem::size_of::<TimeTzADT>() == 16);
const_assert!(core::mem::align_of::<TimeTzADT>() == 8);
const_assert!(core::mem::offset_of!(TimeTzADT, time) == 0);
const_assert!(core::mem::offset_of!(TimeTzADT, zone) == 8);

// ---------------------------------------------------------------------------
// datetkn + DateTimeErrorExtra.  (utils/datetime.h)
// ---------------------------------------------------------------------------

/// Token table entry for time parsing/decoding. (utils/datetime.h)
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct datetkn {
    /// Always NUL-terminated (`TOKMAXLEN + 1`).
    pub token: [c_char; 11],
    /// See field type codes above.
    pub r#type: c_char,
    /// Meaning depends on `type`; lands at offset 12 (no padding -- offset is already 4-aligned).
    pub value: i32,
}

const_assert!(core::mem::size_of::<datetkn>() == 16);
const_assert!(core::mem::align_of::<datetkn>() == 4);
const_assert!(core::mem::offset_of!(datetkn, token) == 0);
const_assert!(core::mem::offset_of!(datetkn, r#type) == 11);
const_assert!(core::mem::offset_of!(datetkn, value) == 12);

/// Auxiliary info carried out of datetime parse errors. (utils/datetime.h)
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct DateTimeErrorExtra {
    /// Incorrect time zone name (DTERR_BAD_TIMEZONE / DTERR_BAD_ZONE_ABBREV).
    pub dtee_timezone: *const c_char,
    /// Relevant time zone abbreviation (DTERR_BAD_ZONE_ABBREV).
    pub dtee_abbrev: *const c_char,
}

const_assert!(core::mem::size_of::<DateTimeErrorExtra>() == 16);
const_assert!(core::mem::align_of::<DateTimeErrorExtra>() == 8);
const_assert!(core::mem::offset_of!(DateTimeErrorExtra, dtee_timezone) == 0);
const_assert!(core::mem::offset_of!(DateTimeErrorExtra, dtee_abbrev) == 8);

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

pub const MONTHS_PER_YEAR: c_int = 12;
/// Assumes exactly 30 days per month ("MD").
pub const DAYS_PER_MONTH: c_int = 30;
pub const DAYS_PER_WEEK: c_int = 7;
pub const HOURS_PER_DAY: c_int = 24;

pub const SECS_PER_YEAR: c_int = 36525 * 864;
pub const SECS_PER_DAY: c_int = 86400;
/// Seconds per hour ("HR").
pub const SECS_PER_HOUR: c_int = 3600;
/// Seconds per minute ("MIN"/"SEC").
pub const SECS_PER_MINUTE: c_int = 60;
pub const MINS_PER_HOUR: c_int = 60;

pub const USECS_PER_DAY: i64 = 86_400_000_000;
pub const USECS_PER_HOUR: i64 = 3_600_000_000;
pub const USECS_PER_MINUTE: i64 = 60_000_000;
pub const USECS_PER_SEC: i64 = 1_000_000;

pub const MAX_TZDISP_HOUR: c_int = 15;
pub const TZDISP_LIMIT: c_int = (MAX_TZDISP_HOUR + 1) * SECS_PER_HOUR;

pub const MAX_TIMESTAMP_PRECISION: c_int = 6;
pub const MAX_INTERVAL_PRECISION: c_int = 6;
pub const MAX_TIME_PRECISION: c_int = 6;

// ---------------------------------------------------------------------------
// Fundamental time field definitions for parsing. (utils/datetime.h)
// ---------------------------------------------------------------------------

pub const AM: c_int = 0;
pub const PM: c_int = 1;
pub const HR24: c_int = 2;

pub const AD: c_int = 0;
pub const BC: c_int = 1;

// ---------------------------------------------------------------------------
// Field types for time decoding (the "field-type group"). (utils/datetime.h)
//
// Values for YEAR/MONTH/DAY/HOUR/MINUTE/SECOND must stay in 0..14 so the
// associated bitmasks fit in the left half of an INTERVAL's typmod value.
// ---------------------------------------------------------------------------

pub const RESERV: c_int = 0;
pub const MONTH: c_int = 1;
pub const YEAR: c_int = 2;
pub const DAY: c_int = 3;
pub const JULIAN: c_int = 4;
/// Fixed-offset timezone abbreviation.
pub const TZ: c_int = 5;
/// Fixed-offset timezone abbrev, DST.
pub const DTZ: c_int = 6;
/// Dynamic timezone abbreviation.
pub const DYNTZ: c_int = 7;
pub const IGNORE_DTF: c_int = 8;
pub const AMPM: c_int = 9;
pub const HOUR: c_int = 10;
pub const MINUTE: c_int = 11;
pub const SECOND: c_int = 12;
pub const MILLISECOND: c_int = 13;
pub const MICROSECOND: c_int = 14;
pub const DOY: c_int = 15;
pub const DOW: c_int = 16;
pub const UNITS: c_int = 17;
pub const ADBC: c_int = 18;
/* these are only for relative dates */
pub const AGO: c_int = 19;
pub const ABS_BEFORE: c_int = 20;
pub const ABS_AFTER: c_int = 21;
/* generic fields to help with parsing */
pub const ISODATE: c_int = 22;
pub const ISOTIME: c_int = 23;
/* these are only for parsing intervals */
pub const WEEK: c_int = 24;
pub const DECADE: c_int = 25;
pub const CENTURY: c_int = 26;
pub const MILLENNIUM: c_int = 27;
/// "DST" as a separate word.
pub const DTZMOD: c_int = 28;
/// Reserved for unrecognized string values.
pub const UNKNOWN_FIELD: c_int = 31;

// ---------------------------------------------------------------------------
// Token field definitions (the DTK_* codes). (utils/datetime.h)
// ---------------------------------------------------------------------------

pub const DTK_NUMBER: c_int = 0;
pub const DTK_STRING: c_int = 1;

pub const DTK_DATE: c_int = 2;
pub const DTK_TIME: c_int = 3;
pub const DTK_TZ: c_int = 4;
pub const DTK_AGO: c_int = 5;

pub const DTK_SPECIAL: c_int = 6;
pub const DTK_EARLY: c_int = 9;
pub const DTK_LATE: c_int = 10;
pub const DTK_EPOCH: c_int = 11;
pub const DTK_NOW: c_int = 12;
pub const DTK_YESTERDAY: c_int = 13;
pub const DTK_TODAY: c_int = 14;
pub const DTK_TOMORROW: c_int = 15;
pub const DTK_ZULU: c_int = 16;

pub const DTK_DELTA: c_int = 17;
pub const DTK_SECOND: c_int = 18;
pub const DTK_MINUTE: c_int = 19;
pub const DTK_HOUR: c_int = 20;
pub const DTK_DAY: c_int = 21;
pub const DTK_WEEK: c_int = 22;
pub const DTK_MONTH: c_int = 23;
pub const DTK_QUARTER: c_int = 24;
pub const DTK_YEAR: c_int = 25;
pub const DTK_DECADE: c_int = 26;
pub const DTK_CENTURY: c_int = 27;
pub const DTK_MILLENNIUM: c_int = 28;
pub const DTK_MILLISEC: c_int = 29;
pub const DTK_MICROSEC: c_int = 30;
pub const DTK_JULIAN: c_int = 31;

pub const DTK_DOW: c_int = 32;
pub const DTK_DOY: c_int = 33;
pub const DTK_TZ_HOUR: c_int = 34;
pub const DTK_TZ_MINUTE: c_int = 35;
pub const DTK_ISOYEAR: c_int = 36;
pub const DTK_ISODOW: c_int = 37;

/// Bit mask for a field type: `0x01 << t`. (utils/datetime.h)
#[inline]
pub const fn DTK_M(t: c_int) -> c_int {
    0x01 << t
}

// ---------------------------------------------------------------------------
// Working-buffer / token sizes. (utils/datetime.h)
// ---------------------------------------------------------------------------

/// Working buffer size for input/output of interval, timestamp, etc.
pub const MAXDATELEN: c_int = 128;
/// Maximum possible number of fields in a date string.
pub const MAXDATEFIELDS: c_int = 25;
/// Only this many chars are stored in `datetktbl`.
pub const TOKMAXLEN: c_int = 10;

// ---------------------------------------------------------------------------
// Result codes for DecodeTimezoneName(). (utils/datetime.h)
// ---------------------------------------------------------------------------

pub const TZNAME_FIXED_OFFSET: c_int = 0;
pub const TZNAME_DYNTZ: c_int = 1;
pub const TZNAME_ZONE: c_int = 2;

// ---------------------------------------------------------------------------
// DateTimeParseError negative result codes. (utils/datetime.h)
// ---------------------------------------------------------------------------

pub const DTERR_BAD_FORMAT: c_int = -1;
pub const DTERR_FIELD_OVERFLOW: c_int = -2;
/// Triggers hint about DateStyle.
pub const DTERR_MD_FIELD_OVERFLOW: c_int = -3;
pub const DTERR_INTERVAL_OVERFLOW: c_int = -4;
pub const DTERR_TZDISP_OVERFLOW: c_int = -5;
pub const DTERR_BAD_TIMEZONE: c_int = -6;
pub const DTERR_BAD_ZONE_ABBREV: c_int = -7;

// ---------------------------------------------------------------------------
// DateStyle output styles. (these feed `DateStyle`)
//
// `USE_ISO_DATES`, `DATEORDER_MDY`, and `INTSTYLE_POSTGRES` already exist in
// `types.rs`; re-export them here so all DateStyle/IntervalStyle constants are
// available from one module without duplicating their definitions.
// ---------------------------------------------------------------------------

pub use crate::types::{DATEORDER_MDY, INTSTYLE_POSTGRES, USE_ISO_DATES};

pub const USE_POSTGRES_DATES: c_int = 0;
// USE_ISO_DATES = 1 (re-exported from types.rs)
pub const USE_SQL_DATES: c_int = 2;
pub const USE_GERMAN_DATES: c_int = 3;
pub const USE_XSD_DATES: c_int = 4;

pub const DATEORDER_YMD: c_int = 0;
pub const DATEORDER_DMY: c_int = 1;
// DATEORDER_MDY = 2 (re-exported from types.rs)

// INTSTYLE_POSTGRES = 0 (re-exported from types.rs)
pub const INTSTYLE_POSTGRES_VERBOSE: c_int = 1;
pub const INTSTYLE_SQL_STANDARD: c_int = 2;
pub const INTSTYLE_ISO_8601: c_int = 3;

// ---------------------------------------------------------------------------
// Interval typmod (range/precision) packing. (utils/timestamp.h)
// ---------------------------------------------------------------------------

/// `INTERVAL_MASK(b)` == `1 << b`, where `b` is a field-type code
/// (YEAR/MONTH/DAY/HOUR/MINUTE/SECOND).
#[inline]
pub const fn INTERVAL_MASK(b: c_int) -> c_int {
    1 << b
}

pub const INTERVAL_FULL_RANGE: c_int = 0x7FFF;
pub const INTERVAL_RANGE_MASK: c_int = 0x7FFF;
pub const INTERVAL_FULL_PRECISION: c_int = 0xFFFF;
pub const INTERVAL_PRECISION_MASK: c_int = 0xFFFF;

/// Pack precision `p` and range `r` into an interval typmod.
#[inline]
pub const fn INTERVAL_TYPMOD(p: c_int, r: c_int) -> c_int {
    ((r & INTERVAL_RANGE_MASK) << 16) | (p & INTERVAL_PRECISION_MASK)
}

/// Extract the precision component of an interval typmod.
#[inline]
pub const fn INTERVAL_PRECISION(t: c_int) -> c_int {
    t & INTERVAL_PRECISION_MASK
}

/// Extract the range component of an interval typmod.
#[inline]
pub const fn INTERVAL_RANGE(t: c_int) -> c_int {
    (t >> 16) & INTERVAL_RANGE_MASK
}

// ---------------------------------------------------------------------------
// Julian-date support and range limits. (datatype/timestamp.h)
// ---------------------------------------------------------------------------

pub const JULIAN_MINYEAR: c_int = -4713;
pub const JULIAN_MINMONTH: c_int = 11;
pub const JULIAN_MINDAY: c_int = 24;
pub const JULIAN_MAXYEAR: c_int = 5_874_898;
pub const JULIAN_MAXMONTH: c_int = 6;
pub const JULIAN_MAXDAY: c_int = 3;

pub const UNIX_EPOCH_JDATE: c_int = 2_440_588;
pub const POSTGRES_EPOCH_JDATE: c_int = 2_451_545;

pub const DATETIME_MIN_JULIAN: c_int = 0;
pub const DATE_END_JULIAN: c_int = 2_147_483_494;
pub const TIMESTAMP_END_JULIAN: c_int = 109_203_528;

pub const MIN_TIMESTAMP: i64 = -211_813_488_000_000_000;
pub const END_TIMESTAMP: i64 = 9_223_371_331_200_000_000;
