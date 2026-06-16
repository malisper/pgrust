//! Date/time scalar vocabulary (`datatype/timestamp.h`, `datatype/date.h`,
//! `utils/datetime.h`), trimmed to the items ports consume so far.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::string::String;

// ---------------------------------------------------------------------------
// Scalar ADT types (datatype/timestamp.h, datatype/date.h).
// ---------------------------------------------------------------------------

/// `typedef int64 Timestamp;`
pub type Timestamp = i64;
/// `typedef int64 TimestampTz;`
pub type TimestampTz = i64;
/// `typedef int64 TimeOffset;`
pub type TimeOffset = i64;
/// `typedef int32 fsec_t;` — fractional seconds, in microseconds.
pub type fsec_t = i32;
/// `typedef int32 DateADT;`
pub type DateADT = i32;
/// `typedef int64 TimeADT;`
pub type TimeADT = i64;

/// C: `struct Interval` (datatype/timestamp.h).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Interval {
    /// All time units other than days, months and years.
    pub time: TimeOffset,
    /// Days, after `time` for alignment.
    pub day: i32,
    /// Months and years, after `time` for alignment.
    pub month: i32,
}

/// C: `struct pg_itm` (datatype/timestamp.h) — broken-down interval.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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

/// C: `struct TimeTzADT` (datatype/date.h).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimeTzADT {
    /// All time units other than months and years.
    pub time: TimeADT,
    /// Numeric time zone, in seconds.
    pub zone: i32,
}

/// C: the extra detail filled in by `DateTimeParseError` callers
/// (`struct DateTimeErrorExtra`, utils/datetime.h).
#[derive(Clone, Debug, Default)]
pub struct DateTimeErrorExtra {
    /// Incorrect time zone name (DTERR_BAD_TIMEZONE / DTERR_BAD_ZONE_ABBREV).
    pub dtee_timezone: Option<String>,
    /// Relevant time zone abbreviation (DTERR_BAD_ZONE_ABBREV).
    pub dtee_abbrev: Option<String>,
}

// ---------------------------------------------------------------------------
// Infinity sentinels (datatype/timestamp.h).
// ---------------------------------------------------------------------------

pub const TIMESTAMP_MINUS_INFINITY: i64 = i64::MIN;
pub const TIMESTAMP_INFINITY: i64 = i64::MAX;
pub const DT_NOBEGIN: i64 = TIMESTAMP_MINUS_INFINITY;
pub const DT_NOEND: i64 = TIMESTAMP_INFINITY;

// ---------------------------------------------------------------------------
// Unit constants (datatype/timestamp.h).
// ---------------------------------------------------------------------------

pub const MONTHS_PER_YEAR: i32 = 12;
pub const DAYS_PER_MONTH: i32 = 30;
pub const DAYS_PER_WEEK: i32 = 7;
pub const HOURS_PER_DAY: i32 = 24;
pub const SECS_PER_DAY: i32 = 86400;
pub const SECS_PER_HOUR: i32 = 3600;
pub const SECS_PER_MINUTE: i32 = 60;
pub const MINS_PER_HOUR: i32 = 60;
pub const USECS_PER_DAY: i64 = 86_400_000_000;
pub const USECS_PER_HOUR: i64 = 3_600_000_000;
pub const USECS_PER_MINUTE: i64 = 60_000_000;
pub const USECS_PER_SEC: i64 = 1_000_000;
pub const MAX_TZDISP_HOUR: i32 = 15;

// ---------------------------------------------------------------------------
// DateTimeParseError codes (utils/datetime.h).
// ---------------------------------------------------------------------------

pub const DTERR_BAD_FORMAT: i32 = -1;
pub const DTERR_FIELD_OVERFLOW: i32 = -2;
pub const DTERR_MD_FIELD_OVERFLOW: i32 = -3;
pub const DTERR_INTERVAL_OVERFLOW: i32 = -4;
pub const DTERR_TZDISP_OVERFLOW: i32 = -5;
pub const DTERR_BAD_TIMEZONE: i32 = -6;
pub const DTERR_BAD_ZONE_ABBREV: i32 = -7;

// ---------------------------------------------------------------------------
// Julian date range constants (datatype/timestamp.h).
// ---------------------------------------------------------------------------

pub const JULIAN_MINYEAR: i32 = -4713;
pub const JULIAN_MINMONTH: i32 = 11;
pub const JULIAN_MINDAY: i32 = 24;
pub const JULIAN_MAXYEAR: i32 = 5_874_898;
pub const JULIAN_MAXMONTH: i32 = 6;
pub const JULIAN_MAXDAY: i32 = 3;
pub const POSTGRES_EPOCH_JDATE: i32 = 2_451_545;
/// `UNIX_EPOCH_JDATE == date2j(1970,1,1)` (datatype/timestamp.h).
pub const UNIX_EPOCH_JDATE: i32 = 2_440_588;
pub const DATETIME_MIN_JULIAN: i32 = 0;
pub const DATE_END_JULIAN: i32 = 2_147_483_494;

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
