pub use types_core::{
    TimestampTz, DATEORDER_DMY, DATEORDER_MDY, DATEORDER_YMD, INTSTYLE_POSTGRES,
    USE_GERMAN_DATES, USE_ISO_DATES, USE_POSTGRES_DATES, USE_SQL_DATES,
};

#[allow(non_camel_case_types)]
pub type fsec_t = i32;
pub type Timestamp = i64;
pub type TimeOffset = i64;

pub const INTSTYLE_POSTGRES_VERBOSE: i32 = 1;
pub const INTSTYLE_SQL_STANDARD: i32 = 2;
pub const INTSTYLE_ISO_8601: i32 = 3;
pub const USE_XSD_DATES: i32 = 4;

pub const AM: i32 = 0;
pub const PM: i32 = 1;
pub const HR24: i32 = 2;

pub const AD: i32 = 0;
pub const BC: i32 = 1;

pub const RESERV: i32 = 0;
pub const MONTH: i32 = 1;
pub const YEAR: i32 = 2;
pub const DAY: i32 = 3;
pub const JULIAN: i32 = 4;
pub const TZ: i32 = 5;
pub const DTZ: i32 = 6;
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
pub const AGO: i32 = 19;
pub const ABS_BEFORE: i32 = 20;
pub const ABS_AFTER: i32 = 21;
pub const ISODATE: i32 = 22;
pub const ISOTIME: i32 = 23;
pub const WEEK: i32 = 24;
pub const DECADE: i32 = 25;
pub const CENTURY: i32 = 26;
pub const MILLENNIUM: i32 = 27;
pub const DTZMOD: i32 = 28;
pub const UNKNOWN_FIELD: i32 = 31;

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

#[allow(non_snake_case)]
#[inline(always)]
pub const fn DTK_M(t: i32) -> i32 {
    0x01 << t
}

pub const DTK_ALL_SECS_M: i32 = DTK_M(SECOND) | DTK_M(MILLISECOND) | DTK_M(MICROSECOND);
pub const DTK_DATE_M: i32 = DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY);
pub const DTK_TIME_M: i32 = DTK_M(HOUR) | DTK_M(MINUTE) | DTK_ALL_SECS_M;

pub const MAXDATELEN: usize = 128;
pub const MAXDATEFIELDS: usize = 25;
pub const TOKMAXLEN: usize = 10;
pub const MAXTZLEN: usize = 10;

pub const DTERR_BAD_FORMAT: i32 = -1;
pub const DTERR_FIELD_OVERFLOW: i32 = -2;
pub const DTERR_MD_FIELD_OVERFLOW: i32 = -3;
pub const DTERR_INTERVAL_OVERFLOW: i32 = -4;
pub const DTERR_TZDISP_OVERFLOW: i32 = -5;
pub const DTERR_BAD_TIMEZONE: i32 = -6;
pub const DTERR_BAD_ZONE_ABBREV: i32 = -7;

pub const TZNAME_FIXED_OFFSET: i32 = 0;
pub const TZNAME_DYNTZ: i32 = 1;
pub const TZNAME_ZONE: i32 = 2;

pub const MONTHS_PER_YEAR: i32 = 12;
pub const DAYS_PER_MONTH: i32 = 30;
pub const DAYS_PER_WEEK: i32 = 7;
pub const HOURS_PER_DAY: i32 = 24;
pub const MINS_PER_HOUR: i32 = 60;
pub const SECS_PER_MINUTE: i32 = 60;
pub const SECS_PER_HOUR: i32 = 3600;
pub const SECS_PER_DAY: i32 = 86400;
pub const USECS_PER_DAY: i64 = 86_400_000_000;
pub const USECS_PER_HOUR: i64 = 3_600_000_000;
pub const USECS_PER_MINUTE: i64 = 60_000_000;
pub const USECS_PER_SEC: i64 = 1_000_000;

pub const MAX_TZDISP_HOUR: i32 = 15;
pub const TZDISP_LIMIT: i32 = (MAX_TZDISP_HOUR + 1) * SECS_PER_HOUR;
pub const MAX_TIMESTAMP_PRECISION: i32 = 6;
pub const MAX_TIME_PRECISION: i32 = 6;
pub const MAX_INTERVAL_PRECISION: i32 = 6;

pub const JULIAN_MINYEAR: i32 = -4713;
pub const JULIAN_MINMONTH: i32 = 11;
pub const JULIAN_MINDAY: i32 = 24;
pub const JULIAN_MAXYEAR: i32 = 5874898;
pub const JULIAN_MAXMONTH: i32 = 6;
pub const JULIAN_MAXDAY: i32 = 3;

pub const UNIX_EPOCH_JDATE: i32 = 2440588;
pub const POSTGRES_EPOCH_JDATE: i32 = 2451545;

#[allow(non_snake_case)]
#[inline(always)]
pub const fn IS_VALID_JULIAN(y: i32, m: i32, _d: i32) -> bool {
    (y > JULIAN_MINYEAR || (y == JULIAN_MINYEAR && m >= JULIAN_MINMONTH))
        && (y < JULIAN_MAXYEAR || (y == JULIAN_MAXYEAR && m < JULIAN_MAXMONTH))
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct pg_tm {
    pub tm_sec: i32,
    pub tm_min: i32,
    pub tm_hour: i32,
    pub tm_mday: i32,
    /// origin 1, not 0 (datetime convention; differs from POSIX struct tm)
    pub tm_mon: i32,
    /// full year, not year-1900
    pub tm_year: i32,
    pub tm_wday: i32,
    pub tm_yday: i32,
    pub tm_isdst: i32,
    pub tm_gmtoff: i64,
    pub tm_zone: Option<&'static str>,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct pg_itm {
    pub tm_usec: i32,
    pub tm_sec: i32,
    pub tm_min: i32,
    pub tm_hour: i64,
    pub tm_mday: i32,
    pub tm_mon: i32,
    pub tm_year: i32,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct pg_itm_in {
    pub tm_usec: i64,
    pub tm_mday: i32,
    pub tm_mon: i32,
    pub tm_year: i32,
}

/// typlen 16, typalign d: time(8) + day(4) + month(4), field order per
/// datatype/timestamp.h; all-min/all-max field values encode -/+infinity.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Interval {
    pub time: i64,
    pub day: i32,
    pub month: i32,
}

const _: () = {
    assert!(core::mem::size_of::<Interval>() == 16);
    assert!(core::mem::offset_of!(Interval, time) == 0);
    assert!(core::mem::offset_of!(Interval, day) == 8);
    assert!(core::mem::offset_of!(Interval, month) == 12);
};

impl Interval {
    pub const NOBEGIN: Interval = Interval { time: i64::MIN, day: i32::MIN, month: i32::MIN };
    pub const NOEND: Interval = Interval { time: i64::MAX, day: i32::MAX, month: i32::MAX };

    #[inline]
    pub const fn is_nobegin(&self) -> bool {
        self.month == i32::MIN && self.day == i32::MIN && self.time == i64::MIN
    }

    #[inline]
    pub const fn is_noend(&self) -> bool {
        self.month == i32::MAX && self.day == i32::MAX && self.time == i64::MAX
    }

    #[inline]
    pub const fn not_finite(&self) -> bool {
        self.is_nobegin() || self.is_noend()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DateTimeErrorExtra<'a> {
    pub dtee_timezone: Option<&'a [u8]>,
    pub dtee_abbrev: Option<&'a [u8]>,
}

#[derive(Clone, Copy, Debug)]
pub struct DateTkn {
    pub token: [u8; TOKMAXLEN + 1],
    pub typ: i8,
    pub value: i32,
}

const _: () = assert!(core::mem::size_of::<DateTkn>() == 16);

impl DateTkn {
    #[inline]
    pub fn token_bytes(&self) -> &[u8] {
        let len = self.token.iter().position(|&b| b == 0).unwrap_or(TOKMAXLEN + 1);
        &self.token[..len]
    }
}

// INTERVAL typmod range masks (utils/timestamp.h); DecodeTimeCommon keys on them.
#[allow(non_snake_case)]
#[inline(always)]
pub const fn INTERVAL_MASK(b: i32) -> i32 {
    1 << b
}
pub const INTERVAL_FULL_RANGE: i32 = 0x7FFF;
