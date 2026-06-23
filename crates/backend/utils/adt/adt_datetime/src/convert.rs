//! Broken-down-time conversion cores shared across the date/time subsystem,
//! ported from `src/backend/utils/adt/timestamp.c` plus the range-check macros
//! from `src/include/datatype/timestamp.h` / `src/include/utils/date.h`.
//!
//! These are the tiny, pure-arithmetic, seam-free helpers that the decode
//! engine (`decode.rs`) and the value-type modules (`date`/`time`/`timestamp`)
//! all share: the Julian/date range checks (`IS_VALID_JULIAN`/`IS_VALID_DATE`),
//! the microsecond-of-day split/compose (`dt2time`/`time2t`), the timezone
//! shift (`dt2local`), and the `TimestampTz` -> `pg_time_t` reduction
//! (`timestamptz_to_time_t`).  They are ported here (ahead of the full
//! timestamp/date modules) so that the decode engine has its prerequisite cores
//! in one canonical, seam-free home.  When the full `timestamp.rs`/`date.rs`
//! modules land, they re-export from here rather than redefining.
//!
//! `dt2time`/`time2t` write/read the hour/min/sec/fsec fields via `&mut i32` /
//! by value (the idiomatic analogue of the C out-pointers).
//!
//! Idiomatic surface: plain `i32`/`i64`, owned values.  No raw pointers,
//! `extern "C"`, `c_int`, or `libc`.

use ::types_core::pg_time_t;
use ::types_datetime::{
    DATETIME_MIN_JULIAN, DATE_END_JULIAN, JULIAN_MAXMONTH, JULIAN_MAXYEAR, JULIAN_MINMONTH,
    JULIAN_MINYEAR, MINS_PER_HOUR, POSTGRES_EPOCH_JDATE, SECS_PER_DAY, SECS_PER_MINUTE,
    UNIX_EPOCH_JDATE, USECS_PER_HOUR, USECS_PER_MINUTE, USECS_PER_SEC,
};
use ::types_datetime::{fsec_t, DateADT, Timestamp, TimestampTz};

// ---------------------------------------------------------------------------
// Range-check macros (datatype/timestamp.h, utils/date.h)
// ---------------------------------------------------------------------------

/// `IS_VALID_JULIAN(y, m, d)` (`datatype/timestamp.h`).  Mirrors the C macro:
/// the day argument is unused, only year/month bound the range.
#[inline]
pub fn IS_VALID_JULIAN(y: i32, m: i32, _d: i32) -> bool {
    (y > JULIAN_MINYEAR || (y == JULIAN_MINYEAR && m >= JULIAN_MINMONTH))
        && (y < JULIAN_MAXYEAR || (y == JULIAN_MAXYEAR && m < JULIAN_MAXMONTH))
}

/// `IS_VALID_DATE(d)` (`datatype/timestamp.h`) -- range-check a Postgres-numbered
/// date.
#[inline]
pub fn IS_VALID_DATE(d: DateADT) -> bool {
    ((DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE)..(DATE_END_JULIAN - POSTGRES_EPOCH_JDATE))
        .contains(&d)
}

// ---------------------------------------------------------------------------
// dt2time / time2t / dt2local
// ---------------------------------------------------------------------------

/// `dt2time()` -- split a microsecond-of-day count into hour/min/sec/fsec.
/// (`utils/adt/timestamp.c`)
pub fn dt2time(jd: Timestamp, hour: &mut i32, min: &mut i32, sec: &mut i32, fsec: &mut fsec_t) {
    let mut time = jd;

    *hour = (time / USECS_PER_HOUR) as i32;
    time -= (*hour as i64) * USECS_PER_HOUR;
    *min = (time / USECS_PER_MINUTE) as i32;
    time -= (*min as i64) * USECS_PER_MINUTE;
    *sec = (time / USECS_PER_SEC) as i32;
    *fsec = (time - (*sec as i64) * USECS_PER_SEC) as fsec_t;
}

/// `time2t()` -- compose hour/min/sec/fsec into a microsecond-of-day count.
/// (`utils/adt/timestamp.c`)
pub fn time2t(hour: i32, min: i32, sec: i32, fsec: fsec_t) -> Timestamp {
    ((((((hour as i64) * MINS_PER_HOUR as i64) + min as i64) * SECS_PER_MINUTE as i64)
        + sec as i64)
        * USECS_PER_SEC)
        + fsec as i64
}

/// `dt2local()` -- shift `dt` by `timezone` seconds.  (`utils/adt/timestamp.c`)
///
/// C computes `dt -= (timezone * USECS_PER_SEC)` with signed (wrapping) integer
/// arithmetic; the caller's `IS_VALID_TIMESTAMP` check rejects an out-of-range
/// result, so use `wrapping_*` here to match C's wrap and avoid a debug panic.
pub fn dt2local(dt: Timestamp, timezone: i32) -> Timestamp {
    dt.wrapping_sub((timezone as i64).wrapping_mul(USECS_PER_SEC))
}

/// `timestamptz_to_time_t()` -- convert a `TimestampTz` (microseconds since the
/// PG epoch, UTC) to a `pg_time_t` (seconds since the Unix epoch).  Used to
/// pick the probe instant for dynamic-abbreviation resolution.
/// (`utils/adt/timestamp.c`)
pub fn timestamptz_to_time_t(t: TimestampTz) -> pg_time_t {
    (t / USECS_PER_SEC + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY as i64)
        as pg_time_t
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_valid_julian_bounds() {
        // A normal year is valid.
        assert!(IS_VALID_JULIAN(2000, 1, 1));
        // The min boundary: year -4713, month 11 is the first valid month.
        assert!(IS_VALID_JULIAN(JULIAN_MINYEAR, JULIAN_MINMONTH, 1));
        assert!(!IS_VALID_JULIAN(JULIAN_MINYEAR, JULIAN_MINMONTH - 1, 1));
        assert!(!IS_VALID_JULIAN(JULIAN_MINYEAR - 1, 12, 1));
        // The max boundary: year 5874898, month 6 (JULIAN_MAXMONTH) is out.
        assert!(IS_VALID_JULIAN(JULIAN_MAXYEAR, JULIAN_MAXMONTH - 1, 1));
        assert!(!IS_VALID_JULIAN(JULIAN_MAXYEAR, JULIAN_MAXMONTH, 1));
        assert!(!IS_VALID_JULIAN(JULIAN_MAXYEAR + 1, 1, 1));
    }

    #[test]
    fn is_valid_date_bounds() {
        // Epoch day (2000-01-01) is day 0, inside the range.
        assert!(IS_VALID_DATE(0));
        assert!(!IS_VALID_DATE(DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE - 1));
        assert!(!IS_VALID_DATE(DATE_END_JULIAN - POSTGRES_EPOCH_JDATE));
    }

    #[test]
    fn dt2time_time2t_round_trip() {
        // 12:34:56.789000 as microseconds of day.
        let usec_of_day = time2t(12, 34, 56, 789_000);
        let mut h = 0;
        let mut m = 0;
        let mut s = 0;
        let mut f: fsec_t = 0;
        dt2time(usec_of_day, &mut h, &mut m, &mut s, &mut f);
        assert_eq!((h, m, s, f), (12, 34, 56, 789_000));
    }

    #[test]
    fn dt2local_shifts_by_seconds() {
        // Shifting by 3600s (1 hour east) subtracts one hour of microseconds.
        assert_eq!(dt2local(0, 3600), -USECS_PER_HOUR);
        assert_eq!(dt2local(0, -3600), USECS_PER_HOUR);
    }

    #[test]
    fn timestamptz_to_time_t_epoch() {
        // The PG epoch (TimestampTz 0 == 2000-01-01 00:00 UTC) maps to the Unix
        // seconds of 2000-01-01.
        let expected = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY as i64;
        assert_eq!(timestamptz_to_time_t(0), expected);
        // One second past the epoch.
        assert_eq!(timestamptz_to_time_t(USECS_PER_SEC), expected + 1);
    }
}
