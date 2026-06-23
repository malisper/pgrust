//! `in_range` support cores for window-frame `RANGE ... PRECEDING/FOLLOWING`
//! boundaries, ported from `timestamp.c` / `date.c` (idiomatic, safe Rust).
//!
//! For each `(val, base, offset, sub, less)` we compute `base +/- offset`
//! (the frame boundary) using the ported timestamp/interval arithmetic, then
//! return `val <= boundary` (when `less`) or `val >= boundary` (otherwise).
//!
//! Mirrors:
//!   * `in_range_timestamptz_interval()` (timestamp.c:3857)
//!   * `in_range_timestamp_interval()`   (timestamp.c:3894)
//!   * `in_range_interval_interval()`    (timestamp.c:3935)
//!   * `in_range_date_interval()`        (date.c:1104)
//!   * `in_range_time_interval()`        (date.c:2163)
//!   * `in_range_timetz_interval()`      (date.c:2715)
//!
//! Per SQL spec the offset must be non-negative; we interpret that via
//! `interval_sign()` exactly as the C operators do (and, for time/timetz, via
//! the `offset->time < 0` test, since those disregard the month/day fields).

use types_datetime::{Interval, TimeTzADT};
use ::types_error::ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE;
use types_datetime::{DateADT, TimeADT, Timestamp, TimestampTz};
use types_error::{PgError};

use crate::date::date2timestamp;
use crate::interval::{
    interval_cmp_internal, interval_mi, interval_pl, interval_sign, INTERVAL_IS_NOBEGIN,
    INTERVAL_IS_NOEND,
};
use crate::timestamp::{
    timestamp_mi_interval, timestamp_pl_interval, timestamptz_mi_interval_internal,
    timestamptz_pl_interval_internal, DtResult, TIMESTAMP_IS_NOBEGIN, TIMESTAMP_IS_NOEND,
};
use crate::timetz::timetz_cmp_internal;

/// "invalid preceding or following size in window function"
/// (`ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE`).
fn invalid_preceding_or_following_size() -> PgError {
    PgError::error("invalid preceding or following size in window function")
        .with_sqlstate(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE)
}

/// `in_range_timestamptz_interval()` (timestamp.c:3857) CORE.
pub fn in_range_timestamptz_interval(
    val: TimestampTz,
    base: TimestampTz,
    offset: &Interval,
    sub: bool,
    less: bool,
) -> DtResult<bool> {
    if interval_sign(offset) < 0 {
        return Err(invalid_preceding_or_following_size());
    }

    // Deal with cases where both base and offset are infinite.  As for float and
    // numeric types, we assume that all values infinitely precede +infinity and
    // infinitely follow -infinity.
    if INTERVAL_IS_NOEND(offset)
        && (if sub {
            TIMESTAMP_IS_NOEND(base)
        } else {
            TIMESTAMP_IS_NOBEGIN(base)
        })
    {
        return Ok(true);
    }

    let sum = if sub {
        timestamptz_mi_interval_internal(base, offset, None)?
    } else {
        timestamptz_pl_interval_internal(base, offset, None)?
    };

    if less {
        Ok(val <= sum)
    } else {
        Ok(val >= sum)
    }
}

/// `in_range_timestamp_interval()` (timestamp.c:3894) CORE.
pub fn in_range_timestamp_interval(
    val: Timestamp,
    base: Timestamp,
    offset: &Interval,
    sub: bool,
    less: bool,
) -> DtResult<bool> {
    if interval_sign(offset) < 0 {
        return Err(invalid_preceding_or_following_size());
    }

    if INTERVAL_IS_NOEND(offset)
        && (if sub {
            TIMESTAMP_IS_NOEND(base)
        } else {
            TIMESTAMP_IS_NOBEGIN(base)
        })
    {
        return Ok(true);
    }

    let sum = if sub {
        timestamp_mi_interval(base, offset)?
    } else {
        timestamp_pl_interval(base, offset)?
    };

    if less {
        Ok(val <= sum)
    } else {
        Ok(val >= sum)
    }
}

/// `in_range_interval_interval()` (timestamp.c:3935) CORE.
pub fn in_range_interval_interval(
    val: &Interval,
    base: &Interval,
    offset: &Interval,
    sub: bool,
    less: bool,
) -> DtResult<bool> {
    if interval_sign(offset) < 0 {
        return Err(invalid_preceding_or_following_size());
    }

    if INTERVAL_IS_NOEND(offset)
        && (if sub {
            INTERVAL_IS_NOEND(base)
        } else {
            INTERVAL_IS_NOBEGIN(base)
        })
    {
        return Ok(true);
    }

    let sum = if sub {
        interval_mi(base, offset)?
    } else {
        interval_pl(base, offset)?
    };

    if less {
        Ok(interval_cmp_internal(val, &sum) <= 0)
    } else {
        Ok(interval_cmp_internal(val, &sum) >= 0)
    }
}

/// `in_range_date_interval()` (date.c:1104) CORE.
pub fn in_range_date_interval(
    val: DateADT,
    base: DateADT,
    offset: &Interval,
    sub: bool,
    less: bool,
) -> DtResult<bool> {
    let val_stamp = date2timestamp(val)?;
    let base_stamp = date2timestamp(base)?;

    in_range_timestamp_interval(val_stamp, base_stamp, offset, sub, less)
}

/// `in_range_time_interval()` (date.c:2163) CORE.
pub fn in_range_time_interval(
    val: TimeADT,
    base: TimeADT,
    offset: &Interval,
    sub: bool,
    less: bool,
) -> DtResult<bool> {
    // Like time_pl_interval/time_mi_interval, we disregard the month and day
    // fields of the offset.  This also catches -infinity.
    if offset.time < 0 {
        return Err(invalid_preceding_or_following_size());
    }

    // Adding an infinite (or very large) interval might cause integer overflow;
    // subtraction cannot overflow here.
    let sum = if sub {
        base - offset.time
    } else {
        match base.checked_add(offset.time) {
            Some(sum) => sum,
            None => return Ok(less),
        }
    };

    if less {
        Ok(val <= sum)
    } else {
        Ok(val >= sum)
    }
}

/// `in_range_timetz_interval()` (date.c:2715) CORE.
pub fn in_range_timetz_interval(
    val: &TimeTzADT,
    base: &TimeTzADT,
    offset: &Interval,
    sub: bool,
    less: bool,
) -> DtResult<bool> {
    if offset.time < 0 {
        return Err(invalid_preceding_or_following_size());
    }

    let sum_time = if sub {
        base.time - offset.time
    } else {
        match base.time.checked_add(offset.time) {
            Some(sum) => sum,
            None => return Ok(less),
        }
    };
    let sum = TimeTzADT {
        time: sum_time,
        zone: base.zone,
    };

    if less {
        Ok(timetz_cmp_internal(val, &sum) <= 0)
    } else {
        Ok(timetz_cmp_internal(val, &sum) >= 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::types_datetime::USECS_PER_HOUR;

    fn hours_interval(n: i64) -> Interval {
        Interval {
            time: n * USECS_PER_HOUR,
            day: 0,
            month: 0,
        }
    }

    fn ts(hours: i64) -> Timestamp {
        hours * USECS_PER_HOUR
    }

    #[test]
    fn timestamp_range_preceding() {
        let base = ts(10);
        let offset = hours_interval(2); // boundary = 8
        assert!(in_range_timestamp_interval(ts(9), base, &offset, true, false).unwrap());
        assert!(!in_range_timestamp_interval(ts(7), base, &offset, true, false).unwrap());
        assert!(in_range_timestamp_interval(ts(8), base, &offset, true, false).unwrap());
    }

    #[test]
    fn timestamp_range_following() {
        let base = ts(10);
        let offset = hours_interval(2); // boundary = 12
        assert!(in_range_timestamp_interval(ts(11), base, &offset, false, true).unwrap());
        assert!(!in_range_timestamp_interval(ts(13), base, &offset, false, true).unwrap());
    }

    #[test]
    fn negative_offset_is_rejected() {
        let neg = hours_interval(-1);
        let err = in_range_timestamp_interval(ts(1), ts(1), &neg, false, true).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE);
    }

    #[test]
    fn timestamptz_range_preceding_following() {
        let base = ts(100);
        let offset = hours_interval(5);
        assert!(in_range_timestamptz_interval(ts(96), base, &offset, true, false).unwrap());
        assert!(!in_range_timestamptz_interval(ts(110), base, &offset, false, true).unwrap());
    }

    #[test]
    fn time_range_preceding_following() {
        let h = |h: i64| h * USECS_PER_HOUR;
        let base = h(10);
        let offset = hours_interval(2);
        assert!(in_range_time_interval(h(9), base, &offset, true, false).unwrap());
        assert!(!in_range_time_interval(h(13), base, &offset, false, true).unwrap());
        let neg = hours_interval(-1);
        assert_eq!(
            in_range_time_interval(h(1), base, &neg, false, true)
                .unwrap_err()
                .sqlstate(),
            ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE
        );
    }

    #[test]
    fn timetz_range_following() {
        let tz = |h: i64| TimeTzADT {
            time: h * USECS_PER_HOUR,
            zone: 0,
        };
        let base = tz(10);
        let offset = hours_interval(2);
        let val = tz(11);
        assert!(in_range_timetz_interval(&val, &base, &offset, false, true).unwrap());
        assert!(!in_range_timetz_interval(&tz(13), &base, &offset, false, true).unwrap());
    }

    #[test]
    fn interval_range_following() {
        let val = hours_interval(11);
        let base = hours_interval(10);
        let offset = hours_interval(2);
        assert!(in_range_interval_interval(&val, &base, &offset, false, true).unwrap());
        assert!(
            !in_range_interval_interval(&hours_interval(13), &base, &offset, false, true).unwrap()
        );
    }

    #[test]
    fn date_range_following() {
        let one_day = Interval {
            time: 0,
            day: 1,
            month: 0,
        };
        assert!(in_range_date_interval(100, 100, &one_day, false, true).unwrap());
        assert!(!in_range_date_interval(102, 100, &one_day, false, true).unwrap());
    }
}
