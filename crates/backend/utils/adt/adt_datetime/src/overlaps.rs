//! SQL `OVERLAPS` operator cores, ported from `timestamp.c` / `date.c`
//! (idiomatic, safe Rust).
//!
//! `(s1, e1) OVERLAPS (s2, e2)` per the SQL spec.  The hard part is the NULL
//! handling: the spec requires a non-NULL answer in some cases where some of
//! the inputs are NULL.  Mirrors `overlaps_timestamp()` (timestamp.c:2690),
//! `overlaps_time()` (date.c:1848), and `overlaps_timetz()` (date.c:2760).
//!
//! In C the endpoints are passed as generic `Datum`s with separate `*IsNull`
//! flags.  In safe Rust we model each endpoint as an `Option<T>` (`None` == SQL
//! NULL) and return `Option<bool>` (`None` == SQL NULL / unknown).  The
//! comparison cores from the value modules supply the `>` / `<` tests.

use core::cmp::Ordering;

use ::types_datetime::TimeTzADT;
use ::types_datetime::{TimeADT, Timestamp};

use crate::time::time_cmp;
use crate::timestamp::timestamp_cmp_internal;
use crate::timetz::timetz_cmp_internal;

/// Shared OVERLAPS algorithm parameterized over a three-way comparison.
///
/// `cmp(a, b)` must return `Less`/`Equal`/`Greater` for `a < b`/`a == b`/
/// `a > b`.  `cmp` is only ever called on two non-NULL values.
fn overlaps_generic<T: Clone>(
    s1: Option<T>,
    e1: Option<T>,
    s2: Option<T>,
    e2: Option<T>,
    cmp: impl Fn(&T, &T) -> Ordering,
) -> Option<bool> {
    let mut ts1 = s1;
    let mut te1 = e1;
    let mut ts2 = s2;
    let mut te2 = e2;

    // Track the "IsNull" of the *end* endpoints separately because the swap for
    // a single-NULL interval moves the non-NULL endpoint into `ts` and marks
    // `te` as NULL (matching the C code's `teNIsNull = true`).
    let te1_is_null;
    let te2_is_null;

    // If both endpoints of interval 1 are null, the result is null (unknown).
    // If just one endpoint is null, take ts1 as the non-null one. Otherwise,
    // take ts1 as the lesser endpoint.
    if ts1.is_none() {
        te1.as_ref()?;
        // swap null for non-null
        ts1 = te1.take();
        te1_is_null = true;
    } else {
        te1_is_null = te1.is_none();
        if !te1_is_null {
            let a = ts1.as_ref().unwrap();
            let b = te1.as_ref().unwrap();
            if cmp(a, b) == Ordering::Greater {
                std::mem::swap(&mut ts1, &mut te1);
            }
        }
    }

    // Likewise for interval 2.
    if ts2.is_none() {
        te2.as_ref()?;
        ts2 = te2.take();
        te2_is_null = true;
    } else {
        te2_is_null = te2.is_none();
        if !te2_is_null {
            let a = ts2.as_ref().unwrap();
            let b = te2.as_ref().unwrap();
            if cmp(a, b) == Ordering::Greater {
                std::mem::swap(&mut ts2, &mut te2);
            }
        }
    }

    // At this point neither ts1 nor ts2 is null, so consider three cases.
    let ts1_ref = ts1.as_ref().unwrap();
    let ts2_ref = ts2.as_ref().unwrap();
    match cmp(ts1_ref, ts2_ref) {
        Ordering::Greater => {
            // This case is ts1 < te2 OR te1 < te2.
            if te2_is_null {
                return None;
            }
            if cmp(ts1_ref, te2.as_ref().unwrap()) == Ordering::Less {
                return Some(true);
            }
            if te1_is_null {
                return None;
            }
            // If te1 is not null then we had ts1 <= te1 above, and we just found
            // ts1 >= te2, hence te1 >= te2.
            Some(false)
        }
        Ordering::Less => {
            // This case is ts2 < te1 OR te2 < te1.
            if te1_is_null {
                return None;
            }
            if cmp(ts2_ref, te1.as_ref().unwrap()) == Ordering::Less {
                return Some(true);
            }
            if te2_is_null {
                return None;
            }
            Some(false)
        }
        Ordering::Equal => {
            // For ts1 = ts2: "true if both ends non-null, else null".
            if te1_is_null || te2_is_null {
                return None;
            }
            Some(true)
        }
    }
}

/// `overlaps_timestamp()` (timestamp.c:2690) CORE -- SQL `OVERLAPS` for
/// `timestamp`/`timestamptz` (one int64 representation, so this core serves
/// both via `timestamp_cmp_internal`).
pub fn overlaps_timestamp(
    ts1: Option<Timestamp>,
    te1: Option<Timestamp>,
    ts2: Option<Timestamp>,
    te2: Option<Timestamp>,
) -> Option<bool> {
    overlaps_generic(ts1, te1, ts2, te2, |a, b| {
        timestamp_cmp_internal(*a, *b).cmp(&0)
    })
}

/// `overlaps_time()` (date.c:1848) CORE -- SQL `OVERLAPS` for `time`.
pub fn overlaps_time(
    ts1: Option<TimeADT>,
    te1: Option<TimeADT>,
    ts2: Option<TimeADT>,
    te2: Option<TimeADT>,
) -> Option<bool> {
    overlaps_generic(ts1, te1, ts2, te2, |a, b| time_cmp(*a, *b).cmp(&0))
}

/// `overlaps_timetz()` (date.c:2760) CORE -- SQL `OVERLAPS` for `timetz`.
pub fn overlaps_timetz(
    ts1: Option<TimeTzADT>,
    te1: Option<TimeTzADT>,
    ts2: Option<TimeTzADT>,
    te2: Option<TimeTzADT>,
) -> Option<bool> {
    overlaps_generic(ts1, te1, ts2, te2, |a, b| timetz_cmp_internal(a, b).cmp(&0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::types_datetime::USECS_PER_HOUR;

    fn ts(hours: i64) -> Timestamp {
        hours * USECS_PER_HOUR
    }

    #[test]
    fn timestamp_overlapping_intervals_are_true() {
        assert_eq!(
            overlaps_timestamp(Some(ts(1)), Some(ts(4)), Some(ts(2)), Some(ts(5))),
            Some(true)
        );
        assert_eq!(
            overlaps_timestamp(Some(ts(4)), Some(ts(1)), Some(ts(5)), Some(ts(2))),
            Some(true)
        );
    }

    #[test]
    fn timestamp_touching_is_half_open_false() {
        assert_eq!(
            overlaps_timestamp(Some(ts(1)), Some(ts(2)), Some(ts(2)), Some(ts(3))),
            Some(false)
        );
    }

    #[test]
    fn timestamp_disjoint_is_false() {
        assert_eq!(
            overlaps_timestamp(Some(ts(1)), Some(ts(2)), Some(ts(3)), Some(ts(4))),
            Some(false)
        );
    }

    #[test]
    fn timestamp_equal_starts_are_true() {
        assert_eq!(
            overlaps_timestamp(Some(ts(2)), Some(ts(4)), Some(ts(2)), Some(ts(3))),
            Some(true)
        );
        assert_eq!(
            overlaps_timestamp(Some(ts(2)), Some(ts(4)), Some(ts(2)), None),
            None
        );
    }

    #[test]
    fn timestamp_null_endpoints_match_spec() {
        assert_eq!(
            overlaps_timestamp(None, None, Some(ts(1)), Some(ts(2))),
            None
        );
        assert_eq!(
            overlaps_timestamp(Some(ts(1)), None, Some(ts(3)), Some(ts(4))),
            None
        );
        assert_eq!(
            overlaps_timestamp(Some(ts(1)), None, Some(ts(0)), Some(ts(3))),
            Some(true)
        );
        assert_eq!(
            overlaps_timestamp(None, Some(ts(5)), Some(ts(1)), Some(ts(2))),
            None
        );
        assert_eq!(
            overlaps_timestamp(None, Some(ts(1)), Some(ts(0)), Some(ts(3))),
            Some(true)
        );
    }

    #[test]
    fn time_truth_table() {
        let h = |h: i64| h * USECS_PER_HOUR;
        assert_eq!(
            overlaps_time(Some(h(1)), Some(h(4)), Some(h(2)), Some(h(5))),
            Some(true)
        );
        assert_eq!(
            overlaps_time(Some(h(1)), Some(h(2)), Some(h(2)), Some(h(3))),
            Some(false)
        );
        assert_eq!(
            overlaps_time(Some(h(1)), Some(h(2)), Some(h(3)), Some(h(4))),
            Some(false)
        );
        assert_eq!(overlaps_time(Some(h(1)), Some(h(2)), None, None), None);
    }

    #[test]
    fn timetz_truth_table() {
        let tz = |h: i64| TimeTzADT {
            time: h * USECS_PER_HOUR,
            zone: 0,
        };
        assert_eq!(
            overlaps_timetz(Some(tz(1)), Some(tz(4)), Some(tz(2)), Some(tz(5))),
            Some(true)
        );
        assert_eq!(
            overlaps_timetz(Some(tz(1)), Some(tz(2)), Some(tz(2)), Some(tz(3))),
            Some(false)
        );
        assert_eq!(
            overlaps_timetz(Some(tz(1)), Some(tz(2)), Some(tz(3)), Some(tz(4))),
            Some(false)
        );
        assert_eq!(
            overlaps_timetz(Some(tz(1)), None, Some(tz(3)), Some(tz(4))),
            None
        );
    }
}
