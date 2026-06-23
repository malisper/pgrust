//! Calendar core: Gregorian <-> Julian-day conversions and the month-length
//! table, ported verbatim from `src/backend/utils/adt/datetime.c`.
//!
//! These routines are pure and infallible.  `date2j` / `j2date` / `j2day`
//! mirror the C implementations exactly, including `j2date`'s use of *unsigned*
//! arithmetic internally so that BC and edge dates round-trip correctly.
//!
//! Idiomatic: plain `i32` everywhere (the C `int` parameters), and `j2date`
//! returns the broken-down `(year, month, day)` by value instead of writing
//! through out-pointers.

use types_datetime::MONTHS_PER_YEAR;

/// Number of days in each month, indexed `[isleap][month-1]`.  The trailing
/// `0` slot mirrors the C `day_tab[2][13]` shape (13 columns per row).
///
/// (`utils/adt/datetime.c`)
pub const day_tab: [[i32; 13]; 2] = [
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
    [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
];

/// Is `y` a leap year?  (`IS_LEAP_YEAR` / `isleap` macro from `timestamp.h`.)
///
/// Returns 0 or 1 so it can index `day_tab` exactly as the C code does.
#[inline]
pub fn isleap(y: i32) -> usize {
    ((y) % 4 == 0 && ((y) % 100 != 0 || (y) % 400 == 0)) as usize
}

/// `IS_LEAP_YEAR(y)` -- boolean alias of [`isleap`].
#[inline]
pub fn IS_LEAP_YEAR(y: i32) -> bool {
    isleap(y) != 0
}

/// `date2j()` -- convert a Gregorian calendar date to a Julian day number.
///
/// Inputs are signed `i32` (matching the C `int` parameters).  This is exact
/// for all Julian day counts representable in a 32-bit integer.
///
/// (`utils/adt/datetime.c`)
pub fn date2j(mut year: i32, mut month: i32, day: i32) -> i32 {
    if month > 2 {
        month += 1;
        year += 4800;
    } else {
        month += 13;
        year += 4799;
    }

    let century = year / 100;
    let julian = year * 365 - 32167;
    let mut julian = julian + (year / 4 - century + century / 4);
    julian += 7834 * month / 256 + day;

    julian
}

/// `j2date()` -- convert a Julian day number to a Gregorian calendar date.
///
/// Returns `(year, month, day)`.  Internally this uses *unsigned* arithmetic,
/// exactly as the C original does, so it stays correct for BC and other edge
/// dates where signed division would behave differently.
///
/// (`utils/adt/datetime.c`)
pub fn j2date(jd: i32) -> (i32, i32, i32) {
    let mut julian: u32;
    let mut quad: u32;
    let mut y: i32;

    julian = jd as u32;
    julian = julian.wrapping_add(32044);
    quad = julian / 146097;
    let extra: u32 = (julian.wrapping_sub(quad.wrapping_mul(146097)))
        .wrapping_mul(4)
        .wrapping_add(3);
    julian = julian
        .wrapping_add(60)
        .wrapping_add(quad.wrapping_mul(3))
        .wrapping_add(extra / 146097);
    quad = julian / 1461;
    julian = julian.wrapping_sub(quad.wrapping_mul(1461));
    y = (julian.wrapping_mul(4) / 1461) as i32;
    julian = (if y != 0 {
        (julian.wrapping_add(305)) % 365
    } else {
        (julian.wrapping_add(306)) % 366
    })
    .wrapping_add(123);
    y = y.wrapping_add((quad as i32).wrapping_mul(4));
    let year = y - 4800;
    quad = julian.wrapping_mul(2141) / 65536;
    let day = (julian as i32) - (7834 * quad as i32 / 256);
    let month = (quad as i32 + 10) % MONTHS_PER_YEAR + 1;

    (year, month, day)
}

/// `j2day()` -- convert a Julian day number to a day-of-week (`0..6 == Sun..Sat`).
///
/// Note: callers sometimes pass `date - 1` to get the `0..6 = Mon..Sun`
/// convention; that works because this is just a modulo.
///
/// (`utils/adt/datetime.c`)
pub fn j2day(mut date: i32) -> i32 {
    date += 1;
    date %= 7;
    /* Cope if division truncates towards zero, as it probably does */
    if date < 0 {
        date += 7;
    }

    date
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date2j_known_anchors() {
        // Postgres epoch 2000-01-01 == JD 2451545.
        assert_eq!(date2j(2000, 1, 1), 2_451_545);
        // Unix epoch 1970-01-01 == JD 2440588.
        assert_eq!(date2j(1970, 1, 1), 2_440_588);
    }

    #[test]
    fn date2j_j2date_round_trip_range() {
        // Round-trip across a wide range, including year boundaries.
        for &(y, m, d) in &[
            (1, 1, 1),
            (1, 12, 31),
            (1999, 12, 31),
            (2000, 1, 1),
            (2000, 2, 29), // leap day
            (2024, 1, 15),
            (2400, 2, 29),
            (5000, 6, 30),
        ] {
            let jd = date2j(y, m, d);
            let (ry, rm, rd) = j2date(jd);
            assert_eq!((ry, rm, rd), (y, m, d), "round-trip failed for {y}-{m}-{d}");
        }
    }

    #[test]
    fn date2j_j2date_round_trip_bc() {
        // A BC date: internally Postgres represents 1 BC as year 0, etc.
        // year 0 (== 1 BC) and a deeply-negative year must round-trip.
        for &(y, m, d) in &[(0, 1, 1), (0, 12, 31), (-100, 6, 15), (-4713, 11, 24)] {
            let jd = date2j(y, m, d);
            let (ry, rm, rd) = j2date(jd);
            assert_eq!(
                (ry, rm, rd),
                (y, m, d),
                "BC round-trip failed for {y}-{m}-{d}"
            );
        }
    }

    #[test]
    fn date2j_dense_round_trip() {
        // Dense scan over a few years crossing month/year boundaries.
        let start = date2j(1998, 1, 1);
        let end = date2j(2003, 12, 31);
        for jd in start..=end {
            let (y, m, d) = j2date(jd);
            assert_eq!(date2j(y, m, d), jd);
        }
    }

    #[test]
    fn j2day_weekdays() {
        // 2000-01-01 was a Saturday (== 6).
        assert_eq!(j2day(date2j(2000, 1, 1)), 6);
        // 2024-01-15 was a Monday (== 1).
        assert_eq!(j2day(date2j(2024, 1, 15)), 1);
    }

    #[test]
    fn isleap_matches_rule() {
        assert_eq!(isleap(2000), 1);
        assert_eq!(isleap(1900), 0);
        assert_eq!(isleap(2024), 1);
        assert_eq!(isleap(2023), 0);
        assert!(IS_LEAP_YEAR(2024));
        assert!(!IS_LEAP_YEAR(2023));
    }

    #[test]
    fn day_tab_february_lengths() {
        assert_eq!(day_tab[0][1], 28);
        assert_eq!(day_tab[1][1], 29);
        assert_eq!(day_tab[isleap(2024)][1], 29);
        assert_eq!(day_tab[isleap(2023)][1], 28);
    }
}
