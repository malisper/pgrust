//! ISO-8601 week-date helpers, ported from `src/backend/utils/adt/timestamp.c`.
//!
//! These are pure, infallible (modulo the documented integer-overflow hazards
//! that the C originals also carry) Gregorian <-> ISO-week conversions built on
//! the calendar core (`date2j` / `j2date` / `j2day`).  Original names and
//! behavior are preserved.  The functions that update several outputs at once
//! (`isoweek2date` / `isoweekdate2date`) take `&mut i32` references, the
//! idiomatic-safe analogue of the C out-pointers.

use crate::calendar::{date2j, j2date, j2day};

/// `isoweek2j()` -- Julian day of the first day (Monday) of the given ISO-8601
/// year and week.
///
/// XXX: as the C comment notes, this carries integer-overflow hazards, but
/// restructuring for soft errors isn't worth it; we mirror the C arithmetic.
///
/// (`utils/adt/timestamp.c`)
pub fn isoweek2j(year: i32, week: i32) -> i32 {
    /* fourth day of current year */
    let day4 = date2j(year, 1, 4);

    /* day0 == offset to first day of week (Monday) */
    let day0 = j2day(day4 - 1);

    ((week - 1) * 7) + (day4 - day0)
}

/// `isoweek2date()` -- convert ISO week of year number to date.  The `year`
/// field must be passed in as the ISO year; it is updated to the Gregorian
/// year along with `mon` / `mday`.  (`utils/adt/timestamp.c`)
pub fn isoweek2date(woy: i32, year: &mut i32, mon: &mut i32, mday: &mut i32) {
    let (y, m, d) = j2date(isoweek2j(*year, woy));
    *year = y;
    *mon = m;
    *mday = d;
}

/// `isoweekdate2date()` -- convert an ISO-8601 week date (ISO year, ISO week)
/// plus a Gregorian day-of-week into a Gregorian date.  `year` must be passed
/// in as the ISO year; it is updated to the Gregorian year along with
/// `mon` / `mday`.  (`utils/adt/timestamp.c`)
pub fn isoweekdate2date(isoweek: i32, wday: i32, year: &mut i32, mon: &mut i32, mday: &mut i32) {
    let mut jday = isoweek2j(*year, isoweek);
    /* convert Gregorian week start (Sunday=1) to ISO week start (Monday=1) */
    if wday > 1 {
        jday += wday - 2;
    } else {
        jday += 6;
    }
    let (y, m, d) = j2date(jday);
    *year = y;
    *mon = m;
    *mday = d;
}

/// `date2isoweek()` -- ISO week number of the year for a Gregorian date.
///
/// (`utils/adt/timestamp.c`)
pub fn date2isoweek(year: i32, mon: i32, mday: i32) -> i32 {
    /* current day */
    let dayn = date2j(year, mon, mday);

    /* fourth day of current year */
    let mut day4 = date2j(year, 1, 4);

    /* day0 == offset to first day of week (Monday) */
    let mut day0 = j2day(day4 - 1);

    /*
     * We need the first week containing a Thursday, otherwise this day falls
     * into the previous year for purposes of counting weeks
     */
    if dayn < day4 - day0 {
        day4 = date2j(year - 1, 1, 4);
        day0 = j2day(day4 - 1);
    }

    let mut result: f64 = ((dayn - (day4 - day0)) / 7 + 1) as f64;

    /*
     * Sometimes the last few days in a year will fall into the first week of
     * the next year, so check for this.
     */
    if result >= 52.0 {
        day4 = date2j(year + 1, 1, 4);
        day0 = j2day(day4 - 1);

        if dayn >= day4 - day0 {
            result = ((dayn - (day4 - day0)) / 7 + 1) as f64;
        }
    }

    result as i32
}

/// `date2isoyear()` -- ISO-8601 year number for a Gregorian date.
///
/// Note: zero or negative results follow the year-zero-exists convention.
///
/// (`utils/adt/timestamp.c`)
pub fn date2isoyear(mut year: i32, mon: i32, mday: i32) -> i32 {
    /* current day */
    let dayn = date2j(year, mon, mday);

    /* fourth day of current year */
    let mut day4 = date2j(year, 1, 4);

    /* day0 == offset to first day of week (Monday) */
    let mut day0 = j2day(day4 - 1);

    /*
     * We need the first week containing a Thursday, otherwise this day falls
     * into the previous year for purposes of counting weeks
     */
    if dayn < day4 - day0 {
        day4 = date2j(year - 1, 1, 4);
        day0 = j2day(day4 - 1);
        year -= 1;
    }

    let result: f64 = ((dayn - (day4 - day0)) / 7 + 1) as f64;

    /*
     * Sometimes the last few days in a year will fall into the first week of
     * the next year, so check for this.
     */
    if result >= 52.0 {
        day4 = date2j(year + 1, 1, 4);
        day0 = j2day(day4 - 1);

        if dayn >= day4 - day0 {
            year += 1;
        }
    }

    year
}

/// `date2isoyearday()` -- ISO-8601 day-of-year for a Gregorian date.
///
/// Possible return values are 1 through 371 (364 in non-leap years).
///
/// (`utils/adt/timestamp.c`)
pub fn date2isoyearday(year: i32, mon: i32, mday: i32) -> i32 {
    date2j(year, mon, mday) - isoweek2j(date2isoyear(year, mon, mday), 1) + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date2isoweek_known_dates() {
        // 2024-01-01 is a Monday in ISO week 1 of 2024.
        assert_eq!(date2isoweek(2024, 1, 1), 1);
        // 2024-12-31 is a Tuesday in ISO week 1 of 2025.
        assert_eq!(date2isoweek(2024, 12, 31), 1);
        // 2020-12-31 -> ISO week 53.
        assert_eq!(date2isoweek(2020, 12, 31), 53);
        // 2005-01-01 belongs to ISO week 53 of 2004.
        assert_eq!(date2isoweek(2005, 1, 1), 53);
        // A mid-year date: 2024-06-15 is in ISO week 24.
        assert_eq!(date2isoweek(2024, 6, 15), 24);
    }

    #[test]
    fn date2isoyear_known_dates() {
        // 2024-12-31 is in ISO year 2025.
        assert_eq!(date2isoyear(2024, 12, 31), 2025);
        // 2005-01-01 is in ISO year 2004.
        assert_eq!(date2isoyear(2005, 1, 1), 2004);
        assert_eq!(date2isoyear(2024, 6, 15), 2024);
    }

    #[test]
    fn isoweek2date_round_trips() {
        // ISO 2024-W24 Monday should be 2024-06-10.
        let mut year = 2024;
        let mut mon = 0;
        let mut mday = 0;
        isoweek2date(24, &mut year, &mut mon, &mut mday);
        assert_eq!((year, mon, mday), (2024, 6, 10));
    }

    #[test]
    fn isoweekdate2date_round_trips() {
        // ISO year 2024, week 24, Gregorian wday for a Wednesday (Sun=1..Sat=7
        // => Wed=4) should land on 2024-06-12.
        let mut year = 2024;
        let mut mon = 0;
        let mut mday = 0;
        isoweekdate2date(24, 4, &mut year, &mut mon, &mut mday);
        assert_eq!((year, mon, mday), (2024, 6, 12));
    }

    #[test]
    fn date2isoyearday_range() {
        let d = date2isoyearday(2024, 1, 1);
        assert!((1..=371).contains(&d));
        // 2024-01-01 is the first ISO day of ISO year 2024.
        assert_eq!(d, 1);
    }
}
