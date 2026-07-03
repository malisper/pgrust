//! ISO-8601 week helpers (timestamp.c: isoweek2j/date2isoweek/date2isoyear/
//! date2isoyearday). Owned by date.c in C; inlined here (self-contained over
//! date2j/j2day) until that unit lands, since to_char DCH needs them directly.

use ::adt_datetime::{date2j, j2date, j2day};

pub fn isoweek2j(year: i32, week: i32) -> i32 {
    let day4 = date2j(year, 1, 4);
    let day0 = j2day(day4 - 1);
    ((week - 1) * 7) + (day4 - day0)
}

pub fn isoweek2date(woy: i32, year: &mut i32, mon: &mut i32, mday: &mut i32) {
    j2date(isoweek2j(*year, woy), year, mon, mday);
}

pub fn isoweekdate2date(isoweek: i32, wday: i32, year: &mut i32, mon: &mut i32, mday: &mut i32) {
    let mut jday = isoweek2j(*year, isoweek);
    if wday > 1 {
        jday += wday - 2;
    } else {
        jday += 6;
    }
    j2date(jday, year, mon, mday);
}

pub fn date2isoweek(year: i32, mon: i32, mday: i32) -> i32 {
    let dayn = date2j(year, mon, mday);
    let mut day4 = date2j(year, 1, 4);
    let mut day0 = j2day(day4 - 1);

    if dayn < day4 - day0 {
        day4 = date2j(year - 1, 1, 4);
        day0 = j2day(day4 - 1);
    }

    let mut result = (dayn - (day4 - day0)) / 7 + 1;

    if result >= 52 {
        day4 = date2j(year + 1, 1, 4);
        day0 = j2day(day4 - 1);
        if dayn >= day4 - day0 {
            result = (dayn - (day4 - day0)) / 7 + 1;
        }
    }

    result
}

pub fn date2isoyear(year: i32, mon: i32, mday: i32) -> i32 {
    let dayn = date2j(year, mon, mday);
    let mut day4 = date2j(year, 1, 4);
    let mut day0 = j2day(day4 - 1);
    let mut year = year;

    if dayn < day4 - day0 {
        day4 = date2j(year - 1, 1, 4);
        day0 = j2day(day4 - 1);
        year -= 1;
    }

    let result = (dayn - (day4 - day0)) / 7 + 1;

    if result >= 52 {
        day4 = date2j(year + 1, 1, 4);
        day0 = j2day(day4 - 1);
        if dayn >= day4 - day0 {
            year += 1;
        }
    }

    year
}

pub fn date2isoyearday(year: i32, mon: i32, mday: i32) -> i32 {
    date2j(year, mon, mday) - isoweek2j(date2isoyear(year, mon, mday), 1) + 1
}
