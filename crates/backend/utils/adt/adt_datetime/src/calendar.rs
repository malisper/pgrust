pub static DAY_TAB: [[i32; 13]; 2] = [
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
    [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
];

pub static MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

pub static DAYS: [&str; 7] = [
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
];

#[inline]
pub const fn isleap(y: i32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

// C (datetime.c date2j) computes in plain int arithmetic and PostgreSQL
// builds with -fwrapv: out-of-Julian-range years (to_char over huge interval
// year counts routes here through date2isoyear) must WRAP exactly as C does,
// not panic (fnconf batch-1, to_char(interval) crash family).
pub const fn date2j(mut year: i32, mut month: i32, day: i32) -> i32 {
    if month > 2 {
        month += 1;
        year = year.wrapping_add(4800);
    } else {
        month += 13;
        year = year.wrapping_add(4799);
    }

    let century = year / 100;
    let mut julian = year.wrapping_mul(365).wrapping_sub(32167);
    julian = julian.wrapping_add(year / 4 - century + century / 4);
    julian = julian.wrapping_add(7834 * month / 256 + day);

    julian
}

pub fn j2date(jd: i32, year: &mut i32, month: &mut i32, day: &mut i32) {
    let mut julian = jd as u32;
    julian = julian.wrapping_add(32044);
    let mut quad = julian / 146097;
    let extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    let mut y = (julian * 4 / 1461) as i32;
    julian = if y != 0 { (julian + 305) % 365 } else { (julian + 306) % 366 } + 123;
    y += (quad * 4) as i32;
    *year = y - 4800;
    quad = julian * 2141 / 65536;
    *day = (julian - 7834 * quad / 256) as i32;
    *month = ((quad + 10) % 12) as i32 + 1;
}

pub const fn j2day(mut date: i32) -> i32 {
    date = date.wrapping_add(1);
    date %= 7;
    if date < 0 {
        date += 7;
    }
    date
}

// The isoweek family below mirrors C (timestamp.c/date.c helpers) which is
// compiled with -fwrapv; every add/sub on julian-day values must wrap, since
// date2j legitimately returns wrapped values for out-of-range years.
pub fn isoweek2j(year: i32, week: i32) -> i32 {
    let day4 = date2j(year, 1, 4);
    let day0 = j2day(day4.wrapping_sub(1));
    (week - 1).wrapping_mul(7).wrapping_add(day4.wrapping_sub(day0))
}

pub fn isoweek2date(woy: i32, year: &mut i32, mon: &mut i32, mday: &mut i32) {
    j2date(isoweek2j(*year, woy), year, mon, mday);
}

pub fn isoweekdate2date(isoweek: i32, wday: i32, year: &mut i32, mon: &mut i32, mday: &mut i32) {
    let mut jday = isoweek2j(*year, isoweek);
    if wday > 1 {
        jday = jday.wrapping_add(wday - 2);
    } else {
        jday = jday.wrapping_add(6);
    }
    j2date(jday, year, mon, mday);
}

pub fn date2isoweek(year: i32, mon: i32, mday: i32) -> i32 {
    let dayn = date2j(year, mon, mday);
    let mut day4 = date2j(year, 1, 4);
    let mut day0 = j2day(day4.wrapping_sub(1));

    if dayn < day4.wrapping_sub(day0) {
        day4 = date2j(year.wrapping_sub(1), 1, 4);
        day0 = j2day(day4.wrapping_sub(1));
    }

    let mut result = dayn.wrapping_sub(day4.wrapping_sub(day0)) / 7 + 1;

    if result >= 52 {
        day4 = date2j(year.wrapping_add(1), 1, 4);
        day0 = j2day(day4.wrapping_sub(1));
        if dayn >= day4.wrapping_sub(day0) {
            result = dayn.wrapping_sub(day4.wrapping_sub(day0)) / 7 + 1;
        }
    }

    result
}

pub fn date2isoyear(year: i32, mon: i32, mday: i32) -> i32 {
    let dayn = date2j(year, mon, mday);
    let mut day4 = date2j(year, 1, 4);
    let mut day0 = j2day(day4.wrapping_sub(1));
    let mut year = year;

    if dayn < day4.wrapping_sub(day0) {
        day4 = date2j(year.wrapping_sub(1), 1, 4);
        day0 = j2day(day4.wrapping_sub(1));
        year = year.wrapping_sub(1);
    }

    let result = dayn.wrapping_sub(day4.wrapping_sub(day0)) / 7 + 1;

    if result >= 52 {
        day4 = date2j(year.wrapping_add(1), 1, 4);
        day0 = j2day(day4.wrapping_sub(1));
        if dayn >= day4.wrapping_sub(day0) {
            year = year.wrapping_add(1);
        }
    }

    year
}

pub fn date2isoyearday(year: i32, mon: i32, mday: i32) -> i32 {
    date2j(year, mon, mday)
        .wrapping_sub(isoweek2j(date2isoyear(year, mon, mday), 1))
        .wrapping_add(1)
}
