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
    // Every arithmetic op here wraps: `month` and `day` are unconstrained at
    // this entry (the ISO week/year helpers and to_char hand through caller
    // arithmetic), so the month bump and the 7834*month product overflow for
    // large |month| exactly where C relies on -fwrapv. Found by
    // datetime_engine_diff at month == i32::MAX.
    if month > 2 {
        month = month.wrapping_add(1);
        year = year.wrapping_add(4800);
    } else {
        month = month.wrapping_add(13);
        year = year.wrapping_add(4799);
    }

    let century = year / 100;
    let mut julian = year.wrapping_mul(365).wrapping_sub(32167);
    julian = julian.wrapping_add(year / 4 - century + century / 4);
    // `+ day` must also wrap: interval tm_mday is caller-unbounded (e.g.
    // to_char(interval '2147483647 days', 'I') reaches here with
    // day = INT_MAX; real PG 18.3 wraps and prints -1, docker-confirmed;
    // found by fmt_dch_diff fuzz 2026-07-30). The 7834*month product wraps
    // too (datetime_engine_diff at month == i32::MAX).
    julian = julian.wrapping_add((7834i32.wrapping_mul(month) / 256).wrapping_add(day));

    julian
}

pub fn j2date(jd: i32, year: &mut i32, month: &mut i32, day: &mut i32) {
    // C j2date does this in `unsigned int`, where wraparound is DEFINED and
    // exercised: out-of-Julian-range inputs reach here via the to_char /
    // to_date format engine (e.g. to_date('2-45887','2J'); real 18.3 wraps,
    // found by fmt_dch_diff 2026-07-31). Every op wraps to match.
    let mut julian = jd as u32;
    julian = julian.wrapping_add(32044);
    let mut quad = julian / 146097;
    // C computes in unsigned int, which wraps by definition; a checked op
    // here is a ported-in panic for out-of-Julian-range inputs (found by
    // proofs/datetime-b hlp::eq_j2date_spots at jd=i32::MAX, by p1-laney's
    // fuzz witness '4955-120@BC'::timestamp, and by fmt_dch_diff via
    // to_date('2-45887','2J') — real 18.3 wraps; docker-confirmed).
    let extra = (julian.wrapping_sub(quad.wrapping_mul(146097)))
        .wrapping_mul(4)
        .wrapping_add(3);
    julian = julian.wrapping_add(60u32.wrapping_add(quad.wrapping_mul(3)).wrapping_add(extra / 146097));
    quad = julian / 1461;
    julian = julian.wrapping_sub(quad.wrapping_mul(1461));
    let mut y = (julian.wrapping_mul(4) / 1461) as i32;
    julian = if y != 0 {
        julian.wrapping_add(305) % 365
    } else {
        julian.wrapping_add(306) % 366
    }
    .wrapping_add(123);
    y = y.wrapping_add(quad.wrapping_mul(4) as i32);
    *year = y.wrapping_sub(4800);
    quad = julian.wrapping_mul(2141) / 65536;
    *day = julian.wrapping_sub(7834u32.wrapping_mul(quad) / 256) as i32;
    *month = ((quad.wrapping_add(10)) % 12) as i32 + 1;
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
    // `week` is unconstrained here (to_char/extract hand through whatever the
    // caller computed), and C's own comment on this function admits the
    // overflow hazard it leaves to -fwrapv; a checked `week - 1` is a
    // ported-in panic at week == i32::MIN (found by datetime_engine_diff).
    week.wrapping_sub(1)
        .wrapping_mul(7)
        .wrapping_add(day4.wrapping_sub(day0))
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
