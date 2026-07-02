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

pub const fn date2j(mut year: i32, mut month: i32, day: i32) -> i32 {
    if month > 2 {
        month += 1;
        year += 4800;
    } else {
        month += 13;
        year += 4799;
    }

    let century = year / 100;
    let mut julian = year * 365 - 32167;
    julian += year / 4 - century + century / 4;
    julian += 7834 * month / 256 + day;

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
    date += 1;
    date %= 7;
    if date < 0 {
        date += 7;
    }
    date
}
