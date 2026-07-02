//! IANA tz engine (localtime.c): TZif load, POSIX TZ parse, calendar math.

#![allow(clippy::manual_range_contains)]

mod clock;
mod load;
#[cfg(test)]
mod tests;

pub use clock::{
    pg_get_next_timezone_abbrev, pg_get_timezone_name, pg_get_timezone_offset, pg_gmtime,
    pg_interpret_timezone_abbrev, pg_localtime, pg_next_dst_boundary, pg_timezone_abbrev_is_known,
    pg_tz_acceptable, DstBoundary, NextDstBoundary,
};
pub use load::{tzload, tzparse, TzLoadError};
pub use types_core::primitive::pg_time_t;

pub const TZ_MAX_TIMES: usize = 2000;
pub const TZ_MAX_TYPES: usize = 256;
pub const TZ_MAX_CHARS: usize = 50;
pub const TZ_MAX_LEAPS: usize = 50;
pub const TZ_STRLEN_MAX: usize = 255;
// C chars[]: BIGGEST(BIGGEST(TZ_MAX_CHARS + 1, sizeof "GMT"), 2 * (TZ_STRLEN_MAX + 1)).
pub const CHARS_SIZE: usize = 2 * (TZ_STRLEN_MAX + 1);

pub(crate) const SECSPERMIN: i64 = 60;
pub(crate) const MINSPERHOUR: i64 = 60;
pub(crate) const HOURSPERDAY: i64 = 24;
pub const DAYSPERWEEK: i32 = 7;
pub const DAYSPERNYEAR: i32 = 365;
pub const DAYSPERLYEAR: i32 = 366;
pub const MONSPERYEAR: usize = 12;
pub(crate) const SECSPERHOUR: i64 = SECSPERMIN * MINSPERHOUR;
pub(crate) const SECSPERDAY: i64 = SECSPERHOUR * HOURSPERDAY;
pub(crate) const YEARSPERREPEAT: i32 = 400;
pub(crate) const AVGSECSPERYEAR: i64 = 31_556_952;
pub(crate) const SECSPERREPEAT: i64 = YEARSPERREPEAT as i64 * AVGSECSPERYEAR;
pub(crate) const SECSPERREPEAT_BITS: u32 = 34;
pub(crate) const EPOCH_YEAR: i32 = 1970;
pub(crate) const EPOCH_WDAY: i32 = 4;
pub const TM_YEAR_BASE: i32 = 1900;
pub(crate) const TIME_T_MIN: pg_time_t = pg_time_t::MIN;
pub(crate) const TIME_T_MAX: pg_time_t = pg_time_t::MAX;
pub(crate) const POSTGRES_EPOCH_JDATE: i64 = 2_451_545;
pub(crate) const UNIX_EPOCH_JDATE: i64 = 2_440_588;
pub(crate) const WILDABBR: &str = "   ";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TtInfo {
    pub tt_utoff: i32,
    pub tt_isdst: bool,
    pub tt_desigidx: i32,
    pub tt_ttisstd: bool,
    pub tt_ttisut: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LsInfo {
    pub ls_trans: pg_time_t,
    pub ls_corr: i64,
}

pub struct TzState {
    pub leapcnt: i32,
    pub timecnt: i32,
    pub typecnt: i32,
    pub charcnt: i32,
    pub goback: bool,
    pub goahead: bool,
    pub ats: [pg_time_t; TZ_MAX_TIMES],
    pub types: [u8; TZ_MAX_TIMES],
    pub ttis: [TtInfo; TZ_MAX_TYPES],
    pub chars: [u8; CHARS_SIZE],
    pub lsis: [LsInfo; TZ_MAX_LEAPS],
    pub defaulttype: i32,
}

impl TzState {
    pub const fn new() -> Self {
        TzState {
            leapcnt: 0,
            timecnt: 0,
            typecnt: 0,
            charcnt: 0,
            goback: false,
            goahead: false,
            ats: [0; TZ_MAX_TIMES],
            types: [0; TZ_MAX_TIMES],
            ttis: [TtInfo {
                tt_utoff: 0,
                tt_isdst: false,
                tt_desigidx: 0,
                tt_ttisstd: false,
                tt_ttisut: false,
            }; TZ_MAX_TYPES],
            chars: [0; CHARS_SIZE],
            lsis: [LsInfo {
                ls_trans: 0,
                ls_corr: 0,
            }; TZ_MAX_LEAPS],
            defaulttype: 0,
        }
    }
}

impl Default for TzState {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TzState {
    fn clone(&self) -> Self {
        TzState { ..*self }
    }
}

pub struct PgTz {
    pub tzname: [u8; TZ_STRLEN_MAX + 1],
    pub state: TzState,
}

impl PgTz {
    pub fn new(name: &[u8], state: TzState) -> Self {
        let mut tzname = [0u8; TZ_STRLEN_MAX + 1];
        let n = name.len().min(TZ_STRLEN_MAX);
        tzname[..n].copy_from_slice(&name[..n]);
        PgTz { tzname, state }
    }

    pub fn name(&self) -> &[u8] {
        cstr_bytes(&self.tzname, 0)
    }
}

/// POSIX-convention broken-down time (pgtime.h `struct pg_tm`): `tm_mon` is
/// 0-based, `tm_year` is year-1900; `tm_zone` borrows the zone's abbreviation
/// table exactly as C's pointer does.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PgTm<'a> {
    pub tm_sec: i32,
    pub tm_min: i32,
    pub tm_hour: i32,
    pub tm_mday: i32,
    pub tm_mon: i32,
    pub tm_year: i32,
    pub tm_wday: i32,
    pub tm_yday: i32,
    pub tm_isdst: i32,
    pub tm_gmtoff: i64,
    pub tm_zone: Option<&'a str>,
}

pub(crate) fn is_leap(year: i32) -> bool {
    (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0)
}

pub(crate) fn year_lengths(leap: bool) -> i32 {
    if leap {
        DAYSPERLYEAR
    } else {
        DAYSPERNYEAR
    }
}

pub(crate) fn mon_lengths(leap: bool, month: usize) -> i32 {
    const MON_LENGTHS: [[i32; MONSPERYEAR]; 2] = [
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    ];
    MON_LENGTHS[leap as usize][month]
}

pub(crate) fn increment_overflow(ip: &mut i32, j: i32) -> bool {
    match ip.checked_add(j) {
        Some(v) => {
            *ip = v;
            false
        }
        None => true,
    }
}

pub(crate) fn increment_overflow_time(tp: &mut pg_time_t, j: i64) -> bool {
    match tp.checked_add(j) {
        Some(v) => {
            *tp = v;
            false
        }
        None => true,
    }
}

pub(crate) fn cstr_bytes(chars: &[u8], start: usize) -> &[u8] {
    let slice = chars.get(start..).unwrap_or(&[]);
    let end = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
    &slice[..end]
}

pub(crate) fn cstr_str(chars: &[u8], start: usize) -> &str {
    core::str::from_utf8(cstr_bytes(chars, start)).unwrap_or(WILDABBR)
}
