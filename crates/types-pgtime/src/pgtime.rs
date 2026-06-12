//! `pgtime.h` / `pgtz.h` / `tzfile.h` shared types.

extern crate alloc;

use alloc::string::String;

pub use types_core::primitive::pg_time_t;

/// pgtime.h — maximum length of a timezone name/POSIX TZ string (not
/// including the trailing NUL).
pub const TZ_STRLEN_MAX: usize = 255;
/// tzfile.h:100 — maximum number of transition times.
pub const TZ_MAX_TIMES: usize = 2000;
/// tzfile.h:103 — maximum number of transition types (limited by what
/// `unsigned char`s can hold).
pub const TZ_MAX_TYPES: usize = 256;
/// tzfile.h:105 — maximum number of abbreviation characters in a TZif file.
pub const TZ_MAX_CHARS: usize = 50;
/// tzfile.h:108 — maximum number of leap second corrections.
pub const TZ_MAX_LEAPS: usize = 50;
/// pgtz.h: char chars[BIGGEST(BIGGEST(TZ_MAX_CHARS + 1, 4),
/// 2 * (TZ_STRLEN_MAX + 1))] — the in-memory abbreviation buffer is sized for
/// POSIX TZ strings, not just TZif file contents.
pub const CHARS_SIZE: usize = 2 * (TZ_STRLEN_MAX + 1);

/// A broken-down calendar time, mirroring PostgreSQL's `struct pg_tm`
/// (pgtime.h).
///
/// CAUTION (as in C): the IANA timezone library follows the POSIX convention
/// that `tm_mon` counts from 0 and `tm_year` is relative to 1900, but
/// Postgres' datetime functions generally treat `tm_mon` as counting from 1
/// and `tm_year` as relative to 1 BC.
///
/// `tm_zone` borrows the timezone abbreviation for this instant from the
/// zone's abbreviation table (C's `tm_zone` points into `state.chars`), or is
/// `None` where C would have left the field unset.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct pg_tm<'tz> {
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
    pub tm_zone: Option<&'tz str>,
}

/// A loaded timezone: its canonical name plus the parsed transition state
/// (C `struct pg_tz` in pgtz.h, shared with pgtz.c).
#[derive(Clone)]
pub struct pg_tz {
    name: String,
    state: state,
}

impl pg_tz {
    /// Construct a timezone from a name and parsed state. Used by the pgtz
    /// unit (`pg_tzset`/`pg_tzset_offset`), which owns timezone caching.
    pub fn new(name: String, state: state) -> Self {
        Self { name, state }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn state(&self) -> &state {
        &self.state
    }
}

/// Parsed TZif/POSIX transition state, mirroring `struct state` in pgtz.h.
/// Fixed-size arrays keep the bounded-allocation behavior of the C original
/// (all sizes are compile-time constants, never data-derived). Shared by
/// localtime.c (tzload/tzparse) and pgtz.c (which builds `pg_tz` values).
#[derive(Clone)]
pub struct state {
    pub leapcnt: i32,
    pub timecnt: i32,
    pub typecnt: i32,
    pub charcnt: i32,
    pub goback: bool,
    pub goahead: bool,
    pub ats: [pg_time_t; TZ_MAX_TIMES],
    pub types: [u8; TZ_MAX_TIMES],
    pub ttis: [ttinfo; TZ_MAX_TYPES],
    pub chars: [u8; CHARS_SIZE],
    pub lsis: [lsinfo; TZ_MAX_LEAPS],
    /// The time type to use for early times or if no transitions. Always
    /// zero for recent tzdb releases; might be nonzero for data from tzdb
    /// 2018e or earlier.
    pub defaulttype: i32,
}

impl Default for state {
    fn default() -> Self {
        Self {
            leapcnt: 0,
            timecnt: 0,
            typecnt: 0,
            charcnt: 0,
            goback: false,
            goahead: false,
            ats: [0; TZ_MAX_TIMES],
            types: [0; TZ_MAX_TIMES],
            ttis: [ttinfo::default(); TZ_MAX_TYPES],
            chars: [0; CHARS_SIZE],
            lsis: [lsinfo::default(); TZ_MAX_LEAPS],
            defaulttype: 0,
        }
    }
}

/// Leap second information (C `struct lsinfo`, pgtz.h).
#[derive(Copy, Clone, Default)]
pub struct lsinfo {
    /// Transition time.
    pub ls_trans: pg_time_t,
    /// Correction to apply.
    pub ls_corr: i64,
}

/// Time type information (C `struct ttinfo`, pgtz.h).
#[derive(Copy, Clone, Default)]
pub struct ttinfo {
    /// UT offset in seconds.
    pub tt_utoff: i32,
    /// Used to set `tm_isdst`.
    pub tt_isdst: bool,
    /// Abbreviation list index.
    pub tt_desigidx: i32,
    /// Transition is std time.
    pub tt_ttisstd: bool,
    /// Transition is UT.
    pub tt_ttisut: bool,
}
