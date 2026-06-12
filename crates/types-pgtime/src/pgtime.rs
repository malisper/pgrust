//! `struct pg_tm` (`src/include/pgtime.h`) and the time-unit constants from
//! `src/timezone/private.h` (shared by localtime.c, strftime.c, and the
//! datetime consumers).

use core::ffi::CStr;

// Time unit constants from `src/timezone/private.h`.
pub const SECSPERMIN: i32 = 60;
pub const MINSPERHOUR: i32 = 60;
pub const HOURSPERDAY: i32 = 24;
pub const DAYSPERWEEK: i32 = 7;
pub const DAYSPERNYEAR: i32 = 365;
pub const DAYSPERLYEAR: i32 = 366;
pub const MONSPERYEAR: i32 = 12;
pub const TM_YEAR_BASE: i32 = 1900;

/// Broken-down timestamp, mirroring PostgreSQL's `struct pg_tm` (`pgtime.h`).
///
/// C's `tm_zone` is a borrowed `const char *` into long-lived timezone state
/// (the `pg_tz` data / static tzdata strings); it is never allocated per
/// `pg_tm`. The port keeps it borrowed: `Option<&'tz CStr>`, `None` when the
/// zone abbreviation is not determinable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
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
    /// Seconds east of UTC; C declares this `long int`.
    pub tm_gmtoff: i64,
    /// Time-zone abbreviation; `None` when not determinable.
    pub tm_zone: Option<&'tz CStr>,
}
