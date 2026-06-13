//! `struct pg_tm` (`pgtime.h`) — the timezone-aware broken-down time produced
//! by `pg_localtime`/`pg_gmtime`.

use alloc::string::String;

/// `struct pg_tm` (`pgtime.h`). Field conventions follow the producing
/// function exactly as in C (e.g. `pg_localtime` leaves `tm_mon` 0-based).
/// `tm_zone` is the abbreviation (`const char *tm_zone`), `None` mirroring a
/// NULL pointer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct pg_tm {
    pub tm_sec: i32,
    pub tm_min: i32,
    pub tm_hour: i32,
    pub tm_mday: i32,
    pub tm_mon: i32,
    pub tm_year: i32,
    pub tm_wday: i32,
    pub tm_yday: i32,
    pub tm_isdst: i32,
    /// `long int tm_gmtoff` — seconds east of GMT.
    pub tm_gmtoff: i64,
    pub tm_zone: Option<String>,
}

/// `struct pg_tz` (`timezone/pgtz.h`), trimmed to the canonical name
/// (`char TZname[TZ_STRLEN_MAX + 1]`). The tzdata `struct state` payload
/// stays with the pgtz owner, which widens this type when it lands; until
/// then consumers only carry the value across the `pg_localtime` seam.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct pg_tz {
    /// Canonically-cased timezone name.
    pub TZname: String,
}

/// `TZ_STRLEN_MAX` (`pgtime.h`).
pub const TZ_STRLEN_MAX: usize = 255;
