//! Seam declarations for the owner `src/backend/utils/adt/date.c`'s ISO-week
//! helpers (`isoweek.c` group: `date2isoweek`, `date2isoyear`,
//! `date2isoyearday`, `isoweek2date`, `isoweekdate2date`, `isoweek2j`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_datetime::YmdDate;

seam_core::seam!(
    /// `date2isoweek(year, mon, mday)` (isoweek.c): ISO 8601 week number.
    pub fn date2isoweek(year: i32, mon: i32, mday: i32) -> i32
);

seam_core::seam!(
    /// `date2isoyear(year, mon, mday)` (isoweek.c): ISO 8601 week-numbering year.
    pub fn date2isoyear(year: i32, mon: i32, mday: i32) -> i32
);

seam_core::seam!(
    /// `date2isoyearday(year, mon, mday)` (isoweek.c): day-of-week-numbering-year
    /// (1..371) within the ISO year.
    pub fn date2isoyearday(year: i32, mon: i32, mday: i32) -> i32
);

seam_core::seam!(
    /// `isoweek2date(woy, &year, &mon, &mday)` (isoweek.c): the Gregorian date of
    /// the Monday of ISO week `woy` in `year`.
    pub fn isoweek2date(woy: i32, year: i32) -> YmdDate
);

seam_core::seam!(
    /// `isoweekdate2date(isoweek, wday, &year, &mon, &mday)` (isoweek.c): the
    /// Gregorian date of ISO weekday `wday` in week `isoweek` of `year`.
    pub fn isoweekdate2date(isoweek: i32, wday: i32, year: i32) -> YmdDate
);

seam_core::seam!(
    /// `isoweek2j(year, week)` (isoweek.c): the Julian day of the Monday of ISO
    /// week `week` in `year`.
    pub fn isoweek2j(year: i32, week: i32) -> i32
);
