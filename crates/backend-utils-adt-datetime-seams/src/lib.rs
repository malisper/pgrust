//! Seam declarations for the owner `src/backend/utils/adt/datetime.c`.
//!
//! `tzparser.c`'s `load_tzoffsets` hands its parsed entries to
//! `ConvertTimeZoneAbbrevs`, which re-allocates them into a `guc_malloc`'d
//! `TimeZoneAbbrevTable` in datetime.c's storage format. The owner is not yet
//! ported; calls panic until it lands.

seam_core::seam!(
    /// `ConvertTimeZoneAbbrevs(abbrevs, n)` (`utils/adt/datetime.c`): convert
    /// the parsed `tzEntry` array into a `guc_malloc`'d `TimeZoneAbbrevTable`.
    /// Returns `None` on allocation failure (the C `NULL` return that
    /// `load_tzoffsets` reports as "out of memory").
    pub fn convert_time_zone_abbrevs(
        abbrevs: Vec<types_misc_more2::TzEntry>,
    ) -> Option<types_misc_more2::TimeZoneAbbrevTable>
);

use types_datetime::{TzAbbrevMatch, TzHandle, YmdDate};
use types_pgtime::pg_tm;

seam_core::seam!(
    /// `date2j(y, m, d)` (datetime.c): Julian day number for a Gregorian date.
    pub fn date2j(year: i32, month: i32, day: i32) -> i32
);

seam_core::seam!(
    /// `j2date(jd, &year, &month, &day)` (datetime.c): Gregorian date for a
    /// Julian day number.
    pub fn j2date(jd: i32) -> YmdDate
);

seam_core::seam!(
    /// `ValidateDate(fmask, isjulian, is2digits, bc, tm)` (datetime.c): range-
    /// and field-validate a partially-decoded `tm`, adjusting century / BC.
    /// Returns the C `int` status (`0` on success, a `DTERR_*` code otherwise).
    /// `tm` is updated in place.
    pub fn validate_date(fmask: i32, is2digits: bool, bc: bool, tm: &mut pg_tm) -> i32
);

seam_core::seam!(
    /// `DetermineTimeZoneOffset(tm, tzp)` (datetime.c): resolve the GMT offset
    /// (seconds) for the broken-down local time in `tm` under the session
    /// timezone, updating `tm`'s DST/zone fields.
    pub fn determine_time_zone_offset(tm: &mut pg_tm) -> i32
);

seam_core::seam!(
    /// `DetermineTimeZoneAbbrevOffset(tm, abbr, tzp)` (datetime.c): resolve the
    /// GMT offset (seconds) for the named dynamic abbreviation `abbr` at the
    /// time in `tm`, under the timezone identified by `tzp`.
    pub fn determine_time_zone_abbrev_offset(tm: &mut pg_tm, abbr: &str, tzp: TzHandle) -> i32
);

seam_core::seam!(
    /// `DecodeTimezoneAbbrevPrefix(str, &offset, &tz)` (datetime.c): scan the
    /// leading bytes of `s` for a known timezone abbreviation. See
    /// [`TzAbbrevMatch`] for the owned output shape.
    pub fn decode_timezone_abbrev_prefix(s: &[u8]) -> TzAbbrevMatch
);
