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
