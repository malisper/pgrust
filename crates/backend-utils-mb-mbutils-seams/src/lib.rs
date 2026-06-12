//! Seam declarations for the `backend-utils-mb-mbutils` unit
//! (`utils/mb/mbutils.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `GetDatabaseEncoding()` (mbutils.c): the database encoding id. Pure
    /// global read.
    pub fn get_database_encoding() -> i32
);

seam_core::seam!(
    /// `GetDatabaseEncodingName()` (mbutils.c): the database encoding's name
    /// (a static string in C). Pure read.
    pub fn get_database_encoding_name() -> String
);

seam_core::seam!(
    /// `is_encoding_supported_by_icu(encoding)` (mbutils.c): whether ICU
    /// collations work with the encoding. Pure table lookup.
    pub fn is_encoding_supported_by_icu(encoding: i32) -> bool
);
