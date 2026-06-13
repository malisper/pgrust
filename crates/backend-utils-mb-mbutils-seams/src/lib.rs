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
    /// — a pointer into the static `pg_enc2name` table in C, so no
    /// allocation: `&'static str` mirrors the static-table read.
    pub fn get_database_encoding_name() -> &'static str
);

seam_core::seam!(
    /// `is_encoding_supported_by_icu(encoding)` (mbutils.c): whether ICU
    /// collations work with the encoding. Pure table lookup.
    pub fn is_encoding_supported_by_icu(encoding: i32) -> bool
);
