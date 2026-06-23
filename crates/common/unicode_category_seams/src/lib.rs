//! Seam declarations for `src/common/unicode_category.c` — the generated
//! Unicode general-category lookup tables.
//!
//! The owning unit (`common-unicode-category`) installs these from its
//! `init_seams()`. The seam exists because the consumer (`varlena`) would
//! otherwise pull in the large generated category tables across a cycle.

seam_core::seam!(
    /// `unicode_category(pg_wchar code)` (`common/unicode_category.c`) — the
    /// general category (`PG_U_*`, a `pg_unicode_category` value widened to
    /// `int`) of the Unicode code point `code`. A pure perfect-hash table
    /// lookup; cannot `ereport`. Returns `PG_U_UNASSIGNED` (0) for unassigned
    /// code points.
    pub fn unicode_category(code: types_core::PgWChar) -> i32
);
