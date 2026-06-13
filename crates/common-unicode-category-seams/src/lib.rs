//! Seam declarations for `src/common/unicode_category.c` — the generated
//! Unicode general-category lookup tables.
//!
//! The owning unit (`common/unicode_category`) installs these from its
//! `init_seams()` when it lands; until then a call panics loudly. This unit is
//! NOT yet ported (no `common-unicode-category` crate exists), so callers
//! reach it through this seam (mirror-pg-and-panic: a genuinely-unported
//! owner, not a stand-in).

seam_core::seam!(
    /// `unicode_category(pg_wchar code)` (`common/unicode_category.c`) — the
    /// general category (`PG_U_*`, a `pg_unicode_category` value widened to
    /// `int`) of the Unicode code point `code`. A pure perfect-hash table
    /// lookup; cannot `ereport`. Returns `PG_U_UNASSIGNED` (0) for unassigned
    /// code points.
    pub fn unicode_category(code: types_core::PgWChar) -> i32
);
