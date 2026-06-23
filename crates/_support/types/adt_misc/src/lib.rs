//! Signature vocabulary for `backend-utils-adt-misc` (`utils/adt/misc.c`): the
//! row/step structs that appear in the crate's ported logic and in the seam
//! declarations of its unported callees.

use ::types_core::Oid;

/// One row of `pg_get_keywords()` (misc.c:417): the keyword name, its category
/// letter/description and its bare-label flag/description. Assembled from
/// `ScanKeywords`/`ScanKeywordCategories`/`ScanKeywordBareLabel`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeywordRow {
    /// `values[0]` — the keyword text (`GetScanKeyword(i, &ScanKeywords)`).
    pub word: Vec<u8>,
    /// `values[1]` — category letter: `U`/`C`/`T`/`R`, or `None` for the
    /// "shouldn't be possible" default arm.
    pub catcode: Option<&'static str>,
    /// `values[3]` — translated category description (or `None`).
    pub catdesc: Option<&'static str>,
    /// `values[2]` — `"true"`/`"false"` (the bare-label flag, rendered).
    pub barelabel: &'static str,
    /// `values[4]` — translated bare-label description.
    pub baredesc: &'static str,
}

/// One row of `pg_get_catalog_foreign_keys()` (misc.c:495): a single
/// `sys_fk_relationships[]` entry rendered into the tuple's column values. The
/// `fk_columns`/`pk_columns` `text[]` values are produced by the seam's
/// `array_in` call (misc.c:539/544).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogForeignKeyRow {
    /// `values[0]` — `fkrel->fk_table`.
    pub fktable: Oid,
    /// `values[1]` — `array_in(fkrel->fk_columns)` rendered as text[] elements.
    pub fkcols: Vec<Vec<u8>>,
    /// `values[2]` — `fkrel->pk_table`.
    pub pktable: Oid,
    /// `values[3]` — `array_in(fkrel->pk_columns)` rendered as text[] elements.
    pub pkcols: Vec<Vec<u8>>,
    /// `values[4]` — `fkrel->is_array`.
    pub is_array: bool,
    /// `values[5]` — `fkrel->is_opt`.
    pub is_opt: bool,
}

/// One step of `pg_basetype`'s domain-stack walk (misc.c:590).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TypeBaseStep {
    /// `typTup->typtype == TYPTYPE_DOMAIN` (misc.c:599).
    pub is_domain: bool,
    /// `typTup->typbasetype` (misc.c:606): the next type to inspect when this is
    /// a domain.
    pub typbasetype: Oid,
}
