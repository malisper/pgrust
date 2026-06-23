//! `pg_language` catalog row layout and constants (`catalog/pg_language.h`,
//! PostgreSQL 18.3), trimmed to what the `backend-commands-proclang` port
//! reads and writes for `CREATE [OR REPLACE] LANGUAGE`.

use ::types_core::primitive::{AttrNumber, Oid};

/* ==========================================================================
 * Catalog relation + index OIDs (pg_language.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `LanguageRelationId` — `pg_language` (OID 2612).
pub const LanguageRelationId: Oid = 2612;
/// `LanguageNameIndexId` — `pg_language_name_index` (OID 2681), unique on
/// `(lanname)`.
pub const LanguageNameIndexId: Oid = 2681;
/// `LanguageOidIndexId` — `pg_language_oid_index` (OID 2682), the unique pkey
/// on `(oid)`.
pub const LanguageOidIndexId: Oid = 2682;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_language).
 * ======================================================================== */

pub const Anum_pg_language_oid: AttrNumber = 1;
pub const Anum_pg_language_lanname: AttrNumber = 2;
pub const Anum_pg_language_lanowner: AttrNumber = 3;
pub const Anum_pg_language_lanispl: AttrNumber = 4;
pub const Anum_pg_language_lanpltrusted: AttrNumber = 5;
pub const Anum_pg_language_lanplcallfoid: AttrNumber = 6;
pub const Anum_pg_language_laninline: AttrNumber = 7;
pub const Anum_pg_language_lanvalidator: AttrNumber = 8;
pub const Anum_pg_language_lanacl: AttrNumber = 9;

/// `Natts_pg_language` — number of columns (8 fixed + the `lanacl` varlen).
pub const Natts_pg_language: usize = 9;

/* ==========================================================================
 * Row carriers.
 * ======================================================================== */

/// The fixed columns of one scanned `pg_language` row
/// (`(Form_pg_language) GETSTRUCT(tup)`), trimmed to the fields the proclang
/// replace path reads (`oldform->oid`); `lanowner` is carried so the replace
/// path can confirm it leaves ownership untouched.
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_language {
    pub oid: Oid,
    pub lanowner: Oid,
}

/// The values `CreateProceduralLanguage` builds for `heap_form_tuple` /
/// `heap_modify_tuple`. `lanname` is the 64-byte `NameData` image; `lanacl` is
/// always inserted NULL (`nulls[Anum_pg_language_lanacl - 1] = true`).
#[derive(Clone, Debug)]
pub struct PgLanguageInsertRow {
    pub lanname: [u8; 64],
    pub lanowner: Oid,
    pub lanispl: bool,
    pub lanpltrusted: bool,
    pub lanplcallfoid: Oid,
    pub laninline: Oid,
    pub lanvalidator: Oid,
}
