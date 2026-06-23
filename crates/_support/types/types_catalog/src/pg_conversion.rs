//! `pg_conversion` catalog row layout and constants
//! (`catalog/pg_conversion.h`, PostgreSQL 18.3), trimmed to what the
//! `backend-catalog-pg-conversion` port reads.

use types_core::primitive::Oid;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_conversion.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `ConversionRelationId` — `pg_conversion` (OID 2607).
pub const ConversionRelationId: Oid = 2607;
/// `ConversionOidIndexId` — `pg_conversion_oid_index` (OID 2670).
pub const ConversionOidIndexId: Oid = 2670;
/// `ConversionNameNspIndexId` — `pg_conversion_name_nsp_index` (OID 2669).
pub const ConversionNameNspIndexId: Oid = 2669;
/// `ConversionDefaultIndexId` — `pg_conversion_default_index` (OID 2668).
pub const ConversionDefaultIndexId: Oid = 2668;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_conversion).
 * ======================================================================== */

pub const Anum_pg_conversion_oid: i16 = 1;
pub const Anum_pg_conversion_conname: i16 = 2;
pub const Anum_pg_conversion_connamespace: i16 = 3;
pub const Anum_pg_conversion_conowner: i16 = 4;
pub const Anum_pg_conversion_conforencoding: i16 = 5;
pub const Anum_pg_conversion_contoencoding: i16 = 6;
pub const Anum_pg_conversion_conproc: i16 = 7;
pub const Anum_pg_conversion_condefault: i16 = 8;

/// `Natts_pg_conversion` — number of columns.
pub const Natts_pg_conversion: usize = 8;

/* ==========================================================================
 * Row carriers.
 * ======================================================================== */

/// The fixed-width scalar columns of one scanned `pg_conversion` row
/// (`(Form_pg_conversion) GETSTRUCT(tup)`). `conname` is the 64-byte
/// `NameData` image. All columns are non-null fixed-length.
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_conversion {
    pub oid: Oid,
    pub conname: [u8; 64],
    pub connamespace: Oid,
    pub conowner: Oid,
    pub conforencoding: i32,
    pub contoencoding: i32,
    pub conproc: Oid,
    pub condefault: bool,
}

/// The values `ConversionCreate` builds for `heap_form_tuple` +
/// `CatalogTupleInsert` (the `oid` column is freshly allocated by the owner via
/// `GetNewOidWithIndex`, so it is NOT carried here). `conname` is the 64-byte
/// `NameData` image. All columns are non-null fixed-length.
#[derive(Clone, Copy, Debug)]
pub struct PgConversionInsertRow {
    pub conname: [u8; 64],
    pub connamespace: Oid,
    pub conowner: Oid,
    pub conforencoding: i32,
    pub contoencoding: i32,
    pub conproc: Oid,
    pub condefault: bool,
}
