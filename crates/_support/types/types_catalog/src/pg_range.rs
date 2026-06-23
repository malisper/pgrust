//! `pg_range` catalog row layout and constants (`catalog/pg_range.h`,
//! PostgreSQL 18.3), trimmed to what the `backend-catalog-pg-range` port reads.

use ::types_core::primitive::Oid;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_range.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `RangeRelationId` — `pg_range` (OID 3541).
pub const RangeRelationId: Oid = 3541;
/// `RangeTypidIndexId` — `pg_range_rngtypid_index` (OID 3542).
pub const RangeTypidIndexId: Oid = 3542;
/// `RangeMultirangeTypidIndexId` — `pg_range_rngmultitypid_index` (OID 2228).
pub const RangeMultirangeTypidIndexId: Oid = 2228;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_range).
 * ======================================================================== */

pub const Anum_pg_range_rngtypid: i16 = 1;
pub const Anum_pg_range_rngsubtype: i16 = 2;
pub const Anum_pg_range_rngmultitypid: i16 = 3;
pub const Anum_pg_range_rngcollation: i16 = 4;
pub const Anum_pg_range_rngsubopc: i16 = 5;
pub const Anum_pg_range_rngcanonical: i16 = 6;
pub const Anum_pg_range_rngsubdiff: i16 = 7;

/// `Natts_pg_range` — number of columns.
pub const Natts_pg_range: usize = 7;

/* ==========================================================================
 * Row carrier.
 * ======================================================================== */

/// The values `RangeCreate` builds for `heap_form_tuple` +
/// `CatalogTupleInsert`. `pg_range` has no `oid` column; all columns are
/// non-null fixed-length. (`rngcanonical` / `rngsubdiff` are `regproc`, i.e.
/// `Oid`.)
#[derive(Clone, Copy, Debug)]
pub struct PgRangeInsertRow {
    pub rngtypid: Oid,
    pub rngsubtype: Oid,
    pub rngmultitypid: Oid,
    pub rngcollation: Oid,
    pub rngsubopc: Oid,
    pub rngcanonical: Oid,
    pub rngsubdiff: Oid,
}
