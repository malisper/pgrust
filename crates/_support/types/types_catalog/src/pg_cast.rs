//! `pg_cast` catalog row layout and constants (`catalog/pg_cast.h`,
//! PostgreSQL 18.3), trimmed to what the `backend-catalog-pg-cast` port reads.

use ::types_core::primitive::Oid;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_cast.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `CastRelationId` — `pg_cast` (OID 2605).
pub const CastRelationId: Oid = 2605;
/// `CastOidIndexId` — `pg_cast_oid_index` (OID 2660).
pub const CastOidIndexId: Oid = 2660;
/// `CastSourceTargetIndexId` — `pg_cast_source_target_index` (OID 2661).
pub const CastSourceTargetIndexId: Oid = 2661;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_cast).
 * ======================================================================== */

pub const Anum_pg_cast_oid: i16 = 1;
pub const Anum_pg_cast_castsource: i16 = 2;
pub const Anum_pg_cast_casttarget: i16 = 3;
pub const Anum_pg_cast_castfunc: i16 = 4;
pub const Anum_pg_cast_castcontext: i16 = 5;
pub const Anum_pg_cast_castmethod: i16 = 6;

/// `Natts_pg_cast` — number of columns.
pub const Natts_pg_cast: usize = 6;

/* ==========================================================================
 * Row carriers.
 * ======================================================================== */

/// The fixed-width scalar columns of one scanned `pg_cast` row
/// (`(Form_pg_cast) GETSTRUCT(tup)`). All columns are non-null fixed-length.
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_cast {
    pub oid: Oid,
    pub castsource: Oid,
    pub casttarget: Oid,
    pub castfunc: Oid,
    pub castcontext: i8,
    pub castmethod: i8,
}

/// The values `CastCreate` builds for `heap_form_tuple` + `CatalogTupleInsert`
/// (the `oid` column is freshly allocated by the owner via
/// `GetNewOidWithIndex`, so it is NOT carried here). All columns are non-null
/// fixed-length.
#[derive(Clone, Copy, Debug)]
pub struct PgCastInsertRow {
    pub castsource: Oid,
    pub casttarget: Oid,
    pub castfunc: Oid,
    pub castcontext: i8,
    pub castmethod: i8,
}
