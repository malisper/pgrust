//! `pg_transform` catalog row layout and constants (`catalog/pg_transform.h`,
//! PostgreSQL 18.3), trimmed to what the `CreateTransform` catalog insert reads.

use types_core::primitive::Oid;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_transform.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `TransformRelationId` — `pg_transform` (OID 3576).
pub const TransformRelationId: Oid = 3576;
/// `TransformOidIndexId` — `pg_transform_oid_index` (OID 3574).
pub const TransformOidIndexId: Oid = 3574;
/// `TransformTypeLangIndexId` — `pg_transform_type_lang_index` (OID 3575).
pub const TransformTypeLangIndexId: Oid = 3575;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_transform).
 * ======================================================================== */

pub const Anum_pg_transform_oid: i16 = 1;
pub const Anum_pg_transform_trftype: i16 = 2;
pub const Anum_pg_transform_trflang: i16 = 3;
pub const Anum_pg_transform_trffromsql: i16 = 4;
pub const Anum_pg_transform_trftosql: i16 = 5;

/// `Natts_pg_transform` — number of columns.
pub const Natts_pg_transform: usize = 5;

/* ==========================================================================
 * Row carriers.
 * ======================================================================== */

/// The values `CreateTransform` builds for `heap_form_tuple` +
/// `CatalogTupleInsert` / `heap_modify_tuple` + `CatalogTupleUpdate`. All
/// columns are non-null fixed-length OIDs (`trffromsql`/`trftosql` may be
/// `InvalidOid`). The `oid` column is freshly allocated by the owner via
/// `GetNewOidWithIndex`, so it is not carried here.
#[derive(Clone, Copy, Debug)]
pub struct PgTransformInsertRow {
    pub trftype: Oid,
    pub trflang: Oid,
    pub trffromsql: Oid,
    pub trftosql: Oid,
}
