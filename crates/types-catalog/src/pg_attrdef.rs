//! `pg_attrdef` catalog row layout, attribute numbers, and the INSERT carrier
//! (`catalog/pg_attrdef.h`, PostgreSQL 18.3).
//!
//! `pg_attrdef` records column default expressions. `StoreAttrDefault`
//! (`catalog/heap.c`) forms a 4-column row — `oid`, `adrelid`, `adnum`, and the
//! `adbin` `pg_node_tree` text — and `CatalogTupleInsert`s it. The
//! catalog-indexing owner forms the heap tuple from [`PgAttrdefInsertRow`].

extern crate alloc;

use alloc::string::String;

use types_core::primitive::Oid;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_attrdef.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `AttrDefaultRelationId` — `pg_attrdef` (OID 2604).
pub const AttrDefaultRelationId: Oid = 2604;
/// `AttrDefaultIndexId` — `pg_attrdef_adrelid_adnum_index` (OID 2656).
pub const AttrDefaultIndexId: Oid = 2656;
/// `AttrDefaultOidIndexId` — `pg_attrdef_oid_index` (OID 2657).
pub const AttrDefaultOidIndexId: Oid = 2657;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_attrdef).
 * ======================================================================== */

pub const Anum_pg_attrdef_oid: i16 = 1;
pub const Anum_pg_attrdef_adrelid: i16 = 2;
pub const Anum_pg_attrdef_adnum: i16 = 3;
pub const Anum_pg_attrdef_adbin: i16 = 4;

/// `Natts_pg_attrdef` — number of columns (pg_attrdef.h).
pub const Natts_pg_attrdef: usize = 4;

/* ==========================================================================
 * Row carriers.
 * ======================================================================== */

/// The fixed-width scalar columns of a scanned `pg_attrdef` row
/// (`(Form_pg_attrdef) GETSTRUCT(tup)`). The `adbin` `pg_node_tree` column is
/// variable-length and is not part of this fixed projection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FormData_pg_attrdef {
    pub oid: Oid,
    pub adrelid: Oid,
    pub adnum: i16,
}

/// The values `StoreAttrDefault` (`catalog/heap.c`) builds for `heap_form_tuple`
/// + `CatalogTupleInsert`. The `oid` column is freshly allocated by the owner
/// via `GetNewOidWithIndex`, so it is NOT carried here. `adbin` is the
/// `nodeToString` representation of the default expression (`BKI_FORCE_NOT_NULL`,
/// always present).
#[derive(Clone, Debug)]
pub struct PgAttrdefInsertRow {
    pub adrelid: Oid,
    pub adnum: i16,
    pub adbin: String,
}
