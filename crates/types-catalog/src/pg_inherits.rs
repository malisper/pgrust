//! `pg_inherits` catalog row layout and constants (`catalog/pg_inherits.h`,
//! PostgreSQL 18.3), trimmed to what the `backend-catalog-pg-inherits` port
//! reads and writes.

use types_core::primitive::{AttrNumber, Oid};

/* ==========================================================================
 * Catalog relation + index OIDs (pg_inherits.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `InheritsRelationId` — `pg_inherits` (OID 2611).
pub const InheritsRelationId: Oid = 2611;
/// `InheritsRelidSeqnoIndexId` — `pg_inherits_relid_seqno_index` (OID 2680),
/// the unique pkey on `(inhrelid, inhseqno)`.
pub const InheritsRelidSeqnoIndexId: Oid = 2680;
/// `InheritsParentIndexId` — `pg_inherits_parent_index` (OID 2187), on
/// `(inhparent)`.
pub const InheritsParentIndexId: Oid = 2187;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_inherits).
 * ======================================================================== */

pub const Anum_pg_inherits_inhrelid: AttrNumber = 1;
pub const Anum_pg_inherits_inhparent: AttrNumber = 2;
pub const Anum_pg_inherits_inhseqno: AttrNumber = 3;
pub const Anum_pg_inherits_inhdetachpending: AttrNumber = 4;

/// `Natts_pg_inherits` — number of columns.
pub const Natts_pg_inherits: usize = 4;

/* ==========================================================================
 * Row carriers.
 * ======================================================================== */

/// The columns of one scanned `pg_inherits` row
/// (`(Form_pg_inherits) GETSTRUCT(tup)`). All columns are non-null
/// fixed-length.
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_inherits {
    pub inhrelid: Oid,
    pub inhparent: Oid,
    pub inhseqno: i32,
    pub inhdetachpending: bool,
}

/// The values `StoreSingleInheritance` builds for `heap_form_tuple` +
/// `CatalogTupleInsert` (`inhdetachpending` is always inserted `false`). All
/// columns are non-null fixed-length.
#[derive(Clone, Copy, Debug)]
pub struct PgInheritsInsertRow {
    pub inhrelid: Oid,
    pub inhparent: Oid,
    pub inhseqno: i32,
    pub inhdetachpending: bool,
}

/// The full replacement row `MarkInheritDetached` writes back via
/// `CatalogTupleUpdate` (the C `heap_copytuple` of the scanned tuple with
/// `inhdetachpending` flipped to `true`). Every `pg_inherits` column is
/// fixed-width and NOT NULL, so re-forming the whole row from the scanned
/// values (with the one changed column) is bit-identical to the C's in-place
/// `GETSTRUCT` field set. The carrier holds the new value of every column.
#[derive(Clone, Copy, Debug)]
pub struct PgInheritsUpdateRow {
    pub inhrelid: Oid,
    pub inhparent: Oid,
    pub inhseqno: i32,
    pub inhdetachpending: bool,
}
