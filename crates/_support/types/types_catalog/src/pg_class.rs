//! `pg_class` catalog row layout, attribute numbers, and the full-row INSERT
//! carrier (`catalog/pg_class.h`, PostgreSQL 18.3).
//!
//! This is the DDL-write face of `pg_class`: the complete column set
//! (`InsertPgClassTuple` in `catalog/heap.c` builds every column from
//! `new_rel_desc->rd_rel` plus the `relacl` / `reloptions` Datums). It is
//! distinct from the relcache-read projection `rel::FormData_pg_class`
//! (the `rd_rel` payload, whose C definition explicitly omits the variable-
//! length tail `relacl` / `reloptions` / `relpartbound`) and from the CLUSTER
//! swap face `types_cluster::PgClassForm`. The catalog-indexing owner forms a
//! heap tuple from [`PgClassInsertRow`] against the open pg_class descriptor
//! and `CatalogTupleInsert`s it.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::{Oid, TransactionId};

/* ==========================================================================
 * Catalog relation + index OIDs (pg_class.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `RelationRelationId` — `pg_class` (OID 1259).
pub const RelationRelationId: Oid = 1259;
/// `ClassOidIndexId` — `pg_class_oid_index` (OID 2662).
pub const ClassOidIndexId: Oid = 2662;
/// `ClassNameNspIndexId` — `pg_class_relname_nsp_index` (OID 2663).
pub const ClassNameNspIndexId: Oid = 2663;
/// `ClassTblspcRelfilenodeIndexId` — `pg_class_tblspc_relfilenode_index`
/// (OID 3455).
pub const ClassTblspcRelfilenodeIndexId: Oid = 3455;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_class; pg_class.h).
 * ======================================================================== */

pub const Anum_pg_class_oid: i16 = 1;
pub const Anum_pg_class_relname: i16 = 2;
pub const Anum_pg_class_relnamespace: i16 = 3;
pub const Anum_pg_class_reltype: i16 = 4;
pub const Anum_pg_class_reloftype: i16 = 5;
pub const Anum_pg_class_relowner: i16 = 6;
pub const Anum_pg_class_relam: i16 = 7;
pub const Anum_pg_class_relfilenode: i16 = 8;
pub const Anum_pg_class_reltablespace: i16 = 9;
pub const Anum_pg_class_relpages: i16 = 10;
pub const Anum_pg_class_reltuples: i16 = 11;
pub const Anum_pg_class_relallvisible: i16 = 12;
pub const Anum_pg_class_relallfrozen: i16 = 13;
pub const Anum_pg_class_reltoastrelid: i16 = 14;
pub const Anum_pg_class_relhasindex: i16 = 15;
pub const Anum_pg_class_relisshared: i16 = 16;
pub const Anum_pg_class_relpersistence: i16 = 17;
pub const Anum_pg_class_relkind: i16 = 18;
pub const Anum_pg_class_relnatts: i16 = 19;
pub const Anum_pg_class_relchecks: i16 = 20;
pub const Anum_pg_class_relhasrules: i16 = 21;
pub const Anum_pg_class_relhastriggers: i16 = 22;
pub const Anum_pg_class_relhassubclass: i16 = 23;
pub const Anum_pg_class_relrowsecurity: i16 = 24;
pub const Anum_pg_class_relforcerowsecurity: i16 = 25;
pub const Anum_pg_class_relispopulated: i16 = 26;
pub const Anum_pg_class_relreplident: i16 = 27;
pub const Anum_pg_class_relispartition: i16 = 28;
pub const Anum_pg_class_relrewrite: i16 = 29;
pub const Anum_pg_class_relfrozenxid: i16 = 30;
pub const Anum_pg_class_relminmxid: i16 = 31;
pub const Anum_pg_class_relacl: i16 = 32;
pub const Anum_pg_class_reloptions: i16 = 33;
pub const Anum_pg_class_relpartbound: i16 = 34;

/// `Natts_pg_class` — number of columns (pg_class.h).
pub const Natts_pg_class: usize = 34;

/* ==========================================================================
 * Full-row INSERT carrier.
 * ======================================================================== */

/// Every `pg_class` column `InsertPgClassTuple` (`catalog/heap.c`) writes when
/// creating a new relation. The fixed columns are taken from the new
/// relation's `rd_rel` (`new_rel_desc->rd_rel`); `oid` is `new_rel_oid`. The
/// trailing variable-length columns mirror the C `relacl` / `reloptions`
/// Datum arguments and the always-NULL `relpartbound`:
///
/// * `relacl` — `None` is the C `relacl == (Datum) 0` (store SQL NULL); `Some`
///   is the already-built `aclitem[]` varlena image.
/// * `reloptions` — `None` is the C `reloptions == (Datum) 0` (store SQL NULL);
///   `Some` is the already-built `text[]` varlena image.
/// * `relpartbound` is always stored NULL here (C: `nulls[... relpartbound ...]
///   = true`; it is set later by updating the tuple if necessary).
#[derive(Clone, Debug)]
pub struct PgClassInsertRow {
    /// `new_rel_oid` (the C argument; the relation's already-assigned OID).
    pub oid: Oid,
    /// `NameData relname` — a `namestrcpy`-normalized 64-byte image.
    pub relname: [u8; 64],
    pub relnamespace: Oid,
    pub reltype: Oid,
    pub reloftype: Oid,
    pub relowner: Oid,
    pub relam: Oid,
    pub relfilenode: Oid,
    pub reltablespace: Oid,
    pub relpages: i32,
    pub reltuples: f32,
    pub relallvisible: i32,
    pub relallfrozen: i32,
    pub reltoastrelid: Oid,
    pub relhasindex: bool,
    pub relisshared: bool,
    pub relpersistence: i8,
    pub relkind: i8,
    pub relnatts: i16,
    pub relchecks: i16,
    pub relhasrules: bool,
    pub relhastriggers: bool,
    pub relrowsecurity: bool,
    pub relforcerowsecurity: bool,
    pub relhassubclass: bool,
    pub relispopulated: bool,
    pub relreplident: i8,
    pub relispartition: bool,
    pub relrewrite: Oid,
    pub relfrozenxid: TransactionId,
    /// `MultiXactId relminmxid` (stored as the underlying `uint32`).
    pub relminmxid: u32,
    /// `relacl` — the built `aclitem[]` varlena image, or `None` for SQL NULL.
    pub relacl: Option<Vec<u8>>,
    /// `reloptions` — the built `text[]` varlena image, or `None` for SQL NULL.
    pub reloptions: Option<Vec<u8>>,
}
