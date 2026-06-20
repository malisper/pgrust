//! `pg_attribute` catalog row layout, attribute numbers, and the INSERT
//! carriers (`catalog/pg_attribute.h`, PostgreSQL 18.3).
//!
//! This is the DDL-write face of `pg_attribute`: the complete column set
//! `InsertPgAttributeTuples` (`catalog/heap.c`) writes. The fixed-layout part
//! (columns 1..=20, the part copied into tuple descriptors) is taken from each
//! `Form_pg_attribute` (`TupleDescAttr(tupdesc, i)`); the variable-length /
//! nullable tail (`attstattarget` / `attacl` / `attoptions` / `attfdwoptions`
//! / `attmissingval`) comes from the per-attribute `FormExtraData_pg_attribute`
//! (`attstattarget` / `attoptions`) or is stored NULL ("not set for new
//! columns").
//!
//! The catalog-indexing owner forms one heap tuple per attribute from a
//! [`PgAttributeInsertRow`] against the open pg_attribute descriptor and
//! multi-inserts the batch (`CatalogTuplesMultiInsertWithInfo`).

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::Oid;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_attribute.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `AttributeRelationId` — `pg_attribute` (OID 1249).
pub const AttributeRelationId: Oid = 1249;
/// `AttributeRelidNameIndexId` — `pg_attribute_relid_attnam_index` (OID 2658).
pub const AttributeRelidNameIndexId: Oid = 2658;
/// `AttributeRelidNumIndexId` — `pg_attribute_relid_attnum_index` (OID 2659).
pub const AttributeRelidNumIndexId: Oid = 2659;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_attribute).
 * ======================================================================== */

pub const Anum_pg_attribute_attrelid: i16 = 1;
pub const Anum_pg_attribute_attname: i16 = 2;
pub const Anum_pg_attribute_atttypid: i16 = 3;
pub const Anum_pg_attribute_attlen: i16 = 4;
pub const Anum_pg_attribute_attnum: i16 = 5;
pub const Anum_pg_attribute_atttypmod: i16 = 6;
pub const Anum_pg_attribute_attndims: i16 = 7;
pub const Anum_pg_attribute_attbyval: i16 = 8;
pub const Anum_pg_attribute_attalign: i16 = 9;
pub const Anum_pg_attribute_attstorage: i16 = 10;
pub const Anum_pg_attribute_attcompression: i16 = 11;
pub const Anum_pg_attribute_attnotnull: i16 = 12;
pub const Anum_pg_attribute_atthasdef: i16 = 13;
pub const Anum_pg_attribute_atthasmissing: i16 = 14;
pub const Anum_pg_attribute_attidentity: i16 = 15;
pub const Anum_pg_attribute_attgenerated: i16 = 16;
pub const Anum_pg_attribute_attisdropped: i16 = 17;
pub const Anum_pg_attribute_attislocal: i16 = 18;
pub const Anum_pg_attribute_attinhcount: i16 = 19;
pub const Anum_pg_attribute_attcollation: i16 = 20;
pub const Anum_pg_attribute_attstattarget: i16 = 21;
pub const Anum_pg_attribute_attacl: i16 = 22;
pub const Anum_pg_attribute_attoptions: i16 = 23;
pub const Anum_pg_attribute_attfdwoptions: i16 = 24;
pub const Anum_pg_attribute_attmissingval: i16 = 25;

/// `Natts_pg_attribute` — number of columns (pg_attribute.h).
pub const Natts_pg_attribute: usize = 25;

/* ==========================================================================
 * INSERT carriers.
 * ======================================================================== */

/// One `pg_attribute` row `InsertPgAttributeTuples` writes for a new relation's
/// column. Columns 1..=20 are the fixed-layout part copied from the
/// `Form_pg_attribute` (`TupleDescAttr(tupdesc, i)`); the trailing nullable
/// fields mirror the `FormExtraData_pg_attribute` Datums and the always-NULL
/// `attacl` / `attfdwoptions` / `attmissingval` columns.
///
/// * `attstattarget` — `None` is the C `attrs_extra->attstattarget.isnull`
///   (or the no-`tupdesc_extra` path: store SQL NULL); `Some` is the explicit
///   statistics target.
/// * `attoptions` — `None` is SQL NULL; `Some` is the built `text[]` varlena
///   image (`attrs_extra->attoptions.value`).
/// * `attacl` / `attfdwoptions` / `attmissingval` are always stored NULL here
///   (C: "not set for new columns").
#[derive(Clone, Debug)]
pub struct PgAttributeInsertRow {
    pub attrelid: Oid,
    /// `NameData attname` — a `namestrcpy`-normalized 64-byte image.
    pub attname: [u8; 64],
    pub atttypid: Oid,
    pub attlen: i16,
    pub attnum: i16,
    pub atttypmod: i32,
    pub attndims: i16,
    pub attbyval: bool,
    pub attalign: i8,
    pub attstorage: i8,
    pub attcompression: i8,
    pub attnotnull: bool,
    pub atthasdef: bool,
    pub atthasmissing: bool,
    pub attidentity: i8,
    pub attgenerated: i8,
    pub attisdropped: bool,
    pub attislocal: bool,
    pub attinhcount: i16,
    pub attcollation: Oid,
    /// `attstattarget` — `None` for SQL NULL (the default / no explicit value).
    pub attstattarget: Option<i16>,
    /// `attoptions` — the built `text[]` varlena image, or `None` for SQL NULL.
    pub attoptions: Option<Vec<u8>>,
}

/* ==========================================================================
 * UPDATE carrier (the ALTER TABLE per-`Anum` field-modify path).
 * ======================================================================== */

/// The selectively-replaced columns an `ALTER TABLE` subcommand writes back to
/// an existing `pg_attribute` row (the `ATExec*` pattern:
/// `SearchSysCacheCopy(ATTNUM, relid, attnum)` → modify the `Form_pg_attribute`
/// field(s) → `CatalogTupleUpdate`).
///
/// Each field is wrapped in [`Option`]: `Some` means *replace this column*
/// (mirroring the C `replaces[Anum_pg_attribute_xxx - 1] = true`), `None` means
/// *leave the on-disk value untouched* (the column is carried over from the
/// original scanned tuple by `heap_modify_tuple`). For the nullable columns the
/// inner value is itself an `Option`: `Some(Some(v))` stores `v`, `Some(None)`
/// stores SQL NULL, `None` leaves the column unchanged.
///
/// The columns cover the portable `ALTER TABLE` F2+ subcommand families:
///
/// * `attnotnull` — `SET`/`DROP NOT NULL` (`ATExecSetNotNull` /
///   `ATExecDropNotNull`).
/// * `atthasdef` — `SET`/`DROP DEFAULT` (`StoreAttrDefault` /
///   `RemoveAttrDefault` clear the flag on the pg_attribute row).
/// * `attstattarget` — `SET STATISTICS` (`ATExecSetStatistics`); `Some(None)`
///   stores SQL NULL (reset to default).
/// * `attstorage` — `SET STORAGE` (`ATExecSetStorage`).
/// * `attoptions` — `SET`/`RESET (...)` per-column options
///   (`ATExecSetOptions`); the built `text[]` varlena image, or SQL NULL.
/// * `atttypid` / `atttypmod` / `attcollation` / `attndims` / `attlen` /
///   `attbyval` / `attalign` / `attstorage` — `ALTER COLUMN TYPE`
///   (`ATExecAlterColumnType` rewrites the type-layout columns).
/// * `attisdropped` / `attgenerated` / `attinhcount` / `attislocal` — `DROP
///   COLUMN` (`ATExecDropColumn`: set `attisdropped = true`, clear
///   `attgenerated`/`atthasdef`/`attinhcount`, rename to `........pg.dropped.N`).
/// * `attname` — `RENAME COLUMN` (`renameatt_internal`); a `namestrcpy`-padded
///   64-byte image.
#[derive(Clone, Debug, Default)]
pub struct PgAttributeUpdateRow {
    /// `attname` — `RENAME COLUMN` (a `namestrcpy`-normalized 64-byte image).
    pub attname: Option<[u8; 64]>,
    pub atttypid: Option<Oid>,
    pub attlen: Option<i16>,
    pub atttypmod: Option<i32>,
    pub attndims: Option<i16>,
    pub attbyval: Option<bool>,
    pub attalign: Option<i8>,
    pub attstorage: Option<i8>,
    pub attcompression: Option<i8>,
    pub attnotnull: Option<bool>,
    pub atthasdef: Option<bool>,
    pub atthasmissing: Option<bool>,
    pub attidentity: Option<i8>,
    pub attgenerated: Option<i8>,
    pub attisdropped: Option<bool>,
    pub attislocal: Option<bool>,
    pub attinhcount: Option<i16>,
    pub attcollation: Option<Oid>,
    /// `attstattarget` — `Some(Some(v))` stores `v`, `Some(None)` stores SQL
    /// NULL (`SET STATISTICS DEFAULT`), `None` leaves the column unchanged.
    pub attstattarget: Option<Option<i16>>,
    /// `attoptions` — `Some(Some(image))` stores the built `text[]` varlena,
    /// `Some(None)` stores SQL NULL (`RESET`), `None` leaves it unchanged.
    pub attoptions: Option<Option<Vec<u8>>>,
    /// `attmissingval` — `Some(Some(image))` stores the built `anyarray`
    /// varlena (`StoreAttrMissingVal`'s `construct_array`), `Some(None)` stores
    /// SQL NULL (`RelationClearMissing` clears the missing value), `None`
    /// leaves it unchanged.
    pub attmissingval: Option<Option<Vec<u8>>>,
    /// `attacl` — `Some(Some(image))` stores the built `aclitem[]` varlena
    /// (`change_owner_fix_column_acls`'s owner-rewrite), `Some(None)` stores SQL
    /// NULL, `None` leaves the column unchanged.
    pub attacl: Option<Option<Vec<u8>>>,
}
