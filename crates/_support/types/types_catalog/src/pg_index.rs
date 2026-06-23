//! `pg_index` catalog row layout, attribute numbers, and the full-row INSERT
//! carrier (`catalog/pg_index.h`, PostgreSQL 18.3).
//!
//! This is the DDL-write face of `pg_index`: the complete 23-column set that
//! `UpdateIndexRelation` (`catalog/index.c`) builds when creating a new index.
//! It is distinct from the relcache-read projection
//! `rel::FormData_pg_index` (the trimmed `rd_index` payload that ports
//! actually read) and from the in-place UPDATE producer. The catalog-indexing
//! owner forms a heap tuple from [`PgIndexInsertRow`] against the open pg_index
//! descriptor and `CatalogTupleInsert`s it.
//!
//! Field/value/order audited column-for-column against `pg_index.h`'s
//! `CATALOG(pg_index,2610,IndexRelationId)` block and the
//! `values[Anum_pg_index_* - 1] = ...` assignment table in
//! `UpdateIndexRelation`.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use ::types_core::primitive::{AttrNumber, Oid};

/* ==========================================================================
 * Catalog relation + index OIDs (pg_index.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `IndexRelationId` — `pg_index` (OID 2610, the `CATALOG(pg_index,2610,...)`).
pub const IndexRelationId: Oid = 2610;
/// `IndexIndrelidIndexId` — `pg_index_indrelid_index` (OID 2678).
pub const IndexIndrelidIndexId: Oid = 2678;
/// `IndexRelidIndexId` — `pg_index_indexrelid_index` (OID 2679, the pkey).
pub const IndexRelidIndexId: Oid = 2679;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_index; pg_index.h).
 * 1-based, matching the C declaration order in the CATALOG(...) block.
 * ======================================================================== */

pub const Anum_pg_index_indexrelid: i16 = 1;
pub const Anum_pg_index_indrelid: i16 = 2;
pub const Anum_pg_index_indnatts: i16 = 3;
pub const Anum_pg_index_indnkeyatts: i16 = 4;
pub const Anum_pg_index_indisunique: i16 = 5;
pub const Anum_pg_index_indnullsnotdistinct: i16 = 6;
pub const Anum_pg_index_indisprimary: i16 = 7;
pub const Anum_pg_index_indisexclusion: i16 = 8;
pub const Anum_pg_index_indimmediate: i16 = 9;
pub const Anum_pg_index_indisclustered: i16 = 10;
pub const Anum_pg_index_indisvalid: i16 = 11;
pub const Anum_pg_index_indcheckxmin: i16 = 12;
pub const Anum_pg_index_indisready: i16 = 13;
pub const Anum_pg_index_indislive: i16 = 14;
pub const Anum_pg_index_indisreplident: i16 = 15;
pub const Anum_pg_index_indkey: i16 = 16;
pub const Anum_pg_index_indcollation: i16 = 17;
pub const Anum_pg_index_indclass: i16 = 18;
pub const Anum_pg_index_indoption: i16 = 19;
pub const Anum_pg_index_indexprs: i16 = 20;
pub const Anum_pg_index_indpred: i16 = 21;

/// `Natts_pg_index` — number of columns (pg_index.h: 21 columns total; the
/// `indkey`/`indcollation`/`indclass`/`indoption` vectors and
/// `indexprs`/`indpred` pg_node_trees are each a single column).
pub const Natts_pg_index: usize = 21;

/* ==========================================================================
 * indoption bits (EXPOSE_TO_CLIENT_CODE block of pg_index.h).
 * ======================================================================== */

/// `INDOPTION_DESC` — values are in reverse order.
pub const INDOPTION_DESC: u16 = 0x0001;
/// `INDOPTION_NULLS_FIRST` — NULLs are first instead of last.
pub const INDOPTION_NULLS_FIRST: u16 = 0x0002;

/* ==========================================================================
 * Full-row INSERT carrier.
 * ======================================================================== */

/// Every `pg_index` column `UpdateIndexRelation` (`catalog/index.c`) writes
/// when creating a new index. The scalar columns are the `Int16GetDatum` /
/// `BoolGetDatum` / `ObjectIdGetDatum` values built from the `IndexInfo` and the
/// `UpdateIndexRelation` arguments; the four vector columns
/// (`indkey`/`indcollation`/`indclass`/`indoption`) are the
/// `buildint2vector`/`buildoidvector`-packed arrays; the two `pg_node_tree`
/// columns are the `nodeToString` images:
///
/// * `indkey` — `indexInfo->ii_IndexAttrNumbers[0 .. ii_NumIndexAttrs]`
///   (`buildint2vector(NULL, ii_NumIndexAttrs)` then fill). Length is
///   `indnatts`.
/// * `indcollation` / `indclass` — the `collationOids` / `opclassOids` arrays
///   (`buildoidvector(..., ii_NumIndexKeyAttrs)`). Length is `indnkeyatts`.
/// * `indoption` — the `coloptions` array (`buildint2vector(...,
///   ii_NumIndexKeyAttrs)`). Length is `indnkeyatts`.
/// * `indexprs` — `nodeToString(indexInfo->ii_Expressions)` as a `text` varlena,
///   or `None` for the C `exprsDatum == (Datum) 0` (store SQL NULL). Carried
///   pre-serialized (the `nodeToString` image), mirroring `pg_attrdef.adbin`.
/// * `indpred` — `nodeToString(make_ands_explicit(indexInfo->ii_Predicate))` as
///   a `text` varlena, or `None` for SQL NULL. Carried pre-serialized.
///
/// The boolean columns `indisclustered`, `indcheckxmin`, and `indisreplident`
/// are always written `false` by `UpdateIndexRelation`, and `indislive` always
/// `true`; they are still carried explicitly so the carrier is a faithful image
/// of every value the C writes.
#[derive(Clone, Debug)]
pub struct PgIndexInsertRow {
    /// `indexrelid` — `ObjectIdGetDatum(indexoid)`.
    pub indexrelid: Oid,
    /// `indrelid` — `ObjectIdGetDatum(heapoid)`.
    pub indrelid: Oid,
    /// `indnatts` — `Int16GetDatum(indexInfo->ii_NumIndexAttrs)`.
    pub indnatts: i16,
    /// `indnkeyatts` — `Int16GetDatum(indexInfo->ii_NumIndexKeyAttrs)`.
    pub indnkeyatts: i16,
    /// `indisunique` — `BoolGetDatum(indexInfo->ii_Unique)`.
    pub indisunique: bool,
    /// `indnullsnotdistinct` — `BoolGetDatum(indexInfo->ii_NullsNotDistinct)`.
    pub indnullsnotdistinct: bool,
    /// `indisprimary` — `BoolGetDatum(primary)`.
    pub indisprimary: bool,
    /// `indisexclusion` — `BoolGetDatum(isexclusion)`.
    pub indisexclusion: bool,
    /// `indimmediate` — `BoolGetDatum(immediate)`.
    pub indimmediate: bool,
    /// `indisclustered` — `BoolGetDatum(false)` in `UpdateIndexRelation`.
    pub indisclustered: bool,
    /// `indisvalid` — `BoolGetDatum(isvalid)`.
    pub indisvalid: bool,
    /// `indcheckxmin` — `BoolGetDatum(false)` in `UpdateIndexRelation`.
    pub indcheckxmin: bool,
    /// `indisready` — `BoolGetDatum(isready)`.
    pub indisready: bool,
    /// `indislive` — `BoolGetDatum(true)` in `UpdateIndexRelation`.
    pub indislive: bool,
    /// `indisreplident` — `BoolGetDatum(false)` in `UpdateIndexRelation`.
    pub indisreplident: bool,
    /// `indkey` — the heap-attribute numbers (`int2vector`); length `indnatts`.
    pub indkey: Vec<AttrNumber>,
    /// `indcollation` — collation OIDs (`oidvector`); length `indnkeyatts`.
    pub indcollation: Vec<Oid>,
    /// `indclass` — opclass OIDs (`oidvector`); length `indnkeyatts`.
    pub indclass: Vec<Oid>,
    /// `indoption` — per-column flags (`int2vector`); length `indnkeyatts`.
    pub indoption: Vec<i16>,
    /// `indexprs` — the `nodeToString(ii_Expressions)` image, or `None` for SQL
    /// NULL (the C `exprsDatum == (Datum) 0`).
    pub indexprs: Option<String>,
    /// `indpred` — the `nodeToString(make_ands_explicit(ii_Predicate))` image,
    /// or `None` for SQL NULL (the C `predDatum == (Datum) 0`).
    pub indpred: Option<String>,
}
