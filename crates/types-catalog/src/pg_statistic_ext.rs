//! `pg_statistic_ext` catalog row layout and constants
//! (`catalog/pg_statistic_ext.h`, PostgreSQL 18.3), trimmed to what the
//! `backend-commands-statscmds` port reads/writes.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::primitive::Oid;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_statistic_ext.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `StatisticExtRelationId` — `pg_statistic_ext` (OID 3381).
pub const StatisticExtRelationId: Oid = 3381;
/// `StatisticExtOidIndexId` — `pg_statistic_ext_oid_index` (OID 3380).
pub const StatisticExtOidIndexId: Oid = 3380;
/// `StatisticExtNameIndexId` — `pg_statistic_ext_name_index` (OID 3997).
pub const StatisticExtNameIndexId: Oid = 3997;
/// `StatisticExtRelidIndexId` — `pg_statistic_ext_relid_index` (OID 3379).
pub const StatisticExtRelidIndexId: Oid = 3379;
/// `StatisticExtDataRelationId` — `pg_statistic_ext_data` (OID 3429).
pub const StatisticExtDataRelationId: Oid = 3429;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_statistic_ext).
 * ======================================================================== */

pub const Anum_pg_statistic_ext_oid: i16 = 1;
pub const Anum_pg_statistic_ext_stxrelid: i16 = 2;
pub const Anum_pg_statistic_ext_stxname: i16 = 3;
pub const Anum_pg_statistic_ext_stxnamespace: i16 = 4;
pub const Anum_pg_statistic_ext_stxowner: i16 = 5;
pub const Anum_pg_statistic_ext_stxkeys: i16 = 6;
pub const Anum_pg_statistic_ext_stxstattarget: i16 = 7;
pub const Anum_pg_statistic_ext_stxkind: i16 = 8;
pub const Anum_pg_statistic_ext_stxexprs: i16 = 9;

/// `Natts_pg_statistic_ext` — number of columns.
pub const Natts_pg_statistic_ext: usize = 9;

/* ==========================================================================
 * Statistics kind chars (pg_statistic_ext.h EXPOSE_TO_CLIENT_CODE).
 * ======================================================================== */

/// `STATS_EXT_NDISTINCT` — `'d'`.
pub const STATS_EXT_NDISTINCT: i8 = b'd' as i8;
/// `STATS_EXT_DEPENDENCIES` — `'f'`.
pub const STATS_EXT_DEPENDENCIES: i8 = b'f' as i8;
/// `STATS_EXT_MCV` — `'m'`.
pub const STATS_EXT_MCV: i8 = b'm' as i8;
/// `STATS_EXT_EXPRESSIONS` — `'e'`.
pub const STATS_EXT_EXPRESSIONS: i8 = b'e' as i8;

/* ==========================================================================
 * Row carriers.
 * ======================================================================== */

/// The values `CreateStatistics` builds for `heap_form_tuple` +
/// `CatalogTupleInsert` (`commands/statscmds.c`). The `oid` column is freshly
/// allocated by the indexing owner via `GetNewOidWithIndex`, so it is NOT
/// carried here.
///
/// * `stxname` is a NUL-padded 64-byte `NameData` image (the C `namestrcpy`
///   already ran in the port).
/// * `stxstattarget` is left NULL on a fresh CREATE (the C
///   `nulls[Anum_pg_statistic_ext_stxstattarget - 1] = true`), so it is not
///   carried.
/// * `stxkeys` is the sorted attnum list, packed into an `int2vector` by the
///   indexing owner (`buildint2vector`).
/// * `stxkind` is the enabled-kinds char list, packed into a `char[]`
///   `ArrayType` by the indexing owner (`construct_array_builtin(..., CHAROID)`).
/// * `stxexprs` is the `nodeToString(stxexprs)` serialization (already built by
///   the port), packed into a `text` by the indexing owner; `None` ⇒ the column
///   is NULL.
#[derive(Clone, Debug)]
pub struct PgStatisticExtInsertRow {
    pub stxrelid: Oid,
    pub stxname: [u8; 64],
    pub stxnamespace: Oid,
    pub stxowner: Oid,
    pub stxkeys: Vec<i16>,
    pub stxkind: Vec<i8>,
    pub stxexprs: Option<String>,
}
