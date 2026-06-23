//! ABI vocabulary for `backend/commands/statscmds.c` (CREATE/ALTER/DROP
//! STATISTICS — extended statistics objects).
//!
//! These `#[repr(C)]` structs / constants cross the boundary between the
//! rewritten `backend-commands-statscmds` crate and the rest of the backend.
//! They mirror, with identical layout/order/width, the C definitions in
//!   * `src/include/nodes/parsenodes.h`            (`AlterStatsStmt`;
//!     `CreateStatsStmt` / `StatsElem` already live in
//!     `commands_ddl_parsenodes.rs` and are re-used, not redefined)
//!   * `src/include/catalog/pg_statistic_ext.h` / `pg_statistic_ext_d.h`
//!     (`FormData_pg_statistic_ext`, the relation/index OIDs, the column
//!     attribute numbers `Anum_*`, `Natts_pg_statistic_ext`, and the
//!     `STATS_EXT_*` statistics-kind char codes)
//!   * `src/include/catalog/pg_statistic_ext_data.h` /
//!     `pg_statistic_ext_data_d.h` (`StatisticExtDataRelationId`, that
//!     catalog's `Anum_*` / `Natts_*` and the stxoid+inh index OID)
//!
//! The bare-`Id` / `Anum_` / `Natts_` spellings reproduce the C macro names so
//! the port reads 1:1 against statscmds.c.  `STATISTIC_EXT_RELATION_ID` already
//! exists in `catalog.rs`; the `StatisticExtRelationId` C-spelling alias here
//! points at it (single source of truth).

use core::ffi::c_char;

use crate::{int2vector, List, NameData, Node, NodeTag, Oid};

/* ---------------------------------------------------------------------------
 * nodes/parsenodes.h — AlterStatsStmt
 *
 * (CreateStatsStmt + StatsElem are defined in commands_ddl_parsenodes.rs.)
 * ------------------------------------------------------------------------- */

/// `typedef struct AlterStatsStmt` (parsenodes.h) — ALTER STATISTICS ... SET
/// STATISTICS.  `stxstattarget` is the parser `Node *` (an `Integer` value
/// node, or NULL for the default).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterStatsStmt {
    pub type_: NodeTag,
    /// qualified name (list of String)
    pub defnames: *mut List,
    /// statistics target
    pub stxstattarget: *mut Node,
    /// skip error if statistics object is missing
    pub missing_ok: bool,
}

/* ---------------------------------------------------------------------------
 * catalog/pg_statistic_ext.h / pg_statistic_ext_d.h
 * ------------------------------------------------------------------------- */

/// `StatisticExtRelationId` — `pg_statistic_ext` (`pg_statistic_ext_d.h`: 3381).
/// Alias of `catalog::STATISTIC_EXT_RELATION_ID` (single source of truth).
pub const StatisticExtRelationId: Oid = crate::catalog::STATISTIC_EXT_RELATION_ID;
/// `StatisticExtOidIndexId` — the OID index on `pg_statistic_ext` (3380).
pub const StatisticExtOidIndexId: Oid = 3380;
/// `StatisticExtNameIndexId` — the (name, namespace) index (3997).
pub const StatisticExtNameIndexId: Oid = 3997;
/// `StatisticExtRelidIndexId` — the stxrelid index (3379).
pub const StatisticExtRelidIndexId: Oid = 3379;

// pg_statistic_ext attribute numbers (`Anum_pg_statistic_ext_*`).
pub const Anum_pg_statistic_ext_oid: i32 = 1;
pub const Anum_pg_statistic_ext_stxrelid: i32 = 2;
pub const Anum_pg_statistic_ext_stxname: i32 = 3;
pub const Anum_pg_statistic_ext_stxnamespace: i32 = 4;
pub const Anum_pg_statistic_ext_stxowner: i32 = 5;
pub const Anum_pg_statistic_ext_stxkeys: i32 = 6;
pub const Anum_pg_statistic_ext_stxstattarget: i32 = 7;
pub const Anum_pg_statistic_ext_stxkind: i32 = 8;
pub const Anum_pg_statistic_ext_stxexprs: i32 = 9;

/// `Natts_pg_statistic_ext` — number of columns in `pg_statistic_ext` (9).
pub const Natts_pg_statistic_ext: usize = 9;

// Statistics-kind char codes (`STATS_EXT_*`, pg_statistic_ext.h).
/// `STATS_EXT_NDISTINCT` — n-distinct coefficients.
pub const STATS_EXT_NDISTINCT: c_char = b'd' as c_char;
/// `STATS_EXT_DEPENDENCIES` — functional dependencies.
pub const STATS_EXT_DEPENDENCIES: c_char = b'f' as c_char;
/// `STATS_EXT_MCV` — most-common-values list.
pub const STATS_EXT_MCV: c_char = b'm' as c_char;
/// `STATS_EXT_EXPRESSIONS` — per-expression statistics.
pub const STATS_EXT_EXPRESSIONS: c_char = b'e' as c_char;

/// `FormData_pg_statistic_ext` (`pg_statistic_ext.h`) — the fixed-width prefix
/// of a `pg_statistic_ext` tuple (everything up to and including the
/// `BKI_FORCE_NOT_NULL` `stxkeys`; the nullable/var-length `stxstattarget`,
/// `stxkind`, `stxexprs` columns are accessed via `heap_getattr`, not this
/// struct, exactly as in C where they sit behind `#ifdef CATALOG_VARLEN`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_statistic_ext {
    /// oid
    pub oid: Oid,
    /// relation containing the attributes
    pub stxrelid: Oid,
    /// statistics object name
    pub stxname: NameData,
    /// OID of statistics object's namespace
    pub stxnamespace: Oid,
    /// statistics object's owner
    pub stxowner: Oid,
    /// array of column keys
    pub stxkeys: int2vector,
}

/// `Form_pg_statistic_ext` — pointer to a `pg_statistic_ext` tuple's
/// fixed-width prefix (`GETSTRUCT` result).
pub type Form_pg_statistic_ext = *mut FormData_pg_statistic_ext;

/* ---------------------------------------------------------------------------
 * catalog/pg_statistic_ext_data.h / pg_statistic_ext_data_d.h
 * ------------------------------------------------------------------------- */

/// `StatisticExtDataRelationId` — `pg_statistic_ext_data` (3429).
pub const StatisticExtDataRelationId: Oid = 3429;
/// `StatisticExtDataStxoidInhIndexId` — the (stxoid, stxdinherit) index (3433).
pub const StatisticExtDataStxoidInhIndexId: Oid = 3433;

// pg_statistic_ext_data attribute numbers (`Anum_pg_statistic_ext_data_*`).
pub const Anum_pg_statistic_ext_data_stxoid: i32 = 1;
pub const Anum_pg_statistic_ext_data_stxdinherit: i32 = 2;
pub const Anum_pg_statistic_ext_data_stxdndistinct: i32 = 3;
pub const Anum_pg_statistic_ext_data_stxddependencies: i32 = 4;
pub const Anum_pg_statistic_ext_data_stxdmcv: i32 = 5;
pub const Anum_pg_statistic_ext_data_stxdexpr: i32 = 6;

/// `Natts_pg_statistic_ext_data` — number of columns in `pg_statistic_ext_data`.
pub const Natts_pg_statistic_ext_data: usize = 6;
