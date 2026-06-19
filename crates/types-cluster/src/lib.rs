//! CLUSTER / VACUUM FULL command vocabulary (`commands/cluster.h`,
//! `nodes/parsenodes.h` `ClusterStmt`/`DefElem`, `commands/vacuum.h`
//! `struct VacuumCutoffs`, `catalog/index.h` reindex flags,
//! `commands/progress.h` CLUSTER progress constants), trimmed to what the
//! `backend-commands-cluster` port consumes.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::{MultiXactId, Oid, RelFileNumber, TransactionId};

/* ----------------------------------------------------------------
 * catalog/pg_class.h: the writable pg_class row copy swap_relation_files
 * mutates (a GETSTRUCT view of a SearchSysCacheCopy1 HeapTuple)
 * ---------------------------------------------------------------- */

/// The fields of `Form_pg_class` (`catalog/pg_class.h`) that
/// `swap_relation_files` / `copy_table_data` read and write on the *catalog
/// row* (the writable `SearchSysCacheCopy1` tuple), as opposed to the relcache
/// `rd_rel`. The owner's syscache seam deforms the pg_class tuple into this,
/// the cluster swap mutates it, and the owner reforms it before
/// `CatalogTupleUpdate`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PgClassForm {
    /// `NameData relname`.
    pub relname: String,
    /// `Oid relnamespace`.
    pub relnamespace: Oid,
    /// `RelFileNumber relfilenode` (`oid relfilenode` in pg_class).
    pub relfilenode: RelFileNumber,
    /// `Oid reltablespace`.
    pub reltablespace: Oid,
    /// `Oid relam`.
    pub relam: Oid,
    /// `Oid reltoastrelid`.
    pub reltoastrelid: Oid,
    /// `bool relisshared`.
    pub relisshared: bool,
    /// `char relpersistence`.
    pub relpersistence: u8,
    /// `char relkind`.
    pub relkind: u8,
    /// `int16 relnatts` — number of (live + dropped) user columns.
    pub relnatts: i16,
    /// `int32 relpages`.
    pub relpages: i32,
    /// `float4 reltuples`.
    pub reltuples: f32,
    /// `int32 relallvisible`.
    pub relallvisible: i32,
    /// `int32 relallfrozen`.
    pub relallfrozen: i32,
    /// `TransactionId relfrozenxid`.
    pub relfrozenxid: TransactionId,
    /// `MultiXactId relminmxid`.
    pub relminmxid: MultiXactId,
}

/* ----------------------------------------------------------------
 * commands/cluster.h: ClusterParams + CLUOPT flag bits
 * ---------------------------------------------------------------- */

/// `CLUOPT_VERBOSE` — print progress info.
pub const CLUOPT_VERBOSE: i32 = 0x01;
/// `CLUOPT_RECHECK` — recheck relation state.
pub const CLUOPT_RECHECK: i32 = 0x02;
/// `CLUOPT_RECHECK_ISCLUSTERED` — recheck relation state for indisclustered.
pub const CLUOPT_RECHECK_ISCLUSTERED: i32 = 0x04;

/// `ClusterParams` (`commands/cluster.h`): `{ bits32 options; }`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClusterParams {
    /// bitmask of `CLUOPT_*`.
    pub options: i32,
}

impl ClusterParams {
    /// `ClusterParams params = {0};`
    pub fn new() -> Self {
        ClusterParams { options: 0 }
    }
}

/* ----------------------------------------------------------------
 * nodes/parsenodes.h: DefElem and ClusterStmt
 * ---------------------------------------------------------------- */

/// The `Node *arg` of a `DefElem`, restricted to the value-node tags the
/// `defGet*` accessors read (`nodes/value.h`). `None` is the C NULL `arg`
/// (option given without a value).
#[derive(Clone, Debug, PartialEq)]
pub enum DefElemArg {
    /// `T_Integer` — `intVal`.
    Integer(i64),
    /// `T_Float` — `floatVal` (kept as the original spelling).
    Float(String),
    /// `T_Boolean` — `boolVal`.
    Boolean(bool),
    /// `T_String` — `strVal`.
    String(String),
}

/// `DefElem` (`nodes/parsenodes.h`), trimmed to the fields the CLUSTER option
/// parse reads (`defname`/`arg`/`location`).
#[derive(Clone, Debug, PartialEq)]
pub struct DefElem {
    /// `char *defnamespace` — NULL if unqualified name.
    pub defnamespace: Option<String>,
    /// `char *defname`.
    pub defname: String,
    /// `Node *arg` — the option value, or `None` (C NULL).
    pub arg: Option<DefElemArg>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: i32,
}

/// `ClusterStmt` (`nodes/parsenodes.h`).
#[derive(Clone, Debug, PartialEq)]
pub struct ClusterStmt {
    /// `RangeVar *relation` — relation being clustered, or `None` if all.
    pub relation: Option<types_tuple::access::RangeVar>,
    /// `char *indexname` — original index defined, or `None`.
    pub indexname: Option<String>,
    /// `List *params` — list of `DefElem` nodes.
    pub params: Vec<DefElem>,
}

/* ----------------------------------------------------------------
 * parser/parse_node.h: ParseState (opaque to this consumer)
 * ---------------------------------------------------------------- */

/// `ParseState` (`parser/parse_node.h`). Unified (K1 phase 4) onto the single
/// canonical full struct in [`types_nodes::parsestmt`]. CLUSTER (and the other
/// DDL consumers re-exporting through here) only forward the pointer to
/// `parser_errposition(pstate, location)`; the parser (its owner) fills the
/// rest. Re-exported for type identity — the struct now carries the full
/// ~36-field set and an `'mcx` lifetime.
pub use types_nodes::parsestmt::ParseState;

/* ----------------------------------------------------------------
 * commands/vacuum.h: struct VacuumCutoffs
 * ---------------------------------------------------------------- */

/// `struct VacuumCutoffs` (`commands/vacuum.h`): the freeze/cutoff values
/// `vacuum_get_cutoffs` computes and `cluster`/`copy_table_data` consume.
/// Canonically defined in `types_vacuum::vacuum`; re-exported here so existing
/// `types_cluster::VacuumCutoffs` paths keep working.
pub use types_vacuum::vacuum::VacuumCutoffs;

/* ----------------------------------------------------------------
 * catalog/index.h: reindex_relation flag bits + ReindexParams
 * ---------------------------------------------------------------- */

/// `REINDEX_REL_PROCESS_TOAST`.
pub const REINDEX_REL_PROCESS_TOAST: i32 = 0x01;
/// `REINDEX_REL_SUPPRESS_INDEX_USE`.
pub const REINDEX_REL_SUPPRESS_INDEX_USE: i32 = 0x02;
/// `REINDEX_REL_CHECK_CONSTRAINTS`.
pub const REINDEX_REL_CHECK_CONSTRAINTS: i32 = 0x04;
/// `REINDEX_REL_FORCE_INDEXES_UNLOGGED`.
pub const REINDEX_REL_FORCE_INDEXES_UNLOGGED: i32 = 0x08;
/// `REINDEX_REL_FORCE_INDEXES_PERMANENT`.
pub const REINDEX_REL_FORCE_INDEXES_PERMANENT: i32 = 0x10;

/// `ReindexParams` (`catalog/index.h`), trimmed to the fields CLUSTER passes
/// (`{0}` — no concurrency, default tablespace).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReindexParams {
    /// `bits32 options` — `REINDEXOPT_*` (CLUSTER passes 0).
    pub options: i32,
    /// `Oid tablespaceOid` — `InvalidOid` for "keep".
    pub tablespace_oid: types_core::Oid,
}

/* ----------------------------------------------------------------
 * commands/progress.h: CLUSTER progress reporting
 * ---------------------------------------------------------------- */

/// `PROGRESS_CLUSTER_COMMAND` parameter index.
pub const PROGRESS_CLUSTER_COMMAND: i32 = 0;
/// `PROGRESS_CLUSTER_PHASE` parameter index.
pub const PROGRESS_CLUSTER_PHASE: i32 = 1;

/// `PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES`.
pub const PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES: i64 = 5;
/// `PROGRESS_CLUSTER_PHASE_REBUILD_INDEX`.
pub const PROGRESS_CLUSTER_PHASE_REBUILD_INDEX: i64 = 6;
/// `PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP`.
pub const PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP: i64 = 7;

/// `PROGRESS_CLUSTER_COMMAND_CLUSTER`.
pub const PROGRESS_CLUSTER_COMMAND_CLUSTER: i64 = 1;
/// `PROGRESS_CLUSTER_COMMAND_VACUUM_FULL`.
pub const PROGRESS_CLUSTER_COMMAND_VACUUM_FULL: i64 = 2;

/// `PROGRESS_COMMAND_CLUSTER` (`utils/backend_progress.h` `ProgressCommandType`:
/// INVALID=0, VACUUM=1, ANALYZE=2, CLUSTER=3, CREATE_INDEX=4, COPY=5, BASEBACKUP=6).
pub const PROGRESS_COMMAND_CLUSTER: i32 = 3;

/* ----------------------------------------------------------------
 * Cross-seam helper records owned by this consumer's vocabulary
 * ---------------------------------------------------------------- */

/// The writable `pg_index` row copy `SearchSysCacheCopy1(INDEXRELID)` yields
/// (the tuple's `GETSTRUCT` view), trimmed to the boolean flag columns the
/// in-place `pg_index` row mutators read/write. `mark_index_clustered`
/// (cluster.c) reads/writes `indisclustered`/`indisvalid`;
/// `index_set_state_flags` and `index_constraint_create`'s
/// `INDEX_CONSTR_CREATE_UPDATE_INDEX` leg (catalog/index.c) read/write the
/// remaining `indis*`/`indimmediate`/`indcheckxmin` flags. (The variable-length
/// `indkey`/`indcollation`/… columns are never mutated by these paths, so they
/// are not carried; the consumer re-reads the whole tuple by `t_self` and only
/// `heap_modify_tuple`s the changed columns.)
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PgIndexForm {
    /// `bool indisprimary` (`Anum_pg_index_indisprimary` = 7).
    pub indisprimary: bool,
    /// `bool indimmediate` (`Anum_pg_index_indimmediate` = 9).
    pub indimmediate: bool,
    /// `bool indisclustered` (`Anum_pg_index_indisclustered` = 10).
    pub indisclustered: bool,
    /// `bool indisvalid` (`Anum_pg_index_indisvalid` = 11).
    pub indisvalid: bool,
    /// `bool indcheckxmin` (`Anum_pg_index_indcheckxmin` = 12).
    pub indcheckxmin: bool,
    /// `bool indisready` (`Anum_pg_index_indisready` = 13).
    pub indisready: bool,
    /// `bool indislive` (`Anum_pg_index_indislive` = 14).
    pub indislive: bool,
    /// `bool indisreplident` (`Anum_pg_index_indisreplident` = 15).
    pub indisreplident: bool,
}

/// The out-params of `table_relation_copy_for_cluster` (`access/tableam.h`):
/// the (possibly AM-adjusted) freeze/cutoff and the tuple counters.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct CopyForClusterResult {
    /// `*pFreezeXid` after the AM ran (may differ from the input).
    pub new_frozen_xid: TransactionId,
    /// `*pCutoffMulti` after the AM ran.
    pub new_cutoff_multi: MultiXactId,
    /// `*num_tuples`.
    pub num_tuples: f64,
    /// `*tups_vacuumed`.
    pub tups_vacuumed: f64,
    /// `*tups_recently_dead`.
    pub tups_recently_dead: f64,
}

/// `Datum reloptions` token from `SysCacheGetAttr(Anum_pg_class_reloptions)`
/// (NULL when unset). The reloptions value is a `bytea` (varlena) the catalog
/// owner round-trips opaquely into `heap_create_with_catalog` /
/// `NewHeapCreateToastTable`; this consumer only forwards it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RelOptionsToken {
    /// `true` when the pg_class reloptions attribute was NULL.
    pub is_null: bool,
    /// the raw varlena bytes (empty when `is_null`).
    pub bytes: Vec<u8>,
}

/// `CatalogIndexState` (the on-disk C type is `struct ResultRelInfo *`).
///
/// The cut-down owned representation of a catalog's open indexes plus the heap
/// relation — exactly the three `ResultRelInfo` fields `CatalogIndexInsert`
/// reads:
///   * `ri_RelationDesc`     → [`Self::heap_relation`]
///   * `ri_NumIndices`       → `index_descs.len()`
///   * `ri_IndexRelationDescs` / `ri_IndexRelationInfo` → the parallel
///     [`Self::index_descs`] / [`Self::index_infos`] vectors.
///
/// `CatalogOpenIndexes` shares `execUtils.c`'s `ResultRelInfo` only as an
/// allocation convenience; it deliberately builds **no** `EState` (so partial /
/// expressional / exclusion indexes on catalogs are unsupported). The port
/// therefore models the cut-down state directly rather than routing through
/// `execIndexing.c`.
///
/// Built by `CatalogOpenIndexes` (≈ `ExecOpenIndices` minus the `EState`),
/// torn down by `CatalogCloseIndexes` (both in the catalog-indexing owner).
///
/// This is the real owned value the catalog-mutation lifecycle threads: the
/// `catalog_open_indexes` seam returns it, the `*_with_info_*` seams borrow it
/// `&mut`, and `catalog_close_indexes` consumes it. It lives in this leaf type
/// crate (not the owner) so the seam declarations and the cross-crate consumers
/// (cluster, large-object) can name the value directly — no opaque handle.
pub struct CatalogIndexState<'mcx> {
    /// `indstate->ri_RelationDesc` — the open catalog (heap) relation. A
    /// borrow-free alias of the caller's open relation (no release authority),
    /// as in `CatalogOpenIndexes` where `ri_RelationDesc = heapRel` merely
    /// points at the caller's open relation.
    pub heap_relation: types_rel::Relation<'mcx>,
    /// `indstate->ri_IndexRelationDescs` — the open index relations, in the
    /// relcache `RelationGetIndexList` order `ExecOpenIndices` uses. Each
    /// carries its own `RowExclusiveLock` release authority taken by
    /// `index_open`, released by `CatalogCloseIndexes`.
    pub index_descs: Vec<types_rel::Relation<'mcx>>,
    /// `indstate->ri_IndexRelationInfo` — the per-index `IndexInfo`, parallel
    /// to [`Self::index_descs`].
    pub index_infos: Vec<types_nodes::execnodes::IndexInfo<'mcx>>,
}
