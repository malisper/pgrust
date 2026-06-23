//! Vocabulary for `backend/postmaster/autovacuum.c`: the process-local
//! in-memory data types (`AvlDbase`/`AvRelation`/`AutovacTable`) and the
//! by-value carrier structs the catalog/pgstat-reader seams return
//! (`AvwDbase`/`DbStatEntry`/`PgClassScanRow`/`TabStatEntry`/`RecheckClassRow`).

extern crate alloc;
use alloc::string::String;

use ::types_core::{MultiXactId, Oid, TimestampTz, TransactionId};
use ::types_reloptions::{AutoVacOpts, StdRdOptions};
use ::types_vacuum::vacuum::VacuumParams;

/// `struct avl_dbase` (`autovacuum.c`) — a database in the launcher's
/// process-local `DatabaseList`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AvlDbase {
    /// `adl_datid` — hash key.
    pub adl_datid: Oid,
    /// `adl_next_worker`.
    pub adl_next_worker: TimestampTz,
    /// `adl_score`.
    pub adl_score: i32,
}

/// `struct av_relation` (`autovacuum.c`) — the toast→main relid mapping built
/// in `do_autovacuum`'s first pass.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AvRelation {
    /// `ar_toastrelid` — hash key.
    pub ar_toastrelid: Oid,
    /// `ar_relid`.
    pub ar_relid: Oid,
    /// `ar_hasrelopts`.
    pub ar_hasrelopts: bool,
    /// `ar_reloptions` — copy of the main table's `AutoVacOpts` (valid only
    /// when `ar_hasrelopts`).
    pub ar_reloptions: AutoVacOpts,
}

/// `struct autovac_table` (`autovacuum.c`) — a table that has been rechecked
/// and confirmed to need vacuum/analyze.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AutovacTable {
    /// `at_relid`.
    pub at_relid: Oid,
    /// `at_params`.
    pub at_params: VacuumParams,
    /// `at_storage_param_vac_cost_delay`.
    pub at_storage_param_vac_cost_delay: f64,
    /// `at_storage_param_vac_cost_limit`.
    pub at_storage_param_vac_cost_limit: i32,
    /// `at_dobalance`.
    pub at_dobalance: bool,
    /// `at_sharedrel`.
    pub at_sharedrel: bool,
    /// `at_relname` — set later, in `do_autovacuum`, before vacuum.
    pub at_relname: Option<String>,
    /// `at_nspname`.
    pub at_nspname: Option<String>,
    /// `at_datname`.
    pub at_datname: Option<String>,
}

/// One `pg_database` row materialized by `get_database_list` — the by-value
/// carrier in place of the C `avw_dbase` list element.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AvwDbase {
    /// `pgdatabase->oid`.
    pub adw_datid: Oid,
    /// `pstrdup(NameStr(pgdatabase->datname))`.
    pub adw_name: String,
    /// `pgdatabase->datfrozenxid`.
    pub adw_frozenxid: TransactionId,
    /// `pgdatabase->datminmxid`.
    pub adw_minmulti: MultiXactId,
}

/// The `PgStat_StatDBEntry` fields the launcher scheduler reads. `None` models
/// the C `NULL` (no pgstat entry).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DbStatEntry {
    /// `entry->last_autovac_time`.
    pub last_autovac_time: TimestampTz,
}

/// One `pg_class` row materialized by `do_autovacuum`'s two seqscan passes.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PgClassScanRow {
    /// `classForm->oid`.
    pub oid: Oid,
    /// `classForm->relkind`.
    pub relkind: u8,
    /// `classForm->relpersistence`.
    pub relpersistence: u8,
    /// `classForm->relisshared`.
    pub relisshared: bool,
    /// `classForm->relnamespace`.
    pub relnamespace: Oid,
    /// `classForm->reltoastrelid`.
    pub reltoastrelid: Oid,
    /// `classForm->relfrozenxid`.
    pub relfrozenxid: TransactionId,
    /// `classForm->relminmxid`.
    pub relminmxid: MultiXactId,
    /// `classForm->reltuples`.
    pub reltuples: f32,
    /// `classForm->relpages`.
    pub relpages: i32,
    /// `classForm->relallfrozen`.
    pub relallfrozen: i32,
    /// `NameStr(classForm->relname)` (for DEBUG logging).
    pub relname: String,
    /// The parsed reloptions from `extractRelOptions(tuple, pg_class_desc, NULL)`,
    /// cast to `StdRdOptions`, or `None` when the row has no reloptions.  The
    /// `.autovacuum` projection (`extract_autovac_opts`) is done in-crate.
    pub relopts: Option<StdRdOptions>,
}

/// The `PgStat_StatTabEntry` fields `relation_needs_vacanalyze` reads. `None`
/// models the C `NULL`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TabStatEntry {
    /// `tabentry->dead_tuples`.
    pub dead_tuples: f32,
    /// `tabentry->ins_since_vacuum`.
    pub ins_since_vacuum: f32,
    /// `tabentry->mod_since_analyze`.
    pub mod_since_analyze: f32,
}

/// A `pg_class` row re-fetched (`SearchSysCacheCopy1(RELOID, ...)`) in
/// `do_autovacuum`'s orphan-temp recheck loop.  Carries exactly the fields the
/// in-crate recheck predicates and LOG message consume; `None` from the seam
/// models the C "tuple not found / no longer the same relation" case.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrphanClassRow {
    /// `classForm->relkind`.
    pub relkind: u8,
    /// `classForm->relpersistence`.
    pub relpersistence: u8,
    /// `classForm->relnamespace`.
    pub relnamespace: Oid,
    /// `NameStr(classForm->relname)`.
    pub relname: String,
}

/// A `pg_class` row re-fetched from the syscache in `table_recheck_autovac`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RecheckClassRow {
    /// `classForm->relkind`.
    pub relkind: u8,
    /// `classForm->relisshared`.
    pub relisshared: bool,
    /// `classForm->relfrozenxid`.
    pub relfrozenxid: TransactionId,
    /// `classForm->relminmxid`.
    pub relminmxid: MultiXactId,
    /// `classForm->reltuples`.
    pub reltuples: f32,
    /// `classForm->relpages`.
    pub relpages: i32,
    /// `classForm->relallfrozen`.
    pub relallfrozen: i32,
    /// `NameStr(classForm->relname)`.
    pub relname: String,
    /// The parsed reloptions from `extractRelOptions(classTup, pg_class_desc,
    /// NULL)`, cast to `StdRdOptions`, or `None`.  The `.autovacuum` projection
    /// (`extract_autovac_opts`) is done in-crate.
    pub relopts: Option<StdRdOptions>,
}
