//! Seam declarations for the `backend-commands-vacuum` unit
//! (`commands/vacuum.c`): the cross-cutting VACUUM helpers other AMs call.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! Below the original inward decls live the OUTWARD seams `commands/vacuum.c`
//! itself reaches into not-yet-ported owners for (xact / snapshot / GUC / ACL /
//! relcache field reads / catalog seqscans + inplace updates / SLRU truncation
//! / lock manager / autovacuum cost globals / per-index AM ops / cost-delay
//! internals). They follow the same panic-until-owner pattern. Relations are
//! `Oid` tokens; small DTO row/result structs that cross the seam are defined
//! here.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::{bits32, MultiXactId, Oid, TransactionId};
use types_error::PgResult;
use types_rel::FormData_pg_class;
use types_tuple::heaptuple::ItemPointerData;
use types_vacuum::vacuum::VacuumParams;
use types_vacuum::vacuumlazy::{StrategyHandle, TidStore, UpdateRelStatsArgs};
use types_vacuum::vacuumparallel::{IndexBulkDeleteResult, IndexVacuumInfo, VacDeadItemsInfo};
use mcx::Mcx;

// =======================================================================
// DTO structs that cross the outward seams (small value snapshots; the
// owning catalog/relcache units fill them from the real structures).
// =======================================================================

/// A row of `pg_class` from `get_all_vacuum_rels`'s catalog seqscan: the OID,
/// relkind, and the full `Form_pg_class` (for `vacuum_is_permitted_for_relation`).
pub struct PgClassScanRow<'mcx> {
    pub oid: Oid,
    pub relkind: u8,
    pub class_form: FormData_pg_class<'mcx>,
}

/// A row of `pg_class` from `vac_update_datfrozenxid`'s seqscan (just the xid
/// horizons + relkind it needs).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PgClassFrozenRow {
    pub relkind: u8,
    pub relfrozenxid: TransactionId,
    pub relminmxid: MultiXactId,
}

/// A row of `pg_database` from `vac_truncate_clog`'s seqscan.
#[derive(Clone, Debug)]
pub struct PgDatabaseFrozenRow {
    pub oid: Oid,
    pub datname: String,
    pub datfrozenxid: TransactionId,
    pub datminmxid: MultiXactId,
    pub is_invalid: bool,
}

/// Result of `vac_update_relstats_apply` (the catalog inplace-update worker):
/// the `*_updated` out-flags + the corruption-overwrite flags and old values
/// the caller uses to emit the data-corruption WARNINGs.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RelStatsApplyResult {
    pub frozenxid_updated: bool,
    pub minmulti_updated: bool,
    pub futurexid: bool,
    pub futuremxid: bool,
    pub old_frozenxid: TransactionId,
    pub old_minmulti: MultiXactId,
}

/// Result of `vac_update_datfrozenxid_apply` (pg_database inplace-update
/// worker): the effective (possibly advanced) values + whether anything was
/// dirtied.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DatFrozenApplyResult {
    pub eff_frozen_xid: TransactionId,
    pub eff_min_multi: MultiXactId,
    pub dirty: bool,
}

/// Result of `index_open(indexoid, lockmode)` for `vac_open_indexes`: the index
/// Oid plus its `indisready` flag.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct OpenedIndex {
    pub index: Oid,
    pub indisready: bool,
}

seam_core::seam!(
    /// The btbulkdelete `IndexBulkDeleteCallback(htup, callback_state)`: does
    /// this heap TID belong to a tuple being deleted by VACUUM? The callback
    /// and its state live in the VACUUM driver; `callback_state_handle`
    /// identifies the state. Infallible (a pure membership test).
    pub fn vacuum_tid_is_dead(
        tid: types_tuple::heaptuple::ItemPointerData,
        callback_state_handle: u64,
    ) -> bool
);

seam_core::seam!(
    /// `vacuum_delay_point(is_analyze = false)` (vacuum.c): cost-based VACUUM
    /// delay / interrupt check, called while holding no buffer lock. `Err`
    /// carries a pending `ProcessInterrupts` `ereport(ERROR)` (query cancel).
    pub fn vacuum_delay_point() -> PgResult<()>
);

seam_core::seam!(
    /// `memset(&params, 0, sizeof(VacuumParams)); vacuum_get_cutoffs(OldHeap,
    /// &params, &cutoffs)` (vacuum.c): freeze/cutoff computation for CLUSTER.
    pub fn vacuum_get_cutoffs(
        old_heap: &types_rel::Relation<'_>,
    ) -> PgResult<types_cluster::VacuumCutoffs>
);

// =======================================================================
// OUTWARD seams — leaves vacuum.c reaches in not-yet-ported owners.
// =======================================================================

// ---- commands/defrem.h + utils/guc.h option parsing (ExecVacuum) ------
seam_core::seam!(pub fn def_get_int32(defname: String, arg: Option<backend_commands_define_seams::DefElemArg>) -> PgResult<i32>);
seam_core::seam!(
    /// `defGetString(def)` rendered to a plain owned `String` (the owner
    /// allocates internally; vacuum.c only compares/`parse_int`s the text).
    pub fn def_get_string_text(defname: String, arg: Option<backend_commands_define_seams::DefElemArg>) -> PgResult<String>
);
seam_core::seam!(
    /// `parse_int(value, &result, GUC_UNIT_KB, &hintmsg)` — returns
    /// `(ok, result, hintmsg)`.
    pub fn parse_int_kb(value: String) -> PgResult<(bool, i32, Option<String>)>
);

// ---- access/xact.h -----------------------------------------------------
seam_core::seam!(pub fn is_in_transaction_block(is_top_level: bool) -> PgResult<bool>);

// ---- utils/snapmgr.h ---------------------------------------------------
// `active_snapshot_set` / `pop_active_snapshot` / `push_active_snapshot_transaction`
// are owned by snapmgr.c and called through `backend-utils-time-snapmgr-seams`
// (the vacuum caller adopts the owner's bare-`bool` `ActiveSnapshotSet` contract).

// ---- postmaster/autovacuum.h + cost globals ---------------------------
seam_core::seam!(pub fn am_autovacuum_worker_process() -> PgResult<bool>);
seam_core::seam!(pub fn vacuum_update_costs() -> PgResult<()>);
seam_core::seam!(pub fn set_vacuum_cost_balance_local(v: i32) -> PgResult<()>);
seam_core::seam!(pub fn clear_parallel_cost_pointers() -> PgResult<()>);
seam_core::seam!(pub fn vacuum_max_eager_freeze_failure_rate() -> PgResult<f64>);
seam_core::seam!(pub fn vacuum_buffer_usage_limit() -> PgResult<i32>);
seam_core::seam!(pub fn get_access_strategy_with_size(ring_size: i32) -> PgResult<StrategyHandle>);

// ---- utils/acl.h -------------------------------------------------------
seam_core::seam!(pub fn database_ownercheck() -> PgResult<bool>);
seam_core::seam!(pub fn pg_class_aclcheck_maintain(relid: Oid) -> PgResult<bool>);

// ---- access/table.h + storage/lmgr.h + catalog/namespace.h ------------
seam_core::seam!(pub fn try_relation_open(relid: Oid, lmode: i32) -> PgResult<Option<Oid>>);
seam_core::seam!(pub fn conditional_lock_relation_oid(relid: Oid, lmode: i32) -> PgResult<bool>);
seam_core::seam!(pub fn range_var_get_relid_extended(
    relation: types_nodes::rawnodes::RangeVar<'_>,
    lockmode: i32,
    rvr_opts: i32,
) -> PgResult<Oid>);
seam_core::seam!(pub fn search_syscache_class<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<Option<FormData_pg_class<'mcx>>>);
// `find_all_inheritors` -> backend-catalog-pg-inherits-seams (owner-installed).
seam_core::seam!(pub fn unlock_relation_oid(relid: Oid, lockmode: i32) -> PgResult<()>);
// `relation_close` -> backend-access-table-table-seams (owner-installed).
seam_core::seam!(pub fn lock_relation_id_for_session(rel: Oid, lockmode: i32) -> PgResult<()>);
seam_core::seam!(pub fn unlock_relation_id_for_session(rel: Oid, lockmode: i32) -> PgResult<()>);

// ---- get_all_vacuum_rels seqscan --------------------------------------
seam_core::seam!(pub fn scan_all_pg_class<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<PgClassScanRow<'mcx>>>);

// ---- procarray.h / transam.h / multixact.h ----------------------------
// `get_oldest_non_removable_transaction_id` -> backend-storage-ipc-procarray-seams.
// `read_next_transaction_id` -> backend-access-transam-varsup-seams.
// `get_oldest_multixact_id` (get_oldest_multi_xact_id) / `read_next_multixact_id` /
// `multixact_member_freeze_threshold` -> backend-access-transam-multixact-seams.

// ---- GUCs (utils/guc.h, guc_hooks) ------------------------------------
seam_core::seam!(pub fn autovacuum_freeze_max_age() -> PgResult<i32>);
seam_core::seam!(pub fn autovacuum_multixact_freeze_max_age() -> PgResult<i32>);
seam_core::seam!(pub fn vacuum_freeze_min_age() -> PgResult<i32>);
seam_core::seam!(pub fn vacuum_multixact_freeze_min_age() -> PgResult<i32>);
seam_core::seam!(pub fn vacuum_freeze_table_age() -> PgResult<i32>);
seam_core::seam!(pub fn vacuum_multixact_freeze_table_age() -> PgResult<i32>);
seam_core::seam!(pub fn vacuum_failsafe_age() -> PgResult<i32>);
seam_core::seam!(pub fn vacuum_multixact_failsafe_age() -> PgResult<i32>);
seam_core::seam!(pub fn vacuum_truncate() -> PgResult<bool>);

// ---- relcache field reads (Oid -> field value) ------------------------
// `rel_frozenxid_minmxid` / `rel_pages_tuples` / `rel_lock_relid` /
// `rel_std_rd_options` / `rel_reltoastrelid` / `rel_relowner` (the by-OID
// `rd_rel` / `rd_options` / `rd_lockInfo` reads) -> the owner installs them, so
// they live in `backend-utils-cache-relcache-seams` (with `StdRdOptionsView`).
seam_core::seam!(pub fn relation_get_relation_name(rel: Oid) -> PgResult<String>);
seam_core::seam!(pub fn rel_relkind(rel: Oid) -> PgResult<u8>);
seam_core::seam!(pub fn relation_is_other_temp(rel: Oid) -> PgResult<bool>);

// ---- catalog inplace-update workers -----------------------------------
seam_core::seam!(pub fn vac_update_relstats_apply(
    relation: Oid,
    args: UpdateRelStatsArgs,
) -> PgResult<RelStatsApplyResult>);
seam_core::seam!(pub fn vac_update_datfrozenxid_apply(
    new_frozen_xid: TransactionId,
    new_min_multi: MultiXactId,
    last_sane_frozen_xid: TransactionId,
    last_sane_min_multi: MultiXactId,
) -> PgResult<DatFrozenApplyResult>);
seam_core::seam!(pub fn scan_pg_class_frozenids() -> PgResult<Vec<PgClassFrozenRow>>);
seam_core::seam!(pub fn scan_pg_database_frozenids() -> PgResult<Vec<PgDatabaseFrozenRow>>);
seam_core::seam!(pub fn lock_database_frozen_ids() -> PgResult<()>);
// `force_transaction_id_limit_update` -> backend-access-transam-varsup-seams.

// ---- vac_truncate_clog leaves -----------------------------------------
seam_core::seam!(pub fn lock_wraplimits_vacuum() -> PgResult<()>);
seam_core::seam!(pub fn unlock_wraplimits_vacuum() -> PgResult<()>);
// `my_database_id` -> backend-utils-init-small-seams.
seam_core::seam!(pub fn elog_debug2_skip_invalid_db(datname: String) -> PgResult<()>);
seam_core::seam!(pub fn async_notify_freeze_xids(frozen_xid: TransactionId) -> PgResult<()>);
// `advance_oldest_commit_ts_xid` / `truncate_commit_ts` -> backend-access-transam-commit-ts-seams.
// `truncate_clog` -> backend-access-transam-clog-seams.
// `truncate_multixact` / `set_multixact_id_limit` (set_multi_xact_id_limit)
//   -> backend-access-transam-multixact-seams.
seam_core::seam!(pub fn set_transaction_id_limit(frozen_xid: TransactionId, oldest_xid_datoid: Oid) -> PgResult<()>);

// ---- vacuum_rel leaves ------------------------------------------------
seam_core::seam!(pub fn set_proc_in_vacuum_flags(is_wraparound: bool) -> PgResult<()>);
seam_core::seam!(pub fn check_for_interrupts() -> PgResult<()>);
seam_core::seam!(pub fn injection_point(name: String) -> PgResult<()>);
seam_core::seam!(pub fn cluster_rel_for_vacuum_full(rel: Oid, verbose: bool) -> PgResult<()>);
seam_core::seam!(pub fn table_relation_vacuum(rel: Oid, params: VacuumParams, bstrategy: StrategyHandle) -> PgResult<()>);
seam_core::seam!(pub fn at_eoxact_guc(is_commit: bool, nestlevel: i32) -> PgResult<()>);
// `get_user_id_and_sec_context` -> backend-utils-init-miscinit-seams.
seam_core::seam!(pub fn set_user_id_and_sec_context(userid: Oid, sec_context: i32) -> PgResult<()>);
seam_core::seam!(pub fn new_guc_nest_level() -> PgResult<i32>);
// `restrict_search_path` -> backend-utils-misc-guc-seams.

// ---- vac_open_indexes / vac_*_one_index leaves ------------------------
seam_core::seam!(pub fn relation_get_index_list(relation: Oid) -> PgResult<Vec<Oid>>);
seam_core::seam!(pub fn index_open(indexoid: Oid, lockmode: i32) -> PgResult<OpenedIndex>);
seam_core::seam!(pub fn index_close(index: Oid, lockmode: i32) -> PgResult<()>);
seam_core::seam!(pub fn index_bulk_delete(
    ivinfo: IndexVacuumInfo,
    istat: Option<IndexBulkDeleteResult>,
    dead_items: TidStore,
) -> PgResult<IndexBulkDeleteResult>);
seam_core::seam!(pub fn report_index_scanned(ivinfo: IndexVacuumInfo, num_items: i64) -> PgResult<()>);
seam_core::seam!(pub fn dead_items_info_num_items(info: VacDeadItemsInfo) -> PgResult<i64>);
seam_core::seam!(pub fn index_vacuum_cleanup(
    ivinfo: IndexVacuumInfo,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>>);
seam_core::seam!(pub fn report_index_cleanup(ivinfo: IndexVacuumInfo, istat: Option<IndexBulkDeleteResult>) -> PgResult<()>);
seam_core::seam!(pub fn tid_store_is_member(dead_items: TidStore, tid: ItemPointerData) -> PgResult<bool>);

// ---- vacuum_delay_point internals -------------------------------------
seam_core::seam!(pub fn interrupt_pending() -> PgResult<bool>);
seam_core::seam!(pub fn vacuum_cost_active() -> PgResult<bool>);
seam_core::seam!(pub fn config_reload_pending() -> PgResult<bool>);
seam_core::seam!(pub fn set_config_reload_pending(v: bool) -> PgResult<()>);
seam_core::seam!(pub fn process_config_file_sighup() -> PgResult<()>);
seam_core::seam!(pub fn vacuum_cost_delay() -> PgResult<f64>);
seam_core::seam!(pub fn vacuum_cost_limit() -> PgResult<i32>);
seam_core::seam!(pub fn vacuum_shared_cost_balance_is_set() -> PgResult<bool>);
seam_core::seam!(pub fn vacuum_cost_balance() -> PgResult<i32>);
seam_core::seam!(pub fn track_cost_delay_timing() -> PgResult<bool>);
seam_core::seam!(pub fn vacuum_sleep(usec: i64, track: bool) -> PgResult<i64>);
seam_core::seam!(pub fn is_parallel_worker() -> PgResult<bool>);
seam_core::seam!(pub fn add_parallel_vacuum_worker_delay_ns(delay_ns: i64) -> PgResult<()>);
seam_core::seam!(pub fn time_since_last_delay_report_ns() -> PgResult<i64>);
seam_core::seam!(pub fn parallel_vacuum_worker_delay_ns() -> PgResult<i64>);
seam_core::seam!(pub fn progress_parallel_incr_delay_time(delay_ns: i64) -> PgResult<()>);
seam_core::seam!(pub fn reset_last_delay_report_time() -> PgResult<()>);
seam_core::seam!(pub fn set_parallel_vacuum_worker_delay_ns(v: i64) -> PgResult<()>);
seam_core::seam!(pub fn progress_incr_analyze_delay_time(delay_ns: i64) -> PgResult<()>);
seam_core::seam!(pub fn progress_incr_vacuum_delay_time(delay_ns: i64) -> PgResult<()>);
seam_core::seam!(pub fn postmaster_died() -> PgResult<bool>);
seam_core::seam!(pub fn exit_process(code: i32) -> !);
seam_core::seam!(pub fn autovacuum_update_cost_limit() -> PgResult<()>);

// ---- compute_parallel_delay internals ---------------------------------
seam_core::seam!(pub fn read_vacuum_active_nworkers() -> PgResult<u32>);
seam_core::seam!(pub fn shared_cost_balance_add_fetch(v: i32) -> PgResult<u32>);
seam_core::seam!(pub fn shared_cost_balance_sub_fetch(v: i32) -> PgResult<u32>);
seam_core::seam!(pub fn add_vacuum_cost_balance_local(v: i32) -> PgResult<i32>);

// ---- vacuum.c cost-state globals (owned here as thread_local; installed by
//      the owner). Read & write the VacuumFailsafeActive / VacuumCostActive /
//      VacuumCostBalance globals. ----------------------------------------
seam_core::seam!(pub fn vacuum_failsafe_active() -> PgResult<bool>);
seam_core::seam!(pub fn set_vacuum_failsafe_active(v: bool) -> PgResult<()>);
seam_core::seam!(pub fn set_vacuum_cost_active(v: bool) -> PgResult<()>);
seam_core::seam!(pub fn set_vacuum_cost_balance(v: i32) -> PgResult<()>);
// `bits32` is referenced by callers wiring options; keep the import live.
const _: bits32 = 0;

// =======================================================================
// vacuumparallel.c outward seams.
//
// The owners (vacuum.c index-AM bridges, table.c, relcache, lsyscache,
// optimizer/paths GUCs, pgstat, instrument, tidstore, freelist, proc,
// tcopprot, error_context_stack) are not yet reconciled to this handle
// model, so these default to the seam_core loud panic until installed.
// =======================================================================

seam_core::seam!(
    /// `vac_bulkdel_one_index(&ivinfo, istat, dead_items, &dead_items_info)`
    /// (vacuum.c) — one index bulk-deletion pass, returns the updated stats.
    pub fn vac_bulkdel_one_index(
        ivinfo: IndexVacuumInfo,
        istat: Option<IndexBulkDeleteResult>,
        dead_items: TidStore,
        dead_items_info: VacDeadItemsInfo,
    ) -> PgResult<IndexBulkDeleteResult>
);
seam_core::seam!(
    /// `vac_cleanup_one_index(&ivinfo, istat)` (vacuum.c) — one index cleanup
    /// pass (`NULL` => `None` when amvacuumcleanup returns no stats).
    pub fn vac_cleanup_one_index(
        ivinfo: IndexVacuumInfo,
        istat: Option<IndexBulkDeleteResult>,
    ) -> PgResult<Option<IndexBulkDeleteResult>>
);
seam_core::seam!(
    /// `vac_open_indexes(rel, lockmode, &nindexes, &indrels)` — open all of the
    /// relation's indexes (the worker path needs only the index Oids).
    pub fn vac_open_indexes_lock(rel: Oid, lockmode: i32) -> PgResult<Vec<Oid>>
);
seam_core::seam!(
    /// `vac_close_indexes(nindexes, indrels, lockmode)`.
    pub fn vac_close_indexes_lock(indrels: Vec<Oid>, lockmode: i32) -> PgResult<()>
);
seam_core::seam!(
    /// `table_open(relid, lockmode)`.
    pub fn table_open_lock(relid: Oid, lockmode: i32) -> PgResult<Oid>
);
seam_core::seam!(
    /// `table_close(rel, lockmode)`.
    pub fn table_close_lock(rel: Oid, lockmode: i32) -> PgResult<()>
);
seam_core::seam!(
    /// `indrel->rd_indam->amparallelvacuumoptions`.
    pub fn am_parallel_vacuum_options(indrel: Oid) -> PgResult<u8>
);
seam_core::seam!(
    /// `indrel->rd_indam->amusemaintenanceworkmem`.
    pub fn am_use_maintenance_work_mem(indrel: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `RelationGetNumberOfBlocks(indrel)`.
    pub fn relation_get_number_of_blocks_pv(indrel: Oid) -> PgResult<u32>
);
seam_core::seam!(
    /// `get_namespace_name(RelationGetNamespace(rel))`.
    pub fn relation_get_namespace_name_pv(rel: Oid) -> PgResult<String>
);
seam_core::seam!(
    /// `min_parallel_index_scan_size` GUC (optimizer/paths.h).
    pub fn min_parallel_index_scan_size() -> PgResult<i32>
);
seam_core::seam!(
    /// `max_parallel_maintenance_workers` GUC.
    pub fn max_parallel_maintenance_workers() -> PgResult<i32>
);
seam_core::seam!(
    /// `IsUnderPostmaster`.
    pub fn is_under_postmaster_pv() -> PgResult<bool>
);
seam_core::seam!(
    /// Read the `maintenance_work_mem` GUC.
    pub fn pv_maintenance_work_mem() -> PgResult<i32>
);
seam_core::seam!(
    /// Set the `maintenance_work_mem` GUC (worker path — guc.c would complain so
    /// the owner pokes the global directly).
    pub fn set_pv_maintenance_work_mem(v: i32) -> PgResult<()>
);
seam_core::seam!(
    /// `pgstat_get_my_query_id()`.
    pub fn pgstat_get_my_query_id() -> PgResult<i64>
);
seam_core::seam!(
    /// `pgstat_report_activity(STATE_RUNNING, query)`.
    pub fn pgstat_report_activity_running_pv(query: String) -> PgResult<()>
);
seam_core::seam!(
    /// `pgstat_report_query_id(queryid, force)`.
    pub fn pgstat_report_query_id_pv(queryid: i64, force: bool) -> PgResult<()>
);
seam_core::seam!(
    /// `pgstat_progress_parallel_incr_param(index, incr)`.
    pub fn pgstat_progress_parallel_incr_param(index: i32, incr: i64) -> PgResult<()>
);
seam_core::seam!(
    /// `FreeAccessStrategy(strategy)`.
    pub fn free_access_strategy_pv(strategy: StrategyHandle) -> PgResult<()>
);
seam_core::seam!(
    /// `GetAccessStrategyWithSize(BAS_VACUUM, ring_size)` — worker's own ring.
    pub fn get_access_strategy_with_size_basvac(ring_size: i32) -> PgResult<StrategyHandle>
);
seam_core::seam!(
    /// `GetAccessStrategyBufferCount(bstrategy)` — number of buffers in the
    /// strategy ring (`freelist.c`); `0` for the NULL strategy.
    pub fn get_access_strategy_buffer_count(strategy: StrategyHandle) -> PgResult<i32>
);
seam_core::seam!(
    /// `InstrStartParallelQuery()`.
    pub fn instr_start_parallel_query_pv() -> PgResult<()>
);
seam_core::seam!(
    /// `InstrAccumParallelQuery(&buffer_usage[worker], &wal_usage[worker])` —
    /// the leader-side accumulation of one worker's usage.
    pub fn instr_accum_parallel_query_pv(worker: i32) -> PgResult<()>
);
seam_core::seam!(
    /// `InstrEndParallelQuery(&buffer_usage[worker], &wal_usage[worker])` —
    /// the worker storing its own usage into its DSM slot.
    pub fn instr_end_parallel_query_pv(worker: i32) -> PgResult<()>
);
seam_core::seam!(
    /// Push `parallel_vacuum_error_callback` onto `error_context_stack`.
    pub fn push_parallel_vacuum_error_context() -> PgResult<()>
);
seam_core::seam!(
    /// Pop the parallel-vacuum error context off `error_context_stack`.
    pub fn pop_parallel_vacuum_error_context() -> PgResult<()>
);
seam_core::seam!(
    /// `TidStoreCreateShared(max_bytes, tranche_id)`.
    pub fn tid_store_create_shared_pv(max_bytes: usize, tranche_id: i32) -> PgResult<TidStore>
);
seam_core::seam!(
    /// `TidStoreGetHandle(ts)`.
    pub fn tid_store_get_handle_pv(ts: TidStore) -> PgResult<types_dsa::DsaPointer>
);
seam_core::seam!(
    /// `dsa_get_handle(TidStoreGetDSA(ts))`.
    pub fn tid_store_get_dsa_handle_pv(ts: TidStore) -> PgResult<types_dsa::DsaHandle>
);
seam_core::seam!(
    /// `TidStoreAttach(dsa_handle, handle)`.
    pub fn tid_store_attach_pv(
        dsa: types_dsa::DsaHandle,
        ptr: types_dsa::DsaPointer,
    ) -> PgResult<TidStore>
);
seam_core::seam!(
    /// `TidStoreDestroy(ts)`.
    pub fn tid_store_destroy_pv(ts: TidStore) -> PgResult<()>
);
seam_core::seam!(
    /// `TidStoreDetach(ts)`.
    pub fn tid_store_detach_pv(ts: TidStore) -> PgResult<()>
);
seam_core::seam!(
    /// Read `debug_query_string` (`None` when `NULL`).
    pub fn debug_query_string_pv() -> PgResult<Option<String>>
);
seam_core::seam!(
    /// Set `debug_query_string`.
    pub fn set_debug_query_string_pv(s: Option<String>) -> PgResult<()>
);
seam_core::seam!(
    /// `Assert(MyProc->statusFlags == PROC_IN_VACUUM)`.
    pub fn my_proc_in_vacuum_only() -> PgResult<bool>
);
seam_core::seam!(
    /// Enable/disable `VacuumSharedCostBalance` (set to `&shared->cost_balance`
    /// initialized to `initial`, or `NULL`).
    pub fn set_vacuum_shared_cost_balance_enable(enable: bool, initial: u32) -> PgResult<()>
);
seam_core::seam!(
    /// Enable/disable `VacuumActiveNWorkers`.
    pub fn set_vacuum_active_nworkers_enable(enable: bool, initial: u32) -> PgResult<()>
);
seam_core::seam!(
    /// `VacuumActiveNWorkers != NULL`.
    pub fn vacuum_active_nworkers_is_set() -> PgResult<bool>
);
seam_core::seam!(
    /// `pg_atomic_add_fetch_u32(VacuumActiveNWorkers, v)`.
    pub fn vacuum_active_nworkers_add(v: u32) -> PgResult<()>
);
seam_core::seam!(
    /// `pg_atomic_sub_fetch_u32(VacuumActiveNWorkers, v)`.
    pub fn vacuum_active_nworkers_sub(v: u32) -> PgResult<()>
);
seam_core::seam!(
    /// `pg_atomic_read_u32(VacuumSharedCostBalance)`.
    pub fn vacuum_shared_cost_balance_read() -> PgResult<u32>
);
