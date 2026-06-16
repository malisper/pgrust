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

use types_core::{bits32, BlockNumber, MultiXactId, Oid, TransactionId};
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

/// Projection of `rel->rd_options` viewed as `StdRdOptions` for VACUUM
/// (`access/reloptions.h`). `has_options` is false when `rd_options == NULL`.
/// `vacuum_truncate` carries the `(vacuum_truncate_set, vacuum_truncate)` pair.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct StdRdOptionsView {
    pub has_options: bool,
    pub vacuum_index_cleanup: u8,
    pub max_eager_freeze_failure_rate: f64,
    pub vacuum_truncate: Option<(bool, bool)>,
}

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
seam_core::seam!(pub fn prevent_in_transaction_block(is_top_level: bool, stmt_type: String) -> PgResult<()>);
seam_core::seam!(pub fn is_in_transaction_block(is_top_level: bool) -> PgResult<bool>);
seam_core::seam!(pub fn start_transaction_command() -> PgResult<()>);
seam_core::seam!(pub fn commit_transaction_command() -> PgResult<()>);
seam_core::seam!(pub fn command_counter_increment() -> PgResult<()>);

// ---- utils/snapmgr.h ---------------------------------------------------
seam_core::seam!(pub fn active_snapshot_set() -> PgResult<bool>);
seam_core::seam!(pub fn pop_active_snapshot() -> PgResult<()>);
seam_core::seam!(pub fn push_active_snapshot_transaction() -> PgResult<()>);

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
seam_core::seam!(pub fn find_all_inheritors(relid: Oid, lockmode: i32) -> PgResult<Vec<Oid>>);
seam_core::seam!(pub fn unlock_relation_oid(relid: Oid, lockmode: i32) -> PgResult<()>);
seam_core::seam!(pub fn relation_close(rel: Oid, lockmode: i32) -> PgResult<()>);
seam_core::seam!(pub fn lock_relation_id_for_session(rel: Oid, lockmode: i32) -> PgResult<()>);
seam_core::seam!(pub fn unlock_relation_id_for_session(rel: Oid, lockmode: i32) -> PgResult<()>);

// ---- get_all_vacuum_rels seqscan --------------------------------------
seam_core::seam!(pub fn scan_all_pg_class<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<PgClassScanRow<'mcx>>>);

// ---- procarray.h / transam.h / multixact.h ----------------------------
seam_core::seam!(pub fn get_oldest_non_removable_transaction_id(rel: Option<Oid>) -> PgResult<TransactionId>);
seam_core::seam!(pub fn get_oldest_multixact_id() -> PgResult<MultiXactId>);
seam_core::seam!(pub fn read_next_transaction_id() -> PgResult<TransactionId>);
seam_core::seam!(pub fn read_next_multixact_id() -> PgResult<MultiXactId>);
seam_core::seam!(pub fn multixact_member_freeze_threshold() -> PgResult<i32>);

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
seam_core::seam!(pub fn rel_frozenxid_minmxid(rel: Oid) -> PgResult<(TransactionId, MultiXactId)>);
seam_core::seam!(pub fn rel_pages_tuples(rel: Oid) -> PgResult<(BlockNumber, f64)>);
seam_core::seam!(pub fn relation_get_relation_name(rel: Oid) -> PgResult<String>);
seam_core::seam!(pub fn rel_relkind(rel: Oid) -> PgResult<u8>);
seam_core::seam!(pub fn relation_is_other_temp(rel: Oid) -> PgResult<bool>);
seam_core::seam!(pub fn rel_lock_relid(rel: Oid) -> PgResult<Oid>);
seam_core::seam!(pub fn rel_std_rd_options(rel: Oid) -> PgResult<StdRdOptionsView>);
seam_core::seam!(pub fn rel_reltoastrelid(rel: Oid) -> PgResult<Oid>);
seam_core::seam!(pub fn rel_relowner(rel: Oid) -> PgResult<Oid>);

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
seam_core::seam!(pub fn force_transaction_id_limit_update() -> PgResult<bool>);

// ---- vac_truncate_clog leaves -----------------------------------------
seam_core::seam!(pub fn lock_wraplimits_vacuum() -> PgResult<()>);
seam_core::seam!(pub fn unlock_wraplimits_vacuum() -> PgResult<()>);
seam_core::seam!(pub fn my_database_id() -> PgResult<Oid>);
seam_core::seam!(pub fn elog_debug2_skip_invalid_db(datname: String) -> PgResult<()>);
seam_core::seam!(pub fn async_notify_freeze_xids(frozen_xid: TransactionId) -> PgResult<()>);
seam_core::seam!(pub fn advance_oldest_commit_ts_xid(frozen_xid: TransactionId) -> PgResult<()>);
seam_core::seam!(pub fn truncate_clog(frozen_xid: TransactionId, oldest_xid_datoid: Oid) -> PgResult<()>);
seam_core::seam!(pub fn truncate_commit_ts(frozen_xid: TransactionId) -> PgResult<()>);
seam_core::seam!(pub fn truncate_multixact(min_multi: MultiXactId, minmulti_datoid: Oid) -> PgResult<()>);
seam_core::seam!(pub fn set_transaction_id_limit(frozen_xid: TransactionId, oldest_xid_datoid: Oid) -> PgResult<()>);
seam_core::seam!(pub fn set_multixact_id_limit(min_multi: MultiXactId, minmulti_datoid: Oid, is_startup: bool) -> PgResult<()>);

// ---- vacuum_rel leaves ------------------------------------------------
seam_core::seam!(pub fn set_proc_in_vacuum_flags(is_wraparound: bool) -> PgResult<()>);
seam_core::seam!(pub fn check_for_interrupts() -> PgResult<()>);
seam_core::seam!(pub fn injection_point(name: String) -> PgResult<()>);
seam_core::seam!(pub fn cluster_rel_for_vacuum_full(rel: Oid, verbose: bool) -> PgResult<()>);
seam_core::seam!(pub fn table_relation_vacuum(rel: Oid, params: VacuumParams, bstrategy: StrategyHandle) -> PgResult<()>);
seam_core::seam!(pub fn at_eoxact_guc(is_commit: bool, nestlevel: i32) -> PgResult<()>);
seam_core::seam!(pub fn get_user_id_and_sec_context() -> PgResult<(Oid, i32)>);
seam_core::seam!(pub fn set_user_id_and_sec_context(userid: Oid, sec_context: i32) -> PgResult<()>);
seam_core::seam!(pub fn new_guc_nest_level() -> PgResult<i32>);
seam_core::seam!(pub fn restrict_search_path() -> PgResult<()>);

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
