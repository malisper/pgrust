//! External-dependency seams for `backend/postmaster/autovacuum.c`.
//!
//! These are the boundaries between the launcher/worker scheduling state
//! machine (ported in `backend-postmaster-autovacuum`) and the runtime it
//! drives: the `AutoVacuumShmem` lifecycle + the index-keyed shmem accessors
//! (worker free/running lists, slot fields, the `wi_dobalance` atomic,
//! `av_startingWorker`, `av_nworkersForBalance`, the work-item array), the
//! AutovacuumLock/ScheduleLock LWLocks, process lifecycle/signalling,
//! latch/sleep/interrupts/GUC reload, timestamp helpers, xid/multixact
//! horizons, pgstat reporting, the catalog/relcache name lookups,
//! transaction/snapshot/vacuum execution, the cost-delay GUC plumbing, and the
//! memory-context lifecycle.
//!
//! Each call panics loudly until its owner is ported and installs it.
//!
//! NOTE (design debt — banked in DESIGN_DEBT.md): these are consolidated here,
//! mirroring autovacuum.c's own external surface, rather than each living in
//! its true owner's `-seams` crate. The shmem accessors and signalling are
//! genuinely owned by the autovacuum subsystem itself (defined in
//! autovacuum.c); the genuinely-foreign subset (vacuum execution, pgstat,
//! catalog seqscans, the lock manager, xact/snapshot) should migrate to their
//! owners' seam crates as those owners land.

extern crate alloc;

use seam_core::seam;

use types_core::{BlockNumber, MultiXactId, Oid, TimestampTz, TransactionId};
use types_error::PgResult;
use types_vacuum::vacuum::VacuumParams;
use types_vacuum::vacuumparallel::BufferAccessStrategyHandle as BufferStrategyHandle;
use types_autovacuum::{
    AvwDbase, DbStatEntry, OrphanClassRow, PgClassScanRow, RecheckClassRow, TabStatEntry,
};

seam!(pub fn autovacuum_shmem_init(worker_slots: i32) -> PgResult<()>);
seam!(pub fn get_launcher_pid() -> i32);
seam!(pub fn set_launcher_pid(pid: i32));
seam!(pub fn my_proc_pid() -> i32);
seam!(pub fn get_av_signal(which: i32) -> bool);
seam!(pub fn set_av_signal(which: i32, value: bool));
seam!(pub fn autovacuum_lock_acquire_exclusive() -> PgResult<()>);
seam!(pub fn autovacuum_lock_acquire_shared() -> PgResult<()>);
seam!(pub fn autovacuum_lock_release() -> PgResult<()>);
seam!(pub fn autovacuum_lock_held_by_me() -> bool);
seam!(pub fn autovacuum_schedule_lock_acquire_exclusive() -> PgResult<()>);
seam!(pub fn autovacuum_schedule_lock_release() -> PgResult<()>);
seam!(pub fn free_workers_count() -> u32);
seam!(pub fn free_workers_pop_head() -> i32);
seam!(pub fn free_workers_push_head(slot: i32));
seam!(pub fn running_workers_push_head(slot: i32));
seam!(pub fn worker_links_delete(slot: i32));
seam!(pub fn running_workers_slots() -> alloc::vec::Vec<i32>);
seam!(pub fn worker_get_dboid(slot: i32) -> Oid);
seam!(pub fn worker_set_dboid(slot: i32, dboid: Oid));
seam!(pub fn worker_get_tableoid(slot: i32) -> Oid);
seam!(pub fn worker_set_tableoid(slot: i32, tableoid: Oid));
seam!(pub fn worker_get_sharedrel(slot: i32) -> bool);
seam!(pub fn worker_set_sharedrel(slot: i32, sharedrel: bool));
seam!(pub fn worker_get_launchtime(slot: i32) -> TimestampTz);
seam!(pub fn worker_set_launchtime(slot: i32, t: TimestampTz));
seam!(pub fn worker_proc_is_set(slot: i32) -> bool);
seam!(pub fn worker_set_proc(slot: i32, set_to_myproc: bool));
seam!(pub fn worker_dobalance_unlocked_test(slot: i32) -> bool);
seam!(pub fn worker_dobalance_test_set(slot: i32));
seam!(pub fn worker_dobalance_clear(slot: i32));
seam!(pub fn starting_worker_slot() -> i32);
seam!(pub fn set_starting_worker_slot(slot: i32));
seam!(pub fn nworkers_for_balance_read() -> u32);
seam!(pub fn nworkers_for_balance_write(n: u32));
seam!(pub fn workitem_get_used(i: i32) -> bool);
seam!(pub fn workitem_get_active(i: i32) -> bool);
seam!(pub fn workitem_set_active(i: i32, v: bool));
seam!(pub fn workitem_set_used(i: i32, v: bool));
seam!(pub fn workitem_get_database(i: i32) -> Oid);
seam!(pub fn workitem_get_type(i: i32) -> i32);
seam!(pub fn workitem_get_relation(i: i32) -> Oid);
seam!(pub fn workitem_get_block_number(i: i32) -> BlockNumber);
seam!(pub fn workitem_fill(i: i32, av_type: i32, database: Oid, relation: Oid, blkno: BlockNumber));
seam!(pub fn send_start_autovac_worker_signal());
seam!(pub fn kill_launcher_sigusr2(launcherpid: i32));
seam!(pub fn register_free_worker_info());
seam!(pub fn proc_exit(code: i32) -> !);
seam!(pub fn wait_latch(timeout_ms: i64));
seam!(pub fn pg_usleep(usec: i64));
seam!(pub fn shutdown_request_pending() -> bool);
seam!(pub fn config_reload_pending() -> bool);
seam!(pub fn set_config_reload_pending(v: bool));
seam!(pub fn process_config_file() -> PgResult<()>);
seam!(pub fn got_sigusr2() -> bool);
seam!(pub fn set_got_sigusr2(v: bool));
seam!(pub fn process_launcher_barrier_and_catchup_interrupts() -> PgResult<()>);
seam!(pub fn check_for_interrupts() -> PgResult<()>);
seam!(pub fn get_current_timestamp() -> TimestampTz);
seam!(pub fn timestamp_difference_exceeds(start: TimestampTz, stop: TimestampTz, msec: i32) -> bool);
seam!(pub fn timestamp_difference(start: TimestampTz, stop: TimestampTz) -> (i64, i32));
seam!(pub fn timestamp_tz_plus_milliseconds(tz: TimestampTz, ms: i64) -> TimestampTz);
seam!(pub fn read_next_transaction_id() -> TransactionId);
seam!(pub fn read_next_multixact_id() -> MultiXactId);
seam!(pub fn multixact_member_freeze_threshold() -> i32);
seam!(pub fn pgstat_report_autovac(dbid: Oid));
seam!(pub fn pgstat_report_activity_running(activity: alloc::string::String));
seam!(pub fn my_database_id() -> Oid);
seam!(pub fn temp_namespace_is_idle(namespace: Oid) -> PgResult<bool>);
// Orphan-temp-table drop leaf operations.  The recheck/decision control flow
// (predicates, LOG decision) is ported in-crate (`worker::do_autovacuum`);
// these are the genuinely-foreign lock-manager / catalog / deletion /
// transaction leaves it drives.
seam!(pub fn conditional_lock_relation_oid_exclusive(relid: Oid) -> bool);
seam!(pub fn unlock_relation_oid_exclusive(relid: Oid));
seam!(pub fn orphan_recheck_fetch_class_row(relid: Oid) -> Option<OrphanClassRow>);
seam!(pub fn conditional_lock_namespace_object_share(namespace: Oid) -> bool);
seam!(pub fn get_namespace_name(namespace: Oid) -> Option<alloc::string::String>);
seam!(pub fn perform_deletion_orphan_temp_table(relid: Oid) -> PgResult<()>);
seam!(pub fn syscache_rel_isshared(relid: Oid) -> Option<bool>);
seam!(pub fn get_rel_name(relid: Oid) -> Option<alloc::string::String>);
seam!(pub fn get_rel_namespace_name(relid: Oid) -> Option<alloc::string::String>);
seam!(pub fn get_database_name(dboid: Oid) -> Option<alloc::string::String>);
seam!(pub fn database_uses_zero_freeze_ages() -> PgResult<bool>);
seam!(pub fn start_transaction_command() -> PgResult<()>);
seam!(pub fn commit_transaction_command() -> PgResult<()>);
seam!(pub fn push_active_snapshot() -> PgResult<()>);
seam!(pub fn pop_active_snapshot() -> PgResult<()>);
seam!(pub fn active_snapshot_set() -> bool);
seam!(pub fn get_vacuum_access_strategy() -> BufferStrategyHandle);
seam!(pub fn autovacuum_do_vac_analyze(
    relid: Oid,
    nspname: alloc::string::String,
    relname: alloc::string::String,
    params: VacuumParams,
    bstrategy: BufferStrategyHandle,
) -> PgResult<()>);
seam!(pub fn perform_brin_summarize_range(relation: Oid, blkno: BlockNumber) -> PgResult<()>);
seam!(pub fn set_query_cancel_pending(v: bool));
// The PG_CATCH body when a per-table vacuum/analyze errors out: HOLD_INTERRUPTS,
// EmitErrorReport (of the passed-in error, which the caller has already adorned
// with autovacuum's `errcontext("automatic {vacuum,analyze} of table ...")`),
// AbortOutOfAnyTransaction, FlushErrorState, MemoryContextReset(PortalContext),
// StartTransactionCommand, RESUME_INTERRUPTS.  All foreign error/xact
// machinery; the errcontext text and the catch-and-continue control flow are
// ported in-crate.
seam!(pub fn emit_report_and_restart_after_table_error(err: types_error::PgError));
seam!(pub fn vac_update_datfrozenxid() -> PgResult<()>);
seam!(pub fn set_vacuum_cost_delay(v: f64));
seam!(pub fn vacuum_cost_delay() -> f64);
seam!(pub fn vacuum_cost_delay_guc() -> f64);
seam!(pub fn set_vacuum_cost_limit(v: i32));
seam!(pub fn vacuum_cost_limit() -> i32);
seam!(pub fn vacuum_cost_limit_guc() -> i32);
seam!(pub fn vacuum_failsafe_active() -> bool);
seam!(pub fn vacuum_cost_active() -> bool);
seam!(pub fn set_vacuum_cost_active(v: bool));
seam!(pub fn set_vacuum_cost_balance(v: i32));
seam!(pub fn vacuum_freeze_min_age() -> i32);
seam!(pub fn vacuum_freeze_table_age() -> i32);
seam!(pub fn vacuum_multixact_freeze_min_age() -> i32);
seam!(pub fn vacuum_multixact_freeze_table_age() -> i32);
seam!(pub fn vacuum_max_eager_freeze_failure_rate() -> f64);
seam!(pub fn autovac_mem_cxt_create_and_switch(name: alloc::string::String));
seam!(pub fn switch_to_autovac_mem_cxt());
seam!(pub fn portal_context_create());
seam!(pub fn portal_context_reset());

// Catalog/pgstat readers (the seqscan + reloption-extraction bodies live
// behind these because table_open/heap_getnext/syscache are unported).
seam!(pub fn pgstat_fetch_stat_dbentry(datid: Oid) -> Option<DbStatEntry>);
seam!(pub fn pgstat_fetch_stat_tabentry(isshared: bool, relid: Oid) -> Option<TabStatEntry>);
seam!(pub fn get_database_list() -> PgResult<alloc::vec::Vec<AvwDbase>>);
seam!(pub fn do_autovacuum_scan_pg_class() -> PgResult<(alloc::vec::Vec<PgClassScanRow>, alloc::vec::Vec<PgClassScanRow>)>);
seam!(pub fn recheck_fetch_class_row(relid: Oid) -> Option<RecheckClassRow>);
