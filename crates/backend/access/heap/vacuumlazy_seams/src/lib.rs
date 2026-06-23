//! Seams for the lazy (concurrent) heap VACUUM driver
//! (`src/backend/access/heap/vacuumlazy.c`).
//!
//! Two roles live here:
//!
//! * **Inward** — [`heap_vacuum_rel`] is the driver's public entry. The
//!   command layer (`commands/vacuum.c`) calls it across a dependency cycle;
//!   the owning `backend-access-heap-vacuumlazy` crate installs it from its
//!   `init_seams()`.
//! * **Outward** — every other declaration is a function the driver reaches in
//!   a *not-yet-ported* owner (heap-page prune/freeze + visibility predicates,
//!   the visibility map, the TID store, the buffer manager / read stream, the
//!   FSM, the lock manager, relation truncation, parallel vacuum, the
//!   vacuum-command cutoff/relstat layer, progress / pgstat reporting, misc
//!   backend infra). These default to the [`seam_core::seam!`] loud panic and
//!   are installed by their owner when it lands — there is no silent fallback,
//!   nothing fabricates a buffer, page, TID-store result, visibility decision,
//!   or cutoff.

#![allow(non_snake_case)]

use alloc::string::String;
use alloc::vec::Vec;

extern crate alloc;

use types_core::{
    BlockNumber, Buffer, MultiXactId, OffsetNumber, Oid, TimestampTz, TransactionId,
};
use types_error::PgResult;
use rel::Relation;
use types_vacuum::vacuum::{HeapTupleFreeze, VacuumCutoffs, VacuumParams};
use types_vacuum::vacuumlazy::{
    GlobalVisStateHandle, LinePointerState, ParallelVacuumInit, ParallelVacuumInitArgs,
    ParallelVacuumStateHandle, PruneAndFreezeArgs, PruneAndFreezeOut, ReadStreamHandle,
    ScanCallback, TidStore, TidStoreIterHandle, UpdateRelStatsArgs, VmSetArgs,
};
use types_storage::buf::BufferAccessStrategy;
use types_vacuum::vacuumparallel::{IndexBulkDeleteResult, IndexVacuumInfo, VacDeadItemsInfo};

// =======================================================================
// Inward — the driver's public entry.
// =======================================================================

seam_core::seam!(
    /// `heap_vacuum_rel(rel, params, bstrategy)` — perform VACUUM for one heap
    /// relation. The caller has already established a transaction and opened
    /// and locked the relation; the owned `rel` is held for the whole scan
    /// (the `PlannerRun(mcx)` analog).
    pub fn heap_vacuum_rel<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: Relation<'mcx>,
        params: VacuumParams,
        bstrategy: BufferAccessStrategy,
    ) -> PgResult<()>
);

// =======================================================================
// commands/vacuum.h — cutoff / relstat / per-index command layer.
// =======================================================================

seam_core::seam!(
    /// `vacuum_get_cutoffs(rel, params, &cutoffs)` — compute the freeze/removal
    /// cutoffs; returns `aggressive`.
    pub fn vacuum_get_cutoffs<'mcx>(
        rel: &Relation<'mcx>,
        params: VacuumParams,
        cutoffs: &mut VacuumCutoffs,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `vacuum_xid_failsafe_check(cutoffs)`.
    pub fn vacuum_xid_failsafe_check(cutoffs: VacuumCutoffs) -> PgResult<bool>
);

seam_core::seam!(
    /// `vac_open_indexes(rel, RowExclusiveLock, &nindexes, &indrels)`. The
    /// `mcx` carries the driver run's arena so the opened index `Relation`s are
    /// allocated with the run's lifetime.
    pub fn vac_open_indexes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &Relation<'mcx>,
    ) -> PgResult<Vec<Relation<'mcx>>>
);

seam_core::seam!(
    /// `vac_close_indexes(nindexes, indrels, NoLock)`.
    pub fn vac_close_indexes<'mcx>(indrels: Vec<Relation<'mcx>>) -> PgResult<()>
);

seam_core::seam!(
    /// `vac_update_relstats(...)` — returns `(frozenxid_updated,
    /// minmulti_updated)`.
    pub fn vac_update_relstats(args: UpdateRelStatsArgs) -> PgResult<(bool, bool)>
);

seam_core::seam!(
    /// `vac_estimate_reltuples(relation, total_pages, scanned_pages,
    /// scanned_tuples)`.
    pub fn vac_estimate_reltuples(
        relation: Oid,
        total_pages: BlockNumber,
        scanned_pages: BlockNumber,
        scanned_tuples: f64,
    ) -> PgResult<f64>
);

seam_core::seam!(
    /// `vac_bulkdel_one_index(ivinfo, istat, dead_items, dead_items_info)`.
    pub fn vac_bulkdel_one_index(
        ivinfo: IndexVacuumInfo,
        istat: Option<IndexBulkDeleteResult>,
        dead_items: TidStore,
        dead_items_info: VacDeadItemsInfo,
    ) -> PgResult<IndexBulkDeleteResult>
);

seam_core::seam!(
    /// `vac_cleanup_one_index(ivinfo, istat)`.
    pub fn vac_cleanup_one_index(
        ivinfo: IndexVacuumInfo,
        istat: Option<IndexBulkDeleteResult>,
    ) -> PgResult<Option<IndexBulkDeleteResult>>
);

seam_core::seam!(
    /// `vacuum_delay_point(is_analyze)` — cost-based delay + interrupt check.
    pub fn vacuum_delay_point(is_analyze: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `VacuumFailsafeActive` (global flag) — read.
    pub fn vacuum_failsafe_active() -> PgResult<bool>
);
seam_core::seam!(
    /// `VacuumFailsafeActive = v` — write.
    pub fn set_vacuum_failsafe_active(v: bool) -> PgResult<()>
);
seam_core::seam!(
    /// `VacuumCostActive = v` — write.
    pub fn set_vacuum_cost_active(v: bool) -> PgResult<()>
);
seam_core::seam!(
    /// `VacuumCostBalance = v` — write.
    pub fn set_vacuum_cost_balance(v: i32) -> PgResult<()>
);

// =======================================================================
// access/heapam.h — heap-page prune/freeze + visibility predicates.
// =======================================================================

seam_core::seam!(
    /// `heap_page_prune_and_freeze(...)`.
    pub fn heap_page_prune_and_freeze(args: PruneAndFreezeArgs) -> PgResult<PruneAndFreezeOut>
);

seam_core::seam!(
    /// `log_heap_prune_and_freeze(...)` — emit the combined
    /// `XLOG_HEAP2_PRUNE_FREEZE` record. Used for page pruning (redirects /
    /// dead / removed), opportunistic/required freezing, and VACUUM's 2nd-pass
    /// reap (which only ever passes `unused`, with the other arrays empty and
    /// `cleanup_lock=false`). The freeze-plan array is deduplicated into the
    /// record by the owner.
    pub fn log_heap_prune_and_freeze(
        relation: Oid,
        buffer: Buffer,
        conflict_xid: TransactionId,
        cleanup_lock: bool,
        reason: i32,
        frozen: Vec<HeapTupleFreeze>,
        redirected: Vec<OffsetNumber>,
        dead: Vec<OffsetNumber>,
        unused: Vec<OffsetNumber>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_tuple_should_freeze(tuple, &cutoffs, &relfrozen_xid, &relmin_mxid)`
    /// — also advances the no-freeze relfrozen/relmin trackers (returned).
    pub fn heap_tuple_should_freeze(
        buffer: Buffer,
        offnum: OffsetNumber,
        cutoffs: VacuumCutoffs,
        relfrozen_xid_in: TransactionId,
        relmin_mxid_in: MultiXactId,
    ) -> PgResult<(bool, TransactionId, MultiXactId)>
);

seam_core::seam!(
    /// `heap_tuple_needs_eventual_freeze(tuple)`.
    pub fn heap_tuple_needs_eventual_freeze(
        buffer: Buffer,
        offnum: OffsetNumber,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `HeapTupleSatisfiesVacuum(tuple, OldestXmin, buffer)` — returns the
    /// `HTSV_Result` integer code.
    pub fn heap_tuple_satisfies_vacuum<'mcx>(
        rel: &Relation<'mcx>,
        buffer: Buffer,
        offnum: OffsetNumber,
        oldest_xmin: TransactionId,
    ) -> PgResult<i32>
);

// The `tidstore_*` outward seams (access/tidstore.h) were moved to
// `backend-access-common-tidstore-seams`, whose stem matches their true owner
// `backend-access-common-tidstore`.

// =======================================================================
// access/visibilitymap.h — the per-block visibility map.
// =======================================================================

seam_core::seam!(
    /// `visibilitymap_count(rel, &all_visible, &all_frozen)`.
    pub fn visibilitymap_count<'mcx>(rel: &Relation<'mcx>) -> PgResult<(BlockNumber, BlockNumber)>
);
seam_core::seam!(
    /// `visibilitymap_get_status(rel, heap_blk, &vmbuf)` — returns the `VM_*`
    /// status bits and the (possibly newly pinned) vm buffer.
    pub fn visibilitymap_get_status<'mcx>(
        rel: &Relation<'mcx>,
        heap_blk: BlockNumber,
        vmbuf_in: Buffer,
    ) -> PgResult<(u8, Buffer)>
);
seam_core::seam!(
    /// `visibilitymap_pin(rel, heap_blk, &vmbuf)`.
    pub fn visibilitymap_pin<'mcx>(
        rel: &Relation<'mcx>,
        heap_blk: BlockNumber,
        vmbuf_in: Buffer,
    ) -> PgResult<Buffer>
);
seam_core::seam!(
    /// `visibilitymap_set(...)` — returns the previous vm bits (`old_vmbits`).
    pub fn visibilitymap_set<'mcx>(rel: &Relation<'mcx>, args: VmSetArgs) -> PgResult<u8>
);
seam_core::seam!(
    /// `visibilitymap_clear(rel, heap_blk, vmbuf, flags)` — clear the given VM
    /// bits for `heap_blk`. Returns C's `bool`: whether any bit was actually
    /// cleared (heap_lock_tuple / heap_lock_updated_tuple_rec use it to decide
    /// whether to set `XLH_LOCK_ALL_FROZEN_CLEARED`).
    pub fn visibilitymap_clear<'mcx>(
        rel: &Relation<'mcx>,
        heap_blk: BlockNumber,
        vmbuf: Buffer,
        flags: u8,
    ) -> PgResult<bool>
);

// =======================================================================
// storage/read_stream.h — sequential read stream over a relation fork.
// =======================================================================

seam_core::seam!(
    /// `read_stream_begin_relation(flags, bstrategy, rel, fork, cb, cb_arg,
    /// per_buffer_size)`. The owned model passes a `ScanCallback` tag (and the
    /// TID-store iter handle for the reap stream) instead of a fn ptr; the
    /// callback bodies themselves run in-crate (see
    /// `scan_block::heap_vac_scan_next_block` and
    /// `scan_block::vacuum_reap_lp_read_stream_next`), so the buffer is read
    /// through `read_buffer_extended`.
    pub fn read_stream_begin_relation<'mcx>(
        flags: i32,
        bstrategy: BufferAccessStrategy,
        rel: &Relation<'mcx>,
        fork: i32,
        callback: ScanCallback,
        reap_iter: TidStoreIterHandle,
    ) -> PgResult<ReadStreamHandle>
);
seam_core::seam!(
    /// `read_stream_end(stream)`.
    pub fn read_stream_end(stream: ReadStreamHandle) -> PgResult<()>
);

// =======================================================================
// storage/bufmgr.h — buffer pin / lock / page access for the driver.
// =======================================================================

seam_core::seam!(
    /// `ReadBufferExtended(rel, fork, blkno, RBM_NORMAL, bstrategy)`.
    pub fn read_buffer_extended<'mcx>(
        rel: &Relation<'mcx>,
        fork: i32,
        blkno: BlockNumber,
        bstrategy: BufferAccessStrategy,
    ) -> PgResult<Buffer>
);
seam_core::seam!(
    /// `PrefetchBuffer(rel, fork, blkno)`.
    pub fn prefetch_buffer<'mcx>(rel: &Relation<'mcx>, fork: i32, blkno: BlockNumber) -> PgResult<()>
);
// `release_buffer` / `unlock_release_buffer` re-homed to the canonical owner
// `backend-storage-buffer-bufmgr-seams` (bufmgr.c); caller binds there.
// `lock_buffer` / `lock_buffer_for_cleanup` /
// `conditional_lock_buffer_for_cleanup` / `mark_buffer_dirty` /
// `buffer_get_block_number` re-homed to the canonical owner
// `backend-storage-buffer-bufmgr-seams` (bufmgr.c); caller binds there.
seam_core::seam!(
    /// `CheckBufferIsPinnedOnce(buffer)`.
    pub fn check_buffer_is_pinned_once(buffer: Buffer) -> PgResult<()>
);

// ---- page reads/writes over the buffer the substrate owns ------------

seam_core::seam!(
    /// `PageGetHeapFreeSpace(BufferGetPage(buffer))`.
    pub fn page_get_heap_free_space(buffer: Buffer) -> PgResult<usize>
);
seam_core::seam!(
    /// `PageIsNew(BufferGetPage(buffer))`.
    pub fn page_is_new(buffer: Buffer) -> PgResult<bool>
);
seam_core::seam!(
    /// `PageIsEmpty(BufferGetPage(buffer))`.
    pub fn page_is_empty(buffer: Buffer) -> PgResult<bool>
);
seam_core::seam!(
    /// `PageIsAllVisible(BufferGetPage(buffer))`.
    pub fn page_is_all_visible(buffer: Buffer) -> PgResult<bool>
);
seam_core::seam!(
    /// `PageSetAllVisible(BufferGetPage(buffer))`.
    pub fn page_set_all_visible(buffer: Buffer) -> PgResult<()>
);
seam_core::seam!(
    /// `PageClearAllVisible(BufferGetPage(buffer))`.
    pub fn page_clear_all_visible(buffer: Buffer) -> PgResult<()>
);
seam_core::seam!(
    /// `PageGetLSN(BufferGetPage(buffer)) == InvalidXLogRecPtr`?
    pub fn page_lsn_is_invalid(buffer: Buffer) -> PgResult<bool>
);
seam_core::seam!(
    /// `PageGetMaxOffsetNumber(BufferGetPage(buffer))`.
    pub fn page_get_max_offset_number(buffer: Buffer) -> PgResult<OffsetNumber>
);
seam_core::seam!(
    /// `log_newpage_buffer(buffer, page_std)`.
    pub fn log_newpage_buffer(buffer: Buffer, page_std: bool) -> PgResult<()>
);
seam_core::seam!(
    /// `PageTruncateLinePointerArray(BufferGetPage(buffer))`.
    pub fn page_truncate_line_pointer_array(buffer: Buffer) -> PgResult<()>
);
seam_core::seam!(
    /// Read the `ItemId` flag state at `(buffer, offnum)`.
    pub fn page_item_id_state(
        buffer: Buffer,
        offnum: OffsetNumber,
    ) -> PgResult<LinePointerState>
);
seam_core::seam!(
    /// `ItemIdSetUnused(PageGetItemId(page, offnum))`.
    pub fn page_item_id_set_unused(buffer: Buffer, offnum: OffsetNumber) -> PgResult<()>
);

// =======================================================================
// storage/freespace.h — the free space map.
// =======================================================================

seam_core::seam!(
    /// `FreeSpaceMapVacuumRange(rel, start, end)`.
    pub fn free_space_map_vacuum_range<'mcx>(
        rel: &Relation<'mcx>,
        start: BlockNumber,
        end: BlockNumber,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `RecordPageWithFreeSpace(rel, heap_blk, spaceavail)`.
    pub fn record_page_with_free_space<'mcx>(
        rel: &Relation<'mcx>,
        heap_blk: BlockNumber,
        spaceavail: usize,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `GetRecordedFreeSpace(rel, heap_blk)`.
    pub fn get_recorded_free_space<'mcx>(rel: &Relation<'mcx>, heap_blk: BlockNumber) -> PgResult<usize>
);

// =======================================================================
// storage/lmgr.h + catalog/storage.h — relation locks / truncation / size.
// =======================================================================

seam_core::seam!(
    /// `UnlockRelation(rel, lockmode)`.
    pub fn unlock_relation<'mcx>(rel: &Relation<'mcx>, lockmode: i32) -> PgResult<()>
);
seam_core::seam!(
    /// `ConditionalLockRelation(rel, lockmode)`.
    pub fn conditional_lock_relation<'mcx>(rel: &Relation<'mcx>, lockmode: i32) -> PgResult<bool>
);
seam_core::seam!(
    /// `LockHasWaitersRelation(rel, lockmode)`.
    pub fn lock_has_waiters_relation<'mcx>(rel: &Relation<'mcx>, lockmode: i32) -> PgResult<bool>
);
seam_core::seam!(
    /// `RelationTruncate(rel, nblocks)`.
    pub fn relation_truncate<'mcx>(rel: &Relation<'mcx>, nblocks: BlockNumber) -> PgResult<()>
);
seam_core::seam!(
    /// `RelationGetNumberOfBlocks(rel)`.
    pub fn relation_get_number_of_blocks<'mcx>(rel: &Relation<'mcx>) -> PgResult<BlockNumber>
);
seam_core::seam!(
    /// `RelationNeedsWAL(rel)`.
    pub fn relation_needs_wal<'mcx>(rel: &Relation<'mcx>) -> PgResult<bool>
);
seam_core::seam!(
    /// `RelationUsesLocalBuffers(rel)` — true for temp tables.
    pub fn relation_uses_local_buffers<'mcx>(rel: &Relation<'mcx>) -> PgResult<bool>
);

// ---- relcache field reads the driver does inline in C ----------------

seam_core::seam!(
    /// `RelationGetNamespace(rel)`.
    pub fn relation_get_namespace<'mcx>(rel: &Relation<'mcx>) -> PgResult<Oid>
);
seam_core::seam!(
    /// `RelationGetRelationName(rel)`.
    pub fn relation_get_relation_name<'mcx>(rel: &Relation<'mcx>) -> PgResult<String>
);
seam_core::seam!(
    /// `rel->rd_rel->relisshared`.
    pub fn relation_is_shared<'mcx>(rel: &Relation<'mcx>) -> PgResult<bool>
);
seam_core::seam!(
    /// `rel->rd_rel->reltuples` widened to f64.
    pub fn relation_get_reltuples<'mcx>(rel: &Relation<'mcx>) -> PgResult<f64>
);

// =======================================================================
// vacuumparallel.c — parallel index vacuuming.
// =======================================================================

seam_core::seam!(
    /// `parallel_vacuum_init(...)`.
    pub fn parallel_vacuum_init(args: ParallelVacuumInitArgs) -> PgResult<ParallelVacuumInit>
);
seam_core::seam!(
    /// `parallel_vacuum_end(pvs, istats)`.
    ///
    /// In C this is `void parallel_vacuum_end(ParallelVacuumState *pvs,
    /// IndexBulkDeleteResult **istats)`, where `istats[]` is an OUT-parameter:
    /// the per-index stats accumulated in the DSM-resident `pvs->indstats[]`
    /// are copied back into the caller's array before the parallel context is
    /// torn down. Since the repo cannot expose a `&mut` slice across this seam,
    /// the updated stats are RETURNED here (one entry per index, `None` for an
    /// index whose stats were never updated) and the caller stores them.
    pub fn parallel_vacuum_end(
        pvs: ParallelVacuumStateHandle,
    ) -> PgResult<Vec<Option<IndexBulkDeleteResult>>>
);
seam_core::seam!(
    /// `parallel_vacuum_get_dead_items(pvs, &dead_items_info)`.
    pub fn parallel_vacuum_get_dead_items(
        pvs: ParallelVacuumStateHandle,
    ) -> PgResult<(TidStore, VacDeadItemsInfo)>
);
seam_core::seam!(
    /// `parallel_vacuum_reset_dead_items(pvs)`.
    pub fn parallel_vacuum_reset_dead_items(pvs: ParallelVacuumStateHandle) -> PgResult<()>
);
seam_core::seam!(
    /// `parallel_vacuum_bulkdel_all_indexes(pvs, num_table_tuples,
    /// num_index_scans)`.
    pub fn parallel_vacuum_bulkdel_all_indexes(
        pvs: ParallelVacuumStateHandle,
        num_table_tuples: f64,
        num_index_scans: i32,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `parallel_vacuum_cleanup_all_indexes(pvs, num_table_tuples,
    /// num_index_scans, estimated_count)`.
    pub fn parallel_vacuum_cleanup_all_indexes(
        pvs: ParallelVacuumStateHandle,
        num_table_tuples: f64,
        num_index_scans: i32,
        estimated_count: bool,
    ) -> PgResult<()>
);

// =======================================================================
// commands/progress.h + pgstat — progress + cumulative stats reporting.
// =======================================================================

seam_core::seam!(
    /// `pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM, relid)`.
    pub fn pgstat_progress_start_command(cmdtype: i32, relid: Oid) -> PgResult<()>
);
seam_core::seam!(
    /// `pgstat_progress_update_param(index, val)`.
    pub fn pgstat_progress_update_param(index: i32, val: i64) -> PgResult<()>
);
seam_core::seam!(
    /// `pgstat_progress_update_multi_param(nparam, index[], val[])`.
    pub fn pgstat_progress_update_multi_param(
        index: Vec<i32>,
        val: Vec<i64>,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `pgstat_progress_end_command()`.
    pub fn pgstat_progress_end_command() -> PgResult<()>
);
seam_core::seam!(
    /// `pgstat_report_vacuum(tableoid, shared, livetuples, deadtuples,
    /// starttime)`.
    pub fn pgstat_report_vacuum(
        tableoid: Oid,
        shared: bool,
        livetuples: i64,
        deadtuples: i64,
        starttime: TimestampTz,
    ) -> PgResult<()>
);

// =======================================================================
// misc backend infra.
// =======================================================================

seam_core::seam!(
    /// `GlobalVisTestFor(rel)`.
    pub fn global_vis_test_for<'mcx>(rel: &Relation<'mcx>) -> PgResult<GlobalVisStateHandle>
);
// `pg_global_prng_uint32` is re-homed to `pg-prng-seams` (owner
// `pg-prng`), whose stem matches its true owner `src/common/prng.c`.

seam_core::seam!(
    /// `get_database_name(MyDatabaseId)`.
    pub fn get_database_name(dboid: Oid) -> PgResult<String>
);
seam_core::seam!(
    /// `get_namespace_name(nspoid)`.
    pub fn get_namespace_name(nspoid: Oid) -> PgResult<String>
);
// `check_for_interrupts` re-homed to the canonical owner
// `backend-tcop-postgres-seams` (`CHECK_FOR_INTERRUPTS()`); caller binds there.
seam_core::seam!(
    /// `WaitLatch(MyLatch, wakeEvents, timeout_ms, wait_event_info)`.
    pub fn wait_latch(
        wake_events: i32,
        timeout_ms: i64,
        wait_event_info: u32,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ResetLatch(MyLatch)`.
    pub fn reset_latch() -> PgResult<()>
);
seam_core::seam!(
    /// `AmAutoVacuumWorkerProcess()`.
    pub fn am_autovacuum_worker_process() -> PgResult<bool>
);
seam_core::seam!(
    /// `maintenance_work_mem` GUC (KB).
    pub fn maintenance_work_mem() -> PgResult<i32>
);
seam_core::seam!(
    /// `autovacuum_work_mem` GUC (KB; -1 = unset).
    pub fn autovacuum_work_mem() -> PgResult<i32>
);
seam_core::seam!(
    /// `MyDatabaseId`.
    pub fn my_database_id() -> PgResult<Oid>
);
seam_core::seam!(
    /// `ReadNextTransactionId()`.
    pub fn read_next_transaction_id() -> PgResult<TransactionId>
);
seam_core::seam!(
    /// `GetCurrentTimestamp()`.
    pub fn get_current_timestamp() -> PgResult<TimestampTz>
);
seam_core::seam!(
    /// `TimestampDifferenceExceeds(start, stop, msec)`.
    pub fn timestamp_difference_exceeds(
        start: TimestampTz,
        stop: TimestampTz,
        msec: i32,
    ) -> PgResult<bool>
);
seam_core::seam!(
    /// `TimestampDifference(start, stop, &secs, &microsecs)` — returns
    /// `(secs, microsecs)`.
    pub fn timestamp_difference(
        start: TimestampTz,
        stop: TimestampTz,
    ) -> PgResult<(i64, i32)>
);
seam_core::seam!(
    /// `track_io_timing` GUC.
    pub fn track_io_timing() -> PgResult<bool>
);
seam_core::seam!(
    /// `track_cost_delay_timing` GUC.
    pub fn track_cost_delay_timing() -> PgResult<bool>
);
seam_core::seam!(
    /// `pgStatBlockReadTime` accumulator (microseconds).
    pub fn pgstat_block_read_time() -> PgResult<i64>
);
seam_core::seam!(
    /// `pgStatBlockWriteTime` accumulator (microseconds).
    pub fn pgstat_block_write_time() -> PgResult<i64>
);
seam_core::seam!(
    /// `MyBEEntry->st_progress_param[idx]`.
    pub fn my_be_entry_progress_param(idx: i32) -> PgResult<i64>
);

// ---- instrumentation usage diffs / rusage (logged at the end) --------

seam_core::seam!(
    /// `pgWalUsage` snapshot — `(wal_records, wal_fpi, wal_bytes,
    /// wal_buffers_full)`.
    pub fn pg_wal_usage() -> PgResult<(i64, i64, u64, i64)>
);
seam_core::seam!(
    /// `pgBufferUsage` snapshot — `(shared_hit, shared_read, shared_dirtied,
    /// local_hit, local_read, local_dirtied)`.
    pub fn pg_buffer_usage() -> PgResult<(i64, i64, i64, i64, i64, i64)>
);
// `pg_rusage_init` / `pg_rusage_show` are re-homed to `pg-rusage-seams`
// (owner `backend-utils-misc-pg-rusage`), whose stem matches their true owner
// `src/backend/utils/misc/pg_rusage.c`. The start snapshot is now the caller's
// own `PgRUsage` value rather than seam-runtime state.

// ---- error-context stack (errcontext callback) ----------------------

seam_core::seam!(
    /// Push the vacuum `errcontext` callback.
    pub fn push_error_context() -> PgResult<()>
);
seam_core::seam!(
    /// Pop the vacuum `errcontext` callback.
    pub fn pop_error_context() -> PgResult<()>
);

// =======================================================================
// vacuumlazy_header_reads — page-resident tuple-header reads used by
// heap_page_is_all_visible (storage/bufpage.h tuple-header accessors).
// =======================================================================

seam_core::seam!(
    /// `HeapTupleHeaderXminCommitted(PageGetItem(page, off))` for the tuple at
    /// `(buffer, offnum)`.
    pub fn header_xmin_committed(buffer: Buffer, offnum: OffsetNumber) -> PgResult<bool>
);

seam_core::seam!(
    /// `HeapTupleHeaderGetXmin(PageGetItem(page, off))` (frozen →
    /// `FrozenTransactionId`) for the tuple at `(buffer, offnum)`.
    pub fn header_get_xmin(buffer: Buffer, offnum: OffsetNumber) -> PgResult<TransactionId>
);
