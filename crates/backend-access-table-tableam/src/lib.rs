//! Port of `src/backend/access/table/tableam.c` — table access method
//! routines too big to be inline functions — plus the `tableam.h` inline
//! dispatch wrappers this translation unit itself instantiates
//! (`table_index_fetch_begin`/`_tuple`/`_end`, `table_tuple_insert`/
//! `_delete`/`_update`).
//!
//! The dispatch model mirrors C: `relation->rd_tableam` is a vtable
//! ([`types_tableam::TableAmRoutine`], fetched through the relcache owner's
//! seam) whose callbacks the wrappers invoke. The open relation crosses as a
//! [`types_rel::Relation`] handle; scan and index-fetch descriptors are
//! owned values created by the AM.
//!
//! `default_table_access_method` and `synchronize_seqscans` are this unit's
//! GUC globals — backend-local state, so `thread_local!`.

#![allow(non_snake_case)]

use std::boxed::Box;
use std::cell::{Cell, RefCell};
use std::string::String;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{
    BlockNumber, ForkNumber, BLCKSZ, InvalidBlockNumber, InvalidForkNumber, FSM_FORKNUM,
    MAIN_FORKNUM, MaxBlockNumber, VISIBILITYMAP_FORKNUM,
};
use types_rel::Relation;
use types_core::xact::TransactionIdIsValid;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_nodes::{TupleSlotKind, TupleTableSlot};
use types_snapshot::snapshot::IsMVCCSnapshot;
use types_tableam::relscan::{
    ParallelBlockTableScanExt, ParallelBlockTableScanWorkerData, ParallelTableScanDescData,
    TableScanDesc, TableScanDescData, SO_ALLOW_PAGEMODE, SO_ALLOW_STRAT, SO_ALLOW_SYNC,
    SO_TEMP_SNAPSHOT, SO_TYPE_SEQSCAN, SO_TYPE_TIDSCAN,
};
use types_tableam::scankey::ScanKeyData;
use types_tableam::tableam::{
    BulkInsertStateData, IndexFetchTableData, LockTupleMode, LockTupleNoKeyExclusive, Snapshot,
    TM_FailureData, TM_Result, TU_UpdateIndexes, TableAmRoutine,
};
use types_tuple::access::{RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_VIEW};
use types_tuple::heaptuple::ItemPointerData;

use backend_utils_cache_relcache_seams as relcache;

/// Install this crate's seam implementations. No other crate declares seams
/// owned by this unit yet, so there is nothing to install.
pub fn init_seams() {}

// ===========================================================================
// Constants controlling parallel-seqscan block allocation (tableam.c)
// ===========================================================================

/// The number of I/O chunks we try to break a parallel seqscan down into.
const PARALLEL_SEQSCAN_NCHUNKS: u32 = 2048;
/// Ramp down size of allocations when we've only this number of chunks left.
const PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS: u32 = 64;
/// Cap the size of parallel I/O chunks to this number of blocks.
const PARALLEL_SEQSCAN_MAX_CHUNK_SIZE: u32 = 8192;

// ===========================================================================
// GUC variables (tableam.c)
// ===========================================================================

/// `DEFAULT_TABLE_ACCESS_METHOD` (`access/tableam.h`).
pub const DEFAULT_TABLE_ACCESS_METHOD: &str = "heap";

thread_local! {
    /// `char *default_table_access_method` — the `default_table_access_method`
    /// GUC.
    static DEFAULT_TABLE_ACCESS_METHOD_GUC: RefCell<String> =
        RefCell::new(String::from(DEFAULT_TABLE_ACCESS_METHOD));
    /// `bool synchronize_seqscans` — the `synchronize_seqscans` GUC.
    static SYNCHRONIZE_SEQSCANS_GUC: Cell<bool> = const { Cell::new(true) };
}

/// Read the `default_table_access_method` GUC.
pub fn default_table_access_method() -> String {
    DEFAULT_TABLE_ACCESS_METHOD_GUC.with(|v| v.borrow().clone())
}

/// Assign the `default_table_access_method` GUC (the guc.c assign hook's
/// store).
pub fn set_default_table_access_method(value: &str) {
    DEFAULT_TABLE_ACCESS_METHOD_GUC.with(|v| *v.borrow_mut() = String::from(value));
}

/// Read the `synchronize_seqscans` GUC.
pub fn synchronize_seqscans() -> bool {
    SYNCHRONIZE_SEQSCANS_GUC.with(Cell::get)
}

/// Assign the `synchronize_seqscans` GUC.
pub fn set_synchronize_seqscans(value: bool) {
    SYNCHRONIZE_SEQSCANS_GUC.with(|v| v.set(value));
}

// ===========================================================================
// Shared helpers
// ===========================================================================

/// `relation->rd_tableam` where C dereferences it unconditionally: a missing
/// vtable is the C NULL-pointer crash, so panic loudly.
fn am(relation: &Relation<'_>) -> TableAmRoutine {
    relcache::relation_rd_tableam::call(relation)
        .expect("relation has no table access method (C would dereference NULL rd_tableam)")
}

/// The `unlikely(TransactionIdIsValid(CheckXidAlive) && !bsysscan)` guard
/// used by the dispatch wrappers. We don't expect direct calls with valid
/// CheckXidAlive for catalog or regular tables; see the comments in xact.c
/// where these variables are declared.
fn unexpected_during_logical_decoding() -> bool {
    TransactionIdIsValid(backend_access_transam_xact_seams::check_xid_alive::call())
        && !backend_access_transam_xact_seams::bsysscan::call()
}

/// `elog(ERROR, ...)` — an internal error.
fn elog_error(message: impl Into<String>) -> PgError {
    PgError::error(message)
}

/// `add_size(s1, s2)` (`storage/shmem.c`): overflow-checked addition raising
/// C's error. Local private mirror of the unported shmem.c helper.
fn add_size(s1: usize, s2: usize) -> PgResult<usize> {
    s1.checked_add(s2).ok_or_else(|| {
        PgError::error("requested shared memory size overflows size_t")
            .with_sqlstate(types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED)
    })
}

/// `pg_nextpower2_32(num)` (`port/pg_bitutils.h`, static inline) — the next
/// power of 2 >= num, for num in [1, 2^31].
fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num > 0);
    if num & num.wrapping_sub(1) == 0 {
        return num;
    }
    // 1 << (pg_leftmost_one_pos32(num) + 1)
    1u32 << (31 - num.leading_zeros() + 1)
}

// ===========================================================================
// Slot functions (tableam.c)
// ===========================================================================

/// `table_slot_callbacks(relation)` — which slot callbacks (here: which slot
/// class) suit the relation.
pub fn table_slot_callbacks(relation: &Relation<'_>) -> TupleSlotKind {
    if let Some(am) = relcache::relation_rd_tableam::call(relation) {
        (am.slot_callbacks)(relation)
    } else if relation.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
        // Historically FDWs expect to store heap tuples in slots. Continue
        // handing them one, to make it less painful to adapt FDWs to new
        // versions. The cost of a heap slot over a virtual slot is pretty
        // small.
        TupleSlotKind::HeapTuple
    } else {
        // These need to be supported, as some parts of the code (like COPY)
        // need to create slots for such relations too. It seems better to
        // centralize the knowledge that a heap slot is the right thing in
        // that case here.
        debug_assert!({
            let relkind = relation.rd_rel.relkind;
            relkind == RELKIND_VIEW || relkind == RELKIND_PARTITIONED_TABLE
        });
        TupleSlotKind::Virtual
    }
}

/// `table_slot_create(relation, reglist)` — create a slot suitable for the
/// relation.
///
/// In C the optional `reglist` (`List **`) receives the new slot via
/// `lappend` so the caller can drop it later; in the owned model the caller
/// owns the returned slot and registers it itself (push it onto the list
/// standing in for `*reglist`).
pub fn table_slot_create<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'_>,
) -> PgResult<TupleTableSlot> {
    let tts_cb = table_slot_callbacks(relation);
    let tupdesc = Some(mcx::alloc_in(mcx, relation.rd_att.clone_in(mcx)?)?);
    backend_executor_execTuples_seams::make_single_tuple_table_slot::call(mcx, tupdesc, tts_cb)
}

// ===========================================================================
// Table scan functions (tableam.c)
// ===========================================================================

/// `table_beginscan_catalog(relation, nkeys, key)`.
pub fn table_beginscan_catalog<'mcx>(
    relation: &Relation<'mcx>,
    nkeys: i32,
    key: Vec<ScanKeyData>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags =
        SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE | SO_TEMP_SNAPSHOT;
    let relid = relation.rd_id; // RelationGetRelid(relation)
    let snapshot = backend_utils_time_snapmgr_seams::register_snapshot::call(
        backend_utils_time_snapmgr_seams::get_catalog_snapshot::call(relid)?,
    )?;

    (am(relation).scan_begin)(relation, Some(snapshot), nkeys, key, None, flags)
}

// ===========================================================================
// Parallel table scan related functions (tableam.c)
// ===========================================================================

/// `table_parallelscan_estimate(rel, snapshot)`.
pub fn table_parallelscan_estimate(rel: &Relation<'_>, snapshot: &Snapshot) -> PgResult<usize> {
    let mut sz: usize = 0;

    match snapshot {
        Some(s) if IsMVCCSnapshot(s) => {
            sz = add_size(
                sz,
                backend_utils_time_snapmgr_seams::estimate_snapshot_space::call(s),
            )?;
        }
        _ => {
            // Assert(snapshot == SnapshotAny)
            debug_assert!(snapshot.is_none());
        }
    }

    sz = add_size(sz, (am(rel).parallelscan_estimate)(rel))?;

    Ok(sz)
}

/// `table_parallelscan_initialize(rel, pscan, snapshot)`.
pub fn table_parallelscan_initialize(
    rel: &Relation<'_>,
    pscan: &mut ParallelTableScanDescData,
    snapshot: &Snapshot,
) -> PgResult<()> {
    let snapshot_off = (am(rel).parallelscan_initialize)(rel, pscan)?;

    pscan.phs_snapshot_off = snapshot_off;

    match snapshot {
        Some(s) if IsMVCCSnapshot(s) => {
            // SerializeSnapshot(snapshot, (char *) pscan + pscan->phs_snapshot_off)
            pscan.phs_snapshot_data =
                Some(backend_utils_time_snapmgr_seams::serialize_snapshot::call(s)?);
            pscan.phs_snapshot_any = false;
        }
        _ => {
            // Assert(snapshot == SnapshotAny)
            debug_assert!(snapshot.is_none());
            pscan.phs_snapshot_any = true;
        }
    }

    Ok(())
}

/// `table_beginscan_parallel(relation, pscan)`.
pub fn table_beginscan_parallel<'mcx>(
    relation: &Relation<'mcx>,
    pscan: Arc<ParallelTableScanDescData>,
) -> PgResult<TableScanDesc<'mcx>> {
    let mut flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;

    debug_assert!(types_storage::RelFileLocatorEquals(
        &relation.rd_locator,
        &pscan.phs_locator
    ));

    let snapshot: Snapshot;
    if !pscan.phs_snapshot_any {
        // Snapshot was serialized -- restore it
        let bytes = pscan
            .phs_snapshot_data
            .as_deref()
            .expect("parallel scan descriptor carries no serialized snapshot");
        let restored = backend_utils_time_snapmgr_seams::restore_snapshot::call(bytes)?;
        snapshot = Some(backend_utils_time_snapmgr_seams::register_snapshot::call(
            restored,
        )?);
        flags |= SO_TEMP_SNAPSHOT;
    } else {
        // SnapshotAny passed by caller (not serialized)
        snapshot = None;
    }

    (am(relation).scan_begin)(relation, snapshot, 0, Vec::new(), Some(pscan), flags)
}

// ===========================================================================
// Index scan related functions (tableam.h wrappers used by this unit)
// ===========================================================================

/// `table_index_fetch_begin(rel)` (tableam.h inline).
pub fn table_index_fetch_begin<'mcx>(
    rel: &Relation<'mcx>,
) -> PgResult<Box<IndexFetchTableData<'mcx>>> {
    (am(rel).index_fetch_begin)(rel)
}

/// `table_index_fetch_end(scan)` (tableam.h inline).
pub fn table_index_fetch_end(scan: Box<IndexFetchTableData<'_>>) -> PgResult<()> {
    let routine = am(&scan.rel);
    (routine.index_fetch_end)(scan)
}

/// `table_index_fetch_tuple(scan, tid, snapshot, slot, call_again, all_dead)`
/// (tableam.h inline).
pub fn table_index_fetch_tuple(
    scan: &mut IndexFetchTableData<'_>,
    tid: &ItemPointerData,
    snapshot: &Snapshot,
    slot: &mut TupleTableSlot,
    call_again: &mut bool,
    all_dead: Option<&mut bool>,
) -> PgResult<bool> {
    // We don't expect direct calls to table_index_fetch_tuple with valid
    // CheckXidAlive for catalog or regular tables. See detailed comments in
    // xact.c where these variables are declared.
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_index_fetch_tuple call during logical decoding",
        ));
    }

    let routine = am(&scan.rel);
    (routine.index_fetch_tuple)(scan, tid, snapshot, slot, call_again, all_dead)
}

/// `table_index_fetch_tuple_check(rel, tid, snapshot, all_dead)` (tableam.c).
///
/// To perform the check simply start an index scan, create the necessary
/// slot, do the heap lookup, and shut everything down again. This could be
/// optimized, but is unlikely to matter from a performance POV. If there
/// frequently are live index pointers also matching a unique index key, the
/// CPU overhead of this routine is unlikely to matter.
///
/// Note that `tid` may be modified when we return true if the AM supports
/// storing multiple row versions reachable via a single index entry (like
/// heap's HOT).
pub fn table_index_fetch_tuple_check<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'_>,
    tid: &mut ItemPointerData,
    snapshot: Snapshot,
    all_dead: Option<&mut bool>,
) -> PgResult<bool> {
    let mut call_again = false;

    let mut slot = table_slot_create(mcx, rel)?;
    let mut scan = table_index_fetch_begin(rel)?;
    let found = table_index_fetch_tuple(
        &mut scan,
        tid,
        &snapshot,
        &mut slot,
        &mut call_again,
        all_dead,
    )?;
    table_index_fetch_end(scan)?;
    backend_executor_execTuples_seams::exec_drop_single_tuple_table_slot::call(slot)?;

    Ok(found)
}

// ===========================================================================
// Non-modifying operations on individual tuples (tableam.c)
// ===========================================================================

/// `table_tuple_get_latest_tid(scan, tid)`.
pub fn table_tuple_get_latest_tid(
    scan: &mut TableScanDescData<'_>,
    tid: &mut ItemPointerData,
) -> PgResult<()> {
    let tableam = am(&scan.rs_rd);

    // We don't expect direct calls to table_tuple_get_latest_tid with valid
    // CheckXidAlive for catalog or regular tables. See detailed comments in
    // xact.c where these variables are declared.
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_tuple_get_latest_tid call during logical decoding",
        ));
    }

    // Since this can be called with user-supplied TID, don't trust the input
    // too much.
    if !(tableam.tuple_tid_valid)(scan, tid)? {
        let blk = tid.ip_blkid.block_number();
        let off = tid.ip_posid;
        let relname = scan.rs_rd.name();
        return Err(PgError::error(format!(
            "tid ({blk}, {off}) is not valid for relation \"{relname}\""
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    (tableam.tuple_get_latest_tid)(scan, tid)
}

// ===========================================================================
// Table scan setup/teardown wrappers (tableam.h inline)
// ===========================================================================

/// `table_beginscan_tid(rel, snapshot)` (tableam.h inline) — alternative entry
/// point for setting up a `TableScanDesc` for a TID scan.
pub fn table_beginscan_tid<'mcx>(
    rel: &Relation<'mcx>,
    snapshot: Snapshot,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_TIDSCAN;
    (am(rel).scan_begin)(rel, snapshot, 0, Vec::new(), None, flags)
}

/// `table_endscan(scan)` (tableam.h inline) — end a relation scan.
pub fn table_endscan(scan: TableScanDesc<'_>) -> PgResult<()> {
    let routine = am(&scan.rs_rd);
    (routine.scan_end)(scan)
}

/// `table_rescan(scan, key)` (tableam.h inline) — restart a relation scan.
pub fn table_rescan(scan: &mut TableScanDescData<'_>, key: Option<&[ScanKeyData]>) -> PgResult<()> {
    let routine = am(&scan.rs_rd);
    (routine.scan_rescan)(scan, key, false, false, false, false)
}

/// `table_tuple_tid_valid(scan, tid)` (tableam.h inline) — verify `tid` is a
/// potentially valid tuple identifier.
pub fn table_tuple_tid_valid(
    scan: &mut TableScanDescData<'_>,
    tid: &ItemPointerData,
) -> PgResult<bool> {
    let routine = am(&scan.rs_rd);
    (routine.tuple_tid_valid)(scan, tid)
}

/// `table_tuple_fetch_row_version(rel, tid, snapshot, slot)` (tableam.h inline)
/// — fetch the tuple at `tid` into `slot`, after a visibility test against
/// `snapshot`.
pub fn table_tuple_fetch_row_version(
    rel: &Relation<'_>,
    tid: &ItemPointerData,
    snapshot: &Snapshot,
    slot: &mut TupleTableSlot,
) -> PgResult<bool> {
    // We don't expect direct calls to table_tuple_fetch_row_version with valid
    // CheckXidAlive for catalog or regular tables. See detailed comments in
    // xact.c where these variables are declared.
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_tuple_fetch_row_version call during logical decoding",
        ));
    }

    (am(rel).tuple_fetch_row_version)(rel, tid, snapshot, slot)
}

// ===========================================================================
// Manipulations of physical tuples (tableam.h wrappers used by this unit)
// ===========================================================================

/// `table_tuple_insert(rel, slot, cid, options, bistate)` (tableam.h inline).
pub fn table_tuple_insert(
    rel: &Relation<'_>,
    slot: &mut TupleTableSlot,
    cid: types_core::xact::CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    (am(rel).tuple_insert)(rel, slot, cid, options, bistate)
}

/// `table_tuple_delete(rel, tid, cid, snapshot, crosscheck, wait, tmfd,
/// changingPart)` (tableam.h inline).
pub fn table_tuple_delete(
    rel: &Relation<'_>,
    tid: &ItemPointerData,
    cid: types_core::xact::CommandId,
    snapshot: &Snapshot,
    crosscheck: &Snapshot,
    wait: bool,
    tmfd: &mut TM_FailureData,
    changingPart: bool,
) -> PgResult<TM_Result> {
    (am(rel).tuple_delete)(rel, tid, cid, snapshot, crosscheck, wait, tmfd, changingPart)
}

/// `table_tuple_update(rel, otid, slot, cid, snapshot, crosscheck, wait,
/// tmfd, lockmode, update_indexes)` (tableam.h inline).
#[allow(clippy::too_many_arguments)]
pub fn table_tuple_update(
    rel: &Relation<'_>,
    otid: &ItemPointerData,
    slot: &mut TupleTableSlot,
    cid: types_core::xact::CommandId,
    snapshot: &Snapshot,
    crosscheck: &Snapshot,
    wait: bool,
    tmfd: &mut TM_FailureData,
    lockmode: &mut LockTupleMode,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<TM_Result> {
    (am(rel).tuple_update)(
        rel,
        otid,
        slot,
        cid,
        snapshot,
        crosscheck,
        wait,
        tmfd,
        lockmode,
        update_indexes,
    )
}

// ===========================================================================
// Functions to make modifications a bit simpler (tableam.c)
// ===========================================================================

/// `simple_table_tuple_insert(rel, slot)` — insert a tuple.
///
/// Currently, this routine differs from `table_tuple_insert` only in
/// supplying a default command ID and not allowing access to the speedup
/// options.
pub fn simple_table_tuple_insert(rel: &Relation<'_>, slot: &mut TupleTableSlot) -> PgResult<()> {
    let cid = backend_access_transam_xact_seams::get_current_command_id::call(true)?;
    table_tuple_insert(rel, slot, cid, 0, None)
}

/// `simple_table_tuple_delete(rel, tid, snapshot)` — delete a tuple.
///
/// This routine may be used to delete a tuple when concurrent updates of the
/// target tuple are not expected (for example, because we have a lock on the
/// relation associated with the tuple). Any failure is reported via
/// `ereport()`.
pub fn simple_table_tuple_delete(
    rel: &Relation<'_>,
    tid: &ItemPointerData,
    snapshot: &Snapshot,
) -> PgResult<()> {
    let mut tmfd = TM_FailureData::default();

    let cid = backend_access_transam_xact_seams::get_current_command_id::call(true)?;
    let result = table_tuple_delete(
        rel,
        tid,
        cid,
        snapshot,
        &None, // InvalidSnapshot
        true,  // wait for commit
        &mut tmfd,
        false, // changingPart
    )?;

    match result {
        // Tuple was already updated in current command?
        TM_Result::TM_SelfModified => Err(elog_error("tuple already updated by self")),
        // done successfully
        TM_Result::TM_Ok => Ok(()),
        TM_Result::TM_Updated => Err(elog_error("tuple concurrently updated")),
        TM_Result::TM_Deleted => Err(elog_error("tuple concurrently deleted")),
        other => Err(elog_error(format!(
            "unrecognized table_tuple_delete status: {}",
            other as u32
        ))),
    }
}

/// `simple_table_tuple_update(rel, otid, slot, snapshot, update_indexes)` —
/// replace a tuple.
///
/// This routine may be used to update a tuple when concurrent updates of the
/// target tuple are not expected (for example, because we have a lock on the
/// relation associated with the tuple). Any failure is reported via
/// `ereport()`.
pub fn simple_table_tuple_update(
    rel: &Relation<'_>,
    otid: &ItemPointerData,
    slot: &mut TupleTableSlot,
    snapshot: &Snapshot,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<()> {
    let mut tmfd = TM_FailureData::default();
    let mut lockmode: LockTupleMode = LockTupleNoKeyExclusive;

    let cid = backend_access_transam_xact_seams::get_current_command_id::call(true)?;
    let result = table_tuple_update(
        rel,
        otid,
        slot,
        cid,
        snapshot,
        &None, // InvalidSnapshot
        true,  // wait for commit
        &mut tmfd,
        &mut lockmode,
        update_indexes,
    )?;

    match result {
        // Tuple was already updated in current command?
        TM_Result::TM_SelfModified => Err(elog_error("tuple already updated by self")),
        // done successfully
        TM_Result::TM_Ok => Ok(()),
        TM_Result::TM_Updated => Err(elog_error("tuple concurrently updated")),
        TM_Result::TM_Deleted => Err(elog_error("tuple concurrently deleted")),
        other => Err(elog_error(format!(
            "unrecognized table_tuple_update status: {}",
            other as u32
        ))),
    }
}

// ===========================================================================
// Helper functions to implement parallel scans for block oriented AMs
// (tableam.c)
// ===========================================================================

/// `table_block_parallelscan_estimate(rel)`.
pub fn table_block_parallelscan_estimate(_rel: &Relation<'_>) -> usize {
    core::mem::size_of::<ParallelTableScanDescData>()
}

/// `table_block_parallelscan_initialize(rel, pscan)`.
pub fn table_block_parallelscan_initialize(
    rel: &Relation<'_>,
    pscan: &mut ParallelTableScanDescData,
) -> PgResult<usize> {
    pscan.phs_locator = rel.rd_locator;
    let phs_nblocks = backend_storage_buffer_bufmgr_seams::
        relation_get_number_of_blocks_in_fork::call(rel.rd_id, MAIN_FORKNUM)?;
    // compare phs_syncscan initialization to similar logic in initscan
    pscan.phs_syncscan = synchronize_seqscans()
        && !rel.uses_local_buffers()
        && phs_nblocks > (backend_utils_init_small_seams::nbuffers::call() / 4) as BlockNumber;
    // SpinLockInit(&bpscan->phs_mutex); bpscan->phs_startblock =
    // InvalidBlockNumber; pg_atomic_init_u64(&bpscan->phs_nallocated, 0) —
    // a freshly defaulted block extension.
    let block = ParallelBlockTableScanExt {
        phs_nblocks,
        ..ParallelBlockTableScanExt::default()
    };
    pscan.block = Some(block);

    Ok(core::mem::size_of::<ParallelTableScanDescData>())
}

/// `table_block_parallelscan_reinitialize(rel, pscan)`.
pub fn table_block_parallelscan_reinitialize(
    _rel: &Relation<'_>,
    pscan: &ParallelTableScanDescData,
) {
    let bpscan = block_ext(pscan);
    // pg_atomic_write_u64(&bpscan->phs_nallocated, 0)
    bpscan.phs_nallocated.store(0, Ordering::SeqCst);
}

/// `table_block_parallelscan_startblock_init(rel, pbscanwork, pbscan)` —
/// find and set the scan's startblock.
///
/// Determine where the parallel seq scan should start. This function may be
/// called many times, once by each parallel worker. We must be careful only
/// to set the startblock once.
pub fn table_block_parallelscan_startblock_init(
    rel: &Relation<'_>,
    pbscanwork: &mut ParallelBlockTableScanWorkerData,
    pbscan: &ParallelTableScanDescData,
) -> PgResult<()> {
    let mut sync_startpage: BlockNumber = InvalidBlockNumber;
    let bpscan = block_ext(pbscan);

    // Reset the state we use for controlling allocation size.
    *pbscanwork = ParallelBlockTableScanWorkerData::default();

    // StaticAssertStmt(MaxBlockNumber <= 0xFFFFFFFE, ...)
    const _: () = assert!(
        MaxBlockNumber <= 0xFFFF_FFFE,
        "pg_nextpower2_32 may be too small for non-standard BlockNumber width"
    );

    // We determine the chunk size based on the size of the relation. First
    // we split the relation into PARALLEL_SEQSCAN_NCHUNKS chunks but we then
    // take the next highest power of 2 number of the chunk size. This means
    // we split the relation into somewhere between PARALLEL_SEQSCAN_NCHUNKS
    // and PARALLEL_SEQSCAN_NCHUNKS / 2 chunks.
    pbscanwork.phsw_chunk_size = pg_nextpower2_32(core::cmp::max(
        bpscan.phs_nblocks / PARALLEL_SEQSCAN_NCHUNKS,
        1,
    ));

    // Ensure we don't go over the maximum chunk size with larger tables.
    // This means we may get much more than PARALLEL_SEQSCAN_NCHUNKS for
    // larger tables. Too large a chunk size has been shown to be detrimental
    // to synchronous scan performance.
    pbscanwork.phsw_chunk_size =
        core::cmp::min(pbscanwork.phsw_chunk_size, PARALLEL_SEQSCAN_MAX_CHUNK_SIZE);

    // retry:
    loop {
        // Grab the spinlock.
        let mut startblock = bpscan
            .phs_startblock
            .lock()
            .expect("phs_mutex poisoned");

        // If the scan's startblock has not yet been initialized, we must do
        // so now. If this is not a synchronized scan, we just start at block
        // 0, but if it is a synchronized scan, we must get the starting
        // position from the synchronized scan machinery. We can't hold the
        // spinlock while doing that, though, so release the spinlock, get
        // the information we need, and retry. If nobody else has initialized
        // the scan in the meantime, we'll fill in the value we fetched on
        // the second time through.
        if *startblock == InvalidBlockNumber {
            if !pbscan.phs_syncscan {
                *startblock = 0;
            } else if sync_startpage != InvalidBlockNumber {
                *startblock = sync_startpage;
            } else {
                drop(startblock); // SpinLockRelease(&pbscan->phs_mutex)
                sync_startpage = backend_access_common_syncscan_seams::ss_get_location::call(
                    rel.rd_id,
                    bpscan.phs_nblocks,
                )?;
                continue; // goto retry
            }
        }
        // SpinLockRelease(&pbscan->phs_mutex) — guard drops here.
        break;
    }

    Ok(())
}

/// `table_block_parallelscan_nextpage(rel, pbscanwork, pbscan)` — get the
/// next page to scan.
///
/// Even if there are no pages left to scan, another backend could have
/// grabbed a page to scan and not yet finished looking at it, so it doesn't
/// follow that the scan is done when the first backend gets an
/// InvalidBlockNumber return.
pub fn table_block_parallelscan_nextpage(
    rel: &Relation<'_>,
    pbscanwork: &mut ParallelBlockTableScanWorkerData,
    pbscan: &ParallelTableScanDescData,
) -> PgResult<BlockNumber> {
    let bpscan = block_ext(pbscan);
    let nallocated: u64;

    // The logic below allocates block numbers out to parallel workers in a
    // way that each worker will receive a set of consecutive block numbers
    // to scan ("chunks"; sized in table_block_parallelscan_startblock_init,
    // ramped down towards the end of the scan). phsw_chunk_remaining tracks
    // the blocks remaining in this worker's chunk; phs_nallocated tracks how
    // many blocks have been allocated to workers overall, and may exceed
    // phs_nblocks because workers keep fetch-and-adding after the scan is
    // exhausted — which is why it must be 64 bits wide.

    // First check if we have any remaining blocks in a previous chunk for
    // this worker. We must consume all of the blocks from that before we
    // allocate a new chunk to the worker.
    if pbscanwork.phsw_chunk_remaining > 0 {
        // Give them the next block in the range and update the remaining
        // number of blocks.
        pbscanwork.phsw_nallocated = pbscanwork.phsw_nallocated.wrapping_add(1);
        nallocated = pbscanwork.phsw_nallocated;
        pbscanwork.phsw_chunk_remaining = pbscanwork.phsw_chunk_remaining.wrapping_sub(1);
    } else {
        // When we've only got PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS chunks
        // remaining in the scan, we half the chunk size. Since we reduce the
        // chunk size here, we'll hit this again after doing
        // PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS at the new size. After a few
        // iterations of this, we'll end up doing the last few blocks with
        // the chunk size set to 1.
        //
        // C computes `phs_nblocks - (phsw_chunk_size * RAMPDOWN)` in 32-bit
        // BlockNumber arithmetic, which wraps; replicate exactly before
        // widening to u64 for the comparison.
        if pbscanwork.phsw_chunk_size > 1
            && pbscanwork.phsw_nallocated
                > bpscan.phs_nblocks.wrapping_sub(
                    pbscanwork
                        .phsw_chunk_size
                        .wrapping_mul(PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS),
                ) as u64
        {
            pbscanwork.phsw_chunk_size >>= 1;
        }

        pbscanwork.phsw_nallocated = bpscan
            .phs_nallocated
            .fetch_add(pbscanwork.phsw_chunk_size as u64, Ordering::SeqCst);
        nallocated = pbscanwork.phsw_nallocated;

        // Set the remaining number of blocks in this chunk so that
        // subsequent calls from this worker continue on with this chunk
        // until it's done.
        pbscanwork.phsw_chunk_remaining = pbscanwork.phsw_chunk_size.wrapping_sub(1);
    }

    let phs_startblock = *bpscan.phs_startblock.lock().expect("phs_mutex poisoned");

    let page: BlockNumber = if nallocated >= bpscan.phs_nblocks as u64 {
        InvalidBlockNumber // all blocks have been allocated
    } else {
        (nallocated
            .wrapping_add(phs_startblock as u64)
            .wrapping_rem(bpscan.phs_nblocks as u64)) as BlockNumber
    };

    // Report scan location. Normally, we report the current page number.
    // When we reach the end of the scan, though, we report the starting
    // page, not the ending page, just so the starting positions for later
    // scans doesn't slew backwards. We only report the position at the end
    // of the scan once, though: subsequent callers will report nothing.
    if pbscan.phs_syncscan {
        if page != InvalidBlockNumber {
            backend_access_common_syncscan_seams::ss_report_location::call(rel.rd_id, page)?;
        } else if nallocated == bpscan.phs_nblocks as u64 {
            backend_access_common_syncscan_seams::ss_report_location::call(
                rel.rd_id,
                phs_startblock,
            )?;
        }
    }

    Ok(page)
}

/// The block-oriented extension of a shared parallel-scan descriptor — the C
/// `(ParallelBlockTableScanDesc) pscan` downcast; absent means the C cast
/// would have read uninitialized memory, so panic loudly.
fn block_ext(pscan: &ParallelTableScanDescData) -> &ParallelBlockTableScanExt {
    pscan
        .block
        .as_ref()
        .expect("parallel scan descriptor is not block-oriented")
}

// ===========================================================================
// Helper functions to implement relation sizing for block oriented AMs
// (tableam.c)
// ===========================================================================

/// `table_block_relation_size(rel, forkNumber)`.
///
/// If a table AM uses the various relation forks as the sole place where
/// data is stored, and if it uses them in the expected manner (e.g. the
/// actual data is in the main fork rather than some other), it can use this
/// implementation of the relation_size callback rather than implementing its
/// own.
pub fn table_block_relation_size(rel: &Relation<'_>, forkNumber: ForkNumber) -> PgResult<u64> {
    let mut nblocks: u64 = 0;

    // RelationGetSmgr(rel) — the smgr handle is the (locator, backend) pair.
    let rlocator = rel.rd_locator;
    let backend = rel.rd_backend;

    // InvalidForkNumber indicates returning the size for all forks
    if forkNumber == InvalidForkNumber {
        // C: `for (int i = 0; i < MAX_FORKNUM; i++)` — i.e. every fork below
        // MAX_FORKNUM (INIT_FORKNUM is excluded).
        for fork in [MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM] {
            nblocks = nblocks.wrapping_add(backend_storage_smgr_seams::smgrnblocks::call(
                rlocator, backend, fork,
            )? as u64);
        }
    } else {
        nblocks =
            backend_storage_smgr_seams::smgrnblocks::call(rlocator, backend, forkNumber)? as u64;
    }

    Ok(nblocks.wrapping_mul(BLCKSZ as u64))
}

/// `table_block_relation_estimate_size(rel, attr_widths, pages, tuples,
/// allvisfrac, overhead_bytes_per_tuple, usable_bytes_per_page)`.
///
/// This function can't be directly used as the implementation of the
/// relation_estimate_size callback, because it has a few additional
/// parameters. Instead, it is intended to be used as a helper function; the
/// caller can pass through the arguments to its relation_estimate_size
/// function plus the additional values required here.
///
/// `overhead_bytes_per_tuple` should contain the approximate number of bytes
/// of storage required to store a tuple above and beyond what is required
/// for the tuple data proper (tuple header, item pointer). This is only used
/// for query planning, so a table AM where the value is not constant could
/// choose to pass a "best guess".
///
/// `usable_bytes_per_page` should contain the approximate number of bytes
/// per page usable for tuple data, excluding the page header and any
/// anticipated special space.
#[allow(clippy::too_many_arguments)]
pub fn table_block_relation_estimate_size(
    rel: &Relation<'_>,
    attr_widths: Option<&mut [i32]>,
    pages: &mut BlockNumber,
    tuples: &mut f64,
    allvisfrac: &mut f64,
    overhead_bytes_per_tuple: usize,
    usable_bytes_per_page: usize,
) -> PgResult<()> {
    // it should have storage, so we can call the smgr
    let mut curpages: BlockNumber =
        backend_storage_buffer_bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
            rel.rd_id,
            MAIN_FORKNUM,
        )?;

    // coerce values in pg_class to more desirable types
    let relpages = rel.rd_rel.relpages as BlockNumber;
    let reltuples = rel.rd_rel.reltuples as f64;
    let relallvisible = rel.rd_rel.relallvisible as BlockNumber;

    // HACK: if the relation has never yet been vacuumed, use a minimum size
    // estimate of 10 pages. The idea here is to avoid assuming a
    // newly-created table is really small, even if it currently is, because
    // that may not be true once some data gets loaded into it. Once a vacuum
    // or analyze cycle has been done on it, it's more reasonable to believe
    // the size is somewhat stable.
    //
    // (Note that this is only an issue if the plan gets cached and used
    // again after the table has been filled. What we're trying to avoid is
    // using a nestloop-type plan on a table that has grown substantially
    // since the plan was made. Normally, autovacuum/autoanalyze will occur
    // once enough inserts have happened and cause cached-plan invalidation;
    // but that doesn't happen instantaneously, and it won't happen at all
    // for cases such as temporary tables.)
    //
    // We test "never vacuumed" by seeing whether reltuples < 0.
    //
    // If the table has inheritance children, we don't apply this heuristic.
    // Totally empty parent tables are quite common, so we should be willing
    // to believe that they are empty.
    if curpages < 10 && reltuples < 0.0 && !rel.rd_rel.relhassubclass {
        curpages = 10;
    }

    // report estimated # pages
    *pages = curpages;
    // quick exit if rel is clearly empty
    if curpages == 0 {
        *tuples = 0.0;
        *allvisfrac = 0.0;
        return Ok(());
    }

    // estimate number of tuples from previous tuple density
    let density: f64;
    if reltuples >= 0.0 && relpages > 0 {
        density = reltuples / relpages as f64;
    } else {
        // When we have no data because the relation was never yet vacuumed,
        // estimate tuple width from attribute datatypes. We assume here that
        // the pages are completely full, which is OK for tables but is
        // probably an overestimate for indexes. Fortunately
        // get_relation_info() can clamp the overestimate to the parent
        // table's size.
        //
        // Note: this code intentionally disregards alignment considerations,
        // because (a) that would be gilding the lily considering how crude
        // the estimate is, (b) it creates platform dependencies in the
        // default plans which are kind of a headache for regression testing,
        // and (c) different table AMs might use different padding schemes.

        // Without reltuples/relpages, we also need to consider fillfactor.
        // The other branch considers it implicitly by calculating density
        // from actual relpages/reltuples statistics.
        let fillfactor = rel.get_fillfactor(HEAP_DEFAULT_FILLFACTOR);

        let mut tuple_width =
            backend_optimizer_util_plancat_seams::get_rel_data_width::call(rel.rd_id, attr_widths)?;
        tuple_width = (tuple_width as usize).wrapping_add(overhead_bytes_per_tuple) as i32;
        // note: integer division is intentional here (C Size arithmetic)
        let raw = usable_bytes_per_page
            .wrapping_mul(fillfactor as usize)
            .wrapping_div(100)
            .wrapping_div(tuple_width as usize);
        // There's at least one row on the page, even with low fillfactor.
        density = backend_optimizer_path_costsize_seams::clamp_row_est::call(raw as f64);
    }
    // C uses rint(), which rounds half to even.
    *tuples = (density * curpages as f64).round_ties_even();

    // We use relallvisible as-is, rather than scaling it up like we do for
    // the pages and tuples counts, on the theory that any pages added since
    // the last VACUUM are most likely not marked all-visible. But costsize.c
    // wants it converted to a fraction.
    if relallvisible == 0 || curpages == 0 {
        *allvisfrac = 0.0;
    } else if relallvisible as f64 >= curpages as f64 {
        *allvisfrac = 1.0;
    } else {
        *allvisfrac = relallvisible as f64 / curpages as f64;
    }

    Ok(())
}

/// `HEAP_DEFAULT_FILLFACTOR` (`access/htup_details.h`).
const HEAP_DEFAULT_FILLFACTOR: i32 = 100;

#[cfg(test)]
mod tests;
