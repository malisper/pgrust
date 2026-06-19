//! `access/heap/heapam_handler.c::heapam_index_build_range_scan` (+ the
//! `table_index_build_scan` / `table_index_build_range_scan` table-AM dispatch
//! entry points it backs) — the serial CREATE INDEX / REINDEX heap scan that
//! every index AM's `ambuild` drives via `table_index_build_scan`.
//!
//! Scan all (or a block range of) the heap's tuples, do the index build's own
//! visibility test (`SnapshotAny` + `HeapTupleSatisfiesVacuum`, or trust the
//! MVCC snapshot for a concurrent build), evaluate the partial-index predicate,
//! extract the index key values via `FormIndexDatum`, and hand each qualifying
//! tuple (under its HOT-chain root TID) to the AM's per-tuple `callback`.
//!
//! Ported faithfully from C; the FormIndexDatum leg is inlined here (catalog/
//! index.c is unported and its `form_index_datum` seam is uninstalled + on a
//! divergent word-model Datum contract owned by the genam value-description
//! consumer; the build path needs canonical `Datum<'mcx>`, so the small
//! FormIndexDatum body — `slot_getattr` / `slot_getsysattr` /
//! `ExecEvalExprSwitchContext` — runs locally over the canonical slot).

use mcx::{Mcx, PgVec};

use backend_utils_error::ereport;
use types_core::primitive::{AttrNumber, BlockNumber};
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERROR, WARNING};
use types_rel::Relation;
use types_scan::sdir::ForwardScanDirection;
use types_snapshot::snapshot::{HTSV_Result, IsMVCCSnapshot};
use types_tableam::relscan::TableScanDescData;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{
    HeapTupleData, ItemPointerData, HEAP_HOT_UPDATED, HEAP_ONLY_TUPLE,
};

use backend_access_heap_heapam as heapam;
use backend_access_heap_heapam_visibility as visibility;
use backend_catalog_catalog_seams as catalog_seam;
use backend_executor_execExpr_seams as expr_seam;
use backend_executor_execTuples_seams as slot_seam;
use backend_executor_execUtils_seams as exec_util_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;

use types_core::fmgr::INDEX_MAX_KEYS;
use types_core::xact::{InvalidTransactionId, TransactionIdIsValid};

/// `BUFFER_LOCK_UNLOCK` / `BUFFER_LOCK_SHARE` (bufmgr.h).
const BUFFER_LOCK_UNLOCK: i32 = 0;
const BUFFER_LOCK_SHARE: i32 = 1;

/// `PROGRESS_SCAN_BLOCKS_TOTAL` / `PROGRESS_SCAN_BLOCKS_DONE`
/// (commands/progress.h).
const PROGRESS_SCAN_BLOCKS_TOTAL: i32 = 15;
const PROGRESS_SCAN_BLOCKS_DONE: i32 = 16;

/// `InvalidBlockNumber` (block.h).
const INVALID_BLOCK_NUMBER: BlockNumber = 0xFFFF_FFFF;
/// `InvalidOffsetNumber` (off.h).
const INVALID_OFFSET_NUMBER: u16 = 0;

/// `HeapTupleIsHeapOnly(tuple)` — `t_data->t_infomask2 & HEAP_ONLY_TUPLE`
/// (htup_details.h).
fn heap_tuple_is_heap_only(tuple: &HeapTupleData<'_>) -> bool {
    let hdr = tuple
        .t_data
        .as_ref()
        .expect("heap_tuple_is_heap_only: tuple has no header");
    (hdr.t_infomask2 & HEAP_ONLY_TUPLE) != 0
}

/// `HeapTupleIsHotUpdated(tuple)` — `(t_infomask2 & HEAP_HOT_UPDATED) != 0 &&
/// (t_infomask & HEAP_XMAX_INVALID) == 0 && !HeapTupleHeaderXminInvalid`
/// (htup_details.h). Matches the heapam crate's private predicate.
fn heap_tuple_is_hot_updated(tuple: &HeapTupleData<'_>) -> bool {
    let hdr = tuple
        .t_data
        .as_ref()
        .expect("heap_tuple_is_hot_updated: tuple has no header");
    (hdr.t_infomask2 & HEAP_HOT_UPDATED) != 0
        && (hdr.t_infomask & types_tuple::heaptuple::HEAP_XMAX_INVALID) == 0
        && !visibility::htup::HeapTupleHeaderXminInvalid(hdr)
}

/// `heapam_scan_get_blocks_done(hscan)` (heapam_handler.c): how many blocks the
/// scan has completed, accounting for a non-zero start block wrapping the end
/// of the relation. (Serial build: no parallel descriptor; we read the
/// scan-private start/nblocks/current fields.)
fn heapam_scan_get_blocks_done(scan: &mut TableScanDescData<'_>) -> BlockNumber {
    let hscan = heapam::scan::heap_scan_state(scan);
    let startblock = hscan.rs_startblock;
    let cblock = hscan.rs_cblock;
    let nblocks = hscan.rs_nblocks;
    if cblock > startblock {
        cblock - startblock
    } else {
        // Might have wrapped around the end of the relation.
        nblocks - startblock + cblock
    }
}

/// `FormIndexDatum(indexInfo, slot, estate, values, isnull)` (catalog/index.c),
/// inlined here over the canonical slot. Computes the index tuple's column
/// values from the slot's row, evaluating any index expressions in the estate's
/// per-tuple context. Returns the `(values, isnull)` arrays (canonical
/// `Datum<'mcx>`), exactly the inputs the AM `callback` consumes.
///
/// `econtext` must already have `ecxt_scantuple == slot`; the caller sets that.
/// `expr_states` are the compiled `ii_ExpressionsState` (built once by the
/// caller, the C "first time through" leg, hoisted out of the per-tuple call).
#[allow(clippy::type_complexity)]
fn form_index_datum<'mcx>(
    index_info: &types_nodes::execnodes::IndexInfo<'mcx>,
    expr_states: &mut PgVec<'mcx, mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>,
    slot: types_nodes::SlotId,
    econtext: types_nodes::EcxtId,
    estate: &mut types_nodes::EStateData<'mcx>,
) -> PgResult<(PgVec<'mcx, Datum<'mcx>>, [bool; INDEX_MAX_KEYS as usize])> {
    let mcx = estate.es_query_cxt;

    let n = index_info.ii_NumIndexAttrs as usize;
    let mut values: PgVec<'mcx, Datum<'mcx>> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut isnull = [false; INDEX_MAX_KEYS as usize];

    // Index into the prepared expression states as we consume expr columns.
    let mut indexpr_item: usize = 0;
    let num_states = expr_states.len();

    for i in 0..n {
        let keycol: AttrNumber = index_info.ii_IndexAttrNumbers[i];
        if keycol < 0 {
            // System column: slot_getsysattr against the slot's stored tuple.
            let sd = estate.slot_data_mut(slot);
            let (d, is_null) = slot_seam::slot_getsysattr::call(mcx, sd, keycol)?;
            values.push(d);
            isnull[i] = is_null;
        } else if keycol != 0 {
            // Plain index column; get the value directly from the heap tuple.
            let (d, is_null) = slot_seam::slot_getattr::call(estate, slot, keycol)?;
            values.push(d);
            isnull[i] = is_null;
        } else {
            // Index expression --- need to evaluate it.
            if indexpr_item >= num_states {
                return Err(PgError::error("wrong number of index expressions"));
            }
            let state: &mut types_nodes::execexpr::ExprState<'mcx> =
                &mut expr_states[indexpr_item];
            let (d, is_null) =
                expr_seam::exec_eval_expr_switch_context::call(state, econtext, estate)?;
            values.push(d);
            isnull[i] = is_null;
            indexpr_item += 1;
        }
    }

    if indexpr_item != num_states {
        return Err(PgError::error("wrong number of index expressions"));
    }

    Ok((values, isnull))
}

/// Per-tuple action of the visibility branch: whether to index the tuple and
/// whether it counts as "alive" for uniqueness checking / reltuples.
struct IndexDecision {
    index_it: bool,
    tuple_is_alive: bool,
    /// Add 1 to reltuples for this tuple.
    count_live: bool,
    /// The caller must drop the buffer lock, `XactLockTableWait(xwait)`, and
    /// recheck visibility (the C `goto recheck`).
    wait_xwait: Option<types_core::TransactionId>,
}

/// `heapam_index_build_range_scan(...)` (heapam_handler.c) — the serial /
/// parallel-from-scan index build heap scan.
///
/// `scan` is always `None` here (serial build path): the provider begins its
/// own heap scan. The parallel-build leg (`scan != NULL`) is not reachable
/// through the current `table_index_build_scan` consumers; parallel index
/// builds (which would pass an existing parallel `scan`) are unported.
#[allow(clippy::too_many_arguments)]
pub fn heapam_index_build_range_scan<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    // `indexRelation` is only passed to the C `callback`; the owned-model
    // callback closure already captures the index relation, so the provider
    // never forwards it.
    _index_relation: &Relation<'mcx>,
    index_info: &mut types_nodes::execnodes::IndexInfo<'mcx>,
    allow_sync: bool,
    anyvisible: bool,
    progress: bool,
    start_blockno: BlockNumber,
    numblocks: BlockNumber,
    callback: &mut dyn FnMut(ItemPointerData, &[Datum<'mcx>], &[bool], bool) -> PgResult<()>,
) -> PgResult<f64> {
    // sanity checks
    debug_assert!(heap_relation.rd_rel.relam != 0);

    // Remember if it's a system catalog.
    let is_system_catalog = catalog_seam::is_system_relation::call(heap_relation)?;

    // See whether we're verifying uniqueness/exclusion properties.
    let checking_uniqueness = index_info.ii_Unique || index_info.ii_ExclusionOps.is_some();

    // "Any visible" mode is not compatible with uniqueness checks.
    debug_assert!(!(anyvisible && checking_uniqueness));

    // Need an EState for evaluation of index expressions and partial-index
    // predicates.  Also a slot to hold the current tuple.
    let mut estate = expr_seam::create_executor_state::call(mcx)?;
    let econtext = exec_util_seam::get_per_tuple_expr_context::call(&mut estate)?;
    let slot_data = backend_access_table_tableam::table_slot_create(mcx, heap_relation)?;
    let slot = estate.push_slot_data(slot_data)?;

    // Arrange for econtext's scan tuple to be the tuple under test.
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    // Set up execution state for predicate, if any.
    let predicate_src: Option<Vec<types_nodes::primnodes::Expr>> = index_info
        .ii_Predicate
        .as_ref()
        .map(|p| p.iter().cloned().collect());
    let mut predicate = expr_seam::exec_prepare_qual::call(
        predicate_src.as_deref(),
        &mut estate,
    )?;

    // FormIndexDatum's "first time through" expression-state setup
    // (ii_ExpressionsState = ExecPrepareExprList(ii_Expressions, estate)),
    // hoisted out of the per-tuple loop. `ii_ExpressionsState` is nulled at
    // teardown (it would point into the now-gone estate), so we keep the
    // compiled states in this local across the scan instead.
    let mut expr_states: PgVec<'mcx, mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>> =
        if let Some(exprs) = index_info.ii_Expressions.as_ref() {
            let exprs: Vec<types_nodes::primnodes::Expr> = exprs.iter().cloned().collect();
            expr_seam::exec_prepare_expr_list::call(&exprs, &mut estate)?
        } else {
            mcx::vec_with_capacity_in(mcx, 0)?
        };

    // Prepare for scan of the base relation.  In a normal index build we use
    // SnapshotAny (None) because we must retrieve all tuples and do our own
    // time-qual checks. In a concurrent build, or during bootstrap, we take a
    // regular MVCC snapshot and index whatever's live according to that.
    let mut oldest_xmin = InvalidTransactionId;

    // okay to ignore lazy VACUUMs here.
    if !backend_utils_init_miscinit::IsBootstrapProcessingMode() && !index_info.ii_Concurrent {
        oldest_xmin = backend_storage_ipc_procarray_seams::get_oldest_non_removable_transaction_id::call(
            heap_relation.rd_id,
        )?;
    }

    // Serial index build: begin our own heap scan, registering a snapshot whose
    // lifetime is under our direct control if we use an MVCC snapshot.
    let mut need_unregister_snapshot = false;
    let snapshot: types_tableam::tableam::Snapshot = if !TransactionIdIsValid(oldest_xmin) {
        let snap = backend_utils_time_snapmgr_seams::get_transaction_snapshot::call()?;
        let snap = backend_utils_time_snapmgr_seams::register_snapshot::call(snap)?;
        need_unregister_snapshot = true;
        Some(snap)
    } else {
        // SnapshotAny.
        None
    };

    // table_beginscan_strat(heapRelation, snapshot, 0, NULL, true, allow_sync).
    let mut scan = backend_access_table_tableam::table_beginscan_strat(
        mcx,
        heap_relation,
        snapshot.clone(),
        0,
        mcx::vec_with_capacity_in(mcx, 0)?,
        true,
        allow_sync,
    )?;

    // snapshot == SnapshotAny <=> rs_snapshot is None. Must have called
    // GetOldestNonRemovableTransactionId() iff using SnapshotAny.
    let is_snapshot_any = snapshot.is_none();
    debug_assert!(is_snapshot_any || matches!(snapshot.as_ref(), Some(s) if IsMVCCSnapshot(s)));

    // Publish number of blocks to scan.
    if progress {
        let nblocks = heapam::scan::heap_scan_state(&mut scan).rs_nblocks;
        backend_utils_activity_small_seams::pgstat_progress_update_param::call(
            PROGRESS_SCAN_BLOCKS_TOTAL,
            nblocks as i64,
        )?;
    }

    // Set our scan endpoints.
    if !allow_sync {
        heapam::scan::heap_setscanlimits(&mut scan, start_blockno, numblocks);
    } else {
        // syncscan can only be requested on whole relation.
        debug_assert_eq!(start_blockno, 0);
        debug_assert_eq!(numblocks, INVALID_BLOCK_NUMBER);
    }

    let mut reltuples: f64 = 0.0;
    let mut previous_blkno = INVALID_BLOCK_NUMBER;
    let mut root_blkno = INVALID_BLOCK_NUMBER;
    let mut root_offsets: Vec<u16> = Vec::new();

    // Scan all tuples in the base relation.
    loop {
        // heap_getnext returns a borrow of the scan's rs_ctup; copy out what we
        // need and clone the tuple so the scan is free to be mutated below.
        let got = heapam::scan::heap_getnext(mcx, &mut scan, ForwardScanDirection)?;
        let Some(tuple_ref) = got else { break };

        let t_self = tuple_ref.tuple.t_self;
        let is_heap_only = heap_tuple_is_heap_only(&tuple_ref.tuple);
        let is_hot_updated = heap_tuple_is_hot_updated(&tuple_ref.tuple);
        let mut heap_tuple = tuple_ref.clone_in(mcx)?;

        // CHECK_FOR_INTERRUPTS(): cooperative-cancellation point; the owned
        // model has no signal machinery reachable here (procsignal unported),
        // so it is a no-op (mirror-PG: the loop is otherwise faithful).

        let cbuf = heapam::scan::heap_scan_state(&mut scan).rs_cbuf;
        let cblock = heapam::scan::heap_scan_state(&mut scan).rs_cblock;

        // Report scan progress, if asked to.
        if progress {
            let blocks_done = heapam_scan_get_blocks_done(&mut scan);
            if blocks_done != previous_blkno {
                backend_utils_activity_small_seams::pgstat_progress_update_param::call(
                    PROGRESS_SCAN_BLOCKS_DONE,
                    blocks_done as i64,
                )?;
                previous_blkno = blocks_done;
            }
        }

        // On reaching a new page, (re)build the HOT-chain root-offset map.
        if cblock != root_blkno {
            bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_SHARE)?;
            root_offsets = backend_access_heap_pruneheap_seams::heap_get_root_tuples::call(mcx, cbuf)?;
            bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_UNLOCK)?;
            root_blkno = cblock;
        }

        let tuple_is_alive;
        if is_snapshot_any {
            // Do our own time-qual check, with the C `recheck:` retry loop for
            // concurrent in-progress insert/delete under uniqueness checking.
            let decision = loop {
                bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_SHARE)?;
                let htsv = visibility::HeapTupleSatisfiesVacuum(
                    &mut heap_tuple.tuple,
                    oldest_xmin,
                    cbuf,
                )?;
                let dec = classify_vacuum_result(
                    htsv,
                    &heap_tuple.tuple,
                    anyvisible,
                    checking_uniqueness,
                    is_hot_updated,
                    is_system_catalog,
                    heap_relation,
                    index_info,
                )?;
                if let Some(xwait) = dec.wait_xwait {
                    // Must drop the lock on the buffer before we wait.
                    bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_UNLOCK)?;
                    backend_storage_lmgr_lmgr_seams::xact_lock_table_wait::call(
                        xwait,
                        rel_name(heap_relation),
                        t_self,
                        types_storage::lock::XLTW_Oper::InsertIndexUnique,
                    )?;
                    // goto recheck.
                    continue;
                }
                break dec;
            };
            bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_UNLOCK)?;

            if decision.count_live {
                reltuples += 1.0;
            }
            if !decision.index_it {
                continue;
            }
            tuple_is_alive = decision.tuple_is_alive;
        } else {
            // heap_getnext did the time-qual check.
            tuple_is_alive = true;
            reltuples += 1.0;
        }

        // MemoryContextReset(econtext->ecxt_per_tuple_memory).
        exec_util_seam::reset_expr_context::call(&mut estate, econtext)?;

        // Set up for predicate or expression evaluation: store the tuple in the
        // scan slot.
        let sd = estate.slot_data_mut(slot);
        slot_seam::exec_store_buffer_heap_tuple::call(heap_tuple.clone_in(mcx)?, sd, cbuf)?;

        // In a partial index, discard tuples that don't satisfy the predicate.
        if let Some(pred) = predicate.as_mut() {
            if !expr_seam::exec_qual::call(pred, econtext, &mut estate)? {
                continue;
            }
        }

        // Extract all the attributes we use in this index, and note which are
        // null. Also evaluates any expressions needed.
        let (values, isnull) =
            form_index_datum(index_info, &mut expr_states, slot, econtext, &mut estate)?;

        if is_heap_only {
            // For a heap-only tuple, pretend its TID is that of the root.
            let offnum = backend_storage_page::ItemPointerGetOffsetNumber(&t_self);

            // If a HOT tuple points to a root we don't know about, obtain root
            // items afresh; if that still fails, report corruption.
            if offset_root(&root_offsets, offnum) == INVALID_OFFSET_NUMBER {
                bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_SHARE)?;
                root_offsets =
                    backend_access_heap_pruneheap_seams::heap_get_root_tuples::call(mcx, cbuf)?;
                bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_UNLOCK)?;
            }

            let root_off = offset_root(&root_offsets, offnum);
            if !offset_number_is_valid(root_off) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DATA_CORRUPTED)
                    .errmsg_internal(format!(
                        "failed to find parent tuple for heap-only tuple at ({},{}) in table \"{}\"",
                        backend_storage_page::ItemPointerGetBlockNumber(&t_self),
                        offnum,
                        rel_name(heap_relation),
                    ))
                    .into_error());
            }

            let mut tid = ItemPointerData::default();
            backend_storage_page::ItemPointerSet(
                &mut tid,
                backend_storage_page::ItemPointerGetBlockNumber(&t_self),
                root_off,
            );
            callback(tid, &values, &isnull[..index_info.ii_NumIndexAttrs as usize], tuple_is_alive)?;
        } else {
            callback(
                t_self,
                &values,
                &isnull[..index_info.ii_NumIndexAttrs as usize],
                tuple_is_alive,
            )?;
        }
    }

    // Report scan progress one last time.
    if progress {
        let blks_done = heapam::scan::heap_scan_state(&mut scan).rs_nblocks;
        backend_utils_activity_small_seams::pgstat_progress_update_param::call(
            PROGRESS_SCAN_BLOCKS_DONE,
            blks_done as i64,
        )?;
    }

    // table_endscan(scan).
    backend_access_table_tableam::table_endscan(scan)?;

    // We can now forget our snapshot, if set and registered by us.
    if need_unregister_snapshot {
        if let Some(snap) = snapshot {
            backend_utils_time_snapmgr_seams::unregister_snapshot::call(snap);
        }
    }

    // ExecDropSingleTupleTableSlot(slot) (heapam_handler.c). C's
    // table_slot_create made a STANDALONE TTSOpsBufferHeapTuple slot, and the
    // last ExecStoreBufferHeapTuple in the loop left a *buffer pin* on the final
    // heap page (taken via IncrBufferRefCount, living in the bufmgr private
    // refcount + the current ResourceOwner — NOT in the owned SlotData value).
    // ExecDropSingleTupleTableSlot calls ExecClearTuple first, whose
    // tts_buffer_heap_clear runs ReleaseBuffer(bslot->buffer); dropping the slot
    // (or freeing the estate) alone does NOT release that pin. Skipping the clear
    // leaked one buffer pin per CREATE INDEX / REINDEX heap scan ("resource was
    // not closed" at statement end). Clear the slot to release the pin, matching C.
    slot_seam::exec_clear_tuple_by_id::call(&mut estate, slot)?;

    // FreeExecutorState(estate).
    expr_seam::free_executor_state::call(estate)?;

    // These may have been pointing to the now-gone estate.
    index_info.ii_ExpressionsState = None;
    index_info.ii_PredicateState = None;
    drop(expr_states);
    drop(predicate);

    Ok(reltuples)
}

/// Decide what to do with a tuple given its `HeapTupleSatisfiesVacuum` result,
/// matching analyze.c's `heapam_scan_analyze_next_tuple` counting so CREATE
/// INDEX and ANALYZE agree on reltuples.
#[allow(clippy::too_many_arguments)]
fn classify_vacuum_result<'mcx>(
    htsv: HTSV_Result,
    tuple: &HeapTupleData<'mcx>,
    anyvisible: bool,
    checking_uniqueness: bool,
    is_hot_updated: bool,
    is_system_catalog: bool,
    heap_relation: &Relation<'mcx>,
    index_info: &mut types_nodes::execnodes::IndexInfo<'mcx>,
) -> PgResult<IndexDecision> {
    use types_snapshot::snapshot::HTSV_Result::*;

    let hdr = tuple
        .t_data
        .as_ref()
        .expect("classify_vacuum_result: tuple has no header");

    Ok(match htsv {
        HEAPTUPLE_DEAD => IndexDecision {
            index_it: false,
            tuple_is_alive: false,
            count_live: false,
            wait_xwait: None,
        },
        HEAPTUPLE_LIVE => IndexDecision {
            index_it: true,
            tuple_is_alive: true,
            count_live: true,
            wait_xwait: None,
        },
        HEAPTUPLE_RECENTLY_DEAD => {
            // Index it anyway to preserve MVCC semantics, unless it was
            // HOT-updated (then only index the live tuple at the chain end, and
            // mark the index unusable for old snapshots).
            let index_it = if is_hot_updated {
                index_info.ii_BrokenHotChain = true;
                false
            } else {
                true
            };
            IndexDecision {
                index_it,
                tuple_is_alive: false,
                count_live: false,
                wait_xwait: None,
            }
        }
        HEAPTUPLE_INSERT_IN_PROGRESS => {
            if anyvisible {
                return Ok(IndexDecision {
                    index_it: true,
                    tuple_is_alive: true,
                    count_live: true,
                    wait_xwait: None,
                });
            }
            let xwait = visibility::htup::HeapTupleHeaderGetXmin(hdr);
            let mut count_live = false;
            if !backend_access_transam_xact_seams::transaction_id_is_current_transaction_id::call(xwait)
            {
                if !is_system_catalog {
                    // elog(WARNING, "concurrent insert in progress ...")
                    ereport_warning_concurrent_insert(heap_relation)?;
                }
                if checking_uniqueness {
                    return Ok(IndexDecision {
                        index_it: false,
                        tuple_is_alive: false,
                        count_live: false,
                        wait_xwait: Some(xwait),
                    });
                }
            } else {
                // Count INSERT_IN_PROGRESS as live only when inserted by our own
                // transaction (consistent with analyze.c).
                count_live = true;
            }
            // We must index such tuples (good if the build commits).
            IndexDecision {
                index_it: true,
                tuple_is_alive: true,
                count_live,
                wait_xwait: None,
            }
        }
        HEAPTUPLE_DELETE_IN_PROGRESS => {
            if anyvisible {
                return Ok(IndexDecision {
                    index_it: true,
                    tuple_is_alive: false,
                    count_live: true,
                    wait_xwait: None,
                });
            }
            let xwait = visibility::HeapTupleHeaderGetUpdateXid(hdr)?;
            if !backend_access_transam_xact_seams::transaction_id_is_current_transaction_id::call(xwait)
            {
                if !is_system_catalog {
                    // elog(WARNING, "concurrent delete in progress ...")
                    ereport_warning_concurrent_delete(heap_relation)?;
                }
                if checking_uniqueness || is_hot_updated {
                    return Ok(IndexDecision {
                        index_it: false,
                        tuple_is_alive: false,
                        count_live: false,
                        wait_xwait: Some(xwait),
                    });
                }
                // Otherwise index it but don't check uniqueness; count as live
                // (deleted by another, possibly-aborting xact).
                IndexDecision {
                    index_it: true,
                    tuple_is_alive: false,
                    count_live: true,
                    wait_xwait: None,
                }
            } else if is_hot_updated {
                // HOT-updated tuple deleted by our own xact — treat as
                // RECENTLY_DEAD HOT-updated.
                index_info.ii_BrokenHotChain = true;
                IndexDecision {
                    index_it: false,
                    tuple_is_alive: false,
                    count_live: false,
                    wait_xwait: None,
                }
            } else {
                // Regular tuple deleted by our own xact: index it, no uniqueness
                // check, not counted in reltuples.
                IndexDecision {
                    index_it: true,
                    tuple_is_alive: false,
                    count_live: false,
                    wait_xwait: None,
                }
            }
        }
    })
}

/// `elog(WARNING, "concurrent insert in progress within table \"%s\"")`.
fn ereport_warning_concurrent_insert(rel: &Relation<'_>) -> PgResult<()> {
    ereport(WARNING)
        .errmsg_internal(format!(
            "concurrent insert in progress within table \"{}\"",
            rel_name(rel)
        ))
        .finish(ErrorLocation::new(
            "heapam_handler.c",
            1483,
            "heapam_index_build_range_scan",
        ))
}

/// `elog(WARNING, "concurrent delete in progress within table \"%s\"")`.
fn ereport_warning_concurrent_delete(rel: &Relation<'_>) -> PgResult<()> {
    ereport(WARNING)
        .errmsg_internal(format!(
            "concurrent delete in progress within table \"{}\"",
            rel_name(rel)
        ))
        .finish(ErrorLocation::new(
            "heapam_handler.c",
            1541,
            "heapam_index_build_range_scan",
        ))
}

/// `RelationGetRelationName(rel)`.
fn rel_name(rel: &Relation<'_>) -> String {
    rel.rd_rel.relname.to_string()
}

/// `root_offsets[offnum - 1]`, or `InvalidOffsetNumber` if the offset is out of
/// the (re)built map.
fn offset_root(root_offsets: &[u16], offnum: u16) -> u16 {
    let idx = offnum as usize;
    if idx == 0 || idx > root_offsets.len() {
        INVALID_OFFSET_NUMBER
    } else {
        root_offsets[idx - 1]
    }
}

/// `OffsetNumberIsValid(offnum)` (off.h).
fn offset_number_is_valid(offnum: u16) -> bool {
    offnum != INVALID_OFFSET_NUMBER
}

/// `table_index_build_range_scan` provider entry (heap AM dispatch).
#[allow(clippy::too_many_arguments)]
pub fn provider_index_build_range_scan<'mcx>(
    mcx: Mcx<'mcx>,
    table_rel: &Relation<'mcx>,
    index_rel: &Relation<'mcx>,
    index_info: &mut types_nodes::execnodes::IndexInfo<'mcx>,
    allow_sync: bool,
    anyvisible: bool,
    progress: bool,
    start_blockno: BlockNumber,
    numblocks: BlockNumber,
    callback: &mut dyn FnMut(ItemPointerData, &[Datum<'mcx>], &[bool], bool) -> PgResult<()>,
) -> PgResult<f64> {
    heapam_index_build_range_scan(
        mcx,
        table_rel,
        index_rel,
        index_info,
        allow_sync,
        anyvisible,
        progress,
        start_blockno,
        numblocks,
        callback,
    )
}

/// `table_index_build_scan` provider entry (heap AM dispatch). The C inline
/// `table_index_build_scan` forwards to `index_build_range_scan` over the whole
/// relation: `anyvisible = false`, `start_blockno = 0`, `numblocks =
/// InvalidBlockNumber` (tableam.h).
#[allow(clippy::too_many_arguments)]
pub fn provider_index_build_scan<'mcx>(
    mcx: Mcx<'mcx>,
    table_rel: &Relation<'mcx>,
    index_rel: &Relation<'mcx>,
    index_info: &mut types_nodes::execnodes::IndexInfo<'mcx>,
    allow_sync: bool,
    progress: bool,
    callback: &mut dyn FnMut(ItemPointerData, &[Datum<'mcx>], &[bool], bool) -> PgResult<()>,
) -> PgResult<f64> {
    heapam_index_build_range_scan(
        mcx,
        table_rel,
        index_rel,
        index_info,
        allow_sync,
        false,
        progress,
        0,
        INVALID_BLOCK_NUMBER,
        callback,
    )
}

