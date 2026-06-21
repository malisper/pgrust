//! `access/heap/heapam_handler.c::heapam_index_validate_scan` (+ the
//! `table_index_validate_scan` table-AM dispatch entry it backs) — the second
//! table scan of a concurrent index build (`validate_index`).
//!
//! Scan the heap under the reference snapshot and "merge join" it against the
//! sorted list of TIDs already in the index (the `state.tuplesort`, gathered by
//! `validate_index`'s bulkdelete callback). Any heap tuple whose root TID is
//! missing from the index is inserted (`index_insert`), after evaluating the
//! partial-index predicate and extracting the index key values via
//! `FormIndexDatum`. HOT-only tuples are indexed under their root TID, with a
//! per-page `in_index[]` map so we can "look back" within the current page.
//!
//! Ported faithfully from C. The `FormIndexDatum` / predicate-eval / slot setup
//! reuse the same owned-model idioms as the sibling
//! `heapam_index_build_range_scan` (see `build_scan.rs`).

use mcx::{Mcx, PgVec};

use backend_utils_error::ereport;
use types_core::primitive::BlockNumber;
use types_error::{PgResult, ERRCODE_DATA_CORRUPTED, ERROR};
use types_rel::Relation;
use types_scan::sdir::ForwardScanDirection;
use types_tuple::heaptuple::ItemPointerData;

use backend_access_heap_heapam as heapam;
use backend_executor_execExpr_seams as expr_seam;
use backend_executor_execTuples_seams as slot_seam;
use backend_executor_execUtils_seams as exec_util_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;

use types_tableam::amapi::IndexUniqueCheck;
use types_tableam::index_info_carrier::IndexInfoCarrier;

use crate::build_scan::{
    form_index_datum, heap_tuple_is_heap_only, offset_number_is_valid, offset_root, rel_name,
};

/// `BUFFER_LOCK_UNLOCK` / `BUFFER_LOCK_SHARE` (bufmgr.h).
const BUFFER_LOCK_UNLOCK: i32 = 0;
const BUFFER_LOCK_SHARE: i32 = 1;

/// `PROGRESS_SCAN_BLOCKS_TOTAL` / `PROGRESS_SCAN_BLOCKS_DONE`
/// (commands/progress.h).
const PROGRESS_SCAN_BLOCKS_TOTAL: i32 = 15;
const PROGRESS_SCAN_BLOCKS_DONE: i32 = 16;

/// `InvalidBlockNumber` (block.h).
const INVALID_BLOCK_NUMBER: BlockNumber = 0xFFFF_FFFF;

/// `itemptr_decode(itemptr, encoded)` (catalog/index.h): decode the int64
/// TID-sort representation back into an `ItemPointerData`. The encoding (see
/// `itemptr_encode` in `backend-catalog-index`) puts the block in the high 32
/// bits above a 16-bit offset.
fn itemptr_decode(encoded: i64) -> ItemPointerData {
    let block = (encoded >> 16) as BlockNumber;
    let offset = (encoded & 0xFFFF) as u16;
    let mut tid = ItemPointerData::default();
    backend_storage_page::ItemPointerSet(&mut tid, block, offset);
    tid
}

use backend_access_table_tableam_seams::ValidateScanCounters;

/// `heapam_index_validate_scan(heapRelation, indexRelation, indexInfo, snapshot,
/// state)` (heapam_handler.c). `get_next_index_tid` pulls the next encoded TID
/// from the caller's sorted tuplesort (returns `None` at end of sort); this is
/// the owned-model stand-in for the inline `tuplesort_getdatum(state->tuplesort,
/// ...)` call (the tuplesort is owned by `validate_index`, above the seam).
#[allow(clippy::too_many_arguments)]
pub fn heapam_index_validate_scan<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &mut types_nodes::execnodes::IndexInfo<'mcx>,
    snapshot: types_tableam::tableam::Snapshot,
    counters: &mut ValidateScanCounters,
    get_next_index_tid: &mut dyn FnMut() -> PgResult<Option<i64>>,
) -> PgResult<()> {
    // sanity checks
    debug_assert!(index_relation.rd_rel.relam != 0);

    // Need an EState for evaluation of index expressions and partial-index
    // predicates.  Also a slot to hold the current tuple.
    let mut estate = expr_seam::create_executor_state::call(mcx)?;
    let econtext = exec_util_seam::get_per_tuple_expr_context::call(&mut estate)?;
    // C: slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation),
    //                                    &TTSOpsHeapTuple);
    // A plain heap-tuple slot holds NO buffer pin (the scanned tuple is copied
    // into it via ExecStoreHeapTuple below). Using a buffer-pinning slot here
    // leaks one heap-page pin per validated tuple ("resource was not closed").
    let tupdesc = Some(mcx::alloc_in(mcx, heap_relation.rd_att.clone_in(mcx)?)?);
    let slot_data = slot_seam::make_single_tuple_table_slot::call(
        mcx,
        tupdesc,
        types_nodes::TupleSlotKind::HeapTuple,
    )?;
    let slot = estate.push_slot_data(slot_data)?;

    // Arrange for econtext's scan tuple to be the tuple under test.
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    // Set up execution state for predicate, if any.
    let predicate_src: Option<Vec<types_nodes::primnodes::Expr>> = index_info
        .ii_Predicate
        .as_ref()
        .map(|p| p.iter().cloned().collect());
    let mut predicate =
        expr_seam::exec_prepare_qual::call(predicate_src.as_deref(), &mut estate)?;

    // FormIndexDatum's "first time through" expression-state setup, hoisted out
    // of the per-tuple loop (mirrors build_scan).
    let mut expr_states: PgVec<'mcx, mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>> =
        if let Some(exprs) = index_info.ii_Expressions.as_ref() {
            let exprs: Vec<types_nodes::primnodes::Expr> = exprs.iter().cloned().collect();
            expr_seam::exec_prepare_expr_list::call(&exprs, &mut estate)?
        } else {
            mcx::vec_with_capacity_in(mcx, 0)?
        };

    // Prepare for scan of the base relation.  We need just those tuples
    // satisfying the passed-in reference snapshot.  We must disable syncscan
    // here, because it's critical that we read from block zero forward to match
    // the sorted TIDs. (table_beginscan_strat(rel, snapshot, 0, NULL, true,
    // false) — allow_sync = false.)
    let mut scan = backend_access_table_tableam::table_beginscan_strat(
        mcx,
        heap_relation,
        snapshot.clone(),
        0,
        mcx::vec_with_capacity_in(mcx, 0)?,
        true,
        false,
    )?;

    // pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_TOTAL, hscan->rs_nblocks).
    {
        let nblocks = heapam::scan::heap_scan_state(&mut scan).rs_nblocks;
        backend_utils_activity_small_seams::pgstat_progress_update_param::call(
            PROGRESS_SCAN_BLOCKS_TOTAL,
            nblocks as i64,
        )?;
    }

    // State variables for the merge.
    let mut indexcursor: Option<ItemPointerData> = None;
    let mut tuplesort_empty = false;

    // Per-page HOT-root state.
    let mut root_blkno = INVALID_BLOCK_NUMBER;
    let mut root_offsets: Vec<u16> = Vec::new();
    let mut in_index: Vec<bool> = Vec::new();
    let mut previous_blkno = INVALID_BLOCK_NUMBER;

    // Scan all tuples matching the snapshot.
    loop {
        let got = heapam::scan::heap_getnext(mcx, &mut scan, ForwardScanDirection)?;
        let Some(tuple_ref) = got else { break };

        let heapcursor = tuple_ref.tuple.t_self;
        let is_heap_only = heap_tuple_is_heap_only(&tuple_ref.tuple);
        let heap_tuple = tuple_ref.clone_in(mcx)?;

        // CHECK_FOR_INTERRUPTS(): no signal machinery reachable here (procsignal
        // unported); a no-op, the loop is otherwise faithful.

        counters.htups += 1.0;

        let cbuf = heapam::scan::heap_scan_state(&mut scan).rs_cbuf;
        let cblock = heapam::scan::heap_scan_state(&mut scan).rs_cblock;

        if previous_blkno == INVALID_BLOCK_NUMBER || cblock != previous_blkno {
            backend_utils_activity_small_seams::pgstat_progress_update_param::call(
                PROGRESS_SCAN_BLOCKS_DONE,
                cblock as i64,
            )?;
            previous_blkno = cblock;
        }

        // As in build_scan, index heap-only tuples under their root tuples; so
        // on reaching a new page, build a map of root item offsets on the page,
        // and clear the per-page in_index[] "look-back" map.
        if cblock != root_blkno {
            bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_SHARE)?;
            root_offsets =
                backend_access_heap_pruneheap_seams::heap_get_root_tuples::call(mcx, cbuf)?;
            bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_UNLOCK)?;

            // memset(in_index, 0, sizeof(in_index)); sized to MaxHeapTuplesPerPage
            // in C — here sized to the page's root-offset map (one slot per line
            // pointer), which is sufficient (offsets index into it below).
            in_index = alloc_zeroed_bools(root_offsets.len());

            root_blkno = cblock;
        }

        // Convert actual tuple TID to root TID.
        let mut root_tuple = heapcursor;
        let mut root_offnum = backend_storage_page::ItemPointerGetOffsetNumber(&heapcursor);

        if is_heap_only {
            root_offnum = offset_root(&root_offsets, root_offnum);
            if !offset_number_is_valid(root_offnum) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DATA_CORRUPTED)
                    .errmsg_internal(format!(
                        "failed to find parent tuple for heap-only tuple at ({},{}) in table \"{}\"",
                        backend_storage_page::ItemPointerGetBlockNumber(&heapcursor),
                        backend_storage_page::ItemPointerGetOffsetNumber(&heapcursor),
                        rel_name(heap_relation),
                    ))
                    .into_error());
            }
            backend_storage_page::ItemPointerSetOffsetNumber(&mut root_tuple, root_offnum);
        }

        // "merge" by skipping through the index tuples until we find or pass the
        // current root tuple.
        while !tuplesort_empty
            && (indexcursor.is_none()
                || backend_storage_page::ItemPointerCompare(
                    indexcursor.as_ref().unwrap(),
                    &root_tuple,
                ) < 0)
        {
            if let Some(ref ic) = indexcursor {
                // Remember index items seen earlier on the current heap page.
                if backend_storage_page::ItemPointerGetBlockNumber(ic) == root_blkno {
                    let off = backend_storage_page::ItemPointerGetOffsetNumber(ic) as usize;
                    if off >= 1 && off <= in_index.len() {
                        in_index[off - 1] = true;
                    }
                }
            }

            match get_next_index_tid()? {
                Some(encoded) => {
                    indexcursor = Some(itemptr_decode(encoded));
                }
                None => {
                    tuplesort_empty = true;
                    indexcursor = None;
                }
            }
        }

        // If the tuplesort has overshot *and* we didn't see a match earlier,
        // then this tuple is missing from the index, so insert it.
        let overshot = tuplesort_empty
            || backend_storage_page::ItemPointerCompare(
                indexcursor.as_ref().unwrap(),
                &root_tuple,
            ) > 0;
        let root_idx = root_offnum as usize;
        let already_in_index =
            root_idx >= 1 && root_idx <= in_index.len() && in_index[root_idx - 1];

        if overshot && !already_in_index {
            // MemoryContextReset(econtext->ecxt_per_tuple_memory).
            exec_util_seam::reset_expr_context::call(&mut estate, econtext)?;

            // Set up for predicate or expression evaluation: store the tuple.
            // C: ExecStoreHeapTuple(heapTuple, slot, false) — a plain
            // (non-buffer) store; `false` because the slot does not own the
            // copied tuple. This holds no buffer pin (unlike the buffer store,
            // which would leak the scanned heap page).
            let sd = estate.slot_data_mut(slot);
            slot_seam::exec_store_heap_tuple::call(heap_tuple.clone_in(mcx)?, sd, false)?;
            let _ = cbuf;

            // In a partial index, discard tuples that don't satisfy the predicate.
            if let Some(pred) = predicate.as_mut() {
                if !expr_seam::exec_qual::call(pred, econtext, &mut estate)? {
                    continue;
                }
            }

            // Extract all the attributes we use in this index, and note which
            // are null. Also evaluates any expressions needed.
            let (values, isnull) =
                form_index_datum(index_info, &mut expr_states, slot, econtext, &mut estate)?;

            // If the tuple is already committed dead, we still can't suppress
            // uniqueness checking (HOT-chain proxy); the index AM rechecks
            // liveness before declaring a uniqueness error.
            let check_unique = if index_info.ii_Unique {
                IndexUniqueCheck::UNIQUE_CHECK_YES
            } else {
                IndexUniqueCheck::UNIQUE_CHECK_NO
            };

            let n = index_info.ii_NumIndexAttrs as usize;
            {
                let mut carrier = IndexInfoCarrier::new(index_info);
                backend_access_index_indexam::index_insert(
                    mcx,
                    index_relation,
                    &values[..n],
                    &isnull[..n],
                    &root_tuple,
                    heap_relation,
                    check_unique,
                    false,
                    &mut carrier,
                )?;
            }

            counters.tups_inserted += 1.0;
        }
    }

    // table_endscan(scan).
    backend_access_table_tableam::table_endscan(scan)?;

    // ExecDropSingleTupleTableSlot(slot) / FreeExecutorState(estate): the slot
    // is owned by `estate.es_tupleTable` (push_slot_data), so freeing the
    // executor state tears down the slot too (mirrors build_scan teardown).
    expr_seam::free_executor_state::call(estate)?;

    // These may have been pointing to the now-gone estate.
    index_info.ii_ExpressionsState = None;
    index_info.ii_PredicateState = None;

    Ok(())
}

/// `memset(in_index, 0, ...)` — a freshly-zeroed per-page bool map.
fn alloc_zeroed_bools(n: usize) -> Vec<bool> {
    vec![false; n]
}

/// `table_index_validate_scan` provider entry (heap AM dispatch).
#[allow(clippy::too_many_arguments)]
pub fn provider_index_validate_scan<'mcx>(
    mcx: Mcx<'mcx>,
    table_rel: &Relation<'mcx>,
    index_rel: &Relation<'mcx>,
    index_info: &mut types_nodes::execnodes::IndexInfo<'mcx>,
    snapshot: types_tableam::tableam::Snapshot,
    counters: &mut ValidateScanCounters,
    get_next_index_tid: &mut dyn FnMut() -> PgResult<Option<i64>>,
) -> PgResult<()> {
    heapam_index_validate_scan(
        mcx,
        table_rel,
        index_rel,
        index_info,
        snapshot,
        counters,
        get_next_index_tid,
    )
}
