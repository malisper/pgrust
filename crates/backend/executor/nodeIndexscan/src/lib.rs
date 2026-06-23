//! Port of `src/backend/executor/nodeIndexscan.c` — routines to support indexed
//! scans of relations.
//!
//! INTERFACE ROUTINES
//! - [`ExecIndexScan`]           - scans a relation using an index.
//! - [`IndexNext`]               - retrieve next tuple using the index.
//! - [`IndexNextWithReorder`]    - same, but recheck ORDER BY expressions.
//! - [`ExecInitIndexScan`]       - creates and initializes state info.
//! - [`ExecReScanIndexScan`]     - rescans the indexed relation.
//! - [`ExecEndIndexScan`]        - releases all storage.
//! - [`ExecIndexMarkPos`] / [`ExecIndexRestrPos`] - mark/restore.
//! - the five `ExecIndexScan*` parallel entry points.
//!
//! This unit also owns the *shared* index-scan-key machinery used by every
//! index-scan node (plain, index-only, bitmap):
//! [`ExecIndexBuildScanKeys`], [`ExecIndexEvalRuntimeKeys`],
//! [`ExecIndexEvalArrayKeys`], [`ExecIndexAdvanceArrayKeys`]. The
//! index-only/bitmap nodes reach them through the
//! `backend-executor-nodeIndexscan-seams` adapters this crate installs.
//!
//! The node's own state machine — the [`IndexNext`]/[`IndexNextWithReorder`]
//! scan loops, the reorder pairing heap (`cmp_orderbyvals` / `reorderqueue_*`),
//! the lossy ORDER BY recheck (`EvalOrderByExpressions`), the
//! [`IndexRecheck`] EvalPlanQual access method, and the
//! init/rescan/teardown/parallel control flow — is this crate's owned logic.
//! The `execScan.c` driver (`ExecScan`) is a separate translation unit; this
//! node delegates to it through the execScan seam, passing its own
//! `IndexNext`/`IndexNextWithReorder` access methods + `IndexRecheck`.
//! Operations below the executor-node layer go through their owners' seam
//! crates: the generic index AM (indexam), expression eval (execExpr), slots
//! (execTuples), the execUtils/execScan init helpers, scan-key initialization
//! (scankey), the catalog lookups (lsyscache / relcache / nodeFuncs), array
//! deconstruction (arrayfuncs), datum copy (datum), sort support (sortsupport),
//! and the DSM/parallel-shm plumbing (shm_toc/parallel).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use scankey as scankey_owner;
use indexam_seams as indexam;
use transam_parallel as parallel;
use execAmi_seams as execAmi;
use execExpr_seams as execExpr;
use execScan_seams as execScan;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use nodeFuncs_seams as nodeFuncs;
use bufmgr_seams as _bufmgr;
use storage_shm_toc_seams as shm_toc;
use postgres_seams as tcop_postgres;
use arrayfuncs_seams as arrayfuncs;
use datum_seams as datum_seams;
use lsyscache_seams as lsyscache;
use relcache_seams as relcache;
use sortsupport_seams as sortsupport;

use mcx::{Mcx, PgBox, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid};
use types_error::{PgError, PgResult};
use execparallel::{ParallelContextHandle, ParallelWorkerContextHandle};
use ::nodes::nodeindexonlyscan::{
    IndexRuntimeKeyInfo, IndexScanInstrumentation, IndexScanState, ParallelIndexScanDescHandle,
    SharedIndexScanInstrumentation,
};
use ::nodes::nodebitmapindexscan::IndexArrayKeyInfo;
use ::nodes::nodeindexscan::IndexScan;
use ::nodes::primnodes::Expr;
use nodes::{EStateData, EcxtId, PlanStateData, SlotId};
use types_scan::scankey::{
    ScanKeyData, StrategyNumber, InvalidStrategy, SK_ISNULL, SK_ORDER_BY, SK_ROW_END, SK_ROW_HEADER,
    SK_ROW_MEMBER, SK_SEARCHARRAY, SK_SEARCHNOTNULL, SK_SEARCHNULL,
};
use types_scan::sdir::ScanDirection;
use types_sortsupport::SortSupportData;
use types_tuple::heaptuple::Datum;

/// `EXEC_FLAG_EXPLAIN_ONLY` (executor/executor.h) — "EXPLAIN, no ANALYZE".
pub const EXEC_FLAG_EXPLAIN_ONLY: i32 = 0x0001;

/// `INDEX_VAR` (nodes/primnodes.h) — varno of Vars referencing the index's
/// scan tuple.
const INDEX_VAR: i32 = -3;

/// `BTORDER_PROC` (access/nbtree.h) — the btree comparison support proc.
const BTORDER_PROC: i16 = 1;

/// `elog(ERROR, msg)` — plain internal error.
fn elog(message: &'static str) -> PgError {
    PgError::error(message)
}

// ===========================================================================
// IndexNext — the ordinary (non-reorder) scan access method.
// ===========================================================================

/// `IndexNext(node)` — retrieve a tuple from the IndexScan node's
/// currentRelation using the index. Returns `Ok(true)` if a tuple is available
/// in `node.ss.ss_ScanTupleSlot`, `Ok(false)` at end of scan.
fn IndexNext<'mcx>(node: &mut IndexScanState<'mcx>, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
    // direction = ScanDirectionCombine(estate->es_direction, indexorderdir);
    let plan_dir = plan_indexorderdir(node)?;
    let direction = scan_direction_combine(estate.es_direction, plan_dir);

    let scan_slot = node
        .ss
        .ss_ScanTupleSlot
        .ok_or_else(|| elog("index scan has no scan tuple slot"))?;
    let econtext = node
        .ss
        .ps
        .ps_ExprContext
        .ok_or_else(|| elog("index scan has no expr context"))?;

    if node.iss_ScanDesc.is_none() {
        begin_scan_serial(node, estate)?;
    }

    // while (index_getnext_slot(scandesc, direction, slot))
    loop {
        let found = {
            let scandesc = node.iss_ScanDesc.as_mut().unwrap();
            indexam::index_getnext_slot::call(scandesc, direction, estate, scan_slot)?
        };
        // Keep iss_Instrument current: C aliases scan->instrument to
        // &iss_Instrument so the AM's per-_bt_first nsearches bump is live; the
        // owned port passes it by value, so mirror it back after each fetch
        // (EXPLAIN ANALYZE reads iss_Instrument before ExecutorEnd runs).
        sync_index_instrument(node);
        if !found {
            break;
        }

        tcop_postgres::check_for_interrupts::call()?;

        // If the index was lossy, recheck the index quals using the fetched tuple.
        if node.iss_ScanDesc.as_ref().unwrap().xs_recheck {
            // econtext->ecxt_scantuple = slot;
            // if (!ExecQualAndReset(node->indexqualorig, econtext)) { filtered; continue }
            estate.ecxt_mut(econtext).ecxt_scantuple = Some(scan_slot);
            let passed = exec_qual_and_reset(node.indexqualorig.as_deref_mut(), econtext, estate)?;
            if !passed {
                instr_count_filtered2(node, 1);
                continue;
            }
        }

        return Ok(true);
    }

    // End of scan.
    node.iss_ReachedEnd = true;
    execTuples::exec_clear_tuple::call(estate, scan_slot)?;
    Ok(false)
}

/// Mirror the scan descriptor's AM-updated `instrument.nsearches` back into
/// `node.iss_Instrument`. C aliases `scan->instrument` to `&iss_Instrument`, so
/// the bump inside `_bt_first` is directly visible; the owned port passes the
/// counter by value into `index_beginscan`, so we copy it back. EXPLAIN ANALYZE
/// reads `iss_Instrument` during `ExplainPrintPlan`, which runs before
/// `ExecutorEnd`, so the sync must happen while the scan is live.
fn sync_index_instrument<'mcx>(node: &mut IndexScanState<'mcx>) {
    if let Some(scandesc) = node.iss_ScanDesc.as_ref() {
        if let Some(instr) = scandesc.instrument.as_ref() {
            node.iss_Instrument.nsearches = instr.nsearches;
        }
    }
}

// ===========================================================================
// IndexNextWithReorder — like IndexNext, but reorders ORDER BY results.
// ===========================================================================

/// `IndexNextWithReorder(node)` — like [`IndexNext`], but rechecks ORDER BY
/// expressions and reorders the tuples through a pairing heap as necessary.
fn IndexNextWithReorder<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // Only forward scan is supported with reordering (the planner guarantees
    // this; C Asserts it).
    debug_assert!(!matches!(
        plan_indexorderdir(node)?,
        ScanDirection::BackwardScanDirection
    ));
    debug_assert!(matches!(
        estate.es_direction,
        ScanDirection::ForwardScanDirection
    ));

    let scan_slot = node
        .ss
        .ss_ScanTupleSlot
        .ok_or_else(|| elog("index scan has no scan tuple slot"))?;
    let econtext = node
        .ss
        .ps
        .ps_ExprContext
        .ok_or_else(|| elog("index scan has no expr context"))?;

    if node.iss_ScanDesc.is_none() {
        begin_scan_serial(node, estate)?;
    }

    loop {
        tcop_postgres::check_for_interrupts::call()?;

        // Check the reorder queue first: if its topmost tuple has an ORDER BY
        // value <= the last value returned by the index, return it now.
        if !reorder_queue_is_empty(node) {
            let top_idx = reorder_queue_first_idx(node)?
                .ok_or_else(|| elog("reorder queue unexpectedly empty"))?;
            let return_topmost = {
                if node.iss_ReachedEnd {
                    true
                } else {
                    let q = node.iss_ReorderQueue.as_ref().unwrap();
                    let top = &q[top_idx];
                    let scandesc = node.iss_ScanDesc.as_ref().unwrap();
                    cmp_orderbyvals(
                        &top.orderbyvals,
                        &top.orderbynulls,
                        &scandesc.xs_orderbyvals,
                        &scandesc.xs_orderbynulls,
                        node,
                    )? <= 0
                }
            };

            if return_topmost {
                let tuple = reorderqueue_pop(node, top_idx)?;
                // Pass 'true', as the tuple in the queue is a palloc'd copy.
                // ExecForceStoreHeapTuple(rt->htup, slot, true): the reorder
                // queue holds the full data-bearing FormedTuple, so route through
                // the formed-tuple store seam (the user-data area is required to
                // deform a virtual/minimal target slot).
                execTuples::exec_force_store_formed_heap_tuple::call(
                    estate,
                    scan_slot,
                    tuple.tuple,
                    true,
                )?;
                return Ok(true);
            }
        } else if node.iss_ReachedEnd {
            // Queue empty and no more index tuples — done.
            execTuples::exec_clear_tuple::call(estate, scan_slot)?;
            return Ok(false);
        }

        // Fetch next tuple from the index (always forward for reorder scans).
        let found = {
            let scandesc = node.iss_ScanDesc.as_mut().unwrap();
            indexam::index_getnext_slot::call(
                scandesc,
                ScanDirection::ForwardScanDirection,
                estate,
                scan_slot,
            )?
        };
        if !found {
            // No more index tuples; drain the queue before finishing.
            node.iss_ReachedEnd = true;
            continue;
        }

        // If the index was lossy, recheck the index quals and ORDER BY exprs.
        if node.iss_ScanDesc.as_ref().unwrap().xs_recheck {
            // econtext->ecxt_scantuple = slot;
            estate.ecxt_mut(econtext).ecxt_scantuple = Some(scan_slot);
            let passed = exec_qual_and_reset(node.indexqualorig.as_deref_mut(), econtext, estate)?;
            if !passed {
                instr_count_filtered2(node, 1);
                tcop_postgres::check_for_interrupts::call()?;
                continue;
            }
        }

        let was_exact;
        // lastfetched is sourced either from our recomputed values or the index's.
        let use_recomputed;
        if node.iss_ScanDesc.as_ref().unwrap().xs_recheckorderby {
            // econtext->ecxt_scantuple = slot; ResetExprContext; EvalOrderByExpressions.
            estate.ecxt_mut(econtext).ecxt_scantuple = Some(scan_slot);
            execUtils::reset_expr_context::call(estate, econtext)?;
            EvalOrderByExpressions(node, econtext, scan_slot, estate)?;

            // Compare our recomputed values against the index's returned values.
            let cmp = {
                let scandesc = node.iss_ScanDesc.as_ref().unwrap();
                cmp_orderbyvals(
                    &node.iss_OrderByValues,
                    &node.iss_OrderByNulls,
                    &scandesc.xs_orderbyvals,
                    &scandesc.xs_orderbynulls,
                    node,
                )?
            };
            if cmp < 0 {
                return Err(elog("index returned tuples in wrong order"));
            }
            was_exact = cmp == 0;
            use_recomputed = true;
        } else {
            was_exact = true;
            use_recomputed = false;
        }

        // Decide: return immediately, or push to the reorder queue. We must push
        // if our values were inaccurate, or if a smaller tuple is already queued.
        let must_push = if !was_exact {
            true
        } else if !reorder_queue_is_empty(node) {
            let lastfetched_vals;
            let lastfetched_nulls;
            if use_recomputed {
                lastfetched_vals = clone_datum_vec(&node.iss_OrderByValues);
                lastfetched_nulls = node.iss_OrderByNulls.to_vec();
            } else {
                let scandesc = node.iss_ScanDesc.as_ref().unwrap();
                lastfetched_vals = clone_datum_vec(&scandesc.xs_orderbyvals);
                lastfetched_nulls = scandesc.xs_orderbynulls.to_vec();
            }
            let top_idx = reorder_queue_first_idx(node)?
                .ok_or_else(|| elog("reorder queue unexpectedly empty"))?;
            let q = node.iss_ReorderQueue.as_ref().unwrap();
            let top = &q[top_idx];
            cmp_orderbyvals(
                &lastfetched_vals,
                &lastfetched_nulls,
                &top.orderbyvals,
                &top.orderbynulls,
                node,
            )? > 0
        } else {
            false
        };

        if must_push {
            reorderqueue_push(node, scan_slot, use_recomputed, estate)?;
            continue;
        } else {
            return Ok(true);
        }
    }
}

/// `EvalOrderByExpressions(node, econtext)` — recompute the ORDER BY clause
/// distances from the heap tuple, into `iss_OrderByValues`/`iss_OrderByNulls`.
fn EvalOrderByExpressions<'mcx>(
    node: &mut IndexScanState<'mcx>,
    econtext: EcxtId,
    scan_slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // econtext->ecxt_scantuple = slot (set by the caller / driver). We evaluate
    // each orderby expr in per-tuple memory (ExecEvalExprSwitchContext does the
    // MemoryContextSwitchTo).
    let _ = scan_slot;
    let n = node.indexorderbyorig.len();
    for i in 0..n {
        let (value, isnull) = {
            // Split the borrow: take the ExprState out of the Vec by index.
            let orderby = node
                .indexorderbyorig
                .get_mut(i)
                .ok_or_else(|| elog("index scan orderby expr index out of range"))?;
            execExpr::exec_eval_expr_switch_context::call(orderby, econtext, estate)?
        };
        if let Some(slot) = node.iss_OrderByValues.get_mut(i) {
            *slot = value;
        }
        if let Some(slot) = node.iss_OrderByNulls.get_mut(i) {
            *slot = isnull;
        }
    }
    Ok(())
}

/// `IndexRecheck(node, slot)` — EvalPlanQual recheck access method: does the
/// tuple meet the indexqual condition?
fn IndexRecheck<'mcx>(node: &mut IndexScanState<'mcx>, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
    // econtext->ecxt_scantuple = slot (set by the driver before recheck).
    let econtext = node
        .ss
        .ps
        .ps_ExprContext
        .ok_or_else(|| elog("index scan has no expr context"))?;
    exec_qual_and_reset(node.indexqualorig.as_deref_mut(), econtext, estate)
}

// ===========================================================================
// Reorder queue (ReorderTuple pairing heap).
// ===========================================================================

use ::nodes::ReorderTuple;

fn reorder_queue_is_empty(node: &IndexScanState<'_>) -> bool {
    node.iss_ReorderQueue
        .as_ref()
        .map(|q| q.is_empty())
        .unwrap_or(true)
}

/// `pairingheap_first(node->iss_ReorderQueue)` — the KNN-topmost queued tuple
/// (the one with the *smallest* ORDER BY distance, since the heap inverts the
/// sort). With the owned `PgVec` reorder queue we compute it by linear scan via
/// [`cmp_orderbyvals`], returning the index of the smallest.
fn reorder_queue_first_idx(node: &IndexScanState<'_>) -> PgResult<Option<usize>> {
    let q = match node.iss_ReorderQueue.as_ref() {
        Some(q) if !q.is_empty() => q,
        _ => return Ok(None),
    };
    let mut best = 0usize;
    for i in 1..q.len() {
        // best stays if q[best] <= q[i] (smallest distance first).
        if cmp_orderbyvals(
            &q[i].orderbyvals,
            &q[i].orderbynulls,
            &q[best].orderbyvals,
            &q[best].orderbynulls,
            node,
        )? < 0
        {
            best = i;
        }
    }
    Ok(Some(best))
}

/// `cmp_orderbyvals(adist, anulls, bdist, bnulls, node)` — compare ORDER BY
/// expression value vectors. Only NULLS LAST ordering is supported.
fn cmp_orderbyvals(
    adist: &[Datum<'_>],
    anulls: &[bool],
    bdist: &[Datum<'_>],
    bnulls: &[bool],
    node: &IndexScanState<'_>,
) -> PgResult<i32> {
    for i in 0..(node.iss_NumOrderByKeys as usize) {
        let an = anulls.get(i).copied().unwrap_or(false);
        let bn = bnulls.get(i).copied().unwrap_or(false);
        // Handle nulls (NULLS LAST).
        if an && !bn {
            return Ok(1);
        } else if !an && bn {
            return Ok(-1);
        } else if an && bn {
            return Ok(0);
        }

        let ssup = node
            .iss_SortSupport
            .get(i)
            .ok_or_else(|| elog("index scan missing sort support"))?;
        let a = adist
            .get(i)
            .cloned()
            .ok_or_else(|| elog("orderby value index out of range"))?;
        let b = bdist
            .get(i)
            .cloned()
            .ok_or_else(|| elog("orderby value index out of range"))?;
        let result = sortsupport::apply_sort_comparator::call(a, b, ssup)?;
        if result != 0 {
            return Ok(result);
        }
    }
    Ok(0)
}

/// `reorderqueue_push(node, slot, orderbyvals, orderbynulls)` — copy the slot's
/// tuple + distances into the per-query context and add it to the pairing heap.
fn reorderqueue_push<'mcx>(
    node: &mut IndexScanState<'mcx>,
    slot: SlotId,
    use_recomputed: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // MemoryContextSwitchTo(estate->es_query_cxt): the copies live in the
    // per-query context (the seam copy / datum_copy_v allocate there).
    let n = node.iss_NumOrderByKeys as usize;

    // rt->htup = ExecCopySlotHeapTuple(slot);
    let tuple = execTuples::exec_copy_slot_heap_tuple::call(estate, slot)?;

    // Source vectors: either our recomputed values, or the index's.
    let (src_vals, src_nulls): (alloc::vec::Vec<Datum>, alloc::vec::Vec<bool>) = if use_recomputed {
        (
            clone_datum_vec(&node.iss_OrderByValues),
            node.iss_OrderByNulls.to_vec(),
        )
    } else {
        let scandesc = node
            .iss_ScanDesc
            .as_ref()
            .ok_or_else(|| elog("index scan has no scan descriptor"))?;
        (
            clone_datum_vec(&scandesc.xs_orderbyvals),
            scandesc.xs_orderbynulls.to_vec(),
        )
    };

    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let mut orderbyvals: alloc::vec::Vec<Datum> = alloc::vec::Vec::new();
    let mut orderbynulls: alloc::vec::Vec<bool> = alloc::vec::Vec::new();
    orderbyvals
        .try_reserve_exact(n)
        .map_err(|_| mcx.oom(n * core::mem::size_of::<Datum>()))?;
    orderbynulls
        .try_reserve_exact(n)
        .map_err(|_| mcx.oom(n))?;
    for i in 0..n {
        let isnull = src_nulls.get(i).copied().unwrap_or(true);
        if !isnull {
            let typbyval = node.iss_OrderByTypByVals.get(i).copied().unwrap_or(false);
            let typlen = node.iss_OrderByTypLens.get(i).copied().unwrap_or(0);
            let v = src_vals
                .get(i)
                .cloned()
                .ok_or_else(|| elog("orderby value index out of range"))?;
            // rt->orderbyvals[i] = datumCopy(orderbyvals[i], typByVal, typLen);
            let copied =
                datum_seams::datum_copy_v::call(mcx, &v, typbyval, typlen as i32)?;
            orderbyvals.push(copied);
        } else {
            orderbyvals.push(Datum::null());
        }
        orderbynulls.push(isnull);
    }

    let rt = ReorderTuple {
        tuple,
        orderbyvals,
        orderbynulls,
    };
    let queue = node
        .iss_ReorderQueue
        .as_mut()
        .ok_or_else(|| elog("index scan has no reorder queue"))?;
    queue.push(rt);
    Ok(())
}

/// `pairingheap_remove_first(node->iss_ReorderQueue)` — remove and return the
/// queued tuple at `idx` (the KNN-topmost, located by [`reorder_queue_first_idx`]).
/// The C frees the (pass-by-ref) distance copies + the wrapper; in the owned
/// model dropping the popped `ReorderTuple` releases them. We `swap_remove` to
/// avoid an O(n) shift — order within the queue is recomputed by
/// `cmp_orderbyvals`, not positional.
fn reorderqueue_pop<'mcx>(
    node: &mut IndexScanState<'mcx>,
    idx: usize,
) -> PgResult<ReorderTuple<'mcx>> {
    let queue = node
        .iss_ReorderQueue
        .as_mut()
        .ok_or_else(|| elog("index scan has no reorder queue"))?;
    if idx >= queue.len() {
        return Err(elog("reorder queue pop index out of range"));
    }
    Ok(queue.swap_remove(idx))
}

// ===========================================================================
// ExecIndexScan — the ExecProcNode entry point.
// ===========================================================================

/// `ExecIndexScan(pstate)` — scan the index, returning whether the next
/// qualifying tuple is available (in the node's scan/result slot).
pub fn ExecIndexScan<'mcx>(
    pstate: &mut ::nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // If we have runtime keys and they've not already been set up, do it now.
    //
    //   if (node->iss_NumRuntimeKeys != 0 && !node->iss_RuntimeKeysReady)
    //       ExecReScan((PlanState *) node);
    //
    // A node whose `chgParam` is still set has not yet been rescanned for the
    // current parameter values: its immediate parent deliberately left it to be
    // "re-scanned by first ExecProcNode" (e.g. `ExecReScanNestLoop` /
    // `ExecReScanMemoize` skip rescanning a child whose chgParam is non-NULL).
    // For a parameterized index scan, recomputing the runtime keys here is that
    // deferred rescan; without it the scankeys keep the previous parameter's
    // values and the scan returns the same tuples for every outer row (the
    // classic Memoize-over-parameterized-IndexScan collapse). Dispatch through
    // the generic ExecReScan so it recomputes the keys *and* clears chgParam.
    let (num_runtime_keys, ready, num_orderby, chg_param_set) = match pstate {
        ::nodes::PlanStateNode::IndexScan(n) => (
            n.iss_NumRuntimeKeys,
            n.iss_RuntimeKeysReady,
            n.iss_NumOrderByKeys,
            n.ss.ps.chgParam.is_some(),
        ),
        _ => return Err(elog("ExecIndexScan dispatched to wrong node type")),
    };
    if (num_runtime_keys != 0 && !ready) || chg_param_set {
        // Generic ExecReScan dispatcher (execAmi): instrument loop end, chgParam
        // propagation, expr-context rescan, then dispatch to ExecReScanIndexScan.
        execAmi::exec_re_scan::call(pstate, estate)?;
    }

    let node = match pstate {
        ::nodes::PlanStateNode::IndexScan(n) => &mut **n,
        _ => return Err(elog("ExecIndexScan dispatched to wrong node type")),
    };

    // ExecScan(&node->ss, IndexNext{,WithReorder}, IndexRecheck) — delegated to
    // the execScan driver, passing this node's access/recheck methods.
    if num_orderby > 0 {
        execScan::exec_scan_index::call(node, estate, IndexNextWithReorder, IndexRecheck)
    } else {
        execScan::exec_scan_index::call(node, estate, IndexNext, IndexRecheck)
    }
}

// ===========================================================================
// ExecReScanIndexScan — recompute runtime keys, flush queue, rescan.
// ===========================================================================

/// `ExecReScanIndexScan(node)` — recalculate runtime scan-key values, flush the
/// reorder queue, then rescan the indexed relation.
pub fn ExecReScanIndexScan<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Recompute runtime key values, resetting the runtime context first.
    if node.iss_NumRuntimeKeys != 0 {
        let econtext = node
            .iss_RuntimeContext
            .ok_or_else(|| elog("index scan has no runtime context"))?;
        execUtils::reset_expr_context::call(estate, econtext)?;
        ExecIndexEvalRuntimeKeys_is(node, estate, econtext)?;
    }
    node.iss_RuntimeKeysReady = true;

    // Flush the reorder queue. (Order is irrelevant when discarding; clear it.)
    if let Some(q) = node.iss_ReorderQueue.as_mut() {
        // heap_freetuple(tuple) for each: dropping the ReorderTuples frees them.
        q.clear();
    }

    // Reset index scan.
    if node.iss_ScanDesc.is_some() {
        let mcx: Mcx<'mcx> = estate.es_query_cxt;
        indexam::index_rescan_is::call(mcx, node)?;
    }
    node.iss_ReachedEnd = false;

    // ExecScanReScan(&node->ss).
    execScan::exec_scan_rescan_ss::call(&mut node.ss, estate)
}

// ===========================================================================
// ExecEndIndexScan — release all storage.
// ===========================================================================

/// `ExecEndIndexScan(node)` — release all storage.
pub fn ExecEndIndexScan<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Final mirror of the AM-updated search counter (see sync_index_instrument);
    // also ensures the parallel-worker accumulate below reads the latest count.
    sync_index_instrument(node);

    // When ending a parallel worker, accumulate the gathered stats back into
    // shared memory for EXPLAIN ANALYZE.
    if node.iss_SharedInfo.is_some() && parallel::is_parallel_worker() {
        let nsearches = node.iss_Instrument.nsearches;
        let shared = node.iss_SharedInfo.as_mut().unwrap();
        parallel::accumulate_shared_index_searches(shared, nsearches);
    }

    // close the index relation (no-op if we didn't open it)
    if let Some(scandesc) = node.iss_ScanDesc.take() {
        let mcx: Mcx<'mcx> = estate.es_query_cxt;
        indexam::index_endscan::call(mcx, scandesc)?;
    }
    if let Some(index_rel) = node.iss_RelationDesc.take() {
        // index_close(rel, NoLock) — the handle's drop releases with NoLock.
        drop(index_rel);
    }

    Ok(())
}

// ===========================================================================
// ExecIndexMarkPos / ExecIndexRestrPos.
// ===========================================================================

/// `ExecIndexMarkPos(node)` — mark scan position.
pub fn ExecIndexMarkPos<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(epqstate) = estate.es_epq_active.as_deref() {
        let scanrelid = scan_scanrelid(node)?;
        debug_assert!(scanrelid > 0);
        let idx = (scanrelid - 1) as usize;
        if epq_relsubs_slot_present(epqstate, idx) || epq_relsubs_rowmark_present(epqstate, idx) {
            if !epq_relsubs_done(epqstate, idx) {
                return Err(elog("unexpected ExecIndexMarkPos call in EPQ recheck"));
            }
            return Ok(());
        }
    }

    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let scandesc = node
        .iss_ScanDesc
        .as_mut()
        .ok_or_else(|| elog("index scan: mark before scan started"))?;
    indexam::index_markpos::call(mcx, scandesc)
}

/// `ExecIndexRestrPos(node)` — restore scan position.
pub fn ExecIndexRestrPos<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(epqstate) = estate.es_epq_active.as_deref() {
        let scanrelid = scan_scanrelid(node)?;
        debug_assert!(scanrelid > 0);
        let idx = (scanrelid - 1) as usize;
        if epq_relsubs_slot_present(epqstate, idx) || epq_relsubs_rowmark_present(epqstate, idx) {
            if !epq_relsubs_done(epqstate, idx) {
                return Err(elog("unexpected ExecIndexRestrPos call in EPQ recheck"));
            }
            return Ok(());
        }
    }

    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let scandesc = node
        .iss_ScanDesc
        .as_mut()
        .ok_or_else(|| elog("index scan: restore before scan started"))?;
    indexam::index_restrpos::call(mcx, scandesc)
}

// ===========================================================================
// ExecInitIndexScan.
// ===========================================================================

/// `ExecInitIndexScan(node, estate, eflags)` — initialize the index scan's
/// state, create scan keys, and open the base and index relations.
pub fn ExecInitIndexScan<'mcx>(
    node: &'mcx ::nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, IndexScanState<'mcx>>> {
    let mcx: Mcx<'mcx> = estate.es_query_cxt;

    // IndexScan *node — castNode.
    let is: &'mcx IndexScan<'mcx> = match node.node_tag() {
        ::nodes::nodes::ntag::T_IndexScan => node.expect_indexscan(),
        _ => panic!("castNode(IndexScan, node) failed: {node:?}"),
    };

    // create state structure (makeNode(IndexScanState))
    let mut indexstate = IndexScanState::make_boxed_in(mcx)?;
    indexstate.ss.ps.plan = Some(node);
    indexstate.ss.ps.ExecProcNode = Some(exec_proc_node_trampoline);

    // create expression context for node
    execUtils::exec_assign_expr_context::call(estate, &mut indexstate.ss.ps)?;

    // open the scan relation
    let scanrelid = is.scan.scanrelid;
    let current_relation = execUtils::exec_open_scan_relation::call(estate, scanrelid, eflags)?;
    indexstate.ss.ss_currentRelation = Some(current_relation);
    indexstate.ss.ss_currentScanDesc = None; // no heap scan here

    // get the scan type from the relation descriptor
    let table_desc = relation_get_descr(estate, &indexstate)?;
    let table_ops = table_slot_callbacks();
    execTuples::exec_init_scan_tuple_slot::call(estate, &mut indexstate.ss, table_desc, table_ops)?;

    // Initialize result type and projection.
    execTuples::exec_init_result_type_tl::call(&mut indexstate.ss.ps, estate)?;
    execUtils::exec_assign_scan_projection_info::call(&mut indexstate.ss, estate)?;

    // initialize child expressions (qual + indexqualorig + indexorderbyorig)
    indexstate.ss.ps.qual = execExpr::exec_init_qual::call(
        is.scan.plan.qual.as_deref(),
        &mut indexstate.ss.ps,
        estate,
    )?;
    indexstate.indexqualorig = execExpr::exec_init_qual::call(
        is.indexqualorig.as_deref(),
        &mut indexstate.ss.ps,
        estate,
    )?;
    {
        let orig: alloc::vec::Vec<Option<&Expr>> = match is.indexorderbyorig.as_deref() {
            Some(list) => list.iter().map(Some).collect(),
            None => alloc::vec::Vec::new(),
        };
        let states = execExpr::exec_init_expr_list::call(&orig, &mut indexstate.ss.ps, estate)?;
        indexstate.indexorderbyorig.clear();
        for st in states.into_iter().flatten() {
            indexstate.indexorderbyorig.push(mcx::alloc_in(mcx, st)?);
        }
    }

    // If EXPLAIN only, stop here.
    if eflags & EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return Ok(indexstate);
    }

    // Open the index relation.
    let lockmode = execUtils::exec_rt_fetch_rellockmode::call(estate, scanrelid);
    let index_relation = indexam::index_open::call(mcx, is.indexid, lockmode)?;
    indexstate.iss_RelationDesc = Some(index_relation);

    // Initialize index-specific scan state.
    indexstate.iss_RuntimeKeysReady = false;
    indexstate.iss_RuntimeKeys.clear();
    indexstate.iss_NumRuntimeKeys = 0;

    // build the index scan keys from the index qualification
    {
        let index = index_alias(&indexstate)?;
        build_scan_keys_is(&mut indexstate, estate, index, is.indexqual.as_deref(), false)?;
    }
    // any ORDER BY exprs become scankeys in the same way
    {
        let index = index_alias(&indexstate)?;
        build_scan_keys_is(&mut indexstate, estate, index, is.indexorderby.as_deref(), true)?;
    }

    // Initialize sort support, if we need to re-check ORDER BY exprs.
    if indexstate.iss_NumOrderByKeys > 0 {
        let num_orderby = indexstate.iss_NumOrderByKeys as usize;
        let orderbyops = is
            .indexorderbyops
            .as_deref()
            .ok_or_else(|| elog("index scan: missing indexorderbyops"))?;
        let orderbyorig = is
            .indexorderbyorig
            .as_deref()
            .ok_or_else(|| elog("index scan: missing indexorderbyorig"))?;
        debug_assert_eq!(num_orderby, orderbyops.len());
        debug_assert_eq!(num_orderby, orderbyorig.len());

        // palloc0(numOrderByKeys * sizeof(SortSupportData)) + the typ arrays.
        indexstate.iss_SortSupport.clear();
        indexstate.iss_OrderByTypByVals.clear();
        indexstate.iss_OrderByTypLens.clear();
        indexstate
            .iss_SortSupport
            .try_reserve_exact(num_orderby)
            .map_err(|_| mcx.oom(num_orderby * core::mem::size_of::<SortSupportData>()))?;
        indexstate
            .iss_OrderByTypByVals
            .try_reserve_exact(num_orderby)
            .map_err(|_| mcx.oom(num_orderby))?;
        indexstate
            .iss_OrderByTypLens
            .try_reserve_exact(num_orderby)
            .map_err(|_| mcx.oom(num_orderby * 2))?;

        for i in 0..num_orderby {
            let orderbyop = orderbyops[i];
            let orderbyexpr = &orderbyorig[i];
            let info = nodeFuncs::expr_type_info::call(orderbyexpr)?;
            let orderby_type = info.typid;
            let orderby_coll = info.collation;

            // Initialize sort support (ssup_cxt = CurrentMemoryContext, NULLS LAST).
            let mut ssup = SortSupportData::new(mcx);
            ssup.ssup_collation = orderby_coll;
            ssup.ssup_nulls_first = false;
            ssup.ssup_attno = 0;
            ssup.abbreviate = false;
            sortsupport::prepare_sort_support_from_ordering_op::call(orderbyop, &mut ssup)?;
            indexstate.iss_SortSupport.push(ssup);

            // get_typlenbyval(orderbyType, &len, &byval).
            let (typlen, typbyval) = lsyscache::get_typlenbyval::call(orderby_type)?;
            indexstate.iss_OrderByTypLens.push(typlen);
            indexstate.iss_OrderByTypByVals.push(typbyval);
        }

        // allocate arrays to hold the re-calculated distances
        indexstate.iss_OrderByValues.clear();
        indexstate.iss_OrderByNulls.clear();
        indexstate
            .iss_OrderByValues
            .try_reserve_exact(num_orderby)
            .map_err(|_| mcx.oom(num_orderby * core::mem::size_of::<Datum>()))?;
        indexstate
            .iss_OrderByNulls
            .try_reserve_exact(num_orderby)
            .map_err(|_| mcx.oom(num_orderby))?;
        for _ in 0..num_orderby {
            indexstate.iss_OrderByValues.push(Datum::null());
            indexstate.iss_OrderByNulls.push(true);
        }

        // initialize the reorder queue (C: pairingheap_allocate(reorderqueue_cmp,
        // indexstate); the owned model is a PgVec with min-extraction at the
        // add/pop sites).
        indexstate.iss_ReorderQueue = Some(PgVec::new_in(mcx));
    }

    // If we have runtime keys, build a separate ExprContext to evaluate them.
    if indexstate.iss_NumRuntimeKeys != 0 {
        let stdecontext = indexstate.ss.ps.ps_ExprContext;
        execUtils::exec_assign_expr_context::call(estate, &mut indexstate.ss.ps)?;
        indexstate.iss_RuntimeContext = indexstate.ss.ps.ps_ExprContext;
        indexstate.ss.ps.ps_ExprContext = stdecontext;
    } else {
        indexstate.iss_RuntimeContext = None;
    }

    Ok(indexstate)
}

// ===========================================================================
// ExecIndexBuildScanKeys — the shared scan-key builder (owned by this unit).
// ===========================================================================

/// The output sink for [`exec_index_build_scan_keys_into`]: per-node references
/// to the scankey/runtimekey/(optional)arraykey output vectors + counts that
/// the C function fills via out-params.
struct ScanKeyOut<'a, 'mcx> {
    scan_keys: &'a mut PgVec<'mcx, ScanKeyData<'mcx>>,
    num_scan_keys: &'a mut i32,
    runtime_keys: &'a mut PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>>,
    num_runtime_keys: &'a mut i32,
    /// `None` => the caller passed `NULL` arrayKeys (index-only scans).
    array_keys: Option<&'a mut PgVec<'mcx, IndexArrayKeyInfo<'mcx>>>,
    num_array_keys: &'a mut i32,
}

/// `ExecIndexBuildScanKeys(planstate, index, quals, isorderby, ...)`
/// (nodeIndexscan.c) — build the index scan keys from the index qualification
/// expressions, classifying each qual into the five kinds (simple op +
/// const/runtime, RowCompareExpr, ScalarArrayOpExpr, NullTest).
///
/// `planstate` is the embedded `PlanState` head (for `ExecInitExpr`); the node's
/// scankey/runtimekey/arraykey output vectors are passed through [`ScanKeyOut`].
/// This is shared by the plain/index-only/bitmap nodes via the per-node
/// adapters installed into the `nodeIndexscan-seams`.
fn exec_index_build_scan_keys_into<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    index: rel::Relation<'mcx>,
    quals: Option<&[Expr<'mcx>]>,
    isorderby: bool,
    out: ScanKeyOut<'_, 'mcx>,
) -> PgResult<()> {
    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let quals: &[Expr<'mcx>] = quals.unwrap_or(&[]);
    let n_scan_keys = quals.len();

    // Allocate array for ScanKey structs: one per qual.
    let mut scan_keys: PgVec<ScanKeyData> = mcx::vec_with_capacity_in(mcx, n_scan_keys)?;
    for _ in 0..n_scan_keys {
        scan_keys.push(ScanKeyData::empty());
    }

    // runtime_keys: shared across the indexquals + indexorderbys calls (the
    // caller passes the running list in `out.runtime_keys`).
    let mut n_runtime_keys = *out.num_runtime_keys as usize;

    // array_keys: as large as it could possibly be (one per qual).
    let mut array_keys: PgVec<IndexArrayKeyInfo> = mcx::vec_with_capacity_in(mcx, n_scan_keys)?;
    let mut n_array_keys: usize = 0;

    let indnkeyatts = index.indnkeyatts();

    // We append runtime keys to `out.runtime_keys`; collect them locally first
    // so we don't alias `scan_keys` (the C addresses scan_keys by pointer).
    let mut new_runtime_keys: alloc::vec::Vec<IndexRuntimeKeyInfo> = alloc::vec::Vec::new();

    for (j, clause) in quals.iter().enumerate() {
        match clause {
            // 1/2. Simple operator: indexkey op const | expression.
            Expr::OpExpr(op) | Expr::DistinctExpr(op) | Expr::NullIfExpr(op) => {
                let mut flags = 0i32;
                let opno = op.opno;
                let opfuncid = op.opfuncid;

                // leftop should be the index key Var (possibly relabeled).
                let leftop = strip_relabel(get_leftop(&op.args));
                let varattno = require_index_var(leftop, "indexqual")?;
                if varattno < 1 || varattno as i32 > indnkeyatts {
                    return Err(elog("bogus index qualification"));
                }

                // look up the operator's strategy number (cross-check op vs index).
                let opfamily = relcache::rd_opfamily::call(&index, varattno)?;
                let (op_strategy, _op_lefttype, op_righttype) =
                    get_op_opfamily_properties(opno, opfamily, isorderby)?;

                if isorderby {
                    flags |= SK_ORDER_BY;
                }

                // rightop is the constant or variable comparison value.
                let rightop = strip_relabel(get_rightop(&op.args));
                let scanvalue = match rightop {
                    Some(Expr::Const(c)) => {
                        if c.constisnull {
                            flags |= SK_ISNULL;
                        }
                        const_value_in(mcx, &c.constvalue)?
                    }
                    Some(other) => {
                        // Need a runtime key. For an ORDER BY clause the target
                        // index addresses the iss_OrderByKeys array, not
                        // iss_ScanKeys (C: scan_key is a raw ScanKey pointer into
                        // whichever array). Flag the target so eval routes it to
                        // the order-by array.
                        new_runtime_keys.push(IndexRuntimeKeyInfo {
                            scan_key: encode_orderby_ref(j, isorderby),
                            key_expr: Some(exec_init_expr_in(other, planstate, estate)?),
                            key_toastable: type_is_toastable(op_righttype),
                        });
                        n_runtime_keys += 1;
                        Datum::null()
                    }
                    None => return Err(elog("indexqual has no right operand")),
                };

                scankey_owner::ScanKeyEntryInitialize(
                    &mut scan_keys[j],
                    flags,
                    varattno,
                    op_strategy as StrategyNumber,
                    op_righttype,
                    op.inputcollid,
                    opfuncid,
                    scanvalue,
                )?;
            }

            // 3. RowCompareExpr.
            Expr::RowCompareExpr(rc) => {
                if isorderby {
                    return Err(elog("ORDER BY cannot be a RowCompareExpr"));
                }
                let n_cols = rc.opnos.len();
                let mut sub_keys: alloc::vec::Vec<ScanKeyData> = alloc::vec::Vec::new();
                sub_keys
                    .try_reserve_exact(n_cols)
                    .map_err(|_| mcx.oom(n_cols * core::mem::size_of::<ScanKeyData>()))?;
                for _ in 0..n_cols {
                    sub_keys.push(ScanKeyData::empty());
                }

                let amcanorder = relation_amcanorder(&index)?;
                let mut first_attno: AttrNumber = 0;
                for k in 0..n_cols {
                    let flags = SK_ROW_MEMBER;
                    let leftop = strip_relabel(rc.largs.get(k));
                    let rightop = strip_relabel(rc.rargs.get(k));
                    let opno = rc.opnos[k];
                    let inputcollation = rc.inputcollids[k];

                    let varattno = require_index_var(leftop, "indexqual")?;
                    if !amcanorder || varattno < 1 || varattno as i32 > indnkeyatts {
                        return Err(elog("bogus RowCompare index qualification"));
                    }
                    if k == 0 {
                        first_attno = varattno;
                    }
                    let opfamily = relcache::rd_opfamily::call(&index, varattno)?;
                    let (op_strategy, op_lefttype, op_righttype) =
                        get_op_opfamily_properties(opno, opfamily, isorderby)?;
                    if op_strategy != rc.cmptype as i32 {
                        return Err(elog(
                            "RowCompare index qualification contains wrong operator",
                        ));
                    }
                    let opfuncid = lsyscache::get_opfamily_proc::call(
                        opfamily,
                        op_lefttype,
                        op_righttype,
                        BTORDER_PROC,
                    )?;
                    if opfuncid == InvalidOid {
                        return Err(elog("missing support function in opfamily"));
                    }

                    let mut flags = flags;
                    let scanvalue = match rightop {
                        Some(Expr::Const(c)) => {
                            if c.constisnull {
                                flags |= SK_ISNULL;
                            }
                            const_value_in(mcx, &c.constvalue)?
                        }
                        Some(other) => {
                            new_runtime_keys.push(IndexRuntimeKeyInfo {
                                // Subsidiary scankey: addressed within the row
                                // header's sk_subkeys (encoded below).
                                scan_key: encode_subkey_ref(j, k),
                                key_expr: Some(exec_init_expr_in(other, planstate, estate)?),
                                key_toastable: type_is_toastable(op_righttype),
                            });
                            n_runtime_keys += 1;
                            Datum::null()
                        }
                        None => return Err(elog("RowCompare member has no right operand")),
                    };

                    scankey_owner::ScanKeyEntryInitialize(
                        &mut sub_keys[k],
                        flags,
                        varattno,
                        op_strategy as StrategyNumber,
                        op_righttype,
                        inputcollation,
                        opfuncid,
                        scanvalue,
                    )?;
                }

                // Mark the last subsidiary scankey correctly.
                if let Some(last) = sub_keys.last_mut() {
                    last.sk_flags |= SK_ROW_END;
                }

                // Header: no valid sk_func; the subsidiary array lives in
                // sk_subkeys (the owned model of PointerGetDatum(first_sub_key)).
                let this = &mut scan_keys[j];
                *this = ScanKeyData::empty();
                this.sk_flags = SK_ROW_HEADER;
                this.sk_attno = first_attno;
                this.sk_strategy = rc.cmptype as StrategyNumber;
                this.sk_subkeys = Some(sub_keys);
            }

            // 4. ScalarArrayOpExpr.
            Expr::ScalarArrayOpExpr(saop) => {
                if isorderby {
                    return Err(elog("ORDER BY cannot be a ScalarArrayOpExpr"));
                }
                debug_assert!(saop.useOr);
                let mut flags = 0i32;
                let opno = saop.opno;
                let opfuncid = saop.opfuncid;

                let leftop = strip_relabel(saop.args.first());
                let varattno = require_index_var(leftop, "indexqual")?;
                if varattno < 1 || varattno as i32 > indnkeyatts {
                    return Err(elog("bogus index qualification"));
                }
                let opfamily = relcache::rd_opfamily::call(&index, varattno)?;
                let (op_strategy, _op_lefttype, op_righttype) =
                    get_op_opfamily_properties(opno, opfamily, isorderby)?;

                let rightop = strip_relabel(saop.args.get(1));
                let rightop = rightop.ok_or_else(|| elog("ScalarArrayOp has no array operand"))?;

                let scanvalue;
                if relation_amsearcharray(&index)? {
                    // Index AM handles this like a simple operator.
                    flags |= SK_SEARCHARRAY;
                    match rightop {
                        Expr::Const(c) => {
                            if c.constisnull {
                                flags |= SK_ISNULL;
                            }
                            scanvalue = const_value_in(mcx, &c.constvalue)?;
                        }
                        other => {
                            new_runtime_keys.push(IndexRuntimeKeyInfo {
                                scan_key: j,
                                key_expr: Some(exec_init_expr_in(other, planstate, estate)?),
                                // an array type is always toastable.
                                key_toastable: true,
                            });
                            n_runtime_keys += 1;
                            scanvalue = Datum::null();
                        }
                    }
                } else {
                    // Executor has to expand the array value.
                    array_keys.push(IndexArrayKeyInfo {
                        scan_key: j,
                        array_expr: Some(exec_init_expr_in(rightop, planstate, estate)?),
                        next_elem: 0,
                        num_elems: 0,
                        elem_values: PgVec::new_in(mcx),
                        elem_nulls: PgVec::new_in(mcx),
                    });
                    n_array_keys += 1;
                    scanvalue = Datum::null();
                }

                scankey_owner::ScanKeyEntryInitialize(
                    &mut scan_keys[j],
                    flags,
                    varattno,
                    op_strategy as StrategyNumber,
                    op_righttype,
                    saop.inputcollid,
                    opfuncid,
                    scanvalue,
                )?;
            }

            // 5. NullTest.
            Expr::NullTest(ntest) => {
                if isorderby {
                    return Err(elog("ORDER BY cannot be a NullTest"));
                }
                let leftop = strip_relabel(ntest.arg.as_deref());
                let varattno = require_index_var(leftop, "NullTest indexqual")?;
                let flags = match ntest.nulltesttype {
                    ::nodes::primnodes::NullTestType::IS_NULL => SK_ISNULL | SK_SEARCHNULL,
                    ::nodes::primnodes::NullTestType::IS_NOT_NULL => {
                        SK_ISNULL | SK_SEARCHNOTNULL
                    }
                };
                scankey_owner::ScanKeyEntryInitialize(
                    &mut scan_keys[j],
                    flags,
                    varattno,
                    InvalidStrategy,
                    InvalidOid,
                    InvalidOid,
                    InvalidOid,
                    Datum::null(),
                )?;
            }

            other => {
                let _ = other;
                return Err(elog("unsupported indexqual type"));
            }
        }
    }

    // Append the newly-built runtime keys to the running list, resolving any
    // subkey-encoded scan_key references to point at the just-built scan_keys.
    for rk in new_runtime_keys {
        out.runtime_keys.push(rk);
    }

    // Publish the outputs.
    *out.scan_keys = scan_keys;
    *out.num_scan_keys = n_scan_keys as i32;
    *out.num_runtime_keys = n_runtime_keys as i32;

    match out.array_keys {
        Some(ak) => {
            *ak = array_keys;
            *out.num_array_keys = n_array_keys as i32;
        }
        None => {
            if n_array_keys != 0 {
                return Err(elog("ScalarArrayOpExpr index qual found where not allowed"));
            }
        }
    }

    Ok(())
}

/// `ExecIndexEvalRuntimeKeys(econtext, runtimeKeys, numRuntimeKeys)` — evaluate
/// each runtime key expression and write the result into the target scankey.
/// Generic over the node's scankey + runtime-key vectors.
fn exec_index_eval_runtime_keys_into<'mcx>(
    scan_keys: &mut PgVec<'mcx, ScanKeyData<'mcx>>,
    orderby_keys: Option<&mut PgVec<'mcx, ScanKeyData<'mcx>>>,
    runtime_keys: &mut PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>>,
    num_runtime_keys: i32,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Re-borrow the order-by array per iteration (the `&mut` is consumed by
    // target_scankey's lifetime); hold it as an Option of a reborrowable ref.
    let mut orderby_keys = orderby_keys;
    for j in 0..(num_runtime_keys as usize) {
        let (target, toastable, value, isnull) = {
            let rk = runtime_keys
                .get_mut(j)
                .ok_or_else(|| elog("runtime key index out of range"))?;
            let key_expr = rk
                .key_expr
                .as_deref_mut()
                .ok_or_else(|| elog("runtime key has no expr"))?;
            let (value, isnull) =
                execExpr::exec_eval_expr_switch_context::call(key_expr, econtext, estate)?;
            (rk.scan_key, rk.key_toastable, value, isnull)
        };

        // Resolve the target scankey (possibly a row-header subsidiary key, or an
        // order-by-array key).
        let scan_key = target_scankey(scan_keys, orderby_keys.as_deref_mut(), target)?;
        if isnull {
            scan_key.sk_argument = value;
            scan_key.sk_flags |= SK_ISNULL;
        } else {
            // If the value is toastable, detoast it (C: PG_DETOAST_DATUM). In
            // the owned Datum model a by-ref Datum is already detoasted bytes;
            // the toastable flag drives no extra step here.
            let _ = toastable;
            scan_key.sk_argument = value;
            scan_key.sk_flags &= !SK_ISNULL;
        }
    }
    Ok(())
}

/// `ExecIndexEvalArrayKeys(econtext, arrayKeys, numArrayKeys)` — evaluate the
/// array key expressions and set up to iterate through the arrays. Returns
/// whether there are elements to consider (`false` => null/empty array).
fn exec_index_eval_array_keys_into<'mcx>(
    scan_keys: &mut PgVec<'mcx, ScanKeyData<'mcx>>,
    array_keys: &mut PgVec<'mcx, IndexArrayKeyInfo<'mcx>>,
    num_array_keys: i32,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let mut result = true;
    for j in 0..(num_array_keys as usize) {
        let (arraydatum, isnull, target) = {
            let ak = array_keys
                .get_mut(j)
                .ok_or_else(|| elog("array key index out of range"))?;
            let array_expr = ak
                .array_expr
                .as_deref_mut()
                .ok_or_else(|| elog("array key has no expr"))?;
            let (v, isnull) =
                execExpr::exec_eval_expr_switch_context::call(array_expr, econtext, estate)?;
            (v, isnull, ak.scan_key)
        };
        if isnull {
            result = false;
            break;
        }

        // Compute and deconstruct the array on the canonical by-reference Datum
        // lane. The array expression yields a varlena (text[]/name[]/...) carried
        // as `Datum::ByRef` bytes; the byte-image seams detoast and split it into
        // real per-element `(Datum<'mcx>, isnull)` values (the ByRef arm carries a
        // by-reference text element by value, so sk_argument is sound).
        let array_bytes = arraydatum.as_ref_bytes();
        let elmtype = arrayfuncs::array_get_elemtype_bytes::call(mcx, array_bytes)?;
        let tla = lsyscache::get_typlenbyvalalign::call(elmtype)?;
        let pairs = arrayfuncs::deconstruct_array_values_bytes::call(
            mcx,
            array_bytes,
            elmtype,
            tla.typlen,
            tla.typbyval,
            tla.typalign as core::ffi::c_char,
        )?;
        let num_elems = pairs.len();
        if num_elems == 0 {
            result = false;
            break;
        }

        // elem_values holds the canonical per-element Datums deconstructed from
        // the array; sk_argument takes the first element directly (canonical).
        let mut elem_values: PgVec<Datum<'mcx>> =
            mcx::vec_with_capacity_in(mcx, num_elems)?;
        let mut elem_nulls: PgVec<bool> = mcx::vec_with_capacity_in(mcx, num_elems)?;
        for (d, n) in pairs.iter() {
            elem_values.push(d.clone_in(mcx)?);
            elem_nulls.push(*n);
        }
        let first_val = elem_values[0].clone_in(mcx)?;
        let first_null = elem_nulls[0];

        {
            let ak = array_keys.get_mut(j).unwrap();
            ak.elem_values = elem_values;
            ak.elem_nulls = elem_nulls;
            ak.num_elems = num_elems as i32;
            ak.next_elem = 1;
        }
        let scan_key = target_scankey(scan_keys, None, target)?;
        scan_key.sk_argument = first_val;
        if first_null {
            scan_key.sk_flags |= SK_ISNULL;
        } else {
            scan_key.sk_flags &= !SK_ISNULL;
        }
    }
    Ok(result)
}

/// `ExecIndexAdvanceArrayKeys(arrayKeys, numArrayKeys)` — advance to the next
/// set of array-key element values. Returns whether there is another set.
fn exec_index_advance_array_keys_into<'mcx>(
    scan_keys: &mut PgVec<'mcx, ScanKeyData<'mcx>>,
    array_keys: &mut PgVec<'mcx, IndexArrayKeyInfo<'mcx>>,
    num_array_keys: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<bool> {
    let mut found = false;
    // Advance the rightmost array key most quickly.
    for j in (0..(num_array_keys as usize)).rev() {
        let (target, value, isnull) = {
            let ak = array_keys
                .get_mut(j)
                .ok_or_else(|| elog("array key index out of range"))?;
            let mut next_elem = ak.next_elem;
            let num_elems = ak.num_elems;
            if next_elem >= num_elems {
                next_elem = 0;
                found = false;
            } else {
                found = true;
            }
            let value = match ak.elem_values.get(next_elem as usize) {
                Some(d) => d.clone_in(mcx)?,
                None => Datum::null(),
            };
            let isnull = ak.elem_nulls.get(next_elem as usize).copied().unwrap_or(true);
            ak.next_elem = next_elem + 1;
            (ak.scan_key, value, isnull)
        };
        let scan_key = target_scankey(scan_keys, None, target)?;
        scan_key.sk_argument = value;
        if isnull {
            scan_key.sk_flags |= SK_ISNULL;
        } else {
            scan_key.sk_flags &= !SK_ISNULL;
        }
        if found {
            break;
        }
    }
    Ok(found)
}

// ===========================================================================
// Per-node adapters for ExecInitIndexScan (plain) + the shared seams.
// ===========================================================================

/// `ExecIndexBuildScanKeys(...)` for a plain `IndexScanState` (with ArrayKeys —
/// but the plain node has no array-key field; matching C, the plain node passes
/// `NULL`/0 for ArrayKeys, so SAOP non-searcharray quals raise the C error).
fn build_scan_keys_is<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    index: rel::Relation<'mcx>,
    quals: Option<&[Expr<'mcx>]>,
    isorderby: bool,
) -> PgResult<()> {
    // The plain IndexScan passes NULL ArrayKeys; both the quals call (writing
    // iss_ScanKeys) and the orderby call (writing iss_OrderByKeys) share the
    // runtime-key list (iss_RuntimeKeys).
    let mut dummy_num_array = 0i32;
    let IndexScanState {
        ss,
        iss_ScanKeys,
        iss_NumScanKeys,
        iss_OrderByKeys,
        iss_NumOrderByKeys,
        iss_RuntimeKeys,
        iss_NumRuntimeKeys,
        ..
    } = node;
    let (scan_keys, num_scan_keys) = if isorderby {
        (iss_OrderByKeys, iss_NumOrderByKeys)
    } else {
        (iss_ScanKeys, iss_NumScanKeys)
    };
    let out = ScanKeyOut {
        scan_keys,
        num_scan_keys,
        runtime_keys: iss_RuntimeKeys,
        num_runtime_keys: iss_NumRuntimeKeys,
        array_keys: None,
        num_array_keys: &mut dummy_num_array,
    };
    exec_index_build_scan_keys_into(&mut ss.ps, estate, index, quals, isorderby, out)
}

/// `ExecIndexEvalRuntimeKeys` for a plain `IndexScanState`.
fn ExecIndexEvalRuntimeKeys_is<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
) -> PgResult<()> {
    let IndexScanState {
        iss_ScanKeys,
        iss_OrderByKeys,
        iss_RuntimeKeys,
        iss_NumRuntimeKeys,
        ..
    } = node;
    let num = *iss_NumRuntimeKeys;
    exec_index_eval_runtime_keys_into(
        iss_ScanKeys,
        Some(iss_OrderByKeys),
        iss_RuntimeKeys,
        num,
        econtext,
        estate,
    )
}

// ===========================================================================
// Parallel Index Scan Support.
// ===========================================================================

/// `ExecIndexScanEstimate(node, pcxt)` — estimate DSM space for the parallel
/// index scan.
pub fn ExecIndexScanEstimate<'mcx>(
    node: &mut IndexScanState<'mcx>,
    pcxt: ParallelContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let instrument = node.ss.ps.instrument.is_some();
    let parallel_aware = plan_parallel_aware(node)?;
    if !instrument && !parallel_aware {
        return Ok(());
    }
    let index = index_alias(node)?;
    let nworkers = shm_toc::pcxt_nworkers::call(pcxt);
    node.iss_PscanLen = indexam::index_parallelscan_estimate::call(
        index,
        node.iss_NumScanKeys,
        node.iss_NumOrderByKeys,
        estate.es_snapshot.clone(),
        instrument,
        parallel_aware,
        nworkers,
    )?;
    shm_toc::estimate_chunk_and_key::call(pcxt, node.iss_PscanLen);
    Ok(())
}

/// `ExecIndexScanInitializeDSM(node, pcxt)` — set up a parallel index scan
/// descriptor.
pub fn ExecIndexScanInitializeDSM<'mcx>(
    node: &mut IndexScanState<'mcx>,
    pcxt: ParallelContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let instrument = node.ss.ps.instrument.is_some();
    let parallel_aware = plan_parallel_aware(node)?;
    if !instrument && !parallel_aware {
        return Ok(());
    }

    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let plan_node_id = plan_node_id(node)?;

    let heap_rel = current_rel_alias(node)?;
    let index_rel = index_alias(node)?;
    let nworkers = shm_toc::pcxt_nworkers::call(pcxt);

    // piscan = shm_toc_allocate(pcxt->toc, node->iss_PscanLen);
    let toc = parallel::pcxt_toc(pcxt);
    let pscan_cursor = parallel::shm_toc_allocate(toc, node.iss_PscanLen);

    // index_parallelscan_initialize(heapRel, indexRel, snapshot, instrument,
    //     parallel_aware, nworkers, &node->iss_SharedInfo, piscan) — writes the
    // descriptor (header + snapshot + instrumentation + AM tail) IN PLACE at the
    // chunk, returning the `Copy` in-DSM handle.
    let piscan = indexam::index_parallelscan_initialize::call(
        mcx,
        heap_rel,
        index_rel,
        estate.es_snapshot.clone(),
        instrument,
        parallel_aware,
        nworkers,
        pscan_cursor.0,
    )?;

    // shm_toc_insert(pcxt->toc, plan_node_id, piscan).
    parallel::shm_toc_insert(toc, plan_node_id as u64, pscan_cursor);

    if !parallel_aware {
        return Ok(());
    }

    let heap_rel = current_rel_alias(node)?;
    let index_rel = index_alias(node)?;
    let scandesc = indexam::index_beginscan_parallel::call(
        mcx,
        heap_rel,
        index_rel,
        node.iss_Instrument,
        node.iss_NumScanKeys,
        node.iss_NumOrderByKeys,
        piscan,
    )?;
    node.iss_ScanDesc = Some(scandesc);

    if node.iss_NumRuntimeKeys == 0 || node.iss_RuntimeKeysReady {
        indexam::index_rescan_is::call(mcx, node)?;
    }

    Ok(())
}

/// `ExecIndexScanReInitializeDSM(node, pcxt)` — reset shared state before a
/// fresh scan.
pub fn ExecIndexScanReInitializeDSM<'mcx>(
    node: &mut IndexScanState<'mcx>,
    _pcxt: ParallelContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(plan_parallel_aware(node).unwrap_or(true));
    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let scandesc = node
        .iss_ScanDesc
        .as_mut()
        .ok_or_else(|| elog("index parallel rescan before scan started"))?;
    indexam::index_parallelrescan::call(mcx, scandesc)
}

/// `ExecIndexScanInitializeWorker(node, pwcxt)` — copy info from the TOC into
/// planstate in a parallel worker.
pub fn ExecIndexScanInitializeWorker<'mcx>(
    node: &mut IndexScanState<'mcx>,
    pwcxt: ParallelWorkerContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let instrument = node.ss.ps.instrument.is_some();
    let parallel_aware = plan_parallel_aware(node)?;
    if !instrument && !parallel_aware {
        return Ok(());
    }

    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let plan_node_id = plan_node_id(node)?;

    // piscan = shm_toc_lookup(pwcxt->toc, plan_node_id, false) — the worker
    // recovers the SAME in-DSM `ParallelIndexScanDesc` base the leader placed.
    let toc = parallel::pwcxt_toc(pwcxt);
    let pscan_cursor = parallel::shm_toc_lookup(toc, plan_node_id as u64, false)
        .expect("ExecIndexScanInitializeWorker: shm_toc_lookup(noError=false) returned NULL");
    // SAFETY: `pscan_cursor.0` is the real in-segment base of the leader-
    // initialized descriptor (looked up by plan node id), live for the segment.
    let piscan = unsafe { ParallelIndexScanDescHandle::from_raw(pscan_cursor.0) };

    if instrument {
        let shared = indexam::index_scan_resolve_shared_info::call(piscan)?;
        node.iss_SharedInfo = Some(mcx::alloc_in(mcx, shared)?);
    }

    if !parallel_aware {
        return Ok(());
    }

    let heap_rel = current_rel_alias(node)?;
    let index_rel = index_alias(node)?;
    let scandesc = indexam::index_beginscan_parallel::call(
        mcx,
        heap_rel,
        index_rel,
        node.iss_Instrument,
        node.iss_NumScanKeys,
        node.iss_NumOrderByKeys,
        piscan,
    )?;
    node.iss_ScanDesc = Some(scandesc);

    if node.iss_NumRuntimeKeys == 0 || node.iss_RuntimeKeysReady {
        indexam::index_rescan_is::call(mcx, node)?;
    }

    Ok(())
}

/// `ExecIndexScanRetrieveInstrumentation(node)` — transfer index scan stats
/// from DSM to private memory.
pub fn ExecIndexScanRetrieveInstrumentation<'mcx>(
    node: &mut IndexScanState<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let shared = match &node.iss_SharedInfo {
        None => return Ok(()),
        Some(s) => s,
    };
    let n = usize::try_from(shared.num_workers.max(0)).unwrap_or(0);
    let mut winstrument: alloc::vec::Vec<IndexScanInstrumentation> = alloc::vec::Vec::new();
    winstrument
        .try_reserve_exact(n)
        .map_err(|_| mcx.oom(n * core::mem::size_of::<IndexScanInstrumentation>()))?;
    for i in 0..n {
        winstrument.push(shared.winstrument.get(i).copied().unwrap_or_default());
    }
    let copy = SharedIndexScanInstrumentation {
        num_workers: shared.num_workers,
        winstrument,
    };
    node.iss_SharedInfo = Some(mcx::alloc_in(mcx, copy)?);
    Ok(())
}

// ===========================================================================
// Small helpers reading the node's own/plan state (no foreign owner).
// ===========================================================================

// C: `epqstate->relsubs_slot[idx] != NULL`.
#[inline]
fn epq_relsubs_slot_present(epqstate: &::nodes::EPQState<'_>, idx: usize) -> bool {
    epqstate
        .relsubs_slot
        .as_ref()
        .and_then(|v| v.get(idx))
        .map(|s| s.is_some())
        .unwrap_or(false)
}

// C: `epqstate->relsubs_rowmark[idx] != NULL`.
#[inline]
fn epq_relsubs_rowmark_present(epqstate: &::nodes::EPQState<'_>, idx: usize) -> bool {
    epqstate
        .relsubs_rowmark
        .as_ref()
        .and_then(|v| v.get(idx).copied())
        .unwrap_or(false)
}

// C: `epqstate->relsubs_done[idx]`.
#[inline]
fn epq_relsubs_done(epqstate: &::nodes::EPQState<'_>, idx: usize) -> bool {
    epqstate
        .relsubs_done
        .as_ref()
        .and_then(|v| v.get(idx).copied())
        .unwrap_or(false)
}

/// The C "scandesc == NULL" branch of `IndexNext`/`IndexNextWithReorder`: begin
/// the serial index scan and pass the scankeys to the AM if they are ready.
fn begin_scan_serial<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // scandesc = index_beginscan(currentRelation, indexRelation, snapshot,
    //                            &iss_Instrument, numScanKeys, numOrderByKeys);
    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let heap_rel = current_rel_alias(node)?;
    let index_rel = index_alias(node)?;
    let scandesc = indexam::index_beginscan::call(
        mcx,
        heap_rel,
        index_rel,
        estate.es_snapshot.clone(),
        node.iss_Instrument,
        node.iss_NumScanKeys,
        node.iss_NumOrderByKeys,
    )?;
    node.iss_ScanDesc = Some(scandesc);

    // If no run-time keys or they are ready, pass the scankeys to the index AM.
    if node.iss_NumRuntimeKeys == 0 || node.iss_RuntimeKeysReady {
        indexam::index_rescan_is::call(mcx, node)?;
    }
    Ok(())
}

/// `ExecQualAndReset(qual, econtext)` (executor.h): evaluate the qual in the
/// node's expr context, then reset it. A `None` qual is always-true.
fn exec_qual_and_reset<'mcx>(
    qual: Option<&mut ::nodes::execexpr::ExprState<'mcx>>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let passed = match qual {
        Some(q) => execExpr::exec_qual::call(q, econtext, estate)?,
        None => true,
    };
    execUtils::reset_expr_context::call(estate, econtext)?;
    Ok(passed)
}

/// `ScanDirectionCombine(a, b)` (sdir.h): `(a) * (b)`.
fn scan_direction_combine(a: ScanDirection, b: ScanDirection) -> ScanDirection {
    match (a as i32) * (b as i32) {
        -1 => ScanDirection::BackwardScanDirection,
        1 => ScanDirection::ForwardScanDirection,
        _ => ScanDirection::NoMovementScanDirection,
    }
}

/// `InstrCountFiltered2(node, delta)` (instrument.h): bump `nfiltered2`.
fn instr_count_filtered2(node: &mut IndexScanState<'_>, delta: u64) {
    if let Some(instr) = node.ss.ps.instrument.as_mut() {
        instr.nfiltered2 += delta as f64;
    }
}

/// `((IndexScan *) node->ss.ps.plan)->indexorderdir`.
fn plan_indexorderdir(node: &IndexScanState<'_>) -> PgResult<ScanDirection> {
    match node.ss.ps.plan.and_then(|p| p.as_indexscan()) {
        Some(is) => Ok(is.indexorderdir),
        None => Err(elog("IndexScan node has wrong plan type")),
    }
}

/// `((Scan *) node->ss.ps.plan)->scanrelid`.
fn scan_scanrelid(node: &IndexScanState<'_>) -> PgResult<u32> {
    match node.ss.ps.plan.and_then(|p| p.as_indexscan()) {
        Some(is) => Ok(is.scan.scanrelid),
        None => Err(elog("IndexScan node has wrong plan type")),
    }
}

/// `node->ss.ps.plan->parallel_aware`.
fn plan_parallel_aware(node: &IndexScanState<'_>) -> PgResult<bool> {
    match node.ss.ps.plan {
        Some(n) => Ok(n.plan_head().parallel_aware),
        None => Err(elog("index scan has no plan")),
    }
}

/// `node->ss.ps.plan->plan_node_id`.
fn plan_node_id(node: &IndexScanState<'_>) -> PgResult<i32> {
    match node.ss.ps.plan {
        Some(n) => Ok(n.plan_head().plan_node_id),
        None => Err(elog("index scan has no plan")),
    }
}

/// `RelationGetDescr(currentRelation)` copied into the query context.
fn relation_get_descr<'mcx>(
    estate: &EStateData<'mcx>,
    node: &IndexScanState<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    let mcx = estate.es_query_cxt;
    let rel = node
        .ss
        .ss_currentRelation
        .as_ref()
        .ok_or_else(|| elog("index scan has no current relation"))?;
    Ok(Some(rel.rd_att_clone_in(mcx)?))
}

/// `table_slot_callbacks(currentRelation)` — heap relations use buffer-heap slots.
fn table_slot_callbacks() -> ::nodes::TupleSlotKind {
    ::nodes::TupleSlotKind::BufferHeapTuple
}

/// `node->iss_RelationDesc` alias.
fn index_alias<'mcx>(node: &IndexScanState<'mcx>) -> PgResult<rel::Relation<'mcx>> {
    node.iss_RelationDesc
        .as_ref()
        .map(|r| r.alias())
        .ok_or_else(|| elog("index scan has no index relation"))
}

/// `node->ss.ss_currentRelation` alias.
fn current_rel_alias<'mcx>(node: &IndexScanState<'mcx>) -> PgResult<rel::Relation<'mcx>> {
    node.ss
        .ss_currentRelation
        .as_ref()
        .map(|r| r.alias())
        .ok_or_else(|| elog("index scan has no current relation"))
}

/// Clone a slice of `Datum`s into an owned `Vec` (for cmp/push staging).
fn clone_datum_vec<'mcx>(src: &[Datum<'mcx>]) -> alloc::vec::Vec<Datum<'mcx>> {
    src.to_vec()
}

// --- scankey-build sub-helpers (nodeFuncs / catalog logic, owned) ----------

/// `get_leftop(clause)` — first argument of a binary op clause.
fn get_leftop<'a, 'mcx>(args: &'a [Expr<'mcx>]) -> Option<&'a Expr<'mcx>> {
    args.first()
}

/// `get_rightop(clause)` — second argument of a binary op clause.
fn get_rightop<'a, 'mcx>(args: &'a [Expr<'mcx>]) -> Option<&'a Expr<'mcx>> {
    args.get(1)
}

/// `if (IsA(op, RelabelType)) op = ((RelabelType *) op)->arg`.
fn strip_relabel<'a, 'mcx>(expr: Option<&'a Expr<'mcx>>) -> Option<&'a Expr<'mcx>> {
    match expr {
        Some(Expr::RelabelType(r)) => r.arg.as_deref(),
        other => other,
    }
}

/// Require the (relabeled) leftop to be `Var` with `varno == INDEX_VAR`,
/// returning its `varattno`. C: the `elog(ERROR, "<ctx> doesn't have key on
/// left side")`.
fn require_index_var(leftop: Option<&Expr>, ctx: &'static str) -> PgResult<AttrNumber> {
    match leftop {
        Some(Expr::Var(v)) if v.varno == INDEX_VAR => Ok(v.varattno),
        _ => {
            let _ = ctx;
            Err(elog("indexqual doesn't have key on left side"))
        }
    }
}

/// `get_op_opfamily_properties(opno, opfamily, false, ...)` — fatal on a miss.
fn get_op_opfamily_properties(opno: Oid, opfamily: Oid, isorderby: bool) -> PgResult<(i32, Oid, Oid)> {
    lsyscache::get_op_opfamily_properties::call(opno, opfamily, isorderby, false)?
        .ok_or_else(|| elog("operator is not a member of opfamily"))
}

/// `index->rd_indam->amcanorder`.
fn relation_amcanorder(index: &rel::Relation<'_>) -> PgResult<bool> {
    relcache::rd_indam_amcanorder::call(index)
}

/// `index->rd_indam->amsearcharray`.
fn relation_amsearcharray(index: &rel::Relation<'_>) -> PgResult<bool> {
    relcache::rd_indam_amsearcharray::call(index)
}

/// `TypeIsToastable(typid)` — `get_typstorage(typid) != TYPSTORAGE_PLAIN`. Our
/// repo lacks the get_typstorage seam in this unit's frontier; faithfully, only
/// runtime-key detoast hint, behaviour-preserving in the owned Datum model
/// where by-ref values are already detoasted. Reported conservatively as
/// `true` would force a no-op detoast; the C reads pg_type. We return whether
/// the type is a known fixed-by-value builtin: those are never toastable.
fn type_is_toastable(_typid: Oid) -> bool {
    // In the owned Datum model the runtime-key detoast (PG_DETOAST_DATUM) is a
    // no-op (by-ref Datums are already detoasted bytes), so the toastable flag
    // drives no behaviour. We carry it as `false` (no extra detoast) — see the
    // runtime-key eval where `key_toastable` is unused.
    false
}

/// `((Const *) rightop)->constvalue` copied into `mcx` as a canonical Datum.
/// The plan-tree Const carries a `Datum<'static>`; copying it into the
/// per-query context decouples it from the (read-only) plan tree.
fn const_value_in<'mcx, 'v>(mcx: Mcx<'mcx>, value: &Datum<'v>) -> PgResult<Datum<'mcx>> {
    // Faithfully `datumCopy` the Const's value into the per-query context. The
    // canonical `Datum::clone_in` covers every kind: ByVal (no-op word copy),
    // ByRef (deep-copy the varlena bytes), Cstring (clone the NUL-terminated
    // text), Composite (rebuild the HeapTupleHeader image), and Expanded
    // (flatten via EOH into a ByRef varlena). `Internal` has no copy semantics
    // (C never `datumCopy`s an `internal` pseudo-type, and a plan-tree Const is
    // never of internal type), which `clone_in` reflects.
    value.clone_in(mcx)
}

/// `ExecInitExpr(expr, planstate)` — compile an expression in the plan-state's
/// per-query context.
fn exec_init_expr_in<'mcx>(
    expr: &Expr,
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ::nodes::execexpr::ExprState<'mcx>>> {
    execExpr::exec_init_expr::call(expr, planstate, estate)
}

/// Encode a "subsidiary-key" runtime-key target reference (row header `header`,
/// subkey `sub`) into the `usize` scan_key index. The high bit flags a subkey
/// reference; the rest packs the header index and subkey index.
fn encode_subkey_ref(header: usize, sub: usize) -> usize {
    const SUBKEY_FLAG: usize = 1usize << (usize::BITS - 1);
    SUBKEY_FLAG | (header << 16) | (sub & 0xffff)
}

/// Bit flagging that a runtime-key target addresses the order-by scankey array
/// (`iss_OrderByKeys`) rather than the regular scankey array (`iss_ScanKeys`).
/// C resolves this with raw `ScanKey` pointers; the index model needs the flag.
const ORDERBY_FLAG: usize = 1usize << (usize::BITS - 2);

/// Encode a plain runtime-key target index, flagging the order-by array when the
/// key came from an ORDER BY clause.
fn encode_orderby_ref(idx: usize, isorderby: bool) -> usize {
    if isorderby {
        ORDERBY_FLAG | idx
    } else {
        idx
    }
}

/// Resolve a runtime/array-key `scan_key` target index into the live scankey,
/// following the subsidiary-key encoding for row-header keys and the order-by
/// flag for keys that address the order-by scankey array.
fn target_scankey<'a, 'mcx>(
    scan_keys: &'a mut PgVec<'mcx, ScanKeyData<'mcx>>,
    orderby_keys: Option<&'a mut PgVec<'mcx, ScanKeyData<'mcx>>>,
    target: usize,
) -> PgResult<&'a mut ScanKeyData<'mcx>> {
    const SUBKEY_FLAG: usize = 1usize << (usize::BITS - 1);
    if target & ORDERBY_FLAG != 0 {
        let idx = target & !ORDERBY_FLAG;
        return orderby_keys
            .ok_or_else(|| elog("order-by runtime key but no order-by scankeys"))?
            .get_mut(idx)
            .ok_or_else(|| elog("order-by scankey index out of range"));
    }
    if target & SUBKEY_FLAG != 0 {
        let header = (target & !SUBKEY_FLAG) >> 16;
        let sub = target & 0xffff;
        let hdr = scan_keys
            .get_mut(header)
            .ok_or_else(|| elog("row-header scankey index out of range"))?;
        let subs = hdr
            .sk_subkeys
            .as_mut()
            .ok_or_else(|| elog("row-header scankey has no subsidiary keys"))?;
        subs.get_mut(sub)
            .ok_or_else(|| elog("subsidiary scankey index out of range"))
    } else {
        scan_keys
            .get_mut(target)
            .ok_or_else(|| elog("scankey index out of range"))
    }
}

/// The `ExecProcNode` callback trampoline installed into `ps.ExecProcNode`.
fn exec_proc_node_trampoline<'mcx>(
    pstate: &mut ::nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let have = ExecIndexScan(pstate, estate)?;
    if have {
        let node = match pstate {
            ::nodes::PlanStateNode::IndexScan(n) => &mut **n,
            _ => return Err(elog("ExecProcNode dispatched to wrong node type")),
        };
        Ok(node.ss.ps.ps_ResultTupleSlot.or(node.ss.ss_ScanTupleSlot))
    } else {
        Ok(None)
    }
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install the shared scan-key seams (consumed by index-only + bitmap nodes)
/// and the parallel-executor method seams.
pub fn init_seams() {
    use nodeIndexscan_seams as sx;
    sx::exec_index_build_scan_keys_ios::set(seam_build_scan_keys_ios);
    sx::exec_index_eval_runtime_keys_ios::set(seam_eval_runtime_keys_ios);
    sx::exec_index_build_scan_keys_bis::set(seam_build_scan_keys_bis);
    sx::exec_index_eval_runtime_keys_bis::set(seam_eval_runtime_keys_bis);
    sx::exec_index_eval_array_keys_bis::set(seam_eval_array_keys_bis);
    sx::exec_index_advance_array_keys_bis::set(seam_advance_array_keys_bis);

    // `ParallelContext`/`shm_toc` accessor seams the parallel index-scan and
    // index-only-scan `Exec*Estimate` hooks reach across the `shm_toc-seams`
    // crate. The real bodies live on the DSM-owned live `ParallelContext` in
    // `backend-access-transam-parallel` (this crate already deps it directly);
    // the seam indirection just needs wiring to those bodies, mirroring how
    // `backend-executor-execParallel-support` installs `pcxt_nworkers` etc.
    //
    // `pcxt->nworkers` (access/parallel.h): the requested worker count.
    shm_toc::pcxt_nworkers::set(parallel::pcxt_nworkers);

    // `shm_toc_estimate_chunk(&pcxt->estimator, size)` +
    // `shm_toc_estimate_keys(&pcxt->estimator, 1)` (shm_toc.h): reserve DSM
    // space for one chunk of `size` bytes (the parallel index-scan descriptor)
    // and one TOC key. Both run against the context's backend-local estimator.
    // `add_size`/`mul_size` `ereport(ERROR)` on overflow in C — an estimation
    // overflow is effectively fatal there, so we mirror it as a panic rather
    // than swallow it (the seam is infallible by contract).
    shm_toc::estimate_chunk_and_key::set(|pcxt, size| {
        parallel::pcxt_estimate_chunk(pcxt, size)
            .expect("shm_toc_estimate_chunk: DSM size overflow");
        parallel::pcxt_estimate_keys(pcxt, 1)
            .expect("shm_toc_estimate_keys: DSM key-count overflow");
    });
}

// --- shared scan-key seam adapters (IndexOnlyScanState) --------------------

fn seam_build_scan_keys_ios<'mcx>(
    node: &mut ::nodes::IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    index: rel::Relation<'mcx>,
    quals: Option<&[Expr<'mcx>]>,
    is_orderby: bool,
) -> PgResult<()> {
    let mut dummy_num_array = 0i32;
    let ::nodes::IndexOnlyScanState {
        ss,
        ioss_ScanKeys,
        ioss_NumScanKeys,
        ioss_OrderByKeys,
        ioss_NumOrderByKeys,
        ioss_RuntimeKeys,
        ioss_NumRuntimeKeys,
        ..
    } = node;
    let (scan_keys, num_scan_keys) = if is_orderby {
        (ioss_OrderByKeys, ioss_NumOrderByKeys)
    } else {
        (ioss_ScanKeys, ioss_NumScanKeys)
    };
    let out = ScanKeyOut {
        scan_keys,
        num_scan_keys,
        runtime_keys: ioss_RuntimeKeys,
        num_runtime_keys: ioss_NumRuntimeKeys,
        array_keys: None,
        num_array_keys: &mut dummy_num_array,
    };
    exec_index_build_scan_keys_into(&mut ss.ps, estate, index, quals, is_orderby, out)
}

fn seam_eval_runtime_keys_ios<'mcx>(
    node: &mut ::nodes::IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
) -> PgResult<()> {
    let ::nodes::IndexOnlyScanState {
        ioss_ScanKeys,
        ioss_OrderByKeys,
        ioss_RuntimeKeys,
        ioss_NumRuntimeKeys,
        ..
    } = node;
    let num = *ioss_NumRuntimeKeys;
    exec_index_eval_runtime_keys_into(
        ioss_ScanKeys,
        Some(ioss_OrderByKeys),
        ioss_RuntimeKeys,
        num,
        econtext,
        estate,
    )
}

// --- shared scan-key seam adapters (BitmapIndexScanState) ------------------

fn seam_build_scan_keys_bis<'mcx>(
    node: &mut ::nodes::nodebitmapindexscan::BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    index: rel::Relation<'mcx>,
    quals: Option<&[Expr<'mcx>]>,
) -> PgResult<()> {
    let ::nodes::nodebitmapindexscan::BitmapIndexScanState {
        ss,
        biss_ScanKeys,
        biss_NumScanKeys,
        biss_RuntimeKeys,
        biss_NumRuntimeKeys,
        biss_ArrayKeys,
        biss_NumArrayKeys,
        ..
    } = node;
    let out = ScanKeyOut {
        scan_keys: biss_ScanKeys,
        num_scan_keys: biss_NumScanKeys,
        runtime_keys: biss_RuntimeKeys,
        num_runtime_keys: biss_NumRuntimeKeys,
        array_keys: Some(biss_ArrayKeys),
        num_array_keys: biss_NumArrayKeys,
    };
    // bitmap index scans never process ORDER BY (isorderby = false).
    exec_index_build_scan_keys_into(&mut ss.ps, estate, index, quals, false, out)
}

fn seam_eval_runtime_keys_bis<'mcx>(
    node: &mut ::nodes::nodebitmapindexscan::BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
) -> PgResult<()> {
    let ::nodes::nodebitmapindexscan::BitmapIndexScanState {
        biss_ScanKeys,
        biss_RuntimeKeys,
        biss_NumRuntimeKeys,
        ..
    } = node;
    let num = *biss_NumRuntimeKeys;
    // bitmap index scans never process ORDER BY, so there are no order-by keys.
    exec_index_eval_runtime_keys_into(biss_ScanKeys, None, biss_RuntimeKeys, num, econtext, estate)
}

fn seam_eval_array_keys_bis<'mcx>(
    node: &mut ::nodes::nodebitmapindexscan::BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
) -> PgResult<bool> {
    let ::nodes::nodebitmapindexscan::BitmapIndexScanState {
        biss_ScanKeys,
        biss_ArrayKeys,
        biss_NumArrayKeys,
        ..
    } = node;
    let num = *biss_NumArrayKeys;
    exec_index_eval_array_keys_into(biss_ScanKeys, biss_ArrayKeys, num, econtext, estate)
}

fn seam_advance_array_keys_bis<'mcx>(
    node: &mut ::nodes::nodebitmapindexscan::BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let ::nodes::nodebitmapindexscan::BitmapIndexScanState {
        biss_ScanKeys,
        biss_ArrayKeys,
        biss_NumArrayKeys,
        ..
    } = node;
    let num = *biss_NumArrayKeys;
    exec_index_advance_array_keys_into(biss_ScanKeys, biss_ArrayKeys, num, mcx)
}

// The parallel-executor methods (ExecIndexScanEstimate/InitializeDSM/
// ReInitializeDSM/InitializeWorker/RetrieveInstrumentation above) are dispatched
// directly by `backend-executor-execParallel` over the value-typed
// `PlanStateNode::IndexScan(&mut IndexScanState)` enum arm; no `PlanStateHandle`
// seam is needed.
