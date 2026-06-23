//! Port of `src/backend/executor/nodeSort.c` — routines to handle sorting of
//! relations.
//!
//! INTERFACE ROUTINES
//! - [`ExecSort`]        - sort the next tuple from the subplan
//! - [`ExecInitSort`]    - initialize node and subnodes
//! - [`ExecEndSort`]     - shutdown node and subnodes
//! - [`ExecSortMarkPos`] / [`ExecSortRestrPos`] - mark/restore the sort position
//! - [`ExecReScanSort`]  - rescan the sorted output
//! - parallel-query support: [`ExecSortEstimate`] / [`ExecSortInitializeDSM`] /
//!   [`ExecSortInitializeWorker`] / [`ExecSortRetrieveInstrumentation`]
//!
//! Sort feeds every tuple from its outer subtree into `tuplesort.c`, which
//! buffers the result in memory or a temp file, then returns the sorted tuples
//! one at a time. Two paths:
//!
//! * a **Datum sort** when the result is a single column (`datumSort`), and
//! * a **tuple sort** otherwise.
//!
//! `ExecSort` returns `Ok(true)` when the node's result slot
//! (`node.ss.ps.ps_ResultTupleSlot`) now holds a tuple (the C `return slot`)
//! and `Ok(false)` when it is empty (the C `return slot` over a cleared slot).
//!
//! Calls into unported owners (tuplesort.c, execProcnode.c, execTuples.c,
//! execUtils.c, execAmi.c, tcop/postgres.c's `ProcessInterrupts`, globals.c's
//! `work_mem`, and the parallel-executor / shm subsystems) go through those
//! owners' seam crates and panic until the owners land.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use execAmi_seams as execAmi;
use execProcnode_seams as execProcnode;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use transam_parallel as parallel;
use transam_parallel as parallel_sup;
use postgres_seams as tcop_postgres;
use init_small_seams as globals;
use tuplesort_seams as tuplesort;
use ::types_parallel::shared_dsm_object;

use ::mcx::{alloc_in, PgBox, PgVec};
use ::types_error::PgResult;
use ::execparallel::{
    ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle,
};
use ::nodes::execnodes::{ForwardScanDirection, ScanDirectionIsForward};
use ::nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use ::nodes::nodesort::{
    SharedSortInfo, SharedSortInfoHeader, Sort, SortStateData, TuplesortInstrumentation,
    TUPLESORT_ALLOWBOUNDED, TUPLESORT_NONE, TUPLESORT_RANDOMACCESS,
};
use ::nodes::{EStateData, PlanStateNode, SlotId, TupleSlotKind};

/// `offsetof(SharedSortInfo, sinstrument) + nworkers * sizeof(TuplesortInstrumentation)`
/// — the byte size of a `SharedSortInfo` carrying `nworkers` per-worker slots.
/// (`offsetof(SharedSortInfo, sinstrument)` is `sizeof(SharedSortInfoHeader)`
/// MAXALIGN'd up to `TuplesortInstrumentation`'s alignment.)
#[inline]
fn shared_sort_info_size(nworkers: usize) -> usize {
    use core::mem::{align_of, size_of};
    let h = size_of::<SharedSortInfoHeader>();
    let a = align_of::<TuplesortInstrumentation>();
    let off = (h + a - 1) & !(a - 1);
    off + nworkers * size_of::<TuplesortInstrumentation>()
}

/// `&shared_info->sinstrument[worker_index]` — the in-segment address of this
/// worker's slot in the DSM `SharedSortInfo` flex array.
#[inline]
fn sinstrument_slot_cursor(
    chunk: ::execparallel::SerializeCursor,
    worker_index: i32,
) -> ::execparallel::SerializeCursor {
    use core::mem::{align_of, size_of};
    let h = size_of::<SharedSortInfoHeader>();
    let a = align_of::<TuplesortInstrumentation>();
    let off = (h + a - 1) & !(a - 1);
    ::execparallel::SerializeCursor(
        chunk.0 + off + (worker_index as usize) * size_of::<TuplesortInstrumentation>(),
    )
}

/// Install this crate's seam implementations. nodeSort owns the inward
/// parallel-instrumentation hooks declared in `backend-executor-nodeSort-seams`
/// (the parallel executor dispatches to them by node tag).
pub fn init_seams() {
    nodeSort_seams::exec_sort_estimate::set(exec_sort_estimate_shim);
    nodeSort_seams::exec_sort_initialize_dsm::set(exec_sort_initialize_dsm_shim);
    nodeSort_seams::exec_sort_initialize_worker::set(
        exec_sort_initialize_worker_shim,
    );
    nodeSort_seams::exec_sort_retrieve_instrumentation::set(
        exec_sort_retrieve_instrumentation_shim,
    );
}

// ===========================================================================
// ExecSort — the ExecProcNode callback.
// ===========================================================================

/// `ExecSort(pstate)` — the `PlanState.ExecProcNode` callback.
///
/// On the first call, reads every tuple from the outer plan and feeds it to
/// `tuplesort.c`; subsequent calls just fetch the next sorted tuple. Returns
/// whether the node's result slot now holds a tuple.
pub fn ExecSort<'mcx>(
    node: &mut SortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    tcop_postgres::check_for_interrupts::call()?;

    // get state info from node
    //   estate = node->ss.ps.state; dir = estate->es_direction;
    let dir = estate.es_direction;

    // If first time through, read all tuples from outer plan and pass them to
    // tuplesort.c. Subsequent calls just fetch tuples from tuplesort.
    if !node.sort_Done {
        let mut tuplesortopts: i32 = TUPLESORT_NONE;

        // Want to scan subplan in the forward direction while creating the
        // sorted data.
        estate.es_direction = ForwardScanDirection;

        // Initialize tuplesort module.
        //   outerNode = outerPlanState(node);
        //   tupDesc = ExecGetResultType(outerNode);
        let mcx = estate.es_query_cxt;
        let work_mem = globals::work_mem::call();

        if node.randomAccess {
            tuplesortopts |= TUPLESORT_RANDOMACCESS;
        }
        if node.bounded {
            tuplesortopts |= TUPLESORT_ALLOWBOUNDED;
        }

        // Build the sort state. The Sort plan-node sort keys are read from the
        // node's plan back-link; the outer TupleDesc from the outer node's
        // result type. Every borrow below is a shared borrow of `node`, so they
        // coexist; the begin seam touches neither `node` nor `estate`.
        let mut tuplesortstate = if node.datumSort {
            // tuplesort_begin_datum(TupleDescAttr(tupDesc, 0)->atttypid,
            //                       plannode->sortOperators[0],
            //                       plannode->collations[0],
            //                       plannode->nullsFirst[0], work_mem, NULL, opts);
            let plannode = sort_plan(node)?;
            let sort_operator = *plannode
                .sortOperators
                .first()
                .ok_or_else(missing_sortkey)?;
            let sort_collation = *plannode.collations.first().ok_or_else(missing_sortkey)?;
            let nulls_first = *plannode.nullsFirst.first().ok_or_else(missing_sortkey)?;
            let outer = outer_plan_state(node)?;
            let datum_type = execTuples::exec_get_result_type::call(&outer.ps_head())
                .ok_or_else(missing_result_type)?
                .attr(0)
                .atttypid;
            tuplesort::tuplesort_begin_datum::call(
                mcx,
                datum_type,
                sort_operator,
                sort_collation,
                nulls_first,
                work_mem,
                tuplesortopts,
            )?
        } else {
            // tuplesort_begin_heap(tupDesc, plannode->numCols, plannode->sortColIdx,
            //                      plannode->sortOperators, plannode->collations,
            //                      plannode->nullsFirst, work_mem, NULL, opts);
            let plannode = sort_plan(node)?;
            let num_cols = plannode.numCols;
            let outer = outer_plan_state(node)?;
            let tupdesc = execTuples::exec_get_result_type::call(&outer.ps_head())
                .ok_or_else(missing_result_type)?;
            tuplesort::tuplesort_begin_heap::call(
                mcx,
                tupdesc,
                num_cols,
                &plannode.sortColIdx,
                &plannode.sortOperators,
                &plannode.collations,
                &plannode.nullsFirst,
                work_mem,
                tuplesortopts,
            )?
        };

        if node.bounded {
            //   tuplesort_set_bound(tuplesortstate, node->bound);
            tuplesort::tuplesort_set_bound::call(&mut tuplesortstate, node.bound)?;
        }

        // Scan the subplan and feed all the tuples to tuplesort.
        if node.datumSort {
            loop {
                //   slot = ExecProcNode(outerNode); if (TupIsNull(slot)) break;
                let slot_id = match next_outer_slot(node, estate)? {
                    None => break,
                    Some(id) => id,
                };
                //   slot_getsomeattrs(slot, 1);
                //   tuplesort_putdatum(tuplesortstate, slot->tts_values[0],
                //                      slot->tts_isnull[0]);
                let (val, is_null) =
                    execTuples::slot_getsomeattr::call(estate, slot_id, 1)?;
                // The datum-sort column is `slot->tts_values[0]`; the
                // tuplesort_putdatum seam now takes the canonical `Datum<'_>`,
                // so the value flows through unchanged.
                tuplesort::tuplesort_putdatum::call(&mut tuplesortstate, val, is_null)?;
            }
        } else {
            loop {
                //   slot = ExecProcNode(outerNode); if (TupIsNull(slot)) break;
                let slot_id = match next_outer_slot(node, estate)? {
                    None => break,
                    Some(id) => id,
                };
                //   tuplesort_puttupleslot(tuplesortstate, slot);
                // C's tuplesort_puttupleslot does ExecCopySlotMinimalTuple(slot),
                // which slot_getallattrs(slot) first; the owned puttupleslot seam
                // forms the MinimalTuple from the slot's deformed value/null arrays,
                // so the slot must be fully deconstructed before the put.
                let _ = execTuples::slot_getallattrs_by_id::call(estate, slot_id)?;
                tuplesort::tuplesort_puttupleslot::call(
                    &mut tuplesortstate,
                    estate.slot(slot_id),
                )?;
            }
        }

        // Complete the sort.
        tuplesort::tuplesort_performsort::call(&mut tuplesortstate)?;

        // restore to user specified direction
        estate.es_direction = dir;

        // finally set the sorted flag to true
        node.sort_Done = true;
        node.bounded_Done = node.bounded;
        node.bound_Done = node.bound;

        // Stash the completed sort state in the node.
        node.tuplesortstate = Some(alloc_in(estate.es_query_cxt, tuplesortstate)?);

        if node.shared_info.is_some() && node.am_worker {
            //   Assert(IsParallelWorker());
            //   Assert(ParallelWorkerNumber <= node->shared_info->num_workers);
            //   si = &node->shared_info->sinstrument[ParallelWorkerNumber];
            //   tuplesort_get_stats(tuplesortstate, si);
            let ts = node
                .tuplesortstate
                .as_deref_mut()
                .expect("just installed above");
            let stats = tuplesort::tuplesort_get_stats::call(ts);
            store_worker_stats(node, stats)?;
        }
    }

    // Fetch the next sorted item from the appropriate tuplesort function.
    //   slot = node->ss.ps.ps_ResultTupleSlot;
    let slot = node
        .ss
        .ps
        .ps_ResultTupleSlot
        .ok_or_else(|| missing_result_slot())?;
    let forward = ScanDirectionIsForward(dir);
    let ts = node
        .tuplesortstate
        .as_deref_mut()
        .expect("sort_Done implies tuplesortstate is set");

    if node.datumSort {
        // For datum sorts we manage the slot ourselves and leave it clear when
        // tuplesort_getdatum returns false.
        //   ExecClearTuple(slot);
        //   if (tuplesort_getdatum(tuplesortstate, ScanDirectionIsForward(dir),
        //                          false, &slot->tts_values[0],
        //                          &slot->tts_isnull[0], NULL))
        //       ExecStoreVirtualTuple(slot);
        let (found, val, is_null) =
            tuplesort::tuplesort_getdatum::call(ts, forward, false)?;
        if found {
            // `tuplesort_getdatum` already returns the canonical `Datum<'mcx>`
            // (by-value or by-reference), and `exec_store_first_datum` consumes
            // the same canonical type, so the sorted column value flows through
            // unchanged — no shim round-trip (the old `from_usize(as_usize())`
            // hop would have panicked on a by-reference value).
            execTuples::exec_store_first_datum::call(estate, slot, val, is_null)?;
            Ok(true)
        } else {
            execTuples::exec_clear_tuple::call(estate, slot)?;
            Ok(false)
        }
    } else {
        // tuplesort_gettupleslot manages the slot for us, emptying it when it
        // runs out of tuples.
        //   (void) tuplesort_gettupleslot(tuplesortstate,
        //                                 ScanDirectionIsForward(dir), false,
        //                                 slot, NULL);
        let produced = tuplesort::tuplesort_gettupleslot::call(
            ts,
            forward,
            false,
            estate.slot_data_mut(slot),
        )?;
        Ok(produced)
    }
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitSort`]:
/// `castNode(SortState, pstate)` then run [`ExecSort`], returning the result
/// slot's id (the C `return slot`) or `None`.
fn exec_sort_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::Sort(node) => node,
        other => panic!("castNode(SortState, pstate) failed: {other:?}"),
    };
    if ExecSort(node, estate)? {
        Ok(node.ss.ps.ps_ResultTupleSlot)
    } else {
        Ok(None)
    }
}

// ===========================================================================
// ExecInitSort.
// ===========================================================================

/// `ExecInitSort(node, estate, eflags)` — create the run-time state for the
/// sort plan node and initialize its outer subtree.
///
/// Allocated in `estate.es_query_cxt` (C: `makeNode` in the per-query context),
/// so fallible on OOM. Panics if `node` is not a `Sort` (the C `castNode`).
pub fn ExecInitSort<'mcx>(
    node: &'mcx ::nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut eflags: i32,
) -> PgResult<PgBox<'mcx, SortStateData<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let sort: &'mcx Sort<'mcx> = node.expect_sort();

    // create state structure
    //   sortstate = makeNode(SortState);
    //   sortstate->ss.ps.plan = (Plan *) node;
    //   sortstate->ss.ps.state = estate;
    //   sortstate->ss.ps.ExecProcNode = ExecSort;
    let mut sortstate = alloc_in(mcx, SortStateData::default())?;
    sortstate.ss.ps.plan = Some(node);
    sortstate.ss.ps.ExecProcNode = Some(exec_sort_node);

    // We must have random access to the sort output to do backward scan or
    // mark/restore. We also prefer to materialize the sort output if we might
    // be called on to rewind and replay it many times.
    sortstate.randomAccess =
        (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) != 0;

    sortstate.bounded = false;
    sortstate.sort_Done = false;
    sortstate.tuplesortstate = None;

    // Miscellaneous initialization
    //
    // Sort nodes don't initialize their ExprContexts because they never call
    // ExecQual or ExecProject.

    // initialize child nodes
    //
    // We shield the child node from the need to support REWIND, BACKWARD, or
    // MARK/RESTORE.
    eflags &= !(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

    //   outerPlanState(sortstate) = ExecInitNode(outerPlan(node), estate, eflags);
    let outer_plan = sort.plan.lefttree.as_deref();
    sortstate.ss.ps.lefttree =
        execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;

    // Initialize scan slot and type.
    //   ExecCreateScanSlotFromOuterPlan(estate, &sortstate->ss, &TTSOpsVirtual);
    execUtils::exec_create_scan_slot_from_outer_plan::call(
        estate,
        &mut sortstate.ss,
        TupleSlotKind::Virtual,
    )?;

    // Initialize return slot and type. No need to initialize projection info
    // because this node doesn't do projections.
    //   ExecInitResultTupleSlotTL(&sortstate->ss.ps, &TTSOpsMinimalTuple);
    //   sortstate->ss.ps.ps_ProjInfo = NULL;
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut sortstate.ss.ps,
        estate,
        TupleSlotKind::MinimalTuple,
    )?;
    sortstate.ss.ps.ps_ProjInfo = None;

    // outerTupDesc = ExecGetResultType(outerPlanState(sortstate));
    //
    // We perform a Datum sort when we're sorting just a single column,
    // otherwise we perform a tuple sort.
    //   if (outerTupDesc->natts == 1) sortstate->datumSort = true;
    //   else sortstate->datumSort = false;
    let natts = {
        let outer = outer_plan_state(&sortstate)?;
        execTuples::exec_get_result_type::call(&outer.ps_head())
            .ok_or_else(|| missing_result_type())?
            .natts
    };
    sortstate.datumSort = natts == 1;

    Ok(sortstate)
}

// ===========================================================================
// ExecEndSort.
// ===========================================================================

/// `ExecEndSort(node)` — release tuplesort resources and shut down the subplan.
pub fn ExecEndSort<'mcx>(
    node: &mut SortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Release tuplesort resources
    //   if (node->tuplesortstate != NULL)
    //       tuplesort_end((Tuplesortstate *) node->tuplesortstate);
    //   node->tuplesortstate = NULL;
    if let Some(ts) = node.tuplesortstate.take() {
        tuplesort::tuplesort_end::call(ts)?;
    }

    // shut down the subplan
    //   ExecEndNode(outerPlanState(node));
    let outer = node
        .ss
        .ps
        .lefttree
        .as_deref_mut()
        .ok_or_else(|| missing_outer_plan_state())?;
    execProcnode::exec_end_node::call(outer, estate)
}

// ===========================================================================
// Mark / restore.
// ===========================================================================

/// `ExecSortMarkPos(node)` — save the current position in the sorted output.
pub fn ExecSortMarkPos(node: &mut SortStateData<'_>) -> PgResult<()> {
    // if we haven't sorted yet, just return
    if !node.sort_Done {
        return Ok(());
    }

    //   tuplesort_markpos((Tuplesortstate *) node->tuplesortstate);
    let ts = node
        .tuplesortstate
        .as_deref_mut()
        .expect("sort_Done implies tuplesortstate is set");
    tuplesort::tuplesort_markpos::call(ts)
}

/// `ExecSortRestrPos(node)` — restore the last saved sort position.
pub fn ExecSortRestrPos(node: &mut SortStateData<'_>) -> PgResult<()> {
    // if we haven't sorted yet, just return.
    if !node.sort_Done {
        return Ok(());
    }

    // restore the scan to the previously marked position
    //   tuplesort_restorepos((Tuplesortstate *) node->tuplesortstate);
    let ts = node
        .tuplesortstate
        .as_deref_mut()
        .expect("sort_Done implies tuplesortstate is set");
    tuplesort::tuplesort_restorepos::call(ts)
}

// ===========================================================================
// ExecReScanSort.
// ===========================================================================

/// `ExecReScanSort(node)` — rescan the sort node, re-sorting if necessary or
/// rewinding the existing sorted output.
pub fn ExecReScanSort<'mcx>(
    node: &mut SortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // If we haven't sorted yet, just return. If outerplan's chgParam is not
    // NULL then it will be re-scanned by ExecProcNode, else no reason to
    // re-scan it at all.
    if !node.sort_Done {
        return Ok(());
    }

    // must drop pointer to sort result tuple
    //   ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    let slot = node
        .ss
        .ps
        .ps_ResultTupleSlot
        .ok_or_else(|| missing_result_slot())?;
    execTuples::exec_clear_tuple::call(estate, slot)?;

    // If subnode is to be rescanned then we forget previous sort results; we
    // have to re-read the subplan and re-sort. Also must re-sort if the
    // bounded-sort parameters changed or we didn't select randomAccess.
    // Otherwise we can just rewind and rescan the sorted output.
    //   PlanState *outerPlan = outerPlanState(node);
    let outer_chgparam_present = node
        .ss
        .ps
        .lefttree
        .as_deref()
        .ok_or_else(|| missing_outer_plan_state())?
        .ps_head()
        .chgParam
        .is_some();

    if outer_chgparam_present
        || node.bounded != node.bounded_Done
        || node.bound != node.bound_Done
        || !node.randomAccess
    {
        //   node->sort_Done = false;
        //   tuplesort_end((Tuplesortstate *) node->tuplesortstate);
        //   node->tuplesortstate = NULL;
        node.sort_Done = false;
        if let Some(ts) = node.tuplesortstate.take() {
            tuplesort::tuplesort_end::call(ts)?;
        }

        // if chgParam of subnode is not null then plan will be re-scanned by
        // first ExecProcNode.
        //   if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
        if !outer_chgparam_present {
            let outer = node
                .ss
                .ps
                .lefttree
                .as_deref_mut()
                .ok_or_else(|| missing_outer_plan_state())?;
            execAmi::exec_re_scan::call(outer, estate)?;
        }
    } else {
        //   tuplesort_rescan((Tuplesortstate *) node->tuplesortstate);
        let ts = node
            .tuplesortstate
            .as_deref_mut()
            .expect("sort_Done implies tuplesortstate is set");
        tuplesort::tuplesort_rescan::call(ts)?;
    }

    Ok(())
}

// ===========================================================================
// Parallel query support.
//
// These four hooks are dispatched to by the parallel executor (execParallel.c)
// over a live `PlanState` tree it walks via opaque handles. nodeSort owns the
// C control flow (the instrument/nworkers guards, the shm chunk sizing, the
// `am_worker` flag, copying shared->private); the handle-addressed reads/writes
// of the not-yet-ported `ParallelContext`, `shm_toc`, and live `SortState`
// node go through the parallel-executor support seams.
// ===========================================================================

/// `ExecSortEstimate(node, pcxt)` — estimate the shared-memory space required
/// to propagate sort statistics.
///
/// The C `(SortState *) node` cast is the owned `&mut SortStateData` borrow; the
/// node's own fields (`ss.ps.instrument`, the `SharedSortInfo` chunk sizing) are
/// read directly. Only the orthogonal `ParallelContext`/`shm_toc` estimator
/// (still an unported subsystem held by handle) goes through the `pcxt_*` support
/// seams — exactly as nodeHash/nodeMemoize do.
pub fn ExecSortEstimate<'mcx>(
    node: &mut SortStateData<'mcx>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    //   if (!node->ss.ps.instrument || pcxt->nworkers == 0) return;
    if node.ss.ps.instrument.is_none() || parallel_sup::pcxt_nworkers(pcxt) == 0 {
        return Ok(());
    }

    let nworkers = parallel_sup::pcxt_nworkers(pcxt) as usize;

    //   size = mul_size(pcxt->nworkers, sizeof(TuplesortInstrumentation));
    //   size = add_size(size, offsetof(SharedSortInfo, sinstrument));
    let size = shared_dsm_object::estimate_flex(shared_sort_info_size(nworkers));

    //   shm_toc_estimate_chunk(&pcxt->estimator, size);
    //   shm_toc_estimate_keys(&pcxt->estimator, 1);
    let estimator = parallel_sup::pcxt_estimator(pcxt);
    parallel_sup::shm_toc_estimate_chunk(estimator, size);
    parallel_sup::shm_toc_estimate_keys(estimator, 1);
    Ok(())
}

/// `ExecSortInitializeDSM(node, pcxt)` — initialize DSM space for sort stats.
///
/// The leader `shm_toc_allocate`s a `SharedSortInfo` chunk in DSM, zeroes it,
/// sets `num_workers`, and registers it under `node->ss.ps.plan->plan_node_id`,
/// stashing the DSM pointer in `node->shared_info` so each worker's `ExecSort`
/// copyback lands in the shared bytes. With the owned node in hand the
/// instrument/nworkers guard and the chunk sizing run directly. What is genuinely
/// missing is the **DSM-resident `shared_info` carrier**: the merged
/// `SortStateData.shared_info` is an in-process `PgBox<SharedSortInfo>`
/// (types-nodes), which cannot hold the DSM `SharedRef`/chunk cursor, and
/// `SharedRef` is unstorable in `types-nodes` anyway (it lives in the parallel
/// keystone crate). Re-typing the carrier is a contract-divergence from the
/// merged nodeSort port and would force a rewrite of the worker copyback in
/// `ExecSort`'s `store_worker_stats`. This is the same blocker nodeAgg's
/// `ExecAggInitializeDSM` hits; mirror-and-panic into the DSM owner until the
/// carrier surface lands.
pub fn ExecSortInitializeDSM<'mcx>(
    node: &mut SortStateData<'mcx>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    //   if (!node->ss.ps.instrument || pcxt->nworkers == 0) return;
    let nworkers = parallel_sup::pcxt_nworkers(pcxt);
    if node.ss.ps.instrument.is_none() || nworkers == 0 {
        return Ok(());
    }

    let plan_node_id = sort_plan(node)?.plan.plan_node_id;

    //   size = offsetof(SharedSortInfo, sinstrument)
    //          + pcxt->nworkers * sizeof(TuplesortInstrumentation);
    let size = shared_sort_info_size(nworkers as usize);

    //   node->shared_info = shm_toc_allocate(pcxt->toc, size);
    let toc = parallel_sup::pcxt_toc(pcxt);
    let chunk = parallel_sup::shm_toc_allocate(toc, shared_dsm_object::estimate_flex(size));

    // A parallel query with instrument-bearing workers always has a real DSM
    // segment.
    let seg = parallel_sup::pcxt_seg(pcxt)
        .expect("ExecSortInitializeDSM: instrumenting parallel query without a DSM segment");

    //   /* ensure any unfilled slots will contain zeroes */
    //   memset(node->shared_info, 0, size);
    //   node->shared_info->num_workers = pcxt->nworkers;
    let (_hdr, _tail) =
        shared_dsm_object::place_flex::<SharedSortInfoHeader, TuplesortInstrumentation>(
            seg,
            chunk,
            nworkers as usize,
            SharedSortInfoHeader { num_workers: nworkers },
            |_i| TuplesortInstrumentation::default(),
        );

    //   shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, node->shared_info);
    parallel_sup::shm_toc_insert(toc, plan_node_id as u64, chunk);

    node.shared_info = Some(SharedSortInfo::Dsm {
        chunk,
        seg,
        num_workers: nworkers,
    });
    Ok(())
}

/// `ExecSortInitializeWorker(node, pwcxt)` — attach a worker to DSM space.
///
/// `node->shared_info = shm_toc_lookup(pwcxt->toc, plan_node_id, true);
/// node->am_worker = true;` — the worker looks up the leader's chunk by
/// `plan_node_id` and would attach to it. `am_worker` is an owned `bool` that
/// would be set directly, but the `shm_toc_lookup` result is a DSM `SharedRef`
/// the in-process `PgBox<SharedSortInfo>` carrier cannot hold (same blocker as
/// `ExecSortInitializeDSM` / nodeAgg's `ExecAggInitializeWorker`); doing only the
/// `am_worker` write while skipping the attach would silently diverge from C (the
/// worker would then have no shared_info to copy its stats into). Mirror-and-panic
/// into the DSM owner until the carrier surface lands.
pub fn ExecSortInitializeWorker<'mcx>(
    node: &mut SortStateData<'mcx>,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    //   node->shared_info =
    //       shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, true);
    //   node->am_worker = true;
    //
    // The lookup is `noError = true` (missing_ok): when the leader is NOT
    // instrumenting (no EXPLAIN ANALYZE) `ExecSortInitializeDSM` returns early
    // without inserting any chunk, so this lookup finds nothing and the C leaves
    // `node->shared_info == NULL`.
    let plan_node_id = sort_plan(node)?.plan.plan_node_id;
    let toc = parallel::pwcxt_toc(pwcxt);
    node.am_worker = true;
    match parallel::shm_toc_lookup(toc, plan_node_id as u64, true) {
        None => {
            // Leader was not instrumenting: no shared stats area to attach to.
            node.shared_info = None;
        }
        Some(chunk) => {
            // Attach to the leader's DSM `SharedSortInfo`: recover num_workers
            // from the in-segment header. The worker writes ONLY its own
            // `sinstrument[ParallelWorkerNumber]` slot later (in `ExecSort`).
            let seg = parallel::pwcxt_seg(pwcxt);
            let (hdr, _tail) = shared_dsm_object::attach_flex::<
                SharedSortInfoHeader,
                TuplesortInstrumentation,
            >(seg, chunk, 0);
            let num_workers = hdr.get().num_workers;
            node.shared_info = Some(SharedSortInfo::Dsm {
                chunk,
                seg,
                num_workers,
            });
        }
    }
    Ok(())
}

/// `ExecSortRetrieveInstrumentation(node)` — transfer sort statistics from DSM
/// to private memory.
///
/// `if (node->shared_info == NULL) return;` runs directly on the owned node. The
/// C then `palloc`s a private `SharedSortInfo` and `memcpy`s the DSM bytes into
/// it. With the merged in-process `PgBox<SharedSortInfo>` carrier no DSM
/// round-trip ever happened (see `ExecSortInitializeDSM`), so there are no
/// worker-populated DSM slots to copy out; faithfully closing this needs the
/// DSM-resident carrier the Init paths also need. Same blocker as nodeAgg's
/// `ExecAggRetrieveInstrumentation`; mirror-and-panic until it lands.
pub fn ExecSortRetrieveInstrumentation<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    node: &mut SortStateData<'mcx>,
) -> PgResult<()> {
    //   SharedSortInfo *si;
    //   if (node->shared_info == NULL) return;
    let (chunk, seg, num_workers) = match node.shared_info {
        Some(SharedSortInfo::Dsm {
            chunk,
            seg,
            num_workers,
        }) => (chunk, seg, num_workers),
        // Already a backend-local copy, or NULL: nothing to retrieve.
        _ => return Ok(()),
    };

    //   size = offsetof(SharedSortInfo, sinstrument)
    //          + node->shared_info->num_workers * sizeof(TuplesortInstrumentation);
    //   si = palloc(size); memcpy(si, node->shared_info, size); node->shared_info = si;
    //
    // The DSM segment is still mapped here (the C runs this before detach). Read
    // the flex array out of the segment and snapshot it into a backend-local
    // `PgVec`; `node->shared_info` then becomes the `Local` arm.
    let (_hdr, tail) =
        shared_dsm_object::attach_flex::<SharedSortInfoHeader, TuplesortInstrumentation>(
            seg,
            chunk,
            num_workers as usize,
        );

    let mut copy: PgVec<'mcx, TuplesortInstrumentation> =
        PgVec::with_capacity_in(num_workers as usize, mcx);
    for &elem in tail.get().iter() {
        copy.push(elem);
    }
    node.shared_info = Some(SharedSortInfo::Local {
        num_workers,
        sinstrument: copy,
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Seam shims installed into `backend-executor-nodeSort-seams`.
//
// `execParallel` dispatches the per-node parallel hooks generically, holding a
// `PlanState *` (the opaque [`PlanStateHandle`]); the C `ExecSortEstimate` etc.
// begin with the `(SortState *) node` cast. Recovering the live `SortStateData`
// from the handle is the executor's `PlanState` pointer registry — that
// pointer-table is the unported executor surface (cf. task #165/#169), so each
// shim performs the C cast through `resolve_sort_state` (which panics until that
// registry lands) and then runs the real, owned-typed entry point above. This
// mirrors nodeAgg's `aggapi` shims exactly.
// ---------------------------------------------------------------------------

/// `(SortState *) node` — recover the live `SortStateData` a `PlanStateHandle`
/// refers to. The executor's `PlanState` pointer registry that backs this lookup
/// is not yet ported.
fn resolve_sort_state<'mcx>(_node: PlanStateHandle) -> &'mcx mut SortStateData<'mcx> {
    panic!(
        "backend-executor-nodeSort: resolving a PlanStateHandle to the live SortState needs the \
         executor PlanState pointer registry (unported); the (SortState *) node cast in the \
         ExecSort* parallel hooks cannot run yet"
    );
}

/// `CurrentMemoryContext` (`planstate->state->es_query_cxt`) at the
/// `ExecSortRetrieveInstrumentation` call site — recovered from the same
/// unported executor surface that backs `resolve_sort_state`, so it shares that
/// panic. (The live executor dispatches `ExecSortRetrieveInstrumentation`
/// directly over its owned `SortState` from `ExecParallelRetrieveInstrumentation`,
/// recovering the mcx from the node's EState back-link; this handle-shim path is
/// only reached by a hypothetical pointer-registry caller.)
fn resolve_retrieve_mcx<'mcx>(_node: PlanStateHandle) -> ::mcx::Mcx<'mcx> {
    panic!(
        "backend-executor-nodeSort: the CurrentMemoryContext for \
         ExecSortRetrieveInstrumentation's palloc'd copy is recovered from the unported executor \
         surface (PlanState pointer registry); cannot run yet"
    );
}

/// Seam shim for `ExecSortEstimate`.
fn exec_sort_estimate_shim(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()> {
    ExecSortEstimate(resolve_sort_state(node), pcxt)
}

/// Seam shim for `ExecSortInitializeDSM`.
fn exec_sort_initialize_dsm_shim(
    node: PlanStateHandle,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    ExecSortInitializeDSM(resolve_sort_state(node), pcxt)
}

/// Seam shim for `ExecSortInitializeWorker`.
fn exec_sort_initialize_worker_shim(
    node: PlanStateHandle,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    ExecSortInitializeWorker(resolve_sort_state(node), pwcxt)
}

/// Seam shim for `ExecSortRetrieveInstrumentation`.
fn exec_sort_retrieve_instrumentation_shim(node: PlanStateHandle) -> PgResult<()> {
    ExecSortRetrieveInstrumentation(resolve_retrieve_mcx(node), resolve_sort_state(node))
}

// ===========================================================================
// In-crate helpers.
// ===========================================================================

/// `(Sort *) node->ss.ps.plan` — the Sort plan node the state aliases.
fn sort_plan<'a, 'mcx>(node: &'a SortStateData<'mcx>) -> PgResult<&'a Sort<'mcx>> {
    match node.ss.ps.plan {
        Some(p) => Ok(p.expect_sort()),
        None => Err(missing_plan()),
    }
}

/// `outerPlanState(node)` — `node->ss.ps.lefttree`.
fn outer_plan_state<'a, 'mcx>(
    node: &'a SortStateData<'mcx>,
) -> PgResult<&'a PlanStateNode<'mcx>> {
    node.ss
        .ps
        .lefttree
        .as_deref()
        .ok_or_else(|| missing_outer_plan_state())
}

/// `slot = ExecProcNode(outerNode); TupIsNull(slot) ? None : Some(slot)`.
fn next_outer_slot<'mcx>(
    node: &mut SortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let outer = node
        .ss
        .ps
        .lefttree
        .as_deref_mut()
        .ok_or_else(|| missing_outer_plan_state())?;
    let slot = execProcnode::exec_proc_node::call(outer, estate)?;
    match slot {
        Some(id) if !estate.slot(id).is_empty() => Ok(Some(id)),
        _ => Ok(None),
    }
}

/// `node->shared_info->sinstrument[ParallelWorkerNumber] = stats` — store this
/// worker's tuplesort statistics in its slot of the shared array (the C
/// `tuplesort_get_stats(tuplesortstate, si)` target). The slot index
/// (`ParallelWorkerNumber`) is a fact of the parallel subsystem, so the write
/// goes through its support seam.
fn store_worker_stats<'mcx>(
    node: &mut SortStateData<'mcx>,
    stats: TuplesortInstrumentation,
) -> PgResult<()> {
    //   si = &node->shared_info->sinstrument[ParallelWorkerNumber];
    //   tuplesort_get_stats(tuplesortstate, si);
    //
    // The worker writes ONLY its own slot in the DSM `SharedSortInfo` flex
    // array; the worker is the sole writer of that element, satisfying
    // `with_mut`'s sole-accessor obligation. The worker number is owned by
    // access/parallel.c (the support seam).
    let worker_number = parallel::parallel_worker_number();
    let shared = node
        .shared_info
        .as_ref()
        .expect("caller checked shared_info.is_some()");
    match shared {
        SharedSortInfo::Dsm {
            chunk,
            seg,
            num_workers,
        } => {
            debug_assert!(worker_number >= 0);
            debug_assert!(worker_number <= *num_workers);
            if worker_number < 0 || worker_number >= *num_workers {
                return Err(worker_slot_oob(worker_number as usize, *num_workers as usize));
            }
            let elem = sinstrument_slot_cursor(*chunk, worker_number);
            shared_dsm_object::with_mut::<TuplesortInstrumentation, ()>(*seg, elem, |si| {
                *si = stats;
            });
            Ok(())
        }
        // The worker copyback only runs in a parallel worker whose
        // `ExecSortInitializeWorker` attached the DSM area; a `Local` arm here is
        // impossible (it only exists in the leader after retrieve).
        SharedSortInfo::Local { .. } => {
            Err(worker_slot_oob(worker_number as usize, 0))
        }
    }
}

// --- recoverable errors (internal-error ereports) -------------------------

fn ereport_internal(msg: &'static str) -> ::types_error::PgError {
    ::types_error::PgError::error(msg).with_sqlstate(::types_error::ERRCODE_INTERNAL_ERROR)
}

fn missing_plan() -> ::types_error::PgError {
    ereport_internal("Sort node has no plan back-link")
}
fn missing_outer_plan_state() -> ::types_error::PgError {
    ereport_internal("Sort node has no outer plan state")
}
fn missing_result_slot() -> ::types_error::PgError {
    ereport_internal("Sort node result slot not initialized")
}
fn missing_result_type() -> ::types_error::PgError {
    ereport_internal("Sort outer node result type not set")
}
fn missing_sortkey() -> ::types_error::PgError {
    ereport_internal("Sort plan node has no sort keys")
}
fn worker_slot_oob(_idx: usize, _len: usize) -> ::types_error::PgError {
    ereport_internal("Sort worker instrumentation slot out of range")
}

#[cfg(test)]
mod tests;
