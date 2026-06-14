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

use backend_executor_execAmi_seams as execAmi;
use backend_executor_execParallel_support_seams as parallel_sup;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_access_transam_parallel_seams as parallel;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_init_small_seams as globals;
use backend_utils_sort_tuplesort_seams as tuplesort;

use mcx::{alloc_in, PgBox};
use types_error::PgResult;
use types_execparallel::{
    ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle, Size,
};
use types_nodes::execnodes::{ForwardScanDirection, ScanDirectionIsForward};
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use types_nodes::nodesort::{
    Sort, SortStateData, TuplesortInstrumentation, TUPLESORT_ALLOWBOUNDED, TUPLESORT_NONE,
    TUPLESORT_RANDOMACCESS,
};
use types_nodes::{EStateData, PlanStateNode, SlotId, TupleSlotKind};

/// `offsetof(SharedSortInfo, sinstrument)` (execnodes.h): an `int num_workers`
/// followed by the `TuplesortInstrumentation[]` flexible array, which begins on
/// an 8-byte boundary (`TuplesortInstrumentation` contains an `int64`). The C
/// (and c2rust) use the literal byte offset `8`.
const SHARED_SORT_INFO_HEADER: Size = 8;

/// `sizeof(TuplesortInstrumentation)` — used to size the per-worker shm chunk.
fn sizeof_instrumentation() -> Size {
    core::mem::size_of::<TuplesortInstrumentation>()
}

/// Install this crate's seam implementations. nodeSort owns the inward
/// parallel-instrumentation hooks declared in `backend-executor-nodeSort-seams`
/// (the parallel executor dispatches to them by node tag).
pub fn init_seams() {
    backend_executor_nodeSort_seams::exec_sort_estimate::set(ExecSortEstimate);
    backend_executor_nodeSort_seams::exec_sort_initialize_dsm::set(ExecSortInitializeDSM);
    backend_executor_nodeSort_seams::exec_sort_initialize_worker::set(ExecSortInitializeWorker);
    backend_executor_nodeSort_seams::exec_sort_retrieve_instrumentation::set(
        ExecSortRetrieveInstrumentation,
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
                    execTuples::slot_getsomeattr::call(mcx, estate.slot_mut(slot_id), 1)?;
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
            // The sorted datum is a scalar machine word (`slot->tts_values[0]`);
            // carry it into the canonical value's by-value arm for the store.
            let val = types_tuple::backend_access_common_heaptuple::Datum::from_usize(
                val.as_usize(),
            );
            execTuples::exec_store_first_datum::call(estate, slot, val, is_null)?;
            Ok(true)
        } else {
            execTuples::exec_clear_tuple::call(estate.slot_mut(slot))?;
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
            estate.slot_mut(slot),
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
    node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut eflags: i32,
) -> PgResult<PgBox<'mcx, SortStateData<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let sort: &'mcx Sort<'mcx> = match node {
        types_nodes::nodes::Node::Sort(s) => s,
        other => panic!("castNode(Sort, node) failed: {other:?}"),
    };

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
    execTuples::exec_clear_tuple::call(estate.slot_mut(slot))?;

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
pub fn ExecSortEstimate(
    node: PlanStateHandle,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    //   if (!node->ss.ps.instrument || pcxt->nworkers == 0) return;
    if !parallel_sup::sort_instrument_present::call(node)
        || parallel_sup::pcxt_nworkers::call(pcxt) == 0
    {
        return Ok(());
    }

    //   size = mul_size(pcxt->nworkers, sizeof(TuplesortInstrumentation));
    //   size = add_size(size, offsetof(SharedSortInfo, sinstrument));
    let nworkers = parallel_sup::pcxt_nworkers::call(pcxt);
    let size = (nworkers as Size) * sizeof_instrumentation() + SHARED_SORT_INFO_HEADER;

    //   shm_toc_estimate_chunk(&pcxt->estimator, size);
    //   shm_toc_estimate_keys(&pcxt->estimator, 1);
    parallel_sup::pcxt_estimate_chunk::call(pcxt, size)?;
    parallel_sup::pcxt_estimate_keys::call(pcxt, 1)?;
    Ok(())
}

/// `ExecSortInitializeDSM(node, pcxt)` — initialize DSM space for sort stats.
pub fn ExecSortInitializeDSM(
    node: PlanStateHandle,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    //   if (!node->ss.ps.instrument || pcxt->nworkers == 0) return;
    if !parallel_sup::sort_instrument_present::call(node)
        || parallel_sup::pcxt_nworkers::call(pcxt) == 0
    {
        return Ok(());
    }

    //   size = offsetof(SharedSortInfo, sinstrument)
    //          + pcxt->nworkers * sizeof(TuplesortInstrumentation);
    let nworkers = parallel_sup::pcxt_nworkers::call(pcxt);
    let size = SHARED_SORT_INFO_HEADER + (nworkers as Size) * sizeof_instrumentation();
    let plan_node_id = parallel_sup::sort_plan_node_id::call(node);

    //   node->shared_info = shm_toc_allocate(pcxt->toc, size);
    //   memset(node->shared_info, 0, size);
    //   node->shared_info->num_workers = pcxt->nworkers;
    //   shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, node->shared_info);
    parallel_sup::sort_initialize_dsm_shared_info::call(node, pcxt, nworkers, plan_node_id, size)
}

/// `ExecSortInitializeWorker(node, pwcxt)` — attach a worker to DSM space.
pub fn ExecSortInitializeWorker(
    node: PlanStateHandle,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    //   node->shared_info =
    //       shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, true);
    //   node->am_worker = true;
    let plan_node_id = parallel_sup::sort_plan_node_id::call(node);
    parallel_sup::sort_initialize_worker_shared_info::call(node, pwcxt, plan_node_id)?;
    parallel_sup::sort_set_am_worker::call(node);
    Ok(())
}

/// `ExecSortRetrieveInstrumentation(node)` — transfer sort statistics from DSM
/// to private memory.
pub fn ExecSortRetrieveInstrumentation(node: PlanStateHandle) -> PgResult<()> {
    //   if (node->shared_info == NULL) return;
    if !parallel_sup::sort_shared_info_present::call(node) {
        return Ok(());
    }

    //   size = offsetof(SharedSortInfo, sinstrument)
    //          + node->shared_info->num_workers * sizeof(TuplesortInstrumentation);
    //   si = palloc(size); memcpy(si, node->shared_info, size); node->shared_info = si;
    let num_workers = parallel_sup::sort_shared_info_num_workers::call(node);
    let size = SHARED_SORT_INFO_HEADER + (num_workers as Size) * sizeof_instrumentation();
    parallel_sup::sort_retrieve_shared_info::call(node, size)
}

// ===========================================================================
// In-crate helpers.
// ===========================================================================

/// `(Sort *) node->ss.ps.plan` — the Sort plan node the state aliases.
fn sort_plan<'a, 'mcx>(node: &'a SortStateData<'mcx>) -> PgResult<&'a Sort<'mcx>> {
    match node.ss.ps.plan {
        Some(types_nodes::nodes::Node::Sort(s)) => Ok(s),
        Some(other) => panic!("Sort node's plan back-link is not a Sort: {other:?}"),
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
    // The shared_info container lives in this node's owned tree; the
    // worker-number index belongs to the parallel subsystem. With the live
    // SortState resolved here (we hold &mut node), the write is a direct field
    // store once the worker number is known — but the worker number is owned by
    // access/parallel.c, so it is obtained via the support seam, and the slot
    // is populated here.
    let worker_number = parallel::parallel_worker_number::call();
    let shared = node
        .shared_info
        .as_deref_mut()
        .expect("caller checked shared_info.is_some()");
    debug_assert!(worker_number >= 0);
    debug_assert!(worker_number <= shared.num_workers);
    let idx = worker_number as usize;
    // The shm-allocated array has num_workers slots, all present (zeroed by the
    // DSM init). The owned vector mirrors that: ensure the slot exists.
    if idx < shared.sinstrument.len() {
        shared.sinstrument[idx] = stats;
        Ok(())
    } else {
        Err(worker_slot_oob(idx, shared.sinstrument.len()))
    }
}

// --- recoverable errors (internal-error ereports) -------------------------

fn ereport_internal(msg: &'static str) -> types_error::PgError {
    types_error::PgError::error(msg).with_sqlstate(types_error::ERRCODE_INTERNAL_ERROR)
}

fn missing_plan() -> types_error::PgError {
    ereport_internal("Sort node has no plan back-link")
}
fn missing_outer_plan_state() -> types_error::PgError {
    ereport_internal("Sort node has no outer plan state")
}
fn missing_result_slot() -> types_error::PgError {
    ereport_internal("Sort node result slot not initialized")
}
fn missing_result_type() -> types_error::PgError {
    ereport_internal("Sort outer node result type not set")
}
fn missing_sortkey() -> types_error::PgError {
    ereport_internal("Sort plan node has no sort keys")
}
fn worker_slot_oob(_idx: usize, _len: usize) -> types_error::PgError {
    ereport_internal("Sort worker instrumentation slot out of range")
}

#[cfg(test)]
mod tests;
