//! Port of `src/backend/executor/nodeGatherMerge.c` — scan a plan in multiple
//! workers, and do order-preserving merge.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitGatherMerge`]      - initialize the GatherMerge node.
//! - [`ExecGatherMerge`]          - retrieve the next tuple from the node.
//! - [`ExecEndGatherMerge`]       - shut down the GatherMerge node.
//! - [`ExecShutdownGatherMerge`]  - destroy the parallel-worker setup.
//! - [`ExecReScanGatherMerge`]    - rescan the GatherMerge node.
//!
//! A GatherMerge node launches 1+ parallel workers that each run a copy of the
//! same already-sorted child plan, then merges their output streams — together
//! with the leader's own local copy of the plan — into a single sorted stream.
//! It keeps the head tuple of every participant (leader + workers) in a binary
//! heap keyed on the sort columns and repeatedly emits the heap minimum.
//!
//! The five interface routines, the file-local statics
//! (`ExecShutdownGatherMergeWorkers`, `gather_merge_setup`, `gather_merge_init`,
//! `gather_merge_clear_tuples`, `gather_merge_getnext`, `gather_merge_readnext`,
//! `load_tuple_array`, `gm_readnext_tuple`, `heap_compare_slots`), and the
//! node-local binary heap (`lib/binaryheap.c`, specialized to slot indices: a
//! leaf algorithm with no dependency cycle) are this crate's owned logic.
//! Operations below the executor-node layer go through the owners' seam crates:
//!
//! - interrupt servicing (`CHECK_FOR_INTERRUPTS`) → tcop/postgres;
//! - child dispatch / init / teardown (`ExecProcNode` / `ExecInitNode` /
//!   `ExecEndNode`) → execProcnode; rescan (`ExecReScan`) → execAmi;
//! - per-node expr-context setup/reset, result-type setup, projection
//!   (`ExecAssignExprContext` / `ExecGetResultType` / `ExecInitResultTypeTL` /
//!   `ExecConditionalAssignProjectionInfo`) → execUtils (direct, no cycle) and
//!   `ResetExprContext` / `ExecProject` → execUtils-seams / execExpr-seams;
//! - slot setup/clear/store and attribute access (`ExecInitExtraTupleSlot` /
//!   `ExecClearTuple` / `ExecStoreMinimalTuple` / `slot_getattr`) → execTuples;
//! - sort-key setup and comparison (`PrepareSortSupportFromOrderingOp` /
//!   `ApplySortComparator`) → sortsupport;
//! - the parallel-executor DSM setup, worker launch, reader creation and finish
//!   (`ExecInitParallelPlan` / `ExecParallelReinitialize` /
//!   `LaunchParallelWorkers` / `ExecParallelCreateReaders` /
//!   `ExecParallelFinish` / `ExecParallelCleanup`) → execParallel /
//!   access-transam-parallel; tuple-queue reads (`TupleQueueReaderNext`, which
//!   the owner returns as the `heap_copy_minimal_tuple(tup, 0)` copy the C makes
//!   before buffering) → tqueue; the `bms_add_member` of the rescan param →
//!   nodes/bitmapset; the `parallel_leader_participation` GUC → guc-tables
//!   (direct, no cycle).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use backend_access_transam_parallel_seams as parallel;
use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execParallel_seams as execParallel;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils_seams;
use backend_executor_tqueue_seams as tqueue;
use backend_nodes_core_seams as bitmapset;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_sort_sortsupport_seams as sortsupport;

use mcx::{alloc_in, Mcx, PgBox};
use types_core::primitive::AttrNumber;
use types_datum::Datum;
/// The binary heap's `bh_nodes` (owned by `types-nodes`) now carries the
/// canonical [`Datum`](types_tuple::backend_access_common_heaptuple::Datum)
/// enum. `binaryheap.c` packs an `int32` `SlotNumber` via `Int32GetDatum`, so
/// every heap entry is a `ByVal` slot index here.
use types_tuple::backend_access_common_heaptuple::Datum as HeapDatum;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::nodegathermerge::{
    GMReaderTupleBuffer, GatherMerge, GatherMergeStateData, MAX_TUPLE_STORE,
};
use types_nodes::nodemergeappend::BinaryHeap;
use types_nodes::{
    Bitmapset, EStateData, PlanStateData, PlanStateNode, SlotId, TupleSlotKind,
};
use types_sortsupport::SortSupportData;
use types_tuple::heaptuple::{MinimalTuple, MinimalTupleData};

/// `SlotNumber` (nodeGatherMerge.c) — `typedef int32 SlotNumber;`. A slot /
/// participant index stored in the heap. Provides no formal type-safety; it
/// makes the code self-documenting.
type SlotNumber = i32;

/// `OUTER_VAR` (primnodes.h) — special varno denoting the outer subplan, used
/// by `ExecConditionalAssignProjectionInfo`. `#define OUTER_VAR (-2)`.
const OUTER_VAR: i32 = -2;

/// Install this crate's implementations into its seam slots. nodeGatherMerge
/// has no `<unit>-seams` crate: its functions are reached through the executor
/// dispatch (execProcnode / execAmi), which depend on this crate directly
/// without a cycle.
pub fn init_seams() {}

// ===========================================================================
// Dispatch callback.
// ===========================================================================

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitGatherMerge`]:
/// `castNode(GatherMergeState, pstate)` then run [`ExecGatherMerge`].
fn exec_gather_merge_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::GatherMerge(node) => node,
        other => panic!("castNode(GatherMergeState, pstate) failed: {other:?}"),
    };
    ExecGatherMerge(node, estate)
}

// ===========================================================================
// Interface routines (1:1 with nodeGatherMerge.c).
// ===========================================================================

/// `ExecInitGatherMerge(node, estate, eflags)` — initialize the GatherMerge
/// node.
pub fn ExecInitGatherMerge<'mcx>(
    plan_node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, GatherMergeStateData<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let node: &'mcx GatherMerge<'mcx> = match plan_node {
        types_nodes::nodes::Node::GatherMerge(g) => g,
        other => panic!("castNode(GatherMerge, node) failed: {other:?}"),
    };

    // Gather merge node doesn't have innerPlan node.
    //   Assert(innerPlan(node) == NULL);
    debug_assert!(node.plan.righttree.is_none());

    // create state structure
    //   gm_state = makeNode(GatherMergeState);
    //   gm_state->ps.plan = (Plan *) node;
    //   gm_state->ps.state = estate;
    //   gm_state->ps.ExecProcNode = ExecGatherMerge;
    let mut ps = PlanStateData::default();
    ps.plan = Some(plan_node);
    ps.ExecProcNode = Some(exec_gather_merge_node);

    //   gm_state->initialized = false;
    //   gm_state->gm_initialized = false;
    //   gm_state->tuples_needed = -1;
    // (set on the constructed state below)

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &gm_state->ps);
    backend_executor_execUtils::ExecAssignExprContext(estate, &mut ps)?;

    // GatherMerge doesn't support checking a qual (it's always more efficient to
    // do it in the child node).
    //   Assert(!node->plan.qual);
    debug_assert!(node.plan.qual.is_none());

    // now initialize outer plan
    //   outerNode = outerPlan(node);
    //   outerPlanState(gm_state) = ExecInitNode(outerNode, estate, eflags);
    let outer_plan = node.plan.lefttree.as_deref();
    let outer_plan_state = execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;
    ps.lefttree = outer_plan_state;

    // Leader may access ExecProcNode result directly (if need_to_scan_locally),
    // or from workers via tuple queue.  So we can't trivially rely on the slot
    // type being fixed for expressions evaluated within this node.
    //   gm_state->ps.outeropsset = true;
    //   gm_state->ps.outeropsfixed = false;
    //
    // This repo's trimmed `PlanStateData` omits the `outerops*` slot-type
    // optimization fields (it carries only `scanops*`/`resultops*`), so these
    // two stores have no field to land in; recording the intent that the
    // outer slot ops are non-fixed is a behavior-preserving no-op here.

    // Store the tuple descriptor into gather merge state, so we can use it while
    // initializing the gather merge slots.
    //   tupDesc = ExecGetResultType(outerPlanState(gm_state));
    //   gm_state->tupDesc = tupDesc;
    let tup_desc = {
        let outer = ps
            .lefttree
            .as_deref()
            .expect("ExecInitGatherMerge: outerPlanState is NULL");
        match backend_executor_execUtils::ExecGetResultType(outer.ps_head()) {
            Some(d) => Some(alloc_in(mcx, d.clone_in(mcx)?)?),
            None => None,
        }
    };

    // Initialize result type and projection.
    //   ExecInitResultTypeTL(&gm_state->ps);
    //   ExecConditionalAssignProjectionInfo(&gm_state->ps, tupDesc, OUTER_VAR);
    execTuples::exec_init_result_type_tl::call(&mut ps, estate)?;
    {
        let input_desc = tup_desc
            .as_deref()
            .expect("ExecInitGatherMerge: child result type is NULL");
        backend_executor_execUtils::ExecConditionalAssignProjectionInfo(
            &mut ps, estate, input_desc, OUTER_VAR,
        )?;
    }

    // Without projections result slot type is not trivially known, see comment
    // above.
    //   if (gm_state->ps.ps_ProjInfo == NULL) {
    //       gm_state->ps.resultopsset = true;
    //       gm_state->ps.resultopsfixed = false;
    //   }
    if ps.ps_ProjInfo.is_none() {
        ps.resultopsset = true;
        ps.resultopsfixed = false;
    }

    // initialize sort-key information
    //   if (node->numCols) { ... }
    let num_cols = node.numCols;
    let num_cols_usize =
        usize::try_from(num_cols).map_err(|_| elog_error("GatherMerge has a negative numCols"))?;
    let mut gm_sortkeys: mcx::PgVec<'mcx, SortSupportData<'mcx>> =
        mcx::vec_with_capacity_in(mcx, num_cols_usize)?;
    if num_cols != 0 {
        //   gm_state->gm_nkeys = node->numCols;
        //   gm_state->gm_sortkeys = palloc0(sizeof(SortSupportData) * node->numCols);
        //   for (i = 0; i < node->numCols; i++) { ... }
        for i in 0..num_cols_usize {
            // SortSupport sortKey = gm_state->gm_sortkeys + i;
            // sortKey->ssup_cxt = CurrentMemoryContext;
            let mut sort_key = SortSupportData::new(mcx);
            sort_key.ssup_collation = *node
                .collations
                .get(i)
                .ok_or_else(|| elog_error("GatherMerge collations array too short"))?;
            sort_key.ssup_nulls_first = *node
                .nullsFirst
                .get(i)
                .ok_or_else(|| elog_error("GatherMerge nullsFirst array too short"))?;
            sort_key.ssup_attno = *node
                .sortColIdx
                .get(i)
                .ok_or_else(|| elog_error("GatherMerge sortColIdx array too short"))?;
            // We don't perform abbreviated key conversion here, for the same
            // reasons that it isn't used in MergeAppend.
            //   sortKey->abbreviate = false;
            sort_key.abbreviate = false;

            // PrepareSortSupportFromOrderingOp(node->sortOperators[i], sortKey);
            let ordering_op = *node
                .sortOperators
                .get(i)
                .ok_or_else(|| elog_error("GatherMerge sortOperators array too short"))?;
            sortsupport::prepare_sort_support_from_ordering_op::call(ordering_op, &mut sort_key)?;
            gm_sortkeys.push(sort_key);
        }
    }

    // Now allocate the workspace for gather merge.
    //   gather_merge_setup(gm_state);
    let mut gm_state = alloc_in(
        mcx,
        GatherMergeStateData {
            ps,
            initialized: false,
            gm_initialized: false,
            need_to_scan_locally: false,
            tuples_needed: -1,
            tupDesc: tup_desc,
            gm_nkeys: num_cols,
            gm_sortkeys,
            pei: None,
            nworkers_launched: 0,
            nreaders: 0,
            gm_slots: mcx::vec_with_capacity_in(mcx, 0)?,
            reader: mcx::vec_with_capacity_in(mcx, 0)?,
            gm_tuple_buffers: mcx::vec_with_capacity_in(mcx, 0)?,
            gm_heap: None,
        },
    )?;
    gather_merge_setup(&mut gm_state, node, estate)?;

    Ok(gm_state)
}

/// `ExecGatherMerge(pstate)` — the `PlanState.ExecProcNode` callback: scans the
/// relation via multiple workers and returns the next qualifying tuple.
///
/// 1:1 with `static TupleTableSlot *ExecGatherMerge(PlanState *pstate)`.
/// Returns `None` for `TupIsNull(slot)` (end of scan); otherwise the id of the
/// produced slot (a participant's slot, or the projection's output slot).
pub fn ExecGatherMerge<'mcx>(
    node: &mut GatherMergeStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    tcop_postgres::check_for_interrupts::call()?;

    // As with Gather, we don't launch workers until this node is actually
    // executed.
    if !node.initialized {
        let mcx = estate.es_query_cxt;

        // GatherMerge *gm = castNode(GatherMerge, node->ps.plan);
        //
        // Read the planner-set scalars off the plan up front, then drop the
        // immutable plan borrow so the launch path can take `&mut node`.
        let (num_workers, init_param) = {
            let gm = gather_merge_plan(node);
            (num_workers_of(gm), clone_bitmapset(mcx, gm.initParam.as_deref())?)
        };

        // Sometimes we might have to run without parallelism; but if parallel
        // mode is active then we can try to fire up some workers.
        //   if (gm->num_workers > 0 && estate->es_use_parallel_mode)
        if num_workers > 0 && estate.es_use_parallel_mode {

            // Initialize, or re-initialize, shared state needed by workers.
            //   if (!node->pei)
            //       node->pei = ExecInitParallelPlan(outerPlanState(node),
            //           estate, gm->initParam, gm->num_workers, node->tuples_needed);
            //   else
            //       ExecParallelReinitialize(outerPlanState(node), node->pei,
            //           gm->initParam);
            let tuples_needed = node.tuples_needed;
            if node.pei.is_none() {
                let outer = node
                    .ps
                    .lefttree
                    .as_deref_mut()
                    .ok_or_else(|| elog_error("GatherMerge has no outer plan state"))?;
                let pei = execParallel::exec_init_parallel_plan_owned::call(
                    mcx,
                    outer,
                    estate,
                    init_param.as_deref(),
                    num_workers,
                    tuples_needed,
                )?;
                node.pei = Some(alloc_in(mcx, pei)?);
            } else {
                let outer = node
                    .ps
                    .lefttree
                    .as_deref_mut()
                    .ok_or_else(|| elog_error("GatherMerge has no outer plan state"))?;
                let pei = node
                    .pei
                    .as_deref_mut()
                    .ok_or_else(|| elog_error("GatherMerge has no parallel info"))?;
                execParallel::exec_parallel_reinitialize_owned::call(
                    mcx,
                    outer,
                    pei,
                    init_param.as_deref(),
                )?;
            }

            // Try to launch workers.
            //   pcxt = node->pei->pcxt;
            //   LaunchParallelWorkers(pcxt);
            //   node->nworkers_launched = pcxt->nworkers_launched;
            //   estate->es_parallel_workers_to_launch += pcxt->nworkers_to_launch;
            //   estate->es_parallel_workers_launched += pcxt->nworkers_launched;
            let pcxt = node
                .pei
                .as_deref()
                .and_then(|pei| pei.pcxt)
                .ok_or_else(|| elog_error("GatherMerge parallel context is missing"))?;
            parallel::launch_parallel_workers::call(pcxt)?;
            let nworkers_launched = parallel::pcxt_nworkers_launched::call(pcxt);
            let nworkers_to_launch = parallel::pcxt_nworkers_to_launch::call(pcxt);
            node.nworkers_launched = nworkers_launched;
            estate.es_parallel_workers_to_launch += nworkers_to_launch;
            estate.es_parallel_workers_launched += nworkers_launched;

            // Set up tuple queue readers to read the results.
            //   if (pcxt->nworkers_launched > 0) { ... } else { ... }
            if nworkers_launched > 0 {
                // ExecParallelCreateReaders(node->pei);
                // node->nreaders = pcxt->nworkers_launched;
                // node->reader = palloc(node->nreaders * sizeof(...));
                // memcpy(node->reader, node->pei->reader, ...);
                {
                    let pei = node
                        .pei
                        .as_deref_mut()
                        .ok_or_else(|| elog_error("GatherMerge has no parallel info"))?;
                    execParallel::ExecParallelCreateReaders::call(mcx, pei)?;
                }
                node.nreaders = nworkers_launched;
                let nreaders = usize::try_from(nworkers_launched)
                    .map_err(|_| elog_error("GatherMerge nreaders is negative"))?;
                let mut reader: mcx::PgVec<'mcx, types_execparallel::TupleQueueReaderHandle> =
                    mcx::vec_with_capacity_in(mcx, nreaders)?;
                {
                    let pei = node
                        .pei
                        .as_deref()
                        .ok_or_else(|| elog_error("GatherMerge has no parallel info"))?;
                    for i in 0..nreaders {
                        let r = *pei.reader.get(i).ok_or_else(|| {
                            elog_error("GatherMerge pei->reader array too short")
                        })?;
                        reader.push(r);
                    }
                }
                node.reader = reader;
            } else {
                // No workers?  Then never mind.
                //   node->nreaders = 0;
                //   node->reader = NULL;
                node.nreaders = 0;
                node.reader = mcx::vec_with_capacity_in(mcx, 0)?;
            }
        }

        // allow leader to participate if enabled or no choice
        //   if (parallel_leader_participation || node->nreaders == 0)
        //       node->need_to_scan_locally = true;
        //   node->initialized = true;
        if backend_utils_misc_guc_tables::vars::parallel_leader_participation.read()
            || node.nreaders == 0
        {
            node.need_to_scan_locally = true;
        }
        node.initialized = true;
    }

    // Reset per-tuple memory context to free any expression evaluation storage
    // allocated in the previous tuple cycle.
    //   econtext = node->ps.ps_ExprContext;
    //   ResetExprContext(econtext);
    let econtext = node
        .ps
        .ps_ExprContext
        .ok_or_else(|| elog_error("GatherMerge has no expression context"))?;
    execUtils_seams::reset_expr_context::call(estate, econtext)?;

    // Get next tuple, either from one of our workers, or by running the plan
    // ourselves.
    //   slot = gather_merge_getnext(node);
    //   if (TupIsNull(slot)) return NULL;
    let slot_index = match gather_merge_getnext(node, estate)? {
        Some(i) => i,
        None => return Ok(None),
    };
    let slot_id = slot_at(node, slot_index)?;

    // If no projection is required, we're done.
    //   if (node->ps.ps_ProjInfo == NULL) return slot;
    if node.ps.ps_ProjInfo.is_none() {
        return Ok(Some(slot_id));
    }

    // Form the result tuple using ExecProject(), and return it.
    //   econtext->ecxt_outertuple = slot;
    //   return ExecProject(node->ps.ps_ProjInfo);
    //
    // ExecProject reads `ps_ExprContext->ecxt_outertuple`; the seam installs
    // the leading participant slot there and runs the node's projection.
    set_outer_tuple(estate, econtext, slot_id);
    let out = execExpr::exec_project::call(&mut node.ps, estate)?;
    Ok(Some(out))
}

/// `ExecEndGatherMerge(node)` — free any storage allocated through C routines.
///
/// ```c
/// void ExecEndGatherMerge(GatherMergeState *node)
/// {
///     ExecEndNode(outerPlanState(node));  /* let children clean up first */
///     ExecShutdownGatherMerge(node);
/// }
/// ```
pub fn ExecEndGatherMerge<'mcx>(
    node: &mut GatherMergeStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // let children clean up first
    if let Some(outer) = node.ps.lefttree.as_deref_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }
    ExecShutdownGatherMerge(node)?;
    Ok(())
}

/// `ExecShutdownGatherMerge(node)` — destroy the setup for parallel workers
/// including the parallel context.
///
/// ```c
/// void ExecShutdownGatherMerge(GatherMergeState *node)
/// {
///     ExecShutdownGatherMergeWorkers(node);
///     if (node->pei != NULL) {
///         ExecParallelCleanup(node->pei);
///         node->pei = NULL;
///     }
/// }
/// ```
pub fn ExecShutdownGatherMerge<'mcx>(node: &mut GatherMergeStateData<'mcx>) -> PgResult<()> {
    exec_shutdown_gather_merge_workers(node)?;

    // Now destroy the parallel context.
    if node.pei.is_some() {
        let pei = node
            .pei
            .as_deref_mut()
            .ok_or_else(|| elog_error("GatherMerge has no parallel info"))?;
        execParallel::ExecParallelCleanup::call(pei)?;
        node.pei = None;
    }
    Ok(())
}

/// `ExecShutdownGatherMergeWorkers(node)` — stop all the parallel workers.
///
/// ```c
/// static void ExecShutdownGatherMergeWorkers(GatherMergeState *node)
/// {
///     if (node->pei != NULL)
///         ExecParallelFinish(node->pei);
///     /* Flush local copy of reader array */
///     if (node->reader)
///         pfree(node->reader);
///     node->reader = NULL;
/// }
/// ```
fn exec_shutdown_gather_merge_workers<'mcx>(
    node: &mut GatherMergeStateData<'mcx>,
) -> PgResult<()> {
    if node.pei.is_some() {
        let pei = node
            .pei
            .as_deref_mut()
            .ok_or_else(|| elog_error("GatherMerge has no parallel info"))?;
        execParallel::ExecParallelFinish::call(pei)?;
    }

    // Flush local copy of reader array. The C `pfree(node->reader)` becomes a
    // clear of the owned `PgVec` (the boxed readers are released); the
    // subsequent `node->reader = NULL` is the empty `PgVec`.
    node.reader.clear();
    Ok(())
}

/// `ExecReScanGatherMerge(node)` — prepare to re-scan the result of a
/// GatherMerge.
///
/// 1:1 with `void ExecReScanGatherMerge(GatherMergeState *node)`.
pub fn ExecReScanGatherMerge<'mcx>(
    node: &mut GatherMergeStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // GatherMerge *gm = (GatherMerge *) node->ps.plan;
    let rescan_param = gather_merge_plan(node).rescan_param;

    // Make sure any existing workers are gracefully shut down.
    exec_shutdown_gather_merge_workers(node)?;

    // Free any unused tuples, so we don't leak memory across rescans.
    gather_merge_clear_tuples(node, estate)?;

    // Mark node so that shared state will be rebuilt at next call.
    //   node->initialized = false;
    //   node->gm_initialized = false;
    node.initialized = false;
    node.gm_initialized = false;

    // Set child node's chgParam to tell it that the next scan might deliver a
    // different set of rows within the leader process.
    //   if (gm->rescan_param >= 0)
    //       outerPlan->chgParam =
    //           bms_add_member(outerPlan->chgParam, gm->rescan_param);
    if rescan_param >= 0 {
        let outer = node
            .ps
            .lefttree
            .as_deref_mut()
            .ok_or_else(|| elog_error("GatherMerge has no outer plan state"))?;
        let outer_ps = outer.ps_head_mut();
        let chg = outer_ps.chgParam.take();
        outer_ps.chgParam = Some(bitmapset::bms_add_member::call(mcx, chg, rescan_param)?);
    }

    // If chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.  Note: because this does nothing if we have a rescan_param,
    // it's currently guaranteed that parallel-aware child nodes will not see a
    // ReScan call until after they get a ReInitializeDSM call.
    //   if (outerPlan->chgParam == NULL)
    //       ExecReScan(outerPlan);
    let chgparam_is_empty = node
        .ps
        .lefttree
        .as_deref()
        .map(|o| o.ps_head().chgParam.is_none())
        .unwrap_or(true);
    if chgparam_is_empty {
        let outer = node
            .ps
            .lefttree
            .as_deref_mut()
            .ok_or_else(|| elog_error("GatherMerge has no outer plan state"))?;
        execAmi::exec_re_scan::call(outer, estate)?;
    }
    Ok(())
}

// ===========================================================================
// Node-private statics (1:1 with nodeGatherMerge.c).
// ===========================================================================

/// `gather_merge_setup(gm_state)` — set up the data structures we'll need for
/// Gather Merge.
///
/// In `gm_slots[]`, index 0 is for the leader, indexes 1..=n for workers. The
/// values placed into `gm_heap` correspond to indexes in `gm_slots[]`. The
/// `gm_tuple_buffers[]` array is indexed 0..n-1; it has no entry for the leader.
fn gather_merge_setup<'mcx>(
    gm_state: &mut GatherMergeStateData<'mcx>,
    gm: &GatherMerge<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // int nreaders = gm->num_workers;
    let nreaders = gm.num_workers;
    let nreaders_usize =
        usize::try_from(nreaders).map_err(|_| elog_error("GatherMerge has a negative num_workers"))?;

    // Allocate gm_slots for the number of workers + one more slot for leader.
    //   gm_state->gm_slots = palloc0((nreaders + 1) * sizeof(TupleTableSlot *));
    let mut gm_slots: mcx::PgVec<'mcx, Option<SlotId>> =
        mcx::vec_with_capacity_in(mcx, nreaders_usize + 1)?;
    for _ in 0..(nreaders_usize + 1) {
        gm_slots.push(None);
    }

    // Allocate the tuple slot and tuple array for each worker.
    //   gm_state->gm_tuple_buffers = palloc0(nreaders * sizeof(GMReaderTupleBuffer));
    let mut gm_tuple_buffers: mcx::PgVec<'mcx, GMReaderTupleBuffer<'mcx>> =
        mcx::vec_with_capacity_in(mcx, nreaders_usize)?;

    //   for (i = 0; i < nreaders; i++) { ... }
    for i in 0..nreaders_usize {
        // Allocate the tuple array with length MAX_TUPLE_STORE.
        //   gm_state->gm_tuple_buffers[i].tuple = palloc0(sizeof(MinimalTuple) * MAX_TUPLE_STORE);
        let mut tuple: mcx::PgVec<'mcx, MinimalTuple<'mcx>> =
            mcx::vec_with_capacity_in(mcx, MAX_TUPLE_STORE as usize)?;
        for _ in 0..MAX_TUPLE_STORE {
            tuple.push(None);
        }
        gm_tuple_buffers.push(GMReaderTupleBuffer {
            tuple,
            nTuples: 0,
            readCounter: 0,
            done: false,
        });

        // Initialize tuple slot for worker.
        //   gm_state->gm_slots[i + 1] =
        //       ExecInitExtraTupleSlot(gm_state->ps.state, gm_state->tupDesc,
        //                              &TTSOpsMinimalTuple);
        let tup_desc = match &gm_state.tupDesc {
            Some(d) => Some(alloc_in(mcx, d.clone_in(mcx)?)?),
            None => None,
        };
        let slot = execTuples::exec_init_extra_tuple_slot::call(
            estate,
            tup_desc,
            TupleSlotKind::MinimalTuple,
        )?;
        gm_slots[i + 1] = Some(slot);
    }
    gm_state.gm_tuple_buffers = gm_tuple_buffers;
    gm_state.gm_slots = gm_slots;

    // Allocate the resources for the merge.
    //   gm_state->gm_heap = binaryheap_allocate(nreaders + 1, heap_compare_slots, gm_state);
    let heap = BinaryHeap::allocate(mcx, nreaders_usize + 1)?;
    gm_state.gm_heap = Some(alloc_in(mcx, heap)?);
    Ok(())
}

/// `gather_merge_init(gm_state)` — initialize the Gather Merge.
///
/// Reset data structures to ensure they're empty. Then pull at least one tuple
/// from leader + each worker (or set its "done" indicator), and set up the
/// heap. 1:1 with nodeGatherMerge.c (including the `reread` goto loop).
fn gather_merge_init<'mcx>(
    gm_state: &mut GatherMergeStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // int nreaders = gm_state->nreaders;
    let nreaders = gm_state.nreaders;
    // bool nowait = true;
    let mut nowait = true;

    // Assert that gather_merge_setup made enough space.
    //   Assert(nreaders <= castNode(GatherMerge, gm_state->ps.plan)->num_workers);
    debug_assert!(nreaders <= gather_merge_plan(gm_state).num_workers);

    // Reset leader's tuple slot to empty.
    //   gm_state->gm_slots[0] = NULL;
    gm_state.gm_slots[0] = None;

    // Reset the tuple slot and tuple array for each worker.
    //   for (i = 0; i < nreaders; i++) { ... }
    let mut i: i32 = 0;
    while i < nreaders {
        // Reset tuple array to empty / done flag to not-done.
        //   gm_state->gm_tuple_buffers[i].nTuples = 0;
        //   gm_state->gm_tuple_buffers[i].readCounter = 0;
        //   gm_state->gm_tuple_buffers[i].done = false;
        let buf = &mut gm_state.gm_tuple_buffers[i as usize];
        buf.nTuples = 0;
        buf.readCounter = 0;
        buf.done = false;
        // Ensure output slot is empty.
        //   ExecClearTuple(gm_state->gm_slots[i + 1]);
        clear_slot(gm_state, estate, (i + 1) as usize)?;
        i += 1;
    }

    // Reset binary heap to empty.
    //   binaryheap_reset(gm_state->gm_heap);
    binaryheap_reset(heap_mut(gm_state)?);

    // First, try to read a tuple from each worker (including leader) in nowait
    // mode.  After this, if not all workers were able to produce a tuple (or a
    // "done" indication), then re-read from remaining workers, this time using
    // wait mode.  Add all live readers to the heap.
    //
    // reread:
    'reread: loop {
        //   for (i = 0; i <= nreaders; i++) { ... }
        let mut i: i32 = 0;
        while i <= nreaders {
            tcop_postgres::check_for_interrupts::call()?;

            // skip this source if already known done
            //   if ((i == 0) ? gm_state->need_to_scan_locally :
            //       !gm_state->gm_tuple_buffers[i - 1].done)
            let live = if i == 0 {
                gm_state.need_to_scan_locally
            } else {
                !gm_state.gm_tuple_buffers[(i - 1) as usize].done
            };
            if live {
                if slot_is_null(gm_state, estate, i as usize) {
                    // Don't have a tuple yet, try to get one.
                    //   if (gather_merge_readnext(gm_state, i, nowait))
                    //       binaryheap_add_unordered(gm_state->gm_heap, Int32GetDatum(i));
                    if gather_merge_readnext(gm_state, i, nowait, estate)? {
                        binaryheap_add_unordered(heap_mut(gm_state)?, HeapDatum::from_i32(i))?;
                    }
                } else {
                    // We already got at least one tuple from this worker, but
                    // might as well see if it has any more ready by now.
                    //   load_tuple_array(gm_state, i);
                    load_tuple_array(gm_state, i, estate)?;
                }
            }
            i += 1;
        }

        // need not recheck leader, since nowait doesn't matter for it
        //   for (i = 1; i <= nreaders; i++) { ... }
        let mut goto_reread = false;
        let mut i: i32 = 1;
        while i <= nreaders {
            if !gm_state.gm_tuple_buffers[(i - 1) as usize].done
                && slot_is_null(gm_state, estate, i as usize)
            {
                nowait = false;
                goto_reread = true;
                break;
            }
            i += 1;
        }
        if goto_reread {
            continue 'reread;
        }
        break;
    }

    // Now heapify the heap.
    //   binaryheap_build(gm_state->gm_heap);
    binaryheap_build_node(gm_state, estate)?;

    //   gm_state->gm_initialized = true;
    gm_state.gm_initialized = true;
    Ok(())
}

/// `gather_merge_clear_tuples(gm_state)` — clear out the tuple table slot, and
/// any unused pending tuples, for each gather merge input.
///
/// ```c
/// static void gather_merge_clear_tuples(GatherMergeState *gm_state)
/// {
///     for (i = 0; i < gm_state->nreaders; i++) {
///         GMReaderTupleBuffer *tuple_buffer = &gm_state->gm_tuple_buffers[i];
///         while (tuple_buffer->readCounter < tuple_buffer->nTuples)
///             pfree(tuple_buffer->tuple[tuple_buffer->readCounter++]);
///         ExecClearTuple(gm_state->gm_slots[i + 1]);
///     }
/// }
/// ```
fn gather_merge_clear_tuples<'mcx>(
    gm_state: &mut GatherMergeStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let nreaders = gm_state.nreaders;
    let mut i: i32 = 0;
    while i < nreaders {
        // The C `pfree` of each unread buffered tuple becomes a drop of the
        // owned `MinimalTuple` (replacing the slot with `None` releases it).
        let buf = &mut gm_state.gm_tuple_buffers[i as usize];
        while buf.readCounter < buf.nTuples {
            let rc = buf.readCounter;
            buf.tuple[rc as usize] = None;
            buf.readCounter = rc + 1;
        }

        //   ExecClearTuple(gm_state->gm_slots[i + 1]);
        clear_slot(gm_state, estate, (i + 1) as usize)?;
        i += 1;
    }
    Ok(())
}

/// `gather_merge_getnext(gm_state)` — read the next tuple for gather merge,
/// fetching the sorted tuple out of the heap. Returns the *slot index* of the
/// leading participant, or `None` when all queues and the heap are exhausted.
///
/// 1:1 with nodeGatherMerge.c.
fn gather_merge_getnext<'mcx>(
    gm_state: &mut GatherMergeStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotNumber>> {
    if !gm_state.gm_initialized {
        // First time through: pull the first tuple from each participant, and
        // set up the heap.
        //   gather_merge_init(gm_state);
        gather_merge_init(gm_state, estate)?;
    } else {
        // Otherwise, pull the next tuple from whichever participant we returned
        // from last time, and reinsert that participant's index into the heap,
        // because it might now compare differently against the other elements.
        //   i = DatumGetInt32(binaryheap_first(gm_state->gm_heap));
        let i = binaryheap_first(heap_ref(gm_state)?)?.as_i32();

        //   if (gather_merge_readnext(gm_state, i, false))
        //       binaryheap_replace_first(gm_state->gm_heap, Int32GetDatum(i));
        //   else
        //       (void) binaryheap_remove_first(gm_state->gm_heap);
        if gather_merge_readnext(gm_state, i, false, estate)? {
            binaryheap_replace_first_node(gm_state, HeapDatum::from_i32(i), estate)?;
        } else {
            // reader exhausted, remove it from heap
            binaryheap_remove_first_node(gm_state, estate)?;
        }
    }

    if binaryheap_empty(heap_ref(gm_state)?) {
        // All the queues are exhausted, and so is the heap.
        //   gather_merge_clear_tuples(gm_state);
        //   return NULL;
        gather_merge_clear_tuples(gm_state, estate)?;
        Ok(None)
    } else {
        // Return next tuple from whichever participant has the leading one.
        //   i = DatumGetInt32(binaryheap_first(gm_state->gm_heap));
        //   return gm_state->gm_slots[i];
        let i = binaryheap_first(heap_ref(gm_state)?)?.as_i32();
        Ok(Some(i))
    }
}

/// `load_tuple_array(gm_state, reader)` — read tuple(s) for the given reader in
/// nowait mode, and load into its tuple array, until we have `MAX_TUPLE_STORE`
/// of them or would have to block. 1:1 with nodeGatherMerge.c.
fn load_tuple_array<'mcx>(
    gm_state: &mut GatherMergeStateData<'mcx>,
    reader: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Don't do anything if this is the leader.
    //   if (reader == 0) return;
    if reader == 0 {
        return Ok(());
    }

    // tuple_buffer = &gm_state->gm_tuple_buffers[reader - 1];
    let bufidx = (reader - 1) as usize;

    // If there's nothing in the array, reset the counters to zero.
    //   if (tuple_buffer->nTuples == tuple_buffer->readCounter)
    //       tuple_buffer->nTuples = tuple_buffer->readCounter = 0;
    {
        let buf = &mut gm_state.gm_tuple_buffers[bufidx];
        if buf.nTuples == buf.readCounter {
            buf.readCounter = 0;
            buf.nTuples = 0;
        }
    }

    // Try to fill additional slots in the array.
    //   for (i = tuple_buffer->nTuples; i < MAX_TUPLE_STORE; i++) { ... }
    let mut i: i32 = gm_state.gm_tuple_buffers[bufidx].nTuples;
    while i < MAX_TUPLE_STORE {
        // tuple = gm_readnext_tuple(gm_state, reader, true, &tuple_buffer->done);
        let mut done = gm_state.gm_tuple_buffers[bufidx].done;
        let tuple = gm_readnext_tuple(gm_state, reader, true, &mut done, estate)?;
        gm_state.gm_tuple_buffers[bufidx].done = done;
        //   if (!tuple) break;
        let tuple = match tuple {
            Some(t) => t,
            None => break,
        };
        //   tuple_buffer->tuple[i] = tuple;
        //   tuple_buffer->nTuples++;
        let buf = &mut gm_state.gm_tuple_buffers[bufidx];
        buf.tuple[i as usize] = Some(tuple);
        buf.nTuples += 1;
        i += 1;
    }
    Ok(())
}

/// `gather_merge_readnext(gm_state, reader, nowait)` — store the next tuple for
/// a given reader into the appropriate slot. Returns `true` on success, `false`
/// if not (reader exhausted, or we didn't want to wait). Sets the done flag if
/// the reader is found exhausted. 1:1 with nodeGatherMerge.c.
fn gather_merge_readnext<'mcx>(
    gm_state: &mut GatherMergeStateData<'mcx>,
    reader: i32,
    nowait: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // If we're being asked to generate a tuple from the leader, then we just
    // call ExecProcNode as normal to produce one.
    //   if (reader == 0) { ... return false; }
    if reader == 0 {
        if gm_state.need_to_scan_locally {
            // Install our DSA area while executing the plan.
            //   estate->es_query_dsa = gm_state->pei ? gm_state->pei->area : NULL;
            //   outerTupleSlot = ExecProcNode(outerPlan);
            //   estate->es_query_dsa = NULL;
            let saved_dsa = estate.es_query_dsa;
            estate.es_query_dsa = gm_state.pei.as_deref().and_then(|pei| pei.area);
            let outer_tuple_slot = {
                let outer = gm_state
                    .ps
                    .lefttree
                    .as_deref_mut()
                    .ok_or_else(|| elog_error("GatherMerge has no outer plan state"))?;
                execProcnode::exec_proc_node::call(outer, estate)?
            };
            // C unconditionally writes `estate->es_query_dsa = NULL`; the owned
            // form restores the prior value (always NULL here, matching C).
            estate.es_query_dsa = saved_dsa;

            //   if (!TupIsNull(outerTupleSlot)) {
            //       gm_state->gm_slots[0] = outerTupleSlot;
            //       return true;
            //   }
            //   /* need_to_scan_locally serves as "done" flag for leader */
            //   gm_state->need_to_scan_locally = false;
            if !tup_is_null(outer_tuple_slot, estate) {
                gm_state.gm_slots[0] = outer_tuple_slot;
                return Ok(true);
            }
            gm_state.need_to_scan_locally = false;
        }
        return Ok(false);
    }

    // Otherwise, check the state of the relevant tuple buffer.
    //   tuple_buffer = &gm_state->gm_tuple_buffers[reader - 1];
    let bufidx = (reader - 1) as usize;

    let tup: MinimalTuple<'mcx>;
    let n_tuples = gm_state.gm_tuple_buffers[bufidx].nTuples;
    let read_counter = gm_state.gm_tuple_buffers[bufidx].readCounter;
    if n_tuples > read_counter {
        // Return any tuple previously read that is still buffered.
        //   tup = tuple_buffer->tuple[tuple_buffer->readCounter++];
        let buf = &mut gm_state.gm_tuple_buffers[bufidx];
        let rc = buf.readCounter;
        tup = buf.tuple[rc as usize].take();
        buf.readCounter = rc + 1;
    } else if gm_state.gm_tuple_buffers[bufidx].done {
        // Reader is known to be exhausted.
        //   return false;
        return Ok(false);
    } else {
        // Read and buffer next tuple.
        //   tup = gm_readnext_tuple(gm_state, reader, nowait, &tuple_buffer->done);
        //   if (!tup) return false;
        let mut done = gm_state.gm_tuple_buffers[bufidx].done;
        let read = gm_readnext_tuple(gm_state, reader, nowait, &mut done, estate)?;
        gm_state.gm_tuple_buffers[bufidx].done = done;
        let read = match read {
            Some(t) => t,
            None => return Ok(false),
        };
        tup = Some(read);

        // Attempt to read more tuples in nowait mode and store them in the
        // pending-tuple array for the reader.
        //   load_tuple_array(gm_state, reader);
        load_tuple_array(gm_state, reader, estate)?;
    }

    //   Assert(tup);
    debug_assert!(tup.is_some());

    // Build the TupleTableSlot for the given tuple.
    //   ExecStoreMinimalTuple(tup, gm_state->gm_slots[reader], true);
    let slot_id = gm_state.gm_slots[reader as usize].ok_or_else(|| {
        elog_error("gather_merge_readnext: worker slot must have been allocated by gather_merge_setup")
    })?;
    execTuples::exec_store_minimal_tuple::call(estate, tup, slot_id, true)?;

    Ok(true)
}

/// `gm_readnext_tuple(gm_state, nreader, nowait, done)` — attempt to read a
/// tuple from the given worker. 1:1 with nodeGatherMerge.c.
fn gm_readnext_tuple<'mcx>(
    gm_state: &mut GatherMergeStateData<'mcx>,
    nreader: i32,
    nowait: bool,
    done: &mut bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    let mcx = estate.es_query_cxt;

    // Check for async events, particularly messages from workers.
    tcop_postgres::check_for_interrupts::call()?;

    // Attempt to read a tuple.
    //
    // Note that TupleQueueReaderNext will just return NULL for a worker which
    // fails to initialize.  We'll treat that worker as having produced no
    // tuples; WaitForParallelWorkersToFinish will error out when we get there.
    //   reader = gm_state->reader[nreader - 1];
    //   tup = TupleQueueReaderNext(reader, nowait, done);
    let reader = *gm_state.reader.get((nreader - 1) as usize).ok_or_else(|| {
        elog_error("gm_readnext_tuple: active reader must be present for a launched worker")
    })?;
    // The landed tqueue contract returns the next tuple's on-wire minimal-tuple
    // byte image (`None` once exhausted / would-block) plus the C `*done`
    // out-parameter (true once the queue is detached). Mirror the C
    // `TupleQueueReaderNext(reader, nowait, done)` call, propagating `*done`.
    let (bytes, reader_done) = tqueue::tuple_queue_reader_next::call(reader, nowait)?;
    *done = reader_done;

    // Since we'll be buffering these across multiple calls, we need to make a
    // copy.
    //   return tup ? heap_copy_minimal_tuple(tup, 0) : NULL;
    //
    // The C copies the queue-memory pointer with `heap_copy_minimal_tuple`
    // before buffering it. Here the owner returns a copy of the wire bytes; we
    // reassemble the owned `MinimalTuple` (leading `t_len` word + body) into
    // `mcx`, which is the buffer-lived copy the C makes. `None` (no tuple)
    // passes through as the NULL return.
    match bytes {
        None => Ok(None),
        Some(image) => {
            // image == MinimalTupleData::to_minimal_bytes(): the leading 4-byte
            // `t_len` word followed by the body the reader re-stores.
            let t_len = u32::from_ne_bytes([image[0], image[1], image[2], image[3]]);
            let body = &image[core::mem::size_of::<u32>()..];
            let mtup = MinimalTupleData::from_minimal_parts(mcx, t_len, body)?;
            Ok(Some(alloc_in(mcx, mtup)?))
        }
    }
}

/// `heap_compare_slots(a, b, arg)` — compare the tuples in the two given slots,
/// for the binary heap. Returns the comparison inverted
/// (`INVERT_COMPARE_RESULT`) because the heap is a max-heap but we want the
/// smallest tuple at the top.
///
/// 1:1 with `static int32 heap_compare_slots(Datum a, Datum b, void *arg)`.
/// `slots`/`sortkeys` are borrowed from the node (the C `arg` is the
/// `GatherMergeState *`); the heap operations split this borrow off `gm_heap`.
fn heap_compare_slots<'mcx>(
    slots: &[Option<SlotId>],
    sortkeys: &[SortSupportData<'mcx>],
    a: HeapDatum<'_>,
    b: HeapDatum<'_>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<i32> {
    // SlotNumber slot1 = DatumGetInt32(a); SlotNumber slot2 = DatumGetInt32(b);
    let slot1: SlotNumber = a.as_i32();
    let slot2: SlotNumber = b.as_i32();
    let i1 = usize::try_from(slot1).map_err(|_| elog_error("GatherMerge heap slot index is negative"))?;
    let i2 = usize::try_from(slot2).map_err(|_| elog_error("GatherMerge heap slot index is negative"))?;
    let id1 = slots
        .get(i1)
        .copied()
        .flatten()
        .ok_or_else(|| elog_error("GatherMerge compare slot is empty"))?;
    let id2 = slots
        .get(i2)
        .copied()
        .flatten()
        .ok_or_else(|| elog_error("GatherMerge compare slot is empty"))?;

    //   for (nkey = 0; nkey < node->gm_nkeys; nkey++) { ... }
    for sort_key in sortkeys {
        // AttrNumber attno = sortKey->ssup_attno;
        let attno: AttrNumber = sort_key.ssup_attno;

        // datum1 = slot_getattr(s1, attno, &isNull1);
        let a1 = execTuples::slot_getattr_by_id::call(estate, id1, attno)?;
        // datum2 = slot_getattr(s2, attno, &isNull2);
        let a2 = execTuples::slot_getattr_by_id::call(estate, id2, attno)?;

        // compare = ApplySortComparator(datum1, isNull1, datum2, isNull2, sortKey);
        let mut compare =
            ApplySortComparator(a1.value, a1.isnull, a2.value, a2.isnull, sort_key)?;
        if compare != 0 {
            // INVERT_COMPARE_RESULT(compare); return compare;
            compare = INVERT_COMPARE_RESULT(compare);
            return Ok(compare);
        }
    }
    Ok(0)
}

// ===========================================================================
// In-crate binary-heap library (lib/binaryheap.c), specialized to the node's
// slot-index comparator. A leaf algorithm: no dependency cycle, so it is
// implemented here rather than seamed (mirrors nodeMergeAppend).
// ===========================================================================

/// `binaryheap_reset(heap)` — reset the heap to empty, keeping its capacity.
fn binaryheap_reset(heap: &mut BinaryHeap<'_>) {
    heap.bh_size = 0;
    heap.bh_has_heap_property = true;
    heap.bh_nodes.clear();
}

/// `binaryheap_empty(h)` — true if the heap has no entries.
fn binaryheap_empty(heap: &BinaryHeap<'_>) -> bool {
    heap.bh_size == 0
}

/// `binaryheap_add_unordered(heap, d)` — add `d` at the end without restoring
/// the heap property (paired with [`binaryheap_build_node`]). An overflow is
/// the C `elog(ERROR, "out of binary heap slots")`.
fn binaryheap_add_unordered<'mcx>(heap: &mut BinaryHeap<'mcx>, d: HeapDatum<'mcx>) -> PgResult<()> {
    if heap.bh_size >= heap.bh_space {
        return Err(elog_error("out of binary heap slots"));
    }
    heap.bh_has_heap_property = false;
    heap.bh_nodes.push(d);
    heap.bh_size += 1;
    Ok(())
}

/// `binaryheap_first(heap)` — peek at the heap's top (root) entry. The caller
/// must ensure the heap is non-empty.
fn binaryheap_first<'mcx>(heap: &BinaryHeap<'mcx>) -> PgResult<HeapDatum<'mcx>> {
    heap.bh_nodes
        .first()
        .cloned()
        .ok_or_else(|| elog_error("binaryheap_first on empty heap"))
}

/// `binaryheap_remove_first(heap)` over the node — remove the top entry,
/// rebalancing with [`sift_down`]. Splits the node borrow for the comparator.
fn binaryheap_remove_first_node<'mcx>(
    gm_state: &mut GatherMergeStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<HeapDatum<'mcx>> {
    let mut heap = gm_state
        .gm_heap
        .take()
        .ok_or_else(|| elog_error("GatherMerge has no binary heap"))?;
    let result = (|| {
        if binaryheap_empty(&heap) {
            return Err(elog_error("binaryheap_remove_first on empty heap"));
        }
        // extract the root node, which will be the result
        let result = heap.bh_nodes[0].clone();

        // easy if heap contains one element
        if heap.bh_size == 1 {
            heap.bh_size -= 1;
            heap.bh_nodes.pop();
            return Ok(result);
        }

        // Remove the last node, placing it in the vacated root entry, and sift
        // the new root node down to its correct position.
        heap.bh_size -= 1;
        let last = heap
            .bh_nodes
            .pop()
            .ok_or_else(|| elog_error("binaryheap underflow"))?;
        heap.bh_nodes[0] = last;
        sift_down(&mut heap, 0, &gm_state.gm_slots, &gm_state.gm_sortkeys, estate)?;
        Ok(result)
    })();
    gm_state.gm_heap = Some(heap);
    result
}

/// `binaryheap_build(heap)` over the node — assemble a valid heap in O(n) from
/// the nodes added by [`binaryheap_add_unordered`], using [`heap_compare_slots`].
fn binaryheap_build_node<'mcx>(
    gm_state: &mut GatherMergeStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut heap = gm_state
        .gm_heap
        .take()
        .ok_or_else(|| elog_error("GatherMerge has no binary heap"))?;
    let result = (|| {
        // for (i = parent_offset(heap->bh_size - 1); i >= 0; i--) sift_down(heap, i);
        if heap.bh_size >= 1 {
            let start = parent_offset(heap.bh_size - 1);
            let mut i = start;
            while i >= 0 {
                sift_down(&mut heap, i, &gm_state.gm_slots, &gm_state.gm_sortkeys, estate)?;
                i -= 1;
            }
        }
        heap.bh_has_heap_property = true;
        Ok(())
    })();
    gm_state.gm_heap = Some(heap);
    result
}

/// `binaryheap_replace_first(heap, d)` over the node — replace the topmost
/// element and re-heapify with [`sift_down`].
fn binaryheap_replace_first_node<'mcx>(
    gm_state: &mut GatherMergeStateData<'mcx>,
    d: HeapDatum<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut heap = gm_state
        .gm_heap
        .take()
        .ok_or_else(|| elog_error("GatherMerge has no binary heap"))?;
    let result = (|| {
        if binaryheap_empty(&heap) {
            return Err(elog_error("binaryheap_replace_first on empty heap"));
        }
        heap.bh_nodes[0] = d;
        if heap.bh_size > 1 {
            sift_down(&mut heap, 0, &gm_state.gm_slots, &gm_state.gm_sortkeys, estate)?;
        }
        Ok(())
    })();
    gm_state.gm_heap = Some(heap);
    result
}

/// Offset of the parent of the node at index `i`.
fn parent_offset(i: i32) -> i32 {
    (i - 1) / 2
}

/// Offset of the left child of the node at index `i`.
fn left_offset(i: i32) -> i32 {
    2 * i + 1
}

/// Offset of the right child of the node at index `i`.
fn right_offset(i: i32) -> i32 {
    2 * i + 2
}

/// `sift_down(heap, node_off)` — sift a node down from its current position to
/// satisfy the heap property, using [`heap_compare_slots`]. 1:1 with
/// `lib/binaryheap.c`'s `sift_down`.
fn sift_down<'mcx>(
    heap: &mut BinaryHeap<'mcx>,
    node_off: i32,
    slots: &[Option<SlotId>],
    sortkeys: &[SortSupportData<'mcx>],
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut node_off = node_off;
    let node_val = heap.bh_nodes[node_off as usize].clone();

    loop {
        let left_off = left_offset(node_off);
        let right_off = right_offset(node_off);
        let mut swap_off = left_off;

        // Is the right child larger than the left child?
        if right_off < heap.bh_size {
            let left_val = heap.bh_nodes[left_off as usize].clone();
            let right_val = heap.bh_nodes[right_off as usize].clone();
            if heap_compare_slots(slots, sortkeys, left_val, right_val, estate)? < 0 {
                swap_off = right_off;
            }
        }

        // If no children or parent is >= the larger child, heap condition is
        // satisfied, and we're done.
        if left_off >= heap.bh_size {
            break;
        }
        let swap_val = heap.bh_nodes[swap_off as usize].clone();
        if heap_compare_slots(slots, sortkeys, node_val.clone(), swap_val, estate)? >= 0 {
            break;
        }

        // Otherwise, swap the hole with the child that violates the heap
        // property; then go on to check its children.
        heap.bh_nodes[node_off as usize] = heap.bh_nodes[swap_off as usize].clone();
        node_off = swap_off;
    }
    // Re-fill the hole.
    heap.bh_nodes[node_off as usize] = node_val;
    Ok(())
}

// ===========================================================================
// Small in-crate node-layer helpers.
// ===========================================================================

/// `castNode(GatherMerge, gm_state->ps.plan)` — the node's concrete plan.
fn gather_merge_plan<'a, 'mcx>(gm_state: &'a GatherMergeStateData<'mcx>) -> &'a GatherMerge<'mcx> {
    match gm_state
        .ps
        .plan
        .as_deref()
        .expect("GatherMergeState has no plan")
    {
        types_nodes::nodes::Node::GatherMerge(g) => g,
        other => panic!("castNode(GatherMerge, gm_state->ps.plan) failed: {other:?}"),
    }
}

/// `gm->num_workers`.
fn num_workers_of(gm: &GatherMerge<'_>) -> i32 {
    gm.num_workers
}

/// Borrow the node's binary heap for read.
fn heap_ref<'a, 'mcx>(gm_state: &'a GatherMergeStateData<'mcx>) -> PgResult<&'a BinaryHeap<'mcx>> {
    gm_state
        .gm_heap
        .as_deref()
        .ok_or_else(|| elog_error("GatherMerge has no binary heap"))
}

/// Borrow the node's binary heap for write (used by the comparator-free heap
/// operations: `add_unordered`, `reset`).
fn heap_mut<'a, 'mcx>(
    gm_state: &'a mut GatherMergeStateData<'mcx>,
) -> PgResult<&'a mut BinaryHeap<'mcx>> {
    gm_state
        .gm_heap
        .as_deref_mut()
        .ok_or_else(|| elog_error("GatherMerge has no binary heap"))
}

/// `gm_state->gm_slots[idx]` — the slot id at participant index `idx`.
fn slot_at(gm_state: &GatherMergeStateData<'_>, idx: SlotNumber) -> PgResult<SlotId> {
    let i = usize::try_from(idx).map_err(|_| elog_error("GatherMerge slot index is negative"))?;
    gm_state
        .gm_slots
        .get(i)
        .copied()
        .flatten()
        .ok_or_else(|| elog_error("GatherMerge leading slot is empty"))
}

/// `TupIsNull(gm_slots[idx])` (tuptable.h) — true if the slot is absent or
/// marked empty (`TTS_FLAG_EMPTY`).
fn slot_is_null(
    gm_state: &GatherMergeStateData<'_>,
    estate: &EStateData<'_>,
    idx: usize,
) -> bool {
    match gm_state.gm_slots.get(idx).and_then(|s| *s) {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `TupIsNull(slot)` for a freshly-produced child slot id.
fn tup_is_null(slot: Option<SlotId>, estate: &EStateData<'_>) -> bool {
    match slot {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `ExecClearTuple(gm_state->gm_slots[idx])` — clear the slot at `idx` (a no-op
/// when the slot is absent, as worker slots are always allocated).
fn clear_slot<'mcx>(
    gm_state: &mut GatherMergeStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    idx: usize,
) -> PgResult<()> {
    if let Some(id) = gm_state.gm_slots[idx] {
        execTuples::exec_clear_tuple::call(estate.slot_mut(id))?;
    }
    Ok(())
}

/// `econtext->ecxt_outertuple = slot` — install the leading participant slot as
/// the projection's outer tuple before `ExecProject`.
fn set_outer_tuple(estate: &mut EStateData<'_>, econtext: types_nodes::EcxtId, slot: SlotId) {
    estate.ecxt_mut(econtext).ecxt_outertuple = Some(slot);
}

/// `INVERT_COMPARE_RESULT(var)` (sortsupport.h) — flip the sign of a three-way
/// comparison result while avoiding the `-INT_MIN` overflow corner case.
///
/// ```c
/// #define INVERT_COMPARE_RESULT(var) ((var) = ((var) < 0) ? 1 : -(var))
/// ```
fn INVERT_COMPARE_RESULT(var: i32) -> i32 {
    if var < 0 {
        1
    } else {
        var.wrapping_neg()
    }
}

/// `ApplySortComparator(datum1, isNull1, datum2, isNull2, ssup)` (sortsupport.h)
/// — three-way compare two datums with the supplied `SortSupport`, honouring
/// NULL ordering (`ssup_nulls_first`) and reverse ordering (`ssup_reverse`).
///
/// ```c
/// static inline int
/// ApplySortComparator(Datum datum1, bool isNull1, Datum datum2, bool isNull2,
///                     SortSupport ssup) {
///     int compare;
///     if (isNull1) {
///         if (isNull2) compare = 0;
///         else if (ssup->ssup_nulls_first) compare = -1;
///         else compare = 1;
///     } else if (isNull2) {
///         compare = ssup->ssup_nulls_first ? 1 : -1;
///     } else {
///         compare = ApplyUnsignedSortComparator(datum1, datum2, ssup);
///         if (ssup->ssup_reverse) INVERT_COMPARE_RESULT(compare);
///     }
///     return compare;
/// }
/// ```
fn ApplySortComparator(
    datum1: Datum,
    is_null1: bool,
    datum2: Datum,
    is_null2: bool,
    ssup: &SortSupportData<'_>,
) -> PgResult<i32> {
    let compare = if is_null1 {
        if is_null2 {
            0
        } else if ssup.ssup_nulls_first {
            -1
        } else {
            1
        }
    } else if is_null2 {
        if ssup.ssup_nulls_first {
            1
        } else {
            -1
        }
    } else {
        let mut compare = sortsupport::apply_sort_comparator::call(datum1, datum2, ssup)?;
        if ssup.ssup_reverse {
            compare = INVERT_COMPARE_RESULT(compare);
        }
        compare
    };
    Ok(compare)
}

/// Deep-clone an optional bitmapset (the owned-tree stand-in for the C reuse of
/// the same `Bitmapset *` while a `&mut node` borrow is live). Copying
/// allocates, so it is fallible.
fn clone_bitmapset<'mcx>(
    mcx: Mcx<'mcx>,
    set: Option<&Bitmapset<'_>>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    match set {
        Some(s) => Ok(Some(alloc_in(mcx, s.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// `elog(ERROR, msg)` — internal-error text with `ERRCODE_INTERNAL_ERROR`.
fn elog_error(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

#[cfg(test)]
mod tests;
