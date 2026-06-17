//! Port of `src/backend/executor/nodeGather.c` — support routines for scanning
//! a plan via multiple workers.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitGather`]      - initialize the Gather node.
//! - [`ExecGather`]          - retrieve the next tuple from the node.
//! - [`ExecEndGather`]       - shut down the Gather node.
//! - [`ExecShutdownGather`]  - destroy the parallel-worker setup.
//! - [`ExecReScanGather`]    - rescan the Gather node.
//!
//! A Gather node launches parallel workers to run multiple copies of a plan.
//! It can also run the plan itself (the "leader"), if the workers are not
//! available or have not started up yet. It then merges all of the results it
//! produces and the results from the workers into a single output stream.
//!
//! The five interface routines and the file-local statics (`ExecGather`,
//! `gather_getnext`, `gather_readnext`, `ExecShutdownGatherWorkers`) are this
//! crate's owned logic. Operations below the executor-node layer go through the
//! owners' seam crates, exactly as in the sibling `nodeGatherMerge`:
//!
//! - interrupt servicing (`CHECK_FOR_INTERRUPTS`) → tcop/postgres;
//! - child dispatch / init / teardown (`ExecProcNode` / `ExecInitNode` /
//!   `ExecEndNode`) → execProcnode; rescan (`ExecReScan`) → execAmi;
//! - per-node expr-context setup/reset, result-type setup, projection
//!   (`ExecAssignExprContext` / `ExecGetResultType` / `ExecInitResultTypeTL` /
//!   `ExecConditionalAssignProjectionInfo`) → execUtils (direct, no cycle) and
//!   `ResetExprContext` / `ExecProject` → execUtils-seams / execExpr-seams;
//! - slot setup/clear/store (`ExecInitExtraTupleSlot` / `ExecClearTuple` /
//!   `ExecStoreMinimalTuple`) → execTuples;
//! - the parallel-executor DSM setup, worker launch, reader creation and finish
//!   (`ExecInitParallelPlan` / `ExecParallelReinitialize` /
//!   `LaunchParallelWorkers` / `ExecParallelCreateReaders` /
//!   `ExecParallelFinish` / `ExecParallelCleanup`) → execParallel /
//!   access-transam-parallel; tuple-queue reads (`TupleQueueReaderNext`) →
//!   tqueue; the `bms_add_member` of the rescan param → nodes/bitmapset; the
//!   `parallel_leader_participation` GUC → guc-tables (direct, no cycle); the
//!   `WaitLatch`/`ResetLatch` of the leader's wait loop → storage/ipc/latch.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use backend_access_transam_parallel as parallel;
use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execParallel_seams as execParallel;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils_seams;
use backend_executor_tqueue_seams as tqueue;
use backend_nodes_core_seams as bitmapset;
use backend_storage_ipc_latch_seams as latch;
use backend_tcop_postgres_seams as tcop_postgres;

use mcx::{alloc_in, Mcx, PgBox};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::nodegather::{Gather, GatherStateData};
use types_nodes::{Bitmapset, EStateData, PlanStateData, PlanStateNode, SlotId, TupleSlotKind};
use types_pgstat::wait_event::WAIT_EVENT_EXECUTE_GATHER;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET};
use types_tuple::backend_access_common_heaptuple::FormedMinimalTuple;

/// Decode a flat C `MinimalTuple` byte image (the tuple-queue wire bytes) into
/// the payload-bearing [`FormedMinimalTuple`] carrier.
fn mintuple_from_flat<'mcx>(
    mcx: Mcx<'mcx>,
    blob: &[u8],
) -> PgResult<FormedMinimalTuple<'mcx>> {
    use backend_access_common_heaptuple::flat::MinimalTupleFlatError;
    match backend_access_common_heaptuple::flat::minimal_tuple_from_flat(mcx, blob) {
        Ok(mtup) => Ok(mtup),
        Err(MinimalTupleFlatError::Pg(err)) => Err(err),
        Err(other) => panic!("minimal_tuple_from_flat on a tuple-queue image failed: {other:?}"),
    }
}

/// `OUTER_VAR` (primnodes.h) — special varno denoting the outer subplan, used
/// by `ExecConditionalAssignProjectionInfo`. `#define OUTER_VAR (-2)`.
const OUTER_VAR: i32 = -2;

/// Install this crate's implementations into its seam slots. nodeGather has no
/// `<unit>-seams` crate: its functions are reached through the executor
/// dispatch (execProcnode / execAmi), which depend on this crate directly
/// without a cycle (mirrors nodeGatherMerge).
pub fn init_seams() {}

// ===========================================================================
// Dispatch callback.
// ===========================================================================

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitGather`]:
/// `castNode(GatherState, pstate)` then run [`ExecGather`].
fn exec_gather_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::Gather(node) => node,
        other => panic!("castNode(GatherState, pstate) failed: {other:?}"),
    };
    ExecGather(node, estate)
}

// ===========================================================================
// Interface routines (1:1 with nodeGather.c).
// ===========================================================================

/// `ExecInitGather(node, estate, eflags)` — initialize the Gather node.
pub fn ExecInitGather<'mcx>(
    plan_node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, GatherStateData<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let node: &'mcx Gather<'mcx> = match plan_node {
        types_nodes::nodes::Node::Gather(g) => g,
        other => panic!("castNode(Gather, node) failed: {other:?}"),
    };

    // Gather node doesn't have innerPlan node.
    //   Assert(innerPlan(node) == NULL);
    debug_assert!(node.plan.righttree.is_none());

    // create state structure
    //   gatherstate = makeNode(GatherState);
    //   gatherstate->ps.plan = (Plan *) node;
    //   gatherstate->ps.state = estate;
    //   gatherstate->ps.ExecProcNode = ExecGather;
    let mut ps = PlanStateData::default();
    ps.plan = Some(plan_node);
    ps.ExecProcNode = Some(exec_gather_node);

    //   gatherstate->initialized = false;
    //   gatherstate->need_to_scan_locally =
    //       !node->single_copy && parallel_leader_participation;
    //   gatherstate->tuples_needed = -1;
    let need_to_scan_locally = !node.single_copy
        && backend_utils_misc_guc_tables::vars::parallel_leader_participation.read();

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &gatherstate->ps);
    backend_executor_execUtils::ExecAssignExprContext(estate, &mut ps)?;

    // now initialize outer plan
    //   outerNode = outerPlan(node);
    //   outerPlanState(gatherstate) = ExecInitNode(outerNode, estate, eflags);
    //   tupDesc = ExecGetResultType(outerPlanState(gatherstate));
    let outer_plan = node.plan.lefttree.as_deref();
    let outer_plan_state = execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;
    ps.lefttree = outer_plan_state;
    let tup_desc = {
        let outer = ps
            .lefttree
            .as_deref()
            .expect("ExecInitGather: outerPlanState is NULL");
        match backend_executor_execUtils::ExecGetResultType(outer.ps_head()) {
            Some(d) => Some(alloc_in(mcx, d.clone_in(mcx)?)?),
            None => None,
        }
    };

    // Leader may access ExecProcNode result directly (if need_to_scan_locally),
    // or from workers via tuple queue.  So we can't trivially rely on the slot
    // type being fixed for expressions evaluated within this node.
    //   gatherstate->ps.outeropsset = true;
    //   gatherstate->ps.outeropsfixed = false;
    //
    // This repo's trimmed `PlanStateData` omits the `outerops*` slot-type
    // optimization fields, so these two stores have no field to land in;
    // recording the intent that the outer slot ops are non-fixed is a
    // behavior-preserving no-op here.

    // Initialize result type and projection.
    //   ExecInitResultTypeTL(&gatherstate->ps);
    //   ExecConditionalAssignProjectionInfo(&gatherstate->ps, tupDesc, OUTER_VAR);
    execTuples::exec_init_result_type_tl::call(&mut ps, estate)?;
    {
        let input_desc = tup_desc
            .as_deref()
            .expect("ExecInitGather: child result type is NULL");
        backend_executor_execUtils::ExecConditionalAssignProjectionInfo(
            &mut ps, estate, input_desc, OUTER_VAR,
        )?;
    }

    // Without projections result slot type is not trivially known, see comment
    // above.
    //   if (gatherstate->ps.ps_ProjInfo == NULL) {
    //       gatherstate->ps.resultopsset = true;
    //       gatherstate->ps.resultopsfixed = false;
    //   }
    if ps.ps_ProjInfo.is_none() {
        ps.resultopsset = true;
        ps.resultopsfixed = false;
    }

    // Initialize funnel slot to same tuple descriptor as outer plan.
    //   gatherstate->funnel_slot = ExecInitExtraTupleSlot(estate, tupDesc,
    //                                                     &TTSOpsMinimalTuple);
    let funnel_desc = match &tup_desc {
        Some(d) => Some(alloc_in(mcx, d.clone_in(mcx)?)?),
        None => None,
    };
    let funnel_slot =
        execTuples::exec_init_extra_tuple_slot::call(estate, funnel_desc, TupleSlotKind::MinimalTuple)?;

    // Gather doesn't support checking a qual (it's always more efficient to do
    // it in the child node).
    //   Assert(!node->plan.qual);
    debug_assert!(node.plan.qual.is_none());

    let gatherstate = alloc_in(
        mcx,
        GatherStateData {
            ps,
            initialized: false,
            need_to_scan_locally,
            tuples_needed: -1,
            funnel_slot: Some(funnel_slot),
            pei: None,
            nworkers_launched: 0,
            nreaders: 0,
            nextreader: 0,
            reader: mcx::vec_with_capacity_in(mcx, 0)?,
        },
    )?;

    Ok(gatherstate)
}

/// `ExecGather(pstate)` — the `PlanState.ExecProcNode` callback: scans the
/// relation via multiple workers and returns the next qualifying tuple.
///
/// 1:1 with `static TupleTableSlot *ExecGather(PlanState *pstate)`. Returns
/// `None` for `TupIsNull(slot)` (end of scan); otherwise the id of the produced
/// slot (the funnel slot, the leader's child slot, or the projection's output).
pub fn ExecGather<'mcx>(
    node: &mut GatherStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    tcop_postgres::check_for_interrupts::call()?;

    // Initialize the parallel context and workers on first execution. We do
    // this on first execution rather than during node initialization, as it
    // needs to allocate a large dynamic segment, so it is better to do it only
    // if it is really needed.
    if !node.initialized {
        let mcx = estate.es_query_cxt;

        // Gather *gather = (Gather *) node->ps.plan;
        //
        // Read the planner-set scalars off the plan up front, then drop the
        // immutable plan borrow so the launch path can take `&mut node`.
        let (num_workers, single_copy, init_param) = {
            let gather = gather_plan(node);
            (
                gather.num_workers,
                gather.single_copy,
                clone_bitmapset(mcx, gather.initParam.as_deref())?,
            )
        };

        // Sometimes we might have to run without parallelism; but if parallel
        // mode is active then we can try to fire up some workers.
        //   if (gather->num_workers > 0 && estate->es_use_parallel_mode)
        if num_workers > 0 && estate.es_use_parallel_mode {
            // Initialize, or re-initialize, shared state needed by workers.
            //   if (!node->pei)
            //       node->pei = ExecInitParallelPlan(outerPlanState(node),
            //           estate, gather->initParam, gather->num_workers,
            //           node->tuples_needed);
            //   else
            //       ExecParallelReinitialize(outerPlanState(node), node->pei,
            //           gather->initParam);
            let tuples_needed = node.tuples_needed;
            if node.pei.is_none() {
                let outer = node
                    .ps
                    .lefttree
                    .as_deref_mut()
                    .ok_or_else(|| elog_error("Gather has no outer plan state"))?;
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
                    .ok_or_else(|| elog_error("Gather has no outer plan state"))?;
                let pei = node
                    .pei
                    .as_deref_mut()
                    .ok_or_else(|| elog_error("Gather has no parallel info"))?;
                execParallel::exec_parallel_reinitialize_owned::call(
                    mcx,
                    outer,
                    estate,
                    pei,
                    init_param.as_deref(),
                )?;
            }

            // Register backend workers. We might not get as many as we
            // requested, or indeed any at all.
            //   pcxt = node->pei->pcxt;
            //   LaunchParallelWorkers(pcxt);
            //   node->nworkers_launched = pcxt->nworkers_launched;
            //   estate->es_parallel_workers_to_launch += pcxt->nworkers_to_launch;
            //   estate->es_parallel_workers_launched += pcxt->nworkers_launched;
            let pcxt = node
                .pei
                .as_deref()
                .and_then(|pei| pei.pcxt)
                .ok_or_else(|| elog_error("Gather parallel context is missing"))?;
            parallel::launch_parallel_workers(pcxt)?;
            let nworkers_launched = parallel::pcxt_nworkers_launched(pcxt);
            let nworkers_to_launch = parallel::pcxt_nworkers_to_launch(pcxt);
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
                        .ok_or_else(|| elog_error("Gather has no parallel info"))?;
                    execParallel::ExecParallelCreateReaders::call(mcx, pei)?;
                }
                node.nreaders = nworkers_launched;
                let nreaders = usize::try_from(nworkers_launched)
                    .map_err(|_| elog_error("Gather nreaders is negative"))?;
                let mut reader: mcx::PgVec<'mcx, types_execparallel::TupleQueueReaderHandle> =
                    mcx::vec_with_capacity_in(mcx, nreaders)?;
                {
                    let pei = node
                        .pei
                        .as_deref()
                        .ok_or_else(|| elog_error("Gather has no parallel info"))?;
                    for i in 0..nreaders {
                        let r = *pei
                            .reader
                            .get(i)
                            .ok_or_else(|| elog_error("Gather pei->reader array too short"))?;
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
            // node->nextreader = 0;
            node.nextreader = 0;
        }

        // Run plan locally if no workers or enabled and not single-copy.
        //   node->need_to_scan_locally = (node->nreaders == 0)
        //       || (!gather->single_copy && parallel_leader_participation);
        node.need_to_scan_locally = (node.nreaders == 0)
            || (!single_copy
                && backend_utils_misc_guc_tables::vars::parallel_leader_participation.read());
        node.initialized = true;
    }

    // Reset per-tuple memory context to free any expression evaluation storage
    // allocated in the previous tuple cycle.
    //   econtext = node->ps.ps_ExprContext;
    //   ResetExprContext(econtext);
    let econtext = node
        .ps
        .ps_ExprContext
        .ok_or_else(|| elog_error("Gather has no expression context"))?;
    execUtils_seams::reset_expr_context::call(estate, econtext)?;

    // Get next tuple, either from one of our workers, or by running the plan
    // ourselves.
    //   slot = gather_getnext(node);
    //   if (TupIsNull(slot)) return NULL;
    let slot_id = match gather_getnext(node, estate)? {
        Some(i) => i,
        None => return Ok(None),
    };

    // If no projection is required, we're done.
    //   if (node->ps.ps_ProjInfo == NULL) return slot;
    if node.ps.ps_ProjInfo.is_none() {
        return Ok(Some(slot_id));
    }

    // Form the result tuple using ExecProject(), and return it.
    //   econtext->ecxt_outertuple = slot;
    //   return ExecProject(node->ps.ps_ProjInfo);
    set_outer_tuple(estate, econtext, slot_id);
    let out = execExpr::exec_project::call(&mut node.ps, estate)?;
    Ok(Some(out))
}

/// `ExecEndGather(node)` — frees any storage allocated through C routines.
///
/// ```c
/// void ExecEndGather(GatherState *node)
/// {
///     ExecEndNode(outerPlanState(node));  /* let children clean up first */
///     ExecShutdownGather(node);
/// }
/// ```
pub fn ExecEndGather<'mcx>(
    node: &mut GatherStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // let children clean up first
    if let Some(outer) = node.ps.lefttree.as_deref_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }
    ExecShutdownGather(node)?;
    Ok(())
}

/// `gather_getnext(gatherstate)` — read the next tuple. We might fetch a tuple
/// from one of the tuple queues using [`gather_readnext`], or, if no tuple
/// queue contains a tuple and the single_copy flag is not set, we might
/// generate one locally instead.
///
/// 1:1 with `static TupleTableSlot *gather_getnext(GatherState *gatherstate)`.
/// Returns the id of the slot holding the tuple, or `None` (the cleared funnel
/// slot) when exhausted.
fn gather_getnext<'mcx>(
    gatherstate: &mut GatherStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // TupleTableSlot *fslot = gatherstate->funnel_slot;
    let fslot = gatherstate
        .funnel_slot
        .ok_or_else(|| elog_error("Gather has no funnel slot"))?;

    // while (gatherstate->nreaders > 0 || gatherstate->need_to_scan_locally)
    while gatherstate.nreaders > 0 || gatherstate.need_to_scan_locally {
        tcop_postgres::check_for_interrupts::call()?;

        // if (gatherstate->nreaders > 0)
        if gatherstate.nreaders > 0 {
            //   tup = gather_readnext(gatherstate);
            let tup = gather_readnext(gatherstate, estate)?;

            //   if (HeapTupleIsValid(tup)) {
            //       ExecStoreMinimalTuple(tup, fslot, false);  /* don't pfree */
            //       return fslot;
            //   }
            if let Some(tup) = tup {
                execTuples::exec_store_minimal_tuple::call(estate, tup, fslot, false)?;
                return Ok(Some(fslot));
            }
        }

        // if (gatherstate->need_to_scan_locally)
        if gatherstate.need_to_scan_locally {
            // Install our DSA area while executing the plan.
            //   estate->es_query_dsa = gatherstate->pei ? gatherstate->pei->area : NULL;
            //   outerTupleSlot = ExecProcNode(outerPlan);
            //   estate->es_query_dsa = NULL;
            let saved_dsa = estate.es_query_dsa;
            estate.es_query_dsa = gatherstate.pei.as_deref().and_then(|pei| pei.area);
            let outer_tuple_slot = {
                let outer = gatherstate
                    .ps
                    .lefttree
                    .as_deref_mut()
                    .ok_or_else(|| elog_error("Gather has no outer plan state"))?;
                execProcnode::exec_proc_node::call(outer, estate)?
            };
            // C unconditionally writes `estate->es_query_dsa = NULL`; the owned
            // form restores the prior value (always NULL here, matching C).
            estate.es_query_dsa = saved_dsa;

            //   if (!TupIsNull(outerTupleSlot)) return outerTupleSlot;
            //   gatherstate->need_to_scan_locally = false;
            if !tup_is_null(outer_tuple_slot, estate) {
                return Ok(outer_tuple_slot);
            }
            gatherstate.need_to_scan_locally = false;
        }
    }

    // return ExecClearTuple(fslot);
    execTuples::exec_clear_tuple::call(estate, fslot)?;
    Ok(None)
}

/// `gather_readnext(gatherstate)` — attempt to read a tuple from one of our
/// parallel workers.
///
/// 1:1 with `static MinimalTuple gather_readnext(GatherState *gatherstate)`.
/// Returns `None` for the C `NULL` return (the leader should generate a tuple
/// itself, or all readers are done).
fn gather_readnext<'mcx>(
    gatherstate: &mut GatherStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // int nvisited = 0;
    let mut nvisited: i32 = 0;

    // for (;;)
    loop {
        // Check for async events, particularly messages from workers.
        tcop_postgres::check_for_interrupts::call()?;

        // Attempt to read a tuple, but don't block if none is available.
        //
        // Note that TupleQueueReaderNext will just return NULL for a worker
        // which fails to initialize.  We'll treat that worker as having
        // produced no tuples; WaitForParallelWorkersToFinish will error out
        // when we get there.
        //   Assert(gatherstate->nextreader < gatherstate->nreaders);
        //   reader = gatherstate->reader[gatherstate->nextreader];
        //   tup = TupleQueueReaderNext(reader, true, &readerdone);
        debug_assert!(gatherstate.nextreader < gatherstate.nreaders);
        let reader = *gatherstate
            .reader
            .get(gatherstate.nextreader as usize)
            .ok_or_else(|| elog_error("gather_readnext: active reader must be present"))?;
        // The landed tqueue contract returns the next tuple's on-wire
        // minimal-tuple byte image (`None` once exhausted / would-block) plus
        // the C `*readerdone` out-parameter (true once the queue is detached).
        let (bytes, readerdone) = tqueue::tuple_queue_reader_next::call(reader, true)?;

        // If this reader is done, remove it from our working array of active
        // readers.  If all readers are done, we're outta here.
        //   if (readerdone) { ... }
        if readerdone {
            // Assert(!tup);
            debug_assert!(bytes.is_none());
            // --gatherstate->nreaders;
            gatherstate.nreaders -= 1;
            // if (gatherstate->nreaders == 0) {
            //     ExecShutdownGatherWorkers(gatherstate);
            //     return NULL;
            // }
            if gatherstate.nreaders == 0 {
                exec_shutdown_gather_workers(gatherstate)?;
                return Ok(None);
            }
            // memmove(&reader[nextreader], &reader[nextreader + 1],
            //         sizeof(...) * (nreaders - nextreader));
            // (the owned `PgVec` form: remove the entry at `nextreader`, which
            // shifts the trailing readers left, mirroring the C memmove over
            // the now-`nreaders`-long active prefix.)
            let idx = gatherstate.nextreader as usize;
            gatherstate.reader.remove(idx);
            // if (gatherstate->nextreader >= gatherstate->nreaders)
            //     gatherstate->nextreader = 0;
            if gatherstate.nextreader >= gatherstate.nreaders {
                gatherstate.nextreader = 0;
            }
            continue;
        }

        // If we got a tuple, return it.
        //   if (tup) return tup;
        //
        // The C copies the queue-memory pointer with `heap_copy_minimal_tuple`
        // (done by the tqueue owner here, which returns a copy of the wire
        // bytes); we reassemble the owned `MinimalTuple` into `mcx`.
        if let Some(image) = bytes {
            // `image` is the tuple's contiguous C MinimalTuple byte image (the
            // flat blob, `t_len` first); decode it into the owned carrier.
            let mtup = mintuple_from_flat(mcx, &image)?;
            return Ok(Some(mtup));
        }

        // Advance nextreader pointer in round-robin fashion.  Note that we only
        // reach this code if we weren't able to get a tuple from the current
        // worker.  We used to advance the nextreader pointer after every tuple,
        // but it turns out to be much more efficient to keep reading from the
        // same queue until that would require blocking.
        //   gatherstate->nextreader++;
        //   if (gatherstate->nextreader >= gatherstate->nreaders)
        //       gatherstate->nextreader = 0;
        gatherstate.nextreader += 1;
        if gatherstate.nextreader >= gatherstate.nreaders {
            gatherstate.nextreader = 0;
        }

        // Have we visited every (surviving) TupleQueueReader?
        //   nvisited++;
        //   if (nvisited >= gatherstate->nreaders) { ... }
        nvisited += 1;
        if nvisited >= gatherstate.nreaders {
            // If (still) running plan locally, return NULL so caller can
            // generate another tuple from the local copy of the plan.
            //   if (gatherstate->need_to_scan_locally) return NULL;
            if gatherstate.need_to_scan_locally {
                return Ok(None);
            }

            // Nothing to do except wait for developments.
            //   (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
            //                    WAIT_EVENT_EXECUTE_GATHER);
            //   ResetLatch(MyLatch);
            //   nvisited = 0;
            latch::wait_latch_my_latch::call(
                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                0,
                WAIT_EVENT_EXECUTE_GATHER,
            )?;
            latch::reset_latch_my_latch::call();
            nvisited = 0;
        }
    }
}

/// `ExecShutdownGatherWorkers(node)` — stop all the parallel workers.
///
/// ```c
/// static void ExecShutdownGatherWorkers(GatherState *node)
/// {
///     if (node->pei != NULL)
///         ExecParallelFinish(node->pei);
///     /* Flush local copy of reader array */
///     if (node->reader)
///         pfree(node->reader);
///     node->reader = NULL;
/// }
/// ```
fn exec_shutdown_gather_workers<'mcx>(node: &mut GatherStateData<'mcx>) -> PgResult<()> {
    if node.pei.is_some() {
        let pei = node
            .pei
            .as_deref_mut()
            .ok_or_else(|| elog_error("Gather has no parallel info"))?;
        execParallel::ExecParallelFinish::call(pei)?;
    }

    // Flush local copy of reader array. The C `pfree(node->reader)` becomes a
    // clear of the owned `PgVec`; the subsequent `node->reader = NULL` is the
    // empty `PgVec`.
    node.reader.clear();
    Ok(())
}

/// `ExecShutdownGather(node)` — destroy the setup for parallel workers
/// including the parallel context.
///
/// ```c
/// void ExecShutdownGather(GatherState *node)
/// {
///     ExecShutdownGatherWorkers(node);
///     if (node->pei != NULL) {
///         ExecParallelCleanup(node->pei);
///         node->pei = NULL;
///     }
/// }
/// ```
pub fn ExecShutdownGather<'mcx>(node: &mut GatherStateData<'mcx>) -> PgResult<()> {
    exec_shutdown_gather_workers(node)?;

    // Now destroy the parallel context.
    if node.pei.is_some() {
        // `pei` (node->pei) and the leader's outerPlanState (node->ps.lefttree)
        // are disjoint fields, so both can be borrowed mutably at once.
        let outer = node
            .ps
            .lefttree
            .as_deref_mut()
            .ok_or_else(|| elog_error("Gather has no outer plan state"))?;
        let pei = node
            .pei
            .as_deref_mut()
            .ok_or_else(|| elog_error("Gather has no parallel info"))?;
        execParallel::ExecParallelCleanup::call(pei, outer)?;
        node.pei = None;
    }
    Ok(())
}

/// `ExecReScanGather(node)` — prepare to re-scan the result of a Gather.
///
/// 1:1 with `void ExecReScanGather(GatherState *node)`.
pub fn ExecReScanGather<'mcx>(
    node: &mut GatherStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // Gather *gather = (Gather *) node->ps.plan;
    let rescan_param = gather_plan(node).rescan_param;

    // Make sure any existing workers are gracefully shut down.
    exec_shutdown_gather_workers(node)?;

    // Mark node so that shared state will be rebuilt at next call.
    //   node->initialized = false;
    node.initialized = false;

    // Set child node's chgParam to tell it that the next scan might deliver a
    // different set of rows within the leader process.
    //   if (gather->rescan_param >= 0)
    //       outerPlan->chgParam =
    //           bms_add_member(outerPlan->chgParam, gather->rescan_param);
    if rescan_param >= 0 {
        let outer = node
            .ps
            .lefttree
            .as_deref_mut()
            .ok_or_else(|| elog_error("Gather has no outer plan state"))?;
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
            .ok_or_else(|| elog_error("Gather has no outer plan state"))?;
        execAmi::exec_re_scan::call(outer, estate)?;
    }
    Ok(())
}

// ===========================================================================
// Small in-crate node-layer helpers.
// ===========================================================================

/// `castNode(Gather, node->ps.plan)` — the node's concrete plan.
fn gather_plan<'a, 'mcx>(node: &'a GatherStateData<'mcx>) -> &'a Gather<'mcx> {
    match node.ps.plan.as_deref().expect("GatherState has no plan") {
        types_nodes::nodes::Node::Gather(g) => g,
        other => panic!("castNode(Gather, node->ps.plan) failed: {other:?}"),
    }
}

/// `TupIsNull(slot)` for a freshly-produced child slot id.
fn tup_is_null(slot: Option<SlotId>, estate: &EStateData<'_>) -> bool {
    match slot {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `econtext->ecxt_outertuple = slot` — install the produced slot as the
/// projection's outer tuple before `ExecProject`.
fn set_outer_tuple(estate: &mut EStateData<'_>, econtext: types_nodes::EcxtId, slot: SlotId) {
    estate.ecxt_mut(econtext).ecxt_outertuple = Some(slot);
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
