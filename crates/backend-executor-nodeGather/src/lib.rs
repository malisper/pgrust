//! Port of `src/backend/executor/nodeGather.c` — support routines for scanning
//! a plan via multiple workers.
//!
//! A Gather executor launches parallel workers to run multiple copies of a
//! plan. It can also run the plan itself, if the workers are not available or
//! have not started up yet. It then merges all of the results it produces and
//! the results from the workers into a single output stream. Therefore it will
//! normally be used with a plan where running multiple copies of the same plan
//! does not produce duplicate output, such as a parallel-aware `SeqScan`.
//!
//! Alternatively a Gather node can be configured to use just one worker and the
//! single-copy flag can be set; then the Gather runs the plan in one worker and
//! does not execute the plan itself.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitGather`]      - create and initialize a Gather node.
//! - [`ExecGather`]          - the `PlanState.ExecProcNode` callback.
//! - [`ExecEndGather`]       - shut down the Gather node.
//! - [`ExecReScanGather`]    - rescan the Gather.
//! - [`ExecShutdownGather`]  - destroy the parallel context.
//!
//! The node state machine, the `gather_getnext` / `gather_readnext` worker /
//! local merge loop, and the `node->reader` working-array bookkeeping
//! (`palloc`/`memcpy`/`memmove`/`pfree`, modeled as a `PgVec` of reader
//! handles) are this crate's owned logic. Operations below the executor-node
//! layer go through the owners' seam crates:
//!
//! - interrupt servicing (`CHECK_FOR_INTERRUPTS`) → tcop/postgres;
//! - child dispatch / init / teardown / rescan
//!   (`ExecProcNode` / `ExecInitNode` / `ExecEndNode` / `ExecReScan`) →
//!   execProcnode / execAmi;
//! - econtext / slot / result-type / projection setup
//!   (`ExecAssignExprContext` / `ExecInitResultTypeTL` /
//!   `ExecConditionalAssignProjectionInfo` / `ExecGetResultType` /
//!   `ExecInitExtraTupleSlot`) → execUtils / execTuples;
//! - the per-tuple `ExprContext` reset (owned, over the EState pool) and
//!   `ExecProject` (`execExpr`);
//! - slot ops (`ExecStoreMinimalTuple` / `ExecClearTuple`) → execTuples;
//! - the parallel-executor / parallel-context machinery
//!   (`ExecInitParallelPlan` / `ExecParallelReinitialize` /
//!   `ExecParallelCreateReaders` / `ExecParallelFinish` /
//!   `ExecParallelCleanup` / `LaunchParallelWorkers`) → execParallel /
//!   access/transam/parallel;
//! - the tuple-queue reader (`TupleQueueReaderNext`) → tqueue;
//! - the latch wait (`WaitLatch` / `ResetLatch`) → storage/ipc/latch;
//! - `bms_add_member` → nodes/bitmapset.
//!
//! `parallel_leader_participation` is a per-backend GUC read directly from the
//! GUC table (its `thread_local` storage), not a seam.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use backend_access_transam_parallel_seams as parallel;
use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execParallel_seams as execParallel;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_executor_tqueue_seams as tqueue;
use backend_nodes_core_seams as bitmapset;
use backend_storage_ipc_latch_seams as latch;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_misc_guc_tables::vars::parallel_leader_participation;

use mcx::{alloc_in, slice_in, PgBox};
use types_error::PgResult;
use types_execparallel::{EStateHandle, PlanStateHandle, TuplesNeeded};
use types_nodes::executor::TupleSlotKind;
use types_nodes::nodes::Node;
use types_nodes::{EStateData, Gather, GatherStateData, PlanStateNode, SlotId};
use types_pgstat::wait_event::WAIT_EVENT_EXECUTE_GATHER;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET};

/// `OUTER_VAR` (primnodes.h) — the special varno meaning "outer subplan
/// output". PG18.3 value is -2 (INNER_VAR=-1, OUTER_VAR=-2, INDEX_VAR=-3).
pub const OUTER_VAR: i32 = -2;

/// nodeGather is reached through the executor dispatch (execProcnode), which
/// can depend on this crate directly, so it declares no `<unit>-seams` crate
/// and installs nothing.
pub fn init_seams() {}

#[cfg(test)]
mod tests;

/// `(Gather *) node->ps.plan` — the Gather plan node aliased by the state
/// node's `PlanState.plan`. Panics if the planstate's plan link is not a
/// Gather (a node-construction bug; the C just casts the pointer).
fn gather_plan<'a, 'mcx>(node: &'a GatherStateData<'mcx>) -> &'a Gather<'mcx> {
    match node.ps.plan {
        Some(Node::Gather(g)) => g,
        _ => panic!("GatherState.ps.plan is not a Gather node"),
    }
}

/// `TupIsNull(slot)` — true if the slot is absent or marked empty
/// (`TTS_FLAG_EMPTY`). The slot id resolves through the EState slot pool.
#[inline]
fn tup_is_null(slot: Option<SlotId>, estate: &EStateData<'_>) -> bool {
    match slot {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `ExecInitGather(node, estate, eflags)` — create and initialize a Gather node.
///
/// Ported 1:1 from nodeGather.c. `node_ref` is the shared, read-only plan-tree
/// `Node::Gather` the dispatch (`ExecInitNode`) holds; the state node aliases
/// it for its lifetime (`gatherstate->ps.plan = (Plan *) node`).
pub fn ExecInitGather<'mcx>(
    node_ref: &'mcx Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, GatherStateData<'mcx>>> {
    let node: &'mcx Gather<'mcx> = match node_ref {
        Node::Gather(g) => g,
        _ => panic!("ExecInitGather called with a non-Gather plan node"),
    };

    // Gather node doesn't have innerPlan node.
    debug_assert!(node.plan.righttree.is_none());

    // create state structure: makeNode(GatherState).
    let mcx = estate.es_query_cxt;
    let mut gatherstate = alloc_in(mcx, GatherStateData::new(mcx))?;
    // gatherstate->ps.plan = (Plan *) node;
    gatherstate.ps.plan = Some(node_ref);
    // gatherstate->ps.ExecProcNode = ExecGather;
    gatherstate.ps.ExecProcNode = Some(exec_gather_dispatch);

    gatherstate.initialized = false;
    // need_to_scan_locally = !node->single_copy && parallel_leader_participation
    gatherstate.need_to_scan_locally =
        !node.single_copy && parallel_leader_participation.read();
    gatherstate.tuples_needed = -1;

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &gatherstate->ps);
    execUtils::exec_assign_expr_context::call(estate, &mut gatherstate.ps)?;

    // now initialize outer plan:
    //   outerNode = outerPlan(node);
    //   outerPlanState(gatherstate) = ExecInitNode(outerNode, estate, eflags);
    let outer_node: Option<&'mcx Node<'mcx>> = node.plan.lefttree.as_deref();
    let outer_ps = execProcnode::exec_init_node::call(mcx, outer_node, estate, eflags)?;
    gatherstate.ps.lefttree = outer_ps;

    //   tupDesc = ExecGetResultType(outerPlanState(gatherstate));
    // We compute it here and pass it to the projection setup / funnel-slot
    // creation below. The result descriptor is cloned into mcx (C shares the
    // child's TupleDesc *).
    let tup_desc: Option<types_tuple::heaptuple::TupleDescData<'mcx>> =
        match gatherstate.ps.lefttree.as_deref() {
            Some(outer) => match execTuples::exec_get_result_type::call(outer.ps_head()) {
                Some(d) => Some(d.clone_in(mcx)?),
                None => None,
            },
            None => None,
        };

    // Leader may access ExecProcNode result directly (if need_to_scan_locally),
    // or from workers via tuple queue.  So we can't trivially rely on the slot
    // type being fixed for expressions evaluated within this node.
    //   gatherstate->ps.outeropsset = true;
    //   gatherstate->ps.outeropsfixed = false;
    // (PlanStateData does not carry the *outerops* fields yet; they land with
    // execProcnode's slot-type machinery. The dependent logic — projection
    // skip and result-ops copy — is performed inside
    // ExecConditionalAssignProjectionInfo, which is the only consumer.)

    // Initialize result type and projection:
    //   ExecInitResultTypeTL(&gatherstate->ps);
    execTuples::exec_init_result_type_tl::call(&mut gatherstate.ps)?;
    //   ExecConditionalAssignProjectionInfo(&gatherstate->ps, tupDesc, OUTER_VAR);
    execUtils::exec_conditional_assign_projection_info::call(
        &mut gatherstate.ps,
        estate,
        tup_desc.as_ref(),
        OUTER_VAR,
    )?;

    // Without projections result slot type is not trivially known, see comment
    // above:
    //   if (gatherstate->ps.ps_ProjInfo == NULL) {
    //       gatherstate->ps.resultopsset = true;
    //       gatherstate->ps.resultopsfixed = false;
    //   }
    if gatherstate.ps.ps_ProjInfo.is_none() {
        gatherstate.ps.resultopsset = true;
        gatherstate.ps.resultopsfixed = false;
    }

    // Initialize funnel slot to same tuple descriptor as outer plan.
    //   gatherstate->funnel_slot =
    //       ExecInitExtraTupleSlot(estate, tupDesc, &TTSOpsMinimalTuple);
    let funnel_desc: types_tuple::heaptuple::TupleDesc<'mcx> = match &tup_desc {
        Some(d) => Some(alloc_in(mcx, d.clone_in(mcx)?)?),
        None => None,
    };
    gatherstate.funnel_slot = Some(execTuples::exec_init_extra_tuple_slot::call(
        estate,
        funnel_desc,
        TupleSlotKind::MinimalTuple,
    )?);

    // Gather doesn't support checking a qual (it's always more efficient to do
    // it in the child node).
    debug_assert!(node.plan.qual.is_none());

    Ok(gatherstate)
}

/// The `ExecProcNode` callback installed into `PlanState.ExecProcNode`. The
/// dispatch hands the concrete `PlanStateNode` and the estate; recover the
/// Gather state and run [`ExecGather`].
fn exec_gather_dispatch<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    match pstate {
        PlanStateNode::Gather(node) => ExecGather(node, estate),
        _ => panic!("ExecGather dispatched on a non-Gather PlanState"),
    }
}

/// `ExecGather(pstate)` — the `PlanState.ExecProcNode` callback.
///
/// Scans the relation via multiple workers and returns the next qualifying
/// tuple. Ported 1:1 from nodeGather.c.
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
        let (num_workers, single_copy, rescan_param) = {
            let g = gather_plan(node);
            (g.num_workers, g.single_copy, g.rescan_param)
        };
        let _ = rescan_param;

        // Sometimes we might have to run without parallelism; but if parallel
        // mode is active then we can try to fire up some workers.
        if num_workers > 0 && estate.es_use_parallel_mode {
            // Initialize, or re-initialize, shared state needed by workers.
            //   if (!node->pei)
            //       node->pei = ExecInitParallelPlan(outerPlanState(node),
            //                                        estate, gather->initParam,
            //                                        gather->num_workers,
            //                                        node->tuples_needed);
            //   else
            //       ExecParallelReinitialize(outerPlanState(node), node->pei,
            //                                gather->initParam);
            // `gather->initParam` (the send-params set), materialized as an
            // owned value so the node's immutable borrow is released before we
            // take `&mut node.pei` below. A NULL `initParam` is the empty set.
            let send: types_nodes::bitmapset::Bitmapset<'mcx> = match gather_plan(node).initParam.as_deref() {
                Some(b) => b.clone_in(mcx)?,
                None => types_nodes::bitmapset::Bitmapset {
                    words: mcx::PgVec::new_in(mcx),
                },
            };
            let tuples_needed: TuplesNeeded = node.tuples_needed;
            if node.pei.is_none() {
                let pei = execParallel::ExecInitParallelPlan::call(
                    mcx,
                    PlanStateHandle(0),
                    EStateHandle(0),
                    &send,
                    num_workers,
                    tuples_needed,
                )?;
                node.pei = Some(alloc_in(mcx, pei)?);
            } else {
                let pei = node.pei.as_mut().unwrap();
                execParallel::ExecParallelReinitialize::call(
                    mcx,
                    PlanStateHandle(0),
                    pei,
                    &send,
                )?;
            }

            // Register backend workers. We might not get as many as we
            // requested, or indeed any at all.
            //   pcxt = node->pei->pcxt;
            //   LaunchParallelWorkers(pcxt);
            let pcxt = node
                .pei
                .as_ref()
                .unwrap()
                .pcxt
                .expect("pei->pcxt must be set after ExecInitParallelPlan");
            parallel::launch_parallel_workers::call(pcxt)?;

            // We save # workers launched for the benefit of EXPLAIN.
            //   node->nworkers_launched = pcxt->nworkers_launched;
            let nworkers_launched = parallel::pcxt_nworkers_launched::call(pcxt);
            node.nworkers_launched = nworkers_launched;

            // Count number of workers originally wanted and actually launched.
            //   estate->es_parallel_workers_to_launch += pcxt->nworkers_to_launch;
            //   estate->es_parallel_workers_launched += pcxt->nworkers_launched;
            estate.es_parallel_workers_to_launch += parallel::pcxt_nworkers_to_launch::call(pcxt);
            estate.es_parallel_workers_launched += nworkers_launched;

            // Set up tuple queue readers to read the results.
            if nworkers_launched > 0 {
                //   ExecParallelCreateReaders(node->pei);
                {
                    let pei = node.pei.as_mut().unwrap();
                    execParallel::ExecParallelCreateReaders::call(mcx, pei)?;
                }
                //   node->nreaders = pcxt->nworkers_launched;
                //   node->reader = palloc(node->nreaders * sizeof(...));
                //   memcpy(node->reader, node->pei->reader, ...);
                node.nreaders = nworkers_launched;
                let src = &node.pei.as_ref().unwrap().reader[..nworkers_launched as usize];
                node.reader = slice_in(mcx, src)?;
            } else {
                // No workers?  Then never mind.
                //   node->nreaders = 0; node->reader = NULL;
                node.nreaders = 0;
                node.reader = mcx::PgVec::new_in(mcx);
            }
            node.nextreader = 0;
        }

        // Run plan locally if no workers or enabled and not single-copy.
        //   node->need_to_scan_locally = (node->nreaders == 0)
        //       || (!gather->single_copy && parallel_leader_participation);
        node.need_to_scan_locally =
            (node.nreaders == 0) || (!single_copy && parallel_leader_participation.read());
        node.initialized = true;
    }

    // Reset per-tuple memory context to free any expression evaluation storage
    // allocated in the previous tuple cycle.
    //   econtext = node->ps.ps_ExprContext; ResetExprContext(econtext);
    if let Some(econtext) = node.ps.ps_ExprContext {
        estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
    }

    // Get next tuple, either from one of our workers, or by running the plan
    // ourselves.
    //   slot = gather_getnext(node); if (TupIsNull(slot)) return NULL;
    let slot = gather_getnext(node, estate)?;
    if tup_is_null(slot, estate) {
        return Ok(None);
    }

    // If no projection is required, we're done: return slot.
    //   if (node->ps.ps_ProjInfo == NULL) return slot;
    if node.ps.ps_ProjInfo.is_none() {
        return Ok(slot);
    }

    // Form the result tuple using ExecProject(), and return it.
    //   econtext->ecxt_outertuple = slot;
    //   return ExecProject(node->ps.ps_ProjInfo);
    if let Some(econtext) = node.ps.ps_ExprContext {
        estate.ecxt_mut(econtext).ecxt_outertuple = slot;
    }
    Ok(Some(execExpr::exec_project::call(&mut node.ps, estate)?))
}

/// `ExecEndGather(node)` — frees any storage allocated through C routines.
///
/// ```c
/// void
/// ExecEndGather(GatherState *node)
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
    if let Some(outer) = node.ps.lefttree.as_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }
    ExecShutdownGather(node)
}

/// `gather_getnext(gatherstate)` — read the next tuple.
///
/// We might fetch a tuple from one of the tuple queues using
/// [`gather_readnext`], or if no tuple queue contains a tuple and the
/// `single_copy` flag is not set, generate one locally instead. Ported 1:1.
fn gather_getnext<'mcx>(
    gatherstate: &mut GatherStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    //   PlanState *outerPlan = outerPlanState(gatherstate);
    //   TupleTableSlot *fslot = gatherstate->funnel_slot;
    let fslot = gatherstate.funnel_slot;

    while gatherstate.nreaders > 0 || gatherstate.need_to_scan_locally {
        tcop_postgres::check_for_interrupts::call()?;

        if gatherstate.nreaders > 0 {
            //   tup = gather_readnext(gatherstate);
            let tup = gather_readnext(gatherstate, estate)?;

            // if (HeapTupleIsValid(tup))
            if tup.is_some() {
                // ExecStoreMinimalTuple(tup, fslot, false);
                // return fslot;
                let fid = fslot.expect("funnel_slot must be initialized");
                execTuples::exec_store_minimal_tuple::call(estate, tup, fid, false)?;
                return Ok(fslot);
            }
        }

        if gatherstate.need_to_scan_locally {
            // Install our DSA area while executing the plan.
            //   estate->es_query_dsa =
            //       gatherstate->pei ? gatherstate->pei->area : NULL;
            estate.es_query_dsa = gatherstate.pei.as_ref().and_then(|p| p.area);

            // outerTupleSlot = ExecProcNode(outerPlan);
            let outer_tuple_slot = {
                let outer = gatherstate
                    .ps
                    .lefttree
                    .as_mut()
                    .expect("Gather has an outer plan");
                execProcnode::exec_proc_node::call(outer, estate)?
            };

            //   estate->es_query_dsa = NULL;
            estate.es_query_dsa = None;

            // if (!TupIsNull(outerTupleSlot)) return outerTupleSlot;
            if !tup_is_null(outer_tuple_slot, estate) {
                return Ok(outer_tuple_slot);
            }

            //   gatherstate->need_to_scan_locally = false;
            gatherstate.need_to_scan_locally = false;
        }
    }

    // return ExecClearTuple(fslot);
    if let Some(fid) = fslot {
        execTuples::exec_clear_tuple::call(estate.slot_mut(fid))?;
    }
    Ok(fslot)
}

/// `gather_readnext(gatherstate)` — attempt to read a tuple from one of our
/// parallel workers. Ported 1:1 (the `for(;;)` round-robin loop).
fn gather_readnext<'mcx>(
    gatherstate: &mut GatherStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<types_tuple::heaptuple::MinimalTuple<'mcx>> {
    let mcx = estate.es_query_cxt;
    let mut nvisited: i32 = 0;

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
        debug_assert!(gatherstate.nextreader < gatherstate.nreaders);
        //   reader = gatherstate->reader[gatherstate->nextreader];
        //   tup = TupleQueueReaderNext(reader, true, &readerdone);
        let next = gatherstate.nextreader as usize;
        let reader = gatherstate.reader[next];
        let read = tqueue::tuple_queue_reader_next::call(mcx, reader, true)?;
        let readerdone = read.done;
        let tup = read.tup;

        // If this reader is done, remove it from our working array of active
        // readers.  If all readers are done, we're outta here.
        if readerdone {
            // Assert(!tup);
            debug_assert!(tup.is_none());
            //   --gatherstate->nreaders;
            gatherstate.nreaders -= 1;
            if gatherstate.nreaders == 0 {
                exec_shutdown_gather_workers(gatherstate)?;
                return Ok(None);
            }
            // memmove(&reader[nextreader], &reader[nextreader + 1],
            //         sizeof(ptr) * (nreaders - nextreader));
            //
            // Slide the tail of the working array left by one, dropping the
            // entry at `nextreader`. The PgVec still holds the old
            // `nreaders + 1` entries here; rotate the to-drop entry to the tail
            // and pop it (capacity, hence the spine charge, is retained — the C
            // array's allocation is likewise untouched). The reader handles are
            // executor/tqueue-owned (the C `pfree` frees only the pointer
            // array, never the readers), so popping carries no extra free.
            gatherstate.reader.as_mut_slice()[next..].rotate_left(1);
            gatherstate.reader.pop();
            //   if (gatherstate->nextreader >= gatherstate->nreaders)
            //       gatherstate->nextreader = 0;
            if gatherstate.nextreader >= gatherstate.nreaders {
                gatherstate.nextreader = 0;
            }
            continue;
        }

        // If we got a tuple, return it.
        if tup.is_some() {
            return Ok(tup);
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
        nvisited += 1;
        if nvisited >= gatherstate.nreaders {
            // If (still) running plan locally, return NULL so caller can
            // generate another tuple from the local copy of the plan.
            if gatherstate.need_to_scan_locally {
                return Ok(None);
            }

            // Nothing to do except wait for developments.
            //   (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
            //                    WAIT_EVENT_EXECUTE_GATHER);
            //   ResetLatch(MyLatch);
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
/// static void
/// ExecShutdownGatherWorkers(GatherState *node)
/// {
///     if (node->pei != NULL)
///         ExecParallelFinish(node->pei);
///     if (node->reader)
///         pfree(node->reader);
///     node->reader = NULL;
/// }
/// ```
fn exec_shutdown_gather_workers<'mcx>(node: &mut GatherStateData<'mcx>) -> PgResult<()> {
    if let Some(pei) = node.pei.as_mut() {
        execParallel::ExecParallelFinish::call(pei)?;
    }

    // Flush local copy of reader array: `if (node->reader) pfree(node->reader);
    // node->reader = NULL;`. The owned `reader` PgVec is replaced with an empty
    // one in the same context (releasing the old spine); the executor/tqueue-
    // owned reader handles it referenced carry no charge of their own (the C
    // `pfree` frees only the working array of pointers, not the readers).
    let alloc = *node.reader.allocator();
    node.reader = mcx::PgVec::new_in(alloc);
    Ok(())
}

/// `ExecShutdownGather(node)` — destroy the setup for parallel workers including
/// the parallel context.
///
/// ```c
/// void
/// ExecShutdownGather(GatherState *node)
/// {
///     ExecShutdownGatherWorkers(node);
///     if (node->pei != NULL)
///     {
///         ExecParallelCleanup(node->pei);
///         node->pei = NULL;
///     }
/// }
/// ```
pub fn ExecShutdownGather<'mcx>(node: &mut GatherStateData<'mcx>) -> PgResult<()> {
    exec_shutdown_gather_workers(node)?;

    // Now destroy the parallel context.
    //   ExecParallelCleanup(node->pei); node->pei = NULL;
    if let Some(pei) = node.pei.as_mut() {
        execParallel::ExecParallelCleanup::call(pei)?;
        node.pei = None;
    }
    Ok(())
}

/// `ExecReScanGather(node)` — prepare to re-scan the result of a Gather.
///
/// Ported 1:1 from nodeGather.c.
pub fn ExecReScanGather<'mcx>(
    node: &mut GatherStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    //   Gather    *gather = (Gather *) node->ps.plan;
    //   PlanState *outerPlan = outerPlanState(node);
    let rescan_param = gather_plan(node).rescan_param;
    let mcx = estate.es_query_cxt;

    // Make sure any existing workers are gracefully shut down.
    exec_shutdown_gather_workers(node)?;

    // Mark node so that shared state will be rebuilt at next call.
    node.initialized = false;

    // Set child node's chgParam to tell it that the next scan might deliver a
    // different set of rows within the leader process.
    //   if (gather->rescan_param >= 0)
    //       outerPlan->chgParam =
    //           bms_add_member(outerPlan->chgParam, gather->rescan_param);
    if rescan_param >= 0 {
        let outer = node.ps.lefttree.as_mut().expect("Gather has an outer plan");
        let chg = outer.ps_head_mut().chgParam.take();
        let updated = bitmapset::bms_add_member::call(mcx, chg, rescan_param)?;
        outer.ps_head_mut().chgParam = Some(updated);
    }

    // If chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.  Note: because this does nothing if we have a rescan_param,
    // it's currently guaranteed that parallel-aware child nodes will not see a
    // ReScan call until after they get a ReInitializeDSM call.
    //   if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    let chgparam_is_null = node
        .ps
        .lefttree
        .as_ref()
        .map(|o| o.ps_head().chgParam.is_none())
        .unwrap_or(true);
    if chgparam_is_null {
        let outer = node.ps.lefttree.as_mut().expect("Gather has an outer plan");
        execAmi::exec_re_scan::call(outer, estate)?;
    }
    Ok(())
}
