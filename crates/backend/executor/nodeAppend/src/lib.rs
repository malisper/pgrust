//! Port of `src/backend/executor/nodeAppend.c` — routines to handle Append
//! plan nodes.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitAppend`]    - initialize the append node
//! - [`ExecAppend`]        - retrieve the next tuple from the node
//! - [`ExecEndAppend`]     - shut down the append node
//! - [`ExecReScanAppend`]  - rescan the append node
//!
//! Each Append node contains a list of one or more subplans which are
//! iteratively processed (forwards or backwards): tuples are retrieved by
//! executing the `as_whichplan`th subplan until it stops returning tuples, at
//! which point that plan is shut down and the next started up. Append nodes
//! support unions and inheritance/partition queries, run-time partition
//! pruning, parallel-aware subplan selection, and asynchronous (FDW) subplans.
//!
//! The owned tree replaces C's `PlanState.state` back-pointer by threading
//! `&mut EStateData` explicitly. The `node->choose_next_subplan` C function
//! pointer is the [`AppendChooseStrategy`] sentinel selected at init / DSM
//! setup. `ExecAppend` returns the produced row's arena [`SlotId`], or `None`
//! for the C end-of-scan `ExecClearTuple(ps_ResultTupleSlot)` empty slot.
//!
//! Calls into unported owners — the bitmapset set ops (`nodes/bitmapset.c`),
//! the dispatch crates (`execProcnode.c`/`execAmi.c`/`execUtils.c`), the slot
//! ops (`execTuples.c`), run-time pruning (`execPartition.c`), the async
//! executor (`execAsync.c`), the parallel-DSM infrastructure
//! (`access/parallel.c`/`shm_toc.c`), the wait-event set
//! (`storage/ipc/waiteventset.c`), the latch (`storage/ipc/latch.c`),
//! `add_size` (`storage/ipc/shmem.c`), and `CHECK_FOR_INTERRUPTS`
//! (`tcop/postgres.c`) — go through those owners' seam crates and panic until
//! the owners land.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use transam_parallel as parallel;
use ::transam_parallel::shared_dsm_object;
use execAmi_seams as execAmi;
use execAsync_seams as execAsync;
use execPartition_seams as execPartition;
use execProcnode_seams as execProcnode;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use instrument_seams as instrument;
use nodeForeignscan_seams as nodeForeignscan;
use nodes_core_seams as bms;
use latch_seams as latch;
use ipc_shmem_seams as shmem;
use ::waiteventset_seams::WaitEventSet;
use lwlock_seams as lwlock;
use postgres_seams as tcop_postgres;
use init_small_seams as globals;

use ::mcx::{Mcx, PgBox};
use ::types_core::PGINVALID_SOCKET;
use ::types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::execparallel::{
    DsmSegmentHandle, ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle,
};
use ::nodes::executor::EXEC_FLAG_MARK;
use ::nodes::nodeappend::{
    pa_finished_offset, Append, AppendChooseStrategy, AppendStateData, AsyncRequestData,
    PaFinished, ParallelAppendState, ParallelAppendStateHandle,
};
use ::nodes::nodes::Node;
use ::nodes::{Bitmapset, EStateData, ScanDirectionIsForward, SlotId, TupleSlotKind};
use ::types_pgstat::wait_event::WAIT_EVENT_APPEND_READY;
use ::types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_SOCKET_READABLE};
use ::types_storage::LWLockMode;

/// `INVALID_SUBPLAN_INDEX` (nodeAppend.c).
const INVALID_SUBPLAN_INDEX: i32 = -1;
/// `EVENT_BUFFER_SIZE` (nodeAppend.c).
const EVENT_BUFFER_SIZE: i32 = 16;

// ===========================================================================
// Install this crate's implementations into its inward seam slots.
//
// The four parallel-Append public entry points now take the OWNED
// `&mut AppendStateData` (mirroring the already-owned nodeBitmapHeapscan /
// nodeHashjoin / nodeAgg parallel surfaces) and read node fields directly; the
// orthogonal DSM `shm_toc` estimate/allocate/lookup goes through the
// `backend-access-transam-parallel` seams (the DSM owner), keeping the DSM
// layout behind those.
//
// `backend-executor-nodeAppend-seams` still declares the four methods in the
// handle-based shape `execParallel.c` calls them with
// (`PlanStateHandle`/`ParallelContext|WorkerContextHandle`), because the
// parallel executor dispatches the per-node hooks generically over a
// `PlanState *`. Each seam slot is installed with a thin shim that performs the
// C `(AppendState *) node` cast through `resolve_append_state` (the executor
// `PlanState`-pointer registry, unported — panics until it lands) and then runs
// the real owned entry point above. This mirrors nodeHashjoin's
// `exec_hashjoin_*_shim` / nodeAgg's `resolve_agg_state` pq-seam shims.
// ===========================================================================

/// Install every seam in `backend-executor-nodeAppend-seams`.
pub fn init_seams() {
    nodeAppend_seams::exec_append_estimate::set(exec_append_estimate_shim);
    nodeAppend_seams::exec_append_initialize_dsm::set(
        exec_append_initialize_dsm_shim,
    );
    nodeAppend_seams::exec_append_reinitialize_dsm::set(
        exec_append_reinitialize_dsm_shim,
    );
    nodeAppend_seams::exec_append_initialize_worker::set(
        exec_append_initialize_worker_shim,
    );

    // execAsync.c is re-homed here (the Append node is its sole caller). The C
    // dispatch reaches the requestee/requestor through `areq`'s raw
    // back-pointers; the owned versions reach them through the `AppendStateData`
    // and the request's `request_index`.
    execAsync::exec_async_request::set(ExecAsyncRequest);
    execAsync::exec_async_configure_wait::set(ExecAsyncConfigureWait);
    execAsync::exec_async_notify::set(ExecAsyncNotify);
}

// ---------------------------------------------------------------------------
// Handle-resolving shims installed into `backend-executor-nodeAppend-seams`.
//
// `execParallel` holds the opaque `PlanStateHandle`; the C `ExecAppendEstimate`
// etc. begin with the `(AppendState *) node` cast. Recovering the live
// `AppendStateData` from the handle is the executor's `PlanState`-pointer
// registry — that pointer table is the unported executor surface, so each shim
// performs the cast through `resolve_append_state` (which panics until that
// registry lands) and then runs the real, owned entry point.
// ---------------------------------------------------------------------------

/// `(AppendState *) node` — recover the live `AppendStateData` a
/// `PlanStateHandle` refers to. The executor's `PlanState` pointer registry that
/// backs this lookup is not yet ported.
fn resolve_append_state<'mcx>(_node: PlanStateHandle) -> &'mcx mut AppendStateData<'mcx> {
    panic!(
        "backend-executor-nodeAppend: resolving a PlanStateHandle to the live AppendState needs \
         the executor PlanState pointer registry (unported); the (AppendState *) node cast in the \
         ExecAppend* parallel hooks cannot run yet"
    );
}

/// Seam shim for `ExecAppendEstimate`.
fn exec_append_estimate_shim(
    node: PlanStateHandle,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    ExecAppendEstimate(resolve_append_state(node), pcxt)
}

/// Seam shim for `ExecAppendInitializeDSM`.
fn exec_append_initialize_dsm_shim(
    node: PlanStateHandle,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    ExecAppendInitializeDSM(resolve_append_state(node), pcxt)
}

/// Seam shim for `ExecAppendReInitializeDSM`.
fn exec_append_reinitialize_dsm_shim(
    node: PlanStateHandle,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    ExecAppendReInitializeDSM(resolve_append_state(node), pcxt)
}

/// Seam shim for `ExecAppendInitializeWorker`.
fn exec_append_initialize_worker_shim(
    node: PlanStateHandle,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    ExecAppendInitializeWorker(resolve_append_state(node), pwcxt)
}

// ===========================================================================
// Node state machine.
// ===========================================================================

/// `ExecInitAppend(node, estate, eflags)` — begin all of the subscans of the
/// append node.
///
/// (This is potentially wasteful, since the entire result of the append node
/// may not be scanned, but this way all of the structures get allocated in the
/// executor's top-level memory block instead of that of the call to
/// `ExecAppend`.)
pub fn ExecInitAppend<'mcx>(
    mcx: Mcx<'mcx>,
    node: &'mcx Node<'mcx>,
    append: &'mcx Append<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, AppendStateData<'mcx>>> {
    // check for unsupported flags
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);

    // create new AppendState for our append node
    let mut appendstate = ::mcx::alloc_in(mcx, AppendStateData::make(mcx))?;
    // appendstate->ps.plan = (Plan *) node; appendstate->ps.state = estate;
    // appendstate->ps.ExecProcNode = ExecAppend;
    appendstate.ps.plan = Some(node);
    appendstate.ps.ExecProcNode = Some(exec_append_node);

    // Let choose_next_subplan_* function handle setting the first subplan.
    appendstate.as_whichplan = INVALID_SUBPLAN_INDEX;
    appendstate.as_syncdone = false;
    appendstate.as_begun = false;

    let nplans: i32;
    // The set of subplan indexes to initialize.
    let validsubplans: Option<PgBox<'mcx, Bitmapset<'mcx>>>;

    // If run-time partition pruning is enabled, then set that up now.
    if append.part_prune_index >= 0 {
        // Set up pruning data structure. This also initializes the set of
        // subplans to initialize (validsubplans) by taking into account the
        // result of performing initial pruning if any.
        let n_total = i32::try_from(append.appendplans.len())
            .map_err(|_| elog_error("Append has too many subplans"))?;
        let (prunestate, initially_valid) = execPartition::exec_init_partition_exec_pruning::call(
            mcx,
            &mut appendstate.ps,
            estate,
            n_total,
            append.part_prune_index,
            append.apprelids.as_deref(),
        )?;
        validsubplans = initially_valid;
        nplans = bms::bms_num_members::call(validsubplans.as_deref());

        // When no run-time pruning is required and there's at least one
        // subplan, we can fill as_valid_subplans immediately, preventing later
        // calls to ExecFindMatchingSubPlans.
        let do_exec_prune = prunestate.do_exec_prune;
        appendstate.as_prune_state = Some(prunestate);
        if !do_exec_prune && nplans > 0 {
            appendstate.as_valid_subplans =
                bms::bms_add_range::call(mcx, None, 0, nplans - 1)?;
            appendstate.as_valid_subplans_identified = true;
        }
    } else {
        nplans = i32::try_from(append.appendplans.len())
            .map_err(|_| elog_error("Append has too many subplans"))?;

        // When run-time partition pruning is not enabled we can just mark all
        // subplans as valid; they must also all be initialized.
        debug_assert!(nplans > 0);
        // The C aliases `as_valid_subplans` and `validsubplans` onto the same
        // `bms_add_range(NULL, 0, nplans-1)` set; the loop below only walks
        // `validsubplans` (never mutating it), so an equal-by-construction
        // working set is built for the walk — behaviorally identical.
        appendstate.as_valid_subplans = bms::bms_add_range::call(mcx, None, 0, nplans - 1)?;
        validsubplans = bms::bms_add_range::call(mcx, None, 0, nplans - 1)?;
        appendstate.as_valid_subplans_identified = true;
        appendstate.as_prune_state = None;
    }

    // appendplanstates = (PlanState **) palloc(nplans * sizeof(PlanState *));
    appendstate
        .appendplans
        .try_reserve(nplans as usize)
        .map_err(|_| mcx.oom(nplans as usize * core::mem::size_of::<usize>()))?;

    // call ExecInitNode on each of the valid plans to be executed and save the
    // results into the appendplanstates array. While at it, find out the first
    // valid partial plan.
    let mut j: i32 = 0;
    let mut asyncplans: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    let mut nasyncplans: i32 = 0;
    let mut firstvalid: i32 = nplans;
    let es_epq_active = estate_epq_active(estate);

    let mut i: i32 = -1;
    loop {
        // while ((i = bms_next_member(validsubplans, i)) >= 0)
        i = bms::bms_next_member::call(validsubplans.as_deref(), i);
        if i < 0 {
            break;
        }
        // Plan *initNode = (Plan *) list_nth(node->appendplans, i);
        let init_node = append
            .appendplans
            .get(i as usize)
            .ok_or_else(|| elog_error("Append subplan index out of range"))?;

        // Record async subplans. When executing EvalPlanQual, we treat them as
        // sync ones; don't do this when initializing an EvalPlanQual plan tree.
        if init_node.plan_head().async_capable && !es_epq_active {
            asyncplans = Some(bms::bms_add_member::call(mcx, asyncplans, j)?);
            nasyncplans += 1;
        }

        // Record the lowest appendplans index which is a valid partial plan.
        if i >= append.first_partial_plan && j < firstvalid {
            firstvalid = j;
        }

        // appendplanstates[j++] = ExecInitNode(initNode, estate, eflags);
        let child = execProcnode::exec_init_node::call(mcx, Some(init_node), estate, eflags)?;
        appendstate.appendplans.push(child);
        j += 1;
    }

    appendstate.as_first_partial_plan = firstvalid;
    appendstate.as_nplans = nplans;

    // Initialize Append's result tuple type and slot. If the child plans all
    // produce the same fixed slot type, we can use that slot type; otherwise
    // make a virtual slot. (The result slot is used only to return a null
    // tuple at end of execution; real tuples are returned in the children's
    // own result slots.)
    let appendops =
        execUtils::exec_get_common_slot_ops::call(&appendstate.appendplans[..], j, estate)?;
    match appendops {
        Some(ops) => {
            execTuples::exec_init_result_tuple_slot_tl::call(&mut appendstate.ps, estate, ops)?;
        }
        None => {
            execTuples::exec_init_result_tuple_slot_tl::call(
                &mut appendstate.ps,
                estate,
                TupleSlotKind::Virtual,
            )?;
            // show that the output slot type is not fixed
            appendstate.ps.resultopsset = true;
            appendstate.ps.resultopsfixed = false;
        }
    }

    // Initialize async state.
    appendstate.as_asyncplans = asyncplans;
    appendstate.as_nasyncplans = nasyncplans;
    appendstate.as_asyncrequests.clear();
    appendstate.as_asyncresults.clear();
    appendstate.as_nasyncresults = 0;
    appendstate.as_nasyncremain = 0;
    appendstate.as_needrequest = None;
    appendstate.as_valid_asyncplans = None;

    if nasyncplans > 0 {
        // appendstate->as_asyncrequests = palloc0(nplans * sizeof(AsyncRequest *));
        appendstate
            .as_asyncrequests
            .try_reserve(nplans as usize)
            .map_err(|_| mcx.oom(nplans as usize * core::mem::size_of::<usize>()))?;
        for _ in 0..nplans {
            appendstate.as_asyncrequests.push(None);
        }

        // i = -1; while ((i = bms_next_member(asyncplans, i)) >= 0)
        let mut i: i32 = -1;
        loop {
            i = bms::bms_next_member::call(appendstate.as_asyncplans.as_deref(), i);
            if i < 0 {
                break;
            }
            // areq = palloc(sizeof(AsyncRequest)); areq->requestor = appendstate;
            // areq->requestee = appendplanstates[i]; areq->request_index = i; ...
            //
            // The `requestor`/`requestee` raw back-pointers alias `appendstate`
            // and `appendplans[i]`; the owned async dispatch reaches them
            // through the node and `request_index` (the load-bearing field).
            let areq = ::mcx::alloc_in(
                mcx,
                AsyncRequestData {
                    request_index: i,
                    callback_pending: false,
                    request_complete: false,
                    result: None,
                },
            )?;
            *appendstate
                .as_asyncrequests
                .get_mut(i as usize)
                .ok_or_else(|| elog_error("Append async request index out of range"))? = Some(areq);
        }

        // appendstate->as_asyncresults = palloc0(nasyncplans * sizeof(TupleTableSlot *));
        appendstate
            .as_asyncresults
            .try_reserve(nasyncplans as usize)
            .map_err(|_| mcx.oom(nasyncplans as usize * core::mem::size_of::<usize>()))?;
        for _ in 0..nasyncplans {
            appendstate.as_asyncresults.push(None);
        }

        if appendstate.as_valid_subplans_identified {
            classify_matching_subplans(mcx, &mut appendstate)?;
        }
    }

    // Miscellaneous initialization
    appendstate.ps.ps_ProjInfo = None;

    // For parallel query, this will be overridden later.
    appendstate.choose_next_subplan = AppendChooseStrategy::Locally;

    Ok(appendstate)
}

/// `PlanState.ExecProcNode` adapter for an Append node: recover the
/// `AppendStateData` from the `PlanStateNode` enum and dispatch to [`ExecAppend`]
/// (mirrors the C function-pointer call through `node->ExecProcNode`).
fn exec_append_node<'mcx>(
    pstate: &mut ::nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let mcx = estate.es_query_cxt;
    let node = match pstate {
        ::nodes::PlanStateNode::Append(node) => node,
        other => panic!("castNode(AppendState, pstate) failed: tag {}", other.tag()),
    };
    ExecAppend(mcx, node, estate)
}

/// `ExecAppend(pstate)` — the `PlanState.ExecProcNode` callback; handles
/// iteration over multiple subplans. Returns the produced row's arena
/// [`SlotId`], or `None` for the C end-of-scan empty-slot return.
pub fn ExecAppend<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // If this is the first call after Init or ReScan, we need to do the
    // initialization work.
    if !node.as_begun {
        debug_assert!(node.as_whichplan == INVALID_SUBPLAN_INDEX);
        debug_assert!(!node.as_syncdone);

        // Nothing to do if there are no subplans.
        if node.as_nplans == 0 {
            clear_result_tuple_slot(node, estate)?;
            return Ok(None);
        }

        // If there are any async subplans, begin executing them.
        if node.as_nasyncplans > 0 {
            ExecAppendAsyncBegin(mcx, node, estate)?;
        }

        // If no sync subplan has been chosen, we must choose one before
        // proceeding.
        if !choose_next_subplan(mcx, node, estate)? && node.as_nasyncremain == 0 {
            clear_result_tuple_slot(node, estate)?;
            return Ok(None);
        }

        debug_assert!(
            node.as_syncdone || (node.as_whichplan >= 0 && node.as_whichplan < node.as_nplans)
        );

        // And we're initialized.
        node.as_begun = true;
    }

    loop {
        tcop_postgres::check_for_interrupts::call()?;

        // try to get a tuple from an async subplan if any
        if node.as_syncdone || !bms::bms_is_empty::call(node.as_needrequest.as_deref()) {
            if let Some(result) = ExecAppendAsyncGetNext(mcx, node, estate)? {
                return Ok(result);
            }
            debug_assert!(!node.as_syncdone);
            debug_assert!(bms::bms_is_empty::call(node.as_needrequest.as_deref()));
        }

        // figure out which sync subplan we are currently processing
        debug_assert!(node.as_whichplan >= 0 && node.as_whichplan < node.as_nplans);

        // get a tuple from the subplan
        let whichplan = node.as_whichplan as usize;
        let result = {
            let subnode = node
                .appendplans
                .get_mut(whichplan)
                .and_then(|slot| slot.as_deref_mut())
                .ok_or_else(|| elog_error("Append child plan state is missing"))?;
            execProcnode::exec_proc_node::call(subnode, estate)?
        };

        // if (!TupIsNull(result)) return result;
        if !tup_is_null(result, estate) {
            // If the subplan gave us something then return it as-is. We do NOT
            // make use of the result slot set up in ExecInitAppend.
            return Ok(result);
        }

        // wait or poll for async events if any. We do this before checking for
        // the end of iteration, because it might drain the remaining async
        // subplans.
        if node.as_nasyncremain > 0 {
            ExecAppendAsyncEventWait(mcx, node, estate)?;
        }

        // choose new sync subplan; if no sync/async subplans, we're done
        if !choose_next_subplan(mcx, node, estate)? && node.as_nasyncremain == 0 {
            clear_result_tuple_slot(node, estate)?;
            return Ok(None);
        }
    }
}

/// `ExecEndAppend(node)` — shuts down the subscans of the append node.
pub fn ExecEndAppend<'mcx>(
    node: &mut AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // for (i = 0; i < nplans; i++) ExecEndNode(appendplans[i]);
    let nplans = node.as_nplans as usize;
    for i in 0..nplans {
        let subnode = node
            .appendplans
            .get_mut(i)
            .and_then(|slot| slot.as_deref_mut())
            .ok_or_else(|| elog_error("Append child plan state is missing"))?;
        execProcnode::exec_end_node::call(subnode, estate)?;
    }
    Ok(())
}

/// `ExecReScanAppend(node)` — rescans the append node.
pub fn ExecReScanAppend<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let nasyncplans = node.as_nasyncplans;

    // If any PARAM_EXEC Params used in pruning expressions have changed, then
    // we'd better unset the valid subplans so that they are reselected for the
    // new parameter values.
    let prune_overlap = match node.as_prune_state.as_deref() {
        Some(prune) => bms::bms_overlap::call(
            node.ps.chgParam.as_deref(),
            prune.execparamids.as_deref(),
        ),
        None => false,
    };
    if prune_overlap {
        node.as_valid_subplans_identified = false;
        bms::bms_free::call(node.as_valid_subplans.take());
        node.as_valid_subplans = None;
        bms::bms_free::call(node.as_valid_asyncplans.take());
        node.as_valid_asyncplans = None;
    }

    let chg_param_present = node.ps.chgParam.is_some();
    let nplans = node.as_nplans as usize;
    for i in 0..nplans {
        // ExecReScan doesn't know about my subplans, so I have to do
        // changed-parameter signaling myself.
        if chg_param_present {
            // Split-borrow: clone the parent set so the child borrow and the
            // (read-only) parent chgParam can coexist (the C reads the live
            // set; the cloned copy is identical).
            let newchg = match node.ps.chgParam.as_deref() {
                Some(b) => b.clone_in(mcx)?,
                None => return Err(elog_error("ExecReScanAppend: chgParam present but missing")),
            };
            let subnode = node
                .appendplans
                .get_mut(i)
                .and_then(|slot| slot.as_deref_mut())
                .ok_or_else(|| elog_error("Append child plan state is missing"))?;
            execUtils::update_changed_param_set::call(mcx, subnode.ps_head_mut(), &newchg)?;
        }

        // If chgParam of subnode is not null then plan will be re-scanned by
        // first ExecProcNode or by first ExecAsyncRequest.
        let subnode = node
            .appendplans
            .get_mut(i)
            .and_then(|slot| slot.as_deref_mut())
            .ok_or_else(|| elog_error("Append child plan state is missing"))?;
        if subnode.ps_head().chgParam.is_none() {
            execAmi::exec_re_scan::call(subnode, estate)?;
        }
    }

    // Reset async state
    if nasyncplans > 0 {
        let mut i: i32 = -1;
        loop {
            i = bms::bms_next_member::call(node.as_asyncplans.as_deref(), i);
            if i < 0 {
                break;
            }
            if let Some(areq) = node
                .as_asyncrequests
                .get_mut(i as usize)
                .and_then(|slot| slot.as_deref_mut())
            {
                areq.callback_pending = false;
                areq.request_complete = false;
                areq.result = None;
            }
        }

        node.as_nasyncresults = 0;
        node.as_nasyncremain = 0;
        bms::bms_free::call(node.as_needrequest.take());
        node.as_needrequest = None;
    }

    // Let choose_next_subplan_* function handle setting the first subplan.
    node.as_whichplan = INVALID_SUBPLAN_INDEX;
    node.as_syncdone = false;
    node.as_begun = false;
    Ok(())
}

// ===========================================================================
//                      Parallel Append Support
// ===========================================================================

/// `ExecAppendEstimate(node, pcxt)` — compute the amount of space we'll need in
/// the parallel query DSM, and inform `pcxt->estimator` about our needs.
///
/// nodeAppend owns this control flow over its OWNED [`AppendStateData`]: the
/// `as_nplans` read and `pstate_len` write are plain field accesses on the node.
/// Only the orthogonal `shm_toc` reservation (which lives behind the DSM owner,
/// `access/parallel.c`/`shm_toc.c`) goes through `backend-access-transam-parallel`
/// seams.
pub fn ExecAppendEstimate(
    node: &mut AppendStateData<'_>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // node->pstate_len = add_size(offsetof(ParallelAppendState, pa_finished),
    //                             sizeof(bool) * node->as_nplans);
    let base = pa_finished_offset();
    // sizeof(bool) == sizeof(PaFinished) == 1 (the C `bool` array element).
    let tail = shmem::add_size::call(
        0,
        core::mem::size_of::<PaFinished>() * node.as_nplans as usize,
    )?;
    node.pstate_len = shmem::add_size::call(base, tail)?;

    // shm_toc_estimate_chunk(&pcxt->estimator, node->pstate_len);
    // shm_toc_estimate_keys(&pcxt->estimator, 1);
    let estimator = parallel::pcxt_estimator(pcxt);
    parallel::shm_toc_estimate_chunk(estimator, node.pstate_len);
    parallel::shm_toc_estimate_keys(estimator, 1);
    Ok(())
}

/// `ExecAppendInitializeDSM(node, pcxt)` — set up shared state for Parallel
/// Append.
///
/// nodeAppend owns the C control flow over its OWNED [`AppendStateData`] (the
/// `pstate_len`/`plan_node_id` reads, the `choose_next_subplan` strategy switch).
/// The orthogonal DSM allocation (`shm_toc_allocate`) is a real call into the
/// `access/parallel.c`/`shm_toc.c` owner via its seams. The DSM-resident
/// `ParallelAppendState` carrier is placed DIRECTLY in the just-allocated DSM
/// chunk through the keystone [`shared_dsm_object`] primitive: the flat-repr
/// header (`pa_lock` + `pa_next_plan`) followed by the zeroed
/// `pa_finished[node->as_nplans]` tail in the SAME chunk, so every worker that
/// `shm_toc_lookup`s the same key shares the one cross-process coordination
/// struct. `node->as_pstate` becomes the `Copy` handle into those bytes — exactly
/// the model nodeHashjoin's `ExecHashJoinInitializeDSM` uses for its
/// `ParallelHashJoinState`.
pub fn ExecAppendInitializeDSM(
    node: &mut AppendStateData<'_>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // pstate = shm_toc_allocate(pcxt->toc, node->pstate_len);
    let plan_node_id = append_plan_node_id(node);
    let nplans = node.as_nplans as usize;
    let pstate_len = node.pstate_len;
    let toc = parallel::pcxt_toc(pcxt);
    let chunk = parallel::shm_toc_allocate(toc, pstate_len);
    let seg = pcxt_seg_handle(pcxt);

    // memset(pstate, 0, node->pstate_len);
    // LWLockInitialize(&pstate->pa_lock, LWTRANCHE_PARALLEL_APPEND);
    //
    // Placement-init the flat-repr header in the chunk via the keystone (the
    // leader is the sole writer pre-launch); the `Default` header has a freshly
    // initialized LWLock state (zeroed) and `pa_next_plan = 0`, then
    // `LWLockInitialize` stamps the tranche id. The `pa_finished[]` tail bytes
    // are zeroed in the same window (C's `memset(pstate, 0, pstate_len)`).
    let header = shared_dsm_object::place_value::<ParallelAppendState>(
        seg,
        chunk,
        ParallelAppendState::default(),
    );
    shared_dsm_object::with_mut::<ParallelAppendState, ()>(seg, chunk, |pstate| {
        lwlock::lwlock_initialize::call(
            &mut pstate.pa_lock,
            ::types_storage::storage::LWTRANCHE_PARALLEL_APPEND,
        );
    });
    // Zero the `pa_finished[]` tail (the residual of `memset(pstate, 0, ...)`):
    // place a default (`false`) `PaFinished` byte for each subplan.
    let finished_off = pa_finished_offset();
    let finished_addr = chunk.0 + finished_off;
    init_pa_finished_tail(finished_addr, nplans);

    // shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, pstate);
    parallel::shm_toc_insert(toc, plan_node_id as u64, chunk);

    // node->as_pstate = pstate;
    node.as_pstate = Some(ParallelAppendStateHandle::from_shared(
        header,
        finished_addr,
        nplans,
    ));
    // node->choose_next_subplan = choose_next_subplan_for_leader;
    node.choose_next_subplan = AppendChooseStrategy::Leader;
    Ok(())
}

/// `ExecAppendReInitializeDSM(node, pcxt)` — reset shared state before
/// beginning a fresh scan.
///
/// In C this resets the DSM-resident `node->as_pstate`; over the owned node it
/// resets the [`ParallelAppendState`] carrier directly (the field is the C
/// `pstate->...` write). The carrier is only present once the (unported, see
/// [`ExecAppendInitializeDSM`]) DSM handoff has installed it.
pub fn ExecAppendReInitializeDSM(
    node: &mut AppendStateData<'_>,
    _pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // ParallelAppendState *pstate = node->as_pstate;
    // pstate->pa_next_plan = 0;
    // memset(pstate->pa_finished, 0, sizeof(bool) * node->as_nplans);
    //
    // The reinit window is the leader resetting shared state between scans after
    // all participants detached, so the relaxed atomic stores under the
    // (uncontended) shared `&self` are sound and behaviour-preserving.
    let nplans = node.as_nplans as usize;
    let pstate = node
        .as_pstate
        .ok_or_else(|| elog_error("ExecAppendReInitializeDSM: as_pstate is NULL"))?;
    pstate.header().set_pa_next_plan(0);
    for slot in pstate.finished().iter().take(nplans) {
        slot.set(false);
    }
    Ok(())
}

/// `ExecAppendInitializeWorker(node, pwcxt)` — copy relevant information from
/// the TOC into planstate, and initialize whatever is required to choose and
/// execute the optimal subplan.
///
/// nodeAppend owns the `plan_node_id` read and the `choose_next_subplan` worker
/// switch over the owned node; the `shm_toc_lookup` of the leader's chunk is a
/// real call into the DSM owner's seams. The worker recovers the SAME in-DSM
/// [`ParallelAppendState`] the leader placed in [`ExecAppendInitializeDSM`] by
/// attaching to the looked-up chunk through the keystone (the segment handle is
/// only the `'seg` lifetime carrier, never dereferenced), and stores the `Copy`
/// handle as `node->as_pstate`.
pub fn ExecAppendInitializeWorker(
    node: &mut AppendStateData<'_>,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    // node->as_pstate = shm_toc_lookup(pwcxt->toc, node->ps.plan->plan_node_id, false);
    let plan_node_id = append_plan_node_id(node);
    let nplans = node.as_nplans as usize;
    let toc = parallel::pwcxt_toc(pwcxt);
    let chunk = parallel::shm_toc_lookup(toc, plan_node_id as u64, false)
        .expect("ExecAppendInitializeWorker: shm_toc_lookup(noError=false) returned NULL");

    // The worker attaches to the leader-placed header (no init) and recovers the
    // `pa_finished[]` tail at `offsetof(ParallelAppendState, pa_finished)` in the
    // SAME chunk. The segment handle is only the `'seg` lifetime carrier.
    let seg = DsmSegmentHandle(0);
    let header = shared_dsm_object::attach::<ParallelAppendState>(seg, chunk);
    let finished_addr = chunk.0 + pa_finished_offset();
    node.as_pstate = Some(ParallelAppendStateHandle::from_shared(
        header,
        finished_addr,
        nplans,
    ));

    // node->choose_next_subplan = choose_next_subplan_for_worker;
    node.choose_next_subplan = AppendChooseStrategy::Worker;
    Ok(())
}

/// The `pcxt->seg` handle as the `DsmSegmentHandle` the keystone uses purely as
/// the `'seg` lifetime carrier (it never dereferences it). `None` (leader-only,
/// no DSM) maps to the sentinel `DsmSegmentHandle(0)`, the same convention
/// nodeSeqscan uses.
fn pcxt_seg_handle(pcxt: ParallelContextHandle) -> DsmSegmentHandle {
    match parallel::pcxt_seg(pcxt) {
        Some(seg) => seg,
        None => DsmSegmentHandle(0),
    }
}

/// Zero the `pa_finished[nplans]` flexible-array tail of a freshly-allocated
/// chunk (the residual of the C `memset(pstate, 0, pstate_len)`): placement-move
/// a default (`false`) [`PaFinished`] byte into each slot. The leader is the
/// sole writer pre-launch, so the raw placement writes over those in-segment
/// bytes are valid.
fn init_pa_finished_tail(finished_addr: usize, nplans: usize) {
    let base = finished_addr as *mut PaFinished;
    // SAFETY: `finished_addr` is `chunk + offsetof(ParallelAppendState,
    // pa_finished)` inside a real `shm_toc_allocate`'d chunk sized to hold
    // `nplans` contiguous `PaFinished` bytes (the `pstate_len` estimate). The
    // leader is the sole writer pre-launch, so each placement write is valid.
    for i in 0..nplans {
        unsafe { core::ptr::write(base.add(i), PaFinished::default()) };
    }
}

/// `node->ps.plan->plan_node_id` — the toc key the shared
/// [`ParallelAppendState`] is registered under.
#[inline]
fn append_plan_node_id(node: &AppendStateData<'_>) -> i32 {
    node.ps
        .plan
        .map(|n| n.plan_head().plan_node_id)
        .expect("AppendState.ps.plan")
}

/// Dispatch `node->choose_next_subplan(node)` (the C function-pointer call).
fn choose_next_subplan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    match node.choose_next_subplan {
        AppendChooseStrategy::Locally => choose_next_subplan_locally(mcx, node, estate),
        AppendChooseStrategy::Leader => choose_next_subplan_for_leader(mcx, node, estate),
        AppendChooseStrategy::Worker => choose_next_subplan_for_worker(mcx, node, estate),
    }
}

/// `choose_next_subplan_locally(node)` — choose next sync subplan for a
/// non-parallel-aware Append, returning false if there are no more.
fn choose_next_subplan_locally<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let mut whichplan = node.as_whichplan;

    // We should never be called when there are no subplans.
    debug_assert!(node.as_nplans > 0);

    // Nothing to do if syncdone.
    if node.as_syncdone {
        return Ok(false);
    }

    // If first call then have the bms member function choose the first valid
    // sync subplan by initializing whichplan to -1. If there happen to be no
    // valid sync subplans then the bms member function returns a negative
    // number, letting us exit returning false.
    if whichplan == INVALID_SUBPLAN_INDEX {
        if node.as_nasyncplans > 0 {
            // We'd have filled as_valid_subplans already.
            debug_assert!(node.as_valid_subplans_identified);
        } else if !node.as_valid_subplans_identified {
            node.as_valid_subplans = find_matching_subplans(mcx, node, estate)?;
            node.as_valid_subplans_identified = true;
        }

        whichplan = -1;
    }

    // Ensure whichplan is within the expected range.
    debug_assert!(whichplan >= -1 && whichplan <= node.as_nplans);

    let nextplan = if ScanDirectionIsForward(estate.es_direction) {
        bms::bms_next_member::call(node.as_valid_subplans.as_deref(), whichplan)
    } else {
        bms::bms_prev_member::call(node.as_valid_subplans.as_deref(), whichplan)
    };

    if nextplan < 0 {
        // Set as_syncdone if in async mode.
        if node.as_nasyncplans > 0 {
            node.as_syncdone = true;
        }
        return Ok(false);
    }

    node.as_whichplan = nextplan;

    Ok(true)
}

/// `choose_next_subplan_for_leader(node)` — try to pick a plan which doesn't
/// commit us to doing much work locally, so that as much work as possible is
/// done in the workers. Cheapest subplans are at the end.
fn choose_next_subplan_for_leader<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // Backward scan is not supported by parallel-aware plans.
    debug_assert!(ScanDirectionIsForward(estate.es_direction));
    // We should never be called when there are no subplans.
    debug_assert!(node.as_nplans > 0);

    let guard = lwlock_acquire(node)?;

    if node.as_whichplan != INVALID_SUBPLAN_INDEX {
        // Mark just-completed subplan as finished.
        pa_finished_set(node, node.as_whichplan, true);
    } else {
        // Start with last subplan.
        node.as_whichplan = node.as_nplans - 1;

        // If we've yet to determine the valid subplans then do so now. If
        // run-time pruning is disabled then the valid subplans will always be
        // set to all subplans.
        if !node.as_valid_subplans_identified {
            node.as_valid_subplans = find_matching_subplans(mcx, node, estate)?;
            node.as_valid_subplans_identified = true;

            // Mark each invalid plan as finished to allow the loop below to
            // select the first valid subplan.
            mark_invalid_subplans_as_finished(node)?;
        }
    }

    // Loop until we find a subplan to execute.
    while pa_finished_get(node, node.as_whichplan) {
        if node.as_whichplan == 0 {
            pa_next_plan_set(node, INVALID_SUBPLAN_INDEX);
            node.as_whichplan = INVALID_SUBPLAN_INDEX;
            guard.release()?;
            return Ok(false);
        }

        // We needn't pay attention to as_valid_subplans here as all invalid
        // plans have been marked as finished.
        node.as_whichplan -= 1;
    }

    // If non-partial, immediately mark as finished.
    if node.as_whichplan < node.as_first_partial_plan {
        pa_finished_set(node, node.as_whichplan, true);
    }

    guard.release()?;

    Ok(true)
}

/// `choose_next_subplan_for_worker(node)` — choose next subplan for a
/// parallel-aware Append, returning false if there are no more.
///
/// We start from the first plan and advance through the list; when we get back
/// to the end, we loop back to the first partial plan. This assigns the
/// non-partial plans first in order of descending cost and then spreads out the
/// workers as evenly as possible across the remaining partial plans.
fn choose_next_subplan_for_worker<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // Backward scan is not supported by parallel-aware plans.
    debug_assert!(ScanDirectionIsForward(estate.es_direction));
    // We should never be called when there are no subplans.
    debug_assert!(node.as_nplans > 0);

    let guard = lwlock_acquire(node)?;

    // Mark just-completed subplan as finished.
    if node.as_whichplan != INVALID_SUBPLAN_INDEX {
        pa_finished_set(node, node.as_whichplan, true);
    }
    // If we've yet to determine the valid subplans then do so now.
    else if !node.as_valid_subplans_identified {
        node.as_valid_subplans = find_matching_subplans(mcx, node, estate)?;
        node.as_valid_subplans_identified = true;

        mark_invalid_subplans_as_finished(node)?;
    }

    // If all the plans are already done, we have nothing to do.
    if pa_next_plan_get(node) == INVALID_SUBPLAN_INDEX {
        guard.release()?;
        return Ok(false);
    }

    // Save the plan from which we are starting the search.
    node.as_whichplan = pa_next_plan_get(node);

    // Loop until we find a valid subplan to execute.
    while pa_finished_get(node, pa_next_plan_get(node)) {
        let nextplan =
            bms::bms_next_member::call(node.as_valid_subplans.as_deref(), pa_next_plan_get(node));
        if nextplan >= 0 {
            // Advance to the next valid plan.
            pa_next_plan_set(node, nextplan);
        } else if node.as_whichplan > node.as_first_partial_plan {
            // Try looping back to the first valid partial plan, if there is
            // one. If there isn't, arrange to bail out below.
            let nextplan = bms::bms_next_member::call(
                node.as_valid_subplans.as_deref(),
                node.as_first_partial_plan - 1,
            );
            let v = if nextplan < 0 {
                node.as_whichplan
            } else {
                nextplan
            };
            pa_next_plan_set(node, v);
        } else {
            // At last plan, and either there are no partial plans or we've
            // tried them all. Arrange to bail out.
            pa_next_plan_set(node, node.as_whichplan);
        }

        if pa_next_plan_get(node) == node.as_whichplan {
            // We've tried everything!
            pa_next_plan_set(node, INVALID_SUBPLAN_INDEX);
            guard.release()?;
            return Ok(false);
        }
    }

    // Pick the plan we found, and advance pa_next_plan one more time.
    node.as_whichplan = pa_next_plan_get(node);
    let advanced =
        bms::bms_next_member::call(node.as_valid_subplans.as_deref(), pa_next_plan_get(node));
    pa_next_plan_set(node, advanced);

    // If there are no more valid plans then try setting the next plan to the
    // first valid partial plan.
    if pa_next_plan_get(node) < 0 {
        let nextplan = bms::bms_next_member::call(
            node.as_valid_subplans.as_deref(),
            node.as_first_partial_plan - 1,
        );

        if nextplan >= 0 {
            pa_next_plan_set(node, nextplan);
        } else {
            // There are no valid partial plans, and we already chose the last
            // non-partial plan; so flag that there's nothing more for our
            // fellow workers to do.
            pa_next_plan_set(node, INVALID_SUBPLAN_INDEX);
        }
    }

    // If non-partial, immediately mark as finished.
    if node.as_whichplan < node.as_first_partial_plan {
        pa_finished_set(node, node.as_whichplan, true);
    }

    guard.release()?;

    Ok(true)
}

/// `mark_invalid_subplans_as_finished(node)` — marks the
/// `ParallelAppendState`'s `pa_finished` as true for each invalid subplan.
/// Only called for parallel Append with run-time pruning enabled.
fn mark_invalid_subplans_as_finished(node: &mut AppendStateData<'_>) -> PgResult<()> {
    // Only valid to call this while in parallel Append mode.
    debug_assert!(node.as_pstate.is_some());
    // Shouldn't have been called when run-time pruning is not enabled.
    debug_assert!(node.as_prune_state.is_some());

    // Nothing to do if all plans are valid.
    if bms::bms_num_members::call(node.as_valid_subplans.as_deref()) == node.as_nplans {
        return Ok(());
    }

    // Mark all non-valid plans as finished.
    for i in 0..node.as_nplans {
        if !bms::bms_is_member::call(i, node.as_valid_subplans.as_deref()) {
            pa_finished_set(node, i, true);
        }
    }
    Ok(())
}

// ===========================================================================
//                      Asynchronous Append Support
// ===========================================================================

/// `ExecAppendAsyncBegin(node)` — begin executing designated async-capable
/// subplans.
fn ExecAppendAsyncBegin<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Backward scan is not supported by async-aware Appends.
    debug_assert!(ScanDirectionIsForward(estate.es_direction));
    // We should never be called when there are no subplans / async subplans.
    debug_assert!(node.as_nplans > 0);
    debug_assert!(node.as_nasyncplans > 0);

    // If we've yet to determine the valid subplans then do so now.
    if !node.as_valid_subplans_identified {
        node.as_valid_subplans = find_matching_subplans(mcx, node, estate)?;
        node.as_valid_subplans_identified = true;

        classify_matching_subplans(mcx, node)?;
    }

    // Initialize state variables.
    node.as_syncdone = bms::bms_is_empty::call(node.as_valid_subplans.as_deref());
    node.as_nasyncremain = bms::bms_num_members::call(node.as_valid_asyncplans.as_deref());

    // Nothing to do if there are no valid async subplans.
    if node.as_nasyncremain == 0 {
        return Ok(());
    }

    // Make a request for each of the valid async subplans.
    let mut i: i32 = -1;
    loop {
        i = bms::bms_next_member::call(node.as_valid_asyncplans.as_deref(), i);
        if i < 0 {
            break;
        }
        {
            let areq = node
                .as_asyncrequests
                .get(i as usize)
                .and_then(|slot| slot.as_deref())
                .ok_or_else(|| elog_error("Append async request is missing"))?;
            debug_assert!(areq.request_index == i);
            debug_assert!(!areq.callback_pending);
        }
        execAsync::exec_async_request::call(node, i, estate)?;
    }
    Ok(())
}

/// `ExecAppendAsyncGetNext(node, &result)` — get the next tuple from any of the
/// asynchronous subplans. `Some(result)` is the C `*result` + `true`; `None`
/// the C `false` (continue with the sync subplans). The inner `Option<SlotId>`
/// distinguishes a real slot from the empty/NULL `ExecClearTuple` result.
fn ExecAppendAsyncGetNext<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<SlotId>>> {
    // We should never be called when there are no valid async subplans.
    debug_assert!(node.as_nasyncremain > 0);

    // Request a tuple asynchronously.
    if let Some(result) = ExecAppendAsyncRequest(node, estate)? {
        return Ok(Some(result));
    }

    while node.as_nasyncremain > 0 {
        tcop_postgres::check_for_interrupts::call()?;

        // Wait or poll for async events.
        ExecAppendAsyncEventWait(mcx, node, estate)?;

        // Request a tuple asynchronously.
        if let Some(result) = ExecAppendAsyncRequest(node, estate)? {
            return Ok(Some(result));
        }

        // Break from loop if there's any sync subplan that isn't complete.
        if !node.as_syncdone {
            break;
        }
    }

    // If all sync subplans are complete, we're totally done scanning the given
    // node. Otherwise, we're done with the asynchronous stuff but must continue
    // scanning the sync subplans.
    if node.as_syncdone {
        debug_assert!(node.as_nasyncremain == 0);
        clear_result_tuple_slot(node, estate)?;
        return Ok(Some(None));
    }

    Ok(None)
}

/// `ExecAppendAsyncRequest(node, &result)` — request a tuple asynchronously.
fn ExecAppendAsyncRequest<'mcx>(
    node: &mut AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<SlotId>>> {
    // Nothing to do if there are no async subplans needing a new request.
    if bms::bms_is_empty::call(node.as_needrequest.as_deref()) {
        debug_assert!(node.as_nasyncresults == 0);
        return Ok(None);
    }

    // If there are any asynchronously-generated results that have not yet been
    // returned, we have nothing to do; just return one of them.
    if node.as_nasyncresults > 0 {
        node.as_nasyncresults -= 1;
        let result = node
            .as_asyncresults
            .get(node.as_nasyncresults as usize)
            .copied()
            .ok_or_else(|| elog_error("Append async result is missing"))?;
        return Ok(Some(result));
    }

    // Make a new request for each of the async subplans that need it.
    let needrequest = node.as_needrequest.take();
    let mut i: i32 = -1;
    loop {
        i = bms::bms_next_member::call(needrequest.as_deref(), i);
        if i < 0 {
            break;
        }
        execAsync::exec_async_request::call(node, i, estate)?;
    }
    bms::bms_free::call(needrequest);

    // Return one of the asynchronously-generated results if any.
    if node.as_nasyncresults > 0 {
        node.as_nasyncresults -= 1;
        let result = node
            .as_asyncresults
            .get(node.as_nasyncresults as usize)
            .copied()
            .ok_or_else(|| elog_error("Append async result is missing"))?;
        return Ok(Some(result));
    }

    Ok(None)
}

/// `ExecAppendAsyncEventWait(node)` — wait or poll for file descriptor events
/// and fire callbacks.
fn ExecAppendAsyncEventWait<'mcx>(
    _mcx: Mcx<'mcx>,
    node: &mut AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut nevents = node.as_nasyncplans + 2;
    let timeout: i64 = if node.as_syncdone { -1 } else { 0 };

    // We should never be called when there are no valid async subplans.
    debug_assert!(node.as_nasyncremain > 0);

    // The C `node->as_eventset` is created and freed within this call (NULL
    // everywhere else); the owned model holds it as a stack guard whose `Drop`
    // is `FreeWaitEventSet` (the C explicit frees become scope exits).
    let eventset = WaitEventSet::create(nevents)?;
    eventset.add_event(WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, None, None)?;

    // Give each waiting subplan a chance to add an event.
    let mut i: i32 = -1;
    loop {
        i = bms::bms_next_member::call(node.as_asyncplans.as_deref(), i);
        if i < 0 {
            break;
        }
        let pending = node
            .as_asyncrequests
            .get(i as usize)
            .and_then(|slot| slot.as_deref())
            .map(|areq| areq.callback_pending)
            .unwrap_or(false);
        if pending {
            execAsync::exec_async_configure_wait::call(node, i, estate)?;
        }
    }

    // No need for further processing if none of the subplans configured any
    // events.
    if eventset.num_registered_events() == 1 {
        return Ok(());
    }

    // Add the process latch to the set, so that we wake up to process the
    // standard interrupts with CHECK_FOR_INTERRUPTS(). For historical reasons
    // this MUST be added after the ExecAsyncConfigureWait() calls (postgres_fdw
    // relies on `GetNumRegisteredWaitEvents(set) == 1`).
    let my_latch = latch::my_latch::call();
    eventset.add_event(WL_LATCH_SET, PGINVALID_SOCKET, Some(my_latch), None)?;

    // Return at most EVENT_BUFFER_SIZE events in one call.
    if nevents > EVENT_BUFFER_SIZE {
        nevents = EVENT_BUFFER_SIZE;
    }

    // If the timeout is -1, wait until at least one event occurs. If the
    // timeout is 0, poll for events, but do not wait at all.
    let mut occurred =
        [::types_storage::waiteventset::WaitEvent::default(); EVENT_BUFFER_SIZE as usize];
    let occurred = &mut occurred[..nevents as usize];
    let noccurred = eventset.wait(timeout, occurred, WAIT_EVENT_APPEND_READY)?;
    if noccurred == 0 {
        return Ok(());
    }

    // Deliver notifications.
    for w in occurred.iter().take(noccurred as usize) {
        // Each waiting subplan should have registered its wait event with
        // `user_data` pointing back to its AsyncRequest. C recovers the single
        // matched request via `(AsyncRequest *) w->user_data`; the owned model
        // carries the request's `request_index` (== the subplan index in
        // `as_asyncrequests`, see ExecAppendAsyncBegin) as the non-aliasing
        // `user_data` key, so the lookup recovers exactly that one request.
        if (w.events & WL_SOCKET_READABLE) != 0 {
            let request_index = w
                .user_data
                .ok_or_else(|| elog_error("Append async wait event has no AsyncRequest"))?;
            let pending = node
                .as_asyncrequests
                .get(request_index as usize)
                .and_then(|slot| slot.as_deref())
                .map(|areq| areq.callback_pending)
                .unwrap_or(false);
            if pending {
                // Mark it as no longer needing a callback. We must do this
                // before dispatching the callback in case the callback resets
                // the flag.
                if let Some(areq) = node
                    .as_asyncrequests
                    .get_mut(request_index as usize)
                    .and_then(|slot| slot.as_deref_mut())
                {
                    areq.callback_pending = false;
                }

                // Do the actual work.
                execAsync::exec_async_notify::call(node, request_index, estate)?;
            }
        }

        // Handle standard interrupts.
        if (w.events & WL_LATCH_SET) != 0 {
            latch::reset_latch_my_latch::call();
            tcop_postgres::check_for_interrupts::call()?;
        }
    }
    Ok(())
}

// ===========================================================================
// `execAsync.c` — re-homed onto the Append node (its sole caller). The C
// dispatch switches on `nodeTag(areq->requestee)` and reaches the requestee
// `PlanState` / requestor `AppendState` through `areq`'s raw back-pointers; the
// owned versions reach them through the `AppendStateData` and the request's
// `request_index` (== the requestee's index in `appendplans`/`as_asyncrequests`,
// see `ExecAppendAsyncBegin`). Only `ForeignScanState` is async-capable.
// ===========================================================================

/// Detach the request record at `request_index` from the node so the requestee
/// (`appendplans[request_index]`) and the requestor (`node`) can be borrowed
/// alongside it. The C `AsyncRequest *areq` is a separately-`palloc`ed struct,
/// so detaching is borrow-only bookkeeping (the slot is restored before return).
fn take_async_request<'mcx>(
    node: &mut AppendStateData<'mcx>,
    request_index: i32,
) -> PgResult<PgBox<'mcx, AsyncRequestData>> {
    node.as_asyncrequests
        .get_mut(request_index as usize)
        .and_then(Option::take)
        .ok_or_else(|| elog_error("Append async request is missing"))
}

/// `n` for the C `InstrStopNode(instr, TupIsNull(areq->result) ? 0.0 : 1.0)`.
fn tuples_for_result(areq: &AsyncRequestData) -> f64 {
    if areq.result.is_none() {
        0.0
    } else {
        1.0
    }
}

/// Run the requestee `ForeignScanState`'s async callback, or `elog(ERROR)` for a
/// non-async-capable requestee (the C `switch (nodeTag(areq->requestee))`
/// default). `dispatch` selects which of the three `ExecAsyncForeignScan*`
/// entry points runs.
fn dispatch_requestee<'mcx>(
    requestee: &mut ::nodes::PlanStateNode<'mcx>,
    areq: &mut AsyncRequestData,
    which: AsyncDispatch,
) -> PgResult<()> {
    match requestee {
        ::nodes::PlanStateNode::ForeignScan(fss) => match which {
            AsyncDispatch::Request => {
                nodeForeignscan::exec_async_foreignscan_request::call(fss, areq)
            }
            AsyncDispatch::ConfigureWait => {
                nodeForeignscan::exec_async_foreignscan_configure_wait::call(fss, areq)
            }
            AsyncDispatch::Notify => {
                nodeForeignscan::exec_async_foreignscan_notify::call(fss, areq)
            }
        },
        other => Err(PgError::error(alloc::format!(
            "unrecognized node type: {}",
            other.tag().0
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)),
    }
}

/// Which `ExecAsyncForeignScan*` callback [`dispatch_requestee`] runs.
#[derive(Clone, Copy)]
enum AsyncDispatch {
    Request,
    ConfigureWait,
    Notify,
}

/// `ExecAsyncRequest(areq)` (execAsync.c) — asynchronously request a tuple from
/// the requestee `node.appendplans[request_index]`.
pub fn ExecAsyncRequest<'mcx>(
    node: &mut AppendStateData<'mcx>,
    request_index: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let mut areq = take_async_request(node, request_index)?;
    let res = (|| {
        let requestee = node
            .appendplans
            .get_mut(request_index as usize)
            .and_then(Option::as_deref_mut)
            .ok_or_else(|| elog_error("Append async requestee is missing"))?;

        // if (areq->requestee->chgParam != NULL) ExecReScan(areq->requestee);
        if requestee.ps_head().chgParam.is_some() {
            execAmi::exec_re_scan::call(requestee, estate)?;
        }

        // must provide our own instrumentation support
        if let Some(instr) = requestee.ps_head_mut().instrument.as_deref_mut() {
            instrument::instr_start_node::call(instr)?;
        }

        dispatch_requestee(requestee, &mut areq, AsyncDispatch::Request)?;

        // ExecAsyncResponse(areq) -> ExecAsyncAppendResponse(areq). C reads
        // `TupIsNull(areq->result)` for the instrumentation tuple count *after*
        // the response (which never clears `areq->result`); the owned response
        // moves the result out, so the count is captured here beforehand.
        let n = tuples_for_result(&areq);
        ExecAsyncAppendResponse(mcx, node, &mut areq)?;

        // must provide our own instrumentation support
        if let Some(requestee) = node
            .appendplans
            .get_mut(request_index as usize)
            .and_then(Option::as_deref_mut)
        {
            if let Some(instr) = requestee.ps_head_mut().instrument.as_deref_mut() {
                instrument::instr_stop_node::call(instr, n)?;
            }
        }
        Ok(())
    })();
    *node
        .as_asyncrequests
        .get_mut(request_index as usize)
        .ok_or_else(|| elog_error("Append async request index out of range"))? = Some(areq);
    res
}

/// `ExecAsyncConfigureWait(areq)` (execAsync.c) — give the requestee a chance to
/// register its file descriptor in the caller's wait-event set.
pub fn ExecAsyncConfigureWait<'mcx>(
    node: &mut AppendStateData<'mcx>,
    request_index: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = estate;
    let mut areq = take_async_request(node, request_index)?;
    let res = (|| {
        let requestee = node
            .appendplans
            .get_mut(request_index as usize)
            .and_then(Option::as_deref_mut)
            .ok_or_else(|| elog_error("Append async requestee is missing"))?;

        if let Some(instr) = requestee.ps_head_mut().instrument.as_deref_mut() {
            instrument::instr_start_node::call(instr)?;
        }

        dispatch_requestee(requestee, &mut areq, AsyncDispatch::ConfigureWait)?;

        if let Some(instr) = requestee.ps_head_mut().instrument.as_deref_mut() {
            instrument::instr_stop_node::call(instr, 0.0)?;
        }
        Ok(())
    })();
    *node
        .as_asyncrequests
        .get_mut(request_index as usize)
        .ok_or_else(|| elog_error("Append async request index out of range"))? = Some(areq);
    res
}

/// `ExecAsyncNotify(areq)` (execAsync.c) — notify the requestee that the file
/// descriptor it was waiting on has become ready.
pub fn ExecAsyncNotify<'mcx>(
    node: &mut AppendStateData<'mcx>,
    request_index: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let mut areq = take_async_request(node, request_index)?;
    let res = (|| {
        let requestee = node
            .appendplans
            .get_mut(request_index as usize)
            .and_then(Option::as_deref_mut)
            .ok_or_else(|| elog_error("Append async requestee is missing"))?;

        if let Some(instr) = requestee.ps_head_mut().instrument.as_deref_mut() {
            instrument::instr_start_node::call(instr)?;
        }

        dispatch_requestee(requestee, &mut areq, AsyncDispatch::Notify)?;

        // ExecAsyncResponse(areq) -> ExecAsyncAppendResponse(areq); capture the
        // instrumentation tuple count before the response moves the result out
        // (C reads `TupIsNull(areq->result)` afterward — the response never
        // clears it).
        let n = tuples_for_result(&areq);
        ExecAsyncAppendResponse(mcx, node, &mut areq)?;

        if let Some(requestee) = node
            .appendplans
            .get_mut(request_index as usize)
            .and_then(Option::as_deref_mut)
        {
            if let Some(instr) = requestee.ps_head_mut().instrument.as_deref_mut() {
                instrument::instr_stop_node::call(instr, n)?;
            }
        }
        Ok(())
    })();
    *node
        .as_asyncrequests
        .get_mut(request_index as usize)
        .ok_or_else(|| elog_error("Append async request index out of range"))? = Some(areq);
    res
}

/// `ExecAsyncAppendResponse(areq)` — receive a response from an asynchronous
/// request we made. The C derives `node` from `areq->requestor`; the owned tree
/// has no aliasing back-pointer, so the requestor (the `AppendState`) is passed
/// explicitly.
pub fn ExecAsyncAppendResponse<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut AppendStateData<'mcx>,
    areq: &mut AsyncRequestData,
) -> PgResult<()> {
    // TupleTableSlot *slot = areq->result;
    // Nothing to do if the request is pending.
    if !areq.request_complete {
        debug_assert!(areq.callback_pending);
        return Ok(());
    }

    // If the result is NULL or an empty slot, there's nothing more to do.
    // Stage-2: areq.result is the delivered tuple's arena id — Some only when a
    // real row was produced (ExecAsyncRequestDone contract), so presence is the
    // TupIsNull verdict on this dormant path.
    if areq.result.is_none() {
        debug_assert!(!areq.callback_pending);
        node.as_nasyncremain -= 1;
        return Ok(());
    }

    // Save result so we can return it.
    debug_assert!(node.as_nasyncresults < node.as_nasyncplans);
    let slot = areq.result.take();
    let idx = node.as_nasyncresults as usize;
    if let Some(dst) = node.as_asyncresults.get_mut(idx) {
        *dst = slot;
    } else {
        return Err(elog_error("Append async result slot out of range"));
    }
    node.as_nasyncresults += 1;

    // Mark the subplan that returned a result as ready for a new request. We
    // don't launch another one here immediately because it might complete.
    node.as_needrequest = Some(bms::bms_add_member::call(
        mcx,
        node.as_needrequest.take(),
        areq.request_index,
    )?);
    Ok(())
}

/// `classify_matching_subplans(node)` — classify the node's `as_valid_subplans`
/// into sync ones and async ones, adjust it to contain sync ones only, and save
/// async ones in the node's `as_valid_asyncplans`.
fn classify_matching_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut AppendStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(node.as_valid_subplans_identified);
    debug_assert!(node.as_valid_asyncplans.is_none());

    // Nothing to do if there are no valid subplans.
    if bms::bms_is_empty::call(node.as_valid_subplans.as_deref()) {
        node.as_syncdone = true;
        node.as_nasyncremain = 0;
        return Ok(());
    }

    // Nothing to do if there are no valid async subplans.
    if !bms::bms_overlap::call(
        node.as_valid_subplans.as_deref(),
        node.as_asyncplans.as_deref(),
    ) {
        node.as_nasyncremain = 0;
        return Ok(());
    }

    // Get valid async subplans.
    let valid_asyncplans = bms::bms_intersect::call(
        mcx,
        node.as_asyncplans.as_deref(),
        node.as_valid_subplans.as_deref(),
    )?;

    // Adjust the valid subplans to contain sync subplans only.
    node.as_valid_subplans =
        bms::bms_del_members::call(node.as_valid_subplans.take(), valid_asyncplans.as_deref());

    // Save valid async subplans.
    node.as_valid_asyncplans = valid_asyncplans;
    Ok(())
}

// ===========================================================================
// In-crate helpers.
// ===========================================================================

/// `node->as_valid_subplans = ExecFindMatchingSubPlans(node->as_prune_state,
/// false, NULL)`. The C reads `node->as_prune_state` (which is non-NULL on this
/// path); the owned form borrows it mutably for the seam call.
fn find_matching_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let prune = node
        .as_prune_state
        .as_deref_mut()
        .ok_or_else(|| elog_error("Append has no partition prune state"))?;
    execPartition::exec_find_matching_subplans::call(mcx, prune, estate, false)
}

/// `ExecClearTuple(node->ps.ps_ResultTupleSlot)` — clear the node's result slot
/// in place (the C end-of-scan empty-slot return).
fn clear_result_tuple_slot<'mcx>(
    node: &AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(id) = node.ps.ps_ResultTupleSlot {
        execTuples::exec_clear_tuple::call(estate, id)?;
    }
    Ok(())
}

/// `TupIsNull(slot)` over a returned arena slot id: `None` is the C NULL
/// pointer; `Some(id)` resolves through the `estate` arena and tests
/// `TTS_EMPTY`.
#[inline]
fn tup_is_null(slot: Option<SlotId>, estate: &EStateData<'_>) -> bool {
    match slot {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `estate->es_epq_active != NULL` — whether an EvalPlanQual is currently
/// active. The C test at nodeAppend.c:204 is `estate->es_epq_active == NULL`,
/// which the call site spells as `!estate_epq_active(estate)`.
/// The trimmed `EState` does not carry `es_epq_active` yet (it lands with the
/// EPQ port); no Append-over-EPQ path reaches here, so EPQ is never active.
fn estate_epq_active(_estate: &EStateData<'_>) -> bool {
    false
}

/// `pstate->pa_finished[i]` — the relaxed atomic load issued while holding
/// `pa_lock` (the C plain read on the DSM-resident `bool` slot).
fn pa_finished_get(node: &AppendStateData<'_>, i: i32) -> bool {
    node.as_pstate
        .and_then(|pstate| pstate.finished().get(i as usize).map(|f| f.get()))
        .unwrap_or(false)
}

/// `pstate->pa_finished[i] = value` — the relaxed atomic store under `pa_lock`.
fn pa_finished_set(node: &mut AppendStateData<'_>, i: i32, value: bool) {
    if let Some(pstate) = node.as_pstate {
        if let Some(slot) = pstate.finished().get(i as usize) {
            slot.set(value);
        }
    }
}

/// `pstate->pa_next_plan` — the relaxed atomic load under `pa_lock`.
fn pa_next_plan_get(node: &AppendStateData<'_>) -> i32 {
    node.as_pstate
        .map(|pstate| pstate.header().pa_next_plan())
        .unwrap_or(INVALID_SUBPLAN_INDEX)
}

/// `pstate->pa_next_plan = value` — the relaxed atomic store under `pa_lock`.
fn pa_next_plan_set(node: &mut AppendStateData<'_>, value: i32) {
    if let Some(pstate) = node.as_pstate {
        pstate.header().set_pa_next_plan(value);
    }
}

/// `LWLockAcquire(&pstate->pa_lock, LW_EXCLUSIVE)`, returning the guard whose
/// `Drop` is the abort-path release and whose `release()` is the C
/// `LWLockRelease(&pstate->pa_lock)`.
///
/// `pa_lock` lives in the DSM-resident [`ParallelAppendState`] — the SAME
/// in-segment bytes every worker maps — reached through the `Copy`
/// [`ParallelAppendStateHandle`]. The lock is `Sync` over its atomic state and
/// is never moved; the held lock object is separate from the backend-local
/// `AppendState` fields the holder mutates (`as_whichplan`,
/// `as_valid_subplans`) and from the DSM-resident `pa_next_plan`/`pa_finished[]`
/// (interior-mutable), so deriving the lock borrow off the handle does not
/// freeze the `&mut node` the surrounding code needs.
fn lwlock_acquire<'a>(
    node: &AppendStateData<'_>,
) -> PgResult<lwlock::LWLockGuard<'a>> {
    let pstate = node
        .as_pstate
        .ok_or_else(|| elog_error("Append has no parallel state"))?;
    // SAFETY: `pa_lock` is in the DSM-resident `ParallelAppendState` header
    // (live for the DSM segment, which outlives the handle); it is `Sync` over
    // its interior-mutable atomic state and is never moved.
    let lock: &'a ::types_storage::LWLock =
        unsafe { &*core::ptr::addr_of!(pstate.header().pa_lock) };
    lwlock::lwlock_acquire::call(
        lock,
        LWLockMode::LW_EXCLUSIVE,
        globals::my_proc_number::call(),
    )
}

/// `elog(ERROR, msg)` — internal error with `ERRCODE_INTERNAL_ERROR`.
fn elog_error(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

extern crate alloc;
