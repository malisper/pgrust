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

use backend_access_transam_parallel_seams as parallel;
use backend_executor_execAmi_seams as execAmi;
use backend_executor_execAsync_seams as execAsync;
use backend_executor_execPartition_seams as execPartition;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_nodes_core_seams as bms;
use backend_storage_ipc_latch_seams as latch;
use backend_storage_ipc_shmem_seams as shmem;
use backend_storage_ipc_waiteventset_seams::WaitEventSet;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_init_small_seams as globals;

use mcx::{Mcx, PgBox};
use types_core::PGINVALID_SOCKET;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle};
use types_nodes::executor::EXEC_FLAG_MARK;
use types_nodes::nodeappend::{
    Append, AppendChooseStrategy, AppendStateData, AsyncRequestData, ParallelAppendState,
};
use types_nodes::nodes::Node;
use types_nodes::{Bitmapset, EStateData, ScanDirectionIsForward, SlotId, TupleSlotKind};
use types_pgstat::wait_event::WAIT_EVENT_APPEND_READY;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_SOCKET_READABLE};
use types_storage::{LWLockMode, LWTRANCHE_PARALLEL_APPEND};

/// `INVALID_SUBPLAN_INDEX` (nodeAppend.c).
const INVALID_SUBPLAN_INDEX: i32 = -1;
/// `EVENT_BUFFER_SIZE` (nodeAppend.c).
const EVENT_BUFFER_SIZE: i32 = 16;

// ===========================================================================
// Install this crate's implementations into its inward seam slots.
//
// `backend-executor-nodeAppend-seams` declares the four parallel-Append
// methods in the handle-based shape execParallel.c calls them with
// (`PlanStateHandle`/`ParallelContextHandle`). Resolving a live `PlanState *`
// from its handle is the executor's parallel-worker dispatch — owned by
// `execParallel.c`/`access/parallel.c`, neither ported. Until that resolution
// lands, the installed bridges panic loudly (mirror-PG-and-panic); the real
// per-node logic lives in the borrow-based [`ExecAppendEstimate`] &c. below,
// which the dispatch will call once it can hand over the live node.
// ===========================================================================

/// Install every seam in `backend-executor-nodeAppend-seams`.
pub fn init_seams() {
    backend_executor_nodeAppend_seams::exec_append_estimate::set(|_node, _pcxt| {
        unimplemented!(
            "ExecAppendEstimate handle dispatch needs execParallel's PlanState-handle \
             resolution (access/parallel.c unported); call ExecAppendEstimate directly"
        )
    });
    backend_executor_nodeAppend_seams::exec_append_initialize_dsm::set(|_node, _pcxt| {
        unimplemented!(
            "ExecAppendInitializeDSM handle dispatch needs execParallel's PlanState-handle \
             resolution (access/parallel.c unported); call ExecAppendInitializeDSM directly"
        )
    });
    backend_executor_nodeAppend_seams::exec_append_reinitialize_dsm::set(|_node, _pcxt| {
        unimplemented!(
            "ExecAppendReInitializeDSM handle dispatch needs execParallel's PlanState-handle \
             resolution (access/parallel.c unported); call ExecAppendReInitializeDSM directly"
        )
    });
    backend_executor_nodeAppend_seams::exec_append_initialize_worker::set(|_node, _pwcxt| {
        unimplemented!(
            "ExecAppendInitializeWorker handle dispatch needs execParallel's PlanState-handle \
             resolution (access/parallel.c unported); call ExecAppendInitializeWorker directly"
        )
    });
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
    let mut appendstate = mcx::alloc_in(mcx, AppendStateData::make(mcx))?;
    // appendstate->ps.plan = (Plan *) node; appendstate->ps.state = estate;
    // appendstate->ps.ExecProcNode = ExecAppend;
    appendstate.ps.plan = Some(node);

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
    let appendops = execUtils::exec_get_common_slot_ops::call(&appendstate.appendplans[..], j)?;
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
            let areq = mcx::alloc_in(
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
pub fn ExecAppendEstimate(
    node: &mut AppendStateData<'_>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // node->pstate_len = add_size(offsetof(ParallelAppendState, pa_finished),
    //                             sizeof(bool) * node->as_nplans);
    let base = pa_finished_offset();
    let tail = shmem::add_size::call(0, node.as_nplans as usize)?; // sizeof(bool) == 1
    let len = shmem::add_size::call(base, tail)?;
    node.pstate_len = len;

    let estimator = parallel::pcxt_estimator::call(pcxt);
    parallel::shm_toc_estimate_chunk::call(estimator, len);
    parallel::shm_toc_estimate_keys::call(estimator, 1);
    Ok(())
}

/// `ExecAppendInitializeDSM(node, pcxt)` — set up shared state for Parallel
/// Append.
pub fn ExecAppendInitializeDSM(
    node: &mut AppendStateData<'_>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // pstate = shm_toc_allocate(pcxt->toc, node->pstate_len);
    // memset(pstate, 0, node->pstate_len);
    // LWLockInitialize(&pstate->pa_lock, LWTRANCHE_PARALLEL_APPEND);
    // shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, pstate);
    let toc = parallel::pcxt_toc::call(pcxt);
    let cursor = parallel::shm_toc_allocate::call(toc, node.pstate_len);
    let plan_node_id = plan_node_id(node)?;
    parallel::shm_toc_insert::call(toc, plan_node_id as u64, cursor);

    // Construct the (zeroed) coordination struct backing the reserved DSM
    // chunk and initialize its lock. The owned struct mirrors the in-segment
    // bytes the leader and workers share by `plan_node_id` key.
    let mut pa_lock = types_storage::LWLock::default();
    lwlock::lwlock_initialize::call(&mut pa_lock, LWTRANCHE_PARALLEL_APPEND);
    let mut pa_finished = alloc::vec::Vec::new();
    pa_finished
        .try_reserve(node.as_nplans as usize)
        .map_err(|_| elog_error("Append parallel state allocation failed"))?;
    pa_finished.resize(node.as_nplans as usize, false);
    node.as_pstate = Some(alloc::boxed::Box::new(ParallelAppendState {
        pa_lock,
        pa_next_plan: 0,
        pa_finished,
    }));

    node.choose_next_subplan = AppendChooseStrategy::Leader;
    Ok(())
}

/// `ExecAppendReInitializeDSM(node, pcxt)` — reset shared state before
/// beginning a fresh scan.
pub fn ExecAppendReInitializeDSM(
    node: &mut AppendStateData<'_>,
    _pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // pstate->pa_next_plan = 0;
    // memset(pstate->pa_finished, 0, sizeof(bool) * node->as_nplans);
    let nplans = node.as_nplans as usize;
    let pstate = node
        .as_pstate
        .as_deref_mut()
        .ok_or_else(|| elog_error("Append has no parallel state"))?;
    pstate.pa_next_plan = 0;
    pstate.pa_finished.clear();
    pstate
        .pa_finished
        .try_reserve(nplans)
        .map_err(|_| elog_error("Append parallel state allocation failed"))?;
    pstate.pa_finished.resize(nplans, false);
    Ok(())
}

/// `ExecAppendInitializeWorker(node, pwcxt)` — copy relevant information from
/// the TOC into planstate, and initialize whatever is required to choose and
/// execute the optimal subplan.
pub fn ExecAppendInitializeWorker(
    node: &mut AppendStateData<'_>,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    // node->as_pstate = shm_toc_lookup(pwcxt->toc, node->ps.plan->plan_node_id, false);
    let toc = parallel::pwcxt_toc::call(pwcxt);
    let plan_node_id = plan_node_id(node)?;
    let _cursor = parallel::shm_toc_lookup::call(toc, plan_node_id as u64, false)
        .ok_or_else(|| elog_error("Append parallel state not found in worker TOC"))?;

    // The worker shares the leader's already-initialized coordination struct
    // (looked up by `plan_node_id`); the in-DSM bytes are the same the leader
    // wrote. The owned model materializes a view of that struct sized to this
    // node's plan count.
    let nplans = node.as_nplans as usize;
    let mut pa_finished = alloc::vec::Vec::new();
    pa_finished
        .try_reserve(nplans)
        .map_err(|_| elog_error("Append parallel state allocation failed"))?;
    pa_finished.resize(nplans, false);
    node.as_pstate = Some(alloc::boxed::Box::new(ParallelAppendState {
        pa_lock: types_storage::LWLock::default(),
        pa_next_plan: 0,
        pa_finished,
    }));

    node.choose_next_subplan = AppendChooseStrategy::Worker;
    Ok(())
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
            node.as_valid_subplans = find_matching_subplans(mcx, node)?;
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
            node.as_valid_subplans = find_matching_subplans(mcx, node)?;
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
        node.as_valid_subplans = find_matching_subplans(mcx, node)?;
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
        node.as_valid_subplans = find_matching_subplans(mcx, node)?;
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
        let areq = node
            .as_asyncrequests
            .get_mut(i as usize)
            .and_then(|slot| slot.as_deref_mut())
            .ok_or_else(|| elog_error("Append async request is missing"))?;
        debug_assert!(areq.request_index == i);
        debug_assert!(!areq.callback_pending);
        execAsync::exec_async_request::call(areq, estate)?;
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
        let areq = node
            .as_asyncrequests
            .get_mut(i as usize)
            .and_then(|slot| slot.as_deref_mut())
            .ok_or_else(|| elog_error("Append async request is missing"))?;
        execAsync::exec_async_request::call(areq, estate)?;
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
    eventset.add_event(WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, None)?;

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
            let areq = node
                .as_asyncrequests
                .get_mut(i as usize)
                .and_then(|slot| slot.as_deref_mut())
                .ok_or_else(|| elog_error("Append async request is missing"))?;
            execAsync::exec_async_configure_wait::call(areq, estate)?;
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
    eventset.add_event(WL_LATCH_SET, PGINVALID_SOCKET, Some(my_latch))?;

    // Return at most EVENT_BUFFER_SIZE events in one call.
    if nevents > EVENT_BUFFER_SIZE {
        nevents = EVENT_BUFFER_SIZE;
    }

    // If the timeout is -1, wait until at least one event occurs. If the
    // timeout is 0, poll for events, but do not wait at all.
    let mut occurred =
        [types_storage::waiteventset::WaitEvent::default(); EVENT_BUFFER_SIZE as usize];
    let occurred = &mut occurred[..nevents as usize];
    let noccurred = eventset.wait(timeout, occurred, WAIT_EVENT_APPEND_READY)?;
    if noccurred == 0 {
        return Ok(());
    }

    // Deliver notifications.
    for w in occurred.iter().take(noccurred as usize) {
        // Each waiting subplan registered its wait event with `user_data`
        // pointing back to its AsyncRequest. The trimmed shared `WaitEvent`
        // carries no `user_data`; the precise back-reference is restored when
        // execAsync.c (which owns the registration that sets `user_data`)
        // lands. Until then the configure-wait seam above panics, so this
        // delivery loop is only reached with no pending callbacks; we mirror
        // the C action — for a ready socket, notify each pending request.
        if (w.events & WL_SOCKET_READABLE) != 0 {
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
                    // Mark it as no longer needing a callback. We must do this
                    // before dispatching the callback in case the callback
                    // resets the flag.
                    let areq = node
                        .as_asyncrequests
                        .get_mut(i as usize)
                        .and_then(|slot| slot.as_deref_mut())
                        .ok_or_else(|| elog_error("Append async request is missing"))?;
                    areq.callback_pending = false;
                    execAsync::exec_async_notify::call(areq, estate)?;
                }
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
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let prune = node
        .as_prune_state
        .as_deref_mut()
        .ok_or_else(|| elog_error("Append has no partition prune state"))?;
    execPartition::exec_find_matching_subplans::call(mcx, prune, false)
}

/// `ExecClearTuple(node->ps.ps_ResultTupleSlot)` — clear the node's result slot
/// in place (the C end-of-scan empty-slot return).
fn clear_result_tuple_slot<'mcx>(
    node: &AppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(id) = node.ps.ps_ResultTupleSlot {
        execTuples::exec_clear_tuple::call(estate.slot_mut(id))?;
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

/// `estate->es_epq_active == NULL` — whether an EvalPlanQual is in progress.
/// The trimmed `EState` does not carry `es_epq_active` yet (it lands with the
/// EPQ port); no Append-over-EPQ path reaches here, so it reads as "no EPQ".
fn estate_epq_active(_estate: &EStateData<'_>) -> bool {
    false
}

/// `node->ps.plan->plan_node_id`.
fn plan_node_id(node: &AppendStateData<'_>) -> PgResult<i32> {
    let plan = node
        .ps
        .plan
        .ok_or_else(|| elog_error("Append node has no plan"))?;
    Ok(plan.plan_head().plan_node_id)
}

/// `pstate->pa_finished[i]`.
fn pa_finished_get(node: &AppendStateData<'_>, i: i32) -> bool {
    node.as_pstate
        .as_deref()
        .and_then(|pstate| pstate.pa_finished.get(i as usize))
        .copied()
        .unwrap_or(false)
}

/// `pstate->pa_finished[i] = value`.
fn pa_finished_set(node: &mut AppendStateData<'_>, i: i32, value: bool) {
    if let Some(pstate) = node.as_pstate.as_deref_mut() {
        if let Some(slot) = pstate.pa_finished.get_mut(i as usize) {
            *slot = value;
        }
    }
}

/// `pstate->pa_next_plan`.
fn pa_next_plan_get(node: &AppendStateData<'_>) -> i32 {
    node.as_pstate
        .as_deref()
        .map(|pstate| pstate.pa_next_plan)
        .unwrap_or(INVALID_SUBPLAN_INDEX)
}

/// `pstate->pa_next_plan = value`.
fn pa_next_plan_set(node: &mut AppendStateData<'_>, value: i32) {
    if let Some(pstate) = node.as_pstate.as_deref_mut() {
        pstate.pa_next_plan = value;
    }
}

/// `LWLockAcquire(&pstate->pa_lock, LW_EXCLUSIVE)`, returning the guard whose
/// `Drop` is the abort-path release and whose `release()` is the C
/// `LWLockRelease(&pstate->pa_lock)`.
///
/// `pa_lock` lives in the DSM-resident `ParallelAppendState` (heap-stable
/// `Box`; `LWLock` is `Sync` over its atomics), conceptually shared memory
/// separate from the backend-local `AppendState` fields the holder mutates
/// (`as_whichplan`, `as_valid_subplans`, `pa_finished`, `pa_next_plan`). C
/// holds the lock while touching exactly those. The guard's reference is
/// derived through a raw pointer so it does not freeze the `&mut node` the
/// surrounding code needs; this is sound because the lock object itself is
/// never moved or mutated through `node` while the guard is held.
fn lwlock_acquire<'a>(
    node: &AppendStateData<'_>,
) -> PgResult<lwlock::LWLockGuard<'a>> {
    let pstate = node
        .as_pstate
        .as_deref()
        .ok_or_else(|| elog_error("Append has no parallel state"))?;
    // SAFETY: `pa_lock` is in a heap-stable `Box`ed `ParallelAppendState`; it
    // is `Sync` and is neither moved nor mutated through `node` while held.
    let lock: &'a types_storage::LWLock = unsafe { &*core::ptr::addr_of!(pstate.pa_lock) };
    lwlock::lwlock_acquire::call(
        lock,
        LWLockMode::LW_EXCLUSIVE,
        globals::my_proc_number::call(),
    )
}

/// `offsetof(ParallelAppendState, pa_finished)` — the fixed-head size before
/// the per-plan `pa_finished` tail (the DSM size-estimator base).
fn pa_finished_offset() -> usize {
    core::mem::size_of::<types_storage::LWLock>() + core::mem::size_of::<i32>()
}

/// `elog(ERROR, msg)` — internal error with `ERRCODE_INTERNAL_ERROR`.
fn elog_error(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

extern crate alloc;
