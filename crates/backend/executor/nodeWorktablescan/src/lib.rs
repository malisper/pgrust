//! Port of `src/backend/executor/nodeWorktablescan.c` — routines to handle
//! `WorkTableScan` nodes.
//!
//! A `WorkTableScan` node reads from the *working table* tuplestore owned by an
//! ancestor `RecursiveUnion` node (the engine behind `WITH RECURSIVE`). The link
//! to that ancestor's executor state (`rustate`) cannot be resolved during init
//! (there are corner cases where this node's init runs before the
//! `RecursiveUnion`'s), so it is found lazily on the first [`ExecWorkTableScan`]
//! call via the work-table `Param` slot reserved for it (`plan->wtParam`). The
//! scan is intentionally forward-only.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitWorkTableScan`]  — create and initialize the node
//! - [`ExecWorkTableScan`]      — the `PlanState.ExecProcNode` callback
//! - [`ExecReScanWorkTableScan`]— rescan the relation
//!
//! ## Owned-model shape vs. the C ABI
//!
//! In C the descendant `WorkTableScan` recovers a *live pointer* to the ancestor
//! `RecursiveUnionState` from the work-table `Param` slot
//! (`node->rustate = DatumGetPointer(es_param_exec_vals[wtParam].value)`) and
//! reads `rustate->working_table`. The owned model cannot hold a live `&mut`
//! alias to the (self-borrowing) ancestor node, so — exactly as the CTE leader's
//! shared store was hoisted into `EState.es_cte_shared` keyed by `cteParam` —
//! the `RecursiveUnion`'s working-table tuplestore (and its result rowtype) is
//! hoisted into [`EStateData::es_recursive_shared`] keyed by `wtParam`
//! ([`RecursiveUnionSharedState`]). `WorkTableScanStateData.rustate` becomes the
//! resolved `wtParam` index (the C pointer → index translation, like
//! `execPlan → ExecPlanLink`), and both the engine and this scan reach the
//! working table by index through `&mut EStateData`.
//!
//! Like the sibling tuplestore-backed scan node (`nodeNamedtuplestorescan`),
//! this node does **not** reproduce the `execScan.c` driver: it routes the scan
//! loop (interrupts, qual, projection, EvalPlanQual) through the single generic
//! [`exec_scan_worktable`](execScan_seams::exec_scan_worktable)
//! driver, passing its own `WorkTableScanNext`/`WorkTableScanRecheck` access and
//! recheck methods. The init helpers route directly to the real
//! execUtils/execTuples/execScan/execExpr owners.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use execExpr_seams as execExpr;
use execScan_seams as execScan;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use sort_storage_seams as tuplestore;

use ::mcx::{alloc_in, PgBox};
use ::types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::nodes::execnodes::{EStateData, RecursiveUnionSharedState};
use ::nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::nodes::nodeworktablescan::{WorkTableScan, WorkTableScanStateData};
use ::nodes::{SlotId, Tuplestorestate, TupleSlotKind};

fn internal(msg: &str) -> PgError {
    PgError::error(alloc::string::String::from(msg)).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

// ===========================================================================
// Side-table accessors: the ancestor RecursiveUnion's shared state keyed by the
// resolved `wtParam` (see `RecursiveUnionSharedState` / `es_recursive_shared`).
// ===========================================================================

/// The resolved `wtParam` index of this node's shared
/// [`RecursiveUnionSharedState`] (set by [`resolve_rustate`] on the first
/// `ExecWorkTableScan`). Errors if the node has not yet resolved.
#[inline]
fn rustate_idx(node: &WorkTableScanStateData<'_>) -> PgResult<usize> {
    node.rustate
        .and_then(|p| usize::try_from(p).ok())
        .ok_or_else(|| internal("WorkTableScanState rustate not resolved"))
}

/// `&mut es_recursive_shared[wtParam]`, which must already be claimed (the C
/// "the RecursiveUnion published it before any WorkTableScan runs" invariant).
fn shared_mut<'a, 'mcx>(
    node: &WorkTableScanStateData<'mcx>,
    estate: &'a mut EStateData<'mcx>,
) -> PgResult<&'a mut RecursiveUnionSharedState<'mcx>> {
    let idx = rustate_idx(node)?;
    estate
        .es_recursive_shared
        .get_mut(idx)
        .and_then(|s| s.as_mut())
        .ok_or_else(|| internal("WorkTableScan: RecursiveUnion shared state not resolved"))
}

// ===========================================================================
//                              Scan Support
// ===========================================================================

/// `WorkTableScanNext` — the workhorse access method for [`ExecWorkTableScan`].
///
/// Fetches the next tuple from the ancestor `RecursiveUnion`'s working-table
/// tuplestore into the node's scan slot, returning `Ok(true)` when a tuple was
/// loaded, `Ok(false)` once the work table is exhausted.
///
/// Note: we intentionally do not support backward scan. Although it would take
/// only a couple more lines here, it would force `nodeRecursiveunion.c` to
/// create the tuplestore with backward scan enabled, which has a performance
/// cost. We are also assuming that this node is the only reader of the
/// worktable, so we don't need a private read pointer for the tuplestore, nor
/// do we need to tell `tuplestore_gettupleslot` to copy.
fn WorkTableScanNext<'mcx>(
    node: &mut WorkTableScanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // get information from the estate and scan state
    //   Assert(ScanDirectionIsForward(node->ss.ps.state->es_direction));

    // Get the next tuple from tuplestore. Return NULL (here: leave slot empty,
    // signal `false`) if no more tuples.
    //   tuplestorestate = node->rustate->working_table;
    //   slot = node->ss.ss_ScanTupleSlot;
    //   (void) tuplestore_gettupleslot(tuplestorestate, true, false, slot);
    //   return slot;
    let scanslot = node
        .ss
        .ss_ScanTupleSlot
        .expect("WorkTableScanNext: ss_ScanTupleSlot not initialized");

    // The shared `working_table` lives in `es_recursive_shared[wtParam]`. Move
    // the `PgBox` out so the tuplestore call can hold `&mut estate` without a
    // self-alias (the `cte_table` take/put pattern), then restore it.
    let mut working: PgBox<'mcx, Tuplestorestate<'mcx>> = shared_mut(node, estate)?
        .working_table
        .take()
        .ok_or_else(|| internal("WorkTableScan: working_table is NULL"))?;
    let got = tuplestore::tuplestore_gettupleslot::call(&mut working, true, false, scanslot, estate);
    // Restore (even on the error path) so a later call / rescan can reach it.
    if let Ok(shared) = shared_mut(node, estate) {
        shared.working_table = Some(working);
    }
    got
}

/// `WorkTableScanRecheck` — access-method routine to recheck a tuple in
/// EvalPlanQual. Nothing to check for a work-table scan, so it always succeeds.
fn WorkTableScanRecheck<'mcx>(
    _node: &mut WorkTableScanStateData<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // nothing to check
    Ok(true)
}

// ===========================================================================
//                          Public node entry points
// ===========================================================================

/// `ExecWorkTableScan(pstate)` — the `PlanState.ExecProcNode` callback.
///
/// Scans the worktable sequentially and returns the next qualifying tuple
/// (stored in the node's result/scan slot) by calling the generic
/// [`exec_scan_worktable`](execScan::exec_scan_worktable) driver with the
/// work-table access-method functions. On the first call it resolves the
/// ancestor `RecursiveUnion`'s shared state and finishes scan-type and
/// projection setup (deferred from init).
///
/// ```c
/// return ExecScan(&node->ss,
///                 (ExecScanAccessMtd) WorkTableScanNext,
///                 (ExecScanRecheckMtd) WorkTableScanRecheck);
/// ```
pub fn ExecWorkTableScan<'mcx>(
    node: &mut WorkTableScanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // On the first call, find the ancestor RecursiveUnion's state via the Param
    // slot reserved for it. (We can't do this during node init because there are
    // corner cases where we'll get the init call before the RecursiveUnion does.)
    if node.rustate.is_none() {
        //   param = &(estate->es_param_exec_vals[plan->wtParam]);
        //   Assert(param->execPlan == NULL); Assert(!param->isnull);
        //   node->rustate =
        //       castNode(RecursiveUnionState, DatumGetPointer(param->value));
        //   Assert(node->rustate);
        resolve_rustate(node, estate)?;

        // The scan tuple type (ie, the rowtype we expect to find in the work
        // table) is the same as the result rowtype of the ancestor
        // RecursiveUnion node. Note this depends on the assumption that
        // RecursiveUnion doesn't allow projection.
        //   ExecAssignScanType(&node->ss, ExecGetResultType(&node->rustate->ps));
        exec_assign_scan_type_from_rustate(node, estate)?;

        // Now we can initialize the projection info. This must be completed
        // before we can call ExecScan().
        //   ExecAssignScanProjectionInfo(&node->ss);
        execScan::exec_assign_scan_projection_info_worktable::call(node, estate)?;
    }

    execScan::exec_scan_worktable::call(node, estate, WorkTableScanNext, WorkTableScanRecheck)
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitWorkTableScan`]:
/// `castNode(WorkTableScanState, pstate)` then run [`ExecWorkTableScan`].
fn exec_work_table_scan_node<'mcx>(
    pstate: &mut ::nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        ::nodes::PlanStateNode::WorkTableScan(node) => node,
        other => panic!("castNode(WorkTableScanState, pstate) failed: {other:?}"),
    };
    ExecWorkTableScan(node, estate)
}

/// Resolve `node->rustate` from the work-table `Param` slot. In C this recovers
/// the live `RecursiveUnionState *` pointer the ancestor published; here the
/// shared state lives in `EState.es_recursive_shared[wtParam]`, so "resolution"
/// is recording the `wtParam` index (after asserting the ancestor has already
/// claimed the side-table slot, the C `Assert(node->rustate)`).
///
/// ```c
/// ParamExecData *param = &(estate->es_param_exec_vals[plan->wtParam]);
/// Assert(param->execPlan == NULL);
/// Assert(!param->isnull);
/// node->rustate = castNode(RecursiveUnionState, DatumGetPointer(param->value));
/// Assert(node->rustate);
/// ```
fn resolve_rustate<'mcx>(
    node: &mut WorkTableScanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let wt_param = wtparam_of(node);
    let idx = usize::try_from(wt_param)
        .map_err(|_| internal("WorkTableScan: invalid wtParam"))?;
    // Assert(node->rustate): the RecursiveUnion must have published its shared
    // state into the side-table slot before any descendant WorkTableScan runs.
    if estate
        .es_recursive_shared
        .get(idx)
        .and_then(|s| s.as_ref())
        .is_none()
    {
        return Err(internal(
            "WorkTableScan: RecursiveUnion has not published its shared state",
        ));
    }
    node.rustate = Some(wt_param);
    Ok(())
}

/// `ExecAssignScanType(&node->ss, ExecGetResultType(&node->rustate->ps))` — the
/// scan tuple type equals the ancestor `RecursiveUnion`'s result rowtype. The
/// rowtype is the `result_tupdesc` the `RecursiveUnion` stashed in the side-table
/// at publish time (the C reads it off the live ancestor PlanState).
fn exec_assign_scan_type_from_rustate<'mcx>(
    node: &mut WorkTableScanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // Clone the ancestor's result descriptor out of the side-table (C shares the
    // pointer; the slot setup needs its own owned descriptor).
    let tupdesc: types_tuple::heaptuple::TupleDesc<'mcx> = {
        let shared = shared_mut(node, estate)?;
        match shared.result_tupdesc.as_deref() {
            Some(td) => Some(alloc_in(mcx, td.clone_in(mcx)?)?),
            None => None,
        }
    };
    // ExecSetSlotDescriptor(scanstate->ss_ScanTupleSlot, tupDesc).
    let slot = node
        .ss
        .ss_ScanTupleSlot
        .expect("ExecAssignScanType: ss_ScanTupleSlot not initialized");
    execTuples::exec_set_slot_descriptor::call(estate, slot, tupdesc)
}

/// `ExecInitWorkTableScan(node, estate, eflags)` — create and initialize a
/// `WorkTableScan` node.
///
/// The state tree is allocated in `estate.es_query_cxt` (C: `makeNode` in the
/// per-query context current during `ExecInitNode`), so initialization is
/// fallible on OOM. Projection info is *not* initialized here — see
/// [`ExecWorkTableScan`] for details.
pub fn ExecInitWorkTableScan<'mcx>(
    node: &'mcx ::nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, WorkTableScanStateData<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // WorkTableScan *node — the enclosing plan-tree node. Panics if it is not a
    // `WorkTableScan` (the C `castNode`).
    let plan: &'mcx WorkTableScan<'mcx> = node
        .as_worktablescan()
        .expect("castNode(WorkTableScan, node) failed");

    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    // WorkTableScan should not have any children.
    //   Assert(outerPlan(node) == NULL); Assert(innerPlan(node) == NULL);
    debug_assert!(plan.scan.plan.lefttree.is_none());
    debug_assert!(plan.scan.plan.righttree.is_none());

    // create new WorkTableScanState for node
    //   scanstate = makeNode(WorkTableScanState);
    //   scanstate->ss.ps.plan = (Plan *) node;
    //   scanstate->ss.ps.state = estate;
    //   scanstate->ss.ps.ExecProcNode = ExecWorkTableScan;
    //   scanstate->rustate = NULL; /* we'll set this later */
    let mut scanstate = alloc_in(mcx, WorkTableScanStateData::default())?;
    scanstate.ss.ps.plan = Some(node);
    scanstate.ss.ps.ExecProcNode = Some(exec_work_table_scan_node);
    scanstate.rustate = None;

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &scanstate->ss.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut scanstate.ss.ps)?;

    // tuple table initialization
    //   ExecInitResultTypeTL(&scanstate->ss.ps);
    execTuples::exec_init_result_type_tl::call(&mut scanstate.ss.ps, estate)?;

    // signal that return type is not yet known
    //   scanstate->ss.ps.resultopsset = true;
    //   scanstate->ss.ps.resultopsfixed = false;
    scanstate.ss.ps.resultopsset = true;
    scanstate.ss.ps.resultopsfixed = false;

    //   ExecInitScanTupleSlot(estate, &scanstate->ss, NULL, &TTSOpsMinimalTuple);
    // The scan tuple type is not known until the ancestor RecursiveUnion is
    // resolved (ExecWorkTableScan), so pass a NULL descriptor here.
    execTuples::exec_init_scan_tuple_slot::call(
        estate,
        &mut scanstate.ss,
        None,
        TupleSlotKind::MinimalTuple,
    )?;

    // initialize child expressions
    //   scanstate->ss.ps.qual =
    //       ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);
    let qual = plan.scan.plan.qual.as_deref();
    scanstate.ss.ps.qual = execExpr::exec_init_qual::call(qual, &mut scanstate.ss.ps, estate)?;

    // Do not yet initialize projection info, see ExecWorkTableScan() for details.

    Ok(scanstate)
}

/// `ExecReScanWorkTableScan(node)` — rescans the relation.
///
/// ```c
/// void
/// ExecReScanWorkTableScan(WorkTableScanState *node)
/// {
///     if (node->ss.ps.ps_ResultTupleSlot)
///         ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
///     ExecScanReScan(&node->ss);
///     /* No need (or way) to rescan if ExecWorkTableScan not called yet */
///     if (node->rustate)
///         tuplestore_rescan(node->rustate->working_table);
/// }
/// ```
pub fn ExecReScanWorkTableScan<'mcx>(
    node: &mut WorkTableScanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    //   if (node->ss.ps.ps_ResultTupleSlot)
    //       ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    if let Some(slot) = node.ss.ps.ps_ResultTupleSlot {
        execTuples::exec_clear_tuple::call(estate, slot)?;
    }

    //   ExecScanReScan(&node->ss);
    execScan::exec_scan_rescan_worktable::call(node, estate)?;

    // No need (or way) to rescan if ExecWorkTableScan not called yet.
    //   if (node->rustate)
    //       tuplestore_rescan(node->rustate->working_table);
    if node.rustate.is_some() {
        let mut working: PgBox<'mcx, Tuplestorestate<'mcx>> = shared_mut(node, estate)?
            .working_table
            .take()
            .ok_or_else(|| internal("WorkTableScan: working_table is NULL"))?;
        let res = tuplestore::tuplestore_rescan::call(&mut working);
        if let Ok(shared) = shared_mut(node, estate) {
            shared.working_table = Some(working);
        }
        res?;
    }

    Ok(())
}

// ===========================================================================
// publish_wtparam_slot — the deposit end, driven by ExecInitRecursiveUnion.
// ===========================================================================

/// `prmdata = &(estate->es_param_exec_vals[node->wtParam]);
/// Assert(prmdata->execPlan == NULL); prmdata->value = PointerGetDatum(rustate);
/// prmdata->isnull = false;` (nodeRecursiveunion.c `ExecInitRecursiveUnion`).
///
/// Publish the freshly-built `RecursiveUnion`'s shared state — its working-table
/// / intermediate-table tuplestores, its `recursing`/`intermediate_empty`
/// bookkeeping, and its result rowtype — into the
/// [`EStateData::es_recursive_shared`] side-table keyed by `wt_param`, so the
/// engine and its descendant `WorkTableScan` nodes reach the working table by
/// index (the mirror of [`resolve_rustate`], the recovery side). In C the slot
/// stores `PointerGetDatum(rustate)`, a live cross-node alias of the ancestor's
/// executor state; the owned model cannot alias the self-borrowing engine, so the
/// shared fields are hoisted into the side-table here.
///
/// The tuplestores / result descriptor are *moved out* of the just-built
/// `RecursiveUnionStateData` into the side-table; the `RecursiveUnion` executor
/// (`ExecRecursiveUnion`) then reads/swaps them through the side-table too.
pub fn publish_wtparam_slot<'mcx>(
    rustate: &mut ::nodes::noderecursiveunion::RecursiveUnionStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    wt_param: i32,
) -> PgResult<()> {
    let idx = usize::try_from(wt_param)
        .map_err(|_| internal("RecursiveUnion: invalid wtParam"))?;
    // Grow es_recursive_shared to cover idx (unclaimed slots default `None`).
    while estate.es_recursive_shared.len() <= idx {
        estate.es_recursive_shared.push(None);
    }
    // Assert(prmdata->execPlan == NULL): a recursive WITH query's work-table
    // param must not be claimed twice.
    if estate.es_recursive_shared[idx].is_some() {
        return Err(internal(
            "RecursiveUnion: work-table param slot already claimed",
        ));
    }

    // The RecursiveUnion's result rowtype, established by ExecInitRecursiveUnion's
    // ExecInitResultTypeTL before this call.
    let result_tupdesc = match rustate.ps.ps_ResultTupleDesc.as_deref() {
        Some(td) => {
            let mcx = estate.es_query_cxt;
            Some(alloc_in(mcx, td.clone_in(mcx)?)?)
        }
        None => None,
    };

    estate.es_recursive_shared[idx] = Some(RecursiveUnionSharedState {
        working_table: rustate.working_table.take(),
        intermediate_table: rustate.intermediate_table.take(),
        recursing: rustate.recursing,
        intermediate_empty: rustate.intermediate_empty,
        result_tupdesc,
    });
    Ok(())
}

/// `wt_param` of this node's `WorkTableScan` plan
/// (`((WorkTableScan *) node->ss.ps.plan)->wtParam`).
#[inline]
fn wtparam_of(node: &WorkTableScanStateData<'_>) -> i32 {
    node.ss
        .ps
        .plan
        .and_then(|p| p.as_worktablescan())
        .map(|w| w.wtParam)
        .expect("WorkTableScanState: plan is not a WorkTableScan")
}

/// This crate installs no seams of its own. Its outward calls go through the
/// per-owner seam crates (execUtils / execTuples / execScan / execExpr /
/// sort-storage tuplestore), all installed by their owners; `publish_wtparam_slot`
/// and the entry points are called directly by the executor dispatch crate.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
