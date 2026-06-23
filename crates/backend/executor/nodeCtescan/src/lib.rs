//! Port of `src/backend/executor/nodeCtescan.c` — routines to handle CteScan
//! nodes (scanning a `WITH` (CTE) query's materialized output).
//!
//! INTERFACE ROUTINES
//! - [`ExecCteScan`]        - the `ExecProcNode` callback (drives `ExecScan`)
//! - `CteScanNext`          - retrieve next tuple in scan order
//! - `CteScanRecheck`       - EvalPlanQual recheck (always true)
//! - [`ExecInitCteScan`]    - create and initialize a CteScan node
//! - [`ExecEndCteScan`]     - release any storage allocated
//! - [`ExecReScanCteScan`]  - rescan the node
//!
//! Multiple `CteScan` nodes can read from the same CTE query. The first one to
//! initialize becomes the "leader" and owns a shared `Tuplestorestate`; the
//! others allocate their own read pointers into the leader's store. The leader's
//! `eof_cte` flag short-circuits further calls of the CTE subplan once it is
//! exhausted.
//!
//! Following the nodeResult / nodeSeqscan owned-tree model, the C
//! `PlanState.state` back-pointer is replaced by threading `&mut EStateData`,
//! and the access method stages its result in the node's scan slot, reporting
//! availability as a `bool` (the C `slot`/`NULL`).
//!
//! The `leader` link is an aliased self-/cross-reference into the
//! executor-owned node graph; a live mutable alias into another owned node
//! cannot be held in safe Rust, so every operation crossing it — the
//! leader-identity test, the leader's `eof_cte` get/set, the shared tuplestore
//! reads and writes, the subplan dispatch through the leader's `cteplanstate`,
//! and the Param-slot leader handshake at init — goes through the `cte_*` seams
//! owned by execMain (`backend-executor-execMain-seams`), which holds the live
//! node graph + `es_param_exec_vals` / `es_subplanstates`; they panic until
//! execMain lands. The non-aliased leaf operations (`ExecScan`/`ExecScanReScan`,
//! projection, the `Exec*` init helpers, slot ops, qual compile) go through
//! their owners' seam crates. The node-machine control flow stays in-crate.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// `PgError` is a large struct carried by value in `PgResult`; the whole codebase
// returns it by value (matching the other ported executor-node crates).
#![allow(clippy::result_large_err)]

use execMain_seams as execMain;
use execScan_seams as execScan;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use execExpr_seams as execExpr;

use mcx::PgBox;
use types_error::PgResult;
use nodes::executor::{TupleSlotKind, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use nodes::nodectescan::{CteScan, CteScanState};
use nodes::{EStateData, ScanDirectionIsForward, SlotId};

/// Install this crate's seam implementations.
///
/// nodeCtescan owns no `<unit>-seams` crate: it reaches outward only through
/// per-owner seam crates (and the leader-aliased `cte_*` operations are owned
/// by execMain, not this node), so there is nothing to install here. Callers
/// that need this crate's `ExecInitCteScan`/`ExecCteScan`/... depend on it
/// directly without a cycle.
pub fn init_seams() {}

// ===========================================================================
// Node state machine (ported 1:1 from nodeCtescan.c).
// ===========================================================================

/// `CteScanNext` — the workhorse access method for `ExecCteScan`.
///
/// Fetches the next tuple from the shared tuplestore if one is available;
/// otherwise pulls another row from the CTE subplan, appends a copy to the
/// shared tuplestore, and returns it in the node's scan slot.
///
/// Returns `Ok(true)` when a tuple is staged in the node's
/// `ss.ss_ScanTupleSlot`; `Ok(false)` when the C returns `NULL` / clears the
/// slot.
pub fn CteScanNext<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // get state info from node
    //   estate = node->ss.ps.state;
    //   dir = estate->es_direction;
    //   forward = ScanDirectionIsForward(dir);
    //   tuplestorestate = node->leader->cte_table;
    //   tuplestore_select_read_pointer(tuplestorestate, node->readptr);
    //   slot = node->ss.ss_ScanTupleSlot;
    let forward = ScanDirectionIsForward(estate.es_direction);
    execMain::cte_tuplestore_select_read_pointer::call(node, estate)?;

    // If we are not at the end of the tuplestore, or are going backwards, try to
    // fetch a tuple from tuplestore.
    //   eof_tuplestore = tuplestore_ateof(tuplestorestate);
    let mut eof_tuplestore = execMain::cte_tuplestore_ateof::call(node, estate)?;

    if !forward && eof_tuplestore {
        if !execMain::cte_leader_eof_cte::call(node, estate)? {
            // When reversing direction at tuplestore EOF, the first gettupleslot
            // call will fetch the last-added tuple; but we want to return the one
            // before that, if possible. So do an extra fetch.
            if !execMain::cte_tuplestore_advance::call(node, forward, estate)? {
                // the tuplestore must be empty
                return Ok(false);
            }
        }
        eof_tuplestore = false;
    }

    // If we can fetch another tuple from the tuplestore, return it.
    //
    // Note: we have to use copy=true in the tuplestore_gettupleslot call, because
    // we are sharing the tuplestore with other nodes that might write into the
    // tuplestore before we get called again.
    if !eof_tuplestore {
        if execMain::cte_tuplestore_gettupleslot::call(node, forward, estate)? {
            return Ok(true);
        }
        if forward {
            eof_tuplestore = true;
        }
    }

    // If necessary, try to fetch another row from the CTE query.
    //
    // Note: the eof_cte state variable exists to short-circuit further calls of
    // the CTE plan.  It's not optional, unfortunately, because some plan node
    // types are not robust about being called again when they've already returned
    // NULL.
    if eof_tuplestore && !execMain::cte_leader_eof_cte::call(node, estate)? {
        // We can only get here with forward==true, so no need to worry about which
        // direction the subplan will go.
        //   cteslot = ExecProcNode(node->cteplanstate);
        //   if (TupIsNull(cteslot)) { node->leader->eof_cte = true; return NULL; }
        if !execMain::cte_exec_proc_node::call(node, estate)? {
            execMain::cte_set_leader_eof_cte::call(node, true, estate)?;
            return Ok(false);
        }

        // There are corner cases where the subplan could change which tuplestore
        // read pointer is active, so be sure to reselect ours before storing the
        // tuple we got.
        execMain::cte_tuplestore_select_read_pointer::call(node, estate)?;

        // Append a copy of the returned tuple to tuplestore.  NOTE: because our
        // read pointer is certainly in EOF state, its read position will move
        // forward over the added tuple.  This is what we want.  Also, any other
        // readers will *not* move past the new tuple, which is what they want.
        execMain::cte_tuplestore_puttupleslot::call(node, estate)?;

        // We MUST copy the CTE query's output tuple into our own slot. This is
        // because other CteScan nodes might advance the CTE query before we are
        // called again, and our output tuple must stay stable over that.
        //   return ExecCopySlot(slot, cteslot);
        execMain::cte_copy_tuple_to_scan_slot::call(node, estate)?;
        return Ok(true);
    }

    // Nothing left ...
    //   return ExecClearTuple(slot);
    clear_scan_tuple_slot(node, estate)?;
    Ok(false)
}

/// `CteScanRecheck` — access-method routine to recheck a tuple in EvalPlanQual.
/// Nothing to check for a CTE scan, so it always succeeds.
pub fn CteScanRecheck<'mcx>(
    _node: &mut CteScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // nothing to check
    Ok(true)
}

/// `ExecClearTuple(node->ss.ss_ScanTupleSlot)` — clear the node's scan slot.
fn clear_scan_tuple_slot<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(slot) = node.ss.ss_ScanTupleSlot {
        execTuples::exec_clear_tuple::call(estate, slot)?;
    }
    Ok(())
}

/// `ExecCteScan(pstate)` — the `PlanState.ExecProcNode` callback.
///
/// Scans the CTE sequentially and returns the next qualifying tuple by calling
/// `ExecScan` with the CTE access-method functions. Returns `Ok(Some(slot))`
/// when a projected tuple was produced, `Ok(None)` at end of scan.
///
/// ```c
/// static TupleTableSlot *
/// ExecCteScan(PlanState *pstate)
/// {
///     CteScanState *node = castNode(CteScanState, pstate);
///     return ExecScan(&node->ss,
///                     (ExecScanAccessMtd) CteScanNext,
///                     (ExecScanRecheckMtd) CteScanRecheck);
/// }
/// ```
pub fn ExecCteScan<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    execScan::exec_scan_cte::call(node, estate, CteScanNext, CteScanRecheck)
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitCteScan`]:
/// `castNode(CteScanState, pstate)` then run [`ExecCteScan`].
fn exec_cte_scan_node<'mcx>(
    pstate: &mut nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        nodes::PlanStateNode::CteScan(node) => node,
        other => panic!("castNode(CteScanState, pstate) failed: {other:?}"),
    };
    ExecCteScan(node, estate)
}

/// `ExecInitCteScan(node, estate, eflags)` — create and initialize a CteScan
/// node.
///
/// Takes the enclosing plan-tree [`Node`](nodes::nodes::Node); the
/// state's `ps.plan` back-link aliases the shared, read-only plan tree exactly
/// as C's `scanstate->ss.ps.plan = (Plan *) node`. Panics if the node is not a
/// `CteScan` (the C `castNode`).
pub fn ExecInitCteScan<'mcx>(
    node: &'mcx nodes::nodes::Node<'mcx>,
    mut eflags: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, CteScanState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // CteScan *node — the enclosing plan-tree node (the C `CteScan *` is the
    // same pointer, via struct embedding). Panics if it is not a `CteScan`.
    let plan: &'mcx CteScan<'mcx> = node.expect_ctescan();

    // check for unsupported flags
    //   Assert(!(eflags & EXEC_FLAG_MARK));
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);

    // For the moment we have to force the tuplestore to allow REWIND, because we
    // might be asked to rescan the CTE even though upper levels didn't tell us to
    // be prepared to do it efficiently.  Annoying, since this prevents truncation
    // of the tuplestore.  XXX FIXME
    eflags |= EXEC_FLAG_REWIND;

    // CteScan should not have any children:
    //   Assert(outerPlan(node) == NULL);
    //   Assert(innerPlan(node) == NULL);
    debug_assert!(plan.scan.plan.lefttree.is_none());
    debug_assert!(plan.scan.plan.righttree.is_none());

    // create new CteScanState for node: makeNode(CteScanState), then wire
    //   scanstate->ss.ps.plan = (Plan *) node;
    //   scanstate->ss.ps.state = estate;
    //   scanstate->ss.ps.ExecProcNode = ExecCteScan;
    //   scanstate->eflags = eflags;
    //   scanstate->cte_table = NULL;
    //   scanstate->eof_cte = false;
    //
    // The C leader-only `cte_table` / `eof_cte` are not node fields in the owned
    // model — they live in the per-CTE `EState.es_cte_shared[cteParam]` entry
    // (see `CteSharedState`), created/cleared by the leader-resolution and
    // tuplestore seams. The node only records its `cteParam` identity.
    let mut scanstate = mcx::alloc_in(mcx, CteScanState::new_in(mcx))?;
    scanstate.ss.ps.plan = Some(node);
    scanstate.ss.ps.ExecProcNode = Some(exec_cte_scan_node);
    scanstate.eflags = eflags;
    scanstate.cte_param = Some(plan.cteParam);

    // Find the already-initialized plan for the CTE query.
    //   scanstate->cteplanstate =
    //       (PlanState *) list_nth(estate->es_subplanstates, node->ctePlanId - 1);
    execMain::cte_link_plan_state::call(&mut scanstate, plan, estate)?;

    // The Param slot associated with the CTE query is used to hold a pointer to
    // the CteState of the first CteScan node that initializes for this CTE. This
    // node will be the one that holds the shared state for all the CTEs,
    // particularly the shared tuplestore.
    //   prmdata = &(estate->es_param_exec_vals[node->cteParam]);
    //   Assert(prmdata->execPlan == NULL);
    //   Assert(!prmdata->isnull);
    //   scanstate->leader = castNode(CteScanState, DatumGetPointer(prmdata->value));
    let is_leader = execMain::cte_resolve_leader::call(&mut scanstate, plan, estate)?;
    if is_leader {
        // I am the leader (resolve_cte_leader published prmdata->value =
        // PointerGetDatum(scanstate) and set scanstate->leader = scanstate).
        //   scanstate->cte_table = tuplestore_begin_heap(true, false, work_mem);
        //   tuplestore_set_eflags(scanstate->cte_table, scanstate->eflags);
        //   scanstate->readptr = 0;
        execMain::cte_tuplestore_begin_heap_leader::call(&mut scanstate, estate)?;
        scanstate.readptr = 0;
    } else {
        // Not the leader
        // Create my own read pointer, and ensure it is at start.
        //   scanstate->readptr =
        //       tuplestore_alloc_read_pointer(scanstate->leader->cte_table,
        //                                     scanstate->eflags);
        //   tuplestore_select_read_pointer(scanstate->leader->cte_table,
        //                                  scanstate->readptr);
        //   tuplestore_rescan(scanstate->leader->cte_table);
        execMain::cte_tuplestore_alloc_read_pointer_follower::call(&mut scanstate, estate)?;
    }

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &scanstate->ss.ps);
    execUtils::exec_assign_expr_context::call(estate, scanstate.ps_mut())?;

    // The scan tuple type (ie, the rowtype we expect to find in the work table)
    // is the same as the result rowtype of the CTE query.
    //   ExecInitScanTupleSlot(estate, &scanstate->ss,
    //                         ExecGetResultType(scanstate->cteplanstate),
    //                         &TTSOpsMinimalTuple);
    init_scan_tuple_slot_from_cte(&mut scanstate, estate)?;

    // Initialize result type and projection.
    //   ExecInitResultTypeTL(&scanstate->ss.ps);
    //   ExecAssignScanProjectionInfo(&scanstate->ss);
    execTuples::exec_init_result_type_tl::call(scanstate.ps_mut(), estate)?;
    execScan::exec_assign_scan_projection_info_cte::call(&mut scanstate, estate)?;

    // initialize child expressions
    //   scanstate->ss.ps.qual =
    //       ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);
    let qual = plan.scan.plan.qual.as_deref();
    scanstate.ss.ps.qual = execExpr::exec_init_qual::call(qual, scanstate.ps_mut(), estate)?;

    Ok(scanstate)
}

/// `ExecInitScanTupleSlot(estate, &scanstate->ss,
/// ExecGetResultType(scanstate->cteplanstate), &TTSOpsMinimalTuple)`.
///
/// The scan rowtype is the result rowtype of the CTE subplan, read through the
/// execTuples `ExecGetResultType` seam from the leader-resolved subplan link.
fn init_scan_tuple_slot_from_cte<'mcx>(
    scanstate: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    // ExecGetResultType(scanstate->cteplanstate), where the C `cteplanstate` is
    // `list_nth(es_subplanstates, ctePlanId - 1)`. The owned model reaches the
    // CTE subplan's plan-state by its recorded `cte_plan_id` identity rather
    // than an aliasing field (the subplan-state is owned by `es_subplanstates`).
    let idx = scanstate
        .cte_plan_id
        .and_then(|id| usize::try_from(id - 1).ok())
        .expect("ExecInitCteScan: cteplanstate linked before slot init");
    let cteplanstate = estate
        .es_subplanstates
        .get(idx)
        .and_then(|b| b.as_deref())
        .expect("ExecInitCteScan: es_subplanstates[ctePlanId-1] present");
    let tupdesc: types_tuple::heaptuple::TupleDesc<'mcx> =
        match execTuples::exec_get_result_type::call(cteplanstate.ps_head()) {
            Some(d) => Some(mcx::alloc_in(mcx, d.clone_in(mcx)?)?),
            None => None,
        };

    let ss = &mut scanstate.ss;
    execTuples::exec_init_scan_tuple_slot::call(estate, ss, tupdesc, TupleSlotKind::MinimalTuple)
}

/// `ExecEndCteScan(node)` — frees any storage allocated through C routines.
///
/// ```c
/// void
/// ExecEndCteScan(CteScanState *node)
/// {
///     /* If I am the leader, free the tuplestore. */
///     if (node->leader == node)
///     {
///         tuplestore_end(node->cte_table);
///         node->cte_table = NULL;
///     }
/// }
/// ```
///
/// `is_leader` answers `node->leader == node`. In the owned model the leader
/// identity was recorded on the node (`node.is_leader`) by `cte_resolve_leader`;
/// the dispatch site passes it here, exactly as the C compares the two pointers.
/// The shared `cte_table` lives in `EState.es_cte_shared[cteParam]`, so freeing
/// it (`tuplestore_end`) clears that side-entry, not a node field.
pub fn ExecEndCteScan<'mcx>(
    node: &mut CteScanState<'mcx>,
    is_leader: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // If I am the leader, free the tuplestore.
    if is_leader {
        execMain::cte_tuplestore_end::call(node, estate)?;
    }
    Ok(())
}

/// `ExecReScanCteScan(node)` — rescans the relation.
///
/// ```c
/// void
/// ExecReScanCteScan(CteScanState *node)
/// {
///     Tuplestorestate *tuplestorestate = node->leader->cte_table;
///     if (node->ss.ps.ps_ResultTupleSlot)
///         ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
///     ExecScanReScan(&node->ss);
///     if (node->leader->cteplanstate->chgParam != NULL)
///     {
///         tuplestore_clear(tuplestorestate);
///         node->leader->eof_cte = false;
///     }
///     else
///     {
///         tuplestore_select_read_pointer(tuplestorestate, node->readptr);
///         tuplestore_rescan(tuplestorestate);
///     }
/// }
/// ```
pub fn ExecReScanCteScan<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    //   if (node->ss.ps.ps_ResultTupleSlot)
    //       ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    if let Some(slot) = node.ss.ps.ps_ResultTupleSlot {
        execTuples::exec_clear_tuple::call(estate, slot)?;
    }

    //   ExecScanReScan(&node->ss);
    execScan::exec_scan_rescan_cte::call(node, estate)?;

    // Clear the tuplestore if a new scan of the underlying CTE is required.  This
    // implicitly resets all the tuplestore's read pointers.  Note that multiple
    // CTE nodes might redundantly clear the tuplestore; that's OK, and not unduly
    // expensive.  We'll stop taking this path as soon as somebody has attempted to
    // read something from the underlying CTE (thereby causing its chgParam to be
    // cleared).
    //   if (node->leader->cteplanstate->chgParam != NULL)
    if execMain::cte_leader_cteplanstate_chgparam_set::call(node, estate)? {
        execMain::cte_tuplestore_clear::call(node, estate)?;
        execMain::cte_set_leader_eof_cte::call(node, false, estate)?;
    } else {
        // Else, just rewind my own pointer.  Either the underlying CTE doesn't
        // need a rescan (and we can re-read what's in the tuplestore now), or
        // somebody else already took care of it.
        //   tuplestore_select_read_pointer(tuplestorestate, node->readptr);
        //   tuplestore_rescan(tuplestorestate);
        execMain::cte_tuplestore_select_read_pointer::call(node, estate)?;
        execMain::cte_tuplestore_rescan::call(node, estate)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests;
