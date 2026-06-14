//! Port of `src/backend/executor/nodeWorktablescan.c` — routines to handle
//! `WorkTableScan` nodes.
//!
//! A `WorkTableScan` node reads from the *working table* tuplestore owned by an
//! ancestor `RecursiveUnion` node (the engine behind `WITH RECURSIVE`). The link
//! to that ancestor's executor state (`rustate`) cannot be resolved during init
//! (there are corner cases where this node's init runs before the
//! `RecursiveUnion`'s), so it is found lazily on the first [`ExecWorkTableScan`]
//! call via the work-table `Param` slot reserved for it (`plan->wtParam` into
//! `EState.es_param_exec_vals`). The scan is intentionally forward-only.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitWorkTableScan`]  — create and initialize the node
//! - [`ExecWorkTableScan`]      — the `PlanState.ExecProcNode` callback
//! - [`ExecReScanWorkTableScan`]— rescan the relation
//!
//! Following the established owned-tree model, the C `PlanState.state`
//! back-pointer is replaced by threading `&mut EStateData` explicitly, and the
//! `ExecProcNode` "return slot" convention becomes a `PgResult<bool>`: `true`
//! when a tuple is available in the node's scan/result slot, `false` once the
//! scan is exhausted. The work-table tuplestore (`tuplestore_gettupleslot` /
//! `tuplestore_rescan`), the resolution of the ancestor `RecursiveUnion`'s state,
//! the execUtils/execScan/execTuples init helpers, and the `execScan.c` driver's
//! leaf operations / EvalPlanQual machinery all live in subsystems below this
//! node and are reached through this crate's seam crate (panicking until those
//! owners land).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use backend_executor_nodeWorktablescan_seams as seam;
use types_error::PgResult;
use types_nodes::execnodes::EStateData;
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, TTS_FLAG_EMPTY};
use types_nodes::nodeworktablescan::{WorkTableScan, WorkTableScanStateData};

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
    // (the Assert is compiled out in release C builds.)

    // Get the next tuple from tuplestore. Return NULL (here: leave slot empty,
    // signal `false`) if no more tuples.
    //   tuplestorestate = node->rustate->working_table;
    //   slot = node->ss.ss_ScanTupleSlot;
    //   (void) tuplestore_gettupleslot(tuplestorestate, true, false, slot);
    //   return slot;
    seam::tuplestore_gettupleslot::call(node, estate)
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
// `execScan.c` driver (`ExecScan` -> `ExecScanExtended` -> `ExecScanFetch`),
// linked into `nodeWorktablescan.o` in C; reproduced here as private functions.
// ===========================================================================

type AccessMtd =
    for<'mcx> fn(&mut WorkTableScanStateData<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;
type RecheckMtd =
    for<'mcx> fn(&mut WorkTableScanStateData<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

/// `TupIsNull(slot)` — true if the node's scan slot is absent or marked empty
/// (`TTS_FLAG_EMPTY`).
#[inline]
fn scan_tuple_is_null(node: &WorkTableScanStateData, estate: &EStateData) -> bool {
    match node.ss.ss_ScanTupleSlot.map(|id| estate.slot(id)) {
        None => true,
        Some(slot) => (slot.tts_flags & TTS_FLAG_EMPTY) != 0,
    }
}

/// `ExecScanFetch` — check interrupts and fetch the next potential tuple.
fn ExecScanFetch<'mcx>(
    node: &mut WorkTableScanStateData<'mcx>,
    epq_active: bool,
    access_mtd: AccessMtd,
    recheck_mtd: RecheckMtd,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    seam::check_for_interrupts::call()?;

    if epq_active {
        // We are inside an EvalPlanQual recheck.
        let scanrelid: u32 = seam::scan_scanrelid::call(node)?;

        if scanrelid == 0 {
            // ForeignScan/CustomScan that pushed a join to the remote side.
            if seam::epq_param_is_member_of_ext_param::call(node)? {
                if !recheck_mtd(node, estate)? {
                    seam::exec_clear_scan_tuple::call(node, estate)?;
                }
                return Ok(!scan_tuple_is_null(node, estate));
            }
        } else if seam::epq_relsubs_done::call(node, scanrelid - 1)? {
            // Return empty slot, as either there is no EPQ tuple for this rel or
            // we already returned it.
            seam::exec_clear_scan_tuple::call(node, estate)?;
            return Ok(false);
        } else if seam::epq_relsubs_slot_present::call(node, scanrelid - 1)? {
            // Return replacement tuple provided by the EPQ caller.
            seam::epq_load_relsubs_slot::call(node, scanrelid - 1)?;

            // Mark to remember that we shouldn't return it again.
            seam::epq_set_relsubs_done::call(node, scanrelid - 1, true)?;

            // Return empty slot if we haven't got a test tuple.
            if scan_tuple_is_null(node, estate) {
                return Ok(false);
            }

            // Check if it meets the access-method conditions.
            if !recheck_mtd(node, estate)? {
                seam::exec_clear_scan_tuple::call(node, estate)?;
                return Ok(false);
            }
            return Ok(true);
        } else if seam::epq_relsubs_rowmark_present::call(node, scanrelid - 1)? {
            // Fetch and return replacement tuple using a non-locking rowmark.
            seam::epq_set_relsubs_done::call(node, scanrelid - 1, true)?;

            if !seam::eval_plan_qual_fetch_row_mark::call(node, scanrelid)? {
                return Ok(false);
            }

            // Return empty slot if we haven't got a test tuple.
            if scan_tuple_is_null(node, estate) {
                return Ok(false);
            }

            // Check if it meets the access-method conditions.
            if !recheck_mtd(node, estate)? {
                seam::exec_clear_scan_tuple::call(node, estate)?;
                return Ok(false);
            }
            return Ok(true);
        }
    }

    // Run the node-type-specific access method function to get the next tuple.
    access_mtd(node, estate)
}

/// `ExecScanExtended` — scan using the specified access method; optionally check
/// the tuple against `qual` and apply `proj_info`.
#[allow(clippy::too_many_arguments)]
fn ExecScanExtended<'mcx>(
    node: &mut WorkTableScanStateData<'mcx>,
    access_mtd: AccessMtd,
    recheck_mtd: RecheckMtd,
    epq_active: bool,
    has_qual: bool,
    has_proj_info: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // interrupt checks are in ExecScanFetch

    // If we have neither a qual to check nor a projection to do, just skip all
    // the overhead and return the raw scan tuple.
    if !has_qual && !has_proj_info {
        seam::reset_per_tuple_expr_context::call(node, estate)?;
        return ExecScanFetch(node, epq_active, access_mtd, recheck_mtd, estate);
    }

    // Reset per-tuple memory context to free any expression evaluation storage
    // allocated in the previous tuple cycle.
    seam::reset_per_tuple_expr_context::call(node, estate)?;

    // Get a tuple from the access method. Loop until we obtain a tuple that
    // passes the qualification.
    loop {
        let have_tuple = ExecScanFetch(node, epq_active, access_mtd, recheck_mtd, estate)?;

        // If the slot returned by the accessMtd contains NULL, then it means
        // there is nothing more to scan so we just return an empty slot, being
        // careful to use the projection result slot so it has correct tupleDesc.
        if !have_tuple {
            if has_proj_info {
                seam::exec_clear_proj_result_slot::call(node, estate)?;
                return Ok(false);
            } else {
                return Ok(false);
            }
        }

        // Place the current tuple into the expr context.
        seam::set_econtext_scantuple_to_scan_slot::call(node, estate)?;

        // Check that the current tuple satisfies the qual-clause.
        if !has_qual || seam::exec_qual::call(node, estate)? {
            // Found a satisfactory scan tuple.
            if has_proj_info {
                // Form a projection tuple, store it in the result tuple slot and
                // return it.
                return seam::exec_project::call(node, estate);
            } else {
                // Here, we aren't projecting, so just return scan tuple.
                return Ok(true);
            }
        } else {
            InstrCountFiltered1(node, 1);
        }

        // Tuple fails qual, so free per-tuple memory and try again.
        seam::reset_per_tuple_expr_context::call(node, estate)?;
    }
}

/// `ExecScan` — the non-inlined `execScan.c` driver. Equivalent to
/// `ExecScanExtended(node, access, recheck, node->ps.state->es_epq_active,
/// node->ps.qual, node->ps.ps_ProjInfo)`.
fn ExecScan<'mcx>(
    node: &mut WorkTableScanStateData<'mcx>,
    access_mtd: AccessMtd,
    recheck_mtd: RecheckMtd,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let epq_active = seam::es_epq_active_present::call(node)?;
    let has_qual = node.ss.ps.qual.is_some();
    let has_proj_info = node.ss.ps.ps_ProjInfo.is_some();
    ExecScanExtended(
        node,
        access_mtd,
        recheck_mtd,
        epq_active,
        has_qual,
        has_proj_info,
        estate,
    )
}

/// `InstrCountFiltered1(node, delta)` — bump `node->ps.instrument->nfiltered1`
/// when instrumentation is enabled.
#[inline]
fn InstrCountFiltered1(node: &mut WorkTableScanStateData, delta: u64) {
    if let Some(instr) = node.ss.ps.instrument.as_mut() {
        instr.nfiltered1 += delta as f64;
    }
}

// ===========================================================================
//                          Public node entry points
// ===========================================================================

/// `ExecWorkTableScan(pstate)` — the `PlanState.ExecProcNode` callback.
///
/// Scans the worktable sequentially and returns whether the next qualifying
/// tuple is available (stored in the node's result/scan slot) by calling
/// [`ExecScan`] with the work-table access-method functions. On the first call
/// it resolves the ancestor `RecursiveUnion`'s state and finishes scan-type and
/// projection setup (deferred from init).
pub fn ExecWorkTableScan<'mcx>(
    node: &mut WorkTableScanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // On the first call, find the ancestor RecursiveUnion's state via the Param
    // slot reserved for it. (We can't do this during node init because there are
    // corner cases where we'll get the init call before the RecursiveUnion does.)
    if node.rustate.is_none() {
        //   param = &(estate->es_param_exec_vals[plan->wtParam]);
        //   Assert(param->execPlan == NULL); Assert(!param->isnull);
        //   node->rustate =
        //       castNode(RecursiveUnionState, DatumGetPointer(param->value));
        //   Assert(node->rustate);
        seam::resolve_rustate::call(node, estate)?;

        // The scan tuple type (ie, the rowtype we expect to find in the work
        // table) is the same as the result rowtype of the ancestor
        // RecursiveUnion node. Note this depends on the assumption that
        // RecursiveUnion doesn't allow projection.
        //   ExecAssignScanType(&node->ss, ExecGetResultType(&node->rustate->ps));
        seam::exec_assign_scan_type_from_rustate::call(node, estate)?;

        // Now we can initialize the projection info. This must be completed
        // before we can call ExecScan().
        //   ExecAssignScanProjectionInfo(&node->ss);
        seam::exec_assign_scan_projection_info::call(node, estate)?;
    }

    //   return ExecScan(&node->ss,
    //                   (ExecScanAccessMtd) WorkTableScanNext,
    //                   (ExecScanRecheckMtd) WorkTableScanRecheck);
    ExecScan(node, WorkTableScanNext, WorkTableScanRecheck, estate)
}

/// `ExecInitWorkTableScan(node, estate, eflags)` — create and initialize a
/// `WorkTableScan` node.
///
/// In C this allocates the node via `makeNode` and returns the pointer; here we
/// build the owned [`WorkTableScanStateData`] (the C `palloc0` initial state)
/// and return it by value. Projection info is *not* initialized here — see
/// [`ExecWorkTableScan`] for details.
pub fn ExecInitWorkTableScan<'mcx>(
    node: &WorkTableScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<WorkTableScanStateData<'mcx>> {
    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    // WorkTableScan should not have any children.
    //   Assert(outerPlan(node) == NULL); Assert(innerPlan(node) == NULL);
    debug_assert!(node.scan.plan.lefttree.is_none());
    debug_assert!(node.scan.plan.righttree.is_none());

    // create new WorkTableScanState for node: makeNode(WorkTableScanState)
    let mut scanstate = WorkTableScanStateData::default();

    // scanstate->ss.ps.plan = (Plan *) node; scanstate->ss.ps.state = estate;
    // scanstate->ss.ps.ExecProcNode = ExecWorkTableScan;
    // scanstate->rustate = NULL; /* we'll set this later */
    //
    // The plan-node and EState links and the ExecProcNode install are wired by
    // the executor node factory (it owns the EState/plan-tree references and the
    // `ExecProcNodeMtd` dispatch slot). It also leaves `rustate` unset (NULL).
    seam::init_plan_state_links::call(&mut scanstate, node, estate)?;

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &scanstate->ss.ps);
    seam::exec_assign_expr_context::call(&mut scanstate, estate)?;

    // tuple table initialization
    //   ExecInitResultTypeTL(&scanstate->ss.ps);
    seam::exec_init_result_type_tl::call(&mut scanstate, estate)?;

    // signal that return type is not yet known
    //   scanstate->ss.ps.resultopsset = true;
    //   scanstate->ss.ps.resultopsfixed = false;
    scanstate.ss.ps.resultopsset = true;
    scanstate.ss.ps.resultopsfixed = false;

    //   ExecInitScanTupleSlot(estate, &scanstate->ss, NULL, &TTSOpsMinimalTuple);
    seam::exec_init_scan_tuple_slot::call(&mut scanstate, estate)?;

    // initialize child expressions
    //   scanstate->ss.ps.qual =
    //       ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);
    seam::exec_init_qual::call(&mut scanstate, node, estate)?;

    // Do not yet initialize projection info, see ExecWorkTableScan() for details.

    Ok(scanstate)
}

/// `ExecReScanWorkTableScan(node)` — rescans the relation.
pub fn ExecReScanWorkTableScan<'mcx>(
    node: &mut WorkTableScanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    //   if (node->ss.ps.ps_ResultTupleSlot)
    //       ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    if node.ss.ps.ps_ResultTupleSlot.is_some() {
        seam::exec_clear_result_tuple_slot::call(node, estate)?;
    }

    //   ExecScanReScan(&node->ss);
    seam::exec_scan_rescan::call(node, estate)?;

    // No need (or way) to rescan if ExecWorkTableScan not called yet.
    //   if (node->rustate)
    //       tuplestore_rescan(node->rustate->working_table);
    if node.rustate.is_some() {
        seam::tuplestore_rescan::call(node, estate)?;
    }

    Ok(())
}

/// Install every seam this crate owns. Wired into `seams-init::init_all()`.
///
/// This node calls outward through the seams in
/// `backend-executor-nodeWorktablescan-seams` (the execUtils/execScan/execTuples
/// init helpers, the work-table tuplestore, the resolution of the ancestor
/// `RecursiveUnion`'s state, and the `execScan.c` driver's leaf operations /
/// EvalPlanQual machinery); those slots are installed by their owning subsystems
/// when they land. The node itself owns no inward-facing seam, so there is
/// nothing to `set()` here yet.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
