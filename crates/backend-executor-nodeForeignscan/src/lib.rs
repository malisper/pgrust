//! Port of `src/backend/executor/nodeForeignscan.c` — routines to support
//! scans of foreign tables.
//!
//! INTERFACE ROUTINES
//! - [`ExecForeignScan`]        - scans a foreign table
//! - [`ExecInitForeignScan`]    - creates and initializes state info
//! - [`ExecReScanForeignScan`]  - rescans the foreign relation
//! - [`ExecEndForeignScan`]     - releases any resources allocated
//!
//! plus the parallel-scan entry points
//! ([`ExecForeignScanEstimate`]/`InitializeDSM`/`ReInitializeDSM`/
//! `InitializeWorker`), [`ExecShutdownForeignScan`], and the three
//! async-execution entry points
//! ([`ExecAsyncForeignScanRequest`]/`ConfigureWait`/`Notify`).
//!
//! The node state machine is held as an owned [`ForeignScanState`] mutated
//! through `&mut` borrows; the C `PlanState.state` back-pointer is replaced by
//! threading `&mut EStateData` explicitly. `ExecForeignScan` returns
//! `Ok(Some(slot))` when a tuple is available (the C `return slot`) and
//! `Ok(None)` for the C `NULL` return.
//!
//! The genuine external boundary is the **FDW provider**: the `FdwRoutine`
//! callbacks an FDW extension installs. The node reads the owned
//! `node.fdwroutine` presence flags for the `if (fdwroutine->X)` /
//! `Assert(fdwroutine->X != NULL)` checks exactly as the C does, and reaches
//! the *invocation* through `backend-foreign-foreign-seams`. The `execScan.c`
//! driver (`ExecScan`/`ExecScanExtended`/`ExecScanFetch`) the C compiler links
//! into `nodeForeignscan.o` is reproduced here as private functions; its leaf
//! operations (interrupts, qual/projection, EvalPlanQual) go through the
//! owning crates' seams.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execMain_seams as execMain;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execScan_seams as execScan;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_foreign_foreign_seams as foreign;
use backend_nodes_core_seams as bitmapset;
use backend_storage_ipc_shm_toc_seams as shm_toc;
use backend_tcop_postgres_seams as tcop_postgres;

use mcx::{alloc_in, PgBox};
use types_error::{PgError, PgResult};
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use types_nodes::nodes::{CmdType, Node};
use types_nodes::{
    AsyncRequest, EStateData, ForeignScan, ForeignScanState, ParallelContext,
    ParallelWorkerContext, PlanStateNode, SlotId, TupleSlotKind,
};

/// Access-method "function pointer" (C `ExecScanAccessMtd`): returns the slot
/// id of the next fetched tuple, or `None` for the C `NULL`. Within this crate
/// it is always [`ForeignNext`].
type AccessMtd<'mcx> =
    fn(&mut ForeignScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<Option<SlotId>>;
/// Recheck-method "function pointer" (C `ExecScanRecheckMtd`): rechecks the
/// tuple in the node's scan slot. Within this crate it is always
/// [`ForeignRecheck`].
type RecheckMtd<'mcx> =
    fn(&mut ForeignScanState<'mcx>, &mut EStateData<'mcx>, SlotId) -> PgResult<bool>;

/// Install this crate's implementations of the parallel-scan inward seams (the
/// ones `execParallel` calls through `backend-executor-nodeForeignscan-seams`).
///
/// `execParallel` holds the foreign-scan node as an opaque
/// `PlanStateHandle`/`ParallelContextHandle` (a not-yet-bridged pointer into
/// executor/DSM-owned state). Until the DSM owner can hand this crate the
/// owned `ForeignScanState`, the parallel path is unreachable, so these
/// installed implementations panic loudly (mirror-PG-and-panic). The owned
/// parallel entry points ([`ExecForeignScanEstimate`] et al.) carry the real C
/// logic and are callable directly.
pub fn init_seams() {
    use backend_executor_nodeForeignscan_seams as pq;
    pq::exec_foreignscan_estimate::set(|_node, _pcxt| {
        panic!(
            "ExecForeignScanEstimate via parallel DSM is unreachable until the \
             DSM owner can pass the owned ForeignScanState (the opaque \
             PlanStateHandle cannot be resolved here yet)"
        )
    });
    pq::exec_foreignscan_initialize_dsm::set(|_node, _pcxt| {
        panic!("ExecForeignScanInitializeDSM via parallel DSM is unreachable until the DSM owner lands")
    });
    pq::exec_foreignscan_reinitialize_dsm::set(|_node, _pcxt| {
        panic!("ExecForeignScanReInitializeDSM via parallel DSM is unreachable until the DSM owner lands")
    });
    pq::exec_foreignscan_initialize_worker::set(|_node, _pwcxt| {
        panic!("ExecForeignScanInitializeWorker via parallel DSM is unreachable until the DSM owner lands")
    });
}

// ===========================================================================
//                              Scan Support
// ===========================================================================

/// `ForeignNext(node)` — the workhorse for `ExecForeignScan`.
///
/// Calls the FDW's `Iterate*` callback in the short-lived per-tuple context,
/// then stamps `tableoid` into the result slot if the plan asks for system
/// columns. Returns the scan slot id when a tuple is available, `None`
/// otherwise (the C `NULL`).
fn ForeignNext<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
    let (operation, fs_system_col) = foreignscan_plan_fields(node);

    // Call the Iterate function in short-lived context:
    //   oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    //
    // The owned tree has no ambient current context; the FDW callbacks here
    // allocate in the node's per-tuple memory by construction (the seam owner
    // enters it). The switch/restore pair is therefore folded into the seamed
    // callback boundary.
    let have_tuple = if operation != CmdType::CMD_SELECT {
        // Direct modifications cannot be re-evaluated, so shouldn't get here
        // during EvalPlanQual processing.
        //   Assert(node->ss.ps.state->es_epq_active == NULL);
        debug_assert!(estate.es_epq_active.is_none());

        // slot = node->fdwroutine->IterateDirectModify(node);
        foreign::iterate_direct_modify::call(node, estate)?
    } else {
        // slot = node->fdwroutine->IterateForeignScan(node);
        foreign::iterate_foreign_scan::call(node, estate)?
    };

    // MemoryContextSwitchTo(oldcontext);  (folded into the seam, see above)

    // The FDW stored its result tuple into the node's scan slot; the returned
    // bool is the C `!TupIsNull(slot)`.
    let slot = node.ss.ss_ScanTupleSlot;

    // Insert valid value into tableoid, the only actually-useful system column.
    //   if (plan->fsSystemCol && !TupIsNull(slot))
    //       slot->tts_tableOid = RelationGetRelid(node->ss.ss_currentRelation);
    if fs_system_col && have_tuple {
        // The owned slot carries no payload yet (the slot model lands with the
        // slot-owning units); stamping tts_tableOid is the FDW/relcache-owned
        // operation, reached through the seam so it stays faithful when the
        // slot payload arrives.
        foreign_stamp_tableoid(node, estate)?;
    }

    if have_tuple {
        Ok(slot)
    } else {
        Ok(None)
    }
}

/// `ForeignRecheck(node, slot)` — access-method routine to recheck a tuple in
/// EvalPlanQual.
fn ForeignRecheck<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
) -> PgResult<bool> {
    // FdwRoutine *fdwroutine = node->fdwroutine;
    // ExprContext *econtext = node->ss.ps.ps_ExprContext;
    //
    // econtext->ecxt_scantuple = slot;
    if let Some(ecxt) = node.ss.ps.ps_ExprContext {
        if let Some(Some(ec)) = estate.es_exprcontexts.get_mut(ecxt.0 as usize) {
            ec.ecxt_scantuple = Some(slot);
        }
    }

    // ResetExprContext(econtext);
    reset_per_tuple_expr_context(node, estate);

    // If an outer join is pushed down, RecheckForeignScan may need to store a
    // different tuple in the slot, because a different set of columns may go to
    // NULL upon recheck. Otherwise, it shouldn't need to change the slot
    // contents, just return true or false. For simple cases, setting
    // fdw_recheck_quals may be easier than providing this callback.
    //
    //   if (fdwroutine->RecheckForeignScan &&
    //       !fdwroutine->RecheckForeignScan(node, slot))
    //       return false;
    if has_recheck_foreign_scan(node) && !foreign::recheck_foreign_scan::call(node, estate)? {
        return Ok(false);
    }

    // return ExecQual(node->fdw_recheck_quals, econtext);
    match (node.fdw_recheck_quals.as_deref_mut(), node.ss.ps.ps_ExprContext) {
        // ExecQual(NULL, ...) is treated as always-true by the caller of an
        // ExprState, but ExecQual itself returns true for a NULL state.
        (None, _) => Ok(true),
        (Some(state), Some(econtext)) => execExpr::exec_qual::call(state, econtext, estate),
        (Some(_), None) => Ok(true),
    }
}

// ===========================================================================
//                          Public node entry points
// ===========================================================================

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitForeignScan`]:
/// `castNode(ForeignScanState, pstate)` then run [`ExecForeignScan`].
fn exec_foreign_scan_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::ForeignScan(node) => node,
        other => panic!("castNode(ForeignScanState, pstate) failed: {other:?}"),
    };
    ExecForeignScan(node, estate)
}

/// `ExecForeignScan(pstate)` — fetch the next tuple from the FDW, check local
/// quals, and return it. Calls [`ExecScan`] with the foreign access/recheck
/// methods.
pub fn ExecForeignScan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
    // EState *estate = node->ss.ps.state;
    let (operation, _) = foreignscan_plan_fields(node);

    // Ignore direct modifications when EvalPlanQual is active --- they are
    // irrelevant for EvalPlanQual rechecking.
    //   if (estate->es_epq_active != NULL && plan->operation != CMD_SELECT)
    //       return NULL;
    if estate.es_epq_active.is_some() && operation != CmdType::CMD_SELECT {
        return Ok(None);
    }

    // return ExecScan(&node->ss, ForeignNext, ForeignRecheck);
    ExecScan(node, ForeignNext, ForeignRecheck, estate)
}

/// `ExecInitForeignScan(node, estate, eflags)` — create and initialize a
/// `ForeignScanState`.
///
/// Takes the enclosing plan-tree [`Node`] (the C `ForeignScan *`): the state's
/// plan back-link aliases the shared, read-only plan tree exactly as C's
/// `scanstate->ss.ps.plan = (Plan *) node`. Panics if the node is not a
/// `ForeignScan` (the C `castNode`).
pub fn ExecInitForeignScan<'mcx>(
    node: &'mcx Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, ForeignScanState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let plan: &'mcx ForeignScan<'mcx> = match node {
        Node::ForeignScan(f) => f,
        other => panic!("castNode(ForeignScan, node) failed: {other:?}"),
    };

    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    // create state structure
    //   scanstate = makeNode(ForeignScanState);
    //   scanstate->ss.ps.plan = (Plan *) node;
    //   scanstate->ss.ps.state = estate;        (threaded estate)
    //   scanstate->ss.ps.ExecProcNode = ExecForeignScan;
    let mut scanstate = alloc_in(mcx, ForeignScanState::default())?;
    scanstate.ss.ps.plan = Some(node);
    scanstate.ss.ps.ExecProcNode = Some(exec_foreign_scan_node);

    // Index scanrelid = node->scan.scanrelid;
    let scanrelid = plan.scan.scanrelid;

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &scanstate->ss.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut scanstate.ss.ps)?;

    // open the scan relation, if any; also acquire function pointers from the
    // FDW's handler.
    let fdwroutine = if scanrelid > 0 {
        // currentRelation = ExecOpenScanRelation(estate, scanrelid, eflags);
        // scanstate->ss.ss_currentRelation = currentRelation;
        let current_relation = execUtils::exec_open_scan_relation::call(estate, scanrelid, eflags)?;
        scanstate.ss.ss_currentRelation = Some(current_relation);
        // fdwroutine = GetFdwRoutineForRelation(currentRelation, true);
        foreign::get_fdw_routine_for_relation::call(&mut scanstate, estate)?
    } else {
        // We can't use the relcache, so get fdwroutine the hard way.
        //   fdwroutine = GetFdwRoutineByServerId(node->fs_server);
        foreign::get_fdw_routine_by_server_id::call(plan.fs_server)?
    };
    let current_relation_is_some = scanstate.ss.ss_currentRelation.is_some();

    // Determine the scan tuple type. If the FDW provided a targetlist
    // describing the scan tuples, use that; else use base relation's rowtype.
    //   if (node->fdw_scan_tlist != NIL || currentRelation == NULL)
    let fdw_scan_tlist_is_nil = plan
        .fdw_scan_tlist
        .as_ref()
        .map(|tl| tl.is_empty())
        .unwrap_or(true);
    let tlistvarno: i32 = if !fdw_scan_tlist_is_nil || !current_relation_is_some {
        // scan_tupdesc = ExecTypeFromTL(node->fdw_scan_tlist);
        let fdw_scan_tlist: &[types_nodes::primnodes::TargetEntry<'mcx>] =
            plan.fdw_scan_tlist.as_deref().unwrap_or(&[]);
        let scan_tupdesc = execTuples::exec_type_from_tl::call(mcx, fdw_scan_tlist)?;
        // ExecInitScanTupleSlot(estate, &scanstate->ss, scan_tupdesc, &TTSOpsHeapTuple);
        execTuples::exec_init_scan_tuple_slot::call(
            estate,
            &mut scanstate.ss,
            scan_tupdesc,
            TupleSlotKind::HeapTuple,
        )?;
        // Node's targetlist will contain Vars with varno = INDEX_VAR
        INDEX_VAR
    } else {
        // don't trust FDWs to return tuples fulfilling NOT NULL constraints:
        //   scan_tupdesc = CreateTupleDescCopy(RelationGetDescr(currentRelation));
        //
        // `ss_currentRelation` is the real owned relcache entry here (this
        // branch is taken only when `current_relation_is_some`); its
        // `rd_att_clone_in` is `CreateTupleDescCopy(RelationGetDescr(rel))`.
        let scan_tupdesc = {
            let rel = scanstate
                .ss
                .ss_currentRelation
                .as_ref()
                .expect("currentRelation is Some in this branch");
            Some(rel.rd_att_clone_in(mcx)?)
        };
        // ExecInitScanTupleSlot(estate, &scanstate->ss, scan_tupdesc, &TTSOpsHeapTuple);
        execTuples::exec_init_scan_tuple_slot::call(
            estate,
            &mut scanstate.ss,
            scan_tupdesc,
            TupleSlotKind::HeapTuple,
        )?;
        // Node's targetlist will contain Vars with varno = scanrelid
        scanrelid as i32
    };

    // Don't know what an FDW might return.
    //   scanstate->ss.ps.scanopsfixed = false;
    scanstate.ss.ps.scanopsfixed = false;
    //   scanstate->ss.ps.scanopsset = true;
    scanstate.ss.ps.scanopsset = true;

    // Initialize result slot, type and projection.
    //   ExecInitResultTypeTL(&scanstate->ss.ps);
    execTuples::exec_init_result_type_tl::call(&mut scanstate.ss.ps, estate)?;
    //   ExecAssignScanProjectionInfoWithVarno(&scanstate->ss, tlistvarno);
    execUtils::exec_assign_scan_projection_info_with_varno::call(
        &mut scanstate.ss,
        estate,
        tlistvarno,
    )?;

    // initialize child expressions
    //   scanstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, scanstate);
    {
        let qual = plan.scan.plan.qual.as_deref();
        scanstate.ss.ps.qual = execExpr::exec_init_qual::call(qual, &mut scanstate.ss.ps, estate)?;
    }
    //   scanstate->fdw_recheck_quals = ExecInitQual(node->fdw_recheck_quals, scanstate);
    {
        let recheck = plan.fdw_recheck_quals.as_deref();
        scanstate.fdw_recheck_quals =
            execExpr::exec_init_qual::call(recheck, &mut scanstate.ss.ps, estate)?;
    }

    // Determine whether to scan the foreign relation asynchronously or not.
    //   scanstate->ss.ps.async_capable = (((Plan *) node)->async_capable &&
    //                                     estate->es_epq_active == NULL);
    scanstate.ss.ps.async_capable =
        plan.scan.plan.async_capable && estate.es_epq_active.is_none();

    // Initialize FDW-related state.
    //   scanstate->fdwroutine = fdwroutine;
    //   scanstate->fdw_state = NULL;
    scanstate.fdwroutine = Some(fdwroutine);
    scanstate.fdw_state = Default::default();

    // For the FDW's convenience, look up the modification target relation's
    // ResultRelInfo. The ModifyTable node should have initialized it for us.
    // Don't look it up when EvalPlanQual is active.
    //   if (node->resultRelation > 0 && estate->es_epq_active == NULL)
    if plan.resultRelation > 0 && estate.es_epq_active.is_none() {
        // if (estate->es_result_relations == NULL ||
        //     estate->es_result_relations[node->resultRelation - 1] == NULL)
        //     elog(ERROR, "result relation not initialized");
        // scanstate->resultRelInfo = estate->es_result_relations[node->resultRelation - 1];
        let idx = (plan.resultRelation - 1) as usize;
        match estate.es_result_relations.get(idx).copied().flatten() {
            Some(rri) => scanstate.resultRelInfo = Some(rri),
            None => return Err(elog_error_result_relation_not_initialized()),
        }
    }

    // Initialize any outer plan.
    //   if (outerPlan(node))
    //       outerPlanState(scanstate) = ExecInitNode(outerPlan(node), estate, eflags);
    if let Some(outer_plan) = plan.scan.plan.lefttree.as_deref() {
        scanstate.ss.ps.lefttree =
            execProcnode::exec_init_node::call(mcx, Some(outer_plan), estate, eflags)?;
    }

    // Tell the FDW to initialize the scan.
    //   if (node->operation != CMD_SELECT)
    if plan.operation != CmdType::CMD_SELECT {
        // Direct modifications cannot be re-evaluated by EvalPlanQual, so don't
        // bother preparing the FDW.
        //   if (estate->es_epq_active == NULL)
        //       fdwroutine->BeginDirectModify(scanstate, eflags);
        if estate.es_epq_active.is_none() {
            foreign::begin_direct_modify::call(&mut scanstate, estate, eflags)?;
        }
    } else {
        // fdwroutine->BeginForeignScan(scanstate, eflags);
        foreign::begin_foreign_scan::call(&mut scanstate, estate, eflags)?;
    }

    // return scanstate;
    Ok(scanstate)
}

/// `ExecEndForeignScan(node)` — free any storage allocated through C routines.
pub fn ExecEndForeignScan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
    // EState *estate = node->ss.ps.state;
    let (operation, _) = foreignscan_plan_fields(node);

    // Let the FDW shut down.
    //   if (plan->operation != CMD_SELECT)
    if operation != CmdType::CMD_SELECT {
        // if (estate->es_epq_active == NULL)
        //     node->fdwroutine->EndDirectModify(node);
        if estate.es_epq_active.is_none() {
            foreign::end_direct_modify::call(node, estate)?;
        }
    } else {
        // node->fdwroutine->EndForeignScan(node);
        foreign::end_foreign_scan::call(node, estate)?;
    }

    // Shut down any outer plan.
    //   if (outerPlanState(node))
    //       ExecEndNode(outerPlanState(node));
    if node.ss.ps.lefttree.is_some() {
        let outer = node
            .ss
            .ps
            .lefttree
            .as_deref_mut()
            .expect("checked Some above");
        execProcnode::exec_end_node::call(outer, estate)?;
    }

    Ok(())
}

/// `ExecReScanForeignScan(node)` — rescan the foreign relation.
pub fn ExecReScanForeignScan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
    // EState *estate = node->ss.ps.state;
    // PlanState *outerPlan = outerPlanState(node);
    let (operation, _) = foreignscan_plan_fields(node);

    // Ignore direct modifications when EvalPlanQual is active.
    //   if (estate->es_epq_active != NULL && plan->operation != CMD_SELECT)
    //       return;
    if estate.es_epq_active.is_some() && operation != CmdType::CMD_SELECT {
        return Ok(());
    }

    // node->fdwroutine->ReScanForeignScan(node);
    foreign::rescan_foreign_scan::call(node, estate)?;

    // If chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode. outerPlan may also be NULL, in which case there is nothing
    // to rescan at all.
    //   if (outerPlan != NULL && outerPlan->chgParam == NULL)
    //       ExecReScan(outerPlan);
    let outer_needs_rescan = node
        .ss
        .ps
        .lefttree
        .as_deref()
        .map(|p| p.ps_head().chgParam.is_none())
        .unwrap_or(false);
    if outer_needs_rescan {
        let outer = node
            .ss
            .ps
            .lefttree
            .as_deref_mut()
            .expect("checked Some above");
        execAmi::exec_re_scan::call(outer, estate)?;
    }

    // ExecScanReScan(&node->ss);
    execScan::exec_scan_rescan_ss::call(&mut node.ss, estate)
}

// ===========================================================================
//                          Parallel Scan Support
// ===========================================================================

/// `ExecForeignScanEstimate(node, pcxt)` — inform the size of the parallel
/// coordination information, if any.
pub fn ExecForeignScanEstimate<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    pcxt: &mut ParallelContext,
) -> PgResult<()> {
    // FdwRoutine *fdwroutine = node->fdwroutine;
    // if (fdwroutine->EstimateDSMForeignScan)
    if has_estimate_dsm(node) {
        // node->pscan_len = fdwroutine->EstimateDSMForeignScan(node, pcxt);
        node.pscan_len = foreign::estimate_dsm_foreign_scan::call(node, pcxt)?;
        // shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
        let pscan_len = node.pscan_len;
        shm_toc::shm_toc_estimate_chunk::call(pcxt, pscan_len)?;
        // shm_toc_estimate_keys(&pcxt->estimator, 1);
        shm_toc::shm_toc_estimate_keys::call(pcxt, 1)?;
    }
    Ok(())
}

/// `ExecForeignScanInitializeDSM(node, pcxt)` — initialize the parallel
/// coordination information.
pub fn ExecForeignScanInitializeDSM<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    pcxt: &mut ParallelContext,
) -> PgResult<()> {
    // FdwRoutine *fdwroutine = node->fdwroutine;
    // if (fdwroutine->InitializeDSMForeignScan)
    if has_initialize_dsm(node) {
        // int plan_node_id = node->ss.ps.plan->plan_node_id;
        // coordinate = shm_toc_allocate(pcxt->toc, node->pscan_len);
        // fdwroutine->InitializeDSMForeignScan(node, pcxt, coordinate);
        // shm_toc_insert(pcxt->toc, plan_node_id, coordinate);
        //
        // The allocate / FDW-init / insert of the DSM chunk are folded into the
        // FDW seam (it receives `pcxt` and the node's `plan_node_id` /
        // `pscan_len`), since the chunk is a storage-owned `void *` the node
        // only brokers.
        foreign::initialize_dsm_foreign_scan::call(node, pcxt)?;
    }
    Ok(())
}

/// `ExecForeignScanReInitializeDSM(node, pcxt)` — reset shared state before
/// beginning a fresh scan.
pub fn ExecForeignScanReInitializeDSM<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    pcxt: &mut ParallelContext,
) -> PgResult<()> {
    // FdwRoutine *fdwroutine = node->fdwroutine;
    // if (fdwroutine->ReInitializeDSMForeignScan)
    if has_reinitialize_dsm(node) {
        // coordinate = shm_toc_lookup(pcxt->toc, plan_node_id, false);
        // fdwroutine->ReInitializeDSMForeignScan(node, pcxt, coordinate);
        //
        // The TOC lookup of the chunk is folded into the FDW seam (see
        // InitializeDSM).
        foreign::reinitialize_dsm_foreign_scan::call(node, pcxt)?;
    }
    Ok(())
}

/// `ExecForeignScanInitializeWorker(node, pwcxt)` — initialization according to
/// the parallel coordination information.
pub fn ExecForeignScanInitializeWorker<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    pwcxt: &mut ParallelWorkerContext,
) -> PgResult<()> {
    // FdwRoutine *fdwroutine = node->fdwroutine;
    // if (fdwroutine->InitializeWorkerForeignScan)
    if has_initialize_worker(node) {
        // coordinate = shm_toc_lookup(pwcxt->toc, plan_node_id, false);
        // fdwroutine->InitializeWorkerForeignScan(node, pwcxt->toc, coordinate);
        foreign::initialize_worker_foreign_scan::call(node, pwcxt)?;
    }
    Ok(())
}

/// `ExecShutdownForeignScan(node)` — give the FDW a chance to stop asynchronous
/// resource consumption and release any resources still held.
pub fn ExecShutdownForeignScan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // FdwRoutine *fdwroutine = node->fdwroutine;
    // if (fdwroutine->ShutdownForeignScan)
    //     fdwroutine->ShutdownForeignScan(node);
    if has_shutdown(node) {
        foreign::shutdown_foreign_scan::call(node, estate)?;
    }
    Ok(())
}

// ===========================================================================
//                          Asynchronous execution
// ===========================================================================

/// `ExecAsyncForeignScanRequest(areq)` — asynchronously request a tuple from a
/// designated async-capable node.
pub fn ExecAsyncForeignScanRequest(areq: &mut AsyncRequest) -> PgResult<()> {
    // ForeignScanState *node = (ForeignScanState *) areq->requestee;
    // FdwRoutine *fdwroutine = node->fdwroutine;
    // Assert(fdwroutine->ForeignAsyncRequest != NULL);
    // fdwroutine->ForeignAsyncRequest(areq);
    //
    // The requestee node's `fdwroutine` is resolved inside the seam (areq holds
    // the requestee PlanState pointer opaquely); the presence Assert and the
    // invocation both run there.
    foreign::foreign_async_request::call(areq)
}

/// `ExecAsyncForeignScanConfigureWait(areq)` — in async mode, configure for a
/// wait.
pub fn ExecAsyncForeignScanConfigureWait(areq: &mut AsyncRequest) -> PgResult<()> {
    // Assert(fdwroutine->ForeignAsyncConfigureWait != NULL);
    // fdwroutine->ForeignAsyncConfigureWait(areq);
    foreign::foreign_async_configure_wait::call(areq)
}

/// `ExecAsyncForeignScanNotify(areq)` — callback invoked when a relevant event
/// has occurred.
pub fn ExecAsyncForeignScanNotify(areq: &mut AsyncRequest) -> PgResult<()> {
    // Assert(fdwroutine->ForeignAsyncNotify != NULL);
    // fdwroutine->ForeignAsyncNotify(areq);
    foreign::foreign_async_notify::call(areq)
}

// ===========================================================================
// `execScan.h` driver (`ExecScan` -> `ExecScanExtended` -> `ExecScanFetch`),
// inlined into `nodeForeignscan.o` in C; reproduced here as private functions
// (the owned-tree callback ABI cannot be driven generically). Leaf ops go
// through their owners' seams.
// ===========================================================================

/// `ExecScanFetch` — check interrupts & fetch next potential tuple. Returns the
/// slot id of the fetched tuple, or `None` (the C `NULL` / cleared slot).
fn ExecScanFetch<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    epq_active: bool,
    access_mtd: AccessMtd<'mcx>,
    recheck_mtd: RecheckMtd<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    if epq_active {
        // We are inside an EvalPlanQual recheck.
        //   Index scanrelid = ((Scan *) node->ps.plan)->scanrelid;
        let scanrelid = scan_scanrelid(node);

        if scanrelid == 0 {
            // ForeignScan/CustomScan which has pushed down a join.
            //   if (bms_is_member(epqstate->epqParam, node->ps.plan->extParam))
            let epq_param = epq_param(estate);
            let is_member = {
                let ext_param = node
                    .ss
                    .ps
                    .plan
                    .map(|p| p.plan_head())
                    .and_then(|ph| ph.extParam.as_deref());
                bitmapset::bms_is_member::call(epq_param, ext_param)
            };
            if is_member {
                // The recheck method stores the correct tuple in the slot.
                //   TupleTableSlot *slot = node->ss_ScanTupleSlot;
                let slot = node
                    .ss
                    .ss_ScanTupleSlot
                    .expect("ExecScanFetch: ss_ScanTupleSlot not initialized");
                //   if (!(*recheckMtd)(node, slot)) ExecClearTuple(slot);
                if !recheck_mtd(node, estate, slot)? {
                    execTuples::exec_clear_tuple::call(estate, slot)?;
                }
                //   return slot;
                return Ok(Some(slot));
            }
        } else if epq_relsubs_done(estate, scanrelid - 1) {
            // Either there is no EPQ tuple for this rel or we already returned
            // it: return ExecClearTuple(slot).
            let slot = node
                .ss
                .ss_ScanTupleSlot
                .expect("ExecScanFetch: ss_ScanTupleSlot not initialized");
            execTuples::exec_clear_tuple::call(estate, slot)?;
            return Ok(None);
        } else if let Some(epq_slot) = epq_relsubs_slot(estate, scanrelid - 1) {
            // Return replacement tuple provided by the EPQ caller.
            //   TupleTableSlot *slot = epqstate->relsubs_slot[scanrelid - 1];
            //   Assert(epqstate->relsubs_rowmark[scanrelid - 1] == NULL);
            debug_assert!(!epq_relsubs_rowmark_present(estate, scanrelid - 1));
            //   epqstate->relsubs_done[scanrelid - 1] = true;
            epq_set_relsubs_done(estate, scanrelid - 1, true);
            //   if (TupIsNull(slot)) return NULL;
            if estate.slot(epq_slot).is_empty() {
                return Ok(None);
            }
            //   if (!(*recheckMtd)(node, slot)) return ExecClearTuple(slot);
            if !recheck_mtd(node, estate, epq_slot)? {
                execTuples::exec_clear_tuple::call(estate, epq_slot)?;
                return Ok(None);
            }
            //   return slot;
            return Ok(Some(epq_slot));
        } else if epq_relsubs_rowmark_present(estate, scanrelid - 1) {
            // Fetch and return replacement tuple using a non-locking rowmark.
            let slot = node
                .ss
                .ss_ScanTupleSlot
                .expect("ExecScanFetch: ss_ScanTupleSlot not initialized");
            //   epqstate->relsubs_done[scanrelid - 1] = true;
            epq_set_relsubs_done(estate, scanrelid - 1, true);
            //   if (!EvalPlanQualFetchRowMark(epqstate, scanrelid, slot)) return NULL;
            if !execMain::eval_plan_qual_fetch_row_mark::call(estate, scanrelid, slot)? {
                return Ok(None);
            }
            //   if (TupIsNull(slot)) return NULL;
            if estate.slot(slot).is_empty() {
                return Ok(None);
            }
            //   if (!(*recheckMtd)(node, slot)) return ExecClearTuple(slot);
            if !recheck_mtd(node, estate, slot)? {
                execTuples::exec_clear_tuple::call(estate, slot)?;
                return Ok(None);
            }
            //   return slot;
            return Ok(Some(slot));
        }
    }

    // Run the node-type-specific access method function to get the next tuple.
    //   return (*accessMtd)(node);
    access_mtd(node, estate)
}

/// `ExecScanExtended` — the qual/projection scan loop.
fn ExecScanExtended<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    access_mtd: AccessMtd<'mcx>,
    recheck_mtd: RecheckMtd<'mcx>,
    epq_active: bool,
    has_qual: bool,
    has_proj_info: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // ExprContext *econtext = node->ps.ps_ExprContext;  (interrupt checks are
    // in ExecScanFetch)

    // If we have neither a qual to check nor a projection to do, just skip all
    // the overhead and return the raw scan tuple.
    if !has_qual && !has_proj_info {
        // ResetExprContext(econtext);
        reset_per_tuple_expr_context(node, estate);
        return ExecScanFetch(node, epq_active, access_mtd, recheck_mtd, estate);
    }

    // Reset per-tuple memory context to free any expression-evaluation storage
    // allocated in the previous tuple cycle.
    reset_per_tuple_expr_context(node, estate);

    // Get a tuple from the access method. Loop until we obtain a tuple that
    // passes the qualification.
    loop {
        let slot = ExecScanFetch(node, epq_active, access_mtd, recheck_mtd, estate)?;

        // If the slot returned by the accessMtd contains NULL, there is nothing
        // more to scan, so return an empty slot --- being careful to use the
        // projection result slot so it has the correct tupleDesc.
        let Some(slot) = slot else {
            if has_proj_info {
                // return ExecClearTuple(projInfo->pi_state.resultslot);
                let result_slot = node
                    .ss
                    .ps
                    .ps_ResultTupleSlot
                    .expect("ExecScanExtended: ps_ResultTupleSlot not initialized");
                execTuples::exec_clear_tuple::call(estate, result_slot)?;
                return Ok(Some(result_slot));
            } else {
                return Ok(None);
            }
        };

        // Place the current tuple into the expr context.
        //   econtext->ecxt_scantuple = slot;
        if let Some(ecxt) = node.ss.ps.ps_ExprContext {
            if let Some(Some(ec)) = estate.es_exprcontexts.get_mut(ecxt.0 as usize) {
                ec.ecxt_scantuple = Some(slot);
            }
        }

        // Check that the current tuple satisfies the qual-clause.
        //   if (qual == NULL || ExecQual(qual, econtext))
        let passes = if !has_qual {
            true
        } else {
            match (node.ss.ps.qual.as_deref_mut(), node.ss.ps.ps_ExprContext) {
                (Some(state), Some(econtext)) => execExpr::exec_qual::call(state, econtext, estate)?,
                _ => true,
            }
        };
        if passes {
            // Found a satisfactory scan tuple.
            if has_proj_info {
                // Form a projection tuple, store it in the result slot, return it.
                //   return ExecProject(projInfo);
                return Ok(Some(execExpr::exec_project::call(&mut node.ss.ps, estate)?));
            } else {
                // Not projecting, so just return the scan tuple.
                return Ok(Some(slot));
            }
        }
        // else InstrCountFiltered1(node, 1);  (instrumentation arrives with
        // its consumer; the count does not affect control flow.)

        // Tuple fails qual, so free per-tuple memory and try again.
        reset_per_tuple_expr_context(node, estate);
    }
}

/// `ExecScan(node, accessMtd, recheckMtd)` — the generic scan driver.
fn ExecScan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    access_mtd: AccessMtd<'mcx>,
    recheck_mtd: RecheckMtd<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // epqstate = node->ps.state->es_epq_active;
    // qual = node->ps.qual; projInfo = node->ps.ps_ProjInfo;
    let epq_active = estate.es_epq_active.is_some();
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

// ===========================================================================
// In-crate helpers: owned-data reads (the `if (fdwroutine->X)` presence checks
// and owned-plan / EPQ-state reads), and the per-tuple context reset.
// ===========================================================================

/// `INDEX_VAR` (primnodes.h) — varno used in the FDW-supplied scan tlist.
/// `#define INDEX_VAR (-3)` (the special-varno family `INNER_VAR=-1`,
/// `OUTER_VAR=-2`, `INDEX_VAR=-3`, `ROWID_VAR=-4`).
const INDEX_VAR: i32 = -3;

/// `((ForeignScan *) node->ss.ps.plan)->operation` and `->fsSystemCol`, read
/// from the node's owned plan view.
#[inline]
fn foreignscan_plan_fields(node: &ForeignScanState<'_>) -> (CmdType, bool) {
    match node.ss.ps.plan {
        Some(Node::ForeignScan(f)) => (f.operation, f.fsSystemCol),
        Some(other) => panic!("ForeignScanState.plan is not a ForeignScan: {other:?}"),
        None => panic!("ForeignScanState.plan is not set"),
    }
}

/// `((Scan *) node->ss.ps.plan)->scanrelid`.
#[inline]
fn scan_scanrelid(node: &ForeignScanState<'_>) -> u32 {
    match node.ss.ps.plan {
        Some(Node::ForeignScan(f)) => f.scan.scanrelid,
        Some(other) => panic!("ForeignScanState.plan is not a ForeignScan: {other:?}"),
        None => panic!("ForeignScanState.plan is not set"),
    }
}

/// `ResetExprContext(node->ss.ps.ps_ExprContext)` — reset the node's per-tuple
/// memory context (`MemoryContextReset(econtext->ecxt_per_tuple_memory)`).
#[inline]
fn reset_per_tuple_expr_context(node: &ForeignScanState<'_>, estate: &mut EStateData<'_>) {
    if let Some(ecxt) = node.ss.ps.ps_ExprContext {
        if let Some(Some(ec)) = estate.es_exprcontexts.get_mut(ecxt.0 as usize) {
            ec.ecxt_per_tuple_memory.reset();
        }
    }
}

/// `slot->tts_tableOid = RelationGetRelid(node->ss.ss_currentRelation)` —
/// stamp the `tableoid` system column into the FDW-returned scan slot. The
/// relid read and the slot payload write are FDW/relcache-/slot-owned, reached
/// through the seam so the operation stays faithful once the slot payload model
/// and the relcache relid read land.
#[inline]
fn foreign_stamp_tableoid<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    foreign::stamp_scan_slot_tableoid::call(node, estate)
}

/// `epqstate->epqParam`.
#[inline]
fn epq_param(estate: &EStateData<'_>) -> i32 {
    estate
        .es_epq_active
        .as_deref()
        .map(|e| e.epqParam)
        .expect("epq_param: es_epq_active not set")
}

/// `epqstate->relsubs_done[idx]`.
#[inline]
fn epq_relsubs_done(estate: &EStateData<'_>, idx: u32) -> bool {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_done.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .unwrap_or(false)
}

/// `epqstate->relsubs_done[idx] = value`.
#[inline]
fn epq_set_relsubs_done(estate: &mut EStateData<'_>, idx: u32, value: bool) {
    if let Some(e) = estate.es_epq_active.as_deref_mut() {
        if let Some(v) = e.relsubs_done.as_mut() {
            if let Some(slot) = v.get_mut(idx as usize) {
                *slot = value;
            }
        }
    }
}

/// `epqstate->relsubs_slot[idx]` (`Some` = a non-NULL C entry).
#[inline]
fn epq_relsubs_slot(estate: &EStateData<'_>, idx: u32) -> Option<SlotId> {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_slot.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .flatten()
}

/// `epqstate->relsubs_rowmark[idx] != NULL`.
#[inline]
fn epq_relsubs_rowmark_present(estate: &EStateData<'_>, idx: u32) -> bool {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_rowmark.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .unwrap_or(false)
}

/// `node->fdwroutine->RecheckForeignScan != NULL`.
#[inline]
fn has_recheck_foreign_scan(node: &ForeignScanState<'_>) -> bool {
    node.fdwroutine
        .as_ref()
        .is_some_and(|r| r.has_recheck_foreign_scan)
}

/// `node->fdwroutine->EstimateDSMForeignScan != NULL`.
#[inline]
fn has_estimate_dsm(node: &ForeignScanState<'_>) -> bool {
    node.fdwroutine
        .as_ref()
        .is_some_and(|r| r.has_estimate_dsm_foreign_scan)
}

/// `node->fdwroutine->InitializeDSMForeignScan != NULL`.
#[inline]
fn has_initialize_dsm(node: &ForeignScanState<'_>) -> bool {
    node.fdwroutine
        .as_ref()
        .is_some_and(|r| r.has_initialize_dsm_foreign_scan)
}

/// `node->fdwroutine->ReInitializeDSMForeignScan != NULL`.
#[inline]
fn has_reinitialize_dsm(node: &ForeignScanState<'_>) -> bool {
    node.fdwroutine
        .as_ref()
        .is_some_and(|r| r.has_reinitialize_dsm_foreign_scan)
}

/// `node->fdwroutine->InitializeWorkerForeignScan != NULL`.
#[inline]
fn has_initialize_worker(node: &ForeignScanState<'_>) -> bool {
    node.fdwroutine
        .as_ref()
        .is_some_and(|r| r.has_initialize_worker_foreign_scan)
}

/// `node->fdwroutine->ShutdownForeignScan != NULL`.
#[inline]
fn has_shutdown(node: &ForeignScanState<'_>) -> bool {
    node.fdwroutine
        .as_ref()
        .is_some_and(|r| r.has_shutdown_foreign_scan)
}

/// `elog(ERROR, "result relation not initialized")`.
fn elog_error_result_relation_not_initialized() -> PgError {
    PgError::error("result relation not initialized")
}
