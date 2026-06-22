//! Port of `src/backend/executor/nodeProjectSet.c` — support for evaluating
//! targetlists containing set-returning functions.
//!
//! `ProjectSet` nodes are inserted by the planner to evaluate set-returning
//! functions (SRFs) in the targetlist. It is guaranteed that all SRFs are
//! directly at the top level of the targetlist (they can't be inside
//! more-complex expressions); if that'd otherwise be the case the planner adds
//! additional `ProjectSet` nodes.
//!
//! INTERFACE ROUTINES
//! - [`ExecProjectSet`]        - retrieve next tuple
//! - [`ExecInitProjectSet`]    - initialize node and subnodes
//! - [`ExecEndProjectSet`]     - shut down node and subnodes
//! - [`ExecReScanProjectSet`]  - rescan the node
//!
//! Following the nodeResult / nodeMergejoin owned-tree model, the C
//! `PlanState.state` back-pointer is replaced by threading `&mut EStateData`
//! explicitly, the outer child plan state lives in `ps.lefttree`
//! (`outerPlanState(node)`), and the produced result-slot id (`return
//! resultSlot`) / `None` (`return NULL`) is handed back rather than a pointer.
//!
//! Calls into unported owners go through those owners' seam crates and panic
//! until the owners land: execProcnode.c's
//! `ExecInitNode`/`ExecProcNode`/`ExecEndNode`; execAmi.c's `ExecReScan`;
//! execUtils.c's `ExecAssignExprContext`; execTuples.c's
//! `ExecInitResultTupleSlotTL`/`ExecClearTuple`/`ExecStoreVirtualTuple` (and the
//! `store_virtual_values` payload write); execExpr.c's
//! `ExecInitExpr`/`ExecEvalExpr`; execSRF.c's
//! `ExecInitFunctionResultSet`/`ExecMakeFunctionResultSet`; tcop/postgres.c's
//! `ProcessInterrupts`.

#![allow(non_snake_case)]

use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execSRF_seams as execSRF;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_tcop_postgres_seams as tcop_postgres;
use mcx::{alloc_in, vec_with_capacity_in, PgBox, PgVec};
use types_error::PgResult;
use types_nodes::execexpr::{ExprDoneCond, SetExprState};
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, TupleSlotKind};
use types_nodes::nodeprojectset::{ProjectSet as ProjectSetPlan, ProjectSetElem, ProjectSetState};
use types_nodes::primnodes::Expr;
use types_nodes::{EStateData, PlanStateNode, SlotId};

/// Name passed to `AllocSetContextCreate` for the per-tSRF argument context
/// (`"tSRF function arguments"` in nodeProjectSet.c).
const TSRF_FUNCTION_ARGUMENTS: &str = "tSRF function arguments";

/// Install this crate's implementations into its seam slots.
///
/// nodeProjectSet has no `<unit>-seams` crate: callers that will need these
/// functions (execProcnode's dispatch tables) can depend on this crate
/// directly without a cycle, since this crate reaches outward only through
/// per-owner seam crates.
pub fn init_seams() {}

/// `ExecProjectSet(pstate)` — return tuples after evaluating the targetlist
/// (which contains set-returning functions).
///
/// Returns `Ok(Some(slot))` (the C non-NULL `resultSlot`) when a row has been
/// produced, `Ok(None)` for end-of-output (the C `NULL`).
pub fn ExecProjectSet<'mcx>(
    node: &mut ProjectSetState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    // econtext = node->ps.ps_ExprContext;
    let econtext = node
        .ps
        .ps_ExprContext
        .expect("ExecProjectSet: ps_ExprContext not initialized");

    // Reset per-tuple context to free expression-evaluation storage allocated
    // for a potentially previously returned tuple. Note that the SRF argument
    // context has a different lifetime and is reset below.
    //   ResetExprContext(econtext);
    estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();

    // Check to see if we're still projecting out tuples from a previous scan
    // tuple (because there is a function-returning-set in the projection
    // expressions). If so, try to project another one.
    //   if (node->pending_srf_tuples)
    //   {
    //       resultSlot = ExecProjectSRF(node, true);
    //       if (resultSlot != NULL) return resultSlot;
    //   }
    if node.pending_srf_tuples {
        if let Some(slot) = ExecProjectSRF(node, true, estate)? {
            return Ok(Some(slot));
        }
    }

    // Get another input tuple and project SRFs from it.
    loop {
        // Reset argument context to free any expression evaluation storage
        // allocated in the previous tuple cycle. Note this can't happen until
        // we're done projecting out tuples from a scan tuple, as ValuePerCall
        // functions are allowed to reference the arguments for each returned
        // tuple. However, if we loop around after finding that no rows are
        // produced from a scan tuple, we should reset, to avoid leaking memory
        // when many successive scan tuples produce no rows.
        //   MemoryContextReset(node->argcontext);
        node.argcontext
            .as_mut()
            .expect("ExecProjectSet: argcontext not initialized")
            .reset();

        // Retrieve tuples from the outer plan until there are no more.
        //   outerPlan = outerPlanState(node);
        //   outerTupleSlot = ExecProcNode(outerPlan);
        let outerPlan = node
            .ps
            .lefttree
            .as_deref_mut()
            .expect("ExecProjectSet: outerPlanState not initialized");
        let outerTupleSlot = execProcnode::exec_proc_node::call(outerPlan, estate)?;

        // if (TupIsNull(outerTupleSlot)) return NULL;
        let outer_id = match outerTupleSlot {
            Some(id) if !estate.slot(id).is_empty() => id,
            _ => return Ok(None),
        };

        // Prepare to compute projection expressions, which will expect to
        // access the input tuples as varno OUTER.
        //   econtext->ecxt_outertuple = outerTupleSlot;
        estate.ecxt_mut(econtext).ecxt_outertuple = Some(outer_id);

        // Evaluate the expressions.
        //   resultSlot = ExecProjectSRF(node, false);
        //   if (resultSlot) return resultSlot;
        if let Some(slot) = ExecProjectSRF(node, false, estate)? {
            return Ok(Some(slot));
        }

        // When we do loop back, we'd better reset the econtext again, just in
        // case the SRF leaked some memory there.
        //   ResetExprContext(econtext);
        estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
    }
}

/// `ExecProjectSRF(node, continuing)` — project a targetlist containing one or
/// more set-returning functions.
///
/// `continuing` indicates whether to continue projecting rows for the same
/// input tuple, or whether a new input tuple is being projected. Returns
/// `Ok(None)` if no output tuple has been produced (the C `NULL`),
/// `Ok(Some(slot))` if a virtual tuple was stored into the node's result slot
/// (the C non-NULL `resultSlot`).
fn ExecProjectSRF<'mcx>(
    node: &mut ProjectSetState<'mcx>,
    continuing: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // TupleTableSlot *resultSlot = node->ps.ps_ResultTupleSlot;
    let resultSlot = node
        .ps
        .ps_ResultTupleSlot
        .expect("ExecProjectSRF: ps_ResultTupleSlot not initialized");
    // ExprContext *econtext = node->ps.ps_ExprContext;
    let econtext = node
        .ps
        .ps_ExprContext
        .expect("ExecProjectSRF: ps_ExprContext not initialized");

    // ExecClearTuple(resultSlot);
    execTuples::exec_clear_tuple::call(estate, resultSlot)?;

    // Call SRFs, as well as plain expressions, in per-tuple context.
    //   oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    // The owned model has no ambient "current context"; each evaluation seam
    // (`exec_eval_expr_switch_context` / `exec_make_function_result_set`)
    // performs its own per-tuple-context switch, so the explicit
    // `MemoryContextSwitchTo` dance is a no-op here.

    // Assume no further tuples are produced unless an ExprMultipleResult is
    // encountered from a set returning function.
    //   node->pending_srf_tuples = false;
    node.pending_srf_tuples = false;

    // hassrf = hasresult = false;
    let mut hassrf = false;
    let mut hasresult = false;

    // The C writes each column's `*result`/`*isnull` into the result slot's
    // `tts_values[argno]`/`tts_isnull[argno]` as it goes, then stores the
    // virtual tuple at the end iff `hasresult`. The slot's value-array payload
    // is owned by execTuples (the `store_virtual_values` seam), so the
    // per-column results are accumulated into these local buffers (this node's
    // mirror of the slot's value arrays) and committed in one `store_virtual_
    // values` call only when a row is produced; otherwise the slot stays cleared
    // (`ExecClearTuple` above), exactly as in C.
    let nelems = node.nelems as usize;
    let mut result: PgVec<'mcx, types_tuple::backend_access_common_heaptuple::Datum<'mcx>> =
        vec_with_capacity_in(estate.es_query_cxt, nelems)?;
    let mut isnull: PgVec<'mcx, bool> = vec_with_capacity_in(estate.es_query_cxt, nelems)?;

    // Disjoint borrows of the node's distinct fields so the SRF argument
    // context (`&node.argcontext`) and the compiled elements / per-elem
    // is-done (`&mut node.elems` / `&mut node.elemdone`) can be held across the
    // per-column loop simultaneously.
    let ProjectSetState {
        elems,
        elemdone,
        argcontext,
        pending_srf_tuples,
        ..
    } = &mut *node;
    let elems = elems.as_mut().expect("ExecProjectSRF: elems not initialized");
    let elemdone = elemdone
        .as_mut()
        .expect("ExecProjectSRF: elemdone not initialized");
    let argcontext = argcontext
        .as_ref()
        .expect("ExecProjectSRF: argcontext not initialized");

    // for (argno = 0; argno < node->nelems; argno++)
    for argno in 0..nelems {
        // ExprDoneCond *isdone = &node->elemdone[argno];
        let isdone_prev = elemdone[argno];

        if continuing && isdone_prev == ExprDoneCond::ExprEndResult {
            // If we're continuing to project output rows from a source tuple,
            // return NULLs once the SRF has been exhausted.
            //   *result = (Datum) 0; *isnull = true; hassrf = true;
            result.push(types_tuple::backend_access_common_heaptuple::Datum::null());
            isnull.push(true);
            hassrf = true;
        } else {
            // Node *elem = node->elems[argno];
            match &mut elems[argno] {
                // else if (IsA(elem, SetExprState))
                ProjectSetElem::Srf(fcache) => {
                    // Evaluate SRF - possibly continuing previously started
                    // output.
                    //   *result = ExecMakeFunctionResultSet(
                    //       (SetExprState *) elem, econtext, node->argcontext,
                    //       isnull, isdone);
                    let (value, this_isnull, this_isdone) =
                        execSRF::exec_make_function_result_set::call(
                            fcache, econtext, argcontext, estate,
                        )?;
                    // execSRF and store_virtual_values now share the canonical
                    // `Datum<'mcx>` value model; carry the result straight
                    // through into the result vector.
                    result.push(value);
                    isnull.push(this_isnull);
                    elemdone[argno] = this_isdone;

                    // if (*isdone != ExprEndResult) hasresult = true;
                    if this_isdone != ExprDoneCond::ExprEndResult {
                        hasresult = true;
                    }
                    // if (*isdone == ExprMultipleResult)
                    //     node->pending_srf_tuples = true;
                    if this_isdone == ExprDoneCond::ExprMultipleResult {
                        *pending_srf_tuples = true;
                    }
                    // hassrf = true;
                    hassrf = true;
                }
                // else { non-SRF tlist expression, just evaluate normally. }
                ProjectSetElem::Plain(exprstate) => {
                    //   *result = ExecEvalExpr((ExprState *) elem, econtext,
                    //                          isnull);
                    //   *isdone = ExprSingleResult;
                    let (value, this_isnull) =
                        execExpr::exec_eval_expr_switch_context::call(exprstate, econtext, estate)?;
                    result.push(value);
                    isnull.push(this_isnull);
                    elemdone[argno] = ExprDoneCond::ExprSingleResult;
                }
            }
        }
    }

    //   MemoryContextSwitchTo(oldcontext);  -- no ambient context

    // ProjectSet should not be used if there's no SRFs.
    //   Assert(hassrf);
    debug_assert!(hassrf, "ProjectSet used with no set-returning functions");

    // If all the SRFs returned ExprEndResult, we consider that as no row being
    // produced.
    //   if (hasresult)
    //   {
    //       ExecStoreVirtualTuple(resultSlot);
    //       return resultSlot;
    //   }
    //   return NULL;
    if hasresult {
        execTuples::store_virtual_values::call(estate, resultSlot, &result, &isnull)?;
        Ok(Some(resultSlot))
    } else {
        Ok(None)
    }
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitProjectSet`]:
/// `castNode(ProjectSetState, pstate)` then run [`ExecProjectSet`].
fn exec_project_set_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::ProjectSet(node) => node,
        other => panic!("castNode(ProjectSetState, pstate) failed: {other:?}"),
    };
    ExecProjectSet(node, estate)
}

/// `ExecInitProjectSet(node, estate, eflags)` — create the run-time state
/// information for the ProjectSet node produced by the planner and initialize
/// outer relations (child nodes).
///
/// Takes the enclosing plan-tree [`Node`](types_nodes::nodes::Node); the state
/// is allocated in `estate.es_query_cxt` (C: `makeNode` in the per-query
/// context during `ExecInitNode`). Panics if the node is not a `ProjectSet`
/// (the C `castNode`).
pub fn ExecInitProjectSet<'mcx>(
    node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, ProjectSetState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let psplan: &'mcx ProjectSetPlan<'mcx> = node.expect_projectset();

    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)));
    debug_assert!(
        (eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)) == 0,
        "ProjectSet does not support EXEC_FLAG_MARK/EXEC_FLAG_BACKWARD"
    );

    // create state structure
    //   state = makeNode(ProjectSetState);
    //   state->ps.plan = (Plan *) node;
    //   state->ps.state = estate;
    //   state->ps.ExecProcNode = ExecProjectSet;
    let mut state = alloc_in(mcx, ProjectSetState::default())?;
    state.ps.plan = Some(node);
    state.ps.ExecProcNode = Some(exec_project_set_node);

    //   state->pending_srf_tuples = false;
    state.pending_srf_tuples = false;

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &state->ps);
    execUtils::exec_assign_expr_context::call(estate, &mut state.ps)?;

    // initialize child nodes
    //   outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags);
    let outerPlan = psplan.plan.lefttree.as_deref();
    state.ps.lefttree = execProcnode::exec_init_node::call(mcx, outerPlan, estate, eflags)?;

    // we don't use inner plan
    //   Assert(innerPlan(node) == NULL);
    debug_assert!(
        psplan.plan.righttree.is_none(),
        "ProjectSet plan must not have an inner plan"
    );

    // tuple table and result type initialization
    //   ExecInitResultTupleSlotTL(&state->ps, &TTSOpsVirtual);
    execTuples::exec_init_result_tuple_slot_tl::call(&mut state.ps, estate, TupleSlotKind::Virtual)?;

    // Create workspace for per-tlist-entry expr state & SRF-is-done state.
    //   state->nelems = list_length(node->plan.targetlist);
    //   state->elems = (Node **) palloc(sizeof(Node *) * state->nelems);
    //   state->elemdone = (ExprDoneCond *) palloc(sizeof(ExprDoneCond) * nelems);
    let nelems = psplan
        .plan
        .targetlist
        .as_ref()
        .map(|tl| tl.len())
        .unwrap_or(0);
    state.nelems = i32::try_from(nelems).map_err(|_| {
        types_error::PgError::error("ProjectSet has too many targetlist entries".to_string())
    })?;
    let mut elems: PgVec<'mcx, ProjectSetElem<'mcx>> = vec_with_capacity_in(mcx, nelems)?;
    let mut elemdone: PgVec<'mcx, ExprDoneCond> = vec_with_capacity_in(mcx, nelems)?;

    // Build expressions to evaluate targetlist. We can't use
    // ExecBuildProjectionInfo here, since that doesn't deal with SRFs. Instead
    // compile each expression separately, using ExecInitFunctionResultSet where
    // applicable.
    //   off = 0;
    //   foreach(lc, node->plan.targetlist) { ...; off++; }
    let econtext = state
        .ps
        .ps_ExprContext
        .expect("ExecInitProjectSet: ExecAssignExprContext left ps_ExprContext unset");
    for off in 0..nelems {
        // TargetEntry *te = (TargetEntry *) lfirst(lc);
        // Expr *expr = te->expr;
        let te = &psplan.plan.targetlist.as_ref().expect("targetlist present")[off];
        let expr = te
            .expr
            .as_deref()
            .expect("ProjectSet targetlist entry has a NULL expr");

        // if ((IsA(expr, FuncExpr) && ((FuncExpr *) expr)->funcretset) ||
        //     (IsA(expr, OpExpr) && ((OpExpr *) expr)->opretset))
        let elem = if expr_returns_set(expr) {
            //   state->elems[off] = (Node *)
            //       ExecInitFunctionResultSet(expr, state->ps.ps_ExprContext,
            //                                 &state->ps);
            let fcache: PgBox<'mcx, SetExprState<'mcx>> =
                execSRF::exec_init_function_result_set::call(expr, econtext, &mut state.ps, estate)?;
            ProjectSetElem::Srf(fcache)
        } else {
            //   Assert(!expression_returns_set((Node *) expr));
            //   state->elems[off] = (Node *) ExecInitExpr(expr, &state->ps);
            let exprstate = execExpr::exec_init_expr::call(expr, &mut state.ps, estate)?;
            ProjectSetElem::Plain(exprstate)
        };
        elems.push(elem);
        // `elemdone[]` entries are written the first time each element is
        // evaluated, exactly as in C (palloc, not palloc0); the placeholder
        // `ExprSingleResult` keeps the array length == nelems.
        elemdone.push(ExprDoneCond::ExprSingleResult);
    }
    state.elems = Some(elems);
    state.elemdone = Some(elemdone);

    // We don't support any qual on ProjectSet nodes.
    //   Assert(node->plan.qual == NIL);
    debug_assert!(
        psplan.plan.qual.as_ref().map(|q| q.is_empty()).unwrap_or(true),
        "ProjectSet nodes do not support a qual"
    );

    // Create a memory context that ExecMakeFunctionResultSet can use to
    // evaluate function arguments in. We can't use the per-tuple context for
    // this because it gets reset too often; but we don't want to leak
    // evaluation results into the query-lifespan context either. We use one
    // context for the arguments of all tSRFs, as they have roughly equivalent
    // lifetimes.
    //   state->argcontext = AllocSetContextCreate(CurrentMemoryContext,
    //                                             "tSRF function arguments",
    //                                             ALLOCSET_DEFAULT_SIZES);
    state.argcontext = Some(mcx.context().new_child(TSRF_FUNCTION_ARGUMENTS));

    Ok(state)
}

/// `ExecEndProjectSet(node)` — free up storage allocated through C routines
/// (here just: shut down the subplan).
pub fn ExecEndProjectSet<'mcx>(
    node: &mut ProjectSetState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // shut down subplans
    //   ExecEndNode(outerPlanState(node));
    //
    // The C calls ExecEndNode unconditionally; ExecEndNode(NULL) is a no-op.
    if let Some(outerPlan) = node.ps.lefttree.as_deref_mut() {
        execProcnode::exec_end_node::call(outerPlan, estate)?;
    }
    Ok(())
}

/// `ExecReScanProjectSet(node)` — rescan the node.
pub fn ExecReScanProjectSet<'mcx>(
    node: &mut ProjectSetState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Forget any incompletely-evaluated SRFs.
    //   node->pending_srf_tuples = false;
    node.pending_srf_tuples = false;

    // Reset any SRF element that was abandoned mid value-per-call series (e.g. a
    // tSRF cut short by an enclosing LIMIT), so re-projecting from this node
    // restarts the function rather than continuing the stale series.
    //
    // In C this is done implicitly: `init_MultiFuncCall` registers
    // `shutdown_MultiFuncCall` on `node->ps_ExprContext`, and the generic
    // `ExecReScan` calls `ReScanExprContext(node->ps_ExprContext)` (firing those
    // callbacks, unbinding each SRF's `flinfo->fn_extra`) before dispatching to
    // `ExecReScanProjectSet`. The owned model cannot register that bare-`fn`
    // callback (the cross-call `fn_extra` lives on the owned call frame), so the
    // teardown is driven here directly over this node's SRF elements.
    if let Some(elems) = node.elems.as_mut() {
        for elem in elems.iter_mut() {
            if let ProjectSetElem::Srf(fcache) = elem {
                execSRF::restart_set_expr_state::call(fcache)?;
            }
        }
    }

    // If chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    //   PlanState *outerPlan = outerPlanState(node);
    //   if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    let outerPlan = node
        .ps
        .lefttree
        .as_deref_mut()
        .expect("ExecReScanProjectSet: outerPlanState not initialized");
    if outerPlan.ps_head().chgParam.is_none() {
        execAmi::exec_re_scan::call(outerPlan, estate)?;
    }
    Ok(())
}

/// `(IsA(expr, FuncExpr) && ((FuncExpr *) expr)->funcretset) || (IsA(expr,
/// OpExpr) && ((OpExpr *) expr)->opretset)` — is this targetlist expression a
/// set-returning function/operator? (the C init walker's discriminant). Any
/// other expression node is a plain (non-set-returning) expression.
fn expr_returns_set(expr: &Expr) -> bool {
    match expr {
        Expr::FuncExpr(func) => func.funcretset,
        Expr::OpExpr(op) => op.opretset,
        _ => false,
    }
}

#[cfg(test)]
mod tests;
