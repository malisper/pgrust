//! Port of `src/backend/executor/nodeResult.c` — support for constant nodes
//! needing special code.
//!
//! `Result` nodes are used in queries where no relations are scanned (e.g.
//! `SELECT 1 * 2`, `INSERT ... VALUES (...)`), and to optimise queries with
//! constant qualifications (the "One-Time Filter" shown by `EXPLAIN`): the
//! constant qual is evaluated once at runtime, and if false an empty result set
//! is returned without running the controlled (outer) plan at all.
//!
//! INTERFACE ROUTINES
//! - [`ExecResult`]          — the `PlanState.ExecProcNode` callback.
//! - [`ExecInitResult`]      — initialize the result node.
//! - [`ExecEndResult`]       — shut down the result node.
//! - [`ExecReScanResult`]    — rescan the result node.
//! - [`ExecResultMarkPos`] / [`ExecResultRestrPos`] — mark/restore delegation.
//!
//! The node's whole state machine (the rs_checkqual / rs_done bookkeeping and
//! the project-once vs fetch-from-outer branching) is this crate's owned logic.
//! Operations below the executor-node layer go through the owners' seam crates:
//! interrupt servicing → tcop/postgres; child dispatch / init / teardown /
//! rescan / mark / restore → execProcnode / execAmi; expression compilation and
//! evaluation (`ExecInitQual` / `ExecQual` / `ExecProject`) → execExpr; econtext
//! / slot / projection setup (`ExecAssignExprContext` /
//! `ExecInitResultTupleSlotTL` / `ExecAssignProjectionInfo`) → execUtils /
//! execTuples. The per-tuple memory-context reset (`ResetExprContext`) is done
//! in-crate on the node's econtext, as in the other ported node crates.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_error::elog;

use mcx::{alloc_in, PgBox};
use types_error::{PgResult, DEBUG2, ERROR};
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, TupleSlotKind};
use types_nodes::noderesult::{Result, ResultStateData};
use types_nodes::{EStateData, PlanStateNode, SlotId};

/// nodeMergejoin / nodeMaterial model: this crate installs nothing of its own.
/// `ExecInitResult` / `ExecResult` / `ExecEndResult` / `ExecReScanResult` are
/// reached through the executor dispatch (execProcnode / execAmi), which depend
/// on this crate directly when they land — no cyclic caller, so no
/// `<unit>-seams` crate.
pub fn init_seams() {}

/// `TupIsNull(slot)` (tuptable.h) — true if `slot` is NULL or marked empty.
/// The slot is an id into `estate.es_tupleTable`.
#[inline]
fn tup_is_null(slot: Option<SlotId>, estate: &EStateData<'_>) -> bool {
    match slot {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `ExecResult(pstate)` — the `PlanState.ExecProcNode` callback.
///
/// Returns the tuples from the outer plan which satisfy the qualification
/// clause. Since result nodes with right subtrees are never planned, the right
/// subtree is ignored entirely. The constant-only qualification is checked
/// first; it always returns `None` if the constant qualification is not
/// satisfied.
pub fn ExecResult<'mcx>(
    node: &mut ResultStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    // econtext = node->ps.ps_ExprContext;
    let econtext = node
        .ps
        .ps_ExprContext
        .expect("ExecResult: ps_ExprContext not created");

    // check constant qualifications like (2 > 1), if not already done
    if node.rs_checkqual {
        // bool qualResult = ExecQual(node->resconstantqual, econtext);
        // ExecQual short-circuits a NULL state to true.
        let qual_result = match node.resconstantqual.as_deref() {
            None => true,
            Some(state) => execExpr::exec_qual::call(state, econtext, estate)?,
        };
        node.rs_checkqual = false;
        if !qual_result {
            node.rs_done = true;
            return Ok(None);
        }
    }

    // Reset per-tuple memory context to free any expression evaluation storage
    // allocated in the previous tuple cycle.
    //   ResetExprContext(econtext) == MemoryContextReset(econtext->ecxt_per_tuple_memory)
    estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();

    // if rs_done is true then it means that we were asked to return a constant
    // tuple and we already did the last time ExecResult() was called, OR that we
    // failed the constant qual check. Either way, now we are through.
    if !node.rs_done {
        // outerPlan = outerPlanState(node);
        if node.ps.lefttree.is_some() {
            // retrieve tuples from the outer plan until there are no more.
            //   outerTupleSlot = ExecProcNode(outerPlan);
            let outer_tuple_slot = {
                let outer = node
                    .ps
                    .lefttree
                    .as_deref_mut()
                    .expect("ExecResult: outer plan state present");
                execProcnode::exec_proc_node::call(outer, estate)?
            };

            // if (TupIsNull(outerTupleSlot)) return NULL;
            if tup_is_null(outer_tuple_slot, estate) {
                return Ok(None);
            }

            // prepare to compute projection expressions, which will expect to
            // access the input tuples as varno OUTER:
            //   econtext->ecxt_outertuple = outerTupleSlot;
            estate.ecxt_mut(econtext).ecxt_outertuple = outer_tuple_slot;
        } else {
            // if we don't have an outer plan, then we are just generating the
            // results from a constant target list. Do it only once.
            node.rs_done = true;
        }

        // form the result tuple using ExecProject(), and return it
        return Ok(Some(execExpr::exec_project::call(&mut node.ps, estate)?));
    }

    Ok(None)
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitResult`]:
/// `castNode(ResultState, pstate)` then run [`ExecResult`].
fn exec_result_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::Result(node) => node,
        other => panic!("castNode(ResultState, pstate) failed: {other:?}"),
    };
    ExecResult(node, estate)
}

/// `ExecResultMarkPos(node)` — delegate mark to the outer plan if present,
/// otherwise log at `DEBUG2` (a no-op at runtime: result nodes with no outer
/// plan have nothing to mark).
pub fn ExecResultMarkPos<'mcx>(
    node: &mut ResultStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // PlanState *outerPlan = outerPlanState(node);
    if let Some(outer) = node.ps.lefttree.as_deref_mut() {
        // ExecMarkPos(outerPlan);
        execAmi::exec_mark_pos::call(outer, estate)
    } else {
        // elog(DEBUG2, "Result nodes do not support mark/restore");
        // Below ERROR — does not unwind; returns Ok(()).
        elog(DEBUG2, "Result nodes do not support mark/restore")
    }
}

/// `ExecResultRestrPos(node)` — delegate restore to the outer plan if present,
/// otherwise raise an error (result nodes with no outer plan cannot restore).
pub fn ExecResultRestrPos<'mcx>(
    node: &mut ResultStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // PlanState *outerPlan = outerPlanState(node);
    if let Some(outer) = node.ps.lefttree.as_deref_mut() {
        // ExecRestrPos(outerPlan);
        execAmi::exec_restr_pos::call(outer, estate)
    } else {
        // elog(ERROR, "Result nodes do not support mark/restore");
        elog(ERROR, "Result nodes do not support mark/restore")
    }
}

/// `ExecInitResult(node, estate, eflags)` — create the run-time state for the
/// result node produced by the planner and initialize outer relations (child
/// nodes).
pub fn ExecInitResult<'mcx>(
    plan_node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, ResultStateData<'mcx>>> {
    let node: &'mcx Result<'mcx> = match plan_node {
        types_nodes::nodes::Node::Result(r) => r,
        other => panic!("castNode(Result, node) failed: {other:?}"),
    };

    // check for unsupported flags:
    //   Assert(!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)) ||
    //          outerPlan(node) != NULL);
    debug_assert!(
        eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD) == 0
            || node.plan.lefttree.is_some()
    );

    let mcx = estate.es_query_cxt;

    // create state structure: resstate = makeNode(ResultState);
    //   resstate->ps.plan = (Plan *) node;
    //   resstate->ps.state = estate;
    //   resstate->ps.ExecProcNode = ExecResult;
    let mut resstate = alloc_in(mcx, ResultStateData::default())?;
    resstate.ps.plan = Some(plan_node);
    resstate.ps.ExecProcNode = Some(exec_result_node);

    //   resstate->rs_done = false;
    //   resstate->rs_checkqual = (node->resconstantqual != NULL);
    resstate.rs_done = false;
    resstate.rs_checkqual = node.resconstantqual.is_some();

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &resstate->ps);
    execUtils::exec_assign_expr_context::call(estate, &mut resstate.ps)?;

    // initialize child nodes:
    //   outerPlanState(resstate) = ExecInitNode(outerPlan(node), estate, eflags);
    let outer_plan = node.plan.lefttree.as_deref();
    resstate.ps.lefttree =
        execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;

    // we don't use inner plan: Assert(innerPlan(node) == NULL);
    debug_assert!(node.plan.righttree.is_none());

    // Initialize result slot, type and projection.
    //   ExecInitResultTupleSlotTL(&resstate->ps, &TTSOpsVirtual);
    //   ExecAssignProjectionInfo(&resstate->ps, NULL);
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut resstate.ps,
        estate,
        TupleSlotKind::Virtual,
    )?;
    execUtils::exec_assign_projection_info::call(&mut resstate.ps, estate, None)?;

    // initialize child expressions
    //   resstate->ps.qual = ExecInitQual(node->plan.qual, (PlanState *) resstate);
    let qual_list = node.plan.qual.as_deref();
    resstate.ps.qual = execExpr::exec_init_qual::call(qual_list, &mut resstate.ps, estate)?;
    //   resstate->resconstantqual =
    //       ExecInitQual((List *) node->resconstantqual, (PlanState *) resstate);
    let resconstantqual_list = node.resconstantqual.as_deref();
    resstate.resconstantqual =
        execExpr::exec_init_qual::call(resconstantqual_list, &mut resstate.ps, estate)?;

    Ok(resstate)
}

/// `ExecEndResult(node)` — frees up storage allocated through C routines by
/// shutting down the subplans.
pub fn ExecEndResult<'mcx>(
    node: &mut ResultStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // shut down subplans: ExecEndNode(outerPlanState(node));
    if let Some(outer) = node.ps.lefttree.as_deref_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }
    Ok(())
}

/// `ExecReScanResult(node)` — reset the result node for a rescan.
pub fn ExecReScanResult<'mcx>(
    node: &mut ResultStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    node.rs_done = false;
    node.rs_checkqual = node.resconstantqual.is_some();

    // If chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    //   if (outerPlan && outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    let outer_chg_null = node
        .ps
        .lefttree
        .as_deref()
        .map(|p| p.ps_head().chgParam.is_none());
    if let Some(true) = outer_chg_null {
        let outer = node
            .ps
            .lefttree
            .as_deref_mut()
            .expect("ExecReScanResult: outer plan state present");
        execAmi::exec_re_scan::call(outer, estate)?;
    }

    Ok(())
}
