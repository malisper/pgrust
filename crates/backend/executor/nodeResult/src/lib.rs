//! Port of `src/backend/executor/nodeResult.c` — support for constant nodes
//! needing special code.
//!
//! Result nodes are used in queries where no relations are scanned (e.g.
//! `SELECT 1*2`, `INSERT ... VALUES`), and to optimise queries with a constant
//! ("one-time") qualification hoisted above a controlled subplan (e.g.
//! `SELECT * FROM emp WHERE 2 > 1`). At runtime the constant qual is evaluated
//! once (the "One-Time Filter"); if false the result set is empty and the
//! controlled plan is never run.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitResult`]        - initialize node and subnodes
//! - [`ExecResult`]            - retrieve next tuple
//! - [`ExecEndResult`]         - shut down node and subnodes
//! - [`ExecResultMarkPos`]     - save current scan position
//! - [`ExecResultRestrPos`]    - restore saved scan position
//! - [`ExecReScanResult`]      - rescan the node
//!
//! Following the nodeMaterial / nodeMergejoin owned-tree model, the C
//! `PlanState.state` back-pointer is replaced by threading `&mut EStateData`
//! explicitly, and `ExecResult` returns the produced result-slot id (the C
//! `return slot` / `ExecProject` return) or `None` (the C `return NULL`).
//!
//! Calls into unported owners (execProcnode.c's
//! `ExecInitNode`/`ExecProcNode`/`ExecEndNode`, execAmi.c's
//! `ExecMarkPos`/`ExecRestrPos`/`ExecReScan`, execTuples.c's
//! `ExecInitResultTupleSlotTL`, execUtils.c's
//! `ExecAssignExprContext`/`ExecAssignProjectionInfo`, execExpr.c's
//! `ExecInitQual`/`ExecQual`/`ExecProject`, tcop/postgres.c's
//! `ProcessInterrupts`) go through those owners' seam crates and panic until
//! the owners land.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use execAmi_seams as execAmi;
use execExpr_seams as execExpr;
use execProcnode_seams as execProcnode;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use postgres_seams as tcop_postgres;
use ::utils_error::elog;
use mcx::{alloc_in, PgBox};
use types_error::{PgResult, DEBUG2, ERROR};
use ::nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, TupleSlotKind};
use ::nodes::noderesult::{Result as ResultPlan, ResultState};
use nodes::{EStateData, PlanStateNode, SlotId};

/// Install this crate's implementations into its seam slots.
///
/// nodeResult has no `<unit>-seams` crate: callers that will need these
/// functions (execProcnode's dispatch tables) can depend on this crate
/// directly without a cycle, since this crate reaches outward only through
/// per-owner seam crates.
pub fn init_seams() {}

/// `ExecResult(pstate)` — returns the tuples from the outer plan which satisfy
/// the qualification clause.
///
/// The qualification containing only constant clauses is checked first before
/// any processing is done; if the constant qual is not satisfied this returns
/// `Ok(None)` (the C `return NULL`). Returns `Ok(Some(slot))` (the C
/// `ExecProject` returned slot) when a tuple is produced.
pub fn ExecResult<'mcx>(
    node: &mut ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    tcop_postgres::check_for_interrupts::call()?;

    // econtext = node->ps.ps_ExprContext;
    let econtext = node
        .ps
        .ps_ExprContext
        .expect("ExecResult: ps_ExprContext not initialized");

    // check constant qualifications like (2 > 1), if not already done
    if node.rs_checkqual {
        // bool qualResult = ExecQual(node->resconstantqual, econtext);
        // ExecQual with a NULL state is always-true.
        let qual_result = match node.resconstantqual.as_deref_mut() {
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
    // allocated in the previous tuple cycle. ResetExprContext(econtext).
    estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();

    // if rs_done is true then it means that we were asked to return a constant
    // tuple and we already did the last time ExecResult() was called, OR that
    // we failed the constant qual check. Either way, now we are through.
    if !node.rs_done {
        // outerPlan = outerPlanState(node);
        if node.ps.lefttree.is_some() {
            // retrieve tuples from the outer plan until there are no more.
            // outerTupleSlot = ExecProcNode(outerPlan);
            let outerPlan = node
                .ps
                .lefttree
                .as_deref_mut()
                .expect("ExecResult: checked Some above");
            let outerTupleSlot = execProcnode::exec_proc_node::call(outerPlan, estate)?;

            // if (TupIsNull(outerTupleSlot)) return NULL;
            let outer_id = match outerTupleSlot {
                Some(id) if !estate.slot(id).is_empty() => id,
                _ => return Ok(None),
            };

            // prepare to compute projection expressions, which will expect to
            // access the input tuples as varno OUTER.
            // econtext->ecxt_outertuple = outerTupleSlot;
            estate.ecxt_mut(econtext).ecxt_outertuple = Some(outer_id);
        } else {
            // if we don't have an outer plan, then we are just generating the
            // results from a constant target list. Do it only once.
            node.rs_done = true;
        }

        // form the result tuple using ExecProject(), and return it
        // return ExecProject(node->ps.ps_ProjInfo);
        let slot = execExpr::exec_project::call(&mut node.ps, estate)?;
        return Ok(Some(slot));
    }

    Ok(None)
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitResult`]:
/// `castNode(ResultState, pstate)` then run [`ExecResult`], returning the
/// produced result-slot id (the C `return slot`) or `None` (the C
/// `return NULL`).
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

/// `ExecResultMarkPos(node)`.
pub fn ExecResultMarkPos<'mcx>(
    node: &mut ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // PlanState *outerPlan = outerPlanState(node);
    if let Some(outerPlan) = node.ps.lefttree.as_deref_mut() {
        execAmi::exec_mark_pos::call(outerPlan, estate)
    } else {
        // elog(DEBUG2, "Result nodes do not support mark/restore");
        elog(DEBUG2, "Result nodes do not support mark/restore")
    }
}

/// `ExecResultRestrPos(node)`.
pub fn ExecResultRestrPos<'mcx>(
    node: &mut ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // PlanState *outerPlan = outerPlanState(node);
    if let Some(outerPlan) = node.ps.lefttree.as_deref_mut() {
        execAmi::exec_restr_pos::call(outerPlan, estate)
    } else {
        // elog(ERROR, "Result nodes do not support mark/restore");
        elog(ERROR, "Result nodes do not support mark/restore")
    }
}

/// `ExecInitResult(node, estate, eflags)`.
///
/// Creates the run-time state information for the result node produced by the
/// planner and initializes outer relations (child nodes). The state tree is
/// allocated in `estate.es_query_cxt` (C: `makeNode` in the per-query context
/// current during `ExecInitNode`), so initialization is fallible on OOM.
///
/// Takes the enclosing plan-tree [`Node`](::nodes::nodes::Node); the
/// state's plan back-link aliases the shared, read-only plan tree exactly as
/// C's `resstate->ps.plan = (Plan *) node` does. Panics if the node is not a
/// `Result` (the C `castNode`).
pub fn ExecInitResult<'mcx>(
    node: &'mcx ::nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, ResultState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let result: &'mcx ResultPlan<'mcx> = node.expect_result();

    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)) ||
    //          outerPlan(node) != NULL);
    debug_assert!(
        (eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)) == 0
            || result.plan.lefttree.is_some()
    );

    // create state structure
    //   resstate = makeNode(ResultState);
    //   resstate->ps.plan = (Plan *) node;
    //   resstate->ps.state = estate;
    //   resstate->ps.ExecProcNode = ExecResult;
    let mut resstate = alloc_in(mcx, ResultState::default())?;
    resstate.ps.plan = Some(node);
    resstate.ps.ExecProcNode = Some(exec_result_node);

    //   resstate->rs_done = false;
    //   resstate->rs_checkqual = (node->resconstantqual != NULL);
    resstate.rs_done = false;
    resstate.rs_checkqual = result.resconstantqual.is_some();

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &resstate->ps);
    execUtils::exec_assign_expr_context::call(estate, &mut resstate.ps)?;

    // initialize child nodes
    //   outerPlanState(resstate) = ExecInitNode(outerPlan(node), estate, eflags);
    let outerPlan = result.plan.lefttree.as_deref();
    resstate.ps.lefttree = execProcnode::exec_init_node::call(mcx, outerPlan, estate, eflags)?;

    // we don't use inner plan
    //   Assert(innerPlan(node) == NULL);
    debug_assert!(result.plan.righttree.is_none());

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
    //   resstate->resconstantqual =
    //       ExecInitQual((List *) node->resconstantqual, (PlanState *) resstate);
    let plan_qual = result.plan.qual.as_deref();
    resstate.ps.qual = execExpr::exec_init_qual::call(plan_qual, &mut resstate.ps, estate)?;
    let resconstantqual = result.resconstantqual.as_deref();
    resstate.resconstantqual =
        execExpr::exec_init_qual::call(resconstantqual, &mut resstate.ps, estate)?;

    Ok(resstate)
}

/// `ExecEndResult(node)` — frees up storage allocated through C routines (shut
/// down subplans).
pub fn ExecEndResult<'mcx>(
    node: &mut ResultState<'mcx>,
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

/// `ExecReScanResult(node)`.
pub fn ExecReScanResult<'mcx>(
    node: &mut ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // node->rs_done = false;
    // node->rs_checkqual = (node->resconstantqual != NULL);
    node.rs_done = false;
    node.rs_checkqual = node.resconstantqual.is_some();

    // If chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    //   PlanState *outerPlan = outerPlanState(node);
    //   if (outerPlan && outerPlan->chgParam == NULL)
    //       ExecReScan(outerPlan);
    if let Some(outerPlan) = node.ps.lefttree.as_deref_mut() {
        if outerPlan.ps_head().chgParam.is_none() {
            execAmi::exec_re_scan::call(outerPlan, estate)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
