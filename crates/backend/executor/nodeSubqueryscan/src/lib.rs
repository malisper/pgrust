//! Port of `src/backend/executor/nodeSubqueryscan.c` — support routines for
//! scanning subqueries (subselects in the range table).
//!
//! This is just enough different from sublinks (`nodeSubplan.c`) to mean that
//! we need two sets of code.
//!
//! INTERFACE ROUTINES
//! - [`ExecSubqueryScan`]         scans a subquery
//! - `SubqueryNext`               retrieve next tuple in sequential order
//! - [`ExecInitSubqueryScan`]     creates and initializes a subqueryscan node
//! - [`ExecEndSubqueryScan`]      releases any storage allocated
//! - [`ExecReScanSubqueryScan`]   rescans the relation
//!
//! `SubqueryNext` returns the subplan's own result slot directly (the C avoids
//! `ExecCopySlot`); the node's own `ScanTupleSlot` is used only for
//! EvalPlanQual rechecks. The cross-node recursion (`ExecProcNode`,
//! `ExecInitNode`, `ExecEndNode`, `ExecReScan`) goes through the central
//! dispatch crates' seams; the `ExecScan` driver and `ExecScanReScan` go
//! through `execScan`; expression-context / scan-slot / result-type /
//! projection setup go through `execUtils`/`execTuples`/`execScan`; qual
//! compilation goes through `execExpr`. Every direct field assignment the C
//! performs is performed here in-crate.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use execAmi_seams as execAmi;
use execExpr_seams as execExpr;
use execProcnode_seams as execProcnode;
use execScan_seams as execScan;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;

use types_error::PgResult;
use nodes::{EStateData, SlotId, SubqueryScan, SubqueryScanState};

// ===========================================================================
// Executor flags (executor.h) consulted by the init path.
// ===========================================================================

/// `EXEC_FLAG_MARK` (executor.h) — caller needs mark/restore support. A
/// subquery scan never supports mark/restore.
const EXEC_FLAG_MARK: i32 = 0x0010;

/// Install this crate's seam implementations. `nodeSubqueryscan.c` owns no
/// inward seams (no other crate calls into it across a cycle — the executor
/// dispatch crate depends on it directly, and it exposes no parallel-scan
/// entry points), so there is nothing to install. Present for the uniform
/// `init_seams()` wiring contract.
pub fn init_seams() {}

// ===========================================================================
//                              Scan Support
// ===========================================================================

/// `SubqueryNext(node)` — the workhorse access method for `ExecSubqueryScan`.
///
/// Gets the next tuple from the sub-query and returns the subplan's own result
/// slot directly, rather than expending extra cycles for `ExecCopySlot()` (the
/// node's own `ScanTupleSlot` is used only for EvalPlanQual rechecks). The
/// cross-node recursion follows the child's installed `ExecProcNode` dispatch
/// through the seam, never a direct sibling-crate call. `None` is the C `NULL`
/// at end of scan.
fn SubqueryNext<'mcx>(
    node: &mut SubqueryScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // Get the next tuple from the sub-query.
    //   slot = ExecProcNode(node->subplan);
    let subplan = node
        .subplan
        .as_deref_mut()
        .expect("SubqueryNext: node->subplan not initialized");
    let slot = execProcnode::exec_proc_node::call(subplan, estate)?;

    // We just return the subplan's result slot.
    Ok(slot)
}

/// `SubqueryRecheck(node, slot)` — access-method routine to recheck a tuple in
/// EvalPlanQual. Nothing to check for a subquery scan, so it always succeeds.
fn SubqueryRecheck<'mcx>(
    _node: &mut SubqueryScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // nothing to check
    Ok(true)
}

// ===========================================================================
//                          Interface Routines
// ===========================================================================

/// `ExecSubqueryScan(pstate)` — the `PlanState.ExecProcNode` callback.
///
/// Scans the subquery sequentially and returns the next qualifying tuple by
/// calling `ExecScan` (the `execScan.c` driver, reached through the seam) with
/// this node's `SubqueryNext` / `SubqueryRecheck` access-method functions.
pub fn ExecSubqueryScan<'mcx>(
    node: &mut SubqueryScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // return ExecScan(&node->ss,
    //                 (ExecScanAccessMtd) SubqueryNext,
    //                 (ExecScanRecheckMtd) SubqueryRecheck);
    execScan::exec_scan_subquery::call(node, estate, SubqueryNext, SubqueryRecheck)
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitSubqueryScan`]:
/// `castNode(SubqueryScanState, pstate)` then run [`ExecSubqueryScan`].
fn exec_subquery_scan_node<'mcx>(
    pstate: &mut nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        nodes::PlanStateNode::SubqueryScan(node) => node,
        other => panic!(
            "castNode(SubqueryScanState, pstate) failed: tag {}",
            other.tag()
        ),
    };
    ExecSubqueryScan(node, estate)
}

/// `ExecInitSubqueryScan(node, estate, eflags)` — create and initialize a
/// `SubqueryScanState`. Allocates the state tree in `estate.es_query_cxt` (C:
/// `makeNode` in the per-query context current during `ExecInitNode`), so it is
/// fallible on OOM.
pub fn ExecInitSubqueryScan<'mcx>(
    node: &'mcx SubqueryScan<'mcx>,
    plan_node: &'mcx nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<mcx::PgBox<'mcx, SubqueryScanState<'mcx>>> {
    // check for unsupported flags
    //   Assert(!(eflags & EXEC_FLAG_MARK));
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);

    // SubqueryScan should not have any "normal" children:
    //   Assert(outerPlan(node) == NULL);
    //   Assert(innerPlan(node) == NULL);
    debug_assert!(node.scan.plan.lefttree.is_none());
    debug_assert!(node.scan.plan.righttree.is_none());

    let mcx = estate.es_query_cxt;

    // create state structure (makeNode(SubqueryScanState))
    //   subquerystate->ss.ps.plan = (Plan *) node;
    //   subquerystate->ss.ps.state = estate;
    //   subquerystate->ss.ps.ExecProcNode = ExecSubqueryScan;
    let mut subquerystate = mcx::alloc_in(mcx, SubqueryScanState::default())?;
    subquerystate.ss.ps.plan = Some(plan_node);
    subquerystate.ss.ps.ExecProcNode = Some(exec_subquery_scan_node);

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &subquerystate->ss.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut subquerystate.ss.ps)?;

    // initialize subquery
    //   subquerystate->subplan = ExecInitNode(node->subplan, estate, eflags);
    let subplan_plan = node.subplan.as_deref();
    let subplan = execProcnode::exec_init_node::call(mcx, subplan_plan, estate, eflags)?;
    subquerystate.subplan = subplan;

    // Initialize scan slot and type (needed by ExecAssignScanProjectionInfo).
    //   ExecInitScanTupleSlot(estate, &subquerystate->ss,
    //                         ExecGetResultType(subquerystate->subplan),
    //                         ExecGetResultSlotOps(subquerystate->subplan, NULL));
    let result_type = {
        let subplan = subquerystate
            .subplan
            .as_deref()
            .expect("ExecInitSubqueryScan: subplan just initialized");
        // ExecGetResultType lends the child's result descriptor; copy it into
        // mcx since ExecInitScanTupleSlot moves an owned descriptor (the owned
        // model passes a copy where C shares the pointer).
        match execTuples::exec_get_result_type::call(subplan.ps_head()) {
            Some(td) => Some(mcx::alloc_in(mcx, td.clone_in(mcx)?)?),
            None => None,
        }
    };
    let result_slot_ops = {
        let subplan = subquerystate
            .subplan
            .as_deref()
            .expect("ExecInitSubqueryScan: subplan just initialized");
        let (ops, _isfixed) =
            execUtils::exec_get_result_slot_ops_isfixed::call(subplan.ps_head(), estate);
        ops
    };
    execTuples::exec_init_scan_tuple_slot::call(
        estate,
        &mut subquerystate.ss,
        result_type,
        result_slot_ops,
    )?;

    // The slot used as the scantuple isn't the slot above (outside of EPQ), but
    // the one from the node below.
    //   subquerystate->ss.ps.scanopsset = true;
    //   subquerystate->ss.ps.scanops =
    //       ExecGetResultSlotOps(subquerystate->subplan,
    //                            &subquerystate->ss.ps.scanopsfixed);
    //   subquerystate->ss.ps.resultopsset = true;
    //   subquerystate->ss.ps.resultops = subquerystate->ss.ps.scanops;
    //   subquerystate->ss.ps.resultopsfixed = subquerystate->ss.ps.scanopsfixed;
    let (scanops, scanopsfixed) = {
        let subplan = subquerystate
            .subplan
            .as_deref()
            .expect("ExecInitSubqueryScan: subplan just initialized");
        execUtils::exec_get_result_slot_ops_isfixed::call(subplan.ps_head(), estate)
    };
    subquerystate.ss.ps.scanopsset = true;
    subquerystate.ss.ps.scanops = Some(scanops);
    subquerystate.ss.ps.scanopsfixed = scanopsfixed;
    subquerystate.ss.ps.resultopsset = true;
    subquerystate.ss.ps.resultops = Some(scanops);
    subquerystate.ss.ps.resultopsfixed = subquerystate.ss.ps.scanopsfixed;

    // Initialize result type and projection.
    //   ExecInitResultTypeTL(&subquerystate->ss.ps);
    //   ExecAssignScanProjectionInfo(&subquerystate->ss);
    execTuples::exec_init_result_type_tl::call(&mut subquerystate.ss.ps, estate)?;
    execScan::exec_assign_scan_projection_info::call(&mut subquerystate.ss, estate)?;

    // initialize child expressions
    //   subquerystate->ss.ps.qual =
    //       ExecInitQual(node->scan.plan.qual, (PlanState *) subquerystate);
    let qual_list = node.scan.plan.qual.as_deref();
    subquerystate.ss.ps.qual =
        execExpr::exec_init_qual::call(qual_list, &mut subquerystate.ss.ps, estate)?;

    Ok(subquerystate)
}

/// `ExecEndSubqueryScan(node)` — free any storage allocated through C routines
/// by closing down the subquery.
pub fn ExecEndSubqueryScan<'mcx>(
    node: &mut SubqueryScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // close down subquery
    //   ExecEndNode(node->subplan);
    if let Some(subplan) = node.subplan.as_deref_mut() {
        execProcnode::exec_end_node::call(subplan, estate)?;
    }
    Ok(())
}

/// `ExecReScanSubqueryScan(node)` — rescan the relation.
pub fn ExecReScanSubqueryScan<'mcx>(
    node: &mut SubqueryScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    //   ExecScanReScan(&node->ss);
    execScan::exec_scan_rescan_ss::call(&mut node.ss, estate)?;

    // ExecReScan doesn't know about my subplan, so I have to do
    // changed-parameter signaling myself.  This is just as well, because the
    // subplan has its own memory context in which its chgParam state lives.
    //   if (node->ss.ps.chgParam != NULL)
    //       UpdateChangedParamSet(node->subplan, node->ss.ps.chgParam);
    //
    // The parent's chgParam is taken out for the call so the child can be
    // borrowed mutably alongside it (it is read-only here; the C passes it as a
    // const set). The C never clears it, so it is restored afterward.
    let chg_param = node.ss.ps.chgParam.take();
    if let Some(chg_param) = chg_param.as_deref() {
        let subplan = node
            .subplan
            .as_deref_mut()
            .expect("ExecReScanSubqueryScan: node->subplan not initialized");
        execUtils::update_changed_param_set::call(mcx, subplan.ps_head_mut(), chg_param)?;
    }
    node.ss.ps.chgParam = chg_param;

    // if chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    //   if (node->subplan->chgParam == NULL)
    //       ExecReScan(node->subplan);
    let subplan = node
        .subplan
        .as_deref_mut()
        .expect("ExecReScanSubqueryScan: node->subplan not initialized");
    if subplan.ps_head().chgParam.is_none() {
        execAmi::exec_re_scan::call(subplan, estate)?;
    }

    Ok(())
}
