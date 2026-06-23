//! Port of `src/backend/executor/nodeGroup.c` — routines to handle group nodes
//! (used for queries with a `GROUP BY` clause, without aggregates).
//!
//! The Group node is designed for handling queries with a `GROUP BY` clause.
//! Its outer plan must deliver tuples that are sorted in the order specified by
//! the grouping columns (ie. tuples from the same group are consecutive). That
//! way, the node just has to compare adjacent tuples to locate group
//! boundaries, returning one projected tuple per group that passes the `HAVING`
//! qual.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitGroup`]   - initialize node and outer subtree
//! - [`ExecGroup`]       - return one tuple per group of matching input tuples
//! - [`ExecEndGroup`]    - shut down node and outer subtree
//! - [`ExecReScanGroup`] - rescan the node
//!
//! Following the nodeResult / nodeLimit owned-tree model, the C
//! `PlanState.state` back-pointer is replaced by threading `&mut EStateData`
//! explicitly, and `ExecGroup` returns the produced result-slot id (the C
//! `return ExecProject(...)`) or `None` (the C `return NULL`).
//!
//! Calls into unported owners (execProcnode.c's `ExecInitNode`/`ExecProcNode`/
//! `ExecEndNode`, execAmi.c's `ExecReScan`, execUtils.c's
//! `ExecAssignExprContext`/`ExecAssignProjectionInfo`/
//! `ExecCreateScanSlotFromOuterPlan`/`ExecGetResultSlotOps`, execTuples.c's
//! `ExecInitResultTupleSlotTL`/`ExecCopySlot`/`ExecClearTuple`/
//! `ExecGetResultType`, execExpr.c's `ExecInitQual`/`ExecQual`/
//! `ExecQualAndReset`/`ExecProject`, execGrouping.c's `execTuplesMatchPrepare`,
//! tcop/postgres.c's `CHECK_FOR_INTERRUPTS`) go through those owners' seam
//! crates and panic until the owners land.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use execAmi_seams as execAmi;
use execExpr_seams as execExpr;
use execGrouping_seams as execGrouping;
use execProcnode_seams as execProcnode;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use postgres_seams as tcop_postgres;
use mcx::{alloc_in, PgBox};
use types_error::PgResult;
use nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, TupleSlotKind};
use nodes::nodegroup::{Group, GroupStateData};
use nodes::{EStateData, PlanStateNode, SlotId};

/// Install this crate's implementations into its seam slots.
///
/// nodeGroup has no `<unit>-seams` crate: callers that will need these
/// functions (execProcnode's dispatch tables) can depend on this crate
/// directly without a cycle, since this crate reaches outward only through
/// per-owner seam crates.
pub fn init_seams() {}

/// `InstrCountFiltered1(node, delta)` (execnodes.h):
///
/// ```c
/// do {
///     if (((PlanState *)(node))->instrument)
///         ((PlanState *)(node))->instrument->nfiltered1 += (delta);
/// } while (0)
/// ```
#[inline]
fn instr_count_filtered1(node: &mut GroupStateData, delta: f64) {
    if let Some(instrument) = node.ss.ps.instrument.as_mut() {
        instrument.nfiltered1 += delta;
    }
}

/// `ExecGroup(pstate)` — the `PlanState.ExecProcNode` callback.
///
/// Return one tuple for each group of matching input tuples. Ported 1:1 from
/// `nodeGroup.c` `ExecGroup`. Returns `Ok(Some(slot))` when a tuple is
/// available in the node's result slot (the C `return ExecProject(...)`) and
/// `Ok(None)` when the scan is exhausted (the C `return NULL`).
pub fn ExecGroup<'mcx>(
    node: &mut GroupStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    // get state info from node
    //   if (node->grp_done) return NULL;
    if node.grp_done {
        return Ok(None);
    }

    //   econtext = node->ss.ps.ps_ExprContext;
    let econtext = node
        .ss
        .ps
        .ps_ExprContext
        .expect("ExecGroup: ps_ExprContext must be set (ExecAssignExprContext ran in init)");

    // The ScanTupleSlot holds the (copied) first tuple of each group.
    //   firsttupleslot = node->ss.ss_ScanTupleSlot;
    let firsttupleslot = node
        .ss
        .ss_ScanTupleSlot
        .expect("ExecGroup: ss_ScanTupleSlot must be set (ExecCreateScanSlotFromOuterPlan ran)");

    // We need not call ResetExprContext here because ExecQualAndReset() will
    // reset the per-tuple memory context once per input tuple.

    // If first time through, acquire first input tuple and determine whether to
    // return it or not.
    //   if (TupIsNull(firsttupleslot))
    if estate.slot(firsttupleslot).is_empty() {
        //   outerslot = ExecProcNode(outerPlanState(node));
        let outerslot = {
            let outer = node
                .ss
                .ps
                .lefttree
                .as_deref_mut()
                .expect("ExecGroup: outer plan state must be initialized");
            execProcnode::exec_proc_node::call(outer, estate)?
        };
        //   if (TupIsNull(outerslot)) { node->grp_done = true; return NULL; }
        let outerslot = match outerslot {
            Some(id) if !estate.slot(id).is_empty() => id,
            _ => {
                // empty input, so return nothing
                node.grp_done = true;
                return Ok(None);
            }
        };

        // Copy tuple into firsttupleslot
        //   ExecCopySlot(firsttupleslot, outerslot);
        copy_into_first(firsttupleslot, outerslot, estate)?;

        // Set it up as input for qual test and projection. The expressions will
        // access the input tuple as varno OUTER.
        //   econtext->ecxt_outertuple = firsttupleslot;
        estate.ecxt_mut(econtext).ecxt_outertuple = Some(firsttupleslot);

        // Check the qual (HAVING clause); if the group does not match, ignore it
        // and fall into scan loop.
        //   if (ExecQual(node->ss.ps.qual, econtext))
        if exec_qual_having(node, econtext, estate)? {
            // Form and return a projection tuple using the first input tuple.
            //   return ExecProject(node->ss.ps.ps_ProjInfo);
            return Ok(Some(execExpr::exec_project::call(&mut node.ss.ps, estate)?));
        } else {
            //   InstrCountFiltered1(node, 1);
            instr_count_filtered1(node, 1.0);
        }
    }

    // This loop iterates once per input tuple group. At the head of the loop, we
    // have finished processing the first tuple of the group and now need to scan
    // over all the other group members.
    //   for (;;)
    loop {
        // Scan over all remaining tuples that belong to this group.
        //   for (;;)
        let outerslot = loop {
            //   outerslot = ExecProcNode(outerPlanState(node));
            let outerslot = {
                let outer = node
                    .ss
                    .ps
                    .lefttree
                    .as_deref_mut()
                    .expect("ExecGroup: outer plan state must be initialized");
                execProcnode::exec_proc_node::call(outer, estate)?
            };
            //   if (TupIsNull(outerslot)) { node->grp_done = true; return NULL; }
            let outerslot = match outerslot {
                Some(id) if !estate.slot(id).is_empty() => id,
                _ => {
                    // no more groups, so we're done
                    node.grp_done = true;
                    return Ok(None);
                }
            };

            // Compare with first tuple and see if this tuple is of the same
            // group. If so, ignore it and keep scanning.
            //   econtext->ecxt_innertuple = firsttupleslot;
            //   econtext->ecxt_outertuple = outerslot;
            {
                let ec = estate.ecxt_mut(econtext);
                ec.ecxt_innertuple = Some(firsttupleslot);
                ec.ecxt_outertuple = Some(outerslot);
            }
            //   if (!ExecQualAndReset(node->eqfunction, econtext)) break;
            let eq = node
                .eqfunction
                .as_deref_mut()
                .expect("ExecGroup: eqfunction must be set (execTuplesMatchPrepare ran in init)");
            if !execExpr::exec_qual_and_reset::call(eq, econtext, estate)? {
                break outerslot;
            }
        };

        // We have the first tuple of the next input group. See if we want to
        // return it.
        // Copy tuple, set up as input for qual test and projection
        //   ExecCopySlot(firsttupleslot, outerslot);
        copy_into_first(firsttupleslot, outerslot, estate)?;
        //   econtext->ecxt_outertuple = firsttupleslot;
        estate.ecxt_mut(econtext).ecxt_outertuple = Some(firsttupleslot);

        // Check the qual (HAVING clause); if the group does not match, ignore it
        // and loop back to scan the rest of the group.
        //   if (ExecQual(node->ss.ps.qual, econtext))
        if exec_qual_having(node, econtext, estate)? {
            // Form and return a projection tuple using the first input tuple.
            //   return ExecProject(node->ss.ps.ps_ProjInfo);
            return Ok(Some(execExpr::exec_project::call(&mut node.ss.ps, estate)?));
        } else {
            //   InstrCountFiltered1(node, 1);
            instr_count_filtered1(node, 1.0);
        }
    }
}

/// `ExecQual(node->ss.ps.qual, econtext)` — evaluate the HAVING qual. A `NULL`
/// `ExprState` (the C `qual == NULL`) is always-true, handled here in-crate;
/// otherwise it goes through the execExpr seam.
fn exec_qual_having<'mcx>(
    node: &mut GroupStateData<'mcx>,
    econtext: nodes::EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    match node.ss.ps.qual.as_deref_mut() {
        None => Ok(true),
        Some(qual) => execExpr::exec_qual::call(qual, econtext, estate),
    }
}

/// `ExecCopySlot(firsttupleslot, outerslot)` — copy the outer plan's returned
/// tuple into the node's scan tuple slot.
fn copy_into_first<'mcx>(
    dst: SlotId,
    src: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
        execTuples::exec_copy_slot::call(estate, dst, src)
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitGroup`]:
/// `castNode(GroupState, pstate)` then run [`ExecGroup`].
fn exec_group_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::Group(node) => node,
        other => panic!("castNode(GroupState, pstate) failed: {other:?}"),
    };
    ExecGroup(node, estate)
}

/// `ExecInitGroup(node, estate, eflags)` — create the run-time information for
/// the group node produced by the planner and initialize its outer subtree.
///
/// The state tree is allocated in `estate.es_query_cxt` (C: `makeNode` in the
/// per-query context current during `ExecInitNode`), so initialization is
/// fallible on OOM. Panics if the node is not a `Group` (the C `castNode`).
pub fn ExecInitGroup<'mcx>(
    node: &'mcx nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, GroupStateData<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let group: &'mcx Group<'mcx> = node.expect_group();

    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    // create state structure
    //   grpstate = makeNode(GroupState);
    //   grpstate->ss.ps.plan = (Plan *) node;
    //   grpstate->ss.ps.state = estate;
    //   grpstate->ss.ps.ExecProcNode = ExecGroup;
    //   grpstate->grp_done = false;
    let mut grpstate = alloc_in(mcx, GroupStateData::default())?;
    grpstate.ss.ps.plan = Some(node);
    grpstate.ss.ps.ExecProcNode = Some(exec_group_node);
    grpstate.grp_done = false;

    // create expression context
    //   ExecAssignExprContext(estate, &grpstate->ss.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut grpstate.ss.ps)?;

    // initialize child nodes
    //   outerPlanState(grpstate) = ExecInitNode(outerPlan(node), estate, eflags);
    let outer_plan = group.plan.lefttree.as_deref();
    grpstate.ss.ps.lefttree = execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;

    // Initialize scan slot and type.
    //   tts_ops = ExecGetResultSlotOps(outerPlanState(&grpstate->ss), NULL);
    //   ExecCreateScanSlotFromOuterPlan(estate, &grpstate->ss, tts_ops);
    let tts_ops = {
        let outer = grpstate
            .ss
            .ps
            .lefttree
            .as_deref()
            .expect("ExecInitGroup: outer plan state must be initialized");
        execTuples::exec_get_result_slot_ops::call(outer.ps_head())
    };
    execUtils::exec_create_scan_slot_from_outer_plan::call(estate, &mut grpstate.ss, tts_ops)?;

    // Initialize result slot, type and projection.
    //   ExecInitResultTupleSlotTL(&grpstate->ss.ps, &TTSOpsVirtual);
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut grpstate.ss.ps,
        estate,
        TupleSlotKind::Virtual,
    )?;
    //   ExecAssignProjectionInfo(&grpstate->ss.ps, NULL);
    execUtils::exec_assign_projection_info::call(&mut grpstate.ss.ps, estate, None)?;

    // initialize child expressions
    //   grpstate->ss.ps.qual = ExecInitQual(node->plan.qual, (PlanState *) grpstate);
    let plan_qual = group.plan.qual.as_deref();
    grpstate.ss.ps.qual = execExpr::exec_init_qual::call(plan_qual, &mut grpstate.ss.ps, estate)?;

    // Precompute fmgr lookup data for inner loop
    //   grpstate->eqfunction =
    //       execTuplesMatchPrepare(ExecGetResultType(outerPlanState(grpstate)),
    //                              node->numCols, node->grpColIdx,
    //                              node->grpOperators, node->grpCollations,
    //                              &grpstate->ss.ps);
    let desc = {
        let outer = grpstate
            .ss
            .ps
            .lefttree
            .as_deref()
            .expect("ExecInitGroup: outer plan state must be initialized");
        let head = outer.ps_head();
        alloc_in(
            mcx,
            execTuples::exec_get_result_type::call(head)
                .expect("ExecInitGroup: outer plan result type must be set")
                .clone_in(mcx)?,
        )?
    };
    grpstate.eqfunction = execGrouping::exec_tuples_match_prepare::call(
        Some(desc),
        group.numCols,
        &group.grpColIdx,
        &group.grpOperators,
        &group.grpCollations,
        &mut grpstate.ss.ps,
        estate,
    )?;

    Ok(grpstate)
}

/// `ExecEndGroup(node)` — shut down the group node by closing the outer
/// subtree.
///
/// ```c
/// void
/// ExecEndGroup(GroupState *node)
/// {
///     PlanState  *outerPlan;
///     outerPlan = outerPlanState(node);
///     ExecEndNode(outerPlan);
/// }
/// ```
pub fn ExecEndGroup<'mcx>(
    node: &mut GroupStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    //   outerPlan = outerPlanState(node);
    //   ExecEndNode(outerPlan);
    //
    // The C calls ExecEndNode unconditionally; ExecEndNode(NULL) is a no-op.
    if let Some(outer) = node.ss.ps.lefttree.as_deref_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }
    Ok(())
}

/// `ExecReScanGroup(node)` — reset the group node for a rescan.
///
/// ```c
/// void
/// ExecReScanGroup(GroupState *node)
/// {
///     PlanState  *outerPlan = outerPlanState(node);
///     node->grp_done = false;
///     ExecClearTuple(node->ss.ss_ScanTupleSlot);
///     if (outerPlan->chgParam == NULL)
///         ExecReScan(outerPlan);
/// }
/// ```
pub fn ExecReScanGroup<'mcx>(
    node: &mut GroupStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    //   node->grp_done = false;
    node.grp_done = false;

    // must clear first tuple
    //   ExecClearTuple(node->ss.ss_ScanTupleSlot);
    if let Some(scan_slot) = node.ss.ss_ScanTupleSlot {
        execTuples::exec_clear_tuple::call(estate, scan_slot)?;
    }

    // if chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    //   if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    if let Some(outer) = node.ss.ps.lefttree.as_deref_mut() {
        if outer.ps_head().chgParam.is_none() {
            execAmi::exec_re_scan::call(outer, estate)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests;
