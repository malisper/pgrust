//! Port of `src/backend/executor/nodeNestloop.c` — routines to support
//! nest-loop joins.
//!
//! INTERFACE ROUTINES
//! - [`ExecNestLoop`]       - process a nestloop join of two plans.
//! - [`ExecInitNestLoop`]   - initialize the join.
//! - [`ExecEndNestLoop`]    - shut down the join.
//! - [`ExecReScanNestLoop`] - rescan the join.
//!
//! A nest-loop join scans the inner relation once per outer tuple. The
//! `nl_NeedNewOuter` / `nl_MatchedOuter` driver loop, the `nestParams`
//! PARAM_EXEC plumbing, the single-match / antijoin / left-join special cases,
//! the per-tuple econtext reset, and the filtered-tuple instrumentation are
//! this crate's owned logic. Operations below the executor-node layer go
//! through the owners' seam crates:
//!
//! - interrupt servicing (`CHECK_FOR_INTERRUPTS`) → tcop/postgres;
//! - child dispatch / init / teardown / rescan
//!   (`ExecProcNode` / `ExecInitNode` / `ExecEndNode` / `ExecReScan`)
//!   → execProcnode / execAmi;
//! - expression compilation and evaluation (`ExecInitQual` / `ExecQual` /
//!   `ExecProject`) → execExpr;
//! - econtext / result-slot / projection setup (`ExecAssignExprContext` /
//!   `ExecAssignProjectionInfo` / `ExecInitResultTupleSlotTL` /
//!   `ExecInitNullTupleSlot` / `ExecGetResultType`) → execUtils / execTuples;
//! - outer-Var deforming (`slot_getattr`) → execTuples;
//! - the changed-param bitmap (`bms_add_member`) → nodes/bitmapset.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use execAmi_seams as execAmi;
use execExpr_seams as execExpr;
use execProcnode_seams as execProcnode;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use nodes_core_seams as nodes_core;
use postgres_seams as tcop_postgres;

use mcx::{alloc_in, PgBox};
use types_error::{PgError, PgResult};
use nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use nodes::jointype::{JOIN_ANTI, JOIN_INNER, JOIN_LEFT, JOIN_SEMI};
use nodes::nodenestloop::{NestLoop, NestLoopStateData};
use nodes::{EStateData, PlanStateNode, SlotId, TupleSlotKind};

/// Plain `elog(ERROR, ...)` for the "should not happen" internal diagnostics.
fn elog(message: alloc::string::String) -> PgError {
    PgError::error(message)
}

/// Install this crate's implementations into its seam slots. nodeNestloop has
/// no `<unit>-seams` crate: its functions are reached through the executor
/// dispatch (execProcnode), which can depend on this crate directly without a
/// cycle.
pub fn init_seams() {}

/// `TupIsNull(slot)` — true if `slot` is NULL or marked empty (`TTS_EMPTY`).
/// The slot is an id into `estate.es_tupleTable`.
#[inline]
fn tup_is_null(slot: Option<SlotId>, estate: &EStateData<'_>) -> bool {
    match slot {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `InstrCountFiltered1(node, 1)` (execnodes.h) — bump the join-qual-filtered
/// counter when instrumentation is active.
#[inline]
fn instr_count_filtered1(node: &mut NestLoopStateData<'_>) {
    if let Some(instr) = node.js.ps.instrument.as_deref_mut() {
        instr.nfiltered1 += 1.0;
    }
}

/// `InstrCountFiltered2(node, 1)` (execnodes.h) — bump the other-qual-filtered
/// counter.
#[inline]
fn instr_count_filtered2(node: &mut NestLoopStateData<'_>) {
    if let Some(instr) = node.js.ps.instrument.as_deref_mut() {
        instr.nfiltered2 += 1.0;
    }
}

// ===========================================================================
// ExecNestLoop
// ===========================================================================

/// `ExecNestLoop(pstate)` — the `PlanState.ExecProcNode` callback. Returns the
/// `SlotId` of the tuple joined from inner and outer tuples that satisfies the
/// qualification clause (C's returned `TupleTableSlot *`), or `Ok(None)` (C
/// `NULL`) when the join is exhausted.
pub fn ExecNestLoop<'mcx>(
    node: &mut NestLoopStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    // get information from the node
    //   nl = (NestLoop *) node->js.ps.plan;
    //   joinqual = node->js.joinqual;
    //   otherqual = node->js.ps.qual;
    //   econtext = node->js.ps.ps_ExprContext;
    let jointype = node.js.jointype;
    let single_match = node.js.single_match;
    let econtext = node
        .js
        .ps
        .ps_ExprContext
        .expect("ExecNestLoop: ps_ExprContext not created");

    // Reset per-tuple memory context to free any expression evaluation storage
    // allocated in the previous tuple cycle. ResetExprContext(econtext).
    estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();

    // Ok, everything is setup for the join so now loop until we return a
    // qualifying join tuple.
    loop {
        // If we don't have an outer tuple, get the next one and reset the inner
        // scan.
        if node.nl_NeedNewOuter {
            //   outerTupleSlot = ExecProcNode(outerPlan);
            let outer_tuple_slot = exec_outer(node, estate)?;

            // if there are no more outer tuples, then the join is complete..
            //   if (TupIsNull(outerTupleSlot)) return NULL;
            if tup_is_null(outer_tuple_slot, estate) {
                return Ok(None);
            }

            // saving new outer tuple information
            //   econtext->ecxt_outertuple = outerTupleSlot;
            //   node->nl_NeedNewOuter = false;
            //   node->nl_MatchedOuter = false;
            estate.ecxt_mut(econtext).ecxt_outertuple = outer_tuple_slot;
            node.nl_NeedNewOuter = false;
            node.nl_MatchedOuter = false;

            // fetch the values of any outer Vars that must be passed to the
            // inner scan, and store them in the appropriate PARAM_EXEC slots.
            //   foreach(lc, nl->nestParams) { ... }
            //
            // `nl->nestParams` is read off the sibling-owned `NestLoop` plan
            // node reached through the `ps.plan` back-link; the C casts
            // `node->js.ps.plan` to `NestLoop *` and walks `nestParams`. The
            // outer slot value just fetched is the source. Collect the
            // (paramno, varattno) pairs first so we can borrow the slot and the
            // EState param array without aliasing the plan reference.
            let nest_params: alloc::vec::Vec<(i32, i16)> = {
                let nl = nestloop_plan(node);
                nl.nestParams
                    .iter()
                    .map(|nlp| {
                        // Assert(IsA(nlp->paramval, Var));
                        // Assert(nlp->paramval->varno == OUTER_VAR);
                        // Assert(nlp->paramval->varattno > 0);
                        // By execution time set_join_references has reduced
                        // paramval to a simple OUTER_VAR Var.
                        let var = match &nlp.paramval {
                            nodes::primnodes::Expr::Var(v) => v,
                            _ => panic!("NestLoopParam paramval is not a Var at execution"),
                        };
                        debug_assert!(var.varattno > 0, "nlp->paramval->varattno > 0");
                        (nlp.paramno, var.varattno)
                    })
                    .collect()
            };
            for (paramno, varattno) in nest_params {
                //   prm = &(econtext->ecxt_param_exec_vals[paramno]);
                //   prm->value = slot_getattr(outerTupleSlot,
                //                             nlp->paramval->varattno,
                //                             &(prm->isnull));
                let outer_id = estate
                    .ecxt(econtext)
                    .ecxt_outertuple
                    .expect("nest-loop outer tuple slot is missing");
                let (value, isnull) =
                    execTuples::slot_getattr::call(estate, outer_id, varattno)?;
                let prm = &mut estate.es_param_exec_vals[paramno as usize];
                // `slot_getattr` now yields the canonical unified value, stored
                // directly into `ParamExecData.value`
                // (`prm->value = slot_getattr(...)`).
                prm.value = value;
                prm.isnull = isnull;

                // Flag parameter value as changed.
                //   innerPlan->chgParam = bms_add_member(innerPlan->chgParam,
                //                                        paramno);
                let inner = node
                    .js
                    .ps
                    .righttree
                    .as_deref_mut()
                    .expect("ExecNestLoop: no inner plan state")
                    .ps_head_mut();
                let mcx = estate.es_query_cxt;
                let chg = inner.chgParam.take();
                inner.chgParam = Some(nodes_core::bms_add_member::call(mcx, chg, paramno)?);
            }

            // now rescan the inner plan
            //   ExecReScan(innerPlan);
            let inner = node
                .js
                .ps
                .righttree
                .as_deref_mut()
                .expect("ExecNestLoop: no inner plan state");
            execAmi::exec_re_scan::call(inner, estate)?;
        }

        // we have an outerTuple, try to get the next inner tuple.
        //   innerTupleSlot = ExecProcNode(innerPlan);
        //   econtext->ecxt_innertuple = innerTupleSlot;
        let inner_tuple_slot = exec_inner(node, estate)?;
        estate.ecxt_mut(econtext).ecxt_innertuple = inner_tuple_slot;

        if tup_is_null(inner_tuple_slot, estate) {
            // no inner tuple, need new outer tuple
            //   node->nl_NeedNewOuter = true;
            node.nl_NeedNewOuter = true;

            //   if (!node->nl_MatchedOuter &&
            //       (node->js.jointype == JOIN_LEFT ||
            //        node->js.jointype == JOIN_ANTI))
            if !node.nl_MatchedOuter && (jointype == JOIN_LEFT || jointype == JOIN_ANTI) {
                // We are doing an outer join and there were no join matches for
                // this outer tuple. Generate a fake join tuple with nulls for
                // the inner tuple, and return it if it passes the non-join
                // quals.
                //   econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;
                let null_inner = node
                    .nl_NullInnerTupleSlot
                    .expect("nest-loop null inner tuple slot is missing");
                estate.ecxt_mut(econtext).ecxt_innertuple = Some(null_inner);

                // testing qualification for outer-join tuple
                //   if (otherqual == NULL || ExecQual(otherqual, econtext))
                if exec_otherqual(node, estate)? {
                    // qualification was satisfied so we project and return the
                    // slot containing the result tuple using ExecProject().
                    //   return ExecProject(node->js.ps.ps_ProjInfo);
                    return Ok(Some(execExpr::exec_project::call(&mut node.js.ps, estate)?));
                } else {
                    //   InstrCountFiltered2(node, 1);
                    instr_count_filtered2(node);
                }
            }

            // Otherwise just return to top of loop for a new outer tuple.
            continue;
        }

        // at this point we have a new pair of inner and outer tuples so we test
        // the inner and outer tuples to see if they satisfy the node's
        // qualification.
        //
        // Only the joinquals determine MatchedOuter status, but all quals must
        // pass to actually return the tuple.
        //   if (ExecQual(joinqual, econtext))
        if exec_joinqual(node, estate)? {
            //   node->nl_MatchedOuter = true;
            node.nl_MatchedOuter = true;

            // In an antijoin, we never return a matched tuple.
            //   if (node->js.jointype == JOIN_ANTI) {
            //       node->nl_NeedNewOuter = true; continue; }
            if jointype == JOIN_ANTI {
                node.nl_NeedNewOuter = true;
                continue; // return to top of loop
            }

            // If we only need to join to the first matching inner tuple, then
            // consider returning this one, but after that continue with next
            // outer tuple.
            //   if (node->js.single_match) node->nl_NeedNewOuter = true;
            if single_match {
                node.nl_NeedNewOuter = true;
            }

            //   if (otherqual == NULL || ExecQual(otherqual, econtext))
            if exec_otherqual(node, estate)? {
                // qualification was satisfied so we project and return the slot
                // containing the result tuple using ExecProject().
                //   return ExecProject(node->js.ps.ps_ProjInfo);
                return Ok(Some(execExpr::exec_project::call(&mut node.js.ps, estate)?));
            } else {
                //   InstrCountFiltered2(node, 1);
                instr_count_filtered2(node);
            }
        } else {
            //   InstrCountFiltered1(node, 1);
            instr_count_filtered1(node);
        }

        // Tuple fails qual, so free per-tuple memory and try again.
        //   ResetExprContext(econtext);
        estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
    }
}

/// `(NestLoop *) node->js.ps.plan` — the sibling-owned `NestLoop` plan node the
/// state node aliases via its `ps.plan` back-link.
fn nestloop_plan<'a, 'mcx>(node: &'a NestLoopStateData<'mcx>) -> &'a NestLoop<'mcx> {
    match node.js.ps.plan {
        Some(p) => p.expect_nestloop(),
        None => panic!("castNode(NestLoop, node->js.ps.plan) failed: node->js.ps.plan is NULL"),
    }
}

/// `ExecProcNode(outerPlanState(node))` — fetch the next outer tuple.
fn exec_outer<'mcx>(
    node: &mut NestLoopStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let outer = node
        .js
        .ps
        .lefttree
        .as_deref_mut()
        .expect("ExecNestLoop: no outer plan state");
    execProcnode::exec_proc_node::call(outer, estate)
}

/// `ExecProcNode(innerPlanState(node))` — fetch the next inner tuple.
fn exec_inner<'mcx>(
    node: &mut NestLoopStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let inner = node
        .js
        .ps
        .righttree
        .as_deref_mut()
        .expect("ExecNestLoop: no inner plan state");
    execProcnode::exec_proc_node::call(inner, estate)
}

/// `ExecQual(node->js.joinqual, econtext)` with the C short-circuit: a `NULL`
/// joinqual (`ExecInitQual(NIL)` → NULL) evaluates to `true`.
fn exec_joinqual<'mcx>(
    node: &mut NestLoopStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let econtext = node
        .js
        .ps
        .ps_ExprContext
        .expect("exec_joinqual: ps_ExprContext not created");
    match node.js.joinqual.as_deref_mut() {
        Some(state) => execExpr::exec_qual::call(state, econtext, estate),
        None => Ok(true),
    }
}

/// `otherqual == NULL || ExecQual(otherqual, econtext)` — the non-join quals
/// (`node->js.ps.qual`), with the C short-circuit when there are none.
fn exec_otherqual<'mcx>(
    node: &mut NestLoopStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let econtext = node
        .js
        .ps
        .ps_ExprContext
        .expect("exec_otherqual: ps_ExprContext not created");
    match node.js.ps.qual.as_deref_mut() {
        Some(state) => execExpr::exec_qual::call(state, econtext, estate),
        None => Ok(true),
    }
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitNestLoop`]:
/// `castNode(NestLoopState, pstate)` then run [`ExecNestLoop`].
fn exec_nestloop_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::NestLoop(node) => node,
        other => panic!("castNode(NestLoopState, pstate) failed: {other:?}"),
    };
    ExecNestLoop(node, estate)
}

// ===========================================================================
// ExecInitNestLoop
// ===========================================================================

/// `ExecInitNestLoop(node, estate, eflags)` — create and initialize the
/// nest-loop run-time state.
pub fn ExecInitNestLoop<'mcx>(
    plan_node: &'mcx nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut eflags: i32,
) -> PgResult<PgBox<'mcx, NestLoopStateData<'mcx>>> {
    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    let mcx = estate.es_query_cxt;

    let node: &'mcx NestLoop<'mcx> = plan_node.expect_nestloop();

    // create state structure: makeNode(NestLoopState)
    //   nlstate->js.ps.plan = (Plan *) node;
    //   nlstate->js.ps.state = estate;
    //   nlstate->js.ps.ExecProcNode = ExecNestLoop;
    // The NodeTag is carried by the `PlanStateNode::NestLoop` variant, so
    // `makeNode(NestLoopState)` is just the zeroed state with its back-links set.
    let mut nlstate = alloc_in(mcx, NestLoopStateData::default())?;
    nlstate.js.ps.plan = Some(plan_node);
    nlstate.js.ps.ExecProcNode = Some(exec_nestloop_node);

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &nlstate->js.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut nlstate.js.ps)?;

    // initialize child nodes
    //
    // If we have no parameters to pass into the inner rel from the outer, tell
    // the inner child that cheap rescans would be good. If we do have such
    // parameters, then there is no point in REWIND support at all in the inner
    // child, because it will always be rescanned with fresh parameter values.
    //   outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
    let outer_plan = node.join.plan.lefttree.as_deref();
    nlstate.js.ps.lefttree =
        execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;

    //   if (node->nestParams == NIL) eflags |= EXEC_FLAG_REWIND;
    //   else eflags &= ~EXEC_FLAG_REWIND;
    if node.nestParams.is_empty() {
        eflags |= EXEC_FLAG_REWIND;
    } else {
        eflags &= !EXEC_FLAG_REWIND;
    }

    //   innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);
    let inner_plan = node.join.plan.righttree.as_deref();
    nlstate.js.ps.righttree =
        execProcnode::exec_init_node::call(mcx, inner_plan, estate, eflags)?;

    // Initialize result slot, type and projection.
    //   ExecInitResultTupleSlotTL(&nlstate->js.ps, &TTSOpsVirtual);
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut nlstate.js.ps,
        estate,
        TupleSlotKind::Virtual,
    )?;
    //   ExecAssignProjectionInfo(&nlstate->js.ps, NULL);
    execUtils::exec_assign_projection_info::call(&mut nlstate.js.ps, estate, None)?;

    // initialize child expressions
    //   nlstate->js.ps.qual = ExecInitQual(node->join.plan.qual, nlstate);
    let qual_list = node.join.plan.qual.as_deref();
    nlstate.js.ps.qual =
        execExpr::exec_init_qual::call(qual_list, &mut nlstate.js.ps, estate)?;
    //   nlstate->js.jointype = node->join.jointype;
    nlstate.js.jointype = node.join.jointype;
    //   nlstate->js.joinqual = ExecInitQual(node->join.joinqual, nlstate);
    let joinqual_list = node.join.joinqual.as_deref();
    nlstate.js.joinqual =
        execExpr::exec_init_qual::call(joinqual_list, &mut nlstate.js.ps, estate)?;

    // detect whether we need only consider the first matching inner tuple
    //   nlstate->js.single_match = (node->join.inner_unique ||
    //                               node->join.jointype == JOIN_SEMI);
    nlstate.js.single_match = node.join.inner_unique || node.join.jointype == JOIN_SEMI;

    // set up null tuples for outer joins, if needed
    match node.join.jointype {
        JOIN_INNER | JOIN_SEMI => {}
        JOIN_LEFT | JOIN_ANTI => {
            //   nlstate->nl_NullInnerTupleSlot =
            //       ExecInitNullTupleSlot(estate,
            //           ExecGetResultType(innerPlanState(nlstate)),
            //           &TTSOpsVirtual);
            let inner_desc = {
                let inner = nlstate
                    .js
                    .ps
                    .righttree
                    .as_deref()
                    .expect("ExecInitNestLoop: ExecInitNode(inner) returned None");
                match execTuples::exec_get_result_type::call(inner.ps_head()) {
                    Some(d) => Some(alloc_in(mcx, d.clone_in(mcx)?)?),
                    None => None,
                }
            };
            nlstate.nl_NullInnerTupleSlot = Some(execTuples::exec_init_null_tuple_slot::call(
                estate,
                inner_desc,
                TupleSlotKind::Virtual,
            )?);
        }
        other => {
            //   elog(ERROR, "unrecognized join type: %d",
            //        (int) node->join.jointype);
            return Err(elog(alloc::format!(
                "unrecognized join type: {}",
                other as u32
            )));
        }
    }

    // finally, wipe the current outer tuple clean.
    //   nlstate->nl_NeedNewOuter = true;
    //   nlstate->nl_MatchedOuter = false;
    nlstate.nl_NeedNewOuter = true;
    nlstate.nl_MatchedOuter = false;

    Ok(nlstate)
}

// ===========================================================================
// ExecEndNestLoop
// ===========================================================================

/// `ExecEndNestLoop(node)` — closes down scans and frees allocated storage.
pub fn ExecEndNestLoop<'mcx>(
    node: &mut NestLoopStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // close down subplans
    //   ExecEndNode(outerPlanState(node));
    if let Some(outer) = node.js.ps.lefttree.as_deref_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }
    //   ExecEndNode(innerPlanState(node));
    if let Some(inner) = node.js.ps.righttree.as_deref_mut() {
        execProcnode::exec_end_node::call(inner, estate)?;
    }
    Ok(())
}

// ===========================================================================
// ExecReScanNestLoop
// ===========================================================================

/// `ExecReScanNestLoop(node)` — rescan the node.
pub fn ExecReScanNestLoop<'mcx>(
    node: &mut NestLoopStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // PlanState *outerPlan = outerPlanState(node);

    // If outerPlan->chgParam is not null then plan will be automatically
    // re-scanned by first ExecProcNode.
    //   if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    let outer_chg_null = node
        .js
        .ps
        .lefttree
        .as_deref()
        .map(|p| p.ps_head().chgParam.is_none())
        .unwrap_or(true);
    if outer_chg_null {
        if let Some(outer) = node.js.ps.lefttree.as_deref_mut() {
            execAmi::exec_re_scan::call(outer, estate)?;
        }
    }

    // innerPlan is re-scanned for each new outer tuple and MUST NOT be
    // re-scanned from here or you'll get troubles from inner index scans when
    // outer Vars are used as run-time keys...

    //   node->nl_NeedNewOuter = true;
    //   node->nl_MatchedOuter = false;
    node.nl_NeedNewOuter = true;
    node.nl_MatchedOuter = false;
    Ok(())
}

extern crate alloc;

#[cfg(test)]
mod tests;
