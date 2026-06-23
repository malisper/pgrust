//! Port of `src/backend/executor/nodeUnique.c` — routines to handle
//! unique'ing of queries where appropriate.
//!
//! `Unique` is a very simple node type that just filters out duplicate tuples
//! from a stream of *sorted* tuples from its subplan. It's essentially a
//! dumbed-down form of `Group`: the duplicate-removal functionality is
//! identical. However, `Unique` doesn't do projection nor qual checking, so
//! it's marginally more efficient for cases where neither is needed.
//!
//! INTERFACE ROUTINES
//! - [`ExecUnique`]       - generate a unique'd temporary relation
//! - [`ExecInitUnique`]   - initialize node and subnodes
//! - [`ExecEndUnique`]    - shutdown node and subnodes
//! - [`ExecReScanUnique`] - rescan the node
//!
//! Assumes tuples returned from the subplan arrive in sorted order, so
//! duplicates can be detected by comparing each new tuple against the
//! previously returned one. The owned model holds the node as a
//! [`UniqueStateData`] mutated through `&mut`; the C `PlanState.state`
//! back-pointer is replaced by threading `&mut EStateData` explicitly, and
//! the externally-owned result/outer slots are arena [`SlotId`]s into
//! `estate.es_tupleTable` (a C `TupleTableSlot *` that may be NULL or
//! `TTS_EMPTY` becomes `Option<SlotId>` normalized against
//! `TupleTableSlot::is_empty`).
//!
//! Everything below the node layer is reached through per-owner seam crates
//! (execProcnode/execTuples/execUtils/execAmi for child dispatch and slot
//! setup, execExpr for qual eval, execGrouping for the equality `ExprState`,
//! tcop/postgres for `CHECK_FOR_INTERRUPTS`), which panic loudly until those
//! owners install real implementations.

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
use ::nodes::executor::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::nodes::nodeunique::{Unique, UniqueStateData};
use nodes::{EStateData, PlanStateNode, SlotId};

/// Install this crate's seam implementations.
///
/// nodeUnique owns no `*-seams` crate: nodeUnique.c is a leaf executor node
/// whose interface routines (`ExecInitUnique`/`ExecEndUnique`/`ExecReScanUnique`
/// and the `ExecUnique` callback) are dispatched by the executor (execProcnode.c
/// / execAmi.c) through their own per-node-tag switches — those dispatch crates
/// depend on this crate directly and call these functions, which does not cycle
/// (this crate depends only on `*-seams` crates, never on the dispatch crates
/// themselves). There are therefore no inward seams to install, and
/// `init_seams()` is empty (invoked by `seams-init::init_all()`).
pub fn init_seams() {}

// ===========================================================================
// Node state machine (ported 1:1 from nodeUnique.c).
// ===========================================================================

/// `ExecUnique(pstate)` — generate a unique'd temporary relation.
///
/// Loops returning only non-duplicate tuples. Tuples are assumed to arrive in
/// sorted order so duplicates can be detected easily; the first tuple of each
/// group is returned. On success the visible tuple lives in
/// `node.ps.ps_ResultTupleSlot`; returns `Ok(Some(slot))` when a tuple is
/// available there (the C `ExecCopySlot` returned slot) and `Ok(None)` at end
/// of subplan (the C `return NULL`).
///
/// ```c
/// static TupleTableSlot *
/// ExecUnique(PlanState *pstate)
/// {
///     UniqueState *node = castNode(UniqueState, pstate);
///     ExprContext *econtext = node->ps.ps_ExprContext;
///     TupleTableSlot *resultTupleSlot;
///     TupleTableSlot *slot;
///     PlanState  *outerPlan;
///
///     CHECK_FOR_INTERRUPTS();
///
///     outerPlan = outerPlanState(node);
///     resultTupleSlot = node->ps.ps_ResultTupleSlot;
///
///     for (;;)
///     {
///         slot = ExecProcNode(outerPlan);
///         if (TupIsNull(slot))
///         {
///             ExecClearTuple(resultTupleSlot);
///             return NULL;
///         }
///         if (TupIsNull(resultTupleSlot))
///             break;
///         econtext->ecxt_innertuple = slot;
///         econtext->ecxt_outertuple = resultTupleSlot;
///         if (!ExecQualAndReset(node->eqfunction, econtext))
///             break;
///     }
///     return ExecCopySlot(resultTupleSlot, slot);
/// }
/// ```
pub fn ExecUnique<'mcx>(
    node: &mut UniqueStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    // resultTupleSlot = node->ps.ps_ResultTupleSlot;
    let result_slot = node
        .ps
        .ps_ResultTupleSlot
        .expect("ExecUnique: ps_ResultTupleSlot must be set (ExecInitResultTupleSlotTL ran in init)");

    // now loop, returning only non-duplicate tuples. We assume that the tuples
    // arrive in sorted order so we can detect duplicates easily. The first
    // tuple of each group is returned.
    let slot = loop {
        // fetch a tuple from the outer subplan
        //   slot = ExecProcNode(outerPlan);
        let slot = fetch_outer_tuple(node, estate)?;

        // if (TupIsNull(slot)) { ExecClearTuple(resultTupleSlot); return NULL; }
        let slot = match slot {
            None => {
                // end of subplan, so we're done
                execTuples::exec_clear_tuple::call(estate, result_slot)?;
                return Ok(None);
            }
            Some(id) => id,
        };

        // Always return the first tuple from the subplan.
        //   if (TupIsNull(resultTupleSlot)) break;
        if estate.slot(result_slot).is_empty() {
            break slot;
        }

        // Else test if the new tuple and the previously returned tuple match.
        // If so then we loop back and fetch another new tuple from the subplan.
        //   econtext->ecxt_innertuple = slot;
        //   econtext->ecxt_outertuple = resultTupleSlot;
        let econtext = node
            .ps
            .ps_ExprContext
            .expect("ExecUnique: ps_ExprContext must be set (ExecAssignExprContext ran in init)");
        {
            let ec = estate.ecxt_mut(econtext);
            ec.ecxt_innertuple = Some(slot);
            ec.ecxt_outertuple = Some(result_slot);
        }
        //   if (!ExecQualAndReset(node->eqfunction, econtext)) break;
        if !exec_qual_and_reset(node, estate)? {
            break slot;
        }
    };

    // We have a new tuple different from the previous saved tuple (if any).
    // Save it and return it. We must copy it because the source subplan won't
    // guarantee that this source tuple is still accessible after fetching the
    // next source tuple.
    //   return ExecCopySlot(resultTupleSlot, slot);
        execTuples::exec_copy_slot::call(estate, result_slot, slot)?;
    Ok(Some(result_slot))
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitUnique`]:
/// `castNode(UniqueState, pstate)` then run [`ExecUnique`].
fn exec_unique_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::Unique(node) => node,
        other => panic!("castNode(UniqueState, pstate) failed: {other:?}"),
    };
    ExecUnique(node, estate)
}

/// `ExecQualAndReset(node->eqfunction, econtext)` (executor.h) — `ExecQual`
/// followed by `ResetExprContext`. A static inline helper in `executor.h`,
/// ported in-crate; the qual evaluation routes through execExpr.
///
/// `node.eqfunction` is the equality `ExprState`; `ExecQual` returns `true` for
/// a NULL state (a zero-column key compiles to `None`), in which case the qual
/// seam is not consulted but the per-tuple memory is still reset (the C
/// `ResetExprContext` always runs).
///
/// ```c
/// static inline bool
/// ExecQualAndReset(ExprState *state, ExprContext *econtext)
/// {
///     bool ret = ExecQual(state, econtext);
///     ResetExprContext(econtext);
///     return ret;
/// }
/// ```
fn exec_qual_and_reset<'mcx>(
    node: &mut UniqueStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let econtext = node
        .ps
        .ps_ExprContext
        .expect("ExecUnique: ps_ExprContext must be set");

    match node.eqfunction.as_deref_mut() {
        // ExecQualAndReset(state, econtext): evaluate then reset in one call.
        Some(state) => execExpr::exec_qual_and_reset::call(state, econtext, estate),
        // ExecQual(NULL, econtext) is always-true; ResetExprContext still runs.
        None => {
            estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
            Ok(true)
        }
    }
}

/// `slot = ExecProcNode(outerPlan)` + `TupIsNull(slot)` normalization: pull the
/// next tuple from the child subplan, mapping the C `TupIsNull` (NULL pointer
/// OR `TTS_EMPTY` slot) into `None`.
fn fetch_outer_tuple<'mcx>(
    node: &mut UniqueStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let outer = node
        .ps
        .lefttree
        .as_deref_mut()
        .expect("ExecUnique: outerPlanState(node) must be initialized");
    let produced = execProcnode::exec_proc_node::call(outer, estate)?;
    Ok(match produced {
        Some(id) if !estate.slot(id).is_empty() => Some(id),
        _ => None,
    })
}

/// `ExecInitUnique(node, estate, eflags)` — initialize the unique node state
/// structures and the node's subplan.
///
/// The state tree is allocated in `estate.es_query_cxt` (C: `makeNode` in the
/// per-query context current during `ExecInitNode`), so initialization is
/// fallible on OOM. Panics if the node is not a `Unique` (the C `castNode`).
///
/// ```c
/// UniqueState *
/// ExecInitUnique(Unique *node, EState *estate, int eflags)
/// {
///     UniqueState *uniquestate;
///
///     Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
///
///     uniquestate = makeNode(UniqueState);
///     uniquestate->ps.plan = (Plan *) node;
///     uniquestate->ps.state = estate;
///     uniquestate->ps.ExecProcNode = ExecUnique;
///
///     ExecAssignExprContext(estate, &uniquestate->ps);
///
///     outerPlanState(uniquestate) = ExecInitNode(outerPlan(node), estate, eflags);
///
///     ExecInitResultTupleSlotTL(&uniquestate->ps, &TTSOpsMinimalTuple);
///     uniquestate->ps.ps_ProjInfo = NULL;
///
///     uniquestate->eqfunction =
///         execTuplesMatchPrepare(ExecGetResultType(outerPlanState(uniquestate)),
///                                node->numCols, node->uniqColIdx,
///                                node->uniqOperators, node->uniqCollations,
///                                &uniquestate->ps);
///     return uniquestate;
/// }
/// ```
pub fn ExecInitUnique<'mcx>(
    node: &'mcx ::nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, UniqueStateData<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let unique: &'mcx Unique<'mcx> = node.expect_unique();

    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    // create state structure
    //   uniquestate = makeNode(UniqueState);
    //   uniquestate->ps.plan = (Plan *) node;
    //   uniquestate->ps.state = estate;
    //   uniquestate->ps.ExecProcNode = ExecUnique;
    let mut uniquestate = alloc_in(mcx, UniqueStateData::default())?;
    uniquestate.ps.plan = Some(node);
    uniquestate.ps.ExecProcNode = Some(exec_unique_node);

    // create expression context
    //   ExecAssignExprContext(estate, &uniquestate->ps);
    execUtils::exec_assign_expr_context::call(estate, &mut uniquestate.ps)?;

    // then initialize outer plan
    //   outerPlanState(uniquestate) = ExecInitNode(outerPlan(node), estate, eflags);
    let outer_plan = unique.plan.lefttree.as_deref();
    uniquestate.ps.lefttree = execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;

    // Initialize result slot and type. Unique nodes do no projections, so
    // initialize projection info for this node appropriately.
    //   ExecInitResultTupleSlotTL(&uniquestate->ps, &TTSOpsMinimalTuple);
    //   uniquestate->ps.ps_ProjInfo = NULL;
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut uniquestate.ps,
        estate,
        TupleSlotKind::MinimalTuple,
    )?;
    uniquestate.ps.ps_ProjInfo = None;

    // Precompute fmgr lookup data for inner loop.
    //   uniquestate->eqfunction =
    //       execTuplesMatchPrepare(ExecGetResultType(outerPlanState(uniquestate)),
    //                              node->numCols, node->uniqColIdx,
    //                              node->uniqOperators, node->uniqCollations,
    //                              &uniquestate->ps);
    let desc = {
        let outer = uniquestate
            .ps
            .lefttree
            .as_deref()
            .expect("ExecInitUnique: outerPlanState(uniquestate) must be initialized");
        match execTuples::exec_get_result_type::call(outer.ps_head()) {
            Some(d) => Some(alloc_in(mcx, d.clone_in(mcx)?)?),
            None => None,
        }
    };
    let uniq_col_idx = unique.uniqColIdx.as_deref().unwrap_or(&[]);
    let uniq_operators = unique.uniqOperators.as_deref().unwrap_or(&[]);
    let uniq_collations = unique.uniqCollations.as_deref().unwrap_or(&[]);
    uniquestate.eqfunction = execGrouping::exec_tuples_match_prepare::call(
        desc,
        unique.numCols,
        uniq_col_idx,
        uniq_operators,
        uniq_collations,
        &mut uniquestate.ps,
        estate,
    )?;

    Ok(uniquestate)
}

/// `ExecEndUnique(node)` — shut down the subplan and free resources allocated
/// to this node.
///
/// ```c
/// void
/// ExecEndUnique(UniqueState *node)
/// {
///     ExecEndNode(outerPlanState(node));
/// }
/// ```
pub fn ExecEndUnique<'mcx>(
    node: &mut UniqueStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExecEndNode(outerPlanState(node));
    //
    // The C calls ExecEndNode unconditionally; ExecEndNode(NULL) is a no-op.
    if let Some(outer) = node.ps.lefttree.as_deref_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }
    Ok(())
}

/// `ExecReScanUnique(node)` — rescan the node.
///
/// ```c
/// void
/// ExecReScanUnique(UniqueState *node)
/// {
///     PlanState  *outerPlan = outerPlanState(node);
///
///     ExecClearTuple(node->ps.ps_ResultTupleSlot);
///
///     if (outerPlan->chgParam == NULL)
///         ExecReScan(outerPlan);
/// }
/// ```
pub fn ExecReScanUnique<'mcx>(
    node: &mut UniqueStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // must clear result tuple so first input tuple is returned
    //   ExecClearTuple(node->ps.ps_ResultTupleSlot);
    let result_slot = node
        .ps
        .ps_ResultTupleSlot
        .expect("ExecReScanUnique: ps_ResultTupleSlot must be set");
    execTuples::exec_clear_tuple::call(estate, result_slot)?;

    // if chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    //   if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    let outer_chgparam_present = node
        .ps
        .lefttree
        .as_deref()
        .expect("ExecReScanUnique: outerPlanState(node) must be initialized")
        .ps_head()
        .chgParam
        .is_some();
    if !outer_chgparam_present {
        let outer = node
            .ps
            .lefttree
            .as_deref_mut()
            .expect("ExecReScanUnique: outerPlanState(node) must be initialized");
        execAmi::exec_re_scan::call(outer, estate)?;
    }

    Ok(())
}
