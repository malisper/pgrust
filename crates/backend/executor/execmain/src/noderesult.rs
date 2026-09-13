use ::execexpr::{
    exec_build_projection_info_subplans, exec_init_qual_subplans, exec_project, exec_qual,
    ExprState,
};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{alloc_in, PgBox};
use ::types_error::PgResult;
use ::types_nodes::plannodes::Result as ResultPlan;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};

use crate::procnode::{
    exec_end_node, exec_init_node, exec_proc_node, with_eval_slots, PlanStateBase, PlanStateNode,
};
use crate::typefromtl::exec_type_from_tl;

pub struct ResultState<'mcx> {
    pub ps: PlanStateBase<'mcx>,
    pub outer: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
    /// One-Time Filter, one compiled qual program PER CLAUSE of the
    /// implicit-AND list. C's ExecQual evaluates the clause list serially and
    /// short-circuits at the first false; with the pending-initplan hoist
    /// being per-program here (not lazy per-Param as in C's
    /// ExecEvalParamExec), a single program would run initplans that a
    /// preceding false clause proves unreachable (covdiff CONSTFOLD: C shows
    /// the InitPlan `never executed`, loops 0). Per-clause programs restore
    /// the observable order: each clause's owed initplans run only when that
    /// clause is reached.
    pub resconstantqual: Option<::mcx::PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>>,
    pub rs_done: bool,
    pub rs_checkqual: bool,
    /// C outerPlan->chgParam: a pending child rescan, applied at the first
    /// pull after the one-time filter passes.
    pub outer_chg: ::types_nodes::bitmapset::Bitmapset<'mcx>,
}

/// `ExecInitResult` (nodeResult.c).
pub fn exec_init_result<'mcx>(
    node: &'mcx ResultPlan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<ResultState<'mcx>> {
    debug_assert!(
        eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD) == 0 || node.plan.lefttree.is_some()
    );
    let mcx = estate.es_query_cxt;
    let ecxt = estate.exec_assign_expr_context();
    let outer = exec_init_node(node.plan.lefttree, estate, eflags)?;
    debug_assert!(node.plan.righttree.is_none());

    let desc = exec_type_from_tl(&node.plan.targetlist)?;
    let slot = estate.exec_init_extra_tuple_slot(Some(desc.clone()), TupleSlotKind::Virtual);
    let params = estate.param_bind();
    let (proj, qual, resconstantqual) =
        ::executils::with_subplan_compile_env(estate, |env| -> PgResult<_> {
            let proj =
                exec_build_projection_info_subplans(mcx, &node.plan.targetlist, None, params, env)?;
            let qual = exec_init_qual_subplans(mcx, &node.plan.qual, params, env)?;
            let resconstantqual = match node.resconstantqual {
                None => None,
                Some(n) => {
                    let list = n.as_list().unwrap_or_else(|| {
                        panic!(
                            "Result.resconstantqual: expected List, got {:?}",
                            n.node_tag()
                        )
                    });
                    // One program per clause: see the ResultState field doc.
                    let mut clauses = ::mcx::PgVec::new_in(mcx);
                    for clause in list.iter() {
                        let mut single = ::types_nodes::list::NodeList::nil();
                        single.lappend(mcx, clause)?;
                        let state = exec_init_qual_subplans(mcx, &single, params, env.clone())?
                            .expect("non-empty qual list compiles to a program");
                        clauses.push(state);
                    }
                    (!clauses.is_empty()).then_some(clauses)
                }
            };
            Ok((proj, qual, resconstantqual))
        })?;

    let outer = match outer {
        Some(o) => Some(alloc_in(mcx, o)?),
        None => None,
    };
    let rs_checkqual = resconstantqual.is_some();
    Ok(ResultState {
        ps: PlanStateBase {
            plan: &node.plan,
            ps_ExprContext: Some(ecxt),
            ps_ResultTupleDesc: Some(desc),
            ps_ResultTupleSlot: Some(slot),
            ps_ProjInfo: Some(proj),
            qual,
        },
        outer,
        resconstantqual,
        rs_done: false,
        rs_checkqual,
        outer_chg: ::types_nodes::bitmapset::Bitmapset::empty(),
    })
}

/// `ExecResult` (nodeResult.c).
pub fn exec_result<'mcx>(
    node: &mut ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    crate::cfi()?;
    let ecxt = node
        .ps
        .ps_ExprContext
        .expect("ResultState without ExprContext");

    if node.rs_checkqual && !lane_result_gate(node, estate)? {
        return Ok(None);
    }

    estate.reset_expr_context(ecxt);

    if node.rs_done {
        return Ok(None);
    }

    if let Some(outer) = node.outer.as_deref_mut() {
        if !node.outer_chg.is_empty() {
            let chg = core::mem::replace(
                &mut node.outer_chg,
                ::types_nodes::bitmapset::Bitmapset::empty(),
            );
            let outer_plan = node.ps.plan.lefttree.expect("Result outer plan");
            crate::execami::exec_re_scan_with_chg(outer, outer_plan, estate, &chg)?;
        }
        let Some(outer_slot) = exec_proc_node(outer, estate)? else {
            return Ok(None);
        };
        estate.ecxt_mut(ecxt).ecxt_outertuple = Some(outer_slot);
    } else {
        node.rs_done = true;
    }

    Ok(Some(lane_result_project(&mut node.ps, estate)?))
}

// ===========================================================================
// Lane-executor-v2 result seams. The lane's ResultOp / childless drive live
// in `lanev2.rs`; the arms below ARE `exec_result`'s (the Volcano body above
// calls the same functions), so the lane runs the SAME one-time gate and
// (subplan-aware) projection — no reimplementation, and a Volcano fallback at
// any call boundary sees exactly C's state (rs_checkqual / rs_done).
// ===========================================================================

/// `exec_result`'s childless (no-FROM) body, one pull's worth: entry CFI →
/// one-time gate → per-call ctx reset → drained guard → mark done + project
/// — the row-mode `ResultRowSource` face's copy (`lanev2::rowmode`).
/// `try_own_result`'s childless arm keeps its own INLINE duplicate of these
/// statements (the integration contract's pre-approved entry-cost fallback,
/// se-entrycost: outlining the select1 hot path's body cost it entry
/// instructions); the two bodies MUST stay statement-identical — the
/// rowmode_ab childless-Result seam corpus pins both knob positions.
/// `exec_result` itself keeps its two-arm body above — same seams
/// (`lane_result_gate`/`lane_result_project`), same state
/// (`rs_checkqual`/`rs_done`), so a Volcano fallback at any call boundary is
/// byte-safe.
#[inline]
pub(crate) fn lane_result_childless_next<'mcx>(
    node: &mut ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(node.outer.is_none());
    crate::cfi()?;
    if node.rs_checkqual && !lane_result_gate(node, estate)? {
        return Ok(None);
    }
    let ecxt = node
        .ps
        .ps_ExprContext
        .expect("ResultState without ExprContext");
    estate.reset_expr_context(ecxt);
    if node.rs_done {
        return Ok(None);
    }
    node.rs_done = true;
    Ok(Some(lane_result_project(&mut node.ps, estate)?))
}

/// `exec_result`'s One-Time Filter arm (`rs_checkqual`): evaluate
/// `resconstantqual` once, clause by clause in list order with C's ExecQual
/// short-circuit — each clause's pending-initplan $n params run lazily at
/// first fetch inside the clause (C ExecEvalParamExec; a false earlier
/// clause must leave later clauses' initplans un-run) — consuming
/// `rs_checkqual`. False → the node is done for good (`rs_done`),
/// no row is ever produced.
pub(crate) fn lane_result_gate<'mcx>(
    node: &mut ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    debug_assert!(node.rs_checkqual);
    let ecxt = node
        .ps
        .ps_ExprContext
        .expect("ResultState without ExprContext");
    let mut qual_result = true;
    let clauses = node
        .resconstantqual
        .as_deref_mut()
        .expect("rs_checkqual without qual");
    for clause in clauses.iter_mut() {
        // Subplan and pending-initplan param clauses ride the suspension
        // driver: a clause's $n initplans run lazily at first fetch (C
        // ExecEvalParamExec), so a false earlier clause leaves later
        // clauses' initplans un-run.
        let passes = if clause.has_subplan() || !clause.param_exec_deps().is_empty() {
            ::executils::exec_qual_with_subplans(Some(&mut **clause), estate, ecxt)?
        } else {
            with_eval_slots(estate, ecxt, None, |slots, _, _| {
                exec_qual(Some(&mut **clause), slots)
            })?
        };
        if !passes {
            qual_result = false;
            break;
        }
    }
    node.rs_checkqual = false;
    if !qual_result {
        node.rs_done = true;
    }
    Ok(qual_result)
}

/// `exec_result`'s projection tail: the (subplan- and pending-initplan-
/// aware) projection into the result slot. The caller has already
/// staged the input — `ecxt_outertuple` set for a child row, untouched for
/// the no-FROM single row — exactly as the Volcano body does.
pub(crate) fn lane_result_project<'mcx>(
    ps: &mut crate::procnode::PlanStateBase<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ExecSlotId> {
    let ecxt = ps.ps_ExprContext.expect("ResultState without ExprContext");
    let result_slot = ps
        .ps_ResultTupleSlot
        .expect("ResultState without result slot");
    let proj = ps
        .ps_ProjInfo
        .as_deref_mut()
        .expect("ResultState without projection");
    // Subplan and pending-initplan param projections ride the suspension
    // driver (lazy PARAM_EXEC fetch, C ExecEvalParamExec): an initplan in a
    // never-taken COALESCE/CASE arm stays un-run.
    if proj.has_subplan() || !proj.param_exec_deps().is_empty() {
        ::executils::exec_project_with_subplans(proj, estate, ecxt, result_slot)?;
    } else {
        with_eval_slots(estate, ecxt, Some(result_slot), |slots, result, mcx| {
            exec_project(proj, slots, result.unwrap(), mcx)
        })?;
    }
    Ok(result_slot)
}

/// `ExecEndResult` (nodeResult.c).
pub fn exec_end_result<'mcx>(
    node: &mut ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node.outer.as_deref_mut() {
        Some(outer) => exec_end_node(outer, estate),
        None => Ok(()),
    }
}

// resconstantqual exempt: an ExprState (fn_extra memos); release_owned takes it.
::mcx::forget_safe_struct!(
    ResultState<'_> { ps, outer, rs_done, rs_checkqual, outer_chg; resconstantqual },
);
