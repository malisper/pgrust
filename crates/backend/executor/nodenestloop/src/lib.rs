// nodeNestloop.c, INNER/LEFT/SEMI/ANTI arms; children stay with the
// ExecProcNode dispatcher via NestLoopChild.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::execexpr::{
    exec_build_projection_info_subplans, exec_init_qual_subplans, exec_project, exec_qual,
    EvalSlots, ExprState,
};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::PgBox;
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::types_nodes::plannodes::NestLoop;
use ::types_nodes::JoinType;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[inline(always)]
fn cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

pub trait NestLoopChild<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>;
    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()>;
    /// ExecReScan after this join bound fresh nestParams (chgParam-driven).
    fn rescan_with_chg(
        &mut self,
        plan: ::types_nodes::Node<'mcx>,
        estate: &mut EStateData<'mcx>,
        chg: &::types_nodes::bitmapset::Bitmapset<'mcx>,
    ) -> PgResult<()>;
}

pub struct NestLoopState<'mcx> {
    pub plan: &'mcx NestLoop<'mcx>,
    pub ps_ExprContext: EcxtId,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    proj: PgBox<'mcx, ExprState<'mcx>>,
    joinqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    otherqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    js_single_match: bool,
    nl_fill_outer: bool,
    nl_NullInnerTupleSlot: Option<ExecSlotId>,
    pub nl_NeedNewOuter: bool,
    pub nl_MatchedOuter: bool,
    // Outer-tlist source per nestParam, resolved once at init.
    nest_params: ::mcx::PgVec<'mcx, NestParamSlot>,
    nest_param_set: ::types_nodes::bitmapset::Bitmapset<'mcx>,
    // InstrCountFiltered1/2 slot for this join node (nodeNestloop.c).
    js_instr: Option<u32>,
}

#[derive(Clone, Copy)]
struct NestParamSlot {
    paramno: i32,
    attno: i16,
}

/// `ExecInitNestLoop` minus child linkage: the caller inits the outer child
/// with the unmodified eflags, the inner child with EXEC_FLAG_REWIND added.
pub fn exec_init_nest_loop<'mcx>(
    node: &'mcx NestLoop<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    result_desc: Rc<TupleDescData<'static>>,
    inner_desc: &Rc<TupleDescData<'static>>,
) -> PgResult<NestLoopState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    if !matches!(
        node.join.jointype,
        JoinType::JOIN_INNER | JoinType::JOIN_LEFT | JoinType::JOIN_SEMI | JoinType::JOIN_ANTI
    ) {
        // unported: ExecInitNestLoop (nodeNestloop.c) RIGHT/FULL lane.
        return Err(Box::new(
            PgError::error(format!(
                "nested loop with join type {:?} is not yet implemented",
                node.join.jointype
            ))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    let nl_fill_outer =
        matches!(node.join.jointype, JoinType::JOIN_LEFT | JoinType::JOIN_ANTI);
    let nl_NullInnerTupleSlot = if nl_fill_outer {
        let slot_id = estate
            .exec_init_extra_tuple_slot(Some(inner_desc.clone()), TupleSlotKind::Virtual);
        exectuples::exec_store_all_null_tuple(
            &mut estate.es_tupleTable[slot_id.0 as usize],
            estate.es_query_cxt,
        );
        Some(slot_id)
    } else {
        None
    };
    let mcx = estate.es_query_cxt;
    let mut nest_params: ::mcx::PgVec<'mcx, NestParamSlot> = ::mcx::PgVec::new_in(mcx);
    let mut nest_param_set = ::types_nodes::bitmapset::Bitmapset::empty();
    for nlp_node in &node.nestParams {
        let nlp = nlp_node
            .as_nest_loop_param()
            .expect("nestParams cell is a NestLoopParam");
        let v = nlp
            .paramval
            .as_var()
            .expect("NestLoopParam value is a simple Var");
        debug_assert!(v.varno == ::types_nodes::primnodes::OUTER_VAR && v.varattno > 0);
        nest_params.push(NestParamSlot { paramno: nlp.paramno, attno: v.varattno });
        nest_param_set.add_member(mcx, nlp.paramno)?;
    }
    let ps_ExprContext = estate.exec_assign_expr_context();

    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::Virtual);
    let params = estate.param_bind();
    let (proj, otherqual, joinqual) =
        ::executils::with_subplan_compile_env(estate, |env| -> PgResult<_> {
            let proj = exec_build_projection_info_subplans(
                mcx,
                &node.join.plan.targetlist,
                None,
                params,
                env,
            )?;
            let otherqual = exec_init_qual_subplans(mcx, &node.join.plan.qual, params, env)?;
            let joinqual = exec_init_qual_subplans(mcx, &node.join.joinqual, params, env)?;
            Ok((proj, otherqual, joinqual))
        })?;

    Ok(NestLoopState {
        plan: node,
        ps_ExprContext,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        proj,
        joinqual,
        otherqual,
        js_single_match: node.join.inner_unique
            || node.join.jointype == JoinType::JOIN_SEMI,
        nl_fill_outer,
        nl_NullInnerTupleSlot,
        nl_NeedNewOuter: true,
        nl_MatchedOuter: false,
        nest_params,
        nest_param_set,
        js_instr: if estate.es_instrument != 0 {
            Some(u32::try_from(node.join.plan.plan_node_id).expect("plan_node_id is non-negative"))
        } else {
            None
        },
    })
}

pub fn exec_nest_loop<'mcx, O, I>(
    node: &mut NestLoopState<'mcx>,
    outer: &mut O,
    inner: &mut I,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>>
where
    O: NestLoopChild<'mcx>,
    I: NestLoopChild<'mcx>,
{
    cfi()?;
    let ecxt = node.ps_ExprContext;
    estate.reset_expr_context(ecxt);

    loop {
        if node.nl_NeedNewOuter {
            let Some(outer_slot) = outer.exec_proc(estate)? else {
                return Ok(None);
            };
            estate.ecxt_mut(ecxt).ecxt_outertuple = Some(outer_slot);
            node.nl_NeedNewOuter = false;
            node.nl_MatchedOuter = false;
            if node.nest_params.is_empty() {
                inner.rescan(estate)?;
            } else {
                // Bind the outer Vars into their PARAM_EXEC slots, then
                // rescan the inner with the changed-param set.
                for &NestParamSlot { paramno, attno } in node.nest_params.iter() {
                    let mut isnull = false;
                    let value = exectuples::slot_getattr(
                        &mut estate.es_tupleTable[outer_slot.0 as usize],
                        attno as i32,
                        &mut isnull,
                    );
                    let prm = &mut estate.es_param_exec_vals[paramno as usize];
                    prm.value = value;
                    prm.isnull = isnull;
                }
                let inner_plan =
                    node.plan.join.plan.righttree.expect("nestloop inner plan");
                inner.rescan_with_chg(inner_plan, estate, &node.nest_param_set)?;
            }
        }

        let inner_slot = inner.exec_proc(estate)?;
        estate.ecxt_mut(ecxt).ecxt_innertuple = inner_slot;

        if inner_slot.is_none() {
            node.nl_NeedNewOuter = true;
            if !node.nl_MatchedOuter && node.nl_fill_outer {
                let null_inner = node.nl_NullInnerTupleSlot.expect("null inner slot");
                estate.ecxt_mut(ecxt).ecxt_innertuple = Some(null_inner);
                let pass = eval_join_qual(node.otherqual.as_deref_mut(), estate, ecxt)?;
                if pass {
                    let result_slot = node.ps_ResultTupleSlot;
                    let proj = &mut *node.proj;
                    project_join_tuple(estate, ecxt, result_slot, proj)?;
                    return Ok(Some(result_slot));
                }
                estate.instr_count_filtered2(node.js_instr);
                estate.reset_expr_context(ecxt);
            }
            continue;
        }

        let matched = eval_join_qual(node.joinqual.as_deref_mut(), estate, ecxt)?;
        if matched {
            node.nl_MatchedOuter = true;
            // An antijoin never returns a matched tuple.
            if node.plan.join.jointype == JoinType::JOIN_ANTI {
                node.nl_NeedNewOuter = true;
                continue;
            }
            if node.js_single_match {
                node.nl_NeedNewOuter = true;
            }
            let pass = eval_join_qual(node.otherqual.as_deref_mut(), estate, ecxt)?;
            if pass {
                let result_slot = node.ps_ResultTupleSlot;
                let proj = &mut *node.proj;
                project_join_tuple(estate, ecxt, result_slot, proj)?;
                return Ok(Some(result_slot));
            }
            estate.instr_count_filtered2(node.js_instr);
        } else {
            estate.instr_count_filtered1(node.js_instr);
        }
        estate.reset_expr_context(ecxt);
    }
}

// ===========================================================================
// Lane-executor-v2 seam (design §4: NestLoop hosting). The wiring lives in
// `execmain/src/lanev2.rs`; these entry points delegate to the SAME per-row
// arms `exec_nest_loop` runs (`eval_join_qual` / `project_join_tuple`) over
// the SAME `NestLoopState` cross-call flags (`nl_NeedNewOuter` /
// `nl_MatchedOuter` — C's own state machine; no new fields), so falling back
// to `exec_nest_loop` at any call boundary resumes from coherent node state,
// and the lane's join output (outer order × inner rescan order) is C's
// exactly. The INNER child stays a Volcano child: the lane drives it through
// the same `NestLoopChild` calls (`exec_proc` / `rescan` / `rescan_with_chg`)
// at the same points, so exec-param-driven runtime keys on an inner index
// scan are (re)evaluated in the rescan preamble exactly as C's ExecReScan
// path does — automatically, with no lane-side special case.
// ===========================================================================

/// Structural admission, join side. All four ported join types (INNER / LEFT
/// / SEMI / ANTI) are admitted — the lane emit is `exec_nest_loop`'s own loop
/// body, fill arm included (init asserts RIGHT/FULL never reach this node).
/// Refused: instrumented (`js_instr` is `Some` iff es_instrument != 0 at
/// init), and subplan- / initplan-param-bearing joinqual / otherqual /
/// projection — the lane's ecxt-reset cadence is per outer row rather than
/// per Volcano pull (memory-only for plain computation), so suspension
/// hosting and the pending-initplan hoist stay on the row path.
pub fn lane_nest_loop_admissible(node: &NestLoopState<'_>) -> bool {
    node.js_instr.is_none()
        && node
            .joinqual
            .as_deref()
            .is_none_or(|q| !q.has_subplan() && q.param_exec_deps().is_empty())
        && node
            .otherqual
            .as_deref()
            .is_none_or(|q| !q.has_subplan() && q.param_exec_deps().is_empty())
        && !node.proj.has_subplan()
        && node.proj.param_exec_deps().is_empty()
}

/// True while no drive (lane or row-path) has touched this join: the lane
/// verdict is memoized at first engagement, so admitting only an untouched
/// node guarantees the lane owns the node's whole life (never a mid-stream
/// takeover from row-path-left state — the outer scan would already be
/// mid-stream in per-tuple mode). Join-untouched implies both children are
/// untouched: this join is their only puller.
pub fn lane_nest_loop_untouched<'mcx>(
    node: &NestLoopState<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    node.nl_NeedNewOuter
        && !node.nl_MatchedOuter
        && estate.ecxt(node.ps_ExprContext).ecxt_outertuple.is_none()
}

/// The join's PARAM_EXEC slot set (nestParams → paramno members), resolved
/// at init. Read-only admission surface for the runtime NL-inner-index arm:
/// every exec param a worker-side expression may reference must be a member
/// (the params are bound per outer row inside the worker's own estate by
/// `lane_accept_outer` — self-contained under the transferred subtree).
pub fn lane_nest_param_set<'a, 'mcx>(
    node: &'a NestLoopState<'mcx>,
) -> &'a ::types_nodes::bitmapset::Bitmapset<'mcx> {
    &node.nest_param_set
}

/// Current outer row's inner drain unfinished? (`nl_NeedNewOuter` is C's own
/// cross-call flag, so a paused expansion resumes exactly across the Volcano
/// pull boundary, as `ExecNestLoop`'s own cross-call state does.)
#[inline]
pub fn lane_probe_pending(node: &NestLoopState<'_>) -> bool {
    !node.nl_NeedNewOuter
}

/// Accept one outer row: `exec_nest_loop`'s nl_NeedNewOuter arm minus the
/// outer pull (the row arrives pushed) — bind the outer tuple into the join
/// ecxt, assign the join's exec params from it (nestParams → PARAM_EXEC
/// slots), and RESCAN the inner Volcano child with the changed-param set,
/// exactly the row path's per-outer-row prologue.
pub fn lane_accept_outer<'mcx, I: NestLoopChild<'mcx>>(
    node: &mut NestLoopState<'mcx>,
    inner: &mut I,
    estate: &mut EStateData<'mcx>,
    outer_slot: ExecSlotId,
) -> PgResult<()> {
    cfi()?;
    debug_assert!(node.nl_NeedNewOuter, "accept with a pending inner drain");
    let ecxt = node.ps_ExprContext;
    estate.reset_expr_context(ecxt);
    estate.ecxt_mut(ecxt).ecxt_outertuple = Some(outer_slot);
    node.nl_NeedNewOuter = false;
    node.nl_MatchedOuter = false;
    if node.nest_params.is_empty() {
        inner.rescan(estate)
    } else {
        // Bind the outer Vars into their PARAM_EXEC slots, then rescan the
        // inner with the changed-param set (exec_nest_loop's arm, verbatim).
        for &NestParamSlot { paramno, attno } in node.nest_params.iter() {
            let mut isnull = false;
            let value = exectuples::slot_getattr(
                &mut estate.es_tupleTable[outer_slot.0 as usize],
                attno as i32,
                &mut isnull,
            );
            let prm = &mut estate.es_param_exec_vals[paramno as usize];
            prm.value = value;
            prm.isnull = isnull;
        }
        let inner_plan = node.plan.join.plan.righttree.expect("nestloop inner plan");
        inner.rescan_with_chg(inner_plan, estate, &node.nest_param_set)
    }
}

/// Next joined tuple for the accepted outer row: `exec_nest_loop`'s loop body
/// with the outer pull replaced by `Ok(None)` (the lane feeds the next outer
/// through `lane_accept_outer`). Same inner pulls, same qual evaluations,
/// same projection, same LEFT/ANTI fill arm, same SEMI / ANTI / single-match
/// (inner_unique) state transitions, in the same order — byte-identical
/// output by construction.
pub fn lane_probe_next<'mcx, I: NestLoopChild<'mcx>>(
    node: &mut NestLoopState<'mcx>,
    inner: &mut I,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    if node.nl_NeedNewOuter {
        return Ok(None);
    }
    cfi()?;
    let ecxt = node.ps_ExprContext;
    loop {
        let inner_slot = inner.exec_proc(estate)?;
        estate.ecxt_mut(ecxt).ecxt_innertuple = inner_slot;

        if inner_slot.is_none() {
            node.nl_NeedNewOuter = true;
            if !node.nl_MatchedOuter && node.nl_fill_outer {
                let null_inner = node.nl_NullInnerTupleSlot.expect("null inner slot");
                estate.ecxt_mut(ecxt).ecxt_innertuple = Some(null_inner);
                let pass = eval_join_qual(node.otherqual.as_deref_mut(), estate, ecxt)?;
                if pass {
                    let result_slot = node.ps_ResultTupleSlot;
                    let proj = &mut *node.proj;
                    project_join_tuple(estate, ecxt, result_slot, proj)?;
                    return Ok(Some(result_slot));
                }
                estate.instr_count_filtered2(node.js_instr);
                estate.reset_expr_context(ecxt);
            }
            return Ok(None);
        }

        let matched = eval_join_qual(node.joinqual.as_deref_mut(), estate, ecxt)?;
        if matched {
            node.nl_MatchedOuter = true;
            // An antijoin never returns a matched tuple.
            if node.plan.join.jointype == JoinType::JOIN_ANTI {
                node.nl_NeedNewOuter = true;
                return Ok(None);
            }
            if node.js_single_match {
                node.nl_NeedNewOuter = true;
            }
            let pass = eval_join_qual(node.otherqual.as_deref_mut(), estate, ecxt)?;
            if pass {
                let result_slot = node.ps_ResultTupleSlot;
                let proj = &mut *node.proj;
                project_join_tuple(estate, ecxt, result_slot, proj)?;
                return Ok(Some(result_slot));
            }
            estate.instr_count_filtered2(node.js_instr);
        } else {
            estate.instr_count_filtered1(node.js_instr);
        }
        estate.reset_expr_context(ecxt);
        if node.nl_NeedNewOuter {
            // single-match emit denied by otherqual: the row path's loop pulls
            // a new outer; the lane's next outer arrives via accept.
            return Ok(None);
        }
    }
}

/// `ExecEndNestLoop`: child-only teardown; the caller ends the children.
pub fn exec_end_nest_loop(node: &mut NestLoopState<'_>) {
    node.joinqual = None;
    node.otherqual = None;
    node.proj.release_frames();
    node.ps_ResultTupleDesc = None;
}

/// `ExecReScanNestLoop`: caller rescans the outer child; the inner MUST NOT
/// be rescanned here (ExecNestLoop rescans it per outer tuple).
pub fn exec_rescan_nest_loop(node: &mut NestLoopState<'_>) {
    node.nl_NeedNewOuter = true;
    node.nl_MatchedOuter = false;
}

#[inline(always)]
fn eval_join_qual<'mcx>(
    qual: Option<&mut ExprState<'mcx>>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
) -> PgResult<bool> {
    // C ExecQual(NULL) returns true before any slot access (constraint: the
    // hashjoin eval_probe_qual fast path; its absence here cost memoize_lat
    // ~36M instr/q in with_qual_slots calls on None quals).
    if qual.is_none() {
        return Ok(true);
    }
    // ExecEvalParamExec pending-initplan arm, hoisted out of the interpreter.
    let deps = qual.as_ref().unwrap().param_exec_deps();
    if !deps.is_empty() {
        ::executils::exec_eval_param_exec_params(estate, deps)?;
    }
    if qual.as_ref().is_some_and(|q| q.has_subplan()) {
        return ::executils::exec_qual_with_subplans(qual, estate, ecxt);
    }
    with_qual_slots(estate, ecxt, |slots| exec_qual(qual, slots))
}

fn with_qual_slots<'mcx, R>(
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    f: impl FnOnce(&mut EvalSlots<'_, 'mcx>) -> PgResult<R>,
) -> PgResult<R> {
    let (inner_id, outer_id) = {
        let e = estate.ecxt(ecxt);
        (
            e.ecxt_innertuple.expect("nestloop inner tuple set"),
            e.ecxt_outertuple.expect("nestloop outer tuple set"),
        )
    };
    let table = &mut estate.es_tupleTable[..];
    let [inner, outer] = table
        .get_disjoint_mut([inner_id.0 as usize, outer_id.0 as usize])
        .expect("distinct in-range nestloop slot ids");
    let mut slots = EvalSlots { scan: None, inner: Some(inner), outer: Some(outer) };
    f(&mut slots)
}

fn project_join_tuple<'mcx>(
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    result: ExecSlotId,
    proj: &mut ExprState<'mcx>,
) -> PgResult<()> {
    // ExecEvalParamExec pending-initplan arm, hoisted out of the interpreter.
    let deps = proj.param_exec_deps();
    if !deps.is_empty() {
        ::executils::exec_eval_param_exec_params(estate, deps)?;
    }
    if proj.has_subplan() {
        return ::executils::exec_project_with_subplans(proj, estate, ecxt, result);
    }
    let mcx = estate.es_query_cxt;
    let (inner_id, outer_id) = {
        let e = estate.ecxt(ecxt);
        (
            e.ecxt_innertuple.expect("nestloop inner tuple set"),
            e.ecxt_outertuple.expect("nestloop outer tuple set"),
        )
    };
    let table = &mut estate.es_tupleTable[..];
    let [inner, outer, result] = table
        .get_disjoint_mut([inner_id.0 as usize, outer_id.0 as usize, result.0 as usize])
        .expect("distinct in-range nestloop slot ids");
    let mut slots = EvalSlots { scan: None, inner: Some(inner), outer: Some(outer) };
    exec_project(proj, &mut slots, result, mcx)
}

// Exempt: all released in exec_end_nest_loop (proj via release_frames).
mcx::forget_safe_struct!(
    NestParamSlot { paramno, attno },
    NestLoopState<'_> { plan, ps_ExprContext, ps_ResultTupleSlot,
        js_single_match, nl_fill_outer, nl_NullInnerTupleSlot,
        nl_NeedNewOuter, nl_MatchedOuter, nest_params, nest_param_set, js_instr;
        ps_ResultTupleDesc, proj, joinqual, otherqual },
);
