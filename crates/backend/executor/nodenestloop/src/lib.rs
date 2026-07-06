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
use ::types_error::PgResult;
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
    assert!(
        matches!(
            node.join.jointype,
            JoinType::JOIN_INNER
                | JoinType::JOIN_LEFT
                | JoinType::JOIN_SEMI
                | JoinType::JOIN_ANTI
        ),
        "ExecInitNestLoop (nodeNestloop.c): jointype {:?}; RIGHT/FULL lane unported",
        node.join.jointype
    );
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
        }
        estate.reset_expr_context(ecxt);
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
        nl_NeedNewOuter, nl_MatchedOuter, nest_params, nest_param_set;
        ps_ResultTupleDesc, proj, joinqual, otherqual },
);
