// nodeNestloop.c, inner-join arm; children stay with the ExecProcNode
// dispatcher via NestLoopChild. LEFT/SEMI/ANTI and nestParams are loud at init.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::execexpr::{
    exec_build_projection_info, exec_init_qual, exec_project, exec_qual, EvalSlots, ExprState,
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
}

pub struct NestLoopState<'mcx> {
    pub plan: &'mcx NestLoop<'mcx>,
    pub ps_ExprContext: EcxtId,
    pub ps_ResultTupleDesc: Rc<TupleDescData<'static>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    proj: PgBox<'mcx, ExprState<'mcx>>,
    joinqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    otherqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    js_single_match: bool,
    pub nl_NeedNewOuter: bool,
    pub nl_MatchedOuter: bool,
}

/// `ExecInitNestLoop` minus child linkage: the caller inits the outer child
/// with the unmodified eflags, the inner child with EXEC_FLAG_REWIND added.
pub fn exec_init_nest_loop<'mcx>(
    node: &'mcx NestLoop<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    result_desc: Rc<TupleDescData<'static>>,
) -> PgResult<NestLoopState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    assert!(
        node.join.jointype == JoinType::JOIN_INNER,
        "ExecInitNestLoop (nodeNestloop.c): jointype {:?}; LEFT/SEMI/ANTI lane unported",
        node.join.jointype
    );
    assert!(
        node.nestParams.is_nil(),
        "ExecInitNestLoop (nodeNestloop.c): nestParams; parameterized-inner lane unported"
    );
    let mcx = estate.es_query_cxt;
    let ps_ExprContext = estate.exec_assign_expr_context();

    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::Virtual);
    let proj = exec_build_projection_info(mcx, &node.join.plan.targetlist, None)?;
    let otherqual = exec_init_qual(mcx, &node.join.plan.qual)?;
    let joinqual = exec_init_qual(mcx, &node.join.joinqual)?;

    Ok(NestLoopState {
        plan: node,
        ps_ExprContext,
        ps_ResultTupleDesc: result_desc,
        ps_ResultTupleSlot,
        proj,
        joinqual,
        otherqual,
        js_single_match: node.join.inner_unique,
        nl_NeedNewOuter: true,
        nl_MatchedOuter: false,
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
            inner.rescan(estate)?;
        }

        let inner_slot = inner.exec_proc(estate)?;
        estate.ecxt_mut(ecxt).ecxt_innertuple = inner_slot;

        if inner_slot.is_none() {
            node.nl_NeedNewOuter = true;
            // JOIN_LEFT/JOIN_ANTI null-extension is loud at init.
            continue;
        }

        let joinqual = node.joinqual.as_deref_mut();
        let matched = with_qual_slots(estate, ecxt, |slots| exec_qual(joinqual, slots))?;
        if matched {
            node.nl_MatchedOuter = true;
            if node.js_single_match {
                node.nl_NeedNewOuter = true;
            }
            let otherqual = node.otherqual.as_deref_mut();
            let pass = with_qual_slots(estate, ecxt, |slots| exec_qual(otherqual, slots))?;
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
pub fn exec_end_nest_loop(_node: &mut NestLoopState<'_>) {}

/// `ExecReScanNestLoop`: caller rescans the outer child; the inner MUST NOT
/// be rescanned here (ExecNestLoop rescans it per outer tuple).
pub fn exec_rescan_nest_loop(node: &mut NestLoopState<'_>) {
    node.nl_NeedNewOuter = true;
    node.nl_MatchedOuter = false;
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
