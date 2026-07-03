// nodeProjectSet.c + execSRF.c's ExecMakeFunctionResultSet half
// (ValuePerCall only; SFRM_Materialize results and set-returning operators
// are loud).

use std::ptr::NonNull;

use ::datum::Datum;
use ::execexpr::{exec_eval_expr, exec_init_expr, EvalSlots, ExprState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{alloc_in, Mcx, MemoryContext, PgBox, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED};
use ::types_fmgr::{
    ExprDoneCond, FmgrInfo, LocalFcinfo, ReturnSetInfo, SetFunctionReturnMode, SFRM_Materialize,
    SFRM_ValuePerCall,
};
use ::types_nodes::plannodes::ProjectSet;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};

use crate::procnode::{
    exec_end_node, exec_init_node, exec_proc_node, with_eval_slots, PlanStateBase, PlanStateNode,
};
use crate::typefromtl::exec_type_from_tl;

const PROJECT_SET_MAX_ARGS: usize = 4;

// SetExprState (ValuePerCall slice); args_valid is C's setArgsValid.
struct SrfElem<'mcx> {
    flinfo: FmgrInfo,
    args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
    fcinfo: LocalFcinfo<PROJECT_SET_MAX_ARGS>,
    rsinfo: ReturnSetInfo,
    args_valid: bool,
}

enum Elem<'mcx> {
    Srf(SrfElem<'mcx>),
    Scalar(PgBox<'mcx, ExprState<'mcx>>),
}

pub struct ProjectSetState<'mcx> {
    pub ps: PlanStateBase<'mcx>,
    pub outer: PgBox<'mcx, PlanStateNode<'mcx>>,
    elems: PgVec<'mcx, Elem<'mcx>>,
    elemdone: PgVec<'mcx, ExprDoneCond>,
    pending_srf_tuples: bool,
}

/// `ExecInitProjectSet` (nodeProjectSet.c) + `ExecInitFunctionResultSet`
/// (execSRF.c).
pub fn exec_init_project_set<'mcx>(
    node: &'mcx ProjectSet<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<ProjectSetState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD) == 0);
    debug_assert!(node.plan.righttree.is_none());
    debug_assert!(node.plan.qual.is_nil());
    let mcx = estate.es_query_cxt;
    let ecxt = estate.exec_assign_expr_context();
    let outer = exec_init_node(node.plan.lefttree, estate, eflags)?
        .expect("ProjectSet has an outer plan");

    let desc = exec_type_from_tl(&node.plan.targetlist)?;
    let slot = estate.exec_init_extra_tuple_slot(Some(desc.clone()), TupleSlotKind::Virtual);

    let mut elems: PgVec<'mcx, Elem<'mcx>> = PgVec::new_in(mcx);
    let mut elemdone: PgVec<'mcx, ExprDoneCond> = PgVec::new_in(mcx);
    for tle_node in &node.plan.targetlist {
        let tle = tle_node
            .as_target_entry()
            .expect("targetlist cell is a TargetEntry");
        let expr = tle.expr;
        if let Some(oe) = expr.as_op_expr() {
            if oe.opretset {
                panic!(
                    "ExecInitFunctionResultSet (execSRF.c): set-returning operator \
                     unported — unit backend-executor-execSRF"
                );
            }
        }
        let elem = match expr.as_func_expr() {
            Some(fe) if fe.funcretset => {
                if fe.args.len() > PROJECT_SET_MAX_ARGS {
                    panic!(
                        "ExecInitFunctionResultSet: {}-argument SRF — widen the fcinfo \
                         frame",
                        fe.args.len()
                    );
                }
                let mut args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>> = PgVec::new_in(mcx);
                for arg in &fe.args {
                    // Query-context args replace C's argContext: by-ref arg
                    // datums must outlive per-tuple resets between rows.
                    let mut state = exec_init_expr(mcx, Some(arg), estate.param_bind())?
                        .expect("non-NULL arg expression");
                    state.arm_result_mcx(mcx);
                    args.push(state);
                }
                let flinfo = fmgr_core::fmgr_info(fe.funcid)?;
                debug_assert!(flinfo.fn_retset);
                let mut fcinfo = LocalFcinfo::<PROJECT_SET_MAX_ARGS>::new(fe.inputcollid);
                fcinfo.nargs = fe.args.len() as i16;
                Elem::Srf(SrfElem {
                    flinfo,
                    args,
                    fcinfo,
                    rsinfo: ReturnSetInfo::new(SFRM_ValuePerCall | SFRM_Materialize),
                    args_valid: false,
                })
            }
            _ => Elem::Scalar(
                exec_init_expr(mcx, Some(expr), estate.param_bind())?
                    .expect("non-NULL tlist expression"),
            ),
        };
        elems.push(elem);
        elemdone.push(ExprDoneCond::ExprSingleResult);
    }

    Ok(ProjectSetState {
        ps: PlanStateBase {
            plan: &node.plan,
            ps_ExprContext: Some(ecxt),
            ps_ResultTupleDesc: Some(desc),
            ps_ResultTupleSlot: Some(slot),
            ps_ProjInfo: None,
            qual: None,
        },
        outer: alloc_in(mcx, outer)?,
        elems,
        elemdone,
        pending_srf_tuples: false,
    })
}

/// `ExecProjectSet` (nodeProjectSet.c).
pub fn exec_project_set<'mcx>(
    node: &mut ProjectSetState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    crate::cfi()?;
    let ecxt = node.ps.ps_ExprContext.expect("ProjectSetState without ExprContext");
    estate.reset_expr_context(ecxt);

    if node.pending_srf_tuples && exec_project_srf(node, estate, true)? {
        return Ok(node.ps.ps_ResultTupleSlot);
    }

    loop {
        let Some(outer_slot) = exec_proc_node(&mut node.outer, estate)? else {
            return Ok(None);
        };
        estate.ecxt_mut(ecxt).ecxt_outertuple = Some(outer_slot);
        if exec_project_srf(node, estate, false)? {
            return Ok(node.ps.ps_ResultTupleSlot);
        }
        estate.reset_expr_context(ecxt);
    }
}

/// `ExecProjectSRF` (nodeProjectSet.c): true iff a row was stored.
fn exec_project_srf<'mcx>(
    node: &mut ProjectSetState<'mcx>,
    estate: &mut EStateData<'mcx>,
    continuing: bool,
) -> PgResult<bool> {
    let ecxt = node.ps.ps_ExprContext.expect("ProjectSetState without ExprContext");
    let result = node.ps.ps_ResultTupleSlot.expect("ProjectSetState without result slot");
    let per_tuple: NonNull<MemoryContext> =
        NonNull::from(estate.ecxt(ecxt).per_tuple_mcx().context());
    node.pending_srf_tuples = false;
    let elems = &mut node.elems;
    let elemdone = &mut node.elemdone;
    let pending = &mut node.pending_srf_tuples;
    with_eval_slots(estate, ecxt, Some(result), |slots, rslot, mcx| {
        let rslot = rslot.expect("result slot provided");
        exectuples::exec_clear_tuple(rslot, mcx);
        // SAFETY: the ExprContext lives in the estate for the whole query;
        // only its slot-id triple is mutably borrowed by with_eval_slots.
        let per_tuple = unsafe { per_tuple.as_ref() }.mcx();
        let mut hasresult = false;
        for (i, elem) in elems.iter_mut().enumerate() {
            let (value, isnull) = match elem {
                Elem::Srf(srf) => {
                    // Exhausted SRFs pad with NULLs until all are done.
                    if continuing && elemdone[i] == ExprDoneCond::ExprEndResult {
                        (Datum::null(), true)
                    } else {
                        let (v, vnull, isdone) =
                            exec_make_function_result_set(srf, slots, per_tuple)?;
                        elemdone[i] = isdone;
                        if isdone != ExprDoneCond::ExprEndResult {
                            hasresult = true;
                        }
                        if isdone == ExprDoneCond::ExprMultipleResult {
                            *pending = true;
                        }
                        (v, vnull)
                    }
                }
                Elem::Scalar(state) => {
                    // SAFETY: per-tuple context outlives this row's datums —
                    // consumed before the next reset (nodeagg precedent).
                    unsafe { state.arm_result_mcx_raw(per_tuple) };
                    let nd = exec_eval_expr(state, slots)?;
                    elemdone[i] = ExprDoneCond::ExprSingleResult;
                    (nd.value, nd.isnull)
                }
            };
            let base = rslot.base_mut();
            base.tts_values[i] = value;
            base.tts_isnull[i] = isnull;
        }
        if hasresult {
            exectuples::exec_store_virtual_tuple(rslot);
        }
        Ok(hasresult)
    })
}

/// `ExecMakeFunctionResultSet` (execSRF.c), ValuePerCall arm.
fn exec_make_function_result_set<'mcx>(
    srf: &mut SrfElem<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    per_tuple: Mcx<'_>,
) -> PgResult<(Datum, bool, ExprDoneCond)> {
    if !srf.args_valid {
        for i in 0..srf.args.len() {
            let nd = exec_eval_expr(&mut srf.args[i], slots)?;
            if nd.isnull {
                srf.fcinfo.set_arg_null(i);
            } else {
                srf.fcinfo.set_arg(i, nd.value);
            }
        }
    } else {
        srf.args_valid = false;
    }

    if srf.flinfo.fn_strict && srf.fcinfo.has_null_args() {
        // Strict SRF with a NULL argument: an empty set.
        return Ok((Datum::null(), true, ExprDoneCond::ExprEndResult));
    }

    srf.fcinfo.resultinfo = srf.rsinfo.as_fmnode_ptr();
    // SAFETY: re-armed before every invoke; the per-tuple context outlives
    // the call and its result is consumed before the next reset.
    unsafe { srf.fcinfo.set_result_mcx(per_tuple) };
    srf.fcinfo.isnull = false;
    srf.rsinfo.returnMode = SetFunctionReturnMode::ValuePerCall;
    srf.rsinfo.isDone = ExprDoneCond::ExprSingleResult;
    let result = srf.flinfo.invoke(&mut srf.fcinfo)?;

    match srf.rsinfo.returnMode {
        SetFunctionReturnMode::ValuePerCall => {
            let isdone = srf.rsinfo.isDone;
            if isdone == ExprDoneCond::ExprMultipleResult {
                if !srf.flinfo.fn_retset {
                    return Err(value_per_call_violated());
                }
                srf.args_valid = true;
            }
            Ok((result, srf.fcinfo.isnull, isdone))
        }
        SetFunctionReturnMode::Materialize => panic!(
            "ExecMakeFunctionResultSet (execSRF.c): SFRM_Materialize function result \
             — tuplestore-returning SRFs unported"
        ),
    }
}

#[cold]
#[inline(never)]
fn value_per_call_violated() -> Box<PgError> {
    Box::new(
        PgError::error("table-function protocol for value-per-call mode was not followed")
            .with_sqlstate(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
    )
}

/// `ExecEndProjectSet` (nodeProjectSet.c).
pub fn exec_end_project_set<'mcx>(
    node: &mut ProjectSetState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    exec_end_node(&mut node.outer, estate)
}

/// `ExecReScanProjectSet` (nodeProjectSet.c). C's ReScanExprContext fires
/// shutdown_MultiFuncCall to drop cross-call SRF state; the fn_extra Box
/// drops here instead, and setArgsValid resets with it (ShutdownSetExpr).
pub fn exec_re_scan_project_set_local(node: &mut ProjectSetState<'_>) {
    node.pending_srf_tuples = false;
    let ProjectSetState { elems, elemdone, .. } = node;
    for (i, elem) in elems.iter_mut().enumerate() {
        if let Elem::Srf(srf) = elem {
            srf.args_valid = false;
            srf.flinfo.fn_extra = None;
            elemdone[i] = ExprDoneCond::ExprSingleResult;
        }
    }
}

pub(crate) fn release_project_set(node: &mut ProjectSetState<'_>) {
    node.elems.clear();
}

// Exempt: elems (compiled ExprStates + FmgrInfo fn_extra Boxes) released in
// release_owned; elemdone is drop-free (foreign Copy enum, uncensusable here).
::mcx::forget_safe_struct!(
    ProjectSetState<'_> { ps, outer, pending_srf_tuples; elems, elemdone },
);
