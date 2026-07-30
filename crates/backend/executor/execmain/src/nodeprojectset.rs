// nodeProjectSet.c + execSRF.c's ExecMakeFunctionResultSet half
// (set-returning operators are loud).

use std::ptr::NonNull;
use std::rc::Rc;

use ::datum::Datum;
use ::execexpr::{exec_eval_expr, exec_init_expr_subplans, EvalSlots, ExprState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{alloc_in, Mcx, MemoryContext, PgBox, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED};
use ::types_fmgr::{
    ExprDoneCond, FmgrInfo, LocalFcinfo, ReturnSetInfo, SetFunctionReturnMode, SFRM_Materialize,
    SFRM_ValuePerCall, TRACK_FUNC_ALL,
};
use ::types_nodes::plannodes::ProjectSet;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};

use crate::procnode::{
    exec_end_node, exec_init_node, exec_proc_node, with_eval_slots, PlanStateBase, PlanStateNode,
};
use crate::typefromtl::exec_type_from_tl;

// C sizes the fcinfo per call (SizeForFunctionCallInfo(nargs), up to
// FUNC_MAX_ARGS); this inline frame caps the SRF arg count instead — the
// init-time guard below rejects wider calls.
const PROJECT_SET_MAX_ARGS: usize = 8;

// SetExprState; args_valid is C's setArgsValid, result_desc/result_slot/
// result_store are funcResultDesc/funcResultSlot/funcResultStore.
struct SrfElem<'mcx> {
    flinfo: FmgrInfo,
    args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
    fcinfo: LocalFcinfo<PROJECT_SET_MAX_ARGS>,
    rsinfo: ReturnSetInfo,
    args_valid: bool,
    result_desc: Option<Rc<::types_tuple::TupleDescData<'mcx>>>,
    returns_tuple: bool,
    result_slot: Option<::types_slot::SlotData<'mcx>>,
    result_store: Option<::tuplestore::Tuplestore>,
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

    let params = estate.param_bind();
    let (elems, elemdone) = ::executils::with_subplan_compile_env(estate, |env| -> PgResult<_> {
    let mut elems: PgVec<'mcx, Elem<'mcx>> = PgVec::new_in(mcx);
    let mut elemdone: PgVec<'mcx, ExprDoneCond> = PgVec::new_in(mcx);
    for tle_node in &node.plan.targetlist {
        let tle = tle_node
            .as_target_entry()
            .expect("targetlist cell is a TargetEntry");
        let expr = tle.expr;
        // C ExecInitFunctionResultSet (execSRF.c) takes FuncExpr and OpExpr
        // alike: (funcid, args, inputcollid) feed the same init_sexpr.
        let srf_parts = if let Some(fe) = expr.as_func_expr() {
            fe.funcretset.then(|| (fe.funcid, &fe.args, fe.inputcollid))
        } else if let Some(oe) = expr.as_op_expr() {
            oe.opretset.then(|| (oe.opfuncid, &oe.args, oe.inputcollid))
        } else {
            None
        };
        let elem = match srf_parts {
            Some((srf_funcid, srf_args, srf_inputcollid)) => {
                if srf_args.len() > PROJECT_SET_MAX_ARGS {
                    panic!(
                        "ExecInitFunctionResultSet: {}-argument SRF — widen the fcinfo \
                         frame",
                        srf_args.len()
                    );
                }
                let mut args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>> = PgVec::new_in(mcx);
                for arg in srf_args {
                    // Query-context args replace C's argContext: by-ref arg
                    // datums must outlive per-tuple resets between rows.
                    let mut state = exec_init_expr_subplans(mcx, Some(arg), params, env)?
                        .expect("non-NULL arg expression");
                    state.arm_result_mcx(mcx);
                    args.push(state);
                }
                let mut flinfo = fmgr_core::fmgr_info(srf_funcid)?;
                // C init_sexpr: fmgr_info_set_expr — get_fn_expr_argtype
                // consumers read arg types off the call expression.
                flinfo.fn_expr = Some(::execexpr::erase_fn_expr(mcx, expr)?);
                debug_assert!(flinfo.fn_retset);
                let mut fcinfo = LocalFcinfo::<PROJECT_SET_MAX_ARGS>::new(srf_inputcollid);
                fcinfo.nargs = srf_args.len() as i16;
                let resolved = funcapi::get_expr_result_type(mcx, Some(expr))?;
                let (result_desc, returns_tuple) = match resolved.class {
                    funcapi::TypeFuncClass::Composite
                    | funcapi::TypeFuncClass::CompositeDomain => (
                        Some(Rc::new(resolved.result_tuple_desc.unwrap_or_else(|| {
                            panic!("init_sexpr (execSRF.c): composite result without tupdesc")
                        }))),
                        true,
                    ),
                    funcapi::TypeFuncClass::Scalar => {
                        let mut d = tupdesc::CreateTemplateTupleDesc(mcx, 1)?;
                        tupdesc::TupleDescInitEntry(
                            &mut d,
                            1,
                            None,
                            resolved.result_type_id,
                            -1,
                            0,
                        )?;
                        tupdesc::TupleDescInitEntryCollation(
                            &mut d,
                            1,
                            ::execscan::expr_collation(expr),
                        );
                        (Some(Rc::new(d)), false)
                    }
                    // C funcReturnsTuple: RECORD is a rowtype; the read
                    // slot builds lazily from rsinfo.setDesc (execSRF.c).
                    funcapi::TypeFuncClass::Record => (None, true),
                    _ => (None, false),
                };
                let result_slot = result_desc.as_ref().map(|d| {
                    ::exectuples::make_tuple_table_slot(
                        mcx,
                        TupleSlotKind::MinimalTuple,
                        Some(d.clone()),
                    )
                });
                Elem::Srf(SrfElem {
                    flinfo,
                    args,
                    fcinfo,
                    rsinfo: ReturnSetInfo::new(SFRM_ValuePerCall | SFRM_Materialize),
                    args_valid: false,
                    result_desc,
                    returns_tuple,
                    result_slot,
                    result_store: None,
                })
            }
            _ => Elem::Scalar(
                exec_init_expr_subplans(mcx, Some(expr), params, env)?
                    .expect("non-NULL tlist expression"),
            ),
        };
        elems.push(elem);
        elemdone.push(ExprDoneCond::ExprSingleResult);
    }
    Ok((elems, elemdone))
    })?;

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

/// `ExecProjectSet` (nodeProjectSet.c): the per-call prologue (CFI + entry
/// per-tuple reset), then the SAME body the lane's row-mode face drives —
/// `LaneProjectSet::resume_expansion` for a pending expansion,
/// `LaneProjectSet::accept` per child row (one body, two faces).
pub fn exec_project_set<'mcx>(
    node: &mut ProjectSetState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    crate::cfi()?;
    let ecxt = node.ps.ps_ExprContext.expect("ProjectSetState without ExprContext");
    estate.reset_expr_context(ecxt);

    let (mut view, outer) = lane_project_set_split(node);
    if view.pending() {
        if let Some(slot) = view.resume_expansion(estate)? {
            return Ok(Some(slot));
        }
    }

    loop {
        let Some(outer_slot) = exec_proc_node(outer, estate)? else {
            return Ok(None);
        };
        if let Some(slot) = view.accept(estate, outer_slot)? {
            return Ok(Some(slot));
        }
    }
}

// ===========================================================================
// Lane-executor-v2 row-mode seams (lanev2/rowmode.rs). `LaneProjectSet` is
// the lane's disjoint-borrow view over ProjectSetState — everything except
// `outer` — so a row-mode driver can hold the op view and the child node
// simultaneously (the SortNode-destructure precedent). The methods below ARE
// `exec_project_set`'s own body (the Volcano face above calls the same
// functions): all SRF cross-call state (`pending_srf_tuples`, `args_valid`,
// `elemdone`, `result_store`) stays the node's own C state, so a Volcano
// fallback at any PG call boundary sees exactly the state its own next call
// expects. Reset cadence contract (by-ref SRF datums live in per-tuple
// memory): the ENTRY reset belongs to the caller's per-call prologue
// (`exec_project_set` above; `try_own_project_set` replays it), covering
// C's continuing-call entry reset; `accept` runs the loop-bottom reset only
// after a non-producing row, exactly as the Volcano loop does.
// ===========================================================================

/// Split a ProjectSetState into (op view, child node).
pub(crate) fn lane_project_set_split<'a, 'mcx>(
    node: &'a mut ProjectSetState<'mcx>,
) -> (LaneProjectSet<'a, 'mcx>, &'a mut PgBox<'mcx, PlanStateNode<'mcx>>) {
    let ProjectSetState { ps, outer, elems, elemdone, pending_srf_tuples } = node;
    (LaneProjectSet { base: ps, elems, elemdone, pending: pending_srf_tuples }, outer)
}

/// The disjoint-borrow op view (everything but `outer`); `Elem` stays
/// private to this module.
pub(crate) struct LaneProjectSet<'a, 'mcx> {
    base: &'a mut PlanStateBase<'mcx>,
    elems: &'a mut PgVec<'mcx, Elem<'mcx>>,
    elemdone: &'a mut PgVec<'mcx, ExprDoneCond>,
    pending: &'a mut bool,
}

impl<'mcx> LaneProjectSet<'_, 'mcx> {
    /// C's `pending_srf_tuples` — a half-emitted expansion exists.
    pub(crate) fn pending(&self) -> bool {
        *self.pending
    }

    /// `exec_project_set`'s loop body over ONE staged child row: set
    /// `ecxt_outertuple`, `ExecProjectSRF(continuing=false)`; on no-row the
    /// loop-bottom per-tuple ctx reset runs before returning `None` (the
    /// caller feeds the next child row).
    pub(crate) fn accept(
        &mut self,
        estate: &mut EStateData<'mcx>,
        outer: ExecSlotId,
    ) -> PgResult<Option<ExecSlotId>> {
        let ecxt = self.base.ps_ExprContext.expect("ProjectSetState without ExprContext");
        estate.ecxt_mut(ecxt).ecxt_outertuple = Some(outer);
        if exec_project_srf(self, estate, false)? {
            return Ok(self.base.ps_ResultTupleSlot);
        }
        estate.reset_expr_context(ecxt);
        Ok(None)
    }

    /// `exec_project_set`'s continuing arm: `ExecProjectSRF(continuing=true)`
    /// after the caller's per-call prologue reset (= the entry reset of C's
    /// continuing call). `None` = expansion done (the caller feeds the next
    /// child row, with NO intervening reset — C falls straight into its
    /// child-pull loop).
    pub(crate) fn resume_expansion(
        &mut self,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        debug_assert!(*self.pending);
        if exec_project_srf(self, estate, true)? {
            return Ok(self.base.ps_ResultTupleSlot);
        }
        Ok(None)
    }
}

/// `ExecProjectSRF` (nodeProjectSet.c): true iff a row was stored.
fn exec_project_srf<'mcx>(
    node: &mut LaneProjectSet<'_, 'mcx>,
    estate: &mut EStateData<'mcx>,
    continuing: bool,
) -> PgResult<bool> {
    let ecxt = node.base.ps_ExprContext.expect("ProjectSetState without ExprContext");
    let result = node.base.ps_ResultTupleSlot.expect("ProjectSetState without result slot");
    // C runs pending initplans lazily inside ExecEvalExpr (ExecEvalParamExec,
    // execExprInterp.c); the SRF args' and scalar elems' $n params resolve
    // here instead (execscan note).
    for elem in node.elems.iter() {
        match elem {
            Elem::Srf(srf) => {
                for arg in srf.args.iter() {
                    let deps = arg.param_exec_deps();
                    if !deps.is_empty() {
                        ::executils::exec_eval_param_exec_params(estate, deps)?;
                    }
                }
            }
            Elem::Scalar(state) => {
                let deps = state.param_exec_deps();
                if !deps.is_empty() {
                    ::executils::exec_eval_param_exec_params(estate, deps)?;
                }
            }
        }
    }
    let per_tuple: NonNull<MemoryContext> =
        NonNull::from(estate.ecxt(ecxt).per_tuple_mcx().context());
    *node.pending = false;
    let elems = &mut *node.elems;
    let elemdone = &mut *node.elemdone;
    let pending = &mut *node.pending;
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
                            exec_make_function_result_set(srf, slots, per_tuple, mcx)?;
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

// C's "restart:" store-read leg: pop the next materialized row, or clear the
// exhausted store.
fn read_result_store<'mcx>(
    srf: &mut SrfElem<'mcx>,
    per_tuple: Mcx<'_>,
    query_mcx: Mcx<'mcx>,
) -> PgResult<(Datum, bool, ExprDoneCond)> {
    let store = srf.result_store.as_mut().expect("caller checked");
    let slot = srf
        .result_slot
        .as_mut()
        .expect("materialize SRF result without a resolved result tupdesc");
    if store.gettupleslot(true, false, slot, query_mcx)? {
        if srf.returns_tuple {
            let d = ::exectuples::exec_fetch_slot_heap_tuple_datum(slot, query_mcx, per_tuple)?;
            Ok((d, false, ExprDoneCond::ExprMultipleResult))
        } else {
            let mut isnull = false;
            let d = ::exectuples::slot_getattr(slot, 1, &mut isnull);
            Ok((d, isnull, ExprDoneCond::ExprMultipleResult))
        }
    } else {
        srf.result_store = None;
        Ok((Datum::null(), true, ExprDoneCond::ExprEndResult))
    }
}

/// `ExecMakeFunctionResultSet` (execSRF.c).
fn exec_make_function_result_set<'mcx>(
    srf: &mut SrfElem<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    per_tuple: Mcx<'_>,
    query_mcx: Mcx<'mcx>,
) -> PgResult<(Datum, bool, ExprDoneCond)> {
    if srf.result_store.is_some() {
        return read_result_store(srf, per_tuple, query_mcx);
    }

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

    // SAFETY: Rc keeps the desc image stable while the rsinfo aliases it for
    // this call.
    srf.rsinfo.expectedDesc = srf
        .result_desc
        .as_ref()
        .map(|d| NonNull::from(&**d).cast::<core::ffi::c_void>());
    // SAFETY: re-armed before every invoke; the per-tuple context outlives
    // the call and its result is consumed before the next reset.
    unsafe { srf.fcinfo.set_result_mcx(per_tuple) };
    // C: pgstat_init_function_usage's `pgstat_track_functions <= fn_stats`
    // early-out, hoisted to the caller as the crate's API requires.
    let fcu = if srf.flinfo.fn_stats < TRACK_FUNC_ALL
        && ::pgstat::function::pgstat_track_functions() > srf.flinfo.fn_stats as i32
    {
        Some(::pgstat::function::pgstat_init_function_usage(srf.flinfo.fn_oid)?)
    } else {
        None
    };
    srf.fcinfo.isnull = false;
    srf.rsinfo.returnMode = SetFunctionReturnMode::ValuePerCall;
    srf.rsinfo.isDone = ExprDoneCond::ExprSingleResult;
    // Arm resultinfo LAST, after every direct rsinfo field write above: each
    // safe `&mut srf.rsinfo` access invalidates a previously armed pointer's
    // provenance, and the callee re-derives through it (rsinfo_mut). C's
    // contract is the same — execSRF.c re-arms per ExecMakeFunctionResultSet
    // call. (Miri F6, notes/miri-pilot-lane.md.)
    srf.fcinfo.resultinfo = srf.rsinfo.as_fmnode_ptr();
    let result = srf.flinfo.invoke(&mut srf.fcinfo)?;
    if let Some(fcu) = &fcu {
        ::pgstat::function::pgstat_end_function_usage(
            fcu,
            srf.rsinfo.isDone != ExprDoneCond::ExprMultipleResult,
        );
    }

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
        SetFunctionReturnMode::Materialize => {
            if srf.rsinfo.isDone != ExprDoneCond::ExprSingleResult || !srf.flinfo.fn_retset {
                return Err(materialize_violated());
            }
            match srf.rsinfo.setResult.take() {
                Some(set_result) => {
                    let mut store = *set_result
                        .downcast::<::tuplestore::Tuplestore>()
                        .expect("rsinfo.setResult downcasts to Tuplestore");
                    store.rescan()?;
                    srf.result_store = Some(store);
                    // C: a RECORD SRF's read slot comes from rsinfo.setDesc.
                    if srf.result_slot.is_none() {
                        let Some(set_desc) = srf.rsinfo.setDesc else {
                            return Err(setof_record_not_accepted());
                        };
                        // SAFETY: setDesc contract — live for this call; the
                        // copy owns its storage in the query context.
                        let src = unsafe {
                            set_desc.cast::<::types_tuple::TupleDescData<'_>>().as_ref()
                        };
                        let d = Rc::new(::tupdesc::CreateTupleDescCopy(query_mcx, src)?);
                        srf.result_slot = Some(::exectuples::make_tuple_table_slot(
                            query_mcx,
                            TupleSlotKind::MinimalTuple,
                            Some(d),
                        ));
                    }
                    read_result_store(srf, per_tuple, query_mcx)
                }
                None => Ok((Datum::null(), true, ExprDoneCond::ExprEndResult)),
            }
        }
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn setof_record_not_accepted() -> Box<PgError> {
    Box::new(
        PgError::error(
            "function returning setof record called in context that cannot accept type record",
        )
        .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn materialize_violated() -> Box<PgError> {
    Box::new(
        PgError::error("table-function protocol for materialize mode was not followed")
            .with_sqlstate(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
    )
}

#[track_caller]
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
    shutdown_srf_elems(node)?;
    exec_end_node(&mut node.outer, estate)
}

// ShutdownExprContext(isCommit=true) for callbacks the callee planted via
// rsinfo.srf_shutdown; must run before resowner release checks. Abort paths
// skip it (C isCommit=false; resowner reclaims).
fn shutdown_srf_elems(node: &mut ProjectSetState<'_>) -> PgResult<()> {
    for elem in node.elems.iter_mut() {
        if let Elem::Srf(srf) = elem {
            if let Some(f) = srf.rsinfo.srf_shutdown.take() {
                f(&mut srf.flinfo)?;
            }
        }
    }
    Ok(())
}

/// `ExecReScanProjectSet` (nodeProjectSet.c). C's ReScanExprContext fires
/// shutdown_MultiFuncCall to drop cross-call SRF state; the fn_extra Box
/// drops here instead, and setArgsValid resets with it (ShutdownSetExpr).
pub fn exec_re_scan_project_set_local(node: &mut ProjectSetState<'_>) -> PgResult<()> {
    shutdown_srf_elems(node)?;
    node.pending_srf_tuples = false;
    let ProjectSetState { elems, elemdone, .. } = node;
    for (i, elem) in elems.iter_mut().enumerate() {
        if let Elem::Srf(srf) = elem {
            srf.args_valid = false;
            srf.flinfo.fn_extra = None;
            srf.result_store = None;
            elemdone[i] = ExprDoneCond::ExprSingleResult;
        }
    }
    Ok(())
}

pub(crate) fn release_project_set(node: &mut ProjectSetState<'_>) {
    node.elems.clear();
}

// Exempt: elems (compiled ExprStates + FmgrInfo fn_extra Boxes) released in
// release_owned; elemdone is drop-free (foreign Copy enum, uncensusable here).
::mcx::forget_safe_struct!(
    ProjectSetState<'_> { ps, outer, pending_srf_tuples; elems, elemdone },
);
