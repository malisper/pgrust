//! `execExpr-func-subscript` family — function-call, whole-row, subscripting
//! and SubPlan expression init/eval.
//!
//! Owns `ExecInitFunc` (the FUNCEXPR strict/fusage classification that picks
//! the `EEOP_FUNCEXPR*` opcode), `ExecInitSubPlanExpr`, `ExecInitWholeRowVar`,
//! `ExecInitSubscriptingRef` (+ `isAssignmentIndirectionExpr`), and the SubPlan
//! exec-state seams nodeSubplan drives (`testexpr` init/eval, the
//! `projLeft`/`projRight` projections and their result-slot accessors).
//!
//! The two projections and the combining expression's compiled state are
//! execExpr-owned machinery that `SubPlanState` keeps in inherited-opaque
//! [`Opaque`] slots (`projLeft`/`projRight`/`testexpr`). The writer
//! ([`execExpr_domain_agg::build_hash_projections_and_exprs`] for the hashed
//! path and [`sub_init_testexpr`] for the combining expression) stores a
//! [`ProjCarrier`] / [`TestExprCarrier`] in those slots; the seams here read
//! them back and route the heavy lifting — `ExecProject`,
//! `ExecEvalExprSwitchContext`, `ExecClearTuple`, `slot_getattr`,
//! `slot_attisnull` — through the owning unit's seam (execExpr-core for the
//! projection/eval, execTuples for the slot accessors).

extern crate alloc;

use ::types_core::AttrNumber;
use ::types_error::PgResult;
use ::nodes::execexpr::{ExprState, ProjectionInfo, SubPlanState};
use ::nodes::execnodes::Opaque;
use ::nodes::primnodes::{etag, Expr};
use nodes::{EStateData, EcxtId, SlotId};

use execExpr_seams::{ProjectionKind, SlotAttr};
use nodeSubplan_seams as nodesubplan;

// --- inherited-opaque carriers ------------------------------------------------
//
// `projLeft`/`projRight`/`testexpr` are `Opaque` (`Box<dyn Any + 'static>`) on
// `SubPlanState` because execExpr/execGrouping were unported when the node type
// was carved. The real payloads carry the EState arena lifetime `'mcx`, which
// `dyn Any`'s `'static` bound forbids; like the partition-pruning plan-data
// carried through `Opaque` in execPartition, the payload is built and dropped
// inside the EState's per-query context, so we erase the lifetime on store and
// reinstate the caller's `'mcx` on borrow. A carrier is only ever read back by
// this owning unit, threaded with the same `&mut EStateData<'mcx>` it was built
// under.

/// `ProjectionInfo *` payload for a `projLeft`/`projRight` `Opaque` slot, plus
/// the EState slot-pool id of `pi_state.resultslot` (the projection's output
/// slot, allocated by `ExecBuildProjectionInfo`). The result slot lives in the
/// EState slot pool in the owned model, so its id is what the slot accessors
/// (`slot_getattr` / `slot_attisnull` / `ExecClearTuple` / natts) address.
pub struct ProjCarrier<'mcx> {
    /// The compiled `ProjectionInfo` (`projLeft`/`projRight`), allocated in the
    /// EState's per-query context.
    pub proj: mcx::PgBox<'mcx, ProjectionInfo<'mcx>>,
    /// `node->proj*->pi_state.resultslot` — the result slot's pool id.
    pub resultslot: SlotId,
}

/// `ExprState *` payload for a `testexpr` (or any combining-expression)
/// `Opaque` slot.
pub struct TestExprCarrier<'mcx> {
    /// The compiled combining `ExprState`, allocated in the EState's per-query
    /// context.
    pub state: mcx::PgBox<'mcx, ExprState<'mcx>>,
}

/// Store a freshly built `ProjCarrier` into a projection's `Opaque` slot,
/// erasing the arena lifetime (see the module note). Used by the projection
/// builder (`build_hash_projections_and_exprs`).
pub fn store_proj_carrier(slot: &mut Opaque, carrier: ProjCarrier<'_>) {
    // SAFETY: the payload's `'mcx` allocations outlive every read, which only
    // happens while the owning EState is alive and threaded back in; the
    // lifetime is reinstated on borrow.
    let erased: ProjCarrier<'static> = unsafe { core::mem::transmute(carrier) };
    *slot = Opaque(Some(alloc::boxed::Box::new(erased)));
}

/// Store a freshly built `TestExprCarrier` into a `testexpr` `Opaque` slot.
pub fn store_testexpr_carrier(slot: &mut Opaque, carrier: TestExprCarrier<'_>) {
    let erased: TestExprCarrier<'static> = unsafe { core::mem::transmute(carrier) };
    *slot = Opaque(Some(alloc::boxed::Box::new(erased)));
}

/// Borrow the `ProjCarrier` out of a projection `Opaque` slot, reinstating the
/// caller's `'mcx`. Panics loudly (the inherited-opaque contract) if the slot
/// is NULL or holds the wrong payload.
fn proj_carrier<'a, 'mcx>(slot: &'a Opaque, which: ProjectionKind) -> &'a ProjCarrier<'mcx> {
    let any = slot
        .0
        .as_ref()
        .unwrap_or_else(|| panic!("SubPlanState {which:?} projection not built"));
    let erased = any
        .downcast_ref::<ProjCarrier<'static>>()
        .unwrap_or_else(|| panic!("SubPlanState {which:?} projection is not a ProjCarrier"));
    // SAFETY: reinstate the arena lifetime erased on store.
    unsafe { core::mem::transmute::<&ProjCarrier<'static>, &ProjCarrier<'mcx>>(erased) }
}

/// Mutable form of [`proj_carrier`].
fn proj_carrier_mut<'a, 'mcx>(
    slot: &'a mut Opaque,
    which: ProjectionKind,
) -> &'a mut ProjCarrier<'mcx> {
    let any = slot
        .0
        .as_mut()
        .unwrap_or_else(|| panic!("SubPlanState {which:?} projection not built"));
    let erased = any
        .downcast_mut::<ProjCarrier<'static>>()
        .unwrap_or_else(|| panic!("SubPlanState {which:?} projection is not a ProjCarrier"));
    unsafe { core::mem::transmute::<&mut ProjCarrier<'static>, &mut ProjCarrier<'mcx>>(erased) }
}

/// Borrow the `TestExprCarrier` out of the `testexpr` `Opaque` slot.
fn testexpr_carrier<'a, 'mcx>(slot: &'a Opaque) -> &'a TestExprCarrier<'mcx> {
    let any = slot
        .0
        .as_ref()
        .expect("SubPlanState testexpr not built");
    let erased = any
        .downcast_ref::<TestExprCarrier<'static>>()
        .expect("SubPlanState testexpr is not a TestExprCarrier");
    unsafe { core::mem::transmute::<&TestExprCarrier<'static>, &TestExprCarrier<'mcx>>(erased) }
}

/// Mutably borrow the `TestExprCarrier` out of the `testexpr` `Opaque` slot.
fn testexpr_carrier_mut<'a, 'mcx>(slot: &'a mut Opaque) -> &'a mut TestExprCarrier<'mcx> {
    let any = slot
        .0
        .as_mut()
        .expect("SubPlanState testexpr not built");
    let erased = any
        .downcast_mut::<TestExprCarrier<'static>>()
        .expect("SubPlanState testexpr is not a TestExprCarrier");
    unsafe {
        core::mem::transmute::<&mut TestExprCarrier<'static>, &mut TestExprCarrier<'mcx>>(erased)
    }
}

/// Drain the aggregate/window-function discovery channels of a sub-expression
/// `ExprState` (a SubPlan's `projLeft` / `projRight` / `testexpr`) into the
/// enclosing parent `ExprState`.
///
/// In C, `ExecInitSubPlan` builds these projections with the SAME parent
/// `PlanState` (`ExecBuildProjectionInfo(lefttlist, NULL, slot, parent, NULL)`),
/// so any `Aggref`/`WindowFunc` found while compiling the SubPlan's lefthand
/// args is appended directly onto `parent->aggs` / `parent->funcs`. The owned
/// model builds each projection as a self-contained `ExprState` whose
/// discovery lands on its own `pi_state.found_aggs` / `found_window_funcs`; we
/// must hoist those into the enclosing compile's `state` so the parent nodeAgg
/// (`numaggs = max_aggno + 1`) / nodeWindowAgg sees them. Without this, an
/// aggregate that appears ONLY inside a SubPlan's LHS (e.g.
/// `(1 = any(array_agg(f1))) = any (select ...)`) is invisible to nodeAgg,
/// leaving `numaggs = 0` and an empty `ecxt_aggvalues`.
fn drain_subexpr_found_channels<'mcx>(
    src: &mut ExprState<'mcx>,
    dst: &mut ExprState<'mcx>,
) {
    if let Some(aggs) = src.found_aggs.take() {
        match dst.found_aggs.as_mut() {
            Some(d) => {
                for a in aggs {
                    d.push(a);
                }
            }
            None => dst.found_aggs = Some(aggs),
        }
    }
    if let Some(wfs) = src.found_window_funcs.take() {
        match dst.found_window_funcs.as_mut() {
            Some(d) => {
                for w in wfs {
                    d.push(w);
                }
            }
            None => dst.found_window_funcs = Some(wfs),
        }
    }
}

/// Hoist the agg/window discovery channels collected while building a freshly
/// built `SubPlanState`'s `projLeft` / `projRight` / `testexpr` sub-expressions
/// into the enclosing compile's `state` (see [`drain_subexpr_found_channels`]).
fn drain_subplan_found_channels<'mcx>(
    sstate: &mut SubPlanState<'mcx>,
    state: &mut ExprState<'mcx>,
) {
    if sstate.projLeft.0.is_some() {
        let carrier = proj_carrier_mut(&mut sstate.projLeft, ProjectionKind::Left);
        drain_subexpr_found_channels(&mut carrier.proj.pi_state, state);
    }
    if sstate.projRight.0.is_some() {
        let carrier = proj_carrier_mut(&mut sstate.projRight, ProjectionKind::Right);
        drain_subexpr_found_channels(&mut carrier.proj.pi_state, state);
    }
    if sstate.testexpr.0.is_some() {
        let carrier = testexpr_carrier_mut(&mut sstate.testexpr);
        drain_subexpr_found_channels(&mut carrier.state, state);
    }
}

/// Pick the named projection's `Opaque` slot off the node.
fn proj_slot<'a, 'mcx>(node: &'a SubPlanState<'mcx>, which: ProjectionKind) -> &'a Opaque {
    match which {
        ProjectionKind::Left => &node.projLeft,
        ProjectionKind::Right => &node.projRight,
    }
}

/// Mutable form of [`proj_slot`].
fn proj_slot_mut<'a, 'mcx>(
    node: &'a mut SubPlanState<'mcx>,
    which: ProjectionKind,
) -> &'a mut Opaque {
    match which {
        ProjectionKind::Left => &mut node.projLeft,
        ProjectionKind::Right => &mut node.projRight,
    }
}

/// `sstate->testexpr = ExecInitExpr((Expr *) subplan->testexpr, parent)`
/// (nodeSubplan.c:833) — compile the combining expression into `node->testexpr`.
pub fn sub_init_testexpr<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // C: sstate->testexpr = ExecInitExpr((Expr *) subplan->testexpr, parent);
    //
    // A SubPlan with a NULL testexpr (e.g. EXISTS/EXPR/ARRAY sublinks) leaves
    // testexpr NULL — ExecInitExpr(NULL, ...) returns NULL. Mirror that: an
    // absent combining expression leaves the Opaque slot NULL.
    let subplan = node
        .subplan
        .as_ref()
        .expect("SubPlanState.subplan is NULL in ExecInitSubPlanExpr");
    let testexpr = match subplan.testexpr.as_ref() {
        Some(e) => e,
        None => {
            node.testexpr = Opaque::default();
            return Ok(());
        }
    };

    // The parent PlanState ExecInitExpr threads through is the SubPlan's parent
    // expression context (C `sstate->testexpr = ExecInitExpr(testexpr, parent)`).
    // The owned `exec_init_expr` ignores the `parent` head — it back-fills the
    // `ExprState.parent` via `stamp_expr_parents` and reaches the `EState`
    // through the explicitly-threaded `estate` (stamped onto `es_link`) — and a
    // combining `testexpr` is only PARAM_EXEC references over the sub-select's
    // output columns (no Var/SubPlan that would consult the parent head). So a
    // throwaway `PlanStateData` head satisfies the signature faithfully. (The
    // C `sstate->planstate` aliasing field is not materialized in the owned
    // model — the child plan state is owned by `es_subplanstates` and reached by
    // `plan_id` index at run time, never through `node.planstate`.)
    let mut parent_head = ::nodes::execnodes::PlanStateData::default();

    // `ExecInitExpr` is owned by execExpr-core; route through its seam.
    let state = crate::execExpr_core::exec_init_expr(testexpr, &mut parent_head, estate)?;

    store_testexpr_carrier(&mut node.testexpr, TestExprCarrier { state });
    Ok(())
}

/// `ExecProject(node->projLeft|projRight)` (nodeSubplan.c:133-134 / :605).
pub fn sub_exec_project<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
    which: ProjectionKind,
) -> PgResult<()> {
    // C (projLeft path): node->projLeft->pi_exprContext = econtext;
    //                    slot = ExecProject(node->projLeft);
    // C (projRight path): slot = ExecProject(node->projRight);  // uses the
    //                    node's own innerecontext, set at init — `econtext`
    //                    is ignored on this side (see seam doc).
    {
        let carrier = proj_carrier_mut(proj_slot_mut(node, which), which);
        match which {
            ProjectionKind::Left => {
                // hack alert (per C): retarget the lefthand projection at the
                // supplied per-tuple econtext before projecting.
                carrier.proj.pi_exprContext = Some(econtext);
            }
            ProjectionKind::Right => { /* projRight keeps its innerecontext */ }
        }
    }

    // `ExecProject` is owned by execExpr-core; route through its seam over the
    // explicit ProjectionInfo. The result lands in the projection's result
    // slot (its pool id is `carrier.resultslot`).
    let carrier = proj_carrier_mut(proj_slot_mut(node, which), which);
    let _resultslot = crate::execExpr_core::exec_project_info(&mut carrier.proj, estate)?;
    Ok(())
}

/// The `SlotId` of the named projection's result slot
/// (`node->proj*->pi_state.resultslot`).
pub fn sub_proj_result_slot_id<'mcx>(
    node: &SubPlanState<'mcx>,
    _estate: &EStateData<'mcx>,
    which: ProjectionKind,
) -> SlotId {
    proj_carrier(proj_slot(node, which), which).resultslot
}

/// `ExecClearTuple(node->proj*->pi_state.resultslot)`.
pub fn sub_clear_proj_result_slot<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    which: ProjectionKind,
) -> PgResult<()> {
    // C: ExecClearTuple(slot);  /* the projection's result slot */
    let slot_id = proj_carrier(proj_slot(node, which), which).resultslot;
    // `ExecClearTuple` over a pool slot is owned by execTuples; the result slot
    // lives in the EState slot pool, addressed by its id.
    execTuples_seams::exec_clear_tuple::call(estate, slot_id)
}

/// `slot->tts_tupleDescriptor->natts` of the named projection's result slot
/// (`slotAllNulls`/`slotNoNulls`).
pub fn proj_result_slot_natts(
    node: &SubPlanState<'_>,
    estate: &EStateData<'_>,
    which: ProjectionKind,
) -> i32 {
    // C: int ncols = slot->tts_tupleDescriptor->natts;
    let slot_id = proj_carrier(proj_slot(node, which), which).resultslot;
    // natts of a pool slot is owned by execTuples; route through its seam.
    execTuples_seams::slot_natts::call(estate, slot_id)
}

/// `slot_attisnull(slot, attnum)` over the named projection's result slot
/// (`slotAllNulls`/`slotNoNulls`).
pub fn proj_result_slot_attisnull<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    which: ProjectionKind,
    attnum: i32,
) -> PgResult<bool> {
    // C: slot_attisnull(slot, i)  — which deforms up to `i` and reads
    // tts_isnull[i-1]; modeled here as slot_getattr's isnull over the pool slot
    // (`slot_getattr` is the deforming accessor; slot_attisnull is its is-null
    // half).
    let slot_id = proj_carrier(proj_slot(node, which), which).resultslot;
    let attr =
        execTuples_seams::slot_getattr_by_id::call(estate, slot_id, attnum as AttrNumber)?;
    Ok(attr.isnull)
}

/// `slot_getattr(node->projLeft result slot, att, &isnull)`
/// (`execTuplesUnequal` `slot1`).
pub fn proj_left_slot_getattr<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    att: AttrNumber,
) -> PgResult<SlotAttr<'mcx>> {
    // C: attr1 = slot_getattr(slot1, att, &isNull1);  // slot1 == projLeft slot
    let slot_id = proj_carrier(proj_slot(node, ProjectionKind::Left), ProjectionKind::Left)
        .resultslot;
    let attr = execTuples_seams::slot_getattr_by_id::call(estate, slot_id, att)?;
    Ok(SlotAttr {
        value: attr.value,
        isnull: attr.isnull,
    })
}

/// `ExecEvalExprSwitchContext(node->testexpr, econtext, &rownull)`
/// (nodeSubplan.c:399).
pub fn eval_testexpr_switch_context<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
) -> PgResult<(types_tuple::heaptuple::Datum<'mcx>, bool)> {
    // C: rowresult = ExecEvalExprSwitchContext(node->testexpr, econtext,
    //                                          &rownull);
    let carrier = testexpr_carrier_mut(&mut node.testexpr);
    // `ExecEvalExprSwitchContext` is owned by execExpr-core; route through its
    // seam over the compiled combining ExprState.
    crate::execExpr_core::exec_eval_expr_switch_context(&mut carrier.state, econtext, estate)
}

// ===========================================================================
// ExecInitExprRec helper functions — execExpr.c's own compilation routines for
// FuncExpr/OpExpr, WholeRowVar, SubscriptingRef and SubPlan nodes. These are
// invoked by the (execExpr-core-owned) `ExecInitExprRec` dispatch switch once it
// learns to model the corresponding `Expr` enum variants. They emit
// `ExprEvalStep`s into the `ExprState` under construction.
//
// Result-location reality (see execExpr_core's module note): C threads raw
// `Datum *resv`/`bool *resnull` pointers through `ExecInitExprRec`, and these
// routines recurse into *distinct* output cells (`&fcinfo->args[i].value`,
// `&sbsrefstate->upperindex[i]`, `&sbsrefstate->replacevalue`, …). The owned
// keystone models each such cell as a `ResultCellId` into the `ExprState`'s
// `ResultCellArena`, allocated by `new_result_cell` and named on the step;
// `exec_init_expr_rec` takes that target and is crate-exported. `ExecInitFunc`
// below uses exactly this: a fresh arena cell per argument (recursed into for a
// non-Const arg, written directly for a Const) recorded in `Func.arg_cells`.
// `ExecInitSubscriptingRef` still parks on a genuine cross-unit owner
// (`getSubscriptingRoutines`), and `ExecInitSubPlanExpr` on nodeSubplan; those
// keep the full faithful C structure for the logic this family owns and route
// only the genuinely-unported owner calls to a loud panic ("mirror PG and
// panic").

/// `#define FUNC_MAX_ARGS 100` (pg_config_manual.h).
const FUNC_MAX_ARGS: i32 = ::types_core::primitive::FUNC_MAX_ARGS as i32;

/// `isAssignmentIndirectionExpr(expr)` (execExpr.c:3489) — recognize a nested
/// assignment-indirection expression: `FieldStore`/`SubscriptingRef` whose
/// innermost arg is a `CaseTestExpr` (the placeholder the planner installs for
/// the to-be-modified container), peeling `CoerceToDomain`/`RelabelType`. A
/// fully faithful port — pure node inspection, no step emission.
pub(crate) fn is_assignment_indirection_expr(expr: Option<&Expr>) -> bool {
    let expr = match expr {
        // C: if (expr == NULL) return false;
        None => return false,
        Some(e) => e,
    };
    match expr.expr_tag() {
        // C: if (IsA(expr, FieldStore)) { ... if arg is CaseTestExpr ... }
        etag::T_FieldStore => {
            let fstore = expr.expect_fieldstore();
            if matches!(
                fstore.arg.as_deref().map(|a| a.expr_tag()),
                Some(etag::T_CaseTestExpr)
            ) {
                return true;
            }
        }
        // C: else if (IsA(expr, SubscriptingRef)) { ... if refexpr is CaseTestExpr }
        etag::T_SubscriptingRef => {
            let sbs_ref = expr.expect_subscriptingref();
            if matches!(
                sbs_ref.refexpr.as_deref().map(|a| a.expr_tag()),
                Some(etag::T_CaseTestExpr)
            ) {
                return true;
            }
        }
        // C: else if (IsA(expr, CoerceToDomain)) return recurse(cd->arg);
        etag::T_CoerceToDomain => {
            let cd = expr.expect_coercetodomain();
            return is_assignment_indirection_expr(cd.arg.as_deref());
        }
        // C: else if (IsA(expr, RelabelType)) return recurse(r->arg);
        etag::T_RelabelType => {
            let r = expr.expect_relabeltype();
            return is_assignment_indirection_expr(r.arg.as_deref());
        }
        _ => {}
    }
    // C: return false;
    false
}

/// `ExecInitFunc(scratch, node, args, funcid, inputcollid, state)`
/// (execExpr.c:2704) — set up the [`Func`] step for a function/operator call:
/// ACL-check `funcid`, look up its `FmgrInfo`/`FunctionCallInfo`, recurse each
/// non-Const argument into its `fcinfo->args[i]` cell, then pick the
/// `EEOP_FUNCEXPR*` opcode by strictness × pg_stat_function-tracking.
///
/// [`Func`]: ::nodes::execexpr::ExprEvalStepData::Func
pub(crate) fn exec_init_func<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    scratch: &mut ::nodes::execexpr::ExprEvalStep<'mcx>,
    node: &Expr<'mcx>,
    args: &[Expr<'mcx>],
    funcid: ::types_core::Oid,
    inputcollid: ::types_core::Oid,
    state: &mut ExprState<'mcx>,
) -> PgResult<()> {
    use ::nodes::execexpr::{ExprEvalOp, ExprEvalStepData, ResultCell};

    // C: int nargs = list_length(args);
    let nargs = args.len() as i32;

    // C: aclresult = object_aclcheck(ProcedureRelationId, funcid, GetUserId(),
    //                                ACL_EXECUTE);
    //    if (aclresult != ACLCHECK_OK)
    //        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(funcid));
    //    InvokeFunctionExecuteHook(funcid);
    //
    // GetUserId() (miscinit), object_aclcheck/aclcheck_error (catalog ACL) and
    // the function-execute object-access hook (objectaccess) are cross-unit
    // owners, each routed through its seam.
    let aclresult = aclchk_seams::object_aclcheck::call(
        parsenodes::ProcedureRelationId,
        funcid,
        miscinit_seams::get_user_id::call(),
        types_acl::ACL_EXECUTE,
    )?;
    if aclresult != types_acl::ACLCHECK_OK {
        // C: aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(funcid));
        let funcname = lsyscache_seams::get_func_name::call(mcx, funcid)?
            .map(|s| s.to_string());
        aclchk_seams::aclcheck_error::call(
            aclresult,
            ::nodes::parsenodes::OBJECT_FUNCTION,
            funcname,
        )?;
    }
    // C: InvokeFunctionExecuteHook(funcid);
    objectaccess::invoke_function_execute_hook(funcid)?;

    // C: Safety check on nargs (parser should already have caught this).
    if nargs > FUNC_MAX_ARGS {
        // C: ereport(ERROR, errcode(ERRCODE_TOO_MANY_ARGUMENTS),
        //            errmsg_plural("cannot pass more than %d argument(s) to a function",
        //                          ..., FUNC_MAX_ARGS, FUNC_MAX_ARGS));
        return Err(::types_error::PgError::error(format!(
            "cannot pass more than {FUNC_MAX_ARGS} arguments to a function"
        ))
        .with_sqlstate(::types_error::ERRCODE_TOO_MANY_ARGUMENTS));
    }

    // C: scratch->d.func.finfo = palloc0(sizeof(FmgrInfo));
    //    scratch->d.func.fcinfo_data = palloc0(SizeForFunctionCallInfo(nargs));
    //    flinfo = scratch->d.func.finfo;  fcinfo = scratch->d.func.fcinfo_data;
    //
    // C: fmgr_info(funcid, flinfo);  fmgr_info_set_expr((Node *) node, flinfo);
    //
    // `fmgr_info` resolves funcid through the fmgr seam into the (trimmed)
    // FmgrInfo carrying fn_oid/fn_strict/fn_retset/fn_stats/fn_addr. The C
    // FmgrInfo embeds the native call address; the seam returns fn_addr as an
    // opaque address word (0 = unresolved), and the owned interpreter
    // re-dispatches by fn_oid at call time (the fmgr-seam contract), so the
    // step's typed `fn_addr: Option<PGFunction>` stays None — the Func step
    // carries `finfo` (with fn_oid) for the interpreter to re-resolve.
    // `fmgr_info_set_expr` stashes the call node on flinfo->fn_expr (carried
    // erased on the FmgrInfo); polymorphic-type resolution reads it back at call
    // time through `get_fn_expr_rettype`/`get_fn_expr_argtype`. The by-OID fmgr
    // dispatch re-resolves the FmgrInfo and so would drop this; the interpreter
    // re-threads the step's fn_expr to the callee at call time (see
    // `func_step_fn_expr`).
    let mut flinfo = fmgr_seams::fmgr_info::call(mcx, funcid)?;
    fmgr_seams::fmgr_info_set_expr::call(mcx, &mut flinfo, node)?;

    // C: InitFunctionCallInfoData(*fcinfo, flinfo, nargs, inputcollid, NULL, NULL);
    //    scratch->d.func.fn_addr = flinfo->fn_addr;  scratch->d.func.nargs = nargs;
    //
    // `InitFunctionCallInfoData(Fcinfo, Flinfo, Nargs, Collation, Context,
    // Resultinfo)` stamps flinfo / fncollation = Collation / nargs / NULL
    // context / NULL resultinfo / isnull = false onto the call frame. The
    // arguments the C threads into fcinfo->args[i] are carried instead by the
    // Func step's `arg_cells` (one ResultCellId per arg), which the interpreter
    // gathers into `fcinfo->args` immediately before dispatch; we leave `args`
    // empty here (the palloc0 frame). #296: the widened frame now records
    // `inputcollid` as `fcinfo->fncollation`, surviving to call time.
    let fcinfo_data = mcx::alloc_in(
        mcx,
        ::nodes::fmgr::FunctionCallInfoBaseData {
            // C: the frame points at the one `flinfo`; the owned frame carries
            // an `FmgrInfo` copy. `FmgrInfo` is no longer `Copy` (it carries the
            // erased `fn_expr`), so clone the lookup info into the frame and keep
            // the original for the Func step below.
            flinfo: Some(flinfo.clone()),
            context: None,
            resultinfo: None,
            fncollation: inputcollid,
            isnull: false,
            nargs: nargs as i16,
            args: Vec::new(),
            // Value-per-call SRF channel (#349): unused here (non-set function).
            ..Default::default()
        },
    )?;

    // C: We only support non-set functions here.
    //    if (flinfo->fn_retset) ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED,
    //        "set-valued function called in context that cannot accept a set");
    if flinfo.fn_retset {
        return Err(::types_error::PgError::error(
            "set-valued function called in context that cannot accept a set",
        )
        .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // C: Build code to evaluate arguments directly into the fcinfo struct.
    //    argno = 0;
    //    foreach(lc, args) {
    //        Expr *arg = (Expr *) lfirst(lc);
    //        if (IsA(arg, Const)) {
    //            Const *con = (Const *) arg;
    //            fcinfo->args[argno].value = con->constvalue;
    //            fcinfo->args[argno].isnull = con->constisnull;
    //        } else {
    //            ExecInitExprRec(arg, state, &fcinfo->args[argno].value,
    //                            &fcinfo->args[argno].isnull);
    //        }
    //        argno++;
    //    }
    //
    // In the owned model each argument gets its own result cell; the cell id is
    // recorded in `arg_cells[argno]` (the interpreter gathers these into the
    // call frame). A non-Const arg compiles a step sequence that writes its
    // cell each round; a Const arg (the C optimization that avoids re-evaluating
    // constants every round) emits no step — its value is written directly into
    // the cell, where it persists (the cell is never reused), exactly like C's
    // one-time write into fcinfo->args[argno].
    let mut arg_cells = mcx::vec_with_capacity_in(mcx, nargs as usize)?;
    for arg in args {
        let cell = crate::execExpr_core::new_result_cell(mcx, state)?;
        if let Expr::Const(con) = arg {
            // C: fcinfo->args[argno].value = con->constvalue;
            //    fcinfo->args[argno].isnull = con->constisnull;
            state.result_cells.set(
                cell,
                ResultCell {
                    value: con.constvalue.clone(),
                    isnull: con.constisnull,
                },
            );
        } else {
            // C: ExecInitExprRec(arg, state, &fcinfo->args[argno].value,
            //                    &fcinfo->args[argno].isnull);
            crate::execExpr_core::exec_init_expr_rec(mcx, arg, state, cell)?;
        }
        arg_cells.push(cell);
    }

    // C: Insert appropriate opcode depending on strictness and stats level.
    //    if (pgstat_track_functions <= flinfo->fn_stats) {
    //        if (flinfo->fn_strict && nargs > 0) {
    //            if (nargs == 1) opcode = EEOP_FUNCEXPR_STRICT_1;
    //            else if (nargs == 2) opcode = EEOP_FUNCEXPR_STRICT_2;
    //            else opcode = EEOP_FUNCEXPR_STRICT;
    //        } else opcode = EEOP_FUNCEXPR;
    //    } else {
    //        if (flinfo->fn_strict && nargs > 0) opcode = EEOP_FUNCEXPR_STRICT_FUSAGE;
    //        else opcode = EEOP_FUNCEXPR_FUSAGE;
    //    }
    let track_functions = guc_tables::vars::pgstat_track_functions.read();
    let opcode = if track_functions <= i32::from(flinfo.fn_stats) {
        if flinfo.fn_strict && nargs > 0 {
            // Choose nargs-optimized implementation if available.
            match nargs {
                1 => ExprEvalOp::EEOP_FUNCEXPR_STRICT_1,
                2 => ExprEvalOp::EEOP_FUNCEXPR_STRICT_2,
                _ => ExprEvalOp::EEOP_FUNCEXPR_STRICT,
            }
        } else {
            ExprEvalOp::EEOP_FUNCEXPR
        }
    } else if flinfo.fn_strict && nargs > 0 {
        ExprEvalOp::EEOP_FUNCEXPR_STRICT_FUSAGE
    } else {
        ExprEvalOp::EEOP_FUNCEXPR_FUSAGE
    };

    // Stamp the scratch step (the caller pushes it). `make_ro` is false for an
    // ordinary FuncExpr/OpExpr (only NULLIF sets it; that arm lives in the
    // core dispatch). `fn_addr` stays None — the interpreter re-resolves by
    // `finfo.fn_oid` (the fmgr-seam contract).
    scratch.opcode = opcode;
    scratch.d = ExprEvalStepData::Func {
        finfo: Some(mcx::alloc_in(mcx, flinfo)?),
        fcinfo_data: Some(fcinfo_data),
        arg_cells: Some(arg_cells),
        fn_addr: None,
        nargs,
        make_ro: false,
    };

    Ok(())
}

/// `case T_ScalarArrayOpExpr:` (execExpr.c:1266) — compile a
/// `scalar op ANY/ALL (array)` test. Selects the comparison function
/// (`negfuncid` for hashed NOT IN, else `opfuncid`), ACL-checks it (and the
/// hash function when hashed), resolves the fmgr lookup, builds the 2-arg call
/// frame, then evaluates the scalar into `fcinfo->args[0]` (a fresh cell) and
/// the array into the step's result cell (`resv`) before pushing either an
/// `EEOP_HASHED_SCALARARRAYOP` (when `hashfuncid` is set) or `EEOP_SCALARARRAYOP`
/// step.
pub(crate) fn exec_init_scalar_array_op<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    scratch: &mut ::nodes::execexpr::ExprEvalStep<'mcx>,
    opexpr: &::nodes::primnodes::ScalarArrayOpExpr<'mcx>,
    state: &mut ExprState<'mcx>,
    resv: ::nodes::execexpr::ResultCellId,
) -> PgResult<()> {
    use ::nodes::execexpr::{ExprEvalOp, ExprEvalStepData};

    // C: Select the correct comparison function. For hashed NOT IN the opfuncid
    //    is the inequality function and negfuncid is the equality function we
    //    must use for hash probes.
    let cmpfuncid = if opexpr.negfuncid != ::types_core::InvalidOid {
        debug_assert!(opexpr.hashfuncid != ::types_core::InvalidOid);
        opexpr.negfuncid
    } else {
        opexpr.opfuncid
    };

    // C: Assert(list_length(opexpr->args) == 2);
    //    scalararg = linitial(opexpr->args); arrayarg = lsecond(opexpr->args);
    debug_assert_eq!(opexpr.args.len(), 2);
    let scalararg = &opexpr.args[0];
    let arrayarg = &opexpr.args[1];

    // C: Check permission to call function (the comparison function).
    //    aclresult = object_aclcheck(...); if (!OK) aclcheck_error(...);
    //    InvokeFunctionExecuteHook(cmpfuncid);
    let aclresult = aclchk_seams::object_aclcheck::call(
        parsenodes::ProcedureRelationId,
        cmpfuncid,
        miscinit_seams::get_user_id::call(),
        types_acl::ACL_EXECUTE,
    )?;
    if aclresult != types_acl::ACLCHECK_OK {
        let funcname = lsyscache_seams::get_func_name::call(mcx, cmpfuncid)?
            .map(|s| s.to_string());
        aclchk_seams::aclcheck_error::call(
            aclresult,
            ::nodes::parsenodes::OBJECT_FUNCTION,
            funcname,
        )?;
    }
    objectaccess::invoke_function_execute_hook(cmpfuncid)?;

    // C: if (OidIsValid(opexpr->hashfuncid)) { ACL-check the hash function too. }
    if opexpr.hashfuncid != ::types_core::InvalidOid {
        let aclresult = aclchk_seams::object_aclcheck::call(
            parsenodes::ProcedureRelationId,
            opexpr.hashfuncid,
            miscinit_seams::get_user_id::call(),
            types_acl::ACL_EXECUTE,
        )?;
        if aclresult != types_acl::ACLCHECK_OK {
            let funcname =
                lsyscache_seams::get_func_name::call(mcx, opexpr.hashfuncid)?
                    .map(|s| s.to_string());
            aclchk_seams::aclcheck_error::call(
                aclresult,
                ::nodes::parsenodes::OBJECT_FUNCTION,
                funcname,
            )?;
        }
        objectaccess::invoke_function_execute_hook(opexpr.hashfuncid)?;
    }

    // C: Set up the primary fmgr lookup information.
    //    finfo = palloc0(sizeof(FmgrInfo)); fcinfo = palloc0(SizeForFunctionCallInfo(2));
    //    fmgr_info(cmpfuncid, finfo); fmgr_info_set_expr(node, finfo);
    //    InitFunctionCallInfoData(*fcinfo, finfo, 2, opexpr->inputcollid, NULL, NULL);
    let flinfo = fmgr_seams::fmgr_info::call(mcx, cmpfuncid)?;
    let fcinfo_data = mcx::alloc_in(
        mcx,
        ::nodes::fmgr::FunctionCallInfoBaseData {
            flinfo: Some(flinfo.clone()),
            context: None,
            resultinfo: None,
            fncollation: opexpr.inputcollid,
            isnull: false,
            nargs: 2,
            args: Vec::new(),
            // Value-per-call SRF channel (#349): unused here (non-set function).
            ..Default::default()
        },
    )?;

    if opexpr.hashfuncid != ::types_core::InvalidOid {
        // C (hashed path):
        //    ExecInitExprRec(scalararg, state, &fcinfo->args[0].value/.isnull);
        //    ExecInitExprRec(arrayarg, state, resv, resnull);
        let scalar_cell = crate::execExpr_core::new_result_cell(mcx, state)?;
        crate::execExpr_core::exec_init_expr_rec(mcx, scalararg, state, scalar_cell)?;
        crate::execExpr_core::exec_init_expr_rec(mcx, arrayarg, state, resv)?;

        // C: scratch.opcode = EEOP_HASHED_SCALARARRAYOP;
        //    .inclause = opexpr->useOr; .finfo = finfo; .fcinfo_data = fcinfo;
        //    .saop = opexpr;
        scratch.opcode = ExprEvalOp::EEOP_HASHED_SCALARARRAYOP;
        scratch.d = ExprEvalStepData::HashedScalarArrayOp {
            has_nulls: false,
            inclause: opexpr.useOr,
            elements_tab: None,
            finfo: Some(mcx::alloc_in(mcx, flinfo)?),
            fcinfo_data: Some(fcinfo_data),
            saop: Some(mcx::alloc_in(mcx, opexpr.clone())?),
            scalar_cell,
        };
    } else {
        // C (linear path):
        //    ExecInitExprRec(scalararg, state, &fcinfo->args[0].value/.isnull);
        //    ExecInitExprRec(arrayarg, state, resv, resnull);
        let scalar_cell = crate::execExpr_core::new_result_cell(mcx, state)?;
        crate::execExpr_core::exec_init_expr_rec(mcx, scalararg, state, scalar_cell)?;
        crate::execExpr_core::exec_init_expr_rec(mcx, arrayarg, state, resv)?;

        // C: scratch.opcode = EEOP_SCALARARRAYOP;
        //    .element_type = InvalidOid; .useOr = opexpr->useOr; .finfo = finfo;
        //    .fcinfo_data = fcinfo; .fn_addr = finfo->fn_addr;
        // fn_addr stays None (the interpreter re-resolves by finfo.fn_oid, the
        // fmgr-seam contract). The array side was evaluated into `resv`, which
        // the step deconstructs; `array_cell` records that cell.
        scratch.opcode = ExprEvalOp::EEOP_SCALARARRAYOP;
        scratch.d = ExprEvalStepData::ScalarArrayOp {
            element_type: ::types_core::InvalidOid,
            use_or: opexpr.useOr,
            typlen: 0,
            typbyval: false,
            typalign: 0,
            finfo: Some(mcx::alloc_in(mcx, flinfo)?),
            fcinfo_data: Some(fcinfo_data),
            fn_addr: None,
            scalar_cell,
            array_cell: resv,
        };
    }

    Ok(())
}

/// `ExecInitSubPlanExpr(subplan, state, resv, resnull)` (execExpr.c:2820) —
/// compile a `SubPlan` reference: recurse each `parParam`/`args` pair into the
/// param it sets, emit an `EEOP_PARAM_SET` step per pair, create the
/// `SubPlanState` (nodeSubplan) and register it on the parent, then emit the
/// `EEOP_SUBPLAN` step.
pub(crate) fn exec_init_sub_plan_expr<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    subplan: &::nodes::primnodes::SubPlan<'mcx>,
    state: &mut ExprState<'mcx>,
    resv: ::nodes::execexpr::ResultCellId,
) -> PgResult<()> {
    use ::nodes::execexpr::{ExprEvalOp, ExprEvalStepData};

    // C: ExprEvalStep scratch = {0};
    // C: if (!state->parent)
    //        elog(ERROR, "SubPlan found with no parent plan");
    //
    // C reaches the EState through `state->parent->state`. In the owned model
    // `ExprState.parent` is stamped only AFTER the enclosing PlanStateNode is
    // address-stable (`stamp_expr_parents`), so at this compile point `parent`
    // is still `None`; the faithful test for "is there a parent plan" is the
    // EState back-link the compile entry stamped (`es_link`). A standalone
    // parent-less expression compile (`ExecInitExpr(node, NULL)` /
    // `ExecInitExprWithParams`) leaves it `None`, so a SubPlan there errors
    // exactly as C's `!parent` branch does.
    if state.es_link.is_none() {
        return Err(::types_error::PgError::error(
            "SubPlan found with no parent plan",
        ));
    }

    // C: Assert(list_length(subplan->parParam) == list_length(subplan->args));
    //    forboth(l, subplan->parParam, pvar, subplan->args) {
    //        int paramid = lfirst_int(l);
    //        Expr *arg = (Expr *) lfirst(pvar);
    //        ExecInitExprRec(arg, state, resv, resnull);
    //        scratch.opcode = EEOP_PARAM_SET;
    //        scratch.resvalue = resv; scratch.resnull = resnull;
    //        scratch.d.param.paramid = paramid;
    //        scratch.d.param.paramtype = exprType((Node *) arg);
    //        ExprEvalPushStep(state, &scratch);
    //    }
    //
    // We evaluate each argument expression into resv/resnull (the shared output
    // cell) and immediately follow it with an EEOP_PARAM_SET, so reusing one
    // cell across params is safe — exactly the C rationale. `paramtype` is
    // filled for completeness (the interpreter does not use it); C calls
    // exprType(arg), but the owned model has no exprType seam threaded here, so
    // the EEOP_PARAM_SET step carries InvalidOid for paramtype, matching the C
    // comment "paramtype's not actually used".
    debug_assert_eq!(subplan.parParam.len(), subplan.args.len());
    for (l, pvar) in subplan.parParam.iter().zip(subplan.args.iter()) {
        let paramid = *l;
        let arg: &Expr = pvar;

        crate::execExpr_core::exec_init_expr_rec(mcx, arg, state, resv)?;

        let scratch = ::nodes::execexpr::ExprEvalStep {
            opcode: ExprEvalOp::EEOP_PARAM_SET,
            resvalue: resv,
            resnull: resv,
            d: ExprEvalStepData::Param {
                paramid,
                // C: scratch.d.param.paramtype = exprType((Node *) arg);
                // (declared "not actually used"). No exprType seam threaded
                // here; carry InvalidOid.
                paramtype: ::types_core::InvalidOid,
            },
        };
        crate::execExpr_core::expr_eval_push_step(mcx, state, scratch)?;
    }

    // C: sstate = ExecInitSubPlan(subplan, state->parent);
    //    state->parent->subPlan = lappend(state->parent->subPlan, sstate);
    //    scratch.opcode = EEOP_SUBPLAN;
    //    scratch.resvalue = resv; scratch.resnull = resnull;
    //    scratch.d.subplan.sstate = sstate;
    //    ExprEvalPushStep(state, &scratch);
    //
    // `ExecInitSubPlan` (nodeSubplan) builds the `SubPlanState` — it links to its
    // plan-state tree by 1-based `plan_id` into `es_subplanstates` and reads
    // `es_subplanstates` synchronously, so it needs the `EState`. C reaches that
    // through `state->parent->state`; the owned model has not stamped
    // `state->parent` yet (deferred to `stamp_expr_parents`), so the `EState` is
    // reached through the compile-entry-stamped `es_link` back-pointer (checked
    // non-`None` above). The built `SubPlanState` is carried directly on the
    // `EEOP_SUBPLAN` step (the owned-model equivalent of the C
    // `op->d.subplan.sstate`), where `ExecEvalSubPlan` finds it at run time. The
    // C `state->parent->subPlan = lappend(state->parent->subPlan, sstate)`: the
    // new `SubPlanState` is registered on the parent `PlanState`'s `subPlan`
    // list. That list is consumed by ExecReScan (chgParam propagation into each
    // correlated subplan's child) AND by EXPLAIN (it walks `planstate->subPlan`
    // to print each SubPlan body). In the owned model the executing
    // `SubPlanState` is single-owned on the `EEOP_SUBPLAN` step (below) and the
    // parent `PlanState` is not address-stable at compile time, so we:
    //   (a) record this SubPlan's 1-based `plan_id` on the ExprState's
    //       `found_subplan_ids` discovery channel — the compile entry point
    //       (ExecInitQual/ExecInitExpr/ExecBuildProjectionInfo) drains it into
    //       `parent.sub_plan_ids` (the owned-model split of `PlanState.subPlan`);
    //   (b) store a display-only `SubPlanState` (carrying the `SubPlan` node)
    //       into `EState.es_initplan[plan_id-1]` so EXPLAIN can resolve the
    //       `SubPlan` node for labeling. Regular-subplan `plan_id`s are disjoint
    //       from InitPlan ones (the planner assigns a single global sequence to
    //       `glob->subplans`), so this slot does not collide with any InitPlan's
    //       `es_initplan` entry, and no `init_plan_ids` list references it (so it
    //       is never mistaken for an InitPlan during param eval / rescan). The
    //       child plan-state tree itself already lives in
    //       `es_subplanstates[plan_id-1]` (filled by InitPlan in execMain).
    let owned_subplan: mcx::PgBox<'mcx, ::nodes::primnodes::SubPlan<'mcx>> =
        mcx::alloc_in(mcx, subplan.clone_in(mcx)?)?;

    // sstate = ExecInitSubPlan(subplan, state->parent->state);
    // `es_link` is Some (guarded above); reach the EState through the
    // non-owning back-pointer (the single audited deref, mirroring
    // `parent->state`).
    let mut es_link = state.es_link.expect("ExecInitSubPlanExpr: es_link present (guarded above)");
    let estate = es_link.get_mut();

    // (a) Record the plan_id on the ExprState discovery channel.
    let plan_id = subplan.plan_id;
    state
        .found_subplan_ids
        .get_or_insert_with(alloc::vec::Vec::new)
        .push(plan_id);

    // (b) Register the display-only SubPlanState into es_initplan[plan_id-1]
    // (carrying the SubPlan node for EXPLAIN labeling). Grow the slot vector to
    // cover this 1-based plan_id.
    {
        let display_sstate = ::nodes::execexpr::SubPlanState {
            subplan: Some(mcx::alloc_in(mcx, subplan.clone_in(mcx)?)?),
            ..Default::default()
        };
        let idx = (plan_id as usize).saturating_sub(1);
        while estate.es_initplan.len() <= idx {
            estate.es_initplan.push(None);
        }
        // Keep the FIRST registration for a given plan_id. The same SubPlan
        // (shared plan_id) is referenced by every inheritance/partition child's
        // qual, each carrying its own `adjust_appendrel_attrs`-translated `args`
        // (e.g. correlated `t1_1.a` for the first child, `t1_4.a` for the last).
        // EXPLAIN prints a SubPlan body once, at its first encounter in the plan
        // walk (tracked by `es->printed_subplans`); the first plan node init'd is
        // the first Append child, so its `args` must be the ones the body deparse
        // resolves. Overwriting here with a later child's copy (t1_4) would make
        // EXPLAIN deparse the subplan's correlated Var against the wrong child.
        if estate.es_initplan[idx].is_none() {
            estate.es_initplan[idx] = Some(display_sstate);
        }
    }

    let mut sstate = nodesubplan::exec_init_sub_plan::call(owned_subplan, estate)?;

    // C: ExecInitSubPlan builds projLeft/projRight/testexpr with the SAME parent
    // PlanState, so any Aggref/WindowFunc in the SubPlan's lefthand args is
    // appended onto parent->aggs / parent->funcs. The owned model builds each as
    // a self-contained ExprState; hoist its discovery channels into the enclosing
    // compile's `state` so the parent nodeAgg/nodeWindowAgg sees an aggregate (or
    // window func) that appears ONLY inside the SubPlan's LHS — e.g.
    // `(1 = any(array_agg(f1))) = any (select ...)` — otherwise numaggs stays 0
    // and ecxt_aggvalues is empty.
    drain_subplan_found_channels(&mut sstate, state);

    // scratch.opcode = EEOP_SUBPLAN; scratch.d.subplan.sstate = sstate;
    let scratch = ::nodes::execexpr::ExprEvalStep {
        opcode: ExprEvalOp::EEOP_SUBPLAN,
        resvalue: resv,
        resnull: resv,
        d: ExprEvalStepData::SubPlan {
            sstate: Some(mcx::alloc_in(mcx, sstate)?),
        },
    };
    crate::execExpr_core::expr_eval_push_step(mcx, state, scratch)?;

    Ok(())
}

/// `ExecInitWholeRowVar(scratch, variable, state)` (execExpr.c:3206) — set up an
/// `EEOP_WHOLEROW` step for a whole-row `Var`. Records the OLD/NEW
/// returning-type flags on the `ExprState`, and — for a SubqueryScan/CteScan
/// parent whose subplan emits junk columns — attaches a `JunkFilter`.
pub(crate) fn exec_init_whole_row_var<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    scratch: &mut ::nodes::execexpr::ExprEvalStep<'mcx>,
    variable: &::nodes::primnodes::Var,
    state: &mut ExprState<'mcx>,
) -> PgResult<()> {
    use ::nodes::execexpr::{
        ExprEvalOp, ExprEvalStepData, EEO_FLAG_HAS_NEW, EEO_FLAG_HAS_OLD,
    };
    use ::nodes::primnodes::VarReturningType;

    // C: scratch->opcode = EEOP_WHOLEROW;
    //    scratch->d.wholerow.var = variable;
    //    scratch->d.wholerow.first = true;
    //    scratch->d.wholerow.slow = false;
    //    scratch->d.wholerow.tupdesc = NULL;
    //    scratch->d.wholerow.junkFilter = NULL;
    scratch.opcode = ExprEvalOp::EEOP_WHOLEROW;
    scratch.d = ExprEvalStepData::WholeRow {
        // The keystone `wholerow.var` is an owned `PgBox<Var>` back-pointer;
        // mirror the C `scratch->d.wholerow.var = variable` by boxing a copy of
        // the (small, scalar-only) plan node in the EState per-query context.
        var: Some(mcx::alloc_in(mcx, variable.clone())?),
        first: true,
        slow: false,
        tupdesc: None,
        junk_filter: None,
    };

    // C: if (variable->varreturningtype == VAR_RETURNING_OLD)
    //        state->flags |= EEO_FLAG_HAS_OLD;
    //    else if (variable->varreturningtype == VAR_RETURNING_NEW)
    //        state->flags |= EEO_FLAG_HAS_NEW;
    //
    // The `Var` carries `varreturningtype` (PG18 RETURNING OLD/NEW): a whole-row
    // OLD/NEW var (e.g. RETURNING old / new over the target) must stamp the
    // EEO_FLAG_HAS_OLD/HAS_NEW flag so EEOP_WHOLEROW selects ecxt_oldtuple /
    // ecxt_newtuple at runtime (ExecEvalWholeRowVar). Mirror the C exactly.
    let varreturningtype = variable.varreturningtype;
    match varreturningtype {
        VarReturningType::VAR_RETURNING_OLD => state.flags |= EEO_FLAG_HAS_OLD,
        VarReturningType::VAR_RETURNING_NEW => state.flags |= EEO_FLAG_HAS_NEW,
        VarReturningType::VAR_RETURNING_DEFAULT => {}
    }

    // C: PlanState *parent = state->parent;
    //    if (parent) {
    //        PlanState *subplan = NULL;
    //        switch (nodeTag(parent)) {
    //            case T_SubqueryScanState:
    //                subplan = ((SubqueryScanState *) parent)->subplan; break;
    //            case T_CteScanState:
    //                subplan = ((CteScanState *) parent)->cteplanstate; break;
    //            default: break;
    //        }
    //        if (subplan) { ... detect resjunk cols; if so,
    //            scratch->d.wholerow.junkFilter =
    //                ExecInitJunkFilter(subplan->plan->targetlist,
    //                                   ExecInitExtraTupleSlot(parent->state, NULL,
    //                                                          &TTSOpsVirtual)); } }
    //
    // Only a SubqueryScan or CteScan parent yields a non-NULL `subplan` and thus
    // any junk-filter work; every other parent tag hits the `default:` arm and
    // leaves `subplan` NULL (junkFilter stays NULL — already set above). Neither
    // `SubqueryScanState` nor `CteScanState` has landed as a `PlanStateNode`
    // variant yet (`PlanStateNode::subquery_subplan_state()` is the modeled
    // accessor and returns `None` for every current variant), so the owned model
    // can faithfully realize the `default:` arm for every parent that can reach
    // here: `subplan` is `None`, and the junk-filter block is correctly skipped.
    // C: if (parent) { switch (nodeTag(parent)) { case T_SubqueryScanState / ... }
    //        ... build scratch->d.wholerow.junkFilter from the subplan targetlist }
    //
    // In the owned tree the enclosing `PlanStateNode` enum is only address-stable
    // *after* the node is boxed, so `state.parent` is still unset at this compile
    // point (it is back-filled by `stamp_expr_parents`). The C reads `parent`
    // here only to reach the SubqueryScan/CteScan subplan targetlist and build a
    // JunkFilter to strip the subquery's resjunk (ORDER BY/GROUP BY) columns out
    // of the whole-row result — work that has no address dependency. It is done
    // once the node's enum is stable, in `exec_init_subqueryscan_wholerow_junk`
    // (execProcnode_init's `exec_init_node_finish`), which installs the filter
    // onto this very EEOP_WHOLEROW step. `junk_filter` is left `None` here.
    let _ = mcx;

    Ok(())
}

/// `ExecInitSubscriptingRef(scratch, sbsref, state, resv, resnull)`
/// (execExpr.c:3252) — compile a container `SubscriptingRef` (array/jsonb
/// element fetch or assignment): resolve the type's subscript routines, lay out
/// the `SubscriptingRefState` index workspace, recurse the container expression
/// and each subscript expression, emit the SUBSCRIPTS/OLD/ASSIGN/FETCH steps,
/// and backpatch the null-jump targets.
pub(crate) fn exec_init_subscripting_ref<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    scratch: &mut ::nodes::execexpr::ExprEvalStep<'mcx>,
    sbsref: &::nodes::primnodes::SubscriptingRef<'mcx>,
    state: &mut ExprState<'mcx>,
    resv: ::nodes::execexpr::ResultCellId,
) -> PgResult<()> {
    use ::nodes::execexpr::{
        ExprEvalOp, ExprEvalStepData, SubscriptExecSteps, SubscriptingRefState,
    };

    // C: bool isAssignment = (sbsref->refassgnexpr != NULL);
    //    int nupper = list_length(sbsref->refupperindexpr);
    //    int nlower = list_length(sbsref->reflowerindexpr);
    let is_assignment = sbsref.refassgnexpr.is_some();
    let nupper = sbsref.refupperindexpr.len() as i32;
    let nlower = sbsref.reflowerindexpr.len() as i32;

    // C: sbsroutines = getSubscriptingRoutines(sbsref->refcontainertype, NULL);
    //    if (!sbsroutines)
    //        ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH),
    //                errmsg("cannot subscript type %s because it does not support
    //                        subscripting", format_type_be(sbsref->refcontainertype)), ...);
    let sbsroutines = lsyscache_seams::get_subscripting_routines::call(
        sbsref.refcontainertype,
    )?;
    let sbsroutines = match sbsroutines {
        Some((routines, _typelem)) => routines,
        None => {
            // C: format_type_be(refcontainertype) for the error message.
            let typname = format_type_seams::format_type_be_owned::call(sbsref.refcontainertype)?;
            return Err(::types_error::PgError::error(format!(
                "cannot subscript type {typname} because it does not support subscripting"
            ))
            .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH));
        }
    };

    // C: sbsrefstate = palloc0(MAXALIGN(sizeof(SubscriptingRefState)) +
    //                          (nupper + nlower) * (sizeof(Datum) + 2*sizeof(bool)));
    //    sbsrefstate->isassignment = isAssignment;
    //    sbsrefstate->numupper = nupper; sbsrefstate->numlower = nlower;
    //    /* set up per-subscript arrays */ ... upperindex/lowerindex/
    //    upperprovided/lowerprovided/upperindexnull/lowerindexnull
    //
    // The single-block carve becomes typed owned vectors; the index arrays are
    // zero-initialized (filled by the SUBSCRIPTS step at runtime from the arena
    // cells the subscript expressions write).
    let mut sbsrefstate = SubscriptingRefState {
        isassignment: is_assignment,
        numupper: nupper,
        numlower: nlower,
        upperprovided: Some(zeroed_bools(mcx, nupper)?),
        upperindex: Some(zeroed_datums(mcx, nupper)?),
        upperindexnull: Some(zeroed_bools(mcx, nupper)?),
        lowerprovided: Some(zeroed_bools(mcx, nlower)?),
        lowerindex: Some(zeroed_datums(mcx, nlower)?),
        lowerindexnull: Some(zeroed_bools(mcx, nlower)?),
        upper_cells: Some(zeroed_cells(mcx, nupper)?),
        lower_cells: Some(zeroed_cells(mcx, nlower)?),
        ..SubscriptingRefState::default()
    };

    // C: memset(&methods, 0, sizeof(methods));
    //    sbsroutines->exec_setup(sbsref, sbsrefstate, &methods);
    //
    // exec_setup is the type-specific compilation hook; named by the
    // SubscriptRoutines handler discriminant, it fills the SubscriptExecSteps
    // method discriminants and the typed workspace. Run it here (the array
    // family lives in this crate; jsonb etc. would be their owners).
    let mut methods = SubscriptExecSteps::default();
    subscript_exec_setup(&sbsroutines, sbsref, &mut sbsrefstate, &mut methods)?;

    // C: ExecInitExprRec(sbsref->refexpr, state, resv, resnull);
    let refexpr = sbsref
        .refexpr
        .as_deref()
        .expect("SubscriptingRef.refexpr is NULL");
    crate::execExpr_core::exec_init_expr_rec(mcx, refexpr, state, resv)?;

    // adjust_jumps records the step indices needing a backpatch to the end.
    let mut adjust_jumps: mcx::PgVec<'mcx, usize> = mcx::vec_with_capacity_in(mcx, 0)?;

    // C: if (!isAssignment && sbsroutines->fetch_strict) {
    //        scratch->opcode = EEOP_JUMP_IF_NULL; scratch->d.jump.jumpdone = -1;
    //        ExprEvalPushStep(state, scratch);
    //        adjust_jumps = lappend_int(adjust_jumps, state->steps_len - 1); }
    if !is_assignment && sbsroutines.fetch_strict {
        let jump = ::nodes::execexpr::ExprEvalStep {
            opcode: ExprEvalOp::EEOP_JUMP_IF_NULL,
            resvalue: resv,
            resnull: resv,
            d: ExprEvalStepData::Jump { jumpdone: -1 },
        };
        crate::execExpr_core::expr_eval_push_step(mcx, state, jump)?;
        adjust_jumps.push((state.steps_len - 1) as usize);
    }

    // C: Evaluate upper subscripts. For each, if NULL it's an omitted slice
    //    bound (upperprovided=false, upperindexnull=true); else
    //    upperprovided=true and ExecInitExprRec(e, state,
    //    &sbsrefstate->upperindex[i], &sbsrefstate->upperindexnull[i]).
    //
    // In the owned model each provided subscript compiles into a fresh arena
    // cell recorded in upper_cells[i]; the SUBSCRIPTS step gathers the cells
    // into upperindex/upperindexnull at runtime.
    for (i, e) in sbsref.refupperindexpr.iter().enumerate() {
        match e {
            None => {
                sbsrefstate.upperprovided.as_mut().unwrap()[i] = false;
                sbsrefstate.upperindexnull.as_mut().unwrap()[i] = true;
            }
            Some(e) => {
                sbsrefstate.upperprovided.as_mut().unwrap()[i] = true;
                let cell = crate::execExpr_core::new_result_cell(mcx, state)?;
                crate::execExpr_core::exec_init_expr_rec(mcx, e, state, cell)?;
                sbsrefstate.upper_cells.as_mut().unwrap()[i] = Some(cell);
            }
        }
    }

    // C: Evaluate lower subscripts similarly.
    for (i, e) in sbsref.reflowerindexpr.iter().enumerate() {
        match e {
            None => {
                sbsrefstate.lowerprovided.as_mut().unwrap()[i] = false;
                sbsrefstate.lowerindexnull.as_mut().unwrap()[i] = true;
            }
            Some(e) => {
                sbsrefstate.lowerprovided.as_mut().unwrap()[i] = true;
                let cell = crate::execExpr_core::new_result_cell(mcx, state)?;
                crate::execExpr_core::exec_init_expr_rec(mcx, e, state, cell)?;
                sbsrefstate.lower_cells.as_mut().unwrap()[i] = Some(cell);
            }
        }
    }

    // In C all the SBSREF steps point at one shared `palloc`'d
    // SubscriptingRefState. In the owned model a Box cannot be aliased across
    // several steps, so each step carries its own structural copy via
    // `clone_state*` (below). This is behavior-preserving: the init-time fields
    // (provided/cells/numupper/...) are identical, and the runtime conversion
    // arrays (upperindex/workspace) are re-derived per step by SUBSCRIPTS/the
    // fetch/assign bodies from the same arena cells — the steps run in sequence
    // on one ExprState, and SUBSCRIPTS' converted workspace is regenerated by
    // each consuming step (each carries the same converted-from cells).
    let sbsrefstate_box = mcx::alloc_in(mcx, sbsrefstate)?;

    // C: SBSREF_SUBSCRIPTS checks and converts all subscripts at once.
    //    if (methods.sbs_check_subscripts) {
    //        scratch->opcode = EEOP_SBSREF_SUBSCRIPTS;
    //        scratch->d.sbsref_subscript.subscriptfunc = methods.sbs_check_subscripts;
    //        scratch->d.sbsref_subscript.state = sbsrefstate;
    //        scratch->d.sbsref_subscript.jumpdone = -1;
    //        ExprEvalPushStep(state, scratch);
    //        adjust_jumps = lappend_int(adjust_jumps, state->steps_len - 1); }
    if let Some(check) = methods.sbs_check_subscripts {
        let step = ::nodes::execexpr::ExprEvalStep {
            opcode: ExprEvalOp::EEOP_SBSREF_SUBSCRIPTS,
            resvalue: resv,
            resnull: resv,
            d: ExprEvalStepData::SbsRefSubscript {
                subscriptfunc: Some(check),
                state: Some(clone_state(mcx, &sbsrefstate_box)?),
                jumpdone: -1,
            },
        };
        crate::execExpr_core::expr_eval_push_step(mcx, state, step)?;
        adjust_jumps.push((state.steps_len - 1) as usize);
    }

    if is_assignment {
        // C: if (!methods.sbs_assign) ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED,
        //        "type %s does not support subscripted assignment");
        if methods.sbs_assign.is_none() {
            let typname = format_type_seams::format_type_be_owned::call(sbsref.refcontainertype)?;
            return Err(::types_error::PgError::error(format!(
                "type {typname} does not support subscripted assignment"
            ))
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
        }

        // C: if (isAssignmentIndirectionExpr(sbsref->refassgnexpr)) { ... OLD ... }
        let nested = is_assignment_indirection_expr(sbsref.refassgnexpr.as_deref());
        let prev_cell = if nested {
            // C: if (!methods.sbs_fetch_old) ereport(ERROR, ...);
            let fetch_old = methods.sbs_fetch_old.ok_or_else(|| {
                ::types_error::PgError::error(
                    "type does not support subscripted assignment",
                )
                .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            })?;
            // The OLD step writes prevvalue/prevnull; alias them to an arena
            // cell so the nested CaseTestExpr can read them.
            let prev_cell = crate::execExpr_core::new_result_cell(mcx, state)?;
            // C: scratch->opcode = EEOP_SBSREF_OLD;
            //    scratch->d.sbsref.subscriptfunc = methods.sbs_fetch_old;
            //    scratch->d.sbsref.state = sbsrefstate; ExprEvalPushStep(state, scratch);
            let step = ::nodes::execexpr::ExprEvalStep {
                opcode: ExprEvalOp::EEOP_SBSREF_OLD,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::SbsRef {
                    subscriptfunc: Some(fetch_old),
                    state: Some(clone_state_with_prev(mcx, &sbsrefstate_box, prev_cell)?),
                },
            };
            crate::execExpr_core::expr_eval_push_step(mcx, state, step)?;
            Some(prev_cell)
        } else {
            None
        };

        // C: save/restore innermost_caseval/casenull around evaluating the
        //    replacement value into &replacevalue/&replacenull. The owned model
        //    has a single innermost_caseval ResultCellId carrying both
        //    value+null (the ResultCell pairs them), aliased here to prev_cell
        //    (which the SBSREF_OLD step populates).
        let save_caseval = state.innermost_caseval;
        if let Some(prev_cell) = prev_cell {
            state.innermost_caseval = Some(prev_cell);
        }

        // C: ExecInitExprRec(sbsref->refassgnexpr, state,
        //                    &sbsrefstate->replacevalue, &sbsrefstate->replacenull);
        let replace_cell = crate::execExpr_core::new_result_cell(mcx, state)?;
        let refassgnexpr = sbsref
            .refassgnexpr
            .as_deref()
            .expect("SubscriptingRef.refassgnexpr present for assignment");
        crate::execExpr_core::exec_init_expr_rec(mcx, refassgnexpr, state, replace_cell)?;

        state.innermost_caseval = save_caseval;

        // C: scratch->opcode = EEOP_SBSREF_ASSIGN;
        //    scratch->d.sbsref.subscriptfunc = methods.sbs_assign;
        //    scratch->d.sbsref.state = sbsrefstate; ExprEvalPushStep(state, scratch);
        let step = ::nodes::execexpr::ExprEvalStep {
            opcode: ExprEvalOp::EEOP_SBSREF_ASSIGN,
            resvalue: resv,
            resnull: resv,
            d: ExprEvalStepData::SbsRef {
                subscriptfunc: methods.sbs_assign,
                state: Some(clone_state_for_assign(
                    mcx,
                    &sbsrefstate_box,
                    replace_cell,
                    prev_cell,
                )?),
            },
        };
        crate::execExpr_core::expr_eval_push_step(mcx, state, step)?;
        let _ = replace_cell;
    } else {
        // C: array fetch is much simpler.
        //    scratch->opcode = EEOP_SBSREF_FETCH;
        //    scratch->d.sbsref.subscriptfunc = methods.sbs_fetch;
        //    scratch->d.sbsref.state = sbsrefstate; ExprEvalPushStep(state, scratch);
        let step = ::nodes::execexpr::ExprEvalStep {
            opcode: ExprEvalOp::EEOP_SBSREF_FETCH,
            resvalue: resv,
            resnull: resv,
            d: ExprEvalStepData::SbsRef {
                subscriptfunc: methods.sbs_fetch,
                state: Some(clone_state(mcx, &sbsrefstate_box)?),
            },
        };
        crate::execExpr_core::expr_eval_push_step(mcx, state, step)?;
    }

    // C: adjust jump targets — SBSREF_SUBSCRIPTS.jumpdone / JUMP_IF_NULL.jumpdone
    //    = state->steps_len.
    let target = state.steps_len;
    let steps = state.steps.as_mut().unwrap();
    for &j in adjust_jumps.iter() {
        match &mut steps[j].d {
            ExprEvalStepData::SbsRefSubscript { jumpdone, .. } => {
                debug_assert_eq!(*jumpdone, -1);
                *jumpdone = target;
            }
            ExprEvalStepData::Jump { jumpdone } => {
                debug_assert_eq!(*jumpdone, -1);
                *jumpdone = target;
            }
            _ => unreachable!("subscript adjust_jumps step is neither SBSREF_SUBSCRIPTS nor JUMP_IF_NULL"),
        }
    }

    // scratch is unused here (we push our own fully-formed steps); silence it.
    let _ = scratch;
    Ok(())
}

/// Allocate a zero-initialized `PgVec<bool>` of length `n` (the C
/// `palloc0`-carved `bool` index sub-arrays).
fn zeroed_bools<'mcx>(mcx: mcx::Mcx<'mcx>, n: i32) -> PgResult<mcx::PgVec<'mcx, bool>> {
    let mut v = mcx::vec_with_capacity_in(mcx, n.max(0) as usize)?;
    for _ in 0..n.max(0) {
        v.push(false);
    }
    Ok(v)
}

/// Allocate a zero-initialized `PgVec<Datum>` of length `n`.
fn zeroed_datums<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    n: i32,
) -> PgResult<mcx::PgVec<'mcx, types_tuple::heaptuple::Datum<'mcx>>> {
    let mut v = mcx::vec_with_capacity_in(mcx, n.max(0) as usize)?;
    for _ in 0..n.max(0) {
        v.push(types_tuple::heaptuple::Datum::null());
    }
    Ok(v)
}

/// Allocate a `PgVec<Option<ResultCellId>>` of length `n`, all `None`.
fn zeroed_cells<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    n: i32,
) -> PgResult<mcx::PgVec<'mcx, Option<::nodes::execexpr::ResultCellId>>> {
    let mut v = mcx::vec_with_capacity_in(mcx, n.max(0) as usize)?;
    for _ in 0..n.max(0) {
        v.push(None);
    }
    Ok(v)
}

/// Deep-clone a populated `SubscriptingRefState` into a fresh box for a step.
/// The C steps share one `palloc`'d struct; the owned steps each carry their
/// own copy. The init-time fields (provided/cells/numupper/...) are identical
/// across steps and the runtime index/workspace conversion is per-step and
/// independent (each step re-derives the converted subscripts), so per-step
/// copies are behavior-preserving.
fn clone_state<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    src: &mcx::PgBox<'mcx, ::nodes::execexpr::SubscriptingRefState<'mcx>>,
) -> PgResult<mcx::PgBox<'mcx, ::nodes::execexpr::SubscriptingRefState<'mcx>>> {
    clone_state_inner(mcx, src, None, None)
}

/// As [`clone_state`] but recording the `prev_cell` alias for an OLD step.
fn clone_state_with_prev<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    src: &mcx::PgBox<'mcx, ::nodes::execexpr::SubscriptingRefState<'mcx>>,
    prev_cell: ::nodes::execexpr::ResultCellId,
) -> PgResult<mcx::PgBox<'mcx, ::nodes::execexpr::SubscriptingRefState<'mcx>>> {
    clone_state_inner(mcx, src, None, Some(prev_cell))
}

/// As [`clone_state`] but recording the `replace_cell` (and optional
/// `prev_cell`) for an ASSIGN step.
fn clone_state_for_assign<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    src: &mcx::PgBox<'mcx, ::nodes::execexpr::SubscriptingRefState<'mcx>>,
    replace_cell: ::nodes::execexpr::ResultCellId,
    prev_cell: Option<::nodes::execexpr::ResultCellId>,
) -> PgResult<mcx::PgBox<'mcx, ::nodes::execexpr::SubscriptingRefState<'mcx>>> {
    clone_state_inner(mcx, src, Some(replace_cell), prev_cell)
}

fn clone_state_inner<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    src: &mcx::PgBox<'mcx, ::nodes::execexpr::SubscriptingRefState<'mcx>>,
    replace_cell: Option<::nodes::execexpr::ResultCellId>,
    prev_cell: Option<::nodes::execexpr::ResultCellId>,
) -> PgResult<mcx::PgBox<'mcx, ::nodes::execexpr::SubscriptingRefState<'mcx>>> {
    use ::nodes::execexpr::SubscriptingRefState;
    let s = &**src;
    let clone_bools = |v: &Option<mcx::PgVec<'mcx, bool>>| -> PgResult<Option<mcx::PgVec<'mcx, bool>>> {
        match v {
            None => Ok(None),
            Some(b) => {
                let mut nv = mcx::vec_with_capacity_in(mcx, b.len())?;
                for &x in b.iter() {
                    nv.push(x);
                }
                Ok(Some(nv))
            }
        }
    };
    let clone_cells = |v: &Option<mcx::PgVec<'mcx, Option<::nodes::execexpr::ResultCellId>>>|
     -> PgResult<Option<mcx::PgVec<'mcx, Option<::nodes::execexpr::ResultCellId>>>> {
        match v {
            None => Ok(None),
            Some(b) => {
                let mut nv = mcx::vec_with_capacity_in(mcx, b.len())?;
                for &x in b.iter() {
                    nv.push(x);
                }
                Ok(Some(nv))
            }
        }
    };
    let dst = SubscriptingRefState {
        isassignment: s.isassignment,
        workspace: s.workspace.clone(),
        numupper: s.numupper,
        upperprovided: clone_bools(&s.upperprovided)?,
        upperindex: Some(zeroed_datums(mcx, s.numupper)?),
        upperindexnull: clone_bools(&s.upperindexnull)?,
        numlower: s.numlower,
        lowerprovided: clone_bools(&s.lowerprovided)?,
        lowerindex: Some(zeroed_datums(mcx, s.numlower)?),
        lowerindexnull: clone_bools(&s.lowerindexnull)?,
        replacevalue: types_tuple::heaptuple::Datum::null(),
        replacenull: false,
        prevvalue: types_tuple::heaptuple::Datum::null(),
        prevnull: false,
        upper_cells: clone_cells(&s.upper_cells)?,
        lower_cells: clone_cells(&s.lower_cells)?,
        replace_cell,
        prev_cell,
    };
    mcx::alloc_in(mcx, dst)
}

/// `sbsroutines->exec_setup(sbsref, sbsrefstate, &methods)` (nodes/subscripting.h)
/// — the type-specific compilation hook. Dispatches on the
/// [`SubscriptHandler`] discriminant to the per-type `exec_setup` body. The
/// array family (`array_exec_setup`, arraysubs.c) lives here; other handlers
/// would be reached at their owners.
fn subscript_exec_setup<'mcx>(
    routines: &::nodes::execexpr::SubscriptRoutines,
    sbsref: &::nodes::primnodes::SubscriptingRef<'mcx>,
    sbsrefstate: &mut ::nodes::execexpr::SubscriptingRefState<'mcx>,
    methods: &mut ::nodes::execexpr::SubscriptExecSteps,
) -> PgResult<()> {
    use ::nodes::execexpr::SubscriptHandler;
    match routines.handler {
        SubscriptHandler::Array | SubscriptHandler::RawArray => {
            array_exec_setup(sbsref, sbsrefstate, methods)
        }
        SubscriptHandler::Jsonb => jsonb_exec_setup(sbsref, sbsrefstate, methods),
    }
}

/// `jsonb_exec_setup(sbsref, sbsrefstate, methods)` (jsonbsubs.c) — set up the
/// jsonb subscript workspace and method discriminants. Unlike arrays there is
/// no limit on the number of subscripts (jsonb has no nesting limit) and no
/// slice support (the transform errored on slices).
fn jsonb_exec_setup<'mcx>(
    sbsref: &::nodes::primnodes::SubscriptingRef<'mcx>,
    sbsrefstate: &mut ::nodes::execexpr::SubscriptingRefState<'mcx>,
    methods: &mut ::nodes::execexpr::SubscriptExecSteps,
) -> PgResult<()> {
    use ::nodes::execexpr::{JsonbSubWorkspace, SubscriptMethod, SubscriptWorkspace};

    // C: int nupper = sbsref->refupperindexpr->length;
    //    workspace = palloc0(MAXALIGN(sizeof(JsonbSubWorkspace)) +
    //                        nupper * (sizeof(Datum) + sizeof(Oid)));
    //    workspace->expectArray = false;
    let nupper = sbsref.refupperindexpr.len();
    let mut workspace = JsonbSubWorkspace {
        expect_array: false,
        // workspace->index is re-derived per step; workspace->indexOid is
        // collected below from exprType of each subscript.
        index_oid: alloc::vec::Vec::with_capacity(nupper),
    };

    // C: foreach(lc, sbsref->refupperindexpr)
    //        workspace->indexOid[i] = exprType(expr);
    //
    // jsonb subscripting does not support slices, so every upper index is a
    // real expression (the transform errored on omitted/NULL slice bounds).
    for e in sbsref.refupperindexpr.iter() {
        let expr = e.as_ref().expect(
            "jsonb_exec_setup: jsonb subscript has no slices, every upper index is provided",
        );
        let typid = nodeFuncs_seams::expr_type_info::call(expr)?.typid;
        workspace.index_oid.push(typid);
    }

    sbsrefstate.workspace = SubscriptWorkspace::Jsonb(workspace);

    // C: pass back step functions.
    //    methods->sbs_check_subscripts = jsonb_subscript_check_subscripts;
    //    methods->sbs_fetch     = jsonb_subscript_fetch;
    //    methods->sbs_assign    = jsonb_subscript_assign;
    //    methods->sbs_fetch_old = jsonb_subscript_fetch_old;
    methods.sbs_check_subscripts = Some(SubscriptMethod::JsonbCheckSubscripts);
    methods.sbs_fetch = Some(SubscriptMethod::JsonbFetch);
    methods.sbs_assign = Some(SubscriptMethod::JsonbAssign);
    methods.sbs_fetch_old = Some(SubscriptMethod::JsonbFetchOld);
    Ok(())
}

/// `array_exec_setup(sbsref, sbsrefstate, methods)` (arraysubs.c) — set up the
/// array subscript workspace and method discriminants.
fn array_exec_setup<'mcx>(
    sbsref: &::nodes::primnodes::SubscriptingRef<'mcx>,
    sbsrefstate: &mut ::nodes::execexpr::SubscriptingRefState<'mcx>,
    methods: &mut ::nodes::execexpr::SubscriptExecSteps,
) -> PgResult<()> {
    use ::nodes::execexpr::{
        ArraySubWorkspace, SubscriptMethod, SubscriptWorkspace, MAXDIM,
    };

    // C: bool is_slice = (sbsrefstate->numlower != 0);
    let is_slice = sbsrefstate.numlower != 0;

    // C: if (sbsrefstate->numupper > MAXDIM) ereport(ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    //        "number of array dimensions (%d) exceeds the maximum allowed (%d)");
    if sbsrefstate.numupper > MAXDIM as i32 {
        return Err(::types_error::PgError::error(format!(
            "number of array dimensions ({}) exceeds the maximum allowed ({})",
            sbsrefstate.numupper, MAXDIM
        ))
        .with_sqlstate(::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }

    // C: if (numlower != 0 && numupper != numlower)
    //        elog(ERROR, "upper and lower index lists are not same length");
    if sbsrefstate.numlower != 0 && sbsrefstate.numupper != sbsrefstate.numlower {
        return Err(::types_error::PgError::error(
            "upper and lower index lists are not same length",
        ));
    }

    // C: workspace = palloc(sizeof(ArraySubWorkspace));
    //    workspace->refelemtype = sbsref->refelemtype;
    //    workspace->refattrlength = get_typlen(sbsref->refcontainertype);
    //    get_typlenbyvalalign(sbsref->refelemtype, &refelemlength, &refelembyval, &refelemalign);
    let refattrlength =
        lsyscache_seams::get_typlen::call(sbsref.refcontainertype)?;
    let lbva =
        lsyscache_seams::get_typlenbyvalalign::call(sbsref.refelemtype)?;
    let mut workspace = ArraySubWorkspace {
        refelemtype: sbsref.refelemtype,
        refattrlength,
        refelemlength: lbva.typlen,
        refelembyval: lbva.typbyval,
        refelemalign: lbva.typalign as u8,
        ..ArraySubWorkspace::default()
    };
    let _ = &mut workspace;
    sbsrefstate.workspace = SubscriptWorkspace::Array(workspace);

    // C: pass back step functions.
    //    methods->sbs_check_subscripts = array_subscript_check_subscripts;
    //    if (is_slice) { fetch=fetch_slice; assign=assign_slice; fetch_old=fetch_old_slice; }
    //    else          { fetch=fetch;       assign=assign;       fetch_old=fetch_old; }
    methods.sbs_check_subscripts = Some(SubscriptMethod::ArrayCheckSubscripts);
    if is_slice {
        methods.sbs_fetch = Some(SubscriptMethod::ArrayFetchSlice);
        methods.sbs_assign = Some(SubscriptMethod::ArrayAssignSlice);
        methods.sbs_fetch_old = Some(SubscriptMethod::ArrayFetchOldSlice);
    } else {
        methods.sbs_fetch = Some(SubscriptMethod::ArrayFetch);
        methods.sbs_assign = Some(SubscriptMethod::ArrayAssign);
        methods.sbs_fetch_old = Some(SubscriptMethod::ArrayFetchOld);
    }
    Ok(())
}
