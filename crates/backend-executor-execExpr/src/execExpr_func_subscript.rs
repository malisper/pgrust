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

use types_core::AttrNumber;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::execexpr::{ExprState, ProjectionInfo, SubPlanState};
use types_nodes::execnodes::Opaque;
use types_nodes::primnodes::Expr;
use types_nodes::{EStateData, EcxtId, SlotId};

use backend_executor_execExpr_seams::{ProjectionKind, SlotAttr};

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
    // expression context. The owned model lends it as a `&mut PlanStateData`;
    // route through the execExpr-core ExecInitExpr seam (this crate). The
    // parent is the node's own planstate's head (the subselect plan state),
    // which is what carries the EState param/slot context here.
    let parent_node = node
        .planstate
        .as_mut()
        .expect("SubPlanState.planstate is NULL in ExecInitSubPlanExpr");
    let parent = parent_node.ps_head_mut();

    // `ExecInitExpr` is owned by execExpr-core; route through its seam.
    let state = crate::execExpr_core::exec_init_expr(testexpr, parent, estate)?;

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
    let carrier = proj_carrier(proj_slot(node, which), which);
    let _resultslot = crate::execExpr_core::exec_project_info(&carrier.proj, estate)?;
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
    // `ExecClearTuple` over a pool slot is owned by execTuples; route through
    // its slot accessor. The result slot lives in the EState slot pool.
    let slot = estate.slot_mut(slot_id);
    backend_executor_execTuples_seams::exec_clear_tuple::call(slot)
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
    backend_executor_execTuples_seams::slot_natts::call(estate, slot_id)
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
        backend_executor_execTuples_seams::slot_getattr_by_id::call(estate, slot_id, attnum as AttrNumber)?;
    Ok(attr.isnull)
}

/// `slot_getattr(node->projLeft result slot, att, &isnull)`
/// (`execTuplesUnequal` `slot1`).
pub fn proj_left_slot_getattr<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    att: AttrNumber,
) -> PgResult<SlotAttr> {
    // C: attr1 = slot_getattr(slot1, att, &isNull1);  // slot1 == projLeft slot
    let slot_id = proj_carrier(proj_slot(node, ProjectionKind::Left), ProjectionKind::Left)
        .resultslot;
    let attr = backend_executor_execTuples_seams::slot_getattr_by_id::call(estate, slot_id, att)?;
    Ok(SlotAttr {
        value: attr.value,
        isnull: attr.isnull,
    })
}

/// `ExecEvalExprSwitchContext(node->testexpr, econtext, &rownull)`
/// (nodeSubplan.c:399).
pub fn eval_testexpr_switch_context<'mcx>(
    node: &SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
) -> PgResult<(Datum, bool)> {
    // C: rowresult = ExecEvalExprSwitchContext(node->testexpr, econtext,
    //                                          &rownull);
    let carrier = testexpr_carrier(&node.testexpr);
    // `ExecEvalExprSwitchContext` is owned by execExpr-core; route through its
    // seam over the compiled combining ExprState.
    crate::execExpr_core::exec_eval_expr_switch_context(&carrier.state, econtext, estate)
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
// keystone models a step's output as `ExprEvalStep::{resvalue,resnull}:
// Option<PgBox<Datum/bool>>` where `None` means "the ExprState's own
// resvalue/resnull"; it grew a `ResultCellId`/`ResultCellArena`, but
// `ExprEvalStep`'s result fields are not yet keyed off that arena, so a step
// cannot yet *name* a distinct cell, and execExpr-core's `exec_init_expr_rec`
// neither takes a result-cell target nor is exported. Per "mirror PG and
// panic", these routines carry the full faithful C structure for the logic this
// family owns (the aclcheck/strict classification, the sbsrefstate layout, the
// jump backpatching, the param-set/subplan step emission) and route the
// recursive-descent-into-a-distinct-cell points — execExpr-core's own,
// not-yet-landed `ExecInitExprRec` result-cell-target model — to a loud panic,
// exactly as `exec_init_expr_rec` already does for OpExpr/FuncExpr.

/// `#define FUNC_MAX_ARGS 100` (pg_config_manual.h).
const FUNC_MAX_ARGS: i32 = types_core::primitive::FUNC_MAX_ARGS as i32;

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
    match expr {
        // C: if (IsA(expr, FieldStore)) { ... if arg is CaseTestExpr ... }
        Expr::FieldStore(fstore) => {
            if matches!(fstore.arg.as_deref(), Some(Expr::CaseTestExpr(_))) {
                return true;
            }
        }
        // C: else if (IsA(expr, SubscriptingRef)) { ... if refexpr is CaseTestExpr }
        Expr::SubscriptingRef(sbs_ref) => {
            if matches!(sbs_ref.refexpr.as_deref(), Some(Expr::CaseTestExpr(_))) {
                return true;
            }
        }
        // C: else if (IsA(expr, CoerceToDomain)) return recurse(cd->arg);
        Expr::CoerceToDomain(cd) => {
            return is_assignment_indirection_expr(cd.arg.as_deref());
        }
        // C: else if (IsA(expr, RelabelType)) return recurse(r->arg);
        Expr::RelabelType(r) => {
            return is_assignment_indirection_expr(r.arg.as_deref());
        }
        _ => {}
    }
    // C: return false;
    false
}

/// `ExecInitFunc(scratch, node, args, funcid, inputcollid, state)`
/// (execExpr.c:2716) — set up the [`Func`] step for a function/operator call:
/// ACL-check `funcid`, look up its `FmgrInfo`/`FunctionCallInfo`, recurse each
/// non-Const argument into its `fcinfo->args[i]` cell, then pick the
/// `EEOP_FUNCEXPR*` opcode by strictness × pg_stat_function-tracking.
///
/// [`Func`]: types_nodes::execexpr::ExprEvalStepData::Func
pub(crate) fn exec_init_func<'mcx>(
    _scratch: &mut types_nodes::execexpr::ExprEvalStep<'mcx>,
    _node: &Expr,
    args: &[Expr],
    _funcid: types_core::Oid,
    _inputcollid: types_core::Oid,
    _state: &mut ExprState<'mcx>,
) -> PgResult<()> {
    let nargs = args.len() as i32;
    // C: aclresult = object_aclcheck(ProcedureRelationId, funcid, GetUserId(),
    //                                ACL_EXECUTE);
    //    if (aclresult != ACLCHECK_OK) aclcheck_error(...);
    //    InvokeFunctionExecuteHook(funcid);
    //
    // The catalog ACL check (backend-catalog-aclchk), the object-access hook,
    // and fmgr_info (backend-utils-fmgr) are cross-unit callees that route
    // through their owner seams. The keystone result-cell model that the
    // per-argument `ExecInitExprRec(arg, state, &fcinfo->args[i].value, ...)`
    // descent and the `scratch.d.func.{finfo,fcinfo_data,fn_addr}` wiring need
    // (a step naming a distinct fcinfo arg cell) has not landed — see the
    // module note. Faithful structure, in-unit dep not yet modeled.
    if nargs > FUNC_MAX_ARGS {
        // C: ereport(ERROR, errcode(ERRCODE_TOO_MANY_ARGUMENTS),
        //            errmsg_plural("cannot pass more than %d argument(s)...", ...));
        panic!(
            "execExpr-func-subscript: ExecInitFunc — too many function arguments \
             ({nargs} > {FUNC_MAX_ARGS}) (faithful errpath; the ereport sink is threaded \
             with the core compiler)"
        );
    }
    panic!(
        "execExpr-func-subscript: ExecInitFunc not fully ported — needs the execExpr-core \
         ExecInitExprRec per-argument result-cell target model (each argument compiles into \
         its own fcinfo->args[i] cell) plus the object_aclcheck / fmgr_info owner seams"
    );
}

/// `ExecInitSubPlanExpr(subplan, state, resv, resnull)` (execExpr.c:2820) —
/// compile a `SubPlan` reference: recurse each `parParam`/`args` pair into the
/// param it sets, emit an `EEOP_PARAM_SET` step per pair, create the
/// `SubPlanState` (nodeSubplan) and register it on the parent, then emit the
/// `EEOP_SUBPLAN` step.
pub(crate) fn exec_init_sub_plan_expr<'mcx>(
    _subplan: &types_nodes::primnodes::SubPlan<'mcx>,
    state: &mut ExprState<'mcx>,
    _resv: Option<&mut Datum>,
    _resnull: Option<&mut bool>,
) -> PgResult<()> {
    // C: if (state->parent == NULL)
    //        elog(ERROR, "SubPlan found with no parent plan");
    if state.parent.is_none() {
        panic!("execExpr-func-subscript: ExecInitSubPlanExpr — SubPlan found with no parent plan");
    }
    // C: forboth(l, subplan->parParam, pvar, subplan->args) {
    //        ExecInitExprRec(arg, state, resv, resnull);
    //        scratch.opcode = EEOP_PARAM_SET; scratch.d.param.paramid = paramid;
    //        scratch.d.param.paramtype = exprType(arg); ExprEvalPushStep(...);
    //    }
    //    sstate = ExecInitSubPlan(subplan, state->parent);
    //    state->parent->subPlan = lappend(state->parent->subPlan, sstate);
    //    scratch.opcode = EEOP_SUBPLAN; scratch.d.subplan.sstate = sstate;
    //    ExprEvalPushStep(...);
    //
    // The per-arg descent needs execExpr-core's not-yet-landed ExecInitExprRec
    // result-cell-target model (it recurses into the shared resv/resnull cell);
    // `ExecInitSubPlan` is owned by nodeSubplan (cross-unit seam). Faithful
    // structure; in-unit + cross-unit deps not yet modeled.
    panic!(
        "execExpr-func-subscript: ExecInitSubPlanExpr not fully ported — needs the execExpr-core \
         ExecInitExprRec recursion entry (for the EEOP_PARAM_SET argument descent) and the \
         nodeSubplan ExecInitSubPlan owner seam"
    );
}

/// `ExecInitWholeRowVar(scratch, variable, state)` (execExpr.c:3206) — set up an
/// `EEOP_WHOLEROW` step for a whole-row `Var`. Records the OLD/NEW
/// returning-type flags on the `ExprState`, and — for a SubqueryScan/CteScan
/// parent whose subplan emits junk columns — attaches a `JunkFilter`.
pub(crate) fn exec_init_whole_row_var<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    scratch: &mut types_nodes::execexpr::ExprEvalStep<'mcx>,
    variable: &types_nodes::primnodes::Var,
    state: &mut ExprState<'mcx>,
) -> PgResult<()> {
    use types_nodes::execexpr::{
        ExprEvalOp, ExprEvalStepData, EEO_FLAG_HAS_NEW, EEO_FLAG_HAS_OLD,
    };
    use types_nodes::primnodes::VarReturningType;

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
        junk_filter: 0,
    };

    // C: if (variable->varreturningtype == VAR_RETURNING_OLD)
    //        state->flags |= EEO_FLAG_HAS_OLD;
    //    else if (variable->varreturningtype == VAR_RETURNING_NEW)
    //        state->flags |= EEO_FLAG_HAS_NEW;
    //
    // The keystone `Var` does not yet carry `varreturningtype`; it defaults to
    // VAR_RETURNING_DEFAULT, so neither flag is set (matching the common path).
    let varreturningtype = VarReturningType::VAR_RETURNING_DEFAULT;
    match varreturningtype {
        VarReturningType::VAR_RETURNING_OLD => state.flags |= EEO_FLAG_HAS_OLD,
        VarReturningType::VAR_RETURNING_NEW => state.flags |= EEO_FLAG_HAS_NEW,
        VarReturningType::VAR_RETURNING_DEFAULT => {}
    }

    // C: if (parent) { ... SubqueryScanState/CteScanState junk-filter setup,
    //        scratch->d.wholerow.junkFilter = ExecInitJunkFilter(...); }
    //
    // The parent PlanState's SubqueryScan/CteScan subplan-targetlist
    // introspection and ExecInitJunkFilter / ExecInitExtraTupleSlot are owned
    // by execProcnode / execJunk / execTuples (cross-unit). With no parent the
    // C skips the whole block; that is the only shape this family compiles
    // standalone, so leave junkFilter NULL — exactly the C `!parent` path.
    if state.parent.is_some() {
        panic!(
            "execExpr-func-subscript: ExecInitWholeRowVar — parent-bearing junk-filter setup \
             not ported (needs the SubqueryScan/CteScan subplan-targetlist introspection and \
             the ExecInitJunkFilter / ExecInitExtraTupleSlot owner seams)"
        );
    }

    Ok(())
}

/// `ExecInitSubscriptingRef(scratch, sbsref, state, resv, resnull)`
/// (execExpr.c:3252) — compile a container `SubscriptingRef` (array/jsonb
/// element fetch or assignment): resolve the type's subscript routines, lay out
/// the `SubscriptingRefState` index workspace, recurse the container expression
/// and each subscript expression, emit the SUBSCRIPTS/OLD/ASSIGN/FETCH steps,
/// and backpatch the null-jump targets.
pub(crate) fn exec_init_subscripting_ref<'mcx>(
    _scratch: &mut types_nodes::execexpr::ExprEvalStep<'mcx>,
    sbsref: &types_nodes::primnodes::SubscriptingRef,
    _state: &mut ExprState<'mcx>,
    _resv: Option<&mut Datum>,
    _resnull: Option<&mut bool>,
) -> PgResult<()> {
    // C: bool isAssignment = (sbsref->refassgnexpr != NULL);
    //    int nupper = list_length(sbsref->refupperindexpr);
    //    int nlower = list_length(sbsref->reflowerindexpr);
    //    sbsroutines = getSubscriptingRoutines(sbsref->refcontainertype, NULL);
    //    if (!sbsroutines) ereport(ERROR, "cannot subscript type %s ...");
    let _is_assignment = sbsref.refassgnexpr.is_some();
    let _nupper = sbsref.refupperindexpr.len() as i32;
    let _nlower = sbsref.reflowerindexpr.len() as i32;

    // The subscript-handler resolution (getSubscriptingRoutines ->
    // backend-utils-adt subscripting), the SubscriptExecSteps method install
    // (sbsroutines->exec_setup), and the per-subscript / container / assign
    // descent (ExecInitExprRec into the sbsrefstate->{upper,lower}index[i] and
    // ->replacevalue cells, plus the innermost_caseval save/restore) all need
    // either a cross-unit owner seam or execExpr-core's not-yet-landed
    // ExecInitExprRec result-cell-target model. The `isAssignmentIndirectionExpr`
    // gate (this family's own logic) is ported above and consulted here in the
    // assignment path. Faithful structure; deps not yet modeled.
    //
    // C (assignment-indirection gate, faithfully ported, consulted below):
    //    if (isAssignmentIndirectionExpr(sbsref->refassgnexpr)) { ... EEOP_SBSREF_OLD ... }
    let _needs_old = is_assignment_indirection_expr(sbsref.refassgnexpr.as_deref());

    panic!(
        "execExpr-func-subscript: ExecInitSubscriptingRef not fully ported — needs the \
         getSubscriptingRoutines owner seam (backend-utils-adt subscripting) and the \
         execExpr-core ExecInitExprRec result-cell-target model for the container/subscript/\
         assign descent"
    );
}
