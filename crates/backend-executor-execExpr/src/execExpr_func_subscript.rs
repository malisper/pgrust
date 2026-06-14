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
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
) -> PgResult<(Datum, bool)> {
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
/// (execExpr.c:2704) — set up the [`Func`] step for a function/operator call:
/// ACL-check `funcid`, look up its `FmgrInfo`/`FunctionCallInfo`, recurse each
/// non-Const argument into its `fcinfo->args[i]` cell, then pick the
/// `EEOP_FUNCEXPR*` opcode by strictness × pg_stat_function-tracking.
///
/// [`Func`]: types_nodes::execexpr::ExprEvalStepData::Func
pub(crate) fn exec_init_func<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    scratch: &mut types_nodes::execexpr::ExprEvalStep<'mcx>,
    node: &Expr,
    args: &[Expr],
    funcid: types_core::Oid,
    inputcollid: types_core::Oid,
    state: &mut ExprState<'mcx>,
) -> PgResult<()> {
    use types_nodes::execexpr::{ExprEvalOp, ExprEvalStepData, ResultCell};

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
    let aclresult = backend_catalog_aclchk_seams::object_aclcheck::call(
        types_parsenodes::ProcedureRelationId,
        funcid,
        backend_utils_init_miscinit_seams::get_user_id::call(),
        types_acl::ACL_EXECUTE,
    )?;
    if aclresult != types_acl::ACLCHECK_OK {
        // C: aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(funcid));
        let funcname = backend_utils_cache_lsyscache_seams::get_func_name::call(mcx, funcid)?
            .map(|s| s.to_string());
        backend_catalog_aclchk_seams::aclcheck_error::call(
            aclresult,
            types_nodes::parsenodes::OBJECT_FUNCTION,
            funcname,
        )?;
    }
    // C: InvokeFunctionExecuteHook(funcid);
    backend_catalog_objectaccess::invoke_function_execute_hook(funcid)?;

    // C: Safety check on nargs (parser should already have caught this).
    if nargs > FUNC_MAX_ARGS {
        // C: ereport(ERROR, errcode(ERRCODE_TOO_MANY_ARGUMENTS),
        //            errmsg_plural("cannot pass more than %d argument(s) to a function",
        //                          ..., FUNC_MAX_ARGS, FUNC_MAX_ARGS));
        return Err(types_error::PgError::error(format!(
            "cannot pass more than {FUNC_MAX_ARGS} arguments to a function"
        ))
        .with_sqlstate(types_error::ERRCODE_TOO_MANY_ARGUMENTS));
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
    // `fmgr_info_set_expr` would stash the call node on flinfo->fn_expr (used by
    // polymorphic-type resolution at call time); the trimmed FmgrInfo has no
    // fn_expr field, so that store is dropped here (inherited trim).
    let _ = node;
    let flinfo = backend_utils_fmgr_fmgr_seams::fmgr_info::call(mcx, funcid)?;

    // C: InitFunctionCallInfoData(*fcinfo, flinfo, nargs, inputcollid, NULL, NULL);
    //    scratch->d.func.fn_addr = flinfo->fn_addr;  scratch->d.func.nargs = nargs;
    //
    // The owned FunctionCallInfoBaseData is trimmed (the fmgr owner widens it
    // with flinfo/nargs/fncollation/args[]); the arguments the C threads into
    // fcinfo->args[i] are carried instead by the Func step's `arg_cells`
    // (one ResultCellId per arg), which the interpreter gathers into the call
    // frame immediately before dispatch. We allocate the palloc0 fcinfo frame
    // (defaulted) so the step shape matches C; inputcollid is recorded by the
    // owner when it widens the frame.
    let _ = inputcollid;
    let fcinfo_data = mcx::alloc_in(
        mcx,
        types_nodes::fmgr::FunctionCallInfoBaseData::default(),
    )?;

    // C: We only support non-set functions here.
    //    if (flinfo->fn_retset) ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED,
    //        "set-valued function called in context that cannot accept a set");
    if flinfo.fn_retset {
        return Err(types_error::PgError::error(
            "set-valued function called in context that cannot accept a set",
        )
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
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
                    value: con.constvalue,
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
    let track_functions = backend_utils_misc_guc_tables::vars::pgstat_track_functions.read();
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

/// `ExecInitSubPlanExpr(subplan, state, resv, resnull)` (execExpr.c:2820) —
/// compile a `SubPlan` reference: recurse each `parParam`/`args` pair into the
/// param it sets, emit an `EEOP_PARAM_SET` step per pair, create the
/// `SubPlanState` (nodeSubplan) and register it on the parent, then emit the
/// `EEOP_SUBPLAN` step.
pub(crate) fn exec_init_sub_plan_expr<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    subplan: &types_nodes::primnodes::SubPlan<'mcx>,
    state: &mut ExprState<'mcx>,
    resv: types_nodes::execexpr::ResultCellId,
) -> PgResult<()> {
    use types_nodes::execexpr::{ExprEvalOp, ExprEvalStepData};

    // C: ExprEvalStep scratch = {0};
    // C: if (!state->parent)
    //        elog(ERROR, "SubPlan found with no parent plan");
    if state.parent.is_none() {
        return Err(types_error::PgError::error(
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

        let scratch = types_nodes::execexpr::ExprEvalStep {
            opcode: ExprEvalOp::EEOP_PARAM_SET,
            resvalue: resv,
            resnull: resv,
            d: ExprEvalStepData::Param {
                paramid,
                // C: scratch.d.param.paramtype = exprType((Node *) arg);
                // (declared "not actually used"). No exprType seam threaded
                // here; carry InvalidOid.
                paramtype: types_core::InvalidOid,
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
    // `ExecInitSubPlan` is owned by nodeSubplan (builds the SubPlanState, sets
    // up the hash tables / projections / combining-expr); it is not exported as
    // a callable seam here, and the parent PlanState's `subPlan` list append is
    // the parent owner's. The PARAM_SET argument-evaluation steps above are this
    // unit's own logic and are emitted; the EEOP_SUBPLAN step needs the
    // nodeSubplan-built SubPlanState the step must carry. Faithful structure;
    // genuine cross-unit blocker.
    let _ = subplan;
    panic!(
        "execExpr-func-subscript: ExecInitSubPlanExpr — ExecInitSubPlan(subplan, state->parent) \
         is owned by nodeSubplan (builds the SubPlanState the EEOP_SUBPLAN step carries) and is \
         not exported as a callable seam; the state->parent->subPlan lappend is the parent \
         PlanState owner's. The EEOP_PARAM_SET argument descent above is this unit's own logic \
         and is emitted."
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
    if let Some(parent) = state.parent.as_deref() {
        use types_nodes::nodes::T_SubqueryScanState;

        let subplan: Option<&types_nodes::planstate::PlanStateNode<'mcx>> = match parent.tag() {
            // C: case T_SubqueryScanState: subplan = ...->subplan;
            //    case T_CteScanState: subplan = ...->cteplanstate;
            // Reached via the modeled SubqueryScan/CteScan child-plan accessor.
            // `CteScanState` has no node tag landed yet; a CteScan parent cannot
            // exist as a `PlanStateNode` variant, so only the SubqueryScan tag is
            // matchable today (both share the `default: break` -> NULL outcome
            // until their variants land).
            t if t == T_SubqueryScanState => parent.subquery_subplan_state(),
            // C: default: break; — subplan stays NULL.
            _ => None,
        };

        if let Some(subplan) = subplan {
            // C: foreach(tlist, subplan->plan->targetlist)
            //        if (tle->resjunk) { junk_filter_needed = true; break; }
            //    if (junk_filter_needed)
            //        scratch->d.wholerow.junkFilter = ExecInitJunkFilter(...);
            //
            // The subplan-targetlist resjunk scan plus ExecInitJunkFilter /
            // ExecInitExtraTupleSlot (execJunk / execTuples owners) build the
            // JunkFilter the step parks; route loudly only when a real
            // SubqueryScan/CteScan parent is threaded (impossible today — neither
            // variant has landed, so this arm is unreachable for current parents).
            let _ = subplan;
            panic!(
                "execExpr-func-subscript: ExecInitWholeRowVar — a SubqueryScan/CteScan parent \
                 needs the subplan-targetlist resjunk scan + ExecInitJunkFilter / \
                 ExecInitExtraTupleSlot (execJunk / execTuples owner seams) to build the \
                 whole-row JunkFilter"
            );
        }
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
    _mcx: mcx::Mcx<'mcx>,
    _scratch: &mut types_nodes::execexpr::ExprEvalStep<'mcx>,
    sbsref: &types_nodes::primnodes::SubscriptingRef,
    _state: &mut ExprState<'mcx>,
    _resv: types_nodes::execexpr::ResultCellId,
) -> PgResult<()> {
    // C: bool isAssignment = (sbsref->refassgnexpr != NULL);
    //    int nupper = list_length(sbsref->refupperindexpr);
    //    int nlower = list_length(sbsref->reflowerindexpr);
    let _is_assignment = sbsref.refassgnexpr.is_some();
    let _nupper = sbsref.refupperindexpr.len() as i32;
    let _nlower = sbsref.reflowerindexpr.len() as i32;

    // C: sbsroutines = getSubscriptingRoutines(sbsref->refcontainertype, NULL);
    //    if (!sbsroutines)
    //        ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH),
    //                errmsg("cannot subscript type %s because it does not support
    //                        subscripting", format_type_be(sbsref->refcontainertype)),
    //                ...);
    //
    // `getSubscriptingRoutines` (backend-utils-adt subscripting: looks up the
    // type's `SubscriptRoutines` via the typsubscript support function) is the
    // FIRST cross-unit call and has no exported seam in this repo. Everything
    // downstream is gated on its `SubscriptRoutines`:
    //
    //  * the `SubscriptingRefState` workspace can be laid out (own logic:
    //    isassignment/numupper/numlower + the upper/lower index+provided+null
    //    arrays modeled as `PgVec`s, the C `palloc0(MAXALIGN(...) + (nupper +
    //    nlower)*(sizeof(Datum)+2*sizeof(bool)))` single-block carve),
    //  * but `sbsroutines->exec_setup(sbsref, sbsrefstate, &methods)` fills the
    //    `SubscriptExecSteps` (the `sbs_check_subscripts` / `sbs_fetch` /
    //    `sbs_fetch_old` / `sbs_assign` subroutine pointers + `fetch_strict`)
    //    that the EEOP_SBSREF_SUBSCRIPTS / _OLD / _ASSIGN / _FETCH steps must
    //    carry and that the `!isAssignment && fetch_strict` JUMP_IF_NULL and the
    //    assignment `sbs_assign`/`sbs_fetch_old` presence checks branch on.
    //
    // The container/subscript/assign argument descent below it
    // (`ExecInitExprRec(sbsref->refexpr, ..., resv)`, each subscript into
    // `&sbsrefstate->{upper,lower}index[i]` / `[i]null`, and — gated by
    // `isAssignmentIndirectionExpr(sbsref->refassgnexpr)` (this family's own
    // helper, ported above) — the EEOP_SBSREF_OLD step + the
    // innermost_caseval/innermost_casenull save/restore around
    // `ExecInitExprRec(sbsref->refassgnexpr, ..., &replacevalue/&replacenull)`)
    // is this unit's own logic and is expressible against the landed result-cell
    // arena + `Func`/`SbsRef` step vocab — but it cannot run before the
    // `SubscriptRoutines` (and thus the step subroutine pointers) exist. Per
    // "mirror PG and panic", route loudly at the first unported owner.
    panic!(
        "execExpr-func-subscript: ExecInitSubscriptingRef — getSubscriptingRoutines(\
         refcontainertype) (backend-utils-adt subscripting) has no exported seam; its \
         SubscriptRoutines->exec_setup fills the SubscriptExecSteps (sbs_check_subscripts/\
         sbs_fetch/sbs_fetch_old/sbs_assign + fetch_strict) the EEOP_SBSREF_* steps carry and \
         branch on. The SubscriptingRefState workspace layout, the container/subscript/assign \
         argument descent, and the isAssignmentIndirectionExpr-gated SBSREF_OLD step are this \
         unit's own logic and land once the getSubscriptingRoutines seam is exported."
    );
}
