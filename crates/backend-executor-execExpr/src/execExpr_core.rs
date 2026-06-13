//! `execExpr-core` family — the expression-compiler dispatch + setup spine.
//!
//! Owns the public `ExecInit*` / `ExecBuild*Projection` / `ExecPrepare*` entry
//! points, the `ExecInitExprRec` opcode-emission switch (and its helpers
//! `ExprEvalPushStep` / `ExecCreateExprSetupSteps` / `expr_setup_walker` /
//! `ExecComputeSlotInfo` / `ExecReadyExpr`), and the plain `ExecQual` /
//! `ExecProject` / `ExecEvalExprSwitchContext` evaluation entry points. The
//! emission switch and the compiled program live in the EState's per-query
//! context; the interpreter (execExprInterp, the cycle partner) walks them.
//!
//! Result-location model: C threads raw `Datum *resv` / `bool *resnull`
//! pointers (usually `&state->resvalue` / `&state->resnull`) through the
//! recursion so several steps can share one output cell. The owned keystone
//! replaces those raw pointers with a per-`ExprState` [`ResultCellArena`]
//! (mirroring the `SlotId`/`EcxtId` precedent): every step's `resvalue`/
//! `resnull` is a [`ResultCellId`] index into the arena. The well-known cell
//! [`STATE_RESULT_CELL`] aliases the `ExprState`'s own `resvalue`/`resnull` —
//! the C `&state->resvalue` default target. Function arguments, CASE/domain
//! test values, and bool-step NULL trackers each allocate their own cells, so
//! `ExecInitExprRec` can thread distinct output targets exactly as C does.

use mcx::{Mcx, PgBox, PgVec};
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::execexpr::{
    ExprEvalOp, ExprEvalRowtypeCache, ExprEvalStep, ExprEvalStepData, ExprSetupInfo, ExprState,
    ProjectionInfo, ResultCell, ResultCellId, VarReturningType, EEO_FLAG_HAS_NEW, EEO_FLAG_HAS_OLD,
    EEO_FLAG_IS_QUAL, STATE_RESULT_CELL,
};
use types_nodes::execnodes::PlanStateData;
use types_nodes::nodehashjoin::HashJoinState;
use types_nodes::parsestmt::ParamListInfoHandle;
use types_nodes::primnodes::{
    BoolExprType, Expr, NullTestType, ParamKind, VarReturningType as VrtKind,
};
use types_nodes::{EStateData, EcxtId, SlotId};
use types_tuple::heaptuple::{ItemPointerData, TupleDescData};

use backend_executor_execExpr_seams::HashJoinQualKind;

/// `#define INNER_VAR (-1)` (primnodes.h special varnos).
const INNER_VAR: i32 = -1;
/// `#define OUTER_VAR (-2)`.
const OUTER_VAR: i32 = -2;
/// `#define INDEX_VAR (-3)` — handled by the `default` switch arm in C.
#[allow(dead_code)]
const INDEX_VAR: i32 = -3;

// ===========================================================================
// makeNode(ExprState) + result-cell arena helpers + ExprEvalPushStep +
// ExecReadyExpr (spine primitives)
// ===========================================================================

/// `makeNode(ExprState)` (execExpr.c) — a fresh, empty `ExprState`. The
/// well-known [`STATE_RESULT_CELL`] (the `&state->resvalue` target) is allocated
/// lazily by [`ensure_result_arena`] on first use.
fn make_expr_state<'mcx>() -> ExprState<'mcx> {
    ExprState::default()
}

/// Allocate the result-cell arena and its well-known [`STATE_RESULT_CELL`] if it
/// has not been allocated yet. Idempotent; called before any cell is allocated
/// or any step pushed.
fn ensure_result_arena<'mcx>(mcx: Mcx<'mcx>, state: &mut ExprState<'mcx>) -> PgResult<()> {
    if state.result_cells.cells.is_none() {
        let mut cells = mcx::vec_with_capacity_in(mcx, 1)?;
        // cell 0 == STATE_RESULT_CELL (the ExprState's own resvalue/resnull).
        cells.push(ResultCell::default());
        state.result_cells.cells = Some(cells);
    }
    Ok(())
}

/// Allocate a fresh result cell in `state`'s arena and return its
/// [`ResultCellId`] — the owned-model replacement for `palloc(sizeof(Datum))`
/// of a dedicated `Datum *`/`bool *` output target.
pub(crate) fn new_result_cell<'mcx>(mcx: Mcx<'mcx>, state: &mut ExprState<'mcx>) -> PgResult<ResultCellId> {
    ensure_result_arena(mcx, state)?;
    let cells = state.result_cells.cells.as_mut().unwrap();
    let id = ResultCellId(cells.len() as u32);
    cells.push(ResultCell::default());
    Ok(id)
}

/// `EEOP_DONE_RETURN` scratch step (the trailing step appended by every
/// expression compile that yields a value).
fn done_return_step<'mcx>(resv: ResultCellId) -> ExprEvalStep<'mcx> {
    ExprEvalStep {
        opcode: ExprEvalOp::EEOP_DONE_RETURN,
        resvalue: resv,
        resnull: resv,
        d: ExprEvalStepData::NoPayload,
    }
}

/// `EEOP_DONE_NO_RETURN` scratch step — the trailing step appended by a
/// projection compile (it assigns into the result slot rather than returning a
/// scalar value, so there is nothing to return).
fn done_no_return_step<'mcx>() -> ExprEvalStep<'mcx> {
    ExprEvalStep {
        opcode: ExprEvalOp::EEOP_DONE_NO_RETURN,
        resvalue: STATE_RESULT_CELL,
        resnull: STATE_RESULT_CELL,
        d: ExprEvalStepData::NoPayload,
    }
}

/// `ExprEvalPushStep(es, s)` (execExpr.c) — append a step to `es->steps`,
/// growing the array on the same 16-then-doubling schedule as C's
/// `palloc`/`repalloc` and keeping `steps_len`/`steps_alloc` in lock-step.
///
/// In C the steps array is a raw `palloc`'d block; here it is the owned
/// `PgVec<ExprEvalStep>` field, charged against the EState's per-query context.
pub fn expr_eval_push_step<'mcx>(
    mcx: Mcx<'mcx>,
    es: &mut ExprState<'mcx>,
    s: ExprEvalStep<'mcx>,
) -> PgResult<()> {
    if es.steps.is_none() {
        es.steps = Some(mcx::vec_with_capacity_in(mcx, 0)?);
    }
    let step_size = core::mem::size_of::<ExprEvalStep<'mcx>>();
    if es.steps_alloc == 0 {
        es.steps_alloc = 16;
        es.steps
            .as_mut()
            .unwrap()
            .try_reserve(16)
            .map_err(|_| mcx.oom(16 * step_size))?;
    } else if es.steps_alloc == es.steps_len {
        es.steps_alloc *= 2;
        let want = es.steps_alloc as usize;
        let have = es.steps.as_ref().unwrap().len();
        es.steps
            .as_mut()
            .unwrap()
            .try_reserve(want - have)
            .map_err(|_| mcx.oom((want - have) * step_size))?;
    }
    es.steps.as_mut().unwrap().push(s);
    es.steps_len += 1;
    Ok(())
}

/// `ExecReadyExpr(state)` (execExpr.c) — prepare a freshly compiled expression
/// for execution. JIT is not yet modeled; route to the interpreter's
/// `ExecReadyInterpretedExpr` (execExprInterp, the cycle partner).
fn exec_ready_expr<'mcx>(state: &mut ExprState<'mcx>) -> PgResult<()> {
    // C: if (jit_compile_expr(state)) return;  (JIT unported)
    backend_executor_execExprInterp_seams::exec_ready_interpreted_expr::call(state)
}

// ===========================================================================
// Setup-step prescan: expr_setup_walker / ExecComputeSlotInfo /
// ExecPushExprSetupSteps / ExecCreateExprSetupSteps
// ===========================================================================

/// `expr_setup_walker(node, info)` (execExpr.c) — accumulate the highest
/// attnum referenced from each input slot. The C walker descends the whole tree
/// (expression_tree_walker); here we descend the modeled child links of every
/// `Expr` variant. Aggref / WindowFunc / GroupingFunc argument lists are NOT
/// descended into (their args are evaluated separately), matching C, and
/// SubPlan is handled by accumulating the MULTIEXPR count (not modeled deeply).
fn expr_setup_walker(node: &Expr, info: &mut ExprSetupInfo) {
    match node {
        Expr::Var(variable) => {
            let attnum = variable.varattno;
            match variable.varno {
                INNER_VAR => {
                    info.last_attnums.last_inner = info.last_attnums.last_inner.max(attnum)
                }
                OUTER_VAR => {
                    info.last_attnums.last_outer = info.last_attnums.last_outer.max(attnum)
                }
                _ => info.last_attnums.last_scan = info.last_attnums.last_scan.max(attnum),
            }
        }
        // Pure-leaf nodes — no child expressions to descend.
        Expr::Const(_)
        | Expr::Param(_)
        | Expr::CaseTestExpr(_)
        | Expr::CoerceToDomainValue(_)
        | Expr::SetToDefault(_)
        | Expr::CurrentOfExpr(_)
        | Expr::NextValueExpr(_)
        | Expr::SQLValueFunction(_)
        | Expr::Aggref(_)
        | Expr::GroupingFunc(_)
        | Expr::WindowFunc(_)
        | Expr::MergeSupportFunc(_) => {}
        // Single-arg passthrough / coercion nodes.
        Expr::RelabelType(e) => descend_opt(e.arg.as_deref(), info),
        Expr::CollateExpr(e) => descend_opt(e.arg.as_deref(), info),
        Expr::CoerceViaIO(e) => descend_opt(e.arg.as_deref(), info),
        Expr::ConvertRowtypeExpr(e) => descend_opt(e.arg.as_deref(), info),
        Expr::FieldSelect(e) => descend_opt(e.arg.as_deref(), info),
        Expr::NamedArgExpr(e) => descend_opt(e.arg.as_deref(), info),
        Expr::NullTest(e) => descend_opt(e.arg.as_deref(), info),
        Expr::BooleanTest(e) => descend_opt(e.arg.as_deref(), info),
        Expr::CoerceToDomain(e) => descend_opt(e.arg.as_deref(), info),
        Expr::ArrayCoerceExpr(e) => descend_opt(e.arg.as_deref(), info),
        // Operator / function nodes — descend their argument lists.
        Expr::FuncExpr(e) => descend_list(&e.args, info),
        Expr::OpExpr(e) | Expr::DistinctExpr(e) | Expr::NullIfExpr(e) => {
            descend_list(&e.args, info)
        }
        Expr::BoolExpr(e) => descend_list(&e.args, info),
        Expr::CoalesceExpr(e) => descend_list(&e.args, info),
        Expr::MinMaxExpr(e) => descend_list(&e.args, info),
        Expr::ArrayExpr(e) => descend_list(&e.elements, info),
        // CASE: arg + each WHEN's (expr,result) + ELSE.
        Expr::CaseExpr(e) => {
            descend_opt(e.arg.as_deref(), info);
            for w in &e.args {
                descend_opt(w.expr.as_deref(), info);
                descend_opt(w.result.as_deref(), info);
            }
            descend_opt(e.defresult.as_deref(), info);
        }
        // Remaining node kinds carry children but are routed to owner-family
        // panics in ExecInitExprRec; for the prescan we conservatively don't
        // need their attnums (they error before any FETCHSOME would be reached).
        _ => {}
    }
}

/// Helper: walk an optional boxed child expression.
fn descend_opt(node: Option<&Expr>, info: &mut ExprSetupInfo) {
    if let Some(n) = node {
        expr_setup_walker(n, info);
    }
}

/// Helper: walk a `Vec<Expr>` argument list.
fn descend_list(list: &[Expr], info: &mut ExprSetupInfo) {
    for n in list {
        expr_setup_walker(n, info);
    }
}

/// `ExecComputeSlotInfo(state, op)` (execExpr.c) — decide whether an
/// `EEOP_*_FETCHSOME` deform step is required and, if the slot type is fixed,
/// pin its descriptor/ops. With no `parent` (the only shape this family
/// compiles standalone) the slot is never fixed, so the step is always
/// required and stays in its non-fixed form — exactly the C `!parent` branch.
fn exec_compute_slot_info<'mcx>(state: &ExprState<'mcx>, op: &mut ExprEvalStep<'mcx>) -> bool {
    let _ = state;
    debug_assert!(matches!(
        op.opcode,
        ExprEvalOp::EEOP_INNER_FETCHSOME
            | ExprEvalOp::EEOP_OUTER_FETCHSOME
            | ExprEvalOp::EEOP_SCAN_FETCHSOME
            | ExprEvalOp::EEOP_OLD_FETCHSOME
            | ExprEvalOp::EEOP_NEW_FETCHSOME
    ));

    // The parent PlanState's slot-ops introspection (ExecGetResultSlotOps /
    // inneropsfixed / scanops...) is owned by execUtils/execProcnode; for a
    // parent-bearing compile that machinery routes through those seams. Until
    // then we mirror C's `!parent` branch: nothing is fixed, so leave the fetch
    // step non-fixed and keep it.
    if let ExprEvalStepData::Fetch {
        fixed,
        known_desc,
        kind,
        ..
    } = &mut op.d
    {
        if known_desc.is_none() {
            *fixed = false;
            *kind = None;
        }
    }
    true
}

/// `ExecPushExprSetupSteps(state, info)` (execExpr.c) — emit the leading
/// `EEOP_*_FETCHSOME` deform steps for each input slot referenced.
fn exec_push_expr_setup_steps<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    info: &ExprSetupInfo,
) -> PgResult<()> {
    let last = &info.last_attnums;
    for (opcode, last_var) in [
        (ExprEvalOp::EEOP_INNER_FETCHSOME, last.last_inner),
        (ExprEvalOp::EEOP_OUTER_FETCHSOME, last.last_outer),
        (ExprEvalOp::EEOP_SCAN_FETCHSOME, last.last_scan),
    ] {
        if last_var > 0 {
            let mut scratch = ExprEvalStep {
                opcode,
                resvalue: STATE_RESULT_CELL,
                resnull: STATE_RESULT_CELL,
                d: ExprEvalStepData::Fetch {
                    last_var: last_var as i32,
                    fixed: false,
                    known_desc: None,
                    kind: None,
                },
            };
            if exec_compute_slot_info(state, &mut scratch) {
                expr_eval_push_step(mcx, state, scratch)?;
            }
        }
    }

    if info.multiexpr_subplans != 0 {
        panic!(
            "execExpr-core: MULTIEXPR SubPlan setup not ported (needs execExpr_func_subscript \
             ExecInitSubPlanExpr + the SubPlan node state)"
        );
    }
    Ok(())
}

/// `ExecCreateExprSetupSteps(state, node)` (execExpr.c).
fn exec_create_expr_setup_steps<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    node: &Expr,
) -> PgResult<()> {
    let mut info = ExprSetupInfo::default();
    expr_setup_walker(node, &mut info);
    exec_push_expr_setup_steps(mcx, state, &info)
}

/// `ExecCreateExprSetupSteps(state, (Node *) targetList)` over a projection
/// target list — `expr_setup_walker` descends each non-NULL `TargetEntry.expr`
/// (the C walker recurses into the `TargetEntry` nodes of the list and on into
/// their `expr` children).
fn exec_create_expr_setup_steps_tlist<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    target_list: &[types_nodes::TargetEntry<'mcx>],
) -> PgResult<()> {
    let mut info = ExprSetupInfo::default();
    for tle in target_list {
        if let Some(e) = tle.expr.as_deref() {
            expr_setup_walker(e, &mut info);
        }
    }
    exec_push_expr_setup_steps(mcx, state, &info)
}

/// `ExecCreateExprSetupSteps(state, (Node *) list)` over a qual list.
fn exec_create_expr_setup_steps_list<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    nodes: &[Expr],
) -> PgResult<()> {
    let mut info = ExprSetupInfo::default();
    for node in nodes {
        expr_setup_walker(node, &mut info);
    }
    exec_push_expr_setup_steps(mcx, state, &info)
}

// ExecInitFunc lives in execExpr_func_subscript (this crate's func/subscript
// family); the dispatch arms below route FuncExpr/OpExpr/DistinctExpr/NullIfExpr
// through `crate::execExpr_func_subscript::exec_init_func`.

// ===========================================================================
// ExecInitExprRec — the opcode-emission switch
// ===========================================================================

/// `ExecInitExprRec(node, state, resv, resnull)` (execExpr.c) — append the
/// steps that evaluate `node`, leaving the result in the caller's output cell
/// (`resv`, a [`ResultCellId`] into `state`'s arena).
pub(crate) fn exec_init_expr_rec<'mcx>(
    mcx: Mcx<'mcx>,
    node: &Expr,
    state: &mut ExprState<'mcx>,
    resv: ResultCellId,
) -> PgResult<()> {
    // C: check_stack_depth(); — guarded by the host stack here.
    match node {
        // ----- T_Var -----
        Expr::Var(variable) => {
            let mut scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_SCAN_VAR, // set below
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::Var {
                    attnum: 0,
                    vartype: variable.vartype,
                    varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
                },
            };

            if variable.varattno == types_core::InvalidAttrNumber {
                // Whole-row Var: ExecInitWholeRowVar fills the EEOP_WHOLEROW
                // scratch (owned by the func/subscript family), then we push it.
                let mut scratch = scratch_for(resv);
                crate::execExpr_func_subscript::exec_init_whole_row_var(
                    mcx,
                    &mut scratch,
                    variable,
                    state,
                )?;
                expr_eval_push_step(mcx, state, scratch)?;
                return Ok(());
            } else if variable.varattno <= 0 {
                // system column
                set_var_payload(&mut scratch, variable.varattno as i32, variable.vartype);
                scratch.opcode = match variable.varno {
                    INNER_VAR => ExprEvalOp::EEOP_INNER_SYSVAR,
                    OUTER_VAR => ExprEvalOp::EEOP_OUTER_SYSVAR,
                    _ => sysvar_opcode_for(state, VrtKind::VAR_RETURNING_DEFAULT),
                };
            } else {
                // regular user column
                set_var_payload(
                    &mut scratch,
                    variable.varattno as i32 - 1,
                    variable.vartype,
                );
                scratch.opcode = match variable.varno {
                    INNER_VAR => ExprEvalOp::EEOP_INNER_VAR,
                    OUTER_VAR => ExprEvalOp::EEOP_OUTER_VAR,
                    _ => var_opcode_for(state, VrtKind::VAR_RETURNING_DEFAULT),
                };
            }
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_Const -----
        Expr::Const(con) => {
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_CONST,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::ConstVal {
                    value: con.constvalue,
                    isnull: con.constisnull,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_Param -----
        Expr::Param(param) => {
            match param.paramkind {
                ParamKind::PARAM_EXEC => {
                    let scratch = ExprEvalStep {
                        opcode: ExprEvalOp::EEOP_PARAM_EXEC,
                        resvalue: resv,
                        resnull: resv,
                        d: ExprEvalStepData::Param {
                            paramid: param.paramid,
                            paramtype: param.paramtype,
                        },
                    };
                    expr_eval_push_step(mcx, state, scratch)?;
                }
                ParamKind::PARAM_EXTERN => {
                    // If a paramCompile hook were present (ext_params /
                    // parent->state->es_param_list_info) it would be used; the
                    // owned model does not thread the hook here, so emit the
                    // standard EEOP_PARAM_EXTERN step (C's else branch).
                    let scratch = ExprEvalStep {
                        opcode: ExprEvalOp::EEOP_PARAM_EXTERN,
                        resvalue: resv,
                        resnull: resv,
                        d: ExprEvalStepData::Param {
                            paramid: param.paramid,
                            paramtype: param.paramtype,
                        },
                    };
                    expr_eval_push_step(mcx, state, scratch)?;
                }
                other => {
                    return Err(types_error::PgError::error(format!(
                        "unrecognized paramkind: {}",
                        other as i32
                    )));
                }
            }
            Ok(())
        }

        // ----- T_Aggref -----
        Expr::Aggref(aggref) => {
            // The parent AggState->aggs accumulation is owned by nodeAgg; the
            // owned model lends the parent explicitly. Emit the EEOP_AGGREF
            // step (the planner-set aggno drives it); the aggs-list append is
            // performed by the nodeAgg owner when it threads the parent.
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_AGGREF,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::Aggref {
                    aggno: aggref.aggno,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_GroupingFunc -----
        Expr::GroupingFunc(grp) => {
            // C reads agg->groupingSets off the parent Agg plan to decide
            // whether to carry the cols; without the threaded parent we carry
            // the cols (the common grouping-sets case), matching the EXPLAIN
            // semantics. The interpreter consults the parent at runtime.
            let mut clauses = mcx::vec_with_capacity_in(mcx, grp.cols.len())?;
            for &c in &grp.cols {
                clauses.push(c);
            }
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_GROUPING_FUNC,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::GroupingFunc {
                    clauses: Some(clauses),
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_MergeSupportFunc -----
        Expr::MergeSupportFunc(_) => {
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_MERGE_SUPPORT_FUNC,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::NoPayload,
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_FuncExpr / T_OpExpr / T_DistinctExpr / T_NullIfExpr -----
        Expr::FuncExpr(func) => {
            let mut scratch = scratch_for(resv);
            crate::execExpr_func_subscript::exec_init_func(
                mcx,
                &mut scratch,
                node,
                &func.args,
                func.funcid,
                func.inputcollid,
                state,
            )?;
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        // OpExpr/DistinctExpr/NullIfExpr: C looks up op->opfuncid (the operator's
        // implementing function) before ExecInitFunc. The keystone OpExpr is
        // trimmed to opno/args, so the opfuncid comes from get_opcode(opno)
        // (lsyscache owner). ExecInitFunc structurally blocks on fmgr_info
        // anyway; pass opno as the funcid placeholder (InvalidOid inputcollid)
        // so the dispatch is shaped, then panic in ExecInitFunc.
        Expr::OpExpr(op) => {
            let mut scratch = scratch_for(resv);
            crate::execExpr_func_subscript::exec_init_func(mcx, &mut scratch, node, &op.args, op.opno, types_core::InvalidOid, state)?;
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        Expr::DistinctExpr(op) => {
            let mut scratch = scratch_for(resv);
            crate::execExpr_func_subscript::exec_init_func(mcx, &mut scratch, node, &op.args, op.opno, types_core::InvalidOid, state)?;
            scratch.opcode = ExprEvalOp::EEOP_DISTINCT;
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        Expr::NullIfExpr(op) => {
            let mut scratch = scratch_for(resv);
            crate::execExpr_func_subscript::exec_init_func(mcx, &mut scratch, node, &op.args, op.opno, types_core::InvalidOid, state)?;
            scratch.opcode = ExprEvalOp::EEOP_NULLIF;
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_NamedArgExpr (transparent wrapper around its arg) -----
        Expr::NamedArgExpr(nae) => {
            let arg = nae
                .arg
                .as_deref()
                .expect("NamedArgExpr.arg must be present");
            exec_init_expr_rec(mcx, arg, state, resv)
        }

        // ----- T_RelabelType (no-op coercion) -----
        Expr::RelabelType(relabel) => {
            let arg = relabel.arg.as_deref().expect("RelabelType.arg present");
            exec_init_expr_rec(mcx, arg, state, resv)
        }

        // ----- T_CollateExpr (planner removes it; transparent if seen) -----
        Expr::CollateExpr(collate) => {
            let arg = collate.arg.as_deref().expect("CollateExpr.arg present");
            exec_init_expr_rec(mcx, arg, state, resv)
        }

        // ----- T_BoolExpr -----
        Expr::BoolExpr(boolexpr) => {
            let nargs = boolexpr.args.len();
            // allocate scratch NULL-tracker cell shared by all AND/OR steps
            let anynull = if boolexpr.boolop != BoolExprType::NOT_EXPR {
                new_result_cell(mcx, state)?
            } else {
                STATE_RESULT_CELL // unused for NOT
            };

            let mut adjust_jumps: PgVec<'mcx, usize> = mcx::vec_with_capacity_in(mcx, nargs)?;
            for (off, arg) in boolexpr.args.iter().enumerate() {
                // Evaluate argument into our output variable.
                exec_init_expr_rec(mcx, arg, state, resv)?;

                let opcode = match boolexpr.boolop {
                    BoolExprType::AND_EXPR => {
                        if off == 0 {
                            ExprEvalOp::EEOP_BOOL_AND_STEP_FIRST
                        } else if off + 1 == nargs {
                            ExprEvalOp::EEOP_BOOL_AND_STEP_LAST
                        } else {
                            ExprEvalOp::EEOP_BOOL_AND_STEP
                        }
                    }
                    BoolExprType::OR_EXPR => {
                        if off == 0 {
                            ExprEvalOp::EEOP_BOOL_OR_STEP_FIRST
                        } else if off + 1 == nargs {
                            ExprEvalOp::EEOP_BOOL_OR_STEP_LAST
                        } else {
                            ExprEvalOp::EEOP_BOOL_OR_STEP
                        }
                    }
                    BoolExprType::NOT_EXPR => ExprEvalOp::EEOP_BOOL_NOT_STEP,
                };
                let scratch = ExprEvalStep {
                    opcode,
                    resvalue: resv,
                    resnull: resv,
                    d: ExprEvalStepData::BoolExpr {
                        anynull,
                        jumpdone: -1,
                    },
                };
                expr_eval_push_step(mcx, state, scratch)?;
                adjust_jumps.push((state.steps_len - 1) as usize);
            }

            // adjust jump targets
            let target = state.steps_len;
            let steps = state.steps.as_mut().expect("boolexpr steps");
            for &j in adjust_jumps.iter() {
                if let ExprEvalStepData::BoolExpr { jumpdone, .. } = &mut steps[j].d {
                    debug_assert_eq!(*jumpdone, -1);
                    *jumpdone = target;
                }
            }
            Ok(())
        }

        // ----- T_CaseExpr -----
        Expr::CaseExpr(caseexpr) => {
            // If there's a test expression, C evaluates it into a caseval/
            // casenull workspace cell and (only if get_typlen(exprType(arg)) ==
            // -1, i.e. a varlena that could be an expanded datum) emits an
            // EEOP_MAKE_READONLY over it. The varlena decision needs
            // exprType (nodeFuncs) + get_typlen (lsyscache owner) — and no
            // get_typlen seam is exported yet, so a simple CASE (with arg) would
            // require guessing the R/O step. Per "mirror PG and panic", route
            // the simple-CASE form loudly; searched CASE (no arg, the common
            // form) is ported in full below.
            let case_cell = if caseexpr.arg.is_some() {
                panic!(
                    "execExpr-core: simple CASE (CaseExpr with a test arg) needs \
                     get_typlen(exprType(arg)) to decide the EEOP_MAKE_READONLY R/O step \
                     (lsyscache owner seam not exported); searched CASE is ported"
                );
            } else {
                None
            };

            let mut adjust_jumps: PgVec<'mcx, usize> =
                mcx::vec_with_capacity_in(mcx, caseexpr.args.len())?;

            for when in &caseexpr.args {
                // Make testexpr result available to CaseTestExpr nodes within
                // the condition (save/restore innermost_caseval).
                let save = state.innermost_caseval;
                state.innermost_caseval = case_cell;

                let cond = when.expr.as_deref().expect("CaseWhen.expr present");
                exec_init_expr_rec(mcx, cond, state, resv)?;

                state.innermost_caseval = save;

                // If WHEN result isn't true, jump to next CASE arm.
                let scratch = ExprEvalStep {
                    opcode: ExprEvalOp::EEOP_JUMP_IF_NOT_TRUE,
                    resvalue: resv,
                    resnull: resv,
                    d: ExprEvalStepData::Jump { jumpdone: -1 },
                };
                expr_eval_push_step(mcx, state, scratch)?;
                let whenstep = (state.steps_len - 1) as usize;

                // If true, evaluate THEN result into CASE's result variables.
                let result = when.result.as_deref().expect("CaseWhen.result present");
                exec_init_expr_rec(mcx, result, state, resv)?;

                // Emit JUMP to end of CASE.
                let scratch = ExprEvalStep {
                    opcode: ExprEvalOp::EEOP_JUMP,
                    resvalue: resv,
                    resnull: resv,
                    d: ExprEvalStepData::Jump { jumpdone: -1 },
                };
                expr_eval_push_step(mcx, state, scratch)?;
                adjust_jumps.push((state.steps_len - 1) as usize);

                // Set WHEN test's jump target to the next arm.
                let next = state.steps_len;
                if let ExprEvalStepData::Jump { jumpdone } =
                    &mut state.steps.as_mut().unwrap()[whenstep].d
                {
                    *jumpdone = next;
                }
            }

            // transformCaseExpr always adds a default; evaluate ELSE.
            let defresult = caseexpr.defresult.as_deref().expect("CASE defresult present");
            exec_init_expr_rec(mcx, defresult, state, resv)?;

            // adjust jump targets to the end.
            let target = state.steps_len;
            let steps = state.steps.as_mut().unwrap();
            for &j in adjust_jumps.iter() {
                if let ExprEvalStepData::Jump { jumpdone } = &mut steps[j].d {
                    debug_assert_eq!(*jumpdone, -1);
                    *jumpdone = target;
                }
            }
            Ok(())
        }

        // ----- T_CaseTestExpr -----
        Expr::CaseTestExpr(_) => {
            let scratch = match state.innermost_caseval {
                None => ExprEvalStep {
                    opcode: ExprEvalOp::EEOP_CASE_TESTVAL_EXT,
                    resvalue: resv,
                    resnull: resv,
                    d: ExprEvalStepData::NoPayload,
                },
                Some(cell) => ExprEvalStep {
                    opcode: ExprEvalOp::EEOP_CASE_TESTVAL,
                    resvalue: resv,
                    resnull: resv,
                    d: ExprEvalStepData::CaseTest { value: cell },
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_CoalesceExpr -----
        Expr::CoalesceExpr(coalesce) => {
            let mut adjust_jumps: PgVec<'mcx, usize> =
                mcx::vec_with_capacity_in(mcx, coalesce.args.len())?;
            for e in &coalesce.args {
                // evaluate argument directly into result datum
                exec_init_expr_rec(mcx, e, state, resv)?;
                // if not null, skip to end
                let scratch = ExprEvalStep {
                    opcode: ExprEvalOp::EEOP_JUMP_IF_NOT_NULL,
                    resvalue: resv,
                    resnull: resv,
                    d: ExprEvalStepData::Jump { jumpdone: -1 },
                };
                expr_eval_push_step(mcx, state, scratch)?;
                adjust_jumps.push((state.steps_len - 1) as usize);
            }
            let target = state.steps_len;
            let steps = state.steps.as_mut().unwrap();
            for &j in adjust_jumps.iter() {
                if let ExprEvalStepData::Jump { jumpdone } = &mut steps[j].d {
                    debug_assert_eq!(*jumpdone, -1);
                    *jumpdone = target;
                }
            }
            Ok(())
        }

        // ----- T_SQLValueFunction -----
        Expr::SQLValueFunction(_svf) => {
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_SQLVALUEFUNCTION,
                resvalue: resv,
                resnull: resv,
                // The original SQLValueFunction node is parked as an opaque
                // address in the keystone payload; the interpreter reads op/type
                // off the node. Not threaded here yet — but the step shape and
                // dispatch are correct.
                d: ExprEvalStepData::SqlValueFunction { svf: 0 },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_NullTest -----
        Expr::NullTest(ntest) => {
            if ntest.argisrow {
                // Row null-test needs a rowcache and the composite-deform path
                // (ExecEvalRowNull[NotNull]); the rowcache lives in the step,
                // but the runtime tupdesc lookup is the typcache owner's. The
                // scalar path below is fully ported; the row path routes loudly.
                panic!(
                    "execExpr-core: row NullTest (argisrow) needs the composite-type rowcache \
                     deform path (typcache owner); scalar NullTest is ported"
                );
            }
            let opcode = match ntest.nulltesttype {
                NullTestType::IS_NULL => ExprEvalOp::EEOP_NULLTEST_ISNULL,
                NullTestType::IS_NOT_NULL => ExprEvalOp::EEOP_NULLTEST_ISNOTNULL,
            };
            // first evaluate argument into result variable
            let arg = ntest.arg.as_deref().expect("NullTest.arg present");
            exec_init_expr_rec(mcx, arg, state, resv)?;
            // then push the test
            let scratch = ExprEvalStep {
                opcode,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::NullTestRow {
                    rowcache: ExprEvalRowtypeCache::default(),
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_BooleanTest -----
        Expr::BooleanTest(btest) => {
            use types_nodes::primnodes::BoolTestType;
            // Evaluate argument directly into result datum.
            let arg = btest.arg.as_deref().expect("BooleanTest.arg present");
            exec_init_expr_rec(mcx, arg, state, resv)?;
            let opcode = match btest.booltesttype {
                BoolTestType::IS_TRUE => ExprEvalOp::EEOP_BOOLTEST_IS_TRUE,
                BoolTestType::IS_NOT_TRUE => ExprEvalOp::EEOP_BOOLTEST_IS_NOT_TRUE,
                BoolTestType::IS_FALSE => ExprEvalOp::EEOP_BOOLTEST_IS_FALSE,
                BoolTestType::IS_NOT_FALSE => ExprEvalOp::EEOP_BOOLTEST_IS_NOT_FALSE,
                BoolTestType::IS_UNKNOWN => ExprEvalOp::EEOP_NULLTEST_ISNULL,
                BoolTestType::IS_NOT_UNKNOWN => ExprEvalOp::EEOP_NULLTEST_ISNOTNULL,
            };
            let scratch = ExprEvalStep {
                opcode,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::NoPayload,
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_CurrentOfExpr -----
        Expr::CurrentOfExpr(_) => {
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_CURRENTOFEXPR,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::NoPayload,
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_NextValueExpr -----
        Expr::NextValueExpr(nve) => {
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_NEXTVALUEEXPR,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::NextValueExpr {
                    seqid: nve.seqid,
                    seqtypid: nve.typeId,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_FieldSelect -----
        Expr::FieldSelect(fselect) => {
            // Evaluate the input rowtype value into the result cell, then the
            // FIELDSELECT step extracts the field. The rowcache is filled at
            // runtime (the typcache owner supplies the descriptor); the step
            // shape and the arg recursion are own logic.
            let arg = fselect.arg.as_deref().expect("FieldSelect.arg present");
            exec_init_expr_rec(mcx, arg, state, resv)?;
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_FIELDSELECT,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::FieldSelect {
                    fieldnum: fselect.fieldnum,
                    resulttype: fselect.resulttype,
                    rowcache: ExprEvalRowtypeCache::default(),
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- nodes routed to owner families / structural blockers -----
        Expr::ScalarArrayOpExpr(_) => panic!(
            "execExpr-core: ScalarArrayOpExpr needs the fmgr_info seam + (for the hashed form) \
             typcache; per-arg fcinfo cells are modeled. Port lands with the fmgr_info seam."
        ),
        Expr::MinMaxExpr(_) => panic!(
            "execExpr-core: MinMaxExpr needs lookup_type_cache(TYPECACHE_CMP_PROC) + fmgr_info \
             (typcache/fmgr owner seams)"
        ),
        Expr::ArrayExpr(_) => panic!(
            "execExpr-core: ArrayExpr needs get_typlenbyvalalign element-type info (lsyscache \
             owner seam) for the per-element workspace; arg recursion is modeled"
        ),
        Expr::RowExpr(_) => panic!(
            "execExpr-core: RowExpr needs the result tupdesc (BlessTupleDesc/typcache owner)"
        ),
        Expr::RowCompareExpr(_) => panic!(
            "execExpr-core: RowCompareExpr needs per-column fmgr_info comparison lookups (fmgr \
             owner seam)"
        ),
        Expr::SubscriptingRef(sbsref) => {
            let mut scratch = scratch_for(resv);
            crate::execExpr_func_subscript::exec_init_subscripting_ref(
                mcx, &mut scratch, sbsref, state, resv,
            )
        }
        Expr::CoerceViaIO(_) => panic!(
            "execExpr-core: CoerceViaIO needs getTypeOutputInfo/getTypeInputInfo + fmgr_info \
             (lsyscache/fmgr owner seams); owned by execExpr_func_subscript"
        ),
        Expr::ArrayCoerceExpr(_) => panic!(
            "execExpr-core: ArrayCoerceExpr needs the per-element ExprState + array_map state \
             (execExpr_func_subscript / arrayfuncs owner seams)"
        ),
        Expr::ConvertRowtypeExpr(_) => panic!(
            "execExpr-core: ConvertRowtypeExpr needs the in/out rowtype caches + TupleConversionMap \
             (typcache/tupconvert owner seams)"
        ),
        Expr::FieldStore(_) => panic!(
            "execExpr-core: FieldStore (DEFORM/FORM pair) needs the composite rowcache + column \
             workspace (typcache owner seam); owned by execExpr_func_subscript"
        ),
        Expr::CoerceToDomain(_) | Expr::CoerceToDomainValue(_) => panic!(
            "execExpr-core: domain coercion is owned by execExpr_domain_agg \
             (ExecInitCoerceToDomain) — needs the domain constraint list (typcache owner)"
        ),
        Expr::SubPlan(subplan) => {
            // `Expr::SubPlan` carries a `Box<SubPlan<'static>>` (the lifetime-free
            // Expr enum erases the arena lifetime); the SubPlan tree is allocated
            // in the EState per-query context, so reinstate the compiler's `'mcx`
            // to thread it (same lifetime-erasure precedent as the Opaque
            // carriers in this crate).
            let sub: &types_nodes::primnodes::SubPlan<'mcx> = unsafe {
                core::mem::transmute::<
                    &types_nodes::primnodes::SubPlan<'static>,
                    &types_nodes::primnodes::SubPlan<'mcx>,
                >(&subplan.0)
            };
            crate::execExpr_func_subscript::exec_init_sub_plan_expr(mcx, sub, state, resv)
        }
        Expr::AlternativeSubPlan(_) => panic!(
            "execExpr-core: AlternativeSubPlan must be replaced by a concrete SubPlan before \
             execution (planner: select cheapest alternative)"
        ),
        Expr::WindowFunc(_) => panic!(
            "execExpr-core: WindowFunc setup is owned by nodeWindowAgg (WindowFuncExprState); the \
             parent WindowAggState must be threaded"
        ),
        Expr::XmlExpr(_) | Expr::JsonValueExpr(_) | Expr::JsonConstructorExpr(_)
        | Expr::JsonIsPredicate(_) | Expr::JsonExpr(_) => panic!(
            "execExpr-core: XML/JSON expression compilation is owned by execExpr_json \
             (ExecInitJsonExpr / ExecInitJsonConstructor / xml)"
        ),
        Expr::SetToDefault(_) => panic!(
            "execExpr-core: SetToDefault must have been replaced before execution (planner); \
             reaching ExecInitExprRec with one is a planner error"
        ),
        Expr::SubLink(_) => panic!(
            "execExpr-core: SubLink is always replaced by a SubPlan before execution"
        ),
        Expr::InferenceElem(_) => panic!(
            "execExpr-core: InferenceElem is a planner-only unique-index inference node, never \
             compiled"
        ),
        Expr::ReturningExpr(_) => panic!(
            "execExpr-core: ReturningExpr compilation is owned by execExpr_modify"
        ),
        // #[non_exhaustive] guard.
        _ => panic!("execExpr-core: ExecInitExprRec — unhandled Expr node kind"),
    }
}

/// Helper: a zero-initialized scratch step targeting `resv`.
pub(crate) fn scratch_for<'mcx>(resv: ResultCellId) -> ExprEvalStep<'mcx> {
    ExprEvalStep {
        opcode: ExprEvalOp::EEOP_FUNCEXPR,
        resvalue: resv,
        resnull: resv,
        d: ExprEvalStepData::NoPayload,
    }
}

/// Set the `Var` payload's attnum/vartype (varreturningtype left default; the
/// keystone Var does not carry RETURNING old/new, so always DEFAULT).
fn set_var_payload(scratch: &mut ExprEvalStep<'_>, attnum: i32, vartype: types_core::Oid) {
    if let ExprEvalStepData::Var {
        attnum: a,
        vartype: vt,
        ..
    } = &mut scratch.d
    {
        *a = attnum;
        *vt = vartype;
    }
}

/// Pick the scan VAR opcode for the var's RETURNING type, setting the EEO flags
/// for OLD/NEW (the C `default` varno arm). The keystone Var carries no
/// RETURNING type, so this is always DEFAULT; kept for fidelity to the C switch.
fn var_opcode_for<'mcx>(state: &mut ExprState<'mcx>, vrt: VrtKind) -> ExprEvalOp {
    match vrt {
        VrtKind::VAR_RETURNING_DEFAULT => ExprEvalOp::EEOP_SCAN_VAR,
        VrtKind::VAR_RETURNING_OLD => {
            state.flags |= EEO_FLAG_HAS_OLD;
            ExprEvalOp::EEOP_OLD_VAR
        }
        VrtKind::VAR_RETURNING_NEW => {
            state.flags |= EEO_FLAG_HAS_NEW;
            ExprEvalOp::EEOP_NEW_VAR
        }
    }
}

/// Scan SYSVAR opcode for the var's RETURNING type (C `default` arm, sysvar).
fn sysvar_opcode_for<'mcx>(state: &mut ExprState<'mcx>, vrt: VrtKind) -> ExprEvalOp {
    match vrt {
        VrtKind::VAR_RETURNING_DEFAULT => ExprEvalOp::EEOP_SCAN_SYSVAR,
        VrtKind::VAR_RETURNING_OLD => {
            state.flags |= EEO_FLAG_HAS_OLD;
            ExprEvalOp::EEOP_OLD_SYSVAR
        }
        VrtKind::VAR_RETURNING_NEW => {
            state.flags |= EEO_FLAG_HAS_NEW;
            ExprEvalOp::EEOP_NEW_SYSVAR
        }
    }
}

// ===========================================================================
// Public ExecInit* entry points
// ===========================================================================

/// `ExecInitExpr(node, parent)` (execExpr.c) — compile one expression tree into
/// an executable [`ExprState`] in the EState's per-query context.
pub fn exec_init_expr<'mcx>(
    node: &Expr,
    parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let _ = parent;
    let mcx = estate.es_query_cxt;

    let mut state = make_expr_state();
    state.ext_params = 0;
    ensure_result_arena(mcx, &mut state)?;

    exec_create_expr_setup_steps(mcx, &mut state, node)?;
    exec_init_expr_rec(mcx, node, &mut state, STATE_RESULT_CELL)?;
    expr_eval_push_step(mcx, &mut state, done_return_step(STATE_RESULT_CELL))?;
    exec_ready_expr(&mut state)?;

    mcx::alloc_in(mcx, state)
}

/// `ExecInitExprWithParams(node, ext_params)` (execExpr.c).
pub fn exec_init_expr_with_params<'mcx>(
    node: &Expr,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let _ = econtext;
    let mcx = estate.es_query_cxt;

    let mut state = make_expr_state();
    state.ext_params = 0;
    ensure_result_arena(mcx, &mut state)?;

    exec_create_expr_setup_steps(mcx, &mut state, node)?;
    exec_init_expr_rec(mcx, node, &mut state, STATE_RESULT_CELL)?;
    expr_eval_push_step(mcx, &mut state, done_return_step(STATE_RESULT_CELL))?;
    exec_ready_expr(&mut state)?;

    mcx::alloc_in(mcx, state)
}

/// `ExecInitQual(qual, parent)` (execExpr.c) — compile an implicitly-ANDed qual
/// list into a single [`ExprState`]; empty qual → `None` (always-true).
pub fn exec_init_qual<'mcx>(
    qual: Option<&[Expr]>,
    parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    let _ = parent;
    let qual = match qual {
        None => return Ok(None),
        Some(q) if q.is_empty() => return Ok(None),
        Some(q) => q,
    };
    let mcx = estate.es_query_cxt;

    let mut state = make_expr_state();
    state.ext_params = 0;
    state.flags = EEO_FLAG_IS_QUAL;
    ensure_result_arena(mcx, &mut state)?;

    exec_create_expr_setup_steps_list(mcx, &mut state, qual)?;

    let mut adjust_jumps: PgVec<'mcx, usize> = mcx::vec_with_capacity_in(mcx, qual.len())?;
    for node in qual {
        exec_init_expr_rec(mcx, node, &mut state, STATE_RESULT_CELL)?;
        let scratch = ExprEvalStep {
            opcode: ExprEvalOp::EEOP_QUAL,
            resvalue: STATE_RESULT_CELL,
            resnull: STATE_RESULT_CELL,
            d: ExprEvalStepData::QualExpr { jumpdone: -1 },
        };
        expr_eval_push_step(mcx, &mut state, scratch)?;
        adjust_jumps.push((state.steps_len - 1) as usize);
    }

    let jump_target = state.steps_len;
    let steps = state.steps.as_mut().expect("qual has steps");
    for &jump in adjust_jumps.iter() {
        let as_step = &mut steps[jump];
        debug_assert!(matches!(as_step.opcode, ExprEvalOp::EEOP_QUAL));
        if let ExprEvalStepData::QualExpr { jumpdone } = &mut as_step.d {
            debug_assert_eq!(*jumpdone, -1);
            *jumpdone = jump_target;
        }
    }

    expr_eval_push_step(mcx, &mut state, done_return_step(STATE_RESULT_CELL))?;
    exec_ready_expr(&mut state)?;

    Ok(Some(mcx::alloc_in(mcx, state)?))
}

/// `ExecInitExprList(nodes, parent)` (execExpr.c).
pub fn exec_init_expr_list<'mcx>(
    nodes: &[Option<&Expr>],
    parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgVec<'mcx, Option<ExprState<'mcx>>>> {
    let mcx = estate.es_query_cxt;
    let mut result: PgVec<'mcx, Option<ExprState<'mcx>>> =
        mcx::vec_with_capacity_in(mcx, nodes.len())?;
    for e in nodes {
        match e {
            None => result.push(None),
            Some(node) => {
                let state = exec_init_expr(node, parent, estate)?;
                result.push(Some(PgBox::into_inner(state)));
            }
        }
    }
    Ok(result)
}

/// `ExecPrepareExpr(node, estate)` (execExpr.c) — compile a single expression
/// for use outside a normal executor node (parent = NULL).
///
/// C body:
/// ```c
/// oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
/// node = expression_planner(node);
/// result = ExecInitExpr(node, NULL);
/// MemoryContextSwitchTo(oldcontext);
/// return result;
/// ```
///
/// The `MemoryContextSwitchTo(estate->es_query_cxt)` is implicit here:
/// [`exec_init_expr`] already allocates the program in `estate.es_query_cxt`,
/// so there is no separate "current context" to switch into/out of. The
/// `parent = NULL` shape is matched by [`exec_init_expr_no_parent`] (which
/// threads no `PlanState`). The one genuine cross-unit callee is
/// `expression_planner(node)` (optimizer/planner.c — const-folding /
/// SQL-function inlining), which has no reachable owner seam in this crate's
/// dependency set; per mirror-PG-and-panic it loud-panics naming that owner.
pub fn exec_prepare_expr<'mcx>(
    node: &Expr,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    // node = expression_planner(node);
    //
    // expression_planner() (optimizer/planner.c) const-folds and inlines the
    // standalone expression before compilation. It is unported and has no
    // reachable owner seam; the rest of ExecPrepareExpr (the parent-less
    // ExecInitExpr compile, below) is this crate's own logic and is ready.
    let _ = estate;
    panic!(
        "execExpr-core: ExecPrepareExpr needs expression_planner (optimizer/planner.c, \
         unported — no reachable owner seam); the standalone compile that follows it is \
         ExecInitExpr(node, NULL), implemented here as exec_init_expr_no_parent. \
         expr node tag carried for the planner call: {:?}",
        core::mem::discriminant(node)
    );
}

/// `ExecInitExpr(node, NULL)` (execExpr.c) — the `parent = NULL` shape used by
/// [`exec_prepare_expr`] and `ExecInitExprWithParams`. Identical to
/// [`exec_init_expr`] but threads no `PlanState` (the owned spine already
/// ignores `parent` — see [`exec_init_expr`] — so this compiles the same
/// program; kept as a distinct entry to mirror the C `ExecInitExpr(node, NULL)`
/// call sites and to make the parent-less contract explicit).
pub fn exec_init_expr_no_parent<'mcx>(
    node: &Expr,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let mut state = make_expr_state();
    state.ext_params = 0;
    ensure_result_arena(mcx, &mut state)?;

    exec_create_expr_setup_steps(mcx, &mut state, node)?;
    exec_init_expr_rec(mcx, node, &mut state, STATE_RESULT_CELL)?;
    expr_eval_push_step(mcx, &mut state, done_return_step(STATE_RESULT_CELL))?;
    exec_ready_expr(&mut state)?;

    mcx::alloc_in(mcx, state)
}

/// `ExecPrepareExprList(exprList, estate)` (execExpr.c).
pub fn exec_prepare_expr_list<'mcx>(
    expr_list: &[Expr],
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>> {
    let mcx = estate.es_query_cxt;
    let mut result: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>> =
        mcx::vec_with_capacity_in(mcx, expr_list.len())?;
    for e in expr_list {
        result.push(exec_prepare_expr(e, estate)?);
    }
    Ok(result)
}

/// `ExecBuildProjectionInfo(targetList, econtext, slot, parent, inputDesc)`
/// (execExpr.c) — the full step-emission body, taking the C arguments
/// explicitly. This is the in-unit compiler the modify/partition seam wrappers
/// (execExpr_modify) call with their own target lists / slots.
///
/// `econtext` is the projection's expression context id (`pi_exprContext`);
/// `input_desc` is the optional cross-check descriptor; the result slot's
/// descriptor is not consulted during compilation (only at `ExecProject`
/// time), so it is not threaded here. The compiled program is embedded in the
/// returned [`ProjectionInfo`]'s `pi_state`.
///
/// Faithful to the C: for each tlist column, a "safe non-system Var" emits the
/// fast-path `EEOP_ASSIGN_*_VAR`; otherwise the column expression is compiled
/// into `state.resvalue`/`state.resnull` ([`STATE_RESULT_CELL`]) and moved with
/// `EEOP_ASSIGN_TMP[_MAKE_RO]` (the MAKE_RO arm chosen when the column type is
/// varlena — `get_typlen(exprType) == -1`).
pub fn exec_build_projection_info_impl<'mcx>(
    estate: &mut EStateData<'mcx>,
    target_list: &[types_nodes::TargetEntry<'mcx>],
    econtext: EcxtId,
    input_desc: Option<&TupleDescData<'_>>,
) -> PgResult<PgBox<'mcx, ProjectionInfo<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // ProjectionInfo *projInfo = makeNode(ProjectionInfo);
    let mut proj_info = ProjectionInfo::default();
    // projInfo->pi_exprContext = econtext;
    proj_info.pi_exprContext = Some(econtext);
    // state = &projInfo->pi_state;  state->expr = (Expr *) targetList;
    // state->parent = parent;  state->ext_params = NULL;  (the owned spine does
    // not thread parent/ext_params; mirrors ExecInitExpr's parent-ignoring path)
    let state = &mut proj_info.pi_state;
    state.ext_params = 0;
    ensure_result_arena(mcx, state)?;

    // ExecCreateExprSetupSteps(state, (Node *) targetList);
    exec_create_expr_setup_steps_tlist(mcx, state, target_list)?;

    // foreach(lc, targetList) — resno is positional (the planner assigns
    // sequential resnos 1..n over the tlist; the trimmed TargetEntry models it
    // positionally, see ExecTypeFromTLInternal's cur_resno).
    for (i, tle) in target_list.iter().enumerate() {
        let resno = (i as i32) + 1;
        let tle_expr = tle.expr.as_deref();

        // If tlist expression is a safe non-system Var, use ASSIGN_*_VAR.
        let mut variable: Option<&types_nodes::Var> = None;
        let mut attnum: i32 = 0;
        let mut is_safe_var = false;

        if let Some(Expr::Var(v)) = tle_expr {
            if v.varattno > 0 {
                variable = Some(v);
                attnum = v.varattno as i32;

                if input_desc.is_none() {
                    is_safe_var = true; // can't check, just assume OK
                } else {
                    let id = input_desc.unwrap();
                    if attnum <= id.natts {
                        // Form_pg_attribute attr = TupleDescAttr(inputDesc, attnum - 1);
                        let attr = id.attr((attnum - 1) as usize);
                        // If dropped or type-mismatched, don't use ASSIGN_*_VAR.
                        if !attr.attisdropped && v.vartype == attr.atttypid {
                            is_safe_var = true;
                        }
                    }
                }
            }
        }

        if is_safe_var {
            let v = variable.unwrap();
            let opcode = match v.varno {
                INNER_VAR => ExprEvalOp::EEOP_ASSIGN_INNER_VAR,
                OUTER_VAR => ExprEvalOp::EEOP_ASSIGN_OUTER_VAR,
                // INDEX_VAR handled by default case. The C switches on
                // variable->varreturningtype (DEFAULT/OLD/NEW) to pick
                // ASSIGN_SCAN_VAR / ASSIGN_OLD_VAR / ASSIGN_NEW_VAR (setting
                // EEO_FLAG_HAS_OLD/NEW). The trimmed keystone `Var` carries no
                // `varreturningtype` field, so — exactly as the non-assign Var
                // arm in `exec_init_expr_rec` does — this is always the DEFAULT
                // (SCAN) case.
                _ => ExprEvalOp::EEOP_ASSIGN_SCAN_VAR,
            };
            let scratch = ExprEvalStep {
                opcode,
                resvalue: STATE_RESULT_CELL,
                resnull: STATE_RESULT_CELL,
                d: ExprEvalStepData::AssignVar {
                    attnum: attnum - 1,
                    resultnum: resno - 1,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
        } else {
            // Compile the column expression into state->resvalue/resnull, then
            // move it into the result slot column.
            let expr = tle_expr.expect("ExecBuildProjectionInfo: NULL tlist expr");
            exec_init_expr_rec(mcx, expr, state, STATE_RESULT_CELL)?;

            // Force R/O but only if it could be an expanded datum:
            // get_typlen(exprType(tle->expr)) == -1.
            let typid = backend_nodes_nodeFuncs_seams::expr_type_info::call(expr)?.typid;
            let typlen = backend_utils_cache_lsyscache_seams::get_typlenbyvalalign::call(typid)?
                .typlen;
            let opcode = if typlen == -1 {
                ExprEvalOp::EEOP_ASSIGN_TMP_MAKE_RO
            } else {
                ExprEvalOp::EEOP_ASSIGN_TMP
            };
            let scratch = ExprEvalStep {
                opcode,
                resvalue: STATE_RESULT_CELL,
                resnull: STATE_RESULT_CELL,
                d: ExprEvalStepData::AssignTmp {
                    resultnum: resno - 1,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
        }
    }

    // scratch.opcode = EEOP_DONE_NO_RETURN; ExprEvalPushStep(state, &scratch);
    expr_eval_push_step(mcx, state, done_no_return_step())?;
    // ExecReadyExpr(state);
    exec_ready_expr(state)?;

    mcx::alloc_in(mcx, proj_info)
}

/// `ExecBuildProjectionInfo(...)` (execExpr.c) — seam-facing variant that reads
/// the target list off `planstate->plan->targetlist` and the result slot /
/// econtext off `planstate`. The per-column step emission is fully implemented
/// in [`exec_build_projection_info_impl`]; what is missing is the `Plan`
/// base-struct `targetlist` view in `types-nodes` (only specific scan plan
/// nodes model their own `targetlist`, not the generic `Plan.targetlist` the C
/// reads via `planstate->plan->targetlist`). Until that lands this routes
/// loudly; callers with an explicit target list use
/// [`exec_build_projection_info_impl`].
pub fn exec_build_projection_info<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    input_desc: Option<&TupleDescData<'_>>,
) -> PgResult<PgBox<'mcx, ProjectionInfo<'mcx>>> {
    let _ = (planstate, estate, input_desc);
    panic!(
        "execExpr-core: ExecBuildProjectionInfo(planstate) needs the generic Plan.targetlist \
         view in types-nodes (planstate->plan->targetlist) which is not modeled on the Node \
         enum; the per-column ExecInitExprRec recursion + EEOP_ASSIGN_* emission are implemented \
         in exec_build_projection_info_impl and reached by callers with an explicit target list"
    );
}

/// `ExecBuildUpdateProjection(targetList, evalTargetList, targetColnos, relDesc,
/// econtext, slot, parent)` (execExpr.c) — the full step-emission body, taking
/// the C arguments explicitly. This is the in-unit compiler the modify/partition
/// seam wrappers call. It also performs the `ExecCheckPlanOutput`-equivalent
/// sanity checks inline (column count / order, dropped columns, type match).
///
/// `rel_desc` describes the relation being updated; `econtext` is the
/// projection's expression context id. The compiled program is embedded in the
/// returned [`ProjectionInfo`]'s `pi_state`.
pub fn exec_build_update_projection_impl<'mcx>(
    estate: &mut EStateData<'mcx>,
    target_list: &[types_nodes::TargetEntry<'mcx>],
    eval_target_list: bool,
    target_colnos: &[i32],
    rel_desc: &TupleDescData<'_>,
    econtext: EcxtId,
) -> PgResult<PgBox<'mcx, ProjectionInfo<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // ProjectionInfo *projInfo = makeNode(ProjectionInfo);
    let mut proj_info = ProjectionInfo::default();
    proj_info.pi_exprContext = Some(econtext);
    let state = &mut proj_info.pi_state;
    // if (evalTargetList) state->expr = (Expr *) targetList; else state->expr = NULL;
    state.ext_params = 0;
    ensure_result_arena(mcx, state)?;

    // Count non-junk columns and verify junk comes last.
    let mut n_assignable_cols: i32 = 0;
    let mut saw_junk = false;
    for tle in target_list {
        if tle.resjunk {
            saw_junk = true;
        } else {
            if saw_junk {
                return Err(types_error::PgError::error(
                    "subplan target list is out of order".to_string(),
                ));
            }
            n_assignable_cols += 1;
        }
    }

    // We should have one targetColnos entry per non-junk column.
    if n_assignable_cols != target_colnos.len() as i32 {
        return Err(types_error::PgError::error(
            "targetColnos does not match subplan target list".to_string(),
        ));
    }

    // Build a bitmapset of the columns in targetColnos.
    let mut assigned_cols: Option<PgBox<'mcx, types_nodes::Bitmapset<'mcx>>> = None;
    for &targetattnum in target_colnos {
        assigned_cols = Some(backend_nodes_core_seams::bms_add_member::call(
            mcx,
            assigned_cols.take(),
            targetattnum,
        )?);
    }
    let assigned_cols_ref = assigned_cols.as_deref();

    // Insert EEOP_*_FETCHSOME steps. The scan tuple must be deconstructed at
    // least as far as the last old column we need: scan relDesc from the top
    // and stop at the first not-dropped, not-assigned column.
    let mut deform = ExprSetupInfo::default();
    let natts = rel_desc.natts;
    for attnum in (1..=natts).rev() {
        let attr = rel_desc.compact_attr((attnum - 1) as usize);
        if attr.attisdropped {
            continue;
        }
        if backend_nodes_core_seams::bms_is_member::call(attnum, assigned_cols_ref) {
            continue;
        }
        deform.last_attnums.last_scan = attnum as types_core::AttrNumber;
        break;
    }

    // If evaluating the tlist, incorporate its input requirements too; else
    // we'll just fetch the appropriate number of "outer" columns.
    if eval_target_list {
        for tle in target_list {
            if let Some(e) = tle.expr.as_deref() {
                expr_setup_walker(e, &mut deform);
            }
        }
    } else {
        deform.last_attnums.last_outer = n_assignable_cols as types_core::AttrNumber;
    }

    // ExecPushExprSetupSteps(state, &deform);
    exec_push_expr_setup_steps(mcx, state, &deform)?;

    // Generate code to evaluate/assign each non-junk tlist column. forboth()
    // iterates over exactly the non-junk columns (guaranteed by the order check
    // above), paired with targetColnos.
    let mut outerattnum: i32 = 0;
    let mut col_iter = target_colnos.iter();
    for tle in target_list {
        if tle.resjunk {
            continue;
        }
        let &targetattnum = col_iter
            .next()
            .expect("ExecBuildUpdateProjection: targetColnos exhausted");

        // ExecCheckPlanOutput-equivalent sanity checks.
        if targetattnum <= 0 || targetattnum > natts {
            return Err(types_error::PgError::error(
                "table row type and query-specified row type do not match: \
                 Query has too many columns."
                    .to_string(),
            ));
        }
        let attr = rel_desc.attr((targetattnum - 1) as usize);
        if attr.attisdropped {
            return Err(types_error::PgError::error(format!(
                "table row type and query-specified row type do not match: \
                 Query provides a value for a dropped column at ordinal position {}.",
                targetattnum
            )));
        }
        let tle_expr = tle
            .expr
            .as_deref()
            .expect("ExecBuildUpdateProjection: NULL tlist expr");
        let expr_typid = backend_nodes_nodeFuncs_seams::expr_type_info::call(tle_expr)?.typid;
        if expr_typid != attr.atttypid {
            return Err(types_error::PgError::error(format!(
                "table row type and query-specified row type do not match: \
                 Table has type oid {} at ordinal position {}, but query expects type oid {}.",
                attr.atttypid, targetattnum, expr_typid
            )));
        }

        // OK, generate code to perform the assignment.
        if eval_target_list {
            // Evaluate the TLE's expression into state->resvalue/resnull and
            // assign it (no "safe Var" fast-path, no R/O hoops — per the C).
            exec_init_expr_rec(mcx, tle_expr, state, STATE_RESULT_CELL)?;
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_ASSIGN_TMP,
                resvalue: STATE_RESULT_CELL,
                resnull: STATE_RESULT_CELL,
                d: ExprEvalStepData::AssignTmp {
                    resultnum: targetattnum - 1,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
        } else {
            // Just assign from the outer tuple.
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_ASSIGN_OUTER_VAR,
                resvalue: STATE_RESULT_CELL,
                resnull: STATE_RESULT_CELL,
                d: ExprEvalStepData::AssignVar {
                    attnum: outerattnum,
                    resultnum: targetattnum - 1,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
        }
        outerattnum += 1;
    }

    // Copy over any old columns that were not assigned to, and set dropped
    // columns to NULL.
    for attnum in 1..=natts {
        let attr = rel_desc.compact_attr((attnum - 1) as usize);
        if attr.attisdropped {
            // Put a null into the ExprState's resvalue/resnull ...
            let const_step = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_CONST,
                resvalue: STATE_RESULT_CELL,
                resnull: STATE_RESULT_CELL,
                d: ExprEvalStepData::ConstVal {
                    value: Datum::null(),
                    isnull: true,
                },
            };
            expr_eval_push_step(mcx, state, const_step)?;
            // ... then assign it to the result slot.
            let assign_step = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_ASSIGN_TMP,
                resvalue: STATE_RESULT_CELL,
                resnull: STATE_RESULT_CELL,
                d: ExprEvalStepData::AssignTmp {
                    resultnum: attnum - 1,
                },
            };
            expr_eval_push_step(mcx, state, assign_step)?;
        } else if !backend_nodes_core_seams::bms_is_member::call(attnum, assigned_cols_ref) {
            // Certainly the right type, so needn't check.
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_ASSIGN_SCAN_VAR,
                resvalue: STATE_RESULT_CELL,
                resnull: STATE_RESULT_CELL,
                d: ExprEvalStepData::AssignVar {
                    attnum: attnum - 1,
                    resultnum: attnum - 1,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
        }
    }

    // scratch.opcode = EEOP_DONE_NO_RETURN; ExprEvalPushStep(state, &scratch);
    expr_eval_push_step(mcx, state, done_no_return_step())?;
    // ExecReadyExpr(state);
    exec_ready_expr(state)?;

    mcx::alloc_in(mcx, proj_info)
}

/// `ExecBuildUpdateProjection(...)` (execExpr.c) — seam-facing variant that
/// stores the built `ri_projectNew` on the pooled `ResultRelInfo`. The full
/// step-emission body lives in [`exec_build_update_projection_impl`]; this
/// wrapper additionally needs the `ResultRelInfo.ri_projectNew` /
/// `ri_RootResultRelInfo` fields (the relation descriptor source and the
/// projection store target), which are not yet modeled on the pooled
/// `ResultRelInfo` in `types-nodes`. Until then it routes loudly; the modify
/// family reaches [`exec_build_update_projection_impl`] with explicit args.
pub fn exec_build_update_projection<'mcx>(
    mtstate: &mut types_nodes::ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    update_colnos: &[i32],
) -> PgResult<()> {
    let _ = (mtstate, estate, result_rel_info, update_colnos);
    panic!(
        "execExpr-core: ExecBuildUpdateProjection(resultRelInfo) needs the pooled \
         ResultRelInfo.ri_projectNew / ri_RootResultRelInfo fields (not yet modeled in \
         types-nodes) for the relation-descriptor source and projection store; the full \
         step-emission body is implemented in exec_build_update_projection_impl and reached \
         by callers with an explicit target list / relDesc"
    );
}

// ===========================================================================
// Evaluation entry points — dispatched to the interpreter (execExprInterp)
// ===========================================================================

/// `ExecEvalExprSwitchContext(state, econtext, &isnull)` (executor.h).
pub fn exec_eval_expr_switch_context<'mcx>(
    state: &ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    backend_executor_execExprInterp_seams::exec_eval_expr_switch_context::call(
        state, econtext, estate,
    )
}

/// `(ItemPointer) DatumGetPointer(ExecEvalExprSwitchContext(...))` (executor.h).
pub fn exec_eval_tid_expr_switch_context<'mcx>(
    state: &ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(ItemPointerData, bool)> {
    let _ = (state, econtext, estate);
    panic!(
        "execExpr-core: ExecEvalExprSwitchContext yielding an ItemPointer must be evaluated by \
         the interpreter (execExprInterp), which dereferences the result Datum"
    );
}

/// `ExecEvalExprSwitchContext(...)` yielding a `tid[]` array `Datum`.
pub fn exec_eval_array_expr_switch_context<'mcx>(
    state: &ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    backend_executor_execExprInterp_seams::exec_eval_expr_switch_context::call(
        state, econtext, estate,
    )
}

/// `ExecQual(state, econtext)` (executor.h).
pub fn exec_qual<'mcx>(
    state: &ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let (ret, isnull) = exec_eval_expr_switch_context(state, econtext, estate)?;
    Ok(!isnull && ret.as_bool())
}

/// `ExecQualAndReset(state, econtext)` (executor.h).
pub fn exec_qual_and_reset<'mcx>(
    state: &ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let ret = exec_qual(state, econtext, estate)?;
    backend_executor_execUtils_seams::reset_expr_context::call(estate, econtext)?;
    Ok(ret)
}

/// `ExecProject(projInfo)` (executor.h) — `ExecProject(planstate->ps_ProjInfo)`.
///
/// The C inline:
/// ```c
/// econtext = projInfo->pi_exprContext;
/// state = &projInfo->pi_state;
/// slot = state->resultslot;
/// ExecClearTuple(slot);
/// ExecEvalExprNoReturn(state, econtext);   // runs the EEOP_ASSIGN_* steps
/// slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
/// slot->tts_flags &= ~TTS_FLAG_EMPTY;
/// return slot;
/// ```
/// Two genuine cross-unit pieces are missing: (1) the embedded
/// `ExprState.resultslot` is a `PgBox<TupleTableSlot>` in the keystone model,
/// not a pool [`SlotId`], so the projection's result slot cannot be resolved to
/// the [`SlotId`] this seam must return (the keystone left `resultslot` as a
/// `PgBox`, unlike `resvalue`→`ResultCellId`); and (2) execExprInterp (the
/// cycle partner) exports only `exec_eval_expr_switch_context` — there is no
/// no-return projection-eval entry (`ExecEvalExprNoReturn`) that runs the
/// assign program and reports the written slot. Until those land this routes
/// loudly.
pub fn exec_project<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<SlotId> {
    let _ = (planstate, estate);
    panic!(
        "execExpr-core: ExecProject(ps_ProjInfo) needs (1) the projection result slot as a \
         pool SlotId — the keystone ExprState.resultslot is still a PgBox<TupleTableSlot>, not \
         a SlotId — and (2) the execExprInterp no-return projection-eval seam \
         (ExecEvalExprNoReturn); neither is exported yet"
    );
}

/// `ExecProject(proj_info)` (executor.h) of an explicitly-supplied projection.
/// Same C inline as [`exec_project`], over a caller-supplied `ProjectionInfo`
/// (the MERGE per-action `mas_proj` / RETURNING projections). Blocked on the
/// same two pieces: the `ExprState.resultslot` `SlotId` linkage and the
/// execExprInterp no-return projection-eval seam.
pub fn exec_project_info<'mcx>(
    proj_info: &ProjectionInfo<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<SlotId> {
    let _ = (proj_info, estate);
    panic!(
        "execExpr-core: ExecProject(projInfo) needs the projection result slot as a pool SlotId \
         (ExprState.resultslot is still a PgBox<TupleTableSlot>) and the execExprInterp \
         no-return projection-eval seam (ExecEvalExprNoReturn); neither is exported yet"
    );
}

/// `CreateExecutorState()` (execUtils.c) — a throwaway EState.
pub fn create_executor_state<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, EStateData<'mcx>>> {
    let _ = mcx;
    panic!(
        "execExpr-core: CreateExecutorState alias needs the execUtils seam \
         (backend-executor-execUtils owns CreateExecutorState; no seam exported yet)"
    );
}

/// `FreeExecutorState(estate)` (execUtils.c).
pub fn free_executor_state<'mcx>(estate: PgBox<'mcx, EStateData<'mcx>>) -> PgResult<()> {
    let _ = estate;
    panic!(
        "execExpr-core: FreeExecutorState alias needs the execUtils seam \
         (backend-executor-execUtils owns FreeExecutorState; no seam exported yet)"
    );
}

/// `EvaluateParams` leaf (prepare.c).
pub fn eval_exec_param_into_list<'mcx>(
    param_li: ParamListInfoHandle,
    exprstate: &ExprState<'mcx>,
    param_index: i32,
    ptype: types_core::Oid,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (param_li, exprstate, param_index, ptype, estate);
    panic!(
        "execExpr-core: EvaluateParams leaf needs GetPerTupleExprContext (execUtils) and the \
         ParamListInfo slot-write (params unit); no seams exported yet"
    );
}

// ===========================================================================
// HashJoin convenience wrappers
// ===========================================================================

/// `ExecQual` for a hash-join node's `js.joinqual` / `js.ps.qual`.
pub fn exec_hashjoin_qual<'mcx>(
    node: &mut HashJoinState<'mcx>,
    joinqual: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let _ = (node, joinqual, estate);
    panic!(
        "execExpr-core: ExecQual over a HashJoin qual needs the owned HashJoinState field \
         accessors (js.ps.ps_ExprContext / js.joinqual / js.ps.qual) threaded from nodeHashjoin"
    );
}

/// `ExecInitQual` of one of a hash-join node's qual lists.
pub fn exec_init_hashjoin_qual<'mcx>(
    node: &mut HashJoinState<'mcx>,
    kind: HashJoinQualKind,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (node, kind, estate);
    panic!(
        "execExpr-core: ExecInitQual over a HashJoin qual list needs the owned HashJoin plan \
         accessors (node->join.plan.qual / joinqual / hashclauses) threaded from nodeHashjoin"
    );
}

/// `ExecProject(node->js.ps.ps_ProjInfo)` for a hash-join node.
pub fn exec_hashjoin_project<'mcx>(
    node: &mut HashJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<SlotId> {
    let _ = (node, estate);
    panic!(
        "execExpr-core: ExecProject of the HashJoin result projection needs the owned \
         ProjectionInfo / interpreter projection dispatch"
    );
}

/// `DatumGetUInt32(ExecEvalExprSwitchContext(hj_OuterHash, ...))`.
pub fn eval_outer_hash<'mcx>(
    node: &mut HashJoinState<'mcx>,
    isnull: &mut bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<u32> {
    let _ = (node, isnull, estate);
    panic!(
        "execExpr-core: evaluating the HashJoin outer hash needs the owned HashJoinState field \
         accessors (hj_OuterHash / js.ps.ps_ExprContext) threaded from nodeHashjoin"
    );
}
