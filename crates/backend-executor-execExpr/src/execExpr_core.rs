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
// The canonical unified value type (Datum-unification keystone) — what
// `ExprEvalStepData::ConstVal { value }` carries, and the value the interpreter
// eval seam (`exec_eval_*_switch_context`) returns.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_error::PgResult;
use types_nodes::execexpr::{
    ExprEvalOp, ExprEvalRowtypeCache, ExprEvalStep, ExprEvalStepData, ExprSetupInfo, ExprState,
    ProjectionInfo, ResultCell, ResultCellId, VarReturningType, EEO_FLAG_HAS_NEW, EEO_FLAG_HAS_OLD,
    EEO_FLAG_IS_QUAL, EEO_FLAG_NEW_IS_NULL, EEO_FLAG_OLD_IS_NULL, STATE_RESULT_CELL,
};
use types_nodes::execnodes::PlanStateData;
use types_nodes::nodehashjoin::HashJoinState;
use types_nodes::primnodes::{
    etag, BoolExprType, Const, Expr, NullTestType, ParamKind, VarReturningType as VrtKind,
};
use types_nodes::execnodes::EStateLink;
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

/// `#define BTORDER_PROC 1` (access/nbtree.h) — the btree comparison support
/// proc; the row-compare per-column comparison uses it.
const BTORDER_PROC: i16 = 1;

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
    match node.expr_tag() {
        etag::T_Var => {
            let variable = node.as_var().expect("Var");
            let attnum = variable.varattno;
            match variable.varno {
                INNER_VAR => {
                    info.last_attnums.last_inner = info.last_attnums.last_inner.max(attnum)
                }
                OUTER_VAR => {
                    info.last_attnums.last_outer = info.last_attnums.last_outer.max(attnum)
                }
                _ => match variable.varreturningtype {
                    VrtKind::VAR_RETURNING_DEFAULT => {
                        info.last_attnums.last_scan = info.last_attnums.last_scan.max(attnum)
                    }
                    VrtKind::VAR_RETURNING_OLD => {
                        info.last_attnums.last_old = info.last_attnums.last_old.max(attnum)
                    }
                    VrtKind::VAR_RETURNING_NEW => {
                        info.last_attnums.last_new = info.last_attnums.last_new.max(attnum)
                    }
                },
            }
        }
        // Pure-leaf nodes — no child expressions to descend.
        etag::T_Const
        | etag::T_Param
        | etag::T_CaseTestExpr
        | etag::T_CoerceToDomainValue
        | etag::T_SetToDefault
        | etag::T_CurrentOfExpr
        | etag::T_NextValueExpr
        | etag::T_SQLValueFunction
        | etag::T_Aggref
        | etag::T_GroupingFunc
        | etag::T_WindowFunc
        | etag::T_MergeSupportFunc => {}
        // Single-arg passthrough / coercion nodes.
        etag::T_RelabelType => descend_opt(node.expect_relabeltype().arg.as_deref(), info),
        etag::T_CollateExpr => descend_opt(node.expect_collateexpr().arg.as_deref(), info),
        etag::T_CoerceViaIO => descend_opt(node.expect_coerceviaio().arg.as_deref(), info),
        etag::T_ConvertRowtypeExpr => {
            descend_opt(node.expect_convertrowtypeexpr().arg.as_deref(), info)
        }
        etag::T_FieldSelect => descend_opt(node.expect_fieldselect().arg.as_deref(), info),
        etag::T_ReturningExpr => descend_opt(
            node.as_returningexpr()
                .expect("ReturningExpr")
                .retexpr
                .as_deref(),
            info,
        ),
        etag::T_NamedArgExpr => descend_opt(node.expect_namedargexpr().arg.as_deref(), info),
        etag::T_NullTest => descend_opt(node.expect_nulltest().arg.as_deref(), info),
        etag::T_BooleanTest => descend_opt(node.expect_booleantest().arg.as_deref(), info),
        etag::T_CoerceToDomain => descend_opt(node.expect_coercetodomain().arg.as_deref(), info),
        etag::T_ArrayCoerceExpr => descend_opt(node.expect_arraycoerceexpr().arg.as_deref(), info),
        // Operator / function nodes — descend their argument lists.
        etag::T_FuncExpr => descend_list(&node.expect_funcexpr().args, info),
        etag::T_OpExpr | etag::T_DistinctExpr | etag::T_NullIfExpr => {
            let e = node
                .as_opexpr()
                .or_else(|| node.as_distinctexpr())
                .or_else(|| node.as_nullifexpr())
                .expect("OpExpr/DistinctExpr/NullIfExpr");
            descend_list(&e.args, info)
        }
        etag::T_BoolExpr => descend_list(&node.expect_boolexpr().args, info),
        etag::T_CoalesceExpr => descend_list(&node.expect_coalesceexpr().args, info),
        etag::T_MinMaxExpr => descend_list(&node.expect_minmaxexpr().args, info),
        etag::T_ArrayExpr => descend_list(&node.expect_arrayexpr().elements, info),
        // CASE: arg + each WHEN's (expr,result) + ELSE.
        etag::T_CaseExpr => {
            let e = node.expect_caseexpr();
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
        (ExprEvalOp::EEOP_OLD_FETCHSOME, last.last_old),
        (ExprEvalOp::EEOP_NEW_FETCHSOME, last.last_new),
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
    node: &Expr<'mcx>,
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
///
/// `pub(crate)` so the hash/equality builders in `execExpr_domain_agg`
/// (`ExecBuildHash32Expr`) can run the same FETCHSOME-deform prescan the C
/// runs via `ExecCreateExprSetupSteps(state, (Node *) hash_exprs)` — exactly the
/// sibling-spine sharing already used for [`exec_init_expr_rec`].
pub(crate) fn exec_create_expr_setup_steps_list<'mcx>(
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
    node: &Expr<'mcx>,
    state: &mut ExprState<'mcx>,
    resv: ResultCellId,
) -> PgResult<()> {
    // C: check_stack_depth(); — guarded by the host stack here.
    match node.expr_tag() {
        // ----- T_Var -----
        etag::T_Var => {
            let variable = node.as_var().expect("Var");
            let mut scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_SCAN_VAR, // set below
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::Var {
                    attnum: 0,
                    vartype: variable.vartype,
                    varreturningtype: variable.varreturningtype,
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
                if let ExprEvalStepData::Var { varreturningtype, .. } = &mut scratch.d {
                    *varreturningtype = variable.varreturningtype;
                }
                scratch.opcode = match variable.varno {
                    INNER_VAR => ExprEvalOp::EEOP_INNER_SYSVAR,
                    OUTER_VAR => ExprEvalOp::EEOP_OUTER_SYSVAR,
                    _ => sysvar_opcode_for(state, variable.varreturningtype),
                };
            } else {
                // regular user column
                set_var_payload(
                    &mut scratch,
                    variable.varattno as i32 - 1,
                    variable.vartype,
                );
                if let ExprEvalStepData::Var { varreturningtype, .. } = &mut scratch.d {
                    *varreturningtype = variable.varreturningtype;
                }
                scratch.opcode = match variable.varno {
                    INNER_VAR => ExprEvalOp::EEOP_INNER_VAR,
                    OUTER_VAR => ExprEvalOp::EEOP_OUTER_VAR,
                    _ => var_opcode_for(state, variable.varreturningtype),
                };
            }
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_Const -----
        etag::T_Const => {
            let con = node.expect_const();
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_CONST,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::ConstVal {
                    value: con.constvalue.clone(),
                    isnull: con.constisnull,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_Param -----
        etag::T_Param => {
            let param = node.expect_param();
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
        etag::T_Aggref => {
            let aggref = node.expect_aggref();
            // C (execExpr.c ExecInitExprRec T_Aggref):
            //   AggState *aggstate = castNode(AggState, state->parent);
            //   aggstate->aggs = lappend(aggstate->aggs, astate);
            //   ... scratch.d.aggref.aggno = aggref->aggno;
            // The parent-AggState->aggs accumulation cannot mutate the parent
            // directly here: the parent surface is the head-only PlanStateData
            // (and during ExecInitAgg the in-flight AggState is not yet a
            // PlanStateNode). So the discovered Aggref is collected onto the
            // ExprState's `found_aggs` channel; the nodeAgg owner drains it into
            // aggstate->aggs after compilation (planner-set aggno makes the
            // collection-order divergence behaviorally inert).
            if state.found_aggs.is_none() {
                state.found_aggs = Some(mcx::vec_with_capacity_in(mcx, 1)?);
            }
            state
                .found_aggs
                .as_mut()
                .expect("found_aggs just initialized")
                .push(aggref.clone_in(mcx)?);

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
        etag::T_GroupingFunc => {
            let grp = node.expect_groupingfunc();
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
        etag::T_MergeSupportFunc => {
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
        etag::T_FuncExpr => {
            let func = node.expect_funcexpr();
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
        // OpExpr/DistinctExpr/NullIfExpr: C passes op->opfuncid (the PG_PROC oid
        // of the implementing function the planner filled in) and
        // op->inputcollid to ExecInitFunc — NOT op->opno (a PG_OPERATOR oid),
        // which would resolve the wrong function in fmgr_info.
        etag::T_OpExpr => {
            let op = node.as_opexpr().expect("OpExpr");
            let mut scratch = scratch_for(resv);
            crate::execExpr_func_subscript::exec_init_func(mcx, &mut scratch, node, &op.args, op.opfuncid, op.inputcollid, state)?;
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        etag::T_DistinctExpr => {
            let op = node.expect_distinctexpr();
            let mut scratch = scratch_for(resv);
            crate::execExpr_func_subscript::exec_init_func(mcx, &mut scratch, node, &op.args, op.opfuncid, op.inputcollid, state)?;
            scratch.opcode = ExprEvalOp::EEOP_DISTINCT;
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        etag::T_NullIfExpr => {
            let op = node.expect_nullifexpr();
            let mut scratch = scratch_for(resv);
            crate::execExpr_func_subscript::exec_init_func(mcx, &mut scratch, node, &op.args, op.opfuncid, op.inputcollid, state)?;
            scratch.opcode = ExprEvalOp::EEOP_NULLIF;
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_NamedArgExpr (transparent wrapper around its arg) -----
        etag::T_NamedArgExpr => {
            let nae = node.expect_namedargexpr();
            let arg = nae
                .arg
                .as_deref()
                .expect("NamedArgExpr.arg must be present");
            exec_init_expr_rec(mcx, arg, state, resv)
        }

        // ----- T_RelabelType (no-op coercion) -----
        etag::T_RelabelType => {
            let relabel = node.expect_relabeltype();
            let arg = relabel.arg.as_deref().expect("RelabelType.arg present");
            exec_init_expr_rec(mcx, arg, state, resv)
        }

        // ----- T_CollateExpr (planner removes it; transparent if seen) -----
        etag::T_CollateExpr => {
            let collate = node.expect_collateexpr();
            let arg = collate.arg.as_deref().expect("CollateExpr.arg present");
            exec_init_expr_rec(mcx, arg, state, resv)
        }

        // ----- T_BoolExpr -----
        etag::T_BoolExpr => {
            let boolexpr = node.expect_boolexpr();
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
        etag::T_CaseExpr => {
            let caseexpr = node.expect_caseexpr();
            // If there's a test expression, C evaluates it into a caseval/
            // casenull workspace cell where CaseTestExpr placeholders find it,
            // and (only if get_typlen(exprType(arg)) == -1, i.e. a varlena that
            // could be an expanded datum) emits an EEOP_MAKE_READONLY over it.
            // (execExpr.c:1782-1808).
            let case_cell = if let Some(arg) = caseexpr.arg.as_deref() {
                // Evaluate testexpr into caseval/casenull workspace (one cell
                // carries both Datum and isnull in the owned model — the
                // owned-model replacement for palloc(sizeof(Datum)) +
                // palloc(sizeof(bool))).
                let caseval = new_result_cell(mcx, state)?;
                exec_init_expr_rec(mcx, arg, state, caseval)?;

                // Since value might be read multiple times, force to R/O — but
                // only if it could be an expanded datum (get_typlen == -1).
                let argtype = backend_nodes_nodeFuncs_seams::expr_type_info::call(arg)?.typid;
                if backend_utils_cache_lsyscache_seams::get_typlen::call(argtype)? == -1 {
                    // change caseval in-place (resvalue == d.make_readonly.value).
                    let scratch = ExprEvalStep {
                        opcode: ExprEvalOp::EEOP_MAKE_READONLY,
                        resvalue: caseval,
                        resnull: caseval,
                        d: ExprEvalStepData::MakeReadOnly { value: caseval },
                    };
                    expr_eval_push_step(mcx, state, scratch)?;
                }
                Some(caseval)
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
        etag::T_CaseTestExpr => {
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
        etag::T_CoalesceExpr => {
            let coalesce = node.expect_coalesceexpr();
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
        etag::T_SQLValueFunction => {
            // C: scratch.d.sqlvaluefunction.svf = (SQLValueFunction *) node;
            // The node is `Copy`, so the owned step carries it inline.
            let svf = match node {
                Expr::SQLValueFunction(s) => *s,
                _ => unreachable!("T_SQLValueFunction tag without SQLValueFunction payload"),
            };
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_SQLVALUEFUNCTION,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::SqlValueFunction { svf },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // ----- T_NullTest -----
        etag::T_NullTest => {
            let ntest = node.expect_nulltest();
            // C: pick the scalar vs row opcode by nulltesttype × argisrow. The
            // row path (EEOP_NULLTEST_ROWIS[NOT]NULL) drives ExecEvalRowNull[NotNull],
            // which decodes the composite Datum and per-field heap_attisnull tests
            // it via the typcache rowtype lookup (now ported).
            let opcode = match (ntest.nulltesttype, ntest.argisrow) {
                (NullTestType::IS_NULL, false) => ExprEvalOp::EEOP_NULLTEST_ISNULL,
                (NullTestType::IS_NULL, true) => ExprEvalOp::EEOP_NULLTEST_ROWISNULL,
                (NullTestType::IS_NOT_NULL, false) => ExprEvalOp::EEOP_NULLTEST_ISNOTNULL,
                (NullTestType::IS_NOT_NULL, true) => ExprEvalOp::EEOP_NULLTEST_ROWISNOTNULL,
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
        etag::T_BooleanTest => {
            let btest = node.expect_booleantest();
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
        etag::T_CurrentOfExpr => {
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
        etag::T_NextValueExpr => {
            let nve = node.expect_nextvalueexpr();
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
        etag::T_FieldSelect => {
            let fselect = node.expect_fieldselect();
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
        // ----- T_ScalarArrayOpExpr -----
        etag::T_ScalarArrayOpExpr => {
            let opexpr = node.expect_scalararrayopexpr();
            let mut scratch = scratch_for(resv);
            crate::execExpr_func_subscript::exec_init_scalar_array_op(
                mcx, &mut scratch, opexpr, state, resv,
            )?;
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        // ----- T_MinMaxExpr -----
        etag::T_MinMaxExpr => {
            let minmaxexpr = node.expect_minmaxexpr();
            let nelems = minmaxexpr.args.len();

            // Look up the btree comparison function for the datatype.
            //   typentry = lookup_type_cache(minmaxexpr->minmaxtype,
            //                                TYPECACHE_CMP_PROC);
            //   if (!OidIsValid(typentry->cmp_proc)) ereport(ERROR, ...);
            let cmp_proc = backend_utils_cache_typcache_seams::lookup_element_cmp_proc::call(
                minmaxexpr.minmaxtype,
            )?;
            if cmp_proc == types_core::InvalidOid {
                return Err(types_error::PgError::error(format!(
                    "could not identify a comparison function for type {}",
                    minmaxexpr.minmaxtype
                ))
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_FUNCTION));
            }

            // Perform function lookup.
            //   finfo = palloc0(sizeof(FmgrInfo));
            //   fcinfo = palloc0(SizeForFunctionCallInfo(2));
            //   fmgr_info(typentry->cmp_proc, finfo);
            //   fmgr_info_set_expr((Node *) node, finfo);
            //   InitFunctionCallInfoData(*fcinfo, finfo, 2,
            //                            minmaxexpr->inputcollid, NULL, NULL);
            let flinfo = backend_utils_fmgr_fmgr_seams::fmgr_info::call(mcx, cmp_proc)?;
            let fcinfo_data = mcx::alloc_in(
                mcx,
                types_nodes::fmgr::FunctionCallInfoBaseData {
                    flinfo: Some(flinfo.clone()),
                    context: None,
                    resultinfo: None,
                    fncollation: minmaxexpr.inputcollid,
                    isnull: false,
                    nargs: 2,
                    args: Vec::new(),
                    ..Default::default()
                },
            )?;

            // Allocate space to store arguments (the C `scratch.d.minmax.values`
            // / `nulls` Datum/bool workspace), pre-sized to `nelems` so the
            // interpreter can index `values[off]`/`nulls[off]`.
            let mut values: PgVec<'mcx, DatumV<'mcx>> = mcx::vec_with_capacity_in(mcx, nelems)?;
            let mut nulls: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, nelems)?;
            for _ in 0..nelems {
                values.push(DatumV::null());
                nulls.push(false);
            }

            // Evaluate expressions into minmax->values/nulls. The C writes each
            // arg directly into `&scratch.d.minmax.values[off]`; the owned model
            // gives each argument its own result cell and records it in
            // `arg_cells`, which the interpreter gathers into `values`/`nulls`
            // immediately before the comparison loop.
            let mut arg_cells: PgVec<'mcx, types_nodes::execexpr::ResultCellId> =
                mcx::vec_with_capacity_in(mcx, nelems)?;
            for e in &minmaxexpr.args {
                let cell = crate::execExpr_core::new_result_cell(mcx, state)?;
                exec_init_expr_rec(mcx, e, state, cell)?;
                arg_cells.push(cell);
            }

            // And push the final comparison.
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_MINMAX,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::MinMax {
                    values: Some(values),
                    nulls: Some(nulls),
                    arg_cells: Some(arg_cells),
                    nelems: nelems as i32,
                    op: match minmaxexpr.op {
                        types_nodes::primnodes::MinMaxOp::IS_GREATEST => {
                            types_nodes::execexpr::MinMaxOp::IS_GREATEST
                        }
                        types_nodes::primnodes::MinMaxOp::IS_LEAST => {
                            types_nodes::execexpr::MinMaxOp::IS_LEAST
                        }
                    },
                    finfo: Some(mcx::alloc_in(mcx, flinfo)?),
                    fcinfo_data: Some(fcinfo_data),
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        // ----- T_ArrayExpr -----
        etag::T_ArrayExpr => {
            let arrayexpr = node.expect_arrayexpr();
            // Evaluate by computing each element, then forming the array.
            // Elements are computed into per-element result cells associated
            // with the ARRAYEXPR step (the owned-model stand-in for C's
            // scratch.d.arrayexpr.elemvalues[]/elemnulls[] scratch arrays).
            let nelems = arrayexpr.elements.len() as i32;

            // do one-time catalog lookup for type info
            let tlbva = backend_utils_cache_lsyscache_seams::get_typlenbyvalalign::call(
                arrayexpr.element_typeid,
            )?;

            // prepare to evaluate all arguments: each element gets its own
            // result cell; the cell id is recorded in elem_cells[elemoff]. The
            // interpreter gathers these cells into elemvalues/elemnulls just
            // before fabricating the array (mirroring C's per-element write into
            // &scratch.d.arrayexpr.elemvalues[elemoff]).
            let mut elem_cells = mcx::vec_with_capacity_in(mcx, nelems as usize)?;
            for e in &arrayexpr.elements {
                let cell = new_result_cell(mcx, state)?;
                exec_init_expr_rec(mcx, e, state, cell)?;
                elem_cells.push(cell);
            }

            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_ARRAYEXPR,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::ArrayExpr {
                    elemvalues: None,
                    elemnulls: None,
                    elem_cells: Some(elem_cells),
                    nelems,
                    elemtype: arrayexpr.element_typeid,
                    elemlength: tlbva.typlen,
                    elembyval: tlbva.typbyval,
                    elemalign: tlbva.typalign as u8,
                    multidims: arrayexpr.multidims,
                },
            };

            // and then collect all into an array
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        // ----- T_RowExpr -----
        etag::T_RowExpr => {
            const RECORDOID: types_core::Oid = 2249;
            let rowexpr = node.expect_rowexpr();
            let nargs = rowexpr.args.len();

            // Build tupdesc to describe result tuples. (`TupleDesc` is
            // `Option<PgBox<TupleDescData>>`; both arms produce a present box.)
            let tupdesc: PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>> =
                if rowexpr.row_typeid == RECORDOID {
                    // generic record, use types of given expressions
                    let mut td = backend_executor_execTuples_seams::exec_type_from_expr_list::call(
                        mcx,
                        &rowexpr.args,
                    )?
                    .expect("ExecTypeFromExprList returned no tupdesc");
                    // ... but adopt RowExpr's column aliases (ExecTypeSetColNames).
                    // Only OK to rename on a not-yet-blessed RECORD type.
                    if !rowexpr.colnames.is_empty() {
                        for (colno, cname) in rowexpr.colnames.iter().enumerate() {
                            if colno >= td.natts as usize {
                                break;
                            }
                            let attr = td.attr_mut(colno);
                            // Do nothing for empty aliases or dropped columns.
                            if cname.is_empty() || attr.attisdropped {
                                continue;
                            }
                            attr.attname.namestrcpy(cname);
                        }
                    }
                    // Bless the tupdesc so it can be looked up later.
                    backend_executor_execTuples_seams::bless_tuple_desc::call(mcx, Some(td))?
                        .expect("BlessTupleDesc returned no tupdesc")
                } else {
                    // it's been cast to a named type, use that.
                    backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc_copy::call(
                        mcx,
                        rowexpr.row_typeid,
                        -1,
                    )?
                };

            // In the named-type case the tupdesc could have more columns than
            // the args list (columns added since the ROW() was parsed); those
            // extra columns go to nulls. nelems = Max(nargs, tupdesc->natts).
            debug_assert!(nargs <= tupdesc.natts as usize);
            let nelems = core::cmp::max(nargs, tupdesc.natts as usize);

            // Per-field result cells (the owned replacement for C's
            // &scratch.d.row.elemvalues[i] / elemnulls[i] write targets). Extra
            // columns beyond the args list (and dropped columns) carry the
            // STATE_RESULT_CELL sentinel and read as NULL (the interpreter forces
            // elemnulls[i] = true for them).
            let mut elem_cells: PgVec<ResultCellId> = mcx::vec_with_capacity_in(mcx, nelems)?;

            // Set up evaluation, skipping any deleted columns.
            for i in 0..nelems {
                let att = tupdesc.attr(i);
                if i < nargs && !att.attisdropped {
                    // Guard against ALTER COLUMN TYPE since the RowExpr was made.
                    let e = &rowexpr.args[i];
                    let etype = backend_nodes_nodeFuncs_seams::expr_type_info::call(e)?.typid;
                    if etype != att.atttypid {
                        return Err(types_error::PgError::error("ROW() column has wrong type")
                            .with_detail(format!(
                                "ROW() column has type {} instead of type {}",
                                etype, att.atttypid
                            )));
                    }
                    // Evaluate column expr into its workspace cell.
                    let cell = new_result_cell(mcx, state)?;
                    exec_init_expr_rec(mcx, e, state, cell)?;
                    elem_cells.push(cell);
                } else {
                    // Dropped column, or an extra named-type column past the args
                    // list: insert a NULL (C makeNullConst(INT4OID) then evaluates
                    // it; the owned model records the sentinel and reads NULL).
                    elem_cells.push(STATE_RESULT_CELL);
                }
            }

            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_ROW,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::Row {
                    tupdesc: Some(tupdesc),
                    elemvalues: None,
                    elemnulls: None,
                    elem_cells: Some(elem_cells),
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        // ----- T_RowCompareExpr ----- (execExpr.c:2059-2177)
        etag::T_RowCompareExpr => {
            let rcexpr = node.expect_rowcompareexpr();
            let nopers = rcexpr.opnos.len();

            // Iterate over each field, prepare comparisons. To handle NULL
            // results, prepare jumps to after the expression. If a comparison
            // yields a != 0 result, jump to the final step.
            debug_assert_eq!(rcexpr.largs.len(), nopers);
            debug_assert_eq!(rcexpr.rargs.len(), nopers);
            debug_assert_eq!(rcexpr.opfamilies.len(), nopers);
            debug_assert_eq!(rcexpr.inputcollids.len(), nopers);

            let mut adjust_jumps: PgVec<'mcx, usize> =
                mcx::vec_with_capacity_in(mcx, nopers)?;

            // forfive(l_left_expr, largs, l_right_expr, rargs, l_opno, opnos,
            //         l_opfamily, opfamilies, l_inputcollid, inputcollids)
            for i in 0..nopers {
                let left_expr = &rcexpr.largs[i];
                let right_expr = &rcexpr.rargs[i];
                let opno = rcexpr.opnos[i];
                let opfamily = rcexpr.opfamilies[i];
                let inputcollid = rcexpr.inputcollids[i];

                // get_op_opfamily_properties(opno, opfamily, false,
                //                            &strategy, &lefttype, &righttype);
                let (_strategy, lefttype, righttype) =
                    backend_utils_cache_lsyscache_seams::get_op_opfamily_properties::call(
                        opno, opfamily, false, false,
                    )?
                    .expect("get_op_opfamily_properties(missing_ok=false) returns Some");
                // proc = get_opfamily_proc(opfamily, lefttype, righttype, BTORDER_PROC);
                let proc = backend_utils_cache_lsyscache_seams::get_opfamily_proc::call(
                    opfamily,
                    lefttype,
                    righttype,
                    BTORDER_PROC,
                )?;
                if proc == types_core::InvalidOid {
                    return Err(types_error::PgError::error(format!(
                        "missing support function {}({},{}) in opfamily {}",
                        BTORDER_PROC, lefttype, righttype, opfamily
                    )));
                }

                // Set up the primary fmgr lookup information.
                //   finfo = palloc0(sizeof(FmgrInfo));
                //   fcinfo = palloc0(SizeForFunctionCallInfo(2));
                //   fmgr_info(proc, finfo); fmgr_info_set_expr((Node *) node, finfo);
                //   InitFunctionCallInfoData(*fcinfo, finfo, 2, inputcollid, NULL, NULL);
                let finfo = backend_utils_fmgr_fmgr_seams::fmgr_info::call(mcx, proc)?;
                let fcinfo_data = mcx::alloc_in(
                    mcx,
                    types_nodes::fmgr::FunctionCallInfoBaseData {
                        flinfo: Some(finfo.clone()),
                        context: None,
                        resultinfo: None,
                        fncollation: inputcollid,
                        isnull: false,
                        nargs: 2,
                        args: Vec::new(),
                        ..Default::default()
                    },
                )?;

                // Evaluate left and right args directly into fcinfo. The C
                // recursion writes through &fcinfo->args[0/1]; the owned model
                // gives each argument its own result cell and records it in
                // arg_cells, which the interpreter gathers into the call frame
                // immediately before the comparison.
                let mut arg_cells: PgVec<'mcx, types_nodes::execexpr::ResultCellId> =
                    mcx::vec_with_capacity_in(mcx, 2)?;
                let lcell = new_result_cell(mcx, state)?;
                exec_init_expr_rec(mcx, left_expr, state, lcell)?;
                arg_cells.push(lcell);
                let rcell = new_result_cell(mcx, state)?;
                exec_init_expr_rec(mcx, right_expr, state, rcell)?;
                arg_cells.push(rcell);

                let scratch = ExprEvalStep {
                    opcode: ExprEvalOp::EEOP_ROWCOMPARE_STEP,
                    resvalue: resv,
                    resnull: resv,
                    d: ExprEvalStepData::RowCompareStep {
                        finfo: Some(mcx::alloc_in(mcx, finfo)?),
                        fcinfo_data: Some(fcinfo_data),
                        arg_cells: Some(arg_cells),
                        // fn_addr stays None — the interpreter re-resolves by
                        // finfo.fn_oid (the by-OID fmgr dispatch).
                        fn_addr: None,
                        // jump targets filled below.
                        jumpnull: -1,
                        jumpdone: -1,
                    },
                };
                expr_eval_push_step(mcx, state, scratch)?;
                adjust_jumps.push((state.steps_len - 1) as usize);
            }

            // We could have a zero-column rowtype, in which case the rows
            // necessarily compare equal.
            if nopers == 0 {
                let scratch = ExprEvalStep {
                    opcode: ExprEvalOp::EEOP_CONST,
                    resvalue: resv,
                    resnull: resv,
                    d: ExprEvalStepData::ConstVal {
                        value: DatumV::from_i32(0),
                        isnull: false,
                    },
                };
                expr_eval_push_step(mcx, state, scratch)?;
            }

            // Finally, examine the last comparison result.
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_ROWCOMPARE_FINAL,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::RowCompareFinal {
                    cmptype: rcexpr.cmptype,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;

            // adjust jump targets.
            let steps_len = state.steps_len;
            let steps = state.steps.as_mut().unwrap();
            for &j in adjust_jumps.iter() {
                if let ExprEvalStepData::RowCompareStep {
                    jumpdone, jumpnull, ..
                } = &mut steps[j].d
                {
                    debug_assert_eq!(*jumpdone, -1);
                    debug_assert_eq!(*jumpnull, -1);
                    // jump to comparison evaluation (the ROWCOMPARE_FINAL step).
                    *jumpdone = steps_len - 1;
                    // jump to the following expression.
                    *jumpnull = steps_len;
                }
            }
            Ok(())
        }
        etag::T_SubscriptingRef => {
            let sbsref = node.expect_subscriptingref();
            let mut scratch = scratch_for(resv);
            crate::execExpr_func_subscript::exec_init_subscripting_ref(
                mcx, &mut scratch, sbsref, state, resv,
            )
        }
        // ----- T_CoerceViaIO -----
        etag::T_CoerceViaIO => {
            let iocoerce = node.expect_coerceviaio();
            // C: ExecInitExprRec(iocoerce->arg, state, resv, resnull);
            let arg = iocoerce.arg.as_deref().expect("CoerceViaIO.arg present");
            exec_init_expr_rec(mcx, arg, state, resv)?;

            // C: if (state->escontext == NULL) opcode = EEOP_IOCOERCE;
            //    else opcode = EEOP_IOCOERCE_SAFE;
            let opcode = if state.escontext.is_none() {
                ExprEvalOp::EEOP_IOCOERCE
            } else {
                ExprEvalOp::EEOP_IOCOERCE_SAFE
            };

            // C: lookup the source type's output function (1-arg call frame).
            //    getTypeOutputInfo(exprType(iocoerce->arg), &iofunc, &typisvarlena);
            //    fmgr_info(iofunc, finfo_out);
            //    InitFunctionCallInfoData(*fcinfo_out, finfo_out, 1, InvalidOid, NULL, NULL);
            let src_type = backend_nodes_nodeFuncs_seams::expr_type_info::call(arg)?.typid;
            let (out_func, _typisvarlena) =
                backend_utils_cache_lsyscache_seams::get_type_output_info::call(src_type)?;
            let finfo_out = backend_utils_fmgr_fmgr_seams::fmgr_info::call(mcx, out_func)?;
            let fcinfo_data_out = mcx::alloc_in(
                mcx,
                types_nodes::fmgr::FunctionCallInfoBaseData {
                    flinfo: Some(finfo_out.clone()),
                    context: None,
                    resultinfo: None,
                    fncollation: types_core::InvalidOid,
                    isnull: false,
                    nargs: 1,
                    args: vec![types_datum::NullableDatum {
                        value: types_datum::Datum::from_usize(0),
                        isnull: false,
                    }],
                    // Value-per-call SRF channel (#349): unused (I/O coerce frame).
                    ..Default::default()
                },
            )?;

            // C: lookup the result type's input function (3-arg call frame).
            //    getTypeInputInfo(iocoerce->resulttype, &iofunc, &typioparam);
            //    fmgr_info(iofunc, finfo_in);
            //    InitFunctionCallInfoData(*fcinfo_in, finfo_in, 3, InvalidOid, NULL, NULL);
            let (in_func, typioparam) =
                backend_utils_cache_lsyscache_seams::get_type_input_info::call(
                    iocoerce.resulttype,
                )?;
            let finfo_in = backend_utils_fmgr_fmgr_seams::fmgr_info::call(mcx, in_func)?;

            // C: We can preload the second and third arguments for the input
            //    function, since they're constants.
            //    fcinfo_in->args[1].value = ObjectIdGetDatum(typioparam); .isnull = false;
            //    fcinfo_in->args[2].value = Int32GetDatum(-1); .isnull = false;
            //    fcinfo_in->context = (Node *) state->escontext;
            //
            // The soft-error context (`state->escontext`) is the soft-error sink
            // owned by the not-yet-ported elog/ErrorSaveContext layer; the
            // owned model parks it as an opaque address on ExprState and the
            // IoCoerce frame's `context` stays None until that lands (only the
            // EEOP_IOCOERCE_SAFE path reads it; the common EEOP_IOCOERCE path
            // never does).
            let fcinfo_data_in = mcx::alloc_in(
                mcx,
                types_nodes::fmgr::FunctionCallInfoBaseData {
                    flinfo: Some(finfo_in.clone()),
                    context: None,
                    resultinfo: None,
                    fncollation: types_core::InvalidOid,
                    isnull: false,
                    nargs: 3,
                    args: vec![
                        // args[0] — the (cstring) value, filled at eval.
                        types_datum::NullableDatum {
                            value: types_datum::Datum::from_usize(0),
                            isnull: false,
                        },
                        // args[1] — typioparam (constant).
                        types_datum::NullableDatum {
                            value: types_datum::Datum::from_oid(typioparam),
                            isnull: false,
                        },
                        // args[2] — typmod = -1 (constant).
                        types_datum::NullableDatum {
                            value: types_datum::Datum::from_i32(-1),
                            isnull: false,
                        },
                    ],
                    // Value-per-call SRF channel (#349): unused (I/O coerce frame).
                    ..Default::default()
                },
            )?;

            let scratch = ExprEvalStep {
                opcode,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::IoCoerce {
                    finfo_out: Some(mcx::alloc_in(mcx, finfo_out)?),
                    fcinfo_data_out: Some(fcinfo_data_out),
                    finfo_in: Some(mcx::alloc_in(mcx, finfo_in)?),
                    fcinfo_data_in: Some(fcinfo_data_in),
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        // ----- T_ArrayCoerceExpr -----
        etag::T_ArrayCoerceExpr => {
            let acoerce = node.expect_arraycoerceexpr();
            // C: ExecInitExprRec(acoerce->arg, state, resv, resnull);
            let arg = acoerce.arg.as_deref().expect("ArrayCoerceExpr.arg present");
            exec_init_expr_rec(mcx, arg, state, resv)?;

            // C: resultelemtype = get_element_type(acoerce->resulttype);
            //    if (!OidIsValid(resultelemtype))
            //        ereport(ERROR, errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            //                errmsg("target type is not an array"));
            let resultelemtype =
                backend_utils_cache_lsyscache_seams::get_element_type::call(acoerce.resulttype)?
                    .unwrap_or(types_core::InvalidOid);
            if resultelemtype == types_core::InvalidOid {
                return Err(types_error::PgError::error("target type is not an array")
                    .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE));
            }

            // C: Construct a sub-expression for the per-element coercion; don't
            //    ready it until after the triviality check. It has no Var refs
            //    but does have a CaseTestExpr for the source element value.
            //    elemstate = makeNode(ExprState);
            //    elemstate->expr = acoerce->elemexpr;
            //    elemstate->parent = state->parent;
            //    elemstate->ext_params = state->ext_params;
            //    elemstate->innermost_caseval = palloc(sizeof(Datum));
            //    elemstate->innermost_casenull = palloc(sizeof(bool));
            //    ExecInitExprRec(acoerce->elemexpr, elemstate,
            //                    &elemstate->resvalue, &elemstate->resnull);
            let elemexpr = acoerce
                .elemexpr
                .as_deref()
                .expect("ArrayCoerceExpr.elemexpr present");
            let mut elemstate = make_expr_state();
            // C: elemstate->parent = state->parent;  (a raw PlanState* copy).
            // The parent is owned (PgBox, not Clone) in this model and is only
            // consulted to set up SubPlan / whole-row Var references — which a
            // per-element array coercion sub-expression cannot contain (it has
            // no Var refs by construction, only a CaseTestExpr for the source
            // element value). So leaving it unset is faithful for every shape
            // this sub-expression can take.
            elemstate.parent = None;
            // Inherit the EState back-link so any nested compile sees the same
            // executor state (the per-element coercion cannot contain a SubPlan,
            // but keep the link consistent with the C `elemstate` sharing
            // `parent->state`).
            elemstate.es_link = state.es_link;
            elemstate.ext_params = state.ext_params;
            ensure_result_arena(mcx, &mut elemstate)?;
            // C palloc's a dedicated innermost_caseval/casenull cell for the
            // element value the CaseTestExpr reads; allocate one in the
            // sub-state's arena.
            elemstate.innermost_caseval = Some(new_result_cell(mcx, &mut elemstate)?);
            // The element coercion result is written into the sub-state's own
            // resvalue/resnull (STATE_RESULT_CELL), the C `&elemstate->resvalue`.
            exec_init_expr_rec(mcx, elemexpr, &mut elemstate, STATE_RESULT_CELL)?;

            // C: if (elemstate->steps_len == 1 &&
            //        elemstate->steps[0].opcode == EEOP_CASE_TESTVAL)
            //        elemstate = NULL;   /* trivial, no per-element work */
            //    else { append EEOP_DONE_RETURN; ExecReadyExpr(elemstate); }
            let trivial = elemstate.steps_len == 1
                && matches!(
                    elemstate
                        .steps
                        .as_ref()
                        .and_then(|s| s.first())
                        .map(|s| s.opcode),
                    Some(ExprEvalOp::EEOP_CASE_TESTVAL)
                );
            let elemexprstate = if trivial {
                None
            } else {
                expr_eval_push_step(mcx, &mut elemstate, done_return_step(STATE_RESULT_CELL))?;
                exec_ready_expr(&mut elemstate)?;
                Some(mcx::alloc_in(mcx, elemstate)?)
            };

            // C: scratch.opcode = EEOP_ARRAYCOERCE;
            //    scratch.d.arraycoerce.elemexprstate = elemstate;
            //    scratch.d.arraycoerce.resultelemtype = resultelemtype;
            //    if (elemstate) scratch.d.arraycoerce.amstate = palloc0(sizeof(ArrayMapState));
            //    else           scratch.d.arraycoerce.amstate = NULL;
            //
            // `ArrayMapState` is array_map's runtime workspace, opaque to this
            // layer (the arrayfuncs owner's struct); the interpreter allocates it
            // on first use, so we leave it as the parked-address sentinel 0,
            // exactly as C leaves it NULL when there is no per-element work and
            // zeroed (lazily filled) when there is.
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_ARRAYCOERCE,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::ArrayCoerce {
                    elemexprstate,
                    resultelemtype,
                    amstate: 0,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        // ----- T_ConvertRowtypeExpr -----
        etag::T_ConvertRowtypeExpr => {
            let convert = node.expect_convertrowtypeexpr();
            // C: rowcachep = palloc(2 * sizeof(ExprEvalRowtypeCache));
            //    rowcachep[0].cacheptr = NULL; rowcachep[1].cacheptr = NULL;
            // The in/out rowtype caches are fresh (cacheptr == NULL); the
            // interpreter fills them at runtime from the typcache. They are
            // out-of-line in C for space; here each is its own boxed
            // ExprEvalRowtypeCache on the step payload.
            let incache = mcx::alloc_in(mcx, ExprEvalRowtypeCache::default())?;
            let outcache = mcx::alloc_in(mcx, ExprEvalRowtypeCache::default())?;

            // C: ExecInitExprRec(convert->arg, state, resv, resnull);
            let arg = convert.arg.as_deref().expect("ConvertRowtypeExpr.arg present");
            exec_init_expr_rec(mcx, arg, state, resv)?;

            // C: scratch.opcode = EEOP_CONVERT_ROWTYPE;
            //    scratch.d.convert_rowtype.inputtype = exprType((Node *) convert->arg);
            //    scratch.d.convert_rowtype.outputtype = convert->resulttype;
            //    scratch.d.convert_rowtype.incache = &rowcachep[0];
            //    scratch.d.convert_rowtype.outcache = &rowcachep[1];
            //    scratch.d.convert_rowtype.map = NULL;
            //
            // `exprType((Node *) convert->arg)` is a pure node-type inspection
            // owned by nodeFuncs (already threaded here). `map` (the
            // TupleConversionMap) is built lazily by the interpreter, so it stays
            // NULL (the parked-address sentinel 0) exactly as C palloc-NULLs it.
            let inputtype = backend_nodes_nodeFuncs_seams::expr_type_info::call(arg)?.typid;
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_CONVERT_ROWTYPE,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::ConvertRowtype {
                    inputtype,
                    outputtype: convert.resulttype,
                    incache: Some(incache),
                    outcache: Some(outcache),
                    map: 0,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        // ----- T_FieldStore -----
        etag::T_FieldStore => {
            let fstore = node.expect_fieldstore();

            // /* find out the number of columns in the composite type */
            // tupDesc = lookup_rowtype_tupdesc(fstore->resulttype, -1);
            // ncolumns = tupDesc->natts;
            // ReleaseTupleDesc(tupDesc);
            let tup_desc = backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(
                mcx,
                fstore.resulttype,
                -1,
            )?;
            let ncolumns = tup_desc.natts;

            // /* create workspace for column values */
            // values = (Datum *) palloc(sizeof(Datum) * ncolumns);
            // nulls  = (bool  *) palloc(sizeof(bool)  * ncolumns);
            //
            // The owned model replaces the two flat workspace arrays with a
            // per-column arena ResultCellId. Allocate one cell per column up
            // front; DEFORM writes each, the newval sub-exprs target their
            // field's cell, and FORM gathers them all into heap_form_tuple.
            let mut col_cells: PgVec<ResultCellId> =
                mcx::vec_with_capacity_in(mcx, ncolumns as usize)?;
            for _ in 0..ncolumns {
                col_cells.push(new_result_cell(mcx, state)?);
            }

            // /* create shared composite-type-lookup cache struct */
            // rowcachep = palloc(sizeof(ExprEvalRowtypeCache));
            // rowcachep->cacheptr = NULL;
            //
            // C shares one rowcachep between the DEFORM and FORM steps; in the
            // owned model the rowtype lookup goes through the internally-cached
            // typcache seam (the void* cacheptr cannot round-trip), so the cache
            // carries no cross-step state — give each step its own default box.
            let deform_rowcache = mcx::alloc_in(mcx, ExprEvalRowtypeCache::default())?;
            let form_rowcache = mcx::alloc_in(mcx, ExprEvalRowtypeCache::default())?;

            // /* emit code to evaluate the composite input value */
            // ExecInitExprRec(fstore->arg, state, resv, resnull);
            let arg = fstore.arg.as_deref().expect("FieldStore.arg present");
            exec_init_expr_rec(mcx, arg, state, resv)?;

            // /* next, deform the input tuple into our workspace */
            // scratch.opcode = EEOP_FIELDSTORE_DEFORM;
            // scratch.d.fieldstore.fstore   = fstore;
            // scratch.d.fieldstore.rowcache = rowcachep;
            // scratch.d.fieldstore.values   = values;
            // scratch.d.fieldstore.nulls    = nulls;
            // scratch.d.fieldstore.ncolumns = ncolumns;
            // ExprEvalPushStep(state, &scratch);
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_FIELDSTORE_DEFORM,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::FieldStore {
                    resulttype: fstore.resulttype,
                    rowcache: Some(deform_rowcache),
                    col_cells: Some(col_cells.clone()),
                    ncolumns,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;

            // /* evaluate new field values, store in workspace columns */
            // forboth(l1, fstore->newvals, l2, fstore->fieldnums) { ... }
            debug_assert_eq!(fstore.newvals.len(), fstore.fieldnums.len());
            for (e, &fieldnum) in fstore.newvals.iter().zip(fstore.fieldnums.iter()) {
                // if (fieldnum <= 0 || fieldnum > ncolumns)
                //     elog(ERROR, "field number %d is out of range in FieldStore", ...);
                if fieldnum <= 0 || fieldnum as i32 > ncolumns {
                    return Err(types_error::PgError::error(format!(
                        "field number {fieldnum} is out of range in FieldStore"
                    )));
                }

                // The field's workspace cell is both the CaseTestExpr source (so a
                // nested FieldStore/SubscriptingRef newval can read the old value
                // being replaced) and the result address for this sub-expression.
                // In the owned model both halves are the one paired ResultCell.
                //
                // save_innermost_caseval = state->innermost_caseval;
                // state->innermost_caseval = &values[fieldnum - 1];
                // (innermost_casenull rides along on the same paired cell)
                let field_cell = col_cells[(fieldnum - 1) as usize];
                let save_caseval = state.innermost_caseval;
                state.innermost_caseval = Some(field_cell);

                // ExecInitExprRec(e, state, &values[fieldnum-1], &nulls[fieldnum-1]);
                exec_init_expr_rec(mcx, e, state, field_cell)?;

                // state->innermost_caseval = save_innermost_caseval;
                state.innermost_caseval = save_caseval;
            }

            // /* finally, form result tuple */
            // scratch.opcode = EEOP_FIELDSTORE_FORM; ... ExprEvalPushStep(state, &scratch);
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_FIELDSTORE_FORM,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::FieldStore {
                    resulttype: fstore.resulttype,
                    rowcache: Some(form_rowcache),
                    col_cells: Some(col_cells),
                    ncolumns,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        // ----- T_CoerceToDomain -----
        etag::T_CoerceToDomain => {
            let ctest = node.expect_coercetodomain();
            // C: ExecInitCoerceToDomain(&scratch, ctest, state, resv, resnull);
            // The DOMAIN_NOTNULL/DOMAIN_CHECK step emission is owned by the
            // execExpr_domain_agg sibling family (it threads the
            // InitDomainConstraintRef typcache lookup and the per-CHECK
            // recursion); the dispatch routes there, exactly as the C switch
            // delegates to ExecInitCoerceToDomain.
            let mut scratch = scratch_for(resv);
            let arg = ctest.arg.as_deref().expect("CoerceToDomain.arg present");
            crate::execExpr_domain_agg::exec_init_coerce_to_domain(
                mcx,
                &mut scratch,
                ctest.resulttype,
                arg,
                state,
                resv,
            )
        }

        // ----- T_CoerceToDomainValue -----
        etag::T_CoerceToDomainValue => {
            // C: read from innermost_domainval; if NULL (a standalone domain
            //    check rather than one embedded in a larger expression) we must
            //    read from econtext->domainValue_datum via the specialized
            //    EEOP_DOMAIN_TESTVAL_EXT op.
            let scratch = match state.innermost_domainval {
                None => ExprEvalStep {
                    opcode: ExprEvalOp::EEOP_DOMAIN_TESTVAL_EXT,
                    resvalue: resv,
                    resnull: resv,
                    d: ExprEvalStepData::NoPayload,
                },
                Some(cell) => ExprEvalStep {
                    opcode: ExprEvalOp::EEOP_DOMAIN_TESTVAL,
                    resvalue: resv,
                    resnull: resv,
                    // C shares the casetest union variant with DOMAIN_TESTVAL:
                    //   scratch.d.casetest.value = state->innermost_domainval;
                    //   scratch.d.casetest.isnull = state->innermost_domainnull;
                    d: ExprEvalStepData::CaseTest { value: cell },
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        etag::T_SubPlan => {
            let subplan = node.expect_subplan();
            // `Expr::SubPlan` now carries `Box<SubPlan<'mcx>>` (the `'mcx`-threaded
            // Expr enum); the SubPlan tree is the same `'mcx` arena, so it is
            // borrowed directly (the former `'static`→`'mcx` transmute is gone).
            let sub: &types_nodes::primnodes::SubPlan<'mcx> = &subplan.0;
            crate::execExpr_func_subscript::exec_init_sub_plan_expr(mcx, sub, state, resv)
        }
        etag::T_AlternativeSubPlan => panic!(
            "execExpr-core: AlternativeSubPlan must be replaced by a concrete SubPlan before \
             execution (planner: select cheapest alternative)"
        ),
        etag::T_WindowFunc => {
            let wfunc = node.expect_windowfunc();
            // C (execExpr.c ExecInitExprRec T_WindowFunc):
            //   WindowFuncExprState *wfstate = makeNode(WindowFuncExprState);
            //   wfstate->wfunc = wfunc;
            //   if (state->parent && IsA(state->parent, WindowAggState)) {
            //       winstate->funcs = lappend(winstate->funcs, wfstate);
            //       nfuncs = ++winstate->numfuncs;
            //       if (wfunc->winagg) winstate->numaggs++;
            //       wfstate->args = ExecInitExprList(wfunc->args, state->parent);
            //       wfstate->aggfilter = ExecInitExpr(wfunc->aggfilter, state->parent);
            //   } else elog(ERROR, "WindowFunc found in non-WindowAgg plan node");
            //   scratch.opcode = EEOP_WINDOW_FUNC;
            //   scratch.d.window_func.wfstate = wfstate;
            //
            // The C `state->parent` IS the in-flight WindowAggState; the owned
            // model has not stamped `state.parent` yet (deferred to
            // `stamp_expr_parents`), so the `WindowFuncExprState`s are collected
            // on the ExprState's `found_window_funcs` channel and drained into
            // `winstate.funcs` by `ExecInitWindowAgg` after this projection
            // compile (the window analogue of the `found_aggs` channel). The
            // "WindowFunc found in non-WindowAgg plan node" guard becomes the
            // `es_link.is_none()` test: a parentless `ExecInitExpr` leaves it
            // None, exactly C's `!state->parent` arm. The args/aggfilter are
            // compiled with their own EState back-link (reached through
            // `es_link`, as the SubPlan compile does).
            if state.es_link.is_none() {
                return Err(types_error::PgError::error(
                    "WindowFunc found in non-WindowAgg plan node",
                ));
            }

            // Compile args + aggfilter against the EState (reached through the
            // compile-entry-stamped `es_link`). The args are independent
            // ExprStates owned by the WindowFuncExprState, exactly like C's
            // `ExecInitExprList(wfunc->args, state->parent)`.
            let mut es_link =
                state.es_link.expect("WindowFunc: es_link present (guarded above)");
            let estate = es_link.get_mut();

            let arg_refs: PgVec<'mcx, Option<&Expr>> = {
                let mut v: PgVec<'mcx, Option<&Expr>> =
                    mcx::vec_with_capacity_in(mcx, wfunc.args.len())?;
                for a in &wfunc.args {
                    v.push(Some(a));
                }
                v
            };
            let arg_states = crate::execExpr_core::exec_init_expr_list_for_window(
                arg_refs.as_slice(),
                estate,
            )?;
            let aggfilter_state = match wfunc.aggfilter.as_deref() {
                None => None,
                Some(f) => Some(crate::execExpr_core::exec_init_expr_no_parent_box(f, estate)?),
            };

            // makeNode(WindowFuncExprState); wfstate->wfunc = wfunc.
            let wfstate = types_nodes::nodewindowagg::WindowFuncExprState {
                wfunc: Some(mcx::alloc_in(mcx, wfunc.clone_in(mcx)?)?),
                args: arg_states,
                aggfilter: aggfilter_state,
                // wfuncno is assigned by ExecInitWindowAgg's dedup loop.
                wfuncno: 0,
            };

            // winstate->funcs = lappend(winstate->funcs, wfstate) — collected on
            // the discovery channel; funcidx is the append position.
            if state.found_window_funcs.is_none() {
                state.found_window_funcs = Some(mcx::vec_with_capacity_in(mcx, 1)?);
            }
            let funcs = state
                .found_window_funcs
                .as_mut()
                .expect("found_window_funcs just initialized");
            let funcidx = funcs.len() as i32;
            funcs.push(mcx::alloc_in(mcx, wfstate)?);

            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_WINDOW_FUNC,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::WindowFunc { funcidx },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }
        // ----- T_JsonExpr (JSON_VALUE / JSON_QUERY / JSON_EXISTS / JSON_TABLE) -----
        etag::T_JsonExpr => {
            let jsexpr = node.expect_jsonexpr();
            // No need to initialize a full JsonExprState for JSON_TABLE(),
            // because the upstream caller tfuncFetchRows() is only interested
            // in the value of formatted_expr (the document). (execExpr.c:2493)
            if jsexpr.op == types_nodes::primnodes::JsonExprOp::JSON_TABLE_OP {
                let formatted_expr = jsexpr
                    .formatted_expr
                    .as_deref()
                    .expect("JSON_TABLE JsonExpr.formatted_expr present");
                exec_init_expr_rec(mcx, formatted_expr, state, resv)
            } else {
                crate::execExpr_json::exec_init_json_expr(mcx, jsexpr, state, resv)
            }
        }
        // ----- T_XmlExpr (XMLELEMENT / XMLFOREST / XMLCONCAT / ...) -----
        etag::T_XmlExpr => {
            let xexpr = node.expect_xmlexpr();
            crate::execExpr_json::exec_init_xml_expr(mcx, xexpr, state, resv)
        }
        // ----- T_JsonValueExpr (bare `expr [FORMAT ...]`) -----
        etag::T_JsonValueExpr => {
            let jve = node.expect_jsonvalueexpr();
            crate::execExpr_json::exec_init_json_value_expr(mcx, jve, state, resv)
        }
        // ----- T_JsonConstructorExpr (JSON_OBJECT / JSON_ARRAY / ...) -----
        etag::T_JsonConstructorExpr => {
            let ctor = node.expect_jsonconstructorexpr();
            crate::execExpr_json::exec_init_json_constructor(mcx, ctor, state, resv)
        }
        // ----- T_JsonIsPredicate (IS JSON [VALUE|OBJECT|ARRAY|SCALAR]) -----
        etag::T_JsonIsPredicate => {
            let pred = node.expect_jsonispredicate();
            crate::execExpr_json::exec_init_json_is_predicate(mcx, pred, state, resv)
        }
        etag::T_SetToDefault => panic!(
            "execExpr-core: SetToDefault must have been replaced before execution (planner); \
             reaching ExecInitExprRec with one is a planner error"
        ),
        etag::T_SubLink => panic!(
            "execExpr-core: SubLink is always replaced by a SubPlan before execution"
        ),
        etag::T_InferenceElem => panic!(
            "execExpr-core: InferenceElem is a planner-only unique-index inference node, never \
             compiled"
        ),
        // ----- T_ReturningExpr -----
        etag::T_ReturningExpr => {
            let rexpr = node
                .as_returningexpr()
                .expect("ReturningExpr");

            // Skip expression evaluation if OLD/NEW row doesn't exist.
            let nullflag = if rexpr.retold {
                EEO_FLAG_OLD_IS_NULL
            } else {
                EEO_FLAG_NEW_IS_NULL
            };
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_RETURNINGEXPR,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::ReturningExpr {
                    nullflag,
                    jumpdone: -1, // set below
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            let retstep = (state.steps_len - 1) as usize;

            // Steps to evaluate expression to return.
            let retexpr = rexpr
                .retexpr
                .as_deref()
                .expect("ReturningExpr.retexpr present");
            exec_init_expr_rec(mcx, retexpr, state, resv)?;

            // Jump target used if OLD/NEW row doesn't exist.
            let target = state.steps_len;
            let steps = state.steps.as_mut().expect("returningexpr steps");
            if let ExprEvalStepData::ReturningExpr { jumpdone, .. } = &mut steps[retstep].d {
                *jumpdone = target;
            }

            // Update ExprState flags.
            if rexpr.retold {
                state.flags |= EEO_FLAG_HAS_OLD;
            } else {
                state.flags |= EEO_FLAG_HAS_NEW;
            }
            Ok(())
        }
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
pub fn exec_init_expr<'mcx, 'e>(
    node: &Expr<'e>,
    parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    // `parent` is the embedded `PlanState` head, not the enclosing
    // `PlanStateNode` enum. The `ExprState.parent` back-link the C
    // `state->parent = parent` sets must point at the *enum* (its consumers —
    // `EEOP_GROUPING_FUNC` etc. — call `parent.as_agg_state()`, an enum method),
    // whose address is not yet stable while this node is still being built. So
    // `parent` is back-filled by `ExecInitNode` (`PlanStateNode::stamp_expr_parents`)
    // right after the node is boxed; the head is unused here.
    let _ = parent;
    let mcx = estate.es_query_cxt;

    // C aliases the caller's `node` pointer; the owned model clones it into the
    // per-query context (`'mcx`) up front and drives both `state.expr` and the
    // opcode-emission recursion off that `'mcx` copy — so the input `node` only
    // needs to be valid for the call (an independent `'e`), matching C's read-only
    // use of the passed expression (a plan-tree node the executor only reads).
    let node: &Expr<'mcx> = &*mcx::leak_in(mcx::alloc_in(mcx, node.clone_in(mcx)?)?);

    let mut state = make_expr_state();
    // C `state->expr = node` — retain the original expression on the ExprState
    // (read back by callers like JSON_TABLE's PASSING-args loop, which derives
    // each variable's exprType/exprTypmod from state->expr).
    state.expr = Some(mcx::alloc_in(mcx, node.clone_in(mcx)?)?);
    // C `state->parent = parent` reaches the EState via `parent->state`; the
    // owned model defers parent stamping to `stamp_expr_parents`, so we stamp
    // the non-owning EState back-link here (a parent IS present at this entry —
    // it is `ExecInitExpr(node, parent)`), letting a SubPlan compile find
    // `es_subplanstates` synchronously.
    state.es_link = Some(EStateLink::from_ref(estate));
    state.ext_params = 0;
    ensure_result_arena(mcx, &mut state)?;

    exec_create_expr_setup_steps(mcx, &mut state, node)?;
    exec_init_expr_rec(mcx, node, &mut state, STATE_RESULT_CELL)?;
    expr_eval_push_step(mcx, &mut state, done_return_step(STATE_RESULT_CELL))?;
    exec_ready_expr(&mut state)?;

    mcx::alloc_in(mcx, state)
}

/// `ExecInitExprWithParams(node, ext_params)` (execExpr.c).
pub fn exec_init_expr_with_params<'mcx, 'e>(
    node: &Expr<'e>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let _ = econtext;
    let mcx = estate.es_query_cxt;

    // Clone the read-only input into `'mcx` and drive the opcode recursion off
    // that copy (see `exec_init_expr`); the input `node` only needs `'e`.
    let node: &Expr<'mcx> = &*mcx::leak_in(mcx::alloc_in(mcx, node.clone_in(mcx)?)?);

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
    qual: Option<&[Expr<'mcx>]>,
    parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    // `parent` is the embedded head, not the enclosing `PlanStateNode` enum; the
    // `ExprState.parent` back-link (consumed by `EEOP_GROUPING_FUNC` etc. via the
    // enum method `parent.as_agg_state()`) is back-filled by `ExecInitNode`
    // (`PlanStateNode::stamp_expr_parents`) once the node's enum is boxed and
    // address-stable. See `exec_init_expr`.
    let _ = parent;
    exec_init_qual_no_parent(qual, estate)
}

/// `ExecInitQual(qual, NULL)` (execExpr.c) — the parent-less variant of
/// [`exec_init_qual`], used by [`exec_prepare_qual`]. The owned spine already
/// ignores `parent` (only the non-owning `es_link` back-pointer is threaded, and
/// that points at `estate` either way), so this compiles the identical program;
/// kept as a distinct entry to mirror the C `ExecInitQual(qual, NULL)` shape.
pub fn exec_init_qual_no_parent<'mcx>(
    qual: Option<&[Expr<'mcx>]>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    let qual = match qual {
        None => return Ok(None),
        Some(q) if q.is_empty() => return Ok(None),
        Some(q) => q,
    };
    let mcx = estate.es_query_cxt;

    let mut state = make_expr_state();
    // A parent IS present at this entry (`ExecInitQual(qual, parent)`); stamp the
    // non-owning EState back-link so a SubPlan in the qual can find
    // `es_subplanstates` at compile time (parent itself is stamped later by
    // `stamp_expr_parents`).
    state.es_link = Some(EStateLink::from_ref(estate));
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

/// `ExecPrepareQual(qual, estate)` (execExpr.c) — prepare a standalone qual with
/// no parent `PlanState`. C does
/// `qual = (List *) expression_planner((Expr *) qual)` then
/// `ExecInitQual(qual, NULL)`, in `estate->es_query_cxt`.
///
/// An empty qual (the C `NIL`) compiles to `None` (the always-true `NULL`
/// ExprState) without touching the planner — the index-build path's
/// non-partial-index case. A non-empty qual is const-folded element-by-element
/// through the `expression_planner_value` VALUE seam (eval_const_expressions +
/// fix_opfuncids — an implicit-AND qual list folds each top-level element
/// independently), then compiled by the parent-less [`exec_init_qual_no_parent`]
/// (which sets `EEO_FLAG_IS_QUAL`, so `ExecQual` semantics apply: NULL → FALSE).
pub fn exec_prepare_qual<'mcx>(
    qual: Option<&[Expr<'mcx>]>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    // MemoryContextSwitchTo(estate->es_query_cxt) is implicit: the compile in
    // exec_init_qual_no_parent allocates in estate.es_query_cxt.
    let qual = match qual {
        None => return Ok(None),
        Some(q) if q.is_empty() => return Ok(None),
        Some(q) => q,
    };
    let mcx = estate.es_query_cxt;

    // qual = (List *) expression_planner((Expr *) qual);
    //
    // The `expression_planner_value` VALUE seam plans into `mcx` but is typed with
    // the planner's notional `'static` erasure; re-clone the planned result back
    // into the query context (`mcx`) so it is properly typed `Expr<'mcx>` for the
    // compile below (`exec_init_expr_rec` threads the node tree as `'mcx`).
    let mut planned: Vec<Expr<'mcx>> = Vec::with_capacity(qual.len());
    for q in qual {
        let owned = q.clone_in(mcx)?.erase_lifetime();
        let pl = backend_optimizer_plan_planner_pc_seams::expression_planner_value::call(mcx, owned)?;
        planned.push(pl.clone_in(mcx)?);
    }

    // ExecInitQual(qual, NULL);
    exec_init_qual_no_parent(Some(&planned), estate)
}

/// `ExecInitExprList(nodes, parent)` (execExpr.c).
pub fn exec_init_expr_list<'mcx>(
    nodes: &[Option<&Expr<'mcx>>],
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

/// `ExecInitExprList(nodes, NULL)` (execExpr.c) — the parentless variant,
/// used by `ValuesNext` for the single-use per-row VALUES expressions. Same as
/// [`exec_init_expr_list`] but compiles each element with no parent
/// `PlanState` (the C `ExecInitExprList(exprlist, NULL)`), so nothing in the
/// transient eval state links into the permanent plan tree and JIT is disabled.
pub fn exec_init_expr_list_no_parent<'mcx>(
    nodes: &[Option<&Expr<'mcx>>],
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgVec<'mcx, Option<ExprState<'mcx>>>> {
    let mcx = estate.es_query_cxt;
    let mut result: PgVec<'mcx, Option<ExprState<'mcx>>> =
        mcx::vec_with_capacity_in(mcx, nodes.len())?;
    for e in nodes {
        match e {
            None => result.push(None),
            Some(node) => {
                let state = exec_init_expr_no_parent(node, estate)?;
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
/// SQL-function inlining), reached over the deps-less `expression_planner_value`
/// VALUE seam (installed by the planner unit) so the executor does not depend on
/// the optimizer owner directly.
pub fn exec_prepare_expr<'mcx>(
    node: &Expr<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // node = expression_planner(node);
    //
    // expression_planner() (optimizer/planner.c) const-folds and inlines the
    // standalone expression before compilation (eval_const_expressions(NULL,
    // node) + fix_opfuncids). The owning seam consumes the Expr by value and
    // returns the planned Expr allocated in `mcx`, so we clone the borrowed
    // `node` into the query context first.
    // The VALUE seam plans into `mcx` but is typed with the planner's notional
    // `'static` erasure; re-clone the planned result back into the query context
    // so it is properly typed `Expr<'mcx>` for the compile below.
    let owned = node.clone_in(mcx)?.erase_lifetime();
    let planned =
        backend_optimizer_plan_planner_pc_seams::expression_planner_value::call(mcx, owned)?
            .clone_in(mcx)?;

    // result = ExecInitExpr(node, NULL);  — the parent-less compile.
    exec_init_expr_no_parent(&planned, estate)
}

/// `ExecPrepareCheck(qual, estate)` (execExpr.c) — compile an implicit-AND
/// `qual` list as a CHECK constraint (a NULL conjunction result is TRUE) for
/// use outside a normal Plan tree.
///
/// C does `qual = (List *) expression_planner((Expr *) qual)` then
/// `ExecInitCheck(qual, NULL)` in `estate->es_query_cxt`. An empty `qual` (the
/// C `NIL`) compiles to `None` (the always-true `NULL` ExprState) without
/// touching the planner; a non-empty `qual` is const-folded element-by-element
/// through the `expression_planner_value` VALUE seam (eval_const_expressions +
/// fix_opfuncids; folding an implicit-AND list folds each top-level element
/// independently — there is no AND wrapper to fold across), then
/// `ExecInitCheck(make_ands_explicit(qual), NULL)` = `make_ands_explicit` +
/// the parent-less [`exec_init_expr_no_parent`]. `ExecInitCheck` compiles the
/// AND-clause with `ExecInitExpr` (NOT `ExecInitQual`), so the `IS_QUAL` flag is
/// left clear — `ExecCheck` treats a NULL result as TRUE.
pub fn exec_prepare_check<'mcx>(
    qual: &[Expr<'mcx>],
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    // MemoryContextSwitchTo(estate->es_query_cxt) is implicit: the compile in
    // exec_init_expr_no_parent allocates in estate.es_query_cxt.
    if qual.is_empty() {
        // ExecInitCheck(NIL, ...) == NULL
        return Ok(None);
    }
    let mcx = estate.es_query_cxt;

    // qual = (List *) expression_planner((Expr *) qual);
    //
    // Re-clone each planned element back into the query context (`mcx`); the VALUE
    // seam is typed with the planner's notional `'static` erasure.
    let mut planned: Vec<Expr<'mcx>> = Vec::with_capacity(qual.len());
    for q in qual {
        let owned = q.clone_in(mcx)?.erase_lifetime();
        let pl = backend_optimizer_plan_planner_pc_seams::expression_planner_value::call(mcx, owned)?;
        planned.push(pl.clone_in(mcx)?);
    }

    // ExecInitCheck(qual, NULL): make_ands_explicit(qual) then
    // ExecInitExpr(result, NULL).
    let anded = backend_nodes_core::makefuncs::make_ands_explicit(planned);
    Ok(Some(exec_init_expr_no_parent(&anded, estate)?))
}

/// `ExecInitExpr(node, NULL)` (execExpr.c) — the `parent = NULL` shape used by
/// [`exec_prepare_expr`] and `ExecInitExprWithParams`. Identical to
/// [`exec_init_expr`] but threads no `PlanState` (the owned spine already
/// ignores `parent` — see [`exec_init_expr`] — so this compiles the same
/// program; kept as a distinct entry to mirror the C `ExecInitExpr(node, NULL)`
/// call sites and to make the parent-less contract explicit).
pub fn exec_init_expr_no_parent<'mcx>(
    node: &Expr<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let mut state = make_expr_state();
    // C `state->expr = node` (ExecInitExpr).
    state.expr = Some(mcx::alloc_in(mcx, node.clone_in(mcx)?)?);
    state.ext_params = 0;
    ensure_result_arena(mcx, &mut state)?;

    exec_create_expr_setup_steps(mcx, &mut state, node)?;
    exec_init_expr_rec(mcx, node, &mut state, STATE_RESULT_CELL)?;
    expr_eval_push_step(mcx, &mut state, done_return_step(STATE_RESULT_CELL))?;
    exec_ready_expr(&mut state)?;

    mcx::alloc_in(mcx, state)
}

/// `ExecInitExpr(node, NULL)` (execExpr.c) compiled into a **bare
/// `MemoryContext`** with no owning `EState` — the spine the domain-constraint
/// `ExecInitExpr(check_expr, NULL)` (utils/cache/typcache.c
/// `prep_domain_constraints`) needs. Identical opcode emission to
/// [`exec_init_expr_no_parent`] (`ExecCreateExprSetupSteps` + `ExecInitExprRec`
/// + `EEOP_DONE_RETURN` + `ExecReadyExpr`), but allocates the program directly
/// in `mcx` (the C `refctx`) instead of an `estate.es_query_cxt`, and stamps no
/// `es_link` — a domain CHECK predicate references no Vars/Params/SubPlans (only
/// the domain test value the econtext supplies at eval time), so no `EState`
/// back-link is needed at compile time. Returns the compiled state by value (the
/// domain-constraint registry owns it inside the same `mcx` bundle).
pub fn compile_standalone_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: &Expr<'mcx>,
) -> PgResult<ExprState<'mcx>> {
    let mut state = make_expr_state();
    state.ext_params = 0;
    ensure_result_arena(mcx, &mut state)?;

    exec_create_expr_setup_steps(mcx, &mut state, node)?;
    exec_init_expr_rec(mcx, node, &mut state, STATE_RESULT_CELL)?;
    expr_eval_push_step(mcx, &mut state, done_return_step(STATE_RESULT_CELL))?;
    exec_ready_expr(&mut state)?;

    Ok(state)
}

/// `ExecInitExpr(node, state->parent)` for a window function argument /
/// aggfilter sub-expression — compiles `node` into its own `ExprState` and
/// stamps the EState back-link (`es_link`) so a nested SubPlan / param eval can
/// reach the EState synchronously, exactly as [`exec_init_expr`] does. The
/// `WindowAggState` parent's enum address is not yet stable (the back-link is
/// stamped by `ExecInitWindowAgg`/`stamp_expr_parents` afterward), so the
/// `parent` head pointer is not threaded; only `es_link` is set, which is all
/// the window-API argument evaluators (`WinGetFuncArg*`) need.
pub fn exec_init_expr_no_parent_box<'mcx>(
    node: &Expr<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    // C `ExecInitExpr(node, state->parent)`: a parent IS present, so a nested
    // SubPlan (e.g. `lead(ten, (SELECT ...)) OVER (...)`) must find the EState
    // synchronously. The `es_link` back-link must be stamped BEFORE the
    // `ExecInitExprRec` recursion — `ExecInitSubPlanExpr` reads it during that
    // recursion (it is the owned-model stand-in for the C `!state->parent`
    // test). Stamping it after recursion (as a thin wrapper over the
    // parent-less compile) would make a window-arg SubPlan see `es_link == None`
    // and wrongly error "SubPlan found with no parent plan". So we inline the
    // compile here with `es_link` set up front, mirroring `exec_init_expr`.
    let mcx = estate.es_query_cxt;

    let mut state = make_expr_state();
    state.es_link = Some(EStateLink::from_ref(estate));
    state.ext_params = 0;
    ensure_result_arena(mcx, &mut state)?;

    exec_create_expr_setup_steps(mcx, &mut state, node)?;
    exec_init_expr_rec(mcx, node, &mut state, STATE_RESULT_CELL)?;
    expr_eval_push_step(mcx, &mut state, done_return_step(STATE_RESULT_CELL))?;
    exec_ready_expr(&mut state)?;

    mcx::alloc_in(mcx, state)
}

/// `ExecInitExprList(wfunc->args, state->parent)` for a window function's
/// argument list — compiles each argument into its own EState-linked
/// `ExprState` and returns them in the `WindowFuncExprState.args` shape
/// (`Option<PgVec<PgBox<ExprState>>>`). `None` for an empty list (the C NIL).
pub fn exec_init_expr_list_for_window<'mcx>(
    nodes: &[Option<&Expr<'mcx>>],
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>>> {
    if nodes.is_empty() {
        return Ok(None);
    }
    let mcx = estate.es_query_cxt;
    let mut out: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>> =
        mcx::vec_with_capacity_in(mcx, nodes.len())?;
    for e in nodes {
        // Window function args are never NULL list elements.
        let node = e.expect("window function argument list element is NULL");
        out.push(exec_init_expr_no_parent_box(node, estate)?);
    }
    Ok(Some(out))
}

/// `ExecPrepareExprList(exprList, estate)` (execExpr.c).
pub fn exec_prepare_expr_list<'mcx>(
    expr_list: &[Expr<'mcx>],
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
    slot: Option<SlotId>,
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
    // Stamp the non-owning EState back-link (a parent IS present at this entry,
    // `ExecBuildProjectionInfo(targetList, ..., parent, ...)`), so a SubPlan in
    // the projection target list (e.g. `SELECT x IN (SELECT ...)`) can find
    // `es_subplanstates` synchronously at compile time. The `parent` PlanState
    // itself is stamped later by `stamp_expr_parents`.
    state.es_link = Some(EStateLink::from_ref(estate));
    state.ext_params = 0;
    // state->resultslot = slot; — the projection's output slot (a pool SlotId,
    // C's `TupleTableSlot *`). The interpreter's EEOP_ASSIGN_* arms write the
    // projected columns into this slot at ExecProject time.
    state.resultslot = slot;
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
            // INDEX_VAR handled by default case. The C switches on
            // variable->varreturningtype (DEFAULT/OLD/NEW) to pick
            // ASSIGN_SCAN_VAR / ASSIGN_OLD_VAR / ASSIGN_NEW_VAR (setting
            // EEO_FLAG_HAS_OLD/NEW), so OLD/NEW columns read from the OLD/NEW
            // tuple slot rather than the scan slot.
            let opcode = match v.varno {
                INNER_VAR => ExprEvalOp::EEOP_ASSIGN_INNER_VAR,
                OUTER_VAR => ExprEvalOp::EEOP_ASSIGN_OUTER_VAR,
                _ => match v.varreturningtype {
                    VrtKind::VAR_RETURNING_DEFAULT => ExprEvalOp::EEOP_ASSIGN_SCAN_VAR,
                    VrtKind::VAR_RETURNING_OLD => {
                        state.flags |= EEO_FLAG_HAS_OLD;
                        ExprEvalOp::EEOP_ASSIGN_OLD_VAR
                    }
                    VrtKind::VAR_RETURNING_NEW => {
                        state.flags |= EEO_FLAG_HAS_NEW;
                        ExprEvalOp::EEOP_ASSIGN_NEW_VAR
                    }
                },
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

/// `ExecBuildProjectionInfo(targetList, econtext, slot, parent, inputDesc)`
/// (execExpr.c) — seam-facing variant mirroring the C `ExecAssignProjectionInfo`
/// call shape: it reads the target list off `planstate->plan->targetlist` and
/// the expression context off `planstate->ps_ExprContext`, then emits the
/// per-column projection program via [`exec_build_projection_info_impl`].
pub fn exec_build_projection_info<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    input_desc: Option<&TupleDescData<'_>>,
) -> PgResult<PgBox<'mcx, ProjectionInfo<'mcx>>> {
    // planstate->plan->targetlist — the generic `Plan` base targetlist, read off
    // whichever plan-node variant the `PlanState` aliases, via the generic
    // `Plan` base accessor `Node::plan_head` (which projects the embedded `Plan`
    // struct's `targetlist` uniformly across every plan-node variant — Result,
    // SeqScan, Agg, Sort, … — matching C's `((Plan *) node)->targetlist`). The
    // plan tree is borrowed (`&'mcx Node`, never copied into the state), so the
    // target-list slice outlives this call independently of the `&mut estate`
    // borrow. `NIL` (modeled as `None`) is an empty tlist → a no-op projection.
    let target_list: &[types_nodes::TargetEntry<'mcx>] = planstate
        .plan
        .as_deref()
        .expect("ExecBuildProjectionInfo: PlanState has no plan")
        .plan_head()
        .targetlist
        .as_deref()
        .unwrap_or(&[]);

    // planstate->ps_ExprContext — the node's expression-evaluation context.
    let econtext = planstate
        .ps_ExprContext
        .expect("ExecBuildProjectionInfo: PlanState has no ps_ExprContext");

    // planstate->ps_ResultTupleSlot — the projection's output slot (C's
    // ExecAssignProjectionInfo passes `planstate->ps_ResultTupleSlot` as the
    // `slot` argument). Already a pool SlotId.
    let slot = planstate.ps_ResultTupleSlot;

    exec_build_projection_info_impl(estate, target_list, econtext, slot, input_desc)
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
    slot: Option<SlotId>,
) -> PgResult<PgBox<'mcx, ProjectionInfo<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // ProjectionInfo *projInfo = makeNode(ProjectionInfo);
    let mut proj_info = ProjectionInfo::default();
    proj_info.pi_exprContext = Some(econtext);
    let state = &mut proj_info.pi_state;
    // if (evalTargetList) state->expr = (Expr *) targetList; else state->expr = NULL;
    state.ext_params = 0;
    // state->resultslot = slot; — the UPDATE projection's output slot.
    state.resultslot = slot;
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
                    value: DatumV::null(),
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
    let mcx = estate.es_query_cxt;

    // subplan->targetlist — the UPDATE subplan's (already-evaluated) target list.
    // Borrowed off the plan tree, which outlives this call independently of the
    // `&mut estate` borrow.
    let target_list: &[types_nodes::TargetEntry<'mcx>] = mtstate
        .ps
        .plan
        .as_deref()
        .expect("ExecBuildUpdateProjection: mtstate->ps.plan is NULL")
        .outer_plan()
        .expect("ExecBuildUpdateProjection: outerPlan(node) is NULL")
        .plan_head()
        .targetlist
        .as_deref()
        .unwrap_or(&[]);

    // relDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc). The impl reads
    // it by reference; clone into `mcx` so it does not alias the `&mut estate`.
    let rel_desc: TupleDescData<'mcx> = {
        let rel = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecBuildUpdateProjection: result relation must be open");
        rel.rd_att.clone_in(mcx)?
    };

    // mtstate->ps.ps_ExprContext — the projection's expression context.
    let econtext = mtstate
        .ps
        .ps_ExprContext
        .expect("ExecBuildUpdateProjection: mtstate->ps has no ps_ExprContext");

    // resultRelInfo->ri_newTupleSlot — the projection's output slot.
    let new_tuple_slot = estate.result_rel(result_rel_info).ri_newTupleSlot;

    // resultRelInfo->ri_projectNew = ExecBuildUpdateProjection(subplan->targetlist,
    //     false /* subplan did the evaluation */, updateColnos, relDesc, econtext,
    //     ri_newTupleSlot, &mtstate->ps);
    let proj = exec_build_update_projection_impl(
        estate,
        target_list,
        false,
        update_colnos,
        &rel_desc,
        econtext,
        new_tuple_slot,
    )?;

    let rri = estate.result_rel_mut(result_rel_info);
    rri.ri_projectNew = Some(proj);
    rri.ri_has_project_new = true;
    Ok(())
}

// ===========================================================================
// Evaluation entry points — dispatched to the interpreter (execExprInterp)
// ===========================================================================

/// `ExecEvalExprSwitchContext(state, econtext, &isnull)` (executor.h).
pub fn exec_eval_expr_switch_context<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(DatumV<'mcx>, bool)> {
    backend_executor_execExprInterp_seams::exec_eval_expr_switch_context::call(
        state, econtext, estate,
    )
}

/// `(ItemPointer) DatumGetPointer(ExecEvalExprSwitchContext(...))` (executor.h).
pub fn exec_eval_tid_expr_switch_context<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(ItemPointerData, bool)> {
    // itemptr = (ItemPointer) DatumGetPointer(
    //     ExecEvalExprSwitchContext(tidexpr->exprstate, econtext, &isNull));
    let (datum, isnull) = exec_eval_expr_switch_context(state, econtext, estate)?;
    if isnull {
        // The caller (nodeTidscan) discards the value when isNull; return a
        // placeholder ItemPointer that it never reads.
        return Ok((ItemPointerData::new(0, 0), true));
    }
    // The TID Datum crosses by reference as the canonical 6-byte ItemPointerData
    // image: BlockIdData{bi_hi, bi_lo} (two native-endian uint16) then the
    // uint16 ip_posid offset (storage/itemptr.h native struct layout).
    let image = datum.as_ref_bytes();
    let bi_hi = u16::from_ne_bytes([image[0], image[1]]);
    let bi_lo = u16::from_ne_bytes([image[2], image[3]]);
    let off = u16::from_ne_bytes([image[4], image[5]]);
    let block_number = ((bi_hi as u32) << 16) | bi_lo as u32;
    Ok((ItemPointerData::new(block_number, off), false))
}

/// `ExecEvalExprSwitchContext(...)` yielding a `tid[]` array `Datum`.
pub fn exec_eval_array_expr_switch_context<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(DatumV<'mcx>, bool)> {
    backend_executor_execExprInterp_seams::exec_eval_expr_switch_context::call(
        state, econtext, estate,
    )
}

/// `ExecCheck(state, econtext)` (execExpr.c) — evaluate a check expression
/// (e.g. a CHECK constraint or WITH CHECK OPTION). Unlike `ExecQual`, a NULL
/// result is treated as TRUE.
pub fn exec_check<'mcx>(
    state: Option<&mut ExprState<'mcx>>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // short-circuit (here and in ExecInitCheck) for empty restriction list
    let state = match state {
        None => return Ok(true),
        Some(state) => state,
    };

    // verify that expression was not compiled using ExecInitQual
    debug_assert!(state.flags & EEO_FLAG_IS_QUAL == 0);

    let (ret, isnull) = exec_eval_expr_switch_context(state, econtext, estate)?;

    if isnull {
        return Ok(true);
    }

    Ok(ret.as_bool())
}

/// `ExecQual(state, econtext)` (executor.h).
pub fn exec_qual<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let (ret, isnull) = exec_eval_expr_switch_context(state, econtext, estate)?;
    Ok(!isnull && ret.as_bool())
}

/// `ExecQualAndReset(state, econtext)` (executor.h).
pub fn exec_qual_and_reset<'mcx>(
    state: &mut ExprState<'mcx>,
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
pub fn exec_project<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<SlotId> {
    // ExecProject(planstate->ps_ProjInfo). The projection is owned by the
    // PlanState; borrow it mutably (the eval mutates pi_state's scratch cells —
    // C's `ProjectionInfo *` is non-const).
    let mut proj_info = planstate
        .ps_ProjInfo
        .take()
        .expect("ExecProject: PlanState has no ps_ProjInfo");
    let slot = exec_project_info(&mut proj_info, estate);
    // restore the projection on the PlanState
    planstate.ps_ProjInfo = Some(proj_info);
    slot
}

/// `ExecProject(proj_info)` (executor.h) of an explicitly-supplied projection
/// (the MERGE per-action `mas_proj` / RETURNING / subscript projections). The C
/// `static inline` body:
/// ```c
/// econtext = projInfo->pi_exprContext;
/// state = &projInfo->pi_state;
/// slot = state->resultslot;
/// ExecClearTuple(slot);
/// ExecEvalExprNoReturnSwitchContext(state, econtext);  // runs EEOP_ASSIGN_* steps
/// slot->tts_flags &= ~TTS_FLAG_EMPTY;                  // ExecStoreVirtualTuple inline
/// slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
/// return slot;
/// ```
pub fn exec_project_info<'mcx>(
    proj_info: &mut ProjectionInfo<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<SlotId> {
    // econtext = projInfo->pi_exprContext;
    let econtext = proj_info
        .pi_exprContext
        .expect("ExecProject: ProjectionInfo has no pi_exprContext");
    // state = &projInfo->pi_state;  slot = state->resultslot;
    let slot = proj_info
        .pi_state
        .resultslot
        .expect("ExecProject: ProjectionInfo's ExprState has no resultslot");

    // ExecClearTuple(slot): clear any former contents so the slot's
    // Datum/isnull arrays are safe to use as workspace.
    backend_executor_execTuples_seams::exec_clear_tuple::call(estate, slot)?;

    // ExecEvalExprNoReturnSwitchContext(state, econtext): run the compiled
    // assign program; it writes the projected columns into the result slot's
    // tts_values/tts_isnull arrays. The owned interpreter resolves the result
    // slot from state->resultslot (the SlotId) and writes through `estate`.
    backend_executor_execExprInterp_seams::exec_eval_expr_no_return::call(
        &mut proj_info.pi_state,
        econtext,
        estate,
    )?;

    // Mark the result slot as a valid virtual tuple (inlined
    // ExecStoreVirtualTuple): clear TTS_FLAG_EMPTY and set tts_nvalid to the
    // descriptor's column count.
    let result = estate.slot_mut(slot);
    result.tts_flags &= !types_nodes::tuptable::TTS_FLAG_EMPTY;
    let natts = result
        .tts_tupleDescriptor
        .as_ref()
        .expect("ExecProject: result slot has no tuple descriptor")
        .natts;
    result.tts_nvalid = natts as i16;

    Ok(slot)
}

/// `CreateExecutorState()` (execUtils.c) — a throwaway EState.
pub fn create_executor_state<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, EStateData<'mcx>>> {
    backend_executor_execUtils_seams::create_executor_state::call(mcx)
}

/// `FreeExecutorState(estate)` (execUtils.c).
pub fn free_executor_state<'mcx>(estate: PgBox<'mcx, EStateData<'mcx>>) -> PgResult<()> {
    backend_executor_execUtils_seams::free_executor_state::call(estate)
}

/// The executor-backed constant test of `operator_predicate_proof`
/// (optimizer/util/predtest.c:1983-2020). Given the resolved btree
/// constant-comparison operator `test_op` and the two `Const`s (`pred_const`,
/// `clause_const`), build `test_op(pred_const, clause_const)` as a boolean
/// `OpExpr`, compile it, and evaluate it in a throwaway executor state, exactly
/// as the C `operator_predicate_proof` tail does:
///
/// ```c
/// estate = CreateExecutorState();
/// oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
/// test_expr = make_opclause(test_op, BOOLOID, false,
///                           (Expr *) pred_const, (Expr *) clause_const,
///                           InvalidOid, pred_collation);
/// fix_opfuncids((Node *) test_expr);
/// test_exprstate = ExecInitExpr(test_expr, NULL);
/// test_result = ExecEvalExprSwitchContext(test_exprstate,
///                                         GetPerTupleExprContext(estate),
///                                         &isNull);
/// MemoryContextSwitchTo(oldcontext);
/// FreeExecutorState(estate);
/// if (isNull) { elog(DEBUG2, "null predicate test result"); return false; }
/// return DatumGetBool(test_result);
/// ```
///
/// Returns `Some(true)`/`Some(false)` for the boolean result, or `None` for a
/// NULL result (the C "null predicate test result" DEBUG2, treated as
/// non-proof by the caller). `Err` carries any evaluation `ereport(ERROR)`.
///
/// The throwaway `EState` owns a private root memory context (the owned-model
/// equivalent of `CreateExecutorState`'s per-query context, which in C is a
/// fresh child of `CurrentMemoryContext`): the seam takes no `Mcx`, so it roots
/// its own short-lived arena and tears it down when the context drops — exactly
/// the `CreateExecutorState`/`FreeExecutorState` bracket the C performs. The
/// `MemoryContextSwitchTo(es_query_cxt)` C wraps the build/eval in is implicit
/// here: every allocation routes through `es_query_cxt` (the EState's context)
/// by construction.
pub fn eval_const_test(
    test_op: types_core::Oid,
    pred_const: &Const,
    clause_const: &Const,
    collation: types_core::Oid,
) -> PgResult<Option<bool>> {
    // estate = CreateExecutorState();  — a self-contained executor state with
    // its own per-query memory context. The seam has no ambient Mcx, so root a
    // fresh standalone context here, mirroring CreateExecutorState() allocating
    // its "ExecutorState" context under CurrentMemoryContext.
    let cx = mcx::MemoryContext::new("ExecutorState");
    let mcx = cx.mcx();
    let mut estate = EStateData::new_in(mcx);

    // test_expr = make_opclause(test_op, BOOLOID, false,
    //                           (Expr *) pred_const, (Expr *) clause_const,
    //                           InvalidOid, pred_collation);
    //
    // make_opclause consumes its operands by value; the C passes the existing
    // Const nodes (which live in the caller's context and outlive the EState),
    // so cloning them into the throwaway expression is faithful.
    let mut test_expr = backend_nodes_core::makefuncs::make_opclause(
        test_op,
        types_core::catalog::BOOLOID,
        false,
        Expr::Const(pred_const.clone()),
        Some(Expr::Const(clause_const.clone())),
        types_core::InvalidOid,
        collation,
    );

    // fix_opfuncids((Node *) test_expr);
    backend_nodes_core::nodefuncs::fix_opfuncids(&mut test_expr)?;

    // test_exprstate = ExecInitExpr(test_expr, NULL);
    let mut test_exprstate = exec_init_expr_no_parent(&test_expr, &mut estate)?;

    // test_result = ExecEvalExprSwitchContext(test_exprstate,
    //                                         GetPerTupleExprContext(estate),
    //                                         &isNull);
    let econtext =
        backend_executor_execUtils_seams::get_per_tuple_expr_context::call(&mut estate)?;
    let (test_result, is_null) =
        exec_eval_expr_switch_context(&mut test_exprstate, econtext, &mut estate)?;

    // Extract the boolean result *before* the EState (and its context) drop —
    // `test_result` borrows `es_query_cxt`. DatumGetBool(test_result) is a copy.
    let result = if is_null {
        // elog(DEBUG2, "null predicate test result"); return false;  — surfaced
        // to the caller as the "null result → non-proof" None.
        None
    } else {
        Some(test_result.as_bool())
    };

    // FreeExecutorState(estate);  — the EState and its working storage are
    // released when the compiled program, the EState, and finally the private
    // context drop (the owned-model equivalent; this seam's throwaway EState
    // holds no ExprContext shutdown callbacks, JIT context, or partition
    // directory to run down). `cx` outlives both borrowers and drops last at
    // end of scope, freeing the per-query arena.
    drop(test_exprstate);
    drop(estate);
    // `cx` drops here at end of scope (last, after both borrowers), freeing the
    // per-query arena. An explicit `drop(cx)` would move it out while the
    // invariant `'mcx` borrow is still in scope, so the natural scope-end drop
    // is what closes the bracket.

    Ok(result)
}

/// `evaluate_expr(expr, result_type, result_typmod, result_collation)`
/// (optimizer/util/clauses.c:4975) — the executor-backed const-evaluator the
/// planner's `eval_const_expressions` falls back on for the expression shapes
/// the in-crate fmgr fast path does not handle (SAOP / MinMax / Row /
/// SubscriptingRef / FieldSelect-on-Const / ConvertRowtype / ArrayCoerce /
/// estimate-mode SQLValueFunction / multidim ArrayExpr — and any all-Const
/// FuncExpr/OpExpr/NullIfExpr the fast path declines).
///
/// Faithful to C:
///
/// ```c
/// estate = CreateExecutorState();
/// oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
/// fix_opfuncids((Node *) expr);
/// exprstate = ExecInitExpr(expr, NULL);
/// const_val = ExecEvalExprSwitchContext(exprstate,
///                                       GetPerTupleExprContext(estate),
///                                       &const_is_null);
/// get_typlenbyval(result_type, &resultTypLen, &resultTypByVal);
/// MemoryContextSwitchTo(oldcontext);
/// if (!const_is_null) { ... detoast / datumCopy out of sub-context ... }
/// FreeExecutorState(estate);
/// return (Expr *) makeConst(result_type, result_typmod, result_collation,
///                           resultTypLen, const_val, const_is_null,
///                           resultTypByVal);
/// ```
///
/// The "copy result out of sub-context + forcibly detoast varlena" tail is
/// performed by `makeConst` itself in the owned model: `make_const`
/// (backend-nodes-core) detoasts a `constlen == -1` value and copies a
/// by-reference image into the backend-lifetime const-value context, yielding a
/// `Datum<'static>` that outlives the throwaway EState — exactly the C
/// `PG_DETOAST_DATUM_COPY` / `datumCopy` step. We therefore build the Const
/// *before* the EState (and its per-query context) drop, since the result Datum
/// borrows `es_query_cxt`.
///
/// **Owner:** the executor (execExpr); the seam declaration lives on
/// `backend-optimizer-util-clauses-seams` (clauses.c is the C home), installed
/// here from `init_seams`, exactly as `eval_const_test` is installed onto the
/// predtest seam crate.
pub fn evaluate_expr_fallback(
    expr: Expr<'static>,
    result_type: types_core::Oid,
    result_typmod: i32,
    result_collation: types_core::Oid,
) -> PgResult<Expr<'static>> {
    // estate = CreateExecutorState();  — a throwaway executor state with its own
    // per-query memory context (the owned-model equivalent of CreateExecutorState
    // rooting an "ExecutorState" context under CurrentMemoryContext). The seam
    // has no ambient Mcx, so root a fresh standalone context here.
    let cx = mcx::MemoryContext::new("ExecutorState");
    let mcx = cx.mcx();
    let mut estate = EStateData::new_in(mcx);

    // The seam hands the expression in at the planner's notional `'static`; clone
    // it into this throwaway EState's per-query arena (`mcx`) to compile it, since
    // `exec_init_expr_no_parent` threads the node tree as `'mcx` (Expr is invariant).
    let mut expr = expr.clone_in(mcx)?;
    // fix_opfuncids((Node *) expr);  — make sure any opfuncids are filled in.
    backend_nodes_core::nodefuncs::fix_opfuncids(&mut expr)?;

    // exprstate = ExecInitExpr(expr, NULL);  — prepare for execution. (We can't
    // use ExecPrepareExpr because it'd recursively invoke eval_const_expressions.)
    let mut exprstate = exec_init_expr_no_parent(&expr, &mut estate)?;

    // const_val = ExecEvalExprSwitchContext(exprstate,
    //                                       GetPerTupleExprContext(estate),
    //                                       &const_is_null);
    let econtext =
        backend_executor_execUtils_seams::get_per_tuple_expr_context::call(&mut estate)?;
    let (const_val, const_is_null) =
        exec_eval_expr_switch_context(&mut exprstate, econtext, &mut estate)?;

    // get_typlenbyval(result_type, &resultTypLen, &resultTypByVal);
    let (result_typlen, result_typbyval) =
        backend_utils_cache_lsyscache_seams::get_typlenbyval::call(result_type)?;

    // return (Expr *) makeConst(...);
    //
    // make_const performs C's "copy result out of sub-context + forcibly detoast
    // varlena" tail: a `constlen == -1` value is detoasted, and a by-reference
    // image is copied into the backend-lifetime const-value context (yielding a
    // `Datum<'static>` that survives the EState teardown below). Build it *before*
    // dropping the EState, since `const_val` borrows `es_query_cxt`.
    let result = backend_nodes_core::makefuncs::make_const(
        // make_const re-homes any by-reference image into its own
        // backend-lifetime const-value context (`const_value_mcx`) and leaves
        // by-value/cstring values as `'static`, so the returned `Const` (which
        // carries `Datum<'static>`) outlives this throwaway EState. The Mcx
        // handed in is only the staging arena for the (rare) detoast scratch,
        // so the EState's own per-query context is correct and alive here.
        estate.es_query_cxt,
        result_type,
        result_typmod,
        result_collation,
        result_typlen as i32,
        const_val,
        const_is_null,
        result_typbyval,
    )?;

    // FreeExecutorState(estate);  — release the compiled program, the EState, and
    // finally the private per-query context (the owned-model equivalent; this
    // throwaway EState holds no ExprContext shutdown callbacks / JIT context /
    // partition directory to run down). `cx` drops last at end of scope, after
    // both borrowers, freeing the per-query arena.
    drop(exprstate);
    drop(estate);

    // `make_const` re-homed the by-reference image into the backend-lifetime
    // const-value context (the `Const` carries `Datum<'static>`), so the result
    // outlives this throwaway EState; erase to the seam's `'static` return.
    Ok(Expr::Const(result).erase_lifetime())
}

thread_local! {
    /// Backend-lifetime context for prepared-parameter by-reference datum
    /// images. `EvaluateParams` evaluates each `$n` value into the EState's
    /// per-tuple context, then deep-copies it into the param list's long-lived
    /// storage (C palloc's `paramLI` in `CreateExecutorState`'s context, which
    /// outlives the per-tuple context the value is computed in). The owned value
    /// `ParamListInfoData` carries `Datum<'static>`, so the copy target is this
    /// leaked, never-reset context.
    static PREPARED_PARAM_CONTEXT: &'static mcx::MemoryContext =
        Box::leak(Box::new(mcx::MemoryContext::new("PreparedParams")));
}

/// `EvaluateParams` leaf (prepare.c): for the `i`-th prepared `ExprState`, set
/// `paramLI->params[i]`: `ptype = param_types[i]`, `pflags = PARAM_FLAG_CONST`,
/// `value = ExecEvalExprSwitchContext(n, GetPerTupleExprContext(estate),
/// &prm->isnull)`.
pub fn eval_exec_param_into_list<'mcx>(
    param_li: &mut types_nodes::params::ParamListInfoData<'static>,
    exprstate: &mut ExprState<'mcx>,
    param_index: i32,
    ptype: types_core::Oid,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // prm->value = ExecEvalExprSwitchContext(n, GetPerTupleExprContext(estate),
    //                                        &prm->isnull);
    //
    // C evaluates the compiled `ExprState` in place — the interpreter threads
    // its scratch result cells through the *same* state built by
    // `ExecPrepareExprList`. The state must NOT be cloned: `ExprState::clone`
    // deliberately drops the compiled `steps` program (it only carries the
    // lightweight handle fields), so a cloned state has no instructions and
    // `CheckExprStillValid`/`ExecInterpExpr` would panic ("steps not built").
    let econtext =
        backend_executor_execUtils_seams::get_per_tuple_expr_context::call(estate)?;
    let (value, isnull) = exec_eval_expr_switch_context(exprstate, econtext, estate)?;

    // Deep-copy the computed value out of the per-tuple context into the param
    // list's backend-lifetime storage (`Datum<'static>`). For a by-value datum
    // this is a word copy; for by-reference it re-allocs the bytes.
    let owned_value: types_nodes::params::Datum<'static> =
        PREPARED_PARAM_CONTEXT.with(|c| value.clone_in(c.mcx()))?;

    let prm = &mut param_li.params[param_index as usize];
    prm.ptype = ptype;
    prm.pflags = types_nodes::params::PARAM_FLAG_CONST;
    prm.value = owned_value;
    prm.isnull = isnull;

    Ok(())
}

// ===========================================================================
// HashJoin convenience wrappers
// ===========================================================================

/// `ExecQual` for a hash-join node's `js.joinqual` / `js.ps.qual`.
///
/// C (nodeHashjoin.c:554, :558): `ExecQual(joinqual, econtext)` /
/// `ExecQual(otherqual, econtext)` where `econtext = node->js.ps.ps_ExprContext`,
/// `joinqual = node->js.joinqual`, `otherqual = node->js.ps.qual`. A `NULL`
/// `ExprState` is treated as always-true by the caller, which gates the call on
/// `*_present`; this wrapper is only invoked when the selected state is non-NULL.
pub fn exec_hashjoin_qual<'mcx>(
    node: &mut HashJoinState<'mcx>,
    joinqual: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let econtext = node
        .js
        .ps
        .ps_ExprContext
        .expect("ExecHashJoin qual: PlanState has no ps_ExprContext");
    let state = if joinqual {
        node.js
            .joinqual
            .as_deref_mut()
            .expect("ExecHashJoin: js.joinqual is NULL but caller invoked ExecQual on it")
    } else {
        node.js
            .ps
            .qual
            .as_deref_mut()
            .expect("ExecHashJoin: js.ps.qual is NULL but caller invoked ExecQual on it")
    };
    exec_qual(state, econtext, estate)
}

/// `ExecInitQual` of one of a hash-join node's qual lists.
///
/// C (nodeHashjoin.c:916-921):
/// ```c
/// hjstate->js.ps.qual  = ExecInitQual(node->join.plan.qual, hjstate);
/// hjstate->js.joinqual = ExecInitQual(node->join.joinqual,  hjstate);
/// hjstate->hashclauses = ExecInitQual(node->hashclauses,    hjstate);
/// ```
/// The source list lives on the read-only plan node (`js.ps.plan`, a borrowed
/// `&'mcx Node::HashJoin`); the compiled `ExprState` is stored on the matching
/// state field.
pub fn exec_init_hashjoin_qual<'mcx>(
    node: &mut HashJoinState<'mcx>,
    kind: HashJoinQualKind,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    // The plan node is a borrowed `&'mcx Node`; copy the reference out so the
    // borrow is tied to `'mcx`, not to `node`, freeing `&mut node.js.ps` below.
    let plan = node
        .js
        .ps
        .plan
        .expect("ExecInitHashJoin: js.ps.plan is NULL");
    let hj = match plan.as_hashjoin() {
        Some(h) => h,
        None => panic!("ExecInitHashJoin: js.ps.plan is not a HashJoin node: {plan:?}"),
    };

    match kind {
        HashJoinQualKind::Qual => {
            let qual = hj.join.plan.qual.as_deref();
            let compiled = exec_init_qual(qual, &mut node.js.ps, estate)?;
            node.js.ps.qual = compiled;
        }
        HashJoinQualKind::JoinQual => {
            let qual = hj.join.joinqual.as_deref();
            let compiled = exec_init_qual(qual, &mut node.js.ps, estate)?;
            node.js.joinqual = compiled;
        }
        HashJoinQualKind::HashClauses => {
            // `node->hashclauses` is a heterogeneous expression `List`; each
            // element is an `OpExpr` (`Node::Expr`). Materialize the `Expr`
            // slice ExecInitQual expects.
            let exprs: PgVec<'mcx, Expr> = match hj.hashclauses.as_deref() {
                None => mcx::vec_with_capacity_in(mcx, 0)?,
                Some(list) => {
                    let mut out = mcx::vec_with_capacity_in(mcx, list.len())?;
                    for n in list.iter() {
                        if let Some(e) = n.as_expr() {
                            // A hashclause `OpExpr` may carry context-allocated
                            // children (a SubPlan when the key is `a = (SELECT
                            // ...)`); deep-copy through `Expr::clone_in` rather
                            // than the panicking derived `.clone()`.
                            out.push(e.clone_in(mcx)?);
                        } else {
                            panic!(
                                "ExecInitHashJoin: hashclauses element is not an \
                                 expression node: {n:?}"
                            );
                        }
                    }
                    out
                }
            };
            let slice = if exprs.is_empty() {
                None
            } else {
                Some(exprs.as_slice())
            };
            let compiled = exec_init_qual(slice, &mut node.js.ps, estate)?;
            node.hashclauses = compiled;
        }
    }
    Ok(())
}

/// `ExecProject(node->js.ps.ps_ProjInfo)` for a hash-join node
/// (nodeHashjoin.c:591/:618/:647).
pub fn exec_hashjoin_project<'mcx>(
    node: &mut HashJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<SlotId> {
    exec_project(&mut node.js.ps, estate)
}

/// `DatumGetUInt32(ExecEvalExprSwitchContext(hj_OuterHash, econtext, isNull))`
/// (nodeHashjoin.c:1012/:1085) — evaluate the outer tuple's hash-key expression
/// against the node's per-tuple expression context.
pub fn eval_outer_hash<'mcx>(
    node: &mut HashJoinState<'mcx>,
    isnull: &mut bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<u32> {
    let econtext = node
        .js
        .ps
        .ps_ExprContext
        .expect("ExecHashGetHashValue: PlanState has no ps_ExprContext");
    let state = node
        .hj_OuterHash
        .as_deref_mut()
        .expect("ExecHashGetHashValue: hj_OuterHash ExprState is NULL");
    let (datum, null) = exec_eval_expr_switch_context(state, econtext, estate)?;
    *isnull = null;
    Ok(datum.as_u32())
}
