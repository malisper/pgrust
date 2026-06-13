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
//! types model a step's output as `ExprEvalStep::{resvalue,resnull}:
//! Option<PgBox<Datum/bool>>`, where `None` denotes "the ExprState's own
//! `resvalue`/`resnull`" — the `&state->resvalue` case. A separate output cell
//! (e.g. a function call's `fcinfo->args[i]`, a CASE test slot) cannot be
//! aliased by an owned box, so any node whose compilation needs a distinct
//! output target is routed to a loud panic until the keystone grows a result
//! arena: the modeled subset (Var / Const / CurrentOfExpr) writes only into
//! the single shared result cell, matching C exactly.

use mcx::{Mcx, PgBox, PgVec};
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::execexpr::{
    ExprEvalOp, ExprEvalStep, ExprEvalStepData, ExprSetupInfo, ExprState, ProjectionInfo,
    VarReturningType, EEO_FLAG_IS_QUAL,
};
use types_nodes::execnodes::PlanStateData;
use types_nodes::nodehashjoin::HashJoinState;
use types_nodes::parsestmt::ParamListInfoHandle;
use types_nodes::primnodes::Expr;
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
// makeNode(ExprState) + ExprEvalPushStep + ExecReadyExpr (spine primitives)
// ===========================================================================

/// `makeNode(ExprState)` (execExpr.c) — a fresh, empty `ExprState`.
fn make_expr_state<'mcx>() -> ExprState<'mcx> {
    ExprState::default()
}

/// `EEOP_DONE_RETURN` scratch step (the trailing step appended by every
/// expression compile that yields a value).
fn done_return_step<'mcx>() -> ExprEvalStep<'mcx> {
    ExprEvalStep {
        opcode: ExprEvalOp::EEOP_DONE_RETURN,
        resvalue: None,
        resnull: None,
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
/// attnum referenced from each input slot (and the MULTIEXPR subplans). Only
/// `Var` carries an attnum to record; the walk over sub-expressions of the
/// other modeled node kinds is performed by the caller's recursion. Aggref /
/// WindowFunc / GroupingFunc arguments are deliberately not descended into (C),
/// but those node kinds are not modeled here, so the point is moot.
fn expr_setup_walker(node: &Expr, info: &mut ExprSetupInfo) {
    if let Expr::Var(variable) = node {
        let attnum = variable.varattno;
        match variable.varno {
            INNER_VAR => info.last_attnums.last_inner = info.last_attnums.last_inner.max(attnum),
            OUTER_VAR => info.last_attnums.last_outer = info.last_attnums.last_outer.max(attnum),
            // INDEX_VAR and real-relation varnos: scan slot. (VAR_RETURNING_OLD
            // / _NEW are not modeled by the keystone Var; default = scan.)
            _ => info.last_attnums.last_scan = info.last_attnums.last_scan.max(attnum),
        }
    }
    // Const / OpExpr / ScalarArrayOpExpr / CurrentOfExpr carry no Vars in the
    // modeled subset (OpExpr/ScalarArrayOpExpr args would, but those node
    // kinds are routed to a panic in ExecInitExprRec). No further descent.
}

/// `ExecComputeSlotInfo(state, op)` (execExpr.c) — decide whether an
/// `EEOP_*_FETCHSOME` deform step is required and, if the slot type is fixed,
/// pin its descriptor/ops. With no `parent` (the only shape this family
/// compiles standalone) the slot is never fixed, so the step is always
/// required and stays in its non-fixed form — exactly the C `!parent` branch.
///
/// Returns `true` if the deforming step should be kept.
fn exec_compute_slot_info<'mcx>(state: &ExprState<'mcx>, op: &mut ExprEvalStep<'mcx>) -> bool {
    debug_assert!(matches!(
        op.opcode,
        ExprEvalOp::EEOP_INNER_FETCHSOME
            | ExprEvalOp::EEOP_OUTER_FETCHSOME
            | ExprEvalOp::EEOP_SCAN_FETCHSOME
            | ExprEvalOp::EEOP_OLD_FETCHSOME
            | ExprEvalOp::EEOP_NEW_FETCHSOME
    ));

    // The parent PlanState's slot-ops introspection (ExecGetResultSlotOps /
    // ExecGetResultType / inneropsfixed / scanops...) is owned by execUtils /
    // execProcnode; for a parent-bearing compile that machinery must be routed
    // through those seams. Until then we mirror C's `!parent` branch: nothing
    // is fixed, so leave the fetch step non-fixed and keep it.
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
    // Non-fixed (or non-virtual) slots always need the deform step.
    true
}

/// `ExecPushExprSetupSteps(state, info)` (execExpr.c) — emit the leading
/// `EEOP_*_FETCHSOME` deform steps for each input slot referenced by the
/// expression, then the MULTIEXPR-subplan steps. (MULTIEXPR subplans are not
/// modeled here; the keystone `ExprSetupInfo::multiexpr_subplans` is a count.)
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
                resvalue: None,
                resnull: None,
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
             ExecInitSubPlanExpr + the SubPlan node keystone)"
        );
    }
    Ok(())
}

/// `ExecCreateExprSetupSteps(state, node)` (execExpr.c) — prescan a single
/// expression to find required setup, then emit the setup steps.
fn exec_create_expr_setup_steps<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    node: &Expr,
) -> PgResult<()> {
    let mut info = ExprSetupInfo::default();
    expr_setup_walker(node, &mut info);
    exec_push_expr_setup_steps(mcx, state, &info)
}

/// `ExecCreateExprSetupSteps(state, (Node *) list)` over a qual list — prescan
/// each member, accumulating into one `ExprSetupInfo`, then emit once.
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

// ===========================================================================
// ExecInitExprRec — the opcode-emission switch
// ===========================================================================

/// `ExecInitExprRec(node, state, resv, resnull)` (execExpr.c) — append the
/// steps that evaluate `node`, leaving the result in the caller's output cell.
///
/// In this owned model the only output cell a step can name is the
/// `ExprState`'s own `resvalue`/`resnull` (`resv == NULL`/`None`). The modeled
/// node kinds (`Var`, `Const`, `CurrentOfExpr`) each emit a single step writing
/// there, matching C with `resv = &state->resvalue`. Node kinds that need a
/// distinct output cell or sub-node recursion (`OpExpr`, `ScalarArrayOpExpr`,
/// and all the unmodeled primnodes) are routed to a loud panic — their owners
/// (execExpr_func_subscript et al.) and the keystone result arena have not
/// landed.
fn exec_init_expr_rec<'mcx>(
    mcx: Mcx<'mcx>,
    node: &Expr,
    state: &mut ExprState<'mcx>,
) -> PgResult<()> {
    // C: check_stack_depth(); — guard handled by the host stack here.
    // Step's output location is always the caller's cell; in the owned model
    // that is the ExprState's own resvalue/resnull (resv/resnull == None).
    match node {
        Expr::Var(variable) => {
            // cases ordered as in enum NodeTag — T_Var first.
            let mut scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_SCAN_VAR, // set below
                resvalue: None,
                resnull: None,
                d: ExprEvalStepData::Var {
                    attnum: 0,
                    vartype: variable.vartype,
                    varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
                },
            };

            if variable.varattno == types_core::InvalidAttrNumber {
                // whole-row Var — owned by execExpr_func_subscript
                // (ExecInitWholeRowVar).
                panic!(
                    "execExpr-core: whole-row Var compilation not ported (owner: \
                     execExpr_func_subscript::ExecInitWholeRowVar)"
                );
            } else if variable.varattno <= 0 {
                // system column
                if let ExprEvalStepData::Var { attnum, .. } = &mut scratch.d {
                    *attnum = variable.varattno as i32;
                }
                scratch.opcode = match variable.varno {
                    INNER_VAR => ExprEvalOp::EEOP_INNER_SYSVAR,
                    OUTER_VAR => ExprEvalOp::EEOP_OUTER_SYSVAR,
                    // INDEX_VAR + real relations: scan sysvar (VAR_RETURNING
                    // OLD/NEW not modeled by the keystone Var).
                    _ => ExprEvalOp::EEOP_SCAN_SYSVAR,
                };
            } else {
                // regular user column
                if let ExprEvalStepData::Var { attnum, .. } = &mut scratch.d {
                    *attnum = variable.varattno as i32 - 1;
                }
                scratch.opcode = match variable.varno {
                    INNER_VAR => ExprEvalOp::EEOP_INNER_VAR,
                    OUTER_VAR => ExprEvalOp::EEOP_OUTER_VAR,
                    _ => ExprEvalOp::EEOP_SCAN_VAR,
                };
            }

            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        Expr::Const(con) => {
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_CONST,
                resvalue: None,
                resnull: None,
                d: ExprEvalStepData::ConstVal {
                    value: con.constvalue,
                    isnull: con.constisnull,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        Expr::CurrentOfExpr(_) => {
            // T_CurrentOfExpr: a single EEOP_CURRENTOFEXPR step with no operands.
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_CURRENTOFEXPR,
                resvalue: None,
                resnull: None,
                d: ExprEvalStepData::NoPayload, // no union payload in C
            };
            expr_eval_push_step(mcx, state, scratch)?;
            Ok(())
        }

        // OpExpr / DistinctExpr / NullIfExpr emit EEOP_FUNCEXPR_* after
        // recursing each argument into a *distinct* fcinfo->args[i] cell — a
        // result target the owned keystone cannot alias yet, and ExecInitFunc
        // belongs to execExpr_func_subscript.
        Expr::OpExpr(_) => panic!(
            "execExpr-core: OpExpr compilation not ported (needs ExecInitFunc + per-arg \
             fcinfo result cells; owner: execExpr_func_subscript)"
        ),
        Expr::ScalarArrayOpExpr(_) => panic!(
            "execExpr-core: ScalarArrayOpExpr compilation not ported (owner: \
             execExpr_func_subscript)"
        ),

        // Every other primnode (Param, Aggref, WindowFunc, FuncExpr, BoolExpr,
        // SubPlan, CaseExpr, ArrayExpr, RowExpr, CoalesceExpr, MinMaxExpr,
        // NullTest, BooleanTest, CoerceToDomain, FieldSelect, FieldStore,
        // SubscriptingRef, JsonExpr, …) is not yet modeled by the keystone Expr
        // enum (#[non_exhaustive]). Compiling one would require its node shape
        // and (mostly) a distinct result cell; loud-panic per "mirror PG and
        // panic" until the keystone and the owning family modules land.
        _ => panic!(
            "execExpr-core: ExecInitExprRec — unmodeled Expr node kind not yet ported \
             (keystone Expr enum is #[non_exhaustive]; only Var/Const/CurrentOfExpr compile here)"
        ),
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
    // NULL `node` → NULL ExprState is handled by the caller passing through
    // Option; the seam takes a non-optional &Expr (a present node).
    let _ = parent;
    let mcx = estate.es_query_cxt;

    let mut state = make_expr_state();
    // state->expr / state->parent / state->ext_params: the owned model does not
    // thread the parent back-pointer here (callers lend it explicitly); expr is
    // a debug-only back-link, left None.
    state.ext_params = 0;

    exec_create_expr_setup_steps(mcx, &mut state, node)?;
    exec_init_expr_rec(mcx, node, &mut state)?;
    expr_eval_push_step(mcx, &mut state, done_return_step())?;
    exec_ready_expr(&mut state)?;

    mcx::alloc_in(mcx, state)
}

/// `ExecInitExprWithParams(node, ext_params)` (execExpr.c) — compile a
/// standalone expression with no parent PlanState, only external params.
pub fn exec_init_expr_with_params<'mcx>(
    node: &Expr,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let _ = econtext;
    let mcx = estate.es_query_cxt;

    let mut state = make_expr_state();
    state.ext_params = 0; // ecxt_param_list_info threaded by the param owner

    exec_create_expr_setup_steps(mcx, &mut state, node)?;
    exec_init_expr_rec(mcx, node, &mut state)?;
    expr_eval_push_step(mcx, &mut state, done_return_step())?;
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
    // short-circuit for empty restriction list → NULL ExprState
    let qual = match qual {
        None => return Ok(None),
        Some(q) if q.is_empty() => return Ok(None),
        Some(q) => q,
    };
    let mcx = estate.es_query_cxt;

    let mut state = make_expr_state();
    state.ext_params = 0;
    // mark expression as to be used with ExecQual()
    state.flags = EEO_FLAG_IS_QUAL;

    exec_create_expr_setup_steps_list(mcx, &mut state, qual)?;

    // Compile each qual clause, each followed by an EEOP_QUAL test that
    // short-circuits to the end on false/null. Record the QUAL step indices to
    // backpatch their jumpdone to the final step.
    let mut adjust_jumps: PgVec<'mcx, usize> = mcx::vec_with_capacity_in(mcx, qual.len())?;
    for node in qual {
        // first evaluate expression (into state->resvalue/resnull)
        exec_init_expr_rec(mcx, node, &mut state)?;
        // then emit EEOP_QUAL to detect false-or-null
        let scratch = ExprEvalStep {
            opcode: ExprEvalOp::EEOP_QUAL,
            resvalue: None,
            resnull: None,
            d: ExprEvalStepData::QualExpr { jumpdone: -1 },
        };
        expr_eval_push_step(mcx, &mut state, scratch)?;
        adjust_jumps.push((state.steps_len - 1) as usize);
    }

    // adjust jump targets → the final step index (steps_len after DONE push is
    // the index just past the last QUAL; C sets jumpdone = steps_len here,
    // before pushing DONE, so it lands on the DONE step).
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

    // last qual yielded TRUE into the result cell — just emit DONE_RETURN.
    expr_eval_push_step(mcx, &mut state, done_return_step())?;
    exec_ready_expr(&mut state)?;

    Ok(Some(mcx::alloc_in(mcx, state)?))
}

/// `ExecInitExprList(nodes, parent)` (execExpr.c) — compile a list of
/// expressions into a positional list of [`ExprState`]s.
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
            // NULL Expr → NULL ExprState (positional correspondence preserved).
            None => result.push(None),
            Some(node) => {
                let state = exec_init_expr(node, parent, estate)?;
                // unbox into the positional list (the C list holds ExprState*).
                result.push(Some(PgBox::into_inner(state)));
            }
        }
    }
    Ok(result)
}

/// `ExecPrepareExpr(node, estate)` (execExpr.c) — compile a single expression
/// for use outside a normal executor node (parent = NULL).
pub fn exec_prepare_expr<'mcx>(
    node: &Expr,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    // C switches to estate->es_query_cxt (we always allocate there) and runs
    // expression_planner(node) first. expression_planner (the planner's
    // const-folding / SQL-function-inlining pass) is owned by the optimizer;
    // route through its seam. Until then, the planner pass would be skipped —
    // but skipping it would silently diverge, so panic loudly.
    panic!(
        "execExpr-core: ExecPrepareExpr needs expression_planner (optimizer/planner.c, \
         unported); compile the already-planned expression via ExecInitExpr instead"
    );
}

/// `ExecPrepareExprList(exprList, estate)` (execExpr.c) — compile a list of
/// expressions into a parallel list of [`ExprState`]s.
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

/// `ExecBuildProjectionInfo(...)` (execExpr.c) — build the compiled projection
/// program for a node's target list.
pub fn exec_build_projection_info<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    input_desc: Option<&TupleDescData<'_>>,
) -> PgResult<PgBox<'mcx, ProjectionInfo<'mcx>>> {
    // The target list, the node's ps_ExprContext and ps_ResultTupleSlot, and
    // the slot-ops/descriptor introspection all live behind execUtils /
    // execProcnode for a parent-bearing build, and each tlist entry recurses
    // through ExecInitExprRec into the shared result cell (plus EEOP_ASSIGN_*).
    // The keystone TargetEntry list off `planstate->plan` and the result-slot
    // wiring are not threaded here yet; emitting an approximate program would
    // silently diverge, so panic loudly until the owned projection wiring lands.
    let _ = (planstate, estate, input_desc);
    panic!(
        "execExpr-core: ExecBuildProjectionInfo needs the owned target-list / result-slot \
         wiring (planstate->plan->targetlist, ps_ResultTupleSlot via execUtils/execProcnode)"
    );
}

/// `ExecBuildUpdateProjection(...)` (execExpr.c) — build the UPDATE "new tuple"
/// projection for a result relation.
pub fn exec_build_update_projection<'mcx>(
    mtstate: &mut types_nodes::ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    update_colnos: &[i32],
) -> PgResult<()> {
    let _ = (mtstate, estate, result_rel_info, update_colnos);
    panic!(
        "execExpr-core: ExecBuildUpdateProjection needs the owned ResultRelInfo / target-list \
         wiring (ri_projectNew, ri_RootResultRelInfo) not yet threaded"
    );
}

// ===========================================================================
// Evaluation entry points — dispatched to the interpreter (execExprInterp)
// ===========================================================================

/// `ExecEvalExprSwitchContext(state, econtext, &isnull)` (executor.h) — run a
/// compiled [`ExprState`] in `econtext`'s per-tuple memory.
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
    // The interpreter produces a Datum whose pointer word is an ItemPointer;
    // dereferencing it must happen where the value was produced (the owned model
    // cannot reinterpret a Datum word). The interpreter exposes only the Datum
    // eval; an ItemPointer-yielding variant is owned by execExprInterp.
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
    // The result is an ordinary array Datum; the same dispatch as the scalar
    // eval suffices, the caller deconstructs the array.
    backend_executor_execExprInterp_seams::exec_eval_expr_switch_context::call(
        state, econtext, estate,
    )
}

/// `ExecQual(state, econtext)` (executor.h) — evaluate a boolean qual program.
pub fn exec_qual<'mcx>(
    state: &ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // C: ret = ExecEvalExprSwitchContext(state, econtext, &isnull);
    //    return !isnull && DatumGetBool(ret);
    // (The compiled EEOP_QUAL program already forces a NULL result to FALSE, so
    // the interpreter returns the boolean directly; we still apply the NULL→
    // false rule for safety, matching the executor.h inline.)
    let (ret, isnull) = exec_eval_expr_switch_context(state, econtext, estate)?;
    Ok(!isnull && ret.as_bool())
}

/// `ExecQualAndReset(state, econtext)` (executor.h) — `ExecQual` then
/// `ResetExprContext(econtext)`.
pub fn exec_qual_and_reset<'mcx>(
    state: &ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let ret = exec_qual(state, econtext, estate)?;
    // ResetExprContext(econtext) — owned by execUtils; route through its seam.
    backend_executor_execUtils_seams::reset_expr_context::call(estate, econtext)?;
    Ok(ret)
}

/// `ExecProject(projInfo)` (executor.h) — form a node's projected result tuple.
pub fn exec_project<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<SlotId> {
    // ExecProject runs ps_ProjInfo->pi_state into ps_ProjInfo's result slot via
    // the interpreter; reading ps_ProjInfo off planstate and wiring the result
    // slot back is the owned-projection plumbing that ExecBuildProjectionInfo
    // would have set up. Routed to a panic for the same reason as the builder.
    let _ = (planstate, estate);
    panic!(
        "execExpr-core: ExecProject needs the owned ProjectionInfo / result-slot wiring \
         (ps_ProjInfo) and the interpreter projection dispatch"
    );
}

/// `ExecProject(proj_info)` (executor.h) of an explicitly-supplied projection.
pub fn exec_project_info<'mcx>(
    proj_info: &ProjectionInfo<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<SlotId> {
    let _ = (proj_info, estate);
    panic!(
        "execExpr-core: ExecProject of an explicit ProjectionInfo needs the interpreter \
         projection dispatch (execExprInterp) and the result-slot id"
    );
}

/// `CreateExecutorState()` (execUtils.c) — a throwaway EState for evaluating
/// parameter expressions. (execUtils owns the canonical one; execExpr exposes a
/// seam alias for the PREPARE/EXECUTE drivers that compile params.)
pub fn create_executor_state<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, EStateData<'mcx>>> {
    // CreateExecutorState is execUtils.c's canonical function; execExpr exposes
    // this only as a seam alias for the PREPARE/EXECUTE drivers. execUtils does
    // not yet expose it through a seam crate, so route loudly until it does.
    let _ = mcx;
    panic!(
        "execExpr-core: CreateExecutorState alias needs the execUtils seam \
         (backend-executor-execUtils owns CreateExecutorState; no seam exported yet)"
    );
}

/// `FreeExecutorState(estate)` (execUtils.c) — release the throwaway EState.
pub fn free_executor_state<'mcx>(estate: PgBox<'mcx, EStateData<'mcx>>) -> PgResult<()> {
    let _ = estate;
    panic!(
        "execExpr-core: FreeExecutorState alias needs the execUtils seam \
         (backend-executor-execUtils owns FreeExecutorState; no seam exported yet)"
    );
}

/// `EvaluateParams` leaf (prepare.c) — evaluate the `i`-th prepared expression
/// into `paramLI->params[i]` as a `PARAM_FLAG_CONST` value.
pub fn eval_exec_param_into_list<'mcx>(
    param_li: ParamListInfoHandle,
    exprstate: &ExprState<'mcx>,
    param_index: i32,
    ptype: types_core::Oid,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // value = ExecEvalExprSwitchContext(n, GetPerTupleExprContext(estate),
    //                                   &prm->isnull); then set
    // params[param_index] = { ptype, PARAM_FLAG_CONST, value, isnull }.
    //
    // GetPerTupleExprContext (executor.h) and the per-slot write into the
    // opaque ParamListInfo are both owned elsewhere (execUtils for the econtext
    // id; the params unit for the slot write), and neither is exposed through a
    // seam yet. Route loudly rather than evaluate against a fabricated context.
    let _ = (param_li, exprstate, param_index, ptype, estate);
    panic!(
        "execExpr-core: EvaluateParams leaf needs GetPerTupleExprContext (execUtils) and the \
         ParamListInfo slot-write (params unit); no seams exported yet"
    );
}

// ===========================================================================
// HashJoin convenience wrappers (ExecInitQual / ExecQual / ExecProject over a
// HashJoinState) — read the node's plan lists / projection and delegate.
// ===========================================================================

/// `ExecQual` for a hash-join node's `js.joinqual` / `js.ps.qual`.
pub fn exec_hashjoin_qual<'mcx>(
    node: &mut HashJoinState<'mcx>,
    joinqual: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // econtext = node->js.ps.ps_ExprContext; state = joinqual ? js.joinqual :
    // js.ps.qual; then ExecQual(state, econtext) (NULL state ⇒ always-true).
    // Reading the compiled qual ExprState and the econtext id off the owned
    // HashJoinState needs the node-field accessors threaded by nodeHashjoin.
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
    // Reading the source list off node->plan and storing the compiled state
    // back onto the matching field needs the owned HashJoin plan-node accessors
    // and the &mut-node/&list split; the owner (nodeHashjoin) threads them.
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
    // DatumGetUInt32(ExecEvalExprSwitchContext(node->hj_OuterHash,
    //   node->js.ps.ps_ExprContext, isnull)). Reading hj_OuterHash and the
    // econtext id off the owned HashJoinState needs nodeHashjoin's accessors.
    let _ = (node, isnull, estate);
    panic!(
        "execExpr-core: evaluating the HashJoin outer hash needs the owned HashJoinState field \
         accessors (hj_OuterHash / js.ps.ps_ExprContext) threaded from nodeHashjoin"
    );
}
