//! `execExpr-domain-agg` family — domain coercion, aggregate transition, and
//! grouping/hash equality program builders.
//!
//! Owns `ExecInitCoerceToDomain`, `ExecBuildAggTrans` / `ExecBuildAggTransCall`,
//! `ExecBuildGroupingEqual`, `ExecBuildParamSetEqual`,
//! `ExecBuildHash32FromAttrs` / `ExecBuildHash32Expr`. The hashed-subplan
//! init path (`classify_testexpr` / `resolve_combining_op` /
//! `build_hash_projections_and_exprs`) is built on the grouping-equal + hash
//! builders, so its seams land here.

use ::mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use ::types_core::catalog::PROCEDURE_RELATION_ID;
use ::types_core::fmgr::FmgrInfo;
use ::types_core::{AttrNumber, Oid};
// The canonical unified value type (Datum-unification keystone) — what
// `ExprEvalStepData::HashDatumInitValue { init_value }` carries.
use ::types_tuple::heaptuple::Datum;
use ::types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::nodes::execexpr::{
    ExprEvalOp, ExprEvalStep, ExprEvalStepData, ExprState, LastAttnumInfo, ResultCell,
    ResultCellId, VarReturningType, EEO_FLAG_IS_QUAL, STATE_RESULT_CELL,
};
use ::nodes::execexpr::SubPlanState;
use ::nodes::executor::TupleSlotKind;
use ::nodes::nodeagg::{do_aggsplit_combine, AggStrategy, Aggref};
use ::nodeAgg::AggStateData;
use ::nodes::parsenodes::OBJECT_FUNCTION;
use ::nodes::primnodes::{etag, Expr, OpExpr, AND_EXPR};
use ::nodes::EStateData;
use ::types_tuple::heaptuple::TupleDescData;

/// `#define INNER_VAR (-1)` (primnodes.h special varnos) — local mirror (the
/// core constant is module-private); used by the agg setup walker.
const INNER_VAR: i32 = -1;
/// `#define OUTER_VAR (-2)`.
const OUTER_VAR: i32 = -2;

use crate::execExpr_core as core;
use aclchk_seams as aclchk;
use ::cache::typcache::{DOM_CONSTRAINT_CHECK, DOM_CONSTRAINT_NOTNULL};
use objectaccess_seams as objectaccess;
use ::execExpr_seams::{CombiningOpInfo, CombiningTestExpr};
use lsyscache_seams as lsyscache;
use fmgr_seams as fmgr_seam;
use miscinit_seams as miscinit;
use ::types_acl::{ACLCHECK_OK, ACL_EXECUTE};

// ===========================================================================
// Spine primitives — local mirrors of the `execExpr_core` arena helpers.
//
// `ExprEvalPushStep` is exported by `execExpr_core`, but the result-cell arena
// helpers (`ensure_result_arena` / `new_result_cell`), the deform-step slot
// classifier (`ExecComputeSlotInfo`), and `ExecReadyExpr` are private to that
// module. This family emits the same step programs, so it re-states those small
// primitives here (their bodies are line-for-line the `execExpr_core` ones).
// The giant `ExecInitExprRec` opcode-emission dispatch is NOT restated — it is
// core-owned and ~600 lines; the builders that recurse into arbitrary
// sub-expressions (`ExecBuildAggTrans` / `ExecBuildHash32Expr` /
// `ExecInitCoerceToDomain`) name that core surface at the genuine call site.
// ===========================================================================

/// `makeNode(ExprState)` + arena allocation of the well-known
/// [`STATE_RESULT_CELL`] (the C `&state->resvalue` target).
fn make_expr_state<'mcx>(mcx: Mcx<'mcx>) -> PgResult<ExprState<'mcx>> {
    let mut state = ExprState::default();
    ensure_result_arena(mcx, &mut state)?;
    Ok(state)
}

/// `ensure_result_arena` (mirror of `execExpr_core::ensure_result_arena`) —
/// allocate the arena + cell 0 (`STATE_RESULT_CELL`) if not yet present.
fn ensure_result_arena<'mcx>(mcx: Mcx<'mcx>, state: &mut ExprState<'mcx>) -> PgResult<()> {
    if state.result_cells.cells.is_none() {
        let mut cells = ::mcx::vec_with_capacity_in(mcx, 1)?;
        cells.push(ResultCell::default());
        state.result_cells.cells = Some(cells);
    }
    Ok(())
}

/// `palloc(sizeof(Datum))` of a dedicated result target — allocate a fresh
/// arena cell and return its [`ResultCellId`] (mirror of
/// `execExpr_core::new_result_cell`).
fn new_result_cell<'mcx>(mcx: Mcx<'mcx>, state: &mut ExprState<'mcx>) -> PgResult<ResultCellId> {
    ensure_result_arena(mcx, state)?;
    let cells = state.result_cells.cells.as_mut().unwrap();
    let id = ResultCellId(cells.len() as u32);
    cells.push(ResultCell::default());
    Ok(id)
}

/// `ExecComputeSlotInfo(state, op)` for a freshly-built `EEOP_*_FETCHSOME` step
/// (mirror of `execExpr_core::exec_compute_slot_info`). With no fixed parent
/// slot the deform step is always required and stays non-fixed.
fn exec_compute_slot_info<'mcx>(state: &ExprState<'mcx>, op: &mut ExprEvalStep<'mcx>) -> bool {
    let _ = state;
    if let ExprEvalStepData::Fetch {
        fixed,
        known_desc,
        kind,
        ..
    } = &mut op.d
    {
        // A `known_desc`/`kind` is supplied by every caller in this family (the
        // hashed/grouping desc is always known), so the slot is "fixed" to that
        // descriptor — exactly the C path where `op->d.fetch.known_desc` is set.
        if known_desc.is_some() {
            *fixed = true;
        } else {
            *fixed = false;
            *kind = None;
        }
    }
    true
}

/// `ExecReadyExpr(state)` — route to the interpreter's
/// `ExecReadyInterpretedExpr` (execExprInterp, the cycle partner).
fn exec_ready_expr<'mcx>(state: &mut ExprState<'mcx>) -> PgResult<()> {
    execExprInterp_seams::exec_ready_interpreted_expr::call(state)
}

/// Push an `EEOP_*_FETCHSOME` deform step for `last_var` columns of `desc`
/// (slot ops `kind`), running `ExecComputeSlotInfo` first (it always keeps the
/// step in this family). Helper shared by the three attr/expr hash + equality
/// builders.
fn push_fetchsome<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    opcode: ExprEvalOp,
    last_var: AttrNumber,
    desc: &TupleDescData<'mcx>,
    kind: TupleSlotKind,
) -> PgResult<()> {
    let _ = desc;
    let mut scratch = ExprEvalStep {
        opcode,
        resvalue: STATE_RESULT_CELL,
        resnull: STATE_RESULT_CELL,
        d: ExprEvalStepData::Fetch {
            last_var: last_var as i32,
            fixed: false,
            // The C stores `scratch.d.fetch.known_desc = desc`; the owned
            // `Fetch.known_desc` is an owned `PgBox<TupleDescData>` (deep-copy
            // territory). The descriptor is borrowed from the caller and the
            // deform classification only needs its `kind`, so we record the
            // `kind` and leave `known_desc` un-cloned (None). `fixed` is set
            // from `kind` by `exec_compute_slot_info`.
            known_desc: None,
            kind: Some(kind),
        },
    };
    if exec_compute_slot_info(state, &mut scratch) {
        core::expr_eval_push_step(mcx, state, scratch)?;
    }
    Ok(())
}

// ===========================================================================
// ExecInitCoerceToDomain (execExpr.c:3524) — DOMAIN_NOTNULL / DOMAIN_CHECK /
// CoerceToDomainValue (TESTVAL) step emission for a CoerceToDomain node.
// ===========================================================================

/// `ExecInitCoerceToDomain(scratch, ctest, state, resv, resnull)`
/// (execExpr.c:3524) — emit the domain-constraint check steps for a
/// `CoerceToDomain`. The argument is evaluated into `resv`/`resnull`; each
/// `NOTNULL` constraint tests it in place, each `CHECK` constraint evaluates a
/// (possibly R/O-forced) check expression and tests its result.
///
/// `scratch` is the caller's reusable step (its `resvalue`/`resnull` already
/// point at `resv`); `state` is the ExprState being compiled. The C threads the
/// `CoerceToDomainValue` read location through
/// `state->innermost_domainval`/`innermost_domainnull` while recursing into
/// each CHECK expression, save/restoring around the recursion.
///
/// Two leaf operations are genuine cross-owner calls, parked per "mirror PG and
/// panic":
///   * `InitDomainConstraintRef(ctest->resulttype, ...)` — the compiled
///     `DomainConstraintState` list is produced by **typcache** (its
///     `lookup_type_cache(TYPECACHE_DOMAIN_CONSTRAINT_INFO)` path); no typcache
///     seam is exported to this crate yet.
///   * `ExecInitExprRec(con->check_expr, ...)` for each CHECK — the
///     opcode-emission recursion is owned by `execExpr_core` and is not exposed
///     as a sibling-callable surface.
pub fn exec_init_coerce_to_domain<'mcx>(
    mcx: Mcx<'mcx>,
    scratch: &mut ExprEvalStep<'mcx>,
    ctest_resulttype: Oid,
    ctest_arg: &Expr<'mcx>,
    state: &mut ExprState<'mcx>,
    resv: ResultCellId,
) -> PgResult<()> {
    // scratch->d.domaincheck.resulttype = ctest->resulttype;
    // scratch->d.domaincheck.checkvalue = NULL;  (allocated lazily below)
    // scratch->d.domaincheck.escontext = state->escontext;
    scratch.d = ExprEvalStepData::DomainCheck {
        constraintname: None,
        checkvalue: STATE_RESULT_CELL, // "NULL" sentinel; replaced on first CHECK
        resulttype: ctest_resulttype,
        // C: scratch->d.domaincheck.escontext = state->escontext. When this
        // CoerceToDomain is compiled inside a JsonExpr ON ERROR / ON EMPTY
        // behavior expression, state.escontext is Some(jsestate), so a domain
        // CHECK violation steers ON ERROR softly instead of throwing hard.
        escontext: state.escontext,
    };

    // ExecInitExprRec(ctest->arg, state, resv, resnull); — evaluate the argument
    // directly into the caller's result cell.
    core::exec_init_expr_rec(mcx, ctest_arg, state, resv)?;

    // Collect the constraints associated with the domain. As of PG v10 these are
    // baked into the ExprState at init time (InitDomainConstraintRef with
    // need_exprstate == false: the CHECK expressions are compiled here via
    // ExecInitExprRec, so typcache need not provide compiled exprs).
    let constraints = typcache_seams::domain_constraint_list::call(
        ctest_resulttype,
    )?;

    // The lazily-allocated CHECK output workspace and the (shared) resulttype /
    // escontext the DomainCheck steps carry — read once from the scratch the
    // caller seeded above.
    let (resulttype, escontext) = match &scratch.d {
        ExprEvalStepData::DomainCheck {
            resulttype,
            escontext,
            ..
        } => (*resulttype, *escontext),
        _ => unreachable!("scratch.d seeded to DomainCheck above"),
    };

    // foreach(l, constraint_ref->constraints) { ... }
    let mut domainval: Option<ResultCellId> = None;
    // scratch->d.domaincheck.checkvalue == NULL until the first CHECK.
    let mut checkvalue_alloc: Option<ResultCellId> = None;
    for con in &constraints {
        // scratch->d.domaincheck.constraintname = con->name;
        let constraintname = Some(::mcx::PgString::from_str_in(&con.name, mcx)?);

        if con.constrainttype == DOM_CONSTRAINT_NOTNULL {
            // scratch->opcode = EEOP_DOMAIN_NOTNULL; ExprEvalPushStep. The
            // NOTNULL step shares the domaincheck payload; checkvalue is the
            // current sentinel (unused on this path).
            let step = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_DOMAIN_NOTNULL,
                resvalue: scratch.resvalue,
                resnull: scratch.resnull,
                d: ExprEvalStepData::DomainCheck {
                    constraintname,
                    checkvalue: checkvalue_alloc.unwrap_or(STATE_RESULT_CELL),
                    resulttype,
                    escontext,
                },
            };
            core::expr_eval_push_step(mcx, state, step)?;
        } else if con.constrainttype == DOM_CONSTRAINT_CHECK {
            // Allocate workspace for CHECK output if we didn't yet.
            let checkvalue = match checkvalue_alloc {
                Some(c) => c,
                None => {
                    let c = new_result_cell(mcx, state)?;
                    checkvalue_alloc = Some(c);
                    c
                }
            };

            // First CHECK: decide where CoerceToDomainValue nodes read from.
            if domainval.is_none() {
                // Since value might be read multiple times, force to R/O — but
                // only if it could be an expanded datum (typlen == -1).
                if lsyscache_get_typlen(ctest_resulttype)? == -1 {
                    let dv = new_result_cell(mcx, state)?;
                    // scratch2 = {0}; EEOP_MAKE_READONLY reading resv -> dv.
                    let scratch2 = ExprEvalStep {
                        opcode: ExprEvalOp::EEOP_MAKE_READONLY,
                        resvalue: dv,
                        resnull: dv,
                        d: ExprEvalStepData::MakeReadOnly { value: resv },
                    };
                    core::expr_eval_push_step(mcx, state, scratch2)?;
                    domainval = Some(dv);
                } else {
                    // No, so it's fine to read from resv/resnull.
                    domainval = Some(resv);
                }
            }
            let dv = domainval.unwrap();

            // Set up value to be returned by CoerceToDomainValue nodes; save and
            // restore innermost_domainval in case this node is itself within a
            // check expression for another domain.
            let save_dv = state.innermost_domainval;
            state.innermost_domainval = Some(dv);
            let check_expr = con
                .check_expr
                .as_ref()
                .expect("DOM_CONSTRAINT_CHECK with no check_expr");
            // The cached `check_expr` is the typcache's `'static`-erased tree;
            // clone it into the executor query arena (`mcx`) to compile it, since
            // `exec_init_expr_rec` threads the node tree as `'mcx` (Expr is invariant).
            let check_expr = check_expr.clone_in(mcx)?;
            // ExecInitExprRec(con->check_expr, state, checkvalue, checknull)
            core::exec_init_expr_rec(mcx, &check_expr, state, checkvalue)?;
            state.innermost_domainval = save_dv;

            // scratch->opcode = EEOP_DOMAIN_CHECK; ExprEvalPushStep.
            let step = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_DOMAIN_CHECK,
                resvalue: scratch.resvalue,
                resnull: scratch.resnull,
                d: ExprEvalStepData::DomainCheck {
                    constraintname,
                    checkvalue,
                    resulttype,
                    escontext,
                },
            };
            core::expr_eval_push_step(mcx, state, step)?;
        } else {
            return Err(PgError::error(format!(
                "unrecognized constraint type: {}",
                con.constrainttype
            )));
        }
    }
    Ok(())
}

// ===========================================================================
// ExecBuildAggTrans (execExpr.c:3679) — full per-trans / per-grouping-set
// transition-function-call program for an Agg phase.
// ===========================================================================

/// `ExecBuildAggTrans(aggstate, phase, doSort, doHash, nullcheck)`
/// (execExpr.c:3679) — build the transition/combine program for one grouping
/// sets phase: per-trans, emit the filter jump, evaluate the aggregate input
/// into the transfn fcinfo args (or the sort/uniq slot for ORDER BY/DISTINCT),
/// the strict-input null check, the presorted-DISTINCT check, then one
/// transition-call step per concurrently-computed grouping set (via
/// [`exec_build_agg_trans_call`]), and fix up every early-bailout jump to the
/// next trans.
///
/// `phase` is the index into `aggstate.phases`. The C reads the whole
/// `AggStatePerTrans` vocabulary (`transfn_fcinfo`, `deserialfn_*`,
/// `aggsortrequired`, `numInputs`/`numTransInputs`, `transtypeByVal`,
/// `initValueIsNull`, `sortslot`) and writes step outputs into the
/// externally-owned `trans_fcinfo->args[]` / `sortslot->tts_values[]` cells.
///
/// Genuine cross-owner blockers (parked per "mirror PG and panic"):
///   * `ExecInitExprRec(source_tle->expr, ...)` for each aggregate-input
///     argument and the filter — the opcode-emission recursion is owned by
///     `execExpr_core` (private).
///   * The C threads `&trans_fcinfo->args[N].value`/`.isnull` and
///     `&sortslot->tts_values[N]` as the recursion output target. Those are
///     **external** result cells (in the nodeAgg-owned `AggStatePerTrans` /
///     `TupleTableSlot`), which the owned `ResultCellArena` (cells internal to
///     this `ExprState`) cannot name; the `AggTrans`/`AggStrictInputCheck`/
///     `AggDeserialize` payloads carry the `pertrans`/fcinfo back-pointers as
///     parked addresses and have no arg-cell vector. Wiring those external
///     targets is a keystone-type change beyond this family's module.
/// Seam adapter for `exec_build_agg_trans` — recovers the concrete
/// `AggStateData<'mcx>` from the erased [`AggStateLive`] carrier that crosses
/// the `nodeAgg -> execExpr` seam edge, then delegates to the real builder.
/// (nodeAgg sits above execExpr, so it cannot call the concrete fn directly;
/// the seam carries the AggState type-erased, this adapter downcasts it.)
pub fn seam_exec_build_agg_trans<'mcx>(
    mcx: Mcx<'mcx>,
    aggstate: &mut (dyn ::nodes::aggstate_carrier::AggStateLive<'mcx> + 'mcx),
    phase: i32,
    do_sort: bool,
    do_hash: bool,
    nullcheck: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let concrete = ::nodes::aggstate_carrier::downcast_agg_state_mut::<AggStateData<'mcx>>(
        aggstate,
    )
    .expect(
        "exec_build_agg_trans: PlanState payload is not an AggStateData (tag mismatch across \
         the nodeAgg->execExpr seam)",
    );
    exec_build_agg_trans(mcx, concrete, phase, do_sort, do_hash, nullcheck, estate)
}

pub fn exec_build_agg_trans<'mcx>(
    mcx: Mcx<'mcx>,
    aggstate: &mut AggStateData<'mcx>,
    phase: i32,
    do_sort: bool,
    do_hash: bool,
    nullcheck: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    // state = makeNode(ExprState); state->expr = (Expr *) aggstate;
    // state->parent = &aggstate->ss.ps;  scratch.resvalue=&state->resvalue.
    //
    // The C `state->expr = (Expr *) aggstate` is a debug back-link only; the
    // owned ExprState.expr is an `Option<PgBox<Expr>>` and an AggState is not an
    // Expr node, so the back-link is left unset (it is never dereferenced during
    // evaluation). `state->parent = &aggstate->ss.ps` is a back-pointer whose
    // only runtime use is `state->parent->state` (the EState) when an aggregate
    // ARGUMENT contains a SubPlan (e.g. `sum((SELECT ...))`); `exec_init_sub_plan_expr`
    // reaches the EState through `ExprState.es_link`, so stamp it from the
    // EState the caller passed (the owned-model equivalent of `parent->state`).
    let mut state = make_expr_state(mcx)?;
    state.es_link = Some(::nodes::execnodes::EStateLink::from_ref(estate));
    let is_combine = do_aggsplit_combine(aggstate.aggsplit);

    // The C reusable `scratch` step lives on the stack and is byte-copied into
    // the program by ExprEvalPushStep; here each push builds its own owned step
    // (the owned step payload is moved per push). `scratch.resvalue`/`resnull`
    // default to STATE_RESULT_CELL (the C `&state->resvalue`/`&state->resnull`).

    // -----------------------------------------------------------------------
    // First figure out which slots, and how many columns from each, we're going
    // to need (expr_setup_walker over each pertrans aggref's aggdirectargs /
    // args / aggorder / aggdistinct / aggfilter), then ExecPushExprSetupSteps.
    //
    // `expr_setup_walker` / `ExecPushExprSetupSteps` are private to
    // `execExpr_core`; this family restates the small accumulator
    // (`agg_setup_walker`, mirroring the core walker's child-link descent) and
    // emits the FETCHSOME deform prefix with the local `push_fetchsome` (the
    // file-level mirror of the core spine helper). The aggorder/aggdistinct
    // SortGroupClause lists are not modeled on `Aggref` (they reference the
    // `args` columns by position and carry no Vars of their own), so walking
    // `aggdirectargs` + `args` (TargetEntry exprs) + `aggfilter` covers every
    // attribute the deform must reach — matching the C walk.
    // -----------------------------------------------------------------------
    let num_trans = aggstate.numtrans;
    let mut deform = LastAttnumInfo::default();
    {
        let pertrans_vec = aggstate
            .pertrans
            .as_ref()
            .expect("ExecBuildAggTrans: aggstate->pertrans is NULL");
        for transno in 0..num_trans as usize {
            let pertrans = &pertrans_vec[transno];
            let aggref = pertrans
                .aggref
                .as_ref()
                .expect("ExecBuildAggTrans: pertrans->aggref is NULL");
            if let Some(directargs) = aggref.aggdirectargs.as_ref() {
                for e in directargs.iter() {
                    agg_setup_walker(e, &mut deform);
                }
            }
            if let Some(args) = aggref.args.as_ref() {
                for tle in args.iter() {
                    if let Some(e) = tle.expr.as_deref() {
                        agg_setup_walker(e, &mut deform);
                    }
                }
            }
            if let Some(f) = aggref.aggfilter.as_deref() {
                agg_setup_walker(f, &mut deform);
            }
        }
    }
    push_setup_steps(mcx, &mut state, &deform)?;

    // -----------------------------------------------------------------------
    // Emit instructions for each transition value / grouping set combination.
    // -----------------------------------------------------------------------
    for transno in 0..num_trans as usize {
        // Read the per-trans predicates we need up front (the borrow of
        // `aggstate.pertrans` cannot be held across the &mut state pushes /
        // the &aggstate ExecBuildAggTransCall call, so snapshot the scalar
        // fields and the transfn strictness here).
        let p = pertrans_pred(aggstate, transno);

        // List of step indices whose early-bailout jump must be pointed past the
        // whole trans (the C `adjust_bailout`).
        let mut adjust_bailout: PgVec<'mcx, i32> = ::mcx::vec_with_capacity_in(mcx, 4)?;

        // strictnulls / strictargs selection (C locals): for the strict-input
        // check, either a `nulls` array (sorted paths) or the per-arg cells
        // (`args + 1`). In the owned model the per-arg cells are arena
        // ResultCellIds; `strict_arg_cells` collects them for the ARGS variant,
        // `strict_uses_nulls` marks the NULLS variant.
        let mut strict_arg_cells: Option<PgVec<'mcx, ResultCellId>> = None;
        let mut strict_uses_nulls = false;

        // The transition function's per-row INPUT arg cells (`&trans_fcinfo->args[i
        // + 1]`), kept separately from `strict_arg_cells` (which is consumed by
        // the strict-input-check step) so they can be threaded into the
        // transition-call step's `AggTrans.arg_cells` for the interpreter to
        // gather into `fcinfo->args[1..]`. Empty for ordered (DATUM/TUPLE) and
        // zero-input aggregates (e.g. `count(*)`).
        let mut trans_input_cells: PgVec<'mcx, ResultCellId> =
            ::mcx::vec_with_capacity_in(mcx, p.num_trans_inputs.max(0) as usize)?;

        // ---- filter (before evaluating input; skipped when combining) ----
        if p.has_aggfilter && !is_combine {
            // evaluate filter expression into &state->resvalue/&state->resnull.
            // The aggref subtree is borrowed read-only from aggstate; clone the
            // small filter Expr so the borrow does not collide with the &mut
            // state recursion (the C uses the node in place — read-only).
            let aggfilter = aggref_of(aggstate, transno)
                .aggfilter
                .as_deref()
                .expect("ExecBuildAggTrans: aggfilter present but NULL")
                .clone_in(mcx)?;
            core::exec_init_expr_rec(mcx, &aggfilter, &mut state, STATE_RESULT_CELL)?;
            // and jump out if false
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_JUMP_IF_NOT_TRUE,
                resvalue: STATE_RESULT_CELL,
                resnull: STATE_RESULT_CELL,
                d: ExprEvalStepData::Jump { jumpdone: -1 }, // adjust later
            };
            core::expr_eval_push_step(mcx, &mut state, scratch)?;
            adjust_bailout.push(state.steps_len - 1);
        }

        // ---- evaluate arguments to aggregate/combine function ----
        let mut argno: i32 = 0;
        if is_combine {
            // Combining two aggregate transition values. The input is a,
            // potentially deserialized, transition value rather than a tuple.
            debug_assert_eq!(p.num_sort_cols, 0);
            debug_assert_eq!(p.aggref_args_len, 1);

            // source_tle = linitial(pertrans->aggref->args). Clone the (single)
            // source Expr so the read-only borrow does not collide with the
            // &mut state recursion (C uses the node in place).
            let source_expr = arg_tle_expr_clone(aggstate, transno, 0, mcx)?;

            if !p.deserialfn_valid {
                // No deserialization: recurse the source straight into the
                // transfn input arg cell (&trans_fcinfo->args[argno + 1]).
                let arg_cell = new_result_cell(mcx, &mut state)?;
                core::exec_init_expr_rec(mcx, &source_expr, &mut state, arg_cell)?;
                strict_arg_cells = Some(single_cell_vec(mcx, arg_cell)?);
                trans_input_cells.push(arg_cell);
            } else {
                // deserialfn_oid set: we must deserialize the input transition
                // state before calling the combine function.
                //
                //   FunctionCallInfo ds_fcinfo = pertrans->deserialfn_fcinfo;
                //   ExecInitExprRec(source_tle->expr, state,
                //                   &ds_fcinfo->args[0].value,
                //                   &ds_fcinfo->args[0].isnull);
                //   ds_fcinfo->args[1] = dummy (PointerGetDatum(NULL), notnull);
                //
                // The owned model mirrors the C `ds_fcinfo` frame the same way
                // the HashDatum path mirrors its hash fcinfo: a fresh
                // `init_fcinfo` call frame (`AggDeserialize.fcinfo_data`, an
                // owned PgBox) plus the F0-modeled `arg_cell` arena id standing
                // in for the `&ds_fcinfo->args[0]` aliasing target the
                // serialized-state sub-expression evaluates into. The dummy
                // args[1] is a no-op here — the trimmed FunctionCallInfoBaseData
                // carries no `args[]` vector; the interpreter supplies the dummy
                // second argument from the deserialfn frame at call time. The
                // re-resolved deserialfn FmgrInfo / collation are threaded by the
                // owner alongside the frame, exactly like the transfn frame.
                // InitFunctionCallInfoData(*ds_fcinfo, &pertrans->deserialfn, 2,
                //   InvalidOid, (Node *) aggstate, NULL) — point the frame at the
                // resolved deserialfn FmgrInfo so the interpreter re-dispatches by
                // its fn_oid (the collation stays InvalidOid; deserialfns ignore it).
                let mut ds_fcinfo = init_fcinfo(mcx, ::types_core::InvalidOid)?;
                ds_fcinfo.flinfo = Some(p.deserialfn.clone());
                let ds_arg_cell = new_result_cell(mcx, &mut state)?;
                core::exec_init_expr_rec(mcx, &source_expr, &mut state, ds_arg_cell)?;

                // The deserialize step writes into the transfn's first real
                // argument cell (the C `&trans_fcinfo->args[argno + 1]`); name it
                // with a fresh arena cell so the strict-input check and the
                // transition call read it as the transfn input.
                let out_cell = new_result_cell(mcx, &mut state)?;
                trans_input_cells.push(out_cell);

                // Don't call a strict deserialization function with NULL input.
                let opcode = if p.deserialfn_strict {
                    ExprEvalOp::EEOP_AGG_STRICT_DESERIALIZE
                } else {
                    ExprEvalOp::EEOP_AGG_DESERIALIZE
                };
                let scratch = ExprEvalStep {
                    opcode,
                    resvalue: out_cell,
                    resnull: out_cell,
                    d: ExprEvalStepData::AggDeserialize {
                        fcinfo_data: Some(ds_fcinfo),
                        arg_cell: ds_arg_cell,
                        jumpnull: -1, // adjust later
                    },
                };
                core::expr_eval_push_step(mcx, &mut state, scratch)?;
                // Don't add an adjustment unless the function is strict.
                if p.deserialfn_strict {
                    adjust_bailout.push(state.steps_len - 1);
                }
                // strictargs = trans_fcinfo->args + 1 (the C sets it once at the
                // top of the isCombine branch); the single transfn input is the
                // deserialize step's output cell.
                strict_arg_cells = Some(single_cell_vec(mcx, out_cell)?);
            }
            argno += 1;
            debug_assert_eq!(p.num_inputs, argno);
        } else if !p.aggsortrequired {
            // Normal transition function without ORDER BY / DISTINCT, or with
            // ORDER BY / DISTINCT but presorted input. strictargs =
            // trans_fcinfo->args + 1.
            let mut cells: PgVec<'mcx, ResultCellId> =
                ::mcx::vec_with_capacity_in(mcx, p.num_trans_inputs.max(0) as usize)?;
            let nargs = p.aggref_args_len;
            for i in 0..nargs {
                // Don't initialize args for any ORDER BY clause that might exist
                // in a presorted aggregate.
                if argno == p.num_trans_inputs {
                    break;
                }
                let arg = arg_tle_expr_clone(aggstate, transno, i, mcx)?;
                // Recurse into &trans_fcinfo->args[argno + 1].
                let arg_cell = new_result_cell(mcx, &mut state)?;
                core::exec_init_expr_rec(mcx, &arg, &mut state, arg_cell)?;
                cells.push(arg_cell);
                trans_input_cells.push(arg_cell);
                argno += 1;
            }
            debug_assert_eq!(p.num_trans_inputs, argno);
            strict_arg_cells = Some(cells);
        } else if p.num_inputs == 1 {
            // Non-presorted DISTINCT and/or ORDER BY case, single sort column.
            debug_assert_eq!(p.aggref_args_len, 1);
            let arg = arg_tle_expr_clone(aggstate, transno, 0, mcx)?;
            // Recurse into &state->resvalue / &state->resnull.
            core::exec_init_expr_rec(mcx, &arg, &mut state, STATE_RESULT_CELL)?;
            // strictnulls = &state->resnull
            strict_uses_nulls = true;
            argno += 1;
            debug_assert_eq!(p.num_inputs, argno);
        } else {
            // Non-presorted DISTINCT and/or ORDER BY case, multiple sort
            // columns. The C recurses each input into
            // &pertrans->sortslot->tts_values[argno] / tts_isnull[argno] — the
            // value/null cells of the nodeAgg-owned input TupleTableSlot — and
            // sets `strictnulls = nulls` (sortslot->tts_isnull).
            //
            // The owned-model TupleTableSlot does not expose its
            // tts_values/tts_isnull as addressable per-attribute ResultCells, so
            // each input is recursed into a fresh arena cell instead; the
            // interpreter's EEOP_AGG_ORDERED_TRANS_TUPLE step reads those cells
            // and stages them onto the sortslot through store_virtual_values
            // (the same indirection EEOP_AGG_PRESORTED_DISTINCT_MULTI uses to
            // avoid naming the slot cells). The strict-input null check reads
            // the same per-column cells via the ARGS variant — equivalent to C
            // scanning sortslot->tts_isnull, since those nulls came from these
            // very recursions.
            let mut cells: PgVec<'mcx, ResultCellId> =
                ::mcx::vec_with_capacity_in(mcx, p.aggref_args_len.max(0) as usize)?;
            for i in 0..p.aggref_args_len {
                let arg = arg_tle_expr_clone(aggstate, transno, i, mcx)?;
                // Recurse into &values[argno] / &nulls[argno].
                let arg_cell = new_result_cell(mcx, &mut state)?;
                core::exec_init_expr_rec(mcx, &arg, &mut state, arg_cell)?;
                cells.push(arg_cell);
                trans_input_cells.push(arg_cell);
                argno += 1;
            }
            debug_assert_eq!(p.num_inputs, argno);
            strict_arg_cells = Some(cells);
        }

        // ---- strict-input null check ----
        //
        // For a strict transfn, nothing happens on a NULL input; keep the prior
        // transValue. True for both plain and sorted/distinct aggregates.
        if p.transfn_strict && p.num_trans_inputs > 0 {
            let opcode = if strict_uses_nulls {
                ExprEvalOp::EEOP_AGG_STRICT_INPUT_CHECK_NULLS
            } else if strict_arg_cells.is_some() && p.num_trans_inputs == 1 {
                ExprEvalOp::EEOP_AGG_STRICT_INPUT_CHECK_ARGS_1
            } else {
                ExprEvalOp::EEOP_AGG_STRICT_INPUT_CHECK_ARGS
            };
            // scratch.d.agg_strict_input_check = { nulls, args, jumpnull, nargs }
            //
            // NULLS variant: C sets `nulls = strictnulls`. The only reachable
            // NULLS emission here is the single-column non-presorted sort path,
            // where `strictnulls = &state->resnull` — i.e. the well-known
            // STATE_RESULT_CELL (cell 0), into which that path's single input was
            // just evaluated. The interpreter reads STATE_RESULT_CELL's is-null
            // for the (single) strict check, so no per-cell `nulls`/`arg_cells`
            // vector is needed (the multi-column sort path, whose strictnulls is
            // the nodeAgg-owned sortslot->tts_isnull, panics above). `nulls` /
            // `args` stay None (they are the C owned-workspace copies, not the
            // aliasing source).
            //
            // ARGS variants: name the per-arg cells the transfn input
            // sub-expressions evaluated into (strict_arg_cells), the
            // owned-model replacement for the C `args = trans_fcinfo->args + 1`.
            let arg_cells = if strict_uses_nulls {
                None
            } else {
                strict_arg_cells.take()
            };
            let scratch = ExprEvalStep {
                opcode,
                resvalue: STATE_RESULT_CELL,
                resnull: STATE_RESULT_CELL,
                d: ExprEvalStepData::AggStrictInputCheck {
                    args: None,
                    nulls: None,
                    arg_cells,
                    nargs: p.num_trans_inputs,
                    jumpnull: -1, // adjust later
                },
            };
            core::expr_eval_push_step(mcx, &mut state, scratch)?;
            adjust_bailout.push(state.steps_len - 1);
        }

        // ---- DISTINCT aggregates with pre-sorted input ----
        if p.num_distinct_cols > 0 && !p.aggsortrequired {
            let opcode = if p.num_distinct_cols > 1 {
                ExprEvalOp::EEOP_AGG_PRESORTED_DISTINCT_MULTI
            } else {
                ExprEvalOp::EEOP_AGG_PRESORTED_DISTINCT_SINGLE
            };
            // scratch.d.agg_presorted_distinctcheck.pertrans = pertrans;
            // For the SINGLE variant the C reads pertrans->transfn_fcinfo->args[1],
            // which the input recursion populated; this owned model put the input
            // into trans_input_cells[0] (args[1]), so thread that cell so the
            // interpreter can copy it into the per-trans fcinfo. MULTI reads the
            // sortslot and ignores it.
            let input_cell = trans_input_cells
                .first()
                .copied()
                .unwrap_or(STATE_RESULT_CELL);
            // MULTI: thread ALL numTransInputs input cells so the interpreter can
            // stage them onto the per-trans sortslot by-reference-faithfully (each
            // cell carries the input Datum on its ByRef arm for a typbyval=false
            // attribute). C recurses each input straight into
            // &pertrans->transfn_fcinfo->args[i + 1]; SINGLE keeps the input_cell
            // path and leaves this empty.
            let mut input_cells: PgVec<'mcx, ResultCellId> =
                ::mcx::vec_with_capacity_in(mcx, trans_input_cells.len())?;
            if opcode == ExprEvalOp::EEOP_AGG_PRESORTED_DISTINCT_MULTI {
                for &c in trans_input_cells.iter() {
                    input_cells.push(c);
                }
            }
            let scratch = ExprEvalStep {
                opcode,
                resvalue: STATE_RESULT_CELL,
                resnull: STATE_RESULT_CELL,
                d: ExprEvalStepData::AggPresortedDistinctCheck {
                    pertrans: transno,
                    aggcontext: 0,
                    input_cell,
                    input_cells,
                    jumpdistinct: -1, // adjust later
                },
            };
            core::expr_eval_push_step(mcx, &mut state, scratch)?;
            adjust_bailout.push(state.steps_len - 1);
        }

        // ---- call transition function (once per concurrently-evaluated set) ----
        if do_sort {
            let process_grouping_sets = phase_numsets(aggstate, phase).max(1);
            let mut setoff: i32 = 0;
            for setno in 0..process_grouping_sets {
                exec_build_agg_trans_call(
                    mcx, &mut state, aggstate, transno, transno as i32, setno, setoff, false,
                    nullcheck, &trans_input_cells,
                )?;
                setoff += 1;
            }
        }

        if do_hash {
            let num_hashes = aggstate.num_hashes;
            // In MIXED mode, there'll be preceding transition values.
            let mut setoff: i32 = if aggstate.aggstrategy != AggStrategy::AggHashed {
                aggstate.maxsets
            } else {
                0
            };
            for setno in 0..num_hashes {
                exec_build_agg_trans_call(
                    mcx, &mut state, aggstate, transno, transno as i32, setno, setoff, true,
                    nullcheck, &trans_input_cells,
                )?;
                setoff += 1;
            }
        }

        // ---- adjust early bail-out jump target(s) ----
        for bail in adjust_bailout.iter().copied() {
            let target = state.steps_len;
            let steps = state
                .steps
                .as_mut()
                .expect("ExecBuildAggTrans: steps array is NULL during jump fixup");
            let as_step = &mut steps[bail as usize];
            match &mut as_step.d {
                ExprEvalStepData::Jump { jumpdone }
                    if as_step.opcode == ExprEvalOp::EEOP_JUMP_IF_NOT_TRUE =>
                {
                    debug_assert_eq!(*jumpdone, -1);
                    *jumpdone = target;
                }
                ExprEvalStepData::AggStrictInputCheck { jumpnull, .. } => {
                    debug_assert!(matches!(
                        as_step.opcode,
                        ExprEvalOp::EEOP_AGG_STRICT_INPUT_CHECK_ARGS
                            | ExprEvalOp::EEOP_AGG_STRICT_INPUT_CHECK_ARGS_1
                            | ExprEvalOp::EEOP_AGG_STRICT_INPUT_CHECK_NULLS
                    ));
                    debug_assert_eq!(*jumpnull, -1);
                    *jumpnull = target;
                }
                ExprEvalStepData::AggDeserialize { jumpnull, .. } => {
                    debug_assert_eq!(as_step.opcode, ExprEvalOp::EEOP_AGG_STRICT_DESERIALIZE);
                    debug_assert_eq!(*jumpnull, -1);
                    *jumpnull = target;
                }
                ExprEvalStepData::AggPresortedDistinctCheck { jumpdistinct, .. } => {
                    debug_assert!(matches!(
                        as_step.opcode,
                        ExprEvalOp::EEOP_AGG_PRESORTED_DISTINCT_SINGLE
                            | ExprEvalOp::EEOP_AGG_PRESORTED_DISTINCT_MULTI
                    ));
                    debug_assert_eq!(*jumpdistinct, -1);
                    *jumpdistinct = target;
                }
                _ => debug_assert!(false, "unexpected bail-out step opcode"),
            }
        }
    }

    // scratch.resvalue = NULL; scratch.resnull = NULL;
    // scratch.opcode = EEOP_DONE_NO_RETURN; ExprEvalPushStep.
    let done = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_DONE_NO_RETURN,
        resvalue: STATE_RESULT_CELL,
        resnull: STATE_RESULT_CELL,
        d: ExprEvalStepData::NoPayload,
    };
    core::expr_eval_push_step(mcx, &mut state, done)?;

    exec_ready_expr(&mut state)?;

    // C: `ExecInitSubPlan` appends every SubPlan compiled into this transition
    // program onto `state->parent->subPlan` (where `state->parent` is
    // `&aggstate->ss.ps`). An aggregate ARGUMENT can itself contain a correlated
    // SubPlan — e.g. `max((SELECT i.unique2 FROM tenk1 i WHERE i.unique1 =
    // o.unique1))` (sublink within an outer-level aggregate, aggregates.sql) —
    // and those SubPlans are discovered while `exec_init_expr_rec` walks the
    // aggref args here. In the owned model the discovery rides the ExprState's
    // `found_subplan_ids` channel; drain it into the AggState's PlanState head
    // `sub_plan_ids` (the owned-model split of `PlanState.subPlan`) so EXPLAIN
    // walks the SubPlan body and ExecReScanAgg propagates chgParam into it. The
    // standard qual/projection compiles drain via the `ExecInitQual` /
    // `ExecBuildProjectionInfo` entry points; the transition program is built
    // directly here and so must drain explicitly, exactly as the C does at
    // SubPlan-init time.
    core::drain_found_subplan_ids(mcx, &mut aggstate.ss.ps, &mut state)?;

    ::mcx::alloc_in(mcx, state)
}

/// `ExecBuildAggTransCall(state, aggstate, scratch, fcinfo, pertrans, transno,
/// setno, setoff, ishash, nullcheck)` (execExpr.c:4021) — emit the single
/// transition-call step (and its optional pergroup NULL-pointer check) for one
/// (trans, grouping-set) pair, selecting the BYVAL/BYREF × strict × init-strict
/// opcode for non-ordered aggregates or the ORDERED_TRANS_{DATUM,TUPLE} opcode
/// for ordered ones, then fixing up the nullcheck jump.
///
/// `pertrans` is the index into `aggstate.pertrans`. `scratch` is the reusable
/// step. The `aggcontext` is `aggstate.hashcontext` (ishash) or
/// `aggstate.aggcontexts[setno]`; in the owned model those `ExprContext`s are
/// EState-pool ids threaded by nodeAgg, parked as addresses in the `AggTrans`
/// payload. This routine's control flow + opcode selection are this family's
/// own logic; the `AggStatePerTrans` back-pointer and the `aggcontext` id are
/// genuine nodeAgg-owned products carried as parked addresses.
#[allow(clippy::too_many_arguments)]
pub fn exec_build_agg_trans_call<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    aggstate: &AggStateData<'mcx>,
    pertrans: usize,
    transno: i32,
    setno: i32,
    setoff: i32,
    ishash: bool,
    nullcheck: bool,
    trans_input_cells: &[ResultCellId],
) -> PgResult<()> {
    // aggcontext = ishash ? aggstate->hashcontext : aggstate->aggcontexts[setno];
    // In the owned model an `ExprContext *` is an `EcxtId` into the EState pool
    // (`aggstate.hashcontext` / `aggstate.aggcontexts[setno]`, threaded by
    // ExecInitAgg before ExecBuildAggTrans runs). Resolve and store the real
    // id — the C `op->d.agg_trans.aggcontext` — so the transition step's by-ref
    // `datumCopy` target is the correct per-grouping-set (or hash) context.
    let aggcontext: Option<::nodes::execnodes::EcxtId> = if ishash {
        aggstate.hashcontext
    } else {
        aggstate
            .aggcontexts
            .as_ref()
            .and_then(|v| v.get(setno as usize).copied())
    };

    let mut adjust_jumpnull: i32 = -1;

    // add check for NULL pointer?
    if nullcheck {
        let scratch = ExprEvalStep {
            opcode: ExprEvalOp::EEOP_AGG_PLAIN_PERGROUP_NULLCHECK,
            resvalue: STATE_RESULT_CELL,
            resnull: STATE_RESULT_CELL,
            d: ExprEvalStepData::AggPlainPergroupNullcheck {
                setoff,
                jumpnull: -1, // adjust later
            },
        };
        core::expr_eval_push_step(mcx, state, scratch)?;
        adjust_jumpnull = state.steps_len - 1;
    }

    // Determine appropriate transition implementation (see the C banner): for
    // non-ordered aggregates and presorted ORDER BY/DISTINCT, pick BYVAL vs
    // BYREF and within each INIT_STRICT (strict + no initial value) vs STRICT
    // (strict + has initial value) vs plain; for ordered aggregates, DATUM
    // (single input) vs TUPLE (multiple).
    let p = pertrans_pred(aggstate, pertrans);
    let opcode = if !p.aggsortrequired {
        if p.transtype_by_val {
            if p.transfn_strict && p.init_value_is_null {
                ExprEvalOp::EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL
            } else if p.transfn_strict {
                ExprEvalOp::EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL
            } else {
                ExprEvalOp::EEOP_AGG_PLAIN_TRANS_BYVAL
            }
        } else if p.transfn_strict && p.init_value_is_null {
            ExprEvalOp::EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF
        } else if p.transfn_strict {
            ExprEvalOp::EEOP_AGG_PLAIN_TRANS_STRICT_BYREF
        } else {
            ExprEvalOp::EEOP_AGG_PLAIN_TRANS_BYREF
        }
    } else if p.num_inputs == 1 {
        ExprEvalOp::EEOP_AGG_ORDERED_TRANS_DATUM
    } else {
        ExprEvalOp::EEOP_AGG_ORDERED_TRANS_TUPLE
    };

    // scratch->d.agg_trans = { pertrans, setno, setoff, transno, aggcontext };
    // The ORDERED_TRANS_DATUM (single-column) opcode feeds its input through
    // `&state->resvalue`/`&state->resnull` (STATE_RESULT_CELL) — leave arg_cells
    // empty. ORDERED_TRANS_TUPLE (multi-column) needs the per-column input cells
    // so the interpreter can stage them onto the sortslot virtual tuple via
    // store_virtual_values (the owned-model replacement for C recursing each
    // input straight into &sortslot->tts_values[i]); thread them through
    // arg_cells. The plain TRANS opcodes gather these into `fcinfo->args[1..]`.
    let arg_cells: PgVec<'mcx, ResultCellId> =
        if opcode == ExprEvalOp::EEOP_AGG_ORDERED_TRANS_DATUM {
            ::mcx::vec_with_capacity_in(mcx, 0)?
        } else {
            let mut v: PgVec<'mcx, ResultCellId> =
                ::mcx::vec_with_capacity_in(mcx, trans_input_cells.len())?;
            for &c in trans_input_cells {
                v.push(c);
            }
            v
        };
    let scratch = ExprEvalStep {
        opcode,
        resvalue: STATE_RESULT_CELL,
        resnull: STATE_RESULT_CELL,
        d: ExprEvalStepData::AggTrans {
            pertrans,
            aggcontext,
            setno,
            transno,
            setoff,
            arg_cells,
        },
    };
    core::expr_eval_push_step(mcx, state, scratch)?;

    // fix up jumpnull
    if adjust_jumpnull != -1 {
        let target = state.steps_len;
        let steps = state
            .steps
            .as_mut()
            .expect("ExecBuildAggTransCall: steps array is NULL during jumpnull fixup");
        let as_step = &mut steps[adjust_jumpnull as usize];
        match &mut as_step.d {
            ExprEvalStepData::AggPlainPergroupNullcheck { jumpnull, .. } => {
                debug_assert_eq!(
                    as_step.opcode,
                    ExprEvalOp::EEOP_AGG_PLAIN_PERGROUP_NULLCHECK
                );
                debug_assert_eq!(*jumpnull, -1);
                *jumpnull = target;
            }
            _ => debug_assert!(false, "jumpnull fixup target is not a PERGROUP_NULLCHECK"),
        }
    }
    Ok(())
}

// ===========================================================================
// ExecBuildHash32FromAttrs (execExpr.c:4143) — hash an inner tuple's keyColIdx
// attributes (combining per-column hashes) into a uint32 ExprState.
// ===========================================================================

/// `ExecBuildHash32FromAttrs(desc, ops, hashfunctions, collations, numCols,
/// keyColIdx, parent, init_value)` (execExpr.c:4143) — build an ExprState whose
/// evaluation hashes `numCols` inner-tuple attributes (named by `keyColIdx`,
/// 1-based) with the given per-column hash functions, combining the per-column
/// results (and optionally seeding with `init_value`).
///
/// Emits: one `EEOP_INNER_FETCHSOME` deform to the highest keyColIdx, an
/// optional `EEOP_HASHDATUM_SET_INITVAL`, then per column an `EEOP_INNER_VAR`
/// (into the hash fcinfo's arg 0) and an `EEOP_HASHDATUM_FIRST`/`_NEXT32` call.
/// The final column's result lands in the state's `resvalue`; intermediate
/// results land in an `iresult` `NullableDatum`.
///
/// The per-column `FmgrInfo` are supplied by the caller (`hashfunctions[i]`,
/// already `fmgr_info`'d). In the owned model the `HashDatum` payload carries
/// `finfo`/`fcinfo_data` as owned boxes plus the single `arg_cell`
/// [`ResultCellId`] standing in for the C `&fcinfo->args[0]` aliasing target
/// (the F0 shared model added this field): the `EEOP_INNER_VAR` step writes that
/// arena cell, and the interpreter gathers it into `fcinfo->args[0]` immediately
/// before the hash call. The running hash flows through `resvalue` (the
/// intermediate arena cell for non-final steps, the state result for the last);
/// `iresult` is the per-step intermediate `NullableDatum` workspace.
#[allow(clippy::too_many_arguments)]
pub fn exec_build_hash32_from_attrs<'mcx>(
    mcx: Mcx<'mcx>,
    desc: &TupleDescData<'mcx>,
    ops: TupleSlotKind,
    hashfunctions: &[FmgrInfo],
    collations: &[Oid],
    num_cols: i32,
    key_col_idx: &[AttrNumber],
    init_value: u32,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    debug_assert!(num_cols >= 0);
    let mut state = make_expr_state(mcx)?;

    // We need an intermediate hash slot only if more than one value is combined.
    // The C `iresult = palloc(sizeof(NullableDatum))`; the owned running-hash
    // location is a dedicated arena cell (the cell `resvalue` of the non-final
    // steps points at).
    let need_iresult = (num_cols as i64) + ((init_value != 0) as i64) > 1;
    let iresult_cell = if need_iresult {
        Some(new_result_cell(mcx, &mut state)?)
    } else {
        None
    };

    // find the highest attnum so we deform the tuple to that point
    let mut last_attnum: AttrNumber = 0;
    for i in 0..num_cols as usize {
        last_attnum = last_attnum.max(key_col_idx[i]);
    }

    // EEOP_INNER_FETCHSOME deform step.
    push_fetchsome(
        mcx,
        &mut state,
        ExprEvalOp::EEOP_INNER_FETCHSOME,
        last_attnum,
        desc,
        ops,
    )?;

    // init_value handling: with no initial value the first column uses
    // HASHDATUM_FIRST; otherwise a SET_INITVAL step seeds the intermediate (or
    // the state result if no columns) and the first column uses NEXT32.
    let mut opcode = ExprEvalOp::EEOP_HASHDATUM_FIRST;
    if init_value != 0 {
        // resvalue = numCols>0 ? &iresult->value : &state->resvalue
        let (resv, resn) = if num_cols > 0 {
            let c = iresult_cell.expect("init_value with numCols>0 needs iresult");
            (c, c)
        } else {
            (STATE_RESULT_CELL, STATE_RESULT_CELL)
        };
        let initstep = ExprEvalStep {
            opcode: ExprEvalOp::EEOP_HASHDATUM_SET_INITVAL,
            resvalue: resv,
            resnull: resn,
            d: ExprEvalStepData::HashDatumInitValue {
                // UInt32GetDatum(init_value)
                init_value: Datum::from_u32(init_value),
            },
        };
        core::expr_eval_push_step(mcx, &mut state, initstep)?;
        // When using an initial value use the NEXT32 ops as the FIRST ops would
        // overwrite the stored initial value.
        opcode = ExprEvalOp::EEOP_HASHDATUM_NEXT32;
    }

    for i in 0..num_cols as usize {
        let inputcollid = collations[i];
        let attnum = (key_col_idx[i] as i32) - 1;

        // finfo = &hashfunctions[i]; fcinfo = palloc0(SizeForFunctionCallInfo(1));
        // InitFunctionCallInfoData(*fcinfo, finfo, 1, inputcollid, NULL, NULL);
        let finfo = ::mcx::alloc_in(mcx, hashfunctions[i].clone())?;
        let fcinfo = init_fcinfo(mcx, inputcollid)?;

        // Fetch inner Var for this attnum and store it in the 1st arg of the
        // hash func — the owned `arg_cell` stands in for `&fcinfo->args[0]`.
        let arg_cell = new_result_cell(mcx, &mut state)?;
        let varstep = ExprEvalStep {
            opcode: ExprEvalOp::EEOP_INNER_VAR,
            resvalue: arg_cell,
            resnull: arg_cell,
            d: ExprEvalStepData::Var {
                attnum,
                vartype: desc.attr(attnum as usize).atttypid,
                varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            },
        };
        core::expr_eval_push_step(mcx, &mut state, varstep)?;

        // Call the hash function. The final column's result lands in the
        // ExprState; intermediate values land in the iresult arena cell.
        let (resv, resn) = if i == (num_cols as usize) - 1 {
            (STATE_RESULT_CELL, STATE_RESULT_CELL)
        } else {
            let c = iresult_cell.expect("intermediate hash column needs iresult");
            (c, c)
        };
        // NEXT32 opcodes read the running hash from the shared accumulator cell
        // (the C `iresult->value`, aliased by every intermediate step's
        // resvalue). In the owned model that accumulator IS `iresult_cell`, the
        // cell the intermediate steps write to; carry its id so NEXT32 can read
        // the prior column's hash back. Present exactly when combining is needed
        // (FIRSTs ignore it).
        let iresult = if need_iresult { iresult_cell } else { None };
        let hashstep = ExprEvalStep {
            opcode,
            resvalue: resv,
            resnull: resn,
            d: ExprEvalStepData::HashDatum {
                finfo: Some(finfo),
                fcinfo_data: Some(fcinfo),
                // The owned model re-resolves the function at call time from
                // finfo.fn_oid (the fmgr seam returns no typed PGFunction); the
                // typed fn_addr stays None.
                fn_addr: None,
                arg_cell,
                jumpdone: -1,
                iresult,
            },
        };
        core::expr_eval_push_step(mcx, &mut state, hashstep)?;

        // subsequent attnums must be combined with the previous
        opcode = ExprEvalOp::EEOP_HASHDATUM_NEXT32;
    }

    push_done_return(mcx, &mut state)?;
    exec_ready_expr(&mut state)?;
    Ok(::mcx::alloc_in(mcx, state)?)
}

// ===========================================================================
// ExecBuildHash32Expr (execExpr.c:4302) — hash a list of arbitrary expressions.
// ===========================================================================

/// `ExecBuildHash32Expr(desc, ops, hashfunc_oids, collations, hash_exprs,
/// opstrict, parent, init_value, keep_nulls)` (execExpr.c:4302) — like
/// [`exec_build_hash32_from_attrs`] but hashes arbitrary expressions
/// (`hash_exprs`) rather than tuple attributes: it runs `ExecCreateExprSetupSteps`
/// over the expression list, then per expression recurses the value into the
/// hash fcinfo's arg 0, looks up the hash function (`fmgr_info(hashfunc_oids[i])`),
/// and emits the strict/non-strict `HASHDATUM_FIRST`/`_NEXT32` call, finally
/// fixing up the null-skip jumps.
///
/// The hash-arg cell (`&fcinfo->args[0]`) is the F0-modeled `HashDatum.arg_cell`
/// the per-expr recursion evaluates into; `ExecCreateExprSetupSteps` /
/// `ExecInitExprRec` are the sibling-shared `execExpr_core` spine
/// (`exec_create_expr_setup_steps_list` / `exec_init_expr_rec`), and
/// `fmgr_info(hashfunc_oids[i])` crosses the fmgr seam.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
pub fn exec_build_hash32_expr<'mcx>(
    mcx: Mcx<'mcx>,
    es_link: ::nodes::execnodes::EStateLink,
    desc: &TupleDescData<'mcx>,
    ops: TupleSlotKind,
    hashfunc_oids: &[Oid],
    collations: &[Oid],
    hash_exprs: &[Expr<'mcx>],
    opstrict: &[bool],
    init_value: u32,
    keep_nulls: bool,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let _ = (desc, ops);
    let num_exprs = hash_exprs.len() as i32;
    debug_assert_eq!(num_exprs as usize, collations.len());

    let mut state = make_expr_state(mcx)?;
    // C `ExecBuildHash32Expr(..., PlanState *parent, ...)` sets `state->parent =
    // parent`, through which a hash-key SubPlan reaches `parent->state ->
    // es_subplanstates`. Stamp the non-owning EState back-link so a correlated
    // SubPlan embedded in a hash key (`a = (SELECT ...)`) finds its parent plan.
    state.es_link = Some(es_link);

    // Insert setup steps as needed: ExecCreateExprSetupSteps(state, (Node *)
    // hash_exprs) — the FETCHSOME deform prescan over the expression list (the
    // core-owned expr_setup_walker spine, shared with ExecInitQual).
    core::exec_create_expr_setup_steps_list(mcx, &mut state, hash_exprs)?;

    // Intermediate hash slot, needed only when more than one value is combined.
    let need_iresult = (num_exprs as i64) + ((init_value != 0) as i64) > 1;
    let iresult_cell = if need_iresult {
        Some(new_result_cell(mcx, &mut state)?)
    } else {
        None
    };

    // init_value handling: choose the FIRST[_STRICT] vs NEXT32[_STRICT] base
    // opcodes (NEXT32 when seeded, so the SET_INITVAL is not overwritten).
    let mut strict_opcode = ExprEvalOp::EEOP_HASHDATUM_FIRST_STRICT;
    let mut opcode = ExprEvalOp::EEOP_HASHDATUM_FIRST;
    if init_value != 0 {
        let (resv, resn) = if num_exprs > 0 {
            let c = iresult_cell.expect("init_value with num_exprs>0 needs iresult");
            (c, c)
        } else {
            (STATE_RESULT_CELL, STATE_RESULT_CELL)
        };
        let initstep = ExprEvalStep {
            opcode: ExprEvalOp::EEOP_HASHDATUM_SET_INITVAL,
            resvalue: resv,
            resnull: resn,
            d: ExprEvalStepData::HashDatumInitValue {
                init_value: Datum::from_u32(init_value),
            },
        };
        core::expr_eval_push_step(mcx, &mut state, initstep)?;
        strict_opcode = ExprEvalOp::EEOP_HASHDATUM_NEXT32_STRICT;
        opcode = ExprEvalOp::EEOP_HASHDATUM_NEXT32;
    }

    let mut adjust_jumps: ::mcx::PgVec<'mcx, usize> =
        ::mcx::vec_with_capacity_in(mcx, num_exprs.max(0) as usize)?;

    for (i, expr) in hash_exprs.iter().enumerate() {
        let inputcollid = collations[i];
        let funcid = hashfunc_oids[i];

        // finfo = palloc0(sizeof(FmgrInfo)); fmgr_info(funcid, finfo).
        let finfo = ::mcx::alloc_in(mcx, fmgr_seam::fmgr_info::call(mcx, funcid)?)?;
        let fcinfo = init_fcinfo(mcx, inputcollid)?;

        // Build the steps to evaluate the hash function's argument so the value
        // is stored in the 0th argument of the hash func (owned `arg_cell`).
        let arg_cell = new_result_cell(mcx, &mut state)?;
        core::exec_init_expr_rec(mcx, expr, &mut state, arg_cell)?;

        // Final expr's result lands in the state; intermediate ones in iresult.
        let (resv, resn) = if i == (num_exprs as usize) - 1 {
            (STATE_RESULT_CELL, STATE_RESULT_CELL)
        } else {
            let c = iresult_cell.expect("intermediate hash expr needs iresult");
            (c, c)
        };
        // NEXT32 reads the running hash from the shared accumulator cell
        // (`iresult_cell`), mirroring C's aliased `iresult->value`.
        let iresult = if need_iresult { iresult_cell } else { None };

        // strict opcode iff the hash func is strict and we are not keeping NULLs.
        let step_opcode = if opstrict[i] && !keep_nulls {
            strict_opcode
        } else {
            opcode
        };
        let hashstep = ExprEvalStep {
            opcode: step_opcode,
            resvalue: resv,
            resnull: resn,
            d: ExprEvalStepData::HashDatum {
                finfo: Some(finfo),
                fcinfo_data: Some(fcinfo),
                fn_addr: None,
                arg_cell,
                jumpdone: -1,
                iresult,
            },
        };
        core::expr_eval_push_step(mcx, &mut state, hashstep)?;
        adjust_jumps.push((state.steps_len - 1) as usize);

        // For subsequent keys combine with the previous hashes.
        strict_opcode = ExprEvalOp::EEOP_HASHDATUM_NEXT32_STRICT;
        opcode = ExprEvalOp::EEOP_HASHDATUM_NEXT32;
    }

    // adjust jump targets: each FIRST/NEXT32[_STRICT] step jumps past the end.
    let jump_target = state.steps_len;
    if let Some(steps) = state.steps.as_mut() {
        for &j in adjust_jumps.iter() {
            if let ExprEvalStepData::HashDatum { jumpdone, .. } = &mut steps[j].d {
                debug_assert_eq!(*jumpdone, -1);
                *jumpdone = jump_target;
            }
        }
    }

    push_done_return(mcx, &mut state)?;
    exec_ready_expr(&mut state)?;
    Ok(::mcx::alloc_in(mcx, state)?)
}

// ===========================================================================
// ExecBuildGroupingEqual (execExpr.c:4467) — NOT-DISTINCT equality qual over
// keyColIdx columns of an inner (left) vs outer (right) tuple.
// ===========================================================================

/// `ExecBuildGroupingEqual(ldesc, rdesc, lops, rops, numCols, keyColIdx,
/// eqfunctions, collations, parent)` (execExpr.c:4467) — build an ExprState
/// (usable with `ExecQual`) that returns true iff the inner/outer tuples are NOT
/// DISTINCT across `numCols` columns (two nulls match; null vs non-null don't).
///
/// `numCols == 0` returns NULL (always-true, per the `ExecQual` special case).
/// Otherwise it emits inner+outer `FETCHSOME` deforms to the max attno, then —
/// from the last sort key backward — per column: object_aclcheck + `fmgr_info`
/// of the equality function, an `EEOP_INNER_VAR` and `EEOP_OUTER_VAR` into the
/// 2-arg fcinfo, an `EEOP_NOT_DISTINCT` call, and an `EEOP_QUAL` short-circuit;
/// finally it fixes up the QUAL jumps and appends `EEOP_DONE_RETURN`.
///
/// `None` return ≙ the C `numCols == 0` NULL. The deform + jump-fixup +
/// DONE_RETURN are this family's own logic and are emitted; the per-column
/// `object_aclcheck`/`fmgr_info`/`InitFunctionCallInfoData` (catalog + fmgr) and
/// the `EEOP_*_VAR -> &fcinfo->args[N]` arg-cell wiring (the `Func` payload's
/// `arg_cells`, which IS modeled, but the fcinfo `args[]` is trimmed away in this
/// crate's `types-nodes` view) are the genuine cross-owner / keystone-gap pieces.
#[allow(clippy::too_many_arguments)]
pub fn exec_build_grouping_equal<'mcx>(
    mcx: Mcx<'mcx>,
    ldesc: &TupleDescData<'mcx>,
    rdesc: &TupleDescData<'mcx>,
    lops: TupleSlotKind,
    rops: TupleSlotKind,
    num_cols: i32,
    key_col_idx: &[AttrNumber],
    eqfunctions: &[Oid],
    collations: &[Oid],
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    // When no columns are compared, the result is always true (ExecQual special
    // case): return NULL.
    if num_cols == 0 {
        return Ok(None);
    }

    // state->expr = NULL; state->flags = EEO_FLAG_IS_QUAL; state->parent=parent.
    let mut state = make_expr_state(mcx)?;
    state.flags = EEO_FLAG_IS_QUAL;

    // compute max needed attribute
    let mut maxatt: i32 = -1;
    for natt in 0..num_cols as usize {
        let attno = key_col_idx[natt] as i32;
        if attno > maxatt {
            maxatt = attno;
        }
    }
    debug_assert!(maxatt >= 0);

    // push inner + outer deform steps
    push_fetchsome(
        mcx,
        &mut state,
        ExprEvalOp::EEOP_INNER_FETCHSOME,
        maxatt as AttrNumber,
        ldesc,
        lops,
    )?;
    push_fetchsome(
        mcx,
        &mut state,
        ExprEvalOp::EEOP_OUTER_FETCHSOME,
        maxatt as AttrNumber,
        rdesc,
        rops,
    )?;

    // Per-column comparison (from the last/least-significant key backward —
    // most likely to differ for sorted input). Each column looks up the
    // equality function (ACL_EXECUTE + fmgr_info), emits the inner/outer Var
    // steps writing fcinfo args[0]/[1] (owned arena cells), a NOT_DISTINCT call,
    // and a QUAL short-circuit.
    let mut adjust_jumps: ::mcx::PgVec<'mcx, usize> =
        ::mcx::vec_with_capacity_in(mcx, num_cols.max(0) as usize)?;
    let mut natt = num_cols;
    while {
        natt -= 1;
        natt >= 0
    } {
        let attno = key_col_idx[natt as usize] as i32;
        let latt_typid = ldesc.attr((attno - 1) as usize).atttypid;
        let ratt_typid = rdesc.attr((attno - 1) as usize).atttypid;
        let foid = eqfunctions[natt as usize];
        let collid = collations[natt as usize];

        emit_eq_column(
            mcx,
            &mut state,
            foid,
            collid,
            attno - 1,
            latt_typid,
            attno - 1,
            ratt_typid,
            &mut adjust_jumps,
        )?;
    }

    fixup_qual_jumps(&mut state, &adjust_jumps);
    push_done_return(mcx, &mut state)?;
    exec_ready_expr(&mut state)?;
    Ok(Some(::mcx::alloc_in(mcx, state)?))
}

// ===========================================================================
// ExecBuildParamSetEqual (execExpr.c:4626) — equality qual over a fixed set of
// param expressions (one fcinfo per attno, inner vs outer, NULLs equal).
// ===========================================================================

/// `ExecBuildParamSetEqual(desc, lops, rops, eqfunctions, collations,
/// param_exprs, parent)` (execExpr.c:4626) — build an ExprState (usable with
/// `ExecQual`) that returns true iff the inner/outer tuples are equal across all
/// `list_length(param_exprs)` columns, treating NULLs as equal. Structurally
/// identical to [`exec_build_grouping_equal`] but it always compares the first
/// `maxatt` attributes (one per param expr) front-to-back and never returns NULL.
///
/// Same cross-owner / keystone-gap blockers as [`exec_build_grouping_equal`]
/// (object_aclcheck + fmgr_info per column; the `EEOP_*_VAR -> &fcinfo->args[N]`
/// arg cells against the trimmed `FunctionCallInfoBaseData`).
#[allow(clippy::too_many_arguments)]
pub fn exec_build_param_set_equal<'mcx>(
    mcx: Mcx<'mcx>,
    desc: &TupleDescData<'mcx>,
    lops: TupleSlotKind,
    rops: TupleSlotKind,
    eqfunctions: &[Oid],
    collations: &[Oid],
    param_exprs: &[Expr<'mcx>],
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    // state->expr = NULL; state->flags = EEO_FLAG_IS_QUAL.
    let mut state = make_expr_state(mcx)?;
    state.flags = EEO_FLAG_IS_QUAL;

    let maxatt = param_exprs.len() as AttrNumber;

    // push inner + outer deform steps (both over `desc`, with lops/rops).
    push_fetchsome(
        mcx,
        &mut state,
        ExprEvalOp::EEOP_INNER_FETCHSOME,
        maxatt,
        desc,
        lops,
    )?;
    push_fetchsome(
        mcx,
        &mut state,
        ExprEvalOp::EEOP_OUTER_FETCHSOME,
        maxatt,
        desc,
        rops,
    )?;

    // Per-column comparison, front-to-back over every attno (one per param
    // expr). Same per-column body as ExecBuildGroupingEqual; both Var steps read
    // the same `desc` attribute (inner vs outer slot).
    let mut adjust_jumps: ::mcx::PgVec<'mcx, usize> =
        ::mcx::vec_with_capacity_in(mcx, maxatt.max(0) as usize)?;
    for attno in 0..maxatt as i32 {
        let att_typid = desc.attr(attno as usize).atttypid;
        let foid = eqfunctions[attno as usize];
        let collid = collations[attno as usize];

        emit_eq_column(
            mcx,
            &mut state,
            foid,
            collid,
            attno,
            att_typid,
            attno,
            att_typid,
            &mut adjust_jumps,
        )?;
    }

    fixup_qual_jumps(&mut state, &adjust_jumps);
    push_done_return(mcx, &mut state)?;
    exec_ready_expr(&mut state)?;
    Ok(::mcx::alloc_in(mcx, state)?)
}

// ===========================================================================
// ExecBuildAggTrans / ExecBuildAggTransCall local helpers
// ===========================================================================

/// `expr_setup_walker(node, info)` restricted to the attnum accumulation the
/// agg-trans deform prefix needs (mirror of `execExpr_core::expr_setup_walker`'s
/// child-link descent; that walker is module-private to core). Descends the same
/// modeled `Expr` child links and records the highest inner/outer/scan attnum;
/// Aggref/WindowFunc/GroupingFunc argument lists are NOT descended (their args
/// are compiled separately), matching C.
fn agg_setup_walker(node: &Expr, info: &mut LastAttnumInfo) {
    match node.expr_tag() {
        etag::T_Var => {
            let variable = node.as_var().expect("Var");
            let attnum = variable.varattno;
            match variable.varno {
                INNER_VAR => info.last_inner = info.last_inner.max(attnum),
                OUTER_VAR => info.last_outer = info.last_outer.max(attnum),
                _ => info.last_scan = info.last_scan.max(attnum),
            }
        }
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
        etag::T_RelabelType => agg_descend_opt(node.expect_relabeltype().arg.as_deref(), info),
        etag::T_CollateExpr => agg_descend_opt(node.expect_collateexpr().arg.as_deref(), info),
        etag::T_CoerceViaIO => agg_descend_opt(node.expect_coerceviaio().arg.as_deref(), info),
        etag::T_ConvertRowtypeExpr => {
            agg_descend_opt(node.expect_convertrowtypeexpr().arg.as_deref(), info)
        }
        etag::T_FieldSelect => agg_descend_opt(node.expect_fieldselect().arg.as_deref(), info),
        etag::T_NamedArgExpr => agg_descend_opt(node.expect_namedargexpr().arg.as_deref(), info),
        etag::T_NullTest => agg_descend_opt(node.expect_nulltest().arg.as_deref(), info),
        etag::T_BooleanTest => agg_descend_opt(node.expect_booleantest().arg.as_deref(), info),
        etag::T_CoerceToDomain => {
            agg_descend_opt(node.expect_coercetodomain().arg.as_deref(), info)
        }
        etag::T_ArrayCoerceExpr => {
            agg_descend_opt(node.expect_arraycoerceexpr().arg.as_deref(), info)
        }
        etag::T_FuncExpr => agg_descend_list(&node.expect_funcexpr().args, info),
        etag::T_OpExpr | etag::T_DistinctExpr | etag::T_NullIfExpr => {
            let e = node
                .as_opexpr()
                .or_else(|| node.as_distinctexpr())
                .or_else(|| node.as_nullifexpr())
                .expect("OpExpr/DistinctExpr/NullIfExpr");
            agg_descend_list(&e.args, info)
        }
        etag::T_BoolExpr => agg_descend_list(&node.expect_boolexpr().args, info),
        etag::T_CoalesceExpr => agg_descend_list(&node.expect_coalesceexpr().args, info),
        etag::T_MinMaxExpr => agg_descend_list(&node.expect_minmaxexpr().args, info),
        etag::T_ArrayExpr => agg_descend_list(&node.expect_arrayexpr().elements, info),
        etag::T_CaseExpr => {
            let e = node.expect_caseexpr();
            agg_descend_opt(e.arg.as_deref(), info);
            for w in &e.args {
                agg_descend_opt(w.expr.as_deref(), info);
                agg_descend_opt(w.result.as_deref(), info);
            }
            agg_descend_opt(e.defresult.as_deref(), info);
        }
        _ => {}
    }
}

fn agg_descend_opt(node: Option<&Expr>, info: &mut LastAttnumInfo) {
    if let Some(n) = node {
        agg_setup_walker(n, info);
    }
}

fn agg_descend_list(list: &[Expr], info: &mut LastAttnumInfo) {
    for n in list {
        agg_setup_walker(n, info);
    }
}

/// `ExecPushExprSetupSteps(state, info)` for the agg-trans prefix — emit one
/// `EEOP_{INNER,OUTER,SCAN}_FETCHSOME` deform step per referenced input slot
/// (mirror of `execExpr_core::exec_push_expr_setup_steps`; that function is
/// module-private to core). The agg-trans deform never references the
/// MULTIEXPR-subplan setup list, so only the three slot deforms are emitted.
fn push_setup_steps<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    info: &LastAttnumInfo,
) -> PgResult<()> {
    for (opcode, last_var) in [
        (ExprEvalOp::EEOP_INNER_FETCHSOME, info.last_inner),
        (ExprEvalOp::EEOP_OUTER_FETCHSOME, info.last_outer),
        (ExprEvalOp::EEOP_SCAN_FETCHSOME, info.last_scan),
    ] {
        if last_var > 0 {
            // No known descriptor here (the agg input slot's tupdesc is not
            // threaded into this builder), so the deform stays non-fixed,
            // matching C's `!parent`-shaped ExecComputeSlotInfo path. Built
            // inline (mirror of execExpr_core::exec_push_expr_setup_steps) with
            // `known_desc: None, kind: None`, then run through the file-local
            // `exec_compute_slot_info` which keeps the (non-fixed) step.
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
                core::expr_eval_push_step(mcx, state, scratch)?;
            }
        }
    }
    Ok(())
}

/// Borrow the `Aggref` of `aggstate->pertrans[transno]`.
fn aggref_of<'a, 'mcx>(aggstate: &'a AggStateData<'mcx>, transno: usize) -> &'a Aggref<'mcx> {
    aggstate
        .pertrans
        .as_ref()
        .expect("ExecBuildAggTrans: aggstate->pertrans is NULL")[transno]
        .aggref
        .as_ref()
        .expect("ExecBuildAggTrans: pertrans->aggref is NULL")
}

/// Clone `pertrans->aggref->args[i]->expr` (the i-th aggregated-argument source
/// expression). The C uses the node in place (read-only); cloning the small
/// plan-node subtree sidesteps the read-borrow-vs-`&mut state` conflict in the
/// recursion. The deep copy goes through `Expr::clone_in` (not a shallow
/// `.clone()`): an aggregate argument can be a `SubLink`-turned-`SubPlan`
/// (e.g. `sum((SELECT ...))`), whose context-allocated children only the
/// `clone_in` path can deep-copy — the derived `Clone` panics on them.
fn arg_tle_expr_clone<'mcx>(
    aggstate: &AggStateData<'mcx>,
    transno: usize,
    i: usize,
    mcx: Mcx<'mcx>,
) -> PgResult<Expr<'mcx>> {
    let aggref = aggref_of(aggstate, transno);
    let args = aggref
        .args
        .as_ref()
        .expect("ExecBuildAggTrans: pertrans->aggref->args is NULL");
    args[i]
        .expr
        .as_deref()
        .expect("ExecBuildAggTrans: aggregated-arg TargetEntry.expr is NULL")
        .clone_in(mcx)
}

/// Allocate a single-element `PgVec<ResultCellId>` (the strict-input-check's
/// `arg_cells` for a one-argument transition function).
fn single_cell_vec<'mcx>(mcx: Mcx<'mcx>, cell: ResultCellId) -> PgResult<PgVec<'mcx, ResultCellId>> {
    let mut v = ::mcx::vec_with_capacity_in(mcx, 1)?;
    v.push(cell);
    Ok(v)
}

/// `phase->numsets` for the `phase` index into `aggstate->phases`.
fn phase_numsets(aggstate: &AggStateData<'_>, phase: i32) -> i32 {
    aggstate
        .phases
        .as_ref()
        .expect("ExecBuildAggTrans: aggstate->phases is NULL")[phase as usize]
        .numsets
}

/// The per-trans scalar predicates `ExecBuildAggTrans` / `ExecBuildAggTransCall`
/// read off the nodeAgg-owned `AggStatePerTransData` (snapshotted so the borrow
/// is not held across the `&mut state` pushes). `transfn_strict` /
/// `deserialfn_strict` are `fcinfo->flinfo->fn_strict` for the transfn /
/// deserialfn (the FmgrInfo bound on the pertrans).
struct PertransPred {
    has_aggfilter: bool,
    aggsortrequired: bool,
    num_inputs: i32,
    num_trans_inputs: i32,
    num_sort_cols: i32,
    num_distinct_cols: i32,
    aggref_args_len: usize,
    deserialfn_valid: bool,
    deserialfn_strict: bool,
    /// `&pertrans->deserialfn` — the resolved deserialfn FmgrInfo (built by
    /// `build_pertrans_for_aggref`) the C `ds_fcinfo->flinfo` points at. The
    /// interpreter re-dispatches by `deserialfn.fn_oid` under the deserialfn
    /// frame's collation; threaded onto the `ds_fcinfo` frame at build time.
    deserialfn: FmgrInfo,
    transfn_strict: bool,
    transtype_by_val: bool,
    init_value_is_null: bool,
}

/// Snapshot the [`PertransPred`] for `aggstate->pertrans[transno]`.
fn pertrans_pred(aggstate: &AggStateData<'_>, transno: usize) -> PertransPred {
    let pt = &aggstate
        .pertrans
        .as_ref()
        .expect("ExecBuildAggTrans: aggstate->pertrans is NULL")[transno];
    let aggref = pt
        .aggref
        .as_ref()
        .expect("ExecBuildAggTrans: pertrans->aggref is NULL");
    PertransPred {
        has_aggfilter: aggref.aggfilter.is_some(),
        aggsortrequired: pt.aggsortrequired,
        num_inputs: pt.num_inputs,
        num_trans_inputs: pt.num_trans_inputs,
        num_sort_cols: pt.num_sort_cols,
        num_distinct_cols: pt.num_distinct_cols,
        aggref_args_len: aggref.args.as_ref().map(|a| a.len()).unwrap_or(0),
        // OidIsValid(pertrans->deserialfn_oid)
        deserialfn_valid: pt.deserialfn_oid != ::types_core::InvalidOid,
        deserialfn_strict: pt.deserialfn.fn_strict,
        deserialfn: pt.deserialfn.clone(),
        // trans_fcinfo->flinfo->fn_strict
        transfn_strict: pt.transfn.fn_strict,
        transtype_by_val: pt.transtype_by_val,
        init_value_is_null: pt.init_value_is_null,
    }
}

// ===========================================================================
// Small local helpers
// ===========================================================================

/// Shallow-copy a scratch step for `ExprEvalPushStep` (the C reuses one stack
/// `scratch` and pushes a byte-copy each time; the owned step's payload is moved
/// per-push, so we rebuild the payload the caller just set).
fn clone_step<'mcx>(s: &ExprEvalStep<'mcx>) -> ExprEvalStep<'mcx> {
    ExprEvalStep {
        opcode: s.opcode,
        resvalue: s.resvalue,
        resnull: s.resnull,
        d: ExprEvalStepData::NoPayload,
    }
}

/// `fcinfo = palloc0(SizeForFunctionCallInfo(nargs));
/// InitFunctionCallInfoData(*fcinfo, finfo, nargs, inputcollid, NULL, NULL)`.
/// The per-arg cells the C `args[]` aliases are modeled as the step payload's
/// `arg_cell`/`arg_cells` arena ids (gathered into `fcinfo->args` by the
/// interpreter), so the call frame carries no args at init. #296: the widened
/// frame records `inputcollid` as `fcinfo->fncollation`, surviving to call
/// time; `finfo` is carried by the step payload separately (these agg/distinct
/// frames re-resolve at dispatch), so `flinfo` stays NULL here.
fn init_fcinfo<'mcx>(
    mcx: Mcx<'mcx>,
    inputcollid: Oid,
) -> PgResult<PgBox<'mcx, ::nodes::fmgr::FunctionCallInfoBaseData<'mcx>>> {
    ::mcx::alloc_in(
        mcx,
        ::nodes::fmgr::FunctionCallInfoBaseData {
            fncollation: inputcollid,
            ..Default::default()
        },
    )
}

/// Push the terminating `EEOP_DONE_RETURN` step (C: `scratch.resvalue = NULL;
/// scratch.resnull = NULL; scratch.opcode = EEOP_DONE_RETURN`). The owned NULL
/// result target is the (unused) `STATE_RESULT_CELL`.
fn push_done_return<'mcx>(mcx: Mcx<'mcx>, state: &mut ExprState<'mcx>) -> PgResult<()> {
    let step = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_DONE_RETURN,
        resvalue: STATE_RESULT_CELL,
        resnull: STATE_RESULT_CELL,
        d: ExprEvalStepData::NoPayload,
    };
    core::expr_eval_push_step(mcx, state, step)
}

/// Per-column ACL_EXECUTE check the two equality builders run before
/// `fmgr_info` (execExpr.c:4536-4542 / 4694-4700):
///   aclresult = object_aclcheck(ProcedureRelationId, foid, GetUserId(), ACL_EXECUTE);
///   if (aclresult != ACLCHECK_OK) aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(foid));
///   InvokeFunctionExecuteHook(foid);
/// then `fmgr_info(foid)` returning the populated `FmgrInfo`.
fn aclcheck_and_fmgr_info<'mcx>(mcx: Mcx<'mcx>, foid: Oid) -> PgResult<FmgrInfo> {
    let aclresult =
        aclchk::object_aclcheck::call(PROCEDURE_RELATION_ID, foid, miscinit::get_user_id::call(), ACL_EXECUTE)?;
    if aclresult != ACLCHECK_OK {
        let name = lsyscache::get_func_name::call(mcx, foid)?.map(|s| s.to_string());
        aclchk::aclcheck_error::call(aclresult, OBJECT_FUNCTION, name)?;
    }

    // InvokeFunctionExecuteHook(foid) — fires only when an object_access_hook is
    // installed. The general OAT_FUNCTION_EXECUTE invocation is owned by
    // objectaccess and has no seam exported yet; mirror-PG-and-panic on the
    // (rare) hook-present path, no-op otherwise.
    if objectaccess::object_access_hook_present::call() {
        panic!(
            "execExpr-domain-agg: InvokeFunctionExecuteHook(foid) (OAT_FUNCTION_EXECUTE \
             object-access hook, owned by backend-catalog-objectaccess) has no seam exported; \
             reached only when an object_access_hook is installed"
        );
    }

    // finfo = palloc0(sizeof(FmgrInfo)); fmgr_info(foid, finfo);
    // fmgr_info_set_expr(NULL, finfo);
    fmgr_seam::fmgr_info::call(mcx, foid)
}

/// One column of `ExecBuildGroupingEqual` / `ExecBuildParamSetEqual`
/// (execExpr.c:4530-4588 / 4688-4746): ACL_EXECUTE + `fmgr_info(foid)`, a 2-arg
/// `InitFunctionCallInfoData`, then the
///   EEOP_INNER_VAR -> &fcinfo->args[0],  EEOP_OUTER_VAR -> &fcinfo->args[1],
///   EEOP_NOT_DISTINCT (the eq-func call),  EEOP_QUAL (short-circuit)
/// steps; records the QUAL step index in `adjust_jumps` for later fixup. The
/// owned `Func.arg_cells` are the two arena cells the Var steps write (the C
/// `&fcinfo->args[0]/[1]`), gathered by the interpreter before the call.
#[allow(clippy::too_many_arguments)]
fn emit_eq_column<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    foid: Oid,
    collid: Oid,
    left_attnum: i32,
    left_vartype: Oid,
    right_attnum: i32,
    right_vartype: Oid,
    adjust_jumps: &mut ::mcx::PgVec<'mcx, usize>,
) -> PgResult<()> {
    // Check permission to call function + look it up.
    let finfo = ::mcx::alloc_in(mcx, aclcheck_and_fmgr_info(mcx, foid)?)?;
    let fcinfo = init_fcinfo(mcx, collid)?;

    // left arg: EEOP_INNER_VAR -> &fcinfo->args[0]
    let arg0 = new_result_cell(mcx, state)?;
    let leftstep = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_INNER_VAR,
        resvalue: arg0,
        resnull: arg0,
        d: ExprEvalStepData::Var {
            attnum: left_attnum,
            vartype: left_vartype,
            varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
        },
    };
    core::expr_eval_push_step(mcx, state, leftstep)?;

    // right arg: EEOP_OUTER_VAR -> &fcinfo->args[1]
    let arg1 = new_result_cell(mcx, state)?;
    let rightstep = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_OUTER_VAR,
        resvalue: arg1,
        resnull: arg1,
        d: ExprEvalStepData::Var {
            attnum: right_attnum,
            vartype: right_vartype,
            varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
        },
    };
    core::expr_eval_push_step(mcx, state, rightstep)?;

    // evaluate distinctness: EEOP_NOT_DISTINCT, result into state->resvalue.
    let mut arg_cells: ::mcx::PgVec<'mcx, ResultCellId> = ::mcx::vec_with_capacity_in(mcx, 2)?;
    arg_cells.push(arg0);
    arg_cells.push(arg1);
    let ndstep = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_NOT_DISTINCT,
        resvalue: STATE_RESULT_CELL,
        resnull: STATE_RESULT_CELL,
        d: ExprEvalStepData::Func {
            finfo: Some(finfo),
            fcinfo_data: Some(fcinfo),
            arg_cells: Some(arg_cells),
            // re-resolved at call time from finfo.fn_oid (no typed PGFunction).
            fn_addr: None,
            nargs: 2,
            make_ro: false,
        },
    };
    core::expr_eval_push_step(mcx, state, ndstep)?;

    // then emit EEOP_QUAL to detect if result is false (or null).
    let qualstep = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_QUAL,
        resvalue: STATE_RESULT_CELL,
        resnull: STATE_RESULT_CELL,
        d: ExprEvalStepData::QualExpr { jumpdone: -1 },
    };
    core::expr_eval_push_step(mcx, state, qualstep)?;
    adjust_jumps.push((state.steps_len - 1) as usize);
    Ok(())
}

/// Resolve every recorded `EEOP_QUAL` step's `jumpdone` to the end of the
/// program (execExpr.c: the `foreach(lc, adjust_jumps)` fixup).
fn fixup_qual_jumps<'mcx>(state: &mut ExprState<'mcx>, adjust_jumps: &[usize]) {
    let jump_target = state.steps_len;
    if let Some(steps) = state.steps.as_mut() {
        for &j in adjust_jumps {
            debug_assert!(matches!(steps[j].opcode, ExprEvalOp::EEOP_QUAL));
            if let ExprEvalStepData::QualExpr { jumpdone } = &mut steps[j].d {
                debug_assert_eq!(*jumpdone, -1);
                *jumpdone = jump_target;
            }
        }
    }
}

/// `get_typlen(typid)` (lsyscache.c) — the type's `typlen`, used by the domain
/// MAKE_READONLY decision (`typlen == -1` means a possibly-expanded varlena).
fn lsyscache_get_typlen(typid: Oid) -> PgResult<i16> {
    lsyscache_seams::get_typlen::call(typid)
}

// ===========================================================================
// Hashed-subplan combining helpers (nodeSubplan.c) — unchanged from scaffold.
// ===========================================================================

/// Classify `subplan->testexpr` for the hashed-subplan init path
/// (`IsA(OpExpr)` / `is_andclause` / else) (nodeSubplan.c:922-938).
pub fn classify_testexpr(node: &SubPlanState<'_>) -> CombiningTestExpr {
    let subplan = node
        .subplan
        .as_ref()
        .expect("buildSubPlanHash: SubPlanState.subplan is NULL");
    let testexpr = subplan
        .testexpr
        .as_ref()
        .expect("buildSubPlanHash: hashable subplan->testexpr is NULL");

    let texpr: &Expr = testexpr;
    match texpr.expr_tag() {
        etag::T_OpExpr => CombiningTestExpr::SingleOp,
        etag::T_BoolExpr if texpr.expect_boolexpr().boolop == AND_EXPR => {
            CombiningTestExpr::AndClause {
                ncols: texpr.expect_boolexpr().args.len() as i32,
            }
        }
        _ => CombiningTestExpr::Unrecognized {
            node_tag: node_tag_of(texpr),
        },
    }
}

/// `nodeTag(node)` for the `else` arm of `classify_testexpr` — purely
/// diagnostic; the planner only builds `OpExpr`/AND-clause hashable testexprs.
fn node_tag_of(expr: &Expr) -> i32 {
    let _ = expr;
    -1
}

/// Resolve combining-operator `idx` of the testexpr's `oplist`
/// (nodeSubplan.c:980-1001): `opfuncid`, RHS-type equality op, hash functions,
/// `inputcollid`.
pub fn resolve_combining_op(node: &SubPlanState<'_>, idx: usize) -> PgResult<CombiningOpInfo> {
    let subplan = node
        .subplan
        .as_ref()
        .expect("buildSubPlanHash: SubPlanState.subplan is NULL");
    let testexpr = subplan
        .testexpr
        .as_ref()
        .expect("buildSubPlanHash: hashable subplan->testexpr is NULL");

    let opexpr = oplist_op(testexpr, idx);
    let opfuncid = opexpr.opfuncid;

    let (_lhs_eq_oper, rhs_eq_oper) = lsyscache::get_compatible_hash_operators::call(opexpr.opno)?
        .ok_or_else(|| {
            PgError::error(format!(
                "could not find compatible hash operator for operator {}",
                opexpr.opno
            ))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        })?;

    let rhs_eq_funcoid = lsyscache::get_opcode::call(rhs_eq_oper)?;

    let (left_hashfn, right_hashfn) =
        lsyscache::get_op_hash_functions::call(opexpr.opno)?.ok_or_else(|| {
            PgError::error(format!(
                "could not find hash function for hash operator {}",
                opexpr.opno
            ))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        })?;

    let inputcollid = opexpr.inputcollid;

    Ok(CombiningOpInfo {
        opfuncid,
        rhs_eq_funcoid,
        left_hashfn,
        right_hashfn,
        inputcollid,
    })
}

/// `lfirst_node(OpExpr, list_nth_cell(oplist, idx))`.
fn oplist_op<'a, 'mcx>(testexpr: &'a Expr<'mcx>, idx: usize) -> &'a OpExpr<'mcx> {
    let elem = match testexpr.expr_tag() {
        etag::T_OpExpr => {
            assert!(idx == 0, "oplist index {idx} out of range for single OpExpr");
            return testexpr.as_opexpr().expect("OpExpr");
        }
        etag::T_BoolExpr if testexpr.expect_boolexpr().boolop == AND_EXPR => testexpr
            .expect_boolexpr()
            .args
            .get(idx)
            .unwrap_or_else(|| panic!("oplist index {idx} out of range for and-clause args")),
        _ => panic!("resolve_combining_op: subplan->testexpr is neither OpExpr nor AND-clause BoolExpr: {testexpr:?}"),
    };
    match elem.expr_tag() {
        etag::T_OpExpr => elem.as_opexpr().expect("OpExpr"),
        _ => panic!("resolve_combining_op: and-clause arg {idx} is not an OpExpr: {elem:?}"),
    }
}

/// Build the hashed-subplan projections + the `lhs_hash_expr` / `cur_eq_comp`
/// expression states (nodeSubplan.c:964-978, 1009-1053): `ExecTypeFromTL` /
/// `ExecBuildProjectionInfo` / `ExecBuildHash32FromAttrs` /
/// `ExecBuildGroupingEqual` over the raw testexpr tree.
pub fn build_hash_projections_and_exprs<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    lhs_hash_funcs: &[FmgrInfo],
    cross_eq_funcoids: &[Oid],
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let ncols = node.numCols;

    // Build the lefthand / righthand tlists from each combining OpExpr's two
    // args (C nodeSubplan.c:959-978): makeTargetEntry(expr, i, NULL, false).
    // The raw `subplan->testexpr` Expr tree is read through `oplist_op`.
    let subplan = node
        .subplan
        .as_ref()
        .expect("build_hash_projections_and_exprs: SubPlanState.subplan is NULL");
    let testexpr = subplan
        .testexpr
        .as_ref()
        .expect("build_hash_projections_and_exprs: hashable subplan->testexpr is NULL")
        .clone_in(mcx)?;

    let mut lefttlist: PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> =
        vec_with_capacity_in(mcx, ncols as usize)?;
    let mut righttlist: PgVec<'mcx, ::nodes::primnodes::TargetEntry<'mcx>> =
        vec_with_capacity_in(mcx, ncols as usize)?;

    for i in 1..=ncols {
        let opexpr = oplist_op(&testexpr, (i - 1) as usize);
        debug_assert!(opexpr.args.len() == 2);

        // expr = (Expr *) linitial(opexpr->args);
        let lexpr = opexpr.args[0].clone_in(mcx)?;
        lefttlist.push(nodes_core::makefuncs::make_target_entry(
            mcx,
            lexpr,
            i as AttrNumber,
            None,
            false,
        )?);

        // expr = (Expr *) lsecond(opexpr->args);
        let rexpr = opexpr.args[1].clone_in(mcx)?;
        righttlist.push(nodes_core::makefuncs::make_target_entry(
            mcx,
            rexpr,
            i as AttrNumber,
            None,
            false,
        )?);
    }

    let key_col_idx = node
        .keyColIdx
        .as_deref()
        .expect("build_hash_projections_and_exprs: keyColIdx not set")
        .to_vec();
    let tab_collations = node
        .tab_collations
        .as_deref()
        .expect("build_hash_projections_and_exprs: tab_collations not set")
        .to_vec();

    // tupDescLeft = ExecTypeFromTL(lefttlist);
    let tup_desc_left = execTuples_seams::exec_type_from_tl::call(mcx, &lefttlist)?
        .expect("ExecTypeFromTL returned NULL descriptor for lefttlist");
    // slot = ExecInitExtraTupleSlot(estate, tupDescLeft, &TTSOpsVirtual);
    let slot_left = execTuples_seams::exec_init_extra_tuple_slot::call(
        estate,
        Some(::mcx::alloc_in(mcx, tup_desc_left.clone_in(mcx)?)?),
        TupleSlotKind::Virtual,
    )?;
    // sstate->projLeft = ExecBuildProjectionInfo(lefttlist, NULL, slot, parent, NULL);
    // The lefthand projection's exprcontext is filled in later (the C "hack
    // alert!" — `sub_exec_project` retargets `pi_exprContext` at run time);
    // build with the innerecontext as a placeholder, then null it back out.
    let inner_ec = node
        .innerecontext
        .expect("build_hash_projections_and_exprs: innerecontext not set");
    let mut proj_left =
        core::exec_build_projection_info_impl(estate, &lefttlist, inner_ec, Some(slot_left), None)?;
    proj_left.pi_exprContext = None;
    crate::execExpr_func_subscript::store_proj_carrier(
        &mut node.projLeft,
        crate::execExpr_func_subscript::ProjCarrier { proj: proj_left, resultslot: slot_left },
    );

    // sstate->descRight = tupDescRight = ExecTypeFromTL(righttlist);
    let tup_desc_right = execTuples_seams::exec_type_from_tl::call(mcx, &righttlist)?
        .expect("ExecTypeFromTL returned NULL descriptor for righttlist");
    // slot = ExecInitExtraTupleSlot(estate, tupDescRight, &TTSOpsVirtual);
    let slot_right = execTuples_seams::exec_init_extra_tuple_slot::call(
        estate,
        Some(::mcx::alloc_in(mcx, tup_desc_right.clone_in(mcx)?)?),
        TupleSlotKind::Virtual,
    )?;
    // sstate->projRight = ExecBuildProjectionInfo(righttlist, sstate->innerecontext,
    //                                             slot, sstate->planstate, NULL);
    let proj_right =
        core::exec_build_projection_info_impl(estate, &righttlist, inner_ec, Some(slot_right), None)?;
    crate::execExpr_func_subscript::store_proj_carrier(
        &mut node.projRight,
        crate::execExpr_func_subscript::ProjCarrier { proj: proj_right, resultslot: slot_right },
    );
    node.descRight = Some(tup_desc_right);

    // sstate->lhs_hash_expr = ExecBuildHash32FromAttrs(tupDescLeft, &TTSOpsVirtual,
    //     lhs_hash_funcs, sstate->tab_collations, sstate->numCols,
    //     sstate->keyColIdx, parent, 0);
    let lhs_hash_expr = exec_build_hash32_from_attrs(
        mcx,
        &tup_desc_left,
        TupleSlotKind::Virtual,
        lhs_hash_funcs,
        &tab_collations,
        ncols,
        &key_col_idx,
        0,
    )?;
    node.lhs_hash_expr = Some(lhs_hash_expr);

    // sstate->cur_eq_comp = ExecBuildGroupingEqual(tupDescLeft, tupDescRight,
    //     &TTSOpsVirtual, &TTSOpsMinimalTuple, ncols, sstate->keyColIdx,
    //     cross_eq_funcoids, sstate->tab_collations, parent);
    let cur_eq_comp = exec_build_grouping_equal(
        mcx,
        &tup_desc_left,
        node.descRight.as_deref().unwrap(),
        TupleSlotKind::Virtual,
        TupleSlotKind::MinimalTuple,
        ncols,
        &key_col_idx,
        cross_eq_funcoids,
        &tab_collations,
    )?;
    node.cur_eq_comp = cur_eq_comp;

    Ok(())
}
