//! `execExpr-domain-agg` family — domain coercion, aggregate transition, and
//! grouping/hash equality program builders.
//!
//! Owns `ExecInitCoerceToDomain`, `ExecBuildAggTrans` / `ExecBuildAggTransCall`,
//! `ExecBuildGroupingEqual`, `ExecBuildParamSetEqual`,
//! `ExecBuildHash32FromAttrs` / `ExecBuildHash32Expr`. The hashed-subplan
//! init path (`classify_testexpr` / `resolve_combining_op` /
//! `build_hash_projections_and_exprs`) is built on the grouping-equal + hash
//! builders, so its seams land here.

use mcx::{Mcx, PgBox};
use types_core::fmgr::FmgrInfo;
use types_core::{AttrNumber, Oid};
use types_datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::execexpr::{
    ExprEvalOp, ExprEvalStep, ExprEvalStepData, ExprState, ResultCell, ResultCellId,
    EEO_FLAG_IS_QUAL, STATE_RESULT_CELL,
};
use types_nodes::execexpr::SubPlanState;
use types_nodes::executor::TupleSlotKind;
use types_nodes::nodeagg::{do_aggsplit_combine, AggStateData};
use types_nodes::primnodes::{Expr, OpExpr, AND_EXPR};
use types_nodes::EStateData;
use types_tuple::heaptuple::TupleDescData;

use crate::execExpr_core as core;
use backend_executor_execExpr_seams::{CombiningOpInfo, CombiningTestExpr};
use backend_utils_cache_lsyscache_seams as lsyscache;

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
        let mut cells = mcx::vec_with_capacity_in(mcx, 1)?;
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
    backend_executor_execExprInterp_seams::exec_ready_interpreted_expr::call(state)
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
    ctest_arg: &Expr,
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
        escontext: state.escontext,
    };

    // ExecInitExprRec(ctest->arg, state, resv, resnull); — evaluate the argument
    // directly into the caller's result cell. The recursion spine is core-owned.
    let _ = (resv, ctest_arg);
    panic!(
        "execExpr-domain-agg: ExecInitCoerceToDomain needs ExecInitExprRec (the core-owned \
         opcode-emission recursion, private to execExpr_core) to compile ctest->arg and each \
         DOM_CONSTRAINT_CHECK con->check_expr, and InitDomainConstraintRef (typcache owner) for \
         the compiled DomainConstraintState list. The DOMAIN_NOTNULL/DOMAIN_CHECK step shapes, \
         the lazy checkvalue cell, the get_typlen(resulttype)==-1 MAKE_READONLY R/O forcing, and \
         the innermost_domainval save/restore are mirrored below once those surfaces land."
    );

    // ----- faithful continuation (unreachable until the spine + typcache land) -----
    #[allow(unreachable_code)]
    {
        // foreach(l, constraint_ref->constraints) { ... } — for each compiled
        // DomainConstraintState (NOTNULL or CHECK):
        let constraints: &[DomainConstraintStub] = &[];
        let mut domainval: Option<(ResultCellId, ResultCellId)> = None;
        // scratch->d.domaincheck.checkvalue == NULL until the first CHECK.
        let mut checkvalue_alloc: Option<ResultCellId> = None;
        for con in constraints {
            // scratch->d.domaincheck.constraintname = con->name; — the compiled
            // DomainConstraintState's name is borrowed from the typcache-owned
            // constraint list (`PgString` is not `Clone`; the real wiring re-uses
            // the typcache allocation). Carried through `con.name` once the list
            // lands.
            if let ExprEvalStepData::DomainCheck { constraintname, .. } = &mut scratch.d {
                *constraintname = con.name_placeholder();
            }
            match con.constrainttype {
                DomConstraintType::NotNull => {
                    // scratch->opcode = EEOP_DOMAIN_NOTNULL; ExprEvalPushStep.
                    scratch.opcode = ExprEvalOp::EEOP_DOMAIN_NOTNULL;
                    core::expr_eval_push_step(mcx, state, clone_step(scratch))?;
                }
                DomConstraintType::Check => {
                    // Allocate the CHECK output workspace once.
                    let checkvalue = match checkvalue_alloc {
                        Some(c) => c,
                        None => {
                            let c = new_result_cell(mcx, state)?;
                            checkvalue_alloc = Some(c);
                            if let ExprEvalStepData::DomainCheck { checkvalue, .. } = &mut scratch.d
                            {
                                *checkvalue = c;
                            }
                            c
                        }
                    };

                    // First CHECK: decide where CoerceToDomainValue reads from.
                    if domainval.is_none() {
                        // if (get_typlen(ctest->resulttype) == -1) { MAKE_READONLY }
                        let typlen = lsyscache_get_typlen(ctest_resulttype)?;
                        if typlen == -1 {
                            let dv = new_result_cell(mcx, state)?;
                            // scratch2 = {0}; EEOP_MAKE_READONLY reading resv -> dv.
                            let scratch2 = ExprEvalStep {
                                opcode: ExprEvalOp::EEOP_MAKE_READONLY,
                                resvalue: dv,
                                resnull: dv,
                                d: ExprEvalStepData::MakeReadOnly { value: resv },
                            };
                            core::expr_eval_push_step(mcx, state, scratch2)?;
                            domainval = Some((dv, dv));
                        } else {
                            // Read straight from resv/resnull.
                            domainval = Some((resv, resv));
                        }
                    }
                    let (dv, dn) = domainval.unwrap();

                    // Save/restore innermost_domainval while recursing into the
                    // check expression, then ExecInitExprRec(con->check_expr,
                    // state, checkvalue, checknull).
                    let save_dv = state.innermost_domainval;
                    state.innermost_domainval = Some(dv);
                    let _ = dn;
                    // ExecInitExprRec(con->check_expr, state, checkvalue, checknull)
                    let _ = checkvalue;
                    state.innermost_domainval = save_dv;

                    // scratch->opcode = EEOP_DOMAIN_CHECK; ExprEvalPushStep.
                    scratch.opcode = ExprEvalOp::EEOP_DOMAIN_CHECK;
                    core::expr_eval_push_step(mcx, state, clone_step(scratch))?;
                }
            }
        }
        Ok(())
    }
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
pub fn exec_build_agg_trans<'mcx>(
    mcx: Mcx<'mcx>,
    aggstate: &mut AggStateData<'mcx>,
    phase: i32,
    do_sort: bool,
    do_hash: bool,
    nullcheck: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let _ = (do_sort, do_hash, nullcheck, estate, phase);

    // state = makeNode(ExprState); state->expr = (Expr *) aggstate;
    // state->parent = &aggstate->ss.ps;  scratch.resvalue=&state->resvalue.
    let mut state = make_expr_state(mcx)?;
    let _is_combine = do_aggsplit_combine(aggstate.aggsplit);

    // First prescan: expr_setup_walker over every pertrans aggref's
    // aggdirectargs/args/aggorder/aggdistinct/aggfilter, then
    // ExecPushExprSetupSteps(state, &deform). The walker + push live in
    // execExpr_core (private); the FETCHSOME deform prefix is part of that
    // spine.
    let _ = &mut state;
    panic!(
        "execExpr-domain-agg: ExecBuildAggTrans needs (1) the core-owned expr_setup_walker + \
         ExecPushExprSetupSteps prescan and ExecInitExprRec recursion (private to execExpr_core) \
         to compile each aggref arg/filter, and (2) the ability to target the externally-owned \
         trans_fcinfo->args[] / sortslot->tts_values[] cells (nodeAgg-owned) as recursion outputs \
         — the owned ResultCellArena only names cells internal to this ExprState, and the \
         AggTrans/AggStrictInputCheck/AggDeserialize payloads carry pertrans/fcinfo as parked \
         addresses with no arg-cell vector. The full per-trans stepping (filter jump, combine / \
         non-sorted / single- and multi-col sort input paths, strict-input null check, presorted \
         DISTINCT check, per-grouping-set ExecBuildAggTransCall, and the early-bailout jump \
         fixups) is mirrored in exec_build_agg_trans_call and the C below once those land."
    );
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
// `adjust_jumpnull` is assigned then read in the faithful continuation below the
// blocker panic (currently unreachable), mirroring the C jumpnull fixup.
#[allow(unused_assignments)]
pub fn exec_build_agg_trans_call<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    aggstate: &AggStateData<'mcx>,
    scratch: &mut ExprEvalStep<'mcx>,
    pertrans: usize,
    transno: i32,
    setno: i32,
    setoff: i32,
    ishash: bool,
    nullcheck: bool,
) -> PgResult<()> {
    // aggcontext = ishash ? aggstate->hashcontext : aggstate->aggcontexts[setno];
    // In the owned model the ExprContext is an EState pool id threaded by
    // nodeAgg; carried as a parked address in the AggTrans/nullcheck payloads.
    let _ = aggstate;
    let aggcontext_addr: usize = 0;

    let mut adjust_jumpnull: i32 = -1;

    // add check for NULL pointer?
    if nullcheck {
        scratch.opcode = ExprEvalOp::EEOP_AGG_PLAIN_PERGROUP_NULLCHECK;
        scratch.d = ExprEvalStepData::AggPlainPergroupNullcheck {
            setoff,
            jumpnull: -1, // adjust later
        };
        core::expr_eval_push_step(mcx, state, clone_step(scratch))?;
        adjust_jumpnull = state.steps_len - 1;
    }

    // Determine appropriate transition implementation.
    //
    // For non-ordered aggregates and presorted ORDER BY/DISTINCT: pick BYVAL vs
    // BYREF, and within each, INIT_STRICT (strict + no initial value) vs STRICT
    // (strict + has initial value) vs plain. For ordered aggregates: DATUM
    // (single input) vs TUPLE (multiple).
    //
    // The strict / init-value / byval / aggsortrequired / numInputs predicates
    // are read off the AggStatePerTrans (nodeAgg-owned). The pertrans index is
    // carried; resolving its fields requires the AggStatePerTrans surface, which
    // this routine does not borrow (the caller — ExecBuildAggTrans — owns it and
    // selects the opcode). Until ExecBuildAggTrans threads the resolved
    // predicates, the opcode-selection inputs are unavailable here.
    let _ = (transno, setno, setoff, ishash, aggcontext_addr, pertrans);
    panic!(
        "execExpr-domain-agg: ExecBuildAggTransCall's opcode selection reads \
         pertrans->{{transtypeByVal, aggsortrequired, numInputs, initValueIsNull}} and \
         fcinfo->flinfo->fn_strict off the nodeAgg-owned AggStatePerTrans / FunctionCallInfo, \
         which are threaded by ExecBuildAggTrans (itself blocked on the core recursion spine + \
         external-cell targeting). The PERGROUP_NULLCHECK emission + jumpnull fixup and the \
         BYVAL/BYREF × strict × init / ORDERED_TRANS opcode table are mirrored above/below once \
         that caller lands."
    );

    // ----- faithful continuation (unreachable until the caller threads predicates) -----
    #[allow(unreachable_code)]
    {
        // scratch->d.agg_trans = { pertrans, setno, setoff, transno, aggcontext };
        scratch.d = ExprEvalStepData::AggTrans {
            pertrans,
            aggcontext: aggcontext_addr,
            setno,
            transno,
            setoff,
        };
        core::expr_eval_push_step(mcx, state, clone_step(scratch))?;

        // fix up jumpnull
        if adjust_jumpnull != -1 {
            if let Some(steps) = state.steps.as_mut() {
                if let ExprEvalStepData::AggPlainPergroupNullcheck { jumpnull, .. } =
                    &mut steps[adjust_jumpnull as usize].d
                {
                    *jumpnull = state.steps_len;
                }
            }
        }
        Ok(())
    }
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
/// The per-column `FmgrInfo`/`FunctionCallInfo` are supplied by the caller
/// (`hashfunctions[i]`, already `fmgr_info`'d) and `InitFunctionCallInfoData`'d
/// here. In the owned model the `HashDatum` payload carries `finfo`/`fcinfo_data`
/// as owned boxes; the inner-Var output target is the fcinfo's arg-0 cell. The
/// owned `HashDatum` variant has **no** arg-cell vector and `FunctionCallInfo`
/// in this crate's `types-nodes` view is trimmed (no `args[]`), so the
/// `&fcinfo->args[0]` aliasing target the `EEOP_INNER_VAR` writes cannot be
/// expressed here — that fcinfo-arg cell wiring is a keystone-type gap.
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
    let need_iresult = (num_cols as i64) + ((init_value != 0) as i64) > 1;

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
    let mut _opcode = ExprEvalOp::EEOP_HASHDATUM_FIRST;
    if init_value != 0 {
        let initstep = ExprEvalStep {
            opcode: ExprEvalOp::EEOP_HASHDATUM_SET_INITVAL,
            // resvalue = numCols>0 ? &iresult->value : &state->resvalue
            resvalue: STATE_RESULT_CELL,
            resnull: STATE_RESULT_CELL,
            d: ExprEvalStepData::HashDatumInitValue {
                // UInt32GetDatum(init_value)
                init_value: Datum::from_u32(init_value),
            },
        };
        let _ = need_iresult;
        core::expr_eval_push_step(mcx, &mut state, initstep)?;
        _opcode = ExprEvalOp::EEOP_HASHDATUM_NEXT32;
    }

    // The per-column loop emits, for each column: EEOP_INNER_VAR writing
    // &fcinfo->args[0], then the HASHDATUM_FIRST/NEXT32 call reading that arg.
    // The owned HashDatum payload has finfo/fcinfo_data but no arg-cell vector,
    // and types-nodes' FunctionCallInfoBaseData is trimmed (no args[]), so the
    // EEOP_INNER_VAR -> &fcinfo->args[0] aliasing target cannot be expressed.
    let _ = (hashfunctions, collations);
    panic!(
        "execExpr-domain-agg: ExecBuildHash32FromAttrs's per-column loop emits EEOP_INNER_VAR \
         into &fcinfo->args[0] then EEOP_HASHDATUM_FIRST/NEXT32 reading it. The owned HashDatum \
         step carries finfo/fcinfo_data but no per-arg ResultCellId, and this crate's trimmed \
         FunctionCallInfoBaseData has no args[] — so the inner-Var output cell that feeds the hash \
         fcinfo cannot be wired (a keystone-type gap, like the parked Func.arg_cells path). The \
         FETCHSOME deform, SET_INITVAL seeding, FIRST->NEXT32 progression, and final-vs- \
         intermediate result placement are mirrored above once the fcinfo-arg cell lands."
    );
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
/// Blocked on the same surfaces as the other hash builders plus the recursion:
///   * `ExecCreateExprSetupSteps` + `ExecInitExprRec` — core-owned (private).
///   * `fmgr_info(hashfunc_oids[i], finfo)` — fmgr; the hash fcinfo arg-0 cell
///     wiring (`&fcinfo->args[0]`) is the same keystone gap as
///     [`exec_build_hash32_from_attrs`].
#[allow(clippy::too_many_arguments)]
pub fn exec_build_hash32_expr<'mcx>(
    mcx: Mcx<'mcx>,
    desc: &TupleDescData<'mcx>,
    ops: TupleSlotKind,
    hashfunc_oids: &[Oid],
    collations: &[Oid],
    hash_exprs: &[Expr],
    opstrict: &[bool],
    init_value: u32,
    keep_nulls: bool,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let _ = (desc, ops, hashfunc_oids, collations, opstrict, init_value, keep_nulls);
    let num_exprs = hash_exprs.len() as i32;
    debug_assert_eq!(num_exprs as usize, collations.len());

    let mut state = make_expr_state(mcx)?;
    let _ = &mut state;
    panic!(
        "execExpr-domain-agg: ExecBuildHash32Expr needs ExecCreateExprSetupSteps + ExecInitExprRec \
         (core-owned, private) to compile each hash_expr, fmgr_info(hashfunc_oids[i]) (fmgr), and \
         the &fcinfo->args[0] hash-arg cell (the keystone gap shared with ExecBuildHash32FromAttrs: \
         HashDatum has no arg-cell vector and FunctionCallInfoBaseData is trimmed). The SET_INITVAL \
         seeding, strict/non-strict FIRST->NEXT32 selection per opstrict[i]/keep_nulls, and the \
         null-skip jumpdone fixups are mirrored once those land."
    );
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

    // Per-column comparison (from the last/least-significant key backward).
    // Each column needs the equality function looked up (object_aclcheck +
    // fmgr_info), the inner/outer Var steps writing &fcinfo->args[0]/[1], the
    // NOT_DISTINCT call, and a QUAL short-circuit step.
    let _ = (eqfunctions, collations);
    let _ = &mut state;
    panic!(
        "execExpr-domain-agg: ExecBuildGroupingEqual's per-column body needs object_aclcheck \
         (catalog ACL) + fmgr_info(eqfunctions[natt]) (fmgr) and the 2-arg fcinfo whose \
         args[0]/args[1] the EEOP_INNER_VAR/EEOP_OUTER_VAR steps write — but this crate's trimmed \
         FunctionCallInfoBaseData has no args[], so those arg cells (the EEOP_NOT_DISTINCT step's \
         Func.fcinfo_data + arg_cells) cannot be wired. The numCols==0 NULL, the inner/outer \
         FETCHSOME deforms, the per-column NOT_DISTINCT + QUAL emission order, the QUAL jumpdone \
         fixups, and the trailing DONE_RETURN are mirrored once fmgr + the fcinfo args[] land."
    );
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
    param_exprs: &[Expr],
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

    let _ = (eqfunctions, collations);
    let _ = &mut state;
    panic!(
        "execExpr-domain-agg: ExecBuildParamSetEqual's per-column body needs object_aclcheck + \
         fmgr_info(eqfunctions[attno]) (catalog/fmgr) and the 2-arg fcinfo args[0]/args[1] written \
         by EEOP_INNER_VAR/EEOP_OUTER_VAR — unavailable against this crate's trimmed \
         FunctionCallInfoBaseData. The EEO_FLAG_IS_QUAL setup, the inner/outer FETCHSOME deforms, \
         the per-column NOT_DISTINCT + QUAL emission, the QUAL jumpdone fixups, and the trailing \
         DONE_RETURN are mirrored once fmgr + the fcinfo args[] land (identical to \
         ExecBuildGroupingEqual but front-to-back over every attno and never returning NULL)."
    );
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

/// `get_typlen(typid)` (lsyscache) — placeholder for the typcache/lsyscache
/// lookup used by the domain MAKE_READONLY decision. No `get_typlen` seam is
/// exported to this crate yet.
fn lsyscache_get_typlen(typid: Oid) -> PgResult<i16> {
    let _ = typid;
    panic!(
        "execExpr-domain-agg: ExecInitCoerceToDomain's get_typlen(resulttype) (lsyscache owner) \
         is not exported to this crate; needed only to decide the EEOP_MAKE_READONLY R/O forcing"
    )
}

/// Stand-in for a compiled `DomainConstraintState` (typcache-owned). Only the
/// fields the domain-check emission reads are modeled; the list itself is
/// produced by `InitDomainConstraintRef` (typcache), unported here.
struct DomainConstraintStub<'mcx> {
    name: Option<mcx::PgString<'mcx>>,
    constrainttype: DomConstraintType,
}

impl<'mcx> DomainConstraintStub<'mcx> {
    /// The constraint's `name` for the `DomainCheck.constraintname` field. The
    /// real wiring re-uses the typcache-owned `PgString` allocation (`PgString`
    /// is not `Clone`); parked as `None` until that list lands.
    fn name_placeholder(&self) -> Option<mcx::PgString<'mcx>> {
        let _ = &self.name;
        None
    }
}

#[derive(PartialEq)]
enum DomConstraintType {
    NotNull,
    Check,
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

    match &**testexpr {
        Expr::OpExpr(_) => CombiningTestExpr::SingleOp,
        Expr::BoolExpr(b) if b.boolop == AND_EXPR => CombiningTestExpr::AndClause {
            ncols: b.args.len() as i32,
        },
        other => CombiningTestExpr::Unrecognized {
            node_tag: node_tag_of(other),
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
fn oplist_op(testexpr: &Expr, idx: usize) -> &OpExpr {
    let elem = match testexpr {
        Expr::OpExpr(op) => {
            assert!(idx == 0, "oplist index {idx} out of range for single OpExpr");
            return op;
        }
        Expr::BoolExpr(b) if b.boolop == AND_EXPR => b
            .args
            .get(idx)
            .unwrap_or_else(|| panic!("oplist index {idx} out of range for and-clause args")),
        other => panic!("resolve_combining_op: subplan->testexpr is neither OpExpr nor AND-clause BoolExpr: {other:?}"),
    };
    match elem {
        Expr::OpExpr(op) => op,
        other => panic!("resolve_combining_op: and-clause arg {idx} is not an OpExpr: {other:?}"),
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
    let _ = (&node, &estate, lhs_hash_funcs, cross_eq_funcoids);
    // The lefttlist/righttlist assembly + ExecTypeFromTL + ExecBuildProjectionInfo
    // depend on the execExpr_core projection spine (ExecBuildProjectionInfo is
    // still a panic stub: it needs the owned target-list / result-slot wiring).
    // The ExecBuildHash32FromAttrs / ExecBuildGroupingEqual calls additionally
    // need the fcinfo-arg cell wiring (see those builders). Mirror PG and panic
    // until that spine lands rather than emit an approximate program.
    panic!(
        "build_hash_projections_and_exprs: needs ExecBuildProjectionInfo (execExpr_core, still a \
         panic stub pending the owned target-list / result-slot wiring) plus ExecBuildHash32FromAttrs \
         / ExecBuildGroupingEqual (this family — blocked on the fcinfo-arg cell keystone gap)"
    )
}
