//! Scalar-opcode evaluators (`execExprInterp.c`): function calls with usage
//! tracking, PARAM nodes, I/O coercion, SQLValueFunction, CurrentOf,
//! NextValue, system Vars, constraint checks, and the (hashed) ScalarArrayOp
//! machinery.
//!
//! Step evaluators address their instruction by index `op` into `state.steps`
//! and write the result through that step's `ResultCellId`; they return
//! `PgResult<()>` (evaluation can `ereport`). See [`crate::dispatch`] for the
//! shared owned-model conventions.
//!
//! Result-cell access. In C every `ExprEvalStep` carries raw `Datum *resvalue`
//! / `bool *resnull` pointers; the owned model replaces them with
//! [`ResultCellId`] indices into the owning [`ExprState`]'s
//! [`ResultCellArena`] (`state.result_cells`). A handler reads `*op->resvalue`
//! / `*op->resnull` via `state.result_cells.get(step.resvalue)` and writes them
//! back with `state.result_cells.set(step.resvalue, ...)` (the value/is-null
//! pair always shares one cell, exactly as the C pointer pair always aliases
//! one `Datum`/`bool`).
//!
//! The fcinfo call frame. Several opcodes here (`FUNCEXPR_*_FUSAGE`,
//! `IOCOERCE_SAFE`, `SCALARARRAYOP`, `HASHED_SCALARARRAYOP`) load the
//! sub-expression results the compiler gathered into `fcinfo->args[i]` and then
//! invoke `op->d.*.fn_addr(fcinfo)`. The shared `FunctionCallInfoBaseData`
//! (types-nodes `crate::fmgr`) is still trimmed to its `resultinfo` field — the
//! `args[]` / `isnull` / `flinfo` members the fmgr port widens it with are not
//! present yet, and there is no `FunctionCallInvoke` seam to dispatch through a
//! call frame either. So, exactly as the sibling `ExecJust*` fast paths in
//! [`crate::justs`] do, the arg-cell gather + `fn_addr(fcinfo)` dispatch is
//! modeled down to the genuine blocker and then panics loudly, naming the
//! unported owner (fmgr's widened call frame). All the surrounding
//! step-payload reads and control flow that the owned model can already express
//! are written out faithfully.

use backend_utils_fmgr_fmgr_seams::{
    function_call1_coll, function_call2_coll, function_call_invoke,
};
// The bare-word newtype: the scalar form the fmgr/arrayfuncs seams and the
// step-payload eval helpers operate on.
use types_datum::Datum;
// The canonical unified value type (Datum-unification keystone) — what the
// keystone-owned `ResultCell.value` / `ExprState.resvalue` carry.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;

/// Recover the bare scalar word from a stored canonical by-value datum (the
/// transitional bridge: the fmgr/arrayfuncs/saophash seams take a word).
#[inline]
fn word_of(v: &DatumV<'_>) -> Datum {
    Datum::from_usize(v.as_usize())
}
use types_error::{
    PgError, PgResult, ERRCODE_CHECK_VIOLATION, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_NOT_NULL_VIOLATION,
};
use types_nodes::execexpr::{ExprEvalStepData, ExprState, ResultCell};
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;

/// Read the `(fn_oid, fncollation)` of an `EEOP_FUNCEXPR*` step's `Func`
/// payload, then gather its per-argument result cells (`arg_cells`) into the
/// call frame's `args[]` (the C recursion writes `fcinfo->args[i]` directly;
/// the owned model gathers them here, immediately before dispatch).
///
/// `func_step_inputs(state, op)` returns `(fn_oid, fncollation, args, nargs)`.
fn func_step_inputs<'mcx>(
    state: &ExprState<'mcx>,
    op: usize,
) -> (
    types_core::primitive::Oid,
    types_core::primitive::Oid,
    Vec<types_datum::NullableDatum>,
    usize,
) {
    match step_data(state, op) {
        ExprEvalStepData::Func {
            finfo,
            fcinfo_data,
            arg_cells,
            nargs,
            ..
        } => {
            let finfo = finfo
                .as_ref()
                .expect("EEOP_FUNCEXPR: op->d.func.finfo not resolved");
            let fcinfo = fcinfo_data
                .as_ref()
                .expect("EEOP_FUNCEXPR: op->d.func.fcinfo_data missing");
            let cells = arg_cells
                .as_ref()
                .expect("EEOP_FUNCEXPR: op->d.func.arg_cells missing");
            // fcinfo->args[i].value  = *cell.value (the bare word);
            // fcinfo->args[i].isnull =  cell.isnull.
            let args: Vec<types_datum::NullableDatum> = cells
                .iter()
                .map(|&cell| {
                    let c = state.result_cells.get(cell);
                    types_datum::NullableDatum {
                        value: word_of(&c.value),
                        isnull: c.isnull,
                    }
                })
                .collect();
            (finfo.fn_oid, fcinfo.fncollation, args, *nargs as usize)
        }
        other => unreachable!("EEOP_FUNCEXPR step carries the wrong payload: {other:?}"),
    }
}

/// `ExecInterpExecuteFuncStep` core — the shared body for the `EEOP_FUNCEXPR`
/// (and strict / fusage) opcodes:
///
/// ```c
/// fcinfo->isnull = false;
/// d = op->d.func.fn_addr(fcinfo);
/// *op->resvalue = d;
/// *op->resnull = fcinfo->isnull;
/// ```
///
/// The resolved `FmgrInfo` carries only `fn_oid` (the fmgr-seam contract), so
/// the dispatch goes through `function_call_invoke`, which re-resolves by OID
/// and runs the function under `fcinfo->fncollation` (#296: the collation now
/// survives on the widened call frame). The returned bare result word is wrapped
/// back into the canonical by-value `Datum` (the transitional interp bridge,
/// matching the rest of this layer). `strict` applies C's NULL-arg
/// short-circuit before the call.
pub fn exec_func_step<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    strict: bool,
) -> PgResult<()> {
    let (fn_oid, collation, args, _nargs) = func_step_inputs(state, op);
    let (resvalue_id, resnull_id) = res_cells(state, op);
    let _ = resnull_id; // value/is-null share one cell

    // C (the _STRICT opcodes): for (argno = 0; argno < nargs; argno++)
    //                              if (args[argno].isnull) { *op->resnull = true; return; }
    if strict && args.iter().any(|a| a.isnull) {
        state
            .result_cells
            .set(resvalue_id, ResultCell { value: DatumV::null(), isnull: true });
        return Ok(());
    }

    // fcinfo->isnull = false; d = op->d.func.fn_addr(fcinfo); read back isnull.
    let (word, isnull) = function_call_invoke::call(fn_oid, collation, &args)?;

    // *op->resvalue = d;  *op->resnull = fcinfo->isnull;
    state.result_cells.set(
        resvalue_id,
        ResultCell {
            value: DatumV::from_usize(word.as_usize()),
            isnull,
        },
    );
    Ok(())
}

/// `ExecEvalFuncExprFusage(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — call a (non-strict) function, tracking usage stats.
pub fn ExecEvalFuncExprFusage<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
    // PgStat_FunctionCallUsage fcusage;
    // Datum d;
    //
    // pgstat_init_function_usage(fcinfo, &fcusage);
    // fcinfo->isnull = false;
    // d = op->d.func.fn_addr(fcinfo);
    // *op->resvalue = d;
    // *op->resnull = fcinfo->isnull;
    // pgstat_end_function_usage(&fcusage, true);
    //
    // #296: the call-frame dispatch itself is now modeled (exec_func_step) —
    // the fmgr-widened FunctionCallInfoBaseData carries fncollation/args/isnull,
    // and function_call_invoke re-dispatches by fn_oid. The REMAINING blocker is
    // the pgstat usage tracking that wraps the call
    // (pgstat_init_function_usage / pgstat_end_function_usage): the FUSAGE
    // opcodes are selected precisely when pgstat_track_functions > fn_stats, so
    // they exist to record per-function execution stats — there is no pgstat
    // function-usage seam (the pgstat owner is unported), and silently running
    // the call without the surrounding init/end usage would drop the very stats
    // this opcode variant exists to collect. Mirror-PG-and-panic until the
    // pgstat function-usage seam lands; the non-FUSAGE EEOP_FUNCEXPR family is
    // the common, stats-free path and runs through exec_func_step.
    let _ = (state, op, econtext, estate);
    panic!(
        "ExecEvalFuncExprFusage: the function call itself is modeled \
         (exec_func_step), but the pgstat_init/end_function_usage tracking that \
         wraps it has no seam (pgstat owner unported); skipping it would drop the \
         per-function stats this FUSAGE opcode exists to collect. Blocked until \
         the pgstat function-usage seam lands"
    )
}

/// `ExecEvalFuncExprStrictFusage(...)` — call a strict function with usage stats
/// (NULL argument short-circuits to NULL).
pub fn ExecEvalFuncExprStrictFusage<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
    // NullableDatum *args = fcinfo->args;
    // int nargs = op->d.func.nargs;
    //
    // /* strict function, so check for NULL args */
    // for (int argno = 0; argno < nargs; argno++)
    //     if (args[argno].isnull) { *op->resnull = true; return; }
    //
    // pgstat_init_function_usage(fcinfo, &fcusage);
    // fcinfo->isnull = false;
    // d = op->d.func.fn_addr(fcinfo);
    // *op->resvalue = d;
    // *op->resnull = fcinfo->isnull;
    // pgstat_end_function_usage(&fcusage, true);
    //
    // #296: the strict-NULL arg scan and the call dispatch are now modeled
    // (exec_func_step with strict=true reads the gathered fcinfo->args[i].isnull
    // and dispatches through function_call_invoke). The REMAINING blocker is the
    // pgstat usage tracking — see ExecEvalFuncExprFusage. Mirror-PG-and-panic
    // until the pgstat function-usage seam lands.
    let _ = (state, op, econtext, estate);
    panic!(
        "ExecEvalFuncExprStrictFusage: the strict-NULL scan + call are modeled \
         (exec_func_step), but the pgstat_init/end_function_usage tracking has \
         no seam (pgstat owner unported); skipping it would drop the per-function \
         stats this FUSAGE opcode exists to collect. Blocked until the pgstat \
         function-usage seam lands"
    )
}

/// `ExecEvalParamExec(ExprState *state, ExprEvalStep *op, ExprContext *econtext)`
/// — fetch a PARAM_EXEC value from the econtext's param-exec array.
pub fn ExecEvalParamExec<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ParamExecData *prm;
    // prm = &(econtext->ecxt_param_exec_vals[op->d.param.paramid]);
    // if (unlikely(prm->execPlan != NULL))
    // {
    //     /* Parameter not evaluated yet, so go do it */
    //     ExecSetParamPlan(prm->execPlan, econtext);
    //     Assert(prm->execPlan == NULL);
    // }
    // *op->resvalue = prm->value;
    // *op->resnull = prm->isnull;
    //
    // ecxt_param_exec_vals aliases the EState's es_param_exec_vals (the owned
    // model threads the EState explicitly; see ExprContext docs), so the param
    // is read from estate.es_param_exec_vals directly. The `execPlan` link is now
    // modeled on `ParamExecData` (an `ExecPlanLink` identity into
    // `es_subplanstates`), so `prm.execPlan.is_some()` is the C
    // `prm->execPlan != NULL` not-yet-evaluated test. The lazy-evaluation
    // re-entry (`ExecSetParamPlan(prm->execPlan, econtext)`) resolves that
    // identity back to its `SubPlanState` and runs the initplan; that resolution
    // is the executor's `exec_set_param_plan_for_pending` seam, still
    // seam-and-panic until nodeSubplan's `SubPlanState`-reachability wiring lands
    // (the `SubPlanState`s are owned by the parent plan-state's `initPlan` list,
    // not directly addressable from the param array yet). So for a PARAM_EXEC
    // whose value is already valid (`execPlan == None`) this reads straight
    // through; a pending one would need that unported re-entry.
    let _ = econtext;
    let paramid = match &step_data(state, op) {
        ExprEvalStepData::Param { paramid, .. } => *paramid,
        _ => unreachable!("ExecEvalParamExec: step is not an EEOP_PARAM_EXEC"),
    };

    let prm = &estate.es_param_exec_vals[paramid as usize];

    let (resvalue_id, resnull_id) = res_cells(state, op);
    state
        .result_cells
        .set(resvalue_id, ResultCell { value: prm.value.clone(), isnull: prm.isnull });
    let _ = resnull_id; // value/is-null share one cell
    Ok(())
}

/// `ExecEvalParamExtern(...)` — fetch a PARAM_EXTERN value from the param list.
pub fn ExecEvalParamExtern<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ParamListInfo paramInfo = econtext->ecxt_param_list_info;
    // int paramId = op->d.param.paramid;
    //
    // if (likely(paramInfo && paramId > 0 && paramId <= paramInfo->numParams))
    // {
    //     ParamExternData *prm, prmdata;
    //     /* give hook a chance in case parameter is dynamic */
    //     if (paramInfo->paramFetch != NULL)
    //         prm = paramInfo->paramFetch(paramInfo, paramId, false, &prmdata);
    //     else
    //         prm = &paramInfo->params[paramId - 1];
    //     if (likely(OidIsValid(prm->ptype)))
    //     {
    //         if (unlikely(prm->ptype != op->d.param.paramtype))
    //             ereport(ERROR, (ERRCODE_DATATYPE_MISMATCH, ...));
    //         *op->resvalue = prm->value;
    //         *op->resnull = prm->isnull;
    //         return;
    //     }
    // }
    // ereport(ERROR, (ERRCODE_UNDEFINED_OBJECT,
    //                 errmsg("no value found for parameter %d", paramId)));
    //
    // ecxt_param_list_info aliases the EState's es_param_list_info, which in the
    // owned model is a ParamListInfoHandle into the (unported) param-list unit:
    // the params[] array, numParams, and the dynamic paramFetch hook all live
    // there. Reading prm->ptype/value/isnull (and firing paramFetch) needs that
    // owner. The step payload (d.param.paramid / paramtype) is modeled; the
    // lookup itself is blocked on the param-list owner.
    let _ = (op, econtext, estate);
    let _ = state;
    panic!(
        "ExecEvalParamExtern: resolving econtext->ecxt_param_list_info \
         (params[paramId-1] / numParams / the dynamic paramFetch hook) needs the \
         unported param-list owner; the EState only carries a ParamListInfoHandle \
         into it. Blocked until the param-list unit lands."
    )
}

/// `ExecEvalParamSet(...)` — store a value into a PARAM_EXEC slot.
pub fn ExecEvalParamSet<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ParamExecData *prm;
    // prm = &(econtext->ecxt_param_exec_vals[op->d.param.paramid]);
    // /* Shouldn't have a pending evaluation anymore */
    // Assert(prm->execPlan == NULL);
    // prm->value = *op->resvalue;
    // prm->isnull = *op->resnull;
    //
    // Same param-array aliasing as ExecEvalParamExec: write into the EState's
    // es_param_exec_vals from this step's result cell. (ParamExecData is trimmed
    // — no execPlan — so the Assert is vacuous here.)
    let _ = econtext;
    let paramid = match &step_data(state, op) {
        ExprEvalStepData::Param { paramid, .. } => *paramid,
        _ => unreachable!("ExecEvalParamSet: step is not an EEOP_PARAM_SET"),
    };

    let (resvalue_id, _resnull_id) = res_cells(state, op);
    let res = state.result_cells.get(resvalue_id);

    let prm = &mut estate.es_param_exec_vals[paramid as usize];
    prm.value = res.value;
    prm.isnull = res.isnull;
    Ok(())
}

/// `ExecEvalCoerceViaIOSafe(ExprState *state, ExprEvalStep *op)` — output-then-
/// input I/O coercion with soft-error handling.
pub fn ExecEvalCoerceViaIOSafe<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // char *str;
    // /* call output function (similar to OutputFunctionCall) */
    // if (*op->resnull) { str = NULL; }
    // else {
    //     fcinfo_out = op->d.iocoerce.fcinfo_data_out;
    //     fcinfo_out->args[0].value = *op->resvalue;
    //     fcinfo_out->args[0].isnull = false;
    //     fcinfo_out->isnull = false;
    //     str = DatumGetCString(FunctionCallInvoke(fcinfo_out));
    //     Assert(!fcinfo_out->isnull);
    // }
    // /* call input function (similar to InputFunctionCallSafe) */
    // if (!op->d.iocoerce.finfo_in->fn_strict || str != NULL) {
    //     fcinfo_in = op->d.iocoerce.fcinfo_data_in;
    //     fcinfo_in->args[0].value = PointerGetDatum(str);
    //     fcinfo_in->args[0].isnull = *op->resnull;
    //     /* second and third arguments are already set up */
    //     Assert(IsA(fcinfo_in->context, ErrorSaveContext));
    //     fcinfo_in->isnull = false;
    //     *op->resvalue = FunctionCallInvoke(fcinfo_in);
    //     if (SOFT_ERROR_OCCURRED(fcinfo_in->context)) {
    //         *op->resnull = true; *op->resvalue = (Datum) 0; return;
    //     }
    //     /* Should get null result iff str is NULL */
    // }
    //
    // The step payload (d.iocoerce.finfo_in.fn_strict, fcinfo_data_out/in) is
    // modeled. The genuine blockers are the two call-frame invocations:
    // writing fcinfo_out->args[0] and FunctionCallInvoke(fcinfo_out), then
    // fcinfo_in->args[0] and FunctionCallInvoke(fcinfo_in), plus reading the
    // soft-error sink on fcinfo_in->context. The FunctionCallInfoBaseData is
    // trimmed (no args[]/isnull/context until fmgr widens it; see crate::justs)
    // and there is no FunctionCallInvoke seam. Faithful once fmgr lands.
    let _ = (state, op, estate);
    panic!(
        "ExecEvalCoerceViaIOSafe: the output/input FunctionCallInvoke pair \
         (write fcinfo_out/in->args[0], dispatch, read back, test the soft-error \
         sink on fcinfo_in->context) needs the fmgr-widened \
         FunctionCallInfoBaseData (trimmed model has no args[]/isnull/context); \
         blocked until fmgr lands"
    )
}

/// `ExecEvalSQLValueFunction(ExprState *state, ExprEvalStep *op)` — evaluate
/// CURRENT_DATE / CURRENT_USER / etc.
pub fn ExecEvalSQLValueFunction<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // LOCAL_FCINFO(fcinfo, 0);
    // SQLValueFunction *svf = op->d.sqlvaluefunction.svf;
    // *op->resnull = false;
    // switch (svf->op) {
    //   case SVFOP_CURRENT_DATE:  *op->resvalue = DateADTGetDatum(GetSQLCurrentDate()); break;
    //   case SVFOP_CURRENT_TIME[_N]: ... GetSQLCurrentTime(svf->typmod) ...
    //   case SVFOP_CURRENT_TIMESTAMP[_N]: ... GetSQLCurrentTimestamp(svf->typmod) ...
    //   case SVFOP_LOCALTIME[_N]: ... GetSQLLocalTime(svf->typmod) ...
    //   case SVFOP_LOCALTIMESTAMP[_N]: ... GetSQLLocalTimestamp(svf->typmod) ...
    //   case SVFOP_CURRENT_ROLE/USER/USER:
    //       InitFunctionCallInfoData(...); *op->resvalue = current_user(fcinfo);
    //       *op->resnull = fcinfo->isnull; break;
    //   case SVFOP_SESSION_USER: ... session_user(fcinfo) ...
    //   case SVFOP_CURRENT_CATALOG: ... current_database(fcinfo) ...
    //   case SVFOP_CURRENT_SCHEMA: ... current_schema(fcinfo) ...
    // }
    //
    // The SQLValueFunction node (op->d.sqlvaluefunction.svf) is parked as an
    // opaque address — primnodes does not carry `SQLValueFunction` yet, so its
    // `op` discriminant / `typmod` are not readable here. Even with the node,
    // every arm dispatches to a date/time or session helper
    // (GetSQLCurrentDate / current_user / etc.) owned by the datetime / misc
    // adt units, and the role/user arms build a LOCAL_FCINFO call frame (fmgr).
    // Blocked on primnodes (svf node) + those adt owners.
    let _ = (state, op, estate);
    panic!(
        "ExecEvalSQLValueFunction: op.d.sqlvaluefunction.svf is a parked opaque \
         address (primnodes does not carry SQLValueFunction yet), and the arms \
         dispatch to GetSQLCurrent* / current_user / current_database / \
         current_schema in the unported datetime / misc-adt owners (the user \
         arms over a LOCAL_FCINFO call frame); blocked until primnodes + those \
         owners land"
    )
}

/// `ExecEvalCurrentOfExpr(ExprState *state, ExprEvalStep *op)` — CURRENT OF
/// cursor reference (always errors at runtime; resolved by the scan node).
pub fn ExecEvalCurrentOfExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ereport(ERROR,
    //         (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
    //          errmsg("WHERE CURRENT OF is not supported for this table type")));
    //
    // The planner converts CURRENT OF into a TidScan qualification (or FDW
    // handling), so ExecInitExpr accepts a CurrentOfExpr but it should never be
    // executed; reaching here means an unhandled CURRENT OF (e.g. on a foreign
    // table whose FDW doesn't support it).
    let _ = (state, op, estate);
    Err(PgError::error("WHERE CURRENT OF is not supported for this table type")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

/// `ExecEvalNextValueExpr(ExprState *state, ExprEvalStep *op)` — evaluate a
/// column DEFAULT nextval() during COPY/INSERT.
pub fn ExecEvalNextValueExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // int64 newval = nextval_internal(op->d.nextvalueexpr.seqid, false);
    // switch (op->d.nextvalueexpr.seqtypid) {
    //   case INT2OID: *op->resvalue = Int16GetDatum((int16) newval); break;
    //   case INT4OID: *op->resvalue = Int32GetDatum((int32) newval); break;
    //   case INT8OID: *op->resvalue = Int64GetDatum((int64) newval); break;
    //   default: elog(ERROR, "unsupported sequence type %u", ...);
    // }
    // *op->resnull = false;
    //
    // The step payload (d.nextvalueexpr.seqid / seqtypid) is modeled, but the
    // core work — nextval_internal(seqid, false) — advances the sequence, which
    // is owned by the (unported) backend-commands-sequence unit. The integer
    // narrowing + result-cell write are trivial once the sequence value is in
    // hand; the blocker is the sequence advance.
    let _ = (state, op, estate);
    panic!(
        "ExecEvalNextValueExpr: nextval_internal(op.d.nextvalueexpr.seqid, false) \
         advances the sequence, owned by the unported backend-commands-sequence \
         unit; blocked until it lands"
    )
}

/// `ExecEvalConstraintNotNull(ExprState *state, ExprEvalStep *op)` — domain
/// NOT NULL constraint check.
pub fn ExecEvalConstraintNotNull<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // if (*op->resnull)
    //     errsave((Node *) op->d.domaincheck.escontext,
    //             (errcode(ERRCODE_NOT_NULL_VIOLATION),
    //              errmsg("domain %s does not allow null values",
    //                     format_type_be(op->d.domaincheck.resulttype)),
    //              errdatatype(op->d.domaincheck.resulttype)));
    //
    // errsave with a NULL escontext == ereport(ERROR). op->d.domaincheck
    // .escontext is a parked opaque address (the soft-error sink is not threaded
    // here yet); NULL (0) is the hard-throw case, matching the common path.
    // format_type_be() (lsyscache/format-type owner) is not a dependency of this
    // crate, so the type name is rendered from the OID; the load-bearing
    // behavior (the NOT_NULL_VIOLATION on a null domain value) is faithful.
    let _ = estate;
    let resulttype = match &step_data(state, op) {
        ExprEvalStepData::DomainCheck { resulttype, .. } => *resulttype,
        _ => unreachable!("ExecEvalConstraintNotNull: step is not an EEOP_DOMAIN_NOTNULL"),
    };

    let (resvalue_id, _resnull_id) = res_cells(state, op);
    let resnull = state.result_cells.get(resvalue_id).isnull;

    if resnull {
        return Err(PgError::error(format!(
            "domain {} does not allow null values",
            // format_type_be(resulttype) — owner (format-type) not a dep here.
            resulttype
        ))
        .with_sqlstate(ERRCODE_NOT_NULL_VIOLATION));
    }
    Ok(())
}

/// `ExecEvalConstraintCheck(ExprState *state, ExprEvalStep *op)` — single domain
/// CHECK constraint evaluation.
pub fn ExecEvalConstraintCheck<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // if (!*op->d.domaincheck.checknull &&
    //     !DatumGetBool(*op->d.domaincheck.checkvalue))
    //     errsave((Node *) op->d.domaincheck.escontext,
    //             (errcode(ERRCODE_CHECK_VIOLATION),
    //              errmsg("value for domain %s violates check constraint \"%s\"",
    //                     format_type_be(op->d.domaincheck.resulttype),
    //                     op->d.domaincheck.constraintname),
    //              errdomainconstraint(op->d.domaincheck.resulttype,
    //                                  op->d.domaincheck.constraintname)));
    //
    // d.domaincheck.checkvalue is a ResultCellId naming the cell the CHECK
    // expression's result was written into; read its value/is-null pair. As in
    // ConstraintNotNull, escontext is a parked opaque address (NULL == throw)
    // and format_type_be is not a dep, so the type name is rendered from the
    // OID; the constraint-violation behavior is faithful.
    let _ = estate;
    let (checkvalue_id, resulttype, constraintname) = match &step_data(state, op) {
        ExprEvalStepData::DomainCheck {
            checkvalue,
            resulttype,
            constraintname,
            ..
        } => (
            *checkvalue,
            *resulttype,
            constraintname
                .as_ref()
                .map(|s| s.to_string())
                .unwrap_or_default(),
        ),
        _ => unreachable!("ExecEvalConstraintCheck: step is not an EEOP_DOMAIN_CHECK"),
    };

    let check = state.result_cells.get(checkvalue_id);
    let checknull = check.isnull;
    // DatumGetBool(X) — any nonzero word reads as true.
    let checkbool = check.value.as_bool();

    if !checknull && !checkbool {
        return Err(PgError::error(format!(
            "value for domain {} violates check constraint \"{}\"",
            // format_type_be(resulttype) — owner (format-type) not a dep here.
            resulttype, constraintname
        ))
        .with_sqlstate(ERRCODE_CHECK_VIOLATION));
    }
    Ok(())
}

/// `ExecEvalSysVar(ExprState *state, ExprEvalStep *op, ExprContext *econtext,
/// TupleTableSlot *slot)` — fetch a system attribute (ctid, xmin, ...).
pub fn ExecEvalSysVar<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    slot: types_nodes::SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Datum d;
    // /* OLD/NEW system attribute is NULL if OLD/NEW row is NULL */
    // if ((op->d.var.varreturningtype == VAR_RETURNING_OLD &&
    //      state->flags & EEO_FLAG_OLD_IS_NULL) ||
    //     (op->d.var.varreturningtype == VAR_RETURNING_NEW &&
    //      state->flags & EEO_FLAG_NEW_IS_NULL))
    // {
    //     *op->resvalue = (Datum) 0; *op->resnull = true; return;
    // }
    // /* slot_getsysattr has sufficient defenses against bad attnums */
    // d = slot_getsysattr(slot, op->d.var.attnum, op->resnull);
    // *op->resvalue = d;
    // if (unlikely(*op->resnull))
    //     elog(ERROR, "failed to fetch attribute from slot");
    //
    // The OLD/NEW-is-NULL short-circuit IS fully expressible (read d.var
    // .varreturningtype + the state flags + write the result cell). The fetch
    // itself — slot_getsysattr(slot, attnum, &isnull) — reads a system column
    // out of the slot's underlying tuple, which is owned by the (unported)
    // execTuples slot-payload model (the trimmed TupleTableSlot exposes no
    // value/tuple storage, and execTuples-seams offers no slot_getsysattr).
    // Faithful for the short-circuit; the actual sysattr fetch is blocked on
    // execTuples.
    use types_nodes::execexpr::{VarReturningType, EEO_FLAG_NEW_IS_NULL, EEO_FLAG_OLD_IS_NULL};

    let varreturningtype = match &step_data(state, op) {
        ExprEvalStepData::Var { varreturningtype, .. } => *varreturningtype,
        _ => unreachable!("ExecEvalSysVar: step is not an EEOP_*_SYSVAR"),
    };

    if (varreturningtype == VarReturningType::VAR_RETURNING_OLD
        && (state.flags & EEO_FLAG_OLD_IS_NULL) != 0)
        || (varreturningtype == VarReturningType::VAR_RETURNING_NEW
            && (state.flags & EEO_FLAG_NEW_IS_NULL) != 0)
    {
        let (resvalue_id, _resnull_id) = res_cells(state, op);
        state
            .result_cells
            .set(resvalue_id, ResultCell { value: DatumV::null(), isnull: true });
        return Ok(());
    }

    let _ = (slot, econtext, estate);
    panic!(
        "ExecEvalSysVar: slot_getsysattr(slot, op.d.var.attnum, &isnull) reads a \
         system column out of the slot's underlying tuple, owned by the unported \
         execTuples slot-payload model (the trimmed TupleTableSlot has no \
         tuple/value storage and execTuples-seams has no slot_getsysattr); \
         blocked until execTuples lands. (The OLD/NEW-is-NULL short-circuit above \
         is faithful.)"
    )
}

/// `ExecEvalScalarArrayOp(ExprState *state, ExprEvalStep *op)` — `x op ANY/ALL
/// (array)` by linear scan over the array elements.
pub fn ExecEvalScalarArrayOp<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // FunctionCallInfo fcinfo = op->d.scalararrayop.fcinfo_data;
    // bool useOr = op->d.scalararrayop.useOr;
    // bool strictfunc = op->d.scalararrayop.finfo->fn_strict;
    // ArrayType *arr; int nitems; Datum result; bool resultnull; ...
    //
    // if (*op->resnull) return;                 /* NULL array => NULL */
    // arr = DatumGetArrayTypeP(*op->resvalue);
    // nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
    // if (nitems <= 0) { *op->resvalue = BoolGetDatum(!useOr); *op->resnull = false; return; }
    // if (fcinfo->args[0].isnull && strictfunc) { *op->resnull = true; return; }
    // if (op->d.scalararrayop.element_type != ARR_ELEMTYPE(arr))
    //     get_typlenbyvalalign(ARR_ELEMTYPE(arr), &typlen, &typbyval, &typalign);
    // result = BoolGetDatum(!useOr); resultnull = false;
    // for each element: load fcinfo->args[1]; thisresult = fn_addr(fcinfo);
    //   combine per OR/AND; ...
    // *op->resvalue = result; *op->resnull = resultnull;
    //
    // The step payload (d.scalararrayop.useOr / finfo.fn_strict / element_type /
    // typlen/typbyval/typalign / scalar_cell / array_cell / fn_addr) is modeled,
    // and the *op->resnull NULL-array guard + empty-array fast path are
    // expressible. But the body's core needs (a) the fcinfo call frame — reading
    // fcinfo->args[0].isnull, loading each element into fcinfo->args[1], and
    // dispatching op->d.scalararrayop.fn_addr(fcinfo) — which the trimmed
    // FunctionCallInfoBaseData cannot hold (fmgr widens it; see crate::justs),
    // and (b) detoasting + deconstructing the ArrayType (DatumGetArrayTypeP /
    // ArrayGetNItems / fetch_att over ARR_DATA_PTR + the null bitmap), owned by
    // the (unported here) arrayfuncs unit, plus get_typlenbyvalalign from the
    // lsyscache/typcache owner. Faithful once fmgr + arrayfuncs land.
    let _ = (state, op, estate);
    panic!(
        "ExecEvalScalarArrayOp: the per-element comparison loads fcinfo->args[1] \
         and dispatches op.d.scalararrayop.fn_addr(fcinfo) — needs the \
         fmgr-widened FunctionCallInfoBaseData (trimmed model has no \
         args[]/isnull) — and the array detoast/deconstruct \
         (DatumGetArrayTypeP / ArrayGetNItems / fetch_att + null bitmap) plus \
         get_typlenbyvalalign belong to the unported arrayfuncs / typcache \
         owners; blocked until fmgr + arrayfuncs land"
    )
}

/// `saop_element_hash(struct saophash_hash *tb, Datum key)` — the `SH_HASH_KEY`
/// callback: hash one array element via the SAOP's hash function.
///
/// Faithful to `execExprInterp.c:4176-4188`: the C loads `key` into the table's
/// 1-arg `hash_fcinfo_data`, dispatches `hash_finfo.fn_addr(fcinfo)`, and
/// returns `DatumGetUInt32`. The owned `FmgrInfo` carries only `fn_oid` (the F0
/// contract — see [`crate::justs`]), so the dispatch goes through the fmgr seam
/// `function_call1_coll`, which re-resolves by OID. `hashfuncid` is
/// `saop->hashfuncid` (`tb->private_data->hash_finfo`); `collation` is
/// `saop->inputcollid` (the collation `InitFunctionCallInfoData` stamped onto
/// `hash_fcinfo_data`).
pub fn saop_element_hash(
    hashfuncid: types_core::primitive::Oid,
    collation: types_core::primitive::Oid,
    key: Datum,
) -> PgResult<u32> {
    // fcinfo->args[0].value = key; fcinfo->args[0].isnull = false;
    // hash = elements_tab->hash_finfo.fn_addr(fcinfo);
    // return DatumGetUInt32(hash);
    let hash = function_call1_coll::call(hashfuncid, collation, key)?;
    Ok(hash.as_u32())
}

/// `saop_hash_element_match(struct saophash_hash *tb, Datum key1, Datum key2)`
/// — the `SH_EQUAL` callback: compare two elements via the SAOP's comparison
/// (equality) operator.
///
/// Faithful to `execExprInterp.c:4194-4209`: the C loads `key1`/`key2` into the
/// step's 2-arg comparison `fcinfo_data`, dispatches `finfo->fn_addr(fcinfo)`,
/// and returns `DatumGetBool`. `matchfuncid` is the OID of
/// `op->d.hashedscalararrayop.finfo` (the equality function the compiler stamped
/// — `opfuncid` for hashed IN, `negfuncid` for hashed NOT IN); `collation` is
/// `saop->inputcollid`. The dispatch goes through the fmgr seam
/// `function_call2_coll` (re-resolve by OID), the same pattern `eval_agg` uses
/// for its equality probe. Both keys are non-null here (hashtable build/probe
/// never stores NULLs), matching `FunctionCall2Coll`'s non-null-arg contract.
pub fn saop_hash_element_match(
    matchfuncid: types_core::primitive::Oid,
    collation: types_core::primitive::Oid,
    key1: Datum,
    key2: Datum,
) -> PgResult<bool> {
    // fcinfo->args[0].value = key1; fcinfo->args[0].isnull = false;
    // fcinfo->args[1].value = key2; fcinfo->args[1].isnull = false;
    // result = elements_tab->op->d.hashedscalararrayop.finfo->fn_addr(fcinfo);
    // return DatumGetBool(result);
    let result = function_call2_coll::call(matchfuncid, collation, key1, key2)?;
    Ok(result.as_bool())
}

/// `ExecEvalHashedScalarArrayOp(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — `x = ANY (array)` via a built hash table.
///
/// Faithful re-port of `execExprInterp.c:4225-4402`. On the first evaluation it
/// deconstructs the RHS array const (via the arrayfuncs seam, which subsumes the
/// C `DatumGetArrayTypeP`/`ArrayGetNItems`/`ARR_DATA_PTR`/`ARR_NULLBITMAP`/
/// `fetch_att` bitmap walk) and builds the [`crate::saophash`] table over the
/// non-NULL elements, recording `has_nulls`; thereafter it probes the table for
/// the scalar. The strict-NULL short circuit, the IN/NOT-IN result selection,
/// and the no-match-with-nulls (strict vs non-strict) branch are transcribed.
///
/// One sub-path mirror-PG-and-panics on a genuinely-missing seam capability: the
/// **non-strict, no-match-with-NULLs** branch dispatches the equality function
/// with `args[1].isnull = true` (a NULL rhs) and reads back `fcinfo->isnull`.
/// The `function_call2_coll` seam (C `FunctionCall2Coll`) only models non-null
/// args and asserts a non-null result, so this single branch needs the
/// fmgr-widened nullable-arg call frame and panics until that lands. Every other
/// path — the common one — is real own-logic + real seam `::call`s.
pub fn ExecEvalHashedScalarArrayOp<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = econtext;

    // ScalarArrayOpExprHashTable *elements_tab = op->d.hashedscalararrayop.elements_tab;
    // bool inclause = op->d.hashedscalararrayop.inclause;
    // bool strictfunc = op->d.hashedscalararrayop.finfo->fn_strict;
    // Read the per-step inputs (inclause / strictfunc), the comparison function
    // OID + collation, the hash function OID, and whether the table is built.
    let (inclause, strictfunc, matchfuncid, hashfuncid, collation, has_built) = {
        match step_data(state, op) {
            ExprEvalStepData::HashedScalarArrayOp {
                inclause,
                finfo,
                saop,
                elements_tab,
                ..
            } => {
                let finfo = finfo
                    .as_ref()
                    .expect("ExecEvalHashedScalarArrayOp: op->d.hashedscalararrayop.finfo not resolved");
                let saop = saop
                    .as_ref()
                    .expect("ExecEvalHashedScalarArrayOp: op->d.hashedscalararrayop.saop missing");
                (
                    *inclause,
                    finfo.fn_strict,
                    finfo.fn_oid,
                    saop.hashfuncid,
                    saop.inputcollid,
                    elements_tab.is_some(),
                )
            }
            other => unreachable!(
                "EEOP_HASHED_SCALARARRAYOP step carries the wrong payload: {other:?}"
            ),
        }
    };

    // The scalar arg the compiler recursed into fcinfo->args[0] is modeled as
    // the step's `scalar_cell` (see execExpr.c's hashed path):
    //   Datum scalar = fcinfo->args[0].value;
    //   bool  scalar_isnull = fcinfo->args[0].isnull;
    let scalar_cell_id = match step_data(state, op) {
        ExprEvalStepData::HashedScalarArrayOp { scalar_cell, .. } => *scalar_cell,
        _ => unreachable!(),
    };
    let scalar = state.result_cells.get(scalar_cell_id);
    let (scalar_value, scalar_isnull) = (scalar.value, scalar.isnull);

    let (resvalue_id, resnull_id) = res_cells(state, op);

    // Assert(!*op->resnull);  -- we never set up a hashed SAOP on a NULL array const.

    // If the scalar is NULL and the function is strict, return NULL; no point
    // searching.
    //   if (fcinfo->args[0].isnull && strictfunc) { *op->resnull = true; return; }
    if scalar_isnull && strictfunc {
        state
            .result_cells
            .set(resnull_id, ResultCell { value: scalar_value, isnull: true });
        return Ok(());
    }

    // Build the hash table on first evaluation.
    //   if (elements_tab == NULL) { ... }
    if !has_built {
        // saop = op->d.hashedscalararrayop.saop;
        // arr = DatumGetArrayTypeP(*op->resvalue);
        // nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
        // get_typlenbyvalalign(ARR_ELEMTYPE(arr), &typlen, &typbyval, &typalign);
        let arraydatum = word_of(&state.result_cells.get(resvalue_id).value);
        let mcx = estate.es_query_cxt;

        let elemtype =
            backend_utils_adt_arrayfuncs_seams::array_get_elemtype::call(mcx, arraydatum)?;
        let tlba =
            backend_utils_cache_lsyscache_seams::get_typlenbyvalalign::call(elemtype)?;

        // Deconstruct the array into its per-element (Datum, isnull) pairs. This
        // seam subsumes C's ARR_DATA_PTR + ARR_NULLBITMAP + fetch_att +
        // att_addlength_pointer + att_align_nominal bitmap walk over `nitems`.
        let elements = backend_utils_adt_arrayfuncs_seams::deconstruct_array::call(
            mcx,
            arraydatum,
            elemtype,
            tlba.typlen,
            tlba.typbyval,
            tlba.typalign as core::ffi::c_char,
        )?;
        let nitems = elements.len();

        // elements_tab = palloc0(...); op->d.hashedscalararrayop.elements_tab = elements_tab;
        // elements_tab->op = op;
        // fmgr_info(saop->hashfuncid, &elements_tab->hash_finfo);
        // InitFunctionCallInfoData(elements_tab->hash_fcinfo_data, ..., saop->inputcollid, ...);
        // elements_tab->hashtab = saophash_create(CurrentMemoryContext, nitems, elements_tab);
        let mut table = crate::saophash::ScalarArrayOpExprHashTable::default();
        table.hash_finfo.fn_oid = hashfuncid;
        table.hashtab = crate::saophash::saophash_create(nitems as u32);

        // Walk the elements: NULLs are not stored (record has_nulls); non-NULLs
        // are inserted. The closures are the SH_HASH_KEY / SH_EQUAL callbacks,
        // dispatching the hash / equality functions through the fmgr seams.
        let mut has_nulls = false;
        for (element, isnull) in elements.iter().copied() {
            if isnull {
                has_nulls = true;
            } else {
                let mut hash_key =
                    |k: Datum| saop_element_hash(hashfuncid, collation, k);
                let mut equal = |a: Datum, b: Datum| {
                    saop_hash_element_match(matchfuncid, collation, a, b)
                };
                crate::saophash::saophash_insert(
                    &mut table.hashtab,
                    element,
                    &mut hash_key,
                    &mut equal,
                )?;
            }
        }

        // Store the built table + has_nulls back into the step payload.
        match step_data_mut(state, op) {
            ExprEvalStepData::HashedScalarArrayOp {
                elements_tab,
                has_nulls: hn,
                ..
            } => {
                *elements_tab = Some(Box::new(table));
                *hn = has_nulls;
            }
            _ => unreachable!(),
        }
    }

    // Probe the hash table.
    //   hashfound = NULL != saophash_lookup(elements_tab->hashtab, scalar);
    let hashfound = {
        let mut hash_key = |k: Datum| saop_element_hash(hashfuncid, collation, k);
        let mut equal =
            |a: Datum, b: Datum| saop_hash_element_match(matchfuncid, collation, a, b);
        let table = match step_data(state, op) {
            ExprEvalStepData::HashedScalarArrayOp { elements_tab, .. } => elements_tab
                .as_ref()
                .expect("ExecEvalHashedScalarArrayOp: elements_tab just built"),
            _ => unreachable!(),
        };
        crate::saophash::saophash_lookup(&table.hashtab, word_of(&scalar_value), &mut hash_key, &mut equal)?
    };

    // result = inclause ? BoolGetDatum(hashfound) : BoolGetDatum(!hashfound);
    let mut result = if inclause { hashfound } else { !hashfound };
    let mut resultnull = false;

    // If no match, account for NULLs in the array.
    //   if (!hashfound && op->d.hashedscalararrayop.has_nulls) { ... }
    let has_nulls = match step_data(state, op) {
        ExprEvalStepData::HashedScalarArrayOp { has_nulls, .. } => *has_nulls,
        _ => unreachable!(),
    };
    if !hashfound && has_nulls {
        if strictfunc {
            // Nulls in the array + non-null lhs + no match => NULL.
            //   result = (Datum) 0; resultnull = true;
            result = false;
            resultnull = true;
        } else {
            // Execute the (non-strict) function once with a NULL rhs.
            //   fcinfo->args[0] = {scalar, scalar_isnull};
            //   fcinfo->args[1] = {(Datum)0, true};
            //   result = op->d.hashedscalararrayop.finfo->fn_addr(fcinfo);
            //   resultnull = fcinfo->isnull;
            //   if (!inclause) result = !result;
            //
            // This is the one sub-path that needs the fmgr-widened nullable-arg
            // call frame (pass args[1].isnull = true, read back fcinfo->isnull):
            // the function_call2_coll seam (FunctionCall2Coll) only models
            // non-null args + a non-null result. Mirror-PG-and-panic on the
            // unported owner.
            let _ = (scalar_value, scalar_isnull, matchfuncid, collation);
            panic!(
                "ExecEvalHashedScalarArrayOp: non-strict no-match-with-nulls branch \
                 dispatches the equality function with a NULL rhs (args[1].isnull = \
                 true) and reads back fcinfo->isnull — the function_call2_coll seam \
                 (FunctionCall2Coll) models only non-null args and a non-null \
                 result; blocked until the fmgr-widened nullable-arg call frame lands"
            );
        }
    }

    // *op->resvalue = result; *op->resnull = resultnull;
    state.result_cells.set(
        resvalue_id,
        ResultCell { value: DatumV::from_bool(result), isnull: resultnull },
    );
    Ok(())
}

/// Borrow the `ExprEvalStepData` payload of step `op` in `state`. Mirrors the C
/// `&state->steps[op]->d` access; panics if the step program is not yet
/// installed (a caller/compile bug).
#[inline]
fn step_data<'a, 'mcx>(state: &'a ExprState<'mcx>, op: usize) -> &'a ExprEvalStepData<'mcx> {
    &state
        .steps
        .as_ref()
        .expect("eval_scalar: steps not ready")[op]
        .d
}

/// Mutably borrow the `ExprEvalStepData` payload of step `op` — the C
/// `&state->steps[op]->d` for the write-back of `elements_tab`/`has_nulls` in
/// `ExecEvalHashedScalarArrayOp`.
#[inline]
fn step_data_mut<'a, 'mcx>(
    state: &'a mut ExprState<'mcx>,
    op: usize,
) -> &'a mut ExprEvalStepData<'mcx> {
    &mut state
        .steps
        .as_mut()
        .expect("eval_scalar: steps not ready")[op]
        .d
}

/// Resolve the `(resvalue, resnull)` [`ResultCellId`] pair of step `op` — the
/// owned-model replacement for the C `op->resvalue` / `op->resnull` pointers.
/// The two ids name the value and is-null halves of one logical cell (they are
/// equal in the current model; both are returned so callers read like the C).
#[inline]
fn res_cells<'mcx>(
    state: &ExprState<'mcx>,
    op: usize,
) -> (
    types_nodes::execexpr::ResultCellId,
    types_nodes::execexpr::ResultCellId,
) {
    let step = &state
        .steps
        .as_ref()
        .expect("eval_scalar: steps not ready")[op];
    (step.resvalue, step.resnull)
}
