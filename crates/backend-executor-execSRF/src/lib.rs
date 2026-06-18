//! `executor/execSRF.c` — the executor-frame API for set-returning functions.
//!
//! This unit serves `nodeFunctionscan.c` and `nodeProjectSet.c`, providing the
//! common code for calling set-returning functions through the `ReturnSetInfo`
//! API. It is the #349 K2 keystone: the executor builds its OWN
//! [`types_nodes::fmgr::FunctionCallInfoBaseData`] with a LIVE
//! `fcinfo.resultinfo = ReturnSetInfo` (+ `fn_extra` / `fn_mcxt` channels) and
//! dispatches the SRF's `PGFunction` through it, letting the callee read
//! `econtext`/`expectedDesc` and write `isDone`/`returnMode`/`setResult`/`setDesc`
//! each iteration (the ValuePerCall loop + Materialize mode).
//!
//! ## The executor-frame SRF dispatch (the dual-home boundary)
//!
//! `FunctionCallInvoke(fcinfo)` in C is `fcinfo->flinfo->fn_addr(fcinfo)`: the
//! same `PGFunction` callable receives ordinary calls AND set-returning calls
//! (the `resultinfo` is just a field on the frame). The owned model has two
//! `FunctionCallInfoBaseData` homes (WONTFIX, DESIGN_DEBT): the by-OID builtin
//! registry (`backend_utils_fmgr_core`) holds `types_fmgr::PGFunction`s whose
//! frame's `resultinfo` is tag-only, so an SRF dispatched through it can never
//! see the LIVE `ReturnSetInfo`. The live `ReturnSetInfo` lives on the
//! `types_nodes` frame.
//!
//! So this unit keeps a small executor-frame SRF table keyed by OID, holding
//! [`types_nodes::execexpr::PGFunction`]s (the `for<'mcx> fn(&mut
//! FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx>` whose frame DOES carry the
//! live `ReturnSetInfo`). This is the faithful `FunctionCallInvoke`-with-
//! `resultinfo` over the executor frame — it mirrors `fmgr_builtins[]` for the
//! executor-frame ABI, exactly as the C `fn_addr` is the same callable for both
//! call shapes. SRFs register their executor-frame core here (e.g.
//! `generate_series_int4`, OID 1066/1067/1068, registered by
//! `backend-utils-adt-int`'s `init_seams`).

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use backend_utils_error::ereport;
use mcx::{Mcx, MemoryContext, PgBox};
use types_core::fmgr::FmgrInfo;
use types_core::Oid;
use types_datum::NullableDatum;
use types_error::error::{
    ERRCODE_DATATYPE_MISMATCH, ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INTERNAL_ERROR, ERRCODE_TOO_MANY_ARGUMENTS,
};
use types_error::{PgResult, ERROR};
use types_nodes::execexpr::{ExprDoneCond, SetExprState};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::{
    ReturnSetInfo, SetFunctionReturnMode, Tuplestorestate, SFRM_Materialize,
    SFRM_Materialize_Preferred, SFRM_Materialize_Random, SFRM_ValuePerCall,
};
use types_nodes::primnodes::Expr;
use types_nodes::{EcxtId, EStateData, PlanStateData};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::TupleDescData;

use backend_executor_execSRF_seams as seams;

mod generate_series;
mod srf_registry;
pub use srf_registry::{register_srf, srf_invoke_by_oid, srf_is_registered};

#[cfg(test)]
mod tests;

/// Install this unit's seams. Idempotent in spirit (the seam registry tolerates
/// re-set in tests via the framework). Called from `seams-init`.
pub fn init_seams() {
    seams::exec_init_table_function_result::set(ExecInitTableFunctionResult);
    seams::exec_make_table_function_result::set(ExecMakeTableFunctionResult);
    seams::exec_init_function_result_set::set(ExecInitFunctionResultSet);
    seams::exec_make_function_result_set::set(ExecMakeFunctionResultSet);
    // The executor-frame `fmgrtab` analogue for the int4/int8 generate_series
    // SRFs (the by-OID builtin registry's tag-only resultinfo can't carry the
    // live ReturnSetInfo — WONTFIX dual-home).
    generate_series::register_generate_series();
}

// ===========================================================================
//  init_sexpr — initialize a SetExprState node during first use (execSRF.c:695)
// ===========================================================================

/// `init_sexpr(foid, input_collation, node, sexpr, parent, sexprCxt, allowSRF,
/// needDescForSRF)` (execSRF.c:695).
///
/// The faithful C does the `object_aclcheck` / `InvokeFunctionExecuteHook` /
/// `FUNC_MAX_ARGS` guard, `fmgr_info_cxt` + `fmgr_info_set_expr`, builds the
/// `fcinfo`, and (for a `fn_retset` function with `needDescForSRF`) prepares the
/// expected `funcResultDesc` via `get_expr_result_type`.
///
/// In the owned model the function is dispatched through the executor-frame SRF
/// table (`srf_registry`), so the FmgrInfo carries the OID + the resolved
/// `proisstrict`/`proretset` flags (read by lsyscache). The fcinfo is built
/// sized for the args. The `funcResultDesc` precomputation belongs to the
/// targetlist (ProjectSet) path and is computed there; the table-function path
/// (`ExecMakeTableFunctionResult`) builds its descriptor lazily from the
/// expected/returned type, so `needDescForSRF` is `false` for it.
fn init_sexpr<'mcx>(
    foid: Oid,
    input_collation: Oid,
    sexpr: &mut SetExprState<'mcx>,
    allow_srf: bool,
    _need_desc_for_srf: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // C: aclresult = object_aclcheck(ProcedureRelationId, foid, GetUserId(),
    //                                ACL_EXECUTE); ...; InvokeFunctionExecuteHook(foid);
    // (Execute-permission check + hook — not modeled at this layer; the planner
    // already resolved the call. Faithful to the no-op when ACL is open.)

    let numargs = sexpr.args.as_ref().map(|a| a.len()).unwrap_or(0);

    // C: if (list_length(sexpr->args) > FUNC_MAX_ARGS) ereport(...);
    // FUNC_MAX_ARGS = 100. A planner-checked call never exceeds it; surface
    // loudly if it does.
    const FUNC_MAX_ARGS: usize = 100;
    if numargs > FUNC_MAX_ARGS {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
            .errmsg("cannot pass more than 100 arguments to a function")
            .into_error());
    }

    // C: fmgr_info_cxt(foid, &(sexpr->func), sexprCxt);
    //    fmgr_info_set_expr((Node *) sexpr->expr, &(sexpr->func));
    // The owned FmgrInfo carries the OID and resolved flags; the executor-frame
    // SRF table is the `fn_addr` re-resolution at dispatch.
    let fn_retset = backend_utils_cache_lsyscache_seams::get_func_retset::call(foid)?;
    let fn_strict = backend_utils_cache_lsyscache_seams::func_strict::call(foid)?;
    sexpr.func = FmgrInfo {
        fn_addr: 0,
        fn_oid: foid,
        fn_nargs: numargs as i16,
        fn_strict,
        fn_retset,
        fn_stats: 0,
        fn_expr: None,
    };

    // C: sexpr->fcinfo = palloc(SizeForFunctionCallInfo(numargs));
    //    InitFunctionCallInfoData(*sexpr->fcinfo, &(sexpr->func), numargs,
    //                             input_collation, NULL, NULL);
    let mut args = Vec::with_capacity(numargs);
    args.resize(numargs, NullableDatum::default());
    let fcinfo = FunctionCallInfoBaseData {
        flinfo: Some(sexpr.func.clone()),
        context: None,
        resultinfo: None,
        fncollation: input_collation,
        isnull: false,
        nargs: numargs as i16,
        args,
        ref_args: Vec::new(),
        fn_extra: None,
        fn_mcxt: None,
    };
    sexpr.fcinfo = Some(mcx::alloc_in(estate.es_query_cxt, fcinfo)?);

    // C: if (sexpr->func.fn_retset && !allowSRF) ereport(ERROR, "set-valued
    //    function called in context that cannot accept a set");
    if sexpr.func.fn_retset && !allow_srf {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("set-valued function called in context that cannot accept a set")
            .into_error());
    }

    // C: Assert(sexpr->func.fn_retset == sexpr->funcReturnsSet);
    // (the caller set funcReturnsSet; keep them in sync for the ProjectSet path.)

    // C: funcResultStore = NULL; funcResultSlot = NULL; shutdown_reg = false;
    sexpr.funcResultStore = None;
    sexpr.funcResultSlot = None;
    sexpr.shutdown_reg = false;

    Ok(())
}

// ===========================================================================
//  ExecInitTableFunctionResult (execSRF.c:55)
// ===========================================================================

/// `ExecInitTableFunctionResult(expr, econtext, parent)` (execSRF.c:55) — build
/// the [`SetExprState`] for a function in a range-table function (FunctionScan /
/// ROWS FROM).
fn ExecInitTableFunctionResult<'mcx>(
    expr: &Expr,
    _econtext: EcxtId,
    parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, SetExprState<'mcx>>> {
    let per_query = estate.es_query_cxt;
    let mut state = SetExprState::default();
    // C: state->funcReturnsSet = false; state->func.fn_oid = InvalidOid;
    state.funcReturnsSet = false;
    state.func.fn_oid = Oid::default();

    // C: state->expr = expr;
    state.expr = Some(mcx::alloc_in(per_query, expr.clone_in(per_query)?)?);

    if let Expr::FuncExpr(func) = expr {
        // C: state->funcReturnsSet = func->funcretset;
        //    state->args = ExecInitExprList(func->args, parent);
        //    init_sexpr(func->funcid, func->inputcollid, expr, state, parent,
        //               econtext->ecxt_per_query_memory, func->funcretset, false);
        state.funcReturnsSet = func.funcretset;
        state.args = Some(init_expr_list(&func.args, parent, estate)?);
        init_sexpr(func.funcid, func.inputcollid, &mut state, func.funcretset, false, estate)?;
    } else {
        // C: state->elidedFuncState = ExecInitExpr(expr, parent);
        let es = backend_executor_execExpr_seams::exec_init_expr::call(expr, parent, estate)?;
        state.elidedFuncState = Some(es);
    }

    mcx::alloc_in(per_query, state)
}

/// `ExecInitExprList(args, parent)` over the function's argument expressions.
/// A NULL `Expr *` cell compiles to a `None` `ExprState` in C, but the SetExprState
/// `args` carries `ExprState` by value (positional), so we surface any NULL cell
/// loudly (an SRF call argument list never contains a NULL expression).
fn init_expr_list<'mcx>(
    args: &[Expr],
    parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<mcx::PgVec<'mcx, types_nodes::execexpr::ExprState<'mcx>>> {
    let _ = parent;
    let refs: Vec<Option<&Expr>> = args.iter().map(Some).collect();
    let states =
        backend_executor_execExpr_seams::exec_init_expr_list_no_parent::call(&refs, estate)?;
    let mut out = mcx::PgVec::new_in(estate.es_query_cxt);
    out.try_reserve(states.len()).map_err(|_| {
        estate
            .es_query_cxt
            .oom(states.len() * core::mem::size_of::<types_nodes::execexpr::ExprState>())
    })?;
    for s in states.into_iter() {
        out.push(s.expect("SRF argument expression compiled to a non-NULL ExprState"));
    }
    Ok(out)
}

// ===========================================================================
//  ExecMakeTableFunctionResult (execSRF.c:100) — the K2 value-per-call loop
// ===========================================================================

/// `ExecMakeTableFunctionResult(setexpr, econtext, argContext, expectedDesc,
/// randomAccess)` (execSRF.c:100) — evaluate a table function, producing a
/// materialized result in a Tuplestore. The faithful ValuePerCall +
/// Materialize-mode loop, dispatching the SRF through the executor-frame table
/// while threading the live `ReturnSetInfo`.
fn ExecMakeTableFunctionResult<'mcx>(
    setexpr: &mut SetExprState<'mcx>,
    econtext: EcxtId,
    arg_context: &mut MemoryContext,
    expected_desc: &TupleDescData<'mcx>,
    random_access: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, Tuplestorestate<'mcx>>> {
    let per_query: Mcx<'mcx> = estate.es_query_cxt;

    // C: MemoryContextReset(argContext);
    //    callerContext = MemoryContextSwitchTo(argContext);
    arg_context.reset();

    // C: funcrettype = exprType((Node *) setexpr->expr);
    //    returnsTuple = type_is_rowtype(funcrettype);
    let funcrettype =
        backend_nodes_core::nodefuncs::expr_type(setexpr.expr.as_deref())?;
    let returns_tuple =
        backend_utils_cache_lsyscache_seams::type_is_rowtype::call(funcrettype)?;

    // C: rsinfo.type = T_ReturnSetInfo; econtext/expectedDesc/allowedModes/...
    let mut allowed_modes =
        SFRM_ValuePerCall | SFRM_Materialize | SFRM_Materialize_Preferred;
    if random_access {
        allowed_modes |= SFRM_Materialize_Random;
    }
    let mut rsinfo = ReturnSetInfo {
        econtext: Some(econtext),
        expectedDesc: Some(mcx::alloc_in(per_query, expected_desc.clone_in(per_query)?)?),
        allowedModes: allowed_modes,
        returnMode: SetFunctionReturnMode::ValuePerCall,
        isDone: ExprDoneCond::ExprSingleResult,
        setResult: Tuplestorestate::default(),
        setDesc: None,
    };

    // For a scalar return type the loop builds a 1-column descriptor lazily.
    let mut tupdesc: Option<PgBox<'mcx, TupleDescData<'mcx>>> = None;
    let mut first_time = true;
    let returns_set = setexpr.funcReturnsSet;
    let elided = setexpr.elidedFuncState.is_some();

    // C: fcinfo = palloc(SizeForFunctionCallInfo(...));
    //    InitFunctionCallInfoData(*fcinfo, &(setexpr->func), ...,
    //                             setexpr->fcinfo->fncollation, NULL, &rsinfo);
    // The owned model dispatches through `setexpr->fcinfo` (the long-lived call
    // frame); the live ReturnSetInfo is threaded onto it for the call, then
    // recovered. `fn_extra`/`fn_mcxt` channels persist across the row series.

    'no_function_result: {
        if !elided {
            // C: ExecEvalFuncArgs(fcinfo, setexpr->args, econtext);
            // The args were compiled into setexpr->args; evaluate them in the
            // argContext (the caller already switched into it).
            exec_eval_func_args(setexpr, econtext, estate)?;

            // C: if (setexpr->func.fn_strict) { for each arg if NULL goto
            //    no_function_result; }
            if setexpr.func.fn_strict {
                let fcinfo = setexpr
                    .fcinfo
                    .as_ref()
                    .expect("ExecMakeTableFunctionResult: fcinfo not initialized");
                if fcinfo.args.iter().any(|a| a.isnull) {
                    break 'no_function_result;
                }
            }
        }

        // C: MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
        //    for (;;) { ... ValuePerCall protocol ... }
        loop {
            // CHECK_FOR_INTERRUPTS();
            // C: ResetExprContext(econtext);
            estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();

            // C: rsinfo.isDone = ExprSingleResult; result = FunctionCallInvoke(fcinfo);
            let (result, result_isnull) = if !elided {
                let fcinfo = setexpr
                    .fcinfo
                    .as_mut()
                    .expect("ExecMakeTableFunctionResult: fcinfo not initialized");
                fcinfo.isnull = false;
                // Thread the live ReturnSetInfo + cross-call channels onto the
                // frame for the call, dispatch, then take it back.
                fcinfo.resultinfo = Some(core::mem::take(&mut rsinfo));
                fcinfo.fn_mcxt = Some(per_query);
                fcinfo.resultinfo.as_mut().unwrap().isDone =
                    ExprDoneCond::ExprSingleResult;
                let foid = setexpr.func.fn_oid;
                let res = srf_invoke_by_oid(foid, fcinfo)?;
                let isnull = fcinfo.isnull;
                rsinfo = fcinfo
                    .resultinfo
                    .take()
                    .expect("ExecMakeTableFunctionResult: resultinfo round-trip");
                (res, isnull)
            } else {
                // C: result = ExecEvalExpr(setexpr->elidedFuncState, econtext,
                //                          &fcinfo->isnull); rsinfo.isDone = ExprSingleResult;
                let st = setexpr
                    .elidedFuncState
                    .as_deref_mut()
                    .expect("elidedFuncState present");
                let (d, isnull) =
                    backend_executor_execExpr_seams::exec_eval_expr_switch_context::call(
                        st, econtext, estate,
                    )?;
                rsinfo.isDone = ExprDoneCond::ExprSingleResult;
                (d, isnull)
            };

            // C: if (rsinfo.returnMode == SFRM_ValuePerCall) { ... }
            match rsinfo.returnMode {
                SetFunctionReturnMode::ValuePerCall => {
                    // C: if (rsinfo.isDone == ExprEndResult) break;
                    if rsinfo.isDone == ExprDoneCond::ExprEndResult {
                        break;
                    }

                    // C: if (first_time) { build tuplestore (+scalar tupdesc) }
                    if first_time {
                        let ts = backend_utils_sort_storage_seams::tuplestore_begin_heap::call(
                            per_query,
                            random_access,
                            false,
                            backend_utils_init_small_seams::work_mem::call(),
                        )?;
                        rsinfo.setResult = allocator_api2::boxed::Box::into_inner(ts);
                        if !returns_tuple {
                            // CreateTemplateTupleDesc(1) + TupleDescInitEntry(1,
                            //     "column", funcrettype, -1, 0).
                            let td = backend_access_common_tupdesc::CreateTemplateTupleDesc(
                                per_query, 1,
                            )?;
                            let mut td = mcx::alloc_in(per_query, td)?;
                            backend_access_common_tupdesc::TupleDescInitEntry(
                                &mut td,
                                1,
                                Some("column"),
                                funcrettype,
                                -1,
                                0,
                            )?;
                            tupdesc = Some(td);
                            // rsinfo.setDesc points at the built desc (a copy for
                            // the cross-check below).
                            rsinfo.setDesc =
                                Some(mcx::alloc_in(per_query, tupdesc.as_ref().unwrap().clone_in(per_query)?)?);
                        }
                    }

                    // C: store current resultset item.
                    if returns_tuple {
                        // Composite return: C stores the returned HeapTupleHeader
                        // Datum directly (`tuplestore_puttuple`). The owned model
                        // carries it as `Datum::Composite(FormedTuple)`; decomposing
                        // it into per-column values + `tuplestore_putvalues` is
                        // BLOCKED on the by-reference-Datum varlena-header convention
                        // unification (the deep follow-on): `heap_deform_tuple` /
                        // `tuplestore_putvalues` carry a varlena `ByRef` as the FULL
                        // on-disk image (header included), but the const/output lane
                        // (`textout` → `text_to_cstring`) reads a HEADER-LESS payload,
                        // so a text column round-tripped through a tuplestore prints
                        // with a stray header byte. Surfaces loudly until the ByRef
                        // varlena-header convention is unified tree-wide.
                        let _ = (&result, result_isnull);
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                            .errmsg(
                                "composite-returning table function not yet supported \
                                 (by-reference Datum varlena-header convention unification \
                                 keystone: heap-tuple ByRef is header-full, the output lane \
                                 reads header-less)",
                            )
                            .into_error());
                    } else {
                        // C: tuplestore_putvalues(tupstore, tupdesc, &result,
                        //                         &fcinfo->isnull);
                        let td = tupdesc
                            .as_deref()
                            .expect("scalar SRF: tupdesc built on first_time");
                        let values = [result];
                        let nulls = [result_isnull];
                        backend_utils_sort_storage_seams::tuplestore_putvalues::call(
                            &mut rsinfo.setResult,
                            td,
                            &values,
                            &nulls,
                        )?;
                    }

                    // C: if (rsinfo.isDone != ExprMultipleResult) break;
                    if rsinfo.isDone != ExprDoneCond::ExprMultipleResult {
                        break;
                    }

                    // C: if (!returnsSet) ereport(ERROR, "table-function
                    //    protocol for value-per-call mode was not followed");
                    if !returns_set {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED)
                            .errmsg(
                                "table-function protocol for value-per-call mode was not followed",
                            )
                            .into_error());
                    }
                }
                SetFunctionReturnMode::Materialize => {
                    // C: if (!first_time || rsinfo.isDone != ExprSingleResult ||
                    //        !returnsSet) ereport(ERROR, "... materialize ...");
                    if !first_time
                        || rsinfo.isDone != ExprDoneCond::ExprSingleResult
                        || !returns_set
                    {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED)
                            .errmsg(
                                "table-function protocol for materialize mode was not followed",
                            )
                            .into_error());
                    }
                    // Done evaluating the set result.
                    break;
                }
            }

            first_time = false;
        }
    }

    // no_function_result:
    // C: if (rsinfo.setResult == NULL) { create tuplestore; if (!returnsSet)
    //    putvalues a single all-nulls row from expectedDesc; }
    if rsinfo.setResult.payload().is_none() {
        let ts = backend_utils_sort_storage_seams::tuplestore_begin_heap::call(
            per_query,
            random_access,
            false,
            backend_utils_init_small_seams::work_mem::call(),
        )?;
        rsinfo.setResult = allocator_api2::boxed::Box::into_inner(ts);

        if !returns_set {
            // natts all-nulls row from expectedDesc.
            let natts = expected_desc.natts.max(0) as usize;
            let values: Vec<Datum> = (0..natts).map(|_| Datum::default()).collect();
            let nulls: Vec<bool> = (0..natts).map(|_| true).collect();
            backend_utils_sort_storage_seams::tuplestore_putvalues::call(
                &mut rsinfo.setResult,
                expected_desc,
                &values,
                &nulls,
            )?;
        }
    }

    // C: if (rsinfo.setDesc) { tupledesc_match(expectedDesc, rsinfo.setDesc);
    //    if (rsinfo.setDesc->tdrefcount == -1) FreeTupleDesc(rsinfo.setDesc); }
    if let Some(set_desc) = rsinfo.setDesc.as_deref() {
        tupledesc_match(expected_desc, set_desc)?;
        // Dynamically-allocated TupleDesc is dropped by ownership (RAII).
    }

    // C: MemoryContextSwitchTo(callerContext); return rsinfo.setResult;
    let setResult = core::mem::take(&mut rsinfo.setResult);
    mcx::alloc_in(per_query, setResult)
}

/// `ExecEvalFuncArgs(fcinfo, argList, econtext)` (execSRF.c:833) — evaluate the
/// function's argument expressions into `fcinfo->args[]`.
fn exec_eval_func_args<'mcx>(
    sexpr: &mut SetExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Evaluate each compiled arg ExprState into the call frame's args cells.
    // The arg states live in `sexpr.args`; the frame in `sexpr.fcinfo`.
    let n = sexpr.args.as_ref().map(|a| a.len()).unwrap_or(0);
    for i in 0..n {
        let (value, isnull) = {
            let argstate = &mut sexpr.args.as_mut().unwrap()[i];
            backend_executor_execExpr_seams::exec_eval_expr_switch_context::call(
                argstate, econtext, estate,
            )?
        };
        let fcinfo = sexpr
            .fcinfo
            .as_mut()
            .expect("ExecEvalFuncArgs: fcinfo not initialized");
        // The compiled argument expression produced a canonical
        // `types_tuple::Datum`; the fmgr call frame carries the bare-word
        // `args[i].value` plus the by-reference `ref_args[i]` side channel.
        // Marshal each kind onto the frame: a by-value scalar is the bare word
        // (no referent); a by-reference value (text/varlena/cstring/composite)
        // passes a null word plus its image in `ref_args[i]` — exactly the C
        // "`args[i].value` is a pointer to the referent" convention, so the
        // callee's `PG_GETARG_TEXT_PP`/`PG_GETARG_CSTRING` readers see the
        // value. (The old `as_usize()` downgrade panicked on a by-ref arg —
        // the `pg_input_error_info('junk','bool')` wall.)
        use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
        use types_nodes::fmgr::FmgrArgRef;
        match value {
            CanonDatum::ByVal(word) => {
                fcinfo.args[i].value = types_datum::Datum::from_usize(word);
            }
            CanonDatum::ByRef(bytes) => {
                fcinfo.args[i].value = types_datum::Datum::null();
                fcinfo.set_ref_arg(i, FmgrArgRef::Varlena(bytes.as_slice().to_vec()));
            }
            CanonDatum::Cstring(s) => {
                fcinfo.args[i].value = types_datum::Datum::null();
                fcinfo.set_ref_arg(i, FmgrArgRef::Cstring(s.to_string()));
            }
            CanonDatum::Composite(t) => {
                fcinfo.args[i].value = types_datum::Datum::null();
                fcinfo.set_ref_arg(i, FmgrArgRef::Varlena(t.to_datum_image()));
            }
            CanonDatum::Expanded(_) | CanonDatum::Internal(_) => {
                return Err(types_error::PgError::error(
                    "ExecEvalFuncArgs: Expanded/Internal argument not supported on the SRF call frame",
                ));
            }
        }
        fcinfo.args[i].isnull = isnull;
    }
    Ok(())
}

// ===========================================================================
//  ExecInitFunctionResultSet / ExecMakeFunctionResultSet (ProjectSet path)
// ===========================================================================

/// `ExecInitFunctionResultSet(expr, econtext, parent)` (execSRF.c:443) — prepare
/// a targetlist SRF for execution (nodeProjectSet.c).
fn ExecInitFunctionResultSet<'mcx>(
    expr: &Expr,
    _econtext: EcxtId,
    parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, SetExprState<'mcx>>> {
    let per_query = estate.es_query_cxt;
    let mut state = SetExprState::default();
    // C: state->funcReturnsSet = true; state->func.fn_oid = InvalidOid;
    state.funcReturnsSet = true;
    state.func.fn_oid = Oid::default();
    state.expr = Some(mcx::alloc_in(per_query, expr.clone_in(per_query)?)?);

    match expr {
        Expr::FuncExpr(func) => {
            // C: state->args = ExecInitExprList(func->args, parent);
            //    init_sexpr(func->funcid, func->inputcollid, ..., true, true);
            state.args = Some(init_expr_list(&func.args, parent, estate)?);
            init_sexpr(func.funcid, func.inputcollid, &mut state, true, true, estate)?;
        }
        Expr::OpExpr(op) => {
            // C: state->args = ExecInitExprList(op->args, parent);
            //    init_sexpr(op->opfuncid, op->inputcollid, ..., true, true);
            state.args = Some(init_expr_list(&op.args, parent, estate)?);
            init_sexpr(op.opfuncid, op.inputcollid, &mut state, true, true, estate)?;
        }
        other => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg(alloc::format!("unrecognized node type: {other:?}"))
                .into_error());
        }
    }

    // C: Assert(state->func.fn_retset);  (the selected function returns a set.)
    mcx::alloc_in(estate.es_query_cxt, state)
}

/// `ExecMakeFunctionResultSet(fcache, econtext, argContext, &isNull, &isDone)`
/// (execSRF.c:496) — evaluate a targetlist SRF and return one result row's
/// `(Datum, isNull, isDone)`. nodeProjectSet.c.
fn ExecMakeFunctionResultSet<'mcx>(
    _fcache: &mut SetExprState<'mcx>,
    _econtext: EcxtId,
    _arg_context: &MemoryContext,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool, ExprDoneCond)> {
    // The ProjectSet (targetlist-SRF) value-per-call + tuplestore-drain loop
    // shares the same executor-frame SRF dispatch as the table-function path.
    // It additionally needs `funcResultSlot` (a TupleTableSlot) drained from the
    // funcResultStore via `tuplestore_gettupleslot` + `slot_getattr` /
    // `ExecFetchSlotHeapTupleDatum`, plus `RegisterExprContextCallback` for the
    // mid-series shutdown. The slot-drain leg crosses the execTuples slot owner;
    // it is wired by nodeProjectSet's own tests today. The table-function path
    // (`ExecMakeTableFunctionResult`) is the K2 milestone and is fully wired
    // above; the ProjectSet entry is left as the documented loud boundary so it
    // is not a silent fake.
    Err(ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(
            "ExecMakeFunctionResultSet (targetlist-SRF drain) not yet wired \
             (table-function path ExecMakeTableFunctionResult is the K2 milestone)",
        )
        .into_error())
}

// ===========================================================================
//  tupledesc_match (execSRF.c:942)
// ===========================================================================

/// `tupledesc_match(dst_tupdesc, src_tupdesc)` (execSRF.c:942) — check that the
/// function's result tuple type matches what the query expects (number of
/// attributes; per-attribute binary-coercibility, ignoring dropped columns
/// whose physical storage still matches).
fn tupledesc_match<'mcx>(
    dst: &TupleDescData<'mcx>,
    src: &TupleDescData<'mcx>,
) -> PgResult<()> {
    // C: if (dst->natts != src->natts) ereport(ERROR, "function return row and
    //    query-specified return row do not match");
    if dst.natts != src.natts {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("function return row and query-specified return row do not match")
            .into_error());
    }
    // Per-attribute checks: the full IsBinaryCoercible / dropped-column physical
    // storage cross-check needs the per-attribute Form_pg_attribute fields and
    // the coercion catalog. For the scalar 1-column SRF path the natts check is
    // the operative guard (a single matching column). The richer per-attribute
    // RECORD cross-check lands with the composite-returning path.
    Ok(())
}
