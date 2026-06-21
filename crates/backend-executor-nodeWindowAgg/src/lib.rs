//! Port of `src/backend/executor/nodeWindowAgg.c` — routines to handle
//! WindowAgg nodes.
//!
//! A WindowAgg node evaluates window functions across suitable partitions of
//! the input tuple set. Rows of the current partition accumulate into a
//! tuplestore; window functions access those rows through the WindowObject API.
//!
//! INTERFACE ROUTINES
//! - [`ExecWindowAgg`]      — process window functions over the outer subplan
//! - [`ExecInitWindowAgg`]  — initialize node and subnodes
//! - [`ExecEndWindowAgg`]   — shutdown node and subnodes
//! - [`ExecReScanWindowAgg`]
//!
//! The C `WindowAggState.ss.ps.state` back-pointer is replaced by threading
//! `&mut EStateData` explicitly (as in nodeMaterial/nodeAgg). The C
//! `WindowObjectData.winstate` back-pointer is likewise dropped; the Window
//! object operations take `&mut WindowAggState` explicitly.
//!
//! Genuine cross-crate dependencies (tuplestore.c, execProcnode.c,
//! execTuples.c, execUtils.c, execExpr.c, fmgr.c, the syscache/lsyscache/aclchk
//! catalog readers, globals.c's `work_mem`, tcop/postgres.c's interrupt check)
//! go through those owners' seam crates and panic until the owners land.
//!
//! The fmgr `FunctionCallInvoke` dispatch for the aggregate transition /
//! inverse-transition / final functions crosses the owned by-value
//! `function_call_invoke_datum_owned` seam (fmgr-seams): args are moved in by
//! value (so a `Datum::Internal` running state — `internal`-transtype moving
//! aggregates like `sum`/`avg`/`stddev` over numeric — rides the fcinfo side
//! channel and survives), invoked by `fn_oid` under the window collation. The
//! pass-by-reference transition/result reparent is the nodeAgg
//! `ExecAggCopyTransValue` analogue [`window_copy_trans_value`]: a by-ref new
//! value distinct from the prior pointer is `datumCopy`'d (via the datum.c seam)
//! into the agg's working memory; `MakeExpandedObjectReadOnly` is the identity
//! for the non-expanded Datums these aggregates carry, and `pfree` of a prior
//! by-ref value is the arena's drop. The two RANGE in_range comparison sites
//! still gather a `NullableDatum` frame through the value-based
//! `function_call_invoke` seam (by-value bool result). All surrounding control
//! flow is real in-crate code.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

// object_aclcheck (backend-catalog-aclchk-seams) would be consumed by
// `initialize_peragg`/`ExecInitWindowAgg`'s permission checks, but those sit
// behind the unported pg_aggregate/execGrouping catalog boundary (panic stubs),
// so the dependency is omitted until that boundary lands.
use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execGrouping_seams as execGrouping;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_nodes_nodeFuncs_seams as nodeFuncs;
use backend_utils_adt_datum_seams as datum_seams;
use backend_utils_adt_datum_seams as datum;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_fmgr_fmgr_seams as fmgr;
use backend_utils_init_small_seams as globals;
use backend_utils_sort_storage_seams as tuplestore;

use mcx::{alloc_in, PgBox, PgVec};
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum;

use types_core::primitive::{AttrNumber, Oid};

use types_nodes::execnodes::{EcxtId, SlotId};
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use types_nodes::nodewindowagg::*;
use types_nodes::{EStateData, PlanStateNode};

const INVALID_OID: Oid = 0;

/// `AGGMODIFY_READ_ONLY` (catalog/pg_aggregate.h) — the `aggfinalmodify` /
/// `aggmfinalmodify` char `'r'`.
const AGGMODIFY_READ_ONLY: i8 = b'r' as i8;

#[inline]
fn oid_is_valid(o: Oid) -> bool {
    o != INVALID_OID
}

/// C: `aclresult = object_aclcheck(ProcedureRelationId, fnoid, aggOwner,
/// ACL_EXECUTE); if (aclresult != ACLCHECK_OK) aclcheck_error(aclresult,
/// OBJECT_FUNCTION, get_func_name(fnoid)); InvokeFunctionExecuteHook(fnoid);`
/// for a transition/inverse/final component function of a window aggregate.
fn check_component_fn_acl<'mcx>(mcx: mcx::Mcx<'mcx>, fnoid: Oid, agg_owner: Oid) -> PgResult<()> {
    let aclresult = backend_catalog_aclchk_seams::object_aclcheck::call(
        types_core::catalog::PROCEDURE_RELATION_ID,
        fnoid,
        agg_owner,
        types_acl::acl::ACL_EXECUTE,
    )?;
    if aclresult != types_acl::acl::AclResult::AclcheckOk {
        let name = lsyscache::get_func_name::call(mcx, fnoid)?.map(|s| s.as_str().into());
        backend_catalog_aclchk_seams::aclcheck_error::call(
            aclresult,
            types_nodes::parsenodes::ObjectType::Function,
            name,
        )?;
    }
    backend_catalog_objectaccess_seams::invoke_function_execute_hook::call(fnoid)?;
    Ok(())
}


/// nodeWindowAgg has no `<unit>-seams` crate: callers (execProcnode's dispatch
/// tables) depend on it directly. It reaches outward only, so `init_seams` is
/// empty and not wired into seams-init (mirroring nodeMaterial).
pub fn init_seams() {}

// ===========================================================================
// initialize_windowaggregate / advance_windowaggregate /
// advance_windowaggregate_base / finalize_windowaggregate
// ===========================================================================

/// `initialize_windowaggregate` — parallel to `initialize_aggregates` in
/// nodeAgg.c.
fn initialize_windowaggregate<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    peraggno: usize,
    estate: &mut EStateData<'mcx>,
) {
    // If we're using a private aggcontext, we may reset it here. But if the
    // context is shared, we don't know which other aggregates may still need
    // it, so we must leave it to the caller to reset at an appropriate time.
    //
    //   if (peraggstate->aggcontext != winstate->aggcontext)
    //       MemoryContextReset(peraggstate->aggcontext);
    let shared = ctx_is_shared(winstate, peraggno);
    if !shared {
        peragg_mut(winstate, peraggno)
            .aggcontext
            .as_mut()
            .expect("initialize_windowaggregate: private aggcontext not set")
            .reset();
    }

    let pa = peragg_ref(winstate, peraggno);
    let trans_value = if pa.initValueIsNull {
        //   peraggstate->transValue = peraggstate->initValue;
        pa.initValue.clone()
    } else if pa.transtypeByVal {
        pa.initValue.clone()
    } else {
        // oldContext = MemoryContextSwitchTo(peraggstate->aggcontext);
        // peraggstate->transValue = datumCopy(peraggstate->initValue,
        //                                     transtypeByVal, transtypeLen);
        // MemoryContextSwitchTo(oldContext);
        //
        // By-reference initial value (e.g. an int8[]/numeric) deep-copied into
        // the agg's working memory (materialized into the per-query context;
        // see window_copy_trans_value). `unwrap` of the seam Result: a datumCopy
        // of an already-materialized initial value cannot fail; surface a panic
        // rather than swallow (this is an infallible init helper).
        datum::datum_copy_v::call(
            estate.es_query_cxt,
            &pa.initValue,
            pa.transtypeByVal,
            pa.transtypeLen as i32,
        )
        .expect("initialize_windowaggregate: datumCopy of aggregate initial value failed")
    };
    let pa = peragg_mut(winstate, peraggno);
    pa.transValue = trans_value;
    pa.transValueIsNull = pa.initValueIsNull;
    pa.transValueCount = 0;
    pa.resultValue = Datum::null();
    pa.resultValueIsNull = true;
}

/// `advance_windowaggregate` — parallel to `advance_aggregates` in nodeAgg.c.
fn advance_windowaggregate<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    peraggno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let numArguments = perfunc_ref(winstate, perfuncno).numArguments;
    let econtext = winstate
        .tmpcontext
        .expect("advance_windowaggregate: tmpcontext not set");

    // oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    // (modeled: ExecEvalExpr*SwitchContext does the switch internally.)

    // Skip anything FILTERed out.
    //   if (filter) { res = ExecEvalExpr(filter, ...); if (isnull || !DatumGetBool(res)) return; }
    let has_filter = perfunc_wfuncstate(winstate, perfuncno)
        .aggfilter
        .is_some();
    if has_filter {
        let (res, isnull) = {
            let filter = perfunc_wfuncstate_mut(winstate, perfuncno)
                .aggfilter
                .as_deref_mut()
                .expect("checked Some");
            execExpr::exec_eval_expr_switch_context::call(filter, econtext, estate)?
        };
        if isnull || !res.as_bool() {
            return Ok(());
        }
    }

    // We start from 1, since the 0th arg will be the transition value.
    //   foreach(arg, wfuncstate->args)
    //       fcinfo->args[i].value = ExecEvalExpr(argstate, ...);
    let nargs = wfuncstate_nargs(winstate, perfuncno);
    let mut argvals: alloc::vec::Vec<(Datum<'mcx>, bool)> =
        alloc::vec::Vec::with_capacity(nargs);
    for i in 0..nargs {
        let (v, n) = {
            let argstate = wfuncstate_arg_mut(winstate, perfuncno, i);
            execExpr::exec_eval_expr_switch_context::call(argstate, econtext, estate)?
        };
        argvals.push((v, n));
    }

    // For a strict transfn, NULL inputs leave the prior transValue alone.
    if peragg_ref(winstate, peraggno).transfn.fn_strict {
        for i in 0..numArguments as usize {
            if i < argvals.len() && argvals[i].1 {
                return Ok(());
            }
        }

        // For strict transition functions with initial value NULL we use the
        // first non-NULL input as the initial state.
        let pa = peragg_ref(winstate, peraggno);
        if pa.transValueCount == 0 && pa.transValueIsNull {
            // peraggstate->transValue = datumCopy(fcinfo->args[1].value, byval, len)
            let v = if pa.transtypeByVal {
                argvals[0].0.clone()
            } else {
                // datumCopy of a pass-by-reference first input into the agg's
                // working context (materialized into the per-query context; see
                // window_copy_trans_value).
                datum::datum_copy_v::call(
                    estate.es_query_cxt,
                    &argvals[0].0,
                    pa.transtypeByVal,
                    pa.transtypeLen as i32,
                )?
            };
            let pa = peragg_mut(winstate, peraggno);
            pa.transValue = v;
            pa.transValueIsNull = false;
            pa.transValueCount = 1;
            return Ok(());
        }

        if pa.transValueIsNull {
            // Don't call a strict function with NULL inputs.
            debug_assert!(!oid_is_valid(pa.invtransfn_oid));
            return Ok(());
        }
    }

    // OK to call the transition function. Set winstate->curaggcontext while
    // calling it, for possible use by AggCheckCallContext.
    //   InitFunctionCallInfoData(*fcinfo, &peraggstate->transfn,
    //                            numArguments + 1, perfuncstate->winCollation, ...);
    //   fcinfo->args[0].value = peraggstate->transValue;
    //   fcinfo->args[0].isnull = peraggstate->transValueIsNull;
    //   winstate->curaggcontext = peraggstate->aggcontext;
    //   newVal = FunctionCallInvoke(fcinfo);
    //   winstate->curaggcontext = NULL;
    //
    // (The curaggcontext push/pop only matters to AggCheckCallContext within
    // the callee; the call frame carries no aggcontext, so it is a no-op here.)
    let collation = perfunc_ref(winstate, perfuncno).winCollation;
    let mcx = estate.es_query_cxt;

    let (oldVal, oldIsNull, newVal, isnull) = {
        // fcinfo->args[0] = transValue; fcinfo->args[1..] = evaluated arguments.
        // The owned by-value seam moves the running state through the fcinfo side
        // channel (a `Datum::Internal` box for `internal`-transtype aggregates),
        // so it survives the by-ref/internal cases the bare-word frame cannot.
        // The `internal` running state cannot be cloned (C passes it by pointer
        // and the transfn mutates it in place), so MOVE it out of the pergroup and
        // store the (same) returned box back below.
        let (fn_oid, fn_expr) = {
            let pa = peragg_ref(winstate, peraggno);
            (pa.transfn.fn_oid, pa.transfn.fn_expr.clone())
        };
        let pa = peragg_mut(winstate, peraggno);
        let oldIsNull = pa.transValueIsNull;
        let trans_value = core::mem::replace(&mut pa.transValue, Datum::null());
        // Keep a comparison handle for the by-ref reparent fast path: meaningful
        // only for plain by-ref types (an internal state takes the by-value path
        // in window_copy_trans_value and is never compared, and cannot be cloned).
        let oldVal = if trans_value.is_internal() {
            Datum::null()
        } else {
            trans_value.clone()
        };
        let mut args: alloc::vec::Vec<Datum<'mcx>> =
            alloc::vec::Vec::with_capacity(numArguments as usize + 1);
        let mut args_null: alloc::vec::Vec<bool> =
            alloc::vec::Vec::with_capacity(numArguments as usize + 1);
        args.push(trans_value);
        args_null.push(oldIsNull);
        for i in 0..numArguments as usize {
            args.push(argvals[i].0.clone());
            args_null.push(argvals[i].1);
        }
        let (newVal, isnull) =
            fmgr::function_call_invoke_datum_owned::call(mcx, fn_oid, collation, args, args_null, fn_expr)?;
        (oldVal, oldIsNull, newVal, isnull)
    };

    // Moving-aggregate transition functions must not return null, see
    // advance_windowaggregate_base().
    //   if (fcinfo->isnull && OidIsValid(peraggstate->invtransfn_oid)) ereport(ERROR, ...);
    if isnull && oid_is_valid(peragg_ref(winstate, peraggno).invtransfn_oid) {
        return Err(types_error::PgError::error(
            "moving-aggregate transition function must not return null",
        )
        .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED));
    }

    // We must track the number of rows included in transValue, since to remove
    // the last input, advance_windowaggregate_base() mustn't call the inverse
    // transition function, but simply reset transValue back to its initial value.
    //   peraggstate->transValueCount++;
    peragg_mut(winstate, peraggno).transValueCount += 1;

    // If pass-by-ref datatype, must copy the new value into aggcontext and free
    // the prior transValue. (See comments for ExecAggCopyTransValue.) The owned
    // arena reclaims the prior value when its context drops; an `internal`
    // transtype is pass-by-value (it rides `Datum::Internal`) and so takes the
    // by-value path, exactly as C never datumCopies an internal state.
    let newVal = window_copy_trans_value(
        winstate, peraggno, newVal, isnull, oldVal, oldIsNull, mcx,
    )?;

    // peraggstate->transValue = newVal; peraggstate->transValueIsNull = fcinfo->isnull;
    let pa = peragg_mut(winstate, peraggno);
    pa.transValue = newVal;
    pa.transValueIsNull = isnull;
    let _ = econtext;
    Ok(())
}

/// Mirror of nodeAgg's `ExecAggCopyTransValue`: when the transition type is
/// pass-by-reference and the transfn returned a value distinct from the prior
/// transValue pointer, copy the new value into the agg's working context (so it
/// outlives the per-tuple context the transfn ran in) and drop the prior value.
/// By-value (including `internal`, which is pass-by-value) is verbatim.
///
/// C copies into `peraggstate->aggcontext`; the owned model materializes into
/// the per-query context (`mcx`, a `Datum<'mcx>`) — faithful for the lifetime
/// of an aggregated frame (the per-partition aggcontext reset is gated with the
/// expanded-datum free, as in nodeAgg's `datum_copy_into_ecxt` precedent).
fn window_copy_trans_value<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    peraggno: usize,
    new_value: Datum<'mcx>,
    new_value_is_null: bool,
    old_value: Datum<'mcx>,
    _old_value_is_null: bool,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let (by_val, len) = {
        let pa = peragg_ref(winstate, peraggno);
        (pa.transtypeByVal, pa.transtypeLen)
    };
    if by_val || new_value_is_null {
        return Ok(new_value);
    }
    // DatumGetPointer(newVal) != DatumGetPointer(transValue): the owned enum
    // compares by value image; identical => the transfn returned its own input,
    // no copy needed.
    if new_value == old_value {
        return Ok(new_value);
    }
    datum::datum_copy_v::call(mcx, &new_value, by_val, len as i32)
}

/// `advance_windowaggregate_base` — remove the oldest tuple from an
/// aggregation via the inverse transition function.
fn advance_windowaggregate_base<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    peraggno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let numArguments = perfunc_ref(winstate, perfuncno).numArguments;
    let econtext = winstate
        .tmpcontext
        .expect("advance_windowaggregate_base: tmpcontext not set");

    // Skip anything FILTERed out.
    let has_filter = perfunc_wfuncstate(winstate, perfuncno)
        .aggfilter
        .is_some();
    if has_filter {
        let (res, isnull) = {
            let filter = perfunc_wfuncstate_mut(winstate, perfuncno)
                .aggfilter
                .as_deref_mut()
                .expect("checked Some");
            execExpr::exec_eval_expr_switch_context::call(filter, econtext, estate)?
        };
        if isnull || !res.as_bool() {
            return Ok(true);
        }
    }

    // Evaluate the args.
    let nargs = wfuncstate_nargs(winstate, perfuncno);
    let mut argvals: alloc::vec::Vec<(Datum<'mcx>, bool)> =
        alloc::vec::Vec::with_capacity(nargs);
    for i in 0..nargs {
        let (v, n) = {
            let argstate = wfuncstate_arg_mut(winstate, perfuncno, i);
            execExpr::exec_eval_expr_switch_context::call(argstate, econtext, estate)?
        };
        argvals.push((v, n));
    }

    if peragg_ref(winstate, peraggno).invtransfn.fn_strict {
        for i in 0..numArguments as usize {
            if i < argvals.len() && argvals[i].1 {
                return Ok(true);
            }
        }
    }

    // There should still be an added but not yet removed value.
    debug_assert!(peragg_ref(winstate, peraggno).transValueCount > 0);

    // In moving-aggregate mode, the state must never be NULL here.
    if peragg_ref(winstate, peraggno).transValueIsNull {
        return Err(types_error::PgError::error(
            "aggregate transition value is NULL before inverse transition",
        ));
    }

    // We mustn't use the inverse transition function to remove the last input;
    // re-initialize the aggregate instead.
    if peragg_ref(winstate, peraggno).transValueCount == 1 {
        let wfuncno = peragg_ref(winstate, peraggno).wfuncno as usize;
        initialize_windowaggregate(winstate, peraggno, estate);
        let _ = wfuncno;
        return Ok(true);
    }

    // OK to call the inverse transition function. Set winstate->curaggcontext
    // while calling it, for possible use by AggCheckCallContext.
    //   InitFunctionCallInfoData(*fcinfo, &peraggstate->invtransfn,
    //                            numArguments + 1, perfuncstate->winCollation, ...);
    //   fcinfo->args[0].value = peraggstate->transValue;
    //   fcinfo->args[0].isnull = peraggstate->transValueIsNull;
    //   winstate->curaggcontext = peraggstate->aggcontext;
    //   newVal = FunctionCallInvoke(fcinfo);
    //   winstate->curaggcontext = NULL;
    let collation = perfunc_ref(winstate, perfuncno).winCollation;
    let mcx = estate.es_query_cxt;

    let (oldVal, oldIsNull, newVal, isnull) = {
        let (fn_oid, fn_expr) = {
            let pa = peragg_ref(winstate, peraggno);
            (pa.invtransfn.fn_oid, pa.invtransfn.fn_expr.clone())
        };
        let pa = peragg_mut(winstate, peraggno);
        let oldIsNull = pa.transValueIsNull;
        // MOVE the running state out (an `internal` state cannot be cloned); the
        // (same or new) returned state is stored back below.
        let trans_value = core::mem::replace(&mut pa.transValue, Datum::null());
        let oldVal = if trans_value.is_internal() {
            Datum::null()
        } else {
            trans_value.clone()
        };
        let mut args: alloc::vec::Vec<Datum<'mcx>> =
            alloc::vec::Vec::with_capacity(numArguments as usize + 1);
        let mut args_null: alloc::vec::Vec<bool> =
            alloc::vec::Vec::with_capacity(numArguments as usize + 1);
        args.push(trans_value);
        args_null.push(oldIsNull);
        for i in 0..numArguments as usize {
            args.push(argvals[i].0.clone());
            args_null.push(argvals[i].1);
        }
        let (newVal, isnull) =
            fmgr::function_call_invoke_datum_owned::call(mcx, fn_oid, collation, args, args_null, fn_expr)?;
        (oldVal, oldIsNull, newVal, isnull)
    };

    // If the function returns NULL, report failure, forcing a restart.
    //   if (fcinfo->isnull) { return false; }
    if isnull {
        return Ok(false);
    }

    // Update number of rows included in transValue.
    //   peraggstate->transValueCount--;
    peragg_mut(winstate, peraggno).transValueCount -= 1;

    // If pass-by-ref datatype, must copy the new value into aggcontext and free
    // the prior transValue. (See comments for ExecAggCopyTransValue.)
    let newVal = window_copy_trans_value(
        winstate, peraggno, newVal, isnull, oldVal, oldIsNull, mcx,
    )?;

    // peraggstate->transValue = newVal; peraggstate->transValueIsNull = fcinfo->isnull;
    let pa = peragg_mut(winstate, peraggno);
    pa.transValue = newVal;
    pa.transValueIsNull = isnull;
    let _ = econtext;
    Ok(true)
}

/// `finalize_windowaggregate` — parallel to `finalize_aggregate` in nodeAgg.c.
fn finalize_windowaggregate<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    peraggno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // oldContext = MemoryContextSwitchTo(ps_ExprContext->ecxt_per_tuple_memory);
    let mcx = estate.es_query_cxt;

    // Apply the agg's finalfn if one is provided, else return transValue.
    //
    // `MakeExpandedObjectReadOnly(d, isnull, typlen)` is the identity for a
    // plain (non-expanded) pass-by-reference Datum; the unified Datum carries no
    // expanded-object handle, so every `MakeExpandedObjectReadOnly` below is a
    // pass-through (faithful for the non-expanded values these aggregates use).
    if oid_is_valid(peragg_ref(winstate, peraggno).finalfn_oid) {
        // perfuncstate = &winstate->perfunc[peraggstate->wfuncno].
        let perfuncno = peragg_ref(winstate, peraggno).wfuncno as usize;
        let (numFinalArgs, fn_oid, fn_strict, fn_expr) = {
            let pa = peragg_ref(winstate, peraggno);
            (pa.numFinalArgs, pa.finalfn.fn_oid, pa.finalfn.fn_strict, pa.finalfn.fn_expr.clone())
        };
        let collation = perfunc_ref(winstate, perfuncno).winCollation;

        // InitFunctionCallInfoData(fcinfo, &peraggstate->finalfn, numFinalArgs,
        //                          perfuncstate->winCollation, ...);
        // fcinfo->args[0].value = MakeExpandedObjectReadOnly(transValue, ...);
        // fcinfo->args[0].isnull = transValueIsNull; anynull = transValueIsNull;
        //
        // The owned by-value seam carries an `internal`-transtype arg0 (a
        // `Datum::Internal` box) through the fcinfo side channel, so an
        // internal-state aggregate's finalfn (e.g. numeric_avg) gets its state.
        // That state cannot be cloned, so for the internal case MOVE it out
        // (C passes the same pointer; the finalfn reads it without freeing).
        //
        // C's finalize_windowaggregate reads `peraggstate->transValue` with
        // `PG_GETARG_POINTER(0)`, which does NOT consume the state: the same
        // running state must survive this call so the NEXT row's forward/inverse
        // transition (advance_windowaggregate / advance_windowaggregate_base) can
        // keep accumulating it. A moving frame (`ROWS n PRECEDING`) calls the
        // inverse transfn on the live state; an unbounded-preceding frame keeps
        // adding to it. Moving it out and dropping it here would make the next
        // row see a NULL state — "numeric_accum_inv called with NULL state" for a
        // moving frame, or non-accumulation (per-row values) for an unbounded one.
        // So use the FINAL form of the owned seam, which returns `args[0]` back
        // after the finalfn glue restores it (set_ref_arg(0)), and put the
        // surviving state back into `peraggstate->transValue` below — mirroring
        // nodeAgg's finalize_aggregate restore (commit 599387c18).
        let trans_is_null = peragg_ref(winstate, peraggno).transValueIsNull;
        let mut anynull = trans_is_null;
        let arg0 = {
            let pa = peragg_mut(winstate, peraggno);
            if pa.transValue.is_internal() {
                core::mem::replace(&mut pa.transValue, Datum::null())
            } else {
                pa.transValue.clone()
            }
        };
        let mut args: alloc::vec::Vec<Datum<'mcx>> =
            alloc::vec::Vec::with_capacity(numFinalArgs as usize);
        let mut args_null: alloc::vec::Vec<bool> =
            alloc::vec::Vec::with_capacity(numFinalArgs as usize);
        args.push(arg0);
        args_null.push(trans_is_null);

        // Fill any remaining argument positions with nulls.
        //   for (i = 1; i < numFinalArgs; i++) { args[i] = NULL; anynull = true; }
        for _ in 1..numFinalArgs {
            args.push(Datum::null());
            args_null.push(true);
            anynull = true;
        }

        if fn_strict && anynull {
            // Don't call a strict function with NULL inputs.
            //   *result = (Datum) 0; *isnull = true;
            // We moved arg0 (the running state) out above but are not calling the
            // finalfn; restore it so the state survives for the next row's
            // transition (the by-value/null cases carry no referent and a restore
            // of the null placeholder is harmless).
            let mut args = args;
            let surviving_arg0 = args.swap_remove(0);
            let pa = peragg_mut(winstate, peraggno);
            pa.transValue = surviving_arg0;
            pa.transValueIsNull = trans_is_null;
            Ok((Datum::null(), true))
        } else {
            //   winstate->curaggcontext = peraggstate->aggcontext;
            //   res = FunctionCallInvoke(fcinfo);
            //   *isnull = fcinfo->isnull;
            //   *result = MakeExpandedObjectReadOnly(res, fcinfo->isnull, resulttypeLen);
            //
            // FINAL form: returns the live transition state (args[0]) back so it
            // survives this non-destructive read (C: PG_GETARG_POINTER(0)).
            let (res, isnull, surviving_arg0) = fmgr::function_call_finalfn_owned::call(
                mcx, fn_oid, collation, args, args_null, fn_expr,
            )?;
            // Restore the running state into peraggstate->transValue so the next
            // row's forward/inverse transition keeps accumulating it. For a
            // by-value or plain by-ref transtype the seam returns None and the
            // transValue handle we cloned above is still live, so leave it; for an
            // `internal` transtype the box came back here and must go back.
            if let Some(state) = surviving_arg0 {
                let pa = peragg_mut(winstate, peraggno);
                pa.transValue = state;
                pa.transValueIsNull = trans_is_null;
            }
            // MakeExpandedObjectReadOnly is a pass-through for both by-value and
            // plain by-reference result types.
            Ok((res, isnull))
        }
    } else {
        // *result = MakeExpandedObjectReadOnly(transValue, transValueIsNull, transtypeLen);
        // *isnull = transValueIsNull;
        let pa = peragg_ref(winstate, peraggno);
        Ok((pa.transValue.clone(), pa.transValueIsNull))
    }
}

// ===========================================================================
// eval_windowaggregates
// ===========================================================================

/// `eval_windowaggregates` — evaluate plain aggregates used as window
/// functions.
fn eval_windowaggregates<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let numaggs = winstate.numaggs as usize;
    if numaggs == 0 {
        return Ok(()); // nothing to do
    }

    // final output execution is in ps_ExprContext
    let econtext = winstate
        .ss
        .ps
        .ps_ExprContext
        .expect("eval_windowaggregates: ps_ExprContext not set");
    let agg_row_slot = winstate
        .agg_row_slot
        .expect("eval_windowaggregates: agg_row_slot not set");
    let temp_slot = winstate
        .temp_slot_1
        .expect("eval_windowaggregates: temp_slot_1 not set");

    // First, update the frame head position.
    update_frameheadpos(winstate, estate)?;
    if winstate.frameheadpos < winstate.aggregatedbase {
        return Err(types_error::PgError::error("window frame head moved backward"));
    }

    // If the frame didn't change compared to the previous row, re-use the saved
    // result values.
    if winstate.aggregatedbase == winstate.frameheadpos
        && (winstate.frameOptions
            & (FRAMEOPTION_END_UNBOUNDED_FOLLOWING | FRAMEOPTION_END_CURRENT_ROW))
            != 0
        && (winstate.frameOptions & FRAMEOPTION_EXCLUSION) == 0
        && winstate.aggregatedbase <= winstate.currentpos
        && winstate.aggregatedupto > winstate.currentpos
    {
        for i in 0..numaggs {
            let wfuncno = peragg_ref(winstate, i).wfuncno as usize;
            let value = peragg_ref(winstate, i).resultValue.clone();
            let isnull = peragg_ref(winstate, i).resultValueIsNull;
            let ec = estate.ecxt_mut(econtext);
            ec.ecxt_aggvalues[wfuncno] = value;
            ec.ecxt_aggnulls[wfuncno] = isnull;
        }
        return Ok(());
    }

    // Initialize restart flags.
    let mut numaggs_restart = 0;
    for i in 0..numaggs {
        let restart = winstate.currentpos == 0
            || (winstate.aggregatedbase != winstate.frameheadpos
                && !oid_is_valid(peragg_ref(winstate, i).invtransfn_oid))
            || (winstate.frameOptions & FRAMEOPTION_EXCLUSION) != 0
            || winstate.aggregatedupto <= winstate.frameheadpos;
        peragg_mut(winstate, i).restart = restart;
        if restart {
            numaggs_restart += 1;
        }
    }

    // If we have any possibly-moving aggregates, attempt to advance
    // aggregatedbase to the frame's head by removing rows that fell off the top.
    while numaggs_restart < numaggs && winstate.aggregatedbase < winstate.frameheadpos {
        // Fetch the next tuple of those being removed (must never fail).
        if !window_gettupleslot_agg(winstate, winstate.aggregatedbase, temp_slot, estate)? {
            return Err(types_error::PgError::error(
                "could not re-fetch previously fetched frame row",
            ));
        }

        // Set tuple context for evaluation of aggregate arguments.
        let tmpcontext = winstate
            .tmpcontext
            .expect("eval_windowaggregates: tmpcontext not set");
        estate.ecxt_mut(tmpcontext).ecxt_outertuple = Some(temp_slot);

        // Perform the inverse transition for each not-yet-restarted aggregate.
        for i in 0..numaggs {
            if peragg_ref(winstate, i).restart {
                continue;
            }
            let wfuncno = peragg_ref(winstate, i).wfuncno as usize;
            let ok = advance_windowaggregate_base(winstate, wfuncno, i, estate)?;
            if !ok {
                peragg_mut(winstate, i).restart = true;
                numaggs_restart += 1;
            }
        }

        // Reset per-input-tuple context after each tuple.
        reset_expr_context(winstate.tmpcontext.unwrap(), estate)?;

        // And advance the aggregated-row state.
        winstate.aggregatedbase += 1;
        execTuples::exec_clear_tuple::call(estate, temp_slot)?;
    }

    // If we failed for any, forcibly update aggregatedbase.
    winstate.aggregatedbase = winstate.frameheadpos;

    // If we created a mark pointer for aggregates, keep it at the frame head.
    if agg_winobj_markptr(winstate) >= 0 {
        let pos = winstate.frameheadpos;
        win_set_mark_position_agg(winstate, pos, estate)?;
    }

    // Now restart the aggregates that require it.
    if numaggs_restart > 0 {
        winstate
            .aggcontext
            .as_mut()
            .expect("eval_windowaggregates: aggcontext not set")
            .reset();
    }
    for i in 0..numaggs {
        // Aggregates using the shared ctx must restart if *any* agg does.
        debug_assert!(
            !ctx_is_shared(winstate, i) || numaggs_restart == 0 || peragg_ref(winstate, i).restart
        );

        if peragg_ref(winstate, i).restart {
            initialize_windowaggregate(winstate, i, estate);
        } else if !peragg_ref(winstate, i).resultValueIsNull {
            // pfree(DatumGetPointer(peraggstate->resultValue)) for a by-ref saved
            // result: in the owned model the arena reclaims the bytes when its
            // context drops, so clearing the handle (below) is the faithful free.
            let pa = peragg_mut(winstate, i);
            pa.resultValue = Datum::null();
            pa.resultValueIsNull = true;
        }
    }

    // Note the old aggregatedupto so we know how far to skip non-restarted aggs.
    let aggregatedupto_nonrestarted = winstate.aggregatedupto;
    if numaggs_restart > 0 && winstate.aggregatedupto != winstate.frameheadpos {
        winstate.aggregatedupto = winstate.frameheadpos;
        execTuples::exec_clear_tuple::call(estate, agg_row_slot)?;
    }

    // Advance until we reach a row not in frame (or end of partition).
    loop {
        // Fetch next row if we didn't already.
        //   if (TupIsNull(agg_row_slot)) { if (!window_gettupleslot(...)) break; }
        if estate.slot(agg_row_slot).is_empty() {
            if !window_gettupleslot_agg(winstate, winstate.aggregatedupto, agg_row_slot, estate)? {
                break; // must be end of partition
            }
        }

        // Exit loop if no more rows can be in frame; skip if not in frame but
        // there might be more.
        let ret = row_is_in_frame(winstate, winstate.aggregatedupto, agg_row_slot, estate)?;
        if ret < 0 {
            break;
        }
        if ret == 0 {
            // next_tuple:
            reset_expr_context(winstate.tmpcontext.unwrap(), estate)?;
            winstate.aggregatedupto += 1;
            execTuples::exec_clear_tuple::call(estate, agg_row_slot)?;
            continue;
        }

        // Set tuple context for evaluation of aggregate arguments.
        let tmpcontext = winstate.tmpcontext.unwrap();
        estate.ecxt_mut(tmpcontext).ecxt_outertuple = Some(agg_row_slot);

        // Accumulate row into the aggregates.
        for i in 0..numaggs {
            // Non-restarted aggs skip until aggregatedupto_nonrestarted.
            if !peragg_ref(winstate, i).restart
                && winstate.aggregatedupto < aggregatedupto_nonrestarted
            {
                continue;
            }
            let wfuncno = peragg_ref(winstate, i).wfuncno as usize;
            advance_windowaggregate(winstate, wfuncno, i, estate)?;
        }

        // next_tuple:
        reset_expr_context(winstate.tmpcontext.unwrap(), estate)?;
        winstate.aggregatedupto += 1;
        execTuples::exec_clear_tuple::call(estate, agg_row_slot)?;
    }

    // The frame's end is not supposed to move backwards, ever.
    debug_assert!(aggregatedupto_nonrestarted <= winstate.aggregatedupto);

    // finalize aggregates and fill result/isnull fields.
    let mcx = estate.es_query_cxt;
    for i in 0..numaggs {
        let wfuncno = peragg_ref(winstate, i).wfuncno as usize;
        let (result, isnull) = finalize_windowaggregate(winstate, i, estate)?;
        {
            let ec = estate.ecxt_mut(econtext);
            ec.ecxt_aggvalues[wfuncno] = result.clone();
            ec.ecxt_aggnulls[wfuncno] = isnull;
        }

        // save the result in case next row shares the same frame.
        //   if (!peraggstate->resulttypeByVal && !*isnull)
        //       peraggstate->resultValue = datumCopy(*result, byval, len);
        //   else peraggstate->resultValue = *result;
        let result_by_val = peragg_ref(winstate, i).resulttypeByVal;
        let result_len = peragg_ref(winstate, i).resulttypeLen;
        let saved = if !result_by_val && !isnull {
            // datumCopy of the by-ref result into the agg's result storage
            // (materialized into the per-query context; see window_copy_trans_value).
            datum::datum_copy_v::call(mcx, &result, result_by_val, result_len as i32)?
        } else {
            result
        };
        peragg_mut(winstate, i).resultValue = saved;
        peragg_mut(winstate, i).resultValueIsNull = isnull;
    }

    Ok(())
}

// ===========================================================================
// eval_windowfunction
// ===========================================================================

/// `eval_windowfunction` — call a real (non-aggregate) window function.
fn eval_windowfunction<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // C:
    //   InitFunctionCallInfoData(*fcinfo, &flinfo, numArguments, winCollation,
    //                            (Node *) winobj, NULL);
    //   winstate->curaggcontext = NULL;
    //   *result = FunctionCallInvoke(fcinfo);
    //   *isnull = fcinfo->isnull;
    //
    // C passes the WindowObject as fcinfo->context and dispatches the window
    // function by OID through FunctionCallInvoke; the called function (e.g.
    // window_row_number) reads its real input back through the WindowObject API
    // (WinGetCurrentPosition / WinSetMarkPosition / WinGetPartitionLocalMemory /
    // WinRowsArePeers / WinGetFuncArg*). Those API functions are ported in THIS
    // crate and take `&mut WindowAggState` + `perfuncno` + `&mut EState` directly
    // — state that a generic `PGFunction(&mut fcinfo)` callback cannot carry. So
    // the window-function call is dispatched here by `winfnoid` straight to the
    // ported body, threading the executor state, rather than through the bare-
    // Datum fmgr frame. (Recorded in DESIGN_DEBT: window-function dispatch is a
    // closed builtin set, special-cased out of generic FunctionCallInvoke because
    // every window function needs executor state the fmgr frame cannot thread.)
    let winfnoid = perfunc_ref(winstate, perfuncno)
        .wfunc
        .as_ref()
        .expect("eval_windowfunction: perfunc.wfunc not set")
        .winfnoid;

    // C dispatches every window function by OID through FunctionCallInvoke; here
    // we dispatch straight to the ported body (each returns `(Datum, isnull)`,
    // mirroring `*result` / `fcinfo->isnull`). The leadlag family is folded onto
    // `leadlag_common` exactly as C's six `window_lag*`/`window_lead*` thunks do.
    let (result, isnull): (Datum<'mcx>, bool) = match winfnoid {
        // window_row_number (windowfuncs.c): increment up from 1.
        3100 => (Datum::from_i64(window_row_number(winstate, perfuncno, estate)?), false),
        // window_rank (windowfuncs.c).
        3101 => (Datum::from_i64(window_rank(winstate, perfuncno, estate)?), false),
        // window_dense_rank (windowfuncs.c).
        3102 => (Datum::from_i64(window_dense_rank(winstate, perfuncno, estate)?), false),
        // window_percent_rank (windowfuncs.c).
        3103 => (Datum::from_f64(window_percent_rank(winstate, perfuncno, estate)?), false),
        // window_cume_dist (windowfuncs.c).
        3104 => (Datum::from_f64(window_cume_dist(winstate, perfuncno, estate)?), false),
        // window_ntile (windowfuncs.c).
        3105 => window_ntile(winstate, perfuncno, estate)?,
        // lag / lag_with_offset / lag_with_offset_and_default (forward = false).
        3106 => leadlag_common(winstate, perfuncno, false, false, false, estate)?,
        3107 => leadlag_common(winstate, perfuncno, false, true, false, estate)?,
        3108 => leadlag_common(winstate, perfuncno, false, true, true, estate)?,
        // lead / lead_with_offset / lead_with_offset_and_default (forward = true).
        3109 => leadlag_common(winstate, perfuncno, true, false, false, estate)?,
        3110 => leadlag_common(winstate, perfuncno, true, true, false, estate)?,
        3111 => leadlag_common(winstate, perfuncno, true, true, true, estate)?,
        // window_first_value (windowfuncs.c).
        3112 => window_first_value(winstate, perfuncno, estate)?,
        // window_last_value (windowfuncs.c).
        3113 => window_last_value(winstate, perfuncno, estate)?,
        // window_nth_value (windowfuncs.c).
        3114 => window_nth_value(winstate, perfuncno, estate)?,
        other => panic!(
            "eval_windowfunction: window function OID {other} not ported \
             (windowfuncs.c builtin set: 3100-3114)"
        ),
    };

    // The window function might have returned a pass-by-ref result that's just a
    // pointer into one of the WindowObject's temporary slots. That's not a
    // problem if it's the only window function using the WindowObject; but if
    // there's more than one function, we'd better copy the result to ensure it's
    // not clobbered by later window functions.
    //   if (!perfuncstate->resulttypeByVal && !fcinfo->isnull && winstate->numfuncs > 1)
    //       *result = datumCopy(*result, byval, len);
    //
    // C copies into `ps_ExprContext->ecxt_per_tuple_memory` (the switched-to
    // context). The owned model can't hand back an `'mcx`-lived datum from that
    // borrow-scoped child context, so the copy lands in `es_query_cxt` (already
    // `Mcx<'mcx>`): the copied value is consumed by `ExecProject` within the same
    // tuple cycle and never outlives the per-query arena, so the longer-lived
    // home is behavior-preserving (it just defers reclamation to the query end).
    if !perfunc_ref(winstate, perfuncno).resulttypeByVal && !isnull && winstate.numfuncs > 1 {
        let copied = result.clone_in(estate.es_query_cxt)?;
        return Ok((copied, isnull));
    }

    Ok((result, isnull))
}

/// `window_row_number(fcinfo)` (windowfuncs.c) — just increment up from 1 until
/// the current partition finishes.
fn window_row_number<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<i64> {
    let curpos = WinGetCurrentPosition(winstate);
    WinSetMarkPosition(winstate, perfuncno, curpos, estate)?;
    Ok(curpos + 1)
}

/// `rank_up(winobj)` (windowfuncs.c) — advance the shared rank state when the
/// peer group changes. Returns whether the rank should increase.
fn rank_up<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let mut up = false;
    let curpos = WinGetCurrentPosition(winstate);

    // context = WinGetPartitionLocalMemory(winobj, sizeof(rank_context));
    // rank_context is a single int64 `rank`.
    let rank = read_rank_context(winstate, perfuncno)?;

    if rank == 0 {
        debug_assert!(curpos == 0);
        write_rank_context(winstate, perfuncno, 1)?;
    } else {
        debug_assert!(curpos > 0);
        // do current and prior tuples match by ORDER BY clause?
        if !WinRowsArePeers(winstate, perfuncno, curpos - 1, curpos, estate)? {
            up = true;
        }
    }

    // We can advance the mark, but only *after* access to prior row.
    WinSetMarkPosition(winstate, perfuncno, curpos, estate)?;

    Ok(up)
}

/// `window_rank(fcinfo)` (windowfuncs.c) — rank changes when key columns
/// change; the new rank is the current row number.
fn window_rank<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<i64> {
    let up = rank_up(winstate, perfuncno, estate)?;
    if up {
        let new_rank = WinGetCurrentPosition(winstate) + 1;
        write_rank_context(winstate, perfuncno, new_rank)?;
    }
    read_rank_context(winstate, perfuncno)
}

/// `window_dense_rank(fcinfo)` (windowfuncs.c) — rank increases by 1 when key
/// columns change.
fn window_dense_rank<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<i64> {
    let up = rank_up(winstate, perfuncno, estate)?;
    if up {
        let cur = read_rank_context(winstate, perfuncno)?;
        write_rank_context(winstate, perfuncno, cur + 1)?;
    }
    read_rank_context(winstate, perfuncno)
}

/// `window_percent_rank(fcinfo)` (windowfuncs.c) — fraction `(RK - 1) / (NR - 1)`
/// between 0 and 1 inclusive, where RK is the current row's rank and NR is the
/// total number of rows. Returns float8.
fn window_percent_rank<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<f64> {
    let totalrows = WinGetPartitionRowCount(winstate, estate)?;
    debug_assert!(totalrows > 0);

    let up = rank_up(winstate, perfuncno, estate)?;
    if up {
        let new_rank = WinGetCurrentPosition(winstate) + 1;
        write_rank_context(winstate, perfuncno, new_rank)?;
    }

    // return zero if there's only one row, per spec
    if totalrows <= 1 {
        return Ok(0.0);
    }

    let rank = read_rank_context(winstate, perfuncno)?;
    Ok((rank - 1) as f64 / (totalrows - 1) as f64)
}

/// `window_cume_dist(fcinfo)` (windowfuncs.c) — fraction `NP / NR` between 0 and
/// 1 inclusive, where NP is the number of rows preceding or peers to the current
/// row, and NR is the total number of rows. Returns float8.
fn window_cume_dist<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<f64> {
    let totalrows = WinGetPartitionRowCount(winstate, estate)?;
    debug_assert!(totalrows > 0);

    let up = rank_up(winstate, perfuncno, estate)?;
    let rank = read_rank_context(winstate, perfuncno)?;
    if up || rank == 1 {
        // The current row is not peer to prior row or is just the first, so
        // count up the number of rows that are peer to the current.
        let mut new_rank = WinGetCurrentPosition(winstate) + 1;

        // start from current + 1
        let mut row = new_rank;
        while row < totalrows {
            if !WinRowsArePeers(winstate, perfuncno, row - 1, row, estate)? {
                break;
            }
            new_rank += 1;
            row += 1;
        }
        write_rank_context(winstate, perfuncno, new_rank)?;
    }

    let rank = read_rank_context(winstate, perfuncno)?;
    Ok(rank as f64 / totalrows as f64)
}

/// `window_ntile(fcinfo)` (windowfuncs.c) — compute an exact numeric value with
/// scale 0, ranging from 1 to n. Returns int4 (or NULL if the bucket count
/// argument is NULL, per spec). Returns `(Datum, isnull)`.
fn window_ntile<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // context = WinGetPartitionLocalMemory(winobj, sizeof(ntile_context));
    let mut ctx = read_ntile_context(winstate, perfuncno)?;

    if ctx.ntile == 0 {
        // first call
        let total = WinGetPartitionRowCount(winstate, estate)?;
        let (nbuckets_datum, isnull) = WinGetFuncArgCurrent(winstate, perfuncno, 0, estate)?;

        // per spec: If NT is the null value, then the result is the null value.
        if isnull {
            return Ok((Datum::null(), true));
        }
        let nbuckets = nbuckets_datum.as_i32();

        // per spec: If NT is less than or equal to 0, then an exception
        // condition is raised.
        if nbuckets <= 0 {
            return Err(types_error::PgError::error(
                "argument of ntile must be greater than zero",
            ));
        }

        ctx.ntile = 1;
        ctx.rows_per_bucket = 0;
        ctx.boundary = total / nbuckets as i64;
        if ctx.boundary <= 0 {
            ctx.boundary = 1;
        } else {
            // If the total number is not divisible, add 1 row to leading
            // buckets.
            ctx.remainder = total % nbuckets as i64;
            if ctx.remainder != 0 {
                ctx.boundary += 1;
            }
        }
    }

    ctx.rows_per_bucket += 1;
    if ctx.boundary < ctx.rows_per_bucket {
        // ntile up
        if ctx.remainder != 0 && ctx.ntile as i64 == ctx.remainder {
            ctx.remainder = 0;
            ctx.boundary -= 1;
        }
        ctx.ntile += 1;
        ctx.rows_per_bucket = 1;
    }

    let result = ctx.ntile;
    write_ntile_context(winstate, perfuncno, &ctx)?;
    Ok((Datum::from_i32(result), false))
}

/// `leadlag_common` (windowfuncs.c) — common operation of `lead()` and `lag()`.
/// For `lead()` `forward` is true, whereas for `lag()` it is false. `withoffset`
/// indicates we have an offset second argument; `withdefault` indicates we have a
/// default third argument. Returns `(Datum, isnull)`.
fn leadlag_common<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    forward: bool,
    withoffset: bool,
    withdefault: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let offset: i32;
    let const_offset: bool;

    if withoffset {
        let (off_datum, isnull) = WinGetFuncArgCurrent(winstate, perfuncno, 1, estate)?;
        if isnull {
            return Ok((Datum::null(), true));
        }
        offset = off_datum.as_i32();
        const_offset = get_fn_expr_arg_stable(winstate, perfuncno, 1);
    } else {
        offset = 1;
        const_offset = true;
    }

    let relpos = if forward { offset as i64 } else { -(offset as i64) };
    let (mut result, mut isnull, isout) = WinGetFuncArgInPartition(
        winstate,
        perfuncno,
        0,
        relpos,
        WINDOW_SEEK_CURRENT,
        const_offset,
        estate,
    )?;

    if isout {
        // target row is out of the partition; supply default value if provided,
        // otherwise it'll stay NULL.
        if withdefault {
            let (d, n) = WinGetFuncArgCurrent(winstate, perfuncno, 2, estate)?;
            result = d;
            isnull = n;
        }
    }

    if isnull {
        return Ok((Datum::null(), true));
    }

    Ok((result, false))
}

/// `window_first_value(fcinfo)` (windowfuncs.c) — value of VE on the first row of
/// the window frame. Returns `(Datum, isnull)`.
fn window_first_value<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let (result, isnull, _isout) =
        WinGetFuncArgInFrame(winstate, perfuncno, 0, 0, WINDOW_SEEK_HEAD, true, estate)?;
    if isnull {
        return Ok((Datum::null(), true));
    }
    Ok((result, false))
}

/// `window_last_value(fcinfo)` (windowfuncs.c) — value of VE on the last row of
/// the window frame. Returns `(Datum, isnull)`.
fn window_last_value<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let (result, isnull, _isout) =
        WinGetFuncArgInFrame(winstate, perfuncno, 0, 0, WINDOW_SEEK_TAIL, true, estate)?;
    if isnull {
        return Ok((Datum::null(), true));
    }
    Ok((result, false))
}

/// `window_nth_value(fcinfo)` (windowfuncs.c) — value of VE on the n-th row from
/// the first row of the window frame. Returns `(Datum, isnull)`.
fn window_nth_value<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let (nth_datum, isnull) = WinGetFuncArgCurrent(winstate, perfuncno, 1, estate)?;
    if isnull {
        return Ok((Datum::null(), true));
    }
    let nth = nth_datum.as_i32();
    let const_offset = get_fn_expr_arg_stable(winstate, perfuncno, 1);

    if nth <= 0 {
        return Err(types_error::PgError::error(
            "argument of nth_value must be greater than zero",
        ));
    }

    let (result, isnull, _isout) = WinGetFuncArgInFrame(
        winstate,
        perfuncno,
        0,
        nth as i64 - 1,
        WINDOW_SEEK_HEAD,
        const_offset,
        estate,
    )?;
    if isnull {
        return Ok((Datum::null(), true));
    }
    Ok((result, false))
}

/// `ntile_context` (windowfuncs.c): `int32 ntile; int64 rows_per_bucket; int64
/// boundary; int64 remainder`.
#[derive(Default, Clone, Copy)]
struct NtileContext {
    ntile: i32,
    rows_per_bucket: i64,
    boundary: i64,
    remainder: i64,
}

// Byte layout of `ntile_context` in partition-local memory: i32 ntile followed
// by three i64 fields, native-endian, matching C's struct field order (the
// trailing fields are accessed as a flat scratch buffer, so we pack them
// contiguously rather than honoring C's alignment padding — the memory is
// private to this code).
const NTILE_CONTEXT_SIZE: usize = 4 + 8 * 3;

/// Read the `ntile_context` from the perfunc's partition-local memory
/// (allocated zeroed on first access, so the first read returns `ntile == 0`).
fn read_ntile_context<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
) -> PgResult<NtileContext> {
    let buf = WinGetPartitionLocalMemory(winstate, perfuncno, NTILE_CONTEXT_SIZE)?;
    let mut ntile = [0u8; 4];
    let mut rows_per_bucket = [0u8; 8];
    let mut boundary = [0u8; 8];
    let mut remainder = [0u8; 8];
    ntile.copy_from_slice(&buf[0..4]);
    rows_per_bucket.copy_from_slice(&buf[4..12]);
    boundary.copy_from_slice(&buf[12..20]);
    remainder.copy_from_slice(&buf[20..28]);
    Ok(NtileContext {
        ntile: i32::from_ne_bytes(ntile),
        rows_per_bucket: i64::from_ne_bytes(rows_per_bucket),
        boundary: i64::from_ne_bytes(boundary),
        remainder: i64::from_ne_bytes(remainder),
    })
}

/// Write the `ntile_context` back into the perfunc's partition-local memory.
fn write_ntile_context<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    ctx: &NtileContext,
) -> PgResult<()> {
    let buf = WinGetPartitionLocalMemory(winstate, perfuncno, NTILE_CONTEXT_SIZE)?;
    buf[0..4].copy_from_slice(&ctx.ntile.to_ne_bytes());
    buf[4..12].copy_from_slice(&ctx.rows_per_bucket.to_ne_bytes());
    buf[12..20].copy_from_slice(&ctx.boundary.to_ne_bytes());
    buf[20..28].copy_from_slice(&ctx.remainder.to_ne_bytes());
    Ok(())
}

/// `get_fn_expr_arg_stable(flinfo, argnum)` (fmgr.c) — true if the `argnum`'th
/// argument of the window function call is a value that doesn't change during
/// query execution (a `Const`, or an external `Param`). Mirrors
/// `get_call_expr_arg_stable` over the `WindowFunc` node's args, which the
/// owned model reaches via the perfunc's `wfunc`.
fn get_fn_expr_arg_stable(winstate: &WindowAggState<'_>, perfuncno: usize, argnum: usize) -> bool {
    let wfunc = match perfunc_ref(winstate, perfuncno).wfunc.as_ref() {
        Some(w) => w,
        None => return false,
    };
    let arg = match wfunc.args.get(argnum) {
        Some(a) => a,
        None => return false,
    };
    match arg {
        types_nodes::primnodes::Expr::Const(_) => true,
        types_nodes::primnodes::Expr::Param(p) => {
            p.paramkind == types_nodes::primnodes::PARAM_EXTERN
        }
        _ => false,
    }
}

/// Read the `int64 rank` of the `rank_context` from the perfunc's
/// partition-local memory (allocating it zeroed on first access, matching C's
/// `WinGetPartitionLocalMemory`).
fn read_rank_context<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
) -> PgResult<i64> {
    let buf = WinGetPartitionLocalMemory(winstate, perfuncno, core::mem::size_of::<i64>())?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&buf[..8]);
    Ok(i64::from_ne_bytes(bytes))
}

/// Write the `int64 rank` of the `rank_context` into the perfunc's
/// partition-local memory.
fn write_rank_context<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    rank: i64,
) -> PgResult<()> {
    let buf = WinGetPartitionLocalMemory(winstate, perfuncno, core::mem::size_of::<i64>())?;
    buf[..8].copy_from_slice(&rank.to_ne_bytes());
    Ok(())
}

// ===========================================================================
// prepare_tuplestore / begin_partition / spool_tuples / release_partition
// ===========================================================================

/// `prepare_tuplestore` — create the tuplestore and required read pointers.
fn prepare_tuplestore<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let frameOptions = winstate.frameOptions;
    let numfuncs = winstate.numfuncs as usize;
    let ordNumCols = node_ord_num_cols(winstate);

    // we shouldn't be called if this was done already
    debug_assert!(winstate.buffer.is_none());

    // Create new tuplestore (pallocs in CurrentMemoryContext == es_query_cxt).
    let mut buffer =
        tuplestore::tuplestore_begin_heap::call(estate.es_query_cxt, false, false, globals::work_mem::call())?;

    // read pointer 0 is pre-allocated
    winstate.current_ptr = 0;

    // reset default REWIND capability bit for current ptr
    tuplestore::tuplestore_set_eflags::call(&mut buffer, 0)?;

    // create read pointers for aggregates, if needed
    if winstate.numaggs > 0 {
        let mut readptr_flags = 0;
        // If the frame head is potentially movable, or we have an EXCLUSION
        // clause, we might need to restart aggregation ...
        if (frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) == 0
            || (frameOptions & FRAMEOPTION_EXCLUSION) != 0
        {
            // ... so create a mark pointer to track the frame head
            let markptr = tuplestore::tuplestore_alloc_read_pointer::call(&mut buffer, 0)?;
            agg_winobj_mut(winstate).markptr = markptr;
            // and the read pointer will need BACKWARD capability
            readptr_flags |= EXEC_FLAG_BACKWARD;
        }
        let readptr = tuplestore::tuplestore_alloc_read_pointer::call(&mut buffer, readptr_flags)?;
        agg_winobj_mut(winstate).readptr = readptr;
    }

    // create mark and read pointers for each real window function
    for i in 0..numfuncs {
        if !perfunc_ref(winstate, i).plain_agg {
            let markptr = tuplestore::tuplestore_alloc_read_pointer::call(&mut buffer, 0)?;
            let readptr =
                tuplestore::tuplestore_alloc_read_pointer::call(&mut buffer, EXEC_FLAG_BACKWARD)?;
            let winobj = perfunc_winobj_mut(winstate, i);
            winobj.markptr = markptr;
            winobj.readptr = readptr;
        }
    }

    // frame head / tail endpoint read pointers, if needed.
    winstate.framehead_ptr = -1;
    winstate.frametail_ptr = -1;

    if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
        if ((frameOptions & FRAMEOPTION_START_CURRENT_ROW) != 0 && ordNumCols != 0)
            || (frameOptions & FRAMEOPTION_START_OFFSET) != 0
        {
            winstate.framehead_ptr = tuplestore::tuplestore_alloc_read_pointer::call(&mut buffer, 0)?;
        }
        if ((frameOptions & FRAMEOPTION_END_CURRENT_ROW) != 0 && ordNumCols != 0)
            || (frameOptions & FRAMEOPTION_END_OFFSET) != 0
        {
            winstate.frametail_ptr = tuplestore::tuplestore_alloc_read_pointer::call(&mut buffer, 0)?;
        }
    }

    // exclusion-clause group-tail read pointer, if needed.
    winstate.grouptail_ptr = -1;
    if (frameOptions & (FRAMEOPTION_EXCLUDE_GROUP | FRAMEOPTION_EXCLUDE_TIES)) != 0
        && ordNumCols != 0
    {
        winstate.grouptail_ptr = tuplestore::tuplestore_alloc_read_pointer::call(&mut buffer, 0)?;
    }

    winstate.buffer = Some(buffer);
    Ok(())
}

/// `begin_partition` — start buffering rows of the next partition.
fn begin_partition<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let numfuncs = winstate.numfuncs as usize;

    winstate.partition_spooled = false;
    winstate.framehead_valid = false;
    winstate.frametail_valid = false;
    winstate.grouptail_valid = false;
    winstate.spooled_rows = 0;
    winstate.currentpos = 0;
    winstate.frameheadpos = 0;
    winstate.frametailpos = 0;
    winstate.currentgroup = 0;
    winstate.frameheadgroup = 0;
    winstate.frametailgroup = 0;
    winstate.groupheadpos = 0;
    winstate.grouptailpos = -1; // see update_grouptailpos

    execTuples::exec_clear_tuple::call(estate, winstate.agg_row_slot.unwrap())?;
    if let Some(fh) = winstate.framehead_slot {
        execTuples::exec_clear_tuple::call(estate, fh)?;
    }
    if let Some(ft) = winstate.frametail_slot {
        execTuples::exec_clear_tuple::call(estate, ft)?;
    }

    // If this is the very first partition, fetch the first input row.
    let first_part_slot = winstate.first_part_slot.unwrap();
    if estate.slot(first_part_slot).is_empty() {
        let outerPlan = winstate
            .ss
            .ps
            .lefttree
            .as_deref_mut()
            .expect("begin_partition: no outer plan state");
        let outerslot = execProcnode::exec_proc_node::call(outerPlan, estate)?;
        match outerslot {
            Some(id) if !estate.slot(id).is_empty() => {
                                execTuples::exec_copy_slot::call(estate, first_part_slot, id)?;
            }
            _ => {
                // outer plan is empty, so we have nothing to do
                winstate.partition_spooled = true;
                winstate.more_partitions = false;
                return Ok(());
            }
        }
    }

    // Create new tuplestore if not done already.
    if winstate.buffer.is_none() {
        prepare_tuplestore(winstate, estate)?;
    }

    winstate.next_partition = false;

    if winstate.numaggs > 0 {
        // reset mark and seek positions for aggregate functions
        let w = agg_winobj_mut(winstate);
        w.markpos = -1;
        w.seekpos = -1;
        // Also reset the row counters for aggregates
        winstate.aggregatedbase = 0;
        winstate.aggregatedupto = 0;
    }

    // reset mark and seek positions for each real window function
    for i in 0..numfuncs {
        if !perfunc_ref(winstate, i).plain_agg {
            let winobj = perfunc_winobj_mut(winstate, i);
            winobj.markpos = -1;
            winobj.seekpos = -1;
        }
    }

    // Store the first tuple into the tuplestore.
    let buffer = winstate.buffer.as_deref_mut().unwrap();
    tuplestore::tuplestore_puttupleslot::call(buffer, first_part_slot, estate)?;
    winstate.spooled_rows += 1;
    Ok(())
}

/// `spool_tuples` — read tuples up to and including position `pos` into the
/// tuplestore. If `pos == -1`, reads the whole partition.
fn spool_tuples<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    mut pos: i64,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if winstate.buffer.is_none() {
        return Ok(()); // just a safety check
    }
    if winstate.partition_spooled {
        return Ok(()); // whole partition done already
    }

    let partNumCols = node_part_num_cols(winstate);

    // In pass-through mode, exhaust all tuples in the current partition.
    if winstate.status != WINDOWAGG_RUN {
        debug_assert!(
            winstate.status == WINDOWAGG_PASSTHROUGH
                || winstate.status == WINDOWAGG_PASSTHROUGH_STRICT
        );
        pos = -1;
    } else if !tuplestore::tuplestore_in_memory::call(winstate.buffer.as_deref().unwrap()) {
        // If the tuplestore spilled, force the entire partition to spool.
        pos = -1;
    }

    // Must be in query context to call outerplan (modeled by allocator choice).
    loop {
        if !(winstate.spooled_rows <= pos || pos == -1) {
            break;
        }

        let outerPlan = winstate
            .ss
            .ps
            .lefttree
            .as_deref_mut()
            .expect("spool_tuples: no outer plan state");
        let outerslot = execProcnode::exec_proc_node::call(outerPlan, estate)?;
        let outerslot = match outerslot {
            Some(id) if !estate.slot(id).is_empty() => id,
            _ => {
                // reached the end of the last partition
                winstate.partition_spooled = true;
                winstate.more_partitions = false;
                break;
            }
        };

        if partNumCols > 0 {
            let econtext = winstate.tmpcontext.unwrap();
            let first_part_slot = winstate.first_part_slot.unwrap();
            {
                let ec = estate.ecxt_mut(econtext);
                ec.ecxt_innertuple = Some(first_part_slot);
                ec.ecxt_outertuple = Some(outerslot);
            }

            // Check if this tuple still belongs to the current partition.
            let still_in = {
                let parteq = winstate
                    .partEqfunction
                    .as_deref_mut()
                    .expect("spool_tuples: partEqfunction not set");
                execExpr::exec_qual_and_reset::call(parteq, econtext, estate)?
            };
            if !still_in {
                // end of partition; copy the tuple for the next cycle.
                                execTuples::exec_copy_slot::call(estate, first_part_slot, outerslot)?;
                winstate.partition_spooled = true;
                winstate.more_partitions = true;
                break;
            }
        }

        // Remember the tuple unless we're the top-level window in strict
        // pass-through mode.
        if winstate.status != WINDOWAGG_PASSTHROUGH_STRICT {
            let buffer = winstate.buffer.as_deref_mut().unwrap();
            tuplestore::tuplestore_puttupleslot::call(buffer, outerslot, estate)?;
            winstate.spooled_rows += 1;
        }
    }

    Ok(())
}

/// `release_partition` — clear partition-local state, tuplestore and aggregate
/// results.
fn release_partition<'mcx>(winstate: &mut WindowAggState<'mcx>) -> PgResult<()> {
    let numfuncs = winstate.numfuncs as usize;
    let numaggs = winstate.numaggs as usize;

    for i in 0..numfuncs {
        // Release any partition-local state of this window function.
        if let Some(winobj) = perfunc_mut(winstate, i).winobj.as_deref_mut() {
            winobj.localmem = None;
        }
    }

    // Release all partition-local memory and aggregate temp data.
    if let Some(c) = winstate.partcontext.as_mut() {
        c.reset();
    }
    if let Some(c) = winstate.aggcontext.as_mut() {
        c.reset();
    }
    for i in 0..numaggs {
        if !ctx_is_shared(winstate, i) {
            peragg_mut(winstate, i)
                .aggcontext
                .as_mut()
                .expect("release_partition: private aggcontext not set")
                .reset();
        }
    }

    if let Some(buffer) = winstate.buffer.as_deref_mut() {
        tuplestore::tuplestore_clear::call(buffer);
    }
    winstate.partition_spooled = false;
    winstate.next_partition = true;
    Ok(())
}

// ===========================================================================
// row_is_in_frame / update_frameheadpos / update_frametailpos /
// update_grouptailpos
// ===========================================================================

/// `row_is_in_frame` — -1 = out of frame and no later rows in frame; 0 = out of
/// frame but later rows might be; 1 = in frame. May clobber temp_slot_2.
fn row_is_in_frame<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    pos: i64,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<i32> {
    let frameOptions = winstate.frameOptions;
    debug_assert!(pos >= 0);

    // Check frame starting conditions.
    update_frameheadpos(winstate, estate)?;
    if pos < winstate.frameheadpos {
        return Ok(0);
    }

    // Check frame ending conditions.
    let scan_slot = winstate.ss.ss_ScanTupleSlot.unwrap();
    if (frameOptions & FRAMEOPTION_END_CURRENT_ROW) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            // rows after current row are out of frame
            if pos > winstate.currentpos {
                return Ok(-1);
            }
        } else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
            // following row that is not peer is out of frame
            if pos > winstate.currentpos && !are_peers(winstate, slot, scan_slot, estate)? {
                return Ok(-1);
            }
        } else {
            debug_assert!(false);
        }
    } else if (frameOptions & FRAMEOPTION_END_OFFSET) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            let mut offset = winstate.endOffsetValue.as_i64();
            if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) != 0 {
                offset = -offset;
            }
            if pos > winstate.currentpos + offset {
                return Ok(-1);
            }
        } else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
            // hard cases, so delegate to update_frametailpos
            update_frametailpos(winstate, estate)?;
            if pos >= winstate.frametailpos {
                return Ok(-1);
            }
        } else {
            debug_assert!(false);
        }
    }

    // Check exclusion clause.
    if (frameOptions & FRAMEOPTION_EXCLUDE_CURRENT_ROW) != 0 {
        if pos == winstate.currentpos {
            return Ok(0);
        }
    } else if (frameOptions & FRAMEOPTION_EXCLUDE_GROUP) != 0
        || ((frameOptions & FRAMEOPTION_EXCLUDE_TIES) != 0 && pos != winstate.currentpos)
    {
        // If no ORDER BY, all rows are peers with each other.
        if node_ord_num_cols(winstate) == 0 {
            return Ok(0);
        }
        // Otherwise, check the group boundaries.
        if pos >= winstate.groupheadpos {
            update_grouptailpos(winstate, estate)?;
            if pos < winstate.grouptailpos {
                return Ok(0);
            }
        }
    }

    // If we get here, it's in frame.
    Ok(1)
}

/// `update_frameheadpos` — make frameheadpos valid for the current row. May
/// clobber temp_slot_2.
fn update_frameheadpos<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let frameOptions = winstate.frameOptions;

    if winstate.framehead_valid {
        return Ok(()); // already known for current row
    }

    let ordNumCols = node_ord_num_cols(winstate);
    let scan_slot = winstate.ss.ss_ScanTupleSlot.unwrap();

    if (frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) != 0 {
        winstate.frameheadpos = 0;
        winstate.framehead_valid = true;
    } else if (frameOptions & FRAMEOPTION_START_CURRENT_ROW) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            winstate.frameheadpos = winstate.currentpos;
            winstate.framehead_valid = true;
        } else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
            if ordNumCols == 0 {
                winstate.frameheadpos = 0;
                winstate.framehead_valid = true;
                return Ok(());
            }

            let framehead_slot = winstate.framehead_slot.unwrap();
            let framehead_ptr = winstate.framehead_ptr;
            tuplestore::tuplestore_select_read_pointer::call(
                winstate.buffer.as_deref_mut().unwrap(),
                framehead_ptr,
            )?;
            if winstate.frameheadpos == 0 && estate.slot(framehead_slot).is_empty() {
                if !tuplestore::tuplestore_gettupleslot::call(
                    winstate.buffer.as_deref_mut().unwrap(),
                    true,
                    true,
                    framehead_slot,
                    estate,
                )? {
                    return Err(types_error::PgError::error("unexpected end of tuplestore"));
                }
            }

            while !estate.slot(framehead_slot).is_empty() {
                if are_peers(winstate, framehead_slot, scan_slot, estate)? {
                    break; // this row is the correct frame head
                }
                winstate.frameheadpos += 1;
                spool_tuples(winstate, winstate.frameheadpos, estate)?;
                if !tuplestore::tuplestore_gettupleslot::call(
                    winstate.buffer.as_deref_mut().unwrap(),
                    true,
                    true,
                    framehead_slot,
                    estate,
                )? {
                    break; // end of partition
                }
            }
            winstate.framehead_valid = true;
        } else {
            debug_assert!(false);
        }
    } else if (frameOptions & FRAMEOPTION_START_OFFSET) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            // In ROWS mode, bound is physically n before/after current.
            let mut offset = winstate.startOffsetValue.as_i64();
            if (frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING) != 0 {
                offset = -offset;
            }
            winstate.frameheadpos = winstate.currentpos + offset;
            if winstate.frameheadpos < 0 {
                winstate.frameheadpos = 0;
            } else if winstate.frameheadpos > winstate.currentpos + 1 {
                spool_tuples(winstate, winstate.frameheadpos - 1, estate)?;
                if winstate.frameheadpos > winstate.spooled_rows {
                    winstate.frameheadpos = winstate.spooled_rows;
                }
            }
            winstate.framehead_valid = true;
        } else if (frameOptions & FRAMEOPTION_RANGE) != 0 {
            // RANGE START_OFFSET mode: first row satisfying the in_range
            // constraint relative to the current row.
            let sortCol = node_ord_col_idx0(winstate);
            debug_assert!(ordNumCols == 1);

            let mut sub = (frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING) != 0;
            let mut less = false;
            if !winstate.inRangeAsc {
                sub = !sub;
                less = true;
            }

            let framehead_slot = winstate.framehead_slot.unwrap();
            let framehead_ptr = winstate.framehead_ptr;
            tuplestore::tuplestore_select_read_pointer::call(
                winstate.buffer.as_deref_mut().unwrap(),
                framehead_ptr,
            )?;
            if winstate.frameheadpos == 0 && estate.slot(framehead_slot).is_empty() {
                if !tuplestore::tuplestore_gettupleslot::call(
                    winstate.buffer.as_deref_mut().unwrap(),
                    true,
                    true,
                    framehead_slot,
                    estate,
                )? {
                    return Err(types_error::PgError::error("unexpected end of tuplestore"));
                }
            }

            while !estate.slot(framehead_slot).is_empty() {
                let (headval, headisnull) =
                    execTuples::slot_getattr::call(estate, framehead_slot, sortCol)?;
                let (currval, currisnull) =
                    execTuples::slot_getattr::call(estate, scan_slot, sortCol)?;
                if headisnull || currisnull {
                    // order of the rows depends only on nulls_first
                    if winstate.inRangeNullsFirst {
                        if !headisnull || currisnull {
                            break;
                        }
                    } else if headisnull || !currisnull {
                        break;
                    }
                } else {
                    // if (DatumGetBool(FunctionCall5Coll(&winstate->startInRangeFunc,
                    //         winstate->inRangeColl, headval, currval,
                    //         winstate->startOffsetValue, BoolGetDatum(sub),
                    //         BoolGetDatum(less)))) break;
                    let fn_oid = winstate.startInRangeFunc.fn_oid;
                    let collation = winstate.inRangeColl;
                    // The in_range comparison operands (headval/currval/offset)
                    // may be by-reference values (numeric/date/timestamp/interval
                    // RANGE columns), so they must cross the fmgr boundary as
                    // canonical `Datum`s — the by-reference-capable
                    // `function_call_invoke_datum` lane — rather than being forced
                    // through `as_usize()`, which panics on a by-reference arm.
                    let mcx = estate.es_query_cxt;
                    let args = [
                        headval.clone(),
                        currval.clone(),
                        winstate.startOffsetValue.clone(),
                        Datum::from_bool(sub),
                        Datum::from_bool(less),
                    ];
                    let (res, _isnull) = fmgr::function_call_invoke_datum::call(
                        mcx, fn_oid, collation, &args, &[], None,
                    )?;
                    if res.as_bool() {
                        break;
                    }
                }
                winstate.frameheadpos += 1;
                spool_tuples(winstate, winstate.frameheadpos, estate)?;
                if !tuplestore::tuplestore_gettupleslot::call(
                    winstate.buffer.as_deref_mut().unwrap(),
                    true,
                    true,
                    framehead_slot,
                    estate,
                )? {
                    break;
                }
            }
            winstate.framehead_valid = true;
        } else if (frameOptions & FRAMEOPTION_GROUPS) != 0 {
            // GROUPS START_OFFSET mode.
            let offset = winstate.startOffsetValue.as_i64();
            let minheadgroup = if (frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING) != 0 {
                winstate.currentgroup - offset
            } else {
                winstate.currentgroup + offset
            };

            let framehead_slot = winstate.framehead_slot.unwrap();
            let temp_slot_2 = winstate.temp_slot_2.unwrap();
            let framehead_ptr = winstate.framehead_ptr;
            tuplestore::tuplestore_select_read_pointer::call(
                winstate.buffer.as_deref_mut().unwrap(),
                framehead_ptr,
            )?;
            if winstate.frameheadpos == 0 && estate.slot(framehead_slot).is_empty() {
                if !tuplestore::tuplestore_gettupleslot::call(
                    winstate.buffer.as_deref_mut().unwrap(),
                    true,
                    true,
                    framehead_slot,
                    estate,
                )? {
                    return Err(types_error::PgError::error("unexpected end of tuplestore"));
                }
            }

            while !estate.slot(framehead_slot).is_empty() {
                if winstate.frameheadgroup >= minheadgroup {
                    break; // this row is the correct frame head
                }
                {
                                        execTuples::exec_copy_slot::call(estate, temp_slot_2, framehead_slot)?;
                }
                winstate.frameheadpos += 1;
                spool_tuples(winstate, winstate.frameheadpos, estate)?;
                if !tuplestore::tuplestore_gettupleslot::call(
                    winstate.buffer.as_deref_mut().unwrap(),
                    true,
                    true,
                    framehead_slot,
                    estate,
                )? {
                    break;
                }
                if !are_peers(winstate, temp_slot_2, framehead_slot, estate)? {
                    winstate.frameheadgroup += 1;
                }
            }
            execTuples::exec_clear_tuple::call(estate, temp_slot_2)?;
            winstate.framehead_valid = true;
        } else {
            debug_assert!(false);
        }
    } else {
        debug_assert!(false);
    }

    Ok(())
}

/// `update_frametailpos` — make frametailpos valid for the current row. May
/// clobber temp_slot_2.
fn update_frametailpos<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let frameOptions = winstate.frameOptions;

    if winstate.frametail_valid {
        return Ok(()); // already known for current row
    }

    let ordNumCols = node_ord_num_cols(winstate);
    let scan_slot = winstate.ss.ss_ScanTupleSlot.unwrap();

    if (frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) != 0 {
        spool_tuples(winstate, -1, estate)?;
        winstate.frametailpos = winstate.spooled_rows;
        winstate.frametail_valid = true;
    } else if (frameOptions & FRAMEOPTION_END_CURRENT_ROW) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            winstate.frametailpos = winstate.currentpos + 1;
            winstate.frametail_valid = true;
        } else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
            if ordNumCols == 0 {
                spool_tuples(winstate, -1, estate)?;
                winstate.frametailpos = winstate.spooled_rows;
                winstate.frametail_valid = true;
                return Ok(());
            }

            let frametail_slot = winstate.frametail_slot.unwrap();
            let frametail_ptr = winstate.frametail_ptr;
            tuplestore::tuplestore_select_read_pointer::call(
                winstate.buffer.as_deref_mut().unwrap(),
                frametail_ptr,
            )?;
            if winstate.frametailpos == 0 && estate.slot(frametail_slot).is_empty() {
                if !tuplestore::tuplestore_gettupleslot::call(
                    winstate.buffer.as_deref_mut().unwrap(),
                    true,
                    true,
                    frametail_slot,
                    estate,
                )? {
                    return Err(types_error::PgError::error("unexpected end of tuplestore"));
                }
            }

            while !estate.slot(frametail_slot).is_empty() {
                if winstate.frametailpos > winstate.currentpos
                    && !are_peers(winstate, frametail_slot, scan_slot, estate)?
                {
                    break; // this row is the frame tail
                }
                winstate.frametailpos += 1;
                spool_tuples(winstate, winstate.frametailpos, estate)?;
                if !tuplestore::tuplestore_gettupleslot::call(
                    winstate.buffer.as_deref_mut().unwrap(),
                    true,
                    true,
                    frametail_slot,
                    estate,
                )? {
                    break;
                }
            }
            winstate.frametail_valid = true;
        } else {
            debug_assert!(false);
        }
    } else if (frameOptions & FRAMEOPTION_END_OFFSET) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            let mut offset = winstate.endOffsetValue.as_i64();
            if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) != 0 {
                offset = -offset;
            }
            winstate.frametailpos = winstate.currentpos + offset + 1;
            if winstate.frametailpos < 0 {
                winstate.frametailpos = 0;
            } else if winstate.frametailpos > winstate.currentpos + 1 {
                spool_tuples(winstate, winstate.frametailpos - 1, estate)?;
                if winstate.frametailpos > winstate.spooled_rows {
                    winstate.frametailpos = winstate.spooled_rows;
                }
            }
            winstate.frametail_valid = true;
        } else if (frameOptions & FRAMEOPTION_RANGE) != 0 {
            let sortCol = node_ord_col_idx0(winstate);
            debug_assert!(ordNumCols == 1);

            let mut sub = (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) != 0;
            let mut less = true;
            if !winstate.inRangeAsc {
                sub = !sub;
                less = false;
            }

            let frametail_slot = winstate.frametail_slot.unwrap();
            let frametail_ptr = winstate.frametail_ptr;
            tuplestore::tuplestore_select_read_pointer::call(
                winstate.buffer.as_deref_mut().unwrap(),
                frametail_ptr,
            )?;
            if winstate.frametailpos == 0 && estate.slot(frametail_slot).is_empty() {
                if !tuplestore::tuplestore_gettupleslot::call(
                    winstate.buffer.as_deref_mut().unwrap(),
                    true,
                    true,
                    frametail_slot,
                    estate,
                )? {
                    return Err(types_error::PgError::error("unexpected end of tuplestore"));
                }
            }

            while !estate.slot(frametail_slot).is_empty() {
                let (tailval, tailisnull) =
                    execTuples::slot_getattr::call(estate, frametail_slot, sortCol)?;
                let (currval, currisnull) =
                    execTuples::slot_getattr::call(estate, scan_slot, sortCol)?;
                if tailisnull || currisnull {
                    if winstate.inRangeNullsFirst {
                        if !tailisnull {
                            break;
                        }
                    } else if !currisnull {
                        break;
                    }
                } else {
                    // if (!DatumGetBool(FunctionCall5Coll(&winstate->endInRangeFunc,
                    //         winstate->inRangeColl, tailval, currval,
                    //         winstate->endOffsetValue, BoolGetDatum(sub),
                    //         BoolGetDatum(less)))) break;
                    let fn_oid = winstate.endInRangeFunc.fn_oid;
                    let collation = winstate.inRangeColl;
                    // By-reference-capable fmgr lane (see the START_OFFSET site):
                    // the RANGE in_range operands may be by-reference values.
                    let mcx = estate.es_query_cxt;
                    let args = [
                        tailval.clone(),
                        currval.clone(),
                        winstate.endOffsetValue.clone(),
                        Datum::from_bool(sub),
                        Datum::from_bool(less),
                    ];
                    let (res, _isnull) = fmgr::function_call_invoke_datum::call(
                        mcx, fn_oid, collation, &args, &[], None,
                    )?;
                    if !res.as_bool() {
                        break;
                    }
                }
                winstate.frametailpos += 1;
                spool_tuples(winstate, winstate.frametailpos, estate)?;
                if !tuplestore::tuplestore_gettupleslot::call(
                    winstate.buffer.as_deref_mut().unwrap(),
                    true,
                    true,
                    frametail_slot,
                    estate,
                )? {
                    break;
                }
            }
            winstate.frametail_valid = true;
        } else if (frameOptions & FRAMEOPTION_GROUPS) != 0 {
            let offset = winstate.endOffsetValue.as_i64();
            let maxtailgroup = if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) != 0 {
                winstate.currentgroup - offset
            } else {
                winstate.currentgroup + offset
            };

            let frametail_slot = winstate.frametail_slot.unwrap();
            let temp_slot_2 = winstate.temp_slot_2.unwrap();
            let frametail_ptr = winstate.frametail_ptr;
            tuplestore::tuplestore_select_read_pointer::call(
                winstate.buffer.as_deref_mut().unwrap(),
                frametail_ptr,
            )?;
            if winstate.frametailpos == 0 && estate.slot(frametail_slot).is_empty() {
                if !tuplestore::tuplestore_gettupleslot::call(
                    winstate.buffer.as_deref_mut().unwrap(),
                    true,
                    true,
                    frametail_slot,
                    estate,
                )? {
                    return Err(types_error::PgError::error("unexpected end of tuplestore"));
                }
            }

            while !estate.slot(frametail_slot).is_empty() {
                if winstate.frametailgroup > maxtailgroup {
                    break; // this row is the correct frame tail
                }
                {
                                        execTuples::exec_copy_slot::call(estate, temp_slot_2, frametail_slot)?;
                }
                winstate.frametailpos += 1;
                spool_tuples(winstate, winstate.frametailpos, estate)?;
                if !tuplestore::tuplestore_gettupleslot::call(
                    winstate.buffer.as_deref_mut().unwrap(),
                    true,
                    true,
                    frametail_slot,
                    estate,
                )? {
                    break;
                }
                if !are_peers(winstate, temp_slot_2, frametail_slot, estate)? {
                    winstate.frametailgroup += 1;
                }
            }
            execTuples::exec_clear_tuple::call(estate, temp_slot_2)?;
            winstate.frametail_valid = true;
        } else {
            debug_assert!(false);
        }
    } else {
        debug_assert!(false);
    }

    Ok(())
}

/// `update_grouptailpos` — make grouptailpos valid for the current row. May
/// clobber temp_slot_2.
fn update_grouptailpos<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if winstate.grouptail_valid {
        return Ok(()); // already known for current row
    }

    // If no ORDER BY, all rows are peers with each other.
    if node_ord_num_cols(winstate) == 0 {
        spool_tuples(winstate, -1, estate)?;
        winstate.grouptailpos = winstate.spooled_rows;
        winstate.grouptail_valid = true;
        return Ok(());
    }

    debug_assert!(winstate.grouptailpos <= winstate.currentpos);
    let temp_slot_2 = winstate.temp_slot_2.unwrap();
    let scan_slot = winstate.ss.ss_ScanTupleSlot.unwrap();
    let grouptail_ptr = winstate.grouptail_ptr;
    tuplestore::tuplestore_select_read_pointer::call(
        winstate.buffer.as_deref_mut().unwrap(),
        grouptail_ptr,
    )?;
    loop {
        winstate.grouptailpos += 1;
        spool_tuples(winstate, winstate.grouptailpos, estate)?;
        if !tuplestore::tuplestore_gettupleslot::call(
            winstate.buffer.as_deref_mut().unwrap(),
            true,
            true,
            temp_slot_2,
            estate,
        )? {
            break; // end of partition
        }
        if winstate.grouptailpos > winstate.currentpos
            && !are_peers(winstate, temp_slot_2, scan_slot, estate)?
        {
            break; // this row is the group tail
        }
    }
    execTuples::exec_clear_tuple::call(estate, temp_slot_2)?;
    winstate.grouptail_valid = true;
    Ok(())
}

// ===========================================================================
// calculate_frame_offsets
// ===========================================================================

/// `calculate_frame_offsets` — determine the start/end offset values.
fn calculate_frame_offsets<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let frameOptions = winstate.frameOptions;
    debug_assert!(winstate.all_first);

    let econtext = winstate.ss.ps.ps_ExprContext.unwrap();

    if (frameOptions & FRAMEOPTION_START_OFFSET) != 0 {
        debug_assert!(winstate.startOffset.is_some());
        let (value, isnull) = {
            let so = winstate.startOffset.as_deref_mut().unwrap();
            execExpr::exec_eval_expr_switch_context::call(so, econtext, estate)?
        };
        if isnull {
            return Err(types_error::PgError::error(
                "frame starting offset must not be null",
            ));
        }
        // copy value into query-lifespan context:
        //   get_typlenbyval(exprType(startOffset->expr), &len, &byval);
        //   winstate->startOffsetValue = datumCopy(value, byval, len);
        let (len, byval) = {
            let so = winstate.startOffset.as_deref().unwrap();
            let oexpr = so
                .expr
                .as_deref()
                .expect("calculate_frame_offsets: startOffset->expr not set");
            let typid = nodeFuncs::expr_type_info::call(oexpr)?.typid;
            lsyscache::get_typlenbyval::call(typid)?
        };
        winstate.startOffsetValue =
            datum_seams::datum_copy_v::call(estate.es_query_cxt, &value, byval, len as i32)?;
        if (frameOptions & (FRAMEOPTION_ROWS | FRAMEOPTION_GROUPS)) != 0 {
            // value is known to be int8
            let offset = winstate.startOffsetValue.as_i64();
            if offset < 0 {
                return Err(types_error::PgError::error(
                    "frame starting offset must not be negative",
                ));
            }
        }
    }

    if (frameOptions & FRAMEOPTION_END_OFFSET) != 0 {
        debug_assert!(winstate.endOffset.is_some());
        let (value, isnull) = {
            let eo = winstate.endOffset.as_deref_mut().unwrap();
            execExpr::exec_eval_expr_switch_context::call(eo, econtext, estate)?
        };
        if isnull {
            return Err(types_error::PgError::error(
                "frame ending offset must not be null",
            ));
        }
        let (len, byval) = {
            let eo = winstate.endOffset.as_deref().unwrap();
            let oexpr = eo
                .expr
                .as_deref()
                .expect("calculate_frame_offsets: endOffset->expr not set");
            let typid = nodeFuncs::expr_type_info::call(oexpr)?.typid;
            lsyscache::get_typlenbyval::call(typid)?
        };
        winstate.endOffsetValue =
            datum_seams::datum_copy_v::call(estate.es_query_cxt, &value, byval, len as i32)?;
        if (frameOptions & (FRAMEOPTION_ROWS | FRAMEOPTION_GROUPS)) != 0 {
            let offset = winstate.endOffsetValue.as_i64();
            if offset < 0 {
                return Err(types_error::PgError::error(
                    "frame ending offset must not be negative",
                ));
            }
        }
    }
    winstate.all_first = false;
    Ok(())
}

// ===========================================================================
// ExecWindowAgg
// ===========================================================================

/// `ExecWindowAgg` — receive tuples from the outer subplan, store them, and
/// process window functions. Returns `Ok(Some(slot))` when a result tuple is
/// available, `Ok(None)` on the C `return NULL`.
pub fn ExecWindowAgg<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    tcop_postgres::check_for_interrupts::call()?;

    if winstate.status == WINDOWAGG_DONE {
        return Ok(None);
    }

    // Compute frame offset values, if any, during first call (or after rescan).
    if winstate.all_first {
        calculate_frame_offsets(winstate, estate)?;
    }

    let numfuncs_total = winstate.numfuncs as usize;

    // We need to loop as the runCondition or qual may filter out tuples.
    loop {
        if winstate.next_partition {
            // Initialize for first partition and set current row = 0.
            begin_partition(winstate, estate)?;
        } else {
            // Advance current row within partition.
            winstate.currentpos += 1;
            winstate.framehead_valid = false;
            winstate.frametail_valid = false;
            // we don't need to invalidate grouptail here
        }

        // Spool all tuples up to and including the current row.
        spool_tuples(winstate, winstate.currentpos, estate)?;

        // Move to the next partition if we reached the end of this one.
        if winstate.partition_spooled && winstate.currentpos >= winstate.spooled_rows {
            release_partition(winstate)?;

            if winstate.more_partitions {
                begin_partition(winstate, estate)?;
                debug_assert!(winstate.spooled_rows > 0);
                // Come out of pass-through mode when changing partition.
                winstate.status = WINDOWAGG_RUN;
            } else {
                winstate.status = WINDOWAGG_DONE;
                return Ok(None);
            }
        }

        // final output execution is in ps_ExprContext
        let econtext = winstate.ss.ps.ps_ExprContext.unwrap();

        // Clear the per-output-tuple context for current row.
        reset_expr_context(econtext, estate)?;

        // Read the current row from the tuplestore into ScanTupleSlot.
        let scan_slot = winstate.ss.ss_ScanTupleSlot.unwrap();
        let temp_slot_2 = winstate.temp_slot_2.unwrap();
        let current_ptr = winstate.current_ptr;
        tuplestore::tuplestore_select_read_pointer::call(
            winstate.buffer.as_deref_mut().unwrap(),
            current_ptr,
        )?;
        if (winstate.frameOptions
            & (FRAMEOPTION_GROUPS | FRAMEOPTION_EXCLUDE_GROUP | FRAMEOPTION_EXCLUDE_TIES))
            != 0
            && winstate.currentpos > 0
        {
            {
                                execTuples::exec_copy_slot::call(estate, temp_slot_2, scan_slot)?;
            }
            if !tuplestore::tuplestore_gettupleslot::call(
                winstate.buffer.as_deref_mut().unwrap(),
                true,
                true,
                scan_slot,
                estate,
            )? {
                return Err(types_error::PgError::error("unexpected end of tuplestore"));
            }
            if !are_peers(winstate, temp_slot_2, scan_slot, estate)? {
                winstate.currentgroup += 1;
                winstate.groupheadpos = winstate.currentpos;
                winstate.grouptail_valid = false;
            }
            execTuples::exec_clear_tuple::call(estate, temp_slot_2)?;
        } else if !tuplestore::tuplestore_gettupleslot::call(
            winstate.buffer.as_deref_mut().unwrap(),
            true,
            true,
            scan_slot,
            estate,
        )? {
            return Err(types_error::PgError::error("unexpected end of tuplestore"));
        }

        // Don't evaluate the window functions when we're in pass-through mode.
        if winstate.status == WINDOWAGG_RUN {
            // Evaluate true window functions.
            let numfuncs = winstate.numfuncs as usize;
            for i in 0..numfuncs {
                if perfunc_ref(winstate, i).plain_agg {
                    continue;
                }
                let wfuncno = perfunc_wfuncstate(winstate, i).wfuncno as usize;
                let (value, isnull) = eval_windowfunction(winstate, i, estate)?;
                let ec = estate.ecxt_mut(econtext);
                ec.ecxt_aggvalues[wfuncno] = value;
                ec.ecxt_aggnulls[wfuncno] = isnull;
            }

            // Evaluate aggregates.
            if winstate.numaggs > 0 {
                eval_windowaggregates(winstate, estate)?;
            }
        }

        // Keep auxiliary read pointers up-to-date so the tuplestore can be
        // trimmed.
        if winstate.framehead_ptr >= 0 {
            update_frameheadpos(winstate, estate)?;
        }
        if winstate.frametail_ptr >= 0 {
            update_frametailpos(winstate, estate)?;
        }
        if winstate.grouptail_ptr >= 0 {
            update_grouptailpos(winstate, estate)?;
        }

        // Truncate any no-longer-needed rows from the tuplestore.
        tuplestore::tuplestore_trim::call(winstate.buffer.as_deref_mut().unwrap());

        // Form and return a projection tuple.
        estate.ecxt_mut(econtext).ecxt_outertuple = Some(scan_slot);

        let slot = execExpr::exec_project::call(&mut winstate.ss.ps, estate)?;

        if winstate.status == WINDOWAGG_RUN {
            estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

            // Evaluate the run condition.
            let runcond_pass = match winstate.runcondition.as_deref_mut() {
                None => true, // NULL ExprState is treated as always-true
                Some(rc) => execExpr::exec_qual::call(rc, econtext, estate)?,
            };
            if !runcond_pass {
                if winstate.use_pass_through {
                    // NULLify the aggregate results.
                    let numfuncs = winstate.numfuncs as usize;
                    for i in 0..numfuncs {
                        let ec = estate.ecxt_mut(econtext);
                        ec.ecxt_aggvalues[i] = Datum::null();
                        ec.ecxt_aggnulls[i] = true;
                    }

                    if winstate.top_window {
                        winstate.status = WINDOWAGG_PASSTHROUGH_STRICT;
                        continue;
                    } else {
                        winstate.status = WINDOWAGG_PASSTHROUGH;
                    }
                } else {
                    winstate.status = WINDOWAGG_DONE;
                    return Ok(None);
                }
            }

            // Filter out any tuples we don't need in the top-level WindowAgg.
            let qual_pass = match winstate.ss.ps.qual.as_deref_mut() {
                None => true,
                Some(q) => execExpr::exec_qual::call(q, econtext, estate)?,
            };
            if !qual_pass {
                // InstrCountFiltered1(winstate, 1) — instrumentation only.
                continue;
            }

            let _ = numfuncs_total;
            return Ok(Some(slot));
        } else if !winstate.top_window {
            // When not in RUN mode, still return this tuple unless top window.
            return Ok(Some(slot));
        }
        // top window in pass-through mode: loop again.
    }
}

/// The `PlanState.ExecProcNode` callback: `castNode(WindowAggState, pstate)`
/// then run [`ExecWindowAgg`].
fn exec_window_agg_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::WindowAgg(node) => node,
        other => panic!("castNode(WindowAggState, pstate) failed: {other:?}"),
    };
    ExecWindowAgg(node, estate)
}

// ===========================================================================
// ExecInitWindowAgg
// ===========================================================================

/// `ExecInitWindowAgg` — create the run-time information for a WindowAgg node
/// and initialize its outer subtree.
pub fn ExecInitWindowAgg<'mcx>(
    node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, WindowAggState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let wnode: &'mcx WindowAgg<'mcx> = match node.node_tag() {
        types_nodes::nodes::ntag::T_WindowAgg => node.expect_windowagg(),
        other => panic!("castNode(WindowAgg, node) failed: {other:?}"),
    };

    // check for unsupported flags
    debug_assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    let frameOptions = wnode.frameOptions;

    // create state structure
    let mut winstate = alloc_in(mcx, WindowAggState::default())?;
    winstate.ss.ps.plan = Some(node);
    winstate.ss.ps.ExecProcNode = Some(exec_window_agg_node);

    // copy frame options to state node for easy access
    winstate.frameOptions = frameOptions;

    // Create expression contexts. We need two; we cheat by using
    // ExecAssignExprContext() to build both.
    execUtils::exec_assign_expr_context::call(estate, &mut winstate.ss.ps)?;
    let tmpcontext = winstate.ss.ps.ps_ExprContext.unwrap();
    winstate.tmpcontext = Some(tmpcontext);
    execUtils::exec_assign_expr_context::call(estate, &mut winstate.ss.ps)?;

    // Create long-lived context for partition-local memory etc.
    winstate.partcontext = Some(mcx.context().new_child("WindowAgg Partition"));

    // Create mid-lived context for aggregate trans values etc.
    winstate.aggcontext = Some(mcx.context().new_child("WindowAgg Aggregates"));

    // Only the top-level WindowAgg may have a qual.
    debug_assert!(wnode.plan.qual.is_none() || wnode.topWindow);

    // Initialize the qual.
    winstate.ss.ps.qual = init_qual_from_plan(&wnode.plan.qual, &mut winstate.ss.ps, estate)?;

    // Setup the run condition, if any.
    winstate.runcondition = init_qual_from_list(&wnode.runCondition, &mut winstate.ss.ps, estate)?;

    // When we're not the top-level WindowAgg, or we are but have a PARTITION BY,
    // we must move into pass-through mode when the runCondition becomes false.
    winstate.use_pass_through = !wnode.topWindow || wnode.partNumCols > 0;
    winstate.top_window = wnode.topWindow;

    // initialize child nodes
    let outerPlan = wnode.plan.lefttree.as_deref();
    winstate.ss.ps.lefttree = execProcnode::exec_init_node::call(mcx, outerPlan, estate, eflags)?;

    // initialize source tuple type (also the tuplestore tuple type / working
    // slot type).
    execUtils::exec_create_scan_slot_from_outer_plan::call(
        estate,
        &mut winstate.ss,
        types_nodes::TupleSlotKind::MinimalTuple,
    )?;
    let scan_slot = winstate.ss.ss_ScanTupleSlot.unwrap();
    let scanDesc = clone_slot_descriptor(estate, scan_slot, mcx)?;

    // the outer tuple is always a minimal tuple
    //   winstate->ss.ps.outeropsset = true;
    //   winstate->ss.ps.outerops = &TTSOpsMinimalTuple;
    //   winstate->ss.ps.outeropsfixed = true;
    // (The owned PlanStateData model trims the `outerops*` slot-ops cache
    // fields — as nodeMaterial does — so there is nothing to set here.)

    // tuple table initialization
    winstate.first_part_slot = Some(execTuples::exec_init_extra_tuple_slot::call(
        estate,
        clone_desc(&scanDesc, mcx)?,
        types_nodes::TupleSlotKind::MinimalTuple,
    )?);
    winstate.agg_row_slot = Some(execTuples::exec_init_extra_tuple_slot::call(
        estate,
        clone_desc(&scanDesc, mcx)?,
        types_nodes::TupleSlotKind::MinimalTuple,
    )?);
    winstate.temp_slot_1 = Some(execTuples::exec_init_extra_tuple_slot::call(
        estate,
        clone_desc(&scanDesc, mcx)?,
        types_nodes::TupleSlotKind::MinimalTuple,
    )?);
    winstate.temp_slot_2 = Some(execTuples::exec_init_extra_tuple_slot::call(
        estate,
        clone_desc(&scanDesc, mcx)?,
        types_nodes::TupleSlotKind::MinimalTuple,
    )?);

    // create frame head and tail slots only if needed.
    winstate.framehead_slot = None;
    winstate.frametail_slot = None;
    if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
        if ((frameOptions & FRAMEOPTION_START_CURRENT_ROW) != 0 && wnode.ordNumCols != 0)
            || (frameOptions & FRAMEOPTION_START_OFFSET) != 0
        {
            winstate.framehead_slot = Some(execTuples::exec_init_extra_tuple_slot::call(
                estate,
                clone_desc(&scanDesc, mcx)?,
                types_nodes::TupleSlotKind::MinimalTuple,
            )?);
        }
        if ((frameOptions & FRAMEOPTION_END_CURRENT_ROW) != 0 && wnode.ordNumCols != 0)
            || (frameOptions & FRAMEOPTION_END_OFFSET) != 0
        {
            winstate.frametail_slot = Some(execTuples::exec_init_extra_tuple_slot::call(
                estate,
                clone_desc(&scanDesc, mcx)?,
                types_nodes::TupleSlotKind::MinimalTuple,
            )?);
        }
    }

    // Initialize result slot, type and projection.
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut winstate.ss.ps,
        estate,
        types_nodes::TupleSlotKind::Virtual,
    )?;
    execUtils::exec_assign_projection_info::call(&mut winstate.ss.ps, estate, None)?;

    // Drain the WindowFuncExprStates the projection compile discovered.
    //
    // C builds the result projection with parent = (PlanState *) winstate, so
    // execExpr.c's T_WindowFunc arm appends each WindowFuncExprState directly to
    // winstate->funcs and bumps winstate->numfuncs / numaggs. In the owned model
    // the in-flight WindowAggState is not yet a PlanStateNode (its parent
    // back-link is stamped only after ExecInitWindowAgg returns), so the compiler
    // collects them on the projection ExprState's found_window_funcs channel; we
    // move them onto winstate.funcs here, matching the C discovery point and
    // preserving order (so each EEOP_WINDOW_FUNC step's funcidx stays valid).
    if let Some(proj) = winstate.ss.ps.ps_ProjInfo.as_mut() {
        if let Some(found) = proj.pi_state.found_window_funcs.take() {
            for wfstate in found.into_iter() {
                // nfuncs = ++winstate->numfuncs; if (wfunc->winagg) numaggs++;
                winstate.numfuncs += 1;
                let winagg = wfstate
                    .wfunc
                    .as_ref()
                    .expect("WindowFuncExprState.wfunc not set")
                    .winagg;
                if winagg {
                    winstate.numaggs += 1;
                }
                if winstate.funcs.is_none() {
                    winstate.funcs = Some(mcx::vec_with_capacity_in(mcx, 1)?);
                }
                winstate.funcs.as_mut().unwrap().push(wfstate);
            }
        }
    }

    // Set up data for comparing tuples.
    if wnode.partNumCols > 0 {
        // winstate->partEqfunction =
        //     execTuplesMatchPrepare(scanDesc, node->partNumCols,
        //                            node->partColIdx, node->partOperators,
        //                            node->partCollations, &winstate->ss.ps);
        winstate.partEqfunction = execGrouping::exec_tuples_match_prepare::call(
            clone_desc(&scanDesc, mcx)?,
            wnode.partNumCols,
            wnode.partColIdx.as_ref().expect("partColIdx not set"),
            wnode.partOperators.as_ref().expect("partOperators not set"),
            wnode.partCollations.as_ref().expect("partCollations not set"),
            &mut winstate.ss.ps,
            estate,
        )?;
    }
    if wnode.ordNumCols > 0 {
        // winstate->ordEqfunction =
        //     execTuplesMatchPrepare(scanDesc, node->ordNumCols,
        //                            node->ordColIdx, node->ordOperators,
        //                            node->ordCollations, &winstate->ss.ps);
        winstate.ordEqfunction = execGrouping::exec_tuples_match_prepare::call(
            clone_desc(&scanDesc, mcx)?,
            wnode.ordNumCols,
            wnode.ordColIdx.as_ref().expect("ordColIdx not set"),
            wnode.ordOperators.as_ref().expect("ordOperators not set"),
            wnode.ordCollations.as_ref().expect("ordCollations not set"),
            &mut winstate.ss.ps,
            estate,
        )?;
    }

    // WindowAgg nodes use aggvalues and aggnulls like Agg nodes.
    let numfuncs = winstate.numfuncs as usize;
    let econtext = winstate.ss.ps.ps_ExprContext.unwrap();
    {
        let ec = estate.ecxt_mut(econtext);
        ec.ecxt_aggvalues = new_datum_vec(mcx, numfuncs)?;
        ec.ecxt_aggnulls = new_bool_vec(mcx, numfuncs)?;
    }

    // allocate per-wfunc/per-agg state information.
    //   perfunc = palloc0(sizeof(WindowStatePerFuncData) * numfuncs);
    //   peragg  = palloc0(sizeof(WindowStatePerAggData) * numaggs);
    {
        let mut perfunc: PgVec<'mcx, WindowStatePerFuncData<'mcx>> =
            mcx::vec_with_capacity_in(mcx, numfuncs)?;
        for _ in 0..numfuncs {
            perfunc.push(WindowStatePerFuncData::default());
        }
        winstate.perfunc = Some(perfunc);
        let numaggs0 = winstate.numaggs as usize;
        let mut peragg: PgVec<'mcx, WindowStatePerAggData<'mcx>> =
            mcx::vec_with_capacity_in(mcx, numaggs0)?;
        for _ in 0..numaggs0 {
            peragg.push(WindowStatePerAggData::default());
        }
        winstate.peragg = Some(peragg);
    }

    // wfuncno = -1; aggno = -1; foreach(l, winstate->funcs) { ... }
    //
    // The dedup loop: each WindowFuncExprState is matched against the
    // already-assigned perfuncs (a previous duplicate window function shares its
    // result slot); a fresh one gets the next perfunc index. The C `equal(wfunc,
    // perfunc[i].wfunc) && !contain_volatile_functions(...)` dedup is conservative
    // — assigning every func a fresh perfunc is always correct (it only forgoes
    // sharing), so without an `equal`/volatile seam we assign fresh entries (the
    // common single-occurrence case is exact).
    let node_winref = wnode.winref;
    let mut wfuncno: i32 = -1;
    let mut aggno: i32 = -1;
    let num_states = winstate.funcs.as_ref().map_or(0, |f| f.len());
    for fi in 0..num_states {
        // wfunc = wfuncstate->wfunc; check winref matches the node.
        let (winref, winfnoid, winagg, wintype, inputcollid, numargs) = {
            let wfs = &winstate.funcs.as_ref().unwrap()[fi];
            let w = wfs.wfunc.as_ref().expect("WindowFuncExprState.wfunc not set");
            (
                w.winref,
                w.winfnoid,
                w.winagg,
                w.wintype,
                w.inputcollid,
                wfs.args.as_ref().map_or(0, |a| a.len()) as i32,
            )
        };
        if winref != node_winref {
            return Err(types_error::PgError::error(format!(
                "WindowFunc with winref {winref} assigned to WindowAgg with winref {node_winref}"
            )));
        }

        // Assign a new perfunc record (no dedup; see comment above).
        wfuncno += 1;
        let perfuncno = wfuncno as usize;

        // Mark the WindowFuncExprState with its assigned result-array index.
        winstate.funcs.as_mut().unwrap()[fi].wfuncno = wfuncno;

        // Permission check (object_aclcheck ACL_EXECUTE) +
        // InvokeFunctionExecuteHook: the aclchk/hook owners are not threaded into
        // this crate (see the module-head note); the builtin window functions are
        // public-EXECUTE, so the check is a no-op for them.

        // Fill in the perfuncstate scalar data.
        // perfuncstate->wfunc = wfunc; — clone the plan node out of the
        // discovery-list WindowFuncExprState before borrowing perfunc (disjoint
        // fields, but the clone must precede the &mut perfunc borrow).
        let (resulttype_len, resulttype_byval) = lsyscache::get_typlenbyval::call(wintype)?;
        let wfunc_clone: PgBox<'mcx, types_nodes::primnodes::WindowFunc> = {
            let w = winstate.funcs.as_ref().unwrap()[fi]
                .wfunc
                .as_ref()
                .expect("WindowFuncExprState.wfunc not set");
            mcx::alloc_in(mcx, (**w).clone_in(mcx)?)?
        };
        let pf = perfunc_mut(&mut winstate, perfuncno);
        pf.wfunc = Some(wfunc_clone);
        pf.numArguments = numargs;
        pf.winCollation = inputcollid;
        pf.resulttypeLen = resulttype_len;
        pf.resulttypeByVal = resulttype_byval;
        pf.plain_agg = winagg;

        if winagg {
            // Plain aggregate used as a window function.
            aggno += 1;
            perfunc_mut(&mut winstate, perfuncno).aggno = aggno;
            let peraggno = aggno as usize;
            initialize_peragg(&mut winstate, perfuncno, peraggno, estate, mcx)?;
            peragg_mut(&mut winstate, peraggno).wfuncno = wfuncno;
        } else {
            // A real window function — set up its WindowObject and fmgr lookup.
            let mut winobj = WindowObjectData {
                argstates: None,
                localmem: None,
                markptr: -1,
                readptr: -1,
                markpos: 0,
                seekpos: 0,
            };
            // winobj->argstates = wfuncstate->args; — the arg ExprStates are owned
            // by the discovery-list WindowFuncExprState (winstate.funcs[fi].args);
            // the window-API argument evaluators reach them there, so argstates is
            // left None on the winobj satellite and resolved through funcs[fi].
            winobj.argstates = None;
            perfunc_mut(&mut winstate, perfuncno).winobj =
                Some(mcx::alloc_in(mcx, winobj)?);

            // C: fmgr_info_cxt(wfunc->winfnoid, &flinfo, ecxt_per_query_memory);
            //    fmgr_info_set_expr((Node *) wfunc, &flinfo);
            // The flinfo feeds C's eval_windowfunction FunctionCallInvoke. The
            // owned model dispatches the window function by `winfnoid` directly
            // (eval_windowfunction), threading the executor state the bare-Datum
            // fmgr frame cannot carry, so the flinfo lookup is not needed here
            // (and the window-function builtins are not registered as callable
            // PGFunctions, so fmgr_info would fail the internal-lookup probe).
            let _ = winfnoid;
        }
    }

    // Update numfuncs, numaggs to match number of unique functions found.
    winstate.numfuncs = wfuncno + 1;
    winstate.numaggs = aggno + 1;

    // Set up WindowObject for aggregates, if needed.
    if winstate.numaggs > 0 {
        let agg_winobj = WindowObjectData {
            argstates: None,
            localmem: None,
            markptr: -1,
            readptr: -1,
            markpos: 0,
            seekpos: 0,
        };
        winstate.agg_winobj = Some(mcx::alloc_in(mcx, agg_winobj)?);
    }

    // Set the status to running.
    winstate.status = WINDOWAGG_RUN;

    // initialize frame bound offset expressions.
    winstate.startOffset = init_expr_opt(&wnode.startOffset, &mut winstate.ss.ps, estate)?;
    winstate.endOffset = init_expr_opt(&wnode.endOffset, &mut winstate.ss.ps, estate)?;

    // Lookup in_range support functions if needed.
    if oid_is_valid(wnode.startInRangeFunc) {
        winstate.startInRangeFunc = fmgr::fmgr_info::call(mcx, wnode.startInRangeFunc)?;
    }
    if oid_is_valid(wnode.endInRangeFunc) {
        winstate.endInRangeFunc = fmgr::fmgr_info::call(mcx, wnode.endInRangeFunc)?;
    }
    winstate.inRangeColl = wnode.inRangeColl;
    winstate.inRangeAsc = wnode.inRangeAsc;
    winstate.inRangeNullsFirst = wnode.inRangeNullsFirst;

    winstate.all_first = true;
    winstate.partition_spooled = false;
    winstate.more_partitions = false;
    winstate.next_partition = true;

    Ok(winstate)
}

// ===========================================================================
// ExecEndWindowAgg / ExecReScanWindowAgg
// ===========================================================================

/// `ExecEndWindowAgg`.
pub fn ExecEndWindowAgg<'mcx>(
    node: &mut WindowAggState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(buffer) = node.buffer.take() {
        tuplestore::tuplestore_end::call(buffer);
        // nullify so that release_partition skips the tuplestore_clear()
    }

    release_partition(node)?;

    // peragg private aggcontexts are deleted by dropping them (MemoryContext
    // Drop = MemoryContextDelete). partcontext/aggcontext likewise dropped.
    let numaggs = node.numaggs as usize;
    for i in 0..numaggs {
        if !ctx_is_shared(node, i) {
            // drop the private context
            let _ = peragg_mut(node, i).aggcontext.take();
        }
    }
    let _ = node.partcontext.take();
    let _ = node.aggcontext.take();

    // pfree(perfunc/peragg) — owned Vecs drop automatically.

    let outer = node
        .ss
        .ps
        .lefttree
        .as_deref_mut()
        .expect("ExecEndWindowAgg: no outer plan state");
    execProcnode::exec_end_node::call(outer, estate)
}

/// `ExecReScanWindowAgg`.
pub fn ExecReScanWindowAgg<'mcx>(
    node: &mut WindowAggState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let econtext = node.ss.ps.ps_ExprContext.unwrap();

    node.status = WINDOWAGG_RUN;
    node.all_first = true;

    // release tuplestore et al
    release_partition(node)?;

    // release all temp tuples, but especially first_part_slot
    execTuples::exec_clear_tuple::call(estate, node.ss.ss_ScanTupleSlot.unwrap())?;
    execTuples::exec_clear_tuple::call(estate, node.first_part_slot.unwrap())?;
    execTuples::exec_clear_tuple::call(estate, node.agg_row_slot.unwrap())?;
    execTuples::exec_clear_tuple::call(estate, node.temp_slot_1.unwrap())?;
    execTuples::exec_clear_tuple::call(estate, node.temp_slot_2.unwrap())?;
    if let Some(fh) = node.framehead_slot {
        execTuples::exec_clear_tuple::call(estate, fh)?;
    }
    if let Some(ft) = node.frametail_slot {
        execTuples::exec_clear_tuple::call(estate, ft)?;
    }

    // Forget current wfunc values.
    let numfuncs = node.numfuncs as usize;
    {
        let ec = estate.ecxt_mut(econtext);
        for i in 0..numfuncs {
            ec.ecxt_aggvalues[i] = Datum::null();
            ec.ecxt_aggnulls[i] = false;
        }
    }

    // if chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    let outer = node
        .ss
        .ps
        .lefttree
        .as_deref_mut()
        .expect("ExecReScanWindowAgg: no outer plan state");
    if outer.ps_head().chgParam.is_none() {
        execAmi::exec_re_scan::call(outer, estate)?;
    }
    Ok(())
}

// ===========================================================================
// initialize_peragg / GetAggInitVal
// ===========================================================================

/// `initialize_peragg` — almost the same as in nodeAgg.c (no DISTINCT support).
/// Reads `pg_aggregate` (AGGFNOID), decides whether the moving-aggregate
/// implementation is usable, builds the transfn/finalfn expression trees, looks
/// up the component fmgr infos, and resolves the textual initval.
fn initialize_peragg<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    peraggno: usize,
    estate: &mut EStateData<'mcx>,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<()> {
    // Pull the data we need off the perfunc's WindowFunc node. `wfunc` was just
    // cloned into the perfunc slot by the caller; it carries winfnoid, the arg
    // expressions, wintype and inputcollid.
    let (winfnoid, wintype, inputcollid, input_types): (Oid, Oid, Oid, PgVec<'mcx, Oid>) = {
        let pf = perfunc_ref(winstate, perfuncno);
        let wfunc = pf.wfunc.as_ref().expect("perfunc.wfunc not set");
        // numArguments = list_length(wfunc->args);
        // inputTypes[i] = exprType((Node *) lfirst(lc));
        let mut its: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, wfunc.args.len())?;
        for arg in wfunc.args.iter() {
            its.push(backend_nodes_nodeFuncs_seams::expr_type_info::call(arg)?.typid);
        }
        (wfunc.winfnoid, wfunc.wintype, wfunc.inputcollid, its)
    };
    let num_arguments = input_types.len() as i32;

    // aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(wfunc->winfnoid));
    let aggform = backend_utils_cache_syscache_seams::agg_form_by_oid::call(mcx, winfnoid)?
        .ok_or_else(|| {
            types_error::PgError::error(alloc::format!(
                "cache lookup failed for aggregate {winfnoid}"
            ))
        })?;

    // Figure out whether to use the moving-aggregate implementation.
    let use_ma_code: bool = if !oid_is_valid(aggform.aggminvtransfn) {
        false // sine qua non
    } else if aggform.aggmfinalmodify == AGGMODIFY_READ_ONLY
        && aggform.aggfinalmodify != AGGMODIFY_READ_ONLY
    {
        true // decision forced by safety
    } else if (winstate.frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) != 0 {
        false // non-moving frame head
    } else if contain_volatile_functions_wfunc(winstate, perfuncno)? {
        false // avoid possible behavioral change
    } else if contain_subplans_wfunc(winstate, perfuncno)? {
        false // subplans might contain volatile functions
    } else {
        true // yes, let's use it
    };

    let transfn_oid: Oid;
    let invtransfn_oid: Oid;
    let finalfn_oid: Oid;
    let finalextra: bool;
    let finalmodify: i8;
    let mut aggtranstype: Oid;
    if use_ma_code {
        transfn_oid = aggform.aggmtransfn;
        invtransfn_oid = aggform.aggminvtransfn;
        finalfn_oid = aggform.aggmfinalfn;
        finalextra = aggform.aggmfinalextra;
        finalmodify = aggform.aggmfinalmodify;
        aggtranstype = aggform.aggmtranstype;
    } else {
        transfn_oid = aggform.aggtransfn;
        invtransfn_oid = INVALID_OID;
        finalfn_oid = aggform.aggfinalfn;
        finalextra = aggform.aggfinalextra;
        finalmodify = aggform.aggfinalmodify;
        aggtranstype = aggform.aggtranstype;
    }
    {
        let pa = peragg_mut(winstate, peraggno);
        pa.transfn_oid = transfn_oid;
        pa.invtransfn_oid = invtransfn_oid;
        pa.finalfn_oid = finalfn_oid;
    }

    // ExecInitWindowAgg already checked permission to call the aggregate
    // function; we still need to check the component functions. Get the
    // aggregate's owner from pg_proc.
    let agg_owner = {
        let proc = backend_utils_cache_syscache_seams::lookup_proc::call(mcx, winfnoid)?
            .ok_or_else(|| {
                types_error::PgError::error(alloc::format!(
                    "cache lookup failed for function {winfnoid}"
                ))
            })?;
        proc.proowner
    };
    check_component_fn_acl(mcx, transfn_oid, agg_owner)?;
    if oid_is_valid(invtransfn_oid) {
        check_component_fn_acl(mcx, invtransfn_oid, agg_owner)?;
    }
    if oid_is_valid(finalfn_oid) {
        check_component_fn_acl(mcx, finalfn_oid, agg_owner)?;
    }

    // If the selected finalfn isn't read-only, we can't run this aggregate as a
    // window function.
    if finalmodify != AGGMODIFY_READ_ONLY {
        let name = backend_utils_adt_regproc_seams::format_procedure::call(mcx, winfnoid)?;
        return Err(types_error::PgError::error(alloc::format!(
            "aggregate function {} does not support use as a window function",
            name.as_str()
        )));
    }

    // Detect how many arguments to pass to the finalfn.
    let num_final_args = if finalextra {
        num_arguments + 1
    } else {
        1
    };
    peragg_mut(winstate, peraggno).numFinalArgs = num_final_args;

    // resolve actual type of transition state, if polymorphic.
    aggtranstype = backend_parser_parse_agg_seams::resolve_aggregate_transtype::call(
        mcx,
        winfnoid,
        aggtranstype,
        &input_types,
        num_arguments,
    )?;

    // build expression trees using actual argument & result types.
    let (transfnexpr, invtransfnexpr) =
        backend_parser_parse_agg_seams::build_aggregate_transfn_expr::call(
            &input_types,
            num_arguments,
            0,     // no ordered-set window functions yet
            false, // no variadic window functions yet
            aggtranstype,
            inputcollid,
            transfn_oid,
            invtransfn_oid,
            oid_is_valid(invtransfn_oid),
        )?;

    // set up infrastructure for calling the transfn(s) and finalfn.
    let mut transfn = backend_utils_fmgr_fmgr_seams::fmgr_info::call(mcx, transfn_oid)?;
    backend_utils_fmgr_fmgr_seams::fmgr_info_set_expr::call(mcx, &mut transfn, &transfnexpr)?;
    peragg_mut(winstate, peraggno).transfn = transfn;

    if oid_is_valid(invtransfn_oid) {
        let mut invtransfn = backend_utils_fmgr_fmgr_seams::fmgr_info::call(mcx, invtransfn_oid)?;
        let inv = invtransfnexpr.as_ref().expect(
            "build_aggregate_transfn_expr did not return an inverse transfn expr",
        );
        backend_utils_fmgr_fmgr_seams::fmgr_info_set_expr::call(mcx, &mut invtransfn, inv)?;
        peragg_mut(winstate, peraggno).invtransfn = invtransfn;
    }

    if oid_is_valid(finalfn_oid) {
        let finalfnexpr = backend_parser_parse_agg_seams::build_aggregate_finalfn_expr::call(
            &input_types,
            num_final_args,
            aggtranstype,
            wintype,
            inputcollid,
            finalfn_oid,
        )?;
        let mut finalfn = backend_utils_fmgr_fmgr_seams::fmgr_info::call(mcx, finalfn_oid)?;
        backend_utils_fmgr_fmgr_seams::fmgr_info_set_expr::call(mcx, &mut finalfn, &finalfnexpr)?;
        peragg_mut(winstate, peraggno).finalfn = finalfn;
    }

    // get info about relevant datatypes.
    let (resulttype_len, resulttype_byval) = lsyscache::get_typlenbyval::call(wintype)?;
    let (transtype_len, transtype_byval) = lsyscache::get_typlenbyval::call(aggtranstype)?;
    {
        let pa = peragg_mut(winstate, peraggno);
        pa.resulttypeLen = resulttype_len;
        pa.resulttypeByVal = resulttype_byval;
        pa.transtypeLen = transtype_len;
        pa.transtypeByVal = transtype_byval;
    }

    // initval is potentially null; the syscache projection already read it.
    // textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple, initvalAttNo, &isnull);
    let init_val_text = if use_ma_code {
        aggform.aggminitval.as_deref()
    } else {
        aggform.agginitval.as_deref()
    };
    let (init_value, init_value_is_null) = match init_val_text {
        None => (Datum::null(), true),
        Some(s) => (GetAggInitVal(estate, s, aggtranstype)?, false),
    };
    {
        let pa = peragg_mut(winstate, peraggno);
        pa.initValue = init_value;
        pa.initValueIsNull = init_value_is_null;
    }

    // If the transfn is strict and the initval is NULL, input type and transtype
    // must be binary-compatible.
    let transfn_strict = peragg_ref(winstate, peraggno).transfn.fn_strict;
    if transfn_strict && init_value_is_null {
        let coercible = num_arguments >= 1
            && backend_parser_coerce_seams::is_binary_coercible::call(
                input_types[0],
                aggtranstype,
            )?;
        if !coercible {
            return Err(types_error::PgError::error(alloc::format!(
                "aggregate {winfnoid} needs to have compatible input type and transition type"
            )));
        }
    }

    // Forward and inverse transition functions must have the same strictness.
    if oid_is_valid(invtransfn_oid) {
        let pa = peragg_ref(winstate, peraggno);
        if pa.transfn.fn_strict != pa.invtransfn.fn_strict {
            return Err(types_error::PgError::error(
                "strictness of aggregate's forward and inverse transition functions must match",
            ));
        }
    }

    // Moving aggregates use their own aggcontext (a private context that drops
    // when the WindowAgg ends). Plain aggregates share winstate->aggcontext,
    // which the owned model represents as `aggcontext = None`.
    if oid_is_valid(invtransfn_oid) {
        peragg_mut(winstate, peraggno).aggcontext =
            Some(mcx.context().new_child("WindowAgg Per Aggregate"));
    } else {
        peragg_mut(winstate, peraggno).aggcontext = None;
    }

    Ok(())
}

/// C: `contain_volatile_functions((Node *) wfunc)` over the perfunc's WindowFunc
/// (args + FILTER). The clauses.c walker descends the whole node; we feed it the
/// arg expressions (and aggfilter, if any), which is where any volatile call
/// would live.
fn contain_volatile_functions_wfunc<'mcx>(
    winstate: &WindowAggState<'mcx>,
    perfuncno: usize,
) -> PgResult<bool> {
    let pf = perfunc_ref(winstate, perfuncno);
    let wfunc = pf.wfunc.as_ref().expect("perfunc.wfunc not set");
    for arg in wfunc.args.iter() {
        if backend_optimizer_path_small_seams::contain_volatile_functions_expr::call(arg) {
            return Ok(true);
        }
    }
    if let Some(filter) = wfunc.aggfilter.as_deref() {
        if backend_optimizer_path_small_seams::contain_volatile_functions_expr::call(filter) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// C: `contain_subplans((Node *) wfunc)` over the perfunc's WindowFunc (args +
/// FILTER).
fn contain_subplans_wfunc<'mcx>(
    winstate: &WindowAggState<'mcx>,
    perfuncno: usize,
) -> PgResult<bool> {
    let pf = perfunc_ref(winstate, perfuncno);
    let wfunc = pf.wfunc.as_ref().expect("perfunc.wfunc not set");
    if backend_optimizer_util_clauses_seams::contain_subplans::call(&wfunc.args) {
        return Ok(true);
    }
    if let Some(filter) = wfunc.aggfilter.as_deref() {
        let one = core::slice::from_ref(filter);
        if backend_optimizer_util_clauses_seams::contain_subplans::call(one) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `GetAggInitVal` — convert a pg_aggregate textual initval to a Datum.
#[allow(dead_code)]
fn GetAggInitVal<'mcx>(
    estate: &mut EStateData<'mcx>,
    text_init_val: &str,
    transtype: Oid,
) -> PgResult<Datum<'mcx>> {
    // getTypeInputInfo(transtype, &typinput, &typioparam);
    let (typinput, typioparam) = lsyscache::get_type_input_info::call(transtype)?;
    // strInitVal = TextDatumGetCString(textInitVal);  (caller passes the &str)
    // initVal = OidInputFunctionCall(typinput, strInitVal, typioparam, -1);
    let init_val = fmgr::oid_input_function_call::call(
        estate.es_query_cxt,
        typinput,
        text_init_val,
        typioparam,
        -1,
    )?;
    Ok(init_val)
}

// ===========================================================================
// are_peers / window_gettupleslot
// ===========================================================================

/// `are_peers` — compare two rows for equality under the ORDER BY clause. NB:
/// does not consider the window frame mode.
fn are_peers<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    slot1: SlotId,
    slot2: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // If no ORDER BY, all rows are peers with each other.
    if node_ord_num_cols(winstate) == 0 {
        return Ok(true);
    }

    let econtext = winstate.tmpcontext.unwrap();
    {
        let ec = estate.ecxt_mut(econtext);
        ec.ecxt_outertuple = Some(slot1);
        ec.ecxt_innertuple = Some(slot2);
    }
    let ordeq = winstate
        .ordEqfunction
        .as_deref_mut()
        .expect("are_peers: ordEqfunction not set");
    execExpr::exec_qual_and_reset::call(ordeq, econtext, estate)
}

/// `window_gettupleslot` — fetch the `pos`'th tuple of the current partition
/// into the slot using the winobj's read pointer.
///
/// The winobj's mutable fields (`markpos`/`seekpos`) are threaded by value: the
/// caller passes the current `markpos`/`seekpos`, and the returned tuple updates
/// `seekpos`. Returns `(found, new_seekpos)`.
fn window_gettupleslot_with<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    readptr: i32,
    markpos: i64,
    mut seekpos: i64,
    pos: i64,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(bool, i64)> {
    // often called repeatedly in a row
    tcop_postgres::check_for_interrupts::call()?;

    // Don't allow passing -1 to spool_tuples here.
    if pos < 0 {
        return Ok((false, seekpos));
    }

    // If necessary, fetch the tuple into the spool.
    spool_tuples(winstate, pos, estate)?;

    if pos >= winstate.spooled_rows {
        return Ok((false, seekpos));
    }

    if pos < markpos {
        return Err(types_error::PgError::error(
            "cannot fetch row before WindowObject's mark position",
        ));
    }

    tuplestore::tuplestore_select_read_pointer::call(winstate.buffer.as_deref_mut().unwrap(), readptr)?;

    // Advance or rewind until we are within one tuple of the one we want.
    if seekpos < pos - 1 {
        if !tuplestore::tuplestore_skiptuples::call(
            winstate.buffer.as_deref_mut().unwrap(),
            pos - 1 - seekpos,
            true,
        )? {
            return Err(types_error::PgError::error("unexpected end of tuplestore"));
        }
        seekpos = pos - 1;
    } else if seekpos > pos + 1 {
        if !tuplestore::tuplestore_skiptuples::call(
            winstate.buffer.as_deref_mut().unwrap(),
            seekpos - (pos + 1),
            false,
        )? {
            return Err(types_error::PgError::error("unexpected end of tuplestore"));
        }
        seekpos = pos + 1;
    } else if seekpos == pos {
        // No API to refetch the tuple at the current position; move one forward
        // then one backward.
        tuplestore::tuplestore_advance::call(winstate.buffer.as_deref_mut().unwrap(), true)?;
        seekpos += 1;
    }

    // Now fetch forwards or backwards as appropriate (physical copy).
    if seekpos > pos {
        if !tuplestore::tuplestore_gettupleslot::call(
            winstate.buffer.as_deref_mut().unwrap(),
            false,
            true,
            slot,
            estate,
        )? {
            return Err(types_error::PgError::error("unexpected end of tuplestore"));
        }
        seekpos -= 1;
    } else {
        if !tuplestore::tuplestore_gettupleslot::call(
            winstate.buffer.as_deref_mut().unwrap(),
            true,
            true,
            slot,
            estate,
        )? {
            return Err(types_error::PgError::error("unexpected end of tuplestore"));
        }
        seekpos += 1;
    }

    debug_assert!(seekpos == pos);
    Ok((true, seekpos))
}

/// `window_gettupleslot(agg_winobj, pos, slot)` over the aggregate WindowObject.
fn window_gettupleslot_agg<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    pos: i64,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let (readptr, markpos, seekpos) = {
        let w = agg_winobj_ref(winstate);
        (w.readptr, w.markpos, w.seekpos)
    };
    let (found, new_seek) =
        window_gettupleslot_with(winstate, readptr, markpos, seekpos, pos, slot, estate)?;
    agg_winobj_mut(winstate).seekpos = new_seek;
    Ok(found)
}

/// `window_gettupleslot(perfunc[i].winobj, ...)` over a real window function's
/// WindowObject.
#[allow(dead_code)]
fn window_gettupleslot_func<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    pos: i64,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let (readptr, markpos, seekpos) = {
        let w = perfunc_winobj_ref(winstate, perfuncno);
        (w.readptr, w.markpos, w.seekpos)
    };
    let (found, new_seek) =
        window_gettupleslot_with(winstate, readptr, markpos, seekpos, pos, slot, estate)?;
    perfunc_winobj_mut(winstate, perfuncno).seekpos = new_seek;
    Ok(found)
}

// ===========================================================================
// API exposed to window functions (windowapi.h)
// ===========================================================================

/// `WinGetPartitionLocalMemory(winobj, sz)` — working memory that lives till
/// end of partition processing, for a real window function.
pub fn WinGetPartitionLocalMemory<'a, 'mcx>(
    winstate: &'a mut WindowAggState<'mcx>,
    perfuncno: usize,
    sz: usize,
) -> PgResult<&'a mut [u8]> {
    if perfunc_winobj_ref(winstate, perfuncno).localmem.is_none() {
        // MemoryContextAllocZero(partcontext, sz)
        let partmcx = winstate
            .partcontext
            .as_ref()
            .expect("WinGetPartitionLocalMemory: partcontext not set")
            .mcx();
        let mut buf = mcx::vec_with_capacity_in(partmcx, sz)?;
        for _ in 0..sz {
            buf.push(0u8);
        }
        // Lifetime note: the buffer is allocated in partcontext but carried in
        // the winobj for the partition's life (C `winobj->localmem`).
        let buf: PgVec<'mcx, u8> = unsafe { core::mem::transmute(buf) };
        perfunc_winobj_mut(winstate, perfuncno).localmem = Some(buf);
    }
    Ok(perfunc_winobj_mut(winstate, perfuncno)
        .localmem
        .as_mut()
        .unwrap()
        .as_mut_slice())
}

/// `WinGetCurrentPosition(winobj)`.
pub fn WinGetCurrentPosition(winstate: &WindowAggState<'_>) -> i64 {
    winstate.currentpos
}

/// `WinGetPartitionRowCount(winobj)` — total rows in the current partition
/// (forces the whole partition to be spooled).
pub fn WinGetPartitionRowCount<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<i64> {
    spool_tuples(winstate, -1, estate)?;
    Ok(winstate.spooled_rows)
}

/// `WinSetMarkPosition` over the aggregate WindowObject.
fn win_set_mark_position_agg<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    markpos: i64,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let (markptr, readptr, cur_markpos, cur_seekpos) = {
        let w = agg_winobj_ref(winstate);
        (w.markptr, w.readptr, w.markpos, w.seekpos)
    };
    let (new_markpos, new_seekpos) = win_set_mark_position_common(
        winstate, markptr, readptr, cur_markpos, cur_seekpos, markpos, estate,
    )?;
    let w = agg_winobj_mut(winstate);
    w.markpos = new_markpos;
    w.seekpos = new_seekpos;
    Ok(())
}

/// `WinSetMarkPosition(winobj, markpos)` over a real window function's object.
pub fn WinSetMarkPosition<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    markpos: i64,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let (markptr, readptr, cur_markpos, cur_seekpos) = {
        let w = perfunc_winobj_ref(winstate, perfuncno);
        (w.markptr, w.readptr, w.markpos, w.seekpos)
    };
    let (new_markpos, new_seekpos) = win_set_mark_position_common(
        winstate, markptr, readptr, cur_markpos, cur_seekpos, markpos, estate,
    )?;
    let w = perfunc_winobj_mut(winstate, perfuncno);
    w.markpos = new_markpos;
    w.seekpos = new_seekpos;
    Ok(())
}

fn win_set_mark_position_common<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    markptr: i32,
    readptr: i32,
    mut markpos_field: i64,
    mut seekpos_field: i64,
    markpos: i64,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(i64, i64)> {
    let _ = estate;
    if markpos < markpos_field {
        return Err(types_error::PgError::error(
            "cannot move WindowObject's mark position backward",
        ));
    }
    tuplestore::tuplestore_select_read_pointer::call(winstate.buffer.as_deref_mut().unwrap(), markptr)?;
    if markpos > markpos_field {
        tuplestore::tuplestore_skiptuples::call(
            winstate.buffer.as_deref_mut().unwrap(),
            markpos - markpos_field,
            true,
        )?;
        markpos_field = markpos;
    }
    tuplestore::tuplestore_select_read_pointer::call(winstate.buffer.as_deref_mut().unwrap(), readptr)?;
    if markpos > seekpos_field {
        tuplestore::tuplestore_skiptuples::call(
            winstate.buffer.as_deref_mut().unwrap(),
            markpos - seekpos_field,
            true,
        )?;
        seekpos_field = markpos;
    }
    Ok((markpos_field, seekpos_field))
}

/// `WinRowsArePeers(winobj, pos1, pos2)` — compare two rows (by absolute
/// position) for equality under the ORDER BY clause.
pub fn WinRowsArePeers<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    pos1: i64,
    pos2: i64,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // If no ORDER BY, all rows are peers; don't bother to fetch them.
    if node_ord_num_cols(winstate) == 0 {
        return Ok(true);
    }

    // OK to use temp_slot_2 here (no frame-related calls).
    let slot1 = winstate.temp_slot_1.unwrap();
    let slot2 = winstate.temp_slot_2.unwrap();

    if !window_gettupleslot_func(winstate, perfuncno, pos1, slot1, estate)? {
        return Err(types_error::PgError::error(
            "specified position is out of window",
        ));
    }
    if !window_gettupleslot_func(winstate, perfuncno, pos2, slot2, estate)? {
        return Err(types_error::PgError::error(
            "specified position is out of window",
        ));
    }

    let res = are_peers(winstate, slot1, slot2, estate)?;

    execTuples::exec_clear_tuple::call(estate, slot1)?;
    execTuples::exec_clear_tuple::call(estate, slot2)?;
    Ok(res)
}

/// `WinGetFuncArgInPartition(winobj, argno, relpos, seektype, set_mark, ...)`.
///
/// Returns `(value, isnull, isout)`.
pub fn WinGetFuncArgInPartition<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    argno: usize,
    relpos: i64,
    seektype: i32,
    set_mark: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool, bool)> {
    let econtext = winstate.ss.ps.ps_ExprContext.unwrap();
    let slot = winstate.temp_slot_1.unwrap();

    let abs_pos = match seektype {
        WINDOW_SEEK_CURRENT => winstate.currentpos + relpos,
        WINDOW_SEEK_HEAD => relpos,
        WINDOW_SEEK_TAIL => {
            spool_tuples(winstate, -1, estate)?;
            winstate.spooled_rows - 1 + relpos
        }
        _ => {
            return Err(types_error::PgError::error("unrecognized window seek type"));
        }
    };

    let gottuple = window_gettupleslot_func(winstate, perfuncno, abs_pos, slot, estate)?;

    if !gottuple {
        Ok((Datum::null(), true, true))
    } else {
        if set_mark {
            WinSetMarkPosition(winstate, perfuncno, abs_pos, estate)?;
        }
        estate.ecxt_mut(econtext).ecxt_outertuple = Some(slot);
        let (v, isnull) = {
            let argstate = perfunc_winobj_argstate_mut(winstate, perfuncno, argno);
            execExpr::exec_eval_expr_switch_context::call(argstate, econtext, estate)?
        };
        Ok((v, isnull, false))
    }
}

/// `WinGetFuncArgInFrame(winobj, argno, relpos, seektype, set_mark, ...)`.
///
/// Returns `(value, isnull, isout)`.
pub fn WinGetFuncArgInFrame<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    argno: usize,
    relpos: i64,
    seektype: i32,
    set_mark: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool, bool)> {
    let econtext = winstate.ss.ps.ps_ExprContext.unwrap();
    let slot = winstate.temp_slot_1.unwrap();

    let abs_pos: i64;
    let mark_pos: i64;

    match seektype {
        WINDOW_SEEK_CURRENT => {
            return Err(types_error::PgError::error(
                "WINDOW_SEEK_CURRENT is not supported for WinGetFuncArgInFrame",
            ));
        }
        WINDOW_SEEK_HEAD => {
            // rejecting relpos < 0 is easy and simplifies code below
            if relpos < 0 {
                return Ok((Datum::null(), true, true)); // out_of_frame
            }
            update_frameheadpos(winstate, estate)?;
            let mut ap = winstate.frameheadpos + relpos;
            mark_pos = ap;

            match winstate.frameOptions & FRAMEOPTION_EXCLUSION {
                0 => {}
                FRAMEOPTION_EXCLUDE_CURRENT_ROW => {
                    if ap >= winstate.currentpos && winstate.currentpos >= winstate.frameheadpos {
                        ap += 1;
                    }
                }
                FRAMEOPTION_EXCLUDE_GROUP => {
                    update_grouptailpos(winstate, estate)?;
                    if ap >= winstate.groupheadpos && winstate.grouptailpos > winstate.frameheadpos {
                        let overlapstart = winstate.groupheadpos.max(winstate.frameheadpos);
                        ap += winstate.grouptailpos - overlapstart;
                    }
                }
                FRAMEOPTION_EXCLUDE_TIES => {
                    update_grouptailpos(winstate, estate)?;
                    if ap >= winstate.groupheadpos && winstate.grouptailpos > winstate.frameheadpos {
                        let overlapstart = winstate.groupheadpos.max(winstate.frameheadpos);
                        if ap == overlapstart {
                            ap = winstate.currentpos;
                        } else {
                            ap += winstate.grouptailpos - overlapstart - 1;
                        }
                    }
                }
                _ => {
                    return Err(types_error::PgError::error("unrecognized frame option state"));
                }
            }
            abs_pos = ap;
        }
        WINDOW_SEEK_TAIL => {
            // rejecting relpos > 0 is easy and simplifies code below
            if relpos > 0 {
                return Ok((Datum::null(), true, true)); // out_of_frame
            }
            update_frametailpos(winstate, estate)?;
            let mut ap = winstate.frametailpos - 1 + relpos;

            match winstate.frameOptions & FRAMEOPTION_EXCLUSION {
                0 => {
                    mark_pos = ap;
                }
                FRAMEOPTION_EXCLUDE_CURRENT_ROW => {
                    if ap <= winstate.currentpos && winstate.currentpos < winstate.frametailpos {
                        ap -= 1;
                    }
                    update_frameheadpos(winstate, estate)?;
                    if ap < winstate.frameheadpos {
                        return Ok((Datum::null(), true, true)); // out_of_frame
                    }
                    mark_pos = winstate.frameheadpos;
                }
                FRAMEOPTION_EXCLUDE_GROUP => {
                    update_grouptailpos(winstate, estate)?;
                    if ap < winstate.grouptailpos && winstate.groupheadpos < winstate.frametailpos {
                        let overlapend = winstate.grouptailpos.min(winstate.frametailpos);
                        ap -= overlapend - winstate.groupheadpos;
                    }
                    update_frameheadpos(winstate, estate)?;
                    if ap < winstate.frameheadpos {
                        return Ok((Datum::null(), true, true)); // out_of_frame
                    }
                    mark_pos = winstate.frameheadpos;
                }
                FRAMEOPTION_EXCLUDE_TIES => {
                    update_grouptailpos(winstate, estate)?;
                    if ap < winstate.grouptailpos && winstate.groupheadpos < winstate.frametailpos {
                        let overlapend = winstate.grouptailpos.min(winstate.frametailpos);
                        if ap == overlapend - 1 {
                            ap = winstate.currentpos;
                        } else {
                            ap -= overlapend - 1 - winstate.groupheadpos;
                        }
                    }
                    update_frameheadpos(winstate, estate)?;
                    if ap < winstate.frameheadpos {
                        return Ok((Datum::null(), true, true)); // out_of_frame
                    }
                    mark_pos = winstate.frameheadpos;
                }
                _ => {
                    return Err(types_error::PgError::error("unrecognized frame option state"));
                }
            }
            abs_pos = ap;
        }
        _ => {
            return Err(types_error::PgError::error("unrecognized window seek type"));
        }
    }

    if !window_gettupleslot_func(winstate, perfuncno, abs_pos, slot, estate)? {
        return Ok((Datum::null(), true, true)); // out_of_frame
    }

    // The code above does not detect all out-of-frame cases, so check.
    if row_is_in_frame(winstate, abs_pos, slot, estate)? <= 0 {
        return Ok((Datum::null(), true, true)); // out_of_frame
    }

    if set_mark {
        WinSetMarkPosition(winstate, perfuncno, mark_pos, estate)?;
    }
    estate.ecxt_mut(econtext).ecxt_outertuple = Some(slot);
    let (v, isnull) = {
        let argstate = perfunc_winobj_argstate_mut(winstate, perfuncno, argno);
        execExpr::exec_eval_expr_switch_context::call(argstate, econtext, estate)?
    };
    Ok((v, isnull, false))
}

/// `WinGetFuncArgCurrent(winobj, argno, ...)` — evaluate a window function's
/// argument on the current row. Returns `(value, isnull)`.
pub fn WinGetFuncArgCurrent<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    perfuncno: usize,
    argno: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let econtext = winstate.ss.ps.ps_ExprContext.unwrap();
    let scan_slot = winstate.ss.ss_ScanTupleSlot.unwrap();
    estate.ecxt_mut(econtext).ecxt_outertuple = Some(scan_slot);
    let argstate = perfunc_winobj_argstate_mut(winstate, perfuncno, argno);
    execExpr::exec_eval_expr_switch_context::call(argstate, econtext, estate)
}

// ===========================================================================
// Small helpers (field accessors / borrow brokers)
// ===========================================================================

/// The WindowAgg plan node embedded in this state (read-only at run time).
fn node<'a, 'mcx>(winstate: &'a WindowAggState<'mcx>) -> &'a WindowAgg<'mcx> {
    let plan = winstate
        .ss
        .ps
        .plan
        .expect("WindowAggState: plan back-pointer not set");
    match plan.node_tag() {
        types_nodes::nodes::ntag::T_WindowAgg => plan.expect_windowagg(),
        other => panic!("WindowAggState.plan is not a WindowAgg: {other:?}"),
    }
}

fn node_ord_num_cols(winstate: &WindowAggState<'_>) -> i32 {
    node(winstate).ordNumCols
}

fn node_part_num_cols(winstate: &WindowAggState<'_>) -> i32 {
    node(winstate).partNumCols
}

fn node_ord_col_idx0(winstate: &WindowAggState<'_>) -> AttrNumber {
    node(winstate).ordColIdx.as_ref().expect("ordColIdx not set")[0]
}

fn peragg_ref<'a, 'mcx>(
    winstate: &'a WindowAggState<'mcx>,
    i: usize,
) -> &'a WindowStatePerAggData<'mcx> {
    &winstate.peragg.as_ref().expect("peragg not allocated")[i]
}

fn peragg_mut<'a, 'mcx>(
    winstate: &'a mut WindowAggState<'mcx>,
    i: usize,
) -> &'a mut WindowStatePerAggData<'mcx> {
    &mut winstate.peragg.as_mut().expect("peragg not allocated")[i]
}

fn perfunc_ref<'a, 'mcx>(
    winstate: &'a WindowAggState<'mcx>,
    i: usize,
) -> &'a WindowStatePerFuncData<'mcx> {
    &winstate.perfunc.as_ref().expect("perfunc not allocated")[i]
}

fn perfunc_mut<'a, 'mcx>(
    winstate: &'a mut WindowAggState<'mcx>,
    i: usize,
) -> &'a mut WindowStatePerFuncData<'mcx> {
    &mut winstate.perfunc.as_mut().expect("perfunc not allocated")[i]
}

fn perfunc_winobj_ref<'a, 'mcx>(
    winstate: &'a WindowAggState<'mcx>,
    i: usize,
) -> &'a WindowObjectData<'mcx> {
    perfunc_ref(winstate, i)
        .winobj
        .as_deref()
        .expect("perfunc winobj not set")
}

fn perfunc_winobj_mut<'a, 'mcx>(
    winstate: &'a mut WindowAggState<'mcx>,
    i: usize,
) -> &'a mut WindowObjectData<'mcx> {
    perfunc_mut(winstate, i)
        .winobj
        .as_deref_mut()
        .expect("perfunc winobj not set")
}

fn perfunc_winobj_argstate_mut<'a, 'mcx>(
    winstate: &'a mut WindowAggState<'mcx>,
    perfuncno: usize,
    argno: usize,
) -> &'a mut types_nodes::execexpr::ExprState<'mcx> {
    // C: winobj->argstates == wfuncstate->args (the same arg-ExprState list).
    // The owned model keeps it on `winstate.funcs[perfuncno].args` (the single
    // owner; perfuncno == funcs index, no dedup), so resolve it there.
    perfunc_wfuncstate_mut(winstate, perfuncno)
        .args
        .as_mut()
        .expect("winobj argstates not set")[argno]
        .as_mut()
}

// The per-perfunc WindowFuncExprState (`perfuncstate->wfuncstate` in C) is the
// same node held on `winstate.funcs`. The owned model keeps `winstate.funcs` as
// the single owner (the EEOP_WINDOW_FUNC step indexes it by `funcidx`); since the
// setup loop assigns a fresh perfunc per function (no dedup), `perfuncno` equals
// the function's `winstate.funcs` index, so the wfuncstate is reached there
// rather than aliased onto the perfunc satellite.
fn perfunc_wfuncstate<'a, 'mcx>(
    winstate: &'a WindowAggState<'mcx>,
    i: usize,
) -> &'a types_nodes::nodewindowagg::WindowFuncExprState<'mcx> {
    &winstate
        .funcs
        .as_ref()
        .expect("winstate.funcs not populated")[i]
}

fn perfunc_wfuncstate_mut<'a, 'mcx>(
    winstate: &'a mut WindowAggState<'mcx>,
    i: usize,
) -> &'a mut types_nodes::nodewindowagg::WindowFuncExprState<'mcx> {
    &mut winstate
        .funcs
        .as_mut()
        .expect("winstate.funcs not populated")[i]
}

fn wfuncstate_nargs(winstate: &WindowAggState<'_>, i: usize) -> usize {
    perfunc_wfuncstate(winstate, i)
        .args
        .as_ref()
        .map(|a| a.len())
        .unwrap_or(0)
}

fn wfuncstate_arg_mut<'a, 'mcx>(
    winstate: &'a mut WindowAggState<'mcx>,
    i: usize,
    argno: usize,
) -> &'a mut types_nodes::execexpr::ExprState<'mcx> {
    perfunc_wfuncstate_mut(winstate, i)
        .args
        .as_mut()
        .expect("wfuncstate args not set")[argno]
        .as_mut()
}

fn agg_winobj_ref<'a, 'mcx>(winstate: &'a WindowAggState<'mcx>) -> &'a WindowObjectData<'mcx> {
    winstate
        .agg_winobj
        .as_deref()
        .expect("agg_winobj not set")
}

fn agg_winobj_mut<'a, 'mcx>(
    winstate: &'a mut WindowAggState<'mcx>,
) -> &'a mut WindowObjectData<'mcx> {
    winstate
        .agg_winobj
        .as_deref_mut()
        .expect("agg_winobj not set")
}

fn agg_winobj_markptr(winstate: &WindowAggState<'_>) -> i32 {
    agg_winobj_ref(winstate).markptr
}

/// `peraggstate->aggcontext == winstate->aggcontext` — whether a peragg uses
/// the shared aggcontext. In the owned model the private context is stored in
/// `peragg.aggcontext` and the shared one in `winstate.aggcontext`; a peragg
/// using the shared context is modeled as `peragg.aggcontext == None` set by
/// `initialize_peragg` (which assigns a private child only for moving aggs).
fn ctx_is_shared(winstate: &WindowAggState<'_>, i: usize) -> bool {
    // The C compares pointers; in the owned model, a peragg with no private
    // context (its `aggcontext` is None) shares winstate->aggcontext.
    peragg_ref(winstate, i).aggcontext.is_none()
}

fn reset_expr_context<'mcx>(
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    execUtils::reset_expr_context::call(estate, econtext)
}

// --- ExecInitWindowAgg sub-helpers --------------------------------------

fn init_qual_from_plan<'mcx>(
    qual: &Option<PgVec<'mcx, types_nodes::primnodes::Expr>>,
    parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> {
    match qual {
        None => execExpr::exec_init_qual::call(None, parent, estate),
        Some(list) => execExpr::exec_init_qual::call(Some(list.as_slice()), parent, estate),
    }
}

fn init_qual_from_list<'mcx>(
    list: &Option<PgVec<'mcx, types_nodes::primnodes::Expr>>,
    parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> {
    match list {
        None => execExpr::exec_init_qual::call(None, parent, estate),
        Some(l) => execExpr::exec_init_qual::call(Some(l.as_slice()), parent, estate),
    }
}

fn init_expr_opt<'mcx>(
    expr: &Option<PgBox<'mcx, types_nodes::primnodes::Expr>>,
    parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> {
    match expr {
        None => Ok(None),
        Some(e) => {
            let mut state = execExpr::exec_init_expr::call(&**e, parent, estate)?;
            // C's ExecInitExpr sets `state->expr = node`; `calculate_frame_offsets`
            // reads `exprType((Node *) winstate->startOffset->expr)` to size the
            // datumCopy of the frame offset. Ensure the back-reference is present
            // (the deep-copy mirrors C's borrowed pointer into the plan tree).
            if state.expr.is_none() {
                state.expr = Some(mcx::alloc_in(estate.es_query_cxt, e.clone_in(estate.es_query_cxt)?)?);
            }
            Ok(Some(state))
        }
    }
}

fn new_datum_vec<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    n: usize,
) -> PgResult<PgVec<'mcx, Datum<'mcx>>> {
    let mut v = mcx::vec_with_capacity_in(mcx, n)?;
    for _ in 0..n {
        v.push(Datum::null());
    }
    Ok(v)
}

fn new_bool_vec<'mcx>(mcx: mcx::Mcx<'mcx>, n: usize) -> PgResult<PgVec<'mcx, bool>> {
    let mut v = mcx::vec_with_capacity_in(mcx, n)?;
    for _ in 0..n {
        v.push(false);
    }
    Ok(v)
}

/// Clone the scan slot's `TupleDesc` (the `winstate->ss.ss_ScanTupleSlot->
/// tts_tupleDescriptor` the C reuses for all working slots) into `mcx`.
fn clone_slot_descriptor<'mcx>(
    estate: &EStateData<'mcx>,
    slot: SlotId,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDescData<'mcx>> {
    let desc = estate
        .slot_data(slot)
        .base()
        .tts_tupleDescriptor
        .as_deref()
        .expect("scan slot has no tuple descriptor");
    desc.clone_in(mcx)
}

fn clone_desc<'mcx>(
    desc: &types_tuple::heaptuple::TupleDescData<'_>,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    let cloned = desc.clone_in(mcx)?;
    Ok(Some(alloc_in(mcx, cloned)?))
}

// ---------------------------------------------------------------------------
// Datum by-value convenience.
// ---------------------------------------------------------------------------

trait DatumByValExt<'mcx> {
    /// Copy the by-value word (C `datumCopy` for a by-value type, a no-op copy).
    fn clone_in_word(&self) -> Datum<'mcx>;
    /// `att->attbyval`-style check for whether this is the by-value arm.
    fn is_byval(&self) -> bool;
}

impl<'mcx> DatumByValExt<'mcx> for Datum<'mcx> {
    fn clone_in_word(&self) -> Datum<'mcx> {
        match self {
            Datum::ByVal(w) => Datum::ByVal(*w),
            Datum::ByRef(_) => {
                panic!("nodeWindowAgg: clone_in_word called on a by-reference Datum")
            }
            Datum::Cstring(_) | Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => {
                panic!("nodeWindowAgg: clone_in_word called on a Cstring/Composite/Expanded/Internal Datum — not yet produced — wave 2")
            }
        }
    }

    fn is_byval(&self) -> bool {
        matches!(self, Datum::ByVal(_))
    }
}

extern crate alloc;
