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
//! The fmgr `FunctionCallInvoke` / `FunctionCall5Coll` dispatch crosses the
//! value-based `function_call_invoke` seam (fmgr-seams): the transfn /
//! invtransfn / finalfn bodies and the two RANGE in_range comparison sites all
//! gather their arguments into a `NullableDatum` call frame and invoke by
//! `fn_oid` under the relevant collation. Only the expanded-datum primitives
//! (`MakeExpandedObjectReadOnly` of a pass-by-ref value / `datumCopy` reparent /
//! `DeleteExpandedObject` / `pfree` of a Datum) have no owner in this repo yet,
//! so the pass-by-reference transition/result copy-free stanzas still panic
//! loudly (mirror-PG-and-panic). All the surrounding control flow is real
//! in-crate code.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

// object_aclcheck (backend-catalog-aclchk-seams) would be consumed by
// `initialize_peragg`/`ExecInitWindowAgg`'s permission checks, but those sit
// behind the unported pg_aggregate/execGrouping catalog boundary (panic stubs),
// so the dependency is omitted until that boundary lands.
use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_tcop_postgres_seams as tcop_postgres;
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

#[inline]
fn oid_is_valid(o: Oid) -> bool {
    o != INVALID_OID
}

/// Build a `function_call_invoke` argument cell from an in-crate `Datum<'mcx>`
/// value/isnull pair. The fmgr-call frame carries bare-word `types_datum`
/// `NullableDatum`s (the seam's value-based ABI), so the canonical
/// `Datum<'mcx>` is collapsed to its raw word here (mirror execExprInterp's
/// `word_of` gather in `func_step_inputs`).
#[inline]
fn nd(value: &Datum<'_>, isnull: bool) -> types_datum::NullableDatum {
    types_datum::NullableDatum {
        value: types_datum::Datum::from_usize(value.as_usize()),
        isnull,
    }
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
fn initialize_windowaggregate(winstate: &mut WindowAggState<'_>, peraggno: usize) {
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

    let pa = peragg_mut(winstate, peraggno);
    if pa.initValueIsNull {
        //   peraggstate->transValue = peraggstate->initValue;
        pa.transValue = pa.initValue.clone_in_word();
    } else {
        // oldContext = MemoryContextSwitchTo(peraggstate->aggcontext);
        // peraggstate->transValue = datumCopy(peraggstate->initValue,
        //                                     transtypeByVal, transtypeLen);
        // MemoryContextSwitchTo(oldContext);
        if pa.transtypeByVal {
            pa.transValue = pa.initValue.clone_in_word();
        } else {
            panic!(
                "backend-executor-nodeWindowAgg::initialize_windowaggregate: \
                 datumCopy of a pass-by-reference aggregate initial value into the \
                 private aggcontext has no owner (the by-reference datum-arena/copy \
                 primitive is unported); no seam exists yet"
            );
        }
    }
    let pa = peragg_mut(winstate, peraggno);
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
            if pa.transtypeByVal {
                let v = argvals[0].0.clone_in_word();
                let pa = peragg_mut(winstate, peraggno);
                pa.transValue = v;
                pa.transValueIsNull = false;
                pa.transValueCount = 1;
            } else {
                panic!(
                    "backend-executor-nodeWindowAgg::advance_windowaggregate: \
                     datumCopy of a pass-by-reference first input into the aggcontext \
                     has no owner (by-reference datum-arena/copy primitive unported); \
                     no seam exists yet"
                );
            }
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
    let pa = peragg_ref(winstate, peraggno);
    let fn_oid = pa.transfn.fn_oid;
    let collation = perfunc_ref(winstate, perfuncno).winCollation;

    // fcinfo->args[0] = transValue; fcinfo->args[1..] = the evaluated arguments.
    let mut args: alloc::vec::Vec<types_datum::NullableDatum> =
        alloc::vec::Vec::with_capacity(numArguments as usize + 1);
    args.push(nd(&pa.transValue, pa.transValueIsNull));
    for i in 0..numArguments as usize {
        args.push(nd(&argvals[i].0, argvals[i].1));
    }

    let (newVal, isnull) = fmgr::function_call_invoke::call(fn_oid, collation, &args)?;

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
    // the prior transValue. (See comments for ExecAggCopyTransValue.)
    if !peragg_ref(winstate, peraggno).transtypeByVal {
        panic!(
            "backend-executor-nodeWindowAgg::advance_windowaggregate: \
             reparenting a pass-by-reference transition value into the aggcontext \
             (datumCopy / DeleteExpandedObject / pfree of the prior value) has no owner \
             (by-reference datum-arena / expanded-datum primitives unported); no seam exists yet"
        );
    }

    // peraggstate->transValue = newVal; peraggstate->transValueIsNull = fcinfo->isnull;
    let pa = peragg_mut(winstate, peraggno);
    pa.transValue = Datum::from_usize(newVal.as_usize());
    pa.transValueIsNull = isnull;
    let _ = econtext;
    Ok(())
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
        initialize_windowaggregate(winstate, peraggno);
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
    let pa = peragg_ref(winstate, peraggno);
    let fn_oid = pa.invtransfn.fn_oid;
    let collation = perfunc_ref(winstate, perfuncno).winCollation;

    let mut args: alloc::vec::Vec<types_datum::NullableDatum> =
        alloc::vec::Vec::with_capacity(numArguments as usize + 1);
    args.push(nd(&pa.transValue, pa.transValueIsNull));
    for i in 0..numArguments as usize {
        args.push(nd(&argvals[i].0, argvals[i].1));
    }

    let (newVal, isnull) = fmgr::function_call_invoke::call(fn_oid, collation, &args)?;

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
    if !peragg_ref(winstate, peraggno).transtypeByVal {
        panic!(
            "backend-executor-nodeWindowAgg::advance_windowaggregate_base: \
             reparenting a pass-by-reference inverse-transition value into the aggcontext \
             (datumCopy / DeleteExpandedObject / pfree of the prior value) has no owner \
             (by-reference datum-arena / expanded-datum primitives unported); no seam exists yet"
        );
    }

    // peraggstate->transValue = newVal; peraggstate->transValueIsNull = fcinfo->isnull;
    let pa = peragg_mut(winstate, peraggno);
    pa.transValue = Datum::from_usize(newVal.as_usize());
    pa.transValueIsNull = isnull;
    let _ = econtext;
    Ok(true)
}

/// `finalize_windowaggregate` — parallel to `finalize_aggregate` in nodeAgg.c.
fn finalize_windowaggregate<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    peraggno: usize,
) -> PgResult<(Datum<'mcx>, bool)> {
    // oldContext = MemoryContextSwitchTo(ps_ExprContext->ecxt_per_tuple_memory);

    // Apply the agg's finalfn if one is provided, else return transValue.
    if oid_is_valid(peragg_ref(winstate, peraggno).finalfn_oid) {
        // perfuncstate = &winstate->perfunc[peraggstate->wfuncno].
        let perfuncno = peragg_ref(winstate, peraggno).wfuncno as usize;
        let pa = peragg_ref(winstate, peraggno);
        let numFinalArgs = pa.numFinalArgs;
        let fn_oid = pa.finalfn.fn_oid;
        let fn_strict = pa.finalfn.fn_strict;
        let collation = perfunc_ref(winstate, perfuncno).winCollation;

        // InitFunctionCallInfoData(fcinfo, &peraggstate->finalfn, numFinalArgs,
        //                          perfuncstate->winCollation, ...);
        // fcinfo->args[0].value = MakeExpandedObjectReadOnly(transValue,
        //                            transValueIsNull, transtypeLen);
        // fcinfo->args[0].isnull = transValueIsNull;
        // anynull = transValueIsNull;
        //
        // For a by-value transition type MakeExpandedObjectReadOnly is a no-op;
        // a pass-by-reference transValue would need the (unported) expanded-datum
        // read-only wrapper as finalfn arg0.
        let pa = peragg_ref(winstate, peraggno);
        if !pa.transtypeByVal {
            panic!(
                "backend-executor-nodeWindowAgg::finalize_windowaggregate: \
                 MakeExpandedObjectReadOnly of a pass-by-reference transition value as \
                 finalfn arg0 has no owner (expanded-datum primitive unported); no seam exists yet"
            );
        }
        let mut anynull = pa.transValueIsNull;
        let mut args: alloc::vec::Vec<types_datum::NullableDatum> =
            alloc::vec::Vec::with_capacity(numFinalArgs as usize);
        args.push(nd(&pa.transValue, pa.transValueIsNull));

        // Fill any remaining argument positions with nulls.
        //   for (i = 1; i < numFinalArgs; i++) { args[i] = NULL; anynull = true; }
        for _ in 1..numFinalArgs {
            args.push(types_datum::NullableDatum::null());
            anynull = true;
        }

        if fn_strict && anynull {
            // Don't call a strict function with NULL inputs.
            //   *result = (Datum) 0; *isnull = true;
            Ok((Datum::null(), true))
        } else {
            //   winstate->curaggcontext = peraggstate->aggcontext;
            //   res = FunctionCallInvoke(fcinfo);
            //   winstate->curaggcontext = NULL;
            //   *isnull = fcinfo->isnull;
            //   *result = MakeExpandedObjectReadOnly(res, fcinfo->isnull, resulttypeLen);
            let (res, isnull) = fmgr::function_call_invoke::call(fn_oid, collation, &args)?;
            if !peragg_ref(winstate, peraggno).resulttypeByVal {
                panic!(
                    "backend-executor-nodeWindowAgg::finalize_windowaggregate: \
                     MakeExpandedObjectReadOnly of a pass-by-reference finalfn result has \
                     no owner (expanded-datum primitive unported); no seam exists yet"
                );
            }
            // For a by-value result type MakeExpandedObjectReadOnly is a no-op.
            Ok((Datum::from_usize(res.as_usize()), isnull))
        }
    } else {
        // *result = MakeExpandedObjectReadOnly(transValue, transValueIsNull, transtypeLen);
        // *isnull = transValueIsNull;
        let pa = peragg_ref(winstate, peraggno);
        if pa.transtypeByVal {
            // For a by-value type MakeExpandedObjectReadOnly is a no-op.
            Ok((pa.transValue.clone_in_word(), pa.transValueIsNull))
        } else {
            panic!(
                "backend-executor-nodeWindowAgg::finalize_windowaggregate: \
                 MakeExpandedObjectReadOnly of a pass-by-reference transition value has \
                 no owner (expanded-datum primitive unported); no seam exists yet"
            );
        }
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
            let value = peragg_ref(winstate, i).resultValue.clone_in_word();
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
            initialize_windowaggregate(winstate, i);
        } else if !peragg_ref(winstate, i).resultValueIsNull {
            if !peragg_ref(winstate, i).resulttypeByVal {
                // pfree(DatumGetPointer(peraggstate->resultValue));
                // (no owner for pfree of a Datum; but by-ref result requires the
                // unported finalfn anyway — this branch is unreachable in the
                // by-value-only paths we can execute.)
                panic!(
                    "backend-executor-nodeWindowAgg::eval_windowaggregates: \
                     pfree of a pass-by-reference saved result has no owner; no seam yet"
                );
            }
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
    for i in 0..numaggs {
        let wfuncno = peragg_ref(winstate, i).wfuncno as usize;
        let (result, isnull) = finalize_windowaggregate(winstate, i)?;
        {
            let ec = estate.ecxt_mut(econtext);
            ec.ecxt_aggvalues[wfuncno] = result.clone_in_word();
            ec.ecxt_aggnulls[wfuncno] = isnull;
        }

        // save the result in case next row shares the same frame.
        if !peragg_ref(winstate, i).resulttypeByVal && !isnull {
            // peraggstate->resultValue = datumCopy(*result, byval, len);
            panic!(
                "backend-executor-nodeWindowAgg::eval_windowaggregates: \
                 datumCopy of a pass-by-reference finalized result into the aggcontext \
                 has no owner; no seam yet"
            );
        } else {
            peragg_mut(winstate, i).resultValue = result;
        }
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
    let _ = (winstate, perfuncno, estate);
    // oldContext = MemoryContextSwitchTo(ps_ExprContext->ecxt_per_tuple_memory);
    // InitFunctionCallInfoData(*fcinfo, &flinfo, numArguments, winCollation,
    //                          (Node *) winobj, NULL);
    // for (argno..) fcinfo->args[argno].isnull = true;
    // winstate->curaggcontext = NULL;
    // *result = FunctionCallInvoke(fcinfo);
    // *isnull = fcinfo->isnull;
    // ... copy if byref and numfuncs > 1 ...
    //
    // The window-function dispatch (FunctionCallInvoke), which reads its real
    // arguments back through the WindowObject API, has no fmgr-invoke seam.
    panic!(
        "backend-executor-nodeWindowAgg::eval_windowfunction: \
         the window function invocation (FunctionCallInvoke) is owned by the \
         not-yet-ported fmgr/execExpr units; no windowfunc-invoke seam exists yet"
    );
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
                    let args = [
                        nd(&headval, false),
                        nd(&currval, false),
                        nd(&winstate.startOffsetValue, false),
                        nd(&Datum::from_bool(sub), false),
                        nd(&Datum::from_bool(less), false),
                    ];
                    let (res, _isnull) =
                        fmgr::function_call_invoke::call(fn_oid, collation, &args)?;
                    if Datum::from_usize(res.as_usize()).as_bool() {
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
                    let args = [
                        nd(&tailval, false),
                        nd(&currval, false),
                        nd(&winstate.endOffsetValue, false),
                        nd(&Datum::from_bool(sub), false),
                        nd(&Datum::from_bool(less), false),
                    ];
                    let (res, _isnull) =
                        fmgr::function_call_invoke::call(fn_oid, collation, &args)?;
                    if !Datum::from_usize(res.as_usize()).as_bool() {
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
        // copy value into query-lifespan context (by-value: just the word)
        if value.is_byval() {
            winstate.startOffsetValue = value.clone_in_word();
        } else {
            panic!(
                "backend-executor-nodeWindowAgg::calculate_frame_offsets: \
                 datumCopy of a pass-by-reference frame starting offset into the \
                 query-lifespan context has no owner; no seam yet"
            );
        }
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
        if value.is_byval() {
            winstate.endOffsetValue = value.clone_in_word();
        } else {
            panic!(
                "backend-executor-nodeWindowAgg::calculate_frame_offsets: \
                 datumCopy of a pass-by-reference frame ending offset into the \
                 query-lifespan context has no owner; no seam yet"
            );
        }
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

    let wnode: &'mcx WindowAgg<'mcx> = match node {
        types_nodes::nodes::Node::WindowAgg(w) => w,
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

    // Set up data for comparing tuples.
    if wnode.partNumCols > 0 {
        // execTuplesMatchPrepare(scanDesc, partNumCols, partColIdx,
        //                        partOperators, partCollations, &ps)
        //
        // execTuplesMatchPrepare lives in execGrouping.c, which has no seam
        // here; the equality-ExprState compile is genuinely cross-crate.
        panic!(
            "backend-executor-nodeWindowAgg::ExecInitWindowAgg: \
             execTuplesMatchPrepare (partition equality) is owned by the \
             not-yet-ported executor/execGrouping unit; no seam exists yet"
        );
    }
    if wnode.ordNumCols > 0 {
        panic!(
            "backend-executor-nodeWindowAgg::ExecInitWindowAgg: \
             execTuplesMatchPrepare (ordering equality) is owned by the \
             not-yet-ported executor/execGrouping unit; no seam exists yet"
        );
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
    // The unique-function-discovery loop over winstate->funcs and per-aggregate
    // setup (initialize_peragg, object_aclcheck, fmgr_info, makeNode(winobj),
    // ...) reads the WindowFuncExprState list the planner/ExecInitExpr produced.
    // Those WindowFuncExprState nodes (with compiled args/aggfilter) are built
    // by execExpr.c's expression-init walk over the targetlist, which has no
    // window-function-state-building seam in this repo yet; winstate->funcs is
    // therefore empty here and the per-func setup loop cannot run.
    //
    // We mirror the C structure: had funcs been populated, the loop below would
    // run; with the unported list it is empty. (numfuncs/numaggs were set by the
    // same unported ExecInitExpr walk, so both are 0 here.)
    debug_assert!(winstate.funcs.is_none());

    // Update numfuncs, numaggs to match number of unique functions found.
    // (With an empty funcs list these stay as the initialized 0.)

    // Set up WindowObject for aggregates, if needed (numaggs is 0 here).

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
///
/// The whole body reads `pg_aggregate` via `SearchSysCache1(AGGFNOID, ...)`,
/// which has no seam in this repo, and resolves transition expressions via
/// parser/optimizer helpers (`resolve_aggregate_transtype`,
/// `build_aggregate_transfn_expr`, `contain_volatile_functions`,
/// `contain_subplans`, `IsBinaryCoercible`, ...) that likewise have no seams.
/// This is the genuine catalog/parser boundary of WindowAgg init.
#[allow(dead_code)]
fn initialize_peragg<'mcx>(
    winstate: &mut WindowAggState<'mcx>,
    peraggno: usize,
    winfnoid: Oid,
) -> PgResult<()> {
    let _ = (winstate, peraggno, winfnoid);
    panic!(
        "backend-executor-nodeWindowAgg::initialize_peragg: \
         the pg_aggregate read (SearchSysCache1(AGGFNOID, ...)) and the aggregate \
         transition-expression resolution (resolve_aggregate_transtype / \
         build_aggregate_transfn_expr / contain_volatile_functions / contain_subplans / \
         IsBinaryCoercible) are owned by the not-yet-ported syscache and \
         parser/optimizer units; no seam exists yet"
    );
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
    match winstate
        .ss
        .ps
        .plan
        .expect("WindowAggState: plan back-pointer not set")
    {
        types_nodes::nodes::Node::WindowAgg(w) => w,
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
    perfunc_mut(winstate, perfuncno)
        .winobj
        .as_deref_mut()
        .expect("perfunc winobj not set")
        .argstates
        .as_mut()
        .expect("winobj argstates not set")[argno]
        .as_mut()
}

fn perfunc_wfuncstate<'a, 'mcx>(
    winstate: &'a WindowAggState<'mcx>,
    i: usize,
) -> &'a types_nodes::nodewindowagg::WindowFuncExprState<'mcx> {
    perfunc_ref(winstate, i)
        .wfuncstate
        .as_deref()
        .expect("perfunc wfuncstate not set")
}

fn perfunc_wfuncstate_mut<'a, 'mcx>(
    winstate: &'a mut WindowAggState<'mcx>,
    i: usize,
) -> &'a mut types_nodes::nodewindowagg::WindowFuncExprState<'mcx> {
    perfunc_mut(winstate, i)
        .wfuncstate
        .as_deref_mut()
        .expect("perfunc wfuncstate not set")
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
        Some(e) => Ok(Some(execExpr::exec_init_expr::call(&**e, parent, estate)?)),
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
