//! Aggregate-transition helpers (`execExprInterp.c`): the inlined transition
//! machinery the AGG_* opcodes call (`ExecInterpExpr` dispatches to these),
//! plus the presorted-distinct filters. These operate on the nodeAgg-owned
//! `AggState` / per-trans / per-group state (boundary slice in
//! [`types_nodes::nodeagg`]).
//!
//! `op` is the step index into `state.steps`; the helpers also receive the
//! `AggState` they transition. Returns `PgResult<()>` (transition functions
//! and datum copies can `ereport`).
//!
//! ## Model boundary: the transfn call frame
//!
//! The plain-transition fast paths (`ExecAggPlainTransByVal` / `ByRef`) and the
//! strict-init path (`ExecAggInitGroup`) read and write
//! `pertrans->transfn_fcinfo->args[]` and invoke the transition function via
//! `FunctionCallInvoke(fcinfo)`. The transfn call frame is the nodeAgg-parked
//! [`types_nodes::fmgr::FunctionCallInfoBaseData`], which now carries the real
//! `args: Vec<NullableDatum>` / `isnull` / `flinfo` payload (#296 widened the
//! frame). The OID-keyed shared fmgr dispatch
//! [`backend_utils_fmgr_fmgr_seams::function_call_invoke`] runs the function by
//! re-resolving `pertrans->transfn.fn_oid` under `pertrans->aggCollation` over
//! the gathered `args`, exactly as the merged eval_scalar.rs `EEOP_FUNCEXPR`
//! path already does. So the by-value transfn call-frame reads/writes and the
//! invoke are implemented faithfully here (Lane 1).
//!
//! What still panics: the **by-reference** transition copy
//! (`ExecAggCopyTransValue` / `datumCopy` into the aggcontext — Lane 4, gated on
//! datum.c's by-ref `Datum` round-trip + expanded-object helpers), and the
//! ordered/presorted paths that reach `op->d.agg_trans.pertrans` through
//! `state->parent` (the AggState-into-interp keystone — Lane 3). Every other
//! line of these handlers — the strictness / no-trans-value branching, the
//! memory-context switches, the pass-by-ref reparenting decision, the
//! presorted-distinct comparisons, and the ordered-aggregate tuplesort feed —
//! is the interpreter's own logic and is implemented faithfully here.

use backend_executor_execTuples_seams::{
    exec_clear_tuple, exec_copy_slot, store_virtual_values,
};
use backend_utils_fmgr_fmgr_seams::function_call2_coll_datum;
use backend_utils_sort_tuplesort_seams::{tuplesort_putdatum, tuplesort_puttupleslot};

// The bare-word newtype: the transition-value form the fmgr/tuplesort seams and
// the nodeAgg-parked transfn frame operate on.
use types_datum::Datum;
// The canonical unified value type (Datum-unification keystone) — what the
// keystone-owned `AggStatePerTransData.lastdatum` / `AggStatePerGroupData
// .trans_value` carry. Transition values are scalar/pointer words under the
// transitional model, so they cross into its by-value arm.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_error::PgResult;

/// Recover the bare scalar word from a stored canonical by-value datum (the
/// transitional bridge: the fmgr/tuplesort seams take a word).
#[inline]
fn word_of(v: &DatumV<'_>) -> Datum {
    Datum::from_usize(v.as_usize())
}
use types_nodes::execexpr::{ExprEvalStepData, ExprState};
use backend_executor_nodeAgg::{
    AggStateData as AggState, AggStatePerGroupData, AggStatePerTransData,
};
use types_nodes::EStateData;

// ---------------------------------------------------------------------------
// transfn call frame (nodeAgg-parked fmgr frame; Lane 1 — by-value invoke)
//
// C: `FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;`, then
// `fcinfo->args[i].value` / `fcinfo->args[i].isnull`, `fcinfo->isnull`, and
// `FunctionCallInvoke(fcinfo)`. The widened `FunctionCallInfoBaseData` carries
// the real `args: Vec<NullableDatum>` / `isnull`, and the transfn OID lives on
// `pertrans->transfn.fn_oid`, so the call dispatches through the OID-keyed
// shared `function_call_invoke` seam (#296), as eval_scalar's EEOP_FUNCEXPR
// path does.
// ---------------------------------------------------------------------------

/// `pertrans->transfn_fcinfo->args[n].value` — read a transfn argument cell.
fn transfn_arg_value(pertrans: &AggStatePerTransData<'_>, n: usize) -> Datum {
    let fcinfo = pertrans
        .transfn_fcinfo
        .as_ref()
        .expect("eval_agg: pertrans->transfn_fcinfo not initialized by ExecInitAgg");
    fcinfo.args[n].value
}

/// `pertrans->transfn_fcinfo->args[n].isnull` — read a transfn argument's null.
/// Paired with [`transfn_arg_value`]; exercised by the unit test. The presorted
/// multi-column DISTINCT path now reads its inputs by-reference-faithfully from
/// the compiled `input_cells` rather than the bare-word fcinfo args, so the
/// non-test build no longer calls this accessor directly.
#[cfg_attr(not(test), allow(dead_code))]
fn transfn_arg_isnull(pertrans: &AggStatePerTransData<'_>, n: usize) -> bool {
    let fcinfo = pertrans
        .transfn_fcinfo
        .as_ref()
        .expect("eval_agg: pertrans->transfn_fcinfo not initialized by ExecInitAgg");
    fcinfo.args[n].isnull
}

/// `fcinfo->args[0].value = transValue; fcinfo->args[0].isnull = isnull;
/// fcinfo->isnull = false; newVal = FunctionCallInvoke(fcinfo);` — set the
/// running transition value into arg 0 and invoke the transition function.
/// Returns `(newVal, fcinfo->isnull)`.
///
/// Lane 1 (by-value): the transfn is re-resolved by `pertrans->transfn.fn_oid`
/// and run under `pertrans->aggCollation` over the gathered `transfn_fcinfo`
/// args via the shared OID-keyed `function_call_invoke` dispatch (the resolved
/// `FmgrInfo` itself cannot cross the seam, mirroring eval_scalar's EEOP_FUNCEXPR
/// path). The strict-null short-circuit for strict transfns is applied by the
/// AGG_PLAIN_TRANS opcode dispatch upstream (the `*_strict` variants), exactly
/// as in C — `ExecAggPlainTransByVal`/`ByRef` themselves never null-check, so
/// neither does this helper.
fn invoke_transfn(
    pertrans: &mut AggStatePerTransData<'_>,
    trans_value: Datum,
    trans_value_is_null: bool,
) -> PgResult<(Datum, bool)> {
    // The transfn OID + collation the call dispatches through.
    let fn_oid = pertrans.transfn.fn_oid;
    let collation = pertrans.agg_collation;

    let fcinfo = pertrans
        .transfn_fcinfo
        .as_mut()
        .expect("eval_agg: pertrans->transfn_fcinfo not initialized by ExecInitAgg");

    // fcinfo->args[0].value = pergroup->transValue;
    // fcinfo->args[0].isnull = pergroup->transValueIsNull;
    fcinfo.args[0].value = trans_value;
    fcinfo.args[0].isnull = trans_value_is_null;
    // fcinfo->isnull = false;  /* just in case transfn doesn't set it */
    fcinfo.isnull = false;

    // newVal = FunctionCallInvoke(fcinfo);  (re-resolved by OID, run under the
    // aggregate collation over the args[] frame just populated). A transfn
    // ereport propagates, as in C's advance_transition_function.
    backend_utils_fmgr_fmgr_seams::function_call_invoke::call(fn_oid, collation, &fcinfo.args)
}

// ---------------------------------------------------------------------------
// pass-by-ref transition-value placement (datum.c / expandeddatum.c)
//
// C: `ExecAggCopyTransValue` deep-copies the new transvalue into the
// aggcontext via `datumCopy` (unless it is a R/W expanded object already
// childed under the aggcontext), and frees the old via `DeleteExpandedObject` /
// `pfree`. In the trimmed model `transValue` is a bare `Datum` word: a
// by-reference datum is a pointer that the shared vocabulary cannot round-trip
// through `datumCopy` (which works on the byte-model `Datum`; see the
// `justs.rs` note), and the expanded-object helpers
// (`DatumIsReadWriteExpandedObject` / `DatumGetEOHP` / `DeleteExpandedObject`)
// are not in the shared vocabulary. So the copy/free body is the
// scalar(datum.c)+expandeddatum owner's; mirrored and panicked here.
// ---------------------------------------------------------------------------

/// `ExecAggCopyTransValue(AggState *aggstate, AggStatePerTrans pertrans,
/// Datum newValue, bool newValueIsNull, Datum oldValue, bool oldValueIsNull)` —
/// copy a new transition value into the aggregate context and free the old.
///
/// `Assert(newValue != oldValue)`; if the new value is NULL it is normalized to
/// `(Datum) 0` so callers can compare new/old without re-checking nullness.
/// The non-trivial body (datumCopy of a by-ref datum, expanded-object detection
/// and reparenting/free) is the datum.c / expandeddatum owner's and is not in
/// the shared vocabulary, so it panics with that rationale.
pub fn ExecAggCopyTransValue<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
    new_value: types_datum::Datum,
    new_value_is_null: bool,
    old_value: types_datum::Datum,
    old_value_is_null: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<types_datum::Datum> {
    let _ = (&aggstate, pertrans, &estate);
    // Assert(newValue != oldValue);
    debug_assert_ne!(new_value, old_value);

    if new_value_is_null {
        // newValue = (Datum) 0;  (then any non-null old value is freed below)
        let _ = (old_value, old_value_is_null);
        // The old-value free for a by-ref/expanded datum is the owner's body
        // (see below); reaching here still needs that free.
        return Ok(copy_or_free_trans_value(
            aggstate,
            pertrans,
            Datum::null(),
            true,
            old_value,
            old_value_is_null,
            estate,
        ));
    }

    Ok(copy_or_free_trans_value(
        aggstate,
        pertrans,
        new_value,
        false,
        old_value,
        old_value_is_null,
        estate,
    ))
}

/// The pass-by-ref copy-into-aggcontext + free-old body of
/// `ExecAggCopyTransValue`: switch to the aggcontext per-tuple memory,
/// `datumCopy` the (non-NULL) new value unless it is already a R/W expanded
/// object childed under the current context, then `DeleteExpandedObject` /
/// `pfree` the old value if non-NULL. Owned by the datum.c / expandeddatum
/// unit (by-ref `Datum` round-trip + expanded-object helpers absent from the
/// shared vocabulary).
fn copy_or_free_trans_value<'mcx>(
    _aggstate: &mut AggState<'mcx>,
    _pertrans: usize,
    _new_value: Datum,
    _new_value_is_null: bool,
    _old_value: Datum,
    _old_value_is_null: bool,
    _estate: &mut EStateData<'mcx>,
) -> Datum {
    panic!(
        "backend-executor-execExprInterp::eval_agg: ExecAggCopyTransValue's by-reference copy \
         (datumCopy of a by-ref Datum, DatumIsReadWriteExpandedObject / DeleteExpandedObject / \
         pfree) is owned by the datum.c / expandeddatum unit; the trimmed model cannot \
         round-trip a by-ref Datum and has no expanded-object helpers yet"
    );
}

/// `ExecAggInitGroup(AggState *aggstate, AggStatePerTrans pertrans,
/// AggStatePerGroup pergroup, ExprContext *aggcontext)` — initialize a group's
/// transition value from the first input row (strict-init transition).
///
/// C copies `fcinfo->args[1].value` into the aggcontext's per-tuple memory via
/// `datumCopy` and marks the group initialized. The argument read is the
/// trimmed transfn call frame, and the by-ref copy into a bare `Datum` is the
/// datum.c owner's; both panic with that rationale.
pub fn ExecAggInitGroup<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
    pergroup: &mut AggStatePerGroupData,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let pt = &aggstate.pertrans.as_ref().expect("ExecAggInitGroup: pertrans not ready")[pertrans];

    // oldContext = MemoryContextSwitchTo(aggcontext->ecxt_per_tuple_memory);
    // pergroup->transValue = datumCopy(fcinfo->args[1].value,
    //                                  pertrans->transtypeByVal,
    //                                  pertrans->transtypeLen);
    let _ = (pt.transtype_by_val, pt.transtype_len, &estate);
    let arg1 = transfn_arg_value(pt, 1);
    pergroup.trans_value =
        DatumV::ByVal(group_init_datum_copy(aggstate, pertrans, arg1, estate).as_usize());

    // pergroup->transValueIsNull = false;
    pergroup.trans_value_is_null = false;
    // pergroup->noTransValue = false;
    pergroup.no_trans_value = false;
    // MemoryContextSwitchTo(oldContext);
    Ok(())
}

/// The `datumCopy(value, transtypeByVal, transtypeLen)` into the aggcontext's
/// per-tuple memory performed by `ExecAggInitGroup`. By-value copies are the
/// verbatim word; the by-reference deep copy of a bare `Datum` is the datum.c
/// owner's body (the trimmed model cannot round-trip a by-ref `Datum`).
fn group_init_datum_copy<'mcx>(
    _aggstate: &mut AggState<'mcx>,
    _pertrans: usize,
    _value: Datum,
    _estate: &mut EStateData<'mcx>,
) -> Datum {
    panic!(
        "backend-executor-execExprInterp::eval_agg: ExecAggInitGroup's datumCopy of the first \
         input value into the aggcontext is owned by the datum.c unit; the trimmed model cannot \
         round-trip a by-reference Datum yet"
    );
}

/// `ExecEvalPreOrderedDistinctSingle(AggState *aggstate,
/// AggStatePerTrans pertrans)` — single-column DISTINCT filter over presorted
/// input; returns whether the current value is distinct from the last.
pub fn ExecEvalPreOrderedDistinctSingle<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // The comparison and the datumCopy land in the per-query context (the
    // value-lifetime-correct context for the non-hashed single-group gate; see
    // `distinct_last_datum_copy`).
    let mcx = estate.es_query_cxt;

    // Read the comparison inputs/config off the pertrans first. `value`/
    // `lastdatum` are the CANONICAL value type — a by-reference DISTINCT key
    // (text/name/numeric) is carried faithfully in its `ByRef` arm, NOT collapsed
    // into a bare scalar word (which panics "scalar accessor called on a
    // by-reference value").
    let (value, isnull, haslast, lastisnull, lastdatum, agg_collation, equalfn_one_oid,
         inputtype_by_val, inputtype_len) = {
        let pt =
            &aggstate.pertrans.as_ref().expect("ExecEvalPreOrderedDistinctSingle: pertrans")[pertrans];
        // Datum value = pertrans->transfn_fcinfo->args[1].value;
        // bool  isnull = pertrans->transfn_fcinfo->args[1].isnull;
        // (Owned model: the interpreter parked the canonical input in
        //  pertrans->distinct_value rather than the bare-word fcinfo args[].)
        (
            pt.distinct_value.clone_in(mcx)?,
            pt.distinct_value_isnull,
            pt.haslast,
            pt.lastisnull,
            pt.lastdatum.clone_in(mcx)?,
            pt.agg_collation,
            pt.equalfn_one.fn_oid,
            pt.inputtype_by_val,
            pt.inputtype_len,
        )
    };

    // if (!pertrans->haslast ||
    //     pertrans->lastisnull != isnull ||
    //     (!isnull && !DatumGetBool(FunctionCall2Coll(&pertrans->equalfnOne,
    //                                                  pertrans->aggCollation,
    //                                                  pertrans->lastdatum, value))))
    let equal = if !haslast || lastisnull != isnull {
        false
    } else if isnull {
        // both null: not distinct (the third disjunct is guarded by !isnull)
        true
    } else {
        // DatumGetBool(FunctionCall2Coll(&equalfnOne, aggCollation, lastdatum, value))
        // Through the by-reference-capable canonical fmgr lane: a by-ref key
        // crosses as its referent bytes (the equality function reads VARDATA_ANY).
        let r = function_call2_coll_datum::call(
            mcx,
            equalfn_one_oid,
            agg_collation,
            lastdatum.clone_in(mcx)?,
            value.clone_in(mcx)?,
        )?;
        r.as_bool()
    };

    if !equal {
        // if (pertrans->haslast && !pertrans->inputtypeByVal && !pertrans->lastisnull)
        //     pfree(DatumGetPointer(pertrans->lastdatum));
        //
        // No explicit free in the owned model: a by-reference `lastdatum` owns its
        // bytes in `mcx` (the per-query context), reclaimed when that context is
        // reset/deleted — exactly the lifetime C's curaggcontext pfree targets.
        let _ = (haslast, inputtype_by_val, lastisnull);

        let new_lastdatum: DatumV = if !isnull {
            // oldContext = MemoryContextSwitchTo(aggstate->curaggcontext->ecxt_per_tuple_memory);
            // pertrans->lastdatum = datumCopy(value, inputtypeByVal, inputtypeLen);
            // MemoryContextSwitchTo(oldContext);
            backend_utils_adt_datum_seams::datum_copy_v::call(
                mcx,
                &value,
                inputtype_by_val,
                inputtype_len as i32,
            )?
        } else {
            // pertrans->lastdatum = (Datum) 0;
            DatumV::null()
        };

        let pt = &mut aggstate
            .pertrans
            .as_mut()
            .expect("ExecEvalPreOrderedDistinctSingle: pertrans")[pertrans];
        pt.haslast = true;
        pt.lastdatum = new_lastdatum;
        pt.lastisnull = isnull;
        return Ok(true);
    }

    Ok(false)
}


/// `ExecEvalPreOrderedDistinctMulti(AggState *aggstate,
/// AggStatePerTrans pertrans)` — multi-column DISTINCT filter over presorted
/// input.
pub fn ExecEvalPreOrderedDistinctMulti<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
    // The per-column input Datums/nulls the interpreter read out of the
    // compiled `input_cells` (one per `numTransInputs`). Each carries its input
    // by-reference-faithfully (a `typbyval = false` DISTINCT column on its
    // `ByRef` arm), the owned-model replacement for C recursing each input into
    // `&pertrans->transfn_fcinfo->args[i + 1]` and then copying
    // `args[i + 1].value` into `sortslot->tts_values[i]`.
    input_values: &[DatumV<'mcx>],
    input_nulls: &[bool],
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // ExprContext *tmpcontext = aggstate->tmpcontext;
    // bool isdistinct = false;

    // Gather the per-input (value, isnull) and the slots/expr involved.
    let (num_trans_inputs, num_inputs, sortslot, uniqslot, haslast) = {
        let pt = &aggstate
            .pertrans
            .as_ref()
            .expect("ExecEvalPreOrderedDistinctMulti: pertrans")[pertrans];
        (
            pt.num_trans_inputs,
            pt.num_inputs,
            pt.sortslot.expect("ExecEvalPreOrderedDistinctMulti: sortslot"),
            pt.uniqslot.expect("ExecEvalPreOrderedDistinctMulti: uniqslot"),
            pt.haslast,
        )
    };

    // for (int i = 0; i < pertrans->numTransInputs; i++) {
    //     pertrans->sortslot->tts_values[i] = pertrans->transfn_fcinfo->args[i + 1].value;
    //     pertrans->sortslot->tts_isnull[i] = pertrans->transfn_fcinfo->args[i + 1].isnull;
    // }
    //
    // The owned model takes the per-column inputs straight from the interpreter's
    // compiled `input_cells` (the by-reference-faithful `DatumV` the input
    // sub-expressions were evaluated into) rather than the bare-word
    // transfn_fcinfo->args[]: a by-ref DISTINCT column (text/bytea/numeric/array)
    // must keep its `ByRef` arm so the comparison-tuple formation
    // (heap_compute_data_size) reads its referent varlena bytes, not a collapsed
    // by-value word (#296 by-ref Datum class).
    assert_eq!(
        input_values.len(),
        num_trans_inputs as usize,
        "ExecEvalPreOrderedDistinctMulti: input_values count {} != numTransInputs {}",
        input_values.len(),
        num_trans_inputs,
    );
    assert_eq!(input_values.len(), input_nulls.len());
    let mut values_v: Vec<DatumV> = Vec::with_capacity(num_trans_inputs as usize);
    let mut isnull: Vec<bool> = Vec::with_capacity(num_trans_inputs as usize);
    for i in 0..num_trans_inputs as usize {
        // Carry each column on its native arm (ByVal AND ByRef AND null): a null
        // column's payload is unused by store_virtual_values (tts_isnull[i]
        // gates it), matching C which leaves tts_values[i] indeterminate there.
        values_v.push(input_values[i].clone_in(estate.es_query_cxt)?);
        isnull.push(input_nulls[i]);
    }

    // ExecClearTuple(pertrans->sortslot);
    // pertrans->sortslot->tts_nvalid = pertrans->numInputs;
    // ExecStoreVirtualTuple(pertrans->sortslot);
    //
    // The owned model fills the slot's virtual-tuple payload through the
    // execTuples owner: store_virtual_values writes the column values/nulls and
    // performs ExecStoreVirtualTuple (which sets tts_nvalid). C only ever sets
    // the first numTransInputs columns; nvalid is numInputs (== numTransInputs
    // for the multi-DISTINCT case, since there are no ORDER BY-only columns).
    debug_assert_eq!(num_inputs, num_trans_inputs);
    store_virtual_values::call(estate, sortslot, &values_v, &isnull)?;

    // save_outer = tmpcontext->ecxt_outertuple;
    // save_inner = tmpcontext->ecxt_innertuple;
    // tmpcontext->ecxt_outertuple = pertrans->sortslot;
    // tmpcontext->ecxt_innertuple = pertrans->uniqslot;
    let tmpcontext_id = aggstate_tmpcontext(aggstate, estate);
    let (save_outer, save_inner) = {
        let ec = estate.ecxt_mut(tmpcontext_id);
        let so = ec.ecxt_outertuple;
        let si = ec.ecxt_innertuple;
        ec.ecxt_outertuple = Some(sortslot);
        ec.ecxt_innertuple = Some(uniqslot);
        (so, si)
    };

    // if (!pertrans->haslast || !ExecQual(pertrans->equalfnMulti, tmpcontext))
    let equal = if haslast {
        // The compiled multi-column DISTINCT comparator ExprState is parked in
        // `pertrans->equalfn_multi`; ExecQual evaluates it over the tmpcontext
        // (whose ecxt_outertuple = sortslot / ecxt_innertuple = uniqslot were
        // just set). Borrow it `&mut` (the seam reads/advances the ExprState) —
        // disjoint from `&mut estate`, so the two mutable borrows coexist (the
        // exact split the nodeAgg `equalfn_multi_qual` non-presorted drain uses).
        let equalfn_multi = aggstate
            .pertrans
            .as_mut()
            .expect("ExecEvalPreOrderedDistinctMulti: pertrans")[pertrans]
            .equalfn_multi
            .as_mut()
            .expect("ExecEvalPreOrderedDistinctMulti: equalfnMulti");
        backend_executor_execExpr_seams::exec_qual::call(
            equalfn_multi,
            tmpcontext_id,
            estate,
        )?
    } else {
        false
    };

    let isdistinct;
    if !haslast || !equal {
        // if (pertrans->haslast) ExecClearTuple(pertrans->uniqslot);
        if haslast {
            exec_clear_tuple::call(estate, uniqslot)?;
        }
        // pertrans->haslast = true;
        aggstate
            .pertrans
            .as_mut()
            .expect("ExecEvalPreOrderedDistinctMulti: pertrans")[pertrans]
            .haslast = true;
        // ExecCopySlot(pertrans->uniqslot, pertrans->sortslot);
                exec_copy_slot::call(estate, uniqslot, sortslot)?;
        // isdistinct = true;
        isdistinct = true;
    } else {
        isdistinct = false;
    }

    // tmpcontext->ecxt_outertuple = save_outer;
    // tmpcontext->ecxt_innertuple = save_inner;
    {
        let ec = estate.ecxt_mut(tmpcontext_id);
        ec.ecxt_outertuple = save_outer;
        ec.ecxt_innertuple = save_inner;
    }

    Ok(isdistinct)
}

/// `aggstate->tmpcontext` — the per-input temporary `ExprContext` id. The
/// nodeAgg model carries `tmpcontext` as an [`EcxtId`] pool index (#165 P0,
/// mirroring `ps_ExprContext`), the same context the interpreter threads
/// through the EState ExprContext pool, so the id is read directly.
fn aggstate_tmpcontext<'mcx>(
    aggstate: &AggState<'mcx>,
    _estate: &EStateData<'mcx>,
) -> types_nodes::execnodes::EcxtId {
    aggstate
        .tmpcontext
        .expect("eval_agg: aggstate->tmpcontext EcxtId not assigned by ExecInitAgg")
}

/// `ExecEvalAggOrderedTransDatum(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — tuplesort-feed a single ORDER BY / DISTINCT
/// aggregate input datum.
pub fn ExecEvalAggOrderedTransDatum<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: types_nodes::execnodes::EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (econtext, &estate);
    // AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
    // int setno = op->d.agg_trans.setno;
    let steps = state
        .steps
        .as_ref()
        .expect("ExecEvalAggOrderedTransDatum: steps not ready");
    let (pertrans, setno, resvalue_id) = match &steps[op].d {
        ExprEvalStepData::AggTrans { pertrans, setno, .. } => {
            (*pertrans, *setno, steps[op].resvalue)
        }
        _ => unreachable!("ExecEvalAggOrderedTransDatum: step is not EEOP_AGG_ORDERED_TRANS_DATUM"),
    };

    // tuplesort_putdatum(pertrans->sortstates[setno], *op->resvalue, *op->resnull);
    // `op->resvalue` is STATE_RESULT_CELL (the trans value the preceding sub-steps
    // wrote into `&state->resvalue`), so read it through `read_cell`, which routes
    // the sentinel to `state.resvalue`/`resnull` (a bare arena read would deref a
    // dead slot 0). Clone the value out before re-borrowing `state` for sortstate.
    let (value, isnull) = crate::interp_loop::read_cell(state, resvalue_id);
    let value = value.clone();

    let sortstate = ordered_sortstate(state, pertrans, setno)?;
    tuplesort_putdatum::call(sortstate, value, isnull)
}

/// `ExecEvalAggOrderedTransTuple(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — tuplesort-feed a multi-column aggregate input row.
pub fn ExecEvalAggOrderedTransTuple<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: types_nodes::execnodes::EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = econtext;
    // AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
    // int setno = op->d.agg_trans.setno;
    // C reads `slot1->tts_values[i]`/`tts_isnull[i]` straight from the sortslot,
    // which ExecInitExprRec wrote each input column into. The owned compiler
    // cannot name the sortslot's per-attribute cells, so it recursed each input
    // into a fresh arena cell instead and threaded them through `arg_cells`;
    // read them here and stage them onto the sortslot via store_virtual_values
    // (the same indirection EEOP_AGG_PRESORTED_DISTINCT_MULTI uses). Read the
    // cells BEFORE re-deriving the &mut AggState — they live on `state`, the
    // sortslot/sortstate on the parent AggState (disjoint, as in C).
    let (pertrans, setno, input_values, input_nulls) = {
        let steps = state
            .steps
            .as_ref()
            .expect("ExecEvalAggOrderedTransTuple: steps not ready");
        let (pertrans, setno, arg_cells) = match &steps[op].d {
            ExprEvalStepData::AggTrans {
                pertrans,
                setno,
                arg_cells,
                ..
            } => (*pertrans, *setno, arg_cells),
            _ => unreachable!(
                "ExecEvalAggOrderedTransTuple: step is not EEOP_AGG_ORDERED_TRANS_TUPLE"
            ),
        };
        let mut values: Vec<DatumV> = Vec::with_capacity(arg_cells.len());
        let mut nulls: Vec<bool> = Vec::with_capacity(arg_cells.len());
        for &c in arg_cells.iter() {
            // read_cell returns the input by-reference-faithfully (its ByRef arm
            // for a typbyval=false sort key), so the comparison-tuple formation
            // reads varlena bytes rather than a collapsed by-value word.
            let (v, n) = crate::interp_loop::read_cell(state, c);
            values.push(v);
            nulls.push(n);
        }
        (pertrans, setno, values, nulls)
    };

    let sortslot = {
        let pt = ordered_pertrans(state, pertrans)?;
        pt.sortslot.expect("ExecEvalAggOrderedTransTuple: sortslot")
    };

    // The sortslot's descriptor (ExecTypeFromTL(aggref->args)) has exactly
    // numInputs columns, the full set of aggregate input expressions the
    // compiler recursed; store all of them.
    let values_v: Vec<DatumV> = input_values
        .iter()
        .map(|v| v.clone_in(estate.es_query_cxt))
        .collect::<PgResult<Vec<_>>>()?;

    // ExecClearTuple(pertrans->sortslot);
    // pertrans->sortslot->tts_nvalid = pertrans->numInputs;
    // ExecStoreVirtualTuple(pertrans->sortslot);
    //
    // store_virtual_values clears the slot, writes the per-column
    // values/isnull, and performs ExecStoreVirtualTuple (which sets tts_nvalid).
    store_virtual_values::call(estate, sortslot, &values_v, &input_nulls)?;

    // tuplesort_puttupleslot(pertrans->sortstates[setno], pertrans->sortslot);
    let slot_ref = estate.slot(sortslot);
    let sortstate = ordered_sortstate(state, pertrans, setno)?;
    tuplesort_puttupleslot::call(sortstate, slot_ref)
}

/// Resolve `op->d.agg_trans.pertrans` to the live `AggStatePerTransData`.
///
/// C: `AggStatePerTrans pertrans = op->d.agg_trans.pertrans;` — a direct
/// pointer into `aggstate->pertrans[]`. The owned model carries that pointer as
/// the pertrans slice index, and reaches the AggState through `state->parent`
/// (`castNode(AggState, state->parent)`, same channel the plain-trans steps
/// use), then indexes its `pertrans` vector.
fn ordered_pertrans<'a, 'mcx>(
    state: &'a ExprState<'mcx>,
    pertrans: usize,
) -> PgResult<&'a AggStatePerTransData<'mcx>> {
    let aggstate = crate::interp_loop::agg_parent_mut(state);
    let pt = &aggstate
        .pertrans
        .as_ref()
        .expect("ordered_pertrans: aggstate->pertrans not built by ExecInitAgg")[pertrans];
    Ok(pt)
}

/// `pertrans->sortstates[setno]` — the per-grouping-set `Tuplesortstate` the
/// ordered-aggregate input is fed into. Reaches the live nodeAgg pertrans (and
/// thus its `sortstates` array) through `state->parent`.
fn ordered_sortstate<'a, 'mcx>(
    state: &'a mut ExprState<'mcx>,
    pertrans: usize,
    setno: i32,
) -> PgResult<&'a mut types_nodes::Tuplesortstate<'mcx>> {
    let aggstate = crate::interp_loop::agg_parent_mut(state);
    let pt = &mut aggstate
        .pertrans
        .as_mut()
        .expect("ordered_sortstate: aggstate->pertrans not built by ExecInitAgg")[pertrans];
    let sortstate = pt
        .sortstates
        .as_mut()
        .expect("ordered_sortstate: pertrans->sortstates not allocated (ORDER BY/DISTINCT agg)")
        [setno as usize]
        .as_mut()
        .expect("ordered_sortstate: pertrans->sortstates[setno] is NULL");
    Ok(&mut *sortstate)
}

/// `ExecAggPlainTransByVal(AggState *aggstate, AggStatePerTrans pertrans,
/// AggStatePerGroup pergroup, ExprContext *aggcontext, int setno)` — pass-by-
/// value plain transition (inlined fast path for the AGG_PLAIN_TRANS opcodes).
pub fn ExecAggPlainTransByVal<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
    pergroup: &mut AggStatePerGroupData,
    aggcontext: types_nodes::execnodes::EcxtId,
    setno: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (aggcontext, &estate);
    // cf. select_current_set():
    // aggstate->curaggcontext = aggcontext;
    aggstate.curaggcontext = ecxt_id_to_aggcontext_index(aggstate, aggcontext);
    // aggstate->current_set = setno;
    aggstate.current_set = setno;
    // aggstate->curpertrans = pertrans;
    aggstate.curpertrans = pertrans as i32;

    // oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);
    // fcinfo->args[0].value = pergroup->transValue;
    // fcinfo->args[0].isnull = pergroup->transValueIsNull;
    // fcinfo->isnull = false;
    // newVal = FunctionCallInvoke(fcinfo);
    let pt = &mut aggstate
        .pertrans
        .as_mut()
        .expect("ExecAggPlainTransByVal: pertrans")[pertrans];
    let (new_val, fcinfo_isnull) =
        invoke_transfn(pt, word_of(&pergroup.trans_value), pergroup.trans_value_is_null)?;

    // pergroup->transValue = newVal;
    pergroup.trans_value = DatumV::ByVal(new_val.as_usize());
    // pergroup->transValueIsNull = fcinfo->isnull;
    pergroup.trans_value_is_null = fcinfo_isnull;
    // MemoryContextSwitchTo(oldContext);
    Ok(())
}

/// `ExecAggPlainTransByRef(...)` — pass-by-reference plain transition.
pub fn ExecAggPlainTransByRef<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
    pergroup: &mut AggStatePerGroupData,
    aggcontext: types_nodes::execnodes::EcxtId,
    setno: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = aggcontext;
    // cf. select_current_set():
    aggstate.curaggcontext = ecxt_id_to_aggcontext_index(aggstate, aggcontext);
    aggstate.current_set = setno;
    aggstate.curpertrans = pertrans as i32;

    // oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);
    // fcinfo->args[0].value = pergroup->transValue;
    // fcinfo->args[0].isnull = pergroup->transValueIsNull;
    // fcinfo->isnull = false;
    // newVal = FunctionCallInvoke(fcinfo);
    let (mut new_val, fcinfo_isnull) = {
        let pt = &mut aggstate
            .pertrans
            .as_mut()
            .expect("ExecAggPlainTransByRef: pertrans")[pertrans];
        invoke_transfn(pt, word_of(&pergroup.trans_value), pergroup.trans_value_is_null)?
    };

    // For pass-by-ref: must copy the new value into aggcontext and free the
    // prior transValue, unless the transfn returned its first input pointer.
    //
    // if (DatumGetPointer(newVal) != DatumGetPointer(pergroup->transValue))
    //     newVal = ExecAggCopyTransValue(aggstate, pertrans, newVal, fcinfo->isnull,
    //                                    pergroup->transValue, pergroup->transValueIsNull);
    if datum_get_pointer(new_val) != datum_get_pointer(word_of(&pergroup.trans_value)) {
        new_val = ExecAggCopyTransValue(
            aggstate,
            pertrans,
            new_val,
            fcinfo_isnull,
            word_of(&pergroup.trans_value),
            pergroup.trans_value_is_null,
            estate,
        )?;
    }

    // pergroup->transValue = newVal;
    pergroup.trans_value = DatumV::ByVal(new_val.as_usize());
    // pergroup->transValueIsNull = fcinfo->isnull;
    pergroup.trans_value_is_null = fcinfo_isnull;
    // MemoryContextSwitchTo(oldContext);
    Ok(())
}

/// `DatumGetPointer(d)` — the pointer-word identity used by the pass-by-ref
/// fast path's "did the transfn return its own input?" comparison. For a by-ref
/// datum this is the pointer value; the comparison is meaningful on the bare
/// `Datum` word the trimmed model carries.
#[inline]
fn datum_get_pointer(d: Datum) -> usize {
    d.as_usize()
}

/// `aggstate->curaggcontext = aggcontext` — store the current aggcontext. The
/// nodeAgg model holds `curaggcontext` as an index into `aggstate->aggcontexts`
/// (the per-grouping-set ExprContext array, now [`EcxtId`]s, #165 P0), whereas
/// the step threads the aggcontext as an EState ExprContext-pool [`EcxtId`].
/// Both name the same ExprContext in C, so the index is the position of the id
/// in `aggcontexts`.
fn ecxt_id_to_aggcontext_index<'mcx>(
    aggstate: &AggState<'mcx>,
    aggcontext: types_nodes::execnodes::EcxtId,
) -> i32 {
    aggstate
        .aggcontexts
        .as_ref()
        .and_then(|a| a.iter().position(|id| *id == aggcontext))
        .map(|i| i as i32)
        .expect("eval_agg: aggcontext EcxtId not found in aggstate->aggcontexts")
}

#[cfg(test)]
mod tests {
    use super::*;
    use backend_executor_nodeAgg::AggStatePerTransData;

    /// `1219` — `pg_proc.dat` OID of `int8inc(int8) -> int8`, a by-value
    /// strict transition function (the SUM/COUNT increment leg).
    const INT8INC_OID: types_core::primitive::Oid = 1219;

    /// Install a `function_call_invoke` stub that mirrors the by-value transfns
    /// the test exercises: `int8inc` returns `args[0] + 1`. The shared seam is a
    /// process-global `OnceLock`, so every invoke_transfn assertion shares this
    /// one installation.
    fn install_invoke_stub() {
        use std::sync::Once;
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            backend_utils_fmgr_fmgr_seams::function_call_invoke::set(
                |fn_oid, _collation, args| match fn_oid {
                    INT8INC_OID => {
                        // int8inc is strict; the by-value fast path never calls
                        // it with a NULL arg0 (the *_strict opcode guards it),
                        // but assert the contract here for clarity.
                        assert!(!args[0].isnull, "int8inc stub: arg0 is NULL");
                        let v = args[0].value.as_i64();
                        Ok((Datum::from_i64(v + 1), false))
                    }
                    other => panic!("function_call_invoke stub: unexpected fn_oid {other}"),
                },
            );
        });
    }

    /// Build a minimal by-value transfn pertrans: `transfn.fn_oid = int8inc`,
    /// a one-element `transfn_fcinfo.args` frame (arg0 = the running trans
    /// value), and a collation.
    fn pertrans_int8inc<'mcx>(mcx: mcx::Mcx<'mcx>) -> AggStatePerTransData<'mcx> {
        let mut pt = AggStatePerTransData::default();
        pt.transfn.fn_oid = INT8INC_OID;
        pt.transfn.fn_strict = true;
        pt.agg_collation = 0; // InvalidOid
        pt.transtype_by_val = true;

        let mut fcinfo = types_nodes::fmgr::FunctionCallInfoBaseData::default();
        // One argument cell (transfn nargs = 1: arg0 is the running state).
        fcinfo.nargs = 1;
        fcinfo.args = vec![types_datum::NullableDatum::null()];
        pt.transfn_fcinfo = Some(mcx::alloc_in(mcx, fcinfo).expect("alloc transfn_fcinfo"));
        pt
    }

    #[test]
    fn invoke_transfn_by_value_increments() {
        install_invoke_stub();
        let ctx = mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let mut pt = pertrans_int8inc(mcx);

        // advance: transValue 41 (non-null) -> int8inc -> 42.
        let (new_val, isnull) = invoke_transfn(&mut pt, Datum::from_i64(41), false)
            .expect("invoke_transfn");
        assert!(!isnull);
        assert_eq!(new_val.as_i64(), 42);

        // The frame's args[0] was written from the passed trans value before the
        // call (mirrors `fcinfo->args[0].value = pergroup->transValue`).
        let fcinfo = pt.transfn_fcinfo.as_ref().unwrap();
        assert_eq!(fcinfo.args[0].value.as_i64(), 41);
        assert!(!fcinfo.args[0].isnull);

        // A second advance feeds the running accumulator: 42 -> 43.
        let (next, _) = invoke_transfn(&mut pt, new_val, false).expect("invoke_transfn 2");
        assert_eq!(next.as_i64(), 43);
    }

    #[test]
    fn transfn_arg_value_and_isnull_read_frame() {
        let ctx = mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let mut pt = pertrans_int8inc(mcx);
        {
            let fcinfo = pt.transfn_fcinfo.as_mut().unwrap();
            fcinfo.args[0] = types_datum::NullableDatum::value(Datum::from_i64(7));
        }
        assert_eq!(transfn_arg_value(&pt, 0).as_i64(), 7);
        assert!(!transfn_arg_isnull(&pt, 0));
    }
}
