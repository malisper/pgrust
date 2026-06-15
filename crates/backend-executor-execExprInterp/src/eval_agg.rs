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
//! `FunctionCallInvoke(fcinfo)`. In this F0 cut the transfn call frame is the
//! nodeAgg-parked [`types_nodes::fmgr::FunctionCallInfoBaseData`], trimmed to
//! its `resultinfo` field: it carries **no** `args` / `isnull` / `flinfo`
//! payload (docs/types.md rule 3), and the resolved C function pointer
//! (`PGFunction`) the call dispatches through is not in the shared vocabulary
//! either ([`FmgrInfo::fn_addr`] is an opaque address word). The compiler
//! gathers the transfn argument sub-expressions into those arg cells before the
//! AGG_PLAIN_TRANS step runs; the actual frame that holds them is nodeAgg's.
//!
//! This is the same gap the merged nodeAgg port documents in `finalize.rs`
//! (`local_fcinfo_set_arg` / `invoke_finalfn`: "the trimmed
//! `FunctionCallInfoBaseData` carries no args"). Mirroring that precedent
//! (mirror-pg-and-panic), the call-frame reads/writes and the invoke are
//! private helpers that panic with a precise rationale until the fmgr call
//! frame is plumbed through the shared vocabulary. Every other line of these
//! handlers — the strictness / no-trans-value branching, the memory-context
//! switches, the pass-by-ref reparenting decision, `datumCopy`, the
//! presorted-distinct comparisons, and the ordered-aggregate tuplesort feed —
//! is the interpreter's own logic and is implemented faithfully here.

use backend_executor_execTuples_seams::{
    exec_clear_tuple, exec_copy_slot, exec_store_virtual_tuple, store_virtual_values,
};
use backend_utils_fmgr_fmgr_seams::function_call2_coll;
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
// transfn call frame (nodeAgg-parked fmgr frame; not in the shared vocabulary)
//
// C: `FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;`, then
// `fcinfo->args[i].value` / `fcinfo->args[i].isnull`, `fcinfo->isnull`, and
// `FunctionCallInvoke(fcinfo)`. The trimmed `FunctionCallInfoBaseData` carries
// none of `args`/`isnull`/`flinfo`, and `FmgrInfo.fn_addr` is an opaque word,
// so these reads/writes and the call cannot be expressed here yet.
// ---------------------------------------------------------------------------

/// `pertrans->transfn_fcinfo->args[n].value` — read a transfn argument cell.
fn transfn_arg_value(_pertrans: &AggStatePerTransData<'_>, _n: usize) -> Datum {
    panic!(
        "backend-executor-execExprInterp::eval_agg: reading pertrans->transfn_fcinfo->args is \
         part of the fmgr call frame; the trimmed FunctionCallInfoBaseData carries no args \
         (fmgr call-frame payload not yet in the shared vocabulary)"
    );
}

/// `pertrans->transfn_fcinfo->args[n].isnull` — read a transfn argument's null.
fn transfn_arg_isnull(_pertrans: &AggStatePerTransData<'_>, _n: usize) -> bool {
    panic!(
        "backend-executor-execExprInterp::eval_agg: reading pertrans->transfn_fcinfo->args is \
         part of the fmgr call frame; the trimmed FunctionCallInfoBaseData carries no args \
         (fmgr call-frame payload not yet in the shared vocabulary)"
    );
}

/// `fcinfo->args[0].value = transValue; fcinfo->args[0].isnull = isnull;
/// fcinfo->isnull = false; newVal = FunctionCallInvoke(fcinfo);` — set the
/// running transition value into arg 0 and invoke the transition function.
/// Returns `(newVal, fcinfo->isnull)`.
fn invoke_transfn(
    _pertrans: &mut AggStatePerTransData<'_>,
    _trans_value: Datum,
    _trans_value_is_null: bool,
) -> (Datum, bool) {
    panic!(
        "backend-executor-execExprInterp::eval_agg: FunctionCallInvoke of pertrans->transfn is \
         the fmgr call dispatch through FmgrInfo.fn_addr (an opaque address word in the shared \
         FmgrInfo) over the trimmed transfn_fcinfo frame; not yet in the shared vocabulary"
    );
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
    let _ = &estate;
    // Read the (immutable) comparison inputs/config off the pertrans first.
    let (value, isnull, haslast, lastisnull, lastdatum, agg_collation, equalfn_one_oid,
         inputtype_by_val, inputtype_len) = {
        let pt =
            &aggstate.pertrans.as_ref().expect("ExecEvalPreOrderedDistinctSingle: pertrans")[pertrans];
        // Datum value = pertrans->transfn_fcinfo->args[1].value;
        // bool  isnull = pertrans->transfn_fcinfo->args[1].isnull;
        (
            transfn_arg_value(pt, 1),
            transfn_arg_isnull(pt, 1),
            pt.haslast,
            pt.lastisnull,
            word_of(&pt.lastdatum),
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
        let r = function_call2_coll::call(equalfn_one_oid, agg_collation, lastdatum, value)?;
        datum_get_bool(r)
    };

    if !equal {
        // if (pertrans->haslast && !pertrans->inputtypeByVal && !pertrans->lastisnull)
        //     pfree(DatumGetPointer(pertrans->lastdatum));
        if haslast && !inputtype_by_val && !lastisnull {
            pfree_last_datum(aggstate, pertrans, lastdatum);
        }

        let new_lastdatum = if !isnull {
            // oldContext = MemoryContextSwitchTo(aggstate->curaggcontext->ecxt_per_tuple_memory);
            // pertrans->lastdatum = datumCopy(value, inputtypeByVal, inputtypeLen);
            // MemoryContextSwitchTo(oldContext);
            let _ = (inputtype_by_val, inputtype_len);
            distinct_last_datum_copy(aggstate, pertrans, value)
        } else {
            // pertrans->lastdatum = (Datum) 0;
            Datum::null()
        };

        let pt = &mut aggstate
            .pertrans
            .as_mut()
            .expect("ExecEvalPreOrderedDistinctSingle: pertrans")[pertrans];
        pt.haslast = true;
        pt.lastdatum = DatumV::ByVal(new_lastdatum.as_usize());
        pt.lastisnull = isnull;
        return Ok(true);
    }

    Ok(false)
}

/// `DatumGetBool(d)` — low bit of the datum word.
#[inline]
fn datum_get_bool(d: Datum) -> bool {
    d.as_bool()
}

/// `pfree(DatumGetPointer(pertrans->lastdatum))` — free a by-reference last
/// datum from the curaggcontext. Owned by the mmgr/datum unit (a by-ref
/// `Datum` pointer is not modeled in the trimmed shared vocabulary).
fn pfree_last_datum<'mcx>(_aggstate: &mut AggState<'mcx>, _pertrans: usize, _lastdatum: Datum) {
    panic!(
        "backend-executor-execExprInterp::eval_agg: pfree of pertrans->lastdatum (a by-reference \
         Datum pointer) is the mmgr owner's; the trimmed model does not carry a by-ref Datum"
    );
}

/// `pertrans->lastdatum = datumCopy(value, inputtypeByVal, inputtypeLen)` into
/// the curaggcontext per-tuple memory. By-ref deep copy is the datum.c owner's.
fn distinct_last_datum_copy<'mcx>(
    _aggstate: &mut AggState<'mcx>,
    _pertrans: usize,
    _value: Datum,
) -> Datum {
    panic!(
        "backend-executor-execExprInterp::eval_agg: datumCopy of the single-column DISTINCT \
         last value into the curaggcontext is owned by the datum.c unit; the trimmed model \
         cannot round-trip a by-reference Datum"
    );
}

/// `ExecEvalPreOrderedDistinctMulti(AggState *aggstate,
/// AggStatePerTrans pertrans)` — multi-column DISTINCT filter over presorted
/// input.
pub fn ExecEvalPreOrderedDistinctMulti<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
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
    let mut values: Vec<Datum> = Vec::with_capacity(num_trans_inputs as usize);
    let mut isnull: Vec<bool> = Vec::with_capacity(num_trans_inputs as usize);
    {
        let pt = &aggstate
            .pertrans
            .as_ref()
            .expect("ExecEvalPreOrderedDistinctMulti: pertrans")[pertrans];
        for i in 0..num_trans_inputs as usize {
            values.push(transfn_arg_value(pt, i + 1));
            isnull.push(transfn_arg_isnull(pt, i + 1));
        }
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
    // The transfn-arg cells are bare scalar words; project each onto the
    // canonical store_virtual_values ABI edge (a C `tts_values[i]` word).
    let values_v: Vec<DatumV> = values.iter().map(|d| DatumV::from_usize(d.as_usize())).collect();
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
        let pt = aggstate
            .pertrans
            .as_ref()
            .expect("ExecEvalPreOrderedDistinctMulti: pertrans")[pertrans]
            .equalfn_multi
            .as_deref()
            .expect("ExecEvalPreOrderedDistinctMulti: equalfnMulti");
        // SAFETY of borrow: equalfn_multi lives in pertrans; exec_qual needs an
        // &ExprState plus &mut estate. Detach the ExprState borrow by reading it
        // through a raw shared reference held only for the call.
        exec_qual_on_pertrans_equalfn_multi(pt, tmpcontext_id, estate)?
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
/// nodeAgg model carries `tmpcontext` as an owned `ExprContext` box rather than
/// an [`EcxtId`] pool index, but the multi-DISTINCT equality and slot swaps run
/// against the EState ExprContext pool the interpreter threads. Resolving the
/// box to its pool id is nodeAgg's wiring (the box and the pool are the same
/// context in C); panicked here until that linkage is exposed.
fn aggstate_tmpcontext<'mcx>(
    _aggstate: &AggState<'mcx>,
    _estate: &EStateData<'mcx>,
) -> types_nodes::execnodes::EcxtId {
    panic!(
        "backend-executor-execExprInterp::eval_agg: resolving aggstate->tmpcontext (a nodeAgg-\
         owned ExprContext box) to its EState ExprContext-pool EcxtId is nodeAgg's wiring; not \
         yet exposed in the shared vocabulary"
    );
}

/// `ExecQual(pertrans->equalfnMulti, tmpcontext)` over the multi-column
/// DISTINCT comparator ExprState. The comparator is the execExpr unit's
/// compiled qual; running it is the `exec_qual` seam. The comparator ExprState
/// is parked inside `pertrans` (a nodeAgg box), so threading it as the
/// `&ExprState` the seam needs alongside `&mut estate` is nodeAgg's wiring.
fn exec_qual_on_pertrans_equalfn_multi<'mcx>(
    _equalfn_multi: &ExprState<'mcx>,
    _econtext: types_nodes::execnodes::EcxtId,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    panic!(
        "backend-executor-execExprInterp::eval_agg: ExecQual of pertrans->equalfnMulti reads a \
         comparator ExprState parked inside the nodeAgg pertrans box; threading it as the \
         exec_qual seam's &ExprState alongside &mut estate is nodeAgg's wiring, not yet exposed"
    );
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
    let (pertrans, setno) = match &steps[op].d {
        ExprEvalStepData::AggTrans { pertrans, setno, .. } => (*pertrans, *setno),
        _ => unreachable!("ExecEvalAggOrderedTransDatum: step is not EEOP_AGG_ORDERED_TRANS_DATUM"),
    };

    // tuplesort_putdatum(pertrans->sortstates[setno], *op->resvalue, *op->resnull);
    // The seam now takes the canonical `Datum<'mcx>`; clone the result cell's
    // value out before re-borrowing `state` to fetch the sortstate.
    let cell = state.result_cells.get(steps[op].resvalue);
    let value = cell.value.clone();
    let isnull = cell.isnull;

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
    let steps = state
        .steps
        .as_ref()
        .expect("ExecEvalAggOrderedTransTuple: steps not ready");
    let (pertrans, setno) = match &steps[op].d {
        ExprEvalStepData::AggTrans { pertrans, setno, .. } => (*pertrans, *setno),
        _ => unreachable!("ExecEvalAggOrderedTransTuple: step is not EEOP_AGG_ORDERED_TRANS_TUPLE"),
    };

    let (sortslot, num_inputs) = {
        let pt = ordered_pertrans(state, pertrans)?;
        (
            pt.sortslot.expect("ExecEvalAggOrderedTransTuple: sortslot"),
            pt.num_inputs,
        )
    };

    // ExecClearTuple(pertrans->sortslot);
    exec_clear_tuple::call(estate, sortslot)?;
    // pertrans->sortslot->tts_nvalid = pertrans->numInputs;
    // ExecStoreVirtualTuple(pertrans->sortslot);
    //
    // The slot's tts_values were filled by the preceding eval steps the
    // compiler emitted; tts_nvalid is set to numInputs by the virtual-tuple
    // store (the execTuples owner). The (value, isnull) payload lives in the
    // nodeAgg sortslot the compiler wrote into — represented through the
    // owner's exec_store_virtual_tuple, which marks nvalid from the slot's
    // already-populated columns.
    let _ = num_inputs;
    exec_store_virtual_tuple::call(estate, sortslot)?;

    // tuplesort_puttupleslot(pertrans->sortstates[setno], pertrans->sortslot);
    let slot_ref = estate.slot(sortslot);
    let sortstate = ordered_sortstate(state, pertrans, setno)?;
    tuplesort_puttupleslot::call(sortstate, slot_ref)
}

/// Resolve `op->d.agg_trans.pertrans` (a nodeAgg-parked opaque address) to the
/// real `AggStatePerTransData`. The AGG_ORDERED_TRANS steps carry the pertrans
/// as an opaque address word; mapping it to the live nodeAgg pertrans slice
/// entry is nodeAgg's wiring (the interpreter reaches the AggState through
/// `state->parent`, which the F0 cut does not yet thread to this crate).
fn ordered_pertrans<'a, 'mcx>(
    _state: &'a ExprState<'mcx>,
    _pertrans: usize,
) -> PgResult<&'a AggStatePerTransData<'mcx>> {
    panic!(
        "backend-executor-execExprInterp::eval_agg: resolving op->d.agg_trans.pertrans (an \
         opaque address) to the live AggStatePerTransData via state->parent (the AggState) is \
         nodeAgg's wiring; state->parent is not yet threaded to this crate"
    );
}

/// `pertrans->sortstates[setno]` — the per-grouping-set `Tuplesortstate` the
/// ordered-aggregate input is fed into. Resolving `op->d.agg_trans.pertrans` to
/// the live nodeAgg pertrans (and thus its `sortstates` array) requires
/// `state->parent`, which the F0 cut does not yet thread to this crate.
fn ordered_sortstate<'a, 'mcx>(
    _state: &'a mut ExprState<'mcx>,
    _pertrans: usize,
    _setno: i32,
) -> PgResult<&'a mut types_nodes::Tuplesortstate<'mcx>> {
    panic!(
        "backend-executor-execExprInterp::eval_agg: reaching pertrans->sortstates[setno] needs \
         op->d.agg_trans.pertrans resolved against state->parent (the AggState); state->parent \
         is not yet threaded to this crate"
    );
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
        invoke_transfn(pt, word_of(&pergroup.trans_value), pergroup.trans_value_is_null);

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
        invoke_transfn(pt, word_of(&pergroup.trans_value), pergroup.trans_value_is_null)
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
/// (the per-grouping-set ExprContext array), whereas the step threads the
/// aggcontext as an EState ExprContext-pool [`EcxtId`]. Mapping the pool id back
/// to the `aggcontexts` index is nodeAgg's wiring (both name the same
/// ExprContext in C); panicked until exposed.
fn ecxt_id_to_aggcontext_index<'mcx>(
    _aggstate: &AggState<'mcx>,
    _aggcontext: types_nodes::execnodes::EcxtId,
) -> i32 {
    panic!(
        "backend-executor-execExprInterp::eval_agg: storing aggstate->curaggcontext maps the \
         step's EState ExprContext-pool EcxtId to an index into aggstate->aggcontexts; that \
         linkage is nodeAgg's wiring, not yet exposed in the shared vocabulary"
    );
}
