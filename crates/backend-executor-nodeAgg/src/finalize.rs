//! Finalize family: running final functions to produce aggregate results
//! (full and partial) and projecting the group's output tuple.

use types_datum::Datum;
use types_error::PgResult;
use types_nodes::nodeagg::{
    AggStateData, AggStatePerAggData, AggStatePerGroupData, AGG_HASHED, AGG_MIXED,
};
use types_nodes::nodeagg::do_aggsplit_skipfinal;
use types_nodes::{EStateData, SlotId};

use crate::transition::{process_ordered_aggregate_multi, process_ordered_aggregate_single};

/// `finalize_aggregate(aggstate, peragg, pergroupstate, &resultVal,
/// &resultIsNull)` — apply the aggregate's final function to its transition
/// value, producing the result Datum and null flag.
///
/// ```c
/// LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);
/// AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];
/// oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
/// /* evaluate direct args into fcinfo->args[1..] (ExecEvalExpr) */
/// i = 1;
/// foreach(lc, peragg->aggdirectargs) {
///     fcinfo->args[i].value = ExecEvalExpr(expr, econtext, &args[i].isnull);
///     anynull |= args[i].isnull; i++;
/// }
/// if (OidIsValid(peragg->finalfn_oid)) {
///     aggstate->curperagg = peragg;
///     InitFunctionCallInfoData(*fcinfo, &peragg->finalfn, numFinalArgs,
///                              pertrans->aggCollation, (Node *) aggstate, NULL);
///     fcinfo->args[0].value =
///         MakeExpandedObjectReadOnly(pergroupstate->transValue,
///                                    pergroupstate->transValueIsNull,
///                                    pertrans->transtypeLen);
///     fcinfo->args[0].isnull = pergroupstate->transValueIsNull;
///     anynull |= pergroupstate->transValueIsNull;
///     for (; i < numFinalArgs; i++) { args[i].value = 0; args[i].isnull = true; anynull = true; }
///     if (fcinfo->flinfo->fn_strict && anynull) { *resultVal = 0; *resultIsNull = true; }
///     else {
///         result = FunctionCallInvoke(fcinfo);
///         *resultIsNull = fcinfo->isnull;
///         *resultVal = MakeExpandedObjectReadOnly(result, fcinfo->isnull, peragg->resulttypeLen);
///     }
///     aggstate->curperagg = NULL;
/// } else {
///     *resultVal = MakeExpandedObjectReadOnly(pergroupstate->transValue,
///                                             pergroupstate->transValueIsNull,
///                                             pertrans->transtypeLen);
///     *resultIsNull = pergroupstate->transValueIsNull;
/// }
/// MemoryContextSwitchTo(oldContext);
/// ```
///
/// The whole body is the fmgr call frame: building a `LOCAL_FCINFO`, the
/// `ExecEvalExpr` of the direct arguments, `InitFunctionCallInfoData` +
/// `FunctionCallInvoke` of the finalfn, and the `MakeExpandedObjectReadOnly`
/// wrap of every state/result `Datum`. None of those have landed: the trimmed
/// `FunctionCallInfoBaseData`/`FmgrInfo` carry none of the consumed fields
/// (`args`/`isnull`/`flinfo`/`fn_strict`), `backend-utils-fmgr-fmgr-seams`
/// declares no `FunctionCallInvoke`/`InitFunctionCallInfoData` slot, and
/// `MakeExpandedObjectReadOnly` (`utils/expandeddatum`) is unported with no
/// seam. The body therefore stands behind a loud panic until those land, per
/// the seam-and-panic discipline.
pub fn finalize_aggregate<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    peragg: &AggStatePerAggData<'mcx>,
    pergroupstate: &mut AggStatePerGroupData,
) -> PgResult<(Datum, bool)> {
    let _ = (aggstate, peragg, pergroupstate);
    panic!(
        "finalize_aggregate: the finalfn fmgr call frame \
         (InitFunctionCallInfoData / FunctionCallInvoke / ExecEvalExpr on the \
         direct args) and MakeExpandedObjectReadOnly are not yet ported"
    );
}

/// `finalize_partialaggregate(aggstate, peragg, pergroupstate, &resultVal,
/// &resultIsNull)` — produce the partial-aggregate output: the transition
/// value as-is, or serialized via the serialfn.
///
/// ```c
/// AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];
/// oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
/// if (OidIsValid(pertrans->serialfn_oid)) {
///     if (pertrans->serialfn.fn_strict && pergroupstate->transValueIsNull) {
///         *resultVal = 0; *resultIsNull = true;
///     } else {
///         FunctionCallInfo fcinfo = pertrans->serialfn_fcinfo;
///         fcinfo->args[0].value = MakeExpandedObjectReadOnly(transValue, isnull, transtypeLen);
///         fcinfo->args[0].isnull = transValueIsNull;
///         fcinfo->isnull = false;
///         result = FunctionCallInvoke(fcinfo);
///         *resultIsNull = fcinfo->isnull;
///         *resultVal = MakeExpandedObjectReadOnly(result, fcinfo->isnull, peragg->resulttypeLen);
///     }
/// } else {
///     *resultVal = MakeExpandedObjectReadOnly(transValue, isnull, transtypeLen);
///     *resultIsNull = pergroupstate->transValueIsNull;
/// }
/// MemoryContextSwitchTo(oldContext);
/// ```
///
/// Same blockers as [`finalize_aggregate`]: the serialfn `FunctionCallInvoke`
/// over `pertrans->serialfn_fcinfo` and `MakeExpandedObjectReadOnly` are
/// unported with no seam, and the trimmed call-frame/`FmgrInfo` types carry
/// none of the consumed fields. The body stands behind a loud panic until
/// those land.
pub fn finalize_partialaggregate<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    peragg: &AggStatePerAggData<'mcx>,
    pergroupstate: &mut AggStatePerGroupData,
) -> PgResult<(Datum, bool)> {
    let _ = (aggstate, peragg, pergroupstate);
    panic!(
        "finalize_partialaggregate: the serialfn FunctionCallInvoke over \
         pertrans->serialfn_fcinfo and MakeExpandedObjectReadOnly are not yet \
         ported"
    );
}

/// `prepare_projection_slot(aggstate, slot, currentSet)` — fill the result
/// slot's grouping columns with the right values/NULLs for `currentSet`
/// (NULLs for columns not in the current grouping set).
///
/// ```c
/// if (aggstate->phase->grouped_cols) {
///     Bitmapset *grouped_cols = aggstate->phase->grouped_cols[currentSet];
///     aggstate->grouped_cols = grouped_cols;
///     if (TTS_EMPTY(slot)) {
///         ExecStoreAllNullTuple(slot);
///     } else if (aggstate->all_grouped_cols) {
///         slot_getsomeattrs(slot, linitial_int(aggstate->all_grouped_cols));
///         foreach(lc, aggstate->all_grouped_cols) {
///             int attnum = lfirst_int(lc);
///             if (!bms_is_member(attnum, grouped_cols))
///                 slot->tts_isnull[attnum - 1] = true;
///         }
///     }
/// }
/// ```
///
/// The phase / grouped-cols selection and the empty-input
/// `ExecStoreAllNullTuple` path are ported here (the slot store goes through
/// the `backend-executor-execTuples` seam). The `all_grouped_cols` branch
/// pokes `slot->tts_isnull[attnum - 1]` and runs `slot_getsomeattrs` +
/// `bms_is_member`: the trimmed `TupleTableSlot` carries no `tts_isnull`
/// payload yet (the slot deform/`slot_getsomeattrs` model has not landed) and
/// `nodes/bitmapset.c`'s `bms_is_member` is unported with no seam, so that
/// branch alone stands behind a loud panic until those land.
pub fn prepare_projection_slot<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    slot: SlotId,
    current_set: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // if (aggstate->phase->grouped_cols)
    let phase_has_grouped_cols = {
        let phase = &aggstate.phases.as_ref().expect("phases array set")
            [aggstate.phase as usize];
        phase.grouped_cols.is_some()
    };

    if phase_has_grouped_cols {
        // aggstate->grouped_cols = aggstate->phase->grouped_cols[currentSet];
        //
        // C aliases the phase's per-set Bitmapset into aggstate->grouped_cols
        // (a shared, non-owning pointer) so GroupingExpr can read it during
        // finalfn evaluation. The owned phase array owns its grouped_cols, so
        // the alias is made by deep-copying the selected set into the
        // aggstate-owned slot (fallible: bms_copy pallocs).
        let copied = {
            let phase = &aggstate.phases.as_ref().expect("phases array set")
                [aggstate.phase as usize];
            let grouped_cols = phase
                .grouped_cols
                .as_ref()
                .expect("phase grouped_cols set")[current_set as usize]
                .clone_in(estate.es_query_cxt)?;
            mcx::alloc_in(estate.es_query_cxt, grouped_cols)?
        };
        aggstate.grouped_cols = Some(copied);

        // if (TTS_EMPTY(slot)) ExecStoreAllNullTuple(slot);
        let slot_empty = estate.es_tupleTable[slot.0 as usize].is_empty();
        if slot_empty {
            backend_executor_execTuples_seams::exec_store_all_null_tuple::call(estate, slot)?;
        } else if aggstate.all_grouped_cols.is_some() {
            // The all_grouped_cols null-forcing branch: slot_getsomeattrs +
            // direct slot->tts_isnull[] poke + bms_is_member, none of which
            // the trimmed slot/Bitmapset model supports yet.
            let _ = (aggstate, slot, current_set);
            panic!(
                "prepare_projection_slot: the all_grouped_cols branch needs \
                 slot_getsomeattrs, the slot tts_isnull payload, and \
                 bms_is_member, none of which are ported yet"
            );
        }
    }

    Ok(())
}

/// `finalize_aggregates(aggstate, peraggs, pergroup)` — finalize every
/// aggregate for the current group into the econtext aggvalues/aggnulls
/// arrays, then advance any DISTINCT/ORDER BY transition first.
///
/// ```c
/// for (transno = 0; transno < aggstate->numtrans; transno++) {
///     AggStatePerTrans pertrans = &aggstate->pertrans[transno];
///     AggStatePerGroup pergroupstate = &pergroup[transno];
///     if (pertrans->aggsortrequired) {
///         Assert(aggstrategy != AGG_HASHED && aggstrategy != AGG_MIXED);
///         if (pertrans->numInputs == 1)
///             process_ordered_aggregate_single(aggstate, pertrans, pergroupstate);
///         else
///             process_ordered_aggregate_multi(aggstate, pertrans, pergroupstate);
///     } else if (pertrans->numDistinctCols > 0 && pertrans->haslast) {
///         pertrans->haslast = false;
///         if (pertrans->numDistinctCols == 1) {
///             if (!pertrans->inputtypeByVal && !pertrans->lastisnull)
///                 pfree(DatumGetPointer(pertrans->lastdatum));
///             pertrans->lastisnull = false;
///             pertrans->lastdatum = (Datum) 0;
///         } else
///             ExecClearTuple(pertrans->uniqslot);
///     }
/// }
/// for (aggno = 0; aggno < aggstate->numaggs; aggno++) {
///     AggStatePerAgg peragg = &peraggs[aggno];
///     int transno = peragg->transno;
///     AggStatePerGroup pergroupstate = &pergroup[transno];
///     if (DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit))
///         finalize_partialaggregate(aggstate, peragg, pergroupstate, &aggvalues[aggno], &aggnulls[aggno]);
///     else
///         finalize_aggregate(aggstate, peragg, pergroupstate, &aggvalues[aggno], &aggnulls[aggno]);
/// }
/// ```
///
/// The `pfree(DatumGetPointer(pertrans->lastdatum))` is a no-op in the owned
/// model: the last-datum copy lives in a memory context the executor frees,
/// not via an explicit `pfree`.
pub fn finalize_aggregates<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    pergroup: &mut [AggStatePerGroupData],
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let numtrans = aggstate.numtrans;
    let aggstrategy = aggstate.aggstrategy;

    // First, finish any DISTINCT/ORDER BY aggregates.
    //
    // pertrans lives inside aggstate->pertrans; the C aliases it while also
    // passing aggstate. Move the pertrans array out for the loop (no aliasing)
    // and restore it afterwards.
    let mut pertrans_vec = aggstate.pertrans.take().expect("pertrans array set");
    for transno in 0..numtrans {
        let pertrans = &mut pertrans_vec[transno as usize];
        let pergroupstate = &mut pergroup[transno as usize];

        if pertrans.aggsortrequired {
            // Assert(aggstrategy != AGG_HASHED && aggstrategy != AGG_MIXED);
            debug_assert!(aggstrategy != AGG_HASHED && aggstrategy != AGG_MIXED);

            if pertrans.num_inputs == 1 {
                process_ordered_aggregate_single(aggstate, pertrans, pergroupstate)?;
            } else {
                process_ordered_aggregate_multi(aggstate, pertrans, pergroupstate)?;
            }
        } else if pertrans.num_distinct_cols > 0 && pertrans.haslast {
            pertrans.haslast = false;

            if pertrans.num_distinct_cols == 1 {
                // if (!inputtypeByVal && !lastisnull) pfree(lastdatum);
                //   — no-op: context-managed in the owned model.
                pertrans.lastisnull = false;
                pertrans.lastdatum = Datum::null();
            } else {
                let uniqslot = pertrans.uniqslot.expect("multi-distinct uniqslot set");
                backend_executor_execTuples_seams::exec_clear_tuple::call(
                    &mut estate.es_tupleTable[uniqslot.0 as usize],
                )?;
            }
        }
    }
    aggstate.pertrans = Some(pertrans_vec);

    // Run the final functions.
    //
    // peragg lives inside aggstate->peragg; move it out for the loop so each
    // finalize_*aggregate call can take &mut aggstate and &peragg at once
    // (the C aliases peraggs = aggstate->peragg as a separate parameter).
    let numaggs = aggstate.numaggs;
    let aggsplit = aggstate.aggsplit;
    let peragg_vec = aggstate.peragg.take().expect("peragg array set");

    // econtext = aggstate->ss.ps.ps_ExprContext;
    let econtext_id = aggstate
        .ss
        .ps
        .ps_ExprContext
        .expect("ps_ExprContext set");

    for aggno in 0..numaggs {
        let peragg = &peragg_vec[aggno as usize];
        let transno = peragg.transno;
        let pergroupstate = &mut pergroup[transno as usize];

        let (value, isnull) = if do_aggsplit_skipfinal(aggsplit) {
            finalize_partialaggregate(aggstate, peragg, pergroupstate)?
        } else {
            finalize_aggregate(aggstate, peragg, pergroupstate)?
        };

        // aggvalues[aggno] = value; aggnulls[aggno] = isnull;
        let econtext = estate.es_exprcontexts[econtext_id.0 as usize]
            .as_mut()
            .expect("ExprContext live");
        econtext.ecxt_aggvalues[aggno as usize] = value;
        econtext.ecxt_aggnulls[aggno as usize] = isnull;
    }
    aggstate.peragg = Some(peragg_vec);

    Ok(())
}

/// `project_aggregates(aggstate)` — evaluate the qual and projection for the
/// current group, returning the projected output slot or `None` if the qual
/// rejected the group.
///
/// ```c
/// ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
/// if (ExecQual(aggstate->ss.ps.qual, econtext))
///     return ExecProject(aggstate->ss.ps.ps_ProjInfo);
/// else
///     InstrCountFiltered1(aggstate, 1);
/// return NULL;
/// ```
pub fn project_aggregates<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // econtext = aggstate->ss.ps.ps_ExprContext;
    let econtext_id = aggstate
        .ss
        .ps
        .ps_ExprContext
        .expect("ps_ExprContext set");

    // ExecQual(aggstate->ss.ps.qual, econtext) — a NULL qual is always true,
    // handled by the seam owner; the owned NULL is `qual = None`.
    let passed = match aggstate.ss.ps.qual.as_ref() {
        Some(qual) => {
            backend_executor_execExpr_seams::exec_qual::call(qual, econtext_id, estate)?
        }
        None => true,
    };

    if passed {
        // return ExecProject(aggstate->ss.ps.ps_ProjInfo);
        let slot =
            backend_executor_execExpr_seams::exec_project::call(&mut aggstate.ss.ps, estate)?;
        Ok(Some(slot))
    } else {
        // InstrCountFiltered1(aggstate, 1):
        //   if (planstate->instrument) planstate->instrument->nfiltered1 += 1;
        if let Some(instr) = aggstate.ss.ps.instrument.as_mut() {
            instr.nfiltered1 += 1.0;
        }
        Ok(None)
    }
}
