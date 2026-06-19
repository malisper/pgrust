//! Finalize family: running final functions to produce aggregate results
//! (full and partial) and projecting the group's output tuple.

use types_core::primitive::{Oid, OidIsValid};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::nodeagg::{AGG_HASHED, AGG_MIXED};
use types_nodes::nodeagg::do_aggsplit_skipfinal;
use crate::aggstate::{
    AggStateData, AggStatePerAggData, AggStatePerGroupData, AggStatePerTransData,
};
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
/// wrap of every state/result `Datum`. `FunctionCallInvoke` +
/// `InitFunctionCallInfoData` ARE ported (fmgr-core, #52) and the shared
/// `FunctionCallInfoBaseData` now carries `args`/`isnull`/`flinfo`/`fncollation`
/// (#296). The genuine blocker is NOT the shared call frame: it is (a) the
/// direct-argument `ExecEvalExpr` over `peragg->aggdirectargs`, owned by the
/// not-yet-ported execExpr eval boundary, and (b) the finalfn's `fcinfo->context`
/// back-reference to the AggState (`(void *) aggstate`, consumed by AggGetAggref)
/// — reaching the nodeAgg-owned AggState as a Node is the #200 keystone this crate
/// does not yet thread. The body therefore stands behind a loud panic until the
/// execExpr eval boundary + AggState-as-Node (#200) land, per the seam-and-panic
/// discipline.
pub fn finalize_aggregate<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    peragg: &AggStatePerAggData<'mcx>,
    pergroupstate: &mut AggStatePerGroupData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);
    // bool anynull = false;
    // AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];
    let mut anynull = false;
    let pertrans = pertrans_for(aggstate, peragg.transno);
    let transtype_len = pertrans.transtype_len;
    let agg_collation = pertrans.agg_collation;

    // oldContext = MemoryContextSwitchTo(
    //     aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
    // (the per-tuple context switch is the ExprContext owner's; the finalfn /
    // direct-arg eval below allocates there.)

    // The finalfn call frame is built as an `args[]` vector dispatched by OID
    // through the fmgr `function_call_invoke_datum` seam (the same by-OID
    // dispatch the transition path uses). `args[0]` is the transition value;
    // `args[1..]` are the ordered-set direct args; the tail is NULL-padded to
    // `numFinalArgs`. The `(void *) aggstate` fcinfo context is the deferred K2
    // re-sign (only AggCheckCallContext/AggGetAggref finalfns need it; the
    // count/min/max/sum/avg finalfns do not).
    let mut final_args: alloc::vec::Vec<Datum<'mcx>> = alloc::vec::Vec::new();
    let mut final_arg_isnull: alloc::vec::Vec<bool> = alloc::vec::Vec::new();
    // placeholder for args[0] (the transition value), filled in below.
    final_args.push(Datum::null());
    final_arg_isnull.push(false);

    // i = 1;
    // foreach(lc, peragg->aggdirectargs) {
    //     ExprState *expr = (ExprState *) lfirst(lc);
    //     fcinfo->args[i].value =
    //         ExecEvalExpr(expr, aggstate->ss.ps.ps_ExprContext, &args[i].isnull);
    //     anynull |= fcinfo->args[i].isnull;
    //     i++;
    // }
    let mut i: i32 = 1;
    if let Some(directargs) = peragg.aggdirectargs.as_ref() {
        for expr in directargs.iter() {
            let (value, isnull) = exec_eval_expr_direct_arg(aggstate, expr);
            final_args.push(value);
            final_arg_isnull.push(isnull);
            anynull |= isnull;
            i += 1;
        }
    }

    let result_val;
    let result_is_null;

    // if (OidIsValid(peragg->finalfn_oid))
    if OidIsValid(peragg.finalfn_oid) {
        // int numFinalArgs = peragg->numFinalArgs;
        let num_final_args = peragg.num_final_args;

        // aggstate->curperagg = peragg; (model: index; set for AggGetAggref)
        // InitFunctionCallInfoData(*fcinfo, &peragg->finalfn, numFinalArgs,
        //                          pertrans->aggCollation, (void *) aggstate, NULL);

        // fcinfo->args[0].value = MakeExpandedObjectReadOnly(
        //     pergroupstate->transValue, pergroupstate->transValueIsNull,
        //     pertrans->transtypeLen);
        // fcinfo->args[0].isnull = pergroupstate->transValueIsNull;
        // anynull |= pergroupstate->transValueIsNull;
        // C reads `args[0] = transValue` without consuming it (nodeAgg owns the
        // aggcontext reset). An `internal` transValue is a `Datum::Internal` box
        // that cannot be cloned, so MOVE it out of the pergroup (it is finalized
        // once per group at end of input); a by-value scalar/by-ref value clones.
        let trans_value = if pergroupstate.trans_value.is_internal() {
            core::mem::replace(&mut pergroupstate.trans_value, Datum::null())
        } else {
            pergroupstate.trans_value.clone()
        };
        let arg0 = make_expanded_object_read_only(
            trans_value,
            pergroupstate.trans_value_is_null,
            transtype_len,
        );
        final_args[0] = arg0;
        final_arg_isnull[0] = pergroupstate.trans_value_is_null;
        anynull |= pergroupstate.trans_value_is_null;

        // for (; i < numFinalArgs; i++) {
        //     fcinfo->args[i].value = (Datum) 0;
        //     fcinfo->args[i].isnull = true;
        //     anynull = true;
        // }
        while i < num_final_args {
            final_args.push(Datum::null());
            final_arg_isnull.push(true);
            anynull = true;
            i += 1;
        }

        // if (fcinfo->flinfo->fn_strict && anynull)
        if finalfn_is_strict(peragg)? && anynull {
            // *resultVal = (Datum) 0; *resultIsNull = true;
            result_val = Datum::null();
            result_is_null = true;
        } else {
            // result = FunctionCallInvoke(fcinfo);
            // *resultIsNull = fcinfo->isnull;
            // *resultVal = MakeExpandedObjectReadOnly(result, fcinfo->isnull,
            //                                         peragg->resulttypeLen);
            let (result, isnull) = invoke_finalfn(
                peragg.finalfn_oid,
                agg_collation,
                final_args,
                final_arg_isnull,
                estate,
            )?;
            result_is_null = isnull;
            result_val =
                make_expanded_object_read_only(result, isnull, peragg.resulttype_len);
        }
        // aggstate->curperagg = NULL;
    } else {
        // *resultVal = MakeExpandedObjectReadOnly(pergroupstate->transValue,
        //                                         pergroupstate->transValueIsNull,
        //                                         pertrans->transtypeLen);
        // *resultIsNull = pergroupstate->transValueIsNull;
        result_val = make_expanded_object_read_only(
            pergroupstate.trans_value.clone(),
            pergroupstate.trans_value_is_null,
            transtype_len,
        );
        result_is_null = pergroupstate.trans_value_is_null;
    }

    // MemoryContextSwitchTo(oldContext);
    Ok((result_val, result_is_null))
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
/// Same blockers as [`finalize_aggregate`]: the shared call frame carries args[]
/// (#296) and `FunctionCallInvoke` is ported (fmgr #52), but the serialfn call
/// over `pertrans->serialfn_fcinfo` needs the nodeAgg-owned per-trans serialfn
/// frame (not yet built by the unported `ExecInitAgg`/build_pertrans path) and the
/// AggState-as-Node (#200) reachability. The body stands behind a loud panic until
/// those land.
pub fn finalize_partialaggregate<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    peragg: &AggStatePerAggData<'mcx>,
    pergroupstate: &mut AggStatePerGroupData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];
    let pertrans = pertrans_for(aggstate, peragg.transno);
    let serialfn_oid = pertrans.serialfn_oid;
    let transtype_len = pertrans.transtype_len;

    // oldContext = MemoryContextSwitchTo(
    //     aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

    let result_val;
    let result_is_null;

    // if (OidIsValid(pertrans->serialfn_oid))
    if OidIsValid(serialfn_oid) {
        // Don't call a strict serialization function with NULL input.
        // if (pertrans->serialfn.fn_strict && pergroupstate->transValueIsNull)
        if serialfn_is_strict(aggstate, peragg.transno) && pergroupstate.trans_value_is_null {
            // *resultVal = (Datum) 0; *resultIsNull = true;
            result_val = Datum::null();
            result_is_null = true;
        } else {
            // FunctionCallInfo fcinfo = pertrans->serialfn_fcinfo;
            // fcinfo->args[0].value = MakeExpandedObjectReadOnly(
            //     pergroupstate->transValue, pergroupstate->transValueIsNull,
            //     pertrans->transtypeLen);
            // fcinfo->args[0].isnull = pergroupstate->transValueIsNull;
            // fcinfo->isnull = false;
            let arg0 = make_expanded_object_read_only(
                pergroupstate.trans_value.clone(),
                pergroupstate.trans_value_is_null,
                transtype_len,
            );
            set_serialfn_arg0(aggstate, peragg.transno, arg0, pergroupstate.trans_value_is_null);

            // *resultVal = FunctionCallInvoke(fcinfo);
            // *resultIsNull = fcinfo->isnull;
            let (result, isnull) = invoke_serialfn(aggstate, peragg.transno);
            result_val = result;
            result_is_null = isnull;
        }
    } else {
        // *resultVal = MakeExpandedObjectReadOnly(pergroupstate->transValue,
        //                                         pergroupstate->transValueIsNull,
        //                                         pertrans->transtypeLen);
        // *resultIsNull = pergroupstate->transValueIsNull;
        result_val = make_expanded_object_read_only(
            pergroupstate.trans_value.clone(),
            pergroupstate.trans_value_is_null,
            transtype_len,
        );
        result_is_null = pergroupstate.trans_value_is_null;
    }

    // MemoryContextSwitchTo(oldContext);
    Ok((result_val, result_is_null))
}

/// `&aggstate->pertrans[transno]` — the per-trans state the finalfn / serialfn
/// reads its strictness, collation, transtype, and call frame from.
fn pertrans_for<'a, 'mcx>(
    aggstate: &'a AggStateData<'mcx>,
    transno: i32,
) -> &'a AggStatePerTransData<'mcx> {
    &aggstate
        .pertrans
        .as_ref()
        .expect("finalize: pertrans array built by ExecInitAgg")[transno as usize]
}

/// `ExecEvalExpr(expr, aggstate->ss.ps.ps_ExprContext, &isnull)` for a finalfn
/// direct argument (ordered-set aggregates). The compiled-expression evaluator
/// is the execExpr owner's; no seam carries it into this path yet.
fn exec_eval_expr_direct_arg<'mcx>(
    _aggstate: &AggStateData<'mcx>,
    _expr: &ExprState,
) -> (Datum<'mcx>, bool) {
    panic!(
        "backend-executor-nodeAgg::finalize_aggregate: ExecEvalExpr of a finalfn direct \
         argument is owned by the not-yet-ported execExpr unit; no seam yet"
    );
}

/// `fcinfo->args[i].value = v; fcinfo->args[i].isnull = isnull;` on the
/// `peragg->finalfn.fn_strict` — the finalfn's strictness. Read from the
/// catalog (`proisstrict`) through the lsyscache `func_strict` seam, the same
/// value `fmgr_info` stamps onto `peragg->finalfn.fn_strict`.
fn finalfn_is_strict(peragg: &AggStatePerAggData<'_>) -> PgResult<bool> {
    backend_utils_cache_lsyscache_seams::func_strict::call(peragg.finalfn_oid)
}

/// `result = FunctionCallInvoke(fcinfo)` for the finalfn; returns `(result,
/// fcinfo->isnull)`. The finalfn is re-resolved and dispatched by OID through
/// the fmgr `function_call_invoke_datum` seam (the same by-OID call frame the
/// transition path uses), under the aggregate's input collation. The `(void *)
/// aggstate` fcinfo context is the deferred K2 re-sign (the count/min/max/sum/avg
/// finalfns do not read it). A null arg rides the canonical NULL `Datum`; the
/// strict short-circuit is applied by the caller.
fn invoke_finalfn<'mcx>(
    finalfn_oid: Oid,
    collation: Oid,
    args: alloc::vec::Vec<Datum<'mcx>>,
    arg_isnull: alloc::vec::Vec<bool>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let mcx = estate.es_query_cxt;
    // The finalfn takes its args by value: an `internal`-transtype aggregate's
    // `args[0]` is a `Datum::Internal` box that cannot be cloned out of a
    // borrow, so it crosses by move through the by-value dispatch (the same form
    // the transition path uses). `arg_isnull[i]` carries `PG_ARGISNULL(i)`.
    backend_utils_fmgr_fmgr_seams::function_call_invoke_datum_owned::call(
        mcx,
        finalfn_oid,
        collation,
        args,
        arg_isnull,
        None,
    )
}

/// `pertrans->serialfn.fn_strict` — the serialfn's strictness from its resolved
/// `FmgrInfo`. `FmgrInfo.fn_strict` IS modeled (fmgr #52); the blocker is reaching
/// the per-trans serialfn FmgrInfo on the nodeAgg-owned `AggStatePerTransData`,
/// set up by the unported `ExecInitAgg`/build path.
fn serialfn_is_strict<'mcx>(_aggstate: &AggStateData<'mcx>, _transno: i32) -> bool {
    panic!(
        "backend-executor-nodeAgg::finalize_partialaggregate: FmgrInfo.fn_strict is \
         modeled (fmgr #52), but the per-trans serialfn FmgrInfo \
         (AggStatePerTransData.serialfn) is not yet populated by the unported \
         ExecInitAgg/build_pertrans path"
    );
}

/// `fcinfo->args[0].value = v; fcinfo->args[0].isnull = isnull; fcinfo->isnull
/// = false;` on `pertrans->serialfn_fcinfo`. The shared call frame carries args[]
/// (#296); the blocker is the nodeAgg-owned per-trans serialfn_fcinfo frame, not
/// yet built/populated by the unported ExecInitAgg/build path.
fn set_serialfn_arg0<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _transno: i32,
    _value: Datum<'_>,
    _isnull: bool,
) {
    panic!(
        "backend-executor-nodeAgg::finalize_partialaggregate: the shared call frame \
         carries args[] (#296), but the nodeAgg-owned per-trans serialfn_fcinfo \
         (AggStatePerTransData.serialfn_fcinfo) is not yet built/populated by the \
         unported ExecInitAgg/build_pertrans path"
    );
}

/// `FunctionCallInvoke(pertrans->serialfn_fcinfo)`; returns `(result,
/// fcinfo->isnull)`. `FunctionCallInvoke` is ported (fmgr-core #52) and the shared
/// call frame carries args[] (#296); the block is the nodeAgg-owned per-trans
/// serialfn_fcinfo frame, not yet built/populated by the unported ExecInitAgg path.
fn invoke_serialfn<'mcx>(_aggstate: &mut AggStateData<'mcx>, _transno: i32) -> (Datum<'mcx>, bool) {
    panic!(
        "backend-executor-nodeAgg::finalize_partialaggregate: FunctionCallInvoke is \
         ported (fmgr-core #52) and the shared call frame carries args[] (#296), but \
         the nodeAgg-owned per-trans serialfn_fcinfo (AggStatePerTransData.serialfn_fcinfo) \
         is not yet built/populated by the unported ExecInitAgg/build_pertrans path"
    );
}

/// `MakeExpandedObjectReadOnly(d, isnull, typlen)` (utils/expandeddatum.h):
/// returns `d` unchanged for a NULL, a non-expanded datum, or a pass-by-value
/// type; for a read-write expanded datum it returns a read-only pointer to the
/// same object. The expanded-datum machinery (`utils/adt/expandeddatum.c`) is
/// not yet ported and carries no seam, so only the trivial cases are handled
/// here and the expanded-pointer case panics until that owner lands.
fn make_expanded_object_read_only<'mcx>(d: Datum<'mcx>, isnull: bool, typlen: i16) -> Datum<'mcx> {
    // C: `if (isnull || typlen != -1 || !VARATT_IS_EXTERNAL_EXPANDED_RW(d))
    //     return d;` — only a read-write *expanded* varlena is copied to a
    // read-only pointer; a NULL, a fixed-length datum, or an ordinary (flat)
    // varlena passes through unchanged.
    if isnull || typlen != -1 {
        return d;
    }
    // The owned `Datum` model only produces a read-write expanded object via
    // the `Expanded` arm (`VARATT_IS_EXTERNAL_EXPANDED_RW`). A plain `ByRef`
    // varlena is a flat value — never an expanded RW object — so it returns
    // unchanged, exactly as C's `MakeExpandedObjectReadOnly` does for a
    // non-expanded varlena. Only a genuine `Expanded` datum would need the
    // expandeddatum.c read-only copy (its target context is the per-tuple
    // ExprContext, the EcxtId carrier gap shared with ExecInitAgg); the
    // aggregate transition/final functions ported so far never produce one.
    match &d {
        Datum::Expanded(_) => panic!(
            "backend-executor-nodeAgg::finalize: MakeExpandedObjectReadOnly on a read-write \
             expanded datum needs the per-tuple ExprContext Mcx (the EcxtId carrier gap \
             shared with ExecInitAgg); no agg-supplied transfn/finalfn produces one yet"
        ),
        _ => d,
    }
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
        let slot_empty = estate.slot(slot).is_empty();
        if slot_empty {
            backend_executor_execTuples_seams::exec_store_all_null_tuple::call(estate, slot)?;
        } else if aggstate.all_grouped_cols.is_some() {
            // all_grouped_cols is arranged in desc order. Force every column
            // referenced for OTHER grouping sets (not in this set's
            // grouped_cols) to NULL.
            //   slot_getsomeattrs(slot, linitial_int(all_grouped_cols));
            //   foreach lc in all_grouped_cols:
            //     if (!bms_is_member(attnum, grouped_cols))
            //         slot->tts_isnull[attnum - 1] = true;
            let mut cols =
                backend_executor_execTuples_seams::slot_getallattrs_by_id::call(estate, slot)?;
            let grouped_cols = aggstate
                .grouped_cols
                .as_ref()
                .map(|b| &**b);
            // all_grouped_cols snapshot.
            let all_gc: alloc::vec::Vec<i32> = aggstate
                .all_grouped_cols
                .as_ref()
                .expect("all_grouped_cols")
                .iter()
                .copied()
                .collect();
            for attnum in all_gc {
                if !backend_nodes_core_seams::bms_is_member::call(attnum, grouped_cols) {
                    // slot->tts_isnull[attnum - 1] = true;
                    cols[(attnum - 1) as usize].1 = true;
                }
            }
            // Re-store the adjusted virtual tuple.
            let mut values =
                mcx::vec_with_capacity_in(estate.es_query_cxt, cols.len())?;
            let mut isnull = mcx::vec_with_capacity_in(estate.es_query_cxt, cols.len())?;
            for c in cols.iter() {
                values.push(c.0.clone());
                isnull.push(c.1);
            }
            backend_executor_execTuples_seams::store_virtual_values::call(
                estate,
                slot,
                values.as_slice(),
                isnull.as_slice(),
            )?;
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
    pergroup: &mut [AggStatePerGroupData<'mcx>],
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
                process_ordered_aggregate_single(aggstate, pertrans, pergroupstate, estate)?;
            } else {
                process_ordered_aggregate_multi(aggstate, pertrans, pergroupstate, estate)?;
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
                backend_executor_execTuples_seams::exec_clear_tuple::call(estate, uniqslot)?;
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
            finalize_aggregate(aggstate, peragg, pergroupstate, estate)?
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
    let passed = match aggstate.ss.ps.qual.as_mut() {
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
