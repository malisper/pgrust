//! Transition family: initializing per-group transition state and advancing
//! it. Covers the simple transfn driver and the ordered/distinct paths that
//! feed sorted input through the transition function.

use mcx::{alloc_in, Mcx};
use types_error::PgResult;
use crate::aggstate::{AggStateData, AggStatePerGroupData, AggStatePerTransData};
use types_nodes::EStateData;
use types_nodes::fmgr::FunctionCallInfoBaseData;

use crate::node_lifecycle::select_current_set;

/// `initialize_aggregate(aggstate, pertrans, pergroupstate)` — (re)initialize
/// one transition value to its initial state (or NULL), creating per-set sort
/// objects for DISTINCT/ORDER BY aggregates.
///
/// ```c
/// static void
/// initialize_aggregate(AggState *aggstate, AggStatePerTrans pertrans,
///                      AggStatePerGroup pergroupstate)
/// ```
pub fn initialize_aggregate<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    pertrans: &mut AggStatePerTransData<'mcx>,
    pergroupstate: &mut AggStatePerGroupData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // Start a fresh sort operation for each DISTINCT/ORDER BY aggregate.
    if pertrans.aggsortrequired {
        let current_set = aggstate.current_set as usize;

        // In case of rescan, maybe there could be an uncompleted sort
        // operation? Clean it up if so.
        //   if (pertrans->sortstates[aggstate->current_set])
        //       tuplesort_end(pertrans->sortstates[aggstate->current_set]);
        if let Some(sortstates) = pertrans.sortstates.as_mut() {
            if let Some(state) = sortstates[current_set].take() {
                backend_utils_sort_tuplesort_seams::tuplesort_end::call(state)?;
            }
        }

        // We use a plain Datum sorter when there's a single input column;
        // otherwise sort the full tuple. (See comments for
        // process_ordered_aggregate_single.)
        let new_state = if pertrans.num_inputs == 1 {
            // Form_pg_attribute attr = TupleDescAttr(pertrans->sortdesc, 0);
            let sortdesc = pertrans
                .sortdesc
                .as_ref()
                .expect("initialize_aggregate: sortdesc not built");
            let attr = &sortdesc.attrs[0];
            let sort_operators = pertrans
                .sort_operators
                .as_ref()
                .expect("initialize_aggregate: sortOperators not built");
            let sort_collations = pertrans
                .sort_collations
                .as_ref()
                .expect("initialize_aggregate: sortCollations not built");
            let sort_nulls_first = pertrans
                .sort_nulls_first
                .as_ref()
                .expect("initialize_aggregate: sortNullsFirst not built");
            // tuplesort_begin_datum(attr->atttypid, sortOperators[0],
            //   sortCollations[0], sortNullsFirst[0], work_mem, NULL,
            //   TUPLESORT_NONE)
            backend_utils_sort_tuplesort_seams::tuplesort_begin_datum::call(
                mcx,
                attr.atttypid,
                sort_operators[0],
                sort_collations[0],
                sort_nulls_first[0],
                work_mem(),
                TUPLESORT_NONE,
            )?
        } else {
            let sortdesc = pertrans
                .sortdesc
                .as_ref()
                .expect("initialize_aggregate: sortdesc not built");
            let sort_col_idx = pertrans
                .sort_col_idx
                .as_ref()
                .expect("initialize_aggregate: sortColIdx not built");
            let sort_operators = pertrans
                .sort_operators
                .as_ref()
                .expect("initialize_aggregate: sortOperators not built");
            let sort_collations = pertrans
                .sort_collations
                .as_ref()
                .expect("initialize_aggregate: sortCollations not built");
            let sort_nulls_first = pertrans
                .sort_nulls_first
                .as_ref()
                .expect("initialize_aggregate: sortNullsFirst not built");
            // tuplesort_begin_heap(sortdesc, numSortCols, sortColIdx,
            //   sortOperators, sortCollations, sortNullsFirst, work_mem, NULL,
            //   TUPLESORT_NONE)
            backend_utils_sort_tuplesort_seams::tuplesort_begin_heap::call(
                mcx,
                sortdesc,
                pertrans.num_sort_cols,
                sort_col_idx,
                sort_operators,
                sort_collations,
                sort_nulls_first,
                work_mem(),
                TUPLESORT_NONE,
            )?
        };

        // The Tuplesortstate * lives in the aggregate context (`mcx`).
        let new_state = alloc_in(mcx, new_state)?;
        let sortstates = pertrans
            .sortstates
            .as_mut()
            .expect("initialize_aggregate: sortstates not allocated");
        sortstates[current_set] = Some(new_state);
    }

    // (Re)set transValue to the initial value.
    //
    // Note that when the initial value is pass-by-ref, we must copy it (into
    // the aggcontext) since we will pfree the transValue later.
    if pertrans.init_value_is_null {
        pergroupstate.trans_value = pertrans.init_value.clone();
    } else {
        // oldContext = MemoryContextSwitchTo(
        //   aggstate->curaggcontext->ecxt_per_tuple_memory);
        // pergroupstate->transValue = datumCopy(pertrans->initValue,
        //   pertrans->transtypeByVal, pertrans->transtypeLen);
        // MemoryContextSwitchTo(oldContext);
        curaggcontext_assert_built(aggstate);
        // datumCopy(pertrans->initValue, transtypeByVal, transtypeLen) into the
        // curaggcontext per-tuple memory. By-value is verbatim; by-reference
        // (e.g. avg's `{0,0}` int8[] initial value) is deep-copied via the
        // datum.c seam into the per-query context (see datum_copy_into_ecxt).
        pergroupstate.trans_value = if pertrans.transtype_by_val {
            pertrans.init_value.clone()
        } else {
            backend_utils_adt_datum_seams::datum_copy_v::call(
                mcx,
                &pertrans.init_value,
                pertrans.transtype_by_val,
                pertrans.transtype_len as i32,
            )?
        };
    }
    pergroupstate.trans_value_is_null = pertrans.init_value_is_null;

    // If the initial value for the transition state doesn't exist in the
    // pg_aggregate table then we will let the first non-NULL value returned
    // from the outer procNode become the initial value. The noTransValue flag
    // signals that we still need to do this.
    pergroupstate.no_trans_value = pertrans.init_value_is_null;

    Ok(())
}

/// `initialize_aggregates(aggstate, pergroups, numReset)` — initialize all
/// aggregate transition values for the first `numReset` grouping sets.
///
/// ```c
/// static void
/// initialize_aggregates(AggState *aggstate, AggStatePerGroup *pergroups,
///                       int numReset)
/// ```
///
/// NB: This cannot be used for hash aggregates, as for those the grouping set
/// number has to be specified from further up.
pub fn initialize_aggregates<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    pergroups: &mut [Option<mcx::PgVec<'mcx, AggStatePerGroupData<'mcx>>>],
    num_reset: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // int numGroupingSets = Max(aggstate->phase->numsets, 1);
    let num_grouping_sets = std::cmp::max(current_phase_numsets(aggstate), 1);
    let num_trans = aggstate.numtrans;

    // if (numReset == 0) numReset = numGroupingSets;
    let num_reset = if num_reset == 0 {
        num_grouping_sets
    } else {
        num_reset
    };

    for setno in 0..num_reset {
        select_current_set(aggstate, setno, false);

        // The pertrans array and the pergroup for this set are disjoint
        // borrows; the C aliases neither destructively. We take the pertrans
        // out of the AggState for the duration of the inner loop so both can
        // be borrowed mutably at once (mirrors the two raw pointers in C).
        let mut transstates = aggstate
            .pertrans
            .take()
            .expect("initialize_aggregates: pertrans not built");

        let pergroup = pergroups[setno as usize]
            .as_mut()
            .expect("initialize_aggregates: pergroup for set not allocated");

        for transno in 0..num_trans as usize {
            let pertrans = &mut transstates[transno];
            let pergroupstate = &mut pergroup[transno];
            initialize_aggregate(aggstate, pertrans, pergroupstate, mcx)?;
        }

        aggstate.pertrans = Some(transstates);
    }

    Ok(())
}

/// `advance_transition_function(aggstate, pertrans, pergroupstate)` — call the
/// transition function once for the values already loaded in
/// `pertrans->transfn_fcinfo`, handling strictness and the initial
/// non-NULL-input substitution.
///
/// ```c
/// static void
/// advance_transition_function(AggState *aggstate, AggStatePerTrans pertrans,
///                             AggStatePerGroup pergroupstate)
/// ```
pub fn advance_transition_function<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    pertrans: &mut AggStatePerTransData<'mcx>,
    pergroupstate: &mut AggStatePerGroupData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
    //
    // This is the ordered/distinct drain path (process_ordered_aggregate_*).
    // The single-input drain loads the just-fetched sorted column into C's
    // `fcinfo->args[1]`; the owned model carries that by-ref-faithful value on
    // `pertrans->distinct_value` / `distinct_value_isnull` (see `fcinfo_set_arg`
    // / the per-trans frame doc). Only `args[1]` is modeled here (single column);
    // the multi-column variant is gated separately on slot deform.
    let _ = pertrans.transfn_fcinfo.as_ref();

    if transfn_is_strict(pertrans) {
        // For a strict transfn, nothing happens when there's a NULL input; we
        // just keep the prior transValue.
        let num_trans_inputs = pertrans.num_trans_inputs;

        // for (i = 1; i <= numTransInputs; i++)
        //     if (fcinfo->args[i].isnull) return;
        for i in 1..=num_trans_inputs {
            if fcinfo_arg_isnull(pertrans, i) {
                return Ok(());
            }
        }

        if pergroupstate.no_trans_value {
            // transValue has not been initialized. This is the first non-NULL
            // input value. We use it as the initial value for transValue. (We
            // already checked that the agg's input type is binary-compatible
            // with its transtype, so straight copy here is OK.)
            //
            // oldContext = MemoryContextSwitchTo(
            //   aggstate->curaggcontext->ecxt_per_tuple_memory);
            // pergroupstate->transValue = datumCopy(fcinfo->args[1].value,
            //   pertrans->transtypeByVal, pertrans->transtypeLen);
            let arg1_value = fcinfo_arg_value(pertrans, 1);
            let aggcontext = curaggcontext_ecxt(aggstate);
            pergroupstate.trans_value = datum_copy_into_ecxt(
                arg1_value,
                pertrans.transtype_by_val,
                pertrans.transtype_len,
                aggcontext,
                estate,
            )?;
            pergroupstate.trans_value_is_null = false;
            pergroupstate.no_trans_value = false;
            return Ok(());
        }

        if pergroupstate.trans_value_is_null {
            // Don't call a strict function with NULL inputs. Note it is
            // possible to get here despite the above tests, if the transfn is
            // strict *and* returned a NULL on a prior cycle. If that happens
            // we will propagate the NULL all the way to the end.
            return Ok(());
        }
    }

    // We run the transition functions in per-input-tuple memory context
    //   oldContext = MemoryContextSwitchTo(
    //     aggstate->tmpcontext->ecxt_per_tuple_memory);

    // set up aggstate->curpertrans for AggGetAggref()
    //   aggstate->curpertrans = pertrans;  (model: index; preserved by caller)

    // OK to call the transition function:
    //   fcinfo->args[0].value = pergroupstate->transValue;
    //   fcinfo->args[0].isnull = pergroupstate->transValueIsNull;
    //   fcinfo->isnull = false;
    //   newVal = FunctionCallInvoke(fcinfo);
    //   aggstate->curpertrans = NULL;
    //
    // The transfn is dispatched through the owned by-value seam
    // (`invoke_transfn` -> `function_call_invoke_datum_owned`), which moves the
    // running state (a `Datum::Internal` box for `internal`-transtype aggregates)
    // through the fcinfo side channel and stamps the live-AggState `fcinfo.context`
    // tag (K1/K2: `AggCheckCallContext` reaches back via the call-context channel).
    // arg[1] is the single ordered/distinct input on `distinct_value`.
    let trans_value = pergroupstate.trans_value.clone();
    let trans_value_is_null = pergroupstate.trans_value_is_null;
    let input_args = [pertrans.distinct_value.clone()];
    let input_args_null = [pertrans.distinct_value_isnull];
    let mcx = estate.es_query_cxt;
    let (new_val, isnull) = invoke_transfn(
        pertrans,
        trans_value.clone(),
        trans_value_is_null,
        &input_args,
        &input_args_null,
        mcx,
    )?;

    //   if (!pertrans->transtypeByVal &&
    //       DatumGetPointer(newVal) != DatumGetPointer(pergroupstate->transValue))
    //       newVal = ExecAggCopyTransValue(...);
    //   pergroupstate->transValue = newVal;
    //   pergroupstate->transValueIsNull = fcinfo->isnull;
    let transtype_by_val = pertrans.transtype_by_val;
    let transtype_len = pertrans.transtype_len;
    let aggcontext = curaggcontext_ecxt(aggstate);
    let new_val = if !transtype_by_val && !datum_ptr_eq(&new_val, &trans_value) {
        ExecAggCopyTransValue(
            aggstate,
            aggstate.curpertrans.max(0) as usize,
            new_val,
            isnull,
            trans_value,
            trans_value_is_null,
            transtype_by_val,
            transtype_len,
            aggcontext,
            estate,
        )?
    } else {
        new_val
    };
    pergroupstate.trans_value = new_val;
    pergroupstate.trans_value_is_null = isnull;
    Ok(())
}

/// `advance_aggregates(aggstate)` — run the compiled `evaltrans` expression
/// for the current input tuple, advancing every aggregate's transition value
/// for every active grouping set.
///
/// ```c
/// static void
/// advance_aggregates(AggState *aggstate)
/// {
///     ExecEvalExprNoReturnSwitchContext(aggstate->phase->evaltrans,
///                                       aggstate->tmpcontext);
/// }
/// ```
pub fn advance_aggregates<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExecEvalExprNoReturnSwitchContext(aggstate->phase->evaltrans,
    //                                   aggstate->tmpcontext);
    //
    // The compiled evaltrans ExprState lives on the current phase; it is run
    // (no result) in the tmpcontext's per-tuple memory. ExecEvalExpr* is owned
    // by the not-yet-ported execExpr unit and reached through its seam.
    let phase = aggstate.phase as usize;
    let tmpcontext = tmpcontext_ecxt(aggstate);
    let phases = aggstate
        .phases
        .as_mut()
        .expect("advance_aggregates: phases not built");
    let evaltrans = phases[phase]
        .evaltrans
        .as_mut()
        .expect("advance_aggregates: phase->evaltrans not compiled");
    backend_executor_execExpr_seams::exec_eval_expr_switch_context::call(
        evaltrans, tmpcontext, estate,
    )?;
    Ok(())
}

// ===========================================================================
// EEOP_AGG_PLAIN_TRANS_* runtime support (execExprInterp.c) — the per-row
// transition-step bodies the compiled `evaltrans` interpreter drives. These
// mutate `aggstate->all_pergroups[setoff][transno]` from inside the
// interpreter (reached through `state->parent`), exactly as C's
// ExecAggInitGroup / ExecAggPlainTransByVal / ExecAggPlainTransByRef do. They
// live here (the AggState owner) and are called directly by
// `backend-executor-execExprInterp` (which depends on this crate, as
// `ExecEvalGroupingFunc` already does).
// ===========================================================================

/// The canonical unified `Datum` (Datum-unification keystone).
type AggDatum<'mcx> = types_tuple::backend_access_common_heaptuple::Datum<'mcx>;

/// `transval = FunctionCallInvoke(pertrans->transfn_fcinfo)` over the unified
/// `Datum` lane — invoke the transition function with `args[0] = transValue`
/// (the running per-group value) followed by the per-row `input_args`
/// (`fcinfo->args[1..]`). The resolved `FmgrInfo` cannot cross the fmgr seam,
/// so the owner re-resolves by `transfn.fn_oid` under `aggCollation`. Returns
/// `(newVal, isnull)`.
///
/// The fcinfo context (`(Node *) aggstate`) the C frame carries is the K1
/// `AggStateContextLink` channel installed at build time; the by-OID re-dispatch
/// seam does not thread it (the deferred K2 re-sign), which only affects transfns
/// that call `AggCheckCallContext`/`AggGetAggref` — the count/min/max/sum/avg
/// transfns do not.
fn invoke_transfn<'mcx>(
    pertrans: &AggStatePerTransData<'mcx>,
    trans_value: AggDatum<'mcx>,
    trans_value_is_null: bool,
    input_args: &[AggDatum<'mcx>],
    input_args_null: &[bool],
    mcx: Mcx<'mcx>,
) -> PgResult<(AggDatum<'mcx>, bool)> {
    // fcinfo->args[0] = transValue (a NULL one is the canonical null Datum);
    // args[1..] = the per-row inputs the compiler evaluated into the arg cells.
    // `args_null` carries each `fcinfo->args[i].isnull` (the canonical word
    // cannot encode SQL NULL): a transition function reads `PG_ARGISNULL(0)` to
    // detect the first call (NULL running state) and `PG_ARGISNULL(i)` per input.
    let mut args: alloc::vec::Vec<AggDatum<'mcx>> =
        alloc::vec::Vec::with_capacity(1 + input_args.len());
    let mut args_null: alloc::vec::Vec<bool> =
        alloc::vec::Vec::with_capacity(1 + input_args.len());
    args.push(trans_value);
    args_null.push(trans_value_is_null);
    for (i, a) in input_args.iter().enumerate() {
        args.push(a.clone());
        args_null.push(input_args_null.get(i).copied().unwrap_or(false));
    }
    // C: `pertrans->transfn_fcinfo->flinfo` is `&pertrans->transfn`, onto which
    // `build_pertrans_for_aggref` ran `fmgr_info_set_expr((Node*) transfnexpr,
    // ...)`. Thread that call node so a polymorphic transition function reads its
    // declared arg/result types (`get_fn_expr_*`). The by-OID re-resolution drops
    // it otherwise.
    let fn_expr = pertrans.transfn.fn_expr.clone();
    // K2 (the live-AggState fcinfo->context across the by-OID re-dispatch): C
    // sets `fcinfo->context = (Node *) aggstate`. The rich frame
    // `pertrans->transfn_fcinfo` carries that as the K1 `AggStateContextLink`;
    // the by-OID `function_call_invoke` seam builds a fresh callee frame inside
    // fmgr-core and can't be handed the link directly, so deposit it on the
    // thread-local channel (RAII-scoped to this one dispatch) — fmgr-core's
    // `init_fcinfo` takes it back onto the callee frame, where a transfn that
    // calls `AggCheckCallContext`/`AggGetAggref`/`AggStateIsShared` recovers the
    // AggState. Plain count/min/max/sum/avg transfns ignore it.
    let _agg_ctx_guard = agg_call_context_guard(pertrans.transfn_fcinfo.as_deref());
    // By-value dispatch: arg 0 may be a `Datum::Internal` (the running
    // aggregate state for an `internal`-transtype aggregate), whose owned
    // `Box<dyn Any>` cannot be cloned out of a borrow. The owned seam moves it
    // into the fmgr by-reference side channel and back out as the result.
    backend_utils_fmgr_fmgr_seams::function_call_invoke_datum_owned::call(
        mcx,
        pertrans.transfn.fn_oid,
        pertrans.agg_collation,
        args,
        args_null,
        fn_expr,
    )
}

/// Deposit the aggregate-call back-pointer (C `fcinfo->context = (Node *)
/// aggstate`) on the fmgr thread-local channel for the about-to-be-issued
/// transfn/finalfn dispatch, returning the RAII guard that clears it after the
/// call. Reads the `AggStateContextLink` off the rich `*_fcinfo` frame the
/// executor built at `ExecInitAgg` time (`new_agg_fcinfo`); `None` (no frame /
/// non-Agg context) deposits nothing, exactly as a plain call.
pub(crate) fn agg_call_context_guard(
    fcinfo: Option<&FunctionCallInfoBaseData<'_>>,
) -> Option<types_fmgr::fmgr::AggCallContextGuard> {
    let link = match fcinfo.and_then(|fc| fc.context.as_ref()) {
        Some(types_nodes::fmgr::FmgrCallContext::Agg(link)) => *link,
        _ => return None,
    };
    let (data, vtable) = link.to_raw();
    Some(types_fmgr::fmgr::AggCallContextGuard::install(
        types_fmgr::fmgr::RawAggContextLink { data, vtable },
    ))
}

/// `ExecAggInitGroup(aggstate, pertrans, pergroup, aggcontext)`
/// (execExprInterp.c:5616) — first-non-NULL-input initialization of a group's
/// transition value: copy `fcinfo->args[1].value` (here `input_args[0]`) into
/// the aggcontext (by-ref) and clear the NULL/noTrans flags.
pub fn ExecAggInitGroup<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    transno: usize,
    setoff: usize,
    aggcontext: Option<types_nodes::execnodes::EcxtId>,
    input_args: &[AggDatum<'mcx>],
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let (transtype_by_val, transtype_len) = {
        let pertrans = pertrans_ref(aggstate, transno);
        (pertrans.transtype_by_val, pertrans.transtype_len)
    };
    let arg1 = input_args
        .first()
        .cloned()
        .expect("ExecAggInitGroup: transfn args[1] missing (numTransInputs must be >= 1)");
    let copied = datum_copy_into_ecxt(arg1, transtype_by_val, transtype_len, aggcontext, estate)?;
    let pergroup = pergroup_mut(aggstate, setoff, transno);
    pergroup.trans_value = copied;
    pergroup.trans_value_is_null = false;
    pergroup.no_trans_value = false;
    Ok(())
}

/// `ExecAggPlainTransByVal(aggstate, pertrans, pergroup, aggcontext, setno)`
/// (execExprInterp.c:5836) — by-value transition: invoke the transfn with the
/// current transValue + inputs and store the new value back. No by-ref copy.
pub fn ExecAggPlainTransByVal<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    transno: usize,
    setoff: usize,
    setno: i32,
    _aggcontext: Option<types_nodes::execnodes::EcxtId>,
    input_args: &[AggDatum<'mcx>],
    input_args_null: &[bool],
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // cf. select_current_set(): aggstate->curaggcontext = aggcontext (= setno for
    // the non-hash path); aggstate->current_set = setno; curpertrans = pertrans.
    aggstate.curaggcontext = setno;
    aggstate.current_set = setno;
    aggstate.curpertrans = transno as i32;

    // MOVE the running value out (replaced immediately below): the `internal`
    // pseudo-type transition state is a `Datum::Internal` box that cannot be
    // cloned (C passes it by pointer); a by-value scalar move is identical to a
    // copy. Internal-transtype aggregates compile to BYVAL (internal is
    // pass-by-value), so this path — not ByRef — carries them.
    let (trans_value, trans_value_is_null) = {
        let pg = pergroup_mut(aggstate, setoff, transno);
        (
            core::mem::replace(&mut pg.trans_value, AggDatum::null()),
            pg.trans_value_is_null,
        )
    };
    let mcx = estate.es_query_cxt;
    let (new_val, isnull) = {
        let pertrans = pertrans_ref(aggstate, transno);
        invoke_transfn(pertrans, trans_value, trans_value_is_null, input_args, input_args_null, mcx)?
    };
    let pergroup = pergroup_mut(aggstate, setoff, transno);
    pergroup.trans_value = new_val;
    pergroup.trans_value_is_null = isnull;
    aggstate.curpertrans = -1;
    Ok(())
}

/// `ExecAggPlainTransByRef(aggstate, pertrans, pergroup, aggcontext, setno)`
/// (execExprInterp.c:5868) — by-reference transition: like the by-value form,
/// but the returned datum must be copied into the aggcontext (and the prior
/// transValue freed) unless the transfn returned its own input pointer.
pub fn ExecAggPlainTransByRef<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    transno: usize,
    setoff: usize,
    setno: i32,
    aggcontext: Option<types_nodes::execnodes::EcxtId>,
    input_args: &[AggDatum<'mcx>],
    input_args_null: &[bool],
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    aggstate.curaggcontext = setno;
    aggstate.current_set = setno;
    aggstate.curpertrans = transno as i32;

    let mcx = estate.es_query_cxt;

    // C `internal`-pseudo-type transition state (e.g. NumericAggState,
    // ArrayBuildState): the running value is a `Datum::Internal` box that cannot
    // be cloned. The transfn mutates the state in place and returns the same
    // pointer, so C's `newVal == pergroupstate->transValue` reparent fast path is
    // always taken (no datumCopy). MOVE the state out (it is NULL on the first
    // call, before any state has been built), invoke, and store the returned
    // state back — no clone, no copy-into-aggcontext, no pointer comparison
    // (`Datum::Internal` is not comparable, as C never compares an internal).
    let is_internal = pertrans_ref(aggstate, transno).aggtranstype == INTERNALOID;
    if is_internal {
        let (trans_value, trans_value_is_null) = {
            let pg = pergroup_mut(aggstate, setoff, transno);
            (
                core::mem::replace(&mut pg.trans_value, AggDatum::null()),
                pg.trans_value_is_null,
            )
        };
        let (new_val, isnull) = {
            let pertrans = pertrans_ref(aggstate, transno);
            invoke_transfn(pertrans, trans_value, trans_value_is_null, input_args, input_args_null, mcx)?
        };
        let pergroup = pergroup_mut(aggstate, setoff, transno);
        pergroup.trans_value = new_val;
        pergroup.trans_value_is_null = isnull;
        aggstate.curpertrans = -1;
        return Ok(());
    }

    let (trans_value, trans_value_is_null) = {
        let pg = pergroup_ref(aggstate, setoff, transno);
        (pg.trans_value.clone(), pg.trans_value_is_null)
    };
    let (transtype_by_val, transtype_len) = {
        let pertrans = pertrans_ref(aggstate, transno);
        (pertrans.transtype_by_val, pertrans.transtype_len)
    };
    let (new_val, isnull) = {
        let pertrans = pertrans_ref(aggstate, transno);
        invoke_transfn(
            pertrans,
            trans_value.clone(),
            trans_value_is_null,
            input_args,
            input_args_null,
            mcx,
        )?
    };

    // For pass-by-ref, must copy the new value into aggcontext and free the prior
    // transValue. But if the transfn returned a pointer to its own input, do
    // nothing. The owned `Datum` enum compares by value image.
    let new_val = if !datum_ptr_eq(&new_val, &trans_value) {
        ExecAggCopyTransValue(
            aggstate,
            transno,
            new_val,
            isnull,
            trans_value,
            trans_value_is_null,
            transtype_by_val,
            transtype_len,
            aggcontext,
            estate,
        )?
    } else {
        new_val
    };

    let pergroup = pergroup_mut(aggstate, setoff, transno);
    pergroup.trans_value = new_val;
    pergroup.trans_value_is_null = isnull;
    aggstate.curpertrans = -1;
    Ok(())
}

/// `ExecAggCopyTransValue(...)` (execExprInterp.c:5668) — ensure the new
/// transition value lives in the aggcontext (copy it there) and free the prior
/// value. The expanded-object R/W fast paths are not modeled (the owned `Datum`
/// carries no expanded-object handle); we copy non-NULL new values and drop the
/// old (the owned-context GC handles the free).
#[allow(clippy::too_many_arguments)]
pub fn ExecAggCopyTransValue<'mcx>(
    _aggstate: &mut AggStateData<'mcx>,
    _transno: usize,
    new_value: AggDatum<'mcx>,
    new_value_is_null: bool,
    _old_value: AggDatum<'mcx>,
    _old_value_is_null: bool,
    transtype_by_val: bool,
    transtype_len: i16,
    aggcontext: Option<types_nodes::execnodes::EcxtId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<AggDatum<'mcx>> {
    if new_value_is_null {
        return Ok(AggDatum::null());
    }
    datum_copy_into_ecxt(new_value, transtype_by_val, transtype_len, aggcontext, estate)
}

/// `process_ordered_aggregate_single(aggstate, pertrans, pergroupstate)` — run
/// the transition function over the sorted single-column input of a
/// DISTINCT/ORDER BY aggregate, eliminating duplicates when DISTINCT.
///
/// ```c
/// static void
/// process_ordered_aggregate_single(AggState *aggstate,
///                                  AggStatePerTrans pertrans,
///                                  AggStatePerGroup pergroupstate)
/// ```
pub fn process_ordered_aggregate_single<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    pertrans: &mut AggStatePerTransData<'mcx>,
    pergroupstate: &mut AggStatePerGroupData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Datum oldVal = (Datum) 0; bool oldIsNull = true; bool haveOldVal = false;
    // Canonical value (Datum unification): `oldVal` mirrors the canonical
    // `newVal` returned by `tuplesort_getdatum`.
    let mut old_val: types_tuple::backend_access_common_heaptuple::Datum =
        types_tuple::backend_access_common_heaptuple::Datum::null();
    let mut old_is_null = true;
    let mut have_old_val = false;
    // MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
    // bool isDistinct = (pertrans->numDistinctCols > 0);
    let is_distinct = pertrans.num_distinct_cols > 0;
    let mut new_abbrev_val: types_tuple::backend_access_common_heaptuple::Datum =
        types_tuple::backend_access_common_heaptuple::Datum::null();
    let mut old_abbrev_val: types_tuple::backend_access_common_heaptuple::Datum =
        types_tuple::backend_access_common_heaptuple::Datum::null();

    // Assert(pertrans->numDistinctCols < 2);
    debug_assert!(pertrans.num_distinct_cols < 2);

    let current_set = aggstate.current_set as usize;
    // Take the sort out of the per-set array for the duration of the drain so
    // the transition function can borrow `pertrans` mutably; it is dropped
    // (tuplesort_end) and the slot left NULL when we are done.
    let mut state = pertrans
        .sortstates
        .as_mut()
        .expect("process_ordered_aggregate_single: sortstates not built")[current_set]
        .take()
        .expect("process_ordered_aggregate_single: sortstate for set is NULL");

    // tuplesort_performsort(pertrans->sortstates[aggstate->current_set]);
    backend_utils_sort_tuplesort_seams::tuplesort_performsort::call(&mut state)?;

    // Load the column into argument 1 (arg 0 will be transition value):
    //   newVal = &fcinfo->args[1].value; isNull = &fcinfo->args[1].isnull;

    // Note: if input type is pass-by-ref, the datums returned by the sort are
    // freshly palloc'd in the per-query context, so we must be careful to
    // pfree them when they are no longer needed.
    //
    // while (tuplesort_getdatum(..., true, false, newVal, isNull, &newAbbrevVal))
    loop {
        let (found, new_val, new_is_null) =
            backend_utils_sort_tuplesort_seams::tuplesort_getdatum::call(&mut state, true, false)?;
        if !found {
            break;
        }
        // The tuplesort seam does not surface the abbreviated key, so the
        // abbreviated-equality fast path always falls through to equalfnOne.
        new_abbrev_val = types_tuple::backend_access_common_heaptuple::Datum::null();

        // Load the fetched datum into the transfn's argument 1.
        fcinfo_set_arg(pertrans, 1, new_val.clone(), new_is_null);

        // Clear and select the working context for evaluation of the equality
        // function and transition function.
        //   MemoryContextReset(workcontext);
        //   oldContext = MemoryContextSwitchTo(workcontext);
        tmpcontext_reset(aggstate, estate)?;

        // If DISTINCT mode, and not distinct from prior, skip it.
        //   if (isDistinct && haveOldVal &&
        //       ((oldIsNull && *isNull) ||
        //        (!oldIsNull && !*isNull && oldAbbrevVal == newAbbrevVal &&
        //         DatumGetBool(FunctionCall2Coll(&pertrans->equalfnOne,
        //                                        pertrans->aggCollation,
        //                                        oldVal, *newVal)))))
        if is_distinct
            && have_old_val
            && ((old_is_null && new_is_null)
                || (!old_is_null
                    && !new_is_null
                    && old_abbrev_val == new_abbrev_val
                    && equalfn_one_call(pertrans, old_val.clone(), new_val.clone())?))
        {
            // MemoryContextSwitchTo(oldContext); continue;
            continue;
        } else {
            advance_transition_function(aggstate, pertrans, pergroupstate, estate)?;

            // MemoryContextSwitchTo(oldContext);

            // Forget the old value, if any, and remember the new one for
            // subsequent equality checks.
            //   if (!pertrans->inputtypeByVal) {
            //       if (!oldIsNull) pfree(DatumGetPointer(oldVal));
            //       if (!*isNull) oldVal = datumCopy(*newVal,
            //           pertrans->inputtypeByVal, pertrans->inputtypeLen);
            //   } else oldVal = *newVal;
            if !pertrans.inputtype_by_val {
                if !old_is_null {
                    pfree_datum(old_val.clone());
                }
                if !new_is_null {
                    old_val = datum_copy_current(
                        new_val.clone(),
                        pertrans.inputtype_by_val,
                        pertrans.inputtype_len,
                        estate,
                    )?;
                }
            } else {
                old_val = new_val.clone();
            }
            old_abbrev_val = new_abbrev_val;
            old_is_null = new_is_null;
            have_old_val = true;
        }
    }

    // if (!oldIsNull && !pertrans->inputtypeByVal) pfree(DatumGetPointer(oldVal));
    if !old_is_null && !pertrans.inputtype_by_val {
        pfree_datum(old_val);
    }

    // tuplesort_end(pertrans->sortstates[aggstate->current_set]);
    // pertrans->sortstates[aggstate->current_set] = NULL; (already taken out)
    backend_utils_sort_tuplesort_seams::tuplesort_end::call(state)?;

    Ok(())
}

/// `process_ordered_aggregate_multi(aggstate, pertrans, pergroupstate)` — the
/// multi-column variant: drains the per-trans tuplesort, eliminating
/// duplicates with the multi-column equality comparator.
///
/// ```c
/// static void
/// process_ordered_aggregate_multi(AggState *aggstate,
///                                 AggStatePerTrans pertrans,
///                                 AggStatePerGroup pergroupstate)
/// ```
pub fn process_ordered_aggregate_multi<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    pertrans: &mut AggStatePerTransData<'mcx>,
    pergroupstate: &mut AggStatePerGroupData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExprContext *tmpcontext = aggstate->tmpcontext;
    // FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
    // TupleTableSlot *slot1 = pertrans->sortslot;
    // TupleTableSlot *slot2 = pertrans->uniqslot;
    let mut slot1 = pertrans
        .sortslot
        .expect("process_ordered_aggregate_multi: sortslot not built");
    let mut slot2 = pertrans.uniqslot;
    let num_trans_inputs = pertrans.num_trans_inputs;
    let num_distinct_cols = pertrans.num_distinct_cols;
    let mut new_abbrev_val: types_tuple::backend_access_common_heaptuple::Datum =
        types_tuple::backend_access_common_heaptuple::Datum::null();
    let mut old_abbrev_val: types_tuple::backend_access_common_heaptuple::Datum =
        types_tuple::backend_access_common_heaptuple::Datum::null();
    let mut have_old_value = false;
    // TupleTableSlot *save = aggstate->tmpcontext->ecxt_outertuple;
    let save = tmpcontext_outertuple(aggstate, estate);

    let current_set = aggstate.current_set as usize;
    // Take the sort out of the per-set array for the drain (see the single
    // variant); dropped via tuplesort_end and left NULL when done.
    let mut state = pertrans
        .sortstates
        .as_mut()
        .expect("process_ordered_aggregate_multi: sortstates not built")[current_set]
        .take()
        .expect("process_ordered_aggregate_multi: sortstate for set is NULL");

    // tuplesort_performsort(pertrans->sortstates[aggstate->current_set]);
    backend_utils_sort_tuplesort_seams::tuplesort_performsort::call(&mut state)?;

    // ExecClearTuple(slot1); if (slot2) ExecClearTuple(slot2);
    exec_clear_tuple_slot(slot1)?;
    if let Some(s2) = slot2 {
        exec_clear_tuple_slot(s2)?;
    }

    // while (tuplesort_gettupleslot(..., true, true, slot1, &newAbbrevVal))
    loop {
        let found = backend_utils_sort_tuplesort_seams::tuplesort_gettupleslot::call(
            &mut state,
            true,
            true,
            resolve_slot_mut(slot1),
        )?;
        if !found {
            break;
        }
        // The tuplesort seam does not surface the abbreviated key, so the
        // abbreviated-equality fast path always falls through to equalfnMulti.
        new_abbrev_val = types_tuple::backend_access_common_heaptuple::Datum::null();

        // CHECK_FOR_INTERRUPTS();
        backend_tcop_postgres_seams::check_for_interrupts::call()?;

        // tmpcontext->ecxt_outertuple = slot1;
        // tmpcontext->ecxt_innertuple = slot2;
        set_tmpcontext_outer_inner(aggstate, estate, Some(slot1), slot2);

        // if (numDistinctCols == 0 || !haveOldValue ||
        //     newAbbrevVal != oldAbbrevVal ||
        //     !ExecQual(pertrans->equalfnMulti, tmpcontext))
        let qual_distinct = if num_distinct_cols == 0
            || !have_old_value
            || new_abbrev_val != old_abbrev_val
        {
            true
        } else {
            !equalfn_multi_qual(aggstate, pertrans)?
        };

        if qual_distinct {
            // Extract the first numTransInputs columns as datums to pass to
            // the transfn.
            //   slot_getsomeattrs(slot1, numTransInputs);
            //   for (i = 0; i < numTransInputs; i++) {
            //       fcinfo->args[i + 1].value = slot1->tts_values[i];
            //       fcinfo->args[i + 1].isnull = slot1->tts_isnull[i];
            //   }
            load_transfn_args_from_slot(aggstate, pertrans, slot1, num_trans_inputs)?;

            advance_transition_function(aggstate, pertrans, pergroupstate, estate)?;

            if num_distinct_cols > 0 {
                // swap the slot pointers to retain the current tuple
                //   TupleTableSlot *tmpslot = slot2; slot2 = slot1;
                //   slot1 = tmpslot;
                let tmpslot = slot2;
                slot2 = Some(slot1);
                slot1 = tmpslot
                    .expect("process_ordered_aggregate_multi: uniqslot required for DISTINCT");
                // avoid ExecQual() calls by reusing abbreviated keys
                old_abbrev_val = new_abbrev_val;
                have_old_value = true;
            }
        }

        // Reset context each time: ResetExprContext(tmpcontext);
        tmpcontext_reset(aggstate, estate)?;

        // ExecClearTuple(slot1);
        exec_clear_tuple_slot(slot1)?;
    }

    // if (slot2) ExecClearTuple(slot2);
    if let Some(s2) = slot2 {
        exec_clear_tuple_slot(s2)?;
    }

    // tuplesort_end(pertrans->sortstates[aggstate->current_set]);
    // pertrans->sortstates[aggstate->current_set] = NULL; (already taken out)
    backend_utils_sort_tuplesort_seams::tuplesort_end::call(state)?;

    // restore previous slot, potentially in use for grouping sets:
    //   tmpcontext->ecxt_outertuple = save;
    set_tmpcontext_outertuple(aggstate, estate, save);

    Ok(())
}

// ---------------------------------------------------------------------------
// File-scope helpers (in-crate marshaling and the small externals reached
// through their owners' seams)
// ---------------------------------------------------------------------------

/// `&aggstate->pertrans[transno]` — the per-trans state.
fn pertrans_ref<'a, 'mcx>(
    aggstate: &'a AggStateData<'mcx>,
    transno: usize,
) -> &'a AggStatePerTransData<'mcx> {
    &aggstate
        .pertrans
        .as_ref()
        .expect("ExecAggPlainTrans: aggstate->pertrans is NULL")[transno]
}

/// `&aggstate->all_pergroups[setoff][transno]` — the per-group state (shared).
fn pergroup_ref<'a, 'mcx>(
    aggstate: &'a AggStateData<'mcx>,
    setoff: usize,
    transno: usize,
) -> &'a AggStatePerGroupData<'mcx> {
    let set = aggstate
        .all_pergroups
        .as_ref()
        .expect("ExecAggPlainTrans: aggstate->all_pergroups is NULL")[setoff]
        .as_ref()
        .expect("ExecAggPlainTrans: all_pergroups[setoff] is NULL");
    &set[transno]
}

/// `&aggstate->all_pergroups[setoff][transno]` — the per-group state (mut).
fn pergroup_mut<'a, 'mcx>(
    aggstate: &'a mut AggStateData<'mcx>,
    setoff: usize,
    transno: usize,
) -> &'a mut AggStatePerGroupData<'mcx> {
    let set = aggstate
        .all_pergroups
        .as_mut()
        .expect("ExecAggPlainTrans: aggstate->all_pergroups is NULL")[setoff]
        .as_mut()
        .expect("ExecAggPlainTrans: all_pergroups[setoff] is NULL");
    &mut set[transno]
}

/// `pergroup->{noTransValue, transValueIsNull}` for the
/// `EEOP_AGG_PLAIN_TRANS_*` interpreter dispatch.
pub fn agg_pergroup_flags(
    aggstate: &AggStateData<'_>,
    setoff: usize,
    transno: usize,
) -> (bool, bool) {
    let pg = pergroup_ref(aggstate, setoff, transno);
    (pg.no_trans_value, pg.trans_value_is_null)
}

/// `aggstate->all_pergroups[setoff] == NULL` for EEOP_AGG_PLAIN_PERGROUP_NULLCHECK.
pub fn agg_pergroup_allaggs_is_null(aggstate: &AggStateData<'_>, setoff: usize) -> bool {
    match aggstate.all_pergroups.as_ref() {
        Some(v) => v.get(setoff).map(|s| s.is_none()).unwrap_or(true),
        None => true,
    }
}

/// `datumCopy(value, byval, len)` into the aggcontext per-tuple memory.
/// By-value datums are returned verbatim; by-reference datums are deep-copied
/// via the datum.c seam. C switches into `aggcontext->ecxt_per_tuple_memory`
/// before the copy so the by-ref transValue is reset with the grouping set; in
/// the owned model the per-grouping-set context's `Mcx` is borrowed (it lives
/// inside the per-query context) and `datum_copy_v` must return a `Datum<'mcx>`,
/// so the value-lifetime-correct allocation context is the per-query context.
/// For the non-hashed single-group path (the current gate: count/min/max/sum/avg
/// without GROUP BY) the aggcontext and the query context coincide in lifetime,
/// so this is faithful; multi-group hashagg (which resets per group) is gated
/// elsewhere (#324/hashagg).
fn datum_copy_into_ecxt<'mcx>(
    value: AggDatum<'mcx>,
    typ_by_val: bool,
    typ_len: i16,
    aggcontext: Option<types_nodes::execnodes::EcxtId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<AggDatum<'mcx>> {
    if typ_by_val {
        return Ok(value);
    }
    let _ = aggcontext;
    let mcx = estate.es_query_cxt;
    backend_utils_adt_datum_seams::datum_copy_v::call(mcx, &value, typ_by_val, typ_len as i32)
}

/// `DatumGetPointer(a) == DatumGetPointer(b)` — pointer identity of two by-ref
/// datums (the C reparent fast-path test). For the owned `Datum` enum this is a
/// by-ref byte-image identity test; conservatively returning `false` (= "copy")
/// is always safe.
fn datum_ptr_eq(a: &AggDatum<'_>, b: &AggDatum<'_>) -> bool {
    a == b
}

/// `TUPLESORT_NONE` (tuplesort.h) — no extra sort options.
/// `INTERNALOID` (catalog/pg_type_d.h) — the `internal` pseudo-type, whose
/// transition value is a `void *` carried on the canonical `Datum::Internal`.
const INTERNALOID: types_core::Oid = 2281;

const TUPLESORT_NONE: i32 = 0;

/// `work_mem` (guc) — sort working-memory limit, in kilobytes. The GUC is a
/// per-backend global owned by `backend-utils-init-small` (`globals.c`); read
/// it through that unit's installed seam (mirrors `spill.rs`'s
/// `get_hash_memory_limit`).
fn work_mem() -> i32 {
    backend_utils_init_small_seams::work_mem::call()
}

/// `aggstate->curaggcontext->ecxt_per_tuple_memory` — assert the currently
/// selected aggregate context (an index into `aggcontexts`) exists. C switches
/// into this context before the `datumCopy`; for a pass-by-value initial value
/// no allocation occurs, and the pass-by-ref `datumCopy` (which needs this as
/// the target context) is the unported datum surface, so we only check the
/// context is present here.
fn curaggcontext_assert_built(aggstate: &AggStateData<'_>) {
    // C: select_current_set() sets `curaggcontext = is_hash ? hashcontext :
    // aggcontexts[setno]`. The owned model carries `curaggcontext` as an i32: a
    // grouping-set index into `aggcontexts`, or the `CURAGGCONTEXT_HASH` (-1)
    // sentinel meaning "use the single hashcontext". Resolve the sentinel to the
    // hashcontext rather than indexing `aggcontexts[-1]`.
    if aggstate.curaggcontext == crate::node_lifecycle::CURAGGCONTEXT_HASH {
        let _: types_nodes::execnodes::EcxtId = aggstate
            .hashcontext
            .expect("curaggcontext: hashcontext not built (hashed path)");
        return;
    }
    let idx = aggstate.curaggcontext as usize;
    let aggcontexts = aggstate
        .aggcontexts
        .as_ref()
        .expect("curaggcontext: aggcontexts not built");
    // `aggcontexts[idx]` is an EcxtId into the EState pool; the C switches into
    // its per-tuple memory before the datumCopy. The per-grouping-set context is
    // present iff the id exists in the array, which is all we assert here (the
    // pass-by-value datumCopy needs no allocation, and the pass-by-ref copy is
    // the unported datum surface).
    let _: types_nodes::EcxtId = aggcontexts[idx];
}

/// `aggstate->curaggcontext` resolved to its [`EcxtId`] (the per-grouping-set
/// aggregate context, or the hashcontext for the hashed path). C switches into
/// `curaggcontext->ecxt_per_tuple_memory` before reparenting a by-ref transition
/// value; the owned by-ref copy targets this id (its lifetime resolution lives
/// in `datum_copy_into_ecxt`).
fn curaggcontext_ecxt(aggstate: &AggStateData<'_>) -> Option<types_nodes::execnodes::EcxtId> {
    if aggstate.curaggcontext == crate::node_lifecycle::CURAGGCONTEXT_HASH {
        return aggstate.hashcontext;
    }
    let idx = aggstate.curaggcontext as usize;
    aggstate.aggcontexts.as_ref().and_then(|c| c.get(idx).copied())
}

/// `aggstate->tmpcontext` (as an [`EcxtId`] for the execExpr seam). The owned
/// model addresses ExprContexts by pool id; the AggState carries the tmpcontext
/// inline, but the evaltrans expression evaluates against the EState's pool, so
/// the id is what the seam needs. The tmpcontext is registered in the EState
/// pool at init; its id is the AggState's stored handle.
fn tmpcontext_ecxt(aggstate: &AggStateData<'_>) -> types_nodes::EcxtId {
    aggstate
        .tmpcontext
        .expect("advance_aggregates: tmpcontext EcxtId not assigned by ExecInitAgg")
}

/// `int Max(aggstate->phase->numsets, 1)` numerator — the current phase's
/// `numsets`.
fn current_phase_numsets(aggstate: &AggStateData<'_>) -> i32 {
    let phase = aggstate.phase as usize;
    let phases = aggstate
        .phases
        .as_ref()
        .expect("initialize_aggregates: phases not built");
    phases[phase].numsets
}

/// `datumCopy(value, typByVal, typLen)` into the given memory context. Datum
/// copy is the datum.c surface; for pass-by-value datums it is a plain copy
/// (no allocation). Pass-by-ref copy is owned by the not-yet-ported datum unit.
/// `datumCopy(value, typByVal, typLen)` into CurrentMemoryContext (the
/// process_ordered_* working context). By-value is verbatim; by-reference is
/// deep-copied via the datum.c seam. The owned model materializes into the
/// per-query context (its `Datum<'mcx>` lifetime), faithful for the single-group
/// drain (the multi-group/hashagg per-group reset is gated elsewhere).
fn datum_copy_current<'mcx>(
    value: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    typ_by_val: bool,
    typ_len: i16,
    estate: &mut EStateData<'mcx>,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    if typ_by_val {
        return Ok(value);
    }
    let mcx = estate.es_query_cxt;
    backend_utils_adt_datum_seams::datum_copy_v::call(mcx, &value, typ_by_val, typ_len as i32)
}

/// `pfree(DatumGetPointer(d))` — free a pass-by-ref sort datum. In the owned
/// model the value is held by an arena-managed `Datum` whose backing memory is
/// reclaimed when the owning context drops (the per-query / working context),
/// so an explicit free is a faithful no-op (the move-out here drops the handle).
fn pfree_datum(_d: types_tuple::backend_access_common_heaptuple::Datum<'_>) {
    // drop(_d): the arena owns the bytes; nothing to hand back to a chunk allocator.
}

/// `pertrans->transfn.fn_strict` — whether the transition function is strict.
/// `FmgrInfo.fn_strict` IS modeled (fmgr #52); the blocker is reaching the
/// resolved per-trans transfn FmgrInfo on the nodeAgg-owned `AggStatePerTransData`,
/// which the unported `ExecInitAgg`/build_pertrans path has not yet populated.
fn transfn_is_strict(pertrans: &AggStatePerTransData<'_>) -> bool {
    pertrans.transfn.fn_strict
}

/// `fcinfo->args[i].isnull` — the transfn call frame's argument null flag.
/// The shared `FunctionCallInfoBaseData` now carries `args[]` (#296); the blocker
/// is that this crate has not yet modeled the nodeAgg-owned *per-trans* call frame
/// `AggStatePerTransData.transfn_fcinfo` as a populated args carrier — it is a
/// long-lived per-trans frame set up by the unported `build_pertrans_for_aggref`
/// / `ExecInitAgg` path, so there is no populated `args[]` here to read.
fn fcinfo_arg_isnull(pertrans: &AggStatePerTransData<'_>, i: i32) -> bool {
    // The ordered/distinct single-column drain only ever loads index 1, carried
    // (by-ref-faithfully) on `distinct_value_isnull` (see `fcinfo_set_arg`).
    debug_assert_eq!(
        i, 1,
        "fcinfo_arg_isnull: only the single-column ordered/distinct args[1] is modeled"
    );
    pertrans.distinct_value_isnull
}

/// `fcinfo->args[i].value` — the transfn call frame's argument value.
fn fcinfo_arg_value<'mcx>(
    pertrans: &AggStatePerTransData<'mcx>,
    i: i32,
) -> types_tuple::backend_access_common_heaptuple::Datum<'mcx> {
    debug_assert_eq!(
        i, 1,
        "fcinfo_arg_value: only the single-column ordered/distinct args[1] is modeled"
    );
    pertrans.distinct_value.clone()
}

/// `fcinfo->args[i].value = v; fcinfo->args[i].isnull = isnull;` — store one
/// argument into the transfn call frame.
///
/// The owned `transfn_fcinfo->args[]` is the bare-word `types_datum::NullableDatum`
/// (#296), which cannot carry a by-reference value: collapsing one into the word
/// via `as_usize()` panics ("scalar accessor called on a by-reference value") for
/// a pass-by-ref aggregate input (text/name/numeric ordered-set DISTINCT key
/// fetched out of the per-trans tuplesort). So the canonical input value is
/// stored into the by-ref-capable `pertrans->distinct_value`/`_isnull` slot (C's
/// `args[1].{value,isnull}` for the single-column ordered/distinct path). The
/// single-column ordered-aggregate drain only ever loads index 1.
fn fcinfo_set_arg<'mcx>(
    pertrans: &mut AggStatePerTransData<'mcx>,
    i: i32,
    value: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    isnull: bool,
) {
    debug_assert_eq!(
        i, 1,
        "fcinfo_set_arg: only the single-column ordered/distinct args[1] store is modeled"
    );
    pertrans.distinct_value = value;
    pertrans.distinct_value_isnull = isnull;
}

/// `pertrans->transfn_fcinfo->args[n] = { value, isnull }` — the interpreter's
/// `EEOP_AGG_PRESORTED_DISTINCT_SINGLE` dispatch loads the just-evaluated input
/// into the per-trans transfn call frame before the distinct comparison reads it
/// (`ExecEvalPreOrderedDistinctSingle` reads `args[1]`). Mirrors C, where the
/// compiler recursed the input directly into `&trans_fcinfo->args[1]`.
///
/// The owned `transfn_fcinfo->args[]` is the bare-word `types_datum::NullableDatum`
/// (#296), which cannot carry a by-reference DISTINCT key (text/name/numeric):
/// collapsing one into the word panics ("scalar accessor called on a
/// by-reference value"). So the canonical input is stored into the by-ref-capable
/// `pertrans->distinct_value`/`distinct_value_isnull` slot, from which
/// `ExecEvalPreOrderedDistinctSingle` reads it. (The `n` index is retained for
/// fidelity with C's `args[1]`; the single-column DISTINCT comparator only ever
/// loads index 1.)
pub fn set_transfn_arg<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    transno: usize,
    n: i32,
    value: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    isnull: bool,
) {
    debug_assert_eq!(
        n, 1,
        "set_transfn_arg: only the single-column DISTINCT args[1] store is modeled"
    );
    let pertrans = &mut aggstate
        .pertrans
        .as_mut()
        .expect("set_transfn_arg: aggstate->pertrans not built")[transno];
    pertrans.distinct_value = value;
    pertrans.distinct_value_isnull = isnull;
}

/// `DatumGetBool(FunctionCall2Coll(&pertrans->equalfnOne, aggCollation,
/// oldVal, newVal))` — the single-column DISTINCT equality comparator.
/// `FunctionCall2Coll` is the fmgr direct-call surface owned by the fmgr unit.
fn equalfn_one_call(
    pertrans: &AggStatePerTransData<'_>,
    _old_val: types_tuple::backend_access_common_heaptuple::Datum<'_>,
    _new_val: types_tuple::backend_access_common_heaptuple::Datum<'_>,
) -> PgResult<bool> {
    let _ = (&pertrans.equalfn_one, pertrans.agg_collation);
    panic!(
        "backend-executor-nodeAgg::process_ordered_aggregate_single: \
         FunctionCall2Coll(equalfnOne) is the fmgr direct-call surface, owned by the \
         not-yet-ported fmgr unit; no seam yet"
    );
}

/// `ExecQual(pertrans->equalfnMulti, tmpcontext)` — the multi-column DISTINCT
/// comparator, a compiled ExprState evaluated through the execExpr seam over
/// the tmpcontext (id into the EState pool).
fn equalfn_multi_qual(
    aggstate: &AggStateData<'_>,
    pertrans: &AggStatePerTransData<'_>,
) -> PgResult<bool> {
    let _ = (pertrans.equalfn_multi.as_ref(), aggstate.tmpcontext);
    panic!(
        "backend-executor-nodeAgg::process_ordered_aggregate_multi: ExecQual(equalfnMulti) \
         needs the tmpcontext ExprContext pool id assigned by ExecInitAgg (not yet ported)"
    );
}

/// `slot_getsomeattrs(slot, n)` then copy `slot->tts_values[i]` /
/// `slot->tts_isnull[i]` into the transfn args. Deforming the slot is the
/// execTuples surface; the trimmed `TupleTableSlot` carries no payload yet, so
/// the copy into the (also-unported) fcinfo args cannot run.
fn load_transfn_args_from_slot(
    _aggstate: &mut AggStateData<'_>,
    _pertrans: &mut AggStatePerTransData<'_>,
    _slot: types_nodes::SlotId,
    _num_trans_inputs: i32,
) -> PgResult<()> {
    panic!(
        "backend-executor-nodeAgg::process_ordered_aggregate_multi: slot_getsomeattrs + the \
         tts_values/tts_isnull -> fcinfo->args copy span the not-yet-ported execTuples (slot \
         payload) and fmgr (call frame) units; no seam yet"
    );
}

/// `ExecClearTuple(slot)` — clear a slot addressed by pool id. The slot-ops
/// dispatch is owned by execTuples; its seam takes a `&mut TupleTableSlot`, but
/// the AggState addresses slots by id only, so the slot lookup is the EState
/// pool's (also unported here). Reached only on the multi-column DISTINCT path.
fn exec_clear_tuple_slot(_slot: types_nodes::SlotId) -> PgResult<()> {
    panic!(
        "backend-executor-nodeAgg::process_ordered_aggregate_multi: ExecClearTuple needs the \
         EState slot-pool lookup to resolve the slot id to a TupleTableSlot; that pool is \
         owned by the not-yet-ported execTuples/EState surface"
    );
}

/// Resolve a sort slot id (`pertrans->sortslot`) to the live `TupleTableSlot`
/// that `tuplesort_gettupleslot` writes into. The multi-column DISTINCT drain
/// addresses slots by pool id only and does not thread the owning `EState`
/// here, so the lookup is the EState slot pool's (the surface that assigns
/// these per-trans slots at ExecInitAgg is not threaded into this path yet).
fn resolve_slot_mut<'a, 'mcx>(
    _slot: types_nodes::SlotId,
) -> &'a mut types_nodes::SlotData<'mcx> {
    panic!(
        "backend-executor-nodeAgg::process_ordered_aggregate_multi: tuplesort_gettupleslot \
         needs the EState slot-pool lookup to resolve the sortslot id to a SlotData; \
         that pool is owned by the not-yet-ported execTuples/EState surface"
    );
}

/// `MemoryContextReset(aggstate->tmpcontext->ecxt_per_tuple_memory)` /
/// `ResetExprContext(tmpcontext)` — reset the tmpcontext's per-tuple memory.
fn tmpcontext_reset<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // tmpcontext is an EcxtId into the EState pool; reset its per-tuple memory.
    let ecxt = aggstate
        .tmpcontext
        .expect("tmpcontext_reset: tmpcontext not built");
    backend_executor_execUtils_seams::reset_expr_context::call(estate, ecxt)
}

/// `aggstate->tmpcontext->ecxt_outertuple` — read.
fn tmpcontext_outertuple<'mcx>(
    aggstate: &AggStateData<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<types_nodes::SlotId> {
    let ecxt = aggstate
        .tmpcontext
        .expect("tmpcontext_outertuple: tmpcontext not built");
    estate.ecxt(ecxt).ecxt_outertuple
}

/// `aggstate->tmpcontext->ecxt_outertuple = slot;` — write.
fn set_tmpcontext_outertuple<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot: Option<types_nodes::SlotId>,
) {
    let ecxt = aggstate
        .tmpcontext
        .expect("set_tmpcontext_outertuple: tmpcontext not built");
    estate.ecxt_mut(ecxt).ecxt_outertuple = slot;
}

/// `tmpcontext->ecxt_outertuple = slot1; tmpcontext->ecxt_innertuple = slot2;`
fn set_tmpcontext_outer_inner<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer: Option<types_nodes::SlotId>,
    inner: Option<types_nodes::SlotId>,
) {
    let ecxt = aggstate
        .tmpcontext
        .expect("set_tmpcontext_outer_inner: tmpcontext not built");
    let tmpcontext = estate.ecxt_mut(ecxt);
    tmpcontext.ecxt_outertuple = outer;
    tmpcontext.ecxt_innertuple = inner;
}
