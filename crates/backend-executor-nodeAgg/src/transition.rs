//! Transition family: initializing per-group transition state and advancing
//! it. Covers the simple transfn driver and the ordered/distinct paths that
//! feed sorted input through the transition function.

use mcx::{alloc_in, Mcx};
use types_error::PgResult;
use crate::aggstate::{AggStateData, AggStatePerGroupData, AggStatePerTransData};
use types_nodes::EStateData;

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
        pergroupstate.trans_value = datum_copy_into(
            pertrans.init_value.clone(),
            pertrans.transtype_by_val,
            pertrans.transtype_len,
        )?;
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
) -> PgResult<()> {
    // FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
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
            curaggcontext_assert_built(aggstate);
            pergroupstate.trans_value = datum_copy_into(
                arg1_value,
                pertrans.transtype_by_val,
                pertrans.transtype_len,
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
    // FunctionCallInvoke + InitFunctionCallInfoData ARE ported (fmgr-core, #52)
    // and the shared FunctionCallInfoBaseData now carries args[]/isnull/fncollation
    // (#296). The genuine blocker is NOT the shared call frame but the nodeAgg-owned
    // per-trans fcinfo *carrier* `AggStatePerTransData.transfn_fcinfo` (the
    // long-lived per-trans args[]/isnull payload + the fcinfo->context back-reference
    // to the AggState) which this crate does not yet model, plus
    // `ExecAggCopyTransValue` (the by-ref transValue reparent/copy) owned by the
    // not-yet-ported execExpr by-ref path. Until that per-trans fcinfo carrier +
    // ExecAggCopyTransValue land, this path panics loudly (mirror-PG-and-panic).
    //
    //   if (!pertrans->transtypeByVal &&
    //       DatumGetPointer(newVal) != DatumGetPointer(pergroupstate->transValue))
    //       newVal = ExecAggCopyTransValue(aggstate, pertrans, newVal,
    //                                      fcinfo->isnull,
    //                                      pergroupstate->transValue,
    //                                      pergroupstate->transValueIsNull);
    //   pergroupstate->transValue = newVal;
    //   pergroupstate->transValueIsNull = fcinfo->isnull;
    panic!(
        "backend-executor-nodeAgg::advance_transition_function: blocked on the \
         per-trans fcinfo carrier (AggStatePerTransData.transfn_fcinfo args/isnull \
         payload + fcinfo->context back-reference) and ExecAggCopyTransValue \
         (execExpr by-ref reparent); FunctionCallInvoke itself is ported (fmgr-core \
         #52) (transtypeByVal={})",
        pertrans.transtype_by_val
    );
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
            advance_transition_function(aggstate, pertrans, pergroupstate)?;

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

            advance_transition_function(aggstate, pertrans, pergroupstate)?;

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

/// `TUPLESORT_NONE` (tuplesort.h) — no extra sort options.
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
fn datum_copy_into<'mcx>(
    value: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    typ_by_val: bool,
    typ_len: i16,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    if typ_by_val {
        // datumCopy of a pass-by-value datum is the value itself.
        return Ok(value);
    }
    let _ = typ_len;
    panic!(
        "backend-executor-nodeAgg::datumCopy: pass-by-reference datumCopy is owned \
         by the not-yet-ported datum (utils/adt/datum.c) unit; no seam yet"
    );
}

/// `datumCopy(value, typByVal, typLen)` into CurrentMemoryContext (the
/// process_ordered_* working context).
fn datum_copy_current<'mcx>(
    value: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    typ_by_val: bool,
    typ_len: i16,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    if typ_by_val {
        return Ok(value);
    }
    let _ = typ_len;
    panic!(
        "backend-executor-nodeAgg::datumCopy: pass-by-reference datumCopy is owned \
         by the not-yet-ported datum (utils/adt/datum.c) unit; no seam yet"
    );
}

/// `pfree(DatumGetPointer(d))` — free a pass-by-ref datum. The chunk allocator
/// (`pfree`) is owned by the not-yet-ported mmgr surface; reached only for
/// pass-by-ref inputs in the DISTINCT path.
fn pfree_datum(_d: types_tuple::backend_access_common_heaptuple::Datum<'_>) {
    panic!(
        "backend-executor-nodeAgg::pfree: freeing a pass-by-reference sort datum is \
         owned by the not-yet-ported mmgr (pfree) surface; no seam yet"
    );
}

/// `pertrans->transfn.fn_strict` — whether the transition function is strict.
/// `FmgrInfo.fn_strict` IS modeled (fmgr #52); the blocker is reaching the
/// resolved per-trans transfn FmgrInfo on the nodeAgg-owned `AggStatePerTransData`,
/// which the unported `ExecInitAgg`/build_pertrans path has not yet populated.
fn transfn_is_strict(pertrans: &AggStatePerTransData<'_>) -> bool {
    let _ = &pertrans.transfn;
    panic!(
        "backend-executor-nodeAgg::advance_transition_function: FmgrInfo.fn_strict is \
         modeled (fmgr #52), but the per-trans transfn FmgrInfo \
         (AggStatePerTransData.transfn) is not yet populated by the unported \
         ExecInitAgg/build_pertrans path"
    );
}

/// `fcinfo->args[i].isnull` — the transfn call frame's argument null flag.
/// The shared `FunctionCallInfoBaseData` now carries `args[]` (#296); the blocker
/// is that this crate has not yet modeled the nodeAgg-owned *per-trans* call frame
/// `AggStatePerTransData.transfn_fcinfo` as a populated args carrier — it is a
/// long-lived per-trans frame set up by the unported `build_pertrans_for_aggref`
/// / `ExecInitAgg` path, so there is no populated `args[]` here to read.
fn fcinfo_arg_isnull(pertrans: &AggStatePerTransData<'_>, _i: i32) -> bool {
    let _ = pertrans.transfn_fcinfo.as_ref();
    panic!(
        "backend-executor-nodeAgg::advance_transition_function: the shared call frame \
         carries args[] (#296), but the nodeAgg-owned per-trans frame \
         AggStatePerTransData.transfn_fcinfo is not yet built/populated by the \
         unported ExecInitAgg/build_pertrans path; nothing to read"
    );
}

/// `fcinfo->args[i].value` — the transfn call frame's argument value.
fn fcinfo_arg_value<'mcx>(
    pertrans: &AggStatePerTransData<'mcx>,
    _i: i32,
) -> types_tuple::backend_access_common_heaptuple::Datum<'mcx> {
    let _ = pertrans.transfn_fcinfo.as_ref();
    panic!(
        "backend-executor-nodeAgg::advance_transition_function: the shared call frame \
         carries args[] (#296), but the nodeAgg-owned per-trans frame \
         AggStatePerTransData.transfn_fcinfo is not yet built/populated by the \
         unported ExecInitAgg/build_pertrans path; nothing to read"
    );
}

/// `fcinfo->args[i].value = v; fcinfo->args[i].isnull = isnull;` — store one
/// argument into the transfn call frame.
fn fcinfo_set_arg(
    pertrans: &mut AggStatePerTransData<'_>,
    _i: i32,
    _value: types_tuple::backend_access_common_heaptuple::Datum<'_>,
    _isnull: bool,
) {
    let _ = pertrans.transfn_fcinfo.as_mut();
    panic!(
        "backend-executor-nodeAgg::process_ordered_aggregate: the shared call frame \
         carries args[] (#296), but the nodeAgg-owned per-trans frame \
         AggStatePerTransData.transfn_fcinfo is not yet built/populated by the \
         unported ExecInitAgg/build_pertrans path; nothing to write into"
    );
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
) -> &'a mut types_nodes::TupleTableSlot<'mcx> {
    panic!(
        "backend-executor-nodeAgg::process_ordered_aggregate_multi: tuplesort_gettupleslot \
         needs the EState slot-pool lookup to resolve the sortslot id to a TupleTableSlot; \
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
