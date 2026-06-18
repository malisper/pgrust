//! Sorted-grouping family: the AGG_PLAIN / AGG_SORTED retrieve path, which
//! reads sorted (or single-group) input, advances transition state per group,
//! and returns one output tuple per group.

use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::nodeagg::{AGG_MIXED, AGG_PLAIN};
use crate::aggstate::AggStateData;
use types_nodes::{EStateData, SlotId};

/// `TupIsNull(slot)` — true if `slot` is NULL or marked empty (`TTS_EMPTY`).
/// The slot is an id into `estate.es_tupleTable`.
#[inline]
fn tup_is_null(slot: Option<SlotId>, estate: &EStateData<'_>) -> bool {
    match slot {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `ReScanExprContext(econtext)` (execUtils.c) — reset the context's per-tuple
// ReScanExprContext is provided by backend-executor-execUtils via the installed
// `re_scan_expr_context` seam (addresses the econtext by its EState-pool
// EcxtId); the Agg driver calls it directly in `agg_retrieve_direct`.

/// `ResetExprContext(econtext)` (executor/executor.h) — reset the context's
/// per-tuple memory. Owned by `backend-executor-execUtils`; not yet wired (see
/// `rescan_expr_context_owned`).
#[inline]
fn reset_expr_context_tmp<'mcx>(
    aggstate: &AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ResetExprContext(aggstate->tmpcontext): tmpcontext is an EcxtId into the
    // EState pool; reset its per-tuple memory through the execUtils seam.
    if let Some(ecxt) = aggstate.tmpcontext {
        backend_executor_execUtils_seams::reset_expr_context::call(estate, ecxt)?;
    }
    Ok(())
}

/// `ExecQual(aggstate->phase->eqfunctions[eqidx], tmpcontext)` over the Agg's
/// per-input-tuple `tmpcontext` (an `EcxtId`). Delegates to the installed
/// `exec_qual` seam owned by `backend-executor-execExpr`.
fn exec_qual_eqfunction<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    eqidx: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let tmpcontext = aggstate
        .tmpcontext
        .expect("exec_qual_eqfunction: tmpcontext not set");
    with_phase_eqfunction(aggstate, eqidx, |state| {
        backend_executor_execExpr_seams::exec_qual::call(state, tmpcontext, estate)
    })
}

/// `ExecQualAndReset(aggstate->phase->eqfunctions[eqidx], tmpcontext)`: like
/// `exec_qual_eqfunction` but resets the per-tuple memory afterwards.
fn exec_qual_and_reset_eqfunction<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    eqidx: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let tmpcontext = aggstate
        .tmpcontext
        .expect("exec_qual_and_reset_eqfunction: tmpcontext not set");
    with_phase_eqfunction(aggstate, eqidx, |state| {
        backend_executor_execExpr_seams::exec_qual_and_reset::call(state, tmpcontext, estate)
    })
}

/// Take the phase's compiled `eqfunctions[eqidx]` `ExprState` out of the node,
/// run `f` against it (mutably, as the seam requires), then put it back. The
/// temporary take avoids a long borrow of `aggstate` across the seam call.
fn with_phase_eqfunction<'mcx, R>(
    aggstate: &mut AggStateData<'mcx>,
    eqidx: usize,
    f: impl FnOnce(&mut ExprState<'mcx>) -> R,
) -> R {
    let phase = aggstate.phase as usize;
    let mut state = {
        let phases = aggstate
            .phases
            .as_mut()
            .expect("with_phase_eqfunction: phases not built");
        phases[phase]
            .eqfunctions
            .as_mut()
            .expect("with_phase_eqfunction: phase eqfunctions not built")[eqidx]
            .take()
            .expect("with_phase_eqfunction: phase eqfunction not compiled")
    };
    let r = f(&mut state);
    let phases = aggstate
        .phases
        .as_mut()
        .expect("with_phase_eqfunction: phases not built");
    phases[phase]
        .eqfunctions
        .as_mut()
        .expect("with_phase_eqfunction: phase eqfunctions not built")[eqidx] = Some(state);
    r
}

/// `ResetTupleHashIterator(htable, iter)` (executor/execGrouping.c): reset the
/// hash iterator to the start of the table. Owned by
/// `backend-executor-execGrouping`; not yet wired through a seam.
#[inline]
fn reset_tuple_hash_iterator(_perhash_idx: usize) -> PgResult<()> {
    panic!(
        "ResetTupleHashIterator (executor/execGrouping.c) is not yet wired: \
         backend-executor-execGrouping owns it"
    )
}

/// `agg_retrieve_direct(aggstate)` — the plain/sorted-grouping driver: read
/// input tuples, detect group boundaries with the per-phase equality
/// functions, advance transition state, and emit each group's projected
/// result. Handles grouping-set phases and rollup. Returns `None` at the end
/// of the scan.
pub fn agg_retrieve_direct<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let mcx = estate.es_query_cxt;

    // Agg *node = aggstate->phase->aggnode;
    // We track the current phase by index; its aggnode strategy/numCols are
    // read on demand to avoid holding a borrow of aggstate across the
    // mutating helper calls.

    // econtext is the per-output-tuple expression context (ps_ExprContext);
    // tmpcontext is the per-input-tuple expression context (aggstate.tmpcontext).
    // Both are read on demand below.

    // AggStatePerAgg peragg = aggstate->peragg;  (an index/borrow held lazily)
    // AggStatePerGroup *pergroups = aggstate->pergroups;

    // firstSlot = aggstate->ss.ss_ScanTupleSlot;
    let first_slot = aggstate
        .ss
        .ss_ScanTupleSlot
        .expect("agg_retrieve_direct: ss_ScanTupleSlot not set");

    // bool hasGroupingSets = aggstate->phase->numsets > 0;
    // int numGroupingSets = Max(aggstate->phase->numsets, 1);
    let cur_phase_numsets = phase_numsets(aggstate);
    let has_grouping_sets = cur_phase_numsets > 0;
    let mut num_grouping_sets = core::cmp::max(cur_phase_numsets, 1);

    // econtext = aggstate->ss.ps.ps_ExprContext (the per-output-tuple context).
    let econtext_id = aggstate
        .ss
        .ps
        .ps_ExprContext
        .expect("agg_retrieve_direct: ps_ExprContext not set");

    // We loop retrieving groups until we find one matching aggstate->ss.ps.qual.
    while !aggstate.agg_done {
        // ReScanExprContext(econtext);
        backend_executor_execUtils_seams::re_scan_expr_context::call(estate, econtext_id)?;

        // Determine how many grouping sets need to be reset at this boundary.
        let mut num_reset = if aggstate.projected_set >= 0
            && aggstate.projected_set < num_grouping_sets
        {
            aggstate.projected_set + 1
        } else {
            num_grouping_sets
        };

        // for (i = 0; i < numReset; i++) ReScanExprContext(aggstate->aggcontexts[i]);
        for i in 0..num_reset {
            let aggctx_id = aggstate
                .aggcontexts
                .as_ref()
                .and_then(|v| v.get(i as usize).copied())
                .expect("agg_retrieve_direct: aggcontexts[i] not set");
            backend_executor_execUtils_seams::re_scan_expr_context::call(estate, aggctx_id)?;
        }

        // Check if input is complete and there are no more groups to project in
        // this phase; move to next phase or mark as done.
        if aggstate.input_done && aggstate.projected_set >= (num_grouping_sets - 1) {
            if aggstate.current_phase < aggstate.numphases - 1 {
                // initialize_phase(aggstate, aggstate->current_phase + 1);
                crate::node_lifecycle::initialize_phase(
                    aggstate,
                    aggstate.current_phase + 1,
                    estate,
                )?;
                aggstate.input_done = false;
                aggstate.projected_set = -1;
                num_grouping_sets = core::cmp::max(phase_numsets(aggstate), 1);
                // node = aggstate->phase->aggnode; (tracked by phase index)
                num_reset = num_grouping_sets;
            } else if aggstate.aggstrategy == AGG_MIXED {
                // Mixed mode; we've output all the grouped stuff and have full
                // hashtables, so switch to outputting those.
                crate::node_lifecycle::initialize_phase(aggstate, 0, estate)?;
                aggstate.table_filled = true;
                // ResetTupleHashIterator(aggstate->perhash[0].hashtable,
                //                        &aggstate->perhash[0].hashiter);
                reset_tuple_hash_iterator(0)?;
                crate::node_lifecycle::select_current_set(aggstate, 0, true);
                return crate::hash_grouping::agg_retrieve_hash_table(aggstate, estate);
            } else {
                aggstate.agg_done = true;
                break;
            }
        }

        // Get the number of columns in the next grouping set after the last
        // projected one (if any).
        let next_set_size = if aggstate.projected_set >= 0
            && aggstate.projected_set < (num_grouping_sets - 1)
        {
            phase_gset_length(aggstate, (aggstate.projected_set + 1) as usize)
        } else {
            0
        };

        // tmpcontext->ecxt_innertuple = econtext->ecxt_outertuple;
        let econtext_outertuple = econtext_outertuple_slot(econtext_id, estate);
        set_tmpcontext_innertuple(aggstate, estate, econtext_outertuple);

        // node->aggstrategy of the current phase.
        let node_aggstrategy = phase_aggstrategy(aggstate);

        // If a subgroup for the current grouping set is present, project it.
        let take_new_group_branch = aggstate.input_done
            || (node_aggstrategy != AGG_PLAIN
                && aggstate.projected_set != -1
                && aggstate.projected_set < (num_grouping_sets - 1)
                && next_set_size > 0
                && !exec_qual_and_reset_eqfunction(
                    aggstate,
                    (next_set_size - 1) as usize,
                    estate,
                )?);

        if take_new_group_branch {
            aggstate.projected_set += 1;
            debug_assert!(aggstate.projected_set < num_grouping_sets);
            debug_assert!(next_set_size > 0 || aggstate.input_done);
        } else {
            // The next projection will always be the first (or only) grouping
            // set (unless the input proves to be empty).
            aggstate.projected_set = 0;

            // If we don't already have the first tuple of the new group, fetch
            // it from the outer plan.
            if aggstate.grp_first_tuple.is_none() {
                // outerslot = fetch_input_tuple(aggstate);
                let outerslot = crate::node_lifecycle::fetch_input_tuple(aggstate, estate)?;
                if !tup_is_null(outerslot, estate) {
                    // Make a copy of the first input tuple; we will use this for
                    // comparisons (in group mode) and for projection.
                    // aggstate->grp_firstTuple = ExecCopySlotHeapTuple(outerslot);
                    let copied = backend_executor_execTuples_seams::exec_copy_slot_heap_tuple::call(
                        estate,
                        outerslot.unwrap(),
                    )?;
                    aggstate.grp_first_tuple = Some(copied);
                } else {
                    // outer plan produced no tuples at all
                    if has_grouping_sets {
                        // If there was no input at all, we need to project rows
                        // only if there are grouping sets of size 0.
                        aggstate.input_done = true;

                        while phase_gset_length(aggstate, aggstate.projected_set as usize) > 0 {
                            aggstate.projected_set += 1;
                            if aggstate.projected_set >= num_grouping_sets {
                                // We can't set agg_done here because we might
                                // have more phases to do, even though the input
                                // is empty. So restart the whole outer loop.
                                break;
                            }
                        }

                        if aggstate.projected_set >= num_grouping_sets {
                            continue;
                        }
                    } else {
                        aggstate.agg_done = true;
                        // If we are grouping, we should produce no tuples too.
                        if node_aggstrategy != AGG_PLAIN {
                            return Ok(None);
                        }
                    }
                }
            }

            // Initialize working state for a new input tuple group.
            // initialize_aggregates(aggstate, pergroups, numReset);
            initialize_aggregates_pergroups(aggstate, num_reset, mcx)?;

            if aggstate.grp_first_tuple.is_some() {
                // Store the copied first input tuple in the slot reserved for it.
                // ExecForceStoreHeapTuple(aggstate->grp_firstTuple, firstSlot, true);
                let first = aggstate.grp_first_tuple.take().unwrap();
                backend_executor_execTuples_seams::exec_force_store_formed_heap_tuple::call(
                    estate, first_slot, first, true,
                )?;
                aggstate.grp_first_tuple = None; // don't keep two pointers

                // set up for first advance_aggregates call
                // tmpcontext->ecxt_outertuple = firstSlot;
                set_tmpcontext_outertuple(aggstate, estate, Some(first_slot));

                // Process each outer-plan tuple, and then fetch the next one,
                // until we exhaust the outer plan or cross a group boundary.
                loop {
                    // During phase 1 only of a mixed agg, we need to update
                    // hashtables as well in advance_aggregates.
                    if aggstate.aggstrategy == AGG_MIXED && aggstate.current_phase == 1 {
                        crate::hash_grouping::lookup_hash_entries(aggstate, estate)?;
                    }

                    // Advance the aggregates (or combine functions).
                    crate::transition::advance_aggregates(aggstate, estate)?;

                    // Reset per-input-tuple context after each tuple.
                    reset_expr_context_tmp(aggstate, estate)?;

                    let outerslot = crate::node_lifecycle::fetch_input_tuple(aggstate, estate)?;
                    if tup_is_null(outerslot, estate) {
                        // no more outer-plan tuples available

                        // if we built hash tables, finalize any spills
                        if aggstate.aggstrategy == AGG_MIXED && aggstate.current_phase == 1 {
                            crate::spill::hashagg_finish_initial_spills(aggstate, estate, mcx)?;
                        }

                        if has_grouping_sets {
                            aggstate.input_done = true;
                            break;
                        } else {
                            aggstate.agg_done = true;
                            break;
                        }
                    }
                    // set up for next advance_aggregates call
                    // tmpcontext->ecxt_outertuple = outerslot;
                    set_tmpcontext_outertuple(aggstate, estate, outerslot);

                    // If we are grouping, check whether we've crossed a group
                    // boundary.
                    let node_aggstrategy = phase_aggstrategy(aggstate);
                    let node_numcols = phase_numcols(aggstate);
                    if node_aggstrategy != AGG_PLAIN && node_numcols > 0 {
                        // tmpcontext->ecxt_innertuple = firstSlot;
                        set_tmpcontext_innertuple(aggstate, estate, Some(first_slot));
                        if !exec_qual_eqfunction(aggstate, (node_numcols - 1) as usize, estate)? {
                            // aggstate->grp_firstTuple = ExecCopySlotHeapTuple(outerslot);
                            let copied =
                                backend_executor_execTuples_seams::exec_copy_slot_heap_tuple::call(
                                    estate,
                                    outerslot.unwrap(),
                                )?;
                            aggstate.grp_first_tuple = Some(copied);
                            break;
                        }
                    }
                }
            }

            // Use the representative input tuple for any references to
            // non-aggregated input columns. econtext->ecxt_outertuple = firstSlot;
            set_econtext_outertuple(econtext_id, estate, Some(first_slot));
        }

        debug_assert!(aggstate.projected_set >= 0);

        let current_set = aggstate.projected_set;

        // prepare_projection_slot(aggstate, econtext->ecxt_outertuple, currentSet);
        let proj_input = econtext_outertuple_slot(econtext_id, estate)
            .expect("agg_retrieve_direct: econtext->ecxt_outertuple not set");
        crate::finalize::prepare_projection_slot(aggstate, proj_input, current_set, estate)?;

        // select_current_set(aggstate, currentSet, false);
        crate::node_lifecycle::select_current_set(aggstate, current_set, false);

        // finalize_aggregates(aggstate, peragg, pergroups[currentSet]);
        finalize_aggregates_for_set(aggstate, current_set, estate)?;

        // If there's no row to project right now, we must continue rather than
        // returning a null since there might be more groups.
        // result = project_aggregates(aggstate);
        let result = crate::finalize::project_aggregates(aggstate, estate)?;
        if result.is_some() {
            return Ok(result);
        }
    }

    // No more groups.
    Ok(None)
}

// ---------------------------------------------------------------------------
// Owned accessors over the current phase / contexts.
//
// The scaffold carries `phase` as an index into `aggstate.phases` and the
// per-output / per-input expression contexts as owned `ExprContext` values.
// These small accessors mirror the C field reads (`aggstate->phase->...`,
// `econtext->...`, `tmpcontext->...`) while keeping the borrows short.
// ---------------------------------------------------------------------------

/// `aggstate->phase->numsets`.
fn phase_numsets(aggstate: &AggStateData<'_>) -> i32 {
    let phases = aggstate
        .phases
        .as_ref()
        .expect("agg_retrieve_direct: phases not built");
    phases[aggstate.phase as usize].numsets
}

/// `aggstate->phase->aggnode->aggstrategy`.
fn phase_aggstrategy(aggstate: &AggStateData<'_>) -> types_nodes::nodeagg::AggStrategy {
    let phases = aggstate
        .phases
        .as_ref()
        .expect("agg_retrieve_direct: phases not built");
    phases[aggstate.phase as usize]
        .aggnode
        .as_ref()
        .expect("agg_retrieve_direct: phase aggnode not set")
        .aggstrategy
}

/// `aggstate->phase->aggnode->numCols`.
fn phase_numcols(aggstate: &AggStateData<'_>) -> i32 {
    let phases = aggstate
        .phases
        .as_ref()
        .expect("agg_retrieve_direct: phases not built");
    phases[aggstate.phase as usize]
        .aggnode
        .as_ref()
        .expect("agg_retrieve_direct: phase aggnode not set")
        .num_cols
}

/// `aggstate->phase->gset_lengths[idx]`.
fn phase_gset_length(aggstate: &AggStateData<'_>, idx: usize) -> i32 {
    let phases = aggstate
        .phases
        .as_ref()
        .expect("agg_retrieve_direct: phases not built");
    let phase = &phases[aggstate.phase as usize];
    match phase.gset_lengths.as_ref() {
        Some(lengths) => lengths[idx],
        None => 0,
    }
}

/// `econtext->ecxt_outertuple` where econtext is the node's per-output-tuple
/// context (`aggstate->ss.ps.ps_ExprContext`, addressed by `econtext_id`).
fn econtext_outertuple_slot<'mcx>(
    econtext_id: types_nodes::EcxtId,
    estate: &EStateData<'mcx>,
) -> Option<SlotId> {
    estate.ecxt(econtext_id).ecxt_outertuple
}

/// `tmpcontext->ecxt_innertuple = slot`.
fn set_tmpcontext_innertuple<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot: Option<SlotId>,
) {
    if let Some(id) = aggstate.tmpcontext {
        estate.ecxt_mut(id).ecxt_innertuple = slot;
    }
}

/// `tmpcontext->ecxt_outertuple = slot`.
fn set_tmpcontext_outertuple<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot: Option<SlotId>,
) {
    if let Some(id) = aggstate.tmpcontext {
        estate.ecxt_mut(id).ecxt_outertuple = slot;
    }
}

/// `econtext->ecxt_outertuple = slot` for the per-output-tuple context
/// (`ps_ExprContext`, addressed by `econtext_id`). The value is consumed by
/// `prepare_projection_slot`.
fn set_econtext_outertuple<'mcx>(
    econtext_id: types_nodes::EcxtId,
    estate: &mut EStateData<'mcx>,
    slot: Option<SlotId>,
) {
    estate.ecxt_mut(econtext_id).ecxt_outertuple = slot;
}

/// `initialize_aggregates(aggstate, pergroups, numReset)` over the node's
/// `pergroups`. The borrow of `pergroups` is taken from the node and threaded
/// through the helper.
fn initialize_aggregates_pergroups<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    num_reset: i32,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<()> {
    // The C `pergroups = aggstate->pergroups` ALIASES the head of
    // `aggstate->all_pergroups` (the first numGroupingSets entries, non-hashed
    // path). In the owned model `pergroups` was a separate split copy; the
    // transition interpreter (`EEOP_AGG_PLAIN_TRANS_*`) mutates
    // `all_pergroups[setoff][transno]`, so initialize/finalize must read/write
    // the SAME storage. Operate directly on `all_pergroups` (whose head IS the
    // sorted-path pergroups) so the alias is faithful.
    let mut all_pergroups = aggstate.all_pergroups.take();
    let result = match all_pergroups.as_mut() {
        Some(pg) => crate::transition::initialize_aggregates(aggstate, pg, num_reset, mcx),
        None => {
            // all_pergroups == NULL: nothing to initialize.
            Ok(())
        }
    };
    aggstate.all_pergroups = all_pergroups;
    result
}

/// `finalize_aggregates(aggstate, peragg, pergroups[currentSet])`: finalize the
/// per-group transition values for the projected grouping set.
fn finalize_aggregates_for_set<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    current_set: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // See initialize_aggregates_pergroups: read the SAME storage the transition
    // interpreter writes (`all_pergroups`, whose head is the sorted-path
    // `pergroups`).
    let mut all_pergroups = aggstate.all_pergroups.take();
    let result = match all_pergroups.as_mut() {
        Some(pg) => match pg[current_set as usize].as_mut() {
            Some(pergroup) => {
                crate::finalize::finalize_aggregates(aggstate, pergroup.as_mut_slice(), estate)
            }
            None => Ok(()),
        },
        None => Ok(()),
    };
    aggstate.all_pergroups = all_pergroups;
    result
}
