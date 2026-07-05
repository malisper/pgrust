// nodeIncrementalSort.c: two-mode sort over prefix-sorted input. The outer
// child stays with the ExecProcNode dispatcher via a fetch closure (nodesort
// precedent). Prefix-key equality is an ExprState program (nodeunique
// precedent) rather than C's per-column fcinfo loop; C compares last presorted
// column first and the program preserves that order.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::execexpr::{exec_build_grouping_equal, exec_qual, EvalSlots, ExprState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{vec_with_capacity_in, Mcx, PgBox};
use ::tuplesort::{Tuplesort, TUPLESORT_ALLOWBOUNDED, TUPLESORT_NONE};
use ::types_core::instrument::IncrementalSortInfo;
use ::types_error::PgResult;
use ::types_nodes::plannodes::IncrementalSort;
use ::types_scan::sdir::{ForwardScanDirection, ScanDirectionIsForward};
use ::types_slot::{SlotData, TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

const DEFAULT_MIN_GROUP_SIZE: i64 = 32;
const DEFAULT_MAX_FULL_SORT_GROUP_SIZE: i64 = 2 * DEFAULT_MIN_GROUP_SIZE;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExecStatus {
    LoadFullsort,
    LoadPrefixsort,
    ReadFullsort,
    ReadPrefixsort,
}

pub struct IncrementalSortState<'mcx> {
    pub plan: &'mcx IncrementalSort<'mcx>,
    pub ps_ExprContext: EcxtId,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    pub bounded: bool,
    pub bound: i64,
    outer_desc: Option<Rc<TupleDescData<'static>>>,
    execution_status: ExecStatus,
    outer_node_done: bool,
    bound_done: i64,
    n_fullsort_remaining: i64,
    fullsort_state: Option<Tuplesort>,
    prefixsort_state: Option<Tuplesort>,
    group_pivot: SlotData<'mcx>,
    transfer_tuple: SlotData<'mcx>,
    presorted_eq: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

/// `ExecInitIncrementalSort` minus child linkage: the caller (execProcnode's
/// arm) inits the outer child with unmodified eflags and passes its result
/// type.
pub fn exec_init_incremental_sort<'mcx>(
    node: &'mcx IncrementalSort<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_desc: &Rc<TupleDescData<'static>>,
    result_desc: Rc<TupleDescData<'static>>,
) -> IncrementalSortState<'mcx> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    let mcx = estate.es_query_cxt;
    let ps_ExprContext = estate.exec_assign_expr_context();
    let ps_ResultTupleSlot = estate
        .exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::MinimalTuple);
    let group_pivot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(outer_desc.clone()));
    let transfer_tuple =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(outer_desc.clone()));
    IncrementalSortState {
        plan: node,
        ps_ExprContext,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        bounded: false,
        bound: 0,
        outer_desc: Some(outer_desc.clone()),
        execution_status: ExecStatus::LoadFullsort,
        outer_node_done: false,
        bound_done: 0,
        n_fullsort_remaining: 0,
        fullsort_state: None,
        prefixsort_state: None,
        group_pivot,
        transfer_tuple,
        presorted_eq: None,
    }
}

// preparePresortedCols: equality resolved once into an ExprState program.
fn prepare_presorted_cols<'mcx>(
    node: &mut IncrementalSortState<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let n = node.plan.nPresortedCols as usize;
    let mut eqfuncoids = vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        let sortop = node.plan.sort.sortOperators[i];
        let (equality_op, _) = lsyscache::amop::get_equality_op_for_ordering_op(sortop)?
            .unwrap_or_else(|| {
                panic!("missing equality operator for ordering operator {sortop}")
            });
        eqfuncoids.push(lsyscache::get_opcode(equality_op)?);
    }
    node.presorted_eq = Some(exec_build_grouping_equal(
        mcx,
        node.outer_desc.as_ref().expect("incremental sort already ended"),
        node.outer_desc.as_ref().expect("incremental sort already ended"),
        &node.plan.sort.sortColIdx[..n],
        &eqfuncoids,
        &node.plan.sort.collations[..n],
    )?);
    Ok(())
}

// isCurrentGroup: NULL == NULL matches (grouping-equal semantics, as C). The
// caller resets ps_ExprContext afterwards.
fn is_current_group<'a, 'mcx>(
    eq: &mut ExprState<'mcx>,
    pivot: &'a mut SlotData<'mcx>,
    tuple: &'a mut SlotData<'mcx>,
) -> PgResult<bool> {
    let mut slots = EvalSlots { scan: None, inner: Some(tuple), outer: Some(pivot) };
    exec_qual(Some(eq), &mut slots)
}

fn fullsort_opts(bounded: bool) -> i32 {
    if bounded {
        TUPLESORT_ALLOWBOUNDED
    } else {
        TUPLESORT_NONE
    }
}

fn record_group(
    estate: &mut EStateData<'_>,
    plan_node_id: i32,
    prefix: bool,
    ts: &mut Tuplesort,
) {
    let stats = ts.get_stats();
    let vec = &mut estate.es_incsort_instrumentation;
    let idx = match vec.iter().position(|(i, _)| *i == plan_node_id) {
        Some(i) => i,
        None => {
            vec.push((plan_node_id, IncrementalSortInfo::default()));
            vec.len() - 1
        }
    };
    let info = &mut vec[idx].1;
    if prefix {
        info.prefixsortGroupInfo.record(&stats);
    } else {
        info.fullsortGroupInfo.record(&stats);
    }
}

// switchToPresortedPrefixMode.
fn switch_to_presorted_prefix_mode<'mcx>(
    node: &mut IncrementalSortState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let dir = estate.es_direction;
    let plan = node.plan;
    let n_presorted = plan.nPresortedCols as usize;

    match &mut node.prefixsort_state {
        None => {
            // Prefix columns are all equal within a group; sort only the rest.
            node.prefixsort_state = Some(Tuplesort::begin_heap(
                node.outer_desc.clone().expect("incremental sort already ended"),
                &plan.sort.sortColIdx[n_presorted..],
                &plan.sort.sortOperators[n_presorted..],
                &plan.sort.collations[n_presorted..],
                &plan.sort.nullsFirst[n_presorted..],
                init_small::globals::work_mem(),
                fullsort_opts(node.bounded),
            )?);
        }
        Some(ts) => ts.reset(),
    }
    if node.bounded {
        node.prefixsort_state.as_mut().unwrap().set_bound(node.bound - node.bound_done);
    }

    let mut n_tuples: i64 = 0;
    while n_tuples < node.n_fullsort_remaining {
        if n_tuples == 0 && !node.transfer_tuple.base().is_empty() {
            // A carried-over tuple opens the next batch and is its pivot.
            node.prefixsort_state
                .as_mut()
                .unwrap()
                .puttupleslot(&mut node.transfer_tuple, mcx)?;
            exectuples::exec_copy_slot(&mut node.group_pivot, &mut node.transfer_tuple, mcx, mcx)?;
        } else {
            let got = node.fullsort_state.as_mut().unwrap().gettupleslot(
                ScanDirectionIsForward(dir),
                false,
                &mut node.transfer_tuple,
                mcx,
            )?;
            debug_assert!(got);
            if node.group_pivot.base().is_empty() {
                exectuples::exec_copy_slot(
                    &mut node.group_pivot,
                    &mut node.transfer_tuple,
                    mcx,
                    mcx,
                )?;
            }
            let matched = is_current_group(
                node.presorted_eq.as_mut().expect("presorted_eq prepared"),
                &mut node.group_pivot,
                &mut node.transfer_tuple,
            )?;
            estate.reset_expr_context(node.ps_ExprContext);
            if matched {
                node.prefixsort_state
                    .as_mut()
                    .unwrap()
                    .puttupleslot(&mut node.transfer_tuple, mcx)?;
            } else {
                // transfer_tuple carries the group opener into the next batch;
                // its image (inside the full sort) outlives this transfer loop.
                exectuples::exec_clear_tuple(&mut node.group_pivot, mcx);
                break;
            }
        }
        n_tuples += 1;
    }

    node.n_fullsort_remaining -= n_tuples;

    if node.n_fullsort_remaining == 0 {
        exectuples::exec_copy_slot(&mut node.group_pivot, &mut node.transfer_tuple, mcx, mcx)?;
        node.execution_status = ExecStatus::LoadPrefixsort;
        exectuples::exec_clear_tuple(&mut node.transfer_tuple, mcx);
    } else {
        let ts = node.prefixsort_state.as_mut().unwrap();
        ts.performsort()?;
        record_group(estate, plan.sort.plan.plan_node_id, true, node.prefixsort_state.as_mut().unwrap());
        if node.bounded {
            node.bound_done = node.bound.min(node.bound_done + n_tuples);
        }
        node.execution_status = ExecStatus::ReadPrefixsort;
    }
    Ok(())
}

/// `ExecIncrementalSort`.
pub fn exec_incremental_sort<'mcx, F>(
    node: &mut IncrementalSortState<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut fetch_outer: F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    if init_small::globals::InterruptPending() {
        postgres_seams::check_for_interrupts::call()?;
    }
    let mcx = estate.es_query_cxt;
    let dir = estate.es_direction;
    let forward = ScanDirectionIsForward(dir);
    let plan = node.plan;
    let mut n_tuples: i64 = 0;

    if matches!(node.execution_status, ExecStatus::ReadFullsort | ExecStatus::ReadPrefixsort) {
        let ts = if node.execution_status == ExecStatus::ReadFullsort {
            node.fullsort_state.as_mut().unwrap()
        } else {
            node.prefixsort_state.as_mut().unwrap()
        };
        let slot = estate.slot_mut(node.ps_ResultTupleSlot);
        if ts.gettupleslot(forward, false, slot, mcx)? {
            return Ok(Some(node.ps_ResultTupleSlot));
        }
        if node.outer_node_done {
            return Ok(None);
        }
        if node.n_fullsort_remaining > 0 {
            // Another prefix key group is still parked in the full sort state.
            switch_to_presorted_prefix_mode(node, estate)?;
        } else {
            node.execution_status = ExecStatus::LoadFullsort;
        }
    }

    estate.es_direction = ForwardScanDirection;

    if node.execution_status == ExecStatus::LoadFullsort {
        match &mut node.fullsort_state {
            None => {
                prepare_presorted_cols(node, mcx)?;
                // The prefix eq detoasts compressed by-ref keys through the
                // frame's result mcx; every eval site resets ps_ExprContext
                // after the compare (C: econtext per-tuple memory).
                // SAFETY: the ps_ExprContext outlives the program (same
                // estate).
                unsafe {
                    node.presorted_eq
                        .as_mut()
                        .expect("presorted_eq prepared")
                        .arm_result_mcx_raw(estate.ecxt(node.ps_ExprContext).per_tuple_mcx())
                };
                node.fullsort_state = Some(Tuplesort::begin_heap(
                    node.outer_desc.clone().expect("incremental sort already ended"),
                    plan.sort.sortColIdx,
                    plan.sort.sortOperators,
                    plan.sort.collations,
                    plan.sort.nullsFirst,
                    init_small::globals::work_mem(),
                    fullsort_opts(node.bounded),
                )?);
            }
            Some(ts) => ts.reset(),
        }

        let min_group_size = if node.bounded {
            let current_bound = node.bound - node.bound_done;
            // Full-sort batches stay small; top-n only pays below the minimum
            // group size.
            if current_bound < DEFAULT_MIN_GROUP_SIZE {
                node.fullsort_state.as_mut().unwrap().set_bound(current_bound);
            }
            DEFAULT_MIN_GROUP_SIZE.min(current_bound)
        } else {
            DEFAULT_MIN_GROUP_SIZE
        };

        if !node.group_pivot.base().is_empty() {
            node.fullsort_state
                .as_mut()
                .unwrap()
                .puttupleslot(&mut node.group_pivot, mcx)?;
            n_tuples += 1;
            if n_tuples != min_group_size {
                exectuples::exec_clear_tuple(&mut node.group_pivot, mcx);
            }
        }

        loop {
            let fetched = fetch_outer(estate)?;
            let Some(outer_id) = fetched else {
                node.outer_node_done = true;
                let ts = node.fullsort_state.as_mut().unwrap();
                ts.performsort()?;
                record_group(estate, plan.sort.plan.plan_node_id, false, node.fullsort_state.as_mut().unwrap());
                node.execution_status = ExecStatus::ReadFullsort;
                break;
            };

            if n_tuples < min_group_size {
                node.fullsort_state
                    .as_mut()
                    .unwrap()
                    .puttupleslot(estate.slot_mut(outer_id), mcx)?;
                n_tuples += 1;
                if n_tuples == min_group_size {
                    exectuples::exec_copy_slot(
                        &mut node.group_pivot,
                        estate.slot_mut(outer_id),
                        mcx,
                        mcx,
                    )?;
                }
            } else {
                let matched = is_current_group(
                    node.presorted_eq.as_mut().expect("presorted_eq prepared"),
                    &mut node.group_pivot,
                    estate.slot_mut(outer_id),
                )?;
                estate.reset_expr_context(node.ps_ExprContext);
                if matched {
                    node.fullsort_state
                        .as_mut()
                        .unwrap()
                        .puttupleslot(estate.slot_mut(outer_id), mcx)?;
                    n_tuples += 1;
                } else {
                    // Group boundary: carry the tuple into the next batch.
                    exectuples::exec_copy_slot(
                        &mut node.group_pivot,
                        estate.slot_mut(outer_id),
                        mcx,
                        mcx,
                    )?;
                    if node.bounded {
                        node.bound_done = node.bound.min(node.bound_done + n_tuples);
                    }
                    let ts = node.fullsort_state.as_mut().unwrap();
                    ts.performsort()?;
                    record_group(
                        estate,
                        plan.sort.plan.plan_node_id,
                        false,
                        node.fullsort_state.as_mut().unwrap(),
                    );
                    node.execution_status = ExecStatus::ReadFullsort;
                    break;
                }
            }

            if n_tuples > DEFAULT_MAX_FULL_SORT_GROUP_SIZE
                && node.execution_status != ExecStatus::ReadFullsort
            {
                // Likely one large prefix group: switch to presorted prefix
                // mode via a FIFO drain of the sorted batch.
                exectuples::exec_clear_tuple(&mut node.group_pivot, mcx);
                let ts = node.fullsort_state.as_mut().unwrap();
                ts.performsort()?;
                record_group(estate, plan.sort.plan.plan_node_id, false, node.fullsort_state.as_mut().unwrap());
                if node.fullsort_state.as_ref().unwrap().used_bound() {
                    let current_bound = node.bound - node.bound_done;
                    n_tuples = current_bound.min(n_tuples);
                }
                node.n_fullsort_remaining = n_tuples;
                switch_to_presorted_prefix_mode(node, estate)?;
                break;
            }
        }
    }

    if node.execution_status == ExecStatus::LoadPrefixsort {
        debug_assert!(!node.group_pivot.base().is_empty());
        loop {
            let fetched = fetch_outer(estate)?;
            let Some(outer_id) = fetched else {
                node.outer_node_done = true;
                break;
            };
            let matched = is_current_group(
                node.presorted_eq.as_mut().expect("presorted_eq prepared"),
                &mut node.group_pivot,
                estate.slot_mut(outer_id),
            )?;
            estate.reset_expr_context(node.ps_ExprContext);
            if matched {
                node.prefixsort_state
                    .as_mut()
                    .unwrap()
                    .puttupleslot(estate.slot_mut(outer_id), mcx)?;
                n_tuples += 1;
            } else {
                exectuples::exec_copy_slot(
                    &mut node.group_pivot,
                    estate.slot_mut(outer_id),
                    mcx,
                    mcx,
                )?;
                break;
            }
        }

        let ts = node.prefixsort_state.as_mut().unwrap();
        ts.performsort()?;
        record_group(estate, plan.sort.plan.plan_node_id, true, node.prefixsort_state.as_mut().unwrap());
        node.execution_status = ExecStatus::ReadPrefixsort;
        if node.bounded {
            node.bound_done = node.bound.min(node.bound_done + n_tuples);
        }
    }

    estate.es_direction = dir;

    let ts = if node.execution_status == ExecStatus::ReadFullsort {
        node.fullsort_state.as_mut().unwrap()
    } else {
        node.prefixsort_state.as_mut().unwrap()
    };
    let slot = estate.slot_mut(node.ps_ResultTupleSlot);
    let got = ts.gettupleslot(forward, false, slot, mcx)?;
    Ok(if got { Some(node.ps_ResultTupleSlot) } else { None })
}

/// `ExecEndIncrementalSort` node-local half; the caller ends the outer child.
pub fn exec_end_incremental_sort(node: &mut IncrementalSortState<'_>) {
    node.fullsort_state = None;
    node.prefixsort_state = None;
    node.presorted_eq = None;
    node.ps_ResultTupleDesc = None;
    node.outer_desc = None;
    node.group_pivot.base_mut().tts_tupleDescriptor = None;
    node.transfer_tuple.base_mut().tts_tupleDescriptor = None;
}

mcx::forget_safe_nodrop!(ExecStatus);

// Exempt: all released in exec_end_incremental_sort.
mcx::forget_safe_struct!(
    IncrementalSortState<'_> { plan, ps_ExprContext, ps_ResultTupleSlot,
        bounded, bound, execution_status, outer_node_done, bound_done,
        n_fullsort_remaining;
        ps_ResultTupleDesc, outer_desc, fullsort_state, prefixsort_state,
        group_pivot, transfer_tuple, presorted_eq },
);

/// `ExecReScanIncrementalSort` node-local half. The caller always rescans the
/// outer child (C's chgParam is always NULL until the Param lanes land).
pub fn exec_rescan_incremental_sort<'mcx>(
    node: &mut IncrementalSortState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(estate.slot_mut(node.ps_ResultTupleSlot), mcx);
    exectuples::exec_clear_tuple(&mut node.group_pivot, mcx);
    exectuples::exec_clear_tuple(&mut node.transfer_tuple, mcx);
    node.outer_node_done = false;
    node.n_fullsort_remaining = 0;
    node.bound_done = 0;
    node.execution_status = ExecStatus::LoadFullsort;
    if let Some(ts) = &mut node.fullsort_state {
        ts.reset();
    }
    if let Some(ts) = &mut node.prefixsort_state {
        ts.reset();
    }
}

/// The `ExecSetTupleBound` IncrementalSortState arm (execProcnode.c).
pub fn incremental_sort_set_tuple_bound(node: &mut IncrementalSortState<'_>, tuples_needed: i64) {
    if tuples_needed < 0 {
        node.bounded = false;
    } else {
        node.bounded = true;
        node.bound = tuples_needed;
    }
}
