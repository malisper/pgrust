// nodeUnique.c: adjacent-duplicate elimination over sorted input. The match
// program is C's execTuplesMatchPrepare -> ExecBuildGroupingEqual; the
// previously returned tuple lives in an owned minimal slot whose image is
// shared into the estate result slot. The outer child stays with the
// ExecProcNode dispatcher via a fetch closure (nodesort precedent).
#![allow(non_snake_case)]

use std::rc::Rc;

use ::execexpr::{exec_build_grouping_equal, exec_qual, EvalSlots, ExprState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{vec_with_capacity_in, PgBox, PgVec};
use ::types_error::PgResult;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Unique;
use ::types_slot::{SlotData, TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

pub struct UniqueState<'mcx> {
    pub plan: &'mcx Unique<'mcx>,
    pub ps_ExprContext: EcxtId,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    prev_slot: SlotData<'mcx>,
    eq: PgBox<'mcx, ExprState<'mcx>>,
    have_prev: bool,
}

/// `ExecInitUnique` minus child linkage: the caller (execProcnode's T_Unique
/// arm) inits the outer child and passes its result type.
pub fn exec_init_unique<'mcx>(
    node: &'mcx Unique<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_desc: &Rc<TupleDescData<'static>>,
    result_desc: Rc<TupleDescData<'static>>,
) -> PgResult<UniqueState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    let mcx = estate.es_query_cxt;
    let ps_ExprContext = estate.exec_assign_expr_context();
    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::MinimalTuple);

    let num_cols = node.numCols as usize;
    debug_assert!(num_cols > 0 && node.uniqColIdx.len() == num_cols);
    let mut eqfuncoids: PgVec<'mcx, u32> = vec_with_capacity_in(mcx, num_cols)?;
    for &op in node.uniqOperators {
        eqfuncoids.push(lsyscache::get_opcode(op)?);
    }
    let eq = exec_build_grouping_equal(
        mcx,
        outer_desc,
        outer_desc,
        node.uniqColIdx,
        &eqfuncoids,
        node.uniqCollations,
    )?;
    let prev_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(outer_desc.clone()));
    Ok(UniqueState {
        plan: node,
        ps_ExprContext,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        prev_slot,
        eq,
        have_prev: false,
    })
}

/// `ExecUnique`: return the first tuple of each run of duplicates.
pub fn exec_unique<'mcx, F>(
    node: &mut UniqueState<'mcx>,
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
    loop {
        let Some(outer_id) = fetch_outer(estate)? else {
            node.have_prev = false;
            exectuples::exec_clear_tuple(&mut node.prev_slot, mcx);
            let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
            exectuples::exec_clear_tuple(result_slot, mcx);
            return Ok(None);
        };
        if !node.have_prev {
            break node.store_and_return(estate, outer_id)?;
        }
        // SAFETY: the per-tuple context outlives this evaluation and is reset
        // right after (C: ExecQual under ecxt_per_tuple_memory; packed-capable
        // eq procs detoast-expand into it).
        unsafe {
            node.eq
                .arm_result_mcx_raw(estate.ecxt(node.ps_ExprContext).per_tuple_mcx())
        };
        let outer_slot = estate.slot_mut(outer_id);
        let mut slots = EvalSlots {
            scan: None,
            inner: Some(&mut *outer_slot),
            outer: Some(&mut node.prev_slot),
        };
        let matched = exec_qual(Some(&mut node.eq), &mut slots)?;
        estate.reset_expr_context(node.ps_ExprContext);
        if !matched {
            break node.store_and_return(estate, outer_id)?;
        }
    };
    Ok(Some(node.ps_ResultTupleSlot))
}

impl<'mcx> UniqueState<'mcx> {
    // ExecCopySlot into the retained prev slot; the copied image is shared
    // into the result slot (both die with the query context).
    fn store_and_return(
        &mut self,
        estate: &mut EStateData<'mcx>,
        outer_id: ExecSlotId,
    ) -> PgResult<()> {
        let mcx = estate.es_query_cxt;
        {
            let outer_slot = estate.slot_mut(outer_id);
            exectuples::exec_copy_slot(&mut self.prev_slot, outer_slot, mcx, mcx)?;
        }
        self.have_prev = true;
        let tup = exectuples::exec_fetch_slot_minimal_tuple(&mut self.prev_slot, mcx, mcx)?;
        let ptr = match tup {
            exectuples::FetchedMinimalTuple::Slot(t, _) => t,
            exectuples::FetchedMinimalTuple::Copied(_) => {
                unreachable!("prev slot was just materialized by exec_copy_slot")
            }
        };
        let result_slot = estate.slot_mut(self.ps_ResultTupleSlot);
        // SAFETY: the image lives in prev_slot until the next store, and the
        // result slot is re-stored before every return (no stale reads).
        unsafe { exectuples::exec_store_minimal_tuple_ptr(result_slot, mcx, ptr) };
        Ok(())
    }
}

/// `ExecEndUnique` node-local half; the caller ends the outer child.
pub fn exec_end_unique(node: &mut UniqueState<'_>) {
    node.prev_slot.base_mut().tts_tupleDescriptor = None;
    node.eq.release_frames();
    node.ps_ResultTupleDesc = None;
}

/// `ExecReScanUnique`; the caller rescans the outer child.
pub fn exec_rescan_unique<'mcx>(node: &mut UniqueState<'mcx>, estate: &mut EStateData<'mcx>) {
    node.have_prev = false;
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(&mut node.prev_slot, mcx);
    let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
    exectuples::exec_clear_tuple(result_slot, mcx);
}

// Exempt: all released in exec_end_unique (eq via release_frames).
mcx::forget_safe_struct!(
    UniqueState<'_> { plan, ps_ExprContext, ps_ResultTupleSlot, have_prev;
        ps_ResultTupleDesc, prev_slot, eq },
);
