// nodeGroup.c: the copied first tuple of each group feeds the qual and
// projection as OUTER; match program per execTuplesMatchPrepare.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::execexpr::{exec_build_grouping_equal, exec_project, exec_qual, EvalSlots, ExprState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{vec_with_capacity_in, PgBox, PgVec};
use ::types_error::PgResult;
use ::types_nodes::plannodes::Group;
use ::types_slot::{SlotData, TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

pub struct GroupState<'mcx> {
    pub plan: &'mcx Group<'mcx>,
    pub ps_ExprContext: EcxtId,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    firsttuple_slot: SlotData<'mcx>,
    // None when numCols == 0 (every key proved constant): one group.
    eq: Option<PgBox<'mcx, ExprState<'mcx>>>,
    qual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    proj: PgBox<'mcx, ExprState<'mcx>>,
    grp_done: bool,
    have_first: bool,
}

/// `ExecInitGroup` minus child linkage; the caller inits the outer child
/// and compiles qual/projection under its subplan env.
pub fn exec_init_group<'mcx>(
    node: &'mcx Group<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_desc: &Rc<TupleDescData<'static>>,
    result_desc: Rc<TupleDescData<'static>>,
    qual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    proj: PgBox<'mcx, ExprState<'mcx>>,
) -> PgResult<GroupState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    let mcx = estate.es_query_cxt;
    let ps_ExprContext = estate.exec_assign_expr_context();
    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::Virtual);

    let num_cols = node.numCols as usize;
    debug_assert!(node.grpColIdx.len() == num_cols);
    let eq = if num_cols > 0 {
        let mut eqfuncoids: PgVec<'mcx, u32> = vec_with_capacity_in(mcx, num_cols)?;
        for &op in node.grpOperators {
            eqfuncoids.push(lsyscache::get_opcode(op)?);
        }
        Some(exec_build_grouping_equal(
            mcx,
            outer_desc,
            outer_desc,
            node.grpColIdx,
            &eqfuncoids,
            node.grpCollations,
        )?)
    } else {
        None
    };
    let firsttuple_slot = exectuples::make_tuple_table_slot(
        mcx,
        TupleSlotKind::MinimalTuple,
        Some(outer_desc.clone()),
    );
    Ok(GroupState {
        plan: node,
        ps_ExprContext,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        firsttuple_slot,
        eq,
        qual,
        proj,
        grp_done: false,
        have_first: false,
    })
}

pub fn exec_group<'mcx, F>(
    node: &mut GroupState<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut fetch_outer: F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    lane_group_cfi()?;
    if node.grp_done {
        return Ok(None);
    }
    loop {
        let Some(outer_id) = fetch_outer(estate)? else {
            lane_group_eof(node);
            return Ok(None);
        };
        if let Some(result) = lane_group_feed(node, estate, outer_id)? {
            return Ok(Some(result));
        }
    }
}

// ===========================================================================
// Lane-executor-v2 streaming-group seam. The lane's GroupOp lives in
// `execmain/src/lanev2.rs`; the per-tuple body below IS `exec_group`'s (the
// Volcano loop above calls the same functions), so the lane runs the SAME
// grouping-equality program, first-tuple copy, qual, and projection — no
// reimplementation, and a Volcano fallback at any call boundary sees exactly
// C's state (grp_done / have_first / the retained first-tuple slot).
// ===========================================================================

/// C's ExecGroup entry interrupt check (conditional, exactly the Volcano
/// entry's), exposed for the lane driver.
pub fn lane_group_cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        postgres_seams::check_for_interrupts::call()?;
    }
    Ok(())
}

/// `exec_group`'s top-of-call drained guard (`grp_done`), for the lane driver.
pub fn lane_group_done(node: &GroupState<'_>) -> bool {
    node.grp_done
}

/// One incoming (sorted) outer tuple — `exec_group`'s per-tuple body: the
/// first tuple, or one whose grouping keys differ from the retained
/// first-of-group tuple, starts a new group (copied into the first-tuple
/// slot); the group head then runs the HAVING qual and, on pass, is projected
/// and returned (`Some(result slot)`). A same-group duplicate — or a group
/// head failing the qual — emits nothing (`None`). Zero grouping columns
/// (every key proved constant) = one group: every tuple after the first is a
/// duplicate, exactly the Volcano loop's `eq == None` continue.
pub fn lane_group_feed<'mcx>(
    node: &mut GroupState<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
) -> PgResult<Option<ExecSlotId>> {
    if node.have_first {
        let Some(eq) = node.eq.as_mut() else {
            // Zero grouping columns: the whole input is one group.
            return Ok(None);
        };
        // SAFETY: per-tuple context outlives the eval; reset per input
        // tuple (C ExecQualAndReset).
        unsafe { eq.arm_result_mcx_raw(estate.ecxt(node.ps_ExprContext).per_tuple_mcx()) };
        estate.reset_expr_context(node.ps_ExprContext);
        let outer_slot = estate.slot_mut(outer_id);
        let mut slots = EvalSlots {
            scan: None,
            inner: Some(&mut node.firsttuple_slot),
            outer: Some(&mut *outer_slot),
        };
        if exec_qual(Some(eq), &mut slots)? {
            return Ok(None);
        }
    }
    node.store_first(estate, outer_id)?;
    if node.check_qual(estate)? {
        return node.project(estate);
    }
    Ok(None)
}

/// `exec_group`'s child-exhausted arm: mark the node drained (no slot
/// clearing — C's ExecGroup returns NULL leaving the retained tuple as-is).
pub fn lane_group_eof(node: &mut GroupState<'_>) {
    node.grp_done = true;
}

impl<'mcx> GroupState<'mcx> {
    fn store_first(
        &mut self,
        estate: &mut EStateData<'mcx>,
        outer_id: ExecSlotId,
    ) -> PgResult<()> {
        let mcx = estate.es_query_cxt;
        let outer_slot = estate.slot_mut(outer_id);
        exectuples::exec_copy_slot(&mut self.firsttuple_slot, outer_slot, mcx, mcx)?;
        self.have_first = true;
        Ok(())
    }

    // C resolves an initplan's PARAM_EXEC lazily inside ExecEvalParamExec;
    // this executor hoists instead, but only once a first-of-group row
    // actually exists to run the qual over (a fully-exhausted outer child
    // must never touch a param C would never read).
    fn check_qual(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        if let Some(q) = self.qual.as_deref() {
            let deps = q.param_exec_deps();
            if !deps.is_empty() {
                ::executils::exec_eval_param_exec_params(estate, deps)?;
            }
        }
        let mut slots =
            EvalSlots { scan: None, inner: None, outer: Some(&mut self.firsttuple_slot) };
        exec_qual(self.qual.as_deref_mut(), &mut slots)
    }

    fn project(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        let deps = self.proj.param_exec_deps();
        if !deps.is_empty() {
            ::executils::exec_eval_param_exec_params(estate, deps)?;
        }
        let mcx = estate.es_query_cxt;
        let result_slot = estate.slot_mut(self.ps_ResultTupleSlot);
        let mut slots =
            EvalSlots { scan: None, inner: None, outer: Some(&mut self.firsttuple_slot) };
        exec_project(&mut self.proj, &mut slots, result_slot, mcx)?;
        Ok(Some(self.ps_ResultTupleSlot))
    }
}

/// `ExecEndGroup` node-local half; the caller ends the outer child.
pub fn exec_end_group(node: &mut GroupState<'_>) {
    node.firsttuple_slot.base_mut().tts_tupleDescriptor = None;
    if let Some(eq) = node.eq.as_mut() {
        eq.release_frames();
    }
    if let Some(q) = node.qual.as_mut() {
        q.release_frames();
    }
    node.proj.release_frames();
    node.qual = None;
    node.ps_ResultTupleDesc = None;
}

/// `ExecReScanGroup`; the caller rescans the outer child.
pub fn exec_rescan_group<'mcx>(node: &mut GroupState<'mcx>, estate: &mut EStateData<'mcx>) {
    node.grp_done = false;
    node.have_first = false;
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(&mut node.firsttuple_slot, mcx);
}

// Exempt: all released in exec_end_group (eq/qual/proj via release_frames).
mcx::forget_safe_struct!(
    GroupState<'_> { plan, ps_ExprContext, ps_ResultTupleSlot, grp_done, have_first;
        ps_ResultTupleDesc, firsttuple_slot, eq, qual, proj },
);
