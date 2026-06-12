//! Seam declarations for the `backend-executor-execTuples` unit
//! (`executor/execTuples.c`): slot creation and the slot-ops virtual calls
//! (`tuptable.h`'s `ExecClearTuple` / `ExecCopySlot` dispatch through the
//! `TupleTableSlotOps` tables that execTuples.c owns).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecInitResultTupleSlotTL(planstate, tts_ops)` (execTuples.c):
    /// initialize the node's result tuple type (from the plan's targetlist)
    /// and create its result slot in the `EState` slot pool, storing the id in
    /// `planstate.ps_ResultTupleSlot`. The slot is allocated in the pool's
    /// context (C: `MakeTupleTableSlot` pallocs in the query context), so the
    /// call is fallible on OOM.
    pub fn exec_init_result_tuple_slot_tl<'mcx>(
        planstate: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        tts_ops: types_nodes::TupleSlotKind,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(slot)` (tuptable.h): clear the slot's contents
    /// (`slot->tts_ops->clear`).
    pub fn exec_clear_tuple(slot: &mut types_nodes::TupleTableSlot) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecCopySlot(dstslot, srcslot)` (tuptable.h): copy the source slot's
    /// tuple into the destination slot (`dstslot->tts_ops->copyslot`). The
    /// copy allocates in `mcx`, the destination slot's memory context (C:
    /// `slot->tts_mcxt`; the trimmed slot carries no payload yet, so the
    /// owned model passes the context explicitly). Fallible on OOM.
    pub fn exec_copy_slot<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        dstslot: &mut types_nodes::TupleTableSlot,
        srcslot: &types_nodes::TupleTableSlot,
    ) -> types_error::PgResult<()>
);
