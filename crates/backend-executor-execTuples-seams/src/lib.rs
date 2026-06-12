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
    /// `planstate.ps_ResultTupleSlot`.
    pub fn exec_init_result_tuple_slot_tl(
        planstate: &mut types_nodes::PlanStateData,
        estate: &mut types_nodes::EStateData,
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
    /// tuple into the destination slot (`dstslot->tts_ops->copyslot`).
    pub fn exec_copy_slot(
        dstslot: &mut types_nodes::TupleTableSlot,
        srcslot: &types_nodes::TupleTableSlot,
    ) -> types_error::PgResult<()>
);
