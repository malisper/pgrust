//! Seam declarations for the `backend-executor-execUtils` unit
//! (`executor/execUtils.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecCreateScanSlotFromOuterPlan(estate, scanstate, tts_ops)`
    /// (execUtils.c): set up the node's scan tuple slot using the outer plan's
    /// result tuple type (`ExecGetResultType(outerPlanState(scanstate))`),
    /// storing the slot id in `scanstate.ss_ScanTupleSlot`.
    pub fn exec_create_scan_slot_from_outer_plan(
        estate: &mut types_nodes::EStateData,
        scanstate: &mut types_nodes::ScanStateData,
        tts_ops: types_nodes::TupleSlotKind,
    ) -> types_error::PgResult<()>
);
