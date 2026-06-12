//! Seam declarations for the `backend-executor-execUtils` unit
//! (`executor/execUtils.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ReScanExprContext(econtext)` (execUtils.c): reset an expression
    /// context in preparation for a rescan of its plan node — run (and forget)
    /// the registered shutdown callbacks, then reset the per-tuple memory.
    /// Callbacks may `ereport(ERROR)`.
    pub fn re_scan_expr_context(
        econtext: &mut types_nodes::ExprContext,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecCreateScanSlotFromOuterPlan(estate, scanstate, tts_ops)`
    /// (execUtils.c): set up the node's scan tuple slot using the outer plan's
    /// result tuple type (`ExecGetResultType(outerPlanState(scanstate))`),
    /// storing the slot id in `scanstate.ss_ScanTupleSlot`. The slot is
    /// allocated in the pool's context, so the call is fallible on OOM.
    pub fn exec_create_scan_slot_from_outer_plan<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        scanstate: &mut types_nodes::execnodes::ScanStateData<'mcx>,
        tts_ops: types_nodes::TupleSlotKind,
    ) -> types_error::PgResult<()>
);
