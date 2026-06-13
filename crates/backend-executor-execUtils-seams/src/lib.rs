//! Seam declarations for the `backend-executor-execUtils` unit
//! (`executor/execUtils.c`).
//!
//! Consumers that can take a direct cargo dependency call the crate directly
//! (AGENTS.md: direct dependency by default). The owner installs every
//! declaration here from its `init_seams()`.

#![allow(non_snake_case)]

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

seam_core::seam!(
    /// `ExecAssignExprContext(estate, &node->js.ps)` (execUtils.c): create the
    /// node's per-node `ExprContext` and store its id in `ps_ExprContext`.
    /// Allocates a new context in the EState pool (fallible on OOM).
    pub fn exec_assign_expr_context<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        ps: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecAssignProjectionInfo(&node->js.ps, NULL)` (execUtils.c): build the
    /// node's projection info from its result tuple type. Allocates; can
    /// `ereport(ERROR)` on an unsupported target expression.
    pub fn exec_assign_projection_info<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        ps: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ResetExprContext(node->js.ps.ps_ExprContext)` (executor.h): reset the
    /// node's per-tuple memory context, freeing per-tuple expression storage.
    pub fn reset_per_tuple_expr_context<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        ps: &types_nodes::execnodes::PlanStateData<'mcx>,
    ) -> types_error::PgResult<()>
);
