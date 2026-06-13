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
    /// `ExecAssignExprContext(estate, planstate)` (execUtils.c): create the
    /// node's per-node expression context (`CreateExprContext(estate)`) and
    /// store its id in `planstate.ps_ExprContext`. Allocates in the EState's
    /// per-query context, so fallible on OOM.
    pub fn exec_assign_expr_context<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        planstate: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CreateExprContext(estate)` (execUtils.c): create a fresh standalone
    /// `ExprContext` in the EState's pool, returning its id. Allocates in the
    /// per-query context (plus a child per-tuple context), so fallible on OOM.
    pub fn create_expr_context<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<types_nodes::EcxtId>
);

seam_core::seam!(
    /// `ExecGetCommonSlotOps(planstates, nplans)` (execUtils.c): if all the
    /// child `PlanState`s return the same fixed slot type, return that slot
    /// ops identity; otherwise `None`. `nplans <= 0` returns `None`. Reads
    /// each child's result slot ops, which it computes from the node — fallible
    /// because `ExecGetResultSlotOps` can run node-init work that
    /// `ereport(ERROR)`s.
    pub fn exec_get_common_slot_ops<'mcx>(
        planstates: &[Option<mcx::PgBox<'mcx, types_nodes::PlanStateNode<'mcx>>>],
        nplans: i32,
    ) -> types_error::PgResult<Option<types_nodes::TupleSlotKind>>
);

seam_core::seam!(
    /// `UpdateChangedParamSet(node, newchg)` (execUtils.c): add the params in
    /// `newchg` that this node depends on (`node->allParam`) to the node's
    /// `chgParam` set. Growth allocates `chgParam` in the per-query context,
    /// so the call takes that context and is fallible on OOM.
    pub fn update_changed_param_set<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        node: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        newchg: &types_nodes::Bitmapset<'_>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecAssignProjectionInfo(planstate, inputDesc)` (execUtils.c): build
    /// the node's `ps_ProjInfo` from its result slot and target list (using the
    /// node's `ps_ResultTupleSlot`/`ps_ExprContext`). The owned model lends the
    /// estate too, since the projection builder reaches the result slot through
    /// it. Allocates the compiled projection, so fallible on OOM; building can
    /// also `ereport(ERROR)` for unsupported expression shapes.
    pub fn exec_assign_projection_info<'mcx>(
        planstate: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        input_desc: Option<&types_tuple::heaptuple::TupleDescData<'_>>,
    ) -> types_error::PgResult<()>
);
