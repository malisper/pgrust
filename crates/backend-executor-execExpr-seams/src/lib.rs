//! Seam declarations for the `backend-executor-execExpr` unit
//! (`executor/execExpr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `ExecBuildProjectionInfo(targetList, econtext, slot, parent,
    /// inputDesc)` (execExpr.c), marshaled over the owned tree: the owner
    /// extracts the target list (`planstate->plan->targetlist`), the node's
    /// `ps_ExprContext` and `ps_ResultTupleSlot` from `planstate` itself,
    /// because the owned tree cannot lend the target list and the node
    /// mutably at once. The compiled projection is allocated in the state
    /// tree's context (fallible on OOM); building can also `ereport(ERROR)`
    /// (unsupported expression shapes).
    pub fn exec_build_projection_info<'mcx>(
        planstate: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        input_desc: Option<&types_tuple::heaptuple::TupleDescData<'_>>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ProjectionInfo>>
);
