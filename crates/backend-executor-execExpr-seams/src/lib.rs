//! Seam declarations for the `backend-executor-execExpr` unit
//! (`executor/execExpr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

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

seam_core::seam!(
    /// `ExecInitExpr(node, parent)` (execExpr.c): compile a single expression
    /// tree into an executable `ExprState`, allocated in the EState's per-query
    /// context. The owned model lends the `parent` plan-state (for slot
    /// descriptors / param context) and the estate. Fallible on OOM and on
    /// unsupported expression shapes (`ereport(ERROR)`).
    pub fn exec_init_expr<'mcx>(
        node: &types_nodes::primnodes::Expr,
        parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState>>
);

seam_core::seam!(
    /// `ExecInitExprWithParams(node, ext_params)` (execExpr.c): compile a
    /// standalone expression tree with no parent `PlanState`, using only the
    /// supplied external params (C: `econtext->ecxt_param_list_info`). The owned
    /// model passes the evaluating `ExprContext`'s id and the estate so the
    /// owner reads `ecxt_param_list_info` off it; the compiled `ExprState` is
    /// allocated in the per-query context. Fallible on OOM and on unsupported
    /// expression shapes (`ereport(ERROR)`).
    pub fn exec_init_expr_with_params<'mcx>(
        node: &types_nodes::primnodes::Expr,
        econtext: types_nodes::EcxtId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState>>
);

seam_core::seam!(
    /// `ExecInitQual(qual, parent)` (execExpr.c): compile an implicitly-ANDed
    /// list of qual clauses into a single `ExprState`. A `None`/empty qual
    /// compiles to `None` (the C `NULL` ExprState, treated as always-true).
    /// Allocated in the per-query context; fallible on OOM / `ereport(ERROR)`.
    pub fn exec_init_qual<'mcx>(
        qual: Option<&[types_nodes::primnodes::Expr]>,
        parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState>>>
);

seam_core::seam!(
    /// `ExecEvalExprSwitchContext(state, econtext, &isnull)` (executor.h):
    /// evaluate a compiled `ExprState` in the given expression context (id into
    /// the EState pool), returning the result `Datum` and its is-null flag. The
    /// evaluation reads the econtext's linked tuples and runs in its per-tuple
    /// memory; fallible on `ereport(ERROR)` from the expression.
    pub fn exec_eval_expr_switch_context<'mcx>(
        state: &types_nodes::execexpr::ExprState,
        econtext: types_nodes::EcxtId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<(types_datum::Datum, bool)>
);

seam_core::seam!(
    /// `ExecQual(state, econtext)` (executor.h): evaluate a compiled boolean
    /// qual `ExprState` over the econtext (id into the EState pool), returning
    /// whether it passed (a `NULL` state is always-true, handled by the
    /// caller). Fallible on `ereport(ERROR)`.
    pub fn exec_qual<'mcx>(
        state: &types_nodes::execexpr::ExprState,
        econtext: types_nodes::EcxtId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecProject(projInfo)` (executor.h): form a projected result tuple
    /// using the node's compiled projection info and its expression context,
    /// storing the result in the projection's output slot. The owned model
    /// lends the node's `PlanState` head (carries `ps_ProjInfo`,
    /// `ps_ExprContext`, `ps_ResultTupleSlot`) and the estate; it returns the
    /// id of the slot the projection wrote (the C returned `TupleTableSlot *`).
    /// Fallible on `ereport(ERROR)` from a projection expression.
    pub fn exec_project<'mcx>(
        planstate: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<types_nodes::SlotId>
);
