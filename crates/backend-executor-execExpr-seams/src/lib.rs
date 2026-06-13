//! Seam declarations for the `backend-executor-execExpr` unit
//! (`executor/execExpr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `CreateExecutorState()` (execUtils.c): build a throwaway `EState` in a
    /// fresh per-query context. The PREPARE/EXECUTE/EXPLAIN drivers use it only
    /// to evaluate parameter expressions and never read its fields, so it
    /// crosses as the opaque [`types_nodes::parsestmt::EStateHandle`]. (Declared
    /// here alongside the parameter-evaluation seams that consume it — the
    /// throwaway-EState-for-params idiom lands with the expression evaluator.)
    /// Allocates.
    pub fn create_executor_state(
    ) -> types_error::PgResult<types_nodes::parsestmt::EStateHandle>
);

seam_core::seam!(
    /// `estate->es_param_list_info = params` — set the external param list on
    /// the throwaway executor state.
    pub fn estate_set_param_list_info(
        estate: types_nodes::parsestmt::EStateHandle,
        params: types_nodes::parsestmt::ParamListInfoHandle,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `FreeExecutorState(estate)` (execUtils.c): release the throwaway
    /// executor state and its per-query context.
    pub fn free_executor_state(
        estate: types_nodes::parsestmt::EStateHandle,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecPrepareExprList(exprs, estate)` (execExpr.c): compile a list of
    /// already analyzed parameter expression nodes into `ExprState`s, one per
    /// input node, in the throwaway executor state's context. The PREPARE
    /// driver threads the owned parameter nodes in and gets opaque
    /// [`types_nodes::parsestmt::ExprStateHandle`]s back. Allocates / can
    /// `ereport(ERROR)`.
    pub fn exec_prepare_expr_list<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        params: &[mcx::PgBox<'mcx, types_nodes::nodes::Node<'mcx>>],
        estate: types_nodes::parsestmt::EStateHandle,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_nodes::parsestmt::ExprStateHandle>>
);

seam_core::seam!(
    /// For the `i`-th prepared `ExprState`, set `paramLI->params[i]`:
    /// `ptype = param_types[i]`, `pflags = PARAM_FLAG_CONST`,
    /// `value = ExecEvalExprSwitchContext(n, GetPerTupleExprContext(estate),
    /// &prm->isnull)` (prepare.c `EvaluateParams`). fmgr/`Datum` value layer.
    /// Can `ereport(ERROR)`.
    pub fn eval_exec_param_into_list(
        param_li: types_nodes::parsestmt::ParamListInfoHandle,
        exprstate: types_nodes::parsestmt::ExprStateHandle,
        param_index: i32,
        ptype: types_core::Oid,
        estate: types_nodes::parsestmt::EStateHandle,
    ) -> types_error::PgResult<()>
);

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
    /// `ExecInitExprList(nodes, parent)` (execExpr.c): compile a list of
    /// expressions into a list of `ExprState`s (`lappend(ExecInitExpr(e))`).
    /// A `None` element (the C NULL `Expr *`) compiles to a `None` cell (the
    /// C NULL `ExprState *`), preserving positional correspondence. Allocated
    /// in the per-query context; fallible on OOM / `ereport(ERROR)`.
    pub fn exec_init_expr_list<'mcx>(
        nodes: &[Option<&types_nodes::primnodes::Expr>],
        parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, Option<types_nodes::execexpr::ExprState>>>
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

/// Which expression-list of a `HashJoin` to compile (`ExecInitQual` inputs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HashJoinQualKind {
    /// `node->join.plan.qual` → `hjstate->js.ps.qual`.
    Qual,
    /// `node->join.joinqual` → `hjstate->js.joinqual`.
    JoinQual,
    /// `node->hashclauses` → `hjstate->hashclauses`.
    HashClauses,
}

seam_core::seam!(
    /// `ExecInitQual(qual, parent)` (execExpr.c): compile one of the hash-join
    /// node's qual expression lists into an `ExprState`, returning `None` for an
    /// empty list (the C `NULL`). The owner reads the source list off the node's
    /// plan and stores the result on the matching field. Allocates; can
    /// `ereport(ERROR)`.
    pub fn exec_init_hashjoin_qual<'mcx>(
        node: &mut types_nodes::nodehashjoin::HashJoinState<'mcx>,
        kind: HashJoinQualKind,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecQual(state, econtext)` (executor.h/execExprInterp.c): evaluate a
    /// boolean qual `ExprState` in the node's per-tuple context. `which`
    /// selects `js.joinqual` (true) or `js.ps.qual` (false). Returns the C
    /// boolean result; can `ereport(ERROR)`.
    pub fn exec_hashjoin_qual<'mcx>(
        node: &mut types_nodes::nodehashjoin::HashJoinState<'mcx>,
        joinqual: bool,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecQualAndReset(state, econtext)` (executor.h): evaluate a compiled
    /// boolean qual `ExprState` over the econtext (id into the EState pool),
    /// then reset the econtext's per-tuple memory (`ResetExprContext`). Returns
    /// whether the qual passed. Fallible on `ereport(ERROR)`.
    pub fn exec_qual_and_reset<'mcx>(
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

seam_core::seam!(
    /// `ExecProject(node->js.ps.ps_ProjInfo)` (executor.h): form the projection
    /// into the node's result slot, returning its slot id. Can `ereport(ERROR)`.
    pub fn exec_hashjoin_project<'mcx>(
        node: &mut types_nodes::nodehashjoin::HashJoinState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<types_nodes::SlotId>
);

seam_core::seam!(
    /// `DatumGetUInt32(ExecEvalExprSwitchContext(hj_OuterHash, econtext,
    /// &isnull))` (execExprInterp.c): evaluate the outer hash-value ExprState in
    /// the node's per-tuple context. Writes the is-null flag and returns the
    /// `uint32` hash value. Can `ereport(ERROR)`.
    pub fn eval_outer_hash<'mcx>(
        node: &mut types_nodes::nodehashjoin::HashJoinState<'mcx>,
        isnull: &mut bool,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<u32>
);
