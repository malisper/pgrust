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
