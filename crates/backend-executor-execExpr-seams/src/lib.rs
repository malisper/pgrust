//! Seam declarations for the `backend-executor-execExpr` unit
//! (`executor/execExpr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_nodes::execexpr::SubPlanState;
use types_nodes::EStateData;

/// Which of a `SubPlanState`'s two projections an operation targets: `projLeft`
/// (lefthand exprs) or `projRight` (subselect output). The compiled
/// `ProjectionInfo`s and their result slots are owned by execExpr.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionKind {
    /// `node->projLeft`.
    Left,
    /// `node->projRight`.
    Right,
}

/// One read of a projection-result-slot attribute: its `Datum` plus is-null.
#[derive(Clone, Copy, Debug)]
pub struct SlotAttr {
    pub value: types_datum::Datum,
    pub isnull: bool,
}

/// Classification of `subplan->testexpr` for the hashed-subplan init path
/// (`IsA(testexpr, OpExpr)` / `is_andclause(testexpr)` / else). The `ncols`
/// in the and-clause arm is `list_length(BoolExpr.args)`.
#[derive(Clone, Copy, Debug)]
pub enum CombiningTestExpr {
    /// `IsA(subplan->testexpr, OpExpr)` — one combining operator.
    SingleOp,
    /// `is_andclause(subplan->testexpr)` — `ncols` combining operators.
    AndClause { ncols: i32 },
    /// Anything else — `elog(ERROR, "unrecognized testexpr type: %d")`; the
    /// owner carries the C `nodeTag(testexpr)`.
    Unrecognized { node_tag: i32 },
}

/// Resolved per-column combining-operator info (one `oplist` entry), used to
/// fill the hash control arrays in `ExecInitSubPlan`.
#[derive(Clone, Copy, Debug)]
pub struct CombiningOpInfo {
    /// `opexpr->opfuncid` — the (potentially cross-type) equality function.
    pub opfuncid: types_core::Oid,
    /// `get_opcode(rhs_eq_oper)` — RHS-type equality function.
    pub rhs_eq_funcoid: types_core::Oid,
    /// `left_hashfn` from `get_op_hash_functions`.
    pub left_hashfn: types_core::Oid,
    /// `right_hashfn` from `get_op_hash_functions`.
    pub right_hashfn: types_core::Oid,
    /// `opexpr->inputcollid` — input collation.
    pub inputcollid: types_core::Oid,
}

seam_core::seam!(
    /// `sstate->testexpr = ExecInitExpr((Expr *) subplan->testexpr, parent)`
    /// (nodeSubplan.c:833): compile the combining expression into the node's
    /// `testexpr` `ExprState`. The owner reads `subplan->testexpr` and the
    /// `parent` plan-state off `node` and the estate. Fallible.
    pub fn sub_init_testexpr<'mcx>(
        node: &mut SubPlanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `node->projLeft->pi_exprContext = econtext` then
    /// `ExecProject(node->projLeft)` / `ExecProject(node->projRight)`
    /// (nodeSubplan.c): project the named side using the supplied expression
    /// context (id into the EState pool) and store the result in that
    /// projection's output slot. The `Right` projection uses `node`'s own
    /// `innerecontext` (set at init), so `econtext` is ignored there. Fallible.
    pub fn sub_exec_project<'mcx>(
        node: &mut SubPlanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        econtext: types_nodes::EcxtId,
        which: ProjectionKind,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(node->proj*->pi_state.resultslot)` (executor.h): clear
    /// the named projection's result slot. Infallible per the C (no allocation).
    pub fn sub_clear_proj_result_slot<'mcx>(
        node: &mut SubPlanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        which: ProjectionKind,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `slot->tts_tupleDescriptor->natts` of the named projection's result
    /// slot (`slotAllNulls`/`slotNoNulls`). Infallible.
    pub fn proj_result_slot_natts(
        node: &SubPlanState<'_>,
        estate: &EStateData<'_>,
        which: ProjectionKind,
    ) -> i32
);

seam_core::seam!(
    /// `slot_attisnull(slot, attnum)` over the named projection's result slot
    /// (`slotAllNulls`/`slotNoNulls`). Fallible (`slot_getsomeattrs` ereport).
    pub fn proj_result_slot_attisnull<'mcx>(
        node: &mut SubPlanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        which: ProjectionKind,
        attnum: i32,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `slot_getattr(node->projLeft result slot, att, &isnull)` — read column
    /// `att` of the lefthand projection slot (`execTuplesUnequal` `slot1`).
    /// Fallible.
    pub fn proj_left_slot_getattr<'mcx>(
        node: &mut SubPlanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        att: types_core::AttrNumber,
    ) -> types_error::PgResult<SlotAttr>
);

seam_core::seam!(
    /// `ExecEvalExprSwitchContext(node->testexpr, econtext, &rownull)`
    /// (nodeSubplan.c:399): evaluate the combining expression over the econtext
    /// (id into the EState pool), returning `(result, isNull)`. Fallible.
    pub fn eval_testexpr_switch_context<'mcx>(
        node: &SubPlanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        econtext: types_nodes::EcxtId,
    ) -> types_error::PgResult<(types_datum::Datum, bool)>
);

seam_core::seam!(
    /// `classify` `subplan->testexpr` for the hashed-init path: `IsA(OpExpr)` /
    /// `is_andclause` / else (nodeSubplan.c:922-938). Infallible (pure node-tag
    /// inspection; the error arm is reported by the caller).
    pub fn classify_testexpr(node: &SubPlanState<'_>) -> CombiningTestExpr
);

seam_core::seam!(
    /// Resolve combining-operator `idx` of the testexpr's `oplist`
    /// (nodeSubplan.c:980-1001): look up `opfuncid`, the cross-type RHS equality
    /// op (`get_compatible_hash_operators` + `get_opcode`), the hash functions
    /// (`get_op_hash_functions`), and `inputcollid`. Fallible: the catalog
    /// `elog(ERROR)` arms ("could not find compatible hash operator", "could not
    /// find hash function") propagate.
    pub fn resolve_combining_op(
        node: &SubPlanState<'_>,
        idx: usize,
    ) -> types_error::PgResult<CombiningOpInfo>
);

seam_core::seam!(
    /// Build the lefthand/righthand tlists from the combining `oplist` (one
    /// `makeTargetEntry` over each `OpExpr`'s two args, reading the raw
    /// `subplan->testexpr` Expr tree), create their tupdescs + virtual slots,
    /// build `projLeft`/`projRight` projections, and build the `lhs_hash_expr` /
    /// `cur_eq_comp` `ExprState`s (nodeSubplan.c:964-978, 1009-1053). All of this
    /// is execExpr-owned machinery (`ExecTypeFromTL` / `ExecBuildProjectionInfo`
    /// / `ExecBuildHash32FromAttrs` / `ExecBuildGroupingEqual`) over the raw
    /// expression tree. The node's already-filled `numCols` / `keyColIdx` /
    /// `tab_collations` control fields are read here; `descRight`, the
    /// projections, and the expr states are written. The two transient fmgr
    /// arrays the C keeps on the stack are lent by the caller:
    /// `lhs_hash_funcs` (for `ExecBuildHash32FromAttrs`) and `cross_eq_funcoids`
    /// (for `ExecBuildGroupingEqual`). All allocation is in the EState's
    /// contexts; fallible.
    pub fn build_hash_projections_and_exprs<'mcx>(
        node: &mut SubPlanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        lhs_hash_funcs: &[types_core::fmgr::FmgrInfo],
        cross_eq_funcoids: &[types_core::Oid],
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
    /// `ExecPrepareExprList(exprList, estate)` (execExpr.c): compile a list of
    /// expression trees into a parallel list of executable `ExprState`s,
    /// allocated in the EState's per-query context. Fallible on OOM and on
    /// unsupported expression shapes (`ereport(ERROR)`).
    pub fn exec_prepare_expr_list<'mcx>(
        expr_list: &[types_nodes::primnodes::Expr],
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<
        mcx::PgVec<'mcx, mcx::PgBox<'mcx, types_nodes::execexpr::ExprState>>,
    >
);

seam_core::seam!(
    /// `(ItemPointer) DatumGetPointer(ExecEvalExprSwitchContext(state,
    /// econtext, &isnull))` (executor.h): evaluate a compiled scalar
    /// TID-yielding `ExprState` in the given expression context and dereference
    /// the resulting `Datum` as an `ItemPointer`, returning the pointed-to
    /// `ItemPointerData` and the is-null flag. (The owned model cannot
    /// reinterpret a `Datum` pointer word itself, so the dereference happens in
    /// the interpreter that produced it.) Fallible on `ereport(ERROR)`.
    pub fn exec_eval_tid_expr_switch_context<'mcx>(
        state: &types_nodes::execexpr::ExprState,
        econtext: types_nodes::EcxtId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<(types_tuple::heaptuple::ItemPointerData, bool)>
);

seam_core::seam!(
    /// `ExecEvalExprSwitchContext(state, econtext, &isnull)` evaluating a
    /// `tid[]`-yielding `ExprState` (executor.h): return the resulting array
    /// `Datum` and is-null flag, for the caller to deconstruct via
    /// `deconstruct_array_builtin`. Fallible on `ereport(ERROR)`.
    pub fn exec_eval_array_expr_switch_context<'mcx>(
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
