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
/// Canonical definition in `types-tuple`.
pub use types_tuple::backend_access_common_heaptuple::SlotAttr;

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
    /// The `SlotId` (into the EState slot pool) of the named projection's
    /// result slot — `node->proj*->pi_state.resultslot`. The slot is
    /// execExpr-owned (created by `ExecBuildProjectionInfo`); its id is exposed
    /// here so the canonical execGrouping `TupleHashTable` operations
    /// (`LookupTupleHashEntry` / `FindTupleHashEntry`) can be driven over the
    /// just-projected tuple. Infallible (the slot exists once the projection
    /// was built at init).
    pub fn sub_proj_result_slot_id<'mcx>(
        node: &SubPlanState<'mcx>,
        estate: &EStateData<'mcx>,
        which: ProjectionKind,
    ) -> types_nodes::SlotId
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
    ) -> types_error::PgResult<SlotAttr<'mcx>>
);

seam_core::seam!(
    /// `ExecEvalExprSwitchContext(node->testexpr, econtext, &rownull)`
    /// (nodeSubplan.c:399): evaluate the combining expression over the econtext
    /// (id into the EState pool), returning `(result, isNull)`. Fallible.
    pub fn eval_testexpr_switch_context<'mcx>(
        node: &mut SubPlanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        econtext: types_nodes::EcxtId,
    ) -> types_error::PgResult<(types_tuple::Datum<'mcx>, bool)>
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
    /// `CreateExecutorState()` (execUtils.c): build a throwaway `EState` in a
    /// fresh per-query context, owned by the caller (the PREPARE/EXECUTE/EXPLAIN
    /// drivers create it only to evaluate parameter expressions). Returns the
    /// real owned [`EStateData`]; the driver sets `es_param_list_info` on it and
    /// threads `&mut` into the parameter-evaluation seams. Allocates.
    pub fn create_executor_state<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, EStateData<'mcx>>>
);

seam_core::seam!(
    /// `FreeExecutorState(estate)` (execUtils.c): release the throwaway
    /// executor state and its per-query context, consuming the owned `EState`.
    pub fn free_executor_state<'mcx>(
        estate: mcx::PgBox<'mcx, EStateData<'mcx>>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// For the `i`-th prepared `ExprState`, set `paramLI->params[i]`:
    /// `ptype = param_types[i]`, `pflags = PARAM_FLAG_CONST`,
    /// `value = ExecEvalExprSwitchContext(n, GetPerTupleExprContext(estate),
    /// &prm->isnull)` (prepare.c `EvaluateParams`). fmgr/`Datum` value layer.
    /// `param_li` is the value param list being filled (`makeParamList` result,
    /// mutated in place); `exprstate` is the real compiled [`ExprState`] from
    /// `exec_prepare_expr_list`. Can `ereport(ERROR)`.
    pub fn eval_exec_param_into_list<'mcx>(
        param_li: &mut types_nodes::params::ParamListInfoData<'static>,
        exprstate: &mut types_nodes::execexpr::ExprState<'mcx>,
        param_index: i32,
        ptype: types_core::Oid,
        estate: &mut EStateData<'mcx>,
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
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ProjectionInfo<'mcx>>>
);

seam_core::seam!(
    /// `ExecInitExpr(node, parent)` (execExpr.c): compile a single expression
    /// tree into an executable `ExprState`, allocated in the EState's per-query
    /// context. The owned model lends the `parent` plan-state (for slot
    /// descriptors / param context) and the estate. Fallible on OOM and on
    /// unsupported expression shapes (`ereport(ERROR)`).
    pub fn exec_init_expr<'mcx, 'e>(
        node: &types_nodes::primnodes::Expr<'e>,
        parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>
);

seam_core::seam!(
    /// `ExecInitExprWithParams(node, ext_params)` (execExpr.c): compile a
    /// standalone expression tree with no parent `PlanState`, using only the
    /// supplied external params (C: `econtext->ecxt_param_list_info`). The owned
    /// model passes the evaluating `ExprContext`'s id and the estate so the
    /// owner reads `ecxt_param_list_info` off it; the compiled `ExprState` is
    /// allocated in the per-query context. Fallible on OOM and on unsupported
    /// expression shapes (`ereport(ERROR)`).
    pub fn exec_init_expr_with_params<'mcx, 'e>(
        node: &types_nodes::primnodes::Expr<'e>,
        econtext: types_nodes::EcxtId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>
);

seam_core::seam!(
    /// `ExecInitQual(qual, parent)` (execExpr.c): compile an implicitly-ANDed
    /// list of qual clauses into a single `ExprState`. A `None`/empty qual
    /// compiles to `None` (the C `NULL` ExprState, treated as always-true).
    /// Allocated in the per-query context; fallible on OOM / `ereport(ERROR)`.
    pub fn exec_init_qual<'mcx>(
        qual: Option<&[types_nodes::primnodes::Expr<'mcx>]>,
        parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>>
);

seam_core::seam!(
    /// `ExecInitQual(qual, NULL)` (execExpr.c) — the parent-less variant of
    /// [`exec_init_qual`]: compile an already-preprocessed implicitly-ANDed
    /// list of qual clauses into a single `ExprState`, with no enclosing
    /// `PlanState`. Unlike [`exec_prepare_qual`] this does NOT re-run
    /// `expression_planner` (the caller — e.g. COPY FROM, whose `whereClause`
    /// was already const-folded/canonicalized in `DoCopy` — has done the
    /// planning). A `None`/empty qual compiles to `None` (the always-true C
    /// `NULL` ExprState). Allocated in `es_query_cxt`; fallible on OOM /
    /// `ereport(ERROR)`.
    pub fn exec_init_qual_no_parent<'mcx>(
        qual: Option<&[types_nodes::primnodes::Expr<'mcx>]>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>>
);

seam_core::seam!(
    /// `ExecPrepareQual(qual, estate)` (execExpr.c): prepare a standalone qual
    /// (implicitly-ANDed `List` of `Expr`) for execution, with no parent
    /// `PlanState`. C switches to `estate->es_query_cxt`, runs
    /// `expression_planner((Expr *) qual)` (const-fold / SQL-function inline),
    /// then `ExecInitQual(qual, NULL)`. An empty qual (the C `NIL`) compiles to
    /// `None` (the C `NULL` ExprState, always-true) WITHOUT touching the
    /// planner. A non-empty qual reaches `expression_planner` (optimizer/
    /// planner.c, unported — no reachable owner seam) and loud-panics there,
    /// mirror-PG-and-panic; the `ExecInitQual` compile that follows is this
    /// crate's own logic. Used by the index-build path (`FormIndexDatum`
    /// partial-index predicate). Allocated in `es_query_cxt`; fallible on OOM /
    /// `ereport(ERROR)`.
    pub fn exec_prepare_qual<'a, 'mcx>(
        qual: Option<&[types_nodes::primnodes::Expr<'a>]>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>>
);

seam_core::seam!(
    /// `ExecInitExprList(nodes, parent)` (execExpr.c): compile a list of
    /// expressions into a list of `ExprState`s (`lappend(ExecInitExpr(e))`).
    /// A `None` element (the C NULL `Expr *`) compiles to a `None` cell (the
    /// C NULL `ExprState *`), preserving positional correspondence. Allocated
    /// in the per-query context; fallible on OOM / `ereport(ERROR)`.
    pub fn exec_init_expr_list<'mcx>(
        nodes: &[Option<&types_nodes::primnodes::Expr<'mcx>>],
        parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, Option<types_nodes::execexpr::ExprState<'mcx>>>>
);

seam_core::seam!(
    /// `ExecInitExprList(nodes, NULL)` (execExpr.c) — the parentless variant.
    /// `ValuesNext` compiles a VALUES row's expression list with `parent =
    /// NULL` so nothing in the transient per-row eval state links into the
    /// permanent plan-state tree (and so JIT is disabled for these single-use
    /// expressions). A `None` element (the C NULL `Expr *`) compiles to a
    /// `None` cell, preserving positional correspondence. Allocated in the
    /// per-query context; fallible on OOM / `ereport(ERROR)`.
    pub fn exec_init_expr_list_no_parent<'mcx>(
        nodes: &[Option<&types_nodes::primnodes::Expr<'mcx>>],
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, Option<types_nodes::execexpr::ExprState<'mcx>>>>
);

seam_core::seam!(
    /// `ExecBuildAggTrans(aggstate, phase, doSort, doHash, nullcheck)`
    /// (execExpr.c:3679): build the transition/combine evaluation program for
    /// one Agg grouping-sets phase. Owned by execExpr (the opcode-emission
    /// recursion is private to `execExpr_core`), but called by nodeAgg's
    /// `ExecInitAgg`, which sits ABOVE execExpr — so the call crosses this seam.
    /// The `AggStateData<'mcx>` lives in `backend-executor-nodeAgg` (above the
    /// seams crate), so it is carried as the erased, tag-checked
    /// [`AggStateLive`](types_nodes::aggstate_carrier::AggStateLive) trait
    /// object; the execExpr implementation downcasts it back to the concrete
    /// `AggStateData`. `phase` indexes `aggstate.phases`. Allocated in the
    /// per-query context; fallible on `ereport(ERROR)`.
    pub fn exec_build_agg_trans<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        aggstate: &mut (dyn types_nodes::aggstate_carrier::AggStateLive<'mcx> + 'mcx),
        phase: i32,
        do_sort: bool,
        do_hash: bool,
        nullcheck: bool,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>
);

seam_core::seam!(
    /// `ExecEvalExprSwitchContext(state, econtext, &isnull)` (executor.h):
    /// evaluate a compiled `ExprState` in the given expression context (id into
    /// the EState pool), returning the result `Datum` and its is-null flag. The
    /// evaluation reads the econtext's linked tuples and runs in its per-tuple
    /// memory; fallible on `ereport(ERROR)` from the expression.
    pub fn exec_eval_expr_switch_context<'mcx>(
        state: &mut types_nodes::execexpr::ExprState<'mcx>,
        econtext: types_nodes::EcxtId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<(types_tuple::Datum<'mcx>, bool)>
);

seam_core::seam!(
    /// `ExecPrepareExprList(exprList, estate)` (execExpr.c): compile a list of
    /// expression trees into a parallel list of executable `ExprState`s,
    /// allocated in the EState's per-query context. Fallible on OOM and on
    /// unsupported expression shapes (`ereport(ERROR)`).
    pub fn exec_prepare_expr_list<'a, 'mcx>(
        expr_list: &[types_nodes::primnodes::Expr<'a>],
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<
        mcx::PgVec<'mcx, mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>,
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
        state: &mut types_nodes::execexpr::ExprState<'mcx>,
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
        state: &mut types_nodes::execexpr::ExprState<'mcx>,
        econtext: types_nodes::EcxtId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<(types_tuple::Datum<'mcx>, bool)>
);

seam_core::seam!(
    /// `ExecQual(state, econtext)` (executor.h): evaluate a compiled boolean
    /// qual `ExprState` over the econtext (id into the EState pool), returning
    /// whether it passed (a `NULL` state is always-true, handled by the
    /// caller). Fallible on `ereport(ERROR)`.
    pub fn exec_qual<'mcx>(
        state: &mut types_nodes::execexpr::ExprState<'mcx>,
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
        state: &mut types_nodes::execexpr::ExprState<'mcx>,
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
    /// `ExecProject(projInfo)` (executor.h) of an explicitly-supplied
    /// `ProjectionInfo` (rather than one read off a `PlanState`): form the
    /// projected result tuple using `proj_info` and its expression context,
    /// returning the id of the slot the projection wrote (the C returned
    /// `TupleTableSlot *`). Used for the MERGE per-action `mas_proj`
    /// projections, which live on the `MergeActionState`, not a node's
    /// `ps_ProjInfo`. Fallible on `ereport(ERROR)` from a projection expression.
    pub fn exec_project_info<'mcx>(
        proj_info: &mut types_nodes::execexpr::ProjectionInfo<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<types_nodes::SlotId>
);

seam_core::seam!(
    /// `mas_whenqual = ExecInitQual((List *) action->qual, &mtstate->ps)`
    /// (execExpr.c): compile one MERGE action's WHEN [NOT MATCHED] AND
    /// conditions into an `ExprState`. A `None` qual (the C `NIL`) compiles to
    /// `None` (the C `NULL`, treated as always-true). The qual `Node`→qual-list
    /// extraction and `ExecInitQual` are owned by execExpr; the per-action
    /// loop/dispatch stays in nodeModifyTable. Allocated in the per-query
    /// context; fallible on OOM / `ereport(ERROR)`.
    pub fn exec_init_merge_when_qual<'mcx>(
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        qual: Option<&[types_nodes::primnodes::Expr<'mcx>]>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>>
);

seam_core::seam!(
    /// `ExecBuildProjectionInfo(action->targetList, econtext, tgtslot,
    /// &mtstate->ps, tgtdesc)` (execExpr.c): build one MERGE INSERT action's
    /// projection over the explicit `target_list`, projecting into the slot
    /// `tgt_slot` (the root "new" tuple slot, or the partitioned root's
    /// `mt_root_tuple_slot`), using `tgt_desc_rel`'s descriptor (the root
    /// relation's). The slot and the desc-source relation are resolved
    /// in-crate by `ExecInitMerge`'s partitioned-vs-inherited control flow; this
    /// seam is a thin `ExecBuildProjectionInfo` leaf. Allocated in the per-query
    /// context; fallible on OOM / `ereport(ERROR)`.
    pub fn exec_build_merge_insert_projection<'mcx>(
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        target_list: &[types_nodes::TargetEntry<'mcx>],
        econtext: types_nodes::EcxtId,
        tgt_slot: types_nodes::SlotId,
        tgt_desc_rel: types_nodes::RriId,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ProjectionInfo<'mcx>>>
);

seam_core::seam!(
    /// `ExecBuildUpdateProjection(action->targetList, true,
    /// action->updateColnos, relationDesc, econtext,
    /// resultRelInfo->ri_newTupleSlot, &mtstate->ps)` (execExpr.c): build one
    /// MERGE UPDATE action's "new tuple" projection over the explicit
    /// `target_list` / `update_colnos`, using `result_rel_info`'s relation
    /// descriptor and its `ri_newTupleSlot`. A thin `ExecBuildUpdateProjection`
    /// leaf; the per-action loop/dispatch stays in nodeModifyTable. Allocated in
    /// the per-query context; fallible on OOM / `ereport(ERROR)`.
    pub fn exec_build_merge_update_projection<'mcx>(
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        target_list: &[types_nodes::TargetEntry<'mcx>],
        update_colnos: &[i32],
        econtext: types_nodes::EcxtId,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ProjectionInfo<'mcx>>>
);

seam_core::seam!(
    /// `ri_MergeJoinCondition = ExecInitQual((List *) joinCondition,
    /// &mtstate->ps)` (nodeModifyTable.c): compile the MERGE join condition for
    /// `result_rel_info` and store the compiled `ExprState` on its pooled
    /// `ResultRelInfo.ri_MergeJoinCondition`. The `joinCondition` is the plan
    /// node's `mergeJoinConditions` entry (a `Node` list, `None` = NULL = the
    /// always-true condition). `ExecInitQual` and the `Node`→qual-list
    /// extraction are owned by execExpr. Fallible on OOM / `ereport(ERROR)`.
    pub fn exec_init_merge_join_condition<'mcx>(
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        join_condition: Option<&[types_nodes::primnodes::Expr<'mcx>]>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The inherited-root WITH CHECK OPTION / RETURNING setup of `ExecInitMerge`
    /// (nodeModifyTable.c L3853-3958): when the MERGE targets an inherited
    /// (non-partitioned) table with INSERT actions, the root `ResultRelInfo` is
    /// not in the `resultRelInfo[]` array, so initialize its WCO constraints and
    /// RETURNING projection here — taking the first plan WCO/RETURNING list as
    /// reference, `build_attrmap_by_name` + `map_variable_attnos` it to the
    /// root's attnos when the root and first result rel differ, `ExecInitQual`
    /// each WCO qual into `ri_WithCheckOptions`/`ri_WithCheckOptionExprs`, and
    /// `ExecBuildProjectionInfo` the RETURNING list into `ri_returningList`/
    /// `ri_projectReturning`. Reads the `ModifyTable` plan node's WCO/RETURNING
    /// lists (the owner interprets them) and the rewrite attmap machinery. A
    /// no-op unless the root differs from `resultRelInfo[0]`, the root is not
    /// partitioned, and `MERGE_INSERT` is among the subcommands. Fallible on
    /// OOM / `ereport(ERROR)`.
    pub fn exec_init_merge_inherited_root<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        root_result_rel_info: types_nodes::RriId,
        first_result_rel: types_nodes::RriId,
        econtext: types_nodes::EcxtId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The WITH CHECK OPTION map-and-build of `ExecInitPartitionInfo`
    /// (execPartition.c L549-614): take `ref_wco_list` (the first plan's WCO
    /// list), `build_attrmap_by_name(partrel, firstResultRel)` +
    /// `map_variable_attnos(.., firstVarno, 0, attmap, partrel reltype)` it into
    /// the leaf partition's attribute numbers, `ExecInitQual` each
    /// `WithCheckOption.qual`, and store `ri_WithCheckOptions` /
    /// `ri_WithCheckOptionExprs` on the leaf `ResultRelInfo` (id into the EState
    /// pool). The `leaf_part_rri` and `first_result_rel` give the partition's and
    /// reference relation's tupdescs (read off the pool). The attmap build,
    /// `map_variable_attnos` rewrite, and `ExecInitQual` are execExpr/rewrite-
    /// owned. Allocated in the per-query context; fallible on OOM /
    /// `ereport(ERROR)`.
    pub fn partition_init_with_check_options<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        leaf_part_rri: types_nodes::RriId,
        first_result_rel: types_nodes::RriId,
        first_varno: types_core::primitive::Index,
        ref_wco_list: &[types_nodes::nodes::Node<'mcx>],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The RETURNING map-and-build of `ExecInitPartitionInfo` (execPartition.c
    /// L616-679): take `ref_returning_list` (the first plan's RETURNING list),
    /// `build_attrmap_by_name(partrel, firstResultRel)` +
    /// `map_variable_attnos(.., firstVarno, 0, attmap, partrel reltype)` it into
    /// the leaf partition's attribute numbers, store `ri_returningList`, and
    /// build `ri_projectReturning` via `ExecBuildProjectionInfo` using
    /// `mtstate->ps.ps_ResultTupleSlot` / `ps_ExprContext` and the partition's
    /// tupdesc. The attmap/rewrite/projection-build machinery is execExpr/
    /// rewrite-owned. Allocated in the per-query context; fallible on OOM /
    /// `ereport(ERROR)`.
    pub fn partition_init_returning<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        leaf_part_rri: types_nodes::RriId,
        first_result_rel: types_nodes::RriId,
        first_varno: types_core::primitive::Index,
        ref_returning_list: &[types_nodes::primnodes::TargetEntry<'mcx>],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The ON CONFLICT DO UPDATE `OnConflictSetState` build of
    /// `ExecInitPartitionInfo` (execPartition.c L730-862): `makeNode(
    /// OnConflictSetState)`, create the per-partition `oc_Existing` slot
    /// (`table_slot_create(partrel)`), then `ExecGetRootToChildMap(leaf_part_rri)`
    /// — when the map is `NULL` (rowtype matches root) reuse the root
    /// `ri_onConflict`'s `oc_ProjSlot` / `oc_ProjInfo` / `oc_WhereClause`;
    /// otherwise translate `on_conflict_set` twice (`map_variable_attnos` over
    /// `INNER_VAR` then `firstVarno`, with `build_attrmap_by_name(partrel,
    /// firstResultRel)`), `adjust_partition_colnos(on_conflict_cols)` to the
    /// partition, `table_slot_create(partrel)` the projection slot, build the
    /// UPDATE SET projection via `ExecBuildUpdateProjection`, and (when
    /// `on_conflict_where` is non-NULL) map+`ExecInitQual` the WHERE clause.
    /// Stores the built `OnConflictSetState` on `leaf_part_rri.ri_onConflict`.
    /// All the slot create / attmap / projection / qual machinery is execExpr/
    /// tableam/rewrite-owned. Allocated in the per-query context; fallible on
    /// OOM / `ereport(ERROR)`.
    pub fn partition_init_on_conflict_update<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        leaf_part_rri: types_nodes::RriId,
        root_result_rel_info: types_nodes::RriId,
        first_result_rel: types_nodes::RriId,
        first_varno: types_core::primitive::Index,
        on_conflict_set: &[types_nodes::primnodes::TargetEntry<'mcx>],
        on_conflict_cols: &[i32],
        on_conflict_where: Option<&[types_nodes::primnodes::Expr<'mcx>]>,
    ) -> types_error::PgResult<()>
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

seam_core::seam!(
    /// `ExecPrepareExpr(node, estate)` (execExpr.c): compile a single
    /// expression tree into an executable `ExprState` for use *outside* a
    /// normal executor node (parent = NULL), switching into the EState's
    /// per-query context first. Used by `ExecInitGenerated` to prepare the
    /// stored-generated-column generation expressions. Allocated in the
    /// per-query context; fallible on OOM / `ereport(ERROR)`.
    pub fn exec_prepare_expr<'a, 'mcx>(
        node: &types_nodes::primnodes::Expr<'a>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>
);

seam_core::seam!(
    /// `ExecProject(resultRelInfo->ri_projectNew)` driven from a result
    /// relation (the UPDATE/INSERT "new tuple" build in nodeModifyTable):
    /// project through the relation's `ri_projectNew` projection, wiring its
    /// econtext's `ecxt_outertuple = plan_slot` and (for UPDATE)
    /// `ecxt_scantuple = old_slot`, and returning the projection's output slot
    /// (`ri_newTupleSlot`). The projection lives on the pooled `ResultRelInfo`,
    /// so the owner reads it by id. Fallible on `ereport(ERROR)`.
    pub fn exec_project_new_tuple<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        plan_slot: types_nodes::SlotId,
        old_slot: Option<types_nodes::SlotId>,
    ) -> types_error::PgResult<types_nodes::SlotId>
);

seam_core::seam!(
    /// `ExecBuildUpdateProjection(targetList, evalTargetList, targetColnos,
    /// relDesc, econtext, slot, parent)` (execExpr.c): build the UPDATE
    /// "new tuple" projection for a result relation, storing it on the pooled
    /// `ResultRelInfo` (`ri_projectNew`, with `ri_projectNewInfoValid` set).
    /// The `update_colnos` map relation columns to subplan output columns.
    /// Allocated in the per-query context; fallible on OOM / `ereport(ERROR)`.
    pub fn exec_build_update_projection<'mcx>(
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        update_colnos: &[i32],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `foreach(ll, wcoList) { wcoExpr = ExecInitQual(wco->qual, &mtstate->ps);
    /// wcoExprs = lappend(wcoExprs, wcoExpr); }` then `ri_WithCheckOptions =
    /// wcoList; ri_WithCheckOptionExprs = wcoExprs;` (nodeModifyTable.c /
    /// execExpr.c): compile every WITH CHECK OPTION constraint qual in `wco_list`
    /// against `mtstate->ps` and store the list + the compiled expr states on
    /// the pooled `ResultRelInfo`. The `WithCheckOption` node's `qual`
    /// extraction and `ExecInitQual` compilation are owned by the rewrite/
    /// execExpr units; the parse-node `qual` is not modeled in the trimmed
    /// `Node` enum, so the whole per-rel compile is routed here. Fallible on
    /// OOM / `ereport(ERROR)`.
    pub fn exec_init_with_check_options<'mcx>(
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        wco_list: &[types_nodes::nodes::Node<'mcx>],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecBuildProjectionInfo(rlist, econtext, slot, &mtstate->ps,
    /// resultRelInfo->ri_RelationDesc->rd_att)` (execExpr.c): build the
    /// RETURNING projection for one result relation from its RETURNING target
    /// list `rlist` and the shared result slot/econtext set up on `mtstate->ps`
    /// (`ps_ResultTupleSlot` / `ps_ExprContext`), storing the compiled
    /// projection on the pooled `ResultRelInfo.ri_projectReturning` and the
    /// list on `ri_returningList`. Allocated in the per-query context; fallible
    /// on OOM / `ereport(ERROR)` (unsupported expression shapes).
    pub fn exec_build_returning_projection<'mcx>(
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        rlist: &[types_nodes::TargetEntry<'mcx>],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecBuildUpdateProjection(node->onConflictSet, true,
    /// node->onConflictCols, relationDesc, econtext, onconfl->oc_ProjSlot,
    /// &mtstate->ps)` (execExpr.c): build the ON CONFLICT DO UPDATE SET
    /// projection for the (single) result relation from the explicit
    /// `on_conflict_set` target list and `on_conflict_cols` column map,
    /// projecting into `proj_slot` (the `oc_ProjSlot` of the table's type) and
    /// using `result_rel_info`'s relation descriptor. A thin
    /// `ExecBuildUpdateProjection` leaf; the surrounding `OnConflictSetState`
    /// construction and field stores stay in nodeModifyTable. Allocated in the
    /// per-query context; fallible on OOM / `ereport(ERROR)`.
    pub fn exec_build_on_conflict_set_projection<'mcx>(
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        on_conflict_set: &[types_nodes::TargetEntry<'mcx>],
        on_conflict_cols: &[i32],
        econtext: types_nodes::EcxtId,
        proj_slot: types_nodes::SlotId,
    ) -> types_error::PgResult<types_nodes::execexpr::ProjectionInfo<'mcx>>
);

seam_core::seam!(
    /// `ExecInitQual((List *) node->onConflictWhere, &mtstate->ps)`
    /// (execExpr.c): compile the ON CONFLICT DO UPDATE WHERE clause into an
    /// `ExprState`. A `None` clause (the C `NULL`) yields `None`. A thin
    /// `ExecInitQual` leaf; the `OnConflictSetState` field store stays
    /// in-crate. Allocated in the per-query context; fallible on OOM /
    /// `ereport(ERROR)`.
    pub fn exec_init_on_conflict_where<'mcx>(
        mtstate: &mut types_nodes::ModifyTableState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        on_conflict_where: Option<&[types_nodes::primnodes::Expr<'mcx>]>,
    ) -> types_error::PgResult<Option<types_nodes::execexpr::ExprState<'mcx>>>
);

seam_core::seam!(
    /// `ExecProject(resultRelInfo->ri_projectReturning)` (executor.h): form the
    /// RETURNING projection for a result relation, returning its output slot
    /// id. The caller (nodeModifyTable's `ExecProcessReturning`) has already
    /// wired the projection's `pi_exprContext` slots
    /// (`ecxt_scantuple`/`ecxt_outertuple`/`ecxt_oldtuple`/`ecxt_newtuple`) and
    /// its `pi_state.flags` (the `EEO_FLAG_*` OLD/NEW bits); the owner just
    /// evaluates the compiled projection. The projection lives on the pooled
    /// `ResultRelInfo`, so the owner reads it by id. Can `ereport(ERROR)` from
    /// a projection expression.
    pub fn exec_project_returning<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
    ) -> types_error::PgResult<types_nodes::SlotId>
);

seam_core::seam!(
    /// `ExecBuildParamSetEqual(desc, lops, rops, eqfunctions, collations,
    /// param_exprs, parent)` (execExpr.c): build an `ExprState` evaluable with
    /// `ExecQual()` that returns true when the expression context's inner/outer
    /// tuples are equal, comparing each of `param_exprs`'s columns with the
    /// matching `eqfunctions[i]` under `collations[i]` (NULLs compare equal).
    /// Used by `ExecInitMemoize` to build the non-binary `cache_eq_expr` over the
    /// node's `hashkeydesc`; the inner slot ops are `TTSOpsMinimalTuple` and the
    /// outer slot ops `TTSOpsVirtual`. The owned model passes the result
    /// `desc`/`lops`/`rops` directly (the C `parent` is only used for slot
    /// descriptors / param context). The compiled `ExprState` is allocated in
    /// `mcx`; fallible on OOM / `ereport(ERROR)`.
    #[allow(clippy::too_many_arguments)]
    pub fn exec_build_param_set_equal<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        desc: &types_tuple::heaptuple::TupleDescData<'mcx>,
        lops: types_nodes::TupleSlotKind,
        rops: types_nodes::TupleSlotKind,
        eqfunctions: &[types_core::Oid],
        collations: &[types_core::Oid],
        param_exprs: &[types_nodes::primnodes::Expr<'mcx>],
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>
);

seam_core::seam!(
    /// `ExecBuildHash32Expr(desc, ops, hashfunc_oids, collations, hash_exprs,
    /// opstrict, parent, init_value, keep_nulls)` (execExpr.c:4302): compile an
    /// `ExprState` that hashes the `hash_exprs` expression list into a single
    /// `uint32` hash value (the per-side hash-value program used by hash joins —
    /// `hjstate->hj_OuterHash` / the inner `HashState`'s `hash_expr`). Each key
    /// `i` is evaluated, its value fed through `fmgr_info(hashfunc_oids[i])`, and
    /// combined via the `HASHDATUM_FIRST`/`_NEXT32`(`_STRICT`) opcodes;
    /// `opstrict[i]` + `!keep_nulls` selects the NULL-aborting strict variant.
    /// `init_value` optionally seeds the running hash. The owned model passes the
    /// node's result `desc`/`ops` directly; the C `parent` (`PlanState *`) reaches
    /// the `EState` for SubPlan attribution, so the non-owning `es_link` back-link
    /// is threaded and stamped on the compiled `ExprState` (a hash key may itself
    /// be a correlated SubPlan, e.g. `t1.a = (SELECT min(a) ...)`, which compiles
    /// to a SUBPLAN step that needs `es_subplanstates`). The compiled `ExprState`
    /// is allocated in `mcx`; fallible on OOM / `ereport(ERROR)`.
    #[allow(clippy::too_many_arguments)]
    pub fn exec_build_hash32_expr<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        es_link: types_nodes::execnodes::EStateLink,
        desc: &types_tuple::heaptuple::TupleDescData<'mcx>,
        ops: types_nodes::TupleSlotKind,
        hashfunc_oids: &[types_core::Oid],
        collations: &[types_core::Oid],
        hash_exprs: &[types_nodes::primnodes::Expr<'mcx>],
        opstrict: &[bool],
        init_value: u32,
        keep_nulls: bool,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>
);

seam_core::seam!(
    /// `ExecBuildHash32FromAttrs(desc, ops, hashfunctions, collations, numCols,
    /// keyColIdx, parent, init_value)` (execExpr.c:4143): compile an `ExprState`
    /// that hashes `num_cols` inner-tuple attributes (named by `key_col_idx`,
    /// 1-based) with the per-column `hashfunctions`, combining the results and
    /// optionally seeding with `init_value`. Built by `BuildTupleHashTable` for
    /// the table's `tab_hash_expr`. The `parent` is only used for JIT/slot
    /// attribution (not modeled), so the owned model takes only `mcx` + the
    /// node's result `desc`/`ops`. Allocated in `mcx`; fallible.
    #[allow(clippy::too_many_arguments)]
    pub fn exec_build_hash32_from_attrs<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        desc: &types_tuple::heaptuple::TupleDescData<'mcx>,
        ops: types_nodes::TupleSlotKind,
        hashfunctions: &[types_core::fmgr::FmgrInfo],
        collations: &[types_core::Oid],
        num_cols: i32,
        key_col_idx: &[types_core::primitive::AttrNumber],
        init_value: u32,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>
);

seam_core::seam!(
    /// `ExecBuildGroupingEqual(ldesc, rdesc, lops, rops, numCols, keyColIdx,
    /// eqfunctions, collations, parent)` (execExpr.c:4467): compile an
    /// `ExprState` (usable with `ExecQual`) returning true iff the inner/outer
    /// tuples are NOT DISTINCT across `num_cols` columns. `num_cols == 0`
    /// returns `None` (the C `NULL`, an always-true qual). Built by
    /// `BuildTupleHashTable` for `tab_eq_func` and by `execTuplesMatchPrepare`.
    /// `parent` (JIT/slot attribution) is not modeled. Allocated in `mcx`;
    /// fallible.
    #[allow(clippy::too_many_arguments)]
    pub fn exec_build_grouping_equal<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        ldesc: &types_tuple::heaptuple::TupleDescData<'mcx>,
        rdesc: &types_tuple::heaptuple::TupleDescData<'mcx>,
        lops: types_nodes::TupleSlotKind,
        rops: types_nodes::TupleSlotKind,
        num_cols: i32,
        key_col_idx: &[types_core::primitive::AttrNumber],
        eqfunctions: &[types_core::Oid],
        collations: &[types_core::Oid],
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>>
);
