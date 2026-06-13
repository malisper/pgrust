//! `execExpr-domain-agg` family — domain coercion, aggregate transition, and
//! grouping/hash equality program builders.
//!
//! Owns `ExecInitCoerceToDomain`, `ExecBuildAggTrans` / `ExecBuildAggTransCall`,
//! `ExecBuildGroupingEqual`, `ExecBuildParamSetEqual`,
//! `ExecBuildHash32FromAttrs` / `ExecBuildHash32Expr`. The hashed-subplan
//! init path (`classify_testexpr` / `resolve_combining_op` /
//! `build_hash_projections_and_exprs`) is built on the grouping-equal + hash
//! builders, so its seams land here.

use types_core::fmgr::FmgrInfo;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::execexpr::SubPlanState;
use types_nodes::primnodes::{Expr, OpExpr, AND_EXPR};
use types_nodes::EStateData;

use backend_executor_execExpr_seams::{CombiningOpInfo, CombiningTestExpr};
use backend_utils_cache_lsyscache_seams as lsyscache;

/// Classify `subplan->testexpr` for the hashed-subplan init path
/// (`IsA(OpExpr)` / `is_andclause` / else) (nodeSubplan.c:922-938).
///
/// Mirrors the C:
/// ```c
/// if (IsA(subplan->testexpr, OpExpr))
///     oplist = list_make1(subplan->testexpr);          /* one combining op  */
/// else if (is_andclause(subplan->testexpr))
///     oplist = castNode(BoolExpr, subplan->testexpr)->args; /* ncols ops    */
/// else
///     elog(ERROR, "unrecognized testexpr type: %d", nodeTag(subplan->testexpr));
/// ncols = list_length(oplist);
/// ```
/// The `ncols` of the and-clause arm is `list_length(BoolExpr.args)`. The
/// `else` arm carries the C `nodeTag(testexpr)` so the caller can raise the
/// `elog(ERROR)`.
pub fn classify_testexpr(node: &SubPlanState<'_>) -> CombiningTestExpr {
    // subplan->testexpr — a hashable SubPlan always has a non-NULL combining
    // expression (the planner only sets useHashTable for ANY/EXISTS SubLinks
    // that carry a testexpr); the C dereferences it unconditionally.
    let subplan = node
        .subplan
        .as_ref()
        .expect("buildSubPlanHash: SubPlanState.subplan is NULL");
    let testexpr = subplan
        .testexpr
        .as_ref()
        .expect("buildSubPlanHash: hashable subplan->testexpr is NULL");

    match &**testexpr {
        // IsA(subplan->testexpr, OpExpr) — single combining operator
        // (oplist = list_make1(testexpr); ncols = 1).
        Expr::OpExpr(_) => CombiningTestExpr::SingleOp,
        // is_andclause(subplan->testexpr) — multiple combining operators
        // (oplist = castNode(BoolExpr, testexpr)->args; ncols =
        // list_length(args)). `is_andclause` is `IsA(BoolExpr) && boolop ==
        // AND_EXPR` (nodeFuncs.h:108-114).
        Expr::BoolExpr(b) if b.boolop == AND_EXPR => CombiningTestExpr::AndClause {
            ncols: b.args.len() as i32,
        },
        // else — the C `elog(ERROR, "unrecognized testexpr type: %d",
        // nodeTag(testexpr))`. The caller raises the elog with the carried
        // node tag.
        other => CombiningTestExpr::Unrecognized {
            node_tag: node_tag_of(other),
        },
    }
}

/// `nodeTag(node)` for the `else` arm of `classify_testexpr`'s dispatch — the
/// integer the C passes to `elog(ERROR, "unrecognized testexpr type: %d")`.
///
/// The value is purely diagnostic (it only formats the error text), and the
/// planner only ever builds a hashable `subplan->testexpr` as an `OpExpr` or an
/// AND-clause `BoolExpr`, so this arm is the genuine "shouldn't see anything
/// else" path. We surface the variant's `Expr`-enum discriminant rather than
/// fabricate a `NodeTag` ordinal table (no `NodeTag` enum is modeled in
/// types-nodes).
fn node_tag_of(expr: &Expr) -> i32 {
    // The two shapes the planner can build are handled by the SingleOp /
    // AndClause arms before this is reached; everything else is the C
    // "shouldn't see anything else" path. Matching PG's exact NodeTag ordinal
    // is not required by any consumer (the C only interpolates it into the
    // error message), so an unmodeled-tag sentinel is faithful.
    let _ = expr;
    -1
}

/// Resolve combining-operator `idx` of the testexpr's `oplist`
/// (nodeSubplan.c:980-1001): `opfuncid`, RHS-type equality op, hash functions,
/// `inputcollid`.
///
/// Mirrors the per-column body of the `foreach(l, oplist)` loop:
/// ```c
/// OpExpr *opexpr = lfirst_node(OpExpr, l);
/// cross_eq_funcoids[i-1] = opexpr->opfuncid;
/// /* fmgr_info(opexpr->opfuncid, &cur_eq_funcs[i-1]) — done by the caller */
/// if (!get_compatible_hash_operators(opexpr->opno, NULL, &rhs_eq_oper))
///     elog(ERROR, "could not find compatible hash operator for operator %u", opexpr->opno);
/// tab_eq_funcoids[i-1] = get_opcode(rhs_eq_oper);
/// if (!get_op_hash_functions(opexpr->opno, &left_hashfn, &right_hashfn))
///     elog(ERROR, "could not find hash function for hash operator %u", opexpr->opno);
/// /* fmgr_info(left_hashfn, &lhs_hash_funcs[i-1]); fmgr_info(right_hashfn, &tab_hash_funcs[i-1]) — caller */
/// tab_collations[i-1] = opexpr->inputcollid;
/// ```
/// `idx` is the 0-based oplist position (the C `i-1`). The lefthand-side
/// `oplist` is `list_make1(testexpr)` for a single OpExpr or `BoolExpr.args`
/// for the and-clause; element `idx` is the OpExpr to resolve.
pub fn resolve_combining_op(node: &SubPlanState<'_>, idx: usize) -> PgResult<CombiningOpInfo> {
    let subplan = node
        .subplan
        .as_ref()
        .expect("buildSubPlanHash: SubPlanState.subplan is NULL");
    let testexpr = subplan
        .testexpr
        .as_ref()
        .expect("buildSubPlanHash: hashable subplan->testexpr is NULL");

    // oplist = IsA(OpExpr) ? list_make1(testexpr) : castNode(BoolExpr)->args;
    // element `idx` is the `OpExpr` we resolve (`lfirst_node(OpExpr, l)`).
    let opexpr = oplist_op(testexpr, idx);

    // cross_eq_funcoids[i-1] = opexpr->opfuncid;   (the cross-type equality fn)
    let opfuncid = opexpr.opfuncid;

    // if (!get_compatible_hash_operators(opexpr->opno, NULL, &rhs_eq_oper))
    //     elog(ERROR, "could not find compatible hash operator for operator %u", opexpr->opno);
    // The C passes NULL for lhs_opno (it only wants the RHS operator); the seam
    // returns both, and we take the RHS.
    let (_lhs_eq_oper, rhs_eq_oper) = lsyscache::get_compatible_hash_operators::call(opexpr.opno)?
        .ok_or_else(|| {
            PgError::error(format!(
                "could not find compatible hash operator for operator {}",
                opexpr.opno
            ))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        })?;

    // sstate->tab_eq_funcoids[i-1] = get_opcode(rhs_eq_oper);
    let rhs_eq_funcoid = lsyscache::get_opcode::call(rhs_eq_oper)?;

    // if (!get_op_hash_functions(opexpr->opno, &left_hashfn, &right_hashfn))
    //     elog(ERROR, "could not find hash function for hash operator %u", opexpr->opno);
    let (left_hashfn, right_hashfn) =
        lsyscache::get_op_hash_functions::call(opexpr.opno)?.ok_or_else(|| {
            PgError::error(format!(
                "could not find hash function for hash operator {}",
                opexpr.opno
            ))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        })?;

    // sstate->tab_collations[i-1] = opexpr->inputcollid;
    let inputcollid = opexpr.inputcollid;

    // The two `fmgr_info` lookups the C performs here
    // (`fmgr_info(opexpr->opfuncid, &cur_eq_funcs[i-1])` and
    // `fmgr_info(left_hashfn/right_hashfn, …)`) are done by the caller in
    // `ExecInitSubPlan`, which owns the per-node fmgr arrays; this resolver only
    // returns the resolved OIDs.
    Ok(CombiningOpInfo {
        opfuncid,
        rhs_eq_funcoid,
        left_hashfn,
        right_hashfn,
        inputcollid,
    })
}

/// `lfirst_node(OpExpr, list_nth_cell(oplist, idx))` — the `idx`-th combining
/// `OpExpr` of the `subplan->testexpr` oplist, where the oplist is
/// `list_make1(testexpr)` for a single `OpExpr` or `castNode(BoolExpr,
/// testexpr)->args` for the and-clause. The C `lfirst_node` asserts the element
/// is an `OpExpr`, so a non-`OpExpr` element is a "can't happen".
fn oplist_op(testexpr: &Expr, idx: usize) -> &OpExpr {
    let elem = match testexpr {
        // oplist = list_make1(subplan->testexpr) — one element.
        Expr::OpExpr(op) => {
            assert!(idx == 0, "oplist index {idx} out of range for single OpExpr");
            return op;
        }
        // oplist = castNode(BoolExpr, subplan->testexpr)->args.
        Expr::BoolExpr(b) if b.boolop == AND_EXPR => b
            .args
            .get(idx)
            .unwrap_or_else(|| panic!("oplist index {idx} out of range for and-clause args")),
        other => panic!("resolve_combining_op: subplan->testexpr is neither OpExpr nor AND-clause BoolExpr: {other:?}"),
    };
    match elem {
        // lfirst_node(OpExpr, l) — the and-clause args are all OpExprs.
        Expr::OpExpr(op) => op,
        other => panic!("resolve_combining_op: and-clause arg {idx} is not an OpExpr: {other:?}"),
    }
}

/// Build the hashed-subplan projections + the `lhs_hash_expr` / `cur_eq_comp`
/// expression states (nodeSubplan.c:964-978, 1009-1053): `ExecTypeFromTL` /
/// `ExecBuildProjectionInfo` / `ExecBuildHash32FromAttrs` /
/// `ExecBuildGroupingEqual` over the raw testexpr tree.
///
/// Mirrors the tail of `buildSubPlanHash`:
/// ```c
/// /* assemble lefttlist/righttlist from each OpExpr's two args */
/// foreach(l, oplist) {
///     expr = linitial(opexpr->args); tle = makeTargetEntry(expr, i, NULL, false);
///     lefttlist = lappend(lefttlist, tle);
///     expr = lsecond(opexpr->args); tle = makeTargetEntry(expr, i, NULL, false);
///     righttlist = lappend(righttlist, tle);
/// }
/// tupDescLeft = ExecTypeFromTL(lefttlist);
/// slot = ExecInitExtraTupleSlot(estate, tupDescLeft, &TTSOpsVirtual);
/// sstate->projLeft = ExecBuildProjectionInfo(lefttlist, NULL, slot, parent, NULL);
/// sstate->descRight = tupDescRight = ExecTypeFromTL(righttlist);
/// slot = ExecInitExtraTupleSlot(estate, tupDescRight, &TTSOpsVirtual);
/// sstate->projRight = ExecBuildProjectionInfo(righttlist, sstate->innerecontext,
///                                             slot, sstate->planstate, NULL);
/// sstate->lhs_hash_expr = ExecBuildHash32FromAttrs(tupDescLeft, &TTSOpsVirtual,
///     lhs_hash_funcs, sstate->tab_collations, sstate->numCols, sstate->keyColIdx,
///     parent, 0);
/// sstate->cur_eq_comp = ExecBuildGroupingEqual(tupDescLeft, tupDescRight,
///     &TTSOpsVirtual, &TTSOpsMinimalTuple, ncols, sstate->keyColIdx,
///     cross_eq_funcoids, sstate->tab_collations, parent);
/// ```
/// The node's already-filled `numCols` / `keyColIdx` / `tab_collations` control
/// fields are read here; `descRight`, the projections, and the expr states are
/// written. `lhs_hash_funcs` feeds `ExecBuildHash32FromAttrs`; `cross_eq_funcoids`
/// feeds `ExecBuildGroupingEqual`.
pub fn build_hash_projections_and_exprs<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    lhs_hash_funcs: &[FmgrInfo],
    cross_eq_funcoids: &[Oid],
) -> PgResult<()> {
    let _ = (&node, &estate, lhs_hash_funcs, cross_eq_funcoids);

    // Step 1: assemble `lefttlist` / `righttlist` by walking the combining
    // `oplist` (`makeTargetEntry(linitial/lsecond(opexpr->args), i, NULL,
    // false)`). The OpExpr argument extraction reads the raw `subplan->testexpr`
    // tree.
    //
    // Step 2: `ExecTypeFromTL` each tlist into a TupleDesc, `ExecInitExtraTupleSlot`
    // a virtual slot for each, and `ExecBuildProjectionInfo` build `projLeft`
    // (parent econtext, filled later) and `projRight` (innerecontext). Store
    // `descRight`.
    //
    // Step 3: `ExecBuildHash32FromAttrs` build `lhs_hash_expr` and
    // `ExecBuildGroupingEqual` build `cur_eq_comp`.
    //
    // Every one of `ExecTypeFromTL`, `ExecBuildProjectionInfo`,
    // `ExecBuildHash32FromAttrs`, and `ExecBuildGroupingEqual` is the execExpr
    // step-emission spine: it allocates a fresh `ExprState`, walks the target
    // list / key columns, and pushes `ExprEvalStep`s via `ExprEvalPushStep` /
    // `ExecComputeSlotInfo` (the `EEOP_*_FETCHSOME` / `EEOP_*_VAR` /
    // `EEOP_ASSIGN_*` / `EEOP_HASHDATUM_*` / `EEOP_NOT_DISTINCT` opcodes). That
    // spine lives in the `execExpr_core` family, which has not landed yet
    // (its `exec_build_projection_info` / step-push / slot-info entry points are
    // still `todo!()`), and the projection/hash builders themselves are not yet
    // present as a callable surface. Mirror PG and panic until the spine lands,
    // rather than restructure around it or emit an approximate program.
    panic!(
        "build_hash_projections_and_exprs: the execExpr step-emission spine \
         (ExecTypeFromTL / ExecBuildProjectionInfo / ExecBuildHash32FromAttrs / \
         ExecBuildGroupingEqual + ExprEvalPushStep / ExecComputeSlotInfo) is not \
         yet ported"
    )
}
