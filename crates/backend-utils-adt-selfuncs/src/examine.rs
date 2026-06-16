//! The variable-recognition layer of selfuncs.c — `examine_variable`,
//! `examine_simple_variable`, `get_restriction_variable`, `get_join_variables`,
//! and `ReleaseVariableStats`.
//!
//! ## Keystone block (RTE-carrier + node-arena)
//!
//! These functions locate the `pg_statistic` data for an expression. The
//! statistics-acquisition core, `examine_simple_variable`, reads
//! `root->simple_rte_array[var->varno]` (an opaque `RangeTblEntryId` in the
//! current `PlannerInfo`), dispatches on `rte->rtekind`, and for
//! `RTE_RELATION` runs `SearchSysCache3(STATRELATTINH, ...)` to pin a
//! `pg_statistic` `HeapTuple`; for `RTE_SUBQUERY`/`RTE_CTE` it recurses into the
//! subquery's `subroot`. None of that is reachable today: the planner's
//! `simple_rte_array` carries opaque RTE handles with no field accessors and no
//! syscache path (the RTE-carrier keystone is not landed), and the
//! `examine_variable` PlaceHolderVar/RelabelType stripping needs to re-allocate
//! a stripped `Node` into the planner arena, which the `&PlannerInfo` seam
//! signature cannot do (the arena is immutable here).
//!
//! Per mirror-PG-and-panic, the recognition entry points are kept structurally
//! intact and panic with a precise rationale when the stats-acquisition or
//! stripping boundary is reached, rather than silently returning a divergent
//! (stats-free) `VariableStatData`. The pure-arithmetic estimators in
//! [`crate::scalar`] / [`crate::ineq`] / [`crate::join`] operate over the
//! resolved [`VariableStatData`] and are fully ported; they become live once
//! this keystone lands.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::{NodeId, PlannerInfo, SpecialJoinInfo};
use types_selfuncs::VariableStatData;

/// `examine_variable(root, node, varRelid, &vardata)` (selfuncs.c) — look up
/// statistical data about an expression. Keystone-blocked: see the module
/// docs. The recognition reaches `examine_simple_variable`'s syscache lookup
/// over the opaque `simple_rte_array`, which is unported.
pub(crate) fn examine_variable<'mcx>(
    _mcx: Mcx<'mcx>,
    _root: &PlannerInfo,
    _node: NodeId,
    _var_relid: i32,
) -> PgResult<VariableStatData> {
    panic!(
        "selfuncs: examine_variable is keystone-blocked — examine_simple_variable reads \
         root->simple_rte_array[varno] (opaque RangeTblEntryId, no rtekind accessor) and runs \
         SearchSysCache3(STATRELATTINH, ...) to pin a pg_statistic tuple, plus the \
         PlaceHolderVar/RelabelType stripping re-allocates into the (here-immutable) planner \
         node arena. The RTE-carrier keystone must land first."
    )
}

/// Seam body for `examine_variable`.
pub fn seam_examine_variable<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    node: NodeId,
    var_relid: i32,
) -> PgResult<VariableStatData> {
    examine_variable(mcx, root, node, var_relid)
}

/// `get_restriction_variable(root, args, varRelid, &vardata, &other,
/// &varonleft)` (selfuncs.c) — recognize a `(var op const)` / `(const op var)`
/// restriction clause. Built on [`examine_variable`] (both sides) plus
/// `estimate_expression_value` on the "other" operand, so it inherits the same
/// keystone block.
pub(crate) fn get_restriction_variable<'mcx>(
    _mcx: Mcx<'mcx>,
    _root: &PlannerInfo,
    args: &[NodeId],
    _var_relid: i32,
) -> PgResult<Option<(VariableStatData, Expr, bool)>> {
    // Fail if not a binary opclause (probably shouldn't happen). This much is
    // structural and matches C's list_length(args) != 2 punt.
    if args.len() != 2 {
        return Ok(None);
    }
    panic!(
        "selfuncs: get_restriction_variable is keystone-blocked — it calls examine_variable on \
         both operands (RTE-carrier / pg_statistic syscache unported) and \
         estimate_expression_value (clauses.c, unported) on the non-variable operand. The \
         RTE-carrier keystone must land first."
    )
}

/// Seam body for `get_restriction_variable`.
pub fn seam_get_restriction_variable<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    args: &[NodeId],
    var_relid: i32,
) -> PgResult<Option<(VariableStatData, Expr, bool)>> {
    get_restriction_variable(mcx, root, args, var_relid)
}

/// `get_join_variables(root, args, sjinfo, &vardata1, &vardata2,
/// &join_is_reversed)` (selfuncs.c) — examine the two operands of a join
/// clause. Built on [`examine_variable`], so it inherits the same keystone
/// block. The `list_length(args) != 2` check is `elog(ERROR)` in C, modeled as
/// the same structural guard.
pub(crate) fn get_join_variables<'mcx>(
    _mcx: Mcx<'mcx>,
    _root: &PlannerInfo,
    args: &[NodeId],
    _sjinfo: &SpecialJoinInfo,
) -> PgResult<(VariableStatData, VariableStatData, bool)> {
    if args.len() != 2 {
        return Err(types_error::PgError::error(
            "join operator should take two arguments",
        ));
    }
    panic!(
        "selfuncs: get_join_variables is keystone-blocked — it calls examine_variable on both \
         join operands (RTE-carrier / pg_statistic syscache unported). The RTE-carrier keystone \
         must land first."
    )
}

/// Seam body for `get_join_variables`.
pub fn seam_get_join_variables<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    args: &[NodeId],
    sjinfo: &SpecialJoinInfo,
) -> PgResult<(VariableStatData, VariableStatData, bool)> {
    get_join_variables(mcx, root, args, sjinfo)
}

/// `ReleaseVariableStats(vardata)` (selfuncs.h) — run `vardata.freefunc` on the
/// pinned `statsTuple`. Since no live `statsTuple` can be acquired yet (see the
/// module docs), a `vardata` with `stats_tuple == None` is a no-op (matching C
/// `if (HeapTupleIsValid(vardata.statsTuple)) (*vardata.freefunc)(...)`); a
/// present tuple would require `ReleaseSysCache` / `pfree`, which is part of the
/// same keystone-blocked syscache path.
pub(crate) fn release_variable_stats(vardata: VariableStatData) {
    match vardata.stats_tuple {
        None => { /* C: HeapTupleIsValid(statsTuple) is false — nothing to free. */ }
        Some(_) => panic!(
            "selfuncs: ReleaseVariableStats on a live statsTuple is keystone-blocked — \
             releasing a pinned pg_statistic syscache tuple (ReleaseSysCache / pfree) is part of \
             the unported syscache path; no live statsTuple can be produced today, so this \
             branch is unreachable until the RTE-carrier keystone lands."
        ),
    }
}

/// Seam body for `release_variable_stats`.
pub fn seam_release_variable_stats(vardata: VariableStatData) {
    release_variable_stats(vardata)
}
