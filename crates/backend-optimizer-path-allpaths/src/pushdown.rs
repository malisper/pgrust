//! Subquery pushdown-safety cluster (allpaths.c).
//!
//! Ports the machinery `set_subquery_pathlist` uses to decide whether the outer
//! query's restriction clauses can be pushed down into a subquery RTE, to push
//! them, and to trim unused subquery outputs:
//!
//! * [`subquery_is_pushdown_safe`] (allpaths.c:3628) + [`recurse_pushdown_safe`]
//!   (3684) + [`check_output_expressions`] (3753) + [`compare_tlist_datatypes`]
//!   (3849) + [`targetIsInAllPartitionLists`] (3882) — the per-column safety
//!   analysis that fills `safety_info.unsafe_flags[]`.
//! * [`qual_is_pushdown_safe`] (3925) — per-clause safety verdict.
//! * [`subquery_push_qual`] (4026) + [`recurse_push_qual`] (4074) — push a safe
//!   qual into the subquery's WHERE/HAVING.
//! * [`check_and_push_window_quals`] (2454) + [`find_window_run_conditions`]
//!   (2263) — the WindowAgg run-condition optimization.
//! * [`remove_unused_subquery_outputs`] (4126) — NULL-out unreferenced outputs.
//!
//! All operate over the owned `Query<'mcx>` value model (`types_nodes`).
//! `find_window_run_conditions` dispatches to the `SupportRequestWFuncMonotonic`
//! support function through a registry table (mirroring
//! `backend-optimizer-util-clauses::support_simplify`): a window function with no
//! registered monotonic-support kernel declines, which is exactly C's behavior
//! for a `prosupport` that returns NULL for this request kind.

extern crate alloc;

use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{AttrNumber, Index, Oid};
use types_error::{PgError, PgResult};
use types_nodes::copy_query::Query;
use types_nodes::nodes::{ntag, Node};
use types_nodes::parsenodes::RangeTblEntry;
use types_nodes::primnodes::{Expr, OpExpr};
use types_nodes::rawnodes::{SortGroupClause, WindowClause, SETOP_EXCEPT};
use types_pathnodes::{NodeId, PlannerInfo, RelId, Relids, RinfoId};

use backend_nodes_core::makefuncs::make_and_qual;
use backend_nodes_core::nodefuncs::{
    expr_collation, expr_type, expr_typmod, expression_returns_set, set_opfuncid,
};

/// Deep-copy a slice of `Expr` into `mcx` via `Expr::clone_in` (C copyObject).
/// The derived `Expr::clone` panics on an owned-subtree child
/// (`Aggref`/`SubLink`/`SubPlan`).
fn clone_exprs_in(exprs: &[Expr], mcx: Mcx<'_>) -> PgResult<Vec<Expr>> {
    let mut out = Vec::with_capacity(exprs.len());
    for e in exprs {
        out.push(e.clone_in(mcx)?);
    }
    Ok(out)
}

/* ==========================================================================
 * pushdown_safety_info struct + UNSAFE_* bit flags (allpaths.c:52-67)
 * ======================================================================== */

/// `UNSAFE_HAS_VOLATILE_FUNC` (allpaths.c:53).
pub(crate) const UNSAFE_HAS_VOLATILE_FUNC: u8 = 1 << 0;
/// `UNSAFE_HAS_SET_FUNC` (allpaths.c:54).
pub(crate) const UNSAFE_HAS_SET_FUNC: u8 = 1 << 1;
/// `UNSAFE_NOTIN_DISTINCTON_CLAUSE` (allpaths.c:55).
pub(crate) const UNSAFE_NOTIN_DISTINCTON_CLAUSE: u8 = 1 << 2;
/// `UNSAFE_NOTIN_PARTITIONBY_CLAUSE` (allpaths.c:56).
pub(crate) const UNSAFE_NOTIN_PARTITIONBY_CLAUSE: u8 = 1 << 3;
/// `UNSAFE_TYPE_MISMATCH` (allpaths.c:57).
pub(crate) const UNSAFE_TYPE_MISMATCH: u8 = 1 << 4;

/// `pushdown_safety_info` (allpaths.c:60-67). `unsafe_flags` is indexed by
/// subquery tlist `resno` (1-based); slot 0 is the C `[0]` padding that
/// `qual_is_pushdown_safe`/`check_output_expressions` never address with a real
/// column, so we size it `len(targetList)+1` exactly as C does.
pub(crate) struct PushdownSafetyInfo {
    /// `unsigned char *unsafeFlags` — bitmask of reasons why each tlist column
    /// is unsafe to reference in a pushed-down qual.
    pub(crate) unsafe_flags: Vec<u8>,
    /// `bool unsafeVolatile` — don't push down volatile quals.
    pub(crate) unsafe_volatile: bool,
    /// `bool unsafeLeaky` — don't push down leaky quals.
    pub(crate) unsafe_leaky: bool,
}

impl PushdownSafetyInfo {
    /// `memset(&safetyInfo, 0, ...)` + `palloc0((list_length+1))` (allpaths.c:2556).
    pub(crate) fn new(tlist_len: usize) -> Self {
        PushdownSafetyInfo {
            unsafe_flags: alloc::vec![0u8; tlist_len + 1],
            unsafe_volatile: false,
            unsafe_leaky: false,
        }
    }
}

/// `pushdown_safe_type` (allpaths.c:70-76).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PushdownSafe {
    /// `PUSHDOWN_UNSAFE` — unsafe to push qual into subquery.
    Unsafe,
    /// `PUSHDOWN_SAFE` — safe to push qual into subquery.
    Safe,
    /// `PUSHDOWN_WINDOWCLAUSE_RUNCOND` — unsafe, but may work as a WindowClause
    /// run condition.
    WindowclauseRuncond,
}

/// `FirstLowInvalidHeapAttributeNumber` (sysattr.h): system columns occupy
/// `-1 .. FirstLowInvalidHeapAttributeNumber+1`; attribute bitmapsets are offset
/// by this so attno 0 (whole-row) maps to a valid bit.
const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -8;

/* ==========================================================================
 * SupportRequestWFuncMonotonic dispatch (prosupport for window functions)
 * ======================================================================== */

/// `MONOTONICFUNC_NONE` (supportnodes.h).
pub(crate) const MONOTONICFUNC_NONE: u32 = 0;
/// `MONOTONICFUNC_INCREASING`.
pub(crate) const MONOTONICFUNC_INCREASING: u32 = 1 << 0;
/// `MONOTONICFUNC_DECREASING`.
pub(crate) const MONOTONICFUNC_DECREASING: u32 = 1 << 1;
/// `MONOTONICFUNC_BOTH`.
pub(crate) const MONOTONICFUNC_BOTH: u32 = MONOTONICFUNC_INCREASING | MONOTONICFUNC_DECREASING;

/// A decomposed `SupportRequestWFuncMonotonic` kernel: given the window
/// function's OID and its `WindowClause`, return the monotonic property bitmask
/// (`SupportRequestWFuncMonotonic.monotonic`). A window function whose
/// `prosupport` does not handle this request returns NULL in C; in the owned
/// model that is an *unregistered* OID (handled in [`call_support_wfunc_monotonic`]).
pub(crate) type SupportWFuncMonotonicFn = fn(winfnoid: Oid, wclause: &WindowClause) -> u32;

fn wfunc_monotonic_table()
-> &'static std::sync::Mutex<std::collections::HashMap<Oid, SupportWFuncMonotonicFn>> {
    static T: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<Oid, SupportWFuncMonotonicFn>>,
    > = std::sync::OnceLock::new();
    T.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Register a `SupportRequestWFuncMonotonic` kernel under its `prosupport` OID.
#[allow(dead_code)]
pub(crate) fn register_support_wfunc_monotonic(
    prosupport: Oid,
    func: SupportWFuncMonotonicFn,
) -> Option<SupportWFuncMonotonicFn> {
    wfunc_monotonic_table()
        .lock()
        .expect("wfunc-monotonic table lock")
        .insert(prosupport, func)
}

/// Run the `prosupport`'s `SupportRequestWFuncMonotonic` kernel, returning the
/// monotonic bitmask, or `None` when the support function declines (no
/// registered kernel = `res == NULL` in C, i.e. `OidFunctionCall1` of a support
/// function that does not handle this request kind).
fn call_support_wfunc_monotonic(prosupport: Oid, winfnoid: Oid, wclause: &WindowClause) -> Option<u32> {
    let func = wfunc_monotonic_table()
        .lock()
        .expect("wfunc-monotonic table lock")
        .get(&prosupport)
        .copied();
    func.map(|f| f(winfnoid, wclause))
}

/* ==========================================================================
 * Small Node/Expr accessors over the owned model.
 * ======================================================================== */

/// Resolve a `NodePtr` (a boxed `Node`) that is expected to wrap a
/// `SortGroupClause`. Returns `None` if it is some other node kind.
fn node_as_sortgroupclause<'a>(node: &'a Node<'_>) -> Option<&'a SortGroupClause> {
    node.as_sortgroupclause()
}

/// Resolve a `NodePtr` expected to wrap a `WindowClause`.
fn node_as_windowclause<'a, 'mcx>(node: &'a Node<'mcx>) -> Option<&'a WindowClause<'mcx>> {
    node.as_windowclause()
}

/// Unwrap a `Node` to its inner `Expr` (the `Node::Expr` arm); `None` for
/// non-expression nodes.
fn node_as_expr<'a>(node: &'a Node<'_>) -> Option<&'a Expr> {
    node.as_expr()
}

/// Collect the `SortGroupClause`s out of a `PgVec<NodePtr>` (distinctClause /
/// partitionClause / orderClause are all SortGroupClause lists).
fn sortgroupclause_list<'mcx>(
    list: &[types_nodes::nodes::NodePtr<'mcx>],
) -> Vec<SortGroupClause> {
    let mut out = Vec::with_capacity(list.len());
    for n in list.iter() {
        if let Some(s) = node_as_sortgroupclause(n) {
            out.push(s.clone());
        }
    }
    out
}

/* ==========================================================================
 * subquery_is_pushdown_safe  (allpaths.c:3628)
 * ======================================================================== */

/// `subquery_is_pushdown_safe(subquery, topquery, safetyInfo)` (allpaths.c:3628).
/// `top` is the top-level subquery (used to look up setop component RTEs and
/// the top setop's `colTypes`); at the top level `subquery` and `top` are the
/// same Query.
pub(crate) fn subquery_is_pushdown_safe(
    mcx: Mcx<'_>,
    subquery: &Query<'_>,
    top: &Query<'_>,
    safety_info: &mut PushdownSafetyInfo,
) -> PgResult<bool> {
    // Check point 1: LIMIT/OFFSET.
    if subquery.limitOffset.is_some() || subquery.limitCount.is_some() {
        return Ok(false);
    }

    // Check point 6: GROUP BY + GROUPING SETS together.
    if !subquery.groupClause.is_empty() && !subquery.groupingSets.is_empty() {
        return Ok(false);
    }

    // Check points 3, 4, 5: DISTINCT / window funcs / target SRFs => no volatile
    // quals.
    if !subquery.distinctClause.is_empty() || subquery.hasWindowFuncs || subquery.hasTargetSRFs {
        safety_info.unsafe_volatile = true;
    }

    // Leaf query: check output expressions for unsafe references.
    if subquery.setOperations.is_none() {
        check_output_expressions(mcx, subquery, safety_info)?;
    }

    // Are we at top level, or looking at a setop component?
    if core::ptr::eq(subquery as *const _, top as *const _) {
        // Top level: check any component queries.
        if let Some(setop) = subquery.setOperations.as_ref() {
            if !recurse_pushdown_safe(mcx, setop, top, safety_info)? {
                return Ok(false);
            }
        }
    } else {
        // Setop component must not have more components (too weird).
        if subquery.setOperations.is_some() {
            return Ok(false);
        }
        // Check whether setop component output types match top level.
        let topop = top
            .setOperations
            .as_ref()
            .and_then(|n| n.as_ref().as_setoperationstmt())
            .ok_or_else(|| PgError::error("subquery_is_pushdown_safe: top has no SetOperationStmt"))?;
        compare_tlist_datatypes(&subquery.targetList, &topop.colTypes, safety_info)?;
    }
    Ok(true)
}

/// `recurse_pushdown_safe(setOp, topquery, safetyInfo)` (allpaths.c:3684).
fn recurse_pushdown_safe(
    mcx: Mcx<'_>,
    set_op: &Node<'_>,
    top: &Query<'_>,
    safety_info: &mut PushdownSafetyInfo,
) -> PgResult<bool> {
    match set_op.node_tag() {
        ntag::T_RangeTblRef => {
            let rtr = set_op.expect_rangetblref();
            // rte = rt_fetch(rtr->rtindex, topquery->rtable); subquery = rte->subquery.
            let idx = (rtr.rtindex as usize)
                .checked_sub(1)
                .ok_or_else(|| PgError::error("recurse_pushdown_safe: bad rtindex"))?;
            let rte = top
                .rtable
                .get(idx)
                .ok_or_else(|| PgError::error("recurse_pushdown_safe: rtindex out of range"))?;
            let subquery = rte
                .subquery
                .as_deref()
                .ok_or_else(|| PgError::error("recurse_pushdown_safe: setop component has no subquery"))?;
            subquery_is_pushdown_safe(mcx, subquery, top, safety_info)
        }
        ntag::T_SetOperationStmt => {
            let op = set_op.expect_setoperationstmt();
            // EXCEPT is no good (point 2 for subquery_is_pushdown_safe).
            if op.op == SETOP_EXCEPT {
                return Ok(false);
            }
            if !recurse_pushdown_safe(mcx, op.larg.as_ref().expect("setop larg"), top, safety_info)? {
                return Ok(false);
            }
            if !recurse_pushdown_safe(mcx, op.rarg.as_ref().expect("setop rarg"), top, safety_info)? {
                return Ok(false);
            }
            Ok(true)
        }
        other => Err(PgError::error(alloc::format!(
            "unrecognized node type in recurse_pushdown_safe: {:?}",
            other
        ))),
    }
}

/// `check_output_expressions(subquery, safetyInfo)` (allpaths.c:3753).
fn check_output_expressions(
    mcx: Mcx<'_>,
    subquery: &Query<'_>,
    safety_info: &mut PushdownSafetyInfo,
) -> PgResult<()> {
    // Expand grouping Vars to underlying expressions when the subquery has a
    // group RTE; otherwise use the tlist as-is.
    //
    // The `hasGroupRTE` leg (`flatten_group_exprs(NULL, subquery, targetList)`)
    // requires the var.c flattener over a `List*`-shaped targetlist; the owned
    // `PgVec<TargetEntry>` carrier does not project to that `List*` node and the
    // flattener owner takes a `&mut PlannerInfo` we do not have here. Mirror the
    // ruleutils seam boundary: a precise panic on the GROUP-RTE leg. It is only
    // reached for a subquery that itself went through grouping-set planning AND
    // is pushdown-eligible — never on the plain-subquery SELECT path.
    if subquery.hasGroupRTE {
        let _ = mcx;
        return Err(PgError::error(
            "check_output_expressions: flatten_group_exprs over a GROUP-RTE subquery \
             targetList is not yet expressible (List*-shaped targetlist + root-less \
             flattener); reached only for a pushdown-eligible grouping-set subquery",
        ));
    }
    let tlist: &[types_nodes::primnodes::TargetEntry<'_>] = &subquery.targetList;

    for tle in tlist.iter() {
        if tle.resjunk {
            continue; // ignore resjunk columns
        }
        let resno = tle.resno as usize;
        let texpr = tle.expr.as_deref();

        // Functions returning sets are unsafe (point 1).
        if subquery.hasTargetSRFs
            && (safety_info.unsafe_flags[resno] & UNSAFE_HAS_SET_FUNC) == 0
            && expression_returns_set(texpr)
        {
            safety_info.unsafe_flags[resno] |= UNSAFE_HAS_SET_FUNC;
            continue;
        }

        // Volatile functions are unsafe (point 2).
        if (safety_info.unsafe_flags[resno] & UNSAFE_HAS_VOLATILE_FUNC) == 0
            && backend_optimizer_util_clauses::grounded::contain_volatile_functions(texpr)?
        {
            safety_info.unsafe_flags[resno] |= UNSAFE_HAS_VOLATILE_FUNC;
            continue;
        }

        // DISTINCT ON: non-DISTINCT columns unsafe (point 3).
        if subquery.hasDistinctOn
            && (safety_info.unsafe_flags[resno] & UNSAFE_NOTIN_DISTINCTON_CLAUSE) == 0
            && !target_is_in_sort_list(tle, &subquery.distinctClause)
        {
            safety_info.unsafe_flags[resno] |= UNSAFE_NOTIN_DISTINCTON_CLAUSE;
            continue;
        }

        // Window functions: columns not in every PARTITION BY unsafe (point 4).
        // (C checks the DISTINCTON bit here — preserved verbatim.)
        if subquery.hasWindowFuncs
            && (safety_info.unsafe_flags[resno] & UNSAFE_NOTIN_DISTINCTON_CLAUSE) == 0
            && !target_is_in_all_partition_lists(tle, subquery)
        {
            safety_info.unsafe_flags[resno] |= UNSAFE_NOTIN_PARTITIONBY_CLAUSE;
            continue;
        }
    }
    Ok(())
}

/// `compare_tlist_datatypes(tlist, colTypes, safetyInfo)` (allpaths.c:3849).
fn compare_tlist_datatypes(
    tlist: &[types_nodes::primnodes::TargetEntry<'_>],
    col_types: &[Oid],
    safety_info: &mut PushdownSafetyInfo,
) -> PgResult<()> {
    let mut col_iter = col_types.iter();
    for tle in tlist.iter() {
        if tle.resjunk {
            continue; // ignore resjunk columns
        }
        let Some(&ct) = col_iter.next() else {
            return Err(PgError::error("wrong number of tlist entries"));
        };
        if expr_type(tle.expr.as_deref())? != ct {
            safety_info.unsafe_flags[tle.resno as usize] |= UNSAFE_TYPE_MISMATCH;
        }
    }
    if col_iter.next().is_some() {
        return Err(PgError::error("wrong number of tlist entries"));
    }
    Ok(())
}

/// `targetIsInAllPartitionLists(tle, query)` (allpaths.c:3882).
fn target_is_in_all_partition_lists(
    tle: &types_nodes::primnodes::TargetEntry<'_>,
    query: &Query<'_>,
) -> bool {
    for wc_node in query.windowClause.iter() {
        let Some(wc) = node_as_windowclause(wc_node) else {
            continue;
        };
        let part = sortgroupclause_list(&wc.partitionClause);
        if !target_is_in_sort_list_clauses(tle, &part) {
            return false;
        }
    }
    true
}

/// `targetIsInSortList(tle, InvalidOid, sortList)` over a `PgVec<NodePtr>`
/// (distinctClause). Extracts the `SortGroupClause`s then defers to the parser
/// helper.
fn target_is_in_sort_list(
    tle: &types_nodes::primnodes::TargetEntry<'_>,
    sort_list: &[types_nodes::nodes::NodePtr<'_>],
) -> bool {
    let scl = sortgroupclause_list(sort_list);
    target_is_in_sort_list_clauses(tle, &scl)
}

fn target_is_in_sort_list_clauses(
    tle: &types_nodes::primnodes::TargetEntry<'_>,
    sort_list: &[SortGroupClause],
) -> bool {
    backend_parser_clause::targetIsInSortList(tle, types_core::primitive::InvalidOid, sort_list)
}

/* ==========================================================================
 * qual_is_pushdown_safe  (allpaths.c:3925)
 * ======================================================================== */

/// `qual_is_pushdown_safe(subquery, rti, rinfo, safetyInfo)` (allpaths.c:3925).
/// `clause` is `rinfo->clause` already resolved out of the optimizer arena;
/// `rinfo_node` is the whole RestrictInfo for the volatile check
/// (`contain_volatile_functions((Node *) rinfo)` walks the rinfo, but the
/// clause is what carries volatile funcs — we pass the clause for that walk too,
/// faithful because a RestrictInfo holds no volatile funcs outside its clause).
pub(crate) fn qual_is_pushdown_safe(
    mcx: Mcx<'_>,
    _subquery: &Query<'_>,
    rti: Index,
    clause: &Expr,
    safety_info: &PushdownSafetyInfo,
) -> PgResult<PushdownSafe> {
    let mut safe = PushdownSafe::Safe;

    // Refuse subselects (point 1).
    if backend_optimizer_util_clauses::grounded::contain_subplans(Some(clause))? {
        return Ok(PushdownSafe::Unsafe);
    }

    // Refuse volatile quals if we found they'd be unsafe (point 2).
    if safety_info.unsafe_volatile
        && backend_optimizer_util_clauses::grounded::contain_volatile_functions(Some(clause))?
    {
        return Ok(PushdownSafe::Unsafe);
    }

    // Refuse leaky quals if told to (point 3).
    if safety_info.unsafe_leaky
        && backend_optimizer_util_clauses::grounded::contain_leaked_vars(Some(clause))?
    {
        return Ok(PushdownSafe::Unsafe);
    }

    // Examine all Vars used in clause. Deep-copy via `clone_in` (the derived
    // `Expr::clone` panics on an owned-subtree child).
    let clause_node = Node::mk_expr(mcx, clause.clone_in(mcx)?);
    let vars = backend_optimizer_util_vars::var::pull_var_clause(
        &clause_node,
        backend_optimizer_util_vars::var::PVC_INCLUDE_PLACEHOLDERS,
    );

    for v in vars.iter() {
        // Punt on PlaceHolderVars (anything that isn't a plain Var) (point, XXX).
        let Expr::Var(var) = v else {
            safe = PushdownSafe::Unsafe;
            break;
        };

        // Punt on lateral references (var->varno != rti).
        if var.varno as Index != rti {
            safe = PushdownSafe::Unsafe;
            break;
        }

        // Subqueries have no system columns; whole-row ref unsafe (point 4).
        debug_assert!(var.varattno >= 0);
        if var.varattno == 0 {
            safe = PushdownSafe::Unsafe;
            break;
        }

        // Check point 5: per-column unsafe flags.
        let flags = safety_info.unsafe_flags[var.varattno as usize];
        if flags != 0 {
            if flags
                & (UNSAFE_HAS_VOLATILE_FUNC
                    | UNSAFE_HAS_SET_FUNC
                    | UNSAFE_NOTIN_DISTINCTON_CLAUSE
                    | UNSAFE_TYPE_MISMATCH)
                != 0
            {
                safe = PushdownSafe::Unsafe;
                break;
            } else {
                // UNSAFE_NOTIN_PARTITIONBY_CLAUSE is ok for run conditions; keep
                // scanning for a Var that's outright unsafe.
                safe = PushdownSafe::WindowclauseRuncond;
            }
        }
    }

    Ok(safe)
}

/* ==========================================================================
 * subquery_push_qual  (allpaths.c:4026)
 * ======================================================================== */

/// `subquery_push_qual(subquery, rte, rti, qual)` (allpaths.c:4026).
/// `qual` is the resolved clause `Expr` from the outer arena.
pub(crate) fn subquery_push_qual<'mcx>(
    mcx: Mcx<'mcx>,
    subquery: &mut Query<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    rti: Index,
    qual: &Expr,
) -> PgResult<()> {
    if subquery.setOperations.is_some() {
        // Recurse to push it separately to each component query.
        let setop = subquery
            .setOperations
            .as_deref()
            .map(|n| n.clone_in(mcx))
            .transpose()?
            .unwrap();
        recurse_push_qual(mcx, &setop, subquery, rte, rti, qual)
    } else {
        // Replace outer Vars in the qual with copies of the subquery's tlist
        // expressions. ReplaceVarsFromTargetList works over a Node in place.
        let mut qual_node = Node::mk_expr(mcx, qual.clone_in(mcx)?);
        let mut has_sublinks = Some(subquery.hasSubLinks);
        backend_rewrite_core::replace::ReplaceVarsFromTargetList(
            &mut qual_node,
            rti as i32,
            0,
            rte,
            &subquery.targetList,
            subquery.resultRelation,
            backend_rewrite_core::replace::ReplaceVarsNoMatchOption::ReportError,
            0,
            &mut has_sublinks,
            mcx,
        )?;
        if let Some(b) = has_sublinks {
            subquery.hasSubLinks = b;
        }

        let new_qual: Option<Expr> = match qual_node.into_expr() {
            Some(e) => Some(e),
            None => return Err(PgError::error("subquery_push_qual: replaced qual is not an Expr")),
        };

        // Attach to HAVING when grouping/aggregation present, else WHERE.
        if subquery.hasAggs
            || !subquery.groupClause.is_empty()
            || !subquery.groupingSets.is_empty()
            || subquery.havingQual.is_some()
        {
            let existing = subquery
                .havingQual
                .take()
                .map(|b| (*b).clone_in(mcx))
                .transpose()?;
            subquery.havingQual = make_and_qual(existing, new_qual)
                .map(|e| mcx::alloc_in(mcx, e))
                .transpose()?;
        } else {
            let jt = subquery
                .jointree
                .as_mut()
                .ok_or_else(|| PgError::error("subquery_push_qual: subquery has no jointree"))?;
            let existing = jt
                .quals
                .as_ref()
                .and_then(|n| node_as_expr(n))
                .map(|e| e.clone_in(mcx))
                .transpose()?;
            jt.quals = make_and_qual(existing, new_qual)
                .map(|e| mcx::alloc_in(mcx, Node::mk_expr(mcx, e)))
                .transpose()?;
        }
        Ok(())
    }
}

/// `recurse_push_qual(setOp, topquery, rte, rti, qual)` (allpaths.c:4074).
fn recurse_push_qual<'mcx>(
    mcx: Mcx<'mcx>,
    set_op: &Node<'mcx>,
    top: &mut Query<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    rti: Index,
    qual: &Expr,
) -> PgResult<()> {
    match set_op.node_tag() {
        ntag::T_RangeTblRef => {
            let rtr = set_op.expect_rangetblref();
            let idx = (rtr.rtindex as usize)
                .checked_sub(1)
                .ok_or_else(|| PgError::error("recurse_push_qual: bad rtindex"))?;
            // Take the component subquery out of the parent rtable, push into it,
            // then store it back (the owned model has no shared pointer).
            let mut subrte = top
                .rtable
                .get(idx)
                .ok_or_else(|| PgError::error("recurse_push_qual: rtindex out of range"))?
                .clone_in(mcx)?;
            let mut subquery = subrte
                .subquery
                .take()
                .map(|b| b.clone_in(mcx))
                .transpose()?
                .ok_or_else(|| PgError::error("recurse_push_qual: component has no subquery"))?;
            subquery_push_qual(mcx, &mut subquery, rte, rti, qual)?;
            subrte.subquery = Some(mcx::alloc_in(mcx, subquery)?);
            top.rtable[idx] = subrte;
            Ok(())
        }
        ntag::T_SetOperationStmt => {
            let op = set_op.expect_setoperationstmt();
            recurse_push_qual(mcx, op.larg.as_ref().expect("setop larg"), top, rte, rti, qual)?;
            recurse_push_qual(mcx, op.rarg.as_ref().expect("setop rarg"), top, rte, rti, qual)
        }
        other => Err(PgError::error(alloc::format!(
            "unrecognized node type in recurse_push_qual: {:?}",
            other
        ))),
    }
}

/* ==========================================================================
 * Window run conditions  (allpaths.c:2263 / 2454)
 * ======================================================================== */

/// `check_and_push_window_quals(subquery, rte, rti, clause, run_cond_attrs)`
/// (allpaths.c:2454). Returns whether the caller must keep the original qual.
pub(crate) fn check_and_push_window_quals(
    mcx: Mcx<'_>,
    subquery: &mut Query<'_>,
    _rti: Index,
    clause: &Expr,
    run_cond_attrs: &mut Relids,
) -> PgResult<bool> {
    // We're only able to use OpExprs with 2 operands.
    let Expr::OpExpr(_) = clause else {
        return Ok(true);
    };
    // Deep-copy the OpExpr via `clone_in` (the derived `Expr::clone` panics on
    // an owned-subtree operand).
    let mut opexpr = match clause.clone_in(mcx)? {
        Expr::OpExpr(o) => o,
        _ => unreachable!(),
    };
    if opexpr.args.len() != 2 {
        return Ok(true);
    }

    // Restrict to strict OpExprs (so NULL'd window results filter out).
    set_opexpr_funcid(&mut opexpr)?;
    if !backend_utils_cache_lsyscache_seams::func_strict::call(opexpr.opfuncid)? {
        return Ok(true);
    }

    // Check the left side of the OpExpr.
    if let Expr::Var(var1) = &opexpr.args[0] {
        if var1.varattno > 0 {
            let idx = (var1.varattno as usize)
                .checked_sub(1)
                .ok_or_else(|| PgError::error("check_and_push_window_quals: bad varattno"))?;
            if let Some(tle) = subquery.targetList.get(idx) {
                let resno = tle.resno;
                if matches!(tle.expr.as_deref(), Some(Expr::WindowFunc(_))) {
                    // Deep-copy via `clone_in` — the node must outlive the
                    // `&mut subquery` call below and the derived `Expr::clone`
                    // panics on an owned-subtree child.
                    let wfunc_node = tle.expr.as_deref().unwrap().clone_in(mcx)?;
                    let mut keep_original = true;
                    if find_window_run_conditions(
                        mcx,
                        subquery,
                        resno,
                        &wfunc_node,
                        &opexpr,
                        true,
                        &mut keep_original,
                        run_cond_attrs,
                    )? {
                        return Ok(keep_original);
                    }
                }
            }
        }
    }

    // And check the right side.
    if let Expr::Var(var2) = &opexpr.args[1] {
        if var2.varattno > 0 {
            let idx = (var2.varattno as usize)
                .checked_sub(1)
                .ok_or_else(|| PgError::error("check_and_push_window_quals: bad varattno"))?;
            if let Some(tle) = subquery.targetList.get(idx) {
                let resno = tle.resno;
                if matches!(tle.expr.as_deref(), Some(Expr::WindowFunc(_))) {
                    // Deep-copy via `clone_in` (see the var1 branch above).
                    let wfunc_node = tle.expr.as_deref().unwrap().clone_in(mcx)?;
                    let mut keep_original = true;
                    if find_window_run_conditions(
                        mcx,
                        subquery,
                        resno,
                        &wfunc_node,
                        &opexpr,
                        false,
                        &mut keep_original,
                        run_cond_attrs,
                    )? {
                        return Ok(keep_original);
                    }
                }
            }
        }
    }

    Ok(true)
}

/// `find_window_run_conditions(subquery, rte, rti, attno, wfunc, opexpr,
/// wfunc_left, keep_original, run_cond_attrs)` (allpaths.c:2263).
///
/// Dispatches to the window function's `SupportRequestWFuncMonotonic` kernel.
/// A window function with no registered monotonic-support kernel declines
/// (mirrors C's `res == NULL` when `prosupport` doesn't serve this request),
/// in which case this returns `false` and leaves `*keep_original = true`.
#[allow(clippy::too_many_arguments)]
fn find_window_run_conditions(
    mcx: Mcx<'_>,
    subquery: &mut Query<'_>,
    attno: AttrNumber,
    wfunc_node: &Expr,
    opexpr: &OpExpr,
    wfunc_left: bool,
    keep_original: &mut bool,
    run_cond_attrs: &mut Relids,
) -> PgResult<bool> {
    *keep_original = true;

    // (C strips leading RelabelTypes; in the owned model the tlist entry is
    // already the WindowFunc — a RelabelType wrapper would have been a different
    // Expr variant and check_and_push_window_quals would not have matched it as
    // a WindowFunc tlist entry. Faithful: only WindowFuncs reach here.)
    let wfunc = match wfunc_node {
        Expr::WindowFunc(w) => w,
        _ => return Ok(false),
    };

    // Can't use it if there are subplans in the WindowFunc.
    if backend_optimizer_util_clauses::grounded::contain_subplans(Some(wfunc_node))? {
        return Ok(false);
    }

    // Check if there's a support function for 'wfunc'.
    let prosupport = backend_utils_cache_lsyscache_seams::get_func_support::call(wfunc.winfnoid)?;
    if prosupport == types_core::primitive::InvalidOid {
        return Ok(false);
    }

    // The value on the other side of the OpExpr must be a pseudo-constant.
    let otherexpr = if wfunc_left { &opexpr.args[1] } else { &opexpr.args[0] };
    if !backend_optimizer_util_clauses::grounded::is_pseudo_constant_clause(Some(otherexpr))? {
        return Ok(false);
    }

    // Find the window clause belonging to the window function.
    let widx = (wfunc.winref as usize)
        .checked_sub(1)
        .ok_or_else(|| PgError::error("find_window_run_conditions: bad winref"))?;
    let wclause = subquery
        .windowClause
        .get(widx)
        .and_then(|n| node_as_windowclause(n))
        .ok_or_else(|| PgError::error("find_window_run_conditions: winref out of range"))?;

    // Call the support function (SupportRequestWFuncMonotonic).
    let monotonic = match call_support_wfunc_monotonic(prosupport, wfunc.winfnoid, wclause) {
        Some(m) => m,
        None => return Ok(false), // res == NULL: support fn doesn't serve this request
    };
    if monotonic == MONOTONICFUNC_NONE {
        return Ok(false);
    }

    // Examine the operator's btree interpretations to pick the run operator.
    let opinfos =
        backend_utils_cache_lsyscache_seams::get_op_index_interpretation::call(mcx, opexpr.opno)?;

    // Deep-copy the OpExpr (the derived `Expr::clone` panics on an owned-subtree
    // operand; `clone_exprs_in` routes each arg through `Expr::clone_in`).
    let clone_runopexpr = |o: &OpExpr| -> PgResult<OpExpr> {
        Ok(OpExpr {
            opno: o.opno,
            opfuncid: o.opfuncid,
            opresulttype: o.opresulttype,
            opretset: o.opretset,
            opcollid: o.opcollid,
            inputcollid: o.inputcollid,
            args: clone_exprs_in(&o.args, mcx)?,
            location: o.location,
        })
    };
    let mut runopexpr: Option<OpExpr> = None;
    let mut runoperator = types_core::primitive::InvalidOid;

    for opinfo in opinfos.iter() {
        let cmptype = opinfo.cmptype;
        if cmptype == COMPARE_LT || cmptype == COMPARE_LE {
            if (wfunc_left && (monotonic & MONOTONICFUNC_INCREASING) != 0)
                || (!wfunc_left && (monotonic & MONOTONICFUNC_DECREASING) != 0)
            {
                *keep_original = false;
                runopexpr = Some(clone_runopexpr(opexpr)?);
                runoperator = opexpr.opno;
            }
            break;
        } else if cmptype == COMPARE_GT || cmptype == COMPARE_GE {
            if (wfunc_left && (monotonic & MONOTONICFUNC_DECREASING) != 0)
                || (!wfunc_left && (monotonic & MONOTONICFUNC_INCREASING) != 0)
            {
                *keep_original = false;
                runopexpr = Some(clone_runopexpr(opexpr)?);
                runoperator = opexpr.opno;
            }
            break;
        } else if cmptype == COMPARE_EQ {
            if (monotonic & MONOTONICFUNC_BOTH) == MONOTONICFUNC_BOTH {
                *keep_original = false;
                runopexpr = Some(clone_runopexpr(opexpr)?);
                runoperator = opexpr.opno;
                break;
            }
            let newcmptype = if monotonic & MONOTONICFUNC_INCREASING != 0 {
                if wfunc_left { COMPARE_LE } else { COMPARE_GE }
            } else if wfunc_left {
                COMPARE_GE
            } else {
                COMPARE_LE
            };
            *keep_original = true;
            runopexpr = Some(clone_runopexpr(opexpr)?);
            runoperator = backend_utils_cache_lsyscache_seams::get_opfamily_member_for_cmptype::call(
                opinfo.opfamily_id,
                opinfo.oplefttype,
                opinfo.oprighttype,
                newcmptype,
            )?;
            break;
        }
    }

    if runopexpr.is_some() {
        // C builds a `WindowFuncRunCondition` node here, appends it to
        // `wfunc->runCondition`, and records `attno` in `run_cond_attrs`. The
        // owned node model does not yet carry a `WindowFuncRunCondition` node
        // (no struct / Node tag / Expr variant), so the run-condition cannot be
        // constructed faithfully. This branch is *unreachable on every current
        // path*: it is gated behind a non-NULL `SupportRequestWFuncMonotonic`
        // result, and the monotonic-support dispatch table
        // (`register_support_wfunc_monotonic`) has no registered kernels, so
        // `call_support_wfunc_monotonic` always declines above. The precise
        // panic marks the genuine boundary (model `WindowFuncRunCondition`,
        // register a window-function monotonic-support kernel) rather than
        // silently dropping the optimization.
        let _ = (runoperator, attno, run_cond_attrs);
        return Err(PgError::error(
            "find_window_run_conditions: WindowFuncRunCondition node not yet modeled \
             (window run-condition pushdown requires a SupportRequestWFuncMonotonic \
             kernel + the WindowFuncRunCondition node, neither present)",
        ));
    }

    Ok(false)
}

/// `set_opfuncid(opexpr)` (nodeFuncs.c) over the owned OpExpr.
fn set_opexpr_funcid(opexpr: &mut OpExpr) -> PgResult<()> {
    set_opfuncid(opexpr)
}

/* ==========================================================================
 * remove_unused_subquery_outputs  (allpaths.c:4126)
 * ======================================================================== */

/// `remove_unused_subquery_outputs(subquery, rel, extra_used_attrs)`
/// (allpaths.c:4126). Reads the outer rel's reltarget exprs + baserestrictinfo
/// (resolved out of `root`'s arena) to compute the used-attrs set, then NULLs
/// unused subquery tlist entries.
pub(crate) fn remove_unused_subquery_outputs<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    rel: RelId,
    subquery: &mut Query<'mcx>,
    extra_used_attrs: Relids,
) -> PgResult<()> {
    let mut attrs_used: Relids = extra_used_attrs;

    // Do nothing for UNION/INTERSECT/EXCEPT.
    if subquery.setOperations.is_some() {
        return Ok(());
    }

    // Regular DISTINCT (not DISTINCT ON): all outputs must be used.
    if !subquery.distinctClause.is_empty() && !subquery.hasDistinctOn {
        return Ok(());
    }

    // Collect output column numbers used by the upper query.
    let relid = root.rel(rel).relid as i32;

    // pull_varattnos over rel->reltarget->exprs. Deep-copy each expr via
    // `clone_in` (the derived `Expr::clone` panics on an owned-subtree child).
    let reltarget_expr_ids: Vec<NodeId> = {
        let rt = root
            .rel(rel)
            .reltarget
            .as_ref()
            .ok_or_else(|| PgError::error("remove_unused_subquery_outputs: rel has no reltarget"))?;
        rt.exprs.clone()
    };
    for nid in reltarget_expr_ids.iter() {
        let e = root.node(*nid).clone_in(mcx)?;
        let n = Node::mk_expr(mcx, e);
        attrs_used = pull_varattnos_relids(mcx, &n, relid, attrs_used)?;
    }

    // pull_varattnos over each un-pushed-down baserestrictinfo clause.
    let base_rinfo_ids: Vec<RinfoId> = root.rel(rel).baserestrictinfo.clone();
    for rid in base_rinfo_ids.iter() {
        let clause_id = root.rinfo(*rid).clause;
        let c = root.node(clause_id).clone_in(mcx)?;
        let n = Node::mk_expr(mcx, c);
        attrs_used = pull_varattnos_relids(mcx, &n, relid, attrs_used)?;
    }

    // Whole-row reference => can't remove anything.
    if relids_is_member(0 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER, &attrs_used) {
        return Ok(());
    }

    // Zap entries we don't need (replace expr with a NULL Const).
    for tle in subquery.targetList.iter_mut() {
        // Keep entries with sortgroupref or resjunk.
        if tle.ressortgroupref != 0 || tle.resjunk {
            continue;
        }
        // Keep if used by the upper query.
        if relids_is_member(
            tle.resno as i32 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER,
            &attrs_used,
        ) {
            continue;
        }

        let texpr = tle.expr.as_deref();
        // Keep set-returning expressions.
        if subquery.hasTargetSRFs && expression_returns_set(texpr) {
            continue;
        }
        // Keep volatile expressions.
        if backend_optimizer_util_clauses::grounded::contain_volatile_functions(texpr)? {
            continue;
        }

        // Replace with a NULL constant of the same exposed type/typmod/collation.
        let ctype = expr_type(texpr)?;
        let ctypmod = expr_typmod(texpr)?;
        let ccoll = expr_collation(texpr)?;
        let null_const = backend_nodes_core::makefuncs::make_null_const(mcx, ctype, ctypmod, ccoll)?;
        tle.expr = Some(mcx::alloc_in(mcx, Expr::Const(null_const))?);
    }

    Ok(())
}

/// `pull_varattnos((Node*) node, relid, &attrs)` returning the updated `Relids`.
fn pull_varattnos_relids<'mcx>(
    _mcx: Mcx<'mcx>,
    node: &Node<'mcx>,
    relid: i32,
    attrs: Relids,
) -> PgResult<Relids> {
    Ok(backend_optimizer_util_vars::var::pull_varattnos(node, relid, attrs))
}

/// `bms_is_member(x, a)` over the planner `Relids`
/// (`Option<Box<Bitmapset{words: Vec<u64>}>>`): is bit `x` set?
fn relids_is_member(x: i32, a: &Relids) -> bool {
    if x < 0 {
        return false;
    }
    let Some(bms) = a.as_ref() else { return false };
    const BITS_PER_BITMAPWORD: i32 = 64;
    let wnum = (x / BITS_PER_BITMAPWORD) as usize;
    if wnum >= bms.words.len() {
        return false;
    }
    (bms.words[wnum] >> (x % BITS_PER_BITMAPWORD)) & 1 != 0
}

/* ==========================================================================
 * CompareType discriminants (cmptype.h)
 * ======================================================================== */

/// `COMPARE_LT` (cmptype.h).
const COMPARE_LT: i32 = 1;
/// `COMPARE_LE`.
const COMPARE_LE: i32 = 2;
/// `COMPARE_EQ`.
const COMPARE_EQ: i32 = 3;
/// `COMPARE_GE`.
const COMPARE_GE: i32 = 4;
/// `COMPARE_GT`.
const COMPARE_GT: i32 = 5;

