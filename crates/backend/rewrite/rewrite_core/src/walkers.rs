//! The read-only `Node`-walker predicates of rewriteManip.c:
//! `contain_aggs_of_level`, `locate_agg_of_level`, `contain_windowfuncs`,
//! `locate_windowfunc`, `checkExprHasSubLink`, `contains_multiexpr_param`,
//! `rangeTableEntry_used`.
//!
//! Each is a 1:1 transcription of the C `bool (*)(Node *, void *)` walker over
//! the central [`nodes_core::node_walker`] engine, whose walker is a Rust
//! `&mut dyn FnMut(&Node) -> bool` closure. C's `IsA(node, X)` dispatch is a
//! match over the [`Node`]/[`Expr`] enum arms (every `Var`-family node is an
//! `Expr` arm carried as `Node::Expr`; `Query` is its own `Node` arm).

#![allow(non_snake_case)]

use nodes_core::node_walker::{
    expression_tree_walker, query_or_expression_tree_walker, query_tree_walker,
    QTW_IGNORE_CTE_SUBQUERIES, QTW_IGNORE_RT_SUBQUERIES,
};
use ::nodes::nodes::{ntag, Node};
use ::nodes::primnodes::ParamKind;

use crate::relids;

// ===========================================================================
// contain_aggs_of_level (rewriteManip.c:84)
// ===========================================================================

/// `contain_aggs_of_level(node, levelsup)` — does `node` contain an aggregate of
/// exactly the given query level? Recurses into subqueries so outer-reference
/// aggregates logically belonging to the level are detected.
pub fn contain_aggs_of_level(node: &Node, levelsup: i32) -> bool {
    let mut sublevels_up = levelsup;
    query_or_expression_tree_walker(
        node,
        &mut |n| contain_aggs_of_level_walker(n, &mut sublevels_up),
        0,
    )
}

fn contain_aggs_of_level_walker(node: &Node, sublevels_up: &mut i32) -> bool {
    match node.node_tag() {
        ntag::T_Aggref => {
            let agg = node.expect_aggref();
            if agg.agglevelsup as i32 == *sublevels_up {
                return true;
            }
            // else fall through to examine argument
            expression_tree_walker(node, &mut |n| contain_aggs_of_level_walker(n, sublevels_up))
        }
        ntag::T_GroupingFunc => {
            let grp = node.expect_groupingfunc();
            if grp.agglevelsup as i32 == *sublevels_up {
                return true;
            }
            expression_tree_walker(node, &mut |n| contain_aggs_of_level_walker(n, sublevels_up))
        }
        ntag::T_Query => {
            let q = node.expect_query();
            *sublevels_up += 1;
            let result =
                query_tree_walker(q, &mut |n| contain_aggs_of_level_walker(n, sublevels_up), 0);
            *sublevels_up -= 1;
            result
        }
        _ => expression_tree_walker(node, &mut |n| contain_aggs_of_level_walker(n, sublevels_up)),
    }
}

// ===========================================================================
// locate_agg_of_level (rewriteManip.c:148)
// ===========================================================================

struct LocateAggCtx {
    agg_location: i32,
    sublevels_up: i32,
}

/// `locate_agg_of_level(node, levelsup)` — parse location of any aggregate of the
/// given level, or -1.
pub fn locate_agg_of_level(node: &Node, levelsup: i32) -> i32 {
    let mut ctx = LocateAggCtx {
        agg_location: -1,
        sublevels_up: levelsup,
    };
    let _ = query_or_expression_tree_walker(
        node,
        &mut |n| locate_agg_of_level_walker(n, &mut ctx),
        0,
    );
    ctx.agg_location
}

fn locate_agg_of_level_walker(node: &Node, ctx: &mut LocateAggCtx) -> bool {
    match node.node_tag() {
        ntag::T_Aggref => {
            let agg = node.expect_aggref();
            if agg.agglevelsup as i32 == ctx.sublevels_up && agg.location >= 0 {
                ctx.agg_location = agg.location;
                return true;
            }
            expression_tree_walker(node, &mut |n| locate_agg_of_level_walker(n, ctx))
        }
        ntag::T_GroupingFunc => {
            let grp = node.expect_groupingfunc();
            if grp.agglevelsup as i32 == ctx.sublevels_up && grp.location >= 0 {
                ctx.agg_location = grp.location;
                return true;
            }
            expression_tree_walker(node, &mut |n| locate_agg_of_level_walker(n, ctx))
        }
        ntag::T_Query => {
            let q = node.expect_query();
            ctx.sublevels_up += 1;
            let result = query_tree_walker(q, &mut |n| locate_agg_of_level_walker(n, ctx), 0);
            ctx.sublevels_up -= 1;
            result
        }
        _ => expression_tree_walker(node, &mut |n| locate_agg_of_level_walker(n, ctx)),
    }
}

// ===========================================================================
// contain_windowfuncs (rewriteManip.c:213)
// ===========================================================================

/// `contain_windowfuncs(node)` — does `node` contain a window function of the
/// current query level? Must not recurse into subselects.
pub fn contain_windowfuncs(node: &Node) -> bool {
    query_or_expression_tree_walker(node, &mut contain_windowfuncs_walker, 0)
}

fn contain_windowfuncs_walker(node: &Node) -> bool {
    if node.is_windowfunc() {
        return true;
    }
    // Mustn't recurse into subselects (no Query arm here).
    expression_tree_walker(node, &mut contain_windowfuncs_walker)
}

// ===========================================================================
// locate_windowfunc (rewriteManip.c:250)
// ===========================================================================

/// `locate_windowfunc(node)` — parse location of any windowfunc of the current
/// query level, or -1.
pub fn locate_windowfunc(node: &Node) -> i32 {
    let mut win_location = -1i32;
    let _ = query_or_expression_tree_walker(
        node,
        &mut |n| locate_windowfunc_walker(n, &mut win_location),
        0,
    );
    win_location
}

fn locate_windowfunc_walker(node: &Node, win_location: &mut i32) -> bool {
    if let Some(wfunc) = node.as_windowfunc() {
        if wfunc.location >= 0 {
            *win_location = wfunc.location;
            return true;
        }
        // else fall through to examine argument
    }
    // Mustn't recurse into subselects.
    expression_tree_walker(node, &mut |n| locate_windowfunc_walker(n, win_location))
}

// ===========================================================================
// checkExprHasSubLink (rewriteManip.c:291)
// ===========================================================================

/// `checkExprHasSubLink(node)` — does `node` contain a SubLink? Examines a Query
/// but does not recurse into its rangetable or CTE-list sub-Queries.
pub fn checkExprHasSubLink(node: &Node) -> bool {
    // C: QTW_IGNORE_RC_SUBQUERIES = ignore both rtable and CTE subqueries.
    query_or_expression_tree_walker(
        node,
        &mut checkExprHasSubLink_walker,
        QTW_IGNORE_RT_SUBQUERIES | QTW_IGNORE_CTE_SUBQUERIES,
    )
}

fn checkExprHasSubLink_walker(node: &Node) -> bool {
    if node.is_sublink() {
        return true;
    }
    expression_tree_walker(node, &mut checkExprHasSubLink_walker)
}

// ===========================================================================
// contains_multiexpr_param (rewriteManip.c:320)
// ===========================================================================

/// `contains_multiexpr_param(node)` — does the expression tree contain a
/// `PARAM_MULTIEXPR` Param? Intentionally does NOT descend into SubLinks: only
/// Params at the current query level are of interest.
pub fn contains_multiexpr_param(node: &Node) -> bool {
    if let Some(p) = node.as_param() {
        if p.paramkind == ParamKind::PARAM_MULTIEXPR {
            return true;
        }
        return false;
    }
    expression_tree_walker(node, &mut contains_multiexpr_param)
}

// ===========================================================================
// rangeTableEntry_used (rewriteManip.c:1057)
// ===========================================================================

struct RteUsedCtx {
    rt_index: i32,
    sublevels_up: i32,
}

/// `rangeTableEntry_used(node, rt_index, sublevels_up)` — is the RTE referenced
/// somewhere in Var nodes or join/setOp trees of a query or expression?
pub fn rangeTableEntry_used(node: &Node, rt_index: i32, sublevels_up: i32) -> bool {
    let mut ctx = RteUsedCtx {
        rt_index,
        sublevels_up,
    };
    query_or_expression_tree_walker(
        node,
        &mut |n| rangeTableEntry_used_walker(n, &mut ctx),
        0,
    )
}

fn rangeTableEntry_used_walker(node: &Node, ctx: &mut RteUsedCtx) -> bool {
    match node.node_tag() {
        ntag::T_Var => {
            let var = node.expect_var();
            if var.varlevelsup as i32 == ctx.sublevels_up
                && (var.varno == ctx.rt_index
                    || relids::is_member(ctx.rt_index, &var.varnullingrels))
            {
                return true;
            }
            false
        }
        ntag::T_CurrentOfExpr => {
            let cexpr = node.expect_currentofexpr();
            if ctx.sublevels_up == 0 && cexpr.cvarno as i32 == ctx.rt_index {
                return true;
            }
            false
        }
        ntag::T_RangeTblRef => {
            let rtr = node.expect_rangetblref();
            if rtr.rtindex == ctx.rt_index && ctx.sublevels_up == 0 {
                return true;
            }
            // the subquery itself is visited separately
            false
        }
        ntag::T_JoinExpr => {
            let j = node.expect_joinexpr();
            if j.rtindex == ctx.rt_index && ctx.sublevels_up == 0 {
                return true;
            }
            // fall through to examine children
            expression_tree_walker(node, &mut |n| rangeTableEntry_used_walker(n, ctx))
        }
        ntag::T_Query => {
            let q = node.expect_query();
            ctx.sublevels_up += 1;
            let result =
                query_tree_walker(q, &mut |n| rangeTableEntry_used_walker(n, ctx), 0);
            ctx.sublevels_up -= 1;
            result
        }
        _ => expression_tree_walker(node, &mut |n| rangeTableEntry_used_walker(n, ctx)),
    }
}
