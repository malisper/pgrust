//! `IncrementVarSublevelsUp` (rewriteManip.c:776) + `_rtable` variant, and
//! `SetVarReturningType` (rewriteManip.c:919). In-place mutation.
//!
//! # RTE `ctelevelsup` and `QTW_EXAMINE_RTES_BEFORE`
//!
//! The C walker passes `QTW_EXAMINE_RTES_BEFORE` so that `query_tree_walker`
//! invokes the walker on each `RangeTblEntry` node itself, letting the
//! `IsA(node, RangeTblEntry)` arm bump `ctelevelsup` for `RTE_CTE` entries. The
//! repo's `Node`-level walker engine does not surface bare RTE nodes (RTEs are
//! not a walked `Node` arm), so the `RTE_CTE` `ctelevelsup` bump is applied
//! directly by iterating the range table when recursing into a `Query` — an
//! observationally identical adaptation to the repo's walker model.

#![allow(non_snake_case)]

use backend_nodes_core::node_walker::{
    expression_tree_walker_mut, query_or_expression_tree_mutator, query_tree_mutator,
    range_table_mutator,
};
use types_nodes::copy_query::Query;
use types_nodes::nodes::Node;
use types_nodes::parsenodes::{RangeTblEntry, RTEKind};
use types_nodes::primnodes::{Expr, VarReturningType};

// ===========================================================================
// IncrementVarSublevelsUp (rewriteManip.c:776)
// ===========================================================================

struct IncrCtx {
    delta_sublevels_up: i32,
    min_sublevels_up: i32,
}

fn IncrementVarSublevelsUp_walker(node: &mut Node, ctx: &mut IncrCtx) -> bool {
    match node {
        Node::Expr(Expr::Var(var)) => {
            if var.varlevelsup as i32 >= ctx.min_sublevels_up {
                var.varlevelsup =
                    (var.varlevelsup as i32 + ctx.delta_sublevels_up) as u32;
            }
            false // done here
        }
        Node::CurrentOfExpr(_) => {
            // this should not happen
            if ctx.min_sublevels_up == 0 {
                panic!("cannot push down CurrentOfExpr");
            }
            false
        }
        Node::Expr(Expr::Aggref(_)) => {
            if let Node::Expr(Expr::Aggref(agg)) = node {
                if agg.agglevelsup as i32 >= ctx.min_sublevels_up {
                    agg.agglevelsup =
                        (agg.agglevelsup as i32 + ctx.delta_sublevels_up) as u32;
                }
            }
            // fall through to recurse into argument
            expression_tree_walker_mut(node, &mut |n| IncrementVarSublevelsUp_walker(n, ctx))
        }
        Node::Expr(Expr::GroupingFunc(_)) => {
            if let Node::Expr(Expr::GroupingFunc(grp)) = node {
                if grp.agglevelsup as i32 >= ctx.min_sublevels_up {
                    grp.agglevelsup =
                        (grp.agglevelsup as i32 + ctx.delta_sublevels_up) as u32;
                }
            }
            expression_tree_walker_mut(node, &mut |n| IncrementVarSublevelsUp_walker(n, ctx))
        }
        Node::Expr(Expr::PlaceHolderVar(_)) => {
            if let Node::Expr(Expr::PlaceHolderVar(phv)) = node {
                if phv.phlevelsup as i32 >= ctx.min_sublevels_up {
                    phv.phlevelsup =
                        (phv.phlevelsup as i32 + ctx.delta_sublevels_up) as u32;
                }
            }
            expression_tree_walker_mut(node, &mut |n| IncrementVarSublevelsUp_walker(n, ctx))
        }
        Node::Expr(Expr::ReturningExpr(_)) => {
            if let Node::Expr(Expr::ReturningExpr(rexpr)) = node {
                if rexpr.retlevelsup >= ctx.min_sublevels_up {
                    rexpr.retlevelsup += ctx.delta_sublevels_up;
                }
            }
            expression_tree_walker_mut(node, &mut |n| IncrementVarSublevelsUp_walker(n, ctx))
        }
        Node::Query(q) => {
            ctx.min_sublevels_up += 1;
            increment_query_ctes(q, ctx);
            let result =
                query_tree_mutator(q, &mut |n| IncrementVarSublevelsUp_walker(n, ctx), 0);
            ctx.min_sublevels_up -= 1;
            result
        }
        _ => expression_tree_walker_mut(node, &mut |n| IncrementVarSublevelsUp_walker(n, ctx)),
    }
}

/// Apply the `RTE_CTE` `ctelevelsup` bump to a Query's range table (the
/// `IsA(node, RangeTblEntry)` arm under `QTW_EXAMINE_RTES_BEFORE`).
fn increment_query_ctes(q: &mut Query, ctx: &IncrCtx) {
    for rte in q.rtable.iter_mut() {
        increment_rte_cte(rte, ctx);
    }
}

#[inline]
fn increment_rte_cte(rte: &mut RangeTblEntry, ctx: &IncrCtx) {
    if rte.rtekind == RTEKind::RTE_CTE && rte.ctelevelsup as i32 >= ctx.min_sublevels_up {
        rte.ctelevelsup = (rte.ctelevelsup as i32 + ctx.delta_sublevels_up) as u32;
    }
}

/// `IncrementVarSublevelsUp(node, delta_sublevels_up, min_sublevels_up)`
/// (rewriteManip.c:880).
pub fn IncrementVarSublevelsUp(node: &mut Node, delta_sublevels_up: i32, min_sublevels_up: i32) {
    let mut ctx = IncrCtx {
        delta_sublevels_up,
        min_sublevels_up,
    };
    // C uses query_or_expression_tree_walker(..., QTW_EXAMINE_RTES_BEFORE).
    // Starting at a Query does NOT increment min_sublevels_up, so we bump the
    // top Query's own RTE_CTE entries at the current level before descending.
    if let Node::Query(q) = node {
        increment_query_ctes(q, &ctx);
    }
    query_or_expression_tree_mutator(
        node,
        &mut |n| IncrementVarSublevelsUp_walker(n, &mut ctx),
        0,
    );
}

/// `IncrementVarSublevelsUp_rtable(rtable, delta_sublevels_up, min_sublevels_up)`
/// (rewriteManip.c:903).
pub fn IncrementVarSublevelsUp_rtable(
    rtable: &mut [RangeTblEntry],
    delta_sublevels_up: i32,
    min_sublevels_up: i32,
) {
    let mut ctx = IncrCtx {
        delta_sublevels_up,
        min_sublevels_up,
    };
    // Examine each RTE node before its contents (QTW_EXAMINE_RTES_BEFORE): bump
    // RTE_CTE ctelevelsup, then walk the RTE's expression trees.
    for rte in rtable.iter_mut() {
        increment_rte_cte(rte, &ctx);
    }
    range_table_mutator(
        rtable,
        &mut |n| IncrementVarSublevelsUp_walker(n, &mut ctx),
        0,
    );
}

// ===========================================================================
// SetVarReturningType (rewriteManip.c:919)
// ===========================================================================

struct SetReturnCtx {
    result_relation: i32,
    sublevels_up: i32,
    returning_type: VarReturningType,
}

fn SetVarReturningType_walker(node: &mut Node, ctx: &mut SetReturnCtx) -> bool {
    match node {
        Node::Expr(Expr::Var(var)) => {
            if var.varno == ctx.result_relation && var.varlevelsup as i32 == ctx.sublevels_up {
                var.varreturningtype = ctx.returning_type;
            }
            false
        }
        Node::Query(q) => {
            ctx.sublevels_up += 1;
            let result =
                query_tree_mutator(q, &mut |n| SetVarReturningType_walker(n, ctx), 0);
            ctx.sublevels_up -= 1;
            result
        }
        _ => expression_tree_walker_mut(node, &mut |n| SetVarReturningType_walker(n, ctx)),
    }
}

/// `SetVarReturningType(node, result_relation, sublevels_up, returning_type)`
/// (rewriteManip.c:966). Expects to start with an expression (not a Query).
pub fn SetVarReturningType(
    node: &mut Node,
    result_relation: i32,
    sublevels_up: i32,
    returning_type: VarReturningType,
) {
    let mut ctx = SetReturnCtx {
        result_relation,
        sublevels_up,
        returning_type,
    };
    SetVarReturningType_walker(node, &mut ctx);
}
