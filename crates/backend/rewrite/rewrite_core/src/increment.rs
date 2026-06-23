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

use alloc::string::ToString;
use ::nodes_core::node_walker::{
    expression_tree_walker_mut, query_or_expression_tree_mutator, query_tree_mutator,
    range_table_mutator,
};
use ::utils_error::ereport;
use types_error::{PgError, PgResult, ERROR};
use ::nodes::copy_query::Query;
use ::nodes::nodes::{ntag, Node};
use ::nodes::parsenodes::{RangeTblEntry, RTEKind};
use ::nodes::primnodes::{Expr, VarReturningType};

/// `elog(ERROR, ...)` shorthand.
fn elog_error(msg: &str) -> PgError {
    ereport(ERROR).errmsg_internal(msg.to_string()).into_error()
}

// ===========================================================================
// IncrementVarSublevelsUp (rewriteManip.c:776)
// ===========================================================================

struct IncrCtx<'mcx> {
    delta_sublevels_up: i32,
    min_sublevels_up: i32,
    /// Captured `elog(ERROR)` from inside the infallible walker callback; the
    /// public entry points surface it as `Err(PgError)` (mirrors C ereport).
    err: Option<PgError>,
    mcx: mcx::Mcx<'mcx>,
}

/// Recurse into a node's children via the in-place walker, supplying a per-call
/// scratch arena for its transient `Node::Expr` wrappers. The walk itself never
/// allocates (it `mem::replace`s children in place); the `Mcx` is threaded only
/// so the future opaque-`Node` flip's `mk_expr` has a context. Scratch is freed
/// on return.
fn incr_walk_children<'mcx>(node: &mut Node<'mcx>, ctx: &mut IncrCtx<'mcx>) -> bool {
    let mcx = ctx.mcx;
    expression_tree_walker_mut(node, &mut |n| IncrementVarSublevelsUp_walker(n, ctx), mcx)
}

fn IncrementVarSublevelsUp_walker<'mcx>(node: &mut Node<'mcx>, ctx: &mut IncrCtx<'mcx>) -> bool {
    if ctx.err.is_some() {
        return true; // abort the remaining walk
    }
    match node.node_tag() {
        ntag::T_Var => {
            let var = node.as_var_mut().unwrap();
            if var.varlevelsup as i32 >= ctx.min_sublevels_up {
                var.varlevelsup =
                    (var.varlevelsup as i32 + ctx.delta_sublevels_up) as u32;
            }
            false // done here
        }
        ntag::T_CurrentOfExpr => {
            // this should not happen
            if ctx.min_sublevels_up == 0 {
                ctx.err = Some(elog_error("cannot push down CurrentOfExpr"));
                return true;
            }
            false
        }
        ntag::T_Aggref => {
            let agg = node.as_aggref_mut().unwrap();
            if agg.agglevelsup as i32 >= ctx.min_sublevels_up {
                agg.agglevelsup =
                    (agg.agglevelsup as i32 + ctx.delta_sublevels_up) as u32;
            }
            // fall through to recurse into argument
            incr_walk_children(node, ctx)
        }
        ntag::T_GroupingFunc => {
            // In an analyzed tree a `GroupingFunc` is carried as the
            // `Expr::GroupingFunc` variant (`NodePayload_Expr`), which shares the
            // `T_GroupingFunc` tag with the standalone raw-parse `NodePayload_
            // GroupingFunc`. The two have different memory layouts, so the
            // Node-level `as_groupingfunc_mut` (which downcasts to the standalone
            // payload) would misread an `Expr`-wrapped node; access the
            // `agglevelsup` through the `Expr` routing, mirroring the `T_Aggref`
            // arm above.
            if let Some(Expr::GroupingFunc(grp)) = node.as_expr_mut() {
                if grp.agglevelsup as i32 >= ctx.min_sublevels_up {
                    grp.agglevelsup =
                        (grp.agglevelsup as i32 + ctx.delta_sublevels_up) as u32;
                }
            } else {
                let grp = node.as_groupingfunc_mut().unwrap();
                if grp.agglevelsup as i32 >= ctx.min_sublevels_up {
                    grp.agglevelsup =
                        (grp.agglevelsup as i32 + ctx.delta_sublevels_up) as u32;
                }
            }
            incr_walk_children(node, ctx)
        }
        ntag::T_PlaceHolderVar => {
            let phv = node.as_placeholdervar_mut().unwrap();
            if phv.phlevelsup as i32 >= ctx.min_sublevels_up {
                phv.phlevelsup =
                    (phv.phlevelsup as i32 + ctx.delta_sublevels_up) as u32;
            }
            incr_walk_children(node, ctx)
        }
        ntag::T_ReturningExpr => {
            let rexpr = node.as_returningexpr_mut().unwrap();
            if rexpr.retlevelsup >= ctx.min_sublevels_up {
                rexpr.retlevelsup += ctx.delta_sublevels_up;
            }
            incr_walk_children(node, ctx)
        }
        ntag::T_Query => {
            let mcx = ctx.mcx;
            let q = node.as_query_mut().unwrap();
            ctx.min_sublevels_up += 1;
            increment_query_ctes(q, ctx);
            let result =
                query_tree_mutator(q, &mut |n| IncrementVarSublevelsUp_walker(n, ctx), 0, mcx);
            ctx.min_sublevels_up -= 1;
            result
        }
        _ => incr_walk_children(node, ctx),
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
pub fn IncrementVarSublevelsUp<'mcx>(
    node: &mut Node<'mcx>,
    delta_sublevels_up: i32,
    min_sublevels_up: i32,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<()> {
    let mut ctx = IncrCtx {
        delta_sublevels_up,
        min_sublevels_up,
        err: None,
        mcx,
    };
    // C uses query_or_expression_tree_walker(..., QTW_EXAMINE_RTES_BEFORE).
    // Starting at a Query does NOT increment min_sublevels_up, so we bump the
    // top Query's own RTE_CTE entries at the current level before descending.
    if let Some(q) = node.as_query_mut() {
        increment_query_ctes(q, &ctx);
    }
    query_or_expression_tree_mutator(
        node,
        &mut |n| IncrementVarSublevelsUp_walker(n, &mut ctx),
        0,
        mcx,
    );
    match ctx.err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `IncrementVarSublevelsUp_rtable(rtable, delta_sublevels_up, min_sublevels_up)`
/// (rewriteManip.c:903).
pub fn IncrementVarSublevelsUp_rtable<'mcx>(
    rtable: &mut [RangeTblEntry<'mcx>],
    delta_sublevels_up: i32,
    min_sublevels_up: i32,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<()> {
    let mut ctx = IncrCtx {
        delta_sublevels_up,
        min_sublevels_up,
        err: None,
        mcx,
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
        mcx,
    );
    match ctx.err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// ===========================================================================
// SetVarReturningType (rewriteManip.c:919)
// ===========================================================================

struct SetReturnCtx<'mcx> {
    result_relation: i32,
    sublevels_up: i32,
    returning_type: VarReturningType,
    mcx: mcx::Mcx<'mcx>,
}

fn SetVarReturningType_walker<'mcx>(node: &mut Node<'mcx>, ctx: &mut SetReturnCtx<'mcx>) -> bool {
    match node.node_tag() {
        ntag::T_Var => {
            let var = node.as_var_mut().unwrap();
            if var.varno == ctx.result_relation && var.varlevelsup as i32 == ctx.sublevels_up {
                var.varreturningtype = ctx.returning_type;
            }
            false
        }
        ntag::T_Query => {
            let mcx = ctx.mcx;
            let q = node.as_query_mut().unwrap();
            ctx.sublevels_up += 1;
            let result =
                query_tree_mutator(q, &mut |n| SetVarReturningType_walker(n, ctx), 0, mcx);
            ctx.sublevels_up -= 1;
            result
        }
        _ => {
            let mcx = ctx.mcx;
            expression_tree_walker_mut(node, &mut |n| SetVarReturningType_walker(n, ctx), mcx)
        }
    }
}

/// `SetVarReturningType(node, result_relation, sublevels_up, returning_type)`
/// (rewriteManip.c:966). Expects to start with an expression (not a Query).
pub fn SetVarReturningType<'mcx>(
    node: &mut Node<'mcx>,
    result_relation: i32,
    sublevels_up: i32,
    returning_type: VarReturningType,
    mcx: mcx::Mcx<'mcx>,
) {
    let mut ctx = SetReturnCtx {
        result_relation,
        sublevels_up,
        returning_type,
        mcx,
    };
    SetVarReturningType_walker(node, &mut ctx);
}
