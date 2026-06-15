//! `OffsetVarNodes` (rewriteManip.c:369) — adjust Var/range-table indexes when
//! appending one query's RT to another.
//!
//! Like the C original, this "cheats and modifies the nodes in-place" — the
//! caller must have copied the tree earlier. The repo's mutator model
//! (`&mut Node -> bool`) is exactly this in-place convention, so the C
//! `expression_tree_walker`-shaped mutator maps directly onto
//! [`backend_nodes_core::node_walker::expression_tree_walker_mut`] /
//! [`query_tree_mutator`].

#![allow(non_snake_case)]

use backend_nodes_core::node_walker::{expression_tree_walker_mut, query_tree_mutator};
use types_nodes::copy_query::Query;
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, ExprRelids};

use crate::relids;

struct OffsetCtx {
    offset: i32,
    sublevels_up: i32,
}

/// `offset_relid_set(relids, offset)` (rewriteManip.c:526) — produce a fresh set
/// with each member shifted up by `offset`.
fn offset_relid_set(relids_set: &ExprRelids, offset: i32) -> ExprRelids {
    let mut result = ExprRelids::default();
    let mut rtindex = -1i32;
    while let Some(m) = relids::next_member(relids_set, rtindex) {
        rtindex = m;
        result = relids::add_member(result, m + offset);
    }
    result
}

fn OffsetVarNodes_walker(node: &mut Node, ctx: &mut OffsetCtx) -> bool {
    match node {
        Node::Expr(Expr::Var(var)) => {
            if var.varlevelsup as i32 == ctx.sublevels_up {
                var.varno += ctx.offset;
                var.varnullingrels = offset_relid_set(&var.varnullingrels, ctx.offset);
                if var.varnosyn > 0 {
                    var.varnosyn = (var.varnosyn as i32 + ctx.offset) as u32;
                }
            }
            false
        }
        Node::CurrentOfExpr(cexpr) => {
            if ctx.sublevels_up == 0 {
                cexpr.cvarno = (cexpr.cvarno as i32 + ctx.offset) as u32;
            }
            false
        }
        Node::RangeTblRef(rtr) => {
            if ctx.sublevels_up == 0 {
                rtr.rtindex += ctx.offset;
            }
            // the subquery itself is visited separately
            false
        }
        Node::JoinExpr(j) => {
            if j.rtindex != 0 && ctx.sublevels_up == 0 {
                j.rtindex += ctx.offset;
            }
            // fall through to examine children
            expression_tree_walker_mut(node, &mut |n| OffsetVarNodes_walker(n, ctx))
        }
        Node::Expr(Expr::PlaceHolderVar(_)) => {
            // mutate phrels/phnullingrels in place, then recurse into children
            if let Node::Expr(Expr::PlaceHolderVar(phv)) = node {
                if phv.phlevelsup as i32 == ctx.sublevels_up {
                    phv.phrels = offset_relid_set(&phv.phrels, ctx.offset);
                    phv.phnullingrels = offset_relid_set(&phv.phnullingrels, ctx.offset);
                }
            }
            expression_tree_walker_mut(node, &mut |n| OffsetVarNodes_walker(n, ctx))
        }
        Node::Query(q) => {
            ctx.sublevels_up += 1;
            let result = query_tree_mutator(q, &mut |n| OffsetVarNodes_walker(n, ctx), 0);
            ctx.sublevels_up -= 1;
            result
        }
        // AppendRelInfo / PlanRowMark / SpecialJoinInfo / PlaceHolderInfo /
        // MinMaxAggInfo are planner auxiliary nodes that do not appear in the
        // central Node universe walked here (the C code Asserts they're absent
        // from parse/rewrite trees, and handles AppendRelInfo only in planner
        // structures that aren't reachable through this walker).
        _ => expression_tree_walker_mut(node, &mut |n| OffsetVarNodes_walker(n, ctx)),
    }
}

/// `OffsetVarNodes(node, offset, sublevels_up)` (rewriteManip.c:475).
pub fn OffsetVarNodes(node: &mut Node, offset: i32, sublevels_up: i32) {
    let mut ctx = OffsetCtx {
        offset,
        sublevels_up,
    };

    // Must be prepared to start with a Query or a bare expression tree; if it's a
    // Query, go straight to query_tree_walker so sublevels_up doesn't increment
    // prematurely.
    if let Node::Query(qry) = node {
        offset_query_self(qry, offset, sublevels_up);
        query_tree_mutator(qry, &mut |n| OffsetVarNodes_walker(n, &mut ctx), 0);
    } else {
        OffsetVarNodes_walker(node, &mut ctx);
    }
}

/// Fix range-table indexes carried directly in the Query header (the
/// `sublevels_up == 0` case from the C entry point).
fn offset_query_self(qry: &mut Query, offset: i32, sublevels_up: i32) {
    if sublevels_up != 0 {
        return;
    }
    if qry.resultRelation != 0 {
        qry.resultRelation += offset;
    }
    if qry.mergeTargetRelation != 0 {
        qry.mergeTargetRelation += offset;
    }
    if let Some(oc) = qry.onConflict.as_deref_mut() {
        if oc.exclRelIndex != 0 {
            oc.exclRelIndex += offset;
        }
    }
    for rm in qry.rowMarks.iter_mut() {
        if let Node::RowMarkClause(rc) = &mut **rm {
            rc.rti = (rc.rti as i32 + offset) as u32;
        }
    }
}
