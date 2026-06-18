//! `ChangeVarNodes` family (rewriteManip.c:539) — adjust Var nodes for a specific
//! change of RT index (`rt_index` → `new_index`), plus `adjust_relid_set`.
//!
//! In-place mutation, mirroring the C "cheat and modify in-place" walker.
//!
//! # The `ChangeVarNodesExtended` callback
//!
//! The C callback is `bool (*)(Node *, ChangeVarNodes_context *)`; it inspects a
//! node before the standard walker and returns `true` to skip it, and it may
//! re-enter the walker over an expression via `ChangeVarNodesWalkExpression`.
//! In C this is expressible because the context is a raw pointer; in Rust a
//! self-referential `&mut dyn FnMut(&mut Node, &mut Context)` stored *inside* the
//! context cannot be constructed. We therefore thread the callback as a separate
//! argument alongside the [`ChangeVarNodesContext`], and expose
//! [`ChangeVarNodesWalkExpression`] for the callback to re-enter the standard
//! walker (without the callback, matching the C control flow where the callback
//! recursion delegates straight to `ChangeVarNodes_walker`).

#![allow(non_snake_case)]

use backend_nodes_core::node_walker::{expression_tree_walker_mut, query_tree_mutator};
use types_nodes::copy_query::Query;
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, ExprRelids};

use crate::relids;

/// `IS_SPECIAL_VARNO(varno)` (primnodes.h) — `((int) (varno) < 0)`. The special
/// varnos (INNER_VAR/OUTER_VAR/INDEX_VAR/ROWID_VAR) are the C negative sentinels
/// (-1..-4); real range-table indices are >= 1.
#[inline]
fn is_special_varno(varno: i32) -> bool {
    varno < 0
}

/// `adjust_relid_set(relids, oldrelid, newrelid)` (rewriteManip.c:760).
pub fn adjust_relid_set(relids_set: &ExprRelids, oldrelid: i32, newrelid: i32) -> ExprRelids {
    if !is_special_varno(oldrelid) && relids::is_member(oldrelid, relids_set) {
        let mut out = relids::copy(relids_set);
        out = relids::del_member(out, oldrelid);
        if !is_special_varno(newrelid) {
            out = relids::add_member(out, newrelid);
        }
        out
    } else {
        relids::copy(relids_set)
    }
}

/// The C `ChangeVarNodes_context` (callback threaded separately — see module
/// docs).
pub struct ChangeVarNodesContext {
    pub rt_index: i32,
    pub new_index: i32,
    pub sublevels_up: i32,
}

/// `ChangeVarNodes_callback` — process a node before the standard walker; a
/// `true` return tells the walker to skip the node entirely.
pub type ChangeVarNodesCallback<'a> = &'a mut dyn FnMut(&mut Node, &mut ChangeVarNodesContext) -> bool;

fn ChangeVarNodes_walker(
    node: &mut Node,
    context: &mut ChangeVarNodesContext,
    callback: &mut Option<ChangeVarNodesCallback>,
) -> bool {
    if let Some(cb) = callback {
        if cb(node, context) {
            return false;
        }
    }

    match node {
        Node::Expr(Expr::Var(var)) => {
            if var.varlevelsup as i32 == context.sublevels_up {
                if var.varno == context.rt_index {
                    var.varno = context.new_index;
                }
                var.varnullingrels =
                    adjust_relid_set(&var.varnullingrels, context.rt_index, context.new_index);
                if var.varnosyn as i32 == context.rt_index {
                    var.varnosyn = context.new_index as u32;
                }
            }
            false
        }
        Node::CurrentOfExpr(cexpr) => {
            if context.sublevels_up == 0 && cexpr.cvarno as i32 == context.rt_index {
                cexpr.cvarno = context.new_index as u32;
            }
            false
        }
        Node::RangeTblRef(rtr) => {
            if context.sublevels_up == 0 && rtr.rtindex == context.rt_index {
                rtr.rtindex = context.new_index;
            }
            false
        }
        Node::JoinExpr(j) => {
            if context.sublevels_up == 0 && j.rtindex == context.rt_index {
                j.rtindex = context.new_index;
            }
            expression_tree_walker_mut(node, &mut |n| {
                ChangeVarNodes_walker(n, context, callback)
            })
        }
        Node::Expr(Expr::PlaceHolderVar(_)) => {
            if let Node::Expr(Expr::PlaceHolderVar(phv)) = node {
                if phv.phlevelsup as i32 == context.sublevels_up {
                    phv.phrels =
                        adjust_relid_set(&phv.phrels, context.rt_index, context.new_index);
                    phv.phnullingrels = adjust_relid_set(
                        &phv.phnullingrels,
                        context.rt_index,
                        context.new_index,
                    );
                }
            }
            expression_tree_walker_mut(node, &mut |n| {
                ChangeVarNodes_walker(n, context, callback)
            })
        }
        Node::Query(q) => {
            context.sublevels_up += 1;
            let result =
                query_tree_mutator(q, &mut |n| ChangeVarNodes_walker(n, context, callback), 0);
            context.sublevels_up -= 1;
            result
        }
        _ => expression_tree_walker_mut(node, &mut |n| {
            ChangeVarNodes_walker(n, context, callback)
        }),
    }
}

/// `ChangeVarNodesExtended(node, rt_index, new_index, sublevels_up, callback)`
/// (rewriteManip.c:676).
pub fn ChangeVarNodesExtended(
    node: &mut Node,
    rt_index: i32,
    new_index: i32,
    sublevels_up: i32,
    callback: Option<ChangeVarNodesCallback>,
) {
    let mut context = ChangeVarNodesContext {
        rt_index,
        new_index,
        sublevels_up,
    };
    let mut callback = callback;

    if let Node::Query(qry) = node {
        change_query_self(qry, rt_index, new_index, sublevels_up);
        query_tree_mutator(
            qry,
            &mut |n| ChangeVarNodes_walker(n, &mut context, &mut callback),
            0,
        );
    } else {
        ChangeVarNodes_walker(node, &mut context, &mut callback);
    }
}

/// `ChangeVarNodes(node, rt_index, new_index, sublevels_up)` (rewriteManip.c:732).
pub fn ChangeVarNodes(node: &mut Node, rt_index: i32, new_index: i32, sublevels_up: i32) {
    ChangeVarNodesExtended(node, rt_index, new_index, sublevels_up, None);
}

/// `ChangeVarNodesWalkExpression(node, context)` (rewriteManip.c:743) — process
/// an expression within a custom `ChangeVarNodesExtended` callback. Re-enters the
/// standard walker without a callback (the callback delegates here only for plain
/// expression recursion).
pub fn ChangeVarNodesWalkExpression(node: &mut Node, context: &mut ChangeVarNodesContext) -> bool {
    let mut no_cb: Option<ChangeVarNodesCallback> = None;
    expression_tree_walker_mut(node, &mut |n| ChangeVarNodes_walker(n, context, &mut no_cb))
}

fn change_query_self(qry: &mut Query, rt_index: i32, new_index: i32, sublevels_up: i32) {
    if sublevels_up != 0 {
        return;
    }
    if qry.resultRelation == rt_index {
        qry.resultRelation = new_index;
    }
    if qry.mergeTargetRelation == rt_index {
        qry.mergeTargetRelation = new_index;
    }
    if let Some(oc) = qry.onConflict.as_deref_mut() {
        if oc.exclRelIndex == rt_index {
            oc.exclRelIndex = new_index;
        }
    }
    for rm in qry.rowMarks.iter_mut() {
        if let Node::RowMarkClause(rc) = &mut **rm {
            if rc.rti as i32 == rt_index {
                rc.rti = new_index as u32;
            }
        }
    }
}
