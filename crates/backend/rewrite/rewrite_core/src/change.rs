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

use nodes_core::node_walker::{expression_tree_walker_mut, query_tree_mutator};
use ::nodes::copy_query::Query;
use ::nodes::nodes::{ntag, Node};
use ::nodes::primnodes::ExprRelids;

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
pub struct ChangeVarNodesContext<'mcx> {
    pub rt_index: i32,
    pub new_index: i32,
    pub sublevels_up: i32,
    /// Query arena the walked tree lives in (opaque `Node` invariance).
    pub mcx: mcx::Mcx<'mcx>,
}

/// `ChangeVarNodes_callback` — process a node before the standard walker; a
/// `true` return tells the walker to skip the node entirely.
pub type ChangeVarNodesCallback<'a> = &'a mut dyn FnMut(&mut Node, &mut ChangeVarNodesContext) -> bool;

pub fn ChangeVarNodes_walker<'mcx>(
    node: &mut Node<'mcx>,
    context: &mut ChangeVarNodesContext<'mcx>,
    callback: &mut Option<ChangeVarNodesCallback>,
) -> bool {
    if let Some(cb) = callback {
        if cb(node, context) {
            return false;
        }
    }

    match node.node_tag() {
        ntag::T_Var => {
            let var = node.as_var_mut().unwrap();
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
        ntag::T_CurrentOfExpr => {
            let cexpr = node.as_currentofexpr_mut().unwrap();
            if context.sublevels_up == 0 && cexpr.cvarno as i32 == context.rt_index {
                cexpr.cvarno = context.new_index as u32;
            }
            false
        }
        ntag::T_RangeTblRef => {
            let rtr = node.as_rangetblref_mut().unwrap();
            if context.sublevels_up == 0 && rtr.rtindex == context.rt_index {
                rtr.rtindex = context.new_index;
            }
            false
        }
        ntag::T_JoinExpr => {
            let j = node.as_joinexpr_mut().unwrap();
            if context.sublevels_up == 0 && j.rtindex == context.rt_index {
                j.rtindex = context.new_index;
            }
            let mcx = context.mcx;
            expression_tree_walker_mut(
                node,
                &mut |n| ChangeVarNodes_walker(n, context, callback),
                mcx,
            )
        }
        ntag::T_PlaceHolderVar => {
            let phv = node.as_placeholdervar_mut().unwrap();
            if phv.phlevelsup as i32 == context.sublevels_up {
                phv.phrels =
                    adjust_relid_set(&phv.phrels, context.rt_index, context.new_index);
                phv.phnullingrels = adjust_relid_set(
                    &phv.phnullingrels,
                    context.rt_index,
                    context.new_index,
                );
            }
            let mcx = context.mcx;
            expression_tree_walker_mut(
                node,
                &mut |n| ChangeVarNodes_walker(n, context, callback),
                mcx,
            )
        }
        ntag::T_Query => {
            let mcx = context.mcx;
            let q = node.as_query_mut().unwrap();
            context.sublevels_up += 1;
            let result =
                query_tree_mutator(q, &mut |n| ChangeVarNodes_walker(n, context, callback), 0, mcx);
            context.sublevels_up -= 1;
            result
        }
        _ => {
            let mcx = context.mcx;
            expression_tree_walker_mut(
                node,
                &mut |n| ChangeVarNodes_walker(n, context, callback),
                mcx,
            )
        }
    }
}

/// `ChangeVarNodesExtended(node, rt_index, new_index, sublevels_up, callback)`
/// (rewriteManip.c:676).
pub fn ChangeVarNodesExtended<'mcx>(
    node: &mut Node<'mcx>,
    rt_index: i32,
    new_index: i32,
    sublevels_up: i32,
    callback: Option<ChangeVarNodesCallback>,
    mcx: mcx::Mcx<'mcx>,
) {
    let mut context = ChangeVarNodesContext {
        rt_index,
        new_index,
        sublevels_up,
        mcx,
    };
    let mut callback = callback;

    if let Some(qry) = node.as_query_mut() {
        change_query_self(qry, rt_index, new_index, sublevels_up);
        query_tree_mutator(
            qry,
            &mut |n| ChangeVarNodes_walker(n, &mut context, &mut callback),
            0,
            mcx,
        );
    } else {
        ChangeVarNodes_walker(node, &mut context, &mut callback);
    }
}

/// `ChangeVarNodes(node, rt_index, new_index, sublevels_up)` (rewriteManip.c:732).
pub fn ChangeVarNodes<'mcx>(
    node: &mut Node<'mcx>,
    rt_index: i32,
    new_index: i32,
    sublevels_up: i32,
    mcx: mcx::Mcx<'mcx>,
) {
    ChangeVarNodesExtended(node, rt_index, new_index, sublevels_up, None, mcx);
}

/// `ChangeVarNodesWalkExpression(node, context)` (rewriteManip.c:743) — process
/// an expression within a custom `ChangeVarNodesExtended` callback. Re-enters the
/// standard walker without a callback (the callback delegates here only for plain
/// expression recursion).
pub fn ChangeVarNodesWalkExpression<'mcx>(
    node: &mut Node<'mcx>,
    context: &mut ChangeVarNodesContext<'mcx>,
) -> bool {
    let mut no_cb: Option<ChangeVarNodesCallback> = None;
    let mcx = context.mcx;
    expression_tree_walker_mut(
        node,
        &mut |n| ChangeVarNodes_walker(n, context, &mut no_cb),
        mcx,
    )
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
        if let Some(rc) = rm.as_rowmarkclause_mut() {
            if rc.rti as i32 == rt_index {
                rc.rti = new_index as u32;
            }
        }
    }
}
