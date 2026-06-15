//! `add_nulling_relids` (rewriteManip.c:1239) and `remove_nulling_relids`
//! (rewriteManip.c:1327) — adjust `Var.varnullingrels` / `PlaceHolderVar`
//! `phnullingrels`/`phrels`.
//!
//! The C mutators copy the Var/PHV before editing its relids, because the input
//! tree may be shared. The repo's mutator model owns the tree in place
//! (`&mut Node`), so we edit the existing node directly — observationally
//! identical (the C "Copy the Var … and replace the copy's field" produces a
//! fresh node with the new relids, which is exactly the in-place result over an
//! owned tree).

#![allow(non_snake_case)]

use backend_nodes_core::node_walker::{
    expression_tree_walker_mut, query_or_expression_tree_mutator, query_tree_mutator,
};
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, ExprRelids};

use crate::relids;

// ===========================================================================
// add_nulling_relids (rewriteManip.c:1239)
// ===========================================================================

struct AddNullingCtx<'a> {
    /// `NULL` target_relids means "all level-zero Vars/PHVs".
    target_relids: Option<&'a ExprRelids>,
    added_relids: &'a ExprRelids,
    sublevels_up: i32,
}

fn add_nulling_relids_mutator(node: &mut Node, ctx: &mut AddNullingCtx) -> bool {
    match node {
        Node::Expr(Expr::Var(var)) => {
            if var.varlevelsup as i32 == ctx.sublevels_up
                && (ctx.target_relids.is_none()
                    || relids::is_member(var.varno, ctx.target_relids.unwrap()))
            {
                var.varnullingrels = relids::union(&var.varnullingrels, ctx.added_relids);
            }
            false
        }
        Node::Expr(Expr::PlaceHolderVar(_)) => {
            if let Node::Expr(Expr::PlaceHolderVar(phv)) = node {
                if phv.phlevelsup as i32 == ctx.sublevels_up
                    && (ctx.target_relids.is_none()
                        || relids::overlap(&phv.phrels, ctx.target_relids.unwrap()))
                {
                    // We don't modify the PHV's expression, only add to
                    // phnullingrels.
                    phv.phnullingrels = relids::union(&phv.phnullingrels, ctx.added_relids);
                    return false;
                }
            }
            // Otherwise fall through to copy the PlaceHolderVar normally
            expression_tree_walker_mut(node, &mut |n| add_nulling_relids_mutator(n, ctx))
        }
        Node::Query(q) => {
            ctx.sublevels_up += 1;
            let result = query_tree_mutator(q, &mut |n| add_nulling_relids_mutator(n, ctx), 0);
            ctx.sublevels_up -= 1;
            result
        }
        _ => expression_tree_walker_mut(node, &mut |n| add_nulling_relids_mutator(n, ctx)),
    }
}

/// `add_nulling_relids(node, target_relids, added_relids)` (rewriteManip.c:1239).
/// `target_relids = None` means all level-zero Vars/PHVs are modified.
pub fn add_nulling_relids(
    node: &mut Node,
    target_relids: Option<&ExprRelids>,
    added_relids: &ExprRelids,
) {
    let mut ctx = AddNullingCtx {
        target_relids,
        added_relids,
        sublevels_up: 0,
    };
    query_or_expression_tree_mutator(
        node,
        &mut |n| add_nulling_relids_mutator(n, &mut ctx),
        0,
    );
}

// ===========================================================================
// remove_nulling_relids (rewriteManip.c:1327)
// ===========================================================================

struct RemoveNullingCtx<'a> {
    removable_relids: &'a ExprRelids,
    except_relids: &'a ExprRelids,
    sublevels_up: i32,
}

fn remove_nulling_relids_mutator(node: &mut Node, ctx: &mut RemoveNullingCtx) -> bool {
    match node {
        Node::Expr(Expr::Var(var)) => {
            if var.varlevelsup as i32 == ctx.sublevels_up
                && !relids::is_member(var.varno, ctx.except_relids)
                && relids::overlap(&var.varnullingrels, ctx.removable_relids)
            {
                var.varnullingrels =
                    relids::difference(&var.varnullingrels, ctx.removable_relids);
            }
            false
        }
        Node::Expr(Expr::PlaceHolderVar(_)) => {
            let matched = if let Node::Expr(Expr::PlaceHolderVar(phv)) = node {
                phv.phlevelsup as i32 == ctx.sublevels_up
                    && !relids::overlap(&phv.phrels, ctx.except_relids)
            } else {
                false
            };
            if matched {
                // Copy the PlaceHolderVar and mutate what's below ...
                expression_tree_walker_mut(node, &mut |n| {
                    remove_nulling_relids_mutator(n, ctx)
                });
                if let Node::Expr(Expr::PlaceHolderVar(phv)) = node {
                    phv.phnullingrels =
                        relids::difference(&phv.phnullingrels, ctx.removable_relids);
                    // We must also update phrels, if it contains a removable RTI.
                    phv.phrels = relids::difference(&phv.phrels, ctx.removable_relids);
                    debug_assert!(!relids::is_empty(&phv.phrels));
                }
                false
            } else {
                // Otherwise fall through to copy the PlaceHolderVar normally
                expression_tree_walker_mut(node, &mut |n| remove_nulling_relids_mutator(n, ctx))
            }
        }
        Node::Query(q) => {
            ctx.sublevels_up += 1;
            let result =
                query_tree_mutator(q, &mut |n| remove_nulling_relids_mutator(n, ctx), 0);
            ctx.sublevels_up -= 1;
            result
        }
        _ => expression_tree_walker_mut(node, &mut |n| remove_nulling_relids_mutator(n, ctx)),
    }
}

/// `remove_nulling_relids(node, removable_relids, except_relids)`
/// (rewriteManip.c:1327).
pub fn remove_nulling_relids(
    node: &mut Node,
    removable_relids: &ExprRelids,
    except_relids: &ExprRelids,
) {
    let mut ctx = RemoveNullingCtx {
        removable_relids,
        except_relids,
        sublevels_up: 0,
    };
    query_or_expression_tree_mutator(
        node,
        &mut |n| remove_nulling_relids_mutator(n, &mut ctx),
        0,
    );
}
