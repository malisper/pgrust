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

use ::nodes_core::node_walker::{
    expression_tree_walker_mut, query_or_expression_tree_mutator, query_tree_mutator,
};
use ::nodes::nodes::{ntag, Node};
use ::nodes::primnodes::ExprRelids;

use crate::relids;

// ===========================================================================
// add_nulling_relids (rewriteManip.c:1239)
// ===========================================================================

struct AddNullingCtx<'a, 'mcx> {
    /// `NULL` target_relids means "all level-zero Vars/PHVs".
    target_relids: Option<&'a ExprRelids>,
    added_relids: &'a ExprRelids,
    sublevels_up: i32,
    /// The query arena the walked tree lives in — the opaque `Node` is invariant,
    /// so the walker's transient `mk_expr` wrappers must share this lifetime.
    mcx: mcx::Mcx<'mcx>,
}

/// Recurse into a node's children via the in-place walker with a per-call
/// scratch arena for its transient `Node::Expr` wrappers (the walk never
/// allocates — `mcx` is threaded only for the future opaque-`Node` flip's
/// `mk_expr`; freed on return).
fn add_walk_children<'mcx>(node: &mut Node<'mcx>, ctx: &mut AddNullingCtx<'_, 'mcx>) -> bool {
    let mcx = ctx.mcx;
    expression_tree_walker_mut(node, &mut |n| add_nulling_relids_mutator(n, ctx), mcx)
}

fn add_nulling_relids_mutator<'mcx>(node: &mut Node<'mcx>, ctx: &mut AddNullingCtx<'_, 'mcx>) -> bool {
    match node.node_tag() {
        ntag::T_Var => {
            let var = node.as_var_mut().unwrap();
            if var.varlevelsup as i32 == ctx.sublevels_up
                && (ctx.target_relids.is_none()
                    || relids::is_member(var.varno, ctx.target_relids.unwrap()))
            {
                var.varnullingrels = relids::union(&var.varnullingrels, ctx.added_relids);
            }
            false
        }
        ntag::T_PlaceHolderVar => {
            let phv = node.as_placeholdervar_mut().unwrap();
            if phv.phlevelsup as i32 == ctx.sublevels_up
                && (ctx.target_relids.is_none()
                    || relids::overlap(&phv.phrels, ctx.target_relids.unwrap()))
            {
                // We don't modify the PHV's expression, only add to
                // phnullingrels.
                phv.phnullingrels = relids::union(&phv.phnullingrels, ctx.added_relids);
                return false;
            }
            // Otherwise fall through to copy the PlaceHolderVar normally
            add_walk_children(node, ctx)
        }
        ntag::T_Query => {
            let mcx = ctx.mcx;
            let q = node.as_query_mut().unwrap();
            ctx.sublevels_up += 1;
            let result = query_tree_mutator(q, &mut |n| add_nulling_relids_mutator(n, ctx), 0, mcx);
            ctx.sublevels_up -= 1;
            result
        }
        _ => add_walk_children(node, ctx),
    }
}

/// `add_nulling_relids(node, target_relids, added_relids)` (rewriteManip.c:1239).
/// `target_relids = None` means all level-zero Vars/PHVs are modified.
pub fn add_nulling_relids<'mcx>(
    node: &mut Node<'mcx>,
    target_relids: Option<&ExprRelids>,
    added_relids: &ExprRelids,
    mcx: mcx::Mcx<'mcx>,
) {
    let mut ctx = AddNullingCtx {
        target_relids,
        added_relids,
        sublevels_up: 0,
        mcx,
    };
    query_or_expression_tree_mutator(
        node,
        &mut |n| add_nulling_relids_mutator(n, &mut ctx),
        0,
        mcx,
    );
}

// ===========================================================================
// remove_nulling_relids (rewriteManip.c:1327)
// ===========================================================================

struct RemoveNullingCtx<'a, 'mcx> {
    removable_relids: &'a ExprRelids,
    except_relids: &'a ExprRelids,
    sublevels_up: i32,
    mcx: mcx::Mcx<'mcx>,
}

/// Recurse into a node's children via the in-place walker with a per-call
/// scratch arena (see [`add_walk_children`] — the walk never allocates; `mcx` is
/// threaded only for the future opaque-`Node` flip).
fn remove_walk_children<'mcx>(node: &mut Node<'mcx>, ctx: &mut RemoveNullingCtx<'_, 'mcx>) -> bool {
    let mcx = ctx.mcx;
    expression_tree_walker_mut(node, &mut |n| remove_nulling_relids_mutator(n, ctx), mcx)
}

fn remove_nulling_relids_mutator<'mcx>(node: &mut Node<'mcx>, ctx: &mut RemoveNullingCtx<'_, 'mcx>) -> bool {
    match node.node_tag() {
        ntag::T_Var => {
            let var = node.as_var_mut().unwrap();
            if var.varlevelsup as i32 == ctx.sublevels_up
                && !relids::is_member(var.varno, ctx.except_relids)
                && relids::overlap(&var.varnullingrels, ctx.removable_relids)
            {
                var.varnullingrels =
                    relids::difference(&var.varnullingrels, ctx.removable_relids);
            }
            false
        }
        ntag::T_PlaceHolderVar => {
            let matched = {
                let phv = node.as_placeholdervar().unwrap();
                phv.phlevelsup as i32 == ctx.sublevels_up
                    && !relids::overlap(&phv.phrels, ctx.except_relids)
            };
            if matched {
                // Copy the PlaceHolderVar and mutate what's below ...
                remove_walk_children(node, ctx);
                let phv = node.as_placeholdervar_mut().unwrap();
                phv.phnullingrels =
                    relids::difference(&phv.phnullingrels, ctx.removable_relids);
                // We must also update phrels, if it contains a removable RTI.
                phv.phrels = relids::difference(&phv.phrels, ctx.removable_relids);
                debug_assert!(!relids::is_empty(&phv.phrels));
                false
            } else {
                // Otherwise fall through to copy the PlaceHolderVar normally
                remove_walk_children(node, ctx)
            }
        }
        ntag::T_Query => {
            let mcx = ctx.mcx;
            let q = node.as_query_mut().unwrap();
            ctx.sublevels_up += 1;
            let result =
                query_tree_mutator(q, &mut |n| remove_nulling_relids_mutator(n, ctx), 0, mcx);
            ctx.sublevels_up -= 1;
            result
        }
        _ => remove_walk_children(node, ctx),
    }
}

/// `remove_nulling_relids(node, removable_relids, except_relids)`
/// (rewriteManip.c:1327).
pub fn remove_nulling_relids<'mcx>(
    node: &mut Node<'mcx>,
    removable_relids: &ExprRelids,
    except_relids: &ExprRelids,
    mcx: mcx::Mcx<'mcx>,
) {
    let mut ctx = RemoveNullingCtx {
        removable_relids,
        except_relids,
        sublevels_up: 0,
        mcx,
    };
    query_or_expression_tree_mutator(
        node,
        &mut |n| remove_nulling_relids_mutator(n, &mut ctx),
        0,
        mcx,
    );
}

/// `remove_nulling_relids((Node *) query, removable_relids, except_relids)` —
/// the `IsA(node, Query)` entry of [`remove_nulling_relids`], applied directly
/// to a `&mut Query` (the repo's owned `root->parse`, which can't be moved into
/// a `Node::Query` wrapper from behind a `&mut` borrow). This is observationally
/// identical to wrapping the `Query` in a `Node::Query` and calling
/// [`remove_nulling_relids`]: the `query_or_expression_tree_mutator`
/// `Node::Query(q)` arm is exactly `query_tree_mutator(q, mutator, flags)` (it
/// does NOT bump `sublevels_up` — the top query is level 0).
pub fn remove_nulling_relids_in_query<'mcx>(
    query: &mut ::nodes::copy_query::Query<'mcx>,
    removable_relids: &ExprRelids,
    except_relids: &ExprRelids,
    mcx: mcx::Mcx<'mcx>,
) {
    let mut ctx = RemoveNullingCtx {
        removable_relids,
        except_relids,
        sublevels_up: 0,
        mcx,
    };
    query_tree_mutator(query, &mut |n| remove_nulling_relids_mutator(n, &mut ctx), 0, mcx);
}
