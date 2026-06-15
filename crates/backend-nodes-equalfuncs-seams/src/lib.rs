//! Seam declarations for `src/backend/nodes/equalfuncs.c` — the generic
//! structural node-equality routine `equal()`.
//!
//! `equal((Node *) a, (Node *) b)` is owned by the not-yet-ported equalfuncs.c.
//! The optimizer's tlist routines (`tlist_member`, `tlist_same_exprs`,
//! `add_to_flat_tlist`, the PathTarget de-dup) compare expression sub-trees with
//! it. indxpath.c's `find_list_position` and the expression-index-column branch
//! of `match_index_to_operand` also compare two clause nodes via `equal()`.
//! The owned-tree analogue takes two `&Expr` and returns whether they are
//! structurally equal. Until equalfuncs.c lands, a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `equal((Node *) a, (Node *) b)` (equalfuncs.c) — structural equality of
    /// two expression sub-trees.
    pub fn equal_expr(a: &types_nodes::primnodes::Expr, b: &types_nodes::primnodes::Expr) -> bool
);
