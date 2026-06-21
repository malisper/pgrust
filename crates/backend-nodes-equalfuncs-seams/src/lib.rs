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
    pub fn equal_expr<'a, 'b>(a: &types_nodes::primnodes::Expr<'a>, b: &types_nodes::primnodes::Expr<'b>) -> bool
);

seam_core::seam!(
    /// `equal((Node *) a, (Node *) b)` (equalfuncs.c) — structural equality of
    /// two arbitrary nodes (used by `transformWindowFuncCall` to de-duplicate
    /// WINDOW definitions: PARTITION BY / ORDER BY clause lists and frame
    /// offset expressions are general nodes, not bare `Expr`s).
    pub fn equal_node<'mcx>(a: &types_nodes::nodes::Node<'mcx>, b: &types_nodes::nodes::Node<'mcx>) -> bool
);

seam_core::seam!(
    /// `equal((List *) a, (List *) b)` over two `List *` of `Expr *` — the
    /// generic `_equalList` form for an expression list (`COMPARE_NODE_FIELD`
    /// over a `List<Expr>`). Used by prepagg's `find_compatible_agg` to compare
    /// `Aggref.aggdirectargs` (an `Expr` list) and by any prep-layer dedup that
    /// compares two owned `Vec<Expr>`. Length then element-wise `equal()`.
    pub fn equal_expr_list<'mcx>(a: &[types_nodes::primnodes::Expr<'mcx>], b: &[types_nodes::primnodes::Expr<'mcx>]) -> bool
);

seam_core::seam!(
    /// `equal((List *) a, (List *) b)` over two `List *` of `TargetEntry *` —
    /// the `_equalList`/`_equalTargetEntry` form for a target list. Used by
    /// prepagg's `find_compatible_agg` to compare `Aggref.args` (analyzed
    /// aggregate argument target lists). Length then element-wise `equal()`.
    pub fn equal_targetentry_list<'mcx>(
        a: &[types_nodes::primnodes::TargetEntry<'mcx>],
        b: &[types_nodes::primnodes::TargetEntry<'mcx>]
    ) -> bool
);

seam_core::seam!(
    /// `equal((List *) a, (List *) b)` over two `List *` of `SortGroupClause *`
    /// — the `_equalList`/`_equalSortGroupClause` form. Used by prepagg's
    /// `find_compatible_agg` to compare `Aggref.aggorder` / `Aggref.aggdistinct`
    /// (ORDER BY / DISTINCT sort-group clause lists). Length then element-wise.
    pub fn equal_sortgroupclause_list(
        a: &[types_nodes::rawnodes::SortGroupClause],
        b: &[types_nodes::rawnodes::SortGroupClause]
    ) -> bool
);
