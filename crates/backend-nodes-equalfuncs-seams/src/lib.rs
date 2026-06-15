//! Seam declaration for `nodes/equalfuncs.c`'s `equal()` over the unified
//! [`types_nodes::primnodes::Expr`] tree.
//!
//! indxpath.c's `find_list_position` and the expression-index-column branch of
//! `match_index_to_operand` test two clause nodes for structural equality via
//! `equal()`. equalfuncs.c is unported; the seam crosses that boundary and
//! defaults to a loud panic until it lands.

use types_nodes::primnodes::Expr;

seam_core::seam!(
    /// `equal(a, b)` (equalfuncs.c) — deep structural equality of two
    /// expression trees.
    pub fn equal_expr(a: &Expr, b: &Expr) -> bool
);
