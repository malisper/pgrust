//! Thin wrappers whose C bodies lean on `expression_planner()`
//! (`plan/planner.c`).
//!
//! `expression_planner(expr)` is `eval_const_expressions(NULL, (Node *) expr)`
//! plus an assertion sweep; the constant-folding engine itself is REAL (see
//! [`crate::fold`]), so these route through it directly.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::primnodes::Expr;

/// `contain_mutable_functions_after_planning(expr)` (clauses.c:488):
/// `expression_planner()` (default-argument insertion + function inlining +
/// constant folding) and then the grounded predicate.
pub fn contain_mutable_functions_after_planning<'mcx>(mcx: Mcx<'mcx>, expr: Expr<'mcx>) -> PgResult<bool> {
    // C: expr = expression_planner(expr); return contain_mutable_functions(expr);
    let planned = crate::fold::eval_const_expressions(mcx, expr)?;
    crate::grounded::contain_mutable_functions(Some(&planned))
}

/// `contain_volatile_functions_after_planning(expr)` (clauses.c:657).
pub fn contain_volatile_functions_after_planning<'mcx>(mcx: Mcx<'mcx>, expr: Expr<'mcx>) -> PgResult<bool> {
    let planned = crate::fold::eval_const_expressions(mcx, expr)?;
    crate::grounded::contain_volatile_functions(Some(&planned))
}
