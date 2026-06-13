//! Seam declarations for the `backend-parser-parse-expr` unit
//! (`parser/parse_expr.c`, `parse_coerce.c`, `parse_collate.c`,
//! `parse_node.c`) covering the EXECUTE-parameter analysis the prepare driver
//! performs per parameter.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::nodes::Node;

/// Result of [`analyze_one_exec_param`] — mirrors the per-parameter body of
/// `EvaluateParams`: `transformExpr(EXPR_KIND_EXECUTE_PARAMETER)`,
/// `exprType`, `coerce_to_target_type(COERCION_ASSIGNMENT,
/// COERCE_IMPLICIT_CAST)`, `assign_expr_collations`, `lfirst(l) = expr`.
/// Returned so the driver reproduces the cannot-be-coerced ereport in-crate
/// with C's exact branch order.
#[derive(Clone, Copy, Debug)]
pub struct AnalyzedExecParam {
    /// `coerce_to_target_type(...)` returned NULL — the coercion failed.
    pub coercion_failed: bool,
    /// `exprType(expr)` of the given (pre-coercion) expression.
    pub given_type_id: Oid,
    /// `exprLocation(lfirst(l))` — the parser error position for the failure.
    pub expr_location: i32,
}

seam_core::seam!(
    /// Transform-analyze-coerce-collate one EXECUTE parameter expression and
    /// store the finished node back into its list cell
    /// (`params[param_index] = expr`), mirroring the per-parameter body of
    /// `EvaluateParams`. `expected_type_id` is `param_types[i]`; `source_text`
    /// is the parse state's `p_sourcetext`. Allocates / can `ereport(ERROR)`.
    pub fn analyze_one_exec_param<'mcx>(
        mcx: Mcx<'mcx>,
        source_text: &str,
        params: &mut [mcx::PgBox<'mcx, Node<'mcx>>],
        param_index: i32,
        expected_type_id: Oid,
    ) -> PgResult<AnalyzedExecParam>
);

seam_core::seam!(
    /// `parser_errposition(pstate, location)` (parse_node.c) — convert a raw
    /// parse `location` into the 1-based character cursor position for an
    /// error's `errposition`. Returns 0 when `location < 0`.
    pub fn parser_errposition(source_text: &str, location: i32) -> PgResult<i32>
);
