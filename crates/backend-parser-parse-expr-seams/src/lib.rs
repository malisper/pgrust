//! Seam declarations for the `backend-parser-parse-expr` unit
//! (`parser/parse_expr.c`, `parse_coerce.c`, `parse_collate.c`,
//! `parse_node.c`) covering the EXECUTE-parameter analysis the prepare driver
//! performs per parameter.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgBox};
use types_core::Oid;
use types_error::PgResult;
use types_tuple::Datum;
use types_nodes::nodes::Node;
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::Expr;

seam_core::seam!(
    /// `transformExpr(pstate, expr, exprKind)` (parse_expr.c) — analyze and
    /// transform a raw-grammar expression node into a fully-typed [`Expr`].
    /// `expr` is the (untransformed) raw `Node`; `None` for a NULL input yields
    /// `None`. Saves/restores `pstate->p_expr_kind`. Allocates / can
    /// `ereport(ERROR)`. Owned by `backend-parser-parse-expr`; consumed by
    /// `parse_target.c` to avoid the parse_target ⇆ parse_expr crate cycle.
    pub fn transformExpr<'mcx>(
        pstate: &mut ParseState<'mcx>,
        expr: Option<Node<'mcx>>,
        expr_kind: ParseExprKind,
    ) -> PgResult<Option<Expr<'static>>>
);

/// Result of [`analyze_one_exec_param`] — mirrors the per-parameter body of
/// `EvaluateParams`: `transformExpr(EXPR_KIND_EXECUTE_PARAMETER)`,
/// `exprType`, `coerce_to_target_type(COERCION_ASSIGNMENT,
/// COERCE_IMPLICIT_CAST)`, `assign_expr_collations`, `lfirst(l) = expr`.
/// Returned so the driver reproduces the cannot-be-coerced ereport in-crate
/// with C's exact branch order.
pub struct AnalyzedExecParam<'mcx> {
    /// The analyzed/coerced/collated expression (`lfirst(l) = expr`), the real
    /// [`Expr`] tree threaded on into `ExecPrepareExprList`. `None` when
    /// `coerce_to_target_type(...)` returned NULL (the C NULL `expr`); the
    /// driver raises the cannot-be-coerced ereport in that case.
    pub expr: Option<PgBox<'mcx, Expr<'static>>>,
    /// `coerce_to_target_type(...)` returned NULL — the coercion failed.
    pub coercion_failed: bool,
    /// `exprType(expr)` of the given (pre-coercion) expression.
    pub given_type_id: Oid,
    /// `exprLocation(lfirst(l))` — the parser error position for the failure.
    pub expr_location: i32,
}

seam_core::seam!(
    /// Transform-analyze-coerce-collate one EXECUTE parameter expression,
    /// mirroring the per-parameter body of `EvaluateParams`:
    /// `expr = transformExpr(EXPR_KIND_EXECUTE_PARAMETER)`,
    /// `coerce_to_target_type(...)`, `assign_expr_collations`, `lfirst(l) = expr`.
    /// The finished real [`Expr`] is returned (the C stores it back into the
    /// list cell; the owned driver collects it into the working `Expr` list it
    /// then hands to `ExecPrepareExprList`). `raw_param` is the original
    /// parser-output node for this cell; `expected_type_id` is `param_types[i]`;
    /// `source_text` is the parse state's `p_sourcetext`. Allocates / can
    /// `ereport(ERROR)`.
    pub fn analyze_one_exec_param<'mcx>(
        mcx: Mcx<'mcx>,
        source_text: &str,
        raw_param: &Node<'mcx>,
        param_index: i32,
        expected_type_id: Oid,
    ) -> PgResult<AnalyzedExecParam<'mcx>>
);

seam_core::seam!(
    /// `parser_errposition(pstate, location)` (parse_node.c) — convert a raw
    /// parse `location` into the 1-based character cursor position for an
    /// error's `errposition`. Returns 0 when `location < 0`.
    pub fn parser_errposition(source_text: &str, location: i32) -> PgResult<i32>
);

seam_core::seam!(
    /// `ParseExprKindName(exprKind)` (parse_expr.c): a human-readable SQL
    /// construct name for a [`ParseExprKind`] (e.g. `"WHERE"`, `"GROUP BY"`),
    /// used in `set-returning functions are not allowed in %s` and similar
    /// error messages. Owned by parse_expr.c; consumed by
    /// `check_srf_call_placement` (parse_func.c) across the parser cycle.
    pub fn parse_expr_kind_name(expr_kind: ParseExprKind) -> &'static str
);

seam_core::seam!(
    /// `DirectFunctionCall1(jsonb_in, CStringGetDatum(val))` — parse a
    /// NUL-free cstring into an on-disk `jsonb` value and return the resulting
    /// `Datum` (a by-ref varlena). Used by `GetJsonBehaviorConst`
    /// (parse_expr.c) to build the `[]` / `{}` jsonb `Const` for EMPTY ARRAY /
    /// EMPTY OBJECT behaviors. Owned by `backend-utils-adt-jsonb` (which links
    /// the jsonb parser); declared here so the parser can build the const
    /// without depending on the jsonb crate. A parse failure raises `Err`.
    pub fn jsonb_const_from_cstring<'mcx>(mcx: Mcx<'mcx>, val: &str) -> PgResult<Datum<'mcx>>
);
