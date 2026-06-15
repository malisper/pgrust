//! Seam declarations for the `backend-optimizer-prep-prepqual` unit
//! (`optimizer/prep/prepqual.c`).
//!
//! The owning unit (`prepqual.c`) installs these from its `init_seams()` when
//! it lands; until then a call panics loudly. `clauses.c`'s const-folding
//! engine calls `negate_clause` from the `BoolExpr NOT_EXPR` arm and from
//! `simplify_boolean_equality`, and `canonicalize_qual` is the qual-canonical
//! form `expression_planner` applies on top of `eval_const_expressions` (not
//! reached by `eval_const_expressions` itself, but declared for the planner
//! callers).

#![allow(non_snake_case)]

seam_core::seam!(
    /// `negate_clause(node)` (prepqual.c): negate a boolean expression,
    /// applying de Morgan / operator-negator simplifications where possible
    /// (e.g. `NOT (a AND b)` → `(NOT a) OR (NOT b)`, `NOT (x = y)` →
    /// `x <> y`). Consumes the owned clause and returns the negated tree.
    /// `Err` carries the catalog-lookup `ereport(ERROR)` surface (the negator
    /// resolution reads `pg_operator`).
    pub fn negate_clause(
        node: types_nodes::primnodes::Expr,
    ) -> types_error::PgResult<types_nodes::primnodes::Expr>
);

seam_core::seam!(
    /// `canonicalize_qual(qual, is_check)` (prepqual.c): convert a qual to
    /// canonical (implicit-AND-of-ORs) form, dropping redundant clauses.
    /// `is_check` selects the CHECK-constraint NULL semantics. `Err` carries
    /// the catalog-lookup `ereport(ERROR)` surface.
    pub fn canonicalize_qual(
        qual: Option<types_nodes::primnodes::Expr>,
        is_check: bool,
    ) -> types_error::PgResult<Option<types_nodes::primnodes::Expr>>
);
