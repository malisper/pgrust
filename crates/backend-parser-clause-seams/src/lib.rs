//! Seam declarations for the `backend-parser-clause` unit
//! (`parser/parse_clause.c`).
//!
//! The owning unit installs these from its `init_seams()`; until then a call
//! panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `transformWhereClause(pstate, clause, exprKind, constructName)`
    /// (parse_clause.c): transform a qualification expression and coerce it to
    /// boolean. Used for WHERE and allied clauses (here the aggregate `FILTER`
    /// clause reached from `ParseFuncOrColumn`, parse_func.c). `Ok(None)` is the
    /// C `NULL` clause / `NULL` result.
    pub fn transform_where_clause<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
        clause: Option<types_nodes::nodes::Node<'mcx>>,
        expr_kind: types_nodes::parsestmt::ParseExprKind,
        construct_name: &str,
    ) -> types_error::PgResult<Option<types_nodes::primnodes::Expr>>
);
