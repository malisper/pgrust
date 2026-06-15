//! Seam declarations for the `backend-parser-parse-target` unit
//! (`parser/parse_target.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly (mirror-PG-and-panic). Consumers (`parse_clause.c`)
//! depend on this `-seams` crate, never on the unported owner.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::{Expr, TargetEntry};

seam_core::seam!(
    /// `transformTargetEntry(pstate, node, expr, exprKind, colname, resjunk)`
    /// (parse_target.c): build a `TargetEntry` from a (possibly already
    /// transformed) expression. `node` is the original parse node; `expr` is the
    /// pre-transformed expression (the caller already ran `transformExpr`, so it
    /// is `Some` here at the parse_clause.c call sites). `colname` is the column
    /// name (`None` => `FigureColname(node)`); `resjunk` flags a junk column.
    /// Allocates the entry / can `ereport(ERROR)`.
    pub fn transform_target_entry<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        node: &Node<'mcx>,
        expr: Expr,
        expr_kind: ParseExprKind,
        colname: Option<&str>,
        resjunk: bool,
    ) -> PgResult<TargetEntry<'mcx>>
);
