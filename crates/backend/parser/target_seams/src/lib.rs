//! Seam declarations for the `backend-parser-parse-target` unit
//! (`parser/parse_target.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly (mirror-PG-and-panic). Consumers (`parse_clause.c`)
//! depend on this `-seams` crate, never on the unported owner.

#![allow(non_snake_case)]

use ::mcx::{Mcx, PgString};
use ::types_error::PgResult;
use ::nodes::nodes::Node;
use ::nodes::parsestmt::{ParseExprKind, ParseState};
use ::nodes::primnodes::{Expr, TargetEntry};

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
        expr: Expr<'static>,
        expr_kind: ParseExprKind,
        colname: Option<&str>,
        resjunk: bool,
    ) -> PgResult<TargetEntry<'mcx>>
);

seam_core::seam!(
    /// `FigureColname(node)` (parse_target.c): derive a column name for a
    /// SELECT-output / FROM-function expression from the raw parse node, using
    /// the SQL spec's heuristics; returns `"?column?"` when none can be found.
    /// Reads the raw parse tree only (no `ParseState`). Allocates the name.
    pub fn FigureColname<'mcx>(mcx: Mcx<'mcx>, node: &Node<'mcx>) -> PgResult<PgString<'mcx>>
);
