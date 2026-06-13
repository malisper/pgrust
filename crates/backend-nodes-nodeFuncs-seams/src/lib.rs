//! Seam declarations for the `backend-nodes-nodeFuncs` unit
//! (`nodes/nodeFuncs.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_core::Oid;
use types_error::PgResult;
use types_nodes::Expr;

/// The `(typid, typmod, collation)` triple `exprType`/`exprTypmod`/
/// `exprCollation` report for one expression node.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExprTypeInfo {
    /// `exprType(expr)`.
    pub typid: Oid,
    /// `exprTypmod(expr)`.
    pub typmod: i32,
    /// `exprCollation(expr)`.
    pub collation: Oid,
}

seam_core::seam!(
    /// `exprType(expr)` / `exprTypmod(expr)` / `exprCollation(expr)`
    /// (nodeFuncs.c): the result type OID, type modifier, and collation of an
    /// expression node, read together. The three C functions are pure node
    /// inspections (no allocation); the bundling lets partition-key build read
    /// all three from one call. `Err` carries the C `elog(ERROR, "unrecognized
    /// node type")` for an unexpected tag.
    pub fn expr_type_info(expr: &Expr) -> PgResult<ExprTypeInfo>
);
