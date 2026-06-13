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
use types_fmgr::ExternalFnExpr;

seam_core::seam!(
    /// `exprType(expr)` / `exprTypmod(expr)` / `exprCollation(expr)`
    /// (nodeFuncs.c): the result type OID, type modifier, and collation of an
    /// expression node, read together. The three C functions are pure node
    /// inspections (no allocation); the bundling lets partition-key build read
    /// all three from one call. `Err` carries the C `elog(ERROR, "unrecognized
    /// node type")` for an unexpected tag.
    pub fn expr_type_info(expr: &Expr) -> PgResult<ExprTypeInfo>
);

seam_core::seam!(
    /// `exprType(node)` (nodeFuncs.c) — the result type Oid of an expression
    /// node. Used by `get_fn_expr_rettype`. Pure read; returns `InvalidOid` for
    /// an unhandled node kind, as C falls through.
    pub fn expr_type(expr: ExternalFnExpr) -> Oid
);

seam_core::seam!(
    /// `get_call_expr_argtype(expr, argnum)` (fmgr.c) — the declared type of the
    /// `argnum`'th argument of a call expression (the `IsA` dispatch over
    /// `FuncExpr`/`OpExpr`/`DistinctExpr`/`ScalarArrayOpExpr`/`NullIfExpr`/
    /// `WindowFunc`, `exprType(list_nth(args, argnum))` with range guard and the
    /// `ScalarArrayOpExpr` element-type hack). Returns `InvalidOid` out of range.
    pub fn call_expr_argtype(expr: ExternalFnExpr, argnum: i32) -> Oid
);

seam_core::seam!(
    /// `get_call_expr_arg_stable(expr, argnum)` (fmgr.c) — true iff the indexed
    /// argument is a `Const` or an external `Param` (the same `IsA` dispatch).
    pub fn call_expr_arg_stable(expr: ExternalFnExpr, argnum: i32) -> bool
);

seam_core::seam!(
    /// `get_fn_expr_variadic` body: `IsA(expr, FuncExpr) ?
    /// ((FuncExpr *) expr)->funcvariadic : false` (fmgr.c).
    pub fn expr_variadic(expr: ExternalFnExpr) -> bool
);
