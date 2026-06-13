//! Seam declarations for the `backend-nodes-nodeFuncs` unit
//! (`nodes/nodeFuncs.c`): expression-node introspection over the planner
//! expression tree.
//!
//! `FmgrInfo.fn_expr` points at a planner expression node
//! (`FuncExpr`/`OpExpr`/`Const`/…). The unified expression-node tree is not
//! ported, so the `get_call_expr_*` accessors that read its payload route
//! through these seams against the opaque [`ExternalFnExpr`] carrier. The owning
//! unit installs them when it lands; until then a call panics loudly.
//!
//! All four are infallible in C (pure tree reads, no `ereport`), so the seams
//! return bare values.

#![allow(non_snake_case)]

use types_core::Oid;
use types_fmgr::ExternalFnExpr;

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
