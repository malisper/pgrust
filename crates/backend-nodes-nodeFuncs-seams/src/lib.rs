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
    /// `CallStmtResultDesc(stmt)` (functioncmds.c:2383) — the polymorphic
    /// output-argument tuple descriptor for a CALL. Re-homed here from
    /// `backend-commands-functioncmds-seams`: the function is keyed entirely by
    /// the unported planner expression node `stmt->funcexpr` (`FuncExpr.funcid`,
    /// which functioncmds carries opaquely — the layered node tree does not yet
    /// model `FuncExpr`), runs `build_function_result_tupdesc_t` over the
    /// `PROCOID` tuple, and re-types each output column from `stmt->outargs[i]`
    /// via `exprType` (the nodeFuncs expression-node inspection). Both the
    /// `funcid` read and the `exprType` fixup are nodeFuncs/nodes-core
    /// expression-tree territory, so the whole body lands with that owner.
    /// Allocating seam: takes `Mcx<'mcx>`, returns the descriptor in the
    /// caller's context (empty/NULL when the procedure has no out-args,
    /// mirroring the C NULL tupdesc). Fallible on the cache-lookup
    /// `ereport(ERROR)`.
    pub fn call_stmt_result_desc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: types_parsenodes::CallStmt,
    ) -> types_error::PgResult<types_tuple::TupleDesc<'mcx>>
);

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

seam_core::seam!(
    /// The non-`FuncExpr`/`OpExpr` arms of `get_expr_result_type` (funcapi.c):
    /// the `IsA` dispatch over `RowExpr`/`Const`/generic expression that
    /// inspects the expression node's tag and per-variant fields (`row_typeid`,
    /// `args`/`colnames`, the RECORD `Const` datum) and runs `exprType` /
    /// `CreateTemplateTupleDesc` / `BlessTupleDesc` / `lookup_rowtype_tupdesc_copy`
    /// / `get_type_func_class` over them. The expression-node tree is owned by
    /// the nodeFuncs/parser side; the funcapi unit cannot read its fields, so the
    /// arm is seamed here. `expr == None` is the C `NULL` (generic `exprType`
    /// path on a NULL node, i.e. `InvalidOid` / `TYPEFUNC_OTHER`). `Err` carries
    /// the lookup/`assign_record_type_typmod` `ereport(ERROR)` surface.
    pub fn get_expr_result_type_node<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        expr: Option<&types_nodes::nodes::Node<'mcx>>,
    ) -> types_error::PgResult<types_nodes::funcapi::ResolvedResultType<'mcx>>
);

seam_core::seam!(
    /// `get_call_expr_argtype(call_expr, argnum)` (fmgr.c:1929) keyed by the
    /// unified plan/expression `Node` the funcapi result-type cluster threads as
    /// its `call_expr` (`resolve_polymorphic_tupdesc` / `_argtypes`). The
    /// argument-bearing expression nodes (`FuncExpr`/`OpExpr`/`DistinctExpr`/
    /// `ScalarArrayOpExpr`/`NullIfExpr`/`WindowFunc`) are not yet modelled by the
    /// plan-tree `Node` enum, so this stays nodeFuncs-owned: the `IsA` dispatch,
    /// `exprType(list_nth(args, argnum))` with the range guard, and the
    /// `ScalarArrayOpExpr` element-type hack all live in nodeFuncs. Returns
    /// `InvalidOid` out of range / for an unhandled kind, as C falls through.
    pub fn get_call_expr_argtype_node<'mcx>(
        call_expr: &types_nodes::nodes::Node<'mcx>,
        argnum: i32,
    ) -> Oid
);

seam_core::seam!(
    /// `exprInputCollation(node)` (nodeFuncs.c) keyed by the unified plan/
    /// expression `Node` the funcapi cluster threads as its `call_expr`. Reads
    /// the input collation a function call uses (the `FuncExpr.inputcollid` /
    /// `OpExpr.inputcollid` / … family); a pure node inspection. The expression
    /// nodes are not yet modelled by the plan-tree `Node` enum, so this stays
    /// nodeFuncs-owned. Returns `InvalidOid` for an unhandled node kind.
    pub fn expr_input_collation_node<'mcx>(
        node: &types_nodes::nodes::Node<'mcx>,
    ) -> Oid
);
