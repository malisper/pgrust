//! Result-type / tuple-descriptor resolution — `funcapi.c` lines 276–588.
//!
//! Given a function's call info, expression node, or OID, determine the kind of
//! datatype it returns and (for composite results) the result `TupleDesc`.
//! `internal_get_result_type` is the workhorse the public entrypoints funnel
//! into.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::ResolvedResultType;
use types_nodes::nodes::Node;
use types_nodes::funcapi::ReturnSetInfo;
use types_tuple::heaptuple::TupleDesc;

/// `get_call_result_type(fcinfo, resultTypeId, resultTupleDesc)`
/// (funcapi.c:276) — classify the result type of the function described by a
/// call info record. Delegates to [`internal_get_result_type`] with the
/// function OID and call expression pulled from `fcinfo->flinfo`.
pub fn get_call_result_type<'mcx>(
    _mcx: Mcx<'mcx>,
    _fcinfo: &FunctionCallInfoBaseData<'mcx>,
) -> PgResult<ResolvedResultType<'mcx>> {
    todo!("funcapi.c:276 get_call_result_type")
}

/// `get_expr_result_type(expr, resultTypeId, resultTupleDesc)`
/// (funcapi.c:299) — classify the result type of an arbitrary expression node;
/// for a `FuncExpr`/`OpExpr` it routes to [`internal_get_result_type`],
/// otherwise it classifies the bare expression type.
pub fn get_expr_result_type<'mcx>(
    _mcx: Mcx<'mcx>,
    _expr: Option<&Node<'mcx>>,
) -> PgResult<ResolvedResultType<'mcx>> {
    todo!("funcapi.c:299 get_expr_result_type")
}

/// `get_func_result_type(functionId, resultTypeId, resultTupleDesc)`
/// (funcapi.c:410) — classify the result type of a function given only its OID
/// (no call expression, so polymorphics cannot be resolved).
pub fn get_func_result_type<'mcx>(
    _mcx: Mcx<'mcx>,
    _function_id: Oid,
) -> PgResult<ResolvedResultType<'mcx>> {
    todo!("funcapi.c:410 get_func_result_type")
}

/// `internal_get_result_type(funcid, call_expr, rsinfo, resultTypeId,
/// resultTupleDesc)` (funcapi.c:430) — the workhorse: fetch the `pg_proc` row,
/// classify `prorettype`, build/resolve the result `TupleDesc` (incl.
/// polymorphic resolution against `call_expr` and the caller's
/// `rsinfo->expectedDesc`), and report the `TypeFuncClass`.
pub fn internal_get_result_type<'mcx>(
    _mcx: Mcx<'mcx>,
    _funcid: Oid,
    _call_expr: Option<&Node<'mcx>>,
    _rsinfo: Option<&ReturnSetInfo<'mcx>>,
) -> PgResult<ResolvedResultType<'mcx>> {
    todo!("funcapi.c:430 internal_get_result_type")
}

/// `get_expr_result_tupdesc(expr, noError)` (funcapi.c:551) — convenience
/// wrapper over [`get_expr_result_type`] that returns just the result
/// `TupleDesc` for a composite-returning expression, or (when `no_error`)
/// `None`/error for a non-composite.
pub fn get_expr_result_tupdesc<'mcx>(
    _mcx: Mcx<'mcx>,
    _expr: Option<&Node<'mcx>>,
    _no_error: bool,
) -> PgResult<TupleDesc<'mcx>> {
    todo!("funcapi.c:551 get_expr_result_tupdesc")
}
