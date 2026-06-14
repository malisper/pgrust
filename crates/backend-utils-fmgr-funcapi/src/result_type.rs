//! Result-type / tuple-descriptor resolution — `funcapi.c` lines 276–588.
//!
//! Given a function's call info, expression node, or OID, determine the kind of
//! datatype it returns and (for composite results) the result `TupleDesc`.
//! `internal_get_result_type` is the workhorse the public entrypoints funnel
//! into.

use mcx::Mcx;
use types_core::primitive::InvalidOid;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERROR};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::{ResolvedResultType, TypeFuncClass};
use types_nodes::nodes::Node;
use types_nodes::funcapi::ReturnSetInfo;
use types_tuple::heaptuple::{TupleDesc, RECORDOID};

use backend_utils_error::ereport;

use crate::polymorphic::{get_type_func_class, resolve_polymorphic_tupdesc};
use crate::proc_info::build_function_result_tupdesc_t;

/* ----------------------------------------------------------------
 *      IsPolymorphicType / polymorphic pseudo-type OIDs (pg_type.h)
 * ---------------------------------------------------------------- */

/// `ANYELEMENTOID` (pg_type.h).
const ANYELEMENTOID: Oid = 2283;
/// `ANYARRAYOID` (pg_type.h).
const ANYARRAYOID: Oid = 2277;
/// `ANYNONARRAYOID` (pg_type.h).
const ANYNONARRAYOID: Oid = 2776;
/// `ANYENUMOID` (pg_type.h).
const ANYENUMOID: Oid = 3500;
/// `ANYRANGEOID` (pg_type.h).
const ANYRANGEOID: Oid = 3831;
/// `ANYMULTIRANGEOID` (pg_type.h).
const ANYMULTIRANGEOID: Oid = 4537;
/// `ANYCOMPATIBLEOID` (pg_type.h).
const ANYCOMPATIBLEOID: Oid = 5077;
/// `ANYCOMPATIBLEARRAYOID` (pg_type.h).
const ANYCOMPATIBLEARRAYOID: Oid = 5078;
/// `ANYCOMPATIBLENONARRAYOID` (pg_type.h).
const ANYCOMPATIBLENONARRAYOID: Oid = 5079;
/// `ANYCOMPATIBLERANGEOID` (pg_type.h).
const ANYCOMPATIBLERANGEOID: Oid = 5080;
/// `ANYCOMPATIBLEMULTIRANGEOID` (pg_type.h).
const ANYCOMPATIBLEMULTIRANGEOID: Oid = 4538;

/// `IsPolymorphicTypeFamily1(typid)` (pg_type.h) — the `anyelement`/`anyarray`/
/// `anynonarray`/`anyenum`/`anyrange`/`anymultirange` family.
fn is_polymorphic_type_family1(typid: Oid) -> bool {
    typid == ANYELEMENTOID
        || typid == ANYARRAYOID
        || typid == ANYNONARRAYOID
        || typid == ANYENUMOID
        || typid == ANYRANGEOID
        || typid == ANYMULTIRANGEOID
}

/// `IsPolymorphicTypeFamily2(typid)` (pg_type.h) — the `anycompatible*` family.
fn is_polymorphic_type_family2(typid: Oid) -> bool {
    typid == ANYCOMPATIBLEOID
        || typid == ANYCOMPATIBLEARRAYOID
        || typid == ANYCOMPATIBLENONARRAYOID
        || typid == ANYCOMPATIBLERANGEOID
        || typid == ANYCOMPATIBLEMULTIRANGEOID
}

/// `IsPolymorphicType(typid)` (pg_type.h) — true for any polymorphic
/// pseudo-type (`IsPolymorphicTypeFamily1 || IsPolymorphicTypeFamily2`).
fn is_polymorphic_type(typid: Oid) -> bool {
    is_polymorphic_type_family1(typid) || is_polymorphic_type_family2(typid)
}

/// `get_call_result_type(fcinfo, resultTypeId, resultTupleDesc)`
/// (funcapi.c:276) — classify the result type of the function described by a
/// call info record. Delegates to [`internal_get_result_type`] with the
/// function OID and call expression pulled from `fcinfo->flinfo`.
pub fn get_call_result_type<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx FunctionCallInfoBaseData<'mcx>,
) -> PgResult<ResolvedResultType<'mcx>> {
    // C: internal_get_result_type(fcinfo->flinfo->fn_oid,
    //                             fcinfo->flinfo->fn_expr,
    //                             (ReturnSetInfo *) fcinfo->resultinfo, ...);
    //
    // `fn_oid`/`fn_expr` live on the fmgr `FmgrInfo` frame the fmgr owner
    // widens (this trimmed `FunctionCallInfoBaseData` has no `flinfo`), so the
    // read is seamed. `resultinfo` is held inline on the owned frame; C casts
    // the `fmNodePtr` to `ReturnSetInfo *` unconditionally here (the caller
    // contract guarantees a ReturnSetInfo for an SRF call).
    let (fn_oid, fn_expr) = backend_utils_fmgr_fmgr_seams::fn_oid_and_expr::call(fcinfo);
    let rsinfo = fcinfo.resultinfo.as_ref();
    internal_get_result_type(mcx, fn_oid, fn_expr, rsinfo)
}

/// `get_expr_result_type(expr, resultTypeId, resultTupleDesc)`
/// (funcapi.c:299) — classify the result type of an arbitrary expression node;
/// for a `FuncExpr`/`OpExpr` it routes to [`internal_get_result_type`],
/// otherwise it classifies the bare expression type.
pub fn get_expr_result_type<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Option<&Node<'mcx>>,
) -> PgResult<ResolvedResultType<'mcx>> {
    // C dispatches on the expression node's tag:
    //   IsA(expr, FuncExpr) -> internal_get_result_type(funcid, expr, NULL, ...)
    //   IsA(expr, OpExpr)   -> internal_get_result_type(get_opcode(opno), expr, NULL, ...)
    //   IsA(expr, RowExpr) && row_typeid == RECORDOID -> build tupdesc from RowExpr
    //   IsA(expr, Const) && consttype == RECORDOID && !constisnull -> RECORD Const
    //   else (generic) -> exprType(expr) + get_type_func_class + lookup_rowtype_tupdesc_copy
    //
    // The `funcid`/`opno` extraction and the RowExpr/Const/generic arms read the
    // expression-node tree (`FuncExpr.funcid`, `OpExpr.opno`, `RowExpr.args`,
    // the RECORD `Const` datum, `exprType`), which is owned by the
    // nodeFuncs/parser side and not reachable from the funcapi unit. The
    // FuncExpr/OpExpr arms then fold back into `internal_get_result_type`; that
    // funnel plus all the other arms is resolved together behind the nodeFuncs
    // owner seam, which calls back into `internal_get_result_type` for the
    // function-call arms (loud panic until nodeFuncs lands).
    backend_nodes_core_seams::get_expr_result_type_node::call(mcx, expr)
}

/// `get_func_result_type(functionId, resultTypeId, resultTupleDesc)`
/// (funcapi.c:410) — classify the result type of a function given only its OID
/// (no call expression, so polymorphics cannot be resolved).
pub fn get_func_result_type<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
) -> PgResult<ResolvedResultType<'mcx>> {
    // C: internal_get_result_type(functionId, NULL, NULL, ...);
    internal_get_result_type(mcx, function_id, None, None)
}

/// `internal_get_result_type(funcid, call_expr, rsinfo, resultTypeId,
/// resultTupleDesc)` (funcapi.c:430) — the workhorse: fetch the `pg_proc` row,
/// classify `prorettype`, build/resolve the result `TupleDesc` (incl.
/// polymorphic resolution against `call_expr` and the caller's
/// `rsinfo->expectedDesc`), and report the `TypeFuncClass`.
pub fn internal_get_result_type<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
    call_expr: Option<&Node<'mcx>>,
    rsinfo: Option<&ReturnSetInfo<'mcx>>,
) -> PgResult<ResolvedResultType<'mcx>> {
    let mut out = ResolvedResultType::default();

    // C: tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    //    if (!HeapTupleIsValid(tp)) elog(ERROR, "cache lookup failed for function %u", funcid);
    //    procform = (Form_pg_proc) GETSTRUCT(tp);
    let procform = backend_utils_cache_syscache_seams::lookup_proc_result_info::call(mcx, funcid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for function {funcid}")))?;

    // C: rettype = procform->prorettype;
    let mut rettype = procform.prorettype;

    // C: tupdesc = build_function_result_tupdesc_t(tp);
    //    (this funcapi unit re-fetches the pg_proc row by OID rather than
    //     threading the HeapTuple, per the scaffold's by-OID convention.)
    let mut tupdesc: TupleDesc<'mcx> = build_function_result_tupdesc_t(mcx, funcid)?;

    // C: if (tupdesc) { ... }  — has OUT parameters.
    if tupdesc.is_some() {
        // It has OUT parameters, so it's basically like a regular composite
        // type, except we have to be able to resolve any polymorphic OUT
        // parameters.
        //
        // C sets *resultTypeId = rettype here, BEFORE resolution.
        out.result_type_id = Some(rettype);

        // C: if (resolve_polymorphic_tupdesc(tupdesc, &procform->proargtypes, call_expr))
        if resolve_polymorphic_tupdesc(&mut tupdesc, &procform.proargtypes, call_expr)? {
            // C: if (tupdesc->tdtypeid == RECORDOID && tupdesc->tdtypmod < 0)
            //        assign_record_type_typmod(tupdesc);
            let td = tupdesc
                .as_mut()
                .expect("resolve_polymorphic_tupdesc leaves the descriptor in place");
            if td.tdtypeid == RECORDOID && td.tdtypmod < 0 {
                backend_utils_cache_typcache_seams::assign_record_type_typmod::call(&mut **td)?;
            }
            // C: if (resultTupleDesc) *resultTupleDesc = tupdesc;
            out.result_tuple_desc = tupdesc;
            out.class = Some(TypeFuncClass::Composite);
        } else {
            // C: *resultTupleDesc = NULL; result = TYPEFUNC_RECORD;
            out.result_tuple_desc = None;
            out.class = Some(TypeFuncClass::Record);
        }

        // C: ReleaseSysCache(tp); return result;
        return Ok(out);
    }

    // C: if (IsPolymorphicType(rettype)) { ... }
    if is_polymorphic_type(rettype) {
        // C: Oid newrettype = exprType(call_expr);
        let newrettype = backend_nodes_nodeFuncs_seams::expr_type::call(node_to_external(call_expr));

        // C: if (newrettype == InvalidOid) ereport(ERROR, DATATYPE_MISMATCH, ...);
        if newrettype == InvalidOid {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "could not determine actual result type for function \"{}\" declared to return type {}",
                    procform.proname.as_str(),
                    backend_utils_adt_format_type_seams::format_type_be_str::call(rettype)?
                ))
                .into_error());
        }
        rettype = newrettype;
    }

    // C: if (resultTypeId) *resultTypeId = rettype;
    //    if (resultTupleDesc) *resultTupleDesc = NULL; /* default result */
    out.result_type_id = Some(rettype);
    out.result_tuple_desc = None;

    // C: result = get_type_func_class(rettype, &base_rettype);
    let (mut result, base_rettype) = get_type_func_class(rettype)?;

    // C: switch (result) { ... }
    match result {
        TypeFuncClass::Composite | TypeFuncClass::CompositeDomain => {
            // C: *resultTupleDesc = lookup_rowtype_tupdesc_copy(base_rettype, -1);
            // (lookup_rowtype_tupdesc already copies out of the typcache.)
            let td =
                backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(mcx, base_rettype, -1)?;
            out.result_tuple_desc = Some(td);
            // Named composite types can't have any polymorphic columns.
        }
        TypeFuncClass::Scalar => {}
        TypeFuncClass::Record => {
            // C: We must get the tupledesc from call context.
            //    if (rsinfo && IsA(rsinfo, ReturnSetInfo) && rsinfo->expectedDesc != NULL)
            if let Some(rsinfo) = rsinfo {
                if let Some(expected) = rsinfo.expectedDesc.as_ref() {
                    result = TypeFuncClass::Composite;
                    // C: *resultTupleDesc = rsinfo->expectedDesc;
                    // The owned frame holds expectedDesc inline; copy it into the
                    // caller's Mcx to hand back an owned descriptor (C aliases the
                    // caller's pointer, which an owned value cannot express).
                    out.result_tuple_desc = Some(mcx::alloc_in(mcx, expected.clone_in(mcx)?)?);
                    // Assume no polymorphic columns here, either.
                }
            }
        }
        TypeFuncClass::Other => {}
    }

    out.class = Some(result);

    // C: ReleaseSysCache(tp); return result;
    Ok(out)
}

/// Adapt the scaffold's plan-`Node` call-expression carrier to the
/// `ExternalFnExpr` tag the nodeFuncs `expr_type` seam consumes. `None` is the
/// C `NULL` call_expr, for which `exprType(NULL)` yields `InvalidOid`.
fn node_to_external(call_expr: Option<&Node<'_>>) -> types_fmgr::ExternalFnExpr {
    types_fmgr::ExternalFnExpr {
        tag: match call_expr {
            Some(node) => node.tag().0,
            None => 0,
        },
    }
}

/// `get_expr_result_tupdesc(expr, noError)` (funcapi.c:551) — convenience
/// wrapper over [`get_expr_result_type`] that returns just the result
/// `TupleDesc` for a composite-returning expression, or (when `no_error`)
/// `None`/error for a non-composite.
pub fn get_expr_result_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Option<&Node<'mcx>>,
    no_error: bool,
) -> PgResult<TupleDesc<'mcx>> {
    // C: functypclass = get_expr_result_type(expr, NULL, &tupleDesc);
    let resolved = get_expr_result_type(mcx, expr)?;

    // C: if (functypclass == TYPEFUNC_COMPOSITE || functypclass == TYPEFUNC_COMPOSITE_DOMAIN)
    //        return tupleDesc;
    if matches!(
        resolved.class,
        Some(TypeFuncClass::Composite) | Some(TypeFuncClass::CompositeDomain)
    ) {
        return Ok(resolved.result_tuple_desc);
    }

    // C: if (!noError) { ... ereport(ERROR, ...) }
    if !no_error {
        // C: Oid exprTypeId = exprType(expr);
        let expr_type_id = backend_nodes_nodeFuncs_seams::expr_type::call(node_to_external(expr));

        if expr_type_id != RECORDOID {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!(
                    "type {} is not composite",
                    backend_utils_adt_format_type_seams::format_type_be_str::call(expr_type_id)?
                ))
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg("record type has not been registered".to_string())
                .into_error());
        }
    }

    // C: return NULL;
    Ok(None)
}
