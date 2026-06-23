//! Result-type / tuple-descriptor resolution — `funcapi.c` lines 276–588.
//!
//! Given a function's call info, expression node, or OID, determine the kind of
//! datatype it returns and (for composite results) the result `TupleDesc`.
//! `internal_get_result_type` is the workhorse the public entrypoints funnel
//! into.

use ::mcx::Mcx;
use ::types_core::primitive::{AttrNumber, InvalidOid};
use ::types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERROR};
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::{ResolvedResultType, TypeFuncClass};
use ::nodes::nodes::Node;
use ::nodes::funcapi::ReturnSetInfo;
use ::types_tuple::heaptuple::{TupleDesc, RECORDOID};

use ::utils_error::ereport;

use crate::polymorphic::{get_type_func_class, resolve_polymorphic_tupdesc, CallExpr};
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
    // The call expression `internal_get_result_type` needs to resolve a
    // polymorphic result type is `fcinfo->flinfo->fn_expr` — the owned `Expr`
    // `fmgr_info_set_expr` stamped, recovered here as the erased carrier (a
    // plan-tree `&Node` cannot model `FuncExpr`/`OpExpr`). C reads it as a bare
    // `Node *`; the owned model carries the field-bearing `Expr` through the
    // `CallExpr` abstraction.
    let (fn_oid, fn_expr_erased) =
        fmgr_seams::fn_oid_and_fn_expr_erased::call(fcinfo);
    let call_expr = fn_expr_erased.map(CallExpr::from_erased);
    let rsinfo = fcinfo.resultinfo.as_ref();
    internal_get_result_type(mcx, fn_oid, call_expr.as_ref(), rsinfo)
}

/// `get_expr_result_type(expr, resultTypeId, resultTupleDesc)`
/// (funcapi.c:299) — classify the result type of an arbitrary expression node;
/// for a `FuncExpr`/`OpExpr` it routes to [`internal_get_result_type`],
/// otherwise it classifies the bare expression type.
pub fn get_expr_result_type<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Option<&Node<'mcx>>,
) -> PgResult<ResolvedResultType<'mcx>> {
    use ::nodes::primnodes::Expr;

    // C dispatches on the expression node's tag:
    //   IsA(expr, FuncExpr) -> internal_get_result_type(funcid, expr, NULL, ...)
    //   IsA(expr, OpExpr)   -> internal_get_result_type(get_opcode(opno), expr, NULL, ...)
    //   IsA(expr, RowExpr) && row_typeid == RECORDOID -> build tupdesc from RowExpr
    //   IsA(expr, Const) && consttype == RECORDOID && !constisnull -> RECORD Const
    //   else (generic) -> exprType(expr) + get_type_func_class + lookup_rowtype_tupdesc_copy

    // C: if (expr && IsA(expr, FuncExpr))
    //        result = internal_get_result_type(((FuncExpr *) expr)->funcid, expr, NULL, ...)
    if let Some(fe) = expr.and_then(|n| n.as_funcexpr()) {
        let call_expr = CallExpr::from_node(mcx, expr.unwrap())?;
        return internal_get_result_type(mcx, fe.funcid, Some(&call_expr), None);
    }

    // C: else if (expr && IsA(expr, OpExpr))
    //        result = internal_get_result_type(get_opcode(((OpExpr *) expr)->opno), expr, NULL, ...)
    if let Some(op) = expr.and_then(|n| n.as_opexpr()) {
        let funcid = lsyscache_seams::get_opcode::call(op.opno)?;
        let call_expr = CallExpr::from_node(mcx, expr.unwrap())?;
        return internal_get_result_type(mcx, funcid, Some(&call_expr), None);
    }

    // C: else if (expr && IsA(expr, RowExpr) && ((RowExpr *) expr)->row_typeid == RECORDOID)
    //        /* We can resolve the record type by generating the tupdesc directly */
    if let Some(Expr::RowExpr(rexpr)) = expr.and_then(|n| n.as_expr()) {
        if rexpr.row_typeid == RECORDOID {
            let mut out = ResolvedResultType::default();
            // tupdesc = CreateTemplateTupleDesc(list_length(rexpr->args));
            let mut tupdesc =
                tupdesc::CreateTemplateTupleDesc(mcx, rexpr.args.len() as i32)?;
            // Assert(list_length(rexpr->args) == list_length(rexpr->colnames));
            debug_assert!(rexpr.args.len() == rexpr.colnames.len());
            // forboth(lcc, rexpr->args, lcn, rexpr->colnames) { ... }
            let mut i: AttrNumber = 1;
            for (col, colname) in rexpr.args.iter().zip(rexpr.colnames.iter()) {
                let info = nodeFuncs_seams::expr_type_info::call(col)?;
                // TupleDescInitEntry(tupdesc, i, colname, exprType(col), exprTypmod(col), 0);
                tupdesc::TupleDescInitEntry(
                    &mut tupdesc,
                    i,
                    Some(colname.as_str()),
                    info.typid,
                    info.typmod,
                    0,
                )?;
                // TupleDescInitEntryCollation(tupdesc, i, exprCollation(col));
                tupdesc::TupleDescInitEntryCollation(
                    &mut tupdesc,
                    i,
                    info.collation,
                )?;
                i += 1;
            }
            // if (resultTypeId) *resultTypeId = rexpr->row_typeid;
            out.result_type_id = Some(rexpr.row_typeid);
            // if (resultTupleDesc) *resultTupleDesc = BlessTupleDesc(tupdesc);
            let td = Some(::mcx::alloc_in(mcx, tupdesc)?);
            out.result_tuple_desc =
                execTuples_seams::bless_tuple_desc::call(mcx, td)?;
            out.class = Some(TypeFuncClass::Composite);
            return Ok(out);
        }
    }

    // C: else if (expr && IsA(expr, Const) && ((Const *) expr)->consttype == RECORDOID
    //             && !((Const *) expr)->constisnull)
    //        /* resolve field names of a RECORD-type Const from its datum's typmod */
    if let Some(c) = expr.and_then(|n| n.as_const()) {
        if c.consttype == RECORDOID && !c.constisnull {
            // When EXPLAIN'ing some queries with SEARCH/CYCLE clauses, we may
            // need to resolve field names of a RECORD-type Const. The datum
            // should contain a typmod that will tell us that.
            //
            // C: rec = DatumGetHeapTupleHeader(((Const *) expr)->constvalue);
            //    tupType = HeapTupleHeaderGetTypeId(rec);
            //    tupTypmod = HeapTupleHeaderGetTypMod(rec);
            //
            // The composite Datum carries the rowtype header either as a live
            // `FormedTuple` (Datum::Composite) or as a flat varlena image
            // (Datum::ByRef); both decode the same DatumTupleFields header
            // (datum_typeid / datum_typmod) that C reads off the
            // HeapTupleHeader.
            use types_tuple::{
                FormedTuple, HeapTupleHeaderGetTypMod, HeapTupleHeaderGetTypeId,
            };
            let formed: FormedTuple<'mcx> = match &c.constvalue {
                ::types_tuple::Datum::Composite(t) => t.clone_in(mcx)?,
                d => FormedTuple::from_datum_image(mcx, d.as_ref_bytes())?,
            };
            let header = formed
                .tuple
                .t_data
                .as_ref()
                .expect("RECORD Const: composite Datum has no header");
            let tup_type = HeapTupleHeaderGetTypeId(header);
            let tup_typmod = HeapTupleHeaderGetTypMod(header);

            let mut out = ResolvedResultType::default();
            // if (resultTypeId) *resultTypeId = tupType;
            out.result_type_id = Some(tup_type);
            if tup_type != RECORDOID || tup_typmod >= 0 {
                // Should be able to look it up.
                // *resultTupleDesc = lookup_rowtype_tupdesc_copy(tupType, tupTypmod);
                let td = typcache_seams::lookup_rowtype_tupdesc_copy::call(
                    mcx, tup_type, tup_typmod,
                )?;
                out.result_tuple_desc = Some(td);
                out.class = Some(TypeFuncClass::Composite);
            } else {
                // This shouldn't really happen ...
                out.result_tuple_desc = None;
                out.class = Some(TypeFuncClass::Record);
            }
            return Ok(out);
        }
    }

    // C: else { /* handle as a generic expression; no chance to resolve RECORD */ }
    let mut out = ResolvedResultType::default();
    // Oid typid = exprType(expr);
    let typid = match expr {
        Some(n) if n.as_expr().is_some() => {
            nodeFuncs_seams::expr_type_info::call(n.as_expr().unwrap())?.typid
        }
        // exprType(NULL) yields InvalidOid; a non-Expr Node is not a valid
        // expression here (C casts to (Node *) and exprType would elog), but the
        // function-in-FROM callers always pass an Expr.
        _ => nodeFuncs_seams::expr_type::call(node_to_external(expr)),
    };
    // if (resultTypeId) *resultTypeId = typid;
    out.result_type_id = Some(typid);
    // if (resultTupleDesc) *resultTupleDesc = NULL;
    out.result_tuple_desc = None;
    // result = get_type_func_class(typid, &base_typid);
    let (result, base_typid) = get_type_func_class(typid)?;
    // if ((result == TYPEFUNC_COMPOSITE || result == TYPEFUNC_COMPOSITE_DOMAIN) && resultTupleDesc)
    //     *resultTupleDesc = lookup_rowtype_tupdesc_copy(base_typid, -1);
    if matches!(
        result,
        TypeFuncClass::Composite | TypeFuncClass::CompositeDomain
    ) {
        let td =
            typcache_seams::lookup_rowtype_tupdesc_copy::call(mcx, base_typid, -1)?;
        out.result_tuple_desc = Some(td);
    }
    out.class = Some(result);
    Ok(out)
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
    call_expr: Option<&CallExpr>,
    rsinfo: Option<&ReturnSetInfo<'mcx>>,
) -> PgResult<ResolvedResultType<'mcx>> {
    let mut out = ResolvedResultType::default();

    // C: tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    //    if (!HeapTupleIsValid(tp)) elog(ERROR, "cache lookup failed for function %u", funcid);
    //    procform = (Form_pg_proc) GETSTRUCT(tp);
    let procform = syscache_seams::lookup_proc_result_info::call(mcx, funcid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for function {funcid}")))?;

    // C: rettype = procform->prorettype;
    let mut rettype = procform.prorettype;

    // C: tupdesc = build_function_result_tupdesc_t(tp);
    //    (this funcapi unit re-fetches the pg_proc row by OID rather than
    //     threading the HeapTuple, per this crate's by-OID convention.)
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
                typcache_seams::assign_record_type_typmod::call(&mut **td)?;
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
        // The polymorphic return type is resolved from the concrete call
        // expression: `exprType` reads `FuncExpr.funcresulttype` (the parser
        // already stamped the resolved element type there). `CallExpr` carries
        // the field-bearing call node (erased owned `Expr`) and answers
        // `exprType`; `None` call_expr yields `InvalidOid` (C `exprType(NULL)`).
        let newrettype = match call_expr {
            Some(ce) => ce.result_type(),
            None => InvalidOid,
        };

        // C: if (newrettype == InvalidOid) ereport(ERROR, DATATYPE_MISMATCH, ...);
        if newrettype == InvalidOid {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "could not determine actual result type for function \"{}\" declared to return type {}",
                    procform.proname.as_str(),
                    format_type_seams::format_type_be_str::call(rettype)?
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
                typcache_seams::lookup_rowtype_tupdesc::call(mcx, base_rettype, -1)?;
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
                    out.result_tuple_desc = Some(::mcx::alloc_in(mcx, expected.clone_in(mcx)?)?);
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

/// Adapt the plan-`Node` call-expression carrier to the
/// `ExternalFnExpr` tag the nodeFuncs `expr_type` seam consumes. `None` is the
/// C `NULL` call_expr, for which `exprType(NULL)` yields `InvalidOid`.
fn node_to_external(call_expr: Option<&Node<'_>>) -> fmgr::ExternalFnExpr {
    fmgr::ExternalFnExpr {
        tag: match call_expr {
            Some(node) => node.tag().0,
            None => 0,
        },
        // This adapter only has the plan `Node` (a tag), not the field-bearing
        // owned `Expr`, so no erased node is available — the accessors fall
        // through to `InvalidOid` (the tag-only contract).
        node: None,
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
        let expr_type_id = nodeFuncs_seams::expr_type::call(node_to_external(expr));

        if expr_type_id != RECORDOID {
            return Err(ereport(ERROR)
                .errcode(::types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!(
                    "type {} is not composite",
                    format_type_seams::format_type_be_str::call(expr_type_id)?
                ))
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errcode(::types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg("record type has not been registered".to_string())
                .into_error());
        }
    }

    // C: return NULL;
    Ok(None)
}
