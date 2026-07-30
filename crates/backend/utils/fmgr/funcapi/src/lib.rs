#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use fmgr::FmgrInfo;
use mcx::{vec_with_capacity_in, Mcx, PgString, PgVec};
use nodes::node_tree::Node;
use nodes::NodeTag;
use syscache_seams::PgProcShape;
use types_core::{AttrNumber, InvalidOid, Oid, RECORDOID, VOIDOID};
use types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_WRONG_OBJECT_TYPE};
use types_tuple::TupleDescData;

#[cfg(test)]
mod tests;

mod srf_mat;

pub use funcapi_srf::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall, srf_return_done, srf_return_next,
    srf_return_next_null, FuncCallContext,
};
pub use srf_mat::{InitMaterializedSRF, MaterializedSRF, MAT_SRF_BLESS, MAT_SRF_USE_EXPECTED_DESC};

pub fn init_seams() {
    fmgr_seams::get_fn_expr_variadic::set(seam_get_fn_expr_variadic);
    fmgr_seams::get_fn_expr_argtype::set(seam_get_fn_expr_argtype);
}

// Seam shims for callers below funcapi in the crate graph (varlena's
// concat/format; a direct dep would cycle through funcapi -> varlena).
fn seam_get_fn_expr_variadic(flinfo: &FmgrInfo) -> bool {
    get_fn_expr_variadic(Some(flinfo))
}

fn seam_get_fn_expr_argtype(flinfo: &FmgrInfo, argnum: i16) -> Oid {
    if argnum < 0 {
        return InvalidOid;
    }
    get_fn_expr_argtype(Some(flinfo), argnum as usize)
}

// pg_type.dat
const CSTRINGOID: Oid = 2275;
const ANYELEMENTOID: Oid = 2283;
const ANYARRAYOID: Oid = 2277;
const ANYNONARRAYOID: Oid = 2776;
const ANYENUMOID: Oid = 3500;
const ANYRANGEOID: Oid = 3831;
const ANYMULTIRANGEOID: Oid = 4537;
const ANYCOMPATIBLEOID: Oid = 5077;
const ANYCOMPATIBLEARRAYOID: Oid = 5078;
const ANYCOMPATIBLENONARRAYOID: Oid = 5079;
const ANYCOMPATIBLERANGEOID: Oid = 5080;
const ANYCOMPATIBLEMULTIRANGEOID: Oid = 4538;

// pg_proc.h
pub const PROARGMODE_IN: i8 = b'i' as i8;
pub const PROARGMODE_OUT: i8 = b'o' as i8;
pub const PROARGMODE_INOUT: i8 = b'b' as i8;
pub const PROARGMODE_VARIADIC: i8 = b'v' as i8;
pub const PROARGMODE_TABLE: i8 = b't' as i8;
pub const PROKIND_FUNCTION: i8 = b'f' as i8;
pub const PROKIND_PROCEDURE: i8 = b'p' as i8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypeFuncClass {
    Scalar,
    Composite,
    CompositeDomain,
    Record,
    Other,
}

#[derive(Debug)]
pub struct ResolvedResultType<'mcx> {
    pub class: TypeFuncClass,
    pub result_type_id: Oid,
    pub result_tuple_desc: Option<TupleDescData<'mcx>>,
}

fn is_polymorphic_type(typid: Oid) -> bool {
    matches!(
        typid,
        ANYELEMENTOID
            | ANYARRAYOID
            | ANYNONARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

// C exprType (nodeFuncs.c, unported unit) over the families a call expression
// carries here; exprType(NULL) == InvalidOid.
fn expr_type(expr: Option<Node<'_>>) -> Oid {
    let Some(node) = expr else {
        return InvalidOid;
    };
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().vartype,
        NodeTag::T_Const => node.as_const().unwrap().consttype,
        NodeTag::T_Param => node.as_param().unwrap().paramtype,
        NodeTag::T_SubscriptingRef => node.as_subscripting_ref().unwrap().refrestype,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funcresulttype,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opresulttype,
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggtype,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wintype,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resulttype,
        NodeTag::T_ArrayCoerceExpr => node.as_array_coerce_expr().unwrap().resulttype,
        NodeTag::T_ConvertRowtypeExpr => node.as_convert_rowtype_expr().unwrap().resulttype,
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casetype,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().coalescetype,
        NodeTag::T_RowExpr => node.as_row_expr().unwrap().row_typeid,
        NodeTag::T_ArrayExpr => node.as_array_expr().unwrap().array_typeid,
        NodeTag::T_JsonValueExpr => {
            expr_type(node.as_json_value_expr().unwrap().formatted_expr)
        }
        NodeTag::T_JsonConstructorExpr => {
            node.as_json_constructor_expr().unwrap().returning.expect("returning").typid
        }
        NodeTag::T_JsonIsPredicate => types_core::catalog::BOOLOID,
        NodeTag::T_FieldSelect => node.as_field_select().unwrap().resulttype,
        NodeTag::T_CoerceToDomain => node.as_coerce_to_domain().unwrap().resulttype,
        NodeTag::T_CoerceToDomainValue => node.as_coerce_to_domain_value().unwrap().typeId,
        NodeTag::T_JsonExpr => node.as_json_expr().unwrap().returning.expect("returning").typid,
        NodeTag::T_SQLValueFunction => node.as_sql_value_function().unwrap().r#type,
        tag => panic!("funcapi exprType: node family {tag:?} not ported"),
    }
}

// AggFnArgTypes carriers (agg trans/final flinfos stand in for C's fake
// transfn FuncExpr) have no expression node; consumers needing arg types go
// through get_fn_expr_argtype, which understands the carrier.
pub fn call_expr_node(flinfo: &FmgrInfo) -> Option<Node<'static>> {
    flinfo.fn_expr.as_ref().and_then(|e| e.downcast_ref::<Node<'static>>().copied())
}

/// C: exprType over a call expression node (get_fn_expr_rettype's core).
pub fn get_call_expr_rettype(node: Node<'_>) -> Oid {
    expr_type(Some(node))
}

/// C: get_fn_expr_rettype (fmgr.c); InvalidOid mirrors the NULL-fn_expr case.
/// Aggregate trans/final flinfos carry the fake FuncExpr's funcresulttype in
/// the AggFnArgTypes carrier.
pub fn get_fn_expr_rettype(flinfo: &FmgrInfo) -> Oid {
    if let Some(rettype) = agg_carrier_rettype(flinfo) {
        return rettype;
    }
    match call_expr_node(flinfo) {
        None => InvalidOid,
        Some(n) => expr_type(Some(n)),
    }
}

fn agg_carrier_rettype(flinfo: &FmgrInfo) -> Option<Oid> {
    flinfo
        .fn_expr
        .as_ref()
        .and_then(|e| e.downcast_ref::<types_core::fmgr::AggFnArgTypes>())
        .map(|agg| agg.rettype)
}

/// C: get_fn_expr_argtype (fmgr.c). Aggregate transfns carry an
/// `AggFnArgTypes` vector where C builds a fake transfn FuncExpr.
pub fn get_fn_expr_argtype(flinfo: Option<&FmgrInfo>, argnum: usize) -> Oid {
    let Some(e) = flinfo.and_then(|f| f.fn_expr.as_ref()) else {
        return InvalidOid;
    };
    if let Some(agg) = e.downcast_ref::<types_core::fmgr::AggFnArgTypes>() {
        return agg.argtypes.get(argnum).copied().unwrap_or(InvalidOid);
    }
    let node = *e
        .downcast_ref::<Node<'static>>()
        .expect("get_fn_expr_argtype: fn_expr does not carry a Node");
    get_call_expr_argtype(node, argnum)
}

/// C: get_call_expr_argtype (fmgr.c) over the ported call-expression
/// families; unknown families return InvalidOid exactly as C does.
pub fn get_call_expr_argtype(node: Node<'_>, argnum: usize) -> Oid {
    let args = match node.node_tag() {
        NodeTag::T_FuncExpr => &node.as_func_expr().unwrap().args,
        NodeTag::T_OpExpr => &node.as_op_expr().unwrap().args,
        _ => return InvalidOid,
    };
    if argnum >= args.len() {
        return InvalidOid;
    }
    expr_type(Some(args.nth(argnum)))
}

/// C: get_call_expr_argtype over an erased fn_expr — a real call-expression
/// node, or the AggFnArgTypes carrier standing in for C's constructed
/// aggregate support-fn FuncExpr (build_aggregate_transfn_expr and kin).
pub fn erased_call_expr_argtype(e: &types_core::fmgr::FnExprErased, argnum: usize) -> Oid {
    if let Some(agg) = e.downcast_ref::<types_core::fmgr::AggFnArgTypes>() {
        return agg.argtypes.get(argnum).copied().unwrap_or(InvalidOid);
    }
    e.downcast_ref::<Node<'static>>().map_or(InvalidOid, |n| get_call_expr_argtype(*n, argnum))
}

/// C: get_fn_expr_rettype's core over an erased fn_expr (node or carrier).
pub fn erased_call_expr_rettype(e: &types_core::fmgr::FnExprErased) -> Oid {
    if let Some(agg) = e.downcast_ref::<types_core::fmgr::AggFnArgTypes>() {
        return agg.rettype;
    }
    e.downcast_ref::<Node<'static>>().map_or(InvalidOid, |n| expr_type(Some(*n)))
}

/// C: get_fn_expr_arg_stable (fmgr.c) — Const or external Param arguments.
pub fn get_fn_expr_arg_stable(flinfo: Option<&FmgrInfo>, argnum: usize) -> bool {
    let Some(e) = flinfo.and_then(|f| f.fn_expr.as_ref()) else {
        return false;
    };
    let Some(node) = e.downcast_ref::<Node<'static>>() else {
        return false;
    };
    let args = match node.node_tag() {
        NodeTag::T_FuncExpr => &node.as_func_expr().unwrap().args,
        NodeTag::T_OpExpr => &node.as_op_expr().unwrap().args,
        _ => return false,
    };
    if argnum >= args.len() {
        return false;
    }
    let arg = args.nth(argnum);
    match arg.node_tag() {
        NodeTag::T_Const => true,
        NodeTag::T_Param => {
            arg.as_param().unwrap().paramkind == nodes::primnodes::ParamKind::PARAM_EXTERN
        }
        _ => false,
    }
}

pub struct VariadicArgs<'mcx> {
    pub args: PgVec<'mcx, datum::Datum>,
    pub types: PgVec<'mcx, Oid>,
    pub nulls: PgVec<'mcx, bool>,
}

/// C: extract_variadic_args (funcapi.c). Ok(None) is C's -1 (an explicit
/// VARIADIC NULL array argument).
pub fn extract_variadic_args<'mcx>(
    mcx: Mcx<'mcx>,
    flinfo: Option<&FmgrInfo>,
    fcinfo: &fmgr::FunctionCallInfoBaseData,
    variadic_start: usize,
    convert_unknown: bool,
) -> PgResult<Option<VariadicArgs<'mcx>>> {
    use types_core::catalog::{TEXTOID, UNKNOWNOID};
    if get_fn_expr_variadic(flinfo) {
        debug_assert_eq!(fcinfo.nargs(), variadic_start + 1);
        if fcinfo.argisnull(variadic_start) {
            return Ok(None);
        }
        // SAFETY: checked non-null; a variadic-any last arg is an array datum.
        let p = unsafe { fcinfo.arg_ptr(variadic_start) };
        // SAFETY: a live varlena readable through its full VARSIZE_ANY.
        let raw =
            unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
        let flat: &'mcx [u8] = detoast_seams::detoast_attr::call(mcx, raw)?.leak();
        let element_type = arrayfuncs::arr_elemtype(flat);
        let (typlen, typbyval, typalign) = lsyscache::get_typlenbyvalalign(element_type)?;
        let (args, nulls) = arrayfuncs::deconstruct_array(
            mcx,
            flat,
            typlen as i32,
            typbyval,
            typalign as u8,
            true,
        )?;
        let mut types = vec_with_capacity_in(mcx, args.len())?;
        types.resize(args.len(), element_type);
        return Ok(Some(VariadicArgs { args, types, nulls }));
    }

    let nargs = fcinfo.nargs() - variadic_start;
    debug_assert!(nargs > 0);
    let mut args: PgVec<'mcx, datum::Datum> = vec_with_capacity_in(mcx, nargs)?;
    let mut types: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, nargs)?;
    let mut nulls: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, nargs)?;
    for i in 0..nargs {
        let argno = i + variadic_start;
        let isnull = fcinfo.argisnull(argno);
        let mut typ = get_fn_expr_argtype(flinfo, argno);
        let val;
        // C: unknown-type literals arrive as cstring pointers.
        if convert_unknown && typ == UNKNOWNOID && get_fn_expr_arg_stable(flinfo, argno) {
            typ = TEXTOID;
            val = if isnull {
                datum::Datum::null()
            } else {
                // SAFETY: a non-null unknown literal is a live cstring.
                let s = unsafe { fcinfo.arg_cstring(argno) }.to_bytes();
                fmgr::varlena_result(varlena::cstring_to_text(mcx, s)?)
            };
        } else {
            val = fcinfo.arg(argno);
        }
        if !types_core::OidIsValid(typ) || (convert_unknown && typ == UNKNOWNOID) {
            return Err(Box::new(
                PgError::error(format!("could not determine data type for argument {}", i + 1))
                    .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
            ));
        }
        args.push(val);
        types.push(typ);
        nulls.push(isnull);
    }
    Ok(Some(VariadicArgs { args, types, nulls }))
}

/// C: get_fn_expr_variadic (fmgr.c) — FuncExpr.funcvariadic; false elsewhere.
pub fn get_fn_expr_variadic(flinfo: Option<&FmgrInfo>) -> bool {
    let Some(e) = flinfo.and_then(|f| f.fn_expr.as_ref()) else {
        return false;
    };
    let Some(node) = e.downcast_ref::<Node<'static>>() else {
        return false;
    };
    match node.node_tag() {
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funcvariadic,
        _ => false,
    }
}

#[track_caller]
#[cold]
fn function_lookup_failed(funcid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "cache lookup failed for function {funcid}"
    )))
}

#[track_caller]
#[cold]
fn unresolved_polymorphic_rettype(funcid: Oid, rettype: Oid) -> Box<PgError> {
    let name = syscache_seams::pg_proc_proname::call(funcid)
        .ok()
        .flatten()
        .map(|n| String::from_utf8_lossy(n.name_str()).into_owned())
        .unwrap_or_default();
    let tname = match format_type::format_type_be(rettype) {
        Ok(t) => t,
        Err(e) => return e,
    };
    Box::new(
        PgError::error(format!(
            "could not determine actual result type for function \"{name}\" declared to return type {tname}"
        ))
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
    )
}

// `expected_desc` is C's `rsinfo->expectedDesc`, the one ReturnSetInfo field
// this path reads; None covers both NULL rsinfo and NULL expectedDesc.
pub fn get_call_result_type<'mcx>(
    mcx: Mcx<'mcx>,
    flinfo: &FmgrInfo,
    expected_desc: Option<&TupleDescData<'_>>,
) -> PgResult<ResolvedResultType<'mcx>> {
    // Aggregate trans/final flinfos have no call FuncExpr; the carrier's
    // rettype stands in for C's fake-FuncExpr funcresulttype.
    let carrier_rettype = agg_carrier_rettype(flinfo).unwrap_or(InvalidOid);
    internal_get_result_type(
        mcx,
        flinfo.fn_oid,
        call_expr_node(flinfo),
        carrier_rettype,
        expected_desc,
    )
}

// DatumGetHeapTupleHeader + HeapTupleHeaderGetTypeId/GetTypMod: decode a
// composite Datum's varlena header without deforming the tuple body.
fn composite_datum_header(mcx: Mcx<'_>, value: datum::Datum) -> PgResult<(Oid, i32)> {
    let src = value.as_usize() as *const u8;
    // SAFETY: `value` is a live composite (RECORDOID) Datum.
    let (hdrp, detoasted) = unsafe {
        if types_tuple::varatt::varatt_is_1b(src)
            || types_tuple::varatt::varatt_is_1b_e(src)
            || !types_tuple::varatt::varatt_is_4b_u(src)
        {
            let image = core::slice::from_raw_parts(src, types_tuple::varatt::varsize_any(src));
            let flat = detoast_seams::detoast_attr::call(mcx, image)?;
            (flat.as_ptr(), Some(flat))
        } else {
            (src, None)
        }
    };
    // SAFETY: hdrp is a plain composite varlena image whose header the
    // caller guarantees is fully present.
    let hdr: types_tuple::HeapTupleHeaderData = unsafe { core::ptr::read_unaligned(hdrp.cast()) };
    drop(detoasted);
    Ok((hdr.type_id(), hdr.typmod()))
}

pub fn get_expr_result_type<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Option<Node<'_>>,
) -> PgResult<ResolvedResultType<'mcx>> {
    if let Some(node) = expr {
        if let Some(fe) = node.as_func_expr() {
            return internal_get_result_type(mcx, fe.funcid, Some(node), InvalidOid, None);
        }
        if let Some(op) = node.as_op_expr() {
            let funcid = lsyscache::get_opcode(op.opno)?;
            return internal_get_result_type(mcx, funcid, Some(node), InvalidOid, None);
        }
        if let Some(re) = node.as_row_expr() {
            // Named rowtypes resolve through the generic typid path below.
            if re.row_typeid == RECORDOID {
                // C: resolve the record type by generating the tupdesc
                // directly from the RowExpr columns, then BlessTupleDesc.
                debug_assert_eq!(re.args.len(), re.colnames.len());
                let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, re.args.len() as i32)?;
                for (i, (col, name)) in re.args.iter().zip(re.colnames.iter()).enumerate() {
                    let attnum = (i + 1) as AttrNumber;
                    let colname = name.as_string().expect("RowExpr colname is a String").sval;
                    tupdesc::TupleDescInitEntry(
                        &mut tupdesc,
                        attnum,
                        Some(colname),
                        nodes_core::expr_type(col),
                        nodes_core::expr_typmod(col),
                        0,
                    )?;
                    tupdesc::TupleDescInitEntryCollation(
                        &mut tupdesc,
                        attnum,
                        nodes_core::expr_collation(col),
                    );
                }
                // C BlessTupleDesc: register the anonymous rowtype's typmod.
                if tupdesc.tdtypeid == RECORDOID && tupdesc.tdtypmod < 0 {
                    typcache_seams::assign_record_type_typmod::call(&mut tupdesc)?;
                }
                return Ok(ResolvedResultType {
                    class: TypeFuncClass::Composite,
                    result_type_id: RECORDOID,
                    result_tuple_desc: Some(tupdesc),
                });
            }
        }
        if let Some(c) = node.as_const() {
            if c.consttype == RECORDOID && !c.constisnull {
                // C: DatumGetHeapTupleHeader + HeapTupleHeaderGetTypeId/GetTypMod
                // (funcapi.c get_expr_result_type, the SEARCH/CYCLE Const leg).
                let (tup_type, tup_typmod) = composite_datum_header(mcx, c.constvalue)?;
                if tup_type != RECORDOID || tup_typmod >= 0 {
                    return Ok(ResolvedResultType {
                        class: TypeFuncClass::Composite,
                        result_type_id: tup_type,
                        result_tuple_desc: Some(typcache_seams::lookup_rowtype_tupdesc_copy::call(
                            mcx, tup_type, tup_typmod,
                        )?),
                    });
                }
                // Shouldn't really happen.
                return Ok(ResolvedResultType {
                    class: TypeFuncClass::Record,
                    result_type_id: tup_type,
                    result_tuple_desc: None,
                });
            }
        }
    }

    let typid = expr_type(expr);
    let (class, base_typid) = get_type_func_class(typid)?;
    let mut out = ResolvedResultType { class, result_type_id: typid, result_tuple_desc: None };
    if matches!(class, TypeFuncClass::Composite | TypeFuncClass::CompositeDomain) {
        out.result_tuple_desc =
            Some(typcache_seams::lookup_rowtype_tupdesc_copy::call(mcx, base_typid, -1)?);
    }
    Ok(out)
}

pub fn get_func_result_type<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
) -> PgResult<ResolvedResultType<'mcx>> {
    internal_get_result_type(mcx, function_id, None, InvalidOid, None)
}

// get_func_result_name: the name of a function's single named OUT parameter,
// else None.
pub fn get_func_result_name<'mcx>(mcx: Mcx<'mcx>, funcid: Oid) -> PgResult<Option<&'mcx str>> {
    let arrays = syscache_seams::pg_proc_result_arrays::call(mcx, funcid)?
        .ok_or_else(|| function_lookup_failed(funcid))?;
    let (Some(argmodes), Some(argnames)) = (arrays.proargmodes, arrays.proargnames) else {
        return Ok(None);
    };
    debug_assert_eq!(argmodes.len(), argnames.len());
    let mut result = None;
    for (i, &mode) in argmodes.iter().enumerate() {
        if mode == PROARGMODE_IN || mode == PROARGMODE_VARIADIC {
            continue;
        }
        debug_assert!(
            mode == PROARGMODE_OUT || mode == PROARGMODE_INOUT || mode == PROARGMODE_TABLE
        );
        if result.is_some() {
            return Ok(None);
        }
        let name = argnames[i].as_str();
        if name.is_empty() {
            return Ok(None);
        }
        result = Some(name);
    }
    // PgString borrows arrays' storage which dies with this frame; re-own.
    match result {
        Some(name) => {
            let bytes = mcx::slice_borrow_in(mcx, name.as_bytes())?;
            // SAFETY: byte-for-byte copy of a &str.
            Ok(Some(unsafe { core::str::from_utf8_unchecked(bytes) }))
        }
        None => Ok(None),
    }
}

// `carrier_rettype` is the AggFnArgTypes carrier's resolved return type
// (InvalidOid outside aggregate contexts); it stands in for exprType over C's
// fake trans/final FuncExpr, which has no Node form here.
fn internal_get_result_type<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
    call_expr: Option<Node<'_>>,
    carrier_rettype: Oid,
    expected_desc: Option<&TupleDescData<'_>>,
) -> PgResult<ResolvedResultType<'mcx>> {
    let procform = syscache_seams::lookup_pg_proc_shape::call(funcid)?
        .ok_or_else(|| function_lookup_failed(funcid))?;

    let mut rettype = procform.prorettype;

    if let Some(mut tupdesc) = build_function_result_tupdesc_t(mcx, funcid, &procform)? {
        let (_, declared_args) = syscache_seams::lookup_pg_proc_signature::call(mcx, funcid)?
            .ok_or_else(|| function_lookup_failed(funcid))?;
        if resolve_polymorphic_tupdesc(&mut tupdesc, &declared_args, call_expr)? {
            if tupdesc.tdtypeid == RECORDOID && tupdesc.tdtypmod < 0 {
                typcache_seams::assign_record_type_typmod::call(&mut tupdesc)?;
            }
            return Ok(ResolvedResultType {
                class: TypeFuncClass::Composite,
                result_type_id: rettype,
                result_tuple_desc: Some(tupdesc),
            });
        }
        return Ok(ResolvedResultType {
            class: TypeFuncClass::Record,
            result_type_id: rettype,
            result_tuple_desc: None,
        });
    }

    if is_polymorphic_type(rettype) {
        let mut newrettype = expr_type(call_expr);
        if newrettype == InvalidOid {
            newrettype = carrier_rettype;
        }
        if newrettype == InvalidOid {
            return Err(unresolved_polymorphic_rettype(funcid, rettype));
        }
        rettype = newrettype;
    }

    let (mut class, base_rettype) = get_type_func_class(rettype)?;
    let mut result_tuple_desc = None;
    match class {
        TypeFuncClass::Composite | TypeFuncClass::CompositeDomain => {
            result_tuple_desc =
                Some(typcache_seams::lookup_rowtype_tupdesc_copy::call(mcx, base_rettype, -1)?);
        }
        TypeFuncClass::Scalar => {}
        TypeFuncClass::Record => {
            if let Some(expected) = expected_desc {
                class = TypeFuncClass::Composite;
                // C aliases rsinfo->expectedDesc; the owned result copies it.
                result_tuple_desc = Some(tupdesc::CreateTupleDescCopy(mcx, expected)?);
            }
        }
        TypeFuncClass::Other => {}
    }

    Ok(ResolvedResultType { class, result_type_id: rettype, result_tuple_desc })
}

pub fn get_expr_result_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Option<Node<'_>>,
    no_error: bool,
) -> PgResult<Option<TupleDescData<'mcx>>> {
    let resolved = get_expr_result_type(mcx, expr)?;

    if matches!(
        resolved.class,
        TypeFuncClass::Composite | TypeFuncClass::CompositeDomain
    ) {
        return Ok(resolved.result_tuple_desc);
    }

    if !no_error {
        let expr_type_id = expr_type(expr);
        let err = if expr_type_id != RECORDOID {
            PgError::error(format!(
                "type {} is not composite",
                format_type::format_type_be(expr_type_id)?
            ))
        } else {
            PgError::error("record type has not been registered")
        };
        return Err(Box::new(err.with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)));
    }

    Ok(None)
}

// C: resolve_polymorphic_tupdesc (funcapi.c:743). Ok(false) is C's `return
// false`: call_expr missing or an input's actual type unidentifiable.
pub fn resolve_polymorphic_tupdesc(
    tupdesc: &mut TupleDescData<'_>,
    declared_args: &[Oid],
    call_expr: Option<Node<'_>>,
) -> PgResult<bool> {
    let mut have_polymorphic_result = false;
    let mut have_result = [false; 8];
    const R_ELEM: usize = 0;
    const R_ARRAY: usize = 1;
    const R_RANGE: usize = 2;
    const R_MULTI: usize = 3;
    const R_C_ELEM: usize = 4;
    const R_C_ARRAY: usize = 5;
    const R_C_RANGE: usize = 6;
    const R_C_MULTI: usize = 7;
    for i in 0..tupdesc.natts as usize {
        let slot = match tupdesc.attr(i).atttypid {
            ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => Some(R_ELEM),
            ANYARRAYOID => Some(R_ARRAY),
            ANYRANGEOID => Some(R_RANGE),
            ANYMULTIRANGEOID => Some(R_MULTI),
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => Some(R_C_ELEM),
            ANYCOMPATIBLEARRAYOID => Some(R_C_ARRAY),
            ANYCOMPATIBLERANGEOID => Some(R_C_RANGE),
            ANYCOMPATIBLEMULTIRANGEOID => Some(R_C_MULTI),
            _ => None,
        };
        if let Some(s) = slot {
            have_polymorphic_result = true;
            have_result[s] = true;
        }
    }
    if !have_polymorphic_result {
        return Ok(true);
    }

    let Some(call_expr) = call_expr else {
        return Ok(false);
    };

    let mut poly = PolymorphicActuals::default();
    let mut anyc = PolymorphicActuals::default();
    for (i, &argtype) in declared_args.iter().enumerate() {
        let actual = match argtype {
            ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => &mut poly.anyelement_type,
            ANYARRAYOID => &mut poly.anyarray_type,
            ANYRANGEOID => &mut poly.anyrange_type,
            ANYMULTIRANGEOID => &mut poly.anymultirange_type,
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => &mut anyc.anyelement_type,
            ANYCOMPATIBLEARRAYOID => &mut anyc.anyarray_type,
            ANYCOMPATIBLERANGEOID => &mut anyc.anyrange_type,
            ANYCOMPATIBLEMULTIRANGEOID => &mut anyc.anymultirange_type,
            _ => continue,
        };
        if *actual == InvalidOid {
            *actual = get_call_expr_argtype(call_expr, i);
            if *actual == InvalidOid {
                return Ok(false);
            }
        }
    }

    if have_result[R_ELEM] && poly.anyelement_type == InvalidOid {
        resolve_anyelement_from_others(&mut poly)?;
    }
    if have_result[R_ARRAY] && poly.anyarray_type == InvalidOid {
        resolve_anyarray_from_others(&mut poly)?;
    }
    if have_result[R_RANGE] && poly.anyrange_type == InvalidOid {
        resolve_anyrange_from_others(&mut poly)?;
    }
    if have_result[R_MULTI] && poly.anymultirange_type == InvalidOid {
        resolve_anymultirange_from_others(&mut poly)?;
    }
    if have_result[R_C_ELEM] && anyc.anyelement_type == InvalidOid {
        resolve_anyelement_from_others(&mut anyc)?;
    }
    if have_result[R_C_ARRAY] && anyc.anyarray_type == InvalidOid {
        resolve_anyarray_from_others(&mut anyc)?;
    }
    if have_result[R_C_RANGE] && anyc.anyrange_type == InvalidOid {
        resolve_anyrange_from_others(&mut anyc)?;
    }
    if have_result[R_C_MULTI] && anyc.anymultirange_type == InvalidOid {
        resolve_anymultirange_from_others(&mut anyc)?;
    }

    let mut anycollation = InvalidOid;
    let mut anycompatcollation = InvalidOid;
    if poly.anyelement_type != InvalidOid {
        anycollation = lsyscache::get_typcollation(poly.anyelement_type)?;
    } else if poly.anyarray_type != InvalidOid {
        anycollation = lsyscache::get_typcollation(poly.anyarray_type)?;
    }
    if anyc.anyelement_type != InvalidOid {
        anycompatcollation = lsyscache::get_typcollation(anyc.anyelement_type)?;
    } else if anyc.anyarray_type != InvalidOid {
        anycompatcollation = lsyscache::get_typcollation(anyc.anyarray_type)?;
    }
    if anycollation != InvalidOid || anycompatcollation != InvalidOid {
        let inputcollation = expr_input_collation(call_expr);
        if inputcollation != InvalidOid {
            if anycollation != InvalidOid {
                anycollation = inputcollation;
            }
            if anycompatcollation != InvalidOid {
                anycompatcollation = inputcollation;
            }
        }
    }

    for i in 0..tupdesc.natts as usize {
        let att = tupdesc.attr(i);
        let atttypid = att.atttypid;
        let (newtype, coll) = match atttypid {
            ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => {
                (poly.anyelement_type, Some(anycollation))
            }
            ANYARRAYOID => (poly.anyarray_type, Some(anycollation)),
            ANYRANGEOID => (poly.anyrange_type, None),
            ANYMULTIRANGEOID => (poly.anymultirange_type, None),
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
                (anyc.anyelement_type, Some(anycompatcollation))
            }
            ANYCOMPATIBLEARRAYOID => (anyc.anyarray_type, Some(anycompatcollation)),
            ANYCOMPATIBLERANGEOID => (anyc.anyrange_type, None),
            ANYCOMPATIBLEMULTIRANGEOID => (anyc.anymultirange_type, None),
            _ => continue,
        };
        let attname = att.attname;
        let name = core::str::from_utf8(attname.name_str()).expect("attname is text");
        tupdesc::TupleDescInitEntry(tupdesc, (i + 1) as AttrNumber, Some(name), newtype, -1, 0)?;
        if let Some(c) = coll {
            tupdesc::TupleDescInitEntryCollation(tupdesc, (i + 1) as AttrNumber, c);
        }
    }
    Ok(true)
}

// C: exprInputCollation (nodeFuncs.c) over the call-expression families.
fn expr_input_collation(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().inputcollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().inputcollid,
        _ => InvalidOid,
    }
}

#[derive(Default)]
struct PolymorphicActuals {
    anyelement_type: Oid,
    anyarray_type: Oid,
    anyrange_type: Oid,
    anymultirange_type: Oid,
}

fn poly_err(sqlstate: types_error::SqlState, msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

fn resolve_anyelement_from_others(a: &mut PolymorphicActuals) -> PgResult<()> {
    if a.anyarray_type != InvalidOid {
        let base = lsyscache::getBaseType(a.anyarray_type)?;
        let elem = lsyscache::get_element_type(base)?;
        if elem == InvalidOid {
            return Err(poly_err(
                ERRCODE_DATATYPE_MISMATCH,
                format!(
                    "argument declared anyarray is not an array but type {}",
                    format_type::format_type_be(base)?
                ),
            ));
        }
        a.anyelement_type = elem;
    } else if a.anyrange_type != InvalidOid {
        let base = lsyscache::getBaseType(a.anyrange_type)?;
        let sub = lsyscache::get_range_subtype(base)?;
        if sub == InvalidOid {
            return Err(poly_err(
                ERRCODE_DATATYPE_MISMATCH,
                format!(
                    "argument declared anyrange is not a range type but type {}",
                    format_type::format_type_be(base)?
                ),
            ));
        }
        a.anyelement_type = sub;
    } else if a.anymultirange_type != InvalidOid {
        let mr_base = lsyscache::getBaseType(a.anymultirange_type)?;
        let mr_range = lsyscache::get_multirange_range(mr_base)?;
        if mr_range == InvalidOid {
            return Err(poly_err(
                ERRCODE_DATATYPE_MISMATCH,
                format!(
                    "argument declared anymultirange is not a multirange type but type {}",
                    format_type::format_type_be(mr_base)?
                ),
            ));
        }
        let r_base = lsyscache::getBaseType(mr_range)?;
        let sub = lsyscache::get_range_subtype(r_base)?;
        if sub == InvalidOid {
            return Err(poly_err(
                ERRCODE_DATATYPE_MISMATCH,
                format!(
                    "argument declared anymultirange does not contain a range type but type {}",
                    format_type::format_type_be(r_base)?
                ),
            ));
        }
        a.anyelement_type = sub;
    } else {
        return Err(Box::new(PgError::error(
            "could not determine polymorphic type".to_string(),
        )));
    }
    Ok(())
}

fn resolve_anyarray_from_others(a: &mut PolymorphicActuals) -> PgResult<()> {
    if a.anyelement_type == InvalidOid {
        resolve_anyelement_from_others(a)?;
    }
    if a.anyelement_type != InvalidOid {
        let arr = lsyscache::get_array_type(a.anyelement_type)?;
        if arr == InvalidOid {
            return Err(poly_err(
                types_error::ERRCODE_UNDEFINED_OBJECT,
                format!(
                    "could not find array type for data type {}",
                    format_type::format_type_be(a.anyelement_type)?
                ),
            ));
        }
        a.anyarray_type = arr;
        Ok(())
    } else {
        Err(Box::new(PgError::error("could not determine polymorphic type".to_string())))
    }
}

fn resolve_anyrange_from_others(a: &mut PolymorphicActuals) -> PgResult<()> {
    // A range type is not deducible from array/base actuals (subtypes are
    // not unique across range types), only from a multirange actual.
    if a.anymultirange_type != InvalidOid {
        let mr_base = lsyscache::getBaseType(a.anymultirange_type)?;
        let mr_range = lsyscache::get_multirange_range(mr_base)?;
        if mr_range == InvalidOid {
            return Err(poly_err(
                ERRCODE_DATATYPE_MISMATCH,
                format!(
                    "argument declared anymultirange is not a multirange type but type {}",
                    format_type::format_type_be(mr_base)?
                ),
            ));
        }
        a.anyrange_type = mr_range;
        Ok(())
    } else {
        Err(Box::new(PgError::error("could not determine polymorphic type".to_string())))
    }
}

fn resolve_anymultirange_from_others(a: &mut PolymorphicActuals) -> PgResult<()> {
    if a.anyrange_type != InvalidOid {
        let r_base = lsyscache::getBaseType(a.anyrange_type)?;
        let mr = lsyscache::get_range_multirange(r_base)?;
        if mr == InvalidOid {
            return Err(poly_err(
                types_error::ERRCODE_UNDEFINED_OBJECT,
                format!(
                    "could not find multirange type for data type {}",
                    format_type::format_type_be(a.anyrange_type)?
                ),
            ));
        }
        a.anymultirange_type = mr;
        Ok(())
    } else {
        Err(Box::new(PgError::error("could not determine polymorphic type".to_string())))
    }
}

// C: resolve_polymorphic_argtypes (funcapi.c). `argmodes` empty means all IN.
// Ok(false) is C's `return false`: an input's actual type was unavailable.
// `call_expr` is the erased fn_expr: aggregate support-fn flinfos carry the
// AggFnArgTypes stand-in where C has a constructed FuncExpr.
pub fn resolve_polymorphic_argtypes(
    argtypes: &mut [Oid],
    argmodes: &[i8],
    call_expr: Option<types_core::fmgr::FnExprErased>,
) -> PgResult<bool> {
    let mut poly = PolymorphicActuals::default();
    let mut anyc = PolymorphicActuals::default();
    let mut have_result = [false; 8];
    const R_ELEM: usize = 0;
    const R_ARRAY: usize = 1;
    const R_RANGE: usize = 2;
    const R_MULTI: usize = 3;
    const R_C_ELEM: usize = 4;
    const R_C_ARRAY: usize = 5;
    const R_C_RANGE: usize = 6;
    const R_C_MULTI: usize = 7;

    let mut inargno = 0usize;
    for i in 0..argtypes.len() {
        let argmode = argmodes.get(i).copied().unwrap_or(PROARGMODE_IN);
        let is_out = argmode == PROARGMODE_OUT || argmode == PROARGMODE_TABLE;
        let slot = match argtypes[i] {
            ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => {
                Some((R_ELEM, &mut poly.anyelement_type))
            }
            ANYARRAYOID => Some((R_ARRAY, &mut poly.anyarray_type)),
            ANYRANGEOID => Some((R_RANGE, &mut poly.anyrange_type)),
            ANYMULTIRANGEOID => Some((R_MULTI, &mut poly.anymultirange_type)),
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
                Some((R_C_ELEM, &mut anyc.anyelement_type))
            }
            ANYCOMPATIBLEARRAYOID => Some((R_C_ARRAY, &mut anyc.anyarray_type)),
            ANYCOMPATIBLERANGEOID => Some((R_C_RANGE, &mut anyc.anyrange_type)),
            ANYCOMPATIBLEMULTIRANGEOID => Some((R_C_MULTI, &mut anyc.anymultirange_type)),
            _ => None,
        };
        if let Some((ridx, actual)) = slot {
            if is_out {
                have_result[ridx] = true;
            } else {
                if *actual == InvalidOid {
                    *actual =
                        call_expr
                            .as_ref()
                            .map_or(InvalidOid, |e| erased_call_expr_argtype(e, inargno));
                    if *actual == InvalidOid {
                        return Ok(false);
                    }
                }
                argtypes[i] = *actual;
            }
        }
        if !is_out {
            inargno += 1;
        }
    }

    if !have_result.iter().any(|&b| b) {
        return Ok(true);
    }

    if have_result[R_ELEM] && poly.anyelement_type == InvalidOid {
        resolve_anyelement_from_others(&mut poly)?;
    }
    if have_result[R_ARRAY] && poly.anyarray_type == InvalidOid {
        resolve_anyarray_from_others(&mut poly)?;
    }
    if have_result[R_RANGE] && poly.anyrange_type == InvalidOid {
        resolve_anyrange_from_others(&mut poly)?;
    }
    if have_result[R_MULTI] && poly.anymultirange_type == InvalidOid {
        resolve_anymultirange_from_others(&mut poly)?;
    }
    if have_result[R_C_ELEM] && anyc.anyelement_type == InvalidOid {
        resolve_anyelement_from_others(&mut anyc)?;
    }
    if have_result[R_C_ARRAY] && anyc.anyarray_type == InvalidOid {
        resolve_anyarray_from_others(&mut anyc)?;
    }
    if have_result[R_C_RANGE] && anyc.anyrange_type == InvalidOid {
        resolve_anyrange_from_others(&mut anyc)?;
    }
    if have_result[R_C_MULTI] && anyc.anymultirange_type == InvalidOid {
        resolve_anymultirange_from_others(&mut anyc)?;
    }

    for t in argtypes.iter_mut() {
        *t = match *t {
            ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => poly.anyelement_type,
            ANYARRAYOID => poly.anyarray_type,
            ANYRANGEOID => poly.anyrange_type,
            ANYMULTIRANGEOID => poly.anymultirange_type,
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => anyc.anyelement_type,
            ANYCOMPATIBLEARRAYOID => anyc.anyarray_type,
            ANYCOMPATIBLERANGEOID => anyc.anyrange_type,
            ANYCOMPATIBLEMULTIRANGEOID => anyc.anymultirange_type,
            other => other,
        };
    }
    Ok(true)
}

// C: cfunc_resolve_polymorphic_argtypes (funccache.c).
pub fn cfunc_resolve_polymorphic_argtypes(
    argtypes: &mut [Oid],
    argmodes: &[i8],
    call_expr: Option<types_core::fmgr::FnExprErased>,
    for_validator: bool,
    proname: &str,
) -> PgResult<()> {
    const INT4ARRAYOID: Oid = 1007;
    const INT4RANGEOID: Oid = 3904;
    const INT4MULTIRANGEOID: Oid = 4451;
    if !for_validator {
        if !resolve_polymorphic_argtypes(argtypes, argmodes, call_expr)? {
            return Err(poly_err(
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                format!(
                    "could not determine actual argument type for polymorphic function \"{proname}\""
                ),
            ));
        }
        let mut inargno = 0usize;
        for i in 0..argtypes.len() {
            let argmode = argmodes.get(i).copied().unwrap_or(PROARGMODE_IN);
            if argmode == PROARGMODE_OUT || argmode == PROARGMODE_TABLE {
                continue;
            }
            if argtypes[i] == RECORDOID || argtypes[i] == types_core::RECORDARRAYOID {
                let resolved = call_expr
                    .as_ref()
                    .map_or(InvalidOid, |e| erased_call_expr_argtype(e, inargno));
                if resolved != InvalidOid {
                    argtypes[i] = resolved;
                }
            }
            inargno += 1;
        }
    } else {
        for t in argtypes.iter_mut() {
            *t = match *t {
                ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID | ANYCOMPATIBLEOID
                | ANYCOMPATIBLENONARRAYOID => types_core::INT4OID,
                ANYARRAYOID | ANYCOMPATIBLEARRAYOID => INT4ARRAYOID,
                ANYRANGEOID | ANYCOMPATIBLERANGEOID => INT4RANGEOID,
                ANYMULTIRANGEOID => INT4MULTIRANGEOID,
                other => other,
            };
        }
    }
    Ok(())
}

pub fn get_type_func_class(typid: Oid) -> PgResult<(TypeFuncClass, Oid)> {
    use lsyscache::{
        TYPTYPE_BASE, TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN, TYPTYPE_ENUM, TYPTYPE_MULTIRANGE,
        TYPTYPE_PSEUDO, TYPTYPE_RANGE,
    };

    let typtype = lsyscache::get_typtype(typid)?;
    if typtype == TYPTYPE_COMPOSITE {
        return Ok((TypeFuncClass::Composite, typid));
    }
    if typtype == TYPTYPE_BASE
        || typtype == TYPTYPE_ENUM
        || typtype == TYPTYPE_RANGE
        || typtype == TYPTYPE_MULTIRANGE
    {
        return Ok((TypeFuncClass::Scalar, typid));
    }
    if typtype == TYPTYPE_DOMAIN {
        let base = lsyscache::getBaseType(typid)?;
        if lsyscache::get_typtype(base)? == TYPTYPE_COMPOSITE {
            return Ok((TypeFuncClass::CompositeDomain, base));
        }
        // Domain base type can't be a pseudotype.
        return Ok((TypeFuncClass::Scalar, base));
    }
    if typtype == TYPTYPE_PSEUDO {
        if typid == RECORDOID {
            return Ok((TypeFuncClass::Record, typid));
        }
        // VOID and CSTRING are legitimate scalars (JDBC convenience, as C).
        if typid == VOIDOID || typid == CSTRINGOID {
            return Ok((TypeFuncClass::Scalar, typid));
        }
    }
    Ok((TypeFuncClass::Other, typid))
}

pub fn build_function_result_tupdesc_t<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
    procform: &PgProcShape,
) -> PgResult<Option<TupleDescData<'mcx>>> {
    if procform.prorettype != RECORDOID {
        return Ok(None);
    }

    let arrays = syscache_seams::pg_proc_result_arrays::call(mcx, funcid)?
        .ok_or_else(|| function_lookup_failed(funcid))?;
    let (Some(argtypes), Some(argmodes)) = (arrays.proallargtypes, arrays.proargmodes) else {
        return Ok(None);
    };

    build_function_result_tupdesc_d(
        mcx,
        procform.prokind,
        &argtypes,
        &argmodes,
        arrays.proargnames.as_deref(),
    )
}

pub fn build_function_result_tupdesc_d<'mcx>(
    mcx: Mcx<'mcx>,
    prokind: i8,
    argtypes: &[Oid],
    argmodes: &[i8],
    argnames: Option<&[PgString<'_>]>,
) -> PgResult<Option<TupleDescData<'mcx>>> {
    let numargs = argtypes.len();
    debug_assert_eq!(argmodes.len(), numargs);
    if let Some(names) = argnames {
        debug_assert_eq!(names.len(), numargs);
    }
    if numargs == 0 {
        return Ok(None);
    }

    let mut out_idxs: PgVec<'_, usize> = vec_with_capacity_in(mcx, numargs)?;
    for (i, &mode) in argmodes.iter().enumerate() {
        if mode == PROARGMODE_IN || mode == PROARGMODE_VARIADIC {
            continue;
        }
        debug_assert!(
            mode == PROARGMODE_OUT || mode == PROARGMODE_INOUT || mode == PROARGMODE_TABLE
        );
        out_idxs.push(i);
    }

    if out_idxs.len() < 2 && prokind != PROKIND_PROCEDURE {
        return Ok(None);
    }

    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, out_idxs.len() as i32)?;
    for (colno, &i) in out_idxs.iter().enumerate() {
        let attnum = (colno + 1) as AttrNumber;
        let named = argnames
            .map(|names| names[i].as_str())
            .filter(|s| !s.is_empty());
        match named {
            Some(name) => {
                tupdesc::TupleDescInitEntry(&mut desc, attnum, Some(name), argtypes[i], -1, 0)?;
            }
            None => {
                let generated = format!("column{}", colno + 1);
                tupdesc::TupleDescInitEntry(
                    &mut desc,
                    attnum,
                    Some(&generated),
                    argtypes[i],
                    -1,
                    0,
                )?;
            }
        }
    }

    Ok(Some(desc))
}
