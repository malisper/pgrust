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

mod srf;
#[cfg(test)]
mod tests;

pub use srf::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall, srf_return_done, srf_return_next,
    srf_return_next_null,
    FuncCallContext, InitMaterializedSRF, MaterializedSRF, MAT_SRF_BLESS,
    MAT_SRF_USE_EXPECTED_DESC,
};

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
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funcresulttype,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opresulttype,
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggtype,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wintype,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resulttype,
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casetype,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().coalescetype,
        NodeTag::T_RowExpr => node.as_row_expr().unwrap().row_typeid,
        tag => panic!("funcapi exprType: node family {tag:?} not ported"),
    }
}

fn call_expr_node(flinfo: &FmgrInfo) -> Option<Node<'static>> {
    flinfo.fn_expr.as_ref().map(|e| {
        *e.downcast_ref::<Node<'static>>()
            .expect("funcapi: fn_expr does not carry a Node")
    })
}

/// C: get_fn_expr_rettype (fmgr.c); InvalidOid mirrors the NULL-fn_expr case.
pub fn get_fn_expr_rettype(flinfo: &FmgrInfo) -> Oid {
    match call_expr_node(flinfo) {
        None => InvalidOid,
        Some(n) => expr_type(Some(n)),
    }
}

/// C: get_fn_expr_argtype (fmgr.c). Aggregate transfns carry an
/// `AggFnArgTypes` vector where C builds a fake transfn FuncExpr.
pub fn get_fn_expr_argtype(flinfo: Option<&FmgrInfo>, argnum: usize) -> Oid {
    let Some(e) = flinfo.and_then(|f| f.fn_expr.as_ref()) else {
        return InvalidOid;
    };
    if let Some(agg) = e.downcast_ref::<types_core::fmgr::AggFnArgTypes>() {
        return agg.0.get(argnum).copied().unwrap_or(InvalidOid);
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

#[cold]
fn function_lookup_failed(funcid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "cache lookup failed for function {funcid}"
    )))
}

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
    internal_get_result_type(mcx, flinfo.fn_oid, call_expr_node(flinfo), expected_desc)
}

pub fn get_expr_result_type<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Option<Node<'_>>,
) -> PgResult<ResolvedResultType<'mcx>> {
    if let Some(node) = expr {
        if let Some(fe) = node.as_func_expr() {
            return internal_get_result_type(mcx, fe.funcid, Some(node), None);
        }
        if let Some(op) = node.as_op_expr() {
            let funcid = lsyscache::get_opcode(op.opno)?;
            return internal_get_result_type(mcx, funcid, Some(node), None);
        }
        if node.node_tag() == NodeTag::T_RowExpr {
            panic!("funcapi get_expr_result_type: RowExpr(RECORD) leg not ported");
        }
        if let Some(c) = node.as_const() {
            if c.consttype == RECORDOID && !c.constisnull {
                panic!(
                    "funcapi get_expr_result_type: RECORD-Const leg not ported \
                     (composite Datum header decode)"
                );
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
    internal_get_result_type(mcx, function_id, None, None)
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

fn internal_get_result_type<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
    call_expr: Option<Node<'_>>,
    expected_desc: Option<&TupleDescData<'_>>,
) -> PgResult<ResolvedResultType<'mcx>> {
    let procform = syscache_seams::lookup_pg_proc_shape::call(funcid)?
        .ok_or_else(|| function_lookup_failed(funcid))?;

    let mut rettype = procform.prorettype;

    if let Some(mut tupdesc) = build_function_result_tupdesc_t(mcx, funcid, &procform)? {
        let (_, declared_args) = syscache_seams::lookup_pg_proc_signature::call(mcx, funcid)?
            .ok_or_else(|| function_lookup_failed(funcid))?;
        if resolve_polymorphic_tupdesc(&mut tupdesc, &declared_args, call_expr) {
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
        let newrettype = expr_type(call_expr);
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

pub fn resolve_polymorphic_tupdesc(
    tupdesc: &mut TupleDescData<'_>,
    _declared_args: &[Oid],
    call_expr: Option<Node<'_>>,
) -> bool {
    let mut have_polymorphic_result = false;
    for i in 0..tupdesc.natts as usize {
        if is_polymorphic_type(tupdesc.attr(i).atttypid) {
            have_polymorphic_result = true;
        }
    }
    if !have_polymorphic_result {
        return true;
    }
    if call_expr.is_none() {
        return false;
    }
    // The remaining leg extracts actuals from the call expression's argument
    // list (get_call_expr_argtype) and rewrites the polymorphic columns.
    panic!("funcapi resolve_polymorphic_tupdesc: polymorphic OUT-column resolution not ported");
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
