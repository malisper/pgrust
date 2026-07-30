#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

#[cfg(test)]
mod tests;

use datum::Datum;
use fmgr::FmgrInfo;
use mcx::Mcx;
use nodes_core::node_funcs::{expr_type, expr_typmod};
use parser_small1::{
    parser_errposition, variable_coerce_param_hook, ParseRefHookState, ParseState,
};
use types_core::primitive::FUNC_MAX_ARGS;
use types_core::catalog::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
    ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
    ANYNONARRAYOID, ANYOID, ANYRANGEOID, BOOLOID, INT2VECTOROID, INT4OID, INTERVALOID,
    OIDVECTOROID, RECORDARRAYOID, RECORDOID, UNKNOWNOID,
};
use types_core::{InvalidOid, Oid, OidIsValid, ParseLoc};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_QUERY_CANCELED, ERROR,
};
use types_nodes::{
    ArrayCoerceExpr, CaseTestExpr, CoerceToDomain, CoerceViaIO, CoercionForm, CollateExpr, Const,
    ConvertRowtypeExpr, FuncExpr, Node, NodeList, NodeTag, Param, RelabelType,
};

// primnodes.h CoercionContext; ordering is load-bearing (ccontext >= castcontext).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u32)]
pub enum CoercionContext {
    COERCION_IMPLICIT = 0,
    COERCION_ASSIGNMENT = 1,
    COERCION_PLPGSQL = 2,
    COERCION_EXPLICIT = 3,
}
pub use CoercionContext::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoercionPathType {
    COERCION_PATH_NONE,
    COERCION_PATH_FUNC,
    COERCION_PATH_RELABELTYPE,
    COERCION_PATH_ARRAYCOERCE,
    COERCION_PATH_COERCEVIAIO,
}
pub use CoercionPathType::*;

const COERCION_CODE_IMPLICIT: i8 = b'i' as i8;
const COERCION_CODE_ASSIGNMENT: i8 = b'a' as i8;
const COERCION_CODE_EXPLICIT: i8 = b'e' as i8;
const COERCION_METHOD_FUNCTION: i8 = b'f' as i8;
const COERCION_METHOD_BINARY: i8 = b'b' as i8;
const COERCION_METHOD_INOUT: i8 = b'i' as i8;
pub const TYPCATEGORY_INVALID: i8 = 0;
pub const TYPCATEGORY_STRING: i8 = b'S' as i8;

// IsPolymorphicTypeFamily1 (pg_type.h).
fn is_polymorphic_type_family1(typid: Oid) -> bool {
    matches!(
        typid,
        ANYELEMENTOID | ANYARRAYOID | ANYNONARRAYOID | ANYENUMOID | ANYRANGEOID
            | ANYMULTIRANGEOID
    )
}

pub fn IsPolymorphicType(typid: Oid) -> bool {
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

// ISCOMPLEX (parse_type.h): typeOrDomainTypeRelid drills through domains.
fn is_complex(typid: Oid) -> PgResult<bool> {
    Ok(OidIsValid(lsyscache::get_typ_typrelid(lsyscache::getBaseType(typid)?)?))
}

fn is_complex_array(typid: Oid) -> PgResult<bool> {
    let elemtype = lsyscache::get_element_type(typid)?;
    Ok(OidIsValid(elemtype) && is_complex(elemtype)?)
}

pub fn coerce_type<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    inputTypeId: Oid,
    targetTypeId: Oid,
    targetTypeMod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    if targetTypeId == inputTypeId {
        return Ok(node);
    }
    if matches!(
        targetTypeId,
        ANYOID | ANYELEMENTOID | ANYNONARRAYOID | ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID
    ) {
        return Ok(node);
    }
    if matches!(
        targetTypeId,
        ANYARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    ) && inputTypeId != UNKNOWNOID
    {
        let baseTypeId = lsyscache::getBaseType(inputTypeId)?;
        if baseTypeId != inputTypeId {
            return Node::mk(
                mcx,
                RelabelType {
                    arg: node,
                    resulttype: baseTypeId,
                    resulttypmod: -1,
                    resultcollid: InvalidOid,
                    relabelformat: cformat,
                    location,
                },
            );
        }
        return Ok(node);
    }
    if inputTypeId == UNKNOWNOID && node.node_tag() == NodeTag::T_Const {
        return coerce_unknown_const(
            mcx,
            pstate,
            node,
            targetTypeId,
            targetTypeMod,
            ccontext,
            cformat,
            location,
        );
    }
    if node.node_tag() == NodeTag::T_Param
        && matches!(pstate.p_ref_hook_state, ParseRefHookState::VarParams(_))
    {
        let encoding = mbutils::GetDatabaseEncoding();
        // SAFETY: parse analysis holds exclusive access to the tree it is
        // transforming; no reference derived from `node` is live here.
        let consumed = unsafe {
            node.with_mut::<Param, _>(|p| {
                variable_coerce_param_hook(pstate, p, targetTypeId, targetTypeMod, location, encoding)
            })
        }
        .unwrap()?;
        if consumed {
            return Ok(node);
        }
    }
    // Push the coercion underneath the COLLATE so it acts before collation —
    // unless the target type is not collatable, where COLLATE is dropped.
    if let Some(coll) = node.as_collate_expr() {
        let arg = coerce_type(
            mcx,
            pstate,
            coll.arg,
            inputTypeId,
            targetTypeId,
            targetTypeMod,
            ccontext,
            cformat,
            location,
        )?;
        if !lsyscache::type_is_collatable(targetTypeId)? {
            return Ok(arg);
        }
        return Node::mk(
            mcx,
            types_nodes::CollateExpr { arg, collOid: coll.collOid, location: coll.location },
        );
    }
    let (pathtype, funcId) = find_coercion_pathway(targetTypeId, inputTypeId, ccontext)?;
    if pathtype != COERCION_PATH_NONE {
        let mut baseTypeMod = targetTypeMod;
        let baseTypeId = lsyscache::getBaseTypeAndTypmod(targetTypeId, &mut baseTypeMod)?;
        if pathtype != COERCION_PATH_RELABELTYPE {
            let result = build_coercion_expression(
                mcx, node, pathtype, funcId, baseTypeId, baseTypeMod, ccontext, cformat, location,
            )?;
            if targetTypeId != baseTypeId {
                return coerce_to_domain(
                    mcx, result, baseTypeId, baseTypeMod, targetTypeId, ccontext, cformat,
                    location, true,
                );
            }
            return Ok(result);
        }
        let result = coerce_to_domain(
            mcx, node, baseTypeId, baseTypeMod, targetTypeId, ccontext, cformat, location, false,
        )?;
        if result.ptr_eq(node) {
            return Node::mk(
                mcx,
                RelabelType {
                    arg: node,
                    resulttype: targetTypeId,
                    resulttypmod: -1,
                    resultcollid: InvalidOid,
                    relabelformat: cformat,
                    location,
                },
            );
        }
        return Ok(result);
    }
    if inputTypeId == RECORDOID && is_complex(targetTypeId)? {
        return coerce_record_to_complex(
            mcx,
            pstate,
            node,
            targetTypeId,
            ccontext,
            cformat,
            location,
        );
    }
    if targetTypeId == RECORDOID && is_complex(inputTypeId)? {
        // C: no RelabelType here.
        return Ok(node);
    }
    if targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId)? {
        // C: no RelabelType here.
        return Ok(node);
    }
    if pg_inherits_seams::type_inherits_from::call(inputTypeId, targetTypeId)?
        || type_is_of_typed_table(inputTypeId, targetTypeId)?
    {
        // C: ConvertRowtypeExpr works only between plain composite types; a
        // domain-over-subclass input is smashed to its base type first.
        let baseTypeId = lsyscache::getBaseType(inputTypeId)?;
        let arg = if baseTypeId != inputTypeId {
            Node::mk(
                mcx,
                RelabelType {
                    arg: node,
                    resulttype: baseTypeId,
                    resulttypmod: -1,
                    resultcollid: InvalidOid,
                    relabelformat: CoercionForm::COERCE_IMPLICIT_CAST,
                    location,
                },
            )?
        } else {
            node
        };
        return Node::mk(
            mcx,
            ConvertRowtypeExpr { arg, resulttype: targetTypeId, convertformat: cformat, location },
        );
    }
    Err(conversion_not_found(inputTypeId, targetTypeId))
}

// typeIsOfTypedTable (parse_coerce.c): is reltypeId the rowtype of a typed
// table OF reloftypeId (or a domain over one)?
fn type_is_of_typed_table(reltypeId: Oid, reloftypeId: Oid) -> PgResult<bool> {
    let relid = lsyscache::get_typ_typrelid(lsyscache::getBaseType(reltypeId)?)?;
    if !OidIsValid(relid) {
        return Ok(false);
    }
    match syscache_seams::pg_class_reloftype::call(relid)? {
        Some(reloftype) => Ok(reloftype == reloftypeId),
        None => Err(Box::new(PgError::error(format!(
            "cache lookup failed for relation {relid}"
        )))),
    }
}

// coerce_record_to_complex (parse_coerce.c).
#[allow(clippy::too_many_arguments)]
fn coerce_record_to_complex<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    targetTypeId: Oid,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    // A RowExpr source is RECORD-typed, so it holds no dropped columns.
    let args = match node.as_row_expr() {
        Some(r) => r.args.clone_in(mcx)?,
        None => match node.as_var() {
            Some(v) if v.varattno == types_core::InvalidAttrNumber => {
                parse_relation_seams::expand_nsitem_vars_at::call(
                    mcx,
                    pstate,
                    v.varno,
                    v.varlevelsup as i32,
                    v.location,
                )?
            }
            _ => return Err(record_cast_error(pstate, targetTypeId, Option::None, location)),
        },
    };
    let mut baseTypeMod = -1;
    let baseTypeId = lsyscache::getBaseTypeAndTypmod(targetTypeId, &mut baseTypeMod)?;
    let tupdesc = typcache_seams::lookup_rowtype_tupdesc_copy::call(mcx, baseTypeId, baseTypeMod)?;
    let mut newargs = NodeList::nil();
    let mut ucolno = 1usize;
    let mut argix = 0usize;
    for i in 0..tupdesc.natts as usize {
        let attr = tupdesc.attr(i);
        if attr.attisdropped {
            newargs.lappend(
                mcx,
                Node::mk_const(mcx, INT4OID, -1, InvalidOid, 4, Datum::null(), true, true)?,
            )?;
            continue;
        }
        if argix >= args.len() {
            return Err(record_cast_error(
                pstate,
                targetTypeId,
                Some("Input has too few columns.".to_string()),
                location,
            ));
        }
        let expr = args.nth(argix);
        let exprtype = expr_type(expr);
        let cexpr = coerce_to_target_type(
            mcx,
            pstate,
            expr,
            exprtype,
            attr.atttypid,
            attr.atttypmod,
            ccontext,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?
        .ok_or_else(|| {
            record_cast_column_error(pstate, exprtype, attr.atttypid, targetTypeId, ucolno, location)
        })?;
        newargs.lappend(mcx, cexpr)?;
        ucolno += 1;
        argix += 1;
    }
    if argix < args.len() {
        return Err(record_cast_error(
            pstate,
            targetTypeId,
            Some("Input has too many columns.".to_string()),
            location,
        ));
    }
    let rowexpr = Node::mk(
        mcx,
        types_nodes::RowExpr {
            args: newargs,
            row_typeid: baseTypeId,
            row_format: cformat,
            colnames: NodeList::nil(),
            location,
        },
    )?;
    if targetTypeId != baseTypeId {
        return coerce_to_domain(
            mcx,
            rowexpr,
            baseTypeId,
            baseTypeMod,
            targetTypeId,
            ccontext,
            cformat,
            location,
            false,
        );
    }
    Ok(rowexpr)
}

#[cold]
fn record_cast_error(
    pstate: &ParseState<'_, '_>,
    target: Oid,
    detail: Option<std::string::String>,
    location: ParseLoc,
) -> Box<types_error::PgError> {
    let tyname = format_type::format_type_be(target).unwrap_or_default();
    let mut e = types_error::PgError::error(format!("cannot cast type record to {tyname}"))
        .with_sqlstate(types_error::ERRCODE_CANNOT_COERCE);
    if let Some(d) = detail {
        e = e.with_detail(d);
    }
    Box::new(e.with_cursor_position(parser_errposition(
        pstate,
        location,
        mbutils::GetDatabaseEncoding(),
    )))
}

#[cold]
fn record_cast_column_error(
    pstate: &ParseState<'_, '_>,
    from: Oid,
    to: Oid,
    target: Oid,
    ucolno: usize,
    location: ParseLoc,
) -> Box<types_error::PgError> {
    let fname = format_type::format_type_be(from).unwrap_or_default();
    let toname = format_type::format_type_be(to).unwrap_or_default();
    let tyname = format_type::format_type_be(target).unwrap_or_default();
    Box::new(
        types_error::PgError::error(format!("cannot cast type record to {tyname}"))
            .with_detail(format!("Cannot cast type {fname} to {toname} in column {ucolno}."))
            .with_sqlstate(types_error::ERRCODE_CANNOT_COERCE)
            .with_cursor_position(parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            )),
    )
}

// C's coerce_type CONSTANT arm: typinput through fmgr (stringTypeDatum).
#[allow(clippy::too_many_arguments)]
fn coerce_unknown_const<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    targetTypeId: Oid,
    targetTypeMod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    let con = node.as_const().unwrap();

    let mut baseTypeMod = targetTypeMod;
    let baseTypeId = lsyscache::getBaseTypeAndTypmod(targetTypeId, &mut baseTypeMod)?;
    let inputTypeMod = if baseTypeId == INTERVALOID { baseTypeMod } else { -1 };

    let Some(io) = syscache_seams::pg_type_io_shape::call(baseTypeId)? else {
        return Err(type_lookup_failed(baseTypeId));
    };
    let constcollid = lsyscache::get_typcollation(baseTypeId)?;

    // C: setup_parser_errposition_callback(pcbstate, pstate, con->location)
    // around stringTypeDatum; retired-callback pattern attaches on Err.
    let constvalue = string_type_datum(mcx, &io, con.constvalue, inputTypeMod, con.constisnull)
        .map_err(|e| {
            if e.sqlstate() == ERRCODE_QUERY_CANCELED {
                return e;
            }
            let pos = parser_errposition(pstate, con.location, mbutils::GetDatabaseEncoding());
            Box::new((*e).with_cursor_position(pos))
        })?;

    let result = Node::mk(
        mcx,
        Const {
            consttype: baseTypeId,
            consttypmod: inputTypeMod,
            constcollid,
            constlen: io.typlen as i32,
            constvalue,
            constisnull: con.constisnull,
            constbyval: io.typbyval,
            location: con.location,
        },
    )?;

    if baseTypeId != targetTypeId {
        return coerce_to_domain(
            mcx,
            result,
            baseTypeId,
            baseTypeMod,
            targetTypeId,
            ccontext,
            cformat,
            location,
            false,
        );
    }
    Ok(result)
}

// C stringTypeDatum → OidInputFunctionCall; the frame is armed with `mcx`
// (result-mcx convention) and the result is still copied by own_datum —
// pre-convention wrappers (textin) return FmgrInfo-scratch that dies with
// the FmgrInfo, and the varlena arm detoasts.
fn string_type_datum<'mcx>(
    mcx: Mcx<'mcx>,
    io: &syscache_seams::PgTypeIoShape,
    value: Datum,
    typmod: i32,
    isnull: bool,
) -> PgResult<Datum> {
    if io.typinput == InvalidOid || !io.typisdefined {
        // Route through the lsyscache errors (shell type / no input function).
        lsyscache::getTypeInputInfo(io.oid)?;
    }
    let typioparam = lsyscache::getTypeIOParam(io);
    let mut flinfo = FmgrInfo::unresolved();
    fmgr_core::fmgr_info_into(io.typinput, &mut flinfo)?;
    if isnull {
        // C's InputFunctionCall: strict typinput short-circuits to a NULL
        // datum; a non-strict one (the pseudo-type *_in family) is invoked
        // with a NULL cstring and raises its own error (e.g. internal_in's
        // "cannot accept a value of type internal") — SELECT NULL::internal.
        return fmgr_core::input_function_call(&mut flinfo, None, typioparam, typmod, mcx);
    }
    let d = fmgr_core::function_call3_coll_in(
        &mut flinfo,
        InvalidOid,
        mcx,
        value,
        Datum::from_oid(typioparam),
        Datum::from_i32(typmod),
    )?;
    own_datum(mcx, d, io.typlen, io.typbyval)
}

fn own_datum<'mcx>(mcx: Mcx<'mcx>, d: Datum, typlen: i16, typbyval: bool) -> PgResult<Datum> {
    if typbyval {
        return Ok(d);
    }
    let p = d.as_usize() as *const u8;
    if typlen == -1 {
        // SAFETY: a strict input function returned a non-null by-reference
        // varlena datum; varsize_any reads only the header, valid for every
        // varlena form including a TOAST-pointer image.
        let image =
            unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
        // C: PG_DETOAST_DATUM over the new Const's value (parse_coerce.c) —
        // flattens external/expanded/compressed/short before the copy.
        return Ok(Datum::from_usize(
            detoast::detoast_attr(mcx, image)?.leak().as_ptr() as usize,
        ));
    }
    // SAFETY: a strict input function returned a non-null by-reference datum
    // of this type's declared layout (NUL-terminated cstring / fixed typlen
    // bytes).
    let bytes: &[u8] = unsafe {
        let len = match typlen {
            -2 => core::ffi::CStr::from_ptr(p.cast()).to_bytes_with_nul().len(),
            n if n > 0 => n as usize,
            n => panic!("own_datum: unexpected typlen {n}"),
        };
        core::slice::from_raw_parts(p, len)
    };
    Ok(Datum::from_usize(mcx::slice_in(mcx, bytes)?.leak().as_ptr() as usize))
}

pub fn can_coerce_type(
    input_typeids: &[Oid],
    target_typeids: &[Oid],
    ccontext: CoercionContext,
) -> PgResult<bool> {
    // C compares nargs entries; defaulted candidates carry longer target lists.
    debug_assert!(input_typeids.len() <= target_typeids.len());
    let mut have_generics = false;
    for (&inputTypeId, &targetTypeId) in input_typeids.iter().zip(target_typeids) {
        if inputTypeId == targetTypeId || targetTypeId == ANYOID {
            continue;
        }
        if IsPolymorphicType(targetTypeId) {
            have_generics = true;
            continue;
        }
        if inputTypeId == UNKNOWNOID {
            continue;
        }
        if find_coercion_pathway(targetTypeId, inputTypeId, ccontext)?.0 != COERCION_PATH_NONE {
            continue;
        }
        if (inputTypeId == RECORDOID && is_complex(targetTypeId)?)
            || (targetTypeId == RECORDOID && is_complex(inputTypeId)?)
        {
            continue;
        }
        if targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId)? {
            continue;
        }
        if pg_inherits_seams::type_inherits_from::call(inputTypeId, targetTypeId)?
            || type_is_of_typed_table(inputTypeId, targetTypeId)?
        {
            continue;
        }
        return Ok(false);
    }
    if have_generics && !check_generic_type_consistency(input_typeids, target_typeids)? {
        return Ok(false);
    }
    Ok(true)
}

fn check_generic_type_consistency(
    actual_arg_types: &[Oid],
    declared_arg_types: &[Oid],
) -> PgResult<bool> {
    let mut elem_typeid = InvalidOid;
    let mut array_typeid = InvalidOid;
    let mut range_typeid = InvalidOid;
    let mut multirange_typeid = InvalidOid;
    let mut anycompatible_range_typeid = InvalidOid;
    let mut anycompatible_range_typelem = InvalidOid;
    let mut anycompatible_multirange_typeid = InvalidOid;
    let mut anycompatible_multirange_typelem = InvalidOid;
    let mut have_anynonarray = false;
    let mut have_anyenum = false;
    let mut have_anycompatible_nonarray = false;
    // C: Oid anycompatible_actual_types[FUNC_MAX_ARGS].
    let mut anycompatible_actual_types = [InvalidOid; FUNC_MAX_ARGS];
    let mut n_anycompatible_args = 0usize;

    for (&actual, &decl_type) in actual_arg_types.iter().zip(declared_arg_types) {
        let mut actual_type = actual;
        match decl_type {
            ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => {
                if decl_type == ANYNONARRAYOID {
                    have_anynonarray = true;
                } else if decl_type == ANYENUMOID {
                    have_anyenum = true;
                }
                if actual_type == UNKNOWNOID {
                    continue;
                }
                if OidIsValid(elem_typeid) && actual_type != elem_typeid {
                    return Ok(false);
                }
                elem_typeid = actual_type;
            }
            ANYARRAYOID => {
                if actual_type == UNKNOWNOID {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                if OidIsValid(array_typeid) && actual_type != array_typeid {
                    return Ok(false);
                }
                array_typeid = actual_type;
            }
            ANYRANGEOID => {
                if actual_type == UNKNOWNOID {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                if OidIsValid(range_typeid) && actual_type != range_typeid {
                    return Ok(false);
                }
                range_typeid = actual_type;
            }
            ANYMULTIRANGEOID => {
                if actual_type == UNKNOWNOID {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                if OidIsValid(multirange_typeid) && actual_type != multirange_typeid {
                    return Ok(false);
                }
                multirange_typeid = actual_type;
            }
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
                if decl_type == ANYCOMPATIBLENONARRAYOID {
                    have_anycompatible_nonarray = true;
                }
                if actual_type == UNKNOWNOID {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                anycompatible_actual_types[n_anycompatible_args] = actual_type;
                n_anycompatible_args += 1;
            }
            ANYCOMPATIBLEARRAYOID => {
                if actual_type == UNKNOWNOID {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                let elem = lsyscache::get_element_type(actual_type)?;
                if !OidIsValid(elem) {
                    return Ok(false);
                }
                anycompatible_actual_types[n_anycompatible_args] = elem;
                n_anycompatible_args += 1;
            }
            ANYCOMPATIBLERANGEOID => {
                if actual_type == UNKNOWNOID {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                if OidIsValid(anycompatible_range_typeid) {
                    if anycompatible_range_typeid != actual_type {
                        return Ok(false);
                    }
                } else {
                    anycompatible_range_typeid = actual_type;
                    anycompatible_range_typelem = lsyscache::get_range_subtype(actual_type)?;
                    if !OidIsValid(anycompatible_range_typelem) {
                        return Ok(false);
                    }
                    anycompatible_actual_types[n_anycompatible_args] =
                        anycompatible_range_typelem;
                    n_anycompatible_args += 1;
                }
            }
            ANYCOMPATIBLEMULTIRANGEOID => {
                if actual_type == UNKNOWNOID {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                if OidIsValid(anycompatible_multirange_typeid) {
                    if anycompatible_multirange_typeid != actual_type {
                        return Ok(false);
                    }
                } else {
                    anycompatible_multirange_typeid = actual_type;
                    anycompatible_multirange_typelem =
                        lsyscache::get_multirange_range(actual_type)?;
                    if !OidIsValid(anycompatible_multirange_typelem) {
                        return Ok(false);
                    }
                }
            }
            _ => {}
        }
    }

    if OidIsValid(array_typeid) && array_typeid != ANYARRAYOID {
        let array_typelem = lsyscache::get_element_type(array_typeid)?;
        if !OidIsValid(array_typelem) {
            return Ok(false);
        }
        if !OidIsValid(elem_typeid) {
            elem_typeid = array_typelem;
        } else if array_typelem != elem_typeid {
            return Ok(false);
        }
    }

    if OidIsValid(multirange_typeid) {
        let multirange_typelem = lsyscache::get_multirange_range(multirange_typeid)?;
        if !OidIsValid(multirange_typelem) {
            return Ok(false);
        }
        if !OidIsValid(range_typeid) {
            range_typeid = multirange_typelem;
            if !OidIsValid(lsyscache::get_range_subtype(multirange_typelem)?) {
                return Ok(false);
            }
        } else if multirange_typelem != range_typeid {
            return Ok(false);
        }
    }

    if OidIsValid(range_typeid) {
        let range_typelem = lsyscache::get_range_subtype(range_typeid)?;
        if !OidIsValid(range_typelem) {
            return Ok(false);
        }
        if !OidIsValid(elem_typeid) {
            elem_typeid = range_typelem;
        } else if range_typelem != elem_typeid {
            return Ok(false);
        }
    }

    if have_anynonarray && OidIsValid(lsyscache::get_base_element_type(elem_typeid)?) {
        return Ok(false);
    }
    if have_anyenum && !lsyscache::type_is_enum(elem_typeid)? {
        return Ok(false);
    }

    if OidIsValid(anycompatible_multirange_typeid) {
        if OidIsValid(anycompatible_range_typeid) {
            if anycompatible_multirange_typelem != anycompatible_range_typeid {
                return Ok(false);
            }
        } else {
            anycompatible_range_typeid = anycompatible_multirange_typelem;
            anycompatible_range_typelem =
                lsyscache::get_range_subtype(anycompatible_range_typeid)?;
            if !OidIsValid(anycompatible_range_typelem) {
                return Ok(false);
            }
            anycompatible_actual_types[n_anycompatible_args] = anycompatible_range_typelem;
            n_anycompatible_args += 1;
        }
    }

    if n_anycompatible_args > 0 {
        let types = &anycompatible_actual_types[..n_anycompatible_args];
        let anycompatible_typeid = select_common_type_from_oids(types, true)?;
        if !OidIsValid(anycompatible_typeid)
            || !verify_common_type_from_oids(anycompatible_typeid, types)?
        {
            return Ok(false);
        }
        if have_anycompatible_nonarray
            && OidIsValid(lsyscache::get_base_element_type(anycompatible_typeid)?)
        {
            return Ok(false);
        }
        if OidIsValid(anycompatible_range_typelem)
            && anycompatible_range_typelem != anycompatible_typeid
        {
            return Ok(false);
        }
    }

    Ok(true)
}

fn select_common_type_from_oids(typeids: &[Oid], noerror: bool) -> PgResult<Oid> {
    debug_assert!(!typeids.is_empty());
    let mut ptype = typeids[0];
    let mut rest = &typeids[1..];

    if ptype != UNKNOWNOID {
        let mut i = 0;
        while i < rest.len() && rest[i] == ptype {
            i += 1;
        }
        if i == rest.len() {
            return Ok(ptype);
        }
        rest = &rest[i..];
    }

    ptype = lsyscache::getBaseType(ptype)?;
    let (mut pcategory, mut pispreferred) = lsyscache::get_type_category_preferred(ptype)?;

    for &rawtype in rest {
        let ntype = lsyscache::getBaseType(rawtype)?;
        if ntype != UNKNOWNOID && ntype != ptype {
            let (ncategory, nispreferred) = lsyscache::get_type_category_preferred(ntype)?;
            if ptype == UNKNOWNOID {
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            } else if ncategory != pcategory {
                if noerror {
                    return Ok(InvalidOid);
                }
                return Err(argument_types_mismatch(ptype, ntype));
            } else if !pispreferred
                && can_coerce_type(&[ptype], &[ntype], COERCION_IMPLICIT)?
                && !can_coerce_type(&[ntype], &[ptype], COERCION_IMPLICIT)?
            {
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            }
        }
    }

    if ptype == UNKNOWNOID {
        ptype = types_core::catalog::TEXTOID;
    }
    Ok(ptype)
}

fn verify_common_type_from_oids(common_type: Oid, typeids: &[Oid]) -> PgResult<bool> {
    for &t in typeids {
        if t != common_type
            && t != UNKNOWNOID
            && !can_coerce_type(&[t], &[common_type], COERCION_IMPLICIT)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[track_caller]
#[cold]
#[inline(never)]
fn argument_types_mismatch(ptype: Oid, ntype: Oid) -> Box<PgError> {
    let (p, n) = match (format_type::format_type_be(ptype), format_type::format_type_be(ntype)) {
        (Ok(p), Ok(n)) => (p, n),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    Box::new(
        PgError::error(format!("argument types {p} and {n} cannot be matched"))
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
    )
}

pub fn TypeCategory(typid: Oid) -> PgResult<i8> {
    type_category(typid)
}

pub fn IsPreferredType(category: i8, typid: Oid) -> PgResult<bool> {
    let (typcategory, typispreferred) = lsyscache::get_type_category_preferred(typid)?;
    Ok(if category == typcategory || category == TYPCATEGORY_INVALID {
        typispreferred
    } else {
        false
    })
}

pub fn init_seams() {
    fmgr_seams::find_json_cast_func::set(|typoid| {
        let (ptype, castfunc) =
            find_coercion_pathway(types_core::catalog::JSONOID, typoid, COERCION_EXPLICIT)?;
        Ok(match ptype {
            COERCION_PATH_FUNC => castfunc,
            _ => InvalidOid,
        })
    });
}

pub fn find_coercion_pathway(
    targetTypeId: Oid,
    sourceTypeId: Oid,
    ccontext: CoercionContext,
) -> PgResult<(CoercionPathType, Oid)> {
    let mut funcid = InvalidOid;
    let mut result = COERCION_PATH_NONE;

    let sourceTypeId =
        if OidIsValid(sourceTypeId) { lsyscache::getBaseType(sourceTypeId)? } else { sourceTypeId };
    let targetTypeId =
        if OidIsValid(targetTypeId) { lsyscache::getBaseType(targetTypeId)? } else { targetTypeId };

    if sourceTypeId == targetTypeId {
        return Ok((COERCION_PATH_RELABELTYPE, funcid));
    }

    match syscache_seams::lookup_pg_cast_shape::call(sourceTypeId, targetTypeId)? {
        Some(cast) => {
            let castcontext = match cast.castcontext {
                COERCION_CODE_IMPLICIT => COERCION_IMPLICIT,
                COERCION_CODE_ASSIGNMENT => COERCION_ASSIGNMENT,
                COERCION_CODE_EXPLICIT => COERCION_EXPLICIT,
                other => {
                    return Err(Box::new(PgError::error(format!(
                        "unrecognized castcontext: {other}"
                    ))))
                }
            };
            if ccontext >= castcontext {
                match cast.castmethod {
                    COERCION_METHOD_FUNCTION => {
                        result = COERCION_PATH_FUNC;
                        funcid = cast.castfunc;
                    }
                    COERCION_METHOD_INOUT => result = COERCION_PATH_COERCEVIAIO,
                    COERCION_METHOD_BINARY => result = COERCION_PATH_RELABELTYPE,
                    other => {
                        return Err(Box::new(PgError::error(format!(
                            "unrecognized castmethod: {other}"
                        ))))
                    }
                }
            }
        }
        None => {
            if targetTypeId != OIDVECTOROID && targetTypeId != INT2VECTOROID {
                let targetElem = lsyscache::get_element_type(targetTypeId)?;
                let sourceElem = lsyscache::get_element_type(sourceTypeId)?;
                if OidIsValid(targetElem)
                    && OidIsValid(sourceElem)
                    && find_coercion_pathway(targetElem, sourceElem, ccontext)?.0
                        != COERCION_PATH_NONE
                {
                    result = COERCION_PATH_ARRAYCOERCE;
                }
            }
            if result == COERCION_PATH_NONE {
                if ccontext >= COERCION_ASSIGNMENT
                    && type_category(targetTypeId)? == TYPCATEGORY_STRING
                {
                    result = COERCION_PATH_COERCEVIAIO;
                } else if ccontext >= COERCION_EXPLICIT
                    && type_category(sourceTypeId)? == TYPCATEGORY_STRING
                {
                    result = COERCION_PATH_COERCEVIAIO;
                }
            }
        }
    }

    if result == COERCION_PATH_NONE && ccontext == COERCION_PLPGSQL {
        result = COERCION_PATH_COERCEVIAIO;
    }
    Ok((result, funcid))
}

fn type_category(typid: Oid) -> PgResult<i8> {
    Ok(lsyscache::get_type_category_preferred(typid)?.0)
}

pub fn IsBinaryCoercible(srctype: Oid, targettype: Oid) -> PgResult<bool> {
    Ok(IsBinaryCoercibleWithCast(srctype, targettype)?.0)
}

// The oid is the pg_cast row when one decided the answer; InvalidOid for the
// hard-wired rules and for failure.
pub fn IsBinaryCoercibleWithCast(srctype: Oid, targettype: Oid) -> PgResult<(bool, Oid)> {
    if srctype == targettype {
        return Ok((true, InvalidOid));
    }
    if matches!(targettype, ANYOID | ANYELEMENTOID | ANYCOMPATIBLEOID) {
        return Ok((true, InvalidOid));
    }
    let srctype = if OidIsValid(srctype) { lsyscache::getBaseType(srctype)? } else { srctype };
    if srctype == targettype {
        return Ok((true, InvalidOid));
    }
    let type_is_array = |t: Oid| -> PgResult<bool> { Ok(OidIsValid(lsyscache::get_element_type(t)?)) };
    if matches!(targettype, ANYARRAYOID | ANYCOMPATIBLEARRAYOID) && type_is_array(srctype)? {
        return Ok((true, InvalidOid));
    }
    if matches!(targettype, ANYNONARRAYOID | ANYCOMPATIBLENONARRAYOID) && !type_is_array(srctype)? {
        return Ok((true, InvalidOid));
    }
    if targettype == ANYENUMOID && lsyscache::type_is_enum(srctype)? {
        return Ok((true, InvalidOid));
    }
    if matches!(targettype, ANYRANGEOID | ANYCOMPATIBLERANGEOID)
        && lsyscache::type_is_range(srctype)?
    {
        return Ok((true, InvalidOid));
    }
    if matches!(targettype, ANYMULTIRANGEOID | ANYCOMPATIBLEMULTIRANGEOID)
        && lsyscache::type_is_multirange(srctype)?
    {
        return Ok((true, InvalidOid));
    }
    if targettype == RECORDOID && is_complex(srctype)? {
        return Ok((true, InvalidOid));
    }
    if targettype == RECORDARRAYOID && is_complex_array(srctype)? {
        return Ok((true, InvalidOid));
    }
    match syscache_seams::lookup_pg_cast_shape::call(srctype, targettype)? {
        Some(cast)
            if cast.castmethod == COERCION_METHOD_BINARY
                && cast.castcontext == COERCION_CODE_IMPLICIT =>
        {
            Ok((true, cast.oid))
        }
        _ => Ok((false, InvalidOid)),
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn poly_mismatch(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH))
}

#[track_caller]
#[cold]
#[inline(never)]
fn poly_not_alike(what: &str, a: Oid, b: Oid) -> Box<PgError> {
    let (fa, fb) = (
        format_type::format_type_be(a).unwrap_or_else(|_| a.to_string()),
        format_type::format_type_be(b).unwrap_or_else(|_| b.to_string()),
    );
    Box::new(
        PgError::error(format!("arguments declared \"{what}\" are not all alike"))
            .with_detail(format!("{fa} versus {fb}"))
            .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn poly_not_consistent(a_name: &str, b_name: &str, a: Oid, b: Oid) -> Box<PgError> {
    let (fa, fb) = (
        format_type::format_type_be(a).unwrap_or_else(|_| a.to_string()),
        format_type::format_type_be(b).unwrap_or_else(|_| b.to_string()),
    );
    Box::new(
        PgError::error(format!(
            "argument declared {a_name} is not consistent with argument declared {b_name}"
        ))
        .with_detail(format!("{fa} versus {fb}"))
        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn no_array_type_for(elem: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "could not find array type for data type {}",
            format_type::format_type_be(elem).unwrap_or_else(|_| elem.to_string())
        ))
        .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
    )
}

/// C `enforce_generic_type_consistency` (parse_coerce.c).
pub fn enforce_generic_type_consistency(
    actual_arg_types: &[Oid],
    declared_arg_types: &mut [Oid],
    rettype: Oid,
    allow_poly: bool,
) -> PgResult<Oid> {
    let nargs = declared_arg_types.len();
    let mut have_poly_anycompatible = false;
    let mut have_poly_unknowns = false;
    let mut elem_typeid = InvalidOid;
    let mut array_typeid = InvalidOid;
    let mut range_typeid = InvalidOid;
    let mut multirange_typeid = InvalidOid;
    let anycompatible_typeid;
    let mut anycompatible_array_typeid = InvalidOid;
    let mut anycompatible_range_typeid = InvalidOid;
    let mut anycompatible_range_typelem = InvalidOid;
    let mut anycompatible_multirange_typeid = InvalidOid;
    let mut anycompatible_multirange_typelem = InvalidOid;
    let mut have_anynonarray = rettype == ANYNONARRAYOID;
    let mut have_anyenum = rettype == ANYENUMOID;
    let mut have_anymultirange = rettype == ANYMULTIRANGEOID;
    let mut have_anycompatible_nonarray = rettype == ANYCOMPATIBLENONARRAYOID;
    let mut have_anycompatible_array = rettype == ANYCOMPATIBLEARRAYOID;
    let mut have_anycompatible_range = rettype == ANYCOMPATIBLERANGEOID;
    let mut have_anycompatible_multirange = rettype == ANYCOMPATIBLEMULTIRANGEOID;
    let mut n_poly_args = 0;
    let mut n_anycompatible_args = 0usize;
    // C: Oid anycompatible_actual_types[FUNC_MAX_ARGS].
    let mut anycompatible_actual_types = [InvalidOid; FUNC_MAX_ARGS];

    for j in 0..nargs {
        let decl_type = declared_arg_types[j];
        let mut actual_type = actual_arg_types[j];
        match decl_type {
            ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => {
                n_poly_args += 1;
                if decl_type == ANYNONARRAYOID {
                    have_anynonarray = true;
                } else if decl_type == ANYENUMOID {
                    have_anyenum = true;
                }
                if actual_type == UNKNOWNOID {
                    have_poly_unknowns = true;
                    continue;
                }
                if allow_poly && decl_type == actual_type {
                    continue;
                }
                if OidIsValid(elem_typeid) && actual_type != elem_typeid {
                    return Err(poly_not_alike("anyelement", elem_typeid, actual_type));
                }
                elem_typeid = actual_type;
            }
            ANYARRAYOID => {
                n_poly_args += 1;
                if actual_type == UNKNOWNOID {
                    have_poly_unknowns = true;
                    continue;
                }
                if allow_poly && decl_type == actual_type {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                if OidIsValid(array_typeid) && actual_type != array_typeid {
                    return Err(poly_not_alike("anyarray", array_typeid, actual_type));
                }
                array_typeid = actual_type;
            }
            ANYRANGEOID => {
                n_poly_args += 1;
                if actual_type == UNKNOWNOID {
                    have_poly_unknowns = true;
                    continue;
                }
                if allow_poly && decl_type == actual_type {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                if OidIsValid(range_typeid) && actual_type != range_typeid {
                    return Err(poly_not_alike("anyrange", range_typeid, actual_type));
                }
                range_typeid = actual_type;
            }
            ANYMULTIRANGEOID => {
                n_poly_args += 1;
                have_anymultirange = true;
                if actual_type == UNKNOWNOID {
                    have_poly_unknowns = true;
                    continue;
                }
                if allow_poly && decl_type == actual_type {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                if OidIsValid(multirange_typeid) && actual_type != multirange_typeid {
                    return Err(poly_not_alike(
                        "anymultirange",
                        multirange_typeid,
                        actual_type,
                    ));
                }
                multirange_typeid = actual_type;
            }
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
                have_poly_anycompatible = true;
                if decl_type == ANYCOMPATIBLENONARRAYOID {
                    have_anycompatible_nonarray = true;
                }
                if actual_type == UNKNOWNOID {
                    continue;
                }
                if allow_poly && decl_type == actual_type {
                    continue;
                }
                anycompatible_actual_types[n_anycompatible_args] = actual_type;
                n_anycompatible_args += 1;
            }
            ANYCOMPATIBLEARRAYOID => {
                have_poly_anycompatible = true;
                have_anycompatible_array = true;
                if actual_type == UNKNOWNOID {
                    continue;
                }
                if allow_poly && decl_type == actual_type {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                let elem = lsyscache::get_element_type(actual_type)?;
                if !OidIsValid(elem) {
                    return Err(poly_mismatch(format!(
                        "argument declared anycompatiblearray is not an array but type {}",
                        format_type::format_type_be(actual_type)?
                    )));
                }
                anycompatible_actual_types[n_anycompatible_args] = elem;
                n_anycompatible_args += 1;
            }
            ANYCOMPATIBLERANGEOID => {
                have_poly_anycompatible = true;
                have_anycompatible_range = true;
                if actual_type == UNKNOWNOID {
                    continue;
                }
                if allow_poly && decl_type == actual_type {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                if OidIsValid(anycompatible_range_typeid) {
                    if anycompatible_range_typeid != actual_type {
                        return Err(poly_not_alike(
                            "anycompatiblerange",
                            anycompatible_range_typeid,
                            actual_type,
                        ));
                    }
                } else {
                    anycompatible_range_typeid = actual_type;
                    anycompatible_range_typelem = lsyscache::get_range_subtype(actual_type)?;
                    if !OidIsValid(anycompatible_range_typelem) {
                        return Err(poly_mismatch(format!(
                            "argument declared anycompatiblerange is not a range type but \
                             type {}",
                            format_type::format_type_be(actual_type)?
                        )));
                    }
                    anycompatible_actual_types[n_anycompatible_args] =
                        anycompatible_range_typelem;
                    n_anycompatible_args += 1;
                }
            }
            ANYCOMPATIBLEMULTIRANGEOID => {
                have_poly_anycompatible = true;
                have_anycompatible_multirange = true;
                if actual_type == UNKNOWNOID {
                    continue;
                }
                if allow_poly && decl_type == actual_type {
                    continue;
                }
                actual_type = lsyscache::getBaseType(actual_type)?;
                if OidIsValid(anycompatible_multirange_typeid) {
                    if anycompatible_multirange_typeid != actual_type {
                        return Err(poly_not_alike(
                            "anycompatiblemultirange",
                            anycompatible_multirange_typeid,
                            actual_type,
                        ));
                    }
                } else {
                    anycompatible_multirange_typeid = actual_type;
                    anycompatible_multirange_typelem =
                        lsyscache::get_multirange_range(actual_type)?;
                    if !OidIsValid(anycompatible_multirange_typelem) {
                        return Err(poly_mismatch(format!(
                            "argument declared anycompatiblemultirange is not a multirange \
                             type but type {}",
                            format_type::format_type_be(actual_type)?
                        )));
                    }
                }
            }
            _ => {}
        }
    }

    if n_poly_args == 0 && !have_poly_anycompatible {
        return Ok(rettype);
    }

    if n_poly_args > 0 {
        if OidIsValid(array_typeid) {
            let array_typelem;
            if array_typeid == ANYARRAYOID {
                if n_poly_args != 1
                    || (rettype != ANYARRAYOID && is_polymorphic_type_family1(rettype))
                {
                    return Err(poly_mismatch(
                        "cannot determine element type of \"anyarray\" argument".to_string(),
                    ));
                }
                array_typelem = ANYELEMENTOID;
            } else {
                array_typelem = lsyscache::get_element_type(array_typeid)?;
                if !OidIsValid(array_typelem) {
                    return Err(poly_mismatch(format!(
                        "argument declared anyarray is not an array but type {}",
                        format_type::format_type_be(array_typeid)?
                    )));
                }
            }
            if !OidIsValid(elem_typeid) {
                elem_typeid = array_typelem;
            } else if array_typelem != elem_typeid {
                return Err(poly_not_consistent(
                    "anyarray",
                    "anyelement",
                    array_typeid,
                    elem_typeid,
                ));
            }
        }

        if OidIsValid(multirange_typeid) {
            let multirange_typelem = lsyscache::get_multirange_range(multirange_typeid)?;
            if !OidIsValid(multirange_typelem) {
                return Err(poly_mismatch(format!(
                    "argument declared anymultirange is not a multirange type but type {}",
                    format_type::format_type_be(multirange_typeid)?
                )));
            }
            if !OidIsValid(range_typeid) {
                range_typeid = multirange_typelem;
            } else if multirange_typelem != range_typeid {
                return Err(poly_not_consistent(
                    "anymultirange",
                    "anyrange",
                    multirange_typeid,
                    range_typeid,
                ));
            }
        } else if have_anymultirange && OidIsValid(range_typeid) {
            multirange_typeid = lsyscache::get_range_multirange(range_typeid)?;
        }

        if OidIsValid(range_typeid) {
            let range_typelem = lsyscache::get_range_subtype(range_typeid)?;
            if !OidIsValid(range_typelem) {
                return Err(poly_mismatch(format!(
                    "argument declared anyrange is not a range type but type {}",
                    format_type::format_type_be(range_typeid)?
                )));
            }
            if !OidIsValid(elem_typeid) {
                elem_typeid = range_typelem;
            } else if range_typelem != elem_typeid {
                return Err(poly_not_consistent(
                    "anyrange",
                    "anyelement",
                    range_typeid,
                    elem_typeid,
                ));
            }
        }

        if !OidIsValid(elem_typeid) {
            if allow_poly {
                elem_typeid = ANYELEMENTOID;
                array_typeid = ANYARRAYOID;
                range_typeid = ANYRANGEOID;
                multirange_typeid = ANYMULTIRANGEOID;
            } else {
                return Err(poly_mismatch(
                    "could not determine polymorphic type because input has type unknown"
                        .to_string(),
                ));
            }
        }

        if have_anynonarray
            && elem_typeid != ANYELEMENTOID
            && OidIsValid(lsyscache::get_base_element_type(elem_typeid)?)
        {
            return Err(poly_mismatch(format!(
                "type matched to anynonarray is an array type: {}",
                format_type::format_type_be(elem_typeid)?
            )));
        }
        if have_anyenum
            && elem_typeid != ANYELEMENTOID
            && !lsyscache::type_is_enum(elem_typeid)?
        {
            return Err(poly_mismatch(format!(
                "type matched to anyenum is not an enum type: {}",
                format_type::format_type_be(elem_typeid)?
            )));
        }
    }

    if have_poly_anycompatible {
        if OidIsValid(anycompatible_multirange_typeid) {
            if OidIsValid(anycompatible_range_typeid) {
                if anycompatible_multirange_typelem != anycompatible_range_typeid {
                    return Err(poly_not_consistent(
                        "anycompatiblemultirange",
                        "anycompatiblerange",
                        anycompatible_multirange_typeid,
                        anycompatible_range_typeid,
                    ));
                }
            } else {
                anycompatible_range_typeid = anycompatible_multirange_typelem;
                anycompatible_range_typelem =
                    lsyscache::get_range_subtype(anycompatible_range_typeid)?;
                if !OidIsValid(anycompatible_range_typelem) {
                    return Err(poly_mismatch(format!(
                        "argument declared anycompatiblemultirange is not a multirange type \
                         but type {}",
                        format_type::format_type_be(anycompatible_multirange_typeid)?
                    )));
                }
                have_anycompatible_range = true;
                anycompatible_actual_types[n_anycompatible_args] =
                    anycompatible_range_typelem;
                n_anycompatible_args += 1;
            }
        } else if have_anycompatible_multirange && OidIsValid(anycompatible_range_typeid) {
            anycompatible_multirange_typeid =
                lsyscache::get_range_multirange(anycompatible_range_typeid)?;
        }

        if n_anycompatible_args > 0 {
            let types = &anycompatible_actual_types[..n_anycompatible_args];
            anycompatible_typeid = select_common_type_from_oids(types, false)?;
            if !verify_common_type_from_oids(anycompatible_typeid, types)? {
                return Err(poly_mismatch(
                    "arguments of anycompatible family cannot be cast to a common type"
                        .to_string(),
                ));
            }
            if have_anycompatible_array {
                anycompatible_array_typeid = lsyscache::get_array_type(anycompatible_typeid)?;
                if !OidIsValid(anycompatible_array_typeid) {
                    return Err(no_array_type_for(anycompatible_typeid));
                }
            }
            if have_anycompatible_range {
                if !OidIsValid(anycompatible_range_typeid) {
                    return Err(poly_mismatch(
                        "could not determine polymorphic type anycompatiblerange because \
                         input has type unknown"
                            .to_string(),
                    ));
                }
                if anycompatible_range_typelem != anycompatible_typeid {
                    return Err(poly_mismatch(format!(
                        "anycompatiblerange type {} does not match anycompatible type {}",
                        format_type::format_type_be(anycompatible_range_typeid)?,
                        format_type::format_type_be(anycompatible_typeid)?
                    )));
                }
            }
            if have_anycompatible_multirange {
                if !OidIsValid(anycompatible_multirange_typeid) {
                    return Err(poly_mismatch(
                        "could not determine polymorphic type anycompatiblemultirange \
                         because input has type unknown"
                            .to_string(),
                    ));
                }
                if anycompatible_range_typelem != anycompatible_typeid {
                    return Err(poly_mismatch(format!(
                        "anycompatiblemultirange type {} does not match anycompatible type {}",
                        format_type::format_type_be(anycompatible_multirange_typeid)?,
                        format_type::format_type_be(anycompatible_typeid)?
                    )));
                }
            }
            if have_anycompatible_nonarray
                && OidIsValid(lsyscache::get_base_element_type(anycompatible_typeid)?)
            {
                return Err(poly_mismatch(format!(
                    "type matched to anycompatiblenonarray is an array type: {}",
                    format_type::format_type_be(anycompatible_typeid)?
                )));
            }
        } else if allow_poly {
            anycompatible_typeid = ANYCOMPATIBLEOID;
            anycompatible_array_typeid = ANYCOMPATIBLEARRAYOID;
            anycompatible_range_typeid = ANYCOMPATIBLERANGEOID;
            anycompatible_multirange_typeid = ANYCOMPATIBLEMULTIRANGEOID;
        } else {
            anycompatible_typeid = types_core::catalog::TEXTOID;
            anycompatible_array_typeid = lsyscache::get_array_type(anycompatible_typeid)?;
            if have_anycompatible_range {
                return Err(poly_mismatch(
                    "could not determine polymorphic type anycompatiblerange because input \
                     has type unknown"
                        .to_string(),
                ));
            }
            if have_anycompatible_multirange {
                return Err(poly_mismatch(
                    "could not determine polymorphic type anycompatiblemultirange because \
                     input has type unknown"
                        .to_string(),
                ));
            }
        }

        for j in 0..nargs {
            match declared_arg_types[j] {
                ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
                    declared_arg_types[j] = anycompatible_typeid;
                }
                ANYCOMPATIBLEARRAYOID => declared_arg_types[j] = anycompatible_array_typeid,
                ANYCOMPATIBLERANGEOID => declared_arg_types[j] = anycompatible_range_typeid,
                ANYCOMPATIBLEMULTIRANGEOID => {
                    declared_arg_types[j] = anycompatible_multirange_typeid;
                }
                _ => {}
            }
        }
    } else {
        anycompatible_typeid = InvalidOid;
    }

    if have_poly_unknowns {
        for j in 0..nargs {
            if actual_arg_types[j] != UNKNOWNOID {
                continue;
            }
            match declared_arg_types[j] {
                ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => {
                    declared_arg_types[j] = elem_typeid;
                }
                ANYARRAYOID => {
                    if !OidIsValid(array_typeid) {
                        array_typeid = lsyscache::get_array_type(elem_typeid)?;
                        if !OidIsValid(array_typeid) {
                            return Err(no_array_type_for(elem_typeid));
                        }
                    }
                    declared_arg_types[j] = array_typeid;
                }
                ANYRANGEOID => {
                    if !OidIsValid(range_typeid) {
                        return Err(poly_mismatch(
                            "could not determine polymorphic type anyrange because input \
                             has type unknown"
                                .to_string(),
                        ));
                    }
                    declared_arg_types[j] = range_typeid;
                }
                ANYMULTIRANGEOID => {
                    if !OidIsValid(multirange_typeid) {
                        return Err(poly_mismatch(
                            "could not determine polymorphic type anymultirange because \
                             input has type unknown"
                                .to_string(),
                        ));
                    }
                    declared_arg_types[j] = multirange_typeid;
                }
                _ => {}
            }
        }
    }

    match rettype {
        ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => Ok(elem_typeid),
        ANYARRAYOID => {
            if !OidIsValid(array_typeid) {
                array_typeid = lsyscache::get_array_type(elem_typeid)?;
                if !OidIsValid(array_typeid) {
                    return Err(no_array_type_for(elem_typeid));
                }
            }
            Ok(array_typeid)
        }
        ANYRANGEOID => {
            assert!(OidIsValid(range_typeid), "could not determine anyrange result");
            Ok(range_typeid)
        }
        ANYMULTIRANGEOID => {
            assert!(OidIsValid(multirange_typeid), "could not determine anymultirange result");
            Ok(multirange_typeid)
        }
        ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
            assert!(OidIsValid(anycompatible_typeid), "could not identify anycompatible type");
            Ok(anycompatible_typeid)
        }
        ANYCOMPATIBLEARRAYOID => {
            assert!(
                OidIsValid(anycompatible_array_typeid),
                "could not identify anycompatiblearray type"
            );
            Ok(anycompatible_array_typeid)
        }
        ANYCOMPATIBLERANGEOID => {
            assert!(
                OidIsValid(anycompatible_range_typeid),
                "could not identify anycompatiblerange type"
            );
            Ok(anycompatible_range_typeid)
        }
        ANYCOMPATIBLEMULTIRANGEOID => {
            assert!(
                OidIsValid(anycompatible_multirange_typeid),
                "could not identify anycompatiblemultirange type"
            );
            Ok(anycompatible_multirange_typeid)
        }
        _ => Ok(rettype),
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn conversion_not_found(inputTypeId: Oid, targetTypeId: Oid) -> Box<PgError> {
    let src = format_type::format_type_be(inputTypeId).unwrap_or_else(|_| inputTypeId.to_string());
    let dst =
        format_type::format_type_be(targetTypeId).unwrap_or_else(|_| targetTypeId.to_string());
    Box::new(PgError::error(format!(
        "failed to find conversion function from {src} to {dst}"
    )))
}

#[track_caller]
#[cold]
#[inline(never)]
fn type_lookup_failed(typid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for type {typid}")))
}

fn build_coercion_expression<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    pathtype: CoercionPathType,
    funcId: Oid,
    targetTypeId: Oid,
    targetTypMod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    let mut nargs: i16 = 0;
    if OidIsValid(funcId) {
        let Some(procs) = syscache_seams::lookup_pg_proc_shape::call(funcId)? else {
            return Err(function_lookup_failed(funcId));
        };
        debug_assert!(!procs.proretset && procs.prokind == b'f' as i8);
        nargs = procs.pronargs;
        debug_assert!((1..=3).contains(&nargs));
    }

    match pathtype {
        COERCION_PATH_FUNC => {
            debug_assert!(OidIsValid(funcId));
            let mut args = NodeList::make1(mcx, node)?;
            if nargs >= 2 {
                let typmod_const = Const {
                    consttype: INT4OID,
                    consttypmod: -1,
                    constcollid: InvalidOid,
                    constlen: 4,
                    constvalue: Datum::from_i32(targetTypMod),
                    constisnull: false,
                    constbyval: true,
                    location: -1,
                };
                args.lappend(mcx, Node::mk(mcx, typmod_const)?)?;
            }
            if nargs == 3 {
                let explicit_const = Const {
                    consttype: BOOLOID,
                    consttypmod: -1,
                    constcollid: InvalidOid,
                    constlen: 1,
                    constvalue: Datum::from_bool(ccontext == COERCION_EXPLICIT),
                    constisnull: false,
                    constbyval: true,
                    location: -1,
                };
                args.lappend(mcx, Node::mk(mcx, explicit_const)?)?;
            }
            Node::mk(
                mcx,
                FuncExpr {
                    funcid: funcId,
                    funcresulttype: targetTypeId,
                    funcretset: false,
                    funcvariadic: false,
                    funcformat: cformat,
                    funccollid: InvalidOid,
                    inputcollid: InvalidOid,
                    args,
                    location,
                },
            )
        }
        COERCION_PATH_ARRAYCOERCE => {
            // Look through any domain over the source array type; the target
            // is a plain array (a domain target reaches coerce_to_domain
            // later). The CaseTestExpr stands in for one source element —
            // safe because the finished elemexpr can contain no CaseExpr or
            // ArrayCoerceExpr.
            let mut sourceBaseTypeMod = expr_typmod(node);
            let sourceBaseTypeId =
                lsyscache::getBaseTypeAndTypmod(expr_type(node), &mut sourceBaseTypeMod)?;
            let ctest_type = lsyscache::get_element_type(sourceBaseTypeId)?;
            debug_assert!(OidIsValid(ctest_type));
            let ctest = Node::mk(
                mcx,
                CaseTestExpr {
                    typeId: ctest_type,
                    typeMod: sourceBaseTypeMod,
                    collation: InvalidOid,
                },
            )?;
            let targetElementType = lsyscache::get_element_type(targetTypeId)?;
            debug_assert!(OidIsValid(targetElementType));
            // C passes a NULL pstate for the elemexpr coercion.
            let pstate = parser_small1::make_parsestate(mcx, Option::None);
            let elemexpr = coerce_to_target_type(
                mcx,
                &pstate,
                ctest,
                ctest_type,
                targetElementType,
                targetTypMod,
                ccontext,
                cformat,
                location,
            )?
            .ok_or_else(|| {
                Box::new(PgError::error(
                    "failed to coerce array element type as expected".to_string(),
                ))
            })?;
            Node::mk(
                mcx,
                ArrayCoerceExpr {
                    arg: node,
                    elemexpr: Some(elemexpr),
                    resulttype: targetTypeId,
                    resulttypmod: expr_typmod(elemexpr),
                    resultcollid: InvalidOid,
                    coerceformat: cformat,
                    location,
                },
            )
        }
        COERCION_PATH_COERCEVIAIO => {
            debug_assert!(!OidIsValid(funcId));
            Node::mk(
                mcx,
                CoerceViaIO {
                    arg: node,
                    resulttype: targetTypeId,
                    resultcollid: InvalidOid,
                    coerceformat: cformat,
                    location,
                },
            )
        }
        _ => Err(Box::new(PgError::error(format!(
            "unsupported pathtype {pathtype:?} in build_coercion_expression"
        )))),
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn function_lookup_failed(funcid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for function {funcid}")))
}

/// C coerce_to_domain (parse_coerce.c:675). resultcollid starts InvalidOid;
/// parse_collate assigns it.
#[allow(clippy::too_many_arguments)]
pub fn coerce_to_domain<'mcx>(
    mcx: Mcx<'mcx>,
    arg: Node<'mcx>,
    baseTypeId: Oid,
    baseTypeMod: i32,
    typeId: Oid,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: ParseLoc,
    hideInputCoercion: bool,
) -> PgResult<Node<'mcx>> {
    debug_assert!(OidIsValid(baseTypeId));
    if baseTypeId == typeId {
        return Ok(arg);
    }
    if hideInputCoercion {
        hide_coercion_node(arg);
    }
    let arg = coerce_type_typmod(
        mcx,
        arg,
        baseTypeId,
        baseTypeMod,
        ccontext,
        CoercionForm::COERCE_IMPLICIT_CAST,
        location,
        false,
    )?;
    Node::mk(
        mcx,
        CoerceToDomain {
            arg,
            resulttype: typeId,
            resulttypmod: -1,
            resultcollid: InvalidOid,
            coercionformat: cformat,
            location,
        },
    )
}

// coerce_null_to_domain (parse_coerce.c): NULL Const of the base type,
// CoerceToDomain-wrapped so runtime NOT NULL/CHECK constraints fire.
pub fn coerce_null_to_domain<'mcx>(
    mcx: Mcx<'mcx>,
    typid: Oid,
    typmod: i32,
    collation: Oid,
    typlen: i32,
    typbyval: bool,
) -> PgResult<Node<'mcx>> {
    let mut base_type_mod = typmod;
    let base_type_id = lsyscache::getBaseTypeAndTypmod(typid, &mut base_type_mod)?;
    let result = Node::mk_const(
        mcx,
        base_type_id,
        base_type_mod,
        collation,
        typlen,
        datum::Datum::null(),
        true,
        typbyval,
    )?;
    if typid != base_type_id {
        return coerce_to_domain(
            mcx,
            result,
            base_type_id,
            base_type_mod,
            typid,
            CoercionContext::COERCION_IMPLICIT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
            false,
        );
    }
    Ok(result)
}

/// Divergences from C: caller passes exprType(expr) (the nodeFuncs slice
/// lives in parse_expr — parse_oper precedent); Ok(None) is C's NULL return.
#[allow(clippy::too_many_arguments)]
pub fn coerce_to_target_type<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
    exprtype: Oid,
    targettype: Oid,
    targettypmod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: ParseLoc,
) -> PgResult<Option<Node<'mcx>>> {
    if !can_coerce_type(&[exprtype], &[targettype], ccontext)? {
        return Ok(None);
    }
    // C: strip ALL stacked CollateExprs, coerce, and reinstall only the
    // topmost — and only when the target type is collatable.
    let mut inner = expr;
    while inner.node_tag() == NodeTag::T_CollateExpr {
        inner = inner.as_collate_expr().unwrap().arg;
    }
    let result = coerce_type(
        mcx, pstate, inner, exprtype, targettype, targettypmod, ccontext, cformat, location,
    )?;
    // Hide only nodes coerce_type generated: it can return the input unchanged
    // (unknown Params are retyped in place), and hide_coercion_node must never
    // run on a node it did not wrap — identity, not type inequality.
    let hide = !result.ptr_eq(inner) && result.as_variant::<Const>().is_none();
    let result = coerce_type_typmod(
        mcx, result, targettype, targettypmod, ccontext, cformat, location, hide,
    )?;
    if expr.node_tag() == NodeTag::T_CollateExpr && lsyscache::type_is_collatable(targettype)? {
        let coll = expr.as_collate_expr().unwrap();
        return Ok(Some(Node::mk(
            mcx,
            CollateExpr { arg: result, collOid: coll.collOid, location: coll.location },
        )?));
    }
    Ok(Some(result))
}

fn hide_coercion_node(node: Node<'_>) {
    // SAFETY: parse tree is analyze-owned; no derived refs live.
    unsafe {
        if node
            .with_mut::<FuncExpr, _>(|f| f.funcformat = CoercionForm::COERCE_IMPLICIT_CAST)
            .is_some()
            || node
                .with_mut::<RelabelType, _>(|r| {
                    r.relabelformat = CoercionForm::COERCE_IMPLICIT_CAST
                })
                .is_some()
            || node
                .with_mut::<CoerceViaIO, _>(|c| {
                    c.coerceformat = CoercionForm::COERCE_IMPLICIT_CAST
                })
                .is_some()
            || node
                .with_mut::<ArrayCoerceExpr, _>(|a| {
                    a.coerceformat = CoercionForm::COERCE_IMPLICIT_CAST
                })
                .is_some()
            || node
                .with_mut::<ConvertRowtypeExpr, _>(|c| {
                    c.convertformat = CoercionForm::COERCE_IMPLICIT_CAST
                })
                .is_some()
            || node
                .with_mut::<CoerceToDomain, _>(|c| {
                    c.coercionformat = CoercionForm::COERCE_IMPLICIT_CAST
                })
                .is_some()
        {
            return;
        }
    }
    panic!("unsupported node type in hide_coercion_node: {:?}", node.node_tag());
}

// find_typmod_coercion_function (parse_coerce.c): a true array type's length
// coercion applies the element type's function through ArrayCoerceExpr.
fn find_typmod_coercion_function(typeId: Oid) -> PgResult<(CoercionPathType, Oid)> {
    let Some(el) = syscache_seams::pg_type_element_shape::call(typeId)? else {
        return Err(type_lookup_failed(typeId));
    };
    let (typeId, mut result) = if lsyscache::is_true_array_type(el.typelem, el.typsubscript) {
        (el.typelem, COERCION_PATH_ARRAYCOERCE)
    } else {
        (typeId, COERCION_PATH_FUNC)
    };
    let funcid = match syscache_seams::lookup_pg_cast_shape::call(typeId, typeId)? {
        Some(cast) => cast.castfunc,
        None => InvalidOid,
    };
    if !OidIsValid(funcid) {
        result = COERCION_PATH_NONE;
    }
    Ok((result, funcid))
}

#[allow(clippy::too_many_arguments)]
fn coerce_type_typmod<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    targetTypeId: Oid,
    targetTypMod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: ParseLoc,
    hideInputCoercion: bool,
) -> PgResult<Node<'mcx>> {
    if targetTypMod == expr_typmod(node) {
        return Ok(node);
    }

    if hideInputCoercion {
        hide_coercion_node(node);
    }

    // A negative typmod needs no coercion step, but C still applies a
    // RelabelType so the expression exposes the intended typmod.
    let (pathtype, funcId) = if targetTypMod < 0 {
        (COERCION_PATH_NONE, InvalidOid)
    } else {
        find_typmod_coercion_function(targetTypeId)?
    };
    if pathtype == COERCION_PATH_NONE {
        return nodes_core::node_funcs::apply_relabel_type(
            mcx,
            node,
            targetTypeId,
            targetTypMod,
            nodes_core::node_funcs::expr_collation(node),
            cformat,
            location,
        );
    }

    build_coercion_expression(
        mcx, node, pathtype, funcId, targetTypeId, targetTypMod, ccontext, cformat, location,
    )
}

/// Divergence from C: caller passes exprType/exprLocation of `node` (the
/// nodeFuncs slice lives in parse_expr — parse_oper precedent).
pub fn coerce_to_boolean<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    input_type_id: Oid,
    node_location: ParseLoc,
    construct_name: &str,
) -> PgResult<Node<'mcx>> {
    let node = if input_type_id != BOOLOID {
        match coerce_to_target_type(
            mcx,
            pstate,
            node,
            input_type_id,
            BOOLOID,
            -1,
            COERCION_ASSIGNMENT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )? {
            Some(newnode) => newnode,
            None => {
                return Err(construct_type_mismatch(
                    pstate,
                    construct_name,
                    BOOLOID,
                    input_type_id,
                    node_location,
                ))
            }
        }
    } else {
        node
    };

    if expression_returns_set(node) {
        return Err(returns_set(pstate, construct_name, node_location));
    }
    Ok(node)
}

/// C `coerce_to_specific_type` (typmod -1); same precomputed exprType/
/// exprLocation divergence as [`coerce_to_boolean`].
pub fn coerce_to_specific_type<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    input_type_id: Oid,
    node_location: ParseLoc,
    target_type_id: Oid,
    construct_name: &str,
) -> PgResult<Node<'mcx>> {
    let node = if input_type_id != target_type_id {
        match coerce_to_target_type(
            mcx,
            pstate,
            node,
            input_type_id,
            target_type_id,
            -1,
            COERCION_ASSIGNMENT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )? {
            Some(newnode) => newnode,
            None => {
                return Err(construct_type_mismatch(
                    pstate,
                    construct_name,
                    target_type_id,
                    input_type_id,
                    node_location,
                ))
            }
        }
    } else {
        node
    };

    if expression_returns_set(node) {
        return Err(returns_set(pstate, construct_name, node_location));
    }
    Ok(node)
}

// Closed-set slice of nodeFuncs.c expression_returns_set over the tags this
// parser lane can produce.
pub fn expression_returns_set(node: Node<'_>) -> bool {
    match node.node_tag() {
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            f.funcretset || f.args.iter().any(expression_returns_set)
        }
        NodeTag::T_OpExpr => {
            let op = node.as_op_expr().unwrap();
            op.opretset || op.args.iter().any(expression_returns_set)
        }
        NodeTag::T_BoolExpr => {
            node.as_bool_expr().unwrap().args.iter().any(expression_returns_set)
        }
        NodeTag::T_ScalarArrayOpExpr => {
            node.as_scalar_array_op_expr().unwrap().args.iter().any(expression_returns_set)
        }
        NodeTag::T_ArrayExpr => {
            node.as_array_expr().unwrap().elements.iter().any(expression_returns_set)
        }
        NodeTag::T_RelabelType => expression_returns_set(node.as_relabel_type().unwrap().arg),
        NodeTag::T_CoerceViaIO => expression_returns_set(node.as_coerce_via_io().unwrap().arg),
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            expression_returns_set(a.arg) || a.elemexpr.is_some_and(expression_returns_set)
        }
        NodeTag::T_ConvertRowtypeExpr => {
            expression_returns_set(node.as_convert_rowtype_expr().unwrap().arg)
        }
        NodeTag::T_CollateExpr => expression_returns_set(node.as_collate_expr().unwrap().arg),
        NodeTag::T_CoalesceExpr => {
            node.as_coalesce_expr().unwrap().args.iter().any(expression_returns_set)
        }
        NodeTag::T_MinMaxExpr => {
            node.as_min_max_expr().unwrap().args.iter().any(expression_returns_set)
        }
        NodeTag::T_SQLValueFunction => false,
        NodeTag::T_NullTest => {
            node.as_null_test().unwrap().arg.is_some_and(expression_returns_set)
        }
        NodeTag::T_BooleanTest => {
            node.as_boolean_test().unwrap().arg.is_some_and(expression_returns_set)
        }
        // C's walker recurses DistinctExpr/NullIfExpr generically (no
        // opretset probe).
        NodeTag::T_DistinctExpr => {
            node.as_distinct_expr().unwrap().args.iter().any(expression_returns_set)
        }
        NodeTag::T_NullIfExpr => {
            node.as_null_if_expr().unwrap().args.iter().any(expression_returns_set)
        }
        NodeTag::T_RowExpr => {
            node.as_row_expr().unwrap().args.iter().any(expression_returns_set)
        }
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            rc.largs.iter().any(expression_returns_set)
                || rc.rargs.iter().any(expression_returns_set)
        }
        NodeTag::T_FieldSelect => expression_returns_set(node.as_field_select().unwrap().arg),
        // expression_tree_walker (nodeFuncs.c) recurses into phexpr; planner
        // callers (make_sort et al.) can see PlaceHolderVars.
        NodeTag::T_PlaceHolderVar => {
            expression_returns_set(node.as_place_holder_var().unwrap().phexpr)
        }
        NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_Var
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_MergeSupportFunc
        | NodeTag::T_CoerceToDomainValue => false,
        NodeTag::T_CoerceToDomain => {
            expression_returns_set(node.as_coerce_to_domain().unwrap().arg)
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            c.arg.is_some_and(expression_returns_set)
                || c.args.iter().any(expression_returns_set)
                || c.defresult.is_some_and(expression_returns_set)
        }
        NodeTag::T_CaseWhen => {
            let cw = node.as_case_when().unwrap();
            cw.expr.is_some_and(expression_returns_set)
                || cw.result.is_some_and(expression_returns_set)
        }
        // C short-circuits these before recursing (parser guarantees no SRF).
        NodeTag::T_Aggref | NodeTag::T_GroupingFunc | NodeTag::T_WindowFunc => false,
        // SubLink is not set-returning; C's walker does not enter subselects.
        NodeTag::T_SubLink => {
            node.as_sub_link().unwrap().testexpr.is_some_and(expression_returns_set)
        }
        // expression_tree_walker (nodeFuncs.c:2245-2256): recurse into
        // testexpr and args, but not into the Plan.
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            sp.testexpr.is_some_and(expression_returns_set)
                || sp.args.iter().any(expression_returns_set)
        }
        // expression_tree_walker (nodeFuncs.c:2257-2258).
        NodeTag::T_AlternativeSubPlan => {
            node.as_alternative_sub_plan().unwrap().subplans.iter().any(expression_returns_set)
        }
        NodeTag::T_ScalarArrayOpExpr => {
            node.as_scalar_array_op_expr().unwrap().args.iter().any(expression_returns_set)
        }
        NodeTag::T_ArrayExpr => {
            node.as_array_expr().unwrap().elements.iter().any(expression_returns_set)
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            sr.refupperindexpr.iter().flatten().any(expression_returns_set)
                || sr.reflowerindexpr.iter().flatten().any(expression_returns_set)
                || sr.refexpr.is_some_and(expression_returns_set)
                || sr.refassgnexpr.is_some_and(expression_returns_set)
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            j.raw_expr.is_some_and(expression_returns_set)
                || j.formatted_expr.is_some_and(expression_returns_set)
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            c.args.iter().any(expression_returns_set)
                || c.func.is_some_and(expression_returns_set)
                || c.coercion.is_some_and(expression_returns_set)
        }
        NodeTag::T_JsonIsPredicate => {
            node.as_json_is_predicate().unwrap().expr.is_some_and(expression_returns_set)
        }
        NodeTag::T_JsonBehavior => {
            node.as_json_behavior().unwrap().expr.is_some_and(expression_returns_set)
        }
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                .into_iter()
                .flatten()
                .any(expression_returns_set)
                || j.passing_values.iter().any(expression_returns_set)
        }
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            x.named_args.iter().any(expression_returns_set)
                || x.args.iter().any(expression_returns_set)
        }
        other => panic!(
            "expression_returns_set (nodeFuncs.c): arm for {other:?} unported — \
             backend-nodes-core lane"
        ),
    }
}

/// C `select_common_type`; same precomputed exprType/exprLocation divergence
/// as [`coerce_to_boolean`], one `(type, location)` pair per expr. The
/// `which_expr` out-parameter is dropped (NULL at every ported call site).
/// `context == None` is C's NULL: return InvalidOid instead of erroring.
pub fn select_common_type(
    pstate: &ParseState<'_, '_>,
    exprs: &[(Oid, ParseLoc)],
    context: Option<&str>,
) -> PgResult<Oid> {
    select_common_type_pick(pstate, exprs, context).map(|(t, _)| t)
}

/// [`select_common_type`] plus C's `which_expr` out-parameter, returned as
/// the index of the winning expr.
pub fn select_common_type_pick(
    pstate: &ParseState<'_, '_>,
    exprs: &[(Oid, ParseLoc)],
    context: Option<&str>,
) -> PgResult<(Oid, usize)> {
    debug_assert!(!exprs.is_empty());
    let mut ptype = exprs[0].0;
    let mut pick = 0usize;
    let mut next = 1usize;

    if ptype != UNKNOWNOID {
        while next < exprs.len() && exprs[next].0 == ptype {
            next += 1;
        }
        if next == exprs.len() {
            return Ok((ptype, pick));
        }
    }

    ptype = lsyscache::getBaseType(ptype)?;
    let (mut pcategory, mut pispreferred) = lsyscache::get_type_category_preferred(ptype)?;

    for (i, &(rawtype, nloc)) in exprs.iter().enumerate().skip(next) {
        let ntype = lsyscache::getBaseType(rawtype)?;
        if ntype != UNKNOWNOID && ntype != ptype {
            let (ncategory, nispreferred) = lsyscache::get_type_category_preferred(ntype)?;
            if ptype == UNKNOWNOID {
                pick = i;
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            } else if ncategory != pcategory {
                let Some(context) = context else { return Ok((InvalidOid, pick)) };
                return Err(common_type_mismatch(pstate, context, ptype, ntype, nloc));
            } else if !pispreferred
                && can_coerce_type(&[ptype], &[ntype], COERCION_IMPLICIT)?
                && !can_coerce_type(&[ntype], &[ptype], COERCION_IMPLICIT)?
            {
                pick = i;
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            }
        }
    }

    if ptype == UNKNOWNOID {
        ptype = types_core::catalog::TEXTOID;
    }
    Ok((ptype, pick))
}

/// C `verify_common_type`; one precomputed exprType per expr.
pub fn verify_common_type(common_type: Oid, expr_types: &[Oid]) -> PgResult<bool> {
    for &t in expr_types {
        if !can_coerce_type(&[t], &[common_type], COERCION_IMPLICIT)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// C `coerce_to_common_type`; precomputed exprType/exprLocation divergence as
/// [`coerce_to_boolean`].
pub fn coerce_to_common_type<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    input_type_id: Oid,
    node_location: ParseLoc,
    target_type_id: Oid,
    context: &str,
) -> PgResult<Node<'mcx>> {
    if input_type_id == target_type_id {
        return Ok(node);
    }
    if can_coerce_type(&[input_type_id], &[target_type_id], COERCION_IMPLICIT)? {
        coerce_type(
            mcx,
            pstate,
            node,
            input_type_id,
            target_type_id,
            -1,
            COERCION_IMPLICIT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )
    } else {
        Err(cannot_coerce(pstate, context, input_type_id, target_type_id, node_location))
    }
}

/// C `select_common_typmod`; caller passes one `(exprType, exprTypmod)` pair
/// per expr (pstate is unused in C too).
pub fn select_common_typmod(exprs: &[(Oid, i32)], common_type: Oid) -> i32 {
    let mut result = -1;
    for (i, &(typ, typmod)) in exprs.iter().enumerate() {
        if typ != common_type {
            return -1;
        }
        if i == 0 {
            result = typmod;
        } else if result != typmod {
            return -1;
        }
    }
    result
}

#[track_caller]
#[cold]
#[inline(never)]
fn common_type_mismatch(
    pstate: &ParseState<'_, '_>,
    context: &str,
    ptype: Oid,
    ntype: Oid,
    location: ParseLoc,
) -> Box<PgError> {
    let (p, n) = match (format_type::format_type_be(ptype), format_type::format_type_be(ntype)) {
        (Ok(p), Ok(n)) => (p, n),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!("{context} types {p} and {n} cannot be matched"))
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "select_common_type")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_coerce(
    pstate: &ParseState<'_, '_>,
    context: &str,
    input_type_id: Oid,
    target_type_id: Oid,
    location: ParseLoc,
) -> Box<PgError> {
    use types_error::ERRCODE_CANNOT_COERCE;
    let (input, target) = match (
        format_type::format_type_be(input_type_id),
        format_type::format_type_be(target_type_id),
    ) {
        (Ok(i), Ok(t)) => (i, t),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_CANNOT_COERCE)
            .errmsg(format!("{context} could not convert type {input} to {target}"))
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "coerce_to_common_type")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn construct_type_mismatch(
    pstate: &ParseState<'_, '_>,
    construct_name: &str,
    target_type_id: Oid,
    input_type_id: Oid,
    location: ParseLoc,
) -> Box<PgError> {
    let (target, input) = match (
        format_type::format_type_be(target_type_id),
        format_type::format_type_be(input_type_id),
    ) {
        (Ok(t), Ok(i)) => (t, i),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "argument of {construct_name} must be type {target}, not type {input}"
            ))
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "coerce_to_boolean")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn returns_set(
    pstate: &ParseState<'_, '_>,
    construct_name: &str,
    location: ParseLoc,
) -> Box<PgError> {
    use types_error::{ErrorLocation, ERRCODE_DATATYPE_MISMATCH, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!("argument of {construct_name} must not return a set"))
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "coerce_to_boolean")),
    )
}
