#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

#[cfg(test)]
mod tests;

use datum::Datum;
use fmgr::FmgrInfo;
use mcx::Mcx;
use parser_small1::{
    parser_errposition, variable_coerce_param_hook, ParseRefHookState, ParseState,
};
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
use types_nodes::{CoercionForm, Const, FuncExpr, Node, NodeList, NodeTag, Param, RelabelType};

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
const TYPCATEGORY_STRING: i8 = b'S' as i8;

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

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("{what} unported — unit backend-parser-coerce")
}

fn is_complex(typid: Oid) -> PgResult<bool> {
    Ok(OidIsValid(lsyscache::get_typ_typrelid(typid)?))
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
    if node.node_tag() == NodeTag::T_CollateExpr {
        unported("coerce_type (parse_coerce.c): CollateExpr push-down arm");
    }
    let (pathtype, funcId) = find_coercion_pathway(targetTypeId, inputTypeId, ccontext)?;
    if pathtype != COERCION_PATH_NONE {
        let mut baseTypeMod = targetTypeMod;
        let baseTypeId = lsyscache::getBaseTypeAndTypmod(targetTypeId, &mut baseTypeMod)?;
        if targetTypeId != baseTypeId {
            unported("coerce_type (parse_coerce.c): coerce_to_domain");
        }
        if pathtype != COERCION_PATH_RELABELTYPE {
            return build_coercion_expression(
                mcx, node, pathtype, funcId, baseTypeId, baseTypeMod, ccontext, cformat, location,
            );
        }
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
    if (inputTypeId == RECORDOID && is_complex(targetTypeId)?)
        || (targetTypeId == RECORDOID && is_complex(inputTypeId)?)
        || (targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId)?)
    {
        unported("coerce_type (parse_coerce.c): RECORD/composite arms");
    }
    if is_complex(inputTypeId)? && is_complex(targetTypeId)? {
        unported("coerce_type (parse_coerce.c): typeInheritsFrom/ConvertRowtypeExpr arm");
    }
    Err(conversion_not_found(inputTypeId, targetTypeId))
}

// C's coerce_type CONSTANT arm: typinput through fmgr (stringTypeDatum).
fn coerce_unknown_const<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    targetTypeId: Oid,
    targetTypeMod: i32,
    _ccontext: CoercionContext,
    _location: ParseLoc,
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
        unported("coerce_type (parse_coerce.c): coerce_to_domain over the CONSTANT arm");
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
        if flinfo.fn_strict {
            return Ok(Datum::null());
        }
        unported("stringTypeDatum (parse_type.c): non-strict typinput with NULL input");
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
    debug_assert_eq!(input_typeids.len(), target_typeids.len());
    for (&inputTypeId, &targetTypeId) in input_typeids.iter().zip(target_typeids) {
        if inputTypeId == targetTypeId || targetTypeId == ANYOID {
            continue;
        }
        if IsPolymorphicType(targetTypeId) {
            unported("can_coerce_type (parse_coerce.c): check_generic_type_consistency");
        }
        if inputTypeId == UNKNOWNOID {
            continue;
        }
        if find_coercion_pathway(targetTypeId, inputTypeId, ccontext)?.0 != COERCION_PATH_NONE {
            continue;
        }
        if (inputTypeId == RECORDOID && is_complex(targetTypeId)?)
            || (targetTypeId == RECORDOID && is_complex(inputTypeId)?)
            || (targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId)?)
            || (is_complex(inputTypeId)? && is_complex(targetTypeId)?)
        {
            unported("can_coerce_type (parse_coerce.c): RECORD/composite/inheritance arms");
        }
        return Ok(false);
    }
    Ok(true)
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
    if srctype == targettype {
        return Ok(true);
    }
    if matches!(targettype, ANYOID | ANYELEMENTOID | ANYCOMPATIBLEOID) {
        return Ok(true);
    }
    let srctype = if OidIsValid(srctype) { lsyscache::getBaseType(srctype)? } else { srctype };
    if srctype == targettype {
        return Ok(true);
    }
    let type_is_array = |t: Oid| -> PgResult<bool> { Ok(OidIsValid(lsyscache::get_element_type(t)?)) };
    if matches!(targettype, ANYARRAYOID | ANYCOMPATIBLEARRAYOID) && type_is_array(srctype)? {
        return Ok(true);
    }
    if matches!(targettype, ANYNONARRAYOID | ANYCOMPATIBLENONARRAYOID) && !type_is_array(srctype)? {
        return Ok(true);
    }
    if targettype == ANYENUMOID && lsyscache::type_is_enum(srctype)? {
        return Ok(true);
    }
    if matches!(targettype, ANYRANGEOID | ANYCOMPATIBLERANGEOID)
        && lsyscache::type_is_range(srctype)?
    {
        return Ok(true);
    }
    if matches!(targettype, ANYMULTIRANGEOID | ANYCOMPATIBLEMULTIRANGEOID)
        && lsyscache::type_is_multirange(srctype)?
    {
        return Ok(true);
    }
    if targettype == RECORDOID && is_complex(srctype)? {
        return Ok(true);
    }
    if targettype == RECORDARRAYOID && is_complex_array(srctype)? {
        return Ok(true);
    }
    match syscache_seams::lookup_pg_cast_shape::call(srctype, targettype)? {
        Some(cast) => Ok(cast.castmethod == COERCION_METHOD_BINARY
            && cast.castcontext == COERCION_CODE_IMPLICIT),
        None => Ok(false),
    }
}

/// Minimal `enforce_generic_type_consistency`: the no-polymorphic fast exit.
pub fn enforce_generic_type_consistency(
    actual_arg_types: &[Oid],
    declared_arg_types: &mut [Oid],
    rettype: Oid,
    _allow_poly: bool,
) -> Oid {
    let _ = actual_arg_types;
    if declared_arg_types.iter().any(|&t| IsPolymorphicType(t)) || IsPolymorphicType(rettype) {
        unported(
            "enforce_generic_type_consistency (parse_coerce.c): polymorphic argument/result \
             resolution",
        );
    }
    rettype
}

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
            unported("build_coercion_expression (parse_coerce.c): ArrayCoerceExpr path")
        }
        COERCION_PATH_COERCEVIAIO => {
            unported("build_coercion_expression (parse_coerce.c): CoerceViaIO path")
        }
        _ => Err(Box::new(PgError::error(format!(
            "unsupported pathtype {pathtype:?} in build_coercion_expression"
        )))),
    }
}

#[cold]
#[inline(never)]
fn function_lookup_failed(funcid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for function {funcid}")))
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
    if expr.node_tag() == NodeTag::T_CollateExpr {
        unported("coerce_to_target_type (parse_coerce.c): CollateExpr strip/reinstall arm");
    }
    let result = coerce_type(
        mcx, pstate, expr, exprtype, targettype, targettypmod, ccontext, cformat, location,
    )?;
    if targettypmod >= 0 {
        unported(
            "coerce_to_target_type (parse_coerce.c): coerce_type_typmod \
             (find_typmod_coercion_function length-coercion lane)",
        );
    }
    Ok(Some(result))
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
fn expression_returns_set(node: Node<'_>) -> bool {
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
        NodeTag::T_RelabelType => expression_returns_set(node.as_relabel_type().unwrap().arg),
        NodeTag::T_NullTest => {
            node.as_null_test().unwrap().arg.is_some_and(expression_returns_set)
        }
        NodeTag::T_Const | NodeTag::T_Param | NodeTag::T_Var | NodeTag::T_CaseTestExpr => false,
        // SubLink is not set-returning; C's walker does not enter subselects.
        NodeTag::T_SubLink => {
            node.as_sub_link().unwrap().testexpr.is_some_and(expression_returns_set)
        }
        other => panic!(
            "expression_returns_set (nodeFuncs.c): arm for {other:?} unported — \
             backend-nodes-core lane"
        ),
    }
}

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
            .with_error_location(ErrorLocation::new("parse_coerce.c", 0, "coerce_to_boolean")),
    )
}

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
            .with_error_location(ErrorLocation::new("parse_coerce.c", 0, "coerce_to_boolean")),
    )
}
