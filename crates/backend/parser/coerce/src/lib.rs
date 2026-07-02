#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

#[cfg(test)]
mod tests;

use datum::Datum;
use fmgr::FmgrInfo;
use mcx::Mcx;
use parser_small1::{variable_coerce_param_hook, ParseRefHookState, ParseState};
use types_core::catalog::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
    ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
    ANYNONARRAYOID, ANYOID, ANYRANGEOID, INT2VECTOROID, INTERVALOID, OIDVECTOROID, RECORDARRAYOID,
    RECORDOID, UNKNOWNOID,
};
use types_core::{InvalidOid, Oid, OidIsValid, ParseLoc};
use types_error::{PgError, PgResult};
use types_nodes::{CoercionForm, Const, Node, NodeTag, Param, RelabelType};

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
        return coerce_unknown_const(mcx, node, targetTypeId, targetTypeMod, ccontext, location);
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
    let (pathtype, _funcId) = find_coercion_pathway(targetTypeId, inputTypeId, ccontext)?;
    if pathtype != COERCION_PATH_NONE {
        let mut baseTypeMod = targetTypeMod;
        let baseTypeId = lsyscache::getBaseTypeAndTypmod(targetTypeId, &mut baseTypeMod)?;
        if pathtype != COERCION_PATH_RELABELTYPE {
            unported(
                "coerce_type (parse_coerce.c): build_coercion_expression \
                 (FUNC/COERCEVIAIO/ARRAYCOERCE paths)",
            );
        }
        if targetTypeId != baseTypeId {
            unported("coerce_type (parse_coerce.c): coerce_to_domain");
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

    let constvalue = string_type_datum(mcx, &io, con.constvalue, inputTypeMod, con.constisnull)?;

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

// C stringTypeDatum → OidInputFunctionCall; the result is copied into `mcx`
// (C's per-call palloc boundary — the fc_ scratch dies with the FmgrInfo).
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
    let d = fmgr_core::function_call3_coll(
        &mut flinfo,
        InvalidOid,
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
    // SAFETY: a strict input function returned a non-null by-reference datum
    // of this type's declared layout (flat varlena / NUL-terminated cstring /
    // fixed typlen bytes).
    let bytes: &[u8] = unsafe {
        let len = match typlen {
            -1 => types_tuple::varatt::varsize_any(p),
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
    // C renders type names via format_type_be (format_type.c, unported).
    Box::new(PgError::error(format!(
        "failed to find conversion function from {inputTypeId} to {targetTypeId}"
    )))
}

#[cold]
#[inline(never)]
fn type_lookup_failed(typid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for type {typid}")))
}
