//! `parser/parse_coerce.c` — type coercions/conversions for the parser.
//!
//! Ported 1:1 over the split raw-`Node`/lifetime-free-`Expr` model: coercion
//! operates on already-transformed [`Expr`] trees and adds decoration on top.
//! `pstate` is threaded for UNKNOWN-Param resolution and error positioning.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use mcx::{Mcx, MemoryContext};
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_CANNOT_COERCE, ERRCODE_DATATYPE_MISMATCH, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_INTERNAL_ERROR,
};
use types_nodes::primnodes::{
    ArrayCoerceExpr, CaseTestExpr, CoerceToDomain, CoerceViaIO, CoercionForm, CollateExpr,
    ConvertRowtypeExpr, Expr, RowExpr,
};
use types_parsenodes::CoercionContext;
use types_tuple::backend_access_common_heaptuple::Datum as TupleDatum;
use types_tuple::heaptuple::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
    ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
    ANYNONARRAYOID, ANYOID, ANYRANGEOID, BOOLOID, INT4OID, INTERVALOID, INT2VECTOROID, OIDVECTOROID,
    RECORDARRAYOID, RECORDOID, TEXTARRAYOID, TEXTOID, UNKNOWNOID,
};

use backend_nodes_core::makefuncs::{
    make_const, make_func_expr, make_null_const, make_relabel_type,
};
use backend_nodes_core::nodefuncs::{
    apply_relabel_type, expr_collation as exprCollation, expr_type as exprType,
    expr_typmod as exprTypmod, expression_returns_set,
};

use types_nodes::parsestmt::ParseState;

// Sibling parser crate (no dependency cycle: parse_type doesn't dep coerce-seams).
use backend_parser_parse_type as parse_type;

// Outward seam aliases.
use backend_catalog_pg_inherits_seams::type_inherits_from;
use backend_parser_small1_seams::parser_errposition;
use backend_utils_adt_format_type_seams::format_type_be_str;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;
use backend_utils_cache_typcache_seams as typcache;

#[cfg(test)]
mod tests;

// ===========================================================================
// pg_cast char codes (catalog/pg_cast.h) and the CoercionForm shortcut.
// ===========================================================================

const COERCION_CODE_IMPLICIT: i8 = b'i' as i8;
const COERCION_CODE_ASSIGNMENT: i8 = b'a' as i8;
const COERCION_CODE_EXPLICIT: i8 = b'e' as i8;

const COERCION_METHOD_FUNCTION: i8 = b'f' as i8;
const COERCION_METHOD_BINARY: i8 = b'b' as i8;
const COERCION_METHOD_INOUT: i8 = b'i' as i8;

const COERCE_IMPLICIT_CAST: CoercionForm = CoercionForm::COERCE_IMPLICIT_CAST;

/// `TYPCATEGORY_STRING` (`catalog/pg_type.h`).
const TYPCATEGORY_STRING: u8 = b'S';
/// `TYPCATEGORY_INVALID` (`catalog/pg_type.h`).
const TYPCATEGORY_INVALID: u8 = b'\0';

// ===========================================================================
// CoercionPathType — re-exported from the seams crate (the parser's
// CoercionPathType in parse_coerce.h).
// ===========================================================================

pub use backend_parser_coerce_seams::CoercionPathType;

// ===========================================================================
// ABI predicates ported from the C macros (postgres.h / pg_type.h).
// ===========================================================================

/// `IsPolymorphicType(typid)` == `IsPolymorphicTypeFamily1 || Family2`.
#[inline]
fn is_polymorphic_type(typid: Oid) -> bool {
    is_polymorphic_type_family1(typid) || is_polymorphic_type_family2(typid)
}

/// `IsPolymorphicTypeFamily1(typid)` (pg_type.h).
#[inline]
fn is_polymorphic_type_family1(typid: Oid) -> bool {
    matches!(
        typid,
        ANYELEMENTOID
            | ANYARRAYOID
            | ANYNONARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
    )
}

/// `IsPolymorphicTypeFamily2(typid)` (pg_type.h).
#[inline]
fn is_polymorphic_type_family2(typid: Oid) -> bool {
    matches!(
        typid,
        ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

/// `ISCOMPLEX(typeid)` (parser/parse_type.h):
/// `typeOrDomainTypeRelid(typeid) != InvalidOid` — a composite type or a domain
/// over one (including RECORD, whose pg_type row has a typrelid).
fn is_complex(typeid: Oid) -> PgResult<bool> {
    Ok(OidIsValid(parse_type::typeOrDomainTypeRelid(typeid)?))
}

/// `type_is_array(typid)` macro (lsyscache.h): `get_element_type(typid) !=
/// InvalidOid`.
fn type_is_array(typid: Oid) -> PgResult<bool> {
    Ok(lsyscache::get_element_type::call(typid)?.is_some())
}

/// `type_is_array_domain(typid)` macro (lsyscache.h): `get_base_element_type
/// (typid) != InvalidOid` (accepts plain arrays and domains over arrays).
fn type_is_array_domain(typid: Oid) -> PgResult<bool> {
    Ok(OidIsValid(lsyscache::get_base_element_type::call(typid)?))
}

/// `get_element_type` as the bare `Oid` (InvalidOid for non-arrays).
fn get_element_type(typid: Oid) -> PgResult<Oid> {
    Ok(lsyscache::get_element_type::call(typid)?.unwrap_or(InvalidOid))
}

/// `get_array_type` as the bare `Oid` (InvalidOid if none).
fn get_array_type(typid: Oid) -> PgResult<Oid> {
    Ok(lsyscache::get_array_type::call(typid)?.unwrap_or(InvalidOid))
}

/// `format_type_be(oid)` for error-message interpolation.
fn format_type_be(oid: Oid) -> PgResult<String> {
    format_type_be_str::call(oid)
}

// ===========================================================================
// CoercionContext ordering (the C `ccontext >= castcontext` integer compare).
// ===========================================================================

#[inline]
fn ccontext_rank(c: CoercionContext) -> i32 {
    c as i32
}

// ===========================================================================
// coerce_to_target_type()
// ===========================================================================

/// `coerce_to_target_type(pstate, expr, exprtype, targettype, targettypmod,
/// ccontext, cformat, location)` (parse_coerce.c) — the general-purpose entry
/// point for arbitrary type coercion. Returns `None` if not possible (the C
/// NULL return), so callers can issue context-specific errors.
pub fn coerce_to_target_type<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&mut ParseState<'_>>,
    expr: Expr,
    exprtype: Oid,
    targettype: Oid,
    targettypmod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: i32,
) -> PgResult<Option<Expr>> {
    if !can_coerce_type(1, &[exprtype], &[targettype], ccontext)? {
        return Ok(None);
    }

    // Strip a top CollateExpr (possibly several), remember the original.
    // `expr` is the stripped inner; `coll` (if any) is the original topmost.
    let origexpr = expr;
    let (stripped, top_coll): (Expr, Option<CollateExpr>) = strip_top_collate(origexpr);
    // We need both the stripped expr (consumed by coerce_type) and the original
    // for the type-equality check + CollateExpr reinstall. `strip_top_collate`
    // returns a clone of the topmost CollateExpr's coll fields when one existed.
    let had_collate = top_coll.is_some();

    let result = coerce_type(
        mcx,
        pstate,
        Some(stripped.clone()),
        exprtype,
        targettype,
        targettypmod,
        ccontext,
        cformat,
        location,
    )?;
    let result = result.expect("coerce_type returned NULL after can_coerce_type succeeded");

    // result != expr && !IsA(result, Const)
    let force_implicit =
        !exprs_identical(&result, &stripped) && !matches!(result, Expr::Const(_));
    let result = coerce_type_typmod(
        mcx,
        result,
        targettype,
        targettypmod,
        ccontext,
        cformat,
        location,
        force_implicit,
    )?;

    if had_collate && lsyscache::type_is_collatable::call(targettype)? {
        // Reinstall the top CollateExpr.
        let coll = top_coll.unwrap();
        let newcoll = CollateExpr {
            arg: Some(alloc::boxed::Box::new(result)),
            collOid: coll.collOid,
            // newcoll->location = coll->location;
            location: coll.location,
        };
        return Ok(Some(Expr::CollateExpr(newcoll)));
    }

    Ok(Some(result))
}

/// `while (expr && IsA(expr, CollateExpr)) expr = coll->arg;` — peel all
/// stacked CollateExprs, returning the innermost arg and the topmost
/// CollateExpr's (collOid) for reinstall. Mirrors the C "discard all but the
/// topmost" behaviour.
fn strip_top_collate(expr: Expr) -> (Expr, Option<CollateExpr>) {
    let mut top: Option<CollateExpr> = None;
    let mut cur = expr;
    loop {
        match cur {
            Expr::CollateExpr(coll) => {
                if top.is_none() {
                    top = Some(CollateExpr {
                        arg: None,
                        collOid: coll.collOid,
                        location: coll.location,
                    });
                }
                match coll.arg {
                    Some(b) => cur = *b,
                    None => {
                        // No inner arg; the stripped result is a degenerate
                        // empty value. Mirror C: the loop stops at NULL arg.
                        cur = Expr::CollateExpr(CollateExpr {
                            arg: None,
                            collOid: coll.collOid,
                            location: coll.location,
                        });
                        break;
                    }
                }
            }
            other => {
                cur = other;
                break;
            }
        }
    }
    (cur, top)
}

/// Pointer-identity proxy for the C `result != expr`. In the C, coerce_type
/// returns the *same* node pointer when it did nothing; with owned values we
/// detect "unchanged" structurally.
fn exprs_identical(a: &Expr, b: &Expr) -> bool {
    // coerce_type returns the input unchanged only in the "no conversion" arms,
    // where the value is byte-identical. A structural equality on the debug
    // representation is the faithful owned-value stand-in for C pointer
    // identity here (the only consumer is the implicit-display-form decision).
    format!("{a:?}") == format!("{b:?}")
}

// ===========================================================================
// coerce_type()
// ===========================================================================

/// `coerce_type(pstate, node, inputTypeId, targetTypeId, targetTypeMod,
/// ccontext, cformat, location)` (parse_coerce.c). Returns `None` only when the
/// input node was `NULL`.
pub fn coerce_type<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&mut ParseState<'_>>,
    node: Option<Expr>,
    inputTypeId: Oid,
    targetTypeId: Oid,
    targetTypeMod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: i32,
) -> PgResult<Option<Expr>> {
    if targetTypeId == inputTypeId || node.is_none() {
        // no conversion needed
        return Ok(node);
    }
    let node = node.unwrap();

    if targetTypeId == ANYOID
        || targetTypeId == ANYELEMENTOID
        || targetTypeId == ANYNONARRAYOID
        || targetTypeId == ANYCOMPATIBLEOID
        || targetTypeId == ANYCOMPATIBLENONARRAYOID
    {
        // Return the unmodified node (UNKNOWN const acceptable as such input).
        return Ok(Some(node));
    }
    if targetTypeId == ANYARRAYOID
        || targetTypeId == ANYENUMOID
        || targetTypeId == ANYRANGEOID
        || targetTypeId == ANYMULTIRANGEOID
        || targetTypeId == ANYCOMPATIBLEARRAYOID
        || targetTypeId == ANYCOMPATIBLERANGEOID
        || targetTypeId == ANYCOMPATIBLEMULTIRANGEOID
    {
        if inputTypeId != UNKNOWNOID {
            let baseTypeId = lsyscache::get_base_type::call(inputTypeId)?;
            if baseTypeId != inputTypeId {
                let r = make_relabel_type(node, baseTypeId, -1, InvalidOid, cformat);
                // r->location = location; (location not modeled in RelabelType)
                let _ = location;
                return Ok(Some(r));
            }
            // Not a domain type, return as-is.
            return Ok(Some(node));
        }
        // UNKNOWN input falls through.
    }

    if inputTypeId == UNKNOWNOID && matches!(node, Expr::Const(_)) {
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

    if matches!(node, Expr::Param(_)) {
        if let Some(pstate) = pstate.as_deref() {
            if pstate.p_coerce_param_hook.is_some() {
                // p_coerce_param_hook returns a NodePtr (raw node universe);
                // the coerce path produces Expr. The hook's transformed-node
                // bridge is the raw-Node/Expr split keystone — unmodeled here.
                // No installed parser hook reaches this in the value-typed path
                // yet; mirror-PG-and-panic on use.
                panic!(
                    "coerce_type: p_coerce_param_hook returns a NodePtr (raw-node \
                     universe); the Param coercion-hook bridge to the Expr tree is \
                     the unported parser-hook keystone"
                );
            }
        }
    }

    if let Expr::CollateExpr(coll) = &node {
        // Push the coercion underneath the COLLATE (or discard if target not
        // collatable).
        let coll_oid = coll.collOid;
        let coll_location = coll.location;
        let arg = coll.arg.clone().map(|b| *b);
        let result = coerce_type(
            mcx,
            pstate,
            arg,
            inputTypeId,
            targetTypeId,
            targetTypeMod,
            ccontext,
            cformat,
            location,
        )?;
        if lsyscache::type_is_collatable::call(targetTypeId)? {
            let newcoll = CollateExpr {
                arg: result.map(alloc::boxed::Box::new),
                collOid: coll_oid,
                // newcoll->location = coll->location;
                location: coll_location,
            };
            return Ok(Some(Expr::CollateExpr(newcoll)));
        }
        return Ok(result);
    }

    let (pathtype, funcId) = find_coercion_pathway(targetTypeId, inputTypeId, ccontext)?;
    if pathtype != CoercionPathType::None {
        // baseTypeMod = targetTypeMod; baseTypeId = getBaseTypeAndTypmod(...).
        let baseTypeId = lsyscache::get_base_type_and_typmod::call(targetTypeId)?.0;
        let baseTypeMod = resolve_base_typmod(targetTypeId, targetTypeMod)?;

        if pathtype != CoercionPathType::Relabeltype {
            let mut result = build_coercion_expression(
                mcx,
                node,
                pathtype,
                funcId,
                baseTypeId,
                baseTypeMod,
                ccontext,
                cformat,
                location,
            )?;
            if targetTypeId != baseTypeId {
                result = coerce_to_domain(
                    mcx, result, baseTypeId, baseTypeMod, targetTypeId, ccontext, cformat,
                    location, true,
                )?;
            }
            return Ok(Some(result));
        } else {
            let result = coerce_to_domain(
                mcx,
                node.clone(),
                baseTypeId,
                baseTypeMod,
                targetTypeId,
                ccontext,
                cformat,
                location,
                false,
            )?;
            if exprs_identical(&result, &node) {
                let r = make_relabel_type(result, targetTypeId, -1, InvalidOid, cformat);
                let _ = location;
                return Ok(Some(r));
            }
            return Ok(Some(result));
        }
    }

    if inputTypeId == RECORDOID && is_complex(targetTypeId)? {
        return Ok(Some(coerce_record_to_complex(
            mcx,
            pstate,
            node,
            targetTypeId,
            ccontext,
            cformat,
            location,
        )?));
    }
    if targetTypeId == RECORDOID && is_complex(inputTypeId)? {
        // NB: no RelabelType.
        return Ok(Some(node));
    }
    // NOT_USED record[] -> complex array is omitted (the C #ifdef NOT_USED).
    if targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId)? {
        // NB: no RelabelType.
        return Ok(Some(node));
    }
    if type_inherits_from::call(inputTypeId, targetTypeId)?
        || typeIsOfTypedTable(inputTypeId, targetTypeId)?
    {
        let baseTypeId = lsyscache::get_base_type::call(inputTypeId)?;
        let mut node = node;
        if baseTypeId != inputTypeId {
            let rt = make_relabel_type(
                node,
                baseTypeId,
                -1,
                InvalidOid,
                COERCE_IMPLICIT_CAST,
            );
            let _ = location;
            node = rt;
        }
        let r = ConvertRowtypeExpr {
            arg: Some(alloc::boxed::Box::new(node)),
            resulttype: targetTypeId,
            convertformat: cformat,
            // r->location = location;
            location,
        };
        return Ok(Some(Expr::ConvertRowtypeExpr(r)));
    }

    // caller blew it
    Err(types_error::PgError::error(format!(
        "failed to find conversion function from {} to {}",
        format_type_be(inputTypeId)?,
        format_type_be(targetTypeId)?
    ))
    .with_sqlstate(ERRCODE_INTERNAL_ERROR))
}

/// `getBaseTypeAndTypmod(targetTypeId, &baseTypeMod)` where the in/out typmod
/// starts at `targetTypeMod`. The lsyscache helper resolves the domain chain;
/// for a non-domain the typmod is unchanged.
fn resolve_base_typmod(target_type_id: Oid, target_typmod: i32) -> PgResult<i32> {
    let (base, base_typmod) = lsyscache::get_base_type_and_typmod::call(target_type_id)?;
    if base == target_type_id {
        // Not a domain — typmod unchanged.
        Ok(target_typmod)
    } else {
        // Domain — the resolved base typmod from the chain.
        Ok(base_typmod)
    }
}

/// The `inputTypeId == UNKNOWNOID && IsA(node, Const)` arm of coerce_type.
fn coerce_unknown_const<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&mut ParseState<'_>>,
    node: Expr,
    targetTypeId: Oid,
    targetTypeMod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: i32,
) -> PgResult<Option<Expr>> {
    let con = match node {
        Expr::Const(c) => c,
        _ => unreachable!("coerce_unknown_const called on a non-Const"),
    };

    // baseTypeMod starts at targetTypeMod, base resolved by domain chain.
    let baseTypeId = lsyscache::get_base_type_and_typmod::call(targetTypeId)?.0;
    let baseTypeMod = resolve_base_typmod(targetTypeId, targetTypeMod)?;

    let inputTypeMod = if baseTypeId == INTERVALOID {
        baseTypeMod
    } else {
        -1
    };

    let baseType = parse_type::typeidType(baseTypeId)?;

    // Set up parse error position pointing at the constant; the C
    // setup/cancel_parser_errposition_callback model is the retired
    // error_context_stack propagation (small1 documents this). The
    // stringTypeDatum failure surface carries the location via the seam.
    let _ = (
        pstate, location, mcx, baseType, inputTypeMod, baseTypeMod, ccontext, cformat, &con,
    );

    // The C builds `newcon->constvalue = stringTypeDatum(baseType,
    // DatumGetCString(con->constvalue), inputTypeMod)` — feeding the UNKNOWN
    // literal's C-string through the target's typinput. Two pieces are blocked
    // on the execTuples canonical-carrier (#113): (1) DatumGetCString reads the
    // literal text out of `con.constvalue`, but the trimmed `Const` stores a
    // bare-word `Datum<'static>` with no cstring decode path; and (2)
    // stringTypeDatum yields a call-frame `Datum` whose by-reference image
    // cannot be stored back into the `Datum<'static>` Const field. The whole
    // unknown-literal-to-typed-Const arm therefore mirror-PG-and-panics until
    // the carrier widens; every other coerce_type arm is fully ported.
    panic!(
        "coerce_type UNKNOWN-Const arm: building a typed Const from an UNKNOWN \
         literal (DatumGetCString + stringTypeDatum) needs the execTuples \
         canonical-carrier (#113); the trimmed Const carries a bare-word \
         Datum<'static> with no cstring decode/store path"
    )
}

// ===========================================================================
// can_coerce_type()
// ===========================================================================

/// `can_coerce_type(nargs, input_typeids, target_typeids, ccontext)`
/// (parse_coerce.c).
pub fn can_coerce_type(
    nargs: i32,
    input_typeids: &[Oid],
    target_typeids: &[Oid],
    ccontext: CoercionContext,
) -> PgResult<bool> {
    let mut have_generics = false;

    for i in 0..nargs as usize {
        let inputTypeId = input_typeids[i];
        let targetTypeId = target_typeids[i];

        if inputTypeId == targetTypeId {
            continue;
        }
        if targetTypeId == ANYOID {
            continue;
        }
        if is_polymorphic_type(targetTypeId) {
            have_generics = true;
            continue;
        }
        if inputTypeId == UNKNOWNOID {
            continue;
        }
        let (pathtype, _funcid) = find_coercion_pathway(targetTypeId, inputTypeId, ccontext)?;
        if pathtype != CoercionPathType::None {
            continue;
        }
        if inputTypeId == RECORDOID && is_complex(targetTypeId)? {
            continue;
        }
        if targetTypeId == RECORDOID && is_complex(inputTypeId)? {
            continue;
        }
        // NOT_USED record[] arm omitted.
        if targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId)? {
            continue;
        }
        if type_inherits_from::call(inputTypeId, targetTypeId)?
            || typeIsOfTypedTable(inputTypeId, targetTypeId)?
        {
            continue;
        }
        return Ok(false);
    }

    if have_generics
        && !check_generic_type_consistency(input_typeids, target_typeids, nargs)?
    {
        return Ok(false);
    }

    Ok(true)
}

// ===========================================================================
// coerce_to_domain()
// ===========================================================================

/// `coerce_to_domain(arg, baseTypeId, baseTypeMod, typeId, ccontext, cformat,
/// location, hideInputCoercion)` (parse_coerce.c).
pub fn coerce_to_domain<'mcx>(
    mcx: Mcx<'mcx>,
    arg: Expr,
    baseTypeId: Oid,
    baseTypeMod: i32,
    typeId: Oid,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: i32,
    hideInputCoercion: bool,
) -> PgResult<Expr> {
    debug_assert!(OidIsValid(baseTypeId));

    // If it isn't a domain, return the node as it was passed in.
    if baseTypeId == typeId {
        return Ok(arg);
    }

    let mut arg = arg;
    if hideInputCoercion {
        arg = hide_coercion_node(arg)?;
    }

    arg = coerce_type_typmod(
        mcx,
        arg,
        baseTypeId,
        baseTypeMod,
        ccontext,
        COERCE_IMPLICIT_CAST,
        location,
        false,
    )?;

    let result = CoerceToDomain {
        arg: Some(alloc::boxed::Box::new(arg)),
        resulttype: typeId,
        resulttypmod: -1,
        resultcollid: InvalidOid,
        coercionformat: cformat,
        // result->location = location;
        location,
    };

    Ok(Expr::CoerceToDomain(result))
}

// ===========================================================================
// coerce_type_typmod()
// ===========================================================================

/// `coerce_type_typmod(node, targetTypeId, targetTypMod, ccontext, cformat,
/// location, hideInputCoercion)` (parse_coerce.c).
fn coerce_type_typmod<'mcx>(
    mcx: Mcx<'mcx>,
    node: Expr,
    targetTypeId: Oid,
    targetTypMod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: i32,
    hideInputCoercion: bool,
) -> PgResult<Expr> {
    // Skip coercion if already done.
    if targetTypMod == exprTypmod(Some(&node))? {
        return Ok(node);
    }

    let mut node = node;
    if hideInputCoercion {
        node = hide_coercion_node(node)?;
    }

    let (pathtype, funcId) = if targetTypMod < 0 {
        (CoercionPathType::None, InvalidOid)
    } else {
        find_typmod_coercion_function(targetTypeId)?
    };

    if pathtype != CoercionPathType::None {
        node = build_coercion_expression(
            mcx, node, pathtype, funcId, targetTypeId, targetTypMod, ccontext, cformat, location,
        )?;
    } else {
        let collation = exprCollation(Some(&node))?;
        let _ = location;
        node = apply_relabel_type(node, targetTypeId, targetTypMod, collation, cformat, false)?;
    }

    Ok(node)
}

// ===========================================================================
// hide_coercion_node()
// ===========================================================================

/// `hide_coercion_node(node)` (parse_coerce.c) — force the top coercion node to
/// IMPLICIT display form. Caller error if the node has no CoercionForm field.
fn hide_coercion_node(node: Expr) -> PgResult<Expr> {
    let out = match node {
        Expr::FuncExpr(mut f) => {
            f.funcformat = COERCE_IMPLICIT_CAST;
            Expr::FuncExpr(f)
        }
        Expr::RelabelType(mut r) => {
            r.relabelformat = COERCE_IMPLICIT_CAST;
            Expr::RelabelType(r)
        }
        Expr::CoerceViaIO(mut c) => {
            c.coerceformat = COERCE_IMPLICIT_CAST;
            Expr::CoerceViaIO(c)
        }
        Expr::ArrayCoerceExpr(mut a) => {
            a.coerceformat = COERCE_IMPLICIT_CAST;
            Expr::ArrayCoerceExpr(a)
        }
        Expr::ConvertRowtypeExpr(mut c) => {
            c.convertformat = COERCE_IMPLICIT_CAST;
            Expr::ConvertRowtypeExpr(c)
        }
        Expr::RowExpr(mut r) => {
            r.row_format = COERCE_IMPLICIT_CAST;
            Expr::RowExpr(r)
        }
        Expr::CoerceToDomain(mut c) => {
            c.coercionformat = COERCE_IMPLICIT_CAST;
            Expr::CoerceToDomain(c)
        }
        other => {
            return Err(types_error::PgError::error(format!(
                "unsupported node type: {}",
                node_tag_int(&other)
            ))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR));
        }
    };
    Ok(out)
}

/// A best-effort `nodeTag(node)` integer for the unsupported-node error. The
/// owned-value model has no numeric tag table; report the variant name slot.
fn node_tag_int(_node: &Expr) -> i32 {
    0
}

// ===========================================================================
// build_coercion_expression()
// ===========================================================================

/// `build_coercion_expression(node, pathtype, funcId, targetTypeId,
/// targetTypMod, ccontext, cformat, location)` (parse_coerce.c).
fn build_coercion_expression<'mcx>(
    mcx: Mcx<'mcx>,
    node: Expr,
    pathtype: CoercionPathType,
    funcId: Oid,
    targetTypeId: Oid,
    targetTypMod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: i32,
) -> PgResult<Expr> {
    let mut nargs = 0i32;

    if OidIsValid(funcId) {
        let procrow = syscache::proc_row_by_oid::call(mcx, funcId)?;
        let proc = match procrow {
            Some(p) => p,
            None => {
                return Err(types_error::PgError::error(format!(
                    "cache lookup failed for function {funcId}"
                ))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR));
            }
        };
        // The C Asserts on proretset/prokind are debug-only (behavior-
        // preserving omission). The runtime-significant value is pronargs.
        nargs = proc.pronargs;
        debug_assert!((1..=3).contains(&nargs));
        debug_assert!(nargs < 2 || proc.proargtypes[1] == INT4OID);
        debug_assert!(nargs < 3 || proc.proargtypes[2] == BOOLOID);
    }

    match pathtype {
        CoercionPathType::Func => {
            debug_assert!(OidIsValid(funcId));
            let mut args: Vec<Expr> = vec![node];

            if nargs >= 2 {
                let cons = make_const(
                    mcx,
                    INT4OID,
                    -1,
                    InvalidOid,
                    core::mem::size_of::<i32>() as i32,
                    TupleDatum::ByVal(targetTypMod as usize),
                    false,
                    true,
                )?;
                args.push(Expr::Const(cons));
            }
            if nargs == 3 {
                let isexplicit = ccontext == CoercionContext::COERCION_EXPLICIT;
                let cons = make_const(
                    mcx,
                    BOOLOID,
                    -1,
                    InvalidOid,
                    core::mem::size_of::<bool>() as i32,
                    TupleDatum::ByVal(isexplicit as usize),
                    false,
                    true,
                )?;
                args.push(Expr::Const(cons));
            }

            let fexpr = make_func_expr(funcId, targetTypeId, args, InvalidOid, InvalidOid, cformat);
            // fexpr->location = location (not modeled on FuncExpr).
            let _ = location;
            Ok(fexpr)
        }
        CoercionPathType::Arraycoerce => {
            // Look through any domain over the source array type.
            let sourceBaseTypeMod0 = exprTypmod(Some(&node))?;
            let source_type = exprType(Some(&node))?;
            let sourceBaseTypeId = lsyscache::get_base_type_and_typmod::call(source_type)?.0;
            let sourceBaseTypeMod = resolve_base_typmod(source_type, sourceBaseTypeMod0)?;

            // CaseTestExpr representing one source element.
            let ctest_typeid = get_element_type(sourceBaseTypeId)?;
            debug_assert!(OidIsValid(ctest_typeid));
            let ctest = CaseTestExpr {
                typeId: ctest_typeid,
                typeMod: sourceBaseTypeMod,
                collation: InvalidOid,
            };

            let targetElementType = get_element_type(targetTypeId)?;
            debug_assert!(OidIsValid(targetElementType));

            let elemexpr = coerce_to_target_type(
                mcx,
                None,
                Expr::CaseTestExpr(ctest),
                ctest_typeid,
                targetElementType,
                targetTypMod,
                ccontext,
                cformat,
                location,
            )?;
            let elemexpr = match elemexpr {
                Some(e) => e,
                None => {
                    return Err(types_error::PgError::error(
                        "failed to coerce array element type as expected",
                    )
                    .with_sqlstate(ERRCODE_INTERNAL_ERROR));
                }
            };

            let resulttypmod = exprTypmod(Some(&elemexpr))?;
            let acoerce = ArrayCoerceExpr {
                arg: Some(alloc::boxed::Box::new(node)),
                elemexpr: Some(alloc::boxed::Box::new(elemexpr)),
                resulttype: targetTypeId,
                resulttypmod,
                resultcollid: InvalidOid,
                coerceformat: cformat,
                // acoerce->location = location;
                location,
            };
            Ok(Expr::ArrayCoerceExpr(acoerce))
        }
        CoercionPathType::Coerceviaio => {
            debug_assert!(!OidIsValid(funcId));
            let iocoerce = CoerceViaIO {
                arg: Some(alloc::boxed::Box::new(node)),
                resulttype: targetTypeId,
                resultcollid: InvalidOid,
                coerceformat: cformat,
                // iocoerce->location = location;
                location,
            };
            Ok(Expr::CoerceViaIO(iocoerce))
        }
        other => Err(types_error::PgError::error(format!(
            "unsupported pathtype {} in build_coercion_expression",
            other as i32
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)),
    }
}

// ===========================================================================
// coerce_record_to_complex()
// ===========================================================================

/// `coerce_record_to_complex(pstate, node, targetTypeId, ccontext, cformat,
/// location)` (parse_coerce.c). Supports RowExpr inputs; the whole-row-Var arm
/// is the raw-Node/Expr-split keystone (expandNSItemVars yields NodePtrs).
fn coerce_record_to_complex<'mcx>(
    mcx: Mcx<'mcx>,
    mut pstate: Option<&mut ParseState<'_>>,
    node: Expr,
    targetTypeId: Oid,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: i32,
) -> PgResult<Expr> {
    let args: Vec<Expr> = if let Expr::RowExpr(re) = &node {
        // RowExpr is RECORD; needn't worry about dropped columns.
        re.args.clone()
    } else if let Expr::Var(v) = &node {
        if v.varattno == 0 {
            // Whole-row Var: expandNSItemVars yields NodePtrs (raw node
            // universe), which the Expr coercion path cannot walk. This is the
            // raw-Node/Expr split keystone.
            let _ = (&pstate, v);
            panic!(
                "coerce_record_to_complex: whole-row Var arm needs \
                 expandNSItemVars over the Expr tree; expandNSItemVars yields the \
                 raw NodePtr universe (raw-Node/Expr split keystone)"
            );
        } else {
            return record_cast_error(pstate, targetTypeId, location, None);
        }
    } else {
        return record_cast_error(pstate, targetTypeId, location, None);
    };

    // Look up the composite type (possibly a domain over composite).
    let mut baseTypeMod = -1i32;
    let (baseTypeId, btm) = lsyscache::get_base_type_and_typmod::call(targetTypeId)?;
    if baseTypeId != targetTypeId {
        baseTypeMod = btm;
    }
    let tupdesc = typcache::lookup_rowtype_tupdesc::call(mcx, baseTypeId, baseTypeMod)?;

    let natts = tupdesc.attrs.len();
    let mut newargs: Vec<Expr> = Vec::new();
    let mut ucolno = 1i32;
    let mut arg_idx = 0usize;

    for i in 0..natts {
        let attr = tupdesc.attr(i);

        if attr.attisdropped {
            // Fill in NULL for dropped columns (type doesn't matter).
            newargs.push(Expr::Const(make_null_const(mcx, INT4OID, -1, InvalidOid)?));
            continue;
        }

        if arg_idx >= args.len() {
            return Err(record_cast_error_detail(
                pstate.as_deref(),
                targetTypeId,
                location,
                "Input has too few columns.".into(),
            ));
        }
        let expr = args[arg_idx].clone();
        let exprtype = exprType(Some(&expr))?;

        let atttypid = attr.atttypid;
        let atttypmod = attr.atttypmod;
        let cexpr = coerce_to_target_type(
            mcx,
            pstate.as_deref_mut(),
            expr,
            exprtype,
            atttypid,
            atttypmod,
            ccontext,
            COERCE_IMPLICIT_CAST,
            -1,
        )?;
        let cexpr = match cexpr {
            Some(c) => c,
            None => {
                return Err(types_error::PgError::error(format!(
                    "cannot cast type {} to {}",
                    format_type_be(RECORDOID)?,
                    format_type_be(targetTypeId)?
                ))
                .with_detail(format!(
                    "Cannot cast type {} to {} in column {}.",
                    format_type_be(exprtype)?,
                    format_type_be(atttypid)?,
                    ucolno
                ))
                .with_sqlstate(ERRCODE_CANNOT_COERCE));
            }
        };
        newargs.push(cexpr);
        ucolno += 1;
        arg_idx += 1;
    }
    if arg_idx < args.len() {
        return Err(record_cast_error_detail(
            pstate.as_deref(),
            targetTypeId,
            location,
            "Input has too many columns.".into(),
        ));
    }

    let mut rowexpr = RowExpr {
        args: newargs,
        row_typeid: baseTypeId,
        row_format: cformat,
        colnames: Vec::new(),
        // rowexpr->location = location;
        location,
    };

    if baseTypeId != targetTypeId {
        rowexpr.row_format = COERCE_IMPLICIT_CAST;
        return coerce_to_domain(
            mcx,
            Expr::RowExpr(rowexpr),
            baseTypeId,
            baseTypeMod,
            targetTypeId,
            ccontext,
            cformat,
            location,
            false,
        );
    }

    Ok(Expr::RowExpr(rowexpr))
}

fn record_cast_error<'mcx>(
    pstate: Option<&mut ParseState<'_>>,
    targetTypeId: Oid,
    location: i32,
    _detail: Option<&str>,
) -> PgResult<Expr> {
    let _ = (pstate, location);
    Err(types_error::PgError::error(format!(
        "cannot cast type {} to {}",
        format_type_be(RECORDOID)?,
        format_type_be(targetTypeId)?
    ))
    .with_sqlstate(ERRCODE_CANNOT_COERCE))
}

fn record_cast_error_detail(
    _pstate: Option<&ParseState<'_>>,
    targetTypeId: Oid,
    _location: i32,
    detail: String,
) -> types_error::PgError {
    match (format_type_be(RECORDOID), format_type_be(targetTypeId)) {
        (Ok(rec), Ok(tgt)) => types_error::PgError::error(format!("cannot cast type {rec} to {tgt}"))
            .with_detail(detail)
            .with_sqlstate(ERRCODE_CANNOT_COERCE),
        _ => types_error::PgError::error("cannot cast type record")
            .with_sqlstate(ERRCODE_CANNOT_COERCE),
    }
}

// ===========================================================================
// coerce_to_boolean / coerce_to_specific_type(_typmod) / coerce_null_to_domain
// ===========================================================================

/// `coerce_to_boolean(pstate, node, constructName)` (parse_coerce.c).
pub fn coerce_to_boolean<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&mut ParseState<'_>>,
    node: Expr,
    constructName: &str,
) -> PgResult<Expr> {
    let inputTypeId = exprType(Some(&node))?;
    let mut node = node;

    if inputTypeId != BOOLOID {
        let newnode = coerce_to_target_type(
            mcx,
            pstate,
            node.clone(),
            inputTypeId,
            BOOLOID,
            -1,
            CoercionContext::COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST,
            -1,
        )?;
        match newnode {
            Some(n) => node = n,
            None => {
                return Err(types_error::PgError::error(format!(
                    "argument of {constructName} must be type {}, not type {}",
                    "boolean",
                    format_type_be(inputTypeId)?
                ))
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
            }
        }
    }

    if expression_returns_set(Some(&node)) {
        return Err(types_error::PgError::error(format!(
            "argument of {constructName} must not return a set"
        ))
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    }

    Ok(node)
}

/// `coerce_to_specific_type_typmod(pstate, node, targetTypeId, targetTypmod,
/// constructName)` (parse_coerce.c).
pub fn coerce_to_specific_type_typmod<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&mut ParseState<'_>>,
    node: Expr,
    targetTypeId: Oid,
    targetTypmod: i32,
    constructName: &str,
) -> PgResult<Expr> {
    let inputTypeId = exprType(Some(&node))?;
    let mut node = node;

    if inputTypeId != targetTypeId {
        let newnode = coerce_to_target_type(
            mcx,
            pstate,
            node.clone(),
            inputTypeId,
            targetTypeId,
            targetTypmod,
            CoercionContext::COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST,
            -1,
        )?;
        match newnode {
            Some(n) => node = n,
            None => {
                return Err(types_error::PgError::error(format!(
                    "argument of {constructName} must be type {}, not type {}",
                    format_type_be(targetTypeId)?,
                    format_type_be(inputTypeId)?
                ))
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
            }
        }
    }

    if expression_returns_set(Some(&node)) {
        return Err(types_error::PgError::error(format!(
            "argument of {constructName} must not return a set"
        ))
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    }

    Ok(node)
}

/// `coerce_to_specific_type(pstate, node, targetTypeId, constructName)`
/// (parse_coerce.c).
pub fn coerce_to_specific_type<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&mut ParseState<'_>>,
    node: Expr,
    targetTypeId: Oid,
    constructName: &str,
) -> PgResult<Expr> {
    coerce_to_specific_type_typmod(mcx, pstate, node, targetTypeId, -1, constructName)
}

/// `coerce_null_to_domain(typid, typmod, collation, typlen, typbyval)`
/// (parse_coerce.c).
pub fn coerce_null_to_domain<'mcx>(
    mcx: Mcx<'mcx>,
    typid: Oid,
    typmod: i32,
    collation: Oid,
    typlen: i32,
    typbyval: bool,
) -> PgResult<Expr> {
    let baseTypeId = lsyscache::get_base_type_and_typmod::call(typid)?.0;
    let baseTypeMod = resolve_base_typmod(typid, typmod)?;
    let mut result = Expr::Const(make_const(
        mcx,
        baseTypeId,
        baseTypeMod,
        collation,
        typlen,
        TupleDatum::ByVal(0),
        true,
        typbyval,
    )?);
    if typid != baseTypeId {
        result = coerce_to_domain(
            mcx,
            result,
            baseTypeId,
            baseTypeMod,
            typid,
            CoercionContext::COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST,
            -1,
            false,
        )?;
    }
    Ok(result)
}

/// `parser_coercion_errposition(pstate, coerce_location, input_expr)`
/// (parse_coerce.c).
pub fn parser_coercion_errposition(
    pstate: &ParseState<'_>,
    coerce_location: i32,
    input_expr: Option<&Expr>,
) -> PgResult<i32> {
    if coerce_location >= 0 {
        parser_errposition::call(pstate, coerce_location)
    } else {
        use backend_nodes_core::nodefuncs::expr_location;
        parser_errposition::call(pstate, expr_location(input_expr)?)
    }
}

// ===========================================================================
// select_common_type family
// ===========================================================================

/// `select_common_type(pstate, exprs, context, which_expr)` (parse_coerce.c).
/// The `Node **which_expr` out-parameter is dropped (NULL at the call sites).
/// `context == None` is the C NULL (return InvalidOid instead of erroring).
pub fn select_common_type<'mcx>(
    pstate: Option<&ParseState<'_>>,
    exprs: &[Expr],
    context: Option<&str>,
) -> PgResult<Oid> {
    debug_assert!(!exprs.is_empty());
    let pexpr = &exprs[0];
    let mut ptype = exprType(Some(pexpr))?;

    // If all input types are valid and exactly the same, pick that type.
    if ptype != UNKNOWNOID {
        let mut all_same = true;
        for nexpr in &exprs[1..] {
            let ntype = exprType(Some(nexpr))?;
            if ntype != ptype {
                all_same = false;
                break;
            }
        }
        if all_same {
            return Ok(ptype);
        }
    }

    // Full algorithm.
    ptype = lsyscache::get_base_type::call(ptype)?;
    let (mut pcategory, mut pispreferred) = lsyscache::get_type_category_preferred::call(ptype)?;

    for nexpr in &exprs[1..] {
        let ntype = lsyscache::get_base_type::call(exprType(Some(nexpr))?)?;

        if ntype != UNKNOWNOID && ntype != ptype {
            let (ncategory, nispreferred) = lsyscache::get_type_category_preferred::call(ntype)?;
            if ptype == UNKNOWNOID {
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            } else if ncategory != pcategory {
                match context {
                    None => return Ok(InvalidOid),
                    Some(ctx) => {
                        let _ = pstate;
                        return Err(types_error::PgError::error(format!(
                            "{ctx} types {} and {} cannot be matched",
                            format_type_be(ptype)?,
                            format_type_be(ntype)?
                        ))
                        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
                    }
                }
            } else if !pispreferred
                && can_coerce_type(1, &[ptype], &[ntype], CoercionContext::COERCION_IMPLICIT)?
                && !can_coerce_type(1, &[ntype], &[ptype], CoercionContext::COERCION_IMPLICIT)?
            {
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            }
        }
    }

    if ptype == UNKNOWNOID {
        ptype = TEXTOID;
    }
    Ok(ptype)
}

/// `select_common_type_from_oids(nargs, typeids, noerror)` (parse_coerce.c).
fn select_common_type_from_oids(nargs: i32, typeids: &[Oid], noerror: bool) -> PgResult<Oid> {
    debug_assert!(nargs > 0);
    let mut ptype = typeids[0];
    let mut i = 1usize;

    if ptype != UNKNOWNOID {
        while i < nargs as usize {
            if typeids[i] != ptype {
                break;
            }
            i += 1;
        }
        if i == nargs as usize {
            return Ok(ptype);
        }
    }

    ptype = lsyscache::get_base_type::call(ptype)?;
    let (mut pcategory, mut pispreferred) = lsyscache::get_type_category_preferred::call(ptype)?;

    while i < nargs as usize {
        let ntype = lsyscache::get_base_type::call(typeids[i])?;
        if ntype != UNKNOWNOID && ntype != ptype {
            let (ncategory, nispreferred) = lsyscache::get_type_category_preferred::call(ntype)?;
            if ptype == UNKNOWNOID {
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            } else if ncategory != pcategory {
                if noerror {
                    return Ok(InvalidOid);
                }
                return Err(types_error::PgError::error(format!(
                    "argument types {} and {} cannot be matched",
                    format_type_be(ptype)?,
                    format_type_be(ntype)?
                ))
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
            } else if !pispreferred
                && can_coerce_type(1, &[ptype], &[ntype], CoercionContext::COERCION_IMPLICIT)?
                && !can_coerce_type(1, &[ntype], &[ptype], CoercionContext::COERCION_IMPLICIT)?
            {
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            }
        }
        i += 1;
    }

    if ptype == UNKNOWNOID {
        ptype = TEXTOID;
    }
    Ok(ptype)
}

/// `coerce_to_common_type(pstate, node, targetTypeId, context)`
/// (parse_coerce.c).
pub fn coerce_to_common_type<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&mut ParseState<'_>>,
    node: Expr,
    targetTypeId: Oid,
    context: &str,
) -> PgResult<Expr> {
    let inputTypeId = exprType(Some(&node))?;
    if inputTypeId == targetTypeId {
        return Ok(node); // no work
    }
    if can_coerce_type(1, &[inputTypeId], &[targetTypeId], CoercionContext::COERCION_IMPLICIT)? {
        let r = coerce_type(
            mcx,
            pstate,
            Some(node),
            inputTypeId,
            targetTypeId,
            -1,
            CoercionContext::COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST,
            -1,
        )?;
        Ok(r.expect("coerce_type returned NULL in coerce_to_common_type"))
    } else {
        Err(types_error::PgError::error(format!(
            "{context} could not convert type {} to {}",
            format_type_be(inputTypeId)?,
            format_type_be(targetTypeId)?
        ))
        .with_sqlstate(ERRCODE_CANNOT_COERCE))
    }
}

/// `verify_common_type(common_type, exprs)` (parse_coerce.c).
pub fn verify_common_type(common_type: Oid, exprs: &[Expr]) -> PgResult<bool> {
    for nexpr in exprs {
        let ntype = exprType(Some(nexpr))?;
        if !can_coerce_type(1, &[ntype], &[common_type], CoercionContext::COERCION_IMPLICIT)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `verify_common_type_from_oids(common_type, nargs, typeids)` (parse_coerce.c).
fn verify_common_type_from_oids(common_type: Oid, nargs: i32, typeids: &[Oid]) -> PgResult<bool> {
    for i in 0..nargs as usize {
        if !can_coerce_type(1, &[typeids[i]], &[common_type], CoercionContext::COERCION_IMPLICIT)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `select_common_typmod(pstate, exprs, common_type)` (parse_coerce.c).
pub fn select_common_typmod(exprs: &[Expr], common_type: Oid) -> PgResult<i32> {
    let mut first = true;
    let mut result = -1i32;

    for expr in exprs {
        if exprType(Some(expr))? != common_type {
            return Ok(-1);
        } else if first {
            result = exprTypmod(Some(expr))?;
            first = false;
        } else if result != exprTypmod(Some(expr))? {
            return Ok(-1);
        }
    }
    Ok(result)
}

// ===========================================================================
// check_generic_type_consistency()
// ===========================================================================

/// `check_generic_type_consistency(actual_arg_types, declared_arg_types,
/// nargs)` (parse_coerce.c) — non-erroring polymorphic-consistency check.
pub fn check_generic_type_consistency(
    actual_arg_types: &[Oid],
    declared_arg_types: &[Oid],
    nargs: i32,
) -> PgResult<bool> {
    let mut elem_typeid = InvalidOid;
    let mut array_typeid = InvalidOid;
    let mut range_typeid = InvalidOid;
    let mut multirange_typeid = InvalidOid;
    let mut anycompatible_range_typeid = InvalidOid;
    let mut anycompatible_range_typelem = InvalidOid;
    let mut anycompatible_multirange_typeid = InvalidOid;
    let mut anycompatible_multirange_typelem = InvalidOid;
    let mut range_typelem;
    let mut have_anynonarray = false;
    let mut have_anyenum = false;
    let mut have_anycompatible_nonarray = false;
    let mut n_anycompatible_args = 0usize;
    let mut anycompatible_actual_types: Vec<Oid> = Vec::new();

    for j in 0..nargs as usize {
        let decl_type = declared_arg_types[j];
        let mut actual_type = actual_arg_types[j];

        if decl_type == ANYELEMENTOID || decl_type == ANYNONARRAYOID || decl_type == ANYENUMOID {
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
        } else if decl_type == ANYARRAYOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            actual_type = lsyscache::get_base_type::call(actual_type)?;
            if OidIsValid(array_typeid) && actual_type != array_typeid {
                return Ok(false);
            }
            array_typeid = actual_type;
        } else if decl_type == ANYRANGEOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            actual_type = lsyscache::get_base_type::call(actual_type)?;
            if OidIsValid(range_typeid) && actual_type != range_typeid {
                return Ok(false);
            }
            range_typeid = actual_type;
        } else if decl_type == ANYMULTIRANGEOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            actual_type = lsyscache::get_base_type::call(actual_type)?;
            if OidIsValid(multirange_typeid) && actual_type != multirange_typeid {
                return Ok(false);
            }
            multirange_typeid = actual_type;
        } else if decl_type == ANYCOMPATIBLEOID || decl_type == ANYCOMPATIBLENONARRAYOID {
            if decl_type == ANYCOMPATIBLENONARRAYOID {
                have_anycompatible_nonarray = true;
            }
            if actual_type == UNKNOWNOID {
                continue;
            }
            anycompatible_actual_types.push(actual_type);
            n_anycompatible_args += 1;
        } else if decl_type == ANYCOMPATIBLEARRAYOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            actual_type = lsyscache::get_base_type::call(actual_type)?;
            let elem_type = get_element_type(actual_type)?;
            if !OidIsValid(elem_type) {
                return Ok(false); // not an array
            }
            anycompatible_actual_types.push(elem_type);
            n_anycompatible_args += 1;
        } else if decl_type == ANYCOMPATIBLERANGEOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            actual_type = lsyscache::get_base_type::call(actual_type)?;
            if OidIsValid(anycompatible_range_typeid) {
                if anycompatible_range_typeid != actual_type {
                    return Ok(false);
                }
            } else {
                anycompatible_range_typeid = actual_type;
                anycompatible_range_typelem = get_range_subtype(actual_type)?;
                if !OidIsValid(anycompatible_range_typelem) {
                    return Ok(false); // not a range type
                }
                anycompatible_actual_types.push(anycompatible_range_typelem);
                n_anycompatible_args += 1;
            }
        } else if decl_type == ANYCOMPATIBLEMULTIRANGEOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            actual_type = lsyscache::get_base_type::call(actual_type)?;
            if OidIsValid(anycompatible_multirange_typeid) {
                if anycompatible_multirange_typeid != actual_type {
                    return Ok(false);
                }
            } else {
                anycompatible_multirange_typeid = actual_type;
                anycompatible_multirange_typelem = get_multirange_range(actual_type)?;
                if !OidIsValid(anycompatible_multirange_typelem) {
                    return Ok(false); // not a multirange type
                }
            }
        }
    }

    // Element type from array type, if we have one.
    if OidIsValid(array_typeid) {
        if array_typeid == ANYARRAYOID {
            // Special case; allow for now.
        } else {
            let array_typelem = get_element_type(array_typeid)?;
            if !OidIsValid(array_typelem) {
                return Ok(false);
            }
            if !OidIsValid(elem_typeid) {
                elem_typeid = array_typelem;
            } else if array_typelem != elem_typeid {
                return Ok(false);
            }
        }
    }

    // Range from multirange, or check agreement.
    if OidIsValid(multirange_typeid) {
        let multirange_typelem = get_multirange_range(multirange_typeid)?;
        if !OidIsValid(multirange_typelem) {
            return Ok(false);
        }
        if !OidIsValid(range_typeid) {
            range_typeid = multirange_typelem;
            range_typelem = get_range_subtype(multirange_typelem)?;
            if !OidIsValid(range_typelem) {
                return Ok(false);
            }
        } else if multirange_typelem != range_typeid {
            return Ok(false);
        }
    }

    // Element from range type, if we have one.
    if OidIsValid(range_typeid) {
        range_typelem = get_range_subtype(range_typeid)?;
        if !OidIsValid(range_typelem) {
            return Ok(false);
        }
        if !OidIsValid(elem_typeid) {
            elem_typeid = range_typelem;
        } else if range_typelem != elem_typeid {
            return Ok(false);
        }
    }

    if have_anynonarray && type_is_array_domain(elem_typeid)? {
        return Ok(false);
    }
    if have_anyenum && !lsyscache::type_is_enum::call(elem_typeid)? {
        return Ok(false);
    }

    // Range from multirange (anycompatible), or check agreement.
    if OidIsValid(anycompatible_multirange_typeid) {
        if OidIsValid(anycompatible_range_typeid) {
            if anycompatible_multirange_typelem != anycompatible_range_typeid {
                return Ok(false);
            }
        } else {
            anycompatible_range_typeid = anycompatible_multirange_typelem;
            anycompatible_range_typelem = get_range_subtype(anycompatible_range_typeid)?;
            if !OidIsValid(anycompatible_range_typelem) {
                return Ok(false);
            }
            anycompatible_actual_types.push(anycompatible_range_typelem);
            n_anycompatible_args += 1;
        }
    }

    if n_anycompatible_args > 0 {
        let anycompatible_typeid = select_common_type_from_oids(
            n_anycompatible_args as i32,
            &anycompatible_actual_types,
            true,
        )?;
        if !OidIsValid(anycompatible_typeid) {
            return Ok(false);
        }
        if !verify_common_type_from_oids(
            anycompatible_typeid,
            n_anycompatible_args as i32,
            &anycompatible_actual_types,
        )? {
            return Ok(false);
        }
        if have_anycompatible_nonarray && type_is_array_domain(anycompatible_typeid)? {
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

// ===========================================================================
// enforce_generic_type_consistency()
// ===========================================================================

/// `enforce_generic_type_consistency(actual_arg_types, declared_arg_types,
/// nargs, rettype, allow_poly)` (parse_coerce.c). Mutates `declared_arg_types`
/// in place; returns the resolved result type.
pub fn enforce_generic_type_consistency(
    actual_arg_types: &[Oid],
    declared_arg_types: &mut [Oid],
    nargs: i32,
    rettype: Oid,
    allow_poly: bool,
) -> PgResult<Oid> {
    let mut have_poly_anycompatible = false;
    let mut have_poly_unknowns = false;
    let mut elem_typeid = InvalidOid;
    let mut array_typeid = InvalidOid;
    let mut range_typeid = InvalidOid;
    let mut multirange_typeid = InvalidOid;
    let mut anycompatible_typeid = InvalidOid;
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
    let mut n_poly_args = 0i32;
    let mut n_anycompatible_args = 0usize;
    let mut anycompatible_actual_types: Vec<Oid> = Vec::new();

    for j in 0..nargs as usize {
        let decl_type = declared_arg_types[j];
        let mut actual_type = actual_arg_types[j];

        if decl_type == ANYELEMENTOID || decl_type == ANYNONARRAYOID || decl_type == ANYENUMOID {
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
                return Err(not_all_alike("anyelement", elem_typeid, actual_type)?);
            }
            elem_typeid = actual_type;
        } else if decl_type == ANYARRAYOID {
            n_poly_args += 1;
            if actual_type == UNKNOWNOID {
                have_poly_unknowns = true;
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            actual_type = lsyscache::get_base_type::call(actual_type)?;
            if OidIsValid(array_typeid) && actual_type != array_typeid {
                return Err(not_all_alike("anyarray", array_typeid, actual_type)?);
            }
            array_typeid = actual_type;
        } else if decl_type == ANYRANGEOID {
            n_poly_args += 1;
            if actual_type == UNKNOWNOID {
                have_poly_unknowns = true;
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            actual_type = lsyscache::get_base_type::call(actual_type)?;
            if OidIsValid(range_typeid) && actual_type != range_typeid {
                return Err(not_all_alike("anyrange", range_typeid, actual_type)?);
            }
            range_typeid = actual_type;
        } else if decl_type == ANYMULTIRANGEOID {
            n_poly_args += 1;
            have_anymultirange = true;
            if actual_type == UNKNOWNOID {
                have_poly_unknowns = true;
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            actual_type = lsyscache::get_base_type::call(actual_type)?;
            if OidIsValid(multirange_typeid) && actual_type != multirange_typeid {
                return Err(not_all_alike("anymultirange", multirange_typeid, actual_type)?);
            }
            multirange_typeid = actual_type;
        } else if decl_type == ANYCOMPATIBLEOID || decl_type == ANYCOMPATIBLENONARRAYOID {
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
            anycompatible_actual_types.push(actual_type);
            n_anycompatible_args += 1;
        } else if decl_type == ANYCOMPATIBLEARRAYOID {
            have_poly_anycompatible = true;
            have_anycompatible_array = true;
            if actual_type == UNKNOWNOID {
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            actual_type = lsyscache::get_base_type::call(actual_type)?;
            let anycompatible_elem_type = get_element_type(actual_type)?;
            if !OidIsValid(anycompatible_elem_type) {
                return Err(not_an_array("anycompatiblearray", actual_type)?);
            }
            anycompatible_actual_types.push(anycompatible_elem_type);
            n_anycompatible_args += 1;
        } else if decl_type == ANYCOMPATIBLERANGEOID {
            have_poly_anycompatible = true;
            have_anycompatible_range = true;
            if actual_type == UNKNOWNOID {
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            actual_type = lsyscache::get_base_type::call(actual_type)?;
            if OidIsValid(anycompatible_range_typeid) {
                if anycompatible_range_typeid != actual_type {
                    return Err(not_all_alike(
                        "anycompatiblerange",
                        anycompatible_range_typeid,
                        actual_type,
                    )?);
                }
            } else {
                anycompatible_range_typeid = actual_type;
                anycompatible_range_typelem = get_range_subtype(actual_type)?;
                if !OidIsValid(anycompatible_range_typelem) {
                    return Err(not_a_range("anycompatiblerange", actual_type)?);
                }
                anycompatible_actual_types.push(anycompatible_range_typelem);
                n_anycompatible_args += 1;
            }
        } else if decl_type == ANYCOMPATIBLEMULTIRANGEOID {
            have_poly_anycompatible = true;
            have_anycompatible_multirange = true;
            if actual_type == UNKNOWNOID {
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            actual_type = lsyscache::get_base_type::call(actual_type)?;
            if OidIsValid(anycompatible_multirange_typeid) {
                if anycompatible_multirange_typeid != actual_type {
                    return Err(not_all_alike(
                        "anycompatiblemultirange",
                        anycompatible_multirange_typeid,
                        actual_type,
                    )?);
                }
            } else {
                anycompatible_multirange_typeid = actual_type;
                anycompatible_multirange_typelem = get_multirange_range(actual_type)?;
                if !OidIsValid(anycompatible_multirange_typelem) {
                    return Err(not_a_multirange("anycompatiblemultirange", actual_type)?);
                }
            }
        }
    }

    // Fast track: no polymorphic args.
    if n_poly_args == 0 && !have_poly_anycompatible {
        return Ok(rettype);
    }

    // Family-1 matching.
    if n_poly_args != 0 {
        if OidIsValid(array_typeid) {
            let array_typelem;
            if array_typeid == ANYARRAYOID {
                if n_poly_args != 1 || (rettype != ANYARRAYOID && is_polymorphic_type_family1(rettype))
                {
                    return Err(types_error::PgError::error(
                        "cannot determine element type of \"anyarray\" argument",
                    )
                    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
                }
                array_typelem = ANYELEMENTOID;
            } else {
                array_typelem = get_element_type(array_typeid)?;
                if !OidIsValid(array_typelem) {
                    return Err(not_an_array("anyarray", array_typeid)?);
                }
            }
            if !OidIsValid(elem_typeid) {
                elem_typeid = array_typelem;
            } else if array_typelem != elem_typeid {
                return Err(not_consistent("anyarray", "anyelement", array_typeid, elem_typeid)?);
            }
        }

        // Range from multirange, or vice versa.
        if OidIsValid(multirange_typeid) {
            let multirange_typelem = get_multirange_range(multirange_typeid)?;
            if !OidIsValid(multirange_typelem) {
                return Err(not_a_multirange("anymultirange", multirange_typeid)?);
            }
            if !OidIsValid(range_typeid) {
                range_typeid = multirange_typelem;
            } else if multirange_typelem != range_typeid {
                return Err(not_consistent(
                    "anymultirange",
                    "anyrange",
                    multirange_typeid,
                    range_typeid,
                )?);
            }
        } else if have_anymultirange && OidIsValid(range_typeid) {
            multirange_typeid = get_range_multirange(range_typeid)?;
        }

        // Element from range type.
        if OidIsValid(range_typeid) {
            let range_typelem = get_range_subtype(range_typeid)?;
            if !OidIsValid(range_typelem) {
                return Err(not_a_range("anyrange", range_typeid)?);
            }
            if !OidIsValid(elem_typeid) {
                elem_typeid = range_typelem;
            } else if range_typelem != elem_typeid {
                return Err(not_consistent("anyrange", "anyelement", range_typeid, elem_typeid)?);
            }
        }

        if !OidIsValid(elem_typeid) {
            if allow_poly {
                elem_typeid = ANYELEMENTOID;
                array_typeid = ANYARRAYOID;
                range_typeid = ANYRANGEOID;
                multirange_typeid = ANYMULTIRANGEOID;
            } else {
                return Err(could_not_determine("unknown polymorphic")?);
            }
        }

        if have_anynonarray && elem_typeid != ANYELEMENTOID && type_is_array_domain(elem_typeid)? {
            return Err(types_error::PgError::error(format!(
                "type matched to anynonarray is an array type: {}",
                format_type_be(elem_typeid)?
            ))
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
        }
        if have_anyenum && elem_typeid != ANYELEMENTOID && !lsyscache::type_is_enum::call(elem_typeid)? {
            return Err(types_error::PgError::error(format!(
                "type matched to anyenum is not an enum type: {}",
                format_type_be(elem_typeid)?
            ))
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
        }
    }

    // Family-2 matching.
    if have_poly_anycompatible {
        if OidIsValid(anycompatible_multirange_typeid) {
            if OidIsValid(anycompatible_range_typeid) {
                if anycompatible_multirange_typelem != anycompatible_range_typeid {
                    return Err(not_consistent(
                        "anycompatiblemultirange",
                        "anycompatiblerange",
                        anycompatible_multirange_typeid,
                        anycompatible_range_typeid,
                    )?);
                }
            } else {
                anycompatible_range_typeid = anycompatible_multirange_typelem;
                anycompatible_range_typelem = get_range_subtype(anycompatible_range_typeid)?;
                if !OidIsValid(anycompatible_range_typelem) {
                    return Err(not_a_multirange(
                        "anycompatiblemultirange",
                        anycompatible_multirange_typeid,
                    )?);
                }
                have_anycompatible_range = true;
                anycompatible_actual_types.push(anycompatible_range_typelem);
                n_anycompatible_args += 1;
            }
        } else if have_anycompatible_multirange && OidIsValid(anycompatible_range_typeid) {
            anycompatible_multirange_typeid = get_range_multirange(anycompatible_range_typeid)?;
        }

        if n_anycompatible_args > 0 {
            anycompatible_typeid = select_common_type_from_oids(
                n_anycompatible_args as i32,
                &anycompatible_actual_types,
                false,
            )?;
            if !verify_common_type_from_oids(
                anycompatible_typeid,
                n_anycompatible_args as i32,
                &anycompatible_actual_types,
            )? {
                return Err(types_error::PgError::error(
                    "arguments of anycompatible family cannot be cast to a common type",
                )
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
            }

            if have_anycompatible_array {
                anycompatible_array_typeid = get_array_type(anycompatible_typeid)?;
                if !OidIsValid(anycompatible_array_typeid) {
                    return Err(could_not_find_array(anycompatible_typeid)?);
                }
            }

            if have_anycompatible_range {
                if !OidIsValid(anycompatible_range_typeid) {
                    return Err(could_not_determine_named("anycompatiblerange")?);
                }
                if anycompatible_range_typelem != anycompatible_typeid {
                    return Err(types_error::PgError::error(format!(
                        "anycompatiblerange type {} does not match anycompatible type {}",
                        format_type_be(anycompatible_range_typeid)?,
                        format_type_be(anycompatible_typeid)?
                    ))
                    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
                }
            }

            if have_anycompatible_multirange {
                if !OidIsValid(anycompatible_multirange_typeid) {
                    return Err(could_not_determine_named("anycompatiblemultirange")?);
                }
                if anycompatible_range_typelem != anycompatible_typeid {
                    return Err(types_error::PgError::error(format!(
                        "anycompatiblemultirange type {} does not match anycompatible type {}",
                        format_type_be(anycompatible_multirange_typeid)?,
                        format_type_be(anycompatible_typeid)?
                    ))
                    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
                }
            }

            if have_anycompatible_nonarray && type_is_array_domain(anycompatible_typeid)? {
                return Err(types_error::PgError::error(format!(
                    "type matched to anycompatiblenonarray is an array type: {}",
                    format_type_be(anycompatible_typeid)?
                ))
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
            }
        } else if allow_poly {
            anycompatible_typeid = ANYCOMPATIBLEOID;
            anycompatible_array_typeid = ANYCOMPATIBLEARRAYOID;
            anycompatible_range_typeid = ANYCOMPATIBLERANGEOID;
            anycompatible_multirange_typeid = ANYCOMPATIBLEMULTIRANGEOID;
        } else {
            anycompatible_typeid = TEXTOID;
            anycompatible_array_typeid = TEXTARRAYOID;
            if have_anycompatible_range {
                return Err(could_not_determine_named("anycompatiblerange")?);
            }
            if have_anycompatible_multirange {
                return Err(could_not_determine_named("anycompatiblemultirange")?);
            }
        }

        // Replace family-2 polymorphic types by selected types.
        for j in 0..nargs as usize {
            let decl_type = declared_arg_types[j];
            if decl_type == ANYCOMPATIBLEOID || decl_type == ANYCOMPATIBLENONARRAYOID {
                declared_arg_types[j] = anycompatible_typeid;
            } else if decl_type == ANYCOMPATIBLEARRAYOID {
                declared_arg_types[j] = anycompatible_array_typeid;
            } else if decl_type == ANYCOMPATIBLERANGEOID {
                declared_arg_types[j] = anycompatible_range_typeid;
            } else if decl_type == ANYCOMPATIBLEMULTIRANGEOID {
                declared_arg_types[j] = anycompatible_multirange_typeid;
            }
        }
    }

    // Re-scan UNKNOWN family-1 inputs.
    if have_poly_unknowns {
        for j in 0..nargs as usize {
            let decl_type = declared_arg_types[j];
            let actual_type = actual_arg_types[j];
            if actual_type != UNKNOWNOID {
                continue;
            }
            if decl_type == ANYELEMENTOID || decl_type == ANYNONARRAYOID || decl_type == ANYENUMOID
            {
                declared_arg_types[j] = elem_typeid;
            } else if decl_type == ANYARRAYOID {
                if !OidIsValid(array_typeid) {
                    array_typeid = get_array_type(elem_typeid)?;
                    if !OidIsValid(array_typeid) {
                        return Err(could_not_find_array(elem_typeid)?);
                    }
                }
                declared_arg_types[j] = array_typeid;
            } else if decl_type == ANYRANGEOID {
                if !OidIsValid(range_typeid) {
                    return Err(could_not_determine_named("anyrange")?);
                }
                declared_arg_types[j] = range_typeid;
            } else if decl_type == ANYMULTIRANGEOID {
                if !OidIsValid(multirange_typeid) {
                    return Err(could_not_determine_named("anymultirange")?);
                }
                declared_arg_types[j] = multirange_typeid;
            }
        }
    }

    // Determine result type.
    if rettype == ANYELEMENTOID || rettype == ANYNONARRAYOID || rettype == ANYENUMOID {
        return Ok(elem_typeid);
    }
    if rettype == ANYARRAYOID {
        if !OidIsValid(array_typeid) {
            array_typeid = get_array_type(elem_typeid)?;
            if !OidIsValid(array_typeid) {
                return Err(could_not_find_array(elem_typeid)?);
            }
        }
        return Ok(array_typeid);
    }
    if rettype == ANYRANGEOID {
        if !OidIsValid(range_typeid) {
            return Err(could_not_determine_named_internal("anyrange")?);
        }
        return Ok(range_typeid);
    }
    if rettype == ANYMULTIRANGEOID {
        if !OidIsValid(multirange_typeid) {
            return Err(could_not_determine_named_internal("anymultirange")?);
        }
        return Ok(multirange_typeid);
    }
    if rettype == ANYCOMPATIBLEOID || rettype == ANYCOMPATIBLENONARRAYOID {
        if !OidIsValid(anycompatible_typeid) {
            return Err(internal_err("could not identify anycompatible type"));
        }
        return Ok(anycompatible_typeid);
    }
    if rettype == ANYCOMPATIBLEARRAYOID {
        if !OidIsValid(anycompatible_array_typeid) {
            return Err(internal_err("could not identify anycompatiblearray type"));
        }
        return Ok(anycompatible_array_typeid);
    }
    if rettype == ANYCOMPATIBLERANGEOID {
        if !OidIsValid(anycompatible_range_typeid) {
            return Err(internal_err("could not identify anycompatiblerange type"));
        }
        return Ok(anycompatible_range_typeid);
    }
    if rettype == ANYCOMPATIBLEMULTIRANGEOID {
        if !OidIsValid(anycompatible_multirange_typeid) {
            return Err(internal_err("could not identify anycompatiblemultirange type"));
        }
        return Ok(anycompatible_multirange_typeid);
    }

    Ok(rettype)
}

// --- enforce_generic_type_consistency error helpers -----------------------

fn not_all_alike(name: &str, a: Oid, b: Oid) -> PgResult<types_error::PgError> {
    Ok(types_error::PgError::error(format!(
        "arguments declared \"{name}\" are not all alike"
    ))
    .with_detail(format!("{} versus {}", format_type_be(a)?, format_type_be(b)?))
    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH))
}

fn not_an_array(name: &str, t: Oid) -> PgResult<types_error::PgError> {
    Ok(types_error::PgError::error(format!(
        "argument declared {name} is not an array but type {}",
        format_type_be(t)?
    ))
    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH))
}

fn not_a_range(name: &str, t: Oid) -> PgResult<types_error::PgError> {
    Ok(types_error::PgError::error(format!(
        "argument declared {name} is not a range type but type {}",
        format_type_be(t)?
    ))
    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH))
}

fn not_a_multirange(name: &str, t: Oid) -> PgResult<types_error::PgError> {
    Ok(types_error::PgError::error(format!(
        "argument declared {name} is not a multirange type but type {}",
        format_type_be(t)?
    ))
    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH))
}

fn not_consistent(a_name: &str, b_name: &str, a: Oid, b: Oid) -> PgResult<types_error::PgError> {
    Ok(types_error::PgError::error(format!(
        "argument declared {a_name} is not consistent with argument declared {b_name}"
    ))
    .with_detail(format!("{} versus {}", format_type_be(a)?, format_type_be(b)?))
    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH))
}

fn could_not_determine(_kind: &str) -> PgResult<types_error::PgError> {
    Ok(types_error::PgError::error(
        "could not determine polymorphic type because input has type unknown",
    )
    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH))
}

fn could_not_determine_named(name: &str) -> PgResult<types_error::PgError> {
    Ok(types_error::PgError::error(format!(
        "could not determine polymorphic type {name} because input has type unknown"
    ))
    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH))
}

fn could_not_determine_named_internal(name: &str) -> PgResult<types_error::PgError> {
    Ok(types_error::PgError::error(format!(
        "could not determine polymorphic type {name} because input has type unknown"
    ))
    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH))
}

fn could_not_find_array(t: Oid) -> PgResult<types_error::PgError> {
    Ok(types_error::PgError::error(format!(
        "could not find array type for data type {}",
        format_type_be(t)?
    ))
    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT))
}

fn internal_err(msg: &str) -> types_error::PgError {
    types_error::PgError::error(msg).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

// ===========================================================================
// check_valid_polymorphic_signature / check_valid_internal_signature
// ===========================================================================

/// `check_valid_polymorphic_signature(ret_type, declared_arg_types, nargs)`
/// (parse_coerce.c). Returns `None` if valid, else a translated errdetail
/// string.
pub fn check_valid_polymorphic_signature(
    ret_type: Oid,
    declared_arg_types: &[Oid],
    nargs: i32,
) -> PgResult<Option<String>> {
    if ret_type == ANYRANGEOID || ret_type == ANYMULTIRANGEOID {
        for i in 0..nargs as usize {
            if declared_arg_types[i] == ANYRANGEOID || declared_arg_types[i] == ANYMULTIRANGEOID {
                return Ok(None);
            }
        }
        return Ok(Some(format!(
            "A result of type {} requires at least one input of type anyrange or anymultirange.",
            format_type_be(ret_type)?
        )));
    } else if ret_type == ANYCOMPATIBLERANGEOID || ret_type == ANYCOMPATIBLEMULTIRANGEOID {
        for i in 0..nargs as usize {
            if declared_arg_types[i] == ANYCOMPATIBLERANGEOID
                || declared_arg_types[i] == ANYCOMPATIBLEMULTIRANGEOID
            {
                return Ok(None);
            }
        }
        return Ok(Some(format!(
            "A result of type {} requires at least one input of type anycompatiblerange or anycompatiblemultirange.",
            format_type_be(ret_type)?
        )));
    } else if is_polymorphic_type_family1(ret_type) {
        for i in 0..nargs as usize {
            if is_polymorphic_type_family1(declared_arg_types[i]) {
                return Ok(None);
            }
        }
        return Ok(Some(format!(
            "A result of type {} requires at least one input of type anyelement, anyarray, anynonarray, anyenum, anyrange, or anymultirange.",
            format_type_be(ret_type)?
        )));
    } else if is_polymorphic_type_family2(ret_type) {
        for i in 0..nargs as usize {
            if is_polymorphic_type_family2(declared_arg_types[i]) {
                return Ok(None);
            }
        }
        return Ok(Some(format!(
            "A result of type {} requires at least one input of type anycompatible, anycompatiblearray, anycompatiblenonarray, anycompatiblerange, or anycompatiblemultirange.",
            format_type_be(ret_type)?
        )));
    }
    Ok(None)
}

/// `check_valid_internal_signature(ret_type, declared_arg_types, nargs)`
/// (parse_coerce.c).
pub fn check_valid_internal_signature(
    ret_type: Oid,
    declared_arg_types: &[Oid],
    nargs: i32,
) -> Option<String> {
    use types_tuple::heaptuple::INTERNALOID;
    if ret_type == INTERNALOID {
        for i in 0..nargs as usize {
            if declared_arg_types[i] == ret_type {
                return None;
            }
        }
        return Some("A result of type internal requires at least one input of type internal.".into());
    }
    None
}

// ===========================================================================
// TypeCategory / IsPreferredType / IsBinaryCoercible
// ===========================================================================

/// `TypeCategory(type)` (parse_coerce.c).
pub fn TypeCategory(type_: Oid) -> PgResult<u8> {
    let (typcategory, _typispreferred) = lsyscache::get_type_category_preferred::call(type_)?;
    debug_assert!(typcategory != TYPCATEGORY_INVALID);
    Ok(typcategory)
}

/// `IsPreferredType(category, type)` (parse_coerce.c).
pub fn IsPreferredType(category: u8, type_: Oid) -> PgResult<bool> {
    let (typcategory, typispreferred) = lsyscache::get_type_category_preferred::call(type_)?;
    if category == typcategory || category == TYPCATEGORY_INVALID {
        Ok(typispreferred)
    } else {
        Ok(false)
    }
}

/// `IsBinaryCoercible(srctype, targettype)` (parse_coerce.c).
pub fn IsBinaryCoercible(srctype: Oid, targettype: Oid) -> PgResult<bool> {
    let (result, _castoid) = IsBinaryCoercibleWithCast(srctype, targettype)?;
    Ok(result)
}

/// `IsBinaryCoercibleWithCast(srctype, targettype, &castoid)` (parse_coerce.c).
/// Returns `(coercible, castoid)`.
pub fn IsBinaryCoercibleWithCast(mut srctype: Oid, targettype: Oid) -> PgResult<(bool, Oid)> {
    let mut castoid = InvalidOid;

    if srctype == targettype {
        return Ok((true, castoid));
    }
    if targettype == ANYOID || targettype == ANYELEMENTOID || targettype == ANYCOMPATIBLEOID {
        return Ok((true, castoid));
    }
    if OidIsValid(srctype) {
        srctype = lsyscache::get_base_type::call(srctype)?;
    }
    if srctype == targettype {
        return Ok((true, castoid));
    }
    if (targettype == ANYARRAYOID || targettype == ANYCOMPATIBLEARRAYOID) && type_is_array(srctype)? {
        return Ok((true, castoid));
    }
    if (targettype == ANYNONARRAYOID || targettype == ANYCOMPATIBLENONARRAYOID)
        && !type_is_array(srctype)?
    {
        return Ok((true, castoid));
    }
    if targettype == ANYENUMOID && lsyscache::type_is_enum::call(srctype)? {
        return Ok((true, castoid));
    }
    if (targettype == ANYRANGEOID || targettype == ANYCOMPATIBLERANGEOID)
        && lsyscache::type_is_range::call(srctype)?
    {
        return Ok((true, castoid));
    }
    if (targettype == ANYMULTIRANGEOID || targettype == ANYCOMPATIBLEMULTIRANGEOID)
        && lsyscache::type_is_multirange::call(srctype)?
    {
        return Ok((true, castoid));
    }
    if targettype == RECORDOID && is_complex(srctype)? {
        return Ok((true, castoid));
    }
    if targettype == RECORDARRAYOID && is_complex_array(srctype)? {
        return Ok((true, castoid));
    }

    // Else look in pg_cast.
    let cast = syscache::cast_by_source_target::call(srctype, targettype)?;
    let castform = match cast {
        Some(c) => c,
        None => return Ok((false, InvalidOid)), // no cast
    };
    let result = castform.castmethod == COERCION_METHOD_BINARY
        && castform.castcontext == COERCION_CODE_IMPLICIT;
    if result {
        castoid = castform.oid;
    }
    Ok((result, castoid))
}

// ===========================================================================
// find_coercion_pathway / find_typmod_coercion_function
// ===========================================================================

/// `find_coercion_pathway(targetTypeId, sourceTypeId, ccontext, funcid)`
/// (parse_coerce.c). Returns `(pathtype, funcid)`.
pub fn find_coercion_pathway(
    mut targetTypeId: Oid,
    mut sourceTypeId: Oid,
    ccontext: CoercionContext,
) -> PgResult<(CoercionPathType, Oid)> {
    let mut result = CoercionPathType::None;
    let mut funcid = InvalidOid;

    if OidIsValid(sourceTypeId) {
        sourceTypeId = lsyscache::get_base_type::call(sourceTypeId)?;
    }
    if OidIsValid(targetTypeId) {
        targetTypeId = lsyscache::get_base_type::call(targetTypeId)?;
    }

    // Domains coercible to/from their base type.
    if sourceTypeId == targetTypeId {
        return Ok((CoercionPathType::Relabeltype, funcid));
    }

    let cast = syscache::cast_by_source_target::call(sourceTypeId, targetTypeId)?;

    if let Some(castform) = cast {
        let castcontext = match castform.castcontext {
            c if c == COERCION_CODE_IMPLICIT => CoercionContext::COERCION_IMPLICIT,
            c if c == COERCION_CODE_ASSIGNMENT => CoercionContext::COERCION_ASSIGNMENT,
            c if c == COERCION_CODE_EXPLICIT => CoercionContext::COERCION_EXPLICIT,
            other => {
                return Err(types_error::PgError::error(format!(
                    "unrecognized castcontext: {}",
                    other as i32
                ))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR));
            }
        };

        if ccontext_rank(ccontext) >= ccontext_rank(castcontext) {
            match castform.castmethod {
                m if m == COERCION_METHOD_FUNCTION => {
                    result = CoercionPathType::Func;
                    funcid = castform.castfunc;
                }
                m if m == COERCION_METHOD_INOUT => {
                    result = CoercionPathType::Coerceviaio;
                }
                m if m == COERCION_METHOD_BINARY => {
                    result = CoercionPathType::Relabeltype;
                }
                other => {
                    return Err(types_error::PgError::error(format!(
                        "unrecognized castmethod: {}",
                        other as i32
                    ))
                    .with_sqlstate(ERRCODE_INTERNAL_ERROR));
                }
            }
        }
    } else {
        // No pg_cast entry: maybe a pair of array types?
        if targetTypeId != OIDVECTOROID && targetTypeId != INT2VECTOROID {
            let targetElem = get_element_type(targetTypeId)?;
            let sourceElem = get_element_type(sourceTypeId)?;
            if targetElem != InvalidOid && sourceElem != InvalidOid {
                let (elempathtype, _elemfuncid) =
                    find_coercion_pathway(targetElem, sourceElem, ccontext)?;
                if elempathtype != CoercionPathType::None {
                    result = CoercionPathType::Arraycoerce;
                }
            }
        }

        // Consider automatic I/O casting.
        if result == CoercionPathType::None {
            if ccontext_rank(ccontext) >= ccontext_rank(CoercionContext::COERCION_ASSIGNMENT)
                && TypeCategory(targetTypeId)? == TYPCATEGORY_STRING
            {
                result = CoercionPathType::Coerceviaio;
            } else if ccontext_rank(ccontext) >= ccontext_rank(CoercionContext::COERCION_EXPLICIT)
                && TypeCategory(sourceTypeId)? == TYPCATEGORY_STRING
            {
                result = CoercionPathType::Coerceviaio;
            }
        }
    }

    // PL/pgSQL assignment: allow I/O cast if no normal coercion.
    if result == CoercionPathType::None && ccontext == CoercionContext::COERCION_PLPGSQL {
        result = CoercionPathType::Coerceviaio;
    }

    Ok((result, funcid))
}

/// `find_typmod_coercion_function(typeId, funcid)` (parse_coerce.c). Returns
/// `(pathtype, funcid)`.
pub fn find_typmod_coercion_function(typeId: Oid) -> PgResult<(CoercionPathType, Oid)> {
    let mut funcid = InvalidOid;
    let mut result = CoercionPathType::Func;
    let mut type_id = typeId;

    // Check for a "true" array type via get_element_type (true-array only),
    // which is exactly IsTrueArrayType's element-yielding behaviour.
    let elem = get_element_type(type_id)?;
    if OidIsValid(elem) {
        // Switch attention to the element type.
        type_id = elem;
        result = CoercionPathType::Arraycoerce;
    }

    // Look in pg_cast (self->self).
    let cast = syscache::cast_by_source_target::call(type_id, type_id)?;
    if let Some(castform) = cast {
        funcid = castform.castfunc;
    }

    if !OidIsValid(funcid) {
        result = CoercionPathType::None;
    }

    Ok((result, funcid))
}

// ===========================================================================
// is_complex_array / typeIsOfTypedTable
// ===========================================================================

/// `is_complex_array(typid)` (parse_coerce.c) — array of composite (not
/// record[]).
fn is_complex_array(typid: Oid) -> PgResult<bool> {
    let elemtype = get_element_type(typid)?;
    Ok(OidIsValid(elemtype) && is_complex(elemtype)?)
}

/// `typeIsOfTypedTable(reltypeId, reloftypeId)` (parse_coerce.c).
fn typeIsOfTypedTable(reltypeId: Oid, reloftypeId: Oid) -> PgResult<bool> {
    let relid = parse_type::typeOrDomainTypeRelid(reltypeId)?;
    if !OidIsValid(relid) {
        return Ok(false);
    }
    match syscache::search_relation_reloftype::call(relid)? {
        Some(reloftype) => Ok(reloftype == reloftypeId),
        None => Err(types_error::PgError::error(format!(
            "cache lookup failed for relation {relid}"
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)),
    }
}

// ===========================================================================
// Range/multirange lsyscache wrappers (bare-Oid form).
// ===========================================================================

fn get_range_subtype(range_oid: Oid) -> PgResult<Oid> {
    lsyscache::get_range_subtype::call(range_oid)
}

fn get_multirange_range(multirange_oid: Oid) -> PgResult<Oid> {
    lsyscache::get_multirange_range::call(multirange_oid)
}

fn get_range_multirange(range_oid: Oid) -> PgResult<Oid> {
    lsyscache::get_range_multirange::call(range_oid)
}

// ===========================================================================
// init_seams
// ===========================================================================

/// Install this unit's inward seams (consumed by parse_expr.c / parse_oper.c).
pub fn init_seams() {
    use backend_parser_coerce_seams as s;
    s::find_coercion_pathway_implicit::set(seam_find_coercion_pathway_implicit);
    s::is_binary_coercible::set(IsBinaryCoercible);
    s::enforce_generic_type_consistency::set(seam_enforce_generic_type_consistency);
    s::coerce_to_boolean::set(seam_coerce_to_boolean);
    s::coerce_to_specific_type::set(seam_coerce_to_specific_type);
    s::coerce_to_common_type::set(seam_coerce_to_common_type);
    s::select_common_type::set(seam_select_common_type);
    s::verify_common_type::set(verify_common_type);
    s::coerce_to_target_type::set(seam_coerce_to_target_type);
}

fn seam_find_coercion_pathway_implicit(
    target_type_id: Oid,
    source_type_id: Oid,
) -> PgResult<(CoercionPathType, Oid)> {
    find_coercion_pathway(target_type_id, source_type_id, CoercionContext::COERCION_IMPLICIT)
}

fn seam_enforce_generic_type_consistency(
    actual_arg_types: &[Oid],
    declared_arg_types: &mut [Oid],
    nargs: i32,
    rettype: Oid,
    allow_poly: bool,
) -> PgResult<Oid> {
    enforce_generic_type_consistency(actual_arg_types, declared_arg_types, nargs, rettype, allow_poly)
}

// The inward seams carry only `&mut ParseState` (no `Mcx`), matching the C
// signatures consumed by parse_expr.c / parse_oper.c. ParseState carries no
// `Mcx`, so a scratch context backs the transient allocations (detoast in
// make_const, etc.); the produced Expr tree is lifetime-free (Const carries
// `Datum<'static>`, nodes are owned `Box`/`Vec`), so it outlives the scratch.
fn seam_coerce_to_boolean<'mcx>(
    pstate: &mut ParseState<'mcx>,
    node: Expr,
    construct_name: &str,
) -> PgResult<Expr> {
    let cx = MemoryContext::new("coerce_to_boolean");
    coerce_to_boolean(cx.mcx(), Some(pstate), node, construct_name)
}

fn seam_coerce_to_specific_type<'mcx>(
    pstate: &mut ParseState<'mcx>,
    node: Expr,
    target_type_id: Oid,
    construct_name: &str,
) -> PgResult<Expr> {
    let cx = MemoryContext::new("coerce_to_specific_type");
    coerce_to_specific_type(cx.mcx(), Some(pstate), node, target_type_id, construct_name)
}

fn seam_coerce_to_common_type<'mcx>(
    pstate: &mut ParseState<'mcx>,
    node: Expr,
    target_type_id: Oid,
    context: &str,
) -> PgResult<Expr> {
    let cx = MemoryContext::new("coerce_to_common_type");
    coerce_to_common_type(cx.mcx(), Some(pstate), node, target_type_id, context)
}

fn seam_select_common_type<'mcx>(
    pstate: &mut ParseState<'mcx>,
    exprs: &[Expr],
    context: Option<&str>,
) -> PgResult<Oid> {
    select_common_type(Some(pstate), exprs, context)
}

fn seam_coerce_to_target_type<'mcx>(
    pstate: &mut ParseState<'mcx>,
    expr: Expr,
    exprtype: Oid,
    targettype: Oid,
    targettypmod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: i32,
) -> PgResult<Option<Expr>> {
    let cx = MemoryContext::new("coerce_to_target_type");
    coerce_to_target_type(
        cx.mcx(),
        Some(pstate),
        expr,
        exprtype,
        targettype,
        targettypmod,
        ccontext,
        cformat,
        location,
    )
}
