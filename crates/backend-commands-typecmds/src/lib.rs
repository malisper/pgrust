#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
// `PgError` is a large shared error type across the whole tree; boxing it would
// diverge from every sibling command crate.
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::manual_range_contains)]

//! Port of `backend/commands/typecmds.c` Family F2 — CREATE TYPE (base / enum /
//! range / composite) plus `RemoveTypeById` (PostgreSQL 18.3).
//!
//! Implemented in-crate with identical control flow, branch order, constants,
//! `TypeCreate` field writes, error codes/messages, and option-decision matrix
//! as PostgreSQL 18.3:
//!
//!   * [`DefineType`]            (typecmds.c:152-651);
//!   * [`RemoveTypeById`]        (typecmds.c:656-689) — the inward seam, real body;
//!   * [`DefineEnum`]            (typecmds.c:1181-1298);
//!   * [`DefineRange`]           (typecmds.c:1380-1759);
//!   * [`DefineCompositeType`]   (typecmds.c:2555-2604);
//!   * `findType{Input,Output,Receive,Send,Typmodin,Typmodout,Analyze,Subscripting}Function`
//!     (typecmds.c:1991-2309);
//!   * `findRange{SubOpclass,CanonicalFunction,SubtypeDiffFunction}`
//!     (typecmds.c:2319-2440);
//!   * `AssignTypeArrayOid` / `AssignTypeMultirangeOid` /
//!     `AssignTypeMultirangeArrayOid` (typecmds.c:2447-2539).
//!
//! Domains (F3) and ALTER (F4) are deliberately out of scope; the F2 entry
//! points never call them.
//!
//! `TypeCreate`/`TypeShellMake`/`makeArrayTypeName`/`moveArrayTypeName`/
//! `makeMultirangeTypeName` (catalog/pg_type.c), `CastCreate` (pg_cast.c),
//! `EnumValuesCreate`/`EnumValuesDelete` (pg_enum.c), `RangeCreate`/`RangeDelete`
//! (pg_range.c) and `LookupFuncName`/`func_signature_string` (parse_func.c) are
//! ported owners and are called directly. The `lsyscache`, namespace ACL,
//! identity, format-type, opclass, syscache-OID, and the `defGet*` accessors
//! cross their canonical per-owner `-seams` crates. `makeRangeConstructors`,
//! `makeMultirangeConstructors` (need `ProcedureCreate`) and the
//! `DefineCompositeType` `DefineRelation` call cross
//! [`backend_commands_typecmds_seams`] outward seams that panic until those
//! unported owners land.

use backend_catalog_namespace::{
    QualifiedNameGetCreationNamespace, RangeVarAdjustRelationPersistence,
    RangeVarGetAndCheckCreationNamespace,
};
use backend_catalog_pg_cast::CastCreate;
use backend_catalog_pg_enum::{AddEnumLabel, EnumValuesCreate, RenameEnumLabel};
use backend_catalog_pg_range::RangeCreate;
use backend_catalog_pg_type::{
    makeArrayTypeName, makeMultirangeTypeName, moveArrayTypeName, TypeCreate, TypeShellMake,
};
use backend_utils_error::{ereport, ThrowErrorData};
use mcx::{Mcx, MemoryContext};

use types_acl::{AclMode, ACLCHECK_OK, ACL_CREATE, ACL_EXECUTE, ACL_USAGE};
use types_catalog::catalog::TYPE_RELATION_ID;
use types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_INTERNAL};
use types_catalog::pg_type::{
    TypeCreateParams, TYPTYPE_BASE, TYPTYPE_ENUM, TYPTYPE_MULTIRANGE, TYPTYPE_PSEUDO, TYPTYPE_RANGE,
};
use types_core::catalog::{BTREE_AM_OID, INT4OID, INTERNALOID, OIDOID, PROCEDURE_RELATION_ID};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::pg_error::ErrorLocation;
use types_error::{
    PgError, PgResult, ERRCODE_AMBIGUOUS_FUNCTION, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR, WARNING,
};
use types_nodes::parsenodes::OBJECT_FUNCTION;
use types_parsenodes::{
    DefElem, Node, TypeName, COERCION_CODE_EXPLICIT, COERCION_METHOD_FUNCTION,
    PROVOLATILE_IMMUTABLE, PROVOLATILE_VOLATILE,
};
use types_tuple::heaptuple::{
    CSTRINGOID, DEFAULT_COLLATION_OID, FLOAT8OID, TEXTOID, TYPALIGN_CHAR, TYPALIGN_DOUBLE,
    TYPALIGN_INT, TYPALIGN_SHORT, TYPSTORAGE_EXTENDED, TYPSTORAGE_EXTERNAL, TYPSTORAGE_MAIN,
    TYPSTORAGE_PLAIN,
};
use types_core::catalog::BOOTSTRAP_SUPERUSERID;
use types_parsenodes::{FUNC_PARAM_VARIADIC, PROKIND_FUNCTION, PROPARALLEL_SAFE};

/// `INTERNALlanguageId` (`catalog/pg_language_d.h`) — the OID of the `internal`
/// language, used for the range/multirange constructor functions.
const INTERNALlanguageId: Oid = 12;

/// `F_FMGR_INTERNAL_VALIDATOR` — the OID of `fmgr_internal_validator` (pg_proc
/// builtin OID 2246), passed to `ProcedureCreate` as the language validator for
/// the internal-language range/multirange constructor functions.
const F_FMGR_INTERNAL_VALIDATOR: Oid = 2246;

use backend_commands_typecmds_seams as me;

use backend_catalog_aclchk_seams::{aclcheck_error, object_aclcheck};
use backend_commands_define_seams::DefElemArg;
use backend_parser_coerce_seams::is_binary_coercible;
use backend_utils_adt_format_type_seams::format_type_be_owned;
use backend_utils_init_miscinit_seams::{get_user_id, superuser};

use backend_catalog_binary_upgrade_seams::{consume_next_pg_type_oid, is_binary_upgrade};
use backend_commands_functioncmds_seams::{
    aclcheck_error_schema, func_signature_string, get_func_name, lookup_func_name,
    name_list_to_string, namespace_aclcheck,
};
use backend_commands_opclasscmds_seams::get_opclass_oid;
use backend_utils_cache_lsyscache_seams::{
    func_volatile, get_default_opclass, get_func_rettype, get_multirange_range,
    get_namespace_name, get_opclass_input_type, get_range_multirange, get_rel_relkind,
    get_typcollation, get_typisdefined, get_typlen, get_typlenbyvalalign, get_typtype,
    type_is_collatable,
};
use backend_utils_cache_syscache_seams::get_type_oid;

// ---------------------------------------------------------------------------
// fmgr OIDs used in TypeCreate calls (utils/fmgroids.h).
// ---------------------------------------------------------------------------

const F_ARRAY_IN: Oid = 750;
const F_ARRAY_OUT: Oid = 751;
const F_ARRAY_RECV: Oid = 2400;
const F_ARRAY_SEND: Oid = 2401;
const F_ARRAY_TYPANALYZE: Oid = 3816;
const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;
const F_RAW_ARRAY_SUBSCRIPT_HANDLER: Oid = 6180;
const F_ENUM_IN: Oid = 3506;
const F_ENUM_OUT: Oid = 3507;
const F_ENUM_RECV: Oid = 3532;
const F_ENUM_SEND: Oid = 3533;
const F_RANGE_IN: Oid = 3834;
const F_RANGE_OUT: Oid = 3835;
const F_RANGE_RECV: Oid = 3836;
const F_RANGE_SEND: Oid = 3837;
const F_RANGE_TYPANALYZE: Oid = 3916;
const F_MULTIRANGE_IN: Oid = 4231;
const F_MULTIRANGE_OUT: Oid = 4232;
const F_MULTIRANGE_RECV: Oid = 4233;
const F_MULTIRANGE_SEND: Oid = 4234;
const F_MULTIRANGE_TYPANALYZE: Oid = 4242;

/// `#define DEFAULT_TYPDELIM ','` (pg_type.h).
const DEFAULT_TYPDELIM: i8 = b',' as i8;

/// `BYTEAOID` (17) — `bytea` (pg_type.dat).
const BYTEAOID: Oid = 17;
/// `BOOLOID` (16).
const BOOLOID: Oid = 16;
/// `CSTRINGARRAYOID` (1263) — `cstring[]` (pg_type.dat).
const CSTRINGARRAYOID: Oid = 1263;

/// `TYPCATEGORY_*` (catalog/pg_type.h).
const TYPCATEGORY_ARRAY: i8 = b'A' as i8;
const TYPCATEGORY_ENUM: i8 = b'E' as i8;
const TYPCATEGORY_RANGE: i8 = b'R' as i8;
const TYPCATEGORY_USER: i8 = b'U' as i8;

/// `NoLock` (lockdefs.h) — used by RangeVarGetAndCheckCreationNamespace.
const NoLock: i32 = 0;

/// `ProcedureRelationId` — pg_proc's OID, used as the `object_aclcheck` classid
/// in the range support-function permission checks.
const ProcedureRelationId: Oid = PROCEDURE_RELATION_ID;
/// `TypeRelationId` — pg_type's OID, used for the `ObjectAddress` class id.
const TypeRelationId: Oid = TYPE_RELATION_ID;

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

/// `ErrorLocation` for the `ereport`s in this file, anchored at typecmds.c.
fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/typecmds.c", lineno, funcname)
}

/// `pg_strcasecmp(a, b) == 0` — ASCII case-insensitive compare.
fn pg_strcaseeq(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// `defel->defname`, as `&str`.
fn def_name(d: &DefElem) -> &str {
    d.defname.as_deref().unwrap_or("")
}

/// Wrap a plain-`String` name list as the `NameList` (`&[Option<String>]`) the
/// namespace foundation crate consumes.
fn as_namelist(names: &[String]) -> Vec<Option<String>> {
    names.iter().map(|s| Some(s.clone())).collect()
}

/// `ObjectAddressSet(addr, TypeRelationId, oid)` — mirrors the C helper; F2
/// returns the `TypeCreate`/`enum`-address directly, so this is unused here but
/// kept for parity (and keeps `TypeRelationId` live for the class id).
#[allow(dead_code)]
fn object_address_set_type(type_oid: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: TypeRelationId,
        objectId: type_oid,
        objectSubId: 0,
    }
}

/// `errorConflictingDefElem(defel, pstate)` — always raises the syntax error.
fn error_conflicting_def_elem(defel: &DefElem) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options")
        .errposition(parser_errposition(defel.location))
        .finish(errloc(0, "typecmds"))
        .expect_err("ereport(ERROR) always yields an Err")
}

/// `parser_errposition(pstate, location)` (parse_node.c). Many `DefineType`
/// call sites are reached without a `ParseState` (the unported-grammar dispatch
/// passes none), where the C `pstate == NULL` no-op applies — those callers pass
/// `None` and get `0`. The `like = <type>` branch, however, does carry the
/// active query string (`pstate->p_sourcetext`, threaded through `DefineType`),
/// so it reproduces the full body:
///   if (location < 0) return 0;
///   if (p_sourcetext == NULL) return 0;
///   pos = pg_mbstrlen_with_len(p_sourcetext, location) + 1;
fn parser_errposition(_location: i32) -> i32 {
    0
}

/// Source-aware `parser_errposition(pstate, location)` for the `DefineType`
/// branches that thread the query string.
fn parser_errposition_src(source: Option<&str>, location: i32) -> i32 {
    if location < 0 {
        return 0;
    }
    match source {
        Some(s) => {
            backend_utils_mb_mbutils_seams::pg_mbstrlen_with_len::call(s.as_bytes(), location) + 1
        }
        None => 0,
    }
}

/// `defGetString(def)` (define.c).
fn defGetString(mcx: Mcx<'_>, defel: &DefElem) -> PgResult<String> {
    let s = backend_commands_define_seams::def_get_string::call(
        mcx,
        defel.defname.clone().unwrap_or_default(),
        defel_arg(mcx, defel)?,
    )?;
    Ok(s.to_string())
}

/// `defGetBoolean(def)` (define.c).
fn defGetBoolean(mcx: Mcx<'_>, defel: &DefElem) -> PgResult<bool> {
    backend_commands_define_seams::def_get_boolean::call(
        defel.defname.clone().unwrap_or_default(),
        defel_arg(mcx, defel)?,
    )
}

/// Project a `DefElem`'s value node into the `DefElemArg` the define.c value
/// accessors switch on (`nodeTag(def->arg)` dispatch).
///
/// The structural `TypeName`/`List` forms are rendered to text here (matching
/// define.c's `defGetString`: `T_TypeName -> TypeNameToString`,
/// `T_List -> NameListToString`); a bare unquoted identifier (e.g.
/// `alignment = double`) parses to a `TypeName` node, so collapsing it to
/// `A_Star` would lose the value.
fn defel_arg(mcx: Mcx<'_>, defel: &DefElem) -> PgResult<Option<DefElemArg>> {
    let Some(node) = defel.arg.as_deref() else {
        return Ok(None);
    };
    Ok(Some(match node {
        Node::Integer(i) => DefElemArg::Integer(i.ival as i64),
        Node::Float(f) => DefElemArg::Float(f.fval.clone().unwrap_or_default()),
        Node::Boolean(b) => DefElemArg::Boolean(b.boolval),
        Node::String(s) => DefElemArg::String(s.sval.clone().unwrap_or_default()),
        Node::TypeName(tn) => DefElemArg::TypeName(TypeNameToString(mcx, tn)?),
        Node::List(_) => DefElemArg::List(NameListToString_seam(mcx, &defGetQualifiedName(defel)?)?),
        Node::A_Star => DefElemArg::AStar,
        _ => DefElemArg::AStar,
    }))
}

/// `defGetTypeName(def)` (define.c) — returns the `TypeName` the `DefElem`'s
/// `arg` carries (a `TypeName` node) or, for a bare-string `arg`, the
/// single-element type name.  Mirrors define.c `defGetTypeName`.
fn defGetTypeName(defel: &DefElem) -> PgResult<TypeName> {
    match defel.arg.as_deref() {
        Some(Node::TypeName(tn)) => Ok(tn.clone()),
        Some(Node::String(s)) => {
            /* Allow a plain string for backwards compatibility */
            Ok(TypeName {
                names: vec![Node::String(s.clone())],
                typeOid: InvalidOid,
                setof: false,
                pct_type: false,
                typmods: Vec::new(),
                typemod: -1,
                arrayBounds: Vec::new(),
                location: defel.location,
            })
        }
        _ => ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "definition of \"{}\" requires a type name",
                def_name(defel)
            ))
            .finish(errloc(0, "defGetTypeName"))
            .map(|()| unreachable!("ereport(ERROR) always yields an Err")),
    }
}

/// `defGetTypeLength(def)` (define.c) — interpret the `DefElem`'s value as an
/// internal-length specifier: a non-negative `int`, or the keyword `variable`.
fn defGetTypeLength(mcx: Mcx<'_>, defel: &DefElem) -> PgResult<i32> {
    match defel.arg.as_deref() {
        Some(Node::Integer(i)) => Ok(i.ival),
        Some(Node::Float(_)) => {
            /* Allow associated values of an Integer-or-Float to be ints */
            let s = defGetString(mcx, defel)?;
            s.parse::<i32>().map_err(|_| {
                ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("invalid argument for \"{}\"", def_name(defel)))
                    .finish(errloc(0, "defGetTypeLength"))
                    .expect_err("ereport(ERROR) always yields an Err")
            })
        }
        Some(Node::String(_)) | Some(Node::TypeName(_)) => {
            let s = if let Some(Node::TypeName(tn)) = defel.arg.as_deref() {
                typename_first_string(tn)
            } else {
                defGetString(mcx, defel)?
            };
            if pg_strcaseeq(&s, "variable") {
                Ok(-1) /* variable length */
            } else {
                ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("invalid argument for \"{}\"", def_name(defel)))
                    .finish(errloc(0, "defGetTypeLength"))
                    .map(|()| unreachable!())
            }
        }
        _ => ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("invalid argument for \"{}\"", def_name(defel)))
            .finish(errloc(0, "defGetTypeLength"))
            .map(|()| unreachable!()),
    }
}

/// First `String` component of a `TypeName`'s name list, as an owned `String`.
fn typename_first_string(tn: &TypeName) -> String {
    tn.names
        .iter()
        .find_map(|n| n.as_string().and_then(|s| s.sval.clone()))
        .unwrap_or_default()
}

/// `defGetQualifiedName(def)` (define.c) — the `DefElem`'s value as a list of
/// name components (a list of `String` value nodes).
fn defGetQualifiedName(defel: &DefElem) -> PgResult<Vec<String>> {
    match defel.arg.as_deref() {
        Some(Node::TypeName(tn)) => Ok(typename_to_namelist(tn)),
        Some(Node::String(s)) => Ok(vec![s.sval.clone().unwrap_or_default()]),
        Some(Node::List(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item.as_string() {
                    Some(s) => out.push(s.sval.clone().unwrap_or_default()),
                    None => {
                        return ereport(ERROR)
                            .errcode(ERRCODE_SYNTAX_ERROR)
                            .errmsg(format!(
                                "name or argument lists may not contain nulls"
                            ))
                            .finish(errloc(0, "defGetQualifiedName"))
                            .map(|()| unreachable!())
                    }
                }
            }
            Ok(out)
        }
        _ => ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "definition of \"{}\" requires a name or argument list",
                def_name(defel)
            ))
            .finish(errloc(0, "defGetQualifiedName"))
            .map(|()| unreachable!()),
    }
}

/// `TypeName` qualified name list flattened to bare strings.
fn typename_to_namelist(tn: &TypeName) -> Vec<String> {
    tn.names
        .iter()
        .map(|n| n.as_string().and_then(|s| s.sval.clone()).unwrap_or_default())
        .collect()
}

/// Project the resolver-facing `types_opclass::TypeName` consumed by the
/// `parse_type.c` seams (`typenameTypeId`/`TypeNameToString`).
fn to_resolver_typename(tn: &TypeName) -> types_opclass::TypeName {
    types_opclass::TypeName {
        names: typename_to_namelist(tn),
        typeOid: tn.typeOid,
        setof: tn.setof,
        pct_type: tn.pct_type,
        typemod: tn.typemod,
        // Project `List *arrayBounds` (Integer A_Const nodes; `-1` for an
        // unbounded `[]`). The resolver inspects emptiness so that `int[]`
        // resolves to the array type, not the element type.
        arrayBounds: tn
            .arrayBounds
            .iter()
            .map(|n| n.as_integer().map(|i| i.ival).unwrap_or(-1))
            .collect(),
        location: tn.location,
    }
}

/// `typenameTypeId(NULL, typeName)` (parse_type.c).
fn typenameTypeId(tn: &TypeName) -> PgResult<Oid> {
    backend_parser_parse_type_seams::typename_type_id::call(&to_resolver_typename(tn))
}

/// `TypeNameToString(typeName)` (parse_type.c).
#[allow(dead_code)]
fn TypeNameToString(mcx: Mcx<'_>, tn: &TypeName) -> PgResult<String> {
    Ok(backend_parser_parse_type_seams::typename_to_string::call(mcx, &to_resolver_typename(tn))?
        .to_string())
}

/// `typenameType(pstate, typeName, NULL)` form fetch (parse_type.c) restricted
/// to the few `Form_pg_type` columns the LIKE branch of `DefineType` reads.
/// `(typlen, typbyval, typalign, typstorage)`.
fn typenameTypeFields(tn: &TypeName) -> PgResult<(i16, bool, i8, i8)> {
    let oid = typenameTypeId(tn)?;
    let (typlen, typbyval) = lsyscache_get_typlenbyval(oid)?;
    let typalign = get_typalign_via_lenbyvalalign(oid)?;
    let typstorage = get_typstorage_seam(oid)?;
    Ok((typlen, typbyval, typalign, typstorage))
}

/// `get_typlenbyval(typid)` (lsyscache) -> `(typlen, typbyval)`.
fn lsyscache_get_typlenbyval(oid: Oid) -> PgResult<(i16, bool)> {
    let r = get_typlenbyvalalign::call(oid)?;
    Ok((r.typlen, r.typbyval))
}

/// `get_typlenbyvalalign(typid, ...)`'s `typalign`.
fn get_typalign_via_lenbyvalalign(oid: Oid) -> PgResult<i8> {
    Ok(get_typlenbyvalalign::call(oid)?.typalign)
}

/// `get_typstorage(typid)` (lsyscache) — the LIKE branch reads `typstorage`.
fn get_typstorage_seam(oid: Oid) -> PgResult<i8> {
    Ok(backend_utils_cache_lsyscache_seams::get_typstorage::call(oid)? as i8)
}

// ===========================================================================
// DefineType   (typecmds.c:152-651)
// ===========================================================================

/// `DefineType(pstate, names, parameters)` (typecmds.c:152) — registers a new
/// base type.  `names` is the qualified type name; `parameters` is the list of
/// `DefElem` option nodes (empty for a parameterless shell-type CREATE).
pub fn DefineType<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[String],
    parameters: &[Node],
    source_text: Option<&str>,
) -> PgResult<ObjectAddress> {
    let mut internalLength: i16 = -1; /* default: variable-length */
    let mut inputName: Option<Vec<String>> = None;
    let mut outputName: Option<Vec<String>> = None;
    let mut receiveName: Option<Vec<String>> = None;
    let mut sendName: Option<Vec<String>> = None;
    let mut typmodinName: Option<Vec<String>> = None;
    let mut typmodoutName: Option<Vec<String>> = None;
    let mut analyzeName: Option<Vec<String>> = None;
    let mut subscriptName: Option<Vec<String>> = None;
    let mut category: i8 = TYPCATEGORY_USER;
    let mut preferred = false;
    let mut delimiter: i8 = DEFAULT_TYPDELIM;
    let mut elemType: Oid = InvalidOid;
    let mut defaultValue: Option<String> = None;
    let mut byValue = false;
    let mut alignment: i8 = TYPALIGN_INT; /* default alignment */
    let mut storage: i8 = TYPSTORAGE_PLAIN; /* default TOAST storage method */
    let mut collation: Oid = InvalidOid;
    let mut likeTypeEl: Option<&DefElem> = None;
    let mut internalLengthEl: Option<&DefElem> = None;
    let mut inputNameEl: Option<&DefElem> = None;
    let mut outputNameEl: Option<&DefElem> = None;
    let mut receiveNameEl: Option<&DefElem> = None;
    let mut sendNameEl: Option<&DefElem> = None;
    let mut typmodinNameEl: Option<&DefElem> = None;
    let mut typmodoutNameEl: Option<&DefElem> = None;
    let mut analyzeNameEl: Option<&DefElem> = None;
    let mut subscriptNameEl: Option<&DefElem> = None;
    let mut categoryEl: Option<&DefElem> = None;
    let mut preferredEl: Option<&DefElem> = None;
    let mut delimiterEl: Option<&DefElem> = None;
    let mut elemTypeEl: Option<&DefElem> = None;
    let mut defaultValueEl: Option<&DefElem> = None;
    let mut byValueEl: Option<&DefElem> = None;
    let mut alignmentEl: Option<&DefElem> = None;
    let mut storageEl: Option<&DefElem> = None;
    let mut collatableEl: Option<&DefElem> = None;
    let inputOid: Oid;
    let outputOid: Oid;
    let mut receiveOid: Oid = InvalidOid;
    let mut sendOid: Oid = InvalidOid;
    let mut typmodinOid: Oid = InvalidOid;
    let mut typmodoutOid: Oid = InvalidOid;
    let mut analyzeOid: Oid = InvalidOid;
    let mut subscriptOid: Oid = InvalidOid;
    let array_type: String;
    let array_oid: Oid;
    let mut typoid: Oid;

    /*
     * As of Postgres 8.4, we require superuser privilege to create a base type.
     */
    if !superuser::call(mcx)? {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("must be superuser to create a base type")
            .finish(errloc(218, "DefineType"))
            .map(|()| unreachable!());
    }

    /* Convert list of names to a name and namespace */
    let names_nl = as_namelist(names);
    let (typeNamespace, typeName) = QualifiedNameGetCreationNamespace(mcx, &names_nl)?;
    let typeName = typeName.to_string();

    /*
     * Look to see if type already exists.
     */
    typoid = get_type_oid::call(&typeName, typeNamespace)?;

    /*
     * If it's not a shell, see if it's an autogenerated array type, and if so
     * rename it out of the way.
     */
    if OidIsValid(typoid) && get_typisdefined::call(typoid)? {
        if moveArrayTypeName(typoid, &typeName, typeNamespace)? {
            typoid = InvalidOid;
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("type \"{typeName}\" already exists"))
                .finish(errloc(252, "DefineType"))
                .map(|()| unreachable!());
        }
    }

    /*
     * If this command is a parameterless CREATE TYPE, then we're just here to
     * make a shell type, so do that (or fail if there already is a shell).
     */
    if parameters.is_empty() {
        if OidIsValid(typoid) {
            return ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("type \"{typeName}\" already exists"))
                .finish(errloc(264, "DefineType"))
                .map(|()| unreachable!());
        }

        let address = TypeShellMake(&typeName, typeNamespace, get_user_id::call())?;
        return Ok(address);
    }

    /*
     * Otherwise, we must already have a shell type, since there is no other way
     * that the I/O functions could have been created.
     */
    if !OidIsValid(typoid) {
        return ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("type \"{typeName}\" does not exist"))
            .errhint("Create the type as a shell type, then create its I/O functions, then do a full CREATE TYPE.")
            .finish(errloc(278, "DefineType"))
            .map(|()| unreachable!());
    }

    /* Extract the parameters from the parameter list */
    for node in parameters {
        let defel = expect_defelem(node, "DefineType")?;
        let defname = def_name(defel);

        let defelp: &mut Option<&DefElem> = if defname == "like" {
            &mut likeTypeEl
        } else if defname == "internallength" {
            &mut internalLengthEl
        } else if defname == "input" {
            &mut inputNameEl
        } else if defname == "output" {
            &mut outputNameEl
        } else if defname == "receive" {
            &mut receiveNameEl
        } else if defname == "send" {
            &mut sendNameEl
        } else if defname == "typmod_in" {
            &mut typmodinNameEl
        } else if defname == "typmod_out" {
            &mut typmodoutNameEl
        } else if defname == "analyze" || defname == "analyse" {
            &mut analyzeNameEl
        } else if defname == "subscript" {
            &mut subscriptNameEl
        } else if defname == "category" {
            &mut categoryEl
        } else if defname == "preferred" {
            &mut preferredEl
        } else if defname == "delimiter" {
            &mut delimiterEl
        } else if defname == "element" {
            &mut elemTypeEl
        } else if defname == "default" {
            &mut defaultValueEl
        } else if defname == "passedbyvalue" {
            &mut byValueEl
        } else if defname == "alignment" {
            &mut alignmentEl
        } else if defname == "storage" {
            &mut storageEl
        } else if defname == "collatable" {
            &mut collatableEl
        } else {
            /* WARNING, not ERROR, for historical backwards-compatibility */
            ereport(WARNING)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("type attribute \"{defname}\" not recognized"))
                .errposition(parser_errposition(defel.location))
                .finish(errloc(333, "DefineType"))?;
            continue;
        };
        if defelp.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        *defelp = Some(defel);
    }

    /*
     * Now interpret the options; we do this separately so that LIKE can be
     * overridden by other options regardless of the ordering in the list.
     */
    if let Some(el) = likeTypeEl {
        // likeType = typenameType(pstate, defGetTypeName(likeTypeEl), NULL);
        // The C threads `pstate` so the "does not exist"/"is only a shell"
        // error carries `parser_errposition(pstate, typeName->location)`; the
        // `typename_type_id` seam strips the ParseState, so we re-attach the
        // cursor position from the threaded query string here.
        let like_type_name = defGetTypeName(el)?;
        let (typlen, typbyval, typalign, typstorage) =
            typenameTypeFields(&like_type_name).map_err(|e| {
                if e.cursor_position().is_none() {
                    e.with_cursor_position(parser_errposition_src(
                        source_text,
                        like_type_name.location,
                    ))
                } else {
                    e
                }
            })?;
        internalLength = typlen;
        byValue = typbyval;
        alignment = typalign;
        storage = typstorage;
    }
    if let Some(el) = internalLengthEl {
        internalLength = defGetTypeLength(mcx, el)? as i16;
    }
    if let Some(el) = inputNameEl {
        inputName = Some(defGetQualifiedName(el)?);
    }
    if let Some(el) = outputNameEl {
        outputName = Some(defGetQualifiedName(el)?);
    }
    if let Some(el) = receiveNameEl {
        receiveName = Some(defGetQualifiedName(el)?);
    }
    if let Some(el) = sendNameEl {
        sendName = Some(defGetQualifiedName(el)?);
    }
    if let Some(el) = typmodinNameEl {
        typmodinName = Some(defGetQualifiedName(el)?);
    }
    if let Some(el) = typmodoutNameEl {
        typmodoutName = Some(defGetQualifiedName(el)?);
    }
    if let Some(el) = analyzeNameEl {
        analyzeName = Some(defGetQualifiedName(el)?);
    }
    if let Some(el) = subscriptNameEl {
        subscriptName = Some(defGetQualifiedName(el)?);
    }
    if let Some(el) = categoryEl {
        let p = defGetString(mcx, el)?;
        let pbytes = p.as_bytes();
        let c0 = if pbytes.is_empty() { 0i8 } else { pbytes[0] as i8 };
        category = c0;
        /* restrict to non-control ASCII */
        if category < 32 || category > 126 {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("invalid type category \"{p}\": must be simple ASCII"))
                .finish(errloc(386, "DefineType"))
                .map(|()| unreachable!());
        }
    }
    if let Some(el) = preferredEl {
        preferred = defGetBoolean(mcx, el)?;
    }
    if let Some(el) = delimiterEl {
        let p = defGetString(mcx, el)?;
        let pbytes = p.as_bytes();
        delimiter = if pbytes.is_empty() { 0i8 } else { pbytes[0] as i8 };
        /* XXX shouldn't we restrict the delimiter? */
    }
    if let Some(el) = elemTypeEl {
        elemType = typenameTypeId(&defGetTypeName(el)?)?;
        /* disallow arrays of pseudotypes */
        if get_typtype::call(elemType)? as i8 == TYPTYPE_PSEUDO {
            return ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "array element type cannot be {}",
                    format_type_be_owned::call(elemType)?
                ))
                .finish(errloc(406, "DefineType"))
                .map(|()| unreachable!());
        }
    }
    if let Some(el) = defaultValueEl {
        defaultValue = Some(defGetString(mcx, el)?);
    }
    if let Some(el) = byValueEl {
        byValue = defGetBoolean(mcx, el)?;
    }
    if let Some(el) = alignmentEl {
        let a = defGetString(mcx, el)?;
        /*
         * Note: if argument was an unquoted identifier, parser will have
         * applied translations to it.
         */
        if pg_strcaseeq(&a, "double")
            || pg_strcaseeq(&a, "float8")
            || pg_strcaseeq(&a, "pg_catalog.float8")
        {
            alignment = TYPALIGN_DOUBLE;
        } else if pg_strcaseeq(&a, "int4") || pg_strcaseeq(&a, "pg_catalog.int4") {
            alignment = TYPALIGN_INT;
        } else if pg_strcaseeq(&a, "int2") || pg_strcaseeq(&a, "pg_catalog.int2") {
            alignment = TYPALIGN_SHORT;
        } else if pg_strcaseeq(&a, "char") || pg_strcaseeq(&a, "pg_catalog.bpchar") {
            alignment = TYPALIGN_CHAR;
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("alignment \"{a}\" not recognized"))
                .finish(errloc(437, "DefineType"))
                .map(|()| unreachable!());
        }
    }
    if let Some(el) = storageEl {
        let a = defGetString(mcx, el)?;
        if pg_strcaseeq(&a, "plain") {
            storage = TYPSTORAGE_PLAIN;
        } else if pg_strcaseeq(&a, "external") {
            storage = TYPSTORAGE_EXTERNAL;
        } else if pg_strcaseeq(&a, "extended") {
            storage = TYPSTORAGE_EXTENDED;
        } else if pg_strcaseeq(&a, "main") {
            storage = TYPSTORAGE_MAIN;
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("storage \"{a}\" not recognized"))
                .finish(errloc(454, "DefineType"))
                .map(|()| unreachable!());
        }
    }
    if let Some(el) = collatableEl {
        collation = if defGetBoolean(mcx, el)? {
            DEFAULT_COLLATION_OID
        } else {
            InvalidOid
        };
    }

    /*
     * make sure we have our required definitions
     */
    let inputName = match inputName {
        Some(n) => n,
        None => {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("type input function must be specified")
                .finish(errloc(465, "DefineType"))
                .map(|()| unreachable!());
        }
    };
    let outputName = match outputName {
        Some(n) => n,
        None => {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("type output function must be specified")
                .finish(errloc(469, "DefineType"))
                .map(|()| unreachable!());
        }
    };

    if typmodinName.is_none() && typmodoutName.is_some() {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("type modifier output function is useless without a type modifier input function")
            .finish(errloc(474, "DefineType"))
            .map(|()| unreachable!());
    }

    /*
     * Convert I/O proc names to OIDs
     */
    inputOid = findTypeInputFunction(mcx, &inputName, typoid)?;
    outputOid = findTypeOutputFunction(mcx, &outputName, typoid)?;
    if let Some(ref n) = receiveName {
        receiveOid = findTypeReceiveFunction(mcx, n, typoid)?;
    }
    if let Some(ref n) = sendName {
        sendOid = findTypeSendFunction(mcx, n, typoid)?;
    }

    /*
     * Convert typmodin/out function proc names to OIDs.
     */
    if let Some(ref n) = typmodinName {
        typmodinOid = findTypeTypmodinFunction(mcx, n)?;
    }
    if let Some(ref n) = typmodoutName {
        typmodoutOid = findTypeTypmodoutFunction(mcx, n)?;
    }

    /*
     * Convert analysis function proc name to an OID.
     */
    if let Some(ref n) = analyzeName {
        analyzeOid = findTypeAnalyzeFunction(mcx, n, typoid)?;
    }

    /*
     * Likewise look up the subscripting function if any.
     */
    if let Some(ref n) = subscriptName {
        subscriptOid = findTypeSubscriptingFunction(mcx, n, typoid)?;
    } else if OidIsValid(elemType) {
        if internalLength > 0 && !byValue && get_typlen::call(elemType)? > 0 {
            subscriptOid = F_RAW_ARRAY_SUBSCRIPT_HANDLER;
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("element type cannot be specified without a subscripting function")
                .finish(errloc(516, "DefineType"))
                .map(|()| unreachable!());
        }
    }

    /*
     * OK, we're done checking, time to make the type.  We must assign the array
     * type OID ahead of calling TypeCreate.
     */
    array_oid = AssignTypeArrayOid()?;

    /*
     * now have TypeCreate do all the real work.
     */
    let owner = get_user_id::call();
    let address = TypeCreate(TypeCreateParams {
        new_type_oid: InvalidOid,
        type_name: typeName.clone(),
        type_namespace: typeNamespace,
        relation_oid: InvalidOid,
        relation_kind: 0,
        owner_id: owner,
        internal_size: internalLength,
        type_type: TYPTYPE_BASE,
        type_category: category,
        type_preferred: preferred,
        type_delim: delimiter,
        input_procedure: inputOid,
        output_procedure: outputOid,
        receive_procedure: receiveOid,
        send_procedure: sendOid,
        typmodin_procedure: typmodinOid,
        typmodout_procedure: typmodoutOid,
        analyze_procedure: analyzeOid,
        subscript_procedure: subscriptOid,
        element_type: elemType,
        is_implicit_array: false,
        array_type: array_oid,
        base_type: InvalidOid,
        default_type_value: defaultValue,
        default_type_bin: None,
        passed_by_value: byValue,
        alignment,
        storage,
        type_mod: -1,
        typ_ndims: 0,
        type_not_null: false,
        type_collation: collation,
    })?;
    debug_assert!(typoid == address.objectId);

    /*
     * Create the array type that goes with it.
     */
    array_type = makeArrayTypeName(&typeName, typeNamespace)?;

    /* alignment must be TYPALIGN_INT or TYPALIGN_DOUBLE for arrays */
    alignment = if alignment == TYPALIGN_DOUBLE {
        TYPALIGN_DOUBLE
    } else {
        TYPALIGN_INT
    };

    TypeCreate(TypeCreateParams {
        new_type_oid: array_oid,
        type_name: array_type,
        type_namespace: typeNamespace,
        relation_oid: InvalidOid,
        relation_kind: 0,
        owner_id: owner,
        internal_size: -1,
        type_type: TYPTYPE_BASE,
        type_category: TYPCATEGORY_ARRAY,
        type_preferred: false,
        type_delim: delimiter,
        input_procedure: F_ARRAY_IN,
        output_procedure: F_ARRAY_OUT,
        receive_procedure: F_ARRAY_RECV,
        send_procedure: F_ARRAY_SEND,
        typmodin_procedure: typmodinOid,
        typmodout_procedure: typmodoutOid,
        analyze_procedure: F_ARRAY_TYPANALYZE,
        subscript_procedure: F_ARRAY_SUBSCRIPT_HANDLER,
        element_type: typoid,
        is_implicit_array: true,
        array_type: InvalidOid,
        base_type: InvalidOid,
        default_type_value: None,
        default_type_bin: None,
        passed_by_value: false,
        alignment,
        storage: TYPSTORAGE_EXTENDED,
        type_mod: -1,
        typ_ndims: 0,
        type_not_null: false,
        type_collation: collation,
    })?;

    Ok(address)
}

// ===========================================================================
// RemoveTypeById   (typecmds.c:656-689) — the inward seam, real body.
// ===========================================================================

/// `RemoveTypeById(typeOid)` (typecmds.c:656) — guts of type deletion.
///
/// `table_open(TypeRelationId)` + `SearchSysCache1(TYPEOID)` +
/// `CatalogTupleDelete(&tup->t_self)` + the `EnumValuesDelete`/`RangeDelete`
/// branch keyed on the fetched form's `typtype` + `ReleaseSysCache` +
/// `table_close` are performed by the pg_type owner's `remove_type_catalog_row`
/// (it returns the deleted row's `typtype`), then this routine fires the
/// enum/range cleanup against the ported pg_enum/pg_range owners.
pub fn RemoveTypeById(type_oid: Oid) -> PgResult<()> {
    /*
     * Delete the pg_type row (table_open RowExclusiveLock + SearchSysCache1 +
     * CatalogTupleDelete + ReleaseSysCache + table_close) and report its
     * typtype so we can do the by-hand enum/range cleanup.
     */
    let typtype = backend_catalog_pg_type_seams::remove_type_catalog_row::call(type_oid)?;

    /*
     * If it is an enum, delete the pg_enum entries too; we don't bother with
     * making dependency entries for those, so it has to be done "by hand" here.
     */
    if typtype == TYPTYPE_ENUM {
        backend_catalog_pg_enum::EnumValuesDelete(type_oid)?;
    }

    /*
     * If it is a range type, delete the pg_range entry too; we don't bother
     * with making a dependency entry for that, so it has to be done "by hand"
     * here.
     */
    if typtype == TYPTYPE_RANGE {
        backend_catalog_pg_range::RangeDelete(type_oid)?;
    }

    Ok(())
}

// ===========================================================================
// DefineEnum   (typecmds.c:1181-1298)
// ===========================================================================

/// `DefineEnum(stmt)` (typecmds.c:1181) — registers a new enum.
///
/// `type_name` is the qualified enum name list; `vals` is the ordered list of
/// label strings (`stmt->vals`, a list of `String` nodes).
pub fn DefineEnum<'mcx>(
    mcx: Mcx<'mcx>,
    type_name: &[String],
    vals: &[String],
) -> PgResult<ObjectAddress> {
    /* Convert list of names to a name and namespace */
    let enum_nl = as_namelist(type_name);
    let (enumNamespace, enumName) = QualifiedNameGetCreationNamespace(mcx, &enum_nl)?;
    let enumName = enumName.to_string();

    /* Check we have creation rights in target namespace */
    let aclresult = namespace_aclcheck::call(enumNamespace, get_user_id::call(), ACL_CREATE)?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error_schema::call(aclresult, get_namespace_name_seam(mcx, enumNamespace)?)?;
    }

    /*
     * Check for collision with an existing type name.  If there is one and it's
     * an autogenerated array, we can rename it out of the way.
     */
    let old_type_oid = get_type_oid::call(&enumName, enumNamespace)?;
    if OidIsValid(old_type_oid) {
        if !moveArrayTypeName(old_type_oid, &enumName, enumNamespace)? {
            return ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("type \"{enumName}\" already exists"))
                .finish(errloc(1213, "DefineEnum"))
                .map(|()| unreachable!());
        }
    }

    /* Allocate OID for array type */
    let enumArrayOid = AssignTypeArrayOid()?;

    let owner = get_user_id::call();

    /* Create the pg_type entry */
    let enumTypeAddr = TypeCreate(TypeCreateParams {
        new_type_oid: InvalidOid,
        type_name: enumName.clone(),
        type_namespace: enumNamespace,
        relation_oid: InvalidOid,
        relation_kind: 0,
        owner_id: owner,
        internal_size: core::mem::size_of::<Oid>() as i16, /* sizeof(Oid) */
        type_type: TYPTYPE_ENUM,
        type_category: TYPCATEGORY_ENUM,
        type_preferred: false,
        type_delim: DEFAULT_TYPDELIM,
        input_procedure: F_ENUM_IN,
        output_procedure: F_ENUM_OUT,
        receive_procedure: F_ENUM_RECV,
        send_procedure: F_ENUM_SEND,
        typmodin_procedure: InvalidOid,
        typmodout_procedure: InvalidOid,
        analyze_procedure: InvalidOid,
        subscript_procedure: InvalidOid,
        element_type: InvalidOid,
        is_implicit_array: false,
        array_type: enumArrayOid,
        base_type: InvalidOid,
        default_type_value: None,
        default_type_bin: None,
        passed_by_value: true,
        alignment: TYPALIGN_INT,
        storage: TYPSTORAGE_PLAIN,
        type_mod: -1,
        typ_ndims: 0,
        type_not_null: false,
        type_collation: InvalidOid,
    })?;

    /* Enter the enum's values into pg_enum */
    let val_refs: Vec<&str> = vals.iter().map(|s| s.as_str()).collect();
    EnumValuesCreate(enumTypeAddr.objectId, &val_refs)?;

    /*
     * Create the array type that goes with it.
     */
    let enumArrayName = makeArrayTypeName(&enumName, enumNamespace)?;

    TypeCreate(TypeCreateParams {
        new_type_oid: enumArrayOid,
        type_name: enumArrayName,
        type_namespace: enumNamespace,
        relation_oid: InvalidOid,
        relation_kind: 0,
        owner_id: owner,
        internal_size: -1,
        type_type: TYPTYPE_BASE,
        type_category: TYPCATEGORY_ARRAY,
        type_preferred: false,
        type_delim: DEFAULT_TYPDELIM,
        input_procedure: F_ARRAY_IN,
        output_procedure: F_ARRAY_OUT,
        receive_procedure: F_ARRAY_RECV,
        send_procedure: F_ARRAY_SEND,
        typmodin_procedure: InvalidOid,
        typmodout_procedure: InvalidOid,
        analyze_procedure: F_ARRAY_TYPANALYZE,
        subscript_procedure: F_ARRAY_SUBSCRIPT_HANDLER,
        element_type: enumTypeAddr.objectId,
        is_implicit_array: true,
        array_type: InvalidOid,
        base_type: InvalidOid,
        default_type_value: None,
        default_type_bin: None,
        passed_by_value: false,
        alignment: TYPALIGN_INT,
        storage: TYPSTORAGE_EXTENDED,
        type_mod: -1,
        typ_ndims: 0,
        type_not_null: false,
        type_collation: InvalidOid,
    })?;

    Ok(enumTypeAddr)
}

// ===========================================================================
// DefineRange   (typecmds.c:1380-1759)
// ===========================================================================

/// `DefineRange(pstate, stmt)` (typecmds.c:1380) — registers a new range type.
///
/// `type_name` is the qualified range name list; `params` is the list of
/// `DefElem` option nodes (`stmt->params`).
pub fn DefineRange<'mcx>(
    mcx: Mcx<'mcx>,
    type_name: &[String],
    params: &[Node],
) -> PgResult<ObjectAddress> {
    let mut multirangeTypeName: Option<String> = None;
    let mut multirangeNamespace: Oid = InvalidOid;
    let rangeArrayOid: Oid;
    let multirangeOid: Oid;
    let multirangeArrayOid: Oid;
    let mut rangeSubtype: Oid = InvalidOid;
    let mut rangeSubOpclassName: Option<Vec<String>> = None;
    let mut rangeCollationName: Option<Vec<String>> = None;
    let mut rangeCanonicalName: Option<Vec<String>> = None;
    let mut rangeSubtypeDiffName: Option<Vec<String>> = None;
    let rangeSubOpclass: Oid;
    let rangeCollation: Oid;
    let rangeCanonical: Oid;
    let rangeSubtypeDiff: Oid;
    let subtypalign: i8;
    let alignment: i8;

    /* Convert list of names to a name and namespace */
    let range_nl = as_namelist(type_name);
    let (typeNamespace, typeName) = QualifiedNameGetCreationNamespace(mcx, &range_nl)?;
    let typeName = typeName.to_string();

    /* Check we have creation rights in target namespace */
    let aclresult = namespace_aclcheck::call(typeNamespace, get_user_id::call(), ACL_CREATE)?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error_schema::call(aclresult, get_namespace_name_seam(mcx, typeNamespace)?)?;
    }

    /*
     * Look to see if type already exists.
     */
    let mut typoid = get_type_oid::call(&typeName, typeNamespace)?;

    /*
     * If it's not a shell, see if it's an autogenerated array type, and if so
     * rename it out of the way.
     */
    if OidIsValid(typoid) && get_typisdefined::call(typoid)? {
        if moveArrayTypeName(typoid, &typeName, typeNamespace)? {
            typoid = InvalidOid;
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("type \"{typeName}\" already exists"))
                .finish(errloc(1439, "DefineRange"))
                .map(|()| unreachable!());
        }
    }

    /*
     * Unlike DefineType(), we don't insist on a shell type existing first, as
     * it's only needed if the user wants to specify a canonical function.
     */

    /* Extract the parameters from the parameter list */
    for node in params {
        let defel = expect_defelem(node, "DefineRange")?;

        if def_name(defel) == "subtype" {
            if OidIsValid(rangeSubtype) {
                return Err(error_conflicting_def_elem(defel));
            }
            /* we can look up the subtype name immediately */
            rangeSubtype = typenameTypeId(&defGetTypeName(defel)?)?;
        } else if def_name(defel) == "subtype_opclass" {
            if rangeSubOpclassName.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            rangeSubOpclassName = Some(defGetQualifiedName(defel)?);
        } else if def_name(defel) == "collation" {
            if rangeCollationName.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            rangeCollationName = Some(defGetQualifiedName(defel)?);
        } else if def_name(defel) == "canonical" {
            if rangeCanonicalName.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            rangeCanonicalName = Some(defGetQualifiedName(defel)?);
        } else if def_name(defel) == "subtype_diff" {
            if rangeSubtypeDiffName.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            rangeSubtypeDiffName = Some(defGetQualifiedName(defel)?);
        } else if def_name(defel) == "multirange_type_name" {
            if multirangeTypeName.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            /* we can look up the subtype name immediately */
            let mrng_nl = as_namelist(&defGetQualifiedName(defel)?);
            let (ns, name) = QualifiedNameGetCreationNamespace(mcx, &mrng_nl)?;
            multirangeNamespace = ns;
            multirangeTypeName = Some(name.to_string());
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "type attribute \"{}\" not recognized",
                    def_name(defel)
                ))
                .finish(errloc(1492, "DefineRange"))
                .map(|()| unreachable!());
        }
    }

    /* Must have a subtype */
    if !OidIsValid(rangeSubtype) {
        return ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("type attribute \"subtype\" is required")
            .finish(errloc(1499, "DefineRange"))
            .map(|()| unreachable!());
    }
    /* disallow ranges of pseudotypes */
    if get_typtype::call(rangeSubtype)? as i8 == TYPTYPE_PSEUDO {
        return ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "range subtype cannot be {}",
                format_type_be_owned::call(rangeSubtype)?
            ))
            .finish(errloc(1504, "DefineRange"))
            .map(|()| unreachable!());
    }

    /* Identify subopclass */
    rangeSubOpclass = findRangeSubOpclass(mcx, rangeSubOpclassName.as_deref(), rangeSubtype)?;

    /* Identify collation to use, if any */
    if type_is_collatable::call(rangeSubtype)? {
        if let Some(ref n) = rangeCollationName {
            rangeCollation =
                backend_catalog_namespace::get_collation_oid(mcx, &as_namelist(n), false)?;
        } else {
            rangeCollation = get_typcollation::call(rangeSubtype)?;
        }
    } else {
        if rangeCollationName.is_some() {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg("range collation specified but subtype does not support collation")
                .finish(errloc(1526, "DefineRange"))
                .map(|()| unreachable!());
        }
        rangeCollation = InvalidOid;
    }

    /* Identify support functions, if provided */
    if let Some(ref n) = rangeCanonicalName {
        if !OidIsValid(typoid) {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("cannot specify a canonical function without a pre-created shell type")
                .errhint("Create the type as a shell type, then create its canonicalization function, then do a full CREATE TYPE.")
                .finish(errloc(1536, "DefineRange"))
                .map(|()| unreachable!());
        }
        rangeCanonical = findRangeCanonicalFunction(mcx, n, typoid)?;
    } else {
        rangeCanonical = InvalidOid;
    }

    if let Some(ref n) = rangeSubtypeDiffName {
        rangeSubtypeDiff = findRangeSubtypeDiffFunction(mcx, n, rangeSubtype)?;
    } else {
        rangeSubtypeDiff = InvalidOid;
    }

    let lba = get_typlenbyvalalign::call(rangeSubtype)?;
    subtypalign = lba.typalign;

    /* alignment must be TYPALIGN_INT or TYPALIGN_DOUBLE for ranges */
    alignment = if subtypalign == TYPALIGN_DOUBLE {
        TYPALIGN_DOUBLE
    } else {
        TYPALIGN_INT
    };

    /* Allocate OID for array type, its multirange, and its multirange array */
    rangeArrayOid = AssignTypeArrayOid()?;
    multirangeOid = AssignTypeMultirangeOid()?;
    multirangeArrayOid = AssignTypeMultirangeArrayOid()?;

    let owner = get_user_id::call();

    /* Create the pg_type entry */
    let address = TypeCreate(TypeCreateParams {
        new_type_oid: InvalidOid,
        type_name: typeName.clone(),
        type_namespace: typeNamespace,
        relation_oid: InvalidOid,
        relation_kind: 0,
        owner_id: owner,
        internal_size: -1,
        type_type: TYPTYPE_RANGE,
        type_category: TYPCATEGORY_RANGE,
        type_preferred: false,
        type_delim: DEFAULT_TYPDELIM,
        input_procedure: F_RANGE_IN,
        output_procedure: F_RANGE_OUT,
        receive_procedure: F_RANGE_RECV,
        send_procedure: F_RANGE_SEND,
        typmodin_procedure: InvalidOid,
        typmodout_procedure: InvalidOid,
        analyze_procedure: F_RANGE_TYPANALYZE,
        subscript_procedure: InvalidOid,
        element_type: InvalidOid,
        is_implicit_array: false,
        array_type: rangeArrayOid,
        base_type: InvalidOid,
        default_type_value: None,
        default_type_bin: None,
        passed_by_value: false,
        alignment,
        storage: TYPSTORAGE_EXTENDED,
        type_mod: -1,
        typ_ndims: 0,
        type_not_null: false,
        type_collation: InvalidOid,
    })?;
    debug_assert!(typoid == InvalidOid || typoid == address.objectId);
    typoid = address.objectId;

    /* Create the multirange that goes with it */
    let multirangeTypeName: String = if let Some(name) = multirangeTypeName {
        /*
         * Look to see if multirange type already exists.
         */
        let old_typoid = get_type_oid::call(&name, multirangeNamespace)?;

        /*
         * If it's not a shell, see if it's an autogenerated array type, and if
         * so rename it out of the way.
         */
        if OidIsValid(old_typoid) && get_typisdefined::call(old_typoid)? {
            if !moveArrayTypeName(old_typoid, &name, multirangeNamespace)? {
                return ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(format!("type \"{name}\" already exists"))
                    .finish(errloc(1619, "DefineRange"))
                    .map(|()| unreachable!());
            }
        }
        name
    } else {
        /* Generate multirange name automatically */
        multirangeNamespace = typeNamespace;
        makeMultirangeTypeName(&typeName, multirangeNamespace)?
    };

    let _mltrngaddress = TypeCreate(TypeCreateParams {
        new_type_oid: multirangeOid,
        type_name: multirangeTypeName.clone(),
        type_namespace: multirangeNamespace,
        relation_oid: InvalidOid,
        relation_kind: 0,
        owner_id: owner,
        internal_size: -1,
        type_type: TYPTYPE_MULTIRANGE,
        type_category: TYPCATEGORY_RANGE,
        type_preferred: false,
        type_delim: DEFAULT_TYPDELIM,
        input_procedure: F_MULTIRANGE_IN,
        output_procedure: F_MULTIRANGE_OUT,
        receive_procedure: F_MULTIRANGE_RECV,
        send_procedure: F_MULTIRANGE_SEND,
        typmodin_procedure: InvalidOid,
        typmodout_procedure: InvalidOid,
        analyze_procedure: F_MULTIRANGE_TYPANALYZE,
        subscript_procedure: InvalidOid,
        element_type: InvalidOid,
        is_implicit_array: false,
        array_type: multirangeArrayOid,
        base_type: InvalidOid,
        default_type_value: None,
        default_type_bin: None,
        passed_by_value: false,
        alignment,
        storage: TYPSTORAGE_EXTENDED, /* 'x' */
        type_mod: -1,
        typ_ndims: 0,
        type_not_null: false,
        type_collation: InvalidOid,
    })?;
    debug_assert!(multirangeOid == _mltrngaddress.objectId);

    /* Create the entry in pg_range */
    RangeCreate(
        mcx,
        typoid,
        rangeSubtype,
        rangeCollation,
        rangeSubOpclass,
        rangeCanonical,
        rangeSubtypeDiff,
        multirangeOid,
    )?;

    /*
     * Create the array type that goes with it.
     */
    let rangeArrayName = makeArrayTypeName(&typeName, typeNamespace)?;

    TypeCreate(TypeCreateParams {
        new_type_oid: rangeArrayOid,
        type_name: rangeArrayName,
        type_namespace: typeNamespace,
        relation_oid: InvalidOid,
        relation_kind: 0,
        owner_id: owner,
        internal_size: -1,
        type_type: TYPTYPE_BASE,
        type_category: TYPCATEGORY_ARRAY,
        type_preferred: false,
        type_delim: DEFAULT_TYPDELIM,
        input_procedure: F_ARRAY_IN,
        output_procedure: F_ARRAY_OUT,
        receive_procedure: F_ARRAY_RECV,
        send_procedure: F_ARRAY_SEND,
        typmodin_procedure: InvalidOid,
        typmodout_procedure: InvalidOid,
        analyze_procedure: F_ARRAY_TYPANALYZE,
        subscript_procedure: F_ARRAY_SUBSCRIPT_HANDLER,
        element_type: typoid,
        is_implicit_array: true,
        array_type: InvalidOid,
        base_type: InvalidOid,
        default_type_value: None,
        default_type_bin: None,
        passed_by_value: false,
        alignment,
        storage: TYPSTORAGE_EXTENDED,
        type_mod: -1,
        typ_ndims: 0,
        type_not_null: false,
        type_collation: InvalidOid,
    })?;

    /* Create the multirange's array type */
    let multirangeArrayName = makeArrayTypeName(&multirangeTypeName, typeNamespace)?;

    TypeCreate(TypeCreateParams {
        new_type_oid: multirangeArrayOid,
        type_name: multirangeArrayName,
        type_namespace: multirangeNamespace,
        relation_oid: InvalidOid,
        relation_kind: 0,
        owner_id: owner,
        internal_size: -1,
        type_type: TYPTYPE_BASE,
        type_category: TYPCATEGORY_ARRAY,
        type_preferred: false,
        type_delim: DEFAULT_TYPDELIM,
        input_procedure: F_ARRAY_IN,
        output_procedure: F_ARRAY_OUT,
        receive_procedure: F_ARRAY_RECV,
        send_procedure: F_ARRAY_SEND,
        typmodin_procedure: InvalidOid,
        typmodout_procedure: InvalidOid,
        analyze_procedure: F_ARRAY_TYPANALYZE,
        subscript_procedure: F_ARRAY_SUBSCRIPT_HANDLER,
        element_type: multirangeOid,
        is_implicit_array: true,
        array_type: InvalidOid,
        base_type: InvalidOid,
        default_type_value: None,
        default_type_bin: None,
        passed_by_value: false,
        alignment,
        storage: TYPSTORAGE_EXTENDED, /* 'x' */
        type_mod: -1,
        typ_ndims: 0,
        type_not_null: false,
        type_collation: InvalidOid,
    })?;

    /* And create the constructor functions for this range type */
    me::make_range_constructors::call(typeName.clone(), typeNamespace, typoid, rangeSubtype)?;
    let castFuncOid = me::make_multirange_constructors::call(
        multirangeTypeName.clone(),
        typeNamespace,
        multirangeOid,
        typoid,
        rangeArrayOid,
    )?;

    /* Create cast from the range type to its multirange type */
    CastCreate(
        mcx,
        typoid,
        multirangeOid,
        castFuncOid,
        InvalidOid,
        InvalidOid,
        COERCION_CODE_EXPLICIT,
        COERCION_METHOD_FUNCTION,
        DEPENDENCY_INTERNAL,
    )?;

    Ok(address)
}

// ===========================================================================
// makeRangeConstructors / makeMultirangeConstructors   (typecmds.c:1769-1972)
// ===========================================================================

/// `makeRangeConstructors(name, namespace, rangeOid, subtype)` (typecmds.c:1769).
///
/// Because there may exist several range types over the same subtype, the range
/// type can't be uniquely determined from the subtype.  So it's impossible to
/// define a polymorphic constructor; we generate new constructor functions
/// explicitly for each range type.  We define 2 functions, with 2 and 3
/// arguments, named `range_constructor2`/`range_constructor3`.
fn makeRangeConstructors(name: &str, namespace: Oid, rangeOid: Oid, subtype: Oid) -> PgResult<()> {
    const PROSRC: [&str; 2] = ["range_constructor2", "range_constructor3"];
    const PRONARGS: [usize; 2] = [2, 3];

    let constructorArgTypes: [Oid; 3] = [subtype, subtype, TEXTOID];

    let referenced = ObjectAddress {
        classId: TypeRelationId,
        objectId: rangeOid,
        objectSubId: 0,
    };

    for i in 0..PROSRC.len() {
        let myself = backend_catalog_pg_proc::ProcedureCreate(
            name,                          /* name: same as range type */
            namespace,                     /* namespace */
            false,                         /* replace */
            false,                         /* returns set */
            rangeOid,                      /* return type */
            BOOTSTRAP_SUPERUSERID,         /* proowner */
            INTERNALlanguageId,            /* language */
            F_FMGR_INTERNAL_VALIDATOR,     /* language validator */
            PROSRC[i],                     /* prosrc */
            None,                          /* probin */
            None,                          /* prosqlbody */
            Vec::new(),                    /* prosqlbody_refs */
            PROKIND_FUNCTION,
            false,                         /* security_definer */
            false,                         /* leakproof */
            false,                         /* isStrict */
            PROVOLATILE_IMMUTABLE,         /* volatility */
            PROPARALLEL_SAFE,              /* parallel safety */
            &constructorArgTypes[..PRONARGS[i]], /* parameterTypes */
            None,                          /* allParameterTypes */
            None,                          /* parameterModes */
            None,                          /* parameterNames */
            Vec::new(),                    /* parameterDefaults */
            None,                          /* trftypes */
            Vec::new(),                    /* trfoids */
            None,                          /* proconfig */
            InvalidOid,                    /* prosupport */
            1.0,                           /* procost */
            0.0,                           /* prorows */
        )?;

        /*
         * Make the constructors internally-dependent on the range type so that
         * they go away silently when the type is dropped.  Note that pg_dump
         * depends on this choice to avoid dumping the constructors.
         */
        backend_catalog_dependency_seams::record_dependency_on::call(
            myself,
            referenced,
            DEPENDENCY_INTERNAL,
        )?;
    }

    Ok(())
}

/// `makeMultirangeConstructors(name, namespace, multirangeOid, rangeOid,
/// rangeArrayOid, *castFuncOid)` (typecmds.c:1845).
///
/// We make a separate multirange constructor for each range type so its name
/// can include the base type, like range constructors do.  Returns the OID of
/// the 1-arg constructor usable to cast from a range to a multirange (the C
/// `*castFuncOid` out-parameter).
fn makeMultirangeConstructors(
    name: &str,
    namespace: Oid,
    multirangeOid: Oid,
    rangeOid: Oid,
    rangeArrayOid: Oid,
) -> PgResult<Oid> {
    let referenced = ObjectAddress {
        classId: TypeRelationId,
        objectId: multirangeOid,
        objectSubId: 0,
    };

    /* 0-arg constructor - for empty multiranges */
    let myself = backend_catalog_pg_proc::ProcedureCreate(
        name,
        namespace,
        false,
        false,
        multirangeOid,
        BOOTSTRAP_SUPERUSERID,
        INTERNALlanguageId,
        F_FMGR_INTERNAL_VALIDATOR,
        "multirange_constructor0",
        None,
        None,
        Vec::new(),
        PROKIND_FUNCTION,
        false,
        false,
        true, /* isStrict */
        PROVOLATILE_IMMUTABLE,
        PROPARALLEL_SAFE,
        &[], /* parameterTypes */
        None,
        None,
        None,
        Vec::new(),
        None,
        Vec::new(),
        None,
        InvalidOid,
        1.0,
        0.0,
    )?;
    backend_catalog_dependency_seams::record_dependency_on::call(
        myself,
        referenced,
        DEPENDENCY_INTERNAL,
    )?;

    /*
     * 1-arg constructor - for casts
     *
     * In theory we shouldn't need both this and the vararg (n-arg) constructor,
     * but having a separate 1-arg function lets us define casts against it.
     */
    let myself = backend_catalog_pg_proc::ProcedureCreate(
        name,
        namespace,
        false,
        false,
        multirangeOid,
        BOOTSTRAP_SUPERUSERID,
        INTERNALlanguageId,
        F_FMGR_INTERNAL_VALIDATOR,
        "multirange_constructor1",
        None,
        None,
        Vec::new(),
        PROKIND_FUNCTION,
        false,
        false,
        true,
        PROVOLATILE_IMMUTABLE,
        PROPARALLEL_SAFE,
        &[rangeOid], /* parameterTypes */
        None,
        None,
        None,
        Vec::new(),
        None,
        Vec::new(),
        None,
        InvalidOid,
        1.0,
        0.0,
    )?;
    backend_catalog_dependency_seams::record_dependency_on::call(
        myself,
        referenced,
        DEPENDENCY_INTERNAL,
    )?;
    let castFuncOid = myself.objectId;

    /* n-arg constructor - vararg */
    let myself = backend_catalog_pg_proc::ProcedureCreate(
        name,
        namespace,
        false,
        false,
        multirangeOid,
        BOOTSTRAP_SUPERUSERID,
        INTERNALlanguageId,
        F_FMGR_INTERNAL_VALIDATOR,
        "multirange_constructor2",
        None,
        None,
        Vec::new(),
        PROKIND_FUNCTION,
        false,
        false,
        true,
        PROVOLATILE_IMMUTABLE,
        PROPARALLEL_SAFE,
        &[rangeArrayOid],            /* parameterTypes */
        Some(vec![rangeArrayOid]),  /* allParameterTypes */
        Some(vec![FUNC_PARAM_VARIADIC]), /* parameterModes */
        None,
        Vec::new(),
        None,
        Vec::new(),
        None,
        InvalidOid,
        1.0,
        0.0,
    )?;
    backend_catalog_dependency_seams::record_dependency_on::call(
        myself,
        referenced,
        DEPENDENCY_INTERNAL,
    )?;

    Ok(castFuncOid)
}

// ===========================================================================
// findType*Function helpers   (typecmds.c:1991-2309)
// ===========================================================================

/// `findTypeInputFunction(procname, typeOid)` (typecmds.c:1991).
fn findTypeInputFunction(mcx: Mcx<'_>, procname: &[String], typeOid: Oid) -> PgResult<Oid> {
    /*
     * Input functions can take a single argument of type CSTRING, or three
     * arguments (string, typioparam OID, typmod).
     */
    let argList = [CSTRINGOID, OIDOID, INT4OID];

    let mut procOid = lookup_func_name::call(procname.to_vec(), 1, argList[..1].to_vec(), true)?;
    let procOid2 = lookup_func_name::call(procname.to_vec(), 3, argList.to_vec(), true)?;
    if OidIsValid(procOid) {
        if OidIsValid(procOid2) {
            return ereport(ERROR)
                .errcode(ERRCODE_AMBIGUOUS_FUNCTION)
                .errmsg(format!(
                    "type input function {} has multiple matches",
                    NameListToString_seam(mcx, procname)?
                ))
                .finish(errloc(2013, "findTypeInputFunction"))
                .map(|()| unreachable!());
        }
    } else {
        procOid = procOid2;
        /* If not found, reference the 1-argument signature in error msg */
        if !OidIsValid(procOid) {
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                .errmsg(format!(
                    "function {} does not exist",
                    func_signature_string::call(procname.to_vec(), 1, argList[..1].to_vec())?
                ))
                .finish(errloc(2022, "findTypeInputFunction"))
                .map(|()| unreachable!());
        }
    }

    /* Input functions must return the target type. */
    if get_func_rettype::call(procOid)? != typeOid {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "type input function {} must return type {}",
                NameListToString_seam(mcx, procname)?,
                format_type_be_owned::call(typeOid)?
            ))
            .finish(errloc(2030, "findTypeInputFunction"))
            .map(|()| unreachable!());
    }

    /* Print warnings if the type's I/O function is marked volatile. */
    if func_volatile::call(procOid)? as i8 == PROVOLATILE_VOLATILE {
        ereport(WARNING)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "type input function {} should not be volatile",
                NameListToString_seam(mcx, procname)?
            ))
            .finish(errloc(2046, "findTypeInputFunction"))?;
    }

    Ok(procOid)
}

/// `findTypeOutputFunction(procname, typeOid)` (typecmds.c:2053).
fn findTypeOutputFunction(mcx: Mcx<'_>, procname: &[String], typeOid: Oid) -> PgResult<Oid> {
    /* Output functions always take a single argument of the type, return cstring. */
    let argList = [typeOid];

    let procOid = lookup_func_name::call(procname.to_vec(), 1, argList.to_vec(), true)?;
    if !OidIsValid(procOid) {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "function {} does not exist",
                func_signature_string::call(procname.to_vec(), 1, argList.to_vec())?
            ))
            .finish(errloc(2067, "findTypeOutputFunction"))
            .map(|()| unreachable!());
    }

    if get_func_rettype::call(procOid)? != CSTRINGOID {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "type output function {} must return type {}",
                NameListToString_seam(mcx, procname)?,
                "cstring"
            ))
            .finish(errloc(2074, "findTypeOutputFunction"))
            .map(|()| unreachable!());
    }

    /* Just a warning for now, per comments in findTypeInputFunction */
    if func_volatile::call(procOid)? as i8 == PROVOLATILE_VOLATILE {
        ereport(WARNING)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "type output function {} should not be volatile",
                NameListToString_seam(mcx, procname)?
            ))
            .finish(errloc(2081, "findTypeOutputFunction"))?;
    }

    Ok(procOid)
}

/// `findTypeReceiveFunction(procname, typeOid)` (typecmds.c:2088).
fn findTypeReceiveFunction(mcx: Mcx<'_>, procname: &[String], typeOid: Oid) -> PgResult<Oid> {
    /*
     * Receive functions can take a single argument of type INTERNAL, or three
     * arguments (internal, typioparam OID, typmod).
     */
    let argList = [INTERNALOID, OIDOID, INT4OID];

    let mut procOid = lookup_func_name::call(procname.to_vec(), 1, argList[..1].to_vec(), true)?;
    let procOid2 = lookup_func_name::call(procname.to_vec(), 3, argList.to_vec(), true)?;
    if OidIsValid(procOid) {
        if OidIsValid(procOid2) {
            return ereport(ERROR)
                .errcode(ERRCODE_AMBIGUOUS_FUNCTION)
                .errmsg(format!(
                    "type receive function {} has multiple matches",
                    NameListToString_seam(mcx, procname)?
                ))
                .finish(errloc(2111, "findTypeReceiveFunction"))
                .map(|()| unreachable!());
        }
    } else {
        procOid = procOid2;
        if !OidIsValid(procOid) {
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                .errmsg(format!(
                    "function {} does not exist",
                    func_signature_string::call(procname.to_vec(), 1, argList[..1].to_vec())?
                ))
                .finish(errloc(2121, "findTypeReceiveFunction"))
                .map(|()| unreachable!());
        }
    }

    /* Receive functions must return the target type. */
    if get_func_rettype::call(procOid)? != typeOid {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "type receive function {} must return type {}",
                NameListToString_seam(mcx, procname)?,
                format_type_be_owned::call(typeOid)?
            ))
            .finish(errloc(2129, "findTypeReceiveFunction"))
            .map(|()| unreachable!());
    }

    if func_volatile::call(procOid)? as i8 == PROVOLATILE_VOLATILE {
        ereport(WARNING)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "type receive function {} should not be volatile",
                NameListToString_seam(mcx, procname)?
            ))
            .finish(errloc(2136, "findTypeReceiveFunction"))?;
    }

    Ok(procOid)
}

/// `findTypeSendFunction(procname, typeOid)` (typecmds.c:2142).
fn findTypeSendFunction(mcx: Mcx<'_>, procname: &[String], typeOid: Oid) -> PgResult<Oid> {
    /* Send functions always take a single argument of the type, return bytea. */
    let argList = [typeOid];

    let procOid = lookup_func_name::call(procname.to_vec(), 1, argList.to_vec(), true)?;
    if !OidIsValid(procOid) {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "function {} does not exist",
                func_signature_string::call(procname.to_vec(), 1, argList.to_vec())?
            ))
            .finish(errloc(2157, "findTypeSendFunction"))
            .map(|()| unreachable!());
    }

    if get_func_rettype::call(procOid)? != BYTEAOID {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "type send function {} must return type {}",
                NameListToString_seam(mcx, procname)?,
                "bytea"
            ))
            .finish(errloc(2164, "findTypeSendFunction"))
            .map(|()| unreachable!());
    }

    if func_volatile::call(procOid)? as i8 == PROVOLATILE_VOLATILE {
        ereport(WARNING)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "type send function {} should not be volatile",
                NameListToString_seam(mcx, procname)?
            ))
            .finish(errloc(2171, "findTypeSendFunction"))?;
    }

    Ok(procOid)
}

/// `findTypeTypmodinFunction(procname)` (typecmds.c:2177).
fn findTypeTypmodinFunction(mcx: Mcx<'_>, procname: &[String]) -> PgResult<Oid> {
    /* typmodin functions always take one cstring[] argument and return int4. */
    let argList = [CSTRINGARRAYOID];

    let procOid = lookup_func_name::call(procname.to_vec(), 1, argList.to_vec(), true)?;
    if !OidIsValid(procOid) {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "function {} does not exist",
                func_signature_string::call(procname.to_vec(), 1, argList.to_vec())?
            ))
            .finish(errloc(2191, "findTypeTypmodinFunction"))
            .map(|()| unreachable!());
    }

    if get_func_rettype::call(procOid)? != INT4OID {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "typmod_in function {} must return type {}",
                NameListToString_seam(mcx, procname)?,
                "integer"
            ))
            .finish(errloc(2198, "findTypeTypmodinFunction"))
            .map(|()| unreachable!());
    }

    if func_volatile::call(procOid)? as i8 == PROVOLATILE_VOLATILE {
        ereport(WARNING)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "type modifier input function {} should not be volatile",
                NameListToString_seam(mcx, procname)?
            ))
            .finish(errloc(2205, "findTypeTypmodinFunction"))?;
    }

    Ok(procOid)
}

/// `findTypeTypmodoutFunction(procname)` (typecmds.c:2211).
fn findTypeTypmodoutFunction(mcx: Mcx<'_>, procname: &[String]) -> PgResult<Oid> {
    /* typmodout functions always take one int4 argument and return cstring. */
    let argList = [INT4OID];

    let procOid = lookup_func_name::call(procname.to_vec(), 1, argList.to_vec(), true)?;
    if !OidIsValid(procOid) {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "function {} does not exist",
                func_signature_string::call(procname.to_vec(), 1, argList.to_vec())?
            ))
            .finish(errloc(2225, "findTypeTypmodoutFunction"))
            .map(|()| unreachable!());
    }

    if get_func_rettype::call(procOid)? != CSTRINGOID {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "typmod_out function {} must return type {}",
                NameListToString_seam(mcx, procname)?,
                "cstring"
            ))
            .finish(errloc(2232, "findTypeTypmodoutFunction"))
            .map(|()| unreachable!());
    }

    if func_volatile::call(procOid)? as i8 == PROVOLATILE_VOLATILE {
        ereport(WARNING)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "type modifier output function {} should not be volatile",
                NameListToString_seam(mcx, procname)?
            ))
            .finish(errloc(2239, "findTypeTypmodoutFunction"))?;
    }

    Ok(procOid)
}

/// `findTypeAnalyzeFunction(procname, typeOid)` (typecmds.c:2245).
fn findTypeAnalyzeFunction(mcx: Mcx<'_>, procname: &[String], _typeOid: Oid) -> PgResult<Oid> {
    /* Analyze functions always take one INTERNAL argument and return bool. */
    let argList = [INTERNALOID];

    let procOid = lookup_func_name::call(procname.to_vec(), 1, argList.to_vec(), true)?;
    if !OidIsValid(procOid) {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "function {} does not exist",
                func_signature_string::call(procname.to_vec(), 1, argList.to_vec())?
            ))
            .finish(errloc(2258, "findTypeAnalyzeFunction"))
            .map(|()| unreachable!());
    }

    if get_func_rettype::call(procOid)? != BOOLOID {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "type analyze function {} must return type {}",
                NameListToString_seam(mcx, procname)?,
                "boolean"
            ))
            .finish(errloc(2265, "findTypeAnalyzeFunction"))
            .map(|()| unreachable!());
    }

    Ok(procOid)
}

/// `findTypeSubscriptingFunction(procname, typeOid)` (typecmds.c:2272).
fn findTypeSubscriptingFunction(mcx: Mcx<'_>, procname: &[String], _typeOid: Oid) -> PgResult<Oid> {
    /*
     * Subscripting support functions always take one INTERNAL argument and
     * return INTERNAL.
     */
    let argList = [INTERNALOID];

    let procOid = lookup_func_name::call(procname.to_vec(), 1, argList.to_vec(), true)?;
    if !OidIsValid(procOid) {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "function {} does not exist",
                func_signature_string::call(procname.to_vec(), 1, argList.to_vec())?
            ))
            .finish(errloc(2289, "findTypeSubscriptingFunction"))
            .map(|()| unreachable!());
    }

    if get_func_rettype::call(procOid)? != INTERNALOID {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "type subscripting function {} must return type {}",
                NameListToString_seam(mcx, procname)?,
                "internal"
            ))
            .finish(errloc(2296, "findTypeSubscriptingFunction"))
            .map(|()| unreachable!());
    }

    /*
     * We disallow array_subscript_handler() from being selected explicitly,
     * since that must only be applied to autogenerated array types.
     */
    if procOid == F_ARRAY_SUBSCRIPT_HANDLER {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "user-defined types cannot use subscripting function {}",
                NameListToString_seam(mcx, procname)?
            ))
            .finish(errloc(2306, "findTypeSubscriptingFunction"))
            .map(|()| unreachable!());
    }

    Ok(procOid)
}

// ===========================================================================
// findRange* helpers   (typecmds.c:2319-2440)
// ===========================================================================

/// `findRangeSubOpclass(opcname, subtype)` (typecmds.c:2319).
fn findRangeSubOpclass(mcx: Mcx<'_>, opcname: Option<&[String]>, subtype: Oid) -> PgResult<Oid> {
    let opcid: Oid;

    if let Some(opcname) = opcname {
        let opc_refs: Vec<&str> = opcname.iter().map(|s| s.as_str()).collect();
        opcid = get_opclass_oid::call(mcx, BTREE_AM_OID, &opc_refs, false)?;

        /*
         * Verify that the operator class accepts this datatype. Note we will
         * accept binary compatibility.
         */
        let opInputType = get_opclass_input_type::call(opcid)?;
        if !is_binary_coercible::call(subtype, opInputType)? {
            return ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "operator class \"{}\" does not accept data type {}",
                    NameListToString_seam(mcx, opcname)?,
                    format_type_be_owned::call(subtype)?
                ))
                .finish(errloc(2337, "findRangeSubOpclass"))
                .map(|()| unreachable!());
        }
    } else {
        opcid = get_default_opclass::call(subtype, BTREE_AM_OID)?;
        if !OidIsValid(opcid) {
            /* We spell the error message identically to ResolveOpClass */
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "data type {} has no default operator class for access method \"{}\"",
                    format_type_be_owned::call(subtype)?,
                    "btree"
                ))
                .errhint("You must specify an operator class for the range type or define a default operator class for the subtype.")
                .finish(errloc(2351, "findRangeSubOpclass"))
                .map(|()| unreachable!());
        }
    }

    Ok(opcid)
}

/// `findRangeCanonicalFunction(procname, typeOid)` (typecmds.c:2358).
fn findRangeCanonicalFunction(_mcx: Mcx<'_>, procname: &[String], typeOid: Oid) -> PgResult<Oid> {
    /*
     * Range canonical functions must take and return the range type, and must
     * be immutable.
     */
    let argList = [typeOid];

    let procOid = lookup_func_name::call(procname.to_vec(), 1, argList.to_vec(), true)?;

    if !OidIsValid(procOid) {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "function {} does not exist",
                func_signature_string::call(procname.to_vec(), 1, argList.to_vec())?
            ))
            .finish(errloc(2377, "findRangeCanonicalFunction"))
            .map(|()| unreachable!());
    }

    if get_func_rettype::call(procOid)? != typeOid {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "range canonical function {} must return range type",
                func_signature_string::call(procname.to_vec(), 1, argList.to_vec())?
            ))
            .finish(errloc(2383, "findRangeCanonicalFunction"))
            .map(|()| unreachable!());
    }

    if func_volatile::call(procOid)? as i8 != PROVOLATILE_IMMUTABLE {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "range canonical function {} must be immutable",
                func_signature_string::call(procname.to_vec(), 1, argList.to_vec())?
            ))
            .finish(errloc(2389, "findRangeCanonicalFunction"))
            .map(|()| unreachable!());
    }

    /* Also, range type's creator must have permission to call function */
    let aclresult = object_aclcheck::call(
        ProcedureRelationId,
        procOid,
        get_user_id::call(),
        ACL_EXECUTE as AclMode,
    )?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error::call(aclresult, OBJECT_FUNCTION, get_func_name::call(procOid)?)?;
    }

    Ok(procOid)
}

/// `findRangeSubtypeDiffFunction(procname, subtype)` (typecmds.c:2399).
fn findRangeSubtypeDiffFunction(_mcx: Mcx<'_>, procname: &[String], subtype: Oid) -> PgResult<Oid> {
    /*
     * Range subtype diff functions must take two arguments of the subtype, must
     * return float8, and must be immutable.
     */
    let argList = [subtype, subtype];

    let procOid = lookup_func_name::call(procname.to_vec(), 2, argList.to_vec(), true)?;

    if !OidIsValid(procOid) {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "function {} does not exist",
                func_signature_string::call(procname.to_vec(), 2, argList.to_vec())?
            ))
            .finish(errloc(2419, "findRangeSubtypeDiffFunction"))
            .map(|()| unreachable!());
    }

    if get_func_rettype::call(procOid)? != FLOAT8OID {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "range subtype diff function {} must return type {}",
                func_signature_string::call(procname.to_vec(), 2, argList.to_vec())?,
                "double precision"
            ))
            .finish(errloc(2426, "findRangeSubtypeDiffFunction"))
            .map(|()| unreachable!());
    }

    if func_volatile::call(procOid)? as i8 != PROVOLATILE_IMMUTABLE {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "range subtype diff function {} must be immutable",
                func_signature_string::call(procname.to_vec(), 2, argList.to_vec())?
            ))
            .finish(errloc(2432, "findRangeSubtypeDiffFunction"))
            .map(|()| unreachable!());
    }

    /* Also, range type's creator must have permission to call function */
    let aclresult = object_aclcheck::call(
        ProcedureRelationId,
        procOid,
        get_user_id::call(),
        ACL_EXECUTE as AclMode,
    )?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error::call(aclresult, OBJECT_FUNCTION, get_func_name::call(procOid)?)?;
    }

    Ok(procOid)
}

// ===========================================================================
// AssignType*Oid   (typecmds.c:2447-2539)
// ===========================================================================

/// `AssignTypeArrayOid(void)` (typecmds.c:2447) — pre-assign the type's array
/// OID for use in `pg_type.typarray`.
///
/// Binary-upgrade override: the C reads the dedicated
/// `binary_upgrade_next_array_pg_type_oid` global; the repo's binary-upgrade
/// model exposes a single `pg_type` OID slot (`consume_next_pg_type_oid`, also
/// used by `TypeCreate`), so all three `AssignType*Oid` consume that one slot in
/// the order they are called — preserving the per-OID validation and clearing.
pub fn AssignTypeArrayOid() -> PgResult<Oid> {
    /* Use binary-upgrade override for pg_type.typarray? */
    if is_binary_upgrade::call() {
        let next = consume_next_pg_type_oid::call();
        if !OidIsValid(next) {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("pg_type array OID value not set when in binary upgrade mode")
                .finish(errloc(2458, "AssignTypeArrayOid"))
                .map(|()| unreachable!());
        }
        Ok(next)
    } else {
        /* table_open(TypeRelationId, AccessShareLock) + GetNewOidWithIndex(...) */
        backend_catalog_pg_type_seams::get_new_type_oid::call()
    }
}

/// `AssignTypeMultirangeOid(void)` (typecmds.c:2480) — pre-assign the range
/// type's multirange OID for use in `pg_type.oid`.
pub fn AssignTypeMultirangeOid() -> PgResult<Oid> {
    /* Use binary-upgrade override for pg_type.oid? */
    if is_binary_upgrade::call() {
        let next = consume_next_pg_type_oid::call();
        if !OidIsValid(next) {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("pg_type multirange OID value not set when in binary upgrade mode")
                .finish(errloc(2491, "AssignTypeMultirangeOid"))
                .map(|()| unreachable!());
        }
        Ok(next)
    } else {
        backend_catalog_pg_type_seams::get_new_type_oid::call()
    }
}

/// `AssignTypeMultirangeArrayOid(void)` (typecmds.c:2513) — pre-assign the range
/// type's multirange array OID for use in `pg_type.typarray`.
pub fn AssignTypeMultirangeArrayOid() -> PgResult<Oid> {
    /* Use binary-upgrade override for pg_type.oid? */
    if is_binary_upgrade::call() {
        let next = consume_next_pg_type_oid::call();
        if !OidIsValid(next) {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("pg_type multirange array OID value not set when in binary upgrade mode")
                .finish(errloc(2524, "AssignTypeMultirangeArrayOid"))
                .map(|()| unreachable!());
        }
        Ok(next)
    } else {
        backend_catalog_pg_type_seams::get_new_type_oid::call()
    }
}

// ===========================================================================
// DefineCompositeType   (typecmds.c:2555-2604)
// ===========================================================================

/// `DefineCompositeType(typevar, coldeflist)` (typecmds.c:2555) — create a
/// composite type relation.  `DefineRelation` does all the work; we just build
/// the `CreateStmt` arguments and check for a type-name collision first.
///
/// `typevar` is the composite type's `RangeVar`; `coldeflist` is the list of
/// `ColumnDef` nodes (the `(...)` after AS).
pub fn DefineCompositeType<'mcx>(
    mcx: Mcx<'mcx>,
    mut typevar: types_nodes::rawnodes::RangeVar<'mcx>,
    coldeflist: mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
) -> PgResult<ObjectAddress> {
    /*
     * now set the parameters for keys/inheritance etc. All of these are
     * uninteresting for composite types... (the CreateStmt is assembled below,
     * after the type-name collision check).
     */

    /*
     * Check for collision with an existing type name. If there is one and it's
     * an autogenerated array, we can rename it out of the way.  This check is
     * here mainly to get a better error message about a "type" instead of below
     * about a "relation".
     */
    let mut access_rv = composite_to_access_range_var(&typevar);
    let typeNamespace = RangeVarGetAndCheckCreationNamespace(mcx, &mut access_rv, NoLock, None)?;
    RangeVarAdjustRelationPersistence(mcx, &mut access_rv, typeNamespace)?;
    /* propagate the (possibly temp-promoted) persistence back to the node */
    typevar.relpersistence = access_rv.relpersistence as i8;

    let relname: String = access_rv.relname.clone();
    let old_type_oid = get_type_oid::call(&relname, typeNamespace)?;
    if OidIsValid(old_type_oid) {
        if !moveArrayTypeName(old_type_oid, &relname, typeNamespace)? {
            return ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("type \"{relname}\" already exists"))
                .finish(errloc(2594, "DefineCompositeType"))
                .map(|()| unreachable!());
        }
    }

    /*
     * Finally create the relation.  This also creates the type.
     *
     *   createStmt->relation = typevar;
     *   createStmt->tableElts = coldeflist;
     *   createStmt->inhRelations = NIL;  ... all uninteresting for composites.
     *   DefineRelation(createStmt, RELKIND_COMPOSITE_TYPE, InvalidOid,
     *                  &address, NULL);
     */
    let create_stmt = types_nodes::ddlnodes::CreateStmt {
        relation: Some(mcx::alloc_in(
            mcx,
            types_nodes::nodes::Node::mk_range_var(mcx, typevar),
        )?),
        tableElts: coldeflist,
        inhRelations: mcx::vec_with_capacity_in(mcx, 0)?,
        partbound: None,
        partspec: None,
        ofTypename: None,
        constraints: mcx::vec_with_capacity_in(mcx, 0)?,
        nnconstraints: mcx::vec_with_capacity_in(mcx, 0)?,
        options: mcx::vec_with_capacity_in(mcx, 0)?,
        oncommit: types_nodes::primnodes::OnCommitAction::ONCOMMIT_NOOP,
        tablespacename: None,
        accessMethod: None,
        if_not_exists: false,
    };

    let address = backend_commands_tablecmds_seams::define_relation::call(
        mcx,
        create_stmt,
        RELKIND_COMPOSITE_TYPE,
        InvalidOid,
        None,
    )?;

    Ok(address)
}

/// Convert the parse-node [`RangeVar`](types_nodes::rawnodes::RangeVar) carried
/// by a `CompositeTypeStmt` into the `access::RangeVar` shape the namespace
/// resolver mutates (mirrors `view.c`'s `to_access_range_var`).
fn composite_to_access_range_var(
    rv: &types_nodes::rawnodes::RangeVar<'_>,
) -> types_tuple::access::RangeVar {
    types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_ref().map(|s| s.as_str().to_string()),
        schemaname: rv.schemaname.as_ref().map(|s| s.as_str().to_string()),
        relname: rv
            .relname
            .as_ref()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/// `ProcessUtilitySlow` `T_CompositeTypeStmt` dispatch target (utility.c:1625):
/// decode the `CompositeTypeStmt`'s `typevar` + `coldeflist` and run the ported
/// [`DefineCompositeType`] body.
fn define_composite_type_seam<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &RichNode<'mcx>,
) -> PgResult<ObjectAddress> {
    let cts = match stmt.as_compositetypestmt() {
        Some(s) => s,
        None => {
            return Err(PgError::error(
                "define_composite_type_seam: statement is not a CompositeTypeStmt",
            ))
        }
    };

    let typevar = match cts.typevar.as_deref().and_then(|n| n.as_rangevar()) {
        Some(rv) => rv.clone_in(mcx)?,
        None => {
            return Err(PgError::error(
                "CREATE TYPE AS (...): typevar is not a RangeVar",
            ))
        }
    };

    /* Deep-copy the coldeflist ColumnDef nodes into a fresh PgVec. */
    let mut coldeflist: mcx::PgVec<types_nodes::nodes::NodePtr> =
        mcx::vec_with_capacity_in(mcx, cts.coldeflist.len())?;
    for n in cts.coldeflist.iter() {
        coldeflist.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
    }

    DefineCompositeType(mcx, typevar, coldeflist)
}

// ===========================================================================
// F3 (DOMAIN) + F4 (ALTER TYPE / RENAME / namespace / owner)
//
// The catalog WRITES go through pg_type owner seams (set_type_owner /
// set_type_namespace / set_type_not_null / set_domain_default /
// alter_type_recurse_update) and the ported pg_constraint / namespace /
// dependency / pg_shdepend / pg_depend / objectaccess owners. The
// DEFAULT/CHECK expression cook (cookDefault / transformExpr /
// coerce_to_{boolean,target_type} / assign_expr_collations) is done in-crate by
// calling the parser owners directly; `deparse_expression` / `nodeToString` are
// reached through this unit's installed outward seams. The composite-rel ALTER
// paths (RenameRelationInternal / ATExecChangeOwner /
// AlterRelationNamespaceInternal) cross tablecmds-seams (no tablecmds owner yet
// → panic).
// ===========================================================================

use types_catalog::catalog::{NAMESPACE_RELATION_ID, RELATION_RELATION_ID};
use types_catalog::pg_type::{
    TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN as PGT_TYPTYPE_DOMAIN,
};
use types_nodes::ddlnodes::{ConstrType, CONSTR_CHECK, CONSTR_DEFAULT, CONSTR_NOTNULL, CONSTR_NULL};
use types_nodes::nodes::Node as RichNode;
use types_nodes::parsenodes::{DropBehavior, ObjectType, DROP_RESTRICT, OBJECT_DOMAIN, OBJECT_SCHEMA};
use types_nodes::parsestmt::ParseExprKind;
use types_nodes::primnodes::Expr;
use types_catalog::pg_constraint::{ConstraintCategory, CONSTRAINT_CHECK, CONSTRAINT_NOTNULL};
use types_error::ERRCODE_INVALID_COLUMN_REFERENCE;

/// `F_DOMAIN_IN` (fmgroids.h) — `domain_in`.
const F_DOMAIN_IN: Oid = 4150;
/// `F_DOMAIN_RECV` (fmgroids.h) — `domain_recv`.
const F_DOMAIN_RECV: Oid = 4151;

/// `RELKIND_COMPOSITE_TYPE` ('c'), as `u8` (matches `get_rel_relkind`).
const RELKIND_COMPOSITE_TYPE: u8 = b'c';
/// `ConstraintRelationId` — pg_constraint's OID (catalog).
const ConstraintRelationId: Oid = types_catalog::catalog::CONSTRAINT_RELATION_ID;
/// `NamespaceRelationId` — pg_namespace's OID.
const NamespaceRelationId: Oid = NAMESPACE_RELATION_ID;
/// `RelationRelationId` — pg_class's OID.
const RelationRelationId: Oid = RELATION_RELATION_ID;
/// `AccessExclusiveLock` (lockdefs.h).
const AccessExclusiveLock: i32 = 8;

/// `makeTypeNameFromNameList(names)` — wrap a bare name list as the resolver
/// `TypeName` consumed by the parse_type.c seams.
fn type_name_from_namelist(names: &[String]) -> types_opclass::TypeName {
    types_opclass::TypeName {
        names: names.to_vec(),
        typeOid: InvalidOid,
        setof: false,
        pct_type: false,
        typemod: -1,
        arrayBounds: Vec::new(),
        location: -1,
    }
}

/// `typenameTypeId(NULL, makeTypeNameFromNameList(names))`.
fn typename_type_id_from_names(names: &[String]) -> PgResult<Oid> {
    backend_parser_parse_type_seams::typename_type_id::call(&type_name_from_namelist(names))
}

/// `SearchSysCacheCopy1(TYPEOID, oid)` + `GETSTRUCT(Form_pg_type)` projected to
/// the fixed-part [`FormData_pg_type`]; `elog(ERROR, "cache lookup failed for
/// type %u")` on a missing row (mirrors the C `!HeapTupleIsValid`).
fn read_type_form(type_oid: Oid) -> PgResult<types_tuple::pg_type::FormData_pg_type> {
    match backend_utils_cache_syscache_seams::pg_type_form::call(type_oid)? {
        Some(f) => Ok(f),
        None => ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for type {type_oid}"))
            .finish(errloc(0, "typecmds"))
            .map(|()| unreachable!()),
    }
}

/// `IsTrueArrayType(typTup)` (typecmds.c:120) — a true array type has a nonzero
/// `typelem` and uses `array_subscript_handler` as its `typsubscript`.
fn is_true_array_type(typ: &types_tuple::pg_type::FormData_pg_type) -> bool {
    OidIsValid(typ.typelem) && typ.typsubscript == F_ARRAY_SUBSCRIPT_HANDLER
}

/// `format_type_be(oid)` text.
fn format_type_be(oid: Oid) -> PgResult<String> {
    format_type_be_owned::call(oid)
}

/// `NameStr(typTup->typname)`.
fn type_name_str(typ: &types_tuple::pg_type::FormData_pg_type) -> String {
    String::from_utf8_lossy(typ.typname.name_str()).into_owned()
}

// ---------------------------------------------------------------------------
// cookDefault / domainAddCheckConstraint / domainAddNotNullConstraint
//
// In C these are `catalog/heap.c` (cookDefault) and `commands/typecmds.c`
// statics. They are ported here as real functions calling the parser /
// pg_constraint owners directly (no cycle), replacing the prior outward-seam
// placeholders.
// ---------------------------------------------------------------------------

/// `cookDefault(pstate, raw_default, atttypid, atttypmod, attname, 0)`
/// (catalog/heap.c:3323) — transform a raw DEFAULT / domain-default expression
/// to the target type and cook it into an executable [`Expr`]. The domain path
/// always passes `attgenerated == 0`, so the generated-column legs are omitted.
/// Returns the cooked node (`Node::Expr`); a NULL-constant default flows through
/// here as a `Const` whose `constisnull` is set, exactly as in C.
fn cook_default<'mcx>(
    mcx: Mcx<'mcx>,
    raw_default: RichNode<'mcx>,
    atttypid: Oid,
    atttypmod: i32,
    attname: &str,
) -> PgResult<Option<RichNode<'mcx>>> {
    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;

    /* Transform raw parsetree to executable expression. */
    let expr = backend_parser_parse_expr::transformExpr(
        &mut pstate,
        Some(raw_default),
        ParseExprKind::EXPR_KIND_COLUMN_DEFAULT,
    )?;
    let mut expr = match expr {
        Some(e) => e,
        None => return Ok(None),
    };

    /*
     * For a default expression, transformExpr() should have rejected column
     * references (Assert(!contain_var_clause(expr)) in C).
     */

    /*
     * Coerce the expression to the correct type and typmod, if given. This
     * matches the parser's processing of non-defaulted expressions
     * (transformAssignedExpr).
     */
    if OidIsValid(atttypid) {
        let type_id = backend_nodes_core::nodefuncs::expr_type(Some(&expr))?;
        let coerced = backend_parser_coerce::coerce_to_target_type(
            mcx,
            Some(&mut pstate),
            expr,
            type_id,
            atttypid,
            atttypmod,
            types_nodes::ddlnodes::CoercionContext::COERCION_ASSIGNMENT,
            types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        expr = match coerced {
            Some(c) => c,
            None => {
                return ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(format!(
                        "column \"{}\" is of type {} but default expression is of type {}",
                        attname,
                        format_type_be(atttypid)?,
                        format_type_be(type_id)?
                    ))
                    .errhint("You will need to rewrite or cast the expression.")
                    .finish(errloc(3375, "cookDefault"))
                    .map(|()| unreachable!());
            }
        };
    }

    /* Finally, take care of collations in the finished expression. */
    backend_parser_parse_collate::assign_expr_collations(Some(&pstate), &mut expr)?;

    Ok(Some(RichNode::mk_expr(mcx, expr)))
}

/// `replace_domain_constraint_value(pstate, cref)` (typecmds.c:3633) — the
/// `p_pre_columnref_hook` used while parsing a domain CHECK constraint: a
/// single-field reference named `value` is replaced by the
/// `CoerceToDomainValue` prepared by [`domain_add_check_constraint`] (carried in
/// `pstate.p_ref_hook_state`), with the reference's location propagated.
fn replace_domain_constraint_value<'mcx>(
    pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
    cref: &types_nodes::rawnodes::ColumnRef<'mcx>,
) -> PgResult<Option<types_nodes::nodes::NodePtr<'mcx>>> {
    use types_nodes::parsestmt::ParseRefHookState;
    if cref.fields.len() == 1 {
        if let Some(s) = cref.fields[0].as_string() {
            let colname = s.sval.as_str();
            if colname == "value" {
                if let ParseRefHookState::DomainCheckValue(template) = &pstate.p_ref_hook_state {
                    let mut dom_val = *template;
                    /* Propagate location knowledge, if any */
                    dom_val.location = cref.location;
                    let mcx = *pstate.p_rtable.allocator();
                    let node = RichNode::mk_expr(mcx, Expr::CoerceToDomainValue(dom_val));
                    return Ok(Some(mcx::alloc_in(mcx, node)?));
                }
            }
        }
    }
    Ok(None)
}

/// `domainAddCheckConstraint(domainOid, domainNamespace, baseTypeOid, typMod,
/// constr, domainName, constrAddr)` (typecmds.c:3504) — assign/validate the
/// CHECK constraint name, cook `constr->raw_expr` into a boolean expression with
/// a `VALUE` → `CoerceToDomainValue` substitution, and `CreateConstraintEntry`.
/// Returns the cooked `conbin` text and (when `want_constr_addr`) the new
/// constraint's address.
fn domain_add_check_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    domain_oid: Oid,
    domain_namespace: Oid,
    base_type_oid: Oid,
    typ_mod: i32,
    constr_node: RichNode<'mcx>,
    domain_name: &str,
    want_constr_addr: bool,
) -> PgResult<(String, Option<ObjectAddress>)> {
    let Some(constr) = constr_node.as_constraint() else {
        unreachable!("domainAddCheckConstraint: not a Constraint")
    };
    debug_assert!(constr.contype == CONSTR_CHECK);

    /* Assign or validate constraint name. */
    let conname: String = match constr.conname.as_deref() {
        Some(name) => {
            if backend_catalog_pg_constraint::ConstraintNameIsUsed(
                mcx,
                ConstraintCategory::Domain,
                domain_oid,
                name,
            )? {
                return ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(format!(
                        "constraint \"{name}\" for domain \"{domain_name}\" already exists"
                    ))
                    .finish(errloc(3527, "domainAddCheckConstraint"))
                    .map(|()| unreachable!());
            }
            name.to_string()
        }
        None => backend_catalog_pg_constraint::ChooseConstraintName(
            mcx,
            domain_name,
            "",
            "check",
            domain_namespace,
            &[],
        )?,
    };

    /* Convert the raw_expr into an EXPR via a parse state with the VALUE hook. */
    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;

    /*
     * Set up a CoerceToDomainValue to represent the occurrence of VALUE in the
     * expression. It appears to have the type of the base type, not the domain.
     */
    let dom_val = types_nodes::primnodes::CoerceToDomainValue {
        typeId: base_type_oid,
        typeMod: typ_mod,
        collation: backend_utils_cache_lsyscache_seams::get_typcollation::call(base_type_oid)?,
        location: -1,
    };
    pstate.p_pre_columnref_hook = Some(replace_domain_constraint_value);
    pstate.p_ref_hook_state =
        types_nodes::parsestmt::ParseRefHookState::DomainCheckValue(dom_val);

    let raw_expr = constr
        .raw_expr
        .as_deref()
        .expect("domainAddCheckConstraint: CHECK constraint has no raw_expr")
        .clone_in(mcx)?;
    let expr = backend_parser_parse_expr::transformExpr(
        &mut pstate,
        Some(raw_expr),
        ParseExprKind::EXPR_KIND_DOMAIN_CHECK,
    )?
    .expect("domainAddCheckConstraint: CHECK expression cannot be NULL");

    /* Make sure it yields a boolean result. */
    let mut expr = backend_parser_coerce::coerce_to_boolean(mcx, Some(&mut pstate), expr, "CHECK")?;

    /* Fix up collation information. */
    backend_parser_parse_collate::assign_expr_collations(Some(&pstate), &mut expr)?;

    /* Domains don't allow variables. */
    if !pstate.p_rtable.is_empty()
        || backend_optimizer_util_var_seams::contain_var_clause::call(&expr)
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg("cannot use table references in domain check constraint")
            .finish(errloc(3576, "domainAddCheckConstraint"))
            .map(|()| unreachable!());
    }

    /* Convert to string form for storage. */
    let expr_node = RichNode::mk_expr(mcx, expr);
    let ccbin = me::node_to_string::call(mcx, expr_node.clone_in(mcx)?)?
        .as_str()
        .to_string();

    /* Store the constraint in pg_constraint. */
    let skip_validation = match constr_node.node_tag() {
        types_nodes::nodes::ntag::T_Constraint => constr_node.expect_constraint().skip_validation,
        _ => unreachable!(),
    };
    let ccoid = backend_catalog_pg_constraint::CreateConstraintEntry(
        mcx,
        &conname,
        domain_namespace,
        CONSTRAINT_CHECK,
        false, /* isDeferrable */
        false, /* isDeferred */
        true,  /* isEnforced */
        !skip_validation,
        InvalidOid, /* parentConstrId */
        InvalidOid, /* relId */
        &[],
        0,
        0,
        domain_oid, /* domainId */
        InvalidOid, /* indexRelId */
        InvalidOid, /* foreignRelId */
        &[],
        &[],
        &[],
        &[],
        0,
        b' ' as i8,
        b' ' as i8,
        &[],
        0,
        b' ' as i8,
        None,             /* exclOp */
        Some(&expr_node), /* conExpr */
        Some(&ccbin),     /* conBin */
        true,             /* conIsLocal */
        0,                /* conInhCount */
        false,            /* conNoInherit */
        false,            /* conPeriod */
        false,            /* is_internal */
    )?;

    let constr_addr = if want_constr_addr {
        Some(ObjectAddress {
            classId: ConstraintRelationId,
            objectId: ccoid,
            objectSubId: 0,
        })
    } else {
        None
    };

    Ok((ccbin, constr_addr))
}

/// `domainAddNotNullConstraint(domainOid, domainNamespace, baseTypeOid, typMod,
/// constr, domainName, constrAddr)` (typecmds.c:3664) — assign/validate the NOT
/// NULL constraint name and `CreateConstraintEntry(CONSTRAINT_NOTNULL, ...)`.
fn domain_add_not_null_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    domain_oid: Oid,
    domain_namespace: Oid,
    _base_type_oid: Oid,
    _typ_mod: i32,
    constr_node: RichNode<'mcx>,
    domain_name: &str,
    want_constr_addr: bool,
) -> PgResult<Option<ObjectAddress>> {
    let Some(constr) = constr_node.as_constraint() else {
        unreachable!("domainAddNotNullConstraint: not a Constraint")
    };
    debug_assert!(constr.contype == CONSTR_NOTNULL);

    /* Assign or validate constraint name. */
    let conname: String = match constr.conname.as_deref() {
        Some(name) => {
            if backend_catalog_pg_constraint::ConstraintNameIsUsed(
                mcx,
                ConstraintCategory::Domain,
                domain_oid,
                name,
            )? {
                return ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(format!(
                        "constraint \"{name}\" for domain \"{domain_name}\" already exists"
                    ))
                    .finish(errloc(3683, "domainAddNotNullConstraint"))
                    .map(|()| unreachable!());
            }
            name.to_string()
        }
        None => backend_catalog_pg_constraint::ChooseConstraintName(
            mcx,
            domain_name,
            "",
            "not_null",
            domain_namespace,
            &[],
        )?,
    };

    let ccoid = backend_catalog_pg_constraint::CreateConstraintEntry(
        mcx,
        &conname,
        domain_namespace,
        CONSTRAINT_NOTNULL,
        false,
        false,
        true,
        !constr.skip_validation,
        InvalidOid,
        InvalidOid,
        &[],
        0,
        0,
        domain_oid,
        InvalidOid,
        InvalidOid,
        &[],
        &[],
        &[],
        &[],
        0,
        b' ' as i8,
        b' ' as i8,
        &[],
        0,
        b' ' as i8,
        None, /* exclOp */
        None, /* conExpr */
        None, /* conBin */
        true, /* conIsLocal */
        0,    /* conInhCount */
        false,
        false,
        false,
    )?;

    if want_constr_addr {
        Ok(Some(ObjectAddress {
            classId: ConstraintRelationId,
            objectId: ccoid,
            objectSubId: 0,
        }))
    } else {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// checkDomainOwner   (typecmds.c:3484)
// ---------------------------------------------------------------------------

/// `checkDomainOwner(tup)` (typecmds.c:3484) — verify the type is a domain and
/// the current user owns it.
pub fn checkDomainOwner(typ: &types_tuple::pg_type::FormData_pg_type) -> PgResult<()> {
    /* Check that this is actually a domain */
    if typ.typtype != PGT_TYPTYPE_DOMAIN {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("{} is not a domain", format_type_be(typ.oid)?))
            .finish(errloc(3490, "checkDomainOwner"))
            .map(|()| unreachable!());
    }

    /* Permission check: must own type */
    if !backend_catalog_aclchk_seams::object_ownercheck::call(
        TypeRelationId,
        typ.oid,
        get_user_id::call(),
    )? {
        backend_catalog_aclchk_seams::aclcheck_error_type::call(
            types_acl::acl::ACLCHECK_NOT_OWNER,
            typ.oid,
        )?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// AlterDomainDefault   (typecmds.c:2613)
// ---------------------------------------------------------------------------

/// `AlterDomainDefault(names, defaultRaw)` (typecmds.c:2613) — ALTER DOMAIN
/// SET/DROP DEFAULT.
pub fn AlterDomainDefault<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[String],
    default_raw: Option<&RichNode>,
) -> PgResult<ObjectAddress> {
    let domainoid = typename_type_id_from_names(names)?;
    let typ = read_type_form(domainoid)?;

    /* Check it's a domain and check user has permission for ALTER DOMAIN */
    checkDomainOwner(&typ)?;

    let mut default_value: Option<String> = None;
    let mut default_bin: Option<String> = None;

    if let Some(raw) = default_raw {
        /* Cook the colDef->raw_expr into an expression. */
        let default_expr = cook_default(
            mcx,
            raw.clone_in(mcx)?,
            typ.typbasetype,
            typ.typtypmod,
            &type_name_str(&typ),
        )?;

        /*
         * If the expression is just a NULL constant, we treat the command like
         * ALTER ... DROP DEFAULT.
         */
        let is_null_const = match default_expr.as_ref() {
            None => true,
            Some(n) if n.is_const() => n.expect_const().constisnull,
            Some(_) => false,
        };
        if is_null_const {
            /* Default is NULL, drop it */
            default_value = None;
            default_bin = None;
        } else {
            let expr = default_expr.unwrap();
            /* require a valid textual representation (deparse) */
            default_value =
                Some(me::deparse_expression::call(mcx, expr.clone_in(mcx)?)?.as_str().to_string());
            default_bin = Some(me::node_to_string::call(mcx, expr)?.as_str().to_string());
        }
    }
    /* else: ALTER ... DROP DEFAULT — both None */

    /* pg_type write + GenerateTypeDependencies + hook (pg_type owner). */
    backend_catalog_pg_type_seams::set_domain_default::call(domainoid, default_value, default_bin)?;

    let _ = mcx;
    Ok(object_address_set_type(domainoid))
}

// ---------------------------------------------------------------------------
// AlterDomainNotNull   (typecmds.c:2742)
// ---------------------------------------------------------------------------

/// `AlterDomainNotNull(names, notNull)` (typecmds.c:2742) — ALTER DOMAIN
/// SET/DROP NOT NULL.
pub fn AlterDomainNotNull<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[String],
    not_null: bool,
) -> PgResult<ObjectAddress> {
    let domainoid = typename_type_id_from_names(names)?;
    let typ = read_type_form(domainoid)?;

    checkDomainOwner(&typ)?;

    /* Is the domain already set to the desired constraint? */
    if typ.typnotnull == not_null {
        return Ok(InvalidObjectAddress());
    }

    if not_null {
        let constr = make_constraint(mcx, CONSTR_NOTNULL);
        domain_add_not_null_constraint(
            mcx,
            domainoid,
            typ.typnamespace,
            typ.typbasetype,
            typ.typtypmod,
            constr,
            &type_name_str(&typ),
            false,
        )?;
        me::validate_domain_not_null_constraint::call(domainoid)?;
    } else {
        let conoid = backend_catalog_pg_constraint_seams::find_domain_not_null_constraint_oid::call(
            mcx, domainoid,
        )?;
        if !OidIsValid(conoid) {
            return ereport(ERROR)
                .errmsg_internal(format!(
                    "could not find not-null constraint on domain \"{}\"",
                    type_name_str(&typ)
                ))
                .finish(errloc(2796, "AlterDomainNotNull"))
                .map(|()| unreachable!());
        }
        backend_catalog_dependency_seams::perform_deletion::call(
            ConstraintRelationId,
            conoid,
            0,
            DROP_RESTRICT,
            0,
        )?;
    }

    /* Okay to update pg_type row. */
    backend_catalog_pg_type_seams::set_type_not_null::call(domainoid, not_null)?;

    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        TypeRelationId,
        domainoid,
        0,
    )?;

    Ok(object_address_set_type(domainoid))
}

// ---------------------------------------------------------------------------
// AlterDomainDropConstraint   (typecmds.c:2828)
// ---------------------------------------------------------------------------

/// `AlterDomainDropConstraint(names, constrName, behavior, missing_ok)`
/// (typecmds.c:2828).
pub fn AlterDomainDropConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[String],
    constr_name: &str,
    behavior: DropBehavior,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let domainoid = typename_type_id_from_names(names)?;
    let typ = read_type_form(domainoid)?;

    checkDomainOwner(&typ)?;

    /*
     * Find and remove the target constraint (pg_constraint scan + the
     * CONSTRAINT_NOTNULL => typnotnull=false side-effect + performDeletion).
     */
    let (found, was_notnull) = backend_catalog_pg_constraint_seams::drop_domain_constraint::call(
        mcx,
        domainoid,
        constr_name.to_string(),
        behavior,
    )?;

    if was_notnull {
        backend_catalog_pg_type_seams::set_type_not_null::call(domainoid, false)?;
    }

    if !found {
        if !missing_ok {
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "constraint \"{constr_name}\" of domain \"{}\" does not exist",
                    type_name_str(&typ)
                ))
                .finish(errloc(2906, "AlterDomainDropConstraint"))
                .map(|()| unreachable!());
        } else {
            ereport(WARNING)
                .errmsg(format!(
                    "constraint \"{constr_name}\" of domain \"{}\" does not exist, skipping",
                    type_name_str(&typ)
                ))
                .finish(errloc(2910, "AlterDomainDropConstraint"))?;
        }
    }

    /*
     * Send out an sinval message for the domain (CacheInvalidateHeapTuple), so
     * dependent plans get rebuilt; this command doesn't change the pg_type row.
     */
    backend_utils_cache_inval_seams::cache_invalidate_heap_tuple::call(TypeRelationId, domainoid)?;

    Ok(object_address_set_type(domainoid))
}

// ---------------------------------------------------------------------------
// AlterDomainAddConstraint   (typecmds.c:2934)
// ---------------------------------------------------------------------------

/// `AlterDomainAddConstraint(names, newConstraint, constrAddr)` (typecmds.c:2934).
pub fn AlterDomainAddConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[String],
    new_constraint: &RichNode,
    want_constr_addr: bool,
) -> PgResult<(ObjectAddress, Option<ObjectAddress>)> {
    let domainoid = typename_type_id_from_names(names)?;
    let typ = read_type_form(domainoid)?;

    checkDomainOwner(&typ)?;

    let constr = match new_constraint.as_constraint() {
        Some(c) => c,
        None => {
            return ereport(ERROR)
                .errmsg_internal(format!(
                    "unrecognized node type: {}",
                    "AlterDomainAddConstraint: not a Constraint"
                ))
                .finish(errloc(2963, "AlterDomainAddConstraint"))
                .map(|()| unreachable!());
        }
    };

    /* enforced by parser */
    debug_assert!(constr.contype == CONSTR_CHECK || constr.contype == CONSTR_NOTNULL);

    let mut constr_addr: Option<ObjectAddress> = None;

    if constr.contype == CONSTR_CHECK {
        let (ccbin, addr) = domain_add_check_constraint(
            mcx,
            domainoid,
            typ.typnamespace,
            typ.typbasetype,
            typ.typtypmod,
            new_constraint.clone_in(mcx)?,
            &type_name_str(&typ),
            want_constr_addr,
        )?;
        constr_addr = addr;

        if !constr.skip_validation {
            me::validate_domain_check_constraint::call(domainoid, ccbin)?;
        }

        backend_utils_cache_inval_seams::cache_invalidate_heap_tuple::call(
            TypeRelationId,
            domainoid,
        )?;
    } else if constr.contype == CONSTR_NOTNULL {
        /* Is the domain already set NOT NULL? */
        if typ.typnotnull {
            return Ok((InvalidObjectAddress(), None));
        }
        constr_addr = domain_add_not_null_constraint(
            mcx,
            domainoid,
            typ.typnamespace,
            typ.typbasetype,
            typ.typtypmod,
            new_constraint.clone_in(mcx)?,
            &type_name_str(&typ),
            want_constr_addr,
        )?;

        if !constr.skip_validation {
            me::validate_domain_not_null_constraint::call(domainoid)?;
        }

        backend_catalog_pg_type_seams::set_type_not_null::call(domainoid, true)?;
    }

    let _ = mcx;
    Ok((object_address_set_type(domainoid), constr_addr))
}

// ---------------------------------------------------------------------------
// AlterDomainValidateConstraint   (typecmds.c:3031)
// ---------------------------------------------------------------------------

/// `AlterDomainValidateConstraint(names, constrName)` (typecmds.c:3031).
pub fn AlterDomainValidateConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[String],
    constr_name: &str,
) -> PgResult<ObjectAddress> {
    let domainoid = typename_type_id_from_names(names)?;
    let typ = read_type_form(domainoid)?;

    checkDomainOwner(&typ)?;

    /* Find and check the target CHECK constraint; get its cooked conbin + oid. */
    let (conoid, conbin) = backend_catalog_pg_constraint_seams::find_domain_check_constraint::call(
        mcx,
        domainoid,
        constr_name.to_string(),
    )?;

    me::validate_domain_check_constraint::call(domainoid, conbin)?;

    /* Now update the catalog (convalidated = true). */
    backend_catalog_pg_constraint_seams::set_constraint_validated::call(mcx, conoid)?;

    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        ConstraintRelationId,
        conoid,
        0,
    )?;

    Ok(object_address_set_type(domainoid))
}

// ---------------------------------------------------------------------------
// DefineDomain   (typecmds.c:697)
// ---------------------------------------------------------------------------

/// `DefineDomain(pstate, stmt)` (typecmds.c:697) — CREATE DOMAIN.
///
/// `domainname` is the qualified domain name; `type_name` is the base
/// `TypeName`; `coll_clause` the optional COLLATE clause name list; `constraints`
/// the list of `Constraint` nodes.
pub fn DefineDomain<'mcx>(
    mcx: Mcx<'mcx>,
    domainname: &[String],
    type_name: &TypeName,
    coll_clause: Option<&[String]>,
    constraints: &[RichNode],
) -> PgResult<ObjectAddress> {
    /* Convert list of names to a name and namespace */
    let names_nl = as_namelist(domainname);
    let (domainNamespace, domain_name_owned) = QualifiedNameGetCreationNamespace(mcx, &names_nl)?;
    let domainName = domain_name_owned.to_string();

    /* Check we have creation rights in target namespace */
    let aclresult = object_aclcheck::call(
        NamespaceRelationId,
        domainNamespace,
        get_user_id::call(),
        ACL_CREATE as AclMode,
    )?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error::call(
            aclresult,
            OBJECT_SCHEMA,
            get_namespace_name_seam(mcx, domainNamespace)?,
        )?;
    }

    /*
     * Check for collision with an existing type name (autogenerated array can
     * be renamed out of the way).
     */
    let old_type_oid = get_type_oid::call(&domainName, domainNamespace)?;
    if OidIsValid(old_type_oid) && !moveArrayTypeName(old_type_oid, &domainName, domainNamespace)? {
        return ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("type \"{domainName}\" already exists"))
            .finish(errloc(757, "DefineDomain"))
            .map(|()| unreachable!());
    }

    /* Look up the base type. */
    let basetypeoid = typenameTypeId(type_name)?;
    let baseType = read_type_form(basetypeoid)?;
    let basetypeMod = baseType.typtypmod;

    /*
     * Base type must be a plain base type, a composite type, another domain, an
     * enum or a range type.
     */
    let typtype = baseType.typtype;
    if typtype != TYPTYPE_BASE
        && typtype != TYPTYPE_COMPOSITE
        && typtype != PGT_TYPTYPE_DOMAIN
        && typtype != TYPTYPE_ENUM
        && typtype != TYPTYPE_RANGE
        && typtype != TYPTYPE_MULTIRANGE
    {
        return ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "\"{}\" is not a valid base type for a domain",
                TypeNameToString(mcx, type_name)?
            ))
            .finish(errloc(783, "DefineDomain"))
            .map(|()| unreachable!());
    }

    let aclresult = object_aclcheck::call(
        TypeRelationId,
        basetypeoid,
        get_user_id::call(),
        ACL_USAGE as AclMode,
    )?;
    if aclresult != ACLCHECK_OK {
        backend_catalog_aclchk_seams::aclcheck_error_type::call(aclresult, basetypeoid)?;
    }

    /* Identify the collation if any */
    let baseColl = baseType.typcollation;
    let domaincoll = if let Some(coll) = coll_clause {
        backend_catalog_namespace::get_collation_oid(mcx, &as_namelist(coll), false)?
    } else {
        baseColl
    };

    /* Complain if COLLATE is applied to an uncollatable type */
    if OidIsValid(domaincoll) && !OidIsValid(baseColl) {
        return ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "collations are not supported by type {}",
                format_type_be(basetypeoid)?
            ))
            .finish(errloc(810, "DefineDomain"))
            .map(|()| unreachable!());
    }

    /* Inherited properties from base type. */
    let byValue = baseType.typbyval;
    let mut alignment = baseType.typalign;
    let storage = baseType.typstorage;
    let internalLength = baseType.typlen;
    let category = baseType.typcategory;
    let delimiter = baseType.typdelim;

    /* I/O Functions */
    let inputProcedure: Oid = F_DOMAIN_IN;
    let outputProcedure: Oid = baseType.typoutput;
    let receiveProcedure: Oid = F_DOMAIN_RECV;
    let sendProcedure: Oid = baseType.typsend;
    let analyzeProcedure: Oid = baseType.typanalyze;

    /* Inherited default value / binary value (via pg_type_default projection). */
    let base_default = backend_utils_cache_syscache_seams::pg_type_default::call(mcx, basetypeoid)?;
    let mut defaultValue: Option<String> =
        base_default.as_ref().and_then(|d| d.typdefault.clone());
    let mut defaultValueBin: Option<String> =
        base_default.as_ref().and_then(|d| d.typdefaultbin.clone());

    /* Run through constraints manually (the validation/typNotNull loop). */
    let mut saw_default = false;
    let mut typNotNull = false;
    let mut nullDefined = false;
    let typNDims = type_name.arrayBounds.len() as i32;

    for node in constraints {
        let constr = match node.as_constraint() {
            Some(c) => c,
            None => {
                return ereport(ERROR)
                    .errmsg_internal("unrecognized node type in CREATE DOMAIN constraints")
                    .finish(errloc(872, "DefineDomain"))
                    .map(|()| unreachable!());
            }
        };
        match constr.contype {
            CONSTR_DEFAULT => {
                if saw_default {
                    return ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg("multiple default expressions")
                        .finish(errloc(885, "DefineDomain"))
                        .map(|()| unreachable!());
                }
                saw_default = true;

                if let Some(raw) = constr.raw_expr.as_deref() {
                    /* Cook the constr->raw_expr into an expression. */
                    let default_expr = cook_default(
                        mcx,
                        raw.clone_in(mcx)?,
                        basetypeoid,
                        basetypeMod,
                        &domainName,
                    )?;
                    let is_null_const = match default_expr.as_ref() {
                        None => true,
                        Some(n) if n.is_const() => n.expect_const().constisnull,
                        Some(_) => false,
                    };
                    if is_null_const {
                        defaultValue = None;
                        defaultValueBin = None;
                    } else {
                        let expr = default_expr.unwrap();
                        defaultValue = Some(
                            me::deparse_expression::call(mcx, expr.clone_in(mcx)?)?
                                .as_str()
                                .to_string(),
                        );
                        defaultValueBin =
                            Some(me::node_to_string::call(mcx, expr)?.as_str().to_string());
                    }
                } else {
                    defaultValue = None;
                    defaultValueBin = None;
                }
            }
            CONSTR_NOTNULL => {
                if nullDefined {
                    if !typNotNull {
                        return ereport(ERROR)
                            .errcode(ERRCODE_SYNTAX_ERROR)
                            .errmsg("conflicting NULL/NOT NULL constraints")
                            .finish(errloc(948, "DefineDomain"))
                            .map(|()| unreachable!());
                    }
                    return ereport(ERROR)
                        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                        .errmsg("redundant NOT NULL constraint definition")
                        .finish(errloc(953, "DefineDomain"))
                        .map(|()| unreachable!());
                }
                if constr.is_no_inherit {
                    return ereport(ERROR)
                        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                        .errmsg("not-null constraints for domains cannot be marked NO INHERIT")
                        .finish(errloc(959, "DefineDomain"))
                        .map(|()| unreachable!());
                }
                typNotNull = true;
                nullDefined = true;
            }
            CONSTR_NULL => {
                if nullDefined && typNotNull {
                    return ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg("conflicting NULL/NOT NULL constraints")
                        .finish(errloc(969, "DefineDomain"))
                        .map(|()| unreachable!());
                }
                typNotNull = false;
                nullDefined = true;
            }
            CONSTR_CHECK => {
                /* Handled after domain creation; here only reject NO INHERIT. */
                if constr.is_no_inherit {
                    return ereport(ERROR)
                        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                        .errmsg("check constraints for domains cannot be marked NO INHERIT")
                        .finish(errloc(986, "DefineDomain"))
                        .map(|()| unreachable!());
                }
            }
            other => {
                /* All other constraint types are errors for domains. */
                return Err(domain_constraint_kind_error(other));
            }
        }
    }

    /* Allocate OID for array type */
    let domainArrayOid = AssignTypeArrayOid()?;
    let owner = get_user_id::call();

    /* Have TypeCreate do all the real work. */
    let address = TypeCreate(TypeCreateParams {
        new_type_oid: InvalidOid,
        type_name: domainName.clone(),
        type_namespace: domainNamespace,
        relation_oid: InvalidOid,
        relation_kind: 0,
        owner_id: owner,
        internal_size: internalLength,
        type_type: PGT_TYPTYPE_DOMAIN,
        type_category: category,
        type_preferred: false,
        type_delim: delimiter,
        input_procedure: inputProcedure,
        output_procedure: outputProcedure,
        receive_procedure: receiveProcedure,
        send_procedure: sendProcedure,
        typmodin_procedure: InvalidOid,
        typmodout_procedure: InvalidOid,
        analyze_procedure: analyzeProcedure,
        subscript_procedure: InvalidOid,
        element_type: InvalidOid,
        is_implicit_array: false,
        array_type: domainArrayOid,
        base_type: basetypeoid,
        default_type_value: defaultValue,
        default_type_bin: defaultValueBin,
        passed_by_value: byValue,
        alignment,
        storage,
        type_mod: basetypeMod,
        typ_ndims: typNDims,
        type_not_null: typNotNull,
        type_collation: domaincoll,
    })?;

    /* Create the array type that goes with it. */
    let domainArrayName = makeArrayTypeName(&domainName, domainNamespace)?;
    alignment = if alignment == TYPALIGN_DOUBLE {
        TYPALIGN_DOUBLE
    } else {
        TYPALIGN_INT
    };

    TypeCreate(TypeCreateParams {
        new_type_oid: domainArrayOid,
        type_name: domainArrayName,
        type_namespace: domainNamespace,
        relation_oid: InvalidOid,
        relation_kind: 0,
        owner_id: owner,
        internal_size: -1,
        type_type: TYPTYPE_BASE,
        type_category: TYPCATEGORY_ARRAY,
        type_preferred: false,
        type_delim: delimiter,
        input_procedure: F_ARRAY_IN,
        output_procedure: F_ARRAY_OUT,
        receive_procedure: F_ARRAY_RECV,
        send_procedure: F_ARRAY_SEND,
        typmodin_procedure: InvalidOid,
        typmodout_procedure: InvalidOid,
        analyze_procedure: F_ARRAY_TYPANALYZE,
        subscript_procedure: F_ARRAY_SUBSCRIPT_HANDLER,
        element_type: address.objectId,
        is_implicit_array: true,
        array_type: InvalidOid,
        base_type: InvalidOid,
        default_type_value: None,
        default_type_bin: None,
        passed_by_value: false,
        alignment,
        storage: TYPSTORAGE_EXTENDED,
        type_mod: -1,
        typ_ndims: 0,
        type_not_null: false,
        type_collation: domaincoll,
    })?;

    /* Process constraints which refer to the domain ID returned by TypeCreate. */
    for node in constraints {
        let Some(constr) = node.as_constraint() else {
            unreachable!("checked above")
        };
        match constr.contype {
            CONSTR_CHECK => {
                domain_add_check_constraint(
                    mcx,
                    address.objectId,
                    domainNamespace,
                    basetypeoid,
                    basetypeMod,
                    node.clone_in(mcx)?,
                    &domainName,
                    false,
                )?;
            }
            CONSTR_NOTNULL => {
                domain_add_not_null_constraint(
                    mcx,
                    address.objectId,
                    domainNamespace,
                    basetypeoid,
                    basetypeMod,
                    node.clone_in(mcx)?,
                    &domainName,
                    false,
                )?;
            }
            _ => {}
        }
        /* CCI so we can detect duplicate constraint names */
        backend_access_transam_xact_seams::command_counter_increment::call()?;
    }

    Ok(address)
}

/// The `default:` error arms of the `DefineDomain` constraint switch
/// (typecmds.c:993-1045) — the unsupported constraint kinds for a domain.
fn domain_constraint_kind_error(contype: ConstrType) -> PgError {
    use types_nodes::ddlnodes::{
        CONSTR_ATTR_DEFERRABLE, CONSTR_ATTR_DEFERRED, CONSTR_ATTR_ENFORCED, CONSTR_ATTR_IMMEDIATE,
        CONSTR_ATTR_NOT_DEFERRABLE, CONSTR_ATTR_NOT_ENFORCED, CONSTR_EXCLUSION, CONSTR_FOREIGN,
        CONSTR_GENERATED, CONSTR_IDENTITY, CONSTR_PRIMARY, CONSTR_UNIQUE,
    };
    let (code, msg) = if contype == CONSTR_UNIQUE {
        (ERRCODE_SYNTAX_ERROR, "unique constraints not possible for domains")
    } else if contype == CONSTR_PRIMARY {
        (ERRCODE_SYNTAX_ERROR, "primary key constraints not possible for domains")
    } else if contype == CONSTR_EXCLUSION {
        (ERRCODE_SYNTAX_ERROR, "exclusion constraints not possible for domains")
    } else if contype == CONSTR_FOREIGN {
        (ERRCODE_SYNTAX_ERROR, "foreign key constraints not possible for domains")
    } else if contype == CONSTR_ATTR_DEFERRABLE
        || contype == CONSTR_ATTR_NOT_DEFERRABLE
        || contype == CONSTR_ATTR_DEFERRED
        || contype == CONSTR_ATTR_IMMEDIATE
    {
        (
            ERRCODE_FEATURE_NOT_SUPPORTED,
            "specifying constraint deferrability not supported for domains",
        )
    } else if contype == CONSTR_GENERATED || contype == CONSTR_IDENTITY {
        (ERRCODE_FEATURE_NOT_SUPPORTED, "specifying GENERATED not supported for domains")
    } else if contype == CONSTR_ATTR_ENFORCED || contype == CONSTR_ATTR_NOT_ENFORCED {
        (
            ERRCODE_INVALID_OBJECT_DEFINITION,
            "specifying constraint enforceability not supported for domains",
        )
    } else {
        (ERRCODE_SYNTAX_ERROR, "unsupported constraint for domain")
    };
    ereport(ERROR)
        .errcode(code)
        .errmsg(msg)
        .finish(errloc(0, "DefineDomain"))
        .expect_err("ereport(ERROR) always yields an Err")
}

/// `makeNode(Constraint); constr->contype = ...; constr->initially_valid =
/// true; constr->location = -1;` — the bare Constraint node `AlterDomainNotNull`
/// builds for the SET NOT NULL path (typecmds.c:2778).
fn make_constraint<'mcx>(mcx: Mcx<'mcx>, contype: ConstrType) -> RichNode<'mcx> {
    RichNode::Constraint(types_nodes::ddlnodes::Constraint {
        contype,
        conname: None,
        deferrable: false,
        initdeferred: false,
        is_enforced: true,
        skip_validation: false,
        initially_valid: true,
        is_no_inherit: false,
        raw_expr: None,
        cooked_expr: None,
        generated_when: 0,
        generated_kind: 0,
        nulls_not_distinct: false,
        keys: mcx::PgVec::new_in(mcx),
        without_overlaps: false,
        including: mcx::PgVec::new_in(mcx),
        exclusions: mcx::PgVec::new_in(mcx),
        options: mcx::PgVec::new_in(mcx),
        indexname: None,
        indexspace: None,
        reset_default_tblspc: false,
        access_method: None,
        where_clause: None,
        pktable: None,
        fk_attrs: mcx::PgVec::new_in(mcx),
        pk_attrs: mcx::PgVec::new_in(mcx),
        fk_with_period: false,
        pk_with_period: false,
        fk_matchtype: 0,
        fk_upd_action: 0,
        fk_del_action: 0,
        fk_del_set_cols: mcx::PgVec::new_in(mcx),
        old_conpfeqop: mcx::PgVec::new_in(mcx),
        old_pktable_oid: InvalidOid,
        location: -1,
    })
}

// ===========================================================================
// F4 — RenameType / AlterTypeOwner / AlterTypeNamespace / AlterType
// ===========================================================================

/// `RenameType(stmt)` (typecmds.c:3739).
pub fn RenameType<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[String],
    new_type_name: &str,
    rename_type: ObjectType,
) -> PgResult<ObjectAddress> {
    let typeOid = typename_type_id_from_names(names)?;
    let typ = read_type_form(typeOid)?;

    /* check permissions on type */
    if !backend_catalog_aclchk_seams::object_ownercheck::call(
        TypeRelationId,
        typeOid,
        get_user_id::call(),
    )? {
        backend_catalog_aclchk_seams::aclcheck_error_type::call(
            types_acl::acl::ACLCHECK_NOT_OWNER,
            typeOid,
        )?;
    }

    /* ALTER DOMAIN used on a non-domain? */
    if rename_type == OBJECT_DOMAIN && typ.typtype != PGT_TYPTYPE_DOMAIN {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("{} is not a domain", format_type_be(typeOid)?))
            .finish(errloc(3771, "RenameType"))
            .map(|()| unreachable!());
    }

    /* free-standing composite type, not a table's rowtype */
    if typ.typtype == TYPTYPE_COMPOSITE
        && get_rel_relkind::call(typ.typrelid)? != RELKIND_COMPOSITE_TYPE
    {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("{} is a table's row type", format_type_be(typeOid)?))
            .errhint("Use ALTER TABLE instead.")
            .finish(errloc(3783, "RenameType"))
            .map(|()| unreachable!());
    }

    /* don't allow direct alteration of array types */
    if is_true_array_type(&typ) {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("cannot alter array type {}", format_type_be(typeOid)?))
            .errhint(format!(
                "You can alter type {}, which will alter the array type as well.",
                format_type_be(typ.typelem)?
            ))
            .finish(errloc(3793, "RenameType"))
            .map(|()| unreachable!());
    }

    /*
     * If type is composite, rename associated pg_class entry too
     * (RenameRelationInternal calls RenameTypeInternal automatically); else
     * RenameTypeInternal directly.
     */
    if typ.typtype == TYPTYPE_COMPOSITE {
        backend_commands_tablecmds_seams::rename_relation_internal::call(
            mcx,
            typ.typrelid,
            new_type_name,
            false,
            false,
        )?;
    } else {
        backend_catalog_pg_type::RenameTypeInternal(typeOid, new_type_name, typ.typnamespace)?;
    }

    Ok(object_address_set_type(typeOid))
}

/// `AlterTypeOwner(names, newOwnerId, objecttype)` (typecmds.c:3820).
pub fn AlterTypeOwner<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[String],
    new_owner_id: Oid,
    objecttype: ObjectType,
) -> PgResult<ObjectAddress> {
    /*
     * Use LookupTypeName here so that shell types can be processed
     * (`lookup_type_name_oid_from_names` is the shell-allowing OID resolver,
     * unlike `typenameTypeId` which rejects a shell type).
     */
    let typeOid = backend_parser_parse_type_seams::lookup_type_name_oid_from_names::call(
        &type_name_from_namelist(names),
    )?;
    let typ = read_type_form(typeOid)?;

    /* Don't allow ALTER DOMAIN on a type */
    if objecttype == OBJECT_DOMAIN && typ.typtype != PGT_TYPTYPE_DOMAIN {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("{} is not a domain", format_type_be(typeOid)?))
            .finish(errloc(3856, "AlterTypeOwner"))
            .map(|()| unreachable!());
    }

    /* free-standing composite type, not a table's rowtype */
    if typ.typtype == TYPTYPE_COMPOSITE
        && get_rel_relkind::call(typ.typrelid)? != RELKIND_COMPOSITE_TYPE
    {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("{} is a table's row type", format_type_be(typeOid)?))
            .errhint("Use ALTER TABLE instead.")
            .finish(errloc(3868, "AlterTypeOwner"))
            .map(|()| unreachable!());
    }

    /* don't allow direct alteration of array types */
    if is_true_array_type(&typ) {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("cannot alter array type {}", format_type_be(typeOid)?))
            .errhint(format!(
                "You can alter type {}, which will alter the array type as well.",
                format_type_be(typ.typelem)?
            ))
            .finish(errloc(3878, "AlterTypeOwner"))
            .map(|()| unreachable!());
    }

    /* don't allow direct alteration of multirange types */
    if typ.typtype == TYPTYPE_MULTIRANGE {
        let rangetype = get_multirange_range::call(typeOid)?;
        let mut b = ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("cannot alter multirange type {}", format_type_be(typeOid)?));
        if OidIsValid(rangetype) {
            b = b.errhint(format!(
                "You can alter type {}, which will alter the multirange type as well.",
                format_type_be(rangetype)?
            ));
        }
        return b
            .finish(errloc(3889, "AlterTypeOwner"))
            .map(|()| unreachable!());
    }

    /* If the new owner is the same as the existing owner, succeed (dump). */
    if typ.typowner != new_owner_id {
        /* Superusers can always do it */
        if !superuser::call(mcx)? {
            /* Otherwise, must be owner of the existing object */
            if !backend_catalog_aclchk_seams::object_ownercheck::call(
                TypeRelationId,
                typ.oid,
                get_user_id::call(),
            )? {
                backend_catalog_aclchk_seams::aclcheck_error_type::call(
                    types_acl::acl::ACLCHECK_NOT_OWNER,
                    typ.oid,
                )?;
            }

            /* Must be able to become new owner */
            backend_utils_adt_acl_seams::check_can_set_role::call(get_user_id::call(), new_owner_id)?;

            /* New owner must have CREATE privilege on namespace */
            let aclresult = object_aclcheck::call(
                NamespaceRelationId,
                typ.typnamespace,
                new_owner_id,
                ACL_CREATE as AclMode,
            )?;
            if aclresult != ACLCHECK_OK {
                aclcheck_error::call(
                    aclresult,
                    OBJECT_SCHEMA,
                    get_namespace_name_seam(mcx, typ.typnamespace)?,
                )?;
            }
        }

        AlterTypeOwner_oid(typeOid, new_owner_id, true)?;
    }

    Ok(object_address_set_type(typeOid))
}

/// `AlterTypeOwner_oid(typeOid, newOwnerId, hasDependEntry)` (typecmds.c:3945) —
/// the inward seam body (installed in `init_seams`).
pub fn AlterTypeOwner_oid(
    type_oid: Oid,
    new_owner_id: Oid,
    has_depend_entry: bool,
) -> PgResult<()> {
    let typ = read_type_form(type_oid)?;

    /*
     * If composite, ATExecChangeOwner fixes the pg_class entry and calls back to
     * AlterTypeOwnerInternal; else AlterTypeOwnerInternal directly.
     */
    if typ.typtype == TYPTYPE_COMPOSITE {
        backend_commands_tablecmds_seams::at_exec_change_owner::call(
            typ.typrelid,
            new_owner_id,
            true,
            AccessExclusiveLock,
        )?;
    } else {
        AlterTypeOwnerInternal(type_oid, new_owner_id)?;
    }

    /* Update owner dependency reference */
    if has_depend_entry {
        backend_catalog_pg_shdepend_seams::changeDependencyOnOwner::call(
            TypeRelationId,
            type_oid,
            new_owner_id,
        )?;
    }

    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        TypeRelationId,
        type_oid,
        0,
    )?;

    Ok(())
}

/// `AlterTypeOwnerInternal(typeOid, newOwnerId)` (typecmds.c:3985) — bare owner
/// change; recurses to array + (range) multirange types. The single-row
/// typowner/typacl write is the pg_type owner seam `set_type_owner`.
pub fn AlterTypeOwnerInternal(type_oid: Oid, new_owner_id: Oid) -> PgResult<()> {
    /* Owner-side write: typowner + (if typacl non-null) aclnewowner. */
    backend_catalog_pg_type_seams::set_type_owner::call(type_oid, new_owner_id)?;

    let typ = read_type_form(type_oid)?;

    /* If it has an array type, update that too */
    if OidIsValid(typ.typarray) {
        AlterTypeOwnerInternal(typ.typarray, new_owner_id)?;
    }

    /* If it is a range type, update the associated multirange too */
    if typ.typtype == TYPTYPE_RANGE {
        let multirange_typeid = get_range_multirange::call(type_oid)?;
        if !OidIsValid(multirange_typeid) {
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "could not find multirange type for data type {}",
                    format_type_be(type_oid)?
                ))
                .finish(errloc(4041, "AlterTypeOwnerInternal"))
                .map(|()| unreachable!());
        }
        AlterTypeOwnerInternal(multirange_typeid, new_owner_id)?;
    }

    Ok(())
}

/// `AlterTypeNamespace(names, newschema, objecttype, oldschema)`
/// (typecmds.c:4053).
pub fn AlterTypeNamespace<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[String],
    newschema: &str,
    objecttype: ObjectType,
    oldschema: Option<&mut Oid>,
) -> PgResult<ObjectAddress> {
    let typeOid = typename_type_id_from_names(names)?;

    /* Don't allow ALTER DOMAIN on a non-domain type */
    if objecttype == OBJECT_DOMAIN && get_typtype::call(typeOid)? as i8 != PGT_TYPTYPE_DOMAIN {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("{} is not a domain", format_type_be(typeOid)?))
            .finish(errloc(4072, "AlterTypeNamespace"))
            .map(|()| unreachable!());
    }

    /* get schema OID and check its permissions */
    let nspOid = backend_catalog_namespace_seams::lookup_creation_namespace::call(newschema)?;

    let mut objsMoved = backend_catalog_dependency_seams::new_object_addresses::call()?;
    let oldNspOid = AlterTypeNamespace_oid(typeOid, nspOid, false, &mut objsMoved)?;
    backend_catalog_dependency_seams::free_object_addresses::call(objsMoved)?;

    if let Some(slot) = oldschema {
        *slot = oldNspOid;
    }

    let _ = mcx;
    Ok(object_address_set_type(typeOid))
}

/// `AlterTypeNamespace_oid(typeOid, nspOid, ignoreDependent, objsMoved)`
/// (typecmds.c:4102).
pub fn AlterTypeNamespace_oid(
    type_oid: Oid,
    nsp_oid: Oid,
    ignore_dependent: bool,
    objs_moved: &mut types_catalog::catalog_dependency::ObjectAddresses,
) -> PgResult<Oid> {
    /* check permissions on type */
    if !backend_catalog_aclchk_seams::object_ownercheck::call(
        TypeRelationId,
        type_oid,
        get_user_id::call(),
    )? {
        backend_catalog_aclchk_seams::aclcheck_error_type::call(
            types_acl::acl::ACLCHECK_NOT_OWNER,
            type_oid,
        )?;
    }

    /* don't allow direct alteration of array types */
    let elemOid = get_element_type_seam(type_oid)?;
    if OidIsValid(elemOid) && get_array_type_seam(elemOid)? == type_oid {
        if ignore_dependent {
            return Ok(InvalidOid);
        }
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("cannot alter array type {}", format_type_be(type_oid)?))
            .errhint(format!(
                "You can alter type {}, which will alter the array type as well.",
                format_type_be(elemOid)?
            ))
            .finish(errloc(4123, "AlterTypeNamespace_oid"))
            .map(|()| unreachable!());
    }

    AlterTypeNamespaceInternal(type_oid, nsp_oid, false, ignore_dependent, true, objs_moved)
}

/// `AlterTypeNamespaceInternal(typeOid, nspOid, isImplicitArray,
/// ignoreDependent, errorOnTableType, objsMoved)` (typecmds.c:4154).
pub fn AlterTypeNamespaceInternal(
    type_oid: Oid,
    nsp_oid: Oid,
    is_implicit_array: bool,
    ignore_dependent: bool,
    error_on_table_type: bool,
    objs_moved: &mut types_catalog::catalog_dependency::ObjectAddresses,
) -> PgResult<Oid> {
    let thisobj = object_address_set_type(type_oid);

    /* Make sure we haven't moved this object previously. */
    if backend_catalog_dependency_seams::object_address_present::call(thisobj, &*objs_moved)? {
        return Ok(InvalidOid);
    }

    let typform = read_type_form(type_oid)?;
    let oldNspOid = typform.typnamespace;
    let arrayOid = typform.typarray;

    /* If the type is already there, skip these next few checks. */
    if oldNspOid != nsp_oid {
        /* common checks on switching namespaces */
        backend_catalog_namespace_seams::check_set_namespace::call(oldNspOid, nsp_oid)?;

        /* check for duplicate name */
        if backend_utils_cache_syscache_seams::type_exists::call(
            &type_name_str(&typform),
            nsp_oid,
        )? {
            return ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!(
                    "type \"{}\" already exists in schema \"{}\"",
                    type_name_str(&typform),
                    get_namespace_name_str(nsp_oid)?
                ))
                .finish(errloc(4201, "AlterTypeNamespaceInternal"))
                .map(|()| unreachable!());
        }
    }

    /* Detect composite type (but not a table rowtype). */
    let isCompositeType = typform.typtype == TYPTYPE_COMPOSITE
        && get_rel_relkind::call(typform.typrelid)? == RELKIND_COMPOSITE_TYPE;

    /* Enforce not-table-type if requested. */
    if typform.typtype == TYPTYPE_COMPOSITE && !isCompositeType {
        if ignore_dependent {
            return Ok(InvalidOid);
        }
        if error_on_table_type {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("{} is a table's row type", format_type_be(type_oid)?))
                .errhint("Use ALTER TABLE instead.")
                .finish(errloc(4225, "AlterTypeNamespaceInternal"))
                .map(|()| unreachable!());
        }
    }

    if oldNspOid != nsp_oid {
        /* OK, modify the pg_type row (typnamespace). */
        backend_catalog_pg_type_seams::set_type_namespace::call(type_oid, nsp_oid)?;
    }

    /*
     * Composite types have pg_class entries; modify the pg_class tuple too +
     * move associated constraints.
     */
    if isCompositeType {
        backend_commands_tablecmds_seams::alter_relation_namespace_internal::call(
            typform.typrelid,
            oldNspOid,
            nsp_oid,
            false,
            objs_moved,
        )?;
        backend_catalog_pg_constraint_seams::alter_constraint_namespaces::call(
            type_relation_mcx(),
            typform.typrelid,
            oldNspOid,
            nsp_oid,
            false,
            objs_moved,
        )?;
    } else if typform.typtype == PGT_TYPTYPE_DOMAIN {
        /* If it's a domain, it might have constraints. */
        backend_catalog_pg_constraint_seams::alter_constraint_namespaces::call(
            type_relation_mcx(),
            type_oid,
            oldNspOid,
            nsp_oid,
            true,
            objs_moved,
        )?;
    }

    /*
     * Update dependency on schema, if any --- a table rowtype has not got one,
     * and neither does an implicit array.
     */
    if oldNspOid != nsp_oid
        && (isCompositeType || typform.typtype != TYPTYPE_COMPOSITE)
        && !is_implicit_array
    {
        let scratch = MemoryContext::new("changeDependencyFor");
        if backend_catalog_pg_depend_seams::changeDependencyFor::call(
            scratch.mcx(),
            TypeRelationId,
            type_oid,
            NamespaceRelationId,
            oldNspOid,
            nsp_oid,
        )? != 1
        {
            return ereport(ERROR)
                .errmsg_internal(format!(
                    "could not change schema dependency for type \"{}\"",
                    format_type_be(type_oid)?
                ))
                .finish(errloc(4280, "AlterTypeNamespaceInternal"))
                .map(|()| unreachable!());
        }
    }

    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        TypeRelationId,
        type_oid,
        0,
    )?;

    backend_catalog_dependency_seams::add_exact_object_address::call(thisobj, objs_moved)?;

    /* Recursively alter the associated array type, if any */
    if OidIsValid(arrayOid) {
        AlterTypeNamespaceInternal(arrayOid, nsp_oid, true, false, true, objs_moved)?;
    }

    Ok(oldNspOid)
}

/// A fresh scratch `Mcx` for the constraint-namespace seam (it takes a `Mcx`
/// arg the owner ignores in favour of its own context).
fn type_relation_mcx<'a>() -> Mcx<'a> {
    // SAFETY-free: the owner's installed body opens its own MemoryContext and
    // does not retain this Mcx; we forge a leaked scratch context's mcx.
    let ctx = Box::leak(Box::new(MemoryContext::new("alter_constraint_namespaces")));
    ctx.mcx()
}

/// `RELATION_RELATION_ID` keepalive (the C composite path opens pg_class).
const _: Oid = RelationRelationId;

// ---------------------------------------------------------------------------
// AlterType / AlterTypeRecurse   (typecmds.c:4310 / 4561)
// ---------------------------------------------------------------------------

/// `AlterType(stmt)` (typecmds.c:4310) — ALTER TYPE <type> SET (option = ...).
///
/// `type_name` is the qualified type name; `options` the `DefElem` option list.
pub fn AlterType<'mcx>(
    mcx: Mcx<'mcx>,
    type_name: &[String],
    options: &[Node],
) -> PgResult<ObjectAddress> {
    let typeOid = typename_type_id_from_names(type_name)?;
    let typForm = read_type_form(typeOid)?;

    let mut atparams = types_catalog::pg_type::TypeAttrUpdate::default();
    let mut requireSuper = false;

    for node in options {
        let defel = expect_defelem(node, "AlterType")?;
        let defname = def_name(defel);

        if defname == "storage" {
            let a = defGetString(mcx, defel)?;
            atparams.storage = if pg_strcaseeq(&a, "plain") {
                TYPSTORAGE_PLAIN
            } else if pg_strcaseeq(&a, "external") {
                TYPSTORAGE_EXTERNAL
            } else if pg_strcaseeq(&a, "extended") {
                TYPSTORAGE_EXTENDED
            } else if pg_strcaseeq(&a, "main") {
                TYPSTORAGE_MAIN
            } else {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("storage \"{a}\" not recognized"))
                    .finish(errloc(4353, "AlterType"))
                    .map(|()| unreachable!());
            };

            /* If the type isn't varlena, it can't support non-PLAIN storage. */
            if atparams.storage != TYPSTORAGE_PLAIN && typForm.typlen != -1 {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("fixed-size types must have storage PLAIN")
                    .finish(errloc(4362, "AlterType"))
                    .map(|()| unreachable!());
            }

            if atparams.storage != TYPSTORAGE_PLAIN && typForm.typstorage == TYPSTORAGE_PLAIN {
                requireSuper = true;
            } else if atparams.storage == TYPSTORAGE_PLAIN && typForm.typstorage != TYPSTORAGE_PLAIN
            {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("cannot change type's storage to PLAIN")
                    .finish(errloc(4381, "AlterType"))
                    .map(|()| unreachable!());
            }
            atparams.update_storage = true;
        } else if defname == "receive" {
            atparams.receive_oid = if defel.arg.is_some() {
                findTypeReceiveFunction(mcx, &defGetQualifiedName(defel)?, typeOid)?
            } else {
                InvalidOid
            };
            atparams.update_receive = true;
            requireSuper = true;
        } else if defname == "send" {
            atparams.send_oid = if defel.arg.is_some() {
                findTypeSendFunction(mcx, &defGetQualifiedName(defel)?, typeOid)?
            } else {
                InvalidOid
            };
            atparams.update_send = true;
            requireSuper = true;
        } else if defname == "typmod_in" {
            atparams.typmodin_oid = if defel.arg.is_some() {
                findTypeTypmodinFunction(mcx, &defGetQualifiedName(defel)?)?
            } else {
                InvalidOid
            };
            atparams.update_typmodin = true;
            requireSuper = true;
        } else if defname == "typmod_out" {
            atparams.typmodout_oid = if defel.arg.is_some() {
                findTypeTypmodoutFunction(mcx, &defGetQualifiedName(defel)?)?
            } else {
                InvalidOid
            };
            atparams.update_typmodout = true;
            requireSuper = true;
        } else if defname == "analyze" {
            atparams.analyze_oid = if defel.arg.is_some() {
                findTypeAnalyzeFunction(mcx, &defGetQualifiedName(defel)?, typeOid)?
            } else {
                InvalidOid
            };
            atparams.update_analyze = true;
            requireSuper = true;
        } else if defname == "subscript" {
            atparams.subscript_oid = if defel.arg.is_some() {
                findTypeSubscriptingFunction(mcx, &defGetQualifiedName(defel)?, typeOid)?
            } else {
                InvalidOid
            };
            atparams.update_subscript = true;
            requireSuper = true;
        } else if matches!(
            defname,
            "input"
                | "output"
                | "internallength"
                | "passedbyvalue"
                | "alignment"
                | "like"
                | "category"
                | "preferred"
                | "default"
                | "element"
                | "delimiter"
                | "collatable"
        ) {
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("type attribute \"{defname}\" cannot be changed"))
                .finish(errloc(4474, "AlterType"))
                .map(|()| unreachable!());
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("type attribute \"{defname}\" not recognized"))
                .finish(errloc(4479, "AlterType"))
                .map(|()| unreachable!());
        }
    }

    /* Permissions check. */
    if requireSuper {
        if !superuser::call(mcx)? {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("must be superuser to alter a type")
                .finish(errloc(4492, "AlterType"))
                .map(|()| unreachable!());
        }
    } else if !backend_catalog_aclchk_seams::object_ownercheck::call(
        TypeRelationId,
        typeOid,
        get_user_id::call(),
    )? {
        backend_catalog_aclchk_seams::aclcheck_error_type::call(
            types_acl::acl::ACLCHECK_NOT_OWNER,
            typeOid,
        )?;
    }

    /* Disallow ALTER TYPE SET on non-base types. */
    if typForm.typtype != TYPTYPE_BASE {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("{} is not a base type", format_type_be(typeOid)?))
            .finish(errloc(4513, "AlterType"))
            .map(|()| unreachable!());
    }
    if is_true_array_type(&typForm) {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("{} is not a base type", format_type_be(typeOid)?))
            .finish(errloc(4522, "AlterType"))
            .map(|()| unreachable!());
    }

    /* Recursively update this type and any arrays/domains over it. */
    AlterTypeRecurse(typeOid, false, atparams)?;

    Ok(object_address_set_type(typeOid))
}

/// `AlterTypeRecurse(typeOid, isImplicitArray, tup, catalog, atparams)`
/// (typecmds.c:4561) — one recursion step. The per-row write + the
/// GenerateTypeDependencies/hook run in the pg_type owner seam
/// `alter_type_recurse_update` (returns the row's `typarray`); the catalog
/// `Relation` arg is implicit (the owner opens pg_type). The domain scan is the
/// pg_type owner seam `scan_domains_over_basetype`.
pub fn AlterTypeRecurse(
    type_oid: Oid,
    is_implicit_array: bool,
    mut atparams: types_catalog::pg_type::TypeAttrUpdate,
) -> PgResult<()> {
    /* Update the current type's tuple + rebuild deps + hook (owner seam). */
    let arrtypoid = backend_catalog_pg_type_seams::alter_type_recurse_update::call(
        type_oid,
        is_implicit_array,
        atparams,
    )?;

    /*
     * Arrays inherit their base type's typmodin/typmodout, but none of the
     * other properties. Recurse to the array type if needed.
     */
    if !is_implicit_array && (atparams.update_typmodin || atparams.update_typmodout) {
        if OidIsValid(arrtypoid) {
            let arrparams = types_catalog::pg_type::TypeAttrUpdate {
                update_typmodin: atparams.update_typmodin,
                update_typmodout: atparams.update_typmodout,
                typmodin_oid: atparams.typmodin_oid,
                typmodout_oid: atparams.typmodout_oid,
                ..Default::default()
            };
            AlterTypeRecurse(arrtypoid, true, arrparams)?;
        }
    }

    /*
     * Recurse to domains; some properties are not inherited by domains, so
     * clear those update flags.
     */
    atparams.update_receive = false;
    atparams.update_typmodin = false;
    atparams.update_typmodout = false;
    atparams.update_subscript = false;

    /* Skip the scan if nothing remains to be done. */
    if !(atparams.update_storage || atparams.update_send || atparams.update_analyze) {
        return Ok(());
    }

    /* Search pg_type for domains over this type, and recurse to each. */
    let domains = backend_catalog_pg_type_seams::scan_domains_over_basetype::call(type_oid)?;
    for domain_oid in domains {
        AlterTypeRecurse(domain_oid, false, atparams)?;
    }

    Ok(())
}

// ===========================================================================
// helpers
// ===========================================================================

/// `InvalidObjectAddress` (objectaddress.h) — `{InvalidOid, InvalidOid, 0}`.
fn InvalidObjectAddress() -> ObjectAddress {
    ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    }
}

/// `get_element_type(typid)` (lsyscache) → `InvalidOid` when none.
fn get_element_type_seam(typid: Oid) -> PgResult<Oid> {
    Ok(backend_utils_cache_lsyscache_seams::get_element_type::call(typid)?.unwrap_or(InvalidOid))
}

/// `get_array_type(typid)` (lsyscache) → `InvalidOid` when none.
fn get_array_type_seam(typid: Oid) -> PgResult<Oid> {
    Ok(backend_utils_cache_lsyscache_seams::get_array_type::call(typid)?.unwrap_or(InvalidOid))
}

/// `get_namespace_name(nspid)` text (panics-safe `String`).
fn get_namespace_name_str(nspid: Oid) -> PgResult<String> {
    let scratch = MemoryContext::new("get_namespace_name");
    let name = get_namespace_name::call(scratch.mcx(), nspid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    Ok(name)
}

/// `lfirst_node(DefElem, ...)` — a parameter-list cell must be a `DefElem`.
fn expect_defelem<'a>(node: &'a Node, fname: &'static str) -> PgResult<&'a DefElem> {
    match node.as_defelem() {
        Some(d) => Ok(d),
        None => Err(ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg_internal(format!("{fname}: parameter list element is not a DefElem"))
            .finish(errloc(0, fname))
            .expect_err("ereport(ERROR) always yields an Err")),
    }
}

/// `NameListToString(names)` (catalog/namespace.c) over a bare name list.
fn NameListToString_seam(_mcx: Mcx<'_>, names: &[String]) -> PgResult<String> {
    name_list_to_string::call(names.to_vec())
}

/// `get_namespace_name(nspid)` (lsyscache) projected for `aclcheck_error_schema`'s
/// objname.
fn get_namespace_name_seam(mcx: Mcx<'_>, nspid: Oid) -> PgResult<Option<String>> {
    Ok(get_namespace_name::call(mcx, nspid)?.map(|s| s.as_str().to_string()))
}

// ---------------------------------------------------------------------------
// checkEnumOwner   (typecmds.c:1303)
// ---------------------------------------------------------------------------

/// `checkEnumOwner(tup)` (typecmds.c:1303) — verify the type is an enum and the
/// current user owns it. The C function reads `Form_pg_type` out of the syscache
/// tuple; here we project it to the fixed-part [`FormData_pg_type`].
fn checkEnumOwner(typ: &types_tuple::pg_type::FormData_pg_type) -> PgResult<()> {
    /* Check that this is actually an enum */
    if typ.typtype != TYPTYPE_ENUM {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("{} is not an enum", format_type_be(typ.oid)?))
            .finish(errloc(1320, "checkEnumOwner"))
            .map(|()| unreachable!());
    }

    /* Permission check: must own type */
    if !backend_catalog_aclchk_seams::object_ownercheck::call(
        TypeRelationId,
        typ.oid,
        get_user_id::call(),
    )? {
        backend_catalog_aclchk_seams::aclcheck_error_type::call(
            types_acl::acl::ACLCHECK_NOT_OWNER,
            typ.oid,
        )?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// AlterEnum   (typecmds.c:1336)
// ---------------------------------------------------------------------------

/// `AlterEnum(stmt)` (typecmds.c:1336) — ALTER TYPE ... ADD VALUE / RENAME VALUE.
///
/// Looks up the enum type by its qualified name, checks it is an enum the user
/// owns, then either renames an existing label (`oldVal` set) or adds a new
/// label (`AddEnumLabel`), placing it before/after a neighbor as requested.
/// Finally fires the post-alter hook and returns the type's `ObjectAddress`.
///
/// `oldVal`/`newVal`/`newValNeighbor` arrive pre-decoded from the rich
/// `AlterEnumStmt` (the seam adapter projects the `String` payloads).
fn AlterEnum(
    type_name: &[String],
    old_val: Option<&str>,
    new_val: &str,
    new_val_neighbor: Option<&str>,
    new_val_is_after: bool,
    skip_if_new_val_exists: bool,
) -> PgResult<ObjectAddress> {
    /* Make a TypeName so we can use standard type lookup machinery */
    let enum_type_oid = typename_type_id_from_names(type_name)?;

    /* SearchSysCache1(TYPEOID, ...) + "cache lookup failed" on a missing row. */
    let tup = read_type_form(enum_type_oid)?;

    /* Check it's an enum and check user has permission to ALTER the enum */
    checkEnumOwner(&tup)?;

    if let Some(old_val) = old_val {
        /* Rename an existing label */
        RenameEnumLabel(enum_type_oid, old_val, new_val)?;
    } else {
        /* Add a new label */
        //
        // `AddEnumLabel` surfaces the IF NOT EXISTS "already exists, skipping"
        // NOTICE as an `Err` carrying a sub-ERROR level (the pg_enum port's
        // convention). In C this path emits the NOTICE and returns normally, so
        // re-report any sub-ERROR diagnostic and continue; propagate real
        // errors.
        if let Err(e) = AddEnumLabel(
            enum_type_oid,
            new_val,
            new_val_neighbor,
            new_val_is_after,
            skip_if_new_val_exists,
        ) {
            if e.level() >= ERROR {
                return Err(e);
            }
            ThrowErrorData(e)?;
        }
    }

    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        TypeRelationId,
        enum_type_oid,
        0,
    )?;

    Ok(object_address_set_type(enum_type_oid))
}

// ---------------------------------------------------------------------------
// init_seams
// ---------------------------------------------------------------------------

/// `pub fn init_seams()` — install typecmds' two INWARD seams:
///   * `RemoveTypeById` — the real `OCLASS_TYPE` drop body (above);
///   * `alter_type_owner_oid` — F4 (`AlterTypeOwner_oid`), the real body.
///
/// The OUTWARD seams declared in this unit's `-seams` crate
/// (`make_range_constructors`, `make_multirange_constructors`) are installed
/// by their real owners (`ProcedureCreate`), not here. The composite
/// `DefineRelation` call goes directly through `backend-commands-tablecmds-seams`.
/// `castNode(List, stmt->object)` + `strVal` over each `String` child — the
/// qualified type name as a `Vec<String>` (the `makeTypeNameFromNameList` input
/// the F4 entry points consume). The generic ALTER dispatch (`commands/alter.c`)
/// hands `stmt->object` as a `List *` `Node`; the F4 bodies want the namelist.
fn names_from_list_node(object: &Node) -> PgResult<Vec<String>> {
    let items = object.as_list().ok_or_else(|| {
        PgError::error("typecmds: ALTER TYPE/DOMAIN object must be a List of String nodes")
    })?;
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        let s = it
            .as_string()
            .ok_or_else(|| PgError::error("strVal: String node expected"))?;
        out.push(s.sval.as_deref().unwrap_or("").to_string());
    }
    Ok(out)
}

/// Inward-seam adapter for `RenameType(RenameStmt *stmt)` (the
/// `commands/alter.c` `ExecRenameStmt` dispatch target): decode `stmt->object`
/// (qualified name `List`) + `stmt->newname` + `stmt->renameType`, then run the
/// ported `RenameType` body.
fn rename_type_seam<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_parsenodes::RenameStmt,
) -> PgResult<ObjectAddress> {
    let object = stmt.object.as_deref().ok_or_else(|| {
        PgError::error("ExecRenameStmt: RENAME object must be set for DOMAIN/TYPE")
    })?;
    let names = names_from_list_node(object)?;
    let new_type_name = stmt.newname.as_deref().unwrap_or("");
    RenameType(mcx, &names, new_type_name, stmt.renameType)
}

/// Inward-seam adapter for `AlterTypeNamespace(names, newschema, objecttype,
/// *oldschema)` — decode the `List` namelist and surface the old-schema OID on
/// the tuple's second slot (the C `*oldschema` out-parameter).
fn alter_type_namespace_seam<'mcx>(
    mcx: Mcx<'mcx>,
    names: &Node,
    newschema: &str,
    objecttype: ObjectType,
    want_oldschema: bool,
) -> PgResult<(ObjectAddress, Oid)> {
    let names = names_from_list_node(names)?;
    let mut oldschema = InvalidOid;
    let addr = AlterTypeNamespace(
        mcx,
        &names,
        newschema,
        objecttype,
        if want_oldschema {
            Some(&mut oldschema)
        } else {
            None
        },
    )?;
    Ok((addr, oldschema))
}

/// Inward-seam adapter for `AlterTypeOwner(names, newOwnerId, objecttype)` —
/// decode the `List` namelist and run the ported body.
fn alter_type_owner_seam<'mcx>(
    mcx: Mcx<'mcx>,
    names: &Node,
    new_owner_id: Oid,
    objecttype: ObjectType,
) -> PgResult<ObjectAddress> {
    let names = names_from_list_node(names)?;
    AlterTypeOwner(mcx, &names, new_owner_id, objecttype)
}

pub fn init_seams() {
    me::RemoveTypeById::set(RemoveTypeById);
    me::alter_type_owner_oid::set(AlterTypeOwner_oid);
    // Generic ALTER dispatch targets (commands/alter.c) — adapt the `List`
    // namelist / `RenameStmt` the dispatcher passes onto the ported F4 bodies.
    me::RenameType::set(rename_type_seam);
    me::AlterTypeNamespace::set(alter_type_namespace_seam);
    me::AlterTypeNamespace_oid::set(AlterTypeNamespace_oid);
    me::AlterTypeOwner::set(alter_type_owner_seam);

    // AlterTableNamespaceInternal (tablecmds.c) moves a relation's row type via
    // this seam during ALTER TABLE ... SET SCHEMA; the body lives here.
    backend_commands_tablecmds_seams::alter_type_namespace_internal::set(
        AlterTypeNamespaceInternal,
    );

    // ATExecChangeOwner (tablecmds.c) changes a relation's row type owner via
    // this seam during ALTER TABLE ... OWNER TO; the body lives here.
    backend_commands_tablecmds_seams::alter_type_owner_internal::set(AlterTypeOwnerInternal);

    // ProcessUtilitySlow dispatch target (utility.c) for CREATE DOMAIN — decode
    // the `CreateDomainStmt` and run the ported `DefineDomain` body.
    backend_tcop_utility_out_seams::define_domain::set(define_domain_seam);

    // ProcessUtilitySlow `T_DefineStmt` dispatch target (utility.c) — the `kind`
    // switch; the OBJECT_TYPE (base type) leg runs the ported `DefineType` body.
    backend_tcop_utility_out_seams::define_stmt::set(define_stmt_seam);

    // ProcessUtilitySlow `T_CreateEnumStmt` / `T_CreateRangeStmt` dispatch
    // targets (utility.c) — decode the parse node and run the ported
    // `DefineEnum` / `DefineRange` bodies.
    backend_tcop_utility_out_seams::define_enum::set(define_enum_seam);
    backend_tcop_utility_out_seams::define_range::set(define_range_seam);

    // ProcessUtilitySlow `T_CompositeTypeStmt` dispatch target (utility.c:1625)
    // — decode the `CompositeTypeStmt` and run the ported `DefineCompositeType`
    // body (which builds the composite CreateStmt and calls DefineRelation with
    // RELKIND_COMPOSITE_TYPE).
    backend_tcop_utility_out_seams::define_composite_type::set(define_composite_type_seam);

    // typecmds.c statics `makeRangeConstructors` / `makeMultirangeConstructors`,
    // reached from `DefineRange`. Modeled as this unit's own outward seams
    // (declared in `-seams`, called from `DefineRange`) so they could panic
    // loudly while `ProcedureCreate` was unported; now ported here and installed.
    me::make_range_constructors::set(make_range_constructors_seam);
    me::make_multirange_constructors::set(make_multirange_constructors_seam);

    // ProcessUtilitySlow `T_AlterTypeStmt` dispatch target (utility.c) — ALTER
    // TYPE … SET (…).
    backend_tcop_utility_out_seams::alter_type::set(alter_type_seam);

    // ProcessUtilitySlow `T_AlterEnumStmt` dispatch target (utility.c) — ALTER
    // TYPE … ADD VALUE / RENAME VALUE.
    backend_tcop_utility_out_seams::alter_enum::set(alter_enum_seam);
}

/// Outward-seam adapter for `AlterEnum((AlterEnumStmt *) stmt)`
/// (utility.c `ProcessUtilitySlow` `T_AlterEnumStmt`): project the rich
/// `typeName` (List of String) and the `String` payloads, then run the ported
/// [`AlterEnum`] body.
fn alter_enum_seam<'mcx>(
    _mcx: Mcx<'mcx>,
    stmt: &RichNode<'mcx>,
) -> PgResult<ObjectAddress> {
    let aes = match stmt.as_alterenumstmt() {
        Some(s) => s,
        None => return Err(PgError::error("alter_enum_seam: statement is not an AlterEnumStmt")),
    };

    // typeName: List of String -> Vec<String>.
    let mut type_name: Vec<String> = Vec::with_capacity(aes.typeName.len());
    for n in aes.typeName.iter() {
        match n.as_string() {
            Some(s) => type_name.push(s.sval.as_str().to_string()),
            None => return Err(PgError::error("ALTER TYPE: enum type name element is not a String")),
        }
    }

    // C requires a newVal for both ADD VALUE and RENAME VALUE TO; the grammar
    // guarantees it. `oldVal`/`newValNeighbor` are optional.
    let new_val = match aes.newVal.as_ref() {
        Some(s) => s.as_str().to_string(),
        None => return Err(PgError::error("ALTER TYPE: enum statement missing new value")),
    };
    let old_val = aes.oldVal.as_ref().map(|s| s.as_str().to_string());
    let neighbor = aes.newValNeighbor.as_ref().map(|s| s.as_str().to_string());

    AlterEnum(
        &type_name,
        old_val.as_deref(),
        &new_val,
        neighbor.as_deref(),
        aes.newValIsAfter,
        aes.skipIfNewValExists,
    )
}

/// Outward-seam adapter for `AlterType(stmt)` (utility.c `ProcessUtilitySlow`
/// `T_AlterTypeStmt`): project the rich `typeName` (List of String) and decode
/// the `options` (List of DefElem) into the flat parsenodes form the ported
/// [`AlterType`] body consumes.
fn alter_type_seam<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &RichNode<'mcx>,
) -> PgResult<ObjectAddress> {
    let ats = match stmt.as_altertypestmt() {
        Some(s) => s,
        None => return Err(PgError::error("alter_type_seam: statement is not an AlterTypeStmt")),
    };

    // typeName: List of String -> Vec<String>.
    let mut type_name: Vec<String> = Vec::with_capacity(ats.typeName.len());
    for n in ats.typeName.iter() {
        match n.as_string() {
            Some(s) => type_name.push(s.sval.as_str().to_string()),
            None => return Err(PgError::error("ALTER TYPE: type name element is not a String")),
        }
    }

    // options: List of DefElem -> Vec<parsenodes::Node::DefElem>.
    let mut options: Vec<Node> = Vec::with_capacity(ats.options.len());
    for n in ats.options.iter() {
        options.push(backend_parser_parse_type::rich_node_to_parse(n)?);
    }

    AlterType(mcx, &type_name, &options)
}

/// Outward-seam adapter for `DefineEnum((CreateEnumStmt *) stmt)`
/// (utility.c `ProcessUtilitySlow`): decode the `CreateEnumStmt`'s qualified
/// type name and the ordered list of label `String` values, then run the
/// ported [`DefineEnum`] body.
fn define_enum_seam<'mcx>(mcx: Mcx<'mcx>, stmt: &RichNode<'mcx>) -> PgResult<ObjectAddress> {
    let ces = match stmt.as_createenumstmt() {
        Some(s) => s,
        None => {
            return Err(PgError::error(
                "define_enum_seam: statement is not a CreateEnumStmt",
            ))
        }
    };

    // typeName: List of String nodes -> Vec<String>.
    let mut type_name: Vec<String> = Vec::with_capacity(ces.typeName.len());
    for n in ces.typeName.iter() {
        match n.as_string() {
            Some(s) => type_name.push(s.sval.as_str().to_string()),
            None => {
                return Err(PgError::error(
                    "CREATE TYPE AS ENUM: type name element is not a String",
                ))
            }
        }
    }

    // vals: List of String nodes (the labels) -> Vec<String>.
    let mut vals: Vec<String> = Vec::with_capacity(ces.vals.len());
    for n in ces.vals.iter() {
        match n.as_string() {
            Some(s) => vals.push(s.sval.as_str().to_string()),
            None => {
                return Err(PgError::error(
                    "CREATE TYPE AS ENUM: enum label is not a String",
                ))
            }
        }
    }

    DefineEnum(mcx, &type_name, &vals)
}

/// Outward-seam adapter for `DefineRange(pstate, (CreateRangeStmt *) stmt)`
/// (utility.c `ProcessUtilitySlow`): decode the `CreateRangeStmt`'s qualified
/// type name and the `DefElem` parameter list, then run the ported
/// [`DefineRange`] body. `pstate` is threaded for parity with C but
/// `DefineRange`'s port does not consult it.
fn define_range_seam<'mcx>(
    mcx: Mcx<'mcx>,
    _pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
    stmt: &RichNode<'mcx>,
) -> PgResult<ObjectAddress> {
    let crs = match stmt.as_createrangestmt() {
        Some(s) => s,
        None => {
            return Err(PgError::error(
                "define_range_seam: statement is not a CreateRangeStmt",
            ))
        }
    };

    // typeName: List of String nodes -> Vec<String>.
    let mut type_name: Vec<String> = Vec::with_capacity(crs.typeName.len());
    for n in crs.typeName.iter() {
        match n.as_string() {
            Some(s) => type_name.push(s.sval.as_str().to_string()),
            None => {
                return Err(PgError::error(
                    "CREATE TYPE AS RANGE: type name element is not a String",
                ))
            }
        }
    }

    // params: List of DefElem nodes -> Vec<parsenodes::Node::DefElem>.
    let mut params: Vec<Node> = Vec::with_capacity(crs.params.len());
    for n in crs.params.iter() {
        params.push(backend_parser_parse_type::rich_node_to_parse(n)?);
    }

    DefineRange(mcx, &type_name, &params)
}

/// Inward-seam adapter for `makeRangeConstructors` (typecmds.c static).
fn make_range_constructors_seam(
    name: String,
    namespace: Oid,
    range_oid: Oid,
    subtype: Oid,
) -> PgResult<()> {
    makeRangeConstructors(&name, namespace, range_oid, subtype)
}

/// Inward-seam adapter for `makeMultirangeConstructors` (typecmds.c static).
fn make_multirange_constructors_seam(
    name: String,
    namespace: Oid,
    multirange_oid: Oid,
    range_oid: Oid,
    range_array_oid: Oid,
) -> PgResult<Oid> {
    makeMultirangeConstructors(&name, namespace, multirange_oid, range_oid, range_array_oid)
}

/// Outward-seam adapter for the `DefineStmt` `kind` switch (utility.c:1395-1450).
/// Only the `OBJECT_TYPE` (base-type) leg is ported here; the other kinds
/// (AGGREGATE / OPERATOR / TS* / COLLATION) live in unported owners and raise
/// loudly. `pstate` is threaded for parity (the C `DefineType` consumes it for
/// `parser_errposition`, which `DefineType`'s port does not yet need).
fn define_stmt_seam<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
    stmt: &RichNode<'mcx>,
) -> PgResult<ObjectAddress> {
    use types_nodes::parsenodes::{OBJECT_AGGREGATE, OBJECT_OPERATOR, OBJECT_TYPE};

    let ds = match stmt.as_definestmt() {
        Some(d) => d,
        None => return Err(PgError::error("define_stmt_seam: statement is not a DefineStmt")),
    };

    // definition: List of DefElem -> Vec<parsenodes::Node::DefElem>.  Shared by
    // all kinds.
    let mut definition: Vec<Node> = Vec::with_capacity(ds.definition.len());
    for n in ds.definition.iter() {
        definition.push(backend_parser_parse_type::rich_node_to_parse(n)?);
    }

    match ds.kind {
        OBJECT_TYPE => {
            // defnames: List of String -> Vec<String>.
            let mut names: Vec<String> = Vec::with_capacity(ds.defnames.len());
            for n in ds.defnames.iter() {
                match n.as_string() {
                    Some(s) => names.push(s.sval.as_str().to_string()),
                    None => {
                        return Err(PgError::error(
                            "CREATE TYPE: type name element is not a String",
                        ))
                    }
                }
            }
            DefineType(
                mcx,
                &names,
                &definition,
                pstate.p_sourcetext.as_ref().map(|s| s.as_str()),
            )
        }
        OBJECT_OPERATOR => {
            // Assert(stmt->args == NIL).
            let names = decode_name_list(&ds.defnames);
            backend_commands_operatorcmds::DefineOperator(mcx, &names, &definition)
        }
        OBJECT_AGGREGATE => {
            // defnames + args (the FunctionParameter list / numDirectArgs pair).
            let names = decode_name_list(&ds.defnames);
            let mut args: Vec<Node> = Vec::with_capacity(ds.args.len());
            for n in ds.args.iter() {
                args.push(backend_parser_parse_type::rich_node_to_parse(n)?);
            }
            backend_commands_aggregatecmds::DefineAggregate(
                mcx,
                pstate,
                &names,
                &args,
                ds.oldstyle,
                &definition,
                ds.replace,
            )
        }
        _ => Err(PgError::error(format!(
            "define_stmt: DefineStmt kind {:?} not yet ported",
            ds.kind
        ))),
    }
}

/// Decode a `List *` of `String`/NULL name elements (a possibly-qualified
/// object name) into the `&[Option<String>]` (`NameList`) shape the
/// `QualifiedNameGetCreationNamespace` callers consume.
fn decode_name_list(defnames: &[types_nodes::nodes::NodePtr<'_>]) -> Vec<Option<String>> {
    defnames
        .iter()
        .map(|n| n.as_string().map(|s| s.sval.as_str().to_string()))
        .collect()
}

/// Outward-seam adapter for `DefineDomain(pstate, (CreateDomainStmt *) stmt)`
/// (utility.c `ProcessUtilitySlow`): decode the `CreateDomainStmt`'s qualified
/// name, base `TypeName`, optional `COLLATE` clause, and constraint list, then
/// run the ported [`DefineDomain`] body. `pstate` is threaded for parity with C
/// but `DefineDomain` does not consult it.
fn define_domain_seam<'mcx>(
    mcx: Mcx<'mcx>,
    _pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
    stmt: &RichNode<'mcx>,
) -> PgResult<ObjectAddress> {
    let cds = match stmt.as_createdomainstmt() {
        Some(s) => s,
        None => {
            return Err(PgError::error(
                "define_domain_seam: statement is not a CreateDomainStmt",
            ))
        }
    };

    // domainname: List of String nodes -> Vec<String>.
    let mut domainname: Vec<String> = Vec::with_capacity(cds.domainname.len());
    for n in cds.domainname.iter() {
        match n.as_string() {
            Some(s) => domainname.push(s.sval.as_str().to_string()),
            None => {
                return Err(PgError::error(
                    "CREATE DOMAIN: domain name element is not a String",
                ))
            }
        }
    }

    // typeName: raw TypeName node -> resolver TypeName.
    let raw_tn = match cds.typeName.as_deref().and_then(|n| n.as_typename()) {
        Some(tn) => tn,
        None => return Err(PgError::error("CREATE DOMAIN: missing or invalid base TypeName")),
    };
    let type_name = backend_parser_parse_type::raw_typename_to_parse(raw_tn)?;

    // collClause: optional CollateClause -> Option<Vec<String>> (the COLLATE name).
    let coll_clause: Option<Vec<String>> =
        match cds.collClause.as_deref().and_then(|n| n.as_collateclause()) {
            Some(cc) => {
                let mut names: Vec<String> = Vec::with_capacity(cc.collname.len());
                for n in cc.collname.iter() {
                    match n.as_string() {
                        Some(s) => names.push(s.sval.as_str().to_string()),
                        None => {
                            return Err(PgError::error(
                                "CREATE DOMAIN: COLLATE name element is not a String",
                            ))
                        }
                    }
                }
                Some(names)
            }
            None => None,
        };

    // constraints: List of Constraint nodes -> Vec<RichNode>.
    let mut constraints: Vec<RichNode<'mcx>> = Vec::with_capacity(cds.constraints.len());
    for n in cds.constraints.iter() {
        constraints.push((**n).clone_in(mcx)?);
    }

    DefineDomain(
        mcx,
        &domainname,
        &type_name,
        coll_clause.as_deref(),
        &constraints,
    )
}
