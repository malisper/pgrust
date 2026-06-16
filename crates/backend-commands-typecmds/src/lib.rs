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
    NameListToString, QualifiedNameGetCreationNamespace, RangeVarAdjustRelationPersistence,
    RangeVarGetAndCheckCreationNamespace,
};
use backend_catalog_pg_cast::CastCreate;
use backend_catalog_pg_enum::EnumValuesCreate;
use backend_catalog_pg_range::RangeCreate;
use backend_catalog_pg_type::{
    makeArrayTypeName, makeMultirangeTypeName, moveArrayTypeName, TypeCreate, TypeShellMake,
};
use backend_utils_error::ereport;
use mcx::Mcx;

use types_acl::{AclMode, ACLCHECK_OK, ACL_CREATE, ACL_EXECUTE};
use types_catalog::catalog::{NAMESPACE_RELATION_ID, TYPE_RELATION_ID};
use types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_INTERNAL};
use types_catalog::pg_type::{
    TypeCreateParams, TYPTYPE_BASE, TYPTYPE_ENUM, TYPTYPE_MULTIRANGE, TYPTYPE_PSEUDO, TYPTYPE_RANGE,
};
use types_core::catalog::{BTREE_AM_OID, INT4OID, INTERNALOID, OIDOID, PROCEDURE_RELATION_ID};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::pg_error::ErrorLocation;
use types_error::{
    PgError, PgResult, ERRCODE_AMBIGUOUS_FUNCTION, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR, WARNING,
};
use types_nodes::parsenodes::OBJECT_FUNCTION;
use types_parsenodes::{
    DefElem, Node, TypeName, COERCION_CODE_EXPLICIT, COERCION_METHOD_FUNCTION,
    PROVOLATILE_IMMUTABLE, PROVOLATILE_VOLATILE,
};
use types_tuple::heaptuple::{
    CSTRINGOID, DEFAULT_COLLATION_OID, FLOAT8OID, TYPALIGN_CHAR, TYPALIGN_DOUBLE, TYPALIGN_INT,
    TYPALIGN_SHORT, TYPSTORAGE_EXTENDED, TYPSTORAGE_EXTERNAL, TYPSTORAGE_MAIN, TYPSTORAGE_PLAIN,
};

use backend_commands_typecmds_seams as me;
use backend_commands_typecmds_seams::TypeCmdsRangeVar;

use backend_catalog_aclchk_seams::{aclcheck_error, object_aclcheck};
use backend_commands_define_seams::DefElemArg;
use backend_parser_coerce_seams::is_binary_coercible;
use backend_utils_adt_format_type_seams::format_type_be_owned;
use backend_utils_init_miscinit_seams::{get_user_id, is_binary_upgrade, superuser};

use backend_catalog_pg_opclass_seams::get_default_opclass;
use backend_commands_functioncmds_seams::{
    aclcheck_error_schema, func_signature_string, get_func_name, lookup_func_name,
    name_list_to_string, namespace_aclcheck,
};
use backend_commands_opclasscmds_seams::get_opclass_oid;
use backend_utils_cache_lsyscache_seams::{
    func_volatile, get_func_rettype, get_opclass_input_type, get_typcollation, get_typisdefined,
    get_typlen, get_typlenbyvalalign, get_typtype, type_is_collatable,
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

/// `RELKIND_COMPOSITE_TYPE` (catalog/pg_class.h).
const RELKIND_COMPOSITE_TYPE: i8 = b'c' as i8;

/// `ProcedureRelationId` — pg_proc's OID, used as the aclcheck classid.
const ProcedureRelationId: Oid = PROCEDURE_RELATION_ID;
/// `NamespaceRelationId` — pg_namespace's OID.
const NamespaceRelationId: Oid = NAMESPACE_RELATION_ID;
/// `TypeRelationId` — pg_type's OID.
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

/// `ObjectAddressSet(addr, TypeRelationId, oid)`.
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

/// `parser_errposition(pstate, location)` — without a `ParseState` we cannot map
/// the byte offset to a cursor position, so (as the unported-grammar dispatch
/// passes no pstate) we mirror the `pstate == NULL` C behaviour: no errposition.
fn parser_errposition(_location: i32) -> i32 {
    0
}

/// `defGetString(def)` (define.c).
fn defGetString(mcx: Mcx<'_>, defel: &DefElem) -> PgResult<String> {
    let s = backend_commands_define_seams::def_get_string::call(
        mcx,
        defel.defname.clone().unwrap_or_default(),
        defel_arg(defel),
    )?;
    Ok(s.to_string())
}

/// `defGetBoolean(def)` (define.c).
fn defGetBoolean(defel: &DefElem) -> PgResult<bool> {
    backend_commands_define_seams::def_get_boolean::call(
        defel.defname.clone().unwrap_or_default(),
        defel_arg(defel),
    )
}

/// Project a `DefElem`'s value node into the `DefElemArg` the define.c value
/// accessors switch on (`nodeTag(def->arg)` dispatch).
fn defel_arg(defel: &DefElem) -> Option<DefElemArg> {
    let node = defel.arg.as_deref()?;
    Some(match node {
        Node::Integer(i) => DefElemArg::Integer(i.ival as i64),
        Node::Float(f) => DefElemArg::Float(f.fval.clone().unwrap_or_default()),
        Node::Boolean(b) => DefElemArg::Boolean(b.boolval),
        Node::String(s) => DefElemArg::String(s.sval.clone().unwrap_or_default()),
        _ => DefElemArg::AStar,
    })
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
        let (typlen, typbyval, typalign, typstorage) = typenameTypeFields(&defGetTypeName(el)?)?;
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
        preferred = defGetBoolean(el)?;
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
        byValue = defGetBoolean(el)?;
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
        collation = if defGetBoolean(el)? {
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
        aclcheck_error_schema::call(aclresult, get_namespace_name_seam(enumNamespace)?)?;
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
        aclcheck_error_schema::call(aclresult, get_namespace_name_seam(typeNamespace)?)?;
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
fn findRangeCanonicalFunction(mcx: Mcx<'_>, procname: &[String], typeOid: Oid) -> PgResult<Oid> {
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
fn findRangeSubtypeDiffFunction(mcx: Mcx<'_>, procname: &[String], subtype: Oid) -> PgResult<Oid> {
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
pub fn AssignTypeArrayOid() -> PgResult<Oid> {
    /* Use binary-upgrade override for pg_type.typarray? */
    if is_binary_upgrade::call() {
        let next = backend_catalog_pg_type_seams::take_binary_upgrade_next_array_pg_type_oid::call()?;
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
        let next = backend_catalog_pg_type_seams::take_binary_upgrade_next_mrng_pg_type_oid::call()?;
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
        let next =
            backend_catalog_pg_type_seams::take_binary_upgrade_next_mrng_array_pg_type_oid::call()?;
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
    typevar: &types_nodes::rawnodes::RangeVar<'mcx>,
    coldeflist: &[Node],
) -> PgResult<ObjectAddress> {
    /*
     * Check for collision with an existing type name. If there is one and it's
     * an autogenerated array, we can rename it out of the way.  This check is
     * here mainly to get a better error message about a "type" instead of below
     * about a "relation".
     */
    let typeNamespace = RangeVarGetAndCheckCreationNamespace(mcx, typevar, NoLock, None)?;
    RangeVarAdjustRelationPersistence(mcx, typevar, typeNamespace)?;
    let relname: String = typevar
        .relname
        .as_ref()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
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
     * DefineRelation (tablecmds.c) builds the composite CreateStmt internally
     * from the carried RangeVar + coldeflist (RELKIND_COMPOSITE_TYPE), the
     * remaining CreateStmt fields being the C's fixed
     * NIL/NIL/NIL/ONCOMMIT_NOOP/NULL/false. Seam panics until tablecmds lands.
     */
    let carrier = TypeCmdsRangeVar {
        catalogname: typevar.catalogname.as_ref().map(|s| s.as_str().to_string()),
        schemaname: typevar.schemaname.as_ref().map(|s| s.as_str().to_string()),
        relname: typevar.relname.as_ref().map(|s| s.as_str().to_string()),
        inh: typevar.inh,
        relpersistence: typevar.relpersistence,
        location: typevar.location,
    };
    let address = me::define_relation_composite::call(carrier, coldeflist.to_vec())?;

    Ok(address)
}

// ===========================================================================
// helpers
// ===========================================================================

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

/// `get_namespace_name(nspid)` projected for `aclcheck_error_schema`'s objname.
fn get_namespace_name_seam(nspid: Oid) -> PgResult<Option<String>> {
    backend_commands_functioncmds_seams::get_namespace_name::call(nspid)
}

// ---------------------------------------------------------------------------
// init_seams
// ---------------------------------------------------------------------------

/// `pub fn init_seams()` — install typecmds' two INWARD seams:
///   * `RemoveTypeById` — the real `OCLASS_TYPE` drop body (above);
///   * `alter_type_owner_oid` — F4 (`AlterTypeOwner_oid`), out of F2 scope, so a
///     loud panic until F4 lands.
///
/// The OUTWARD seams declared in this unit's `-seams` crate
/// (`make_range_constructors`, `make_multirange_constructors`,
/// `define_relation_composite`) are installed by their real owners
/// (`ProcedureCreate`/`DefineRelation`), not here.
pub fn init_seams() {
    me::RemoveTypeById::set(RemoveTypeById);
    me::alter_type_owner_oid::set(|_type_oid, _new_owner_id, _has_depend_entry| {
        panic!("alter_type_owner_oid: AlterTypeOwner_oid not yet ported (typecmds.c F4)")
    });
}
